//! Telegram adapter for the Shinobu bot framework.
//!
//! Connects to the Telegram Bot API via [`teloxide`] and converts incoming
//! updates into [`Event`](snb_core::event::Event)s. Requires a bot token
//! configured in `configs/TGAdapter/config.toml`.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose};
use serde::Deserialize;
use snb_core::adapter::{Adapter, run_async};
use snb_core::context::{self, BotContext};
use snb_core::event::{
    ChatType, ContentItem, Event, EventType, FileSource, ImageSource, Message, TextFormat,
};
use snb_core::plugin::{PluginType, SnbPlugin, Version};
use snb_macros::plugin;
use teloxide::prelude::*;
use teloxide::types::{
    ChatKind, FileId, InputFile, InputMedia, InputMediaDocument, MessageEntityKind, MessageId,
    ParseMode, PublicChatKind, ReplyParameters, UpdateKind,
};

#[derive(Deserialize)]
struct Config {
    bot_token: String,
    api_url: Option<String>,
    #[serde(default, alias = "admin_users", alias = "admin_user_ids")]
    admins: Vec<AdminId>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum AdminId {
    String(String),
    Integer(i64),
}

impl Config {
    fn is_admin(&self, user_id: &str) -> bool {
        self.admins.iter().any(|admin| admin.matches(user_id))
    }
}

impl AdminId {
    fn matches(&self, user_id: &str) -> bool {
        match self {
            Self::String(id) => id == user_id,
            Self::Integer(id) => id.to_string() == user_id,
        }
    }
}

const DEFAULT_CONFIG: &str = r#"# Telegram Adapter Configuration
bot_token = "YOUR_BOT_TOKEN_HERE"
# api_url = "https://api.telegram.org"
admins = []
"#;

// Plugin-wide state. Each plugin is a singleton (one cdylib, one instance), so
// module-level globals mirror the framework's own `context::set_bot` pattern and
// let the stateless `#[adapter]` function and `on_event` share the same data.
// `RwLock<Option<_>>` (not `OnceLock`) so `on_unload` can reset it for reloads.
static CONFIG: RwLock<Option<Config>> = RwLock::new(None);
static TG_BOT: RwLock<Option<Bot>> = RwLock::new(None);
#[plugin]
struct TGAdapter;

impl SnbPlugin for TGAdapter {
    fn new() -> Self {
        Self
    }
    fn name(&self) -> &str {
        "TGAdapter"
    }
    fn version(&self) -> Version {
        Version {
            major: 0,
            minor: 0,
            patch: 1,
        }
    }
    fn plugin_type(&self) -> PluginType {
        PluginType::Adapter
    }
    fn on_load(&mut self, ctx: Arc<dyn BotContext>) {
        context::set_bot(ctx);
        context::set_plugin(self.name());

        match context::plugin().load_config(Path::new("config.toml")) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(config) => {
                    *CONFIG.write().unwrap() = Some(config);
                }
                Err(e) => {
                    log::error!("failed to parse config: {e}");
                }
            },
            Err(_) => {
                if let Err(e) =
                    context::plugin().write_config(Path::new("config.toml"), DEFAULT_CONFIG)
                {
                    log::error!("failed to write default config: {e}");
                }
                log::warn!(
                    "config not found, default config written to configs/TGAdapter/config.toml, please edit it with your bot token"
                );
            }
        }

        context::register_all(self.name());
        log::info!("v{} loaded!", self.version());
    }
    fn on_unload(&mut self) {
        *TG_BOT.write().unwrap() = None;
        *CONFIG.write().unwrap() = None;
        log::info!("unloaded!");
    }
}

async fn tg_dispatcher(bot: Arc<dyn BotContext>) {
    let (token, api_url) = match CONFIG.read().unwrap().as_ref() {
        Some(config) => (config.bot_token.clone(), config.api_url.clone()),
        None => {
            log::error!("bot_token not configured, adapter not starting");
            return;
        }
    };

    let mut tg_bot = Bot::new(token);
    if let Some(api_url) = api_url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        match url::Url::parse(api_url) {
            Ok(url) => {
                log::info!("using custom Telegram API URL: {api_url}");
                tg_bot = tg_bot.set_api_url(url);
            }
            Err(e) => {
                log::error!("invalid api_url {api_url:?}: {e}; falling back to default");
            }
        }
    }
    *TG_BOT.write().unwrap() = Some(tg_bot.clone());

    log::info!("start Telegram dispatcher");

    // Reconnect loop. teloxide's dispatcher *panics* (rather than returning an
    // error) when it can't reach the Telegram API while preparing — e.g. a
    // network timeout on the initial GetMe at startup. We run each attempt in a
    // spawned task so tokio turns such a panic into a `JoinError` instead of
    // letting it unwind off this adapter thread, then retry with exponential
    // backoff so a transient outage no longer takes the whole bot down.
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);
    loop {
        let tg_bot = tg_bot.clone();
        let bot_ctx = bot.clone();
        let attempt = tokio::spawn(async move {
            let handler = |update: Update, bot_ctx: Arc<dyn BotContext>| async move {
                if let Some(event) = convert_update(&update) {
                    bot_ctx.emit_event(event);
                }
                respond(())
            };

            let mut dispatcher =
                Dispatcher::builder(tg_bot, dptree::entry().branch(dptree::endpoint(handler)))
                    .dependencies(dptree::deps![bot_ctx])
                    .build();

            dispatcher.dispatch().await;
        });

        match attempt.await {
            // Dispatcher returned on its own — a clean shutdown, stop retrying.
            Ok(()) => break,
            Err(e) if e.is_panic() => {
                log::error!(
                    "Telegram dispatcher crashed (network unreachable?); reconnecting in {}s",
                    backoff.as_secs()
                );
            }
            // Task cancelled (e.g. runtime shutting down): don't spin, just stop.
            Err(e) => {
                log::error!("Telegram dispatcher task aborted: {e}");
                break;
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

#[derive(Clone, Copy)]
struct TelegramAdapter;

impl Adapter for TelegramAdapter {
    fn run(&self, bot: Arc<dyn BotContext>) {
        run_async(tg_dispatcher(bot));
    }

    fn send(&self, event: &Event) -> anyhow::Result<()> {
        let Some(msg) = &event.message else {
            return Ok(());
        };
        let Some(chat_id) = msg.to.as_deref() else {
            anyhow::bail!("TGAdapter outgoing message is missing message.to chat id");
        };
        let chat_id = chat_id
            .parse::<i64>()
            .with_context(|| format!("invalid Telegram chat id: {chat_id}"))?;
        let bot = TG_BOT
            .read()
            .unwrap()
            .as_ref()
            .cloned()
            .context("TGAdapter bot not initialized")?;
        let event_type = event.event_type.clone();
        let origin = event.source.clone();
        let msg = msg.clone();

        spawn_send_task(async move {
            match event_type {
                EventType::Message => send_message_items(bot, chat_id, msg, origin).await,
                EventType::MessageEdit => edit_message_item(bot, chat_id, msg).await,
                EventType::MessageDelete => delete_message_item(bot, chat_id, msg).await,
                kind => log::debug!("TGAdapter ignored outgoing event type: {kind:?}"),
            }
        });

        Ok(())
    }
}

snb_core::registry::submit! {
    snb_core::registry::AdapterRegistration {
        factory: || Arc::new(TelegramAdapter),
    }
}

fn spawn_send_task<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(future);
    } else {
        std::thread::spawn(move || run_async(future));
    }
}

async fn send_message_items(bot: Bot, chat_id: i64, msg: Message, origin: String) {
    let local_id = msg.id.clone();
    let reply_to = msg.reply_to;
    let delete_after = msg.delete_after;
    let mut text = OutgoingText::default();
    let mut attachments = Vec::new();

    for item in msg.content {
        match item {
            ContentItem::Text {
                text: item_text,
                format,
            } => text.push_text(item_text, format),
            ContentItem::File { .. } | ContentItem::Image { .. } => attachments.push(item),
            ContentItem::Other { kind, .. } => {
                log::debug!("TGAdapter ignored unsupported outgoing content kind: {kind}");
            }
        }
    }

    let caption = text.into_payload();
    let mut sent_ids = Vec::new();
    if attachments.is_empty() {
        if let Some(text) = caption {
            sent_ids
                .extend(send_text(&bot, chat_id, text, reply_to.as_deref(), delete_after).await);
        }
        emit_message_sent_if_needed(
            &origin,
            chat_id,
            local_id.as_deref(),
            sent_ids.first().copied(),
        );
        return;
    }

    if attachments.len() > 1
        && attachments
            .iter()
            .all(|item| matches!(item, ContentItem::File { .. }))
    {
        sent_ids.extend(
            send_file_media_groups(
                &bot,
                chat_id,
                attachments,
                caption,
                reply_to.as_deref(),
                delete_after,
            )
            .await,
        );
        emit_message_sent_if_needed(
            &origin,
            chat_id,
            local_id.as_deref(),
            sent_ids.first().copied(),
        );
        return;
    }

    let mut first_caption = caption;
    for item in attachments {
        match item {
            ContentItem::File {
                source,
                file_name,
                file_id,
            } => {
                sent_ids.extend(
                    send_file(
                        &bot,
                        chat_id,
                        source,
                        file_name,
                        file_id,
                        first_caption.take(),
                        reply_to.as_deref(),
                        delete_after,
                    )
                    .await,
                );
            }
            ContentItem::Image {
                source,
                file_id,
                caption,
            } => {
                let caption = merge_captions(first_caption.take(), caption);
                sent_ids.extend(
                    send_image(
                        &bot,
                        chat_id,
                        source,
                        file_id,
                        caption,
                        reply_to.as_deref(),
                        delete_after,
                    )
                    .await,
                );
            }
            ContentItem::Text { .. } | ContentItem::Other { .. } => unreachable!(),
        }
    }

    emit_message_sent_if_needed(
        &origin,
        chat_id,
        local_id.as_deref(),
        sent_ids.first().copied(),
    );
}

#[derive(Default)]
struct OutgoingText {
    text: String,
    format: Option<TextFormat>,
    has_plain: bool,
    mixed_formats: bool,
}

#[derive(Clone)]
struct FormattedPayload {
    text: String,
    format: Option<TextFormat>,
}

impl OutgoingText {
    fn push_text(&mut self, text: String, format: Option<TextFormat>) {
        self.push(text, format);
    }

    fn push(&mut self, text: String, format: Option<TextFormat>) {
        if text.is_empty() {
            return;
        }

        match format {
            Some(format) if !self.has_plain => {
                if let Some(existing) = self.format {
                    self.mixed_formats |= existing != format;
                } else {
                    self.format = Some(format);
                }
            }
            Some(_) => {
                self.mixed_formats = true;
            }
            None => {
                if self.format.is_some() {
                    self.mixed_formats = true;
                }
                self.has_plain = true;
            }
        }

        self.text.push_str(&text);
    }

    fn into_payload(self) -> Option<FormattedPayload> {
        (!self.text.is_empty()).then_some(FormattedPayload {
            text: self.text,
            format: (!self.mixed_formats && !self.has_plain)
                .then_some(self.format)
                .flatten(),
        })
    }
}

async fn send_text(
    bot: &Bot,
    chat_id: i64,
    text: FormattedPayload,
    reply_to: Option<&str>,
    delete_after: Option<Duration>,
) -> Vec<MessageId> {
    let mut req = bot.send_message(ChatId(chat_id), text.text);
    if let Some(parse_mode) = text.format.map(parse_mode_from_format) {
        req = req.parse_mode(parse_mode);
    }
    if let Some(reply) = reply_parameters(reply_to) {
        req = req.reply_parameters(reply);
    }
    match req.await {
        Ok(sent) => {
            schedule_delete_after(bot, chat_id, sent.id, delete_after);
            vec![sent.id]
        }
        Err(e) => {
            log::error!("TGAdapter send_message error: {e}");
            Vec::new()
        }
    }
}

async fn send_file(
    bot: &Bot,
    chat_id: i64,
    source: FileSource,
    file_name: Option<String>,
    file_id: Option<String>,
    caption: Option<FormattedPayload>,
    reply_to: Option<&str>,
    delete_after: Option<Duration>,
) -> Vec<MessageId> {
    let input = match input_file_from_source(source, file_name, file_id) {
        Ok(input) => input,
        Err(e) => {
            log::error!("TGAdapter cannot prepare file: {e:#}");
            return Vec::new();
        }
    };

    let mut req = bot.send_document(ChatId(chat_id), input);
    if let Some(caption) = caption {
        if let Some(parse_mode) = caption.format.map(parse_mode_from_format) {
            req = req.parse_mode(parse_mode);
        }
        req = req.caption(caption.text);
    }
    if let Some(reply) = reply_parameters(reply_to) {
        req = req.reply_parameters(reply);
    }
    match req.await {
        Ok(sent) => {
            schedule_delete_after(bot, chat_id, sent.id, delete_after);
            vec![sent.id]
        }
        Err(e) => {
            log::error!("TGAdapter send_document error: {e}");
            Vec::new()
        }
    }
}

async fn send_image(
    bot: &Bot,
    chat_id: i64,
    source: ImageSource,
    file_id: Option<String>,
    caption: Option<FormattedPayload>,
    reply_to: Option<&str>,
    delete_after: Option<Duration>,
) -> Vec<MessageId> {
    let input = match input_file_from_image(source, file_id) {
        Ok(input) => input,
        Err(e) => {
            log::error!("TGAdapter cannot prepare image: {e:#}");
            return Vec::new();
        }
    };

    let mut req = bot.send_photo(ChatId(chat_id), input);
    if let Some(caption) = caption {
        if let Some(parse_mode) = caption.format.map(parse_mode_from_format) {
            req = req.parse_mode(parse_mode);
        }
        req = req.caption(caption.text);
    }
    if let Some(reply) = reply_parameters(reply_to) {
        req = req.reply_parameters(reply);
    }
    match req.await {
        Ok(sent) => {
            schedule_delete_after(bot, chat_id, sent.id, delete_after);
            vec![sent.id]
        }
        Err(e) => {
            log::error!("TGAdapter send_photo error: {e}");
            Vec::new()
        }
    }
}

async fn send_file_media_groups(
    bot: &Bot,
    chat_id: i64,
    files: Vec<ContentItem>,
    mut caption: Option<FormattedPayload>,
    reply_to: Option<&str>,
    delete_after: Option<Duration>,
) -> Vec<MessageId> {
    let mut sent_ids = Vec::new();
    let mut chunk = Vec::new();
    for file in files {
        chunk.push(file);
        if chunk.len() == 10 {
            let chunk_caption = caption.take();
            sent_ids.extend(
                send_file_media_group(
                    bot,
                    chat_id,
                    std::mem::take(&mut chunk),
                    chunk_caption,
                    reply_to,
                    delete_after,
                )
                .await,
            );
        }
    }

    if !chunk.is_empty() {
        sent_ids.extend(
            send_file_media_group(bot, chat_id, chunk, caption, reply_to, delete_after).await,
        );
    }

    sent_ids
}

async fn send_file_media_group(
    bot: &Bot,
    chat_id: i64,
    files: Vec<ContentItem>,
    caption: Option<FormattedPayload>,
    reply_to: Option<&str>,
    delete_after: Option<Duration>,
) -> Vec<MessageId> {
    if files.len() == 1 {
        if let Some(ContentItem::File {
            source,
            file_name,
            file_id,
        }) = files.into_iter().next()
        {
            return send_file(
                bot,
                chat_id,
                source,
                file_name,
                file_id,
                caption,
                reply_to,
                delete_after,
            )
            .await;
        }
        return Vec::new();
    }

    let last_index = files.len() - 1;
    let mut media = Vec::with_capacity(files.len());
    for (idx, file) in files.into_iter().enumerate() {
        let ContentItem::File {
            source,
            file_name,
            file_id,
        } = file
        else {
            unreachable!();
        };
        let input = match input_file_from_source(source, file_name, file_id) {
            Ok(input) => input,
            Err(e) => {
                log::error!("TGAdapter cannot prepare file: {e:#}");
                return Vec::new();
            }
        };
        let mut document = InputMediaDocument::new(input);
        if idx == last_index
            && let Some(caption) = caption.clone()
        {
            if let Some(parse_mode) = caption.format.map(parse_mode_from_format) {
                document = document.parse_mode(parse_mode);
            }
            document = document.caption(caption.text);
        }
        media.push(InputMedia::Document(document));
    }

    let mut req = bot.send_media_group(ChatId(chat_id), media);
    if let Some(reply) = reply_parameters(reply_to) {
        req = req.reply_parameters(reply);
    }
    match req.await {
        Ok(sent) => sent
            .into_iter()
            .map(|sent_message| {
                schedule_delete_after(bot, chat_id, sent_message.id, delete_after);
                sent_message.id
            })
            .collect(),
        Err(e) => {
            log::error!("TGAdapter send_media_group error: {e}");
            Vec::new()
        }
    }
}

fn emit_message_sent_if_needed(
    origin: &str,
    chat_id: i64,
    local_id: Option<&str>,
    message_id: Option<MessageId>,
) {
    let (Some(local_id), Some(message_id)) = (local_id, message_id) else {
        return;
    };

    let mut event = Event::message_sent(
        "TGAdapter",
        message_id.0.to_string(),
        Some(local_id.to_string()),
    );
    if let Some(msg) = event.message.as_mut() {
        msg.to = Some(chat_id.to_string());
    }
    event.receiver = Some(origin.to_string());
    context::bot().emit_event(event);
}

async fn edit_message_item(bot: Bot, chat_id: i64, msg: Message) {
    let Some(message_id) = msg.id.as_deref() else {
        log::error!("TGAdapter edit_message missing message.id");
        return;
    };
    let Ok(message_id) = message_id.parse::<i32>() else {
        log::error!("TGAdapter edit_message requires native Telegram message id: {message_id}");
        return;
    };

    let mut text = OutgoingText::default();
    for item in &msg.content {
        if let ContentItem::Text {
            text: item_text,
            format,
        } = item
        {
            text.push_text(item_text.clone(), *format);
        }
    }
    let Some(payload) = text.into_payload() else {
        log::error!("TGAdapter edit_message has no text content");
        return;
    };

    let mut req = bot.edit_message_text(ChatId(chat_id), MessageId(message_id), payload.text);
    if let Some(parse_mode) = payload.format.map(parse_mode_from_format) {
        req = req.parse_mode(parse_mode);
    }
    if let Err(e) = req.await {
        // Telegram returns an error when the new text is identical to the old
        // one; that is harmless for status updates, so only warn.
        log::warn!("TGAdapter edit_message error: {e}");
    }
}

async fn delete_message_item(bot: Bot, chat_id: i64, msg: Message) {
    let Some(message_id) = msg.id else {
        log::error!("TGAdapter delete_message missing message.id");
        return;
    };

    let Ok(message_id) = message_id.parse::<i32>() else {
        log::error!("TGAdapter delete_message requires native Telegram message id: {message_id}");
        return;
    };

    if let Err(e) = bot
        .delete_message(ChatId(chat_id), MessageId(message_id))
        .await
    {
        log::error!("TGAdapter delete_message error: {e}");
    }
}

fn schedule_delete_after(bot: &Bot, chat_id: i64, message_id: MessageId, delay: Option<Duration>) {
    let Some(delay) = delay else {
        return;
    };

    let bot = bot.clone();
    spawn_send_task(async move {
        tokio::time::sleep(delay).await;
        if let Err(e) = bot.delete_message(ChatId(chat_id), message_id).await {
            log::warn!("TGAdapter delete_after delete_message error: {e}");
        }
    });
}

fn merge_captions(
    first: Option<FormattedPayload>,
    second: Option<String>,
) -> Option<FormattedPayload> {
    match (first, second.filter(|s| !s.is_empty())) {
        (Some(first), Some(second)) if first.text == second => Some(first),
        (Some(mut first), Some(second)) => {
            first.text.push('\n');
            first.text.push_str(&second);
            first.format = None;
            Some(first)
        }
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(FormattedPayload {
            text: second,
            format: None,
        }),
        (None, None) => None,
    }
}

#[allow(deprecated)]
fn parse_mode_from_format(format: TextFormat) -> ParseMode {
    match format {
        TextFormat::Markdown => ParseMode::Markdown,
        TextFormat::MarkdownV2 => ParseMode::MarkdownV2,
        TextFormat::Html => ParseMode::Html,
    }
}

fn reply_parameters(reply_to: Option<&str>) -> Option<ReplyParameters> {
    let msg_id = reply_to?.parse::<i32>().ok()?;
    Some(ReplyParameters {
        message_id: teloxide::types::MessageId(msg_id),
        ..Default::default()
    })
}

fn input_file_from_source(
    source: FileSource,
    file_name: Option<String>,
    file_id: Option<String>,
) -> anyhow::Result<InputFile> {
    let mut input = if let Some(file_id) = non_empty(file_id) {
        InputFile::file_id(FileId(file_id))
    } else {
        match source {
            FileSource::Id(file_id) => InputFile::file_id(FileId(file_id)),
            FileSource::Path(path) => InputFile::file(path),
            FileSource::Url(url) => InputFile::url(
                url::Url::parse(&url).with_context(|| format!("invalid file URL: {url}"))?,
            ),
        }
    };

    if let Some(file_name) = non_empty(file_name) {
        input = input.file_name(file_name);
    }

    Ok(input)
}

fn input_file_from_image(
    source: ImageSource,
    file_id: Option<String>,
) -> anyhow::Result<InputFile> {
    if let Some(file_id) = non_empty(file_id) {
        return Ok(InputFile::file_id(FileId(file_id)));
    }

    match source {
        ImageSource::Id(file_id) => Ok(InputFile::file_id(FileId(file_id))),
        ImageSource::Url(url) => Ok(InputFile::url(
            url::Url::parse(&url).with_context(|| format!("invalid image URL: {url}"))?,
        )),
        ImageSource::Path(path) => Ok(InputFile::file(path)),
        ImageSource::Base64(data) => {
            let bytes = decode_base64_image(&data)?;
            Ok(InputFile::memory(bytes))
        }
    }
}

fn decode_base64_image(data: &str) -> anyhow::Result<Vec<u8>> {
    let encoded = data
        .split_once(',')
        .filter(|(prefix, _)| prefix.contains("base64"))
        .map_or(data, |(_, encoded)| encoded)
        .trim();
    general_purpose::STANDARD
        .decode(encoded)
        .context("invalid base64 image data")
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn is_configured_admin(user_id: Option<&str>) -> bool {
    let Some(user_id) = user_id else {
        return false;
    };
    CONFIG
        .read()
        .unwrap()
        .as_ref()
        .is_some_and(|config| config.is_admin(user_id))
}

fn convert_update(update: &Update) -> Option<Event> {
    match &update.kind {
        UpdateKind::Message(msg)
        | UpdateKind::EditedMessage(msg)
        | UpdateKind::ChannelPost(msg)
        | UpdateKind::EditedChannelPost(msg)
        | UpdateKind::BusinessMessage(msg)
        | UpdateKind::EditedBusinessMessage(msg) => convert_message(update, msg),

        kind => {
            let kind_name = match kind {
                UpdateKind::Message(_) => unreachable!(),
                UpdateKind::EditedMessage(_) => unreachable!(),
                UpdateKind::ChannelPost(_) => unreachable!(),
                UpdateKind::EditedChannelPost(_) => unreachable!(),
                UpdateKind::BusinessMessage(_) => unreachable!(),
                UpdateKind::EditedBusinessMessage(_) => unreachable!(),
                UpdateKind::BusinessConnection(_) => "BusinessConnection",
                UpdateKind::DeletedBusinessMessages(_) => "DeletedBusinessMessages",
                UpdateKind::MessageReaction(_) => "MessageReaction",
                UpdateKind::MessageReactionCount(_) => "MessageReactionCount",
                UpdateKind::InlineQuery(_) => "InlineQuery",
                UpdateKind::ChosenInlineResult(_) => "ChosenInlineResult",
                UpdateKind::CallbackQuery(_) => "CallbackQuery",
                UpdateKind::ShippingQuery(_) => "ShippingQuery",
                UpdateKind::PreCheckoutQuery(_) => "PreCheckoutQuery",
                UpdateKind::PurchasedPaidMedia(_) => "PurchasedPaidMedia",
                UpdateKind::Poll(_) => "Poll",
                UpdateKind::PollAnswer(_) => "PollAnswer",
                UpdateKind::MyChatMember(_) => "MyChatMember",
                UpdateKind::ChatMember(_) => "ChatMember",
                UpdateKind::ChatJoinRequest(_) => "ChatJoinRequest",
                UpdateKind::ChatBoost(_) => "ChatBoost",
                UpdateKind::RemovedChatBoost(_) => "RemovedChatBoost",
                UpdateKind::Error(_) => "Error",
            };
            let data = serde_json::to_string(kind).unwrap_or_default();
            Some(Event {
                event_type: snb_core::event::EventType::Other(kind_name.to_string()),
                source: "tg-adapter".to_string(),
                data,
                command: None,
                message: None,
                sender: Some("TGAdapter".to_string()),
                receiver: None,
            })
        }
    }
}

fn convert_attachments(msg: &teloxide::types::Message) -> Vec<ContentItem> {
    let mut items = Vec::new();

    if let Some(document) = msg.document() {
        items.push(ContentItem::File {
            source: FileSource::Id(document.file.id.0.clone()),
            file_name: document.file_name.clone(),
            file_id: Some(document.file.id.0.clone()),
        });
    }

    if let Some(photo) = msg
        .photo()
        .and_then(|photos| photos.iter().max_by_key(|photo| photo.width * photo.height))
    {
        items.push(ContentItem::Image {
            source: ImageSource::Id(photo.file.id.0.clone()),
            file_id: Some(photo.file.id.0.clone()),
            caption: msg.caption().map(str::to_string),
        });
    }

    if let Some(audio) = msg.audio() {
        items.push(ContentItem::File {
            source: FileSource::Id(audio.file.id.0.clone()),
            file_name: audio.file_name.clone(),
            file_id: Some(audio.file.id.0.clone()),
        });
    }

    if let Some(video) = msg.video() {
        items.push(ContentItem::File {
            source: FileSource::Id(video.file.id.0.clone()),
            file_name: video.file_name.clone(),
            file_id: Some(video.file.id.0.clone()),
        });
    }

    if let Some(animation) = msg.animation() {
        items.push(ContentItem::File {
            source: FileSource::Id(animation.file.id.0.clone()),
            file_name: animation.file_name.clone(),
            file_id: Some(animation.file.id.0.clone()),
        });
    }

    if let Some(voice) = msg.voice() {
        items.push(ContentItem::File {
            source: FileSource::Id(voice.file.id.0.clone()),
            file_name: Some("voice.ogg".to_string()),
            file_id: Some(voice.file.id.0.clone()),
        });
    }

    items
}

fn convert_message(update: &Update, msg: &teloxide::types::Message) -> Option<Event> {
    let text = msg.text().or(msg.caption()).unwrap_or("");
    let mut content = if text.is_empty() {
        vec![]
    } else {
        vec![ContentItem::text(text)]
    };
    content.extend(convert_attachments(msg));

    let from = msg.from.as_ref().map(|u| u.id.0.to_string());
    let chat_id = msg.chat.id.0.to_string();
    let chat_type = match &msg.chat.kind {
        ChatKind::Private(_) => ChatType::Private,
        ChatKind::Public(public) => match public.kind {
            PublicChatKind::Group | PublicChatKind::Supergroup(_) => ChatType::Group,
            PublicChatKind::Channel(_) => ChatType::Guild,
        },
    };

    let entities = msg
        .parse_entities()
        .or_else(|| msg.parse_caption_entities())
        .unwrap_or_default();

    let mut at = Vec::new();
    let mut command: Option<snb_core::event::Command> = None;

    for entity in &entities {
        match entity.kind() {
            MessageEntityKind::Mention => {
                at.push(entity.text().to_string());
            }
            MessageEntityKind::TextMention { user } => {
                at.push(user.id.0.to_string());
            }
            MessageEntityKind::BotCommand if command.is_none() => {
                let raw = entity.text();
                let stripped = raw.strip_prefix('/').unwrap_or(raw);
                let cmd = match stripped.find('@') {
                    Some(i) => &stripped[..i],
                    None => stripped,
                };
                let args_start = entity.end();
                let args = if args_start < text.len() {
                    text[args_start..].trim_start()
                } else {
                    ""
                };
                command = Some(snb_core::event::Command {
                    cmd: cmd.to_string(),
                    args: args.to_string(),
                });
            }
            _ => {
                log::debug!("Unresolved message entity kind: {:?}", entity.kind());
            }
        }
    }

    let id = Some(msg.id.0.to_string());
    let reply_to = msg.reply_to_message().map(|m| m.id.0.to_string());
    let is_admin = is_configured_admin(from.as_deref());

    let event_msg = Message {
        id,
        reply_to,
        content,
        from,
        to: Some(chat_id),
        at,
        chat_type: Some(chat_type),
        is_admin,
        delete_after: None,
    };

    let kind_name = match &update.kind {
        UpdateKind::Message(_) => "Message",
        UpdateKind::EditedMessage(_) => "EditedMessage",
        UpdateKind::ChannelPost(_) => "ChannelPost",
        UpdateKind::EditedChannelPost(_) => "EditedChannelPost",
        UpdateKind::BusinessMessage(_) => "BusinessMessage",
        UpdateKind::EditedBusinessMessage(_) => "EditedBusinessMessage",
        _ => unreachable!(),
    };

    let (event_type, command, message) = match command {
        Some(cmd) => (
            snb_core::event::EventType::Command,
            Some(cmd),
            Some(event_msg),
        ),
        None => (snb_core::event::EventType::Message, None, Some(event_msg)),
    };

    Some(Event {
        event_type,
        source: "tg-adapter".to_string(),
        data: kind_name.to_string(),
        command,
        message,
        sender: Some("TGAdapter".to_string()),
        receiver: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_accepts_string_and_numeric_admin_ids() {
        let config: Config = toml::from_str(
            r#"
bot_token = "token"
admins = ["42", 77]
"#,
        )
        .unwrap();

        assert!(config.is_admin("42"));
        assert!(config.is_admin("77"));
        assert!(!config.is_admin("11"));
    }
}
