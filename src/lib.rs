use std::path::Path;
use std::sync::{Arc, OnceLock};

use serde::Deserialize;
use snb_core::adapter::{run_async, Adapter};
use snb_core::context::{self, BotContext, PluginHelper};
use snb_core::event::{ChatType, ContentItem, Event, Message};
use snb_core::plugin::{PluginType, SnbPlugin, Version};
use snb_macros::plugin;
use teloxide::prelude::*;
use teloxide::types::{ChatKind, MessageEntityKind, PublicChatKind, ReplyParameters, UpdateKind};

#[derive(Deserialize)]
struct Config {
    bot_token: String,
    api_url: Option<String>,
}

const DEFAULT_CONFIG: &str = r#"# Telegram Adapter Configuration
bot_token = "YOUR_BOT_TOKEN_HERE"
# api_url = "https://api.telegram.org"
"#;

#[derive(Clone)]
#[plugin]
struct TGAdapter {
    bot_token: Option<String>,
    api_url: Option<String>,
    bot: Arc<OnceLock<Bot>>,
}

impl SnbPlugin for TGAdapter {
    fn new() -> Self {
        Self {
            bot_token: None,
            api_url: None,
            bot: Arc::new(OnceLock::new()),
        }
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
        let name = self.name().to_string();
        let p = PluginHelper::for_plugin(&name);
        let config_path = Path::new("TGAdapter/config.toml");

        match p.load_config(config_path) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(config) => {
                    self.bot_token = Some(config.bot_token);
                    self.api_url = config.api_url;
                }
                Err(e) => {
                    p.error(&format!("failed to parse config: {e}"));
                }
            },
            Err(_) => {
                if let Err(e) = p.write_config(Path::new("config.toml"), DEFAULT_CONFIG) {
                    p.error(&format!("failed to write default config: {e}"));
                }
                p.warn("config not found, default config written to configs/TGAdapter/config.toml, please edit it with your bot token");
            }
        }

        p.register_adapter(self.clone());
        p.info(&format!("v{} loaded!", self.version()));
    }
    fn on_unload(&mut self) {
        log::info!("unloaded!");
    }

    fn on_event(&mut self, event: &Event) {
        if event.receiver.as_deref() != Some(self.name()) {
            return;
        }
        let Some(msg) = &event.message else { return };
        let text = msg.text();
        if text.is_empty() { return; }
        let Some(chat_id_str) = &msg.to else { return };
        let Ok(chat_id) = chat_id_str.parse::<i64>() else { return };
        let Some(bot) = self.bot.get() else {
            log::error!("TGAdapter bot not initialized");
            return;
        };
        let bot = bot.clone();
        let reply_to = msg.reply_to.clone();
        tokio::task::spawn(async move {
            let mut req = bot.send_message(ChatId(chat_id), text);
            if let Some(rid) = reply_to {
                if let Ok(msg_id) = rid.parse::<i32>() {
                    req = req.reply_parameters(ReplyParameters {
                        message_id: teloxide::types::MessageId(msg_id),
                        ..Default::default()
                    });
                }
            }
            if let Err(e) = req.await {
                log::error!("TGAdapter send_message error: {e}");
            }
        });
    }
}

impl Adapter for TGAdapter {
    fn run(&self, bot: Arc<dyn BotContext>) {
        let Some(token) = &self.bot_token else {
            bot.logger()
                .error("TGAdapter", "bot_token not configured, adapter not starting");
            return;
        };

        let tg_bot = Bot::new(token);
        let _ = self.bot.set(tg_bot.clone());

        bot.logger().info("TGAdapter", "start Telegram dispatcher");

        run_async(async move {
            let handler = |update: Update, bot_ctx: Arc<dyn BotContext>| async move {
                if let Some(event) = convert_update(&update) {
                    bot_ctx.emit_event(event);
                }
                respond(())
            };

            let mut dispatcher = Dispatcher::builder(
                tg_bot,
                dptree::entry().branch(dptree::endpoint(handler)),
            )
            .dependencies(dptree::deps![bot.clone()])
            .build();

            dispatcher.dispatch().await;
        });
    }
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

fn convert_message(update: &Update, msg: &teloxide::types::Message) -> Option<Event> {
    let text = msg.text().or(msg.caption()).unwrap_or("");
    let content = if text.is_empty() {
        vec![]
    } else {
        vec![ContentItem::Text(text.to_string())]
    };

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

    let event_msg = Message {
        id,
        reply_to,
        content,
        from,
        to: Some(chat_id),
        at,
        chat_type: Some(chat_type),
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
        Some(cmd) => (snb_core::event::EventType::Command, Some(cmd), Some(event_msg)),
        None => (
            snb_core::event::EventType::Other(kind_name.to_string()),
            None,
            Some(event_msg),
        ),
    };

    Some(Event {
        event_type,
        source: "tg-adapter".to_string(),
        data: String::new(),
        command,
        message,
        sender: Some("TGAdapter".to_string()),
        receiver: None,
    })
}
