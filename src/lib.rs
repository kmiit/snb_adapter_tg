use std::sync::Arc;
use std::time::Duration;

use snb_core::adapter::{Adapter, run_async};
use snb_core::context::{self, BotContext, PluginHelper};
use snb_core::event::Event;
use snb_core::log_info;
use snb_core::plugin::{PluginType, SnbPlugin, Version};
use snb_macros::plugin;

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
        let p = PluginHelper::for_plugin(self.name());
        p.info(&format!("v{} loaded!", self.version()));
        p.register_adapter(Self);
    }
    fn on_unload(&mut self) {
        log_info!(self.name(), "unloaded!");
    }
}

impl Adapter for TGAdapter {
    fn run(&self, bot: Arc<dyn BotContext>) {
        run_async(async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            bot.emit_event(Event::message("tg-adapter", "Hello from TGAdapter!"));
        });
    }
}
