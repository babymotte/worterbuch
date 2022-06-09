mod config;
mod repl;
mod store;
mod subscribers;
mod utils;
mod worterbuch;

#[cfg(feature = "graphql")]
mod server;

use crate::{config::Config, repl::repl, worterbuch::Worterbuch};
use anyhow::Result;
use std::sync::Arc;
use tokio::{spawn, sync::RwLock};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let config = Config::new()?;

    log::debug!("Separator: {}", config.separator);
    log::debug!("Wildcard: {}", config.wildcard);
    log::debug!("Multi-Wildcard: {}", config.multi_wildcard);

    let worterbuch = Worterbuch::with_config(config.clone());
    let worterbuch = Arc::new(RwLock::new(worterbuch));

    #[cfg(feature = "graphql")]
    spawn(server::start(worterbuch.clone(), config));

    spawn(repl(worterbuch));

    Ok(())
}
