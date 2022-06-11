mod repl;
mod server;
mod store;
mod subscribers;
mod worterbuch;

use crate::{repl::repl, worterbuch::Worterbuch};
use ::worterbuch::config::Config;
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    signal::unix::{signal, SignalKind},
    spawn,
    sync::RwLock,
};

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
    spawn(server::gql_warp::start(worterbuch.clone(), config.clone()));

    #[cfg(feature = "tcp")]
    spawn(server::tcp::start(worterbuch.clone(), config.clone()));

    #[cfg(feature = "ws")]
    spawn(server::ws::start(worterbuch.clone(), config.clone()));

    spawn(repl(worterbuch));

    let mut signal = signal(SignalKind::terminate())?;
    signal.recv().await;

    Ok(())
}
