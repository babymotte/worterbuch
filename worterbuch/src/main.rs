mod error;
mod repl;
mod server;
mod store;
mod subscribers;
mod worterbuch;

use crate::{repl::repl, worterbuch::Worterbuch};
use anyhow::Result;
use libworterbuch::config::Config;
use std::sync::Arc;
use tokio::{
    signal::unix::{signal, SignalKind},
    spawn,
    sync::RwLock,
};

#[cfg(not(feature = "multithreaded"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    log::info!("Starting single-threaded instance of Wörterbuch …");
    run().await
}

#[cfg(feature = "multithreaded")]
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    log::info!("Starting multi-threaded instance of Wörterbuch …");
    run().await
}

async fn run() -> Result<()> {
    let config = Config::new()?;

    log::debug!("Separator: {}", config.separator);
    log::debug!("Wildcard: {}", config.wildcard);
    log::debug!("Multi-Wildcard: {}", config.multi_wildcard);

    let worterbuch = Arc::new(RwLock::new(Worterbuch::with_config(config.clone())));

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
