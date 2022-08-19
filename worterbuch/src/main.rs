mod config;
mod persistence;
#[cfg(not(feature = "docker"))]
mod repl;
mod server;
mod stats;
mod store;
mod subscribers;
mod worterbuch;

#[cfg(not(feature = "docker"))]
use crate::repl::repl;
use crate::{config::Config, stats::track_stats, worterbuch::Worterbuch};
use anyhow::Result;
use clap::App;
use std::sync::Arc;
#[cfg(feature = "docker")]
use tokio::signal::unix::{signal, SignalKind};
use tokio::{spawn, sync::RwLock};

#[cfg(not(feature = "multithreaded"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    run("Starting Wörterbuch in single-threaded mode …").await
}

#[cfg(feature = "multithreaded")]
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    run("Starting Wörterbuch in multi-threaded mode …").await
}

async fn run(msg: &str) -> Result<()> {
    let config = Config::new()?;
    let config_pers = config.clone();

    App::new("worterbuch")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("An in-memory data base / message broker hybrid")
        .get_matches();

    log::info!("{msg}");
    log::debug!("Separator: {}", config.separator);
    log::debug!("Wildcard: {}", config.wildcard);
    log::debug!("Multi-Wildcard: {}", config.multi_wildcard);

    let use_persistence = config.use_persistence;

    let worterbuch = if use_persistence {
        persistence::load(config.clone()).await?
    } else {
        Worterbuch::with_config(config.clone())
    };

    let worterbuch = Arc::new(RwLock::new(worterbuch));
    let worterbuch_pers = worterbuch.clone();
    let worterbuch_uptime = worterbuch.clone();

    if use_persistence {
        spawn(persistence::periodic(worterbuch_pers, config_pers));
    }

    spawn(track_stats(worterbuch_uptime, config.clone()));

    #[cfg(feature = "tcp")]
    spawn(server::tcp::start(worterbuch.clone(), config.clone()));

    #[cfg(feature = "web")]
    spawn(server::web::start(worterbuch.clone(), config.clone()));

    #[cfg(feature = "docker")]
    {
        let mut signal = signal(SignalKind::terminate())?;
        signal.recv().await;
    }

    #[cfg(not(feature = "docker"))]
    {
        repl(worterbuch.clone()).await;
    }

    if use_persistence {
        persistence::once(worterbuch.clone(), config.clone()).await?;
    }

    Ok(())
}
