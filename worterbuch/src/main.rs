mod config;
mod persistence;
mod server;
mod stats;
mod store;
mod subscribers;
mod worterbuch;

use crate::{config::Config, stats::track_stats, worterbuch::Worterbuch};
use anyhow::Result;
use clap::App;
use std::sync::Arc;
use tokio::runtime::{self, Runtime};
use tokio::signal::unix::{signal, SignalKind};
use tokio::{spawn, sync::RwLock};

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let config = Config::new()?;

    let single_threaded = config.single_threaded;

    let rt = if single_threaded {
        log::info!("Starting Wörterbuch in single-threaded mode …");
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        log::info!("Starting Wörterbuch in multi-threaded mode …");
        Runtime::new()?
    };

    rt.block_on(run())?;

    Ok(())
}

async fn run() -> Result<()> {
    let config = Config::new()?;
    let config_pers = config.clone();

    App::new("worterbuch")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("An in-memory data base / message broker hybrid")
        .get_matches();

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

    spawn(server::tcp::start(worterbuch.clone(), config.clone()));

    spawn(server::web::start(worterbuch.clone(), config.clone()));

    let mut signal = signal(SignalKind::terminate())?;
    signal.recv().await;

    if use_persistence {
        persistence::once(worterbuch.clone(), config.clone()).await?;
    }

    Ok(())
}
