mod config;
mod persistence;
mod server;
pub mod stats;
mod store;
mod subscribers;
pub mod warp;
mod worterbuch;

pub use crate::worterbuch::*;
pub use config::*;

use crate::stats::track_stats;
use anyhow::Result;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::{spawn, sync::RwLock};

pub async fn start_worterbuch(config: Config) -> Result<()> {
    let config_pers = config.clone();

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
