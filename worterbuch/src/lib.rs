mod config;
mod persistence;
mod server;
mod stats;
mod store;
mod subscribers;
#[cfg(feature = "warp")]
pub mod warp;
mod worterbuch;

pub use crate::worterbuch::*;
pub use config::*;

use crate::stats::track_stats;
use anyhow::Result;
use std::sync::Arc;
#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::{signal, SignalKind};
use tokio::{
    spawn,
    sync::{oneshot, RwLock},
};

pub async fn run_worterbuch(config: Config) -> Result<()> {
    let use_persistence = config.use_persistence;

    let worterbuch = start_worterbuch(config.clone()).await?;

    let (_terminate_tx, terminate_rx) = oneshot::channel::<()>();

    #[cfg(not(target_os = "windows"))]
    spawn(async move {
        match signal(SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
                log::info!("SIGTERM received.");
                if let Err(_) = _terminate_tx.send(()) {
                    log::error!("Error sending terminate signal");
                }
            }
            Err(e) => log::error!("Error registring SIGTERM handler: {e}"),
        }
    });

    terminate_rx.await?;

    log::info!("Shutting down.");

    if use_persistence {
        persistence::once(worterbuch.clone(), config).await?;
    }

    Ok(())
}

pub async fn start_worterbuch(config: Config) -> Result<Arc<RwLock<Worterbuch>>> {
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

    spawn(server::start(worterbuch.clone(), config.clone()));

    Ok(worterbuch)
}
