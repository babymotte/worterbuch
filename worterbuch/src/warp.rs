use crate::server::web::serve;
use crate::stats::track_stats;
use crate::{config::Config, worterbuch::Worterbuch};
use crate::{persistence, server};
use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::RwLock;

use warp::{addr::remote, ws::Ws};
use warp::{Filter, Rejection, Reply};

pub async fn worterbuch_ws_filter(
    config: Config,
) -> Result<impl Filter<Extract = impl Reply, Error = Rejection> + Clone + Send + Sync + 'static> {
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

    if use_persistence {
        persistence::once(worterbuch.clone(), config.clone()).await?;
    }

    let ws_path = "ws";

    log::info!("Mounting ws endpoint at /{ws_path} …");
    let ws = warp::ws().and(warp::path(ws_path)).and(remote()).map(
        move |ws: Ws, remote: Option<SocketAddr>| {
            let worterbuch = worterbuch.clone();
            ws.on_upgrade(move |websocket| async move {
                if let Some(remote) = remote {
                    if let Err(e) = serve(websocket, worterbuch.clone(), remote).await {
                        log::error!("Error in WS connection: {e}");
                    }
                } else {
                    log::error!("Client has no remote address.");
                }
            })
        },
    );

    Ok(ws)
}
