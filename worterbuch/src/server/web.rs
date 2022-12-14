use super::common::process_incoming_message;
use crate::{config::Config, worterbuch::Worterbuch};
use anyhow::Result;
use futures::{sink::SinkExt, stream::StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::{spawn, sync::mpsc};
use uuid::Uuid;
use warp::{addr::remote, ws::Message, ws::Ws};
use warp::{Filter, Reply};

pub(crate) async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) {
    log::info!("Starting Web Server …");

    let wb_ws = worterbuch.clone();
    let ws_path = "ws";

    let ws = {
        log::info!("Mounting ws endpoint at /{ws_path} …");
        warp::ws().and(warp::path(ws_path)).and(remote()).map(
            move |ws: Ws, remote: Option<SocketAddr>| {
                let worterbuch = wb_ws.clone();
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
        )
    };

    let ws_route = ws;

    let start_explorer = config.explorer;

    if start_explorer {
        let explorer_path = "*";
        let explorer = {
            log::info!("Mounting explorer endpoint at {explorer_path} …");
            warp::fs::dir(config.web_root_path.clone())
        };
        let expl_route = explorer;
        let routes = expl_route.or(ws_route);
        run_server(routes, &config).await;
    } else {
        let routes = ws_route;
        run_server(routes, &config).await;
    };
}

async fn run_server<F>(filter: F, config: &Config)
where
    F: Filter + Clone + Send + Sync + 'static,
    F::Extract: Reply,
{
    let server = warp::serve(filter);
    let port = config.web_port;
    let bind_addr = config.bind_addr;
    let cert_path = &config.cert_path;
    let key_path = &config.key_path;

    let addr = (bind_addr, port);

    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        log::info!("Using TLS certificate {}", cert_path);
        log::info!("Using TLS private key {}", key_path);
        log::info!("Starting web server with TLS …");

        server
            .tls()
            .cert_path(cert_path)
            .key_path(key_path)
            .run(addr)
            .await;
    } else {
        log::info!("Starting web server without TLS …");
        server.run(addr).await;
    }

    log::info!("Web server stopped.");
}

pub(crate) async fn serve(
    websocket: warp::ws::WebSocket,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
) -> Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_write, mut client_read) = websocket.split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let msg = Message::binary(bytes);
            if let Err(e) = client_write.send(msg).await {
                log::error!("Error sending message to client {client_id} ({remote_addr}): {e}");
                break;
            }
        }
    });

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …");

    loop {
        if let Some(Ok(incoming_msg)) = client_read.next().await {
            if incoming_msg.is_binary() {
                let data = incoming_msg.as_bytes();
                if !process_incoming_message(client_id, data, worterbuch.clone(), tx.clone())
                    .await?
                {
                    break;
                }
            }
        } else {
            break;
        }
    }

    log::info!("WS stream of client {client_id} ({remote_addr}) closed.");

    let mut wb = worterbuch.write().await;
    wb.disconnected(client_id, remote_addr);

    Ok(())
}
