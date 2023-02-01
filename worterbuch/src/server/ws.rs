use crate::server::common::process_incoming_message;
use crate::worterbuch::Worterbuch;
use anyhow::Result;
use futures::{sink::SinkExt, stream::StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::{spawn, sync::mpsc};
use uuid::Uuid;
use warp::{addr::remote, ws::Message, ws::Ws};
use warp::{Filter, Rejection, Reply};

pub fn worterbuch_ws_filter(
    worterbuch: Arc<RwLock<Worterbuch>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone + Send + Sync + 'static {
    let ws_path = "ws";

    let ws = {
        log::info!("Mounting ws endpoint at /{ws_path} …");
        warp::ws().and(warp::path(ws_path)).and(remote()).map(
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
        )
    };

    ws
}

async fn serve(
    websocket: warp::ws::WebSocket,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
) -> Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let (mut client_write, mut client_read) = websocket.split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let msg = Message::text(bytes);
            if let Err(e) = client_write.send(msg).await {
                log::error!("Error sending message to client {client_id} ({remote_addr}): {e}");
                break;
            }
        }
    });

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …");

    loop {
        if let Some(Ok(incoming_msg)) = client_read.next().await {
            if let Ok(data) = incoming_msg.to_str() {
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
