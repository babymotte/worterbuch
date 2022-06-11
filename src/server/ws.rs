use crate::worterbuch::Worterbuch;
use anyhow::Result;
use futures::{sink::SinkExt, stream::StreamExt};
use std::sync::Arc;
use tokio::{
    spawn,
    sync::{mpsc, RwLock},
};
use warp::{
    ws::{Message, Ws},
    Filter,
};
use worterbuch::config::Config;

use super::async_common::process_incoming_message;

pub(crate) async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) {
    log::info!("Starting Web Server …");

    let ws = warp::ws().map(move |ws: Ws| {
        let worterbuch = worterbuch.clone();
        ws.on_upgrade(|websocket| async move {
            if let Err(e) = serve(websocket, worterbuch.clone()).await {
                log::error!("Error in WS connection: {e}");
            }
        })
    });

    let routes = ws;

    let port = config.web_port;
    let bind_addr = config.bind_addr;
    let cert_path = &config.cert_path;
    let key_path = &config.key_path;

    let server = warp::serve(routes);
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

async fn serve(websocket: warp::ws::WebSocket, worterbuch: Arc<RwLock<Worterbuch>>) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_write, mut client_read) = websocket.split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let msg = Message::binary(bytes);
            if let Err(e) = client_write.send(msg).await {
                log::error!("Error sending message to client: {e}");
                break;
            }
        }
    });

    let mut subscriptions = Vec::new();

    loop {
        if let Some(Ok(incoming_msg)) = client_read.next().await {
            if incoming_msg.is_binary() {
                let data = incoming_msg.as_bytes();
                if !process_incoming_message(
                    data,
                    worterbuch.clone(),
                    tx.clone(),
                    &mut subscriptions,
                )
                .await?
                {
                    break;
                }
            }
        }
    }

    let mut wb = worterbuch.write().await;
    for subs in subscriptions {
        wb.unsubscribe(&subs.0, subs.1);
    }

    Ok(())
}
