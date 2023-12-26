use crate::server::common::{process_incoming_message, CloneableWbApi, Protocol};
use anyhow::anyhow;
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use poem::web::websocket::{Message, WebSocketStream};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    select, spawn,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::ServerMessage;

pub(crate) async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    worterbuch
        .connected(client_id, remote_addr, Protocol::WS)
        .await?;

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

    if let Err(e) = serve_loop(
        client_id,
        remote_addr,
        worterbuch.clone(),
        websocket,
        subsys,
    )
    .await
    {
        log::error!("Error in serve loop: {e}");
    }

    worterbuch.disconnected(client_id, remote_addr).await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocketStream, poem::web::websocket::Message>;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let auth_token = config.auth_token;
    let handshake_required = auth_token.is_some();
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut handshake_complete = false;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (mut ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, mut ws_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::unbounded_channel();

    // websocket send loop
    let subsys_send = subsys.clone();
    spawn(async move {
        while let Some(msg) = ws_send_rx.recv().await {
            send_with_timeout(
                msg,
                &mut ws_tx,
                send_timeout,
                &keepalive_tx_tx,
                &subsys_send,
            )
            .await;
        }
    });

    loop {
        select! {
            recv = ws_rx.next() => if let Some(msg) = recv {
                match msg {
                    Ok(incoming_msg) => {
                        last_keepalive_rx = Instant::now();
                        if let Message::Text(text) = incoming_msg {
                            let (msg_processed, handshake) = process_incoming_message(
                                client_id,
                                &text,
                                &worterbuch,
                                &ws_send_tx,
                                handshake_required,
                                handshake_complete
                            )
                            .await?;
                            handshake_complete |= msg_processed && handshake;
                            if !msg_processed {
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("Error in WebSocket connection: {e}");
                        break;
                    }
                }
            } else {
                log::info!("WS stream of client {client_id} ({remote_addr}) closed.");
                break;
            },
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => last_keepalive_tx = keepalive?,
                None => break,
            },
            _ = keepalive_timer.tick() => {
                // check how long ago the last websocket message was received
                check_client_keepalive(last_keepalive_rx, last_keepalive_tx, handshake_complete, client_id, keepalive_timeout)?;
                // send out websocket message if the last has been more than a second ago
                send_keepalive(last_keepalive_tx, &ws_send_tx, ).await?;
            }
        }
    }

    Ok(())
}

async fn send_keepalive(
    last_keepalive_tx: Instant,
    ws_send_tx: &mpsc::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    if last_keepalive_tx.elapsed().as_secs() >= 1 {
        log::trace!("Sending keepalive");
        ws_send_tx.send(ServerMessage::Keepalive).await?;
    }
    Ok(())
}

fn check_client_keepalive(
    last_keepalive_rx: Instant,
    last_keepalive_tx: Instant,
    handshake_complete: bool,
    client_id: Uuid,
    keepalive_timeout: Duration,
) -> anyhow::Result<()> {
    let lag = last_keepalive_tx - last_keepalive_rx;

    if handshake_complete && lag >= Duration::from_secs(2) {
        log::warn!(
            "Client {} has been inactive for {} seconds …",
            client_id,
            lag.as_secs()
        );
    }

    if handshake_complete && lag >= keepalive_timeout {
        log::warn!(
            "Client {} has been inactive for too long. Disconnecting.",
            client_id
        );
        Err(anyhow!("Client has been inactive for too long"))
    } else {
        Ok(())
    }
}

async fn send_with_timeout(
    msg: ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Duration,
    result_handler: &mpsc::UnboundedSender<anyhow::Result<Instant>>,
    subsys: &SubsystemHandle,
) {
    let json = match serde_json::to_string(&msg) {
        Ok(it) => it,
        Err(e) => {
            handle_encode_error(e, subsys);
            return;
        }
    };

    let msg = Message::Text(json);

    select! {
        r = websocket.send(msg) => {
            if let Err(e) = r {
                result_handler.send(Err(e.into())).ok();
            } else {
                result_handler.send(Ok(Instant::now())).ok();
            }
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout");
            result_handler.send(Err(anyhow!("Send timeout"))).ok();
        },
    }
}

fn handle_encode_error(e: serde_json::Error, subsys: &SubsystemHandle) {
    log::error!("Failed to encode a value to JSON: {e}");
    subsys.request_global_shutdown();
}
