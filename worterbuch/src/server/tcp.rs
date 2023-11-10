use crate::server::common::{process_incoming_message, CloneableWbApi, Protocol};
use anyhow::anyhow;
use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    select, spawn,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{tcp::write_line_and_flush, ProtocolVersion, ServerMessage};

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: IpAddr,
    port: u16,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let addr = format!("{bind_addr}:{port}");

    let proto_version = worterbuch
        .supported_protocol_versions()
        .await
        .unwrap_or(Vec::new())
        .into_iter()
        .last()
        .ok_or_else(|| anyhow!("could not get protocol version"))?;

    log::info!("Starting TCP endpoint at {addr}");
    let listener = TcpListener::bind(&addr).await?;

    loop {
        select! {
            con = listener.accept() => {
                let (socket, remote_addr) = con?;
                let worterbuch = worterbuch.clone();
                let proto_version = proto_version.clone();
                spawn(async move {
                    if let Err(e) = serve(remote_addr, worterbuch, socket, proto_version).await {
                        log::error!("Connection to client {remote_addr} closed with error: {e}");
                    }
                });
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
    proto_version: ProtocolVersion,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    worterbuch
        .connected(client_id, remote_addr, Protocol::TCP)
        .await?;

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

    if let Err(e) = serve_loop(
        client_id,
        remote_addr,
        worterbuch.clone(),
        socket,
        proto_version,
    )
    .await
    {
        log::error!("Error in serve loop: {e}");
    }

    worterbuch.disconnected(client_id, remote_addr).await?;

    Ok(())
}

type TcpSender = OwnedWriteHalf;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
    proto_version: ProtocolVersion,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut handshake_complete = false;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (tcp_rx, mut tcp_tx) = socket.into_split();
    let (tcp_send_tx, mut tcp_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::unbounded_channel();

    // websocket send loop
    spawn(async move {
        while let Some(msg) = tcp_send_rx.recv().await {
            send_with_timeout(msg, &mut tcp_tx, send_timeout, &keepalive_tx_tx).await;
        }
    });

    let tcp_rx = BufReader::new(tcp_rx);
    let mut tcp_rx = tcp_rx.lines();

    loop {
        select! {
            recv = tcp_rx.next_line() => match recv {
                Ok(Some(json)) => {
                    last_keepalive_rx = Instant::now();
                    let res = process_incoming_message(
                        client_id,
                        &json,
                        &worterbuch,
                        &tcp_send_tx,
                        &proto_version,
                    ).await;
                    let (msg_processed, handshake) = res?;
                    handshake_complete |= msg_processed && handshake;
                    if !msg_processed {
                        break;
                    }
                },
                Ok(None) =>  break,
                Err(e) => {
                    log::info!("TCP stream of client {client_id} ({remote_addr}) closed with error:, {e}");
                    break;
                }
            } ,
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => last_keepalive_tx = keepalive?,
                None => break,
            },
            _ = keepalive_timer.tick() => {
                // check how long ago the last websocket message was received
                check_client_keepalive(last_keepalive_rx, last_keepalive_tx, handshake_complete, client_id, keepalive_timeout)?;
                // send out websocket message if the last has been more than a second ago
                send_keepalive(last_keepalive_tx, &tcp_send_tx, ).await?;
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

async fn send_with_timeout<'a>(
    msg: ServerMessage,
    tcp: &mut TcpSender,
    send_timeout: Duration,
    result_handler: &mpsc::UnboundedSender<anyhow::Result<Instant>>,
) {
    select! {
        r = write_line_and_flush(&msg, tcp)  => {
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
