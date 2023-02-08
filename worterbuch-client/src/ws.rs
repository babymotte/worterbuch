use crate::{config::Config, Connection};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use std::{future::Future, io};
use tokio::{net::TcpStream, spawn, sync::broadcast, sync::mpsc};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use worterbuch_common::{
    error::{ConnectionError, ConnectionResult},
    ClientMessage as CM, GraveGoods, Handshake, HandshakeRequest, LastWill, ProtocolVersion,
    ServerMessage as SM,
};

pub async fn connect_with_default_config<F: Future<Output = ()> + Send + 'static>(
    last_will: LastWill,
    grave_goods: GraveGoods,
    on_disconnect: F,
) -> ConnectionResult<(Connection, Config)> {
    let config = Config::new()?;
    Ok((
        connect(
            &config.proto,
            &config.host_addr,
            config.port,
            last_will,
            grave_goods,
            on_disconnect,
        )
        .await?,
        config,
    ))
}

pub async fn connect<F: Future<Output = ()> + Send + 'static>(
    proto: &str,
    host_addr: &str,
    port: u16,
    last_will: LastWill,
    grave_goods: GraveGoods,
    on_disconnect: F,
) -> ConnectionResult<Connection> {
    let url = format!("{proto}://{host_addr}:{port}/ws");
    log::debug!("Connecting to server {url} …");
    let (server, _) = connect_async(url).await?;
    log::debug!("Connected to server.");
    let (mut ws_tx, mut ws_rx) = server.split();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = broadcast::channel(1_000);

    // TODO implement protocol versions properly
    let supported_protocol_versions = vec![ProtocolVersion { major: 0, minor: 5 }];

    let handshake = HandshakeRequest {
        supported_protocol_versions,
        last_will,
        grave_goods,
    };
    let msg = serde_json::to_string(&CM::HandshakeRequest(handshake))?;
    ws_tx.send(Message::Text(msg)).await?;

    match ws_rx.next().await {
        Some(Ok(msg)) => match msg.to_text() {
            Ok(data) => match serde_json::from_str::<SM>(data) {
                Ok(SM::Handshake(handshake)) => connected(
                    ws_tx,
                    ws_rx,
                    cmd_tx,
                    cmd_rx,
                    result_tx,
                    result_rx,
                    on_disconnect,
                    handshake,
                ),
                Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {msg:?}"),
                ))),
                Err(e) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {e}"),
                ))),
            },
            Err(e) => Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("server sent invalid handshake message: {e}"),
            ))),
        },
        Some(Err(e)) => Err(e.into()),
        None => Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "connection closed before handshake",
        ))),
    }
}

fn connected<F: Future<Output = ()> + Send + 'static>(
    mut ws_tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    mut ws_rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    cmd_tx: mpsc::UnboundedSender<CM>,
    mut cmd_rx: mpsc::UnboundedReceiver<CM>,
    result_tx: broadcast::Sender<SM>,
    result_rx: broadcast::Receiver<SM>,
    on_disconnect: F,
    handshake: Handshake,
) -> Result<Connection, ConnectionError> {
    let result_tx_recv = result_tx.clone();
    log::debug!("Handhsake complete: {handshake}");

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if let Ok(Some(data)) = serde_json::to_string(&msg).map(Some) {
                log::debug!("Sending {data}");
                let msg = tungstenite::Message::Text(data);
                if let Err(e) = ws_tx.send(msg).await {
                    log::error!("failed to send ws message: {e}");
                    break;
                }
            } else {
                break;
            }
        }
        // make sure initial rx is not dropped as long as stdin is read
        drop(result_rx);
    });

    spawn(async move {
        loop {
            if let Some(Ok(incoming_msg)) = ws_rx.next().await {
                if incoming_msg.is_text() {
                    if let Ok(data) = incoming_msg.to_text() {
                        log::debug!("Received {data}");
                        match serde_json::from_str(data) {
                            Ok(msg) => {
                                if let Err(e) = result_tx_recv.send(msg) {
                                    log::error!("Error forwarding server message: {e}");
                                }
                            }
                            Err(e) => {
                                log::error!("Error decoding message: {e}");
                            }
                        }
                    }
                }
            } else {
                log::error!("Connection to server lost.");
                on_disconnect.await;
                break;
            }
        }

        log::info!("WS closed.")
    });

    let separator = handshake.separator;
    let wildcard = handshake.wildcard;
    let multi_wildcard = handshake.multi_wildcard;

    Ok(Connection::new(
        cmd_tx,
        result_tx,
        separator,
        wildcard,
        multi_wildcard,
    ))
}
