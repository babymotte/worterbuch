use super::config::Config;
use crate::Connection;
use std::{future::Future, io};
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    spawn,
    sync::{broadcast, mpsc},
};
use worterbuch_common::{
    encode_message,
    error::{ConnectionError, ConnectionResult},
    nonblocking::read_server_message,
    ClientMessage as CM, Handshake, ServerMessage as SM,
};

pub async fn connect_with_default_config<F: Future<Output = ()> + Send + 'static>(
    on_disconnect: F,
) -> ConnectionResult<Connection> {
    let config = Config::new_tcp()?;
    connect(&config.proto, &config.host_addr, config.port, on_disconnect).await
}

pub async fn connect<F: Future<Output = ()> + Send + 'static>(
    _proto: &str,
    host_addr: &str,
    port: u16,
    on_disconnect: F,
) -> ConnectionResult<Connection> {
    let server = TcpStream::connect(format!("{host_addr}:{port}")).await?;
    let (mut tcp_rx, tcp_tx) = server.into_split();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = broadcast::channel(1_000);

    match read_server_message(&mut tcp_rx).await? {
        Some(SM::Handshake(handshake)) => connected(
            tcp_tx,
            tcp_rx,
            cmd_tx,
            cmd_rx,
            result_tx,
            result_rx,
            on_disconnect,
            handshake,
        ),
        Some(other) => Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("server sendt invalid handshake message: {other:?}"),
        ))),
        None => Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "connection closed before handshake",
        ))),
    }
}

fn connected<F: Future<Output = ()> + Send + 'static>(
    mut tcp_tx: OwnedWriteHalf,
    mut tcp_rx: OwnedReadHalf,
    cmd_tx: mpsc::UnboundedSender<CM>,
    mut cmd_rx: mpsc::UnboundedReceiver<CM>,
    result_tx: broadcast::Sender<SM>,
    result_rx: broadcast::Receiver<SM>,
    on_disconnect: F,
    handshake: Handshake,
) -> ConnectionResult<Connection> {
    let result_tx_recv = result_tx.clone();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            match encode_message(&msg) {
                Ok(data) => {
                    if let Err(e) = tcp_tx.write_all(&data).await {
                        log::error!("failed to send tcp message: {e}");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("error encoding message: {e}");
                }
            }
        }
        // make sure initial rx is not dropped as long as commands is read
        drop(result_rx);
    });

    spawn(async move {
        loop {
            match read_server_message(&mut tcp_rx).await {
                Ok(Some(msg)) => {
                    if let Err(e) = result_tx_recv.send(msg) {
                        log::error!("Error forwarding server message: {e}");
                    }
                }
                Ok(None) => {
                    log::error!("Connection to server lost.");
                    on_disconnect.await;
                    break;
                }
                Err(e) => {
                    log::error!("Error decoding message: {e}");
                }
            }
        }
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
