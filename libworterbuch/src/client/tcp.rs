use crate::{
    client::Connection,
    codec::{encode_message, read_server_message},
    error::ConnectionResult,
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    spawn,
    sync::{broadcast, mpsc},
};

pub async fn connect(_proto: &str, host_addr: &str, port: u16) -> ConnectionResult<Connection> {
    let server = TcpStream::connect(format!("{host_addr}:{port}")).await?;
    let (mut tcp_rx, mut tcp_tx) = server.into_split();

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = broadcast::channel(1_000);
    let result_tx_recv = result_tx.clone();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            match encode_message(&msg) {
                Ok(data) => {
                    if let Err(e) = tcp_tx.write_all(&data).await {
                        eprintln!("failed to send tcp message: {e}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("error encoding message: {e}");
                }
            }
        }
        // make sure initial rx is not dropped as long as stdin is read
        drop(result_rx);
    });

    spawn(async move {
        loop {
            match read_server_message(&mut tcp_rx).await {
                Ok(Some(msg)) => {
                    if let Err(e) = result_tx_recv.send(msg) {
                        eprintln!("Error forwarding server message: {e}");
                    }
                }
                Ok(None) => {
                    eprintln!("Connection to server lost.");
                    break;
                }
                Err(e) => {
                    eprintln!("Error decoding message: {e}");
                }
            }
        }
    });

    Ok(Connection::new(cmd_tx, result_tx))
}
