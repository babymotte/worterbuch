use super::common::process_incoming_message;
use crate::{config::Config, worterbuch::Worterbuch};
use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    spawn,
    sync::{
        mpsc::{self},
        RwLock,
    },
};
use uuid::Uuid;
use worterbuch_common::error::WorterbuchResult;

pub async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    log::info!("Starting TCP Server …");

    let bind_addr = config.bind_addr;
    let port = config.tcp_port;

    let server = TcpListener::bind((bind_addr, port)).await?;

    loop {
        let conn = server.accept().await?;
        spawn(serve(conn.0, worterbuch.clone(), conn.1));
    }
}

async fn serve(
    client: TcpStream,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
) -> WorterbuchResult<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_read, mut client_write) = client.into_split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if let Err(e) = client_write.write_all(&bytes).await {
                log::error!("Error sending message to client {client_id} ({remote_addr}): {e}");
                break;
            }
        }
    });

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …");
    loop {
        if !process_incoming_message(
            client_id.clone(),
            &mut client_read,
            worterbuch.clone(),
            tx.clone(),
        )
        .await?
        {
            break;
        }
    }

    log::info!("TCP stream of client {client_id} ({remote_addr}) closed.");

    let mut wb = worterbuch.write().await;
    wb.disconnected(client_id, remote_addr);

    Ok(())
}
