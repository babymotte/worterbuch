use crate::worterbuch::Worterbuch;
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    spawn,
    sync::{
        mpsc::{self},
        RwLock,
    },
};
use worterbuch::config::Config;

use super::async_common::process_incoming_message;

pub async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    log::info!("Starting TCP Server …");

    let bind_addr = config.bind_addr;
    let port = config.tcp_port;

    let server = TcpListener::bind((bind_addr, port)).await?;

    loop {
        let conn = server.accept().await?;
        log::debug!("Client connected from {}", conn.1);
        spawn(serve(conn.0, worterbuch.clone()));
    }
}

async fn serve(client: TcpStream, worterbuch: Arc<RwLock<Worterbuch>>) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_read, mut client_write) = client.into_split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if let Err(e) = client_write.write_all(&bytes).await {
                log::error!("Error sending message to client: {e}");
                break;
            }
        }
    });

    let mut subscriptions = Vec::new();

    loop {
        if !process_incoming_message(
            &mut client_read,
            worterbuch.clone(),
            tx.clone(),
            &mut subscriptions,
        )
        .await?
        {
            break;
        }
    }

    let mut wb = worterbuch.write().await;
    for subs in subscriptions {
        wb.unsubscribe(&subs.0, subs.1);
    }

    Ok(())
}
