use super::common::process_incoming_message;
use crate::{config::Config, server::common::Subscriptions, worterbuch::Worterbuch};
use anyhow::Result;
use libworterbuch::{
    codec::{encode_handshake_message, Handshake, ProtocolVersion},
    error::{WorterbuchError, WorterbuchResult},
};
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

pub async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    log::info!("Starting TCP Server …");

    let bind_addr = config.bind_addr;
    let port = config.tcp_port;

    let server = TcpListener::bind((bind_addr, port)).await?;

    loop {
        let conn = server.accept().await?;
        log::debug!("Client connected from {}", conn.1);
        spawn(serve(conn.0, worterbuch.clone(), conn.1, config.clone()));
    }
}

async fn serve(
    client: TcpStream,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
    config: Config,
) -> WorterbuchResult<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_read, mut client_write) = client.into_split();

    spawn(async move {
        let supported_protocol_versions = vec![ProtocolVersion { major: 0, minor: 1 }];
        let separator = config.separator;
        let wildcard = config.wildcard;
        let multi_wildcard = config.multi_wildcard;
        let handshake = Handshake {
            supported_protocol_versions,
            separator,
            wildcard,
            multi_wildcard,
        };
        let handshake = match encode_handshake_message(&handshake) {
            Ok(it) => it,
            Err(e) => {
                log::error!("Error encoding handshake message: {e}");
                return;
            }
        };
        if let Err(e) = client_write.write_all(&handshake).await {
            log::error!("Error sending handshake message to client: {e}");
            return;
        }
        while let Some(bytes) = rx.recv().await {
            if let Err(e) = client_write.write_all(&bytes).await {
                log::error!("Error sending message to client: {e}");
                break;
            }
        }
    });

    let mut subscriptions = Subscriptions::new();
    let client_id = Uuid::new_v4();

    log::debug!("Receiving messages from client {remote_addr} …");
    loop {
        if !process_incoming_message(
            client_id.clone(),
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
    log::debug!("No more messages from {remote_addr}, closing connection.");

    let mut wb = worterbuch.write().await;
    for (subscription, pattern) in subscriptions {
        match wb.unsubscribe(&pattern, &subscription) {
            Ok(()) => {}
            Err(WorterbuchError::NotSubscribed) => {
                log::warn!("Inconsistent subscription state: tracked subscription {subscription:?} is not present on server.");
            }
            Err(e) => {
                log::warn!("Error while unsubscribing: {e}");
            }
        }
    }

    Ok(())
}
