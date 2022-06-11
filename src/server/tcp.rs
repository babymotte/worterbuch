use crate::worterbuch::Worterbuch;
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    spawn,
    sync::{
        mpsc::{self, UnboundedSender},
        RwLock,
    },
};
use uuid::Uuid;
use worterbuch::{
    codec::{
        encode_ack_message, encode_event_message, encode_state_message, read_message, Ack, Event,
        Get, Set, State, Subscribe,
    },
    config::Config,
    error::{DecodeError, EncodeError},
};

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
        match read_message(&mut client_read).await {
            Ok(Some(worterbuch::codec::Message::Get(msg))) => {
                get(msg, worterbuch.clone(), tx.clone()).await?;
            }
            Ok(Some(worterbuch::codec::Message::Set(msg))) => {
                set(msg, worterbuch.clone(), tx.clone()).await?;
            }
            Ok(Some(worterbuch::codec::Message::Subscribe(msg))) => {
                if let Some(subs) = subscribe(msg, worterbuch.clone(), tx.clone()).await? {
                    subscriptions.push(subs);
                }
            }
            Ok(None) => {
                // client disconnected
                break;
            }
            Err(e) => {
                log::error!("error decoding message: {e}");
                if let DecodeError::IoError(_) = e {
                    break;
                }
                // TODO send special ERR message
            }
            _ => { /* ignore server messages */ }
        }
    }

    let mut wb = worterbuch.write().await;
    for subs in subscriptions {
        wb.unsubscribe(&subs.0, subs.1);
    }

    Ok(())
}

async fn get(
    msg: Get,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> Result<()> {
    let wb = worterbuch.read().await;

    let values = match wb.get_all(&msg.request_pattern) {
        Ok(values) => values,
        Err(e) => {
            handle_store_error(e, client.clone()).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        key_value_pairs: values,
    };

    match encode_state_message(&response) {
        Ok(data) => client.send(data)?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn set(
    msg: Set,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> Result<()> {
    let mut wb = worterbuch.write().await;

    if let Err(e) = wb.set(msg.key, msg.value) {
        handle_store_error(e, client).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
        Ok(data) => client.send(data)?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn subscribe(
    msg: Subscribe,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> Result<Option<(String, Uuid)>> {
    let wb_unsub = worterbuch.clone();
    let mut wb = worterbuch.write().await;

    let (mut rx, subscription) = match wb.subscribe(msg.request_pattern.clone()) {
        Ok(rx) => rx,
        Err(e) => {
            handle_store_error(e, client).await?;
            return Ok(None);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
        Ok(data) => client.send(data)?,
        Err(e) => handle_encode_error(e, client.clone()).await?,
    }

    let transaction_id = msg.transaction_id;
    let request_pattern = msg.request_pattern;
    let request_pattern_recv = request_pattern.clone();

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription} …");
        while let Some((key, value)) = rx.recv().await {
            let event = Event {
                transaction_id: transaction_id.clone(),
                request_pattern: request_pattern_recv.clone(),
                key,
                value,
            };
            match encode_event_message(&event) {
                Ok(data) => {
                    if let Err(e) = client.clone().send(data) {
                        log::error!("Error sending message to client: {e}");
                        break;
                    }
                }
                Err(e) => {
                    if let Err(e) = handle_encode_error(e, client.clone()).await {
                        log::error!("Error sending message to client: {e}");
                        break;
                    }
                }
            }
        }

        let mut wb = wb_unsub.write().await;
        log::debug!("No more events, ending subscription {subscription}.");
        wb.unsubscribe(&request_pattern_recv, subscription);
    });

    Ok(Some((request_pattern, subscription)))
}

async fn handle_encode_error(_e: EncodeError, _client: UnboundedSender<Vec<u8>>) -> Result<()> {
    todo!()
}

async fn handle_store_error(_e: anyhow::Error, _client: UnboundedSender<Vec<u8>>) -> Result<()> {
    todo!()
}
