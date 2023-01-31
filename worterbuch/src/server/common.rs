use crate::worterbuch::Worterbuch;
use serde::Serialize;
use std::sync::Arc;
use tokio::{
    fs::File,
    spawn,
    sync::{mpsc::UnboundedSender, RwLock},
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    Ack, ClientMessage as CM, Err, ErrorCode, Export, Get, HandshakeRequest, Import, KeyValuePair,
    MetaData, PGet, PState, PSubscribe, ServerMessage, Set, State, Subscribe, Unsubscribe,
};

pub async fn process_incoming_message(
    client_id: Uuid,
    msg: &str,
    worterbuch: Arc<RwLock<Worterbuch>>,
    tx: UnboundedSender<String>,
) -> WorterbuchResult<bool> {
    match serde_json::from_str(msg) {
        Ok(Some(CM::HandshakeRequest(msg))) => {
            handshake(msg, worterbuch.clone(), tx.clone(), client_id.clone()).await?;
        }
        Ok(Some(CM::Get(msg))) => {
            get(msg, worterbuch.clone(), tx.clone()).await?;
        }
        Ok(Some(CM::PGet(msg))) => {
            pget(msg, worterbuch.clone(), tx.clone()).await?;
        }
        Ok(Some(CM::Set(msg))) => {
            set(msg, worterbuch.clone(), tx.clone()).await?;
        }
        Ok(Some(CM::Subscribe(msg))) => {
            let unique = msg.unique;
            subscribe(msg, client_id, worterbuch.clone(), tx.clone(), unique).await?;
        }
        Ok(Some(CM::PSubscribe(msg))) => {
            let unique = msg.unique;
            psubscribe(msg, client_id, worterbuch.clone(), tx.clone(), unique).await?;
        }
        Ok(Some(CM::Export(msg))) => export(msg, worterbuch.clone(), tx.clone()).await?,
        Ok(Some(CM::Import(msg))) => import(msg, worterbuch.clone(), tx.clone()).await?,
        Ok(Some(CM::Unsubscribe(msg))) => {
            unsubscribe(msg, worterbuch.clone(), tx.clone(), client_id).await?
        }
        Ok(None) => {
            // client disconnected
            return Ok(false);
        }
        Err(e) => {
            log::error!("Error decoding message: {e}");
            return Ok(false);
        }
    }

    Ok(true)
}

async fn handshake(
    msg: HandshakeRequest,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    let mut wb = worterbuch.write().await;

    let response = match wb.handshake(
        &msg.supported_protocol_versions,
        msg.last_will,
        msg.grave_goods,
        client_id,
    ) {
        Ok(handshake) => handshake,
        Err(e) => {
            handle_store_error(e, client.clone(), 0).await?;
            return Ok(());
        }
    };

    match serde_json::to_string(&ServerMessage::Handshake(response)) {
        Ok(data) => client
            .send(data)
            .context(|| format!("Error sending HANDSHAKE message",))?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn get(
    msg: Get,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    let wb = worterbuch.read().await;

    let key_value = match wb.get(&msg.key) {
        Ok(key_value) => key_value.into(),
        Err(e) => {
            handle_store_error(e, client.clone(), msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        key_value,
    };

    match serde_json::to_string(&ServerMessage::State(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending STATE message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn pget(
    msg: PGet,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    let wb = worterbuch.read().await;

    let values = match wb.pget(&msg.request_pattern) {
        Ok(values) => values.into_iter().map(KeyValuePair::from).collect(),
        Err(e) => {
            handle_store_error(e, client.clone(), msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = PState {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        key_value_pairs: values,
    };

    match serde_json::to_string(&ServerMessage::PState(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending PSTATE message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn set(
    msg: Set,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    let mut wb = worterbuch.write().await;

    if let Err(e) = wb.set(msg.key, msg.value) {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match serde_json::to_string(&ServerMessage::Ack(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn subscribe(
    msg: Subscribe,
    client_id: Uuid,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
    unique: bool,
) -> WorterbuchResult<bool> {
    let wb_unsub = worterbuch.clone();
    let mut wb = worterbuch.write().await;

    let (mut rx, subscription) =
        match wb.subscribe(client_id, msg.transaction_id, msg.key.clone(), unique) {
            Ok(it) => it,
            Err(e) => {
                handle_store_error(e, client, msg.transaction_id).await?;
                return Ok(false);
            }
        };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match serde_json::to_string(&ServerMessage::Ack(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client.clone()).await?,
    }

    let transaction_id = msg.transaction_id;

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription:?} …");
        while let Some(kvs) = rx.recv().await {
            for key_value in kvs {
                let event = State {
                    transaction_id: transaction_id.clone(),
                    key_value,
                };
                match serde_json::to_string(&ServerMessage::State(event)) {
                    Ok(data) => {
                        if let Err(e) = client.clone().send(data) {
                            log::error!("Error sending STATE message to client: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        if let Err(e) = handle_encode_error(e, client.clone()).await {
                            log::error!("Error sending ERROR message to client: {e}");
                            break;
                        }
                    }
                }
            }
        }

        let mut wb = wb_unsub.write().await;
        log::debug!("No more events, ending subscription {subscription:?}.");
        match wb.unsubscribe(client_id, transaction_id) {
            Ok(()) => {
                log::warn!("Subscription {subscription:?} was not cleaned up properly!");
            }
            Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
            Err(e) => {
                log::warn!("Error while unsubscribing: {e}");
            }
        }
    });

    Ok(true)
}

async fn psubscribe(
    msg: PSubscribe,
    client_id: Uuid,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
    unique: bool,
) -> WorterbuchResult<bool> {
    let wb_unsub = worterbuch.clone();
    let mut wb = worterbuch.write().await;

    let (mut rx, subscription) = match wb.psubscribe(
        client_id,
        msg.transaction_id,
        msg.request_pattern.clone(),
        unique,
    ) {
        Ok(rx) => rx,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(false);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match serde_json::to_string(&ServerMessage::Ack(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client.clone()).await?,
    }

    let transaction_id = msg.transaction_id;
    let request_pattern = msg.request_pattern;

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription:?} …");
        while let Some(key_value_pairs) = rx.recv().await {
            let event = PState {
                transaction_id: transaction_id.clone(),
                request_pattern: request_pattern.clone(),
                key_value_pairs,
            };
            match serde_json::to_string(&ServerMessage::PState(event)) {
                Ok(data) => {
                    if let Err(e) = client.clone().send(data) {
                        log::error!("Error sending STATE message to client: {e}");
                        break;
                    }
                }
                Err(e) => {
                    if let Err(e) = handle_encode_error(e, client.clone()).await {
                        log::error!("Error sending ERROR message to client: {e}");
                        break;
                    }
                }
            }
        }

        let mut wb = wb_unsub.write().await;
        log::debug!("No more events, ending subscription {subscription:?}.");
        match wb.unsubscribe(client_id, transaction_id) {
            Ok(()) => {
                log::warn!("Subscription {subscription:?} was not cleaned up properly!");
            }
            Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
            Err(e) => {
                log::warn!("Error while unsubscribing: {e}");
            }
        }
    });

    Ok(true)
}

async fn export(
    msg: Export,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    log::info!("export");
    let wb = worterbuch.read().await;
    let mut file = File::create(&msg.path)
        .await
        .context(|| format!("Error creating file {}", &msg.path))?;
    match wb.export_to_file(&mut file).await {
        Ok(_) => {
            let response = Ack {
                transaction_id: msg.transaction_id,
            };

            match serde_json::to_string(&ServerMessage::Ack(response)) {
                Ok(data) => client.send(data).context(|| {
                    format!(
                        "Error sending ACK message for transaction ID {}",
                        msg.transaction_id
                    )
                })?,
                Err(e) => handle_encode_error(e, client.clone()).await?,
            }
        }
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    }

    Ok(())
}

async fn import(
    msg: Import,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    log::info!("import");
    let mut wb = worterbuch.write().await;

    match wb.import_from_file(&msg.path).await {
        Ok(()) => {
            let response = Ack {
                transaction_id: msg.transaction_id,
            };

            match serde_json::to_string(&ServerMessage::Ack(response)) {
                Ok(data) => client.send(data).context(|| {
                    format!(
                        "Error sending ACK message for transaction ID {}",
                        msg.transaction_id
                    )
                })?,
                Err(e) => handle_encode_error(e, client.clone()).await?,
            }
        }
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    }

    Ok(())
}

async fn unsubscribe(
    msg: Unsubscribe,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<String>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    let mut wb = worterbuch.write().await;

    wb.unsubscribe(client_id, msg.transaction_id)?;
    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match serde_json::to_string(&ServerMessage::Ack(response)) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client.clone()).await?,
    }

    Ok(())
}

async fn handle_encode_error(
    e: serde_json::Error,
    client: UnboundedSender<String>,
) -> WorterbuchResult<()> {
    drop(client);
    panic!("Failed to encode a value to JSON: {e}");
}

async fn handle_store_error(
    e: WorterbuchError,
    client: UnboundedSender<String>,
    transaction_id: u64,
) -> WorterbuchResult<()> {
    let error_code = ErrorCode::from(&e);
    let err_msg = match e {
        WorterbuchError::IllegalWildcard(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::IllegalMultiWildcard(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::MultiWildcardAtIllegalPosition(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::NoSuchValue(key) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!("no value for key '{key}'"))
                .expect("failed to serialize error message"),
        },
        WorterbuchError::NotSubscribed => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!(
                "no subscription found for transaction id '{transaction_id}'"
            ))
            .expect("failed to serialize error message"),
        },
        WorterbuchError::IoError(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e.into(), meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::SerDeError(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e.into(), meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::ProtocolNegotiationFailed => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(
                "server does not implement any of the protocl versions supported by this client",
            )
            .expect("failed to serialize metadata"),
        },
        WorterbuchError::Other(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e, meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::ServerResponse(_) => panic!("store must not produce this error"),
    };
    let msg = serde_json::to_string(&err_msg)
        .expect(&format!("failed to encode error message: {err_msg:?}"));
    client
        .send(msg)
        .context(|| format!("Error sending ERR message to client"))
}

#[derive(Serialize)]
struct Meta {
    cause: String,
    meta: MetaData,
}

impl From<(&Box<dyn std::error::Error + Send + Sync>, MetaData)> for Meta {
    fn from(e: (&Box<dyn std::error::Error + Send + Sync>, MetaData)) -> Self {
        Meta {
            cause: e.0.to_string(),
            meta: e.1,
        }
    }
}
