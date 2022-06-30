use crate::worterbuch::Worterbuch;
use libworterbuch::{
    codec::{
        encode_ack_message, encode_err_message, encode_pstate_message, encode_state_message,
        read_client_message, Ack, ClientMessage as CM, Err, ErrorCode, Export, Get, Import,
        KeyValuePair, MetaData, PGet, PState, PSubscribe, Set, State, Subscribe,
    },
    error::{Context, DecodeError, EncodeError, WorterbuchError, WorterbuchResult},
};
use serde::Serialize;
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    spawn,
    sync::{mpsc::UnboundedSender, RwLock},
};
use uuid::Uuid;

pub async fn process_incoming_message(
    msg: impl AsyncReadExt + Unpin,
    worterbuch: Arc<RwLock<Worterbuch>>,
    tx: UnboundedSender<Vec<u8>>,
    subscriptions: &mut Vec<(String, Uuid)>,
) -> WorterbuchResult<bool> {
    match read_client_message(msg).await {
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
            if let Some(subs) = subscribe(msg, worterbuch.clone(), tx.clone()).await? {
                subscriptions.push(subs);
            }
        }
        Ok(Some(CM::PSubscribe(msg))) => {
            if let Some(subs) = psubscribe(msg, worterbuch.clone(), tx.clone()).await? {
                subscriptions.push(subs);
            }
        }
        Ok(Some(CM::Export(msg))) => export(msg, worterbuch.clone(), tx.clone()).await?,
        Ok(Some(CM::Import(msg))) => import(msg, worterbuch.clone(), tx.clone()).await?,
        Ok(None) => {
            // client disconnected
            return Ok(false);
        }
        Err(e) => {
            log::error!("error decoding message: {e}");
            if let DecodeError::IoError(_) = e {
                return Ok(false);
            }
            // TODO send special ERR message
        }
    }

    Ok(true)
}

async fn get(
    msg: Get,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
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

    match encode_state_message(&response) {
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
    client: UnboundedSender<Vec<u8>>,
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

    match encode_pstate_message(&response) {
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
    client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<()> {
    let mut wb = worterbuch.write().await;

    if let Err(e) = wb.set(msg.key, msg.value) {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
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
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<Option<(String, Uuid)>> {
    let wb_unsub = worterbuch.clone();
    let mut wb = worterbuch.write().await;

    let (mut rx, subscription) = match wb.subscribe(msg.key.clone()) {
        Ok(rx) => rx,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(None);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
        Ok(data) => client.send(data).context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?,
        Err(e) => handle_encode_error(e, client.clone()).await?,
    }

    let transaction_id = msg.transaction_id;
    let key = msg.key;
    let key_recv = key.clone();

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription} …");
        while let Some(kvs) = rx.recv().await {
            for key_value in kvs {
                let event = State {
                    transaction_id: transaction_id.clone(),
                    key_value,
                };
                match encode_state_message(&event) {
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
        log::debug!("No more events, ending subscription {subscription}.");
        wb.unsubscribe(&key_recv, subscription);
    });

    Ok(Some((key, subscription)))
}

async fn psubscribe(
    msg: PSubscribe,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<Option<(String, Uuid)>> {
    let wb_unsub = worterbuch.clone();
    let mut wb = worterbuch.write().await;

    let (mut rx, subscription) = match wb.psubscribe(msg.request_pattern.clone()) {
        Ok(rx) => rx,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(None);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
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
    let request_pattern_recv = request_pattern.clone();
    let request_pattern_out = request_pattern.clone();

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription} …");
        while let Some(key_value_pairs) = rx.recv().await {
            let event = PState {
                transaction_id: transaction_id.clone(),
                request_pattern: request_pattern.clone(),
                key_value_pairs,
            };
            match encode_pstate_message(&event) {
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
        log::debug!("No more events, ending subscription {subscription}.");
        wb.unsubscribe(&request_pattern_recv, subscription);
    });

    Ok(Some((request_pattern_out, subscription)))
}

async fn export(
    msg: Export,
    worterbuch: Arc<RwLock<Worterbuch>>,
    client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<()> {
    log::info!("export");
    let wb = worterbuch.read().await;
    match wb.export_to_file(&msg.path).await {
        Ok(()) => {
            let response = Ack {
                transaction_id: msg.transaction_id,
            };

            match encode_ack_message(&response) {
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
    client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<()> {
    log::info!("import");
    let mut wb = worterbuch.write().await;

    match wb.import_from_file(&msg.path).await {
        Ok(()) => {
            let response = Ack {
                transaction_id: msg.transaction_id,
            };

            match encode_ack_message(&response) {
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

async fn handle_encode_error(
    _e: EncodeError,
    _client: UnboundedSender<Vec<u8>>,
) -> WorterbuchResult<()> {
    todo!()
}

async fn handle_store_error(
    e: WorterbuchError,
    client: UnboundedSender<Vec<u8>>,
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
        WorterbuchError::Other(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e, meta).into())
                .expect("failed to serialize metadata"),
        },
    };
    let msg = encode_err_message(&err_msg)
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
