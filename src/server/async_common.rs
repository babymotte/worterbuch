use crate::worterbuch::Worterbuch;
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    spawn,
    sync::{mpsc::UnboundedSender, RwLock},
};
use uuid::Uuid;
use worterbuch::{
    codec::{
        encode_ack_message, encode_pstate_message, encode_state_message, read_message, Ack, PGet,
        PState, PSubscribe, Set, State,
    },
    error::{DecodeError, EncodeError},
};

pub async fn process_incoming_message(
    msg: impl AsyncReadExt + Unpin,
    worterbuch: Arc<RwLock<Worterbuch>>,
    tx: UnboundedSender<Vec<u8>>,
    subscriptions: &mut Vec<(String, Uuid)>,
) -> Result<bool> {
    match read_message(msg).await {
        Ok(Some(worterbuch::codec::Message::PGet(msg))) => {
            get(msg, worterbuch.clone(), tx.clone()).await?;
        }
        Ok(Some(worterbuch::codec::Message::Set(msg))) => {
            set(msg, worterbuch.clone(), tx.clone()).await?;
        }
        Ok(Some(worterbuch::codec::Message::PSubscribe(msg))) => {
            if let Some(subs) = subscribe(msg, worterbuch.clone(), tx.clone()).await? {
                subscriptions.push(subs);
            }
        }
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
        _ => { /* ignore server messages */ }
    }

    Ok(true)
}

async fn get(
    msg: PGet,
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

    let response = PState {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        key_value_pairs: values,
    };

    match encode_pstate_message(&response) {
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
    msg: PSubscribe,
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
            let event = State {
                transaction_id: transaction_id.clone(),
                key,
                value,
            };
            match encode_state_message(&event) {
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
