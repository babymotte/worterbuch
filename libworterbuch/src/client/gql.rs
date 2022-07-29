use crate::{
    client::Connection,
    codec::{
        ClientMessage as CM, Export, Get, Import, PGet, PSubscribe, ServerMessage as SM, Set,
        Subscribe,
    },
    error::ConnectionResult,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{
    spawn,
    sync::{
        broadcast::{self},
        mpsc,
    },
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::Message},
};
use std::future::Future;

const INIT_MSG_TEMPLATE: &str = r#"{"type":"connection_init","payload":{}}"#;
const GET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"query {get(key: \"$key\") {key value}}","variables":null}}"#;
const PGET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"query {pget(pattern: \"$pattern\") {pattern key value}}","variables":null}}"#;
const SET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {set(key: \"$key\" value: \"$val\")}","variables":null}}"#;
const SUBSCRIBE_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"subscription {subscribe(key: \"$key\") {key value}}","variables":null}}"#;
const PSUBSCRIBE_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"subscription {psubscribe(pattern: \"$pattern\") {pattern key value}}","variables":null}}"#;
const IMPORT_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {import(path: \"$path\")}","variables":null}}"#;
const EXPORT_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {export(path: \"$path\")}","variables":null}}"#;

pub async fn connect<F: Future<Output = ()> + Send + 'static>(
    proto: &str,
    host_addr: &str,
    port: u16,
    on_disconnect: F,
) -> ConnectionResult<Connection> {
    let url = url::Url::parse(&format!("{proto}://{host_addr}:{port}/ws"))?;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = broadcast::channel(1_000);
    let result_tx_recv = result_tx.clone();

    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, read) = ws_stream.split();

    spawn(async move {
        let init = Message::Text(INIT_MSG_TEMPLATE.to_owned());
        if let Err(e) = write.send(init).await {
            log::error!("Error sending WS message: {e}");
            return;
        }
        while let Some(cmd) = cmd_rx.recv().await {
            let msg = encode_ws_message(cmd);
            if let Err(e) = write.send(msg).await {
                log::error!("Error sending WS message: {e}");
                return;
            }
        }

        // make sure initial rx is not dropped as long as stdin is read
        drop(result_rx);
    });

    spawn(async move {
        let mut messages = read.map(decode_ws_message);
        while let Some(msg) = messages.next().await {
            match msg {
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

    Ok(Connection::new(cmd_tx, result_tx))
}

fn encode_ws_message(cmd: CM) -> Message {
    let txt = match cmd {
        CM::Get(Get {
            transaction_id,
            key,
        }) => GET_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$key", &key)
            .to_owned(),
        CM::PGet(PGet {
            transaction_id,
            request_pattern,
        }) => PGET_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$pattern", &request_pattern)
            .to_owned(),
        CM::Set(Set {
            transaction_id,
            key,
            value,
        }) => SET_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$key", &key)
            .replace("$val", &value)
            .to_owned(),
        CM::Subscribe(Subscribe {
            transaction_id,
            key,
        }) => SUBSCRIBE_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$key", &key)
            .to_owned(),
        CM::PSubscribe(PSubscribe {
            transaction_id,
            request_pattern,
        }) => PSUBSCRIBE_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$pattern", &request_pattern)
            .to_owned(),
        CM::Import(Import {
            transaction_id,
            path,
        }) => IMPORT_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$path", &path)
            .to_owned(),
        CM::Export(Export {
            transaction_id,
            path,
        }) => EXPORT_MSG_TEMPLATE
            .replace("$i", &transaction_id.to_string())
            .replace("$path", &path)
            .to_owned(),
    };

    Message::Text(txt)
}

fn decode_ws_message(message: tungstenite::Result<Message>) -> ConnectionResult<Option<SM>> {
    let msg = message?;

    match msg {
        // TODO decode json and extract relevant values
        Message::Text(json) => {
            let _json: Value = serde_json::from_str(&json)?;
            todo!();
        }
        _ => Ok(None),
    }
}
