use crate::{client::Connection, codec::ServerMessage as SM, error::ConnectionResult};
use futures_channel::mpsc::{self, UnboundedSender};
use futures_util::StreamExt;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::{
    spawn,
    sync::broadcast::{self},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::Message},
};

const INIT_MSG_TEMPLATE: &str = r#"{"type":"connection_init","payload":{}}"#;
const GET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"query {get(key: \"$key\") {key value}}","variables":null}}"#;
const PGET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"query {pget(pattern: \"$pattern\") {pattern key value}}","variables":null}}"#;
const SET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {set(key: \"$key\" value: \"$val\")}","variables":null}}"#;
const SUBSCRIBE_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"subscription {subscribe(key: \"$key\") {key value}}","variables":null}}"#;
const PSUBSCRIBE_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"subscription {psubscribe(pattern: \"$pattern\") {pattern key value}}","variables":null}}"#;
const IMPORT_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {import(path: \"$path\")}","variables":null}}"#;
const EXPORT_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {export(path: \"$path\")}","variables":null}}"#;

pub struct GqlConnection {
    cmd_tx: UnboundedSender<Command>,
    result_tx: broadcast::Sender<SM>,
    counter: Arc<Mutex<u64>>,
}

impl GqlConnection {
    fn inc_counter(&mut self) -> u64 {
        let mut counter = self.counter.lock().expect("poisoned counter mutex");
        let i = *counter;
        *counter += 1;
        i
    }
}

impl Connection for GqlConnection {
    fn get(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::Get(key.to_owned(), i))?;
        Ok(i)
    }

    fn pget(&mut self, pattern: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::PGet(pattern.to_owned(), i))?;
        Ok(i)
    }

    fn set(&mut self, key: &str, value: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::Set(key.to_owned(), value.to_owned(), i))?;
        Ok(i)
    }

    fn subscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::Subscrube(key.to_owned(), i))?;
        Ok(i)
    }

    fn psubscribe(&mut self, pattern: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::PSubscrube(pattern.to_owned(), i))?;
        Ok(i)
    }

    fn export(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::Export(path.to_owned(), i))?;
        Ok(i)
    }

    fn import(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx
            .unbounded_send(Command::Import(path.to_owned(), i))?;
        Ok(i)
    }

    fn responses(&mut self) -> broadcast::Receiver<SM> {
        self.result_tx.subscribe()
    }
}

pub async fn connect(proto: &str, addr: &str, port: u16) -> ConnectionResult<GqlConnection> {
    let url = url::Url::parse(&format!("{proto}://{addr}:{port}/ws"))?;

    let (cmd_tx, cmd_rx) = mpsc::unbounded();
    let (result_tx, result_rx) = broadcast::channel(1_000);
    let result_tx_recv = result_tx.clone();

    let (ws_stream, _) = connect_async(url).await?;
    let (write, read) = ws_stream.split();

    spawn(async {
        if let Err(e) = cmd_rx.map(encode_ws_message).forward(write).await {
            eprintln!("Error forwarding commands: {e}");
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

    cmd_tx.unbounded_send(Command::Init)?;

    let con = GqlConnection {
        cmd_tx,
        counter: Arc::default(),
        result_tx,
    };

    Ok(con)
}

fn encode_ws_message(cmd: Command) -> tungstenite::Result<Message> {
    let txt = match cmd {
        Command::Init => INIT_MSG_TEMPLATE.to_owned(),
        Command::Get(k, i) => GET_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$key", &k)
            .to_owned(),
        Command::PGet(p, i) => PGET_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$pattern", &p)
            .to_owned(),
        Command::Set(k, v, i) => SET_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$key", &k)
            .replace("$val", &v)
            .to_owned(),
        Command::Subscrube(k, i) => SUBSCRIBE_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$key", &k)
            .to_owned(),
        Command::PSubscrube(p, i) => PSUBSCRIBE_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$pattern", &p)
            .to_owned(),
        Command::Import(p, i) => IMPORT_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$path", &p)
            .to_owned(),
        Command::Export(p, i) => EXPORT_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$path", &p)
            .to_owned(),
    };

    Ok(Message::Text(txt))
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

enum Command {
    Init,
    Get(String, u64),
    PGet(String, u64),
    Set(String, String, u64),
    Subscrube(String, u64),
    PSubscrube(String, u64),
    Import(String, u64),
    Export(String, u64),
}
