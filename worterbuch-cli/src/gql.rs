use crate::Connection;
use anyhow::Result;
use futures_channel::mpsc::{self, UnboundedSender};
use futures_util::StreamExt;
use serde_json::Value;
use std::env;
use tokio::{
    spawn,
    sync::broadcast::{self, Sender},
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
    counter: u64,
    ack_tx: broadcast::Sender<u64>,
}

impl Connection for GqlConnection {
    fn get(&mut self, key: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::Get(key.to_owned(), i))?;
        Ok(i)
    }

    fn pget(&mut self, pattern: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::PGet(pattern.to_owned(), i))?;
        Ok(i)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::Set(key.to_owned(), value.to_owned(), i))?;
        Ok(i)
    }

    fn subscribe(&mut self, key: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::Subscrube(key.to_owned(), i))?;
        Ok(i)
    }

    fn psubscribe(&mut self, pattern: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::PSubscrube(pattern.to_owned(), i))?;
        Ok(i)
    }

    fn export(&mut self, path: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::Export(path.to_owned(), i))?;
        Ok(i)
    }

    fn import(&mut self, path: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .unbounded_send(Command::Import(path.to_owned(), i))?;
        Ok(i)
    }

    fn acks(&mut self) -> tokio::sync::broadcast::Receiver<u64> {
        self.ack_tx.subscribe()
    }
}

pub async fn connect() -> Result<GqlConnection> {
    let proto = env::var("WORTERBUCH_PROTO").unwrap_or("ws".to_owned());
    let addr = env::var("WORTERBUCH_ADDR").unwrap_or("127.0.0.1".to_owned());
    let port = env::var("WORTERBUCH_GRAPHQL_PORT").unwrap_or("4243".to_owned());

    let url = url::Url::parse(&format!("{proto}://{addr}:{port}/ws"))?;

    let (cmd_tx, cmd_rx) = mpsc::unbounded();
    let (ack_tx, ack_rx) = broadcast::channel(1_000);
    let ack_tx_rcv = ack_tx.clone();

    let (ws_stream, _) = connect_async(url).await?;
    let (write, read) = ws_stream.split();

    spawn(async {
        if let Err(e) = cmd_rx.map(encode_ws_message).forward(write).await {
            eprintln!("Error forwarding commands: {e}");
        }
        // make sure initial rx is not dropped as long as stdin is read
        drop(ack_rx);
    });

    spawn(async move {
        let mut messages = read.map(|msg| decode_ws_message(msg, ack_tx_rcv.clone()));
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(Some(msg)) => println!("{msg}"),
                Err(e) => eprintln!("error decoding message: {e}"),
                _ => {}
            }
        }
    });

    cmd_tx.unbounded_send(Command::Init)?;

    let con = GqlConnection {
        cmd_tx,
        counter: 0,
        ack_tx,
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

fn decode_ws_message(
    message: tungstenite::Result<Message>,
    ack_tx: Sender<u64>,
) -> Result<Option<String>> {
    let msg = message?;

    match msg {
        // TODO decode json and extract relevant values
        Message::Text(json) => {
            let json: Value = serde_json::from_str(&json)?;

            if let Some(tp) = json.get("type") {
                if tp.as_str().expect("type must be a string") == "complete" {
                    let id: u64 = json
                        .get("id")
                        .expect("id must be present")
                        .as_str()
                        .expect("id must be a string in json")
                        .parse()
                        .expect("id must represent a u64");
                    if let Err(e) = ack_tx.send(id) {
                        eprintln!("Error forwarding ack: {e}");
                    }
                }
            }

            if let Some(payload) = json.get("payload") {
                if let Some(data) = payload.get("data") {
                    if let Some(_) = data.get("set") {
                        Ok(None)
                    } else if let Some(get) = data.get("get") {
                        Ok(Some(format!(
                            "{} = {}",
                            get.get("key")
                                .expect("key must be present")
                                .as_str()
                                .expect("key must be a string"),
                            get.get("value")
                                .expect("value must be present")
                                .as_str()
                                .expect("value must be a string")
                        )))
                    } else if let Some(get) = data.get("pget") {
                        let results = get.as_array().expect("get must return an array");
                        if results.is_empty() {
                            Ok(None)
                        } else {
                            let values: Vec<String> = results
                                .iter()
                                .map(|res| {
                                    format!(
                                        "{} = {}",
                                        res.get("key")
                                            .expect("key must be present")
                                            .as_str()
                                            .expect("key must be a string"),
                                        res.get("value")
                                            .expect("value must be present")
                                            .as_str()
                                            .expect("value must be a string")
                                    )
                                })
                                .collect();

                            Ok(Some(values.join("\n")))
                        }
                    } else if let Some(_subscribe) = data.get("subscribe") {
                        println!("{json}");
                        todo!()
                    } else if let Some(_subscribe) = data.get("psubscribe") {
                        println!("{json}");
                        todo!()
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
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
