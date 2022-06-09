use crate::{Command, Connection};
use anyhow::Result;
use async_trait::async_trait;
use futures_channel::mpsc::{self, Sender};
use futures_util::StreamExt;
use serde_json::Value;
use std::env;
use tokio::spawn;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::Message},
};

const INIT_MSG_TEMPLATE: &str = r#"{"type":"connection_init","payload":{}}"#;
const GET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"query {get(pattern: \"$key\") {pattern key value}}","variables":null}}"#;
const SET_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"mutation {set(key: \"$key\" value: \"$val\")}","variables":null}}"#;
const SUBSCRIBE_MSG_TEMPLATE: &str = r#"{"id":"$i","type":"start","payload":{"query":"subscription {all(pattern: \"$key\") {pattern key value}}","variables":null}}"#;

#[derive(Clone)]
pub struct GqlConnection {
    cmd_tx: Sender<Command>,
    counter: u64,
}

#[async_trait]
impl Connection for GqlConnection {
    async fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .try_send(Command::Set(key.to_owned(), value.to_owned(), i))?;
        Ok(())
    }

    async fn get(&mut self, key: &str) -> Result<()> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.try_send(Command::Get(key.to_owned(), i))?;
        Ok(())
    }

    async fn subscribe(&mut self, key: &str) -> Result<()> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx
            .try_send(Command::Subscrube(key.to_owned(), i))?;
        Ok(())
    }
}

pub async fn connect() -> Result<GqlConnection> {
    let proto = env::var("WORTERBUCH_PROTO").unwrap_or("ws".to_owned());
    let addr = env::var("WORTERBUCH_ADDR").unwrap_or("127.0.0.1".to_owned());
    let port = env::var("WORTERBUCH_PORT").unwrap_or("4242".to_owned());

    let url = url::Url::parse(&format!("{proto}://{addr}:{port}/ws"))?;

    let (mut cmd_tx, cmd_rx) = mpsc::channel(1_000);

    let (ws_stream, _) = connect_async(url).await?;
    let (write, read) = ws_stream.split();

    spawn(cmd_rx.map(encode_ws_message).forward(write));
    spawn(async {
        let mut messages = read.map(decode_ws_message);
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(Some(msg)) => println!("{msg}"),
                Err(e) => eprintln!("error decoding message: {e}"),
                _ => {}
            }
        }
    });

    cmd_tx.try_send(Command::Init)?;

    Ok(GqlConnection { cmd_tx, counter: 0 })
}

fn encode_ws_message(cmd: Command) -> tungstenite::Result<Message> {
    let txt = match cmd {
        Command::Init => INIT_MSG_TEMPLATE.to_owned(),
        Command::Get(k, i) => GET_MSG_TEMPLATE
            .replace("$i", &i.to_string())
            .replace("$key", &k)
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
    };

    Ok(Message::Text(txt))
}

fn decode_ws_message(message: tungstenite::Result<Message>) -> Result<Option<String>> {
    let msg = message?;

    match msg {
        // TODO decode json and extract relevant values
        Message::Text(json) => {
            let json: Value = serde_json::from_str(&json)?;
            if let Some(payload) = json.get("payload") {
                if let Some(data) = payload.get("data") {
                    if let Some(_) = data.get("set") {
                        Ok(None)
                    } else if let Some(get) = data.get("get") {
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
                    } else if let Some(subscribe) = data.get("subscribe") {
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
