use crate::Connection;
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    spawn,
    sync::mpsc::{self, UnboundedSender},
};
use tokio_tungstenite::{connect_async, tungstenite};
use worterbuch::{
    codec::{
        encode_get_message, encode_set_message, encode_subscribe_message, read_message, Get,
        Message, Set, Subscribe,
    },
    config::Config,
};

#[derive(Clone)]
pub struct WsConnection {
    cmd_tx: UnboundedSender<Message>,
    counter: u64,
}

#[async_trait]
impl Connection for WsConnection {
    fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.send(Message::Set(Set {
            transaction_id: i,
            key: key.to_owned(),
            value: value.to_owned(),
        }))?;
        Ok(i)
    }

    fn get(&mut self, key: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.send(Message::Get(Get {
            transaction_id: i,
            request_pattern: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn subscribe(&mut self, key: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.send(Message::Subscribe(Subscribe {
            transaction_id: i,
            request_pattern: key.to_owned(),
        }))?;
        Ok(i)
    }
}

pub async fn connect() -> Result<WsConnection> {
    let config = Config::new()?;

    let proto = config.proto.clone();
    let addr = config.bind_addr;
    let port = config.web_port;

    let (server, _) = connect_async(format!("{proto}://{addr}:{port}")).await?;
    let (mut ws_tx, mut ws_rx) = server.split();

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if let Ok(Some(data)) = encode_message(msg) {
                let msg = tungstenite::Message::Binary(data);
                if let Err(e) = ws_tx.send(msg).await {
                    eprintln!("failed to send tcp message: {e}");
                    break;
                }
            }else {
                break;
            }
        }
    });

    spawn(async move {
        loop {
            if let Some(Ok(incoming_msg)) = ws_rx.next().await {
                if incoming_msg.is_binary() {
                    let data = incoming_msg.into_data();
                    match read_message(&*data).await {
                        Ok(Some(worterbuch::codec::Message::State(msg))) => {
                            for (key, value) in msg.key_value_pairs {
                                println!("{key} = {value}");
                            }
                        }
                        Ok(Some(worterbuch::codec::Message::Event(msg))) => {
                            println!("{} = {}", msg.key, msg.value);
                        }
                        Ok(Some(worterbuch::codec::Message::Err(msg))) => {
                            eprintln!("server error {}: {}", msg.error_code, msg.metadata);
                        }
                        Ok(None) => {
                            eprintln!("Connection to server lost.");
                            break;
                        }
                        Err(e) => {
                            eprintln!("error decoding message: {e}");
                        }
                        _ => { /* ignore client messages */ }
                    }
                }
            }
        }
    });

    let con = WsConnection { cmd_tx, counter: 1 };

    Ok(con)
}

fn encode_message(msg: Message) -> Result<Option<Vec<u8>>> {
    match msg {
        Message::Get(msg) => Ok(Some(encode_get_message(&msg)?)),
        Message::Set(msg) => Ok(Some(encode_set_message(&msg)?)),
        Message::Subscribe(msg) => Ok(Some(encode_subscribe_message(&msg)?)),
        _ => Ok(None),
    }
}
