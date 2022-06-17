use crate::Connection;
use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    spawn,
    sync::mpsc::{self, UnboundedSender},
};
use worterbuch::{
    codec::{
        encode_get_message, encode_pget_message, encode_psubscribe_message, encode_set_message,
        encode_subscribe_message, read_message, Get, Message, PGet, PSubscribe, Set, Subscribe,
    },
    config::Config,
};

#[derive(Clone)]
pub struct TcpConnection {
    cmd_tx: UnboundedSender<Message>,
    counter: u64,
}

#[async_trait]
impl Connection for TcpConnection {
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
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn pget(&mut self, key: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.send(Message::PGet(PGet {
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
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn psubscribe(&mut self, request_pattern: &str) -> Result<u64> {
        let i = self.counter;
        self.counter += 1;
        self.cmd_tx.send(Message::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern: request_pattern.to_owned(),
        }))?;
        Ok(i)
    }
}

pub async fn connect() -> Result<TcpConnection> {
    let config = Config::new()?;

    let server = TcpStream::connect(format!("{}:{}", config.bind_addr, config.tcp_port)).await?;
    let (mut tcp_rx, mut tcp_tx) = server.into_split();

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if let Ok(Some(data)) = encode_message(&msg) {
                if let Err(e) = tcp_tx.write_all(&data).await {
                    eprintln!("failed to send tcp message: {e}");
                    break;
                }
            }
        }
    });

    spawn(async move {
        loop {
            match read_message(&mut tcp_rx).await {
                Ok(Some(worterbuch::codec::Message::PState(msg))) => {
                    for (key, value) in msg.key_value_pairs {
                        println!("{key} = {value}");
                    }
                }
                Ok(Some(worterbuch::codec::Message::State(msg))) => {
                    if let Some((key, value)) = msg.key_value {
                        println!("{} = {}", key, value);
                    } else {
                        println!("No result.");
                    }
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
    });

    let con = TcpConnection { cmd_tx, counter: 1 };

    Ok(con)
}

fn encode_message(msg: &Message) -> Result<Option<Vec<u8>>> {
    match msg {
        Message::Get(msg) => Ok(Some(encode_get_message(msg)?)),
        Message::PGet(msg) => Ok(Some(encode_pget_message(msg)?)),
        Message::Set(msg) => Ok(Some(encode_set_message(msg)?)),
        Message::Subscribe(msg) => Ok(Some(encode_subscribe_message(msg)?)),
        Message::PSubscribe(msg) => Ok(Some(encode_psubscribe_message(msg)?)),
        _ => Ok(None),
    }
}
