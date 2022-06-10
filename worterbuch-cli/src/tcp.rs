use crate::Connection;
use anyhow::Result;
use async_trait::async_trait;
use std::{env, sync::Arc, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    spawn,
    sync::{
        mpsc::{self, UnboundedSender},
        RwLock,
    },
};
use worterbuch::codec::{
    encode_get_message, encode_set_message, encode_subscribe_message, read_message, Get, Message,
    Set, Subscribe,
};

#[derive(Clone)]
pub struct TcpConnection {
    cmd_tx: UnboundedSender<Message>,
    counter: u64,
    latest_ticket: Arc<RwLock<u64>>,
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

    async fn wait_for_ticket(&self, ticket: u64) {
        eprintln!("waiting for ticket {ticket} ...");
        // TODO do this in a more elegant fashion
        let current = *self.latest_ticket.read().await;
        while ticket > current {
            eprintln!("{current}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub async fn connect() -> Result<TcpConnection> {
    let addr = env::var("WORTERBUCH_ADDR").unwrap_or("127.0.0.1".to_owned());
    let port = env::var("WORTERBUCH_PORT").unwrap_or("4242".to_owned());

    let server = TcpStream::connect(format!("{addr}:{port}")).await?;
    let (mut tcp_rx, mut tcp_tx) = server.into_split();

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if let Ok(Some(data)) = encode_message(msg) {
                if let Err(e) = tcp_tx.write_all(&data).await {
                    eprintln!("failed to send tcp message: {e}");
                    break;
                }
            }
        }
    });

    let (ticket_tx, mut ticket_rx) = tokio::sync::mpsc::unbounded_channel();

    spawn(async move {
        loop {
            match read_message(&mut tcp_rx).await {
                Ok(worterbuch::codec::Message::State(msg)) => {
                    for (key, value) in msg.key_value_pairs {
                        println!("{key} = {value}");
                    }
                    if let Err(e) = ticket_tx.send(msg.transaction_id) {
                        eprintln!("error sending ticket: {e}");
                        break;
                    }
                }
                Ok(worterbuch::codec::Message::Ack(msg)) => {
                    if let Err(e) = ticket_tx.send(msg.transaction_id) {
                        eprintln!("error sending ticket: {e}");
                        break;
                    }
                }
                Ok(worterbuch::codec::Message::Event(msg)) => {
                    println!("{} = {}", msg.key, msg.value);
                }
                Ok(worterbuch::codec::Message::Err(msg)) => {
                    eprintln!("server error {}: {}", msg.error_code, msg.metadata);
                    if let Err(e) = ticket_tx.send(msg.transaction_id) {
                        eprintln!("error sending ticket: {e}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("error decoding message: {e}");
                }
                _ => { /* ignore client messages */ }
            }
        }
    });

    let con = TcpConnection {
        cmd_tx,
        counter: 1,
        latest_ticket: Arc::default(),
    };

    let latest_ticket = con.latest_ticket.clone();
    spawn(async move {
        while let Some(ticket) = ticket_rx.recv().await {
            *latest_ticket.write().await = ticket;
        }
    });

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
