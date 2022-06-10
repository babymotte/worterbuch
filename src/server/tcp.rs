use crate::{config::Config, worterbuch::Worterbuch};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    spawn,
    sync::RwLock,
};
use worterbuch::{
    codec::{
        encode_ack_message, encode_state_message, read_message, Ack, Get, Set, State, Subscribe,
    },
    error::{DecodeError, EncodeError},
};

pub async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    log::info!("Starting TCP Server...");

    let bind_addr = config.bind_addr;
    let port = config.port;

    let server = TcpListener::bind((bind_addr, port)).await?;

    loop {
        let conn = server.accept().await?;
        log::debug!("Client connected from {}", conn.1);
        spawn(serve(conn.0, worterbuch.clone()));
    }
}

async fn serve(mut client: TcpStream, worterbuch: Arc<RwLock<Worterbuch>>) -> Result<()> {
    loop {
        match read_message(&mut client).await {
            Ok(worterbuch::codec::Message::Get(msg)) => {
                get(msg, worterbuch.clone(), &mut client).await?
            }
            Ok(worterbuch::codec::Message::Set(msg)) => {
                set(msg, worterbuch.clone(), &mut client).await?
            }
            Ok(worterbuch::codec::Message::Subscribe(msg)) => {
                subscribe(msg, worterbuch.clone(), &mut client).await
            }
            Err(e) => {
                log::error!("error decoding message: {e}");
                if let DecodeError::IoError(_) = e {
                    break;
                }
                // TODO send special ERR message
            }
            _ => { /* ignore server messages */ }
        }
    }

    Ok(())
}

async fn get(msg: Get, worterbuch: Arc<RwLock<Worterbuch>>, client: &mut TcpStream) -> Result<()> {
    let wb = worterbuch.read().await;

    let values = match wb.get_all(&msg.request_pattern) {
        Ok(values) => values,
        Err(e) => {
            handle_store_error(e, client).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        key_value_pairs: values,
    };

    match encode_state_message(&response) {
        Ok(data) => client.write_all(&data).await?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn set(msg: Set, worterbuch: Arc<RwLock<Worterbuch>>, client: &mut TcpStream) -> Result<()> {
    let mut wb = worterbuch.write().await;

    if let Err(e) = wb.set(msg.key, msg.value) {
        handle_store_error(e, client).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    match encode_ack_message(&response) {
        Ok(data) => client.write_all(&data).await?,
        Err(e) => handle_encode_error(e, client).await?,
    }

    Ok(())
}

async fn subscribe(msg: Subscribe, worterbuch: Arc<RwLock<Worterbuch>>, client: &mut TcpStream) {
    todo!()
}

async fn handle_encode_error(e: EncodeError, client: &mut TcpStream) -> Result<()> {
    todo!()
}

async fn handle_store_error(e: anyhow::Error, client: &mut TcpStream) -> Result<()> {
    todo!()
}
