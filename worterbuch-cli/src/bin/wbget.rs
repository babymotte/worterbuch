use anyhow::Result;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
#[cfg(feature = "graphql")]
use worterbuch_cli::gql::GqlConnection;
#[cfg(feature = "tcp")]
use worterbuch_cli::tcp::TcpConnection;
#[cfg(feature = "ws")]
use worterbuch_cli::ws::WsConnection;
use worterbuch_cli::Connection;

#[cfg(feature = "tcp")]
async fn connect() -> Result<TcpConnection> {
    worterbuch_cli::tcp::connect().await
}

#[cfg(feature = "ws")]
async fn connect() -> Result<WsConnection> {
    worterbuch_cli::ws::connect().await
}

#[cfg(feature = "graphql")]
async fn connect() -> Result<GqlConnection> {
    worterbuch_cli::gql::connect().await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let mut con = connect().await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    let mut acks = con.acks();

    spawn(async move {
        while let Ok(tid) = acks.recv().await {
            let mut acked = acked_recv.lock().expect("mutex is poisoned");
            if tid > *acked {
                *acked = tid;
            }
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(key)) = lines.next_line().await {
        trans_id = con.get(&key)?;
    }

    loop {
        let acked = *acked.lock().expect("mutex is poisoned");
        if acked < trans_id {
            sleep(Duration::from_millis(100)).await;
        } else {
            break;
        }
    }

    Ok(())
}
