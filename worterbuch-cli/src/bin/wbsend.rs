use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp;
#[cfg(feature = "ws")]
use libworterbuch::client::ws;
use libworterbuch::client::Connection;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::app;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, _json) = app(
        "wbsend",
        "Send a stream of values to a single Wörterbuch key. Values are read from stdin, one value is expected per line.",
        false,
        vec![Arg::with_name("KEY")
            .multiple(false)
            .help("Wörterbuch key to send values to.")
            .takes_value(true)
            .required(true)],
    )?;

    let key = matches.get_one::<String>("KEY").expect("key is required");

    #[cfg(feature = "tcp")]
    let mut con = tcp::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "ws")]
    let mut con = ws::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "graphql")]
    let mut con = gql::connect(&proto, &host_addr, port).await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            let tid = msg.transaction_id();
            let mut acked = acked_recv.lock().expect("mutex is poisoned");
            if tid > *acked {
                *acked = tid;
            }
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(value)) = lines.next_line().await {
        trans_id = con.set(&key, &value)?;
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
