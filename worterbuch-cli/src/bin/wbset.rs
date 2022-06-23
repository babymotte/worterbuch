use anyhow::Result;
use clap::Arg;
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
use worterbuch_cli::{utils::app, Connection};

#[cfg(feature = "tcp")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<TcpConnection> {
    worterbuch_cli::tcp::connect(proto, host, port).await
}

#[cfg(feature = "ws")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<WsConnection> {
    worterbuch_cli::ws::connect(proto, host, port).await
}

#[cfg(feature = "graphql")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<GqlConnection> {
    worterbuch_cli::gql::connect(proto, host, port).await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, _json) = app(
        "wbset",
        "Set values of keys on a Wörterbuch.",
        false,
        vec![Arg::with_name("KEY_VALUE_PAIRS")
            .multiple(true)
            .help(
                r#"Key/value pairs to be set on Wörterbuch in the form "KEY1=VALUE1 KEY2=VALUE2 KEY3=VALUE3 ...". When omitted, key/value pairs will be read from stdin. When reading key/value pairs from stdin, one key/value pair is expected per line."#,
            )
            .takes_value(true)
            .required(false)],
    )?;

    let key_value_pairs = matches.get_many::<String>("KEY_VALUE_PAIRS");

    let mut con = connect(&proto, &host_addr, port).await?;

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

    if let Some(key_calue_pairs) = key_value_pairs {
        for key_calue_pair in key_calue_pairs {
            if let Some(index) = key_calue_pair.find('=') {
                let key = &key_calue_pair[..index];
                let val = &key_calue_pair[index + 1..];
                trans_id = con.set(key, val)?;
            } else {
                eprintln!("no key/value pair (e.g. 'a=b'): {}", key_calue_pair);
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(index) = line.find('=') {
                let key = &line[..index];
                let val = &line[index + 1..];
                trans_id = con.set(key, val)?;
            } else {
                eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
            }
        }
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
