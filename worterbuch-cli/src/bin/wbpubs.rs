use anyhow::Result;
use clap::Arg;
use serde_json::{json, Value};
use std::{
    process,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_message};
use worterbuch_client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (matches, proto, host_addr, port, json, ) = app(
        "wbpubs",
        "Publish a stream of values to a single Wörterbuch key. Values are read from stdin, one value is expected per line.",
        vec![Arg::with_name("KEY")
            .multiple(false)
            .help("Wörterbuch key to publish values to.")
            .takes_value(true)
            .required(true)],
    )?;

    let key = matches.get_one::<String>("KEY").expect("key is required");

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

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
            print_message(&msg, json);
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(value)) = lines.next_line().await {
        if json {
            match serde_json::from_str::<Value>(&value) {
                Ok(value) => trans_id = con.publish(key.to_owned(), value)?,
                Err(e) => {
                    log::error!("Invalid input '{value}': {e}");
                }
            }
        } else {
            trans_id = con.publish(key.to_owned(), json!(value))?;
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