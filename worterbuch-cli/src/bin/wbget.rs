use anyhow::Result;
use clap::Arg;
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
#[cfg(feature = "tcp")]
use worterbuch_client::tcp as wb;
#[cfg(feature = "ws")]
use worterbuch_client::ws as wb;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json, debug) = app(
        "wbget",
        "Get values for keys from a Wörterbuch.",
        vec![Arg::with_name("KEYS")
            .multiple(true)
            .help(
                r#"Keys to be fetched from Wörterbuch in the form "KEY1 KEY2 KEY3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line."#,
            )
            .takes_value(true)
            .required(false)],
    )?;

    let keys = matches.get_many::<String>("KEYS");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = wb::connect(&proto, &host_addr, port, on_disconnect).await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            let tid = msg.transaction_id();
            {
                let mut acked = acked_recv.lock().expect("mutex is poisoned");
                if tid > *acked {
                    *acked = tid;
                }
            }
            print_message(&msg, json, debug);
        }
    });

    if let Some(keys) = keys {
        for key in keys {
            trans_id = con.get(key)?;
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(key)) = lines.next_line().await {
            trans_id = con.get(&key)?;
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
