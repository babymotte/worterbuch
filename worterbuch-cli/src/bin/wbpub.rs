use anyhow::Result;
use clap::Parser;
use serde_json::json;
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
use worterbuch_cli::print_message;
use worterbuch_client::config::Config;
use worterbuch_client::connect;
use worterbuch_client::KeyValuePair;

#[derive(Parser)]
#[command(author, version, about = "Publish values on a Wörterbuch.", long_about = None)]
struct Args {
    /// Connect to the Wörterbuch server using SSL encryption.
    #[arg(short, long)]
    ssl: bool,
    /// The address of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_HOST_ADDRESS will be used. If that is not set, 127.0.0.1 will be used.
    #[arg(short, long)]
    addr: Option<String>,
    /// The port of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_PORT will be used. If that is not set, 4242 will be used.
    #[arg(short, long)]
    port: Option<u16>,
    /// Output data in JSON and expect input data to be JSON.
    #[arg(short, long)]
    json: bool,
    /// Key/value pairs to be published on Wörterbuch in the form "KEY1=VALUE1 KEY2=VALUE2 KEY3=VALUE3 ...". When omitted, key/value pairs will be read from stdin. When reading key/value pairs from stdin, one key/value pair is expected per line.
    key_value_pairs: Option<Vec<String>>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let config = Config::new()?;
    let args: Args = Args::parse();

    let proto = if args.ssl {
        "wss".to_owned()
    } else {
        "ws".to_owned()
    };
    let host_addr = args.addr.unwrap_or(config.host_addr);
    let port = args.port.unwrap_or(config.port);
    let json = args.json;
    let key_value_pairs = args.key_value_pairs;

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let (mut con, mut responses) =
        connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    spawn(async move {
        while let Some(msg) = responses.recv().await {
            let tid = msg.transaction_id();
            let mut acked = acked_recv.lock().expect("mutex is poisoned");
            if tid > *acked {
                *acked = tid;
            }
            print_message(&msg, json);
        }
    });

    if json {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<KeyValuePair>(&line) {
                Ok(KeyValuePair { key, value }) => {
                    trans_id = con.publish_value(key, json!(value)).await?;
                }
                Err(e) => {
                    eprintln!("Error parsing json: {e}");
                }
            }
        }
    } else {
        if let Some(key_calue_pairs) = key_value_pairs {
            for key_calue_pair in key_calue_pairs {
                if let Some(index) = key_calue_pair.find('=') {
                    let key = &key_calue_pair[..index];
                    let val = &key_calue_pair[index + 1..];
                    trans_id = con.publish_value(key.to_owned(), json!(val)).await?;
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
                    trans_id = con.publish_value(key.to_owned(), json!(val)).await?;
                } else {
                    eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
                }
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
