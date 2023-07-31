use anyhow::Result;
use clap::Parser;
use std::{process, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::print_message;
use worterbuch_client::{config::Config, connect};

#[derive(Parser)]
#[command(author, version, about = "Subscribe to values of Wörterbuch keys.", long_about = None)]
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
    /// Wörterbuch keys to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line.
    keys: Option<Vec<String>>,
    /// Only receive unique values, i.e. skip notifications when a key is set to a value it already has.
    #[arg(short, long)]
    unique: Option<bool>,
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
    let keys = args.keys;
    let unique = args.unique.unwrap_or(false);

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            print_message(&msg, json);
        }
    });

    if let Some(keys) = keys {
        for key in keys {
            if unique {
                con.subscribe_unique_async(key.to_owned())?;
            } else {
                con.subscribe_async(key.to_owned())?;
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(key)) = lines.next_line().await {
            if unique {
                con.subscribe_unique_async(key)?;
            } else {
                con.subscribe_async(key)?;
            }
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
