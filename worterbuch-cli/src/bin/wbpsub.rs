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
#[command(author, version, about = "Subscribe to values matching Wörterbuch patterns.", long_about = None)]
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
    /// Wörterbuch patterns to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line.
    patterns: Option<Vec<String>>,
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
    let patterns = args.patterns;
    let unique = args.unique.unwrap_or(false);

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let (mut con, mut responses) =
        connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    spawn(async move {
        while let Some(msg) = responses.recv().await {
            print_message(&msg, json);
        }
    });

    if let Some(patterns) = patterns {
        for pattern in patterns {
            if unique {
                con.psubscribe_unique_async(pattern.to_owned()).await?;
            } else {
                con.psubscribe_async(pattern.to_owned()).await?;
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(pattern)) = lines.next_line().await {
            if unique {
                con.psubscribe_unique_async(pattern).await?;
            } else {
                con.psubscribe_async(pattern).await?;
            }
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
