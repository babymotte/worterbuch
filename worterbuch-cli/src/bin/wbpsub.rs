use anyhow::Result;
use clap::Parser;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::{SubsystemHandle, Toplevel};
use worterbuch_cli::{next_item, print_message, provide_keys};
use worterbuch_client::config::Config;
use worterbuch_client::connect;

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
    unique: bool,
    /// Only receive live values, i.e. do not receive a callback for the state currently stored on the broker.
    #[arg(short, long)]
    live_only: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    Toplevel::new()
        .start("wbpsub", run)
        .catch_signals()
        .handle_shutdown_requests(Duration::from_millis(1000))
        .await?;

    Ok(())
}

async fn run(subsys: SubsystemHandle) -> Result<()> {
    let mut config = Config::new();
    let args: Args = Args::parse();

    config.proto = if args.ssl {
        "wss".to_owned()
    } else {
        "tcp".to_owned()
    };
    config.host_addr = args.addr.unwrap_or(config.host_addr);
    config.port = args.port.unwrap_or(config.port);
    let json = args.json;
    let patterns = args.patterns;
    let unique = args.unique;
    let live_only = args.live_only;

    let (disco_tx, mut disco_rx) = mpsc::channel(1);
    let on_disconnect = async move {
        disco_tx.send(()).await.ok();
    };

    let wb = connect(config, vec![], vec![], on_disconnect).await?;
    let mut responses = wb.all_messages().await?;

    let mut rx = provide_keys(patterns, subsys.clone());
    let mut done = false;

    loop {
        select! {
            _ = subsys.on_shutdown_requested() => break,
            _ = disco_rx.recv() => {
                log::warn!("Connection to server lost.");
                subsys.request_global_shutdown();
            }
            msg = responses.recv() => if let Some(msg) = msg {
                print_message(&msg, json);
            },
            recv = next_item(&mut rx, done) => match recv {
                Some(key) => {
                    wb.psubscribe_async(key, unique,live_only, Some(Duration::from_millis(1))).await?;
                },
                None => done = true,
            },
        }
    }

    Ok(())
}
