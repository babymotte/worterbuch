use anyhow::Result;
use clap::Parser;
use serde_json::json;
use std::process;
use tokio::io::{AsyncBufReadExt, BufReader};
use worterbuch_client::{config::Config, connect};

#[derive(Parser)]
#[command(author, version, about = "Test Wörterbuch's last will funciton.", long_about = None)]
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

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(
        &proto,
        &host_addr,
        port,
        vec![("last_will/hello/world", json!("OFFLINE")).into()],
        vec![],
        on_disconnect,
    )
    .await?;

    con.set_value("last_will/hello/world".to_owned(), json!("ONLINE"))?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(value)) = lines.next_line().await {
        con.set_value("last_will/hello/world".to_owned(), json!(value))?;
    }

    Ok(())
}
