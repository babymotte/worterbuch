use anyhow::Result;
use clap::Parser;
use std::process;
use worterbuch_cli::print_message;
use worterbuch_client::config::Config;
use worterbuch_client::connect;

#[derive(Parser)]
#[command(author, version, about = "Export key/value pairs from Wörterbuch to a JSON file.", long_about = None)]
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
    /// Path to the JSON file to be exported to. Note that this refers to the file system of the server, the file will NOT be downloaded to the client.
    path: String,
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
    let path = args.path;

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut responses = con.responses();
    con.export(path.to_owned())?;
    while let Ok(msg) = responses.recv().await {
        print_message(&msg, false);
        let tid = msg.transaction_id();
        if tid == 1 {
            break;
        }
    }

    Ok(())
}
