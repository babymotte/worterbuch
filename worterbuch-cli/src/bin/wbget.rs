use anyhow::Result;
use clap::Parser;
use std::process;
use tokio::{select, signal};
use worterbuch_cli::{next_key, print_message, provide_keys};
use worterbuch_client::{config::Config, connect};

#[derive(Parser)]
#[command(author, version, about = "Get values for keys from a Wörterbuch.", long_about = None)]
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
    /// Keys to be fetched from Wörterbuch in the form "KEY1 KEY2 KEY3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line.
    keys: Option<Vec<String>>,
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

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut wb = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;
    let mut responses = wb.all_messages().await?;

    let mut trans_id = 0;
    let mut acked = 0;

    let mut rx = provide_keys(keys);

    let mut done = false;

    loop {
        if done && acked >= trans_id {
            break;
        }
        select! {
            _ = signal::ctrl_c() => break,
            msg = responses.recv() => if let Some(msg) = msg {
                let tid = msg.transaction_id();
                if tid > acked {
                    acked = tid;
                }
                print_message(&msg, json);
            },
            recv = next_key(&mut rx, done) => match recv {
                Some(key ) => trans_id = wb.get_async(key).await?,
                None => {
                    done = true;
                }
            },
        }
    }

    Ok(())
}
