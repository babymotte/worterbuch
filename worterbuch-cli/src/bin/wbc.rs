use anyhow::Result;
use clap::Parser;
use serde_json::json;
use std::{
    collections::HashSet,
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
use worterbuch_client::{config::Config, connect, ClientMessage, Connection, TransactionId};

#[derive(Parser)]
#[command(author, version, about = "General purpose Wörterbuch command line client.", long_about = None)]
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
    // TODO this needs a major rework, most of the input json can now be simply passed through to the websocket

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

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut open_trans_ids = HashSet::new();
    let acked_trans_ids = Arc::new(Mutex::new(HashSet::new()));
    let acked_trans_ids_recv = acked_trans_ids.clone();

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            acked_trans_ids_recv
                .lock()
                .expect("mutex is poisoned")
                .insert(msg.transaction_id());
            print_message(&msg, json);
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        process_line(line, &mut con, json, &mut open_trans_ids)?;
    }

    loop {
        let acked = acked_trans_ids.lock().expect("mutex is poisoned").clone();
        if acked != open_trans_ids {
            sleep(Duration::from_millis(100)).await;
        } else {
            break;
        }
    }

    Ok(())
}

fn process_line(
    line: String,
    con: &mut Connection,
    json: bool,
    trans_ids: &mut HashSet<TransactionId>,
) -> Result<()> {
    if json {
        match serde_json::from_str::<ClientMessage>(&line) {
            Ok(msg) => match &msg {
                ClientMessage::Unsubscribe(_) => con.send(msg)?,
                _ => {
                    if !trans_ids.insert(msg.transaction_id()) {
                        eprintln!("You are trying to reuse an existing transaction id, please choose a different one!");
                    } else {
                        con.send(msg)?
                    }
                }
            },
            Err(e) => eprintln!("Not a valid json representation of a client message: {e}"),
        }
    } else {
        let mut split = line.split(" ");
        let cmd = split.next();
        let tail = split.collect::<Vec<&str>>().join(" ");
        match cmd {
            Some("get") => get(tail, con)?,
            Some("pget") => pget(tail, con)?,
            Some("set") => set(tail, con)?,
            Some("sub") => sub(tail, con)?,
            Some("psub") => psub(tail, con)?,
            Some("subu") => subu(tail, con)?,
            Some("psubu") => psubu(tail, con)?,
            Some("") => {}
            Some(other) => eprintln!("unknown command: {other}"),
            None => {}
        }
    }

    Ok(())
}

fn get<'f>(key: String, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.get(key)?;
    }
    Ok(())
}

fn pget<'f>(pattern: String, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no key specified");
    } else {
        con.pget(pattern)?;
    }
    Ok(())
}

fn set<'f>(key_value: String, con: &mut Connection) -> Result<()> {
    let mut split = key_value.split("=");
    let key = match split.next() {
        Some(it) => it,
        None => {
            eprintln!("no key specified");
            return Ok(());
        }
    };
    let value = match split.next() {
        Some(it) => it,
        None => {
            eprintln!("no value specified");
            return Ok(());
        }
    };
    con.set(key.to_owned(), json!(value))?;
    Ok(())
}

fn sub<'f>(key: String, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.subscribe(key)?;
    }
    Ok(())
}

fn psub<'f>(pattern: String, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no pattern specified");
    } else {
        con.psubscribe(pattern)?;
    }
    Ok(())
}

fn subu<'f>(key: String, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.subscribe_unique(key)?;
    }
    Ok(())
}

fn psubu<'f>(pattern: String, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no pattern specified");
    } else {
        con.psubscribe_unique(pattern)?;
    }
    Ok(())
}
