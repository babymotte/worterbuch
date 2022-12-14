use anyhow::Result;
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
use worterbuch_cli::{app, print_message};
#[cfg(feature = "tcp")]
use worterbuch_client::tcp as wb;
#[cfg(feature = "ws")]
use worterbuch_client::ws as wb;
use worterbuch_client::{ClientMessage, Connection, TransactionId};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (_matches, proto, host_addr, port, json, debug) = app(
        "wbc",
        "General purpose Wörterbuch command line client.",
        vec![],
    )?;

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    if debug {
        eprintln!("Server: {proto}://{host_addr}:{port}");
    }

    let mut con = wb::connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

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
            print_message(&msg, json, debug);
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
            Some("get") => get(&tail, con)?,
            Some("pget") => pget(&tail, con)?,
            Some("set") => set(&tail, con)?,
            Some("sub") => sub(&tail, con)?,
            Some("psub") => psub(&tail, con)?,
            Some("subu") => subu(&tail, con)?,
            Some("psubu") => psubu(&tail, con)?,
            Some("imp") => imp(&tail, con)?,
            Some("exp") => exp(&tail, con)?,
            Some("") => {}
            Some(other) => eprintln!("unknown command: {other}"),
            None => {}
        }
    }

    Ok(())
}

fn get<'f>(key: &str, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.get(key)?;
    }
    Ok(())
}

fn pget<'f>(pattern: &str, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no key specified");
    } else {
        con.pget(pattern)?;
    }
    Ok(())
}

fn set<'f>(key_value: &str, con: &mut Connection) -> Result<()> {
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
    con.set(key, value)?;
    Ok(())
}

fn sub<'f>(key: &str, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.subscribe(key)?;
    }
    Ok(())
}

fn psub<'f>(pattern: &str, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no pattern specified");
    } else {
        con.psubscribe(pattern)?;
    }
    Ok(())
}

fn subu<'f>(key: &str, con: &mut Connection) -> Result<()> {
    if key.is_empty() {
        eprintln!("no key specified");
    } else {
        con.subscribe_unique(key)?;
    }
    Ok(())
}

fn psubu<'f>(pattern: &str, con: &mut Connection) -> Result<()> {
    if pattern.is_empty() {
        eprintln!("no pattern specified");
    } else {
        con.psubscribe_unique(pattern)?;
    }
    Ok(())
}

fn imp<'f>(path: &str, con: &mut Connection) -> Result<()> {
    if path.is_empty() {
        eprintln!("no path specified");
    } else {
        con.import(path)?;
    }
    Ok(())
}

fn exp<'f>(path: &str, con: &mut Connection) -> Result<()> {
    if path.is_empty() {
        eprintln!("no path specified");
    } else {
        con.export(path)?;
    }
    Ok(())
}
