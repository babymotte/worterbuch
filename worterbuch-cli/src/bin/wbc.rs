use anyhow::Result;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql as wb;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp as wb;
#[cfg(feature = "ws")]
use libworterbuch::client::ws as wb;
use libworterbuch::{
    client::Connection,
    codec::{ClientMessage, TransactionId},
};
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
use worterbuch_cli::app;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (_, proto, host_addr, port, json) = app(
        "wbc",
        "General purpose Wörterbuch command line client.",
        true,
        vec![],
    )?;

    if !json {
        panic!("only json is implemented for now");
    }

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = wb::connect(&proto, &host_addr, port, on_disconnect).await?;

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
            println!("{}", serde_json::to_string(&msg).expect("cannot fail"));
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
        todo!()
    }

    Ok(())
}
