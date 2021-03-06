use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp;
#[cfg(feature = "ws")]
use libworterbuch::client::ws;
use libworterbuch::{client::Connection, codec::ServerMessage as SM};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_err, print_pstate, print_state};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json) = app(
        "wbpget",
        "Get values for patterns from a Wörterbuch.",
        true,
        vec![Arg::with_name("PATTERNS")
            .multiple(true)
            .help(
                r#"Patterns to be fetched from Wörterbuch in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line."#,
            )
            .takes_value(true)
            .required(false)],
    )?;

    let patterns = matches.get_many::<String>("PATTERNS");

    #[cfg(feature = "tcp")]
    let mut con = tcp::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "ws")]
    let mut con = ws::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "graphql")]
    let mut con = gql::connect(&proto, &host_addr, port).await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            let tid = msg.transaction_id();
            let mut acked = acked_recv.lock().expect("mutex is poisoned");
            if tid > *acked {
                *acked = tid;
            }
            match msg {
                SM::PState(msg) => print_pstate(&msg, json),
                SM::State(msg) => print_state(&msg, json),
                SM::Err(msg) => print_err(&msg, json),
                SM::Ack(_) => {}
            }
        }
    });

    if let Some(patterns) = patterns {
        for pattern in patterns {
            trans_id = con.pget(pattern)?;
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(pattern)) = lines.next_line().await {
            trans_id = con.pget(&pattern)?;
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
