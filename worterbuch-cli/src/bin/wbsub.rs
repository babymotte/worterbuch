use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql as wb;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp as wb;
#[cfg(feature = "ws")]
use libworterbuch::client::ws as wb;
use libworterbuch::codec::ServerMessage as SM;
use std::{process, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_err, print_pstate, print_state};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json) = app(
        "wbsub",
        "Subscribe to values of Wörterbuch keys.",
        true,
        vec![Arg::with_name("KEYS")
            .multiple(true)
            .help(
                r#"Wörterbuch keys to be subscribed to in the form "KEY1 KEY2 KEY3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line."#,
            )
            .takes_value(true)
            .required(false)],
    )?;

    let keys = matches.get_many::<String>("KEYS");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = wb::connect(&proto, &host_addr, port, on_disconnect).await?;

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            match msg {
                SM::PState(msg) => print_pstate(&msg, json),
                SM::State(msg) => print_state(&msg, json),
                SM::Err(msg) => print_err(&msg, json),
                SM::Ack(_) => {}
            }
        }
    });

    if let Some(keys) = keys {
        for key in keys {
            con.subscribe(key)?;
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(key)) = lines.next_line().await {
            con.subscribe(&key)?;
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
