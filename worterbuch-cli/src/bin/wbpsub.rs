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
        "wbpsub",
        "Subscribe to values matching Wörterbuch patterns.",
        true,
        vec![
            Arg::with_name("PATTERNS")
                .multiple(true)
                .help(
                    r#"Wörterbuch patterns to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line."#,
                )
                .takes_value(true)
                .required(false),
            Arg::with_name("UNIQUE")
                .help("Only receive unique values, i.e. skip notifications when a key is set to a value it already has.")
                .long("unique")
                .short('u')
        ],
    )?;

    let patterns = matches.get_many::<String>("PATTERNS");
    let unique = matches.is_present("UNIQUE");

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
                SM::Ack(_) | SM::Handshake(_) => {}
            }
        }
    });

    if let Some(patterns) = patterns {
        for pattern in patterns {
            if unique {
                con.psubscribe_unique(pattern)?;
            } else {
                con.psubscribe(pattern)?;
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(pattern)) = lines.next_line().await {
            if unique {
                con.psubscribe_unique(&pattern)?;
            } else {
                con.psubscribe(&pattern)?;
            }
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
