use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp;
#[cfg(feature = "ws")]
use libworterbuch::client::ws;
use libworterbuch::codec::ServerMessage as SM;
use std::time::Duration;
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
        vec![Arg::with_name("PATTERNS")
            .multiple(true)
            .help(
                r#"Wörterbuch patterns to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line."#,
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

    if let Some(patterns) = patterns {
        for pattern in patterns {
            con.psubscribe(pattern)?;
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(pattern)) = lines.next_line().await {
            con.psubscribe(&pattern)?;
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
