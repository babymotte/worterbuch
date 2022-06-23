use anyhow::Result;
use clap::Arg;
use libworterbuch::codec::ServerMessage as SM;
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
#[cfg(feature = "graphql")]
use worterbuch_cli::gql::GqlConnection;
#[cfg(feature = "tcp")]
use worterbuch_cli::tcp::TcpConnection;
#[cfg(feature = "ws")]
use worterbuch_cli::ws::WsConnection;
use worterbuch_cli::{
    utils::{app, print_err, print_pstate, print_state},
    Connection,
};

#[cfg(feature = "tcp")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<TcpConnection> {
    worterbuch_cli::tcp::connect(proto, host, port).await
}

#[cfg(feature = "ws")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<WsConnection> {
    worterbuch_cli::ws::connect(proto, host, port).await
}

#[cfg(feature = "graphql")]
async fn connect(proto: &str, host: &str, port: u16) -> Result<GqlConnection> {
    worterbuch_cli::gql::connect(proto, host, port).await
}

#[tokio::main]
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

    let mut con = connect(&proto, &host_addr, port).await?;
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
