use anyhow::Result;
use std::{env, time::Duration};
use tokio::time::sleep;
#[cfg(feature = "graphql")]
use worterbuch_cli::gql::GqlConnection;
#[cfg(feature = "tcp")]
use worterbuch_cli::tcp::TcpConnection;
#[cfg(feature = "ws")]
use worterbuch_cli::ws::WsConnection;
use worterbuch_cli::Connection;

#[cfg(feature = "tcp")]
async fn connect() -> Result<TcpConnection> {
    worterbuch_cli::tcp::connect().await
}

#[cfg(feature = "ws")]
async fn connect() -> Result<WsConnection> {
    worterbuch_cli::ws::connect().await
}

#[cfg(feature = "graphql")]
async fn connect() -> Result<GqlConnection> {
    worterbuch_cli::gql::connect().await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let arg = env::args().skip(1).next();

    let request_pattern = match arg {
        Some(pattern) => pattern,
        None => {
            eprintln!("no subscription pattern specified");
            return Ok(());
        }
    };

    let mut con = connect().await?;

    con.subscribe(&request_pattern)?;

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
