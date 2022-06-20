use anyhow::Result;
use std::env;
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

    let mut con = connect().await?;

    let path = match env::args().skip(1).next() {
        Some(path) => path,
        None => {
            eprintln!("No path specified");
            return Ok(());
        }
    };

    let mut acks = con.acks();

    con.export(&path)?;

    acks.recv().await?;

    Ok(())
}
