use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
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

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(index) = line.find('=') {
            let key = &line[..index];
            let val = &line[index + 1..];
            con.set(key, val)?;
        } else {
            eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
        }
    }

    // TODO keep TCP connection open until all requests have been ACKed

    Ok(())
}
