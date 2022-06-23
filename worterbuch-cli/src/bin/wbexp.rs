use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use worterbuch_cli::gql::GqlConnection;
#[cfg(feature = "tcp")]
use worterbuch_cli::tcp::TcpConnection;
#[cfg(feature = "ws")]
use worterbuch_cli::ws::WsConnection;
use worterbuch_cli::{utils::app, Connection};

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

    let (matches, proto, host_addr, port, _json) = app(
        "wbexp",
        "Export key/value pairs from Wörterbuch to a JSON file.",
        false,
        vec![Arg::with_name("PATH")
            .multiple(false)
            .help(
                r#"Path to the JSON file to be exported to. Note that this refers to the file system of the server, the file will NOT be downloaded to the client."#,
            )
            .takes_value(true)
            .required(true)],
    )?;

    let path = matches.get_one::<String>("PATH").expect("path is required");

    let mut con = connect(&proto, &host_addr, port).await?;

    let mut responses = con.responses();

    con.export(path)?;

    responses.recv().await?;

    Ok(())
}
