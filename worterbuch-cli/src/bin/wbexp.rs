use anyhow::Result;
use clap::Arg;
#[cfg(feature = "graphql")]
use libworterbuch::client::gql;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp;
#[cfg(feature = "ws")]
use libworterbuch::client::ws;
use libworterbuch::client::Connection;
use worterbuch_cli::app;

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

    #[cfg(feature = "tcp")]
    let mut con = tcp::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "ws")]
    let mut con = ws::connect(&proto, &host_addr, port).await?;
    #[cfg(feature = "graphql")]
    let mut con = gql::connect(&proto, &host_addr, port).await?;

    let mut responses = con.responses();
    con.export(path)?;
    responses.recv().await?;

    Ok(())
}
