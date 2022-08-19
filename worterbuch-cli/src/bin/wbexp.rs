use anyhow::Result;
use clap::Arg;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp as wb;
#[cfg(feature = "ws")]
use libworterbuch::client::ws as wb;
use std::process;
use worterbuch_cli::{app, print_message};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json, debug) = app(
        "wbexp",
        "Export key/value pairs from Wörterbuch to a JSON file.",
        vec![Arg::with_name("PATH")
            .multiple(false)
            .help(
                r#"Path to the JSON file to be exported to. Note that this refers to the file system of the server, the file will NOT be downloaded to the client."#,
            )
            .takes_value(true)
            .required(true)],
    )?;

    let path = matches.get_one::<String>("PATH").expect("path is required");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = wb::connect(&proto, &host_addr, port, on_disconnect).await?;

    let mut responses = con.responses();
    con.export(path)?;
    while let Ok(msg) = responses.recv().await {
        print_message(&msg, json, debug);
        let tid = msg.transaction_id();
        if tid == 1 {
            break;
        }
    }

    Ok(())
}
