use anyhow::Result;
use clap::Arg;
use std::process;
use worterbuch_cli::{app, print_message};
use worterbuch_client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (matches, proto, host_addr, port, json, ) = app(
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
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut responses = con.responses();
    con.export(path.to_owned())?;
    while let Ok(msg) = responses.recv().await {
        print_message(&msg, json);
        let tid = msg.transaction_id();
        if tid == 1 {
            break;
        }
    }

    Ok(())
}
