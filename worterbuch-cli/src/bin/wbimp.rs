use anyhow::Result;
use clap::Arg;
use std::{
    process,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{spawn, time::sleep};
use worterbuch_cli::{app, print_message};
use worterbuch_client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json,debug) = app(
        "wbimp",
        "Import key/value pairs from JSON files into Wörterbuch.",
        vec![Arg::with_name("PATHS")
            .multiple(true)
            .help(
                r#"Paths to the JSON files to be imported. Note that this refers to the file system of the server, the files will NOT be uploaded from the client."#,
            )
            .takes_value(true)
            .required(true)],
    )?;

    let paths = matches
        .get_many::<String>("PATHS")
        .expect("paths are required");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    if debug {
        eprintln!("Server: {proto}://{host_addr}:{port}");
    }

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut trans_id = 0;
    let acked = Arc::new(Mutex::new(0));
    let acked_recv = acked.clone();

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            let tid = msg.transaction_id();
            let mut acked = acked_recv.lock().expect("mutex is poisoned");
            if tid > *acked {
                *acked = tid;
            }
            print_message(&msg, json, debug);
        }
    });

    for path in paths {
        trans_id = con.import(path.to_owned())?;
    }

    loop {
        let acked = *acked.lock().expect("mutex is poisoned");
        if acked < trans_id {
            sleep(Duration::from_millis(100)).await;
        } else {
            break;
        }
    }

    Ok(())
}
