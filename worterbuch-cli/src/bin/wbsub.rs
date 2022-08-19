use anyhow::Result;
use clap::Arg;
#[cfg(feature = "tcp")]
use libworterbuch::client::tcp as wb;
#[cfg(feature = "ws")]
use libworterbuch::client::ws as wb;
use std::{process, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_message};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json,debug) = app(
        "wbsub",
        "Subscribe to values of Wörterbuch keys.",
        vec![
            Arg::with_name("KEYS")
                .multiple(true)
                .help(
                    r#"Wörterbuch keys to be subscribed to in the form "KEY1 KEY2 KEY3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line."#,
                )
                .takes_value(true)
                .required(false),
            Arg::with_name("UNIQUE")
                .help("Only receive unique values, i.e. skip notifications when a key is set to a value it already has.")
                .long("unique")
                .short('u')
        ]
    )?;

    let keys = matches.get_many::<String>("KEYS");
    let unique = matches.is_present("UNIQUE");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = wb::connect(&proto, &host_addr, port, on_disconnect).await?;

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            print_message(&msg, json, debug);
        }
    });

    if let Some(keys) = keys {
        for key in keys {
            if unique {
                con.subscribe_unique(key)?;
            } else {
                con.subscribe(key)?;
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(key)) = lines.next_line().await {
            if unique {
                con.subscribe_unique(&key)?;
            } else {
                con.subscribe(&key)?;
            }
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
