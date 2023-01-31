use anyhow::Result;
use clap::Arg;
use std::{process, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_message};
use worterbuch_client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (matches, proto, host_addr, port, json, debug) = app(
        "wbpsub",
        "Subscribe to values matching Wörterbuch patterns.",
        vec![
            Arg::with_name("PATTERNS")
                .multiple(true)
                .help(
                    r#"Wörterbuch patterns to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line."#,
                )
                .takes_value(true)
                .required(false),
            Arg::with_name("UNIQUE")
                .help("Only receive unique values, i.e. skip notifications when a key is set to a value it already has.")
                .long("unique")
                .short('u')
        ],
    )?;

    let patterns = matches.get_many::<String>("PATTERNS");
    let unique = matches.is_present("UNIQUE");

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    if debug {
        eprintln!("Server: {proto}://{host_addr}:{port}");
    }

    let mut con = connect(&proto, &host_addr, port, vec![], vec![], on_disconnect).await?;

    let mut responses = con.responses();

    spawn(async move {
        while let Ok(msg) = responses.recv().await {
            print_message(&msg, json, debug);
        }
    });

    if let Some(patterns) = patterns {
        for pattern in patterns {
            if unique {
                con.psubscribe_unique(pattern.to_owned())?;
            } else {
                con.psubscribe(pattern.to_owned())?;
            }
        }
    } else {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(pattern)) = lines.next_line().await {
            if unique {
                con.psubscribe_unique(pattern)?;
            } else {
                con.psubscribe(pattern)?;
            }
        }
    }

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
