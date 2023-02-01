use anyhow::Result;
use clap::Arg;
use serde_json::json;
use std::{
    process,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    spawn,
    time::sleep,
};
use worterbuch_cli::{app, print_message};
use worterbuch_client::connect;
use worterbuch_client::KeyValuePair;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (matches, proto, host_addr, port, json) = app(
        "wbset",
        "Set values of keys on a Wörterbuch.",
        vec![
            Arg::with_name("JSON")
                .short('j')
                .long("json")
                .help(r#"Read key/value pairs as JSON formatted {"key":"some key","value":"some value"} instead of '[key]=[value]' pairs. This option is only available when reading from stdin. One JSON object is expected per line."#)
                .takes_value(false)
                .required(false),
            Arg::with_name("KEY_VALUE_PAIRS")
                .multiple(true)
                .help(
                    r#"Key/value pairs to be set on Wörterbuch in the form "KEY1=VALUE1 KEY2=VALUE2 KEY3=VALUE3 ...". When omitted, key/value pairs will be read from stdin. When reading key/value pairs from stdin, one key/value pair is expected per line."#,
                )
                .takes_value(true)
                .required(false)
        ],
    )?;

    let key_value_pairs = matches.get_many::<String>("KEY_VALUE_PAIRS");

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

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
            print_message(&msg, json);
        }
    });

    if json {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<KeyValuePair>(&line) {
                Ok(KeyValuePair { key, value }) => {
                    trans_id = con.set(key, json!(value))?;
                }
                Err(e) => {
                    eprintln!("Error parsing json: {e}");
                }
            }
        }
    } else {
        if let Some(key_calue_pairs) = key_value_pairs {
            for key_calue_pair in key_calue_pairs {
                if let Some(index) = key_calue_pair.find('=') {
                    let key = &key_calue_pair[..index];
                    let val = &key_calue_pair[index + 1..];
                    trans_id = con.set(key.to_owned(), json!(val))?;
                } else {
                    eprintln!("no key/value pair (e.g. 'a=b'): {}", key_calue_pair);
                }
            }
        } else {
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(index) = line.find('=') {
                    let key = &line[..index];
                    let val = &line[index + 1..];
                    trans_id = con.set(key.to_owned(), json!(val))?;
                } else {
                    eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
                }
            }
        }
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
