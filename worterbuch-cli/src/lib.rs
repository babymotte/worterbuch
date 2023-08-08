use serde::Serialize;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    select, signal, spawn,
    sync::mpsc,
    time::sleep,
};
use worterbuch_client::{Err, Key, KeyValuePair, LsState, PState, ServerMessage as SM, State};

pub async fn next_key(rx: &mut mpsc::Receiver<String>, done: bool) -> Option<String> {
    if done {
        sleep(Duration::from_secs(10)).await;
        None
    } else {
        rx.recv().await
    }
}

pub async fn next_key_value(
    rx: &mut mpsc::Receiver<(Key, Value)>,
    done: bool,
) -> Option<(String, Value)> {
    if done {
        sleep(Duration::from_secs(10)).await;
        None
    } else {
        rx.recv().await
    }
}

pub fn provide_keys(keys: Option<Vec<String>>) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel(1);

    if let Some(keys) = keys {
        spawn(async move {
            for key in keys {
                if tx.send(key).await.is_err() {
                    break;
                }
            }
            drop(tx);
        });
    } else {
        spawn(async move {
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            loop {
                select! {
                    _ = signal::ctrl_c() => break,
                    recv = lines.next_line() => if let Ok(Some(key)) = recv {
                        if tx.send(key).await.is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        });
    }

    rx
}

pub fn provide_key_value_pairs(
    key_value_pairs: Option<Vec<String>>,
    json: bool,
) -> mpsc::Receiver<(Key, Value)> {
    let (tx, rx) = mpsc::channel(1);

    if json {
        spawn(async move {
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str::<KeyValuePair>(&line) {
                    Ok(KeyValuePair { key, value }) => {
                        if tx.send((key, value)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error parsing json: {e}");
                    }
                }
            }
        });
    } else {
        if let Some(key_calue_pairs) = key_value_pairs {
            spawn(async move {
                for key_calue_pair in key_calue_pairs {
                    if let Some(index) = key_calue_pair.find('=') {
                        let key = key_calue_pair[..index].to_owned();
                        let value = key_calue_pair[index + 1..].to_owned();
                        if tx.send((key, json!(value))).await.is_err() {
                            break;
                        }
                    } else {
                        eprintln!("no key/value pair (e.g. 'a=b'): {}", key_calue_pair);
                    }
                }
            });
        } else {
            spawn(async move {
                let mut lines = BufReader::new(tokio::io::stdin()).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(index) = line.find('=') {
                        let key = line[..index].to_owned();
                        let value = line[index + 1..].to_owned();
                        if tx.send((key, json!(value))).await.is_err() {
                            break;
                        }
                    } else {
                        eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
                    }
                }
            });
        }
    }

    rx
}

pub fn print_message(msg: &SM, json: bool) {
    match msg {
        SM::PState(msg) => print_pstate(&msg, json),
        SM::State(msg) => print_state(&msg, json),
        SM::Err(msg) => print_err(&msg, json),
        SM::LsState(msg) => print_ls(&msg, json),
        _ => (),
    }
}

fn print_pstate(msg: &PState, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        println!("{msg}");
    }
}

fn print_state(msg: &State, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        println!("{msg}");
    }
}

fn print_ls(msg: &LsState, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        println!("{msg}");
    }
}

fn print_err(msg: &Err, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        eprintln!("{msg}");
    }
}

fn print_msg_as_json(msg: impl Serialize) {
    match serde_json::to_string(&msg) {
        Ok(json) => println!("{json}"),
        Err(e) => {
            eprintln!("Error converting message to json: {e}");
        }
    }
}
