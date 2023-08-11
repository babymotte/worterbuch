use serde::Serialize;
use serde_json::{json, Value};
use std::{ops::ControlFlow, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    select, spawn,
    sync::mpsc,
    time::sleep,
};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_client::{Err, Key, KeyValuePair, LsState, PState, ServerMessage as SM, State};

pub async fn next_item<T>(rx: &mut mpsc::Receiver<T>, done: bool) -> Option<T> {
    if done {
        sleep(Duration::from_secs(10)).await;
        None
    } else {
        rx.recv().await
    }
}

pub fn provide_keys(keys: Option<Vec<String>>, subsys: SubsystemHandle) -> mpsc::Receiver<String> {
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
                    _ = subsys.on_shutdown_requested() => break,
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

pub fn provide_values(json: bool, subsys: SubsystemHandle) -> mpsc::Receiver<Value> {
    let (tx, rx) = mpsc::channel(1);

    spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            select! {
                _ = subsys.on_shutdown_requested() => break,
                recv = lines.next_line() => if let Ok(Some(line)) = recv {
                    if json {
                        match serde_json::from_str::<Value>(&line) {
                            Ok(value) => {
                                if tx.send(value).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error parsing json: {e}");
                            }
                        }
                    } else {
                        if tx.send(json!(line)).await.is_err() {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }
    });

    rx
}

pub fn provide_key_value_pairs(
    key_value_pairs: Option<Vec<String>>,
    json: bool,
    subsys: SubsystemHandle,
) -> mpsc::Receiver<(Key, Value)> {
    let (tx, rx) = mpsc::channel(1);

    if let Some(key_value_pairs) = key_value_pairs {
        spawn(async move {
            for kvp in key_value_pairs {
                if let ControlFlow::Break(_) = provide_key_value_pair(json, kvp, &tx).await {
                    break;
                }
            }
        });
    } else {
        spawn(async move {
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            loop {
                select! {
                    _ = subsys.on_shutdown_requested() => break,
                    recv = lines.next_line() => if let Ok(Some(kvp)) = recv {
                        if let ControlFlow::Break(_) = provide_key_value_pair(json, kvp, &tx).await {
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

async fn provide_key_value_pair(
    json: bool,
    kvp: String,
    tx: &mpsc::Sender<(String, Value)>,
) -> ControlFlow<()> {
    if json {
        match serde_json::from_str::<KeyValuePair>(&kvp) {
            Ok(KeyValuePair { key, value }) => {
                if tx.send((key, value)).await.is_err() {
                    return ControlFlow::Break(());
                }
            }
            Err(e) => {
                eprintln!("Error parsing json: {e}");
            }
        }
    } else {
        if let Some(index) = kvp.find('=') {
            let key = kvp[..index].to_owned();
            let value = kvp[index + 1..].to_owned();
            if tx.send((key, json!(value))).await.is_err() {
                return ControlFlow::Break(());
            }
        } else {
            eprintln!("no key/value pair (e.g. 'a=b'): {}", kvp);
        }
    }
    ControlFlow::Continue(())
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
