/*
 *  Worterbuch cli clients common module
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use miette::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    ops::ControlFlow,
    thread,
    time::Duration,
};
use tokio::{select, spawn, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::error;
use worterbuch_client::{
    Err, Key, KeyValuePair, KeyValuePairs, LsState, PState, PStateEvent, ServerMessage as SM,
    State, StateEvent,
};

pub async fn next_item<T>(rx: &mut mpsc::Receiver<T>, done: bool) -> Option<T> {
    if done {
        sleep(Duration::from_secs(10)).await;
        None
    } else {
        rx.recv().await
    }
}

pub fn provide_keys(keys: Option<Vec<String>>, subsys: SubsystemHandle, tx: mpsc::Sender<String>) {
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
        subsys.start(SubsystemBuilder::new("read-stdin", |s| async move {
            let (lines_tx, mut lines_rx) = mpsc::channel(1);
            thread::spawn(move || {
                let mut lines = BufReader::new(std::io::stdin()).lines();
                while let Some(Ok(line)) = lines.next() {
                    if let Err(e) = lines_tx.blocking_send(line) {
                        error!("Could not forward line from stdin: {e}");
                    }
                }
            });
            loop {
                select! {
                    _ = s.on_shutdown_requested() => break,
                    recv = lines_rx.recv() => if let Some(key) = recv {
                        if tx.send(key).await.is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            Ok(()) as Result<()>
        }));
    }
}

pub fn provide_values(json: bool, subsys: SubsystemHandle, tx: mpsc::Sender<Value>) {
    subsys.start(SubsystemBuilder::new("read-stdin", move |s| async move {
        let (lines_tx, mut lines_rx) = mpsc::channel(1);
        thread::spawn(move || {
            let mut lines = BufReader::new(std::io::stdin()).lines();
            while let Some(Ok(line)) = lines.next() {
                if let Err(e) = lines_tx.blocking_send(line) {
                    error!("Could not forward line from stdin: {e}");
                }
            }
        });
        loop {
            select! {
                _ = s.on_shutdown_requested() => break,
                recv = lines_rx.recv() => if let Some(line) = recv {
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
                    } else if tx.send(json!(line)).await.is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        Ok(()) as Result<()>
    }));
}

pub fn provide_key_value_pairs(
    key_value_pairs: Option<Vec<String>>,
    json: bool,
    subsys: SubsystemHandle,
    tx: mpsc::Sender<(Key, Value)>,
) {
    if let Some(key_value_pairs) = key_value_pairs {
        spawn(async move {
            for kvp in key_value_pairs {
                if let ControlFlow::Break(_) = provide_key_value_pair(json, kvp, &tx).await {
                    break;
                }
            }
        });
    } else {
        let (lines_tx, mut lines_rx) = mpsc::channel(1);
        thread::spawn(move || {
            let mut lines = BufReader::new(std::io::stdin()).lines();
            while let Some(Ok(line)) = lines.next() {
                if let Err(e) = lines_tx.blocking_send(line) {
                    error!("Could not forward line from stdin: {e}");
                }
            }
        });
        subsys.start(SubsystemBuilder::new("read-stdin", move|s| async move {
            loop {
                select! {
                    _ = s.on_shutdown_requested() => break,
                    recv = lines_rx.recv() => if let Some(line) = recv {
                        if let ControlFlow::Break(_) = provide_key_value_pair(json, line, &tx).await {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            Ok(()) as Result<()>
        }));
    }
}

#[derive(Debug, Deserialize)]
enum Line {
    #[serde(untagged)]
    Kvp(KeyValuePair),
    #[serde(untagged)]
    Kvps(KeyValuePairs),
}

async fn provide_key_value_pair(
    json: bool,
    line: String,
    tx: &mpsc::Sender<(String, Value)>,
) -> ControlFlow<()> {
    if json {
        match serde_json::from_str::<Line>(&line) {
            Ok(Line::Kvp(KeyValuePair { key, value })) => {
                if tx.send((key, value)).await.is_err() {
                    return ControlFlow::Break(());
                }
            }
            Ok(Line::Kvps(kvps)) => {
                for KeyValuePair { key, value } in kvps {
                    if tx.send((key, value)).await.is_err() {
                        return ControlFlow::Break(());
                    }
                }
            }
            Err(e) => {
                eprintln!("Error parsing json: {e}");
            }
        }
    } else if let Some(index) = line.find('=') {
        let key = line[..index].to_owned();
        let value = line[index + 1..].to_owned();
        if tx.send((key, json!(value))).await.is_err() {
            return ControlFlow::Break(());
        }
    } else {
        eprintln!("no key/value pair (e.g. 'a=b'): {line}");
    }
    ControlFlow::Continue(())
}

pub fn print_message(msg: &SM, json: bool, raw: bool) {
    match msg {
        SM::PState(msg) => print_pstate(msg, json, raw),
        SM::State(msg) => print_state(msg, json, raw),
        SM::Err(msg) => print_err(msg, json),
        SM::LsState(msg) => print_ls(msg, json),
        _ => (),
    }
}

pub fn print_change_event(msg: &SM, json: bool) {
    match msg {
        SM::PState(msg) => print_pstate_change(msg, json),
        SM::State(msg) => print_state_change(msg, json),
        SM::Err(msg) => print_err(msg, json),
        _ => (),
    }
}

pub fn print_del_event(msg: &SM, json: bool) {
    match msg {
        SM::PState(msg) => print_pstate_del(msg, json),
        SM::State(msg) => print_state_del(msg, json),
        SM::Err(msg) => print_err(msg, json),
        _ => (),
    }
}

fn print_pstate(msg: &PState, json: bool, raw: bool) {
    match (json, raw) {
        (true, true) => print_msg_as_json(&msg.event),
        (true, false) => print_msg_as_json(msg),
        (false, true) => match &msg.event {
            PStateEvent::KeyValuePairs(kvps) => {
                for kvp in kvps {
                    println!("{kvp}");
                }
            }
            PStateEvent::Deleted(kvps) => {
                for kvp in kvps {
                    println!("{}={}", kvp.key, Value::Null);
                }
            }
        },
        (false, false) => println!("{msg}"),
    }
}

fn print_state(msg: &State, json: bool, raw: bool) {
    match (json, raw) {
        (true, true) => {
            if let StateEvent::Value(val) = &msg.event {
                print_msg_as_json(val);
            } else {
                print_msg_as_json(Value::Null);
            }
        }
        (true, false) => print_msg_as_json(msg),
        (false, true) => {
            if let StateEvent::Value(val) = &msg.event {
                println!("{val}");
            } else {
                println!("{}", Value::Null);
            }
        }
        (false, false) => println!("{msg}"),
    }
}

fn print_ls(msg: &LsState, json: bool) {
    if json {
        print_msg_as_json(msg);
    } else {
        println!("{msg}");
    }
}

fn print_err(msg: &Err, json: bool) {
    if json {
        print_msg_as_json(msg);
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

fn print_state_change(msg: &State, json: bool) {
    if json {
        if let StateEvent::Value(val) = &msg.event {
            print_msg_as_json(val);
        }
    } else if let StateEvent::Value(val) = &msg.event {
        println!("{val}");
    }
}

fn print_state_del(msg: &State, json: bool) {
    if json {
        if let StateEvent::Deleted(val) = &msg.event {
            print_msg_as_json(val);
        }
    } else if let StateEvent::Deleted(val) = &msg.event {
        println!("{val}");
    }
}

fn print_pstate_change(msg: &PState, json: bool) {
    if json {
        if let PStateEvent::KeyValuePairs(kvps) = &msg.event {
            print_msg_as_json(kvps);
        }
    } else if let PStateEvent::KeyValuePairs(kvps) = &msg.event {
        for kvp in kvps {
            println!("{kvp}");
        }
    }
}

fn print_pstate_del(msg: &PState, json: bool) {
    if json {
        if let PStateEvent::Deleted(kvps) = &msg.event {
            print_msg_as_json(kvps);
        }
    } else if let PStateEvent::Deleted(kvps) = &msg.event {
        for kvp in kvps {
            println!("{kvp}");
        }
    }
}
