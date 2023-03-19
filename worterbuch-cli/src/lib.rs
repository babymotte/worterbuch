use serde::Serialize;
use worterbuch_client::{Err, LsState, PState, ServerMessage as SM, State};

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
