use libworterbuch::codec::{Err, KeyValuePair, PState, State};

pub fn print_pstate(msg: &PState, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        for KeyValuePair { key, value } in &msg.key_value_pairs {
            println!("{key} = {value}");
        }
    }
}

pub fn print_state(msg: &State, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        if let Some(KeyValuePair { key, value }) = &msg.key_value {
            println!("{} = {}", key, value);
        } else {
            println!("No result.");
        }
    }
}

pub fn print_err(msg: &Err, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        eprintln!("server error {}: {}", msg.error_code, msg.metadata);
    }
}
