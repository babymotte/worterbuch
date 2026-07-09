use schemars::schema_for;
use worterbuch_common::protocol::client_server::{ClientMessage, ServerMessage};

fn main() {
    eprintln!("Generating JSON schemas...");

    let client_server_client = schema_for!(ClientMessage);
    let client_server_server = schema_for!(ServerMessage);
    // let cluster = schema_for!(ServerMessage);

    println!(
        "{}",
        serde_json::to_string_pretty(&client_server_client).unwrap()
    );
}
