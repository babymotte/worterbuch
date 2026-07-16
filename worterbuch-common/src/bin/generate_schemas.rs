use std::fs;

use schemars::schema_for;
use worterbuch_common::protocol::v1::{ClientMessage, ServerMessage};

fn main() {
    eprintln!("Generating JSON schemas...");

    let client_server_client = schema_for!(ClientMessage);
    let client_server_server = schema_for!(ServerMessage);

    fs::write(
        "schema/client.schema.v1.yaml",
        serde_yaml::to_string(&client_server_client)
            .expect("Failed to serialize client schema")
            .as_bytes(),
    )
    .expect("Failed to write client schema to file");
    fs::write(
        "schema/server.schema.v1.yaml",
        serde_yaml::to_string(&client_server_server)
            .expect("Failed to serialize server schema")
            .as_bytes(),
    )
    .expect("Failed to write server schema to file");
}
