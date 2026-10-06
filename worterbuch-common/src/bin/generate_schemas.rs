/*
 *  Worterbuch JSON schema generator
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
