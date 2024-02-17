/*
 *  Worterbuch cli tool for JSON conversion
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

use anyhow::Result;
use clap::Parser;
use serde_json::Value;
use std::{fs, io::Read};
use worterbuch_client::{config::Config, AuthToken, KeyValuePair};

#[derive(Parser)]
#[command(author, version, about = "Convert JSON into Wörterbuch key/value pairs.", long_about = None)]
struct Args {
    /// Output data in JSON format.
    #[arg(short, long)]
    json: bool,
    /// JSON file to be converted. If omitted, JSON is read from stdin.
    file: Option<String>,
    /// Prefix the keys with a string.
    #[arg(short, long)]
    prefix: Option<String>,
    /// Auth token to be used to authenticate with the server
    #[arg(long)]
    auth: Option<AuthToken>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let mut config = Config::new();
    let args: Args = Args::parse();

    config.auth_token = args.auth.or(config.auth_token);

    let json = if let Some(file) = args.file {
        fs::read_to_string(file)?
    } else {
        let mut json = String::new();
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        handle.read_to_string(&mut json)?;
        json
    };

    let kvps = convert(&json, args.prefix)?;

    if args.json {
        for kvp in kvps {
            let json = serde_json::to_string(&kvp)?;
            println!("{json}");
        }
    } else {
        for KeyValuePair { key, value } in kvps {
            println!("{key}={value}");
        }
    }

    Ok(())
}

fn convert(json: &str, prefix: Option<String>) -> Result<Vec<KeyValuePair>> {
    let parsed: Value = serde_json::from_str(json)?;

    let mut kvps = Vec::new();

    if let Some(object) = parsed.as_object() {
        for (key, value) in object {
            let path = if let Some(prefix) = &prefix {
                format!("{prefix}/{key}")
            } else {
                key.to_owned()
            };
            traverse(&path, value.to_owned(), &mut kvps);
        }
    }

    Ok(kvps)
}

fn traverse(path: &str, value: Value, kvps: &mut Vec<KeyValuePair>) {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            kvps.push((path, value).into())
        }
        Value::Object(o) => {
            for (key, value) in o {
                let path = format!("{path}/{key}");
                traverse(&path, value.to_owned(), kvps);
            }
        }
    }
}
