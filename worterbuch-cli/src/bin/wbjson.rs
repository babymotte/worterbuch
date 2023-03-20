use std::{fs, io::Read};

use anyhow::Result;
use clap::Parser;
use serde_json::Value;
use worterbuch_client::KeyValuePair;

#[derive(Parser)]
#[command(author, version, about = "Convert JSON into Wörterbuch key/value pairs.", long_about = None)]
struct Args {
    /// Output data in JSON format.
    #[arg(short, long)]
    json: bool,
    /// JSON file to be converted. If omitted, JSON is read from stdin.
    file: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let args: Args = Args::parse();

    let json = if let Some(file) = args.file {
        fs::read_to_string(file)?
    } else {
        let mut json = String::new();
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        handle.read_to_string(&mut json)?;
        json
    };

    let kvps = convert(&json)?;

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

fn convert(json: &str) -> Result<Vec<KeyValuePair>> {
    let parsed: Value = serde_json::from_str(json)?;

    let mut kvps = Vec::new();

    if let Some(object) = parsed.as_object() {
        for (key, value) in object {
            traverse(key, value.to_owned(), &mut kvps);
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
