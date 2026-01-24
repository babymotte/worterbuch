/*
 *  Worterbuch cli client for subscribing to changes
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

use clap::Parser;
use miette::Result;
use std::io;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tosub::Subsystem;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use worterbuch_cli::{next_item, print_message, provide_keys};
use worterbuch_client::config::Config;
use worterbuch_client::{AuthToken, connect};

#[derive(Parser)]
#[command(author, version, about = "Subscribe to values of Wörterbuch keys.", long_about = None)]
struct Args {
    /// Connect to the Wörterbuch server using SSL encryption.
    #[arg(short, long)]
    ssl: bool,
    /// The addresses of the Wörterbuch servers in form <ip>:<port>[,<ip2>:<port2>,…]. When omitted, the value of the env var WORTERBUCH_SERVERS will be used. If that is not set, 127.0.0.1:8081 will be used.
    #[arg(short, long)]
    addr: Option<String>,
    /// Output data in JSON and expect input data to be JSON.
    #[arg(short, long)]
    json: bool,
    /// Wörterbuch keys to be subscribed to in the form "PATTERN1 PATTERN2 PATTERN3 ...". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line.
    keys: Option<Vec<String>>,
    /// Only receive unique values, i.e. skip notifications when a key is set to a value it already has.
    #[arg(short, long)]
    unique: bool,
    /// Only receive live values, i.e. do not receive a callback for the state currently stored on the broker.
    #[arg(short, long)]
    live_only: bool,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long)]
    auth: Option<AuthToken>,
    /// Print only the received events
    #[arg(short, long)]
    raw: bool,
    /// Set a client name on the server
    #[arg(short, long)]
    name: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    Subsystem::build_root("wbsub")
        .catch_signals()
        .with_timeout(Duration::from_millis(1000))
        .start(run)
        .join()
        .await;

    Ok(())
}

async fn run(subsys: Subsystem) -> Result<()> {
    let mut config = Config::new();
    let args: Args = Args::parse();

    config.auth_token = args.auth.or(config.auth_token);

    config.proto = if args.ssl {
        "wss".to_owned()
    } else {
        config.proto
    };
    config.servers = args
        .addr
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter_map(|s| s.parse().ok())
                .collect()
        })
        .unwrap_or(config.servers);
    let json = args.json;
    let raw = args.raw;
    let keys = args.keys;
    let unique = args.unique;
    let live_only = args.live_only;

    let (wb, mut on_disconnect) = connect(config).await?;
    if let Some(name) = args.name {
        wb.set_client_name(name).await?;
    }
    let mut responses = wb.all_messages().await?;

    let (tx, mut rx) = mpsc::channel(1);
    subsys.spawn("provide_keys", async move |s| {
        provide_keys(keys, s, tx);
        Ok(()) as Result<()>
    });
    let mut done = false;

    loop {
        select! {
            _ = subsys.shutdown_requested() => break,
            _ = &mut on_disconnect => {
                warn!("Connection to server lost.");
                subsys.request_global_shutdown();
            }
            msg = responses.recv() => if let Some(msg) = msg {
                print_message(&msg, json, raw);
            },
            recv = next_item(&mut rx, done) => match recv {
                Some(key ) => {
                    wb.subscribe_async(key, unique, live_only).await?;
                },
                None => done = true,
            },
        }
    }

    wb.close().await?;

    Ok(())
}
