/*
 *  Worterbuch cli client for listing subkeys
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
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle, Toplevel};
use tracing::warn;
use tracing_subscriber::EnvFilter;
use worterbuch_cli::print_message;
use worterbuch_client::config::Config;
use worterbuch_client::{AuthToken, connect};

#[derive(Parser)]
#[command(author, version, about = "List matching child keys on a Wörterbuch server.", long_about = None)]
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
    /// The key for which to list sub keys. If omitted, root keys will be listed.
    parent_pattern: Option<String>,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long)]
    auth: Option<AuthToken>,
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
    Toplevel::new(|s| async move {
        s.start(SubsystemBuilder::new("wbls", run));
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_millis(1000))
    .await?;

    Ok(())
}

async fn run(subsys: SubsystemHandle) -> Result<()> {
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
    let parent_pattern = args.parent_pattern;

    let (wb, mut on_discnnect) = connect(config).await?;
    if let Some(name) = args.name {
        wb.set_client_name(name).await?;
    }
    let mut responses = wb.all_messages().await?;

    let trans_id = wb.pls_async(parent_pattern).await?;
    let mut acked = 0;

    loop {
        if acked >= trans_id {
            break;
        }
        select! {
            _ = subsys.on_shutdown_requested() => break,
            _ = &mut on_discnnect => {
                warn!("Connection to server lost.");
                subsys.request_shutdown();
            }
            msg = responses.recv() => if let Some(msg) = msg {
                if let Some(tid) = msg.transaction_id() {
                    if tid > acked {
                        acked = tid;
                    }
                }
                print_message(&msg, json, false);
            },
        }
    }

    wb.close().await?;

    Ok(())
}
