/*
 *  Worterbuch cli client for subscribing to changes in the data tree structure
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
use miette::{Context, Result};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tosub::Subsystem;
use tracing::warn;
use worterbuch_cli::{next_item, print_message, provide_keys, server_addresses};
use worterbuch_client::config::Config;
use worterbuch_client::{AuthToken, connect, logging};

#[derive(Parser)]
#[command(author, version, about = "Subscribe to values of Wörterbuch keys.", long_about = None)]
struct Args {
    /// Connect to the Wörterbuch server using SSL encryption.
    #[arg(short, long)]
    ssl: bool,
    /// The address(es) of the Wörterbuch servers to be used.
    #[arg(
        short,
        long,
        name = "HOST:PORT[,HOST:PORT,…]",
        env = "WORTERBUCH_SERVERS",
        default_value = "127.0.0.1:8081"
    )]
    addr: Vec<String>,
    /// Output data in JSON and expect input data to be JSON
    #[arg(short, long)]
    json: bool,
    /// Request that the server send trace data for subscription events
    #[arg(short, long)]
    trace: bool,
    /// Wörterbuch paths to be subscribed to in the form "PATH1 PATH2 PATH3  …". When omitted, paths will be read from stdin. When reading paths from stdin, one path is expected per line
    paths: Option<Vec<String>>,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long, name = "JWT", env = "WORTERBUCH_AUTH_TOKEN")]
    auth: Option<AuthToken>,
    /// Set a client name on the server
    #[arg(
        short,
        long,
        default_value = "wblssub-cli",
        env = "WORTERBUCH_CLIENT_NAME"
    )]
    name: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    logging::init()?;
    tosub::build_root("wblssub")
        .catch_signals()
        .with_timeout(Duration::from_millis(1000))
        .start(run)
        .await?;

    Ok(())
}

async fn run(subsys: Subsystem) -> Result<()> {
    let mut config = Config::new();
    let args: Args = Args::parse();

    config.auth_token = args.auth.or(config.auth_token);

    if args.ssl {
        config.proto = "wss".to_owned();
    }
    server_addresses(&args.addr, &mut config.servers);
    let json = args.json;
    let send_traces = args.trace;
    let paths = args.paths;

    let (wb, mut on_disconnect) = connect(config.clone()).await.wrap_err_with(|| {
        format!(
            "Could not connect to configured server(s): {}",
            config.servers.join(", ")
        )
    })?;
    wb.set_client_name(args.name).await?;
    let mut responses = wb.all_messages().await?;

    let (tx, mut rx) = mpsc::channel(1);
    subsys.spawn("provide_paths", async move |s| {
        provide_keys(paths, s, tx);
        Ok(()) as Result<()>
    });
    let mut done = false;

    loop {
        select! {
            _ = subsys.shutdown_requested() => break,
            _ = &mut on_disconnect => {
                warn!("Connection to server lost.");
                subsys.request_global_shutdown_because("disconnected from server");
            }
            msg = responses.recv() => if let Some(msg) = msg {
                print_message(&msg, json, true);
            },
            recv = next_item(&mut rx, done) => match recv {
                Some(path ) => {
                    wb.subscribe_ls_async(if path.is_empty() {None} else {Some(path)}, send_traces).await?;
                },
                None => done = true,
            },
        }
    }

    Ok(())
}
