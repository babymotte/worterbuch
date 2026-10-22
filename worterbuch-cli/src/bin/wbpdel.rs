/*
 *  Worterbuch cli client for deleting entries
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
use tokio::select;
use tokio::sync::mpsc;
use tosub::Subsystem;
use totils::{CancelOn, Untangle};
use tracing::warn;
use worterbuch_cli::{next_item, print_del_event, print_message, provide_keys, server_addresses};
use worterbuch_client::config::Config;
use worterbuch_client::{AuthToken, connect, logging};

#[derive(Parser)]
#[command(author, version, about = "Delete values for keys from a Wörterbuch.", long_about = None)]
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
    /// Patterns to be deleted from Wörterbuch in the form "PATTERN1 PATTERN2 PATTERN3  …". When omitted, patterns will be read from stdin. When reading patterns from stdin, one pattern is expected per line
    patterns: Option<Vec<String>>,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long, name = "JWT", env = "WORTERBUCH_AUTH_TOKEN")]
    auth: Option<AuthToken>,
    /// Print only the deleted key/value pairs
    #[arg(short, long)]
    raw: bool,
    /// Set a client name on the server
    #[arg(
        short,
        long,
        default_value = "wbpdel-cli",
        env = "WORTERBUCH_CLIENT_NAME"
    )]
    name: String,
    /// Don't return deleted values
    #[arg(short, long)]
    quiet: bool,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    logging::init()?;
    tosub::build_default_root("wbpdel").start(run).await?;

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
    let raw = args.raw;
    let patterns = args.patterns;

    let Some((wb, mut on_disconnect)) = connect(config.clone())
        .or_cancel_on(subsys.shutdown_requested())
        .await
        .untangle()
        .wrap_err_with(|| {
            format!(
                "Could not connect to configured server(s): {}",
                config.servers.join(", ")
            )
        })?
    else {
        return Ok(());
    };
    wb.set_client_name(args.name)
        .or_cancel_on(subsys.shutdown_requested())
        .await
        .untangle()?;
    let mut responses = wb.all_messages().await?;

    let mut trans_id = 0;
    let mut acked = 0;

    let (tx, mut rx) = mpsc::channel(1);
    subsys.spawn("provide_paths", async move |s| {
        provide_keys(patterns, s, tx);
        Ok(()) as Result<()>
    });
    let mut done = false;

    loop {
        if done && acked >= trans_id {
            break;
        }
        select! {
            _ = subsys.shutdown_requested() => break,
            _ = &mut on_disconnect => {
                warn!("Connection to server lost.");
                subsys.request_global_shutdown_because("disconnected from server");
            }
            msg = responses.recv() => if let Some(msg) = msg {
                if let Some(tid) = msg.transaction_id()
                    && tid > acked {
                        acked = tid;
                    }
                if raw {
                    print_del_event(&msg, json);
                } else {
                    print_message(&msg, json, false);
                }
            },
            recv = next_item(&mut rx, done) => match recv {
                Some(key ) => trans_id = wb.pdelete_async(key, args.quiet).await?,
                None => done = true,
            },
        }
    }

    Ok(())
}
