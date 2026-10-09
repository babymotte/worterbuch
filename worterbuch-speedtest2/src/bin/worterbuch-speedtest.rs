/*
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

mod logging;

use clap::Parser;
use miette::IntoDiagnostic;
use std::time::Duration;
use tosub::{CancelOnShutdown, Subsystem};
use tracing::{info, trace};
use worterbuch_client::{AuthToken, config::Config};
use worterbuch_speedtest2::latency;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    pub command: Commands,

    /// Connect to the Wörterbuch server using SSL encryption.
    #[arg(short, long)]
    ssl: bool,
    /// The addresses of the Wörterbuch servers in form <ip>:<port>[,<ip2>:<port2>,…]. When omitted, the value of the env var WORTERBUCH_SERVERS will be used. If that is not set, 127.0.0.1:8081 will be used.
    #[arg(short, long)]
    addr: Vec<String>,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long)]
    auth: Option<AuthToken>,
}

#[derive(clap::Subcommand)]
enum Commands {
    Latency {
        /// Number of publishing clients to use in parallel
        #[arg(long, short, value_name = "PUBLISHERS", default_value = "10")]
        publishers: usize,

        /// Length of the keys to use in the latency test
        #[arg(long, short, value_name = "LENGTH", default_value = "5")]
        key_length: usize,

        /// N-Ariness of the key tree (i.e. how many children each key segment has)
        #[arg(long, short, value_name = "NUMBER", default_value = "5")]
        n_ary: usize,

        /// Number of values each publisher sets per key
        #[arg(long, short, value_name = "NUMBER", default_value = "10")]
        values_per_key: usize,

        /// Make every publisher also a subscriber
        #[arg(long, short, default_value = "false")]
        subscribe: bool,
    },
    // Throughput,
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    let _ = dotenvy::dotenv();
    logging::init()?;

    let args = Args::parse();

    tosub::build_default_root("worterbuch-speedtest")
        .with_timeout(Duration::from_secs(10))
        .start(|s| run(s, args))
        .await?;

    Ok(())
}

async fn run(subsys: Subsystem, args: Args) -> miette::Result<()> {
    let mut client_config = Config::new();

    if let Some(auth_token) = args.auth {
        client_config.auth_token = Some(auth_token);
    }

    if args.ssl {
        client_config.proto = "wss".to_owned();
    }
    if !args.addr.is_empty() {
        client_config.servers = args.addr.into_boxed_slice();
    }

    match args.command {
        Commands::Latency {
            publishers,
            key_length,
            n_ary,
            values_per_key,
            subscribe,
        } => {
            latency_test(
                subsys,
                client_config,
                publishers,
                key_length,
                n_ary,
                values_per_key,
                subscribe,
            )
            .await
        }
    }
}

async fn latency_test(
    subsys: Subsystem,
    client_config: Config,
    publishers: usize,
    key_length: usize,
    n_ary: usize,
    values_per_key: usize,
    subscribe: bool,
) -> Result<(), miette::Error> {
    let Some(test) = latency::LatencyTest::new(
        &subsys,
        publishers,
        key_length,
        n_ary,
        values_per_key,
        client_config,
        subscribe,
    ) else {
        trace!("shutdown requested, not spawning latency test");
        return Ok(());
    };

    let result = match test.join().or_cancel_on_shutdown(&subsys).await {
        Some(Ok(Some(result))) => result,
        Some(Err(e)) => return Err(e).into_diagnostic(),
        _ => return Ok(()),
    };

    info!(
        "Latency test result: set {} values in {:?} ({} values/sec); preparation time: {:?}",
        result.total_key_value_pairs,
        result.run_duration,
        (result.total_key_value_pairs as f64 / result.run_duration.as_secs_f64()).round() as u64,
        result.prepare_duration,
    );

    Ok(())
}
