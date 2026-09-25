mod logging;

use clap::Parser;
use miette::IntoDiagnostic;
use std::time::Duration;
use tosub::{CancelOnShutdown, Subsystem};
use tracing::info;
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
    /// Output data in JSON and expect input data to be JSON.
    #[arg(short, long)]
    json: bool,
    /// Keys to be deleted from Wörterbuch in the form "KEY1 KEY2 KEY3  …". When omitted, keys will be read from stdin. When reading keys from stdin, one key is expected per line.
    keys: Option<Vec<String>>,
    /// Auth token to be used for acquiring authorization from the server
    #[arg(long)]
    auth: Option<AuthToken>,
    /// Print only the value of the deleted key/value pair
    #[arg(short, long)]
    raw: bool,
    /// Set a client name on the server
    #[arg(short, long)]
    name: Option<String>,
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
    },
    // Throughput,
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    dotenvy::dotenv().ok();
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
        } => {
            latency_test(
                subsys,
                client_config,
                publishers,
                key_length,
                n_ary,
                values_per_key,
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
) -> Result<(), miette::Error> {
    let test = latency::LatencyTest::new(
        &subsys,
        publishers,
        key_length,
        n_ary,
        values_per_key,
        client_config,
    );

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
