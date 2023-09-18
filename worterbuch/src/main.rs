use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio_graceful_shutdown::Toplevel;
use worterbuch::run_worterbuch;

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let _args: Args = Args::parse();

    Toplevel::new()
        .start("worterbuch", run_worterbuch)
        .catch_signals()
        .handle_shutdown_requests(Duration::from_millis(1000))
        .await?;

    Ok(())
}
