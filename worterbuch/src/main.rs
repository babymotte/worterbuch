use anyhow::Result;
use clap::Parser;
use std::time::Duration;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio_graceful_shutdown::Toplevel;
use worterbuch::run_worterbuch;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
struct Args {}

#[tokio::main()]
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
