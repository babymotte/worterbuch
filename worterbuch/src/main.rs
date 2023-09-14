use anyhow::Result;
use clap::Parser;
use worterbuch::run_worterbuch;
use worterbuch::Config;

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
struct Args {}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let config = Config::new()?;
    let _args: Args = Args::parse();

    run_worterbuch(config).await?;

    Ok(())
}
