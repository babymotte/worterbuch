use anyhow::Result;
use clap::Parser;
use tokio::runtime::{self, Runtime};
use worterbuch::run_worterbuch;
use worterbuch::Config;

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
struct Args {
    /// Start Wörterbuch in single threaded mode. Default is multi threaded.
    #[arg(short, long)]
    single_threaded: bool,
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let config = Config::new()?;
    let args: Args = Args::parse();

    let single_threaded = args.single_threaded || config.single_threaded;

    let rt = if single_threaded {
        log::info!("Starting Wörterbuch in single-threaded mode …");
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        log::info!("Starting Wörterbuch in multi-threaded mode …");
        Runtime::new()?
    };

    rt.block_on(run_worterbuch(config))?;

    Ok(())
}
