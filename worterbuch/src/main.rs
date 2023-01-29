use anyhow::Result;
use clap::App;
use tokio::runtime::{self, Runtime};
use worterbuch::run_worterbuch;
use worterbuch::Config;

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    App::new("worterbuch")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("An in-memory data base / message broker hybrid")
        .get_matches();

    let config = Config::new()?;

    let single_threaded = config.single_threaded;

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
