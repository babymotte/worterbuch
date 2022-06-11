use anyhow::Result;
use std::{env, time::Duration};
use tokio::time::sleep;
use worterbuch_cli::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let arg = env::args().skip(1).next();

    let request_pattern = match arg {
        Some(pattern) => pattern,
        None => {
            eprintln!("no subscription pattern specified");
            return Ok(());
        }
    };

    #[cfg(feature = "graphql")]
    let mut con = worterbuch_cli::gql::connect().await?;

    #[cfg(not(feature = "graphql"))]
    let mut con = worterbuch_cli::tcp::connect().await?;

    con.subscribe(&request_pattern)?;

    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
