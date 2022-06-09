use anyhow::Result;
use std::io::{self, BufRead};
use worterbuch_cli::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    #[cfg(feature = "graphql")]
    let mut con = worterbuch_cli::gql::connect().await?;

    #[cfg(not(feature = "graphql"))]
    let con = worterbuch_cli::tcp::connect()?;

    for line in io::stdin().lock().lines() {
        let key = line?;
        con.get(&key).await?
    }
    Ok(())
}
