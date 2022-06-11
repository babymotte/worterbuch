use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use worterbuch_cli::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    #[cfg(feature = "graphql")]
    let mut con = worterbuch_cli::gql::connect().await?;

    #[cfg(not(feature = "graphql"))]
    let mut con = worterbuch_cli::tcp::connect().await?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(key)) = lines.next_line().await {
        con.get(&key)?;
    }

    Ok(())
}
