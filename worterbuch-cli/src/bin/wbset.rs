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
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(index) = line.find('=') {
            let key = &line[..index];
            let val = &line[index + 1..];
            con.set(key, val)?;
        } else {
            eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
        }
    }

    // TODO keep TCP connection open until all requests have been ACKed

    Ok(())
}
