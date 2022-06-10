use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use worterbuch_cli::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    #[cfg(feature = "graphql")]
    let mut con = worterbuch_cli::gql::connect().await?;

    #[cfg(not(feature = "graphql"))]
    let con = worterbuch_cli::tcp::connect()?;

    let mut ticket = 0;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(index) = line.find('=') {
            let key = &line[..index];
            let val = &line[index + 1..];
            ticket = con.set(key, val)?;
            if ticket % 1000 == 0 {
                eprintln!("{ticket}");
            }
        } else {
            eprintln!("no key/value pair (e.g. 'a=b'): {}", line);
        }
    }

    eprintln!("all tasks sent, waiting for tickets to complete");

    con.wait_for_ticket(ticket).await;

    Ok(())
}
