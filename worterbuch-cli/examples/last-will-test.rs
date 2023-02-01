use anyhow::Result;
use serde_json::json;
use std::process;
use tokio::io::{AsyncBufReadExt, BufReader};
use worterbuch_cli::app;
use worterbuch_client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (_matches, proto, host_addr, port, _json) = app(
        "last-will-test",
        "Test Wörterbuch's last will funciton.",
        vec![],
    )?;

    let on_disconnect = async move {
        log::warn!("Connection to server lost.");
        process::exit(1);
    };

    let mut con = connect(
        &proto,
        &host_addr,
        port,
        vec![("last_will/hello/world", json!("OFFLINE")).into()],
        vec![],
        on_disconnect,
    )
    .await?;

    con.set("last_will/hello/world".to_owned(), json!("ONLINE"))?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(value)) = lines.next_line().await {
        con.set("last_will/hello/world".to_owned(), json!(value))?;
    }

    Ok(())
}
