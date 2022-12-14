use anyhow::Result;
use std::process;
use tokio::io::{AsyncBufReadExt, BufReader};
use worterbuch_cli::app;
#[cfg(feature = "tcp")]
use worterbuch_client::tcp as wb;
#[cfg(feature = "ws")]
use worterbuch_client::ws as wb;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let (_matches, proto, host_addr, port, _json, debug) = app(
        "last-will-test",
        "Test Wörterbuch's last will funciton.",
        vec![],
    )?;

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    if debug {
        eprintln!("Server: {proto}://{host_addr}:{port}");
    }

    let mut con = wb::connect(
        &proto,
        &host_addr,
        port,
        vec![("last_will/hello/world", "OFFLINE").into()],
        vec![],
        on_disconnect,
    )
    .await?;

    con.set("last_will/hello/world", "ONLINE")?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(value)) = lines.next_line().await {
        con.set("last_will/hello/world", &value)?;
    }

    Ok(())
}
