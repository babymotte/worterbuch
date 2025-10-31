use miette::Result;
use std::{io, time::Duration};
use tokio::{spawn, time::sleep};
use tracing_subscriber::EnvFilter;
use worterbuch_client::{Value, connect_with_default_config};
use worterbuch_common::topic;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (wb1, _, _) = connect_with_default_config().await?;
    let (wb2, _, _) = connect_with_default_config().await?;

    let (mut sub, _) = wb1
        .psubscribe::<Value>(topic!("some", "thing", "?"), true, false, None)
        .await?;

    spawn(async move {
        while let Some(event) = sub.recv().await {
            eprintln!("{event:?}");
        }
    });

    for i in 0..999 {
        wb2.set_async(topic!("some", "thing", i), i).await?;
        sleep(Duration::from_secs(1)).await;
        wb2.delete_async(topic!("some", "thing", i)).await?;
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
