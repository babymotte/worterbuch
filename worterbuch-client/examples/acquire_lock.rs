use miette::{IntoDiagnostic, Result};
use std::{io, time::Duration};
use tokio::{spawn, time::sleep};
use tracing::info;
use tracing_subscriber::EnvFilter;
use worterbuch_client::connect_with_default_config;
use worterbuch_common::topic;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (wb1, _, _) = connect_with_default_config().await?;
    let (wb2, _, _) = connect_with_default_config().await?;

    let thread = spawn(async move {
        sleep(Duration::from_millis(500)).await;

        info!("Client 2 tries to acquire lock.");
        wb2.acquire_lock(topic!("hello", "world")).await?;
        info!("Client 2 has acquired the lock.");

        sleep(Duration::from_secs(3)).await;

        info!("Client 2 releases the lock.");
        wb2.release_lock(topic!("hello", "world")).await?;
        info!("Client 2 has released the lock.");

        Ok::<(), miette::Error>(())
    });

    info!("Client 1 tries to acquire lock.");
    wb1.acquire_lock(topic!("hello", "world")).await?;
    info!("Client 1 has acquired the lock.");

    sleep(Duration::from_secs(3)).await;

    info!("Client 1 releases the lock.");
    wb1.release_lock(topic!("hello", "world")).await?;
    info!("Client 1 has released the lock.");

    thread.await.into_diagnostic()??;

    Ok(())
}
