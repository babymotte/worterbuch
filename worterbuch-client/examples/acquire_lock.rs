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

        // If possible, avoid using this approach and use `locked` instead.
        // If you unintentionally return early from this function, the lock will not be released.
        info!("Client 2 tries to acquire lock.");
        wb2.acquire_lock(topic!("hello", "world")).await?;
        info!("Client 2 has acquired the lock.");

        sleep(Duration::from_secs(3)).await;

        // if an error occurs here, the lock will not be released.
        do_something_that_might_fail()?;

        info!("Client 2 releases the lock.");
        wb2.release_lock(topic!("hello", "world")).await?;
        info!("Client 2 has released the lock.");

        Ok::<(), miette::Error>(())
    });

    {
        info!("Client 1 tries to acquire lock.");

        wb1.locked(topic!("hello", "world"), async || {
            info!("Client 1 has acquired the lock.");

            sleep(Duration::from_secs(3)).await;

            // if an error occurs here, the lock will still be released.
            do_something_that_might_fail()?;

            info!("Client 1 releases the lock.");

            Ok::<(), miette::Error>(())
        })
        .await??;
    }

    thread.await.into_diagnostic()??;

    Ok(())
}

fn do_something_that_might_fail() -> Result<()> {
    // Uncomment to simulate an error.
    // _fail()?;

    Ok(())
}

fn _fail() -> Result<()> {
    eprintln!("Oops, something went wrong!");
    Err(miette::miette!("Simulated error"))
}
