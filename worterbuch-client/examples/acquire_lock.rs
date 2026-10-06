/*
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use miette::{IntoDiagnostic, Result, bail};
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
    bail!("Simulated error")
}
