/*
 *  Entrypoint of the worterbuch server application
 *
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

use miette::{IntoDiagnostic, Result};
use std::env;
#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};
use worterbuch::{Config, run_worterbuch};

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    if env::var("WORTERBUCH_SINGLE_THREADED")
        .map(|v| v.to_ascii_lowercase())
        .as_deref()
        == Ok("true")
    {
        run_single_threaded()
    } else {
        run_multi_threaded()
    }
}

fn run_single_threaded() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_diagnostic()?
        .block_on(start())?;
    Ok(())
}

fn run_multi_threaded() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .into_diagnostic()?
        .block_on(start())?;
    Ok(())
}

async fn start() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = Config::new().await?;

    let cfg = config.clone();

    Toplevel::new(|s| async move {
        s.start(SubsystemBuilder::new("worterbuch", move |s| {
            run_worterbuch(s, config)
        }));
    })
    .catch_signals()
    .handle_shutdown_requests(cfg.shutdown_timeout)
    .await?;

    Ok(())
}
