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

use anyhow::Result;
use clap::Parser;
use std::{io, time::Duration};
#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;
use tokio_graceful_shutdown::Toplevel;
use worterbuch::run_worterbuch;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
struct Args {}

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt().with_writer(io::stderr).init();
    let _args: Args = Args::parse();

    Toplevel::new()
        .start("worterbuch", run_worterbuch)
        .catch_signals()
        .handle_shutdown_requests(Duration::from_secs(10))
        .await?;

    Ok(())
}
