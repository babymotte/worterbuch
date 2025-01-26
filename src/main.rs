/*
 *  Copyright (C) 2025 Michael Bachmann
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

mod cluster_orchestrator;

use miette::Result;
use std::{io, time::Duration};
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .init();
    Toplevel::new(|s| async move {
        s.start(SubsystemBuilder::new(
            "cluster-orchestrator",
            cluster_orchestrator::run,
        ));
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_secs(5))
    .await?;

    Ok(())
}
