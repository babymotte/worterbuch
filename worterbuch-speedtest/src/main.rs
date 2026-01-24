/*
 *  Worterbuch speed test
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

mod latency;
mod throughput;
mod web_ui;

use latency::start_latency_test;
use miette::IntoDiagnostic;
use std::{io, time::Duration};
use throughput::start_throughput_test;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use web_ui::run_web_ui;

#[tokio::main]
async fn main() -> miette::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    Toplevel::new(async move |s| {
        s.start(SubsystemBuilder::new(
            "worterbuch-speedtest",
            run_speedtests_with_ui,
        ));
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_millis(1000))
    .await
    .into_diagnostic()?;

    Ok(())
}

async fn run_speedtests_with_ui(subsys: Subsystem) -> miette::Result<()> {
    let (throughput_api_tx, throughput_api_rx) = mpsc::unbounded_channel();
    let (latency_api_tx, latency_api_rx) = mpsc::unbounded_channel();
    let (throughput_ui_tx, throughput_ui_rx) = mpsc::unbounded_channel();
    let (latency_ui_tx, latency_ui_rx) = mpsc::unbounded_channel();

    subsys.spawn(
        "speedtest-web-ui",
        async |s| {
            run_web_ui(
                s,
                throughput_ui_rx,
                latency_ui_rx,
                throughput_api_tx,
                latency_api_tx,
            )
            .await
        },
    ));
    subsys.spawn(
        "throughput",
        async move |s| {
            start_throughput_test(s, throughput_ui_tx, throughput_api_rx).await
        },
    ));
    subsys.spawn(
        "latency",
        async move |s| start_latency_test(s, latency_ui_tx, latency_api_rx).await,
    ));

    Ok(())
}
