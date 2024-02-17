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

use clap::Parser;
use miette::IntoDiagnostic;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc};
use tokio_graceful_shutdown::{SubsystemHandle, Toplevel};
use worterbuch_client::topic;

/// A speedtest for worterbuch
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of worterbuch clients to use in the test (default is 1)
    #[arg(short, long)]
    clients: Option<usize>,

    /// Target message rate (default is 1000)
    #[arg(short, long)]
    rate: Option<usize>,
}

#[derive(Debug)]
struct Status {
    id: usize,
    sent_offset: u64,
    received_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct Stats {
    timestamps: HashMap<usize, Instant>,
    sent_offsets: HashMap<usize, u64>,
    received_offsets: HashMap<usize, u64>,

    delta_ts: HashMap<usize, Duration>,
    delta_sent_offsets: HashMap<usize, u64>,
    delta_received_offsets: HashMap<usize, u64>,

    lags: HashMap<usize, u64>,
}

impl Stats {
    fn process_status_update(&mut self, status: Status) {
        let now = Instant::now();
        let last_timestamp = self.timestamps.remove(&status.id);
        self.timestamps.insert(status.id, now);

        let last_sent_offset = self.sent_offsets.remove(&status.id);
        self.sent_offsets.insert(status.id, status.sent_offset);

        let last_received_offset = self.received_offsets.remove(&status.id);
        self.received_offsets
            .insert(status.id, status.received_offset);

        if let Some(last_timestamp) = last_timestamp {
            let delta_t = now.duration_since(last_timestamp);
            self.delta_ts.insert(status.id, delta_t);
        }

        if let Some(last_sent_offset) = last_sent_offset {
            let delta_sent_offset = status.sent_offset - last_sent_offset;
            self.delta_sent_offsets.insert(status.id, delta_sent_offset);
        }

        if let Some(last_received_offset) = last_received_offset {
            let delta_received_offset = status.received_offset - last_received_offset;
            self.delta_received_offsets
                .insert(status.id, delta_received_offset);
        }

        let lag = status.sent_offset - status.received_offset;
        self.lags.insert(status.id, lag);
    }

    fn log_stats(&self) {
        let total_delta_sent_offsets = self
            .delta_sent_offsets
            .values()
            .map(|i| *i)
            .reduce(|a, b| a + b)
            .unwrap_or(0);
        let total_delta_received_offsets = self
            .delta_received_offsets
            .values()
            .map(|i| *i)
            .reduce(|a, b| a + b)
            .unwrap_or(0);
        let total_delta_t = self
            .delta_ts
            .values()
            .map(|i| *i)
            .reduce(|a, b| a + b)
            .map(|it| it.as_millis())
            .unwrap_or(0);
        let average_delta_t = total_delta_t as f64 / self.delta_ts.len().max(1) as f64;
        let total_send_rate = (1000.0 * total_delta_sent_offsets as f64) / average_delta_t;
        let total_receive_rate = (1000.0 * total_delta_received_offsets as f64) / average_delta_t;
        let total_lag = self
            .lags
            .values()
            .map(|i: &u64| *i)
            .reduce(|a, b| a + b)
            .unwrap_or(0);

        log::info!(
            "Send rate: {} msg/s, receive rate: {} msg/s, lag: {} msgs",
            total_send_rate as u64,
            total_receive_rate as u64,
            total_lag as u64
        )
    }
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    Toplevel::new()
        .start("worterbuch-speedtest", move |s| run_speedtest(args, s))
        .catch_signals()
        .handle_shutdown_requests(Duration::from_millis(1000))
        .await
        .into_diagnostic()?;

    Ok(())
}

async fn run_speedtest(args: Args, subsys: SubsystemHandle) -> miette::Result<()> {
    let num_clients = args.clients.unwrap_or(1);
    let target_rate = args.rate.unwrap_or(1000);

    let (status_tx, mut status_rx) = mpsc::channel(num_clients);

    let target_rate = target_rate as f64 / num_clients as f64;

    for i in 0..num_clients {
        let result_tx = status_tx.clone();
        subsys.start(&format!("client-{i}"), move |s| {
            client(i, target_rate, result_tx.clone(), s)
        });
    }

    let mut stats = Stats::default();

    let mut stats_timer = tokio::time::interval(Duration::from_secs(1));

    loop {
        select! {
            status = status_rx.recv() => if let Some(status) = status {
                stats.process_status_update(status);
            } else {
                break;
            },
            _ = stats_timer.tick() => stats.log_stats(),
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn client(
    id: usize,
    target_rate: f64,
    result_tx: mpsc::Sender<Status>,
    subsys: SubsystemHandle,
) -> miette::Result<()> {
    let shutdown = subsys.clone();
    let (wb, _) =
        worterbuch_client::connect_with_default_config(async move { shutdown.request_shutdown() })
            .await
            .into_diagnostic()?;

    let mut sent_offset = 0u64;
    let mut received_offset = 0u64;

    let delay = Duration::from_micros((1_000_000f64 / target_rate) as u64);

    let key = topic!("speedtest/client", id, "offset");

    let (mut rx, _) = wb
        .subscribe::<u64>(key.clone(), false, true)
        .await
        .into_diagnostic()?;

    let mut delay_timer = tokio::time::interval(delay);
    let mut status_timer = tokio::time::interval(Duration::from_secs(1));

    loop {
        select! {
            recv = rx.recv() => if let Some(offset) = recv {
                if let Some(offset) = offset { received_offset = offset; }
            } else {
                break;
            },
            _ = status_timer.tick() => result_tx.send(Status { id, sent_offset, received_offset }).await.into_diagnostic()?,
            _ = delay_timer.tick() => {
                sent_offset += 1;
                wb.set(key.clone(), &sent_offset).await.into_diagnostic()?;
            }
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
