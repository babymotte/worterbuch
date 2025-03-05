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

use crate::web_ui::Settings;
use miette::{Context, IntoDiagnostic};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, warn};
use worterbuch_client::topic;

#[derive(Debug)]
struct Status {
    id: usize,
    sent_offset: u64,
    received_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Stats {
    timestamps: HashMap<usize, Instant>,
    sent_offsets: HashMap<usize, u64>,
    received_offsets: HashMap<usize, u64>,

    delta_ts: HashMap<usize, Duration>,
    delta_sent_offsets: HashMap<usize, u64>,
    delta_received_offsets: HashMap<usize, u64>,

    lags: HashMap<usize, u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Severity {
    Green,
    Yellow,
    Red,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatSummary {
    send_rate: u64,
    receive_rate: u64,
    lag: u64,
    severity: Severity,
}

impl Stats {
    fn set_agents(&mut self, agents: usize) {
        if agents < self.timestamps.len() {
            for i in agents..self.timestamps.len() {
                self.timestamps.remove(&i);
                self.sent_offsets.remove(&i);
                self.received_offsets.remove(&i);
                self.delta_ts.remove(&i);
                self.delta_sent_offsets.remove(&i);
                self.delta_received_offsets.remove(&i);
                self.lags.remove(&i);
            }
        }
    }

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
            let delta_sent_offset = status.sent_offset - last_sent_offset.min(status.sent_offset);
            self.delta_sent_offsets.insert(status.id, delta_sent_offset);
        }

        if let Some(last_received_offset) = last_received_offset {
            let delta_received_offset =
                status.received_offset - last_received_offset.min(status.received_offset);
            self.delta_received_offsets
                .insert(status.id, delta_received_offset);
        }

        let lag = status.sent_offset - status.received_offset;
        self.lags.insert(status.id, lag);
    }

    fn summarize(&self) -> StatSummary {
        let total_delta_sent_offsets = self
            .delta_sent_offsets
            .values()
            .copied()
            .reduce(|a, b| a + b)
            .unwrap_or(0);
        let total_delta_received_offsets = self
            .delta_received_offsets
            .values()
            .copied()
            .reduce(|a, b| a + b)
            .unwrap_or(0);
        let total_delta_t = self
            .delta_ts
            .values()
            .copied()
            .reduce(|a, b| a + b)
            .map(|it| it.as_millis())
            .unwrap_or(0);
        let average_delta_t = total_delta_t as f64 / self.delta_ts.len().max(1) as f64;
        let total_send_rate = (1000.0 * total_delta_sent_offsets as f64) / average_delta_t;
        let total_receive_rate = (1000.0 * total_delta_received_offsets as f64) / average_delta_t;
        let total_lag = self
            .lags
            .values()
            .copied()
            .reduce(|a, b| a + b)
            .unwrap_or(0);

        // TODO evaluate severity

        StatSummary {
            send_rate: total_send_rate as u64,
            receive_rate: total_receive_rate as u64,
            lag: total_lag,
            severity: Severity::Green,
        }
    }

    fn log_stats(&self) {
        let summary = self.summarize();

        debug!(
            "Send rate: {} msg/s, receive rate: {} msg/s, lag: {} msgs",
            summary.send_rate, summary.receive_rate, summary.lag
        )
    }
}

pub enum Api {
    Start(usize, usize),
    SetAgents(usize),
    SetTargetRate(usize),
    Stop,
}

pub enum AgentApi {
    Start(f64),
    SetTargetRate(f64),
    Stop,
    Close,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UiApi {
    Running,
    Stopped,
    Stats(StatSummary),
    Settings(Settings),
    CreatingAgents(usize, usize),
}

pub async fn start_throughput_test(
    subsys: SubsystemHandle,
    ui_tx: mpsc::Sender<UiApi>,
    mut api_rx: mpsc::Receiver<Api>,
) -> miette::Result<()> {
    let (status_tx, mut status_rx) = mpsc::channel(1024);

    let mut stats = Stats::default();

    let mut stats_timer = tokio::time::interval(Duration::from_secs(1));

    let mut current_agents = 0;
    let mut current_target_rate = 1000;
    let mut agent_apis = vec![];

    let mut running = false;

    loop {
        select! {
            api = api_rx.recv() => if let Some(api) = api {
                match api {
                    Api::Start(agents, target_rate) => {
                        running = true;
                        current_agents = agents;
                        current_target_rate = target_rate;

                        'outer: while agent_apis.len() < agents {
                            let i = agent_apis.len();
                            let (agent_tx, agent_rx) = mpsc::channel(1);
                            let result_tx = status_tx.clone();
                            let (conn_tx, mut conn_rx) = mpsc::channel(1);
                            debug!("Spawning agent {i}");
                            subsys.start(SubsystemBuilder::new(format!("client-{i}"), move |s| client(i, result_tx, agent_rx, s, conn_tx)));
                            agent_apis.push(agent_tx);
                            'inner: loop {
                                select! {
                                    conn = conn_rx.recv() => {
                                        if conn.is_none() {
                                            error!("Connection result channel closed.");
                                            break 'outer;
                                        }
                                        break 'inner;
                                    },
                                    _ = status_rx.recv() => {
                                        debug!("Dropping status update ...");
                                    },
                                }
                            }
                            debug!("Agent {i} spawned.");
                            ui_tx.send(UiApi::CreatingAgents(agent_apis.len(), agents),).await.into_diagnostic().context("UI API channel closed.")?;
                        }

                        while  agent_apis.len() > agents {
                            if let Some(api) = agent_apis.pop() {
                                api.send(AgentApi::Close).await.into_diagnostic()?;
                            }
                        }

                        for api in &agent_apis {
                            api.send(AgentApi::Start(target_rate as f64 / agents as f64)).await.into_diagnostic()?;
                        }

                        stats.set_agents(agents);
                        ui_tx.send(UiApi::Running).await.into_diagnostic()?;
                    },
                    Api::Stop => {
                        running = false;
                        for api in &agent_apis {
                            api.send(AgentApi::Stop).await.into_diagnostic()?;
                        }
                        ui_tx.send(UiApi::Stopped).await.into_diagnostic()?;
                    },
                    Api::SetAgents(agents) => {
                        current_agents = agents;

                        'outer: while agent_apis.len() < agents {
                            let i = agent_apis.len();
                            let (agent_tx, agent_rx) = mpsc::channel(1);
                            let result_tx = status_tx.clone();
                            let (conn_tx, mut conn_rx) = mpsc::channel(1);
                            debug!("Spawning agent {i}");
                            subsys.start(SubsystemBuilder::new(format!("client-{i}"), move |s| client(i, result_tx, agent_rx, s, conn_tx)));
                            agent_apis.push(agent_tx);
                            'inner: loop {
                                select! {
                                    conn = conn_rx.recv() => {
                                        if conn.is_none() {
                                            error!("Connection result channel closed.");
                                            break 'outer;
                                        }
                                        break 'inner;
                                    },
                                    _ = status_rx.recv() => {
                                        debug!("Dropping status update ...");
                                    },
                                }
                            }
                            debug!("Agent {i} spawned.");
                            ui_tx.send(UiApi::CreatingAgents(agent_apis.len(), agents),).await.into_diagnostic().context("UI API channel closed.")?;
                        }

                        while  agent_apis.len() > agents {
                            if let Some(api) = agent_apis.pop() {
                                api.send(AgentApi::Close).await.into_diagnostic()?;
                            }
                        }

                        if running {
                            for api in &agent_apis {
                                api.send(AgentApi::Start(current_target_rate as f64 / agents as f64)).await.into_diagnostic()?;
                            }
                        }

                        stats.set_agents(agents);
                        ui_tx.send(UiApi::Settings(Settings { agents: Some(agents), target_rate: Some(current_target_rate) })).await.into_diagnostic()?;
                    },
                    Api::SetTargetRate(target_rate) => {
                        current_target_rate = target_rate;
                        for api in &agent_apis {
                            api.send(AgentApi::SetTargetRate(target_rate as f64 / current_agents as f64)).await.into_diagnostic()?;
                        }

                        ui_tx.send(UiApi::Settings(Settings { agents: Some(current_agents), target_rate: Some(target_rate) })).await.into_diagnostic()?;
                    },
                }
            } else {
                break;
            },
            status = status_rx.recv() => if let Some(status) = status {
                stats.process_status_update(status);
            } else {
                break;
            },
            _ = stats_timer.tick() => {
                stats.log_stats();
                ui_tx.send(UiApi::Stats(stats.summarize())).await.into_diagnostic()?;
                ui_tx.send(UiApi::Settings(Settings { agents: Some(current_agents), target_rate: Some(current_target_rate) })).await.into_diagnostic()?;
                if running {
                    ui_tx.send(UiApi::Running).await.into_diagnostic()?;
                } else {
                    ui_tx.send(UiApi::Stopped).await.into_diagnostic()?;
                }
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn client(
    id: usize,
    result_tx: mpsc::Sender<Status>,
    mut api: mpsc::Receiver<AgentApi>,
    subsys: SubsystemHandle,
    on_connected: mpsc::Sender<()>,
) -> miette::Result<()> {
    let (wb, _on_disconnect, _) = worterbuch_client::connect_with_default_config()
        .await
        .into_diagnostic()
        .context("Failed to connect to worterbuch server.")?;

    let mut sent_offset = 0u64;
    let mut received_offset = 0u64;

    let key = topic!("speedtest/throughput/client", id, "offset");

    let (mut rx, _) = wb
        .subscribe::<u64>(key.clone(), false, true)
        .await
        .into_diagnostic()
        .context("Failed to subscribe to agent key.")?;

    let mut delay_timer = tokio::time::interval(Duration::from_secs(1));
    let mut status_timer = tokio::time::interval(Duration::from_secs(1));

    let mut stopped = true;

    on_connected.send(()).await.ok();

    loop {
        select! {
            api = api.recv() => if let Some(api) = api {
                match api {
                    AgentApi::Start(target_rate) => {
                        stopped = false;
                        drop(delay_timer);
                        let delay = Duration::from_micros((1_000_000f64 / target_rate) as u64);
                        delay_timer = tokio::time::interval(delay);
                    },
                    AgentApi::SetTargetRate(target_rate) => {
                        drop(delay_timer);
                        let delay = Duration::from_micros((1_000_000f64 / target_rate) as u64);
                        delay_timer = tokio::time::interval(delay);
                    },
                    AgentApi::Stop => {
                        stopped = true;
                        drop(delay_timer);
                        delay_timer = tokio::time::interval(Duration::from_secs(1));
                    },
                    AgentApi::Close => {
                        drop(delay_timer);
                        drop(status_timer);
                        break;
                    }
                }
            } else {
                warn!("Agent API channel closed.");
                break;
            },
            recv = rx.recv() => if let Some(offset) = recv {
                if let Some(offset) = offset { received_offset = offset; }
            } else {
                warn!("Worterbuch subscription channel closed.");
                break;
            },
            _ = status_timer.tick() => {
                result_tx.send(Status { id, sent_offset, received_offset }).await.into_diagnostic().context("Failed to send status.")?;
            },
            _ = delay_timer.tick(), if !stopped => {
                sent_offset += 1;
                wb.set(key.clone(), &sent_offset).await.into_diagnostic()
                    .context("Failed to set value on worterbuch.")?;
            },
            _ = subsys.on_shutdown_requested() => {
                warn!("Shutdown requested.");
                break;
            },
        }
    }

    Ok(())
}
