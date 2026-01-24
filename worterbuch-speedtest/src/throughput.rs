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
use tosub::Subsystem;
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
        let total_delta_sent_offsets = self.delta_sent_offsets.values().sum::<u64>();
        let total_delta_received_offsets = self.delta_received_offsets.values().sum::<u64>();
        let total_delta_t = self.delta_ts.values().sum::<Duration>().as_millis();
        let average_delta_t = total_delta_t as f64 / self.delta_ts.len().max(1) as f64;
        let total_send_rate = (1000.0 * total_delta_sent_offsets as f64) / average_delta_t;
        let total_receive_rate = (1000.0 * total_delta_received_offsets as f64) / average_delta_t;
        let total_lag = self.lags.values().sum::<u64>();

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

struct ThroughputTest {
    subsys: Subsystem,
    status_tx: mpsc::UnboundedSender<Status>,
    current_agents: usize,
    current_target_rate: usize,
    agent_apis: Vec<mpsc::UnboundedSender<AgentApi>>,
    running: bool,
    ui_tx: mpsc::UnboundedSender<UiApi>,
    stats: Stats,
}

impl ThroughputTest {
    fn new(
        subsys: Subsystem,
        status_tx: mpsc::UnboundedSender<Status>,
        ui_tx: mpsc::UnboundedSender<UiApi>,
        stats: Stats,
    ) -> Self {
        Self {
            subsys,
            status_tx,
            current_agents: 0,
            current_target_rate: 1000,
            agent_apis: vec![],
            running: false,
            ui_tx,
            stats,
        }
    }

    async fn process_api_message(&mut self, api: Api) -> miette::Result<()> {
        match api {
            Api::Start(agents, target_rate) => self.start(agents, target_rate).await?,
            Api::Stop => self.stop().await?,
            Api::SetAgents(agents) => self.set_agents(agents).await?,
            Api::SetTargetRate(target_rate) => self.set_target_rate(target_rate).await?,
        }

        Ok(())
    }

    async fn start(&mut self, agents: usize, target_rate: usize) -> Result<(), miette::Error> {
        self.running = true;
        self.current_agents = agents;
        self.current_target_rate = target_rate;
        while self.agent_apis.len() < agents {
            self.spawn_agent(agents).await?;
        }
        while self.agent_apis.len() > agents {
            self.close_agent().await?;
        }
        let rate_per_agent = target_rate as f64 / agents as f64;
        for api in &self.agent_apis {
            api.send(AgentApi::Start(rate_per_agent))
                .into_diagnostic()?;
        }
        self.stats.set_agents(agents);
        self.ui_tx.send(UiApi::Running).into_diagnostic()?;
        Ok(())
    }

    async fn spawn_agent(&mut self, agents: usize) -> Result<(), miette::Error> {
        let i = self.agent_apis.len();
        let (agent_tx, agent_rx) = mpsc::unbounded_channel();
        let result_tx = self.status_tx.clone();
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        debug!("Spawning agent {i}");
        self.subsys.spawn(format!("client-{i}"), async move |s| {
            client(i, result_tx, agent_rx, s, conn_tx).await
        });
        self.agent_apis.push(agent_tx);
        let conn = conn_rx.recv().await;
        if conn.is_none() {
            error!("Connection result channel closed.");
            return Err(miette::miette!("Failed to spawn agent {i}."));
        }
        debug!("Agent {i} spawned.");
        self.ui_tx
            .send(UiApi::CreatingAgents(self.agent_apis.len(), agents))
            .into_diagnostic()
            .context("UI API channel closed.")?;
        Ok(())
    }

    async fn close_agent(&mut self) -> Result<(), miette::Error> {
        Ok(if let Some(api) = self.agent_apis.pop() {
            api.send(AgentApi::Close).into_diagnostic()?;
        })
    }

    async fn stop(&mut self) -> Result<(), miette::Error> {
        self.running = false;
        for api in &self.agent_apis {
            api.send(AgentApi::Stop).into_diagnostic()?;
        }
        self.ui_tx.send(UiApi::Stopped).into_diagnostic()?;
        Ok(())
    }

    async fn set_agents(&mut self, agents: usize) -> Result<(), miette::Error> {
        self.current_agents = agents;
        while self.agent_apis.len() < agents {
            self.spawn_agent(agents).await?;
        }
        while self.agent_apis.len() > agents {
            self.close_agent().await?;
        }
        if self.running {
            let rate_per_agent = self.current_target_rate as f64 / agents as f64;
            for api in &self.agent_apis {
                api.send(AgentApi::Start(rate_per_agent))
                    .into_diagnostic()?;
            }
        }
        self.stats.set_agents(agents);
        self.ui_tx
            .send(UiApi::Settings(Settings {
                agents: Some(agents),
                target_rate: Some(self.current_target_rate),
            }))
            .into_diagnostic()?;
        Ok(())
    }

    async fn set_target_rate(&mut self, target_rate: usize) -> Result<(), miette::Error> {
        self.current_target_rate = target_rate;

        let rate_per_agent = target_rate as f64 / self.current_agents as f64;
        for api in &self.agent_apis {
            api.send(AgentApi::SetTargetRate(rate_per_agent))
                .into_diagnostic()?;
        }
        self.ui_tx
            .send(UiApi::Settings(Settings {
                agents: Some(self.current_agents),
                target_rate: Some(target_rate),
            }))
            .into_diagnostic()?;
        Ok(())
    }

    async fn report_stats(&mut self) -> miette::Result<()> {
        self.stats.log_stats();
        self.ui_tx
            .send(UiApi::Stats(self.stats.summarize()))
            .into_diagnostic()?;
        self.ui_tx
            .send(UiApi::Settings(Settings {
                agents: Some(self.current_agents),
                target_rate: Some(self.current_target_rate),
            }))
            .into_diagnostic()?;
        if self.running {
            self.ui_tx.send(UiApi::Running).into_diagnostic()?;
        } else {
            self.ui_tx.send(UiApi::Stopped).into_diagnostic()?;
        }
        Ok(())
    }
}

pub async fn start_throughput_test<'a>(
    subsys: Subsystem,
    ui_tx: mpsc::UnboundedSender<UiApi>,
    mut api_rx: mpsc::UnboundedReceiver<Api>,
) -> miette::Result<()> {
    let (status_tx, mut status_rx) = mpsc::unbounded_channel();

    let stats = Stats::default();

    let mut stats_timer = tokio::time::interval(Duration::from_secs(1));

    let mut test = ThroughputTest::new(subsys.clone(), status_tx, ui_tx, stats);

    loop {
        select! {
            Some(api) = api_rx.recv() =>  test.process_api_message(api).await?,
            Some(status) = status_rx.recv() => test.stats.process_status_update(status),
            _ = stats_timer.tick() => test.report_stats().await?,
            _ = subsys.shutdown_requested() => break,
            else => break,
        }
    }

    Ok(())
}

async fn client(
    id: usize,
    result_tx: mpsc::UnboundedSender<Status>,
    mut api: mpsc::UnboundedReceiver<AgentApi>,
    subsys: Subsystem,
    on_connected: mpsc::UnboundedSender<()>,
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

    on_connected.send(()).ok();

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
                result_tx.send(Status { id, sent_offset, received_offset }).into_diagnostic().context("Failed to send status.")?;
            },
            _ = delay_timer.tick(), if !stopped => {

                if (sent_offset - received_offset) > 10_000 {
                    // Don't send more if we are too far ahead
                    continue;
                }

                sent_offset += 1;
                wb.set_async(key.clone(), &sent_offset).await.into_diagnostic()
                    .context("Failed to set value on worterbuch.")?;
            },
            _ = subsys.shutdown_requested() => {
                warn!("Shutdown requested.");
                break;
            },
        }
    }

    Ok(())
}
