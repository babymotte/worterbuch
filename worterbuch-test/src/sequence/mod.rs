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

//! Checks that the server processes the requests of a client in the order in which the client sent them.
//!
//! Every client subscribes to its own key and then repeatedly sends bursts of SET requests with increasing values to
//! that key without waiting for acks in between. Subscription events reflect the order in which the server applied the
//! SETs, so a value that is smaller than its predecessor means two requests were processed out of order. Acks must
//! arrive in the order the requests were sent and every SET must produce exactly one subscription event.

use crate::client::connect_tcp_client;
use miette::{Context, IntoDiagnostic, bail, miette};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::tcp::OwnedWriteHalf,
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tosub::{CancelOnShutdown, Subsystem};
use tracing::{debug, info, warn};
use worterbuch_client::{
    Ack, ClientMessage, Err, Key, ServerMessage, Set, State, StateEvent, Subscribe, TransactionId,
    config::Config,
};

const SUBSCRIPTION_TID: TransactionId = 1;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const TRAILING_EVENTS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Default)]
pub struct SequenceTestResult {
    /// number of SET requests sent
    pub sets: u64,
    /// number of subscription events received
    pub events: u64,
    /// number of subscription events whose value was smaller than that of a previous event
    pub out_of_order_events: u64,
    /// number of acks that did not arrive in the order the requests were sent
    pub out_of_order_acks: u64,
    pub run_duration: Duration,
}

impl SequenceTestResult {
    pub fn missing_events(&self) -> u64 {
        self.sets.saturating_sub(self.events)
    }

    pub fn passed(&self) -> bool {
        self.out_of_order_events == 0 && self.out_of_order_acks == 0 && self.events == self.sets
    }

    fn merge(&mut self, other: &SequenceTestResult) {
        self.sets += other.sets;
        self.events += other.events;
        self.out_of_order_events += other.out_of_order_events;
        self.out_of_order_acks += other.out_of_order_acks;
        self.run_duration = self.run_duration.max(other.run_duration);
    }
}

pub struct SequenceTest {
    subsys: Subsystem<Option<SequenceTestResult>>,
    clients: usize,
    burst: u64,
    duration: Duration,
    key_prefix: Key,
    client_config: Config,
}

impl SequenceTest {
    pub fn new(
        subsys: &Subsystem,
        clients: usize,
        burst: u64,
        duration: Duration,
        key_prefix: Key,
        client_config: Config,
    ) -> Option<Subsystem<Option<SequenceTestResult>>> {
        subsys.spawn("sequence-test", move |subsys| {
            Self {
                subsys,
                clients,
                burst,
                duration,
                key_prefix,
                client_config,
            }
            .run()
        })
    }

    async fn run(self) -> miette::Result<Option<SequenceTestResult>> {
        let mut clients = Vec::new();

        for id in 0..self.clients {
            let client = SequenceTestClient {
                id,
                key: format!("{}/{id}", self.key_prefix),
                burst: self.burst,
                duration: self.duration,
                client_config: self.client_config.clone(),
                result: SequenceTestResult::default(),
                last_value: None,
                next_ack: SUBSCRIPTION_TID + 1,
                acks: 0,
            };
            // the client reports its result through a channel instead of failing its subsystem, since a failed
            // subsystem would shut down the whole application before the result can be reported
            let (result_tx, result_rx) = oneshot::channel();
            if self
                .subsys
                .spawn(format!("sequence-client-{id}"), move |s| async move {
                    let _ = result_tx.send(client.run(s).await);
                })
                .is_none()
            {
                return Ok(None);
            }
            clients.push((id, result_rx));
        }

        let mut result = SequenceTestResult::default();
        for (id, client) in clients {
            let Some(res) = client.or_cancel_on_shutdown(&self.subsys).await else {
                return Ok(None);
            };
            let res = res
                .into_diagnostic()
                .wrap_err_with(|| format!("sequence test client {id} stopped unexpectedly"))?;
            match res.wrap_err_with(|| format!("sequence test client {id} failed"))? {
                Some(client_result) => result.merge(&client_result),
                None => return Ok(None),
            }
        }

        Ok(Some(result))
    }
}

struct SequenceTestClient {
    id: usize,
    key: Key,
    burst: u64,
    duration: Duration,
    client_config: Config,
    result: SequenceTestResult,
    last_value: Option<u64>,
    next_ack: TransactionId,
    acks: u64,
}

impl SequenceTestClient {
    async fn run(mut self, subsys: Subsystem) -> miette::Result<Option<SequenceTestResult>> {
        let (mut writer, mut lines) = connect_tcp_client(&subsys, self.id, &self.client_config)
            .await
            .wrap_err_with(|| format!("client {} could not connect", self.id))?;

        let subscribe = ClientMessage::Subscribe(Subscribe {
            transaction_id: SUBSCRIPTION_TID,
            key: self.key.clone(),
            unique: Some(false),
            live_only: Some(true),
            send_traces: Some(false),
        });
        send(&mut writer, &[subscribe]).await?;
        if !self.receive_subscribe_ack(&subsys, &mut lines).await? {
            return Ok(None);
        }
        debug!(self.id, self.key, "subscribed");

        let start = Instant::now();
        let mut next_tid = SUBSCRIPTION_TID + 1;
        while start.elapsed() < self.duration {
            let sets = (next_tid..next_tid + self.burst)
                .map(|tid| {
                    ClientMessage::Set(Set {
                        transaction_id: tid,
                        key: self.key.clone(),
                        value: json!(tid),
                    })
                })
                .collect::<Vec<_>>();
            send(&mut writer, &sets).await?;
            next_tid += self.burst;
            self.result.sets += self.burst;

            // wait for the whole burst to be acked, so that buffers never fill up and only ordering is tested
            while self.acks < self.result.sets {
                if !self
                    .receive_next(&subsys, &mut lines, RESPONSE_TIMEOUT)
                    .await?
                {
                    return Ok(None);
                }
            }
        }
        self.result.run_duration = start.elapsed();

        // subscription events may arrive after the corresponding acks
        let deadline = Instant::now() + TRAILING_EVENTS_TIMEOUT;
        while self.result.events < self.result.sets {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!(
                    self.id,
                    "{} subscription events missing",
                    self.result.missing_events()
                );
                break;
            }
            match self.receive_next(&subsys, &mut lines, remaining).await {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(_) => break,
            }
        }

        info!(
            "Client {}: {} sets, {} events, {} out of order events, {} out of order acks",
            self.id,
            self.result.sets,
            self.result.events,
            self.result.out_of_order_events,
            self.result.out_of_order_acks
        );

        Ok(Some(self.result))
    }

    async fn receive_subscribe_ack(
        &mut self,
        subsys: &Subsystem,
        lines: &mut mpsc::Receiver<String>,
    ) -> miette::Result<bool> {
        let Some(line) = receive_line(subsys, lines, RESPONSE_TIMEOUT).await? else {
            return Ok(false);
        };
        match parse(&line)? {
            ServerMessage::Ack(Ack { transaction_id }) if transaction_id == SUBSCRIPTION_TID => {
                Ok(true)
            }
            msg => Err(miette!("expected subscription ack, got {msg:?}")),
        }
    }

    /// Processes the next message from the server. Returns `false` if shutdown was requested.
    async fn receive_next(
        &mut self,
        subsys: &Subsystem,
        lines: &mut mpsc::Receiver<String>,
        timeout: Duration,
    ) -> miette::Result<bool> {
        let Some(line) = receive_line(subsys, lines, timeout).await? else {
            return Ok(false);
        };

        match parse(&line)? {
            ServerMessage::Ack(Ack { transaction_id }) => {
                if transaction_id != self.next_ack {
                    warn!(
                        self.id,
                        "expected ack for transaction {}, got {transaction_id}", self.next_ack
                    );
                    self.result.out_of_order_acks += 1;
                }
                self.next_ack = self.next_ack.max(transaction_id + 1);
                self.acks += 1;
            }
            ServerMessage::State(State {
                transaction_id: SUBSCRIPTION_TID,
                event: StateEvent::Value(value),
                ..
            }) => {
                let Some(value) = value.as_u64() else {
                    bail!("unexpected value in subscription event: {value}");
                };
                if self.last_value.is_some_and(|last| value <= last) {
                    warn!(
                        self.id,
                        "value {value} was applied after {}",
                        self.last_value.unwrap_or_default()
                    );
                    self.result.out_of_order_events += 1;
                }
                self.last_value = self.last_value.max(Some(value));
                self.result.events += 1;
            }
            ServerMessage::Err(Err {
                transaction_id,
                error_code,
                metadata,
            }) => {
                bail!(
                    "server returned error {error_code:?} for transaction {transaction_id}: {metadata}"
                );
            }
            msg => {
                bail!("received unexpected message from server: {msg:?}");
            }
        }

        Ok(true)
    }
}

/// Writes all messages and flushes once, so that they arrive at the server pipelined.
async fn send(
    writer: &mut BufWriter<OwnedWriteHalf>,
    msgs: &[ClientMessage],
) -> miette::Result<()> {
    for msg in msgs {
        let mut line = serde_json::to_string(msg).into_diagnostic()?;
        line.push('\n');
        writer.write_all(line.as_bytes()).await.into_diagnostic()?;
    }
    writer
        .flush()
        .await
        .into_diagnostic()
        .wrap_err("error sending requests to server")
}

/// Returns `None` if shutdown was requested.
async fn receive_line(
    subsys: &Subsystem,
    lines: &mut mpsc::Receiver<String>,
    response_timeout: Duration,
) -> miette::Result<Option<String>> {
    select! {
        biased;
        _ = subsys.shutdown_requested() => Ok(None),
        recv = timeout(response_timeout, lines.recv()) => match recv {
            Ok(Some(line)) => Ok(Some(line)),
            Ok(None) => Err(miette!("connection to server closed")),
            Err(_) => Err(miette!("server did not respond within {response_timeout:?}")),
        },
    }
}

fn parse(line: &str) -> miette::Result<ServerMessage> {
    serde_json::from_str(line)
        .into_diagnostic()
        .wrap_err("received invalid data from server")
}
