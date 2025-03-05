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

use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use std::{thread, time::Instant};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{error, info};
use worterbuch_client::{
    ServerMessage, benchmark::generate_dummy_data, connect_with_default_config,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencySettings {
    pub publishers: usize,
    pub n_ary_keys: usize,
    pub key_length: u32,
    pub messages_per_key: usize,
    pub subscribers: usize,
    pub total_messages: usize,
}

#[derive(Debug, Clone)]
pub enum Api {
    Start(LatencySettings),
    Stop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UiApi {
    Running,
    Stopped,
}

pub async fn start_latency_test(
    subsys: SubsystemHandle,
    ui_tx: mpsc::Sender<UiApi>,
    mut api_rx: mpsc::Receiver<Api>,
) -> miette::Result<()> {
    let mut stop_tx = None;

    loop {
        select! {
            recv = api_rx.recv() => if let Some(msg) = recv {
                stop_tx = process_msg(msg, &ui_tx, stop_tx).await?;
            } else {
                break;
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn process_msg(
    msg: Api,
    ui_tx: &mpsc::Sender<UiApi>,
    current_stop_tx: Option<oneshot::Sender<()>>,
) -> miette::Result<Option<oneshot::Sender<()>>> {
    match msg {
        Api::Start(settings) => {
            let start = match &current_stop_tx {
                Some(tx) => tx.is_closed(),
                None => true,
            };

            if start {
                let (stop_tx, stop_rx) = oneshot::channel();
                spawn(run_latency_test(settings, ui_tx.clone(), stop_rx));
                Ok(Some(stop_tx))
            } else {
                Ok(current_stop_tx)
            }
        }
        Api::Stop => {
            if let Some(tx) = current_stop_tx {
                tx.send(()).ok();
            }
            Ok(None)
        }
    }
}

async fn run_latency_test(
    settings: LatencySettings,
    ui_tx: mpsc::Sender<UiApi>,
    mut stop_rx: oneshot::Receiver<()>,
) -> miette::Result<()> {
    info!("Starting latency test.");

    let mut stop_txs = vec![];
    let mut handles = vec![];

    let start = Instant::now();

    for _ in 0..settings.publishers {
        let (stop_tx, stop_rx) = oneshot::channel();
        stop_txs.push(stop_tx);
        let settings = settings.clone();
        handles.push(spawn(start_sender(settings, stop_rx)));
    }

    let mut counter = 0;

    for handle in handles {
        select! {
            set = handle => if let Ok(set) = set { counter += set },
            _ = &mut stop_rx => break,
        }
    }

    for tx in stop_txs {
        tx.send(()).ok();
    }

    let duration = start.elapsed();

    info!(
        "Test stopped after {} s ({} msg/s)",
        duration.as_secs_f32(),
        counter as f32 / duration.as_secs_f32()
    );

    ui_tx.send(UiApi::Stopped).await.into_diagnostic()?;

    Ok(())
}

async fn start_sender(settings: LatencySettings, mut stop_rx: oneshot::Receiver<()>) -> usize {
    let (dummy_tx, mut dummy_rx) = mpsc::channel(1000);

    let n_ary_keys = settings.n_ary_keys;
    let key_length = settings.key_length;
    let values_per_key = settings.messages_per_key;
    let data = generate_dummy_data(n_ary_keys, key_length, values_per_key);

    info!("Dummy data generated.");

    thread::spawn(move || {
        for (key, value) in data {
            if let Err(e) = dummy_tx.blocking_send((key.join("/"), value)) {
                error!("Error sending dummy data: {e}");
                break;
            }
        }
    });

    let (wb, mut on_disconnect) = match connect_with_default_config().await {
        Ok((wb, on_disconnect, _)) => (wb, on_disconnect),
        Err(e) => {
            error!("Could not start worterbuch client: {e}");
            return 0;
        }
    };

    let mut counter = 0;
    let mut tid_sent = 0;
    let mut tid_recieved = 0;

    let mut wb_rx = match wb.all_messages().await.into_diagnostic() {
        Ok(it) => it,
        Err(e) => {
            error!("Could not subscribe to all wb messages: {e}");
            return counter;
        }
    };

    loop {
        select! {
            recv = dummy_rx.recv() => if let Some(data) = recv {
                let key = format!("speedtest/latency/{}",data.0);
                let value = data.1;
                match wb.set(key, &value).await.into_diagnostic()  {
                    Ok(t) => tid_sent = t,
                    Err(e) => {
                        error!("Error setting value: {e}");
                        return counter;
                    }
                }
                counter += 1;
            } else {
                info!("All values set.");
                break;
            },
            msg = wb_rx.recv() => if let Some(msg) = msg {
                if let ServerMessage::Ack(ack) = msg {
                    tid_recieved = ack.transaction_id;
                }
            } else {
                return counter;
            },
            _ = &mut on_disconnect => return counter,
            _ = &mut stop_rx => return counter,
        }
    }

    info!("Waiting for acks.");

    while tid_recieved < tid_sent {
        if let Some(msg) = wb_rx.recv().await {
            if let ServerMessage::Ack(ack) = msg {
                tid_recieved = ack.transaction_id;
            }
        } else {
            break;
        }
    }

    wb.close().await.ok();

    counter
}
