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

use std::time::Duration;

use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use tokio::{select, spawn, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencySettings {
    pub publishers: usize,
    pub n_ary_keys: usize,
    pub key_length: usize,
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
    loop {
        select! {
            recv = api_rx.recv() => {
                log::warn!("{recv:?}");
                ui_tx.send(UiApi::Running).await.into_diagnostic()?;
                let ui_tx = ui_tx.clone();
                // TODO
                spawn(async move {
                    sleep(Duration::from_secs(2)).await;
                    if let Err(e) = ui_tx.send(UiApi::Stopped).await {
                        log::error!("Error sending test results: {e}");
                    }
                });
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
