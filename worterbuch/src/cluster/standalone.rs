/*
 *  Types and helper functions for standalone mode
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

use crate::{
    Config, Servers,
    cluster::{Mode, process_api_call, shutdown},
    error::WorterbuchAppResult,
    server::common::WbFunction,
    worterbuch::Worterbuch,
};
use serde_json::json;
use tokio::sync::mpsc;
use tosub::Subsystem;
use totils::CancelOn;
use tracing::info;
use worterbuch_common::{
    INTERNAL_CLIENT_ID,
    protocol::v1::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    topic,
};

pub async fn run(
    subsys: &Subsystem,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
) -> WorterbuchAppResult<()> {
    info!("Running in STANDALONE mode.");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Standalone),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    loop {
        let Some(recv) = api_rx
            .recv()
            .or_cancel_on(subsys.shutdown_requested())
            .await
        else {
            break;
        };
        match recv {
            Some(function) => process_api_call(&mut worterbuch, function).await,
            None => break,
        }
    }

    info!("Main loop stopped, shutting down.");

    shutdown(subsys, worterbuch, config, servers).await
}
