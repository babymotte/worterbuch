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

use super::config::Config;
use miette::Result;
use tokio::{net::UdpSocket, select};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn lead(subsys: &SubsystemHandle, socket: &mut UdpSocket, config: &Config) -> Result<()> {
    // TODO
    // periodically send heartbeat request and evaluate responses
    // if fewer than quorum - 1 responses are received, assume there is a split brain and go back to follower mode
    log::info!("Starting worterbuch server in leader mode …");
    select! {
        // TODO
        // - start worterbuch server in leader mode
        // - open rest endpoint for clients to connect to to request leader socket addresses

        _ = subsys.on_shutdown_requested() => (),
    }

    Ok(())
}
