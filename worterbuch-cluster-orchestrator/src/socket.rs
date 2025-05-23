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
use miette::{Context, IntoDiagnostic, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::UdpSocket;
use tracing::info;

// #[instrument(ret, err)]
pub async fn init_socket(config: &Config) -> Result<UdpSocket> {
    let bind_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let port = config.raft_port;

    let addr = SocketAddr::new(bind_addr, port);

    info!("Creating node socket at {addr} …");

    let sock = UdpSocket::bind(addr)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("error opening UDP socket at {addr}"))?;

    // TODO configure socket

    Ok(sock)
}
