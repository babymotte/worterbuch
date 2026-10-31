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

use crate::{
    PeerMessage, Ping,
    config::{self},
    socket::init_socket,
};
use miette::{Context, IntoDiagnostic, bail};
use serde_json::json;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    ops::ControlFlow,
    time::Duration,
};
use tosub::Subsystem;
use totils::while_select;
use tracing::{info, warn};

pub async fn run_udp_check(
    subsys: &Subsystem,
    addr: &str,
    config: &config::Config,
) -> miette::Result<()> {
    let udp_check_address: Vec<SocketAddr> = addr
        .to_socket_addrs()
        .into_diagnostic()
        .wrap_err("UDP check address is not a valid socket address")?
        .collect();

    let mut socket = init_socket(config)
        .await
        .wrap_err("failed to create raft UDP socket")?;

    let mut buf = [0u8; 65507];

    let mut interval = tokio::time::interval(Duration::from_secs(1));

    let mut counter = 0;

    while_select! {
        _ = subsys.shutdown_requested() => break,
        _ = interval.tick() => {
            send_ping(&mut socket, counter, &udp_check_address).await.wrap_err("failed to send ping")?;
            counter += 1;
            ControlFlow::Continue(())
        },
        recv = socket.recv(&mut buf) => receive_ping(recv, &buf).await.wrap_err("failed to receive ping")?,
    }

    Ok(())
}

async fn send_ping(
    socket: &mut tokio::net::UdpSocket,
    counter: usize,
    addrs: &[SocketAddr],
) -> miette::Result<()> {
    let max = 60;
    if counter > max {
        bail!("Max ping attempts ({max}) exceeded.");
    }

    let msg = PeerMessage::Ping(Ping(counter));
    let msg = json!(msg).to_string();

    for addr in addrs {
        info!("Sending ping message '{}' to {} …", msg, addr);
        socket
            .send_to(msg.as_bytes(), addr)
            .await
            .into_diagnostic()
            .wrap_err("failed to send UDP message")?;
    }

    Ok(())
}

async fn receive_ping(received: io::Result<usize>, buf: &[u8]) -> miette::Result<ControlFlow<()>> {
    let received = received
        .into_diagnostic()
        .wrap_err("failed to received UDP message")?;
    match serde_json::from_slice::<PeerMessage>(&buf[..received]) {
        Ok(msg) => {
            info!("Received peer message: {}", json!(msg));
            return Ok(ControlFlow::Break(()));
        }
        Err(e) => warn!("received invalid ping: {}", e),
    }
    Ok(ControlFlow::Continue(()))
}
