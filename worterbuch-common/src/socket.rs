/*
 *  Worterbuch socket utils module
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

use miette::{Context, IntoDiagnostic};
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
use std::{
    net::{IpAddr, SocketAddr, TcpListener, UdpSocket},
    time::Duration,
};
use tracing::trace;

#[derive(Debug, Clone)]
pub struct TcpSocketConfig {
    pub keepalive_time: Option<Duration>,
    pub keepalive_interval: Option<Duration>,
    pub keepalive_retries: Option<u32>,
    pub send_timeout: Duration,
}

impl Default for TcpSocketConfig {
    fn default() -> Self {
        Self {
            keepalive_time: Some(Duration::from_secs(1)),
            keepalive_interval: Some(Duration::from_secs(1)),
            keepalive_retries: Some(5),
            send_timeout: Duration::from_secs(5),
        }
    }
}

pub fn create_tcp_server_socket(
    bind_addr: IpAddr,
    port: u16,
    config: TcpSocketConfig,
) -> miette::Result<TcpListener> {
    trace!(?bind_addr, %port);
    let addr: SocketAddr = SocketAddr::new(bind_addr, port);
    trace!(%addr);

    let mut tcp_keepalive = TcpKeepalive::new();
    if let Some(keepalive) = config.keepalive_time {
        tcp_keepalive = tcp_keepalive.with_time(keepalive);
    }
    if let Some(keepalive) = config.keepalive_interval {
        tcp_keepalive = tcp_keepalive.with_interval(keepalive);
    }
    if let Some(retries) = config.keepalive_retries {
        tcp_keepalive = tcp_keepalive.with_retries(retries);
    }

    let socket = Socket::new(domain_for_addr(addr), Type::STREAM, Some(Protocol::TCP))
        .into_diagnostic()
        .wrap_err("failed to create TCP server socket")?;

    #[cfg(not(target_os = "windows"))]
    socket
        .set_reuse_address(true)
        .into_diagnostic()
        .wrap_err("failed to set SO_REUSEADDR option")?;
    socket
        .set_nonblocking(true)
        .into_diagnostic()
        .wrap_err("failed to set nonblocking option")?;
    socket
        .set_keepalive(true)
        .into_diagnostic()
        .wrap_err("failed to set SO_KEEPALIVE option")?;
    socket
        .set_tcp_keepalive(&tcp_keepalive)
        .into_diagnostic()
        .wrap_err("failed to set TCP keepallive option")?;
    #[cfg(target_os = "linux")]
    socket
        .set_tcp_user_timeout(Some(config.send_timeout))
        .into_diagnostic()
        .wrap_err("failed to set TCP_USER_TIMEOUT option")?;
    socket
        .set_tcp_nodelay(true)
        .into_diagnostic()
        .wrap_err("failed to set TCP_NODELAY option")?;
    socket
        .bind(&SockAddr::from(addr))
        .into_diagnostic()
        .wrap_err_with(|| format!("failed to bind socket to address {addr}"))?;
    socket
        .listen(1024)
        .into_diagnostic()
        .wrap_err("creating client listener failed")?;

    Ok(socket.into())
}

pub fn create_udp_server_socket(bind_addr: IpAddr, port: u16) -> miette::Result<UdpSocket> {
    trace!(?bind_addr, %port);
    let addr: SocketAddr = SocketAddr::new(bind_addr, port);
    trace!(%addr);

    let socket = Socket::new(domain_for_addr(addr), Type::DGRAM, Some(Protocol::UDP))
        .into_diagnostic()
        .wrap_err("failed to create UDP server socket")?;

    #[cfg(not(target_os = "windows"))]
    socket
        .set_reuse_address(true)
        .into_diagnostic()
        .wrap_err("failed to set SO_REUSEADDR option")?;
    socket
        .set_nonblocking(true)
        .into_diagnostic()
        .wrap_err("failed to set nonblocking option")?;

    socket
        .bind(&SockAddr::from(addr))
        .into_diagnostic()
        .wrap_err_with(|| format!("failed to bind socket to address {addr}"))?;

    Ok(socket.into())
}

fn domain_for_addr(addr: SocketAddr) -> Domain {
    match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    }
}
