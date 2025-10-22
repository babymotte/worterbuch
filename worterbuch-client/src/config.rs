/*
 *  Worterbuch client config module
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

#[cfg(target_family = "unix")]
use std::path::PathBuf;
use std::{
    env,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    ops::Deref,
    time::Duration,
};
use tracing::{debug, error, instrument};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub proto: String,
    pub servers: Box<[SocketAddr]>,
    pub send_timeout: Option<Duration>,
    pub connection_timeout: Duration,
    pub auth_token: Option<String>,
    pub use_backpressure: bool,
    pub channel_buffer_size: usize,
    #[cfg(target_family = "unix")]
    pub socket_path: Option<PathBuf>,
}

impl Config {
    pub fn load_env(&mut self) {
        if let Ok(val) = env::var("WORTERBUCH_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_SERVERS") {
            self.servers = val
                .split(',')
                .map(str::trim)
                .filter_map(|s| s.to_socket_addrs().ok())
                .flatten()
                .collect();
        } else if let Ok(val) = env::var("WORTERBUCH_HOST_ADDRESS") {
            let default_port = match self.proto.to_lowercase().deref() {
                "ws" | "wss" | "http" | "https" => 8080,
                _ => 8081,
            };
            let port = if let Ok(val) = env::var("WORTERBUCH_PORT") {
                if let Ok(port) = val.parse() {
                    port
                } else {
                    default_port
                }
            } else {
                default_port
            };
            if let Ok(addr) = format!("{val}:{port}").to_socket_addrs() {
                self.servers = addr.collect();
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_SEND_TIMEOUT") {
            if let Ok(secs) = val.parse() {
                self.send_timeout = Some(Duration::from_secs(secs));
            } else {
                error!("invalid timeout: {val}");
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_CONNECTION_TIMEOUT") {
            if let Ok(secs) = val.parse() {
                self.connection_timeout = Duration::from_secs(secs);
            } else {
                error!("invalid timeout: {val}");
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_AUTH_TOKEN") {
            self.auth_token = Some(val);
        }

        if let Ok(val) = env::var("WORTERBUCH_CHANNEL_BUFFER_SIZE") {
            if let Ok(size) = val.parse() {
                self.channel_buffer_size = size;
            } else {
                error!("invalid buffer size: {val}");
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_USE_BACKPRESSURE") {
            let val = val.to_lowercase() == "true";
            self.use_backpressure = val;
        }

        #[cfg(target_family = "unix")]
        {
            if let Ok(val) = env::var("WORTERBUCH_UNIX_SOCKET_PATH") {
                self.socket_path = Some(PathBuf::from(val));
            }
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let proto = "tcp".to_owned();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            8081,
        ))]
        .into();
        let send_timeout = None;
        let connection_timeout = Duration::from_secs(5);
        let channel_buffer_size = 1;
        let use_backpressure = true;
        #[cfg(target_family = "unix")]
        let socket_path = Some("/tmp/worterbuch.socket".into());

        Config {
            proto,
            servers,
            send_timeout,
            connection_timeout,
            auth_token: None,
            channel_buffer_size,
            use_backpressure,
            #[cfg(target_family = "unix")]
            socket_path,
        }
    }
}

impl Config {
    #[instrument]
    pub fn new() -> Self {
        debug!("Creating default config");
        let mut config = Config::default();
        debug!("Loading environment variables");
        config.load_env();
        config
    }

    #[instrument]
    pub fn with_servers(proto: String, servers: Box<[SocketAddr]>) -> Self {
        let mut config = Config::new();
        config.proto = proto;
        config.servers = servers;
        config
    }
}
