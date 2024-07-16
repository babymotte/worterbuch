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

use std::{env, time::Duration};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub proto: String,
    pub host_addr: String,
    pub port: Option<u16>,
    pub keepalive_timeout: Duration,
    pub send_timeout: Duration,
    pub connection_timeout: Duration,
    pub auth_token: Option<String>,
}

impl Config {
    pub fn load_env(&mut self) {
        if let Ok(val) = env::var("WORTERBUCH_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_HOST_ADDRESS") {
            self.host_addr = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_PORT") {
            if let Ok(port) = val.parse() {
                self.port = Some(port);
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_KEEPALIVE_TIMEOUT") {
            if let Ok(secs) = val.parse() {
                self.keepalive_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_SEND_TIMEOUT") {
            if let Ok(secs) = val.parse() {
                self.send_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_CONNECTION_TIMEOUT") {
            if let Ok(secs) = val.parse() {
                self.connection_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = env::var("WORTERBUCH_AUTH_TOKEN") {
            self.auth_token = Some(val);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let proto = "tcp".to_owned();
        let host_addr = "localhost".to_owned();
        let port = Some(8081);
        let keepalive_timeout = Duration::from_secs(5);
        let send_timeout = Duration::from_secs(5);
        let connection_timeout = Duration::from_secs(5);

        Config {
            proto,
            host_addr,
            port,
            keepalive_timeout,
            send_timeout,
            connection_timeout,
            auth_token: None,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        let mut config = Config::default();
        config.load_env();
        config
    }

    pub fn with_address(proto: String, host_addr: String, port: Option<u16>) -> Self {
        let mut config = Config::new();
        config.proto = proto;
        config.host_addr = host_addr;
        config.port = port;
        config
    }
}
