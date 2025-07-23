/*
 *  Worterbuch configuration module
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
    license::{License, load_license},
    telemetry,
};
use clap::Parser;
use serde::Serialize;
use std::{env, net::IpAddr, path::PathBuf, time::Duration};
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};
use tracing::debug;
use worterbuch_common::{
    AuthToken, Path,
    error::{ConfigError, ConfigIntContext, ConfigResult},
};

#[derive(Parser)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
pub struct Args {
    /// Start server in leader mode (requires --sync-port)
    #[arg(
        long,
        conflicts_with = "follower",
        requires = "sync_port",
        default_value_t = false
    )]
    leader: bool,
    /// Start server in follower mode (requires --leader_address)
    #[arg(
        long,
        conflicts_with = "leader",
        requires = "leader_address",
        default_value_t = false
    )]
    follower: bool,
    /// Port at which to listen for cluster peer sync connections (only with --leader)
    #[arg(long, short)]
    sync_port: Option<u16>,
    /// Socket address of the leader node to sync to in the form <IP or hostname>:<port> (only with --follower)
    #[arg(long, short)]
    leader_address: Option<String>,
    /// Instance name
    #[arg(short = 'n', long, env = "WORTERBUCH_INSTANCE_NAME")]
    instance_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Endpoint {
    pub tls: bool,
    pub bind_addr: IpAddr,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WsEndpoint {
    pub endpoint: Endpoint,
    pub public_addr: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnixEndpoint {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Config {
    pub ws_endpoint: Option<WsEndpoint>,
    pub tcp_endpoint: Option<Endpoint>,
    #[cfg(target_family = "unix")]
    pub unix_endpoint: Option<UnixEndpoint>,
    pub use_persistence: bool,
    pub persistence_interval: Duration,
    pub data_dir: Path,
    pub single_threaded: bool,
    pub web_root_path: Option<String>,
    pub keepalive_time: Option<Duration>,
    pub keepalive_interval: Option<Duration>,
    pub keepalive_retries: Option<u32>,
    pub send_timeout: Option<Duration>,
    pub channel_buffer_size: usize,
    pub extended_monitoring: bool,
    pub auth_token: Option<AuthToken>,
    pub license: License,
    pub shutdown_timeout: Duration,
    pub leader: bool,
    pub follower: bool,
    pub sync_port: Option<u16>,
    pub leader_address: Option<String>,
    pub default_export_file_name: Option<String>,
    pub cors_allowed_origins: Option<Vec<String>>,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        self.load_env_with_prefix("WORTERBUCH")
    }

    pub fn load_env_with_prefix(&mut self, prefix: &str) -> ConfigResult<()> {
        if let Ok(val) = env::var(prefix.to_owned() + "_WS_TLS") {
            if let Some(ep) = &mut self.ws_endpoint {
                ep.endpoint.tls = val.to_lowercase() == "true" || val == "1";
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WS_SERVER_PORT") {
            if let Some(ep) = &mut self.ws_endpoint {
                ep.endpoint.port = val.parse().to_port()?;
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WS_BIND_ADDRESS") {
            if let Some(ep) = &mut self.ws_endpoint {
                ep.endpoint.bind_addr = val.parse()?;
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PUBLIC_ADDRESS") {
            if let Some(ep) = &mut self.ws_endpoint {
                ep.public_addr = val;
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_TCP_SERVER_PORT") {
            if let Some(ep) = &mut self.tcp_endpoint {
                ep.port = val.parse().to_port()?;
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_TCP_BIND_ADDRESS") {
            if let Some(ep) = &mut self.tcp_endpoint {
                ep.bind_addr = val.parse()?;
            }
        }

        #[cfg(target_family = "unix")]
        if let Ok(val) = env::var(prefix.to_owned() + "_UNIX_SOCKET_PATH") {
            if let Some(ep) = &mut self.unix_endpoint {
                ep.path = val.into();
            } else {
                self.unix_endpoint = Some(UnixEndpoint { path: val.into() });
            }
        }

        if self.follower || self.leader {
            self.use_persistence = true;
        } else if let Ok(val) = env::var(prefix.to_owned() + "_USE_PERSISTENCE") {
            self.use_persistence = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PERSISTENCE_INTERVAL") {
            let secs = val.parse().to_interval()?;
            self.persistence_interval = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DATA_DIR") {
            self.data_dir = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_SINGLE_THREADED") {
            self.single_threaded = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WEBROOT_PATH") {
            self.web_root_path = Some(val);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_KEEPALIVE_TIME") {
            let secs = val.parse().to_interval()?;
            self.keepalive_time = Some(Duration::from_secs(secs));
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_KEEPALIVE_INTERVAL") {
            let secs = val.parse().to_interval()?;
            self.keepalive_interval = Some(Duration::from_secs(secs));
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_KEEPALIVE_RETRIES") {
            let val = val.parse().to_interval()?;
            self.keepalive_retries = Some(val);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_SEND_TIMEOUT") {
            let secs = val.parse().to_interval()?;
            self.send_timeout = Some(Duration::from_secs(secs));
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_CHANNEL_BUFFER_SIZE") {
            let size = val.parse::<usize>().to_interval()?.max(1);
            self.channel_buffer_size = size;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_EXTENDED_MONITORING") {
            let enabled = val.to_lowercase();
            let enabled = enabled.trim();
            self.extended_monitoring = enabled == "true" || enabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_AUTH_TOKEN") {
            self.auth_token = Some(val);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_SHUTDOWN_TIMEOUT") {
            let secs = val.parse().to_interval()?;
            self.shutdown_timeout = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DEFAULT_EXPORT_FILE_NAME") {
            self.default_export_file_name = Some(val);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_CORS_ALLOWED_ORIGINS") {
            self.cors_allowed_origins = Some(val.split(",").map(|v| v.trim().to_owned()).collect());
        }

        debug!(
            "Config loaded from env:\n---\n{}",
            serde_yaml::to_string(&self).expect("could not serialize config")
        );

        Ok(())
    }

    pub async fn new() -> ConfigResult<Self> {
        let args = Args::parse();
        let hostname = hostname::get()?;
        let cluster_role = if args.leader {
            Some("leader".to_owned())
        } else if args.follower {
            Some("follower".to_owned())
        } else {
            None
        };
        telemetry::init(
            args.instance_name
                .unwrap_or_else(|| hostname.to_string_lossy().into_owned()),
            cluster_role,
        )
        .await?;

        match load_license().await {
            Ok(license) => {
                let mut config = Config {
                    ws_endpoint: Some(WsEndpoint {
                        endpoint: Endpoint {
                            tls: false,
                            bind_addr: [127, 0, 0, 1].into(),
                            port: 8080,
                        },
                        public_addr: "localhost".to_owned(),
                    }),
                    tcp_endpoint: Some(Endpoint {
                        tls: false,
                        bind_addr: [127, 0, 0, 1].into(),
                        port: 8081,
                    }),
                    #[cfg(target_family = "unix")]
                    unix_endpoint: None,
                    use_persistence: false,
                    persistence_interval: Duration::from_secs(30),
                    data_dir: "./data".into(),
                    single_threaded: false,
                    web_root_path: None,
                    keepalive_time: None,
                    keepalive_interval: None,
                    keepalive_retries: None,
                    send_timeout: None,
                    channel_buffer_size: 1_000,
                    extended_monitoring: true,
                    auth_token: None,
                    license,
                    shutdown_timeout: Duration::from_secs(1),
                    follower: args.follower,
                    leader: args.leader,
                    sync_port: args.sync_port,
                    leader_address: args.leader_address,
                    default_export_file_name: None,
                    cors_allowed_origins: None,
                };
                config.load_env()?;
                Ok(config)
            }
            Err(e) => Err(ConfigError::InvalidLicense(e.to_string())),
        }
    }

    pub fn persistence_interval(&self) -> Interval {
        let mut interval = interval_at(
            Instant::now() + self.persistence_interval,
            self.persistence_interval,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval
    }
}
