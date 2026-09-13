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

use crate::license::{License, load_license};
use clap::{Parser, Subcommand};
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    env,
    net::{IpAddr, SocketAddr, TcpListener},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use strum::EnumString;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};
use tracing::debug;
use worterbuch_common::{
    Path,
    error::{ConfigError, ConfigIntContext, ConfigResult},
    protocol::v1::{AuthTokenKey, ProtocolVersion},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, EnumString)]
// #[serde(rename_all = "camelCase")]
pub enum PersistenceMode {
    Json,
    ReDB,
    SQLite,
    Turso,
}

#[derive(Parser, Debug, Clone, PartialEq, Serialize)]
#[command(author, version, about = "An in-memory data base / message broker hybrid", long_about = None)]
pub struct Args {
    /// Instance name
    #[arg(short = 'n', long, value_name = "NAME")]
    pub instance_name: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Serialize)]
pub enum Commands {
    /// Start server in leader mode
    Leader {
        /// Port at which to listen for cluster peer sync connections
        #[arg(long, short, value_name = "PORT", required = true)]
        sync_port: u16,
    },
    /// Start server in follower mode
    Follower {
        /// Socket address of the leader node to sync to
        #[arg(long, short, value_name = "HOST:PORT", required = true)]
        leader_address: String,
    },
    /// Start server in proxy mode
    Proxy {
        /// Socket addresses of potential leader nodes to sync to
        #[arg(long, short, value_name = "HOST:PORT", required = true)]
        leader_addresses: Vec<String>,
    },
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

#[cfg(target_family = "unix")]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnixEndpoint {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuicEndpoint {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub enum ClusterRole {
    #[default]
    Standalone,
    Leader {
        sync_port: u16,
    },
    Follower {
        leader_address: String,
    },
    Proxy {
        leader_addresses: Vec<String>,
    },
}

impl ClusterRole {
    pub fn is_orchestrated(&self) -> bool {
        matches!(
            self,
            ClusterRole::Leader { .. } | ClusterRole::Follower { .. }
        )
    }

    pub fn accept_client_connections(&self) -> bool {
        matches!(
            self,
            ClusterRole::Standalone | ClusterRole::Leader { .. } | ClusterRole::Proxy { .. }
        )
    }

    pub fn provide_server_metadata(&self) -> bool {
        matches!(self, ClusterRole::Standalone | ClusterRole::Leader { .. })
    }

    pub fn restore_from_persistence(&self) -> bool {
        matches!(self, ClusterRole::Standalone | ClusterRole::Leader { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Config {
    pub instance_name: Option<String>,
    pub ws_endpoint: Option<WsEndpoint>,
    pub tcp_endpoint: Option<Endpoint>,
    #[cfg(target_family = "unix")]
    pub unix_endpoint: Option<UnixEndpoint>,
    pub quic_endpoint: Option<QuicEndpoint>,
    pub use_persistence: bool,
    pub persistence_interval: Duration,
    pub persistence_mode: PersistenceMode,
    pub data_dir: Path,
    pub single_threaded: bool,
    pub web_root_path: Option<String>,
    pub keepalive_time: Option<Duration>,
    pub keepalive_interval: Option<Duration>,
    pub keepalive_retries: Option<u32>,
    pub send_timeout: Option<Duration>,
    pub channel_buffer_size: usize,
    pub extended_monitoring: bool,
    pub auth_token_key: Option<AuthTokenKey>,
    pub license: License,
    pub shutdown_timeout: Duration,
    pub role: ClusterRole,
    pub default_export_file_name: Option<String>,
    pub cors_allowed_origins: Option<Vec<String>>,
    pub print_endpoints: bool,
    pub ws_disabled: bool,
    pub tcp_disabled: bool,
    #[cfg(target_family = "unix")]
    pub unix_disabled: bool,
    pub quic_disabled: bool,
    pub exit_on_stdin_close: bool,
    pub license_file: Option<PathBuf>,
    pub initial_sync_timeout: Option<Duration>,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        self.load_env_with_prefix("WORTERBUCH")
    }

    pub fn load_env_with_prefix(&mut self, prefix: &str) -> ConfigResult<()> {
        if let Ok(val) = env::var(prefix.to_owned() + "_WS_TLS")
            && let Some(ep) = &mut self.ws_endpoint
        {
            ep.endpoint.tls = val.to_lowercase() == "true" || val == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WS_SERVER_PORT")
            && let Some(ep) = &mut self.ws_endpoint
        {
            ep.endpoint.port = val.parse().to_port()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WS_BIND_ADDRESS")
            && let Some(ep) = &mut self.ws_endpoint
        {
            ep.endpoint.bind_addr = val.parse()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PUBLIC_ADDRESS")
            && let Some(ep) = &mut self.ws_endpoint
        {
            ep.public_addr = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_TCP_SERVER_PORT")
            && let Some(ep) = &mut self.tcp_endpoint
        {
            ep.port = val.parse().to_port()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_TCP_BIND_ADDRESS")
            && let Some(ep) = &mut self.tcp_endpoint
        {
            ep.bind_addr = val.parse()?;
        }

        #[cfg(target_family = "unix")]
        if let Ok(val) = env::var(prefix.to_owned() + "_UNIX_SOCKET_PATH") {
            if let Some(ep) = &mut self.unix_endpoint {
                ep.path = val.into();
            } else {
                self.unix_endpoint = Some(UnixEndpoint { path: val.into() });
            }
        }

        let quic_cert_path = env::var(prefix.to_owned() + "_QUIC_CERT_PATH").ok();
        let quic_key_path = env::var(prefix.to_owned() + "_QUIC_KEY_PATH").ok();
        if let (Some(cert_path), Some(key_path)) = (quic_cert_path, quic_key_path) {
            if let Some(ep) = &mut self.quic_endpoint {
                ep.cert_path = cert_path.into();
                ep.key_path = key_path.into();
            } else {
                self.quic_endpoint = Some(QuicEndpoint {
                    bind_addr: [127, 0, 0, 1].into(),
                    port: 8082,
                    cert_path: cert_path.into(),
                    key_path: key_path.into(),
                });
            }
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_QUIC_SERVER_PORT")
            && let Some(ep) = &mut self.quic_endpoint
        {
            ep.port = val.parse().to_port()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_QUIC_BIND_ADDRESS")
            && let Some(ep) = &mut self.quic_endpoint
        {
            ep.bind_addr = val.parse()?;
        }

        if matches!(
            self.role,
            ClusterRole::Leader { .. } | ClusterRole::Follower { .. }
        ) {
            self.use_persistence = true;
        } else if let Ok(val) = env::var(prefix.to_owned() + "_USE_PERSISTENCE") {
            self.use_persistence = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PERSISTENCE_INTERVAL") {
            let secs = val.parse().to_interval()?;
            self.persistence_interval = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PERSISTENCE_MODE") {
            self.persistence_mode =
                PersistenceMode::from_str(&val).unwrap_or(PersistenceMode::Json);
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
            self.auth_token_key = Some(val);
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

        if let Ok(val) = env::var(prefix.to_owned() + "_PRINT_ENDPOINTS") {
            let enabled = val.to_lowercase();
            let enabled = enabled.trim();
            self.print_endpoints = enabled == "true" || enabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DISABLE_WS") {
            let disabled = val.to_lowercase();
            let disabled = disabled.trim();
            self.ws_disabled = disabled == "true" || disabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DISABLE_TCP") {
            let disabled = val.to_lowercase();
            let disabled = disabled.trim();
            self.tcp_disabled = disabled == "true" || disabled == "1";
        }

        #[cfg(target_family = "unix")]
        if let Ok(val) = env::var(prefix.to_owned() + "_DISABLE_UNIX") {
            let disabled = val.to_lowercase();
            let disabled = disabled.trim();
            self.unix_disabled = disabled == "true" || disabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DISABLE_QUIC") {
            let disabled = val.to_lowercase();
            let disabled = disabled.trim();
            self.quic_disabled = disabled == "true" || disabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_EXIT_ON_STDIN_CLOSE") {
            let enabled = val.to_lowercase();
            let enabled = enabled.trim();
            self.exit_on_stdin_close = enabled == "true" || enabled == "1";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_LICENSE_FILE") {
            self.license_file = Some(val.into());
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_INITIAL_SYNC_TIMEOUT") {
            let secs = val.parse().to_interval()?;
            self.initial_sync_timeout = Some(Duration::from_secs(secs));
        }

        debug!(
            "Config loaded from env:\n---\n{}",
            serde_yaml::to_string(&self).expect("could not serialize config")
        );

        Ok(())
    }

    pub async fn new(args: Option<Args>) -> ConfigResult<Self> {
        let mut config = Config {
            instance_name: None,
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
            quic_endpoint: None,
            use_persistence: false,
            persistence_interval: Duration::from_secs(30),
            persistence_mode: PersistenceMode::Json,
            data_dir: "./data".into(),
            single_threaded: false,
            web_root_path: None,
            keepalive_time: None,
            keepalive_interval: None,
            keepalive_retries: None,
            send_timeout: None,
            channel_buffer_size: 1_000,
            extended_monitoring: true,
            auth_token_key: None,
            license: License::default(),
            shutdown_timeout: Duration::from_secs(1),
            role: ClusterRole::Standalone,
            default_export_file_name: None,
            cors_allowed_origins: None,
            print_endpoints: false,
            ws_disabled: false,
            tcp_disabled: false,
            #[cfg(target_family = "unix")]
            unix_disabled: false,
            quic_disabled: false,
            exit_on_stdin_close: false,
            license_file: None,
            initial_sync_timeout: None,
        };
        config.load_env()?;
        if let Some(args) = args {
            config.apply_args(args);
        }

        match load_license(config.license_file.as_deref()).await {
            Ok(license) => {
                config.license = license;
                Ok(config)
            }
            Err(e) => Err(ConfigError::InvalidLicense(e.to_string())),
        }
    }

    fn apply_args(&mut self, args: Args) {
        match args.command {
            Some(Commands::Leader { sync_port }) => {
                self.role = ClusterRole::Leader { sync_port };
            }
            Some(Commands::Follower { leader_address }) => {
                self.role = ClusterRole::Follower { leader_address };
            }
            Some(Commands::Proxy { leader_addresses }) => {
                self.role = ClusterRole::Proxy { leader_addresses };
            }
            None => {
                self.role = ClusterRole::Standalone;
            }
        }

        self.instance_name = args.instance_name.clone();
    }

    pub fn persistence_interval(&self) -> Interval {
        let mut interval = interval_at(
            Instant::now() + self.persistence_interval,
            self.persistence_interval,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval
    }

    pub fn start_persistence_backend(&self) -> bool {
        // a follower's sole purpose is to write the leader's state to persistence, so it should always start the persistence backend
        self.use_persistence || matches!(self.role, ClusterRole::Follower { .. })
    }

    pub(crate) fn supported_client_protocol_versions(&self) -> Box<[ProtocolVersion]> {
        match self.role {
            ClusterRole::Standalone | ClusterRole::Leader { .. } | ClusterRole::Follower { .. } => {
                Box::new([
                    ProtocolVersion::new(0, 11),
                    ProtocolVersion::new(1, 1),
                    ProtocolVersion::new(2, 0),
                ])
            }
            ClusterRole::Proxy { .. } => Box::new([ProtocolVersion::new(2, 0)]),
        }
    }
}

#[derive(Serialize)]
enum EndpointAddress {
    Tcp { ip: IpAddr, port: u16 },
    Ws { ip: IpAddr, port: u16 },
    Quic { ip: IpAddr, port: u16 },
}

pub fn print_endpoint(listener: &TcpListener, tcp: bool) -> Result<(), miette::Error> {
    let addr = listener.local_addr().into_diagnostic()?;
    let addr = if tcp {
        EndpointAddress::Tcp {
            ip: addr.ip(),
            port: addr.port(),
        }
    } else {
        EndpointAddress::Ws {
            ip: addr.ip(),
            port: addr.port(),
        }
    };
    let json = json!(addr);
    println!("{json}");
    Ok(())
}

pub fn print_quic_endpoint(addr: SocketAddr) -> Result<(), miette::Error> {
    let addr = EndpointAddress::Quic {
        ip: addr.ip(),
        port: addr.port(),
    };
    let json = json!(addr);
    println!("{json}");
    Ok(())
}
