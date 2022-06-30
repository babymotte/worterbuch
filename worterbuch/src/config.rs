#[cfg(feature = "server")]
use std::net::IpAddr;
use std::{env, net::IpAddr, time::Duration};

use libworterbuch::{
    codec::Path,
    error::{ConfigError, ConfigIntContext, ConfigResult},
};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub separator: char,
    pub wildcard: char,
    pub multi_wildcard: char,
    #[cfg(feature = "tcp")]
    pub tcp_port: u16,
    #[cfg(feature = "graphql")]
    pub graphql_port: u16,
    #[cfg(feature = "web")]
    pub web_port: u16,
    #[cfg(feature = "web")]
    pub proto: String,
    pub bind_addr: IpAddr,
    #[cfg(feature = "web")]
    pub cert_path: Option<String>,
    #[cfg(feature = "web")]
    pub key_path: Option<String>,
    pub persistent_data: bool,
    pub persistence_interval: Duration,
    pub data_dir: Path,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        if let Ok(val) = env::var("WORTERBUCH_SEPARATOR") {
            self.separator = to_separator(val)?;
        }

        if let Ok(val) = env::var("WORTERBUCH_WILDCARD") {
            self.wildcard = to_wildcard(val)?;
        }

        if let Ok(val) = env::var("WORTERBUCH_MULTI_WILDCARD") {
            self.multi_wildcard = to_multi_wildcard(val)?;
        }

        #[cfg(feature = "web")]
        if let Ok(val) = env::var("WORTERBUCH_PROTO") {
            self.proto = val;
        }

        #[cfg(feature = "web")]
        if let Ok(val) = env::var("WORTERBUCH_WEB_PORT") {
            self.web_port = val.parse().as_port()?;
        }

        #[cfg(feature = "tcp")]
        if let Ok(val) = env::var("WORTERBUCH_TCP_PORT") {
            self.tcp_port = val.parse().as_port()?;
        }

        #[cfg(feature = "graphql")]
        if let Ok(val) = env::var("WORTERBUCH_GRAPHQL_PORT") {
            self.graphql_port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_BIND_ADDRESS") {
            self.bind_addr = val.parse()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_USE_PERSISTENCE") {
            self.persistent_data = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var("WORTERBUCH_PERSISTENCE_INTERVAL") {
            let secs = val.parse().as_interval()?;
            self.persistence_interval = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var("WORTERBUCH_DATA_DIR") {
            self.data_dir = val;
        }

        Ok(())
    }

    pub fn new() -> ConfigResult<Self> {
        let mut config = Config::default();
        config.load_env()?;
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            separator: '/',
            wildcard: '?',
            multi_wildcard: '#',
            #[cfg(feature = "tcp")]
            tcp_port: 4242,
            #[cfg(feature = "graphql")]
            graphql_port: 4243,
            #[cfg(feature = "web")]
            web_port: 8080,
            #[cfg(feature = "web")]
            proto: "ws".to_owned(),
            bind_addr: [127, 0, 0, 1].into(),
            #[cfg(feature = "web")]
            cert_path: None,
            #[cfg(feature = "web")]
            key_path: None,
            persistent_data: false,
            persistence_interval: Duration::from_secs(30),
            data_dir: "./data".into(),
        }
    }
}

fn to_separator(str: impl AsRef<str>) -> ConfigResult<char> {
    let str = str.as_ref();
    if str.len() != 1 {
        Err(ConfigError::InvalidSeparator(str.to_owned()))
    } else {
        if let Some(ch) = str.chars().next() {
            Ok(ch)
        } else {
            Err(ConfigError::InvalidSeparator(str.to_owned()))
        }
    }
}

fn to_wildcard(str: impl AsRef<str>) -> ConfigResult<char> {
    let str = str.as_ref();
    if str.len() != 1 {
        Err(ConfigError::InvalidWildcard(str.to_owned()))
    } else {
        if let Some(ch) = str.chars().next() {
            Ok(ch)
        } else {
            Err(ConfigError::InvalidWildcard(str.to_owned()))
        }
    }
}

fn to_multi_wildcard(str: impl AsRef<str>) -> ConfigResult<char> {
    let str = str.as_ref();
    if str.len() != 1 {
        Err(ConfigError::InvalidMultiWildcard(str.to_owned()))
    } else {
        if let Some(ch) = str.chars().next() {
            Ok(ch)
        } else {
            Err(ConfigError::InvalidMultiWildcard(str.to_owned()))
        }
    }
}
