use std::{env, net::IpAddr, time::Duration};
use worterbuch_common::{
    error::{ConfigError, ConfigIntContext, ConfigResult},
    Path,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub separator: char,
    pub wildcard: char,
    pub multi_wildcard: char,
    pub tcp_port: u16,
    pub web_port: u16,
    pub proto: String,
    pub bind_addr: IpAddr,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub use_persistence: bool,
    pub persistence_interval: Duration,
    pub data_dir: Path,
    pub single_threaded: bool,
    pub explorer: bool,
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

        if let Ok(val) = env::var("WORTERBUCH_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_WEB_PORT") {
            self.web_port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_TCP_PORT") {
            self.tcp_port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_BIND_ADDRESS") {
            self.bind_addr = val.parse()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_USE_PERSISTENCE") {
            self.use_persistence = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var("WORTERBUCH_PERSISTENCE_INTERVAL") {
            let secs = val.parse().as_interval()?;
            self.persistence_interval = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var("WORTERBUCH_DATA_DIR") {
            self.data_dir = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_SINGLE_THREADED") {
            self.single_threaded = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var("WORTERBUCH_EXPLORER") {
            self.explorer = val.to_lowercase() == "true";
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
            tcp_port: 4242,
            web_port: 8080,
            proto: "ws".to_owned(),
            bind_addr: [127, 0, 0, 1].into(),
            cert_path: None,
            key_path: None,
            use_persistence: false,
            persistence_interval: Duration::from_secs(30),
            data_dir: "./data".into(),
            single_threaded: false,
            explorer: true,
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
