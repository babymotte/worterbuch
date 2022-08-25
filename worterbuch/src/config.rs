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
    pub web_root_path: String,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        self.load_env_with_prefix("WORTERBUCH")
    }

    pub fn load_env_with_prefix(&mut self, prefix: &str) -> ConfigResult<()> {
        if let Ok(val) = env::var(prefix.to_owned() + "_SEPARATOR") {
            self.separator = to_separator(val)?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WILDCARD") {
            self.wildcard = to_wildcard(val)?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_MULTI_WILDCARD") {
            self.multi_wildcard = to_multi_wildcard(val)?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WEB_PORT") {
            self.web_port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_TCP_PORT") {
            self.tcp_port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_BIND_ADDRESS") {
            self.bind_addr = val.parse()?;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_USE_PERSISTENCE") {
            self.use_persistence = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PERSISTENCE_INTERVAL") {
            let secs = val.parse().as_interval()?;
            self.persistence_interval = Duration::from_secs(secs);
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_DATA_DIR") {
            self.data_dir = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_SINGLE_THREADED") {
            self.single_threaded = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_EXPLORER") {
            self.explorer = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WEBROOT_PATH") {
            self.web_root_path = val;
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
            web_root_path: "build".to_owned(),
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
