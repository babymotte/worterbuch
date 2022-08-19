use std::env;
use worterbuch_common::error::{ConfigError, ConfigIntContext, ConfigResult};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub proto: String,
    pub host_addr: String,
    pub port: u16,
    pub separator: char,
    pub wildcard: char,
    pub multi_wildcard: char,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        if let Ok(val) = env::var("WORTERBUCH_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_HOST_ADDRESS") {
            self.host_addr = val;
        }

        if let Ok(val) = env::var("WORTERBUCH_PORT") {
            self.port = val.parse().as_port()?;
        }

        if let Ok(val) = env::var("WORTERBUCH_SEPARATOR") {
            self.separator = to_separator(val)?;
        }

        if let Ok(val) = env::var("WORTERBUCH_WILDCARD") {
            self.wildcard = to_wildcard(val)?;
        }

        if let Ok(val) = env::var("WORTERBUCH_MULTI_WILDCARD") {
            self.multi_wildcard = to_multi_wildcard(val)?;
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
        #[cfg(all(feature = "ws", feature = "graphql"))]
        eprintln!(
            "Warning: Conflicting features 'ws' and 'graphql' are active, config may be inconsistent!"
        );

        #[cfg(all(feature = "ws", feature = "tcp"))]
        eprintln!(
            "Warning: Conflicting features 'ws' and 'tcp' are active, config may be inconsistent!"
        );

        #[cfg(all(feature = "graphql", feature = "tcp"))]
        eprintln!(
            "Warning: Conflicting features 'graphql' and 'tcp' are active, config may be inconsistent!"
        );

        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        #[cfg(any(feature = "ws", feature = "graphql"))]
        let _proto = "ws".to_owned();
        #[cfg(feature = "tcp")]
        let _proto = "tcp".to_owned();
        #[cfg(not(any(feature = "tcp", feature = "ws", feature = "graphql")))]
        let _proto = "".to_owned();

        let host_addr = "localhost".to_owned();

        #[cfg(feature = "graphql")]
        let _port = 4243;
        #[cfg(feature = "ws")]
        let _port = 8080;
        #[cfg(feature = "tcp")]
        let _port = 4242;
        #[cfg(not(any(feature = "tcp", feature = "ws", feature = "graphql")))]
        let _port = 0;

        Config {
            proto: _proto,
            host_addr,
            port: _port,
            separator: '/',
            wildcard: '?',
            multi_wildcard: '#',
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
