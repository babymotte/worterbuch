use crate::error::ConfigResult;
use std::env;

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub proto: String,
    pub host_addr: String,
    pub port: u16,
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
            self.port = val.parse()?;
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
        }
    }
}
