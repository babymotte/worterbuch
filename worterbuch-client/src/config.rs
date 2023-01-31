use std::env;
use worterbuch_common::error::{ConfigIntContext, ConfigResult};

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
            if let Ok(port) = val.parse().as_port() {
                self.port = port;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn new() -> ConfigResult<Self> {
        let proto = "ws".to_owned();
        let host_addr = "localhost".to_owned();
        let port = 8080;

        let mut config = Config {
            proto,
            host_addr,
            port,
        };

        config.load_env()?;

        Ok(config)
    }
}
