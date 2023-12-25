use std::{env, time::Duration};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub proto: String,
    pub host_addr: String,
    pub port: u16,
    pub keepalive_timeout: Duration,
    pub send_timeout: Duration,
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
                self.port = port;
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

        if let Ok(val) = env::var("WORTERBUCH_AUTH_TOKEN") {
            self.auth_token = Some(val);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let proto = "ws".to_owned();
        let host_addr = "localhost".to_owned();
        let port = 8080;
        let keepalive_timeout = Duration::from_secs(5);
        let send_timeout = Duration::from_secs(5);

        Config {
            proto,
            host_addr,
            port,
            keepalive_timeout,
            send_timeout,
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

    pub fn with_address(proto: String, host_addr: String, port: u16) -> Self {
        let mut config = Config::new();
        config.proto = proto;
        config.host_addr = host_addr;
        config.port = port;
        config
    }
}
