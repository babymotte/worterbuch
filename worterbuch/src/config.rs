use std::{env, net::IpAddr, time::Duration};
use worterbuch_common::{
    error::{ConfigIntContext, ConfigResult},
    Path,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub port: u16,
    pub proto: String,
    pub bind_addr: IpAddr,
    pub use_persistence: bool,
    pub persistence_interval: Duration,
    pub data_dir: Path,
    pub single_threaded: bool,
    pub webapp: bool,
    pub web_root_path: String,
    pub public_address: String,
}

impl Config {
    pub fn load_env(&mut self) -> ConfigResult<()> {
        self.load_env_with_prefix("WORTERBUCH")
    }

    pub fn load_env_with_prefix(&mut self, prefix: &str) -> ConfigResult<()> {
        if let Ok(val) = env::var(prefix.to_owned() + "_PROTO") {
            self.proto = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_SERVER_PORT") {
            self.port = val.parse().as_port()?;
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

        if let Ok(val) = env::var(prefix.to_owned() + "_WEBAPP") {
            self.webapp = val.to_lowercase() == "true";
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_WEBROOT_PATH") {
            self.web_root_path = val;
        }

        if let Ok(val) = env::var(prefix.to_owned() + "_PUBLIC_ADDRESS") {
            self.public_address = val;
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
            port: 8080,
            proto: "ws".to_owned(),
            bind_addr: [127, 0, 0, 1].into(),
            use_persistence: false,
            persistence_interval: Duration::from_secs(30),
            data_dir: "./data".into(),
            single_threaded: false,
            webapp: true,
            web_root_path: "html".to_owned(),
            public_address: "localhost".to_owned(),
        }
    }
}
