use crate::utils::to_char;
use anyhow::{Context, Result};
use std::{env, net::IpAddr};

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub separator: char,
    pub wildcard: char,
    pub multi_wildcard: char,
    pub port: u16,
    pub bind_addr: IpAddr,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Config {
    pub fn load_env(&mut self) -> Result<()> {
        if let Ok(val) = env::var("WORTERBUCH_SEPARATOR") {
            self.separator = to_char(val).context("separator must be a single char")?;
        }

        if let Ok(val) = env::var("WORTERBUCH_WILDCARD") {
            self.wildcard = to_char(val).context("wildcard must be a single char")?;
        }

        if let Ok(val) = env::var("WORTERBUCH_MULTI_WILDCARD") {
            self.multi_wildcard = to_char(val).context("multi-wildcard must be a single char")?;
        }

        if let Ok(val) = env::var("WORTERBUCH_BIND_ADDRESS") {
            let ip: IpAddr = val.parse()?;
            self.bind_addr = ip;
        }

        Ok(())
    }

    // fn load_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
    //     todo!(
    //         "loading config from file {:?} not implemented yet",
    //         path.as_ref()
    //     )
    // }

    // pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
    //     let mut config = Config::default();
    //     config.load_file(path)?;
    //     config.load_env()?;
    //     Ok(config)
    // }

    pub fn new() -> Result<Self> {
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
            port: 4242,
            bind_addr: [127, 0, 0, 1].into(),
            cert_path: None,
            key_path: None,
        }
    }
}
