use anyhow::Result;
use tokio::sync::broadcast;

#[cfg(feature = "graphql")]
pub mod gql;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "ws")]
pub mod ws;

pub trait Connection {
    fn set(&mut self, key: &str, value: &str) -> Result<u64>;
    fn get(&mut self, key: &str) -> Result<u64>;
    fn pget(&mut self, key: &str) -> Result<u64>;
    fn subscribe(&mut self, key: &str) -> Result<u64>;
    fn psubscribe(&mut self, key: &str) -> Result<u64>;
    fn export(&mut self, path: &str) -> Result<u64>;
    fn import(&mut self, path: &str) -> Result<u64>;
    fn acks(&mut self) -> broadcast::Receiver<u64>;
}

pub enum Command {
    Init,
    Get(String, u64),
    PGet(String, u64),
    Set(String, String, u64),
    Subscrube(String, u64),
    PSubscrube(String, u64),
}
