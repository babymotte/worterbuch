#[cfg(feature = "graphql")]
pub mod gql;
#[cfg(feature = "tcp")]
pub mod tcp;
pub mod utils;
#[cfg(feature = "ws")]
pub mod ws;

use anyhow::Result;
use libworterbuch::codec::ServerMessage;
use tokio::sync::broadcast;

pub trait Connection {
    fn set(&mut self, key: &str, value: &str) -> Result<u64>;
    fn get(&mut self, key: &str) -> Result<u64>;
    fn pget(&mut self, key: &str) -> Result<u64>;
    fn subscribe(&mut self, key: &str) -> Result<u64>;
    fn psubscribe(&mut self, key: &str) -> Result<u64>;
    fn export(&mut self, path: &str) -> Result<u64>;
    fn import(&mut self, path: &str) -> Result<u64>;
    fn responses(&mut self) -> broadcast::Receiver<ServerMessage>;
}
