#[cfg(feature = "graphql")]
pub mod gql;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "ws")]
pub mod ws;

use crate::error::ConnectionError;

use super::codec::ServerMessage;
use super::error::ConnectionResult;
use tokio::sync::broadcast;

pub trait Connection {
    fn set(&mut self, key: &str, value: &str) -> std::result::Result<u64, ConnectionError>;
    fn get(&mut self, key: &str) -> ConnectionResult<u64>;
    fn pget(&mut self, key: &str) -> ConnectionResult<u64>;
    fn subscribe(&mut self, key: &str) -> ConnectionResult<u64>;
    fn psubscribe(&mut self, key: &str) -> ConnectionResult<u64>;
    fn export(&mut self, path: &str) -> ConnectionResult<u64>;
    fn import(&mut self, path: &str) -> ConnectionResult<u64>;
    fn responses(&mut self) -> broadcast::Receiver<ServerMessage>;
}
