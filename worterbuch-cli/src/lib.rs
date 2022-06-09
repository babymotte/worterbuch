use anyhow::Result;
use async_trait::async_trait;

#[cfg(feature = "graphql")]
pub mod gql;

#[cfg(not(feature = "graphql"))]
pub mod tcp;

#[async_trait]
pub trait Connection {
    async fn set(&mut self, key: &str, value: &str) -> Result<()>;
    async fn get(&mut self, key: &str) -> Result<()>;
    async fn subscribe(&mut self, key: &str) -> Result<()>;
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Command {
    Init,
    Get(String, u64),
    Set(String, String, u64),
    Subscrube(String, u64),
}
