use crate::Connection;
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone)]
pub struct TcpConnection;

#[async_trait]
impl Connection for TcpConnection {
    fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        todo!()
    }

    fn get(&mut self, key: &str) -> Result<u64> {
        todo!()
    }

    fn subscribe(&mut self, key: &str) -> Result<u64> {
        todo!()
    }

    async fn wait_for_ticket(&self, ticket: u64) {
        todo!()
    }
}

pub fn connect() -> Result<TcpConnection> {
    todo!()
}
