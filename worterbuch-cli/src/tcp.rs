use crate::Connection;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;

#[derive(Clone)]
pub struct TcpConnection;

impl Connection for TcpConnection {
    fn set(&self, key: &str, value: &str) -> Result<()> {
        todo!()
    }

    fn get(&self, key: &str) -> Result<String> {
        todo!()
    }

    fn subscribe(&self, key: &str) -> Result<Receiver<String>> {
        todo!()
    }
}

pub fn connect() -> Result<TcpConnection> {
    todo!()
}
