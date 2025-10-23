use std::io;

use crate::persistence::error::PersistenceError;
use miette::Diagnostic;
use tokio::sync::oneshot;
use worterbuch_common::error::WorterbuchError;

#[derive(Debug, Diagnostic, thiserror::Error)]
pub enum WorterbuchAppError {
    #[error("Persistence error: {0}")]
    PersistenceError(#[from] PersistenceError),
    #[error("Worterbuch error: {0}")]
    WorterbuchError(#[from] WorterbuchError),
    #[error("Config error: {0}")]
    ConfigError(String),
    #[error("Cluster error: {0}")]
    ClusterError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("Channel error: {0}")]
    ChannelError(#[from] oneshot::error::RecvError),
}

pub type WorterbuchAppResult<T> = Result<T, WorterbuchAppError>;
