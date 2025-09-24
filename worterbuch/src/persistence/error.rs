use miette::Diagnostic;
use std::io;
use thiserror::Error;
use worterbuch_common::error::WorterbuchError;

#[derive(Debug, Error, Diagnostic)]
pub enum PersistenceError {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    #[error("(De-)Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Store is locked")]
    StoreLocked,
    #[error("Worterbuch error: {0}")]
    WorterbuchError(#[from] WorterbuchError),
    #[error("Written data is different from data to be written")]
    DataMismatch,
}

pub type PersistenceResult<T> = Result<T, PersistenceError>;
