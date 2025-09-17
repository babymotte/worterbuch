use std::io;

use thiserror::Error;
use worterbuch_common::error::WorterbuchError;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Illegal multi-level wildcard in key: {0}")]
    IllegalMultiWildcard(String),
    #[error("Could not serialize/deserialize JSON: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[cfg(feature = "rocksdb")]
    #[error("RocksDB error: {0}")]
    RocksDbError(#[from] rocksdb::Error),
    #[error("CAS version mismatch")]
    CasVersionMismatch,
    #[error("Value is CAS protected")]
    Cas,
}

impl From<StoreError> for WorterbuchError {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::IllegalMultiWildcard(pattern) => {
                WorterbuchError::IllegalMultiWildcard(pattern)
            }
            StoreError::CasVersionMismatch => WorterbuchError::CasVersionMismatch,
            StoreError::Cas => WorterbuchError::Cas,
            StoreError::SerdeJsonError(e) => WorterbuchError::SerDeError(
                e,
                "Could not serialize/deserialize JSON from/to persistence".to_owned(),
            ),
            #[cfg(feature = "rocksdb")]
            StoreError::RocksDbError(e) => WorterbuchError::IoError(
                io::Error::other(e),
                "read/write error in rocksdb".to_owned(),
            ),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;
