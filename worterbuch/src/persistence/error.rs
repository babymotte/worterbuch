use crate::store::StoreError;
use miette::Diagnostic;
#[cfg(feature = "redb")]
use redb::{
    CommitError, CompactionError, DatabaseError, StorageError, TableError, TransactionError,
};
use std::io;
use thiserror::Error;
use tokio::sync::oneshot;
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
    #[cfg(feature = "redb")]
    #[error("redb database error: {0}")]
    RedbDatabaseBError(#[from] DatabaseError),
    #[cfg(feature = "redb")]
    #[error("redb transaction error: {0}")]
    RedbTransactioneError(#[from] TransactionError),
    #[cfg(feature = "redb")]
    #[error("redb table error: {0}")]
    RedbTableError(#[from] TableError),
    #[cfg(feature = "redb")]
    #[error("redb storage error: {0}")]
    RedbStorageError(#[from] StorageError),
    #[cfg(feature = "redb")]
    #[error("redb compaction error: {0}")]
    RedbCompactionError(#[from] CompactionError),
    #[cfg(feature = "redb")]
    #[error("redb commit error: {0}")]
    RedbCommitError(#[from] CommitError),
    #[cfg(feature = "sqlite")]
    #[error("sqlite error: {0}")]
    SqliteError(#[from] rusqlite::Error),
    #[cfg(feature = "turso")]
    #[error("turso error: {0}")]
    TursoError(#[from] turso::Error),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("internal api error")]
    ApiError(#[from] oneshot::error::RecvError),
}

pub type PersistenceResult<T> = Result<T, PersistenceError>;
