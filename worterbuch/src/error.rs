use libworterbuch::codec::RequestPattern;
use std::{fmt, io};

#[derive(Debug)]
pub enum WorterbuchError {
    IllegalWildcard(RequestPattern),
    IllegalMultiWildcard(RequestPattern),
    MultiWildcardAtIllegalPosition(RequestPattern),
    IoError(io::Error),
    Other(anyhow::Error),
}

impl std::error::Error for WorterbuchError {}

impl fmt::Display for WorterbuchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorterbuchError::IllegalWildcard(rp) => {
                write!(f, "Key contains illegal wildcard: {rp}")
            }
            WorterbuchError::IllegalMultiWildcard(rp) => {
                write!(f, "Key contains illegal multi-wildcard: {rp}")
            }
            WorterbuchError::MultiWildcardAtIllegalPosition(rp) => {
                write!(f, "Key contains multi-wildcard at illegal position: {rp}")
            }
            WorterbuchError::IoError(e) => e.fmt(f),
            WorterbuchError::Other(e) => e.fmt(f),
        }
    }
}

impl From<io::Error> for WorterbuchError {
    fn from(e: io::Error) -> Self {
        WorterbuchError::IoError(e)
    }
}

impl From<anyhow::Error> for WorterbuchError {
    fn from(e: anyhow::Error) -> Self {
        WorterbuchError::Other(e)
    }
}

pub type WorterbuchResult<T> = std::result::Result<T, WorterbuchError>;
