use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    DecodeError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::DecodeError(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::DecodeError(format!("{e}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
