use std::fmt;

#[derive(Debug)]
pub struct DecodeError {
    msg: String,
}

impl DecodeError {
    pub fn with_message(msg: String) -> DecodeError {
        DecodeError { msg }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl<E: std::error::Error> From<E> for DecodeError {
    fn from(e: E) -> Self {
        DecodeError {
            msg: format!("{e}"),
        }
    }
}

pub type DecodeResult<T> = std::result::Result<T, DecodeError>;
