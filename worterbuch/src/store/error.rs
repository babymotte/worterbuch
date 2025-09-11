use worterbuch_common::error::WorterbuchError;

#[derive(Debug)]
pub enum StoreError {
    IllegalMultiWildcard,
}

impl StoreError {
    pub fn for_pattern(self, pattern: String) -> WorterbuchError {
        match self {
            StoreError::IllegalMultiWildcard => WorterbuchError::IllegalMultiWildcard(pattern),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;
