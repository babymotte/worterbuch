use anyhow::{Error, Result};

pub(crate) fn to_char(str: impl AsRef<str>) -> Result<char> {
    let str = str.as_ref();
    if str.len() != 1 {
        Err(Error::msg("string must be of length 1"))
    } else {
        if let Some(ch) = str.chars().next() {
            Ok(ch)
        } else {
            Err(Error::msg("string must be of length 1"))
        }
    }
}
