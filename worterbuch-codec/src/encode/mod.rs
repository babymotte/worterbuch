mod client;
mod server;

pub use client::*;
pub use server::*;

use crate::{
    error::{EncodeError, EncodeResult},
    KeyLength, KeyValuePairs, MetaDataLength, NumKeyValuePairs, NumProtocolVersions, PathLength,
    ProtocolVersions, RequestPatternLength, ValueLength,
};

fn get_request_pattern_length(string: &str) -> EncodeResult<RequestPatternLength> {
    let length = string.len();
    if length > RequestPatternLength::MAX as usize {
        Err(EncodeError::RequestPatternTooLong(length))
    } else {
        Ok(length as RequestPatternLength)
    }
}

fn get_key_length(string: &str) -> EncodeResult<KeyLength> {
    let length = string.len();
    if length > KeyLength::MAX as usize {
        Err(EncodeError::KeyTooLong(length))
    } else {
        Ok(length as KeyLength)
    }
}

fn get_value_length(string: &str) -> EncodeResult<ValueLength> {
    let length = string.len();
    if length > ValueLength::MAX as usize {
        Err(EncodeError::ValueTooLong(length))
    } else {
        Ok(length as ValueLength)
    }
}

fn get_num_key_val_pairs(pairs: &KeyValuePairs) -> EncodeResult<NumKeyValuePairs> {
    let length = pairs.len();
    if length > NumKeyValuePairs::MAX as usize {
        Err(EncodeError::TooManyKeyValuePairs(length))
    } else {
        Ok(length as NumKeyValuePairs)
    }
}

fn get_num_protocol_versions(versions: &ProtocolVersions) -> EncodeResult<NumProtocolVersions> {
    let length = versions.len();
    if length > NumProtocolVersions::MAX as usize {
        Err(EncodeError::TooManyProtocolVersions(length))
    } else {
        Ok(length as NumProtocolVersions)
    }
}

fn get_metadata_length(string: &str) -> EncodeResult<MetaDataLength> {
    let length = string.len();
    if length > MetaDataLength::MAX as usize {
        Err(EncodeError::MetaDataTooLong(length))
    } else {
        Ok(length as MetaDataLength)
    }
}

fn get_path_length(string: &str) -> EncodeResult<PathLength> {
    let length = string.len();
    if length > PathLength::MAX as usize {
        Err(EncodeError::PathTooLong(length))
    } else {
        Ok(length as PathLength)
    }
}
