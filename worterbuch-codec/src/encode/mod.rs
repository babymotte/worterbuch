mod client;
mod server;

pub use client::*;
pub use server::*;

use crate::{
    error::{EncodeError, EncodeResult},
    GraveGoods, Key, KeyLength, KeyValuePairs, LastWill, MetaData, MetaDataLength, NumGraveGoods,
    NumKeyValuePairs, NumLastWill, NumProtocolVersions, Path, PathLength, ProtocolVersions,
    RequestPattern, RequestPatternLength, Value, ValueLength,
};

fn get_request_pattern_length(string: &RequestPattern) -> EncodeResult<RequestPatternLength> {
    let length = string.len();
    if length > RequestPatternLength::MAX as usize {
        Err(EncodeError::RequestPatternTooLong(length))
    } else {
        Ok(length as RequestPatternLength)
    }
}

fn get_key_length(string: &Key) -> EncodeResult<KeyLength> {
    let length = string.len();
    if length > KeyLength::MAX as usize {
        Err(EncodeError::KeyTooLong(length))
    } else {
        Ok(length as KeyLength)
    }
}

fn get_value_length(string: &Value) -> EncodeResult<ValueLength> {
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

fn get_num_last_will(last_will: &LastWill) -> EncodeResult<NumLastWill> {
    let length = last_will.len();
    if length > NumLastWill::MAX as usize {
        Err(EncodeError::TooManyLastWills(length))
    } else {
        Ok(length as NumLastWill)
    }
}

fn get_num_grave_goods(grave_goods: &GraveGoods) -> EncodeResult<NumGraveGoods> {
    let length = grave_goods.len();
    if length > NumGraveGoods::MAX as usize {
        Err(EncodeError::TooManyGraveGoods(length))
    } else {
        Ok(length as NumGraveGoods)
    }
}

fn get_metadata_length(string: &MetaData) -> EncodeResult<MetaDataLength> {
    let length = string.len();
    if length > MetaDataLength::MAX as usize {
        Err(EncodeError::MetaDataTooLong(length))
    } else {
        Ok(length as MetaDataLength)
    }
}

fn get_path_length(string: &Path) -> EncodeResult<PathLength> {
    let length = string.len();
    if length > PathLength::MAX as usize {
        Err(EncodeError::PathTooLong(length))
    } else {
        Ok(length as PathLength)
    }
}
