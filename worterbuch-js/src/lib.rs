use libworterbuch::codec::{blocking::read_server_message, encode_message};
use wasm_bindgen::prelude::*;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub fn encode_client_message(msg: &JsValue) -> Result<Vec<u8>, String> {
    let cm = msg.into_serde().map_err(|e| e.to_string())?;
    encode_message(&cm).map_err(|e| e.to_string())
}

#[wasm_bindgen]
pub fn decode_server_message(data: &[u8]) -> Result<JsValue, String> {
    let sm = read_server_message(&*data).map_err(|e| e.to_string())?;
    JsValue::from_serde(&sm).map_err(|e| e.to_string())
}
