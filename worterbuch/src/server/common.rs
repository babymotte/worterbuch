mod v1_0;

use crate::worterbuch::Worterbuch;
use std::sync::Arc;
use tokio::sync::{mpsc::UnboundedSender, RwLock};
use uuid::Uuid;
use worterbuch_common::{error::WorterbuchResult, ProtocolVersion};

pub async fn process_incoming_message(
    client_id: Uuid,
    msg: &str,
    worterbuch: Arc<RwLock<Worterbuch>>,
    tx: UnboundedSender<String>,
    protocol_version: &ProtocolVersion,
) -> WorterbuchResult<bool> {
    match protocol_version {
        ProtocolVersion { major, minor } if *major < 1 || (*major == 1 && *minor == 0) => {
            v1_0::process_incoming_message(client_id, msg, worterbuch, tx).await
        }
        _ => todo!(),
    }
}
