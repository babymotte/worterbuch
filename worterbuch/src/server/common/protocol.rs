use tokio::sync::mpsc;
use uuid::Uuid;
use v0::V0;
use v1::V1;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    Ack, ClientMessage, ProtocolVersionSegment, ServerMessage,
};

use crate::{auth::JwtClaims, Config};

use super::CloneableWbApi;

mod v0;
mod v1;

#[derive(Debug, Clone)]
enum ProtocolHandler {
    V0(V0),
    V1(V1),
}

impl Default for ProtocolHandler {
    fn default() -> Self {
        ProtocolHandler::V1(V1::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Proto {
    handler: ProtocolHandler,
}

impl Proto {
    pub fn switch_protocol(&mut self, version: ProtocolVersionSegment) -> bool {
        match version {
            0 => {
                self.handler = ProtocolHandler::V0(V0::default());
                true
            }
            1 => {
                self.handler = ProtocolHandler::V1(V1::default());
                true
            }
            _ => false,
        }
    }

    pub async fn process_incoming_message(
        &mut self,
        client_id: Uuid,
        msg: &str,
        worterbuch: &CloneableWbApi,
        tx: &mpsc::Sender<ServerMessage>,
        auth_required: bool,
        authorized: &mut Option<JwtClaims>,
        config: &Config,
    ) -> WorterbuchResult<bool> {
        log::debug!("Received message from client {client_id}: {msg}");
        match serde_json::from_str(msg) {
            Ok(Some(msg)) => {
                if let ClientMessage::ProtocolSwitchRequest(protocol_switch_request) = &msg {
                    log::info!("Switching protocol to v{}", protocol_switch_request.version);
                    if self.switch_protocol(protocol_switch_request.version) {
                        let response = Ack { transaction_id: 0 };
                        log::trace!("Protocol switched, queuing Ack …");
                        let res = tx.send(ServerMessage::Ack(response)).await;
                        log::trace!("Protocol switched, queuing Ack done.");
                        res.context(|| {
                            "Error sending ACK message for transaction ID 0".to_owned()
                        })?;
                    } else {
                        return Err(WorterbuchError::ProtocolNegotiationFailed(
                            protocol_switch_request.version,
                        ));
                    }
                    return Ok(true);
                }

                match &self.handler {
                    ProtocolHandler::V0(v0) => {
                        v0.process_incoming_message(
                            client_id,
                            msg,
                            worterbuch,
                            tx,
                            auth_required,
                            authorized,
                            config,
                        )
                        .await?;
                    }
                    ProtocolHandler::V1(v1) => {
                        v1.process_incoming_message(
                            client_id,
                            msg,
                            worterbuch,
                            tx,
                            auth_required,
                            authorized,
                            config,
                        )
                        .await?;
                    }
                }

                Ok(true)
            }
            Ok(None) => {
                // client disconnected
                Ok(false)
            }
            Err(e) => {
                log::error!("Error decoding message: {e}");
                Ok(false)
            }
        }
    }
}
