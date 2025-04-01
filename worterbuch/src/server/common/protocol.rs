use super::CloneableWbApi;
use crate::{Config, auth::JwtClaims};
use tokio::sync::mpsc;
use tracing::{Instrument, Level, debug, error, info, instrument, trace, trace_span};
use uuid::Uuid;
use v0::V0;
use v1::V1;
use worterbuch_common::{
    Ack, ClientMessage, ProtocolVersionSegment, ServerMessage,
    error::{Context, WorterbuchError, WorterbuchResult},
};

mod v0;
mod v1;

enum ProtocolHandler {
    V0(V0),
    V1(V1),
}

pub struct Proto {
    latest: V1,
    handler: ProtocolHandler,
}

impl Proto {
    pub fn new(
        client_id: Uuid,
        tx: mpsc::Sender<ServerMessage>,
        auth_required: bool,
        config: Config,
        worterbuch: CloneableWbApi,
    ) -> Self {
        let latest = V1::new(V0 {
            auth_required,
            client_id,
            config,
            tx,
            worterbuch,
        });
        Self {
            handler: ProtocolHandler::V1(latest.clone()),
            latest,
        }
    }

    #[instrument(level=Level::TRACE, skip(self))]
    pub fn switch_protocol(&mut self, version: ProtocolVersionSegment) -> bool {
        match version {
            0 => {
                self.handler = ProtocolHandler::V0(self.latest.v0.clone());
                true
            }
            1 => {
                self.handler = ProtocolHandler::V1(self.latest.clone());
                true
            }
            _ => false,
        }
    }

    #[instrument(level=Level::TRACE, skip(self), fields(client_id=%self.client_id()))]
    pub async fn process_incoming_message(
        &mut self,
        msg: &str,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<bool> {
        debug!("Received message from client {}: {}", self.client_id(), msg);
        let deserialized = async { serde_json::from_str(msg) }
            .instrument(trace_span!("from_str"))
            .await;
        match deserialized {
            Ok(Some(msg)) => {
                if let ClientMessage::ProtocolSwitchRequest(protocol_switch_request) = &msg {
                    info!("Switching protocol to v{}", protocol_switch_request.version);
                    if self.switch_protocol(protocol_switch_request.version) {
                        let response = Ack { transaction_id: 0 };
                        trace!("Protocol switched, queuing Ack …");
                        let res = self.tx().send(ServerMessage::Ack(response)).await;
                        trace!("Protocol switched, queuing Ack done.");
                        res.context(|| {
                            "Error sending ACK message for transaction ID 0".to_owned()
                        })?;
                    } else {
                        return Err(WorterbuchError::ProtocolNegotiationFailed(
                            protocol_switch_request.version,
                        ));
                    }
                    return Ok(true);
                } else {
                    match &self.handler {
                        ProtocolHandler::V0(v0) => {
                            v0.process_incoming_message(msg, authorized).await?;
                        }
                        ProtocolHandler::V1(v1) => {
                            v1.process_incoming_message(msg, authorized).await?;
                        }
                    }
                }

                Ok(true)
            }
            Ok(None) => {
                // client disconnected
                Ok(false)
            }
            Err(e) => {
                error!("Error decoding message: {e}");
                Ok(false)
            }
        }
    }

    fn client_id(&self) -> Uuid {
        self.latest.v0.client_id
    }

    fn tx(&self) -> &mpsc::Sender<ServerMessage> {
        &self.latest.v0.tx
    }
}
