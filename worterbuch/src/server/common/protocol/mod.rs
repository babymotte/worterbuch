/*
 *  Worterbuch client protocol implementations
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

mod v0;
mod v1;
mod v2;

use super::CloneableWbApi;
use crate::{Config, auth::JwtClaims, server::common::protocol::v2::V2};
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{Instrument, Level, debug, error, instrument, trace, trace_span};
use v0::V0;
use v1::V1;
use worterbuch_common::{
    ClientId, LockAcquiredReceiver, LockLostReceiver, WbApi,
    error::{Context, WorterbuchError, WorterbuchResult},
    protocol::v1::{
        Ack, ClientMessage, Err, ErrorCode, ProtocolVersionSegment, ServerMessage, TransactionId,
    },
};

pub type ServerMessageBroadcaster = mpsc::Sender<ServerMessage>;

enum ProtocolHandler {
    V0(V0),
    V1(V1),
    V2(V2),
}

pub struct Proto {
    latest: V2,
    handler: ProtocolHandler,
}

impl Proto {
    pub fn new(
        client_id: ClientId,
        tx: ServerMessageBroadcaster,
        auth_required: bool,
        config: Config,
        worterbuch: CloneableWbApi,
    ) -> Self {
        let latest = V2::new(V1::new(V0 {
            auth_required,
            client_id,
            config,
            tx,
            worterbuch,
        }));
        Self {
            handler: ProtocolHandler::V2(latest.clone()),
            latest,
        }
    }

    fn is_supported(&self, protocol_version: &ProtocolVersionSegment) -> bool {
        self.latest
            .v1
            .v0
            .worterbuch
            .supported_client_protocol_versions
            .contains(protocol_version)
    }

    #[instrument(level=Level::TRACE, skip(self))]
    pub fn switch_protocol(&mut self, version: ProtocolVersionSegment) -> bool {
        if !self.is_supported(&version) {
            debug!("Protocol version {version} is not supported by the server.");
            return false;
        }
        match version {
            0 => {
                self.handler = ProtocolHandler::V0(self.latest.v1.v0.clone());
                true
            }
            1 => {
                self.handler = ProtocolHandler::V1(self.latest.v1.clone());
                true
            }
            2 => {
                self.handler = ProtocolHandler::V2(self.latest.clone());
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
            Ok(Some(msg)) => self.process_client_message(msg, authorized).await,
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

    #[instrument(level=Level::TRACE, skip(self), fields(client_id=%self.client_id()))]
    pub async fn process_client_message(
        &mut self,
        msg: ClientMessage,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<bool> {
        if let ClientMessage::ProtocolSwitchRequest(protocol_switch_request) = &msg {
            debug!("Switching protocol to v{}", protocol_switch_request.version);
            if self.switch_protocol(protocol_switch_request.version) {
                self.latest
                    .v1
                    .v0
                    .worterbuch
                    .protocol_switched(self.client_id(), protocol_switch_request.version)
                    .await?;
                let response = Ack { transaction_id: 0 };
                trace!("Protocol switched, queuing Ack …");
                let res = self.tx().send(ServerMessage::Ack(response)).await;
                trace!("Protocol switched, queuing Ack done.");
                res.context(|| "Error sending ACK message for transaction ID 0".to_owned())?;
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
                ProtocolHandler::V2(v2) => {
                    v2.process_incoming_message(msg, authorized).await?;
                }
            }
        }

        Ok(true)
    }

    fn client_id(&self) -> ClientId {
        self.latest.v1.v0.client_id
    }

    fn tx(&self) -> &mpsc::Sender<ServerMessage> {
        &self.latest.v1.v0.tx
    }
}

pub async fn forward_lock_acquired(
    client: ServerMessageBroadcaster,
    transaction_id: TransactionId,
    acquired_rx: LockAcquiredReceiver,
    lost_rx: LockLostReceiver,
) {
    debug!("Receiving lock confirmation for transaction {transaction_id:?} …");
    if !acquired_rx.await.is_ok() {
        debug!("Lock acquisition for transaction {transaction_id:?} failed.");
        return;
    }

    debug!("Lock confirmation for transaction {transaction_id:?} received.");
    if client
        .send(ServerMessage::Ack(Ack { transaction_id }))
        .await
        .is_err()
    {
        // client has apparently already disconnected
        return;
    }

    forward_lock_lost(client, transaction_id, lost_rx).await;
}

async fn forward_lock_lost(
    client: ServerMessageBroadcaster,
    transaction_id: TransactionId,
    lost_rx: LockLostReceiver,
) {
    debug!("Receiving lock lost message for transaction {transaction_id:?} …");
    if !lost_rx.await.is_ok() {
        debug!(
            "Did not receive a lock lost message for transaction id {transaction_id:?} before lock was released."
        );
        return;
    }

    debug!("Lock lost message for transaction {transaction_id:?} received.");
    let _ = client
        .send(ServerMessage::Err(Err {
            transaction_id,
            error_code: ErrorCode::LockLost,
            metadata: json!("Lock lost").to_string(),
        }))
        .await;
}
