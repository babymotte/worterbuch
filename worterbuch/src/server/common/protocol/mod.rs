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

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::CloneableWbApi;
use crate::{Config, auth::JwtClaims, server::common::protocol::v2::V2};
use serde_json::json;
use tokio::select;
use tokio::sync::{
    Semaphore,
    mpsc::{
        self,
        error::{SendError, TryRecvError},
    },
    oneshot,
};
use tosub::Subsystem;
use totils::CancelOn;
use tracing::{Instrument, Level, debug, instrument, trace, trace_span, warn};
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
/// Creates the queue through which all messages for a single client are passed to the task writing to the client's
/// socket.
///
/// Messages that are already available ([`LazyBroadcaster::lazy_send`]) and responses that are still being processed
/// by the core system ([`ServerMessageLazyBroadcaster::send`]) are queued separately. The receiving end delivers them
/// in the order they were queued wherever possible, but an already available message is never held back by a response
/// that is still pending. Otherwise the socket writer would wait for the core system while the core system waits for
/// the socket writer to drain this client's subscription events, which deadlocks the whole server.
pub fn server_message_channel(
    buffer_size: usize,
) -> (ServerMessageLazyBroadcaster, ServerMessageReceiver) {
    let (pending_tx, pending_rx) = mpsc::channel(buffer_size);
    let (ready_tx, ready_rx) = mpsc::channel(buffer_size);
    let tx = ServerMessageLazyBroadcaster {
        seq: Arc::new(AtomicU64::new(0)),
        pending: pending_tx,
        ready: ready_tx,
    };
    let rx = ServerMessageReceiver {
        pending: pending_rx,
        ready: ready_rx,
        pending_closed: false,
        ready_closed: false,
        head: None,
        resolved_head: None,
        next_ready: None,
    };
    (tx, rx)
}

#[derive(Clone)]
pub struct ServerMessageLazyBroadcaster {
    seq: Arc<AtomicU64>,
    pending: mpsc::Sender<(u64, oneshot::Receiver<ServerMessage>)>,
    ready: mpsc::Sender<(u64, ServerMessage)>,
}

impl ServerMessageLazyBroadcaster {
    /// Queues a response that will be delivered to the client once it has been resolved. Pending responses are
    /// delivered in the order they were queued.
    pub async fn send(
        &self,
        rx: oneshot::Receiver<ServerMessage>,
    ) -> Result<(), SendError<oneshot::Receiver<ServerMessage>>> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        self.pending
            .send((seq, rx))
            .await
            .map_err(|SendError((_, rx))| SendError(rx))
    }
}

pub(crate) trait LazyBroadcaster<T> {
    fn lazy_send(&self, msg: T) -> impl Future<Output = Result<(), SendError<T>>>;
}

impl LazyBroadcaster<ServerMessage> for ServerMessageLazyBroadcaster {
    #[instrument(level = Level::TRACE, skip_all, err)]
    async fn lazy_send(&self, msg: ServerMessage) -> Result<(), SendError<ServerMessage>> {
        trace!(enter = "lazy_send");
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let res = self
            .ready
            .send((seq, msg))
            .await
            .map_err(|SendError((_, msg))| SendError(msg));
        trace!(exit = "lazy_send");
        res
    }
}

pub struct ServerMessageReceiver {
    pending: mpsc::Receiver<(u64, oneshot::Receiver<ServerMessage>)>,
    ready: mpsc::Receiver<(u64, ServerMessage)>,
    pending_closed: bool,
    ready_closed: bool,
    head: Option<(u64, oneshot::Receiver<ServerMessage>)>,
    resolved_head: Option<(u64, Result<ServerMessage, ResponseDropped>)>,
    next_ready: Option<(u64, ServerMessage)>,
}

/// A pending response was dropped before it was resolved.
#[derive(Debug)]
pub struct ResponseDropped;

enum QueueUpdate {
    Resolved(Result<ServerMessage, ResponseDropped>),
    Ready(Option<(u64, ServerMessage)>),
    Pending(Option<(u64, oneshot::Receiver<ServerMessage>)>),
}

impl ServerMessageReceiver {
    /// Returns the next message to be sent to the client, an error if a pending response was dropped without being
    /// resolved, or `None` once all senders have been dropped and all queued messages have been delivered.
    ///
    /// This method is cancel safe.
    pub async fn recv(&mut self) -> Option<Result<ServerMessage, ResponseDropped>> {
        loop {
            self.fill_buffers();

            match (&self.resolved_head, &self.next_ready) {
                (Some((head_seq, _)), Some((ready_seq, _))) if ready_seq < head_seq => {
                    return self.next_ready.take().map(|(_, msg)| Ok(msg));
                }
                (Some(_), _) => return self.resolved_head.take().map(|(_, res)| res),
                // the head is either still pending or there is none, so there is nothing to wait for
                (None, Some(_)) => return self.next_ready.take().map(|(_, msg)| Ok(msg)),
                (None, None) => {}
            }

            if self.head.is_none() && self.pending_closed && self.ready_closed {
                return None;
            }

            let ready_closed = self.ready_closed;
            let pending_closed = self.pending_closed;
            let update = if let Some((_, rx)) = &mut self.head {
                select! {
                    res = rx => QueueUpdate::Resolved(res.map_err(|_| ResponseDropped)),
                    msg = self.ready.recv(), if !ready_closed => QueueUpdate::Ready(msg),
                }
            } else {
                select! {
                    msg = self.ready.recv(), if !ready_closed => QueueUpdate::Ready(msg),
                    rx = self.pending.recv(), if !pending_closed => QueueUpdate::Pending(rx),
                }
            };

            match update {
                QueueUpdate::Resolved(res) => {
                    if let Some((seq, _)) = self.head.take() {
                        self.resolved_head = Some((seq, res));
                    }
                }
                QueueUpdate::Ready(Some(msg)) => self.next_ready = Some(msg),
                QueueUpdate::Ready(None) => self.ready_closed = true,
                QueueUpdate::Pending(Some(rx)) => self.head = Some(rx),
                QueueUpdate::Pending(None) => self.pending_closed = true,
            }
        }
    }

    fn fill_buffers(&mut self) {
        if self.head.is_none() && self.resolved_head.is_none() && !self.pending_closed {
            match self.pending.try_recv() {
                Ok(rx) => self.head = Some(rx),
                Err(TryRecvError::Disconnected) => self.pending_closed = true,
                Err(TryRecvError::Empty) => {}
            }
        }

        if let Some((seq, rx)) = &mut self.head {
            match rx.try_recv() {
                Ok(msg) => self.resolved_head = Some((*seq, Ok(msg))),
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.resolved_head = Some((*seq, Err(ResponseDropped)))
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
            if self.resolved_head.is_some() {
                self.head = None;
            }
        }

        if self.next_ready.is_none() && !self.ready_closed {
            match self.ready.try_recv() {
                Ok(msg) => self.next_ready = Some(msg),
                Err(TryRecvError::Disconnected) => self.ready_closed = true,
                Err(TryRecvError::Empty) => {}
            }
        }
    }
}

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
        subsys: Subsystem,
        client_id: ClientId,
        tx: ServerMessageLazyBroadcaster,
        auth_required: bool,
        config: Config,
        worterbuch: CloneableWbApi,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.channel_buffer_size));
        let latest = V2::new(V1::new(V0 {
            subsys,
            auth_required,
            client_id,
            config,
            tx,
            worterbuch,
            semaphore,
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
        trace!(client_id=%self.client_id(), "Received message from client");
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
                warn!("Error decoding message: {e}");
                trace!(%msg, err = ?e);
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
                let res = self.tx().lazy_send(ServerMessage::Ack(response)).await;
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

    fn tx(&self) -> &ServerMessageLazyBroadcaster {
        &self.latest.v1.v0.tx
    }
}

pub async fn forward_lock_acquired(
    subsys: Subsystem,
    client: ServerMessageLazyBroadcaster,
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
        .lazy_send(ServerMessage::Ack(Ack { transaction_id }))
        .await
        .is_err()
    {
        // client has apparently already disconnected
        return;
    }

    forward_lock_lost(subsys, client, transaction_id, lost_rx).await;
}

async fn forward_lock_lost(
    subsys: Subsystem,
    client: ServerMessageLazyBroadcaster,
    transaction_id: TransactionId,
    lost_rx: LockLostReceiver,
) {
    debug!("Receiving lock lost message for transaction {transaction_id:?} …");

    match lost_rx.or_cancel_on(subsys.shutdown_requested()).await {
        Some(Err(_)) => {
            debug!(
                "Did not receive a lock lost message for transaction id {transaction_id:?} before lock was released."
            );
            return;
        }
        None => {
            debug!(
                "Did not receive a lock lost message for transaction id {transaction_id:?} before system was shut down."
            );
            return;
        }
        Some(Ok(_)) => debug!("Lock lost message for transaction {transaction_id:?} received."),
    }

    let msg = ServerMessage::Err(Err {
        transaction_id,
        error_code: ErrorCode::LockLost,
        metadata: json!("Lock lost").to_string(),
    });
    let _ = client.lazy_send(msg).await;
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    fn ack(transaction_id: TransactionId) -> ServerMessage {
        ServerMessage::Ack(Ack { transaction_id })
    }

    async fn next(rx: &mut ServerMessageReceiver) -> ServerMessage {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receiver blocked")
            .expect("channel closed")
            .expect("response dropped")
    }

    #[tokio::test]
    async fn ready_message_overtakes_pending_response() {
        let (tx, mut rx) = server_message_channel(4);

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(resp_rx).await.unwrap();
        tx.lazy_send(ack(2)).await.unwrap();

        assert_eq!(next(&mut rx).await, ack(2));

        resp_tx.send(ack(1)).unwrap();
        assert_eq!(next(&mut rx).await, ack(1));
    }

    #[tokio::test]
    async fn queue_order_is_kept_when_nothing_is_pending() {
        let (tx, mut rx) = server_message_channel(4);

        tx.lazy_send(ack(0)).await.unwrap();
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(resp_rx).await.unwrap();
        resp_tx.send(ack(1)).unwrap();
        tx.lazy_send(ack(2)).await.unwrap();

        assert_eq!(next(&mut rx).await, ack(0));
        assert_eq!(next(&mut rx).await, ack(1));
        assert_eq!(next(&mut rx).await, ack(2));
    }

    #[tokio::test]
    async fn pending_responses_keep_their_order() {
        let (tx, mut rx) = server_message_channel(4);

        let (resp_tx_1, resp_rx_1) = oneshot::channel();
        let (resp_tx_2, resp_rx_2) = oneshot::channel();
        tx.send(resp_rx_1).await.unwrap();
        tx.send(resp_rx_2).await.unwrap();
        resp_tx_2.send(ack(2)).unwrap();

        assert!(
            timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
            "second response must not overtake the first"
        );

        resp_tx_1.send(ack(1)).unwrap();
        assert_eq!(next(&mut rx).await, ack(1));
        assert_eq!(next(&mut rx).await, ack(2));
    }

    #[tokio::test]
    async fn dropped_response_and_closed_channel_are_reported() {
        let (tx, mut rx) = server_message_channel(4);

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(resp_rx).await.unwrap();
        tx.lazy_send(ack(1)).await.unwrap();
        drop(resp_tx);
        drop(tx);

        assert!(matches!(rx.recv().await, Some(Err(ResponseDropped))));
        assert_eq!(rx.recv().await.unwrap().unwrap(), ack(1));
        assert!(rx.recv().await.is_none());
    }
}
