/*
 *  Worterbuch client protocol v1 implementation
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

use super::v0::V0;
use crate::{
    auth::JwtClaims,
    server::common::protocol::{
        LazyBroadcaster, forward_lock_acquired, forward_lock_lost,
        v0::{handle_store_error, handle_store_error_lazy},
    },
};
use tokio::{spawn, sync::oneshot};
use tracing::{Level, instrument, trace};
use worterbuch_common::{
    Privilege, WbApi,
    error::WorterbuchResult,
    protocol::v1::{Ack, CSet, CState, CStateEvent, ClientMessage, Get, Lock, ServerMessage},
};

#[derive(Clone)]
pub struct V1 {
    pub v0: V0,
}

impl V1 {
    pub fn new(v0: V0) -> Self {
        Self { v0 }
    }

    #[instrument(level=Level::TRACE, skip(self), fields(protocol = "v1", client_id=%self.v0.client_id))]
    pub async fn process_incoming_message(
        &self,
        msg: ClientMessage,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<()> {
        match msg {
            ClientMessage::CGet(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Getting CAS value for client {} …", self.v0.client_id);
                    self.cget(msg).await;
                    trace!("Getting CAS value for client {} done.", self.v0.client_id);
                }
            }
            ClientMessage::CSet(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Setting cas value for client {} …", self.v0.client_id);
                    self.cset(msg).await;
                    trace!("Setting cas value for client {} done.", self.v0.client_id);
                }
            }
            ClientMessage::Lock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Locking key for client {} …", self.v0.client_id);
                    self.lock(msg).await;
                    trace!("Locking key for client {} done.", self.v0.client_id);
                }
            }
            ClientMessage::AcquireLock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Locking key for client {} …", self.v0.client_id);
                    self.acquire_lock(msg).await;
                    trace!("Locking key for client {} done.", self.v0.client_id);
                }
            }
            ClientMessage::ReleaseLock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Unlocking key for client {} …", self.v0.client_id);
                    self.release_lock(msg).await;
                    trace!("Unlocking key for client {} done.", self.v0.client_id);
                }
            }
            msg => {
                self.v0.process_incoming_message(msg, authorized).await?;
            }
        }
        Ok(())
    }

    pub async fn cget(&self, msg: Get) {
        let (tx, rx) = oneshot::channel();
        let wb = self.v0.worterbuch.clone();
        let permit = self.v0.acquire_permit().await;
        let client_id = self.v0.client_id;

        tokio::spawn(async move {
            let (value, version) = match wb.cget(msg.key).await {
                Ok(it) => it,
                Err(e) => {
                    handle_store_error(tx, e, msg.transaction_id).await;
                    return;
                }
            };

            let response = CState {
                transaction_id: msg.transaction_id,
                event: CStateEvent { value, version },
            };

            let msg = ServerMessage::CState(response);
            tx.send(msg).ok();

            drop(permit);
        });

        self.v0.tx.send(rx).await.ok();
    }

    pub async fn cset(&self, msg: CSet) {
        let (tx, rx) = oneshot::channel();
        let wb = self.v0.worterbuch.clone();
        let permit = self.v0.acquire_permit().await;
        let client_id = self.v0.client_id;

        tokio::spawn(async move {
            if let Err(e) = wb
                .cset(
                    msg.transaction_id,
                    msg.key,
                    msg.value,
                    msg.version,
                    client_id,
                )
                .await
            {
                handle_store_error(tx, e, msg.transaction_id).await;
                return;
            }

            let response = Ack {
                transaction_id: msg.transaction_id,
            };

            trace!("Value set, queuing Ack …");
            tx.send(ServerMessage::Ack(response)).ok();
            trace!("Value set, queuing Ack done.");

            drop(permit);
        });

        self.v0.tx.send(rx).await.ok();
    }

    pub async fn lock(&self, msg: Lock) {
        let lost_rx = match self
            .v0
            .worterbuch
            .lock(msg.transaction_id, msg.key, self.v0.client_id)
            .await
        {
            Ok(it) => it,
            Err(e) => {
                handle_store_error_lazy(&self.v0.tx, e, msg.transaction_id).await;
                return;
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Key locked, queuing Ack …");
        self.v0
            .tx
            .lazy_send(ServerMessage::Ack(response))
            .await
            .ok();
        trace!("Key locked, queuing Ack done.");

        let client = self.v0.tx.clone();
        let transaction_id = msg.transaction_id;
        spawn(forward_lock_lost(client, transaction_id, lost_rx));
    }

    pub async fn acquire_lock(&self, msg: Lock) {
        let (acquired_rx, lost_rx) = match self
            .v0
            .worterbuch
            .acquire_lock(msg.transaction_id, msg.key, self.v0.client_id)
            .await
        {
            Err(e) => {
                handle_store_error_lazy(&self.v0.tx, e, msg.transaction_id).await;
                return;
            }
            Ok(rx) => rx,
        };

        let client = self.v0.tx.clone();
        let transaction_id = msg.transaction_id;

        spawn(forward_lock_acquired(
            client,
            transaction_id,
            acquired_rx,
            lost_rx,
        ));
    }

    pub async fn release_lock(&self, msg: Lock) {
        if let Err(e) = self
            .v0
            .worterbuch
            .release_lock(msg.transaction_id, msg.key, self.v0.client_id)
            .await
        {
            handle_store_error_lazy(&self.v0.tx, e, msg.transaction_id).await;
            return;
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Key unlocked, queuing Ack …");
        self.v0
            .tx
            .lazy_send(ServerMessage::Ack(response))
            .await
            .ok();
        trace!("Key unlocked, queuing Ack done.");
    }
}
