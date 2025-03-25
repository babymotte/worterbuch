/*
 *  Worterbuch server common module
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

pub mod protocol;

use crate::{Config, INTERNAL_CLIENT_ID, store::ValueEntry, subscribers::SubscriptionId};
use serde::Serialize;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::{
    mpsc::{self, Receiver},
    oneshot,
};
use tracing::{Level, Span, instrument, trace};
use uuid::Uuid;
use worterbuch_common::{
    CasVersion, GraveGoods, Key, KeyValuePairs, LastWill, LiveOnlyFlag, MetaData, PStateEvent,
    Protocol, RegularKeySegment, RequestPattern, StateEvent, TransactionId, UniqueFlag, Value,
    error::WorterbuchResult,
};

#[derive(Debug, Clone, PartialEq)]
struct SubscriptionInfo {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    live_only: bool,
    aggregate_duration: Duration,
    channel_buffer_size: usize,
}

pub enum WbFunction {
    Get(Key, oneshot::Sender<WorterbuchResult<Value>>),
    CGet(Key, oneshot::Sender<WorterbuchResult<(Value, CasVersion)>>),
    Set(
        Key,
        Value,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    CSet(
        Key,
        Value,
        CasVersion,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPubInit(
        TransactionId,
        Key,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPub(
        TransactionId,
        Value,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Publish(Key, Value, oneshot::Sender<WorterbuchResult<()>>),
    Ls(
        Option<Key>,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
    ),
    PLs(
        Option<RequestPattern>,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
    ),
    PGet(
        RequestPattern,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Subscribe(
        Uuid,
        TransactionId,
        Key,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(Receiver<StateEvent>, SubscriptionId)>>,
    ),
    PSubscribe(
        Uuid,
        TransactionId,
        RequestPattern,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(Receiver<PStateEvent>, SubscriptionId)>>,
    ),
    SubscribeLs(
        Uuid,
        TransactionId,
        Option<Key>,
        oneshot::Sender<WorterbuchResult<(Receiver<Vec<RegularKeySegment>>, SubscriptionId)>>,
    ),
    Unsubscribe(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    UnsubscribeLs(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    Delete(Key, Uuid, oneshot::Sender<WorterbuchResult<Value>>),
    PDelete(
        RequestPattern,
        Uuid,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Lock(Key, Uuid, oneshot::Sender<WorterbuchResult<()>>),
    AcquireLock(
        Key,
        Uuid,
        oneshot::Sender<WorterbuchResult<oneshot::Receiver<()>>>,
    ),
    ReleaseLock(Key, Uuid, oneshot::Sender<WorterbuchResult<()>>),
    Connected(Uuid, Option<SocketAddr>, Protocol),
    Disconnected(Uuid, Option<SocketAddr>),
    Config(oneshot::Sender<Config>),
    Export(oneshot::Sender<(Value, GraveGoods, LastWill)>, Span),
    Import(
        String,
        oneshot::Sender<WorterbuchResult<Vec<(String, (ValueEntry, bool))>>>,
    ),
    Len(oneshot::Sender<usize>),
}

#[derive(Clone)]
pub struct CloneableWbApi {
    tx: mpsc::Sender<WbFunction>,
}

impl CloneableWbApi {
    pub fn new(tx: mpsc::Sender<WbFunction>) -> Self {
        CloneableWbApi { tx }
    }

    pub async fn get(&self, key: Key) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Get(key, tx)).await?;
        rx.await?
    }

    pub async fn cget(&self, key: Key) -> WorterbuchResult<(Value, CasVersion)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::CGet(key, tx)).await?;
        rx.await?
    }

    pub async fn pget(&self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PGet(pattern, tx)).await?;
        rx.await?
    }

    #[instrument(level=Level::TRACE, skip(self))]
    pub async fn set(&self, key: Key, value: Value, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();

        trace!("Sending set request to core system …");
        let res = self
            .tx
            .send(WbFunction::Set(key, value, client_id, tx, Span::current()))
            .await;
        trace!("Sending set request to core system done.");
        res?;
        trace!("Waiting for response to set request …");
        let res = rx.await;
        trace!("Waiting for response to set request done.");
        res?
    }

    pub async fn cset(
        &self,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::CSet(key, value, version, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to cset request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to cset request done.");
        }
        res?
    }

    pub async fn lock(&self, key: Key, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending lock request to core system …");
        }
        let res = self.tx.send(WbFunction::Lock(key, client_id, tx)).await;
        if trace {
            trace!("Sending lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to lock request done.");
        }
        res?
    }

    pub async fn acquire_lock(
        &self,
        key: Key,
        client_id: Uuid,
    ) -> WorterbuchResult<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::AcquireLock(key, client_id, tx))
            .await;
        if trace {
            trace!("Sending lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to lock request done.");
        }
        res?
    }

    pub async fn release_lock(&self, key: Key, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending release lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::ReleaseLock(key, client_id, tx))
            .await;
        if trace {
            trace!("Sending release lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to release lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to release lock request done.");
        }
        res?
    }

    pub async fn spub_init(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPubInit(transaction_id, key, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to set request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to set request done.");
        }
        res?
    }

    pub async fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPub(transaction_id, value, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to set request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to set request done.");
        }
        res?
    }

    pub async fn publish(&self, key: Key, value: Value) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Publish(key, value, tx)).await?;
        rx.await?
    }

    pub async fn ls(&self, parent: Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Ls(parent, tx)).await?;
        rx.await?
    }

    pub async fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PLs(parent, tx)).await?;
        rx.await?
    }

    pub async fn subscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(Receiver<StateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Subscribe(
                client_id,
                transaction_id,
                key,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn psubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(Receiver<PStateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PSubscribe(
                client_id,
                transaction_id,
                pattern,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn subscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> WorterbuchResult<(Receiver<Vec<RegularKeySegment>>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::SubscribeLs(
                client_id,
                transaction_id,
                parent,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn unsubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Unsubscribe(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    pub async fn unsubscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::UnsubscribeLs(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    pub async fn delete(&self, key: Key, client_id: Uuid) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Delete(key, client_id, tx)).await?;
        rx.await?
    }

    pub async fn pdelete(
        &self,
        pattern: RequestPattern,
        client_id: Uuid,
    ) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PDelete(pattern, client_id, tx))
            .await?;
        rx.await?
    }

    pub async fn connected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Connected(client_id, remote_addr, protocol))
            .await?;
        Ok(())
    }

    pub async fn disconnected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Disconnected(client_id, remote_addr))
            .await?;
        Ok(())
    }

    pub async fn config(&self) -> WorterbuchResult<Config> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Config(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn export(&self, span: Span) -> WorterbuchResult<(Value, GraveGoods, LastWill)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Export(tx, span)).await?;
        Ok(rx.await?)
    }

    pub async fn import(
        &self,
        json: String,
    ) -> WorterbuchResult<Vec<(String, (ValueEntry, bool))>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Import(json, tx)).await?;
        rx.await?
    }

    pub async fn len(&self) -> WorterbuchResult<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Len(tx)).await?;
        Ok(rx.await?)
    }
}

#[derive(Serialize)]
struct Meta {
    cause: String,
    meta: MetaData,
}

impl From<(&Box<dyn std::error::Error + Send + Sync>, MetaData)> for Meta {
    fn from(e: (&Box<dyn std::error::Error + Send + Sync>, MetaData)) -> Self {
        Meta {
            cause: e.0.to_string(),
            meta: e.1,
        }
    }
}
