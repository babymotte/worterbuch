pub mod buffer;
pub mod config;
pub mod error;
mod ws;

use error::SubscriptionError;
use serde::{de::DeserializeOwned, Serialize};
pub use worterbuch_common::*;
pub use ws::*;

use async_stream::stream;
use futures_core::stream::Stream;
use std::sync::{Arc, Mutex};
use tokio::sync::{
    broadcast::{self},
    mpsc::UnboundedSender,
};
use worterbuch_common::{
    error::{ConnectionError, ConnectionResult, WorterbuchError},
    ClientMessage as CM, Export, Get, Import, KeyValuePairs, PGet, PSubscribe, ServerMessage as SM,
    Set, Subscribe, Value,
};

#[derive(Clone)]
pub struct Connection {
    cmd_tx: UnboundedSender<CM>,
    result_tx: broadcast::Sender<SM>,
    counter: Arc<Mutex<u64>>,
    pub separator: char,
    pub wildcard: char,
    pub multi_wildcard: char,
}

impl Connection {
    pub fn new(
        cmd_tx: UnboundedSender<CM>,
        result_tx: broadcast::Sender<SM>,
        separator: char,
        wildcard: char,
        multi_wildcard: char,
    ) -> Self {
        Self {
            cmd_tx,
            result_tx,
            counter: Arc::new(Mutex::new(1)),
            separator,
            wildcard,
            multi_wildcard,
        }
    }

    pub fn set(&mut self, key: String, value: Value) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Set(Set {
            transaction_id: i,
            key,
            value,
        }))?;
        Ok(i)
    }

    pub fn set_json<T: Serialize>(&mut self, key: String, value: &T) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        let value = serde_json::to_value(value)?;
        self.cmd_tx.send(CM::Set(Set {
            transaction_id: i,
            key,
            value,
        }))?;
        Ok(i)
    }

    pub fn get(&mut self, key: String) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key,
        }))?;
        Ok(i)
    }

    pub fn pget(&mut self, request_pattern: String) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern,
        }))?;
        Ok(i)
    }

    pub fn subscribe(&mut self, key: String) -> ConnectionResult<u64> {
        self.do_subscribe(key, false)
    }

    pub fn subscribe_unique(&mut self, key: String) -> ConnectionResult<u64> {
        self.do_subscribe(key, true)
    }

    fn do_subscribe(&mut self, key: String, unique: bool) -> Result<u64, ConnectionError> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key,
            unique,
        }))?;
        Ok(i)
    }

    pub fn psubscribe(&mut self, request_pattern: String) -> ConnectionResult<u64> {
        self.do_psubscribe(request_pattern, false)
    }

    pub fn psubscribe_unique(&mut self, request_pattern: String) -> ConnectionResult<u64> {
        self.do_psubscribe(request_pattern, true)
    }

    fn do_psubscribe(&mut self, request_pattern: String, unique: bool) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern,
            unique,
        }))?;
        Ok(i)
    }

    pub fn export(&mut self, path: String) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Export(Export {
            transaction_id: i,
            path,
        }))?;
        Ok(i)
    }

    pub fn import(&mut self, path: String) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Import(Import {
            transaction_id: i,
            path,
        }))?;
        Ok(i)
    }

    pub fn responses(&mut self) -> broadcast::Receiver<SM> {
        self.result_tx.subscribe()
    }

    pub async fn get_value(&mut self, key: String) -> ConnectionResult<Value> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key,
        }))?;

        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::State(state) => return Ok(state.key_value.value),
                            SM::Err(msg) => {
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ))
                            }
                            _ => { /* ignore */ }
                        }
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn get_json_value<T: DeserializeOwned>(
        &mut self,
        key: String,
    ) -> ConnectionResult<T> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key,
        }))?;

        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::State(state) => return deserialize_state_con(state),
                            SM::Err(msg) => {
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ))
                            }
                            _ => { /* ignore */ }
                        }
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn pget_values(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<KeyValuePairs> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern,
        }))?;

        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::PState(pstate) => return Ok(pstate.key_value_pairs),
                            SM::Err(msg) => {
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ))
                            }
                            msg => {
                                log::warn!("received unrelated msg with pget tid {tid}: {msg:?}")
                            }
                        }
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn pget_json_values<T: DeserializeOwned>(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<TypedKeyValuePairs<T>> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern,
        }))?;

        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::PState(pstate) => return deserialize_pstate_con(pstate),
                            SM::Err(msg) => {
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ))
                            }
                            msg => {
                                log::warn!("received unrelated msg with pget tid {tid}: {msg:?}")
                            }
                        }
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn subscribe_values(
        &mut self,
        key: String,
    ) -> ConnectionResult<impl Stream<Item = Result<Value, SubscriptionError>>> {
        self.do_subscribe_values(key, false).await
    }

    pub async fn subscribe_json_values<T: DeserializeOwned>(
        &mut self,
        key: String,
    ) -> ConnectionResult<impl Stream<Item = Result<T, SubscriptionError>>> {
        self.do_subscribe_json_values(key, false).await
    }

    pub async fn subscribe_unique_values(
        &mut self,
        key: String,
    ) -> ConnectionResult<impl Stream<Item = Result<Value, SubscriptionError>>> {
        self.do_subscribe_values(key, true).await
    }

    pub async fn subscribe_unique_json_values<T: DeserializeOwned>(
        &mut self,
        key: String,
    ) -> ConnectionResult<impl Stream<Item = Result<T, SubscriptionError>>> {
        self.do_subscribe_json_values(key, true).await
    }

    async fn do_subscribe_values(
        &mut self,
        key: String,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = Result<Value, SubscriptionError>>> {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        let owned_key = key.clone();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key,
            unique,
        }))?;
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!("subscription {tid} to key {owned_key} failed");
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    loop {
                                        match subscr.recv().await{
                                            Ok(msg) =>{let tid = msg.transaction_id();
                                                if tid == i {
                                                    match msg {
                                                        SM::State(state) => {
                                                            yield Ok(state.key_value.value);
                                                        }
                                                        SM::Err(err) => {
                                                            log::error!("Error in subscription of {owned_key}: {err:?}");
                                                            yield Err(SubscriptionError::ServerError(err));
                                                                break;
                                                        }
                                                        msg => log::warn!(
                                                            "received unrelated msg with subscription tid {tid}: {msg:?}"
                                                        ),
                                                    }
                                                }}
                                            Err(e) => {
                                                yield Err(SubscriptionError::RecvError(e));
                                                break;
                                            }
                                        }
                                    }
                                });
                            }
                            msg => log::warn!(
                                "received unrelated msg with subscription tid {tid}: {msg:?}"
                            ),
                        }
                        break;
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(ConnectionError::WorterbuchError(
            WorterbuchError::NotSubscribed,
        ))
    }

    async fn do_subscribe_json_values<T: DeserializeOwned>(
        &mut self,
        key: String,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = Result<T, SubscriptionError>>> {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        let owned_key = key.clone();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key,
            unique,
        }))?;
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!("subscription {tid} to key {owned_key} failed");
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    loop {
                                        match subscr.recv().await{
                                            Ok(msg) =>{let tid = msg.transaction_id();
                                                if tid == i {
                                                    match msg {
                                                        SM::State(state) => {
                                                            yield deserialize_state_sub::<T>(state);
                                                        }
                                                        SM::Err(err) => {
                                                            log::error!("Error in subscription of {owned_key}: {err:?}");
                                                            yield Err(SubscriptionError::ServerError(err));
                                                                break;
                                                        }
                                                        msg => log::warn!(
                                                            "received unrelated msg with subscription tid {tid}: {msg:?}"
                                                        ),
                                                    }
                                                }}
                                            Err(e) => {
                                                yield Err(SubscriptionError::RecvError(e));
                                                break;
                                            }
                                        }
                                    }
                                });
                            }
                            msg => log::warn!(
                                "received unrelated msg with subscription tid {tid}: {msg:?}"
                            ),
                        }
                        break;
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(ConnectionError::WorterbuchError(
            WorterbuchError::NotSubscribed,
        ))
    }

    pub async fn psubscribe_values(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<impl Stream<Item = Result<KeyValuePairs, SubscriptionError>>> {
        self.do_psubscribe_values(request_pattern, false).await
    }

    pub async fn psubscribe_json_values<T: DeserializeOwned>(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<impl Stream<Item = Result<TypedKeyValuePairs<T>, SubscriptionError>>>
    {
        self.do_psubscribe_json_values::<T>(request_pattern, false)
            .await
    }

    pub async fn psubscribe_unique_values(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<impl Stream<Item = Result<KeyValuePairs, SubscriptionError>>> {
        self.do_psubscribe_values(request_pattern, true).await
    }

    pub async fn psubscribe_unique_json_values<T: DeserializeOwned>(
        &mut self,
        request_pattern: String,
    ) -> ConnectionResult<impl Stream<Item = Result<TypedKeyValuePairs<T>, SubscriptionError>>>
    {
        self.do_psubscribe_json_values(request_pattern, true).await
    }

    async fn do_psubscribe_values(
        &mut self,
        request_pattern: String,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = Result<KeyValuePairs, SubscriptionError>>> {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        let owned_pattern = request_pattern.clone();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern,
            unique,
        }))?;
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!("subscription {tid} to pattern {owned_pattern} failed");
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    loop {
                                        match subscr.recv().await {
                                            Ok(msg) => {
                                                let tid = msg.transaction_id();
                                        if tid == i {
                                            match msg {
                                                SM::PState(pstate) => {
                                                    yield Ok(pstate.key_value_pairs);
                                                }
                                                SM::Err(err) => {
                                                    log::error!("Error in subscription of {owned_pattern}: {err:?}");
                                                    yield Err(SubscriptionError::ServerError(err));
                                                    break;
                                                }
                                                _ => { /* ignore */ }
                                            }
                                        }
                                            },
                                            Err(e) => {
                                                log::error!("Error receiving message: {e}");
                                                yield Err(SubscriptionError::RecvError(e));
                                                break;
                                            }
                                        }
                                        // TODO time out
                                    }
                                });
                            }
                            msg => log::warn!(
                                "received unrelated msg with subscription tid {tid}: {msg:?}"
                            ),
                        }
                        break;
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(ConnectionError::WorterbuchError(
            WorterbuchError::NotSubscribed,
        ))
    }

    async fn do_psubscribe_json_values<T: DeserializeOwned>(
        &mut self,
        request_pattern: String,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = Result<TypedKeyValuePairs<T>, SubscriptionError>>>
    {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        let owned_pattern = request_pattern.clone();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern,
            unique,
        }))?;
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!("subscription {tid} to pattern {owned_pattern} failed");
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    loop {
                                        match subscr.recv().await {
                                            Ok(msg) => {
                                                let tid = msg.transaction_id();
                                        if tid == i {
                                            match msg {
                                                SM::PState(pstate) => {
                                                    yield deserialize_pstate_sub(pstate);
                                                }
                                                SM::Err(err) => {
                                                    log::error!("Error in subscription of {owned_pattern}: {err:?}");
                                                    yield Err(SubscriptionError::ServerError(err));
                                                    break;
                                                }
                                                _ => { /* ignore */ }
                                            }
                                        }
                                            },
                                            Err(e) => {
                                                log::error!("Error receiving message: {e}");
                                                yield Err(SubscriptionError::RecvError(e));
                                                break;
                                            }
                                        }
                                        // TODO time out
                                    }
                                });
                            }
                            msg => log::warn!(
                                "received unrelated msg with subscription tid {tid}: {msg:?}"
                            ),
                        }
                        break;
                    }
                    // TODO time out
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(ConnectionError::WorterbuchError(
            WorterbuchError::NotSubscribed,
        ))
    }

    pub fn send(&mut self, msg: CM) -> ConnectionResult<()> {
        self.cmd_tx.send(msg)?;
        Ok(())
    }

    fn inc_counter(&mut self) -> u64 {
        let mut counter = self.counter.lock().expect("counter mutex poisoned");
        let current = *counter;
        *counter += 1;
        current
    }
}

fn deserialize_state_con<T: DeserializeOwned>(state: State) -> Result<T, ConnectionError> {
    let deserialized = serde_json::from_value(state.key_value.value)?;
    Ok(deserialized)
}

fn deserialize_pstate_con<T: DeserializeOwned>(
    pstate: PState,
) -> Result<TypedKeyValuePairs<T>, ConnectionError> {
    let mut pairs = TypedKeyValuePairs::new();
    for KeyValuePair { key, value } in pstate.key_value_pairs {
        let deserialized = serde_json::from_value(value)?;
        pairs.push(TypedKeyValuePair {
            key,
            value: deserialized,
        });
    }
    Ok(pairs)
}

fn deserialize_state_sub<T: DeserializeOwned>(state: State) -> Result<T, SubscriptionError> {
    let deserialized = serde_json::from_value(state.key_value.value)?;
    Ok(deserialized)
}

fn deserialize_pstate_sub<T: DeserializeOwned>(
    pstate: PState,
) -> Result<TypedKeyValuePairs<T>, SubscriptionError> {
    let mut pairs = TypedKeyValuePairs::new();
    for KeyValuePair { key, value } in pstate.key_value_pairs {
        let deserialized = serde_json::from_value(value)?;
        pairs.push(TypedKeyValuePair {
            key,
            value: deserialized,
        });
    }
    Ok(pairs)
}
