pub mod config;
pub mod tcp;
pub mod ws;

use super::error::ConnectionResult;
use crate::{
    codec::{
        ClientMessage as CM, Export, Get, Import, KeyValuePairs, PGet, PSubscribe,
        ServerMessage as SM, Set, Subscribe, Value,
    },
    error::{ConnectionError, WorterbuchError},
};
use async_stream::stream;
use futures_core::stream::Stream;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc::UnboundedSender};

#[derive(Clone)]
pub struct Connection {
    cmd_tx: UnboundedSender<CM>,
    result_tx: broadcast::Sender<SM>,
    counter: Arc<Mutex<u64>>,
}

impl Connection {
    pub fn new(cmd_tx: UnboundedSender<CM>, result_tx: broadcast::Sender<SM>) -> Self {
        Self {
            cmd_tx,
            result_tx,
            counter: Arc::new(Mutex::new(1)),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Set(Set {
            transaction_id: i,
            key: key.to_owned(),
            value: value.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn get(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn pget(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern: key.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn subscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        self.do_subscribe(key, false)
    }

    pub fn subscribe_unique(&mut self, key: &str) -> ConnectionResult<u64> {
        self.do_subscribe(key, true)
    }

    fn do_subscribe(&mut self, key: &str, unique: bool) -> Result<u64, ConnectionError> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key: key.to_owned(),
            unique,
        }))?;
        Ok(i)
    }

    pub fn psubscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        self.do_psubscribe(key, false)
    }

    pub fn psubscribe_unique(&mut self, key: &str) -> ConnectionResult<u64> {
        self.do_psubscribe(key, true)
    }

    fn do_psubscribe(&mut self, key: &str, unique: bool) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern: key.to_owned(),
            unique,
        }))?;
        Ok(i)
    }

    pub fn export(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Export(Export {
            transaction_id: i,
            path: path.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn import(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Import(Import {
            transaction_id: i,
            path: path.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn responses(&mut self) -> broadcast::Receiver<SM> {
        self.result_tx.subscribe()
    }

    pub async fn get_value(&mut self, key: &str) -> ConnectionResult<Value> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key: key.to_owned(),
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

    pub async fn pget_values(&mut self, pattern: &str) -> ConnectionResult<KeyValuePairs> {
        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern: pattern.to_owned(),
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

    pub async fn subscribe_values(
        &mut self,
        key: &str,
    ) -> ConnectionResult<impl Stream<Item = Value>> {
        self.do_subscribe_values(key, false).await
    }

    pub async fn subscribe_unique_values(
        &mut self,
        key: &str,
    ) -> ConnectionResult<impl Stream<Item = Value>> {
        self.do_subscribe_values(key, true).await
    }

    async fn do_subscribe_values(
        &mut self,
        key: &str,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = Value>> {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key: key.to_owned(),
            unique,
        }))?;
        let owned_key = key.to_owned();
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!("subscription {tid} to key {key} failed");
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    while let Ok(msg) = subscr.recv().await {
                                        let tid = msg.transaction_id();
                                        if tid == i {
                                            match msg {
                                                SM::State(state) => {
                                                    yield state.key_value.value;
                                                }
                                                SM::Err(msg) => {
                                                    log::error!("Error in subscription of {owned_key}: {msg:?}");
                                                        break;
                                                }
                                                msg => log::warn!(
                                                    "received unrelated msg with subscription tid {tid}: {msg:?}"
                                                ),
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
        request_pattern: &str,
    ) -> ConnectionResult<impl Stream<Item = KeyValuePairs>> {
        self.do_psubscribe_values(request_pattern, false).await
    }

    pub async fn psubscribe_unique_values(
        &mut self,
        request_pattern: &str,
    ) -> ConnectionResult<impl Stream<Item = KeyValuePairs>> {
        self.do_psubscribe_values(request_pattern, true).await
    }

    async fn do_psubscribe_values(
        &mut self,
        request_pattern: &str,
        unique: bool,
    ) -> ConnectionResult<impl Stream<Item = KeyValuePairs>> {
        let mut subscr = self.responses();
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern: request_pattern.to_owned(),
            unique,
        }))?;
        let owned_pattern = request_pattern.to_owned();
        loop {
            match subscr.recv().await {
                Ok(msg) => {
                    let tid = msg.transaction_id();
                    if tid == i {
                        match msg {
                            SM::Err(msg) => {
                                log::warn!(
                                    "subscription {tid} to pattern {request_pattern} failed"
                                );
                                return Err(ConnectionError::WorterbuchError(
                                    WorterbuchError::ServerResponse(msg),
                                ));
                            }
                            SM::Ack(_) => {
                                return Ok(stream! {
                                    while let Ok(msg) = subscr.recv().await {
                                        let tid = msg.transaction_id();
                                        if tid == i {
                                            match msg {
                                                SM::PState(pstate) => {
                                                    yield pstate.key_value_pairs;
                                                }
                                                SM::Err(msg) => {
                                                    log::error!("Error in subscription of {owned_pattern}: {msg:?}");
                                                    break;
                                                }
                                                _ => { /* ignore */ }
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
