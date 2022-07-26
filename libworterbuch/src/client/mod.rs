pub mod config;
#[cfg(feature = "graphql")]
pub mod gql;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "ws")]
pub mod ws;

use super::error::ConnectionResult;
use crate::codec::{
    ClientMessage as CM, Export, Get, Import, PGet, PSubscribe, ServerMessage as SM, Set,
    Subscribe, Value, NO_SUCH_VALUE,
};
use tokio::{
    spawn,
    sync::{broadcast, mpsc::UnboundedSender, oneshot},
};

#[derive(Clone)]
pub struct Connection {
    cmd_tx: UnboundedSender<CM>,
    result_tx: broadcast::Sender<SM>,
    counter: u64,
}

impl Connection {
    pub fn new(cmd_tx: UnboundedSender<CM>, result_tx: broadcast::Sender<SM>) -> Self {
        Self {
            cmd_tx,
            result_tx,
            counter: 1,
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
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    pub fn psubscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern: key.to_owned(),
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

    pub async fn get_value(&mut self, key: &str) -> ConnectionResult<Option<Value>> {
        let (tx, rx) = oneshot::channel();

        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key: key.to_owned(),
        }))?;

        let owned_key = key.to_owned();
        spawn(async move {
            let mut value = None;
            while let Ok(msg) = subscr.recv().await {
                let tid = msg.transaction_id();
                if tid == i {
                    match msg {
                        SM::State(state) => {
                            value = Some(state.key_value.value);
                        }
                        SM::Err(msg) => {
                            if msg.error_code != NO_SUCH_VALUE {
                                log::error!("Error getting value of {owned_key}: {msg:?}");
                            }
                        }
                        _ => { /* ignore */ }
                    }
                    break;
                }
                // TODO time out
            }

            tx.send(value).ok();
        });

        let response = rx.await?;
        Ok(response)
    }

    pub async fn pget_values(&mut self, pattern: &str) -> ConnectionResult<Option<Vec<Value>>> {
        let (tx, rx) = oneshot::channel();

        let mut subscr = self.responses();

        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern: pattern.to_owned(),
        }))?;

        let owned_pattern = pattern.to_owned();
        spawn(async move {
            let mut value = None;
            while let Ok(msg) = subscr.recv().await {
                let tid = msg.transaction_id();
                if tid == i {
                    match msg {
                        SM::PState(pstate) => {
                            value = Some(
                                pstate
                                    .key_value_pairs
                                    .into_iter()
                                    .map(|kv| kv.value)
                                    .collect(),
                            );
                        }
                        SM::Err(msg) => {
                            if msg.error_code != NO_SUCH_VALUE {
                                log::error!("Error getting values for {owned_pattern}: {msg:?}");
                            }
                        }
                        _ => { /* ignore */ }
                    }
                    break;
                }
                // TODO time out
            }

            tx.send(value).ok();
        });

        let response = rx.await?;
        Ok(response)
    }

    fn inc_counter(&mut self) -> u64 {
        let i = self.counter;
        self.counter += 1;
        i
    }
}
