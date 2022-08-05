use crate::{subscribers::SubscriptionId, worterbuch::Worterbuch};
use futures::Stream;
use juniper::{graphql_object, graphql_subscription, graphql_value, FieldError, FieldResult};
use libworterbuch::{
    codec::{KeyValuePair, TransactionId},
    error::WorterbuchError,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Context {
    pub database: Arc<RwLock<Worterbuch>>,
    pub client_id: Uuid,
    pub tid: Arc<Mutex<TransactionId>>,
    pub subscribed_patterns: Arc<Mutex<HashMap<TransactionId, String>>>,
}
impl juniper::Context for Context {}

impl Context {
    pub fn new(database: Arc<RwLock<Worterbuch>>) -> Self {
        Self {
            client_id: Uuid::new_v4(),
            database,
            tid: Arc::new(Mutex::new(1)),
            subscribed_patterns: Arc::default(),
        }
    }
    pub(crate) fn next_tid(&self) -> TransactionId {
        let mut tidg = self.tid.lock().expect("mutex poisoned");
        let tid = *tidg;
        *tidg = tid + 1;
        tid
    }
}

pub(crate) struct PEvent {
    pattern: String,
    key: String,
    value: String,
}

#[graphql_object(context = Context)]
impl PEvent {
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

pub(crate) struct Event {
    key: String,
    value: String,
}

#[graphql_object(context = Context)]
impl Event {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

pub(crate) struct Query;

#[graphql_object(context = Context)]
impl Query {
    /// Get the current API version
    fn api_version() -> &'static str {
        "0.1"
    }

    /// Gets all values matching the given key pattern.
    async fn pget(&self, pattern: String, context: &Context) -> FieldResult<Vec<PEvent>> {
        let worterbuch = context.database.read().await;
        let result = worterbuch.pget(&pattern)?;
        let result = result
            .into_iter()
            .map(|s| PEvent {
                pattern: pattern.clone(),
                key: s.key,
                value: s.value,
            })
            .collect();
        Ok(result)
    }

    /// Get the value of the given key.
    async fn get(&self, key: String, context: &Context) -> FieldResult<Event> {
        let worterbuch = context.database.read().await;
        let (key, value) = worterbuch.get(&key)?;
        Ok(Event { key, value })
    }
}

pub(crate) struct Mutation;

#[graphql_object(context = Context)]
impl Mutation {
    /// Sets a key to the given value.
    async fn set(&self, key: String, value: String, context: &Context) -> FieldResult<String> {
        let mut worterbuch = context.database.write().await;
        worterbuch.set(key, value)?;
        Ok("Ok".to_owned())
    }

    /// Cancels an active subscription.
    async fn unsubscribe(&self, transaction_id: i32, context: &Context) -> FieldResult<String> {
        let transaction_id = transaction_id as TransactionId;
        let mut worterbuch = context.database.write().await;
        let subscription = SubscriptionId::new(context.client_id, transaction_id as u64);
        let pattern = context
            .subscribed_patterns
            .lock()
            .expect("poisoned mutex")
            .remove(&transaction_id)
            .ok_or_else(|| WorterbuchError::NotSubscribed)?;
        worterbuch.unsubscribe(&pattern, &subscription)?;
        Ok("Ok".to_owned())
    }
}

type PEventStream = Pin<Box<dyn Stream<Item = Result<PEvent, FieldError>> + Send>>;
type EventStream = Pin<Box<dyn Stream<Item = Result<Event, FieldError>> + Send>>;

pub(crate) struct Subscription;

#[graphql_subscription(context = Context)]
impl Subscription {
    /// Subscribe to key/value changes matching a pattern.
    async fn psubscribe(&self, pattern: String, context: &Context) -> PEventStream {
        let mut worterbuch = context.database.write().await;
        let transaction_id = context.next_tid();
        context
            .subscribed_patterns
            .lock()
            .expect("posoned mutex")
            .insert(transaction_id, pattern.clone());
        let rx = worterbuch.psubscribe(context.client_id, transaction_id, pattern.clone());
        let stream = async_stream::stream! {
            if let Ok((mut rx, _)) = rx {
                yield Ok(PEvent{
                    pattern: "transactionId".to_owned(),
                    key: "transactionId".to_owned(),
                    value: format!("{transaction_id}"),
                });
                loop {
                    match rx.recv().await {
                        Some(event) => {
                            for KeyValuePair{ key, value } in event {
                                let event = PEvent{
                                    pattern: pattern.clone(),
                                    key,
                                    value,
                                };
                                yield Ok(event)
                            }
                        },
                        None => {
                            yield Err(FieldError::new(
                                "no more data",
                                graphql_value!("no more data"),
                            ));
                            break;
                        },
                    }
                }
                log::warn!("subscription ended");
            }
        };
        Box::pin(stream)
    }

    /// Subscribe to key/value changes of a key.
    async fn subscribe(&self, key: String, context: &Context) -> EventStream {
        let mut worterbuch = context.database.write().await;
        let transaction_id = context.next_tid();
        context
            .subscribed_patterns
            .lock()
            .expect("posoned mutex")
            .insert(transaction_id, key.clone());
        let rx = worterbuch.subscribe(context.client_id, transaction_id, key.clone());
        let stream = async_stream::stream! {
            if let Ok((mut rx, _)) = rx {
                yield Ok(Event{
                    key: "transactionId".to_owned(),
                    value: format!("{transaction_id}"),
                });
                loop {
                    match rx.recv().await {
                        Some(event) => {
                            for KeyValuePair{ key, value } in event {
                                let event = Event{
                                    key,
                                    value,
                                };
                                yield Ok(event)
                            }
                        },
                        None => {
                            yield Err(FieldError::new(
                                "no more data",
                                graphql_value!("no more data"),
                            ));
                            break;
                        },
                    }
                }
                log::warn!("subscription ended");
            }
        };
        Box::pin(stream)
    }
}

pub(crate) type Schema = juniper::RootNode<'static, Query, Mutation, Subscription>;

pub(crate) fn schema() -> Schema {
    Schema::new(Query, Mutation, Subscription)
}
