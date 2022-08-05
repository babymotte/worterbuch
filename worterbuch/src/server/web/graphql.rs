use crate::{subscribers::SubscriptionId, worterbuch::Worterbuch};
use futures::Stream;
use juniper::{graphql_object, graphql_subscription, graphql_value, FieldError, FieldResult};
use libworterbuch::codec::{KeyValuePair, TransactionId};
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Context {
    pub database: Arc<RwLock<Worterbuch>>,
    pub client_id: Uuid,
}
impl juniper::Context for Context {}

impl Context {
    pub fn new(database: Arc<RwLock<Worterbuch>>) -> Self {
        Self {
            client_id: Uuid::new_v4(),
            database,
        }
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
    async fn unsubscribe(
        &self,
        transaction_id: f64,
        pattern: String,
        context: &Context,
    ) -> FieldResult<String> {
        let mut worterbuch = context.database.write().await;
        let subscription = SubscriptionId::new(context.client_id, transaction_id as u64);
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
    async fn psubscribe(
        &self,
        transaction_id: f64,
        pattern: String,
        context: &Context,
    ) -> PEventStream {
        let mut worterbuch = context.database.write().await;
        let rx = worterbuch.psubscribe(
            context.client_id,
            transaction_id as TransactionId,
            pattern.clone(),
        );
        let stream = async_stream::stream! {
            if let Ok((mut rx, _)) = rx {
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
    async fn subscribe(&self, transaction_id: f64, key: String, context: &Context) -> EventStream {
        let mut worterbuch = context.database.write().await;
        let rx = worterbuch.subscribe(
            context.client_id,
            transaction_id as TransactionId,
            key.clone(),
        );
        let stream = async_stream::stream! {
            if let Ok((mut rx, _)) = rx {
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
