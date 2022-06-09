use crate::worterbuch::Worterbuch;
use futures::Stream;
use juniper::{graphql_object, graphql_subscription, graphql_value, FieldError, FieldResult};
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct Context {
    pub database: Arc<RwLock<Worterbuch>>,
}
impl juniper::Context for Context {}

pub(crate) struct Event {
    pattern: String,
    key: String,
    value: String,
}

#[graphql_object(context = Context)]
impl Event {
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

pub(crate) struct Query;

#[graphql_object(context = Context)]
impl Query {
    /// Get the current API version
    fn api_version() -> &'static str {
        "0.1"
    }

    /// Gets all values matching the given key pattern.
    async fn get(&self, pattern: String, context: &Context) -> FieldResult<Vec<Event>> {
        let worterbuch = context.database.read().await;
        let result = worterbuch.get_all(&pattern)?;
        let result = result
            .into_iter()
            .map(|s| Event {
                pattern: pattern.clone(),
                key: s.0,
                value: s.1,
            })
            .collect();
        Ok(result)
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
}

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, FieldError>> + Send>>;

pub(crate) struct Subscription;

#[graphql_subscription(context = Context)]
impl Subscription {
    /// Subscribe to key/value changes.
    async fn all(&self, pattern: String, context: &Context) -> EventStream {
        let mut worterbuch = context.database.write().await;
        let rx = worterbuch.subscribe(pattern.clone());
        let stream = async_stream::stream! {
            if let Ok(mut rx) = rx {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let event = Event{
                                pattern: pattern.clone(),
                                key: event.0,
                                value: event.1,
                            };
                            yield Ok(event)
                        },
                        Err(e) => {
                            yield Err(FieldError::new(
                                e.to_string(),
                                graphql_value!(e.to_string()),
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
