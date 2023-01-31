use std::fmt;

use tokio::sync::broadcast;
use worterbuch_common::Err;

#[derive(Debug)]
pub enum SubscriptionError {
    RecvError(broadcast::error::RecvError),
    ServerError(Err),
    SerdeError(serde_json::Error),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscriptionError::RecvError(e) => e.fmt(f),
            SubscriptionError::ServerError(e) => e.fmt(f),
            SubscriptionError::SerdeError(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl From<serde_json::Error> for SubscriptionError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(e)
    }
}
