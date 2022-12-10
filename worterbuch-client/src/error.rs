use std::fmt;

use tokio::sync::broadcast;
use worterbuch_common::error::Err;

#[derive(Debug, PartialEq, Clone)]
pub enum SubscriptionError {
    RecvError(broadcast::error::RecvError),
    ServerError(Err),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscriptionError::RecvError(e) => e.fmt(f),
            SubscriptionError::ServerError(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for SubscriptionError {}
