use tokio::sync::broadcast;
use worterbuch_common::error::Err;

pub enum SubscriptionError {
    RecvError(broadcast::error::RecvError),
    ServerError(Err),
}
