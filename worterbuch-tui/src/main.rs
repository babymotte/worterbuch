mod backend;
mod controller;
mod tui;

use crate::controller::{AppApi, ClientAddress};
use std::ops::ControlFlow;
use tokio::spawn;
use tosub::{SubsystemHandle, SubsystemResult};
use totils::while_select;
use worterbuch_client::config::Config;

fn init_logging() {
    // init tracing_subscriber to log to a file
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> SubsystemResult {
    init_logging();

    tosub::build_default_root("worterbuch-tui")
        .start(backend::start)
        .await
}
