/*
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use serde_json::json;
use std::collections::BTreeMap;
use tokio::{select, sync::oneshot};
use tosub::Subsystem;
use totils::{CancelOn, UntangleOk};
use tracing::info;
use worterbuch::{ClusterRole, spawn_worterbuch};
use worterbuch_client::Ack;
use worterbuch_common::{
    protocol::v1::{ServerMessage, State, StateEvent, Value},
    topic,
};

#[tokio::test]
async fn set_and_get_sequences_are_preserved() {
    // worterbuch_common::logging::init().unwrap();

    tosub::build_default_root("set_and_get_sequences_are_preserved")
        .start(run)
        .await
        .unwrap();
}

#[derive(Debug, PartialEq)]
enum Response {
    Ack,
    State(Value),
}

async fn run(subsys: Subsystem) -> miette::Result<()> {
    let sync_port = 6060;
    let proxy_port = 8081;

    // create mock leader instance

    let (started_tx, started) = oneshot::channel();
    crate::test_common::mock_server::start(&subsys, sync_port, started_tx);

    started.await.unwrap();
    info!("Mock server is running, starting test …");

    // create actual proxy instance
    let mut wb_config = worterbuch::Config::new(None).await.unwrap();
    wb_config.role = ClusterRole::Proxy {
        leader_addresses: Box::new([format!("localhost:{}", sync_port)]),
    };

    spawn_worterbuch(&subsys, wb_config, None).await.unwrap();

    // create client that connects to proxy
    let config =
        worterbuch_client::config::Config::with_servers("tcp", [format!("localhost:{proxy_port}")]);
    let (client, mut disconnected) = worterbuch_client::connect(config)
        .or_cancel_on(subsys.shutdown_requested())
        .await
        .untangle_ok()
        .unwrap();

    let key = topic!("hello", "world");
    let mut responses_by_tid = BTreeMap::new();

    let mut responses = client.all_messages().await.unwrap();

    for i in 0..100 {
        let tid = client.set_async(key.clone(), json!(i)).await.unwrap();
        responses_by_tid.insert(tid, Response::Ack);
        let tid = client.get_async(key.clone()).await.unwrap();
        responses_by_tid.insert(tid, Response::State(json!(i)));
    }

    while !responses_by_tid.is_empty() {
        select! {
            _ = &mut disconnected => break,
            _ = subsys.shutdown_requested() => break,
            resp = responses.recv() => check_value(resp, &mut responses_by_tid),
        }
    }

    subsys.request_global_shutdown_because("test complete");

    Ok(())
}

fn check_value(resp: Option<ServerMessage>, tids: &mut BTreeMap<u64, Response>) {
    if let Some(msg) = resp {
        match msg {
            ServerMessage::Ack(Ack { transaction_id }) => {
                let expected_tid = *(tids.keys().next().unwrap());
                assert_eq!(transaction_id, expected_tid, "ack arrived out of order");
                let expected_response = tids.remove(&expected_tid).unwrap();
                assert_eq!(Response::Ack, expected_response, "tids got mixed up");
            }
            ServerMessage::State(State {
                transaction_id,
                event: StateEvent::Value(value),
                ..
            }) => {
                let expected_tid = *(tids.keys().next().unwrap());
                assert_eq!(transaction_id, expected_tid, "state arrived out of order");
                let expected_response = tids.remove(&expected_tid).unwrap();
                assert_eq!(
                    Response::State(value),
                    expected_response,
                    "get and set requests were processed out of order"
                );
            }
            _ => panic!("unexpected server message: {:?}", msg),
        }
    }
}
