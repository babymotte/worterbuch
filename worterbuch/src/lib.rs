mod config;
mod persistence;
mod server;
mod stats;
mod store;
mod subscribers;
mod worterbuch;

pub use crate::worterbuch::*;
pub use config::*;
use server::common::{CloneableWbApi, WbFunction};
use stats::{SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSIONS};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_common::topic;

use crate::stats::track_stats;
use anyhow::Result;
use tokio::{select, sync::mpsc};

pub async fn run_worterbuch(subsys: SubsystemHandle) -> Result<()> {
    let config = Config::new()?;
    let config_pers = config.clone();

    let channel_buffer_size = config.channel_buffer_size;

    let use_persistence = config.use_persistence;

    let mut worterbuch = if use_persistence {
        persistence::load(config.clone()).await?
    } else {
        Worterbuch::with_config(config.clone())
    };

    worterbuch.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSIONS),
        serde_json::to_value(worterbuch.supported_protocol_versions()).expect("cannot fail"),
    )?;

    let (api_tx, mut api_rx) = mpsc::channel(channel_buffer_size);
    let api = CloneableWbApi::new(api_tx);

    let worterbuch_pers = api.clone();
    let worterbuch_uptime = api.clone();

    if use_persistence {
        subsys.start("persistence", |subsys| {
            persistence::periodic(worterbuch_pers, config_pers, subsys)
        });
    }

    let sapi = api.clone();
    let sconf = config.clone();
    subsys.start("stats", |subsys| track_stats(worterbuch_uptime, subsys));
    subsys.start("webserver", |subsys| {
        server::poem::start(sapi, sconf, subsys)
    });

    loop {
        select! {
            recv = api_rx.recv() => match recv {
                Some(function) => process_api_call(&mut worterbuch, function).await,
                None => break,
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    log::info!("Shutting down.");

    if use_persistence {
        persistence::once(&api, config).await?;
    }

    Ok(())
}

async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    match function {
        WbFunction::Handshake(client_protocol_versions, last_will, grave_goods, client_id, tx) => {
            tx.send(worterbuch.handshake(
                &client_protocol_versions,
                last_will,
                grave_goods,
                client_id,
            ))
            .ok();
        }
        WbFunction::Get(key, tx) => {
            tx.send(worterbuch.get(&key)).ok();
        }
        WbFunction::Set(key, value, tx) => {
            tx.send(worterbuch.set(key, value)).ok();
        }
        WbFunction::Publish(key, value, tx) => {
            tx.send(worterbuch.publish(key, value)).ok();
        }
        WbFunction::Ls(parent, tx) => {
            tx.send(worterbuch.ls(&parent)).ok();
        }
        WbFunction::PGet(pattern, tx) => {
            tx.send(worterbuch.pget(&pattern)).ok();
        }
        WbFunction::Subscribe(client_id, transaction_id, key, unique, tx) => {
            tx.send(worterbuch.subscribe(client_id, transaction_id, key, unique))
                .ok();
        }
        WbFunction::PSubscribe(client_id, transaction_id, pattern, unique, tx) => {
            tx.send(worterbuch.psubscribe(client_id, transaction_id, pattern, unique))
                .ok();
        }
        WbFunction::SubscribeLs(client_id, transaction_id, parent, tx) => {
            tx.send(worterbuch.subscribe_ls(client_id, transaction_id, parent))
                .ok();
        }
        WbFunction::Unsubscribe(client_id, transaction_id, tx) => {
            tx.send(worterbuch.unsubscribe(client_id, transaction_id))
                .ok();
        }
        WbFunction::UnsubscribeLs(client_id, transaction_id, tx) => {
            tx.send(worterbuch.unsubscribe_ls(client_id, transaction_id))
                .ok();
        }
        WbFunction::Delete(key, tx) => {
            tx.send(worterbuch.delete(key)).ok();
        }
        WbFunction::PDelete(pattern, tx) => {
            tx.send(worterbuch.pdelete(pattern)).ok();
        }
        WbFunction::Connected(client_id, remote_addr) => {
            worterbuch.connected(client_id, remote_addr);
        }
        WbFunction::Disconnected(client_id, remote_addr) => {
            worterbuch.disconnected(client_id, remote_addr).ok();
        }
        WbFunction::Config(tx) => {
            tx.send(worterbuch.config().clone()).ok();
        }
        WbFunction::Export(tx) => {
            tx.send(worterbuch.export()).ok();
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
        WbFunction::SupportedProtocolVersions(tx) => {
            tx.send(worterbuch.supported_protocol_versions()).ok();
        }
    }
}
