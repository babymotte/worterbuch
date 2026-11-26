/*
 *  Worterbuch client disconnect benchmark
 *
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

//! Measures how long [`Worterbuch::disconnected`] takes for a client with many subscriptions in a
//! large store, in proxy and in standalone (leader) mode.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p worterbuch disconnect_bench -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The scenario can be tuned with environment variables:
//!
//! - `BENCH_KEYS`: total number of keys in the store (default 2,000,000)
//! - `BENCH_TOP_LEVEL`: number of distinct top level key segments (default 1,000)
//! - `BENCH_CLIENTS`: number of other connected clients (default 100)
//! - `BENCH_SUBS`: subscriptions per client, including the disconnecting one (default 500)
//! - `BENCH_SYS_SUBSCRIBER`: if `true`, an additional client subscribes to `$SYS/#` (default false)

#![allow(clippy::as_conversions)]
#![allow(clippy::unwrap_used)]

use super::*;
use std::env;
use std::time::Instant;
use worterbuch_common::protocol::v1::ClientInfo;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|it| it.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .map(|it| it == "true" || it == "1")
        .unwrap_or(false)
}

struct Scenario {
    keys: usize,
    top_level: usize,
    clients: usize,
    subs: usize,
    sys_subscriber: bool,
}

impl Scenario {
    fn from_env() -> Self {
        Scenario {
            keys: env_usize("BENCH_KEYS", 2_000_000),
            top_level: env_usize("BENCH_TOP_LEVEL", 1_000),
            clients: env_usize("BENCH_CLIENTS", 100),
            subs: env_usize("BENCH_SUBS", 500),
            sys_subscriber: env_bool("BENCH_SYS_SUBSCRIBER"),
        }
    }
}

fn client(protocol: Protocol) -> ClientInfo {
    ClientInfo {
        client_id: ClientId::new_v4(),
        protocol,
        client_name: None,
    }
}

/// Keys are laid out as `t{top}/g{group}/k{key}` with 100 keys per group.
fn key(scenario: &Scenario, i: usize) -> String {
    let top = i % scenario.top_level;
    let rest = i / scenario.top_level;
    format!("t{top}/g{}/k{}", rest / 100, rest % 100)
}

/// A mix of exact keys and wildcard patterns spread over the whole key space.
fn pattern(scenario: &Scenario, client_no: usize, sub_no: usize) -> String {
    let i = (client_no * 7919 + sub_no * 104_729) % scenario.keys;
    let k = key(scenario, i);
    match sub_no % 4 {
        0 | 1 => k,
        2 => {
            let mut segs: Vec<&str> = k.split('/').collect();
            segs[2] = "?";
            segs.join("/")
        }
        _ => {
            let segs: Vec<&str> = k.split('/').collect();
            format!("{}/{}/#", segs[0], segs[1])
        }
    }
}

struct Setup {
    wb: Worterbuch,
    victim: ClientInfo,
    // keep receivers and eject channels alive so subscriptions stay valid
    _keep: Vec<Box<dyn std::any::Any + Send>>,
}

async fn setup(scenario: &Scenario, role: ClusterRole, extended_monitoring: bool) -> Setup {
    let mut config = Config::new(None).await.unwrap();
    config.use_persistence = false;
    config.role = role;
    config.extended_monitoring = extended_monitoring;
    config.channel_buffer_size = 1_000_000;
    let mut wb = Worterbuch::with_config(config);

    let start = Instant::now();
    for i in 0..scenario.keys {
        let path = parse_segments(&key(scenario, i)).unwrap();
        wb.store.insert_plain(&path, json!(i), false).unwrap();
    }
    eprintln!(
        "  filled store with {} keys in {:?}",
        scenario.keys,
        start.elapsed()
    );

    let mut keep: Vec<Box<dyn std::any::Any + Send>> = Vec::new();

    if scenario.sys_subscriber {
        let sys_client = client(Protocol::Tcp);
        let (eject, eject_rx) = mpsc::channel(1);
        wb.connected(sys_client.clone(), None, eject).await.unwrap();
        keep.push(Box::new(eject_rx));
        let (mut rx, _) = wb
            .psubscribe(
                "$SYS/#".to_owned(),
                SubscriptionFlags::new(false, true, false),
                TraceData::new(1, sys_client),
            )
            .await
            .unwrap();
        // drain like a well-behaved client would
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
    }

    let start = Instant::now();
    let mut victim = None;
    for c in 0..=scenario.clients {
        let info = client(Protocol::Tcp);
        let (eject, eject_rx) = mpsc::channel(1);
        wb.connected(info.clone(), None, eject).await.unwrap();
        keep.push(Box::new(eject_rx));
        for s in 0..scenario.subs {
            let p = pattern(scenario, c, s);
            let flags = SubscriptionFlags::new(false, true, false);
            let td = TraceData::new(s as TransactionId + 1, info.clone());
            if p.contains('?') || p.contains('#') {
                let (rx, _) = wb.psubscribe(p, flags, td).await.unwrap();
                keep.push(Box::new(rx));
            } else {
                let (rx, _) = wb.subscribe(p, flags, td).await.unwrap();
                keep.push(Box::new(rx));
            }
        }
        if c == 0 {
            victim = Some(info);
        }
    }
    eprintln!(
        "  connected {} clients with {} subscriptions each in {:?} ({} keys in store now)",
        scenario.clients + 1,
        scenario.subs,
        start.elapsed(),
        wb.store.len()
    );

    Setup {
        wb,
        victim: victim.unwrap(),
        _keep: keep,
    }
}

async fn run(scenario: &Scenario, label: &str, role: ClusterRole, extended_monitoring: bool) {
    eprintln!("{label}:");
    let Setup {
        mut wb,
        victim,
        _keep,
    } = setup(scenario, role, extended_monitoring).await;
    let start = Instant::now();
    wb.disconnected(victim, None).await;
    eprintln!("  => disconnected() took {:?}", start.elapsed());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark, run explicitly with --ignored --nocapture"]
async fn disconnect_bench() {
    let scenario = Scenario::from_env();
    eprintln!(
        "keys: {}, top level segments: {}, other clients: {}, subs/client: {}, $SYS/# subscriber: {}",
        scenario.keys, scenario.top_level, scenario.clients, scenario.subs, scenario.sys_subscriber
    );

    let proxy = || ClusterRole::Proxy {
        leader_addresses: Box::new(["localhost:1234".to_owned()]),
    };

    run(&scenario, "proxy, extended monitoring", proxy(), true).await;
    run(&scenario, "proxy, no extended monitoring", proxy(), false).await;
    run(
        &scenario,
        "standalone, extended monitoring",
        ClusterRole::Standalone,
        true,
    )
    .await;
}

/// Same measurement as [`disconnect_bench`], but against a real store loaded from an SQLite file
/// (`BENCH_DB`, default `tests/data/worterbuch.sqlite.db`; the file is opened without modifying
/// its schema, but working on a copy is still recommended).
///
/// Subscriptions of the disconnecting client are sampled from the real keys, either spread across
/// the whole store or concentrated under a single parent (`BENCH_PARENT`, one exact subscription
/// per child of that parent). Further tuning:
///
/// - `BENCH_SUBS`: comma separated subscription counts of the disconnecting client
///   (default `1000,5000,20000`)
/// - `BENCH_CLIENTS` / `BENCH_BG_SUBS`: other clients and their (spread) subscriptions
///   (default 100 / 300)
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark, run explicitly with --ignored --nocapture"]
async fn disconnect_bench_sqlite() {
    let db = env::var("BENCH_DB").unwrap_or_else(|_| "tests/data/worterbuch.sqlite.db".to_owned());
    let parent = env::var("BENCH_PARENT").unwrap_or_else(|_| "stagenetSolution/blocks".to_owned());
    let sub_counts: Vec<usize> = env::var("BENCH_SUBS")
        .unwrap_or_else(|_| "1000,5000,20000".to_owned())
        .split(',')
        .map(|it| it.trim().parse().unwrap())
        .collect();
    let bg_clients = env_usize("BENCH_CLIENTS", 100);
    let bg_subs = env_usize("BENCH_BG_SUBS", 300);

    let start = Instant::now();
    let (store, keys) = crate::persistence::load_store_with_keys(db.as_ref()).unwrap();
    eprintln!(
        "loaded {} keys from {db} in {:?}",
        store.len(),
        start.elapsed()
    );

    let mut config = Config::new(None).await.unwrap();
    config.use_persistence = false;
    config.channel_buffer_size = 1_000_000;
    let mut wb = Worterbuch::with_store(store, config);
    let proxy = || ClusterRole::Proxy {
        leader_addresses: Box::new(["localhost:1234".to_owned()]),
    };
    wb.config.role = proxy();
    wb.config.extended_monitoring = true;

    let mut keep: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
    let mut tid: TransactionId = 0;

    // spread: evenly sampled keys, a mix of exact keys, `?` and `#` patterns
    let spread = |n: usize, offset: usize| -> Vec<String> {
        (0..n)
            .map(|i| {
                let k = &keys[(i * keys.len() / n + offset) % keys.len()];
                let mut segs: Vec<&str> = k.split('/').collect();
                match i % 4 {
                    2 if segs.len() > 1 => {
                        let last = segs.len() - 1;
                        segs[last] = "?";
                        segs.join("/")
                    }
                    3 if segs.len() > 1 => {
                        segs.pop();
                        format!("{}/#", segs.join("/"))
                    }
                    _ => k.clone(),
                }
            })
            .collect()
    };

    // concentrated: one exact key per child of `parent`
    let prefix = format!("{parent}/");
    let mut seen = hashbrown::HashSet::new();
    let concentrated: Vec<String> = keys
        .iter()
        .filter(|k| k.starts_with(&prefix))
        .filter(|k| {
            let child = k[prefix.len()..].split('/').next().unwrap_or_default();
            seen.insert(child.to_owned())
        })
        .cloned()
        .collect();
    eprintln!(
        "{} children with values below {parent}",
        concentrated.len()
    );

    async fn subscribe_all(
        wb: &mut Worterbuch,
        info: &ClientInfo,
        patterns: &[String],
        tid: &mut TransactionId,
        keep: &mut Vec<Box<dyn std::any::Any + Send>>,
    ) {
        for p in patterns {
            *tid += 1;
            let flags = SubscriptionFlags::new(false, true, false);
            let td = TraceData::new(*tid, info.clone());
            if p.contains('?') || p.contains('#') {
                let (rx, _) = wb.psubscribe(p.clone(), flags, td).await.unwrap();
                keep.push(Box::new(rx));
            } else {
                let (rx, _) = wb.subscribe(p.clone(), flags, td).await.unwrap();
                keep.push(Box::new(rx));
            }
        }
    }

    let start = Instant::now();
    for c in 0..bg_clients {
        let info = client(Protocol::Tcp);
        let (eject, eject_rx) = mpsc::channel(1);
        wb.connected(info.clone(), None, eject).await.unwrap();
        keep.push(Box::new(eject_rx));
        subscribe_all(&mut wb, &info, &spread(bg_subs, c * 31), &mut tid, &mut keep).await;
    }
    eprintln!(
        "connected {bg_clients} background clients with {bg_subs} subscriptions each in {:?}",
        start.elapsed()
    );

    let modes: [(&str, fn() -> ClusterRole, bool); 3] = [
        ("proxy, ext. monitoring", || ClusterRole::Proxy {
            leader_addresses: Box::new(["localhost:1234".to_owned()]),
        }, true),
        ("proxy, no ext. monitoring", || ClusterRole::Proxy {
            leader_addresses: Box::new(["localhost:1234".to_owned()]),
        }, false),
        ("standalone, ext. monitoring", || ClusterRole::Standalone, true),
    ];

    for (kind, patterns) in [("spread", None), ("concentrated", Some(&concentrated))] {
        for &n in &sub_counts {
            let patterns = match patterns {
                Some(c) => c.iter().take(n).cloned().collect::<Vec<_>>(),
                None => spread(n, 7),
            };
            for (label, role, ext) in &modes {
                wb.config.role = role();
                wb.config.extended_monitoring = *ext;
                let victim = client(Protocol::Tcp);
                let (eject, _eject_rx) = mpsc::channel(1);
                wb.connected(victim.clone(), None, eject).await.unwrap();
                let mut victim_keep = Vec::new();
                subscribe_all(&mut wb, &victim, &patterns, &mut tid, &mut victim_keep).await;
                let start = Instant::now();
                wb.disconnected(victim, None).await;
                eprintln!(
                    "{kind:>12} {:>6} subs | {label:<28} => disconnected() took {:?}",
                    patterns.len(),
                    start.elapsed()
                );
            }
        }
    }
    drop(keep);
}
