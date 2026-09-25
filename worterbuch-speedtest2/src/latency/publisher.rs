use std::collections::HashSet;

use miette::IntoDiagnostic;
use random_word::Lang;
use serde_json::json;
use tokio::sync::oneshot;
use tosub::{CancelOnShutdown, Subsystem};
#[cfg(feature = "trace")]
use tracing::trace;
use tracing::{debug, info, warn};
use worterbuch_client::config::Config;

use crate::{client::create_tcp_client, latency::LatencyTestResult};

pub struct PublisherApi {
    id: usize,
    prepare_tx: Option<oneshot::Sender<oneshot::Sender<()>>>,
    run_tx: Option<oneshot::Sender<oneshot::Sender<()>>>,
}

impl PublisherApi {
    pub fn prepare(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.prepare_tx
            .take()
            .expect("prepare can only be called once")
            .send(tx)
            .ok();
        debug!("Sent prepare signal to publisher {}", self.id);
        rx
    }

    pub fn run(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.run_tx
            .take()
            .expect("run can only be called once")
            .send(tx)
            .ok();
        debug!("Sent run signal to publisher {}", self.id);
        rx
    }
}

pub struct LatencyTestPublisher {
    subsys: Subsystem,
    id: usize,
    key_length: usize,
    n_ary: usize,
    values_per_key: usize,
    prepare_rx: Option<oneshot::Receiver<oneshot::Sender<()>>>,
    run_rx: Option<oneshot::Receiver<oneshot::Sender<()>>>,
    client_config: Config,
}

impl LatencyTestPublisher {
    pub fn new(
        subsys: &Subsystem<Option<LatencyTestResult>>,
        id: usize,
        key_length: usize,
        n_ary: usize,
        values_per_key: usize,
        client_config: Config,
    ) -> PublisherApi {
        let (prepare_tx, prepare_rx) = oneshot::channel();
        let (run_tx, run_rx) = oneshot::channel();

        subsys.spawn(format!("publisher-{id}"), move |subsys| {
            Self {
                subsys: subsys.clone(),
                id,
                key_length,
                n_ary,
                values_per_key,
                prepare_rx: Some(prepare_rx),
                run_rx: Some(run_rx),
                client_config,
            }
            .run()
        });

        PublisherApi {
            id,
            prepare_tx: Some(prepare_tx),
            run_tx: Some(run_tx),
        }
    }

    async fn run(mut self) -> miette::Result<()> {
        debug!("Publisher {} waiting for prepare signal …", self.id);

        // wait for prepare signal to be sent
        let Some(prepared_tx) = self
            .prepare_rx
            .take()
            .expect("prepare signal already consumed")
            .or_cancel_on_shutdown(&self.subsys)
            .await
        else {
            return Ok(());
        };
        let prepared_tx = prepared_tx.into_diagnostic()?;

        debug!(
            "Publisher {} received prepare signal. Preparing test …",
            self.id
        );

        // prepare test
        #[cfg(feature = "trace")]
        trace!("Publisher {} creating worterbuch client …", self.id);

        let Some(client) = create_tcp_client(&self.subsys, self.id, &self.client_config)
            .or_cancel_on_shutdown(&self.subsys)
            .await
        else {
            return Ok(());
        };
        let mut client = client?;

        #[cfg(feature = "trace")]
        trace!(
            "Publisher {} created worterbuch client. Generating key value pairs …",
            self.id
        );

        let mut keys = HashSet::new();
        keys.insert(format!("speed-test/latency/{}", self.id));
        if self.key_length > 0 {
            for _l in 0..(self.key_length - 1) {
                let mut next_gen = HashSet::new();
                for key in &keys {
                    for _ in 0..self.n_ary {
                        next_key(key, &mut next_gen);
                        if self.subsys.is_shut_down() {
                            return Ok(());
                        }
                    }
                }
                keys = next_gen;
                #[cfg(feature = "trace")]
                trace!(
                    "Publisher {} generated keys for level {}: {:?}",
                    self.id, _l, keys
                );
            }
        }
        #[cfg(feature = "trace")]
        trace!(
            "Publisher {} generated {} keys: {:?}",
            self.id,
            keys.len(),
            keys
        );

        info!("Publisher {} generated {} keys", self.id, keys.len());

        for _ in 0..self.values_per_key {
            for key in &keys {
                if self.subsys.is_shut_down() {
                    return Ok(());
                }
                let value = json!(random_word::get(Lang::En));
                client.prepare_set(key.to_owned(), value).await;
            }
        }

        #[cfg(feature = "trace")]
        trace!(
            "Publisher {} generated key value pairs. Sending prepare done signal …",
            self.id
        );

        // send prepare done signal
        prepared_tx.send(()).ok();

        #[cfg(feature = "trace")]
        trace!(
            "Publisher {} sent prepare done signal. Waiting for run signal …",
            self.id
        );

        // wait for run signal to be sent
        let Some(run_tx) = self
            .run_rx
            .take()
            .expect("run signal already consumed")
            .or_cancel_on_shutdown(&self.subsys)
            .await
        else {
            return Ok(());
        };
        let run_tx = run_tx.into_diagnostic()?;

        debug!(
            "Publisher {} received run signal. Starting test run …",
            self.id
        );

        #[cfg(feature = "trace")]
        trace!("Publisher {} sending out set commands …", self.id);

        if client
            .send_prepared_sets(&self.subsys)
            .or_cancel_on_shutdown(&self.subsys)
            .await
            .is_none()
        {
            return Ok(());
        }

        #[cfg(feature = "trace")]
        trace!(
            "Publisher {} sent out set commands. Waiting for acks …",
            self.id
        );

        if client
            .await_acks(&self.subsys)
            .or_cancel_on_shutdown(&self.subsys)
            .await
            .is_none()
        {
            return Ok(());
        }

        debug!(
            "Publisher {} received all acks. Sending run done signal …",
            self.id
        );

        // send run done signal
        run_tx.send(()).ok();

        #[cfg(feature = "trace")]
        trace!("Publisher {} sent run done signal. Test complete.", self.id);

        Ok(())
    }
}

fn next_key(parent: &String, keys: &mut HashSet<String>) {
    loop {
        let segment = random_word::get(Lang::En);
        let key = format!("{}/{}", parent, segment);
        if keys.insert(key) {
            break;
        } else {
            warn!("Duplicate key, generating a new one …");
        }
    }
}
