mod publisher;

use crate::latency::publisher::LatencyTestPublisher;
use miette::{Context, IntoDiagnostic};
use std::time::{Duration, Instant};
use tosub::{CancelOnShutdown, Subsystem};
use tracing::debug;
use worterbuch_client::config::Config;

#[derive(Debug, Clone)]
pub struct LatencyTestResult {
    pub total_key_value_pairs: usize,
    pub prepare_duration: Duration,
    pub run_duration: Duration,
}

pub struct LatencyTest {
    subsys: Subsystem<Option<LatencyTestResult>>,
    publishers: usize,
    key_length: usize,
    n_ary: usize,
    values_per_key: usize,
    client_config: Config,
}

impl LatencyTest {
    pub fn new(
        subsys: &Subsystem,
        publishers: usize,
        key_length: usize,
        n_ary: usize,
        values_per_key: usize,
        client_config: Config,
    ) -> Subsystem<Option<LatencyTestResult>> {
        subsys.spawn("latency-test", move |subsys| {
            Self {
                subsys,
                publishers,
                key_length,
                n_ary,
                values_per_key,
                client_config,
            }
            .run()
        })
    }

    async fn run(self) -> miette::Result<Option<LatencyTestResult>> {
        let total_key_value_pairs = total_messages(
            self.publishers,
            self.key_length,
            self.n_ary,
            self.values_per_key,
        );

        let mut publishers = Vec::new();

        for i in 0..self.publishers {
            let publisher = LatencyTestPublisher::new(
                &self.subsys,
                i,
                self.key_length,
                self.n_ary,
                self.values_per_key,
                self.client_config.clone(),
            );
            publishers.push(publisher);
        }

        let prepare_futures = publishers
            .iter_mut()
            .map(|p| p.prepare())
            .collect::<Vec<_>>();
        debug!("All prepare signals sent, waiting for publishers to complete preparation");
        let start = Instant::now();
        for future in prepare_futures {
            let Some(res) = future.or_cancel_on_shutdown(&self.subsys).await else {
                return Ok(None);
            };
            res.into_diagnostic()
                .wrap_err("error while waiting for publisher to complete test preparation")?;
        }
        let stop = Instant::now();
        let prepare_duration = stop - start;

        let run_futures = publishers.iter_mut().map(|p| p.run()).collect::<Vec<_>>();
        debug!("All run signals sent, waiting for publishers to complete test run");
        let start = Instant::now();
        for future in run_futures {
            let Some(res) = future.or_cancel_on_shutdown(&self.subsys).await else {
                return Ok(None);
            };
            res.into_diagnostic()
                .wrap_err("error while waiting for publisher to complete test run")?;
        }
        let stop = Instant::now();
        let run_duration = stop - start;

        let result = LatencyTestResult {
            total_key_value_pairs,
            prepare_duration,
            run_duration,
        };

        Ok(Some(result))
    }
}

fn total_messages(
    publishers: usize,
    key_length: usize,
    n_ary: usize,
    values_per_key: usize,
) -> usize {
    if key_length == 0 {
        0
    } else {
        let leaf_nodes_per_tree = n_ary.pow(key_length as u32 - 1);
        publishers * leaf_nodes_per_tree * values_per_key
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn total_messages_are_calculated_correctly() {
        // 1 publisher, key length 2, binary tree, 1 message per key
        // example keys:
        // - hello/world
        // - hello/there
        // -> 2 messages per publisher
        assert_eq!(total_messages(1, 2, 2, 1), 2);

        // 1 publisher, key length 2, tertiary tree, 1 message per key
        // example keys:
        // - hello/world
        // - hello/there
        // - hello/you
        // -> 3 messages per publisher
        assert_eq!(total_messages(1, 2, 3, 1), 3);

        // 1 publisher, key length 3, binary tree, 1 message per key
        // example keys:
        // - hello/world/foo
        // - hello/world/bar
        // - hello/there/foo
        // - hello/there/bar
        // -> 4 messages per publisher
        assert_eq!(total_messages(1, 3, 2, 1), 4);

        // 1 publisher, key length 3, tertiary tree, 1 message per key
        // example keys:
        // - hello/world/foo
        // - hello/world/bar
        // - hello/world/baz
        // - hello/there/foo
        // - hello/there/bar
        // - hello/there/baz
        // - hello/you/foo
        // - hello/you/bar
        // - hello/you/baz
        // -> 9 messages per publisher
        assert_eq!(total_messages(1, 3, 3, 1), 9);

        // 2 publishers, key length 3, binary tree, 3 message per key
        // example keys:
        // - hello/world/foo
        // - hello/world/bar
        // - hello/there/foo
        // - hello/there/bar
        // -> 3 * 4 messages per publisher = 24 messages total
        assert_eq!(total_messages(2, 3, 2, 3), 24);
    }
}
