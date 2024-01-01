use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::time::Duration;
use uuid::Uuid;
use worterbuch::Worterbuch;
use worterbuch_common::{KeyValuePairs, PState, PStateEvent};

const DUMP: &str = include_str!("dump.json");

fn set_without_subscribres((mut wb, kvps): (Worterbuch, KeyValuePairs)) {
    let client_id = Uuid::new_v4();
    for kvp in kvps {
        wb.set(kvp.key, kvp.value, &client_id.to_string())
            .expect("set failed");
    }
}

fn worterbuch_benchmark(c: &mut Criterion) {
    let state: PState = serde_json::from_str(DUMP).expect("invalid json data");
    let kvps = match state.event {
        PStateEvent::KeyValuePairs(kvps) => kvps,
        PStateEvent::Deleted(_) => panic!("invalid json data"),
    };

    let mut benches = c.benchmark_group("worterbuch");
    benches
        .significance_level(0.01)
        .sample_size(1_000)
        .measurement_time(Duration::from_secs(30))
        .throughput(Throughput::Elements(kvps.len() as u64));

    benches.bench_function("set without subscribers", |b| {
        b.iter_batched(
            || (Worterbuch::default(), kvps.clone()),
            set_without_subscribres,
            BatchSize::SmallInput,
        )
    });
    benches.finish();
}

criterion_group!(wb_benches, worterbuch_benchmark);
criterion_main!(wb_benches);
