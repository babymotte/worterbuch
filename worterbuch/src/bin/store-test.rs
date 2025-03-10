use std::{env, sync::mpsc, thread::spawn, time::Instant};
use worterbuch::store::Store;
use worterbuch_common::{benchmark::generate_dummy_data, error::WorterbuchError};

fn main() -> Result<(), WorterbuchError> {
    let mut args = env::args().skip(1);

    let n_ary = args.next().and_then(|n| n.parse().ok()).unwrap_or(3);
    let length = args.next().and_then(|n| n.parse().ok()).unwrap_or(3);
    let values = args.next().and_then(|n| n.parse().ok()).unwrap_or(3);

    let (tx, rx) = mpsc::sync_channel(100);

    let mut store = Store::default();

    eprintln!("Store set up.");

    spawn(move || {
        let data = generate_dummy_data(n_ary, length, values);
        for entry in data {
            tx.send(entry).expect("failed to send dummy data");
        }
    });

    let mut counter = 0;

    let start = Instant::now();
    let mut lap = Instant::now();
    while let Ok((path, value)) = rx.recv() {
        store.insert_plain(&path, value)?;
        counter += 1;
        if counter % 100_000 == 0 {
            let elapsed = lap.elapsed();
            lap = Instant::now();
            eprintln!("{counter}: {} ms", elapsed.as_millis())
        }
    }
    let elapsed = start.elapsed();

    eprintln!(
        "Inserted {counter} values in {} s ({} msg/s)",
        elapsed.as_secs_f32(),
        counter as f32 / elapsed.as_secs_f32()
    );

    Ok(())
}
