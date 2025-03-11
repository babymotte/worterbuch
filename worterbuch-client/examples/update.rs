use miette::Result;
use std::{collections::BTreeSet, io, thread};
use tokio::runtime;
use tracing_subscriber::EnvFilter;
use worterbuch_client::connect_with_default_config;
use worterbuch_common::topic;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut threads = vec![];

    for i in 0..20 {
        let t = thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creating tokio runtime failed");
            rt.block_on(async { add_number(i).await.expect("adding number failed") });
        });
        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }

    Ok(())
}

async fn add_number(i: usize) -> Result<()> {
    let (wb, _, _) = connect_with_default_config().await?;

    wb.update(topic!("hello", "world"), BTreeSet::new, |set| {
        set.insert(i);
    })
    .await?;

    Ok(())
}
