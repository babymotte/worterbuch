use lazy_static::lazy_static;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{spawn, time::sleep};
use tracing::info;

lazy_static! {
    static ref TRIM_TIMER: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
}

/// Schedule a debounced `trim_now()` call 1 second in the future.
/// If called again before the 1 second passes, the timer resets.
pub fn schedule_trim() {
    info!("Scheduling trim …");
    let timer_arc = TRIM_TIMER.clone();

    let mut guard = timer_arc.lock().unwrap();

    // Cancel existing timer if any
    if let Some(handle) = guard.take() {
        handle.abort();
    }

    // Spawn a new delayed task
    let handle = spawn(async move {
        sleep(Duration::from_secs(1)).await;

        info!("Trim triggered.");
        if trim_now() {
            info!("Memory released.");
        }
    });

    *guard = Some(handle);
}

#[cfg(target_os = "linux")]
fn trim_now() -> bool {
    unsafe { libc::malloc_trim(0) != 0 }
}

#[cfg(not(target_os = "linux"))]
fn trim_now() -> bool {
    false
}
