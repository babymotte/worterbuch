/*
 *  Memory management tools
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

use lazy_static::lazy_static;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{spawn, time::sleep};
use tracing::{debug, error};

lazy_static! {
    static ref TRIM_TIMER: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
}

/// Schedule a debounced `trim_now()` call 1 second in the future.
/// If called again before the 1 second passes, the timer resets.
pub fn schedule_trim() {
    debug!("Scheduling trim …");
    let timer_arc = TRIM_TIMER.clone();

    let Ok(mut guard) = timer_arc.lock() else {
        error!("trim schedule mutex guard is poisoned");
        return;
    };

    // Cancel existing timer if any
    if let Some(handle) = guard.take() {
        handle.abort();
    }

    // Spawn a new delayed task
    let handle = spawn(async move {
        sleep(Duration::from_secs(1)).await;

        debug!("Trim triggered.");
        if trim_now() {
            debug!("Memory released.");
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
