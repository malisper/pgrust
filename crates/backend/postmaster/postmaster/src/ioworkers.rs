//! IO-worker reaping and population adjustment.
//!
//! C source: `postmaster/postmaster.c` — `maybe_reap_io_worker`,
//! `maybe_adjust_io_workers`.

#![allow(non_snake_case)]

use ::pmchild::ReleasePostmasterChildSlot;
use ::utils_error::{ereport};
use ::types_error::{ERROR};
use ::types_error::ErrorLocation;

use crate::core::{pm, pm_mut, PMState, IMMEDIATE_SHUTDOWN, MAX_IO_WORKERS, SIGUSR2, B_IO_WORKER};
use crate::helpers::kill;
use crate::startchildren;
use postmaster_seams as sp;

/// C: `static bool maybe_reap_io_worker(int pid)`.
pub fn maybe_reap_io_worker(pid: i32) -> bool {
    let mut i = 0;
    while i < MAX_IO_WORKERS {
        if let Some(child) = pm().io_worker_children[i] {
            if child.pid == pid {
                ReleasePostmasterChildSlot(child);
                pm_mut().io_worker_count -= 1;
                pm_mut().io_worker_children[i] = None;
                return true;
            }
        }
        i += 1;
    }
    false
}

/// C: `static void maybe_adjust_io_workers(void)`.
pub fn maybe_adjust_io_workers() {
    if !sp::pgaio_workers_enabled::call() {
        return;
    }

    // If we're in the final shutting-down state, just wait for processes to exit.
    if pm().pm_state >= PMState::PmWaitIoWorkers {
        return;
    }

    // Don't start new workers during an immediate shutdown either.
    if pm().shutdown >= IMMEDIATE_SHUTDOWN {
        return;
    }

    // Don't start new workers if we're in the shutdown phase of a crash restart.
    if pm().fatal_error && pm().pm_state >= PMState::PmStopBackends {
        return;
    }

    debug_assert!(pm().pm_state < PMState::PmWaitIoWorkers);

    let io_workers = sp::io_workers::call();

    // Not enough running?
    while pm().io_worker_count < io_workers {
        // find unused entry in io_worker_children array
        let mut i = 0;
        while i < MAX_IO_WORKERS {
            if pm().io_worker_children[i].is_none() {
                break;
            }
            i += 1;
        }
        if i == MAX_IO_WORKERS {
            let _ = ereport(ERROR)
                .errmsg_internal("could not find a free IO worker slot")
                .finish(ErrorLocation::new("postmaster.c", 0, "maybe_adjust_io_workers"));
        }

        // Try to launch one.
        let child = startchildren::StartChildProcess(B_IO_WORKER);
        if let Some(c) = child {
            pm_mut().io_worker_children[i] = Some(c);
            pm_mut().io_worker_count += 1;
        } else {
            break; // try again next time
        }
    }

    // Too many running?
    if pm().io_worker_count > io_workers {
        // ask the IO worker in the highest slot to exit
        let mut i = MAX_IO_WORKERS as isize - 1;
        while i >= 0 {
            let idx = i as usize;
            if let Some(child) = pm().io_worker_children[idx] {
                kill(child.pid, SIGUSR2);
                break;
            }
            i -= 1;
        }
    }
}
