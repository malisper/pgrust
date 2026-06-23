//! Background-worker scheduling.
//!
//! C source: `postmaster/postmaster.c` — `StartBackgroundWorker`,
//! `bgworker_should_start_now`, `maybe_start_bgworkers`,
//! `PostmasterMarkPIDForWorkerNotify`.
//!
//! The `RegisteredBgWorker` registry (`BackgroundWorkerList`, owned by
//! `bgworker.c`) is reached through the `background_worker_list` snapshot seam
//! and the `rw_*` accessor seams (the bgworker crate keeps the list private and
//! exposes only index-keyed mutators today; see the seams crate). A default
//! cluster registers no bgworkers, so the snapshot is empty on the SELECT-1
//! happy path.

#![allow(non_snake_case)]

use launch_backend::postmaster_child_launch;
use pmchild::{
    AssignPostmasterChildSlot, ActiveChildListSnapshot, MarkActiveChildBgworkerNotify,
    ReleasePostmasterChildSlot, SetActiveChildBgworkerInfo, SetActiveChildBkendType,
    SetActiveChildPid,
};
use utils_error::{ereport};
use types_error::{DEBUG1, LOG};
use types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED;
use types_startup::StartupData;

use crate::core::{pm, pm_mut, PMState, B_BG_WORKER, SIGUSR1};
use crate::helpers::{here, kill};
use crate::serverloop::BGW_NEVER_RESTART;
use postmaster_seams as sp;

/// C: `BgWorkerStart_PostmasterStart` (0).
pub const BGWORKER_START_POSTMASTER_START: i32 = 0;
/// C: `BgWorkerStart_ConsistentState` (1).
pub const BGWORKER_START_CONSISTENT_STATE: i32 = 1;
/// C: `BgWorkerStart_RecoveryFinished` (2).
pub const BGWORKER_START_RECOVERY_FINISHED: i32 = 2;

/// C: `#define MAX_BGWORKERS_TO_LAUNCH 100`.
const MAX_BGWORKERS_TO_LAUNCH: i32 = 100;

/// C: `static bool StartBackgroundWorker(RegisteredBgWorker *rw)`.
///
/// `rw_index` is the registration index (the position in `BackgroundWorkerList`
/// that the `PMChild.rw` field stores).
pub fn StartBackgroundWorker(rw_index: u32) -> bool {
    debug_assert!(sp::rw_pid::call(rw_index) == 0);

    let bn = AssignPostmasterChildSlot(B_BG_WORKER);
    let bn = match bn {
        Some(c) => c,
        None => {
            let _ = ereport(LOG)
                .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg("no slot available for new background worker process")
                .finish(here("StartBackgroundWorker"));
            sp::rw_set_crashed_at::call(
                rw_index,
                timestamp_seams::get_current_timestamp::call(),
            );
            return false;
        }
    };
    SetActiveChildBkendType(bn.child_slot, B_BG_WORKER);
    SetActiveChildBgworkerInfo(bn.child_slot, Some(rw_index), false);

    let _ = ereport(DEBUG1)
        .errmsg_internal(format!(
            "starting background worker process \"{}\"",
            sp::rw_bgw_name::call(rw_index)
        ))
        .finish(here("StartBackgroundWorker"));

    // The worker's BackgroundWorker payload is owned by bgworker.c; the launch
    // seam-and-panics through StartupData::BgWorker, which the bgworker carrier
    // supplies when widened. A default cluster never reaches here.
    let mut startup_data = StartupData::BgWorker(sp_bgworker_payload(rw_index));
    let worker_pid =
        postmaster_child_launch(B_BG_WORKER, bn.child_slot, &mut startup_data, None);
    if worker_pid == -1 {
        let _ = ereport(LOG)
            .errmsg("could not fork background worker process")
            .finish(here("StartBackgroundWorker"));
        ReleasePostmasterChildSlot(bn);
        sp::rw_set_crashed_at::call(
            rw_index,
            timestamp_seams::get_current_timestamp::call(),
        );
        return false;
    }

    /* in postmaster, fork successful ... */
    sp::rw_set_pid::call(rw_index, worker_pid);
    SetActiveChildPid(bn.child_slot, worker_pid); /* bn->pid = rw->rw_pid */
    sp::report_background_worker_pid::call(rw_index);
    true
}

/// The `RegisteredBgWorker.rw_worker` payload for `rw_index`. The bgworker owner
/// projects the full `BackgroundWorker` struct through the `rw_worker` seam,
/// alongside the other `rw_*` accessors; `StartBackgroundWorker` reads it to seed
/// the forked worker's `StartupData::BgWorker`.
fn sp_bgworker_payload(rw_index: u32) -> types_bgworker::BackgroundWorker {
    sp::rw_worker::call(rw_index)
}

/// C: `static bool bgworker_should_start_now(BgWorkerStartTime start_time)`.
pub fn bgworker_should_start_now(start_time: i32) -> bool {
    match pm().pm_state {
        PMState::PmNoChildren
        | PMState::PmWaitCheckpointer
        | PMState::PmWaitDeadEnd
        | PMState::PmWaitXlogArchival
        | PMState::PmWaitXlogShutdown
        | PMState::PmWaitIoWorkers
        | PMState::PmWaitBackends
        | PMState::PmStopBackends => {}

        PMState::PmRun => {
            if start_time == BGWORKER_START_RECOVERY_FINISHED {
                return true;
            }
            if start_time == BGWORKER_START_CONSISTENT_STATE {
                return true;
            }
            if start_time == BGWORKER_START_POSTMASTER_START {
                return true;
            }
        }

        PMState::PmHotStandby => {
            if start_time == BGWORKER_START_CONSISTENT_STATE {
                return true;
            }
            if start_time == BGWORKER_START_POSTMASTER_START {
                return true;
            }
        }

        PMState::PmRecovery | PMState::PmStartup | PMState::PmInit => {
            if start_time == BGWORKER_START_POSTMASTER_START {
                return true;
            }
        }
    }

    false
}

/// C: `static void maybe_start_bgworkers(void)`.
pub fn maybe_start_bgworkers() {
    let mut num_launched = 0;
    let mut now: types_core::primitive::TimestampTz = 0;

    if pm().fatal_error {
        pm_mut().start_worker_needed = false;
        pm_mut().have_crashed_worker = false;
        return;
    }

    /* Don't need to be called again unless we find a reason below. */
    pm_mut().start_worker_needed = false;
    pm_mut().have_crashed_worker = false;

    for rw in sp::background_worker_list::call() {
        /* ignore if already running */
        if sp::rw_pid::call(rw) != 0 {
            continue;
        }

        /* if marked for death, clean up and remove from list */
        if sp::rw_terminate::call(rw) {
            sp::forget_background_worker::call(rw);
            continue;
        }

        if sp::rw_crashed_at::call(rw) != 0 {
            if sp::rw_bgw_restart_time::call(rw) == BGW_NEVER_RESTART {
                let notify_pid = sp::rw_bgw_notify_pid::call(rw);

                sp::forget_background_worker::call(rw);

                if notify_pid != 0 {
                    kill(notify_pid, SIGUSR1);
                }

                continue;
            }

            /* read system time only when needed */
            if now == 0 {
                now = timestamp_seams::get_current_timestamp::call();
            }

            if !timestamp_difference_exceeds(
                sp::rw_crashed_at::call(rw),
                now,
                sp::rw_bgw_restart_time::call(rw) * 1000,
            ) {
                pm_mut().have_crashed_worker = true;
                continue;
            }
        }

        if bgworker_should_start_now(sp::rw_bgw_start_time::call(rw)) {
            /* reset crash time before trying to start worker */
            sp::rw_set_crashed_at::call(rw, 0);

            if !StartBackgroundWorker(rw) {
                pm_mut().start_worker_needed = true;
                return;
            }

            num_launched += 1;
            if num_launched >= MAX_BGWORKERS_TO_LAUNCH {
                pm_mut().start_worker_needed = true;
                return;
            }
        }
    }
}

/// C: `TimestampDifferenceExceeds(start, stop, msec)` — whether `stop - start`
/// exceeds `msec` milliseconds. `TimestampTz` is microseconds.
#[inline]
fn timestamp_difference_exceeds(start: i64, stop: i64, msec: i32) -> bool {
    let diff = stop - start;
    diff >= (msec as i64) * 1000
}

/// C: `bool PostmasterMarkPIDForWorkerNotify(int pid)`.
///
/// `ActiveChildList` and the `PMChild` slab are owned by pmchild here, so the
/// find-by-pid + in-place `bgworker_notify = true` runs through pmchild's
/// keyed primitive (same semantics, list owned privately).
pub fn PostmasterMarkPIDForWorkerNotify(pid: i32) -> bool {
    // Mirror the C dlist_foreach for fidelity, but commit through the keyed
    // mutator (the snapshot copies are not write-through).
    for bp in ActiveChildListSnapshot() {
        if bp.pid == pid {
            return MarkActiveChildBgworkerNotify(pid);
        }
    }
    false
}
