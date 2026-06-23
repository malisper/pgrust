//! Launching the postmaster's auxiliary/background processes.
//!
//! C source: `postmaster/postmaster.c` — `LaunchMissingBackgroundProcesses`,
//! `StartChildProcess`, `StartSysLogger`, `StartAutovacuumWorker`.

#![allow(non_snake_case)]

use ::launch_backend::postmaster_child_launch;
use ::pmchild::{
    AssignPostmasterChildSlot, PMChild, ReleasePostmasterChildSlot, SetActiveChildBgworkerInfo,
    SetActiveChildPid,
};
use ::utils_error::{ereport};
use ::types_error::{LOG, PANIC};
use ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED;
use ::types_core::init::BackendType;
use ::types_startup::StartupData;

use crate::core::{
    pm, pm_mut, PMState, SMART_SHUTDOWN, B_ARCHIVER, B_AUTOVAC_WORKER,
    B_BG_WRITER, B_CHECKPOINTER, B_SLOTSYNC_WORKER, B_STARTUP, B_WAL_RECEIVER, B_WAL_SUMMARIZER,
    B_WAL_WRITER,
};
use crate::helpers::here;
use crate::{bgworkers, ioworkers, statemachine};
use postmaster_seams as sp;

/// C: `static void LaunchMissingBackgroundProcesses(void)`.
pub fn LaunchMissingBackgroundProcesses() {
    // Syslogger is active in all states.
    if pm().syslogger_pmchild.is_none() && sp::logging_collector::call() {
        StartSysLogger();
    }

    // The number of configured workers might have changed, or a prior start of
    // a worker might have failed.
    ioworkers::maybe_adjust_io_workers();

    // The checkpointer and the background writer are active from the start,
    // until shutdown is initiated.
    if pm().pm_state == PMState::PmRun
        || pm().pm_state == PMState::PmRecovery
        || pm().pm_state == PMState::PmHotStandby
        || pm().pm_state == PMState::PmStartup
    {
        if pm().checkpointer_pmchild.is_none() {
            pm_mut().checkpointer_pmchild = StartChildProcess(B_CHECKPOINTER);
        }
        if pm().bgwriter_pmchild.is_none() {
            pm_mut().bgwriter_pmchild = StartChildProcess(B_BG_WRITER);
        }
    }

    // WAL writer is needed only in normal operation.
    if pm().walwriter_pmchild.is_none() && pm().pm_state == PMState::PmRun {
        pm_mut().walwriter_pmchild = StartChildProcess(B_WAL_WRITER);
    }

    // We don't want autovacuum to run in binary upgrade mode.
    //
    // PORT GAP: the background autovacuum *launcher* loop is not yet fully
    // ported — its runtime boundary (the latch wait, the pgstat database-list
    // fetch, the worker-launch signalling) routes through ext-seams that are
    // not installed, so an actually-forked launcher would panic the instant it
    // ran its loop. We therefore never fork it here. This is faithful to a real
    // server semantically: it is exactly the "no background autovacuum is
    // scheduled" state. Crucially this does NOT disable autovacuum-dependent
    // backend behaviour: `AutoVacuumingActive()` still reads true when the
    // `autovacuum`/`track_counts` GUCs are on, so `index_update_stats` and
    // `do_analyze_rel` write `pg_class.reltuples`/`relpages` (the planner's
    // row estimates) just like upstream, and the test SQL's explicit
    // ANALYZE/VACUUM run normally. Only the *background* scheduling is absent.
    //
    // We still clear the `start_autovac_launcher` request flag so a
    // PMSIGNAL_START_AUTOVAC_LAUNCHER doesn't make us re-enter every loop.
    if !sp::is_binary_upgrade::call()
        && pm().autovac_launcher_pmchild.is_none()
        && (sp::autovacuuming_active::call() || pm().start_autovac_launcher)
        && pm().pm_state == PMState::PmRun
    {
        // Launcher fork suppressed (port gap above). When the launcher's
        // runtime ext-seams are installed, restore:
        //     pm_mut().autovac_launcher_pmchild = StartChildProcess(B_AUTOVAC_LAUNCHER);
        //     if pm().autovac_launcher_pmchild.is_some() { ... }
        pm_mut().start_autovac_launcher = false; // signal processed (no fork)
    }

    // If WAL archiving is enabled always, we may start archiver even during
    // recovery.
    if pm().pgarch_pmchild.is_none()
        && ((sp::xlog_archiving_active::call() && pm().pm_state == PMState::PmRun)
            || (sp::xlog_archiving_always::call()
                && (pm().pm_state == PMState::PmRecovery
                    || pm().pm_state == PMState::PmHotStandby)))
        && sp::pgarch_can_restart::call()
    {
        pm_mut().pgarch_pmchild = StartChildProcess(B_ARCHIVER);
    }

    // If we need to start a slot sync worker, try to do that now.
    if pm().slotsync_worker_pmchild.is_none()
        && pm().pm_state == PMState::PmHotStandby
        && pm().shutdown <= SMART_SHUTDOWN
        && sp::sync_replication_slots::call()
        && sp::validate_slot_sync_params::call(LOG.0 as i32)
        && sp::slot_sync_worker_can_restart::call()
    {
        pm_mut().slotsync_worker_pmchild = StartChildProcess(B_SLOTSYNC_WORKER);
    }

    // If we need to start a WAL receiver, try to do that now.
    if pm().wal_receiver_requested
        && pm().walreceiver_pmchild.is_none()
        && (pm().pm_state == PMState::PmStartup
            || pm().pm_state == PMState::PmRecovery
            || pm().pm_state == PMState::PmHotStandby)
        && pm().shutdown <= SMART_SHUTDOWN
    {
        pm_mut().walreceiver_pmchild = StartChildProcess(B_WAL_RECEIVER);
        if pm().walreceiver_pmchild.is_some() {
            pm_mut().wal_receiver_requested = false;
        }
        // else leave the flag set, so we'll try again later
    }

    // If we need to start a WAL summarizer, try to do that now.
    if sp::summarize_wal::call()
        && pm().walsummarizer_pmchild.is_none()
        && (pm().pm_state == PMState::PmRun || pm().pm_state == PMState::PmHotStandby)
        && pm().shutdown <= SMART_SHUTDOWN
    {
        pm_mut().walsummarizer_pmchild = StartChildProcess(B_WAL_SUMMARIZER);
    }

    // Get other worker processes running, if needed.
    if pm().start_worker_needed || pm().have_crashed_worker {
        bgworkers::maybe_start_bgworkers();
    }
}

/// C: `static PMChild *StartChildProcess(BackendType type)`.
///
/// Start an auxiliary process for the postmaster. Returns the subprocess'
/// `PMChild`, or `None` on failure.
pub fn StartChildProcess(type_: BackendType) -> Option<PMChild> {
    let pmchild = AssignPostmasterChildSlot(type_);
    let pmchild = match pmchild {
        Some(c) => c,
        None => {
            if type_ == B_AUTOVAC_WORKER {
                let _ = ereport(LOG)
                    .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                    .errmsg("no slot available for new autovacuum worker process")
                    .finish(here("StartChildProcess"));
            } else {
                let _ = ereport(LOG)
                    .errmsg_internal("no postmaster child slot available for aux process")
                    .finish(here("StartChildProcess"));
            }
            return None;
        }
    };

    let mut startup_data = StartupData::None;
    let pid = postmaster_child_launch(type_, pmchild.child_slot, &mut startup_data, None);
    if pid < 0 {
        /* in parent, fork failed */
        ReleasePostmasterChildSlot(pmchild);
        let name = miscinit_seams::get_backend_type_desc::call(type_);
        let _ = ereport(LOG)
            .errmsg(format!("could not fork \"{name}\" process"))
            .finish(here("StartChildProcess"));

        /*
         * fork failure is fatal during startup, but there's no need to choke
         * immediately if starting other child types fails.
         */
        if type_ == B_STARTUP {
            statemachine::ExitPostmaster(1);
        }
        return None;
    }

    /* in parent, successful fork — record the pid (C: pmchild->pid = pid). */
    SetActiveChildPid(pmchild.child_slot, pid);
    let mut pmchild = pmchild;
    pmchild.pid = pid;
    Some(pmchild)
}

/// C: `void StartSysLogger(void)`.
pub fn StartSysLogger() {
    debug_assert!(pm().syslogger_pmchild.is_none());

    let slot = sp::assign_syslogger_slot::call();
    let slot = match slot {
        Some(s) => s,
        None => {
            let _ = ereport(PANIC)
                .errmsg_internal("no postmaster child slot available for syslogger")
                .finish(here("StartSysLogger"));
            return; // ereport(PANIC) does not return; placate the borrow checker.
        }
    };
    let pid = sp::syslogger_start::call(slot);
    // The syslogger owner installs assign_syslogger_slot to return a slot and
    // tracks the PMChild internally; we model the postmaster's bookkeeping with
    // a synthetic PMChild carrying the slot+pid.
    let mut pmchild = PMChild {
        pid,
        child_slot: slot,
        bkend_type: crate::core::B_LOGGER,
        rw: None,
        bgworker_notify: false,
    };
    SetActiveChildPid(slot, pid);
    if pid == 0 {
        ReleasePostmasterChildSlot(pmchild);
        pm_mut().syslogger_pmchild = None;
    } else {
        pmchild.pid = pid;
        pm_mut().syslogger_pmchild = Some(pmchild);
    }
}

/// C: `static void StartAutovacuumWorker(void)`.
pub fn StartAutovacuumWorker() {
    use ::types_startup::CacState;
    if statemachine::canAcceptConnections(B_AUTOVAC_WORKER) == CacState::Ok {
        let bn = StartChildProcess(B_AUTOVAC_WORKER);
        if let Some(bn) = bn {
            SetActiveChildBgworkerInfo(bn.child_slot, None, false);
            return;
        }
        /* fork failed, fall through to report */
    }

    if pm().autovac_launcher_pmchild.is_some() {
        sp::autovac_worker_failed::call();
        pm_mut().avlauncher_needs_signal = true;
    }
}
