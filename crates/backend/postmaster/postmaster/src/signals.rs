//! Postmaster signal handlers and their deferred (main-loop) processors.
//!
//! The handlers only set pending-signal flags (in [`crate::core::PostmasterState`])
//! and set the postmaster latch; the matching `process_pm_*` functions run the
//! real work from the main loop.
//!
//! C source: `postmaster/postmaster.c` — `handle_pm_pmsignal_signal`,
//! `handle_pm_child_exit_signal`, `handle_pm_reload_request_signal`,
//! `handle_pm_shutdown_request_signal`, `process_pm_pmsignal`,
//! `process_pm_reload_request`, `process_pm_shutdown_request`, `dummy_handler`.

#![allow(non_snake_case)]

use ::pmsignal::{
    CheckPostmasterSignal, PMSignalReason, QuitSignalReason, SetQuitSignalReason,
};
use ::types_error::{DEBUG2, LOG};

use crate::core::{
    btmask, pm, pm_mut, PMState, FAST_SHUTDOWN, IMMEDIATE_SHUTDOWN, NO_SHUTDOWN, SIGINT, SIGQUIT,
    SIGTERM, SIGUSR1, SIGUSR2, SMART_SHUTDOWN, B_ARCHIVER, B_DEAD_END_BACKEND, B_WAL_SENDER,
};
use crate::helpers::report;
use crate::latchutil::set_latch;
use crate::serverloop::{
    LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY, PM_STATUS_STANDBY, PM_STATUS_STOPPING,
};
use crate::{childmgmt, startchildren, statemachine};
use postmaster_seams as sp;

// ===========================================================================
// Async signal handlers — set a flag and wake the latch.
// ===========================================================================

/// C: `static void handle_pm_pmsignal_signal(SIGNAL_ARGS)`.
pub fn handle_pm_pmsignal_signal(_postgres_signal_arg: i32) {
    pm_mut().pending_pm_pmsignal = true;
    set_latch();
}

/// C: `static void handle_pm_reload_request_signal(SIGNAL_ARGS)`.
pub fn handle_pm_reload_request_signal(_postgres_signal_arg: i32) {
    pm_mut().pending_pm_reload_request = true;
    set_latch();
}

/// C: `static void handle_pm_shutdown_request_signal(SIGNAL_ARGS)`.
pub fn handle_pm_shutdown_request_signal(postgres_signal_arg: i32) {
    match postgres_signal_arg {
        x if x == SIGTERM => {
            /* smart is implied if the other two flags aren't set */
            pm_mut().pending_pm_shutdown_request = true;
        }
        x if x == SIGINT => {
            pm_mut().pending_pm_fast_shutdown_request = true;
            pm_mut().pending_pm_shutdown_request = true;
        }
        x if x == SIGQUIT => {
            pm_mut().pending_pm_immediate_shutdown_request = true;
            pm_mut().pending_pm_shutdown_request = true;
        }
        _ => {}
    }
    set_latch();
}

/// C: `static void handle_pm_child_exit_signal(SIGNAL_ARGS)`.
pub fn handle_pm_child_exit_signal(_postgres_signal_arg: i32) {
    pm_mut().pending_pm_child_exit = true;
    set_latch();
}

// ===========================================================================
// Deferred processors — run from the main server loop.
// ===========================================================================

/// C: `static void process_pm_reload_request(void)`.
pub fn process_pm_reload_request() {
    pm_mut().pending_pm_reload_request = false;

    report(DEBUG2, "process_pm_reload_request", "postmaster received reload request signal");

    if pm().shutdown <= SMART_SHUTDOWN {
        report(LOG, "process_pm_reload_request", "received SIGHUP, reloading configuration files");
        sp::process_config_file_sighup::call();
        childmgmt::SignalChildren(crate::core::SIGHUP, btmask_all_except!(B_DEAD_END_BACKEND));

        /* Reload authentication config files too */
        if !sp::load_hba::call() {
            report(
                LOG,
                "process_pm_reload_request",
                format!("{} was not reloaded", sp::hba_file_name::call()),
            );
        }

        if !sp::load_ident::call() {
            report(
                LOG,
                "process_pm_reload_request",
                format!("{} was not reloaded", sp::ident_file_name::call()),
            );
        }

        // #ifdef USE_SSL — SSL reload is owned by the libpq SSL provider; the
        // postmaster's decision (reload vs destroy) is preserved when SSL is
        // configured. A non-SSL build performs no action here.
    }
}

/// C: `static void process_pm_shutdown_request(void)`.
pub fn process_pm_shutdown_request() {
    let mode: i32;

    report(DEBUG2, "process_pm_shutdown_request", "postmaster received shutdown request signal");

    pm_mut().pending_pm_shutdown_request = false;

    if pm().pending_pm_immediate_shutdown_request {
        pm_mut().pending_pm_immediate_shutdown_request = false;
        pm_mut().pending_pm_fast_shutdown_request = false;
        mode = IMMEDIATE_SHUTDOWN;
    } else if pm().pending_pm_fast_shutdown_request {
        pm_mut().pending_pm_fast_shutdown_request = false;
        mode = FAST_SHUTDOWN;
    } else {
        mode = SMART_SHUTDOWN;
    }

    match mode {
        x if x == SMART_SHUTDOWN => {
            if pm().shutdown >= SMART_SHUTDOWN {
                return;
            }
            pm_mut().shutdown = SMART_SHUTDOWN;
            report(LOG, "process_pm_shutdown_request", "received smart shutdown request");

            let _ = miscinit_seams::add_to_data_dir_lock_file::call(
                LOCK_FILE_LINE_PM_STATUS,
                PM_STATUS_STOPPING,
            );

            if pm().pm_state == PMState::PmRun || pm().pm_state == PMState::PmHotStandby {
                pm_mut().conns_allowed = false;
            } else if pm().pm_state == PMState::PmStartup || pm().pm_state == PMState::PmRecovery {
                statemachine::UpdatePMState(PMState::PmStopBackends);
            }

            statemachine::PostmasterStateMachine();
        }

        x if x == FAST_SHUTDOWN => {
            if pm().shutdown >= FAST_SHUTDOWN {
                return;
            }
            pm_mut().shutdown = FAST_SHUTDOWN;
            report(LOG, "process_pm_shutdown_request", "received fast shutdown request");

            let _ = miscinit_seams::add_to_data_dir_lock_file::call(
                LOCK_FILE_LINE_PM_STATUS,
                PM_STATUS_STOPPING,
            );

            if pm().pm_state == PMState::PmStartup || pm().pm_state == PMState::PmRecovery {
                statemachine::UpdatePMState(PMState::PmStopBackends);
            } else if pm().pm_state == PMState::PmRun || pm().pm_state == PMState::PmHotStandby {
                report(LOG, "process_pm_shutdown_request", "aborting any active transactions");
                statemachine::UpdatePMState(PMState::PmStopBackends);
            }

            statemachine::PostmasterStateMachine();
        }

        x if x == IMMEDIATE_SHUTDOWN => {
            if pm().shutdown >= IMMEDIATE_SHUTDOWN {
                return;
            }
            pm_mut().shutdown = IMMEDIATE_SHUTDOWN;
            report(LOG, "process_pm_shutdown_request", "received immediate shutdown request");

            let _ = miscinit_seams::add_to_data_dir_lock_file::call(
                LOCK_FILE_LINE_PM_STATUS,
                PM_STATUS_STOPPING,
            );

            /* tell children to shut down ASAP (no send_abort_for_crash here) */
            SetQuitSignalReason(QuitSignalReason::PMQUIT_FOR_STOP);
            childmgmt::TerminateChildren(SIGQUIT);
            statemachine::UpdatePMState(PMState::PmWaitBackends);

            /* set stopwatch for them to die */
            pm_mut().abort_start_time = crate::helpers::time_now();

            statemachine::PostmasterStateMachine();
        }

        _ => {}
    }
}

/// C: `static void process_pm_pmsignal(void)`.
pub fn process_pm_pmsignal() {
    let mut request_state_update = false;

    pm_mut().pending_pm_pmsignal = false;

    report(DEBUG2, "process_pm_pmsignal", "postmaster received pmsignal signal");

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_STARTED)
        && pm().pm_state == PMState::PmStartup
        && pm().shutdown == NO_SHUTDOWN
    {
        /* WAL redo has started. We're out of reinitialization. */
        pm_mut().fatal_error = false;
        pm_mut().abort_start_time = 0;
        sp::set_reached_consistency::call(false);

        /* Start the archiver if we're responsible for (re-)archiving files. */
        debug_assert!(pm().pgarch_pmchild.is_none());
        if sp::xlog_archiving_always::call() {
            pm_mut().pgarch_pmchild = startchildren::StartChildProcess(B_ARCHIVER);
        }

        if !sp::enable_hot_standby::call() {
            let _ = miscinit_seams::add_to_data_dir_lock_file::call(
                LOCK_FILE_LINE_PM_STATUS,
                PM_STATUS_STANDBY,
            );
        }

        statemachine::UpdatePMState(PMState::PmRecovery);
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_CONSISTENT)
        && pm().pm_state == PMState::PmRecovery
        && pm().shutdown == NO_SHUTDOWN
    {
        sp::set_reached_consistency::call(true);
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_BEGIN_HOT_STANDBY)
        && pm().pm_state == PMState::PmRecovery
        && pm().shutdown == NO_SHUTDOWN
    {
        report(LOG, "process_pm_pmsignal", "database system is ready to accept read-only connections");

        let _ = miscinit_seams::add_to_data_dir_lock_file::call(
            LOCK_FILE_LINE_PM_STATUS,
            PM_STATUS_READY,
        );

        statemachine::UpdatePMState(PMState::PmHotStandby);
        pm_mut().conns_allowed = true;
        pm_mut().start_worker_needed = true;
    }

    /* Process background worker state changes. */
    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE) {
        sp::background_worker_state_change::call(pm().pm_state < PMState::PmStopBackends);
        pm_mut().start_worker_needed = true;
    }

    /* Tell syslogger to rotate logfile if requested */
    if let Some(syslogger) = pm().syslogger_pmchild {
        if sp::check_logrotate_signal::call() {
            childmgmt::signal_child(syslogger, SIGUSR1);
            sp::remove_logrotate_signal_files::call();
        } else if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_ROTATE_LOGFILE) {
            childmgmt::signal_child(syslogger, SIGUSR1);
        }
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER)
        && pm().shutdown <= SMART_SHUTDOWN
        && pm().pm_state < PMState::PmStopBackends
    {
        pm_mut().start_autovac_launcher = true;
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER)
        && pm().shutdown <= SMART_SHUTDOWN
        && pm().pm_state < PMState::PmStopBackends
    {
        startchildren::StartAutovacuumWorker();
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_START_WALRECEIVER) {
        pm_mut().wal_receiver_requested = true;
    }

    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN) {
        if pm().pm_state == PMState::PmWaitXlogShutdown {
            debug_assert!(pm().shutdown > NO_SHUTDOWN);

            /* Waken archiver for the last time */
            if let Some(pgarch) = pm().pgarch_pmchild {
                childmgmt::signal_child(pgarch, SIGUSR2);
            }

            /* Waken walsenders for the last time. */
            childmgmt::SignalChildren(SIGUSR2, btmask(B_WAL_SENDER));

            statemachine::UpdatePMState(PMState::PmWaitXlogArchival);
        } else if !pm().fatal_error && pm().shutdown != IMMEDIATE_SHUTDOWN {
            report(LOG, "process_pm_pmsignal", "WAL was shut down unexpectedly");
            statemachine::HandleFatalError(QuitSignalReason::PMQUIT_FOR_CRASH, false);
        }

        request_state_update = true;
    }

    /* Try to advance postmaster's state machine, if a child requests it. */
    if CheckPostmasterSignal(PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE) {
        request_state_update = true;
    }

    if request_state_update {
        statemachine::PostmasterStateMachine();
    }

    if pm().startup_pmchild.is_some()
        && (pm().pm_state == PMState::PmStartup
            || pm().pm_state == PMState::PmRecovery
            || pm().pm_state == PMState::PmHotStandby)
        && sp::check_promote_signal::call()
    {
        /* Tell startup process to finish recovery. */
        childmgmt::signal_child(pm().startup_pmchild.unwrap(), SIGUSR2);
    }
}

/// C: `static void dummy_handler(SIGNAL_ARGS)`.
pub fn dummy_handler(_postgres_signal_arg: i32) {}
