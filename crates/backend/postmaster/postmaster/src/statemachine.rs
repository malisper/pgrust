//! The postmaster state machine: startup/shutdown/crash-recovery sequencing.
//!
//! C source: `postmaster/postmaster.c` — `PostmasterStateMachine`,
//! `HandleFatalError`, `HandleChildCrash`, `UpdatePMState`, `pmstate_name`,
//! `ExitPostmaster`, `canAcceptConnections`.

#![allow(non_snake_case)]

use ::pmsignal::{QuitSignalReason, SetQuitSignalReason};
use ::types_error::LOG;
use ::types_startup::CacState;
use ::types_core::init::BackendType;

use crate::core::{
    btmask, pm, pm_mut, BackendTypeMask, PMState, StartupStatusEnum, BTYPE_MASK_ALL,
    BTYPE_MASK_NONE, B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND, B_BG_WORKER,
    B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND, B_INVALID, B_IO_WORKER, B_LOGGER,
    B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP, B_WAL_RECEIVER, B_WAL_SENDER,
    B_WAL_SUMMARIZER, B_WAL_WRITER, IMMEDIATE_SHUTDOWN, NO_SHUTDOWN, SIGABRT, SIGINT, SIGQUIT,
    SIGTERM, SIGUSR2,
};
use crate::helpers::{elog_debug1, report, time_now};
use crate::{childmgmt, ioworkers, serverloop, startchildren};
use postmaster_seams as sp;

/// C: `static void HandleFatalError(QuitSignalReason reason, bool consider_sigabrt)`.
pub fn HandleFatalError(reason: QuitSignalReason, consider_sigabrt: bool) {
    debug_assert!(!pm().fatal_error);
    debug_assert!(pm().shutdown != IMMEDIATE_SHUTDOWN);

    SetQuitSignalReason(reason);

    let sigtosend = if consider_sigabrt && sp::send_abort_for_crash::call() {
        SIGABRT
    } else {
        SIGQUIT
    };

    /*
     * Signal all other child processes to exit. We could exclude dead-end
     * children here, but at least when sending SIGABRT it seems better to
     * include them.
     */
    childmgmt::TerminateChildren(sigtosend);

    pm_mut().fatal_error = true;

    /*
     * Choose the appropriate new state to react to the fatal error.
     */
    match pm().pm_state {
        PMState::PmInit => {
            /* shouldn't have any children */
            debug_assert!(false);
        }
        PMState::PmStartup => {
            /* should have been handled in process_pm_child_exit */
            debug_assert!(false);
        }

        PMState::PmRecovery
        | PMState::PmHotStandby
        | PMState::PmRun
        | PMState::PmStopBackends => {
            UpdatePMState(PMState::PmWaitBackends);
        }

        PMState::PmWaitBackends => { /* there might be more backends to wait for */ }

        PMState::PmWaitXlogShutdown
        | PMState::PmWaitXlogArchival
        | PMState::PmWaitCheckpointer
        | PMState::PmWaitIoWorkers => {
            serverloop::ConfigurePostmasterWaitSet(false);
            UpdatePMState(PMState::PmWaitDeadEnd);
        }

        PMState::PmWaitDeadEnd | PMState::PmNoChildren => {}
    }

    /*
     * .. and if this doesn't happen quickly enough, the clock starts ticking
     * for us to kill them without mercy.
     */
    if pm().abort_start_time == 0 {
        pm_mut().abort_start_time = time_now();
    }
}

/// C: `static void HandleChildCrash(int pid, int exitstatus, const char *procname)`.
pub fn HandleChildCrash(pid: i32, exitstatus: i32, procname: &str) {
    /*
     * We only log messages and send signals if this is the first process crash
     * and we're not doing an immediate shutdown; otherwise we're only here to
     * update postmaster's idea of live processes.
     */
    if pm().fatal_error || pm().shutdown == IMMEDIATE_SHUTDOWN {
        return;
    }

    crate::reaper::LogChildExit(LOG, procname, pid, exitstatus);
    report(LOG, "HandleChildCrash", "terminating any other active server processes");

    /* The crashed process has already been removed from ActiveChildList. */
    HandleFatalError(QuitSignalReason::PMQUIT_FOR_CRASH, true);
}

/// C: `static void PostmasterStateMachine(void)`.
pub fn PostmasterStateMachine() {
    /* If we're doing a smart shutdown, try to advance that state. */
    if pm().pm_state == PMState::PmRun || pm().pm_state == PMState::PmHotStandby {
        if !pm().conns_allowed {
            if childmgmt::CountChildren(btmask(B_BACKEND)) == 0 {
                UpdatePMState(PMState::PmStopBackends);
            }
        }
    }

    if pm().pm_state == PMState::PmStopBackends || pm().pm_state == PMState::PmWaitBackends {
        let mut targetMask: BackendTypeMask = BTYPE_MASK_NONE;

        targetMask = btmask_add!(
            targetMask,
            B_BACKEND,
            B_AUTOVAC_LAUNCHER,
            B_AUTOVAC_WORKER,
            B_BG_WORKER
        );

        targetMask = btmask_add!(
            targetMask,
            B_WAL_WRITER,
            B_BG_WRITER,
            B_SLOTSYNC_WORKER,
            B_WAL_SUMMARIZER
        );

        targetMask = btmask_add!(targetMask, B_STARTUP, B_WAL_RECEIVER);

        if pm().fatal_error || pm().shutdown >= IMMEDIATE_SHUTDOWN {
            targetMask = btmask_add!(
                targetMask,
                B_CHECKPOINTER,
                B_ARCHIVER,
                B_IO_WORKER,
                B_WAL_SENDER
            );
        }

        #[cfg(debug_assertions)]
        {
            let mut remainMask: BackendTypeMask = BTYPE_MASK_NONE;

            remainMask = btmask_add!(remainMask, B_DEAD_END_BACKEND, B_LOGGER);
            remainMask = btmask_add!(
                remainMask,
                B_ARCHIVER,
                B_CHECKPOINTER,
                B_IO_WORKER,
                B_WAL_SENDER
            );
            remainMask = btmask_add!(remainMask, B_INVALID, B_STANDALONE_BACKEND);

            debug_assert!((remainMask.mask | targetMask.mask) == BTYPE_MASK_ALL.mask);
        }

        if pm().pm_state == PMState::PmStopBackends {
            /*
             * Forget any pending requests for background workers, since we're
             * no longer willing to launch any new workers.
             */
            sp::forget_unstarted_background_workers::call();

            childmgmt::SignalChildren(SIGTERM, targetMask);

            UpdatePMState(PMState::PmWaitBackends);
        }

        if childmgmt::CountChildren(targetMask) == 0 {
            if pm().shutdown >= IMMEDIATE_SHUTDOWN || pm().fatal_error {
                UpdatePMState(PMState::PmWaitDeadEnd);
                serverloop::ConfigurePostmasterWaitSet(false);
                childmgmt::SignalChildren(SIGQUIT, btmask(B_DEAD_END_BACKEND));
            } else {
                debug_assert!(pm().shutdown > NO_SHUTDOWN);
                /* Start the checkpointer if not running */
                if pm().checkpointer_pmchild.is_none() {
                    pm_mut().checkpointer_pmchild =
                        startchildren::StartChildProcess(B_CHECKPOINTER);
                }
                /* And tell it to write the shutdown checkpoint */
                if let Some(ckpt) = pm().checkpointer_pmchild {
                    childmgmt::signal_child(ckpt, SIGINT);
                    UpdatePMState(PMState::PmWaitXlogShutdown);
                } else {
                    /*
                     * If we failed to fork a checkpointer, just shut down. We
                     * set FatalError so an "abnormal shutdown" message is logged
                     * when we exit.
                     */
                    HandleFatalError(QuitSignalReason::PMQUIT_FOR_CRASH, false);
                }
            }
        }
    }

    /*
     * PM_WAIT_XLOG_SHUTDOWN -> PM_WAIT_XLOG_ARCHIVAL is in process_pm_pmsignal().
     */

    if pm().pm_state == PMState::PmWaitXlogArchival {
        if childmgmt::CountChildren(btmask_all_except!(
            B_CHECKPOINTER,
            B_IO_WORKER,
            B_LOGGER,
            B_DEAD_END_BACKEND
        )) == 0
        {
            UpdatePMState(PMState::PmWaitIoWorkers);
            childmgmt::SignalChildren(SIGUSR2, btmask(B_IO_WORKER));
        }
    }

    if pm().pm_state == PMState::PmWaitIoWorkers {
        if pm().io_worker_count == 0 {
            UpdatePMState(PMState::PmWaitCheckpointer);

            if let Some(ckpt) = pm().checkpointer_pmchild {
                childmgmt::signal_child(ckpt, SIGUSR2);
            }
        }
    }

    /*
     * PM_WAIT_CHECKPOINTER -> PM_WAIT_DEAD_END is in process_pm_child_exit().
     */

    if pm().pm_state == PMState::PmWaitDeadEnd {
        if childmgmt::CountChildren(btmask_all_except!(B_LOGGER)) == 0 {
            debug_assert!(pm().startup_pmchild.is_none());
            debug_assert!(pm().walreceiver_pmchild.is_none());
            debug_assert!(pm().walsummarizer_pmchild.is_none());
            debug_assert!(pm().bgwriter_pmchild.is_none());
            debug_assert!(pm().checkpointer_pmchild.is_none());
            debug_assert!(pm().walwriter_pmchild.is_none());
            debug_assert!(pm().autovac_launcher_pmchild.is_none());
            debug_assert!(pm().slotsync_worker_pmchild.is_none());
            /* syslogger is not considered here */
            UpdatePMState(PMState::PmNoChildren);
        }
    }

    /*
     * If we've been told to shut down, exit as soon as there are no remaining
     * children.
     */
    if pm().shutdown > NO_SHUTDOWN && pm().pm_state == PMState::PmNoChildren {
        if pm().fatal_error {
            report(LOG, "PostmasterStateMachine", "abnormal database system shutdown");
            ExitPostmaster(1);
        } else {
            ExitPostmaster(0);
        }
    }

    if pm().pm_state == PMState::PmNoChildren {
        if pm().startup_status == StartupStatusEnum::StartupCrashed {
            report(LOG, "PostmasterStateMachine", "shutting down due to startup process failure");
            ExitPostmaster(1);
        }
        if !sp::restart_after_crash::call() {
            report(
                LOG,
                "PostmasterStateMachine",
                "shutting down because \"restart_after_crash\" is off",
            );
            ExitPostmaster(1);
        }
    }

    /*
     * If we need to recover from a crash, wait for all non-syslogger children
     * to exit, then reset shmem and start the startup process.
     */
    if pm().fatal_error && pm().pm_state == PMState::PmNoChildren {
        report(LOG, "PostmasterStateMachine", "all server processes terminated; reinitializing");

        /* remove leftover temporary files after a crash */
        if sp::remove_temp_files_after_crash::call() {
            sp::remove_pg_temp_files::call();
        }

        /* allow background workers to immediately restart */
        sp::reset_background_worker_crash_times::call();

        sp::shmem_exit::call(1);

        /* re-read control file into local memory */
        sp::local_process_control_file::call(true);

        /*
         * Re-create shared memory and semaphores.
         *
         * DIVERGENCE FROM C: in C this destroys the old shared-memory segment
         * (an OS-level SysV/mmap region the crashed child may have corrupted)
         * and allocates a fresh, zeroed one, which every re-forked child then
         * attaches to. In this tree the "shared" structures are process-local
         * statics that backends inherit through fork()'s copy-on-write: the
         * crashed backend only ever scribbled on its OWN private copy, so the
         * postmaster's copy is still intact and consistent. We have just run
         * shmem_exit(1) to drop this process' shmem-exit callbacks / LWLocks,
         * which leaves the postmaster's shmem statics in exactly the state a
         * freshly re-created (but already-populated-by-the-postmaster) segment
         * would be in. The re-forked startup process inherits that copy through
         * fork().
         *
         * Re-running CreateSharedMemoryAndSemaphores here would, in fact,
         * CRASH the postmaster: the per-subsystem *ShmemInit functions publish
         * their structures into write-once cells (e.g. MainLWLockArray's
         * OnceLock, handing out `&'static` references the rest of the process
         * already holds), so a second create panics ("MainLWLockArray
         * published twice") and the panic propagates out of the postmaster's
         * server loop, taking the whole cluster down — which is precisely the
         * crash this guard fixes. Because the postmaster's segment is never
         * corrupted in the fork-COW model, skipping the re-creation is both
         * necessary (the cells cannot be safely re-published) and faithful (the
         * data is already correct).
         */
        if !pm().shmem_created {
            ipci_seams::create_shared_memory_and_semaphores::call();
            pm_mut().shmem_created = true;
        }

        UpdatePMState(PMState::PmStartup);

        /* Make sure we can perform I/O while starting up. */
        ioworkers::maybe_adjust_io_workers();

        pm_mut().startup_pmchild = startchildren::StartChildProcess(B_STARTUP);
        debug_assert!(pm().startup_pmchild.is_some());
        pm_mut().startup_status = StartupStatusEnum::StartupRunning;
        /* crash recovery started, reset SIGKILL flag */
        pm_mut().abort_start_time = 0;

        /* start accepting server socket connection events again */
        serverloop::ConfigurePostmasterWaitSet(true);
    }
}

/// C: `static const char *pmstate_name(PMState state)`.
pub fn pmstate_name(state: PMState) -> &'static str {
    match state {
        PMState::PmInit => "PM_INIT",
        PMState::PmStartup => "PM_STARTUP",
        PMState::PmRecovery => "PM_RECOVERY",
        PMState::PmHotStandby => "PM_HOT_STANDBY",
        PMState::PmRun => "PM_RUN",
        PMState::PmStopBackends => "PM_STOP_BACKENDS",
        PMState::PmWaitBackends => "PM_WAIT_BACKENDS",
        PMState::PmWaitXlogShutdown => "PM_WAIT_XLOG_SHUTDOWN",
        PMState::PmWaitXlogArchival => "PM_WAIT_XLOG_ARCHIVAL",
        PMState::PmWaitIoWorkers => "PM_WAIT_IO_WORKERS",
        PMState::PmWaitDeadEnd => "PM_WAIT_DEAD_END",
        PMState::PmWaitCheckpointer => "PM_WAIT_CHECKPOINTER",
        PMState::PmNoChildren => "PM_NO_CHILDREN",
    }
}

/// C: `static void UpdatePMState(PMState newState)`.
pub fn UpdatePMState(newState: PMState) {
    elog_debug1(
        "UpdatePMState",
        format!(
            "updating PMState from {} to {}",
            pmstate_name(pm().pm_state),
            pmstate_name(newState)
        ),
    );
    pm_mut().pm_state = newState;
}

/// C: `static CAC_state canAcceptConnections(BackendType backend_type)`.
pub fn canAcceptConnections(backend_type: BackendType) -> CacState {
    let result = CacState::Ok;

    debug_assert!(backend_type == B_BACKEND || backend_type == B_AUTOVAC_WORKER);

    if pm().pm_state != PMState::PmRun && pm().pm_state != PMState::PmHotStandby {
        if pm().shutdown > NO_SHUTDOWN {
            return CacState::Shutdown;
        } else if !pm().fatal_error && pm().pm_state == PMState::PmStartup {
            return CacState::Startup;
        } else if !pm().fatal_error && pm().pm_state == PMState::PmRecovery {
            return CacState::NotHotStandby;
        } else {
            return CacState::Recovery;
        }
    }

    if !pm().conns_allowed && backend_type == B_BACKEND {
        return CacState::Shutdown;
    }

    result
}

/// C: `pg_noreturn static void ExitPostmaster(int status)`.
///
/// Do NOT call exit() directly --- always go through here!
pub fn ExitPostmaster(status: i32) -> ! {
    // C also checks `#ifdef HAVE_PTHREAD_IS_THREADED_NP` here, then calls
    // `proc_exit(status)`. The exit primitive is the ipc seam.
    ipc_seams::proc_exit::call(status)
}
