//! Child-process death handling: the SIGCHLD reaper and its helpers.
//!
//! C source: `postmaster/postmaster.c` — `process_pm_child_exit`,
//! `CleanupBackend`, `LogChildExit`.

#![allow(non_snake_case)]

use ::pmchild::{
    FindPostmasterChildByPid, PMChild, ReleasePostmasterChildSlot,
};
use ::utils_error::{ereport};
use ::types_error::{DEBUG1, DEBUG2, DEBUG4};
use ::types_error::{ErrorLevel, LOG};

use crate::core::{
    exit_status_0, exit_status_1, exit_status_3, pm, pm_mut, wexitstatus, wifexited, wifsignaled,
    wtermsig, PMState, StartupStatusEnum, B_BG_WORKER, B_LOGGER, NO_SHUTDOWN, SIGTERM,
    SMART_SHUTDOWN,
};
use crate::helpers::{here, report, waitpid_nohang};
use crate::{childmgmt, ioworkers, serverloop, startchildren, statemachine};
use postmaster_seams as sp;

/// C: `static void process_pm_child_exit(void)`.
pub fn process_pm_child_exit() {
    pm_mut().pending_pm_child_exit = false;

    let _ = ereport(DEBUG4)
        .errmsg_internal("reaping dead processes")
        .finish(here("process_pm_child_exit"));

    loop {
        let (pid, exitstatus) = match waitpid_nohang() {
            Some(reaped) => (reaped.pid, reaped.exitstatus),
            None => break, /* pid <= 0 */
        };

        /* Check if this child was a startup process. */
        if matches!(pm().startup_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().startup_pmchild.unwrap());
            pm_mut().startup_pmchild = None;

            if pm().shutdown > NO_SHUTDOWN
                && (exit_status_0(exitstatus) || exit_status_1(exitstatus))
            {
                pm_mut().startup_status = StartupStatusEnum::StartupNotRunning;
                statemachine::UpdatePMState(PMState::PmWaitBackends);
                continue;
            }

            if exit_status_3(exitstatus) {
                report(LOG, "process_pm_child_exit", "shutdown at recovery target");
                pm_mut().startup_status = StartupStatusEnum::StartupNotRunning;
                pm_mut().shutdown = pm().shutdown.max(SMART_SHUTDOWN);
                childmgmt::TerminateChildren(SIGTERM);
                statemachine::UpdatePMState(PMState::PmWaitBackends);
                continue;
            }

            if pm().pm_state == PMState::PmStartup
                && pm().startup_status != StartupStatusEnum::StartupSignaled
                && !exit_status_0(exitstatus)
            {
                LogChildExit(LOG, "startup process", pid, exitstatus);
                report(
                    LOG,
                    "process_pm_child_exit",
                    "aborting startup due to startup process failure",
                );
                statemachine::ExitPostmaster(1);
            }

            if !exit_status_0(exitstatus) {
                if pm().startup_status == StartupStatusEnum::StartupSignaled {
                    pm_mut().startup_status = StartupStatusEnum::StartupNotRunning;
                    if pm().pm_state == PMState::PmStartup {
                        statemachine::UpdatePMState(PMState::PmWaitBackends);
                    }
                } else {
                    pm_mut().startup_status = StartupStatusEnum::StartupCrashed;
                }
                statemachine::HandleChildCrash(pid, exitstatus, "startup process");
                continue;
            }

            /* Startup succeeded, commence normal operations */
            pm_mut().startup_status = StartupStatusEnum::StartupNotRunning;
            pm_mut().fatal_error = false;
            pm_mut().abort_start_time = 0;
            pm_mut().reached_normal_running = true;

            /*
             * Re-seed this (postmaster) process' copy of the cluster-wide
             * TransamVariables / MultiXactState XID bounds from the control
             * file's checkpoint.
             *
             * In C these are genuine shared memory, so the startup process'
             * StartupXLOG seeding is already visible here. In this tree those
             * "shared" singletons are process-local statics inherited by
             * fork() copy-on-write, so the startup *child*'s writes died with
             * it. We must re-seed the postmaster's own copy now — before any
             * launcher/autovacuum/backend child is forked — so GetSnapshotData
             * in those children sees a valid oldestXid horizon. (xlog.c
             * StartupXLOG, 5634-5642 / 6144-6148.)
             */
            if let Err(e) =
                transam_xlog_seams::seed_transam_variables_from_checkpoint::call()
            {
                LogChildExit(LOG, "startup process", pid, exitstatus);
                report(
                    LOG,
                    "process_pm_child_exit",
                    format!("aborting startup: could not seed transaction-id bounds: {e:?}"),
                );
                statemachine::ExitPostmaster(1);
            }

            statemachine::UpdatePMState(PMState::PmRun);
            pm_mut().conns_allowed = true;
            pm_mut().start_worker_needed = true;

            report(
                LOG,
                "process_pm_child_exit",
                "database system is ready to accept connections",
            );

            let _ = miscinit_seams::add_to_data_dir_lock_file::call(
                crate::serverloop::LOCK_FILE_LINE_PM_STATUS,
                crate::serverloop::PM_STATUS_READY,
            );

            continue;
        }

        /* Was it the bgwriter? */
        if matches!(pm().bgwriter_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().bgwriter_pmchild.unwrap());
            pm_mut().bgwriter_pmchild = None;
            if !exit_status_0(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "background writer process");
            }
            continue;
        }

        /* Was it the checkpointer? */
        if matches!(pm().checkpointer_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().checkpointer_pmchild.unwrap());
            pm_mut().checkpointer_pmchild = None;
            if exit_status_0(exitstatus) && pm().pm_state == PMState::PmWaitCheckpointer {
                statemachine::UpdatePMState(PMState::PmWaitDeadEnd);
                serverloop::ConfigurePostmasterWaitSet(false);
                childmgmt::SignalChildren(SIGTERM, btmask_all_except!(B_LOGGER));
            } else {
                statemachine::HandleChildCrash(pid, exitstatus, "checkpointer process");
            }
            continue;
        }

        /* Was it the wal writer? */
        if matches!(pm().walwriter_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().walwriter_pmchild.unwrap());
            pm_mut().walwriter_pmchild = None;
            if !exit_status_0(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "WAL writer process");
            }
            continue;
        }

        /* Was it the wal receiver? */
        if matches!(pm().walreceiver_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().walreceiver_pmchild.unwrap());
            pm_mut().walreceiver_pmchild = None;
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "WAL receiver process");
            }
            continue;
        }

        /* Was it the wal summarizer? */
        if matches!(pm().walsummarizer_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().walsummarizer_pmchild.unwrap());
            pm_mut().walsummarizer_pmchild = None;
            if !exit_status_0(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "WAL summarizer process");
            }
            continue;
        }

        /* Was it the autovacuum launcher? */
        if matches!(pm().autovac_launcher_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().autovac_launcher_pmchild.unwrap());
            pm_mut().autovac_launcher_pmchild = None;
            if !exit_status_0(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "autovacuum launcher process");
            }
            continue;
        }

        /* Was it the archiver? */
        if matches!(pm().pgarch_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().pgarch_pmchild.unwrap());
            pm_mut().pgarch_pmchild = None;
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "archiver process");
            }
            continue;
        }

        /* Was it the system logger? If so, try to start a new one */
        if matches!(pm().syslogger_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().syslogger_pmchild.unwrap());
            pm_mut().syslogger_pmchild = None;

            /* for safety's sake, launch new logger *first* */
            if sp::logging_collector::call() {
                startchildren::StartSysLogger();
            }

            if !exit_status_0(exitstatus) {
                LogChildExit(LOG, "system logger process", pid, exitstatus);
            }
            continue;
        }

        /* Was it the slot sync worker? */
        if matches!(pm().slotsync_worker_pmchild, Some(bp) if bp.pid == pid) {
            ReleasePostmasterChildSlot(pm().slotsync_worker_pmchild.unwrap());
            pm_mut().slotsync_worker_pmchild = None;
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "slot sync worker process");
            }
            continue;
        }

        /* Was it an IO worker? */
        if ioworkers::maybe_reap_io_worker(pid) {
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                statemachine::HandleChildCrash(pid, exitstatus, "io worker");
            }
            ioworkers::maybe_adjust_io_workers();
            continue;
        }

        /* Was it a backend or a background worker? */
        if let Some(pmchild) = FindPostmasterChildByPid(pid) {
            CleanupBackend(pmchild, exitstatus);
        }
        /*
         * We don't know anything about this child process. Highly unexpected,
         * as we track all the child processes we fork.
         */
        else if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
            statemachine::HandleChildCrash(pid, exitstatus, "untracked child process");
        } else {
            LogChildExit(LOG, "untracked child process", pid, exitstatus);
        }
    } /* loop over pending child-death reports */

    /* After cleaning out the SIGCHLD queue, advance the state machine. */
    statemachine::PostmasterStateMachine();
}

/// C: `static void CleanupBackend(PMChild *bp, int exitstatus)`.
pub fn CleanupBackend(bp: PMChild, exitstatus: i32) {
    let mut crashed = false;
    let mut logged = false;

    /* Construct a process name for the log message */
    let bp_bkend_type = bp.bkend_type;
    let rw = bp.rw;
    let procname: String = if bp_bkend_type == B_BG_WORKER {
        let rw_index = rw.expect("B_BG_WORKER PMChild without rw index");
        let bgw_type = sp::rw_bgw_type::call(rw_index);
        // C: snprintf(namebuf, MAXPGPATH, "background worker \"%s\"", ...)
        let mut s = format!("background worker \"{bgw_type}\"");
        let maxlen = types_core::primitive::MAXPGPATH - 1;
        if s.len() > maxlen {
            let mut end = maxlen;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
        }
        s
    } else {
        miscinit_seams::get_backend_type_desc::call(bp_bkend_type).to_string()
    };

    if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
        crashed = true;
    }

    let bp_pid = bp.pid;
    let bp_bgworker_notify = bp.bgworker_notify;
    /*
     * Release the PMChild entry. If the process attached to shared memory, this
     * also checks that it detached cleanly.
     */
    if !ReleasePostmasterChildSlot(bp) {
        crashed = true;
    }
    // bp is now released; do not touch it again (C sets `bp = NULL`).

    if crashed {
        statemachine::HandleChildCrash(bp_pid, exitstatus, &procname);
        return;
    }

    /*
     * This backend may have been slated to receive SIGUSR1 when some background
     * worker started or stopped. Cancel those notifications.
     */
    if bp_bgworker_notify {
        sp::background_worker_stop_notifications::call(bp_pid);
    }

    /* If it was a background worker, also update its RegisteredBgWorker entry. */
    if bp_bkend_type == B_BG_WORKER {
        let rw_index = rw.expect("B_BG_WORKER PMChild without rw index");
        if !exit_status_0(exitstatus) {
            sp::rw_set_crashed_at::call(
                rw_index,
                timestamp_seams::get_current_timestamp::call(),
            );
        } else {
            sp::rw_set_crashed_at::call(rw_index, 0);
            sp::rw_set_terminate::call(rw_index, true);
        }

        sp::rw_set_pid::call(rw_index, 0);
        sp::report_background_worker_exit::call(rw_index);

        if !logged {
            LogChildExit(
                if exit_status_0(exitstatus) { DEBUG1 } else { LOG },
                &procname,
                bp_pid,
                exitstatus,
            );
            logged = true;
        }

        /* have it be restarted */
        pm_mut().have_crashed_worker = true;
    }

    if !logged {
        LogChildExit(DEBUG2, &procname, bp_pid, exitstatus);
    }
}

/// C: `static void LogChildExit(int lev, const char *procname, int pid, int
/// exitstatus)`.
pub fn LogChildExit(lev: ErrorLevel, procname: &str, pid: i32, exitstatus: i32) {
    let activity: Option<String> = if !exit_status_0(exitstatus) {
        sp::pgstat_get_crashed_backend_activity::call(pid)
    } else {
        None
    };

    if wifexited(exitstatus) {
        let mut b = ereport(lev).errmsg(format!(
            "{} (PID {}) exited with exit code {}",
            procname,
            pid,
            wexitstatus(exitstatus)
        ));
        if let Some(a) = &activity {
            b = b.errdetail(format!("Failed process was running: {a}"));
        }
        let _ = b.finish(here("LogChildExit"));
    } else if wifsignaled(exitstatus) {
        let signame = sp::pg_strsignal::call(wtermsig(exitstatus));
        let mut b = ereport(lev).errmsg(format!(
            "{} (PID {}) was terminated by signal {}: {}",
            procname,
            pid,
            wtermsig(exitstatus),
            signame
        ));
        if let Some(a) = &activity {
            b = b.errdetail(format!("Failed process was running: {a}"));
        }
        let _ = b.finish(here("LogChildExit"));
    } else {
        let mut b = ereport(lev).errmsg(format!(
            "{} (PID {}) exited with unrecognized status {}",
            procname, pid, exitstatus
        ));
        if let Some(a) = &activity {
            b = b.errdetail(format!("Failed process was running: {a}"));
        }
        let _ = b.finish(here("LogChildExit"));
    }
}
