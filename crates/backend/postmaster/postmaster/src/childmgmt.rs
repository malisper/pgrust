//! Child-process signalling and counting.
//!
//! C source: `postmaster/postmaster.c` — `signal_child`, `SignalChildren`,
//! `TerminateChildren`, `CountChildren`.
//!
//! The active child list (`dlist_head ActiveChildList`, owned by `pmchild.c`)
//! is walked through [`::pmchild::ActiveChildListSnapshot`],
//! which snapshots the live [`PMChild`]s in `dlist_foreach` order; the
//! postmaster filters and signals/counts over that snapshot exactly as C
//! iterates the list. Per-entry `bkend_type` relabeling (a backend that became
//! a walsender) is written back through pmchild's keyed mutator.

#![allow(non_snake_case)]

use pmchild::{PMChild, SetActiveChildBkendType, ActiveChildListSnapshot};
use utils_error::{ereport};
use types_error::{DEBUG3, DEBUG4};

use crate::core::{
    btmask_contains, pm, pm_mut, BackendTypeMask, StartupStatusEnum, B_BACKEND, B_LOGGER,
    B_WAL_SENDER, SIGABRT, SIGINT, SIGKILL, SIGQUIT, SIGTERM,
};
use crate::helpers::{here, kill, pm_signame};

/// C: `static void signal_child(PMChild *pmchild, int signal)`.
///
/// Send a signal to a postmaster child process.
///
/// On systems with `setsid()` each child is a process-group leader; for the
/// terminating signals we signal the whole group as well as the child directly
/// (a recently-forked child might not have run `setsid()` yet).
pub fn signal_child(pmchild: PMChild, signal: i32) {
    let pid = pmchild.pid;
    let desc = miscinit_seams::get_backend_type_desc::call(pmchild.bkend_type);
    let _ = ereport(DEBUG3)
        .errmsg_internal(format!(
            "sending signal {}/{} to {} process with pid {}",
            signal,
            pm_signame(signal),
            desc,
            pid,
        ))
        .finish(here("signal_child"));

    if kill(pid, signal) < 0 {
        let _ = ereport(DEBUG3)
            .errmsg_internal(format!("kill({pid},{signal}) failed"))
            .finish(here("signal_child"));
    }

    // C: #ifdef HAVE_SETSID — true on every platform this port targets.
    match signal {
        x if x == SIGINT || x == SIGTERM || x == SIGQUIT || x == SIGKILL || x == SIGABRT => {
            if kill(-pid, signal) < 0 {
                let _ = ereport(DEBUG3)
                    .errmsg_internal(format!("kill({},{signal}) failed", -pid))
                    .finish(here("signal_child"));
            }
        }
        _ => {}
    }
}

/// C: `static bool SignalChildren(int signal, BackendTypeMask targetMask)`.
pub fn SignalChildren(signal: i32, targetMask: BackendTypeMask) -> bool {
    let mut signaled = false;

    for mut bp in ActiveChildListSnapshot() {
        /*
         * If we need to distinguish between B_BACKEND and B_WAL_SENDER, check
         * if any B_BACKEND backends have recently announced that they are
         * actually WAL senders.
         */
        if btmask_contains(targetMask, B_WAL_SENDER) != btmask_contains(targetMask, B_BACKEND)
            && bp.bkend_type == B_BACKEND
            && pmsignal::IsPostmasterChildWalSender(bp.child_slot)
        {
            SetActiveChildBkendType(bp.child_slot, B_WAL_SENDER);
            bp.bkend_type = B_WAL_SENDER;
        }

        if !btmask_contains(targetMask, bp.bkend_type) {
            continue;
        }

        signal_child(bp, signal);
        signaled = true;
    }

    signaled
}

/// C: `static void TerminateChildren(int signal)`.
///
/// Send a termination signal to children. Considers all child processes except
/// the syslogger.
pub fn TerminateChildren(signal: i32) {
    SignalChildren(signal, btmask_all_except!(B_LOGGER));
    if pm().startup_pmchild.is_some()
        && (signal == SIGQUIT || signal == SIGKILL || signal == SIGABRT)
    {
        pm_mut().startup_status = StartupStatusEnum::StartupSignaled;
    }
}

/// C: `static int CountChildren(BackendTypeMask targetMask)`.
pub fn CountChildren(targetMask: BackendTypeMask) -> i32 {
    let mut cnt: i32 = 0;

    for mut bp in ActiveChildListSnapshot() {
        if btmask_contains(targetMask, B_WAL_SENDER) != btmask_contains(targetMask, B_BACKEND)
            && bp.bkend_type == B_BACKEND
            && pmsignal::IsPostmasterChildWalSender(bp.child_slot)
        {
            SetActiveChildBkendType(bp.child_slot, B_WAL_SENDER);
            bp.bkend_type = B_WAL_SENDER;
        }

        if !btmask_contains(targetMask, bp.bkend_type) {
            continue;
        }

        let _ = ereport(DEBUG4)
            .errmsg_internal(format!(
                "{} process {} is still running",
                miscinit_seams::get_backend_type_desc::call(bp.bkend_type),
                bp.pid,
            ))
            .finish(here("CountChildren"));

        cnt += 1;
    }

    cnt
}
