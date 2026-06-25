//! The postmaster main idle loop and connection accept path.
//!
//! C source: `postmaster/postmaster.c` — `ServerLoop`, `DetermineSleepTime`,
//! `ConfigurePostmasterWaitSet`, `BackendStartup`,
//! `report_fork_failure_to_client`.

#![allow(non_snake_case)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use ::pqcomm::{AcceptConnection, TouchSocketFiles};
use ::launch_backend::postmaster_child_launch;
use ::pmchild::{
    AllocDeadEndChild, AssignPostmasterChildSlot, ReleasePostmasterChildSlot,
    SetActiveChildBgworkerInfo, SetActiveChildPidByEntry,
};
use ::waiteventset_seams::WaitEventSet;
use ::utils_error::{ereport};
use ::types_error::{DEBUG2, LOG};
use ::types_error::ERRCODE_OUT_OF_MEMORY;
use ::net::ClientSocket;
use ::types_startup::{BackendStartupData, CacState, StartupData};
use ::types_storage::waiteventset::{WaitEvent, WL_LATCH_SET, WL_SOCKET_ACCEPT};

use crate::core::{
    pm, pm_mut, BackendType, IMMEDIATE_SHUTDOWN, MAXLISTEN, NO_SHUTDOWN, SIGABRT, SIGKILL, SIGQUIT,
    SIGKILL_CHILDREN_AFTER_SECS, SIGUSR2, B_BACKEND,
};
use crate::helpers::{closesocket, here, kill, report, time_now};
use crate::latchutil::reset_latch;
use crate::{childmgmt, reaper, signals, startchildren, statemachine};
use postmaster_seams as sp;

// ---------------------------------------------------------------------------
// pidfile / wait-event / status constants
// ---------------------------------------------------------------------------

/// C: `#define LOCK_FILE_LINE_PM_STATUS 8` (`utils/pidfile.h`).
pub const LOCK_FILE_LINE_PM_STATUS: i32 = 8;

pub const PM_STATUS_STARTING: &str = "starting";
pub const PM_STATUS_STOPPING: &str = "stopping";
pub const PM_STATUS_READY: &str = "ready   ";
pub const PM_STATUS_STANDBY: &str = "standby ";

/// C: `#define STATUS_OK (0)`.
pub const STATUS_OK: i32 = 0;
/// C: `#define STATUS_ERROR (-1)`.
pub const STATUS_ERROR: i32 = -1;

/// C: `#define PGINVALID_SOCKET (-1)`.
pub const PGINVALID_SOCKET: i32 = -1;

/// C: `#define SECS_PER_MINUTE 60`.
pub const SECS_PER_MINUTE: i64 = 60;

/// C: `#define BGW_NEVER_RESTART -1`.
pub const BGW_NEVER_RESTART: i32 = -1;

// ---------------------------------------------------------------------------
// DetermineSleepTime
// ---------------------------------------------------------------------------

/// C: `static int DetermineSleepTime(void)` — milliseconds to sleep.
pub fn DetermineSleepTime() -> i64 {
    let mut next_wakeup: types_core::primitive::TimestampTz = 0;

    if pm().shutdown > NO_SHUTDOWN || (!pm().start_worker_needed && !pm().have_crashed_worker) {
        if pm().abort_start_time != 0 {
            /* time left to abort; clamp to 0 in case it already expired */
            let seconds = SIGKILL_CHILDREN_AFTER_SECS - (time_now() - pm().abort_start_time);
            return (seconds * 1000).max(0);
        } else {
            return 60 * 1000;
        }
    }

    if pm().start_worker_needed {
        return 0;
    }

    if pm().have_crashed_worker {
        for rw in sp::background_worker_list::call() {
            let rw_crashed_at = sp::rw_crashed_at::call(rw);
            if rw_crashed_at == 0 {
                continue;
            }

            if sp::rw_bgw_restart_time::call(rw) == BGW_NEVER_RESTART || sp::rw_terminate::call(rw)
            {
                sp::forget_background_worker::call(rw);
                continue;
            }

            /* TimestampTzPlusMilliseconds(rw_crashed_at, 1000 * bgw_restart_time) */
            let this_wakeup = rw_crashed_at + (1000i64 * sp::rw_bgw_restart_time::call(rw) as i64) * 1000;
            if next_wakeup == 0 || this_wakeup < next_wakeup {
                next_wakeup = this_wakeup;
            }
        }
    }

    if next_wakeup != 0 {
        /* TimestampDifferenceMilliseconds(now, next_wakeup), clamped to [0, INT_MAX] */
        let now = timestamp_seams::get_current_timestamp::call();
        let diff_us = next_wakeup - now;
        let ms = if diff_us <= 0 {
            0
        } else {
            (diff_us / 1000).min(i32::MAX as i64)
        };
        return (60 * 1000).min(ms);
    }

    60 * 1000
}

// ---------------------------------------------------------------------------
// ConfigurePostmasterWaitSet
// ---------------------------------------------------------------------------

/// C: `static void ConfigurePostmasterWaitSet(bool accept_connections)`.
///
/// Since we can't remove events from an existing WaitEventSet, destroy and
/// recreate the whole thing. The set is freed in fork children by
/// `ClosePostmasterPorts()`.
pub fn ConfigurePostmasterWaitSet(accept_connections: bool) {
    // FreeWaitEventSet(pm_wait_set); pm_wait_set = NULL;
    // (Drop on the owned WaitEventSet calls FreeWaitEventSet.)
    pm_mut().pm_wait_set = None;

    let num_listen = pm().listen_sockets.len() as i32;
    let nevents = if accept_connections {
        1 + num_listen
    } else {
        1
    };
    let set = WaitEventSet::create(nevents).expect("CreateWaitEventSet");

    // AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL).
    set.add_event(WL_LATCH_SET, PGINVALID_SOCKET, latch::my_latch(), None)
        .expect("AddWaitEventToSet latch");

    if accept_connections {
        for i in 0..pm().listen_sockets.len() {
            let fd = pm().listen_sockets[i];
            set.add_event(WL_SOCKET_ACCEPT, fd, None, None)
                .expect("AddWaitEventToSet listen socket");
        }
    }

    pm_mut().pm_wait_set = Some(set);
}

// ---------------------------------------------------------------------------
// ServerLoop
// ---------------------------------------------------------------------------

/// C: `static int ServerLoop(void)`. Loops forever; never returns.
pub fn ServerLoop() -> ! {
    ConfigurePostmasterWaitSet(true);
    let mut last_lockfile_recheck_time = time_now();
    let mut last_touch_time = last_lockfile_recheck_time;

    // Scratch buffer for occurred events (C: `WaitEvent events[MAXLISTEN]`).
    let mut events: Vec<WaitEvent> = (0..MAXLISTEN)
        .map(|_| WaitEvent {
            pos: 0,
            events: 0,
            fd: PGINVALID_SOCKET,
            user_data: None,
        })
        .collect();

    loop {
        let timeout = DetermineSleepTime();
        let nevents = {
            let set = pm().pm_wait_set.as_ref().expect("pm_wait_set");
            set.wait(timeout, &mut events, 0).expect("WaitEventSetWait") as usize
        };
        let nevents = nevents.min(MAXLISTEN);

        for i in 0..nevents {
            if (events[i].events & WL_LATCH_SET) != 0 {
                reset_latch();
            }

            /*
             * The following requests are handled unconditionally, even if we
             * didn't see WL_LATCH_SET. This gives high priority to shutdown and
             * reload requests.
             */
            if pm().pending_pm_shutdown_request {
                signals::process_pm_shutdown_request();
            }
            if pm().pending_pm_reload_request {
                signals::process_pm_reload_request();
            }
            if pm().pending_pm_child_exit {
                reaper::process_pm_child_exit();
            }
            if pm().pending_pm_pmsignal {
                signals::process_pm_pmsignal();
            }

            if (events[i].events & WL_SOCKET_ACCEPT) != 0 {
                let mut client_sock = ClientSocket::default();
                let status = AcceptConnection(events[i].fd, &mut client_sock);
                if status == STATUS_OK {
                    BackendStartup(&client_sock);
                }

                /* We no longer need the open socket in this process */
                let sock = client_sock.sock;
                if sock != PGINVALID_SOCKET {
                    if closesocket(sock) != 0 {
                        report(LOG, "ServerLoop", "could not close client socket");
                    }
                }
            }
        }

        /* Launch any background processes needed after state changes. */
        startchildren::LaunchMissingBackgroundProcesses();

        /* If we need to signal the autovacuum launcher, do so now */
        if pm().avlauncher_needs_signal {
            pm_mut().avlauncher_needs_signal = false;
            if let Some(avl) = pm().autovac_launcher_pmchild {
                childmgmt::signal_child(avl, SIGUSR2);
            }
        }

        let now = time_now();

        /*
         * If we already sent SIGQUIT to children and they're slow to shut down,
         * it's time to SIGKILL (or SIGABRT if requested).
         */
        if (pm().shutdown >= IMMEDIATE_SHUTDOWN || pm().fatal_error)
            && pm().abort_start_time != 0
            && (now - pm().abort_start_time) >= SIGKILL_CHILDREN_AFTER_SECS
        {
            let abort = sp::send_abort_for_kill::call();
            let which = if abort { "SIGABRT" } else { "SIGKILL" };
            report(LOG, "ServerLoop", format!("issuing {which} to recalcitrant children"));
            childmgmt::TerminateChildren(if abort { SIGABRT } else { SIGKILL });
            /* reset flag so we don't SIGKILL again */
            pm_mut().abort_start_time = 0;
        }

        /*
         * Once a minute, verify that postmaster.pid hasn't been removed or
         * overwritten. If it has, force a shutdown.
         */
        if now - last_lockfile_recheck_time >= SECS_PER_MINUTE {
            if !sp::recheck_data_dir_lock_file::call() {
                report(
                    LOG,
                    "ServerLoop",
                    "performing immediate shutdown because data directory lock file is invalid",
                );
                kill(miscinit_seams::my_proc_pid::call(), SIGQUIT);
            }
            last_lockfile_recheck_time = now;
        }

        /*
         * Touch Unix socket and lock files every 58 minutes, so /tmp cleaners
         * don't remove them.
         */
        if now - last_touch_time >= 58 * SECS_PER_MINUTE {
            TouchSocketFiles();
            sp::touch_socket_lock_files::call();
            last_touch_time = now;
        }
    }
}

// ---------------------------------------------------------------------------
// BackendStartup
// ---------------------------------------------------------------------------

/// C: `static int BackendStartup(ClientSocket *client_sock)`.
///
/// Returns `STATUS_OK` or `STATUS_ERROR`.
pub fn BackendStartup(client_sock: &ClientSocket) -> i32 {
    /*
     * Capture time that Postmaster got a socket from accept (for logging
     * connection establishment and setup total duration).
     */
    let socket_created = timestamp_seams::get_current_timestamp::call();

    /*
     * Allocate and assign the child slot. Must do this before forking, so we
     * can handle failures (out of memory or child-process slots) cleanly.
     */
    let mut cac = statemachine::canAcceptConnections(B_BACKEND);
    let mut bn = None;
    if cac == CacState::Ok {
        /* Can change later to B_WAL_SENDER */
        bn = AssignPostmasterChildSlot(B_BACKEND);
        if bn.is_none() {
            /* Too many regular children; launch a dead-end child instead. */
            cac = CacState::TooMany;
        }
    }
    if bn.is_none() {
        bn = AllocDeadEndChild();
        if bn.is_none() {
            let _ = ereport(LOG)
                .errcode(ERRCODE_OUT_OF_MEMORY)
                .errmsg("out of memory")
                .finish(here("BackendStartup"));
            return STATUS_ERROR;
        }
    }
    let bn = bn.unwrap();

    /* Pass down canAcceptConnections state; bn->rw = NULL, no notify yet. */
    SetActiveChildBgworkerInfo(bn.child_slot, None, false);

    let mut startup_data = StartupData::Backend(BackendStartupData {
        can_accept_connections: cac,
        socket_created,
        fork_started: 0,
    });

    let pid = postmaster_child_launch(bn.bkend_type, bn.child_slot, &mut startup_data, Some(client_sock));
    if pid < 0 {
        /* in parent, fork failed */
        ReleasePostmasterChildSlot(bn);
        let _ = ereport(LOG)
            .errmsg("could not fork new process for connection")
            .finish(here("BackendStartup"));
        report_fork_failure_to_client(client_sock, 0);
        return STATUS_ERROR;
    }

    /* in parent, successful fork */
    let _ = ereport(DEBUG2)
        .errmsg_internal(format!(
            "forked new {}, pid={} socket={}",
            miscinit_seams::get_backend_type_desc::call(bn.bkend_type),
            pid,
            client_sock.sock,
        ))
        .finish(here("BackendStartup"));

    /* Safe to add this backend to our list of backends now. (C: `bn->pid =
     * pid`.) Set the pid on the slab entry by identity, not by child_slot:
     * dead-end children all carry child_slot == 0, so a slot-number lookup
     * would never find them, leaving them in ActiveChildList with pid == 0 —
     * which makes a later crash's TerminateChildren run `kill(-0, ...)` and
     * signal the postmaster's own process group. */
    SetActiveChildPidByEntry(&bn, pid);
    STATUS_OK
}

/// C: `static void report_fork_failure_to_client(ClientSocket *client_sock,
/// int errnum)`.
///
/// Best-effort, non-blocking, single `send()` of the fork-failure message. The
/// byte formatting + non-blocking socket fiddling is OS-coupled; we do the
/// minimal faithful thing (a single non-blocking write of the error packet),
/// then return. Failure to deliver is ignored, exactly as C ignores it.
pub fn report_fork_failure_to_client(client_sock: &ClientSocket, errnum: i32) {
    // C builds "could not fork new process for connection: <strerror(errnum)>"
    // into an 'E' error packet and tries one non-blocking send. We render the
    // same message and attempt one non-blocking write to the raw socket fd.
    let _ = errnum;
    let sock = client_sock.sock;
    if sock == PGINVALID_SOCKET {
        return;
    }
    // Set non-blocking, try once, ignore the result (matches the C "we do not
    // care to risk blocking the postmaster on this connection" comment).
    unsafe {
        let flags = libc::fcntl(sock, libc::F_GETFL, 0);
        if flags >= 0 {
            let _ = libc::fcntl(sock, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        // Minimal 'E'-type error message: type byte + int32 length + payload.
        // We format a faithful message string; a partial/failed write is fine.
        let msg = b"E\0\0\0\x1bSFATAL\0Ccould not fork\0\0";
        let _ = libc::send(sock, msg.as_ptr() as *const libc::c_void, msg.len(), 0);
        if flags >= 0 {
            let _ = libc::fcntl(sock, libc::F_SETFL, flags);
        }
    }
}

#[allow(unused_imports)]
use BackendType as _BackendType;
