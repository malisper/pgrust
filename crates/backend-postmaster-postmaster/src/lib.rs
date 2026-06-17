//! Port of `src/backend/postmaster/postmaster.c`: the multi-process server
//! supervisor (the postmaster).
//!
//! The postmaster is the master process that supervises a running PostgreSQL
//! server: it listens for connections, forks a backend per connection, launches
//! and supervises the auxiliary daemons (startup, checkpointer, bgwriter,
//! walwriter, autovacuum launcher, archiver, WAL summarizer, IO workers,
//! background workers, syslogger), reaps dead children, and sequences the whole
//! cluster through startup, shutdown, and crash recovery via a state machine
//! (`pmState`).
//!
//! # Module map (1:1 with postmaster.c)
//!
//!   * [`core`] — the single-process [`core::PostmasterState`], the
//!     `BackendTypeMask` bitmask, the `PMState`/`StartupStatusEnum` enums, the
//!     shutdown-mode constants, the wait-status decode helpers.
//!   * [`main_entry`] — [`main_entry::PostmasterMain`], `ClosePostmasterPorts`,
//!     `InitPostmasterDeathWatchHandle`, the listen-socket establishment.
//!   * [`serverloop`] — [`serverloop::ServerLoop`], `DetermineSleepTime`,
//!     `ConfigurePostmasterWaitSet`, `BackendStartup`,
//!     `report_fork_failure_to_client`.
//!   * [`statemachine`] — `PostmasterStateMachine`, `HandleFatalError`,
//!     `HandleChildCrash`, `UpdatePMState`, `canAcceptConnections`,
//!     `ExitPostmaster`.
//!   * [`reaper`] — `process_pm_child_exit` (the SIGCHLD reaper, with
//!     `libc::waitpid`), `CleanupBackend`, `LogChildExit`.
//!   * [`signals`] — the async handlers + the deferred `process_pm_*` processors.
//!   * [`childmgmt`] — `signal_child` / `SignalChildren` / `TerminateChildren` /
//!     `CountChildren`.
//!   * [`startchildren`] — `LaunchMissingBackgroundProcesses`,
//!     `StartChildProcess`, `StartSysLogger`, `StartAutovacuumWorker`.
//!   * [`bgworkers`] — `maybe_start_bgworkers`, `bgworker_should_start_now`,
//!     `StartBackgroundWorker`, `PostmasterMarkPIDForWorkerNotify`.
//!   * [`ioworkers`] — `maybe_reap_io_worker`, `maybe_adjust_io_workers`.
//!   * [`helpers`] / [`latchutil`] — the `ereport`/`pm_signame` wrappers, the
//!     direct libc syscall chokepoints (waitpid/kill/time/closesocket), and the
//!     postmaster-latch set/reset.
//!
//! # Single-process state, real `fork()`
//!
//! The postmaster is strictly single-threaded (it forks rather than threads),
//! so the C file statics are modeled by one owned [`core::PostmasterState`]
//! reached through `pm`/`pm_mut`. Children are created by the real
//! `fork_process()` through `postmaster_child_launch` (the child path —
//! `ClosePostmasterPorts` -> `InitProcess` -> `PostgresMain` — is fully wired
//! by the launch-backend unit).
//!
//! # EXEC_BACKEND (sanctioned exception)
//!
//! pgrust targets the `fork()` model. The C `#ifdef EXEC_BACKEND` blocks (the
//! Windows / non-`fork` variant: `write_nondefault_variables`,
//! `RemovePgTempFilesInDir`, `find_other_exec`, `win32ChildQueue`,
//! `pgwin32_*`, `internal_forkexec`/`SubPostmasterMain`) are intentionally NOT
//! ported; the fork-side code path is ported 100%. See [`main_entry`].
//!
//! # Seams
//!
//! The OS/GUC/config substrate the spine reaches is either a real installed
//! seam (the same surface single-user mode drives — config files, shmem,
//! sockets, the pmchild slab) or a caller-side seam fronting an as-yet-unported
//! owner (`load_hba`, `autovac_init`, the control file, the bgworker carrier
//! accessors, ...), declared in `backend-postmaster-postmaster-seams`. The
//! postmaster-owned seams that other units consume (`postmaster_main`,
//! `close_postmaster_ports`, `signal_child_*`, the death-watch fds, ...) are
//! installed by [`init_seams`].

#![allow(non_snake_case)]

#[macro_use]
pub mod core;
pub mod bgworkers;
pub mod childmgmt;
pub mod fileops;
pub mod gucreads;
pub mod helpers;
pub mod ioworkers;
pub mod latchutil;
pub mod main_entry;
pub mod reaper;
pub mod serverloop;
pub mod signals;
pub mod startchildren;
pub mod statemachine;

pub use bgworkers::PostmasterMarkPIDForWorkerNotify;
pub use main_entry::PostmasterMain;
pub use serverloop::ServerLoop;

use crate::core::{SIGTERM, SIGUSR1};
use backend_postmaster_postmaster_seams as sp;

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seam implementations — the postmaster-owned operations
/// that the rest of the tree consumes.
pub fn init_seams() {
    // The postmaster's main entry, reached from main() for DISPATCH_POSTMASTER.
    sp::postmaster_main::set(PostmasterMain);

    // Child-side teardown of postmaster-only resources (called in every forked
    // child by launch-backend).
    sp::close_postmaster_ports::set(main_entry::ClosePostmasterPorts);

    // PostmasterMarkPIDForWorkerNotify (bgworker.c consumer).
    sp::postmaster_mark_pid_for_worker_notify::set(PostmasterMarkPIDForWorkerNotify);

    // kill(pid, SIG) from the postmaster to a tracked child (bgworker.c).
    sp::signal_child_sigusr1::set(|pid| {
        helpers::kill(pid, SIGUSR1);
    });
    sp::signal_child_sigterm::set(|pid| {
        helpers::kill(pid, SIGTERM);
    });

    // Postmaster-owned signal-handler + process-local-latch setup (the
    // PostmasterMain pqsignal block + InitProcessLocalLatch).
    sp::install_postmaster_signal_handlers::set(main_entry::install_postmaster_signal_handlers);
    sp::init_process_local_latch::set(main_entry::init_process_local_latch);

    // Postmaster death-watch pipe (read by pmsignal / waiteventset).
    sp::read_postmaster_death_watch::set(main_entry::read_postmaster_death_watch);
    sp::postmaster_death_watch_fd::set(main_entry::postmaster_death_watch_fd);

    // `fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFD, FD_CLOEXEC)`
    // (miscinit InitPostmasterChild). The death-watch fd is postmaster-owned, so
    // the cloexec set runs here.
    backend_storage_ipc_pmsignal_seams::set_postmaster_death_watch_cloexec::set(
        main_entry::set_postmaster_death_watch_cloexec,
    );

    // Request a signal on parent (postmaster) death.
    sp::request_parent_death_signal::set(request_parent_death_signal);

    // pmsignal operations expressed from the postmaster's perspective, but
    // implemented by the pmsignal shared-state unit.
    sp::mark_postmaster_child_wal_sender::set(|| {
        backend_storage_ipc_pmsignal::MarkPostmasterChildWalSender();
    });
    sp::send_postmaster_signal_advance_state_machine::set(|| {
        backend_storage_ipc_pmsignal::SendPostmasterSignal(
            backend_storage_ipc_pmsignal::PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE,
        );
    });

    // Postmaster-owned GUC value reads (`*conf->variable` of the GUC globals
    // declared in postmaster.c, read straight from the guc_tables variable
    // slots). The listen-socket loop + the SSL / syslogger / crash-restart
    // launch decisions read these.
    sp::enable_ssl::set(gucreads::enable_ssl);
    sp::logging_collector::set(gucreads::logging_collector);
    sp::restart_after_crash::set(gucreads::restart_after_crash);
    sp::remove_temp_files_after_crash::set(gucreads::remove_temp_files_after_crash);
    sp::send_abort_for_crash::set(gucreads::send_abort_for_crash);
    sp::send_abort_for_kill::set(gucreads::send_abort_for_kill);
    sp::log_hostname::set(gucreads::log_hostname);
    sp::summarize_wal::set(gucreads::summarize_wal);
    sp::enable_hot_standby::set(gucreads::enable_hot_standby);
    sp::sync_replication_slots::set(gucreads::sync_replication_slots);
    sp::post_port_number::set(gucreads::post_port_number);
    sp::max_connections::set(gucreads::max_connections);
    sp::authentication_timeout::set(gucreads::authentication_timeout);
    sp::pre_auth_delay::set(gucreads::pre_auth_delay);
    sp::io_workers::set(gucreads::io_workers);
    sp::listen_addresses::set(gucreads::listen_addresses);
    sp::unix_socket_directories::set(gucreads::unix_socket_directories);

    // `ClientAuthInProgress` (postmaster.c global) — the backend-local flag set
    // around the client_authentication() exchange in PerformAuthentication
    // (postinit.c) and read by error reporting to limit log visibility during
    // auth. postmaster.c declares the global, so its read/write live here. Each
    // process owns its own `PostmasterState` (a forked backend lazily inits its
    // copy), mirroring the C file-static that the backend inherits a copy of.
    sp::client_auth_in_progress::set(|| core::pm().client_auth_in_progress);
    sp::set_client_auth_in_progress::set(|value| {
        core::pm_mut().client_auth_in_progress = value;
    });

    // Postmaster-owned file writes from PostmasterMain (postmaster.c bodies).
    sp::create_opts_file::set(fileops::create_opts_file);
    sp::maybe_write_external_pid_file::set(fileops::maybe_write_external_pid_file);

    // `MemoryContextDelete(PostmasterContext); PostmasterContext = NULL`
    // (auxprocess.c / bgworker.c): a freshly-forked child releases the
    // postmaster's working context. postmaster.c owns `PostmasterContext`, so
    // its lifecycle is installed here; the MemoryContext substrate (anchored
    // on the per-process TopMemoryContext root) lives with the mmgr owner
    // (portalmem `top_context`), which the postmaster created the context in
    // via `create_postmaster_context` at PostmasterMain entry.
    sp::delete_postmaster_context::set(
        backend_utils_mmgr_portalmem::top_context::delete_postmaster_context,
    );

    // `pg_strsignal(signum)` (port/strsignal.c) — the reaper's LogChildExit turns
    // a child's terminating signal into a human-readable name. C wraps libc
    // `strsignal()`, falling back to "unrecognized signal N" on NULL.
    sp::pg_strsignal::set(pg_strsignal);
}

/// `pg_strsignal(int signum)` (port/strsignal.c) — human-readable signal name.
fn pg_strsignal(signum: i32) -> String {
    // SAFETY: strsignal returns a pointer to a (possibly static) NUL-terminated
    // string, or NULL for an unknown signal.
    unsafe {
        let ptr = libc::strsignal(signum);
        if ptr.is_null() {
            format!("unrecognized signal {signum}")
        } else {
            std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// C: `PostmasterDeathSignalInit` core — `prctl(PR_SET_PDEATHSIG, signum)`
/// (Linux) / `procctl(PROC_PDEATHSIG_CTL, &signum)` (FreeBSD). On platforms
/// without a parent-death signal (macOS), this is a no-op success: children
/// still detect postmaster death by reading EOF on the death-watch pipe.
fn request_parent_death_signal(_signum: i32) -> types_error::PgResult<()> {
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = libc::prctl(libc::PR_SET_PDEATHSIG, _signum as libc::c_ulong, 0, 0, 0);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
