//! Port of `src/backend/archive/shell_archive.c` (PostgreSQL 18.3).
//!
//! The built-in archive module: it copies a completed WAL segment to its
//! destination by running the user-specified `archive_command` GUC through the
//! shell (`system(3)`). It is the default archive module — the archiver
//! (`pgarch.c`) selects it whenever `archive_library` is empty and
//! `archive_command` is set.
//!
//! The module is a callback table ([`ArchiveModuleCallbacks`]) handed back from
//! [`shell_archive_init`] (the C `shell_archive_init()`), exposed as the
//! `backend-archive-shell-archive-seams::shell_archive_init` seam and installed
//! by [`init_seams`]. The three callbacks are:
//!
//!   * `shell_archive_configured` — true iff `archive_command` is non-empty.
//!   * `shell_archive_file` — run `archive_command` to copy one segment.
//!   * `shell_archive_shutdown` — log that the archiver is exiting.
//!
//! There is no `startup_cb`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;

use backend_utils_error::ereport;
use mcx::MemoryContext;
use types_error::{ErrorLocation, PgResult};
use types_error::{DEBUG1, DEBUG3, FATAL, LOG};
use types_pgarch::{ArchiveModuleCallbacks, ArchiveModuleState};

use backend_access_transam_xlog_seams as xlog;
use backend_archive_shell_archive_seams as inward;
use backend_utils_activity_waitevent_seams as waitevent;
use common_wait_error_seams as wait_error;

/// `WAIT_EVENT_ARCHIVE_COMMAND` (pgstat wait-event id, `utils/wait_event.h`).
/// IPC class (`PG_WAIT_IPC` = `0x08000000`) entry #2 (`AppendReady`=0,
/// `ArchiveCleanupCommand`=1, `ArchiveCommand`=2).
const WAIT_EVENT_ARCHIVE_COMMAND: u32 = 0x0800_0002;

// ----- error-helper plumbing --------------------------------------------

/// Source location stamped onto raised/emitted reports, mirroring the C
/// `__FILE__`/line macro expansion. (Message text, SQLSTATE, and level are the
/// load-bearing parts; the exact line number is not.)
fn here() -> ErrorLocation {
    ErrorLocation::new("../src/backend/archive/shell_archive.c", 0, "")
}

/// Emit a non-throwing report (`ereport` at `< ERROR`).
fn emit(builder: backend_utils_error::ErrorBuilder) {
    let _ = builder.finish(here());
}

// ----- system(3) plumbing -----------------------------------------------

/// `system(cmd)` — run a shell command, returning the raw `wait(2)` status.
fn raw_system(cmd: &str) -> i32 {
    let bytes = cmd.as_bytes();
    if bytes.iter().any(|&b| b == 0) {
        // A NUL in the command can't form a C string; treat as shell failure.
        return -1;
    }
    let mut v: alloc::vec::Vec<core::ffi::c_char> = alloc::vec::Vec::with_capacity(bytes.len() + 1);
    for &b in bytes {
        v.push(b as core::ffi::c_char);
    }
    v.push(0);
    // SAFETY: `v` is a NUL-terminated C string valid for the call's duration.
    unsafe { libc::system(v.as_ptr()) }
}

/// `fflush(NULL)` — flush all stdio streams before forking a child.
fn raw_fflush_all() {
    // SAFETY: fflush(NULL) flushes all open streams; always safe to call.
    unsafe {
        libc::fflush(core::ptr::null_mut());
    }
}

// ----- the callback table ------------------------------------------------

/// `static const ArchiveModuleCallbacks shell_archive_callbacks`.
///
/// `startup_cb = NULL`; the other three are the functions below. The table is a
/// process-lifetime constant in C; here it is a `static`, matching the
/// `&'static` the `shell_archive_init` seam returns.
static SHELL_ARCHIVE_CALLBACKS: ArchiveModuleCallbacks = ArchiveModuleCallbacks {
    startup_cb: None,
    check_configured_cb: Some(shell_archive_configured),
    archive_file_cb: Some(shell_archive_file),
    shutdown_cb: Some(shell_archive_shutdown),
};

/// `const ArchiveModuleCallbacks *shell_archive_init(void)`.
pub fn shell_archive_init() -> &'static ArchiveModuleCallbacks {
    &SHELL_ARCHIVE_CALLBACKS
}

/// `static bool shell_archive_configured(ArchiveModuleState *state)`.
///
/// Archiving is configured iff `archive_command` is non-empty; otherwise attach
/// an errdetail (consumed by the archiver's "not configured" WARNING) and
/// return false.
fn shell_archive_configured(_state: &mut ArchiveModuleState) -> bool {
    // if (XLogArchiveCommand[0] != '\0') return true;
    if !xlog::xlog_archive_command::call().is_empty() {
        return true;
    }

    // arch_module_check_errdetail("\"%s\" is not set.", "archive_command");
    backend_postmaster_pgarch::set_arch_module_check_errdetail(format!(
        "\"{}\" is not set.",
        "archive_command"
    ));
    false
}

/// `static bool shell_archive_file(ArchiveModuleState *state, const char *file,
/// const char *path)`.
///
/// Build the command by substituting `%f`/`%p` into `archive_command`, run it
/// via `system(3)`, and classify the wait status. A signal death (or hard shell
/// error) aborts the archiver (`FATAL`); any other non-zero exit is logged at
/// `LOG` and reported as failure so the archiver retries the file.
fn shell_archive_file(
    _state: &mut ArchiveModuleState,
    file: &str,
    path: &str,
) -> PgResult<bool> {
    // Transient scratch context for the command string and native path, mirroring
    // the C `pstrdup` / `replace_percent_placeholders` allocations in
    // CurrentMemoryContext that are `pfree`d before returning. Dropping this
    // context at function exit reclaims them.
    let scratch = MemoryContext::new("shell_archive_file");
    let mcx = scratch.mcx();

    // char *nativePath = NULL;
    // if (path) { nativePath = pstrdup(path); make_native_path(nativePath); }
    //
    // The archiver always passes a path; the `if (path)` guard models a possibly
    // NULL C pointer (`&str` is never NULL here). make_native_path() is a no-op
    // on non-Windows targets (it only converts '/' to '\\' under WIN32), so the
    // native path is the path verbatim.
    let native_path: &str = path;

    // xlogarchcmd = replace_percent_placeholders(XLogArchiveCommand,
    //     "archive_command", "fp", file, nativePath);
    let xlog_archive_command = xlog::xlog_archive_command::call();
    let xlogarchcmd = common_percentrepl::replace_percent_placeholders(
        mcx,
        &xlog_archive_command,
        "archive_command",
        &[('f', Some(file)), ('p', Some(native_path))],
    )?;

    // ereport(DEBUG3, errmsg_internal("executing archive command \"%s\"", ...))
    emit(ereport(DEBUG3)
        .errmsg_internal(format!("executing archive command \"{xlogarchcmd}\"")));

    // fflush(NULL);
    raw_fflush_all();
    // pgstat_report_wait_start(WAIT_EVENT_ARCHIVE_COMMAND);
    waitevent::pgstat_report_wait_start::call(WAIT_EVENT_ARCHIVE_COMMAND);
    // rc = system(xlogarchcmd);
    let rc = raw_system(xlogarchcmd.as_str());
    // pgstat_report_wait_end();
    waitevent::pgstat_report_wait_end::call();

    if rc != 0 {
        // If either the shell itself, or a called command, died on a signal,
        // abort the archiver. Also die on a hard "command not found" error. If
        // we overreact it's no big deal; the postmaster restarts the archiver.
        let lev = if wait_error::wait_result_is_any_signal::call(rc, true) {
            FATAL
        } else {
            LOG
        };

        // WIFEXITED / WIFSIGNALED / else, each with the failed command as detail.
        let report = if libc::WIFEXITED(rc) {
            ereport(lev)
                .errmsg(format!(
                    "archive command failed with exit code {}",
                    libc::WEXITSTATUS(rc)
                ))
                .errdetail(format!("The failed archive command was: {xlogarchcmd}"))
        } else if libc::WIFSIGNALED(rc) {
            // Non-WIN32 branch (the WIN32 "terminated by exception" form does not
            // apply to this target).
            ereport(lev)
                .errmsg(format!(
                    "archive command was terminated by signal {}: {}",
                    libc::WTERMSIG(rc),
                    pg_strsignal(libc::WTERMSIG(rc))
                ))
                .errdetail(format!("The failed archive command was: {xlogarchcmd}"))
        } else {
            ereport(lev)
                .errmsg(format!(
                    "archive command exited with unrecognized status {rc}"
                ))
                .errdetail(format!("The failed archive command was: {xlogarchcmd}"))
        };

        // FATAL throws (propagated as Err, caught by the archiver); LOG just
        // emits. Either way the archive attempt failed: return false on the LOG
        // path. (pfree(xlogarchcmd) is the scratch context drop.)
        if lev == FATAL {
            report.finish(here())?;
            // Unreachable in practice: a FATAL report returns Err above.
        } else {
            emit(report);
        }

        return Ok(false);
    }

    // elog(DEBUG1, "archived write-ahead log file \"%s\"", file);
    emit(ereport(DEBUG1).errmsg_internal(format!("archived write-ahead log file \"{file}\"")));
    Ok(true)
}

/// `static void shell_archive_shutdown(ArchiveModuleState *state)`.
fn shell_archive_shutdown(_state: &mut ArchiveModuleState) {
    // elog(DEBUG1, "archiver process shutting down");
    emit(ereport(DEBUG1).errmsg_internal("archiver process shutting down"));
}

/// `pg_strsignal(int signum)` (`port/strsignal.c`) — human-readable signal name.
fn pg_strsignal(signum: i32) -> alloc::string::String {
    // SAFETY: strsignal returns a pointer to a (possibly static) NUL-terminated
    // string, or NULL for an unknown signal.
    unsafe {
        let ptr = libc::strsignal(signum);
        if ptr.is_null() {
            format!("unrecognized signal {signum}")
        } else {
            let cstr = core::ffi::CStr::from_ptr(ptr);
            cstr.to_string_lossy().into_owned()
        }
    }
}

/// Install this unit's inward seam(s).
pub fn init_seams() {
    inward::shell_archive_init::set(shell_archive_init);
}
