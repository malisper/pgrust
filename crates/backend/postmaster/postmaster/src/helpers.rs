//! Small shared helpers used across the postmaster modules: `ereport`/`elog`
//! wrappers stamped with the `postmaster.c` source location, the signal-name
//! renderer, and a few direct libc-syscall chokepoints (waitpid/kill/time/
//! closesocket) that the postmaster owns — exactly parallel to fork-process
//! calling `libc::fork` directly.

#![allow(dead_code)]

use ::utils_error::ereport;
use types_error::{ErrorLevel, ErrorLocation, DEBUG1};

use crate::core::{
    SIGABRT, SIGCHLD, SIGHUP, SIGINT, SIGKILL, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2,
};

/// Build an [`ErrorLocation`] for an `ereport` originating in this file.
#[inline]
pub fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("postmaster.c", 0, funcname)
}

/// Emit `ereport(level, (errmsg(msg)))`. Below-ERROR reports never raise, so
/// the result is discarded.
#[inline]
pub fn report(level: ErrorLevel, funcname: &'static str, msg: impl Into<String>) {
    let _ = ereport(level).errmsg(msg).finish(here(funcname));
}

/// Emit `ereport(level, (errmsg_internal(msg)))`.
#[inline]
pub fn report_internal(level: ErrorLevel, funcname: &'static str, msg: impl Into<String>) {
    let _ = ereport(level).errmsg_internal(msg).finish(here(funcname));
}

/// C: `elog(DEBUG1, ...)` for the pmState-transition trace.
#[inline]
pub fn elog_debug1(funcname: &'static str, msg: impl Into<String>) {
    let _ = ereport(DEBUG1).errmsg_internal(msg).finish(here(funcname));
}

/// Return string representation of a signal.
///
/// C: `static const char *pm_signame(int signal)`.
pub fn pm_signame(signal: i32) -> &'static str {
    // C: #define PM_TOSTR_CASE(sym) case sym: return #sym
    match signal {
        x if x == SIGABRT => "SIGABRT",
        x if x == SIGCHLD => "SIGCHLD",
        x if x == SIGHUP => "SIGHUP",
        x if x == SIGINT => "SIGINT",
        x if x == SIGKILL => "SIGKILL",
        x if x == SIGQUIT => "SIGQUIT",
        x if x == SIGTERM => "SIGTERM",
        x if x == SIGUSR1 => "SIGUSR1",
        x if x == SIGUSR2 => "SIGUSR2",
        _ => {
            debug_assert!(false, "pm_signame: unrecognized signal {signal}");
            "(unknown)"
        }
    }
}

// ---------------------------------------------------------------------------
// Direct libc syscall chokepoints owned by the postmaster.
//
// The postmaster reaches these raw syscalls directly (the `static pid_t
// waitpid` wrapper in C is WIN32-only; on Unix it is the libc syscall). This is
// the blessed syscall-chokepoint pattern — exactly how `fork_process` calls
// `libc::fork`.
// ---------------------------------------------------------------------------

/// `kill(pid, sig)` — returns the raw rc (`< 0` on failure).
#[inline]
pub fn kill(pid: i32, sig: i32) -> i32 {
    unsafe { libc::kill(pid as libc::pid_t, sig) }
}

/// `time(NULL)` — current wall-clock time in seconds.
#[inline]
pub fn time_now() -> i64 {
    unsafe { libc::time(core::ptr::null_mut()) as i64 }
}

/// `closesocket(fd)` (`close(fd)` on Unix) — returns the raw rc.
#[inline]
pub fn closesocket(fd: i32) -> i32 {
    unsafe { libc::close(fd) }
}

/// A reaped child: `(pid, exitstatus)` from `waitpid(-1, &status, WNOHANG)`.
pub struct Reaped {
    pub pid: i32,
    pub exitstatus: i32,
}

/// `waitpid(-1, &exitstatus, WNOHANG)` — reap one dead child without blocking.
///
/// Returns `Some(Reaped)` for a reaped child (`pid > 0`), or `None` when no
/// child is ready (`pid <= 0`), matching the C reaper's `while ((pid =
/// waitpid(-1, &exitstatus, WNOHANG)) > 0)` loop condition.
#[inline]
pub fn waitpid_nohang() -> Option<Reaped> {
    let mut status: libc::c_int = 0;
    let pid = unsafe { libc::waitpid(-1, &mut status as *mut libc::c_int, libc::WNOHANG) };
    if pid > 0 {
        Some(Reaped {
            pid: pid as i32,
            exitstatus: status as i32,
        })
    } else {
        None
    }
}
