//! Port of PostgreSQL's `src/common/wait_error.c` — interpret a
//! `wait(2)`/`system()` raw exit status. Owns and installs the
//! `wait_error_seams` inward seams.
//!
//! The `exit_status` arguments are the RAW wait-status as returned by
//! `libc::system()` / `waitpid` (the same value C passes to these macros), so
//! the `WIFEXITED`/`WEXITSTATUS`/`WIFSIGNALED`/`WTERMSIG` POSIX macros apply
//! directly via the `libc` crate.

/// `wait_result_is_signal(exit_status, signum)` (wait_error.c) — true if the
/// child terminated due to the given signal (or exited 128+signum, the shell's
/// signal-exit convention).
pub fn wait_result_is_signal(exit_status: i32, signum: i32) -> bool {
    if libc::WIFSIGNALED(exit_status) && libc::WTERMSIG(exit_status) == signum {
        return true;
    }
    if libc::WIFEXITED(exit_status) && libc::WEXITSTATUS(exit_status) == 128 + signum {
        return true;
    }
    false
}

/// `wait_result_is_any_signal(exit_status, include_command_not_found)`
/// (wait_error.c) — true if the child terminated due to any signal; a shell exit
/// code above 128 (or above 125 when `include_command_not_found`, covering
/// 126/127 "not executable"/"not found") also counts.
pub fn wait_result_is_any_signal(exit_status: i32, include_command_not_found: bool) -> bool {
    if libc::WIFSIGNALED(exit_status) {
        return true;
    }
    let threshold = if include_command_not_found { 125 } else { 128 };
    if libc::WIFEXITED(exit_status) && libc::WEXITSTATUS(exit_status) > threshold {
        return true;
    }
    false
}

/// `wait_result_to_str(exit_status)` (wait_error.c) — render a child process's
/// exit status as a human-readable string (the `pstrdup`'d-result analog).
pub fn wait_result_to_str(exit_status: i32) -> alloc::string::String {
    use alloc::format;
    if exit_status == -1 {
        // C uses "%m" (the saved errno text). We don't carry errno here; the
        // -1 case is a libc::system failure to fork the shell.
        return alloc::string::String::from("could not execute command");
    }
    if libc::WIFEXITED(exit_status) {
        match libc::WEXITSTATUS(exit_status) {
            126 => alloc::string::String::from("command not executable"),
            127 => alloc::string::String::from("command not found"),
            code => format!("child process exited with exit code {code}"),
        }
    } else if libc::WIFSIGNALED(exit_status) {
        let sig = libc::WTERMSIG(exit_status);
        format!(
            "child process was terminated by signal {sig}: {}",
            pg_strsignal(sig)
        )
    } else {
        format!("child process exited with unrecognized status {exit_status}")
    }
}

/// `pg_strsignal(signum)` (`port/strsignal.c`) — human-readable signal name.
fn pg_strsignal(signum: i32) -> alloc::string::String {
    // SAFETY: libc::strsignal returns a pointer to a static (or thread-local)
    // C string for a valid signal number; we copy it out immediately.
    let ptr = unsafe { libc::strsignal(signum) };
    if ptr.is_null() {
        return format!("unrecognized signal {signum}");
    }
    let cstr = unsafe { core::ffi::CStr::from_ptr(ptr) };
    cstr.to_string_lossy().into_owned()
}

/// Install the inward seams owned by `common/wait_error.c`.
pub fn init_seams() {
    wait_error_seams::wait_result_is_signal::set(wait_result_is_signal);
    wait_error_seams::wait_result_is_any_signal::set(wait_result_is_any_signal);
    wait_error_seams::wait_result_to_str::set(wait_result_to_str);
}

extern crate alloc;

use alloc::format;
