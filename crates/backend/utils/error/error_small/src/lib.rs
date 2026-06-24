//! `utils/error/{assert,csvlog,jsonlog}.c` — the assertion-failure handler and
//! the structured (CSV / JSON) server-log line formatters.
//!
//! `write_csvlog` / `write_jsonlog` are the inward seams consumed by elog's
//! `send_message_to_server_log` dispatch (declared in
//! `backend-utils-error-small-seams`); this crate owns them and installs them
//! from [`init_seams`]. They read the per-report session context and the cached
//! formatted timestamps from the elog crate (`backend-utils-error`), which only
//! depends on the *seams* crate, so there is no cycle.
//!
//! `ExceptionalCondition` (assert.c) is the `Assert()` failure handler. It
//! intentionally does not route through elog (minimal infrastructure), writing
//! to stderr and `abort()`-ing.

use std::io::Write;

use ::utils_error::{backend_log_context, config, reset_formatted_start_time};
use ::types_error::PGErrorVerbosity;

mod csvlog;
mod jsonlog;

pub use csvlog::write_csvlog;
pub use jsonlog::write_jsonlog;

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    error_small_seams::write_csvlog::set(write_csvlog);
    error_small_seams::write_jsonlog::set(write_jsonlog);
}

// ===========================================================================
// assert.c
// ===========================================================================

/// `ExceptionalCondition(conditionName, fileName, lineNumber)` — handle the
/// failure of an `Assert()`.
///
/// We intentionally do not go through elog() here, on the grounds of wanting to
/// minimize the amount of infrastructure that has to be working to report an
/// assertion failure. The C version dumps a backtrace when
/// `HAVE_BACKTRACE_SYMBOLS`; here the abort handler / OS produces that, so we
/// mirror the stderr message and `abort()`.
pub fn exceptional_condition(condition_name: &str, file_name: &str, line_number: i32) -> ! {
    // Report the failure on stderr. (`PointerIsValid` cannot fail for &str.)
    let msg = format!(
        "TRAP: failed Assert(\"{condition_name}\"), File: \"{file_name}\", Line: {line_number}, PID: {}\n",
        assert_pid()
    );
    #[cfg(not(target_family = "wasm"))]
    {
        let mut stderr = std::io::stderr();
        let _ = stderr.write_all(msg.as_bytes());
        // Usually this shouldn't be needed, but make sure the msg went out.
        let _ = stderr.flush();
        std::process::abort();
    }
    // wasm64: std stderr is a no-op and process::abort traps opaquely; route the
    // message to the host stderr, then proc_exit with a nonzero code so the
    // assertion text is actually visible.
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::stderr_write(msg.as_bytes());
        wasm_libc_shim::proc_exit(134) // 128 + SIGABRT
    }
}

/// `getpid()` for the Assert message (`std::process::id()` panics on wasm64).
fn assert_pid() -> u32 {
    #[cfg(not(target_family = "wasm"))]
    {
        std::process::id()
    }
    #[cfg(target_family = "wasm")]
    {
        // SAFETY: getpid is a const-returning shim (no preconditions).
        unsafe { wasm_libc_shim::getpid() as u32 }
    }
}

// ===========================================================================
// Shared session-context helpers (mirror the C globals read by both writers).
// ===========================================================================

/// `MyProcPid` (0 only before the process id is known; the provider defaults to
/// `getpid()`, never 0, so this matches the C `MyProcPid != 0` test staying
/// true once a backend is running).
fn my_proc_pid() -> u32 {
    backend_log_context().map_or_else(assert_pid, |c| c.process_id())
}

/// `MyStartTime` (seconds since the Unix epoch).
fn my_start_time() -> i64 {
    backend_log_context().map_or(0, |c| c.session_start_time())
}

/// Per-process line counter shared in C as two file-static `write_csvlog` /
/// `write_jsonlog` locals. Each writer keeps its own counter (matching the two
/// separate C statics); both reset when `MyProcPid` changes.
#[derive(Default)]
struct LogLineCounter {
    log_line_number: i64,
    log_my_pid: u32,
}

impl LogLineCounter {
    /// Advance the counter for this report, resetting on a pid change and
    /// re-priming the cached start-time string, exactly as the C prologue does.
    fn next(&mut self) -> i64 {
        let pid = my_proc_pid();
        if self.log_my_pid != pid {
            self.log_line_number = 0;
            self.log_my_pid = pid;
            reset_formatted_start_time();
        }
        self.log_line_number += 1;
        self.log_line_number
    }
}

/// `Log_error_verbosity >= PGERROR_VERBOSE`.
fn verbose_location() -> bool {
    config::log_error_verbosity() >= PGErrorVerbosity::Verbose
}
