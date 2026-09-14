//! C: src/common/logging.c — the frontend logging conventions pg_combinebackup
//! uses, restricted to what this tool needs. Output shape is pinned:
//! `pg_combinebackup: error: <msg>`, `pg_combinebackup: warning: <msg>`,
//! `pg_combinebackup: hint: <msg>`, and (for info/debug) `pg_combinebackup: <msg>`.
//! Debug messages print only after -d / --debug (pg_logging_increase_verbosity).
//!
//! `pg_fatal(...)` = pg_log_error + exit(1). Like C's atexit hook, exiting
//! through [`exit_program`] first removes any output directories registered
//! for cleanup (main resets the list on success before exiting 0).

use std::sync::atomic::{AtomicBool, Ordering};

pub const PROGNAME: &str = "pg_combinebackup";

static DEBUG: AtomicBool = AtomicBool::new(false);

pub fn increase_verbosity() {
    DEBUG.store(true, Ordering::Relaxed);
}

pub fn debug_enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

pub fn log_error(msg: &str) {
    eprintln!("{PROGNAME}: error: {msg}");
}

pub fn log_error_hint(msg: &str) {
    eprintln!("{PROGNAME}: hint: {msg}");
}

pub fn log_warning(msg: &str) {
    eprintln!("{PROGNAME}: warning: {msg}");
}

pub fn log_warning_hint(msg: &str) {
    eprintln!("{PROGNAME}: hint: {msg}");
}

pub fn log_info(msg: &str) {
    eprintln!("{PROGNAME}: {msg}");
}

pub fn log_debug(msg: &str) {
    if debug_enabled() {
        eprintln!("{PROGNAME}: {msg}");
    }
}

/// C: %m — strerror(errno) for a std::io::Error, without Rust's
/// " (os error N)" suffix.
pub fn errno_message(e: &std::io::Error) -> String {
    match e.raw_os_error() {
        Some(errnum) => {
            // SAFETY: strerror returns a NUL-terminated static string.
            let s = unsafe { std::ffi::CStr::from_ptr(libc::strerror(errnum)) };
            s.to_string_lossy().into_owned()
        }
        None => e.to_string(),
    }
}

/*
 * Directory-cleanup registry (C: cleanup_dir_list + cleanup_directories_atexit).
 */
struct CleanupDir {
    target_path: std::path::PathBuf,
    rmtopdir: bool,
}

/* The tool is single-threaded, like the C original; a thread_local is the
 * static list without dragging in a lock. */
thread_local! {
    static CLEANUP_DIRS: std::cell::RefCell<Vec<CleanupDir>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// C: remember_to_cleanup_directory.
pub fn remember_to_cleanup_directory(target_path: &std::path::Path, rmtopdir: bool) {
    CLEANUP_DIRS.with(|d| {
        d.borrow_mut()
            .push(CleanupDir { target_path: target_path.to_path_buf(), rmtopdir })
    });
}

/// C: reset_directory_cleanup_list — called on success so nothing is removed.
pub fn reset_directory_cleanup_list() {
    CLEANUP_DIRS.with(|d| d.borrow_mut().clear());
}

/// C: cleanup_directories_atexit. LIFO like the C list (it prepends).
fn cleanup_directories() {
    let dirs: Vec<CleanupDir> = CLEANUP_DIRS.with(|d| std::mem::take(&mut *d.borrow_mut()));
    let mut dirs = dirs;
    while let Some(dir) = dirs.pop() {
        if dir.rmtopdir {
            log_info(&format!(
                "removing output directory \"{}\"",
                dir.target_path.display()
            ));
            if std::fs::remove_dir_all(&dir.target_path).is_err() {
                log_error("failed to remove output directory");
            }
        } else {
            log_info(&format!(
                "removing contents of output directory \"{}\"",
                dir.target_path.display()
            ));
            if !remove_dir_contents(&dir.target_path) {
                log_error("failed to remove contents of output directory");
            }
        }
    }
}

/// C: rmtree(path, rmtopdir=false).
fn remove_dir_contents(path: &std::path::Path) -> bool {
    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        Err(_) => return false,
    };
    let mut ok = true;
    for entry in entries.flatten() {
        let p = entry.path();
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        let r = if is_dir { std::fs::remove_dir_all(&p) } else { std::fs::remove_file(&p) };
        if r.is_err() {
            ok = false;
        }
    }
    ok
}

/// Exit the program, running the C-atexit-equivalent cleanup first.
pub fn exit_program(code: i32) -> ! {
    cleanup_directories();
    std::process::exit(code);
}

/// C: pg_fatal — log the error and exit(1) (via the atexit-equivalent path).
macro_rules! pg_fatal {
    ($($arg:tt)*) => {{
        $crate::app::flog::log_error(&format!($($arg)*));
        $crate::app::flog::exit_program(1)
    }};
}
pub(crate) use pg_fatal;
