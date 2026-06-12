//! syslogger.c's GUC parameters and exported globals.
//!
//! These are OWNED by this crate (not seamed), following the
//! `backend-utils-error::config` pattern: every static holds PostgreSQL's
//! boot-time GUC default and exposes a `pub` setter the owning unit (guc,
//! postmaster) calls when it lands. Per AGENTS.md "Backend-global state"
//! these are per-process values, so they live in `thread_local!`.
//!
//! C boot values (`guc_tables.c` / `syslogger.c` initializers):
//! `Logging_collector = false`, `Log_RotationAge = HOURS_PER_DAY *
//! MINS_PER_HOUR`, `Log_RotationSize = 10 * 1024`, `Log_directory = "log"`,
//! `Log_filename = "postgresql-%Y-%m-%d_%H%M%S.log"`,
//! `Log_truncate_on_rotation = false`, `Log_file_mode = 0600`,
//! `syslogPipe = {-1, -1}`.

use std::cell::{Cell, RefCell};

use crate::{DEFAULT_LOG_ROTATION_AGE, DEFAULT_LOG_ROTATION_SIZE};

thread_local! {
    static LOGGING_COLLECTOR: Cell<bool> = const { Cell::new(false) };
    static LOG_ROTATION_AGE: Cell<i32> = const { Cell::new(DEFAULT_LOG_ROTATION_AGE) };
    static LOG_ROTATION_SIZE: Cell<i32> = const { Cell::new(DEFAULT_LOG_ROTATION_SIZE) };
    static LOG_DIRECTORY: RefCell<String> = RefCell::new(String::from("log"));
    static LOG_FILENAME: RefCell<String> =
        RefCell::new(String::from("postgresql-%Y-%m-%d_%H%M%S.log"));
    static LOG_TRUNCATE_ON_ROTATION: Cell<bool> = const { Cell::new(false) };
    /// `Log_file_mode = S_IRUSR | S_IWUSR`.
    static LOG_FILE_MODE: Cell<i32> = const { Cell::new(0o600) };
    /// `int syslogPipe[2] = {-1, -1}` — exported; the postmaster closes the
    /// read end in `ClosePostmasterPorts` and backends write into [1].
    static SYSLOG_PIPE: Cell<[i32; 2]> = const { Cell::new([-1, -1]) };
}

pub fn logging_collector() -> bool {
    LOGGING_COLLECTOR.with(Cell::get)
}

pub fn set_logging_collector(value: bool) {
    LOGGING_COLLECTOR.with(|c| c.set(value));
}

pub fn log_rotation_age() -> i32 {
    LOG_ROTATION_AGE.with(Cell::get)
}

pub fn set_log_rotation_age(minutes: i32) {
    LOG_ROTATION_AGE.with(|c| c.set(minutes));
}

pub fn log_rotation_size() -> i32 {
    LOG_ROTATION_SIZE.with(Cell::get)
}

pub fn set_log_rotation_size(kilobytes: i32) {
    LOG_ROTATION_SIZE.with(|c| c.set(kilobytes));
}

pub fn log_directory() -> String {
    LOG_DIRECTORY.with(|c| c.borrow().clone())
}

pub fn set_log_directory(directory: String) {
    LOG_DIRECTORY.with(|c| *c.borrow_mut() = directory);
}

pub fn log_filename() -> String {
    LOG_FILENAME.with(|c| c.borrow().clone())
}

pub fn set_log_filename(filename: String) {
    LOG_FILENAME.with(|c| *c.borrow_mut() = filename);
}

pub fn log_truncate_on_rotation() -> bool {
    LOG_TRUNCATE_ON_ROTATION.with(Cell::get)
}

pub fn set_log_truncate_on_rotation(value: bool) {
    LOG_TRUNCATE_ON_ROTATION.with(|c| c.set(value));
}

pub fn log_file_mode() -> i32 {
    LOG_FILE_MODE.with(Cell::get)
}

pub fn set_log_file_mode(mode: i32) {
    LOG_FILE_MODE.with(|c| c.set(mode));
}

pub fn syslog_pipe() -> [i32; 2] {
    SYSLOG_PIPE.with(Cell::get)
}

pub fn set_syslog_pipe(pipe: [i32; 2]) {
    SYSLOG_PIPE.with(|c| c.set(pipe));
}
