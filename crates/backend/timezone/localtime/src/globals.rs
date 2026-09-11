//! `pg_tz *session_timezone` / `pg_tz *log_timezone` (pgtz.c globals). They
//! live here, below elog in the crate DAG, because elog's log_line_prefix
//! timestamps (`setup_formatted_log_time`) read `log_timezone` and pgtz sits
//! above elog. pgtz owns the setters' callers (pg_timezone_initialize, the
//! GUC assign hooks); everything else reads through pgtz's re-exports.
//!
//! Backend-private in C (every process inherits the postmaster's value at
//! fork and the GUC assign hooks re-run per backend), so thread-local here:
//! a backend thread's GUC bind re-fires the assign hooks on its own copy.

use core::cell::Cell;

use crate::PgTz;

thread_local! {
    static SESSION_TIMEZONE: Cell<Option<&'static PgTz>> = const { Cell::new(None) };
    static LOG_TIMEZONE: Cell<Option<&'static PgTz>> = const { Cell::new(None) };
}

#[inline]
pub fn session_timezone() -> Option<&'static PgTz> {
    SESSION_TIMEZONE.with(Cell::get)
}

pub fn set_session_timezone(tz: Option<&'static PgTz>) {
    SESSION_TIMEZONE.with(|c| c.set(tz));
}

#[inline]
pub fn log_timezone() -> Option<&'static PgTz> {
    LOG_TIMEZONE.with(Cell::get)
}

pub fn set_log_timezone(tz: Option<&'static PgTz>) {
    LOG_TIMEZONE.with(|c| c.set(tz));
}
