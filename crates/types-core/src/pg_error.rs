//! Owned error value (`PgError`) and its result alias (`PgResult`).
//!
//! `PgError` is the owned-data analogue of an `ereport`/`elog` longjmp: a
//! callee that would have thrown `ERROR` returns `Err(PgError)` instead, and
//! callers propagate it with `?`. Trimmed from the src-idiomatic `pg_error`
//! module to the items ports consume so far: the level scale, the message, and
//! the `new`/`error` constructors.

extern crate alloc;

use alloc::string::String;

/// `elog.h` error-level scale (the values matter: severity comparisons use
/// `>=`).
pub type ErrorLevel = i32;

pub const DEBUG5: ErrorLevel = 10;
pub const DEBUG1: ErrorLevel = 14;
pub const LOG: ErrorLevel = 15;
pub const INFO: ErrorLevel = 17;
pub const NOTICE: ErrorLevel = 18;
pub const WARNING: ErrorLevel = 19;
pub const ERROR: ErrorLevel = 21;
pub const FATAL: ErrorLevel = 22;
pub const PANIC: ErrorLevel = 23;

pub type PgResult<T> = Result<T, PgError>;

/// An in-flight PostgreSQL error (the owned `ErrorData`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgError {
    /// `int elevel` — error severity (`ERROR`, `FATAL`, ...).
    pub level: ErrorLevel,
    /// `char *message` — the primary error message.
    pub message: String,
}

impl PgError {
    pub fn new(level: ErrorLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
        }
    }

    /// `elog(ERROR, ...)` / `ereport(ERROR, ...)`.
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(ERROR, message)
    }

    pub fn level(&self) -> ErrorLevel {
        self.level
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl core::fmt::Display for PgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "error (level {}): {}", self.level, self.message)
    }
}
