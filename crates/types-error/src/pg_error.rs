//! Owned error value (`PgError`) and its result alias (`PgResult`).
//!
//! `PgError` is a plain, owned data struct describing a PostgreSQL error: its
//! level, SQLSTATE, message, and optional diagnostic fields. It lives in the
//! types stack so that crates which only need to *name* the error type in a
//! signature (notably seam crates) can do so without pulling in the
//! error-reporting subsystem.

use alloc::string::String;
use core::fmt;

use crate::{
    ErrorLevel, SqlState, ERRCODE_INTERNAL_ERROR, ERRCODE_SUCCESSFUL_COMPLETION, ERRCODE_WARNING,
    ERROR, NOTICE, WARNING,
};

pub type PgResult<T> = Result<T, PgError>;

/// Default SQLSTATE for a bare error level, mirroring elog.c's behavior when
/// no explicit `errcode()` is supplied.
pub fn default_sqlstate_for_level(level: ErrorLevel) -> SqlState {
    if level >= ERROR {
        ERRCODE_INTERNAL_ERROR
    } else if level >= WARNING {
        ERRCODE_WARNING
    } else {
        ERRCODE_SUCCESSFUL_COMPLETION
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorLocation {
    pub filename: Option<String>,
    pub lineno: i32,
    pub funcname: Option<String>,
}

impl ErrorLocation {
    pub fn new(filename: impl Into<String>, lineno: i32, funcname: impl Into<String>) -> Self {
        Self {
            filename: Some(filename.into()),
            lineno,
            funcname: Some(funcname.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgError {
    pub level: ErrorLevel,
    pub sqlstate: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    pub location: Option<ErrorLocation>,
}

impl PgError {
    pub fn new(level: ErrorLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            sqlstate: default_sqlstate_for_level(level),
            message: message.into(),
            detail: None,
            hint: None,
            context: None,
            location: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(ERROR, message)
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(WARNING, message)
    }

    pub fn notice(message: impl Into<String>) -> Self {
        Self::new(NOTICE, message)
    }

    pub fn with_sqlstate(mut self, sqlstate: SqlState) -> Self {
        self.sqlstate = sqlstate;
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for PgError {}
