//! Owned error value (`PgError`) and its result alias (`PgResult`).
//!
//! `PgError` is a plain, owned data struct describing a PostgreSQL error: its
//! level, SQLSTATE, message, and the optional diagnostic fields of
//! `ErrorData` (elog.h). `Err(PgError)` is the owned-model carrier for a C
//! `ereport(ERROR, ...)` / `elog(ERROR, ...)` longjmp.

use alloc::string::String;
use core::fmt;

use crate::{
    ErrorLevel, SqlState, ERRCODE_INTERNAL_ERROR, ERRCODE_SUCCESSFUL_COMPLETION, ERRCODE_WARNING,
    ERROR, NOTICE, WARNING,
};

pub type PgResult<T> = Result<T, PgError>;

/// Default SQLSTATE for a bare error level, mirroring elog.c's behavior when no
/// explicit `errcode()` is supplied.
pub fn default_sqlstate_for_level(level: ErrorLevel) -> SqlState {
    if level >= ERROR {
        ERRCODE_INTERNAL_ERROR
    } else if level >= WARNING {
        ERRCODE_WARNING
    } else {
        ERRCODE_SUCCESSFUL_COMPLETION
    }
}

/// The reporting location (`__FILE__` / `__LINE__` / `__func__`) carried by an
/// `ErrorData`.
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

/// Owned mirror of elog.h's `ErrorData` diagnostic fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgError {
    pub level: ErrorLevel,
    pub sqlstate: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub detail_log: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    pub backtrace: Option<String>,
    pub message_id: Option<String>,
    pub domain: Option<String>,
    pub context_domain: Option<String>,
    pub hide_statement: bool,
    pub hide_context: bool,
    pub location: Option<ErrorLocation>,
    pub saved_errno: Option<i32>,
    pub cursor_position: Option<i32>,
    pub internal_position: Option<i32>,
    pub internal_query: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub datatype_name: Option<String>,
    pub constraint_name: Option<String>,
}

impl PgError {
    pub fn new(level: ErrorLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            sqlstate: default_sqlstate_for_level(level),
            message: message.into(),
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
        }
    }

    /// `elog(ERROR, message)` (default SQLSTATE `XX000`).
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(ERROR, message)
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(WARNING, message)
    }

    pub fn notice(message: impl Into<String>) -> Self {
        Self::new(NOTICE, message)
    }

    /// `errcode(sqlstate)`.
    pub fn with_sqlstate(mut self, sqlstate: SqlState) -> Self {
        self.sqlstate = sqlstate;
        self
    }

    pub fn with_location(mut self, location: ErrorLocation) -> Self {
        self.location = Some(location);
        self
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for PgError {}
