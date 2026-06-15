use std::fmt;

use pgrust_pg_ffi::{
    ErrorField, ErrorLevel, SqlState, ERROR, PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME,
    PG_DIAG_DATATYPE_NAME, PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME,
};

use crate::{default_sqlstate_for_level, PgResult};

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
    pub(crate) level: ErrorLevel,
    pub(crate) sqlstate: SqlState,
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) detail_log: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) context: Option<String>,
    pub(crate) backtrace: Option<String>,
    pub(crate) message_id: Option<String>,
    pub(crate) domain: Option<String>,
    pub(crate) context_domain: Option<String>,
    pub(crate) hide_statement: bool,
    pub(crate) hide_context: bool,
    pub(crate) location: Option<ErrorLocation>,
    pub(crate) saved_errno: Option<i32>,
    pub(crate) cursor_position: Option<i32>,
    pub(crate) internal_position: Option<i32>,
    pub(crate) internal_query: Option<String>,
    pub(crate) schema_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) column_name: Option<String>,
    pub(crate) datatype_name: Option<String>,
    pub(crate) constraint_name: Option<String>,
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

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(ERROR, message)
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(pgrust_pg_ffi::WARNING, message)
    }

    pub fn notice(message: impl Into<String>) -> Self {
        Self::new(pgrust_pg_ffi::NOTICE, message)
    }

    pub fn level(&self) -> ErrorLevel {
        self.level
    }

    pub fn sqlstate(&self) -> SqlState {
        self.sqlstate
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub fn detail_log(&self) -> Option<&str> {
        self.detail_log.as_deref()
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }

    pub fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_deref()
    }

    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    pub fn context_domain(&self) -> Option<&str> {
        self.context_domain.as_deref()
    }

    pub fn hide_statement(&self) -> bool {
        self.hide_statement
    }

    pub fn hide_context(&self) -> bool {
        self.hide_context
    }

    pub fn location(&self) -> Option<&ErrorLocation> {
        self.location.as_ref()
    }

    pub fn saved_errno(&self) -> Option<i32> {
        self.saved_errno
    }

    pub fn cursor_position(&self) -> Option<i32> {
        self.cursor_position
    }

    pub fn internal_position(&self) -> Option<i32> {
        self.internal_position
    }

    pub fn internal_query(&self) -> Option<&str> {
        self.internal_query.as_deref()
    }

    pub fn schema_name(&self) -> Option<&str> {
        self.schema_name.as_deref()
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    pub fn column_name(&self) -> Option<&str> {
        self.column_name.as_deref()
    }

    pub fn datatype_name(&self) -> Option<&str> {
        self.datatype_name.as_deref()
    }

    pub fn constraint_name(&self) -> Option<&str> {
        self.constraint_name.as_deref()
    }

    pub fn with_sqlstate(mut self, sqlstate: SqlState) -> Self {
        self.sqlstate = sqlstate;
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_detail_log(mut self, detail_log: impl Into<String>) -> Self {
        self.detail_log = Some(detail_log.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_schema_name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    pub fn with_constraint_name(mut self, constraint_name: impl Into<String>) -> Self {
        self.constraint_name = Some(constraint_name.into());
        self
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = append_context(self.context.take(), context.into());
        self
    }

    pub fn with_backtrace(mut self, backtrace: impl Into<String>) -> Self {
        self.backtrace = Some(backtrace.into());
        self
    }

    pub fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        self.message_id = Some(message_id.into());
        self
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    pub fn with_context_domain(mut self, context_domain: impl Into<String>) -> Self {
        self.context_domain = Some(context_domain.into());
        self
    }

    pub fn with_hide_statement(mut self, hide_statement: bool) -> Self {
        self.hide_statement = hide_statement;
        self
    }

    pub fn with_hide_context(mut self, hide_context: bool) -> Self {
        self.hide_context = hide_context;
        self
    }

    pub fn with_location(
        mut self,
        filename: impl Into<String>,
        lineno: i32,
        funcname: impl Into<String>,
    ) -> Self {
        self.location = Some(ErrorLocation::new(filename, lineno, funcname));
        self
    }

    pub fn with_error_location(mut self, location: ErrorLocation) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_saved_errno(mut self, saved_errno: i32) -> Self {
        self.saved_errno = Some(saved_errno);
        self
    }

    pub fn with_cursor_position(mut self, cursor_position: i32) -> Self {
        self.cursor_position = nonzero_position(cursor_position);
        self
    }

    pub fn with_internal_position(mut self, internal_position: i32) -> Self {
        self.internal_position = nonzero_position(internal_position);
        self
    }

    pub fn with_internal_query(mut self, internal_query: impl Into<String>) -> Self {
        self.internal_query = Some(internal_query.into());
        self
    }

    pub fn with_error_field(
        mut self,
        field: ErrorField,
        value: impl Into<String>,
    ) -> PgResult<Self> {
        self.set_error_field(field, value)?;
        Ok(self)
    }

    /// In-place variant of [`PgError::with_error_field`], used by the stack
    /// frame helpers that mutate the current errordata in place (mirroring C's
    /// `err_generic_string`).
    pub fn set_error_field(&mut self, field: ErrorField, value: impl Into<String>) -> PgResult<()> {
        let value = value.into();
        match field {
            PG_DIAG_SCHEMA_NAME => self.schema_name = Some(value),
            PG_DIAG_TABLE_NAME => self.table_name = Some(value),
            PG_DIAG_COLUMN_NAME => self.column_name = Some(value),
            PG_DIAG_DATATYPE_NAME => self.datatype_name = Some(value),
            PG_DIAG_CONSTRAINT_NAME => self.constraint_name = Some(value),
            _ => {
                return Err(PgError::error(format!(
                    "unsupported ErrorData field id: {}",
                    field.0
                )));
            }
        }
        Ok(())
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PgError {}

pub(crate) fn nonzero_position(position: i32) -> Option<i32> {
    (position != 0).then_some(position)
}

fn append_context(existing: Option<String>, next: String) -> Option<String> {
    match existing {
        Some(mut existing) => {
            existing.push('\n');
            existing.push_str(&next);
            Some(existing)
        }
        None => Some(next),
    }
}
