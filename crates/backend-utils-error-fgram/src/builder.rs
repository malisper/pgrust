use std::io;

use pgrust_pg_ffi::{
    ErrorField, ErrorLevel, SqlState, ERRCODE_CONNECTION_FAILURE, ERRCODE_DISK_FULL,
    ERRCODE_DUPLICATE_FILE, ERRCODE_FILE_NAME_TOO_LONG, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INSUFFICIENT_RESOURCES, ERRCODE_INTERNAL_ERROR, ERRCODE_IO_ERROR,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_UNDEFINED_FILE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, LOG, NOTICE,
    WARNING,
};

use crate::{emit_report, ErrorLocation, PgError, PgResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorBuilder {
    error: PgError,
}

impl ErrorBuilder {
    pub fn new(level: ErrorLevel) -> Self {
        Self {
            error: PgError::new(level, "PostgreSQL error"),
        }
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.error = self.error.with_domain(domain);
        self
    }

    pub fn with_context_domain(mut self, context_domain: impl Into<String>) -> Self {
        self.error = self.error.with_context_domain(context_domain);
        self
    }

    pub fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        self.error = self.error.with_message_id(message_id);
        self
    }

    pub fn with_saved_errno(mut self, saved_errno: i32) -> Self {
        self.error = self.error.with_saved_errno(saved_errno);
        self
    }

    pub fn errcode(mut self, sqlstate: SqlState) -> Self {
        self.error = self.error.with_sqlstate(sqlstate);
        self
    }

    pub fn errcode_for_file_access(mut self) -> Self {
        if let Some(errno) = self.error.saved_errno() {
            self.error = self.error.with_sqlstate(errcode_for_file_access(errno));
        }
        self
    }

    pub fn errcode_for_socket_access(mut self) -> Self {
        if let Some(errno) = self.error.saved_errno() {
            self.error = self.error.with_sqlstate(errcode_for_socket_access(errno));
        }
        self
    }

    pub fn errmsg(mut self, message: impl Into<String>) -> Self {
        let message_id = message.into();
        let message = self.format_message(message_id.clone());
        self.error = self.error.with_message(message).with_message_id(message_id);
        self
    }

    pub fn errmsg_internal(mut self, message: impl Into<String>) -> Self {
        let message = self.format_message(message.into());
        self.error = self.error.with_message(message);
        self
    }

    pub fn errmsg_plural(
        self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        if n == 1 {
            self.errmsg(singular)
        } else {
            self.errmsg(plural)
        }
    }

    pub fn errdetail(mut self, detail: impl Into<String>) -> Self {
        let detail = self.format_message(detail.into());
        self.error = self.error.with_detail(detail);
        self
    }

    pub fn errdetail_internal(mut self, detail: impl Into<String>) -> Self {
        let detail = self.format_message(detail.into());
        self.error = self.error.with_detail(detail);
        self
    }

    pub fn errdetail_log(mut self, detail_log: impl Into<String>) -> Self {
        let detail_log = self.format_message(detail_log.into());
        self.error = self.error.with_detail_log(detail_log);
        self
    }

    pub fn errdetail_log_plural(
        self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        if n == 1 {
            self.errdetail_log(singular)
        } else {
            self.errdetail_log(plural)
        }
    }

    pub fn errdetail_plural(
        self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        if n == 1 {
            self.errdetail(singular)
        } else {
            self.errdetail(plural)
        }
    }

    pub fn errhint(mut self, hint: impl Into<String>) -> Self {
        let hint = self.format_message(hint.into());
        self.error = self.error.with_hint(hint);
        self
    }

    pub fn errhint_internal(mut self, hint: impl Into<String>) -> Self {
        let hint = self.format_message(hint.into());
        self.error = self.error.with_hint(hint);
        self
    }

    pub fn errhint_plural(
        self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        if n == 1 {
            self.errhint(singular)
        } else {
            self.errhint(plural)
        }
    }

    pub fn errcontext_msg(mut self, context: impl Into<String>) -> Self {
        let context = self.format_message(context.into());
        self.error = self.error.with_context(context);
        self
    }

    /// `errtableconstraint(rel, conname)` (relcache.c) -- stores the
    /// schema_name/table_name/constraint_name triple of a table-related
    /// constraint.  The caller resolves the names (the relcache lookups live
    /// outside this crate); `schema_name` is `None` when `get_namespace_name`
    /// returned NULL, matching C's `err_generic_string` (which is a no-op on a
    /// NULL string).
    pub fn errtableconstraint(
        mut self,
        schema_name: Option<impl Into<String>>,
        table_name: impl Into<String>,
        constraint_name: impl Into<String>,
    ) -> Self {
        if let Some(schema) = schema_name {
            self.error = self.error.with_schema_name(schema);
        }
        self.error = self
            .error
            .with_table_name(table_name)
            .with_constraint_name(constraint_name);
        self
    }

    pub fn errhidestmt(mut self, hide_statement: bool) -> Self {
        self.error = self.error.with_hide_statement(hide_statement);
        self
    }

    pub fn errhidecontext(mut self, hide_context: bool) -> Self {
        self.error = self.error.with_hide_context(hide_context);
        self
    }

    pub fn errbacktrace(mut self, backtrace: impl Into<String>) -> Self {
        self.error = self.error.with_backtrace(backtrace);
        self
    }

    pub fn errposition(mut self, cursor_position: i32) -> Self {
        self.error = self.error.with_cursor_position(cursor_position);
        self
    }

    pub fn internalerrposition(mut self, internal_position: i32) -> Self {
        self.error = self.error.with_internal_position(internal_position);
        self
    }

    pub fn internalerrquery(mut self, query: impl Into<String>) -> Self {
        self.error = self.error.with_internal_query(query);
        self
    }

    pub fn err_generic_string(
        mut self,
        field: ErrorField,
        value: impl Into<String>,
    ) -> PgResult<Self> {
        self.error = self.error.with_error_field(field, value)?;
        Ok(self)
    }

    pub fn into_error(self) -> PgError {
        self.error
    }

    pub fn finish(self, location: ErrorLocation) -> PgResult<()> {
        errfinish(self, location)
    }

    fn format_message(&self, message: String) -> String {
        match self.error.saved_errno() {
            Some(errno) => replace_percent_m(&message, errno),
            None => message,
        }
    }
}

pub fn ereport(level: ErrorLevel) -> ErrorBuilder {
    ErrorBuilder::new(level)
}

pub fn errstart(level: ErrorLevel, domain: impl Into<String>) -> ErrorBuilder {
    ereport(level).with_domain(domain)
}

pub fn errstart_cold(level: ErrorLevel, domain: impl Into<String>) -> ErrorBuilder {
    errstart(level, domain)
}

pub fn errfinish(builder: ErrorBuilder, location: ErrorLocation) -> PgResult<()> {
    let error = builder.into_error().with_error_location(location);
    if error.level() >= ERROR {
        Err(error)
    } else {
        if message_level_is_interesting(error.level()) {
            emit_report(&error);
        }
        Ok(())
    }
}

pub fn elog(level: ErrorLevel, message: impl Into<String>) -> PgResult<()> {
    errfinish(
        ereport(level).errmsg_internal(message),
        ErrorLocation {
            filename: None,
            lineno: 0,
            funcname: None,
        },
    )
}

pub fn message_level_is_interesting(level: ErrorLevel) -> bool {
    level >= LOG || level >= NOTICE || level >= WARNING || level >= ERROR
}

pub fn errcode(sqlstate: SqlState, message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(sqlstate)
}

pub fn errmsg(message: impl Into<String>) -> PgError {
    PgError::error(message)
}

pub fn errmsg_internal(message: impl Into<String>) -> PgError {
    PgError::error(message)
}

pub fn errmsg_plural(singular: impl Into<String>, plural: impl Into<String>, n: u64) -> PgError {
    if n == 1 {
        errmsg(singular)
    } else {
        errmsg(plural)
    }
}

pub fn errdetail(error: PgError, detail: impl Into<String>) -> PgError {
    error.with_detail(detail)
}

pub fn errdetail_internal(error: PgError, detail: impl Into<String>) -> PgError {
    error.with_detail(detail)
}

pub fn errdetail_log(error: PgError, detail: impl Into<String>) -> PgError {
    error.with_detail_log(detail)
}

pub fn errdetail_log_plural(
    error: PgError,
    singular: impl Into<String>,
    plural: impl Into<String>,
    n: u64,
) -> PgError {
    if n == 1 {
        error.with_detail_log(singular)
    } else {
        error.with_detail_log(plural)
    }
}

pub fn errdetail_plural(
    error: PgError,
    singular: impl Into<String>,
    plural: impl Into<String>,
    n: u64,
) -> PgError {
    if n == 1 {
        error.with_detail(singular)
    } else {
        error.with_detail(plural)
    }
}

pub fn errhint(error: PgError, hint: impl Into<String>) -> PgError {
    error.with_hint(hint)
}

pub fn errhint_internal(error: PgError, hint: impl Into<String>) -> PgError {
    error.with_hint(hint)
}

pub fn errhint_plural(
    error: PgError,
    singular: impl Into<String>,
    plural: impl Into<String>,
    n: u64,
) -> PgError {
    if n == 1 {
        error.with_hint(singular)
    } else {
        error.with_hint(plural)
    }
}

pub fn errcontext_msg(error: PgError, context: impl Into<String>) -> PgError {
    error.with_context(context)
}

pub fn errbacktrace(error: PgError, backtrace: impl Into<String>) -> PgError {
    error.with_backtrace(backtrace)
}

pub fn errhidestmt(error: PgError, hide_statement: bool) -> PgError {
    error.with_hide_statement(hide_statement)
}

pub fn errhidecontext(error: PgError, hide_context: bool) -> PgError {
    error.with_hide_context(hide_context)
}

pub fn errposition(error: PgError, cursor_position: i32) -> PgError {
    error.with_cursor_position(cursor_position)
}

pub fn internalerrposition(error: PgError, internal_position: i32) -> PgError {
    error.with_internal_position(internal_position)
}

pub fn internalerrquery(error: PgError, query: impl Into<String>) -> PgError {
    error.with_internal_query(query)
}

pub fn err_generic_string(
    error: PgError,
    field: ErrorField,
    value: impl Into<String>,
) -> PgResult<PgError> {
    error.with_error_field(field, value)
}

pub fn errcode_for_file_access(errno: i32) -> SqlState {
    match errno {
        libc::EPERM | libc::EACCES => ERRCODE_INSUFFICIENT_PRIVILEGE,
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        libc::EROFS => ERRCODE_INSUFFICIENT_PRIVILEGE,
        libc::ENOENT => ERRCODE_UNDEFINED_FILE,
        libc::EEXIST => ERRCODE_DUPLICATE_FILE,
        libc::ENOTDIR | libc::EISDIR | libc::ENOTEMPTY => ERRCODE_WRONG_OBJECT_TYPE,
        libc::ENOSPC => ERRCODE_DISK_FULL,
        libc::ENOMEM => ERRCODE_OUT_OF_MEMORY,
        libc::ENFILE | libc::EMFILE => ERRCODE_INSUFFICIENT_RESOURCES,
        libc::EIO => ERRCODE_IO_ERROR,
        libc::ENAMETOOLONG => ERRCODE_FILE_NAME_TOO_LONG,
        _ => ERRCODE_INTERNAL_ERROR,
    }
}

pub fn errcode_for_socket_access(errno: i32) -> SqlState {
    match errno {
        libc::EPIPE
        | libc::ECONNRESET
        | libc::ECONNABORTED
        | libc::EHOSTDOWN
        | libc::EHOSTUNREACH
        | libc::ENETDOWN
        | libc::ENETRESET
        | libc::ENETUNREACH
        | libc::ETIMEDOUT => ERRCODE_CONNECTION_FAILURE,
        _ => ERRCODE_INTERNAL_ERROR,
    }
}

fn replace_percent_m(message: &str, errno: i32) -> String {
    if !message.contains("%m") {
        return message.to_owned();
    }

    let os_error = io::Error::from_raw_os_error(errno).to_string();
    message.replace("%m", &os_error)
}
