//! Owned-value report builder: the ergonomic Rust face of
//! `ereport(elevel, (errcode(...), errmsg(...), ...))`.
//!
//! The builder assembles a `PgError`; `finish` feeds it into the real
//! errstart/errfinish cycle via [`crate::ThrowErrorData`], so severity
//! promotion, output decisions, context callbacks, and per-level recovery all
//! behave exactly as a C ereport.

use types_error::{ErrorField, ErrorLocation, ErrorLevel, PgError, PgResult, SqlState};

use crate::errno;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorBuilder {
    error: PgError,
}

impl ErrorBuilder {
    pub fn new(level: ErrorLevel) -> Self {
        Self {
            error: PgError::new(level, String::new()),
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

    /// Record the errno the report should interpret `%m` against (the C
    /// `edata->saved_errno`, captured at errstart).
    pub fn with_saved_errno(mut self, saved_errno: i32) -> Self {
        self.error = self.error.with_saved_errno(saved_errno);
        self
    }

    pub fn errcode(mut self, sqlstate: SqlState) -> Self {
        self.error = self.error.with_sqlstate(sqlstate);
        self
    }

    pub fn errcode_for_file_access(mut self) -> Self {
        let errnum = self.error.saved_errno.unwrap_or_else(errno::current_errno);
        self.error = self.error.with_sqlstate(errno::sqlstate_for_file_access(errnum));
        self
    }

    pub fn errcode_for_socket_access(mut self) -> Self {
        let errnum = self.error.saved_errno.unwrap_or_else(errno::current_errno);
        self.error = self
            .error
            .with_sqlstate(errno::sqlstate_for_socket_access(errnum));
        self
    }

    pub fn errmsg(mut self, message: impl Into<String>) -> Self {
        let message_id = message.into();
        let message = self.format_message(message_id.clone());
        self.error = self.error.with_message(message).with_message_id(message_id);
        self
    }

    /// Like `errmsg` but untranslated; C errmsg_internal still records the
    /// message id (`edata->message_id = fmt`).
    pub fn errmsg_internal(mut self, message: impl Into<String>) -> Self {
        let message_id = message.into();
        let message = self.format_message(message_id.clone());
        self.error = self.error.with_message(message).with_message_id(message_id);
        self
    }

    /// The message id is always the singular form, as in C
    /// (`edata->message_id = fmt_singular`).
    pub fn errmsg_plural(
        mut self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        let singular = singular.into();
        let picked = if n == 1 { singular.clone() } else { plural.into() };
        let message = self.format_message(picked);
        self.error = self.error.with_message(message).with_message_id(singular);
        self
    }

    pub fn errdetail(mut self, detail: impl Into<String>) -> Self {
        let detail = self.format_message(detail.into());
        self.error = self.error.with_detail(detail);
        self
    }

    pub fn errdetail_internal(self, detail: impl Into<String>) -> Self {
        self.errdetail(detail)
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

    pub fn errhint_internal(self, hint: impl Into<String>) -> Self {
        self.errhint(hint)
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

    pub fn errhidestmt(mut self, hide_statement: bool) -> Self {
        self.error = self.error.with_hide_statement(hide_statement);
        self
    }

    pub fn errhidecontext(mut self, hide_context: bool) -> Self {
        self.error = self.error.with_hide_context(hide_context);
        self
    }

    pub fn errbacktrace(mut self) -> Self {
        crate::report::set_backtrace(&mut self.error, 1);
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

    /// Materialize the built error as an owned value (running the
    /// error-context callbacks, as errfinish would). For code that returns
    /// `Err(...)` directly instead of calling [`ErrorBuilder::finish`].
    pub fn into_error(self) -> PgError {
        let mut error = self.error;
        crate::context_chain::run_error_context_callbacks(&mut error);
        error
    }

    /// `errfinish` for the builder: drive the full report cycle.
    pub fn finish(self, location: ErrorLocation) -> PgResult<()> {
        crate::ThrowErrorData(self.error.with_error_location(location))
    }

    fn format_message(&self, message: String) -> String {
        match self.error.saved_errno {
            Some(errnum) => errno::replace_percent_m(&message, errnum),
            None => message,
        }
    }
}

/// `ereport(level, ...)` — start building a report.
pub fn ereport(level: ErrorLevel) -> ErrorBuilder {
    ErrorBuilder::new(level)
}

/// `elog(level, msg)` — the simple no-fields report.
pub fn elog(level: ErrorLevel, message: impl Into<String>) -> PgResult<()> {
    ereport(level).errmsg_internal(message).finish(ErrorLocation {
        filename: None,
        lineno: 0,
        funcname: None,
    })
}
