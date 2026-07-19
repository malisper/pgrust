//! Owned-value report builder: the Rust face of `ereport(elevel,
//! (errcode(...), errmsg(...), ...))`. `finish` feeds the built `PgError`
//! into the real errstart/errfinish cycle via [`crate::ThrowErrorData`].
//!
//! Suppression fast-gate: for `level < ERROR`, `ereport` checks
//! `message_level_is_interesting` (the same predicate errstart's
//! short-circuit uses) and returns a Suppressed builder — no `PgError` is
//! built, every field method no-ops without converting its argument, and
//! `finish` returns `Ok(())` without entering errstart. This mirrors the C
//! `ereport()` macro's `if (errstart(...))` shape, which skips the whole
//! argument-evaluation body.

use ::types_error::{ErrorField, ErrorLocation, ErrorLevel, PgError, PgResult, SqlState, ERROR};

use crate::{errno, policy};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorBuilder {
    level: ErrorLevel,
    error: Option<PgError>,
}

impl ErrorBuilder {
    // track_caller so the F/L capture lands on the ereport() call site, not
    // this constructor (C's ereport macro records the report site).
    #[cold]
    #[track_caller]
    pub fn new(level: ErrorLevel) -> Self {
        Self {
            level,
            error: Some(PgError::new(level, String::new())),
        }
    }

    #[inline]
    fn suppressed(level: ErrorLevel) -> Self {
        Self { level, error: None }
    }

    #[inline]
    pub fn is_suppressed(&self) -> bool {
        self.error.is_none()
    }

    #[cold]
    #[inline(never)]
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_domain(domain));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn with_context_domain(mut self, context_domain: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_context_domain(context_domain));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_message_id(message_id));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn with_saved_errno(mut self, saved_errno: i32) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_saved_errno(saved_errno));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errcode(mut self, sqlstate: SqlState) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_sqlstate(sqlstate));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errcode_for_file_access(mut self) -> Self {
        if let Some(error) = self.error.take() {
            let errnum = error.saved_errno.unwrap_or_else(errno::current_errno);
            self.error = Some(error.with_sqlstate(errno::sqlstate_for_file_access(errnum)));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errcode_for_socket_access(mut self) -> Self {
        if let Some(error) = self.error.take() {
            let errnum = error.saved_errno.unwrap_or_else(errno::current_errno);
            self.error = Some(error.with_sqlstate(errno::sqlstate_for_socket_access(errnum)));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errmsg(mut self, message: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let message_id = message.into();
            let message = format_message(&error, message_id.clone());
            self.error = Some(error.with_message(message).with_message_id(message_id));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errmsg_internal(mut self, message: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let message_id = message.into();
            let message = format_message(&error, message_id.clone());
            self.error = Some(error.with_message(message).with_message_id(message_id));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errmsg_plural(
        mut self,
        singular: impl Into<String>,
        plural: impl Into<String>,
        n: u64,
    ) -> Self {
        if let Some(error) = self.error.take() {
            let singular = singular.into();
            let picked = if n == 1 { singular.clone() } else { plural.into() };
            let message = format_message(&error, picked);
            self.error = Some(error.with_message(message).with_message_id(singular));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errdetail(mut self, detail: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let detail = format_message(&error, detail.into());
            self.error = Some(error.with_detail(detail));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errdetail_internal(self, detail: impl Into<String>) -> Self {
        self.errdetail(detail)
    }

    #[cold]
    #[inline(never)]
    pub fn errdetail_log(mut self, detail_log: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let detail_log = format_message(&error, detail_log.into());
            self.error = Some(error.with_detail_log(detail_log));
        }
        self
    }

    #[cold]
    #[inline(never)]
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

    #[cold]
    #[inline(never)]
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

    #[cold]
    #[inline(never)]
    pub fn errhint(mut self, hint: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let hint = format_message(&error, hint.into());
            self.error = Some(error.with_hint(hint));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errhint_internal(self, hint: impl Into<String>) -> Self {
        self.errhint(hint)
    }

    #[cold]
    #[inline(never)]
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

    #[cold]
    #[inline(never)]
    pub fn errcontext_msg(mut self, context: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            let context = format_message(&error, context.into());
            self.error = Some(error.with_context(context));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errhidestmt(mut self, hide_statement: bool) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_hide_statement(hide_statement));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errhidecontext(mut self, hide_context: bool) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_hide_context(hide_context));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errbacktrace(mut self) -> Self {
        if let Some(error) = &mut self.error {
            crate::report::set_backtrace(error, 1);
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn errposition(mut self, cursor_position: i32) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_cursor_position(cursor_position));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn internalerrposition(mut self, internal_position: i32) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_internal_position(internal_position));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn internalerrquery(mut self, query: impl Into<String>) -> Self {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_internal_query(query));
        }
        self
    }

    #[cold]
    #[inline(never)]
    pub fn err_generic_string(
        mut self,
        field: ErrorField,
        value: impl Into<String>,
    ) -> PgResult<Self> {
        if let Some(error) = self.error.take() {
            self.error = Some(error.with_error_field(field, value)?);
        }
        Ok(self)
    }

    #[cold]
    #[inline(never)]
    pub fn into_error(self) -> PgError {
        let level = self.level;
        self.error
            .unwrap_or_else(|| PgError::new(level, String::new()))
    }

    #[inline]
    pub fn finish(self, location: ErrorLocation) -> PgResult<()> {
        match self.error {
            Some(error) => crate::ThrowErrorData(error.with_error_location(location)),
            None => Ok(()),
        }
    }
}

#[cold]
#[inline(never)]
fn format_message(error: &PgError, message: String) -> String {
    match error.saved_errno {
        Some(errnum) => errno::replace_percent_m(&message, errnum),
        None => message,
    }
}

#[inline]
#[track_caller]
pub fn ereport(level: ErrorLevel) -> ErrorBuilder {
    if level < ERROR && !policy::message_level_is_interesting(level) {
        return ErrorBuilder::suppressed(level);
    }
    ErrorBuilder::new(level)
}

#[inline]
#[track_caller]
pub fn elog(level: ErrorLevel, message: impl Into<String>) -> PgResult<()> {
    // The empty location merges away in with_error_location: the F/L capture
    // from this call site (via ereport's track_caller) stands, like C's
    // elog macro recording __FILE__/__LINE__.
    ereport(level).errmsg_internal(message).finish(ErrorLocation {
        filename: None,
        lineno: 0,
        funcname: None,
    })
}
