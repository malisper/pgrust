use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use core::fmt;

use crate::{
    ErrorField, ErrorLevel, SqlState, ERRCODE_INTERNAL_ERROR, ERRCODE_SUCCESSFUL_COMPLETION,
    ERRCODE_WARNING, ERROR, NOTICE, PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME,
    PG_DIAG_DATATYPE_NAME, PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME, WARNING,
};

// The error is boxed: `PgError` is ~450 B and `PgResult` is the pervasive
// fallible return type, so inlining it would fatten every fallible frame.
// The one allocation lives on the cold error path only.
pub type PgResult<T> = Result<T, Box<PgError>>;

/// elog.c's default SQLSTATE when no explicit `errcode()` is supplied.
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
    /// Thin generic shell: only the two `.into()` conversions monomorphize per
    /// caller type; the struct build lives once in the non-generic `new_impl`.
    #[inline]
    pub fn new(filename: impl Into<String>, lineno: i32, funcname: impl Into<String>) -> Self {
        Self::new_impl(filename.into(), lineno, funcname.into())
    }

    #[cold]
    fn new_impl(filename: String, lineno: i32, funcname: String) -> Self {
        Self {
            filename: Some(filename),
            lineno,
            funcname: Some(funcname),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgError {
    pub level: ErrorLevel,
    pub sqlstate: SqlState,
    pub message: String,
    // Exact wire bytes for the primary message when C's message is not valid
    // UTF-8 (C error messages are byte strings — e.g. elog's %c of a high
    // "char" byte). When set, the frontend 'E'/'N' message sends these bytes
    // (cstring-truncated at the first NUL) instead of `message`; `message`
    // stays the lossy rendering for Rust-side consumers and the server log.
    pub message_raw: Option<alloc::vec::Vec<u8>>,
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
    // Set once the PL/pgSQL executor has attached its error-callback context
    // for the frame that first reported this error: C runs each
    // `error_context_stack` callback exactly once at report time and a
    // re-thrown error (`RAISE;`) carries the already-built context. Attach
    // once, then frozen. Not printed; not part of equality.
    pub plpgsql_context_attached: bool,
}

impl PgError {
    /// C's ereport/elog capture `__FILE__`/`__LINE__` at every report site and
    /// the wire protocol carries them as the F/L error fields (clients like
    /// pg8000 read them). `#[track_caller]` gives the same for free: the
    /// construction site's file/line, overridable by the explicit
    /// `with_location`/`finish` lanes (which C-parity sites use to also carry
    /// the routine name).
    #[inline]
    #[track_caller]
    pub fn new(level: ErrorLevel, message: impl Into<String>) -> Self {
        // `#[track_caller]` shell: capture the construction site here and pass
        // it explicitly so the non-generic `new_impl` reproduces the same F/L
        // fields without itself being `#[track_caller]`. Only `message.into()`
        // monomorphizes per caller type; the ~24-field build lives once below.
        Self::new_impl(level, message.into(), core::panic::Location::caller())
    }

    #[cold]
    fn new_impl(
        level: ErrorLevel,
        message: String,
        caller: &core::panic::Location<'_>,
    ) -> Self {
        Self {
            level,
            sqlstate: default_sqlstate_for_level(level),
            message,
            message_raw: None,
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
            location: Some(ErrorLocation {
                filename: Some(caller.file().into()),
                lineno: caller.line() as i32,
                funcname: None,
            }),
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    #[cold]
    #[track_caller]
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(ERROR, message)
    }

    /// C elog/ereport with a message that is raw bytes (not guaranteed
    /// UTF-8): the wire sends `bytes` verbatim (NUL-truncated, like the
    /// cstring C builds); `message` carries the lossy rendering for
    /// Rust-side display.
    #[cold]
    pub fn error_raw_message(bytes: alloc::vec::Vec<u8>) -> Self {
        let truncated = match bytes.iter().position(|&b| b == 0) {
            Some(n) => &bytes[..n],
            None => &bytes[..],
        };
        let mut e = Self::new(ERROR, String::from_utf8_lossy(truncated).into_owned());
        e.message_raw = Some(bytes);
        e
    }

    #[cold]
    #[track_caller]
    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(WARNING, message)
    }

    #[cold]
    #[track_caller]
    pub fn notice(message: impl Into<String>) -> Self {
        Self::new(NOTICE, message)
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

    /// Appends to any existing context, newline-separated (C `errcontext()`).
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = append_context(self.context.take(), context.into());
        self
    }

    /// Attach-on-propagation replacement for C's `error_context_stack`
    /// callbacks, applied where C pushed the callback:
    /// `result.map_err(|e| e.add_context("while ..."))`.
    pub fn add_context(self, context: impl Into<String>) -> Self {
        self.with_context(context)
    }

    pub fn add_context_line(&mut self, line: impl Into<String>) {
        self.context = append_context(self.context.take(), line.into());
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

    /// Merge with the construction-site capture: an explicit file/line pair
    /// (any filename, or a positive lineno) replaces the captured pair as a
    /// unit — never mixed — and the funcname (routine) merges independently.
    /// An all-empty location (e.g. `elog`'s) leaves the capture standing, so
    /// a routine-only override never erases the F/L fields on the wire.
    pub fn with_error_location(mut self, location: ErrorLocation) -> Self {
        self.location = Some(match self.location.take() {
            Some(captured) => {
                let explicit_pair = location.filename.is_some() || location.lineno > 0;
                ErrorLocation {
                    filename: if explicit_pair { location.filename } else { captured.filename },
                    lineno: if explicit_pair { location.lineno } else { captured.lineno },
                    funcname: location.funcname.or(captured.funcname),
                }
            }
            None => location,
        });
        self
    }

    /// Attach just the routine (R) error field, C's `__func__`; the F/L
    /// capture from the construction site stands.
    pub fn with_funcname(mut self, funcname: impl Into<String>) -> Self {
        match &mut self.location {
            Some(loc) => loc.funcname = Some(funcname.into()),
            None => {
                self.location = Some(ErrorLocation {
                    filename: None,
                    lineno: 0,
                    funcname: Some(funcname.into()),
                });
            }
        }
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

    /// C's `err_generic_string`.
    pub fn set_error_field(&mut self, field: ErrorField, value: impl Into<String>) -> PgResult<()> {
        let value = value.into();
        match field {
            PG_DIAG_SCHEMA_NAME => self.schema_name = Some(value),
            PG_DIAG_TABLE_NAME => self.table_name = Some(value),
            PG_DIAG_COLUMN_NAME => self.column_name = Some(value),
            PG_DIAG_DATATYPE_NAME => self.datatype_name = Some(value),
            PG_DIAG_CONSTRAINT_NAME => self.constraint_name = Some(value),
            _ => {
                return Err(Box::new(PgError::error(format!(
                    "unsupported ErrorData field id: {}",
                    field.0
                ))));
            }
        }
        Ok(())
    }
}

/// `ErrorSaveContext` semantics: captures a recoverable [`PgError`] or merely
/// records that one occurred; the `errsave` driver lives elsewhere.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SoftErrorContext {
    details_wanted: bool,
    error_occurred: bool,
    error: Option<PgError>,
}

impl SoftErrorContext {
    pub fn new(details_wanted: bool) -> Self {
        Self {
            details_wanted,
            error_occurred: false,
            error: None,
        }
    }

    pub fn details_wanted(&self) -> bool {
        self.details_wanted
    }

    pub fn error_occurred(&self) -> bool {
        self.error_occurred
    }

    pub fn error(&self) -> Option<&PgError> {
        self.error.as_ref()
    }

    pub fn take_error(&mut self) -> Option<PgError> {
        self.error.take()
    }

    pub fn save(&mut self, error: PgError) {
        self.error_occurred = true;
        self.error = Some(error);
    }

    pub fn mark_error_occurred(&mut self) {
        self.error_occurred = true;
    }

    /// C resets `error_occurred = false` after handling a skipped COPY row;
    /// clears any saved error so the context is reusable.
    pub fn reset_error_occurred(&mut self) {
        self.error_occurred = false;
        self.error = None;
    }
}

/// C `ereturn(escontext, errorval, ...)` in value form: with a soft-error
/// context the error is saved into it (full details only when wanted) and
/// `errorval` is returned; without one the error propagates as a hard error.
#[cold]
pub fn ereturn<T>(
    escontext: Option<&mut SoftErrorContext>,
    errorval: T,
    error: PgError,
) -> PgResult<T> {
    match escontext {
        Some(context) => {
            if context.details_wanted() {
                context.save(error);
            } else {
                context.mark_error_occurred();
            }
            Ok(errorval)
        }
        None => Err(Box::new(error)),
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for PgError {}

// `?` applies a single `From`, so bare-`PgError` returns can propagate from
// the boxed `PgResult` channel.
impl From<Box<PgError>> for PgError {
    #[cold]
    fn from(boxed: Box<PgError>) -> Self {
        *boxed
    }
}

/// Recover a [`PgError`] from a `catch_unwind` payload. Must try BOTH the bare
/// `PgError` and the `Box<PgError>` downcast (a `panic_any` of a `PgResult`
/// error carries the box), otherwise a structured error is mistaken for an
/// unstructured panic. On a miss the payload is handed back for the legacy
/// string channel.
#[cold]
pub fn pg_error_from_panic(
    payload: Box<dyn core::any::Any + Send>,
) -> Result<PgError, Box<dyn core::any::Any + Send>> {
    match payload.downcast::<PgError>() {
        Ok(err) => Ok(*err),
        Err(payload) => match payload.downcast::<Box<PgError>>() {
            Ok(boxed) => Ok(**boxed),
            Err(payload) => Err(payload),
        },
    }
}

pub fn nonzero_position(position: i32) -> Option<i32> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ERRCODE_UNDEFINED_TABLE, INFO};

    // Gate: PgError is fat (~450 B); PgResult must stay pointer-thin.
    #[test]
    fn pg_result_does_not_inline_fat_error() {
        let err = core::mem::size_of::<PgError>();
        assert!(
            err >= 256,
            "PgError unexpectedly small ({err} B) — gate assumption stale"
        );
        assert_eq!(
            core::mem::size_of::<PgResult<()>>(),
            core::mem::size_of::<Option<core::ptr::NonNull<PgError>>>(),
            "PgResult<()> should be a single pointer (boxed, niche-packed Err)"
        );
        assert!(
            core::mem::size_of::<PgResult<u64>>() <= 16,
            "PgResult<Datum> should be <=16 B, got {}",
            core::mem::size_of::<PgResult<u64>>()
        );
    }

    #[test]
    fn default_sqlstates_match_elog() {
        assert_eq!(default_sqlstate_for_level(ERROR), ERRCODE_INTERNAL_ERROR);
        assert_eq!(default_sqlstate_for_level(WARNING), ERRCODE_WARNING);
        assert_eq!(
            default_sqlstate_for_level(INFO),
            ERRCODE_SUCCESSFUL_COMPLETION
        );
    }

    #[test]
    fn builder_smoke() {
        let err = PgError::error("relation \"foo\" does not exist")
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE)
            .with_detail("detail")
            .with_hint("hint")
            .with_table_name("foo")
            .with_context("first")
            .with_context("second")
            .with_cursor_position(0);
        assert_eq!(err.level(), ERROR);
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
        assert_eq!(err.detail(), Some("detail"));
        assert_eq!(err.hint(), Some("hint"));
        assert_eq!(err.table_name(), Some("foo"));
        assert_eq!(err.context(), Some("first\nsecond"));
        assert_eq!(err.cursor_position(), None);
    }

    // C parity: every report site carries file/line (the wire F/L fields).
    #[test]
    fn construction_captures_caller_file_line() {
        let before = line!();
        let err = PgError::error("boom");
        let loc = err.location().expect("track_caller capture");
        assert_eq!(loc.filename.as_deref(), Some(file!()));
        assert_eq!(loc.lineno, before as i32 + 1);
        assert_eq!(loc.funcname, None);
    }

    #[test]
    fn location_merge_keeps_capture_under_partial_overrides() {
        // Routine-only override (with_funcname): F/L capture stands.
        let err = PgError::error("x").with_funcname("RevalidateCachedQuery");
        let loc = err.location().unwrap();
        assert_eq!(loc.funcname.as_deref(), Some("RevalidateCachedQuery"));
        assert_eq!(loc.filename.as_deref(), Some(file!()));
        assert!(loc.lineno > 0);

        // All-empty explicit location (elog's): capture stands.
        let err = PgError::error("x").with_error_location(ErrorLocation {
            filename: None,
            lineno: 0,
            funcname: None,
        });
        assert_eq!(err.location().unwrap().filename.as_deref(), Some(file!()));

        // Explicit file/line pair replaces the captured pair as a unit.
        let err = PgError::error("x").with_error_location(ErrorLocation {
            filename: Some("pl_exec.c".into()),
            lineno: 0,
            funcname: Some("exec_stmt_raise".into()),
        });
        let loc = err.location().unwrap();
        assert_eq!(loc.filename.as_deref(), Some("pl_exec.c"));
        assert_eq!(loc.lineno, 0);
        assert_eq!(loc.funcname.as_deref(), Some("exec_stmt_raise"));

        // Full with_location override still wins outright.
        let err = PgError::error("x").with_location("f.c", 7, "fn");
        let loc = err.location().unwrap();
        assert_eq!(
            (loc.filename.as_deref(), loc.lineno, loc.funcname.as_deref()),
            (Some("f.c"), 7, Some("fn"))
        );
    }

    #[test]
    fn error_field_dispatch() {
        let err = PgError::error("x")
            .with_error_field(PG_DIAG_COLUMN_NAME, "col")
            .unwrap();
        assert_eq!(err.column_name(), Some("col"));
        assert!(PgError::error("x").with_error_field(ErrorField(0), "v").is_err());

        let mut soft = SoftErrorContext::new(true);
        assert!(!soft.error_occurred());
        soft.save(PgError::error("boom"));
        assert!(soft.error_occurred());
        assert_eq!(soft.take_error().unwrap().message(), "boom");
    }
}
