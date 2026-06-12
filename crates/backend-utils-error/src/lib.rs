//! Port of `src/backend/utils/error/elog.c` — error logging and reporting.
//!
//! The error vocabulary (`ErrorLevel`, `SqlState`, `PgError`, `PgResult`)
//! lives in `types-error`; this crate owns the reporting machinery: the
//! errstart/errfinish cycle, the re-entrant ErrorData stack, the recursion
//! guard, output policy, log_line_prefix formatting, and the server-log /
//! syslog / frontend writers.
//!
//! # Sanctioned design divergences (audit against these)
//!
//! 1. **PgResult instead of sigsetjmp.** C's PG_TRY/`PG_exception_stack`/
//!    `PG_RE_THROW` nonlocal exit becomes `PgResult` propagation: at ERROR
//!    level `errfinish` pops the frame and returns `Err(PgError)`; the
//!    catching recovery block (PostgresMain when it lands) emits via
//!    [`emit_error_report_for`] and calls [`FlushErrorState`]. Consequences,
//!    all stemming from `PG_exception_stack` not existing:
//!    - errstart's "no handler => promote ERROR to FATAL" clause is dropped
//!      (`ExitOnAnyError` and `proc_exit_inprogress` promotions remain);
//!    - errfinish's FATAL-path `whereToSendOutput = DestNone` reset applies
//!      whenever output was going to a remote client (the C gates it on
//!      startup, i.e. no handler installed);
//!    - `pg_re_throw`'s no-outer-handler FATAL promotion is gone — rethrow is
//!      `Err` propagation;
//!    - the ERROR-path resets of `InterruptHoldoffCount` /
//!      `QueryCancelHoldoffCount` and errfinish's trailing
//!      `CHECK_FOR_INTERRUPTS()` belong to the interrupt machinery
//!      (miscadmin/tcop) and are the catching frame's responsibility.
//! 2. **GUC config is owned, not seamed** ([`config`]): `log_min_messages`,
//!    `client_min_messages`, `whereToSendOutput`, `ClientAuthInProgress`, and
//!    the other globals elog.c reads hold the C boot defaults here and expose
//!    setters for the owning units (guc -> error is acyclic). The logging hot
//!    path contains no seams and cannot panic.
//! 3. **Output sinks via per-owner seam crates**: frontend protocol output
//!    goes through `backend-libpq-pqcomm-seams` (`pq_putmessage`/`pq_flush`);
//!    `write_pipe_chunks` (elog.c's own function) is ported here in
//!    [`report`]; the syslogger's `write_syslogger_file` goes through
//!    `backend-postmaster-syslogger-seams`;
//!    csv/json log lines through `backend-utils-error-small-seams`; FATAL's
//!    `proc_exit(1)` through `backend-storage-ipc-seams`; the FATAL pgstat
//!    note through `backend-utils-activity-pgstat-seams`. All panic until
//!    their owners land — and all are unreachable under the boot defaults
//!    (`whereToSendOutput = DestNone`, stderr logging, no redirection).
//! 4. **PANIC aborts via `std::process::abort()`** (the C `abort()`).
//! 5. **Session/process context behind a provider trait** ([`sink::BackendLogContext`]):
//!    the per-process globals log_line_prefix reads (`MyProcPort`, `MyProc`,
//!    `MyStartTime`, `debug_query_string`, `GetTopTransactionIdIfAny()`,
//!    `pgstat_get_my_query_id()`, ...) are supplied by an installable
//!    provider whose defaults mirror the C boot state.
//! 6. **NLS is disabled** (the non-`ENABLE_NLS` build): `err_gettext` and the
//!    translation calls are identity, so they are not reproduced.
//! 7. **Variadic message functions take preformatted strings**: callers use
//!    `format!`; only `%m` is expanded here (against the saved errno, via
//!    `strerror`), matching `EVALUATE_MESSAGE`'s errno restoration.
//! 8. **Timestamps format in GMT** (the boot-default `log_timezone`; the
//!    timezone GUC machinery is a separate unit). `pg_mbcliplen` in
//!    `write_syslog` becomes a UTF-8 char-boundary clip (owned strings are
//!    UTF-8). win32-only branches (`write_eventlog`, `GetACPEncoding`, the
//!    UTF-16 console path) are not ported.
//! 9. **`ERRORDATA_STACK_SIZE` stack and `CHECK_STACK_DEPTH`** port
//!    faithfully; "no error in flight" surfaces as
//!    `Err("errstart was not called")` rather than a recursive ereport.

mod builder;
pub mod config;
mod context_chain;
pub mod errno;
mod policy;
mod report;
pub mod sink;
mod stack;
mod syslog;

pub use builder::{elog, ereport, ErrorBuilder};
pub use config::{
    assign_backtrace_functions, assign_log_destination, assign_syslog_facility,
    assign_syslog_ident, check_backtrace_functions, check_log_destination,
    matches_backtrace_functions, BacktraceFunctionList,
};
pub use context_chain::{
    append_error_context, error_context_depth, error_context_push, error_context_stack_clear,
    error_context_stack_pop_innermost, ErrorContextGuard,
};
pub use policy::{
    is_log_level_output, message_level_is_interesting, should_output_to_client,
    should_output_to_server,
};
pub use report::{
    append_with_tabs, check_log_of_query, err_sendstring, error_severity, format_elog_string,
    get_backend_type_for_log, get_formatted_log_time, get_formatted_start_time, log_line_prefix,
    log_status_format, pre_format_elog_string, reset_formatted_start_time,
    send_message_to_frontend, send_message_to_server_log, set_backtrace, unpack_sql_state,
    vwrite_stderr, write_console, write_pipe_chunks, write_stderr, DebugFileOpen,
};
pub use sink::{
    backend_log_context, set_backend_log_context, set_emit_log_hook, BackendLogContext,
    EmitLogHook,
};
pub use stack::{
    errbacktrace, errcode, errcode_for_file_access, errcode_for_socket_access, errcontext_msg,
    errdetail, errdetail_internal, errdetail_log, errdetail_log_plural, errdetail_plural,
    errfinish, errhidecontext, errhidestmt, errhint, errhint_internal, errhint_plural, errmsg,
    errmsg_internal, errmsg_plural, errposition, errsave_finish, errsave_start, errstart,
    errstart_cold, emit_error_report_for, err_generic_string, geterrcode, geterrposition,
    getinternalerrposition, in_error_recursion_trouble, internalerrposition, internalerrquery,
    pg_re_throw, reset_statement_suppressed, set_errcontext_domain, CopyErrorData, EmitErrorReport,
    FlushErrorState, FreeErrorData, GetErrorContextStack, ReThrowError, ThrowErrorData,
    ERRORDATA_STACK_SIZE,
};
pub use syslog::write_syslog;
pub use types_error::{ErrorLevel, PgError, PgResult, SoftErrorContext, SqlState};

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    backend_utils_error_seams::ereport::set(stack::ThrowErrorData);
}

#[cfg(test)]
mod tests;
