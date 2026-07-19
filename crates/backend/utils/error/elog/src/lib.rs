//! Port of `src/backend/utils/error/elog.c` — error logging and reporting.
//! The error vocabulary lives in `types_error`; this crate owns the
//! errstart/errfinish cycle, the ErrorData stack, output policy,
//! log_line_prefix formatting, and the server-log/syslog/frontend writers.
//! Sanctioned design divergences: notes/elog-divergences.md.

mod builder;
pub mod config;
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
    backend_log_context, debug_query_string_scope, set_backend_log_context, set_emit_log_hook,
    set_frontend_redirect, with_debug_query_string, BackendLogContext, DebugQueryStringScope,
    EmitLogHook, FrontendRedirect,
};
pub use stack::{
    emit_error_report_for, ereport_msg, err_generic_string, errbacktrace, errcode,
    errcode_for_file_access, errcode_for_socket_access, errcontext_msg, errdetail,
    errdetail_internal, errdetail_log, errdetail_log_plural, errdetail_plural, errfinish,
    errhidecontext, errhidestmt, errhint, errhint_internal, errhint_plural, errmsg,
    errmsg_internal, errmsg_plural, errposition, errsave_finish, errsave_start, errstart,
    errstart_cold, error_stack_clean, geterrcode, geterrposition, getinternalerrposition,
    in_error_recursion_trouble, internalerrposition, internalerrquery, pg_re_throw,
    pop_emit_context_callback, push_emit_context_callback, reset_statement_suppressed,
    clear_emit_context_callbacks, suppress_statement,
    set_errcontext_domain, CopyErrorData, EmitErrorReport, FlushErrorState, FreeErrorData,
    GetErrorContextStack, ReThrowError, ThrowErrorData, ERRORDATA_STACK_SIZE,
};
pub use syslog::write_syslog;
pub use ::types_error::{ErrorLevel, PgError, PgResult, SoftErrorContext, SqlState};

pub fn init_seams() {
    use ::guc_tables::{hooks, vars, GucHookExtra, GucVarAccessors};
    use ::types_error::PGErrorVerbosity;

    elog_seams::ereport::set(stack::ThrowErrorData);
    elog_seams::sqlstate_for_file_access::set(errno::sqlstate_for_file_access);
    elog_seams::ereport_msg::set(stack::ereport_msg);

    vars::log_min_messages.install(GucVarAccessors {
        get: || config::log_min_messages().0,
        set: |v| config::set_log_min_messages(ErrorLevel(v)),
    });
    vars::client_min_messages.install(GucVarAccessors {
        get: || config::client_min_messages().0,
        set: |v| config::set_client_min_messages(ErrorLevel(v)),
    });
    vars::log_min_error_statement.install(GucVarAccessors {
        get: || config::log_min_error_statement().0,
        set: |v| config::set_log_min_error_statement(ErrorLevel(v)),
    });
    vars::Log_error_verbosity.install(GucVarAccessors {
        get: || config::log_error_verbosity() as i32,
        set: |v| {
            config::set_log_error_verbosity(match v {
                0 => PGErrorVerbosity::Terse,
                1 => PGErrorVerbosity::Default,
                2 => PGErrorVerbosity::Verbose,
                // The GUC core validates against log_error_verbosity_options
                // before assigning, so no other value can reach the store.
                other => panic!("invalid Log_error_verbosity value {other}"),
            })
        },
    });
    vars::Log_line_prefix.install(GucVarAccessors {
        get: config::log_line_prefix_format,
        set: config::set_log_line_prefix,
    });
    vars::syslog_sequence_numbers.install(GucVarAccessors {
        get: config::syslog_sequence_numbers,
        set: config::set_syslog_sequence_numbers,
    });
    vars::syslog_split_messages.install(GucVarAccessors {
        get: config::syslog_split_messages,
        set: config::set_syslog_split_messages,
    });
    vars::ExitOnAnyError.install(GucVarAccessors {
        get: config::exit_on_any_error,
        set: config::set_exit_on_any_error,
    });
    vars::Log_destination_string.install(GucVarAccessors {
        get: config::log_destination_string,
        set: config::set_log_destination_string,
    });
    vars::syslog_facility.install(GucVarAccessors {
        get: config::syslog_facility,
        set: config::assign_syslog_facility,
    });

    hooks::check_backtrace_functions.install(|newval, extra, _source| {
        // backtrace_functions boots to "" (never NULL).
        let Some(value) = newval.as_deref() else {
            return Ok(true);
        };
        let list = config::check_backtrace_functions(value)?;
        *extra = list.map(|l| Box::new(l) as GucHookExtra);
        Ok(true)
    });
    hooks::assign_backtrace_functions.install(|_newval, extra| {
        config::assign_backtrace_functions(
            extra
                .and_then(|e| e.downcast_ref::<config::BacktraceFunctionList>())
                .cloned(),
        )
    });
    hooks::check_log_destination.install(|newval, extra, _source| {
        // log_destination boots to "stderr" (never NULL).
        let Some(value) = newval.as_deref() else {
            return Ok(true);
        };
        let dest = config::check_log_destination(value)?;
        *extra = Some(Box::new(dest));
        Ok(true)
    });
    hooks::assign_log_destination.install(|_newval, extra| {
        let dest = extra
            .and_then(|e| e.downcast_ref::<i32>())
            .copied()
            .expect("assign_log_destination requires the extra payload from check_log_destination");
        config::assign_log_destination(dest);
    });
    hooks::assign_syslog_ident.install(|newval, _extra| {
        // syslog_ident boots to "postgres" (never NULL).
        if let Some(value) = newval {
            config::assign_syslog_ident(value);
        }
    });
    hooks::assign_syslog_facility.install(|newval, _extra| config::assign_syslog_facility(newval));
}

#[cfg(test)]
mod tests;
