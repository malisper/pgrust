mod builder;
mod config;
mod raw_transfer;
mod report;
mod sink;
mod soft;
mod sqlstate;
mod stack;
mod value;

pub use builder::{
    elog, ereport, err_generic_string, errbacktrace, errcode, errcode_for_file_access,
    errcode_for_socket_access, errcontext_msg, errdetail, errdetail_internal, errdetail_log,
    errdetail_log_plural, errdetail_plural, errfinish, errhidecontext, errhidestmt, errhint,
    errhint_internal, errhint_plural, errmsg, errmsg_internal, errmsg_plural, errposition,
    errstart, errstart_cold, internalerrposition, internalerrquery, message_level_is_interesting,
    ErrorBuilder,
};
pub use config::{
    assign_backtrace_functions, assign_log_destination, assign_syslog_facility,
    assign_syslog_ident, check_backtrace_functions, check_log_destination, error_log_config,
    matches_backtrace_functions, BacktraceFunctionList, ErrorLogConfig, LogDestination,
};
pub use pgrust_pg_ffi::{
    errcode_is_category, errcode_to_category, make_sqlstate, unpack_sqlstate, ErrorField,
    ErrorLevel, PgrustErrorData, SqlState, COMMERROR, DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5,
    ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_AMBIGUOUS_FUNCTION,
    ERRCODE_BAD_COPY_FILE_FORMAT, ERRCODE_CANNOT_COERCE, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
    ERRCODE_CHECK_VIOLATION, ERRCODE_COLLATION_MISMATCH, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_CONNECTION_FAILURE, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DATA_CORRUPTED,
    ERRCODE_DATA_EXCEPTION, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_DISK_FULL,
    ERRCODE_DUPLICATE_DATABASE, ERRCODE_DUPLICATE_FILE, ERRCODE_DUPLICATE_FUNCTION,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_DUPLICATE_PSTATEMENT, ERRCODE_DUPLICATE_SCHEMA,
    ERRCODE_DUPLICATE_TABLE, ERRCODE_EXCLUSION_VIOLATION,
    ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_FILE_NAME_TOO_LONG, ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    ERRCODE_IDLE_SESSION_TIMEOUT, ERRCODE_INDEX_CORRUPTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INSUFFICIENT_RESOURCES, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE,
    ERRCODE_INVALID_ARGUMENT_FOR_NTILE, ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_CURSOR_NAME,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_INVALID_GRANTOR, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_INVALID_NAME, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_PSTATEMENT_DEFINITION, ERRCODE_INVALID_RECURSION,
    ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_INVALID_TRANSACTION_STATE,
    ERRCODE_IN_FAILED_SQL_TRANSACTION, ERRCODE_IO_ERROR, ERRCODE_LOCK_NOT_AVAILABLE,
    ERRCODE_NAME_TOO_LONG, ERRCODE_NO_ACTIVE_SQL_TRANSACTION, ERRCODE_NO_DATA,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION, ERRCODE_QUERY_CANCELED,
    ERRCODE_READ_ONLY_SQL_TRANSACTION, ERRCODE_RESERVED_NAME,
    ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED, ERRCODE_STATEMENT_TOO_COMPLEX,
    ERRCODE_SUCCESSFUL_COMPLETION, ERRCODE_SYNTAX_ERROR, ERRCODE_SYSTEM_ERROR,
    ERRCODE_S_E_INVALID_SPECIFICATION, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_TOO_MANY_CONNECTIONS, ERRCODE_TRANSACTION_TIMEOUT, ERRCODE_T_R_SERIALIZATION_FAILURE,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_CURSOR, ERRCODE_UNDEFINED_DATABASE,
    ERRCODE_UNDEFINED_FILE, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_PSTATEMENT, ERRCODE_UNDEFINED_SCHEMA, ERRCODE_UNDEFINED_TABLE,
    ERRCODE_UNIQUE_VIOLATION, ERRCODE_UNTRANSLATABLE_CHARACTER, ERRCODE_WARNING,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, FATAL, INFO, LOG, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_EVENTLOG, LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR,
    LOG_DESTINATION_SYSLOG, LOG_SERVER_ONLY, NOTICE, PANIC, PGERROR, PGWARNING,
    PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_DATATYPE_NAME, PG_DIAG_SCHEMA_NAME,
    PG_DIAG_TABLE_NAME, WARNING, WARNING_CLIENT_ONLY,
};
pub use raw_transfer::{pgrust_error_data_free_owned_fields, pgrust_error_data_from_error};
pub use report::{
    append_with_tabs, check_log_of_query, err_sendstring, error_severity, format_elog_string,
    format_error_report, get_backend_type_for_log, get_formatted_log_time,
    get_formatted_start_time, log_line_prefix, log_status_format, pre_format_elog_string,
    reset_formatted_start_time, send_message_to_frontend, send_message_to_server_log,
    unpack_sql_state, vwrite_stderr, write_console, write_pipe_chunks_for_text, write_stderr,
    write_syslog, DebugFileOpen, DebugFileOpenResult, EmitErrorReport, GetErrorContextStack,
};
pub use sink::{
    backend_log_context, emit_error_report, emit_report, set_backend_log_context,
    set_frontend_error_sink, set_report_sink, set_server_log_sink, set_syslogger_sink,
    write_pipe_chunks, BackendLogContext, FrontendErrorSink, ReportSink, ServerLogSink,
    SysloggerSink,
};
pub use soft::{errsave, errsave_start, ErrSaveFrame, ErrSaveStart, SoftErrorContext};
pub use sqlstate::{default_sqlstate_for_level, severity};
pub use stack::{
    geterrcode, geterrposition, getinternalerrposition, in_error_recursion_trouble, pg_re_throw,
    set_errcontext_domain, CopiedErrorData, CopyErrorData, ErrorStackFrame, FlushErrorState,
    FreeErrorData, ReThrowError, ThrowErrorData, ERRORDATA_STACK_SIZE,
};
pub use value::{ErrorLocation, PgError};

pub type PgResult<T> = Result<T, PgError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    const UNDEFINED_TABLE: SqlState = make_sqlstate(*b"42P01");

    static CAPTURED: Mutex<Vec<PgError>> = Mutex::new(Vec::new());
    static REPORT_TEST_LOCK: Mutex<()> = Mutex::new(());
    static SERVER_LOG_CAPTURED: Mutex<Vec<(PgError, String)>> = Mutex::new(Vec::new());
    static FRONTEND_CAPTURED: Mutex<Vec<PgError>> = Mutex::new(Vec::new());
    static SYSLOGGER_CAPTURED: Mutex<Vec<(Vec<u8>, LogDestination)>> = Mutex::new(Vec::new());

    struct CapturingServerLogSink;
    struct CapturingFrontendErrorSink;
    struct CapturingSysloggerSink;
    struct CapturingBackendLogContext;

    static CAPTURING_SERVER_LOG_SINK: CapturingServerLogSink = CapturingServerLogSink;
    static CAPTURING_FRONTEND_ERROR_SINK: CapturingFrontendErrorSink = CapturingFrontendErrorSink;
    static CAPTURING_SYSLOGGER_SINK: CapturingSysloggerSink = CapturingSysloggerSink;
    static CAPTURING_BACKEND_LOG_CONTEXT: CapturingBackendLogContext = CapturingBackendLogContext;

    impl ServerLogSink for CapturingServerLogSink {
        fn write_server_log(&self, error: &PgError, formatted: &str) -> PgResult<()> {
            SERVER_LOG_CAPTURED
                .lock()
                .unwrap()
                .push((error.clone(), formatted.to_owned()));
            Ok(())
        }
    }

    impl FrontendErrorSink for CapturingFrontendErrorSink {
        fn send_error_to_frontend(&self, error: &PgError) -> PgResult<()> {
            FRONTEND_CAPTURED.lock().unwrap().push(error.clone());
            Ok(())
        }
    }

    impl SysloggerSink for CapturingSysloggerSink {
        fn write_pipe_chunks(&self, bytes: &[u8], destination: LogDestination) -> PgResult<()> {
            SYSLOGGER_CAPTURED
                .lock()
                .unwrap()
                .push((bytes.to_vec(), destination));
            Ok(())
        }
    }

    impl BackendLogContext for CapturingBackendLogContext {
        fn backend_type(&self) -> Option<&str> {
            Some("client backend")
        }

        fn application_name(&self) -> Option<&str> {
            Some("psql")
        }

        fn database_name(&self) -> Option<&str> {
            Some("postgres")
        }

        fn user_name(&self) -> Option<&str> {
            Some("postgres")
        }

        fn query_id(&self) -> Option<i64> {
            Some(42)
        }

        fn query_string(&self) -> Option<&str> {
            Some("select 1")
        }

        fn log_min_error_statement(&self) -> ErrorLevel {
            WARNING
        }

        fn top_transaction_id(&self) -> Option<u32> {
            Some(99)
        }

        fn process_id(&self) -> Option<u32> {
            Some(1234)
        }

        fn parallel_leader_process_id(&self) -> Option<u32> {
            Some(1200)
        }

        fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
            Some((3, 44))
        }

        fn remote_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn remote_port(&self) -> Option<&str> {
            Some("5432")
        }

        fn local_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn session_start_time(&self) -> Option<std::time::SystemTime> {
            Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000))
        }

        fn ps_display(&self) -> Option<&str> {
            Some("postgres: session")
        }
    }

    fn capture_report(error: &PgError) {
        CAPTURED.lock().unwrap().push(error.clone());
    }

    #[test]
    fn sqlstate_unpacks_to_postgres_text() {
        assert_eq!(unpack_sqlstate(UNDEFINED_TABLE), *b"42P01");
        assert!(!errcode_is_category(UNDEFINED_TABLE));
    }

    #[test]
    fn legacy_helpers_preserve_error_fields() {
        let error = errhint(
            errdetail(errcode(UNDEFINED_TABLE, "missing relation"), "detail"),
            "hint",
        )
        .with_location("file.rs", 12, "func");

        assert_eq!(error.level(), ERROR);
        assert_eq!(error.sqlstate(), UNDEFINED_TABLE);
        assert_eq!(error.message(), "missing relation");
        assert_eq!(error.detail(), Some("detail"));
        assert_eq!(error.hint(), Some("hint"));
        assert_eq!(error.location().unwrap().lineno, 12);
    }

    #[test]
    fn builder_preserves_full_error_shape() {
        let error = ereport(ERROR)
            .errcode(UNDEFINED_TABLE)
            .errmsg("missing relation")
            .errdetail("detail")
            .errdetail_log("server detail")
            .errhint("hint")
            .errcontext_msg("context")
            .errbacktrace("trace")
            .errhidestmt(true)
            .errhidecontext(true)
            .with_saved_errno(2)
            .errposition(11)
            .internalerrposition(22)
            .internalerrquery("select 1")
            .with_message_id("message id")
            .with_domain("postgres")
            .with_context_domain("postgres")
            .err_generic_string(PG_DIAG_SCHEMA_NAME, "public")
            .unwrap()
            .err_generic_string(PG_DIAG_TABLE_NAME, "users")
            .unwrap()
            .err_generic_string(PG_DIAG_COLUMN_NAME, "id")
            .unwrap()
            .err_generic_string(PG_DIAG_DATATYPE_NAME, "integer")
            .unwrap()
            .err_generic_string(PG_DIAG_CONSTRAINT_NAME, "users_pkey")
            .unwrap()
            .into_error()
            .with_location("file.rs", 12, "func");

        assert_eq!(error.level(), ERROR);
        assert_eq!(error.sqlstate(), UNDEFINED_TABLE);
        assert_eq!(error.message(), "missing relation");
        assert_eq!(error.detail(), Some("detail"));
        assert_eq!(error.detail_log(), Some("server detail"));
        assert_eq!(error.hint(), Some("hint"));
        assert_eq!(error.context(), Some("context"));
        assert_eq!(error.backtrace(), Some("trace"));
        assert_eq!(error.message_id(), Some("message id"));
        assert_eq!(error.domain(), Some("postgres"));
        assert_eq!(error.context_domain(), Some("postgres"));
        assert!(error.hide_statement());
        assert!(error.hide_context());
        assert_eq!(error.saved_errno(), Some(2));
        assert_eq!(error.cursor_position(), Some(11));
        assert_eq!(error.internal_position(), Some(22));
        assert_eq!(error.internal_query(), Some("select 1"));
        assert_eq!(error.schema_name(), Some("public"));
        assert_eq!(error.table_name(), Some("users"));
        assert_eq!(error.column_name(), Some("id"));
        assert_eq!(error.datatype_name(), Some("integer"));
        assert_eq!(error.constraint_name(), Some("users_pkey"));
        assert_eq!(error.location().unwrap().lineno, 12);
    }

    #[test]
    fn errfinish_returns_errors_and_emits_lower_levels() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        CAPTURED.lock().unwrap().clear();
        let previous = set_report_sink(Some(capture_report));

        let warning = ereport(WARNING).errmsg("watch out");
        assert_eq!(
            errfinish(warning, ErrorLocation::new("file", 1, "func")),
            Ok(())
        );

        let error = errfinish(
            ereport(ERROR).errmsg("stop"),
            ErrorLocation::new("file", 2, "func"),
        )
        .unwrap_err();

        set_report_sink(previous);

        assert_eq!(error.message(), "stop");
        assert_eq!(error.location().unwrap().lineno, 2);
        assert_eq!(CAPTURED.lock().unwrap()[0].message(), "watch out");
    }

    #[test]
    fn plural_helpers_select_postgres_style_text() {
        assert_eq!(
            ereport(ERROR)
                .errmsg_plural("%d row", "%d rows", 1)
                .into_error()
                .message(),
            "%d row"
        );
        assert_eq!(
            ereport(ERROR)
                .errhint_plural("one hint", "many hints", 3)
                .into_error()
                .hint(),
            Some("many hints")
        );
    }

    #[test]
    fn saved_errno_replaces_percent_m() {
        let error = ereport(ERROR)
            .with_saved_errno(libc::ENOENT)
            .errmsg("could not open file: %m")
            .into_error();

        assert!(error.message().starts_with("could not open file: "));
        assert!(!error.message().contains("%m"));
    }

    #[test]
    fn errno_helpers_match_postgres_categories() {
        assert_eq!(
            errcode_for_file_access(libc::EACCES),
            ERRCODE_INSUFFICIENT_PRIVILEGE
        );
        assert_eq!(
            errcode_for_file_access(libc::ENOENT),
            ERRCODE_UNDEFINED_FILE
        );
        assert_eq!(
            errcode_for_file_access(libc::EEXIST),
            ERRCODE_DUPLICATE_FILE
        );
        assert_eq!(errcode_for_file_access(libc::ENOSPC), ERRCODE_DISK_FULL);
        assert_eq!(
            errcode_for_socket_access(libc::ECONNRESET),
            ERRCODE_CONNECTION_FAILURE
        );
        assert_eq!(
            errcode_for_socket_access(libc::EINVAL),
            ERRCODE_INTERNAL_ERROR
        );
    }

    #[test]
    fn soft_error_context_saves_or_returns() {
        let mut context = SoftErrorContext::new(true);
        assert_eq!(errsave(Some(&mut context), PgError::error("soft")), Ok(()));
        assert_eq!(context.error().unwrap().message(), "soft");

        let error = errsave(None, PgError::error("hard")).unwrap_err();
        assert_eq!(error.message(), "hard");
    }

    #[test]
    fn errsave_without_details_only_marks_error_occurred() {
        FlushErrorState();
        let mut context = SoftErrorContext::new(false);

        let result = errsave_start(Some(&mut context), Some("postgres")).unwrap();

        assert!(matches!(result, ErrSaveStart::Skipped));
        assert!(context.error_occurred());
        assert!(context.error().is_none());
        assert_eq!(
            geterrcode().unwrap_err().message(),
            "errstart was not called"
        );
    }

    #[test]
    fn errsave_with_details_saves_completed_error() {
        FlushErrorState();
        let mut context = SoftErrorContext::new(true);

        let ErrSaveStart::Active(mut frame) =
            errsave_start(Some(&mut context), Some("postgres")).unwrap()
        else {
            panic!("details-wanted soft error should create a frame");
        };
        frame
            .errcode(UNDEFINED_TABLE)
            .unwrap()
            .errmsg("missing relation")
            .unwrap()
            .errdetail("detail")
            .unwrap()
            .errhint("hint")
            .unwrap()
            .errposition(7)
            .unwrap()
            .internalerrposition(11)
            .unwrap()
            .internalerrquery("select 1")
            .unwrap()
            .set_errcontext_domain("postgres")
            .unwrap();

        assert_eq!(geterrcode().unwrap(), UNDEFINED_TABLE);
        assert_eq!(geterrposition().unwrap(), 7);
        assert_eq!(getinternalerrposition().unwrap(), 11);

        frame
            .finish(ErrorLocation::new("file.rs", 42, "func"))
            .unwrap();

        let error = context.error().unwrap();
        assert!(context.error_occurred());
        assert_eq!(error.level(), ERROR);
        assert_eq!(error.sqlstate(), UNDEFINED_TABLE);
        assert_eq!(error.message(), "missing relation");
        assert_eq!(error.detail(), Some("detail"));
        assert_eq!(error.hint(), Some("hint"));
        assert_eq!(error.cursor_position(), Some(7));
        assert_eq!(error.internal_position(), Some(11));
        assert_eq!(error.internal_query(), Some("select 1"));
        assert_eq!(error.location().unwrap().lineno, 42);
        assert_eq!(
            geterrcode().unwrap_err().message(),
            "errstart was not called"
        );
    }

    #[test]
    fn errsave_without_context_returns_hard_error_on_finish() {
        FlushErrorState();
        let ErrSaveStart::Active(mut frame) = errsave_start(None, Some("postgres")).unwrap() else {
            panic!("hard error path should create a frame");
        };

        frame.errmsg_internal("hard").unwrap();
        let error = frame
            .finish(ErrorLocation::new("file.rs", 9, "func"))
            .unwrap_err();

        assert_eq!(error.level(), ERROR);
        assert_eq!(error.message(), "hard");
        assert_eq!(error.location().unwrap().lineno, 9);
    }

    #[test]
    fn flush_error_state_clears_active_frames() {
        FlushErrorState();
        let ErrSaveStart::Active(mut frame) = errsave_start(None, Some("postgres")).unwrap() else {
            panic!("hard error path should create a frame");
        };
        frame.errcode(UNDEFINED_TABLE).unwrap();
        assert_eq!(geterrcode().unwrap(), UNDEFINED_TABLE);

        FlushErrorState();

        assert_eq!(
            geterrcode().unwrap_err().message(),
            "errstart was not called"
        );
    }

    #[test]
    fn error_stack_is_lifo_and_tracks_recursion_trouble() {
        FlushErrorState();
        let mut frames = Vec::new();
        for index in 0..3 {
            let mut frame = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap();
            frame.errposition(index + 1).unwrap();
            frames.push(frame);
        }

        assert!(in_error_recursion_trouble());
        assert_eq!(geterrposition().unwrap(), 3);
        let popped = frames
            .pop()
            .unwrap()
            .finish(ErrorLocation::new("f", 1, "g"))
            .unwrap();
        assert_eq!(popped.cursor_position(), Some(3));
        assert_eq!(geterrposition().unwrap(), 2);

        FlushErrorState();
    }

    #[test]
    fn error_stack_overflow_returns_panic_error() {
        FlushErrorState();
        let mut frames = Vec::new();
        for _ in 0..ERRORDATA_STACK_SIZE {
            frames.push(ErrorStackFrame::push(ERROR, Some("postgres")).unwrap());
        }

        let error = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap_err();

        assert_eq!(error.level(), PANIC);
        assert_eq!(error.message(), "ERRORDATA_STACK_SIZE exceeded");
        FlushErrorState();
    }

    #[test]
    fn copy_error_data_clones_current_error() {
        FlushErrorState();
        let mut frame = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap();
        frame
            .errcode(UNDEFINED_TABLE)
            .unwrap()
            .errmsg("missing relation")
            .unwrap()
            .errdetail("detail")
            .unwrap()
            .errhint("hint")
            .unwrap()
            .errposition(7)
            .unwrap();

        let copied = CopyErrorData().unwrap();
        let original = frame
            .finish(ErrorLocation::new("file.rs", 42, "func"))
            .unwrap();

        assert_eq!(copied.error().sqlstate(), UNDEFINED_TABLE);
        assert_eq!(copied.error().message(), "missing relation");
        assert_eq!(copied.error().detail(), Some("detail"));
        assert_eq!(copied.error().hint(), Some("hint"));
        assert_eq!(copied.error().cursor_position(), Some(7));
        assert_eq!(original.location().unwrap().lineno, 42);
        assert_eq!(
            geterrcode().unwrap_err().message(),
            "errstart was not called"
        );
    }

    #[test]
    fn free_error_data_consumes_owned_copy() {
        let copied = CopiedErrorData::new(PgError::error("owned"));

        FreeErrorData(copied);
    }

    #[test]
    fn throw_error_data_returns_copied_error() {
        let copied = CopiedErrorData::new(
            PgError::error("soft failure")
                .with_sqlstate(UNDEFINED_TABLE)
                .with_detail("detail"),
        );

        let error = ThrowErrorData::<()>(copied).unwrap_err();

        assert_eq!(error.sqlstate(), UNDEFINED_TABLE);
        assert_eq!(error.message(), "soft failure");
        assert_eq!(error.detail(), Some("detail"));
    }

    #[test]
    fn rethrow_error_rejects_non_error_levels() {
        let copied = CopiedErrorData::new(PgError::warning("warning"));

        let error = ReThrowError::<()>(copied).unwrap_err();

        assert_eq!(error.level(), PANIC);
        assert_eq!(
            error.message(),
            "ReThrowError called with non-ERROR error data"
        );
    }

    #[test]
    fn rethrow_error_returns_copied_error() {
        let copied = CopiedErrorData::new(PgError::error("hard failure"));

        let error = ReThrowError::<()>(copied).unwrap_err();

        assert_eq!(error.level(), ERROR);
        assert_eq!(error.message(), "hard failure");
    }

    #[test]
    fn pg_re_throw_returns_current_error() {
        FlushErrorState();
        let mut frame = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap();
        frame.errmsg("current failure").unwrap();

        let error = pg_re_throw::<()>().unwrap_err();

        assert_eq!(error.level(), ERROR);
        assert_eq!(error.message(), "current failure");
        assert_eq!(
            geterrcode().unwrap_err().message(),
            "errstart was not called"
        );
    }

    #[test]
    fn pg_re_throw_without_current_error_returns_panic() {
        FlushErrorState();

        let error = pg_re_throw::<()>().unwrap_err();

        assert_eq!(error.level(), PANIC);
        assert_eq!(error.message(), "pg_re_throw tried to return");
    }

    #[test]
    fn emit_error_report_emits_current_stack_error() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        FlushErrorState();
        CAPTURED.lock().unwrap().clear();
        SERVER_LOG_CAPTURED.lock().unwrap().clear();
        FRONTEND_CAPTURED.lock().unwrap().clear();
        let previous = set_report_sink(Some(capture_report));
        let previous_server = set_server_log_sink(Some(&CAPTURING_SERVER_LOG_SINK));
        let previous_frontend = set_frontend_error_sink(Some(&CAPTURING_FRONTEND_ERROR_SINK));
        let mut frame = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap();
        frame.errmsg("current report").unwrap();

        EmitErrorReport().unwrap();

        set_report_sink(previous);
        set_server_log_sink(previous_server);
        set_frontend_error_sink(previous_frontend);
        drop(frame);

        let captured = CAPTURED.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].message(), "current report");
        drop(captured);

        let server_captured = SERVER_LOG_CAPTURED.lock().unwrap();
        assert_eq!(server_captured.len(), 1);
        assert_eq!(server_captured[0].0.message(), "current report");
        assert_eq!(server_captured[0].1, "ERROR:  current report");
        drop(server_captured);

        let frontend_captured = FRONTEND_CAPTURED.lock().unwrap();
        assert_eq!(frontend_captured.len(), 1);
        assert_eq!(frontend_captured[0].message(), "current report");
    }

    #[test]
    fn get_error_context_stack_returns_current_context() {
        FlushErrorState();
        let mut frame = ErrorStackFrame::push(ERROR, Some("postgres")).unwrap();
        frame
            .errcontext_msg("first context")
            .unwrap()
            .errcontext_msg("second context")
            .unwrap();

        assert_eq!(
            GetErrorContextStack().unwrap(),
            "first context\nsecond context"
        );
    }

    #[test]
    fn error_report_helpers_match_postgres_strings() {
        assert_eq!(error_severity(DEBUG1), "DEBUG");
        assert_eq!(error_severity(LOG_SERVER_ONLY), "LOG");
        assert_eq!(error_severity(WARNING_CLIENT_ONLY), "WARNING");
        assert_eq!(error_severity(ERROR), "ERROR");
        assert_eq!(unpack_sql_state(UNDEFINED_TABLE), "42P01");

        let report = format_error_report(
            &PgError::error("missing relation")
                .with_detail("detail")
                .with_hint("hint")
                .with_context("context"),
        );
        assert_eq!(
            report,
            "ERROR:  missing relation\nDETAIL:  detail\nHINT:  hint\nCONTEXT:  context"
        );
    }

    #[test]
    fn write_stderr_accepts_plain_text() {
        write_stderr("").unwrap();
    }

    #[test]
    fn syslogger_sink_receives_pipe_chunks() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        SYSLOGGER_CAPTURED.lock().unwrap().clear();
        let previous = set_syslogger_sink(Some(&CAPTURING_SYSLOGGER_SINK));

        write_pipe_chunks(b"hello", LogDestination(LOG_DESTINATION_STDERR)).unwrap();

        set_syslogger_sink(previous);

        let captured = SYSLOGGER_CAPTURED.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, b"hello");
        assert_eq!(captured[0].1, LogDestination(LOG_DESTINATION_STDERR));
    }

    #[test]
    fn backend_log_context_is_injectable() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        let previous = set_backend_log_context(Some(&CAPTURING_BACKEND_LOG_CONTEXT));

        let context = backend_log_context().unwrap();

        assert_eq!(context.backend_type(), Some("client backend"));
        assert_eq!(context.application_name(), Some("psql"));
        assert_eq!(context.database_name(), Some("postgres"));
        assert_eq!(context.user_name(), Some("postgres"));
        assert_eq!(context.query_id(), Some(42));

        set_backend_log_context(previous);
    }

    #[test]
    fn formatted_log_time_is_cached_and_start_time_can_be_reset() {
        let first = get_formatted_log_time();
        let second = get_formatted_log_time();

        assert_eq!(first, second);
        assert!(first.ends_with(" UTC"));
        assert!(first.contains('.'));

        reset_formatted_start_time();
        let start = get_formatted_start_time();
        assert!(start.ends_with(" UTC"));
        assert!(!start.contains('.'));
    }

    #[test]
    fn log_status_format_uses_injected_backend_context() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        let previous = set_backend_log_context(Some(&CAPTURING_BACKEND_LOG_CONTEXT));
        let error = PgError::error("boom").with_sqlstate(UNDEFINED_TABLE);

        let prefix = log_status_format("%a|%b|%u|%d|%p|%P|%v|%x|%e|%Q|%r|%h|%L|%i|%c", &error);

        assert_eq!(
            prefix,
            "psql|client backend|postgres|postgres|1234|1200|3/44|99|42P01|42|127.0.0.1(5432)|127.0.0.1|127.0.0.1|postgres: session|6553f100.4d2"
        );
        assert_eq!(log_line_prefix("%b:%e", &error), "client backend:42P01");
        assert_eq!(get_backend_type_for_log(), "client backend");

        set_backend_log_context(previous);
    }

    #[test]
    fn log_status_format_applies_padding_and_percent_escape() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        let previous = set_backend_log_context(Some(&CAPTURING_BACKEND_LOG_CONTEXT));
        let error = PgError::error("boom");

        assert_eq!(
            log_status_format("%8p|%-8p|%%", &error),
            "    1234|1234    |%"
        );

        set_backend_log_context(previous);
    }

    #[test]
    fn check_log_of_query_matches_level_hide_and_query_rules() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        let previous = set_backend_log_context(Some(&CAPTURING_BACKEND_LOG_CONTEXT));

        assert!(check_log_of_query(&PgError::warning("warn")));
        assert!(!check_log_of_query(&PgError::notice("notice")));
        assert!(!check_log_of_query(
            &PgError::warning("hidden").with_hide_statement(true)
        ));

        set_backend_log_context(previous);
        assert!(!check_log_of_query(&PgError::warning("warn")));
    }

    #[test]
    fn format_elog_string_replaces_percent_m_with_saved_errno() {
        pre_format_elog_string(libc::ENOENT, Some("postgres"));

        let formatted = format_elog_string("could not open file: %m");

        assert!(formatted.starts_with("could not open file: "));
        assert!(!formatted.contains("%m"));
    }

    #[test]
    fn server_log_helpers_format_and_dispatch() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        SERVER_LOG_CAPTURED.lock().unwrap().clear();
        let previous_server = set_server_log_sink(Some(&CAPTURING_SERVER_LOG_SINK));
        let previous_context = set_backend_log_context(Some(&CAPTURING_BACKEND_LOG_CONTEXT));
        let error = PgError::error("boom").with_detail("detail");

        let message = send_message_to_server_log(&error, "%p ");

        set_server_log_sink(previous_server);
        set_backend_log_context(previous_context);

        let message = message.unwrap();
        assert!(message.starts_with("1234 ERROR:  boom"));
        assert!(message.contains("DETAIL:  detail"));

        let captured = SERVER_LOG_CAPTURED.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0.message(), "boom");
        assert_eq!(captured[0].1, message);
    }

    #[test]
    fn append_with_tabs_inserts_tabs_after_newlines() {
        assert_eq!(append_with_tabs("a\nb\n"), "a\n\tb\n\t");
    }

    #[test]
    fn frontend_and_syslog_helpers_dispatch_through_sinks() {
        let _guard = REPORT_TEST_LOCK.lock().unwrap();
        FRONTEND_CAPTURED.lock().unwrap().clear();
        SERVER_LOG_CAPTURED.lock().unwrap().clear();
        let previous_frontend = set_frontend_error_sink(Some(&CAPTURING_FRONTEND_ERROR_SINK));
        let previous_server = set_server_log_sink(Some(&CAPTURING_SERVER_LOG_SINK));

        send_message_to_frontend(&PgError::error("frontend")).unwrap();
        write_syslog(LOG, "syslog").unwrap();

        set_frontend_error_sink(previous_frontend);
        set_server_log_sink(previous_server);

        assert_eq!(FRONTEND_CAPTURED.lock().unwrap()[0].message(), "frontend");
        assert_eq!(
            SERVER_LOG_CAPTURED.lock().unwrap()[0].0.message(),
            "frontend"
        );
        assert_eq!(SERVER_LOG_CAPTURED.lock().unwrap()[1].0.message(), "syslog");
    }

    #[test]
    fn err_sendstring_appends_to_string_buffer() {
        let mut buffer = String::from("a");

        err_sendstring(&mut buffer, "b");

        assert_eq!(buffer, "ab");
    }

    #[test]
    fn backtrace_functions_check_strips_whitespace_and_splits_on_commas() {
        let parsed = check_backtrace_functions(" foo,bar_baz,\nqux9\t")
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed.functions(),
            &["foo".to_owned(), "bar_baz".to_owned(), "qux9".to_owned()]
        );
        assert!(parsed.matches("bar_baz"));
        assert!(!parsed.matches(""));
    }

    #[test]
    fn backtrace_functions_rejects_invalid_characters() {
        let error = check_backtrace_functions("valid-name").unwrap_err();

        assert_eq!(error.message(), "Invalid character.");
    }

    #[test]
    fn assign_backtrace_functions_updates_config() {
        let parsed = check_backtrace_functions("foo,bar").unwrap();

        assign_backtrace_functions("foo,bar", parsed).unwrap();

        assert!(matches_backtrace_functions("foo"));
        assert!(matches_backtrace_functions("bar"));
        assert!(!matches_backtrace_functions("baz"));
    }

    #[test]
    fn log_destination_check_accepts_postgres_keywords() {
        let parsed = check_log_destination("stderr, csvlog, JSONLOG, syslog").unwrap();

        assert_eq!(
            parsed,
            LogDestination(
                LOG_DESTINATION_STDERR
                    | LOG_DESTINATION_CSVLOG
                    | LOG_DESTINATION_JSONLOG
                    | LOG_DESTINATION_SYSLOG
            )
        );
    }

    #[test]
    fn log_destination_check_handles_quoted_identifiers_and_syntax_errors() {
        assert_eq!(
            check_log_destination("\"stderr\", csvlog").unwrap(),
            LogDestination(LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG)
        );
        assert_eq!(
            check_log_destination("stderr,,csvlog")
                .unwrap_err()
                .message(),
            "List syntax is invalid."
        );
    }

    #[test]
    fn log_destination_check_rejects_unknown_keyword() {
        let error = check_log_destination("stderr,unknown").unwrap_err();

        assert_eq!(error.message(), "Unrecognized key word: \"unknown\".");
    }

    #[test]
    fn log_destination_assignment_updates_config() {
        let destination = check_log_destination("jsonlog").unwrap();

        assign_log_destination("jsonlog", destination).unwrap();

        assert_eq!(error_log_config().log_destination, destination);
    }

    #[test]
    fn syslog_assignments_update_config_and_close_open_state() {
        assign_syslog_ident("postgres").unwrap();
        assign_syslog_facility(3).unwrap();

        let config = error_log_config();
        assert_eq!(config.syslog_ident.as_deref(), Some("postgres"));
        assert_eq!(config.syslog_facility, 3);
        assert!(!config.syslog_open);
    }

    #[test]
    fn transfer_round_trips_owned_strings() {
        let error = PgError::error("primary")
            .with_detail("detail")
            .with_detail_log("server detail")
            .with_hint("hint")
            .with_context("context")
            .with_backtrace("trace")
            .with_message_id("message id")
            .with_domain("postgres")
            .with_context_domain("postgres")
            .with_hide_statement(true)
            .with_hide_context(true)
            .with_location("file", 7, "function")
            .with_saved_errno(2)
            .with_cursor_position(11)
            .with_internal_position(22)
            .with_internal_query("select 1")
            .with_error_field(PG_DIAG_SCHEMA_NAME, "public")
            .unwrap()
            .with_error_field(PG_DIAG_TABLE_NAME, "users")
            .unwrap()
            .with_error_field(PG_DIAG_COLUMN_NAME, "id")
            .unwrap()
            .with_error_field(PG_DIAG_DATATYPE_NAME, "integer")
            .unwrap()
            .with_error_field(PG_DIAG_CONSTRAINT_NAME, "users_pkey")
            .unwrap();

        let raw = pgrust_error_data_from_error(&error);
        let copied = unsafe { PgError::from_raw_transfer(raw) };

        assert_eq!(copied.message(), "primary");
        assert_eq!(copied.detail(), Some("detail"));
        assert_eq!(copied.detail_log(), Some("server detail"));
        assert_eq!(copied.hint(), Some("hint"));
        assert_eq!(copied.context(), Some("context"));
        assert_eq!(copied.backtrace(), Some("trace"));
        assert_eq!(copied.message_id(), Some("message id"));
        assert_eq!(copied.domain(), Some("postgres"));
        assert_eq!(copied.context_domain(), Some("postgres"));
        assert!(copied.hide_statement());
        assert!(copied.hide_context());
        assert_eq!(copied.location().unwrap().filename.as_deref(), Some("file"));
        assert_eq!(copied.saved_errno(), Some(2));
        assert_eq!(copied.cursor_position(), Some(11));
        assert_eq!(copied.internal_position(), Some(22));
        assert_eq!(copied.internal_query(), Some("select 1"));
        assert_eq!(copied.schema_name(), Some("public"));
        assert_eq!(copied.table_name(), Some("users"));
        assert_eq!(copied.column_name(), Some("id"));
        assert_eq!(copied.datatype_name(), Some("integer"));
        assert_eq!(copied.constraint_name(), Some("users_pkey"));
    }

    #[test]
    fn generic_error_field_rejects_unknown_field() {
        let error = PgError::error("base").with_error_field(ErrorField(b'x' as i32), "value");

        assert_eq!(
            error.unwrap_err().message(),
            "unsupported ErrorData field id: 120"
        );
    }
}
