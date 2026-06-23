use std::sync::Mutex;

use ::types_dest::CommandDest;
use types_error::{
    make_sqlstate, ErrorLevel, ErrorLocation, PgError, DEBUG1, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_DISK_FULL, ERRCODE_DUPLICATE_FILE, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INTERNAL_ERROR, ERRCODE_UNDEFINED_FILE, ERROR, FATAL, INFO, LOG, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, LOG_SERVER_ONLY,
    NOTICE, PANIC, WARNING, WARNING_CLIENT_ONLY,
};

use super::*;

const UNDEFINED_TABLE: SqlState = make_sqlstate(*b"42P01");

/// Serializes tests that touch the process-global config / provider / hook.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn set_guc(log_min: ErrorLevel, client_min: ErrorLevel, dest: CommandDest, auth: bool) {
    config::set_log_min_messages(log_min);
    config::set_client_min_messages(client_min);
    config::set_where_to_send_output(dest);
    config::set_client_auth_in_progress(auth);
}

fn reset_guc() {
    set_guc(WARNING, NOTICE, CommandDest::None, false);
}

#[test]
fn is_log_level_output_matches_postgres_log_ordering() {
    // LOG / LOG_SERVER_ONLY go to the server log when log_min_level is LOG or
    // anything at or below ERROR (they sort between ERROR and FATAL).
    assert!(is_log_level_output(LOG, LOG));
    assert!(is_log_level_output(LOG, ERROR));
    assert!(is_log_level_output(LOG, WARNING));
    assert!(is_log_level_output(LOG_SERVER_ONLY, DEBUG1));
    assert!(!is_log_level_output(LOG, FATAL));
    assert!(!is_log_level_output(LOG_SERVER_ONLY, PANIC));

    // WARNING_CLIENT_ONLY is never written to the server log.
    assert!(!is_log_level_output(WARNING_CLIENT_ONLY, DEBUG1));
    assert!(!is_log_level_output(WARNING_CLIENT_ONLY, ERROR));

    // When log_min_level is LOG and elevel != LOG, only FATAL and above pass.
    assert!(is_log_level_output(FATAL, LOG));
    assert!(is_log_level_output(PANIC, LOG));
    assert!(!is_log_level_output(ERROR, LOG));
    assert!(!is_log_level_output(WARNING, LOG));

    // Neither is LOG: plain >= test.
    assert!(is_log_level_output(ERROR, WARNING));
    assert!(is_log_level_output(WARNING, WARNING));
    assert!(!is_log_level_output(NOTICE, WARNING));
}

#[test]
fn should_output_to_server_honors_log_min_messages() {
    let _guard = lock();

    reset_guc();
    assert!(should_output_to_server(WARNING));
    assert!(should_output_to_server(ERROR));
    assert!(!should_output_to_server(NOTICE));
    assert!(should_output_to_server(LOG));

    set_guc(ERROR, NOTICE, CommandDest::None, false);
    assert!(!should_output_to_server(WARNING));
    assert!(should_output_to_server(ERROR));
    assert!(should_output_to_server(LOG));

    set_guc(FATAL, NOTICE, CommandDest::None, false);
    assert!(!should_output_to_server(LOG));
    assert!(should_output_to_server(FATAL));

    reset_guc();
}

#[test]
fn should_output_to_client_honors_dest_auth_and_client_min_messages() {
    let _guard = lock();

    set_guc(WARNING, NOTICE, CommandDest::Remote, false);
    assert!(should_output_to_client(NOTICE));
    assert!(should_output_to_client(ERROR));

    // INFO is always sent to the client regardless of client_min_messages.
    set_guc(ERROR, ERROR, CommandDest::Remote, false);
    assert!(should_output_to_client(INFO));
    assert!(!should_output_to_client(NOTICE));
    assert!(should_output_to_client(ERROR));

    // LOG_SERVER_ONLY is never sent to the client.
    assert!(!should_output_to_client(LOG_SERVER_ONLY));

    // During authentication only ERROR and above reach the client.
    set_guc(NOTICE, NOTICE, CommandDest::Remote, true);
    assert!(!should_output_to_client(NOTICE));
    assert!(!should_output_to_client(INFO));
    assert!(should_output_to_client(ERROR));

    // When output is not directed at a remote client, nothing is sent.
    set_guc(NOTICE, NOTICE, CommandDest::None, false);
    assert!(!should_output_to_client(ERROR));
    assert!(!should_output_to_client(NOTICE));

    reset_guc();
}

#[test]
fn message_level_is_interesting_matches_errstart_decision() {
    let _guard = lock();

    set_guc(PANIC, ERROR, CommandDest::None, false);
    assert!(message_level_is_interesting(ERROR));
    assert!(message_level_is_interesting(FATAL));
    assert!(!message_level_is_interesting(NOTICE));
    assert!(!message_level_is_interesting(WARNING));

    set_guc(PANIC, NOTICE, CommandDest::Remote, false);
    assert!(message_level_is_interesting(NOTICE));

    set_guc(NOTICE, ERROR, CommandDest::None, false);
    assert!(message_level_is_interesting(NOTICE));

    reset_guc();
}

#[test]
fn errno_helpers_match_postgres_categories() {
    assert_eq!(
        errno::sqlstate_for_file_access(errno::EACCES),
        ERRCODE_INSUFFICIENT_PRIVILEGE
    );
    assert_eq!(
        errno::sqlstate_for_file_access(errno::EROFS),
        ERRCODE_INSUFFICIENT_PRIVILEGE
    );
    assert_eq!(
        errno::sqlstate_for_file_access(errno::ENOENT),
        ERRCODE_UNDEFINED_FILE
    );
    assert_eq!(
        errno::sqlstate_for_file_access(errno::EEXIST),
        ERRCODE_DUPLICATE_FILE
    );
    assert_eq!(errno::sqlstate_for_file_access(errno::ENOSPC), ERRCODE_DISK_FULL);
    assert_eq!(
        errno::sqlstate_for_socket_access(errno::ECONNRESET),
        ERRCODE_CONNECTION_FAILURE
    );
    assert_eq!(
        errno::sqlstate_for_socket_access(errno::EINVAL),
        ERRCODE_INTERNAL_ERROR
    );
}

#[test]
fn percent_m_expands_saved_errno() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    assert!(errstart(ERROR, None));
    let expected = errno::strerror(errno::ENOENT);
    // Override the captured errno the way callers control %m in tests.
    pre_format_elog_string(errno::ENOENT, None);
    assert_eq!(format_elog_string("could not open file: %m"), format!("could not open file: {}", expected));

    errmsg("plain message").unwrap();
    let err = pg_re_throw::<()>().unwrap_err();
    assert_eq!(err.message, "plain message");
}

#[test]
fn throw_error_data_returns_err_for_error_level() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    let err = ThrowErrorData(
        PgError::error("stop")
            .with_sqlstate(UNDEFINED_TABLE)
            .with_detail("detail")
            .with_error_location(ErrorLocation::new("dir/file.c", 42, "func")),
    )
    .unwrap_err();

    assert_eq!(err.level, ERROR);
    assert_eq!(err.sqlstate, UNDEFINED_TABLE);
    assert_eq!(err.message, "stop");
    assert_eq!(err.detail.as_deref(), Some("detail"));
    let loc = err.location.unwrap();
    assert_eq!(loc.filename.as_deref(), Some("file.c")); // basename normalized
    assert_eq!(loc.lineno, 42);

    // The stack is clean again.
    assert!(geterrcode().is_err());
}

#[test]
fn throw_error_data_emits_warnings_and_skips_uninteresting() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    fn hook(error: &PgError, _output_to_server: &mut bool) {
        SEEN.lock().unwrap().push(error.message.clone());
    }
    SEEN.lock().unwrap().clear();
    let previous = set_emit_log_hook(Some(hook));

    // WARNING >= log_min_messages(WARNING): emitted to the server log.
    assert_eq!(ThrowErrorData(PgError::warning("watch out")), Ok(()));
    // NOTICE: not routed anywhere under the boot defaults — short-circuited.
    assert_eq!(ThrowErrorData(PgError::notice("nobody cares")), Ok(()));

    set_emit_log_hook(previous);

    let seen = SEEN.lock().unwrap();
    assert_eq!(seen.as_slice(), ["watch out".to_owned()]);
}

#[test]
fn error_stack_mutators_and_recursion_guard() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    assert!(errstart(ERROR, Some("postgres")));
    errcode(UNDEFINED_TABLE).unwrap();
    errmsg("missing relation").unwrap();
    errdetail("detail").unwrap();
    errhint("hint").unwrap();
    errcontext_msg("first").unwrap();
    errcontext_msg("second").unwrap();
    errposition(7).unwrap();
    internalerrposition(11).unwrap();
    internalerrquery(Some("select 1")).unwrap();
    err_generic_string(::types_error::PG_DIAG_TABLE_NAME, "users").unwrap();

    assert_eq!(geterrcode().unwrap(), UNDEFINED_TABLE);
    assert_eq!(geterrposition().unwrap(), 7);
    assert_eq!(getinternalerrposition().unwrap(), 11);
    assert!(!in_error_recursion_trouble());

    let copied = CopyErrorData().unwrap();
    assert_eq!(copied.message, "missing relation");
    assert_eq!(copied.context.as_deref(), Some("first\nsecond"));
    assert_eq!(copied.table_name.as_deref(), Some("users"));
    FreeErrorData(copied);

    let err = pg_re_throw::<()>().unwrap_err();
    assert_eq!(err.message, "missing relation");
    assert_eq!(err.hint.as_deref(), Some("hint"));
    assert_eq!(err.internal_query.as_deref(), Some("select 1"));

    assert_eq!(
        geterrcode().unwrap_err().message,
        "errstart was not called"
    );
    assert_eq!(
        pg_re_throw::<()>().unwrap_err().message,
        "pg_re_throw tried to return"
    );
}

#[test]
fn rethrow_error_checks_level() {
    let err = ReThrowError::<()>(PgError::warning("warn")).unwrap_err();
    assert_eq!(err.level, PANIC);
    assert_eq!(err.message, "ReThrowError called with non-ERROR error data");

    let err = ReThrowError::<()>(PgError::error("hard")).unwrap_err();
    assert_eq!(err.level, ERROR);
    assert_eq!(err.message, "hard");
}

#[test]
fn context_attaches_on_propagation_innermost_first() {
    // docs/query-lifecycle-raii.md: error context attaches on propagation,
    // innermost boundary first (replacing the error_context_stack walk).
    fn inner() -> PgResult<()> {
        Err(PgError::error("kaput"))
    }
    fn middle() -> PgResult<()> {
        inner().map_err(|e| e.add_context("inner frame"))
    }
    let err = middle()
        .map_err(|e| e.add_context("outer frame"))
        .unwrap_err();
    assert_eq!(err.context.as_deref(), Some("inner frame\nouter frame"));
}

#[test]
fn errcontext_msg_appends_to_in_flight_error() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    assert!(errstart(ERROR, None));
    errmsg("kaput").unwrap();
    errcontext_msg("SQL statement \"SELECT 1\"").unwrap();
    let err = pg_re_throw::<()>().unwrap_err();

    let err = Err::<(), _>(err)
        .map_err(|e: PgError| e.add_context("PL/pgSQL function f() line 1"))
        .unwrap_err();
    assert_eq!(
        err.context.as_deref(),
        Some("SQL statement \"SELECT 1\"\nPL/pgSQL function f() line 1")
    );
}

#[test]
fn get_error_context_stack_walks_retired_chain() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    // The error_context_stack callback chain is retired (attach-on-propagation),
    // so the walk fires no callbacks and the scratch entry accumulates nothing.
    assert_eq!(GetErrorContextStack(), None);
    // recursion_depth is balanced (not left elevated): a fresh report can start.
    assert!(!in_error_recursion_trouble());
    assert!(errstart(ERROR, None));
    errmsg("ok").unwrap();
    let _ = pg_re_throw::<()>();
}

#[test]
fn errsave_soft_path_saves_details() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    let mut context = SoftErrorContext::new(true);
    assert!(errsave_start(Some(&mut context), Some("postgres")));
    errcode(UNDEFINED_TABLE).unwrap();
    errmsg("missing relation").unwrap();
    errposition(7).unwrap();
    errsave_finish(Some(&mut context), Some("f.c"), 42, Some("fn")).unwrap();

    assert!(context.error_occurred());
    let saved = context.error().unwrap();
    assert_eq!(saved.level, ERROR);
    assert_eq!(saved.sqlstate, UNDEFINED_TABLE);
    assert_eq!(saved.message, "missing relation");
    assert_eq!(saved.cursor_position, Some(7));
    assert_eq!(saved.location.as_ref().unwrap().lineno, 42);
    assert!(geterrcode().is_err());
}

#[test]
fn errsave_without_details_only_marks_error_occurred() {
    let _guard = lock();
    FlushErrorState();

    let mut context = SoftErrorContext::new(false);
    assert!(!errsave_start(Some(&mut context), Some("postgres")));
    assert!(context.error_occurred());
    assert!(context.error().is_none());
    assert!(geterrcode().is_err());
}

#[test]
fn errsave_without_context_is_a_hard_error() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    assert!(errsave_start(None, Some("postgres")));
    errmsg_internal("hard").unwrap();
    let err = errsave_finish(None, Some("f.c"), 9, Some("fn")).unwrap_err();
    assert_eq!(err.level, ERROR);
    assert_eq!(err.message, "hard");
    assert_eq!(err.location.unwrap().lineno, 9);
}

#[test]
fn builder_finish_routes_through_errfinish() {
    let _guard = lock();
    reset_guc();
    FlushErrorState();

    let err = ereport(ERROR)
        .errcode(UNDEFINED_TABLE)
        .errmsg("missing relation")
        .errdetail("detail")
        .errhint("hint")
        .errhidestmt(true)
        .finish(ErrorLocation::new("file.c", 12, "func"))
        .unwrap_err();

    assert_eq!(err.level, ERROR);
    assert_eq!(err.sqlstate, UNDEFINED_TABLE);
    assert_eq!(err.message, "missing relation");
    assert_eq!(err.detail.as_deref(), Some("detail"));
    assert_eq!(err.hint.as_deref(), Some("hint"));
    assert!(err.hide_statement);
    assert_eq!(err.location.unwrap().lineno, 12);

    assert_eq!(elog(ERROR, "boom").unwrap_err().message, "boom");
}

struct TestContext;

impl BackendLogContext for TestContext {
    fn has_client_port(&self) -> bool {
        true
    }
    fn application_name(&self) -> Option<&str> {
        Some("psql")
    }
    fn user_name(&self) -> Option<&str> {
        Some("postgres")
    }
    fn database_name(&self) -> Option<&str> {
        Some("postgres")
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
    fn backend_type(&self) -> Option<&str> {
        Some("client backend")
    }
    fn process_id(&self) -> u32 {
        1234
    }
    fn lock_group_leader_pid(&self) -> Option<u32> {
        Some(1200)
    }
    fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
        Some((3, 44))
    }
    fn top_transaction_id(&self) -> u32 {
        99
    }
    fn query_id(&self) -> i64 {
        42
    }
    fn query_string(&self) -> Option<&str> {
        Some("select 1")
    }
    fn session_start_time(&self) -> i64 {
        1_700_000_000
    }
    fn ps_display(&self) -> Option<&str> {
        Some("postgres: session")
    }
}

static TEST_CONTEXT: TestContext = TestContext;

#[test]
fn log_status_format_renders_every_escape() {
    let _guard = lock();
    let previous = set_backend_log_context(Some(&TEST_CONTEXT));
    let error = PgError::error("boom").with_sqlstate(UNDEFINED_TABLE);

    let mut buf = String::new();
    log_status_format(
        &mut buf,
        Some("%a|%b|%u|%d|%p|%P|%v|%x|%e|%Q|%r|%h|%L|%i|%c"),
        &error,
    );
    assert_eq!(
        buf,
        "psql|client backend|postgres|postgres|1234|1200|3/44|99|42P01|42|127.0.0.1(5432)|127.0.0.1|127.0.0.1|postgres: session|6553f100.4d2"
    );

    set_backend_log_context(previous);
}

#[test]
fn log_status_format_applies_padding_and_percent_escape() {
    let _guard = lock();
    let previous = set_backend_log_context(Some(&TEST_CONTEXT));
    let error = PgError::error("boom");

    let mut buf = String::new();
    log_status_format(&mut buf, Some("%8p|%-8p|%%"), &error);
    assert_eq!(buf, "    1234|1234    |%");

    // Padding-only escapes for absent values render spaces; %q in a backend
    // is ignored; a trailing bare % is a format error and is dropped.
    set_backend_log_context(None);
    let mut buf = String::new();
    log_status_format(&mut buf, Some("[%4a]%qX%"), &error);
    assert_eq!(buf, "[    ]");

    set_backend_log_context(previous);
}

#[test]
fn log_status_format_q_stops_without_client_port() {
    let _guard = lock();
    let previous = set_backend_log_context(None);
    let error = PgError::error("boom");

    let mut buf = String::new();
    log_status_format(&mut buf, Some("before %q after"), &error);
    assert_eq!(buf, "before ");

    set_backend_log_context(previous);
}

#[test]
fn check_log_of_query_matches_level_hide_and_query_rules() {
    let _guard = lock();
    config::set_log_min_error_statement(WARNING);
    let previous = set_backend_log_context(Some(&TEST_CONTEXT));

    assert!(check_log_of_query(&PgError::warning("warn")));
    assert!(!check_log_of_query(&PgError::notice("notice")));
    assert!(!check_log_of_query(
        &PgError::warning("hidden").with_hide_statement(true)
    ));

    set_backend_log_context(None);
    assert!(!check_log_of_query(&PgError::warning("warn")));

    set_backend_log_context(previous);
    config::set_log_min_error_statement(ERROR);
}

#[test]
fn severity_and_sqlstate_helpers_match_postgres_strings() {
    assert_eq!(error_severity(DEBUG1), "DEBUG");
    assert_eq!(error_severity(LOG_SERVER_ONLY), "LOG");
    assert_eq!(error_severity(WARNING_CLIENT_ONLY), "WARNING");
    assert_eq!(error_severity(ERROR), "ERROR");
    assert_eq!(error_severity(ErrorLevel(99)), "???");
    assert_eq!(unpack_sql_state(UNDEFINED_TABLE), "42P01");
}

#[test]
fn append_with_tabs_inserts_tabs_after_newlines() {
    let mut buf = String::new();
    append_with_tabs(&mut buf, "a\nb\n");
    assert_eq!(buf, "a\n\tb\n\t");
}

#[test]
fn backtrace_functions_check_splits_and_empty_entry_terminates() {
    let parsed = check_backtrace_functions(" foo,bar_baz,\nqux9\t")
        .unwrap()
        .unwrap();
    assert_eq!(parsed.functions(), &["foo", "bar_baz", "qux9"]);
    assert!(parsed.matches("bar_baz"));
    assert!(!parsed.matches(""));

    // ",," produces an empty entry, which (as in the C \0\0 walk) hides
    // everything after it.
    let parsed = check_backtrace_functions("foo,,bar").unwrap().unwrap();
    assert!(parsed.matches("foo"));
    assert!(!parsed.matches("bar"));

    assert_eq!(
        check_backtrace_functions("valid-name").unwrap_err().message,
        "Invalid character."
    );
    assert!(check_backtrace_functions("").unwrap().is_none());
}

#[test]
fn assign_backtrace_functions_updates_matcher() {
    let _guard = lock();
    let parsed = check_backtrace_functions("foo,bar").unwrap();
    assign_backtrace_functions(parsed);

    assert!(matches_backtrace_functions("foo"));
    assert!(matches_backtrace_functions("bar"));
    assert!(!matches_backtrace_functions("baz"));

    assign_backtrace_functions(None);
    assert!(!matches_backtrace_functions("foo"));
}

#[test]
fn log_destination_check_accepts_postgres_keywords() {
    assert_eq!(
        check_log_destination("stderr, csvlog, JSONLOG, syslog").unwrap(),
        LOG_DESTINATION_STDERR
            | LOG_DESTINATION_CSVLOG
            | LOG_DESTINATION_JSONLOG
            | LOG_DESTINATION_SYSLOG
    );
    assert_eq!(
        check_log_destination("\"stderr\", csvlog").unwrap(),
        LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG
    );
    // pg_strcasecmp: quoted identifiers keep their case but still match.
    assert_eq!(
        check_log_destination("\"STDERR\"").unwrap(),
        LOG_DESTINATION_STDERR
    );
    assert_eq!(
        check_log_destination("stderr,,csvlog").unwrap_err().message,
        "List syntax is invalid."
    );
    assert_eq!(
        check_log_destination("stderr,unknown").unwrap_err().message,
        "Unrecognized key word: \"unknown\"."
    );
    // eventlog is win32-only and rejected here.
    assert!(check_log_destination("eventlog").is_err());
}

#[test]
fn formatted_timestamps_cache_and_reset() {
    let _guard = lock();

    let first = get_formatted_log_time();
    let second = get_formatted_log_time();
    assert_eq!(first, second);
    assert!(first.ends_with(" GMT"));
    assert!(first.contains('.'));

    reset_formatted_start_time();
    let start = get_formatted_start_time();
    assert!(start.ends_with(" GMT"));
    assert!(!start.contains('.'));
}

#[test]
fn frontend_message_requires_seam_owner() {
    // send_message_to_frontend reaches into the pqcomm seam crate, which must
    // panic loudly until libpq/pqcomm is ported.
    let result = std::panic::catch_unwind(|| {
        send_message_to_frontend(&PgError::error("boom"));
    });
    assert!(result.is_err());
}
