use std::cell::RefCell;
use std::sync::Once;

use types_core::BOOTSTRAP_SUPERUSERID;
use types_error::ErrorLevel;
use types_guc::*;

use crate::*;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
}

// Emit-log-hook capture; the hook is per thread and one test installs it.
static EMITTED: std::sync::Mutex<Vec<types_error::PgError>> = std::sync::Mutex::new(Vec::new());

// A role pg_parameter_aclcheck_set denies (see setup).
const DENIED_ROLE: types_core::Oid = 0xB168;

fn capture_emitted(error: &types_error::PgError, _output_to_server: &mut bool) {
    EMITTED.lock().unwrap_or_else(|e| e.into_inner()).push(error.clone());
}

// application_name's value backing (guc_tables::backing) is process-global;
// tests that read or write it must not overlap across test threads.
static APPLICATION_NAME_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn test_parse_bool(value: &str) -> Option<bool> {
    let lower = value.to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    for (word, result) in [("true", true), ("false", false), ("yes", true), ("no", false)] {
        if word.starts_with(&lower) {
            return Some(result);
        }
    }
    match lower.as_str() {
        "on" => Some(true),
        "off" | "of" => Some(false),
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();
        xact_seams::is_in_parallel_mode::set(|| false);
        scalar_seams::parse_bool::set(test_parse_bool);
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, role| Ok(role != DENIED_ROLE));
        mbutils_seams::get_database_encoding::set(|| 6);
        mbutils_seams::pg_server_to_client::set(|mcx, s| {
            if !s.starts_with(b"conv:") {
                return Ok(None);
            }
            let mut out = mcx::vec_with_capacity_in(mcx, s.len()).expect("test alloc");
            out.extend(s.iter().map(u8::to_ascii_uppercase));
            Ok(Some(out))
        });
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        timestamp_seams::get_current_timestamp::set(|| 42);
        conffiles_seams::absolute_config_location::set(|location, calling_file| {
            let p = std::path::Path::new(&location);
            if p.is_absolute() {
                p.to_path_buf()
            } else if let Some(calling) = calling_file {
                calling.parent().unwrap_or(std::path::Path::new(".")).join(p)
            } else if let Some(dd) = init_small::globals::DataDir() {
                // C AbsoluteConfigLocation: a relative name with no calling
                // file resolves against DataDir (how postgresql.auto.conf is
                // found).
                std::path::Path::new(&dd).join(p)
            } else {
                p.to_path_buf()
            }
        });
        conffiles_seams::get_conf_files_in_dir::set(|_, _, _| {
            Ok(conffiles_seams::ConfFilesInDir::default())
        });
    });
    initialize_guc_options().unwrap();
}

fn set_session(name: &str, value: Option<&str>) -> PgResult<i32> {
    set_config_option_ext(
        name,
        value,
        PGC_USERSET,
        PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
}

fn show(name: &str) -> Option<String> {
    with_store(|reg| get_config_option_by_name(reg, name, true).unwrap()).unwrap()
}

#[test]
fn boot_defaults_seeded() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(get_int("work_mem"), Some(4096));
    assert_eq!(get_bool("enable_seqscan"), Some(true));
    assert_eq!(get_real("cursor_tuple_fraction"), Some(0.1));
    assert_eq!(get_string("application_name"), Some(Some(String::new())));
    assert_eq!(show("bytea_output"), Some("hex".to_string()));
}

#[test]
fn set_with_units_and_show() {
    setup();
    assert_eq!(set_session("work_mem", Some("8MB")).unwrap(), 1);
    assert_eq!(get_int("work_mem"), Some(8192));
    assert_eq!(show("work_mem"), Some("8MB".to_string()));
    assert_eq!(set_session("work_mem", Some("30720")).unwrap(), 1);
    assert_eq!(show("work_mem"), Some("30MB".to_string()));
}

#[test]
fn invalid_values_error_at_session_source() {
    setup();
    let e = set_session("work_mem", Some("banana")).unwrap_err();
    assert!(e.message().contains("invalid value for parameter \"work_mem\""));

    let e = set_session("work_mem", Some("1XB")).unwrap_err();
    assert_eq!(e.hint(), Some(MEMORY_UNITS_HINT));

    let e = set_session("work_mem", Some("1")).unwrap_err();
    assert!(e.message().contains("outside the valid range"), "{}", e.message());

    let e = set_session("statement_timeout", Some("5banana")).unwrap_err();
    assert_eq!(e.hint(), Some(TIME_UNITS_HINT));

    let e = set_session("enable_seqscan", Some("maybe")).unwrap_err();
    assert!(e.message().contains("requires a Boolean value"));
}

#[test]
fn file_source_rejection_returns_zero() {
    setup();
    let rc = set_config_option_ext(
        "work_mem",
        Some("banana"),
        PGC_SIGHUP,
        PGC_S_FILE,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 0);
}

/// Per-role/db settings (pg_db_role_setting -> ProcessGUCArray, sources
/// PGC_S_USER / PGC_S_DATABASE[_USER] / PGC_S_GLOBAL) resolve elevel 0 to
/// WARNING (C set_config_option): a bad stored value is SKIPPED (rc 0),
/// never an Err — an Err at login would refuse every new connection where
/// C connects with a WARNING (`ALTER ROLE x SET role = <dropped role>`
/// locked pgrust sessions out in the round-11 role-DDL soak).
#[test]
fn user_source_rejection_skips_not_errors() {
    setup();
    for source in [PGC_S_USER, PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_GLOBAL] {
        let rc = set_config_option_ext(
            "work_mem",
            Some("banana"),
            PGC_SUSET,
            source,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            false,
        )
        .unwrap();
        assert_eq!(rc, 0, "source {source:?} must skip, not error");
    }
}

/// The report emitted for a demoted rejection carries the DEMOTED level:
/// C's set_config_option ereport()s at elevel, so the client sees 'N'
/// (WARNING), not 'E' — an 'E' during connection startup makes libpq
/// abort the whole connection.
#[test]
fn demoted_rejection_reports_at_demoted_level() {
    let e = types_error::PgError::new(types_error::ERROR, "role \"gone\" does not exist");
    let demoted = crate::registry::demoted_for_report(e, types_error::WARNING);
    assert_eq!(demoted.level, types_error::WARNING);
    assert_eq!(demoted.message, "role \"gone\" does not exist");
}

#[test]
fn postmaster_param_cannot_change_at_runtime() {
    setup();
    let e = set_session("shared_buffers", Some("1000")).unwrap_err();
    assert!(e.message().contains("cannot be changed without restarting the server"));
    let e = set_session("wal_level", Some("logical")).unwrap_err();
    assert!(e.message().contains("cannot be changed without restarting the server"));
}

#[test]
fn sighup_reread_of_postmaster_param() {
    setup();
    let same = set_config_option_ext(
        "shared_buffers",
        Some("16384"),
        PGC_SIGHUP,
        PGC_S_FILE,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(same, -1);

    let changed = set_config_option_ext(
        "shared_buffers",
        Some("32768"),
        PGC_SIGHUP,
        PGC_S_FILE,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(changed, 0);
    let status = with_store(|reg| reg.find_option("shared_buffers").unwrap().gen().status).unwrap();
    assert!(status & crate::model::GUC_PENDING_RESTART != 0);
}

#[test]
fn higher_source_wins_and_seeds_reset_default() {
    setup();
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    let rc = set_config_option_ext(
        "work_mem",
        Some("2048"),
        PGC_SIGHUP,
        PGC_S_FILE,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, -1);
    assert_eq!(get_int("work_mem"), Some(8192));
    assert_eq!(GetConfigOptionResetString("work_mem"), Some("2048".to_string()));
    assert_eq!(set_session("work_mem", None).unwrap(), 1);
    assert_eq!(get_int("work_mem"), Some(2048));
}

#[test]
fn transaction_abort_restores_prior_value() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    AtEOXact_GUC(false, 1);
    assert_eq!(get_int("work_mem"), Some(4096));
}

#[test]
fn transaction_commit_keeps_set_value() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    AtEOXact_GUC(true, 1);
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn set_local_reverts_on_commit() {
    setup();
    AtStart_GUC();
    let rc = set_config_option_ext(
        "work_mem",
        Some("8192"),
        PGC_USERSET,
        PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_LOCAL,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 1);
    assert_eq!(get_int("work_mem"), Some(8192));
    AtEOXact_GUC(true, 1);
    assert_eq!(get_int("work_mem"), Some(4096));
}

#[test]
fn set_then_set_local_commit_restores_set_value() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    let rc = set_config_option_ext(
        "work_mem",
        Some("16384"),
        PGC_USERSET,
        PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_LOCAL,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 1);
    assert_eq!(get_int("work_mem"), Some(16384));
    AtEOXact_GUC(true, 1);
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn save_scope_pops_at_function_exit() {
    setup();
    AtStart_GUC();
    let nest = NewGUCNestLevel();
    assert_eq!(nest, 2);
    let rc = set_config_option_ext(
        "work_mem",
        Some("8192"),
        PGC_USERSET,
        PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SAVE,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 1);
    AtEOXact_GUC(true, nest);
    assert_eq!(get_int("work_mem"), Some(4096));
    AtEOXact_GUC(true, 1);
}

#[test]
fn subtransaction_abort_restores_within_transaction() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    let sub = NewGUCNestLevel();
    assert_eq!(set_session("work_mem", Some("16384")).unwrap(), 1);
    AtEOXact_GUC(false, sub);
    assert_eq!(get_int("work_mem"), Some(8192));
    AtEOXact_GUC(true, 1);
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn at_eoxact_without_store_is_noop() {
    AtStart_GUC();
    AtEOXact_GUC(true, 1);
}

#[test]
fn report_list_is_o_changed() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
    begin_reporting_guc_options();
    let initial = SENT.with(|s| std::mem::take(&mut *s.borrow_mut()));
    assert!(initial
        .iter()
        .any(|(t, body)| *t == b'S' && body.starts_with(b"application_name\0")));

    report_changed_guc_options();
    assert_eq!(SENT.with(|s| s.borrow().len()), 0);

    assert_eq!(set_session("application_name", Some("psql")).unwrap(), 1);
    report_changed_guc_options();
    let frames = SENT.with(|s| std::mem::take(&mut *s.borrow_mut()));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].1, b"application_name\0psql\0".to_vec());

    report_changed_guc_options();
    assert_eq!(SENT.with(|s| s.borrow().len()), 0);

    assert_eq!(set_session("application_name", Some("psql")).unwrap(), 1);
    report_changed_guc_options();
    assert_eq!(SENT.with(|s| s.borrow().len()), 0);
}

#[test]
fn reset_and_reset_all() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    assert_eq!(set_session("application_name", Some("x")).unwrap(), 1);
    assert_eq!(set_session("work_mem", None).unwrap(), 1);
    assert_eq!(get_int("work_mem"), Some(4096));

    assert_eq!(set_session("work_mem", Some("8192")).unwrap(), 1);
    ResetAllOptions();
    assert_eq!(get_int("work_mem"), Some(4096));
    assert_eq!(get_string("application_name"), Some(Some(String::new())));
}

#[test]
fn custom_placeholder_variables() {
    setup();
    assert_eq!(set_session("my.custom", Some("hello")).unwrap(), 1);
    assert_eq!(show("my.custom"), Some("hello".to_string()));

    let e = set_session("my..bad", Some("x")).unwrap_err();
    assert!(e.message().contains("invalid configuration parameter name"));

    let e = set_session("no_such_parameter", Some("x")).unwrap_err();
    assert!(e.message().contains("unrecognized configuration parameter"));
}

// DefineCustomStringVariable (guc.c:5224 / define_custom_variable
// guc.c:4937): a placeholder set before the library loaded is replaced and
// its committed value and transactional (SET LOCAL) value re-applied in
// order; a fresh definition SHOWs its NULL boot value as ''; redefining a
// real parameter is an internal error; MarkGUCPrefixReserved afterwards
// refuses new names under the prefix but keeps the defined ones settable.
#[test]
fn define_custom_string_variable_adopts_placeholder_values() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("b021ext.app_name", Some("early")).unwrap(), 1);
    let rc = set_config_option_ext(
        "b021ext.app_name",
        Some("local1"),
        PGC_USERSET,
        PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_LOCAL,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 1);
    DefineCustomStringVariable("b021ext.app_name", Some("desc"), None, None, PGC_USERSET, 0)
        .unwrap();
    assert_eq!(show("b021ext.app_name"), Some("local1".to_string()));
    let flags = with_store(|reg| get_config_option_flags(reg, "b021ext.app_name", false).unwrap())
        .unwrap();
    assert_eq!(flags & GUC_CUSTOM_PLACEHOLDER, 0);
    AtEOXact_GUC(true, 1);
    assert_eq!(show("b021ext.app_name"), Some("early".to_string()));

    DefineCustomStringVariable("b021ext.other", Some("desc"), None, None, PGC_USERSET, 0).unwrap();
    assert_eq!(show("b021ext.other"), Some(String::new()));
    let e = DefineCustomStringVariable("b021ext.other", Some("desc"), None, None, PGC_USERSET, 0)
        .unwrap_err();
    assert_eq!(e.message(), "attempt to redefine parameter \"b021ext.other\"");

    MarkGUCPrefixReserved("b021ext");
    let e = set_session("b021ext.nope", Some("x")).unwrap_err();
    assert!(e.message().contains("invalid configuration parameter name"));
    assert_eq!(e.detail(), Some("\"b021ext\" is a reserved prefix."));
    assert_eq!(set_session("b021ext.app_name", Some("later")).unwrap(), 1);
    assert_eq!(show("b021ext.app_name"), Some("later".to_string()));
}

#[test]
fn old_guc_names_map() {
    setup();
    assert_eq!(set_session("sort_mem", Some("8192")).unwrap(), 1);
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn case_insensitive_lookup() {
    setup();
    assert_eq!(set_session("WORK_MEM", Some("8192")).unwrap(), 1);
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn guc_is_name_truncates_long_values() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let long = "a".repeat(100);
    assert_eq!(set_session("application_name", Some(&long)).unwrap(), 1);
    assert_eq!(get_string("application_name"), Some(Some("a".repeat(63))));
}

#[test]
fn enum_values_and_hint() {
    setup();
    assert_eq!(set_session("bytea_output", Some("escape")).unwrap(), 1);
    assert_eq!(get_enum("bytea_output"), Some(0));
    let e = set_session("bytea_output", Some("wat")).unwrap_err();
    assert!(e.hint().unwrap().contains("escape, hex"));
}

#[test]
fn name_compare_and_hash() {
    use core::cmp::Ordering;
    assert_eq!(guc_name_compare("work_mem", "WORK_MEM"), Ordering::Equal);
    assert_eq!(guc_name_compare("a", "ab"), Ordering::Less);
    assert_eq!(guc_name_compare("ab", "a"), Ordering::Greater);
    assert_eq!(guc_name_hash("Work_Mem"), guc_name_hash("work_mem"));
    assert_eq!(convert_guc_name_for_parameter_acl("Sort_Mem"), "work_mem");
}

#[test]
fn parse_int_units() {
    match parse_int("1GB", GUC_UNIT_KB) {
        ParseNum::Ok(v) => assert_eq!(v, 1048576),
        _ => panic!(),
    }
    match parse_int("30s", GUC_UNIT_MS) {
        ParseNum::Ok(v) => assert_eq!(v, 30000),
        _ => panic!(),
    }
    match parse_int("0x10", 0) {
        ParseNum::Ok(v) => assert_eq!(v, 16),
        _ => panic!(),
    }
    match parse_int("10000000000", 0) {
        ParseNum::Err { hint } => assert_eq!(hint, Some("Value exceeds integer range.")),
        _ => panic!(),
    }
    match parse_int("100 MB", GUC_UNIT_KB) {
        ParseNum::Ok(v) => assert_eq!(v, 102400),
        _ => panic!(),
    }
}

#[test]
fn fmt_g_matches_c_printf() {
    assert_eq!(fmt_g(0.0), "0");
    assert_eq!(fmt_g(1.5), "1.5");
    assert_eq!(fmt_g(100.0), "100");
    assert_eq!(fmt_g(1.23456789), "1.23457");
    assert_eq!(fmt_g(1234567.0), "1.23457e+06");
    assert_eq!(fmt_g(0.0001), "0.0001");
    assert_eq!(fmt_g(0.00001), "1e-05");
    assert_eq!(fmt_e(1.5, 2), "1.50e+00");
    assert_eq!(fmt_e(1234.0, 2), "1.23e+03");
}

#[test]
fn parse_long_option_splits_and_underscores() {
    assert_eq!(
        ParseLongOption("some-option=some value"),
        ("some_option".to_string(), Some("some value".to_string()))
    );
    assert_eq!(ParseLongOption("flag-only"), ("flag_only".to_string(), None));
}

#[test]
fn valid_custom_names() {
    assert!(valid_custom_variable_name("foo.bar"));
    assert!(valid_custom_variable_name("foo.bar.baz"));
    assert!(valid_custom_variable_name("foo._bar$2"));
    assert!(!valid_custom_variable_name("foo"));
    assert!(!valid_custom_variable_name("foo."));
    assert!(!valid_custom_variable_name(".bar"));
    assert!(!valid_custom_variable_name("foo..bar"));
    assert!(!valid_custom_variable_name("1foo.bar"));
}

// guc.c:1613: InitializeGUCOptionsFromEnvironment re-applies the rlimit
// default on every reload, so dropping max_stack_depth from the file lands
// on the platform default (2MB/DYNAMIC_DEFAULT), not the 100kB boot value.
#[test]
fn reload_restores_rlimit_stack_depth_default() {
    setup();
    let rlimit = stack_depth_core::get_stack_depth_rlimit();
    let new_limit = rlimit.saturating_sub(stack_depth_core::STACK_DEPTH_SLOP) / 1024;
    if rlimit <= 0 || new_limit <= 100 {
        return;
    }
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("guc_msd_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("postgresql.conf");
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    SetConfigOption("config_file", Some(conf.to_str().unwrap()), PGC_POSTMASTER, PGC_S_OVERRIDE)
        .unwrap();
    let source_of = || {
        with_store(|reg| reg.find_option("max_stack_depth").unwrap().gen().source).unwrap()
    };

    std::fs::write(&conf, "max_stack_depth = '1MB'\n").unwrap();
    crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
        .unwrap();
    assert_eq!((get_int("max_stack_depth"), source_of()), (Some(1024), PGC_S_FILE));

    std::fs::write(&conf, "").unwrap();
    crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
        .unwrap();
    let (expected, source) = crate::store::boot_limit_and_source(new_limit);
    assert_eq!((get_int("max_stack_depth"), source_of()), (Some(expected), source));
}

// guc.c:1873: a set-but-empty data_directory / hba_file is not "unset"; it
// goes through make_absolute_path and resolves to the working directory.
#[test]
fn select_config_files_empty_paths_resolve_to_cwd() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("guc_scf_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("postgresql.conf"), "data_directory = ''\nhba_file = ''\n").unwrap();

    assert!(crate::select::SelectConfigFiles(Some(dir.to_str().unwrap()), "postgres").unwrap());
    let cwd = std::env::current_dir().unwrap().to_str().unwrap().to_string();
    assert_eq!(get_string("data_directory"), Some(Some(cwd.clone())));
    assert_eq!(get_string("hba_file"), Some(Some(cwd)));
    assert_eq!(
        get_string("ident_file"),
        Some(Some(dir.join("pg_ident.conf").to_str().unwrap().to_string()))
    );
}

#[test]
fn process_config_file_applies_and_reverts() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("guc_pcf_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("postgresql.conf");
    init_small::globals::SetDataDir(dir.to_str().unwrap());

    std::fs::write(&conf, "work_mem = 2MB\nwork_mem = 8MB\napplication_name = 'from_file'\nnot.known = 'kept'\n").unwrap();
    SetConfigOption("config_file", Some(conf.to_str().unwrap()), PGC_POSTMASTER, PGC_S_OVERRIDE)
        .unwrap();

    let clean =
        crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
            .unwrap();
    assert!(clean);
    assert_eq!(get_int("work_mem"), Some(8192));
    assert_eq!(get_string("application_name"), Some(Some("from_file".to_string())));
    assert_eq!(show("not.known"), Some("kept".to_string()));
    assert_eq!(pg_reload_time(), 42);
    let (source, sourcefile) = with_store(|reg| {
        let gen = reg.find_option("work_mem").unwrap().gen();
        (gen.source, gen.sourcefile.clone())
    })
    .unwrap();
    assert_eq!(source, PGC_S_FILE);
    assert_eq!(sourcefile.as_deref(), conf.to_str());

    // Removal from the file reverts to the boot default on reload.
    std::fs::write(&conf, "application_name = 'from_file'\n").unwrap();
    let clean =
        crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
            .unwrap();
    assert!(clean);
    assert_eq!(get_int("work_mem"), Some(4096));
    assert_eq!(with_store(|reg| reg.find_option("work_mem").unwrap().gen().source).unwrap(), PGC_S_DEFAULT);

    // An unknown non-custom name is a recorded error; settings are not applied.
    std::fs::write(&conf, "no_such_thing = 1\nwork_mem = 3MB\n").unwrap();
    let clean =
        crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
            .unwrap();
    assert!(!clean);
    assert_eq!(get_int("work_mem"), Some(4096));
}

// guc.c:459-476: a PGC_POSTMASTER parameter that came from the file and is
// no longer in it is reported AND recorded as an error item (name NULL,
// no file/line, ignore) so pg_file_settings shows the removal. Audit
// a186-candidate-fp-misc-guc-p1-e735f2236e8cc368e9e6-1.
#[test]
fn process_config_file_removed_postmaster_param_records_error_item() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("guc_pcf_removed_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("postgresql.conf");
    init_small::globals::SetDataDir(dir.to_str().unwrap());

    // Initial load (postmaster context): port comes from the file.
    std::fs::write(&conf, "port = 5499\n").unwrap();
    SetConfigOption("config_file", Some(conf.to_str().unwrap()), PGC_POSTMASTER, PGC_S_OVERRIDE)
        .unwrap();
    let clean = crate::process_config::process_config_file_internal(
        PGC_POSTMASTER,
        true,
        types_error::LOG,
    )
    .unwrap();
    assert!(clean);
    assert_eq!(get_int("port"), Some(5499));
    assert_eq!(with_store(|reg| reg.find_option("port").unwrap().gen().reset_source).unwrap(), PGC_S_FILE);

    // Removed from the file: a show_all_file_settings-style scan (SIGHUP,
    // apply_settings=false) must return the error item C records.
    std::fs::write(&conf, "application_name = 'unrelated'\n").unwrap();
    let (clean, items) = crate::process_config::process_config_file_internal_list(
        PGC_SIGHUP,
        false,
        types_error::LOG,
    )
    .unwrap();
    assert!(!clean);
    let err_items: Vec<&guc_file::ConfigVariable> = items.iter().filter(|i| i.errmsg.is_some()).collect();
    assert_eq!(err_items.len(), 1, "items: {items:?}");
    let item = err_items[0];
    assert_eq!(
        item.errmsg.as_deref(),
        Some("parameter \"port\" cannot be changed without restarting the server")
    );
    assert!(item.name.is_none());
    assert!(item.value.is_none());
    assert!(item.filename.is_none());
    assert_eq!(item.sourceline, 0);
    assert!(item.ignore);
    assert!(!item.applied);
    // The error item is appended after the parsed entries (C's tail append).
    assert_eq!(items.last().map(|i| i.errmsg.is_some()), Some(true));
    // Not applied: the value from the file stands.
    assert_eq!(get_int("port"), Some(5499));
}

// gucdup corpus: C's ProcessConfigFileInternal is LAST-wins for duplicate
// entries within one pass (earlier occurrences are marked ignorable), across
// include files, and postgresql.auto.conf — parsed after the main file — must
// override it all. Byte-verified against C 18.3 twin boots by
// scripts/gucdup-repro-e2e.sh.
#[test]
fn process_config_file_duplicate_orderings_last_wins() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("guc_dup_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("postgresql.conf");
    let auto_conf = dir.join("postgresql.auto.conf");
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    SetConfigOption("config_file", Some(conf.to_str().unwrap()), PGC_POSTMASTER, PGC_S_OVERRIDE)
        .unwrap();

    let reload = || {
        crate::process_config::process_config_file_internal(PGC_SIGHUP, true, types_error::LOG)
            .unwrap()
    };

    // Inline duplicate: the later entry wins.
    std::fs::write(&conf, "work_mem = 2MB\nwork_mem = 8MB\n").unwrap();
    assert!(reload());
    assert_eq!(get_int("work_mem"), Some(8192));

    // Duplicate via an include placed after the inline entry: include wins.
    std::fs::write(dir.join("extra.conf"), "work_mem = 16MB\n").unwrap();
    std::fs::write(&conf, "work_mem = 2MB\ninclude 'extra.conf'\n").unwrap();
    assert!(reload());
    assert_eq!(get_int("work_mem"), Some(16384));

    // Include first, inline later: the inline entry wins.
    std::fs::write(&conf, "include 'extra.conf'\nwork_mem = 2MB\n").unwrap();
    assert!(reload());
    assert_eq!(get_int("work_mem"), Some(2048));

    // postgresql.auto.conf is parsed after the main file: the ALTER SYSTEM
    // value wins over the main file, even over a later main-file duplicate.
    std::fs::write(&conf, "work_mem = 2MB\nwork_mem = 8MB\n").unwrap();
    std::fs::write(&auto_conf, "work_mem = 32MB\n").unwrap();
    assert!(reload());
    assert_eq!(get_int("work_mem"), Some(32768));

    // Case-variant duplicate: find_option matches case-insensitively, but dup
    // pruning compares exact spellings (C strcmp); both entries survive and
    // apply in file order, so the later spelling still wins.
    std::fs::remove_file(&auto_conf).unwrap();
    std::fs::write(&conf, "work_mem = 2MB\nWORK_MEM = 8MB\n").unwrap();
    assert!(reload());
    assert_eq!(get_int("work_mem"), Some(8192));
}

#[test]
fn seams_route_to_bodies() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let level = NewGUCNestLevel();
    AtEOXact_GUC(true, level);
    AtStart_GUC();
    AtEOXact_GUC(true, 1);
    guc_seams::set_config_option_internal_dynamic_default::call("application_name", "seamtest")
        .unwrap();
    assert_eq!(get_string("application_name"), Some(Some("seamtest".to_string())));
}

// GUCArrayAdd/Delete + the secdef proconfig seam (fmgr_security_definer's
// GUC push/pop protocol).
fn array_setup() {
    setup();
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        superuser_seams::superuser::set(|| Ok(true));
    });
    miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);
}

#[test]
fn guc_array_add_replaces_in_place_and_deletes() {
    array_setup();
    let a = GUCArrayAdd(&[], "work_mem", "64MB").unwrap();
    assert_eq!(a, vec!["work_mem=64MB".to_string()]);
    let a = GUCArrayAdd(&a, "enable_seqscan", "off").unwrap();
    assert_eq!(a.len(), 2);
    let a = GUCArrayAdd(&a, "work_mem", "128MB").unwrap();
    assert_eq!(a, vec!["work_mem=128MB".to_string(), "enable_seqscan=off".to_string()]);
    let a = GUCArrayDelete(&a, "work_mem").unwrap().unwrap();
    assert_eq!(a, vec!["enable_seqscan=off".to_string()]);
    assert!(GUCArrayDelete(&a, "enable_seqscan").unwrap().is_none());
}

#[test]
fn guc_array_add_validates_name_and_value() {
    array_setup();
    let e = GUCArrayAdd(&[], "no_such_setting", "x").unwrap_err();
    assert!(e.message().contains("unrecognized configuration parameter"), "{}", e.message());
    let e = GUCArrayAdd(&[], "work_mem", "banana").unwrap_err();
    assert!(e.message().contains("invalid value for parameter"), "{}", e.message());
}

// guc.c:6745: validate_option_array_item looks the name up with
// skip_errors = skipIfNoPermissions || reset_custom, so an unknown custom
// name under a reserved prefix raises assignable_custom_variable_name's
// 42602 ("... is a reserved prefix.") from GUCArrayAdd, while RESET (value
// NULL, reset_custom) skips the lookup errors and reaches the placeholder
// permission check. Audit a186-candidate-fp-misc-guc-p3-4c87efc31a803236318e-1.
#[test]
fn guc_array_add_reserved_prefix_is_invalid_name() {
    array_setup();
    MarkGUCPrefixReserved("b035rsv");
    let e = GUCArrayAdd(&[], "b035rsv.bogus", "x").unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_NAME, "{}", e.message());
    assert_eq!(e.message(), "invalid configuration parameter name \"b035rsv.bogus\"");
    assert_eq!(e.detail(), Some("\"b035rsv\" is a reserved prefix."));
    // Malformed custom names take the same skip_errors=false path (42602).
    let e = GUCArrayAdd(&[], "b035rsv.bad..name", "x").unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_NAME, "{}", e.message());
    // RESET of the same unknown reserved-prefix name is allowed (superuser).
    assert!(GUCArrayDelete(&["other.x=1".to_string()], "b035rsv.bogus").unwrap().is_some());
    // A plain unknown name is still 42704.
    let e = GUCArrayAdd(&[], "no_such_setting_b035", "x").unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNDEFINED_OBJECT, "{}", e.message());
}

#[test]
fn guc_array_add_rejects_postmaster_guc() {
    array_setup();
    // C validate_option_array_item calls set_config_option with
    // superuser()?PGC_SUSET:PGC_USERSET (guc.c:6780-6782), never the
    // variable's PGC_POSTMASTER context. Unfixed: Ok.
    let e = GUCArrayAdd(&[], "max_connections", "100").unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_CANT_CHANGE_RUNTIME_PARAM);
    assert!(
        e.message().contains("cannot be changed without restarting the server"),
        "{}",
        e.message()
    );
}

#[test]
fn process_guc_array_secdef_pushes_and_nest_pop_restores() {
    array_setup();
    assert_eq!(get_int("work_mem"), Some(4096));
    let nest = NewGUCNestLevel();
    guc_seams::process_guc_array_secdef::call(&["work_mem=64MB".to_string()]).unwrap();
    assert_eq!(get_int("work_mem"), Some(65536));
    AtEOXact_GUC(true, nest);
    assert_eq!(get_int("work_mem"), Some(4096));
}

#[test]
fn session_bind_transfers_leader_state() {
    setup();
    assert_eq!(set_session("cursor_tuple_fraction", Some("0.25")).unwrap(), 1);
    assert_eq!(
        set_config_option_ext(
            "statement_timeout",
            Some("7s"),
            PGC_SIGHUP,
            PGC_S_FILE,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            false,
        )
        .unwrap(),
        1
    );
    let caps = crate::store::capture_session_gucs();
    std::thread::spawn(move || {
        setup();
        assert!(!crate::store::session_bound());
        let binding = crate::store::bind_session_gucs(&caps).unwrap();
        assert!(crate::store::session_bound());
        assert_eq!(get_real("cursor_tuple_fraction"), Some(0.25));
        assert_eq!(get_int("statement_timeout"), Some(7000));
        // Session-sourced bind resets to the boot default; file-sourced bind
        // became the reset value (make_default), exactly as restore would.
        set_config_option_ext(
            "cursor_tuple_fraction",
            None,
            PGC_USERSET,
            PGC_S_SESSION,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            false,
        )
        .unwrap();
        assert_eq!(get_real("cursor_tuple_fraction"), Some(0.1));
        set_config_option_ext(
            "statement_timeout",
            None,
            PGC_USERSET,
            PGC_S_SESSION,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            false,
        )
        .unwrap();
        assert_eq!(get_int("statement_timeout"), Some(7000));
        drop(binding);
        assert!(!crate::store::session_bound());
    })
    .join()
    .unwrap();
}

#[test]
fn session_bind_guard_rejects_double_bind() {
    setup();
    let caps = crate::store::capture_session_gucs();
    std::thread::spawn(move || {
        setup();
        let _binding = crate::store::bind_session_gucs(&caps).unwrap();
        let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::store::bind_session_gucs(&caps)
        }));
        assert!(again.is_err(), "second bind on a bound thread must panic");
    })
    .join()
    .unwrap();
}

#[test]
fn session_bind_matches_string_restore_end_state() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(set_session("work_mem", Some("8MB")).unwrap(), 1);
    assert_eq!(set_session("application_name", Some("bindcheck")).unwrap(), 1);
    // Restrict both legs to the vars this test set: the unit-test env lacks
    // the owning units of several always-nondefault vars (external enum
    // options slots), which the string-restore leg would have to re-parse.
    let touched = ["work_mem", "application_name"];
    let mut caps = crate::store::capture_session_gucs();
    caps.retain(|c| touched.contains(&c.name()));
    let mut strings = crate::store::capture_nondefault_variables();
    strings.retain(|v| touched.contains(&v.name.as_str()));
    assert_eq!(caps.len(), 2);
    assert_eq!(strings.len(), 2);
    let bound = std::thread::spawn(move || {
        setup();
        let _binding = crate::store::bind_session_gucs(&caps).unwrap();
        with_store(|reg| {
            reg.iter()
                .filter(|v| ["work_mem", "application_name"].contains(&v.name()))
                .map(|v| {
                    (
                        v.name().to_string(),
                        crate::registry::show_guc_option(v, false),
                        v.gen().source,
                        v.gen().scontext,
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap()
    })
    .join()
    .unwrap();
    let restored = std::thread::spawn(move || {
        setup();
        crate::store::restore_nondefault_variables(&strings).unwrap();
        with_store(|reg| {
            reg.iter()
                .filter(|v| ["work_mem", "application_name"].contains(&v.name()))
                .map(|v| {
                    (
                        v.name().to_string(),
                        crate::registry::show_guc_option(v, false),
                        v.gen().source,
                        v.gen().scontext,
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap()
    })
    .join()
    .unwrap();
    assert_eq!(bound, restored);
}

thread_local! {
    static HAS_PRIVS: Cell<bool> = const { Cell::new(false) };
}

// GetConfigOption(restrict_privileged=true) over a GUC_SUPERUSER_ONLY
// option: C's ConfigOptionIsVisible gate — has_privs_of_role(GetUserId(),
// ROLE_PG_READ_ALL_SETTINGS) — with the exact 42501 error. Pre-fix this
// panicked instead of resolving the privilege check.
#[test]
fn get_config_option_superuser_only_gate() {
    setup();
    static SEAM: Once = Once::new();
    SEAM.call_once(|| {
        acl_seams::has_privs_of_role::set(|_member, role| {
            assert_eq!(role, 3374, "pg_read_all_settings");
            Ok(HAS_PRIVS.get())
        });
    });
    miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);

    HAS_PRIVS.set(false);
    let err = GetConfigOption("krb_server_keyfile", false, true).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);
    assert_eq!(err.message(), "permission denied to examine \"krb_server_keyfile\"");
    assert_eq!(
        err.detail(),
        Some(
            "Only roles with privileges of the \"pg_read_all_settings\" role may examine this parameter."
        )
    );
    // Unprivileged read without the restriction still passes (C callers
    // that pass restrict_privileged=false skip the gate entirely).
    assert!(GetConfigOption("krb_server_keyfile", false, false).unwrap().is_some());
    // With the role privilege the restricted read passes too.
    HAS_PRIVS.set(true);
    assert!(GetConfigOption("krb_server_keyfile", false, true).unwrap().is_some());
    // Non-superuser-only options never consult the gate.
    HAS_PRIVS.set(false);
    assert!(GetConfigOption("work_mem", false, true).unwrap().is_some());
}

// idx 114 regression: a reused pooled parallel-worker thread must not carry a
// PRIOR session's client-startup / ALTER ROLE|DATABASE SET GUCs (sources <=
// PGC_S_OVERRIDE) into another session's task. ResetAllOptions cannot evict
// that class (it preserves those values and their reset_val stamps, by C
// parity); reset_store_to_process_base scrubs the thread to the fresh-backend
// baseline a C worker process would start from.
#[test]
fn reset_store_to_process_base_evicts_prior_session_low_source_gucs() {
    setup();

    let name = "maintenance_work_mem";
    let boot = with_store(|reg| match reg.find_option(name).unwrap() {
        GucVariable::Int(c) => c.boot_val,
        _ => unreachable!(),
    })
    .unwrap();

    let int_state = |name: &str| -> (i32, GucSource) {
        with_store(|reg| {
            let v = reg.find_option(name).unwrap();
            let val = match v {
                GucVariable::Int(c) => c.value.unwrap(),
                _ => unreachable!(),
            };
            (val, v.gen().source)
        })
        .unwrap()
    };

    // A prior session installs a value at PGC_S_CLIENT (a client startup
    // option). This is the make_default path: it also stamps reset_val.
    let attacker = boot + 1024;
    set_config_option_ext(
        name,
        Some(&attacker.to_string()),
        PGC_USERSET,
        PGC_S_CLIENT,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(int_state(name).0, attacker);

    // RESET ALL preserves it (source <= PGC_S_OVERRIDE) -- exactly why it is
    // insufficient on a reused pooled thread.
    crate::store::reset_all_options();
    assert_eq!(
        int_state(name).0,
        attacker,
        "ResetAllOptions must (C parity) keep a <= PGC_S_OVERRIDE value"
    );

    // The fix: scrub the reused thread to the session-neutral fresh baseline.
    // (Exercise the registry-level scrub directly; the public
    // reset_store_to_process_base wraps this and then re-overlays the
    // process-global postmaster base, which is not deterministic under the
    // shared test binary.)
    with_store_mut(crate::registry::reset_session_options_to_boot).unwrap();

    let (value, source) = int_state(name);
    assert_eq!(value, boot, "scrub must return the value to its boot default");
    assert_eq!(source, PGC_S_DEFAULT, "scrub must clear the per-session source");

    // The poisoned reset_val stamp must be gone too, so a later RESET cannot
    // resurrect the prior session's value.
    let reset_val = with_store(|reg| match reg.find_option(name).unwrap() {
        GucVariable::Int(c) => c.reset_val,
        _ => unreachable!(),
    })
    .unwrap();
    assert_eq!(reset_val, boot, "scrub must rebuild reset_val to boot");
}

fn reentrant_string_check(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: GucSource,
) -> types_error::PgResult<bool> {
    // check_datestyle's shape: 'DEFAULT' resolves against the reset value.
    if newval.as_deref() == Some("reset") {
        *newval = Some(GetConfigOptionResetString("createrole_self_grant").expect("reset value"));
    }
    Ok(true)
}

#[test]
fn check_hook_may_read_the_store() {
    setup();
    guc_tables::hooks::check_createrole_self_grant.install_if_absent(reentrant_string_check);
    assert_eq!(set_session("createrole_self_grant", Some("set, inherit")).unwrap(), 1);
    assert_eq!(get_string("createrole_self_grant"), Some(Some("set, inherit".to_string())));
    assert_eq!(set_session("createrole_self_grant", Some("reset")).unwrap(), 1);
    assert_eq!(get_string("createrole_self_grant"), Some(Some(String::new())));
}

// help_config.c printMixedStruct rows as postgres 18.6 --describe-config
// prints them (bool/int/real show the pre-InitializeOneGUCOption reset_val
// zero; strings/enums their boot_val), displayStruct hiding, C ordering.
#[test]
fn describe_config_rows_match_c() {
    setup();
    // Enum option sets that live in GUC slots are installed by their owning
    // units (transam_xlog/dsm/aio), which this test binary does not link;
    // stub them empty (the guc_funcs SHOW ALL tests do the same) — their
    // rows print an empty value column here, so the asserted ENUM row is an
    // Inline-options one.
    for slot in [
        &guc_tables::option_sets::archive_mode_options,
        &guc_tables::option_sets::dynamic_shared_memory_options,
        &guc_tables::option_sets::io_method_options,
        &guc_tables::option_sets::recovery_target_action_options,
        &guc_tables::option_sets::wal_level_options,
        &guc_tables::option_sets::wal_sync_method_options,
    ] {
        slot.install_if_absent(&[]);
    }
    let text = crate::help_config::guc_info_text();
    let lines: Vec<&str> = text.lines().collect();
    for want in [
        "enable_seqscan\tuser\tQuery Tuning / Planner Method Configuration\tBOOLEAN\tFALSE\t\t\tEnables the planner's use of sequential-scan plans.\t",
        "shared_buffers\tpostmaster\tResource Usage / Memory\tINTEGER\t0\t16\t1073741823\tSets the number of shared memory buffers used by the server.\t",
        "cpu_tuple_cost\tuser\tQuery Tuning / Planner Cost Constants\tREAL\t0\t0\t1.79769e+308\tSets the planner's estimate of the cost of processing each tuple (row).\t",
        "log_line_prefix\tsighup\tReporting and Logging / What to Log\tSTRING\t%m [%p] \t\t\tControls information prefixed to each log line.\tAn empty string means no prefix.",
        "wal_compression\tsuperuser\tWrite-Ahead Log / Settings\tENUM\toff\t\t\tCompresses full-page writes written in WAL file with specified method.\t",
    ] {
        assert!(lines.contains(&want), "missing row: {want:?}");
    }
    // displayStruct: GUC_DISALLOW_IN_FILE / GUC_NOT_IN_SAMPLE / GUC_NO_SHOW_ALL
    // rows never print (config_file is GUC_DISALLOW_IN_FILE; C omits it).
    assert!(!lines.iter().any(|l| l.starts_with("config_file\t")), "config_file must be hidden");
    // guc_var_compare order.
    for w in lines.windows(2) {
        let (a, b) = (w[0].split('\t').next().unwrap(), w[1].split('\t').next().unwrap());
        assert_ne!(guc_name_compare(a, b), std::cmp::Ordering::Greater, "{a} before {b}");
    }
    assert!(lines.len() >= 300, "only {} rows", lines.len());
}

// reapply_stacked_values (guc.c:5054): oldvarstack is captured before the
// recursion, so an entry the recursion pushed takes this level's nest level
// even when this level's own assignment was rejected.
#[test]
fn reapply_adjusts_recursion_pushed_entry_when_own_assignment_fails() {
    setup();
    AtStart_GUC();
    assert_eq!(set_session("b168nest.v", Some("one")).unwrap(), 1);
    let inner = NewGUCNestLevel();
    let rc = set_config_option_ext(
        "b168nest.v",
        Some("two"),
        PGC_USERSET,
        PGC_S_SESSION,
        DENIED_ROLE,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(rc, 1);
    DefineCustomStringVariable("b168nest.v", Some("desc"), None, None, PGC_SUSET, 0).unwrap();
    assert_eq!(show("b168nest.v"), Some("one".to_string()));
    AtEOXact_GUC(false, inner);
    assert_eq!(show("b168nest.v"), Some(String::new()));
    AtEOXact_GUC(true, 1);
}

// MarkGUCPrefixReserved (guc.c:5298): the removal WARNINGs come in
// guc_hashtab bucket order (C 18.6 on these names: zed b c a f g d e h
// long_name_x) and run the error-context callbacks like any ereport.
#[test]
fn prefix_reservation_warnings_in_hash_order_with_context() {
    setup();
    for name in ["a", "b", "c", "d", "e", "f", "g", "h", "long_name_x", "zed"] {
        assert_eq!(set_session(&format!("postgres_fdw.{name}"), Some("1")).unwrap(), 1);
    }
    let callback = elog::push_emit_context_callback(Box::new(|e| {
        e.add_context_line("SQL statement \"LOAD 'postgres_fdw'\"");
    }));
    let prev = elog::set_emit_log_hook(Some(capture_emitted));
    MarkGUCPrefixReserved("postgres_fdw");
    elog::set_emit_log_hook(prev);
    elog::pop_emit_context_callback(callback);
    let emitted = std::mem::take(&mut *EMITTED.lock().unwrap_or_else(|e| e.into_inner()));
    let names: Vec<String> = emitted
        .iter()
        .map(|e| {
            assert_eq!(e.level, types_error::WARNING);
            assert_eq!(e.detail(), Some("\"postgres_fdw\" is now a reserved prefix."));
            assert_eq!(e.context(), Some("SQL statement \"LOAD 'postgres_fdw'\""));
            e.message()
                .trim_start_matches("invalid configuration parameter name \"postgres_fdw.")
                .trim_end_matches("\", removing it")
                .to_string()
        })
        .collect();
    assert_eq!(names, ["zed", "b", "c", "a", "f", "g", "d", "e", "h", "long_name_x"]);
}

// ReportGUCOption (guc.c:2645) sends both strings through pq_sendstring's
// client_encoding conversion.
#[test]
fn parameter_status_is_sent_in_client_encoding() {
    setup();
    let _guard = APPLICATION_NAME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
    begin_reporting_guc_options();
    SENT.with(|s| s.borrow_mut().clear());
    assert_eq!(set_session("application_name", Some("conv:psql")).unwrap(), 1);
    report_changed_guc_options();
    let frames = SENT.with(|s| std::mem::take(&mut *s.borrow_mut()));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].1, b"application_name\0CONV:PSQL\0".to_vec());
    assert_eq!(show("application_name"), Some("conv:psql".to_string()));
}

// guc.c:5245: a postmaster-time (shared_preload_libraries) custom definition
// survives into a child's rebuilt registry, ahead of its reserved prefix.
#[test]
fn inherited_custom_definitions_survive_child_registry_rebuild() {
    setup();
    DefineCustomStringVariable("b168pre.name", Some("desc"), None, Some("boot"), PGC_USERSET, 0)
        .unwrap();
    MarkGUCPrefixReserved("b168pre");
    let definitions = custom_string_definitions();
    assert!(definitions.iter().any(|d| d.name == "b168pre.name"));
    let prefixes = reserved_class_prefixes();
    std::thread::spawn(move || {
        setup();
        inherit_reserved_class_prefixes(&prefixes);
        inherit_custom_string_definitions(&definitions);
        crate::store::initialize_guc_options_for_child(&[]).unwrap();
        assert_eq!(show("b168pre.name"), Some("boot".to_string()));
        assert_eq!(set_session("b168pre.name", Some("x")).unwrap(), 1);
        assert_eq!(show("b168pre.name"), Some("x".to_string()));
    })
    .join()
    .unwrap();
}

// guc.c: GUC_PENDING_RESTART set in the postmaster is inherited by every
// later fork, so a fresh session reports pg_settings.pending_restart = true
// for a PGC_POSTMASTER variable ALTER SYSTEM changed since boot.
#[test]
fn pending_restart_status_crosses_the_child_bind() {
    setup();
    let changed = set_config_option_ext(
        "shared_buffers",
        Some("32768"),
        PGC_SIGHUP,
        PGC_S_FILE,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ErrorLevel(0),
        false,
    )
    .unwrap();
    assert_eq!(changed, 0);
    let caps = crate::store::capture_session_gucs();
    let cap = caps.iter().find(|c| c.name() == "shared_buffers").expect("captured");
    assert!(cap.pending_restart());
    assert_eq!(cap.source(), PGC_S_DEFAULT);
    let base = crate::layers::GucBaseSnapshot::for_tests(crate::store::capture_session_gucs());
    assert!(!base.contains("shared_buffers"));
    std::thread::spawn(move || {
        setup();
        let _binding = crate::store::bind_session_gucs(&caps).unwrap();
        let (status, source) = with_store(|reg| {
            let v = reg.find_option("shared_buffers").unwrap();
            (v.gen().status, v.gen().source)
        })
        .unwrap();
        assert!(status & crate::model::GUC_PENDING_RESTART != 0);
        assert_eq!(source, PGC_S_DEFAULT);
    })
    .join()
    .unwrap();
}
