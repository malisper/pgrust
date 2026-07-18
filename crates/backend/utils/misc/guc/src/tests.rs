use std::cell::RefCell;
use std::sync::Once;

use types_core::BOOTSTRAP_SUPERUSERID;
use types_error::ErrorLevel;
use types_guc::*;

use crate::*;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
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
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(|| 6);
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
