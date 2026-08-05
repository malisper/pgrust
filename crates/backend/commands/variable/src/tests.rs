use std::sync::Once;

use adt_datetime::consts::{DATEORDER_MDY, DATEORDER_YMD, USE_GERMAN_DATES, USE_ISO_DATES};
use adt_datetime::tz;
use types_guc::*;

use crate::*;

fn test_parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        guc_tables::init_seams();
        pgtz::init_seams();
        elog::init_seams();
        guc::init_seams();
        init_seams();
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        scalar_seams::parse_bool::set(test_parse_bool);
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(mbutils::GetDatabaseEncoding);
        pqcomm_seams::pq_putmessage::set(|_, _| Ok(0));
        timestamp_seams::get_current_timestamp::set(|| 42);
    });
    guc::initialize_guc_options().unwrap();
}

#[test]
fn boot_defaults_flow_through_hooks() {
    setup();
    assert_eq!(guc::store::get_string("DateStyle").unwrap().as_deref(), Some("ISO, MDY"));
    assert_eq!(adt_datetime::settings::date_style(), USE_ISO_DATES);
    assert_eq!(adt_datetime::settings::date_order(), DATEORDER_MDY);
    assert!(tz::session_timezone().is_some());
    assert!(tz::log_timezone().is_some());
    assert_eq!(show_timezone(), "GMT");
    assert_eq!(show_log_timezone(), "GMT");
    assert_eq!(show_random_seed(), "unavailable");
    assert_eq!(show_role(), "none");
    assert_eq!(io_combine_limit(), 16);
}

#[test]
fn datestyle_canonicalizes_and_assigns() {
    setup();
    let mut val = Some("german, ymd".to_string());
    let mut extra = None;
    assert!(check_datestyle(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("German, YMD"));
    assign_datestyle(val.as_deref(), extra.as_ref());
    assert_eq!(adt_datetime::settings::date_style(), USE_GERMAN_DATES);
    assert_eq!(adt_datetime::settings::date_order(), DATEORDER_YMD);
    // GERMAN implies DMY unless order is given explicitly.
    let mut val = Some("German".to_string());
    let mut extra = None;
    assert!(check_datestyle(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("German, DMY"));
    adt_datetime::settings::set_date_style(USE_ISO_DATES);
    adt_datetime::settings::set_date_order(DATEORDER_MDY);
}

#[test]
fn datestyle_conflicts_and_garbage_fail() {
    setup();
    guc::reset_guc_check_error();
    let mut val = Some("ISO, SQL".to_string());
    assert!(!check_datestyle(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(
        guc::take_guc_check_error().detail.as_deref(),
        Some("Conflicting \"DateStyle\" specifications.")
    );
    guc::reset_guc_check_error();
    let mut val = Some("bogus".to_string());
    assert!(!check_datestyle(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(
        guc::take_guc_check_error().detail.as_deref(),
        Some("Unrecognized key word: \"bogus\".")
    );
    let mut val = Some("DEFAULT, ISO".to_string());
    assert!(check_datestyle(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("ISO, MDY"));
}

#[test]
fn timezone_zero_offset_is_gmt() {
    setup();
    let mut val = Some("0".to_string());
    let mut extra = None;
    assert!(check_timezone(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assign_timezone(val.as_deref(), extra.as_ref());
    assert_eq!(show_timezone(), "<+00>-00");
}

#[test]
fn timezone_numeric_hours_use_offset_zone() {
    setup();
    let mut val = Some("5".to_string());
    let mut extra = None;
    assert!(check_timezone(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assign_timezone(val.as_deref(), extra.as_ref());
    assert_eq!(show_timezone(), "<+05>-05");
}

#[test]
fn timezone_iana_zone_through_real_engine() {
    setup();
    let mut val = Some("America/New_York".to_string());
    let mut extra = None;
    if check_timezone(&mut val, &mut extra, PGC_S_SESSION).unwrap() {
        assign_timezone(val.as_deref(), extra.as_ref());
        assert_eq!(show_timezone(), "America/New_York");
    }
    let mut val = Some("Not/A_Zone".to_string());
    assert!(!check_timezone(&mut val, &mut None, PGC_S_SESSION).unwrap());
}

#[test]
fn timezone_abbreviations_load_and_install() {
    setup();
    let installs = [
        "/tmp/pgrust_pginstall/share/postgresql",
        "/opt/homebrew/share/postgresql@18",
    ];
    let Some(share) = installs
        .into_iter()
        .find(|d| std::path::Path::new(&format!("{d}/timezonesets")).is_dir())
    else {
        return;
    };
    std::env::set_var("PGRUST_PGSHAREDIR", share);

    let mut extra = None;
    let mut val = Some("Default".to_string());
    assert!(check_timezone_abbreviations(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assert!(extra.is_some());
    assign_timezone_abbreviations(val.as_deref(), extra.as_ref());
    let tbl = tz::zoneabbrevtbl().expect("table installed");
    let est = adt_datetime::decode::datebsearch(b"est", tbl.abbrevs).expect("est known");
    assert_eq!(est.value, -18000);

    let mut bad = Some("NoSuchFileHere".to_string());
    assert!(!check_timezone_abbreviations(&mut bad, &mut None, PGC_S_SESSION).unwrap());
}

#[test]
fn transaction_hooks_allow_changes_outside_transaction() {
    setup();
    let mut rw = false;
    assert!(check_transaction_read_only(&mut rw, &mut None, PGC_S_SESSION).unwrap());
    let mut iso = types_core::XACT_SERIALIZABLE;
    assert!(check_transaction_isolation(&mut iso, &mut None, PGC_S_SESSION).unwrap());
    let mut def = true;
    assert!(check_transaction_deferrable(&mut def, &mut None, PGC_S_SESSION).unwrap());
}

#[test]
fn random_seed_arms_only_for_interactive_sources() {
    setup();
    let mut extra = None;
    assert!(check_random_seed(&mut 0.5, &mut extra, PGC_S_DEFAULT).unwrap());
    assign_random_seed(0.5, extra.as_ref());
}

#[test]
fn random_seed_interactive_assign_reseeds_via_setseed() {
    setup();
    let mut extra = None;
    check_random_seed(&mut 0.5, &mut extra, PGC_S_SESSION).unwrap();
    assign_random_seed(0.5, extra.as_ref());
    // setseed(0.5) tape (Homebrew C 18.3): drandom() must resume it.
    assert_eq!(pseudorandomfuncs::drandom(), 0.9851677175347999);
    // Armed flag is one-shot: a second assign with the same extra is a no-op.
    assign_random_seed(0.25, extra.as_ref());
    assert_eq!(pseudorandomfuncs::drandom(), 0.825301858027981);
}

#[test]
fn client_encoding_canonicalizes_with_unicode_kluge() {
    setup();
    let mut val = Some("utf-8".to_string());
    let mut extra = None;
    assert!(check_client_encoding(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("UTF8"));
    assert_eq!(extra.as_ref().unwrap().downcast_ref::<i32>(), Some(&wchar::PG_UTF8));

    let mut val = Some("UNICODE".to_string());
    assert!(check_client_encoding(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("UNICODE"));

    let mut val = Some("no-such-encoding".to_string());
    assert!(!check_client_encoding(&mut val, &mut None, PGC_S_SESSION).unwrap());
}

#[test]
fn role_none_is_hardwired() {
    setup();
    let mut val = Some("none".to_string());
    let mut extra = None;
    assert!(check_role(&mut val, &mut extra, PGC_S_SESSION).unwrap());
    assign_role(val.as_deref(), extra.as_ref());
    assert_eq!(show_role(), "none");
}

#[test]
fn session_authorization_null_and_no_transaction() {
    setup();
    let mut val = None;
    assert!(check_session_authorization(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assign_session_authorization(None, None);
    let mut val = Some("alice".to_string());
    assert!(!check_session_authorization(&mut val, &mut None, PGC_S_SESSION).unwrap());
}

#[test]
fn canonicalize_path_matches_path_c() {
    assert_eq!(canonicalize_path("/a//b/./c/.."), "/a/b");
    assert_eq!(canonicalize_path("/../.."), "/");
    assert_eq!(canonicalize_path("../.."), "../..");
    assert_eq!(canonicalize_path("../dir/.."), "..");
    assert_eq!(canonicalize_path("a/.."), ".");
    assert_eq!(canonicalize_path("/a/b///"), "/a/b");
    assert_eq!(canonicalize_path("/"), "/");
    assert_eq!(canonicalize_path("log"), "log");
}

#[test]
fn clean_ascii_hex_escapes_non_printables() {
    let mut val = Some("h\u{00e9}llo\u{0007}".to_string());
    assert!(check_application_name(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("h\\xc3\\xa9llo\\x07"));
    let mut val = Some("plain".to_string());
    assert!(check_cluster_name(&mut val, &mut None, PGC_S_SESSION).unwrap());
    assert_eq!(val.as_deref(), Some("plain"));
}

#[test]
fn io_combine_limits_track_min_of_pair() {
    setup();
    assign_io_max_combine_limit(8, None);
    assert_eq!(io_combine_limit(), 8);
    assign_io_combine_limit(4, None);
    assert_eq!(io_combine_limit(), 4);
    assign_io_max_combine_limit(2, None);
    assert_eq!(io_combine_limit(), 2);
    assign_io_max_combine_limit(16, None);
    assign_io_combine_limit(16, None);
    assert_eq!(io_combine_limit(), 16);
}

#[test]
fn octal_show_hooks() {
    setup();
    assert_eq!(show_data_directory_mode(), "0700");
    assert_eq!(show_log_file_mode(), "0600");
    assert_eq!(show_unix_socket_permissions(), "0777");
}

#[test]
fn build_gate_checks() {
    setup();
    let mut on = true;
    assert!(!check_bonjour(&mut on, &mut None, PGC_S_SESSION).unwrap());
    let mut on = true;
    assert!(check_ssl(&mut on, &mut None, PGC_S_SESSION).unwrap());
    let mut on = true;
    assert!(!check_default_with_oids(&mut on, &mut None, PGC_S_SESSION).unwrap());
    let mut off = false;
    assert!(check_bonjour(&mut off, &mut None, PGC_S_SESSION).unwrap());
}
