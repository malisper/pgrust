// file_copy_method=clone: clone_file (copydir.c:236) is ported for the two
// platforms whose C builds compile the "clone" enum entry in
// (HAVE_COPYFILE+COPYFILE_CLONE_FORCE on macOS, HAVE_COPY_FILE_RANGE on
// Linux), so SET accepts it there. Other targets keep C's pruned option list
// and the stock invalid-value ERROR with the available-values hint.

use types_guc::{GucContext, GucSource};

fn setup_seams() {
    guc_tables::init_seams();
    elog::init_seams();
    guc::init_seams();
    xact_seams::is_in_parallel_mode::set(|| false);
    scalar_seams::parse_bool::set(|value| match value {
        "true" | "on" | "yes" | "1" => Some(true),
        "false" | "off" | "no" | "0" => Some(false),
        _ => None,
    });
    aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
    mbutils_seams::get_database_encoding::set(|| 6);
    timestamp_seams::get_current_timestamp::set(|| 0);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn set_clone_is_accepted_where_the_platform_arm_exists() {
    setup_seams();
    guc::store::initialize_guc_options().unwrap();

    // PGC_S_ARGV sidesteps the session-user fixture; the enum-value lookup
    // under test is source-independent (SET goes through the same lookup).
    guc::SetConfigOption(
        "file_copy_method",
        Some("clone"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect("clone_file is ported on this platform");

    // The default value still works.
    guc::SetConfigOption(
        "file_copy_method",
        Some("copy"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect("copy is the boot default");
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[test]
fn set_clone_is_a_clean_invalid_value_error() {
    setup_seams();
    guc::store::initialize_guc_options().unwrap();

    let err = guc::SetConfigOption(
        "file_copy_method",
        Some("clone"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect_err("no clone arm on this platform");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("invalid value for parameter"),
        "expected the stock enum rejection, got: {msg}"
    );
}
