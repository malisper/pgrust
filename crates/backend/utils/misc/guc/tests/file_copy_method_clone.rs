// file_copy_method=clone must refuse at SET time: clone_file (copydir.c) is
// unported, and the accepted-then-panic shape (USERSET value detonating later
// inside CREATE DATABASE's copydir) is the GL-INERT-AUDIT-1 incident class.
// C compile-gates the "clone" entry out of file_copy_method_options on
// platforms without support; pgrust ships the same pruned list, so the
// refusal is the stock invalid-value ERROR with the available-values hint.

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

#[test]
fn set_clone_is_a_clean_invalid_value_error() {
    setup_seams();
    guc::store::initialize_guc_options().unwrap();

    // PGC_S_ARGV sidesteps the session-user fixture; the enum-value lookup
    // under test is source-independent (SET goes through the same lookup).
    let err = guc::SetConfigOption(
        "file_copy_method",
        Some("clone"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect_err("clone is unported and must not be accepted");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("invalid value for parameter"),
        "expected the stock enum rejection, got: {msg}"
    );

    // The ported value still works.
    guc::SetConfigOption(
        "file_copy_method",
        Some("copy"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect("copy is the ported arm");
}
