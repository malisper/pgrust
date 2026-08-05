// file_copy_method's "clone" entry tracks clone_file (copydir.c) support the
// way C compile-gates it (guc_tables.c:489). Product builds carry the entry
// (macOS copyfile, Linux copy_file_range), so SET file_copy_method=clone is
// accepted there. Sim builds gate it out — sim fds are foreign to the kernel
// clone syscalls — presenting the same surface as a C build without
// HAVE_COPYFILE/HAVE_COPY_FILE_RANGE: the refusal is the stock invalid-value
// ERROR with the available-values hint, never accepted-then-panic (the
// GL-INERT-AUDIT-1 incident class).

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

#[cfg(not(pgrust_sim))]
#[test]
fn set_clone_is_accepted() {
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
    .expect("clone is ported and product builds carry the entry");

    // The boot-default value still works.
    guc::SetConfigOption(
        "file_copy_method",
        Some("copy"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .expect("copy is the boot-default arm");
}

#[cfg(pgrust_sim)]
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
    .expect_err("clone is gated out under pgrust_sim and must not be accepted");
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
    .expect("copy is the boot-default arm");
}
