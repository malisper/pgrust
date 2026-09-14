//! Unit tests for the logical.c port.

use std::sync::Mutex;

use types_error::ERRCODE_INSUFFICIENT_PRIVILEGE;

// Stand-in for the product boot's GUC storage; one process-wide cell, so
// every value-dependent assertion lives in the single test below.
static OUTPUT_PLUGIN_LIBRARIES: Mutex<Option<String>> = Mutex::new(None);

fn set_output_plugin_libraries(v: Option<&str>) {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        guc_tables::vars::output_plugin_libraries_string.install(guc_tables::GucVarAccessors {
            get: || OUTPUT_PLUGIN_LIBRARIES.lock().unwrap().clone(),
            set: |v| *OUTPUT_PLUGIN_LIBRARIES.lock().unwrap() = v,
        });
    });
    *OUTPUT_PLUGIN_LIBRARIES.lock().unwrap() = v.map(str::to_owned);
}

// upstream 2a29b607dbbb (18.6): StartupDecodingContext refuses any plugin
// output_plugin_libraries does not name, superusers included.
#[test]
fn output_plugin_must_be_named_in_output_plugin_libraries() {
    // The boot default blesses both built-in plugins.
    set_output_plugin_libraries(Some("pgoutput, test_decoding"));
    assert!(super::check_output_plugin_allowed("pgoutput").is_ok());
    assert!(super::check_output_plugin_allowed("test_decoding").is_ok());

    // Refused with C's message, SQLSTATE, log-only detail and client hint.
    set_output_plugin_libraries(Some("pgoutput"));
    let err = super::check_output_plugin_allowed("test_decoding").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    assert_eq!(
        err.message(),
        "library \"test_decoding\" may not be used as an output plugin"
    );
    assert_eq!(
        err.detail_log(),
        Some("The configuration parameter \"output_plugin_libraries\" (currently 'pgoutput') does not name this library as a trusted output plugin.")
    );
    assert_eq!(
        err.hint(),
        Some("If it is safe for all REPLICATION users to use this library as an output plugin, add it to \"output_plugin_libraries\" and reload the server configuration.")
    );

    // Matching is exact: no case folding, no path or suffix variants.
    assert!(super::check_output_plugin_allowed("PGOUTPUT").is_err());
    assert!(super::check_output_plugin_allowed("pgoutput.so").is_err());
    assert!(super::check_output_plugin_allowed("$libdir/pgoutput").is_err());

    // GUC_LIST_QUOTE syntax: a quoted item matches its unquoted content.
    set_output_plugin_libraries(Some("\"test_decoding\" , pgoutput"));
    assert!(super::check_output_plugin_allowed("test_decoding").is_ok());

    // Empty, unset or malformed (logged) lists bless nothing.
    set_output_plugin_libraries(Some(""));
    assert!(super::check_output_plugin_allowed("pgoutput").is_err());
    set_output_plugin_libraries(None);
    assert!(super::check_output_plugin_allowed("pgoutput").is_err());
    set_output_plugin_libraries(Some("pgoutput,,test_decoding"));
    assert!(super::check_output_plugin_allowed("pgoutput").is_err());
}

// ---- audit-remediation b060 witnesses ---------------------------------------

// logical.c:340: ctx->twophase is set when ANY of begin_prepare_cb,
// prepare_cb, commit_prepared_cb, rollback_prepared_cb, stream_prepare_cb or
// filter_prepare_cb is registered (row
// a186-candidate-fp-logical-logical-da164a0c0e8328ab5f83-1).
#[test]
fn twophase_enabled_by_every_prepare_family_callback() {
    fn noop_prepare(
        _: &mut crate::OutputPluginContext,
        _: &mut reorderbuffer::ReorderBuffer,
        _: reorderbuffer::TxnId,
        _: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()> {
        Ok(())
    }
    fn noop_filter_prepare(
        _: &mut crate::OutputPluginContext,
        _: types_core::TransactionId,
        _: &str,
    ) -> types_error::PgResult<bool> {
        Ok(false)
    }
    let none = crate::OutputPluginCallbacks::default();
    assert!(!crate::twophase_from_callbacks(&none));
    let mut only_stream_prepare = crate::OutputPluginCallbacks::default();
    only_stream_prepare.stream_prepare_cb = Some(noop_prepare);
    assert!(crate::twophase_from_callbacks(&only_stream_prepare));
    let mut only_filter_prepare = crate::OutputPluginCallbacks::default();
    only_filter_prepare.filter_prepare_cb = Some(noop_filter_prepare);
    assert!(crate::twophase_from_callbacks(&only_filter_prepare));
    let mut only_prepare = crate::OutputPluginCallbacks::default();
    only_prepare.prepare_cb = Some(noop_prepare);
    assert!(crate::twophase_from_callbacks(&only_prepare));
}

// logical.c:1512: "logical streaming at prepare time requires a %s callback"
// (row a186-candidate-fp-logical-logical-f4eea20753ce973e923a-1).
#[test]
fn missing_stream_prepare_cb_message_matches_c() {
    let err = crate::missing_stream_prepare_cb().unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        err.message(),
        "logical streaming at prepare time requires a stream_prepare_cb callback"
    );
}

// ---- audit-remediation b137 witnesses ---------------------------------------

// logical.c:148: the standby wal_level guard message is "... on the primary"
// with no trailing "server" (row
// a186-candidate-fp-logical-logical-5d27db2fb40ad5f19058-1).
#[test]
fn standby_wal_level_error_omits_trailing_server_word() {
    let err = super::standby_wal_level_below_logical_error();
    assert_eq!(
        err.sqlstate(),
        types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
    );
    assert_eq!(
        err.message(),
        "logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary"
    );
}

// enlargeStringInfo (stringinfo.c:357): appends are refused once they would
// reach MaxAllocSize, and the first refusal surfaces as C's 54000 with the
// (len, needed) detail at the next OutputPluginWrite.
#[test]
fn out_buffer_refuses_growth_past_max_alloc_size_like_enlarge_string_info() {
    assert!(!super::enlarge_refused(0, 1));
    assert!(!super::enlarge_refused(0x3fff_fffd, 1));
    assert!(super::enlarge_refused(0x3fff_fffe, 1));
    assert!(super::enlarge_refused(0x2000_0000, 0x1fff_ffff));
    assert!(!super::enlarge_refused(0x2000_0000, 0x1fff_fffe));

    let mut out = super::OutBuf::default();
    out.push_str("ab");
    assert!(out.check_limit().is_ok());
    out.overflow = Some((0x3fff_fffe, 1));
    let err = out.check_limit().unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(err.message(), "string buffer exceeds maximum allowed length (1073741823 bytes)");
    assert_eq!(
        err.detail(),
        Some("Cannot enlarge string buffer containing 1073741822 bytes by 1 more bytes.")
    );
    out.push_str("dropped");
    assert_eq!(out.as_bytes(), b"ab");
    out.clear();
    assert!(out.check_limit().is_ok());
}
