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
