use super::*;

#[test]
fn table_counts_match_compiled_backend_shape() {
    assert_eq!(ConfigureNamesBool.len(), 115);
    assert_eq!(ConfigureNamesInt.len(), 147);
    assert_eq!(ConfigureNamesReal.len(), 26);
    assert_eq!(ConfigureNamesString.len(), 75);
    assert_eq!(ConfigureNamesEnum.len(), 41);
    assert_eq!(all_settings().count(), 404);
    assert_eq!(GucContext_Names.len(), PGC_USERSET as usize + 1);
    assert_eq!(GucSource_Names.len(), PGC_S_SESSION as usize + 1);
    assert_eq!(config_group_names.len(), DEVELOPER_OPTIONS as usize + 1);
    assert_eq!(config_type_names.len(), PGC_ENUM as usize + 1);
}

#[test]
fn common_options_are_present_with_postgres_defaults() {
    let seqscan = find_option("enable_seqscan").unwrap();
    assert_eq!(seqscan.value_kind(), GucValueKind::Bool);
    assert_eq!(seqscan.default_value(), GucDefaultValue::Bool(true));
    assert_eq!(seqscan.group(), QUERY_TUNING_METHOD);
    assert_eq!(seqscan.variable(), "enable_seqscan");

    let stack = find_option("max_stack_depth").unwrap();
    assert_eq!(stack.value_kind(), GucValueKind::Int);
    assert_eq!(stack.default_value(), GucDefaultValue::Int(100));
    assert_eq!(stack.check_hook(), Some("check_max_stack_depth"));
    assert_eq!(stack.assign_hook(), Some("assign_max_stack_depth"));

    let log_destination = find_option("log_destination").unwrap();
    assert_eq!(log_destination.value_kind(), GucValueKind::String);
    assert_eq!(
        log_destination.default_value(),
        GucDefaultValue::String(Some("stderr"))
    );
    assert_eq!(log_destination.check_hook(), Some("check_log_destination"));

    let bytea_output = find_option("bytea_output").unwrap();
    assert_eq!(bytea_output.value_kind(), GucValueKind::Enum);
    assert_eq!(
        bytea_output.default_value(),
        GucDefaultValue::Enum(consts::BYTEA_OUTPUT_HEX)
    );
    let opts = bytea_output.options().unwrap();
    assert_eq!(opts[0].name, "escape");
    assert_eq!(opts[0].val, consts::BYTEA_OUTPUT_ESCAPE);

    // Compiled-in string defaults src-idiomatic had stubbed to None.
    assert_eq!(
        find_option("default_table_access_method")
            .unwrap()
            .default_value(),
        GucDefaultValue::String(Some("heap"))
    );
    assert_eq!(
        find_option("server_version").unwrap().default_value(),
        GucDefaultValue::String(Some("18.3"))
    );
}

#[test]
fn extern_option_sets_carry_array_names() {
    let wal_level = find_option("wal_level").unwrap();
    assert_eq!(wal_level.options(), None);
    match wal_level {
        GucSetting::Enum(s) => assert_eq!(s.option_set, Some("wal_level_options")),
        _ => panic!("wal_level should be an enum GUC"),
    }
    let inline = find_option("backslash_quote").unwrap();
    assert!(inline.options().is_some());
}

#[test]
fn message_level_options_match_elog_values() {
    let level = find_option("log_min_messages").unwrap();
    let opts = level.options().unwrap();
    let warning = opts.iter().find(|o| o.name == "warning").unwrap();
    assert_eq!(warning.val, types_error::WARNING.0);
    assert_eq!(
        level.default_value(),
        GucDefaultValue::Enum(types_error::WARNING.0)
    );
}

#[derive(Default)]
struct RecordingProvider {
    checked: std::cell::RefCell<Vec<(String, i32)>>,
}

impl GucHookProvider for RecordingProvider {
    fn check_int(&self, hook: &str, newval: i32, _source: GucSource) -> PgResult<bool> {
        self.checked.borrow_mut().push((hook.to_owned(), newval));
        Ok(false)
    }
}

#[test]
fn check_setting_delegates_to_hook_provider() {
    let provider = RecordingProvider::default();
    let setting = find_option("max_stack_depth").unwrap();

    assert!(!check_setting(&provider, setting, GucDefaultValue::Int(2048), PGC_S_TEST).unwrap());
    assert_eq!(
        provider.checked.borrow().as_slice(),
        &[("check_max_stack_depth".to_owned(), 2048)]
    );
}

#[test]
fn mismatched_value_type_is_an_error() {
    let provider = NoopGucHookProvider;
    let setting = find_option("max_stack_depth").unwrap();
    assert!(check_setting(&provider, setting, GucDefaultValue::Bool(true), PGC_S_TEST).is_err());
    assert!(assign_setting(&provider, setting, GucDefaultValue::Bool(true)).is_err());
}

#[test]
fn name_tables_round_trip_indices() {
    assert_eq!(GucContext_Names[PGC_INTERNAL as usize], "internal");
    assert_eq!(GucContext_Names[PGC_USERSET as usize], "user");
    assert_eq!(GucSource_Names[PGC_S_DEFAULT as usize], "default");
    assert_eq!(GucSource_Names[PGC_S_FILE as usize], "configuration file");
    assert_eq!(config_group_names[FILE_LOCATIONS as usize], "File Locations");
    assert_eq!(config_type_names[PGC_BOOL as usize], "bool");
    assert_eq!(config_type_names[PGC_ENUM as usize], "enum");
}
