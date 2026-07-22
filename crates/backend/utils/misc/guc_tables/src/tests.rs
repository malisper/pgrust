use super::*;
use types_guc::*;

fn find(name: &str) -> GucSetting {
    all_settings()
        .find(|setting| setting.name() == name)
        .unwrap_or_else(|| panic!("no built-in GUC named {name}"))
}

#[test]
fn table_counts_match_compiled_backend_shape() {
    // 41 + 1: regex_engine (re2 product dispatch, hidden).
    // Bool: +3 over the compiled C backend for pgrust.lane_executor
    // (pgrust-only, the lane-v2 master gate),
    // pgrust.regex_pattern_program (pgrust-only, the anchored
    // pattern-program regex fast tier, hidden), and
    // pgrust.condition_cache (pgrust-only, the pgrcolumnar condition cache
    // gate; +1 Int for its size), +3 pg_stat_statements.*
    // (statically defined custom GUCs; no DefineCustomXxxVariable here).
    // Int/Enum: +1 each for pg_stat_statements.max / .track;
    // Int +1 pgrust.condition_cache_size.
    // pgvector hnsw.*: Int +2 / Real +1 / Enum +1 (contrib GUCs defined
    // statically here).
    // M5-0 (pgrust-only, docs/design/m5-planner.md §2.2): Enum +1
    // pgrust.parallel_engine, Int +1 pgrust.runtime_dop.
    // auto_explain.* (statically defined custom GUCs, like pg_stat_statements):
    // Bool +8, Int +2, Real +1, Enum +2.
    // H7 (pgrust-only): String +1 pgrust.resource_counters (PGC_INTERNAL
    // computed channel for the simharness F8 resource-baseline hook).
    // env-to-guc train (pgrust-only; INTENTIONAL C byte-identity divergence of
    // pg_settings / SHOW ALL — these are new pgrust.* rows for the public
    // release, migrating former PGRUST_* env vars to registered GUCs):
    //   Bool +2: pgrust.runtime (the runtime-pool master switch, was
    //     PGRUST_RUNTIME) and pgrust.mem_autotune (boot memory auto-tune gate,
    //     was PGRUST_MEM_AUTOTUNE) -> 129 + 2 = 131.
    //   Int +8: the deferred per-arm pool-GUC recipe
    //     (docs/design/jit-parallel-defaults.md §3): pgrust.runtime_scan_pool /
    //     runtime_agg_pool / runtime_distinct_pool / runtime_hashjoin_pool /
    //     runtime_sort_pool / runtime_bitmap_pool / lane_parallel_pool /
    //     gather_fair_stride -> 154 + 8 = 162.
    //   Total 435 + 10 = 445.
    //   GL-M41-3 flip: + pgrust.runtime_vacuum_pool (Bool 131 -> 132) = 446.
    // GL-STRDEFECTS-1: + pgrust.regex_re2_linked (pgrust-only preset, the
    //   RE2-linkage runtime witness; Bool 132 -> 133) = 447.
    // GL-MEMWATCH-1 (pgrust-only, composed at t43): the memory-watchdog
    //   family — Bool +2 (pgrust.memory_watchdog, pgrust.memory_watchdog_dump
    //   -> 135), Int +4 (pgrust.memory_watchdog_interval / _threshold /
    //   _limit, plus the hidden developer hog pgrust.memory_watchdog_test_hog
    //   -> 166) = 453.
    assert_eq!(ConfigureNamesBool.len(), 135);
    assert_eq!(ConfigureNamesInt.len(), 166);
    assert_eq!(ConfigureNamesReal.len(), 28);
    assert_eq!(ConfigureNamesString.len(), 77);
    assert_eq!(ConfigureNamesEnum.len(), 47);
    assert_eq!(all_settings().count(), 453);
    assert_eq!(GucContext_Names.len(), PGC_USERSET as usize + 1);
    assert_eq!(GucSource_Names.len(), PGC_S_SESSION as usize + 1);
    assert_eq!(config_group_names.len(), DEVELOPER_OPTIONS as usize + 1);
    assert_eq!(config_type_names.len(), PGC_ENUM as usize + 1);
}

#[test]
fn common_options_are_present_with_postgres_defaults() {
    let seqscan = find("enable_seqscan");
    assert_eq!(seqscan.value_kind(), GucValueKind::Bool);
    assert_eq!(seqscan.default_value(), GucDefaultValue::Bool(true));
    assert_eq!(seqscan.group(), QUERY_TUNING_METHOD);
    assert_eq!(seqscan.variable_c_symbol(), "enable_seqscan");

    let GucSetting::Int(stack) = find("max_stack_depth") else {
        panic!("max_stack_depth should be an int GUC");
    };
    assert_eq!(stack.boot_val, GucDefaultValue::Int(100));
    assert_eq!(
        stack.check_hook.unwrap().c_symbol(),
        "check_max_stack_depth"
    );
    assert_eq!(
        stack.assign_hook.unwrap().c_symbol(),
        "assign_max_stack_depth"
    );
    assert!(std::ptr::eq(
        stack.check_hook.unwrap(),
        &hooks::check_max_stack_depth
    ));

    let GucSetting::String(log_destination) = find("log_destination") else {
        panic!("log_destination should be a string GUC");
    };
    assert_eq!(
        log_destination.boot_val,
        GucDefaultValue::String(Some("stderr"))
    );
    assert_eq!(
        log_destination.check_hook.unwrap().c_symbol(),
        "check_log_destination"
    );

    let bytea_output = find("bytea_output");
    assert_eq!(bytea_output.value_kind(), GucValueKind::Enum);
    assert_eq!(
        bytea_output.default_value(),
        GucDefaultValue::Enum(consts::BYTEA_OUTPUT_HEX)
    );
    let opts = bytea_output.options().unwrap().entries();
    assert_eq!(opts[0].name, "escape");
    assert_eq!(opts[0].val, consts::BYTEA_OUTPUT_ESCAPE);

    assert_eq!(
        find("default_table_access_method").default_value(),
        GucDefaultValue::String(Some("heap"))
    );
    assert_eq!(
        find("server_version").default_value(),
        GucDefaultValue::String(Some("18.3"))
    );
}

#[test]
fn extern_option_sets_are_typed_slots() {
    let GucSetting::Enum(wal_level) = find("wal_level") else {
        panic!("wal_level should be an enum GUC");
    };
    match wal_level.options {
        GucEnumOptions::External(slot) => {
            assert_eq!(slot.c_symbol(), "wal_level_options");
            assert!(std::ptr::eq(slot, &option_sets::wal_level_options));
        }
        GucEnumOptions::Inline(_) => panic!("wal_level_options is owned by another unit"),
    }
    assert!(matches!(
        find("backslash_quote").options().unwrap(),
        GucEnumOptions::Inline(_)
    ));
}

#[test]
fn message_level_options_match_elog_values() {
    let level = find("log_min_messages");
    let opts = level.options().unwrap().entries();
    let warning = opts.iter().find(|o| o.name == "warning").unwrap();
    assert_eq!(warning.val, types_error::WARNING.0);
    assert_eq!(
        level.default_value(),
        GucDefaultValue::Enum(types_error::WARNING.0)
    );
}

#[test]
fn installed_hook_dispatches_through_the_table_entry() {
    use std::sync::atomic::{AtomicI32, Ordering};

    static SEEN: AtomicI32 = AtomicI32::new(0);

    fn recording_check(
        newval: &mut i32,
        extra: &mut Option<GucHookExtra>,
        _source: GucSource,
    ) -> types_error::PgResult<bool> {
        SEEN.store(*newval, Ordering::SeqCst);
        *extra = Some(Box::new(*newval * 2));
        *newval += 1;
        Ok(true)
    }

    hooks::check_max_stack_depth.install(recording_check);

    let GucSetting::Int(stack) = find("max_stack_depth") else {
        panic!("max_stack_depth should be an int GUC");
    };
    let mut newval = 2048;
    let mut extra = None;
    let ok = stack.check_hook.unwrap().get()(&mut newval, &mut extra, PGC_S_TEST).unwrap();
    assert!(ok);
    assert_eq!(SEEN.load(Ordering::SeqCst), 2048);
    assert_eq!(newval, 2049);
    assert_eq!(*extra.unwrap().downcast::<i32>().unwrap(), 4096);
}

#[test]
fn installed_variable_accessors_read_and_write_the_owner_storage() {
    use std::cell::Cell;

    thread_local! {
        static STORAGE: Cell<bool> = const { Cell::new(true) };
    }

    vars::enable_seqscan.install(GucVarAccessors {
        get: || STORAGE.with(Cell::get),
        set: |v| STORAGE.with(|c| c.set(v)),
    });

    let GucSetting::Bool(seqscan) = find("enable_seqscan") else {
        panic!("enable_seqscan should be a bool GUC");
    };
    assert!(seqscan.variable.read());
    seqscan.variable.write(false);
    assert!(!seqscan.variable.read());
}

#[test]
#[should_panic(expected = "check_bonjour used before its owning unit installed it")]
fn uninstalled_hook_slot_panics_loudly() {
    let _ = hooks::check_bonjour.get();
}

#[test]
#[should_panic(expected = "enable_indexscan used before its owning unit installed it")]
fn uninstalled_variable_slot_panics_loudly() {
    let _ = vars::enable_indexscan.read();
}

#[test]
#[should_panic(expected = "installed twice")]
fn duplicate_install_panics() {
    fn show() -> String {
        String::new()
    }
    hooks::show_archive_command.install(show);
    hooks::show_archive_command.install(show);
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

#[test]
fn setting_names_are_unique() {
    let mut names: Vec<&str> = all_settings().map(|s| s.name()).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(before, names.len());
}

#[test]
fn m5_probe_requires_a_live_pool() {
    // t34-config review, defect 3: with every GUC at its default
    // (parallel_engine=runtime, pgrust.runtime=on, lane_executor=on) but NO
    // pool spawned — exactly a unit-test process — the M5-3 suppression
    // probe must stay inert: a suppressed Gather with no pool to pick the
    // plan up is silent serial execution.
    assert!(crate::runtime_pool::parallel_engine_is_runtime());
    assert!(crate::backing::pgrust_runtime());
    assert!(!crate::runtime_pool::runtime_pool_live());
    assert!(!crate::parallel_engine::m5_gather_suppression_active());
    // Once the postmaster's rtpool start publishes liveness, the probe arms
    // (process-lifetime flag: production never unsets it, so no restore).
    crate::runtime_pool::set_runtime_pool_live();
    assert!(crate::parallel_engine::m5_gather_suppression_active());
}

#[test]
fn lz4_build_config_is_reflected_in_option_sets() {
    let opts = find("default_toast_compression").options().unwrap().entries();
    assert!(!opts.iter().any(|o| o.name == "lz4"));
    let wal = find("wal_compression").options().unwrap().entries();
    assert!(!wal.iter().any(|o| o.name == "lz4" || o.name == "zstd"));
    let GucSetting::Enum(style) = find("IntervalStyle") else {
        panic!("IntervalStyle should be an enum GUC");
    };
    assert!(style.assign_hook.is_none());
    let GucSetting::Int(stack) = find("max_stack_depth") else {
        panic!("max_stack_depth should be an int GUC");
    };
    assert_eq!(stack.boot_val, GucDefaultValue::Int(100));
}
