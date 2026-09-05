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
    // testmode M1 (pgrust-only, docs/design/test-views.md D1): the
    //   ephemeral-database janitor — String +1 pgrust.ephemeral_db_prefix
    //   (-> 78), Int +1 pgrust.ephemeral_db_grace (-> 167) = 455.
    // testmode M3 (pgrust-only, docs/design/test-views.md D2): the
    //   mint-on-connect security posture — String +1
    //   pgrust.ephemeral_db_mint_roles (-> 79), Int +1
    //   pgrust.ephemeral_db_max_per_role (-> 168) = 457. (The former
    //   pgrust.ephemeral_db_default_template was DELETED with the bare
    //   mint form, ruling 2026-08-05.)
    // testmode D3 warm pool (pgrust-only, test-views.md warm-pool addendum):
    //   Int +1 pgrust.ephemeral_db_pool_size (-> 169) = 458.
    // testmode mint-strategy addendum (pgrust-only): Int +1
    //   pgrust.ephemeral_db_wal_log_threshold (-> 170) = 459.
    // testmode prewarm addendum (pgrust-only): Bool +1
    //   pgrust.ephemeral_db_prewarm (-> 136) = 460.
    // dl-verstring (pgrust-only, version-string ruling 2026-08-06): Enum +1
    //   pgrust.version_string_style (-> 48) = 461.
    // covdiff E1-A (pgrust-only): Bool +1 pgrust.explain_runtime_verdicts
    //   (-> 137) = 462 — EXPLAIN display gate for runtime refusal verdicts,
    //   default off (C-parity default output).
    // connection-scaling D1+D6 (pgrust-only, docs/design/
    //   connection-scaling.md): Int +3 max_active_queries,
    //   connection_queue_size, connection_queue_timeout (-> 173) = 465.
    // connection-scaling D3.4 (pgrust-only, same doc): Int +1
    //   idle_passivate_timeout (-> 174) = 466.
    // connection-scaling GUC-ification pass (pgrust-only, same doc):
    //   Int +2 catcache_size_limit, relcache_size_limit (-> 176),
    //   Bool +1 shared_catalog_cache (-> 138) = 469.
    // connection-scaling wave 4 (floor work, same doc):
    //   Bool +1 idle_passivate_stack (-> 139) = 470.
    // heap-on-sqe v1 (pgrust-only, heap-face.md): Bool +1
    //   pgrust.sqe_heap (-> 140) = 471 — the heap-face safety switch,
    //   default off.
    // sqe→main merge (pgrust-only, the sqe campaign lands): Int +1
    //   pgrust.sqe_threads (-> 177) = 472 — the engine worker width
    //   (0 = auto), formerly the PGRUST_SQE_THREADS env spelling.
    // upstream 2a29b607dbbb (18.6, CVE-2026-6471): String +1
    //   output_plugin_libraries (-> 80) = 473 — the C 18.6 GUC.
    // audit-remediation b090 (contrib/pg_prewarm/autoprewarm.c:128 _PG_init
    //   DefineCustomIntVariable): Int +1 pg_prewarm.autoprewarm_interval
    //   (-> 178) = 474 — the C 18.6 custom GUC, statically defined like
    //   auto_explain.*.
    // audit-remediation b150 (contrib/pg_trgm/trgm_op.c:145 _PG_init, three
    //   DefineCustomRealVariable): Real +3 pg_trgm.similarity_threshold /
    //   word_similarity_threshold / strict_word_similarity_threshold
    //   (-> 31) = 477 — the C 18.6 custom GUCs, statically defined like
    //   auto_explain.*.
    // audit-remediation b033 (contrib/pgcrypto/pgcrypto.c:70 _PG_init): Enum +1
    //   pgcrypto.builtin_crypto_enabled (-> 49) = 478 — the C 18.6 custom GUC,
    //   statically defined like pg_stat_statements.* / auto_explain.*.
    // audit b013 (pl_handler.c _PG_init:158-203, statically defined like
    //   auto_explain's): Bool +2 plpgsql.print_strict_params /
    //   plpgsql.check_asserts (-> 142), String +2 plpgsql.extra_warnings /
    //   plpgsql.extra_errors (-> 82), Enum +1 plpgsql.variable_conflict
    //   (-> 50) = 483.
    assert_eq!(ConfigureNamesBool.len(), 142);
    assert_eq!(ConfigureNamesInt.len(), 178);
    assert_eq!(ConfigureNamesReal.len(), 31);
    assert_eq!(ConfigureNamesString.len(), 82);
    assert_eq!(ConfigureNamesEnum.len(), 50);
    assert_eq!(all_settings().count(), 483);
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
        GucDefaultValue::String(Some("18.6"))
    );
}

#[test]
fn version_string_style_defaults_postgres_first() {
    // dl-verstring ruling 2026-08-06: the DEFAULT must be postgres_first so
    // first-number parsers of version() read the PostgreSQL compatibility
    // version. Flipping this default is a client-visible ecosystem break.
    let style = find("pgrust.version_string_style");
    assert_eq!(style.value_kind(), GucValueKind::Enum);
    assert_eq!(
        style.default_value(),
        GucDefaultValue::Enum(consts::VERSION_STRING_POSTGRES_FIRST)
    );
    assert_eq!(style.group(), CUSTOM_OPTIONS);
    let opts = style.options().unwrap().entries();
    assert_eq!(opts.len(), 2);
    assert_eq!(opts[0].name, "postgres_first");
    assert_eq!(opts[0].val, consts::VERSION_STRING_POSTGRES_FIRST);
    assert_eq!(opts[1].name, "pgrust_first");
    assert_eq!(opts[1].val, consts::VERSION_STRING_PGRUST_FIRST);
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
fn default_toast_compression_is_installed_by_the_product_boot() {
    // Gate-blindness regression (main-red adjudication 2026-08-04):
    // heaptoast's toast_compress_datum falls back to this slot for an
    // invalid attcompression (heaptoast internals.rs — C
    // toast_internals.c:59), so it is read on EVERY default-compression
    // TOAST compress of a real server. Drift commit 0eb4c6c1fd1 landed
    // that reader with only heaptoast's own test-local install; every
    // TOAST insert on mains 882239cc94/dc5c2ba5ed died with the
    // slots.rs used-before-install panic (regress leg ERRORs + the
    // cbstore-lane-e2e backend panics). init_seams() — the product boot
    // path — must install it; reverting the install_if_absent in
    // install_guc_tables_owned_vars() must turn this test red
    // (mutation-witnessed 2026-08-05: revert reproduced the exact CI cluster
    // panic signature locally).
    crate::init_seams();
    assert!(
        vars::default_toast_compression.installed(),
        "product boot (init_seams) must install default_toast_compression — \
         a test-only install leaves every real-server TOAST compress panicking"
    );
    // Boot value is pglz (C toast_compression.c boot default) and the
    // accessors must round-trip through the session backing cell.
    assert_eq!(vars::default_toast_compression.read(), crate::consts::TOAST_PGLZ_COMPRESSION);
    vars::default_toast_compression.write(crate::consts::TOAST_LZ4_COMPRESSION);
    assert_eq!(vars::default_toast_compression.read(), crate::consts::TOAST_LZ4_COMPRESSION);
    vars::default_toast_compression.write(crate::consts::TOAST_PGLZ_COMPRESSION);
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
fn file_copy_method_options_match_platform_clone_support() {
    let opts = find("file_copy_method").options().unwrap().entries();
    let copy = opts.iter().find(|o| o.name == "copy").unwrap();
    assert_eq!(copy.val, consts::FILE_COPY_METHOD_COPY);
    assert!(!copy.hidden);
    #[cfg(not(pgrust_sim))]
    {
        let clone = opts.iter().find(|o| o.name == "clone").unwrap();
        assert_eq!(clone.val, consts::FILE_COPY_METHOD_CLONE);
        assert!(!clone.hidden);
    }
    #[cfg(pgrust_sim)]
    assert!(!opts.iter().any(|o| o.name == "clone"));
    assert_eq!(
        find("file_copy_method").default_value(),
        GucDefaultValue::Enum(consts::FILE_COPY_METHOD_COPY)
    );
}

#[test]
fn lz4_build_config_is_reflected_in_option_sets() {
    // TOAST lz4 is implemented (lz4_flex); WAL lz4/zstd compression is not.
    let opts = find("default_toast_compression").options().unwrap().entries();
    assert!(opts.iter().any(|o| o.name == "lz4" && o.val == consts::TOAST_LZ4_COMPRESSION));
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

// contrib/pg_prewarm/autoprewarm.c:128-138 (_PG_init): DefineCustomIntVariable
// "pg_prewarm.autoprewarm_interval" — default 300, range 0..INT_MAX/1000,
// PGC_SIGHUP, GUC_UNIT_S, no hooks. Defined before the
// process_shared_preload_libraries_in_progress check, so every backend that
// loads the library has it (SHOW -> "5min"; SET -> 55P02 "cannot be changed
// now"). Regression for audit-18.6 b090
// (a186-candidate-fp-contrib-pg_prewarm-autoprewarm-0ff933858b20be8ef07c-1):
// pgrust knew no such GUC (42704 on SHOW, a bare placeholder on SET).
#[test]
fn pg_prewarm_autoprewarm_interval_matches_autoprewarm_c() {
    let GucSetting::Int(interval) = find("pg_prewarm.autoprewarm_interval") else {
        panic!("pg_prewarm.autoprewarm_interval should be an int GUC");
    };
    assert_eq!(interval.context, PGC_SIGHUP);
    assert_eq!(interval.group, CUSTOM_OPTIONS);
    assert_eq!(interval.flags, GUC_UNIT_S);
    assert_eq!(interval.boot_val, GucDefaultValue::Int(300));
    assert_eq!(interval.min, 0);
    assert_eq!(interval.max, i32::MAX / 1000);
    assert_eq!(
        interval.short_desc,
        Some("Sets the interval between dumps of shared buffers")
    );
    assert_eq!(
        interval.long_desc,
        Some("If set to zero, time-based dumping is disabled.")
    );
    assert!(interval.check_hook.is_none());
    assert!(interval.assign_hook.is_none());
    assert!(interval.show_hook.is_none());
    assert_eq!(interval.variable.c_symbol(), "autoprewarm_interval");
}

// contrib/pg_trgm/trgm_op.c:145-190 (_PG_init): three DefineCustomRealVariable
// GUCs — pg_trgm.similarity_threshold (0.3f), word_similarity_threshold
// (0.6f), strict_word_similarity_threshold (0.5f); each 0.0 .. 1.0,
// PGC_USERSET, flags 0, no hooks, the C bootValue being the float literal
// widened to double. Regression for audit-18.6 b150
// (a186-candidate-fp-contrib-pg_trgm-trgm_op-d92afd7477a9c4dfd11d-1):
// pgrust rode the placeholder store, so SET accepted any value.
#[test]
fn pg_trgm_thresholds_match_trgm_op_c() {
    for (name, boot, desc, symbol) in [
        (
            "pg_trgm.similarity_threshold",
            0.3f32 as f64,
            "Sets the threshold used by the % operator.",
            "similarity_threshold",
        ),
        (
            "pg_trgm.word_similarity_threshold",
            0.6f32 as f64,
            "Sets the threshold used by the <% operator.",
            "word_similarity_threshold",
        ),
        (
            "pg_trgm.strict_word_similarity_threshold",
            0.5f32 as f64,
            "Sets the threshold used by the <<% operator.",
            "strict_word_similarity_threshold",
        ),
    ] {
        let GucSetting::Real(t) = find(name) else {
            panic!("{name} should be a real GUC");
        };
        assert_eq!(t.context, PGC_USERSET, "{name}");
        assert_eq!(t.group, CUSTOM_OPTIONS, "{name}");
        assert_eq!(t.flags, 0, "{name}");
        assert_eq!(t.boot_val, GucDefaultValue::Real(boot), "{name}");
        assert_eq!(t.min, 0.0, "{name}");
        assert_eq!(t.max, 1.0, "{name}");
        assert_eq!(t.short_desc, Some(desc), "{name}");
        assert_eq!(t.long_desc, Some("Valid range is 0.0 .. 1.0."), "{name}");
        assert!(t.check_hook.is_none(), "{name}");
        assert!(t.assign_hook.is_none(), "{name}");
        assert!(t.show_hook.is_none(), "{name}");
        assert_eq!(t.variable.c_symbol(), symbol, "{name}");
    }
}
