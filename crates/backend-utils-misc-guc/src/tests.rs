//! Behavioural tests for the GUC core, ported from the original idiomatic
//! tree and adapted to this repo's restructured `guc_tables` (hook/var slots).
//!
//! The store is one process-global `static mut` (exactly as the C file-static
//! `guc_hashtab` is); cargo runs `#[test]`s on parallel threads, which would
//! race it, so the tests serialize on [`GUC_TEST_LOCK`] and are sequenced by a
//! single driver per concern. The outward seams and the few hook/var slots the
//! exercised GUCs reference are installed with trivial single-backend defaults.

use std::sync::Mutex;

use types_core::BOOTSTRAP_SUPERUSERID;
use types_error::{ErrorLevel, ERROR};
use types_guc::{PGC_POSTMASTER, PGC_SIGHUP, PGC_S_OVERRIDE, PGC_USERSET};

use crate::live::{
    get_bool, get_int, get_real, get_enum, initialize_guc_options, is_initialized, set_config_option_global,
    with_store,
};
use crate::process_config::ConfigItem;

static GUC_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Install the outward seams + the hook/var slots the exercised GUCs reach,
/// once, with trivial single-backend defaults. `install` is `set`/OnceLock-once,
/// so this is guarded to run a single time across the serialized tests.
fn install_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Outward seams (owned by other crates) the SET / apply / permission
        // paths reach.
        backend_utils_init_small_seams::is_under_postmaster::set(|| false);
        backend_access_transam_xact_seams::is_in_parallel_mode::set(|| false);
        backend_utils_init_miscinit_seams::in_local_user_id_change::set(|| false);
        backend_utils_init_miscinit_seams::in_security_restricted_operation::set(|| false);
        backend_catalog_aclchk_seams::pg_parameter_aclcheck::set(|_name, _role, _mode| {
            Ok(types_acl::AclResult::AclcheckOk)
        });
        // GUCArray* permission checks: single-backend superuser, fixed user id.
        backend_utils_misc_superuser_seams::superuser::set(|| Ok(true));
        backend_utils_init_miscinit_seams::get_user_id::set(|| BOOTSTRAP_SUPERUSERID);
        backend_utils_adt_scalar_seams::parse_bool::set(default_parse_bool);
        // `truncate_identifier` (scansup) for GUC_IS_NAME values: return the
        // bytes unchanged in the supplied context (single-backend no-clip).
        backend_parser_scansup_seams::truncate_identifier::set(|mcx, ident, _warn| {
            mcx::slice_in(mcx, ident)
        });
        // The pq sink for ReportGUCOption (no-op accept).
        backend_libpq_pqcomm_seams::pq_putmessage::set(|_msgtype, _body| Ok(0));

        // Hook slots the report test's `application_name` SET reaches: install
        // trivial accept-everything bodies (the real hooks live in their
        // owners). check accepts; assign is a no-op.
        use backend_utils_misc_guc_tables::hooks;
        hooks::check_application_name.install(|_v, _e, _s| Ok(true));
        hooks::assign_application_name.install(|_v, _e| {});
        // `client_encoding` hooks: the SIGHUP apply path re-sets it with the
        // dynamic default, reaching its check/assign slots.
        hooks::check_client_encoding.install(|_v, _e, _s| Ok(true));
        hooks::assign_client_encoding.install(|_v, _e| {});
    });
}

/// A minimal `parse_bool` matching the C `on`/`off`/`true`/`false`/`1`/`0`
/// surface the SET path needs (the real owner is adt-scalar; the test installs
/// this so a bool SET resolves without that crate's init_seams).
fn default_parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

#[test]
fn unified_store_boot_read_set_round_trip() {
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();

    initialize_guc_options();
    assert!(is_initialized());

    // Boot defaults read THROUGH the unified store.
    assert_eq!(get_int("bgwriter_lru_maxpages"), Some(100));
    assert_eq!(get_int("bgwriter_delay"), Some(200));
    assert_eq!(get_int("max_connections"), Some(100));
    assert_eq!(get_bool("fsync"), Some(true));
    assert_eq!(get_bool("enable_seqscan"), Some(true));
    assert_eq!(get_real("bgwriter_lru_multiplier"), Some(2.0));
    assert_eq!(get_enum("wal_level"), Some(1)); // WAL_LEVEL_REPLICA

    // The resolved guc_tables seed real values for the GUCs the prior idiomatic
    // tree left as unresolved IntExpr placeholders.
    assert!(get_int("checkpoint_flush_after").is_some());
    assert!(get_int("bgwriter_flush_after").is_some());

    // Wrong-type / unknown access returns None rather than panicking.
    assert_eq!(get_bool("bgwriter_lru_maxpages"), None);
    assert_eq!(get_int("fsync"), None);
    assert_eq!(get_int("no_such_guc_xyz"), None);

    // SET round-trip: a write through the unified store changes the read-through
    // accessor. `bgwriter_delay` has literal bounds (10..10000).
    let rc = set_config_option_global(
        "bgwriter_delay",
        Some("321"),
        PGC_POSTMASTER,
        PGC_S_OVERRIDE,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("SET bgwriter_delay should apply");
    assert_eq!(rc, 1);
    assert_eq!(get_int("bgwriter_delay"), Some(321));

    // The change marked the variable for reporting.
    let needs_report = with_store(|reg| {
        reg.find_option("bgwriter_delay")
            .map(|v| v.gen().status & crate::model::GUC_NEEDS_REPORT != 0)
            .unwrap_or(false)
    })
    .unwrap_or(false);
    assert!(needs_report, "applied SET should mark GUC_NEEDS_REPORT");

    // Out-of-range SET is rejected at ERROR.
    let err = set_config_option_global(
        "bgwriter_delay",
        Some("999999"),
        PGC_POSTMASTER,
        PGC_S_OVERRIDE,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    );
    assert!(err.is_err(), "out-of-range SET should error at ERROR elevel");
}

#[test]
fn guc_stack_save_rollback_and_commit() {
    // The transactional GUC stack (guc_stack.c): a GUC_ACTION_SAVE set inside a
    // nest level is rolled back by AtEOXact_GUC(isCommit=false) and kept by
    // AtEOXact_GUC(isCommit=true) at the same level (GUC_SAVE always restores
    // prior on commit too — it's the function-scoped form). work_mem is a
    // PGC_USERSET int with literal bounds, so a PGC_S_SESSION set applies.
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    let boot = get_int("work_mem").expect("work_mem present");

    // Open a nest level (NewGUCNestLevel -> 1+).
    let save_level = crate::NewGUCNestLevel();
    assert!(save_level >= 1);

    // SAVE-set work_mem (the proconfig / SET LOCAL-style transient form).
    let rc = set_config_option_global(
        "work_mem",
        Some("65536"),
        PGC_USERSET,
        types_guc::PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SAVE,
        true,
        ERROR,
        false,
    )
    .expect("SAVE set work_mem");
    assert_eq!(rc, 1);
    assert_eq!(get_int("work_mem"), Some(65536));

    // Abort the nest level: the prior (boot) value is restored.
    crate::at_eoxact_guc(false, save_level);
    assert_eq!(get_int("work_mem"), Some(boot), "abort restores prior value");

    // Reopen + SAVE-set + commit: GUC_SAVE restores prior on commit as well.
    let save_level = crate::NewGUCNestLevel();
    set_config_option_global(
        "work_mem",
        Some("65536"),
        PGC_USERSET,
        types_guc::PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SAVE,
        true,
        ERROR,
        false,
    )
    .expect("SAVE set work_mem");
    assert_eq!(get_int("work_mem"), Some(65536));
    crate::at_eoxact_guc(true, save_level);
    assert_eq!(
        get_int("work_mem"),
        Some(boot),
        "commit of a GUC_SAVE level restores prior"
    );
}

#[test]
fn apply_config_variables_round_trip() {
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    // Boot default of bgwriter_lru_maxpages is 100 (PGC_SIGHUP context, so a
    // PGC_S_FILE set applies).
    assert_eq!(get_int("bgwriter_lru_maxpages"), Some(100));

    let mut items = vec![ConfigItem {
        name: "bgwriter_lru_maxpages".into(),
        value: "250".into(),
        filename: "postgresql.conf".into(),
        sourceline: 7,
        ignore: false,
        applied: false,
        errmsg: None,
    }];
    let mut conf_file = "postgresql.conf".to_string();
    let ok = crate::apply_config_variables(
        &mut items,
        PGC_SIGHUP,
        true,
        ErrorLevel(0),
        &mut conf_file,
        0,
    )
    .expect("apply should not throw");

    assert!(ok, "no error expected");
    assert!(items[0].applied, "the setting should be marked applied");
    assert_eq!(get_int("bgwriter_lru_maxpages"), Some(250));

    // Reload with the setting REMOVED: reverts to the boot default.
    let mut empty: Vec<ConfigItem> = Vec::new();
    let ok = crate::apply_config_variables(
        &mut empty,
        PGC_SIGHUP,
        true,
        ErrorLevel(0),
        &mut conf_file,
        0,
    )
    .expect("reload should not throw");
    assert!(ok);
    assert_eq!(get_int("bgwriter_lru_maxpages"), Some(100));
}

#[test]
fn report_changed_guc_options_dedup() {
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    // application_name is GUC_REPORT with a literal "" boot value, and its
    // check/assign hook slots are installed above.
    let rc = set_config_option_global(
        "application_name",
        Some("psql"),
        PGC_POSTMASTER,
        PGC_S_OVERRIDE,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("SET application_name should apply");
    assert_eq!(rc, 1);

    let changed = crate::report_changed_guc_options();
    assert_eq!(changed, 1, "exactly one changed reportable GUC");

    // A second report sends nothing (GUC_NEEDS_REPORT was cleared).
    let again = crate::report_changed_guc_options();
    assert_eq!(again, 0);
}

#[test]
fn report_guc_option_last_reported_dedup() {
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    // C ReportGUCOption (guc.c:2634) suppresses the ParameterStatus frame when
    // the rendered value equals the variable's last_reported, and records
    // last_reported after sending.
    let set_app = |val: &str| {
        set_config_option_global(
            "application_name",
            Some(val),
            PGC_POSTMASTER,
            PGC_S_OVERRIDE,
            BOOTSTRAP_SUPERUSERID,
            crate::GUC_ACTION_SET,
            true,
            ERROR,
            false,
        )
        .expect("SET application_name should apply")
    };

    // First change to a genuinely new value: reports (1), records "psql".
    assert_eq!(set_app("psql"), 1);
    assert_eq!(crate::report_changed_guc_options(), 1);

    // Re-SET to the SAME value: marks GUC_NEEDS_REPORT again, but the rendered
    // value equals last_reported, so ReportGUCOption's dedup suppresses the
    // frame -> 0 transmitted. The NEEDS_REPORT bit is still drained.
    assert_eq!(set_app("psql"), 1);
    assert_eq!(
        crate::report_changed_guc_options(),
        0,
        "value equal to last_reported must be suppressed (guc.c:2638)"
    );
    assert_eq!(crate::report_changed_guc_options(), 0);

    // A genuinely new value reports again and updates last_reported.
    assert_eq!(set_app("pgbench"), 1);
    assert_eq!(crate::report_changed_guc_options(), 1);
}

#[test]
fn enum_seqscan_set_reset() {
    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    assert_eq!(get_bool("enable_seqscan"), Some(true));

    let rc = set_config_option_global(
        "enable_seqscan",
        Some("off"),
        PGC_USERSET,
        types_guc::PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("session SET should apply");
    assert_eq!(rc, 1);
    assert_eq!(get_bool("enable_seqscan"), Some(false));

    // RESET (value = NULL at a non-default source) returns reset_val.
    let rc = set_config_option_global(
        "enable_seqscan",
        None,
        PGC_USERSET,
        types_guc::PGC_S_SESSION,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("RESET should apply");
    assert_eq!(rc, 1);
    assert_eq!(get_bool("enable_seqscan"), Some(true), "RESET returns reset_val");
}

/// GUCArrayAdd / GUCArrayDelete / GUCArrayReset over the `Vec<String>` model
/// (guc.c:6494/6572/6642). The store must be initialized so the validation
/// path's `set_config_option(PGC_S_TEST)` resolves a real (USERSET) variable.
#[test]
fn guc_array_add_delete_reset_round_trip() {
    use crate::guc_array::{GUCArrayAdd, GUCArrayDelete, GUCArrayReset};

    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    // Use `enable_seqscan` / `enable_indexscan` (PGC_USERSET bool) — their
    // var/hook slots are wired by `initialize_guc_options`, so the validation
    // path's `set_config_option(PGC_S_TEST)` resolves a real variable.

    // Add into a NULL array -> one-element array.
    let a = GUCArrayAdd(None, "enable_seqscan", "off").expect("add to null array");
    assert_eq!(a, vec!["enable_seqscan=off".to_string()]);

    // Add a second distinct setting -> appended after the end.
    let a = GUCArrayAdd(Some(a), "enable_indexscan", "off").expect("add second");
    assert_eq!(
        a,
        vec![
            "enable_seqscan=off".to_string(),
            "enable_indexscan=off".to_string()
        ]
    );

    // Re-add an existing name -> replace in place (not appended).
    let a = GUCArrayAdd(Some(a), "enable_seqscan", "on").expect("replace existing");
    assert_eq!(
        a,
        vec![
            "enable_seqscan=on".to_string(),
            "enable_indexscan=off".to_string()
        ]
    );

    // Delete one entry -> the other survives.
    let a = GUCArrayDelete(Some(a), "enable_seqscan")
        .expect("delete")
        .expect("array not yet empty");
    assert_eq!(a, vec!["enable_indexscan=off".to_string()]);

    // Delete the last entry -> None (store SQL NULL).
    let empty = GUCArrayDelete(Some(a.clone()), "enable_indexscan").expect("delete last");
    assert_eq!(empty, None);

    // Reset as superuser (install_once installs superuser()->true) drops all.
    let reset = GUCArrayReset(a).expect("reset");
    assert_eq!(reset, None);
}

// ---------------------------------------------------------------------------
// Parallel-worker GUC-state transfer (serialize.rs): EstimateGUCStateSpace /
// SerializeGUCState / RestoreGUCState round-trips.
// ---------------------------------------------------------------------------

#[test]
fn serialize_restore_round_trip() {
    use crate::live::{get_real, with_store, with_store_mut};
    use crate::serialize::{estimate_guc_state_space, restore_guc_state, serialize_guc_state};

    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    // Establish a few non-default values at PGC_S_OVERRIDE so they survive the
    // source-priority comparison and become non-skippable.
    for (name, value) in [
        ("bgwriter_delay", "321"),
        ("fsync", "off"),
        ("bgwriter_lru_multiplier", "3.5"),
    ] {
        set_config_option_global(
            name,
            Some(value),
            PGC_POSTMASTER,
            PGC_S_OVERRIDE,
            BOOTSTRAP_SUPERUSERID,
            crate::GUC_ACTION_SET,
            true,
            ERROR,
            false,
        )
        .unwrap_or_else(|_| panic!("set {name}"));
    }

    // Leader: estimate, allocate, serialize.
    let size = with_store(estimate_guc_state_space).expect("store initialized");
    assert!(size > std::mem::size_of::<usize>());
    let mut buf = vec![0u8; size];
    with_store(|reg| serialize_guc_state(reg, &mut buf))
        .expect("store")
        .expect("serialize fits within estimate");

    // RestoreGUCState first resets every non-skippable GUC to its default (so
    // set_config_option's source-priority comparison won't reject the re-set),
    // then applies the serialized values. To prove the round trip moves real
    // bytes, perturb one value to a different non-default before restoring: the
    // internal reset clears it back to default, and the deserialized leader
    // value is then re-applied.
    set_config_option_global(
        "bgwriter_delay",
        Some("777"),
        PGC_POSTMASTER,
        PGC_S_OVERRIDE,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("perturb bgwriter_delay");
    assert_eq!(get_int("bgwriter_delay"), Some(777));

    with_store_mut(|reg| restore_guc_state(reg, &buf))
        .expect("store")
        .expect("restore succeeds");

    // The leader's non-default values are reproduced in the worker store.
    assert_eq!(get_int("bgwriter_delay"), Some(321));
    assert_eq!(get_bool("fsync"), Some(false));
    assert_eq!(get_real("bgwriter_lru_multiplier"), Some(3.5));
}

#[test]
fn estimate_is_upper_bound() {
    use crate::live::with_store;
    use crate::serialize::{estimate_guc_state_space, serialize_guc_state};

    let _guard = GUC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    install_once();
    initialize_guc_options();

    set_config_option_global(
        "bgwriter_delay",
        Some("250"),
        PGC_POSTMASTER,
        PGC_S_OVERRIDE,
        BOOTSTRAP_SUPERUSERID,
        crate::GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .expect("set bgwriter_delay");

    // The serialized payload must fit within the estimate.
    let size = with_store(estimate_guc_state_space).expect("store");
    let mut buf = vec![0u8; size];
    with_store(|reg| serialize_guc_state(reg, &mut buf))
        .expect("store")
        .expect("payload fits within estimate");
}
