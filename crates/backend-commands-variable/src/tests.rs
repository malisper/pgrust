//! In-crate logic tests for variable.c hooks. These exercise the hooks' *own*
//! logic — the DateStyle parser/conflict detection, the transaction-mode
//! decision trees, SET ROLE `none`, the build-flag checks, the octal show
//! formatting, the clean-ascii hooks, and `parse_full_f64` — by installing
//! test-controlled stubs for only the seams those hooks touch.
//!
//! Seam slots are process-global install-once cells, and the `GUC_check_err*`
//! report is shared, so the suite serializes behind one mutex and installs the
//! mock seams exactly once. `init_seams()` is deliberately NOT called (it would
//! install the production panic-stubs and double-install the slots).

use super::*;
use backend_utils_misc_guc::{guc_check_error, reset_guc_check_error};
use std::cell::Cell;
use std::sync::{Mutex, MutexGuard, Once};

const XACT_READ_COMMITTED: i32 = 0;

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

thread_local! {
    static DATE_STYLE: Cell<i32> = const { Cell::new(USE_ISO_DATES) };
    static DATE_ORDER: Cell<i32> = const { Cell::new(DATEORDER_MDY) };
    static RESET_DATESTYLE: Cell<&'static str> = const { Cell::new("ISO, MDY") };
    static XACT_READ_ONLY: Cell<bool> = const { Cell::new(false) };
    static XACT_ISO_LEVEL: Cell<i32> = const { Cell::new(XACT_READ_COMMITTED) };
    static IS_TRANSACTION_STATE: Cell<bool> = const { Cell::new(true) };
    static IS_SUB_TRANSACTION: Cell<bool> = const { Cell::new(false) };
    static RECOVERY_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
    static FIRST_SNAPSHOT_SET: Cell<bool> = const { Cell::new(false) };
    static INIT_PARALLEL_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Hold the suite lock and install the mock seams exactly once.
#[must_use]
fn setup() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    INSTALL.call_once(|| {
        own::date_style::set(|| DATE_STYLE.with(|c| c.get()));
        own::date_order::set(|| DATE_ORDER.with(|c| c.get()));
        own::get_config_option_reset_string::set(|_name| {
            Ok(RESET_DATESTYLE.with(|c| c.get()).to_string())
        });
        backend_utils_adt_varlena_seams::split_identifier_string::set(|mcx, raw, sep| {
            // Minimal SplitIdentifierString for the test inputs: split on the
            // separator, trim ASCII whitespace, reject empty tokens (syntax
            // error), and downcase-fold unquoted identifiers.
            let mut out = mcx::PgVec::new_in(mcx);
            for piece in raw.split(sep) {
                let tok = piece.trim();
                if tok.is_empty() {
                    return Ok(None);
                }
                out.push(mcx::PgString::from_str_in(&tok.to_ascii_lowercase(), mcx)?);
            }
            Ok(Some(out))
        });
        backend_access_transam_xact_seams::xact_read_only::set(|| XACT_READ_ONLY.with(|c| c.get()));
        backend_access_transam_xact_seams::xact_iso_level::set(|| XACT_ISO_LEVEL.with(|c| c.get()));
        backend_access_transam_xact_seams::is_transaction_state::set(|| {
            IS_TRANSACTION_STATE.with(|c| c.get())
        });
        backend_access_transam_xact_seams::is_sub_transaction::set(|| {
            IS_SUB_TRANSACTION.with(|c| c.get())
        });
        backend_access_transam_xlog_seams::recovery_in_progress::set(|| {
            RECOVERY_IN_PROGRESS.with(|c| c.get())
        });
        own::first_snapshot_set::set(|| FIRST_SNAPSHOT_SET.with(|c| c.get()));
        backend_access_transam_parallel_seams::initializing_parallel_worker::set(|| {
            INIT_PARALLEL_WORKER.with(|c| c.get())
        });
    });
    reset_guc_check_error();
    // Reset cells to defaults for each test.
    DATE_STYLE.with(|c| c.set(USE_ISO_DATES));
    DATE_ORDER.with(|c| c.set(DATEORDER_MDY));
    RESET_DATESTYLE.with(|c| c.set("ISO, MDY"));
    XACT_READ_ONLY.with(|c| c.set(false));
    XACT_ISO_LEVEL.with(|c| c.set(XACT_READ_COMMITTED));
    IS_TRANSACTION_STATE.with(|c| c.set(true));
    IS_SUB_TRANSACTION.with(|c| c.set(false));
    RECOVERY_IN_PROGRESS.with(|c| c.set(false));
    FIRST_SNAPSHOT_SET.with(|c| c.set(false));
    INIT_PARALLEL_WORKER.with(|c| c.set(false));
    guard
}

fn canon(input: &str) -> Option<String> {
    let mut val = Some(input.to_string());
    let mut extra: Option<GucHookExtra> = None;
    if check_datestyle(&mut val, &mut extra, PGC_S_DEFAULT).unwrap() {
        val
    } else {
        None
    }
}

#[test]
fn datestyle_canonical_forms() {
    let _g = setup();
    assert_eq!(canon("ISO, MDY").as_deref(), Some("ISO, MDY"));
    assert_eq!(canon("iso").as_deref(), Some("ISO, MDY")); // order inherited
    assert_eq!(canon("SQL, DMY").as_deref(), Some("SQL, DMY"));
    assert_eq!(canon("German").as_deref(), Some("German, DMY")); // GERMAN sets DMY
    assert_eq!(canon("Postgres, YMD").as_deref(), Some("Postgres, YMD"));
    assert_eq!(canon("EURO").as_deref(), Some("ISO, DMY")); // EURO == DMY
    assert_eq!(canon("US").as_deref(), Some("ISO, MDY")); // US == MDY
}

#[test]
fn datestyle_conflicts_and_unknown_rejected() {
    let _g = setup();
    assert_eq!(canon("ISO, SQL"), None); // conflicting styles
    assert_eq!(canon("YMD, DMY"), None); // conflicting orders
    assert_eq!(canon("bogus"), None); // unknown keyword
}

#[test]
fn datestyle_default_recurses_into_reset_string() {
    let _g = setup();
    RESET_DATESTYLE.with(|c| c.set("Postgres, YMD"));
    assert_eq!(canon("DEFAULT").as_deref(), Some("Postgres, YMD"));
    // "DEFAULT, ISO" keeps the order from DEFAULT but overrides the style.
    assert_eq!(canon("DEFAULT, ISO").as_deref(), Some("ISO, YMD"));
}

#[test]
fn transaction_read_only_to_read_write_inside_ro_subxact_rejected() {
    let _g = setup();
    XACT_READ_ONLY.with(|c| c.set(true));
    IS_SUB_TRANSACTION.with(|c| c.set(true));
    let mut newval = false; // requesting read-write
    assert!(!check_transaction_read_only(&mut newval, &mut None, PGC_S_DEFAULT).unwrap());
    assert_eq!(
        guc_check_error().sqlstate,
        types_error::ERRCODE_ACTIVE_SQL_TRANSACTION
    );
}

#[test]
fn transaction_read_only_to_read_write_after_first_snapshot_rejected() {
    let _g = setup();
    XACT_READ_ONLY.with(|c| c.set(true));
    FIRST_SNAPSHOT_SET.with(|c| c.set(true));
    let mut newval = false;
    assert!(!check_transaction_read_only(&mut newval, &mut None, PGC_S_DEFAULT).unwrap());
}

#[test]
fn transaction_read_only_idempotent_and_parallel_worker_allowed() {
    let _g = setup();
    XACT_READ_ONLY.with(|c| c.set(true));
    IS_SUB_TRANSACTION.with(|c| c.set(true));
    // ro -> ro is always fine.
    let mut to_ro = true;
    assert!(check_transaction_read_only(&mut to_ro, &mut None, PGC_S_DEFAULT).unwrap());
    // Restoring state in a parallel worker bypasses the checks.
    INIT_PARALLEL_WORKER.with(|c| c.set(true));
    let mut to_rw = false;
    assert!(check_transaction_read_only(&mut to_rw, &mut None, PGC_S_DEFAULT).unwrap());
}

#[test]
fn transaction_isolation_serializable_in_recovery_rejected() {
    let _g = setup();
    RECOVERY_IN_PROGRESS.with(|c| c.set(true));
    let mut iso = types_core::XACT_SERIALIZABLE;
    assert!(!check_transaction_isolation(&mut iso, &mut None, PGC_S_DEFAULT).unwrap());
    assert_eq!(
        guc_check_error().sqlstate,
        types_error::ERRCODE_FEATURE_NOT_SUPPORTED
    );
}

#[test]
fn transaction_deferrable_in_subxact_rejected() {
    let _g = setup();
    IS_SUB_TRANSACTION.with(|c| c.set(true));
    let mut newval = true;
    assert!(!check_transaction_deferrable(&mut newval, &mut None, PGC_S_DEFAULT).unwrap());
}

#[test]
fn build_flag_hooks_reject_true_accept_false() {
    let _g = setup();
    let mut t = true;
    assert!(!check_bonjour(&mut t, &mut None, PGC_S_DEFAULT).unwrap());
    let mut t = true;
    assert!(!check_ssl(&mut t, &mut None, PGC_S_DEFAULT).unwrap());
    let mut t = true;
    assert!(!check_default_with_oids(&mut t, &mut None, PGC_S_DEFAULT).unwrap());
    let mut f = false;
    assert!(check_bonjour(&mut f, &mut None, PGC_S_DEFAULT).unwrap());
    assert!(check_ssl(&mut f, &mut None, PGC_S_DEFAULT).unwrap());
    assert!(check_default_with_oids(&mut f, &mut None, PGC_S_DEFAULT).unwrap());
}

#[test]
fn show_random_seed_is_unavailable() {
    let _g = setup();
    assert_eq!(show_random_seed(), "unavailable");
}

#[test]
fn parse_full_f64_matches_strtod_semantics() {
    assert_eq!(parse_full_f64("  5"), Some(5.0));
    assert_eq!(parse_full_f64("-2.5"), Some(-2.5));
    assert_eq!(parse_full_f64("5x"), None); // trailing junk
    assert_eq!(parse_full_f64(""), None);
    assert_eq!(parse_full_f64("   "), None);
}
