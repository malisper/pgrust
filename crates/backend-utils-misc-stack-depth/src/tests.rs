//! Tests for the `stack_depth.c` port.
//!
//! The module's mutable state lives in per-backend `thread_local`s, and the
//! GUC check-error channel is a process-global seam. All tests run on one
//! thread and serialize through a single mutex so the shared state and the
//! captured GUC detail/hint are deterministic.

use super::*;

use std::sync::{Mutex, OnceLock};

use backend_utils_misc_guc_file_seams as guc_seam;

/// Last detail/hint captured by the installed GUC check-error seams.
static CAPTURED: Mutex<(Option<String>, Option<String>)> = Mutex::new((None, None));

/// Serializes the whole suite (the statics + capture buffer are global) and
/// installs capturing GUC check-error seams once.
fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        guc_seam::guc_check_errdetail::set(|d| CAPTURED.lock().unwrap().0 = Some(d));
        guc_seam::guc_check_errhint::set(|h| CAPTURED.lock().unwrap().1 = Some(h));
    });
    guard
}

/// Reset the module statics to the C boot defaults and clear the capture.
fn reset_all() {
    set_max_stack_depth(100);
    assign_max_stack_depth(100);
    STACK_BASE_PTR.with(|c| c.set(0));
    STACK_DEPTH_RLIMIT_CACHE.with(|c| c.set(0));
    *CAPTURED.lock().unwrap() = (None, None);
}

#[test]
fn set_and_restore_stack_base_round_trips() {
    let _g = test_lock();
    reset_all();

    // set_stack_base() returns the OLD value (0/NULL) the first time.
    let old = set_stack_base();
    assert_eq!(old, 0);

    // A second set returns the previous (non-NULL) base.
    let prev = set_stack_base();
    assert_ne!(prev, 0);

    // restore puts the NULL state back.
    restore_stack_base(old);
    assert_eq!(STACK_BASE_PTR.with(Cell::get), 0);
}

#[test]
fn uninitialized_stack_base_is_never_too_deep() {
    let _g = test_lock();
    reset_all();
    // Even with a 0kB budget, a NULL base must report "not too deep".
    assign_max_stack_depth(0);
    assert!(!stack_is_too_deep());
    check_stack_depth().unwrap();
}

#[test]
fn assign_updates_kilobytes_and_bytes() {
    let _g = test_lock();
    reset_all();
    set_max_stack_depth(256);
    assign_max_stack_depth(256);
    assert_eq!(max_stack_depth(), 256);
    assert_eq!(max_stack_depth_bytes(), 256 * 1024);
}

#[test]
fn check_stack_depth_reports_postgres_error_shape() {
    let _g = test_lock();
    reset_all();
    // 0-byte budget -> any nonzero distance from the base is too deep.
    set_max_stack_depth(0);
    assign_max_stack_depth(0);
    set_stack_base();
    // Recurse one frame so the current SP differs from the recorded base.
    fn deeper() -> PgResult<()> {
        check_stack_depth()
    }
    let error = deeper().unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_STATEMENT_TOO_COMPLEX);
    assert_eq!(error.message(), "stack depth limit exceeded");
    assert_eq!(
        error.hint(),
        Some("Increase the configuration parameter \"max_stack_depth\" (currently 0kB), after ensuring the platform's stack depth limit is adequate.")
    );
}

#[test]
fn rlimit_is_finite_or_unknown_and_caches() {
    let _g = test_lock();
    reset_all();
    let first = get_stack_depth_rlimit();
    // -1 (unknown), SSIZE_MAX (infinite/overflow), or a finite positive value.
    assert!(first == -1 || first > 0);
    // Cached: the second call returns the identical value.
    assert_eq!(get_stack_depth_rlimit(), first);
}

#[test]
fn check_max_stack_depth_unknown_rlimit_accepts_anything() {
    let _g = test_lock();
    reset_all();
    // Force the cache to "unknown" (-1): with rlimit <= 0 the C check passes.
    STACK_DEPTH_RLIMIT_CACHE.with(|c| c.set(-1));
    assert!(check_max_stack_depth(i32::MAX, GucSource::PGC_S_TEST));
}

#[test]
fn check_max_stack_depth_accepts_within_limit() {
    let _g = test_lock();
    reset_all();
    // rlimit = 8 MiB; slop = 512 KiB.
    STACK_DEPTH_RLIMIT_CACHE.with(|c| c.set(8 * 1024 * 1024));
    let allowed_kb = ((8 * 1024 * 1024 - STACK_DEPTH_SLOP) / 1024) as i32;
    assert!(check_max_stack_depth(allowed_kb, GucSource::PGC_S_TEST));
}

#[test]
fn check_max_stack_depth_sets_guc_detail_and_hint() {
    let _g = test_lock();
    reset_all();
    let rlimit: isize = 8 * 1024 * 1024;
    STACK_DEPTH_RLIMIT_CACHE.with(|c| c.set(rlimit));

    let too_high_kb = ((rlimit - STACK_DEPTH_SLOP) / 1024 + 1) as i32;
    assert!(!check_max_stack_depth(too_high_kb, GucSource::PGC_S_TEST));

    let captured = CAPTURED.lock().unwrap();
    assert_eq!(
        captured.0,
        Some(format!(
            "\"max_stack_depth\" must not exceed {}kB.",
            (rlimit - STACK_DEPTH_SLOP) / 1024
        ))
    );
    assert_eq!(
        captured.1.as_deref(),
        Some("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.")
    );
}
