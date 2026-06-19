//! GUC variable backing storage owned by `pgstat.c`.
//!
//! C declares two plain process-global GUC variables in `pgstat.c`:
//!
//! ```c
//! bool        pgstat_track_counts = false;
//! int         pgstat_fetch_consistency = PGSTAT_FETCH_CONSISTENCY_CACHE;
//! ```
//!
//! These are `conf->variable` backing stores: the GUC machinery seeds them
//! from `boot_val` at startup and writes them on `SET`, and the rest of the
//! backend reads them directly (e.g. `pgstat_track_counts` gates every count
//! macro; `pgstat_fetch_consistency` selects the stats-read snapshot mode).
//! Neither comes from the control file.
//!
//! The idiomatic port holds the same per-process storage in `thread_local`
//! cells and exposes get/set accessors that [`crate::init_seams`] installs into
//! the GUC table slots ([`backend_utils_misc_guc_tables::vars`]) via
//! [`backend_utils_misc_guc_tables::GucVarAccessors`], so the engine's
//! `.read()` / `.write()` resolve to this storage.

use core::cell::Cell;

thread_local! {
    /// `bool pgstat_track_counts` (pgstat.c). Boot default `false`; the GUC
    /// engine overrides it from the `track_counts` `boot_val` (`true`).
    static TRACK_COUNTS: Cell<bool> = const { Cell::new(false) };

    /// `int pgstat_fetch_consistency` (pgstat.c). Boot default
    /// `PGSTAT_FETCH_CONSISTENCY_CACHE` (`1`); the GUC engine overrides it from
    /// the `stats_fetch_consistency` `boot_val` (also `cache`).
    static FETCH_CONSISTENCY: Cell<i32> = const { Cell::new(1) };

    /// The `pgstat_fetch_consistency` value the assign hook last acted on. C's
    /// `assign_stats_fetch_consistency` reads the OLD `pgstat_fetch_consistency`
    /// global (the hook fires before `*conf->variable = newval`); this port's GUC
    /// engine writes the variable slot *before* firing the deferred assign hook,
    /// so the hook tracks the prior value here. Seeded to the boot value (`1`).
    static FETCH_CONSISTENCY_PREV: Cell<i32> = const { Cell::new(1) };

    /// `int pgstat_track_functions` (pgstat.c). Boot default `TRACK_FUNC_NONE`
    /// (`0`); the GUC engine overrides it from the `track_functions` `boot_val`
    /// (also `none`). Read by `ExecInitFunc` (execExpr.c) to decide whether to
    /// wrap a function call in stats-usage accounting.
    static TRACK_FUNCTIONS: Cell<i32> = const { Cell::new(0) };
}

/// Read `pgstat_track_counts`.
pub fn track_counts() -> bool {
    TRACK_COUNTS.with(|c| c.get())
}

/// Write `pgstat_track_counts` (GUC assign).
pub fn set_track_counts(v: bool) {
    TRACK_COUNTS.with(|c| c.set(v));
}

/// Read `pgstat_fetch_consistency`.
pub fn fetch_consistency() -> i32 {
    FETCH_CONSISTENCY.with(|c| c.get())
}

/// Write `pgstat_fetch_consistency` (GUC assign).
pub fn set_fetch_consistency(v: i32) {
    FETCH_CONSISTENCY.with(|c| c.set(v));
}

/// `assign_stats_fetch_consistency(int newval, void *extra)` (pgstat.c). Changing
/// this value invalidates the currently-cached stats snapshot, so the next
/// access fetches the latest data — but don't drop it unnecessarily.
pub fn assign_stats_fetch_consistency(
    newval: i32,
    _extra: Option<&backend_utils_misc_guc_tables::GucHookExtra>,
) {
    let changed = FETCH_CONSISTENCY_PREV.with(|c| {
        let old = c.get();
        c.set(newval);
        old != newval
    });
    if changed {
        // C: `if (pgstat_fetch_consistency != newval) pgstat_clear_snapshot();`.
        crate::pgstat_core::pgstat_clear_snapshot();
    }
}

/// Read `pgstat_track_functions`.
pub fn track_functions() -> i32 {
    TRACK_FUNCTIONS.with(|c| c.get())
}

/// Write `pgstat_track_functions` (GUC assign).
pub fn set_track_functions(v: i32) {
    TRACK_FUNCTIONS.with(|c| c.set(v));
}
