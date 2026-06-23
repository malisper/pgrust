//! xlog.c-owned GUC variable storage, accessors, and assign hooks.
//!
//! Mirrors the file-scope GUC globals in xlog.c that back the
//! `max_wal_size`/`min_wal_size`/`checkpoint_completion_target` settings, plus
//! the `assign_max_wal_size`/`assign_checkpoint_completion_target` hooks the GUC
//! machinery fires during `InitializeGUCOptions`. Each hook recomputes the
//! derived `CheckPointSegments` (xlog.c:CalculateCheckpointSegments) and
//! publishes it through [`crate::write::set_check_point_segments`].

extern crate std;

use core::cell::Cell;

use crate::{write, CalculateCheckpointSegments};

std::thread_local! {
    /// `int max_wal_size_mb` (xlog.c GUC global). Boot default mirrors the
    /// guc_tables boot_val (DEFAULT_MAX_WAL_SEGS * 16).
    static MAX_WAL_SIZE_MB: Cell<i32> = const { Cell::new(1024) };
    /// `int min_wal_size_mb` (xlog.c GUC global).
    static MIN_WAL_SIZE_MB: Cell<i32> = const { Cell::new(80) };
    /// `double CheckPointCompletionTarget` (xlog.c GUC global).
    static CHECKPOINT_COMPLETION_TARGET: Cell<f64> = const { Cell::new(0.9) };
    /// `bool EnableHotStandby = false;` (xlog.c:146) — the `hot_standby` GUC.
    static ENABLE_HOT_STANDBY: Cell<bool> = const { Cell::new(false) };
}

// --- max_wal_size_mb accessors (conf->variable) -----------------------------

fn get_max_wal_size_mb() -> i32 {
    MAX_WAL_SIZE_MB.with(Cell::get)
}
fn set_max_wal_size_mb(v: i32) {
    MAX_WAL_SIZE_MB.with(|c| c.set(v));
}

// --- min_wal_size_mb accessors ----------------------------------------------

fn get_min_wal_size_mb() -> i32 {
    MIN_WAL_SIZE_MB.with(Cell::get)
}
fn set_min_wal_size_mb(v: i32) {
    MIN_WAL_SIZE_MB.with(|c| c.set(v));
}

// --- CheckPointCompletionTarget accessors -----------------------------------

fn get_checkpoint_completion_target() -> f64 {
    CHECKPOINT_COMPLETION_TARGET.with(Cell::get)
}
fn set_checkpoint_completion_target(v: f64) {
    CHECKPOINT_COMPLETION_TARGET.with(|c| c.set(v));
}

/// `assign_max_wal_size(newval, extra)` (xlog.c:2224): set `max_wal_size_mb`
/// then recompute `CheckPointSegments`.
fn assign_max_wal_size_hook(
    newval: i32,
    _extra: Option<&::guc_tables::GucHookExtra>,
) {
    set_max_wal_size_mb(newval);
    recompute_checkpoint_segments();
}

/// `assign_checkpoint_completion_target(newval, extra)` (xlog.c:2231): set
/// `CheckPointCompletionTarget` then recompute `CheckPointSegments`.
fn assign_checkpoint_completion_target_hook(
    newval: f64,
    _extra: Option<&::guc_tables::GucHookExtra>,
) {
    set_checkpoint_completion_target(newval);
    recompute_checkpoint_segments();
}

/// `CalculateCheckpointSegments()` over the current GUC globals; publishes the
/// result into the driver-local `CheckPointSegments` cache.
fn recompute_checkpoint_segments() {
    let segs = CalculateCheckpointSegments(
        get_max_wal_size_mb(),
        crate::shmem::wal_segment_size(),
        get_checkpoint_completion_target(),
    );
    write::set_check_point_segments(segs);
}

/// Install the xlog-owned GUC variable accessors and assign hooks into the
/// guc-tables slots. Called from [`crate::init_seams`].
pub fn install() {
    use ::guc_tables::{hooks, vars, GucVarAccessors};

    vars::max_wal_size_mb.install(GucVarAccessors {
        get: get_max_wal_size_mb,
        set: set_max_wal_size_mb,
    });
    vars::min_wal_size_mb.install(GucVarAccessors {
        get: get_min_wal_size_mb,
        set: set_min_wal_size_mb,
    });
    vars::CheckPointCompletionTarget.install(GucVarAccessors {
        get: get_checkpoint_completion_target,
        set: set_checkpoint_completion_target,
    });
    vars::EnableHotStandby.install(GucVarAccessors {
        get: || ENABLE_HOT_STANDBY.with(Cell::get),
        set: |v| ENABLE_HOT_STANDBY.with(|c| c.set(v)),
    });

    hooks::assign_max_wal_size.install(assign_max_wal_size_hook);
    hooks::assign_checkpoint_completion_target.install(assign_checkpoint_completion_target_hook);
}
