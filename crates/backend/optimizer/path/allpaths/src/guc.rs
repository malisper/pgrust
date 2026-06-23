//! allpaths.c-owned GUC variable storage and accessors.
//!
//! `allpaths.c` defines four file-scope GUC globals (the `conf->variable`
//! backing store for the matching `guc_tables.c` entries):
//!
//! ```c
//! bool enable_geqo = false;          /* just in case GUC doesn't set it */
//! int  geqo_threshold;
//! int  min_parallel_table_scan_size;
//! int  min_parallel_index_scan_size;
//! ```
//!
//! These are plain `bool`/`int` globals seeded by the GUC machinery from their
//! `boot_val`s during `InitializeGUCOptions`; the planner reads them at
//! join-search and parallel-worker-sizing time. allpaths.c is their canonical
//! home, so it installs the `conf->variable` accessors into the named guc-table
//! slots from [`crate::init_seams`]. The boot-value defaults below mirror the
//! `boot_val`s in guc_tables.c.

use core::cell::Cell;

use ::guc_tables::{vars, GucVarAccessors};

// guc_tables.c boot_vals.
//   "geqo"                         -> true
//   "geqo_threshold"               -> 12
//   "min_parallel_table_scan_size" -> (8 * 1024 * 1024) / BLCKSZ
//   "min_parallel_index_scan_size" -> (512 * 1024) / BLCKSZ
const BLCKSZ: i32 = 8192;
const DEFAULT_GEQO_THRESHOLD: i32 = 12;
const DEFAULT_MIN_PARALLEL_TABLE_SCAN_SIZE: i32 = (8 * 1024 * 1024) / BLCKSZ;
const DEFAULT_MIN_PARALLEL_INDEX_SCAN_SIZE: i32 = (512 * 1024) / BLCKSZ;

std::thread_local! {
    /// `bool enable_geqo` — boot_val `true`.
    static ENABLE_GEQO: Cell<bool> = const { Cell::new(true) };
    /// `int geqo_threshold` — boot_val 12.
    static GEQO_THRESHOLD: Cell<i32> = const { Cell::new(DEFAULT_GEQO_THRESHOLD) };
    /// `int min_parallel_table_scan_size` — boot_val (8MB / BLCKSZ).
    static MIN_PARALLEL_TABLE_SCAN_SIZE: Cell<i32> =
        const { Cell::new(DEFAULT_MIN_PARALLEL_TABLE_SCAN_SIZE) };
    /// `int min_parallel_index_scan_size` — boot_val (512KB / BLCKSZ).
    static MIN_PARALLEL_INDEX_SCAN_SIZE: Cell<i32> =
        const { Cell::new(DEFAULT_MIN_PARALLEL_INDEX_SCAN_SIZE) };
}

fn get_enable_geqo() -> bool {
    ENABLE_GEQO.with(Cell::get)
}
fn set_enable_geqo(v: bool) {
    ENABLE_GEQO.with(|c| c.set(v));
}

fn get_geqo_threshold() -> i32 {
    GEQO_THRESHOLD.with(Cell::get)
}
fn set_geqo_threshold(v: i32) {
    GEQO_THRESHOLD.with(|c| c.set(v));
}

fn get_min_parallel_table_scan_size() -> i32 {
    MIN_PARALLEL_TABLE_SCAN_SIZE.with(Cell::get)
}
fn set_min_parallel_table_scan_size(v: i32) {
    MIN_PARALLEL_TABLE_SCAN_SIZE.with(|c| c.set(v));
}

fn get_min_parallel_index_scan_size() -> i32 {
    MIN_PARALLEL_INDEX_SCAN_SIZE.with(Cell::get)
}
fn set_min_parallel_index_scan_size(v: i32) {
    MIN_PARALLEL_INDEX_SCAN_SIZE.with(|c| c.set(v));
}

/// Install the `conf->variable` accessors for the four allpaths.c-owned GUCs
/// into the guc_tables slots. Called once from [`crate::init_seams`] at
/// single-threaded startup.
pub(crate) fn install_allpaths_gucs() {
    vars::enable_geqo.install(GucVarAccessors {
        get: get_enable_geqo,
        set: set_enable_geqo,
    });
    vars::geqo_threshold.install(GucVarAccessors {
        get: get_geqo_threshold,
        set: set_geqo_threshold,
    });
    vars::min_parallel_table_scan_size.install(GucVarAccessors {
        get: get_min_parallel_table_scan_size,
        set: set_min_parallel_table_scan_size,
    });
    vars::min_parallel_index_scan_size.install(GucVarAccessors {
        get: get_min_parallel_index_scan_size,
        set: set_min_parallel_index_scan_size,
    });
}
