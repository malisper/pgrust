#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `contrib/pg_stat_statements/pg_stat_statements.c` — track planning and
//! execution statistics of all SQL statements, ported 1:1 from C.
//!
//! Registered as the in-process ported library `pg_stat_statements` (mirroring
//! `contrib-pg-prewarm`): the SQL emitted by the extension scripts resolves the
//! `LANGUAGE C AS 'MODULE_PATHNAME', '<sym>'` functions through the
//! dynamic-loader unit's ported-library registry (the Rust backend exposes no C
//! ABI). The module's `_PG_init` installs the executor/planner/utility hooks,
//! registers the five custom GUCs, and installs the `shmem_request` /
//! `shmem_startup` hooks; the shared state (`pgssSharedState` + the `pgssEntry`
//! HTAB) lives in genuine shared memory via `ShmemInitStruct` / `ShmemInitHash`.

mod normalize;
mod qtext;
mod shmem;
mod srf;
mod store;

use core::cell::Cell;

use ::datum::Datum;
use ::types_error::PgError;
use fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};

/// The simple (suffix-free, directory-free) module name.
pub(crate) const LIBRARY: &str = "pg_stat_statements";

/// The named LWLock tranche name (also the `ShmemInitStruct`/`ShmemInitHash`
/// names).
pub(crate) const PGSS_SHMEM_NAME: &str = "pg_stat_statements";

/// Magic number identifying the stats file format (`PGSS_FILE_HEADER`).
pub(crate) const PGSS_FILE_HEADER: u32 = 0x2022_0408;

// Usage / decay constants (pg_stat_statements.c:94-100).
pub(crate) const USAGE_INIT: f64 = 1.0;
pub(crate) const ASSUMED_MEDIAN_INIT: f64 = 10.0;
pub(crate) const ASSUMED_LENGTH_INIT: usize = 1024;
pub(crate) const USAGE_DECREASE_FACTOR: f64 = 0.99;
pub(crate) const STICKY_DECREASE_FACTOR: f64 = 0.50;
pub(crate) const USAGE_DEALLOC_PERCENT: i64 = 5;

/// `USAGE_EXEC(duration)` — always 1.0.
#[inline]
pub(crate) fn usage_exec(_duration: f64) -> f64 {
    1.0
}

/// `pgssStoreKind` (pg_stat_statements.c:119). `PGSS_PLAN`/`PGSS_EXEC` MUST be
/// 0/1 (array indices into the `Counters` arrays).
#[allow(dead_code)] // the kind passed by the post-parse jstate store (next increment)
pub(crate) const PGSS_INVALID: i32 = -1;
pub(crate) const PGSS_PLAN: usize = 0;
pub(crate) const PGSS_EXEC: usize = 1;
pub(crate) const PGSS_NUMKIND: usize = 2;

/// `pgssVersion` (pg_stat_statements.c:106).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PgssVersion {
    V1_0 = 0,
    V1_1,
    V1_2,
    V1_3,
    V1_8,
    V1_9,
    V1_10,
    V1_11,
    V1_12,
}

/// `PGSSTrackLevel` (pg_stat_statements.c:280).
pub(crate) const PGSS_TRACK_NONE: i32 = 0;
pub(crate) const PGSS_TRACK_TOP: i32 = 1;
pub(crate) const PGSS_TRACK_ALL: i32 = 2;

// ===========================================================================
// Shared-memory data structures (genuine MAP_SHARED layout; #[repr(C)]).
// ===========================================================================

/// `pgssHashKey` (pg_stat_statements.c:143) — the identity of a hashtable entry.
/// Hashed by `tag_hash` (HASH_BLOBS), so padding MUST be zeroed by the writer.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct PgssHashKey {
    pub userid: types_core::Oid,
    pub dbid: types_core::Oid,
    pub queryid: i64,
    pub toplevel: bool,
    // Explicit padding so the key is a fixed-size blob with deterministic bytes
    // (C lets the compiler insert 7 bytes after `toplevel`; pgss_store memsets
    // the whole key to clear it — we mirror that by zeroing this field).
    pub _pad: [u8; 7],
}

/// `Counters` (pg_stat_statements.c:154) — the per-statement accumulated stats.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Counters {
    pub calls: [i64; PGSS_NUMKIND],
    pub total_time: [f64; PGSS_NUMKIND],
    pub min_time: [f64; PGSS_NUMKIND],
    pub max_time: [f64; PGSS_NUMKIND],
    pub mean_time: [f64; PGSS_NUMKIND],
    pub sum_var_time: [f64; PGSS_NUMKIND],
    pub rows: i64,
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub local_blks_hit: i64,
    pub local_blks_read: i64,
    pub local_blks_dirtied: i64,
    pub local_blks_written: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub shared_blk_read_time: f64,
    pub shared_blk_write_time: f64,
    pub local_blk_read_time: f64,
    pub local_blk_write_time: f64,
    pub temp_blk_read_time: f64,
    pub temp_blk_write_time: f64,
    pub usage: f64,
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: u64,
    pub wal_buffers_full: i64,
    pub jit_functions: i64,
    pub jit_generation_time: f64,
    pub jit_inlining_count: i64,
    pub jit_deform_time: f64,
    pub jit_deform_count: i64,
    pub jit_inlining_time: f64,
    pub jit_optimization_count: i64,
    pub jit_optimization_time: f64,
    pub jit_emission_count: i64,
    pub jit_emission_time: f64,
    pub parallel_workers_to_launch: i64,
    pub parallel_workers_launched: i64,
}

impl Counters {
    pub(crate) fn zeroed() -> Counters {
        // SAFETY: Counters is a plain-old-data #[repr(C)] of integers/floats;
        // all-zero is a valid bit pattern (mirrors C's memset(&counters, 0)).
        unsafe { core::mem::zeroed() }
    }

    /// `IS_STICKY(c)` — `(calls[PLAN] + calls[EXEC]) == 0`.
    #[inline]
    pub(crate) fn is_sticky(&self) -> bool {
        self.calls[PGSS_PLAN] + self.calls[PGSS_EXEC] == 0
    }
}

/// `pgssEntry` (pg_stat_statements.c:231). `key` MUST be first (HASH_BLOBS keys
/// on the leading `keysize` bytes). Not `Copy` (the `mutex` is an `AtomicU32`);
/// the dump/restore paths copy its bytes via `ptr::read_unaligned` / a raw byte
/// slice instead.
#[repr(C)]
pub(crate) struct PgssEntry {
    pub key: PgssHashKey,
    pub counters: Counters,
    pub query_offset: usize,
    pub query_len: i32,
    pub encoding: i32,
    pub stats_since: i64,
    pub minmax_stats_since: i64,
    /// `slock_t mutex` — the per-entry spinlock protecting the counters. Modeled
    /// as a single atomic word (the same representation dynahash's own shmem
    /// spinlocks use); accessed via [`shmem::SpinLock`].
    pub mutex: core::sync::atomic::AtomicU32,
}

/// `pgssGlobalStats` (pg_stat_statements.c:218).
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct PgssGlobalStats {
    pub dealloc: i64,
    pub stats_reset: i64,
}

/// `pgssSharedState` (pg_stat_statements.c:246).
#[repr(C)]
pub(crate) struct PgssSharedState {
    /// `LWLock *lock` — the named-tranche lock protecting hashtable
    /// search/modification. Stored as a raw pointer into the MAP_SHARED LWLock
    /// array (the C `LWLock *`).
    pub lock: *const types_storage::storage::LWLock,
    pub cur_median_usage: f64,
    pub mean_query_len: usize,
    /// `slock_t mutex` — protects extent/n_writers/gc_count/stats.
    pub mutex: core::sync::atomic::AtomicU32,
    pub extent: usize,
    pub n_writers: i32,
    pub gc_count: i32,
    pub stats: PgssGlobalStats,
}

// SAFETY: PgssSharedState lives in the genuine MAP_SHARED segment; the raw
// `lock` pointer addresses the shared LWLock array (valid in every process).
unsafe impl Sync for PgssSharedState {}
unsafe impl Send for PgssSharedState {}

// ===========================================================================
// Process-local state (the C file-scope statics).
// ===========================================================================

thread_local! {
    /// Current nesting depth of planner/ExecutorRun/ProcessUtility calls.
    pub(crate) static NESTING_LEVEL: Cell<i32> = const { Cell::new(0) };

    // GUC backing stores (the C `pgss_max` etc.). Read/written via the
    // `GucVarAccessors` installed in `register_custom_gucs`.
    pub(crate) static PGSS_MAX: Cell<i32> = const { Cell::new(5000) };
    pub(crate) static PGSS_TRACK: Cell<i32> = const { Cell::new(PGSS_TRACK_TOP) };
    pub(crate) static PGSS_TRACK_UTILITY: Cell<bool> = const { Cell::new(true) };
    pub(crate) static PGSS_TRACK_PLANNING: Cell<bool> = const { Cell::new(false) };
    pub(crate) static PGSS_SAVE: Cell<bool> = const { Cell::new(true) };
}

#[inline]
pub(crate) fn nesting_level() -> i32 {
    NESTING_LEVEL.with(Cell::get)
}
#[inline]
pub(crate) fn pgss_max() -> i32 {
    PGSS_MAX.with(Cell::get)
}
#[inline]
pub(crate) fn pgss_track() -> i32 {
    PGSS_TRACK.with(Cell::get)
}
// Read by the ProcessUtility / planner hooks (the next-increment lane that
// threads the real Query/PlannedStmt through those hooks).
#[inline]
#[allow(dead_code)]
pub(crate) fn pgss_track_utility() -> bool {
    PGSS_TRACK_UTILITY.with(Cell::get)
}
#[inline]
#[allow(dead_code)]
pub(crate) fn pgss_track_planning() -> bool {
    PGSS_TRACK_PLANNING.with(Cell::get)
}
#[inline]
pub(crate) fn pgss_save() -> bool {
    PGSS_SAVE.with(Cell::get)
}

/// `pgss_enabled(level)` (pg_stat_statements.c:302).
#[inline]
pub(crate) fn pgss_enabled(level: i32) -> bool {
    // !IsParallelWorker() — pgrust parallel workers don't run the hooks in a way
    // that double-counts here; the nesting/track gate matches C for the
    // top-level/all distinction.
    let track = pgss_track();
    track == PGSS_TRACK_ALL || (track == PGSS_TRACK_TOP && level == 0)
}

// ===========================================================================
// Error raising (mirror contrib-pg-prewarm): the one dispatch point every
// PGFunction crosses downcasts the panic payload to the structured PgError.
// ===========================================================================

pub(crate) fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

// ===========================================================================
// fmgr argument accessors.
// ===========================================================================

pub(crate) fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo
        .arg(i)
        .expect("pg_stat_statements: missing oid arg")
        .value
        .as_oid()
}
pub(crate) fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo
        .arg(i)
        .expect("pg_stat_statements: missing int8 arg")
        .value
        .as_i64()
}
pub(crate) fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("pg_stat_statements: missing bool arg")
        .value
        .as_bool()
}

// ===========================================================================
// SQL-callable functions (PGFunction shape: fn(&mut fcinfo) -> Datum).
// ===========================================================================

/// `pg_stat_statements_reset()` (1.0..) — reset all entries.
fn fc_pg_stat_statements_reset(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match store::entry_reset(0, 0, 0, false) {
        Ok(_) => Datum::null(),
        Err(e) => raise(e),
    }
}

/// `pg_stat_statements_reset_1_7(userid, dbid, queryid)`.
fn fc_pg_stat_statements_reset_1_7(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let userid = arg_oid(fcinfo, 0);
    let dbid = arg_oid(fcinfo, 1);
    let queryid = arg_int64(fcinfo, 2);
    match store::entry_reset(userid, dbid, queryid, false) {
        Ok(_) => Datum::null(),
        Err(e) => raise(e),
    }
}

/// `pg_stat_statements_reset_1_11(userid, dbid, queryid, minmax_only)` RETURNS
/// timestamptz.
fn fc_pg_stat_statements_reset_1_11(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let userid = arg_oid(fcinfo, 0);
    let dbid = arg_oid(fcinfo, 1);
    let queryid = arg_int64(fcinfo, 2);
    let minmax_only = arg_bool(fcinfo, 3);
    match store::entry_reset(userid, dbid, queryid, minmax_only) {
        Ok(ts) => {
            fcinfo.isnull = false;
            Datum::from_i64(ts)
        }
        Err(e) => raise(e),
    }
}

macro_rules! pgss_version_fn {
    ($name:ident, $ver:expr) => {
        fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let showtext = arg_bool(fcinfo, 0);
            match srf::pg_stat_statements_internal(fcinfo, $ver, showtext) {
                Ok(()) => Datum::null(),
                Err(e) => raise(e),
            }
        }
    };
}
pgss_version_fn!(fc_pg_stat_statements_1_2, PgssVersion::V1_2);
pgss_version_fn!(fc_pg_stat_statements_1_3, PgssVersion::V1_3);
pgss_version_fn!(fc_pg_stat_statements_1_8, PgssVersion::V1_8);
pgss_version_fn!(fc_pg_stat_statements_1_9, PgssVersion::V1_9);
pgss_version_fn!(fc_pg_stat_statements_1_10, PgssVersion::V1_10);
pgss_version_fn!(fc_pg_stat_statements_1_11, PgssVersion::V1_11);
pgss_version_fn!(fc_pg_stat_statements_1_12, PgssVersion::V1_12);

/// Legacy 1.0/1.1 entry point (always showtext=true; api detected from natts).
fn fc_pg_stat_statements(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match srf::pg_stat_statements_internal(fcinfo, PgssVersion::V1_0, true) {
        Ok(()) => Datum::null(),
        Err(e) => raise(e),
    }
}

/// `pg_stat_statements_info()` RETURNS record.
fn fc_pg_stat_statements_info(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match srf::pg_stat_statements_info(fcinfo) {
        Ok(image) => {
            fcinfo.set_ref_result(::fmgr::boundary::RefPayload::Composite(image));
            fcinfo.isnull = false;
            Datum::null()
        }
        Err(e) => raise(e),
    }
}

// ===========================================================================
// Builtin-library registration + _PG_init.
// ===========================================================================

fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "pg_stat_statements_reset" => Some(fc_pg_stat_statements_reset),
        "pg_stat_statements_reset_1_7" => Some(fc_pg_stat_statements_reset_1_7),
        "pg_stat_statements_reset_1_11" => Some(fc_pg_stat_statements_reset_1_11),
        "pg_stat_statements" => Some(fc_pg_stat_statements),
        "pg_stat_statements_1_2" => Some(fc_pg_stat_statements_1_2),
        "pg_stat_statements_1_3" => Some(fc_pg_stat_statements_1_3),
        "pg_stat_statements_1_8" => Some(fc_pg_stat_statements_1_8),
        "pg_stat_statements_1_9" => Some(fc_pg_stat_statements_1_9),
        "pg_stat_statements_1_10" => Some(fc_pg_stat_statements_1_10),
        "pg_stat_statements_1_11" => Some(fc_pg_stat_statements_1_11),
        "pg_stat_statements_1_12" => Some(fc_pg_stat_statements_1_12),
        "pg_stat_statements_info" => Some(fc_pg_stat_statements_info),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// `_PG_init` (pg_stat_statements.c:384). Runs when the module is loaded via
/// `shared_preload_libraries`. Registers GUCs, installs hooks.
fn pg_init() -> ::types_error::PgResult<()> {
    // In order to create our shared memory area, we have to be loaded via
    // shared_preload_libraries. If not, fall out without hooking into the main
    // system. (We still allow the SQL functions to be created.)
    if !miscinit::process_shared_preload_libraries_in_progress() {
        return Ok(());
    }

    // Inform the postmaster that we want query_id calculation if
    // compute_query_id = auto.
    queryjumble::enable_query_id();

    register_custom_gucs();

    // Install hooks.
    shmem::install_hooks();
    store::install_exec_hooks();

    Ok(())
}

/// The custom-GUC registration block of `_PG_init` (split out so it can run once
/// the GUC store is up, mirroring `backend-pl-plpgsql-handler`).
fn register_custom_gucs() {
    use ::misc_guc::custom;
    use ::guc_tables::GucVarAccessors;
    use types_guc::{PGC_POSTMASTER, PGC_SIGHUP, PGC_SUSET};

    fn get_max() -> i32 {
        PGSS_MAX.with(Cell::get)
    }
    fn set_max(v: i32) {
        PGSS_MAX.with(|c| c.set(v));
    }
    let _ = custom::define_custom_int_variable(
        "pg_stat_statements.max",
        Some("Sets the maximum number of statements tracked by pg_stat_statements."),
        None,
        GucVarAccessors {
            get: get_max,
            set: set_max,
        },
        5000,
        100,
        i32::MAX / 2,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    fn get_track() -> i32 {
        PGSS_TRACK.with(Cell::get)
    }
    fn set_track(v: i32) {
        PGSS_TRACK.with(|c| c.set(v));
    }
    let _ = custom::define_custom_enum_variable(
        "pg_stat_statements.track",
        Some("Selects which statements are tracked by pg_stat_statements."),
        None,
        GucVarAccessors {
            get: get_track,
            set: set_track,
        },
        PGSS_TRACK_TOP,
        TRACK_OPTIONS,
        PGC_SUSET,
        0,
        None,
        None,
        None,
    );

    fn get_track_utility() -> bool {
        PGSS_TRACK_UTILITY.with(Cell::get)
    }
    fn set_track_utility(v: bool) {
        PGSS_TRACK_UTILITY.with(|c| c.set(v));
    }
    let _ = custom::define_custom_bool_variable(
        "pg_stat_statements.track_utility",
        Some("Selects whether utility commands are tracked by pg_stat_statements."),
        None,
        GucVarAccessors {
            get: get_track_utility,
            set: set_track_utility,
        },
        true,
        PGC_SUSET,
        0,
        None,
        None,
        None,
    );

    fn get_track_planning() -> bool {
        PGSS_TRACK_PLANNING.with(Cell::get)
    }
    fn set_track_planning(v: bool) {
        PGSS_TRACK_PLANNING.with(|c| c.set(v));
    }
    let _ = custom::define_custom_bool_variable(
        "pg_stat_statements.track_planning",
        Some("Selects whether planning duration is tracked by pg_stat_statements."),
        None,
        GucVarAccessors {
            get: get_track_planning,
            set: set_track_planning,
        },
        false,
        PGC_SUSET,
        0,
        None,
        None,
        None,
    );

    fn get_save() -> bool {
        PGSS_SAVE.with(Cell::get)
    }
    fn set_save(v: bool) {
        PGSS_SAVE.with(|c| c.set(v));
    }
    let _ = custom::define_custom_bool_variable(
        "pg_stat_statements.save",
        Some("Save pg_stat_statements statistics across server shutdowns."),
        None,
        GucVarAccessors {
            get: get_save,
            set: set_save,
        },
        true,
        PGC_SIGHUP,
        0,
        None,
        None,
        None,
    );

    custom::mark_guc_prefix_reserved("pg_stat_statements");
}

/// `track_options[]` (pg_stat_statements.c:287).
static TRACK_OPTIONS: &[::types_guc::config_enum_entry] = &[
    ::types_guc::config_enum_entry {
        name: "none",
        val: PGSS_TRACK_NONE,
        hidden: false,
    },
    ::types_guc::config_enum_entry {
        name: "top",
        val: PGSS_TRACK_TOP,
        hidden: false,
    },
    ::types_guc::config_enum_entry {
        name: "all",
        val: PGSS_TRACK_ALL,
        hidden: false,
    },
];

/// Install this unit's inward seams: register the `pg_stat_statements` module
/// with the dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(
        dfmgr_seams::BuiltinLibraryEntry {
            name: LIBRARY,
            lookup,
            pg_init: Some(pg_init),
        },
    );
}
