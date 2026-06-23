//! Shared core: GUC globals, constants, per-backend process-local state, and
//! the in-memory data types of `autovacuum.c`.
//!
//! C's file-scope globals are per-backend (the launcher and each worker are
//! separate processes/backends), so they are `thread_local!` here, never
//! shared statics (AGENTS.md "Backend-global state"). The shared-memory data
//! types (`WorkerInfoData`, `AutoVacuumWorkItem`, `AutoVacuumShmemStruct`)
//! live behind the index-keyed accessor seams (the substrate owns the layout);
//! what this module holds is the launcher's process-local list element
//! (`AvlDbase`), the worker's database carrier (`AvwDbase`), the toast→main
//! mapping element (`AvRelation`), and the recheck result (`AutovacTable`).

use ::core::cell::Cell;

use ::types_core::{InvalidBlockNumber, InvalidOid, MultiXactId, Oid, TransactionId};

pub use ::types_autovacuum::{AutovacTable, AvlDbase, AvRelation, AvwDbase};

/* =========================================================================
 * GUC parameters (`autovacuum.c` lines 118-136). Per-backend GUC knobs, set by
 * the GUC machinery; held as thread-locals so the scheduling/threshold math
 * reads them per-backend.
 * ========================================================================= */

macro_rules! av_guc {
    ($(#[$m:meta])* $get:ident, $set:ident, $cell:ident, $ty:ty, $init:expr) => {
        thread_local!(static $cell: Cell<$ty> = const { Cell::new($init) });
        $(#[$m])*
        #[inline]
        pub fn $get() -> $ty {
            $cell.with(|c| c.get())
        }
        #[inline]
        pub fn $set(v: $ty) {
            $cell.with(|c| c.set(v));
        }
    };
}

// These cells back the autovacuum.c GUC globals (the `*conf->variable` storage
// for the matching guc_tables.c entries). They MUST be seeded with the C
// `boot_val` from guc_tables.c — the same discipline `guc-tables::backing` uses
// for its own GUC cells. This seeding is load-bearing: `InitializeGUCOptions`
// applies each boot_val (`registry::apply_value`) before this crate's
// `init_seams()` installs the `vars::*` accessor, and `apply_value` only writes
// the owner backing store when the accessor is already `installed()`. So the
// boot-time write is skipped here; without the correct seed these cells would
// keep a placeholder (formerly 0) for the process lifetime — making e.g.
// `autovacuum_freeze_max_age`/`autovacuum_multixact_freeze_max_age` read 0 and
// `vacuum_get_cutoffs` spuriously emit "cutoff for freezing multixacts is far
// in the past" on a fresh cluster.
//
// `autovacuum_start_daemon` (the `autovacuum` GUC) is seeded with the C
// boot_val `true` (guc_tables.c). The postmaster will no longer fork the
// (not-yet-fully-ported) background launcher even when this is on — that fork
// is suppressed in `LaunchMissingBackgroundProcesses` (faithful "no background
// autovacuum scheduled" semantics) — so a `true` boot_val is safe and lets the
// backend-side relstats writes in `index_update_stats`/`do_analyze_rel` fire
// (they gate on `AutoVacuumingActive()`), matching real PostgreSQL.
av_guc!(autovacuum_start_daemon, set_autovacuum_start_daemon, AUTOVACUUM_START_DAEMON, bool, true);
av_guc!(autovacuum_worker_slots, set_autovacuum_worker_slots, AUTOVACUUM_WORKER_SLOTS, i32, 16);
av_guc!(autovacuum_max_workers, set_autovacuum_max_workers, AUTOVACUUM_MAX_WORKERS, i32, 3);
av_guc!(autovacuum_work_mem, set_autovacuum_work_mem, AUTOVACUUM_WORK_MEM, i32, -1);
av_guc!(autovacuum_naptime, set_autovacuum_naptime, AUTOVACUUM_NAPTIME, i32, 60);
av_guc!(autovacuum_vac_thresh, set_autovacuum_vac_thresh, AUTOVACUUM_VAC_THRESH, i32, 50);
av_guc!(autovacuum_vac_max_thresh, set_autovacuum_vac_max_thresh, AUTOVACUUM_VAC_MAX_THRESH, i32, 100000000);
av_guc!(autovacuum_vac_scale, set_autovacuum_vac_scale, AUTOVACUUM_VAC_SCALE, f64, 0.2);
av_guc!(autovacuum_vac_ins_thresh, set_autovacuum_vac_ins_thresh, AUTOVACUUM_VAC_INS_THRESH, i32, 1000);
av_guc!(autovacuum_vac_ins_scale, set_autovacuum_vac_ins_scale, AUTOVACUUM_VAC_INS_SCALE, f64, 0.2);
av_guc!(autovacuum_anl_thresh, set_autovacuum_anl_thresh, AUTOVACUUM_ANL_THRESH, i32, 50);
av_guc!(autovacuum_anl_scale, set_autovacuum_anl_scale, AUTOVACUUM_ANL_SCALE, f64, 0.1);
av_guc!(autovacuum_freeze_max_age, set_autovacuum_freeze_max_age, AUTOVACUUM_FREEZE_MAX_AGE, i32, 200000000);
av_guc!(autovacuum_multixact_freeze_max_age, set_autovacuum_multixact_freeze_max_age, AUTOVACUUM_MULTIXACT_FREEZE_MAX_AGE, i32, 400000000);
av_guc!(autovacuum_vac_cost_delay, set_autovacuum_vac_cost_delay, AUTOVACUUM_VAC_COST_DELAY, f64, 2.0);
av_guc!(autovacuum_vac_cost_limit, set_autovacuum_vac_cost_limit, AUTOVACUUM_VAC_COST_LIMIT, i32, -1);
av_guc!(Log_autovacuum_min_duration, set_Log_autovacuum_min_duration, LOG_AUTOVACUUM_MIN_DURATION, i32, 600000);

// `pgstat_track_counts` (`utils/activity/pgstat.c`) — autovacuum refuses to
// run without it. The ONE true backing store for this process-global lives in
// the pgstat owner crate (whose GUC accessor the engine writes); at runtime
// `AutoVacuumingActive()` reads that live value through the
// `pgstat_track_counts` ext-seam, NOT this cell. This local cell exists only as
// the test-controllable backing the unit tests install the seam against (they
// have no pgstat crate). Seeded to the C boot_val `true`.
av_guc!(pgstat_track_counts, set_pgstat_track_counts, PGSTAT_TRACK_COUNTS, bool, true);

/* =========================================================================
 * Constants (`autovacuum.c` lines 139-273).
 * ========================================================================= */

/// `MIN_AUTOVAC_SLEEPTIME` — minimum time between two launcher awakenings (ms).
pub const MIN_AUTOVAC_SLEEPTIME: f64 = 100.0;
/// `MAX_AUTOVAC_SLEEPTIME` — seconds.
pub const MAX_AUTOVAC_SLEEPTIME: i64 = 300;

/// `NUM_WORKITEMS` — size of the `av_workItems` shmem array.
pub const NUM_WORKITEMS: i32 = 256;

/// `AutoVacForkFailed` — `av_signal[]` index: failed trying to start a worker.
pub const AutoVacForkFailed: i32 = 0;
/// `AutoVacRebalance` — `av_signal[]` index: rebalance the cost limits.
pub const AutoVacRebalance: i32 = 1;

/// `AutoVacuumWorkItemType::AVW_BRINSummarizeRange` (`postmaster/autovacuum.h`).
pub const AVW_BRINSummarizeRange: i32 = 0;

/* =========================================================================
 * File-static per-backend process-local state (`autovacuum.c` lines 151-317).
 * ========================================================================= */

av_guc!(av_storage_param_cost_delay, set_av_storage_param_cost_delay, AV_STORAGE_PARAM_COST_DELAY, f64, -1.0);
av_guc!(av_storage_param_cost_limit, set_av_storage_param_cost_limit, AV_STORAGE_PARAM_COST_LIMIT, i32, -1);

// `recentXid` — comparison point for `freeze_max_age`.
av_guc!(recentXid, set_recentXid, RECENT_XID, TransactionId, 0);
// `recentMulti` — comparison point for the multixact freeze age.
av_guc!(recentMulti, set_recentMulti, RECENT_MULTI, MultiXactId, 0);

// `default_freeze_min_age` (varies by database).
av_guc!(default_freeze_min_age, set_default_freeze_min_age, DEFAULT_FREEZE_MIN_AGE, i32, 0);
// `default_freeze_table_age`.
av_guc!(default_freeze_table_age, set_default_freeze_table_age, DEFAULT_FREEZE_TABLE_AGE, i32, 0);
// `default_multixact_freeze_min_age`.
av_guc!(default_multixact_freeze_min_age, set_default_multixact_freeze_min_age, DEFAULT_MULTIXACT_FREEZE_MIN_AGE, i32, 0);
// `default_multixact_freeze_table_age`.
av_guc!(default_multixact_freeze_table_age, set_default_multixact_freeze_table_age, DEFAULT_MULTIXACT_FREEZE_TABLE_AGE, i32, 0);

// `int AutovacuumLauncherPid` — PID of launcher, valid only in a worker while
// shutting down.
av_guc!(AutovacuumLauncherPid, set_AutovacuumLauncherPid, AUTOVACUUM_LAUNCHER_PID, i32, 0);

// `MyWorkerInfo` — this worker's slot index (the C `WorkerInfo` pointer), or
// -1 (the C `NULL`) when not a worker.
av_guc!(MyWorkerInfo, set_MyWorkerInfo, MY_WORKER_INFO, i32, -1);

/* =========================================================================
 * Inline transam/multixact comparison macros (`access/transam.h`,
 * `access/multixact.h`). Ported in-crate (faithful precedent) so the
 * wraparound math is byte-for-byte.
 * ========================================================================= */

/// `FirstNormalTransactionId` (`access/transam.h`).
pub const FirstNormalTransactionId: TransactionId = 3;
/// `FirstMultiXactId` (`access/multixact.h`).
pub const FirstMultiXactId: MultiXactId = 1;

/// `TransactionIdIsNormal(xid)` — `xid >= FirstNormalTransactionId`.
#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `MultiXactIdIsValid(multi)` — `multi != InvalidMultiXactId` (0).
#[inline]
pub const fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != 0
}

/// `TransactionIdPrecedes(id1, id2)` (`access/transam.c`) — id1 logically
/// precedes id2 in modulo-2^32 transaction-id space; special xids never
/// precede normal ones.
#[inline]
pub fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `MultiXactIdPrecedes(multi1, multi2)` (`access/transam/multixact.c`) — the
/// MultiXactId analogue of `TransactionIdPrecedes` (modulo-2^32 comparison).
#[inline]
pub fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    let diff = multi1.wrapping_sub(multi2) as i32;
    diff < 0
}

/// `OidIsValid(oid)` — `oid != InvalidOid`.
#[inline]
pub const fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/* =========================================================================
 * pg_class relkind / relpersistence byte values (`catalog/pg_class.h`).
 * ========================================================================= */

/// `RELKIND_RELATION` — `'r'`.
pub const RELKIND_RELATION: u8 = b'r';
/// `RELKIND_MATVIEW` — `'m'`.
pub const RELKIND_MATVIEW: u8 = b'm';
/// `RELKIND_TOASTVALUE` — `'t'`.
pub const RELKIND_TOASTVALUE: u8 = b't';
/// `RELPERSISTENCE_TEMP` — `'t'`.
pub const RELPERSISTENCE_TEMP: u8 = b't';

/// `StatisticRelationId` (`catalog/pg_statistic.h`) — `ANALYZE` refuses to work
/// with `pg_statistic`.
pub const StatisticRelationId: Oid = 2619;

/// `static AutoVacOpts *extract_autovac_opts(HeapTuple tup, TupleDesc
/// pg_class_desc)` (`autovacuum.c` lines 2718-2737).
///
/// Given a pg_class tuple, return the AutoVacOpts portion of reloptions, if set;
/// otherwise return `None`.
///
/// The `extractRelOptions(tup, pg_class_desc, NULL)` parse is foreign (the
/// reloptions parser), so it is performed by the catalog-reader seam, which
/// hands us its `StdRdOptions` result (`relopts`).  This function performs
/// autovacuum's own part: the relkind assertion and the `memcpy(av,
/// &((StdRdOptions *) relopts)->autovacuum, sizeof(AutoVacOpts))` projection.
pub fn extract_autovac_opts(
    relkind: u8,
    relopts: Option<types_reloptions::StdRdOptions>,
) -> Option<types_reloptions::AutoVacOpts> {
    debug_assert!(
        relkind == RELKIND_RELATION
            || relkind == RELKIND_MATVIEW
            || relkind == RELKIND_TOASTVALUE
    );

    /* C: if (relopts == NULL) return NULL; */
    let relopts = relopts?;

    /*
     * C: av = palloc(sizeof(AutoVacOpts));
     *    memcpy(av, &(((StdRdOptions *) relopts)->autovacuum), sizeof(AutoVacOpts));
     */
    Some(relopts.autovacuum)
}

/// `BlockNumberIsValid(blockNumber)` — `blockNumber != InvalidBlockNumber`.
#[inline]
pub const fn BlockNumberIsValid(block_number: ::types_core::BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}
