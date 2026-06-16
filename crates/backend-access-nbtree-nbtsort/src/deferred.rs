//! The heap-scan-driven serial build of `nbtsort.c` and the parallel build
//! coordination.
//!
//! ## Serial build (ported here)
//! `btbuild` / `_bt_spools_heapscan` / `_bt_build_callback` / `_bt_spool` drive
//! the serial CREATE INDEX path: create the spool(s), scan the heap once via
//! `table_index_build_scan` feeding a per-tuple callback closure into the
//! spool(s), then hand off to the (already-grounded) [`crate::_bt_leafbuild`]
//! sort + leaf-load. The per-tuple callback crosses the
//! `table_index_build_scan` seam as a `&mut dyn FnMut` closure (the seam
//! mechanism carries closures — exactly as proven by hash `hashbuild` and brin
//! `brinbuild`), so the older "a function seam cannot carry a closure"
//! rationale was false. The build scan bottoms out at the heap AM provider
//! (`heapam_handler.c` `index_build_range_scan`), the sanctioned
//! seam-and-panic boundary until that provider lands.
//!
//! ### Residual (genuinely blocked field read)
//! [`index_info_serial_flags`] is seam-and-panic: the AM `ambuild` callback
//! receives the type-erased `types_tableam::amapi::IndexInfo` (matching the
//! hash / spgist siblings and the `table_index_build_scan` seam contract),
//! whose `payload: Option<Box<dyn Any>>` exposes no `ii_Unique` /
//! `ii_NullsNotDistinct`. The btree serial spool setup needs those two flags
//! (to enable the uniqueness check in `tuplesort_begin_index_btree` and to
//! decide whether the dead-tuple `spool2` is created). No accessor or concrete
//! payload bridges the erased `amapi::IndexInfo` to the executor's
//! `execnodes::IndexInfo` in this repo, so the two-flag read is the only part
//! that panics; the entire scan / callback / spool plumbing around it is real.
//!
//! ## Parallel build (deferred behind a loud panic)
//! `_bt_begin_parallel` / `_bt_end_parallel` / `_bt_parallel_estimate_shared` /
//! `_bt_parallel_heapscan` / `_bt_leader_participate_as_worker` /
//! `_bt_parallel_scan_and_sort` / `_bt_parallel_build_main` coordinate leader
//! and worker processes through dynamic shared memory: a `ParallelContext` +
//! DSM segment + `shm_toc` holding the shared `BTShared` (with an embedded
//! spinlock + condition variable), the shared tuplesort `Sharedsort`, the
//! parallel table scan descriptor, and per-worker WAL/buffer usage arrays. That
//! machinery lives across `access/transam/parallel.c`, `storage/ipc`,
//! `utils/sort`, tableam, snapshots and instrumentation — sibling subsystems
//! whose ports are not present. These are NOT silently stubbed: they panic
//! loudly (sanctioned mirror-and-panic, never a placeholder stub-panic).

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::TUPLESORT_NONE;
use types_rel::Relation;
use types_tableam::amapi::IndexBuildResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_utils_activity_small::backend_progress::{
    pgstat_progress_update_multi_param, pgstat_progress_update_param,
};
use backend_utils_sort_tuplesort_seams as tuplesort;

use crate::BTSpool;

// progress-reporting constants (commands/progress.h + access/nbtree.h).
const PROGRESS_CREATEIDX_SUBPHASE: i32 = 10;
const PROGRESS_CREATEIDX_TUPLES_TOTAL: i32 = 11;
const PROGRESS_SCAN_BLOCKS_TOTAL: i32 = 15;
const PROGRESS_SCAN_BLOCKS_DONE: i32 = 16;
const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN: i64 = 2;

/// `BTBuildState` (nbtsort.c) — status record for the spooling/sorting build.
/// The `btleader` field (parallel-build leader state) is omitted: the parallel
/// build path is deferred, so a serial build never has a leader.
struct BTBuildState<'mcx> {
    /// `bool isunique`
    isunique: bool,
    /// `bool nulls_not_distinct`
    nulls_not_distinct: bool,
    /// `bool havedead` — saw a dead tuple during the scan (-> needs `spool2`).
    havedead: bool,
    /// `Relation heap` — mirrors the C `BTBuildState.heap`; set for the
    /// (deferred) parallel path's benefit, not read on the serial path (where
    /// the spool carries its own `heap`).
    #[allow(dead_code)]
    heap: Relation<'mcx>,
    /// `BTSpool *spool` — primary spool.
    spool: Option<BTSpool<'mcx>>,
    /// `BTSpool *spool2` — dead-tuple spool (unique indexes only).
    spool2: Option<BTSpool<'mcx>>,
    /// `double indtuples` — # tuples accepted into the index.
    indtuples: f64,
}

/// `index_info_serial_flags(indexInfo)` — read `(ii_Unique, ii_NullsNotDistinct)`
/// off the build's `IndexInfo`.
///
/// SEAM-AND-PANIC (genuine residual): the AM `ambuild` callback receives the
/// type-erased `types_tableam::amapi::IndexInfo` (matching the hash / spgist
/// siblings and the `table_index_build_scan` seam contract), whose
/// `payload: Option<Box<dyn Any>>` exposes no `ii_Unique` /
/// `ii_NullsNotDistinct`. No accessor or concrete payload bridges the erased
/// `amapi::IndexInfo` to the executor's `execnodes::IndexInfo` in this repo, so
/// these two flags — needed to enable the uniqueness check and to decide the
/// dead-tuple `spool2` — are unreachable. Lands once the erased
/// `amapi::IndexInfo` carries the build flags (or the build seam re-keys onto
/// `execnodes::IndexInfo`, as `table_index_build_range_scan` already does).
fn index_info_serial_flags(index_info: &types_tableam::amapi::IndexInfo) -> (bool, bool) {
    let _ = index_info;
    panic!(
        "nbtsort: btbuild needs ii_Unique / ii_NullsNotDistinct, but the AM \
         ambuild callback's type-erased amapi::IndexInfo (payload: dyn Any) \
         exposes neither and nothing bridges it to execnodes::IndexInfo; carry \
         the build flags on amapi::IndexInfo (or re-key the build seam onto \
         execnodes::IndexInfo) here"
    )
}

/// `btbuild()` — build a new btree index (the AM's `ambuild`).
pub fn btbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut types_tableam::amapi::IndexInfo,
) -> PgResult<IndexBuildResult> {
    let (isunique, nulls_not_distinct) = index_info_serial_flags(index_info);

    let mut buildstate = BTBuildState {
        isunique,
        nulls_not_distinct,
        havedead: false,
        heap: heap.alias(),
        spool: None,
        spool2: None,
        indtuples: 0.0,
    };

    // We expect to be called exactly once for any index relation. If that's
    // not the case, big trouble's what we have.
    if backend_storage_buffer_bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        index,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )? != 0
    {
        return Err(PgError::error("index already contains data"));
    }

    let reltuples = _bt_spools_heapscan(mcx, heap, index, &mut buildstate, index_info)?;

    // Finish the build by (1) completing the sort of the spool file, (2)
    // inserting the sorted tuples into btree pages and (3) building the upper
    // levels.  Finally, it may also be necessary to end use of parallelism.
    {
        let mut spool = buildstate
            .spool
            .take()
            .ok_or_else(|| PgError::error("btbuild: primary spool present"))?;
        let mut spool2 = buildstate.spool2.take();
        crate::_bt_leafbuild(mcx, &mut spool, spool2.as_mut())?;
        crate::_bt_spooldestroy(mcx, spool)?;
        if let Some(spool2) = spool2 {
            crate::_bt_spooldestroy(mcx, spool2)?;
        }
    }
    // (buildstate.btleader is always NULL in the serial path -> no
    // _bt_end_parallel.)

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: buildstate.indtuples,
    })
}

/// `_bt_spools_heapscan()` — create spools, scan the heap filling them.
fn _bt_spools_heapscan<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    buildstate: &mut BTBuildState<'mcx>,
    index_info: &mut types_tableam::amapi::IndexInfo,
) -> PgResult<f64> {
    // We size the sort area as maintenance_work_mem rather than work_mem to
    // speed index creation.  This should be OK since a single backend can't
    // run multiple index creations in parallel.
    let maintenance_work_mem = backend_utils_misc_guc_seams::maintenance_work_mem::call();

    // Report table scan phase started.
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_SUBPHASE,
        PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN,
    );

    // Begin serial tuplesort. (Parallel coordination is deferred, so the
    // ii_ParallelWorkers / btleader / SortCoordinate branch is not taken.)
    let sortstate = tuplesort::tuplesort_begin_index_btree::call(
        mcx,
        heap,
        index,
        buildstate.isunique,
        buildstate.nulls_not_distinct,
        maintenance_work_mem,
        TUPLESORT_NONE,
    )?;
    buildstate.spool = Some(BTSpool {
        sortstate,
        heap: heap.alias(),
        index: index.alias(),
        isunique: buildstate.isunique,
        nulls_not_distinct: buildstate.nulls_not_distinct,
    });

    // If building a unique index, put dead tuples in a second spool to keep
    // them out of the uniqueness check.  We expect that the second spool (for
    // dead tuples) won't get very full, so we give it only work_mem.
    if buildstate.isunique {
        let work_mem = maintenance_work_mem;
        let sortstate2 = tuplesort::tuplesort_begin_index_btree::call(
            mcx, heap, index, false, false, work_mem, TUPLESORT_NONE,
        )?;
        buildstate.spool2 = Some(BTSpool {
            sortstate: sortstate2,
            heap: heap.alias(),
            index: index.alias(),
            isunique: false,
            nulls_not_distinct: false,
        });
    }

    // Fill spool using the serial heap scan. (The parallel heapscan branch is
    // deferred.)
    let reltuples = {
        let bs = &mut *buildstate;
        backend_access_table_tableam_seams::table_index_build_scan::call(
            heap,
            index,
            index_info,
            true,
            true,
            &mut |tid: ItemPointerData,
                  values: &[Datum<'mcx>],
                  isnull: &[bool],
                  tuple_is_alive: bool|
                  -> PgResult<()> {
                _bt_build_callback(tid, values, isnull, tuple_is_alive, bs)
            },
        )?
    };

    // Set the progress target for the next phase.  Reset the block number
    // values set by table_index_build_scan.
    pgstat_progress_update_multi_param(
        &[
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
        ],
        &[buildstate.indtuples as i64, 0, 0],
    );

    // okay, all heap tuples are spooled
    if buildstate.spool2.is_some() && !buildstate.havedead {
        // spool2 turns out to be unnecessary
        if let Some(spool2) = buildstate.spool2.take() {
            crate::_bt_spooldestroy(mcx, spool2)?;
        }
    }

    Ok(reltuples)
}

/// `_bt_build_callback()` — per-heap-tuple build-scan callback. Inserts the
/// index tuple into the appropriate spool for subsequent processing.
fn _bt_build_callback<'mcx>(
    tid: ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    tuple_is_alive: bool,
    buildstate: &mut BTBuildState<'mcx>,
) -> PgResult<()> {
    // insert the index tuple into the appropriate spool file for subsequent
    // processing
    if tuple_is_alive || buildstate.spool2.is_none() {
        let spool = buildstate
            .spool
            .as_mut()
            .ok_or_else(|| PgError::error("_bt_build_callback: primary spool present"))?;
        _bt_spool(spool, tid, values, isnull)?;
    } else {
        // dead tuples are put into spool2
        buildstate.havedead = true;
        let spool2 = buildstate
            .spool2
            .as_mut()
            .ok_or_else(|| PgError::error("_bt_build_callback: dead-tuple spool present"))?;
        _bt_spool(spool2, tid, values, isnull)?;
    }

    buildstate.indtuples += 1.0;
    Ok(())
}

/// `_bt_spool()` — spool an index tuple, via `tuplesort_putindextuplevalues`.
pub fn _bt_spool<'mcx>(
    btspool: &mut BTSpool<'mcx>,
    self_tid: ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    // tuplesort_putindextuplevalues(btspool->sortstate, btspool->index,
    //                               self, values, isnull);
    //
    // The seam borrows `btspool.index` while mutating `btspool.sortstate`; split
    // the borrow so both can be reached at once.
    let BTSpool {
        sortstate, index, ..
    } = btspool;
    tuplesort::tuplesort_putindextuplevalues::call(sortstate, index, self_tid, values, isnull)
}

// ===========================================================================
// Parallel build coordination — deferred behind a loud panic (see module docs).
// ===========================================================================

/// `_bt_begin_parallel()` — create the parallel context and launch workers.
pub fn _bt_begin_parallel() -> ! {
    panic!("nbtsort::_bt_begin_parallel is deferred (parallel build / DSM)");
}

/// `_bt_end_parallel()` — shut down workers, destroy the parallel context.
pub fn _bt_end_parallel() -> ! {
    panic!("nbtsort::_bt_end_parallel is deferred (parallel build / DSM)");
}

/// `_bt_parallel_estimate_shared()` — size of the shared build state.
pub fn _bt_parallel_estimate_shared() -> ! {
    panic!("nbtsort::_bt_parallel_estimate_shared is deferred (parallel build / DSM)");
}

/// `_bt_parallel_heapscan()` — within the leader, wait for end of heap scan.
pub fn _bt_parallel_heapscan() -> ! {
    panic!("nbtsort::_bt_parallel_heapscan is deferred (parallel build / DSM)");
}

/// `_bt_leader_participate_as_worker()` — leader participates as a worker.
pub fn _bt_leader_participate_as_worker() -> ! {
    panic!("nbtsort::_bt_leader_participate_as_worker is deferred (parallel build / DSM)");
}

/// `_bt_parallel_scan_and_sort()` — a worker's portion of a parallel sort.
pub fn _bt_parallel_scan_and_sort() -> ! {
    panic!("nbtsort::_bt_parallel_scan_and_sort is deferred (parallel build / DSM)");
}

/// `_bt_parallel_build_main()` — a worker's portion of a parallel build.
pub fn _bt_parallel_build_main() -> ! {
    panic!("nbtsort::_bt_parallel_build_main is deferred (parallel build / DSM)");
}
