//! pgvector 0.8.5 hnswbuild.c parallel build driver: `HnswBeginParallel`,
//! `HnswParallelBuildMain`, `HnswParallelScanAndInsert`, `ParallelHeapScan`,
//! `HnswEndParallel` and `ComputeParallelWorkers`.
//!
//! C shares the graph through a DSM area with relative pointers; pgrust's
//! parallel workers are threads, so the shared state is an `Arc<HnswShared>`
//! handed to them through the parallel context's private slot and the graph
//! itself is the same `Arc<SharedGraph>` every participant's `BuildState`
//! holds. Recorded DIVERGENCES:
//!
//! * `CreateParallelContext` library name is `"postgres"`, not C's `"vector"`:
//!   pgrust has no dynamic loading and `LookupParallelWorkerFunction` panics
//!   on any other library.
//! * `compute_parallel_workers` reimplements the part of PG's
//!   `plan_create_index_workers` that C's `ComputeParallelWorkers` consumes as
//!   a 0/non-0 gate, in `compute_parallel_worker`'s own order: the heap's
//!   `parallel_workers` reloption wins outright (no page-size gate), and only
//!   when it is unset does the log3 ramp over `min_parallel_table_scan_size`
//!   decide. It does NOT apply C's parallel-safety analysis of the index
//!   expressions/predicate (pgrust's `is_parallel_safe` needs a `PlannerRun`):
//!   any expression or predicate index builds serially. It also does NOT apply
//!   C's 32MB-of-`maintenance_work_mem`-per-participant floor (our
//!   participants share one graph budget rather than each owning a sort).
//! * `compute_parallel_workers` refuses a heap whose table AM is not `heap`:
//!   this build lane goes through `table_index_build_scan_with`, which is
//!   heap-only in pgrust, where C dispatches through the AM's
//!   `index_build_range_scan`.
//! * There is no `PARALLEL_KEY_QUERY_TEXT` hand-off: pgrust's
//!   `InitializeParallelDSM` already carries the leader's activity state.
//! * The graph budget is plain `maintenance_work_mem` for both a serial and a
//!   parallel build: C sizes a DSM segment instead and therefore subtracts a
//!   3MB allowance for the rest of the segment and clamps at
//!   `HNSW_MAX_GRAPH_MEMORY`. Neither applies to a heap allocation.
//! * The leader waits on a `Condvar` with a timeout and re-checks worker
//!   liveness, where C sleeps on a `ConditionVariable` that a dying worker's
//!   process exit implicitly signals.
//!
//! Teardown: every path that *refuses* to go parallel (`nworkers <= 0`,
//! `LaunchParallelWorkers == 0`, or an error before the launch) unregisters
//! the snapshot, destroys the parallel context and leaves parallel mode
//! before returning. After a successful launch, an error raised out of the
//! leader's scan or out of `parallel_heap_scan` deliberately does NOT run
//! `end_parallel`: the transaction abort's `AtEOXact_Parallel` /
//! resource-owner cleanup terminates and joins the workers, exactly as C
//! relies on its longjmp to the abort path.

use std::any::Any;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use execindexing::IndexInfo;
use mcx::Mcx;
use pgvector_hnsw::utils::init_support;
use types_core::{ForkNumber, Oid};
use types_error::{PgError, PgResult, DEBUG1};
use types_rel::lock::{
    AccessExclusiveLock, RowExclusiveLock, ShareLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use types_rel::Relation;

use crate::graph::SharedGraph;
use crate::{insert_tuple, BuildState};

/// C: `base == NULL ? 0 : 1024 * 1024` in `InsertTuple` — in a parallel build
/// every participant leaves this much headroom so that the window between the
/// memory check and the allocation (C: between releasing the allocator lock
/// and `HnswAlloc`) can never overrun the budget.
pub(crate) const PARALLEL_MEMORY_MARGIN: usize = 1024 * 1024;

/// C: `HnswShared` (hnsw.h). `graphData` becomes the shared `Arc<SharedGraph>`
/// and the trailing `ParallelTableScanDesc` becomes a separately allocated
/// `Arc` whose address is stable for the scans that point into it.
pub struct HnswShared {
    /* Immutable state */
    pub heaprelid: Oid,
    pub indexrelid: Oid,
    pub isconcurrent: bool,

    /* Worker progress: C's workersdonecv + mutex + (nparticipantsdone, reltuples) */
    pub done: Mutex<(i32, f64)>,
    pub workers_done: Condvar,

    /// C: `ParallelTableScanFromHnswShared(shared)`; published by the leader
    /// before it launches workers.
    pub pscan: OnceLock<Arc<tableam::ParallelTableScanDescShared>>,

    /* The graph itself, plus the per-participant build parameters C rederives
     * in each worker's InitBuildState. */
    pub graph: Arc<SharedGraph>,
    pub m: i32,
    pub ef_construction: i32,
    pub dimensions: i32,
    pub ml: f64,
    pub max_level: i32,
}

/// C: `HnswLeader`.
pub(crate) struct Leader {
    pcxt: parallel::ParallelContextId,
    /// C: `nparticipanttuplesorts` minus the leader (which always participates).
    nlaunched: i32,
    snapshot: Option<snapmgr::Snapshot>,
}

/// The pure arithmetic of the planner's `compute_parallel_worker` for the
/// heap-only case (`index_pages < 0`, `rel_parallel_workers == -1`): the log3
/// ramp over `min_parallel_table_scan_size`, zero below the threshold, capped
/// at `max_workers`. Kept byte-for-byte in step with
/// `optimizer/path/allpaths::compute_parallel_worker`.
pub(crate) fn workers_for_pages(
    heap_pages: f64,
    min_scan_size_pages: f64,
    max_workers: i32,
) -> i32 {
    let min_table = min_scan_size_pages.max(1.0);
    if heap_pages >= 0.0 && heap_pages < min_scan_size_pages {
        return 0;
    }
    let mut threshold = min_table;
    let mut workers: i32 = 1;
    while heap_pages >= threshold * 3.0 {
        workers += 1;
        threshold *= 3.0;
        if threshold > (i32::MAX / 3) as f64 {
            break;
        }
    }
    workers.min(max_workers)
}

/// `compute_parallel_worker`'s decision order for a heap-only relation,
/// composed with `ComputeParallelWorkers`'s use of it: the heap's
/// `parallel_workers` reloption is taken BEFORE the size gate (a table with
/// the reloption set builds in parallel even below
/// `min_parallel_table_scan_size`), and only when it is unset (-1) does the
/// log3 ramp decide — and then only as a 0/non-0 gate, C returning
/// `max_parallel_maintenance_workers` itself.
pub(crate) fn workers_for_heap(
    relopt: i32,
    heap_pages: f64,
    min_scan_size_pages: f64,
    max_workers: i32,
) -> i32 {
    if relopt != -1 {
        return relopt.min(max_workers).max(0);
    }
    if workers_for_pages(heap_pages, min_scan_size_pages, max_workers) == 0 {
        return 0;
    }
    max_workers
}

/// C: `ComputeParallelWorkers` (hnswbuild.c) over `plan_create_index_workers`.
pub(crate) fn compute_parallel_workers(
    heap: &Relation<'_>,
    index: &Relation<'_>,
    index_info: &IndexInfo<'_>,
) -> i32 {
    let _ = index;
    let max_workers = guc_tables::vars::max_parallel_maintenance_workers.read();
    if max_workers <= 0 {
        return 0;
    }
    // DIVERGENCE (see module header): the parallel scan goes through
    // `table_index_build_scan_with`, which is heap-only here.
    if heap.rd_rel.relam != tableam::HEAP_TABLE_AM_OID {
        return 0;
    }
    // plan_create_index_workers: never parallelize a temp table's index.
    if heap.rd_rel.relpersistence == types_core::catalog::RELPERSISTENCE_TEMP {
        return 0;
    }
    // DIVERGENCE (see module header): C tests the expressions/predicate with
    // is_parallel_safe; we refuse both outright.
    if !index_info.ii_Expressions.is_nil() || !index_info.ii_Predicate.is_nil() {
        return 0;
    }
    let heap_pages =
        bufmgr::RelationGetNumberOfBlocksInFork(heap, ForkNumber::MAIN_FORKNUM).unwrap_or(0) as f64;
    let min_pages = guc_tables::vars::min_parallel_table_scan_size.read() as f64;
    workers_for_heap(heap.get_parallel_workers(-1), heap_pages, min_pages, max_workers)
}

/// C: `HnswBeginParallel`. `Ok(None)` means "back out, do a serial build".
pub(crate) fn begin_parallel(
    shared: &Arc<HnswShared>,
    heap: &Relation<'_>,
    isconcurrent: bool,
    request: i32,
) -> PgResult<Option<Leader>> {
    debug_assert!(request > 0);
    xact::EnterParallelMode();

    let pcxt = match parallel::CreateParallelContext("postgres", "HnswParallelBuildMain", request) {
        Ok(p) => p,
        Err(e) => {
            xact::ExitParallelMode();
            return Err(e);
        }
    };

    // C: SnapshotAny for a non-concurrent build (None here), else a registered
    // transaction snapshot.
    let snapshot: Option<snapmgr::Snapshot> = if isconcurrent {
        let snap = snapmgr::GetTransactionSnapshot()?;
        Some(snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered a snapshot"))
    } else {
        None
    };

    let back_out = |snapshot: &Option<snapmgr::Snapshot>| -> PgResult<()> {
        if let Some(s) = snapshot {
            snapmgr::UnregisterSnapshot(Some(s));
        }
        parallel::DestroyParallelContext(pcxt)?;
        xact::ExitParallelMode();
        Ok(())
    };

    if let Err(e) = parallel::InitializeParallelDSM(pcxt) {
        let _ = back_out(&snapshot);
        return Err(e);
    }
    // C: `pcxt->seg == NULL` — no shared resources, fall back to serial.
    if parallel::nworkers(pcxt) <= 0 {
        back_out(&snapshot)?;
        return Ok(None);
    }

    let mut pscan = Arc::new(tableam::ParallelTableScanDescShared::default());
    {
        let target = Arc::get_mut(&mut pscan).expect("freshly created shared descriptor");
        let r = tableam::table_parallelscan_initialize(heap, target, &snapshot);
        if let Err(e) = r {
            let _ = back_out(&snapshot);
            return Err(e);
        }
    }
    shared
        .pscan
        .set(pscan)
        .unwrap_or_else(|_| unreachable!("parallel scan descriptor published once"));

    parallel::set_private(pcxt, Arc::clone(shared) as Arc<dyn Any + Send + Sync>);

    let nlaunched = match parallel::LaunchParallelWorkers(pcxt) {
        Ok(n) => n,
        Err(e) => {
            let _ = back_out(&snapshot);
            return Err(e);
        }
    };
    if nlaunched == 0 {
        // C: HnswEndParallel then serial build.
        parallel::WaitForParallelWorkersToFinish(pcxt)?;
        back_out(&snapshot)?;
        return Ok(None);
    }

    let _ = elog::ereport(DEBUG1)
        .errmsg_internal(format!("using {nlaunched} parallel workers"))
        .finish(loc("HnswBeginParallel"));

    Ok(Some(Leader { pcxt, nlaunched, snapshot }))
}

/// C: `HnswEndParallel`.
pub(crate) fn end_parallel(leader: Leader) -> PgResult<()> {
    let wait = parallel::WaitForParallelWorkersToFinish(leader.pcxt);
    if let Some(s) = &leader.snapshot {
        snapmgr::UnregisterSnapshot(Some(s));
    }
    let destroy = parallel::DestroyParallelContext(leader.pcxt);
    xact::ExitParallelMode();
    wait?;
    destroy
}

/// C: `ParallelHeapScan` — the leader waits until every participant (itself
/// included) has finished its portion of the heap scan, then adopts the shared
/// `reltuples`.
///
/// C sleeps on a `ConditionVariable` that a dying worker process implicitly
/// wakes. Our workers are threads, so a worker that ERRORs never touches the
/// counter: the wait therefore uses a bounded `wait_timeout` and, on each
/// wakeup, (a) drains parallel messages — which raises a worker's ERROR in the
/// leader — and (b) checks whether every worker has already stopped, in which
/// case it hands off to `WaitForParallelWorkersToFinish` so the error surfaces
/// rather than hanging.
pub(crate) fn parallel_heap_scan(leader: &Leader, shared: &Arc<HnswShared>) -> PgResult<f64> {
    // C runs this at the tail of HnswBeginParallel, right after the leader's
    // own participation: a worker that died before attaching is an
    // initialization failure, not something to wait for.
    parallel::WaitForParallelWorkersToAttach(leader.pcxt)?;

    let target = leader.nlaunched + 1;
    loop {
        {
            let done = lk(&shared.done);
            if done.0 >= target {
                return Ok(done.1);
            }
        }

        postgres_seams::check_for_interrupts::call()?;
        parallel::ProcessParallelMessages()?;

        if parallel::parallel_workers_all_stopped(leader.pcxt) {
            // Re-check under the lock: a worker may have finished (and exited)
            // between the counter read above and this liveness probe.
            let done = lk(&shared.done);
            if done.0 >= target {
                return Ok(done.1);
            }
            drop(done);
            // Every worker is gone with the scan unfinished: surface whatever
            // killed them instead of waiting forever.
            parallel::WaitForParallelWorkersToFinish(leader.pcxt)?;
            return Err(PgError::error(
                "parallel worker exited before finishing the hnsw heap scan",
            )
            .into());
        }

        let done = lk(&shared.done);
        let _ = shared
            .workers_done
            .wait_timeout(done, Duration::from_millis(100))
            .unwrap_or_else(|e| e.into_inner());
    }
}

/// C: `HnswParallelScanAndInsert` — one participant's portion of the scan.
pub(crate) fn parallel_scan_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    shared: &Arc<HnswShared>,
    is_leader: bool,
) -> PgResult<()> {
    let mut index_info = execindexing::BuildIndexInfo(mcx, index)?;
    index_info.ii_Concurrent = shared.isconcurrent;

    // C rebuilds the whole HnswBuildState per participant (InitBuildState);
    // only the graph is shared. The support functions must be per-participant
    // (FmgrInfo is not Send).
    let mut bs = BuildState {
        heap: Some(heap),
        index,
        fork_num: ForkNumber::MAIN_FORKNUM,
        m: shared.m,
        ef_construction: shared.ef_construction,
        dimensions: shared.dimensions,
        ml: shared.ml,
        max_level: shared.max_level,
        support: init_support(index)?,
        graph: Arc::clone(&shared.graph),
        reltuples: 0.0,
        memory_margin: PARALLEL_MEMORY_MARGIN,
    };

    let scan = {
        let pscan = shared.pscan.get().expect("leader published the parallel scan descriptor");
        tableam::table_beginscan_parallel(mcx, heap, pscan)?
    };

    let mut inner_err: Option<Box<PgError>> = None;
    // BuildState is threaded via raw pointer: the callback is FnMut and
    // borrows would alias bs.
    let bs_ptr: *mut BuildState<'_, 'mcx> = &mut bs;
    let scanned = execindexing::table_index_build_scan_with(
        mcx,
        heap,
        index,
        &mut index_info,
        true,
        Some(scan),
        |_index_rel, tid, values, isnull, _alive| {
            // SAFETY: this participant's own BuildState, touched only from
            // this thread; it outlives the scan.
            let bs = unsafe { &mut *bs_ptr };
            match insert_tuple(bs, values, isnull, tid) {
                Ok(true) => {
                    bs.graph.inc_indtuples();
                    Ok(())
                }
                Ok(false) => Ok(()),
                Err(e) => {
                    inner_err = Some(e);
                    Err(PgError::error("hnsw build insert failed").into())
                }
            }
        },
    );
    let reltuples = match scanned {
        Ok(n) => n,
        Err(e) => return Err(inner_err.unwrap_or(e)),
    };

    // C: record statistics under the shared mutex, then signal the leader.
    {
        let mut done = lk(&shared.done);
        done.0 += 1;
        done.1 += reltuples;
    }
    shared.workers_done.notify_all();

    let _ = elog::ereport(DEBUG1)
        .errmsg_internal(format!(
            "{} processed {} tuples",
            if is_leader { "leader" } else { "worker" },
            reltuples as i64
        ))
        .finish(loc("HnswParallelScanAndInsert"));

    Ok(())
}

/// C: `HnswParallelBuildMain`.
pub fn hnsw_parallel_build_main(ps: &parallel::ParallelShared) -> PgResult<()> {
    let private = ps.private().expect("HnswParallelBuildMain without shared state");
    let shared: Arc<HnswShared> = private
        .downcast::<HnswShared>()
        .unwrap_or_else(|_| panic!("HnswParallelBuildMain private state is HnswShared"));

    // Lock modes known to be obtained by index.c.
    let (heap_lockmode, index_lockmode): (LOCKMODE, LOCKMODE) = if shared.isconcurrent {
        (ShareUpdateExclusiveLock, RowExclusiveLock)
    } else {
        (ShareLock, AccessExclusiveLock)
    };

    let owner = mcx::MemoryContext::new_bump("hnsw parallel build worker");
    let mcx = owner.mcx();
    let heap = table_seams::table_open::call(mcx, shared.heaprelid, heap_lockmode)?;
    let index = match indexam_seams::index_open::call(mcx, shared.indexrelid, index_lockmode) {
        Ok(i) => i,
        Err(e) => {
            let _ = heap.close(heap_lockmode);
            return Err(e);
        }
    };

    let r = parallel_scan_and_insert(mcx, &heap, &index, &shared, false);

    let ci = index.close(index_lockmode);
    let ch = heap.close(heap_lockmode);
    r?;
    ci?;
    ch
}

pub fn init_seams() {
    parallel::register_parallel_worker_entrypoint(
        "HnswParallelBuildMain",
        hnsw_parallel_build_main,
    );
}

fn lk<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn loc(func: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, func)
}

#[cfg(test)]
mod tests {
    use super::*;

    // C's compute_parallel_worker takes the reloption branch BEFORE the size
    // gate: a table with parallel_workers set builds in parallel even below
    // min_parallel_table_scan_size.
    #[test]
    fn reloption_wins_over_the_size_gate() {
        // 100 pages, far below the 1024-page threshold.
        assert_eq!(workers_for_heap(-1, 100.0, 1024.0, 8), 0, "no reloption: size gate refuses");
        assert_eq!(workers_for_heap(3, 100.0, 1024.0, 8), 3, "reloption skips the size gate");
        assert_eq!(workers_for_heap(9, 100.0, 1024.0, 4), 4, "capped by max_workers");
        assert_eq!(workers_for_heap(0, 1.0e9, 1024.0, 8), 0, "parallel_workers = 0 means serial");
        // Above the gate and without a reloption, C returns
        // max_parallel_maintenance_workers itself, not the log3 count.
        assert_eq!(workers_for_heap(-1, 3072.0, 1024.0, 8), 8);
    }

    #[test]
    fn worker_count_follows_log3_rule() {
        assert_eq!(workers_for_pages(100.0, 1024.0, 8), 0);
        assert_eq!(workers_for_pages(1024.0, 1024.0, 8), 1);
        assert_eq!(workers_for_pages(3072.0, 1024.0, 8), 2);
        assert_eq!(workers_for_pages(9216.0, 1024.0, 8), 3);
        assert_eq!(workers_for_pages(9216.0, 1024.0, 2), 2);
        assert_eq!(workers_for_pages(1.0e9, 1024.0, 8), 8);
    }
}
