//! vacuumparallel.c: parallel index vacuum/cleanup. Thread-native per
//! docs/parallel-query-design.md — PVShared is an Arc of typed fields (no
//! shm_toc), dead items cross as an Arc<[ItemPointerData]> snapshot of the
//! leader's flat tid vec (C: shared TidStore in DSA), and each worker opens
//! its own relations. Divergences recorded in CATALOG: buffer/WAL usage
//! transfer and progress reporting skipped (consumers elided repo-wide),
//! queryid/debug_query_string not forwarded, error-context callback elided.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use ::amapi::{
    amparallelvacuumoptions, amusemaintenanceworkmem, VACUUM_OPTION_MAX_VALID_VALUE,
    VACUUM_OPTION_NO_PARALLEL, VACUUM_OPTION_PARALLEL_BULKDEL, VACUUM_OPTION_PARALLEL_CLEANUP,
    VACUUM_OPTION_PARALLEL_COND_CLEANUP,
};
use ::commands_vacuum::{
    set_vacuum_cost_balance_local, set_vacuum_shared_cost, vac_bulkdel_one_index,
    vac_cleanup_one_index, vac_close_indexes, vac_open_indexes, vacuum_shared_cost,
    VacuumSharedCost,
};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{BlockNumber, ForkNumber, Oid, BLCKSZ};
use ::types_error::PgResult;
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_rel::lock::{RowExclusiveLock, ShareUpdateExclusiveLock};
use ::types_rel::{Relation, RelationData};
use ::types_relscan::IndexAmKind;
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use ::types_tuple::ItemPointerData;

use init_small::globals as g;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PvIndVacStatus {
    Initial,
    NeedBulkdelete,
    NeedCleanup,
    Completed,
}

struct PvIndStats {
    status: PvIndVacStatus,
    parallel_workers_can_process: bool,
    istat_updated: bool,
    istat: IndexBulkDeleteResult,
}

struct PvShared {
    relid: Oid,
    maintenance_work_mem_worker: i32,
    ring_nbuffers: i32,
    reltuples: AtomicU64,
    estimated_count: AtomicBool,
    idx: AtomicU32,
    cost: Arc<VacuumSharedCost>,
    indstats: Vec<Mutex<PvIndStats>>,
    dead_items: Mutex<Arc<[ItemPointerData]>>,
}

/// Leader state; relations/bstrategy are passed per call (arena-tied).
pub struct ParallelVacuumState {
    pcxt: parallel::ParallelContextId,
    shared: Arc<PvShared>,
    will_parallel_vacuum: Vec<bool>,
    nindexes: usize,
    nindexes_parallel_bulkdel: i32,
    nindexes_parallel_cleanup: i32,
    nindexes_parallel_condcleanup: i32,
}

fn index_am_kind(indrel: &RelationData<'_>) -> IndexAmKind {
    IndexAmKind::from_relam(indrel.rd_rel.relam)
}

/// None = can't do parallel vacuum (C returns NULL).
pub fn parallel_vacuum_init(
    indrels: &[Relation<'_>],
    nrequested_workers: i32,
    vac_work_mem: i32,
    bstrategy: &BufferAccessStrategy,
    rel_id: Oid,
) -> PgResult<Option<ParallelVacuumState>> {
    debug_assert!(nrequested_workers >= 0);
    debug_assert!(!indrels.is_empty());
    let nindexes = indrels.len();

    let mut will_parallel_vacuum = vec![false; nindexes];
    let parallel_workers =
        parallel_vacuum_compute_workers(indrels, nrequested_workers, &mut will_parallel_vacuum)?;
    if parallel_workers <= 0 {
        return Ok(None);
    }

    xact::EnterParallelMode();
    let pcxt =
        parallel::CreateParallelContext("postgres", "parallel_vacuum_main", parallel_workers)?;

    let mut nindexes_mwm = 0;
    let mut nindexes_parallel_bulkdel = 0;
    let mut nindexes_parallel_cleanup = 0;
    let mut nindexes_parallel_condcleanup = 0;
    let mut indstats = Vec::with_capacity(nindexes);
    for (i, indrel) in indrels.iter().enumerate() {
        let vacoptions = amparallelvacuumoptions(index_am_kind(indrel));
        debug_assert!(
            vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP == 0
                || vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP == 0
        );
        debug_assert!(vacoptions <= VACUUM_OPTION_MAX_VALID_VALUE);
        indstats.push(Mutex::new(PvIndStats {
            status: PvIndVacStatus::Initial,
            parallel_workers_can_process: false,
            istat_updated: false,
            istat: IndexBulkDeleteResult::default(),
        }));
        if !will_parallel_vacuum[i] {
            continue;
        }
        if amusemaintenanceworkmem(index_am_kind(indrel)) {
            nindexes_mwm += 1;
        }
        if vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL != 0 {
            nindexes_parallel_bulkdel += 1;
        }
        if vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP != 0 {
            nindexes_parallel_cleanup += 1;
        }
        if vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP != 0 {
            nindexes_parallel_condcleanup += 1;
        }
    }

    parallel::InitializeParallelDSM(pcxt)?;

    let maintenance_work_mem_worker = if nindexes_mwm > 0 {
        g::maintenance_work_mem() / parallel_workers.min(nindexes_mwm)
    } else {
        g::maintenance_work_mem()
    };
    let _ = vac_work_mem; // dead_items stay leader-local (flat-vec divergence)

    let shared = Arc::new(PvShared {
        relid: rel_id,
        maintenance_work_mem_worker,
        ring_nbuffers: bufmgr_seams::get_access_strategy_buffer_count::call(bstrategy),
        reltuples: AtomicU64::new(0f64.to_bits()),
        estimated_count: AtomicBool::new(false),
        idx: AtomicU32::new(0),
        cost: Arc::new(VacuumSharedCost {
            cost_balance: AtomicU32::new(0),
            active_nworkers: AtomicU32::new(0),
        }),
        indstats,
        dead_items: Mutex::new(Arc::from(Vec::new())),
    });
    parallel::set_private(pcxt, Arc::clone(&shared) as Arc<dyn std::any::Any + Send + Sync>);

    Ok(Some(ParallelVacuumState {
        pcxt,
        shared,
        will_parallel_vacuum,
        nindexes,
        nindexes_parallel_bulkdel,
        nindexes_parallel_cleanup,
        nindexes_parallel_condcleanup,
    }))
}

pub fn parallel_vacuum_end(
    pvs: ParallelVacuumState,
    istats: &mut [Option<IndexBulkDeleteResult>],
) -> PgResult<()> {
    debug_assert!(!parallel::IsParallelWorker());
    debug_assert_eq!(istats.len(), pvs.nindexes);

    for (i, slot) in pvs.shared.indstats.iter().enumerate() {
        let s = slot.lock().unwrap_or_else(|e| e.into_inner());
        istats[i] = if s.istat_updated { Some(s.istat) } else { None };
    }

    parallel::DestroyParallelContext(pvs.pcxt)?;
    xact::ExitParallelMode();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn parallel_vacuum_bulkdel_all_indexes(
    pvs: &mut ParallelVacuumState,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
    dead_items: &[ItemPointerData],
    num_table_tuples: f64,
    num_index_scans: i32,
) -> PgResult<()> {
    debug_assert!(!parallel::IsParallelWorker());

    pvs.shared.reltuples.store(num_table_tuples.to_bits(), SeqCst);
    pvs.shared.estimated_count.store(true, SeqCst);
    *pvs.shared.dead_items.lock().unwrap_or_else(|e| e.into_inner()) = Arc::from(dead_items);

    parallel_vacuum_process_all_indexes(pvs, mcx, heaprel, indrels, bstrategy, num_index_scans, true)
}

#[allow(clippy::too_many_arguments)]
pub fn parallel_vacuum_cleanup_all_indexes(
    pvs: &mut ParallelVacuumState,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
    num_table_tuples: f64,
    num_index_scans: i32,
    estimated_count: bool,
) -> PgResult<()> {
    debug_assert!(!parallel::IsParallelWorker());

    pvs.shared.reltuples.store(num_table_tuples.to_bits(), SeqCst);
    pvs.shared.estimated_count.store(estimated_count, SeqCst);
    *pvs.shared.dead_items.lock().unwrap_or_else(|e| e.into_inner()) = Arc::from(Vec::new());

    parallel_vacuum_process_all_indexes(
        pvs,
        mcx,
        heaprel,
        indrels,
        bstrategy,
        num_index_scans,
        false,
    )
}

fn parallel_vacuum_compute_workers(
    indrels: &[Relation<'_>],
    nrequested: i32,
    will_parallel_vacuum: &mut [bool],
) -> PgResult<i32> {
    if !g::IsUnderPostmaster() || guc_tables::vars::max_parallel_maintenance_workers.read() == 0 {
        return Ok(0);
    }

    let mut nindexes_parallel_bulkdel = 0;
    let mut nindexes_parallel_cleanup = 0;
    for (i, indrel) in indrels.iter().enumerate() {
        let vacoptions = amparallelvacuumoptions(index_am_kind(indrel));
        let nblocks: BlockNumber = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            indrel,
            ForkNumber::MAIN_FORKNUM,
        )?;
        if vacoptions == VACUUM_OPTION_NO_PARALLEL
            || (nblocks as i64) < guc_tables::vars::min_parallel_index_scan_size.read() as i64
        {
            continue;
        }
        will_parallel_vacuum[i] = true;
        if vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL != 0 {
            nindexes_parallel_bulkdel += 1;
        }
        if vacoptions & (VACUUM_OPTION_PARALLEL_CLEANUP | VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0
        {
            nindexes_parallel_cleanup += 1;
        }
    }

    let nindexes_parallel: i32 = i32::max(nindexes_parallel_bulkdel, nindexes_parallel_cleanup) - 1;
    if nindexes_parallel <= 0 {
        return Ok(0);
    }

    let parallel_workers =
        if nrequested > 0 { nrequested.min(nindexes_parallel) } else { nindexes_parallel };
    Ok(parallel_workers.min(guc_tables::vars::max_parallel_maintenance_workers.read()))
}

fn parallel_vacuum_process_all_indexes(
    pvs: &mut ParallelVacuumState,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
    num_index_scans: i32,
    vacuum: bool,
) -> PgResult<()> {
    debug_assert!(!parallel::IsParallelWorker());

    let (new_status, mut nworkers) = if vacuum {
        (PvIndVacStatus::NeedBulkdelete, pvs.nindexes_parallel_bulkdel)
    } else {
        let mut n = pvs.nindexes_parallel_cleanup;
        if num_index_scans == 0 {
            n += pvs.nindexes_parallel_condcleanup;
        }
        (PvIndVacStatus::NeedCleanup, n)
    };

    nworkers -= 1;
    nworkers = nworkers.min(parallel::nworkers(pvs.pcxt));

    for (i, slot) in pvs.shared.indstats.iter().enumerate() {
        let mut s = slot.lock().unwrap_or_else(|e| e.into_inner());
        debug_assert!(s.status == PvIndVacStatus::Initial);
        s.status = new_status;
        s.parallel_workers_can_process = pvs.will_parallel_vacuum[i]
            && parallel_vacuum_index_is_parallel_safe(&indrels[i], num_index_scans, vacuum);
    }

    pvs.shared.idx.store(0, SeqCst);

    if nworkers > 0 {
        if num_index_scans > 0 {
            parallel::ReinitializeParallelDSM(pvs.pcxt)?;
        }

        pvs.shared.cost.cost_balance.store(g::VacuumCostBalance() as u32, SeqCst);
        pvs.shared.cost.active_nworkers.store(0, SeqCst);

        parallel::ReinitializeParallelWorkers(pvs.pcxt, nworkers);
        parallel::LaunchParallelWorkers(pvs.pcxt)?;

        if parallel::nworkers_launched(pvs.pcxt) > 0 {
            g::SetVacuumCostBalance(0);
            set_vacuum_cost_balance_local(0);
            set_vacuum_shared_cost(Some(Arc::clone(&pvs.shared.cost)));
        }
        // "launched %d parallel vacuum workers ..." at elevel: DEBUG2 without
        // VERBOSE (loud upstream), so not emitted.
    }

    parallel_vacuum_process_unsafe_indexes(pvs, mcx, heaprel, indrels, bstrategy)?;
    parallel_vacuum_process_safe_indexes(
        &pvs.shared,
        mcx,
        heaprel,
        indrels,
        bstrategy,
        vacuum_shared_cost().is_some(),
    )?;

    if nworkers > 0 {
        parallel::WaitForParallelWorkersToFinish(pvs.pcxt)?;
        // Buffer/WAL usage accumulation skipped: pgBufferUsage is derived
        // from live bufmgr counters here and its vacuum consumers (VERBOSE,
        // autovacuum log) are elided/loud.
    }

    for (i, slot) in pvs.shared.indstats.iter().enumerate() {
        let mut s = slot.lock().unwrap_or_else(|e| e.into_inner());
        if s.status != PvIndVacStatus::Completed {
            let status = s.status;
            drop(s);
            panic!(
                "parallel index vacuum on index \"{}\" is not completed (status {status:?})",
                indrels[i].name()
            );
        }
        s.status = PvIndVacStatus::Initial;
    }

    // Carry the shared balance back to the heap scan; disable shared costing.
    if let Some(shared_cost) = vacuum_shared_cost() {
        g::SetVacuumCostBalance(shared_cost.cost_balance.load(SeqCst) as i32);
        set_vacuum_shared_cost(None);
    }
    Ok(())
}

fn parallel_vacuum_process_safe_indexes(
    shared: &PvShared,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
    cost_active: bool,
) -> PgResult<()> {
    if cost_active {
        shared.cost.active_nworkers.fetch_add(1, SeqCst);
    }
    let result = (|| loop {
        let idx = shared.idx.fetch_add(1, SeqCst) as usize;
        if idx >= indrels.len() {
            return Ok(());
        }
        let can_process = shared.indstats[idx]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .parallel_workers_can_process;
        if !can_process {
            continue;
        }
        parallel_vacuum_process_one_index(shared, mcx, heaprel, &indrels[idx], idx, bstrategy)?;
    })();
    if cost_active {
        shared.cost.active_nworkers.fetch_sub(1, SeqCst);
    }
    result
}

fn parallel_vacuum_process_unsafe_indexes(
    pvs: &ParallelVacuumState,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
) -> PgResult<()> {
    debug_assert!(!parallel::IsParallelWorker());
    let cost_active = vacuum_shared_cost().is_some();
    if cost_active {
        pvs.shared.cost.active_nworkers.fetch_add(1, SeqCst);
    }
    let result = (|| {
        for (i, indrel) in indrels.iter().enumerate() {
            let can_process = pvs.shared.indstats[i]
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .parallel_workers_can_process;
            if can_process {
                continue;
            }
            parallel_vacuum_process_one_index(&pvs.shared, mcx, heaprel, indrel, i, bstrategy)?;
        }
        Ok(())
    })();
    if cost_active {
        pvs.shared.cost.active_nworkers.fetch_sub(1, SeqCst);
    }
    result
}

fn parallel_vacuum_process_one_index(
    shared: &PvShared,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrel: &Relation<'_>,
    idx: usize,
    bstrategy: &BufferAccessStrategy,
) -> PgResult<()> {
    let (status, istat) = {
        let s = shared.indstats[idx].lock().unwrap_or_else(|e| e.into_inner());
        (s.status, if s.istat_updated { Some(s.istat) } else { None })
    };

    let ivinfo = nbtree::IndexVacuumInfo {
        index: indrel,
        heaprel,
        analyze_only: false,
        estimated_count: shared.estimated_count.load(SeqCst),
        num_heap_tuples: f64::from_bits(shared.reltuples.load(SeqCst)),
        strategy: bstrategy.clone(),
    };

    let istat_res: Option<IndexBulkDeleteResult> = match status {
        PvIndVacStatus::NeedBulkdelete => {
            let dead_items =
                Arc::clone(&shared.dead_items.lock().unwrap_or_else(|e| e.into_inner()));
            Some(vac_bulkdel_one_index(mcx, &ivinfo, istat, &dead_items)?)
        }
        PvIndVacStatus::NeedCleanup => vac_cleanup_one_index(mcx, &ivinfo, istat)?,
        _ => panic!(
            "unexpected parallel vacuum index status {status:?} for index \"{}\"",
            indrel.name()
        ),
    };

    {
        let mut s = shared.indstats[idx].lock().unwrap_or_else(|e| e.into_inner());
        if let Some(res) = istat_res {
            s.istat = res;
            s.istat_updated = true;
        }
        s.status = PvIndVacStatus::Completed;
    }
    backend_progress::pgstat_progress_parallel_incr_param(
        backend_progress::progress::PROGRESS_VACUUM_INDEXES_PROCESSED,
        1,
    );
    Ok(())
}

fn parallel_vacuum_index_is_parallel_safe(
    indrel: &RelationData<'_>,
    num_index_scans: i32,
    vacuum: bool,
) -> bool {
    let vacoptions = amparallelvacuumoptions(index_am_kind(indrel));
    if vacuum {
        return vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL != 0;
    }
    if vacoptions & (VACUUM_OPTION_PARALLEL_CLEANUP | VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0 {
        return false;
    }
    !(num_index_scans > 0 && vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP != 0)
}

/// Worker entrypoint; the substrate has already connected to the database,
/// restored leader state, and entered parallel mode.
fn parallel_vacuum_main(pshared: &parallel::ParallelShared) -> PgResult<()> {
    let shared = pshared
        .private()
        .expect("parallel_vacuum_main without private state")
        .downcast::<PvShared>()
        .unwrap_or_else(|_| panic!("parallel_vacuum_main private state is not PvShared"));

    let _ = elog::elog(::types_error::DEBUG1, "starting parallel vacuum worker");

    let ctx = MemoryContext::new("parallel vacuum worker");
    let mcx = ctx.mcx();

    let rel = table::table_open(mcx, shared.relid, ShareUpdateExclusiveLock)?;
    let indrels = vac_open_indexes(mcx, &rel, RowExclusiveLock)?;
    debug_assert!(!indrels.is_empty());

    if shared.maintenance_work_mem_worker > 0 {
        g::set_maintenance_work_mem(shared.maintenance_work_mem_worker);
    }

    autovacuum_seams::vacuum_update_costs::call()?;
    g::SetVacuumCostBalance(0);
    set_vacuum_cost_balance_local(0);
    set_vacuum_shared_cost(Some(Arc::clone(&shared.cost)));

    let bstrategy = bufmgr_seams::get_access_strategy_with_size::call(
        BufferAccessStrategyType::BasVacuum,
        shared.ring_nbuffers * (BLCKSZ as i32 / 1024),
    );

    let result =
        parallel_vacuum_process_safe_indexes(&shared, mcx, &rel, &indrels, &bstrategy, true);

    if guc_tables::vars::track_cost_delay_timing.read() {
        backend_progress::pgstat_progress_parallel_incr_param(
            backend_progress::progress::PROGRESS_VACUUM_DELAY_TIME,
            ::commands_vacuum::parallel_vacuum_worker_delay_ns(),
        );
    }

    set_vacuum_shared_cost(None);
    bufmgr_seams::free_access_strategy::call(bstrategy);
    vac_close_indexes(indrels, RowExclusiveLock)?;
    table::table_close(rel, ShareUpdateExclusiveLock)?;
    result
}

pub fn init_seams() {
    parallel::register_parallel_worker_entrypoint("parallel_vacuum_main", parallel_vacuum_main);
}
