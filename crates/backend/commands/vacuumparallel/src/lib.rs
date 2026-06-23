#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

//! Faithful port of `backend/commands/vacuumparallel.c` — parallel-vacuum
//! coordination (PostgreSQL 18.3).
//!
//! In a parallel vacuum we perform both index bulk deletion and index cleanup
//! with parallel worker processes. Individual indexes are processed by one
//! vacuum process. `ParallelVacuumState` holds shared information plus the
//! memory space for dead items in the DSA area. Workers are launched at the
//! start of each parallel index bulk-deletion / cleanup pass; once all indexes
//! are processed the workers exit. Each pass re-initializes the parallel
//! context so the same DSM can be reused.
//!
//! ## Repo handle model
//!
//! At the vacuum layer this repo addresses substrate-owned objects by opaque
//! `Copy` handles: relations are [`Oid`], the dead-items store is a
//! [`TidStore`], the buffer-access ring is the real `BufferAccessStrategy`, the parallel
//! context is a [`ParallelContextHandle`], and the lazy-vacuum driver addresses
//! *this* state by an opaque [`ParallelVacuumStateHandle`]. The owned
//! `ParallelVacuumState`/`PVShared`/`PVIndStats` live PRIVATELY in a process-
//! global registry keyed by that handle's id — they never cross a seam by
//! value.
//!
//! ## DSM leader→worker handoff
//!
//! `vacuumparallel.c` keeps the leader/worker shared state in a DSM segment
//! addressed by `shm_toc` keys:
//!
//! - `PARALLEL_VACUUM_KEY_SHARED      = 1` — the `PVShared` snapshot.
//! - `PARALLEL_VACUUM_KEY_QUERY_TEXT  = 2` — `debug_query_string`.
//! - `PARALLEL_VACUUM_KEY_BUFFER_USAGE= 3` — per-worker `BufferUsage`.
//! - `PARALLEL_VACUUM_KEY_WAL_USAGE   = 4` — per-worker `WalUsage`.
//! - `PARALLEL_VACUUM_KEY_INDEX_STATS = 5` — the `PVIndStats` array.
//!
//! The actual `shm_toc_estimate_*` sizing calls are mirrored exactly through
//! the parallel-infra seams, but the *typed* data the worker reads back can't
//! be carried through the (untyped) `shm_toc_lookup` helpers the parallel crate
//! exposes. So this crate keeps a process-global "current DSM" side table
//! (`DSM`) holding the owned snapshots, written by the leader in
//! `parallel_vacuum_init`/`process_all_indexes` and read by
//! [`parallel_vacuum_main`]. This is in-process only (the same model as the
//! src-idiomatic register_*/lookup_* seams), so it is implemented as crate-
//! private functions over a thread_local, not as seams.

extern crate alloc;
// The leader/worker registry + DSM side table use process-global thread-local
// state (`std::thread_local!` + `std::collections::BTreeMap`), the same model
// the seam machinery itself uses; pull in std for them.
extern crate std;

use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use std::collections::BTreeMap;
use std::thread_local;

use utils_error::ereport;
use types_error::{ErrorLevel, ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERROR};

use types_core::instrument::{BufferUsage, WalUsage};
use types_core::Oid;
use types_dsa::{DsaHandle, DsaPointer};
use execparallel::ParallelContextHandle;
use types_storage::buf::BufferAccessStrategy;
use types_storage::lock::{NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_storage::storage::LWTRANCHE_PARALLEL_VACUUM_DSA;
use types_vacuum::vacuumlazy::{
    ParallelVacuumInit, ParallelVacuumInitArgs, ParallelVacuumStateHandle, TidStore,
};
use types_vacuum::vacuumparallel::{
    IndexBulkDeleteResult, IndexVacuumInfo, VacDeadItemsInfo, VacuumSharedCostState,
};
use alloc::sync::Arc;

use vacuum_seams as vac;
use vacuumlazy_seams as vl;
use transam_parallel as p;
use parallel_rt_seams as prt;

use table_seams as table_seam;
use amapi_seams as amapi;
use indexam_seams as indexam;
use relcache_seams as relcache_sx;

use tidstore as tidstore;
use instrument as instrument;
use support as buffer_support;
use lmgr_proc::proc_misc;
use postgres::globals as tcop;
use init_small::globals as initglobals;
use status as activity_status;
use activity_small::backend_progress as activity_progress;

// =======================================================================
// vacuumparallel.c constants.
// =======================================================================

/// `BLCKSZ` (`pg_config.h`).
const BLCKSZ: i32 = 8192;

/// `DEBUG2` as `ivinfo.message_level` (`utils/elog.h`).
const MESSAGE_LEVEL_DEBUG2: i32 = DEBUG2.0;

/// `PROGRESS_VACUUM_INDEXES_PROCESSED` (`commands/progress.h`).
const PROGRESS_VACUUM_INDEXES_PROCESSED: i32 = 11;
/// `PROGRESS_VACUUM_DELAY_TIME` (`commands/progress.h`).
const PROGRESS_VACUUM_DELAY_TIME: i32 = 12;

/// `amparallelvacuumoptions` flag bits (`access/vacuum.h`).
const VACUUM_OPTION_NO_PARALLEL: u8 = 0;
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;
const VACUUM_OPTION_PARALLEL_CLEANUP: u8 = 1 << 2;
const VACUUM_OPTION_MAX_VALID_VALUE: u8 =
    VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP | VACUUM_OPTION_PARALLEL_CLEANUP;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/vacuumparallel.c", 0, funcname)
}

// =======================================================================
// Private owned state (never crosses a seam by value).
// =======================================================================

/// `PVIndVacStatus` — status used during parallel index vacuum or cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
enum PVIndVacStatus {
    Initial = 0,
    NeedBulkdelete,
    NeedCleanup,
    Completed,
}

/// `PVShared` — shared information among parallel workers (DSM-resident in C).
///
/// Holds atomics (`cost_balance`/`active_nworkers`/`idx`) so it is neither
/// `Copy` nor `Clone`; it lives in the registry behind `&mut`.
struct PVShared {
    relid: Oid,
    elevel: i32,
    queryid: i64,
    reltuples: f64,
    estimated_count: bool,
    maintenance_work_mem_worker: i32,
    ring_nbuffers: i32,
    /// `pg_atomic_uint32 cost_balance` + `pg_atomic_uint32 active_nworkers` —
    /// the genuinely-shared cost-state cell that lives in the DSM segment. Held
    /// by `Arc` so the leader's enable-seam hands the SAME atomics to the
    /// `VacuumSharedCostBalance`/`VacuumActiveNWorkers` globals, and the worker
    /// codepath attaches to the same cell (faithful single-process image of the
    /// shared DSM page).
    cost_state: Arc<VacuumSharedCostState>,
    idx: core::sync::atomic::AtomicU32,
    dead_items_dsa_handle: DsaHandle,
    dead_items_handle: DsaPointer,
    dead_items_info: VacDeadItemsInfo,
}

impl Default for PVShared {
    fn default() -> Self {
        PVShared {
            relid: Oid::from(0u32),
            elevel: 0,
            queryid: 0,
            reltuples: 0.0,
            estimated_count: false,
            maintenance_work_mem_worker: 0,
            ring_nbuffers: 0,
            cost_state: VacuumSharedCostState::new(0, 0),
            idx: core::sync::atomic::AtomicU32::new(0),
            dead_items_dsa_handle: 0,
            dead_items_handle: 0,
            dead_items_info: VacDeadItemsInfo::default(),
        }
    }
}

/// `PVIndStats` — per-index status + bulk-deletion stats.
#[derive(Clone, Copy)]
struct PVIndStats {
    status: PVIndVacStatus,
    parallel_workers_can_process: bool,
    istat_updated: bool,
    istat: IndexBulkDeleteResult,
}

impl Default for PVIndStats {
    fn default() -> Self {
        PVIndStats {
            status: PVIndVacStatus::Initial,
            parallel_workers_can_process: false,
            istat_updated: false,
            istat: IndexBulkDeleteResult::default(),
        }
    }
}

/// `ParallelVacuumState` — the owned per-vacuum coordination state.
struct ParallelVacuumState {
    /// `NULL` for worker processes.
    pcxt: Option<ParallelContextHandle>,
    heaprel: Oid,
    indrels: Vec<Oid>,
    nindexes: i32,
    shared: PVShared,
    indstats: Vec<PVIndStats>,
    dead_items: TidStore,
    buffer_usage: Vec<BufferUsage>,
    wal_usage: Vec<WalUsage>,
    will_parallel_vacuum: Vec<bool>,
    nindexes_parallel_bulkdel: i32,
    nindexes_parallel_cleanup: i32,
    nindexes_parallel_condcleanup: i32,
    /// `BufferAccessStrategy bstrategy` — the worker's OWN VACUUM ring, the real
    /// `freelist.c` object (`Rc<RefCell<BufferAccessStrategyData>>`, `None` for
    /// the C `NULL` strategy). Each parallel worker creates its own ring sized
    /// from the DSM-shared `ring_nbuffers`; the leader never populates this field
    /// (its inherited real `BufferAccessStrategy` is consumed only for
    /// `ring_nbuffers` in `parallel_vacuum_init`).
    bstrategy: BufferAccessStrategy,
    relnamespace: Option<String>,
    relname: Option<String>,
    indname: Option<String>,
    status: PVIndVacStatus,
}

impl Default for ParallelVacuumState {
    fn default() -> Self {
        ParallelVacuumState {
            pcxt: None,
            heaprel: Oid::from(0u32),
            indrels: Vec::new(),
            nindexes: 0,
            shared: PVShared::default(),
            indstats: Vec::new(),
            dead_items: TidStore::none(),
            buffer_usage: Vec::new(),
            wal_usage: Vec::new(),
            will_parallel_vacuum: Vec::new(),
            nindexes_parallel_bulkdel: 0,
            nindexes_parallel_cleanup: 0,
            nindexes_parallel_condcleanup: 0,
            bstrategy: None,
            relnamespace: None,
            relname: None,
            indname: None,
            status: PVIndVacStatus::Initial,
        }
    }
}

// =======================================================================
// Registry: handle id -> owned ParallelVacuumState.
// =======================================================================

thread_local! {
    static REGISTRY: RefCell<BTreeMap<u64, ParallelVacuumState>> = RefCell::new(BTreeMap::new());
    /// Monotonic id counter; id 0 == none (the C `NULL`).
    static NEXT_ID: RefCell<u64> = const { RefCell::new(1) };
}

fn registry_insert(state: ParallelVacuumState) -> ParallelVacuumStateHandle {
    let id = NEXT_ID.with(|n| {
        let mut n = n.borrow_mut();
        let id = *n;
        *n += 1;
        id
    });
    REGISTRY.with(|r| r.borrow_mut().insert(id, state));
    ParallelVacuumStateHandle::new(id)
}

/// Run `f` against the registry entry for `pvs`, propagating its result.
fn with_state<R>(
    pvs: ParallelVacuumStateHandle,
    f: impl FnOnce(&mut ParallelVacuumState) -> PgResult<R>,
) -> PgResult<R> {
    REGISTRY.with(|r| {
        let mut map = r.borrow_mut();
        match map.get_mut(&pvs.id) {
            Some(state) => f(state),
            None => ereport(ERROR)
                .errmsg("parallel vacuum state handle is not registered")
                .finish(here("with_state"))
                .map(|()| unreachable!("ereport(ERROR) does not return")),
        }
    })
}

// =======================================================================
// DSM leader->worker handoff side table (PARALLEL_VACUUM_KEY_* analogue).
// =======================================================================

/// Snapshot of the worker-visible fields of `PVShared`
/// (`PARALLEL_VACUUM_KEY_SHARED`). The scalar fields are snapshotted by value;
/// the shared cost-state cell crosses by `Arc` clone so the worker codepath
/// gets the SAME atomics the leader allocated (the genuinely-shared DSM page),
/// and re-installs `VacuumSharedCostBalance`/`VacuumActiveNWorkers` pointing at
/// them via the cost-balance enable seams.
#[derive(Clone)]
struct SharedSnapshot {
    relid: Oid,
    elevel: i32,
    queryid: i64,
    maintenance_work_mem_worker: i32,
    ring_nbuffers: i32,
    cost_state: Arc<VacuumSharedCostState>,
    dead_items_dsa_handle: DsaHandle,
    dead_items_handle: DsaPointer,
}

impl Default for SharedSnapshot {
    fn default() -> Self {
        SharedSnapshot {
            relid: Oid::from(0u32),
            elevel: 0,
            queryid: 0,
            maintenance_work_mem_worker: 0,
            ring_nbuffers: 0,
            cost_state: VacuumSharedCostState::new(0, 0),
            dead_items_dsa_handle: 0,
            dead_items_handle: 0,
        }
    }
}

#[derive(Default)]
struct DsmContents {
    /// `PARALLEL_VACUUM_KEY_SHARED`.
    shared: SharedSnapshot,
    /// `PARALLEL_VACUUM_KEY_INDEX_STATS`.
    indstats: Vec<PVIndStats>,
    /// `PARALLEL_VACUUM_KEY_QUERY_TEXT` (`None` == no `debug_query_string`).
    query_text: Option<String>,
    /// `PARALLEL_VACUUM_KEY_BUFFER_USAGE` — per-worker `BufferUsage` slots in
    /// the DSM. The worker writes its own slot in `InstrEndParallelQuery`; the
    /// leader reads slot `i` in `InstrAccumParallelQuery`.
    buffer_usage: Vec<BufferUsage>,
    /// `PARALLEL_VACUUM_KEY_WAL_USAGE` — per-worker `WalUsage` slots in the DSM.
    wal_usage: Vec<WalUsage>,
}

thread_local! {
    static DSM: RefCell<DsmContents> = RefCell::new(DsmContents::default());
    /// C's file-local `save_pgBufferUsage` / `save_pgWalUsage`: the
    /// `InstrStartParallelQuery` snapshot held by this backend between Start and
    /// End. `None` until `instr_start_parallel_query_pv` runs.
    static PV_INSTR_SNAPSHOT: core::cell::Cell<Option<instrument::ParallelQueryUsageSnapshot>> =
        const { core::cell::Cell::new(None) };
}

/// `InstrStartParallelQuery()` — note current usage; mirror C's file-local save.
fn instr_start_parallel_query_pv_impl() -> PgResult<()> {
    let snap = instrument::InstrStartParallelQuery();
    PV_INSTR_SNAPSHOT.with(|c| c.set(Some(snap)));
    Ok(())
}

/// `InstrEndParallelQuery(&buffer_usage[worker], &wal_usage[worker])` — the
/// worker stores its deltas (since the saved snapshot) into its DSM slot.
fn instr_end_parallel_query_pv_impl(worker: i32) -> PgResult<()> {
    let snap = PV_INSTR_SNAPSHOT
        .with(|c| c.get())
        .expect("InstrEndParallelQuery without a prior InstrStartParallelQuery");
    let mut buf = BufferUsage::default();
    let mut wal = WalUsage::default();
    instrument::InstrEndParallelQuery(snap, &mut buf, &mut wal);
    dsm_store_worker_usage(worker as usize, buf, wal);
    Ok(())
}

/// `InstrAccumParallelQuery(&pvs->buffer_usage[i], &pvs->wal_usage[i])` — leader
/// accumulates worker `i`'s DSM slot into its own running stats.
fn instr_accum_parallel_query_pv_impl(worker: i32) -> PgResult<()> {
    let (buf, wal) = dsm_load_worker_usage(worker as usize);
    instrument::InstrAccumParallelQuery(&buf, &wal);
    Ok(())
}

fn dsm_store_shared(s: SharedSnapshot) {
    DSM.with(|d| d.borrow_mut().shared = s);
}
fn dsm_store_indstats(stats: &[PVIndStats]) {
    DSM.with(|d| d.borrow_mut().indstats = stats.to_vec());
}
fn dsm_store_one_indstats(idx: usize, snapshot: PVIndStats) {
    DSM.with(|d| {
        let mut d = d.borrow_mut();
        if idx < d.indstats.len() {
            d.indstats[idx] = snapshot;
        }
    });
}
fn dsm_store_query_text(q: Option<String>) {
    DSM.with(|d| d.borrow_mut().query_text = q);
}
fn dsm_lookup_shared() -> SharedSnapshot {
    DSM.with(|d| d.borrow().shared.clone())
}
fn dsm_lookup_indstats() -> Vec<PVIndStats> {
    DSM.with(|d| d.borrow().indstats.clone())
}
fn dsm_lookup_query_text() -> Option<String> {
    DSM.with(|d| d.borrow().query_text.clone())
}
/// Allocate the per-worker `BufferUsage`/`WalUsage` DSM slots (zeroed); the
/// leader calls this so workers and the leader-accum path share the same slots.
fn dsm_alloc_usage_slots(nworkers: usize) {
    DSM.with(|d| {
        let mut d = d.borrow_mut();
        d.buffer_usage = alloc::vec![BufferUsage::default(); nworkers];
        d.wal_usage = alloc::vec![WalUsage::default(); nworkers];
    });
}
/// Worker writes its `BufferUsage`/`WalUsage` deltas into its own DSM slot.
fn dsm_store_worker_usage(worker: usize, buf: BufferUsage, wal: WalUsage) {
    DSM.with(|d| {
        let mut d = d.borrow_mut();
        if worker < d.buffer_usage.len() {
            d.buffer_usage[worker] = buf;
            d.wal_usage[worker] = wal;
        }
    });
}
/// Leader reads worker `i`'s `BufferUsage`/`WalUsage` DSM slot.
fn dsm_load_worker_usage(worker: usize) -> (BufferUsage, WalUsage) {
    DSM.with(|d| {
        let d = d.borrow();
        if worker < d.buffer_usage.len() {
            (d.buffer_usage[worker], d.wal_usage[worker])
        } else {
            (BufferUsage::default(), WalUsage::default())
        }
    })
}

// =======================================================================
// Conversions between the two strategy-handle newtypes.
// =======================================================================


// =======================================================================
// Public seam entrypoints (registry borrow happens here).
// =======================================================================

/// `parallel_vacuum_init(rel, indrels, nindexes, nrequested_workers,
/// vac_work_mem, elevel, bstrategy)` (vacuumparallel.c:242). On success returns
/// the handle plus the dead-items store and its sizing info; on the "can't go
/// parallel" path returns a `none()` handle (the C `NULL`).
fn parallel_vacuum_init(args: ParallelVacuumInitArgs) -> PgResult<ParallelVacuumInit> {
    let ParallelVacuumInitArgs {
        rel,
        indrels,
        nindexes,
        nrequested,
        vac_work_mem,
        elevel,
        bstrategy,
    } = args;

    /*
     * A parallel vacuum must be requested and there must be indexes on the
     * relation.
     */
    debug_assert!(nrequested >= 0);
    debug_assert!(nindexes > 0);

    // The C `CreateParallelContext` allocates the worker array + DSM bookkeeping
    // in TopTransactionContext; the parallel-infra seams need an Mcx, and the
    // leader transiently reopens each index `Relation` (to read its index-AM
    // options / block count off the real value type, exactly as C reads
    // `indrel->rd_indam->...` off the already-open leader relations). There is
    // no Mcx in the inward `parallel_vacuum_init` contract, so use a crate-owned
    // context for the duration of the calls (mirrors the src-idiomatic
    // `MemoryContext::new` idiom for the same calls).
    let ctx = mcx::MemoryContext::new("parallel_vacuum_init");
    let mcx = ctx.mcx();

    /*
     * Compute the number of parallel vacuum workers to launch.
     */
    let mut will_parallel_vacuum = alloc::vec![false; nindexes as usize];
    let parallel_workers =
        parallel_vacuum_compute_workers(mcx, &indrels, nrequested, &mut will_parallel_vacuum)?;
    if parallel_workers <= 0 {
        /* Can't perform vacuum in parallel -- return NULL */
        return Ok(ParallelVacuumInit {
            pvs: ParallelVacuumStateHandle::none(),
            dead_items: TidStore::none(),
            dead_items_info: VacDeadItemsInfo::default(),
        });
    }

    let mut pvs = ParallelVacuumState {
        indrels: indrels.clone(),
        nindexes,
        will_parallel_vacuum,
        // The leader's inherited `bstrategy` (the real `BufferAccessStrategy`) is
        // consumed only for `ring_nbuffers` below; `pvs.bstrategy` (the worker
        // ring) stays `None` on the leader — each worker creates its own.
        heaprel: rel,
        ..Default::default()
    };

    prt::enter_parallel_mode::call()?;

    let pcxt = p::create_parallel_context(
        mcx,
        String::from("postgres"),
        String::from("parallel_vacuum_main"),
        parallel_workers,
    )?;
    debug_assert!(p::pcxt_nworkers(pcxt) > 0);
    pvs.pcxt = Some(pcxt);
    let pcxt_nworkers = p::pcxt_nworkers(pcxt);

    let estimator = p::pcxt_estimator(pcxt);

    /* Estimate size for index vacuum stats -- PARALLEL_VACUUM_KEY_INDEX_STATS */
    let est_indstats_len = mul_size(core::mem::size_of::<PVIndStats>(), nindexes as usize);
    p::shm_toc_estimate_chunk(estimator, est_indstats_len);
    p::shm_toc_estimate_keys(estimator, 1);

    /* Estimate size for shared information -- PARALLEL_VACUUM_KEY_SHARED */
    let est_shared_len = core::mem::size_of::<PVShared>();
    p::shm_toc_estimate_chunk(estimator, est_shared_len);
    p::shm_toc_estimate_keys(estimator, 1);

    /*
     * Estimate space for BufferUsage and WalUsage --
     * PARALLEL_VACUUM_KEY_BUFFER_USAGE and PARALLEL_VACUUM_KEY_WAL_USAGE.
     */
    p::shm_toc_estimate_chunk(
        estimator,
        mul_size(core::mem::size_of::<BufferUsage>(), pcxt_nworkers as usize),
    );
    p::shm_toc_estimate_keys(estimator, 1);
    p::shm_toc_estimate_chunk(
        estimator,
        mul_size(core::mem::size_of::<WalUsage>(), pcxt_nworkers as usize),
    );
    p::shm_toc_estimate_keys(estimator, 1);

    /* Finally, estimate PARALLEL_VACUUM_KEY_QUERY_TEXT space */
    let debug_query = vac::debug_query_string_pv::call()?;
    let querylen = match &debug_query {
        Some(q) => {
            let querylen = q.len();
            p::shm_toc_estimate_chunk(estimator, querylen + 1);
            p::shm_toc_estimate_keys(estimator, 1);
            querylen
        }
        None => 0, /* keep compiler quiet */
    };
    let _ = querylen;

    p::initialize_parallel_dsm(mcx, pcxt)?;

    /* Prepare index vacuum stats */
    let mut indstats: Vec<PVIndStats> = alloc::vec![PVIndStats::default(); nindexes as usize];
    let mut nindexes_mwm = 0;
    for i in 0..nindexes as usize {
        /* Recover the leader's already-open index Relation from the relcache
         * (NoLock: the index is held under RowExclusiveLock by the leader) to
         * read its index-AM options off the real value type. */
        let indrel = vac::index_open_lock::call(mcx, pvs.indrels[i], NoLock)?;
        let vacoptions = vac::am_parallel_vacuum_options::call(&indrel)?;

        /*
         * Cleanup option should be either disabled, always performing in
         * parallel or conditionally performing in parallel.
         */
        debug_assert!(
            (vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0
                || (vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0
        );
        debug_assert!(vacoptions <= VACUUM_OPTION_MAX_VALID_VALUE);

        if !pvs.will_parallel_vacuum[i] {
            continue;
        }

        if vac::am_use_maintenance_work_mem::call(&indrel)? {
            nindexes_mwm += 1;
        }

        /*
         * Remember the number of indexes that support parallel operation for
         * each phase.
         */
        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0 {
            pvs.nindexes_parallel_bulkdel += 1;
        }
        if (vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0 {
            pvs.nindexes_parallel_cleanup += 1;
        }
        if (vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0 {
            pvs.nindexes_parallel_condcleanup += 1;
        }
    }
    /* shm_toc_insert(toc, PARALLEL_VACUUM_KEY_INDEX_STATS, indstats) */
    dsm_store_indstats(&indstats);
    pvs.indstats = core::mem::take(&mut indstats);

    /* Prepare shared information */
    let mut shared = PVShared {
        relid: rel,
        elevel,
        queryid: vac::pgstat_get_my_query_id::call()?,
        ..Default::default()
    };
    let mwm = vac::pv_maintenance_work_mem::call()?;
    shared.maintenance_work_mem_worker = if nindexes_mwm > 0 {
        mwm / core::cmp::min(parallel_workers, nindexes_mwm)
    } else {
        mwm
    };
    shared.dead_items_info.max_bytes = vac_work_mem as usize * 1024;

    /* Prepare DSA space for dead items */
    let dead_items = vac::tid_store_create_shared_pv::call(
        shared.dead_items_info.max_bytes,
        LWTRANCHE_PARALLEL_VACUUM_DSA,
    )?;
    pvs.dead_items = dead_items;
    shared.dead_items_handle = vac::tid_store_get_handle_pv::call(dead_items)?;
    shared.dead_items_dsa_handle = vac::tid_store_get_dsa_handle_pv::call(dead_items)?;

    /* Use the same buffer size for all workers */
    shared.ring_nbuffers = vac::get_access_strategy_buffer_count::call(bstrategy)?;

    /* pg_atomic_init_u32(&(shared->cost_balance), 0); ...active_nworkers, ...idx */
    shared
        .cost_state
        .cost_balance
        .store(0, core::sync::atomic::Ordering::Relaxed);
    shared
        .cost_state
        .active_nworkers
        .store(0, core::sync::atomic::Ordering::Relaxed);
    shared.idx.store(0, core::sync::atomic::Ordering::Relaxed);

    /* shm_toc_insert(toc, PARALLEL_VACUUM_KEY_SHARED, shared) */
    dsm_store_shared(shared_snapshot(&shared));
    pvs.shared = shared;

    /*
     * Allocate space for each worker's BufferUsage and WalUsage; no need to
     * initialize.
     */
    pvs.buffer_usage = alloc::vec![BufferUsage::default(); pcxt_nworkers as usize];
    pvs.wal_usage = alloc::vec![WalUsage::default(); pcxt_nworkers as usize];
    /* The per-worker slots live in the DSM segment; the worker writes its slot
     * and the leader reads slot i to accumulate. */
    dsm_alloc_usage_slots(pcxt_nworkers as usize);

    /* Store query string for workers */
    dsm_store_query_text(debug_query);

    let dead_items_info = pvs.shared.dead_items_info;
    let dead_items = pvs.dead_items;

    /* Success -- register the state and return its handle. */
    let handle = registry_insert(pvs);

    Ok(ParallelVacuumInit {
        pvs: handle,
        dead_items,
        dead_items_info,
    })
}

/// `parallel_vacuum_end(pvs, istats)` (vacuumparallel.c:435) — destroy the
/// parallel context and end parallel mode.
///
/// In C this copies the per-index stats into the caller's `istats[]` first
/// (`istats[i]` = `pvs->indstats[i].istat` when `istat_updated`, else `NULL`).
/// The repo seam returns that array instead; the caller stores it.
/// The teardown order matches C: destroy tidstore, destroy parallel context,
/// then exit parallel mode (see the C comment about ExitParallelMode), and only
/// then drop the owned state (the `pfree(pvs)` analogue).
fn parallel_vacuum_end(
    pvs: ParallelVacuumStateHandle,
) -> PgResult<Vec<Option<IndexBulkDeleteResult>>> {
    debug_assert!(!vac::is_parallel_worker::call()?);

    // Remove the entry up front so the borrow is released before we call the
    // teardown seams (which may re-enter the registry on the error path).
    let state = REGISTRY.with(|r| r.borrow_mut().remove(&pvs.id));
    let state = match state {
        Some(s) => s,
        None => {
            return ereport(ERROR)
                .errmsg("parallel vacuum state handle is not registered")
                .finish(here("parallel_vacuum_end"))
                .map(|()| unreachable!("ereport(ERROR) does not return"));
        }
    };

    /*
     * Copy the updated statistics. For each index, hand back the DSM-resident
     * `istat` if it was updated, else `None` (the C `istats[i] = NULL`).
     */
    let mut istats: Vec<Option<IndexBulkDeleteResult>> =
        alloc::vec![None; state.nindexes as usize];
    for i in 0..state.nindexes as usize {
        let indstats = &state.indstats[i];
        if indstats.istat_updated {
            istats[i] = Some(indstats.istat);
        } else {
            istats[i] = None;
        }
    }

    if !state.dead_items.is_none() {
        vac::tid_store_destroy_pv::call(state.dead_items)?;
    }

    if let Some(pcxt) = state.pcxt {
        p::destroy_parallel_context(pcxt)?;
    }
    prt::exit_parallel_mode::call()?;

    /* `state` is dropped here — the `pfree(pvs->will_parallel_vacuum)` +
     * `pfree(pvs)` analogue. */
    drop(state);

    Ok(istats)
}

/// `parallel_vacuum_get_dead_items(pvs, &dead_items_info)`
/// (vacuumparallel.c:466).
fn parallel_vacuum_get_dead_items(
    pvs: ParallelVacuumStateHandle,
) -> PgResult<(TidStore, VacDeadItemsInfo)> {
    with_state(pvs, |state| {
        Ok((state.dead_items, state.shared.dead_items_info))
    })
}

/// `parallel_vacuum_reset_dead_items(pvs)` (vacuumparallel.c:474).
fn parallel_vacuum_reset_dead_items(pvs: ParallelVacuumStateHandle) -> PgResult<()> {
    with_state(pvs, |state| {
        let max_bytes = state.shared.dead_items_info.max_bytes;

        /*
         * Free the current tidstore and return allocated DSA segments to the OS.
         * Then recreate the tidstore with the same max_bytes limitation.
         */
        if !state.dead_items.is_none() {
            vac::tid_store_destroy_pv::call(state.dead_items)?;
        }
        let dead_items =
            vac::tid_store_create_shared_pv::call(max_bytes, LWTRANCHE_PARALLEL_VACUUM_DSA)?;
        state.dead_items = dead_items;

        /* Update the DSA pointer for dead_items to the new one */
        state.shared.dead_items_dsa_handle = vac::tid_store_get_dsa_handle_pv::call(dead_items)?;
        state.shared.dead_items_handle = vac::tid_store_get_handle_pv::call(dead_items)?;

        /* Reset the counter */
        state.shared.dead_items_info.num_items = 0;

        /* keep the DSM snapshot consistent with the new handle */
        dsm_store_shared(shared_snapshot(&state.shared));

        Ok(())
    })
}

/// `parallel_vacuum_bulkdel_all_indexes(pvs, num_table_tuples, num_index_scans)`
/// (vacuumparallel.c:499).
fn parallel_vacuum_bulkdel_all_indexes(
    pvs: ParallelVacuumStateHandle,
    num_table_tuples: f64,
    num_index_scans: i32,
) -> PgResult<()> {
    with_state(pvs, |state| {
        debug_assert!(!vac::is_parallel_worker::call()?);

        /*
         * We can only provide an approximate value of num_heap_tuples, at least
         * for now.
         */
        state.shared.reltuples = num_table_tuples;
        state.shared.estimated_count = true;

        parallel_vacuum_process_all_indexes(state, num_index_scans, true)
    })
}

/// `parallel_vacuum_cleanup_all_indexes(pvs, num_table_tuples, num_index_scans,
/// estimated_count)` (vacuumparallel.c:518).
fn parallel_vacuum_cleanup_all_indexes(
    pvs: ParallelVacuumStateHandle,
    num_table_tuples: f64,
    num_index_scans: i32,
    estimated_count: bool,
) -> PgResult<()> {
    with_state(pvs, |state| {
        debug_assert!(!vac::is_parallel_worker::call()?);

        /*
         * We can provide a better estimate of total number of surviving tuples
         * (we assume indexes are more interested in that than in the number of
         * nominally live tuples).
         */
        state.shared.reltuples = num_table_tuples;
        state.shared.estimated_count = estimated_count;

        parallel_vacuum_process_all_indexes(state, num_index_scans, false)
    })
}

// =======================================================================
// Internal functions (operate on &mut ParallelVacuumState).
// =======================================================================

/// `mul_size(s1, s2)` — the C aborts on overflow; the result is only ever a byte
/// count handed to the estimator, so saturate.
#[inline]
fn mul_size(s1: usize, s2: usize) -> usize {
    s1.saturating_mul(s2)
}

/// Build a worker-visible snapshot of `PVShared`.
fn shared_snapshot(shared: &PVShared) -> SharedSnapshot {
    SharedSnapshot {
        relid: shared.relid,
        elevel: shared.elevel,
        queryid: shared.queryid,
        maintenance_work_mem_worker: shared.maintenance_work_mem_worker,
        ring_nbuffers: shared.ring_nbuffers,
        cost_state: Arc::clone(&shared.cost_state),
        dead_items_dsa_handle: shared.dead_items_dsa_handle,
        dead_items_handle: shared.dead_items_handle,
    }
}

/// `parallel_vacuum_compute_workers(indrels, nindexes, nrequested,
/// will_parallel_vacuum)` (vacuumparallel.c:548).
//
// `unreachable_code` is allowed because parallel vacuum is gated to serial via
// an early `return Ok(0)` (see the gate comment + DESIGN_DEBT.md). The faithful
// worker-count computation is retained below the gate so it can be re-enabled
// unchanged once the cross-process DSM port lands.
#[allow(unreachable_code)]
fn parallel_vacuum_compute_workers<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    indrels: &[Oid],
    nrequested: i32,
    will_parallel_vacuum: &mut [bool],
) -> PgResult<i32> {
    let nindexes = indrels.len();
    let mut nindexes_parallel_bulkdel = 0;
    let mut nindexes_parallel_cleanup = 0;

    /*
     * We don't allow performing parallel operation in standalone backend or
     * when parallelism is disabled.
     */
    if !vac::is_under_postmaster_pv::call()? || vac::max_parallel_maintenance_workers::call()? == 0 {
        return Ok(0);
    }

    /*
     * Parallel vacuum is gated to serial. vacuumparallel.c's shared state
     * (ParallelVacuumState / the shared per-index stats DSM segment) currently
     * lives in process-private / thread-local memory that real fork(2) workers
     * cannot inherit, so launching parallel index-vacuum workers hangs the
     * leader (workers can't see the leader's shared vacuum state). VACUUM
     * output is identical serial vs parallel (parallel index vacuum is a
     * perf-only optimization that splits index work across workers; rows,
     * stats and results are unchanged), so returning 0 here — exactly as C
     * does when max_parallel_maintenance_workers = 0 or parallelism is
     * unavailable — degrades to a correct serial vacuum. Un-gate when the
     * shared state is ported to a genuine cross-process DSM carrier. See
     * DESIGN_DEBT.md ("Parallel vacuum gated to serial").
     */
    return Ok(0);

    /*
     * Compute the number of indexes that can participate in parallel vacuum.
     */
    for i in 0..nindexes {
        let indrel = vac::index_open_lock::call(mcx, indrels[i], NoLock)?;
        let vacoptions = vac::am_parallel_vacuum_options::call(&indrel)?;

        /* Skip index that is not a suitable target for parallel index vacuum */
        if vacoptions == VACUUM_OPTION_NO_PARALLEL
            || vac::relation_get_number_of_blocks_pv::call(&indrel)?
                < vac::min_parallel_index_scan_size::call()? as u32
        {
            continue;
        }

        will_parallel_vacuum[i] = true;

        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0 {
            nindexes_parallel_bulkdel += 1;
        }
        if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0)
            || ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0)
        {
            nindexes_parallel_cleanup += 1;
        }
    }

    let mut nindexes_parallel =
        core::cmp::max(nindexes_parallel_bulkdel, nindexes_parallel_cleanup);

    /* The leader process takes one index */
    nindexes_parallel -= 1;

    /* No index supports parallel vacuum */
    if nindexes_parallel <= 0 {
        return Ok(0);
    }

    /* Compute the parallel degree */
    let mut parallel_workers = if nrequested > 0 {
        core::cmp::min(nrequested, nindexes_parallel)
    } else {
        nindexes_parallel
    };

    /* Cap by max_parallel_maintenance_workers */
    parallel_workers =
        core::cmp::min(parallel_workers, vac::max_parallel_maintenance_workers::call()?);

    Ok(parallel_workers)
}

/// `parallel_vacuum_process_all_indexes(pvs, num_index_scans, vacuum)`
/// (vacuumparallel.c:610) — leader-process only.
fn parallel_vacuum_process_all_indexes(
    pvs: &mut ParallelVacuumState,
    num_index_scans: i32,
    vacuum: bool,
) -> PgResult<()> {
    debug_assert!(!vac::is_parallel_worker::call()?);

    let new_status;
    let mut nworkers;

    if vacuum {
        new_status = PVIndVacStatus::NeedBulkdelete;
        /* Determine the number of parallel workers to launch */
        nworkers = pvs.nindexes_parallel_bulkdel;
    } else {
        new_status = PVIndVacStatus::NeedCleanup;
        /* Determine the number of parallel workers to launch */
        nworkers = pvs.nindexes_parallel_cleanup;

        /* Add conditionally parallel-aware indexes if in the first time call */
        if num_index_scans == 0 {
            nworkers += pvs.nindexes_parallel_condcleanup;
        }
    }

    /* The leader process will participate */
    nworkers -= 1;

    /*
     * It is possible that parallel context is initialized with fewer workers
     * than the number of indexes that need a separate worker in the current
     * phase, so we need to consider it.
     */
    let pcxt = pvs
        .pcxt
        .ok_or_else(|| PgError::error("leader must hold a parallel context"))?;
    nworkers = core::cmp::min(nworkers, p::pcxt_nworkers(pcxt));

    /*
     * Set index vacuum status and mark whether parallel vacuum worker can
     * process it.
     */
    // Reopen each index Relation transiently (NoLock relcache recover) to read
    // its index-AM options off the real value type in `is_parallel_safe`.
    let safe_ctx = mcx::MemoryContext::new("parallel_vacuum_index_is_parallel_safe");
    let safe_mcx = safe_ctx.mcx();
    for i in 0..pvs.nindexes as usize {
        debug_assert!(pvs.indstats[i].status == PVIndVacStatus::Initial);
        let new_can_process = pvs.will_parallel_vacuum[i]
            && parallel_vacuum_index_is_parallel_safe(
                safe_mcx,
                pvs.indrels[i],
                num_index_scans,
                vacuum,
            )?;
        let indstats = &mut pvs.indstats[i];
        indstats.status = new_status;
        indstats.parallel_workers_can_process = new_can_process;
        let snapshot = *indstats;
        dsm_store_one_indstats(i, snapshot);
    }

    /* Reset the parallel index processing and progress counters */
    pvs.shared.idx.store(0, core::sync::atomic::Ordering::Relaxed);

    /* Setup the shared cost-based vacuum delay and launch workers */
    if nworkers > 0 {
        /* Reinitialize parallel context to relaunch parallel workers */
        if num_index_scans > 0 {
            p::reinitialize_parallel_dsm(pcxt)?;
        }

        /*
         * Set up shared cost balance and the number of active workers for
         * vacuum delay.  We need to do this before launching workers as
         * otherwise, they might not see the updated values for these
         * parameters.
         */
        pvs.shared.cost_state.cost_balance.store(
            vac::vacuum_cost_balance::call()? as u32,
            core::sync::atomic::Ordering::Relaxed,
        );
        pvs.shared
            .cost_state
            .active_nworkers
            .store(0, core::sync::atomic::Ordering::Relaxed);

        /* Keep the worker-visible snapshot consistent. */
        dsm_store_shared(shared_snapshot(&pvs.shared));

        /*
         * The number of workers can vary between bulkdelete and cleanup phase.
         */
        p::reinitialize_parallel_workers(pcxt, nworkers);

        p::launch_parallel_workers(pcxt)?;

        if p::pcxt_nworkers_launched(pcxt) > 0 {
            /*
             * Reset the local cost values for leader backend as we have
             * already accumulated the remaining balance of heap.
             */
            vac::set_vacuum_cost_balance::call(0)?;
            vac::set_vacuum_cost_balance_local::call(0)?;

            /* Enable shared cost balance for leader backend
             * (VacuumSharedCostBalance = &(shared->cost_balance);
             *  VacuumActiveNWorkers = &(shared->active_nworkers);) */
            vac::set_vacuum_shared_cost_balance_enable::call(Some(Arc::clone(
                &pvs.shared.cost_state,
            )))?;
            vac::set_vacuum_active_nworkers_enable::call(Some(Arc::clone(
                &pvs.shared.cost_state,
            )))?;
        }

        let nworkers_launched = p::pcxt_nworkers_launched(pcxt);
        if vacuum {
            ereport(ErrorLevel(pvs.shared.elevel))
                .errmsg(ngettext_workers_vacuuming(nworkers_launched, nworkers))
                .finish(here("parallel_vacuum_process_all_indexes"))?;
        } else {
            ereport(ErrorLevel(pvs.shared.elevel))
                .errmsg(ngettext_workers_cleanup(nworkers_launched, nworkers))
                .finish(here("parallel_vacuum_process_all_indexes"))?;
        }
    }

    /* Vacuum the indexes that can be processed by only leader process */
    parallel_vacuum_process_unsafe_indexes(pvs)?;

    /*
     * Join as a parallel worker.  The leader vacuums alone processes all
     * parallel-safe indexes in the case where no workers are launched.
     */
    parallel_vacuum_process_safe_indexes(pvs)?;

    /*
     * Next, accumulate buffer and WAL usage.  (This must wait for the workers
     * to finish, or we might get incomplete data.)
     */
    if nworkers > 0 {
        /* Wait for all vacuum workers to finish */
        p::wait_for_parallel_workers_to_finish(pcxt)?;

        for i in 0..p::pcxt_nworkers_launched(pcxt) {
            vac::instr_accum_parallel_query_pv::call(i)?;
        }
    }

    /*
     * Reset all index status back to initial (while checking that we have
     * vacuumed all indexes).
     */
    for i in 0..pvs.nindexes as usize {
        if pvs.indstats[i].status != PVIndVacStatus::Completed {
            let indname = vac::relation_get_relation_name::call(pvs.indrels[i])?;
            return ereport(ERROR)
                .errmsg(alloc::format!(
                    "parallel index vacuum on index \"{}\" is not completed",
                    indname
                ))
                .finish(here("parallel_vacuum_process_all_indexes"))
                .map(|()| ());
        }

        pvs.indstats[i].status = PVIndVacStatus::Initial;
    }

    /*
     * Carry the shared balance value to heap scan and disable shared costing.
     */
    if vac::vacuum_shared_cost_balance_is_set::call()? {
        vac::set_vacuum_cost_balance::call(vac::vacuum_shared_cost_balance_read::call()? as i32)?;
        /* VacuumSharedCostBalance = NULL; VacuumActiveNWorkers = NULL; */
        vac::set_vacuum_shared_cost_balance_enable::call(None)?;
        vac::set_vacuum_active_nworkers_enable::call(None)?;
    }

    Ok(())
}

/// `parallel_vacuum_process_safe_indexes(pvs)` (vacuumparallel.c:773) — index
/// vacuum/cleanup loop run by both the leader and worker processes.
fn parallel_vacuum_process_safe_indexes(pvs: &mut ParallelVacuumState) -> PgResult<()> {
    /*
     * Increment the active worker count if we are able to launch any worker.
     */
    if vac::vacuum_active_nworkers_is_set::call()? {
        vac::vacuum_active_nworkers_add::call(1)?;
    }

    /* Loop until all indexes are vacuumed */
    loop {
        /* Get an index number to process */
        let idx = pvs
            .shared
            .idx
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);

        /* Done for all indexes? */
        if idx >= pvs.nindexes as u32 {
            break;
        }

        /*
         * Skip vacuuming index that is unsafe for workers or has an unsuitable
         * target for parallel index vacuum (this is vacuumed in
         * parallel_vacuum_process_unsafe_indexes() by the leader).
         */
        if !pvs.indstats[idx as usize].parallel_workers_can_process {
            continue;
        }

        /* Do vacuum or cleanup of the index */
        let indrel = pvs.indrels[idx as usize];
        parallel_vacuum_process_one_index(pvs, indrel, idx as usize)?;
    }

    /*
     * We have completed the index vacuum so decrement the active worker count.
     */
    if vac::vacuum_active_nworkers_is_set::call()? {
        vac::vacuum_active_nworkers_sub::call(1)?;
    }

    Ok(())
}

/// `parallel_vacuum_process_unsafe_indexes(pvs)` (vacuumparallel.c:827) —
/// leader-only vacuuming of indexes that are not parallel safe.
fn parallel_vacuum_process_unsafe_indexes(pvs: &mut ParallelVacuumState) -> PgResult<()> {
    debug_assert!(!vac::is_parallel_worker::call()?);

    /*
     * Increment the active worker count if we are able to launch any worker.
     */
    if vac::vacuum_active_nworkers_is_set::call()? {
        vac::vacuum_active_nworkers_add::call(1)?;
    }

    for i in 0..pvs.nindexes as usize {
        /* Skip, indexes that are safe for workers */
        if pvs.indstats[i].parallel_workers_can_process {
            continue;
        }

        /* Do vacuum or cleanup of the index */
        let indrel = pvs.indrels[i];
        parallel_vacuum_process_one_index(pvs, indrel, i)?;
    }

    /*
     * We have completed the index vacuum so decrement the active worker count.
     */
    if vac::vacuum_active_nworkers_is_set::call()? {
        vac::vacuum_active_nworkers_sub::call(1)?;
    }

    Ok(())
}

/// `parallel_vacuum_process_one_index(pvs, indrel, indstats)`
/// (vacuumparallel.c:864). `idx` is the index's slot in `pvs.indstats`.
fn parallel_vacuum_process_one_index(
    pvs: &mut ParallelVacuumState,
    indrel: Oid,
    idx: usize,
) -> PgResult<()> {
    /*
     * Update the pointer to the corresponding bulk-deletion result if someone
     * has already updated it.
     */
    let istat: Option<IndexBulkDeleteResult> = if pvs.indstats[idx].istat_updated {
        Some(pvs.indstats[idx].istat)
    } else {
        None
    };

    let ivinfo = IndexVacuumInfo {
        index: indrel,
        heaprel: pvs.heaprel,
        analyze_only: false,
        report_progress: false,
        estimated_count: pvs.shared.estimated_count,
        message_level: MESSAGE_LEVEL_DEBUG2,
        num_heap_tuples: pvs.shared.reltuples,
        strategy: pvs.bstrategy.clone(),
    };
    debug_assert_eq!(ivinfo.message_level, DEBUG2.0);

    /* Update error traceback information */
    pvs.indname = Some(vac::relation_get_relation_name::call(indrel)?);
    pvs.status = pvs.indstats[idx].status;

    let istat_res = match pvs.indstats[idx].status {
        PVIndVacStatus::NeedBulkdelete => Some(vac::vac_bulkdel_one_index::call(
            ivinfo,
            istat,
            pvs.dead_items,
            pvs.shared.dead_items_info,
        )?),
        PVIndVacStatus::NeedCleanup => vac::vac_cleanup_one_index::call(ivinfo, istat)?,
        _ => {
            let indname = vac::relation_get_relation_name::call(indrel)?;
            return ereport(ERROR)
                .errmsg(alloc::format!(
                    "unexpected parallel vacuum index status {} for index \"{}\"",
                    pvs.indstats[idx].status as i32,
                    indname
                ))
                .finish(here("parallel_vacuum_process_one_index"))
                .map(|()| ());
        }
    };

    /*
     * Copy the index bulk-deletion result returned from ambulkdelete and
     * amvacuumcleanup to the shared array if it's the first cycle ...
     */
    if !pvs.indstats[idx].istat_updated {
        if let Some(res) = istat_res {
            pvs.indstats[idx].istat = res;
            pvs.indstats[idx].istat_updated = true;
            /* The seam returns an owned value, so there is no locally-allocated
             * result to `pfree`. */
        }
    }

    /*
     * Update the status to completed.  No need to lock here since each worker
     * touches different indexes.
     */
    pvs.indstats[idx].status = PVIndVacStatus::Completed;

    /* Publish the updated slot back to the shared array. */
    let snapshot = pvs.indstats[idx];
    dsm_store_one_indstats(idx, snapshot);

    /* Reset error traceback information */
    pvs.status = PVIndVacStatus::Completed;
    pvs.indname = None;

    /*
     * Call the parallel variant of pgstat_progress_incr_param so workers can
     * report progress of index vacuum to the leader.
     */
    vac::pgstat_progress_parallel_incr_param::call(PROGRESS_VACUUM_INDEXES_PROCESSED, 1)?;

    Ok(())
}

/// `parallel_vacuum_index_is_parallel_safe(indrel, num_index_scans, vacuum)`
/// (vacuumparallel.c:950).
fn parallel_vacuum_index_is_parallel_safe<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    indrel: Oid,
    num_index_scans: i32,
    vacuum: bool,
) -> PgResult<bool> {
    let indrel = vac::index_open_lock::call(mcx, indrel, NoLock)?;
    let vacoptions = vac::am_parallel_vacuum_options::call(&indrel)?;

    /* In parallel vacuum case, check if it supports parallel bulk-deletion */
    if vacuum {
        return Ok((vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0);
    }

    /* Not safe, if the index does not support parallel cleanup */
    if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0)
        && ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0)
    {
        return Ok(false);
    }

    /*
     * Not safe, if the index supports parallel cleanup conditionally, but we
     * have already processed the index (for bulkdelete).
     */
    if num_index_scans > 0 && ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0) {
        return Ok(false);
    }

    Ok(true)
}

/// `parallel_vacuum_main(seg, toc)` (vacuumparallel.c:988) — the parallel-worker
/// entry point. Attaches to the shared state, opens table+indexes, runs the
/// safe-index processing loop, reports usage, then detaches.
pub fn parallel_vacuum_main() -> PgResult<()> {
    /*
     * A parallel vacuum worker must have only PROC_IN_VACUUM flag since we
     * don't support parallel vacuum for autovacuum as of now.
     */
    debug_assert!(vac::my_proc_in_vacuum_only::call()?);

    ereport(DEBUG1)
        .errmsg("starting parallel vacuum worker")
        .finish(here("parallel_vacuum_main"))?;

    let shared = dsm_lookup_shared();

    /* Set debug_query_string for individual workers */
    let sharedquery = dsm_lookup_query_text();
    vac::set_debug_query_string_pv::call(sharedquery.clone())?;
    vac::pgstat_report_activity_running_pv::call(sharedquery.unwrap_or_default())?;

    /* Track query ID */
    vac::pgstat_report_query_id_pv::call(shared.queryid, false)?;

    /*
     * The parallel-vacuum worker runs inside its own transaction (the parallel
     * bootstrap started one before dispatching here), so it has its own memory
     * context / arena. Reopen the heap + indexes as owned `Relation<'mcx>`
     * allocated in that arena — held live for the whole worker call, exactly as
     * C holds `rel` open from `table_open` to `table_close` (vacuumparallel.c:
     * 1026..1108).
     */
    let ctx = mcx::MemoryContext::new("parallel_vacuum_main");
    let mcx = ctx.mcx();

    /*
     * Open table.  The lock mode is the same as the leader process.
     */
    let rel = vac::table_open_lock::call(mcx, shared.relid, ShareUpdateExclusiveLock)?;

    /*
     * Open all indexes. indrels are sorted in order by OID, matching the
     * leader's.
     */
    let indrels = vac::vac_open_indexes_lock::call(rel.rd_id, RowExclusiveLock)?;
    let nindexes = indrels.len() as i32;
    debug_assert!(nindexes > 0);

    /*
     * Apply the desired value of maintenance_work_mem within this process.
     */
    if shared.maintenance_work_mem_worker > 0 {
        vac::set_pv_maintenance_work_mem::call(shared.maintenance_work_mem_worker)?;
    }

    /* Set index statistics */
    let indstats = dsm_lookup_indstats();

    /* Find dead_items in shared memory */
    let dead_items =
        vac::tid_store_attach_pv::call(shared.dead_items_dsa_handle, shared.dead_items_handle)?;

    /* Set cost-based vacuum delay */
    vac::vacuum_update_costs::call()?;
    vac::set_vacuum_cost_balance::call(0)?;
    vac::set_vacuum_cost_balance_local::call(0)?;
    /* VacuumSharedCostBalance = &(shared->cost_balance);
     * VacuumActiveNWorkers = &(shared->active_nworkers);
     * The worker attached to the same shared cell, so it points the globals at
     * the leader's atomics. */
    vac::set_vacuum_shared_cost_balance_enable::call(Some(Arc::clone(&shared.cost_state)))?;
    vac::set_vacuum_active_nworkers_enable::call(Some(Arc::clone(&shared.cost_state)))?;

    /* Set parallel vacuum state */
    let relnamespace = vac::relation_get_namespace_name_pv::call(&rel)?;
    let relname = vac::relation_get_relation_name::call(rel.rd_id)?;
    let heaprel_oid = rel.rd_id;
    let mut pvs = ParallelVacuumState {
        indrels,
        nindexes,
        indstats,
        shared: PVShared {
            relid: shared.relid,
            elevel: shared.elevel,
            queryid: shared.queryid,
            reltuples: 0.0,
            estimated_count: false,
            maintenance_work_mem_worker: shared.maintenance_work_mem_worker,
            ring_nbuffers: shared.ring_nbuffers,
            cost_state: Arc::clone(&shared.cost_state),
            idx: core::sync::atomic::AtomicU32::new(0),
            dead_items_dsa_handle: shared.dead_items_dsa_handle,
            dead_items_handle: shared.dead_items_handle,
            dead_items_info: VacDeadItemsInfo::default(),
        },
        dead_items,
        relnamespace: Some(relnamespace),
        relname: Some(relname),
        heaprel: heaprel_oid,
        /* These fields will be filled during index vacuum or cleanup */
        indname: None,
        status: PVIndVacStatus::Initial,
        ..Default::default()
    };

    /* Each parallel VACUUM worker gets its own access strategy. */
    pvs.bstrategy =
        vac::get_access_strategy_with_size_basvac::call(pvs.shared.ring_nbuffers * (BLCKSZ / 1024))?;

    /* Setup error traceback support for ereport() */
    vac::push_parallel_vacuum_error_context::call()?;

    /* Prepare to track buffer usage during parallel execution */
    vac::instr_start_parallel_query_pv::call()?;

    /* Process indexes to perform vacuum/cleanup */
    parallel_vacuum_process_safe_indexes(&mut pvs)?;

    /* Report buffer/WAL usage during parallel execution */
    let pwn = p::parallel_worker_number();
    vac::instr_end_parallel_query_pv::call(pwn)?;

    /* Report any remaining cost-based vacuum delay time */
    if vac::track_cost_delay_timing::call()? {
        vac::pgstat_progress_parallel_incr_param::call(
            PROGRESS_VACUUM_DELAY_TIME,
            vac::parallel_vacuum_worker_delay_ns::call()?,
        )?;
    }

    vac::tid_store_detach_pv::call(dead_items)?;

    /* Pop the error context stack */
    vac::pop_parallel_vacuum_error_context::call()?;

    vac::vac_close_indexes_lock::call(pvs.indrels.clone(), RowExclusiveLock)?;
    vac::table_close_lock::call(rel, ShareUpdateExclusiveLock)?;
    vac::free_access_strategy_pv::call(core::mem::take(&mut pvs.bstrategy))?;

    /* The worker's relation arena is released with the worker transaction. */
    drop(ctx);

    Ok(())
}

/// `parallel_vacuum_error_callback(arg)` (vacuumparallel.c:1118) — error context
/// callback for errors during parallel index vacuum. Returns the `errcontext`
/// message to append, or `None` for the initial/completed states.
fn parallel_vacuum_error_callback(errinfo: &ParallelVacuumState) -> Option<String> {
    let indname = errinfo.indname.as_deref().unwrap_or("");
    let relnamespace = errinfo.relnamespace.as_deref().unwrap_or("");
    let relname = errinfo.relname.as_deref().unwrap_or("");

    match errinfo.status {
        PVIndVacStatus::NeedBulkdelete => Some(alloc::format!(
            "while vacuuming index \"{indname}\" of relation \"{relnamespace}.{relname}\""
        )),
        PVIndVacStatus::NeedCleanup => Some(alloc::format!(
            "while cleaning up index \"{indname}\" of relation \"{relnamespace}.{relname}\""
        )),
        PVIndVacStatus::Initial | PVIndVacStatus::Completed => None,
    }
}

// =======================================================================
// small helpers
// =======================================================================

/// `ngettext("launched %d parallel vacuum worker for index vacuuming (planned:
/// %d)", "...workers...", n)` — singular/plural chosen on `nworkers_launched`,
/// exactly as the C `ngettext` call does.
fn ngettext_workers_vacuuming(nworkers_launched: i32, planned: i32) -> String {
    if nworkers_launched == 1 {
        alloc::format!(
            "launched {nworkers_launched} parallel vacuum worker for index vacuuming (planned: {planned})"
        )
    } else {
        alloc::format!(
            "launched {nworkers_launched} parallel vacuum workers for index vacuuming (planned: {planned})"
        )
    }
}

/// As [`ngettext_workers_vacuuming`] but for the index-cleanup phase.
fn ngettext_workers_cleanup(nworkers_launched: i32, planned: i32) -> String {
    if nworkers_launched == 1 {
        alloc::format!(
            "launched {nworkers_launched} parallel vacuum worker for index cleanup (planned: {planned})"
        )
    } else {
        alloc::format!(
            "launched {nworkers_launched} parallel vacuum workers for index cleanup (planned: {planned})"
        )
    }
}

// =======================================================================
// Seam wiring.
// =======================================================================

/// Install this crate's inward consumer-contract seams (declared in
/// `backend-access-heap-vacuumlazy-seams`). The worker entry point
/// [`parallel_vacuum_main`] is a plain public fn (the parallel-infra bgworker
/// dispatch will reach it directly), and `parallel_vacuum_error_callback` is
/// reached through the error-context machinery the worker pushes.
pub fn init_seams() {
    vl::parallel_vacuum_init::set(parallel_vacuum_init);
    vl::parallel_vacuum_end::set(parallel_vacuum_end);
    vl::parallel_vacuum_get_dead_items::set(parallel_vacuum_get_dead_items);
    vl::parallel_vacuum_reset_dead_items::set(parallel_vacuum_reset_dead_items);
    vl::parallel_vacuum_bulkdel_all_indexes::set(parallel_vacuum_bulkdel_all_indexes);
    vl::parallel_vacuum_cleanup_all_indexes::set(parallel_vacuum_cleanup_all_indexes);

    install_pv_outward_seams();

    // `parallel_vacuum_error_callback` is referenced by the error-context path;
    // keep it live for the linker until that wiring lands.
    let _ = parallel_vacuum_error_callback as fn(&ParallelVacuumState) -> Option<String>;
}

/// Install the vacuumparallel.c `*_pv` outward seams (declared in the shared
/// `backend-commands-vacuum-seams` hub, `::call`ed only by this crate) whose
/// owning subsystem has a real value-typed provider. Each wrapper is a thin
/// marshal + delegate. The remaining `*_pv` seams stay seam-and-panic until
/// their keystone lands (the #4 by-OID heap relation reopen); see the
/// allowlist note in seams-init.
fn install_pv_outward_seams() {
    // --- tidstore.c shared TID store (radixtree-backed) ---------------------
    vac::tid_store_create_shared_pv::set(tidstore::TidStoreCreateShared);
    vac::tid_store_get_handle_pv::set(|ts| tidstore::TidStoreGetHandle(&ts));
    vac::tid_store_get_dsa_handle_pv::set(|ts| tidstore::TidStoreGetDSA(&ts));
    vac::tid_store_attach_pv::set(tidstore::TidStoreAttach);
    vac::tid_store_destroy_pv::set(|ts| tidstore::TidStoreDestroy(&ts));
    vac::tid_store_detach_pv::set(|ts| tidstore::TidStoreDetach(&ts));

    // --- per-worker DSM instrument usage slots (instrument.c) ---------------
    // InstrStartParallelQuery saves into a backend-local snapshot (C's file-local
    // save_pgBufferUsage); InstrEndParallelQuery writes the worker's deltas into
    // its DSM slot; InstrAccumParallelQuery folds slot i into the leader's stats.
    vac::instr_start_parallel_query_pv::set(instr_start_parallel_query_pv_impl);
    vac::instr_end_parallel_query_pv::set(instr_end_parallel_query_pv_impl);
    vac::instr_accum_parallel_query_pv::set(instr_accum_parallel_query_pv_impl);

    // --- tcop debug_query_string -------------------------------------------
    vac::debug_query_string_pv::set(|| Ok(tcop::debug_query_string().map(String::from)));
    vac::set_debug_query_string_pv::set(set_debug_query_string_pv_impl);

    // --- miscinit IsUnderPostmaster ----------------------------------------
    vac::is_under_postmaster_pv::set(|| Ok(initglobals::IsUnderPostmaster()));

    // --- pgstat / backend_status -------------------------------------------
    vac::pgstat_get_my_query_id::set(|| Ok(activity_status::pgstat_get_my_query_id()));
    vac::pgstat_report_query_id_pv::set(|queryid, force| {
        activity_status::pgstat_report_query_id(queryid, force);
        Ok(())
    });
    vac::pgstat_report_activity_running_pv::set(pgstat_report_activity_running_pv_impl);
    vac::pgstat_progress_parallel_incr_param::set(pgstat_progress_parallel_incr_param_impl);

    // --- freelist.c worker buffer-access strategy --------------------------
    // `GetAccessStrategyWithSize(BAS_VACUUM, ring_size_kb)` — each parallel
    // worker creates its OWN ring sized from the DSM-shared `ring_nbuffers`.
    vac::get_access_strategy_with_size_basvac::set(|ring_size_kb| {
        buffer_support::get_access_strategy_with_size(
            types_storage::buf::BufferAccessStrategyType::BasVacuum,
            ring_size_kb,
        )
    });
    // `FreeAccessStrategy(strategy)` — drop the worker's ring (NULL is a no-op).
    vac::free_access_strategy_pv::set(|strategy| {
        buffer_support::free_access_strategy(strategy);
        Ok(())
    });

    // --- freelist.c leader/serial buffer-access strategy -------------------
    // `GetAccessStrategyWithSize(BAS_VACUUM, ring_size_kb)` — the leader/serial
    // VACUUM (`ExecVacuum`) creates the one shared `vac_strategy` ring.
    vac::get_access_strategy_with_size::set(|ring_size_kb| {
        buffer_support::get_access_strategy_with_size(
            types_storage::buf::BufferAccessStrategyType::BasVacuum,
            ring_size_kb,
        )
    });
    // `GetAccessStrategyBufferCount(bstrategy)` — number of buffers in the
    // leader's inherited ring (`0` for the NULL strategy), used by
    // `parallel_vacuum_init` to seed the DSM-shared `ring_nbuffers`.
    vac::get_access_strategy_buffer_count::set(|strategy| {
        Ok(buffer_support::get_access_strategy_buffer_count(&strategy))
    });

    // --- proc.c MyProc->statusFlags PROC_IN_VACUUM -------------------------
    // `MyProc->statusFlags |= PROC_IN_VACUUM [| PROC_VACUUM_FOR_WRAPAROUND]`
    // (vacuum.c:2066) and the `MyProc->statusFlags == PROC_IN_VACUUM` worker
    // assert (vacuumparallel.c:1007).
    vac::set_proc_in_vacuum_flags::set(proc_misc::set_my_proc_in_vacuum_flags);
    vac::my_proc_in_vacuum_only::set(|| Ok(proc_misc::my_proc_status_flags_is_in_vacuum_only()));

    // --- parallel-vacuum error-context callback ----------------------------
    // C pushes `parallel_vacuum_error_callback` onto `error_context_stack` for
    // the duration of `parallel_vacuum_process_safe_indexes`. This tree retires
    // the ambient `error_context_stack` chain (docs/query-lifecycle-raii.md):
    // error context attaches on propagation rather than via an ambient callback
    // walk, so installing/removing the callback is a faithful no-op. The
    // callback body itself is `parallel_vacuum_error_callback` (below), driven
    // off `pvs.{status,indname,relnamespace,relname}` which the index loop keeps
    // current exactly as C does.
    vac::push_parallel_vacuum_error_context::set(|| Ok(()));
    vac::pop_parallel_vacuum_error_context::set(|| Ok(()));

    // --- by-OID heap/index reopen -> real value-typed Relation accessors -----
    // The parallel-vacuum worker (and the leader transiently, when reading an
    // index's AM options) reopens the heap + indexes BY OID as owned
    // `Relation<'mcx>`s allocated in the caller's transaction arena. These 6
    // seams used to carry Oid tokens; they now carry the real value types,
    // delegating to the canonical table-AM / index-AM / relcache providers.

    // `table_open(relid, lockmode)` — recover the owned `Relation` from the
    // relcache (the worker takes the real lock; the leader passes NoLock to
    // recover an already-locked index without re-locking), exactly the
    // open-then-recover idiom the heap/vac_open_indexes path uses.
    vac::table_open_lock::set(|mcx, relid, lockmode| {
        table_seam::table_open::call(mcx, relid, lockmode)
    });
    // `table_close(rel, lockmode)` — drop the relcache reference; the lock is
    // held until commit, so `Relation::close` releases the entry.
    vac::table_close_lock::set(|rel, lockmode| rel.close(lockmode));

    // `index_open(indexoid, lockmode)` — recover an already-locked index
    // `Relation` from the relcache (NoLock; the leader holds RowExclusiveLock).
    // The C code keeps the open `Relation *indrels[]`; here the index is carried
    // as an OID and recovered through the index-AM `index_open`, which validates
    // the index relkind (the table-AM `table_open` would reject it).
    vac::index_open_lock::set(|mcx, indexoid, lockmode| {
        indexam::index_open::call(mcx, indexoid, lockmode)
    });

    // `indrel->rd_indam->amparallelvacuumoptions` / `->amusemaintenanceworkmem`
    // — resolve the index AM routine from the index's `rd_rel->relam` and read
    // the flag off it.
    vac::am_parallel_vacuum_options::set(|indrel| {
        Ok(amapi::get_index_am_routine_by_amid::call(indrel.rd_rel.relam)?.amparallelvacuumoptions)
    });
    vac::am_use_maintenance_work_mem::set(|indrel| {
        Ok(amapi::get_index_am_routine_by_amid::call(indrel.rd_rel.relam)?.amusemaintenanceworkmem)
    });

    // `RelationGetNumberOfBlocks(indrel)` — the relcache/smgr block count.
    vac::relation_get_number_of_blocks_pv::set(|rel| {
        relcache_sx::relation_get_number_of_blocks::call(rel)
    });

    // `get_namespace_name(RelationGetNamespace(rel))` — the schema name for the
    // worker's reopened heap relation (via the lsyscache seam already on the
    // vacuumlazy hub).
    vac::relation_get_namespace_name_pv::set(|rel| {
        vl::get_namespace_name::call(rel.rd_rel.relnamespace)
    });
}

/// `debug_query_string = sharedquery` (vacuumparallel.c:1015). The worker copies
/// the leader's query text into a backend-lifetime allocation; C points
/// `debug_query_string` at the DSM-resident string for the worker's life. Mirror
/// that backend-lifetime ownership with a leak — the worker process exits when
/// the query ends, so this is freed at process teardown exactly as in C.
fn set_debug_query_string_pv_impl(s: Option<String>) -> PgResult<()> {
    let leaked: Option<&'static str> = s.map(|q| &*alloc::boxed::Box::leak(q.into_boxed_str()));
    tcop::set_debug_query_string(leaked);
    Ok(())
}

/// `pgstat_report_activity(STATE_RUNNING, query)` (vacuumparallel.c:1016).
fn pgstat_report_activity_running_pv_impl(query: String) -> PgResult<()> {
    activity_status::pgstat_report_activity(
        activity_status::STATE_RUNNING,
        Some(query.as_bytes()),
    );
    Ok(())
}

/// `pgstat_progress_parallel_incr_param(index, incr)` (vacuumparallel.c:943,1099).
/// The owner builds the worker→leader libpq Progress message in the caller's
/// memory context; the inward seam contract carries no `Mcx`, so use a private
/// crate context for the duration of the call (the same idiom this crate uses in
/// `parallel_vacuum_init`).
fn pgstat_progress_parallel_incr_param_impl(index: i32, incr: i64) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("pgstat_progress_parallel_incr_param");
    activity_progress::pgstat_progress_parallel_incr_param(ctx.mcx(), index, incr)
}
