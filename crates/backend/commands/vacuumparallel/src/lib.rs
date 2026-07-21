//! vacuumparallel.c: parallel index vacuum/cleanup. Thread-native per
//! docs/parallel-query-design.md — PVShared is an Arc of typed fields (no
//! shm_toc), dead items cross as an Arc<[ItemPointerData]> snapshot of the
//! leader's flat tid vec (C: shared TidStore in DSA), and each worker opens
//! its own relations. Divergences recorded in CATALOG: buffer/WAL usage
//! transfer and progress reporting skipped (consumers elided repo-wide),
//! queryid/debug_query_string not forwarded, error-context callback elided.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering::SeqCst,
};
use std::sync::{Arc, Mutex, OnceLock};

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
use ::types_error::{PgError, PgResult, ERROR};
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
    /// M4.1 pool channel: the per-pass payload slot (the pool driver reads
    /// it; cleared by the leader at every pass end) plus the leader-flushed
    /// progress relays — a pool serve has no launched-worker progress
    /// sender, so workers count here and the LEADER (owner of the
    /// pg_stat_progress_vacuum row) publishes.
    pool_pass: Mutex<Option<Arc<PvPoolPass>>>,
    pool_indexes_processed: AtomicI64,
    pool_delay_ns: AtomicI64,
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
    /// M4.1: binder target + standing driver installed on the pcxt at init
    /// — index passes may engage the pool channel (pool_engage_pass).
    pool_armed: bool,
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
        pool_pass: Mutex::new(None),
        pool_indexes_processed: AtomicI64::new(0),
        pool_delay_ns: AtomicI64::new(0),
    });
    parallel::set_private(pcxt, Arc::clone(&shared) as Arc<dyn std::any::Any + Send + Sync>);

    // M4.1 pool channel (arm once per vacuum; passes engage per pass):
    // binder target + standing driver on THIS pcxt. Fail-closed: any
    // refusal here leaves the launched bgworker gang the driver of record,
    // byte-exactly.
    let mut pool_armed = false;
    if pool_index_enabled() {
        let policy = parallel::query_task_policy_probe();
        if !(policy.temp_state || policy.serializable || policy.pending_invalidations)
            && parallel::InstallQueryTaskBinding(pcxt, policy).is_ok()
        {
            parallel::set_standing_driver(
                pcxt,
                parallel::standing::StandingDriver {
                    drive: parallel_vacuum_pool_driver,
                    deferred_bind: false,
                },
            );
            ensure_pool_shutdown_registered();
            pool_armed = true;
        } else {
            ptrace("pool channel refused (binder policy probe)");
        }
    }

    Ok(Some(ParallelVacuumState {
        pcxt,
        shared,
        will_parallel_vacuum,
        nindexes,
        nindexes_parallel_bulkdel,
        nindexes_parallel_cleanup,
        nindexes_parallel_condcleanup,
        pool_armed,
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

// ---------------------------------------------------------------------------
// M4.1 pool channel (driver swap, index phase): index passes served by
// PGPROC-leasing pool workers through the M2 inc-2 bound-descriptor gate at
// QoS class Utility. The launched bgworker gang stays the fallback on every
// refusal, and the flipped-kill switch (pgrust.runtime_vacuum_pool, default
// ON since the train-40 flip) restores it wholesale when set off.
//
// Q2 (Track 4.2 long-unit discipline) — CHUNKED-CLAIM RESUMABLE SWEEPS:
// the M4.1 "one index = one coarse morsel" unit held a claim (and its
// execution permit un-preemptibly) for a whole index sweep, so abort/
// cancel/QoS-reclaim latency on this task set was bounded by one INDEX,
// not the ~t_max sizer envelope. The chunks arm (default ON under the pool
// switch; PGRUST_RUNTIME_VACUUM_INDEX_CHUNKS=0 restores the coarse arm)
// replaces it with a TICKET STREAM: granule g of a StreamSource is the
// ticket "advance unit tickets[g] by one quantum (~t_max of leaf blocks)";
// the resumable scan state (nbtree::BtVacChunkedScan) lives in the unit,
// so successive quanta may be served by DIFFERENT workers. SERIAL PER
// INDEX BY CONSTRUCTION: a unit's successor ticket is published only by
// the completer of its previous quantum — at most one claim per index is
// ever in flight (no locks to wait on, no permit burned idle; unit.busy is
// the overlap ASSERTION, failing closed). Every scheduler discipline now
// applies at quantum cadence: abort observed, ledger Yield honored, stride
// charged, permit parked — the reclaim bound the QoS design assumes.
// INTRA-INDEX PARALLELISM ADJUDICATED OUT (see nbtree::vacuum's chunk
// banner): the backtrack/cycle-id envelope and page-deletion bookkeeping
// are single-scan-instance state; resumability is the honest increment.
// LEADER PARTICIPATION (M4.1 debt #2, retired on the chunks arm): once the
// pool channel is live the leader joins the ticket work through the
// bounded drive (runtime::drive_pinned_tasks, ≤1 task per burst), keeping
// its CFI/cancel cadence at ~t_max independent of index size — the park
// loop's cancel guarantee, preserved. On the coarse arm it still parks.
// RESIDUAL (recorded): non-btree AMs have no chunked sweep — their units
// run whole inside a single ticket (the M4.1 coarse shape, per index).
// ---------------------------------------------------------------------------

/// M4.1 driver switch for the parallel-index passes (same switch as the
/// heap phase's driver swap). FLIPPED-KILL: default ON since the train-40
/// flip (GL-M41-3); `pgrust.runtime_vacuum_pool = off` (env seed
/// PGRUST_RUNTIME_VACUUM_POOL=0|off) restores the launched gang. Layered
/// under the runtime master switch and the standing module's own gates
/// (POOLBIND/POOLDB inside try_engage_pool).
fn pool_index_enabled() -> bool {
    guc_tables::backing::pgrust_runtime_vacuum_pool()
        && runtime::runtime_enabled()
        && runtime::global().is_some()
        && miscinit::GetMyBackendType() != ::types_core::BackendType::AutovacWorker
        && !parallel::IsParallelWorker()
}

/// Q2 kill switch (Track 4.2), layered UNDER PGRUST_RUNTIME_VACUUM_POOL —
/// consulted only when the pool channel engages, so the launched/serial
/// channels never see it. Default ON under the pool arm (the chunked shape
/// IS Q2's deliverable); `PGRUST_RUNTIME_VACUUM_INDEX_CHUNKS=0` restores
/// M4.1's one-index-per-claim coarse morsels exactly.
fn pool_index_chunks_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_CHUNKS").is_ok_and(|v| v.trim() == "0")
    })
}

/// Chunk quantum: index blocks advanced per claimed ticket. Sized so one
/// quantum ≈ the sizer envelope (~t_max of leaf-page work) — the claim
/// cadence IS the abort/QoS-reclaim cadence. Calibration knob, not product
/// surface (the PGRUST_RUNTIME_STRIDE precedent).
fn pool_index_quantum() -> u32 {
    static Q: OnceLock<u32> = OnceLock::new();
    *Q.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_QUANTUM")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|q| *q > 0)
            .unwrap_or(256)
    })
}

/// Q2 leader participation (retires M4.1 debt #2). Chunks-arm only — the
/// coarse arm's leader must keep parking (a whole-index burst would break
/// the park loop's cancel cadence). Default ON.
fn pool_index_leader_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_LEADER").is_ok_and(|v| v.trim() == "0")
    })
}

/// Engagement trace (PGRUST_VACUUM_TRACE=1, the vacuum battery's oracle
/// channel). Default-off, zero cost.
fn ptrace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_VACUUM_TRACE").is_ok_and(|v| v.trim() == "1"))
}

/// Claim-cadence witness channel (PGRUST_VACUUM_CLAIM_TIMING=1): per-claim
/// service time on the index-pass pool arms — the LOCAL exhibit for the Q2
/// reclaim bound (a claim's service time bounds how long a permit is held
/// un-preemptibly; the fleet letter's probe is the reportable number).
/// Observability only, pg_clock (DST P2), default-off zero cost.
fn claim_timing_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_VACUUM_CLAIM_TIMING").is_ok_and(|v| v.trim() == "1")
    })
}

fn ptrace(msg: &str) {
    if ptrace_enabled() {
        eprintln!("vacuum-pool-index: {msg}");
    }
}

/// One pool-engaged index pass (bulkdel or cleanup): participation board +
/// the task-set work. Granule g ⇒ the pass's g-th parallel-safe index.
struct PvPoolPass {
    shared: Arc<PvShared>,
    /// The binder target (shared_for(pcxt)) — the driver binds eagerly
    /// through it.
    target: Arc<parallel::ParallelShared>,
    rg: OnceLock<runtime::WeakRgHandle>,
    /// Board-entry slot, held across the leader join so the
    /// PRIVATE_SHUTDOWN hook can complete the standing join on leader
    /// unwinds (the shutdown_standing_join law).
    board: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    /// Ordinals (into indrels/indstats) of this pass's parallel-safe
    /// indexes. Coarse arm: granule g ⇒ safe[g]. Chunks arm: unit u works
    /// index safe[u]; granules are stream tickets (see `tickets`).
    safe: Vec<usize>,
    /// Q2 chunks arm (empty/None on the coarse arm): one unit per
    /// parallel-safe index; stream granule g is the ticket "advance unit
    /// tickets[g] by one quantum". Serial-per-index by construction — a
    /// unit's successor ticket is published only by the completer of its
    /// previous quantum.
    units: Vec<PvUnit>,
    /// Granule → unit ordinal; appended under the lock BEFORE the stream
    /// watermark advances, so a claimed granule always resolves.
    tickets: Mutex<Vec<u32>>,
    stream: Option<Arc<runtime::StreamSource>>,
    /// Units not yet Completed; the LAST completion closes the stream.
    units_left: AtomicUsize,
    started: AtomicUsize,
    refused: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
}

/// One chunks-arm index unit: the resumable sweep state between tickets.
struct PvUnit {
    /// Ordinal into indrels/indstats.
    ord: usize,
    /// Overlap ASSERTION for the serial-per-index invariant (the mechanism
    /// is publish-after-work; this witnesses it and fails closed).
    busy: AtomicBool,
    run: Mutex<PvUnitRun>,
    /// GL-M41-2 width census (trace-gated, observability only): first-quantum
    /// mono_ms, quanta served by pool workers vs the participating leader,
    /// and busy time actually spent inside quanta (vs. inter-ticket gaps).
    census_t0_ms: OnceLock<i64>,
    census_quanta_pool: AtomicU64,
    census_quanta_leader: AtomicU64,
    census_busy_ns: AtomicU64,
}

enum PvUnitRun {
    /// First ticket not yet served.
    Pending,
    /// Resumable btree sweeps (nbtree owns the state; Drop releases the
    /// cycle slot on abandoned bulkdelete scans).
    Bulkdelete(nbtree::BtVacChunkedScan),
    Cleanup(nbtree::BtVacChunkedScan),
    Done,
}

impl PvPoolPass {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, SeqCst);
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

/// One-index-per-claim source: every granule is its own hard boundary and
/// claims run whole-boundary, so a claim is exactly one index regardless of
/// sizer ramp state (the "coarse per-index morsel" of record).
struct IndexGranuleSource {
    total: u64,
}

impl runtime::MorselSource for IndexGranuleSource {
    fn total_granules(&self) -> u64 {
        self.total
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        start + 1
    }

    fn startup_c0(&self) -> u64 {
        1
    }

    fn whole_boundary_claims(&self) -> bool {
        true
    }
}

/// Per-participant index-pass context: the participant's OWN opened
/// relations + strategy, published for run_morsel through a thread-local
/// pointer (the heap phase's WORKER_CX pattern — this thread is the only
/// driver of its lane).
struct PvWorkerCx<'a, 'mcx> {
    mcx: Mcx<'a>,
    heaprel: &'a RelationData<'mcx>,
    indrels: &'a [Relation<'mcx>],
    bstrategy: &'a BufferAccessStrategy,
}

thread_local! {
    static POOL_CX: std::cell::Cell<*mut PvWorkerCx<'static, 'static>> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

impl runtime::TaskSetWork for PvPoolPass {
    fn run_morsel(&self, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(SeqCst) {
            // Already aborting: the claim drains without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => self.fail(
                PgError::new(ERROR, "parallel index vacuum worker panicked in a morsel")
                    .into(),
            ),
        }
    }

    fn finalize(&self) {
        // Index stats land in PvShared.indstats under their own locks; the
        // leader verifies Completed statuses after the join.
    }
}

impl PvPoolPass {
    fn morsel_body(&self, range: runtime::MorselRange) -> PgResult<()> {
        let p = POOL_CX.with(|c| c.get());
        if p.is_null() {
            return Err(
                PgError::new(ERROR, "index vacuum morsel without a bound worker").into()
            );
        }
        // SAFETY: set by THIS thread's drive frame; the frame outlives the
        // drive, and run_morsel only executes on the claiming thread.
        let cx: &mut PvWorkerCx<'_, '_> = unsafe { &mut *p };
        for g in range {
            if self.failed.load(SeqCst) {
                return Ok(());
            }
            // Both arms claim whole single granules (one ticket / one
            // index), so per-granule service time IS per-claim service
            // time — the un-preemptible permit-hold the witness bounds.
            let t0 = claim_timing_enabled().then(pg_clock::MonoStamp::now);
            if self.stream.is_some() {
                // Q2 chunks arm: granule = one quantum ticket.
                self.chunk_ticket(cx, g)?;
            } else {
                // Coarse arm (PGRUST_RUNTIME_VACUUM_INDEX_CHUNKS=0): the
                // M4.1 one-index-per-claim unit, byte-exactly.
                let i = self.safe[g as usize];
                parallel_vacuum_process_one_index(
                    &self.shared,
                    cx.mcx,
                    cx.heaprel,
                    &cx.indrels[i],
                    i,
                    cx.bstrategy,
                )?;
            }
            if let Some(t0) = t0 {
                eprintln!("vacuum-pool-index: claim_us={}", t0.elapsed_ns() / 1000);
            }
        }
        Ok(())
    }

    /// Serve one stream ticket: advance the ticket's unit by one quantum,
    /// then publish the successor (publish-after-work = the serial-per-
    /// index mechanism) or complete the unit (the LAST completion closes
    /// the stream). The wake after publish/close is the StreamSource
    /// producer contract.
    fn chunk_ticket(&self, cx: &PvWorkerCx<'_, '_>, g: u64) -> PgResult<()> {
        let u = {
            let t = self.tickets.lock().unwrap_or_else(|p| p.into_inner());
            t[g as usize] as usize
        };
        let unit = &self.units[u];
        if unit.busy.swap(true, SeqCst) {
            debug_assert!(false, "overlapping claims on one index-vacuum unit");
            return Err(PgError::new(
                ERROR,
                "index-vacuum chunk protocol violated (overlapping claims on one index)",
            )
            .into());
        }
        // GL-M41-2 width census (trace-gated): quanta per server class +
        // in-quantum busy time; gaps between tickets show up as
        // (end - t0) - busy in the unit-done line.
        let census = ptrace_enabled().then(|| {
            unit.census_t0_ms.get_or_init(pg_clock::mono_ms);
            pg_clock::MonoStamp::now()
        });
        let r = self.unit_quantum(cx, unit);
        if let Some(t0) = census {
            unit.census_busy_ns.fetch_add(t0.elapsed_ns(), SeqCst);
            if parallel::standing::serving_on_pool() {
                unit.census_quanta_pool.fetch_add(1, SeqCst);
            } else {
                unit.census_quanta_leader.fetch_add(1, SeqCst);
            }
        }
        unit.busy.store(false, SeqCst);
        let more = r?;
        let stream = self.stream.as_ref().expect("chunk ticket on the coarse arm");
        if more {
            let n = {
                let mut t = self.tickets.lock().unwrap_or_else(|p| p.into_inner());
                t.push(u as u32);
                t.len() as u64
            };
            stream.publish(n);
        } else if self.units_left.fetch_sub(1, SeqCst) == 1 {
            stream.close();
        } else {
            return Ok(());
        }
        if let Some(rt) = runtime::global() {
            rt.notify_source_progress();
        }
        // GL-M41-2 fix (publish wake, the serve-start wake's other half): a
        // participating leader that found no claimable ticket parks on its
        // latch — notify_source_progress wakes only pool workers. Hand a
        // pool worker's freshly published ticket to the (possibly parked)
        // leader too; a leader-published ticket needs no wake (the leader
        // is awake) and self-wakes would only cost a spurious re-poll.
        if more
            && pool_index_leader_enabled()
            && parallel::standing::serving_on_pool()
        {
            latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                self.target.parallel_leader_proc_number,
            ));
        }
        Ok(())
    }

    /// One quantum of one unit. True = the unit has more work (a successor
    /// ticket is owed). Chunkable AMs (btree) step their resumable sweep;
    /// the rest run whole inside this single ticket (recorded residual:
    /// their sweeps stay single-unit, the M4.1 coarse shape).
    fn unit_quantum(&self, cx: &PvWorkerCx<'_, '_>, unit: &PvUnit) -> PgResult<bool> {
        let shared = &self.shared;
        let i = unit.ord;
        let indrel = &cx.indrels[i];
        let ivinfo = nbtree::IndexVacuumInfo {
            index: indrel,
            heaprel: cx.heaprel,
            analyze_only: false,
            estimated_count: shared.estimated_count.load(SeqCst),
            num_heap_tuples: f64::from_bits(shared.reltuples.load(SeqCst)),
            strategy: cx.bstrategy.clone(),
        };
        let mut run = unit.run.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(&*run, PvUnitRun::Pending) {
            let (status, istat) = {
                let s = shared.indstats[i].lock().unwrap_or_else(|e| e.into_inner());
                (s.status, if s.istat_updated { Some(s.istat) } else { None })
            };
            match status {
                PvIndVacStatus::NeedBulkdelete => {
                    match indexam::index_bulk_delete_chunked_begin(&ivinfo, istat)? {
                        Some(scan) => *run = PvUnitRun::Bulkdelete(scan),
                        None => {
                            drop(run);
                            return self.unit_whole(cx, unit, i);
                        }
                    }
                }
                PvIndVacStatus::NeedCleanup => {
                    match indexam::index_vacuum_cleanup_chunked_begin(&ivinfo, istat)? {
                        Some(nbtree::BtChunkedCleanup::Scan(scan)) => {
                            *run = PvUnitRun::Cleanup(scan)
                        }
                        Some(nbtree::BtChunkedCleanup::Done(res)) => {
                            *run = PvUnitRun::Done;
                            drop(run);
                            self.unit_complete(i, res);
                            return Ok(false);
                        }
                        None => {
                            drop(run);
                            return self.unit_whole(cx, unit, i);
                        }
                    }
                }
                _ => panic!(
                    "unexpected parallel vacuum index status {status:?} for index \"{}\"",
                    indrel.name()
                ),
            }
        }
        // Step the in-flight sweep by one quantum (the begin ticket falls
        // through here: begin + first quantum share ticket 0).
        let quantum = pool_index_quantum();
        let done = match &mut *run {
            PvUnitRun::Bulkdelete(scan) => {
                let dead =
                    Arc::clone(&shared.dead_items.lock().unwrap_or_else(|e| e.into_inner()));
                nbtree::bt_chunked_scan_step(&ivinfo, scan, Some(&dead), quantum)?
            }
            PvUnitRun::Cleanup(scan) => {
                nbtree::bt_chunked_scan_step(&ivinfo, scan, None, quantum)?
            }
            PvUnitRun::Pending | PvUnitRun::Done => {
                unreachable!("stepped index-vacuum unit without a live sweep")
            }
        };
        if !done {
            return Ok(true);
        }
        let fin = std::mem::replace(&mut *run, PvUnitRun::Done);
        drop(run);
        let res = match fin {
            PvUnitRun::Bulkdelete(scan) => {
                Some(nbtree::bt_chunked_bulkdelete_finish(&ivinfo, scan)?)
            }
            PvUnitRun::Cleanup(scan) => nbtree::bt_chunked_cleanup_finish(&ivinfo, scan)?,
            PvUnitRun::Pending | PvUnitRun::Done => unreachable!("finished unit re-finished"),
        };
        self.unit_complete(i, res);
        Ok(false)
    }

    /// Non-chunkable AM: the whole sweep inside this single ticket
    /// (parallel_vacuum_process_one_index owns stats/status/progress).
    fn unit_whole(&self, cx: &PvWorkerCx<'_, '_>, unit: &PvUnit, i: usize) -> PgResult<bool> {
        parallel_vacuum_process_one_index(
            &self.shared,
            cx.mcx,
            cx.heaprel,
            &cx.indrels[i],
            i,
            cx.bstrategy,
        )?;
        *unit.run.lock().unwrap_or_else(|p| p.into_inner()) = PvUnitRun::Done;
        Ok(false)
    }

    /// The completion tail of parallel_vacuum_process_one_index, chunk
    /// form: stats + status under the slot lock, then the progress relay
    /// (pool serves count into the leader-published relay; the
    /// participating LEADER owns the pg_stat_progress_vacuum row and
    /// publishes directly).
    fn unit_complete(&self, i: usize, istat_res: Option<IndexBulkDeleteResult>) {
        {
            let mut s = self.shared.indstats[i].lock().unwrap_or_else(|e| e.into_inner());
            if let Some(res) = istat_res {
                s.istat = res;
                s.istat_updated = true;
            }
            s.status = PvIndVacStatus::Completed;
        }
        if parallel::standing::serving_on_pool() {
            self.shared.pool_indexes_processed.fetch_add(1, SeqCst);
        } else {
            backend_progress::pgstat_progress_parallel_incr_param(
                backend_progress::progress::PROGRESS_VACUUM_INDEXES_PROCESSED,
                1,
            );
        }
        if ptrace_enabled() {
            if let Some(unit) = self.units.iter().find(|u| u.ord == i) {
                let t0 = unit.census_t0_ms.get().copied().unwrap_or(0);
                ptrace(&format!(
                    "unit-done i={i} start_ms={t0} end_ms={} quanta_pool={} quanta_leader={} busy_ms={}",
                    pg_clock::mono_ms(),
                    unit.census_quanta_pool.load(SeqCst),
                    unit.census_quanta_leader.load(SeqCst),
                    unit.census_busy_ns.load(SeqCst) / 1_000_000,
                ));
            }
        }
    }

    /// Teardown sweep (leader cleanup + the PRIVATE_SHUTDOWN hook): drop
    /// mid-sweep unit state DETERMINISTICALLY — BtVacChunkedScan's Drop
    /// releases the btree cycle-slot registration, and the NEXT vacuum of
    /// the same index must not race an Arc-chain drop for it ("multiple
    /// active vacuums"). Runs strictly after the RG reached an outcome and
    /// serves detached, so no step is in flight.
    fn release_units(&self) {
        for unit in &self.units {
            *unit.run.lock().unwrap_or_else(|p| p.into_inner()) = PvUnitRun::Done;
        }
    }
}

/// The bound descriptor's serve: parallel::standing::pool_serve mapped onto
/// the runtime's verdict (the standing_channel adapter precedent).
fn vacuum_index_pooldb_serve(
    payload: &Arc<dyn std::any::Any + Send + Sync>,
) -> runtime::BoundServe {
    match parallel::standing::pool_serve(payload) {
        parallel::standing::PoolServe::Served => runtime::BoundServe::Served,
        parallel::standing::PoolServe::Refused => runtime::BoundServe::Refused,
        parallel::standing::PoolServe::Closed => runtime::BoundServe::Closed,
    }
}

/// The standing/pool driver: runs ON a pool worker inside serve_ticket —
/// leased PGPROC identity live, parallel-worker impersonation + lock-group
/// membership installed. Owns the EAGER binder wrap around the
/// parallel_vacuum_main-shaped drive body: the binder's statement half
/// supplies what the launched substrate supplies a bgworker. Exit-committed
/// unwinds (FATAL) rethrow to the pool glue.
fn parallel_vacuum_pool_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(pv) = private.downcast::<PvShared>() else { return };
    let Some(pass) = pv.pool_pass.lock().unwrap_or_else(|p| p.into_inner()).clone() else {
        // Board raced pass teardown (close precedes the slot clear, so this
        // is a straggler): nothing to serve.
        return;
    };
    // The submit → handle-store window: the publication wake can beat the
    // leader's rg-handle store; wait briefly (a never-stored handle
    // degrades to the drive body's fail-closed refusal).
    let mut spins = 0u32;
    while pass.rg.get().is_none() && spins < 10_000 {
        std::thread::yield_now();
        spins += 1;
    }
    let target = Arc::clone(&pass.target);
    let entered = std::cell::Cell::new(false);
    let r = catch_unwind(AssertUnwindSafe(|| {
        parallel::with_query_task_binding(&target, || {
            entered.set(true);
            pool_index_drive(&pv, &pass)
        })
    }));
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if !entered.get() {
                // Binder refusal (validate() gate): fail-closed
                // non-participation — the leader's nobody-participates
                // probe falls back to the launched gang.
                ptrace(&format!("pool bind refused: {}", e.message()));
                pass.refused.fetch_add(1, SeqCst);
            } else if !pass.failed.load(SeqCst) {
                pass.fail(e);
            }
        }
        Err(unwind) => {
            pass.fail(
                PgError::new(ERROR, "parallel index vacuum pool executor panicked").into(),
            );
            latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                shared.parallel_leader_proc_number,
            ));
            if parallel::standing::is_exit_unwind(&*unwind) {
                std::panic::resume_unwind(unwind);
            }
            return;
        }
    }
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The drive body (parallel_vacuum_main, pool form): identity + cost
/// ceremony, then an external-lane pinned drive claiming per-index morsels.
/// Errors record into the pass (first-wins) and return Err so the binder
/// aborts the worker transaction (resowner releases the residue).
fn pool_index_drive(shared: &Arc<PvShared>, pass: &Arc<PvPoolPass>) -> PgResult<()> {
    let Some(rt) = runtime::global() else {
        pass.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    let Some(rg) = pass.rg.get().and_then(|w| w.upgrade()) else {
        pass.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    // Process-wide external-lane lease: exhaustion = fail-closed
    // non-participation.
    let Some(lane) = rt.acquire_external_lane() else {
        pass.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    let mut lane_local = lane.local();

    let ctx = MemoryContext::new("parallel vacuum pool worker");
    let mcx = ctx.mcx();
    let rel = match table::table_open(mcx, shared.relid, ShareUpdateExclusiveLock) {
        Ok(rel) => rel,
        Err(e) => {
            // Clean pre-drive failure: record it and reap the RG (a pinned
            // RG needs SOME driver to run its protocol cleanup).
            pass.fail(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = rt.drive_pinned(&mut lane_local, &rg);
            }
            return Ok(());
        }
    };
    let indrels = match vac_open_indexes(mcx, &rel, RowExclusiveLock) {
        Ok(v) => v,
        Err(e) => {
            pass.fail(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = rt.drive_pinned(&mut lane_local, &rg);
            }
            let _ = table::table_close(rel, ShareUpdateExclusiveLock);
            return Ok(());
        }
    };
    debug_assert!(!indrels.is_empty());

    if shared.maintenance_work_mem_worker > 0 {
        g::set_maintenance_work_mem(shared.maintenance_work_mem_worker);
    }

    autovacuum_seams::vacuum_update_costs::call()?;
    g::SetVacuumCostBalance(0);
    set_vacuum_cost_balance_local(0);
    set_vacuum_shared_cost(Some(Arc::clone(&shared.cost)));
    shared.cost.active_nworkers.fetch_add(1, SeqCst);
    pass.started.fetch_add(1, SeqCst);
    // GL-M41-2 fix (serve-start wake): the leader's participation gate
    // (started > 0, pool_leader_join) parks on the leader latch, but no
    // wake fired at serve start — only completion/refusal/error and the
    // ~1s stall recheck set it. Any pass shorter than the recheck cadence
    // therefore ran WITHOUT the leader (width = nworkers, vs the launched
    // gang's nworkers + participating leader): the constant per-vacuum W
    // wall gap. Chunks arm only (the coarse arm's leader parks by design);
    // PGRUST_RUNTIME_VACUUM_INDEX_LEADER=0 kills it with participation.
    if pass.stream.is_some() && pool_index_leader_enabled() {
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            pass.target.parallel_leader_proc_number,
        ));
    }

    let bstrategy = bufmgr_seams::get_access_strategy_with_size::call(
        BufferAccessStrategyType::BasVacuum,
        shared.ring_nbuffers * (BLCKSZ as i32 / 1024),
    );

    let mut cx = PvWorkerCx { mcx, heaprel: &rel, indrels: &indrels, bstrategy: &bstrategy };
    // Publish the worker cx for run_morsel (this thread only), drive, clear.
    // SAFETY (lifetime erasure): cx outlives the drive on this frame; the
    // pointer is cleared before cx drops.
    POOL_CX.with(|c| {
        c.set(unsafe {
            core::mem::transmute::<*mut PvWorkerCx<'_, '_>, *mut PvWorkerCx<'static, 'static>>(
                &mut cx as *mut PvWorkerCx<'_, '_>,
            )
        })
    });
    let _outcome = rt.drive_pinned(&mut lane_local, &rg);
    POOL_CX.with(|c| c.set(std::ptr::null_mut()));
    drop(cx);

    // Teardown (parallel_vacuum_main order).
    shared.cost.active_nworkers.fetch_sub(1, SeqCst);
    set_vacuum_shared_cost(None);
    if guc_tables::vars::track_cost_delay_timing.read() {
        shared
            .pool_delay_ns
            .fetch_add(::commands_vacuum::parallel_vacuum_worker_delay_ns(), SeqCst);
    }
    bufmgr_seams::free_access_strategy::call(bstrategy);
    vac_close_indexes(indrels, RowExclusiveLock)?;
    table::table_close(rel, ShareUpdateExclusiveLock)?;

    if pass.failed.load(SeqCst) {
        // A recorded error (possibly this worker's, possibly a sibling's):
        // return Err so the binder aborts the worker transaction and
        // resowner releases any residue (the leader rethrows the recorded
        // error; this message is never user-visible on that path).
        return Err(Box::new(PgError::new(
            ERROR,
            "parallel index vacuum worker failed (see leader error)",
        )));
    }
    Ok(())
}

/// Leader-side relay flush: pool workers count processed indexes / delay
/// time into PvShared; the leader (owner of the progress row) publishes.
fn flush_pool_progress(shared: &PvShared) {
    let n = shared.pool_indexes_processed.swap(0, SeqCst);
    if n > 0 {
        backend_progress::pgstat_progress_incr_param(
            backend_progress::progress::PROGRESS_VACUUM_INDEXES_PROCESSED,
            n,
        );
    }
}

fn flush_pool_delay(shared: &PvShared) {
    let ns = shared.pool_delay_ns.swap(0, SeqCst);
    if ns > 0 {
        backend_progress::pgstat_progress_incr_param(
            backend_progress::progress::PROGRESS_VACUUM_DELAY_TIME,
            ns,
        );
    }
}

/// Everything the leader join needs from one engaged pass.
struct PoolEngaged {
    pass: Arc<PvPoolPass>,
    entry: Arc<parallel::standing::StandingEngagement>,
    rg: runtime::RgHandle,
    waiter: runtime::CompletionWaiter,
}

/// Abort + BOUNDED drain of the pinned RG (the vacuumlazy drain_rg shape).
/// False = leaked (dead participant) — callers surface an error rather
/// than wait forever.
fn pool_drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else { return false };
    let mut local = lane.local();
    rt.try_drain_pinned(&mut local, rg, 4000).is_some()
}

/// PRIVATE_SHUTDOWN hook: a leader unwind between pool engage and the join's
/// own cleanup reaches here through DestroyParallelContext — complete the RG
/// (drain releases drives parked on the aborted generation) and the standing
/// join (close + await detach) so pool participants never outlive the leader
/// arena.
fn parallel_vacuum_pool_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(pv) = private.downcast_ref::<PvShared>() else { return };
    let pass = pv.pool_pass.lock().unwrap_or_else(|p| p.into_inner()).take();
    let Some(pass) = pass else { return };
    if let Some(rt) = runtime::global() {
        if let Some(rg) = pass.rg.get().and_then(|w| w.upgrade()) {
            if rg.try_outcome().is_none() {
                pool_drain_rg(rt, &rg);
            }
        }
    }
    let board = pass.board.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some(entry) = board {
        parallel::standing::close_and_await(&entry);
    }
    // Q2: deterministic release of mid-sweep chunk state (cycle slots).
    pass.release_units();
}

fn ensure_pool_shutdown_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_private_shutdown(parallel_vacuum_pool_shutdown);
    });
}

/// First-claim deadline (the standing channel's knob): an unclaimed
/// engagement after this long means the pool cannot serve us — launched
/// fallback (correctness never depends on this).
fn pool_claim_deadline() -> std::time::Duration {
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(*MS.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_GANG_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

/// Engage the pool channel for one index pass: build the pass payload +
/// board, run the cost carry-in, submit the bound utility RG. None =
/// channel unavailable / nothing parallel-safe — the launched gang takes
/// over with NOTHING consumed (the pass payload is cleared).
fn pool_engage_pass(
    pvs: &ParallelVacuumState,
    nworkers: i32,
) -> Option<PoolEngaged> {
    let rt = runtime::global()?;
    let safe: Vec<usize> = pvs
        .shared
        .indstats
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.lock().unwrap_or_else(|e| e.into_inner()).parallel_workers_can_process
        })
        .map(|(i, _)| i)
        .collect();
    if safe.is_empty() {
        return None;
    }
    let target = parallel::shared_for(pvs.pcxt);
    let entry = parallel::standing::try_engage_pool(&target, nworkers.max(0) as usize)?;
    // Q2 chunks arm: one unit per safe index, one seed ticket each; the
    // stream is claimable the moment the submission publishes.
    let chunked = pool_index_chunks_enabled();
    let (units, tickets, stream, units_left) = if chunked {
        let units: Vec<PvUnit> = safe
            .iter()
            .map(|&ord| PvUnit {
                ord,
                busy: AtomicBool::new(false),
                run: Mutex::new(PvUnitRun::Pending),
                census_t0_ms: OnceLock::new(),
                census_quanta_pool: AtomicU64::new(0),
                census_quanta_leader: AtomicU64::new(0),
                census_busy_ns: AtomicU64::new(0),
            })
            .collect();
        let tickets: Vec<u32> = (0..units.len() as u32).collect();
        let stream = Arc::new(runtime::StreamSource::new());
        stream.publish(tickets.len() as u64);
        let n = units.len();
        (units, tickets, Some(stream), n)
    } else {
        (Vec::new(), Vec::new(), None, 0)
    };
    let pass = Arc::new(PvPoolPass {
        shared: Arc::clone(&pvs.shared),
        target,
        rg: OnceLock::new(),
        board: Mutex::new(Some(Arc::clone(&entry))),
        safe,
        units,
        tickets: Mutex::new(tickets),
        stream,
        units_left: AtomicUsize::new(units_left),
        started: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
    });
    *pvs.shared.pool_pass.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&pass));

    // C's leader cost-balance carry-in (the launched block's discipline) —
    // BEFORE the submission goes live: pool workers may serve the instant
    // the publication lands.
    pvs.shared.cost.cost_balance.store(g::VacuumCostBalance() as u32, SeqCst);
    pvs.shared.cost.active_nworkers.store(0, SeqCst);

    static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
    let source: Arc<dyn runtime::MorselSource> = match &pass.stream {
        // Q2 chunks arm: the ticket stream (quantum claims).
        Some(s) => Arc::clone(s) as _,
        // Coarse arm: M4.1's one-index-per-claim geometry.
        None => Arc::new(IndexGranuleSource { total: pass.safe.len() as u64 }),
    };
    let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(&pass) as _;
    let descriptor = runtime::BoundDescriptor {
        serve: vacuum_index_pooldb_serve,
        payload: Arc::clone(&entry) as Arc<dyn std::any::Any + Send + Sync>,
        width: 0, // utility: never charges interactive demand
    };
    let (rg, waiter) = rt.submit_pinned_bound_utility(
        runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, SeqCst),
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        },
        0,
        Some(runtime::WidthRequest::unbounded(nworkers.max(1) as u32)),
        descriptor,
    );
    pass.rg
        .set(rg.downgrade())
        .unwrap_or_else(|_| unreachable!("rg set once per pass payload"));

    // The leader's own accounting joins the shared-balance discipline for
    // the pass (its unsafe-index processing runs concurrently with pool
    // participants).
    g::SetVacuumCostBalance(0);
    set_vacuum_cost_balance_local(0);
    set_vacuum_shared_cost(Some(Arc::clone(&pvs.shared.cost)));

    ptrace(&format!(
        "engaged pass tickets={nworkers} safe_indexes={}",
        pass.safe.len()
    ));
    if pass.stream.is_some() {
        // The Q2 witness line (battery oracle): the chunks arm engaged.
        ptrace(&format!(
            "chunked pass: units={} quantum={} leader={}",
            pass.units.len(),
            pool_index_quantum(),
            pool_index_leader_enabled(),
        ));
    }
    Some(PoolEngaged { pass, entry, rg, waiter })
}

/// One pool pass's leader join verdict.
enum PoolPassWait {
    Done,
    /// Nothing claimed (started == 0): statuses untouched — the launched
    /// gang takes over.
    Fallback,
}

/// The leader's join loop for one pool pass (the standing channel's wait
/// shape + the relay flush): poll completion + interrupts + participation
/// counters; every exit path completes the RG if needed, closes the board
/// entry, awaits detach, clears the pass slot, and flushes the relays.
/// Q2, chunks arm: once the pool channel is LIVE (a serve started) the
/// leader also participates between poll duties through the bounded drive
/// — ≤1 task (~t_max) per burst keeps the park loop's cancel guarantee
/// index-size-independent. It stays parked-only until then so the
/// started==0 fallback verdicts (refusal / claim deadline → launched gang)
/// are never usurped by leader-only progress.
fn pool_leader_join(
    pvs: &ParallelVacuumState,
    eng: &PoolEngaged,
    mcx: Mcx<'_>,
    heaprel: &RelationData<'_>,
    indrels: &[Relation<'_>],
    bstrategy: &BufferAccessStrategy,
) -> PgResult<PoolPassWait> {
    let rt = runtime::global().expect("engaged with a live runtime");
    let shared = &pvs.shared;
    let entry = &eng.entry;
    let cleanup = |shared: &PvShared, entry: &Arc<parallel::standing::StandingEngagement>| {
        // Take the board slot first (the shutdown hook's double-close
        // guard), then the arena-lifetime join, then the pass slot; the
        // unit sweep (Q2) runs after detach so no step is in flight.
        eng.pass.board.lock().unwrap_or_else(|p| p.into_inner()).take();
        parallel::standing::close_and_await(entry);
        eng.pass.release_units();
        *shared.pool_pass.lock().unwrap_or_else(|p| p.into_inner()) = None;
        flush_pool_progress(shared);
        flush_pool_delay(shared);
    };
    // Q2 leader participation state (chunks arm only): the external lane +
    // worker-local are acquired lazily at first eligibility; the leader's
    // own opened relations back its morsel bodies (the launched channel's
    // leader-participation identity, unchanged).
    let leader_eligible = eng.pass.stream.is_some() && pool_index_leader_enabled();
    let mut leader_drive: Option<(runtime::ExternalLane, runtime::WorkerLocal)> = None;
    let mut leader_cx = PvWorkerCx { mcx, heaprel, indrels, bstrategy };
    // DST P2: the claim deadline is BEHAVIORAL (changes execution path) —
    // its clock is pg_clock (the standing_channel precedent).
    let t0 = pg_clock::MonoStamp::now();
    let mut traced = false;
    // GL-M41-2 census: leader-participation bursts (drive calls / tasks
    // actually ran) + total join time, emitted on the Done exit.
    let mut census_bursts: u64 = 0;
    let mut census_ran: u64 = 0;
    loop {
        if let Some(o) = eng.waiter.try_wait() {
            cleanup(shared, entry);
            if !traced {
                ptrace(&format!(
                    "engaged pooldb dop={} safe_indexes={}",
                    entry.claimed(),
                    eng.pass.safe.len()
                ));
            }
            ptrace(&format!(
                "pass-done join_ms={} claimed={} leader_bursts={census_bursts} leader_ran={census_ran}",
                t0.elapsed_ns() / 1_000_000,
                entry.claimed(),
            ));
            if let Some(e) = eng.pass.take_error() {
                return Err(e);
            }
            if o == runtime::RgOutcome::Aborted {
                postgres_seams::check_for_interrupts::call()?;
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel index vacuum pool pass aborted",
                )));
            }
            if eng.pass.started.load(SeqCst) == 0 {
                // Completed but nobody participated (only possible
                // pre-claim; an empty pass is refused at engage).
                return Ok(PoolPassWait::Fallback);
            }
            return Ok(PoolPassWait::Done);
        }
        // Order matters on every error exit: abort THEN drain (the leader's
        // protocol cleanup completes the RG and releases workers parked in
        // their drives) THEN close-and-await.
        if let Err(e) = postgres_seams::check_for_interrupts::call() {
            eng.rg.abort();
            pool_drain_rg(rt, &eng.rg);
            cleanup(shared, entry);
            return Err(e);
        }
        // Progress relay: the leader owns the pg_stat_progress_vacuum row.
        flush_pool_progress(shared);
        let claimed = entry.claimed();
        if !traced && claimed > 0 {
            ptrace(&format!(
                "engaged pooldb dop={claimed} safe_indexes={}",
                eng.pass.safe.len()
            ));
            traced = true;
        }
        let started = eng.pass.started.load(SeqCst);
        let refused = entry.refused() + eng.pass.refused.load(SeqCst);
        if started == 0 && refused >= entry.tickets() {
            pool_drain_rg(rt, &eng.rg);
            cleanup(shared, entry);
            ptrace(&format!("pooldb refused ({refused} refusals) — launched fallback"));
            return Ok(PoolPassWait::Fallback);
        }
        if started == 0
            && entry.detached() >= claimed
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > pool_claim_deadline()
        {
            pool_drain_rg(rt, &eng.rg);
            cleanup(shared, entry);
            ptrace("pooldb claim deadline — launched fallback");
            return Ok(PoolPassWait::Fallback);
        }
        if claimed > 0 && started > 0 && entry.detached() >= claimed {
            if let Some(o) = eng.waiter.try_wait() {
                let _ = o;
                continue;
            }
            if let Some(e) = eng.pass.take_error() {
                eng.rg.abort();
                pool_drain_rg(rt, &eng.rg);
                cleanup(shared, entry);
                return Err(e);
            }
            eng.rg.abort();
            pool_drain_rg(rt, &eng.rg);
            cleanup(shared, entry);
            return Err(Box::new(PgError::new(
                ERROR,
                "parallel index vacuum pool executors exited before completing the pass",
            )));
        }
        // Q2 leader participation burst (chunks arm, pool live, healthy):
        // ≤1 task per burst; a burst that RAN work re-polls immediately —
        // the latch park is only for idle windows (every claimable ticket
        // has a parked serve to run it, so nothing is stranded).
        let mut participated = false;
        if leader_eligible && started > 0 && !eng.pass.failed.load(SeqCst) {
            if leader_drive.is_none() {
                if let Some(lane) = rt.acquire_external_lane() {
                    let local = lane.local();
                    leader_drive = Some((lane, local));
                }
            }
            if let Some((_lane, local)) = leader_drive.as_mut() {
                shared.cost.active_nworkers.fetch_add(1, SeqCst);
                // Publish the leader's relations for the morsel bodies
                // (the drive frame outlives the burst; cleared before).
                POOL_CX.with(|c| {
                    c.set(unsafe {
                        core::mem::transmute::<
                            *mut PvWorkerCx<'_, '_>,
                            *mut PvWorkerCx<'static, 'static>,
                        >(&mut leader_cx as *mut PvWorkerCx<'_, '_>)
                    })
                });
                let (_outcome, ran) = rt.drive_pinned_tasks(local, &eng.rg, 1);
                POOL_CX.with(|c| c.set(std::ptr::null_mut()));
                shared.cost.active_nworkers.fetch_sub(1, SeqCst);
                census_bursts += 1;
                census_ran += ran as u64;
                participated = ran > 0;
            }
        }
        if participated {
            continue;
        }
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            eng.rg.abort();
            pool_drain_rg(rt, &eng.rg);
            cleanup(shared, entry);
            return Err(e);
        }
    }
}

/// The launched bgworker gang's per-pass bring-up (the pre-M4.1 block,
/// hoisted so the pool channel's fallback shares it). `carry_in` = run the
/// leader cost-balance carry-in (skipped on a pool fallback: the shared
/// balance already holds it plus the leader's unsafe-index accrual).
fn launch_gang(
    pvs: &ParallelVacuumState,
    nworkers: i32,
    num_index_scans: i32,
    carry_in: bool,
) -> PgResult<()> {
    let census_t0 = ptrace_enabled().then(pg_clock::MonoStamp::now);
    if num_index_scans > 0 {
        parallel::ReinitializeParallelDSM(pvs.pcxt)?;
    }

    if carry_in {
        pvs.shared.cost.cost_balance.store(g::VacuumCostBalance() as u32, SeqCst);
        pvs.shared.cost.active_nworkers.store(0, SeqCst);
    }

    parallel::ReinitializeParallelWorkers(pvs.pcxt, nworkers);
    parallel::LaunchParallelWorkers(pvs.pcxt)?;

    if parallel::nworkers_launched(pvs.pcxt) > 0 {
        g::SetVacuumCostBalance(0);
        set_vacuum_cost_balance_local(0);
        set_vacuum_shared_cost(Some(Arc::clone(&pvs.shared.cost)));
    }
    if let Some(t0) = census_t0 {
        ptrace(&format!(
            "gang-launch nworkers={nworkers} launched={} ms={}",
            parallel::nworkers_launched(pvs.pcxt),
            t0.elapsed_ns() / 1_000_000,
        ));
    }
    Ok(())
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

    // GL-M41-2: whole-pass wall + channel label (trace-gated).
    let census_t0 = ptrace_enabled().then(pg_clock::MonoStamp::now);
    let mut census_channel = "leader-serial";

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

    // M4.1 pool channel first (armed + workers wanted): per-index coarse
    // morsels on PGPROC-leasing pool workers; the launched bgworker gang
    // stays the fallback on every refusal (fail-closed layering: pool →
    // launched → leader-serial).
    let mut pool: Option<PoolEngaged> = None;
    if nworkers > 0 && pvs.pool_armed {
        pool = pool_engage_pass(pvs, nworkers);
    }

    if nworkers > 0 && pool.is_none() {
        launch_gang(pvs, nworkers, num_index_scans, /* carry_in */ true)?;
        // "launched %d parallel vacuum workers ..." at elevel: DEBUG2 without
        // VERBOSE (loud upstream), so not emitted.
    }

    parallel_vacuum_process_unsafe_indexes(pvs, mcx, heaprel, indrels, bstrategy)?;

    match &pool {
        Some(eng) => match pool_leader_join(pvs, eng, mcx, heaprel, indrels, bstrategy)? {
            PoolPassWait::Done => {
                census_channel = "pool";
            }
            PoolPassWait::Fallback => {
                // Nothing consumed (started == 0, statuses untouched): the
                // launched gang takes over. The cost carry-in is SKIPPED —
                // the shared balance already holds it plus the leader's
                // unsafe-index accrual from the engaged window.
                census_channel = "pool-fallback-launched";
                launch_gang(pvs, nworkers, num_index_scans, /* carry_in */ false)?;
                parallel_vacuum_process_safe_indexes(
                    &pvs.shared,
                    mcx,
                    heaprel,
                    indrels,
                    bstrategy,
                    vacuum_shared_cost().is_some(),
                )?;
                let wait_t0 = ptrace_enabled().then(pg_clock::MonoStamp::now);
                parallel::WaitForParallelWorkersToFinish(pvs.pcxt)?;
                if let Some(t0) = wait_t0 {
                    ptrace(&format!("gang-wait ms={}", t0.elapsed_ns() / 1_000_000));
                }
            }
        },
        None => {
            if nworkers > 0 {
                census_channel = "launched";
            }
            parallel_vacuum_process_safe_indexes(
                &pvs.shared,
                mcx,
                heaprel,
                indrels,
                bstrategy,
                vacuum_shared_cost().is_some(),
            )?;

            if nworkers > 0 {
                let wait_t0 = ptrace_enabled().then(pg_clock::MonoStamp::now);
                parallel::WaitForParallelWorkersToFinish(pvs.pcxt)?;
                if let Some(t0) = wait_t0 {
                    ptrace(&format!("gang-wait ms={}", t0.elapsed_ns() / 1_000_000));
                }
                // Buffer/WAL usage accumulation skipped: pgBufferUsage is
                // derived from live bufmgr counters here and its vacuum
                // consumers (VERBOSE, autovacuum log) are elided/loud.
            }
        }
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
    if let Some(t0) = census_t0 {
        ptrace(&format!(
            "pass-all op={} channel={census_channel} nworkers={nworkers} ms={}",
            if vacuum { "bulkdel" } else { "cleanup" },
            t0.elapsed_ns() / 1_000_000,
        ));
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
    // GL-M41-2 width census (trace-gated): who processed this index, when it
    // started (shared mono epoch — overlap reads directly off start+dur), and
    // how long it took. Covers the launched workers, the participating/serial
    // leader, the unsafe-index loop, and the pool COARSE arm (the chunks arm
    // reports per-unit through unit_complete instead).
    let census_t0 = ptrace_enabled().then(|| (pg_clock::mono_ms(), pg_clock::MonoStamp::now()));
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
    if parallel::standing::serving_on_pool() {
        // M4.1 pool worker: no launched-channel progress sender exists on a
        // pool serve — relay through the shared counter; the LEADER (owner
        // of the pg_stat_progress_vacuum row) publishes at its park-loop
        // cadence, so the row stays truthful within one poll quantum.
        shared.pool_indexes_processed.fetch_add(1, SeqCst);
    } else {
        backend_progress::pgstat_progress_parallel_incr_param(
            backend_progress::progress::PROGRESS_VACUUM_INDEXES_PROCESSED,
            1,
        );
    }
    if let Some((start_ms, t0)) = census_t0 {
        let who = if parallel::IsParallelWorker() {
            "launched"
        } else if parallel::standing::serving_on_pool() {
            "pool"
        } else {
            "leader"
        };
        ptrace(&format!(
            "idx-one i={idx} name={} op={status:?} who={who} start_ms={start_ms} ms={}",
            indrel.name(),
            t0.elapsed_ns() / 1_000_000,
        ));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// ARMED-WITNESS leg (scripts/armed-witness-e2e.sh registry row
    /// `vacuum-pool`): the arming env must TAKE through every once-cached
    /// gate on the M4.1 kill-switch chain — the silent-no-witness failure
    /// mode (misspelled knob, stale process-static cache) the registry
    /// exists to kill. Engagement itself is witnessed by the server-level
    /// battery (scripts/vacuum-pool-e2e.sh, its armed legs self-arm);
    /// this leg mechanically proves the env plumbing in the CONFIRM world.
    /// Unarmed (the default `cargo test` world) it self-SKIPs, so the
    /// default units leg stays byte-identical.
    #[test]
    fn armed_pool_kill_switch_chain_takes() {
        if std::env::var("PGRUST_RUNTIME_VACUUM_POOL").as_deref() != Ok("1") {
            eprintln!(
                "SKIP: armed_pool_kill_switch_chain_takes (PGRUST_RUNTIME_VACUUM_POOL unset)"
            );
            return;
        }
        assert!(
            runtime::runtime_enabled(),
            "PGRUST_RUNTIME=1 must ride the registry row's env"
        );
        runtime::install_global(runtime::Runtime::new(runtime::RuntimeConfig::new(2)));
        assert!(
            parallel::standing::pool_binding_enabled(),
            "POOLBIND default-on gate observes the armed world"
        );
        assert!(
            parallel::standing::pooldb_enabled(),
            "PGRUST_RUNTIME_POOLDB=1 must ride the registry row's env (the inc-2 gate)"
        );
        assert!(
            pool_index_enabled(),
            "the vacuum index-pass pool gate admits under the armed env"
        );
        // GL-M41-3 flipped-kill polarity: the backing cell IS the switch
        // (env seeds it at boot; unit processes see the flipped default).
        // Off must close the gate; restore the default after.
        assert!(
            guc_tables::backing::pgrust_runtime_vacuum_pool(),
            "pgrust.runtime_vacuum_pool defaults ON since the train-40 flip"
        );
        guc_tables::backing::set_pgrust_runtime_vacuum_pool(false);
        assert!(
            !pool_index_enabled(),
            "pgrust.runtime_vacuum_pool = off must restore the launched gang"
        );
        guc_tables::backing::set_pgrust_runtime_vacuum_pool(true);
    }

    /// Q2 posture: the chunk sub-switches are LAYERED UNDER the pool kill
    /// switch (nothing here engages without PGRUST_RUNTIME_VACUUM_POOL=1 —
    /// matching M4.1's default-OFF posture) and default to the chunked
    /// shape + leader participation when the pool arm is live. Self-skips
    /// under explicit overrides (the armed-witness legs set them).
    #[test]
    fn chunk_gates_default_posture() {
        if std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_CHUNKS").is_ok()
            || std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_QUANTUM").is_ok()
            || std::env::var("PGRUST_RUNTIME_VACUUM_INDEX_LEADER").is_ok()
        {
            eprintln!("SKIP: chunk_gates_default_posture (env overrides present)");
            return;
        }
        assert!(pool_index_chunks_enabled(), "chunks default ON under the pool arm");
        assert!(pool_index_quantum() > 0, "quantum must be positive");
        assert_eq!(pool_index_quantum(), 256, "default quantum of record");
        assert!(pool_index_leader_enabled(), "leader participation default ON");
    }

    /// One-index-per-claim contract: every granule is its own hard boundary
    /// (whole-boundary claims make a claim exactly one index — the coarse
    /// long-unit morsel of record). Unconditional: pure geometry.
    #[test]
    fn index_granule_source_one_index_per_claim() {
        use runtime::MorselSource as _;
        let s = IndexGranuleSource { total: 5 };
        assert_eq!(s.total_granules(), 5);
        for g in 0..5u64 {
            assert_eq!(s.next_boundary_after(g), g + 1, "granule {g} is its own boundary");
        }
        assert!(s.whole_boundary_claims());
        assert_eq!(s.startup_c0(), 1);
    }
}
