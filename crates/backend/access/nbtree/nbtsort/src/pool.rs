//! M4.2 (all-morsels Track 3 item 2): parallel btree BUILD driver on the
//! morsel-pool workers. Pool workers claim heap BLOCK-RANGE morsels (the
//! scan phase), run the ordinary build-scan body (visibility routing, HOT
//! root lookup, predicate, FormIndexDatum) on their own opened relations
//! under the M2 inc-2 bound-descriptor gate, FORM index tuples, and stream
//! the formed images to the LEADER in batches; the leader feeds them into
//! its ONE tuplesort (spool2 routing for unique builds unchanged) and runs
//! the untouched sort + load tail. QoS class Utility (Q0 stride weight +
//! Q1 capped ledger tier), kill switch PGRUST_RUNTIME_INDEXBUILD_POOL
//! default OFF, fail-closed to the serial build on every refusal.
//!
//! CENSUS OF RECORD (2026-07-21): pgrust has NO bgworker-gang parallel
//! btree build — index_build (catalog_index) is "btree only, serial only";
//! nbtsort.c's _bt_begin_parallel/_bt_parallel_scan_and_sort family was
//! never ported. The M4.1 fail-closed layering "pool → launched gang →
//! serial" therefore degenerates here to "pool → serial": the middle tier
//! is vacuous by census, not by choice. The serial path is byte-identical
//! when the pool arm is off or refuses.
//!
//! SORT-PHASE ADJUDICATION (charter #2): per-worker tuplesorts with a
//! leader K-way merge (C's shape) are NOT buildable today — `Tuplesort` is
//! structurally thread-affine: spilled tapes resolve VFD indices against a
//! thread-local fd cache (fd/vfd.rs) with temp files registered on the
//! thread-local CurrentResourceOwner, and the Index sort variant holds
//! lifetime-erased non-atomic relcache Rc clones aliasing the opening
//! thread's relation (tuplesort lib.rs begin_index_btree). Moving or
//! draining a worker's sort from the leader is unsound for any spilled
//! sort and races the relcache refcount even in-memory. The runtime sort
//! machinery (post-colstage) owns runtime COLUMN layouts, not index-tuple
//! streams — riding it means an indextuple lane it does not have. The
//! honest M4.2 increment is therefore scan/form-parallel + leader-owned
//! sort: unique enforcement, spool2 dead-tuple routing, dedup, and page
//! build all run the serial code on the leader, and because the btree
//! sort comparator is TOTAL (key then heap-TID tiebreak), the sorted
//! sequence — hence every index page image — is independent of arrival
//! order: the pool arm's index file is byte-identical to the serial
//! build's under an equal visibility horizon. NAMED DEBT (M4.2-1):
//! parallel sort runs need either a transferable tuplesort (Arc'd
//! descriptor + fileset-backed tapes; the BufFileCreateFileSet primitive
//! exists unused) or a per-worker emit protocol (each worker drives its
//! OWN pinned RG at Utility and serves leader-published refill tickets
//! from its thread-affine sort — the Q2 ticket shape, inverted), plus
//! leader-side inter-worker unique enforcement. Sketch banked in the
//! night record.
//!
//! CONCURRENTLY (charter #1): OUT OF SCOPE here BY CHOICE, and recorded
//! honestly — C's parallel build DOES serve CREATE INDEX CONCURRENTLY
//! (serialized MVCC snapshot to the gang); our increment gates on
//! !ii_Concurrent and CIC keeps the serial scan. The pool binder restores
//! the leader's transaction snapshot either way; lifting the gate is a
//! follow-up once the MVCC-lane parity legs exist.
//!
//! UNCOMMITTED-DDL BINDING: at index_build time the creating transaction
//! necessarily holds unbroadcast invalidation messages (the index's own
//! pg_class/pg_index rows), which the query-task binder refuses by
//! default. This driver installs the binding with `invals_flush` (the
//! M4.2 opt-in): every bind blankets the pool thread's caches
//! (InvalidateSystemCaches — C fresh-process parity, so cache loads
//! rebuild under the bound snapshot/xid and SEE the leader's uncommitted
//! rows) and taints them eagerly for the next adoption (abort broadcasts
//! nothing — the launched substrate's note_caches_tainted law).
//!
//! Stability note (concurrent-window-bisect): this driver rides the same
//! standing-gang machinery under repair in that lane; the diff is
//! separable (default-OFF knob, zero shared-machinery edits beyond the
//! opt-in binder flag) and the battery carries a stop/start-during-build
//! leg.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use ::mcx::{Mcx, MemoryContext};
use ::types_core::{BlockNumber, ForkNumber, Oid, TransactionId};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_rel::lock::{RowExclusiveLock, ShareLock};
use ::types_rel::Relation;
use ::types_tuple::itemptr::ItemPointerData;

use execindexing::IndexInfo;

/// M4.2 kill switch: `PGRUST_RUNTIME_INDEXBUILD_POOL=1` arms the pool
/// channel for non-concurrent btree builds. DEFAULT OFF. Layered under the
/// runtime master switch and the standing module's own gates
/// (POOLBIND/POOLDB inside try_engage_pool) — the M4.1 chain shape.
fn pool_build_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_INDEXBUILD_POOL").is_ok_and(|v| v.trim() == "1")
    }) && runtime::runtime_enabled()
        && runtime::global().is_some()
        && miscinit::GetMyBackendType() != ::types_core::BackendType::AutovacWorker
        && !parallel::IsParallelWorker()
}

/// Formed-tuple batch target (bytes shipped per queue push). Calibration
/// knob, not product surface (the PGRUST_RUNTIME_STRIDE precedent).
fn batch_bytes() -> usize {
    static B: OnceLock<usize> = OnceLock::new();
    *B.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_INDEXBUILD_BATCH_KB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|k| *k > 0)
            .unwrap_or(256)
            * 1024
    })
}

/// Queue backpressure cap (bytes of undrained batches): workers wait —
/// permits donated — once the leader falls this far behind. Calibration
/// knob.
fn queue_cap() -> usize {
    static C: OnceLock<usize> = OnceLock::new();
    *C.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_INDEXBUILD_QUEUE_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|m| *m > 0)
            .unwrap_or(16)
            * 1024
            * 1024
    })
}

/// Engagement trace (`PGRUST_INDEXBUILD_TRACE=1`, the battery's oracle
/// channel). Default-off, zero cost.
fn btrace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_INDEXBUILD_TRACE").is_ok_and(|v| v.trim() == "1"))
}

/// Claim-cadence witness channel (`PGRUST_INDEXBUILD_CLAIM_TIMING=1`):
/// per-claim service time on the scan task set — the un-preemptible
/// permit-hold bound the long-unit law cares about. Observability only,
/// pg_clock (DST P2), default-off zero cost.
fn claim_timing_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_INDEXBUILD_CLAIM_TIMING").is_ok_and(|v| v.trim() == "1")
    })
}

fn btrace(msg: &str) {
    if btrace_enabled() {
        eprintln!("btbuild-pool: {msg}");
    }
}

/// First-claim deadline (the standing channel's knob, shared with M4.1):
/// an unclaimed engagement after this long means the pool cannot serve us
/// — serial fallback (correctness never depends on this).
fn pool_claim_deadline() -> std::time::Duration {
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(*MS.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_GANG_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

/// One pool-engaged build scan: participation board + batch queue + the
/// task-set work. Granule g = heap block g; the sizer batches contiguous
/// granules into ~t_max claims, so per-claim service time rides the
/// standard duration envelope (no long units in the scan phase).
struct BtBuildPool {
    heap_oid: Oid,
    index_oid: Oid,
    /// Hoisted visibility horizon: computed ONCE by the leader before
    /// submit and shared, so every worker routes DEAD vs RECENTLY_DEAD
    /// exactly as one serial scan started at the same instant would.
    oldest_xmin: TransactionId,
    leader_proc: i32,
    /// The binder target (shared_for(pcxt)) — the driver binds eagerly
    /// through it.
    target: Arc<parallel::ParallelShared>,
    rg: OnceLock<runtime::WeakRgHandle>,
    /// Board-entry slot, held across the leader join so the
    /// PRIVATE_SHUTDOWN hook can complete the standing join on leader
    /// unwinds (the shutdown_standing_join law).
    board: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    /// Formed-tuple batches (records: u32 len | u8 alive | image bytes).
    queue: Mutex<std::collections::VecDeque<Vec<u8>>>,
    queued_bytes: AtomicUsize,
    /// Workers park here (permit donated) when the queue is over cap.
    space: Condvar,
    started: AtomicUsize,
    refused: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// Leader teardown in progress: shippers must drain out (a worker
    /// parked on a full queue would otherwise stall the abort drain).
    closing: AtomicBool,
    broken_hot_chain: AtomicBool,
    /// Workers' per-range sums, accumulated under a cold-path lock.
    reltuples: Mutex<f64>,
}

impl BtBuildPool {
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
        // Release any sibling parked on a full queue.
        self.space.notify_all();
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    /// Ship one formed-tuple batch to the leader. Backpressure: over-cap
    /// pushes wait with the execution permit DONATED (blocking facade —
    /// the wait is I/O-shaped: the leader is the consumer), bounded waits
    /// so failure/teardown is observed. The permit is reacquired with NO
    /// locks held (lock-then-donate would let permit-holding siblings
    /// queue on the mutex behind a permit-starved holder).
    fn ship(&self, batch: Vec<u8>) -> PgResult<()> {
        let bytes = batch.len();
        let mut batch = Some(batch);
        loop {
            if self.failed.load(SeqCst) || self.closing.load(SeqCst) {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel index build pool pass is shutting down",
                )));
            }
            {
                let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                let queued = self.queued_bytes.load(SeqCst);
                if q.is_empty() || queued + bytes <= queue_cap() {
                    q.push_back(batch.take().expect("unshipped batch"));
                    self.queued_bytes.fetch_add(bytes, SeqCst);
                    drop(q);
                    latch::SetLatch(::types_storage::latch::LatchHandle::proc(self.leader_proc));
                    return Ok(());
                }
            }
            let section = runtime::blocking_io_section();
            {
                let q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                if self.queued_bytes.load(SeqCst) + bytes > queue_cap() && !q.is_empty() {
                    let _ = self
                        .space
                        .wait_timeout(q, std::time::Duration::from_millis(2))
                        .unwrap_or_else(|p| p.into_inner());
                }
            }
            drop(section);
        }
    }

    fn add_reltuples(&self, n: f64) {
        *self.reltuples.lock().unwrap_or_else(|p| p.into_inner()) += n;
    }
}

/// Heap block-range source: granule = one heap block, no interior
/// boundaries — the duration-adaptive sizer shapes claims to ~t_max.
struct BtBlockSource {
    nblocks: u64,
}

impl runtime::MorselSource for BtBlockSource {
    fn total_granules(&self) -> u64 {
        self.nblocks
    }
}

/// Per-participant build context: the participant's OWN opened relations +
/// IndexInfo + form scratch, published for run_morsel through a
/// thread-local pointer (the vacuumparallel POOL_CX pattern — this thread
/// is the only driver of its lane).
struct BtWorkerCx<'a, 'mcx> {
    mcx: Mcx<'a>,
    heap: &'a Relation<'mcx>,
    index: &'a Relation<'mcx>,
    index_info: IndexInfo<'a>,
    /// Per-tuple form scratch, reset after each record is copied out.
    scratch: MemoryContext,
}

thread_local! {
    static BT_CX: std::cell::Cell<*mut BtWorkerCx<'static, 'static>> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

impl runtime::TaskSetWork for BtBuildPool {
    fn run_morsel(&self, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(SeqCst) {
            // Already aborting: the claim drains without work.
            return;
        }
        let t0 = claim_timing_enabled().then(pg_clock::MonoStamp::now);
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => self.fail(
                PgError::new(ERROR, "parallel index build worker panicked in a morsel").into(),
            ),
        }
        if let Some(t0) = t0 {
            eprintln!("btbuild-pool: claim_us={}", t0.elapsed_ns() / 1000);
        }
    }

    fn finalize(&self) {
        // Completion is the leader's verdict (waiter + final queue drain).
    }
}

impl BtBuildPool {
    /// One claimed block range: scan it with the ordinary build-scan body
    /// (this participant's relations/IndexInfo), form each surviving tuple
    /// under an identical descriptor, and ship record batches.
    fn morsel_body(&self, range: runtime::MorselRange) -> PgResult<()> {
        let p = BT_CX.with(|c| c.get());
        if p.is_null() {
            return Err(
                PgError::new(ERROR, "index build morsel without a bound worker").into()
            );
        }
        // SAFETY: set by THIS thread's drive frame; the frame outlives the
        // drive, and run_morsel only executes on the claiming thread.
        let cx: &mut BtWorkerCx<'_, '_> = unsafe { &mut *p };
        let start = range.start as BlockNumber;
        let len = (range.end - range.start) as BlockNumber;

        let mut batch: Vec<u8> = Vec::with_capacity(batch_bytes() + 512);
        let mut err: Option<Box<PgError>> = None;
        let (mcx, heap, index, index_info, scratch) =
            (cx.mcx, cx.heap, cx.index, &mut cx.index_info, &mut cx.scratch);
        let reltuples = execindexing::table_index_build_range_scan_with_xmin(
            mcx,
            heap,
            index,
            index_info,
            /* allow_sync */ false,
            /* anyvisible */ false,
            start,
            len,
            Some(self.oldest_xmin),
            |_index_rel, tid, values, isnull, tuple_is_alive| {
                match self.form_record(scratch, index, tid, values, isnull, tuple_is_alive, &mut batch)
                {
                    Ok(()) => {}
                    Err(e) => {
                        err = Some(e);
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "parallel index build record aborted",
                        )));
                    }
                }
                if batch.len() >= batch_bytes() {
                    if let Err(e) = self.ship(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(batch_bytes() + 512),
                    )) {
                        err = Some(e);
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "parallel index build record aborted",
                        )));
                    }
                }
                Ok(())
            },
        );
        if let Some(e) = err {
            return Err(e);
        }
        let reltuples = reltuples?;
        if !batch.is_empty() {
            self.ship(batch)?;
        }
        self.add_reltuples(reltuples);
        if index_info.ii_BrokenHotChain {
            self.broken_hot_chain.store(true, SeqCst);
        }
        Ok(())
    }

    /// Form one index tuple (image + TID, exactly putindextuplevalues'
    /// front half) and append its record to the batch.
    #[allow(clippy::too_many_arguments)]
    fn form_record(
        &self,
        scratch: &mut MemoryContext,
        index: &Relation<'_>,
        tid: &ItemPointerData,
        values: &[datum::Datum],
        isnull: &[bool],
        alive: bool,
        batch: &mut Vec<u8>,
    ) -> PgResult<()> {
        let mut buf =
            nbtree::itup::index_form_tuple(scratch.mcx(), &index.rd_att, values, isnull)?;
        // SAFETY: t_tid = first 6 bytes of the owned image (itup.h).
        unsafe {
            buf.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(*tid);
        }
        let len = buf.size();
        batch.extend_from_slice(&(len as u32).to_ne_bytes());
        batch.push(u8::from(alive));
        // SAFETY: freshly formed live image of `len` bytes.
        batch.extend_from_slice(unsafe { core::slice::from_raw_parts(buf.as_ptr(), len) });
        drop(buf);
        scratch.reset();
        Ok(())
    }
}

/// The bound descriptor's serve: parallel::standing::pool_serve mapped onto
/// the runtime's verdict (the M4.1 adapter shape).
fn btbuild_pooldb_serve(
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
/// membership installed. Owns the EAGER binder wrap (invals_flush target:
/// the bind blankets this thread's caches so the leader's uncommitted
/// catalog rows — the index being built — resolve). Exit-committed unwinds
/// (FATAL) rethrow to the pool glue.
fn btbuild_pool_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(bp) = private.downcast::<BtBuildPool>() else { return };
    // The submit → handle-store window: the publication wake can beat the
    // leader's rg-handle store; wait briefly (a never-stored handle
    // degrades to the drive body's fail-closed refusal).
    let mut spins = 0u32;
    while bp.rg.get().is_none() && spins < 10_000 {
        std::thread::yield_now();
        spins += 1;
    }
    let target = Arc::clone(&bp.target);
    let entered = std::cell::Cell::new(false);
    let r = catch_unwind(AssertUnwindSafe(|| {
        parallel::with_query_task_binding(&target, || {
            entered.set(true);
            pool_build_drive(&bp)
        })
    }));
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if !entered.get() {
                // Binder refusal (validate() gate): fail-closed
                // non-participation — the leader's nobody-participates
                // probe falls back to the serial build.
                btrace(&format!("pool bind refused: {}", e.message()));
                bp.refused.fetch_add(1, SeqCst);
            } else if !bp.failed.load(SeqCst) {
                bp.fail(e);
            }
        }
        Err(unwind) => {
            bp.fail(PgError::new(ERROR, "parallel index build pool executor panicked").into());
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

/// The drive body (the _bt_parallel_build_main analog, pool form): open
/// this participant's relations under the group locks C's worker takes
/// (heap ShareLock, index RowExclusiveLock), build its own IndexInfo, then
/// an external-lane pinned drive claiming block-range morsels. Errors
/// record into the pass (first-wins) and return Err so the binder aborts
/// the worker transaction (resowner releases the residue).
fn pool_build_drive(bp: &Arc<BtBuildPool>) -> PgResult<()> {
    let Some(rt) = runtime::global() else {
        bp.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    let Some(rg) = bp.rg.get().and_then(|w| w.upgrade()) else {
        bp.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    // Process-wide external-lane lease: exhaustion = fail-closed
    // non-participation.
    let Some(lane) = rt.acquire_external_lane() else {
        bp.refused.fetch_add(1, SeqCst);
        return Ok(());
    };
    let mut lane_local = lane.local();

    let ctx = MemoryContext::new("parallel index build pool worker");
    let mcx = ctx.mcx();
    let reap = |e: Box<PgError>,
                rt: &'static Arc<runtime::Runtime>,
                lane_local: &mut runtime::WorkerLocal,
                rg: &runtime::RgHandle| {
        // Clean pre-drive failure: record it and reap the RG (a pinned RG
        // needs SOME driver to run its protocol cleanup).
        bp.fail(e);
        if rg.try_outcome().is_none() {
            rg.abort();
            let _ = rt.drive_pinned(lane_local, rg);
        }
    };
    let heap = match table::table_open(mcx, bp.heap_oid, ShareLock) {
        Ok(rel) => rel,
        Err(e) => {
            reap(e, rt, &mut lane_local, &rg);
            return Ok(());
        }
    };
    let index = match indexam::index_open(mcx, bp.index_oid, RowExclusiveLock) {
        Ok(rel) => rel,
        Err(e) => {
            reap(e, rt, &mut lane_local, &rg);
            let _ = table::table_close(heap, ShareLock);
            return Ok(());
        }
    };
    let mut index_info = match execindexing::BuildIndexInfo(mcx, &index) {
        Ok(ii) => ii,
        Err(e) => {
            reap(e, rt, &mut lane_local, &rg);
            let _ = indexam::index_close(index, RowExclusiveLock);
            let _ = table::table_close(heap, ShareLock);
            return Ok(());
        }
    };
    // The pool arm serves non-concurrent builds only (gated at engage).
    index_info.ii_Concurrent = false;

    bp.started.fetch_add(1, SeqCst);

    let mut cx = BtWorkerCx {
        mcx,
        heap: &heap,
        index: &index,
        index_info,
        scratch: MemoryContext::new_bump("btbuild pool form scratch"),
    };
    // Publish the worker cx for run_morsel (this thread only), drive, clear.
    // SAFETY (lifetime erasure): cx outlives the drive on this frame; the
    // pointer is cleared before cx drops.
    BT_CX.with(|c| {
        c.set(unsafe {
            core::mem::transmute::<*mut BtWorkerCx<'_, '_>, *mut BtWorkerCx<'static, 'static>>(
                &mut cx as *mut BtWorkerCx<'_, '_>,
            )
        })
    });
    let _outcome = rt.drive_pinned(&mut lane_local, &rg);
    BT_CX.with(|c| c.set(std::ptr::null_mut()));
    drop(cx);

    indexam::index_close(index, RowExclusiveLock)?;
    table::table_close(heap, ShareLock)?;

    if bp.failed.load(SeqCst) {
        // A recorded error (possibly this worker's, possibly a sibling's):
        // return Err so the binder aborts the worker transaction and
        // resowner releases any residue (the leader rethrows the recorded
        // error; this message is never user-visible on that path).
        return Err(Box::new(PgError::new(
            ERROR,
            "parallel index build worker failed (see leader error)",
        )));
    }
    Ok(())
}

/// PRIVATE_SHUTDOWN hook: a leader unwind between pool engage and the
/// join's own cleanup reaches here through DestroyParallelContext —
/// complete the RG (drain releases drives parked on the aborted
/// generation) and the standing join (close + await detach) so pool
/// participants never outlive the leader arena.
fn btbuild_pool_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(bp) = private.downcast_ref::<BtBuildPool>() else { return };
    bp.closing.store(true, SeqCst);
    bp.space.notify_all();
    if let Some(rt) = runtime::global() {
        if let Some(rg) = bp.rg.get().and_then(|w| w.upgrade()) {
            if rg.try_outcome().is_none() {
                pool_drain_rg(rt, &rg);
            }
        }
    }
    let board = bp.board.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some(entry) = board {
        parallel::standing::close_and_await(&entry);
    }
}

fn ensure_pool_shutdown_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_private_shutdown(btbuild_pool_shutdown);
    });
}

/// Launched-substrate entrypoint stub: this context never launches
/// bgworkers (there is no gang tier for btree build — census above); the
/// registration only satisfies the context's entrypoint vocabulary.
fn btbuild_pool_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Err(Box::new(PgError::new(
        ERROR,
        "btbuild_pool_main is pool-only (no launched-gang tier exists for btree build)",
    )))
}

fn ensure_entrypoint_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint("btbuild_pool_main", btbuild_pool_main);
    });
}

/// Abort + BOUNDED drain of the pinned RG (the M4.1 drain shape). False =
/// leaked (dead participant) — callers surface an error rather than wait
/// forever.
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

/// plan_create_index_workers analog (C divergences recorded): the heap's
/// parallel_workers reloption short-circuits; otherwise the
/// compute_parallel_worker log-3 ramp over min_parallel_table_scan_size;
/// capped by max_parallel_maintenance_workers. NO maintenance_work_mem
/// split (C divides the sort budget across participants; our sort is
/// leader-owned and undivided) and NO 32MB-per-participant floor for the
/// same reason.
fn compute_build_workers(heap: &Relation<'_>, nblocks: BlockNumber) -> i32 {
    let cap = guc_tables::vars::max_parallel_maintenance_workers.read();
    if cap <= 0 {
        return 0;
    }
    let opt = heap.get_parallel_workers(-1);
    if opt >= 0 {
        return opt.min(cap);
    }
    let min_blocks = guc_tables::vars::min_parallel_table_scan_size.read().max(1) as u64;
    let blocks = nblocks as u64;
    if blocks < min_blocks {
        return 0;
    }
    let mut workers: i32 = 1;
    let mut threshold = min_blocks.saturating_mul(3);
    while workers < cap && blocks >= threshold {
        workers += 1;
        threshold = threshold.saturating_mul(3);
    }
    workers.min(cap)
}

/// The leader-side admission gate: every refusal is fail-closed to the
/// serial build (byte-identical path). Returns the worker count.
fn pool_build_admit(
    heap: &Relation<'_>,
    index_info: &IndexInfo<'_>,
    nblocks: BlockNumber,
) -> Option<i32> {
    if !pool_build_enabled() {
        return None;
    }
    if index_info.ii_Concurrent {
        // RECORDED: C's parallel build serves CIC; this increment scopes
        // it out (module banner).
        btrace("refused: CONCURRENTLY out of scope for the pool arm");
        return None;
    }
    if catalog::IsSystemRelation(heap) {
        btrace("refused: system relation");
        return None;
    }
    if heap.rd_rel.relpersistence == ::types_core::RELPERSISTENCE_TEMP {
        btrace("refused: temp relation");
        return None;
    }
    // C plan_create_index_workers parity: expressions/predicate must be
    // parallel SAFE (walker forced by a non-'s' glob hazard; empty param
    // space).
    for expr in index_info.ii_Expressions.iter() {
        if !clauses::is_parallel_safe(b'u' as i8, true, Vec::new(), expr).unwrap_or(false) {
            btrace("refused: parallel-unsafe index expression");
            return None;
        }
    }
    for pred in index_info.ii_Predicate.iter() {
        if !clauses::is_parallel_safe(b'u' as i8, true, Vec::new(), pred).unwrap_or(false) {
            btrace("refused: parallel-unsafe index predicate");
            return None;
        }
    }
    if nblocks == 0 {
        return None;
    }
    let workers = compute_build_workers(heap, nblocks);
    if workers <= 0 {
        btrace("refused: zero workers (size/guc)");
        return None;
    }
    let policy = parallel::query_task_policy_probe();
    if policy.temp_state || policy.serializable {
        btrace("refused: binder policy probe (temp/serializable)");
        return None;
    }
    Some(workers)
}

/// The pool feed's verdict.
pub(crate) enum PoolFeed {
    /// The pool scanned the heap and every formed tuple was fed into the
    /// leader's spools; `reltuples` is the workers' sum.
    Fed { reltuples: f64 },
    /// Nothing consumed (refusal-class verdicts only): the serial scan
    /// takes over, byte-identically.
    Fallback,
}

/// Engage the pool for the build scan and feed the leader's spools from
/// worker batches. `put` receives (image, alive) for every formed tuple —
/// the caller routes spool1/spool2 and counts, exactly as its serial
/// callback does. Fallback is returned ONLY on refusal-class verdicts
/// (zero tuples consumed — asserted); post-participation failures raise.
pub(crate) fn pool_feed_spools<'mcx>(
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    put: &mut dyn FnMut(&[u8], bool) -> PgResult<()>,
) -> PgResult<PoolFeed> {
    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(heap, ForkNumber::MAIN_FORKNUM)?;
    let Some(nworkers) = pool_build_admit(heap, index_info, nblocks) else {
        return Ok(PoolFeed::Fallback);
    };
    let Some(rt) = runtime::global() else { return Ok(PoolFeed::Fallback) };

    // The serial scan's horizon, hoisted (one procarray read for the whole
    // build; per-tuple conservative — build_scan.rs banner).
    let oldest_xmin = procarray::GetOldestNonRemovableTransactionId(heap)?;
    debug_assert!(oldest_xmin != 0);

    ensure_entrypoint_registered();
    xact::EnterParallelMode();
    let pcxt = match parallel::CreateParallelContext("postgres", "btbuild_pool_main", nworkers) {
        Ok(p) => p,
        Err(e) => {
            xact::ExitParallelMode();
            return Err(e);
        }
    };
    // Everything past here tears down through `teardown` (leader-side
    // paths) or the PRIVATE_SHUTDOWN hook (unwinds inside Destroy).
    let engage = || -> PgResult<Option<(Arc<BtBuildPool>, Arc<parallel::standing::StandingEngagement>)>> {
        parallel::InitializeParallelDSM(pcxt)?;
        let policy0 = parallel::query_task_policy_probe();
        let policy = parallel::QueryTaskBindingPolicy {
            has_params: false,
            temp_state: false,
            serializable: false,
            pending_invalidations: policy0.pending_invalidations,
            // The M4.2 opt-in: index_build ALWAYS carries its own
            // uncommitted catalog rows — binds blanket + taint (module
            // banner).
            invals_flush: true,
        };
        if parallel::InstallQueryTaskBinding(pcxt, policy).is_err() {
            btrace("pool channel refused (binding install)");
            return Ok(None);
        }
        parallel::set_standing_driver(
            pcxt,
            parallel::standing::StandingDriver {
                drive: btbuild_pool_driver,
                deferred_bind: false,
            },
        );
        ensure_pool_shutdown_registered();
        let target = parallel::shared_for(pcxt);
        let Some(entry) = parallel::standing::try_engage_pool(&target, nworkers as usize)
        else {
            btrace("pool channel refused (try_engage_pool)");
            return Ok(None);
        };
        let bp = Arc::new(BtBuildPool {
            heap_oid: heap.rd_id,
            index_oid: index.rd_id,
            oldest_xmin,
            leader_proc: init_small::globals::MyProcNumber(),
            target,
            rg: OnceLock::new(),
            board: Mutex::new(Some(Arc::clone(&entry))),
            queue: Mutex::new(std::collections::VecDeque::new()),
            queued_bytes: AtomicUsize::new(0),
            space: Condvar::new(),
            started: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            error: Mutex::new(None),
            failed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            broken_hot_chain: AtomicBool::new(false),
            reltuples: Mutex::new(0.0),
        });
        parallel::set_private(pcxt, Arc::clone(&bp) as Arc<dyn std::any::Any + Send + Sync>);
        Ok(Some((bp, entry)))
    };

    let engaged = match engage() {
        Ok(Some(v)) => v,
        Ok(None) => {
            let _ = parallel::DestroyParallelContext(pcxt);
            xact::ExitParallelMode();
            return Ok(PoolFeed::Fallback);
        }
        Err(e) => {
            let _ = parallel::DestroyParallelContext(pcxt);
            xact::ExitParallelMode();
            return Err(e);
        }
    };
    let (bp, entry) = engaged;

    static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
    let source: Arc<dyn runtime::MorselSource> =
        Arc::new(BtBlockSource { nblocks: nblocks as u64 });
    let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(&bp) as _;
    let descriptor = runtime::BoundDescriptor {
        serve: btbuild_pooldb_serve,
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
    bp.rg
        .set(rg.downgrade())
        .unwrap_or_else(|_| unreachable!("rg set once per pool build"));
    btrace(&format!(
        "engaged pass tickets={nworkers} blocks={nblocks} unique={}",
        index_info.ii_Unique
    ));

    let verdict = pool_leader_join(rt, &bp, &entry, &rg, &waiter, put);

    // Teardown (every leader path): the join already closed the board and
    // completed the RG; Destroy runs the shutdown hook only for state the
    // join could not reach (unwind coverage), then drops the context.
    let destroy = parallel::DestroyParallelContext(pcxt);
    xact::ExitParallelMode();
    let verdict = verdict?;
    destroy?;

    match verdict {
        PoolJoin::Done => {
            if bp.broken_hot_chain.load(SeqCst) {
                index_info.ii_BrokenHotChain = true;
            }
            let reltuples = *bp.reltuples.lock().unwrap_or_else(|p| p.into_inner());
            btrace(&format!("done reltuples={reltuples}"));
            Ok(PoolFeed::Fed { reltuples })
        }
        PoolJoin::Fallback => {
            btrace("serial fallback (nothing consumed)");
            Ok(PoolFeed::Fallback)
        }
    }
}

enum PoolJoin {
    Done,
    /// Nothing claimed/consumed: statuses untouched — serial takes over.
    Fallback,
}

/// The leader's join loop: drain formed-tuple batches into the caller's
/// spools between completion/interrupt/fallback-verdict polls (the M4.1
/// pool_leader_join shape with the batch drain as the leader's duty).
/// Every exit path completes the RG if needed, closes the board entry,
/// awaits detach.
fn pool_leader_join(
    rt: &'static Arc<runtime::Runtime>,
    bp: &Arc<BtBuildPool>,
    entry: &Arc<parallel::standing::StandingEngagement>,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
    put: &mut dyn FnMut(&[u8], bool) -> PgResult<()>,
) -> PgResult<PoolJoin> {
    let mut consumed = 0u64;
    let cleanup = |bp: &Arc<BtBuildPool>| {
        bp.closing.store(true, SeqCst);
        bp.space.notify_all();
        let board = bp.board.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(entry) = board {
            parallel::standing::close_and_await(&entry);
        }
    };
    // Drain everything currently queued into the caller's spools.
    let mut drain = |bp: &Arc<BtBuildPool>, consumed: &mut u64| -> PgResult<bool> {
        let mut any = false;
        loop {
            let batch = {
                let mut q = bp.queue.lock().unwrap_or_else(|p| p.into_inner());
                q.pop_front()
            };
            let Some(batch) = batch else { break };
            bp.queued_bytes.fetch_sub(batch.len(), SeqCst);
            bp.space.notify_all();
            any = true;
            let mut off = 0usize;
            while off < batch.len() {
                let len =
                    u32::from_ne_bytes(batch[off..off + 4].try_into().expect("record header"))
                        as usize;
                let alive = batch[off + 4] != 0;
                let image = &batch[off + 5..off + 5 + len];
                put(image, alive)?;
                *consumed += 1;
                off += 5 + len;
            }
        }
        Ok(any)
    };
    // DST P2: the claim deadline is BEHAVIORAL (changes execution path) —
    // its clock is pg_clock (the standing_channel precedent).
    let t0 = pg_clock::MonoStamp::now();
    let mut traced = false;
    loop {
        // A put error is a REAL build error (leader-side sort): abort the
        // pass and raise.
        match drain(bp, &mut consumed) {
            Ok(_any) => {}
            Err(e) => {
                pool_drain_rg(rt, rg);
                cleanup(bp);
                return Err(e);
            }
        }
        if let Some(o) = waiter.try_wait() {
            // Final drain AFTER the outcome: all worker pushes
            // happen-before their claims complete, which happen-before
            // the RG outcome.
            let final_drain = drain(bp, &mut consumed);
            cleanup(bp);
            if let Err(e) = final_drain {
                return Err(e);
            }
            if !traced {
                btrace(&format!("engaged pooldb dop={}", entry.claimed()));
            }
            if let Some(e) = bp.take_error() {
                return Err(e);
            }
            if o == runtime::RgOutcome::Aborted {
                postgres_seams::check_for_interrupts::call()?;
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel index build pool pass aborted",
                )));
            }
            if bp.started.load(SeqCst) == 0 {
                // Completed but nobody participated (only possible
                // pre-claim): refusal-class fallback, nothing consumed.
                debug_assert_eq!(consumed, 0, "fallback with consumed tuples");
                return Ok(PoolJoin::Fallback);
            }
            btrace(&format!("consumed tuples={consumed}"));
            return Ok(PoolJoin::Done);
        }
        // Order matters on every error exit: abort THEN drain (the
        // leader's protocol cleanup completes the RG and releases workers
        // parked in their drives) THEN close-and-await.
        if let Err(e) = postgres_seams::check_for_interrupts::call() {
            bp.closing.store(true, SeqCst);
            bp.space.notify_all();
            pool_drain_rg(rt, rg);
            cleanup(bp);
            return Err(e);
        }
        let claimed = entry.claimed();
        if !traced && claimed > 0 {
            btrace(&format!("engaged pooldb dop={claimed}"));
            traced = true;
        }
        let started = bp.started.load(SeqCst);
        let refused = entry.refused() + bp.refused.load(SeqCst);
        if started == 0 && refused >= entry.tickets() {
            pool_drain_rg(rt, rg);
            cleanup(bp);
            btrace(&format!("pooldb refused ({refused} refusals) — serial fallback"));
            debug_assert_eq!(consumed, 0, "fallback with consumed tuples");
            return Ok(PoolJoin::Fallback);
        }
        if started == 0
            && entry.detached() >= claimed
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > pool_claim_deadline()
        {
            pool_drain_rg(rt, rg);
            cleanup(bp);
            btrace("pooldb claim deadline — serial fallback");
            debug_assert_eq!(consumed, 0, "fallback with consumed tuples");
            return Ok(PoolJoin::Fallback);
        }
        if claimed > 0 && started > 0 && entry.detached() >= claimed {
            if let Some(_o) = waiter.try_wait() {
                continue;
            }
            if let Some(e) = bp.take_error() {
                bp.closing.store(true, SeqCst);
                bp.space.notify_all();
                pool_drain_rg(rt, rg);
                cleanup(bp);
                return Err(e);
            }
            bp.closing.store(true, SeqCst);
            bp.space.notify_all();
            pool_drain_rg(rt, rg);
            cleanup(bp);
            return Err(Box::new(PgError::new(
                ERROR,
                "parallel index build pool executors exited before completing the scan",
            )));
        }
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            bp.closing.store(true, SeqCst);
            bp.space.notify_all();
            pool_drain_rg(rt, rg);
            cleanup(bp);
            return Err(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Record encode/decode roundtrip: the leader's parse must recover
    /// exactly what form_record's encoder appended (u32 len | u8 alive |
    /// image), across mixed sizes and flags.
    #[test]
    fn batch_record_roundtrip() {
        let mut batch: Vec<u8> = Vec::new();
        let images: Vec<(Vec<u8>, bool)> = vec![
            ((0u8..16).collect(), true),
            ((0u8..255).collect(), false),
            (vec![7u8; 1], true),
        ];
        for (img, alive) in &images {
            batch.extend_from_slice(&(img.len() as u32).to_ne_bytes());
            batch.push(u8::from(*alive));
            batch.extend_from_slice(img);
        }
        let mut got: Vec<(Vec<u8>, bool)> = Vec::new();
        let mut off = 0usize;
        while off < batch.len() {
            let len = u32::from_ne_bytes(batch[off..off + 4].try_into().unwrap()) as usize;
            let alive = batch[off + 4] != 0;
            got.push((batch[off + 5..off + 5 + len].to_vec(), alive));
            off += 5 + len;
        }
        assert_eq!(off, batch.len());
        assert_eq!(got, images);
    }

    /// Block-source geometry: one granule per heap block, no interior
    /// boundaries (the sizer owns claim shaping), default ramp seed.
    #[test]
    fn block_source_geometry() {
        use runtime::MorselSource as _;
        let s = BtBlockSource { nblocks: 1000 };
        assert_eq!(s.total_granules(), 1000);
        assert_eq!(s.next_boundary_after(0), 1000, "no interior boundaries");
        assert!(!s.whole_boundary_claims());
    }

    /// compute_build_workers ramp shape (log-3 over the scan-size floor,
    /// reloption short-circuit exercised in the e2e where a real Relation
    /// exists). Pure arithmetic here via the guc defaults contract is not
    /// testable without a store; covered by the battery's DOP witness.
    #[test]
    fn worker_ramp_is_monotone_in_blocks() {
        // Shape check on the raw ramp math (min_blocks = 1024 boot
        // default): 3x block growth adds at most one worker.
        let min_blocks: u64 = 1024;
        let ramp = |blocks: u64, cap: i32| -> i32 {
            if blocks < min_blocks {
                return 0;
            }
            let mut w = 1;
            let mut th = min_blocks * 3;
            while w < cap && blocks >= th {
                w += 1;
                th *= 3;
            }
            w
        };
        assert_eq!(ramp(1023, 8), 0);
        assert_eq!(ramp(1024, 8), 1);
        assert_eq!(ramp(3 * 1024, 8), 2);
        assert_eq!(ramp(9 * 1024, 8), 3);
        assert_eq!(ramp(9 * 1024, 2), 2, "cap binds");
    }

    /// ARMED-WITNESS leg (scripts/armed-witness-e2e.sh registry row
    /// `indexbuild-pool`): the arming env must TAKE through every
    /// once-cached gate on the M4.2 kill-switch chain — the
    /// silent-no-witness failure mode the registry exists to kill.
    /// Engagement itself is witnessed by the server-level battery
    /// (scripts/indexbuild-pool-e2e.sh). Unarmed it self-SKIPs, so the
    /// default units leg stays byte-identical.
    #[test]
    fn armed_pool_kill_switch_chain_takes() {
        if std::env::var("PGRUST_RUNTIME_INDEXBUILD_POOL").as_deref() != Ok("1") {
            eprintln!(
                "SKIP: armed_pool_kill_switch_chain_takes (PGRUST_RUNTIME_INDEXBUILD_POOL unset)"
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
            pool_build_enabled(),
            "the index-build pool gate admits under the armed env"
        );
    }

    /// Default posture: every knob on the M4.2 chain defaults to the OFF/
    /// calibrated state (self-skips under explicit overrides — the armed
    /// legs set them).
    #[test]
    fn pool_gates_default_posture() {
        if std::env::var("PGRUST_RUNTIME_INDEXBUILD_POOL").is_ok()
            || std::env::var("PGRUST_RUNTIME_INDEXBUILD_BATCH_KB").is_ok()
            || std::env::var("PGRUST_RUNTIME_INDEXBUILD_QUEUE_MB").is_ok()
        {
            eprintln!("SKIP: pool_gates_default_posture (env overrides present)");
            return;
        }
        assert!(!pool_build_enabled(), "kill switch DEFAULT OFF");
        assert_eq!(batch_bytes(), 256 * 1024, "batch default of record");
        assert_eq!(queue_cap(), 16 * 1024 * 1024, "queue cap default of record");
    }
}
