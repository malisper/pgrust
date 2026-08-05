//! Runtime PLAIN exact-DISTINCT sink (band-2b) — DOP-parallel
//! `Aggregate(AGG_PLAIN, all-DISTINCT) → [Sort →] SeqScan(pgrcolumnar)`, the
//! single-key count(DISTINCT) class (`COUNT(DISTINCT UserID)` / `COUNT(DISTINCT SearchPhrase)`).
//!
//! Why this arm exists: the planner-parallel plan for this shape is
//! `Aggregate ← Gather Merge ← Sort ← Parallel Seq Scan` — every input row
//! ships through the tuple queue and the leader runs the presorted distinct
//! transition serially. The dop16-bandwidth study classified that arm
//! INSTRUCTION-WASTE (the int-key shape at dop16 executes ~9x the serial instructions:
//! per-worker duplication + queue spin), so the fix is routing the shape to
//! a runtime sink over the SERIAL plan, not patching the legacy path.
//!
//! Structure (the grouped `runtime_distinct` sink's skeleton, value-hash
//! partitioned instead of group-hash — nodeagg::plainpd):
//!   ACCEPT   granule-morsel scans in bgworker helpers; each worker stages
//!            the key column (the serial c3bace548 direct-key kernels:
//!            int key lane / dict-coded text window / collected varlena
//!            batch) and inserts into its PLAIN_PD_PARTS value-partitioned
//!            DistinctSets.
//!   SEAL     a plain move (partitioning happened at insert).
//!   COMBINE  one claim per value partition: union every worker's slice
//!            (disjoint across partitions by construction).
//!   ADOPT    the leader concatenates merged partitions into one replay-only
//!            set (`DistinctSet::from_values`), installs it into the plain
//!            agg's set-mode pertrans slot, and the ordinary
//!            `agg_plain_finish` replay (count shortcut included) finishes
//!            the node — HAVING/projection byte-identical to the serial arm.
//!
//! Value identity: the plainpd module doc carries the argument (serial
//! set-mode admission + order-insensitive replay). Every refusal and the
//! budget-crossing path fall back to the serial drives, value-identically.
//!
//! v1 envelope (fail-closed): single set transition on outer column 0,
//! leader-proven direct key staging, no spill (a worker budget crossing
//! aborts to the serial rerun), EXPLAIN ANALYZE refuses (the serial arm
//! serves EA — transparency records the gate).
//!
//! Kill switch: `PGRUST_RUNTIME_PLAINDISTINCT=0`. Arming rides the M5-1
//! router's `ArmClass::Distinct` (this is the distinct-sink family): the
//! bench GUC `pgrust.runtime_distinct_pool` verbatim when set, else
//! engine=runtime at `pgrust.runtime_dop`.

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::nodeagg::plainpd::{
    plain_pd_combine, plain_pd_combine_steal, plain_pd_derive_spec, PlainPdLocal, PlainPdMerged,
    PlainPdSealed, PlainPdSpec, PLAIN_PD_PARTS,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::runtime_instr;
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, trace_feed};
use super::{drain_pipeline, BatchEmit, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};

// ---------------------------------------------------------------------------
// Shared state (the runtime_distinct discipline: one struct, one Arc).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

struct RuntimePlainDistinctShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    spec: Arc<PlainPdSpec>,
    /// SE-T2AGG CAR A: the SELECT-DISTINCT sub-arm (emit the merged VALUES;
    /// the worker key staging may sit at an arbitrary scan column —
    /// `spec.att`). False = the historical count(DISTINCT) arm, untouched.
    sd_values: bool,
    /// GL-LOWDIST-1: this engagement's combine takes the size-asymmetric
    /// low-width path (knob ON and resolved dop within the band bound —
    /// decided once at engage; see runtime_distinct::distinct_lowwidth_maxdop).
    lowwidth: bool,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (drop-guard bumped; the
    /// leader's liveness-reap input).
    exited: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// A worker budget crossed (or a non-staged row surfaced): NOT an error —
    /// the RG aborts and the leader reruns the serial arm (R5 phase 1).
    crossed: AtomicBool,
    /// Combine-phase retained bytes vs the admitted envelope (forked Locals ×
    /// worker_budget — the grouped sink's law).
    merged_bytes: AtomicUsize,
    /// Combine output cells, one per value partition (single writer each).
    out: Vec<UnsafeCell<Option<PlainPdMerged>>>,
    /// The published merged result: partition outputs + OR-reduced seen_null.
    merged: Mutex<Option<(Vec<PlainPdMerged>, bool)>>,
    /// M2 inc-1 standing channel: the live board entry, held for the
    /// PRIVATE_SHUTDOWN standing join (standing_channel, scan discipline).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
}

// SAFETY: (i) each `out` cell has a single writer (the sink contract visits
// every partition exactly once) and is read only by `finalize`, which the
// runtime's last-worker-out orders after every combine; (ii) PlainPdMerged
// is owned plain data; (iii) everything else is Send/Sync by composition.
unsafe impl Send for RuntimePlainDistinctShared {}
unsafe impl Sync for RuntimePlainDistinctShared {}

impl RuntimePlainDistinctShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    /// Bounded-memory / unstaged-shape fallback: abort the RG; the leader
    /// observes `crossed` and reruns the serial arm.
    fn cross(&self, why: &'static str) {
        lane_trace(&format!("runtime-plaindistinct: crossing to serial ({why})"));
        self.crossed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn take_merged(&self) -> Option<(Vec<PlainPdMerged>, bool)> {
        self.merged.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimePlainDistinctShared {
    type Local = PlainPdLocal;
    type Sealed = PlainPdSealed;

    fn fork(&self, _worker: usize) -> PlainPdLocal {
        PlainPdLocal::new(self.spec.worker_budget)
    }

    fn accept_local(&self, local: &mut PlainPdLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime plain-distinct worker panicked in a morsel")
                        .into(),
                );
            }
        }
    }

    fn seal(&self, _worker: usize, local: PlainPdLocal) -> PlainPdSealed {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return PlainPdSealed::empty();
        }
        local.seal()
    }

    fn partitions(&self) -> u64 {
        PLAIN_PD_PARTS as u64
    }

    fn combine(&self, part: u64, sealed: &[PlainPdSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| {
            if self.lowwidth {
                plain_pd_combine_steal(self.spec.is_bytes(), part as usize, sealed)
            } else {
                plain_pd_combine(self.spec.is_bytes(), part as usize, sealed)
            }
        }));
        match r {
            Ok(m) => {
                // R3 envelope (the grouped sink's law): merged results are
                // retained until the leader adopts — meter against forked
                // Locals × worker_budget; crossing takes the serial fallback.
                let b = m.mem_bytes();
                let total = self.merged_bytes.fetch_add(b, Ordering::Relaxed) + b;
                if total > self.spec.worker_budget.saturating_mul(sealed.len().max(1)) {
                    self.cross("combine envelope");
                    return;
                }
                // SAFETY: partition `part` is handed to this claimer alone
                // (sink contract); finalize reads happen-after every combine.
                unsafe { *self.out[part as usize].get() = Some(m) };
            }
            Err(_panic) => {
                self.fail(
                    PgError::new(ERROR, "runtime plain-distinct worker panicked in combine")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self, sealed: &[PlainPdSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        // SAFETY: single-threaded under last-worker-out, after every combine.
        let parts: Vec<PlainPdMerged> =
            self.out.iter().filter_map(|c| unsafe { (*c.get()).take() }).collect();
        let seen_null = sealed.iter().any(|s| s.seen_null());
        *self.merged.lock().unwrap_or_else(|p| p.into_inner()) = Some((parts, seen_null));
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    /// Per-helper detoast scratch (reset per batch when the set is bytes).
    tmp: EcxtId,
    reset_tmp: bool,
    /// The worker scan's own direct-key staging verdict (leader-proven shape;
    /// re-proven per worker executor — a mismatch crosses to serial).
    key_direct: bool,
    key_bytes: bool,
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// The per-morsel accept feed: staged key windows into the worker's
/// partitioned sets. The serial `PlainDistinctAggBuildSink`'s staged reads,
/// retargeted at `PlainPdLocal`; any row the staging cannot serve crosses
/// the engagement to the serial fallback (v1 fail-closed — pgrcolumnar stages
/// every row of a covered key column).
struct PlainAcceptSink<'a> {
    local: &'a mut PlainPdLocal,
    tmp: EcxtId,
    reset_tmp: bool,
    key_bytes: bool,
    kind_i16: bool,
    kind_i32: bool,
    keys: Vec<::datum::Datum>,
    dict_memo: Vec<u64>,
    dict_ident: Option<(bool, u64)>,
    crossed: bool,
}

impl<'mcx> Sink<'mcx> for PlainAcceptSink<'_> {
    fn accept(&mut self, _tuple: ExecSlotId, _estate: &mut EStateData<'mcx>) -> PgResult<SinkFeed> {
        // v1: the direct staged-key feed is the admission (no per-row arm in
        // the workers). A per-row delivery means the staging disengaged —
        // cross to the serial rerun, value-safely.
        self.crossed = true;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PlainAcceptSink<'_> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if self.crossed || self.local.crossed() {
            self.crossed = true;
            return Ok(());
        }
        // Dict-coded text window (the serial drive's zero-decode lane).
        if self.key_bytes {
            if let Some(lane) = emit.key_dict_lane() {
                let t = lane.table;
                let global = t.has_stitch();
                let (ident, size) = if global {
                    ((true, t.gepoch), t.gndv as usize)
                } else {
                    ((false, t.epoch), t.ndict as usize)
                };
                if self.dict_ident != Some(ident) {
                    self.dict_memo.clear();
                    self.dict_memo.resize(size.div_ceil(64), 0);
                    self.dict_ident = Some(ident);
                }
                if t.lazy_ensure.is_some() {
                    for i in pos as usize..n as usize {
                        // SAFETY: the lane covers the staged window's rows.
                        t.ensure_code(unsafe { *lane.codes.add(i) });
                    }
                }
                // SAFETY: the lane covers the staged window's `n` rows and
                // `ndict` dict entries (the fill's contract); consumed
                // before the next window stages.
                let (codes, dict, stitch) = unsafe {
                    (
                        core::slice::from_raw_parts(lane.codes, n as usize),
                        core::slice::from_raw_parts(t.dict, t.ndict as usize),
                        global.then(|| core::slice::from_raw_parts(t.stitch, t.ndict as usize)),
                    )
                };
                self.local.accept_dict_window(
                    estate,
                    self.tmp,
                    &codes[pos as usize..],
                    dict,
                    stitch,
                    &mut self.dict_memo,
                )?;
                if self.reset_tmp {
                    estate.reset_expr_context(self.tmp);
                }
                return Ok(());
            }
        }
        // Integer key lane, whole-slice (the int-distinct hot path).
        if !self.key_bytes {
            if let Some((vals, isnull, fb)) = emit.topk_key_lane(n) {
                if fb.iter().all(|&w| w == 0) {
                    self.local.accept_lane_ints(
                        self.kind_i16,
                        self.kind_i32,
                        &vals[pos as usize..],
                        &isnull[pos as usize..],
                    );
                    return Ok(());
                }
            }
        }
        // Collected staged-key batch (both kinds).
        self.keys.clear();
        let mut saw_null = false;
        for i in pos..n {
            match emit.emit_key(i) {
                Some((d, false)) => self.keys.push(d),
                Some((_, true)) => saw_null = true,
                None => {
                    // A row the key staging cannot serve: cross to serial
                    // (v1 fail-closed; pgrcolumnar covered-key scans stage all).
                    self.crossed = true;
                    return Ok(());
                }
            }
        }
        if self.key_bytes {
            self.local.accept_bytes_datums(estate, self.tmp, &self.keys, saw_null)?;
            if self.reset_tmp {
                estate.reset_expr_context(self.tmp);
            }
        } else {
            self.local.accept_datums_int(self.kind_i16, self.kind_i32, &self.keys, saw_null);
        }
        Ok(())
    }
}

impl RuntimePlainDistinctShared {
    fn morsel_body(
        &self,
        local: &mut PlainPdLocal,
        _worker: usize,
        range: runtime::MorselRange,
    ) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime plain-distinct morsel without a bound executor",
                )));
            };
            if !ex.key_direct {
                // The worker executor could not arm the direct key staging
                // the leader proved — cross, serial rerun.
                self.cross("worker key staging disengaged");
                return Ok(());
            }
            let (tmp, reset_tmp) = (ex.tmp, ex.reset_tmp);
            let key_bytes = ex.key_bytes;
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime plain-distinct worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = plain_worker_scan(d.planstate.as_mut())?;
                    ::nodeseqscan::seq_scan_set_morsel_range(ss, estate, range.start, range.end)?;
                    let mut sink = PlainAcceptSink {
                        local,
                        tmp,
                        reset_tmp,
                        key_bytes,
                        kind_i16: self.spec.kind_is_i16(),
                        kind_i32: self.spec.kind_is_i32(),
                        keys: Vec::new(),
                        dict_memo: Vec::new(),
                        dict_ident: None,
                        crossed: false,
                    };
                    let fed = drain_pipeline(
                        ss,
                        &mut SeqScanSource,
                        &mut SeqScanFilterProject,
                        &mut sink,
                        estate,
                    );
                    let crossed = sink.crossed;
                    fed?;
                    if crossed {
                        self.cross("unstaged rows in worker feed");
                    } else if local.crossed() {
                        self.cross("worker budget");
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the Agg
/// or the Sort; the worker pstmt's planTree is the SeqScan node).
fn plain_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime plain-distinct worker pstmt is not a bare SeqScan",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper (bgworker) plumbing — the runtime_distinct hooks, retargeted.
// ---------------------------------------------------------------------------

fn runtime_plaindistinct_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_plaindistinct_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        return;
    };
    let Ok(payload) = private.downcast::<RuntimePlainDistinctShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit
    // path. HOOK-frame placement (the scan arm's law): the standing driver
    // reuses helper_drive and must NOT bump — standing exits ride the
    // board's claimed/detached accounting.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(&payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime plain-distinct helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): the
/// POST_TASK_PARK body minus the ExitBump; exit-committed unwinds (FATAL)
/// rethrow to the gang glue (a terminated worker must die).
fn runtime_plaindistinct_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimePlainDistinctShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(&payload)));
    if let Err(unwind) = r {
        payload.fail(
            PgError::new(ERROR, "runtime plain-distinct standing executor panicked").into(),
        );
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn runtime_plaindistinct_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimePlainDistinctShared>() else { return };
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    // Standing channel (M2 inc-1): complete the standing join on leader
    // unwind paths (standing_channel::shutdown_standing_join).
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg(payload.rt, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_plaindistinct_main",
            runtime_plaindistinct_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_plaindistinct_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_plaindistinct_private_shutdown);
    });
}

fn helper_drive(payload: &Arc<RuntimePlainDistinctShared>) {
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-plaindistinct: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-plaindistinct: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-plaindistinct: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, &mut lane.borrow_mut())
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                payload.fail(e);
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!(
                    "runtime-plaindistinct: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimePlainDistinctShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _end = super::standing_channel::drive_pool_serve(&payload.rt, local, rg, lane);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // The binder abort-path law (m2-integration): route a released (not
        // finished) executor through the transaction-ABORT unbind.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime plain-distinct worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

fn build_worker_exec(payload: &Arc<RuntimePlainDistinctShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<(EcxtId, bool, bool)> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime plain-distinct worker ExecutorStart");
                x.with_mut(|d| -> PgResult<(EcxtId, bool, bool)> {
                    let estate = &mut d.estate;
                    let ss = plain_worker_scan(d.planstate.as_mut())?;
                    // The serial drive's direct-key staging probe, per worker
                    // executor (arming decides staging). The explicit-attnum
                    // arm (the unprojected physical-tlist shape — the leader
                    // proved the same ladder) serves the SE-T2AGG CAR A
                    // SELECT-DISTINCT sub-arm AND the GL-DISTALPHA-2
                    // PRESORTED-bare count(DISTINCT) face; on projected
                    // scans it refuses by construction (ProjInfo present),
                    // so historical projected engagements are byte-untouched.
                    let key_direct = ::nodeseqscan::seq_scan_sortkey_direct(ss, estate)
                        || ::nodeseqscan::seq_scan_key_direct_att(
                            ss,
                            estate,
                            payload.spec.att,
                        );
                    let key_bytes = key_direct && payload.spec.is_bytes();
                    if key_bytes && ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
                        // Arm the dict-coded key lane when the bank serves it
                        // (the serial drive's own arming call; columnar-only
                        // — heap text rides the collected emit_key batch,
                        // GL-LOWDIST-4 B1).
                        let _ = ::nodeseqscan::seq_scan_key_dict_arm(ss);
                    }
                    Ok((estate.exec_assign_expr_context(), key_direct, key_bytes))
                })
            })
        })();
        match armed {
            Ok((tmp, key_direct, key_bytes)) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    tmp,
                    reset_tmp: payload.spec.is_bytes(),
                    key_direct,
                    key_bytes,
                    errored: std::cell::Cell::new(false),
                });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

// ---------------------------------------------------------------------------
// Leader-side engagement.
// ---------------------------------------------------------------------------

/// `PGRUST_RUNTIME_PLAINDISTINCT=0` kill switch (default ON when the
/// distinct pool arms).
fn plaindistinct_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || std::env::var("PGRUST_RUNTIME_PLAINDISTINCT").as_deref() != Ok("0"))
}

/// SE-T2AGG CAR A arm switch (`PGRUST_LANE_V2_DISTINCT_PLAINSHAPE`, DEFAULT
/// ON since t36 flips2; `=0|off` is the kill — the flipped-kill idiom).
/// FLIP EVIDENCE (GL-T2C FLIP-RECOMMENDED, 2026-07-21, tier2 campaign @
/// 7d8aa9a2b): bare SELECT DISTINCT int 0.164s (6.1x win) / text 0.195s
/// (5.1x win) at 10M/2M, md5 parity every leg, OFF arm inert, GROUP BY
/// control flat, wrapped/hasAggs forms correctly shape-refused; measured
/// in the engagement region (dop 12, floors disabled symmetrically) — the
/// production floor guard (min_dop 12 / low-dop<=3M) bounds exposure.
/// SAME spelling as the m5_suppress probe half
/// (`distinct_plainshape_enabled`) — both read sites flip together
/// (knob-coherence law); the family kill `PGRUST_RUNTIME_PLAINDISTINCT=0`
/// above still disarms everything.
fn selectdistinct_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCT_PLAINSHAPE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

#[cold]
fn refused(estate: &mut EStateData<'_>, ea: bool, node_id: i32, reason: &'static str) {
    lane_trace(&format!("runtime-plaindistinct: refused ({reason})"));
    if ea {
        estate.runtime_ea_record_refusal(node_id, "plain-distinct", reason);
    }
}

/// The runtime plain-distinct arm, probed from the serial plain-distinct
/// drives BEFORE they engage (over-Sort and over-SeqScan shapes). `None` =
/// refused or fell back (nothing consumed; the serial arms run
/// byte-identically). `Some(row)` = the arm owns the node (merged set
/// installed; `agg_plain_finish` produced the row).
///
/// `ss` is the scan under the (skipped) Sort — or the bare scan child. The
/// caller has already applied its own shape gates (`agg_plain_distinct_set_only`
/// / set admissibility + sort fusibility); the gates here are the sink's own.
pub(super) fn try_own_plain_distinct_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
    force_set: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source, like every sibling arm (bench GUC
    // verbatim when set; else engine=runtime arms at pgrust.runtime_dop;
    // else 0 = today's path). Reading the bench GUC alone left this arm
    // UNARMED in every engine=runtime session with the pool GUC unset — the
    // probe suppressed the Gather and execution landed SERIAL (the plain-distinct
    // suppress-then-unarmed class, night/routing-floor-fixes).
    let dop = super::router::arm_dop(super::router::ArmClass::Distinct);
    if dop <= 0 || !runtime::runtime_enabled() || !plaindistinct_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    lane_trace("runtime-plaindistinct: probed");

    let ea = runtime_instr::ea_active(estate);
    let node_id = agg.plan.plan.plan_node_id;

    // v1: EXPLAIN ANALYZE refuses (the serial arm serves EA, value-identically;
    // the transparency record names this gate).
    if ea {
        refused(estate, true, node_id, "ea not threaded (plain-distinct v1)");
        return Ok(None);
    }
    // GL-LOWDIST-4 B1: heap seq scans admit under the knob (the columnar
    // dict/int lanes fall back to the collected emit_key batch on heap).
    if !seq_scan_fusible(ss, estate)?
        || !(::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
            || (::nodeseqscan::seq_scan_is_heap(ss)
                && super::runtime_distinct::distinct_heap_enabled()))
    {
        refused(estate, ea, node_id, "scan not fusible/cbstore");
        return Ok(None);
    }
    if estate.es_epq_active {
        return Ok(None);
    }
    if super::runtime_in_parallel_machinery(ss) {
        refused(estate, ea, node_id, "already in parallel machinery");
        return Ok(None);
    }
    let Some(spec) = plain_pd_derive_spec(agg) else {
        refused(estate, ea, node_id, "spec derivation");
        return Ok(None);
    };
    // The direct staged-key feed is the whole accept path: prove it on the
    // leader's scan (workers re-prove on their own executors) — the sortkey
    // resolution for projected single-column shapes; the explicit-attnum
    // arm (GL-DISTALPHA-2, the sd sub-arm's own ladder) for the unprojected
    // physical-tlist PRESORTED-bare face, where `spec.att` is the scan
    // column by the physical-tlist identity `key_direct_att` itself
    // enforces (it refuses any projected scan).
    if !::nodeseqscan::seq_scan_sortkey_direct(ss, estate)
        && !::nodeseqscan::seq_scan_key_direct_att(ss, estate, spec.att)
    {
        refused(estate, ea, node_id, "key staging not direct");
        return Ok(None);
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused(estate, ea, node_id, "extern params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused(estate, ea, node_id, "exec params");
        return Ok(None);
    }
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused(estate, ea, node_id, "scan node tag");
        return Ok(None);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused(estate, ea, node_id, "parallel-unsafe scan exprs");
        return Ok(None);
    }
    if !estate.es_snapshot.as_deref().is_some_and(::types_snapshot::IsMVCCSnapshot) {
        refused(estate, ea, node_id, "non-MVCC snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        refused(estate, ea, node_id, "binder policy sources");
        return Ok(None);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, source)) = super::runtime_distinct::distinct_task_source(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused(estate, ea, node_id, "granule floor");
        return Ok(None);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, total_granules);
    // GL-LOWDIST-1 leader-parity bump (knob-gated, band-only).
    let (dop, lowwidth) =
        super::runtime_distinct::lowwidth_leader_parity_dop(rt, dop, "runtime-plaindistinct");
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, lowwidth, total_granules, source, spec, scan_node, force_set, false)
}

/// SE-T2AGG CAR A: the plain SELECT-DISTINCT sub-arm — `Agg(AGG_HASHED,
/// zero aggregates) → SeqScan(pgrcolumnar)`, the shape the serial hash-agg
/// breaker owns today (matrix row distinct-plain-shape: "UNKEYED until the
/// hash shape is wired"). Reuses this module's whole worker/combine
/// pipeline (the distinct VALUES are the answer); the adopt tail publishes
/// the merged values as the sink's emit buffers instead of installing a
/// count(DISTINCT) set. `Ok(false)` = refused or fell back (nothing
/// consumed; the serial arms run byte-identically). `Ok(true)` = the arm
/// owns the node (emit adopted OR the node was already drained) — the
/// caller's own pull drains `agg_retrieve_hash_table`'s sink branch.
pub(super) fn try_own_plain_selectdistinct_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Cheap static gates first: the sub-arm knob (default OFF), the family
    // kill, and the zero-aggregate shape precheck (per-pull cost discipline
    // — the full spec derivation runs only past these).
    if !selectdistinct_enabled() || !plaindistinct_enabled() {
        return Ok(false);
    }
    if !::nodeagg::agg_is_hashed(agg)
        || agg.plan.numCols != 1
        || agg.plan.plan.targetlist.len() != 1
        || !agg.plan.plan.qual.is_nil()
    {
        return Ok(false);
    }
    let dop = super::router::arm_dop(super::router::ArmClass::Distinct);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    lane_trace("runtime-plaindistinct: probed (select-distinct)");

    let ea = runtime_instr::ea_active(estate);
    let node_id = agg.plan.plan.plan_node_id;
    // v1: EXPLAIN ANALYZE refuses (the serial arm serves EA,
    // value-identically; the transparency record names this gate).
    if ea {
        refused(estate, true, node_id, "ea not threaded (select-distinct v1)");
        return Ok(false);
    }
    // GL-LOWDIST-4 B1: heap seq scans admit under the knob (the columnar
    // dict/int lanes fall back to the collected emit_key batch on heap).
    if !seq_scan_fusible(ss, estate)?
        || !(::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
            || (::nodeseqscan::seq_scan_is_heap(ss)
                && super::runtime_distinct::distinct_heap_enabled()))
    {
        refused(estate, ea, node_id, "scan not fusible/cbstore");
        return Ok(false);
    }
    if estate.es_epq_active {
        return Ok(false);
    }
    if super::runtime_in_parallel_machinery(ss) {
        refused(estate, ea, node_id, "already in parallel machinery");
        return Ok(false);
    }
    // Plan shape below the Agg: exactly the SeqScan child (the workers
    // receive the scan subtree as their pstmt).
    let Some(scan_node) = agg.plan.plan.lefttree else { return Ok(false) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused(estate, ea, node_id, "scan node tag");
        return Ok(false);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    // The staged key column = the Agg's one grouping column (an arbitrary
    // scan OUTPUT column — AGG_HASHED keeps the scan's physical tlist);
    // its type from the scan tlist's bare Var at that position (anything
    // else refuses fail-closed).
    let key_att = match agg.plan.grpColIdx {
        [c] if *c >= 1 => (*c - 1) as u16,
        _ => {
            refused(estate, ea, node_id, "group key census (select-distinct)");
            return Ok(false);
        }
    };
    let Some(key_typ) = scan_plan
        .scan
        .plan
        .targetlist
        .nth(key_att as usize)
        .as_target_entry()
        .and_then(|te| te.expr.as_var())
        .map(|v| v.vartype)
    else {
        refused(estate, ea, node_id, "scan key col not a bare Var");
        return Ok(false);
    };
    let Some(spec) = ::nodeagg::plainpd::plain_sd_derive_spec(agg, key_typ)? else {
        refused(estate, ea, node_id, "spec derivation (select-distinct)");
        return Ok(false);
    };
    debug_assert_eq!(spec.att, key_att);
    // The direct staged-key feed is the whole accept path: prove it on the
    // leader's scan (workers re-prove on their own executors) — the sortkey
    // resolution for projected single-column shapes, the explicit-attnum
    // arm for the unprojected physical-tlist shape.
    if !::nodeseqscan::seq_scan_sortkey_direct(ss, estate)
        && !::nodeseqscan::seq_scan_key_direct_att(ss, estate, key_att)
    {
        refused(estate, ea, node_id, "key staging not direct");
        return Ok(false);
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused(estate, ea, node_id, "extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(false) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused(estate, ea, node_id, "exec params");
        return Ok(false);
    }
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused(estate, ea, node_id, "parallel-unsafe scan exprs");
        return Ok(false);
    }
    if !estate.es_snapshot.as_deref().is_some_and(::types_snapshot::IsMVCCSnapshot) {
        refused(estate, ea, node_id, "non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        refused(estate, ea, node_id, "binder policy sources");
        return Ok(false);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, source)) = super::runtime_distinct::distinct_task_source(ss, estate)?
    else {
        return Ok(false);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused(estate, ea, node_id, "granule floor");
        return Ok(false);
    }
    let dop = super::runtime_scan::elastic_dop(dop, total_granules);
    // GL-LOWDIST-1 leader-parity bump (knob-gated, band-only).
    let (dop, lowwidth) =
        super::runtime_distinct::lowwidth_leader_parity_dop(rt, dop, "runtime-plaindistinct");
    if ::nodeagg::agg_is_done(agg) {
        return Ok(true);
    }

    // --- Engage (the shared ceremony; the emit_values tail adopts the
    // merged values as sink emit buffers).
    let engaged = engage(
        agg,
        estate,
        rt,
        dop,
        lowwidth,
        total_granules,
        source,
        spec,
        scan_node,
        /* force_set = */ false,
        /* emit_values = */ true,
    )?;
    Ok(engaged.is_some())
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    lowwidth: bool,
    total_granules: u64,
    source: std::sync::Arc<dyn runtime::MorselSource>,
    spec: Arc<PlainPdSpec>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
    force_set: bool,
    emit_values: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // The worker pstmt carries ONLY the scan subtree.
    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    let payload = Arc::new(RuntimePlainDistinctShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path.
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        spec: Arc::clone(&spec),
        sd_values: emit_values,
        lowwidth: {
            if lowwidth {
                lane_trace(&format!("runtime-plaindistinct: low-width combine armed (dop={dop})"));
            }
            lowwidth
        },
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        crossed: AtomicBool::new(false),
        merged_bytes: AtomicUsize::new(0),
        out: (0..PLAIN_PD_PARTS).map(|_| UnsafeCell::new(None)).collect(),
        merged: Mutex::new(None),
        standing: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg,
        estate,
        rt,
        dop,
        total_granules,
        source,
        &payload,
        force_set,
        emit_values,
    );
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// This arm's standing-channel constants (M2 inc-1; see
/// standing_channel::StandingArm — sinks_gate: PGRUST_RUNTIME_POOLBIND_SINKS).
static STANDING_ARM: super::standing_channel::StandingArm =
    super::standing_channel::StandingArm {
        label: "runtime-plaindistinct",
        died: "runtime plain-distinct standing executors exited before completing the build",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN; a budget/shape crossing takes the serial rerun;
/// an unexplained abort surfaces the pending interrupt or reports;
/// completed-but-nobody-participated falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimePlainDistinctShared>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if let Some(e) = payload.take_error() {
        lane_trace(&format!(
            "runtime-plaindistinct: worker-phase error: {}",
            e.message()
        ));
        return Err(e);
    }
    if outcome == runtime::RgOutcome::Aborted {
        if payload.crossed.load(Ordering::SeqCst) {
            lane_trace("runtime-plaindistinct: crossed; serial fallback");
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
            return Ok(EngageOutcome::Fallback);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime plain-distinct pipeline aborted",
        )));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        return Ok(EngageOutcome::Fallback);
    }
    Ok(EngageOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    source: std::sync::Arc<dyn runtime::MorselSource>,
    payload: &Arc<RuntimePlainDistinctShared>,
    force_set: bool,
    emit_values: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_plaindistinct_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm binds EAGERLY (with_query_task_binding); the standing serve
        // re-establishes visibility up front and evicts parked sticky.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: runtime_plaindistinct_standing_driver,
            deferred_bind: false,
        });
        // M2 inc-2: the POOL-DB channel — built BEFORE submit (the bound
        // descriptor must ride the submission: publication keys the
        // pool-visible active bit off it); sinks_gate: POOLBIND_SINKS=0
        // retires this channel with the gang's. None = plain pinned
        // submit, inc-1 byte-exactly.
        let pool = super::standing_channel::try_pool_channel(
            payload.pcxt_shared.get().expect("pcxt shared set above"),
            dop,
            /* sinks_gate */ true,
        );


        // Per-AM morsel source built at admission (distinct_task_source —
        // GL-LOWDIST-4 B1).
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe: _probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let spec = runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        };
        // rg-set-BEFORE-publish (M2 inc-3 rung 3): payload.rg is stored by
        // on_rg before the bound submission can become pool-visible — no
        // "rg gone" refusal churn window.
        let set_rg = |rg: &runtime::RgHandle| {
            payload
                .rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("rg set once"));
        };
        let (rg, waiter) = match &pool {
            Some((_, descriptor)) => {
                rt.submit_pinned_bound(spec, 0, descriptor.clone(), set_rg)
            }
            None => {
                let (rg, waiter) = rt.submit_pinned(spec);
                set_rg(&rg);
                (rg, waiter)
            }
        };
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first — no worker launch, one
        // binder bind per participant; fallback leaves the RG untouched
        // for the launched path below.
        let census =
            format!("kind={}", if payload.spec.is_bytes() { "bytes" } else { "int" });
        match super::standing_channel::standing_wait(
            &STANDING_ARM,
            super::standing_channel::StandingLeader {
                // M2 inc-2: the pool-db board attached at submit (None =
                // gang-first, inc-1 exactly).
                pool: pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
                shared: payload.pcxt_shared.get().expect("pcxt shared set above"),
                slot: &payload.standing,
                started: &payload.started,
                refused: &payload.refused,
                take_error: &|| payload.take_error(),
                drain: &|rg| drain_rg(rt, rg),
                census: &census,
            },
            dop,
            total_granules,
            &rg,
            &waiter,
        )? {
            super::standing_channel::StandingWait::Done(outcome) => {
                return finish_outcome(payload, outcome);
            }
            super::standing_channel::StandingWait::Fallback => {}
        }

        // M2 inc-3 rung 4: the launched-bgworker fallback is DELETED — a
        // board decline goes straight to the serial arm (pool → gang →
        // serial; the NOLAUNCH posture made permanent). Cause attribution
        // ticks the nolaunch-serial floor row inside the shared helper.
        super::standing_channel::launched_fallback_retired(&STANDING_ARM);
        drain_rg(rt, &rg);
        Ok(EngageOutcome::Fallback)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed and this frame's arena can unwind.
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match outcome {
        EngageOutcome::Fallback => {
            stats::tick_engaged(STANDING_ARM.label, stats::EngageChannel::Serial);
            lane_trace("runtime-plaindistinct: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let Some((parts, seen_null)) = payload.take_merged() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime plain-distinct completed without a merged result",
                )));
            };
            stats::tick_owned(ShapeClass::AggBuild);
            let n: usize = parts.iter().map(|m| m.len()).sum();
            lane_trace(&format!("runtime-plaindistinct: complete, distinct={n}"));
            // SE-T2AGG CAR A (plain SELECT DISTINCT): the merged VALUES are
            // the answer — adopt them as the sink's published emit (one key
            // column; every retrieve path drains it through
            // agg_retrieve_hash_table's sink branch). `Some(None)` is the
            // "arm owns the node, nothing consumed yet" marker: the caller
            // returns to its own pull, which drains the adopted emit.
            if emit_values {
                let bufs =
                    ::nodeagg::plainpd::plain_sd_emit_bufs(&payload.spec, &parts, seen_null);
                ::nodeagg::sink::agg_sink_adopt_emit(agg, bufs, 1, None);
                trace_feed("plain-select-distinct runtime drive engaged");
                return Ok(Some(None));
            }
            // Adopt: the serial drives' exact sequence — arm set-mode (the
            // skip-sort shape), fresh pergroups, install the replay-only
            // merged set, ordinary set-mode finalize (count shortcut,
            // HAVING, projection).
            if force_set {
                ::nodeagg::agg_force_distinct_set(agg);
            }
            ::nodeagg::agg_plain_build_begin(agg, estate)?;
            ::nodeagg::plainpd::agg_plain_install_merged_set(agg, parts, seen_null);
            trace_feed("plain-agg distinct-set runtime drive engaged");
            Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
        }
    }
}

fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    // Bounded lane wait (~2s): helper drives settle within a morsel.
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-plaindistinct: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-plaindistinct: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
