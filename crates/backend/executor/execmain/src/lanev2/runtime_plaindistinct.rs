//! Runtime PLAIN exact-DISTINCT sink (band-2b) — DOP-parallel
//! `Aggregate(AGG_PLAIN, all-DISTINCT) → [Sort →] SeqScan(cbstore)`, the CB
//! q5/q6 class (`COUNT(DISTINCT UserID)` / `COUNT(DISTINCT SearchPhrase)`).
//!
//! Why this arm exists: the planner-parallel plan for this shape is
//! `Aggregate ← Gather Merge ← Sort ← Parallel Seq Scan` — every input row
//! ships through the tuple queue and the leader runs the presorted distinct
//! transition serially. The dop16-bandwidth study classified that arm
//! INSTRUCTION-WASTE (q5 dop16 executes ~9x the serial instructions:
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
//! Kill switch: `PGRUST_RUNTIME_PLAINDISTINCT=0`. Pool arming rides
//! `pgrust.runtime_distinct_pool` (this is the distinct-sink family).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::nodeagg::plainpd::{
    plain_pd_combine, plain_pd_derive_spec, PlainPdLocal, PlainPdMerged, PlainPdSealed,
    PlainPdSpec, PLAIN_PD_PARTS,
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
            plain_pd_combine(self.spec.is_bytes(), part as usize, sealed)
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
/// the engagement to the serial fallback (v1 fail-closed — cbstore stages
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
        // Integer key lane, whole-slice (the q5 hot path).
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
                    // (v1 fail-closed; cbstore covered-key scans stage all).
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
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(&payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime plain-distinct helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn runtime_plaindistinct_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimePlainDistinctShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
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
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
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
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg)
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
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
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
                    // executor (arming decides staging).
                    let key_direct = ::nodeseqscan::seq_scan_sortkey_direct(ss, estate);
                    let key_bytes = key_direct && payload.spec.is_bytes();
                    if key_bytes {
                        // Arm the dict-coded key lane when the bank serves it
                        // (the serial drive's own arming call).
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
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_PLAINDISTINCT").as_deref() != Ok("0"))
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
    let dop = ::guc_tables::runtime_pool::runtime_distinct_pool_dop();
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
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        refused(estate, ea, node_id, "scan not fusible/cbstore");
        return Ok(None);
    }
    if estate.es_epq_active {
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused(estate, ea, node_id, "already in parallel machinery");
        return Ok(None);
    }
    // The direct staged-key feed is the whole accept path: prove it on the
    // leader's scan (workers re-prove on their own executors).
    if !::nodeseqscan::seq_scan_sortkey_direct(ss, estate) {
        refused(estate, ea, node_id, "key staging not direct");
        return Ok(None);
    }
    let Some(spec) = plain_pd_derive_spec(agg) else {
        refused(estate, ea, node_id, "spec derivation");
        return Ok(None);
    };
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
    let Some((total_granules, starts)) = ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused(estate, ea, node_id, "granule floor");
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, total_granules, starts, spec, scan_node, force_set)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    spec: Arc<PlainPdSpec>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
    force_set: bool,
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
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        crossed: AtomicBool::new(false),
        merged_bytes: AtomicUsize::new(0),
        out: (0..PLAIN_PD_PARTS).map(|_| UnsafeCell::new(None)).collect(),
        merged: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged = engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, force_set);
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimePlainDistinctShared>,
    force_set: bool,
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

        let source = Arc::new(super::runtime_scan::CbstoreGranuleSource {
            starts: Arc::new(starts),
            coalesce: false,
        });
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe: _probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-plaindistinct: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-plaindistinct: engaged dop={launched} granules={total_granules} kind={}",
            if payload.spec.is_bytes() { "bytes" } else { "int" }
        ));

        // Submit-and-park (the WaitForParallelWorkersToFinish shape).
        let mut all_exited_seen = false;
        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
            let refused_n = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused_n >= launched as usize {
                lane_trace(&format!(
                    "runtime-plaindistinct: all {refused_n} helpers refused the bind"
                ));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-plaindistinct: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, &rg);
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                if payload.crossed.load(Ordering::SeqCst) {
                    lane_trace("runtime-plaindistinct: crossed; serial fallback");
                    stats::tick_refused(
                        ShapeClass::AggBuild,
                        RefuseReason::AdmissionEconomicsFusedDrive,
                    );
                    return Ok(EngageOutcome::Fallback);
                }
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime plain-distinct helpers exited before completing the build",
                )));
            }
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    lane_trace(
                        "runtime-plaindistinct: all helpers exited without completing the RG — reaping",
                    );
                    rg.abort();
                    drain_rg(rt, &rg);
                    continue;
                }
                all_exited_seen = true;
            }
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
        };

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
