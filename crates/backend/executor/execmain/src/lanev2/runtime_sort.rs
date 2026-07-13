//! M3 SORT SINK — parallel top-N (ORDER BY key LIMIT n) on the morsel
//! runtime (docs/design/m3-sort.md §2–§4; docs/design/
//! parallelism-redesign-2026-07.md §2.2/§5-M3).
//!
//! Shape: the SERIAL-plan bounded sort breaker `Sort(bounded) ←
//! SeqScan(cbstore)` (the ClickBench Q24/Q25 class), executed as one
//! SealedParallelSink on the runtime: ACCEPT (granule-morsel scan →
//! PREWHERE → narrow (key, rowref) pushes into a per-worker bounded
//! `TopnHeap` on the tie-ordering rule-2 TOTAL order) → SEAL (parallel
//! per-worker `into_sorted`) → COMBINE (partitions() = 1: one k-way
//! truncate-merge of ≤ W×bound POD entries — the only serial point between
//! scan end and gather) → finalize (publish the winner list). The parked
//! leader adopts the winners and performs refsort v2's ONE
//! late-materialization gather (`seq_scan_gather_row` per winner, ≤ bound
//! rows total — vs the Gather-Merge arm's N_workers × bound disease) into
//! the node's `refsort_out` buffer; the UNCHANGED refsort emit face serves
//! them in merged order.
//!
//! Determinism (design §3): the winner list is the `bound` smallest
//! entries of the union under a total order (rowrefs unique across
//! disjoint granule claims) — a pure function of the table contents,
//! independent of claim order and worker count. No tie tracking, no
//! demote ladder. Ordering/parity law vs non-rowref-canonical serial
//! channels: design §4 (tie-normalized gates + boundary-tie count gate).
//!
//! Memory (design §7): a Local is ≤ bound × 16 B — no work_mem
//! interaction, no budget-crossing path. The only mid-flight fallback is a
//! CONTRACT BREAK (a staged batch without a window ref, a gather miss):
//! recorded, RG aborted, leader reruns the serial arm from scratch
//! (nothing was emitted — the R5 whole-attempt-rerun discipline).
//!
//! Engagement layering (all cheap; absent = today's serial path, byte-
//! and perf-identical): PGRUST_RUNTIME=1 (pool spawned) + SET
//! pgrust.runtime_sort_pool = <dop> (this arm's own DOP knob — NOT the
//! scan knob; the m2-distinct coupling gotcha is deliberately avoided) +
//! PGRUST_RUNTIME_SORT != 0 (arm kill switch). Plan surface stays the
//! serial plan; EXPLAIN unchanged; instrumented runs refuse (EXPLAIN
//! ANALYZE stays C-exact).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::nodesort::sink::{topn_merge, TopnEntry, TopnHeap, TOPN_MAX_BOUND};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::{drain_pipeline, BatchEmit, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};
use super::{lane_trace, seq_scan_fusible, trace_feed};

// ---------------------------------------------------------------------------
// Admission spec (leader-derived plain data; workers read it from the
// payload). The shape law is the refsort census (lanev2 `refsort_arm`,
// duplicated here rather than refactored — the serial arm keeps its own
// kill switch, sticky-refusal memo and Gather-era parallel refusal, none of
// which apply to the sink) PLUS the int-family key vocabulary the POD heap
// encoding requires (the adaptive-walk vocabulary, `CmpOp::for_fn_oid`).
// ---------------------------------------------------------------------------

/// Sort-key datum width for the order-preserving i64 widening
/// (`TopnEntry::encode`'s input contract). The CmpOp families guarantee the
/// widths: Int2/Int4/Int8 compare sign-extended, Oid compares unsigned
/// 32-bit (zero-extended to i64 it stays order-correct).
#[derive(Clone, Copy)]
enum KeyWidth {
    I2,
    I4,
    I8,
    U32,
}

#[inline]
fn key_i64(d: ::datum::Datum, w: KeyWidth) -> i64 {
    match w {
        KeyWidth::I2 => d.as_i16() as i64,
        KeyWidth::I4 => d.as_i32() as i64,
        KeyWidth::I8 => d.as_i64(),
        KeyWidth::U32 => d.as_u32() as i64,
    }
}

struct TopnSpec {
    /// The leading sort key's scan column (0-based; the SoA fast-leg read).
    key_attno_scan: u16,
    /// The key's position in the outer (child output) desc — the fallback
    /// leg reads the key from this projected slot cell.
    key_resno_outer: usize,
    /// Outer resno -> scan attno (the deferred Var-only winner projection).
    tlist_map: Vec<u16>,
    desc: bool,
    nulls_first: bool,
    width: KeyWidth,
    bound: usize,
}

/// Shape derivation (fail-closed; `None` = the serial feed runs unchanged).
fn topn_spec<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> Option<TopnSpec> {
    if !state.bounded || state.bound <= 0 || state.bound > TOPN_MAX_BOUND as i64 {
        return None;
    }
    // Single-column output sorts bare datums (nothing to late-materialize);
    // phase-1 key arity is the refsort envelope.
    if ::nodesort::sort_lane_is_datum(state) {
        return None;
    }
    let plan = state.plan;
    if plan.numCols != 1 || plan.sortColIdx.is_empty() || plan.nullsFirst.is_empty() {
        return None;
    }
    // Window refs only exist for cbstore staged batches.
    if !::nodeseqscan::seq_scan_is_cbstore(ss) {
        return None;
    }
    // Int-family leading key (the POD-heap encoding vocabulary — the
    // adaptive walk's own list; timestamps/dates ride their I8/I4 cmp
    // shapes). Anything else refuses to the serial arms.
    let opfn = ::lsyscache::get_opcode(plan.sortOperators[0]).ok()?;
    use ::execexpr::CmpOp::*;
    let (width, desc) = match ::execexpr::CmpOp::for_fn_oid(opfn) {
        Some(Int2Lt) => (KeyWidth::I2, false),
        Some(Int2Gt) => (KeyWidth::I2, true),
        Some(Int4Lt) => (KeyWidth::I4, false),
        Some(Int4Gt) => (KeyWidth::I4, true),
        Some(Int8Lt) => (KeyWidth::I8, false),
        Some(Int8Gt) => (KeyWidth::I8, true),
        Some(OidLt) => (KeyWidth::U32, false),
        Some(OidGt) => (KeyWidth::U32, true),
        _ => return None,
    };
    let natts = outer_desc.natts as usize;
    let tlist_map: Vec<u16> = match ss.ss.ps_ProjInfo.as_ref() {
        // No projection: outer resno j is scan attno j (physical tlist).
        None => (0..natts as u16).collect(),
        // Projected scans admit only the pure Var-copy census (a computing
        // column deferred to winners could elide C's error).
        Some(p) => {
            let cols = p.pi_state.scan_proj_cols()?;
            if cols.any_arith() || cols.n as usize != natts {
                return None;
            }
            cols.cols[..natts]
                .iter()
                .map(|c| match *c {
                    ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                    _ => None,
                })
                .collect::<Option<Vec<u16>>>()?
        }
    };
    let oc = plan.sortColIdx[0];
    if oc < 1 || oc as usize > natts {
        return None;
    }
    let key_resno_outer = (oc - 1) as usize;
    Some(TopnSpec {
        key_attno_scan: tlist_map[key_resno_outer],
        key_resno_outer,
        tlist_map,
        desc,
        nulls_first: plan.nullsFirst[0],
        width,
        bound: state.bound as usize,
    })
}

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the sink body
// (one struct, one Arc — the runtime_scan/runtime_distinct discipline).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeSortShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// Worker-readable spec pod (plain data; Locals fork from `bound`).
    key_attno_scan: u16,
    key_resno_outer: usize,
    desc: bool,
    nulls_first: bool,
    width: KeyWidth,
    bound: usize,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// First worker-phase error (entry-phase errors ride the ordinary
    /// parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// A sink contract break (staged batch without a window ref): NOT an
    /// error — the RG aborts and the leader reruns the serial arm (R5).
    broke: AtomicBool,
    /// The published winner list (combine writes — partitions()=1, single
    /// claimer; the leader takes after completion).
    winners: Mutex<Option<Vec<TopnEntry>>>,
}

impl RuntimeSortShared {
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

    fn break_contract(&self) {
        self.broke.store(true, Ordering::SeqCst);
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

    fn take_winners(&self) -> Option<Vec<TopnEntry>> {
        self.winners.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation. accept_local/seal/combine are
// INFALLIBLE BY CONTRACT: errors and panics are caught, recorded (first
// wins), and turn into an RG abort — the runtime never sees an unwind.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimeSortShared {
    type Local = TopnHeap;
    type Sealed = Vec<TopnEntry>;

    fn fork(&self, _worker: usize) -> TopnHeap {
        TopnHeap::new(self.bound)
    }

    fn accept_local(&self, local: &mut TopnHeap, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) || self.broke.load(Ordering::SeqCst) {
            return; // aborting: drain the claim without work
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(PgError::new(ERROR, "runtime sort worker panicked in a morsel").into());
            }
        }
    }

    fn seal(&self, _worker: usize, local: TopnHeap) -> Vec<TopnEntry> {
        if self.failed.load(Ordering::SeqCst) || self.broke.load(Ordering::SeqCst) {
            return Vec::new();
        }
        // POD sort — cannot unwind.
        local.into_sorted()
    }

    fn partitions(&self) -> u64 {
        1
    }

    fn combine(&self, part: u64, sealed: &[Vec<TopnEntry>]) {
        debug_assert_eq!(part, 0);
        if self.failed.load(Ordering::SeqCst) || self.broke.load(Ordering::SeqCst) {
            return;
        }
        let merged = topn_merge(sealed, self.bound);
        *self.winners.lock().unwrap_or_else(|p| p.into_inner()) = Some(merged);
    }

    fn finalize(&self, _sealed: &[Vec<TopnEntry>]) {
        // Publish already happened in the (single) combine; nothing to do.
        // Aborted RGs skip finalize; the leader validates the winner slot
        // on the Completed path (protocol-violation error, never silence).
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
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

/// The per-morsel accept feed: narrow (key, rowref) pushes into the
/// worker's bounded heap — the RefSortSink batch loop with the heap in
/// place of the narrow tuplesort (same two-leg key law: clean staged rows
/// read the SoA column; requal/fallback rows run the exact per-row emit —
/// C detoast semantics, C's errors on C's row — and read the projected
/// cell). `broke` = a staged batch without a window ref (or a per-row
/// arrival): the sink cannot carry rowrefs — contract break, RG abort,
/// serial rerun.
struct TopnAcceptSink<'a> {
    heap: &'a mut TopnHeap,
    key_col: u16,
    key_resno: usize,
    desc: bool,
    nulls_first: bool,
    width: KeyWidth,
    broke: bool,
}

impl TopnAcceptSink<'_> {
    #[inline]
    fn push(&mut self, key: ::datum::Datum, isnull: bool, rg: u32, row: u32) {
        let rowref = ((rg as u64) << 32) | row as u64;
        self.heap.push(TopnEntry::encode(
            key_i64(key, self.width),
            isnull,
            self.desc,
            self.nulls_first,
            rowref,
        ));
    }
}

impl<'mcx> Sink<'mcx> for TopnAcceptSink<'_> {
    fn accept(&mut self, _tuple: ExecSlotId, _estate: &mut EStateData<'mcx>) -> PgResult<SinkFeed> {
        // Row-granular arrival = no staged window ref to pair the row with.
        // Never reached from the seqscan drain (its operator overrides
        // consume_batch); defensive break.
        self.broke = true;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for TopnAcceptSink<'_> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if self.broke {
            return Ok(());
        }
        let Some((rg, row0)) = emit.window_ref() else {
            self.broke = true;
            return Ok(());
        };
        // Interrupt cadence floor: one check per staged batch (the fast
        // leg's rows have no per-row seam call; emit-path rows keep their
        // per-row check inside `emit`) — the RefSortSink cadence.
        ::postgres_seams::check_for_interrupts::call()?;
        let fast = emit.refsort_key_batch(self.key_col, n).map(|(_, _, fallback, sel)| {
            let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
            fb[..fallback.len()].copy_from_slice(fallback);
            let selw = sel.map(|s| {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                w[..s.len()].copy_from_slice(s);
                w
            });
            (fb, selw)
        });
        for i in pos..n {
            if let Some((fb, selw)) = &fast {
                let w = (i / 64) as usize;
                let bit = 1u64 << (i % 64);
                if let Some(selw) = selw {
                    if selw[w] & bit == 0 {
                        continue; // qual-filtered (exact whole-qual verdict)
                    }
                }
                if fb[w] & bit == 0 {
                    // Clean staged row: key straight from the SoA column.
                    let (key, isnull) = {
                        let (kvals, knulls, _, _) = emit
                            .refsort_key_batch(self.key_col, n)
                            .expect("refsort key batch stable within a staged batch");
                        (kvals[i as usize], knulls[i as usize])
                    };
                    self.push(key, isnull, rg, row0 + i);
                    continue;
                }
                // Forced-fallback row: exact per-row emit below.
            }
            let Some(id) = emit.emit(i, estate)? else { continue };
            let (key, isnull) = {
                let slot = estate.slot_mut(id);
                ::exectuples::slot_getsomeattrs(slot, self.key_resno as i32 + 1);
                let base = slot.base();
                (base.tts_values[self.key_resno], base.tts_isnull[self.key_resno])
            };
            self.push(key, isnull, rg, row0 + i);
        }
        Ok(())
    }
}

impl RuntimeSortShared {
    fn morsel_body(&self, local: &mut TopnHeap, range: runtime::MorselRange) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime sort morsel without a bound executor",
                )));
            };
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime sort worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = sort_worker_scan(d.planstate.as_mut())?;
                    if !::nodeseqscan::seq_scan_cb_set_granule_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )? {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime sort worker scan is not cbstore",
                        )));
                    }
                    let mut sink = TopnAcceptSink {
                        heap: local,
                        key_col: self.key_attno_scan,
                        key_resno: self.key_resno_outer,
                        desc: self.desc,
                        nulls_first: self.nulls_first,
                        width: self.width,
                        broke: false,
                    };
                    let fed = drain_pipeline(
                        ss,
                        &mut SeqScanSource,
                        &mut SeqScanFilterProject,
                        &mut sink,
                        estate,
                    );
                    let broke = sink.broke;
                    fed?;
                    if broke {
                        trace_feed("runtime sort worker contract break; aborting to serial fallback");
                        self.break_contract();
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the
/// Sort — accept_local drives scan → PREWHERE → narrow pushes; the worker
/// pstmt's planTree is the SeqScan node).
fn sort_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime sort worker plan is not a SeqScan root",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper entry + POST_TASK_PARK drive (the shared ceremony; the hook
// registries are multi-registrant and every hook no-ops on foreign
// payloads).
// ---------------------------------------------------------------------------

fn runtime_sort_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_sort_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeSortShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime sort helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeSortShared>) {
    let _ = shared;
    let Some(target) = payload.pcxt_shared.get() else { return };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return };
    let Some(lane) = payload.rt.acquire_external_lane() else {
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
            } else {
                lane_trace(&format!("runtime-sort: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeSortShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeSortShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime sort worker ExecutorStart");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = sort_worker_scan(d.planstate.as_mut())?;
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::RowFeed {
                            ctx: "runtime sort worker feed",
                            stitch: true,
                        },
                    )
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, errored: std::cell::Cell::new(false) });
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

fn runtime_sort_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeSortShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_sort_main",
            runtime_sort_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_sort_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_sort_private_shutdown);
    });
}

/// `PGRUST_RUNTIME_SORT` arm kill switch (default ON when the runtime is
/// armed; the runtime itself defaults OFF).
fn runtime_sort_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_SORT").as_deref(), Ok("0") | Ok("off"))
    })
}

// ---------------------------------------------------------------------------
// Leader-side engagement.
// ---------------------------------------------------------------------------

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once the
/// arm is ARMED — dop set + runtime on — so unarmed sessions stay silent).
#[cold]
fn refused(reason: &str) {
    lane_trace(&format!("runtime-sort: refused ({reason})"));
}

/// The runtime top-N sink arm, probed from the sort feed's SeqScan branch
/// BEFORE the serial arms arm anything. `Ok(false)` = refused or fell back
/// (nothing consumed, no sort state touched; the serial feed runs
/// byte-identically). `Ok(true)` = the arm owns the node: the winners are
/// gathered and buffered (`refsort_out`), the emit face is live.
pub(super) fn try_own_sort_topn<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_sort_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() || !runtime_sort_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    lane_trace("runtime-sort: probed");

    // --- Shape + session gates (fail-closed; every refusal = serial arm).
    if !seq_scan_fusible(ss, estate)? {
        refused("scan not fusible");
        return Ok(false);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refused("instrumented/epq");
        return Ok(false);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused("already in parallel machinery");
        return Ok(false);
    }
    let Some(spec) = topn_spec(state, ss, outer_desc) else {
        refused("shape spec (bound/key vocabulary/tlist census)");
        return Ok(false);
    };
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(false) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused("exec params");
        return Ok(false);
    }
    // Plan shape below the Sort: exactly THIS SeqScan (the workers receive
    // the SCAN SUBTREE as their pstmt; the Sort need not be the plan root —
    // Limit above it is the whole point of the shape).
    let Some(scan_node) = state.plan.plan.lefttree else { return Ok(false) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused("sort child not SeqScan");
        return Ok(false);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused("parallel-unsafe scan exprs");
        return Ok(false);
    }
    if !estate.es_snapshot.as_deref().is_some_and(::types_snapshot::IsMVCCSnapshot) {
        refused("non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        refused("binder policy sources");
        return Ok(false);
    }

    // --- Geometry: enough granules to be worth a gang. (This also OPENS the
    // leader's scan desc — the winner gather below depends on it.)
    let Some((total_granules, starts)) = ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(false);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused("granule floor");
        return Ok(false);
    }

    // --- Engage.
    engage(state, ss, outer_desc, estate, rt, dop, total_granules, starts, spec, scan_node)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    spec: TopnSpec,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    let payload = Arc::new(RuntimeSortShared {
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
        key_attno_scan: spec.key_attno_scan,
        key_resno_outer: spec.key_resno_outer,
        desc: spec.desc,
        nulls_first: spec.nulls_first,
        width: spec.width,
        bound: spec.bound,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        broke: AtomicBool::new(false),
        winners: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        state,
        ss,
        outer_desc,
        estate,
        rt,
        dop,
        total_granules,
        starts,
        &payload,
        &spec,
    );
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed(Vec<TopnEntry>),
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeSortShared>,
    spec: &TopnSpec,
) -> PgResult<bool> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_sort_main", dop)?;
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

        // Submit the pinned RG (accept → seal → combine) before launch.
        let source = Arc::new(super::runtime_scan::CbstoreGranuleSource { starts });
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
        payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-sort: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        // The launched-DOP census line (the m1-heap-source harness trap:
        // max_worker_processes silently caps DOP probes — every probe
        // config must be able to see the LAUNCHED number, not the asked
        // one).
        lane_trace(&format!(
            "runtime-sort: engaged dop={launched}/{dop} granules={total_granules} bound={}",
            spec.bound
        ));

        // Submit-and-park (the WaitForParallelWorkersToFinish shape).
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
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-sort: all {refused} helpers refused the bind"));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            parallel::wait_parallel_finish_quantum();
        };

        if let Some(e) = payload.take_error() {
            lane_trace(&format!("runtime-sort: worker-phase error: {}", e.message()));
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            if payload.broke.load(Ordering::SeqCst) {
                lane_trace("runtime-sort: sink contract break; serial fallback");
                return Ok(EngageOutcome::Fallback);
            }
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime sort pipeline aborted")));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        let Some(winners) = payload.take_winners() else {
            // Completed with participants but no published winners: a
            // protocol violation, never silently wrong output.
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime sort completed without a winner list",
            )));
        };
        Ok(EngageOutcome::Completed(winners))
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
            lane_trace("runtime-sort: fallback to serial arm");
            Ok(false)
        }
        EngageOutcome::Completed(winners) => {
            adopt_winners(state, ss, outer_desc, estate, spec, winners)
        }
    }
}

/// The leader's late-materialization gather (refsort v2, design §4): decode
/// each winner's rowref, gather the full row through the leader's own scan
/// state, project Var-only into outer format, buffer on the node. ≤ bound
/// rows total. Any gather miss resets the node and falls back to the serial
/// arm BEFORE any output escapes (the winners buffer is node-internal until
/// the emit face pops it — the serial refsort invariant, reused).
fn adopt_winners<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    spec: &TopnSpec,
    winners: Vec<TopnEntry>,
) -> PgResult<bool> {
    ::nodesort::sort_lane_runtime_topn_begin(state);
    let natts = outer_desc.natts as usize;
    let mut values = vec![::datum::Datum::null(); natts];
    let mut isnull = vec![true; natts];
    let mcx = estate.es_query_cxt;
    for e in &winners {
        let r = e.rowref();
        let (rg, row) = ((r >> 32) as u32, r as u32);
        if !::nodeseqscan::seq_scan_gather_row(ss, estate, rg, row) {
            lane_trace("runtime-sort: winner gather failed; serial fallback");
            ::nodesort::sort_lane_reset_for_refeed(state);
            return Ok(false);
        }
        {
            let slot = estate.slot_mut(ss.ss.ss_ScanTupleSlot);
            let base = slot.base();
            for (j, &c) in spec.tlist_map.iter().enumerate() {
                values[j] = base.tts_values[c as usize];
                isnull[j] = base.tts_isnull[c as usize];
                // Needed-set guard (the refsort law): gather_row nulls only
                // unneeded cells (cbstore stores no NULLs), so a null
                // projected cell means the column was outside the scan's
                // needed set — fall back before any output escapes.
                if isnull[j] {
                    lane_trace("runtime-sort: gathered cell outside the needed set; serial fallback");
                    ::nodesort::sort_lane_reset_for_refeed(state);
                    return Ok(false);
                }
            }
        }
        ::nodesort::sort_lane_refsort_push_winner(state, mcx, &values, &isnull)?;
    }
    ::nodesort::sort_lane_runtime_topn_done(state);
    trace_feed("runtime sort sink adopt + refsort emit engaged");
    lane_trace(&format!(
        "runtime-sort: complete, winners={} (bound {})",
        ::nodesort::sort_lane_refsort_winners(state),
        spec.bound
    ));
    Ok(true)
}

/// Reap a pinned RG no helper will drive (abort/fallback paths) — protocol
/// cleanup driving, not leader work execution (§2.5).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) {
    rg.abort();
    let lane = loop {
        if let Some(l) = rt.acquire_external_lane() {
            break l;
        }
        std::thread::yield_now();
    };
    let mut local = lane.local();
    let _ = rt.drive_pinned(&mut local, rg);
}
