//! NL-INNER-INDEX — morselized nested-loop-with-inner-index-probes on the
//! runtime gang (nlindex lane, notes/nlindex-lane.md). The hash-join runtime
//! arms are structurally inert on plans the planner elects as a nested loop
//! driving inner index lookups (small filtered outer, indexed inner) — this
//! arm is the missing engagement family for that plan class.
//!
//! Shape: a SERIAL-plan plain Agg over NestLoop(outer = heap SeqScan,
//! inner = btree IndexScan, exec-param rescan keys), executed by N
//! runtime-launched helpers. The OUTER heap is the work divider (block-range
//! morsel claims — the runtime_scan geometry verbatim); each helper drives
//! its own PRIVATE executor over the whole worker pstmt and, per claimed
//! outer row, runs the UNCHANGED per-outer-row NestLoop program
//! (`lane_accept_outer` → bind nestParams → inner rescan;
//! `lane_probe_next` → same inner pulls, same joinqual/otherqual/projection
//! in the same order — byte-identical join semantics by construction), each
//! joined row feeding the plain-agg fold. Concurrent PRIVATE btree descent +
//! heap fetch is the index-morsels modes-B/C proven surface (private scan
//! descs per helper; pin/lock discipline lives below the AM seam).
//!
//! The plan surface stays the serial plan (force-plans discipline);
//! engagement is FORCED/explicit and DEFAULT OFF:
//!
//!   PGRUST_RUNTIME=1                        (pool master switch)
//!   PGRUST_RUNTIME_NLINDEX=1                (arm ENABLE switch, default OFF)
//!   SET pgrust.runtime_nlindex_pool = <dop> (arming, runtime_pool.rs)
//!   pgrust.lane_executor on                 (lane master switch)
//!
//! ORDERED-DEMAND REFUSAL FLOOR: only the plain-agg fold shape is admitted —
//! the one consumer that is order-insensitive by construction (combined
//! partials are a set-fold; `agg_runtime_partial_admissible` — or its
//! SE-AGGPOLY manifest twin for the sum/avg(numeric) family, exact digit
//! snapshots — additionally requires exactly-combinable transition kinds,
//! so morselized outer order never changes a byte of the answer). Any plan
//! consuming join ORDER never reaches this entry.
//!
//! Fail-closed admission (refuse ⇒ fall through to the serial arms
//! byte-identically, ticked under ShapeClass::NestLoop): plain-Agg root with
//! order-insensitive-exact partials; NestLoop admissible + untouched (C's
//! own state machine unstarted — whole-life ownership); outer child a
//! lane-fusible HEAP SeqScan (the batched page drive's own refuse-set);
//! inner child a forward btree IndexScan, non-parallel-aware, no reorder;
//! MVCC snapshot; no extern params; no subplans; every worker-side
//! expression parallel-safe — with the ONE deliberate widening over the
//! sibling arms: PARAM_EXEC references are admitted iff the paramid is a
//! member of the join's own nestParams set (the params are bound per outer
//! row inside each worker's own estate — self-contained under the
//! transferred subtree; anything else refuses); binder policy sources
//! empty; a planner-estimate OUTER-row floor
//! (PGRUST_RUNTIME_NLINDEX_MIN_OUTER_ROWS, default 10,000 — provisional,
//! GL-NLIDX-1 owns re-measuring).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::guc_tables::runtime_pool::runtime_nlindex_pool_dop;
use ::nodeagg::runtime_partial::{
    agg_poly_export_partial_into, agg_poly_partial_admissible, agg_poly_runtime_combine,
    agg_runtime_combine, agg_runtime_export_partial_into, agg_runtime_partial_admissible,
    exec_agg_poly_runtime_partials, RuntimePartial,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::{Node, NodeTag};

use super::batch_source::{
    heapfeed_v2_enabled, require_bridge, BatchGranuleSource, HeapBatchSource, SeqScanSource,
};
use super::runtime_scan::{drain_rg_raw, elastic_dop, emit_wfin, wfin_enabled, SendConstPstmt};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, lane_trace_enabled};

// ---------------------------------------------------------------------------
// Engagement payload: the parallel context's private state AND the runtime
// task set's work body (one struct, one Arc) — the RuntimeIndexShared shape.
// ---------------------------------------------------------------------------

/// SE-AGGPOLY twin (runtime_scan's `poly_mode`, verbatim): whether this node
/// takes the poly-manifest export path — the sum/avg(numeric) NumericAggState
/// family the int-lane fold plan does not cover, relocated as exact digit
/// snapshots (order-insensitive-exact under reassociation, C's combine
/// composition). Derived identically by the leader (admission + combine) and
/// every worker (exec build) from the plan + the process-constant knob — the
/// worker-congruence discipline; no payload flag needed. Plan-path
/// admissibility WINS when it holds.
fn poly_mode(agg: &::nodeagg::AggStateData<'_>) -> bool {
    super::agg_poly_enabled()
        && !agg_runtime_partial_admissible(agg)
        && agg_poly_partial_admissible(agg)
}

pub(super) struct RuntimeNlIndexShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    /// The worker PlannedStmt (build_worker_pstmt over the serial Agg root).
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    pins_base: usize,
    refused: AtomicUsize,
    started: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// Per-ordinal cumulative partials, overwritten after every claim.
    partials: Vec<Mutex<Option<RuntimePartial>>>,
}

impl RuntimeNlIndexShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn aborted(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
            || self
                .rg
                .get()
                .and_then(|w| w.upgrade())
                .is_some_and(|rg| rg.is_aborted())
    }
}

// ---------------------------------------------------------------------------
// The TaskSetWork body: one claimed outer block range through the storage
// seam, each surviving outer row expanded through the unchanged per-outer-row
// NestLoop program into the plain-agg fold. Infallible by contract:
// errors/panics are recorded and abort the RG.
// ---------------------------------------------------------------------------

struct WorkerNlExec {
    qd: ::types_portal::QueryDescHandle,
    /// THIS helper contributed an error (its executor may be mid-batch —
    /// take the release/abort teardown, not finish/end).
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static WORKER_NLEXEC: std::cell::RefCell<Option<WorkerNlExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_NLEXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

impl runtime::TaskSetWork for RuntimeNlIndexShared {
    fn finalize(&self) {
        // Nothing to do: partials are installed per claim (each worker's
        // final export precedes its settle); the leader combines after
        // completion.
    }

    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(Box::new(PgError::new(
                    ERROR,
                    "runtime nlindex worker panicked",
                )));
            }
        }
    }
}

impl RuntimeNlIndexShared {
    fn morsel_body(&self, worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        WORKER_NLEXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime nlindex morsel without a bound executor",
                )));
            };
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime nlindex worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker plan is not a plain Agg root",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::NestLoop(nlnode) = &mut aps.outer
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker outer node is not a NestLoop",
                        )));
                    };
                    let crate::procnode::NestLoopNode { state: nls, outer, inner, .. } = nlnode;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut **outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker join outer is not a SeqScan",
                        )));
                    };
                    let agg = &mut aps.agg;
                    let interrupted = || self.aborted();
                    // WS-O claim-settle guard (both arms): end_claim runs on
                    // the ERROR path too — a failed claim must not carry its
                    // page pin into the abort drain; the drive error wins the
                    // report (the runtime_scan discipline verbatim).
                    if heapfeed_v2_enabled() && ::nodeseqscan::seq_scan_is_heap(ss) {
                        let mut src = HeapBatchSource::new(&mut *ss);
                        let drove = drive_claim(
                            &mut src, nls, inner, agg, estate, &range, &interrupted,
                        );
                        let settled = src.end_claim(estate);
                        drove?;
                        settled?;
                    } else {
                        let mut src = SeqScanSource::new(&mut *ss);
                        let drove = drive_claim(
                            &mut src, nls, inner, agg, estate, &range, &interrupted,
                        );
                        let settled =
                            if heapfeed_v2_enabled() { src.end_claim(estate) } else { Ok(()) };
                        drove?;
                        settled?;
                    }
                    // Cumulative partial export (in place), once per claim —
                    // the worker's last export precedes its settle, and
                    // therefore RG completion (the runtime_scan discipline).
                    let slot = worker - self.pins_base;
                    let mut g = self.partials[slot].lock().unwrap_or_else(|p| p.into_inner());
                    if poly_mode(&aps.agg) {
                        agg_poly_export_partial_into(
                            &aps.agg,
                            g.get_or_insert_with(Default::default),
                        )?;
                    } else {
                        agg_runtime_export_partial_into(
                            &aps.agg,
                            g.get_or_insert_with(Default::default),
                        )?;
                    }
                    Ok(())
                })
            })
        })
    }
}

/// One claimed block range: position the outer scan, then per staged page
/// batch replay each surviving outer row (the emit re-checks the scan qual
/// per row, the fold-drain discipline) through the per-outer-row NestLoop
/// program — accept (bind nestParams, rescan the private inner index scan)
/// then drain the expansion, each joined row into the plain-agg fold. At a
/// claim's end the last outer row's expansion is fully drained
/// (`nl_NeedNewOuter` back to true), so consecutive claims compose exactly
/// like consecutive outer rows do on the serial path.
fn drive_claim<'mcx, S, F>(
    src: &mut S,
    nls: &mut ::nodenestloop::NestLoopState<'mcx>,
    inner: &mut crate::procnode::PlanStateNode<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    range: &runtime::MorselRange,
    interrupted: &F,
) -> PgResult<()>
where
    S: BatchGranuleSource<'mcx>,
    F: Fn() -> bool,
{
    src.position(estate, runtime::MorselRange { start: range.start, end: range.end })?;
    let clear_inline = !heapfeed_v2_enabled();
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            if clear_inline {
                // End of claim: drop the scan slot's pin (fold drain
                // parity). Knob-ON this moves to the source's end_claim.
                let ss = require_bridge(src)?;
                let mcx = estate.es_query_cxt;
                ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            }
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Emit-dead word skip over the staged kernel-qual bitmap (the
        // perrow_fold_drain discipline): a cleared skip-sel bit is a row the
        // emit rejects with no observable effect — same rows, same order,
        // same errors. Words snapshotted (the emit re-borrows the source).
        let skip = {
            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
            src.skip_sel().map(|s| {
                w[..s.len()].copy_from_slice(s);
                w
            })
        };
        ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
            if let Some(outer_slot) = src.emit(estate, i)? {
                ::nodenestloop::lane_accept_outer(nls, inner, estate, outer_slot)?;
                while let Some(j) = ::nodenestloop::lane_probe_next(nls, inner, estate)? {
                    ::nodeagg::agg_plain_build_accept(agg, estate, j)?;
                }
            }
            Ok(())
        })?;
        if interrupted() {
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: entry-task drive (bind-once — parallel_worker_body's
// init is a strict superset of the query-task binder's bind).
// ---------------------------------------------------------------------------

fn runtime_nlindex_worker_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = shared.private() else { return Ok(()) };
    let Ok(payload) = private.downcast::<RuntimeNlIndexShared>() else { return Ok(()) };
    parallel::gtrace("wn.entry.drive.begin");
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive_entry(&payload)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            mark_self_errored();
            payload.fail(PgError::new(ERROR, "runtime nlindex helper panicked").into());
            let _ = teardown_worker_exec(false);
            if parallel::standing::is_exit_unwind(&*unwind) {
                latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                    shared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(
                ERROR,
                "runtime nlindex worker failed (see leader error)",
            )))
        }
    };
    parallel::gtrace("wn.entry.drive.end");
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
    outcome
}

fn helper_drive_entry(payload: &Arc<RuntimeNlIndexShared>) -> PgResult<()> {
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return Ok(()) };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut local = lane.local();
    payload.started.fetch_add(1, Ordering::SeqCst);
    if let Err(e) = build_worker_exec(payload) {
        payload.fail(e);
        return Ok(());
    }
    let _outcome = payload.rt.drive_pinned(&mut local, &rg);
    if wfin_enabled() {
        emit_wfin("nlindex-launched", lane.ordinal(), &local, &rg);
    }
    let self_errored =
        WORKER_NLEXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if let Err(e) = teardown {
        payload.fail(e);
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime nlindex worker failed (see leader error)",
        )));
    }
    if self_errored {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime nlindex worker failed (see leader error)",
        )));
    }
    Ok(())
}

fn build_worker_exec(payload: &RuntimeNlIndexShared) -> PgResult<()> {
    WORKER_NLEXEC.with(|cell| -> PgResult<()> {
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
                let x = q.exec.as_mut().expect("runtime nlindex worker ExecutorStart");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker plan is not a plain Agg root",
                        )));
                    };
                    let aps = &mut **aps;
                    if !agg_runtime_partial_admissible(&aps.agg) && !poly_mode(&aps.agg) {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker fold plan diverged from the leader's",
                        )));
                    }
                    // Shape re-validation (fail-closed: the worker's
                    // ExecutorStart rebuilt the tree from the transferred
                    // pstmt — it must be the admitted shape). The inner
                    // index scan stays a PRIVATE Volcano child: nothing to
                    // attach; its scan desc opens lazily on the first
                    // per-outer-row rescan, exactly the serial first-row
                    // path.
                    let crate::procnode::PlanStateNode::NestLoop(nlnode) = &mut aps.outer
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker outer node diverged from the leader's",
                        )));
                    };
                    let crate::procnode::NestLoopNode { outer, inner, .. } = nlnode;
                    if !matches!(&**outer, crate::procnode::PlanStateNode::SeqScan(_))
                        || !matches!(&**inner, crate::procnode::PlanStateNode::IndexScan(_))
                    {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime nlindex worker join children diverged from the leader's",
                        )));
                    }
                    ::nodeagg::agg_plain_build_begin(&mut aps.agg, estate)?;
                    Ok(())
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() = Some(WorkerNlExec {
                    qd,
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

/// Tear down the helper's executor (the runtime_scan discipline: clean =
/// finish/end/free; errored = release and let the transaction abort clean
/// up).
fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_NLEXEC.with(|cell| -> PgResult<()> {
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

/// PRIVATE_SHUTDOWN hook: DestroyParallelContext calls this BEFORE waiting
/// for worker exit — abort the RG so every helper's drive loop observes
/// completion and exits (idempotent on completed RGs). No standing channel
/// on this arm (stage 1: launched only).
fn runtime_nlindex_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeNlIndexShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_nlindex_main",
            runtime_nlindex_worker_main,
        );
        parallel::register_parallel_private_shutdown(runtime_nlindex_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission.
// ---------------------------------------------------------------------------

/// Planner-estimate admission floor (OUTER rows): below it the serial NL
/// drive wins outright and launching helpers is pure overhead. PROVISIONAL
/// (the m5-floors philosophy — refusal = fail-closed to the classic path,
/// traced by name); GL-NLIDX-1 owns the re-measure.
fn min_outer_rows() -> f64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_NLINDEX_MIN_OUTER_ROWS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(10_000)
    }) as f64
}

/// Leader-side parallel-safety walk over worker-transferred expressions,
/// widened by ONE deliberate admission over the sibling arms' walk:
/// PARAM_EXEC references whose paramid is a member of `allowed_params` (the
/// join's own nestParams — bound per outer row inside each worker) pass;
/// every other Param, and everything the sibling walk refuses, refuses.
struct NlSafetyCx<'a, 'mcx> {
    safe: bool,
    allowed_params: &'a ::types_nodes::bitmapset::Bitmapset<'mcx>,
}

impl NlSafetyCx<'_, '_> {
    fn check_func(&mut self, funcid: ::types_core::Oid) {
        if self.safe {
            match ::lsyscache::func_parallel(funcid) {
                Ok(p) if p == b's' as i8 => {}
                _ => self.safe = false,
            }
        }
    }
}

impl<'mcx> ::nodes_core::NodeWalker<'mcx> for NlSafetyCx<'_, 'mcx> {
    fn visit(&mut self, n: Node<'mcx>) -> PgResult<bool> {
        if !self.safe {
            return Ok(true);
        }
        match n.node_tag() {
            NodeTag::T_Var | NodeTag::T_Const | NodeTag::T_TargetEntry | NodeTag::T_List => {}
            NodeTag::T_Param => {
                let p = n.as_param().unwrap();
                if !(p.paramkind == ::types_nodes::primnodes::ParamKind::PARAM_EXEC
                    && self.allowed_params.is_member(p.paramid))
                {
                    self.safe = false;
                    return Ok(true);
                }
            }
            NodeTag::T_OpExpr => {
                let op = n.as_op_expr().unwrap();
                self.check_func(op.opfuncid);
            }
            NodeTag::T_FuncExpr => {
                let f = n.as_func_expr().unwrap();
                self.check_func(f.funcid);
            }
            NodeTag::T_ScalarArrayOpExpr => {
                let s = n.as_scalar_array_op_expr().unwrap();
                self.check_func(s.opfuncid);
            }
            NodeTag::T_BoolExpr
            | NodeTag::T_NullTest
            | NodeTag::T_BooleanTest
            | NodeTag::T_RelabelType
            | NodeTag::T_CaseExpr
            | NodeTag::T_CaseWhen
            | NodeTag::T_CoalesceExpr => {}
            // Anything else (SubPlans, SRFs, coercions with side tables, ...)
            // refuses — fail-closed.
            _ => {
                self.safe = false;
                return Ok(true);
            }
        }
        ::nodes_core::expression_tree_walker(n, self)
    }
}

fn exprs_parallel_safe_nl<'mcx>(
    nodes: impl Iterator<Item = Node<'mcx>>,
    allowed_params: &::types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<bool> {
    let mut cx = NlSafetyCx { safe: true, allowed_params };
    for n in nodes {
        use ::nodes_core::NodeWalker as _;
        cx.visit(n)?;
        if !cx.safe {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Record-and-refuse: name the verdict in the trace channel (armed only)
/// and tick the lane accounting under the join's class.
fn refused<T>(reason: &'static str, tick: RefuseReason) -> PgResult<Option<T>> {
    stats::tick_refused(ShapeClass::NestLoop, tick);
    if lane_trace_enabled() {
        lane_trace(&format!("runtime-nlindex: refused ({reason})"));
    }
    Ok(None)
}

/// The plain-agg-over-NestLoop(SeqScan, IndexScan) runtime arm entry.
/// `None` = not engaged (caller falls through byte-identically — nothing
/// was consumed; the leader's own nodes are untouched on every refusal
/// path, and on engagement only the outer scan's desc was opened for
/// geometry, which the serial drive opens identically on first pull).
pub(crate) fn try_own_plain_agg_runtime_nl_index<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Arming + kill-switch layering (all cheap; absent = today's path,
    // zero ticks — the armed-but-dormant OLTP gate law). GL-NLIDX-2: the
    // dop resolves through the M5-1 router — bench pool option verbatim
    // when SET (the FORCED posture), else engine=runtime + the arm's
    // ENABLE switch at pgrust.runtime_dop (the ROUTED posture, fed by the
    // planner's nlidx Gather suppression).
    let dop = super::router::arm_dop(super::router::ArmClass::NlIndex);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    // FORCED vs ROUTED floors: the bench pool path keeps the outer-row
    // floor (armed sessions must cheap-refuse OLTP-sized shapes); a routed
    // shape was already priced by the planner's polarity guards + min_dop
    // band — re-flooring it on post-qual outer rows would refuse exactly
    // the census family (tiny post-qual driver, big scan) the route exists
    // for. The block floor below governs both.
    let forced = runtime_nlindex_pool_dop() > 0;
    let Some(rt) = runtime::global() else { return Ok(None) };

    // --- Plan shape (pointer chases only before the floor).
    let nlplan = nl.state.plan;
    let Some(outer_plan_node) = nlplan.join.plan.lefttree else { return Ok(None) };
    let Some(outer_plan) = outer_plan_node.as_seq_scan() else {
        return refused("outer-not-seqscan", RefuseReason::NonScanChild);
    };
    let Some(inner_plan_node) = nlplan.join.plan.righttree else { return Ok(None) };
    let Some(inner_plan) = inner_plan_node.as_index_scan() else {
        return refused("inner-not-indexscan", RefuseReason::NonScanChild);
    };
    // SIZE FLOOR FIRST (planner estimate of the driving side's rows — the
    // pre-scan signal this arm has): the refusal path stays pointer chases
    // only — no agg walks, no expression-safety walks before the floor.
    // FORCED posture only (see the dop derivation above).
    if forced && outer_plan.scan.plan.plan_rows < min_outer_rows() {
        return refused("tiny-outer-floor", RefuseReason::TinyInputFloor);
    }

    // --- Join side: C's state machine untouched (whole-life ownership) +
    // the serial lane's own admissibility (uninstrumented; subplan-free,
    // exec-param-free joinqual/otherqual/projection).
    if !::nodenestloop::lane_nest_loop_admissible(&nl.state)
        || !::nodenestloop::lane_nest_loop_untouched(&nl.state, estate)
    {
        return refused("join-shape", RefuseReason::JoinShape);
    }

    // --- Agg side: plain fold, per-row drivable, exactly-combinable
    // partials (the order-insensitivity law).
    if !::nodeagg::agg_plain_perrow_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        if lane_trace_enabled() {
            lane_trace("runtime-nlindex: refused (agg-not-drainable)");
        }
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) && !poly_mode(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        if lane_trace_enabled() {
            lane_trace("runtime-nlindex: refused (partials-not-exact)");
        }
        return Ok(None);
    }

    // --- Session gates.
    if estate.es_epq_active {
        return refused("epq", RefuseReason::Epq);
    }
    if super::runtime_in_parallel_role() {
        return refused("in-parallel-mode", RefuseReason::ParallelGate);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return refused("extern-params", RefuseReason::SubplanParam);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    // Subplans refuse outright (initplan outputs are PARAM_EXEC slots this
    // arm does not transfer; the nestParams widening below is the ONLY
    // admitted exec-param source).
    if leader_pstmt.subplans.iter().next().is_some() {
        return refused("subplans", RefuseReason::SubplanParam);
    }
    // The Agg must be the plan root (workers ExecutorStart the whole worker
    // pstmt; a deeper Agg would drag unrelated plan into every helper).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else {
        return refused("agg-not-plan-root", RefuseReason::JoinShape);
    };
    if !std::ptr::eq(root_agg, agg.plan) {
        return refused("agg-not-plan-root", RefuseReason::JoinShape);
    }
    // MVCC snapshot (per-worker visibility parity with the serial drive).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return refused("non-mvcc-snapshot", RefuseReason::NonMvccSnapshot);
    }

    // --- Inner side: a forward, non-parallel-aware, non-reorder btree
    // IndexScan whose life this engagement owns (private Volcano child per
    // worker; exec-param rescan keys are the POINT of the shape and are
    // admitted — bound per outer row inside each worker's own estate).
    {
        let crate::procnode::PlanStateNode::IndexScan(is) = &*nl.inner else {
            return refused("inner-not-indexscan", RefuseReason::NonScanChild);
        };
        if is.iss_ParallelAware {
            return refused("inner-parallel-aware", RefuseReason::ParallelGate);
        }
        if is.iss_ScanDesc.is_some() {
            return refused("inner-scan-touched", RefuseReason::JoinShape);
        }
        if is.iss_OrderBy.is_some() {
            return refused("inner-reorder", RefuseReason::OrderByReorder);
        }
        if !::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir) {
            return refused("inner-desc-order", RefuseReason::DescOrder);
        }
        if !is
            .iss_RelationDesc
            .as_ref()
            .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
        {
            return refused("inner-non-btree", RefuseReason::NonBtree);
        }
    }

    // --- Expression safety (they run on helpers). The outer scan and the
    // join-level expressions must be param-free parallel-safe; the inner
    // scan's expressions admit exactly the join's own nestParams.
    let nest_params = ::nodenestloop::lane_nest_param_set(&nl.state);
    let strict = ::types_nodes::bitmapset::Bitmapset::empty();
    for list in [
        &outer_plan.scan.plan.qual,
        &outer_plan.scan.plan.targetlist,
        &nlplan.join.plan.qual,
        &nlplan.join.plan.targetlist,
        &nlplan.join.joinqual,
    ] {
        if !exprs_parallel_safe_nl(list.iter(), &strict)? {
            return refused("exprs-not-parallel-safe", RefuseReason::SubplanParam);
        }
    }
    for list in [
        &inner_plan.scan.plan.qual,
        &inner_plan.scan.plan.targetlist,
        &inner_plan.indexqual,
        &inner_plan.indexqualorig,
    ] {
        if !exprs_parallel_safe_nl(list.iter(), nest_params)? {
            return refused("inner-exprs-not-parallel-safe", RefuseReason::SubplanParam);
        }
    }

    // Binder policy sources must be empty — a set flag means every helper
    // bind would refuse; don't launch at all.
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        return refused("binder-policy", RefuseReason::ParallelGate);
    }

    // --- Outer geometry: a lane-fusible HEAP SeqScan with enough blocks to
    // be worth a gang (the batched page drive's own refuse-set decides
    // fusibility; pgrcolumnar outers are out of this arm's scope — the NL
    // family it targets is the heap census family).
    let (map, total_granules) = {
        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *nl.outer else {
            return refused("outer-not-seqscan", RefuseReason::NonScanChild);
        };
        if !::nodeseqscan::seq_scan_is_heap(ss) {
            return refused("outer-not-heap", RefuseReason::NoPageBatch);
        }
        if !super::seq_scan_fusible(ss, estate)? {
            return refused("outer-not-fusible", RefuseReason::ChildScanRefused);
        }
        let Some(map) = SeqScanSource::new(&mut *ss).granule_map(estate)? else {
            // Empty relation: nothing to morselize — serial answers it.
            return Ok(None);
        };
        let map = Arc::new(map);
        let total = map.total();
        (map, total)
    };
    if total_granules < 2 * dop as u64 {
        return refused("tiny-block-floor", RefuseReason::TinyInputFloor);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    let dop = elastic_dop(dop, total_granules);
    engage(agg, estate, rt, dop, map, total_granules)
}

fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    map: Arc<runtime::GranuleMap>,
    total_granules: u64,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let agg_node = estate.es_plannedstmt.and_then(|p| p.planTree).expect("gated above");
    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;

    let payload = Arc::new(RuntimeNlIndexShared {
        rt,
        rg: OnceLock::new(),
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
        pins_base: rt.nthreads(),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
    });

    parallel::gtrace("ln.engage.begin");
    xact::EnterParallelMode();
    let engaged = engage_ceremony(estate, rt, dop, map, total_granules, &payload);
    xact::ExitParallelMode();
    parallel::gtrace("ln.engage.end");

    match engaged? {
        EngageOutcome::Fallback => {
            lane_trace("runtime-nlindex: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let parts: Vec<RuntimePartial> = payload
                .partials
                .iter()
                .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
                .collect();
            stats::tick_owned(ShapeClass::NestLoop);
            lane_trace(&format!("runtime-nlindex: complete, partials={}", parts.len()));
            if poly_mode(agg) {
                let combined = agg_poly_runtime_combine(agg, &parts)?;
                Ok(Some(exec_agg_poly_runtime_partials(agg, estate, &combined)?))
            } else {
                let combined = agg_runtime_combine(agg, &parts)?;
                Ok(Some(::nodeagg::runtime_partial::exec_agg_runtime_partials(
                    agg, estate, &combined,
                )?))
            }
        }
    }
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// Everything between Enter/ExitParallelMode: create the context, submit
/// the pinned RG, launch, park (completion poll + parallel-message drain +
/// CFI + latch quantum), reap. Mirrors runtime_index's ceremony (stage 1:
/// launched only, no standing channel).
fn engage_ceremony(
    estate: &mut EStateData<'_>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    map: Arc<runtime::GranuleMap>,
    total_granules: u64,
    payload: &Arc<RuntimeNlIndexShared>,
) -> PgResult<EngageOutcome> {
    let _ = estate;
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_nlindex_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        parallel::set_private(pcxt, Arc::clone(payload) as _);

        // Heap block-range claims: boundary-free geometry, sizer-truncated,
        // non-coalescing — the runtime_scan heap posture verbatim (a
        // boundary-free source opting into whole-boundary claims would take
        // the pipeline in one claim).
        let source: Arc<dyn runtime::MorselSource> =
            Arc::new(runtime::GranuleMapSource::new(map, false, false));
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(payload) as _;
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64 | (1 << 61),
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-nlindex: zero workers launched");
            drain_rg_raw(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-nlindex: engaged dop={launched} blocks={total_granules}"
        ));

        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg_raw(rt, &rg);
                return Err(e);
            }
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-nlindex: all {refused} helpers refused"));
                rg.abort();
                drain_rg_raw(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-nlindex: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg_raw(rt, &rg);
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime nlindex helpers exited before completing the join",
                )));
            }
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg_raw(rt, &rg);
                return Err(e);
            }
        };
        if wfin_enabled() {
            eprintln!(
                "MORSEL|LFIN|qid={}|t_us={}|granules={}|rgs=0|started={}|refused={}|chan=nlindex-launched",
                rg.query_id(),
                rt.now_ns() / 1000,
                total_granules,
                payload.started.load(Ordering::SeqCst),
                payload.refused.load(Ordering::SeqCst),
            );
        }

        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime nlindex pipeline aborted")));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed (helpers reference the leader arena).
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg_raw(rt, rg);
        }
    }
    parallel::gtrace("ln.destroy.begin");
    let destroy = parallel::DestroyParallelContext(pcxt);
    parallel::gtrace("ln.destroy.end");
    let outcome = body?;
    destroy?;
    Ok(outcome)
}
