//! M1 RUNTIME SCAN PIPELINES — the first real pipeline on the morsel
//! runtime (docs/design/parallelism-redesign-2026-07.md §2.1/§2.2/§5-M1).
//!
//! Shape: a SERIAL-plan plain Agg over a cbstore SeqScan (COUNT/plain-agg
//! fold shapes with optional PREWHERE quals — the lane's simplest fold
//! pipelines), executed as ONE runtime TaskSet at DOP N. The plan surface
//! stays the serial plan (force-plans discipline: EXPLAIN unchanged, no
//! planner change); engagement is FORCED/explicit:
//!
//!   PGRUST_RUNTIME=1  (pool spawned at postmaster start, M0 kill switch)
//!   SET pgrust.runtime_scan_pool = <dop>   (arming, runtime_pool.rs)
//!
//! Execution model (submit-and-park, §2.5):
//!  * The LEADER creates a parallel context (vacuumparallel precedent — no
//!    Gather node), installs the query-task binding policy, submits a
//!    PINNED resource group (one task set: CbstoreGranuleSource + the fold
//!    work), launches N helpers through the ordinary wpool machinery, and
//!    PARKS — a WaitForParallelWorkersToFinish-shaped loop (completion poll
//!    + ProcessParallelMessages + CHECK_FOR_INTERRUPTS + latch quantum).
//!  * Each HELPER runs a trivial entry task, and at POST_TASK_PARK — the
//!    hook reserved for exactly this ("the runtime pool/scheduler lane owns
//!    the production park") — binds the query's state through the
//!    QUERY-TASK BINDER (`parallel::with_query_task_binding`, its first
//!    production caller; fail-closed: anything the binder refuses simply
//!    does not participate) and drives the pinned RG's morsels through the
//!    runtime (`Runtime::drive_pinned`): duration-adaptive granule claims,
//!    photo-finish shutdown, last-worker-out finalization.
//!  * Each morsel = a whole-granule range inside one row group (=dict
//!    epoch); the helper positions its own scan (`set_granule_range`) and
//!    re-enters the UNCHANGED serial fold drain (`agg_plain_fold_drain`).
//!  * Partials are exported after EVERY morsel into the worker's slot
//!    (overwrite; export is a few pergroup reads) — so by the time the RG
//!    completes, every contributing worker's FINAL state is installed (its
//!    last export happened inside its last task, before its settle).
//!  * The leader absorbs the combined partial (order-insensitive-exact
//!    kinds only, nodeagg::runtime_partial) and runs the ordinary plain
//!    finalize — output rows byte-identical to the serial arm.
//!
//! Fail-closed admission (refuse ⇒ fall through to the serial fold arm,
//! byte-identically): fold-classified plan with no residual transitions and
//! whitelisted kinds only; no params (extern or exec); binder policy
//! sources empty (no temp namespace, not serializable, no pending invals);
//! parallel-safe scan expressions; MVCC snapshot; not already in parallel
//! mode / a parallel worker; cbstore scan admitted by the lane gate;
//! first-class dynamic gates (EPQ / instrumented / backward) via
//! `seq_scan_fusible`. If every launched helper refuses the bind before
//! any granule is claimed, the leader aborts the (untouched) RG and falls
//! back to the serial arm — engagement can only produce the serial answer
//! or a real error, never a partial one.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::{
    agg_runtime_combine, agg_runtime_export_partial, agg_runtime_partial_admissible,
    exec_agg_runtime_partials, RuntimePartial,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible};

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the runtime task
// set's work body (one struct, one Arc).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeScanShared {
    rt: &'static Arc<runtime::Runtime>,
    /// Weak: the RG's task set holds this struct as its work body — a strong
    /// handle here would leak the cycle. Upgrade fails only after the leader
    /// dropped its handles, at which point nothing executes morsels.
    rg: OnceLock<runtime::WeakRgHandle>,
    /// The context's shared state (binder target). Set right after
    /// InitializeParallelDSM, before any helper launches.
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    /// The worker PlannedStmt (build_worker_pstmt over the serial Agg root).
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// Pin-board base (= runtime nthreads): run_morsel's worker index minus
    /// this is the partial-slot (leased-lane) ordinal.
    pins_base: usize,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// First worker-phase error (fold/executor/binder-cleanup errors; the
    /// entry-phase errors ride the ordinary parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
    /// Per-ordinal cumulative partials, overwritten after every morsel.
    partials: Vec<Mutex<Option<RuntimePartial>>>,
}

impl RuntimeScanShared {
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
}

// ---------------------------------------------------------------------------
// The TaskSetWork body: dispatch one claimed granule range into the bound
// helper's thread-local executor. run_morsel is INFALLIBLE BY CONTRACT
// (drive_pinned doc): errors and panics are caught here, recorded, and turn
// into an RG abort — the runtime protocol never sees an unwind.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    /// THIS helper contributed an error (its executor may be mid-batch —
    /// take the release/abort teardown, not finish/end).
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

impl runtime::TaskSetWork for RuntimeScanShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            // Already aborting: the claim drains without work (aborted
            // generations need not execute every granule — sched.rs).
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
                self.fail(
                    PgError::new(ERROR, "runtime scan worker panicked in a morsel").into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // Nothing to do: partials are installed per morsel (each worker's
        // final export precedes its settle), and the leader combines them
        // after completion.
    }
}

impl RuntimeScanShared {
    fn morsel_body(&self, worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime scan morsel without a bound executor",
                )));
            };
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime scan worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker plan is not a plain Agg root",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker outer node is not a SeqScan",
                        )));
                    };
                    if !::nodeseqscan::seq_scan_cb_set_granule_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )? {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker scan is not cbstore",
                        )));
                    }
                    super::agg_plain_fold_drain(&mut aps.agg, ss, estate)?;
                    // Cumulative partial export (overwrite): the worker's
                    // LAST morsel's export — which precedes its settle, and
                    // therefore RG completion — is the one the leader reads.
                    let partial = agg_runtime_export_partial(&aps.agg)?;
                    let slot = worker - self.pins_base;
                    *self.partials[slot].lock().unwrap_or_else(|p| p.into_inner()) =
                        Some(partial);
                    Ok(())
                })
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: entry task + the POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

/// The launched task body: nothing — all real work happens bound, at
/// POST_TASK_PARK. (The full parallel_worker_body init/teardown around this
/// is what makes the helper a bindable parked helper: connected to the
/// leader's database, lock-group member, IsParallelWorker.)
fn runtime_scan_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

/// POST_TASK_PARK hook (global; fires for EVERY successful parallel worker
/// task): no-op unless the context's private payload is ours.
fn runtime_scan_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeScanShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime scan helper panicked").into());
    }
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeScanShared>) {
    let _ = shared;
    let Some(target) = payload.pcxt_shared.get() else { return };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return };
    // Process-wide lane lease: pin-board lanes are shared across every
    // concurrently-engaged query; exhaustion = fail-closed non-participation.
    let Some(lane) = payload.rt.acquire_external_lane() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, lane.ordinal(), &mut local, &rg)
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                payload.fail(e);
            } else {
                // Binder validate() refusal: fail-closed non-participation.
                // The leader detects the nobody-participates case and falls
                // back to the serial arm. Traced: a persistent refusal on
                // re-parked helpers is a helper-state bug, not a shape gate.
                lane_trace(&format!(
                    "runtime-scan: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

/// Bound execution: build this helper's executor over the shared worker
/// PlannedStmt, drive the pinned RG to completion, tear the executor down.
/// Runs INSIDE the query-task binding (transaction + snapshot + GUCs bound).
fn drive_bound(
    payload: &Arc<RuntimeScanShared>,
    ordinal: usize,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    // Teardown mode is per-HELPER: a foreign worker's error (or a cancel)
    // leaves THIS executor consistent — finish/end/free releases resources
    // cleanly under the about-to-commit binder unbind. Only a self-error
    // (executor possibly mid-batch) takes the release path and lets the
    // binder's transaction abort clean up.
    let self_errored = WORKER_EXEC
        .with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    let _ = ordinal;
    if self_errored {
        // Route the binder through its transaction-ABORT unbind: a released
        // executor may hold registered snapshots, and the normal unbind
        // asserts a cleared xmin (the m2-agg-sink lane hit this live; the
        // scan arm shares the teardown shape). The morsel body recorded the
        // real error first (fail() is first-wins).
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime scan worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

fn build_worker_exec(payload: &Arc<RuntimeScanShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        // Defensive: stale state from an aborted previous drive would alias
        // a freed leader arena.
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
                let x = q.exec.as_mut().expect("runtime scan worker ExecutorStart");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker plan is not a plain Agg root",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker outer node is not a SeqScan",
                        )));
                    };
                    if !agg_runtime_partial_admissible(&aps.agg) {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker fold plan diverged from the leader's",
                        )));
                    }
                    // The serial fold feed's arm/init half, once per worker;
                    // the drain half re-runs per morsel (fold feed split).
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::FoldPrefix { agg: &aps.agg },
                    )?;
                    super::arm_fold_len_lanes(&aps.agg, ss);
                    ::nodeagg::agg_plain_build_begin(&mut aps.agg, estate)?;
                    Ok(())
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

/// Tear down the helper's executor. `clean` = no error contributed by this
/// helper: full finish/end/free; otherwise release the QueryDesc and let
/// the binder's transaction abort clean up (the parallel_query_main error
/// discipline).
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

/// PRIVATE_SHUTDOWN hook (global): DestroyParallelContext calls this BEFORE
/// waiting for worker exit — release anything a helper could still be
/// parked on. For us: abort the RG (idempotent; a completed RG ignores it)
/// so every helper's drive loop observes completion and exits the hook.
fn runtime_scan_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeScanShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_scan_main",
            runtime_scan_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_scan_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_scan_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// The morsel source: cbstore granule geometry.
// ---------------------------------------------------------------------------

/// Granule-addressed morsel source over one cbstore part's geometry: claims
/// are whole-granule ranges that never cross a row-group edge — which is
/// exactly a dictionary-epoch edge (per-RG local dictionaries), so every
/// per-epoch memo (dict-eval, codehist, gmemo) stays worker-coherent and
/// every kernel invocation sees a single dictionary snapshot.
struct CbstoreGranuleSource {
    /// Row-group start prefix sums (len nrgs+1; last = total).
    starts: Vec<u64>,
}

impl runtime::MorselSource for CbstoreGranuleSource {
    fn total_granules(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        match self.starts.binary_search(&start) {
            Ok(i) => self.starts.get(i + 1).copied().unwrap_or_else(|| self.total_granules()),
            Err(i) => self.starts.get(i).copied().unwrap_or_else(|| self.total_granules()),
        }
    }

    /// Granules are 8,192 rows — large against Umbra's 16-tuple C0. Seed the
    /// ramp at 2 granules (~16K rows, tens of µs on fold shapes): one probe
    /// morsel sizes the pipeline without a giant first claim on tiny scans.
    fn startup_c0(&self) -> u64 {
        2
    }
}

// ---------------------------------------------------------------------------
// Leader-side parallel-safety walk (executor-side, fail-closed): the scan's
// qual + targetlist may run on helpers, so every function they invoke must
// be parallel-safe ('s') and every node shape known-transferable.
// ---------------------------------------------------------------------------

struct SafetyCx {
    safe: bool,
}

impl SafetyCx {
    fn check_func(&mut self, funcid: ::types_core::Oid) {
        if self.safe {
            match ::lsyscache::func_parallel(funcid) {
                Ok(p) if p == b's' as i8 => {}
                _ => self.safe = false,
            }
        }
    }
}

impl<'mcx> ::nodes_core::NodeWalker<'mcx> for SafetyCx {
    fn visit(&mut self, n: Node<'mcx>) -> PgResult<bool> {
        if !self.safe {
            return Ok(true);
        }
        match n.node_tag() {
            NodeTag::T_Var | NodeTag::T_Const | NodeTag::T_TargetEntry | NodeTag::T_List => {}
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
            // Anything else (Params, SubPlans, SRFs, coercions with side
            // tables, ...) refuses — fail-closed.
            _ => {
                self.safe = false;
                return Ok(true);
            }
        }
        ::nodes_core::expression_tree_walker(n, self)
    }
}

fn exprs_parallel_safe<'mcx>(nodes: impl Iterator<Item = Node<'mcx>>) -> PgResult<bool> {
    let mut cx = SafetyCx { safe: true };
    for n in nodes {
        use ::nodes_core::NodeWalker as _;
        cx.visit(n)?;
        if !cx.safe {
            return Ok(false);
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Leader-side engagement.
// ---------------------------------------------------------------------------

/// Env floor for engagement (granules): below it the serial fold wins
/// outright and launching helpers is pure overhead.
fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SCAN_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// The runtime scan arm. `None` = not engaged (caller falls through to the
/// serial fold/per-row arms, byte-identically — nothing was consumed).
/// `Some(row)` = the node's one finalized result row (agg_done set).
pub(super) fn try_own_plain_agg_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_scan_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };

    // --- Shape + session gates (fail-closed; every refusal is the serial arm).
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        return Ok(None);
    }
    // Not from within parallel machinery: helpers of helpers don't exist,
    // and a leader already in parallel mode (Gather in flight) must not
    // stack a second context here.
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        return Ok(None);
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        return Ok(None);
    }
    // The Agg must be the plan root (workers ExecutorStart the whole worker
    // pstmt; a deeper Agg would drag unrelated plan into every helper).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else { return Ok(None) };
    if !std::ptr::eq(root_agg, agg.plan) {
        return Ok(None);
    }
    // Scan expressions must be parallel-safe (they run on helpers).
    let Some(scan_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        return Ok(None);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        return Ok(None);
    }
    // MVCC snapshot (visibility folding parity with the serial drive).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Ok(None);
    }
    // Binder policy sources must be empty — a set flag means every helper
    // bind would refuse; don't launch at all.
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return Ok(None);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, starts)) = ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < min_granules().max(2 * dop as u64) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, total_granules, starts)
}

fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // Worker plan: the serial Agg root, execparallel's transfer shape.
    let agg_node = estate.es_plannedstmt.and_then(|p| p.planTree).expect("gated above");
    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;

    let payload = Arc::new(RuntimeScanShared {
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
        pins_base: rt.nthreads(),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
    });

    // Submit-and-park ceremony. EnterParallelMode brackets the context
    // lifetime (CreateParallelContext asserts it); an error unwind aborts
    // the transaction, which destroys live contexts and resets the mode
    // (AtEOXact_Parallel — the Gather discipline).
    xact::EnterParallelMode();
    let engaged = engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload);
    xact::ExitParallelMode();
    engaged
}

/// Everything between Enter/ExitParallelMode. On ANY exit path the parallel
/// context is destroyed (helpers joined) and the RG is completed and
/// drained before the leader's arena can be freed.
#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeScanShared>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_scan_main", dop)?;

    // Every exit past submission must complete the pinned RG (nobody but
    // this frame's helpers can drive it): held OUTSIDE the body closure so
    // `?`-propagated errors reap it too.
    let mut submitted: Option<runtime::RgHandle> = None;

    // From here on, every exit runs the teardown tail below.
    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(
            pcxt,
            parallel::QueryTaskBindingPolicy::default(),
        )?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);

        // Submit the pinned RG before launch: helpers find work immediately.
        let source = Arc::new(CbstoreGranuleSource { starts });
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(payload) as _;
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-scan: zero workers launched");
            drain_rg(rt, payload, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-scan: engaged dop={launched} granules={total_granules}"
        ));

        // Submit-and-park: completion poll + parallel-message drain + CFI +
        // bounded latch quantum (the WaitForParallelWorkersToFinish shape —
        // CompletionWaiter alone would be deaf to worker errors/cancel).
        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg(rt, payload, &rg);
                return Err(e);
            }
            // All-refused with nothing claimed: nobody will ever drive the
            // RG — reap it and fall back to the serial arm (nothing was
            // consumed; the fold arm re-runs from scratch).
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-scan: all {refused} helpers refused the bind"));
                rg.abort();
                drain_rg(rt, payload, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            parallel::wait_parallel_finish_quantum();
        };

        // Worker-phase error (fold error, binder cleanup, panic): rethrow —
        // plain, no extra context, exactly the serial arm's error surface.
        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            // Aborted without a recorded error: a cancel raced us — let the
            // pending interrupt surface; otherwise report the abort.
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime scan pipeline aborted",
            )));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            // Completed but nobody participated (all refused between the
            // poll and completion — possible only for an empty task set).
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before
    // the parallel context is destroyed and before this frame's arena can
    // unwind (helpers reference it). Ordinary completion already happened
    // on the Ok paths; error paths reap here (idempotent).
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, payload, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match outcome {
        EngageOutcome::Fallback => {
            lane_trace("runtime-scan: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let parts: Vec<RuntimePartial> = payload
                .partials
                .iter()
                .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
                .collect();
            let combined = agg_runtime_combine(agg, &parts)?;
            stats::tick_owned(ShapeClass::AggBuild);
            lane_trace(&format!(
                "runtime-scan: complete, partials={} ",
                parts.len()
            ));
            Ok(Some(exec_agg_runtime_partials(agg, estate, &combined)?))
        }
    }
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// Reap a pinned RG no helper will drive (abort/fallback paths): the leader
/// drives the protocol itself — the closed generation refuses every join,
/// so no morsel work runs; the drive just executes invalidate/finalize/
/// completion. This is cleanup driving, not leader work execution (§2.5).
fn drain_rg(
    rt: &'static Arc<runtime::Runtime>,
    payload: &Arc<RuntimeScanShared>,
    rg: &runtime::RgHandle,
) {
    let _ = payload;
    rg.abort();
    // Lane exhaustion here would strand the RG; spin-wait for one (bounded
    // by helper drives, which settle within a morsel).
    let lane = loop {
        if let Some(l) = rt.acquire_external_lane() {
            break l;
        }
        std::thread::yield_now();
    };
    let mut local = lane.local();
    let _ = rt.drive_pinned(&mut local, rg);
}
