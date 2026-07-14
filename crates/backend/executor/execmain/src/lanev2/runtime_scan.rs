//! M1 RUNTIME SCAN PIPELINES — the first real pipeline on the morsel
//! runtime (docs/design/parallelism-redesign-2026-07.md §2.1/§2.2/§5-M1).
//!
//! Shape: a SERIAL-plan plain Agg over a cbstore OR heap SeqScan
//! (COUNT/plain-agg fold shapes with optional PREWHERE/kernel quals — the
//! lane's simplest fold pipelines), executed as ONE runtime TaskSet at DOP
//! N. The morsel source dispatches on the table AM: cbstore granules with
//! row-group hard boundaries ([`CbstoreGranuleSource`]) or heap block
//! ranges with none ([`HeapBlockSource`] — C's
//! table_block_parallelscan_nextpage chunked claim, runtime-adaptive).
//! Heap visibility is per tuple inside the page batch, under the leader
//! snapshot the task binding restores — exactly a C parallel seq scan
//! worker's check. The plan surface
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
//! mode / a parallel worker; a cbstore or plain heap scan admitted by the
//! lane gate; fold-mode shapes prove the fold prefix arms on the leader;
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
    agg_runtime_combine, agg_runtime_export_partial_into, agg_runtime_partial_admissible,
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
    /// inc-2 bind-once: helpers drive from the ENTRY TASK (already fully
    /// bound by parallel_worker_body — a strict superset of the query-task
    /// binder's bind) instead of re-binding at POST_TASK_PARK. The hook
    /// skips these payloads. PGRUST_RUNTIME_ENTRY_DRIVE=0 restores the M1
    /// hook path (kill-switch layering).
    drive_at_entry: bool,
    /// inc-3 standing channel: the live board entry, held so the
    /// PRIVATE_SHUTDOWN hook can complete the standing join (abort +
    /// drain + await detach) on leader unwind paths that never reach
    /// standing_wait's own cleanup — the launched path gets the same
    /// guarantee from DestroyParallelContext's worker-exit wait.
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
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

#[derive(Clone, Copy, PartialEq)]
enum DriveMode {
    /// Fold-classified shape reading lane columns: the serial fold drain.
    Fold,
    /// Count-only census shape (fold plan reads NO columns; the serial
    /// decider refuses it because the footer is the serial lever): the
    /// census drain — kernel-qual selection bitmap counted whole-word via
    /// `fold_batch` (CountStar reads no lanes), fallback/scalar-qual rows
    /// through the per-row program. Graceful when no SoA/bitmap staged.
    Census,
    /// DIRECT MORSEL DRIVE (rowdrive car 1): bare heap count(*) — the
    /// serial fused storeless batch advance (`exec_agg_batched`'s
    /// count-star arm: one checked add per page batch of visible rows,
    /// zero per-row work) run UNCHANGED per block-range claim. No staging,
    /// no fold plan reads, no sink: the per-worker partial is the plain
    /// count transvalue the ordinary partial export/combine already
    /// carries. Admission is the census gate's qual-required carve-out
    /// (heap only, above a block floor, PGRUST_RUNTIME_ROWDRIVE kill).
    StorelessCount,
    /// DIRECT MORSEL DRIVE (rowdrive car 2): heap FOLD shapes whose fold
    /// prefix is UNARMABLE (qual/transition column outside the fixed-width
    /// prefix deform — the m1 LIKE-fold boundary: `count/sum WHERE url
    /// LIKE '%…%'`, text-first tables). The serial row path runs per
    /// claim: RowFeed staging (kernel-qual selection bitmap when the qual
    /// has kernel shape) + `seq_scan_batch_emit` (fetch + qual, per-row
    /// program for fallback rows) + `agg_plain_build_accept` (exec_agg's
    /// single-group loop body verbatim). Serial decide economics are
    /// untouched (heap Refuse stays with the fused per-row drive
    /// serially); DOP N is the whole win. Partials ride the classified
    /// fold plan's export exactly like Fold (`agg_runtime_partial_
    /// admissible` requires zero residuals — fail-closed).
    PerRowFold,
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    mode: DriveMode,
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
            let mode = ex.mode;
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
                    ::nodeseqscan::seq_scan_set_morsel_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )?;
                    match mode {
                        DriveMode::Fold => {
                            super::agg_plain_fold_drain(&mut aps.agg, ss, estate)?
                        }
                        DriveMode::Census => census_drain(&mut aps.agg, ss, estate)?,
                        DriveMode::StorelessCount => {
                            storeless_count_drain(&mut aps.agg, ss, estate)?
                        }
                        DriveMode::PerRowFold => {
                            perrow_fold_drain(&mut aps.agg, ss, estate)?
                        }
                    }
                    // Cumulative partial export (in place): the worker's
                    // LAST morsel's export — which precedes its settle, and
                    // therefore RG completion — is the one the leader reads.
                    // The slot's partial is reused across morsels (retained
                    // capacity; a fresh Vec per morsel was a malloc+free
                    // pair on the engaged path — m2-integration audit).
                    let slot = worker - self.pins_base;
                    let mut g =
                        self.partials[slot].lock().unwrap_or_else(|p| p.into_inner());
                    agg_runtime_export_partial_into(
                        &aps.agg,
                        g.get_or_insert_with(Default::default),
                    )?;
                    Ok(())
                })
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: entry task + the POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

/// The launched task body. M1 shape: nothing — all real work happens bound,
/// at POST_TASK_PARK. (The full parallel_worker_body init/teardown around
/// this is what makes the helper a bindable parked helper: connected to the
/// leader's database, lock-group member, IsParallelWorker.)
///
/// inc-2 bind-once (payload.drive_at_entry): the entry task is ALREADY the
/// bound context — parallel_worker_body's init is a strict superset of the
/// query-task binder's bind (identity, lock group, db connect, transaction,
/// relmap/combocid, snapshots, sinval drain, GUC pin, namespace, client,
/// parallel mode) — so drive the pinned RG right here and skip the
/// unbind->binder-rebind->unbind cycle M1-a attributed. Error surface is
/// preserved: fold/executor errors are recorded in the payload (the leader
/// rethrows them plain, the serial arm's surface) and the entry task
/// returns Ok — never the parallel message channel, exactly the hook
/// path's discipline. The binder's validate() is unnecessary here: the
/// leader's fail-closed admission (policy probe, params, MVCC snapshot)
/// covered the session gates, and the body itself established db/leader/
/// worker-number; the only per-helper refusal left is lane exhaustion.
fn runtime_scan_worker_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = shared.private() else { return Ok(()) };
    let Ok(payload) = private.downcast::<RuntimeScanShared>() else { return Ok(()) };
    if !payload.drive_at_entry {
        return Ok(());
    }
    parallel::gtrace("w.entry.drive.begin");
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive_entry(&payload)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            mark_self_errored();
            payload.fail(PgError::new(ERROR, "runtime scan helper panicked").into());
            let _ = teardown_worker_exec(false);
            if parallel::standing::is_exit_unwind(&*unwind) {
                // Exit-committed unwind: keep dying (ParallelWorkerMain's
                // FATAL arm owns the leader notification).
                latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                    shared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(
                ERROR,
                "runtime scan worker failed (see leader error)",
            )))
        }
    };
    parallel::gtrace("w.entry.drive.end");
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
    outcome
}

/// Entry-task drive: the bound-context body (lane lease + executor build +
/// pinned drive + teardown), errors recorded payload-side. Mirrors
/// helper_drive minus the binder wrap.
///
/// Self-error discipline: the error is recorded payload-side (the leader
/// rethrows it PLAIN — usually before it can drain the message channel,
/// because the completion poll precedes ProcessParallelMessages) AND
/// returned, so parallel_worker_body ABORTS the worker transaction — a
/// released mid-batch executor must not ride a commit (the hook path gets
/// the same via the binder's finish(false); resource-release-at-commit
/// would warn on leaked pins).
fn helper_drive_entry(payload: &Arc<RuntimeScanShared>) -> PgResult<()> {
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return Ok(()) };
    // Process-wide lane lease: exhaustion = fail-closed non-participation
    // (the leader's all-refused fallback counts it exactly like a bind
    // refusal on the hook path).
    let Some(lane) = payload.rt.acquire_external_lane() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut local = lane.local();
    payload.started.fetch_add(1, Ordering::SeqCst);
    if let Err(e) = build_worker_exec(payload) {
        // Clean build failure (qd already released): commit is safe.
        payload.fail(e);
        return Ok(());
    }
    let _outcome = payload.rt.drive_pinned(&mut local, &rg);
    emit_wfin("entry", lane.ordinal(), &local, &rg);
    // Teardown mode per drive_bound: self-error takes the release path;
    // the abort discipline is the transaction-level Err below (the hook
    // path gets the same via the binder's finish(false)).
    let self_errored = WORKER_EXEC
        .with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if let Err(e) = teardown {
        payload.fail(e);
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime scan worker failed (see leader error)",
        )));
    }
    if self_errored {
        // Mid-batch executor released: abort the worker transaction (the
        // morsel body already recorded the original error payload-side).
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime scan worker failed (see leader error)",
        )));
    }
    Ok(())
}

/// PGRUST_RUNTIME_ENTRY_DRIVE=0 restores the M1 POST_TASK_PARK drive.
fn entry_drive_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_ENTRY_DRIVE").map_or(true, |v| v.trim() != "0")
    })
}

/// POST_TASK_PARK hook (global; fires for EVERY successful parallel worker
/// task): no-op unless the context's private payload is ours.
fn runtime_scan_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        // F1 observability: a context with NO private payload can never be
        // driven by any arm — trace it (foreign-payload downcast misses stay
        // silent below: every arm's hook runs for every worker by design).
        lane_trace("runtime-scan: post-task-park without a private payload");
        return;
    };
    let Ok(payload) = private.downcast::<RuntimeScanShared>() else { return };
    if payload.drive_at_entry {
        // inc-2: the entry task already drove this engagement — a second
        // bind+drive here would rebuild the executor against a completed
        // (or aborted) RG for nothing.
        return;
    }
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
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-scan: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-scan: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    // Process-wide lane lease: pin-board lanes are shared across every
    // concurrently-engaged query; exhaustion = fail-closed non-participation.
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-scan: helper refused (no external lane)");
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
                // F1 liveness (the agg-arm wedge mechanism, closed here
                // too): a helper that errored BEFORE joining the drive
                // (build_worker_exec failure) has aborted the RG via
                // fail() — but an aborted PINNED RG still needs a driver to
                // run invalidate/finalize/complete, or the leader waits on
                // the all-stopped backstop's cadence. Drive the closed
                // generation to completion (pure protocol cleanup);
                // post-drive errors find it already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
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
    emit_wfin("bound", ordinal, local, rg);
    // Teardown mode is per-HELPER: a foreign worker's error (or a cancel)
    // leaves THIS executor consistent — finish/end/free releases resources
    // cleanly under the about-to-commit binder unbind. Only a self-error
    // (executor possibly mid-batch) takes the release path and lets the
    // binder's transaction abort clean up.
    let self_errored = WORKER_EXEC
        .with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
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

// ---------------------------------------------------------------------------
// WFIN drive markers — the m0-accept harness's worker-finish channel
// (fabled run-m0-parallel-accept.sh: `MORSEL|WFIN|qid=|pipe=|worker=|t_us=|
// tasks=|task_avg_us=` off server stderr; unknown key=value fields are
// ignored, so the extra decomposition fields are safe). Env-gated
// (PGRUST_WFIN=1, passed via CB_PGRUST_ENV on diagnosis runs): default-off,
// zero cost on the ladder's standing timing arms.
// ---------------------------------------------------------------------------

fn wfin_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_WFIN").map_or(false, |v| v.trim() == "1"))
}

/// This worker's cbstore drive counters (rg_switches, dict_builds,
/// granules_scanned, windows_staged) — read BEFORE teardown releases the qd.
fn worker_cb_counters() -> Option<(u64, u64, u64, u64)> {
    WORKER_EXEC.with(|cell| {
        let b = cell.borrow();
        let ex = b.as_ref()?;
        crate::querydesc::with_qd(ex.qd, |q| {
            let x = q.exec.as_mut()?;
            x.with_mut(|d| {
                let crate::procnode::PlanStateNode::Agg(aps) = d.planstate.as_mut()? else {
                    return None;
                };
                let crate::procnode::PlanStateNode::SeqScan(ss) = &aps.outer else {
                    return None;
                };
                ::nodeseqscan::seq_scan_cb_drive_counters(ss)
            })
        })
    })
}

/// One worker-finish marker per participant per drive. `t_us` is the
/// scheduler clock at the end of this worker's LAST executed morsel (parked
/// waiting after the pipeline's end does not count); the harness's spread =
/// max-min of t_us per qid across workers.
fn emit_wfin(
    chan: &str,
    ordinal: usize,
    local: &runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) {
    if !wfin_enabled() {
        return;
    }
    let d = local.drive;
    let task_avg_us = if d.tasks > 0 { d.busy_ns / d.tasks / 1000 } else { 0 };
    let (rgsw, dictb, gscan, wins) = worker_cb_counters().unwrap_or((0, 0, 0, 0));
    eprintln!(
        "MORSEL|WFIN|qid={}|pipe=0|worker={}|t_us={}|tasks={}|task_avg_us={}|first_us={}|busy_us={}|morsels={}|granules={}|chan={}|cb_rgswitch={}|cb_dictbuild={}|cb_granules={}|cb_windows={}",
        rg.query_id(),
        ordinal,
        d.last_end_ns / 1000,
        d.tasks,
        task_avg_us,
        d.first_claim_ns / 1000,
        d.busy_ns / 1000,
        d.morsels,
        d.granules,
        chan,
        rgsw,
        dictb,
        gscan,
        wins,
    );
}

/// Leader-side completion mark (same clock domain as the workers' t_us):
/// leader wake latency = LFIN t_us − max worker t_us.
fn emit_lfin(
    rt: &Arc<runtime::Runtime>,
    chan: &str,
    rg: &runtime::RgHandle,
    total_granules: u64,
    nrgs: usize,
    payload: &Arc<RuntimeScanShared>,
) {
    if !wfin_enabled() {
        return;
    }
    eprintln!(
        "MORSEL|LFIN|qid={}|t_us={}|granules={}|rgs={}|started={}|refused={}|chan={}",
        rg.query_id(),
        rt.now_ns() / 1000,
        total_granules,
        nrgs,
        payload.started.load(Ordering::SeqCst),
        payload.refused.load(Ordering::SeqCst),
        chan,
    );
}

/// Count the source's hard-boundary regions (row groups on cbstore; 1 on
/// boundary-free sources like heap). LFIN reporting only — called under
/// wfin_enabled(); a binary search per region on cbstore geometry.
fn source_boundary_count(source: &dyn runtime::MorselSource) -> usize {
    let total = source.total_granules();
    let mut n = 0usize;
    let mut at = 0u64;
    while at < total {
        at = source.next_boundary_after(at).max(at + 1);
        n += 1;
    }
    n
}

fn build_worker_exec(payload: &Arc<RuntimeScanShared>) -> PgResult<()> {
    parallel::gtrace("w.exec.build.begin");
    let r = build_worker_exec_inner(payload);
    parallel::gtrace("w.exec.build.end");
    r
}

fn build_worker_exec_inner(payload: &Arc<RuntimeScanShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<DriveMode> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime scan worker ExecutorStart");
                x.with_mut(|d| -> PgResult<DriveMode> {
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
                    let mut mode = drive_mode(&aps.agg, ss);
                    match mode {
                        DriveMode::Fold => {
                            // The serial fold feed's arm/init half, once per
                            // worker; the drain half re-runs per morsel.
                            super::arm_scan_staging(
                                ss,
                                estate,
                                super::ScanFeedShape::FoldPrefix { agg: &aps.agg },
                            )?;
                            if ::nodeseqscan::seq_scan_batch_soa(ss).is_some() {
                                super::arm_fold_len_lanes(&aps.agg, ss);
                            } else if ::nodeseqscan::seq_scan_is_heap(ss)
                                && rowdrive_enabled()
                            {
                                // Unarmable fold prefix on heap: the leader
                                // admitted this shape through the PER-ROW
                                // drive (rowdrive car 2) — the arm verdict
                                // is type-driven, so this worker reaches
                                // the same one. Row-feed staging instead
                                // (kernel-qual bitmap when the qual has
                                // kernel shape).
                                super::arm_scan_staging(
                                    ss,
                                    estate,
                                    super::ScanFeedShape::RowFeed {
                                        ctx: "runtime scan perrow fold feed",
                                        stitch: true,
                                    },
                                )?;
                                mode = DriveMode::PerRowFold;
                            } else {
                                // The fold drain reads staged lane columns;
                                // the leader proved this exact arm on ITS
                                // scan (the decide probe / the engagement
                                // probe), so an unarmed cbstore prefix is a
                                // divergence, not a shape refusal.
                                return Err(Box::new(PgError::new(
                                    ERROR,
                                    "runtime scan worker fold prefix failed to arm",
                                )));
                            }
                        }
                        DriveMode::Census => {
                            // Census shape: row-feed staging (kernel-qual
                            // selection bitmap / PREWHERE when the qual has
                            // kernel shape; stitched tiers on).
                            super::arm_scan_staging(
                                ss,
                                estate,
                                super::ScanFeedShape::RowFeed {
                                    ctx: "runtime scan census feed",
                                    stitch: true,
                                },
                            )?;
                        }
                        DriveMode::StorelessCount => {
                            // Direct morsel drive: NO staging at all — the
                            // drain advances the count once per page batch
                            // of visible rows; nothing reads columns. The
                            // leader proved the bare-count shape on its own
                            // node (same plan), so a diverged worker shape
                            // is an error, never a wrong answer.
                            if !::nodeagg::agg_plain_count_star_shape(&aps.agg) {
                                return Err(Box::new(PgError::new(
                                    ERROR,
                                    "runtime scan worker count-star shape diverged from the leader's",
                                )));
                            }
                        }
                        // Derived above from a failed Fold prefix arm, never
                        // by drive_mode.
                        DriveMode::PerRowFold => unreachable!("derived mode"),
                    }
                    ::nodeagg::agg_plain_build_begin(&mut aps.agg, estate)?;
                    Ok(mode)
                })
            })
        })();
        match armed {
            Ok(mode) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, mode, errored: std::cell::Cell::new(false) });
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
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    // Standing channel (inc-3): a leader unwind (error/panic between
    // publish and standing_wait's own cleanup) reaches here through
    // DestroyParallelContext with claimed workers possibly still driving.
    // Complete the RG (drain releases drives parked on the aborted
    // generation) and hold the frame until every participant detached —
    // the leader arena must outlive their SendConst refs. UNCONDITIONAL
    // on the rg upgrade: a dead weak handle still leaves the board entry
    // occupied (every future try_engage would refuse and parked workers
    // would wedge against an entry nobody removes).
    let entry = payload
        .standing
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    if let Some(entry) = entry {
        if let Some(rg) = &rg {
            if rg.try_outcome().is_none() {
                // drain_rg aborts (idempotent) and drives protocol cleanup.
                drain_rg_raw(payload.rt, rg);
            }
        }
        parallel::standing::close_and_await(&entry);
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
        parallel::standing::register_standing_driver(runtime_scan_standing_driver);
    });
}

/// Fold when the plan reads lane columns (the serial Fold choice); the
/// census shape (no columns) takes the census drain with a qual, the
/// direct storeless drive without one. Deterministic from the classified
/// plan + the plan's scan qual, so leader and workers always agree.
fn drive_mode(
    agg: &::nodeagg::AggStateData<'_>,
    ss: &::nodeseqscan::SeqScanState<'_>,
) -> DriveMode {
    match ::nodeagg::agg_lanefold_plan(agg) {
        Some(plan) if plan.cols.is_empty() => {
            // Qual-less census = the direct storeless drive (admission
            // reaches it only through the rowdrive carve-out: bare heap
            // count(*)); a qual'd census counts its selection bitmap.
            if ss.ss.qual.is_none() {
                DriveMode::StorelessCount
            } else {
                DriveMode::Census
            }
        }
        _ => DriveMode::Fold,
    }
}

/// The census morsel drain: the fold drain's structure specialized to
/// count-only plans (no residuals, no guards, no lane reads, no str-mm
/// memos) with a graceful no-SoA/no-bitmap fallback. Byte-identity: the
/// same rows pass the same qual — the staged bitmap IS the kernel qual's
/// verdict (fallback rows re-check per row through `seq_scan_batch_emit`,
/// exactly the fold drain's discipline) — and a CountStar transition's
/// whole effect is one increment per surviving row, which `fold_batch`
/// applies as a popcount over the same selection.
fn census_drain<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| p.cols.is_empty() && p.resid.is_empty() && !p.guarded));
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's pin (fold drain parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let nwords = (n as usize).div_ceil(64);
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
        let fast = {
            let sel = ::nodeseqscan::seq_scan_batch_qual_sel(ss);
            let bitmap_qual = sel.is_some();
            match ::nodeseqscan::seq_scan_batch_soa(ss) {
                Some(soa) if bitmap_qual || ss.ss.qual.is_none() => {
                    let fb = soa.fallback_words();
                    for w in 0..nwords {
                        rows[w] = sel.map_or(!fb[w], |s| s[w] & !fb[w]);
                        fallback[w] = fb[w];
                    }
                    if n % 64 != 0 {
                        rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                        fallback[nwords - 1] &= (1u64 << (n % 64)) - 1;
                    }
                    true
                }
                _ => false,
            }
        };
        if fast {
            // Fallback rows: full per-row program off the stored tuple
            // (qual re-checked inside emit).
            for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
                while bits != 0 {
                    let i = (w as u32) * 64 + bits.trailing_zeros();
                    bits &= bits - 1;
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                    }
                }
            }
            if rows[..nwords].iter().any(|w| *w != 0) {
                let aggcx = ::nodeagg::agg_aggcontext(agg);
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("checked above: SoA staged");
                let plan =
                    ::nodeagg::agg_lanefold_plan(agg).expect("census drain requires a plan");
                // SAFETY: pergroup_base is the node's once-allocated
                // single-group pergroup array covering every transno;
                // CountStar kernels read no lane columns, so the col
                // contract is vacuous; the plan is unguarded (asserted).
                unsafe {
                    ::lanefold::fold_batch(
                        plan,
                        soa,
                        &rows[..nwords],
                        n as usize,
                        ::nodeagg::agg_plain_pergroup_base(agg),
                        aggcx,
                    )?;
                }
            }
        } else {
            // No staged bitmap/SoA (scalar qual, unstaged batch): the full
            // per-row path for every row.
            for i in 0..n {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                }
            }
        }
    }
    Ok(())
}

/// The DIRECT MORSEL DRIVE drain (rowdrive car 1): bare heap count(*) over
/// one block-range claim — the serial fused storeless inner loop UNCHANGED:
/// `seq_scan_next_pagebatch` (page batch of VISIBLE tuples;
/// `page_collect_tuples` under the task-bound leader snapshot, exactly the
/// serial drive's visibility work) + one checked count advance per batch
/// (`exec_agg_batched`'s storeless count-star arm, refused-advance per-row
/// fallback included). No staging, no fold plan reads, no per-row work.
/// Byte-identity: a count's transvalue composes by addition over any claim
/// partition (order-insensitive-exact); visibility is per tuple, identical
/// per page regardless of which worker visits it.
fn storeless_count_drain<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(::nodeagg::agg_plain_count_star_shape(agg));
    debug_assert!(ss.ss.qual.is_none());
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's pin (fold drain parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        ::nodeagg::agg_plain_count_star_accept_batch(agg, estate, n)?;
    }
    Ok(())
}

/// The PER-ROW direct drive drain (rowdrive car 2): heap fold shapes with
/// an unarmable fold prefix, over one block-range claim — the serial row
/// path unchanged: `seq_scan_next_pagebatch` (visible tuples) +
/// `seq_scan_batch_emit` (fetch + qual per row; the staged kernel-qual
/// selection bitmap short-circuits inside when the RowFeed arm staged one
/// — same rows, same order, same errors: the stitch discipline) +
/// `agg_plain_build_accept` (exec_agg's single-group loop body verbatim).
/// Byte-identity: identical per-row qual verdicts and transition programs
/// per page regardless of which worker visits it; partials are the
/// classified fold plan's order-insensitive-exact export.
fn perrow_fold_drain<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(agg_runtime_partial_admissible(agg));
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's pin (fold drain parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        for i in 0..n {
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The morsel source: cbstore granule geometry.
// ---------------------------------------------------------------------------

/// Granule-addressed morsel source over one cbstore part's geometry: claims
/// are whole-granule ranges that never cross a row-group edge — which is
/// exactly a dictionary-epoch edge (per-RG local dictionaries), so every
/// per-epoch memo (dict-eval, codehist, gmemo) stays worker-coherent and
/// every kernel invocation sees a single dictionary snapshot.
pub(super) struct CbstoreGranuleSource {
    /// Row-group start prefix sums (len nrgs+1; last = total).
    pub(super) starts: Vec<u64>,
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
    /// (Inert under whole_boundary_claims below; kept for the kill switch.)
    fn startup_c0(&self) -> u64 {
        2
    }

    /// Row group == dictionary epoch: a claim that stops short of the RG
    /// edge hands the rest of the RG to another worker, which rebuilds the
    /// RG's dictionary (LZ4 blob decompress) and refills every per-epoch
    /// lane memo (dict-eval predicate sweep) — measured on q21@10M as the
    /// entire runtime-vs-armed drive-phase gap (WFIN decomposition,
    /// notes/runtime-drive-scaling.md: dict_builds 153→243, busy +78% at
    /// DOP15; the armed arm claims whole RGs and scales 13x). Whole-RG
    /// claims are ~8 granules ≈ ~1.2ms on q21-class kernels — the same
    /// cancel/photo-finish granularity the armed arm already ships.
    /// PGRUST_RUNTIME_SPLIT_CLAIMS=1 restores sizer-truncated claims (A/B).
    fn whole_boundary_claims(&self) -> bool {
        static SPLIT: OnceLock<bool> = OnceLock::new();
        !*SPLIT.get_or_init(|| {
            std::env::var("PGRUST_RUNTIME_SPLIT_CLAIMS").map_or(false, |v| v.trim() == "1")
        })
    }
}

/// Block-range morsel source over a heap relation (M1 heap source, the
/// parallelism redesign's "heap morsels are block ranges"): granule = ONE
/// block, C's `table_block_parallelscan_nextpage` claim unit at its finest —
/// the runtime's duration-adaptive sizing builds the multi-block runs C
/// precomputes as chunks, and the last-worker photo-finish replaces C's
/// end-of-scan chunk ramp-down. Heap has no dictionary epochs, so there are
/// no interior hard boundaries (`next_boundary_after` = the trait default:
/// total). Visibility is per tuple inside the page batch
/// (`page_collect_tuples` under the task-bound leader snapshot — exactly a
/// C parallel seq scan worker's check), not a source property.
struct HeapBlockSource {
    /// rs_nblocks at the leader's scan start (the C parallel-scan contract:
    /// blocks appended after scan start are not visited; their tuples are
    /// invisible to the scan snapshot anyway).
    nblocks: u64,
}

impl runtime::MorselSource for HeapBlockSource {
    fn total_granules(&self) -> u64 {
        self.nblocks
    }

    /// A heap block stages ~50-250 tuples (vs 8,192/granule on cbstore).
    /// Seed the ramp at 16 blocks (128KB, a few thousand rows — tens of µs
    /// on fold shapes): same probe-morsel sizing intent as cbstore's C0=2.
    fn startup_c0(&self) -> u64 {
        16
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

pub(super) fn exprs_parallel_safe<'mcx>(nodes: impl Iterator<Item = Node<'mcx>>) -> PgResult<bool> {
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
pub(super) fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SCAN_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// Direct-morsel-drive arm kill (rowdrive car 1): PGRUST_RUNTIME_ROWDRIVE=0
/// disables the storeless-count carve-out; the fold/census arms are
/// untouched. Layered UNDER PGRUST_RUNTIME + the pool GUC like every arm
/// switch.
fn rowdrive_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_ROWDRIVE").map_or(true, |v| v.trim() != "0")
    })
}

/// Engagement floor for the direct storeless-count drive, in heap blocks.
/// The serial fused advance is O(pages) at memory bandwidth — a small heap
/// finishes before the gang is worth waking. Default 8,192 blocks (64MB);
/// the e2e tranche forces it down to exercise engagement.
fn rowdrive_min_blocks() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_ROWDRIVE_MIN_BLOCKS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(8192)
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
    let is_cb = ::nodeseqscan::seq_scan_is_cbstore(ss);
    if !seq_scan_fusible(ss, estate)?
        || !(is_cb || ::nodeseqscan::seq_scan_is_heap(ss))
    {
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(None);
    }
    // Census shapes (fold plan reads no columns) engage only with a qual:
    // cbstore's bare count(*) is the Meta/footer arm's, heap's is the fused
    // storeless batch advance's (O(pages) serially — a per-row parallel
    // drive would lose), and a qual-less census scan would stage nothing
    // for the per-row drive either way. CARVE-OUT (rowdrive car 1, the
    // direct morsel drive): bare heap count(*) — the ONE storeless-count
    // transition shape whose serial O(pages) inner loop runs unchanged per
    // block-range claim (DriveMode::StorelessCount) — engages above a block
    // floor. Fail-closed: anything short of the exact shape refuses here as
    // before (the serial fused drive owns it).
    if ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| p.cols.is_empty())
        && ss.ss.qual.is_none()
    {
        if is_cb
            || !rowdrive_enabled()
            || ss.ss.ps_ProjInfo.is_some()
            || !::nodeagg::agg_plain_count_star_shape(agg)
        {
            return Ok(None);
        }
        // The block floor rides the geometry probe below (the source's
        // granule = one heap block).
    }
    // Fold-mode shapes must have an armable fold prefix on this scan:
    // cbstore proved it at decide time (only the Fold choice reaches here
    // with a column-reading plan); heap arrives through the Refuse choice,
    // which conflates the census shape with fold-not-ready (projected scan
    // or unarmable prefix). The probe is decide's identical idempotent arm
    // — free for an already-armed cbstore scan, decisive for heap. The
    // worker-side arm re-verifies (divergence is an error, not a wrong
    // answer). CARVE-OUT (rowdrive car 2, the per-row direct drive): a
    // heap fold shape whose prefix is UNARMABLE (qual/transition column
    // the fixed-width prefix deform cannot host — the m1 LIKE-fold
    // boundary) takes DriveMode::PerRowFold: the serial row path
    // (emit + accept) N-wide per claim. Projected scans stay refused
    // (fail-closed); cbstore keeps its decide-time verdicts.
    let mut perrow = false;
    if drive_mode(agg, ss) == DriveMode::Fold {
        if ss.ss.ps_ProjInfo.is_some() {
            return Ok(None);
        }
        if !super::probe_arm_fold_prefix(agg, ss, estate)? {
            if is_cb || !rowdrive_enabled() {
                return Ok(None);
            }
            perrow = true;
        }
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

    // --- Geometry: enough granules to be worth a gang. The source is the
    // AM's morsel geometry: cbstore absolute granules with row-group hard
    // boundaries, heap block ranges with none. (The floor is granule-
    // denominated, so its meaning is per-AM: ~8,192 rows/granule on
    // cbstore, one ~8KB block on heap — both env-tunable through the same
    // knob because the e2e floors force it anyway.)
    let source: Arc<dyn runtime::MorselSource> = if is_cb {
        let Some((_, starts)) = ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
        else {
            return Ok(None);
        };
        Arc::new(CbstoreGranuleSource { starts })
    } else {
        let Some(nblocks) = ::nodeseqscan::seq_scan_heap_block_geometry(ss, estate)? else {
            return Ok(None);
        };
        Arc::new(HeapBlockSource { nblocks })
    };
    let total_granules = source.total_granules();
    if total_granules < min_granules().max(2 * dop as u64) {
        return Ok(None);
    }
    // Direct storeless-count drive: its own (higher) floor — the serial
    // fused advance is O(pages); parallel pays only on a big heap.
    let rowdrive = drive_mode(agg, ss) == DriveMode::StorelessCount;
    if rowdrive && total_granules < rowdrive_min_blocks() {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    if rowdrive {
        // Observability (e2e tranche; after the done-pull early-return so
        // one engagement traces once): a following "engaged" line at this
        // query is a DIRECT-DRIVE engagement.
        lane_trace(&format!("runtime-scan: rowdrive admit blocks={total_granules}"));
    } else if perrow {
        lane_trace(&format!(
            "runtime-scan: rowdrive admit perrow blocks={total_granules}"
        ));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, total_granules, source)
}

fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    source: Arc<dyn runtime::MorselSource>,
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
        drive_at_entry: entry_drive_enabled(),
        standing: Mutex::new(None),
    });

    // Submit-and-park ceremony. EnterParallelMode brackets the context
    // lifetime (CreateParallelContext asserts it); an error unwind aborts
    // the transaction, which destroys live contexts and resets the mode
    // (AtEOXact_Parallel — the Gather discipline).
    parallel::gtrace("l.engage.begin");
    xact::EnterParallelMode();
    let engaged = engage_ceremony(agg, estate, rt, dop, total_granules, source, &payload);
    xact::ExitParallelMode();
    parallel::gtrace("l.engage.end");
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
    source: Arc<dyn runtime::MorselSource>,
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
        // nrgs (LFIN reporting only): hard-boundary regions of the source —
        // row groups on cbstore, 1 on boundary-free sources (heap).
        let nrgs = if wfin_enabled() { source_boundary_count(source.as_ref()) } else { 0 };
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

        // M2 pool-binding: STANDING engagement first — no worker launch,
        // no entry task, one binder bind per participant. Fallback (gang
        // unavailable/kill-switched/all-refused/claim-deadline) leaves the
        // RG untouched and takes the launched path below.
        match standing_wait(rt, payload, dop, total_granules, &rg, &waiter)? {
            StandingWait::Done(outcome) => {
                emit_lfin(rt, "standing", &rg, total_granules, nrgs, payload);
                return finish_outcome(payload, outcome);
            }
            StandingWait::Fallback => {}
        }

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
            // LIVENESS: every launched helper's task has ENDED (normal hook
            // exit keeps BGWH_STARTED until after the drive, so this cannot
            // trip mid-drive) yet the RG is incomplete — helpers died
            // without a channel message (post-Terminate death, e.g. an
            // init-path panic-to-ERROR). Nothing claimed => clean fallback;
            // claimed => reap if possible and surface a real error.
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-scan: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, payload, &rg);
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime scan helpers exited before completing the scan",
                )));
            }
            // A raised cancel disposition (statement_timeout /
            // pg_cancel_backend) surfaces from the latch quantum (F1 defect
            // layer 2b): abort + drain the RG, then propagate — exactly the
            // CFI branch above.
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg(rt, payload, &rg);
                return Err(e);
            }
        };
        emit_lfin(rt, "launched", &rg, total_granules, nrgs, payload);

        finish_outcome(payload, outcome)
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
    parallel::gtrace("l.destroy.begin");
    let destroy = parallel::DestroyParallelContext(pcxt);
    parallel::gtrace("l.destroy.end");
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

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN (no extra context — the serial arm's surface, the
/// parity oracle); an unexplained abort surfaces the pending interrupt or
/// reports; a completed-but-nobody-participated RG falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimeScanShared>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
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
}

// ---------------------------------------------------------------------------
// M2 pool-binding: the standing engagement channel (parallel::standing).
// ---------------------------------------------------------------------------

enum StandingWait {
    /// The RG reached an outcome under standing participation.
    Done(runtime::RgOutcome),
    /// Standing path unavailable or refused with the RG UNTOUCHED —
    /// take the launched path.
    Fallback,
}

/// First-claim deadline: parked standing workers wake in microseconds, so
/// an unclaimed engagement after this long means the gang is dead/busy —
/// fall back to the launched path (correctness never depends on this).
fn standing_claim_deadline() -> std::time::Duration {
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(*MS.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_GANG_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

/// The standing channel's submit-and-park: publish the engagement, then
/// poll completion + interrupts + participation counters. Every exit path
/// closes the board entry and waits for claimed participants to detach
/// (the arena-lifetime join — detach is Drop-guaranteed on the workers).
fn standing_wait(
    rt: &'static Arc<runtime::Runtime>,
    payload: &Arc<RuntimeScanShared>,
    dop: i32,
    total_granules: u64,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
) -> PgResult<StandingWait> {
    let shared = payload.pcxt_shared.get().expect("pcxt shared set before standing_wait");
    let Some(entry) = parallel::standing::try_engage(shared, dop.max(0) as usize) else {
        return Ok(StandingWait::Fallback);
    };
    // Leader-unwind containment: PRIVATE_SHUTDOWN completes the standing
    // join if this frame never reaches one of its own cleanup paths (each
    // of which takes the slot back first).
    *payload.standing.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&entry));
    let take_slot = || {
        payload
            .standing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
    };
    let t0 = std::time::Instant::now();
    let mut traced = false;
    loop {
        if let Some(o) = waiter.try_wait() {
            take_slot();
            parallel::standing::close_and_await(&entry);
            if !traced {
                lane_trace(&format!(
                    "runtime-scan: engaged standing dop={} granules={total_granules}",
                    entry.claimed()
                ));
            }
            return Ok(StandingWait::Done(o));
        }
        if let Err(e) = ::postgres_seams::check_for_interrupts::call() {
            // Order matters: abort THEN drain (the leader's protocol
            // cleanup is what completes the RG and releases workers parked
            // in their drives) THEN await detach.
            rg.abort();
            drain_rg(rt, payload, rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(e);
        }
        let claimed = entry.claimed();
        if !traced && claimed > 0 {
            lane_trace(&format!(
                "runtime-scan: engaged standing dop={claimed} granules={total_granules}"
            ));
            traced = true;
        }
        let started = payload.started.load(Ordering::SeqCst);
        let refused = entry.refused() + payload.refused.load(Ordering::SeqCst);
        // Nobody will participate: every ticket-holder refused pre-bind or
        // at the bind/lane stage, before any granule was claimed.
        if started == 0 && refused >= entry.tickets() {
            lane_trace(&format!(
                "runtime-scan: standing refused ({refused} refusals) — launched fallback"
            ));
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // Nothing driving and nothing pending within the deadline: gang
        // dead/busy (claimed==0) OR a smaller-than-tickets gang whose every
        // claimant exited pre-drive without reaching the refusal counters'
        // tickets floor above (started==0, detached>=claimed>0). Either
        // way no granule was consumed; the launched path takes over. A
        // straggler that claims right as we close simply drives the same
        // RG (morsel claims are atomic; its partial combines like any
        // participant's) — close_and_await bounds on its drive.
        if started == 0
            && entry.detached() >= claimed
            && t0.elapsed() > standing_claim_deadline()
        {
            lane_trace("runtime-scan: standing claim deadline — launched fallback");
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // Participants all detached yet the RG is incomplete and no error
        // was recorded: a worker died outside every catch layer (detach is
        // Drop-guaranteed, so this is reachable only through that needle).
        if claimed > 0 && started > 0 && entry.detached() >= claimed {
            if let Some(o) = waiter.try_wait() {
                take_slot();
                parallel::standing::close_and_await(&entry);
                return Ok(StandingWait::Done(o));
            }
            if let Some(e) = payload.take_error() {
                rg.abort();
                drain_rg(rt, payload, rg);
                take_slot();
                parallel::standing::close_and_await(&entry);
                return Err(e);
            }
            rg.abort();
            drain_rg(rt, payload, rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime scan standing executors exited before completing the scan",
            )));
        }
        parallel::wait_parallel_finish_quantum();
    }
}

/// The standing driver (parallel::standing::register_standing_driver):
/// runs ON a standing executor, already impersonated (worker number +
/// lock group). Identical body to the POST_TASK_PARK hook — one binder
/// bind around lane lease + executor build + pinned drive, errors
/// payload-side, leader latch wake on exit.
fn runtime_scan_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeScanShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if let Err(unwind) = r {
        payload.fail(PgError::new(ERROR, "runtime scan standing executor panicked").into());
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        // Exit-committed unwinds (FATAL) must keep unwinding: the worker
        // is terminating; parallel::standing rethrows them to the glue,
        // whose proc_exit drain releases identity. Swallowing one here
        // would resurrect a dead backend into the standing pool.
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// Reap a pinned RG no helper will drive (abort/fallback paths): the leader
/// drives the protocol itself — the closed generation refuses every join,
/// so no morsel work runs; the drive just executes invalidate/finalize/
/// completion. This is cleanup driving, not leader work execution (§2.5).
/// Abort + BOUNDED drain of a pinned RG. True = the RG completed (the
/// normal case: a closed generation refuses every join, so the drive is
/// pure protocol cleanup and settles within a morsel). False = it could not
/// be completed — a participant died holding an unsettled pin (worker-death
/// containment): the RG and its slot are deliberately LEAKED (bounded by
/// the 128-slot array; a process restart resets everything) and the caller
/// must surface an error rather than wait forever.
fn drain_rg(
    rt: &'static Arc<runtime::Runtime>,
    payload: &Arc<RuntimeScanShared>,
    rg: &runtime::RgHandle,
) -> bool {
    let _ = payload;
    drain_rg_raw(rt, rg)
}

fn drain_rg_raw(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
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
        lane_trace("runtime-scan: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-scan: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
