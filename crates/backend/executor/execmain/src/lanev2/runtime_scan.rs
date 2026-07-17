//! M1 RUNTIME SCAN PIPELINES — the first real pipeline on the morsel
//! runtime (docs/design/parallelism-redesign-2026-07.md §2.1/§2.2/§5-M1).
//!
//! Shape: a SERIAL-plan plain Agg over a pgrcolumnar OR heap SeqScan
//! (COUNT/plain-agg fold shapes with optional PREWHERE/kernel quals — the
//! lane's simplest fold pipelines), executed as ONE runtime TaskSet at DOP
//! N. The morsel source is [`runtime::GranuleMapSource`] over the geometry
//! the storage seam publishes ([`super::batch_source::SeqScanSource`]):
//! pgrcolumnar granules with row-group hard boundaries, or heap block
//! ranges with none (C's table_block_parallelscan_nextpage chunked claim,
//! runtime-adaptive).
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
//!    PINNED resource group (one task set: the GranuleMapSource + the fold
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
//! mode / a parallel worker; a pgrcolumnar or plain heap scan admitted by the
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

use super::batch_source::{
    heapfeed_v2_enabled, require_bridge, BatchGranuleSource, HeapBatchSource, SeqScanSource,
};
use super::router::{self, ArmClass, ArmCounter};
use super::runtime_instr::{self, EaRowTally, InstrumentPartial};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, seq_scan_fusible_runtime_ea};

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
    /// LAUNCHED helpers that have EXITED their drive frame (every exit path
    /// — refused bind, errored, drove, panic-unwind — bumps exactly once,
    /// by drop guard; the m35-spill inc-2c `ExitBump` pattern, ported here
    /// per the inc-2c FLAG). Liveness reap input: a pinned RG is invisible
    /// to pool workers, so once `exited >= launched` with the RG incomplete,
    /// nobody will ever step it — the leader must reap or park forever.
    /// LAUNCHED-path only by construction: standing-gang exits are counted
    /// by the standing board's own claimed/detached accounting (standing
    /// runs and fully closes BEFORE any launch, so the counter is exact
    /// against `launched`).
    exited: AtomicUsize,
    /// First worker-phase error (fold/executor/binder-cleanup errors; the
    /// entry-phase errors ride the ordinary parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
    /// Per-ordinal cumulative partials, overwritten after every morsel.
    partials: Vec<Mutex<Option<RuntimePartial>>>,
    /// The scan's granule geometry (row-group start prefix sums = the
    /// morsel source's hard boundaries), shared with the scheduler's
    /// [`runtime::GranuleMapSource`]: a COALESCED claim (sched.rs dop1-tax
    /// fix 1 — several epochs per claim at low live width) is segmented at
    /// these edges inside `morsel_body` ([`runtime::GranuleMap::segments`]),
    /// so `set_granule_range` still sees single-RG ranges and every kernel
    /// invocation sees one dictionary snapshot. Set once at engage, before
    /// any claim; UNSET on the heap and bitmap paths (no interior
    /// boundaries — nothing to segment) exactly as `rg_starts` was.
    map: OnceLock<Arc<runtime::GranuleMap>>,
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
    /// EA-on-morsels instrument partials (ea-morsels.md §2): Some ONLY when
    /// the engagement was admitted under EXPLAIN ANALYZE — None on every
    /// other path (dead-when-off). Same per-ordinal overwrite discipline as
    /// `partials`; read by the leader only on a clean Completed outcome.
    instr: Option<Vec<Mutex<Option<InstrumentPartial>>>>,
    /// TIMER mode (inc-3): one clock pair per claim against `ea_epoch`
    /// (the shared engagement origin — cross-worker comparable). false in
    /// ROWS mode and on every non-EA path: zero clock reads.
    ea_timer: bool,
    ea_epoch: std::time::Instant,
    /// bitmap-morsels: the frozen shared bitmap + mode mapping — Some ONLY
    /// on the bitmap arm's engagements (runtime_bitmap.rs); None on every
    /// scan-arm path (dead-when-off). Set at construction, read per claim.
    bitmap: Option<Arc<super::runtime_bitmap::BitmapMorselCtx>>,
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
    /// bitmap-morsels: the runtime bitmap arm's per-row drive — the node's
    /// UNCHANGED serial fetch+recheck+qual+projection path over a claimed
    /// window of the frozen shared bitmap (runtime_bitmap::drain_claim).
    BitmapPerRow,
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    mode: DriveMode,
    /// THIS helper contributed an error (its executor may be mid-batch —
    /// take the release/abort teardown, not finish/end).
    errored: std::cell::Cell<bool>,
    /// EA-on-morsels: this worker's cumulative instrument partial (only
    /// written when the engagement carries `instr` slots).
    instr: std::cell::RefCell<InstrumentPartial>,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
    /// CEREMONY-V2 lazy bind: the deferred binding for the drive currently
    /// running on this thread. Installed by helper_drive_lazy before
    /// drive_pinned, consumed by morsel_body's first touch, taken back and
    /// finished after the drive on every structured path.
    static LAZY_CTX: std::cell::RefCell<Option<LazyCtx>> =
        const { std::cell::RefCell::new(None) };
}

struct LazyCtx {
    binding: parallel::DeferredQueryTaskBinding,
    /// A FATAL (exit-committed unwind) caught inside the first-touch bind:
    /// run_morsel is infallible by contract (an escaping unwind would
    /// strand the participant's pin), so the exit is stashed here and
    /// resumed by helper_drive_lazy AFTER the drive settles and the
    /// binding is finished.
    exit_unwind: Option<Box<dyn std::any::Any + Send>>,
    /// This worker's own bind/build failed (recorded + RG aborted): later
    /// morsels short-circuit and the post-drive unbind takes the abort
    /// path, exactly the eager wrap's error discipline.
    bind_failed: bool,
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
    /// CEREMONY-V2 first touch: on this worker's FIRST claimed morsel of a
    /// lazy drive, perform the session bind (sticky resume or full bind —
    /// preceded by the standing channel's deferred visibility) and build
    /// the executor. Errors are ordinary morsel errors (recorded + RG
    /// abort — a claimed range cannot be handed back); exit-committed
    /// unwinds are stashed for the post-drive rethrow (run_morsel must not
    /// unwind).
    fn ensure_bound_first_touch(&self) -> PgResult<()> {
        if WORKER_EXEC.with(|cell| cell.borrow().is_some()) {
            return Ok(());
        }
        LAZY_CTX.with(|slot| {
            let mut slot = slot.borrow_mut();
            let Some(ctx) = slot.as_mut() else {
                // Not a lazy drive (entry-task path builds up front): the
                // legacy missing-executor error below reports it.
                return Ok(());
            };
            if ctx.bind_failed {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime scan worker bind failed (recorded upstream)",
                )));
            }
            parallel::gtrace("w.firsttouch.begin");
            let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
                ctx.binding.bind_now()?;
                self.started.fetch_add(1, Ordering::SeqCst);
                build_worker_exec(self)
            }));
            parallel::gtrace("w.firsttouch.end");
            match r {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    ctx.bind_failed = true;
                    Err(e)
                }
                Err(unwind) => {
                    ctx.bind_failed = true;
                    if parallel::standing::is_exit_unwind(&*unwind) {
                        ctx.exit_unwind = Some(unwind);
                    }
                    Err(Box::new(PgError::new(
                        ERROR,
                        "runtime scan worker panicked in the first-touch bind",
                    )))
                }
            }
        })
    }

    fn morsel_body(&self, worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        self.ensure_bound_first_touch()?;
        // TIMER mode: the claim's clock pair (§5 — the ONLY TIMING ON cost).
        let ea_t0 = (self.ea_timer && self.instr.is_some())
            .then(|| self.ea_epoch.elapsed().as_nanos() as u64);
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
                    // bitmap-morsels arm: claimed-window drive over the
                    // frozen shared bitmap (runtime_bitmap::drain_claim —
                    // the node's unchanged serial per-row path), then the
                    // same cumulative partial export as the scan arm below
                    // (EA never admits this arm: no instr block).
                    if let crate::procnode::PlanStateNode::BitmapHeapScan(b) = &mut aps.outer {
                        let Some(ctx) = self.bitmap.as_ref() else {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime bitmap morsel without a bitmap context",
                            )));
                        };
                        super::runtime_bitmap::drain_claim(
                            ctx,
                            &mut aps.agg,
                            b,
                            estate,
                            range.start..range.end,
                        )?;
                        let slot = worker - self.pins_base;
                        let mut g =
                            self.partials[slot].lock().unwrap_or_else(|p| p.into_inner());
                        return agg_runtime_export_partial_into(
                            &aps.agg,
                            g.get_or_insert_with(Default::default),
                        );
                    }
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime scan worker outer node is not a SeqScan",
                        )));
                    };
                    // A COALESCED claim spans several row groups (sched.rs
                    // dop1-tax fix 1): pgrcolumnar claims are segmented at
                    // the RG edges (`GranuleMap::segments` over the shared
                    // engagement geometry) so every positioned range sees
                    // a single dictionary epoch (position's pgrcolumnar
                    // single-RG contract) and every kernel batch one
                    // dictionary snapshot. Heap sources have no interior
                    // boundaries and never coalesce (no map on the
                    // payload): the loop degenerates to one positioned
                    // range (`Segments::whole`). Cancel observability
                    // stays at epoch grain: an abort/failure observed
                    // between segments stops the claim (aborted
                    // generations need not execute every granule — the RG
                    // outcome is discarded).
                    let ea = self.instr.is_some();
                    let mut tally = EaRowTally::default();
                    let map = self.map.get().map(|m| &**m);
                    let interrupted = || {
                        self.failed.load(Ordering::SeqCst)
                            || self
                                .rg
                                .get()
                                .and_then(|w| w.upgrade())
                                .is_some_and(|rg| rg.is_aborted())
                    };
                    // Phase-1 source selection (WS-K): heap claims ride the
                    // dedicated HeapBatchSource iff PGRUST_LANE_V2_HEAPFEED
                    // is on (advisory readahead depth inside position());
                    // the knob-OFF world — and every pgrcolumnar claim —
                    // constructs SeqScanSource exactly as before. Knob-ON,
                    // end-of-claim ownership moves to the source: ONE
                    // end_claim per claim after the segment loop (the
                    // drains skip their inline clear under the same
                    // process-static knob — single owner, trait doc).
                    // WS-O inc-2 claim-settle guard (both arms): end_claim
                    // runs on the ERROR path too — a failed claim must not
                    // carry its page pin into the abort drain (the R3
                    // zero-pins-at-settle law; the drive error wins the
                    // report, the settle error is surfaced only when the
                    // drive itself succeeded).
                    if heapfeed_v2_enabled() && ::nodeseqscan::seq_scan_is_heap(ss) {
                        let mut src = HeapBatchSource::new(&mut *ss);
                        let drove = drive_claim_segments(
                            &mut src,
                            &mut aps.agg,
                            estate,
                            mode,
                            ea,
                            &mut tally,
                            map,
                            range.start..range.end,
                            interrupted,
                        );
                        let settled = src.end_claim(estate);
                        drove?;
                        settled?;
                    } else {
                        let mut src = SeqScanSource::new(&mut *ss);
                        let drove = drive_claim_segments(
                            &mut src,
                            &mut aps.agg,
                            estate,
                            mode,
                            ea,
                            &mut tally,
                            map,
                            range.start..range.end,
                            interrupted,
                        );
                        let settled =
                            if heapfeed_v2_enabled() { src.end_claim(estate) } else { Ok(()) };
                        drove?;
                        settled?;
                    }
                    // EA-on-morsels: fold this claim into the worker's
                    // cumulative instrument partial and export by OVERWRITE
                    // (decision-6, same discipline as the result partial
                    // below — the final export precedes the settle). EXACT
                    // per the dop1-tax contract: accumulate in the local,
                    // export at claim end, never sampled.
                    if let Some(instr) = &self.instr {
                        let mut ip = ex.instr.borrow_mut();
                        ip.claims += 1;
                        ip.granules += range.end - range.start;
                        ip.rows.add(&tally);
                        // Scan-desc counters are per-worker cumulative: the
                        // snapshot IS the running total (prune fold, §1).
                        if let Some(c) = ::nodeseqscan::seq_scan_cb_ea_counters(ss) {
                            ip.prune = c;
                        }
                        if let Some(t0) = ea_t0 {
                            let t1 = self.ea_epoch.elapsed().as_nanos() as u64;
                            runtime_instr::ea_claim_time(&mut ip, t0, t1);
                        }
                        let slot = worker - self.pins_base;
                        *instr[slot].lock().unwrap_or_else(|p| p.into_inner()) = Some(*ip);
                    }
                    // Cumulative partial export (in place), ONCE per claim:
                    // the worker's LAST claim's export — which precedes its
                    // settle, and therefore RG completion — is the one the
                    // leader reads. The slot's partial is reused across
                    // morsels (retained capacity; a fresh Vec per morsel was
                    // a malloc+free pair on the engaged path —
                    // m2-integration audit).
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
    // Every launched helper bumps `exited` exactly once, on EVERY exit path
    // — including the exit-committed resume_unwind below (the leader's
    // liveness reap counts these against `launched`; m35-spill inc-2c port).
    // Launched-frame placement (here and in the POST_TASK_PARK hook, never
    // inside the shared helper_drive): the standing driver must NOT bump —
    // standing exits are accounted by the board's claimed/detached counters,
    // and stale standing bumps would poison the launched loop's threshold.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
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
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("scan");
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
    // WS-O inc-2 pin-board assert (debug-only accessor, contract-approved):
    // a drive that returned settled its pin — anything else is a stranded
    // finalization obligation.
    debug_assert!(payload.rt.debug_pin_settled(&local), "pin unsettled after entry drive");
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
    // Launched-helper exit counter (see runtime_scan_worker_main): this hook
    // is the drive frame when entry-drive is kill-switched off.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload, false)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime scan helper panicked").into());
    }
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeScanShared>, sticky: bool) {
    let _ = shared;
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving (hook + standing frames; the
    // caller's catch layer records it, nobody drives, the leader-side
    // liveness machinery must recover promptly).
    super::test_helper_panic("scan");
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
    if parallel::lazy_bind_enabled() {
        return helper_drive_lazy(payload, target, &rg, sticky);
    }
    helper_drive_eager(payload, target, &rg)
}

/// CEREMONY-V2 lazy drive (notes/runtime-ceremony2.md): enter the pinned
/// drive UNBOUND; the session bind (sticky resume or full bind) + executor
/// build happen at this worker's FIRST morsel claim (morsel_body's
/// first-touch, the sink layer's fork-on-first-touch precedent). A
/// participant that never claims work pays neither. validate() runs
/// pre-drive so refusals keep today's fail-closed non-participation
/// surface; a bind ERROR after a claim is a real query error (the claimed
/// range cannot be handed back).
fn helper_drive_lazy(
    payload: &Arc<RuntimeScanShared>,
    target: &Arc<parallel::ParallelShared>,
    rg: &runtime::RgHandle,
    sticky: bool,
) {
    let binding = match parallel::DeferredQueryTaskBinding::new(target, sticky) {
        Ok(b) => b,
        Err(e) => {
            // Sticky eviction failure: fail-closed non-participation
            // (pre-claim; the RG is untouched by this worker).
            lane_trace(&format!(
                "runtime-scan: helper refused (sticky eviction failed: {})",
                e.message()
            ));
            payload.refused.fetch_add(1, Ordering::SeqCst);
            return;
        }
    };
    if let Err(e) = binding.validate() {
        // Binder validate() refusal: fail-closed non-participation. The
        // leader detects the nobody-participates case and falls back to
        // the serial arm.
        lane_trace(&format!(
            "runtime-scan: helper bind refused: {}",
            e.message()
        ));
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    }
    // Process-wide lane lease: pin-board lanes are shared across every
    // concurrently-engaged query; exhaustion = fail-closed non-participation.
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-scan: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    LAZY_CTX.with(|slot| {
        // A stale ctx can only remain if a previous drive escaped its
        // structured cleanup (a drive_pinned panic — already a protocol
        // invariant break). Drop it: its guard (if any) was reclaimed by
        // DeferredQueryTaskBinding::new's stale-guard containment above.
        let _stale = slot.borrow_mut().replace(LazyCtx {
            binding,
            exit_unwind: None,
            bind_failed: false,
        });
    });
    let _outcome = payload.rt.drive_pinned(&mut local, rg);
    // WS-O inc-2 pin-board assert (as the entry drive's).
    debug_assert!(payload.rt.debug_pin_settled(&local), "pin unsettled after lazy drive");
    parallel::gtrace("w.qtb.body.end");
    emit_wfin("bound", lane.ordinal(), &local, rg);
    let ctx = LAZY_CTX
        .with(|slot| slot.borrow_mut().take())
        .expect("lazy bind ctx present after the drive");
    if ctx.binding.resumed_sticky() {
        lane_trace("runtime-scan: sticky resume");
    }
    if ctx.binding.is_bound() {
        // Teardown mode per drive_bound: self-error takes the release path
        // and the binder's transaction-ABORT unbind cleans up.
        let self_errored = ctx.bind_failed
            || WORKER_EXEC
                .with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
        let teardown = catch_unwind(AssertUnwindSafe(|| teardown_worker_exec(!self_errored)));
        let commit = !self_errored && matches!(teardown, Ok(Ok(())));
        // The binding ALWAYS completes here (sticky park on the clean path,
        // full unbind otherwise) before any error/panic propagates.
        let finish = ctx.binding.finish(commit);
        match teardown {
            Ok(Ok(())) => {}
            Ok(Err(e)) => payload.fail(e),
            Err(unwind) => {
                // The driver's catch layer records the panic; exit-committed
                // unwinds keep dying (standing rethrows them to the glue).
                std::panic::resume_unwind(unwind);
            }
        }
        if let Err(e) = finish {
            payload.fail(e);
        }
        // Post-drive, the RG always has an outcome (drive_pinned returns on
        // one): no closed-generation cleanup drive is needed here.
    }
    if let Some(unwind) = ctx.exit_unwind {
        // A FATAL landed inside the first-touch bind: the worker must die.
        // Cleanup is done (nothing was bound); rethrow to the exit glue.
        std::panic::resume_unwind(unwind);
    }
}

/// The M1/M2 eager drive (PGRUST_RUNTIME_LAZYBIND=0): one binder wrap
/// around lane lease + executor build + pinned drive — byte-for-byte the
/// pre-ceremony-v2 path.
fn helper_drive_eager(
    payload: &Arc<RuntimeScanShared>,
    target: &Arc<parallel::ParallelShared>,
    rg: &runtime::RgHandle,
) {
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
        drive_bound(payload, lane.ordinal(), &mut local, rg)
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
                    let _ = payload.rt.drive_pinned(&mut local, rg);
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
    // WS-O inc-2 pin-board assert (as the entry drive's).
    debug_assert!(payload.rt.debug_pin_settled(local), "pin unsettled after bound drive");
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

/// This worker's pgrcolumnar drive counters (rg_switches, dict_builds,
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
    // submit_us/first_us (M5-4, m5-planner §3.5 submit→service channels):
    // time-to-service = first_us − submit_us; leader completion latency =
    // t_us − submit_us. Same scheduler clock domain as every other field.
    let (submit_ns, first_ns, _done_ns) = rg.service_times();
    eprintln!(
        "MORSEL|LFIN|qid={}|t_us={}|granules={}|rgs={}|started={}|refused={}|submit_us={}|first_us={}|chan={}",
        rg.query_id(),
        rt.now_ns() / 1000,
        total_granules,
        nrgs,
        payload.started.load(Ordering::SeqCst),
        payload.refused.load(Ordering::SeqCst),
        submit_ns / 1000,
        first_ns / 1000,
        chan,
    );
}

fn build_worker_exec(payload: &RuntimeScanShared) -> PgResult<()> {
    parallel::gtrace("w.exec.build.begin");
    let r = build_worker_exec_inner(payload);
    parallel::gtrace("w.exec.build.end");
    r
}

fn build_worker_exec_inner(payload: &RuntimeScanShared) -> PgResult<()> {
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
                    // bitmap-morsels arm: no staging, no fold plan — the
                    // per-claim drive is the node's serial row path; the
                    // leader proved admission on the identical plan.
                    if matches!(&aps.outer, crate::procnode::PlanStateNode::BitmapHeapScan(_)) {
                        if payload.bitmap.is_none() {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime bitmap worker without a bitmap context",
                            )));
                        }
                        super::runtime_bitmap::worker_shape_check(&aps.agg)?;
                        ::nodeagg::agg_plain_build_begin(&mut aps.agg, estate)?;
                        return Ok(DriveMode::BitmapPerRow);
                    }
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
                                // probe), so an unarmed pgrcolumnar prefix is a
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
                            // kernel shape; stitched tiers on) — the same
                            // staging the SERIAL q21-class drive uses (the
                            // lane refuses census and the Volcano pull runs
                            // over the prewhere-staged scan). A qual-only
                            // restage was tried here (dop1-tax inc-2 first
                            // cut) and REVERTED: LIKE quals are not
                            // cmp-const clauses, so it armed NO bitmap and
                            // the drain fell to the per-row path.
                            super::arm_scan_staging(
                                ss,
                                estate,
                                super::ScanFeedShape::RowFeed {
                                    ctx: "runtime scan census feed",
                                    stitch: true,
                                },
                            )?;
                            // BITS-ONLY declaration (dop1-tax2 inc-2): the
                            // census drive consumes selection bits and
                            // fallback emits only — never the staged SoA
                            // cells — so the PREWHERE lane skips its
                            // post-eval materialization (survivor-window
                            // completing deform + per-window dict-lane
                            // gather). This was the measured per-granule
                            // DOP-1 scan tax: SFIN/WFIN counters IDENTICAL
                            // (rgswitch/dictbuild/granules/windows), the
                            // delta pure per-window gather+deform work the
                            // serial Volcano census never does (dist-prof:
                            // gather_dict_lane 2.48% + decompress +4ms,
                            // runtime1 only). pgrcolumnar only (the measured
                            // shape; heap census keeps its kernel-bitmap
                            // staging verbatim). Requal quals refuse inside.
                            // PGRUST_RUNTIME_CENSUS_BITSONLY=0 restores the
                            // materializing arm (A/B).
                            if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
                                && census_bits_only_enabled()
                                && ::nodeseqscan::seq_scan_batch_bits_only(ss)
                            {
                                super::lane_trace("runtime census: bits-only staging");
                            }
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
                        // BitmapHeapScan outers early-return above, never
                        // reaching drive_mode.
                        DriveMode::BitmapPerRow => {
                            unreachable!("bitmap arm returns above")
                        }
                    }
                    ::nodeagg::agg_plain_build_begin(&mut aps.agg, estate)?;
                    Ok(mode)
                })
            })
        })();
        match armed {
            Ok(mode) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    mode,
                    errored: std::cell::Cell::new(false),
                    instr: Default::default(),
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

/// inc-2 bits-only census staging kill switch (default ON):
/// PGRUST_RUNTIME_CENSUS_BITSONLY=0 restores the materializing PREWHERE arm.
fn census_bits_only_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_CENSUS_BITSONLY").map_or(true, |v| v.trim() != "0")
    })
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

/// One claimed granule range through the storage seam, generic over the
/// batch source (Phase-1 WS-K): segment the claim (epoch-integral for
/// pgrcolumnar; heap has no interior boundaries — `Segments::whole`),
/// position, drain per DriveMode. Monomorphizes per source type: the
/// SeqScanSource instantiation is the pre-genericization machine code
/// (#[inline] delegation throughout — the WS-A code-shape-neutral law).
/// `interrupted` is the between-segments abort check (cancel observability
/// stays at epoch grain, exactly the pre-extraction loop).
#[allow(clippy::too_many_arguments)]
fn drive_claim_segments<'mcx, S, F>(
    src: &mut S,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mode: DriveMode,
    ea: bool,
    tally: &mut EaRowTally,
    map: Option<&runtime::GranuleMap>,
    range: runtime::MorselRange,
    interrupted: F,
) -> PgResult<()>
where
    S: BatchGranuleSource<'mcx>,
    F: Fn() -> bool,
{
    let mut segs = match map {
        Some(map) => map.segments(range.start..range.end),
        None => runtime::Segments::whole(range.start..range.end),
    };
    while let Some(seg) = segs.next() {
        src.position(estate, seg)?;
        match mode {
            DriveMode::Fold => {
                if ea {
                    super::agg_plain_fold_drain_ea(agg, src, estate, &mut *tally)?
                } else {
                    super::agg_plain_fold_drain(agg, src, estate)?
                }
            }
            DriveMode::Census => {
                census_drain(agg, src, estate, ea.then_some(&mut *tally))?
            }
            // rowdrive direct-drive modes (car 1/car 2): heap-only
            // admission (no payload map; EA refuses heap at admission), so
            // the segmentation loop degenerates to one positioned range
            // for these arms and no tally is ever needed.
            DriveMode::StorelessCount => storeless_count_drain(agg, src, estate)?,
            DriveMode::PerRowFold => perrow_fold_drain(agg, src, estate)?,
            // Dispatched to runtime_bitmap::drain_claim before this
            // helper's call site (BitmapHeapScan outer).
            DriveMode::BitmapPerRow => {
                unreachable!("bitmap arm returns above")
            }
        }
        if segs.more() && interrupted() {
            break;
        }
    }
    Ok(())
}

/// The census morsel drain: the fold drain's structure specialized to
/// count-only plans (no residuals, no guards, no lane reads, no str-mm
/// memos) with a graceful no-SoA/no-bitmap fallback. Byte-identity: the
/// same rows pass the same qual — the staged bitmap IS the kernel qual's
/// verdict (fallback rows re-check per row through the source's `emit`,
/// exactly the fold drain's discipline) — and a CountStar transition's
/// whole effect is one increment per surviving row, which `fold_batch`
/// applies as a popcount over the same selection.
fn census_drain<'mcx, S: BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
    mut tally: Option<&mut EaRowTally>,
) -> PgResult<()> {
    debug_assert!(::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| p.cols.is_empty() && p.resid.is_empty() && !p.guarded));
    // Scan-invariant qual presence (a plan-fixed field), hoisted once
    // through the bridge; the knob decides end-of-claim clear ownership
    // (process-static — trait-doc single-owner rules).
    let no_qual = require_bridge(src)?.ss.qual.is_none();
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
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        if let Some(t) = tally.as_deref_mut() {
            t.scanned += n as u64;
        }
        let nwords = (n as usize).div_ceil(64);
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
        let fast = {
            let sel = src.qual_sel();
            let bitmap_qual = sel.is_some();
            match src.batch_soa() {
                Some(soa) if bitmap_qual || no_qual => {
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
            if let Some(t) = tally.as_deref_mut() {
                // Window-grain: selected non-fallback rows all count.
                t.survived +=
                    rows[..nwords].iter().map(|w| w.count_ones() as u64).sum::<u64>();
            }
            // Fallback rows: full per-row program off the stored tuple
            // (qual re-checked inside emit).
            for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
                while bits != 0 {
                    let i = (w as u32) * 64 + bits.trailing_zeros();
                    bits &= bits - 1;
                    if let Some(slot) = src.emit(estate, i)? {
                        if let Some(t) = tally.as_deref_mut() {
                            t.survived += 1;
                        }
                        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                    }
                }
            }
            if rows[..nwords].iter().any(|w| *w != 0) {
                let aggcx = ::nodeagg::agg_aggcontext(agg);
                let soa = src.batch_soa().expect("checked above: SoA staged");
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
            // No whole-qual bitmap/SoA (scalar qual, unstaged batch, or a
            // requal tail): the full per-row path, word-skipping emit-dead
            // rows when a skip-sel bitmap is staged (requal-safe — cleared
            // bits are definitive rejections).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                src.skip_sel().map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if let Some(slot) = src.emit(estate, i)? {
                    if let Some(t) = tally.as_deref_mut() {
                        t.survived += 1;
                    }
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                }
                Ok(())
            })?;
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
fn storeless_count_drain<'mcx, S: BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(::nodeagg::agg_plain_count_star_shape(agg));
    debug_assert!(src.seq_scan_bridge().is_none_or(|ss| ss.ss.qual.is_none()));
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
fn perrow_fold_drain<'mcx, S: BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(agg_runtime_partial_admissible(agg));
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
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Emit-dead word skip over the staged kernel-qual bitmap (when the
        // RowFeed arm staged one): a cleared skip-sel bit is a row the emit
        // rejects with no observable effect — same rows, same order, same
        // errors; the per-filtered-row emit call collapses to one word test
        // per 64 rows. Words snapshotted (the emit re-borrows the source).
        let skip = {
            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
            src.skip_sel().map(|s| {
                w[..s.len()].copy_from_slice(s);
                w
            })
        };
        ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
            if let Some(slot) = src.emit(estate, i)? {
                ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The morsel source: pgrcolumnar granule geometry.
// ---------------------------------------------------------------------------

/// Granule-addressed morsel source over one pgrcolumnar part's geometry: claims
/// are whole-granule ranges that never cross a row-group edge — which is
/// exactly a dictionary-epoch edge (per-RG local dictionaries), so every
/// per-epoch memo (dict-eval, codehist, gmemo) stays worker-coherent and
/// every kernel invocation sees a single dictionary snapshot.
///
/// LEGACY of the m2 SINK arms only (distinct/plain-distinct/sort/hashjoin
/// construction sites; runtime_agg keeps its own private copy): the SCAN
/// arm now rides [`runtime::GranuleMapSource`] over the storage seam's
/// [`runtime::GranuleMap`]. Consolidating the sink sites onto
/// GranuleMapSource is WS-A inc-3 (post-integrate) and MUST carry each
/// arm's posture bit-for-bit — see notes/se-ws-a-batchsource.md.
pub(super) struct PgrcolumnarGranuleSource {
    /// Row-group start prefix sums (len nrgs+1; last = total).
    pub(super) starts: Arc<Vec<u64>>,
    /// True only when the consuming work body subdivides multi-epoch claims
    /// (none of the remaining users: the sink drains feed claims straight
    /// into `set_granule_range`). See `coalesce_claims`.
    pub(super) coalesce: bool,
}

impl runtime::MorselSource for PgrcolumnarGranuleSource {
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
        whole_claims()
    }

    /// dop1-tax fix 1: the SCAN arm's morsel_body subdivides a coalesced
    /// claim at these RG edges (one `set_granule_range` + drain per epoch
    /// segment), so multi-epoch claims are legal here — the per-claim drive
    /// re-entry (~30-45µs) amortizes across the claim's epochs at low live
    /// width. The DISTINCT sink shares this source TYPE but feeds claims
    /// straight into `set_granule_range`; it opts out via `coalesce` at
    /// construction. Factor/kill-switch: PGRUST_RUNTIME_COALESCE_EPOCHS
    /// (sched.rs; 1 disables).
    fn coalesce_claims(&self) -> bool {
        self.coalesce
    }
}

/// Whole-boundary claim posture of the pgrcolumnar arms — the
/// PGRUST_RUNTIME_SPLIT_CLAIMS kill switch (1 restores sizer-truncated
/// claims for A/B), read once per process: the OnceLock freezes the first
/// read, so construction-time (`GranuleMapSource`) and claim-time
/// (`PgrcolumnarGranuleSource`) reads are observationally identical.
pub(super) fn whole_claims() -> bool {
    static SPLIT: OnceLock<bool> = OnceLock::new();
    !*SPLIT.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SPLIT_CLAIMS").map_or(false, |v| v.trim() == "1")
    })
}

// The scan arm's heap block-range source (granule = ONE block, C's
// `table_block_parallelscan_nextpage` claim unit at its finest; no interior
// hard boundaries — heap has no dictionary epochs) is now the boundary-free
// `runtime::GranuleMap::unbounded` published by the storage seam
// (`batch_source::SeqScanSource::granule_map`) under a
// `runtime::GranuleMapSource` with sizer-truncated, non-coalescing posture
// — exactly the deleted HeapBlockSource's behavior. Visibility stays per
// tuple inside the page batch (`page_collect_tuples` under the task-bound
// leader snapshot), a drive property, not a source property.

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

/// DOP-elastic admission (tails192 #5, 48xl finding: tiny queries pay a
/// +5-8ms arming tax at 191). Admission was BINARY — engage at the full
/// pool or refuse to serial. Elastic: arm `ceil(total_granules /
/// granules-per-worker)` workers, bounded by the pool — a drive whose work
/// cannot feed the crowd never arms it. The refusal floors are UNCHANGED
/// and still computed against the POOL dop (a refusal means "not worth any
/// gang", independent of this sizing). gpw default 64 = the min_granules
/// engagement floor: a worker bringing less than one floor's worth of
/// granules cannot pay for its own arming. Scope-gated to pools > 32:
/// 16-core behavior (fleet, mt16 vectors, e2e dop censuses) is identity
/// by construction; only wide pools shrink.
/// Kill switch PGRUST_RUNTIME_ELASTIC_DOP=0; tune PGRUST_RUNTIME_ELASTIC_GPW.
pub(super) fn elastic_dop(dop: i32, total_granules: u64) -> i32 {
    // Wide-pool scope gate (mirrors sizing::DOPSCALE_W0): elastic sizing is
    // the DOP-191 arming-tax fix; at pool <= 32 admission behavior is
    // UNCHANGED -- the 16-core fleet, the mt16 vectors, and the e2e dop
    // censuses (launched == asked, runtime-sort leg 2) are identity by
    // construction. Caught by tranche-sort leg2-dop-census at 8b79874ce:
    // a 74-granule fixture at dop=4 engaged 2/2.
    if dop <= 32 {
        return dop;
    }
    static GPW: OnceLock<Option<u64>> = OnceLock::new();
    let gpw = *GPW.get_or_init(|| {
        if std::env::var("PGRUST_RUNTIME_ELASTIC_DOP").is_ok_and(|v| v.trim() == "0") {
            return None;
        }
        Some(
            std::env::var("PGRUST_RUNTIME_ELASTIC_GPW")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|&g| g > 0)
                .unwrap_or(64),
        )
    });
    let Some(gpw) = gpw else { return dop };
    let need = total_granules.div_ceil(gpw).max(1);
    need.min(dop as u64) as i32
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
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    let dop = router::arm_dop(ArmClass::Scan);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // Armed offer reached the admission walk (heap shapes ride the scan
    // arm's entry; they re-class at engagement). Done-repulls (the post-
    // completion pull that exits via agg_is_done below) are not offers —
    // without this gate `offered` double-counts every engagement.
    if !::nodeagg::agg_is_done(agg) {
        router::tick(ArmClass::Scan, ArmCounter::Offered);
    }

    // EA-on-morsels (ea-morsels.md §5/§6): from here the session is ARMED,
    // so under EXPLAIN ANALYZE every refusal records its reason for the
    // transparency line. `ea` admits collection; the MODE gate (rows-only
    // at inc-1) is enforced below with the other session gates.
    let ea = runtime_instr::ea_active(estate);
    let node_id = agg.plan.plan.plan_node_id;

    // --- Shape + session gates (fail-closed; every refusal is the serial arm).
    // Under EA the leader node carries an instr slot, which the serial-lane
    // fusibility memo rightly refuses — the runtime's workers run
    // uninstrumented, so EA admission walks the same gates with only the
    // instrument check vacated (E4).
    let fusible = if ea {
        seq_scan_fusible_runtime_ea(ss, estate)?
    } else {
        seq_scan_fusible(ss, estate)?
    };
    let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
    if !fusible || !(is_cb || ::nodeseqscan::seq_scan_is_heap(ss)) {
        return ea_refused(estate, ea, node_id, "scan-not-fusible");
    }
    // EA instrumentation is pgrcolumnar-only (the ratified lane's tally spans
    // the pgrcolumnar fold/census drives; the rowdrive heap arms carry no
    // tally): heap shapes under EA keep the pre-EA refusal — fail-closed
    // observability, never un-tallied numbers.
    if ea && !is_cb {
        return ea_refused(estate, true, node_id, "heap-not-instrumented");
    }
    if !agg_runtime_partial_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return ea_refused(estate, ea, node_id, "partials-not-order-insensitive-exact");
    }
    // Census shapes (fold plan reads no columns) engage only with a qual:
    // pgrcolumnar's bare count(*) is the Meta/footer arm's, heap's is the fused
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
            return ea_refused(estate, ea, node_id, "census-without-qual");
        }
        // The block floor rides the geometry probe below (the source's
        // granule = one heap block).
    }
    // Fold-mode shapes must have an armable fold prefix on this scan:
    // pgrcolumnar proved it at decide time (only the Fold choice reaches here
    // with a column-reading plan); heap arrives through the Refuse choice,
    // which conflates the census shape with fold-not-ready (projected scan
    // or unarmable prefix). The probe is decide's identical idempotent arm
    // — free for an already-armed pgrcolumnar scan, decisive for heap. The
    // worker-side arm re-verifies (divergence is an error, not a wrong
    // answer). CARVE-OUT (rowdrive car 2, the per-row direct drive): a
    // heap fold shape whose prefix is UNARMABLE (qual/transition column
    // the fixed-width prefix deform cannot host — the m1 LIKE-fold
    // boundary) takes DriveMode::PerRowFold: the serial row path
    // (emit + accept) N-wide per claim. Projected scans stay refused
    // (fail-closed); pgrcolumnar keeps its decide-time verdicts.
    let mut perrow = false;
    if drive_mode(agg, ss) == DriveMode::Fold {
        if ss.ss.ps_ProjInfo.is_some() {
            return ea_refused(estate, ea, node_id, "projected-scan");
        }
        if !super::probe_arm_fold_prefix(agg, ss, estate)? {
            if is_cb || !rowdrive_enabled() {
                return ea_refused(estate, ea, node_id, "fold-prefix-unarmable");
            }
            perrow = true;
        }
    }
    // EA vacates the pre-EA es_instrument blanket refusal (the whole
    // point); EPQ still refuses.
    if estate.es_epq_active {
        return Ok(None);
    }
    // Instrument MODE gate: INSTRUMENT_ROWS (TIMING OFF, inc-1) or
    // INSTRUMENT_TIMER (BUFFERS OFF, inc-3 — one clock pair per claim)
    // engage; BUFFERS/WAL combinations refuse until threaded.
    if ea && !runtime_instr::ea_mode_admissible(estate) {
        return ea_refused(estate, true, node_id, runtime_instr::ea_mode_refuse_reason(estate));
    }
    // Not from within parallel machinery: helpers of helpers don't exist,
    // and a leader already in parallel mode (Gather in flight) must not
    // stack a second context here.
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        return ea_refused(estate, ea, node_id, "in-parallel-mode");
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return ea_refused(estate, ea, node_id, "params");
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        return ea_refused(estate, ea, node_id, "params");
    }
    // The Agg must be the plan root (workers ExecutorStart the whole worker
    // pstmt; a deeper Agg would drag unrelated plan into every helper).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else {
        return ea_refused(estate, ea, node_id, "agg-not-plan-root");
    };
    if !std::ptr::eq(root_agg, agg.plan) {
        return ea_refused(estate, ea, node_id, "agg-not-plan-root");
    }
    // Scan expressions must be parallel-safe (they run on helpers).
    let Some(scan_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        return ea_refused(estate, ea, node_id, "outer-not-seqscan");
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        return ea_refused(estate, ea, node_id, "exprs-not-parallel-safe");
    }
    // MVCC snapshot (visibility folding parity with the serial drive).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return ea_refused(estate, ea, node_id, "non-mvcc-snapshot");
    }
    // Binder policy sources must be empty — a set flag means every helper
    // bind would refuse; don't launch at all.
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return ea_refused(estate, ea, node_id, "binder-policy");
    }

    // --- Geometry: enough granules to be worth a gang. The storage seam
    // (`SeqScanSource::granule_map`) publishes the AM's morsel geometry as
    // ONE GranuleMap: pgrcolumnar absolute granules with row-group hard
    // boundaries, heap block ranges with none — per-AM startup seed baked
    // in. (The floor is granule-denominated, so its meaning is per-AM:
    // ~8,192 rows/granule on pgrcolumnar, one ~8KB block on heap — both
    // env-tunable through the same knob because the e2e floors force it
    // anyway.)
    // nbounds() rides along for the WFIN/LFIN diagnostic channel only:
    // pgrcolumnar row-group (= dictionary epoch) count; heap has no interior
    // hard boundaries, so it honestly reports 0.
    // The map additionally rides to the engagement payload (pgrcolumnar
    // only): morsel_body segments COALESCED claims at the same RG edges the
    // source publishes (dop1-tax fix 1). Heap has no interior boundaries
    // and no dictionary epochs — nothing to segment, nothing to coalesce.
    let Some(map) = SeqScanSource::new(&mut *ss).granule_map(estate)? else {
        return Ok(None);
    };
    let map = Arc::new(map);
    let nrgs = map.nbounds();
    // Claim posture is EXPLICIT per arm (the GranuleMapSource contract,
    // never AM-inferred): the scan arm's pgrcolumnar posture is
    // whole-boundary (per the PGRUST_RUNTIME_SPLIT_CLAIMS kill switch) +
    // coalesce (morsel_body subdivides multi-epoch claims); heap claims
    // stay sizer-truncated with no coalescing — a boundary-free source
    // opting into whole-boundary claims would take the pipeline in one
    // claim.
    let source: Arc<dyn runtime::MorselSource> = Arc::new(runtime::GranuleMapSource::new(
        Arc::clone(&map),
        is_cb && whole_claims(),
        is_cb,
    ));
    let payload_map = is_cb.then(|| Arc::clone(&map));
    let total_granules = source.total_granules();
    if total_granules < min_granules().max(2 * dop as u64) {
        return ea_refused(estate, ea, node_id, "tiny-input-floor");
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
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; the drive arms only what the work can feed.
    let dop = elastic_dop(dop, total_granules);
    let ea_scan_node = ea.then_some(scan_plan.scan.plan.plan_node_id);
    let ea_timer = ea && runtime_instr::ea_timer(estate);
    // Router class resolution (M5-1): heap shapes re-class here — the entry
    // and its refusals are the scan arm's, the engagement is per-source.
    // Counter algebra at this single choke point: Engaged = ceremony
    // entered; Completed = the runtime answered; Fallback = serial rerun.
    let class = if is_cb { ArmClass::Scan } else { ArmClass::Heap };
    router::tick(class, ArmCounter::Engaged);
    // q2box diagnosis channel (trace-armed only, sibling of the refusal
    // line above): one line per engagement ceremony with the resolved DOP
    // and geometry — plain-exec engagement evidence without WFIN.
    if super::lane_trace_enabled() {
        lane_trace(&format!(
            "runtime-scan: engage dop={dop} granules={total_granules} rgs={nrgs}"
        ));
    }
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        total_granules,
        nrgs,
        source,
        payload_map,
        ea_scan_node,
        ea_timer,
        None,
    )?;
    router::tick(
        class,
        if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

/// Record-and-refuse (transparency line, ea-morsels.md §6): armed +
/// instrumented refusals only — every other path is byte-identical today.
/// M5-1: every scan-arm refusal also feeds the router's consolidated
/// taxonomy (entry-arm attribution — heap-shape refusals carry
/// self-describing reasons).
fn ea_refused<T>(
    estate: &mut EStateData<'_>,
    ea: bool,
    node_id: i32,
    reason: &'static str,
) -> PgResult<Option<T>> {
    router::tick_refused(ArmClass::Scan, reason);
    // q2box diagnosis channel: name plain-exec refusals in the server log
    // when the engagement trace is armed (PGRUST_LANE_V2_TRACE=1; default
    // off — byte-identical otherwise). EA runs already surface the reason
    // through the transparency line; this covers the timed/plain arms.
    if super::lane_trace_enabled() {
        lane_trace(&format!("runtime-scan: refused ({reason})"));
    }
    if ea {
        estate.runtime_ea_record_refusal(node_id, "scan", reason);
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    nrgs: usize,
    source: Arc<dyn runtime::MorselSource>,
    map: Option<Arc<runtime::GranuleMap>>,
    ea_scan_node: Option<i32>,
    ea_timer: bool,
    bitmap_ctx: Option<Arc<super::runtime_bitmap::BitmapMorselCtx>>,
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
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        map: OnceLock::new(),
        drive_at_entry: entry_drive_enabled(),
        standing: Mutex::new(None),
        instr: ea_scan_node.map(|_| {
            (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect()
        }),
        ea_timer,
        ea_epoch: std::time::Instant::now(),
        bitmap: bitmap_ctx,
    });
    // Set BEFORE any claim can run (submit happens inside engage_ceremony):
    // morsel_body expects the edges whenever the source coalesces (pgrcolumnar).
    if let Some(map) = map {
        payload
            .map
            .set(map)
            .unwrap_or_else(|_| unreachable!("map set once"));
    }

    // Submit-and-park ceremony. EnterParallelMode brackets the context
    // lifetime (CreateParallelContext asserts it); an error unwind aborts
    // the transaction, which destroys live contexts and resets the mode
    // (AtEOXact_Parallel — the Gather discipline).
    parallel::gtrace("l.engage.begin");
    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg,
        estate,
        rt,
        dop,
        total_granules,
        nrgs,
        source,
        ea_scan_node,
        &payload,
    );
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
    nrgs: usize,
    source: Arc<dyn runtime::MorselSource>,
    ea_scan_node: Option<i32>,
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
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(payload) as _;
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned_with_affinity(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        }, router::session_affinity_token());
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
        let mut all_exited_seen = false;
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
            // LIVENESS REAP (m35-spill inc-2c port — the FLAG named this
            // arm; the agg leg-4d wedge class): a pinned RG is invisible to
            // pool workers (rg.rs — publication never sets the global
            // active bit), so once every launched helper has exited without
            // the RG completing, NOBODY will ever step it and the leader
            // parks forever (the all-stopped probe above cannot see helpers
            // that exited their drive but parked back to the pool). Reap:
            // abort + drain the closed generation ourselves; the next
            // try_wait surfaces Aborted and the existing error/fallback
            // handling (finish_outcome) decides. Two consecutive sightings
            // before reaping let a mid-settlement completion land first —
            // belt only: a helper's exit bump happens-after its drive's
            // completion.complete(), and abort + drive_pinned on a
            // completed RG are benign no-ops.
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    lane_trace(
                        "runtime-scan: all helpers exited without completing the RG — reaping",
                    );
                    rg.abort();
                    drain_rg(rt, payload, &rg);
                    continue;
                }
                all_exited_seen = true;
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
            // EA-on-morsels merge (clean Completed outcomes ONLY — the same
            // invariant as the result partials): fold every worker's final
            // instrument export and write the bypassed scan node's
            // rows/loops/nfiltered (ea-morsels.md §3 — node-exact rows; the
            // Agg root ticks naturally through its procnode wrapper as the
            // result row returns).
            if let (Some(instr), Some(scan_node)) = (&payload.instr, ea_scan_node) {
                let ips: Vec<InstrumentPartial> = instr
                    .iter()
                    .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
                    .collect();
                let merged = runtime_instr::merge(ips.iter());
                runtime_instr::ea_fill_scan_node(estate, scan_node, &merged.rows);
                // Pipeline report for the inc-2 EXPLAIN block (one task
                // set on this arm; partials = non-empty result exports).
                estate.es_runtime_ea_pipelines.push(runtime_instr::ea_pipeline_report(
                    "scan",
                    agg.plan.plan.plan_node_id,
                    scan_node,
                    -1,
                    1,
                    parts.len() as u64,
                    &merged,
                ));
                lane_trace(&format!(
                    "runtime-scan: EA merged workers={} claims={} granules={} \
                     scanned={} survived={}",
                    merged.workers,
                    merged.claims,
                    merged.granules,
                    merged.rows.scanned,
                    merged.rows.survived
                ));
            }
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
    parallel::gtrace("l.publish.begin");
    let engaged = parallel::standing::try_engage(shared, dop.max(0) as usize);
    parallel::gtrace("l.publish.end");
    let Some(entry) = engaged else {
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
            parallel::gtrace("l.close.begin");
            parallel::standing::close_and_await(&entry);
            parallel::gtrace("l.close.end");
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
        // F1 PgResult propagation (train-12 composition seam): a raised
        // cancel disposition (statement_timeout / pg_cancel_backend)
        // surfaces from the latch quantum — the standing-loop mirror of
        // the launched path's F1 defect layer 2b branch and the CFI
        // branch above (abort THEN drain THEN close-and-await).
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            rg.abort();
            drain_rg(rt, payload, rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(e);
        }
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
    // sticky=true: standing gang workers may retain the session bind
    // between same-session engagements (ceremony-v2; launched/wpool
    // helpers must always park boundary-clean, hence false above).
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload, true)));
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
