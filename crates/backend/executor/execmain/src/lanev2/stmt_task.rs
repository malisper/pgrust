//! GL-STMTTASK-1 increment 1 — SERIAL STATEMENT AS A DOP-1 POOL TASK
//! (statement-as-task migration step 1; proposal
//! scratchpad/night/proposal-serial-as-task-migration.md).
//!
//! # What this is
//!
//! The narrowest honest slice of the structural fix for the admission-control
//! class: for an ADMITTED simple-protocol read-only statement, the session
//! thread stops executing the plan itself. It publishes the statement's
//! executor run as a dop-1 engagement on the runtime pool (the same
//! bound/standing engagement substrate every lane arm rides), parks, and
//! drains result rows from the row-emit funnel to the ordinary wire receiver.
//! The WAITING unit (this engagement) and the CPU unit (a pool worker) become
//! different objects; the session thread does protocol only.
//!
//! # Architecture (increment 1, as built)
//!
//! - IDENTITY: the worker wears the session's identity through the
//!   query-task binder (`parallel::with_query_task_binding`) exactly as pool
//!   engagements already do — leased bgworker-shaped PGPROC, lock-group
//!   membership under the session leader, the session's transaction +
//!   active-snapshot + GUC view bound for the drive span. Channel ladder:
//!   pool-db bound engagement (`PGRUST_RUNTIME_POOLDB=1`, per-RG board,
//!   concurrent) first, standing gang second (one board process-wide —
//!   correctness vehicle only), then FALL BACK TO THE INCUMBENT serial loop
//!   byte-identically. There is deliberately NO launched-bgworker fallback:
//!   a statement task that cannot ride a standing thread refuses.
//! - RESULT STREAMING: the worker never touches the client socket. It builds
//!   its own QueryDesc over the shared PlannedStmt and runs the ordinary
//!   per-tuple pull loop (`exec_proc_node`, junk filter included — the
//!   execute_plan plain-loop shape), materializing each result tuple into an
//!   owned MinimalTuple image pushed through the bounded row funnel
//!   ([`super::row_emit::RowEmitSink`]). The session thread drains the
//!   funnel and feeds the REAL DestReceiver (printtup → pqcomm): startup
//!   (RowDescription) already ran on the session thread in
//!   standard_executor_run, shutdown runs there after we return.
//! - PARK/WAKE: the session thread parks on the leader latch
//!   (`parallel::wait_parallel_finish_quantum`); funnel pushes/done set the
//!   latch through the funnel wake hook. The engagement enters the pool as
//!   a fresh (undecayed, p0) RG: under POOL-QOS the scheduler classifies it
//!   INTERACTIVE (sched.rs `qos_interactive_p`: priority >= p0 ⇔ < one
//!   decay quantum of consumed CPU) and its bound width feeds the
//!   interactive-demand ledger (`qos_demand_live`) that draws demoted
//!   serves' permit deferrals and serve-yields — the protection the needle
//!   latency measurement rides.
//! - ERROR IDENTITY: a worker-side ERROR is recorded first-wins in the
//!   payload as the ORIGINAL boxed PgError (message, sqlstate, hints,
//!   position travel by ownership) and re-raised on the session thread
//!   after the engagement joins — the client sees the same ErrorResponse
//!   bytes the incumbent path would have produced, including after rows
//!   were already streamed (mid-statement framing preserved: partial rows
//!   then the error, exactly the stock executor-error cadence). FATAL-class
//!   worker exits are NOT forwarded in inc-1: the exit-committed unwind
//!   kills the worker thread (its proc_exit drain owns cleanup), the
//!   Drop-guaranteed detach satisfies the join, and the leader raises the
//!   died-needle ERROR.
//! - INTERRUPTS: cancel / statement_timeout / client-loss land on the
//!   SESSION thread (its vpid owns the statement); the parked leader
//!   observes them at its CFI/quantum cadence and CHASES the task:
//!   close funnel demand (frees ring-parked producers, stops emit-side
//!   work), abort the RG, and deliver a thread cancel signal to the
//!   serving worker's leased identity (`procsignal::SendThreadSignal(pid,
//!   SIGINT)` — the pg_cancel_backend vector; the drive bracket registers
//!   the cancel disposition on the worker, mirroring the launched-bgworker
//!   signal set, so the worker's next CHECK_FOR_INTERRUPTS — the same
//!   per-page/per-row cadence C cancels at — raises, unwinds through the
//!   ordinary worker error path, and detaches). The leader then drains the
//!   RG bounded and joins the board; the statement's error is the LEADER's
//!   own cancel disposition, byte-identical to stock.
//!
//! # Envelope (everything else refuses BY NAME to the incumbent path)
//!
//! Simple protocol only (exec_simple_query arms exactly one statement's
//! top-level portal run — see `arm_statement`), single statement, CMD_SELECT
//! with a parallel-safe plan tree, no FOR UPDATE (rowMarks), no modifying
//! CTE, no result relations, complete-drain runs only (count = 0 — cursor
//! and extended-protocol row-limited cadences never arm), Remote dest, no
//! instrumentation, no EPQ, non-aborted non-subtransaction session state,
//! binder policy clean (no temp state, not serializable, no pending
//! invalidations, no bound params). The refusal taxonomy is
//! [`StmtTaskRefusal`]; every refusal of an ARMED statement ticks its named
//! counter and (traced builds) a `stmt-task: refused <reason>` line.
//!
//! # Knob
//!
//! `PGRUST_STMT_TASK` — DEFAULT OFF; arms on exactly `1`/`on` (t35
//! exact-spelling law, the pooldb posture parser precedent). OFF is
//! structurally inert: the only knob-OFF cost is the arm-site memoized
//! bool read in exec_simple_query; the executor hook short-circuits on the
//! thread-local armed flag, which OFF can never set.
//!
//! # Bounded resources (contention-evidence laws)
//!
//! This arm CREATES no new bounded resource. It rides three existing
//! bounds, cited per the proposal-contention-evidence-laws discipline:
//! pool execution permits (= cores; the worker's drive holds one under the
//! standard step rhythm, released around parks/blocking sections — the
//! donation facade), external pin-board lanes (MAX_EXTERNAL_LANES; the
//! serving worker's drive leases one; exhaustion = named refusal, never a
//! wait), and the per-worker funnel ring (DEFAULT_RING_CAP rows; the
//! producer parks under the K-standby blocking section, woken by drain or
//! demand-close — never while holding an execution obligation).

//! # Increment 2 (GL-STMTTASK-2) — kill the per-statement ceremony
//!
//! Four changes (scratchpad/night/wakeup-cost-survey.md "Convergent design
//! rules", cited by rule):
//!
//! 1. STANDING-ENGAGEMENT REUSE (`PGRUST_STMT_TASK_STANDING`, default ON
//!    under the master knob; `0`/`off` kills): the per-SESSION funnel is
//!    created once and reset per statement (`RowFunnel::reset_for_reuse`),
//!    and the WORKER side binds through the deferred binder with STICKY
//!    session retention — statement N+1 from the same session pays only
//!    the statement-half resume (`resume_statement`: transaction adopt,
//!    snapshots, invals drain), never the full session bind. Survey rule 6
//!    (ceremony is many slices): the funnel ring allocation and the
//!    session-half GUC/identity ceremony were two of them.
//! 2. POINTER-PASSING, NOT DSM RITUAL (`PGRUST_STMT_TASK_PTRPASS`, default
//!    ON under the master knob; `0`/`off` restores the inc-1 pcxt
//!    ceremony): the binder target is built by
//!    `parallel::statement_task_shared` — no parallel-context list entry,
//!    no error mailboxes, no registry key, no DestroyParallelContext walk;
//!    session-stable state travels by Arc (GUC pin, record registry,
//!    combocid), the plan by leader-arena pointer (inc-1's SendConst), the
//!    snapshot as the thread-native Send struct (never a byte-image
//!    ritual). The leader-unwind containment that DestroyParallelContext's
//!    private-shutdown hook provided is an RAII join guard in the engage
//!    frame. The DSM-shaped path is untouched for REAL parallel
//!    engagements.
//! 3. INLINE-EXECUTE DEFAULT (`PGRUST_STMT_TASK_INLINE`, default ON under
//!    the master knob; `0`/`off` kills): survey rule 1 (Cilk work-first;
//!    Go rejected handoff-with-wakeup; run-on-arrival is the default). If
//!    a pool seat is free WITHOUT WAITING (`Runtime::try_borrow_seat`),
//!    the session thread executes the statement ITSELF through the
//!    incumbent per-tuple loop — literally the same code, with the seat
//!    held for governed accounting — and no submission, no wake, no bind,
//!    no funnel exists. Cancel identity on this path IS the incumbent
//!    path's (CFI raises on the session thread). Enqueue+wake happens
//!    ONLY under contention (no free seat).
//! 4. SPINNER-COUNT WAKE ELISION (`PGRUST_POOL_WAKE_SPINNER`, default OFF,
//!    pool-wide): survey rule 2 (Go nmspinning/wakep) — the pool's
//!    new-work submission wake becomes epoch-bump + spinner check; notify
//!    only when no searcher covers the work, and then exactly ONE idle
//!    worker in LIFO order with the 3-poll starvation cap (survey rule 4).
//!    Lives in `pgsync::ParkLot` / `runtime::sched`, not here; the enqueue
//!    path rides it when armed.
//!
//! The engaged/inline trace lines carry `unparks=<delta>` (the wakeup
//! program's unparks-per-statement metric, survey rule 6): the leader
//! reads the pool park lot's unpark counter around its own ceremony.

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_scan::sdir::ScanDirection;

use runtime::{DrainStep, RowFunnel};

use super::row_emit::{MinImage, RowEmitSink, DEFAULT_RING_CAP};
use super::{lane_trace, lane_trace_enabled};

// ---------------------------------------------------------------------------
// Knob + protocol arming: `postgres_seams::stmt_task_arm` — the one crate
// both the tcop arm site and this hook production-link. The knob
// (`PGRUST_STMT_TASK`, default OFF, exact spellings `1`/`on`) and the
// statement-scoped armed flag are unit-pinned there.
// ---------------------------------------------------------------------------

use ::postgres_seams::stmt_task_arm::take_armed;

// ---------------------------------------------------------------------------
// GL-STMTTASK-2 sub-knobs (all under the master PGRUST_STMT_TASK, which is
// DEFAULT OFF — the server's default posture is unchanged; these are the
// ablation kill levers within the armed lane, the t35 layered-kill shape).
// ---------------------------------------------------------------------------

/// Change 1 kill: `PGRUST_STMT_TASK_STANDING=0|off` restores the
/// per-statement funnel + the eager per-statement worker bind (inc-1).
fn stmt_standing_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_STMT_TASK_STANDING").ok().as_deref().map(str::trim),
            Some("0") | Some("off")
        )
    })
}

/// Change 2 kill: `PGRUST_STMT_TASK_PTRPASS=0|off` restores the inc-1
/// parallel-context ceremony (CreateParallelContext + InitializeParallelDSM
/// + InstallQueryTaskBinding + DestroyParallelContext) per statement.
fn stmt_ptrpass_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_STMT_TASK_PTRPASS").ok().as_deref().map(str::trim),
            Some("0") | Some("off")
        )
    })
}

/// Change 3 kill: `PGRUST_STMT_TASK_INLINE=0|off` disables inline-execute
/// (every admitted statement takes the enqueue path).
fn stmt_inline_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_STMT_TASK_INLINE").ok().as_deref().map(str::trim),
            Some("0") | Some("off")
        )
    })
}

// ---------------------------------------------------------------------------
// Refusal taxonomy + engagement counters (diagnostics; e2e witnesses).
// ---------------------------------------------------------------------------

/// Named refusals of ARMED statements (unarmed statements never reach the
/// gates and are not counted — the incumbent path is not a refusal).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StmtTaskRefusal {
    /// Count-limited run (cursor/extended-protocol cadence reached an armed
    /// statement — defensive; arming excludes these upstream).
    CountLimited,
    /// Not the wire portal receiver (nested run, SQL function, CTAS fill).
    NotRemoteDest,
    /// EXPLAIN ANALYZE / instrumented run.
    Instrumented,
    /// EPQ recheck drive, cursor/SPI budget, parked lane-cursor pipeline.
    ExecutorCadence,
    /// Already inside parallel machinery (worker or parallel mode).
    InParallel,
    /// Runtime pool absent or disabled.
    NoRuntime,
    /// Plan shape: not CMD_SELECT / has rowMarks / modifying CTE / result
    /// relations / utility.
    PlanShape,
    /// Plan tree not parallel-safe (the binder's execution environment is
    /// the parallel-worker one; parallel-restricted/unsafe constructs keep
    /// the incumbent path).
    NotParallelSafe,
    /// Binder policy: temp state, serializable, or pending invalidations.
    BinderPolicy,
    /// No channel served (pool/gang unavailable, refused, or claim
    /// deadline) — the RG was reaped untouched.
    NoChannel,
    /// The dop-1 ceremony builder refused (interrupt holdoff / critical
    /// section, uncommitted enum values) — GL-STMTTASK-2 fast path only;
    /// the launched-DSM path turns the enum case into an ERROR instead.
    CeremonyRefused,
}

impl StmtTaskRefusal {
    fn name(self) -> &'static str {
        match self {
            StmtTaskRefusal::CountLimited => "count-limited",
            StmtTaskRefusal::NotRemoteDest => "not-remote-dest",
            StmtTaskRefusal::Instrumented => "instrumented",
            StmtTaskRefusal::ExecutorCadence => "executor-cadence",
            StmtTaskRefusal::InParallel => "in-parallel",
            StmtTaskRefusal::NoRuntime => "no-runtime",
            StmtTaskRefusal::PlanShape => "plan-shape",
            StmtTaskRefusal::NotParallelSafe => "not-parallel-safe",
            StmtTaskRefusal::BinderPolicy => "binder-policy",
            StmtTaskRefusal::NoChannel => "no-channel",
            StmtTaskRefusal::CeremonyRefused => "ceremony-refused",
        }
    }
}

static STMT_ENGAGED: AtomicU64 = AtomicU64::new(0);
static STMT_COMPLETED: AtomicU64 = AtomicU64::new(0);
static STMT_REFUSED: AtomicU64 = AtomicU64::new(0);
static STMT_INLINE: AtomicU64 = AtomicU64::new(0);

/// (engaged, completed, refused-armed) — tests/diagnostics.
pub fn stmt_task_engagements() -> (u64, u64, u64) {
    (
        STMT_ENGAGED.load(Ordering::SeqCst),
        STMT_COMPLETED.load(Ordering::SeqCst),
        STMT_REFUSED.load(Ordering::SeqCst),
    )
}

/// GL-STMTTASK-2: statements executed on the INLINE fast path (a subset of
/// engaged) — tests/diagnostics.
pub fn stmt_task_inline_count() -> u64 {
    STMT_INLINE.load(Ordering::SeqCst)
}

fn refuse(reason: StmtTaskRefusal) -> PgResult<StmtTaskVerdict> {
    STMT_REFUSED.fetch_add(1, Ordering::SeqCst);
    if lane_trace_enabled() {
        lane_trace(&format!("stmt-task: refused {}", reason.name()));
    }
    Ok(StmtTaskVerdict::Incumbent)
}

/// The hook's answer to execute_plan (GL-STMTTASK-2).
pub(crate) enum StmtTaskVerdict {
    /// Not armed / refused / fell back — the caller runs the serial
    /// per-tuple loop byte-identically.
    Incumbent,
    /// Change 3 (inline-execute): admitted, and a pool seat was free
    /// without waiting — the CALLER runs its own serial loop (literally
    /// the incumbent code) with this seat held for governed accounting.
    /// Cancel/timeout identity on this path IS the incumbent path's.
    Inline(runtime::InlineSeat),
    /// Engaged on the pool; every row was streamed and es_processed set —
    /// the caller skips the serial loop.
    Handled,
}

// ---------------------------------------------------------------------------
// Shared payload (leader arena pstmt + funnel + engagement accounting).
// ---------------------------------------------------------------------------

/// `*const PlannedStmt` shipped to the bound worker. The pstmt lives in the
/// session's per-message arena and outlives the worker: every exit path of
/// the engage ceremony completes the RG and joins the board
/// (`close_and_await`) before this frame — and with it the arena — unwinds
/// (the SendConst contract every runtime arm rides).
struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: leader-arena pstmt, immutable during the engagement, alive until
// the ceremony joins the worker; the worker only reads it.
unsafe impl Send for SendConstPstmt {}
unsafe impl Sync for SendConstPstmt {}

pub(super) struct StmtTaskShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// One ring per pin-board worker index, drained by the session thread.
    funnel: Arc<RowFunnel<MinImage>>,
    /// Board-entry slot held across the wait so the PRIVATE_SHUTDOWN hook
    /// can complete the standing join on leader unwind paths.
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    /// Participants that bound and entered the drive.
    started: AtomicUsize,
    /// Payload-side refusals (bind/lane refusals inside the drive).
    refused: AtomicUsize,
    /// The serving worker's leased-identity pid while it is inside the
    /// drive bracket (0 otherwise) — the leader's cancel-chase target.
    worker_pid: AtomicI32,
    /// First worker-phase error (the original boxed PgError — error
    /// identity travels by ownership).
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
}

impl StmtTaskShared {
    fn new(
        rt: &'static Arc<runtime::Runtime>,
        pstmt: *const PlannedStmt<'static>,
        query_text: String,
        eflags: i32,
        funnel: Arc<RowFunnel<MinImage>>,
    ) -> Arc<StmtTaskShared> {
        Arc::new(StmtTaskShared {
            rt,
            rg: OnceLock::new(),
            pcxt_shared: OnceLock::new(),
            pstmt: SendConstPstmt(pstmt),
            query_text,
            eflags,
            funnel,
            standing: Mutex::new(None),
            started: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            worker_pid: AtomicI32::new(0),
            error: Mutex::new(None),
            failed: AtomicBool::new(false),
        })
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        // Abort the RG so the leader observes completion and close demand so
        // a ring-parked producer wakes (the passthrough fail discipline).
        self.funnel.close_demand();
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Worker side: the single-morsel statement run.
// ---------------------------------------------------------------------------

impl runtime::TaskSetWork for StmtTaskShared {
    fn run_morsel(&self, worker: usize, _range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) || self.funnel.demand_closed() {
            // Aborting/canceled: drop the claim without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.statement_body(worker)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(unwind) => {
                self.fail(PgError::new(ERROR, "statement task worker panicked").into());
                // Exit-committed unwinds (FATAL) must keep dying: the
                // serve/glue layers own the thread's exit drain.
                if parallel::standing::is_exit_unwind(&*unwind) {
                    std::panic::resume_unwind(unwind);
                }
            }
        }
    }

    fn finalize(&self) {
        // Streaming taskset: publish producers-done so the leader drain
        // reaches EOF once the buffered remainder is pumped.
        self.funnel.mark_all_done();
    }
}

impl StmtTaskShared {
    /// The whole admitted statement, on the bound worker, as ONE unit:
    /// build this worker's QueryDesc over the shared plan, run the ordinary
    /// per-tuple pull loop emitting into the funnel, tear down. Runs INSIDE
    /// the query-task binding (session transaction + active snapshot + GUC
    /// view bound) and inside the serve's parallel-worker impersonation.
    fn statement_body(&self, worker: usize) -> PgResult<()> {
        // SAFETY: leader-arena pstmt, alive until the ceremony joins this
        // worker (SendConst contract above).
        let pstmt: &PlannedStmt<'_> = unsafe { &*self.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &self.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let run = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, self.eflags)?;
            self.pull_loop(qd, worker)
        })();
        match run {
            Ok(()) => {
                // Clean teardown while still bound: finish/end/free — the
                // sink arms' per-helper discipline.
                let r = crate::execmain::executor_finish_seam(qd)
                    .and_then(|()| crate::execmain::executor_end_seam(qd));
                match r {
                    Ok(()) => {
                        crate::querydesc::free_query_desc_seam(qd);
                        Ok(())
                    }
                    Err(e) => {
                        crate::querydesc::release_query_desc_seam(qd);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                // Self-error: release (possibly mid-batch executor); the
                // binder's transaction-abort unbind owns resource cleanup.
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    }

    /// The execute_plan plain-loop shape (per-tuple pull, junk filter,
    /// SELECT es_processed accounting) with the receive step swapped for
    /// the funnel emit. Complete-drain only (count-limited runs never
    /// arm), forward direction only (the run seam law).
    ///
    /// GL-STMTTASK-2: the funnel has exactly ONE ring — a dop-1 statement
    /// task has at most one producer per engagement (the single synthetic
    /// morsel's claim is consumed even on a panic, so no second run_morsel
    /// can ever exist), always ring 0 regardless of the serving lane's
    /// ordinal.
    fn pull_loop(&self, qd: ::types_portal::QueryDescHandle, _worker: usize) -> PgResult<()> {
        crate::querydesc::with_qd(qd, |q| {
            let x = q.exec.as_mut().expect("statement task executor state");
            x.with_mut(|d| -> PgResult<()> {
                let crate::querydesc::ExecData { estate, planstate } = d;
                let planstate =
                    planstate.as_mut().expect("statement task run without a plan state");
                estate.es_direction = ScanDirection::ForwardScanDirection;
                let mut sink = RowEmitSink::new(self.funnel.producer(0));
                loop {
                    // Leader chase / client stop: observed per output row
                    // (inner executor CFIs carry the non-emitting stretches
                    // — the SIGINT chase raises there).
                    if self.failed.load(Ordering::SeqCst) || self.funnel.demand_closed() {
                        break;
                    }
                    ::postgres_seams::check_for_interrupts::call()?;
                    estate.reset_per_tuple_expr_context();
                    let Some(mut slot_id) = crate::procnode::exec_proc_node(planstate, estate)?
                    else {
                        break;
                    };
                    if estate.es_junkFilter.is_some() {
                        slot_id = ::execjunk::exec_filter_junk(estate, slot_id);
                    }
                    // emit_blocking materializes + pushes (parking on a full
                    // ring under the K-standby blocking section) and bumps
                    // es_processed; false = demand closed — stop producing.
                    if !sink.emit_blocking(slot_id, estate)? {
                        break;
                    }
                }
                Ok(())
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Worker side: the standing/pool driver (binder wrap + drive).
// ---------------------------------------------------------------------------

/// The worker-side cancel disposition for the drive bracket: mirrors
/// StatementCancelHandler (tcop) — this crate cannot name tcop, and the
/// serve threads register only SIGQUIT at spawn, so the bracket installs
/// this before the drive (left registered after: the launched-bgworker
/// db-connection signal set has it permanently, which is this identity
/// class's C parity).
fn stmt_task_cancel_disposition() {
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
}

/// The engagement driver (rides the ParallelShared; dispatched verbatim by
/// gang serves and pool serves). Runs ON the serving worker, impersonated
/// and lock-grouped by serve_ticket; owns the eager binder wrap + the
/// pinned drive + payload error routing.
fn stmt_task_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<StmtTaskShared>() else { return };
    helper_drive_stmt(&payload);
}

fn helper_drive_stmt(payload: &Arc<StmtTaskShared>) {
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(target) = payload.pcxt_shared.get() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    // GL-STMTTASK-2 change 1 (worker half): bind through the DEFERRED
    // binder with sticky session retention — statement N+1 from the same
    // session on this worker resumes the statement half only. The eager
    // wrap below is the inc-1 shape (kill: PGRUST_STMT_TASK_STANDING=0,
    // or the binder-layer kills PGRUST_RUNTIME_LAZYBIND/STICKY=0).
    if stmt_standing_enabled() && parallel::lazy_bind_enabled() {
        helper_drive_stmt_sticky(payload, target, &rg);
        return;
    }
    // Process-wide pin-board lane lease: exhaustion = fail-closed
    // non-participation (the leader's nobody-participates check falls back).
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("stmt-task: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        // Cancel-chase bracket: install the cancel disposition (see
        // stmt_task_cancel_disposition), clear any stale cancel aimed at a
        // PREVIOUS occupant of this leased identity, and publish the pid
        // the leader chases. Symmetric teardown below.
        procsignal::pqsignal_thread(
            procsignal::signums::SIGINT,
            procsignal::ThreadSignalHandler::Simple(stmt_task_cancel_disposition),
        );
        init_small::globals::SetQueryCancelPending(false);
        payload.worker_pid.store(init_small::globals::MyProcPid(), Ordering::SeqCst);
        let r = drive_bound_stmt(payload, &mut local, &rg, &mut lane.borrow_mut());
        // A chase signal that landed after the statement finished must not
        // leak into this thread's next serve.
        payload.worker_pid.store(0, Ordering::SeqCst);
        init_small::globals::SetQueryCancelPending(false);
        r
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                payload.fail(e);
                // F1 liveness: an aborted PINNED RG still needs a driver to
                // run protocol cleanup to completion, or the leader waits on
                // the died-needle cadence.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                // Binder validate() refusal: fail-closed non-participation.
                lane_trace(&format!("stmt-task: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

/// GL-STMTTASK-2 change 1, worker half: the deferred-binder drive with
/// STICKY session retention (the scan arm's ceremony-v2 shape, dop-1
/// rendering). Sticky is allowed on gang serves unconditionally and on
/// pool serves under the pool-sticky posture (the scan arm's rule); the
/// bind happens PRE-DRIVE (the dop-1 participant holds the board's only
/// ticket and always claims — deferring to first touch would only move
/// the xmin-visible window by microseconds while complicating the error
/// envelope), so a bind failure stays a fail-closed refusal with the RG
/// untouched, exactly inc-1's surface. The binding ALWAYS completes before
/// any error/panic propagates (sticky park on the clean path, full abort
/// unbind otherwise — the scan arm's choreography).
fn helper_drive_stmt_sticky(
    payload: &Arc<StmtTaskShared>,
    target: &Arc<parallel::ParallelShared>,
    rg: &runtime::RgHandle,
) {
    let sticky = !parallel::standing::serving_on_pool()
        || parallel::standing::pool_sticky_enabled();
    let binding = match parallel::DeferredQueryTaskBinding::new(target, sticky) {
        Ok(b) => b,
        Err(e) => {
            lane_trace(&format!(
                "stmt-task: helper refused (sticky eviction failed: {})",
                e.message()
            ));
            payload.refused.fetch_add(1, Ordering::SeqCst);
            return;
        }
    };
    if let Err(e) = binding.validate() {
        lane_trace(&format!("stmt-task: helper bind refused: {}", e.message()));
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    }
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("stmt-task: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let mut lane = Some(lane);
    if let Err(e) = binding.bind_now() {
        // Fail-closed non-participation: the bind's own error path completed
        // the unbind; the RG is untouched by this worker.
        lane_trace(&format!("stmt-task: helper bind refused: {}", e.message()));
        payload.refused.fetch_add(1, Ordering::SeqCst);
        let _ = binding.finish(false);
        return;
    }
    if binding.resumed_sticky() {
        lane_trace("stmt-task: sticky resume");
    }
    payload.started.fetch_add(1, Ordering::SeqCst);
    // Cancel-chase bracket (inc-1 verbatim): install the disposition, clear
    // any stale cancel aimed at a previous occupant of this leased
    // identity, publish the pid the leader chases; symmetric teardown.
    procsignal::pqsignal_thread(
        procsignal::signums::SIGINT,
        procsignal::ThreadSignalHandler::Simple(stmt_task_cancel_disposition),
    );
    init_small::globals::SetQueryCancelPending(false);
    payload.worker_pid.store(init_small::globals::MyProcPid(), Ordering::SeqCst);
    let r = catch_unwind(AssertUnwindSafe(|| {
        drive_bound_stmt(payload, &mut local, rg, &mut lane)
    }));
    payload.worker_pid.store(0, Ordering::SeqCst);
    init_small::globals::SetQueryCancelPending(false);
    let commit = matches!(r, Ok(Ok(())));
    // The binding completes HERE, before any propagation: sticky park on
    // commit, full abort unbind otherwise (its own catch/retry choreography
    // inside). An exit-committed unwind below rethrows AFTER this — the
    // eager wrap ran its finish(false) on that path too.
    let finish = binding.finish(commit);
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            payload.fail(e);
            // F1 liveness: an aborted PINNED RG still needs a driver to run
            // protocol cleanup to completion (inc-1's Err arm).
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = payload.rt.drive_pinned(&mut local, rg);
            }
        }
        Err(unwind) => {
            payload.fail(PgError::new(ERROR, "statement task worker panicked").into());
            if parallel::standing::is_exit_unwind(&*unwind) {
                std::panic::resume_unwind(unwind);
            }
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = payload.rt.drive_pinned(&mut local, rg);
            }
        }
    }
    if let Err(e) = finish {
        payload.fail(e);
    }
}

/// Bound drive: the pool-serve-aware pinned drive (yield-capable under
/// POOL-QOS) around the single statement morsel; error routing mirrors the
/// scan arm's drive_bound.
fn drive_bound_stmt(
    payload: &Arc<StmtTaskShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    let _end = super::standing_channel::drive_pool_serve(payload.rt, local, rg, lane);
    debug_assert!(payload.rt.debug_pin_settled(local), "pin unsettled after statement drive");
    if payload.failed.load(Ordering::SeqCst) {
        // The morsel body recorded the real error (fail() is first-wins);
        // this marker routes the binder through its transaction-abort
        // unbind (the executor was released, not finished).
        return Err(PgError::new(ERROR, "statement task unwound (recorded upstream)").into());
    }
    Ok(())
}

/// Registered launched-path entrypoint. The statement task NEVER launches
/// bgworkers (its fallback is the incumbent serial loop, not a gang) — the
/// registration only keeps the parallel-context name resolvable.
fn stmt_task_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn stmt_task_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<StmtTaskShared>() else { return };
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    payload.funnel.close_demand();
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg_stmt(payload.rt, &payload.funnel, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint("pgrust_stmt_task_main", stmt_task_worker_main);
        parallel::register_parallel_private_shutdown(stmt_task_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader side: chase + drain, the engage ceremony, the wait/pump loop.
// ---------------------------------------------------------------------------

/// Cancel-chase + bounded drain of the pinned RG (the leader's protocol
/// cleanup): close demand (frees ring-parked producers; stops the emit
/// loop), abort the RG, kick the serving worker's leased identity so its
/// next executor CFI raises (the non-emitting-stretch chase), then drive
/// the aborted RG to completion from a leader-acquired lane.
fn chase_and_drain_stmt(
    payload: &Arc<StmtTaskShared>,
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    rg: &runtime::RgHandle,
) -> bool {
    funnel.close_demand();
    rg.abort();
    let pid = payload.worker_pid.load(Ordering::SeqCst);
    if pid != 0 {
        // Best-effort: the worker may already have unpublished (finished).
        let _ = procsignal::SendThreadSignal(pid, procsignal::signums::SIGINT);
    }
    drain_rg_stmt(rt, funnel, rg)
}

/// Re-deliver the cancel until every claimed participant detached, so the
/// close_and_await that follows is bounded. Two races this closes: (1) the
/// first chase can read `worker_pid` as 0 (the worker claimed but had not
/// published yet — the signal was skipped entirely); (2) a signal delivered
/// between the worker's stale-cancel clear and its first CFI is consumed
/// with nothing pending. Detach is Drop-guaranteed once the worker unwinds
/// anywhere, and the executor's CFI cadence (per page / per SRF row / per
/// emit) turns a delivered cancel into that unwind — persistent re-delivery
/// makes the join terminate. Cheap: fires only on leader error paths.
fn chase_until_joined(
    payload: &Arc<StmtTaskShared>,
    entry: &Arc<parallel::standing::StandingEngagement>,
) {
    loop {
        // Read order: claimed BEFORE detached — a straggler claim landing
        // between the reads is UNDER-counted, so the loop terminates once
        // the in-flight serve settles; close_and_await's under-lock
        // recheck covers anything the snapshot missed. The caller closed
        // the board first (close_no_wait), so claims cannot keep arriving.
        let claimed = entry.claimed();
        if entry.detached() >= claimed {
            break;
        }
        let pid = payload.worker_pid.load(Ordering::SeqCst);
        if pid != 0 {
            let _ = procsignal::SendThreadSignal(pid, procsignal::signums::SIGINT);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Abort + bounded drain (the passthrough drain_rg_pt shape). Bounded;
/// returns whether the RG reached an outcome.
fn drain_rg_stmt(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    rg: &runtime::RgHandle,
) -> bool {
    rg.abort();
    funnel.close_demand();
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

/// First-claim deadline for the statement task's board channels (the
/// standing channel's deadline discipline; behavioral, pg_clock domain).
fn stmt_claim_deadline() -> std::time::Duration {
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(crate::once_val(&MS, || {
        std::env::var("PGRUST_STMT_TASK_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

enum StmtWait {
    /// The RG reached an outcome under this channel's participation.
    Done(runtime::RgOutcome),
    /// Channel refused/deadline with the RG untouched (started == 0).
    Fallback,
}

pub(super) enum StmtTaskOutcome {
    /// Engaged and completed; `.0` = rows delivered to the wire receiver.
    Completed(u64),
    /// No channel served; nothing was consumed — run the incumbent loop.
    Fallback,
    /// GL-STMTTASK-2 fast path: the ceremony builder refused (interrupt
    /// holdoff / uncommitted enums) — nothing was built; incumbent loop.
    CeremonyRefused,
}

// ---------------------------------------------------------------------------
// GL-STMTTASK-2 change 1 (leader half): the per-session persistent funnel.
// ---------------------------------------------------------------------------

thread_local! {
    /// The session thread's statement-task funnel (ONE ring — a dop-1
    /// statement task has exactly one producer per engagement, always on
    /// index 0), created with its leader-latch wake hook on the first
    /// armed statement and RESET per statement (RowFunnel::reset_for_reuse)
    /// — the standing-engagement reuse that deletes the per-statement ring
    /// allocation. Session-thread TLS: dies with the thread; never
    /// captured/restored by an envelope (a pure transport cache — carries
    /// no session identity; rows never survive a statement).
    static SESSION_FUNNEL: RefCell<Option<Arc<RowFunnel<MinImage>>>> =
        const { RefCell::new(None) };
}

fn fresh_stmt_funnel() -> Arc<RowFunnel<MinImage>> {
    let funnel: Arc<RowFunnel<MinImage>> = RowFunnel::new(1, DEFAULT_RING_CAP);
    // Producer pushes/done wake the parked leader immediately (the funnel
    // wake hook sets the leader latch; the wait quantum is the backstop).
    let leader_proc = init_small::globals::MyProcNumber();
    funnel.set_wake_hook(Box::new(move || {
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(leader_proc));
    }));
    funnel
}

/// The statement's funnel: the session-persistent one (reset for reuse)
/// under change 1, a fresh one per statement when killed
/// (PGRUST_STMT_TASK_STANDING=0 — the inc-1 allocation shape, sized to the
/// dop-1 truth either way).
fn stmt_funnel() -> Arc<RowFunnel<MinImage>> {
    if !stmt_standing_enabled() {
        return fresh_stmt_funnel();
    }
    SESSION_FUNNEL.with(|slot| {
        let mut slot = slot.borrow_mut();
        match slot.as_ref() {
            Some(f) => {
                // Quiesced by contract: the previous statement's ceremony
                // joined its board (detached == claimed) and dropped its
                // drain before returning.
                f.reset_for_reuse();
                Arc::clone(f)
            }
            None => {
                let f = fresh_stmt_funnel();
                *slot = Some(Arc::clone(&f));
                f
            }
        }
    })
}

/// The statement-task engage ceremony + wait/pump loop. `emit_row` receives
/// each drained image on the SESSION thread and forwards it to the real
/// wire receiver; returns false to stop (client stop). On return the RG is
/// complete and the board joined (and, on the pcxt ceremony, the parallel
/// context destroyed).
pub(super) fn engage_stmt_task(
    rt: &'static Arc<runtime::Runtime>,
    pstmt: *const PlannedStmt<'static>,
    query_text: &str,
    eflags: i32,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<StmtTaskOutcome> {
    let funnel = stmt_funnel();
    let payload =
        StmtTaskShared::new(rt, pstmt, query_text.to_string(), eflags, Arc::clone(&funnel));

    // EnterParallelMode brackets the engagement (the pcxt ceremony's
    // CreateParallelContext asserts it; the fast path keeps the leader-side
    // semantics identical); an error unwind aborts the transaction, which
    // resets the mode (the Gather discipline).
    ::xact::EnterParallelMode();
    let r = if stmt_ptrpass_enabled() {
        engage_ceremony_fast(rt, &funnel, &payload, emit_row)
    } else {
        engage_ceremony_pcxt(rt, &funnel, &payload, emit_row)
    };
    ::xact::ExitParallelMode();
    r
}

/// GL-STMTTASK-2 change 2: the RAII join guard that replaces the pcxt
/// ceremony's private-shutdown hook + teardown tail on the fast path. Drop
/// runs UNCONDITIONALLY (the hook ran at DestroyParallelContext on every
/// inc-1 path, completion included — every action is idempotent): abort a
/// live RG, close funnel demand, complete the standing join so the leader
/// arena never unwinds under a claimed worker (the SendConst contract).
struct StmtJoinGuard {
    payload: Arc<StmtTaskShared>,
}

impl Drop for StmtJoinGuard {
    fn drop(&mut self) {
        let payload = &self.payload;
        let rg = payload.rg.get().and_then(|w| w.upgrade());
        // Happy path (RG complete, board slot already taken by the wait
        // loop's own cleanup): do nothing — the inc-1 hook's unconditional
        // abort() here was a gratuitous herd wake of the whole pool per
        // statement (witnessed by the unparks counter). Abort + demand
        // close + join only when something is actually left to reap.
        let incomplete = rg.as_ref().is_some_and(|rg| rg.try_outcome().is_none());
        let slot_held =
            payload.standing.lock().unwrap_or_else(|p| p.into_inner()).is_some();
        if !incomplete && !slot_held {
            return;
        }
        if let Some(rg) = &rg {
            rg.abort();
        }
        payload.funnel.close_demand();
        super::standing_channel::shutdown_standing_join(
            &payload.standing,
            rg.as_ref(),
            &|rg| drain_rg_stmt(payload.rt, &payload.funnel, rg),
        );
    }
}

/// The driver installed on the engagement's shared state (both
/// ceremonies): deferred+sticky under change 1, the inc-1 eager binder
/// when killed.
fn stmt_driver() -> parallel::standing::StandingDriver {
    parallel::standing::StandingDriver {
        drive: stmt_task_standing_driver,
        deferred_bind: stmt_standing_enabled() && parallel::lazy_bind_enabled(),
    }
}

/// GL-STMTTASK-2 change 2: the dop-1 fast ceremony — binder target built
/// by `parallel::statement_task_shared` (no pcxt list entry, no error
/// mailboxes, no registry key, no Destroy walk), leader-unwind containment
/// by the RAII join guard. The channel ladder and every wait/interrupt/
/// error path are shared with the pcxt ceremony byte-for-byte.
fn engage_ceremony_fast(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    payload: &Arc<StmtTaskShared>,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<StmtTaskOutcome> {
    let Some(shared) = parallel::statement_task_shared(parallel::query_task_policy_probe())?
    else {
        return Ok(StmtTaskOutcome::CeremonyRefused);
    };
    payload
        .pcxt_shared
        .set(Arc::clone(&shared))
        .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
    parallel::set_private_shared(&shared, Arc::clone(payload) as _);
    parallel::set_standing_driver_shared(&shared, stmt_driver());
    // Armed BEFORE the submission so every exit — `?`, panic, completion —
    // completes the RG and joins the board before this frame (and with it
    // the leader arena the SendConst pstmt lives in) unwinds.
    let _join = StmtJoinGuard { payload: Arc::clone(payload) };
    run_channel_ladder(rt, funnel, payload, emit_row)
}

/// The inc-1 parallel-context ceremony (PGRUST_STMT_TASK_PTRPASS=0): kept
/// as the ablation baseline and the belt-and-suspenders fallback. Same
/// channel ladder.
fn engage_ceremony_pcxt(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    payload: &Arc<StmtTaskShared>,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<StmtTaskOutcome> {
    ensure_hooks_registered();
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_stmt_task_main", 1)?;

    let body = (|emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>|
     -> PgResult<StmtTaskOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        // The REAL session policy: the leader gates refused any set flag, so
        // an install that still carries one is a late state change — the
        // binder's validate() refuses fail-closed on the worker.
        parallel::InstallQueryTaskBinding(pcxt, parallel::query_task_policy_probe())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        parallel::set_standing_driver(pcxt, stmt_driver());
        run_channel_ladder(rt, funnel, payload, emit_row)
    })(emit_row);

    // Teardown tail: a submitted RG must be COMPLETE before
    // DestroyParallelContext (the private-shutdown hook covers unwinds;
    // this covers `?` returns). The ladder joins on its own exits; the
    // rg-slot re-check here is belt-and-suspenders.
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        if rg.try_outcome().is_none() {
            drain_rg_stmt(rt, funnel, &rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;
    Ok(outcome)
}

/// The engagement body both ceremonies share: build the pool channel,
/// submit the dop-1 pinned RG, wait/pump on the channel ladder
/// (pooldb → gang → fallback). Byte-identical to the inc-1 body.
fn run_channel_ladder(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    payload: &Arc<StmtTaskShared>,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<StmtTaskOutcome> {
    // POOL-DB channel first (per-RG board, concurrent; the descriptor
    // must ride the submission). None ⇒ gang channel below.
    let pool = super::standing_channel::try_pool_channel(
        payload.pcxt_shared.get().expect("pcxt shared set above"),
        1,
        /* sinks_gate */ false,
    );

    let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(payload) as _;
    let source: Arc<dyn runtime::MorselSource> =
        Arc::new(runtime::SyntheticMorselSource::new(1));
    static NEXT_QID: AtomicUsize = AtomicUsize::new(1);
    let spec = runtime::QuerySpec {
        query_id: NEXT_QID.fetch_add(1, Ordering::SeqCst) as u64,
        tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
    };
    let set_rg = |rg: &runtime::RgHandle| {
        payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
    };
    let (rg, waiter) = match &pool {
        Some((_, descriptor)) => rt.submit_pinned_bound(
            spec,
            super::router::session_affinity_token(),
            descriptor.clone(),
            set_rg,
        ),
        None => {
            let (rg, waiter) =
                rt.submit_pinned_with_affinity(spec, super::router::session_affinity_token());
            set_rg(&rg);
            (rg, waiter)
        }
    };

    // GL-SLEASE-1 discipline: a leased session leader is about to PARK
    // while a pool worker executes its statement — give the permit up
    // for the wait span (re-acquired by the guard's drop).
    let _lease_yield = crate::execmain::serial_lease_yield_for_engagement();

    let mut emitted: u64 = 0;
    let mut stopped = false;

    // Pool channel wait; its refusal closes the board and tries the gang.
    if let Some((entry, _)) = &pool {
        *payload.standing.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(entry));
        match wait_pump(
            payload, rt, funnel, entry, "pooldb", &rg, &waiter, emit_row, &mut emitted,
            &mut stopped,
        )? {
            StmtWait::Done(o) => {
                return finish_stmt(payload, funnel, o, emit_row, &mut emitted, &mut stopped);
            }
            StmtWait::Fallback => {}
        }
    }

    // Standing gang channel (one board process-wide: under concurrency
    // a busy board refuses here and the statement stays incumbent).
    let engaged = parallel::standing::try_engage(
        payload.pcxt_shared.get().expect("pcxt shared set above"),
        1,
    );
    if let Some(entry) = engaged {
        *payload.standing.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&entry));
        match wait_pump(
            payload, rt, funnel, &entry, "standing", &rg, &waiter, emit_row, &mut emitted,
            &mut stopped,
        )? {
            StmtWait::Done(o) => {
                return finish_stmt(payload, funnel, o, emit_row, &mut emitted, &mut stopped);
            }
            StmtWait::Fallback => {}
        }
    }

    // No channel served. The RG is untouched (started == 0 on every
    // Fallback exit) — reap it and let the incumbent loop run. A row
    // can only have been pumped after a start, so emitted == 0 here.
    debug_assert_eq!(emitted, 0, "fallback after rows were streamed");
    drain_rg_stmt(rt, funnel, &rg);
    if payload.started.load(Ordering::SeqCst) != 0 || emitted != 0 {
        // A straggler started against the closing board: the plan may
        // have partially streamed — never rerun. Surface the recorded
        // error or the died shape.
        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        return Err(Box::new(PgError::new(
            ERROR,
            "statement task worker exited before completing the statement",
        )));
    }
    Ok(StmtTaskOutcome::Fallback)
}

/// Drain every currently-available row to the wire (never parks). Sets
/// `stopped` (and closes demand) when the receiver stops.
fn pump_stmt(
    funnel: &Arc<RowFunnel<MinImage>>,
    drain: &mut runtime::FunnelDrain<MinImage>,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
    emitted: &mut u64,
    stopped: &mut bool,
) -> PgResult<()> {
    loop {
        match drain.next() {
            DrainStep::Row(img) => {
                if *stopped {
                    drop(img);
                    continue;
                }
                let cont = emit_row(img)?;
                *emitted += 1;
                if !cont {
                    *stopped = true;
                    funnel.close_demand();
                }
            }
            DrainStep::Idle | DrainStep::Eof => break,
        }
    }
    Ok(())
}

/// The engaged wait/pump loop against ONE board entry. Every exit path
/// closes the entry and joins claimed participants (`close_and_await`);
/// error exits chase the task first (see `chase_and_drain_stmt`).
#[allow(clippy::too_many_arguments)]
fn wait_pump(
    payload: &Arc<StmtTaskShared>,
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    entry: &Arc<parallel::standing::StandingEngagement>,
    channel: &str,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
    emitted: &mut u64,
    stopped: &mut bool,
) -> PgResult<StmtWait> {
    let take_slot = || {
        payload.standing.lock().unwrap_or_else(|p| p.into_inner()).take();
    };
    let mut drain = funnel.drain();
    let t0 = pg_clock::MonoStamp::now();
    loop {
        // Waiter-flag pattern, latch form: ARM, then pump — a push ordered
        // after the arm sets the leader latch (wake hook), one before it is
        // drained by this pump.
        funnel.arm_drain_wait();
        if let Err(e) = pump_stmt(funnel, &mut drain, emit_row, emitted, stopped) {
            // Wire receiver error (client gone mid-stream): chase + join.
            parallel::standing::close_no_wait(entry);
            chase_and_drain_stmt(payload, rt, funnel, rg);
            chase_until_joined(payload, entry);
            take_slot();
            parallel::standing::close_and_await(entry);
            // Interrupt/error-exit witness (the completion path prints
            // "engaged"; chased statements never reach it).
            lane_trace(&format!("stmt-task: chased {channel} dop={}", entry.claimed()));
            return Err(e);
        }
        if let Some(o) = waiter.try_wait() {
            take_slot();
            parallel::standing::close_and_await(entry);
            lane_trace(&format!("stmt-task: engaged {channel} dop={}", entry.claimed()));
            return Ok(StmtWait::Done(o));
        }
        // Session-thread interrupts (pg_cancel_backend, statement_timeout,
        // client-loss disposition): the statement's cancel identity is the
        // LEADER's error — chase the task, join, propagate.
        if let Err(e) = ::postgres_seams::check_for_interrupts::call() {
            parallel::standing::close_no_wait(entry);
            chase_and_drain_stmt(payload, rt, funnel, rg);
            chase_until_joined(payload, entry);
            take_slot();
            parallel::standing::close_and_await(entry);
            // Interrupt/error-exit witness (the completion path prints
            // "engaged"; chased statements never reach it).
            lane_trace(&format!("stmt-task: chased {channel} dop={}", entry.claimed()));
            return Err(e);
        }
        let started = payload.started.load(Ordering::SeqCst);
        let refused = entry.refused() + payload.refused.load(Ordering::SeqCst);
        if started == 0 && refused >= entry.tickets() {
            lane_trace(&format!("stmt-task: {channel} refused ({refused} refusals)"));
            take_slot();
            parallel::standing::close_and_await(entry);
            return Ok(StmtWait::Fallback);
        }
        // Counter read order law (standing_channel): detached BEFORE claimed.
        let detached = entry.detached();
        let claimed_now = entry.claimed();
        if started == 0
            && detached >= claimed_now
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > stmt_claim_deadline()
        {
            lane_trace(&format!("stmt-task: {channel} claim deadline"));
            take_slot();
            parallel::standing::close_and_await(entry);
            return Ok(StmtWait::Fallback);
        }
        // Died needle (yield-kind split per the standing channel law).
        let yielded = entry.yielded();
        let terminal = detached.saturating_sub(yielded);
        if claimed_now > 0 && started > 0 && detached >= claimed_now && terminal > 0 {
            if let Some(o) = waiter.try_wait() {
                take_slot();
                parallel::standing::close_and_await(entry);
                lane_trace(&format!("stmt-task: engaged {channel} dop={}", entry.claimed()));
                return Ok(StmtWait::Done(o));
            }
            if let Some(e) = payload.take_error() {
                chase_and_drain_stmt(payload, rt, funnel, rg);
                take_slot();
                parallel::standing::close_and_await(entry);
                return Err(e);
            }
            parallel::standing::close_no_wait(entry);
            chase_and_drain_stmt(payload, rt, funnel, rg);
            chase_until_joined(payload, entry);
            take_slot();
            parallel::standing::close_and_await(entry);
            // Interrupt/error-exit witness (the completion path prints
            // "engaged"; chased statements never reach it).
            lane_trace(&format!("stmt-task: chased {channel} dop={}", entry.claimed()));
            return Err(Box::new(PgError::new(
                ERROR,
                "statement task worker died before completing the statement",
            )));
        }
        // Bounded leader park; an Err is a RAISED cancel disposition
        // delivered at the latch sleep (the F1 law) — same exit as CFI.
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            parallel::standing::close_no_wait(entry);
            chase_and_drain_stmt(payload, rt, funnel, rg);
            chase_until_joined(payload, entry);
            take_slot();
            parallel::standing::close_and_await(entry);
            // Interrupt/error-exit witness (the completion path prints
            // "engaged"; chased statements never reach it).
            lane_trace(&format!("stmt-task: chased {channel} dop={}", entry.claimed()));
            return Err(e);
        }
    }
}

/// Post-completion tail: pump the buffered remainder to EOF, then surface
/// the worker error / abort verdict, else complete with the row count.
fn finish_stmt(
    payload: &Arc<StmtTaskShared>,
    funnel: &Arc<RowFunnel<MinImage>>,
    outcome: runtime::RgOutcome,
    emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>,
    emitted: &mut u64,
    stopped: &mut bool,
) -> PgResult<StmtTaskOutcome> {
    // finalize marked every ring done; the join completed all producers —
    // this pump reaches EOF without parking.
    let mut drain = funnel.drain();
    pump_stmt(funnel, &mut drain, emit_row, emitted, stopped)?;
    if let Some(e) = payload.take_error() {
        // Mid-statement worker error: rows already streamed stay streamed
        // (stock framing); the ORIGINAL error re-raises here.
        return Err(e);
    }
    if outcome == runtime::RgOutcome::Aborted {
        // Aborted without a recorded error: surface the leader's own pending
        // interrupt if that is what aborted us, else the generic shape.
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "statement task aborted")));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        // Completed without any participant: an empty generation (should be
        // unreachable — the claim deadline catches it first). Fall back.
        debug_assert_eq!(*emitted, 0);
        return Ok(StmtTaskOutcome::Fallback);
    }
    Ok(StmtTaskOutcome::Completed(*emitted))
}

// ---------------------------------------------------------------------------
// The execute_plan hook (leader).
// ---------------------------------------------------------------------------

/// GL-STMTTASK-1 gated hook: when the armed simple-protocol statement's
/// top-level run reaches execute_plan and every envelope gate admits, run
/// the statement as a dop-1 pool task and stream its rows to `dest` — or
/// (GL-STMTTASK-2 change 3) hand the caller an inline seat so the SESSION
/// thread runs it itself through the incumbent loop. See
/// [`StmtTaskVerdict`].
pub(crate) fn try_stmt_task<'mcx, 'd>(
    estate: &mut ::executils::EStateData<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    number_tuples: u64,
    dest: &mut ::tcop_dest::DestReceiver<'d>,
) -> PgResult<StmtTaskVerdict> {
    // The armed flag is the first gate AND is consumed exactly once per
    // statement: the first executor run of the statement is the top-level
    // one, so nested runs (SQL functions under a refused top level) can
    // never inherit the arm.
    if !take_armed() {
        return Ok(StmtTaskVerdict::Incumbent);
    }
    if number_tuples != 0 {
        return refuse(StmtTaskRefusal::CountLimited);
    }
    if dest.mydest() != ::types_dest::CommandDest::Remote {
        return refuse(StmtTaskRefusal::NotRemoteDest);
    }
    if estate.es_instrument != 0 {
        return refuse(StmtTaskRefusal::Instrumented);
    }
    if estate.es_epq_active
        || estate.es_cursor_run_budget.is_some()
        || estate.es_spi_run_budget.is_some()
        || estate.es_lane_cursor_parked
    {
        return refuse(StmtTaskRefusal::ExecutorCadence);
    }
    if parallel::IsParallelWorker() || ::xact::IsInParallelMode() {
        return refuse(StmtTaskRefusal::InParallel);
    }
    let Some(rt) = runtime::global() else {
        return refuse(StmtTaskRefusal::NoRuntime);
    };
    if !runtime::runtime_enabled() {
        return refuse(StmtTaskRefusal::NoRuntime);
    }
    let Some(pstmt_ref) = estate.es_plannedstmt else {
        return refuse(StmtTaskRefusal::PlanShape);
    };
    if pstmt_ref.commandType != CmdType::CMD_SELECT
        || pstmt_ref.hasModifyingCTE
        || pstmt_ref.utilityStmt.is_some()
        || !pstmt_ref.rowMarks.is_nil()
        || !pstmt_ref.resultRelations.is_nil()
    {
        return refuse(StmtTaskRefusal::PlanShape);
    }
    let Some(plan) = pstmt_ref.planTree.and_then(|n| n.as_plan()) else {
        return refuse(StmtTaskRefusal::PlanShape);
    };
    // The worker executes under the parallel-worker environment (the
    // binder's identity model): only plans the planner certified
    // parallel-safe keep bytewise execution parity there.
    if !plan.parallel_safe {
        return refuse(StmtTaskRefusal::NotParallelSafe);
    }
    // Binder policy: shapes validate() would refuse must not publish.
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return refuse(StmtTaskRefusal::BinderPolicy);
    }
    debug_assert!(::snapmgr::ActiveSnapshotSet(), "portal run without an active snapshot");

    // GL-STMTTASK-2 change 3 — INLINE-EXECUTE (run-on-arrival, the survey's
    // Cilk work-first / Go doctrine): if a pool seat is free WITHOUT a
    // wait, the session thread executes the statement ITSELF — the caller
    // runs its own (incumbent) per-tuple loop with the seat held. Idle
    // case: zero submissions, zero wakes, zero binds — ceremony is this
    // gate + the permit try-acquire. Under contention (no free seat) the
    // enqueue path below takes over; the session thread then parks and a
    // worker serves the statement when capacity frees.
    if stmt_inline_enabled() {
        if let Some(seat) = rt.try_borrow_seat() {
            STMT_ENGAGED.fetch_add(1, Ordering::SeqCst);
            STMT_INLINE.fetch_add(1, Ordering::SeqCst);
            // Engagement witness (e2e grep surface) + the wakeup program's
            // per-statement unpark metric: inline performs none.
            lane_trace("stmt-task: engaged inline dop=1 unparks=0");
            return Ok(StmtTaskVerdict::Inline(seat));
        }
    }

    // Wire descriptor: the JUNK-CLEAN result type — what the receiver's
    // startup (RowDescription) was primed with and what the worker's
    // junk-filtered emits carry. A raw plan descriptor here would disagree
    // with both on ORDER-BY-junk shapes (natts mismatch at printtup).
    let desc = match &estate.es_junkFilter {
        Some(jf) => jf.jf_cleanTupType.clone(),
        None => planstate.exec_get_result_type(plan)?,
    };
    let pstmt: *const PlannedStmt<'static> =
        pstmt_ref as *const PlannedStmt<'mcx> as *const PlannedStmt<'static>;
    let query_text = estate.es_sourceText.unwrap_or("");
    let eflags = estate.es_top_eflags;
    let wire_mcx = estate.es_query_cxt;

    // Wire slot: a Minimal slot carrying the result descriptor; the pump
    // stores each image into it and hands it to the REAL receiver.
    let mut wire_slot = ::exectuples::make_tuple_table_slot(
        wire_mcx,
        ::types_slot::TupleSlotKind::MinimalTuple,
        Some(desc),
    );

    STMT_ENGAGED.fetch_add(1, Ordering::SeqCst);
    // The wakeup program's unparks-per-statement metric (survey rule 6):
    // delta of the pool park lot's unpark counter across this statement's
    // whole ceremony. Process-wide (concurrent statements' wakes alias into
    // each other's deltas) — exact in the single-stream measurement rigs.
    let unparks0 = rt.pool_unparks();
    let outcome = engage_stmt_task(
        rt,
        pstmt,
        query_text,
        eflags,
        &mut |img: MinImage| -> PgResult<bool> {
            // SAFETY: `wire_slot` is a Minimal slot; `img` owns the bytes
            // and outlives this store+receive (dropped after).
            unsafe {
                ::exectuples::exec_store_minimal_tuple_ptr(&mut wire_slot, wire_mcx, img.as_mtup_ptr());
            }
            // Lifetime bridge at the dest seam (the funnel emit_row
            // precedent): the receiver only copies datums out during the
            // call and retains no borrow.
            let slot: &mut ::types_slot::SlotData<'d> = unsafe {
                &mut *(&mut wire_slot as *mut ::types_slot::SlotData<'mcx>)
                    .cast::<::types_slot::SlotData<'d>>()
            };
            let cont = dest.receive_slot(slot)?;
            ::exectuples::exec_clear_tuple(&mut wire_slot, wire_mcx);
            drop(img);
            Ok(cont)
        },
    )?;

    match outcome {
        StmtTaskOutcome::Completed(n) => {
            STMT_COMPLETED.fetch_add(1, Ordering::SeqCst);
            if lane_trace_enabled() {
                lane_trace(&format!(
                    "stmt-task: unparks={}",
                    rt.pool_unparks().saturating_sub(unparks0)
                ));
            }
            // Stock accounting: es_processed counts rows the receiver
            // accepted (the plain loop's SELECT arm).
            estate.es_processed = n;
            Ok(StmtTaskVerdict::Handled)
        }
        StmtTaskOutcome::Fallback => refuse(StmtTaskRefusal::NoChannel),
        StmtTaskOutcome::CeremonyRefused => refuse(StmtTaskRefusal::CeremonyRefused),
    }
}

// ---------------------------------------------------------------------------
// Unit pins.
// ---------------------------------------------------------------------------

// Knob/arm unit pins live with the arm (postgres_seams::stmt_task_arm).
