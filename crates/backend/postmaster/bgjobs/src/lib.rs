//! M4 background-job dispatcher (docs/design/m4-bgjobs.md §3.2/§3.3):
//! periodic daemons expressed as Maintenance-class ResourceGroups on the
//! morsel runtime instead of dedicated always-alive threads.
//!
//! One process-lifetime dispatcher thread owns every migrated job's
//! deadline and wake edge:
//! - DEADLINES: a min-scan over idle jobs' due times bounds a
//!   `waiter::park_timeout`; on expiry the job's CYCLE RG (one task set,
//!   one morsel = one daemon loop iteration) is submitted via
//!   `Runtime::submit_maintenance` (the §3.5 pick preference guarantees a
//!   start within ~one task boundary while the pool is live).
//! - LATCH WAKES: while a job is idle the dispatcher publishes ITS OWN
//!   waker handle into the job latch's `waker` word under the owner-at-
//!   wait-entry Dekker protocol (waker store → maybe_sleeping → is_set
//!   recheck — exactly waiteventset's wait_loop arm). Every existing
//!   `SetLatch` caller (StrategyNotifyBgWriter, RequestCheckpoint,
//!   async-commit wakes, thread-signal delivery) then wakes the dispatcher
//!   with NO caller change; the dispatcher converts the set latch into an
//!   immediate cycle submission. Handles are token-validated — a stale
//!   publication is a structural no-op, not a wedge.
//! - SINGLE-FLIGHT: a job never has two cycles in flight; the next
//!   deadline comes from the finished cycle's [`CycleOutcome`].
//! - WATCHDOG: an in-flight cycle that has not STARTED within
//!   `PGRUST_RUNTIME_BGJOBS_WATCHDOG_MS` (default 10s) draws a one-shot
//!   LOG self-report (the stall.rs discipline). Deliberately log-only in
//!   M4: only advisory-deadline daemons migrate (§4 disposition table).
//!
//! KILL SWITCH: `PGRUST_RUNTIME_BGJOBS=1` (default OFF), meaningful only
//! with `PGRUST_RUNTIME=1` (the pool). With the flag off this crate is
//! dead code in every production path — the increment gate is zero
//! behavior change.
//!
//! What this increment deliberately is NOT (yet): no daemon is wired (the
//! bgwriter migration is increments 3-4 — TLS extraction, then the
//! virtual-child postmaster integration + the job envelope bind). Cycle
//! bodies here run UNBOUND on pool workers; real daemon cycles run under
//! the job envelope bind, which owns ResetLatch and signal drains.

use std::sync::atomic::{AtomicU64, Ordering};
// pgsync by crate law (permit-s5, riding the dispatcher spawn door): the
// jobs registry is locked by the REGISTERED dispatcher thread and by
// registering backends — a raw std lock here would be the
// permit-holder-blocks-raw watchdog wedge shape under the scheduler (the
// s2 AVAILABLE-registry precedent). Native arm = identical std re-exports.
use std::sync::Arc;

use pgsync::{Mutex, OnceLock};
use std::time::Duration;

// DST P2 (contract §1.3): dispatcher deadlines in pg_clock's mono domain.
use pg_clock::{Deadline, MonoStamp};

use runtime::{MorselRange, QuerySpec, RgHandle, RgOutcome, Runtime, TaskSetSpec, TaskSetWork};
use types_storage::latch::Latch;

#[cfg(test)]
mod tests;

/// Kill switch. Read once; requires the runtime pool to be running (the
/// caller — launch_backend::rtpool — only starts the dispatcher after the
/// pool spawned).
pub fn bgjobs_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PGRUST_RUNTIME_BGJOBS").is_ok_and(|v| v == "1"))
}

fn watchdog_ms() -> u64 {
    static MS: OnceLock<u64> = OnceLock::new();
    *MS.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_BGJOBS_WATCHDOG_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000)
    })
}

/// Why a cycle was dispatched — the job body's analog of "WaitLatch
/// returned WL_TIMEOUT vs WL_LATCH_SET" (plus the first run).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CycleReason {
    Startup,
    Deadline,
    Wake,
}

/// What the finished cycle wants next. Errors/backoff are the job body's
/// business (the daemons' uniform abort backoff is `Sleep(1s)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Re-arm the deadline at now + duration. The job stays wakeable early
    /// through its latch the whole time.
    Sleep(Duration),
    /// Deregister the job (shutdown leg). Teardown/exit-announce is the
    /// registrant's business (increment 4 runs it bound-as-job before
    /// returning Exit).
    Exit,
}

/// Control-plane verdict from [`BgJob::control`], evaluated ON THE
/// DISPATCHER THREAD while the job is idle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Control {
    Continue,
    /// Clean shutdown: the dispatcher runs [`BgJob::teardown`] and retires
    /// the job.
    Exit,
    /// Crash leg: retire WITHOUT teardown (shared memory is being reset
    /// wholesale — the wpool abandon-not-teardown discipline). The job's
    /// control() does its own crash announce before returning this.
    Abandon,
}

/// One migrated daemon. Implementations run their C loop body ONCE per
/// `run_cycle` call (ResetLatch → drain/interrupt legs → body → stats
/// flush) and return the deadline the C loop tail would have passed to
/// WaitLatch.
///
/// THREAD CONTRACT: `startup`/`control`/`teardown` run on the DISPATCHER
/// thread — the job's stable home for identity TLS, signal drains, and
/// config reloads (docs/design/m4-bgjobs.md §3.2). `run_cycle` runs on an
/// arbitrary pool worker; the implementation binds its envelope there.
pub trait BgJob: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// The job's persistent proc latch — its wake edge. None = pure
    /// deadline job (synthetic tests). The latch's OWNER stays the job
    /// identity; the dispatcher only borrows its waker word while the job
    /// is idle.
    fn latch(&self) -> Option<&'static Latch>;

    /// Identity acquisition (the daemon main's prelude), once, before the
    /// first cycle. Err retires the job immediately — the implementation
    /// owns its failure announce.
    fn startup(&self) -> Result<(), Box<types_error::PgError>> {
        Ok(())
    }

    /// Signal/reload/shutdown processing, every dispatcher pass while the
    /// job is idle, BEFORE wake/deadline evaluation.
    fn control(&self) -> Control {
        Control::Continue
    }

    /// Clean-exit teardown (identity release + exit announce), after
    /// [`Control::Exit`] or [`CycleOutcome::Exit`].
    fn teardown(&self) {}

    /// A job hook or cycle body PANICKED (contained by the dispatcher /
    /// cycle rim). C parity: a daemon thread panic is a child crash — the
    /// implementation announces a WTERMSIG-shaped exit so the postmaster
    /// runs its ordinary crash handling instead of wedging shutdown on a
    /// child that never exits. The job is retired without teardown.
    fn crashed(&self) {}

    /// One daemon cycle, executed on a pool worker under a Maintenance RG.
    fn run_cycle(&self, reason: CycleReason) -> CycleOutcome;
}

/// Job-RG query-id namespace: "BGJ" tag in the high bits + job index.
/// Purely diagnostic (trace lines, stats).
const QUERY_ID_TAG: u64 = 0x4247_4A00_0000_0000;

pub type JobId = usize;

enum Phase {
    /// Waiting for `due` (or a latch wake).
    Idle { due: Deadline },
    /// Cycle RG submitted; single-flight until its completion is seen.
    InFlight {
        submitted: MonoStamp,
        handle: RgHandle,
        waiter: runtime::CompletionWaiter,
        outcome: Arc<Mutex<Option<CycleOutcome>>>,
        stall_logged: bool,
    },
    /// CycleOutcome::Exit processed; slot retired.
    Exited,
}

struct JobEntry {
    job: Arc<dyn BgJob>,
    phase: Phase,
    /// True until the first cycle dispatches — maps the first
    /// deadline-shaped dispatch to [`CycleReason::Startup`].
    never_ran: bool,
    /// Sticky manual wake (poke): survives arriving while a cycle is in
    /// flight — consumed by the next Idle evaluation. (The latch edge needs
    /// no analog: the latch word itself is sticky until the cycle resets
    /// it.)
    wake_pending: bool,
}

struct Shared {
    jobs: Mutex<Vec<JobEntry>>,
    /// The dispatcher thread's packed waiter handle (0 until published).
    /// Producers (cycle finalize, register, poke, and every SetLatch on an
    /// idle job's latch via the published latch waker) unpark it; pending
    /// unparks latch in the waiter slot, so a wake between predicate check
    /// and park is never lost.
    waker: AtomicU64,
    rt: Arc<Runtime>,
}

pub struct Dispatcher {
    shared: Arc<Shared>,
}

static DISPATCHER: OnceLock<Dispatcher> = OnceLock::new();

/// Start the dispatcher iff `PGRUST_RUNTIME_BGJOBS=1`. Called by
/// launch_backend (rtpool glue / job launch); postmaster thread only.
/// Idempotent. `spawner` wraps the dispatcher thread's spawn so the caller
/// can apply the child prelude (fork-inherited globals) — the dispatcher
/// hosts job identity init and config reloads, which need them.
pub fn start_if_enabled(
    rt: &Arc<Runtime>,
    spawner: fn(Box<dyn FnOnce() + Send>) -> std::io::Result<std::thread::JoinHandle<()>>,
) -> Option<&'static Dispatcher> {
    if !bgjobs_enabled() {
        return None;
    }
    Some(DISPATCHER.get_or_init(|| Dispatcher::spawn_with(Arc::clone(rt), spawner)))
}

/// The process dispatcher, if [`start_if_enabled`] started it.
pub fn get() -> Option<&'static Dispatcher> {
    DISPATCHER.get()
}

impl Dispatcher {
    /// Spawn a dispatcher over `rt` with a caller-supplied thread spawner.
    /// Process-lifetime in production; tests may spawn private instances
    /// through [`Dispatcher::spawn`].
    pub fn spawn_with(
        rt: Arc<Runtime>,
        spawner: fn(Box<dyn FnOnce() + Send>) -> std::io::Result<std::thread::JoinHandle<()>>,
    ) -> Dispatcher {
        let shared = Arc::new(Shared {
            jobs: Mutex::new(Vec::new()),
            waker: AtomicU64::new(0),
            rt,
        });
        let thread_shared = Arc::clone(&shared);
        spawner(Box::new(move || dispatcher_loop(thread_shared)))
            .expect("could not spawn bgjobs dispatcher thread");
        Dispatcher { shared }
    }

    /// Bare-thread spawn (tests).
    pub fn spawn(rt: Arc<Runtime>) -> Dispatcher {
        Dispatcher::spawn_with(rt, |body| {
            std::thread::Builder::new().name("pg-bgjobs-dispatcher".into()).spawn(body)
        })
    }

    /// Register a job; its first cycle is submitted immediately with
    /// [`CycleReason::Startup`] (the daemons' thread mains run their first
    /// body before their first WaitLatch, same order).
    pub fn register(&self, job: Arc<dyn BgJob>) -> JobId {
        let id = {
            let mut jobs = self.shared.jobs.lock().unwrap();
            jobs.push(JobEntry {
                job,
                // Due immediately; the dispatcher submits the Startup cycle.
                phase: Phase::Idle { due: Deadline::after(Duration::ZERO) },
                never_ran: true,
                wake_pending: false,
            });
            jobs.len() - 1
        };
        self.shared.unpark_dispatcher();
        id
    }

    /// Manual wake (tests; the production edge is the job's latch). Due
    /// time collapses to now.
    pub fn poke(&self, id: JobId) {
        {
            let mut jobs = self.shared.jobs.lock().unwrap();
            if let Some(e) = jobs.get_mut(id) {
                e.wake_pending = true;
            }
        }
        self.shared.unpark_dispatcher();
    }

    /// True ⇔ the job slot is retired (CycleOutcome::Exit processed).
    pub fn is_exited(&self, id: JobId) -> bool {
        matches!(self.shared.jobs.lock().unwrap()[id].phase, Phase::Exited)
    }
}

impl Shared {
    fn unpark_dispatcher(&self) {
        let w = self.waker.load(Ordering::Acquire);
        if w != 0 {
            let _ = waiter::unpark_word(w);
        }
    }
}

/// The cycle RG's work body: exactly one morsel (single-granule source)
/// runs the job cycle; finalize posts the outcome and wakes the
/// dispatcher. On ABORT the runtime skips finalize — the dispatcher
/// notices through the completion waiter on its next pass (bounded by the
/// park cadence), which is the crash/shutdown leg's drain path.
struct CycleWork {
    shared: Arc<Shared>,
    job: Arc<dyn BgJob>,
    reason: CycleReason,
    outcome: Arc<Mutex<Option<CycleOutcome>>>,
}

impl TaskSetWork for CycleWork {
    fn run_morsel(&self, _worker: usize, range: MorselRange) {
        debug_assert_eq!(range, 0..1, "cycle task sets are single-morsel");
        // Panic rim: a cycle-body panic must never poison the pool worker.
        // The empty outcome slot is the panic signal; the dispatcher's
        // harvest converts it into the job-crash leg (crashed() announce).
        let job = Arc::clone(&self.job);
        let reason = self.reason;
        if let Ok(outcome) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || job.run_cycle(reason)))
        {
            *self.outcome.lock().unwrap() = Some(outcome);
        }
    }

    fn finalize(&self) {
        // Deliberately empty: the dispatcher's wake rides the RG COMPLETION
        // (CompletionWaiter::register_waker_word at submit). finalize runs
        // BEFORE completion is posted in the runtime's last-out protocol, so
        // an unpark here can be consumed by a pass that still observes
        // try_wait()==None — the lost-wake this replaced (found by the
        // deadline-cycles flake, 2026-07-16).
    }
}

fn submit_cycle(shared: &Arc<Shared>, id: JobId, job: Arc<dyn BgJob>, reason: CycleReason) -> Phase {
    let outcome = Arc::new(Mutex::new(None));
    let work = Arc::new(CycleWork {
        shared: Arc::clone(shared),
        job,
        reason,
        outcome: Arc::clone(&outcome),
    });
    let (handle, waiter) = shared.rt.submit_maintenance(QuerySpec {
        query_id: QUERY_ID_TAG | id as u64,
        tasksets: vec![TaskSetSpec {
            source: Arc::new(runtime::SyntheticMorselSource::new(1)),
            work,
            deps: vec![],
        }],
    });
    // Completion wake: registered on the RG's completion word (posted AFTER
    // finalize in last-out — registering there is the only race-free edge).
    // Already-complete => no wake will follow; self-unpark so the next park
    // falls through to the harvest.
    // cfg(loom): register_waker_word does not exist on the loom arm (nothing
    // parks a leader in the models — runtime rg.rs convention); the
    // dispatcher's recheck cadence covers the harvest there.
    #[cfg(not(loom))]
    if waiter.register_waker_word(shared.waker.load(Ordering::Acquire)) {
        shared.unpark_dispatcher();
    }
    Phase::InFlight {
        submitted: MonoStamp::now(),
        handle,
        waiter,
        outcome,
        stall_logged: false,
    }
}

/// One dispatcher pass: harvest finished cycles, dispatch due/woken jobs,
/// run the watchdog. Returns the park bound (None = nothing scheduled,
/// untimed park with recheck cadence).
fn dispatcher_pass(shared: &Arc<Shared>) -> Option<Duration> {
    let now = MonoStamp::now();
    let mut nearest: Option<Duration> = None;
    let wd = Duration::from_millis(watchdog_ms());

    let mut jobs_teardown: Vec<JobId> = Vec::new();
    let mut jobs = shared.jobs.lock().unwrap();
    for id in 0..jobs.len() {
        // Harvest a finished in-flight cycle (immutable borrows only; the
        // phase swap happens after the borrow ends).
        let harvested: Option<Phase> = match &jobs[id].phase {
            Phase::InFlight { waiter, outcome, .. } => waiter.try_wait().map(|rg_outcome| {
                let cycle_outcome = outcome.lock().unwrap().take();
                match (rg_outcome, cycle_outcome) {
                    (RgOutcome::Completed, Some(CycleOutcome::Sleep(d))) => {
                        Phase::Idle { due: now.deadline_after(d) }
                    }
                    (RgOutcome::Completed, Some(CycleOutcome::Exit)) => {
                        jobs_teardown.push(id);
                        Phase::Exited
                    }
                    (RgOutcome::Completed, None) => {
                        // The cycle body panicked on the pool worker (the
                        // rim in CycleWork::run_morsel swallowed it; the
                        // outcome slot stayed empty). C parity: daemon
                        // panic = child crash.
                        let job = Arc::clone(&jobs[id].job);
                        elog_report(&format!(
                            "bgjobs: job \"{}\" cycle panicked; crash-retiring it",
                            job.name()
                        ));
                        let _ = contain(job.as_ref(), "crashed", || job.crashed());
                        Phase::Exited
                    }
                    (RgOutcome::Aborted, _) => {
                        // Crash/shutdown drain (increment 4 wires the
                        // explicit legs): the job stops rescheduling.
                        elog_report(&format!(
                            "bgjobs: job \"{}\" cycle aborted; retiring it",
                            jobs[id].job.name()
                        ));
                        Phase::Exited
                    }
                }
            }),
            _ => None,
        };
        if let Some(p) = harvested {
            jobs[id].phase = p;
            if jobs_teardown.last() == Some(&id) {
                // CycleOutcome::Exit: clean teardown on the dispatcher.
                let job = Arc::clone(&jobs[id].job);
                let _ = contain(job.as_ref(), "teardown", || job.teardown());
                jobs_teardown.pop();
                continue;
            }
        }

        // Decide, with immutable borrows; apply after.
        enum Act {
            Dispatch(CycleReason),
            IdleFor(Duration),
            InFlightTick { stall: bool },
            None,
        }
        let act = match &jobs[id].phase {
            Phase::Idle { due } => {
                let job = &jobs[id].job;
                // Control plane first (dispatcher thread; §3.2): signal
                // drain, reload, shutdown. Runs under the jobs lock —
                // acceptable: the only other lockers are register/poke.
                // Panic containment: a hook panic is a job CRASH (child-
                // crash announce via crashed()), never dispatcher death —
                // an unannounced virtual child wedges shutdown.
                match contain(job.as_ref(), "control", || job.control()) {
                    Some(Control::Continue) => {}
                    Some(Control::Exit) => {
                        let _ = contain(job.as_ref(), "teardown", || job.teardown());
                        jobs[id].phase = Phase::Exited;
                        continue;
                    }
                    Some(Control::Abandon) => {
                        jobs[id].phase = Phase::Exited;
                        continue;
                    }
                    None => {
                        jobs[id].phase = Phase::Exited;
                        continue;
                    }
                }
                // Latch wake: an is_set latch dispatches NOW.
                if job.latch().is_some_and(|l| l.is_set()) {
                    Act::Dispatch(CycleReason::Wake)
                } else if due.as_ns() <= now.as_ns() || jobs[id].wake_pending {
                    Act::Dispatch(CycleReason::Deadline)
                } else {
                    // Stay idle: (re-)publish our waker as the latch's wake
                    // route (owner-at-wait-entry Dekker arm: waker store →
                    // maybe_sleeping → is_set recheck). Publishing every
                    // pass is deliberate — handles are cheap and
                    // republication heals token reissue.
                    if let Some(l) = job.latch() {
                        l.waker.store(shared.waker.load(Ordering::Acquire), Ordering::Release);
                        l.set_maybe_sleeping(true);
                        if l.is_set() {
                            Act::Dispatch(CycleReason::Wake)
                        } else {
                            Act::IdleFor(Duration::from_nanos(due.as_ns().saturating_sub(now.as_ns())))
                        }
                    } else {
                        Act::IdleFor(Duration::from_nanos(due.as_ns().saturating_sub(now.as_ns())))
                    }
                }
            }
            Phase::InFlight { submitted, handle, stall_logged, .. } => Act::InFlightTick {
                stall: !*stall_logged
                    && Duration::from_nanos(now.since_ns(*submitted)) >= wd
                    && handle.stats().tasks_claimed == 0,
            },
            Phase::Exited => Act::None,
        };

        match act {
            Act::Dispatch(reason) => {
                let job = Arc::clone(&jobs[id].job);
                // Retract the idle publication before the cycle runs: the
                // bound cycle prologue ResetLatches, which asserts
                // maybe_sleeping is off (the owner-wait exit protocol). A
                // Startup cycle's reason (register()) rides Deadline here;
                // reason mapping to Startup happens below.
                if let Some(l) = job.latch() {
                    l.set_maybe_sleeping(false);
                }
                let reason = if jobs[id].never_ran { CycleReason::Startup } else { reason };
                if jobs[id].never_ran {
                    // Identity acquisition, once, on the dispatcher (§3.2).
                    match contain(job.as_ref(), "startup", || job.startup()) {
                        Some(Ok(())) => {}
                        Some(Err(e)) => {
                            elog_report(&format!(
                                "bgjobs: job \"{}\" startup failed: {}; retiring it",
                                job.name(),
                                e.message()
                            ));
                            jobs[id].phase = Phase::Exited;
                            continue;
                        }
                        None => {
                            jobs[id].phase = Phase::Exited;
                            continue;
                        }
                    }
                }
                jobs[id].never_ran = false;
                jobs[id].wake_pending = false;
                jobs[id].phase = submit_cycle(shared, id, job, reason);
                // In-flight: watchdog bounds the park.
                nearest = Some(nearest.map_or(wd, |n| n.min(wd)));
            }
            Act::IdleFor(dt) => {
                nearest = Some(nearest.map_or(dt, |n| n.min(dt)));
            }
            Act::InFlightTick { stall } => {
                if stall {
                    let name = jobs[id].job.name();
                    if let Phase::InFlight { stall_logged, submitted, .. } =
                        &mut jobs[id].phase
                    {
                        *stall_logged = true;
                        let waited = now.since_ns(*submitted) / 1_000_000;
                        elog_report(&format!(
                            "bgjobs: job \"{name}\" cycle not started {waited}ms after \
                             submission (pool saturated or wedged); deadline class is \
                             advisory in M4"
                        ));
                    }
                }
                nearest = Some(nearest.map_or(wd, |n| n.min(wd)));
            }
            Act::None => {}
        }
    }
    nearest
}

fn dispatcher_loop(shared: Arc<Shared>) {
    // Publish our waker BEFORE the first pass: producers that saw 0 unpark
    // nothing, but they only exist after register() returns, which unparks
    // through this word — and a pending unpark latches.
    shared.waker.store(waiter::current_handle().as_u64(), Ordering::Release);
    loop {
        let bound = dispatcher_pass(&shared);
        // A wake that landed during the pass is latched in the waiter slot;
        // this park returns immediately and re-passes.
        match bound {
            Some(d) => {
                let _ = waiter::park_timeout(d);
            }
            None => {
                let _ = waiter::park();
            }
        }
    }
}

/// Run a job hook with panic containment (dispatcher thread). A panicking
/// hook yields None; the caller crash-retires the job via crashed(). The
/// dispatcher itself must never die of a job's panic — an unannounced
/// virtual child wedges the postmaster's shutdown legs.
fn contain<R>(job: &dyn BgJob, hook: &str, f: impl FnOnce() -> R) -> Option<R> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => Some(r),
        Err(_) => {
            elog_report(&format!(
                "bgjobs: job \"{}\" {hook} panicked; crash-retiring it",
                job.name()
            ));
            if hook != "crashed" && hook != "teardown" {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.crashed()));
            }
            None
        }
    }
}

/// LOG-level self-report. elog is the production channel; fall back to
/// stderr if the error infrastructure is not up on this thread (tests).
fn elog_report(msg: &str) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = elog::elog(types_error::LOG, msg.to_string());
    }))
    .is_err()
    {
        eprintln!("[bgjobs] {msg}");
    }
}
