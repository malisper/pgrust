//! M2 POOL BINDING — standing runtime executors (parallelism redesign
//! §2.3; notes/m2-pool-binding.md).
//!
//! The M1 runtime-scan arm launches wpool helpers PER ENGAGEMENT and pays
//! full worker launch + a double bind (parallel_worker_body's init for a
//! trivial entry task, then the query-task binder's re-bind at
//! POST_TASK_PARK) — the ~8ms fixed cost M1-a attributed. This module makes
//! the helpers STANDING: a process-lifetime gang of executor threads with
//! bgworker-shaped identity (PGPROC from a boot-reserved segment — see
//! `postinit::InitializeMaxBackends` — so the gang never consumes the
//! bgworker registry, parallel-class, or postmaster-child-slot budgets the
//! legacy arms measure against), DB-PINNED on first use (tech-debt TD-1:
//! bind/unbind is cheap WITHIN a database; cross-db engagements refuse
//! fail-closed and the leader falls back to the launched path).
//!
//! Per engagement a standing worker pays exactly: one condvar wake, one
//! lock-group join, ONE query-task-binder bind (GUC transfer = the query
//! pin's single Arc apply, which also adopts the leader's CURRENT base —
//! so a boot-captured gang GUC base can never leak stale reload state into
//! a query), the driver's executor build + pinned drive, one unbind, one
//! lock-group leave. No thread launch, no entry task, no double bind, no
//! worker-exit join.
//!
//! Parking discipline (wpool precedent, launch_backend wpool docs): workers
//! park BETWEEN engagements on a plain process-local Condvar — never on
//! shared-memory latches — so crash reinit can invalidate them with an
//! epoch bump without woken threads touching reset shared memory
//! (flush_for_crash discipline). DROP DATABASE rides the same
//! `parallel_pool_retire_db` seam wpool uses: gang workers pinned to the
//! dropped database exit (ProcKill returns the PGPROC, RemoveProcFromArray
//! clears the procarray entry CountOtherDBBackends polls).
//!
//! Thread identity is the launch_backend spawn glue's (rtgang): postmaster
//! prelude + InitPostmasterChild + InitProcess(BgWorker, boot-reserved
//! segment) + BaseInit + the synthetic bgworker entry, then
//! `gang_worker_loop` here; the glue also owns the run_child_task-shaped
//! ProcExitThread catch + deferred-callback drain (ProcKill) at exit.
//!
//! Kill-switch layering: the gang exists only under PGRUST_RUNTIME=1 (the
//! reserved PGPROC segment too), engages only for published engagements
//! (the M1 arm's own arming gates those), and PGRUST_RUNTIME_POOLBIND=0
//! disables this module entirely (leader falls back to the launched path).
//!
//! M2 inc-1: ALL runtime SQL arms use this channel — the per-arm driver
//! rides each engagement's ParallelShared (`StandingDriver`,
//! `set_standing_driver`) instead of a process-global slot; the sink arms
//! carry their own layered kill (PGRUST_RUNTIME_POOLBIND_SINKS=0, gated at
//! their call sites in execmain's standing_channel).

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use pgsync::{Condvar, Mutex, OnceLock};

use types_core::{InvalidOid, Oid};
use types_error::WARNING;

use super::ParallelShared;

/// One published engagement: `tickets` participation slots over one
/// ParallelShared. Workers claim tickets, run the registered driver bound,
/// and detach; the leader closes the board entry and waits for
/// detached == claimed before its executor arena may unwind (the SendConst
/// contract's join, replacing DestroyParallelContext's worker-exit wait).
pub struct StandingEngagement {
    shared: Arc<ParallelShared>,
    tickets: usize,
    claimed: AtomicUsize,
    detached: AtomicUsize,
    /// Pre-driver refusals (db mismatch, leader already gone, connect
    /// failure): the worker never reached the arm's payload accounting, so
    /// the board carries them for the leader's nobody-participates check.
    refused: AtomicUsize,
    /// POOL-QOS yields: detaches that were VOLUNTARY morsel-boundary
    /// serve-yields (a demoted serve leaving to free its thread for a
    /// waiting interactive engagement) — never deaths. The leader's died
    /// needle subtracts these (yield-kind split): a board whose every
    /// detach is a yield is HEALTHY (capacity refills via the concurrent
    /// ticket cap below). Write order law: `yielded` bumps strictly BEFORE
    /// its detach lands (both inside yield_detach's lock), and the leader
    /// reads `detached` BEFORE `yielded`, so terminal = detached − yielded
    /// can only UNDER-count terminal detaches (waits instead of erroring —
    /// the safe direction; a real death stabilizes terminal > 0).
    yielded: AtomicUsize,
    /// Yield grants outstanding (granted but not yet yield_detach-settled),
    /// serialized by the gang mutex with the grant check — the atomic form
    /// only so the leader can read it without the lock if ever needed.
    yield_grants: AtomicUsize,
    closed: AtomicBool,
}

impl StandingEngagement {
    pub fn claimed(&self) -> usize {
        self.claimed.load(SeqCst)
    }
    pub fn detached(&self) -> usize {
        self.detached.load(SeqCst)
    }
    pub fn refused(&self) -> usize {
        self.refused.load(SeqCst)
    }
    pub fn yielded(&self) -> usize {
        self.yielded.load(SeqCst)
    }
    pub fn tickets(&self) -> usize {
        self.tickets
    }

    fn try_claim(&self) -> Option<usize> {
        if self.closed.load(SeqCst) {
            return None;
        }
        // Over-claims (fetch_add races, bounded by gang size) are returned.
        //
        // POOL-QOS: the cap is CONCURRENT participants (claims minus
        // detaches), not claims-ever — a voluntary serve-yield returns its
        // capacity, so a later worker can rejoin the engagement (the yield
        // would otherwise bleed the board's width permanently). `detached`
        // is read AFTER the claim fetch_add: every detach follows its
        // claim, so the concurrent count can only be OVER-estimated by the
        // race (an under-admission — refused claims settle and retry via
        // the pick; never an over-admission beyond the historical
        // over-claim class, which the settle below still returns).
        let t = self.claimed.fetch_add(1, SeqCst);
        let det = self.detached.load(SeqCst);
        if t - det.min(t) < self.tickets && !self.closed.load(SeqCst) {
            Some(t)
        } else {
            self.claimed.fetch_sub(1, SeqCst);
            // The leader's Condvar join (close_and_await) may have observed
            // the transient inflated `claimed` at its under-lock check and
            // parked on it; unlike the poll it replaced, a cv join is not
            // lost-wake-tolerant, so the settling decrement carries the same
            // lock-mediated wake as a detach. (Never runs under the gang
            // lock: try_claim is called after the wake scope releases it.)
            let (lock, cv) = gang();
            drop(pgsync::lock(lock));
            cv.notify_all();
            None
        }
    }

    /// POOL-QOS serve-yield grant: may THIS participant leave the
    /// engagement at a morsel boundary? Granted only while at least one
    /// OTHER active participant remains after every outstanding grant is
    /// spent — the last-active guard, serialized under the gang mutex so
    /// two concurrent yielders can never both drain the board (an
    /// all-yielded board would stall the query until a re-claim and trip
    /// the leader's death needles). A granted yield MUST proceed to
    /// [`StandingEngagement::yield_detach`] (no cancel path — the grant is
    /// spent either way).
    pub fn try_grant_yield(&self) -> bool {
        let (lock, _cv) = gang();
        let _g = pgsync::lock(lock);
        let claimed = self.claimed.load(SeqCst);
        let detached = self.detached.load(SeqCst);
        let grants = self.yield_grants.load(SeqCst);
        let active = claimed.saturating_sub(detached).saturating_sub(grants);
        if self.closed.load(SeqCst) || active < 2 {
            return false;
        }
        // Yield budget: at most 2×tickets voluntary leaves per engagement —
        // bounds the parked-lane inventory (yielded participants park their
        // external lanes until the RG completes so a rejoining serve can
        // never inherit a mid-accept per-lane slot) and the rebind churn.
        if self.yielded.load(SeqCst) + grants >= self.tickets.saturating_mul(2) {
            return false;
        }
        self.yield_grants.fetch_add(1, SeqCst);
        true
    }

    /// POOL-QOS yield detach: the granted yielder's OWN detach, performed
    /// under the gang mutex so the grant settle, the yield-kind count, and
    /// the detach land atomically against other grant checks. Marks the
    /// thread so the enclosing serve's DetachGuard (which fires on every
    /// serve exit) skips its own bump — exactly one detach per serve.
    pub fn yield_detach(&self) {
        {
            let (lock, _cv) = gang();
            let _g = pgsync::lock(lock);
            self.yielded.fetch_add(1, SeqCst);
            self.yield_grants.fetch_sub(1, SeqCst);
            self.detached.fetch_add(1, SeqCst);
            YIELD_DETACHED.with(|c| c.set(true));
        }
        // The detach wake, outside the lock (DetachGuard's exact shape).
        latch::SetLatch(types_storage::latch::LatchHandle::proc(
            self.shared.parallel_leader_proc_number,
        ));
        let (lock, cv) = gang();
        drop(pgsync::lock(lock));
        cv.notify_all();
    }
}

thread_local! {
    /// Set by yield_detach, consumed by the enclosing serve's DetachGuard
    /// drop (same thread by construction: the yield happens inside the
    /// driver, inside serve_ticket's frame).
    static YIELD_DETACHED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The board of the POOL serve currently running on this thread
    /// (pool_serve installs it around serve_ticket) — the arms' yield
    /// callback grants against it without any payload plumbing. None on
    /// gang serves (the gang board is one-at-a-time and pooldb0 is the
    /// letter's control arm — it must stay byte-identical).
    static CURRENT_SERVE_BOARD: std::cell::RefCell<Option<Arc<StandingEngagement>>> =
        const { std::cell::RefCell::new(None) };
    /// A yield grant was taken by the current serve's drive; the driver's
    /// enclosing serve_ticket settles it (yield_detach) after the driver's
    /// teardown fully returns — the arena-outlives-workers law: the leader
    /// join must not release before this thread is done with arena refs.
    static YIELD_PENDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// POOL-QOS: the arms' serve-yield grant callback body. Grants against the
/// CURRENT pool serve's board (last-active-guarded); a `true` return also
/// marks the yield pending so serve_ticket settles the detach after the
/// driver returns. False when not on a pool serve, board unavailable, or
/// the guard denies.
pub fn try_grant_yield_current() -> bool {
    CURRENT_SERVE_BOARD.with(|slot| {
        let slot = slot.borrow();
        let Some(board) = slot.as_ref() else { return false };
        if board.try_grant_yield() {
            YIELD_PENDING.with(|c| c.set(true));
            true
        } else {
            false
        }
    })
}

/// Everything a worker does after claiming a ticket happens under this
/// guard: detach is UNCONDITIONAL (error, panic, even a FATAL's
/// ProcExitThread unwind through the gang frame), so the leader's
/// detached==claimed join can never wedge on a dying worker. (abort() is
/// the only escape, and it takes the whole process.)
struct DetachGuard<'a> {
    entry: &'a StandingEngagement,
}

impl Drop for DetachGuard<'_> {
    fn drop(&mut self) {
        // POOL-QOS: a serve that left via yield_detach already detached
        // (under the gang lock, with the yield-kind count) — consume the
        // thread mark and skip the second bump. Same thread by
        // construction; the mark is set only between yield_detach and this
        // drop.
        if YIELD_DETACHED.with(|c| c.replace(false)) {
            return;
        }
        // The modeled detach wake (loom spec: runtime/tests/loom.rs
        // `standing_gang_detach_count_join_no_lost_wake`): bump `detached`
        // FIRST, then a lock-mediated notify — either the leader sees the
        // new count at its under-lock check in close_and_await, or it is
        // already parked when the notify fires. The leader latch set stays:
        // the lanev2 leader poll loops (runtime_scan et al.) park on the
        // leader latch between their own completion re-polls.
        self.entry.detached.fetch_add(1, SeqCst);
        latch::SetLatch(types_storage::latch::LatchHandle::proc(
            self.entry.shared.parallel_leader_proc_number,
        ));
        let (lock, cv) = gang();
        drop(pgsync::lock(lock));
        cv.notify_all();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum SlotState {
    /// Never spawned, or exited (retired/died); try_engage may respawn.
    Vacant,
    /// Thread launched; identity/db init happens on the thread.
    Live,
}

struct GangState {
    slots: Vec<SlotState>,
    current: Option<Arc<StandingEngagement>>,
    /// Crash-reinit fence: bumped (with a wake) before shared memory is
    /// reset; woken workers whose captured epoch mismatches exit RAW —
    /// no shared-memory touch (wpool flush_for_crash discipline).
    epoch: u64,
    /// DROP DATABASE rider: databases whose pinned workers must exit.
    /// A SET, never auto-cleared by workers — a one-shot flag could be
    /// consumed by the first matching worker while a second parked one
    /// misses it (wedging CountOtherDBBackends). Bounded by DROPs per
    /// process lifetime; try_engage prunes an entry when a leader engages
    /// from that database again (the oid exists again).
    retired_dbs: Vec<Oid>,
    retire_all: bool,
}

static GANG: OnceLock<(Mutex<GangState>, Condvar)> = OnceLock::new();
static SPAWNER: OnceLock<fn(usize) -> bool> = OnceLock::new();
static GANG_SIZE: OnceLock<usize> = OnceLock::new();

/// Postmaster shutdown fence (one-way): set when the shutdown state machine
/// stops backends. Parked workers exit CLEAN (their proc_exit drain releases
/// identity against still-live shared memory) and try_engage refuses — the
/// C invariant the shutdown checkpoint assumes is "no live children behind
/// the postmaster's count", and the gang is registry-invisible, so it must
/// sequence itself out. A separate static (not GangState) so a fence set
/// before the first engagement sticks without forcing the board to exist.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub fn shutting_down() -> bool {
    SHUTDOWN.load(SeqCst)
}

/// Postmaster shutdown (PM_STOP_BACKENDS / immediate shutdown): retire the
/// standing gang. One-way; wakes every parked worker. The spawn glue's
/// live-thread count (`launch_backend::rtgang`) is the postmaster's
/// quiescence witness — PM_WAIT_BACKENDS holds until it drains.
pub fn retire_for_shutdown() {
    SHUTDOWN.store(true, SeqCst);
    if GANG.get().is_none() {
        return;
    }
    let (lock, cv) = gang();
    drop(pgsync::lock(lock));
    cv.notify_all();
}

fn gang() -> &'static (Mutex<GangState>, Condvar) {
    GANG.get_or_init(|| {
        (
            Mutex::new(GangState {
                slots: Vec::new(),
                current: None,
                epoch: 0,
                retired_dbs: Vec::new(),
                retire_all: false,
            }),
            Condvar::new(),
        )
    })
}

/// PGRUST_RUNTIME_POOLBIND=0 kills this module (launched-path fallback).
pub fn pool_binding_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_POOLBIND").map_or(true, |v| v.trim() != "0")
    })
}

/// PGPROC-LEASING POOL WORKERS (M2 inc-2 keystone) — rtpool threads take
/// bgworker-shaped identity at spawn and serve per-RG engagement boards
/// through the runtime's bound-descriptor claim gate
/// (scratchpad/night/m2-pool-binding-scope.md §3 inc-2).
///
/// **DEFAULT ON since the GL-POOLDB-1 acceptance re-run**
/// (scratchpad/night/gl-pooldb-1-letter-spec.md §9: dop1 needle p50
/// −0.18% vs the ≤1% bar; needle-under-saturated-mixed-load 0.64× vs the
/// ≤2× bar — the letter's 4.4× small-stream tax INVERTED by the pool-qos
/// interactive tier; heavy-stream p99 123.5s → 10.3s; gang-churn witness
/// + units ALL green at the flip base). `PGRUST_RUNTIME_POOLDB=0|off`
/// restores the standing-gang-first posture byte-identically (the t35
/// flipped-kill law). Layered UNDER PGRUST_RUNTIME_POOLBIND (=0 kills
/// this module wholesale, pool channel included); the sticky
/// (PGRUST_RUNTIME_POOL_STICKY) and QoS (PGRUST_RUNTIME_POOL_QOS) tiers
/// layer under THIS switch and flip with it.
pub fn pooldb_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        pooldb_posture(std::env::var("PGRUST_RUNTIME_POOLDB").ok().as_deref())
    })
}

/// The flip's pure parse (unit-pinned): default ON; `0`/`off` kill; any
/// other value (including the historical arming spelling `1`) is ON.
fn pooldb_posture(v: Option<&str>) -> bool {
    !matches!(v.map(str::trim), Some("0") | Some("off"))
}

/// Boot wiring (launch_backend rtgang): the thread spawner and the gang
/// size (= the boot-reserved PGPROC count). Once; later calls ignored.
pub fn install_spawner(size: usize, f: fn(usize) -> bool) {
    let _ = SPAWNER.set(f);
    let _ = GANG_SIZE.set(size);
}

/// One arm's engagement driver, carried per engagement on its
/// ParallelShared (`super::set_standing_driver`) — the per-arm dispatch
/// that replaced the single process-global driver slot when the sink arms
/// joined the standing channel (M2 inc-1). `drive` runs ON the standing
/// worker, fully impersonated (worker number + lock group), and owns the
/// binder wrap + executor build + pinned drive + payload error routing.
#[derive(Clone, Copy)]
pub struct StandingDriver {
    pub drive: fn(&ParallelShared),
    /// True when the driver binds through the DEFERRED binder
    /// (DeferredQueryTaskBinding — the scan arm's ceremony-v2 lazy
    /// first-touch path): serve_ticket may then defer the procarray/
    /// ProcSignal visibility bracket to the first-touch bind. False = the
    /// driver binds EAGERLY (with_query_task_binding — the sink arms):
    /// visibility is re-established up front and any parked sticky
    /// retention is evicted first (the eager validate()'s envelope gate
    /// refuses over a live retained session bind).
    pub deferred_bind: bool,
}

pub fn gang_size() -> usize {
    GANG_SIZE.get().copied().unwrap_or(0)
}

/// Leader side: publish an engagement for `dop` standing participants.
/// None = the standing path is unavailable (kill switch, no boot wiring,
/// board busy, nothing spawnable) — the caller falls back to the launched
/// path. The returned entry is LIVE: workers may already be claiming.
///
/// The caller must already be a lock-group leader (BecomeLockGroupLeader)
/// and must call `close_and_await` on the returned entry before its
/// executor arena unwinds, on every path.
pub fn try_engage(shared: &Arc<ParallelShared>, dop: usize) -> Option<Arc<StandingEngagement>> {
    if !pool_binding_enabled() || dop == 0 || shutting_down() {
        return None;
    }
    let spawner = SPAWNER.get()?;
    // Per-arm dispatch (M2 inc-1): the engagement must carry its driver.
    shared.standing_driver()?;
    let size = gang_size();
    if size == 0 {
        return None;
    }
    // Workers join the leader's lock group the moment they claim a ticket:
    // the leader must already be a group leader (idempotent; the launched
    // path's LaunchParallelWorkers does the same).
    if lmgr_proc::BecomeLockGroupLeader().is_err() {
        return None;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if g.current.is_some() {
        // One engagement at a time (single-query M2 scope): a busy board
        // falls back to the launched path, never queues.
        return None;
    }
    if g.slots.is_empty() {
        g.slots = vec![SlotState::Vacant; size];
    }
    // Crash-fence recovery: a leader engaging proves reinit completed
    // (backends only run against live shared memory). Pre-crash threads
    // stay fenced by the epoch bump; without this clear, every respawned
    // worker would raw-exit on the stale retire_all and leak its freshly
    // claimed PGPROC against LIVE shmem, draining the bgworker freelist.
    g.retire_all = false;
    // The pool channel's rendering of the same proof (see the static's
    // doc): minting spans may run again.
    POOL_CRASH_PENDING.store(false, SeqCst);
    // The engaging leader IS connected to this database — it exists again;
    // any retire entry for its oid is stale (recreated oid). Prune so
    // freshly-pinned workers don't spuriously exit at their next wake.
    g.retired_dbs.retain(|d| *d != shared.database_id);
    // Respawn vacant slots (first engagement, post-retire, post-death).
    for (i, s) in g.slots.iter_mut().enumerate() {
        if *s == SlotState::Vacant && spawner(i) {
            *s = SlotState::Live;
        }
    }
    if !g.slots.iter().any(|s| *s == SlotState::Live) {
        return None;
    }
    let entry = Arc::new(StandingEngagement {
        shared: Arc::clone(shared),
        tickets: dop,
        claimed: AtomicUsize::new(0),
        detached: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
        yielded: AtomicUsize::new(0),
        yield_grants: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
    });
    g.current = Some(Arc::clone(&entry));
    cv.notify_all();
    Some(entry)
}

/// Close the board entry WITHOUT waiting: no new claims; ticketless parked
/// workers wake off the board. The leader still owes `close_and_await`
/// before its arena unwinds — this is the CHASE-ORDERING face
/// (GL-STMTTASK-1): an interrupt chase against a still-open board races a
/// re-claim storm (every detach notifies the gang cv; parked workers
/// re-claim the aborted-but-open engagement in a tight serve cycle —
/// witnessed at ~4k claims per chase — and a join poll can livelock against
/// perpetually in-flight claims). Close first, then chase, then await.
/// Idempotent with `close_and_await` (same closed store + board clear).
pub fn close_no_wait(entry: &Arc<StandingEngagement>) {
    entry.closed.store(true, SeqCst);
    let (lock, cv) = gang();
    let mut g = pgsync::lock(lock);
    if let Some(cur) = &g.current {
        if Arc::ptr_eq(cur, entry) {
            g.current = None;
        }
    }
    drop(g);
    cv.notify_all();
}

/// Leader side: close the board entry (no new claims) and wait until every
/// claimed participant detached. Interrupt-opaque by design: detach is
/// Drop-guaranteed on the workers, so this wait is bounded by one drive
/// teardown; the caller handles query-level errors/cancel BEFORE calling
/// (abort the RG first — drives observe it at the next morsel boundary).
pub fn close_and_await(entry: &Arc<StandingEngagement>) {
    entry.closed.store(true, SeqCst);
    let (lock, cv) = gang();
    let mut g = pgsync::lock(lock);
    if let Some(cur) = &g.current {
        if Arc::ptr_eq(cur, entry) {
            g.current = None;
        }
    }
    // Wake ticketless workers parked on the board state.
    cv.notify_all();
    // Post-close claim race: try_claim rechecks `closed` after its
    // fetch_add and returns over-claims, so `claimed` is stable once
    // closed is visible and every claimer either detaches or never held
    // a ticket; an over-claim's settling decrement carries its own wake
    // (see try_claim).
    //
    // The detach-count Condvar join (permit step-3 conversion, census §3
    // row 1): replaces the wait_parallel_finish_quantum latch-poll with the
    // lost-wake-FREE shape the loom model
    // `standing_gang_detach_count_join_no_lost_wake` pins — the condition is
    // re-checked under the gang lock, and every counter movement notifies
    // through the same lock, so the wait can never park past the last
    // detach. Interrupt-opacity is unchanged (the poll discarded cancel
    // dispositions here too; the wait stays bounded by one drive teardown
    // because detach is Drop-guaranteed on the workers).
    while entry.detached.load(SeqCst) < entry.claimed.load(SeqCst) {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
    drop(g);
}

/// DROP DATABASE rider (parallel_pool_retire_db seam, alongside wpool's):
/// standing workers pinned to the dropped database exit — releasing their
/// PGPROCs and procarray entries for CountOtherDBBackends.
pub fn retire_db(dboid: Oid) {
    if GANG.get().is_none() {
        return;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if !g.retired_dbs.contains(&dboid) {
        g.retired_dbs.push(dboid);
    }
    cv.notify_all();
}

/// Crash reinit (wpool flush_for_crash discipline): shared memory is about
/// to be reset wholesale — bump the epoch so every woken worker exits RAW,
/// touching nothing shared. Pool-db threads (M2 inc-2) are fenced by the
/// separate `POOL_FENCE` epoch: they park on the runtime eventcount (not
/// the gang condvar) and touch shared memory only inside a serve, so the
/// fence is checked at serve entry — a stale-identity thread exits RAW
/// there (PoolRetireRaw) and its slot respawns cold.
pub fn flush_for_crash() {
    POOL_FENCE.fetch_add(1, SeqCst);
    // Crash window opens: minting spans (identity bring-up, warm-connect)
    // refuse until a leader engages again (the clear below / in the
    // engage paths — an engagement proves reinit completed).
    POOL_CRASH_PENDING.store(true, SeqCst);
    if GANG.get().is_none() {
        return;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    g.epoch += 1;
    g.retire_all = true;
    g.slots.iter_mut().for_each(|s| *s = SlotState::Vacant);
    g.current = None;
    cv.notify_all();
}

/// How a `gang_worker_loop` thread wants to exit; the spawn glue acts.
pub enum GangExit {
    /// Ordinary exit: run `ipc::proc_exit` so the deferred callbacks
    /// (ProcKill, RemoveProcFromArray, sinval cleanup) release identity
    /// against LIVE shared memory.
    Clean,
    /// Crash fence: shared memory may be mid-reset — exit the thread with
    /// NO shared-memory interaction (no callbacks).
    Raw,
}

/// True when an unwind payload is EXIT-COMMITTED (FATAL's ProcExitThread /
/// PanicExitThread): drivers and containment layers must rethrow these —
/// the thread is dying and its proc_exit callback chain owns cleanup.
pub fn is_exit_unwind(payload: &(dyn std::any::Any + Send)) -> bool {
    payload.is::<ipc::ProcExitThread>() || payload.is::<types_error::PanicExitThread>()
}

// ---------------------------------------------------------------------------
// CEREMONY-V2 deferred visibility (lazy bind): a standing worker claiming a
// ticket no longer re-enters the procarray / retakes its ProcSignal slot up
// front — a participant that never claims work stays park-invisible
// throughout. serve_ticket ARMS the deferral before the driver; the deferred
// binder's first-touch bind CONSUMES it (procarray re-add BEFORE any
// snapshot state exists — xmin only counts while visible — plus the
// no-callback ProcSignal re-init); serve_ticket's tail removes both iff the
// deferral ENGAGED. The first-connect path stays eager (InitPostgres adds
// visibility as a side effect of connecting).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum DeferredVis {
    Off,
    Armed,
    Engaged,
}

thread_local! {
    static DEFERRED_VIS: std::cell::Cell<DeferredVis> =
        const { std::cell::Cell::new(DeferredVis::Off) };
}

fn arm_deferred_visibility() {
    DEFERRED_VIS.with(|c| c.set(DeferredVis::Armed));
}

/// Tail read: did the first-touch consume the arming? Resets to Off.
fn take_deferred_visibility_engaged() -> bool {
    DEFERRED_VIS.with(|c| {
        let engaged = c.get() == DeferredVis::Engaged;
        c.set(DeferredVis::Off);
        engaged
    })
}

/// Called by the deferred binder at first touch (query_task_guard::
/// DeferredQueryTaskBinding::bind_now). No-op unless a standing serve armed
/// the deferral on this thread.
pub(crate) fn engage_deferred_visibility() -> types_error::PgResult<()> {
    if DEFERRED_VIS.with(|c| c.get()) != DeferredVis::Armed {
        return Ok(());
    }
    super::gtrace("w.vis.begin");
    procarray_seams::proc_array_add::call(init_small::globals::MyProcNumber())?;
    if let Err(e) = procsignal::ProcSignalReinitStanding(&[]) {
        let _ = elog::elog(
            WARNING,
            format!("standing executor ProcSignal re-init failed: {}", e.message()),
        );
    }
    super::gtrace("w.vis.end");
    DEFERRED_VIS.with(|c| c.set(DeferredVis::Engaged));
    Ok(())
}

/// Glue: mark a slot respawnable (worker exit / init failure).
pub fn note_worker_exit(ordinal: usize) {
    if GANG.get().is_none() {
        return;
    }
    let (lock, _) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(s) = g.slots.get_mut(ordinal) {
        *s = SlotState::Vacant;
    }
}

/// Worker loop, called by the launch_backend spawn glue on a thread that
/// already owns full bgworker-shaped identity (see module doc). Parks on
/// the gang condvar between engagements; returns only to exit.
pub fn gang_worker_loop(_ordinal: usize) -> GangExit {
    let my_epoch = {
        let (lock, _) = gang();
        lock.lock().unwrap_or_else(|p| p.into_inner()).epoch
    };

    loop {
        enum Wake {
            Engage(Arc<StandingEngagement>),
            Blocked(Arc<StandingEngagement>),
            RetireRaw,
            Retire,
        }
        let wake = {
            let (lock, cv) = gang();
            let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
            loop {
                if g.epoch != my_epoch || g.retire_all {
                    break Wake::RetireRaw;
                }
                // Postmaster shutdown: exit CLEAN between engagements
                // (shared memory is live under smart/fast shutdown; the
                // proc_exit drain releases the boot-reserved PGPROC). The
                // crash fence above still wins when both are set.
                if shutting_down() {
                    break Wake::Retire;
                }
                {
                    let mine = init_small::globals::MyDatabaseId();
                    if mine != InvalidOid && g.retired_dbs.contains(&mine) {
                        break Wake::Retire;
                    }
                }
                match g.current.as_ref() {
                    Some(entry) => {
                        let db = entry.shared.database_id;
                        let mine = init_small::globals::MyDatabaseId();
                        // DB-pinning (TD-1): unconnected workers adopt the
                        // engagement's database; connected ones only serve
                        // their own. Mismatch = parked non-participation.
                        if mine == InvalidOid || mine == db {
                            break Wake::Engage(Arc::clone(entry));
                        }
                        break Wake::Blocked(Arc::clone(entry));
                    }
                    None => {}
                }
                g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
            }
        };
        match wake {
            Wake::RetireRaw => {
                // Sticky retention (ceremony-v2) is heap-only and its guard
                // is disarmed while parked: a plain drop, no shared-memory
                // interaction — safe under the crash fence.
                super::query_task_guard::sticky_clear();
                return GangExit::Raw;
            }
            Wake::Retire => {
                super::query_task_guard::sticky_clear();
                // Parked workers are procarray-ABSENT (serve_ticket's
                // bracket); the exit-callback chain (RemoveProcFromArray)
                // expects membership — rejoin before proc_exit.
                if init_small::globals::MyDatabaseId() != InvalidOid {
                    let _ = procarray_seams::proc_array_add::call(
                        init_small::globals::MyProcNumber(),
                    );
                }
                return GangExit::Clean;
            }
            Wake::Engage(entry) => match entry.try_claim() {
                Some(ticket) => serve_ticket(&entry, ticket),
                None => {
                    // Claim-race loser: if still unconnected, WARM UP
                    // against the engagement's database anyway — otherwise
                    // a lower-DOP engagement stream keeps hitting cold
                    // InitPostgres on whichever unconnected worker wins a
                    // later claim race (the connect cost lands INSIDE a
                    // measured query). Cold connects then all happen on
                    // the gang's first engagement, any DOP.
                    warm_connect(&entry);
                    park_until_board_changes(&entry);
                }
            },
            Wake::Blocked(entry) => park_until_board_changes(&entry),
        }
    }
}

/// Ticketless warm-up: connect an unconnected worker to the engagement's
/// database (procarray bracket included — connected-but-parked workers
/// stay invisible to CountOtherDBBackends). A FAILED connect is
/// thread-fatal (see `connect_failed_die`): InitPostgres may have claimed
/// shared identity (the sinval slot is claimed early) before failing, and
/// only this thread's exit-callback drain can release it — surviving
/// half-connected poisons the slot for the server's life. Exit-committed
/// unwinds keep unwinding to the glue.
fn warm_connect(entry: &Arc<StandingEngagement>) {
    if init_small::globals::MyDatabaseId() != InvalidOid
        || entry.shared.database_id == InvalidOid
        // Shutdown fence: never START a cold InitPostgres behind the
        // postmaster's stop — the worker is about to exit; a connect here
        // would run catalog/startup work concurrent with the shutdown
        // checkpoint the state machine is sequencing.
        || shutting_down()
    {
        return;
    }
    // Ticketless shared-memory span: no leader's close_and_await covers a
    // claim-race loser, so the crash reset must wait this connect out
    // through the busy term (a redundant charge on gang threads — they are
    // LIVE-counted for their whole lifetime — but harmless there).
    let _busy = pool_shm_busy_guard();
    // Crash-window re-check under the charge (the retire_all/clear-at-
    // engage pattern): a bump may have landed after this thread's
    // serve-entry gate — never START a cold InitPostgres against memory
    // the reset is about to reclaim.
    if pool_crash_pending() {
        return;
    }
    super::gtrace("g.warmconn.begin");
    let connected = catch_unwind(AssertUnwindSafe(|| {
        bgworker::BackgroundWorkerInitializeConnectionByOid(
            entry.shared.database_id,
            entry.shared.authenticated_user_id,
            bgworker::BGWORKER_BYPASS_ALLOWCONN | bgworker::BGWORKER_BYPASS_ROLELOGINCHECK,
        )
        .and_then(|()| {
            mbutils::SetClientEncoding(mbutils::GetDatabaseEncoding()).map(|_| ())
        })
    }));
    super::gtrace("g.warmconn.end");
    match connected {
        Ok(Ok(())) => {
            // Park-invisibility: InitPostgres added us to the procarray
            // and took a ProcSignal slot; release both until the next
            // claimed ticket re-adds them.
            if let Err(e) = procarray_seams::proc_array_remove::call(
                init_small::globals::MyProcNumber(),
                types_core::InvalidTransactionId,
            ) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor warm-up procarray remove failed: {}",
                        e.message()
                    ),
                );
            }
            procsignal::ProcSignalRelease();
        }
        Ok(Err(e)) => {
            let _ = elog::elog(
                WARNING,
                format!("standing executor warm-up connect failed: {}", e.message()),
            );
            connect_failed_die();
        }
        Err(payload) => {
            if is_exit_unwind(&*payload) {
                resume_unwind(payload);
            }
            let _ = elog::elog(
                WARNING,
                "standing executor warm-up connect panicked".to_string(),
            );
            connect_failed_die();
        }
    }
}

/// A gang worker's InitPostgres failed (Err or a caught generic panic): the
/// thread may hold PARTIALLY-CLAIMED shared identity — SharedInvalBackendInit
/// claims the sinval slot early in the connect, and any later failure
/// (ProcSignalInit, the startup transaction, CheckMyDatabase, an assert)
/// returns with the slot claimed under this thread's pid. In C a bgworker
/// connect failure is FATAL and proc_exit's callback chain releases whatever
/// was claimed; the pre-fix "stay cold and retry" arm instead self-collided
/// forever ("sinval slot for backend N is already in use by process <own
/// pid>" on every retry — INBOX-standing-gang-dev-wedge item 3, poisoning
/// every later standing warm-up). Die the way C does: the glue's deferred
/// drain releases identity against live shared memory and the slot respawns
/// on the next engagement.
fn connect_failed_die() -> ! {
    ipc::proc_exit(1, init_small::globals::MyProcPid())
}

/// The board still shows an engagement we cannot serve (no ticket / wrong
/// db) — wait until it changes so the wake loop does not spin.
fn park_until_board_changes(entry: &Arc<StandingEngagement>) {
    let mine = init_small::globals::MyDatabaseId();
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    while g
        .current
        .as_ref()
        .is_some_and(|cur| Arc::ptr_eq(cur, entry))
        && !g.retire_all
        && !shutting_down()
        && !(mine != InvalidOid && g.retired_dbs.contains(&mine))
    {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
}

/// One claimed ticket: connect if first use (db-pin), impersonate a
/// parallel worker, join the leader's lock group, run the driver (which
/// owns the single binder bind + executor build + pinned drive), then
/// restore the standing state. Detach is Drop-guaranteed.
///
/// PROCARRAY VISIBILITY brackets the bound span (the wpool park
/// precedent: a PARKED worker must be invisible to CountOtherDBBackends
/// — an idle procarray entry pinned to a database blocks DROP DATABASE,
/// whose retire rider fires only in late dropdb cleanup, after the
/// count). First use: InitPostgres's InitProcessPhase2 adds; later
/// engagements re-add here. The NORMAL tail removes; unwind paths
/// deliberately leave the entry for the thread-exit callback
/// (RemoveProcFromArray) — removing under unwind would double-remove at
/// the thread top and abort the callback drain that releases the PGPROC.
fn serve_ticket(entry: &Arc<StandingEngagement>, ticket: usize) {
    let shared = &entry.shared;
    let detach = DetachGuard { entry };
    let mut in_procarray = false;

    // Per-arm dispatch (M2 inc-1): the driver rides the engagement's
    // ParallelShared. Unreachable-missing (try_engage gates on it) —
    // refuse fail-closed rather than expect().
    let Some(driver) = shared.standing_driver() else {
        entry.refused.fetch_add(1, SeqCst);
        return;
    };

    // First engagement on this worker: adopt the engagement's database
    // (exactly parallel_worker_body's connect flags).
    if init_small::globals::MyDatabaseId() == InvalidOid {
        if shared.database_id == InvalidOid {
            entry.refused.fetch_add(1, SeqCst);
            return;
        }
        super::gtrace("g.conn.begin");
        let connected = catch_unwind(AssertUnwindSafe(|| {
            bgworker::BackgroundWorkerInitializeConnectionByOid(
                shared.database_id,
                shared.authenticated_user_id,
                bgworker::BGWORKER_BYPASS_ALLOWCONN | bgworker::BGWORKER_BYPASS_ROLELOGINCHECK,
            )
            .and_then(|()| {
                mbutils::SetClientEncoding(mbutils::GetDatabaseEncoding()).map(|_| ())
            })
        }));
        super::gtrace("g.conn.end");
        match connected {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = elog::elog(
                    WARNING,
                    format!("standing executor connect failed: {}", e.message()),
                );
                // Refuse the ticket for the leader's accounting, then die:
                // the failed InitPostgres may hold a partially-claimed
                // identity (sinval slot) only an exit drain releases (see
                // connect_failed_die). The DetachGuard drops on the unwind.
                entry.refused.fetch_add(1, SeqCst);
                drop(detach);
                connect_failed_die();
            }
            Err(payload) => {
                // FATAL-shaped connect failure: refuse the ticket, detach
                // (guard), and keep the exit unwinding to the glue. A
                // generic (non-exit) panic converts to the same thread-fatal
                // exit as the Err arm — half-connected survival is the
                // sinval-slot self-poison.
                entry.refused.fetch_add(1, SeqCst);
                drop(detach);
                if is_exit_unwind(&*payload) {
                    resume_unwind(payload);
                }
                let _ = elog::elog(
                    WARNING,
                    "standing executor connect panicked".to_string(),
                );
                connect_failed_die();
            }
        }
        in_procarray = true; // InitPostgres's InitProcessPhase2 added us.
    } else {
        if driver.deferred_bind && super::query_task_guard::lazy_bind_enabled() {
            // CEREMONY-V2 lazy bind (deferred-binder drivers only — the
            // sink arms' eager binder never calls the consuming bind_now,
            // so arming it for them would bind with the worker
            // procarray-INVISIBLE): visibility (procarray + ProcSignal) is
            // deferred to the FIRST MORSEL CLAIM — the deferred binder
            // consumes this arming before its snapshot restore (xmin
            // ordering held). A participant that never claims work stays
            // park-invisible and pays neither the procarray lock nor the
            // ProcSignal slot churn.
            arm_deferred_visibility();
        } else {
            // Re-engagement on a connected worker: rejoin the procarray
            // BEFORE any snapshot state exists (the binder's snapshot
            // restore publishes xmin, which only counts while visible),
            // and retake a ProcSignal slot (released at every park — see
            // the tail).
            if let Err(e) = procarray_seams::proc_array_add::call(
                init_small::globals::MyProcNumber(),
            ) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor procarray re-add failed: {}",
                        e.message()
                    ),
                );
                entry.refused.fetch_add(1, SeqCst);
                return;
            }
            in_procarray = true;
            // No-callback variant: the connect-time init registered the
            // exit callback once; re-registering per engagement would grow
            // the exit-callback stack unboundedly.
            if let Err(e) = procsignal::ProcSignalReinitStanding(&[]) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor ProcSignal re-init failed: {}",
                        e.message()
                    ),
                );
            }
        }
    }
    debug_assert_eq!(init_small::globals::MyDatabaseId(), shared.database_id);

    // Parallel-worker impersonation for the binder's validate() and the
    // executor's IsParallelWorker gates; cleared on every exit path.
    super::PARALLEL_WORKER_NUMBER.with(|c| c.set(ticket as i32));
    // EAGER-binding drivers (the sink arms) cannot bind over a parked
    // sticky retention (the eager validate()'s envelope gate refuses a
    // live retained session bind) — evict it with the full session restore
    // FIRST (the deferred binder evicts its own mismatches inside the
    // driver, in DeferredQueryTaskBinding::new). Placement law: eviction
    // runs UNDER the parallel-worker impersonation above — the guard's
    // finish restores the parallel start timestamps, whose setter asserts
    // IsParallelWorker (the deferred path's evictions always ran inside
    // the driver, i.e. impersonated). A failed eviction refuses this
    // ticket fail-closed (pre-bind; the RG is untouched by this worker)
    // and falls through to the normal tail.
    let evict_ok = driver.deferred_bind
        || match super::query_task_guard::sticky_evict_parked() {
            Ok(()) => true,
            Err(e) => {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor sticky eviction failed: {}",
                        e.message()
                    ),
                );
                entry.refused.fetch_add(1, SeqCst);
                false
            }
        };
    let joined = if evict_ok {
        lmgr_proc::BecomeLockGroupMember(
            shared.parallel_leader_proc_number,
            shared.parallel_leader_pid,
        )
    } else {
        // Refused (and counted) above: skip the join and the driver, keep
        // the unimpersonation + park-invisibility tail below.
        Ok(false)
    };
    match joined {
        Ok(false) if !evict_ok => {}
        Ok(true) => {
            // The driver catches its own panics into the payload (the M1
            // hook discipline); this outer catch is containment of last
            // resort so lock-group leave + unimpersonation always run —
            // EXCEPT for exit-committed unwinds (FATAL's ProcExitThread /
            // PanicExitThread): a backend must actually die after FATAL,
            // and its proc_exit callback chain (ProcKill) performs the
            // lock-group leave; swallowing it would leave a terminated
            // worker serving future engagements. DetachGuard already
            // covers the leader's join on this path.
            let r = catch_unwind(AssertUnwindSafe(|| (driver.drive)(shared)));
            // POOL-QOS: settle a granted serve-yield AFTER the driver has
            // fully returned (teardown done — no arena refs remain on this
            // thread) and before the lock-group leave. Covers the caught-
            // generic-panic arm too (the grant was spent either way); an
            // exit-committed unwind leaks its grant — the board dies with
            // the query and a leaked grant only makes the last-active
            // guard stricter (the safe direction).
            if YIELD_PENDING.with(|c| c.replace(false)) {
                entry.yield_detach();
            }
            if let Err(payload) = r {
                if payload.is::<ipc::ProcExitThread>()
                    || payload.is::<types_error::PanicExitThread>()
                {
                    resume_unwind(payload);
                }
            }
            lmgr_proc::LeaveLockGroup();
        }
        Ok(false) => {
            // Leader already gone (cancel raced the publish): refuse.
            entry.refused.fetch_add(1, SeqCst);
        }
        Err(e) => {
            let _ = elog::elog(
                WARNING,
                format!("standing executor lock-group join failed: {}", e.message()),
            );
            entry.refused.fetch_add(1, SeqCst);
        }
    }
    super::PARALLEL_WORKER_NUMBER.with(|c| c.set(-1));
    // GL-GANGWEDGE-1 deterministic wedge injection (inert in production).
    maybe_inject_pool_stall();
    // Park invisibility (see the fn doc): leave the procarray (parked
    // workers must be invisible to CountOtherDBBackends) AND release the
    // ProcSignal slot (a live slot whose owner never drains signals
    // wedges WaitForProcSignalBarrier — dropdb's SMGRRELEASE barrier).
    // Unwind paths above skip both deliberately — the thread-exit
    // callbacks handle them then. A deferred-visibility ticket only
    // removes what its first-touch actually added.
    let in_procarray = in_procarray || take_deferred_visibility_engaged();
    if in_procarray {
        if let Err(e) = procarray_seams::proc_array_remove::call(
            init_small::globals::MyProcNumber(),
            types_core::InvalidTransactionId,
        ) {
            let _ = elog::elog(
                WARNING,
                format!("standing executor procarray remove failed: {}", e.message()),
            );
        }
        procsignal::ProcSignalRelease();
    }
}

// ---------------------------------------------------------------------------
// M2 inc-2 — PGPROC-LEASING POOL WORKERS (scratchpad/night/
// m2-pool-binding-scope.md §3 inc-2): the standing engagement machinery
// re-homed onto the RUNTIME POOL. A leader publishes a per-RG
// StandingEngagement through the runtime's bound descriptor
// (`runtime::submit_pinned_bound`) instead of the process-global gang
// board; idle pool workers whose pick lands on the RG claim tickets through
// `pool_serve` and run serve_ticket VERBATIM (connect-if-first-use,
// park-invisibility bracket, impersonation, lock-group join, per-arm driver
// dispatch, Drop-guaranteed detach). What the re-homing removes: the
// separate gang thread population, the one-engagement-at-a-time board, and
// the gang park/wake machinery — elasticity rides the pool's own permits.
// What it keeps byte-identical: the binder, the visibility bracket, the
// leader's close_and_await join, the exit-unwind discipline.
//
// Thread identity is the rtpool spawn glue's (launch_backend::rtpool under
// PGRUST_RUNTIME_POOLDB=1): rtgang-shaped bring-up (InitProcess from the
// boot-reserved segment, BaseInit) COMPLETED at the first serve gate — an
// engaging leader proves live shared memory, so identity is never minted
// inside a crash-restart window — and verified per serve through the
// installed POOL_GATE; the crash fence (POOL_FENCE) and the DROP DATABASE
// rider are self-checked at serve entry — a parked pool-db thread is
// procarray-invisible and holds no ProcSignal slot, so it never blocks
// either while idle.
// ---------------------------------------------------------------------------

/// Crash-fence epoch for pool-db threads (see `flush_for_crash`). A pool
/// thread captures it at identity bring-up; a mismatch at serve entry means
/// its identity predates a shared-memory reset — exit RAW.
static POOL_FENCE: AtomicUsize = AtomicUsize::new(0);

pub fn pool_fence_epoch() -> usize {
    POOL_FENCE.load(SeqCst)
}

/// GL-POOLDB-HELPERDEATH-1: pool-db threads inside a shared-memory-touching
/// span the crash reset must wait out — deferred identity bring-up
/// (InitProcess/BaseInit at the serve gate), exit-callback drains, and the
/// ticketless warm-connect. The postmaster's PM_WAIT_BACKENDS quiescence
/// gate reads this through the rtgang_live seam sum (launch_backend's
/// runtime_shm_busy_threads): pool threads carry no pmchild slot, no exit
/// announce, and no gang LIVE charge, so without this term the crash reset
/// ran underneath an in-flight exit drain (the round-32 helperdeath wedge:
/// the drain's re-find assert fired holding a lock-table partition LWLock
/// and the swallowed panic leaked the partition forever). PARKED threads
/// never charge it — they are procarray-invisible and touch nothing
/// shared; waiting on them would deadlock the reset (identity retires
/// lazily at serves, by design).
static POOL_SHM_BUSY: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// Crash window open: set by `flush_for_crash` (shared memory is dead or
/// about to be reset), cleared when a leader ENGAGES the pool again —
/// backends only run against live shared memory, so an engagement is the
/// proof reinit completed (the gang board's `retire_all` clear-at-engage
/// pattern, rendered for the pool channel). Consulted by every span that
/// would MINT new shared-memory state without a covering leader join:
/// the deferred identity bring-up (rtpool pool_identity_complete) and the
/// ticketless warm-connect. Spans that only RELEASE state (exit drains)
/// use the identity's own fence epoch instead.
static POOL_CRASH_PENDING: AtomicBool = AtomicBool::new(false);

pub fn pool_crash_pending() -> bool {
    POOL_CRASH_PENDING.load(SeqCst)
}

/// See POOL_SHM_BUSY; consumed by the rtgang_live seam sum.
pub fn pool_shm_busy() -> i32 {
    // The gate-side store-buffering fence (pairs with the guard's; the
    // caller's POOL_FENCE bump precedes this read on the postmaster
    // thread): guarantees this read sees every charge whose own fence
    // check could still have read the PRE-bump epoch.
    std::sync::atomic::fence(SeqCst);
    POOL_SHM_BUSY.load(SeqCst)
}

/// RAII charge on POOL_SHM_BUSY. Acquire BEFORE the span's fence check:
/// the postmaster bumps POOL_FENCE (flush_for_crash) before its quiescence
/// gate ever reads the count, so either the span sees the bump (and exits
/// raw / refuses) or the gate sees the charge and the reset waits the span
/// out against live shared memory. The loom model
/// `pooldb_crash_drain_never_races_reset` (runtime/tests/loom.rs) pins
/// this protocol; its exploration is why both sides carry explicit SeqCst
/// fences (the charge/bump pair is a cross-thread store/load exchange —
/// without the fences each side can read the other's OLD value).
pub struct PoolShmBusyGuard {
    fence_at_entry: usize,
}

pub fn pool_shm_busy_guard() -> PoolShmBusyGuard {
    POOL_SHM_BUSY.fetch_add(1, SeqCst);
    std::sync::atomic::fence(SeqCst);
    PoolShmBusyGuard {
        fence_at_entry: POOL_FENCE.load(SeqCst),
    }
}

/// The state-machine poke the busy guard fires (PMSIGNAL_ADVANCE_STATE_
/// MACHINE) — installed by the rtpool glue (launch_backend), which owns the
/// pmsignal dependency; this crate reaches pmsignal only through seams
/// (the POOL_GATE fn-pointer precedent).
static POOL_BUSY_POKE: OnceLock<fn()> = OnceLock::new();

pub fn install_pool_busy_poke(f: fn()) {
    let _ = POOL_BUSY_POKE.set(f);
}

impl Drop for PoolShmBusyGuard {
    fn drop(&mut self) {
        POOL_SHM_BUSY.fetch_sub(1, SeqCst);
        // Wake the postmaster's quiescence gate ONLY when a crash cycle or
        // shutdown plausibly waits on this span (this charge can be the
        // LAST thing PM_WAIT_BACKENDS waits for; the state machine has no
        // other reason to re-run then). Fence-stable drops (every healthy
        // bring-up/drain/warm-connect) stay poke-free — the gang
        // LiveGuard's unconditional poke is per thread DEATH; this guard
        // is not.
        if (POOL_FENCE.load(SeqCst) != self.fence_at_entry || shutting_down())
            && init_small::globals::IsUnderPostmaster()
        {
            if let Some(poke) = POOL_BUSY_POKE.get() {
                poke();
            }
        }
    }
}

/// GL-GANGWEDGE-1 RUNTIME-GANG STALL INJECTION (`PGRUST_TEST_POOL_STALL_
/// INJECT_MS`; env-gated, inert in production). The runtime-gang analog of
/// execparallel's `PGRUST_TEST_MQ_STALL_INJECT_MS`, which
/// GL-DISCONNECT-WEDGE-1 §7 named as owed ("the e2e's victim shapes and the
/// injection knob need a runtime-gang analog").
///
/// Called at the TAIL of a serve — the query is finished, but the thread takes
/// a `pool_shm_busy_guard` charge and then sleeps FENCE-DEAF: no
/// `shutting_down()` poll, no latch, no condvar. That is precisely the field
/// wedge's mechanism, made deterministic:
/// * the charge is the term `rtgang_live` (runtime_shm_busy_threads) feeds to
///   the postmaster's PM_WAIT_BACKENDS quiescence gate, so the gate cannot
///   pass while this thread sleeps;
/// * the sleep is off every fence-poll point, so `retire_for_shutdown`'s
///   one-way flag and its `notify_all` cannot reach it — a registry-invisible
///   thread carries no pmchild slot, so no signal can reach it either.
///
/// Pre-fix, a fast/smart shutdown hangs here for the whole sleep with no
/// backstop of any kind. Post-fix, the shutdown-stall watchdog escalates to
/// immediate and the forced-exit floor lands within a bounded window.
/// Drives `scripts/gangwedge-shutdown-e2e.sh`.
///
/// One-shot per process: a repeating stall would wedge every serve, and the
/// repro must be able to establish that the server was healthy first.
fn maybe_inject_pool_stall() {
    let Some(ms) = std::env::var("PGRUST_TEST_POOL_STALL_INJECT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    else {
        return;
    };
    static INJECTED: AtomicBool = AtomicBool::new(false);
    if INJECTED.swap(true, SeqCst) {
        return;
    }
    let _busy = pool_shm_busy_guard();
    let _ = elog::elog(
        WARNING,
        format!(
            "GL-GANGWEDGE-1 test injection: runtime executor stalling {ms}ms fence-deaf \
             while holding the shm-busy charge"
        ),
    );
    std::thread::sleep(std::time::Duration::from_millis(ms));
}

/// GL-GANGWEDGE-1 QUIESCENCE-TERM FAULT INJECTOR
/// (`PGRUST_TEST_WEDGE_SHM_BUSY_MS`; env-gated, inert in production).
///
/// Spawns one thread that takes the `POOL_SHM_BUSY` charge and then sleeps
/// FENCE-DEAF for the given duration — no `shutting_down()` poll, no latch, no
/// condvar, and (being registry-invisible) no pmchild slot to signal.
///
/// This synthesizes the WEDGE STATE directly rather than reaching it through a
/// query, and it is the right shape for the shutdown-backstop gate: the thing
/// under test is the postmaster's behaviour when the quiescence term it waits
/// on cannot be cleared, and `runtime_shm_busy_threads` (the `rtgang_live`
/// seam) sums exactly this counter into the PM_WAIT_BACKENDS gate. Driving it
/// through an engagement instead would make the gate depend on which query
/// shapes the runtime router currently covers — i.e. it would silently go
/// VACUOUS whenever engagement coverage moved, which is precisely how the
/// earlier lane's battery missed this class.
///
/// The serve-path injector (`maybe_inject_pool_stall`) remains the vehicle for
/// testing fence-poll coverage on real serves; this one tests the backstop.
pub fn install_wedge_shm_busy_injection() {
    let Some(ms) = std::env::var("PGRUST_TEST_WEDGE_SHM_BUSY_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    else {
        return;
    };
    let _ = std::thread::Builder::new()
        .name("gangwedge-inject".into())
        .spawn(move || {
            let _busy = pool_shm_busy_guard();
            let _ = elog::elog(
                WARNING,
                format!(
                    "GL-GANGWEDGE-1 test injection: holding the runtime quiescence term \
                     (shm-busy) fence-deaf for {ms}ms"
                ),
            );
            std::thread::sleep(std::time::Duration::from_millis(ms));
        });
}

/// Panic payload: a pool-db thread must exit RAW — the rtpool spawn glue
/// catches it, skips the exit-callback drain (shared memory may have been
/// reset under its identity), and respawns the slot cold.
pub struct PoolRetireRaw;

/// Boot wiring (launch_backend rtpool): the per-serve identity gate. Runs
/// ON the pool thread at every serve entry: verifies (or completes) the
/// thread's leased bgworker-shaped identity and the crash fence. `false` =
/// this thread can never bind (bring-up failed) — the serve refuses and the
/// runtime skip-caches the publication. May UNWIND to kill the thread
/// (PoolRetireRaw / ProcExitThread); the glue owns drain + respawn.
static POOL_GATE: OnceLock<fn() -> bool> = OnceLock::new();

pub fn install_pool_gate(f: fn() -> bool) {
    let _ = POOL_GATE.set(f);
}

thread_local! {
    /// True while THIS thread is inside `pool_serve`'s serve_ticket: the
    /// arm drivers consult it for the sticky-retention decision. Before
    /// rung 3, retention was DISABLED wholesale on pool threads — between
    /// engagements a pool thread runs ORDINARY runtime work (maintenance
    /// cycles, unbound task sets), which must never see a retained
    /// session's identity/GUC view. Rung 3 re-enables it under
    /// `pool_sticky_enabled`: the runtime's session-residue gate
    /// (`runtime::evict_session_residue_for_unbound_work`, fed by the
    /// serve adapter's residue hint) evicts the retention at the last gate
    /// before any unbound work runs, so parked retention only ever spans
    /// idle time and same/cross-session serves — exactly the gang envelope
    /// plus the modeled eviction edge. Gang threads (which only ever park
    /// between engagements) keep retention unconditionally.
    static ON_POOL_SERVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// True ⇔ the current engagement is being served by a POOL worker (see
/// ON_POOL_SERVE). Arm drivers pass
/// `sticky = !serving_on_pool() || pool_sticky_enabled()`.
pub fn serving_on_pool() -> bool {
    ON_POOL_SERVE.with(|c| c.get())
}

/// M2 inc-3 rung 3: sticky session retention on POOL serves.
/// `PGRUST_RUNTIME_POOL_STICKY=0` restores the rungs-1-2 posture (eager
/// full session bind on every pool engagement). Default ON — but layered
/// under the pool-db channel itself (`PGRUST_RUNTIME_POOLDB=1`, default
/// OFF), so the default posture of the server is unchanged; the retention
/// safety envelope is the gang's (heap-only parked state, the sticky key +
/// validate_for_sticky_resume gates) plus the runtime's unbound-work
/// eviction gate.
pub fn pool_sticky_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    pooldb_enabled()
        && *ON.get_or_init(|| {
            std::env::var("PGRUST_RUNTIME_POOL_STICKY").map_or(true, |v| v.trim() != "0")
        })
}

/// The runtime's session-residue evictor (installed by the rtpool glue —
/// runtime cannot name this crate): restore the thread to a clean boundary
/// before unbound work, or die. An eviction failure means the session
/// restore itself failed — the thread's session/GUC view is indeterminate
/// and it must NOT survive to run ordinary work (the connect_failed_die
/// rationale: only this thread's exit-callback drain can release its
/// identity safely; the slot respawns cold).
pub fn sticky_evict_for_unbound_work() {
    match super::query_task_guard::sticky_evict_parked() {
        Ok(()) => {}
        Err(e) => {
            let _ = elog::elog(
                WARNING,
                format!(
                    "pool executor sticky eviction failed before unbound work: {}",
                    e.message()
                ),
            );
            ipc::proc_exit(1, init_small::globals::MyProcPid());
        }
    }
}

/// Pool-thread exit hygiene (rtpool glue): drop any parked sticky
/// retention with a PLAIN drop — heap-only, the parked guard is disarmed,
/// no shared-memory interaction (safe on the raw/crash-fence exits too;
/// the gang's RetireRaw discipline).
pub fn sticky_clear_on_pool_exit() {
    super::query_task_guard::sticky_clear();
}

/// Leader side (M2 inc-2): build a POOL engagement board for `dop`
/// participants over one ParallelShared. None = the pool channel is
/// unavailable (kill switches, no driver, no pool identity wiring, lock
/// group failure) — the caller proceeds without a descriptor (standing gang
/// → launched fallback, inc-1 exactly). The entry is INERT until the caller
/// attaches it to a submission via `runtime::submit_pinned_bound` (the
/// publication wake is the pool's engage signal); the caller must
/// `close_and_await` it before its executor arena unwinds, on every path —
/// the standing board's exact leader contract.
pub fn try_engage_pool(
    shared: &Arc<ParallelShared>,
    dop: usize,
) -> Option<Arc<StandingEngagement>> {
    if !pool_binding_enabled() || !pooldb_enabled() || dop == 0 || shutting_down() {
        return None;
    }
    // Per-arm dispatch: the engagement must carry its driver (pool serves
    // dispatch through it exactly like gang serves).
    shared.standing_driver()?;
    // No pool identity wiring ⇒ no pool thread can ever serve.
    POOL_GATE.get()?;
    // A leader engaging proves reinit completed (this leader IS a backend
    // running against live shared memory): close the crash window so the
    // pool's minting spans (identity bring-up, warm-connect) run again —
    // the gang board's try_engage retire_all clear, pool rendering.
    POOL_CRASH_PENDING.store(false, SeqCst);
    // Workers join the leader's lock group the moment they claim a ticket.
    if lmgr_proc::BecomeLockGroupLeader().is_err() {
        return None;
    }
    Some(Arc::new(StandingEngagement {
        shared: Arc::clone(shared),
        tickets: dop,
        claimed: AtomicUsize::new(0),
        detached: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
        yielded: AtomicUsize::new(0),
        yield_grants: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
    }))
}

/// Best-effort procarray re-join for a pool-db thread's GENERIC-PANIC exit
/// (the rtpool glue): a PARKED pool thread is procarray-invisible (the
/// serve bracket), but the exit-callback chain expects membership
/// (RemoveProcFromArray) — re-add before the drain, the gang Wake::Retire
/// discipline. Swallows its own failure (a mid-serve panic dies VISIBLE;
/// the double-add must not stop the drain).
pub fn pool_exit_rejoin_procarray() {
    if init_small::globals::MyDatabaseId() == InvalidOid {
        return;
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = procarray_seams::proc_array_add::call(init_small::globals::MyProcNumber());
    }));
}

/// One `pool_serve` call's verdict (mapped 1:1 onto the runtime's
/// BoundServe by the arm-side adapter — this crate cannot name runtime
/// types, the dependency points the other way).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolServe {
    /// A ticket was claimed and served (serve_ticket ran to its tail).
    Served,
    /// THIS thread cannot serve THIS engagement (no gate, identity
    /// bring-up failure, database-pin mismatch): skip the publication.
    Refused,
    /// Board closed or ticket cap reached: nothing left to serve here.
    Closed,
}

/// Pool-worker side (M2 inc-2): serve one engagement from a bound RG's
/// board. Runs on an rtpool thread through the runtime's claim gate, with
/// the pool execution permit RELEASED (the nested drive has its own permit
/// rhythm). Repeat calls for the same publication are deduplicated by the
/// runtime's skip cache, so the pre-claim refusal count stays ≈ one per
/// mismatched worker — the leader's nobody-participates check fires
/// promptly on an all-mismatched pool without burning tickets.
pub fn pool_serve(payload: &Arc<dyn std::any::Any + Send + Sync>) -> PoolServe {
    let Ok(entry) = Arc::clone(payload).downcast::<StandingEngagement>() else {
        return PoolServe::Refused;
    };
    // SERVE-SPAN busy charge (GL-POOLDB-HELPERDEATH-1 v3 — the load-bearing
    // term): the whole serve (gate, identity completion, claim, drive,
    // teardown) is a shared-memory-touching span the crash reset must wait
    // out. The leader's close_and_await does NOT cover it on every path —
    // a kill9'd leader dies with NO exit callbacks (KilledBySignal skips
    // the drain), so AtEOXact_Parallel/DestroyParallelContext never run,
    // the board is orphaned OPEN, and mid-serve workers keep driving. In C
    // those workers are separate processes the postmaster reaps before the
    // reset; the gang covers them with its lifetime LIVE charge. Without
    // this term the reset ran under a live drive and the drive's own lock
    // teardown asserted against reinitialized tables while holding a
    // lock-table partition LWLock — the leaked partition wedged recovery
    // (round-32 red at both v1 and v2). The charge drops on normal serve
    // exit; a DYING serve's unwind drops it mid-unwind and the glue's
    // fence-checked drain bracket re-charges (the gap touches nothing
    // shared).
    let _busy = pool_shm_busy_guard();
    if entry.closed.load(SeqCst) {
        return PoolServe::Closed;
    }
    // Identity gate (rtpool glue): verify/complete this thread's leased
    // PGPROC identity + crash fence. May unwind to kill the thread.
    let Some(gate) = POOL_GATE.get() else {
        return PoolServe::Refused;
    };
    if !gate() {
        return PoolServe::Refused;
    }
    let mine = init_small::globals::MyDatabaseId();
    // DROP DATABASE rider (self-check): a pool thread pinned to a retired
    // database must not serve with stale caches (recreated-oid hazard) —
    // exit CLEAN before touching the board; the glue's exit drain releases
    // identity (procarray membership restored first: the exit callbacks
    // expect it — the gang's Wake::Retire discipline) and the slot
    // respawns unpinned.
    if mine != InvalidOid {
        let retired = {
            let (lock, _) = gang();
            let g = lock.lock().unwrap_or_else(|p| p.into_inner());
            g.retired_dbs.contains(&mine)
        };
        if retired {
            super::query_task_guard::sticky_clear();
            let _ = procarray_seams::proc_array_add::call(
                init_small::globals::MyProcNumber(),
            );
            ipc::proc_exit(0, init_small::globals::MyProcPid());
        }
    }
    // DB pinning (TD-1): connected threads only serve their own database;
    // unconnected ones adopt the engagement's inside serve_ticket. Counted
    // as a refusal (once per publication per worker — skip-cache dedup)
    // for the leader's started==0 && refused>=tickets fallback.
    //
    // Count-then-wake (DetachGuard's discipline): this is the one refusal
    // that ticks BEFORE any claim exists, so no DetachGuard will ever fire
    // for it — without its own wake the leader's "nobody will participate"
    // check only ran when its latch quantum expired, and every
    // cross-database engagement against a fully-mismatched pool slept the
    // whole MQ-recheck quantum (~1 s additive per execution, GL-ZSTALL-1)
    // before falling back to the gang.
    if mine != InvalidOid && mine != entry.shared.database_id {
        entry.refused.fetch_add(1, SeqCst);
        latch::SetLatch(types_storage::latch::LatchHandle::proc(
            entry.shared.parallel_leader_proc_number,
        ));
        return PoolServe::Refused;
    }
    let Some(ticket) = entry.try_claim() else {
        // Claim-race loser / full board: warm-connect an unconnected
        // thread against the engagement's database anyway (the gang's
        // DOP<size remedy — otherwise a later engagement pays a cold
        // InitPostgres inside a measured query).
        warm_connect(&entry);
        return PoolServe::Closed;
    };
    // ON_POOL_SERVE marks the serve span for the arm drivers' sticky
    // decision (see the thread_local doc: retention rides pool serves only
    // under pool_sticky_enabled, with the runtime's unbound-work eviction
    // gate as the containment). RAII so unwinds (FATAL exits) reset the
    // flag before the glue respawns anything on this thread.
    struct PoolServeReset;
    impl Drop for PoolServeReset {
        fn drop(&mut self) {
            ON_POOL_SERVE.with(|c| c.set(false));
            CURRENT_SERVE_BOARD.with(|slot| slot.borrow_mut().take());
            // A granted-but-unsettled yield can only remain here on an
            // unwind (the structured path settles in serve_ticket);
            // clear the mark so a respawned/reused thread never settles a
            // stale grant against a future board.
            YIELD_PENDING.with(|c| c.set(false));
        }
    }
    ON_POOL_SERVE.with(|c| c.set(true));
    CURRENT_SERVE_BOARD.with(|slot| *slot.borrow_mut() = Some(Arc::clone(&entry)));
    let _reset = PoolServeReset;
    serve_ticket(&entry, ticket);
    PoolServe::Served
}

#[cfg(test)]
mod pooldb_flip_tests {
    /// GL-POOLDB-1 flip pin (t35 flipped-kill law): default ON; `=0|off`
    /// restores the gang-first posture; the historical arming spelling
    /// `=1` stays ON; whitespace trimmed.
    #[test]
    fn pooldb_default_on_kill_spellings() {
        assert!(super::pooldb_posture(None), "unset = flipped default ON");
        assert!(super::pooldb_posture(Some("1")), "historical arming spelling stays ON");
        assert!(super::pooldb_posture(Some("on")), "affirmative spelling ON");
        assert!(!super::pooldb_posture(Some("0")), "=0 kills");
        assert!(!super::pooldb_posture(Some("off")), "=off kills");
        assert!(!super::pooldb_posture(Some(" 0 ")), "kill spelling trimmed");
    }

    /// Armed-witness companion (the add-a-row law's env-gated cargo leg):
    /// self-SKIPs unless the registry row's arming env is present, then
    /// asserts POSITIVELY that the process-level switch resolved to the
    /// posture the env spells — the flip's kill lever provably takes.
    #[test]
    fn pooldb_kill_env_takes() {
        let Ok(v) = std::env::var("PGRUST_RUNTIME_POOLDB") else {
            println!("SKIP: PGRUST_RUNTIME_POOLDB unset (armed-witness leg runs it =0)");
            return;
        };
        let expect = super::pooldb_posture(Some(v.as_str()));
        assert_eq!(
            super::pooldb_enabled(),
            expect,
            "process switch disagrees with the spelled posture"
        );
    }
}
