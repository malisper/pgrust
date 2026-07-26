//! pgsync — THE single lock library (permit-s1 step 1; authority:
//! `docs/design/permit-scheduler.md` @ 521676a30, contract §2).
//!
//! One crate, three worlds; this is the ONLY place a world-selection cfg for
//! synchronization may appear. Call sites import pgsync types UNCONDITIONALLY
//! and never contain `cfg(loom)` / `cfg(pgrust_sim)` for sync:
//!
//! | world  | cfg               | arm |
//! |--------|-------------------|-----|
//! | native | neither           | `pub use std::sync::…` / `std::thread::…` re-exports — zero cost, IDENTICAL types (G1 asm letter) |
//! | loom   | `--cfg loom`      | loom's checked types; API deltas vs std papered CENTRALLY here (see the papering table below) |
//! | sim    | `--cfg pgrust_sim`| std-backed permit-hooked wrappers; wait/wake lists owned by the wrapper so every wakee is a seeded pick |
//!
//! Laws inherited from the runtime crate's `sync.rs` (the repo's first loom
//! pattern, absorbed here) — these are CRATE LAW for every consumer:
//! - `Arc` (and `Weak`) stay `std::sync` everywhere: loom's `Arc` does not
//!   coerce to `Arc<dyn Trait>` and we only use `Arc` for ownership of
//!   immutable or internally-synchronized data.
//! - No statics hold loom types (loom primitives are not
//!   const-constructible); all modeled state is instance-owned.
//!   `OnceLock`-mediated init is the sanctioned pattern for unavoidable
//!   globals — which is exactly why the loom arm's `OnceLock`/`Once` are
//!   std-backed (papering row L3 below), NOT loom composites. For
//!   process-global MODELED state (const-init static slabs/registries) the
//!   sanctioned exception is [`process_global!`] (`global` module): plain
//!   static native/sim, per-iteration lazy state under loom.
//! - Poison-tolerant acquisition goes through [`lock`] (the repo's
//!   `unwrap_or_else(e.into_inner())` discipline; loom's Mutex never poisons
//!   but keeps the API; the sim wrapper propagates std poisoning).
//!
//! What the sim wrappers are NOT (contract §2.2): no product-semantics
//! change, no atomic interception (`atomic` is a std re-export under sim —
//! with one runnable thread every atomic op is totally ordered by the
//! schedule; do not "improve" this), no green threads, no lock-free
//! reimplementation. A sim wrapper is a std primitive PLUS permit
//! bookkeeping. If a wrapper type is ever needed on native, the design is
//! being violated — stop and escalate.
//!
//! Loom papering table (API deltas vs std live HERE, nowhere else — worklog
//! `notes/permit-s1-sync.md` carries the running delta table):
//! - L1 non-const `loom::sync::Mutex::new` → "no statics hold loom types"
//!   law (above).
//! - L2 loom HAS `Barrier` (0.7.2, contrary to the design's expectation) —
//!   re-exported directly; no composite needed.
//! - L3 loom lacks `Once`/`OnceLock` → std-backed under loom (models must
//!   not RACE lazy inits through them; the `once` lint arm keeps the class
//!   enumerated; a checked composite is a later increment if a model needs
//!   one).
//! - L4 loom lacks `thread::sleep` → `yield_now` semantics (existing
//!   loomfast practice).
//! - L5 loom lacks `thread::scope` → deliberately ABSENT under loom (no
//!   model needs it; recorded choice per contract §2.3).
//! - L6 loom `mpsc` has `channel` only → `sync_channel` absent under loom
//!   (composite when a model needs it).
//!
//! The scheduler seam: the sim arm calls [`sim::hooks`] (pinned in WS-SYNC's
//! first commit; WS-CORE owns `src/sim/**` and implements the scheduler
//! behind it). With no hooks installed the sim wrappers degrade to plain std
//! behavior — a sim binary without the permit scheduler behaves exactly as
//! today's (sim-net-e2e / dst-smoke unchanged).

#[cfg(all(loom, pgrust_sim))]
compile_error!("pgsync: the loom and pgrust_sim worlds never compose (contract §0)");

// --- world dispatch ---------------------------------------------------------

#[cfg(not(any(loom, pgrust_sim)))]
mod native;
#[cfg(not(any(loom, pgrust_sim)))]
pub use native::*;

#[cfg(loom)]
mod loom_world;
#[cfg(loom)]
pub use loom_world::*;

#[cfg(pgrust_sim)]
mod sim_world;
#[cfg(pgrust_sim)]
pub use sim_world::*;

/// The permit scheduler's home (`pgsync::sim`): hooks API pinned by WS-SYNC's
/// first commit; the scheduler itself (WS-CORE) lives under this module.
#[cfg(pgrust_sim)]
pub mod sim;

/// The `process_global!` shim (LATCH-LOOM): const-init process-global sync
/// state that is a plain static native/sim and per-iteration lazy state under
/// loom. The sanctioned exception to the "no statics hold loom types" law —
/// see the module doc for the exact contract.
pub mod global;

/// The loom crate, for `process_global!`'s expansion in consumer crates
/// (macro hygiene: `$crate::__loom` keeps the loom dependency HERE — callers
/// need no loom dep of their own to use the shim).
#[cfg(loom)]
#[doc(hidden)]
pub use loom as __loom;

// --- all-worlds helpers (compiled over the dispatched types) ----------------

/// The channel-conversion utility (`waiter_mailbox`, dst-p3-scheduler §3):
/// bounded/unbounded MPMC over [`ParkLot`] eventcounts — all-worlds like the
/// other helpers in this crate.
pub mod mailbox;
pub use mailbox::{mailbox, MailboxReceiver, MailboxSender, TryRecv, TrySend};

/// Poison-tolerant lock (matches the repo's `unwrap_or_else(e.into_inner)`
/// discipline; loom's Mutex never poisons in models but keeps the API; the
/// sim wrapper propagates std poisoning).
pub fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Counting semaphore (Mutex+Condvar; std has no stable Semaphore). Moved
/// verbatim from `runtime/src/sync.rs` (M0): the execution-permit semaphore —
/// exactly `cores` permits; any task-executing thread holds one. The pool
/// runs `cores + K` threads (K standbys, redesign §2.8): permits cap RUNNING
/// tasks, standbys exist to absorb permit releases from declared blocking
/// sections (see [`IoGuard`]). Under sim, `acquire` is a blocking op and is
/// hooked automatically through the wrapped Condvar wait.
pub struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
    /// POOL-QOS priority lane (interactive-class engagements): count of
    /// waiters parked in [`Semaphore::acquire_priority`]. AUTHORITATIVE
    /// updates happen under the `permits` mutex (the loom-explorable
    /// protocol); this atomic is a lock-free MIRROR for off-lock advisory
    /// reads ([`Semaphore::priority_waiting`] — the demoted-holder
    /// step-boundary check). Plain std atomic by design: advisory reads
    /// need no loom exploration, and pgsync's L-rules forbid loom types in
    /// statics.
    prio_waiting: core::sync::atomic::AtomicUsize,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Semaphore {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
            prio_waiting: core::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn acquire(&self) {
        let mut g = lock(&self.permits);
        // Defer to the priority lane: while a priority waiter is parked,
        // ordinary acquirers leave the permit for it (release/priority-take
        // both notify_all, so this wait cannot be lost). Starvation of the
        // ordinary lane is bounded by the interactive class's own size —
        // interactive demand is short by classification (undecayed = <1
        // decay quantum of consumed CPU).
        while *g == 0 || self.prio_waiting.load(core::sync::atomic::Ordering::Relaxed) > 0 {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        *g -= 1;
    }

    /// POOL-QOS: acquire ahead of the ordinary lane (interactive-class
    /// drives). Waits only for a PERMIT; ordinary acquirers defer while any
    /// priority waiter is parked, and demoted permit holders release at
    /// morsel boundaries when [`Semaphore::priority_waiting`] is nonzero —
    /// the two halves that bound interactive first-permit latency by ~one
    /// morsel instead of one query.
    pub fn acquire_priority(&self) {
        let mut g = lock(&self.permits);
        self.prio_waiting.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        while *g == 0 {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        *g -= 1;
        let prev = self.prio_waiting.fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
        drop(g);
        if prev == 1 {
            // Last priority waiter satisfied: wake deferred ordinary
            // acquirers (permits may remain — they were waiting on the
            // priority gate, not the count).
            self.cv.notify_all();
        }
    }

    /// Off-lock advisory read: are priority waiters parked? One Relaxed
    /// load — the demoted-holder step-boundary check's whole cost.
    pub fn priority_waiting(&self) -> usize {
        self.prio_waiting.load(core::sync::atomic::Ordering::Relaxed)
    }

    pub fn try_acquire(&self) -> bool {
        let mut g = lock(&self.permits);
        if *g == 0 || self.prio_waiting.load(core::sync::atomic::Ordering::Relaxed) > 0 {
            return false;
        }
        *g -= 1;
        true
    }

    /// Bounded [`Semaphore::acquire`]: same priority-lane deference, but
    /// gives up (returns false) once a wait span times out instead of
    /// parking forever. `true` ⟺ one permit was taken — the caller owns
    /// exactly the release obligation `acquire` would confer, and no path
    /// leaks a permit (the count is only touched under the mutex).
    ///
    /// Timing slack, by design: every non-timeout wake restarts the full
    /// `timeout` budget (no deadline arithmetic — this compiles and models
    /// identically in all three pgsync worlds, where loom/sim time is not
    /// wall time). Callers use it as a deadlock-escape bound, not a timer:
    /// the serial-lease safe-point admission (GL-SLEASE-2) re-tries on its
    /// sweeper cadence.
    pub fn acquire_timeout(&self, timeout: core::time::Duration) -> bool {
        let mut g = lock(&self.permits);
        loop {
            if *g > 0 && self.prio_waiting.load(core::sync::atomic::Ordering::Relaxed) == 0 {
                *g -= 1;
                return true;
            }
            let (ng, res) =
                self.cv.wait_timeout(g, timeout).unwrap_or_else(|e| e.into_inner());
            g = ng;
            if res.timed_out() {
                // One last under-lock look (a release may have landed with
                // the timeout): take it or report failure.
                if *g > 0
                    && self.prio_waiting.load(core::sync::atomic::Ordering::Relaxed) == 0
                {
                    *g -= 1;
                    return true;
                }
                return false;
            }
        }
    }

    pub fn release(&self) {
        let mut g = lock(&self.permits);
        *g += 1;
        drop(g);
        // notify_all (was notify_one): the woken waiter must be able to be
        // the priority one even with ordinary waiters queued ahead in the
        // condvar's arbitrary order; ordinary wakees re-check the priority
        // gate and re-park. Zero-waiter cost is unchanged.
        self.cv.notify_all();
    }

    pub fn available(&self) -> usize {
        *lock(&self.permits)
    }

    /// Enter a DECLARED BLOCKING SECTION (redesign §2.8): the holder's permit
    /// is released on entry so a standby thread can absorb the freed core,
    /// and reacquired on guard drop. No priority on reacquisition. Capacity
    /// moves; the task does NOT — it stays on its thread with its pin-board
    /// entry and finalization-marker obligations intact.
    ///
    /// Caller contract: must actually hold a permit.
    pub fn io_section(&self) -> IoGuard<'_> {
        self.release();
        IoGuard { sem: self }
    }
}

/// RAII permit release for a declared blocking section. See
/// [`Semaphore::io_section`].
pub struct IoGuard<'a> {
    sem: &'a Semaphore,
}

impl Drop for IoGuard<'_> {
    fn drop(&mut self) {
        self.sem.acquire();
    }
}

/// Eventcount-style park/wake for idle workers. Moved verbatim from
/// `runtime/src/sync.rs` (agg192-contention shape: the epoch is an ATOMIC,
/// not mutex-guarded state — `epoch()` runs on every worker-loop iteration
/// of every worker and must not take a global lock).
///
/// Protocol (lost-wakeup-free): capture `epoch()` BEFORE looking for work;
/// if no work is found, `park(seen)` blocks only while the epoch is
/// unchanged (re-checked UNDER the mutex before waiting; `Condvar::wait`
/// releases that mutex atomically). Every publish of new work bumps the
/// epoch FIRST (SeqCst) and then notifies under the mutex — either the
/// parker sees the new epoch at its under-lock check, or it is already
/// waitable when the waker acquires the mutex, so the notify reaches it.
pub struct ParkLot {
    epoch: atomic::AtomicU64,
    m: Mutex<()>,
    cv: Condvar,
    /// GL-STMTTASK-2 wake elision (the Go nmspinning/wakep protocol): count
    /// of workers in the SEARCH phase — woken (or between tasks) and about
    /// to re-scan for work, not yet parked. A publisher that observes a
    /// spinner may skip its notify entirely: the spinner's re-scan (or its
    /// pre-wait epoch recheck under the idle lock) discovers the new work.
    /// SeqCst throughout; the lost-wake argument is the classic
    /// StoreLoad-fence-then-recheck pair — publisher: bump epoch (SeqCst
    /// RMW) THEN read spinners; parker: decrement spinners (under the idle
    /// lock) THEN recheck epoch. In the SC total order either the publisher
    /// sees the decrement (and wakes an idle worker), or the parker's epoch
    /// recheck sees the bump (and never waits).
    spinners: atomic::AtomicUsize,
    /// LIFO idle-worker stack for DIRECTED wakes ([`ParkLot::park_worker`] /
    /// [`ParkLot::wake_work`]): the most-recently-parked worker is warmest
    /// (Dice EuroSys'17: FIFO handoff is inimical to parking; the Tokio
    /// LIFO-slot precedent), with the 3-poll starvation cap — every 4th
    /// directed wake takes the OLDEST parker instead.
    idle: Mutex<IdleStack>,
    /// Actual thread unparks performed (directed pops + wake_all wakes of
    /// registered/legacy parkers) — the wakeup program's
    /// unparks-per-statement counter substrate.
    unparks: atomic::AtomicU64,
    /// Parkers currently inside [`ParkLot::park`]'s wait (the legacy,
    /// non-registered population), maintained under `m`. wake_all counts
    /// them into `unparks`.
    legacy_parked: atomic::AtomicUsize,
}

/// One pool worker's directed-wake slot (GL-STMTTASK-2). Owned by the worker
/// loop for the thread's lifetime; registered in the lot's LIFO idle stack
/// for the duration of each park.
pub struct WorkerParker {
    woken: atomic::AtomicBool,
    cv: Condvar,
}

impl WorkerParker {
    pub fn new() -> Self {
        WorkerParker { woken: atomic::AtomicBool::new(false), cv: Condvar::new() }
    }
}

impl Default for WorkerParker {
    fn default() -> Self {
        Self::new()
    }
}

struct IdleStack {
    stack: Vec<std::sync::Arc<WorkerParker>>,
    /// Directed-wake round counter for the 3-poll starvation cap.
    wakes: u32,
}

impl ParkLot {
    pub fn new() -> Self {
        ParkLot {
            epoch: atomic::AtomicU64::new(0),
            m: Mutex::new(()),
            cv: Condvar::new(),
            spinners: atomic::AtomicUsize::new(0),
            idle: Mutex::new(IdleStack { stack: Vec::new(), wakes: 0 }),
            unparks: atomic::AtomicU64::new(0),
            legacy_parked: atomic::AtomicUsize::new(0),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(atomic::Ordering::SeqCst)
    }

    pub fn park(&self, seen: u64) {
        let mut g = lock(&self.m);
        while self.epoch.load(atomic::Ordering::SeqCst) == seen {
            self.legacy_parked.fetch_add(1, atomic::Ordering::SeqCst);
            let r = self.cv.wait(g);
            self.legacy_parked.fetch_sub(1, atomic::Ordering::SeqCst);
            g = r.unwrap_or_else(|e| e.into_inner());
        }
    }

    pub fn wake_all(&self) {
        // Bump BEFORE the mutex (see the struct doc's lost-wakeup argument).
        self.epoch.fetch_add(1, atomic::Ordering::SeqCst);
        let g = lock(&self.m);
        let legacy = self.legacy_parked.load(atomic::Ordering::SeqCst) as u64;
        drop(g);
        self.cv.notify_all();
        // Registered (directed-slot) parkers wake too — wake_all keeps its
        // everyone-wakes semantics no matter which park entry was used.
        let woken = {
            let mut idle = lock(&self.idle);
            let n = idle.stack.len() as u64;
            for p in idle.stack.drain(..) {
                p.woken.store(true, atomic::Ordering::SeqCst);
                p.cv.notify_all();
            }
            n
        };
        if legacy + woken > 0 {
            self.unparks.fetch_add(legacy + woken, atomic::Ordering::SeqCst);
        }
    }

    // -----------------------------------------------------------------------
    // GL-STMTTASK-2 — spinner-elided submission wakes + LIFO directed parks.
    // Only the pool worker loop uses park_worker/spin_*; publishers that
    // OPT IN to elision use wake_work. Every other caller keeps
    // park/wake_all byte-identically.
    // -----------------------------------------------------------------------

    /// Enter the SEARCH phase (about to re-scan for work). Pairs with
    /// [`ParkLot::spin_found_work`] or the consuming decrement inside
    /// [`ParkLot::park_worker`].
    pub fn spin_enter(&self) {
        self.spinners.fetch_add(1, atomic::Ordering::SeqCst);
    }

    /// Leave the SEARCH phase because work was found. Go's wakep chain: if
    /// this was the last spinner and idle workers remain, wake one — the
    /// next queued item (if any) gets a searcher without the publisher
    /// having to know.
    pub fn spin_found_work(&self) {
        self.spinners.fetch_sub(1, atomic::Ordering::SeqCst);
        if self.spinners.load(atomic::Ordering::SeqCst) == 0 {
            self.wake_one_idle();
        }
    }

    /// Leave the SEARCH phase because this worker is about to BLOCK on
    /// something that is not the park (the loop-top permit acquire): a
    /// blocked thread is NOT searching, and a submission that elides its
    /// wake against this mark would strand work behind the block (the
    /// flip-battery pooldb-spinner client-kill catch). Same last-spinner
    /// chain as spin_found_work: hand the searcher duty to a parked worker.
    pub fn spin_abandon(&self) {
        self.spinners.fetch_sub(1, atomic::Ordering::SeqCst);
        if self.spinners.load(atomic::Ordering::SeqCst) == 0 {
            self.wake_one_idle();
        }
    }

    /// Park on the directed LIFO stack (pool worker loop). `spinning` =
    /// caller holds a spin_enter mark; it is consumed here (decremented
    /// UNDER the idle lock, before the epoch recheck — the elision
    /// protocol's ordering hinge). Returns on any wake; the caller
    /// re-enters the search phase itself.
    pub fn park_worker(&self, seen: u64, parker: &std::sync::Arc<WorkerParker>, spinning: bool) {
        let mut g = lock(&self.idle);
        if spinning {
            self.spinners.fetch_sub(1, atomic::Ordering::SeqCst);
        }
        // GL-SPINPARK-1: the StoreLoad hinge made EXPLICIT (survey rule 2 —
        // the Go wakep protocol's fence): decrement-then-recheck here pairs
        // with bump-then-read-spinners in wake_work; each side fences
        // between its write and its read so one of them always sees the
        // other. The pure SeqCst accesses are formally sufficient under the
        // C++ SC total order, but the explicit fence is (a) Go parity, (b)
        // robustness, and (c) what loom actually verifies — loom explores
        // AcqRel-like weakenings of SC ACCESSES and only pins the total
        // order at SC FENCES; the parklot elision models deadlock without
        // these two fences (found by parklot_wake_work_elision_never_lost).
        atomic::fence(atomic::Ordering::SeqCst);
        if self.epoch.load(atomic::Ordering::SeqCst) != seen {
            return;
        }
        parker.woken.store(false, atomic::Ordering::SeqCst);
        g.stack.push(std::sync::Arc::clone(parker));
        while !parker.woken.load(atomic::Ordering::SeqCst)
            && self.epoch.load(atomic::Ordering::SeqCst) == seen
        {
            g = parker.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        // Epoch-change (or spurious) exit without a directed wake: the slot
        // may still be registered — deregister so a later wake_work cannot
        // pop a slot nobody waits on and count it as a wake.
        if !parker.woken.load(atomic::Ordering::SeqCst) {
            if let Some(i) = g.stack.iter().position(|p| std::sync::Arc::ptr_eq(p, parker)) {
                g.stack.remove(i);
            }
        }
    }

    /// The ELIDED submission wake (publishers of NEW pool-visible work that
    /// opt in): always bump the epoch (a store+RMW — the fence half of the
    /// protocol), then notify only who actually needs it. LEGACY parkers
    /// (external pinned drivers waiting on this same eventcount for their
    /// RG's next publish) are NEVER elided — their protocol is notify-all
    /// based; the under-`m` count read makes the skip race-free (a parker
    /// increments under `m` before waiting, so either its pre-wait epoch
    /// recheck sees our bump or we see its count). POOL workers get the
    /// spinner elision: a live searcher covers the work by its own re-scan;
    /// otherwise wake exactly ONE idle worker (LIFO; 3-poll starvation
    /// cap).
    pub fn wake_work(&self) {
        self.epoch.fetch_add(1, atomic::Ordering::SeqCst);
        // The publisher half of the StoreLoad hinge — pairs with
        // park_worker's fence (rationale there).
        atomic::fence(atomic::Ordering::SeqCst);
        let legacy = {
            let g = lock(&self.m);
            let n = self.legacy_parked.load(atomic::Ordering::SeqCst);
            drop(g);
            n
        };
        if legacy > 0 {
            self.cv.notify_all();
            self.unparks.fetch_add(legacy as u64, atomic::Ordering::SeqCst);
        }
        if self.spinners.load(atomic::Ordering::SeqCst) > 0 {
            return; // store+fence submission: the spinner's re-scan owns it
        }
        self.wake_one_idle();
    }

    /// Wake ONLY the legacy (non-registered) parkers — events that are not
    /// NEW WORK for pool workers (RG completion: external pinned drivers
    /// re-test their outcome on it; an idle pool worker has nothing to do
    /// with it). The epoch still bumps (any registered parker between its
    /// capture and wait re-checks and re-scans — the safe direction);
    /// directed slots stay parked.
    pub fn wake_legacy(&self) {
        self.epoch.fetch_add(1, atomic::Ordering::SeqCst);
        let legacy = {
            let g = lock(&self.m);
            let n = self.legacy_parked.load(atomic::Ordering::SeqCst);
            drop(g);
            n
        };
        if legacy > 0 {
            self.cv.notify_all();
            self.unparks.fetch_add(legacy as u64, atomic::Ordering::SeqCst);
        }
    }

    fn wake_one_idle(&self) {
        let mut g = lock(&self.idle);
        if g.stack.is_empty() {
            return;
        }
        g.wakes = g.wakes.wrapping_add(1);
        // 3-poll starvation cap: every 4th directed wake takes the OLDEST.
        let p = if g.wakes % 4 == 0 { g.stack.remove(0) } else { g.stack.pop().unwrap() };
        p.woken.store(true, atomic::Ordering::SeqCst);
        p.cv.notify_all();
        self.unparks.fetch_add(1, atomic::Ordering::SeqCst);
    }

    /// Live spinner count (diagnostics / tests).
    pub fn spinners(&self) -> usize {
        self.spinners.load(atomic::Ordering::SeqCst)
    }

    /// Total actual thread unparks this lot performed (the wakeup program's
    /// unparks counter; monotonic).
    pub fn unparks(&self) -> u64 {
        self.unparks.load(atomic::Ordering::SeqCst)
    }

    /// Registered idle (directed-slot) parkers right now (diagnostics).
    pub fn idle_workers(&self) -> usize {
        lock(&self.idle).stack.len()
    }
}

impl Default for ParkLot {
    fn default() -> Self {
        Self::new()
    }
}
