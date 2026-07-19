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
pub use mailbox::{mailbox, MailboxReceiver, MailboxSender, TryRecv};

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
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Semaphore { permits: Mutex::new(permits), cv: Condvar::new() }
    }

    pub fn acquire(&self) {
        let mut g = lock(&self.permits);
        while *g == 0 {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        *g -= 1;
    }

    pub fn try_acquire(&self) -> bool {
        let mut g = lock(&self.permits);
        if *g == 0 {
            return false;
        }
        *g -= 1;
        true
    }

    pub fn release(&self) {
        let mut g = lock(&self.permits);
        *g += 1;
        drop(g);
        self.cv.notify_one();
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
}

impl ParkLot {
    pub fn new() -> Self {
        ParkLot { epoch: atomic::AtomicU64::new(0), m: Mutex::new(()), cv: Condvar::new() }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(atomic::Ordering::SeqCst)
    }

    pub fn park(&self, seen: u64) {
        let mut g = lock(&self.m);
        while self.epoch.load(atomic::Ordering::SeqCst) == seen {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    pub fn wake_all(&self) {
        // Bump BEFORE the mutex (see the struct doc's lost-wakeup argument).
        self.epoch.fetch_add(1, atomic::Ordering::SeqCst);
        let g = lock(&self.m);
        drop(g);
        self.cv.notify_all();
    }
}

impl Default for ParkLot {
    fn default() -> Self {
        Self::new()
    }
}
