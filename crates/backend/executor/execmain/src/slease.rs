//! SERIAL LEASE v2 — floor-gated, donation-aware CPU-seat accounting for
//! session threads (GL-SLEASE-2; Michael's rebuild charter).
//!
//! WHAT IT IS FOR (unchanged since GL-SLEASE-1): one permit ledger for the
//! whole machine — the runtime's execution-permit semaphore accounts pool
//! workers; the lease accounts session threads into the SAME ledger, so
//! sustained serial CPU and pool parallelism cannot oversubscribe cores.
//!
//! WHY v1 DIED (GL-RO-BISECT-2): v1 acquired a permit on EVERY top-level
//! ExecutorRun and HELD IT ACROSS BLOCKING I/O. With permits =
//! available_parallelism, an OLTP posture with backends >> cores convoyed
//! 32-64 I/O-bound backends over ~4 permits (witnessed ~20pp read-only
//! C-ratio collapse; scratchpad/night/GL-RO-BISECT-2-letter.md).
//!
//! THE v2 MECHANISM — two composed rules, both at session altitude:
//!
//! 1. LAZY FLOOR ADMISSION: entering a top-level run publishes a per-thread
//!    slot (a handful of relaxed stores — no lock, no semaphore traffic)
//!    and nothing else. A background sweeper (spawned on first armed enter)
//!    scans slots at a fixed cadence; a run whose ACTIVE age — wall age
//!    minus its reported-wait time (GL-SLEASE-3: the wait-seam wrappers
//!    stamp every C-parity blocking span into the slot's blocked ledger, so
//!    an I/O-bound run is never counted "expensive" for its storage/client
//!    waits; `PGRUST_RUNTIME_SERIAL_LEASE_WALLAGE=1` restores the v2
//!    wall-age posture for A/B) — exceeds the floor
//!    (`PGRUST_RUNTIME_SERIAL_LEASE_FLOOR_MS`, default 250) gets its
//!    owner's InterruptPending raised (the timeout timer-thread pattern),
//!    and the owner acquires its permit at the next CHECK_FOR_INTERRUPTS —
//!    a canonical safe point (never inside a critical section; the
//!    ProcessInterrupts holdoff gates run first). Point/short interactive
//!    queries finish under the floor and NEVER touch the semaphore: the
//!    zero-OLTP-tax bar holds by construction, and the interactive class is
//!    deliberately ungoverned (the governed class is sustained serial CPU).
//!
//! 2. I/O DONATION: while the permit is held, every C-parity blocking wait
//!    donates it. The wait-report seam pair is the single choke point every
//!    ported blocking site already brackets its sleep with; when the lease
//!    is armed, seams_init installs wrappers that call the real reporters
//!    and then this module's hooks: wait-start with a held permit releases
//!    it (state Donated); wait-end reacquires by TRY — on failure the run
//!    continues permit-less in Deficit (never blocks while holding
//!    arbitrary locks mid-wait-path; the deadlock class v1's engagement
//!    yield dodged by construction) and the sweeper re-flags it so the next
//!    safe point re-admits with a bounded `acquire_timeout`. A permit is
//!    therefore held only while the query is actually ON CPU.
//!
//! Safe-point admission is BOUNDED (`ADMIT_TIMEOUT`): CHECK_FOR_INTERRUPTS
//! runs with heavyweight locks / buffer pins legally held, and a permit
//! holder that is not donation-registered (a pool worker outside its
//! declared spill sections) may block on exactly those — an unbounded
//! admission wait could deadlock against it. Timing out to Deficit keeps
//! the session live at a throttled duty cycle (sweeper re-flag -> bounded
//! wait -> slice of work), which is the admission semantics under
//! contention anyway.
//!
//! Umbra-model note (the architectural north star, coordinator steer): in
//! the end state serial queries run as dop-1 pool tasks, I/O suspends the
//! task rather than the thread, and this whole module is dead code. Every
//! hook here is therefore deliberately shallow: two wait-seam wrappers, one
//! ProcessInterrupts tap, one RAII pair in executor_run_seam — nothing
//! reaches deeper into the executor than the blocking points require, so
//! task-ified serial execution deletes this file and four call sites.
//!
//! DEFAULT OFF (the GL-RO-BISECT-2 re-flip posture): `armed()` keeps the
//! t35 spelling — `PGRUST_RUNTIME_SERIAL_LEASE=1` arms; anything else is
//! the unleased pre-flip posture, byte-exact (no sweeper thread, no seam
//! wrappers, no tap — seams_init installs the stock reporters).
//!
//! Structurally inert without a runtime (`runtime::global()` None — no
//! permit ledger exists to unify). Depth guard: nested executor runs (SPI,
//! triggers, RI checks) lease nothing, so re-entry never self-deadlocks on
//! the permit cap. Classic parallel-worker ExecutorRun instances are
//! top-level in their own threads and are governed like sessions (v1
//! parity): floor-gated, donation-registered, deficit-throttled — never
//! deadlocked (progress is guaranteed by the bounded admission wait).
//!
//! wasm32: no threads — the sweeper cannot exist, so an armed lease never
//! admits anyone (runs stay Pending, permit-less). Same known-limitation
//! class as the inert wasm timeout timer.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

use pgsync::{Mutex, OnceLock};

// ---------------------------------------------------------------------------
// Knobs (process-constant, OnceLock-cached — the lane-knob class).
// ---------------------------------------------------------------------------

/// The arming knob — t35 exact spelling, `=1` arms (GL-RO-BISECT-2 re-flip:
/// the unarmed default is the unleased posture, byte-exact).
pub fn armed() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SERIAL_LEASE").is_ok_and(|v| v.trim() == "1")
    })
}

/// GL-SLEASE-3 attribution arm: `PGRUST_RUNTIME_SERIAL_LEASE_SWEEPER=0`
/// keeps the armed posture but never spawns the sweeper (t35 kill spelling,
/// default ON). Armed-but-unswept = slot stores + seam wrappers only: no
/// run is ever flagged, so no admission and no permit is ever held — the
/// piece-wise separation the residual-tax ladder needs. Consulted only on
/// the armed path (unarmed boots never read it). NOT a shipping posture:
/// it silently disables the protective function.
pub fn sweeper_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SERIAL_LEASE_SWEEPER").map_or(true, |v| v.trim() != "0")
    })
}

/// GL-SLEASE-3 attribution arm: `PGRUST_RUNTIME_SERIAL_LEASE_DONATION=0`
/// keeps the armed posture (slots + sweeper + safe-point admission) but
/// leaves the wait-report seams STOCK — no donation wrappers, no IN_WAIT
/// bracketing (seams_init consults this). An admitted run then HOLDS its
/// permit across blocking waits (the v1 disease, deliberately): ladder arm
/// only, never a shipping posture. Consulted only on the armed path.
pub fn donation_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SERIAL_LEASE_DONATION").map_or(true, |v| v.trim() != "0")
    })
}

/// GL-SLEASE-3 fix A/B posture: `PGRUST_RUNTIME_SERIAL_LEASE_WALLAGE=1`
/// restores the v2 wall-age floor (t35 spelling, =1 arms; default OFF =
/// ACTIVE-age flagging — wall age minus reported-wait time, see the Slot
/// blocked ledger). Armed-path-only read; ladder/A-B posture, the shipped
/// armed default is active-age.
fn wall_age_floor() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SERIAL_LEASE_WALLAGE").is_ok_and(|v| v.trim() == "1")
    })
}

/// Admission floor in milliseconds of run age (default 250). Runs shorter
/// than this never touch the permit semaphore. `0` = admit at the first
/// sweep — the closest v2 analog of v1's admit-at-enter, for A/B ladders.
fn floor_ms() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_SERIAL_LEASE_FLOOR_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(250)
    })
}

/// Bounded safe-point admission wait (see the module doc's deadlock
/// analysis). Not a knob: correctness constant, not a tuning surface.
const ADMIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Sweep cadence: a quarter floor, clamped to [25ms, 250ms]. Detection
/// latency for a floor crossing is <= floor + cadence; Deficit re-flag
/// latency is one cadence.
fn sweep_period() -> Duration {
    Duration::from_millis((floor_ms() / 4).clamp(25, 250))
}

// ---------------------------------------------------------------------------
// Monotonic run clock (pg_clock, the one monotonic authority — DST-routed).
// ---------------------------------------------------------------------------

fn now_ns() -> u64 {
    // max(1): 0 stays reserved as the slot's "no run" sentinel even for a
    // read in the clock's first nanosecond. Nanosecond resolution because
    // the blocked-time ledger (GL-SLEASE-3 active-age floor) accumulates
    // µs-scale storage-read spans — ms granularity would round them to 0.
    pg_clock::mono_ns().max(1)
}

// ---------------------------------------------------------------------------
// Per-session slot: the sweeper's only view of a session. All fields are
// ADVISORY mirrors of the session-thread-local authority below — a missed
// or stale read costs one sweep of latency, never correctness (the session
// clears stale admit flags; the sweeper re-derives everything each pass).
// ---------------------------------------------------------------------------

const S_IDLE: u8 = 0;
/// Armed run under the floor: no permit, sweeper watches the age.
const S_PENDING: u8 = 1;
/// Permit held (on CPU).
const S_HELD: u8 = 2;
/// Permit donated across a C-parity blocking wait span.
const S_DONATED: u8 = 3;
/// Permit released across a pool-engagement wait (the v1 yield span).
const S_YIELDED: u8 = 4;
/// Floor-crossed but permit-less (failed try/timed-out admission); the
/// sweeper re-flags at cadence.
const S_DEFICIT: u8 = 5;

struct Slot {
    /// `now_ns()` at top-level run start; 0 = no active run.
    started_ns: AtomicU64,
    /// Mirror of the session's TLS state (advisory; see above).
    state: AtomicU8,
    /// Sweeper -> session: acquire (or retry) at the next safe point.
    admit: AtomicBool,
    /// The owner's CFI flag (`init_small::globals::interrupt_pending_flag`,
    /// Box::leak'd — never dangles; timer-thread precedent). Re-pointed on
    /// slot reuse before the slot is re-published.
    interrupt_flag: AtomicPtr<AtomicBool>,
    /// GL-SLEASE-3 active-age floor: cumulative ns this run spent inside
    /// C-parity reported wait spans (owner-written via the armed wait-seam
    /// wrappers, sweeper-read; reset at enter). The sweeper's floor compares
    /// ACTIVE age = wall age − blocked − the open span, so an I/O-bound run
    /// is never flagged "expensive" for its storage/client waits — the
    /// governed quantity is CPU demand (on-CPU + runq), the design intent.
    /// Wall-age flagging (the v2 behavior) is the WALLAGE=1 A/B posture.
    blocked_ns: AtomicU64,
    /// `now_ns()` at the currently-open reported wait span's start; 0 = no
    /// open span. The sweeper counts the open span as blocked (a session
    /// mid-NVMe-read or mid-client-wait is not accruing active age).
    wait_open_ns: AtomicU64,
}

/// All slots ever created (sweeper scan set) + the reuse freelist (slots
/// whose owning thread exited). Bounded by peak concurrent session threads.
struct Registry {
    all: Mutex<Vec<&'static Slot>>,
    free: Mutex<Vec<&'static Slot>>,
}

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| Registry { all: Mutex::new(Vec::new()), free: Mutex::new(Vec::new()) })
}

/// TLS slot ownership: claims on first armed enter, returns the slot to the
/// freelist at thread exit (the Drop).
struct SlotOwner(&'static Slot);

impl Drop for SlotOwner {
    fn drop(&mut self) {
        self.0.started_ns.store(0, Ordering::Relaxed);
        self.0.state.store(S_IDLE, Ordering::Relaxed);
        self.0.admit.store(false, Ordering::Relaxed);
        self.0.blocked_ns.store(0, Ordering::Relaxed);
        self.0.wait_open_ns.store(0, Ordering::Relaxed);
        let mut free = pgsync::lock(&registry().free);
        free.push(self.0);
    }
}

thread_local! {
    /// Top-level-run depth (v1's guard, verbatim semantics): non-session
    /// TLS, unwound with the executor frames, never carries across queries.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// The session-thread authority for the lease state machine. The slot's
    /// `state` field mirrors it for the sweeper.
    static STATE: Cell<u8> = const { Cell::new(S_IDLE) };
    /// The runtime whose ledger this thread's lease accounts into (cached
    /// at enter; None outside an armed leased run).
    static LEASE_RT: Cell<Option<&'static std::sync::Arc<runtime::Runtime>>> =
        const { Cell::new(None) };
    /// This thread's claimed slot (owner guard lives here for the Drop).
    static SLOT: std::cell::RefCell<Option<SlotOwner>> = const { std::cell::RefCell::new(None) };
    /// True between the armed wait-seam wrappers' start and end calls: the
    /// thread is inside a C-parity reported wait span. The admission tap
    /// DEFERS while set — some ported wait loops run ProcessInterrupts
    /// mid-wait, and taking a permit there would hold it across the
    /// remainder of the block (the v1 disease in miniature). Invariant
    /// bought: a permit is never TAKEN inside a reported wait span.
    static IN_WAIT: Cell<bool> = const { Cell::new(false) };
}

fn my_slot() -> &'static Slot {
    SLOT.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(owner) = s.as_ref() {
            return owner.0;
        }
        let slot: &'static Slot = {
            let popped = pgsync::lock(&registry().free).pop();
            match popped {
                Some(slot) => slot,
                None => {
                    let slot: &'static Slot = Box::leak(Box::new(Slot {
                        started_ns: AtomicU64::new(0),
                        state: AtomicU8::new(S_IDLE),
                        admit: AtomicBool::new(false),
                        interrupt_flag: AtomicPtr::new(std::ptr::null_mut()),
                        blocked_ns: AtomicU64::new(0),
                        wait_open_ns: AtomicU64::new(0),
                    }));
                    pgsync::lock(&registry().all).push(slot);
                    slot
                }
            }
        };
        // Point the slot at THIS thread's CFI flag before any state can
        // publish it to the sweeper (started_ns is still 0 here).
        let flag: &'static AtomicBool = init_small::globals::interrupt_pending_flag();
        slot.interrupt_flag
            .store(flag as *const AtomicBool as *mut AtomicBool, Ordering::Release);
        *s = Some(SlotOwner(slot));
        slot
    })
}

/// One TLS-state + slot-mirror transition (session thread only).
fn set_state(slot: &Slot, s: u8) {
    STATE.set(s);
    slot.state.store(s, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// The sweeper (armed posture only; spawned once, immortal like the timeout
// timer thread; parks on a private Condvar deadline — hook-routed in every
// pgsync world).
// ---------------------------------------------------------------------------

fn ensure_sweeper() {
    // GL-SLEASE-3 ladder arm: armed-but-unswept (see sweeper_enabled).
    if !sweeper_enabled() {
        return;
    }
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        #[cfg(not(target_family = "wasm"))]
        pgsync::thread::Builder::new()
            .name("pg-slease-sweeper".into())
            .spawn(sweeper)
            .expect("could not spawn serial-lease sweeper thread");
    });
}

#[cfg(not(target_family = "wasm"))]
fn sweeper() -> ! {
    let floor_ns = floor_ms().saturating_mul(1_000_000);
    let wall_age = wall_age_floor();
    loop {
        // Pure cadence park (nothing ever unparks us; a lease crossing
        // detected one sweep late is within the design bound). waiter is
        // the sanctioned park surface — a registered thread's timed park
        // drives virtual time in the DST worlds (timer-thread precedent).
        let _ = waiter::park_timeout(sweep_period());
        let now = now_ns();
        let slots = pgsync::lock(&registry().all);
        for slot in slots.iter() {
            let started = slot.started_ns.load(Ordering::Acquire);
            if started == 0 {
                continue;
            }
            let state = slot.state.load(Ordering::Relaxed);
            let flag_it = match state {
                S_PENDING => {
                    // GL-SLEASE-3 active-age floor (the fix): the governed
                    // quantity is CPU DEMAND, so reported-wait time — the
                    // closed spans' ledger plus the currently-open span —
                    // never counts toward "expensive". An I/O-bound run
                    // that spends its wall clock in storage/client waits
                    // stays unflagged; the v1-shaped wall-age posture
                    // remains as the WALLAGE=1 A/B arm. All reads Relaxed
                    // and owner-raced by design: a torn read skews one
                    // sweep, and the admission it could mis-trigger is the
                    // bounded safe-point acquire — never correctness.
                    let mut age = now.saturating_sub(started);
                    if !wall_age {
                        let blocked = slot.blocked_ns.load(Ordering::Relaxed);
                        let open = slot.wait_open_ns.load(Ordering::Relaxed);
                        let open_extra =
                            if open != 0 { now.saturating_sub(open) } else { 0 };
                        age = age.saturating_sub(blocked.saturating_add(open_extra));
                    }
                    age >= floor_ns
                }
                // Re-admission retry cadence for floor-crossed permit-less
                // runs (see the module doc's bounded-admission story).
                S_DEFICIT => true,
                _ => false,
            };
            if flag_it {
                // Re-flag AND re-raise every sweep until the session
                // transitions (the tap defers inside wait spans and the
                // interrupt flag is consumed by every ProcessInterrupts —
                // a one-shot raise could strand an un-actioned admit).
                // swap (not store) feeds the floor-crossing census: a
                // Pending slot flagged from false = exactly one run newly
                // crossing the floor (Pending leaves the state exactly once
                // per run; Deficit re-flags don't re-count). GL-SLEASE-3's
                // admission-census witness — zero-cost when the lanev2
                // stats arm is off (the tick is armed-gated).
                let newly = !slot.admit.swap(true, Ordering::SeqCst);
                if newly && state == S_PENDING {
                    crate::lanev2::tick_serial_lease_floor_crossing();
                }
                // Raise the owner's CFI flag (timer-thread posting parity).
                // No latch wake on purpose: a BLOCKED session needs no
                // permit; it processes the flag at its first safe point
                // after resuming. Worst case the flag is stale (the run
                // ended between our read and this store) — a spurious
                // ProcessInterrupts with no reason set is benign.
                let p = slot.interrupt_flag.load(Ordering::Acquire);
                if !p.is_null() {
                    // SAFETY: only ever null or Box::leak'd (claim path).
                    unsafe { (*p).store(true, Ordering::SeqCst) };
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RAII pair for executor_run_seam (the ONLY enter site; v1 parity).
// ---------------------------------------------------------------------------

pub(crate) struct SerialLease {
    /// True only for the depth-0 frame of an armed run with a runtime:
    /// the frame that owns slot publication and permit reconciliation.
    owner: bool,
}

impl SerialLease {
    pub(crate) fn enter() -> Option<SerialLease> {
        if !armed() {
            return None;
        }
        let depth = DEPTH.with(|c| {
            let d = c.get();
            c.set(d + 1);
            d
        });
        let owner = if depth == 0 {
            if let Some(rt) = runtime::global() {
                ensure_sweeper();
                LEASE_RT.set(Some(rt));
                let slot = my_slot();
                debug_assert_eq!(STATE.get(), S_IDLE, "lease state leaked across runs");
                set_state(slot, S_PENDING);
                // Fresh blocked ledger for this run (no wait span can be
                // open here: spans never straddle a top-level enter).
                slot.blocked_ns.store(0, Ordering::Relaxed);
                slot.wait_open_ns.store(0, Ordering::Relaxed);
                // Publish the run age LAST (Release pairs with the
                // sweeper's Acquire): a slot the sweeper can see is always
                // fully initialized.
                slot.started_ns.store(now_ns(), Ordering::Release);
                // The engagement-yield accounting witness (the
                // serial-pool-overhead gate): v2 ticks at ENTER — the lease
                // machinery engaging — not at permit acquisition, which the
                // floor makes workload-dependent.
                crate::lanev2::tick_serial_lease();
                true
            } else {
                false
            }
        } else {
            false
        };
        Some(SerialLease { owner })
    }
}

impl Drop for SerialLease {
    fn drop(&mut self) {
        if self.owner {
            let slot = my_slot();
            // Retract from the sweeper FIRST, then reconcile the permit:
            // release iff held. Donated/Yielded frames already released
            // (an error unwind between wait-start and wait-end lands here
            // with state Donated — nothing to release, nothing leaked);
            // Pending/Deficit never held.
            slot.started_ns.store(0, Ordering::Relaxed);
            slot.admit.store(false, Ordering::Relaxed);
            if STATE.get() == S_HELD {
                if let Some(rt) = LEASE_RT.get() {
                    rt.execution_permits().release();
                }
            }
            set_state(slot, S_IDLE);
            LEASE_RT.set(None);
        }
        DEPTH.with(|c| c.set(c.get() - 1));
    }
}

// ---------------------------------------------------------------------------
// Engagement yield (v1 API, kept for lanev2's standing channel): release
// the permit across the leader's pool-engagement wait span. v2 delta: the
// re-acquire on drop is BOUNDED (ADMIT_TIMEOUT) — on timeout the leader
// resumes in Deficit and the sweeper-driven safe-point loop re-admits, so
// an unwinding or wedged pool can never park the leader forever.
// ---------------------------------------------------------------------------

pub(crate) struct SerialLeaseYield {
    released: bool,
}

pub(crate) fn serial_lease_yield_for_engagement() -> SerialLeaseYield {
    let released = STATE.get() == S_HELD;
    if released {
        if let Some(rt) = LEASE_RT.get() {
            rt.execution_permits().release();
            set_state(my_slot(), S_YIELDED);
        }
    }
    SerialLeaseYield { released }
}

impl Drop for SerialLeaseYield {
    fn drop(&mut self) {
        if self.released {
            if let Some(rt) = LEASE_RT.get() {
                let slot = my_slot();
                if rt.execution_permits().acquire_timeout(ADMIT_TIMEOUT) {
                    set_state(slot, S_HELD);
                } else {
                    set_state(slot, S_DEFICIT);
                }
            }
        }
    }
}

/// GL-STMTTASK-2 composition bridge (t44): true ⇔ THIS thread's lease state
/// machine currently HOLDS a permit (S_HELD). The stmt-task inline-execute
/// path consults this so a lease-admitted session never borrows a SECOND
/// pool seat. Floor-pending / donated / yielded spans read false — during a
/// donation or yield the permit is exactly NOT held, and a sub-floor run is
/// deliberately un-accounted under the v2 floor economics (the v1 TLS this
/// replaces answered the same question for the always-held v1 lease).
pub(crate) fn serial_lease_currently_held() -> bool {
    STATE.get() == S_HELD
}

// ---------------------------------------------------------------------------
// Donation hooks — called by the armed wait-report seam wrappers
// (seams_init) on EVERY thread that reports waits; non-session threads and
// idle sessions fall through on one TLS load + branch. In-run threads
// additionally stamp the span into the slot's blocked ledger (GL-SLEASE-3
// active-age floor) — one mono clock read + a few relaxed slot ops per
// reported span, paid only while a top-level run is live on the thread.
// ---------------------------------------------------------------------------

/// Wait span opens: a held permit is donated for the span, and (GL-SLEASE-3
/// active-age floor) an in-run thread stamps the span start into its slot's
/// blocked ledger. Idle threads (non-session, or a session between runs)
/// still fall through on one TLS load + branch — the stamping cost lands
/// only on in-run reported waits, which are exactly the spans the ledger
/// must subtract from the floor.
pub fn wait_hook_start() {
    IN_WAIT.set(true);
    let state = STATE.get();
    if state == S_IDLE {
        return;
    }
    let slot = my_slot();
    let now = now_ns();
    // Tolerate an unbalanced double-start (C-parity sites may overwrite an
    // open report): close the stale span into the ledger first.
    let prev_open = slot.wait_open_ns.load(Ordering::Relaxed);
    if prev_open != 0 {
        let span = now.saturating_sub(prev_open);
        slot.blocked_ns
            .store(slot.blocked_ns.load(Ordering::Relaxed).saturating_add(span), Ordering::Relaxed);
    }
    slot.wait_open_ns.store(now, Ordering::Relaxed);
    if state == S_HELD {
        if let Some(rt) = LEASE_RT.get() {
            rt.execution_permits().release();
            set_state(slot, S_DONATED);
            crate::lanev2::tick_serial_lease_donation();
        }
    }
}

/// Wait span closes: TRY to take the permit back; never block here — the
/// wait paths run under arbitrary lock state (module doc, deadlock class).
pub fn wait_hook_end() {
    IN_WAIT.set(false);
    let state = STATE.get();
    if state == S_IDLE {
        return;
    }
    let slot = my_slot();
    // Close the span into the blocked ledger (double-end = no-op: the open
    // stamp was consumed by the first close).
    let open = slot.wait_open_ns.load(Ordering::Relaxed);
    if open != 0 {
        let span = now_ns().saturating_sub(open);
        slot.blocked_ns
            .store(slot.blocked_ns.load(Ordering::Relaxed).saturating_add(span), Ordering::Relaxed);
        slot.wait_open_ns.store(0, Ordering::Relaxed);
    }
    if state == S_DONATED {
        if let Some(rt) = LEASE_RT.get() {
            if rt.execution_permits().try_acquire() {
                set_state(slot, S_HELD);
            } else {
                set_state(slot, S_DEFICIT);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Safe-point admission — the ProcessInterrupts tap (installed by seams_init
// when armed). Runs after the holdoff/crit-section gates, on the canonical
// interrupt-safe points only.
// ---------------------------------------------------------------------------

pub fn admission_tap() {
    let state = STATE.get();
    if state == S_IDLE {
        // Between runs (or a non-session thread): nothing to admit; a
        // sweeper flag that raced the run's end is cleared lazily by the
        // next enter's slot reset — nothing to do here (this thread may
        // not even own a slot).
        return;
    }
    if IN_WAIT.get() {
        // Mid-wait-span ProcessInterrupts: DEFER (see IN_WAIT). The admit
        // flag stays set and the sweeper re-raises next pass.
        return;
    }
    let slot = my_slot();
    if !slot.admit.swap(false, Ordering::SeqCst) {
        return;
    }
    match state {
        S_PENDING | S_DEFICIT => {
            if let Some(rt) = LEASE_RT.get() {
                if rt.execution_permits().acquire_timeout(ADMIT_TIMEOUT) {
                    set_state(slot, S_HELD);
                    crate::lanev2::tick_serial_lease_admitted();
                } else {
                    set_state(slot, S_DEFICIT);
                }
            }
        }
        // Held/Donated/Yielded: the flag was stale (raised before our own
        // transition raced it) — cleared above, nothing to admit.
        _ => {}
    }
}
