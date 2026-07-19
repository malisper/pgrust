//! The permit scheduler core (contract §3.1; design §1) — the
//! [`SchedulerHooks`] implementation behind the pinned `pgsync::sim::hooks`
//! seam.
//!
//! ONE global permit; exactly one runnable thread at any moment; a thread
//! runs from permit receipt to its next pgsync touch. Mandatory handoff on
//! would-block; optional seeded preemption at non-blocking touches
//! (`PGRUST_SIM_PREEMPT_P`); sleeps/timeouts become `TimedPark` on virtual
//! time, which advances ONLY when nothing is runnable, to the earliest
//! deadline (P3 §2.4 verbatim). Deterministic deadlock (all live slots
//! parked in shims, no timed sleeper) is an immediate pick-fail report of
//! all slots + seed — NOT a watchdog case.
//!
//! Seeding: the picker stream is splitmix64 over a seed that is one 8-byte
//! fill drawn through `pg_strong_random` — the sanctioned SimEntropy funnel
//! keyed by `(PGRUST_SIM_SEED, fill_no)` — so every OS-arbitrary choice is
//! a deterministic function of the master seed.
//!
//! Picking (PERMIT-S2 step 1): the GLOBAL scheduler defaults to PCT —
//! Burckhardt, Kothari, Musuvathi, Nagarakatte, "A Randomized Scheduler
//! with Probabilistic Guarantees of Finding Bugs" (ASPLOS 2010): every
//! thread gets a seeded random priority ≥ d at registration; d−1 seeded
//! priority-change points (step index, target priority d−1…1) are drawn per
//! run over an estimated step budget k; the scheduler always runs the
//! highest-priority RUNNABLE slot, and when the running slot's step counter
//! crosses a change point its priority drops to that point's value. For a
//! bug of depth d over n threads and k steps this finds the bug with
//! probability ≥ 1/(n·k^(d−1)) PER RUN — the guarantee uniform random
//! walks lack. Adaptation notes (granularity honest, ledgered in
//! notes/permit-s2.md): a "step" is a pgsync interception (touch / park),
//! not an instruction; `pick_waiter` wait-list choices stay seeded-uniform
//! (the wrapper passes only `n`, not identities); `Spawn` touches never
//! count as steps nor preempt (the spawn fence's invariant, step-1 shape).
//! `PGRUST_SIM_SCHED_ALGO=uniform` restores the step-1 uniform pick +
//! `PGRUST_SIM_PREEMPT_P` coin; instances default to Uniform so the step-1
//! unit battery drives unchanged schedules.
//!
//! Instance discipline: the scheduler is instance-based (unit batteries
//! construct private instances with private virtual clocks so parallel
//! tests never share state); the product wiring is the ONE lazy global
//! ([`global`]) built from the sim knobs, driving the process-wide SimClock
//! (`pg_clock::sim::advance_ns`, driven mode), watched by THE watchdog, and
//! installed into the hooks seam through [`Router`] (which routes every
//! wrapper op by the CALLING THREAD's TLS registration — the same router
//! serves unit instances).
//!
//! The wrapper-protocol note the trait cannot say for us (ledgered in the
//! worklog as a CORE→SYNC coordination row): `pgsync::thread` spawns
//! register CHILD-side, so the OS could delay a child's registration past
//! its parent's next handoff and fork the schedule on wall luck. The
//! scheduler closes that hole with the SPAWN FENCE: `touch(_, Spawn)`
//! (parent, right after the OS spawn) wall-waits until the child's
//! `spawn()` registration has landed. The wait is outside the model (no
//! SCHEDOP line, no virtual time) and bounded by the child's first
//! statement.
//!
//! TLS-teardown rules (P3 §1.6 as adapted by design §3):
//!  1. quantum-scoped teardown — the spawn-door epilogue runs every
//!     registered teardown hook (waiter::retire_current-class) inside the
//!     exiting thread's FINAL quantum, before deregister
//!     ([`Scheduler::exit_cur`]);
//!  2. sim waiter-slot identity is keyed by vpid (exposed via
//!     [`current_vpid`]), never by SLOT_FREE pop order;
//!  3. join wakes on deregister: exit makes every Join-parked slot runnable
//!     (joiners re-check their predicate — spurious-safe per the wrapper
//!     protocol), and the wake post-dates all shared teardown.

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::Location;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

use super::config;
use super::hooks::{self, OpClass, SchedulerHooks, Vpid};
use super::schedlog::{SchedLog, DEFAULT_LOG_CAP};
use super::watchdog;

/// [`SchedulerHooks::current_vpid`] sentinel for threads outside the model
/// (the trait returns a bare `Vpid`). Wait-list entries carrying it are
/// never woken by name — an unregistered thread's parks are OS-scheduled,
/// exactly as before the scheduler existed.
pub const UNREGISTERED_VPID: Vpid = Vpid::MAX;

/// A slot handle returned by [`Scheduler::register`]; slots are append-only
/// for the life of a scheduler (identity is the vpid, rule 2 — an exited
/// slot is never resurrected or re-keyed).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotId(pub(crate) usize);

/// What a deterministic failure (deadlock / virtual-time ceiling) does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailAction {
    /// Panic with the report (instance/unit default — assertable).
    Panic,
    /// Dump the report to stderr and abort the process (global default).
    DumpAbort,
}

/// Where the watchdog dump goes (see `watchdog.rs`).
#[derive(Clone)]
pub enum WatchdogSink {
    /// Dump to stderr and abort (global default; contract §3.2).
    AbortProcess,
    /// Store the dump for assertion (the G3 red-unit sink).
    Capture(Arc<Mutex<Option<String>>>),
}

/// Which virtual clock a scheduler drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockMode {
    /// Instance-local counter (unit batteries — parallel tests never share
    /// clock state, so SCHEDOP time fields stay byte-deterministic).
    Instance,
    /// The process-wide SimClock: `pg_clock::mono_ns()` /
    /// `pg_clock::sim::advance_ns` (driven mode — "the P3 scheduler mode";
    /// the global scheduler owns the advance lever).
    PgClock,
}

/// Which picker drives handoffs (module docs "Picking").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PickAlgo {
    /// Step-1 shape: seeded uniform pick over Runnable + the
    /// `preempt_p` coin at non-blocking touches.
    Uniform,
    /// PCT (Burckhardt et al., ASPLOS 2010): random per-thread priorities
    /// ≥ `depth`, `depth − 1` seeded priority-change points over an
    /// estimated `steps` budget, highest-priority-runnable wins every
    /// handoff. Per-run bug-find probability ≥ 1/(n·kᵈ⁻¹) for depth-d bugs.
    Pct {
        /// Bug depth d (number of ordering constraints; d−1 change points).
        depth: u32,
        /// Estimated step budget k the change points are drawn over.
        steps: u64,
    },
}

pub struct SchedulerConfig {
    /// Picker-stream seed. The global scheduler draws this through the
    /// SimEntropy funnel; instances pin it explicitly.
    pub seed: u64,
    /// Handoff picker. Global default: PCT (knobs `PGRUST_SIM_SCHED_ALGO` /
    /// `PGRUST_SIM_PCT_D` / `PGRUST_SIM_PCT_K`); instance/unit default:
    /// Uniform (the step-1 battery's schedules are pinned to it).
    pub algo: PickAlgo,
    /// Seeded-preemption probability at touches; 0.0 disables (and draws
    /// nothing, keeping the stream stable for pick-only corpora).
    /// Uniform-only: PCT preemption is priority-driven, never a coin.
    pub preempt_p: f64,
    pub log_cap: usize,
    pub stream_log: bool,
    /// Plain run bound on virtual time (design §4: "a config line, not a
    /// detector") — catches never-satisfied-predicate advance loops.
    pub virtual_ceiling_ns: Option<u64>,
    pub clock: ClockMode,
    pub fail: FailAction,
    pub watchdog_timeout_ms: u64,
    pub watchdog_poll_ms: u64,
    pub watchdog_sink: WatchdogSink,
}

impl Default for SchedulerConfig {
    /// Instance/unit defaults: deterministic, panicky, preemption off.
    fn default() -> Self {
        SchedulerConfig {
            seed: 0,
            algo: PickAlgo::Uniform,
            preempt_p: 0.0,
            log_cap: DEFAULT_LOG_CAP,
            stream_log: false,
            virtual_ceiling_ns: None,
            clock: ClockMode::Instance,
            fail: FailAction::Panic,
            watchdog_timeout_ms: config::DEFAULT_WATCHDOG_S * 1000,
            watchdog_poll_ms: 250,
            watchdog_sink: WatchdogSink::AbortProcess,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
    /// Pickable: will receive the permit from some handoff.
    Runnable,
    /// Holds the permit (exactly one slot at a time).
    Running,
    /// Parked in a shim would-block op; leaves only via an explicit wake
    /// (or, for `Join` parks, any exit — predicate-loop protocol).
    Blocked(OpClass),
    /// Parked on a virtual-time deadline (absolute ns).
    TimedParked(u64),
    Exited,
}

impl SlotState {
    fn as_log(&self) -> String {
        match self {
            SlotState::Runnable => "runnable".into(),
            SlotState::Running => "running".into(),
            SlotState::Blocked(k) => format!("blocked({})", k.as_str()),
            SlotState::TimedParked(d) => format!("timedpark({d})"),
            SlotState::Exited => "exited".into(),
        }
    }
}

struct Slot {
    vpid: Vpid,
    name: String,
    state: SlotState,
    /// Permit-grant flag; the parked thread's wait predicate.
    granted: bool,
    /// TimedPark disambiguation: explicit wake vs deadline expiry.
    woken: bool,
    /// PCT priority (seeded at registration, ≥ depth; lowered when the slot
    /// crosses a priority-change point). Unused (0) under Uniform.
    prio: u64,
    /// The last shim event — what the watchdog dump NAMES symbolically
    /// (op-class + `#[track_caller]` Location of the pgsync public op).
    last_event: Option<(&'static str, &'static Location<'static>)>,
}

struct Inner {
    slots: Vec<Slot>,
    by_vpid: HashMap<Vpid, usize>,
    permit_held: bool,
    rng: u64,
    /// Instance-mode virtual now (ns). PgClock mode reads pg_clock instead.
    vnow_ns: u64,
    log: SchedLog,
    /// The spawn fence (see module docs): `touch(Spawn)` bumps
    /// `children_spawned` and wall-waits `children_registered` to catch up.
    children_spawned: u64,
    children_registered: u64,
    /// PCT state: executed-step counter (touches + parks of registered
    /// threads; Spawn excluded), the seeded change points sorted ascending
    /// by step `(step, target_prio)`, and the low-water index into them.
    pct_step: u64,
    pct_cps: Vec<(u64, u64)>,
    pct_next_cp: usize,
}

pub struct Scheduler {
    pub(crate) cfg: SchedulerConfig,
    inner: Mutex<Inner>,
    cv: Condvar,
    /// Handoff heartbeat seq (watchdog food; contract §3.2). The wall stamp
    /// twin was dropped at the F7 ledger row (written, never read — the
    /// watchdog keeps its own `last_change_ms`).
    pub(crate) heartbeat_seq: AtomicU64,
    /// Teardown hooks run in the exiting thread's final quantum (rule 1).
    teardown: Mutex<Vec<Arc<dyn Fn(Vpid) + Send + Sync>>>,
    /// Synthetic-vpid dispenser for [`Scheduler::register_self`]
    /// (main/client-driver/unit threads): `0xC000_0000..` — disjoint from
    /// reserve_child_pid's positive pid range AND from `pgsync::thread`'s
    /// wrapper range (`0x8000_0000..`, allocated in sim_world.rs).
    synth_vpid: AtomicU32,
}

// --- TLS: the calling thread's registration -------------------------------

struct Current {
    sched: Arc<Scheduler>,
    idx: usize,
    vpid: Vpid,
}

thread_local! {
    static CURRENT: RefCell<Option<Current>> = const { RefCell::new(None) };
}

pub(crate) fn current_vpid() -> Option<Vpid> {
    CURRENT.with(|c| c.borrow().as_ref().map(|c| c.vpid))
}

fn current_sched() -> Option<(Arc<Scheduler>, usize, Vpid)> {
    CURRENT.with(|c| {
        c.borrow()
            .as_ref()
            .map(|c| (c.sched.clone(), c.idx, c.vpid))
    })
}

/// The strict shared-universe permit probe (see `sim::current_thread_holds_
/// permit` for the contract). Reads the slot's `granted` flag under the
/// state lock — sim-only diagnostic cost, immaterial (the F6 precedent).
pub(crate) fn current_thread_holds_permit() -> bool {
    if global().is_none() {
        return false;
    }
    match current_sched() {
        None => false,
        Some((s, idx, _)) => plock(&s.inner).slots[idx].granted,
    }
}

/// Rename the CALLING thread's live slot to `new_vpid` — the wpool retention
/// shape (F2): a retained standby serves every task under a fresh synthetic
/// pid, and sim waiter-slot identity keys off `current_vpid`, so the model
/// identity must follow MyProcPid or rule 2 skews. NOT a scheduling event:
/// no step, no handoff, the PCT priority carries. The old vpid retires with
/// the rename — safe because `reserve_child_pid` never re-issues pids, and
/// no wait list can still carry the old vpid (this thread is RUNNING, and
/// wait-list entries are enqueued by the waiter itself). No-op when the
/// thread is unregistered (scheduler off).
#[track_caller]
pub(crate) fn rekey_current(new_vpid: Vpid) {
    let site = Location::caller();
    if let Some((s, idx, old)) = current_sched() {
        if old == new_vpid {
            return;
        }
        {
            let mut inner = plock(&s.inner);
            assert!(
                !inner.by_vpid.contains_key(&new_vpid),
                "pgsync::sim: rekey to a live vpid {new_vpid} (slot identity is keyed by vpid)"
            );
            inner.by_vpid.remove(&old);
            inner.by_vpid.insert(new_vpid, idx);
            inner.slots[idx].vpid = new_vpid;
            inner.log.emit(
                Some(old),
                "Rekey",
                Some(site),
                &[("new", new_vpid.to_string())],
            );
        }
        CURRENT.with(|c| {
            if let Some(cur) = c.borrow_mut().as_mut() {
                cur.vpid = new_vpid;
            }
        });
    }
}

/// Poison-tolerant acquire — the repo discipline (runtime/sync.rs law).
fn plock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// splitmix64 (Steele/Lea/Flood, JPDC 2014) — the picker stream. Same
/// generator family as SimEntropy's fill stream; private copy because the
/// funnel's internals are deliberately not exported.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Scheduler {
    pub fn new(cfg: SchedulerConfig) -> Arc<Scheduler> {
        let mut rng = cfg.seed;
        // PCT: draw the d−1 priority-change points up front (ASPLOS 2010
        // §3: change point i targets priority d−i, at a uniformly random
        // step in [1, k]). Drawn FIRST so the stream position of every
        // later draw (slot priorities) is a pure function of the seed.
        let mut pct_cps: Vec<(u64, u64)> = Vec::new();
        if let PickAlgo::Pct { depth, steps } = cfg.algo {
            let k = steps.max(1);
            for i in 1..depth.max(1) as u64 {
                let step = 1 + splitmix64(&mut rng) % k;
                pct_cps.push((step, depth.max(1) as u64 - i));
            }
            pct_cps.sort_unstable();
        }
        let log = SchedLog::new(cfg.log_cap, cfg.stream_log);
        Arc::new(Scheduler {
            cfg,
            inner: Mutex::new(Inner {
                slots: Vec::new(),
                by_vpid: HashMap::new(),
                permit_held: false,
                rng,
                vnow_ns: 0,
                log,
                children_spawned: 0,
                children_registered: 0,
                pct_step: 0,
                pct_cps,
                pct_next_cp: 0,
            }),
            cv: Condvar::new(),
            heartbeat_seq: AtomicU64::new(0),
            teardown: Mutex::new(Vec::new()),
            synth_vpid: AtomicU32::new(0),
        })
    }

    // --- registration (the spawn door, contract §3.1) ----------------------

    /// Create a slot for `vpid`. Parent-side callers (the launch_backend
    /// spawn door) do this BEFORE the OS spawn so the slot exists — and its
    /// SCHEDOP position is program-ordered — before any child action. The
    /// new slot is Runnable; if the permit is idle a handoff runs
    /// immediately (bootstrap).
    ///
    /// Panics on a duplicate vpid: slot identity is the vpid (rule 2), so a
    /// second registration of a live OR exited vpid is a caller bug.
    #[track_caller]
    pub fn register(&self, vpid: Vpid, name: &str) -> SlotId {
        self.register_at(Location::caller(), vpid, name, false)
    }

    fn register_at(
        &self,
        site: &'static Location<'static>,
        vpid: Vpid,
        name: &str,
        fence_credit: bool,
    ) -> SlotId {
        let mut inner = plock(&self.inner);
        assert!(
            !inner.by_vpid.contains_key(&vpid),
            "pgsync::sim: duplicate vpid {vpid} registration (slot identity is keyed by vpid)"
        );
        let idx = inner.slots.len();
        // PCT: fresh seeded priority ≥ depth (the change-point targets
        // d−1…1 stay strictly below every initial priority; distinctness
        // is near-certain over the 2^32 span and ties break by slot index,
        // deterministically). Uniform burns no draw here (stream shape of
        // step-1 preserved).
        let prio = match self.cfg.algo {
            PickAlgo::Uniform => 0,
            PickAlgo::Pct { depth, .. } => depth as u64 + (splitmix64(&mut inner.rng) >> 32),
        };
        inner.slots.push(Slot {
            vpid,
            name: name.to_string(),
            state: SlotState::Runnable,
            granted: false,
            woken: false,
            prio,
            last_event: None,
        });
        inner.by_vpid.insert(vpid, idx);
        if fence_credit {
            inner.children_registered += 1;
        }
        let actor = current_vpid();
        inner.log.emit(
            actor,
            "Spawn",
            Some(site),
            &[("child", vpid.to_string()), ("name", name.to_string())],
        );
        if !inner.permit_held {
            self.handoff_locked(&mut inner, actor);
        }
        drop(inner);
        // Fence credit and bootstrap grants both need sleepers re-checked.
        self.cv.notify_all();
        SlotId(idx)
    }

    /// Child-side prologue: bind the calling thread to its slot and park
    /// until the first grant. The Enter line is emitted AFTER the grant —
    /// inside the slot's first quantum — so its seq position is
    /// schedule-determined, not OS-start-order-determined.
    #[track_caller]
    pub fn enter(self: &Arc<Self>, slot: SlotId) {
        let site = Location::caller();
        self.enter_at(site, slot);
    }

    fn enter_at(self: &Arc<Self>, site: &'static Location<'static>, slot: SlotId) {
        let vpid = {
            let inner = plock(&self.inner);
            inner.slots[slot.0].vpid
        };
        CURRENT.with(|c| {
            let mut cur = c.borrow_mut();
            // permit-s2 review NB-4 / W-S2-1 "one door per thread", now an
            // ASSERT (permit-s4): a thread entering a second door used to
            // silently overwrite its binding and self-wedge into a 30s
            // watchdog dump; this turns it into an immediate symbolic panic
            // naming both bindings. register_self routes through here too.
            if let Some(prev) = cur.as_ref() {
                panic!(
                    "pgsync sim scheduler: thread entered a second door \
                     (one-door law, W-S2-1): already bound to vpid {:#x}, \
                     now entering as vpid {vpid:#x} at {site}",
                    prev.vpid
                );
            }
            *cur = Some(Current {
                sched: self.clone(),
                idx: slot.0,
                vpid,
            })
        });
        {
            let mut inner = plock(&self.inner);
            if !inner.permit_held {
                self.handoff_locked(&mut inner, Some(vpid));
            }
        }
        self.wait_for_grant(slot.0);
        let mut inner = plock(&self.inner);
        inner.slots[slot.0].last_event = Some(("enter", site));
        inner.log.emit(Some(vpid), "Enter", Some(site), &[]);
    }

    /// register + enter in one call, with a synthetic vpid — the
    /// main/client-driver door. Returns the vpid.
    #[track_caller]
    pub fn register_self(self: &Arc<Self>, name: &str) -> Vpid {
        let vpid = 0xC000_0000u32 + self.synth_vpid.fetch_add(1, Ordering::Relaxed);
        let slot = self.register(vpid, name);
        self.enter(slot);
        vpid
    }

    /// The trait-`spawn` path (`pgsync::thread` wrappers register
    /// CHILD-side): register with spawn-fence credit, then bind + gate.
    fn spawn_child_and_enter(
        self: &Arc<Self>,
        vpid: Vpid,
        site: &'static Location<'static>,
    ) {
        let name = format!("pgsync:thread:{vpid:#x}");
        let slot = self.register_at(site, vpid, &name, true);
        self.enter_at(site, slot);
    }

    // --- the interception surface (instance side of the Router) ------------

    /// PCT step accounting (module docs "Picking"): every non-Spawn
    /// interception by the running thread is one executed step; a
    /// priority-change point whose step is reached drops the RUNNING
    /// slot's priority to the point's target. No-op under Uniform.
    fn pct_step_locked(&self, inner: &mut Inner, idx: usize) {
        if !matches!(self.cfg.algo, PickAlgo::Pct { .. }) {
            return;
        }
        inner.pct_step += 1;
        while inner.pct_next_cp < inner.pct_cps.len()
            && inner.pct_cps[inner.pct_next_cp].0 <= inner.pct_step
        {
            let (_, target) = inner.pct_cps[inner.pct_next_cp];
            inner.pct_next_cp += 1;
            inner.slots[idx].prio = target;
        }
    }

    /// Mandatory handoff on would-block (contract §3.1). The calling thread
    /// must hold the permit; it parks until an explicit wake makes it
    /// Runnable and a later handoff picks it.
    fn block_on_slot(&self, idx: usize, site: &'static Location<'static>, kind: OpClass) {
        {
            let mut inner = plock(&self.inner);
            self.pct_step_locked(&mut inner, idx);
            let slot = &mut inner.slots[idx];
            // Real assert (F6 ledger row): a wrapper-protocol violation must
            // fail loudly in every profile — sim-only code, cost immaterial.
            assert!(
                slot.granted,
                "pgsync::sim: block_on from a thread that does not hold the permit"
            );
            let vpid = slot.vpid;
            slot.state = SlotState::Blocked(kind);
            slot.granted = false;
            slot.woken = false;
            slot.last_event = Some((kind.as_str(), site));
            inner.log.emit(
                Some(vpid),
                "Block",
                Some(site),
                &[("kind", kind.as_str().to_string())],
            );
            self.handoff_locked(&mut inner, Some(vpid));
        }
        self.wait_for_grant(idx);
    }

    /// Make `target` Runnable (wrapper-side wakee delivery: the wrapper owns
    /// its wait list, picks the wakee via [`Scheduler::pick_waiter_slot`],
    /// and delivers the wake here). Does NOT transfer the permit.
    fn wake_vpid(&self, target: Vpid, site: &'static Location<'static>) {
        let mut inner = plock(&self.inner);
        let actor = current_vpid();
        let prev = match inner.by_vpid.get(&target).copied() {
            Some(t) => {
                let prev = inner.slots[t].state.as_log();
                match inner.slots[t].state {
                    SlotState::Blocked(_) | SlotState::TimedParked(_) => {
                        inner.slots[t].state = SlotState::Runnable;
                        inner.slots[t].woken = true;
                    }
                    _ => {}
                }
                prev
            }
            None => "unknown".into(),
        };
        inner.log.emit(
            actor,
            "Wake",
            Some(site),
            &[("target", target.to_string()), ("prev", prev)],
        );
        if !inner.permit_held {
            self.handoff_locked(&mut inner, actor);
        }
    }

    /// Optional-preemption point at a non-blocking touch (unlock / notify /
    /// send / spawn). Uniform draws one preempt_p coin per touch whenever
    /// preemption is enabled, so the pick stream is a stable function of
    /// the schedule; PCT preempts exactly when a change point (or an
    /// earlier wake/spawn) leaves a strictly-higher-priority slot Runnable.
    /// `OpClass::Spawn` touches additionally run the spawn fence (module
    /// docs) BEFORE the draw, and never preempt themselves (nor count as a
    /// PCT step) — the fence invariant, step-1 shape.
    /// SPAWN FENCE (module docs): wall-wait (outside the model) until every
    /// OS-spawned wrapper child's registration has landed, so the slot set
    /// at the next handoff is schedule-determined, not wall-luck. Runs on
    /// the GLOBAL scheduler regardless of the toucher's TLS binding —
    /// wrapper-spawn registration always lands there (`Router::spawn`), so
    /// the spawned-side bump must too or the counters diverge (F1: an
    /// off-model spawner left a permanent surplus that made every later
    /// fence pass vacuously; F4: an instance-bound spawner fence-waited on
    /// its instance for a registration the global received).
    fn spawn_fence(&self) {
        let mut inner = plock(&self.inner);
        inner.children_spawned += 1;
        while inner.children_registered < inner.children_spawned {
            inner = self.cv.wait(inner).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn touch_slot(&self, idx: usize, site: &'static Location<'static>, kind: OpClass) {
        let preempt = {
            let mut inner = plock(&self.inner);
            if kind != OpClass::Spawn {
                self.pct_step_locked(&mut inner, idx);
            }
            let vpid = inner.slots[idx].vpid;
            inner.slots[idx].last_event = Some((kind.as_str(), site));
            let preempt = if kind == OpClass::Spawn {
                false
            } else {
                match self.cfg.algo {
                    PickAlgo::Uniform => {
                        if self.cfg.preempt_p > 0.0 {
                            let d = splitmix64(&mut inner.rng);
                            ((d >> 11) as f64 / (1u64 << 53) as f64) < self.cfg.preempt_p
                        } else {
                            false
                        }
                    }
                    // PCT: highest-priority-runnable must run — hand off iff
                    // some Runnable slot strictly outranks the running one.
                    PickAlgo::Pct { .. } => {
                        let cur = inner.slots[idx].prio;
                        inner
                            .slots
                            .iter()
                            .any(|s| s.state == SlotState::Runnable && s.prio > cur)
                    }
                }
            };
            inner.log.emit(
                Some(vpid),
                "Touch",
                Some(site),
                &[
                    ("kind", kind.as_str().to_string()),
                    ("preempt", if preempt { "1" } else { "0" }.to_string()),
                ],
            );
            if preempt {
                inner.slots[idx].state = SlotState::Runnable;
                inner.slots[idx].granted = false;
                let v = Some(vpid);
                self.handoff_locked(&mut inner, v);
            }
            preempt
        };
        if preempt {
            self.wait_for_grant(idx);
        }
    }

    /// Park until the (relative) `dur` elapses on virtual time or an
    /// explicit wake. Returns `true` iff the deadline expired (the trait's
    /// contract). Virtual time advances only when nothing is runnable
    /// (P3 §2.4).
    fn timed_park_slot(
        &self,
        idx: usize,
        site: &'static Location<'static>,
        dur: core::time::Duration,
    ) -> bool {
        let deadline_ns = {
            let mut inner = plock(&self.inner);
            self.pct_step_locked(&mut inner, idx);
            let now = match self.cfg.clock {
                ClockMode::Instance => inner.vnow_ns,
                ClockMode::PgClock => pg_clock::mono_ns(),
            };
            let deadline_ns = now.saturating_add(dur.as_nanos().min(u64::MAX as u128) as u64);
            let slot = &mut inner.slots[idx];
            // Real assert (F6 ledger row; see block_on_slot).
            assert!(
                slot.granted,
                "pgsync::sim: timed_park from a thread that does not hold the permit"
            );
            let vpid = slot.vpid;
            slot.state = SlotState::TimedParked(deadline_ns);
            slot.granted = false;
            slot.woken = false;
            slot.last_event = Some(("timedpark", site));
            inner.log.emit(
                Some(vpid),
                "TimedPark",
                Some(site),
                &[("deadline", deadline_ns.to_string())],
            );
            self.handoff_locked(&mut inner, Some(vpid));
            deadline_ns
        };
        let _ = deadline_ns;
        self.wait_for_grant(idx);
        let inner = plock(&self.inner);
        !inner.slots[idx].woken
    }

    /// Seeded pick over a wrapper-owned wait list of `n` entries (which
    /// notify_one wakee, which blocked locker acquires, which receiver
    /// wins). Returns an index in `0..n`. `actor` None = an off-model
    /// thread asked (its pick still burns one draw — stream stability).
    fn pick_waiter_slot(
        &self,
        actor: Option<Vpid>,
        site: &'static Location<'static>,
        n: usize,
    ) -> usize {
        assert!(n > 0, "pgsync::sim: pick_waiter over an empty wait list");
        let mut inner = plock(&self.inner);
        let pick = if n == 1 {
            0
        } else {
            (splitmix64(&mut inner.rng) % n as u64) as usize
        };
        inner.log.emit(
            actor,
            "PickWaiter",
            Some(site),
            &[("n", n.to_string()), ("idx", pick.to_string())],
        );
        pick
    }

    /// The spawn-door epilogue (rule 1: quantum-scoped teardown). Runs every
    /// registered teardown hook inside this — the final — quantum, THEN
    /// deregisters (making every Join-parked slot runnable: rule 3, joiners
    /// re-check their predicate), THEN hands the permit off. The OS-level
    /// TLS destructors that run after thread main returns must by then own
    /// no shared state.
    fn exit_cur(&self, idx: usize, site: &'static Location<'static>) {
        // Teardown hooks first, outside the state lock, permit still held.
        let hooks_list: Vec<Arc<dyn Fn(Vpid) + Send + Sync>> = plock(&self.teardown).clone();
        let vpid = {
            let inner = plock(&self.inner);
            inner.slots[idx].vpid
        };
        for h in &hooks_list {
            h(vpid);
        }
        let mut inner = plock(&self.inner);
        inner.log.emit(
            Some(vpid),
            "Exit",
            Some(site),
            &[("teardown", hooks_list.len().to_string())],
        );
        // Rule 3: join wakes on deregister (predicate-loop protocol — wake
        // every Join-parked slot; non-matching joiners re-park).
        for j in 0..inner.slots.len() {
            if inner.slots[j].state == SlotState::Blocked(OpClass::Join) {
                inner.slots[j].state = SlotState::Runnable;
                inner.slots[j].woken = true;
                let jv = inner.slots[j].vpid;
                inner.log.emit(
                    Some(vpid),
                    "Wake",
                    Some(site),
                    &[("target", jv.to_string()), ("prev", "blocked(join)".to_string())],
                );
            }
        }
        inner.slots[idx].state = SlotState::Exited;
        inner.slots[idx].granted = false;
        self.handoff_locked(&mut inner, Some(vpid));
        drop(inner);
        CURRENT.with(|c| *c.borrow_mut() = None);
    }

    /// Register a teardown hook (waiter::retire_current-class): runs with
    /// the exiting thread's vpid inside its final quantum, before joiners
    /// wake. Process-lifetime; hooks must be idempotent per thread.
    pub fn register_teardown_hook(&self, hook: impl Fn(Vpid) + Send + Sync + 'static) {
        plock(&self.teardown).push(Arc::new(hook));
    }

    /// Retire a slot whose OS spawn FAILED before the child ever bound (F3
    /// ledger row, applied at PERMIT-S2 while dooring the wpool site): the
    /// parent-side registration would otherwise leak a Runnable ghost whose
    /// eventual grant wedges the schedule (dump shows site=-). If the
    /// bootstrap handoff already granted the ghost, revoke and hand off
    /// again. Never-entered only: no teardown hooks, no join wakes (no
    /// joiner can hold a handle to a failed spawn).
    #[track_caller]
    pub fn cancel_never_entered(&self, slot: SlotId) {
        let site = Location::caller();
        let actor = current_vpid();
        let mut inner = plock(&self.inner);
        let vpid = inner.slots[slot.0].vpid;
        let was_granted = inner.slots[slot.0].granted;
        inner.slots[slot.0].state = SlotState::Exited;
        inner.slots[slot.0].granted = false;
        inner.log.emit(
            actor,
            "Cancel",
            Some(site),
            &[("child", vpid.to_string())],
        );
        if was_granted {
            self.handoff_locked(&mut inner, actor);
        }
    }

    // --- virtual time -------------------------------------------------------

    /// Current virtual now (ns) on this scheduler's clock.
    pub fn now_ns(&self) -> u64 {
        match self.cfg.clock {
            ClockMode::Instance => plock(&self.inner).vnow_ns,
            ClockMode::PgClock => pg_clock::mono_ns(),
        }
    }

    // --- diagnosis ----------------------------------------------------------

    /// The retained SCHEDOP stream (dump-on-demand; contract §3.3).
    pub fn dump_log(&self) -> String {
        plock(&self.inner).log.dump()
    }

    /// The watchdog's wedge probe (see watchdog.rs). `None` = no wedge
    /// establishable (permit idle, no live holder, or state lock busy).
    pub(crate) fn try_dump_wedge(&self, stalled_ms: u64) -> Option<String> {
        let inner = self.inner.try_lock().ok()?;
        if !inner.permit_held {
            return None;
        }
        let holder = inner.slots.iter().find(|s| s.state == SlotState::Running)?;
        let mut r = String::new();
        r.push_str("SCHEDWATCHDOG: permit-holder-blocked-outside-interception\n");
        r.push_str(&format!(
            "  seed={} stalled_ms={} heartbeat_seq={}\n",
            self.cfg.seed,
            stalled_ms,
            self.heartbeat_seq.load(Ordering::Relaxed)
        ));
        match holder.last_event {
            Some((op, loc)) => r.push_str(&format!(
                "  holder vpid={} name={} last-shim-event={} site={}:{}\n",
                holder.vpid,
                holder.name,
                op,
                loc.file(),
                loc.line()
            )),
            None => r.push_str(&format!(
                "  holder vpid={} name={} last-shim-event=- site=-\n",
                holder.vpid, holder.name
            )),
        }
        r.push_str("  slots:\n");
        for s in &inner.slots {
            r.push_str(&format!(
                "    vpid={} name={} state={}\n",
                s.vpid,
                s.name,
                s.state.as_log()
            ));
        }
        r.push_str(
            "  (holder native stack: not portably capturable from the monitor \
             thread — abort follows; inspect the core / attach a debugger)\n",
        );
        r.push_str("  SCHEDOP tail:\n");
        for line in inner.log.tail(64).lines() {
            r.push_str("    ");
            r.push_str(line);
            r.push('\n');
        }
        Some(r)
    }

    // --- internals ----------------------------------------------------------

    fn wait_for_grant(&self, idx: usize) {
        let mut inner = plock(&self.inner);
        while !inner.slots[idx].granted {
            inner = self.cv.wait(inner).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Pick + grant the next permit holder. Runs with the state lock held;
    /// caller has already taken the current thread out of Running.
    fn handoff_locked(&self, inner: &mut Inner, actor: Option<Vpid>) {
        loop {
            // 1. pick over Runnable slots (slot order = fixed registration
            //    order). Uniform: the seeded draw is the only entropy.
            //    PCT: highest priority wins, ties to the lowest slot index —
            //    deterministic, no draw (the entropy went into the
            //    priorities and change points).
            let runnable: Vec<usize> = inner
                .slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.state == SlotState::Runnable)
                .map(|(i, _)| i)
                .collect();
            if !runnable.is_empty() {
                let pick = if runnable.len() == 1 {
                    0
                } else {
                    match self.cfg.algo {
                        PickAlgo::Uniform => {
                            (splitmix64(&mut inner.rng) % runnable.len() as u64) as usize
                        }
                        PickAlgo::Pct { .. } => runnable
                            .iter()
                            .enumerate()
                            .max_by(|(_, a), (_, b)| {
                                inner.slots[**a]
                                    .prio
                                    .cmp(&inner.slots[**b].prio)
                                    // ties: LOWEST slot index wins (max_by
                                    // keeps the last max; reverse the index
                                    // order so the first-registered slot is
                                    // the deterministic winner).
                                    .then(b.cmp(a))
                            })
                            .map(|(i, _)| i)
                            .expect("non-empty runnable"),
                    }
                };
                let chosen = runnable[pick];
                inner.slots[chosen].granted = true;
                inner.slots[chosen].state = SlotState::Running;
                inner.permit_held = true;
                self.heartbeat_seq.fetch_add(1, Ordering::Relaxed);
                let next = inner.slots[chosen].vpid;
                inner.log.emit(
                    actor,
                    "Grant",
                    None,
                    &[
                        ("pick", format!("{}/{}", pick, runnable.len())),
                        ("next", next.to_string()),
                    ],
                );
                self.cv.notify_all();
                return;
            }

            // 2. nothing runnable: advance virtual time to the earliest
            //    deadline (only lever; P3 §2.4).
            let earliest = inner
                .slots
                .iter()
                .filter_map(|s| match s.state {
                    SlotState::TimedParked(d) => Some(d),
                    _ => None,
                })
                .min();
            if let Some(t) = earliest {
                if let Some(ceiling) = self.cfg.virtual_ceiling_ns {
                    if t > ceiling {
                        let report = self.build_fail_report_locked(
                            inner,
                            "SCHEDCEILING: virtual-time ceiling exceeded (never-satisfied predicate?)",
                            &format!("next_deadline={t} ceiling={ceiling}"),
                        );
                        inner.log.emit(actor, "Ceiling", None, &[]);
                        self.fail(report);
                    }
                }
                match self.cfg.clock {
                    ClockMode::Instance => inner.vnow_ns = t,
                    ClockMode::PgClock => {
                        let now = pg_clock::mono_ns();
                        if t > now {
                            pg_clock::sim::advance_ns(t - now);
                        }
                    }
                }
                let mut woke = 0usize;
                for s in inner.slots.iter_mut() {
                    if let SlotState::TimedParked(d) = s.state {
                        if d <= t {
                            s.state = SlotState::Runnable;
                            // woken stays false: deadline expiry, not a wake.
                            woke += 1;
                        }
                    }
                }
                inner.log.emit(
                    actor,
                    "Advance",
                    None,
                    &[("now", t.to_string()), ("woke", woke.to_string())],
                );
                continue;
            }

            // 3. no runnable, no timed sleeper.
            let blocked = inner
                .slots
                .iter()
                .filter(|s| matches!(s.state, SlotState::Blocked(_)))
                .count();
            if blocked > 0 {
                let report = self.build_fail_report_locked(
                    inner,
                    "SCHEDDEADLOCK: deterministic deadlock — all live slots parked in shims, no timed sleeper",
                    &format!("blocked={blocked}"),
                );
                inner.log.emit(actor, "Deadlock", None, &[]);
                self.fail(report);
            }

            // 4. everything exited (or nothing registered yet): permit idle.
            inner.permit_held = false;
            return;
        }
    }

    fn build_fail_report_locked(&self, inner: &Inner, head: &str, extra: &str) -> String {
        let mut r = String::new();
        r.push_str(head);
        r.push('\n');
        r.push_str(&format!("  seed={} {}\n", self.cfg.seed, extra));
        r.push_str("  slots:\n");
        for s in &inner.slots {
            match s.last_event {
                Some((op, loc)) => r.push_str(&format!(
                    "    vpid={} name={} state={} last={} site={}:{}\n",
                    s.vpid,
                    s.name,
                    s.state.as_log(),
                    op,
                    loc.file(),
                    loc.line()
                )),
                None => r.push_str(&format!(
                    "    vpid={} name={} state={} last=- site=-\n",
                    s.vpid,
                    s.name,
                    s.state.as_log()
                )),
            }
        }
        r.push_str("  SCHEDOP tail:\n");
        for line in inner.log.tail(64).lines() {
            r.push_str("    ");
            r.push_str(line);
            r.push('\n');
        }
        r.push_str("  replay: re-run the seed (single-run semantics)\n");
        r
    }

    fn fail(&self, report: String) -> ! {
        match self.cfg.fail {
            FailAction::Panic => panic!("{report}"),
            FailAction::DumpAbort => {
                eprintln!("{report}");
                std::process::abort();
            }
        }
    }
}

// --- the Router: hooks trait over TLS registration --------------------------

/// The [`SchedulerHooks`] implementation the wrappers see. Every op routes
/// by the CALLING THREAD's TLS registration; threads outside the model fall
/// through to neutral behavior (yield / real sleep / first-waiter picks —
/// exactly today's scheduler-less sim). `spawn` routes to the global
/// scheduler (the only owner of wrapper-thread registration); `wake` and
/// `pick_waiter` from off-model threads also fall through to the global so
/// an unregistered producer can still wake a registered consumer.
pub struct Router;

/// The installable instance (hooks::install wants `&'static dyn`).
pub static ROUTER: Router = Router;

impl SchedulerHooks for Router {
    fn block_on(&self, site: &'static Location<'static>, kind: OpClass) {
        match current_sched() {
            Some((s, idx, _)) => s.block_on_slot(idx, site, kind),
            // Off-model: keep the wrapper's retry loop from hard-spinning.
            None => std::thread::yield_now(),
        }
    }

    fn touch(&self, site: &'static Location<'static>, kind: OpClass) {
        if kind == OpClass::Spawn {
            // F1/F4 fix (PERMIT-S2): the fence pairs with the child-side
            // registration, which ALWAYS lands on the global scheduler
            // (`Router::spawn` below) — so the spawned-side bump runs there
            // too, whatever the toucher's binding (registered slot, unit
            // instance, off-model thread). Conservation holds in all cases.
            if let Some(g) = global() {
                g.spawn_fence();
            }
        }
        if let Some((s, idx, _)) = current_sched() {
            s.touch_slot(idx, site, kind);
        }
    }

    fn timed_park(&self, site: &'static Location<'static>, dur: core::time::Duration) -> bool {
        match current_sched() {
            Some((s, idx, _)) => s.timed_park_slot(idx, site, dur),
            // Off-model: the wrapper falls back to real elapsed time.
            None => {
                std::thread::sleep(dur);
                true
            }
        }
    }

    fn spawn(&self, vpid: Vpid, site: &'static Location<'static>) {
        if let Some(g) = global() {
            g.spawn_child_and_enter(vpid, site);
        }
    }

    #[track_caller]
    fn exit(&self, vpid: Vpid) {
        if let Some((s, idx, cur)) = current_sched() {
            debug_assert_eq!(cur, vpid, "pgsync::sim: exit(vpid) from a different thread");
            s.exit_cur(idx, Location::caller());
        }
    }

    fn pick_waiter(&self, site: &'static Location<'static>, n: usize) -> usize {
        match current_sched() {
            Some((s, _, v)) => s.pick_waiter_slot(Some(v), site, n),
            None => match global() {
                Some(g) => g.pick_waiter_slot(None, site, n),
                None => 0,
            },
        }
    }

    fn wake(&self, vpid: Vpid, site: &'static Location<'static>) {
        match current_sched() {
            Some((s, _, _)) => s.wake_vpid(vpid, site),
            None => {
                if let Some(g) = global() {
                    g.wake_vpid(vpid, site);
                }
            }
        }
    }

    fn current_vpid(&self) -> Vpid {
        current_vpid().unwrap_or(UNREGISTERED_VPID)
    }

    fn now_ns(&self) -> u64 {
        match current_sched() {
            Some((s, _, _)) => s.now_ns(),
            None => match global() {
                Some(g) => g.now_ns(),
                None => pg_clock::mono_ns(),
            },
        }
    }
}

// --- the ONE global scheduler (product wiring) ------------------------------

static GLOBAL: OnceLock<Option<Arc<Scheduler>>> = OnceLock::new();

/// The global permit scheduler, or `None` when `PGRUST_SIM_SCHED` is off
/// (the step-1 default: existing sim corpora — dst-determinism-smoke,
/// sim-net-e2e — run unscheduled and unaffected until a harness opts in).
///
/// Built lazily on first spawn-door touch: picker seed = one 8-byte
/// SimEntropy fill (keyed by `PGRUST_SIM_SEED`), SimClock driven mode, THE
/// watchdog started with the knob timeout, dump-and-abort failure action —
/// and [`ROUTER`] installed into the hooks seam so every pgsync wrapper op
/// routes here.
pub fn global() -> Option<&'static Arc<Scheduler>> {
    GLOBAL
        .get_or_init(|| {
            if !config::sched_enabled() {
                return None;
            }
            let mut seed_bytes = [0u8; 8];
            // Under pgrust_sim the funnel is SimEntropy (infallible fill);
            // the bool is the native-arm OS-entropy failure signal.
            let _ = pg_strong_random::pg_strong_random(&mut seed_bytes);
            let algo = if config::sched_algo_is_pct() {
                PickAlgo::Pct {
                    depth: config::pct_depth(),
                    steps: config::pct_steps(),
                }
            } else {
                PickAlgo::Uniform
            };
            let cfg = SchedulerConfig {
                seed: u64::from_le_bytes(seed_bytes),
                algo,
                preempt_p: config::preempt_p(),
                log_cap: DEFAULT_LOG_CAP,
                stream_log: config::stream_schedlog(),
                virtual_ceiling_ns: config::virtual_ceiling_ns(),
                clock: ClockMode::PgClock,
                fail: FailAction::DumpAbort,
                watchdog_timeout_ms: config::watchdog_timeout_ms(),
                watchdog_poll_ms: 250,
                watchdog_sink: WatchdogSink::AbortProcess,
            };
            let sched = Scheduler::new(cfg);
            watchdog::start(&sched);
            hooks::install(&ROUTER);
            Some(sched)
        })
        .as_ref()
}

// --- test-only instance surface ---------------------------------------------
// The unit battery drives instance schedulers through the SAME Router the
// wrappers use: enter() binds TLS, so trait calls on ROUTER route to the
// test's instance. These helpers only exist so tests can name the trait
// methods tersely.
#[cfg(test)]
pub(crate) fn router() -> &'static dyn SchedulerHooks {
    &ROUTER
}
