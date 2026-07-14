//! The scheduler core: 128-slot global array, morsel claiming, and the
//! LAST-WORKER-OUT FINALIZATION PROTOCOL (SIGMOD'21 §2.4 via
//! notes/morsel-lit-review.md §1.3; correctness-critical — modeled in
//! tests/loom.rs before any perf work, per redesign doc §2.1).
//!
//! # The finalization protocol, exactly
//!
//! Finishing a pipeline must (a) run its finalization AT MOST ONCE, and
//! (b) only after ALL workers finished their in-flight tasks — an empty
//! morsel queue does NOT mean the pipeline is done (literature mistake #2).
//!
//! 1. PUBLISH-TARGET-BEFORE-CLAIM: a worker stores the slot index it is
//!    about to work in the pin board BEFORE reading the slot word.
//! 2. EXHAUSTED → INVALIDATE: the first worker to find the task set's
//!    cursor exhausted (or its generation dead) CASes the slot word from
//!    valid to invalid; the CAS winner is the unique coordinator.
//! 3. MARK: the coordinator scans the pin board and swaps every entry still
//!    pinned to the dying slot (its own included) for a finalization
//!    marker, then adds the number of marked workers to the task set's
//!    finalization counter.
//! 4. SETTLE: every worker clears its pin after finishing its in-flight task;
//!    if it finds a marker it decrements the counter. The counter may go
//!    TRANSIENTLY NEGATIVE (marked workers can decrement before the
//!    coordinator's add lands).
//! 5. LAST OUT: whoever moves the counter to zero is provably the last
//!    worker out; it runs finalize, then activates the RG's next task set
//!    in the SAME slot (or completes the RG and admits the next queued RG).
//!
//! Safety note (why `settle`'s ownership lookup cannot dangle): the slot's
//! ownership entry is only replaced/cleared by LAST OUT, which requires the
//! counter to reach zero, which requires every marked worker's decrement —
//! so a marked worker always finds its task set still owned at settle time.
//! Conversely a worker whose pin was marked can never observe the slot's
//! NEXT occupant as valid: the next occupant is published by last-out,
//! which its own pending decrement blocks.
//!
//! Scheduling policy in M0 is single-RG FIFO: pick = lowest-index active
//! slot; the stride/pass/priority fields exist but are never read
//! (docs/design/inter-query-scheduling.md §5.2/§5.3 activate them in M5).

use std::collections::VecDeque;
use std::sync::Arc;

use crate::clock::Clock;
use crate::morsel::MorselRange;
use crate::rg::{QuerySpec, ResourceGroup, RgClass, RgOutcome};
use crate::sizing::{SizingDecision, SizingParams, TaskSizer};
use crate::stats::{RuntimeStats, RuntimeStatsSnapshot};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::{lock, Mutex, ParkLot, Semaphore};
use crate::taskset::{PinBoard, Slot, TaskSetRt, WorkerMailbox};

/// Umbra's slot-array bound: 128 concurrently-active resource groups; later
/// arrivals wait in the FIFO queue.
pub const DEFAULT_SLOTS: usize = 128;

/// Pin-board lanes reserved for EXTERNAL participant threads (M1: the
/// query's bound parallel helpers driving `Runtime::drive_pinned`). External
/// lanes live above the pool's `nthreads` indexes; the finalization
/// protocol's coordinator scans the whole board, so external participants
/// carry marker obligations exactly like pool workers.
///
/// 256 (DOP-192 readiness): every gang helper leases one lane for the whole
/// drive, so this constant IS the per-process pinned-participation ceiling —
/// at the old 64, a dop-192 engagement silently refused 128+ helpers
/// (fail-closed non-participation). 256 = 192 helpers + vacuum drivers +
/// concurrent-query headroom; the lease mask is `EXTERNAL_LANE_WORDS` words.
pub const MAX_EXTERNAL_LANES: usize = 256;

/// Words of the external-lane lease bitmask (64 lanes per `AtomicU64`).
pub(crate) const EXTERNAL_LANE_WORDS: usize = MAX_EXTERNAL_LANES / 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Executed (part of) a task.
    Ran,
    /// Published against a slot that turned invalid; try again.
    Retry,
    /// No active slot; caller should park (capture the park epoch BEFORE
    /// calling worker_step, then park on it).
    Idle,
    /// Stop requested; worker loop should exit.
    Stop,
}

struct SlotEntry {
    seq: u64,
    ts: Arc<TaskSetRt>,
}

/// One claim attempt's outcome (stream sources add STARVED to the classic
/// claimed/exhausted pair).
enum Claim {
    Range(MorselRange),
    /// Stream source, cursor at an OPEN watermark: nothing claimable NOW but
    /// the set is not exhausted — the worker parks and the producer's
    /// publish/close wakes it.
    Starved,
    Exhausted,
}

/// How a task ended (run_task).
enum TaskEnd {
    /// The set's cursor is exhausted (or its generation dead): drive
    /// invalidation/finalization.
    Exhausted,
    /// Duration budget spent; more work remains.
    Budget,
    /// Stream starvation: no claimable granule and the stream is open. The
    /// worker should park (epoch-guarded — a publish during the task wakes
    /// the park immediately).
    Starved,
}

/// Membership state: slot ownership + the RG wait queue. One mutex, touched
/// only on membership events (publish/finalize/admit) and on worker cache
/// misses — never on the per-task hot path (slot-word seq revalidation hits
/// the thread-local cache).
struct Membership {
    owned: Vec<Option<SlotEntry>>,
    waitq: VecDeque<Arc<ResourceGroup>>,
}

/// Per-drive observability accumulators (the WFIN marker channel —
/// fabled run-m0-parallel-accept.sh parses `MORSEL|WFIN|…` off server
/// stderr). Plain thread-owned data written at morsel/task cadence and
/// read by the drive's owner after completion: no synchronization, no
/// loom-visible operations. Timestamps are the scheduler clock's ns.
#[derive(Default, Clone, Copy)]
pub struct DriveLocal {
    /// Tasks this local executed (claim loops entered with a live join).
    pub tasks: u64,
    pub morsels: u64,
    pub granules: u64,
    /// Sum of executed-morsel durations (excludes claim/park/settle time).
    pub busy_ns: u64,
    /// Clock at the first claimed morsel's execution start; 0 = none ran.
    pub first_claim_ns: u64,
    /// Clock at the end of the last executed morsel (the WFIN t_us).
    pub last_end_ns: u64,
}

/// Thread-local scheduling bookkeeping (one per worker, owned by the worker
/// loop — deliberately NOT thread_local! so loom can drive it).
pub struct WorkerLocal {
    worker: usize,
    /// WFIN drive accumulators (fresh per `Runtime::external_local`; pool
    /// workers accumulate for their thread's lifetime — only the external
    /// pinned drives read them today).
    pub drive: DriveLocal,
    /// Slot-word cache: (seq, task set) per slot; revalidated by one atomic
    /// read of the slot word.
    cache: Vec<Option<(u64, Arc<TaskSetRt>)>>,
    /// Per-slot WFIN accumulation (marker contract; see [`markers_enabled`]).
    /// Untouched (empty vec) when markers are off.
    wfin: Vec<Option<WfinStat>>,
    /// Pinned-drive fast path: the last (slot, seq) this local drove for its
    /// pinned RG. Revalidated by one slot-word read per step; the membership
    /// lock is touched only when it goes stale (publish/finalize events).
    pinned_slot: Option<(usize, u64)>,
    /// INERT until M5: thread-local stride state (SIGMOD'21 §2.3 — each
    /// worker runs stride scheduling locally over the same slot set).
    #[allow(dead_code)]
    local_pass: u64,
    #[allow(dead_code)]
    global_pass: u64,
}

/// `PGRUST_MORSEL_MARKERS=1` arms the acceptance-instrument marker channel:
/// one server-stderr line per (worker, task set) participation —
/// `MORSEL|WFIN|qid=|pipe=|worker=|t_us=|tasks=|task_avg_us=` — parsed by the
/// M0 instruments' worker-finish-spread verdict (fabled
/// m0-acceptance-instruments; ≤1-task-duration acceptance). Default OFF:
/// zero cost beyond one branch per task.
fn markers_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_MORSEL_MARKERS").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Claim-coalescing target for whole-boundary sources (dop1-tax fix 1):
/// a claim spans up to `PGRUST_RUNTIME_COALESCE_EPOCHS / active_workers`
/// epochs (default 8 → ×8 at DOP1, ×4 at 2, ×2 at 3-4, off at ≥5). `1` (or
/// `0`) disables coalescing — the inc-2 one-epoch-per-claim behavior, the
/// A/B arm. Read once.
fn coalesce_epochs() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_COALESCE_EPOCHS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(8)
    })
}

/// One worker's accumulated participation in one published task set.
struct WfinStat {
    seq: u64,
    qid: u64,
    pipe: usize,
    tasks: u64,
    task_ns: u64,
    /// Monotonic end of the worker's LAST task in the set (the finish time
    /// the spread instrument reads).
    last_end_ns: u64,
}

impl WfinStat {
    fn emit(&self, worker: usize) {
        let avg_us = if self.tasks > 0 { self.task_ns / self.tasks / 1000 } else { 0 };
        eprintln!(
            "MORSEL|WFIN|qid={}|pipe={}|worker={}|t_us={}|tasks={}|task_avg_us={}",
            self.qid,
            self.pipe,
            worker,
            self.last_end_ns / 1000,
            self.tasks,
            avg_us
        );
    }
}

impl WorkerLocal {
    /// Fold one completed task into the slot's WFIN accumulator; a stale
    /// accumulator (earlier publication in the same slot) flushes first.
    fn wfin_observe(&mut self, ts: &TaskSetRt, task_ns: u64, end_ns: u64) {
        if self.wfin.is_empty() {
            return;
        }
        let slot = &mut self.wfin[ts.slot];
        match slot {
            Some(s) if s.seq == ts.seq => {
                s.tasks += 1;
                s.task_ns += task_ns;
                s.last_end_ns = end_ns;
            }
            _ => {
                if let Some(old) = slot.take() {
                    old.emit(self.worker);
                }
                *slot = Some(WfinStat {
                    seq: ts.seq,
                    qid: ts.rg.query_id,
                    pipe: ts.index,
                    tasks: 1,
                    task_ns,
                    last_end_ns: end_ns,
                });
            }
        }
    }

    /// Flush the slot's accumulator (the worker observed the set exhausted —
    /// its participation is over; no worker can claim past an exhausted
    /// cursor).
    fn wfin_flush_slot(&mut self, slot: usize) {
        if self.wfin.is_empty() {
            return;
        }
        if let Some(s) = self.wfin[slot].take() {
            s.emit(self.worker);
        }
    }

    /// Flush every accumulator (idle transition / pinned-drive exit): any
    /// remaining participation record is final — the worker is leaving the
    /// execution loop.
    pub(crate) fn wfin_flush_all(&mut self) {
        if self.wfin.is_empty() {
            return;
        }
        for slot in 0..self.wfin.len() {
            self.wfin_flush_slot(slot);
        }
    }
}

pub(crate) struct Scheduler {
    slots: Vec<Slot>,
    /// Active-slot bitmask (2×u64 = 128 slots). M0 reads it directly per
    /// pick (read-mostly, uncontended at 16 workers); M5 syncs it into the
    /// thread-local views via the worker mailboxes.
    active: [AtomicU64; 2],
    /// M4 maintenance preference mask (docs/design/m4-bgjobs.md §3.5):
    /// subset of `active` holding Maintenance-class slots; the pick scans it
    /// first. ADVISORY ONLY — a stale bit resolves to Retry through the slot
    /// word, so no ordering discipline against `active` is needed. Cost on
    /// the foreground path: two zero-mask loads per pick.
    maint: [AtomicU64; 2],
    membership: Mutex<Membership>,
    pins: PinBoard,
    /// INERT until M5: per-worker change/return masks.
    #[allow(dead_code)]
    mailboxes: Vec<WorkerMailbox>,
    pub(crate) park: ParkLot,
    /// External pin-board lane lease bitmask (word w bit b = lane w*64+b
    /// busy). Lanes are leased through Runtime::acquire_external_lane;
    /// MAX_EXTERNAL_LANES = 256 ⇒ 4 words, scanned low-word-first.
    pub(crate) external_lanes: [AtomicU64; EXTERNAL_LANE_WORDS],
    /// Execution-permit semaphore: exactly `permits` (= cores) permits; any
    /// task-executing thread holds one (acquired by the pool loop around
    /// worker_step). The hard runnable cap of the §2.5 permit model. The
    /// pool runs `cores + K` threads (§2.8): the K standbys block here until
    /// a declared blocking section releases a permit through
    /// [`crate::sync::IoGuard`].
    pub(crate) permits: Semaphore,
    stop: AtomicBool,
    clock: Arc<dyn Clock>,
    params: SizingParams,
    pub(crate) stats: RuntimeStats,
    /// Total pool threads (cores + standbys) — the pin board is sized by
    /// THREADS, not permits: a thread blocked in an I/O section keeps its
    /// pin and its finalization-marker obligations.
    nthreads: usize,
    next_seq: AtomicU64,
    next_rg_id: AtomicU64,
    trace: bool,
}

impl Scheduler {
    pub(crate) fn new(
        nthreads: usize,
        permits: usize,
        nslots: usize,
        params: SizingParams,
        clock: Arc<dyn Clock>,
        trace: bool,
    ) -> Scheduler {
        assert!(nthreads > 0);
        assert!(permits > 0 && permits <= nthreads);
        assert!(nslots > 0 && nslots <= DEFAULT_SLOTS);
        Scheduler {
            slots: (0..nslots).map(|_| Slot::new()).collect(),
            active: [AtomicU64::new(0), AtomicU64::new(0)],
            maint: [AtomicU64::new(0), AtomicU64::new(0)],
            membership: Mutex::new(Membership {
                owned: (0..nslots).map(|_| None).collect(),
                waitq: VecDeque::new(),
            }),
            pins: PinBoard::new(nthreads + MAX_EXTERNAL_LANES),
            mailboxes: (0..nthreads).map(|_| WorkerMailbox::new()).collect(),
            park: ParkLot::new(),
            external_lanes: std::array::from_fn(|_| AtomicU64::new(0)),
            permits: Semaphore::new(permits),
            stop: AtomicBool::new(false),
            clock,
            params,
            stats: RuntimeStats::default(),
            nthreads,
            next_seq: AtomicU64::new(0),
            next_rg_id: AtomicU64::new(0),
            trace,
        }
    }

    pub(crate) fn nthreads(&self) -> usize {
        self.nthreads
    }

    pub(crate) fn worker_local(&self, worker: usize) -> WorkerLocal {
        assert!(worker < self.nthreads);
        WorkerLocal {
            worker,
            drive: DriveLocal::default(),
            cache: (0..self.slots.len()).map(|_| None).collect(),
            wfin: if markers_enabled() {
                (0..self.slots.len()).map(|_| None).collect()
            } else {
                Vec::new()
            },
            pinned_slot: None,
            local_pass: 0,
            global_pass: 0,
        }
    }

    /// Bookkeeping for an EXTERNAL participant thread (M1 pinned driver):
    /// pin-board lane `nthreads + ordinal`.
    pub(crate) fn external_local(&self, ordinal: usize) -> WorkerLocal {
        assert!(ordinal < MAX_EXTERNAL_LANES, "external participant lanes exhausted");
        WorkerLocal {
            worker: self.nthreads + ordinal,
            drive: DriveLocal::default(),
            cache: (0..self.slots.len()).map(|_| None).collect(),
            wfin: if markers_enabled() {
                (0..self.slots.len()).map(|_| None).collect()
            } else {
                Vec::new()
            },
            pinned_slot: None,
            local_pass: 0,
            global_pass: 0,
        }
    }

    /// Scheduler clock read (WFIN leader marks share the workers' domain).
    pub(crate) fn clock_now_ns(&self) -> u64 {
        self.clock.now_ns()
    }

    pub(crate) fn snapshot(&self) -> RuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    pub(crate) fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.park.wake_all();
    }

    /// Call sites must guard with `if self.trace { ... }` BEFORE building the
    /// message — the format! argument otherwise allocates on every
    /// submit/publish/finalize even with tracing off (m2-integration
    /// std-collections audit, AGENTS.md rule 7: mallocs are a tracked
    /// metric on engaged paths).
    fn trace(&self, msg: &str) {
        if self.trace {
            eprintln!("[pgrust-runtime] {msg}");
        }
    }

    // ---- membership: submit / publish / admit -----------------------------

    pub(crate) fn submit(&self, spec: QuerySpec, pinned: bool, class: RgClass) -> Arc<ResourceGroup> {
        assert!(
            !(pinned && class == RgClass::Maintenance),
            "maintenance RGs are pool-executed, never pinned"
        );
        let rg_id = self.next_rg_id.fetch_add(1, Ordering::SeqCst) + 1;
        let rg = ResourceGroup::new(rg_id, spec, pinned, class);
        RuntimeStats::tick(&self.stats.rgs_submitted);
        if self.trace {
            self.trace(&format!("rg {} submitted (query {})", rg.rg_id, rg.query_id));
        }
        if rg.tasksets.is_empty() {
            rg.completion.complete(RgOutcome::Completed);
            RuntimeStats::tick(&self.stats.rgs_completed);
            return rg;
        }
        let mut m = lock(&self.membership);
        match class {
            RgClass::Foreground => {
                // FIFO admission: never overtake queued RGs.
                if m.waitq.is_empty() {
                    if let Some(slot) = m.owned.iter().position(Option::is_none) {
                        self.start_rg_locked(&mut m, Arc::clone(&rg), slot);
                        return rg;
                    }
                }
                m.waitq.push_back(Arc::clone(&rg));
            }
            RgClass::Maintenance => {
                // Starvation floor (§3.5): a due job cycle takes any free
                // slot regardless of queued foreground RGs, and if every
                // slot is busy it goes to the FRONT of the queue (the
                // handful of jobs never meaningfully reorders the FIFO).
                if let Some(slot) = m.owned.iter().position(Option::is_none) {
                    self.start_rg_locked(&mut m, Arc::clone(&rg), slot);
                    return rg;
                }
                m.waitq.push_front(Arc::clone(&rg));
            }
        }
        rg
    }

    /// Mark the RG's first task set started and publish it. Caller holds the
    /// membership lock (lock order: membership, then progress).
    fn start_rg_locked(&self, m: &mut Membership, rg: Arc<ResourceGroup>, slot: usize) {
        let first = {
            let mut p = lock(&rg.progress);
            rg.next_ready(&mut p).expect("fresh RG must have a ready task set (index 0)")
        };
        self.publish_taskset_locked(m, rg, first, slot);
    }

    fn publish_taskset_locked(
        &self,
        m: &mut Membership,
        rg: Arc<ResourceGroup>,
        index: usize,
        slot: usize,
    ) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let c0 = rg.tasksets[index].source.startup_c0();
        // Generation binding (H1): hand the work its generation BEFORE the
        // slot word below admits any worker — generation-keyed partial state
        // (the M2 sink plumbing) is armed before the first claim can land.
        rg.tasksets[index].work.bind_generation(rg.generation());
        let whole_claims = rg.tasksets[index].source.whole_boundary_claims();
        let coalesce = whole_claims && rg.tasksets[index].source.coalesce_claims();
        let ts = Arc::new(TaskSetRt {
            rg,
            index,
            slot,
            seq,
            cursor: AtomicU64::new(0),
            sizer: crate::sizing::SizerShared::new(),
            active_workers: AtomicU64::new(0),
            fin_counter: crate::sync::atomic::AtomicI64::new(0),
            finalized: AtomicBool::new(false),
            c0,
            whole_claims,
            coalesce,
        });
        if self.trace {
            self.trace(&format!(
                "publish rg {} taskset {} in slot {slot} seq {seq}",
                ts.rg.rg_id, index
            ));
        }
        let pinned = ts.rg.pinned;
        let class = ts.rg.class;
        m.owned[slot] = Some(SlotEntry { seq, ts });
        self.slots[slot].word.store((seq << 1) | 1, Ordering::SeqCst);
        // Pinned RGs are invisible to the pool's pick: only external
        // participants (drive_pinned) may execute them — pool workers have
        // no session binding for the query (M1; §2.3 retires this in M2+).
        if !pinned {
            self.set_active(slot, class);
        }
        RuntimeStats::tick(&self.stats.tasksets_published);
        // Wake parked workers: new work exists (external pinned drivers
        // park on the same epoch eventcount).
        self.park.wake_all();
    }

    fn set_active(&self, slot: usize, class: RgClass) {
        if class == RgClass::Maintenance {
            self.maint[slot / 64].fetch_or(1u64 << (slot % 64), Ordering::SeqCst);
        }
        self.active[slot / 64].fetch_or(1u64 << (slot % 64), Ordering::SeqCst);
    }

    fn clear_active(&self, slot: usize) {
        self.maint[slot / 64].fetch_and(!(1u64 << (slot % 64)), Ordering::SeqCst);
        self.active[slot / 64].fetch_and(!(1u64 << (slot % 64)), Ordering::SeqCst);
    }

    fn pick_slot(&self) -> Option<usize> {
        // M4 preference: any Maintenance-class slot first (§3.5 starvation
        // floor; mask is usually zero — two loads on the foreground path).
        // A stale hit revalidates through the slot word into Retry.
        for (i, word) in self.maint.iter().enumerate() {
            let mask = word.load(Ordering::SeqCst);
            if mask != 0 {
                let slot = i * 64 + mask.trailing_zeros() as usize;
                if slot < self.slots.len() {
                    return Some(slot);
                }
            }
        }
        // M0 policy: FIFO / lowest-index active slot (single-RG benchmark
        // case degenerates to "the only slot"). M5 replaces this scan with
        // the thread-local lowest-pass stride pick.
        for (i, word) in self.active.iter().enumerate() {
            let mask = word.load(Ordering::SeqCst);
            if mask != 0 {
                let slot = i * 64 + mask.trailing_zeros() as usize;
                if slot < self.slots.len() {
                    return Some(slot);
                }
            }
        }
        None
    }

    // ---- the worker step ---------------------------------------------------

    /// One scheduling decision + at most one task execution. The pool loop
    /// (and the loom models) drive this in a loop; on `Idle` the caller
    /// parks on an epoch captured BEFORE the call.
    pub(crate) fn worker_step(&self, local: &mut WorkerLocal) -> Step {
        if self.stop.load(Ordering::SeqCst) {
            return Step::Stop;
        }
        let Some(slot) = self.pick_slot() else {
            // Idle transition: nothing is runnable — any WFIN accumulation
            // still held is this worker's final word on those sets.
            local.wfin_flush_all();
            return Step::Idle;
        };

        // Protocol step 1: publish-target-before-claim.
        self.pins.publish(local.worker, slot);

        let step = match self.resolve(local, slot) {
            None => Step::Retry,
            Some(ts) => match self.run_task(local, &ts) {
                TaskEnd::Exhausted => {
                    // Protocol step 2: exhausted → invalidate (coordinator
                    // election by slot-word CAS).
                    self.coordinate(&ts);
                    Step::Ran
                }
                TaskEnd::Budget => Step::Ran,
                // Starved stream: park (epoch captured before this step, so
                // a publish that landed mid-task wakes the park at once).
                TaskEnd::Starved => Step::Idle,
            },
        };

        // Protocol step 4: settle own pin; pay any marker debt.
        self.settle(local.worker);
        step
    }

    /// One scheduling step of an EXTERNAL participant restricted to ONE
    /// pinned RG (M1: a bound parallel helper executes only the query whose
    /// session state it carries). Same protocol as `worker_step` — publish
    /// before slot-word read, run, coordinate on exhaustion, settle — with
    /// the pick replaced by a membership lookup of the RG's occupied slot.
    /// Deliberately does NOT observe `stop`: external participants are
    /// session-driven; their exit condition is RG completion (the caller
    /// re-tests `RgHandle::try_outcome` around every step).
    pub(crate) fn worker_step_pinned(
        &self,
        local: &mut WorkerLocal,
        rg: &Arc<ResourceGroup>,
    ) -> Step {
        // Fast path: the cached (slot, seq) revalidated by one slot-word
        // read — the membership lock is a publish/finalize-event cost, not a
        // per-step cost (the sched-probe decision-cost budget).
        let slot = match local.pinned_slot {
            Some((slot, seq))
                if self.slots[slot].word.load(Ordering::SeqCst) == (seq << 1) | 1 =>
            {
                Some(slot)
            }
            _ => {
                let found = {
                    let m = lock(&self.membership);
                    m.owned.iter().enumerate().find_map(|(i, e)| {
                        e.as_ref()
                            .filter(|e| Arc::ptr_eq(&e.ts.rg, rg))
                            .map(|e| (i, e.seq))
                    })
                };
                local.pinned_slot = found;
                found.map(|(slot, _)| slot)
            }
        };
        let Some(slot) = slot else {
            // Queued behind other RGs, or completed: the caller re-tests
            // completion and parks on an epoch captured before this call.
            return Step::Idle;
        };

        // Protocol step 1: publish-target-before-claim.
        self.pins.publish(local.worker, slot);

        let step = match self.resolve(local, slot) {
            None => Step::Retry,
            Some(ts) if !Arc::ptr_eq(&ts.rg, rg) => {
                // The slot rolled to a different RG between lookup and
                // revalidation; not ours to run.
                Step::Retry
            }
            Some(ts) => match self.run_task(local, &ts) {
                TaskEnd::Exhausted => {
                    self.coordinate(&ts);
                    Step::Ran
                }
                TaskEnd::Budget => Step::Ran,
                TaskEnd::Starved => Step::Idle,
            },
        };

        // Protocol step 4: settle own pin; pay any marker debt.
        self.settle(local.worker);
        step
    }

    /// Revalidate the slot word (single atomic read on the cached path) and
    /// return the task set it names.
    fn resolve(&self, local: &mut WorkerLocal, slot: usize) -> Option<Arc<TaskSetRt>> {
        let word = self.slots[slot].word.load(Ordering::SeqCst);
        if word & 1 == 0 {
            return None;
        }
        let seq = word >> 1;
        if let Some((cseq, ts)) = &local.cache[slot] {
            if *cseq == seq {
                return Some(Arc::clone(ts));
            }
        }
        let m = lock(&self.membership);
        let entry = m.owned[slot].as_ref()?;
        if entry.seq != seq {
            return None;
        }
        local.cache[slot] = Some((seq, Arc::clone(&entry.ts)));
        Some(Arc::clone(&entry.ts))
    }

    /// Execute one task: claim boundary-clamped morsel ranges from the shared
    /// cursor until the duration budget is spent, the set is exhausted, or
    /// the generation dies. [`TaskEnd::Exhausted`] ⇔ the task set is
    /// exhausted (or its generation is dead) and finalization should be
    /// driven; [`TaskEnd::Starved`] ⇔ an open stream source has nothing
    /// claimable (park, producer wakes).
    fn run_task(&self, local: &mut WorkerLocal, ts: &Arc<TaskSetRt>) -> TaskEnd {
        RuntimeStats::tick(&self.stats.tasks_claimed);
        RuntimeStats::tick(&ts.rg.stats.tasks_claimed);
        let task_t0 = if local.wfin.is_empty() { 0 } else { self.clock.now_ns() };
        ts.active_workers.fetch_add(1, Ordering::SeqCst);

        // Generation gate (H1): a task of an aborted (closed) generation is
        // unconsumable — the merged lifecycle's fail-closed armed join
        // refuses, so no participant, no morsel. The exhausted path then
        // drives ordinary invalidate/finalize cleanup.
        let end = match ts.rg.handle.join() {
            Err(_refused) => {
                RuntimeStats::tick(&self.stats.generation_refusals);
                TaskEnd::Exhausted
            }
            Ok(participant) => {
                local.drive.tasks += 1;
                let mut sizer = TaskSizer::new(self.params, ts.c0);
                let mut end = TaskEnd::Budget;
                // Per-task observability accumulators (dop1-tax fix 5):
                // morsel/granule/cpu counters are EXACT but flushed to the
                // shared relaxed atomics once per TASK, not per morsel —
                // the counts are observability (snapshots, trace, stride
                // accounting inputs), never safety; nothing reads them at
                // sub-task granularity. Budget/flush metering (the sink
                // lanes' safety accounting) lives operator-side and is
                // untouched.
                let mut t_morsels = 0u64;
                let mut t_granules = 0u64;
                let mut t_cpu_ns = 0u64;
                loop {
                    // Morsel-boundary cancel point (Leis-style): an abort is
                    // observed within one morsel.
                    if ts.rg.is_aborted() {
                        end = TaskEnd::Exhausted;
                        break;
                    }
                    let range = match self.claim_morsel(ts, &mut sizer) {
                        Claim::Range(range) => range,
                        Claim::Starved => {
                            end = TaskEnd::Starved;
                            break;
                        }
                        Claim::Exhausted => {
                            end = TaskEnd::Exhausted;
                            break;
                        }
                    };
                    let granules = range.end - range.start;
                    let t0 = self.clock.now_ns();
                    // Execute under the participant's operation count. A
                    // refusal means the close (abort) landed between the
                    // boundary check and the claim: drain WITHOUT running
                    // the claimed range — aborted generations need not
                    // execute every granule, only never twice.
                    let worker = local.worker;
                    let work = ts.work();
                    if participant
                        .run(|| {
                            work.run_morsel(worker, range);
                            Ok(())
                        })
                        .is_err()
                    {
                        end = TaskEnd::Exhausted;
                        break;
                    }
                    let t1 = self.clock.now_ns();
                    let dt = t1.saturating_sub(t0);
                    // WFIN accumulators (thread-owned plain data).
                    local.drive.morsels += 1;
                    local.drive.granules += granules;
                    local.drive.busy_ns += dt;
                    if local.drive.first_claim_ns == 0 {
                        local.drive.first_claim_ns = t0;
                    }
                    local.drive.last_end_ns = t1;
                    sizer.observe(&ts.sizer, granules, dt);
                    t_morsels += 1;
                    t_granules += granules;
                    t_cpu_ns += dt;
                    if sizer.task_done() {
                        break;
                    }
                }
                if t_morsels > 0 {
                    // INERT stride accounting (M5 reads this).
                    ts.rg.cpu_consumed_ns.fetch_add(t_cpu_ns, Ordering::Relaxed);
                    RuntimeStats::add(&self.stats.morsels_claimed, t_morsels);
                    RuntimeStats::add(&ts.rg.stats.morsels_claimed, t_morsels);
                    RuntimeStats::add(&self.stats.granules_executed, t_granules);
                    RuntimeStats::add(&ts.rg.stats.granules_executed, t_granules);
                }
                // Armed-outcome discipline: a worker's task ends
                // successfully even when it drained an abort — failure is
                // recorded on the lifecycle by the aborting side, never by
                // drained workers. (An unfinished Drop would cancel the
                // generation; complete() is the required exit.)
                let _ = participant.complete();
                end
            }
        };

        ts.active_workers.fetch_sub(1, Ordering::SeqCst);
        RuntimeStats::tick(&self.stats.tasks_completed);
        RuntimeStats::tick(&ts.rg.stats.tasks_completed);
        // WFIN marker channel (off = one branch): fold this task into the
        // slot's accumulator; an observed exhaustion ends this worker's
        // participation (no claim can succeed past an exhausted cursor), so
        // flush its line here.
        if !local.wfin.is_empty() {
            let now = self.clock.now_ns();
            local.wfin_observe(ts, now.saturating_sub(task_t0), now);
            if matches!(end, TaskEnd::Exhausted) {
                local.wfin_flush_slot(ts.slot);
            }
        }
        end
    }

    fn claim_morsel(&self, ts: &TaskSetRt, sizer: &mut TaskSizer) -> Claim {
        // Stream sources: claimable up to the producer's watermark; only a
        // CLOSED stream's watermark is exhaustion (closed read before the
        // watermark inside stream_state — see the MorselSource contract).
        let (total, closed) = match ts.source().stream_state() {
            Some(state) => state,
            None => (ts.source().total_granules(), true),
        };
        loop {
            let cur = ts.cursor.load(Ordering::SeqCst);
            if cur >= total {
                return if closed { Claim::Exhausted } else { Claim::Starved };
            }
            let workers = ts.active_workers.load(Ordering::SeqCst).max(1);
            let (want, decision) = sizer.next_size(&ts.sizer, total - cur, workers);
            // Never split a granule (whole-granule ranges by construction);
            // never cross a row-group / dictionary-epoch boundary.
            let bound = ts.source().next_boundary_after(cur).min(total);
            debug_assert!(bound > cur, "MorselSource boundary contract violated");
            // Whole-boundary claims (drive-scaling inc-2): epoch-heavy
            // sources never stop a claim short of the boundary — a split
            // epoch is executed by 2+ workers, each rebuilding the epoch's
            // dictionary/memo state (the measured q21 DOP15 +78% busy
            // inflation). The sizer still observes for phase/stats.
            let end = if ts.whole_claims {
                // Claim coalescing (dop1-tax fix 1): at LOW live width the
                // per-claim drive re-entry (scan reposition + drain prologue
                // + partial export, ~30-45µs each on q21-class shapes) is
                // the dominant DOP-1 tax — span SEVERAL epochs per claim and
                // let the work body iterate per-epoch segments (one dict
                // snapshot per segment; the epoch rules hold inside the
                // claim). Width signal: `ts.active_workers`, already loaded
                // above for the photo-finish W — the taskset's own live
                // participant count, zero extra cost, and (unlike the pool's
                // running count) it counts EXTERNAL pinned-drive lanes,
                // which is what actually executes M1/M2 engagements. The
                // factor decays to 1 as workers join, so a mid-query DOP
                // widening naturally reverts to single-epoch claims; gating
                // on the Default phase keeps the FIRST claims (Startup ramp,
                // before the gang's joins are all visible) single-epoch —
                // no stale-width giant claim can front-run a widening.
                // Photo-finish (Shutdown) sizing is untouched.
                let mut end = bound;
                if ts.coalesce && decision == SizingDecision::Default {
                    let factor = coalesce_epochs() / workers;
                    if factor > 1 {
                        // Fair-share clamp: never claim past this worker's
                        // 1/W share of the remainder — late tail claims
                        // shrink toward one epoch as remaining work runs
                        // out, preserving the ≤1-task finish-spread posture.
                        let fair_end = cur + ((total - cur) / workers).max(1);
                        for _ in 1..factor {
                            if end >= total || end >= fair_end {
                                break;
                            }
                            end = ts.source().next_boundary_after(end).min(total);
                        }
                    }
                }
                end
            } else {
                cur.saturating_add(want).min(bound).max(cur + 1)
            };
            if ts
                .cursor
                .compare_exchange(cur, end, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let counter = match decision {
                    SizingDecision::Ramp => &self.stats.sizing_ramp,
                    SizingDecision::Default => &self.stats.sizing_default,
                    SizingDecision::Shutdown => &self.stats.sizing_shutdown,
                };
                RuntimeStats::tick(counter);
                return Claim::Range(cur..end);
            }
        }
    }

    /// Protocol steps 2+3: invalidate the slot (unique coordinator via CAS),
    /// then mark still-pinned workers and fund the finalization counter.
    fn coordinate(&self, ts: &Arc<TaskSetRt>) {
        let valid = (ts.seq << 1) | 1;
        if self.slots[ts.slot]
            .word
            .compare_exchange(valid, ts.seq << 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // someone else coordinates (or already did)
        }
        self.clear_active(ts.slot);
        RuntimeStats::tick(&self.stats.tasksets_invalidated);
        if self.trace {
            self.trace(&format!(
                "invalidate rg {} taskset {} slot {} seq {}",
                ts.rg.rg_id, ts.index, ts.slot, ts.seq
            ));
        }
        let mut marked = 0i64;
        for w in 0..self.nthreads + MAX_EXTERNAL_LANES {
            if self.pins.mark(w, ts.slot) {
                marked += 1;
            }
        }
        RuntimeStats::add(&self.stats.finalize_marks, marked as u64);
        let after = ts.fin_counter.fetch_add(marked, Ordering::SeqCst) + marked;
        // The coordinator is itself still pinned (its own mark is in
        // `marked` and its decrement hasn't happened), so `after >= 1` here;
        // the zero check is kept because the protocol's rule is "whoever
        // moves the counter to zero runs finalization", not "the add can't".
        if after == 0 {
            self.last_out(ts);
        }
    }

    /// Protocol step 4: clear own pin; if a coordinator counted us, pay the
    /// decrement — and if that decrement is the zero crossing, we are
    /// provably the last worker out (step 5).
    fn settle(&self, worker: usize) {
        let Some(slot) = self.pins.settle(worker) else {
            return;
        };
        // Safe by protocol: the marked task set is still owned at `slot`
        // because last-out (the only replacer) is blocked on our decrement —
        // see module doc.
        let ts = {
            let m = lock(&self.membership);
            let entry = m.owned[slot]
                .as_ref()
                .expect("marked worker's task set must still be owned");
            Arc::clone(&entry.ts)
        };
        let after = ts.fin_counter.fetch_sub(1, Ordering::SeqCst) - 1;
        if after < 0 {
            // Transiently negative: we drained before the coordinator's add
            // landed. Legal and expected under the protocol.
            RuntimeStats::tick(&self.stats.finalize_negative_observed);
        }
        if after == 0 {
            self.last_out(&ts);
        }
    }

    /// Protocol step 5: at-most-once finalization by the provably-last
    /// worker out, then activate the RG's next task set in the same slot —
    /// or complete the RG and admit the next queued one.
    fn last_out(&self, ts: &Arc<TaskSetRt>) {
        let was = ts.finalized.swap(true, Ordering::SeqCst);
        assert!(!was, "finalization must run at most once");
        let rg = Arc::clone(&ts.rg);
        let aborted = rg.is_aborted();
        if !aborted {
            ts.work().finalize();
        }
        RuntimeStats::tick(&self.stats.finalize_events);
        if self.trace {
            self.trace(&format!(
                "finalize rg {} taskset {} (aborted={aborted})",
                rg.rg_id, ts.index
            ));
        }

        // Progress under the RG lock only (never while holding membership).
        let next = {
            let mut p = lock(&rg.progress);
            p.done[ts.index] = true;
            if aborted {
                p.aborted = true;
                None
            } else {
                rg.next_ready(&mut p)
            }
        };

        match next {
            Some(i) => {
                let mut m = lock(&self.membership);
                debug_assert!(
                    matches!(&m.owned[ts.slot], Some(e) if e.seq == ts.seq),
                    "slot ownership changed before last-out"
                );
                self.publish_taskset_locked(&mut m, rg, i, ts.slot);
            }
            None => {
                #[cfg(debug_assertions)]
                if !aborted {
                    let p = lock(&rg.progress);
                    debug_assert!(
                        p.done.iter().all(|&d| d),
                        "RG completed with unfinished task sets (dep DAG hole)"
                    );
                }
                self.release_slot_and_admit(ts.slot, ts.seq);
                // The RG leaves the scheduler: drain (a no-op — every
                // participant is provably gone, see retire_lifecycle) and
                // retire its generation before the leader wakes.
                rg.retire_lifecycle();
                rg.completion.complete(if aborted {
                    RgOutcome::Aborted
                } else {
                    RgOutcome::Completed
                });
                RuntimeStats::tick(&self.stats.rgs_completed);
                if aborted {
                    RuntimeStats::tick(&self.stats.rgs_aborted);
                }
                // Parked pinned drivers observe completion by re-testing
                // try_outcome after a wake; the completion word itself only
                // unparks registered leader waiters.
                self.park.wake_all();
                if self.trace {
                    self.trace(&format!("rg {} complete (aborted={aborted})", rg.rg_id));
                }
            }
        }
    }

    fn release_slot_and_admit(&self, slot: usize, seq: u64) {
        // RGs popped while already aborted complete without ever running.
        let mut complete_aborted: Vec<Arc<ResourceGroup>> = Vec::new();
        {
            let mut m = lock(&self.membership);
            debug_assert!(
                matches!(&m.owned[slot], Some(e) if e.seq == seq),
                "releasing a slot we do not own"
            );
            m.owned[slot] = None;
            while let Some(rg) = m.waitq.pop_front() {
                if rg.is_aborted() {
                    complete_aborted.push(rg);
                    continue;
                }
                self.start_rg_locked(&mut m, rg, slot);
                break;
            }
        }
        for rg in complete_aborted {
            {
                let mut p = lock(&rg.progress);
                p.aborted = true;
            }
            rg.retire_lifecycle();
            rg.completion.complete(RgOutcome::Aborted);
            RuntimeStats::tick(&self.stats.rgs_completed);
            RuntimeStats::tick(&self.stats.rgs_aborted);
            self.park.wake_all();
        }
    }
}
