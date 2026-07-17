//! TaskSet runtime state + the pieces of the last-worker-out finalization
//! protocol that live per-worker (the pin board) and per-slot (the slot
//! word).
//!
//! PROVENANCE: written fresh from SIGMOD'21 §2.3/§2.4 (Wagner/Kohn/Neumann)
//! as distilled in notes/morsel-lit-review.md §1.2–1.3 and ratified in
//! docs/design/inter-query-scheduling.md §4. The design docs name a
//! `qsched.rs` harvest candidate on the shelved inter-query-scheduling /
//! proto-workstealing branches; verified 2026-07 (this lane): no such file
//! exists on any branch — the correction in the redesign doc §4 ("no loom
//! dependency or model exists on any branch; aspirational references only")
//! extends to the scheduler core itself. Everything here is new work.

use std::sync::Arc;

use crate::lifecycle::Generation;
use crate::morsel::MorselSource;
use crate::rg::{ResourceGroup, TaskSetWork};
use crate::sizing::SizerShared;
use crate::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// Cache-line isolation for hot shared atomics (agg192-contention, 48xl
/// finding #1): 128 bytes = one Neoverse prefetch pair, so a padded field
/// never shares a transfer unit with its neighbors. Layout-only — no
/// semantic or switchable behavior; `Deref` keeps use sites unchanged.
#[repr(align(128))]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Runtime state of one published task set (one pipeline of one RG occupying
/// one scheduler slot at one sequence number).
///
/// Hot-line layout (agg192-contention): `cursor` (CASed per claim by every
/// worker), `sizer` (a Mutex locked twice per morsel), and `active_workers`
/// (SeqCst RMW twice per task + a load per claim) each own a padded line —
/// previously all three plus `fin_counter` packed into one/two cache lines,
/// a single transfer unit absorbing every claim/task RMW of 191 workers.
pub(crate) struct TaskSetRt {
    pub(crate) rg: Arc<ResourceGroup>,
    /// Index of this task set within its RG.
    pub(crate) index: usize,
    /// Slot this task set is published in.
    pub(crate) slot: usize,
    /// Global publication sequence number; `(seq << 1) | 1` is the slot word
    /// while this task set is valid.
    pub(crate) seq: u64,
    /// Morsel claiming cursor: next unclaimed granule. Claims advance it by
    /// CAS of whole boundary-clamped ranges; `cursor >= total` ⇔ exhausted.
    pub(crate) cursor: CachePadded<AtomicU64>,
    /// Shared duration-adaptive sizing state (per-pipeline state machine).
    pub(crate) sizer: CachePadded<SizerShared>,
    /// Workers currently executing a task of this set (W of the photo
    /// finish). Distinct from the pin board: pins are the finalization
    /// protocol's ground truth; this is a sizing input only.
    pub(crate) active_workers: CachePadded<AtomicU64>,
    /// Finalization counter of the last-worker-out protocol. The coordinator
    /// adds the number of successfully-marked workers; each marked worker
    /// subtracts one when it finishes its in-flight task. MAY GO TRANSIENTLY
    /// NEGATIVE (marked workers can decrement before the coordinator's add
    /// lands). Whoever moves it to zero is provably the last worker out and
    /// runs finalization.
    pub(crate) fin_counter: AtomicI64,
    /// At-most-once finalization assert.
    pub(crate) finalized: AtomicBool,
    /// Startup-ramp seed for this pipeline (source-provided C0).
    pub(crate) c0: u64,
    /// Whole-boundary claims (source-provided, cached at publish): every
    /// claim extends to the next hard boundary — per-epoch state is never
    /// split across workers (MorselSource::whole_boundary_claims).
    pub(crate) whole_claims: bool,
    /// Claim coalescing on whole-boundary claims (source-provided, cached
    /// at publish): at low live width one claim may span several epochs —
    /// only for sources whose work body subdivides at epoch edges
    /// (MorselSource::coalesce_claims).
    pub(crate) coalesce: bool,
}

impl TaskSetRt {
    pub(crate) fn source(&self) -> &Arc<dyn MorselSource> {
        &self.rg.tasksets[self.index].source
    }

    pub(crate) fn work(&self) -> &Arc<dyn TaskSetWork> {
        &self.rg.tasksets[self.index].work
    }

    pub(crate) fn generation(&self) -> Generation {
        self.rg.generation()
    }
}

/// Pin-board entry states (one AtomicU64 per worker):
///   EMPTY                  — worker not working / between tasks
///   PINNED(slot)           — worker published intent to work slot `slot`
///                            (STORED BEFORE READING THE SLOT WORD — the
///                            publish-target-before-claim rule)
///   MARKED(slot)           — a coordinator counted this worker into the
///                            dying task set's finalization counter
const PIN_EMPTY: u64 = u64::MAX;
const PIN_MARK: u64 = 1 << 63;

/// The global published-target state array of the finalization protocol:
/// entry w = what worker w is (about to be) working on.
/// Entries are cache-line padded (agg192-contention): every worker writes
/// its own entry once per step (publish + settle) — unpadded, 8 workers
/// shared one line, ping-ponging it on every step of every drive at 191
/// workers. Coordinator `mark` scans stay correct (reads/CASes each padded
/// entry individually).
pub(crate) struct PinBoard {
    entries: Box<[CachePadded<AtomicU64>]>,
}

impl PinBoard {
    pub(crate) fn new(nworkers: usize) -> PinBoard {
        PinBoard {
            entries: (0..nworkers).map(|_| CachePadded(AtomicU64::new(PIN_EMPTY))).collect(),
        }
    }

    /// Publish worker `w`'s target slot. MUST happen before the worker reads
    /// the slot word (SeqCst store; the read is SeqCst too), so a coordinator
    /// that invalidated the slot and then scans the board can never miss a
    /// worker that saw the slot valid.
    pub(crate) fn publish(&self, w: usize, slot: usize) {
        debug_assert_eq!(
            self.entries[w].load(Ordering::SeqCst),
            PIN_EMPTY,
            "publish over an unsettled pin"
        );
        self.entries[w].store(slot as u64, Ordering::SeqCst);
    }

    /// Coordinator: swap worker `w`'s PINNED(slot) entry for a finalization
    /// marker. True ⇔ the worker was still pinned and is now counted.
    pub(crate) fn mark(&self, w: usize, slot: usize) -> bool {
        self.entries[w]
            .compare_exchange(
                slot as u64,
                slot as u64 | PIN_MARK,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    /// WS-O inc-2 debug accessor (contract adjudication "pin-board assert
    /// — debug-only accessor approved"): worker `w`'s entry is EMPTY
    /// (settled). Post-drive asserts and diagnostics ONLY — never a
    /// production branch.
    pub(crate) fn is_settled(&self, w: usize) -> bool {
        self.entries[w].load(Ordering::SeqCst) == PIN_EMPTY
    }

    /// Worker: clear own entry after finishing a task (or bailing on an
    /// invalidated slot). Returns the marked slot if a coordinator counted
    /// us — the caller owes exactly one decrement on that task set's
    /// finalization counter.
    pub(crate) fn settle(&self, w: usize) -> Option<usize> {
        let old = self.entries[w].swap(PIN_EMPTY, Ordering::SeqCst);
        debug_assert_ne!(old, PIN_EMPTY, "settle without publish");
        if old & PIN_MARK != 0 {
            Some((old & !PIN_MARK) as usize)
        } else {
            None
        }
    }
}

/// One slot of the global scheduler array. The word is the single-atomic-read
/// hot path: `(seq << 1) | valid`. `pass`/`stride` are the stride scheduling
/// fields (SIGMOD'21 §2.3), LIVE as of M5-4 (inter-query §5.3): the pick is
/// lowest `pass` among active slots and each executed task advances the
/// slot's pass by `stride × cpu_ns`. With a single active RG the lowest-pass
/// pick degenerates to the only slot — the single-query case is provably
/// today's FIFO pick (and `PGRUST_RUNTIME_STRIDE=0` restores FIFO outright).
///
/// All stride fields are Relaxed and ADVISORY: they order the pick, never
/// execution safety — a stale pick revalidates through the slot word into
/// Retry, exactly like a stale maintenance-mask hit.
#[repr(align(128))]
pub(crate) struct Slot {
    pub(crate) word: AtomicU64,
    /// Priority-weighted virtual time of the occupying RG. Reset to the
    /// scheduler's global-pass watermark when a NEW RG is admitted to the
    /// slot (standard stride join — no credit for queue wait); persists
    /// across the same RG's task-set republishes.
    pub(crate) pass: AtomicU64,
    /// Fixed-point stride (see sched::stride_for): STRIDE1/priority, so pass
    /// advance = (cpu_ns × stride) >> PASS_SHIFT. Refreshed from the RG's
    /// priority at every publish — M5-5's decaying priorities only have to
    /// update `ResourceGroup::priority` and republish/refresh to take effect.
    pub(crate) stride: AtomicU64,
    /// Session-affinity token of the occupying RG's leader (0 = none):
    /// the equal-pass pick tiebreak prefers a slot whose token matches the
    /// picking worker's sticky-bound session (M5-4; ceremony-v2 mechanism).
    pub(crate) session: AtomicU64,
    /// Occupying RG's id (M5+1 pipeline-DAG dispatch; 0 = none). ADVISORY
    /// like every stride word: identifies same-query siblings for the
    /// within-query dependency-depth pick refinement (m5-planner §3.6).
    pub(crate) rg: AtomicU64,
    /// Dependency depth of the occupying pipeline (M5+1): the length of the
    /// longest dependency chain ABOVE it (downstream work it releases).
    /// Within a query at equal pass, the pick prefers the deeper pipeline —
    /// the critical path releases the most downstream work (§3.6).
    pub(crate) depth: AtomicU64,
}

impl Slot {
    pub(crate) fn new() -> Slot {
        Slot {
            word: AtomicU64::new(0),
            pass: AtomicU64::new(0),
            stride: AtomicU64::new(0),
            session: AtomicU64::new(0),
            rg: AtomicU64::new(0),
            depth: AtomicU64::new(0),
        }
    }
}

/// INERT until M5: per-worker change/return mask pair of the ratified
/// dispatcher design (docs/design/inter-query-scheduling.md §5.2; SIGMOD'21
/// §2.3 cross-worker sync). Producers will set bit k with fetch_or when slot
/// k's membership changes; the worker exchanges its masks with zero and folds
/// updates into its thread-local view. M0 workers instead revalidate via the
/// slot word's seq on every pick (equivalent, slightly hotter; the masks ship
/// as data structures so M5 activation is additive).
#[allow(dead_code)]
pub(crate) struct WorkerMailbox {
    pub(crate) change: [AtomicU64; 2],
    pub(crate) ret: [AtomicU64; 2],
}

impl WorkerMailbox {
    pub(crate) fn new() -> WorkerMailbox {
        WorkerMailbox {
            change: [AtomicU64::new(0), AtomicU64::new(0)],
            ret: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}
