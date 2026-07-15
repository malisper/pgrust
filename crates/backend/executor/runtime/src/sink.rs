//! ParallelSink — the M2 sink contract and its TaskSet plumbing (M2 PREP).
//!
//! Design authority: docs/design/m2-sinks.md (this lane's companion doc;
//! donor mapping = nodeagg::merge / Stage-4 radix exchange + paremit,
//! nodeagg::pardistinct, the stringhash tables, refsort + rule-2 rowref).
//! Parent plan: docs/design/parallelism-redesign-2026-07.md §2.2 / §5-M2.
//!
//! STATUS (m2-integration): TWO production sinks ride this module —
//! the M2 aggregation sink (two-set [`ParallelSink`]: execmain
//! lanev2/runtime_agg.rs + nodeagg/sink.rs) and the M2 distinct sink
//! (three-set [`SealedParallelSink`]: lanev2/runtime_distinct.rs +
//! nodeagg/pardistinct.rs). Both engage only under `PGRUST_RUNTIME=1` +
//! their arm knobs (guc_tables::runtime_pool); off-path the runtime crate
//! is dead code (M0 contract). The toy-sink test suites below are the
//! compatibility oracle for the plumbing — they ride unmodified from both
//! lanes (see docs/design/m2-sinks.md §7.1 for why both DAG shapes are
//! kept rather than unified).
//!
//! # The contract
//!
//! One logical pipeline breaker (aggregation table, distinct set, top-N
//! heap) becomes TWO task sets on the runtime, produced by
//! [`sink_tasksets`]:
//!
//! ```text
//!   ACCEPT task set (source = the data pipeline's morsels)
//!     run_morsel  → fork-on-first-touch per worker, then
//!                   ParallelSink::accept_local (thread-local, no shared
//!                   mutation, no locks on the row path)
//!     finalize    → SEAL (last-worker-out, single-threaded): collect the
//!                   generation-matching LocalStates for the combine set
//!   COMBINE task set (source = the sink's partition space, deps=[accept])
//!     run_morsel  → ParallelSink::combine(part, &locals) — each partition
//!                   claimed exactly once; single writer per partition
//!     finalize    → ParallelSink::finalize (last-worker-out,
//!                   single-threaded, MINIMAL — see the paremit resolution
//!                   in m2-sinks.md §4: emission belongs in combine;
//!                   finalize is publish + epilogue)
//! ```
//!
//! # H1 protection (structural, not conventional)
//!
//! Every LocalState is keyed by the runtime lifecycle's [`Generation`]:
//! - the scheduler hands the plumbing its generation at publish time
//!   ([`crate::rg::TaskSetWork::bind_generation`]), strictly before any
//!   worker can claim a morsel;
//! - a worker slot holding a Local of a DIFFERENT generation is dropped and
//!   re-forked, never accepted into (aborted-init leftovers are
//!   unconsumable by construction);
//! - SEAL takes only generation-matching Locals; mismatches are dropped and
//!   counted ([`SinkTaskSets::stale_locals_dropped`]);
//! - COMBINE refuses (fail-closed, no-op) unless the sealed generation
//!   equals its own bound generation.
//!
//! Aborts need no sink-side handling beyond this: the scheduler's
//! generation gate already refuses morsels of a closed generation, and
//! last-worker-out skips `finalize` on aborted RGs, so an aborted sink's
//! Locals are simply dropped with the RG.

use std::sync::Arc;

use crate::lifecycle::Generation;
use crate::morsel::{MorselRange, MorselSource};
use crate::rg::{TaskSetSpec, TaskSetWork};
use crate::sched::MAX_EXTERNAL_LANES;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{lock, Mutex};

/// The M2 sink contract: how a pipeline breaker participates in morsel
/// parallelism. Implementations live with their operators (nodeagg /
/// execmain) in M2 proper; the runtime crate owns only the contract and the
/// TaskSet plumbing that drives it.
///
/// Threading rules (enforced by the plumbing + the finalization protocol):
/// - `fork` — at most once per (worker, generation), on the worker's first
///   morsel of the accept set;
/// - `accept_local` — parallel across workers, but each Local is touched by
///   exactly one worker at a time (worker-indexed slots); implementations
///   must not mutate shared state here (C-parity work_mem budgeting is per
///   participant = per forked Local);
/// - `combine` — parallel across partitions, each partition index visited
///   EXACTLY ONCE for the sink's lifetime; the sink's per-partition output
///   cells therefore have a single writer (the donors' UnsafeCell-per-bucket
///   pattern is sound here for the same reason);
/// - `finalize` — single-threaded under last-worker-out of the combine set,
///   after every `combine` call has returned. MINIMAL by contract: emission
///   work belongs in `combine` (the paremit ruling); `finalize` publishes
///   results / flips the breaker to its Source face.
pub trait ParallelSink: Send + Sync {
    /// Per-worker partial state (thread-local table, distinct-set builder,
    /// bounded heap). `Sync` is required only so the combine phase may read
    /// all sealed Locals concurrently (`&[Local]`).
    type Local: Send + Sync;

    /// Create worker `worker`'s LocalState. Called lazily on the worker's
    /// first accept morsel; at most once per (worker, generation).
    fn fork(&self, worker: usize) -> Self::Local;

    /// Absorb one claimed morsel into `local`. At runtime altitude the
    /// morsel is a granule range; the M2 operator-side implementation drives
    /// its lane pipeline segment over the range and pushes the produced
    /// rows/batches into `local` (see m2-sinks.md §2 layering note).
    fn accept_local(&self, local: &mut Self::Local, worker: usize, range: MorselRange);

    /// SEAL-side per-Local preparation, called ONCE by the accept set's
    /// last-worker-out finalizer on the collected generation-matching Locals
    /// (worker-slot order), strictly before any combine task can observe
    /// them. Single-threaded by the last-worker-out protocol. The M2 agg
    /// sink partitions its remainder tables here (counting sort — O(total
    /// remainder rows), a one-time cost the combine's bucket claims then
    /// read immutably). Default: nothing.
    fn seal(&self, _locals: &mut [Self::Local]) {}

    /// Number of combine partitions (the donors: 256 radix buckets over the
    /// top-8 hash bits; 64 element partitions per distinct set). Fixed for
    /// the sink's lifetime; this is the combine task set's morsel space.
    fn partitions(&self) -> u64;

    /// Merge partition `part` across all sealed Locals — and, where the
    /// shape admits it (paremit identity shapes), finalize + emit the
    /// partition's results in the same pass. Parallel across partitions;
    /// deterministic output requires the donor discipline: visit `locals`
    /// in slice order, first-seen insertion order within the partition.
    /// `worker` is the claiming worker's index (observability — WFIN).
    fn combine(&self, part: u64, worker: usize, locals: &[Self::Local]);

    /// Single-threaded epilogue under last-worker-out of the combine set.
    /// Every `combine` call has returned. Keep it MINIMAL.
    fn finalize(&self, locals: &[Self::Local]);
}

/// H1/fail-closed observability counters, shared between the plumbing and
/// the caller's [`SinkTaskSets`] handle. Non-generic so the handle needs no
/// type parameter.
#[derive(Default)]
pub struct SinkProbe {
    stale_dropped: AtomicU64,
    combine_refusals: AtomicU64,
}

impl SinkProbe {
    /// LocalStates dropped for generation mismatch (0 on every healthy run).
    pub fn stale_locals_dropped(&self) -> u64 {
        self.stale_dropped.load(Ordering::SeqCst)
    }

    /// Combine/finalize refusals against a missing or generation-mismatched
    /// seal (0 on every healthy run — reachable only through protocol
    /// violation or cross-generation confusion; the refusal IS the H1
    /// fail-closed behavior).
    pub fn combine_refusals(&self) -> u64 {
        self.combine_refusals.load(Ordering::SeqCst)
    }
}

/// Shared plumbing state between the accept and combine task sets of one
/// sink instance (one query's one breaker — never reused across queries).
struct SinkShared<S: ParallelSink> {
    sink: Arc<S>,
    /// Generation bound at publish time (re-bound on re-publish). Guards
    /// every Local against cross-generation consumption (H1).
    bound: Mutex<Option<Generation>>,
    /// Worker-indexed LocalState slots, generation-keyed. A slot is only
    /// locked by its own worker on the accept path (uncontended) and by the
    /// sealing finalizer.
    locals: Box<[Mutex<Option<(Generation, S::Local)>>]>,
    /// SEAL output: the accept set's last-worker-out collects the matching-
    /// generation Locals here; the combine set reads them (immutable, via a
    /// transient Arc clone per combine task) and finalize consumes the slot.
    sealed: Mutex<Option<(Generation, Arc<Vec<S::Local>>)>>,
    probe: Arc<SinkProbe>,
}

impl<S: ParallelSink> SinkShared<S> {
    fn bound_generation(&self) -> Generation {
        lock(&self.bound).expect(
            "sink task set ran before bind_generation — scheduler publish contract violated",
        )
    }
}

/// The ACCEPT task set's work: fork-on-first-touch + accept_local, and the
/// SEAL at last-worker-out.
struct SinkAccept<S: ParallelSink>(Arc<SinkShared<S>>);

impl<S: ParallelSink> TaskSetWork for SinkAccept<S> {
    fn bind_generation(&self, generation: Generation) {
        *lock(&self.0.bound) = Some(generation);
    }

    fn run_morsel(&self, worker: usize, range: MorselRange) {
        let generation = self.0.bound_generation();
        let mut slot = lock(&self.0.locals[worker]);
        match &*slot {
            Some((g, _)) if *g == generation => {}
            Some(_) => {
                // A Local from a dead generation (aborted init, prior
                // rescan). Unconsumable by construction: drop, re-fork.
                self.0.probe.stale_dropped.fetch_add(1, Ordering::SeqCst);
                *slot = Some((generation, self.0.sink.fork(worker)));
            }
            None => {
                *slot = Some((generation, self.0.sink.fork(worker)));
            }
        }
        let (_, local) = slot.as_mut().expect("just forked");
        self.0.sink.accept_local(local, worker, range);
    }

    /// SEAL: single-threaded by the last-worker-out protocol. Collect the
    /// generation-matching Locals (worker-index order — deterministic input
    /// order for the combine's first-seen merge discipline); drop the rest.
    fn finalize(&self) {
        let generation = self.0.bound_generation();
        let mut sealed = Vec::new();
        for slot in self.0.locals.iter() {
            match lock(slot).take() {
                Some((g, local)) if g == generation => sealed.push(local),
                Some(_) => {
                    self.0.probe.stale_dropped.fetch_add(1, Ordering::SeqCst);
                }
                None => {}
            }
        }
        // Sink-side SEAL preparation (single-threaded here by protocol);
        // the Locals freeze immutable behind the Arc right after.
        self.0.sink.seal(&mut sealed);
        *lock(&self.0.sealed) = Some((generation, Arc::new(sealed)));
    }
}

/// The COMBINE task set's work: partition-claim combine + the sink's
/// finalize at last-worker-out.
struct SinkCombine<S: ParallelSink>(Arc<SinkShared<S>>);

impl<S: ParallelSink> SinkCombine<S> {
    /// Fail-closed sealed-state read: `None` unless a seal exists AND its
    /// generation matches ours (same RG ⇒ same generation in M0; the check
    /// is the H1 keying, kept even where "impossible").
    fn sealed_for(&self, generation: Generation) -> Option<Arc<Vec<S::Local>>> {
        let g = lock(&self.0.sealed);
        match &*g {
            Some((sg, locals)) if *sg == generation => Some(Arc::clone(locals)),
            _ => None,
        }
    }
}

impl<S: ParallelSink> TaskSetWork for SinkCombine<S> {
    fn bind_generation(&self, generation: Generation) {
        // Same shared state, same RG: accept already bound it; re-binding is
        // idempotent in M0 (single generation) and correct under M1+
        // regeneration (both sets re-publish under the new generation).
        *lock(&self.0.bound) = Some(generation);
    }

    fn run_morsel(&self, worker: usize, range: MorselRange) {
        let generation = self.0.bound_generation();
        let Some(locals) = self.sealed_for(generation) else {
            // Fail-closed: no matching seal ⇒ combine nothing. Reachable
            // only through a protocol violation (combine published before
            // accept finalized) or cross-generation confusion — never
            // silently merge. Observable via SinkProbe::combine_refusals.
            self.0.probe.combine_refusals.fetch_add(1, Ordering::SeqCst);
            return;
        };
        for part in range {
            self.0.sink.combine(part, worker, &locals);
        }
    }

    fn finalize(&self) {
        let generation = self.0.bound_generation();
        let taken = {
            let mut g = lock(&self.0.sealed);
            match g.take() {
                Some((sg, locals)) if sg == generation => Some(locals),
                other => {
                    // Mismatched seal: put it back untouched (fail-closed —
                    // it is some other generation's state, not ours to
                    // consume or destroy) and finalize nothing.
                    *g = other;
                    None
                }
            }
        };
        let Some(locals) = taken else {
            self.0.probe.combine_refusals.fetch_add(1, Ordering::SeqCst);
            return;
        };
        self.0.sink.finalize(&locals);
        // All combine tasks have settled (last-worker-out), so this Arc is
        // the last strong reference; the Locals drop here.
        debug_assert_eq!(Arc::strong_count(&locals), 1, "combine task outlived finalize");
    }
}

/// The combine task set's morsel source: the sink's partition space, one
/// partition per claim (boundary after every granule — partitions are the
/// donors' claim unit, and a single writer per partition is the soundness
/// argument for the sink's output cells).
struct PartitionSource {
    parts: u64,
}

impl MorselSource for PartitionSource {
    fn total_granules(&self) -> u64 {
        self.parts
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(self.parts)
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// The two task-set specs of one sink, plus the plumbing's observability
/// handle. `combine.deps` is wired to `accept_index` (the caller places
/// `accept` at that index in its [`crate::rg::QuerySpec`], `combine`
/// anywhere after it).
pub struct SinkTaskSets {
    pub accept: TaskSetSpec,
    pub combine: TaskSetSpec,
    pub probe: Arc<SinkProbe>,
}

/// Build the accept + combine task sets for `sink` over `source`.
/// `nthreads` is the RUNTIME'S total thread count
/// ([`crate::Runtime::nthreads`]), not the permit count: a standby thread
/// absorbing a permit is a first-class worker index. The Local slot array is
/// sized `nthreads + MAX_EXTERNAL_LANES` — external pinned participants
/// (M1 bound helpers, worker index = nthreads + lane ordinal) are
/// first-class sink workers too, and under the pinned-engagement regime
/// they are the ONLY touchers, so forks ≤ launched helpers = the R3
/// per-launched-worker memory envelope (docs/design/m2-sinks.md §8-R3(a)).
pub fn sink_tasksets<S: ParallelSink + 'static>(
    sink: Arc<S>,
    source: Arc<dyn MorselSource>,
    nthreads: usize,
    accept_index: usize,
) -> SinkTaskSets {
    assert!(nthreads > 0);
    let parts = sink.partitions();
    assert!(parts > 0, "a ParallelSink must expose at least one partition");
    let probe = Arc::new(SinkProbe::default());
    let shared = Arc::new(SinkShared {
        sink,
        bound: Mutex::new(None),
        locals: (0..nthreads + MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        sealed: Mutex::new(None),
        probe: Arc::clone(&probe),
    });
    let accept = TaskSetSpec {
        source,
        work: Arc::new(SinkAccept(Arc::clone(&shared))),
        deps: Vec::new(),
    };
    let combine = TaskSetSpec {
        source: Arc::new(PartitionSource { parts }),
        work: Arc::new(SinkCombine(Arc::clone(&shared))),
        deps: vec![accept_index],
    };
    SinkTaskSets { accept, combine, probe }
}

// ===========================================================================
// Sealed variant (M2 distinct sink; proposed shared extension — flagged for
// reconciliation with the agg-sink lane).
//
// Some sinks' per-worker partial state is not combinable in place: it must
// first be FROZEN into a partitioned wire form (pardistinct's
// `PdBuilder::freeze` → `PdHandedTable`; donor A's exchange flush is the
// same move). Freezing is O(partial data) — running it single-threaded
// inside the accept set's SEAL would serialize a data-sized pass (the
// donors freeze per worker, in parallel). The runtime gives no per-worker
// "your participation ended" event (a worker's last morsel is only known
// in hindsight), so the parallel freeze becomes its own TASK SET:
//
//   ACCEPT  (source = data morsels)      fork + accept_local; finalize = no-op
//   SEALCVT (source = one granule per Local slot, deps=[accept])
//           run_morsel = seal(worker slot) in parallel, one slot per claim;
//           finalize   = SEAL (collect Sealed in slot order)
//   COMBINE (source = partition space, deps=[sealcvt])  as the base contract
//
// Generation keying, fail-closed combine, and the probe counters are
// identical to the two-set plumbing above.
// ===========================================================================

/// A [`ParallelSink`] whose Locals need a per-worker parallel freeze before
/// the combine can read them. Same threading rules as [`ParallelSink`], plus:
/// - `seal` — parallel across Local slots, exactly once per forked Local
///   (its task set claims each slot index once);
/// - `combine`/`finalize` see `&[Sealed]` in worker-slot order.
pub trait SealedParallelSink: Send + Sync {
    /// Per-worker partial build state. `Send` only: exactly one thread
    /// touches a Local at a time (accept by its worker, seal by its claimer).
    type Local: Send;
    /// The frozen, combine-readable form.
    type Sealed: Send + Sync;

    fn fork(&self, worker: usize) -> Self::Local;
    fn accept_local(&self, local: &mut Self::Local, worker: usize, range: MorselRange);
    /// Freeze one worker's Local. Parallel across slots; infallible by
    /// contract (operator-side errors are recorded out of band and abort
    /// the RG — the M1 run_morsel discipline).
    fn seal(&self, worker: usize, local: Self::Local) -> Self::Sealed;
    /// Single-threaded post-freeze hook under last-worker-out of the FREEZE
    /// set: every `seal` call has returned, and this runs on the collected
    /// sealed values (worker-slot order) strictly before any combine claim
    /// can observe them (combine deps on the freeze set). The place for
    /// exactly-once whole-census decisions that the per-slot parallel
    /// `seal` cannot make (the agg sink's TRUE-TABLE-ADOPT census and
    /// top-N SEAL-mode resolution). Default: nothing (the distinct sink
    /// needs no census).
    fn sealed_ready(&self, _sealed: &mut Vec<Self::Sealed>) {}
    fn partitions(&self) -> u64;
    fn combine(&self, part: u64, sealed: &[Self::Sealed]);
    fn finalize(&self, sealed: &[Self::Sealed]);
}

struct SealedShared<S: SealedParallelSink> {
    sink: Arc<S>,
    bound: Mutex<Option<Generation>>,
    locals: Box<[Mutex<Option<(Generation, S::Local)>>]>,
    /// Per-slot sealed output (written by the seal task set, one claimer per
    /// slot), collected into `sealed` at that set's last-worker-out.
    sealed_slots: Box<[Mutex<Option<(Generation, S::Sealed)>>]>,
    sealed: Mutex<Option<(Generation, Arc<Vec<S::Sealed>>)>>,
    probe: Arc<SinkProbe>,
}

impl<S: SealedParallelSink> SealedShared<S> {
    fn bound_generation(&self) -> Generation {
        lock(&self.bound).expect(
            "sink task set ran before bind_generation — scheduler publish contract violated",
        )
    }
}

/// ACCEPT: fork-on-first-touch + accept_local (as the base plumbing); the
/// set's finalize is a no-op — sealing belongs to the seal task set.
struct SealedAccept<S: SealedParallelSink>(Arc<SealedShared<S>>);

impl<S: SealedParallelSink> TaskSetWork for SealedAccept<S> {
    fn bind_generation(&self, generation: Generation) {
        *lock(&self.0.bound) = Some(generation);
    }

    fn run_morsel(&self, worker: usize, range: MorselRange) {
        let generation = self.0.bound_generation();
        let mut slot = lock(&self.0.locals[worker]);
        match &*slot {
            Some((g, _)) if *g == generation => {}
            Some(_) => {
                self.0.probe.stale_dropped.fetch_add(1, Ordering::SeqCst);
                *slot = Some((generation, self.0.sink.fork(worker)));
            }
            None => {
                *slot = Some((generation, self.0.sink.fork(worker)));
            }
        }
        let (_, local) = slot.as_mut().expect("just forked");
        self.0.sink.accept_local(local, worker, range);
    }

    fn finalize(&self) {}
}

/// The seal task set: one granule per Local slot; each claim freezes that
/// slot's Local (if any) in parallel. Finalize = SEAL: collect the sealed
/// values in slot order for the combine set.
struct SealedFreeze<S: SealedParallelSink>(Arc<SealedShared<S>>);

impl<S: SealedParallelSink> TaskSetWork for SealedFreeze<S> {
    fn bind_generation(&self, generation: Generation) {
        *lock(&self.0.bound) = Some(generation);
    }

    fn run_morsel(&self, _worker: usize, range: MorselRange) {
        let generation = self.0.bound_generation();
        for i in range {
            let i = i as usize;
            let taken = lock(&self.0.locals[i]).take();
            match taken {
                Some((g, local)) if g == generation => {
                    let sealed = self.0.sink.seal(i, local);
                    *lock(&self.0.sealed_slots[i]) = Some((generation, sealed));
                }
                Some(_) => {
                    self.0.probe.stale_dropped.fetch_add(1, Ordering::SeqCst);
                }
                None => {}
            }
        }
    }

    fn finalize(&self) {
        let generation = self.0.bound_generation();
        let mut sealed = Vec::new();
        for slot in self.0.sealed_slots.iter() {
            match lock(slot).take() {
                Some((g, s)) if g == generation => sealed.push(s),
                Some(_) => {
                    self.0.probe.stale_dropped.fetch_add(1, Ordering::SeqCst);
                }
                None => {}
            }
        }
        // Exactly-once whole-census hook, strictly before any combine claim
        // (single-threaded here by last-worker-out; combine deps=[freeze]).
        self.0.sink.sealed_ready(&mut sealed);
        *lock(&self.0.sealed) = Some((generation, Arc::new(sealed)));
    }
}

/// COMBINE + finalize: identical protocol to the base plumbing's
/// [`SinkCombine`], reading `Sealed` instead of `Local`.
struct SealedCombine<S: SealedParallelSink>(Arc<SealedShared<S>>);

impl<S: SealedParallelSink> SealedCombine<S> {
    fn sealed_for(&self, generation: Generation) -> Option<Arc<Vec<S::Sealed>>> {
        let g = lock(&self.0.sealed);
        match &*g {
            Some((sg, sealed)) if *sg == generation => Some(Arc::clone(sealed)),
            _ => None,
        }
    }
}

impl<S: SealedParallelSink> TaskSetWork for SealedCombine<S> {
    fn bind_generation(&self, generation: Generation) {
        *lock(&self.0.bound) = Some(generation);
    }

    fn run_morsel(&self, _worker: usize, range: MorselRange) {
        let generation = self.0.bound_generation();
        let Some(sealed) = self.sealed_for(generation) else {
            self.0.probe.combine_refusals.fetch_add(1, Ordering::SeqCst);
            return;
        };
        for part in range {
            self.0.sink.combine(part, &sealed);
        }
    }

    fn finalize(&self) {
        let generation = self.0.bound_generation();
        let taken = {
            let mut g = lock(&self.0.sealed);
            match g.take() {
                Some((sg, sealed)) if sg == generation => Some(sealed),
                other => {
                    *g = other;
                    None
                }
            }
        };
        let Some(sealed) = taken else {
            self.0.probe.combine_refusals.fetch_add(1, Ordering::SeqCst);
            return;
        };
        self.0.sink.finalize(&sealed);
        debug_assert_eq!(
            Arc::strong_count(&sealed),
            1,
            "combine task outlived finalize"
        );
    }
}

/// The three task-set specs of one sealed sink. The caller places them
/// CONSECUTIVELY in its [`crate::rg::QuerySpec`] starting at `accept_index`
/// (freeze at `accept_index + 1`, combine at `accept_index + 2`); the dep
/// wiring assumes that layout.
pub struct SealedSinkTaskSets {
    pub accept: TaskSetSpec,
    pub freeze: TaskSetSpec,
    pub combine: TaskSetSpec,
    pub probe: Arc<SinkProbe>,
}

/// Build accept + freeze + combine task sets for a [`SealedParallelSink`].
/// `nslots` sizes the worker-indexed Local slots; callers whose participants
/// are EXTERNAL pinned-drive lanes (M1 helpers) must pass
/// `nthreads + MAX_EXTERNAL_LANES` — external worker indices start at
/// `nthreads` ([`crate::Runtime::external_local`]).
pub fn sealed_sink_tasksets<S: SealedParallelSink + 'static>(
    sink: Arc<S>,
    source: Arc<dyn MorselSource>,
    nslots: usize,
    accept_index: usize,
) -> SealedSinkTaskSets {
    assert!(nslots > 0);
    let parts = sink.partitions();
    assert!(parts > 0, "a SealedParallelSink must expose at least one partition");
    let probe = Arc::new(SinkProbe::default());
    let shared = Arc::new(SealedShared {
        sink,
        bound: Mutex::new(None),
        locals: (0..nslots).map(|_| Mutex::new(None)).collect(),
        sealed_slots: (0..nslots).map(|_| Mutex::new(None)).collect(),
        sealed: Mutex::new(None),
        probe: Arc::clone(&probe),
    });
    let accept = TaskSetSpec {
        source,
        work: Arc::new(SealedAccept(Arc::clone(&shared))),
        deps: Vec::new(),
    };
    let freeze = TaskSetSpec {
        source: Arc::new(PartitionSource { parts: nslots as u64 }),
        work: Arc::new(SealedFreeze(Arc::clone(&shared))),
        deps: vec![accept_index],
    };
    let combine = TaskSetSpec {
        source: Arc::new(PartitionSource { parts }),
        work: Arc::new(SealedCombine(Arc::clone(&shared))),
        deps: vec![accept_index + 1],
    };
    SealedSinkTaskSets { accept, freeze, combine, probe }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::rg::{QuerySpec, RgOutcome};
    use crate::morsel::SyntheticMorselSource;
    use crate::sizing::SizingParams;
    use crate::{Runtime, RuntimeConfig, WorkerPool};
    use std::sync::atomic::{AtomicBool, AtomicU64 as StdAtomicU64, Ordering as StdOrdering};

    /// Toy grouped-sum sink: value of granule g is g+1, grouped by
    /// g % partitions. Verifies every stage of the contract:
    /// fork ≤ nthreads and counted; accept sums locally; combine sums each
    /// partition across Locals exactly once; finalize sums the partitions
    /// once, after all combines.
    struct ToySink {
        parts: u64,
        forks: StdAtomicU64,
        drops: Arc<StdAtomicU64>,
        combined: Vec<StdAtomicU64>,
        combine_calls: Vec<StdAtomicU64>,
        finalizes: StdAtomicU64,
        result: StdAtomicU64,
        locals_seen_at_finalize: StdAtomicU64,
    }

    struct ToyLocal {
        sums: Vec<u64>,
        drops: Arc<StdAtomicU64>,
    }

    impl Drop for ToyLocal {
        fn drop(&mut self) {
            self.drops.fetch_add(1, StdOrdering::SeqCst);
        }
    }

    impl ToySink {
        fn new(parts: u64) -> Arc<ToySink> {
            Arc::new(ToySink {
                parts,
                forks: StdAtomicU64::new(0),
                drops: Arc::new(StdAtomicU64::new(0)),
                combined: (0..parts).map(|_| StdAtomicU64::new(0)).collect(),
                combine_calls: (0..parts).map(|_| StdAtomicU64::new(0)).collect(),
                finalizes: StdAtomicU64::new(0),
                result: StdAtomicU64::new(0),
                locals_seen_at_finalize: StdAtomicU64::new(0),
            })
        }
    }

    impl ParallelSink for ToySink {
        type Local = ToyLocal;

        fn fork(&self, _worker: usize) -> ToyLocal {
            self.forks.fetch_add(1, StdOrdering::SeqCst);
            ToyLocal { sums: vec![0; self.parts as usize], drops: Arc::clone(&self.drops) }
        }

        fn accept_local(&self, local: &mut ToyLocal, _worker: usize, range: MorselRange) {
            for g in range {
                local.sums[(g % self.parts) as usize] += g + 1;
            }
        }

        fn partitions(&self) -> u64 {
            self.parts
        }

        fn combine(&self, part: u64, _worker: usize, locals: &[ToyLocal]) {
            let prev = self.combine_calls[part as usize].fetch_add(1, StdOrdering::SeqCst);
            assert_eq!(prev, 0, "partition {part} combined twice");
            let sum: u64 = locals.iter().map(|l| l.sums[part as usize]).sum();
            self.combined[part as usize].store(sum, StdOrdering::SeqCst);
        }

        fn finalize(&self, locals: &[ToyLocal]) {
            for (p, calls) in self.combine_calls.iter().enumerate() {
                assert_eq!(
                    calls.load(StdOrdering::SeqCst),
                    1,
                    "partition {p} not combined exactly once before finalize"
                );
            }
            self.locals_seen_at_finalize.store(locals.len() as u64, StdOrdering::SeqCst);
            let total: u64 =
                self.combined.iter().map(|c| c.load(StdOrdering::SeqCst)).sum();
            self.result.store(total, StdOrdering::SeqCst);
            self.finalizes.fetch_add(1, StdOrdering::SeqCst);
        }
    }

    fn expected_sum(total: u64) -> u64 {
        total * (total + 1) / 2
    }

    fn toy_spec(sink: &Arc<ToySink>, total: u64, nthreads: usize) -> (QuerySpec, Arc<SinkProbe>) {
        let SinkTaskSets { accept, combine, probe } = sink_tasksets(
            Arc::clone(sink),
            Arc::new(SyntheticMorselSource::new(total)),
            nthreads,
            0,
        );
        let spec = QuerySpec { query_id: 42, tasksets: vec![accept, combine] };
        (spec, probe)
    }

    #[test]
    fn toy_sink_end_to_end_multiworker() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 1,
            slots: 8,
            sizing: SizingParams::default(),
            trace: false,
        });
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let sink = ToySink::new(16);
        let total = 100_000u64;
        let nthreads = rt.nthreads();
        let (spec, probe) = toy_spec(&sink, total, nthreads);
        let (_h, waiter) = rt.submit(spec);
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        pool.shutdown();

        assert_eq!(sink.result.load(StdOrdering::SeqCst), expected_sum(total));
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
        let forks = sink.forks.load(StdOrdering::SeqCst);
        assert!(forks >= 1 && forks <= nthreads as u64, "forks={forks}");
        assert_eq!(sink.locals_seen_at_finalize.load(StdOrdering::SeqCst), forks);
        // All Locals dropped exactly once, after finalize.
        assert_eq!(sink.drops.load(StdOrdering::SeqCst), forks);
        assert_eq!(probe.stale_locals_dropped(), 0);
        assert_eq!(probe.combine_refusals(), 0);
    }

    /// Single manual worker: the seal→combine→finalize sequencing is fully
    /// deterministic (accept finalizes before combine publishes via the
    /// dep DAG; combine claims each partition once; finalize last).
    #[test]
    fn toy_sink_single_worker_deterministic() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 1,
            standbys: 0,
            slots: 4,
            sizing: SizingParams::default(),
            trace: false,
        });
        let sink = ToySink::new(8);
        let total = 4096u64;
        let (spec, probe) = toy_spec(&sink, total, rt.nthreads());
        let (_h, waiter) = rt.submit(spec);
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        assert_eq!(sink.result.load(StdOrdering::SeqCst), expected_sum(total));
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 1);
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
        assert_eq!(probe.stale_locals_dropped(), 0);
        assert_eq!(probe.combine_refusals(), 0);
    }

    /// Zero-granule input: the accept set finalizes with no forks; combine
    /// runs over empty Locals; finalize still runs exactly once.
    #[test]
    fn toy_sink_empty_input() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 1,
            standbys: 0,
            slots: 4,
            sizing: SizingParams::default(),
            trace: false,
        });
        let sink = ToySink::new(4);
        let (spec, _probe) = toy_spec(&sink, 0, rt.nthreads());
        let (_h, waiter) = rt.submit(spec);
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 0);
        assert_eq!(sink.result.load(StdOrdering::SeqCst), 0);
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
    }

    /// Pinned-engagement leg (M2 phase 1, R3(a)): EXTERNAL participants —
    /// worker indexes above nthreads, the M1 bound-helper shape — drive a
    /// pinned RG's sink task sets end to end. Verifies the Local slot array
    /// covers external pin-board lanes, forks ≤ participants, and the full
    /// accept→seal→combine→finalize protocol under drive_pinned.
    #[test]
    fn toy_sink_pinned_external_participants() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 1,
            slots: 8,
            sizing: SizingParams::default(),
            trace: false,
        });
        let sink = ToySink::new(16);
        let total = 100_000u64;
        let nthreads = rt.nthreads();
        let SinkTaskSets { accept, combine, probe } = sink_tasksets(
            Arc::clone(&sink),
            Arc::new(SyntheticMorselSource::new(total)),
            nthreads,
            0,
        );
        let (handle, waiter) = rt.submit_pinned(QuerySpec {
            query_id: 43,
            tasksets: vec![accept, combine],
        });
        let helpers = 3usize;
        std::thread::scope(|scope| {
            for _ in 0..helpers {
                let rt = Arc::clone(&rt);
                let handle = handle.clone();
                scope.spawn(move || {
                    let lane = rt.acquire_external_lane().expect("external lane");
                    let mut local = lane.local();
                    let outcome = rt.drive_pinned(&mut local, &handle);
                    assert_eq!(outcome, RgOutcome::Completed);
                });
            }
        });
        assert_eq!(waiter.wait(), RgOutcome::Completed);

        assert_eq!(sink.result.load(StdOrdering::SeqCst), expected_sum(total));
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
        let forks = sink.forks.load(StdOrdering::SeqCst);
        assert!(forks >= 1 && forks <= helpers as u64, "forks={forks}");
        assert_eq!(sink.locals_seen_at_finalize.load(StdOrdering::SeqCst), forks);
        assert_eq!(sink.drops.load(StdOrdering::SeqCst), forks);
        assert_eq!(probe.stale_locals_dropped(), 0);
        assert_eq!(probe.combine_refusals(), 0);
    }

    /// H1 leg 1: an aborted RG never combines and never finalizes; every
    /// forked Local is dropped (unconsumable, discarded with the RG).
    #[test]
    fn toy_sink_abort_never_combines_or_finalizes() {
        struct AbortingSink {
            inner: Arc<ToySink>,
            started: AtomicBool,
        }
        impl ParallelSink for AbortingSink {
            type Local = ToyLocal;
            fn fork(&self, w: usize) -> ToyLocal {
                self.inner.fork(w)
            }
            fn accept_local(&self, local: &mut ToyLocal, w: usize, range: MorselRange) {
                self.started.store(true, StdOrdering::SeqCst);
                // Slow morsels so the abort lands mid-accept.
                std::thread::sleep(std::time::Duration::from_micros(50));
                self.inner.accept_local(local, w, range);
            }
            fn partitions(&self) -> u64 {
                self.inner.partitions()
            }
            fn combine(&self, _part: u64, _worker: usize, _locals: &[ToyLocal]) {
                panic!("aborted sink must never combine");
            }
            fn finalize(&self, _locals: &[ToyLocal]) {
                panic!("aborted sink must never finalize");
            }
        }

        let rt = Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 1,
            slots: 8,
            sizing: SizingParams::default(),
            trace: false,
        });
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let inner = ToySink::new(8);
        let sink = Arc::new(AbortingSink {
            inner: Arc::clone(&inner),
            started: AtomicBool::new(false),
        });
        let SinkTaskSets { accept, combine, probe: _probe } = sink_tasksets(
            Arc::clone(&sink),
            Arc::new(SyntheticMorselSource::new(1 << 24).with_c0(1)),
            rt.nthreads(),
            0,
        );
        let (handle, waiter) = rt.submit(QuerySpec {
            query_id: 7,
            tasksets: vec![accept, combine],
        });
        while !sink.started.load(StdOrdering::SeqCst) {
            std::hint::spin_loop();
        }
        handle.abort();
        assert_eq!(waiter.wait(), RgOutcome::Aborted);
        // Join workers first (their slot caches hold TaskSetRt→RG Arcs),
        // then release our RG handles: the aborted RG's task-set specs are
        // the last owners of the plumbing's SinkShared, whose Local slots
        // still hold the un-sealed (never-consumed) partials.
        pool.shutdown();
        drop(handle);
        drop(waiter);
        drop(sink);

        // No combine/finalize ran (the panics above would have fired), and
        // every forked Local was dropped un-consumed.
        let forks = inner.forks.load(StdOrdering::SeqCst);
        assert!(forks >= 1);
        assert_eq!(inner.drops.load(StdOrdering::SeqCst), forks);
        assert_eq!(inner.finalizes.load(StdOrdering::SeqCst), 0);
        for c in &inner.combine_calls {
            assert_eq!(c.load(StdOrdering::SeqCst), 0);
        }
    }

    /// H1 leg 2 (the mechanism itself, driven directly): a Local surviving
    /// from a dead generation is dropped and re-forked on the next accept,
    /// and SEAL never collects it — cross-generation partial state is
    /// unconsumable by construction.
    #[test]
    fn stale_generation_locals_are_unconsumable() {
        let sink = ToySink::new(4);
        let probe = Arc::new(SinkProbe::default());
        let shared = Arc::new(SinkShared {
            sink: Arc::clone(&sink),
            bound: Mutex::new(None),
            locals: (0..2).map(|_| Mutex::new(None)).collect(),
            sealed: Mutex::new(None),
            probe: Arc::clone(&probe),
        });
        let accept = SinkAccept(Arc::clone(&shared));

        // Generation A: worker 0 forks and accepts, then the generation dies
        // WITHOUT sealing (aborted init — the H1 scenario).
        accept.bind_generation(Generation(1));
        accept.run_morsel(0, 0..8);
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 1);

        // Generation B binds (re-publish). Worker 0's stale Local must be
        // dropped and re-forked, its partial sums never accepted into.
        accept.bind_generation(Generation(2));
        accept.run_morsel(0, 0..4);
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 2, "stale Local must be re-forked");
        assert_eq!(sink.drops.load(StdOrdering::SeqCst), 1, "stale Local must be dropped");
        assert_eq!(probe.stale_locals_dropped(), 1);

        // SEAL under generation B collects exactly the one B Local; its sums
        // reflect only B's accepts (granules 0..4: 1+2+3+4 = 10).
        accept.finalize();
        let sealed = lock(&shared.sealed);
        let (g, locals) = sealed.as_ref().expect("sealed");
        assert_eq!(*g, Generation(2));
        assert_eq!(locals.len(), 1);
        assert_eq!(locals[0].sums.iter().sum::<u64>(), 10);
    }

    /// Fail-closed: combine against a seal from a different generation is a
    /// no-op (never merges), finalize refuses to consume it, both tick the
    /// refusal probe.
    #[test]
    fn combine_refuses_mismatched_seal() {
        let sink = ToySink::new(4);
        let probe = Arc::new(SinkProbe::default());
        let shared = Arc::new(SinkShared {
            sink: Arc::clone(&sink),
            bound: Mutex::new(None),
            locals: (0..1).map(|_| Mutex::new(None)).collect(),
            sealed: Mutex::new(Some((Generation(1), Arc::new(Vec::new())))),
            probe: Arc::clone(&probe),
        });
        let combine = SinkCombine(Arc::clone(&shared));
        combine.bind_generation(Generation(2));
        combine.run_morsel(0, 0..4);
        for c in &sink.combine_calls {
            assert_eq!(c.load(StdOrdering::SeqCst), 0);
        }
        combine.finalize();
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 0);
        assert!(lock(&shared.sealed).is_some(), "mismatched seal must be left in place");
        assert_eq!(probe.combine_refusals(), 2);
    }

    // ---- sealed (three-set) variant ---------------------------------------

    /// Toy sealed grouped-sum sink: as [`ToySink`] but the Local must be
    /// FROZEN (sums copied into a `Sealed` wire vec) before combine may read
    /// it. Verifies fork/accept/seal-exactly-once-per-local/combine-exactly-
    /// once-per-partition/finalize-once sequencing and the drop census.
    struct ToySealedSink {
        parts: u64,
        forks: StdAtomicU64,
        seals: StdAtomicU64,
        drops: Arc<StdAtomicU64>,
        combined: Vec<StdAtomicU64>,
        combine_calls: Vec<StdAtomicU64>,
        finalizes: StdAtomicU64,
        result: StdAtomicU64,
        sealed_seen_at_finalize: StdAtomicU64,
    }

    struct ToySealed {
        sums: Vec<u64>,
    }

    impl ToySealedSink {
        fn new(parts: u64) -> Arc<ToySealedSink> {
            Arc::new(ToySealedSink {
                parts,
                forks: StdAtomicU64::new(0),
                seals: StdAtomicU64::new(0),
                drops: Arc::new(StdAtomicU64::new(0)),
                combined: (0..parts).map(|_| StdAtomicU64::new(0)).collect(),
                combine_calls: (0..parts).map(|_| StdAtomicU64::new(0)).collect(),
                finalizes: StdAtomicU64::new(0),
                result: StdAtomicU64::new(0),
                sealed_seen_at_finalize: StdAtomicU64::new(0),
            })
        }
    }

    impl SealedParallelSink for ToySealedSink {
        type Local = ToyLocal;
        type Sealed = ToySealed;

        fn fork(&self, _worker: usize) -> ToyLocal {
            self.forks.fetch_add(1, StdOrdering::SeqCst);
            ToyLocal { sums: vec![0; self.parts as usize], drops: Arc::clone(&self.drops) }
        }

        fn accept_local(&self, local: &mut ToyLocal, _worker: usize, range: MorselRange) {
            for g in range {
                local.sums[(g % self.parts) as usize] += g + 1;
            }
        }

        fn seal(&self, _worker: usize, local: ToyLocal) -> ToySealed {
            self.seals.fetch_add(1, StdOrdering::SeqCst);
            ToySealed { sums: local.sums.clone() }
        }

        fn partitions(&self) -> u64 {
            self.parts
        }

        fn combine(&self, part: u64, sealed: &[ToySealed]) {
            let prev = self.combine_calls[part as usize].fetch_add(1, StdOrdering::SeqCst);
            assert_eq!(prev, 0, "partition {part} combined twice");
            let sum: u64 = sealed.iter().map(|s| s.sums[part as usize]).sum();
            self.combined[part as usize].store(sum, StdOrdering::SeqCst);
        }

        fn finalize(&self, sealed: &[ToySealed]) {
            for (p, calls) in self.combine_calls.iter().enumerate() {
                assert_eq!(
                    calls.load(StdOrdering::SeqCst),
                    1,
                    "partition {p} not combined exactly once before finalize"
                );
            }
            self.sealed_seen_at_finalize.store(sealed.len() as u64, StdOrdering::SeqCst);
            let total: u64 = self.combined.iter().map(|c| c.load(StdOrdering::SeqCst)).sum();
            self.result.store(total, StdOrdering::SeqCst);
            self.finalizes.fetch_add(1, StdOrdering::SeqCst);
        }
    }

    fn toy_sealed_spec(
        sink: &Arc<ToySealedSink>,
        total: u64,
        nslots: usize,
    ) -> (QuerySpec, Arc<SinkProbe>) {
        let SealedSinkTaskSets { accept, freeze, combine, probe } = sealed_sink_tasksets(
            Arc::clone(sink),
            Arc::new(SyntheticMorselSource::new(total)),
            nslots,
            0,
        );
        let spec = QuerySpec { query_id: 43, tasksets: vec![accept, freeze, combine] };
        (spec, probe)
    }

    #[test]
    fn sealed_sink_end_to_end_multiworker() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 1,
            slots: 8,
            sizing: SizingParams::default(),
            trace: false,
        });
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let sink = ToySealedSink::new(16);
        let total = 100_000u64;
        let nslots = rt.nthreads();
        let (spec, probe) = toy_sealed_spec(&sink, total, nslots);
        let (_h, waiter) = rt.submit(spec);
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        pool.shutdown();

        assert_eq!(sink.result.load(StdOrdering::SeqCst), expected_sum(total));
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
        let forks = sink.forks.load(StdOrdering::SeqCst);
        assert!(forks >= 1 && forks <= nslots as u64, "forks={forks}");
        assert_eq!(sink.seals.load(StdOrdering::SeqCst), forks, "seal once per forked Local");
        assert_eq!(sink.sealed_seen_at_finalize.load(StdOrdering::SeqCst), forks);
        // Every Local dropped exactly once (consumed by seal).
        assert_eq!(sink.drops.load(StdOrdering::SeqCst), forks);
        assert_eq!(probe.stale_locals_dropped(), 0);
        assert_eq!(probe.combine_refusals(), 0);
    }

    #[test]
    fn sealed_sink_single_worker_deterministic() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 1,
            standbys: 0,
            slots: 4,
            sizing: SizingParams::default(),
            trace: false,
        });
        let sink = ToySealedSink::new(8);
        let total = 4096u64;
        let (spec, probe) = toy_sealed_spec(&sink, total, rt.nthreads());
        let (_h, waiter) = rt.submit(spec);
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        assert_eq!(sink.result.load(StdOrdering::SeqCst), expected_sum(total));
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 1);
        assert_eq!(sink.seals.load(StdOrdering::SeqCst), 1);
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
        assert_eq!(probe.stale_locals_dropped(), 0);
        assert_eq!(probe.combine_refusals(), 0);
    }

    /// Zero-granule input: no forks, no seals; combine over an empty sealed
    /// vec; finalize exactly once.
    #[test]
    fn sealed_sink_empty_input() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 1,
            standbys: 0,
            slots: 4,
            sizing: SizingParams::default(),
            trace: false,
        });
        let sink = ToySealedSink::new(4);
        let (spec, _probe) = toy_sealed_spec(&sink, 0, rt.nthreads());
        let (_h, waiter) = rt.submit(spec);
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 0);
        assert_eq!(sink.seals.load(StdOrdering::SeqCst), 0);
        assert_eq!(sink.result.load(StdOrdering::SeqCst), 0);
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 1);
    }

    /// H1 for the sealed plumbing, driven directly: stale-generation Locals
    /// are dropped un-sealed by the freeze set; SEAL collects only matching
    /// generations; the combine set fail-closes on a mismatched seal.
    #[test]
    fn sealed_stale_generation_unconsumable() {
        let sink = ToySealedSink::new(4);
        let probe = Arc::new(SinkProbe::default());
        let shared = Arc::new(SealedShared {
            sink: Arc::clone(&sink),
            bound: Mutex::new(None),
            locals: (0..2).map(|_| Mutex::new(None)).collect(),
            sealed_slots: (0..2).map(|_| Mutex::new(None)).collect(),
            sealed: Mutex::new(None),
            probe: Arc::clone(&probe),
        });
        let accept = SealedAccept(Arc::clone(&shared));
        let freeze = SealedFreeze(Arc::clone(&shared));

        accept.bind_generation(Generation(1));
        accept.run_morsel(0, 0..8);
        assert_eq!(sink.forks.load(StdOrdering::SeqCst), 1);

        // Re-publish under generation 2; the freeze set must drop the stale
        // Local un-sealed.
        freeze.bind_generation(Generation(2));
        freeze.run_morsel(0, 0..2);
        assert_eq!(sink.seals.load(StdOrdering::SeqCst), 0, "stale Local must not seal");
        assert_eq!(probe.stale_locals_dropped(), 1);
        freeze.finalize();
        {
            let sealed = lock(&shared.sealed);
            let (g, v) = sealed.as_ref().expect("sealed");
            assert_eq!(*g, Generation(2));
            assert!(v.is_empty());
        }

        // A combine bound to a third generation refuses the seal.
        let combine = SealedCombine(Arc::clone(&shared));
        combine.bind_generation(Generation(3));
        combine.run_morsel(0, 0..4);
        for c in &sink.combine_calls {
            assert_eq!(c.load(StdOrdering::SeqCst), 0);
        }
        combine.finalize();
        assert_eq!(sink.finalizes.load(StdOrdering::SeqCst), 0);
        assert_eq!(probe.combine_refusals(), 2);
    }
}
