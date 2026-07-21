//! Runtime Funnel — the parallel row-EMIT boundary (gather-elimination Phase 2,
//! the one genuinely NEW subsystem).
//!
//! PROVENANCE: `scratchpad/night/research-parallelize-rowemit.md` §4 ("Runtime
//! Funnel") + `gather-elimination-implementation-plan.md` §Phase-2. Everything
//! here is new work; it ports the *policy* of `nodegather.rs::gather_readnext`
//! (round-robin, stick-until-block, drop-drained-reader) and the LIMIT
//! push-down shape, over an in-process transport instead of `shm_mq`.
//!
//! # Why this is a new subsystem, not a new `ParallelSink`
//!
//! Every existing runtime taskset is a **pipeline breaker**: N workers fold
//! morsels into thread-local partials, then a last-worker-out SEAL + COMBINE +
//! finalize emits a small folded result (`sink.rs`). Row emission cannot fold —
//! it must interleave PRODUCTION and CONSUMPTION: workers produce rows into
//! buffers while a consumer drains them to the client wire concurrently. This
//! is the first **non-breaker** (streaming) taskset. See the invariant analysis
//! below for why a concurrent drain does not violate the last-worker-out /
//! generation protocol.
//!
//! # Shape
//!
//! ```text
//!   worker 0 ─push─▶ SpscRing 0 ─┐
//!   worker 1 ─push─▶ SpscRing 1 ─┤ round-robin
//!      …                         ├── FunnelDrain ──▶ leader (pure consumer)
//!   worker k ─push─▶ SpscRing k ─┘   (stick-until-block)      │
//!                                                     dest.receive_slot(wire)
//! ```
//!
//! - **Transport = in-process bounded SPSC ring** (this module), NOT `shm_mq`:
//!   the runtime is threads-in-one-process, so payloads cross by ownership/
//!   pointer with no shared-memory ring copy (research §3 "Replace the
//!   transport"). Bounded capacity IS the memory budget and the back-pressure
//!   knob.
//! - **Producer side** = a worker's [`FunnelProducer`]: append; when the ring
//!   is full, [`FunnelProducer::push_blocking`] parks the worker (the caller
//!   brackets the park in the K-standby blocking-permit section so a standby
//!   absorbs the core — `crate::blocking`). A parked producer wakes when the
//!   drain frees a slot, or when demand is closed (LIMIT).
//! - **Consumer side** = the leader's [`FunnelDrain`]: a PURE consumer running
//!   the ported `gather_readnext` policy. It never produces, never claims a
//!   morsel, never touches the pin board or `fin_counter`.
//!
//! # Ordering
//!
//! UNORDERED (arrival order, like `Gather`) is the ONLY mode built here — the
//! biggest coverage gap (`scan-passthrough`) and the deadlock-safe baseline.
//! Ordered emit (the `nodegathermerge` binary heap over these same rings) is a
//! later increment; the ring transport is order-source-agnostic so it drops in.
//!
//! # INVARIANT ANALYSIS — streaming taskset vs. last-worker-out / generation
//!
//! The `sched.rs` last-worker-out protocol and the `sink.rs` H1 generation
//! discipline were proven for BREAKERS (fold → SEAL → single-writer COMBINE →
//! at-most-once finalize). A concurrently-draining taskset is a new lifecycle
//! shape. The correctness argument that it does NOT violate those invariants:
//!
//! 1. **The drain is invisible to the finalization protocol.** Last-worker-out
//!    is a protocol among POOL WORKERS via the pin board + `fin_counter`
//!    (`taskset.rs`): a coordinator marks pinned workers, each marked worker
//!    decrements on finishing its in-flight morsel, whoever hits zero runs
//!    `finalize()` exactly once. The funnel drain is the LEADER acting as a
//!    pure consumer: it is not a pool worker, holds no pin-board entry, and
//!    never touches `fin_counter`/`cursor`/the sizer. So every last-worker-out
//!    invariant is preserved unchanged — the drain is a reader of SPSC rings
//!    that the protocol does not know exists.
//!
//! 2. **`finalize()` becomes a no-op join point, not a COMBINE.** A row-emit
//!    taskset has nothing to fold, so its `finalize()` does no COMBINE; its
//!    only job is to publish "all producers done" (mark every ring done) so the
//!    drain reaches EOF. This runs AFTER the last worker's last morsel, i.e.
//!    after every producer has stopped pushing — so it cannot race a live
//!    producer. The rings outlive `finalize()` (owned by the funnel `Arc`, not
//!    by any worker) and the drain keeps consuming buffered rows until each ring
//!    is done AND empty.
//!
//! 3. **Generation keying is preserved.** The rings are allocated per taskset
//!    publish and keyed by generation exactly like fold partials
//!    (`TaskSetWork::bind_generation`): a stale generation's ring is never
//!    entered because the fail-closed armed join refuses the task. A row
//!    emitted BEFORE finalize is safe because it is a COPY/owned MinimalTuple
//!    in the ring, not a borrow of per-worker fold state that COMBINE would
//!    later touch — there is no COMBINE to race. (This is the H1 re-proof the
//!    research flagged as highest-risk: emitting before finalize is sound
//!    precisely because emission transfers OWNERSHIP into the ring.)
//!
//! 4. **No back-pressure deadlock — because the leader is a PURE drain.**
//!    A producer parks only when its ring is FULL; a full ring always has data
//!    to drain, so the drain can always make progress. The drain parks only
//!    when EVERY live ring is EMPTY; an empty ring means its producer is
//!    running (or done), never parked-on-full. So "all producers parked" and
//!    "drain parked" cannot hold simultaneously: if any producer is parked its
//!    ring is full → the drain is runnable, not parked. The only remaining wait
//!    is the drain blocking on a slow CLIENT (`receive_slot`), which is the
//!    intended end-to-end back-pressure: client unblocks → drain consumes →
//!    ring frees → producer wakes. A cycle would require the drain to also be a
//!    PRODUCER (needing a full ring to drain before it can produce); keeping
//!    the leader a pure consumer removes that edge. This is why leader
//!    participation is deliberately NOT implemented (a defensible divergence
//!    from PG's `parallel_leader_participation`; better for high-fan-out where
//!    the leader is wire-bound anyway).
//!
//! 5. **LIMIT / early stop is cooperative and lost-wake-free.** The drain, on
//!    reaching the bound, calls [`RowFunnel::close_demand`] which sets a flag
//!    and wakes every producer parklot. A producer checks the flag at every
//!    ring-full park wait and at claim boundaries; a parked-on-full producer
//!    wakes, sees closed, stops, and marks its ring done. The wake bumps the
//!    parklot epoch BEFORE notifying (ParkLot's lost-wakeup-free protocol), so
//!    a producer about to park after the close still observes it. We wake
//!    EAGERLY (per push / per close), deliberately NOT PG14's "wake at ¼-full"
//!    batching, which the research flagged as a LIMIT-latency pothole.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::morsel::MorselRange;
use crate::rg::TaskSetWork;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::ParkLot;
use crate::taskset::CachePadded;

/// Bounded lock-free single-producer / single-consumer ring of owned `T`.
///
/// SPSC DISCIPLINE (caller-enforced): exactly one thread calls the producer
/// methods ([`try_push`](SpscRing::try_push)) and exactly one calls the
/// consumer methods ([`try_pop`](SpscRing::try_pop)) for a given ring. The
/// producer owns `tail`, the consumer owns `head`; each publishes its own
/// index with `Release` and reads the other's with `Acquire`, so no CAS and no
/// lock is on the row path (the `accept_local` no-locks-on-row-path discipline).
///
/// `not_full` is the producer's park lot: the consumer wakes it after freeing a
/// slot from a full ring. The consumer's empty-park is coordinated at the
/// [`RowFunnel`] level (it waits across all rings at once).
pub struct SpscRing<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Capacity (power of two). `len == cap` ⇒ full; `len == 0` ⇒ empty.
    cap: usize,
    mask: usize,
    /// Consumer-owned monotonic read index.
    head: CachePadded<AtomicUsize>,
    /// Producer-owned monotonic write index.
    tail: CachePadded<AtomicUsize>,
    /// Producer park lot (woken by the consumer when a full ring frees a slot,
    /// and by [`RowFunnel::close_demand`]).
    not_full: ParkLot,
}

// SAFETY: `T: Send` crosses the producer→consumer boundary by ownership; the
// head/tail acquire/release handshake publishes the slot write before the
// consumer reads it and the slot read before the producer overwrites it. The
// UnsafeCell is only ever touched by the single producer (write at tail) or the
// single consumer (read at head), never both for the same index concurrently.
unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> SpscRing<T> {
    /// New ring with capacity `cap_pow2` (rounded up to a power of two, min 2).
    pub fn new(cap_pow2: usize) -> SpscRing<T> {
        let cap = cap_pow2.max(2).next_power_of_two();
        let buf = (0..cap).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        SpscRing {
            buf,
            cap,
            mask: cap - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            not_full: ParkLot::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Occupancy as seen by the caller (exact for the owning side, an estimate
    /// for the other).
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.cap
    }

    /// PRODUCER: append `v`. `Err(v)` returns it back when the ring is full.
    pub fn try_push(&self, v: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.cap {
            return Err(v);
        }
        // SAFETY: single producer owns `tail`; the slot at `tail & mask` is free
        // (occupancy < cap) and not being read by the consumer (it reads only
        // indices < tail via the Acquire load above).
        unsafe {
            (*self.buf[tail & self.mask].get()).write(v);
        }
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// CONSUMER: pop the oldest `T`, or `None` when empty. On draining a slot
    /// that was full, wake the producer.
    pub fn try_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        let occ = tail.wrapping_sub(head);
        if occ == 0 {
            return None;
        }
        // SAFETY: single consumer owns `head`; occupancy > 0 so the slot at
        // `head & mask` holds an initialized `T` published by the producer's
        // Release store to `tail`, which our Acquire load synchronized with.
        let v = unsafe { (*self.buf[head & self.mask].get()).assume_init_read() };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        // Wake a producer parked on full. UNCONDITIONAL (not gated on "was the
        // ring full") on purpose: the consumer's Acquire load of `tail` can lag
        // the producer's Release store, so a "was full" test (`occ >= cap`) can
        // MISS a pop-from-full and lose the wake — the producer would then sleep
        // forever while the drain sleeps on empty (a real double-park deadlock,
        // observed and fixed here). Every pop frees a slot, so every pop is a
        // valid wake; ParkLot's epoch makes a spurious wake (no parked producer)
        // a cheap no-op. PERF-TODO (production wiring): gate on a SeqCst
        // "producer waiting" flag (Dekker handshake) to drop the per-row
        // notify; correctness-first here since the funnel ships OFF by default.
        self.not_full.wake_all();
        Some(v)
    }

    /// PRODUCER: epoch to capture before checking full and parking (ParkLot's
    /// lost-wakeup-free protocol).
    pub fn producer_epoch(&self) -> u64 {
        self.not_full.epoch()
    }

    /// PRODUCER: park until the consumer frees a slot (or a `close_demand`
    /// wake), provided the epoch is unchanged since `seen`.
    pub fn producer_park(&self, seen: u64) {
        self.not_full.park(seen);
    }

    /// Wake the producer unconditionally (used by [`RowFunnel::close_demand`]).
    pub fn wake_producer(&self) {
        self.not_full.wake_all();
    }
}

impl<T> Drop for SpscRing<T> {
    fn drop(&mut self) {
        // Drop the initialized head..tail range so owned payloads (e.g. tuple
        // chunks) are released. Single-owner at drop time (no concurrent side).
        while let Some(v) = self.try_pop() {
            drop(v);
        }
    }
}

/// The whole parallel row-emit boundary for one taskset: one [`SpscRing`] per
/// worker, a demand-closed flag (LIMIT), and the drain's empty-park lot.
///
/// The funnel `Arc` OUTLIVES the workers: the rings must survive `finalize()`
/// so the drain can keep consuming buffered rows after the last producer has
/// stopped (invariant #2 above).
pub struct RowFunnel<T> {
    rings: Vec<Arc<SpscRing<T>>>,
    /// Per-ring "producer finished" flag. Set by the producer when its morsel
    /// source is exhausted or demand closed; read by the drain to drop the ring
    /// from rotation once it is also empty (mirrors `gather_readnext`'s `done`).
    done: Vec<AtomicBool>,
    /// LIMIT / early-stop: once set, producers stop and the drain reaches EOF.
    demand_closed: AtomicBool,
    /// The drain's empty-park lot: woken by any producer that pushes into a
    /// previously-empty ring or marks a ring done.
    not_empty: ParkLot,
}

impl<T: Send + 'static> RowFunnel<T> {
    /// One ring per worker, each of capacity `ring_cap` (rounded to a power of
    /// two). `ring_cap` is the per-worker memory/back-pressure budget.
    pub fn new(nworkers: usize, ring_cap: usize) -> Arc<RowFunnel<T>> {
        Arc::new(RowFunnel {
            rings: (0..nworkers).map(|_| Arc::new(SpscRing::new(ring_cap))).collect(),
            done: (0..nworkers).map(|_| AtomicBool::new(false)).collect(),
            demand_closed: AtomicBool::new(false),
            not_empty: ParkLot::new(),
        })
    }

    pub fn nworkers(&self) -> usize {
        self.rings.len()
    }

    /// True once LIMIT has been satisfied by the drain (producers must stop).
    pub fn demand_closed(&self) -> bool {
        self.demand_closed.load(Ordering::Acquire)
    }

    /// LEADER/drain: close demand (LIMIT satisfied). Wakes every producer so a
    /// parked-on-full producer promptly sees the close and stops. Idempotent.
    pub fn close_demand(&self) {
        self.demand_closed.store(true, Ordering::Release);
        for r in &self.rings {
            r.wake_producer();
        }
        self.not_empty.wake_all();
    }

    /// Mark EVERY ring producer-finished. Called by the row-emit taskset's
    /// `finalize()` (the last-worker-out join point): by the time finalize
    /// runs, every worker has settled its last morsel (the last-worker-out
    /// protocol gates on it), so no producer is still pushing — this is the
    /// streaming taskset's "finalize = no-op join that publishes done" contract
    /// (funnel.rs invariant #2). Wakes the drain so it reaches EOF once each
    /// ring is also drained.
    pub fn mark_all_done(&self) {
        for d in &self.done {
            d.store(true, Ordering::Release);
        }
        self.not_empty.wake_all();
    }

    /// A producer handle bound to worker `w`'s ring. One handle per worker
    /// (SPSC discipline).
    pub fn producer(self: &Arc<Self>, w: usize) -> FunnelProducer<T> {
        FunnelProducer { funnel: Arc::clone(self), ring: Arc::clone(&self.rings[w]), w }
    }

    /// The leader's pure-consumer drain over all rings. At most ONE may exist
    /// (SPSC consumer discipline across every ring).
    pub fn drain(self: &Arc<Self>) -> FunnelDrain<T> {
        FunnelDrain::new(Arc::clone(self))
    }
}

/// Worker-side append handle for one ring (SPSC producer).
pub struct FunnelProducer<T> {
    funnel: Arc<RowFunnel<T>>,
    ring: Arc<SpscRing<T>>,
    w: usize,
}

/// Outcome of a blocking push.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Row buffered.
    Pushed,
    /// Demand closed (LIMIT) — the producer must stop; the row was NOT buffered.
    DemandClosed,
}

impl<T: Send + 'static> FunnelProducer<T> {
    /// Non-blocking append. `Err(v)` = ring full (caller may park or retry).
    pub fn try_push(&self, v: T) -> Result<(), T> {
        let r = self.ring.try_push(v);
        if r.is_ok() {
            // Wake a drain parked on all-rings-empty. UNCONDITIONAL for the same
            // reason as `try_pop`'s producer wake: a "was empty" transition test
            // races the drain's index reads and can lose the wake (double-park).
            // Every push adds a row, so every push is a valid wake; a spurious
            // one (no parked drain) is a cheap ParkLot no-op. Same PERF-TODO.
            self.funnel.not_empty.wake_all();
        }
        r
    }

    /// Append, parking while the ring is full. Returns
    /// [`PushOutcome::DemandClosed`] if LIMIT closed demand before the row could
    /// be buffered — the producer must then stop and
    /// [`mark_done`](FunnelProducer::mark_done).
    ///
    /// `mk_section` produces a guard that is HELD ACROSS the park and dropped
    /// when the producer wakes: on a real pool worker this is
    /// `crate::blocking::blocking_io_section()`, which donates the execution
    /// permit for the duration of the park (a K-standby absorbs the core) and
    /// reacquires it on drop. In tests / off a pool worker, `|| ()` is a no-op
    /// guard.
    pub fn push_blocking<G>(&self, mut v: T, mut mk_section: impl FnMut() -> G) -> PushOutcome {
        loop {
            if self.funnel.demand_closed() {
                return PushOutcome::DemandClosed;
            }
            match self.try_push(v) {
                Ok(()) => return PushOutcome::Pushed,
                Err(back) => {
                    v = back;
                    // Capture the epoch BEFORE re-checking full, then park:
                    // a consumer pop (or close) between the check and the park
                    // bumps the epoch and cannot be lost.
                    let seen = self.ring.producer_epoch();
                    if !self.ring.is_full() || self.funnel.demand_closed() {
                        continue;
                    }
                    // Donate the permit for the duration of the park (held
                    // across `producer_park`, reacquired when `_section` drops).
                    let _section = mk_section();
                    self.ring.producer_park(seen);
                }
            }
        }
    }

    /// Mark this worker's ring producer-finished (morsel source exhausted, or
    /// stopping on demand-closed). The drain drops the ring from rotation once
    /// it is also drained. Wakes the drain so EOF is observed promptly.
    pub fn mark_done(&self) {
        self.funnel.done[self.w].store(true, Ordering::Release);
        self.funnel.not_empty.wake_all();
    }
}

/// The row-emit taskset's WORK body (the scheduler-side producer wiring): each
/// claimed morsel range is turned into rows by `produce`, which pushes them into
/// the claiming worker's ring. This is the runtime's first NON-BREAKER
/// `TaskSetWork` — it streams into the funnel instead of folding into a partial.
///
/// - `run_morsel` runs on a pool worker (worker index = its ring); it skips work
///   once demand is closed (LIMIT), so a stalled/last morsel stops promptly.
/// - `finalize` (last-worker-out, single thread) marks every ring done so the
///   leader drain reaches EOF — the streaming taskset's finalize contract.
///
/// `produce(worker, range, &producer)` MUST push via
/// [`FunnelProducer::push_blocking`] (so a full ring parks the worker under the
/// blocking permit) and stop early if it returns [`PushOutcome::DemandClosed`].
pub struct RowEmitWork<T, F>
where
    T: Send + 'static,
    F: Fn(usize, MorselRange, &FunnelProducer<T>) + Send + Sync,
{
    funnel: Arc<RowFunnel<T>>,
    produce: F,
}

impl<T, F> RowEmitWork<T, F>
where
    T: Send + 'static,
    F: Fn(usize, MorselRange, &FunnelProducer<T>) + Send + Sync,
{
    pub fn new(funnel: Arc<RowFunnel<T>>, produce: F) -> Arc<RowEmitWork<T, F>> {
        Arc::new(RowEmitWork { funnel, produce })
    }
}

impl<T, F> TaskSetWork for RowEmitWork<T, F>
where
    T: Send + 'static,
    F: Fn(usize, MorselRange, &FunnelProducer<T>) + Send + Sync,
{
    fn run_morsel(&self, worker: usize, range: MorselRange) {
        // LIMIT already satisfied: drop this claim without producing (the drain
        // closed demand; the parked/subsequent producers stop cooperatively).
        if self.funnel.demand_closed() {
            return;
        }
        let producer = self.funnel.producer(worker);
        (self.produce)(worker, range, &producer);
    }

    fn finalize(&self) {
        self.funnel.mark_all_done();
    }
}

/// One step of the drain.
#[derive(Debug)]
pub enum DrainStep<T> {
    /// A row for the wire.
    Row(T),
    /// Every ring is currently empty but at least one producer is still live —
    /// the caller should park via [`FunnelDrain::park`] (after re-checking) or
    /// do leader-local work. Never returned once EOF is reachable.
    Idle,
    /// Every ring is done AND drained — end of stream.
    Eof,
}

/// LEADER-side pure-consumer drain. Ports `nodegather.rs::gather_readnext`:
/// round-robin, keep draining one ring until it would block, drop a ring once
/// it is done+empty, and (optionally) a fairness stride so no single producer
/// is drained exclusively.
pub struct FunnelDrain<T> {
    funnel: Arc<RowFunnel<T>>,
    /// Active ring indices still in rotation (a done+empty ring is removed —
    /// `gather_readnext`'s `reader.remove(nextreader)`).
    active: Vec<usize>,
    /// Rotation cursor into `active` (`gather_readnext`'s `nextreader`).
    next: usize,
    /// Fairness stride (0 = off = C's behavior: never rotate on a successful
    /// read; drain one ring until it blocks). >0 rotates after that many
    /// consecutive rows (`gather_readnext`'s `fair_stride`).
    fair_stride: i64,
    stride_rem: i64,
}

impl<T: Send + 'static> FunnelDrain<T> {
    fn new(funnel: Arc<RowFunnel<T>>) -> FunnelDrain<T> {
        let n = funnel.nworkers();
        FunnelDrain { funnel, active: (0..n).collect(), next: 0, fair_stride: 0, stride_rem: 0 }
    }

    /// Set the fairness stride (rotate after N consecutive rows from one ring).
    pub fn with_fair_stride(mut self, stride: i64) -> FunnelDrain<T> {
        self.fair_stride = stride.max(0);
        self.stride_rem = self.fair_stride;
        self
    }

    /// Epoch to capture before an [`DrainStep::Idle`] park.
    pub fn park_epoch(&self) -> u64 {
        self.funnel.not_empty.epoch()
    }

    /// Park the drain until a producer pushes or marks a ring done (used on
    /// [`DrainStep::Idle`] when there is no leader-local work).
    pub fn park(&self, seen: u64) {
        self.funnel.not_empty.park(seen);
    }

    /// One drain step (ported `gather_readnext`). Non-blocking: returns
    /// [`DrainStep::Idle`] instead of waiting, so the caller controls parking
    /// (and can interleave `dest.receive_slot` / interrupt checks).
    pub fn next(&mut self) -> DrainStep<T> {
        let mut nvisited = 0usize;
        loop {
            if self.active.is_empty() {
                return DrainStep::Eof;
            }
            debug_assert!(self.next < self.active.len());
            let idx = self.active[self.next];
            let ring = &self.funnel.rings[idx];

            if let Some(v) = ring.try_pop() {
                // Fairness stride: rotate after `fair_stride` consecutive rows
                // so no producer's ring is drained exclusively. C never rotates
                // on a successful read (fair_stride == 0).
                if self.fair_stride > 0 && self.active.len() > 1 {
                    self.stride_rem -= 1;
                    if self.stride_rem <= 0 {
                        self.stride_rem = self.fair_stride;
                        self.advance();
                    }
                }
                return DrainStep::Row(v);
            }

            // Empty. If the producer is done, this ring is drained: remove it
            // (gather_readnext's `done` branch).
            if self.funnel.done[idx].load(Ordering::Acquire) && ring.is_empty() {
                // Re-check emptiness AFTER observing done: done is set AFTER the
                // last push (Release) and our pop uses Acquire on tail, so if
                // done is visible and the ring reads empty, no more rows exist.
                self.active.remove(self.next);
                if self.active.is_empty() {
                    return DrainStep::Eof;
                }
                if self.next >= self.active.len() {
                    self.next = 0;
                }
                continue;
            }

            // Would block on this ring: advance round-robin.
            self.advance();
            nvisited += 1;
            if nvisited >= self.active.len() {
                // Full sweep found nothing and no ring was removable: all live
                // rings are empty. Tell the caller to park.
                return DrainStep::Idle;
            }
        }
    }

    fn advance(&mut self) {
        self.next += 1;
        if self.next >= self.active.len() {
            self.next = 0;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize as StdAtomicUsize, Ordering as StdOrd};
    use std::sync::Arc as StdArc;

    #[test]
    fn ring_fifo_and_bounds() {
        let r: SpscRing<u32> = SpscRing::new(4);
        assert_eq!(r.capacity(), 4);
        for i in 0..4 {
            assert!(r.try_push(i).is_ok());
        }
        assert!(r.is_full());
        assert_eq!(r.try_push(99), Err(99)); // full
        for i in 0..4 {
            assert_eq!(r.try_pop(), Some(i)); // FIFO
        }
        assert!(r.is_empty());
        assert_eq!(r.try_pop(), None);
    }

    #[test]
    fn ring_capacity_rounds_pow2() {
        let r: SpscRing<u8> = SpscRing::new(5);
        assert_eq!(r.capacity(), 8);
        let r2: SpscRing<u8> = SpscRing::new(1);
        assert_eq!(r2.capacity(), 2);
    }

    #[test]
    fn ring_drop_releases_payloads() {
        // Owned payloads left in the ring must be dropped.
        let counter = StdArc::new(StdAtomicUsize::new(0));
        struct Guard(StdArc<StdAtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, StdOrd::SeqCst);
            }
        }
        {
            let r: SpscRing<Guard> = SpscRing::new(4);
            r.try_push(Guard(StdArc::clone(&counter))).ok().unwrap();
            r.try_push(Guard(StdArc::clone(&counter))).ok().unwrap();
            // pop one, leave one for Drop
            drop(r.try_pop());
        }
        assert_eq!(counter.load(StdOrd::SeqCst), 2);
    }

    #[test]
    fn drain_round_robin_and_eof() {
        let f: Arc<RowFunnel<u32>> = RowFunnel::new(2, 8);
        let p0 = f.producer(0);
        let p1 = f.producer(1);
        p0.try_push(10).ok().unwrap();
        p1.try_push(20).ok().unwrap();
        p0.try_push(11).ok().unwrap();

        let mut d = f.drain();
        let mut got = Vec::new();
        p0.mark_done();
        p1.mark_done();
        loop {
            match d.next() {
                DrainStep::Row(v) => got.push(v),
                DrainStep::Eof => break,
                DrainStep::Idle => unreachable!("all producers done, no idle"),
            }
        }
        got.sort();
        assert_eq!(got, vec![10, 11, 20]);
    }

    #[test]
    fn drain_idle_when_empty_but_live() {
        let f: Arc<RowFunnel<u32>> = RowFunnel::new(2, 4);
        let mut d = f.drain();
        // No rows, no done → Idle (not Eof).
        assert!(matches!(d.next(), DrainStep::Idle));
    }

    #[test]
    fn fair_stride_rotates() {
        let f: Arc<RowFunnel<u32>> = RowFunnel::new(2, 16);
        let p0 = f.producer(0);
        let p1 = f.producer(1);
        for i in 0..4 {
            p0.try_push(i).ok().unwrap();
        }
        for i in 100..104 {
            p1.try_push(i).ok().unwrap();
        }
        let mut d = f.drain().with_fair_stride(1);
        let a = d.next();
        let b = d.next();
        match (a, b) {
            (DrainStep::Row(x), DrainStep::Row(y)) => {
                assert!((x < 100) != (y < 100), "stride should alternate rings: {x},{y}");
            }
            _ => panic!("expected two rows"),
        }
    }

    #[test]
    fn close_demand_stops_blocking_producer() {
        // Fill a ring, then a blocking push must return DemandClosed once the
        // drain closes demand — no deadlock.
        let f: Arc<RowFunnel<u32>> = RowFunnel::new(1, 2);
        let p = f.producer(0);
        assert!(p.try_push(1).is_ok());
        assert!(p.try_push(2).is_ok());
        assert!(!f.demand_closed());

        let fc = Arc::clone(&f);
        let producer_thread = std::thread::spawn(move || {
            let p = fc.producer(0);
            p.push_blocking(3, || {})
        });

        std::thread::yield_now();
        f.close_demand();
        let outcome = producer_thread.join().unwrap();
        assert_eq!(outcome, PushOutcome::DemandClosed);
    }

    #[test]
    fn concurrent_produce_drain() {
        // One producer thread, one drain thread, bounded ring: back-pressure
        // via push_blocking must not lose or deadlock.
        const N: u32 = 100_000;
        let f: Arc<RowFunnel<u32>> = RowFunnel::new(1, 64);
        let fc = Arc::clone(&f);
        let prod = std::thread::spawn(move || {
            let p = fc.producer(0);
            for i in 0..N {
                assert_eq!(p.push_blocking(i, || {}), PushOutcome::Pushed);
            }
            p.mark_done();
        });

        let mut d = f.drain();
        let mut got: u64 = 0;
        let mut sum: u64 = 0;
        loop {
            match d.next() {
                DrainStep::Row(v) => {
                    sum += v as u64;
                    got += 1;
                }
                DrainStep::Idle => {
                    let seen = d.park_epoch();
                    // re-check before parking (lost-wakeup-free)
                    match d.next() {
                        DrainStep::Row(v) => {
                            sum += v as u64;
                            got += 1;
                            continue;
                        }
                        DrainStep::Eof => break,
                        DrainStep::Idle => d.park(seen),
                    }
                }
                DrainStep::Eof => break,
            }
        }
        prod.join().unwrap();
        // Drain any tail rows produced between the Idle check and Eof race.
        while let DrainStep::Row(v) = d.next() {
            sum += v as u64;
            got += 1;
        }
        assert_eq!(got, N as u64);
        assert_eq!(sum, (0..N as u64).sum::<u64>());
    }

    // ---- END-TO-END scheduler wiring (real WorkerPool) --------------------
    //
    // These exercise the FULL deferred scheduler path: submit a row-emit
    // taskset (RowEmitWork), pool worker threads run_morsel → produce into the
    // rings (parking on full under the blocking permit), the LEADER runs the
    // funnel drain CONCURRENTLY as a pure consumer (in place of parking on the
    // CompletionWaiter), finalize marks the rings done, EOF, RG completes.
    // This is the model of a parallel passthrough SELECT: each granule = one
    // scanned row emitted to the wire.

    use crate::{
        Runtime, RuntimeConfig, RgOutcome, QuerySpec, SizingParams, SyntheticMorselSource,
        TaskSetSpec, TaskSetWork, WorkerPool,
    };

    fn e2e_runtime() -> Arc<Runtime> {
        Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 2,
            slots: 8,
            sizing: SizingParams::default(),
            trace: false,
        })
    }

    /// Producer closure for a passthrough shape: emit granule index `g` as the
    /// row value, pushing under the blocking permit (K-standby donation), and
    /// stop promptly if demand is closed (LIMIT).
    fn passthrough_produce(_w: usize, range: MorselRange, p: &FunnelProducer<u64>) {
        for g in range {
            match p.push_blocking(g, crate::blocking_io_section) {
                PushOutcome::Pushed => {}
                PushOutcome::DemandClosed => return,
            }
        }
    }

    #[test]
    fn e2e_parallel_passthrough_all_rows_once() {
        let rt = e2e_runtime();
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let n: u64 = 40_000;
        // Small rings relative to N → real back-pressure / producer parking.
        let funnel: Arc<RowFunnel<u64>> = RowFunnel::new(rt.nthreads(), 128);
        let work = RowEmitWork::new(Arc::clone(&funnel), passthrough_produce);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 42,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(n)),
                work: work as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        // LEADER = pure drain, concurrent with the producing pool workers.
        let mut drain = funnel.drain();
        let mut got: Vec<u64> = Vec::with_capacity(n as usize);
        loop {
            let seen = drain.park_epoch();
            match drain.next() {
                DrainStep::Row(v) => got.push(v),
                DrainStep::Idle => drain.park(seen),
                DrainStep::Eof => break,
            }
        }

        assert_eq!(waiter.wait(), RgOutcome::Completed);
        pool.shutdown();

        // Row-correctness: every scanned row delivered EXACTLY once (arrival
        // order is arbitrary — unordered passthrough).
        assert_eq!(got.len(), n as usize, "row count");
        got.sort_unstable();
        assert!(got.iter().copied().eq(0..n), "each row 0..n exactly once");
    }

    #[test]
    fn e2e_parallel_passthrough_limit_no_hang() {
        // LIMIT: the leader stops after `limit` rows and closes demand; parked
        // producers wake, see the close, and stop cooperatively — the RG must
        // still complete (no deadlock/hang), the classic parallel-LIMIT path.
        let rt = e2e_runtime();
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let n: u64 = 200_000;
        let limit: usize = 1000;
        let funnel: Arc<RowFunnel<u64>> = RowFunnel::new(rt.nthreads(), 64);
        let work = RowEmitWork::new(Arc::clone(&funnel), passthrough_produce);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 43,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(n)),
                work: work as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let mut drain = funnel.drain();
        let mut got: usize = 0;
        loop {
            let seen = drain.park_epoch();
            match drain.next() {
                DrainStep::Row(_) => {
                    got += 1;
                    if got >= limit {
                        funnel.close_demand();
                        break;
                    }
                }
                DrainStep::Idle => drain.park(seen),
                DrainStep::Eof => break,
            }
        }

        assert_eq!(got, limit, "delivered exactly LIMIT rows");
        // No hang: producers stop cooperatively and the RG completes.
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        pool.shutdown();
    }
}
