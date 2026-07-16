//! Morsel source vocabulary: granule-addressed pipeline input.
//!
//! Sizing is in GRANULES (the source's smallest indivisible claim unit —
//! for pgrcolumnar in M1 a granule is the 8,192-row unit of format.rs; heap
//! sources will present block-range units). Two hard rules the runtime
//! enforces on every claim (lit-review §5.2):
//!   1. never split a granule — claims are whole-granule ranges;
//!   2. never cross a hard boundary (pgrcolumnar row-group or dictionary-epoch
//!      edge) within one claim, so per-epoch memos (dict-eval, codehist,
//!      gmemo) stay worker-coherent and every kernel invocation sees a
//!      single dictionary snapshot.
//!
//! The scan side implements this trait in M1; M0 tests use
//! [`SyntheticMorselSource`].

use std::ops::Range;

pub trait MorselSource: Send + Sync {
    /// Total granules in the source (fixed for the pipeline's lifetime).
    fn total_granules(&self) -> u64;

    /// The first hard boundary strictly after granule `start` (row-group or
    /// dictionary-epoch edge). A claim starting at `start` must end at or
    /// before this index. Must satisfy `start < result <= total_granules()`
    /// for `start < total_granules()`. Default: no internal boundaries.
    fn next_boundary_after(&self, start: u64) -> u64 {
        let _ = start;
        self.total_granules()
    }

    /// Startup-ramp seed C0 in granules (Umbra: 16). Sources whose granules
    /// are large (pgrcolumnar: 8,192 rows each) may override downward in M1.
    fn startup_c0(&self) -> u64 {
        16
    }

    /// Whole-boundary claims: every claim runs from its start to the NEXT
    /// hard boundary — the duration-adaptive sizer never stops a claim
    /// short of an epoch edge. Sources whose per-EPOCH state dominates the
    /// per-granule work opt in (pgrcolumnar: the runtime-drive-scaling lane's
    /// WFIN decomposition measured q21@10M DOP15 busy inflation of +78%,
    /// tracking dict_builds 153→243 almost exactly — every row group SPLIT
    /// across workers duplicates its dictionary decompress + dict-eval
    /// memo sweep; the armed lane-pool arm claims whole RGs and scales
    /// 13x). The finalization protocol is claim-size-agnostic;
    /// photo-finish granularity becomes one epoch (~8 granules, ~1.2ms on
    /// q21-class kernels — within the <=1-task spread acceptance). Sources
    /// WITHOUT interior boundaries must not opt in (one claim would take
    /// the whole pipeline).
    fn whole_boundary_claims(&self) -> bool {
        false
    }

    /// Claim coalescing (dop1-tax fix 1), on top of whole-boundary claims:
    /// at LOW live width one claim may span SEVERAL boundaries (the factor
    /// decays to one epoch as workers join — sched.rs claim_morsel). Only
    /// sources whose WORK BODY subdivides a multi-boundary claim back into
    /// per-epoch segments may opt in (runtime_scan's morsel_body does; the
    /// sink drains feed a claim straight into `set_granule_range`, which
    /// refuses boundary-crossing ranges — they must not). Ignored unless
    /// `whole_boundary_claims()` is also true.
    fn coalesce_claims(&self) -> bool {
        false
    }

    /// STREAM-FED sources (parallel COPY's segmentator feed): granules
    /// become claimable as a producer publishes them, and the total is
    /// unknown until the stream closes. `Some((watermark, closed))` marks a
    /// stream source: workers may claim granules `< watermark`; a claim
    /// cursor at the watermark of an OPEN stream is STARVED (the worker
    /// parks; the producer's publish/close wakes it through the runtime
    /// park lot — [`crate::Runtime::notify_source_progress`]), and only a
    /// CLOSED stream's watermark is exhaustion. `None` (the default) keeps
    /// the fixed-total contract: `total_granules()` is final at publish.
    ///
    /// CONTRACT: once `closed` reads true the watermark must be final —
    /// implementations publish-then-close so any reader that observes the
    /// close also observes every published granule (read closed BEFORE the
    /// watermark, both SeqCst; see [`StreamSource`]).
    ///
    /// Stream sources must report `total_granules()` == the current
    /// watermark (sizing reads it through `stream_state` on the claim path,
    /// but startup asserts and stats may consult the total).
    fn stream_state(&self) -> Option<(u64, bool)> {
        None
    }
}

/// A producer-fed stream source: claimability advances with `publish`, the
/// total becomes final at `close`. Granule meaning belongs to the work body
/// (parallel COPY: one granule = one input chunk = one row group); every
/// granule is its own hard boundary so a claim is exactly one chunk.
pub struct StreamSource {
    published: crate::sync::atomic::AtomicU64,
    closed: crate::sync::atomic::AtomicBool,
    c0: u64,
}

impl StreamSource {
    pub fn new() -> StreamSource {
        StreamSource {
            published: crate::sync::atomic::AtomicU64::new(0),
            closed: crate::sync::atomic::AtomicBool::new(false),
            c0: 1,
        }
    }

    /// Advance the claimable watermark to `upto` granules (monotonic; a
    /// lower value is a no-op). The producer owes a
    /// [`crate::Runtime::notify_source_progress`] after publishing so
    /// starved workers re-check.
    pub fn publish(&self, upto: u64) {
        self.published.fetch_max(upto, crate::sync::atomic::Ordering::SeqCst);
    }

    /// Close the stream: the current watermark is final. Publish-then-close
    /// (never the reverse); the producer owes a wake after closing.
    pub fn close(&self) {
        self.closed.store(true, crate::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn watermark(&self) -> u64 {
        self.published.load(crate::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for StreamSource {
    fn default() -> Self {
        Self::new()
    }
}

impl MorselSource for StreamSource {
    fn total_granules(&self) -> u64 {
        self.published.load(crate::sync::atomic::Ordering::SeqCst)
    }

    /// One chunk per boundary: a claim never spans two chunks (each chunk
    /// is a full row group's parse+encode — per-epoch state per claim).
    fn next_boundary_after(&self, start: u64) -> u64 {
        start + 1
    }

    fn startup_c0(&self) -> u64 {
        self.c0
    }

    fn whole_boundary_claims(&self) -> bool {
        true
    }

    fn stream_state(&self) -> Option<(u64, bool)> {
        // closed BEFORE watermark: a reader that sees the close sees the
        // final watermark (publish happens-before close, both SeqCst).
        let closed = self.closed.load(crate::sync::atomic::Ordering::SeqCst);
        let watermark = self.published.load(crate::sync::atomic::Ordering::SeqCst);
        Some((watermark, closed))
    }
}

/// Deterministic in-memory source for M0 tests: `total` granules with a hard
/// boundary every `boundary_every` granules (0 = none).
pub struct SyntheticMorselSource {
    total: u64,
    boundary_every: u64,
    c0: u64,
}

impl SyntheticMorselSource {
    pub fn new(total: u64) -> Self {
        SyntheticMorselSource { total, boundary_every: 0, c0: 16 }
    }

    pub fn with_boundaries(total: u64, boundary_every: u64) -> Self {
        assert!(boundary_every > 0);
        SyntheticMorselSource { total, boundary_every, c0: 16 }
    }

    pub fn with_c0(mut self, c0: u64) -> Self {
        assert!(c0 > 0);
        self.c0 = c0;
        self
    }
}

impl MorselSource for SyntheticMorselSource {
    fn total_granules(&self) -> u64 {
        self.total
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        if self.boundary_every == 0 {
            return self.total;
        }
        (((start / self.boundary_every) + 1) * self.boundary_every).min(self.total)
    }

    fn startup_c0(&self) -> u64 {
        self.c0
    }
}

/// A claimed morsel: a whole-granule half-open range, boundary-clamped.
pub type MorselRange = Range<u64>;
