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
use std::sync::Arc;

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
    /// WFIN decomposition measured dict-memo-shape@10M DOP15 busy inflation of +78%,
    /// tracking dict_builds 153→243 almost exactly — every row group SPLIT
    /// across workers duplicates its dictionary decompress + dict-eval
    /// memo sweep; the armed lane-pool arm claims whole RGs and scales
    /// 13x). The finalization protocol is claim-size-agnostic;
    /// photo-finish granularity becomes one epoch (~8 granules, ~1.2ms on
    /// dict-memo-heavy kernels — within the <=1-task spread acceptance). Sources
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

/// AM-private granule geometry, fixed at engagement — the DEFINITION point
/// of the whole-boundary dict-epoch law (rules 1+2 above): a columnar hard
/// boundary is a row-group edge, which is exactly a dictionary-epoch edge;
/// heap block ranges have no interior boundaries. Immutable; Arc-shared by
/// the scheduler-facing source ([`GranuleMapSource`]), the engagement
/// payload, and every worker's claim segmentation — ONE copy of the
/// boundary prefix sums.
pub struct GranuleMap {
    total: u64,
    /// Hard-boundary prefix sums: first == 0, last == total, expected
    /// strictly increasing (duplicate entries — zero-granule row groups —
    /// are tolerated defensively and answer [`GranuleMap::boundary_after`]
    /// exactly as the pre-extraction sources did). Empty ⇒ no interior
    /// boundaries (heap).
    starts: Arc<Vec<u64>>,
    /// Startup-ramp seed (per-AM data, not policy: columnar granules are
    /// 8,192 rows each — seed 2; heap granules are single blocks — seed 16).
    c0: u64,
}

impl GranuleMap {
    /// Columnar geometry over `Part::granule_starts`-shaped prefix sums
    /// (len = nrgs+1, first 0, last = total granules).
    pub fn with_boundaries(starts: Arc<Vec<u64>>, c0: u64) -> GranuleMap {
        debug_assert!(starts.first() == Some(&0), "boundary prefix sums start at 0");
        debug_assert!(
            starts.windows(2).all(|w| w[0] <= w[1]),
            "boundary prefix sums are non-decreasing"
        );
        let total = starts.last().copied().unwrap_or(0);
        GranuleMap { total, starts, c0 }
    }

    /// Boundary-free geometry (heap: granule = one block, no dictionary
    /// epochs).
    pub fn unbounded(total: u64, c0: u64) -> GranuleMap {
        GranuleMap { total, starts: Arc::new(Vec::new()), c0 }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn c0(&self) -> u64 {
        self.c0
    }

    /// Interior hard-boundary count (columnar: nrgs; heap: 0) — the
    /// WFIN/LFIN diagnostic channel's `nrgs`.
    pub fn nbounds(&self) -> usize {
        self.starts.len().saturating_sub(1)
    }

    /// First hard boundary strictly after `start` (the [`MorselSource`]
    /// contract: `start < result <= total` for `start < total`). The
    /// guarded form of the boundary binary search — for boundary-free maps
    /// this is `total`, the trait default.
    #[inline]
    pub fn boundary_after(&self, start: u64) -> u64 {
        match self.starts.binary_search(&start) {
            Ok(i) => self.starts.get(i + 1).copied().unwrap_or(self.total),
            Err(i) => self.starts.get(i).copied().unwrap_or(self.total),
        }
    }

    /// Epoch-integral segmentation of one (possibly COALESCED) claim:
    /// yields consecutive subranges of `claim`, each within one boundary
    /// span, so a positioned range never crosses a dictionary epoch.
    /// Boundary-free maps yield `claim` once.
    #[inline]
    pub fn segments(&self, claim: MorselRange) -> Segments<'_> {
        Segments { starts: &self.starts, next: claim.start, end: claim.end }
    }
}

/// Iterator over the epoch-integral segments of one claim (see
/// [`GranuleMap::segments`]). [`Segments::whole`] is the boundary-free
/// degenerate for consumers whose payload carries no map.
pub struct Segments<'a> {
    /// Hard-boundary prefix sums (empty = boundary-free).
    starts: &'a [u64],
    next: u64,
    end: u64,
}

impl Segments<'static> {
    /// Boundary-free segmentation: yields `claim` once.
    #[inline]
    pub fn whole(claim: MorselRange) -> Segments<'static> {
        Segments { starts: &[], next: claim.start, end: claim.end }
    }
}

impl Segments<'_> {
    /// Unyielded work remains. A consumer's between-segment abort check
    /// fires only while this is true — an abort observed between segments
    /// stops the claim; a fully consumed claim never re-checks.
    #[inline]
    pub fn more(&self) -> bool {
        self.next < self.end
    }
}

impl Iterator for Segments<'_> {
    type Item = MorselRange;

    #[inline]
    fn next(&mut self) -> Option<MorselRange> {
        if self.next >= self.end {
            return None;
        }
        // The guarded form of the boundary search; falling back to `end`
        // (instead of total-then-min) is identical for claims within
        // [0, total], which boundary-clamped claims are by construction.
        let seg_end = match self.starts.binary_search(&self.next) {
            Ok(i) => self.starts.get(i + 1).copied(),
            Err(i) => self.starts.get(i).copied(),
        }
        .unwrap_or(self.end)
        .min(self.end);
        let seg = self.next..seg_end;
        self.next = seg_end;
        Some(seg)
    }
}

/// THE scheduler-facing [`MorselSource`] over a [`GranuleMap`]. Claim
/// posture is EXPLICIT per consuming arm — never inferred from the AM or
/// the geometry: the arms genuinely differ (the scan arm claims whole
/// boundaries per its env kill switch AND coalesces because its work body
/// subdivides multi-epoch claims; sink drains feed claims straight to the
/// AM positioning — whole-boundary, never coalesced; the hash-agg arm's
/// claims are sizer-truncated within a row group). Inferring posture would
/// silently change claim shapes.
pub struct GranuleMapSource {
    map: Arc<GranuleMap>,
    whole_boundary: bool,
    coalesce: bool,
}

impl GranuleMapSource {
    /// Posture flags per the consuming arm (see
    /// [`MorselSource::whole_boundary_claims`] and
    /// [`MorselSource::coalesce_claims`] for what each entails — notably a
    /// boundary-free map must not set `whole_boundary`: one claim would
    /// take the whole pipeline).
    pub fn new(map: Arc<GranuleMap>, whole_boundary: bool, coalesce: bool) -> GranuleMapSource {
        GranuleMapSource { map, whole_boundary, coalesce }
    }
}

impl MorselSource for GranuleMapSource {
    fn total_granules(&self) -> u64 {
        self.map.total()
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        self.map.boundary_after(start)
    }

    fn startup_c0(&self) -> u64 {
        self.map.c0()
    }

    fn whole_boundary_claims(&self) -> bool {
        self.whole_boundary
    }

    fn coalesce_claims(&self) -> bool {
        self.coalesce
    }
}
