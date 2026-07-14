//! Morsel source vocabulary: granule-addressed pipeline input.
//!
//! Sizing is in GRANULES (the source's smallest indivisible claim unit —
//! for cbstore in M1 a granule is the 8,192-row unit of format.rs; heap
//! sources will present block-range units). Two hard rules the runtime
//! enforces on every claim (lit-review §5.2):
//!   1. never split a granule — claims are whole-granule ranges;
//!   2. never cross a hard boundary (cbstore row-group or dictionary-epoch
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
    /// are large (cbstore: 8,192 rows each) may override downward in M1.
    fn startup_c0(&self) -> u64 {
        16
    }

    /// Whole-boundary claims: every claim runs from its start to the NEXT
    /// hard boundary — the duration-adaptive sizer never stops a claim
    /// short of an epoch edge. Sources whose per-EPOCH state dominates the
    /// per-granule work opt in (cbstore: the runtime-drive-scaling lane's
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
