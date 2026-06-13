//! Seam declarations for the HyperLogLog cardinality estimator
//! (`lib/hyperloglog.c`, owned by the `backend-lib-*` unit): the agg spill
//! path estimates per-partition group cardinality with one HLL counter per
//! spill partition.
//!
//! The `hyperLogLogState *` crosses the seam as an opaque counter-handle word
//! (the entries of `HashAggSpill.hll_card`); the owning unit names the
//! concrete `hyperLogLogState` when it lands. These functions never ereport.
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `initHyperLogLog(cState, bwidth)` (hyperloglog.c): initialize a counter
    /// with `bwidth` register-index bits, returning its opaque handle word.
    pub fn init_hyper_log_log(bwidth: u8) -> usize
);

seam_core::seam!(
    /// `addHyperLogLog(cState, hash)` (hyperloglog.c): add a 32-bit hashed
    /// value to the counter.
    pub fn add_hyper_log_log(handle: usize, hash: u32)
);

seam_core::seam!(
    /// `estimateHyperLogLog(cState)` (hyperloglog.c): the current cardinality
    /// estimate.
    pub fn estimate_hyper_log_log(handle: usize) -> f64
);

seam_core::seam!(
    /// `freeHyperLogLog(cState)` (hyperloglog.c): release the counter.
    pub fn free_hyper_log_log(handle: usize)
);
