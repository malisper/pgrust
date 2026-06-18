//! Seams for the pseudo-random number generator (`src/common/pg_prng.c`).
//!
//! `pg_prng` is a low-level common crate; consumers that would form a
//! dependency cycle reach its backend-private global PRNG through this seam.

seam_core::seam!(
    /// `pg_prng_uint32(&pg_global_prng_state)` — draw a `uint32` from the
    /// backend-private global PRNG state. Infallible (no allocation, no
    /// ereport), so it returns the bare value.
    pub fn pg_global_prng_uint32() -> u32
);

seam_core::seam!(
    /// `pg_prng_uint64_range(&pg_global_prng_state, rmin, rmax)` — draw a
    /// `uint64` in the inclusive range `[rmin, rmax]` from the backend-private
    /// global PRNG state. Infallible (no allocation, no ereport).
    pub fn pg_global_prng_uint64_range(rmin: u64, rmax: u64) -> u64
);
