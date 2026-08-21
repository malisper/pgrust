//! Condition-cache analogue (charter addition 2026-08-15): per-granule
//! predicate verdicts recorded on the first execution and reused on hot
//! re-executions — the v1 `pgrcolumnar/src/condcache.rs` mechanism
//! (per-window staged-qual survivor bitmaps over immutable part state,
//! keyed (PartIdent, qual fingerprint, window)) reduced to this harness's
//! shape: the bank is immutable and the predicate identity is the variant
//! itself, so the key degenerates to the granule ordinal.
//!
//! INVALIDATION ASSUMPTION (stated per charter): immutable bank ⇒ none
//! needed here. A live engine needs the write-side hook (v1 gets it for
//! free from part immutability: publishes change file identity, so stale
//! entries become unreachable). That asymmetry is why this is a VARIANT,
//! never a freebie blended into the honest-recompute numbers.
//!
//! P1-1 note: the engine-owned store lives on `engine::Faces` keyed by
//! structured typed fingerprints (ir::Fingerprint — attno, type oid,
//! collation oid, op, canonical datum bytes) so collation-illegal verdict
//! sharing is unrepresentable; write-side invalidation is P5-5, but the
//! key already isolates relation identity via the per-relation Faces
//! handle. Only GVerdict ports from this file (the rest was harness
//! accounting).

/// One granule's cached verdict for a fixed predicate.
#[derive(Clone, Debug)]
pub enum GVerdict {
    /// Zone/predicate erased the granule (no matching rows).
    Skip,
    /// Every row matches.
    AllPass,
    /// Exactly these in-granule row ordinals match (the survivor list —
    /// v1 stores bit words; a row LIST is byte-cheaper at low selectivity,
    /// bitmap cost noted alongside).
    Rows(Vec<u16>),
}
