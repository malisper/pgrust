//! The pure bloom-filter core (spec §8.3; charter §5): build-side insert
//! and probe-side membership share one bit-selection function over the
//! crate hash family ([`crate::hash`]) — one definition, both sides.
//! Extracted verbatim from `pgrc2_meta::bloom` (ruling 2026-08-08; see the
//! crate provenance header). The arming DECISION (`bloom_armed`, typed on
//! the format's `Sortedness`) and the §8.3 section-body locator stay in
//! `pgrc2_meta` — this module is the format-independent core.
//!
//! Policy constants are meta-builder policy, NOT frozen format vocabulary:
//! the section header is self-describing (`k`, `bytes_per_granule`), so
//! probe never assumes the policy.

use crate::hash::meta_hash128;

/// Hash functions per key (Kirsch–Mitzenmacher double hashing).
pub const BLOOM_K_DEFAULT: u32 = 4;
/// Filter bytes per armed granule (16,384 bits ≈ 2 bits/row at full
/// granules; fp ≈ 0.2% at 1k distinct, degrading gracefully — sound at any
/// load because blooms only ever prove ABSENCE).
pub const BLOOM_BYTES_PER_GRANULE_DEFAULT: u32 = 2048;
/// The NDV floor of the arming policy.
///
/// v4 (OD-11, RULED 2026-08-12): RAISED from v3's 8 toward the v2-measured
/// 4096-NDV threshold. Floor-8 arming reached most unclustered eq-bloomable
/// columns (0.5–1GB-class of bloom bytes inside CMP-F's ~2.1GB meta plane at
/// 100m) with ZERO engagement witness; below the measured threshold, zone
/// min/max + PSMA carry equality probes and the bloom spends bytes to say
/// "probably present". The M3 kill-switch A/B (q19 clustered needle + QA
/// uncorrelated needle) is FT-10's decline trigger if both read flat.
pub const BLOOM_NDV_FLOOR: u32 = 4096;

#[inline]
fn bit_positions_hashed(h1: u64, h2: u64, k: u32, nbits: u64) -> impl Iterator<Item = u64> {
    (0..k as u64).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % nbits)
}

#[inline]
fn bit_positions(canonical: &[u8], k: u32, nbits: u64) -> impl Iterator<Item = u64> {
    let (h1, h2) = meta_hash128(canonical);
    bit_positions_hashed(h1, h2, k, nbits)
}

/// Insert from a pre-computed hash pair (the builder hashes each value
/// once and feeds bloom + NDV from the same pair).
pub fn bloom_insert_hashed(block: &mut [u8], k: u32, h1: u64, h2: u64) {
    let nbits = (block.len() as u64) * 8;
    debug_assert!(nbits > 0);
    for bit in bit_positions_hashed(h1, h2, k, nbits) {
        block[(bit / 8) as usize] |= 1 << (bit % 8);
    }
}

/// Insert one canonical value image (spec §18.1 bytes) into a filter block.
pub fn bloom_insert(block: &mut [u8], k: u32, canonical: &[u8]) {
    let (h1, h2) = meta_hash128(canonical);
    bloom_insert_hashed(block, k, h1, h2);
}

/// Membership probe: `false` is definitive absence (the AllFail
/// evidence); `true` proves nothing. A malformed (empty) block answers
/// `true` — consult-nothing is always sound.
pub fn bloom_may_contain(block: &[u8], k: u32, canonical: &[u8]) -> bool {
    let nbits = (block.len() as u64) * 8;
    if nbits == 0 {
        return true;
    }
    for bit in bit_positions(canonical, k, nbits) {
        if block[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Policy-constant pins carried from `pgrc2_meta`'s pin suite (changing
    /// one is a bank-recipe version event, never a silent edit).
    #[test]
    fn bloom_policy_constant_pins() {
        assert_eq!(BLOOM_K_DEFAULT, 4);
        assert_eq!(BLOOM_BYTES_PER_GRANULE_DEFAULT, 2048);
        assert_eq!(BLOOM_NDV_FLOOR, 4096);
    }

    /// Insert/probe share one bit selection: everything inserted is found;
    /// an empty block answers `true` (consult-nothing is sound).
    #[test]
    fn bloom_never_false_negative_smoke() {
        let mut block = vec![0u8; BLOOM_BYTES_PER_GRANULE_DEFAULT as usize];
        for i in 0..64u64 {
            bloom_insert(&mut block, BLOOM_K_DEFAULT, &i.to_le_bytes());
        }
        for i in 0..64u64 {
            assert!(bloom_may_contain(&block, BLOOM_K_DEFAULT, &i.to_le_bytes()));
        }
        assert!(bloom_may_contain(&[], BLOOM_K_DEFAULT, b"anything"));
    }
}
