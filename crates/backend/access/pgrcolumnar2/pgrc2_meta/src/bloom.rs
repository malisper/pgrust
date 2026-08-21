//! Per-granule equality blooms (spec §8.3; charter §5). Build-side insert
//! and probe-side membership share one bit-selection function over the
//! crate hash family ([`crate::hash`]) — one definition, both sides.
//!
//! ## Arming policy (charter-carried: "unclustered ∧ NDV floor")
//!
//! A granule's bloom is KEPT (armed) iff:
//! - the profile says equality blooms are sound (`eq_bloomable` — the
//!   byte-eq == value-eq lattice, `profile.rs`), AND
//! - the granule is UNCLUSTERED (`Sortedness::Unknown`): a
//!   sorted/constant granule prunes by zone keys already, AND
//! - the granule's NDV estimate is at or above [`BLOOM_NDV_FLOOR`]: below
//!   it, min/max + PSMA carry equality probes and the bloom would spend
//!   bytes to say "probably present".
//!
//! Policy constants are meta-builder policy (this crate's), NOT frozen
//! format vocabulary: the section header is self-describing (`k`,
//! `bytes_per_granule`), so probe never assumes the policy.
//!
//! ## Section body (spec §8.3, frozen)
//!
//! `BloomHdr { k: u32, bytes_per_granule: u32 }` (8 B LE) +
//! `[armed bitmap: ceil(granule_count/8) B]` + one
//! `bytes_per_granule` filter block per ARMED granule, in granule order.

use crate::format::meta::Sortedness;

// The pure bloom core (constants, insert, probe — one bit-selection
// definition serving build and probe) lives in the shared `order_key`
// support crate, extracted verbatim from this file per the 2026-08-08
// ruling (the M3-B vendoring charter fulfilled). Re-exported so this
// crate's API and every call site are unchanged.
pub use order_key::bloom::{
    bloom_insert, bloom_insert_hashed, bloom_may_contain, BLOOM_BYTES_PER_GRANULE_DEFAULT,
    BLOOM_K_DEFAULT, BLOOM_NDV_FLOOR,
};

/// The arming decision for one sealed granule (see module doc; the
/// `eq_bloomable` leg is the caller's — the builder consults its profile).
pub fn bloom_armed(sortedness: Sortedness, ndv_est: u32) -> bool {
    sortedness == Sortedness::Unknown && ndv_est >= BLOOM_NDV_FLOOR
}

// ---------------------------------------------------------------------------
// section-body access (spec §8.3 wire form; probe side)
// ---------------------------------------------------------------------------

use crate::format::meta::BLOOM_HDR_LEN;
use crate::format::{FormatError, FormatResult};

/// Locate granule `g`'s filter block in a `Bloom` section body. Returns
/// `Ok(None)` when the granule is not armed; typed refusal on a malformed
/// body (the reader CRC-validates, this still bounds-checks — the
/// #66/#340 law). Also returns the header's `k`.
pub fn bloom_block_for(
    body: &[u8],
    granule_count: u32,
    g: u32,
) -> FormatResult<Option<(u32, &[u8])>> {
    if g >= granule_count {
        return Err(FormatError::Bounds {
            at: "bloom granule ordinal",
        });
    }
    if body.len() < BLOOM_HDR_LEN {
        return Err(FormatError::Truncated { at: "BloomHdr" });
    }
    let k = u32::from_le_bytes(body[0..4].try_into().expect("len 4"));
    let bpg = u32::from_le_bytes(body[4..8].try_into().expect("len 4")) as usize;
    if k == 0 || bpg == 0 {
        return Err(FormatError::Corrupt { at: "BloomHdr" });
    }
    let bitmap_len = (granule_count as usize).div_ceil(8);
    let bitmap_end = BLOOM_HDR_LEN + bitmap_len;
    if body.len() < bitmap_end {
        return Err(FormatError::Truncated {
            at: "bloom armed bitmap",
        });
    }
    let bitmap = &body[BLOOM_HDR_LEN..bitmap_end];
    if bitmap[(g / 8) as usize] & (1 << (g % 8)) == 0 {
        return Ok(None);
    }
    // Rank: armed granules before g.
    let mut rank = 0usize;
    for i in 0..g {
        if bitmap[(i / 8) as usize] & (1 << (i % 8)) != 0 {
            rank += 1;
        }
    }
    let start = bitmap_end + rank * bpg;
    let end = start + bpg;
    if body.len() < end {
        return Err(FormatError::Truncated { at: "bloom block" });
    }
    Ok(Some((k, &body[start..end])))
}
