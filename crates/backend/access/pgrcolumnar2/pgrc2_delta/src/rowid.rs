//! Delta-residency rowid vocabulary (SCAN-PLANE, not format).
//!
//! Sealed rows are addressed by the frozen spec §10 packing
//! (`pgrc2_format::rowid`): part_no (32) | granule (19) | row (13). Delta-
//! resident rows need an identity too (RowId staging under the identical-
//! StageSchema law, late materialization, EPQ capture at M5-H, DML refetch
//! at M5-M) — this module defines their DISJOINT space:
//!
//! ```text
//! bit  63       DELTA_ROWID_TAG (1 = delta-resident)
//! bits 62..16   heap block number of the delta-relation tuple
//! bits 15..0    heap offset number
//! ```
//!
//! The low 48 bits are exactly the lx heap TID packing
//! (`lx_source::rowid_from_tid`: `(block << 16) | offnum`), so a delta
//! rowid round-trips through the same helpers once the tag is stripped.
//!
//! **Disjointness is enforced, not hoped:** a sealed rowid with bit 63 set
//! is a part_no ≥ 2^31. Part numbers are publish-monotone from zero (spec
//! §13 `next_part_no`), so 2^31 parts is unreachable in any real lifetime —
//! but the pair-scan builder still REFUSES such a part typed
//! ([`check_part_no_for_pair`]) rather than assuming it. Tombstone
//! payloads always name SEALED rowids; writing or indexing a delta-tagged
//! tombstone is a typed error (crate doc law), enforced at both the write
//! face (`tombstone`) and the bitmap build (`bitmap`).
//!
//! Never surfaced as a ctid: the TID-surface posture (C12/O-8 typed
//! refusals) is unchanged by this vocabulary.

use crate::{DeltaError, DeltaResult};

/// Bit 63: set ⇔ the rowid addresses a delta-resident heap tuple.
pub const DELTA_ROWID_TAG: u64 = 1 << 63;

/// Largest part_no a pair scan admits: keeps sealed rowids' bit 63 clear
/// so the two spaces cannot alias. (2^31 − 1; parts are publish-monotone
/// from zero, so hitting this bound is corruption, not capacity.)
pub const MAX_PAIR_PART_NO: u32 = (1 << 31) - 1;

/// Pack a delta-relation TID into the tagged delta rowid space.
#[inline]
pub const fn pack_delta_rowid(block: u32, offnum: u16) -> u64 {
    DELTA_ROWID_TAG | ((block as u64) << 16) | offnum as u64
}

/// TRUE iff `rowid` is delta-tagged.
#[inline]
pub const fn is_delta_rowid(rowid: u64) -> bool {
    rowid & DELTA_ROWID_TAG != 0
}

/// Unpack a delta rowid to its (block, offnum); `None` when untagged OR
/// malformed. Bits 62..48 are structurally zero in every packed delta
/// rowid ([`pack_delta_rowid`] takes a u32 block); a nonzero value there
/// is corruption, and truncating it silently (`as u32` — the silent-
/// lossy-serialization class) would alias distinct corrupt rowids onto
/// valid tids. Malformed ⇒ `None`, never a wrapped answer.
#[inline]
pub const fn unpack_delta_rowid(rowid: u64) -> Option<(u32, u16)> {
    if !is_delta_rowid(rowid) {
        return None;
    }
    let untagged = rowid & !DELTA_ROWID_TAG;
    if untagged >> 48 != 0 {
        return None; // corrupt: block bits beyond u32
    }
    Some(((untagged >> 16) as u32, (untagged & 0xFFFF) as u16))
}

/// Pair-scan admission check for one sealed part (typed refusal beyond
/// [`MAX_PAIR_PART_NO`] — the disjointness guard, release-effective and
/// O(parts) at build).
#[inline]
pub fn check_part_no_for_pair(part_no: u32) -> DeltaResult<()> {
    if part_no > MAX_PAIR_PART_NO {
        return Err(DeltaError::PartNoBeyondPairBound { part_no });
    }
    Ok(())
}

// Delta rowids and sealed pair-admissible rowids cannot alias: an admitted
// part_no keeps bit 63 clear (part_no < 2^31 ⇒ top bit of the 32-bit part
// field is 0), and every delta rowid sets it.
const _: () = {
    assert!(pgrc2_format::rowid::ROWID_PART_BITS == 32);
    assert!((MAX_PAIR_PART_NO as u64) < (1u64 << 31));
};
