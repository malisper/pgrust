//! Packed columnar RowId (spec §10; ruling O-8 via the O-M3-5 confirmed
//! conservative core): the (part, granule, row) bit split, FROZEN HERE.
//!
//! `lx_vec::RowId::from_columnar` is the typed constructor hole this split
//! fills — the one-line delegation lands with M3-G's declared lx edits
//! (`lanev3-m1-chunks.md` §5 freeze table; this crate touches no lx code).
//!
//! Order-embedding: within a part, RowId order == row order; across parts,
//! part_no order (publish order, spec §13). The TID surface stays the
//! typed-refusal posture (exactness-sweep C12): a RowId is never surfaced as
//! a ctid.
//!
//! ## SB-10 grain interaction (lanev4 batch-ABI §5, AB-5.1/AB-5.2)
//!
//! The 32/19/13 split is FROZEN and grain-INDEPENDENT: 13 bits hold the
//! MAXIMUM grain (8192 rows) exactly, and SB-10's byte-bounding only ever
//! SHRINKS granules (every `geom::GRAIN_LADDER` grain ≤ 8192), so the row
//! field carries any legal grain's row ordinal with headroom. Producers pack
//! (granule, row) AT THE PART'S ELECTED GRAIN ([`pack_rowid_at`] asserts the
//! grain bound; [`pack_rowid`] keeps the capacity bound); extraction
//! ([`rowid_granule`]/[`rowid_row`] — `granule_of_rowid` in the ledger's
//! vocabulary) is pure bit arithmetic, defined for EVERY rowid regardless of
//! grain — totality is preserved by construction, and the (part, granule)
//! a rowid names exists because the producer packed within the part's
//! footer-recorded geometry. Order-embedding survives any uniform grain:
//! within a part, (granule, row) lexicographic order IS row order.

use crate::geom::{BAND_ROWS, GRANULE_ROWS};

/// Bit budget (spec §10): 32 (part) + 19 (granule) + 13 (row) = 64.
pub const ROWID_PART_BITS: u32 = 32;
pub const ROWID_GRANULE_BITS: u32 = 19;
pub const ROWID_ROW_BITS: u32 = 13;

/// Maximum granules a single part may address.
pub const MAX_GRANULES_PER_PART: u32 = 1 << ROWID_GRANULE_BITS;

// The 10B-row static asserts (the M3-A exit-slice leg, `lanev3-m3-chunks.md`
// §5): the split must address a 10-billion-row table even degenerately.
const _: () = {
    // The split spends the whole word, exactly.
    assert!(ROWID_PART_BITS + ROWID_GRANULE_BITS + ROWID_ROW_BITS == 64);
    // The row field addresses a granule exactly.
    assert!((1u64 << ROWID_ROW_BITS) == GRANULE_ROWS as u64);
    // 10B rows fit even if every part holds only ONE band (65,536 rows):
    // 2^32 parts × 65,536 rows ≥ 10^10.
    assert!((1u128 << ROWID_PART_BITS) * BAND_ROWS as u128 >= 10_000_000_000);
    // Max part capacity: 2^19 granules × 8,192 rows = 2^32 rows per part.
    assert!((1u128 << ROWID_GRANULE_BITS) * GRANULE_ROWS as u128 == 1u128 << 32);
    // DV-local rowids (granule, row) fit u32 exactly (spec §15).
    assert!(ROWID_GRANULE_BITS + ROWID_ROW_BITS == 32);
};

/// Pack (part_no, granule, row) — caller guarantees field ranges (writer/scan
/// internals; debug-asserted). `row` is bounded by the CAPACITY grain here;
/// producers that know the part's elected grain use [`pack_rowid_at`].
#[inline]
pub const fn pack_rowid(part_no: u32, granule: u32, row: u32) -> u64 {
    debug_assert!(granule < MAX_GRANULES_PER_PART);
    debug_assert!(row < GRANULE_ROWS);
    ((part_no as u64) << (ROWID_GRANULE_BITS + ROWID_ROW_BITS))
        | ((granule as u64) << ROWID_ROW_BITS)
        | row as u64
}

/// [`pack_rowid`] with the part's elected granule grain enforced (SB-10):
/// identical packing — the bit split never moves — but the row ordinal is
/// debug-asserted against THIS part's grain, not just the 13-bit capacity.
#[inline]
pub const fn pack_rowid_at(
    part_no: u32,
    granule: u32,
    row: u32,
    grain: crate::geom::GranuleGrain,
) -> u64 {
    debug_assert!(row < grain.rows());
    pack_rowid(part_no, granule, row)
}

#[inline]
pub const fn rowid_part(id: u64) -> u32 {
    (id >> (ROWID_GRANULE_BITS + ROWID_ROW_BITS)) as u32
}

#[inline]
pub const fn rowid_granule(id: u64) -> u32 {
    ((id >> ROWID_ROW_BITS) as u32) & (MAX_GRANULES_PER_PART - 1)
}

#[inline]
pub const fn rowid_row(id: u64) -> u32 {
    (id as u32) & (GRANULE_ROWS - 1)
}

/// DV-local rowid (spec §15): `(granule << 13) | row`, exactly u32.
#[inline]
pub const fn dv_local(granule: u32, row: u32) -> u32 {
    debug_assert!(granule < MAX_GRANULES_PER_PART);
    debug_assert!(row < GRANULE_ROWS);
    (granule << ROWID_ROW_BITS) | row
}

#[inline]
pub const fn dv_local_granule(local: u32) -> u32 {
    local >> ROWID_ROW_BITS
}

#[inline]
pub const fn dv_local_row(local: u32) -> u32 {
    local & (GRANULE_ROWS - 1)
}
