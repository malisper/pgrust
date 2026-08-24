//! Geometry (spec §2; M0-S1 + M0-S4 frozen — re-cut only by a new banked
//! spike verdict; reopen conditions live in the spike docs, never here).
//!
//! Nesting is exact: band = 8 granules; granule = a whole number of 1024-value
//! frames = a whole number of execution batches; every count below is
//! closed-form from the part row total plus the part's ONE granule grain, so
//! root streams store no per-granule row-count arrays. Child streams carry
//! their own value counts (spec §6.5).
//!
//! ## SB-10 byte-bounded granules (lanev4; OD-13 RULED 2026-08-12)
//!
//! v4 makes granule geometry row-AND-byte-bounded WITHOUT giving up
//! closed-form addressing: the writer elects ONE uniform granule grain per
//! part from the power-of-two ladder [`GRAIN_LADDER`] = {8192, 4096, 2048,
//! 1024} — the largest grain at which EVERY column's per-(column,granule)
//! value bytes stay under [`GRANULE_BYTE_BOUND_PROVISIONAL`]. The grain is
//! recorded in the part footer ([`crate::part::FooterFixed::granule_rows`]);
//! all addressing stays closed-form in (rows, grain) via the `*_at`
//! functions below. [`GRANULE_ROWS`] remains the DEFAULT and MAXIMUM grain —
//! every capacity bound (scratch buffers, DV bitmaps, the 13-bit RowId row
//! field) keys off it unchanged; a smaller grain only ever shrinks granules.
//! The band stays exactly [`GRANULES_PER_BAND`] granules OF THE ELECTED
//! GRAIN, so band row counts scale with the grain.
//!
//! The band is the seal/visibility/**claim** quantum: whole-band claims are
//! the M2 §9 law (+78% CPU when an epoch splits); dict epochs are part-scoped
//! (spec §7), so no band claim can split one by construction.

use crate::{FormatError, FormatResult};

/// Execution batch (executor constant, M0-S1; format divisibility-neutral).
pub const BATCH_ROWS: u32 = 1024;
/// Encoding frame — the addressing + SIMD unit inside a granule (M0-S4).
pub const FRAME_VALUES: u32 = 1024;
/// Decode + pruning grain — the DEFAULT and MAXIMUM granule grain (SB-10:
/// smaller ladder grains only shrink granules; every capacity bound in the
/// cone stays keyed to this constant).
pub const GRANULE_ROWS: u32 = 8192;
/// Seal/visibility/claim quantum AT THE DEFAULT GRAIN (capacity bound; the
/// per-part band row count is `grain × GRANULES_PER_BAND`).
pub const BAND_ROWS: u32 = 65_536;
/// Frames per full granule.
pub const FRAMES_PER_GRANULE: u32 = GRANULE_ROWS / FRAME_VALUES;
/// Granules per full band.
pub const GRANULES_PER_BAND: u32 = BAND_ROWS / GRANULE_ROWS;
/// Dictionary lazy-fault grain (spec §7).
pub const DICT_FRAME_ENTRIES: u32 = 1024;
/// Varlena images at or above this go to the overflow stream (spec §6.8).
pub const OVERSIZE_THRESHOLD: u32 = 32 * 1024;
/// A stream extent's section must stay below 4 GiB (u32 intra-section
/// offsets, spec §2).
pub const EXTENT_MAX_LEN: u64 = u32::MAX as u64;

// Nesting laws, compile-time (the freeze itself).
const _: () = {
    assert!(GRANULE_ROWS % BATCH_ROWS == 0);
    assert!(GRANULE_ROWS % FRAME_VALUES == 0);
    assert!(BAND_ROWS % GRANULE_ROWS == 0);
    assert!(FRAMES_PER_GRANULE == 8);
    assert!(GRANULES_PER_BAND == 8);
    assert!(FRAME_VALUES == BATCH_ROWS);
    // Bit-packed frame addressing is byte-exact at any width (spec §6.11):
    // FRAME_VALUES × width ≡ 0 (mod 8) for all widths because 1024 % 8 == 0.
    assert!(FRAME_VALUES % 8 == 0);
};

// ---------------------------------------------------------------------------
// SB-10 granule-grain vocabulary (OD-13 RULED: "let's byte bound granules")
// ---------------------------------------------------------------------------

/// The legal per-part granule grains, LARGEST FIRST (the writer's election
/// order — the largest grain satisfying the byte bound wins). Every entry is
/// a power of two, a multiple of [`FRAME_VALUES`], and at most
/// [`GRANULE_ROWS`]; the ladder floor is one frame's worth of rows.
pub const GRAIN_LADDER: [u32; 4] = [8192, 4096, 2048, 1024];

/// PROVISIONAL byte bound per (column, granule) — 10 MiB, the ClickHouse
/// `index_granularity_bytes` analog default.
///
/// LOUD PROVENANCE NOTE: this CONSTANT is provisional. The QA-1 wide-row
/// census (a named M3 cell, ledger row SB-10) sizes the final bound; only
/// the MECHANISM (uniform per-part ladder grain, closed-form addressing,
/// footer-recorded grain) is SIGNED. Re-cutting the constant is a one-line
/// A-lane change with no layout consequence — the grain is recorded per
/// part, never derived from the bound at read time.
pub const GRANULE_BYTE_BOUND_PROVISIONAL: u64 = 10 * 1024 * 1024;

// Ladder legality, compile-time.
const _: () = {
    let mut i = 0;
    while i < GRAIN_LADDER.len() {
        let g = GRAIN_LADDER[i];
        assert!(g.is_power_of_two());
        assert!(g % FRAME_VALUES == 0);
        assert!(g <= GRANULE_ROWS);
        assert!(g >= FRAME_VALUES);
        // Largest-first, strictly descending.
        if i > 0 {
            assert!(GRAIN_LADDER[i - 1] > g);
        }
        i += 1;
    }
    assert!(GRAIN_LADDER[0] == GRANULE_ROWS);
};

/// One part's elected granule grain (SB-10). Constructible only from a
/// ladder value, so every carrier of a `GranuleGrain` holds a legal grain by
/// construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GranuleGrain {
    rows: u32,
}

impl GranuleGrain {
    /// The default (and maximum) grain — v3 geometry exactly.
    pub const DEFAULT: GranuleGrain = GranuleGrain { rows: GRANULE_ROWS };

    /// Validate a wire/footer grain value against the ladder.
    pub fn from_rows(rows: u32) -> FormatResult<GranuleGrain> {
        let mut i = 0;
        while i < GRAIN_LADDER.len() {
            if GRAIN_LADDER[i] == rows {
                return Ok(GranuleGrain { rows });
            }
            i += 1;
        }
        Err(FormatError::Corrupt {
            at: "granule grain not on the ladder",
        })
    }

    /// Rows per full granule at this grain.
    pub const fn rows(self) -> u32 {
        self.rows
    }

    /// Rows per full band at this grain (band = 8 granules, always).
    pub const fn band_rows(self) -> u32 {
        self.rows * GRANULES_PER_BAND
    }

    /// Frames per full granule at this grain.
    pub const fn frames_per_granule(self) -> u32 {
        self.rows / FRAME_VALUES
    }

    pub const fn is_default(self) -> bool {
        self.rows == GRANULE_ROWS
    }
}

/// Granules in a part of `rows` rows (last may be short) — DEFAULT grain.
///
/// The true closed form is a `u64` (see [`granule_count_at`]); this
/// convenience wrapper keeps its historical `u32` contract for the
/// write/test callers that build legitimate, well-bounded parts. The
/// hostile-input echo validation in `part.rs`/`manifest.rs` calls the
/// `_at` forms directly against the untruncated `u64`, so narrowing here
/// never widens the trust boundary.
pub fn granule_count(rows: u64) -> u32 {
    granule_count_at(rows, GranuleGrain::DEFAULT) as u32
}

/// Bands in a part of `rows` rows (last may be short) — DEFAULT grain.
///
/// See [`granule_count`] for why this convenience wrapper narrows to
/// `u32` while the echo validation uses the untruncated [`band_count_at`].
pub fn band_count(rows: u64) -> u32 {
    band_count_at(rows, GranuleGrain::DEFAULT) as u32
}

/// Rows in granule `g` of a `rows`-row part (0 for out-of-range granules) —
/// DEFAULT grain.
pub fn rows_in_granule(rows: u64, g: u32) -> u32 {
    rows_in_granule_at(rows, GranuleGrain::DEFAULT, g)
}

/// Rows in band `band` of a `rows`-row part (0 for out-of-range bands) —
/// DEFAULT grain.
pub fn rows_in_band(rows: u64, band: u32) -> u32 {
    rows_in_band_at(rows, GranuleGrain::DEFAULT, band)
}

/// Granules in a part of `rows` rows at `grain` (last may be short).
///
/// Returns the TRUE `u64` closed form: truncating to `u32` here would let a
/// hostile footer/manifest store the mod-2^32 residue of an inconsistent
/// `(rows, granule_count)` pair and still pass the decode echo checks
/// (idx 253). Callers that persist the count into a `u32` field narrow at
/// the write side, after bounding by `MAX_GRANULES_PER_PART`.
pub fn granule_count_at(rows: u64, grain: GranuleGrain) -> u64 {
    rows.div_ceil(grain.rows() as u64)
}

/// Bands in a part of `rows` rows at `grain` (last may be short).
///
/// Returns the TRUE `u64` closed form for the same reason as
/// [`granule_count_at`]: band_count is not covered by the
/// `MAX_GRANULES_PER_PART` reject, so echo validation must compare against
/// the untruncated value.
pub fn band_count_at(rows: u64, grain: GranuleGrain) -> u64 {
    rows.div_ceil(grain.band_rows() as u64)
}

/// Rows in granule `g` of a `rows`-row part at `grain` (0 out of range).
pub fn rows_in_granule_at(rows: u64, grain: GranuleGrain, g: u32) -> u32 {
    let start = (g as u64) * grain.rows() as u64;
    if start >= rows {
        return 0;
    }
    (rows - start).min(grain.rows() as u64) as u32
}

/// Rows in band `band` of a `rows`-row part at `grain` (0 out of range).
pub fn rows_in_band_at(rows: u64, grain: GranuleGrain, band: u32) -> u32 {
    let start = (band as u64) * grain.band_rows() as u64;
    if start >= rows {
        return 0;
    }
    (rows - start).min(grain.band_rows() as u64) as u32
}

/// Frames needed for `gvalues` values within one granule (capacity-bounded
/// by the DEFAULT grain — legal for every ladder grain).
pub fn frames_in_granule(gvalues: u32) -> u32 {
    debug_assert!(gvalues <= GRANULE_ROWS);
    gvalues.div_ceil(FRAME_VALUES)
}

/// Frames needed for `gvalues` values within one granule at `grain`.
pub fn frames_in_granule_at(gvalues: u32, grain: GranuleGrain) -> u32 {
    debug_assert!(gvalues <= grain.rows());
    gvalues.div_ceil(FRAME_VALUES)
}

/// Values in frame `f` of a granule holding `gvalues` values.
pub fn values_in_frame(gvalues: u32, f: u32) -> u32 {
    let start = f * FRAME_VALUES;
    if start >= gvalues {
        return 0;
    }
    (gvalues - start).min(FRAME_VALUES)
}

/// The band a granule belongs to.
pub fn band_of_granule(g: u32) -> u32 {
    g / GRANULES_PER_BAND
}
