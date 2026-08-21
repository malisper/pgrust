//! Metadata-plane record layouts (spec §8; charter §5 — a format GUARANTEE,
//! not an arm). M3-E builds and probes these; the bytes are frozen here.
//!
//! Laws carried in the layout:
//! - **Coarse-key law**: `Exact` keys may prove Eq AllPass; `Coarse` keys
//!   prove range verdicts only. `EnumRankReserved` is the O-5 reserved
//!   zone-key slot (v1 enums are eq-only and emit `Absent` — O-M3-5).
//! - **Two-witness null law**: `nonnull` here must equal the validity-bitmap
//!   popcount (spec §6.6); every consumer of one witness cross-checks the
//!   other.
//! - **Advisory-only pruning**: verdicts can only lose speed, never rows —
//!   the exhaustive verdict-vs-decode differential enforces it (M3-E/K).
//! - Text stats carry BOTH byte- and char-length units (the #80 lesson).
//! - Row counts are closed-form (spec §2), never stored.

use crate::wire::{put_i128, put_i64, put_u16, put_u32, put_u64, put_u8, Cur};
use crate::{FormatError, FormatResult};

/// Zone-key kinds (spec §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KeyKind {
    /// No order-embedded key (non-C text order, jsonb-as-value, composites,
    /// extension types, v1 enums): validity + counts only.
    Absent = 0,
    /// The i64 key is exact — Eq AllPass provable.
    Exact = 1,
    /// Prefix/saturating embed — range verdicts only.
    Coarse = 2,
    /// RESERVED (O-5): the enumsortorder-derived key slot. Nothing writes it
    /// at M3; either O-5 transcription lands without a format break.
    EnumRankReserved = 3,
}

impl KeyKind {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> FormatResult<KeyKind> {
        match v {
            0 => Ok(KeyKind::Absent),
            1 => Ok(KeyKind::Exact),
            2 => Ok(KeyKind::Coarse),
            3 => Ok(KeyKind::EnumRankReserved),
            _ => Err(FormatError::Corrupt { at: "KeyKind" }),
        }
    }
}

/// Sortedness facts (spec §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Sortedness {
    Unknown = 0,
    Ascending = 1,
    Descending = 2,
    Constant = 3,
}

impl Sortedness {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> FormatResult<Sortedness> {
        match v {
            0 => Ok(Sortedness::Unknown),
            1 => Ok(Sortedness::Ascending),
            2 => Ok(Sortedness::Descending),
            3 => Ok(Sortedness::Constant),
            _ => Err(FormatError::Corrupt { at: "Sortedness" }),
        }
    }
}

/// Ternary pruning verdict (charter §1; advisory-only by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    AllPass,
    AllFail,
    Mixed,
}

pub const STATS_RECORD_LEN: usize = 80;

/// `StatsRecord.flags` bit 0 — the COMPUTED-STATS WITNESS (#598). Set by the
/// REAL meta builder (`pgrc2_meta`'s `ColumnMeta`) on every record it seals,
/// at every grain; never set by the stand-in. The witness is the on-part
/// fact that separates a computed `sum_i128 == 0` from an uncomputed zero:
/// stand-in-vintage parts carry EXACT `nonnull` with zeroed aggregates and
/// stay admissible forever (TypeSemantics is deliberately outside
/// `schema_fingerprint`; blessed v1 bank keys are frozen), while the probe
/// side derives the column's real profile from the catalog — so without
/// this bit the profile says "SUM is computed" about a record whose builder
/// never computed it. The measured-only answer face
/// (`pgrc2_meta::verdict::{sum_answer, zero_count_answer}`) refuses
/// profile-computed aggregates when the witness is absent (decline to the
/// decode path; declining is always sound).
///
/// What needs NO witness, deliberately: `nonnull` (exact under EVERY
/// builder vintage — it is one leg of the two-witness null law §6.6) and
/// row counts (closed-form, never stored — spec §2). That asymmetry keeps
/// CountStar/CountNonNull answerable over pre-witness parts while
/// SUM/ZeroCount decline — the COUNT metadata-answer slice ships first.
///
/// Format note: `flags` was written 0 by every writer and read by nobody
/// before this bit, so every pre-existing part uniformly reads "witness
/// absent" — the conservative arm — with no format break and no bank
/// re-bless.
pub const STATSF_COMPUTED: u16 = 1 << 0;

/// `StatsRecord.flags` bit 1 — the ALL-ASCII WITNESS (v4 delta; format-ledger
/// row FT-2 amendment P-2, SIGNED at the M2 close). Set by the real meta
/// builder iff EVERY observed non-null value of a BytesAndChars (text-class)
/// column within the record's span is pure ASCII — in which case char units
/// coincide with byte units and `byte_len_sum` doubles as an exact
/// `char_len_sum`.
///
/// The P-2 LICENSING RULE (the #80 lesson applied to the SUM field):
/// `length(text)` counts CHARS while the stored fold fact is `byte_len_sum`
/// — a char-unit fold (SUM(length(col)) class) is licensed ONLY under this
/// witness; absent the witness the consumer demotes to scan (the AD-1
/// demote pattern — declining is always sound). q27/q28 worked on v3 only
/// because URLs are ASCII (a bank-shaped accident); this bit makes the
/// accident a checked fact. Merge law: a coarser grain carries the bit iff
/// every contributing finer record carries it (empty granules are vacuously
/// ASCII). Same no-format-break argument as STATSF_COMPUTED: v3 writers
/// wrote `flags` bit 1 as 0, which reads "witness absent" — conservative.
pub const STATSF_ALL_ASCII: u16 = 1 << 1;

/// The typed footer-aggregate + zone record (80 B, spec §8.1) — the SAME
/// layout at granule, band, and part grain. Wire order == declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StatsRecord {
    pub min_key: i64,
    pub max_key: i64,
    /// Typed SUM in the mergeable form (i128; ints + numeric at shared scale).
    pub sum_i128: i128,
    pub zero_count: u64,
    pub byte_len_sum: u64,
    pub nonnull: u32,
    pub byte_len_min: u32,
    pub byte_len_max: u32,
    pub char_len_min: u32,
    pub char_len_max: u32,
    /// Point estimate; mergeable registers live in the NdvRegisters section.
    pub ndv_est: u32,
    pub key_kind: u8,
    pub sortedness: u8,
    pub flags: u16,
    pub pad: u32,
}

impl StatsRecord {
    /// The all-absent record (a fresh column with no observations).
    pub fn absent() -> StatsRecord {
        StatsRecord {
            min_key: 0,
            max_key: 0,
            sum_i128: 0,
            zero_count: 0,
            byte_len_sum: 0,
            nonnull: 0,
            byte_len_min: 0,
            byte_len_max: 0,
            char_len_min: 0,
            char_len_max: 0,
            ndv_est: 0,
            key_kind: KeyKind::Absent.as_u8(),
            sortedness: Sortedness::Unknown.as_u8(),
            flags: 0,
            pad: 0,
        }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        put_i64(out, self.min_key);
        put_i64(out, self.max_key);
        put_i128(out, self.sum_i128);
        put_u64(out, self.zero_count);
        put_u64(out, self.byte_len_sum);
        put_u32(out, self.nonnull);
        put_u32(out, self.byte_len_min);
        put_u32(out, self.byte_len_max);
        put_u32(out, self.char_len_min);
        put_u32(out, self.char_len_max);
        put_u32(out, self.ndv_est);
        put_u8(out, self.key_kind);
        put_u8(out, self.sortedness);
        put_u16(out, self.flags);
        put_u32(out, self.pad);
        debug_assert_eq!(out.len() - start, STATS_RECORD_LEN);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<StatsRecord> {
        let r = StatsRecord {
            min_key: c.i64("StatsRecord")?,
            max_key: c.i64("StatsRecord")?,
            sum_i128: c.i128("StatsRecord")?,
            zero_count: c.u64("StatsRecord")?,
            byte_len_sum: c.u64("StatsRecord")?,
            nonnull: c.u32("StatsRecord")?,
            byte_len_min: c.u32("StatsRecord")?,
            byte_len_max: c.u32("StatsRecord")?,
            char_len_min: c.u32("StatsRecord")?,
            char_len_max: c.u32("StatsRecord")?,
            ndv_est: c.u32("StatsRecord")?,
            key_kind: c.u8("StatsRecord")?,
            sortedness: c.u8("StatsRecord")?,
            flags: c.u16("StatsRecord")?,
            pad: c.u32("StatsRecord")?,
        };
        KeyKind::from_u8(r.key_kind)?;
        Sortedness::from_u8(r.sortedness)?;
        Ok(r)
    }
}

/// The Stats section body for one column (spec §8.1): granule records, then
/// band records, then ONE part record — counts closed-form from part rows.
pub fn stats_section_len(granule_count: u32, band_count: u32) -> usize {
    (granule_count as usize + band_count as usize + 1) * STATS_RECORD_LEN
}

// ---------------------------------------------------------------------------
// FlatStats (pgrc2.1 §2.1 — flat SoA stats arrays; spec §8.5, NEW)
// ---------------------------------------------------------------------------
//
// FORMAT SPEC (§8.5, pgrc2.1): the `SectionKind::FlatStats` section is the
// SoA transposition of the SAME per-granule facts the §8.1 Stats section
// carries — per (column, stat-kind) fixed-width arrays indexed by granule
// ordinal, plus ONE part-grain rollup entry — so a consult is an
// mmap-and-cast array walk instead of a per-record parse. It is OPTIONAL
// (SectionEntry carries `SECTION_OPTIONAL`), always RAW (never
// `SECTIONF_META_ZSTD`: zstd-wrapping would defeat the cast — the mmap-cast
// law), duplicates §8.1 by construction (both are encoded from the one
// builder record stream in the same seal walk — zero extra fold cost), and
// readers FALL BACK to §8.1 record parsing when it is absent (pre-2.1
// parts). Band grain is deliberately not carried: the flat plane serves the
// granule walk and the part rollup; band consumers stay on §8.1.
//
// Layout (all integers LE; `n = granule_count + 1`, entry `g` for granule
// ordinal `g`, entry `n-1` = the part rollup):
//
//   FlatStatsHdr (16 B):
//     version    u16   = 1
//     flags      u16   FLATSTATSF_* (the MEET over every §8.1 record:
//                      a bit is set iff EVERY contributing record set it)
//     n          u32   granule_count + 1
//     key_kind   u8    the MEET over records: the shared KeyKind when every
//                      record agrees, else `KeyKind::Absent` (key consumers
//                      then fall back to §8.1 per-record kinds)
//     reserved   [u8;7] zero
//   min_key    [i64;  n]  at offset 16          (strings: the 8-byte
//   max_key    [i64;  n]  at offset 16 +  8n     order-preserving truncated
//                                                prefix key, KeyKind::Coarse
//                                                — range pruning only; exact
//                                                bounds via dict[min_code])
//   zero_count [u64;  n]  at offset 16 + 16n
//   sum_i128   [i128; n]  at offset 16 + 24n    (unaligned by construction:
//                                                readers use unaligned loads)
//   nonnull    [u32;  n]  at offset 16 + 40n
//
//   section len = 16 + 44n
//
// Alignment law: the writer 8-aligns every raw section start, and every
// array offset above is a multiple of 8, so the i64/u64 planes cast
// directly over an mmap; the i128 sums plane is only 8-aligned — consumers
// read it with unaligned 16-byte loads.

pub const FLATSTATS_VERSION: u16 = 1;
pub const FLATSTATS_HDR_LEN: usize = 16;
/// Bytes per entry across all five planes (8+8+8+16+4).
pub const FLATSTATS_ENTRY_BYTES: usize = 44;

/// Header flag: every contributing §8.1 record carried [`STATSF_COMPUTED`]
/// — the sums/zero_counts planes are computed facts (else advisory zeros).
pub const FLATSTATSF_COMPUTED: u16 = 1 << 0;
/// Header flag: every contributing record carried [`STATSF_ALL_ASCII`].
pub const FLATSTATSF_ALL_ASCII: u16 = 1 << 1;

/// Exact section length for `granule_count` granules (+1 part rollup).
pub fn flatstats_section_len(granule_count: u32) -> usize {
    FLATSTATS_HDR_LEN + FLATSTATS_ENTRY_BYTES * (granule_count as usize + 1)
}

/// Encode the FlatStats body from the SAME records the §8.1 Stats body is
/// encoded from: `granules` in ordinal order, then the ONE part record.
/// Pure transposition — the facts are byte-identical to §8.1's.
pub fn flatstats_encode(granules: &[StatsRecord], part: &StatsRecord) -> Vec<u8> {
    let n = granules.len() + 1;
    let all = || granules.iter().chain(core::iter::once(part));
    let meet_flag = |bit: u16| all().all(|r| r.flags & bit != 0);
    let mut flags = 0u16;
    if meet_flag(STATSF_COMPUTED) {
        flags |= FLATSTATSF_COMPUTED;
    }
    if meet_flag(STATSF_ALL_ASCII) {
        flags |= FLATSTATSF_ALL_ASCII;
    }
    let kk0 = part.key_kind;
    let key_kind = if all().all(|r| r.key_kind == kk0) {
        kk0
    } else {
        KeyKind::Absent.as_u8()
    };
    let mut out = Vec::with_capacity(FLATSTATS_HDR_LEN + FLATSTATS_ENTRY_BYTES * n);
    put_u16(&mut out, FLATSTATS_VERSION);
    put_u16(&mut out, flags);
    put_u32(&mut out, n as u32);
    put_u8(&mut out, key_kind);
    out.extend_from_slice(&[0u8; 7]);
    for r in all() {
        put_i64(&mut out, r.min_key);
    }
    for r in all() {
        put_i64(&mut out, r.max_key);
    }
    for r in all() {
        put_u64(&mut out, r.zero_count);
    }
    for r in all() {
        put_i128(&mut out, r.sum_i128);
    }
    for r in all() {
        put_u32(&mut out, r.nonnull);
    }
    debug_assert_eq!(out.len(), FLATSTATS_HDR_LEN + FLATSTATS_ENTRY_BYTES * n);
    out
}

/// Zero-copy view over a FlatStats section body (the mmap-cast face).
/// `new` validates version/length; the i64/u64 plane accessors cast when
/// the base is 8-aligned (the writer's raw-section law guarantees it on
/// conformant parts) and return `None` otherwise — consumers then fall
/// back to §8.1. Sums are always unaligned-load accessors.
pub struct FlatStatsRef<'a> {
    body: &'a [u8],
    /// granule_count + 1 (last entry = part rollup).
    pub n: usize,
    pub flags: u16,
    pub key_kind: u8,
}

impl<'a> FlatStatsRef<'a> {
    pub fn new(body: &'a [u8]) -> FormatResult<FlatStatsRef<'a>> {
        if body.len() < FLATSTATS_HDR_LEN {
            return Err(FormatError::Truncated { at: "FlatStats" });
        }
        let mut c = Cur::new(body);
        let version = c.u16("FlatStats")?;
        if version != FLATSTATS_VERSION {
            return Err(FormatError::BadVersion {
                at: "FlatStats",
                got: version as u32,
            });
        }
        let flags = c.u16("FlatStats")?;
        let n = c.u32("FlatStats")? as usize;
        let key_kind = c.u8("FlatStats")?;
        KeyKind::from_u8(key_kind)?;
        if body.len() != FLATSTATS_HDR_LEN + FLATSTATS_ENTRY_BYTES * n || n == 0 {
            return Err(FormatError::Corrupt { at: "FlatStats len" });
        }
        Ok(FlatStatsRef {
            body,
            n,
            flags,
            key_kind,
        })
    }

    fn plane(&self, ord: usize, width: usize) -> &'a [u8] {
        // Plane order/offsets are the spec table above: min(8) max(8)
        // zero(8) sum(16) nonnull(4).
        let off = FLATSTATS_HDR_LEN
            + match ord {
                0 => 0,
                1 => 8 * self.n,
                2 => 16 * self.n,
                3 => 24 * self.n,
                _ => 40 * self.n,
            };
        &self.body[off..off + width * self.n]
    }

    fn cast<T>(&self, ord: usize) -> Option<&'a [T]> {
        let b = self.plane(ord, core::mem::size_of::<T>());
        if b.as_ptr() as usize % core::mem::align_of::<T>() != 0 {
            return None;
        }
        // SAFETY: length exact, alignment checked, T is a plain LE integer
        // plane written by flatstats_encode (this crate only compiles on
        // LE targets — the wire law).
        Some(unsafe { core::slice::from_raw_parts(b.as_ptr() as *const T, self.n) })
    }

    /// `mins()[g]` / entry `n-1` = part rollup. `None` = misaligned base
    /// (non-conformant container): fall back to §8.1.
    pub fn mins(&self) -> Option<&'a [i64]> {
        self.cast::<i64>(0)
    }
    pub fn maxs(&self) -> Option<&'a [i64]> {
        self.cast::<i64>(1)
    }
    pub fn zero_counts(&self) -> Option<&'a [u64]> {
        self.cast::<u64>(2)
    }
    pub fn nonnulls(&self) -> Option<&'a [u32]> {
        self.cast::<u32>(4)
    }
    /// Sum plane entry (always an unaligned load — see the layout note).
    pub fn sum_at(&self, i: usize) -> i128 {
        let b = self.plane(3, 16);
        let mut w = [0u8; 16];
        w.copy_from_slice(&b[i * 16..i * 16 + 16]);
        i128::from_le_bytes(w)
    }
}

// ---------------------------------------------------------------------------
// PartDigest (pgrc2.1 §2.2 — exact NDV + zero_count per (column, part);
// spec §8.6, NEW)
// ---------------------------------------------------------------------------
//
// FORMAT SPEC (§8.6, pgrc2.1): `SectionKind::PartDigest` is one 24 B
// fixed-width record per (column, path_ord) carrying the two exact facts
// that are FREE at seal:
//
//  - **exact NDV**: the dictionary entry count when the part elected
//    DICT_CODES (the dict IS the distinct set); flag `PARTDIGESTF_NDV_EXACT`
//    marks it present. Non-dict parts carry 0 with the flag clear —
//    consumers fall back to the §8.1 part record's `ndv_est` (a point
//    estimate). Drives the L2 partition law and group-table presizing with
//    zero estimation risk on dict parts.
//  - **exact zero_count**: the §8.1 part record's zero tally, re-exposed
//    here under `PARTDIGESTF_ZERO_COMPUTED` (the STATSF_COMPUTED witness at
//    part grain) so the sparse-filter plane reads one tiny record instead
//    of parsing the record stream.
//
// OPTIONAL + raw (same forward-compat + mmap laws as FlatStats). Layout
// (LE): version u16 (=1), flags u16, reserved u32, ndv u64, zero_count u64.

pub const PARTDIGEST_VERSION: u16 = 1;
pub const PARTDIGEST_LEN: usize = 24;
/// `ndv` is the exact dict entry count (part elected DICT_CODES).
pub const PARTDIGESTF_NDV_EXACT: u16 = 1 << 0;
/// `zero_count` carries the computed-witness part-grain zero tally.
pub const PARTDIGESTF_ZERO_COMPUTED: u16 = 1 << 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartDigest {
    pub flags: u16,
    pub ndv: u64,
    pub zero_count: u64,
}

impl PartDigest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(PARTDIGEST_LEN);
        put_u16(&mut out, PARTDIGEST_VERSION);
        put_u16(&mut out, self.flags);
        put_u32(&mut out, 0);
        put_u64(&mut out, self.ndv);
        put_u64(&mut out, self.zero_count);
        debug_assert_eq!(out.len(), PARTDIGEST_LEN);
        out
    }

    pub fn decode(body: &[u8]) -> FormatResult<PartDigest> {
        if body.len() != PARTDIGEST_LEN {
            return Err(FormatError::Truncated { at: "PartDigest" });
        }
        let mut c = Cur::new(body);
        let version = c.u16("PartDigest")?;
        if version != PARTDIGEST_VERSION {
            return Err(FormatError::BadVersion {
                at: "PartDigest",
                got: version as u32,
            });
        }
        let flags = c.u16("PartDigest")?;
        let _reserved = c.u32("PartDigest")?;
        Ok(PartDigest {
            flags,
            ndv: c.u64("PartDigest")?,
            zero_count: c.u64("PartDigest")?,
        })
    }

    /// Exact NDV when carried, else `None` (consumer falls back to
    /// `ndv_est` from the §8.1 part record).
    pub fn ndv_exact(&self) -> Option<u64> {
        (self.flags & PARTDIGESTF_NDV_EXACT != 0).then_some(self.ndv)
    }
}

// ---------------------------------------------------------------------------
// PSMA (spec §8.2)
// ---------------------------------------------------------------------------

/// Positional-SMA table entries per granule block (leading byte → range).
pub const PSMA_ENTRIES: usize = 256;
pub const PSMA_ENTRY_LEN: usize = 4;
pub const PSMA_BLOCK_LEN: usize = PSMA_ENTRIES * PSMA_ENTRY_LEN;

/// One PSMA entry: candidate row range (row ordinals inside the granule,
/// `max_row` exclusive; 0..8192 fits u16 with 8192 representable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PsmaEntry {
    pub min_row: u16,
    pub max_row: u16,
}

// ---------------------------------------------------------------------------
// Bloom (spec §8.3) + NDV registers (spec §8.4)
// ---------------------------------------------------------------------------

pub const BLOOM_HDR_LEN: usize = 8;

/// Bloom section header (armed bitmap + per-armed-granule blocks follow).
/// The hash family is the vendored bloom module (M3-B); arming policy is
/// charter-carried (unclustered ∧ NDV floor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct BloomHdr {
    pub k: u32,
    pub bytes_per_granule: u32,
}

pub const NDV_REGISTERS_HDR_LEN: usize = 8;

/// NDV register blob header (dense HLL, the mergeable form — v8's lesson).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct NdvRegistersHdr {
    /// 1 = dense HLL.
    pub algo: u8,
    pub precision: u8,
    pub pad: u16,
    pub reg_len: u32,
}

// ---------------------------------------------------------------------------
// meta_probe vocabulary (spec §19.3)
// ---------------------------------------------------------------------------

/// Encoding-local stat probes (the `meta_probe` face). `Absent` answers mean
/// "consult the §8 metadata plane" — never an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaProbe {
    RowCount,
    NonNullCount,
    MinKey,
    MaxKey,
    SumI128,
    ZeroCount,
    Sortedness,
}

/// Typed probe answers (typed, never string-rendered — the ducklake
/// catalog-stats mistake).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaAnswer {
    Count(u64),
    Key(i64),
    Sum(i128),
    Sorted(Sortedness),
    /// The encoding cannot answer; consult the metadata plane.
    Absent,
}
