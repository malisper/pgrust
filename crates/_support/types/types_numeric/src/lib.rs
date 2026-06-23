//! Carrier vocabulary for the PostgreSQL `numeric` type
//! (`postgres-18.3/src/backend/utils/adt/numeric.c`).
//!
//! This crate is the KEYSTONE of the `backend-utils-adt-numeric` decomposition:
//! the shared types / on-disk ABI / lifetime foundation that every numeric
//! family compiles against.
//!
//! Two layers live here:
//!
//! * the **on-disk storage ABI** (`NumericData`/`NumericChoice`/`NumericLong`/
//!   `NumericShort`, the `NUMERIC_*` flag constants, `DEC_DIGITS`/`NBASE`, the
//!   typmod pack/unpack helpers, and the safe byte-view accessors over a varlena
//!   `&[u8]`). This mirrors numeric.c ~lines 58-260 + numeric.h. It is alloc-
//!   free and `no_std`-friendly; the existing `jsonb_util` hash/compare path
//!   reads it.
//! * the **in-memory working types** ([`var`]): the arithmetic-time
//!   [`NumericVar`]`<'mcx>` (whose digit buffer is a *charged*
//!   `mcx::PgVec<'mcx, NumericDigit>` — the `'mcx` lifetime threaded through
//!   every family) plus the aggregate-transition states. These bear `PgVec`s
//!   and so depend on `mcx`.

#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

use core::mem::size_of;
use datum::VARHDRSZ;

pub mod var;

// ---------------------------------------------------------------------------
// Digit type and base (numeric.c:58-110).
// ---------------------------------------------------------------------------

/// A single base-NBASE digit (`int16` in the canonical build).
pub type NumericDigit = i16;

/// Base for the digit representation. Values other than 10000 are historical
/// only and unsupported.
pub const NBASE: i32 = 10000;
pub const HALF_NBASE: i32 = 5000;
/// Decimal digits per NBASE digit.
pub const DEC_DIGITS: i32 = 4;
/// Guard digits (measured in NBASE digits) for `mul_var`.
pub const MUL_GUARD_DIGITS: i32 = 2;
/// Guard digits (measured in NBASE digits) for `div_var`.
pub const DIV_GUARD_DIGITS: i32 = 4;
/// `NBASE * NBASE`; must fit in an `i32`.
pub const NBASE_SQR: i32 = NBASE * NBASE; // 100_000_000

// ---------------------------------------------------------------------------
// On-disk vocabulary types (numeric.c:136-159).
//
// The C `union NumericChoice { n_header, n_long, n_short }` becomes a Rust enum;
// the flexible digit arrays become owned `Vec<NumericDigit>`. These are the
// structured-codec carrier (read/written via the byte-view accessors + the
// owning crate's `struct_codec`); the on-disk byte image is the source of truth.
// ---------------------------------------------------------------------------

use alloc::vec::Vec;

/// `struct NumericShort`: a 2-byte header (sign + display scale + weight)
/// followed by the digit array.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NumericShort {
    /// Sign + display scale + weight.
    pub n_header: u16,
    /// Digit array (`NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]`).
    pub n_data: Vec<NumericDigit>,
}

/// `struct NumericLong`: a 2-byte sign/dscale word and a separate 2-byte weight,
/// followed by the digit array.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NumericLong {
    /// Sign + display scale.
    pub n_sign_dscale: u16,
    /// Weight of the first digit.
    pub n_weight: i16,
    /// Digit array (`NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]`).
    pub n_data: Vec<NumericDigit>,
}

/// `union NumericChoice`: the header-word / long form / short form. Which
/// variant is active is determined by the high bits of the first word (see the
/// `NUMERIC_*` flag constants).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NumericChoice {
    /// Raw header word (`n_header`).
    NHeader(u16),
    /// Long form, 4-byte header (`n_long`).
    NLong(NumericLong),
    /// Short form, 2-byte header (`n_short`).
    NShort(NumericShort),
}

/// `struct NumericData`: the `numeric` type as stored on disk — a varlena
/// length header followed by a `NumericChoice`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericData {
    /// Varlena length header. Do not touch directly.
    pub vl_len_: i32,
    /// Choice of storage format.
    pub choice: NumericChoice,
}

// ---------------------------------------------------------------------------
// Header bit-packing constants (numeric.c:163-200).
// ---------------------------------------------------------------------------

/// Mask selecting the two high (sign/format) bits.
pub const NUMERIC_SIGN_MASK: u16 = 0xC000;
pub const NUMERIC_POS: u16 = 0x0000;
pub const NUMERIC_NEG: u16 = 0x4000;
pub const NUMERIC_SHORT: u16 = 0x8000;
pub const NUMERIC_SPECIAL: u16 = 0xC000;

/// `VARHDRSZ` (4) + `sizeof(uint16)` (2) + `sizeof(int16)` (2).
pub const NUMERIC_HDRSZ: usize = 8;
/// `VARHDRSZ` (4) + `sizeof(uint16)` (2).
pub const NUMERIC_HDRSZ_SHORT: usize = 6;

// Special-value sign/format bits (NaN, +Inf, -Inf).
pub const NUMERIC_EXT_SIGN_MASK: u16 = 0xF000;
pub const NUMERIC_NAN: u16 = 0xC000;
pub const NUMERIC_PINF: u16 = 0xD000;
pub const NUMERIC_NINF: u16 = 0xF000;
pub const NUMERIC_INF_SIGN_MASK: u16 = 0x2000;

// Short-format field definitions.
pub const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
pub const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
pub const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
pub const NUMERIC_SHORT_DSCALE_MAX: u16 = NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT;
pub const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
pub const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
pub const NUMERIC_SHORT_WEIGHT_MAX: i32 = NUMERIC_SHORT_WEIGHT_MASK as i32;
pub const NUMERIC_SHORT_WEIGHT_MIN: i32 = -(NUMERIC_SHORT_WEIGHT_MASK as i32 + 1);

// Long-format field definitions.
pub const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
pub const NUMERIC_DSCALE_MAX: u16 = NUMERIC_DSCALE_MASK;

/// Maximum stored weight (`int16` weight in `NumericLong`).
pub const NUMERIC_WEIGHT_MAX: i32 = i16::MAX as i32;

// Typmod/precision/scale limits (numeric.h).
pub const NUMERIC_MAX_PRECISION: i32 = 1000;
pub const NUMERIC_MIN_SCALE: i32 = -1000;
pub const NUMERIC_MAX_SCALE: i32 = 1000;
pub const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;
pub const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
pub const NUMERIC_MAX_RESULT_SCALE: i32 = NUMERIC_MAX_PRECISION * 2;
pub const NUMERIC_MIN_SIG_DIGITS: i32 = 16;

// Sort-support abbreviation constants (numeric.c:404-415). On a 64-bit Datum
// the abbreviation is a 64-bit signed integer; special values use the int64
// extremes (the abbreviation is negated relative to the value, so NaN sorts
// last).
pub const NUMERIC_ABBREV_BITS: i32 = 64;
pub const NUMERIC_ABBREV_NAN: i64 = i64::MIN;
pub const NUMERIC_ABBREV_PINF: i64 = -i64::MAX;
pub const NUMERIC_ABBREV_NINF: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// Safe byte-view accessors over the varlena payload (`&[u8]`).
//
// The slice is the entire on-disk `numeric` value, starting at the varlena
// header. The first header word (byte offset VARHDRSZ) determines the format.
// These mirror the `NUMERIC_*` macros from numeric.c but read the header word
// directly from bytes, so no raw pointers are needed.
// ---------------------------------------------------------------------------

/// `VARHDRSZ_SHORT` (varatt.h): a short (1-byte) varlena header.
pub const VARHDRSZ_SHORT: usize = 1;

/// `VARATT_IS_1B(PTR)` (varatt.h): true when the byte image carries a 1-byte
/// ("short") varlena header. On little-endian the tag lives in the low bit
/// (`0x01`); on big-endian it lives in the high bit (`0x80`).
#[inline]
pub fn varatt_is_1b(num: &[u8]) -> bool {
    if cfg!(target_endian = "big") {
        (num[0] & 0x80) == 0x80
    } else {
        (num[0] & 0x01) == 0x01
    }
}

/// `VARDATA_ANY` offset: the byte offset of the numeric struct (`NumericChoice`)
/// within the on-disk byte image, which is 1 for a short varlena header and
/// `VARHDRSZ` (4) for a long one. A numeric reaching these accessors is always
/// inline (detoasted), never compressed/external.
#[inline]
fn vardata_off(num: &[u8]) -> usize {
    if varatt_is_1b(num) {
        VARHDRSZ_SHORT
    } else {
        VARHDRSZ
    }
}

/// `VARSIZE_ANY(PTR)` (varatt.h): total on-disk byte length of the value,
/// reading either the 1-byte short or the 4-byte long varlena length word.
#[inline]
pub fn varsize_any(num: &[u8]) -> usize {
    if varatt_is_1b(num) {
        // VARSIZE_1B: (header >> 1) & 0x7F (little-endian) / header & 0x7F (big).
        if cfg!(target_endian = "big") {
            (num[0] & 0x7F) as usize
        } else {
            ((num[0] >> 1) & 0x7F) as usize
        }
    } else {
        // VARSIZE_4B: (header >> 2) & 0x3FFFFFFF (little-endian) /
        // header & 0x3FFFFFFF (big).
        let hdr = u32::from_ne_bytes([num[0], num[1], num[2], num[3]]);
        if cfg!(target_endian = "big") {
            (hdr & 0x3FFF_FFFF) as usize
        } else {
            ((hdr >> 2) & 0x3FFF_FFFF) as usize
        }
    }
}

/// Read the 16-bit header word (`choice.n_header`) from a numeric byte slice,
/// indexing from the header-agnostic `VARDATA_ANY` offset (short or long
/// varlena header).
#[inline]
pub fn header_word(num: &[u8]) -> u16 {
    let off = vardata_off(num);
    debug_assert!(num.len() >= off + 2);
    u16::from_ne_bytes([num[off], num[off + 1]])
}

/// Read the long-form weight word (`choice.n_long.n_weight`), indexing from the
/// header-agnostic `VARDATA_ANY` offset.
#[inline]
pub fn long_weight_word(num: &[u8]) -> i16 {
    let off = vardata_off(num);
    debug_assert!(num.len() >= off + 4);
    i16::from_ne_bytes([num[off + 2], num[off + 3]])
}

/// `NUMERIC_FLAGBITS`: the two high sign/format bits.
#[inline]
pub fn numeric_flagbits(num: &[u8]) -> u16 {
    header_word(num) & NUMERIC_SIGN_MASK
}

/// `NUMERIC_IS_SHORT`.
#[inline]
pub fn numeric_is_short(num: &[u8]) -> bool {
    numeric_flagbits(num) == NUMERIC_SHORT
}

/// `NUMERIC_IS_SPECIAL`.
#[inline]
pub fn numeric_is_special(num: &[u8]) -> bool {
    numeric_flagbits(num) == NUMERIC_SPECIAL
}

/// `NUMERIC_HEADER_IS_SHORT`: true when the high bit is set (short OR special).
#[inline]
pub fn numeric_header_is_short(num: &[u8]) -> bool {
    (header_word(num) & 0x8000) != 0
}

/// `NUMERIC_EXT_FLAGBITS`.
#[inline]
pub fn numeric_ext_flagbits(num: &[u8]) -> u16 {
    header_word(num) & NUMERIC_EXT_SIGN_MASK
}

/// `NUMERIC_IS_NAN`.
#[inline]
pub fn numeric_is_nan(num: &[u8]) -> bool {
    header_word(num) == NUMERIC_NAN
}

/// `NUMERIC_IS_PINF`.
#[inline]
pub fn numeric_is_pinf(num: &[u8]) -> bool {
    header_word(num) == NUMERIC_PINF
}

/// `NUMERIC_IS_NINF`.
#[inline]
pub fn numeric_is_ninf(num: &[u8]) -> bool {
    header_word(num) == NUMERIC_NINF
}

/// `NUMERIC_IS_INF`: positive or negative infinity.
#[inline]
pub fn numeric_is_inf(num: &[u8]) -> bool {
    (header_word(num) & !NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF
}

/// `NUMERIC_SIGN`: one of `NUMERIC_POS`/`NEG`/`NAN`/`PINF`/`NINF`.
#[inline]
pub fn numeric_sign(num: &[u8]) -> u16 {
    if numeric_is_short(num) {
        if (header_word(num) & NUMERIC_SHORT_SIGN_MASK) != 0 {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        }
    } else if numeric_is_special(num) {
        numeric_ext_flagbits(num)
    } else {
        numeric_flagbits(num)
    }
}

/// `NUMERIC_DSCALE`: display scale.
#[inline]
pub fn numeric_dscale(num: &[u8]) -> u16 {
    if numeric_header_is_short(num) {
        (header_word(num) & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT
    } else {
        header_word(num) & NUMERIC_DSCALE_MASK
    }
}

/// `NUMERIC_WEIGHT`: weight of the first digit.
///
/// For the short format the 7-bit weight field is SIGNED: bit 0x0040 is the
/// sign bit and is sign-extended (matching the C macro which ORs in
/// `~NUMERIC_SHORT_WEIGHT_MASK` when the sign bit is set).
#[inline]
pub fn numeric_weight(num: &[u8]) -> i32 {
    if numeric_header_is_short(num) {
        let h = header_word(num);
        let sign_ext: i32 = if (h & NUMERIC_SHORT_WEIGHT_SIGN_MASK) != 0 {
            !(NUMERIC_SHORT_WEIGHT_MASK as i32)
        } else {
            0
        };
        sign_ext | ((h & NUMERIC_SHORT_WEIGHT_MASK) as i32)
    } else {
        long_weight_word(num) as i32
    }
}

/// `NUMERIC_HEADER_SIZE`: header byte count for this value's format.
///
/// This is the count of header bytes *before the digit array*: the varlena
/// header (1 for a short varlena, 4 for a long one — `VARDATA_ANY` relative)
/// plus the 2-byte `n_header`, plus the 2-byte `n_weight` for the LONG numeric
/// form. Mirrors C's `NUMERIC_HEADER_SIZE`, whose `VARHDRSZ` term is implicit in
/// `VARDATA_ANY(n)` (i.e. it counts from the start of the on-disk image).
#[inline]
pub fn numeric_header_size(num: &[u8]) -> usize {
    vardata_off(num)
        + size_of::<u16>()
        + if numeric_header_is_short(num) {
            0
        } else {
            size_of::<i16>()
        }
}

/// `NUMERIC_NDIGITS`: number of base-NBASE digits stored.
///
/// `varsize` is the total on-disk byte length of the value (its `VARSIZE`).
#[inline]
pub fn numeric_ndigits(num: &[u8], varsize: usize) -> usize {
    (varsize - numeric_header_size(num)) / size_of::<NumericDigit>()
}

/// Digit slice accessor: the raw bytes of the base-NBASE digit array, i.e. the
/// payload following the header. Native-endian `NumericDigit` pairs.
#[inline]
pub fn numeric_digits(num: &[u8]) -> &[u8] {
    let hdr = numeric_header_size(num);
    &num[hdr..]
}

/// Decode a single digit from the digit byte slice at digit index `i`.
#[inline]
pub fn numeric_digit_at(digits: &[u8], i: usize) -> NumericDigit {
    let off = i * size_of::<NumericDigit>();
    NumericDigit::from_ne_bytes([digits[off], digits[off + 1]])
}

// ---------------------------------------------------------------------------
// Typmod pack/unpack helpers (numeric.c make_numeric_typmod et al.).
// ---------------------------------------------------------------------------

/// `make_numeric_typmod`: pack precision (upper 16 bits) and scale (lower 11
/// bits) into a typmod, offset by `VARHDRSZ`.
#[inline]
pub fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ as i32
}

/// `is_valid_numeric_typmod`: valid typmods are at least `VARHDRSZ`.
#[inline]
pub fn is_valid_numeric_typmod(typmod: i32) -> bool {
    typmod >= VARHDRSZ as i32
}

/// `numeric_typmod_precision`: extract precision from a typmod.
#[inline]
pub fn numeric_typmod_precision(typmod: i32) -> i32 {
    ((typmod - VARHDRSZ as i32) >> 16) & 0xffff
}

/// `numeric_typmod_scale`: extract scale from a typmod. The scale may be
/// negative; sign-extend the 11-bit two's-complement field via `(x^1024)-1024`.
#[inline]
pub fn numeric_typmod_scale(typmod: i32) -> i32 {
    (((typmod - VARHDRSZ as i32) & 0x7ff) ^ 1024) - 1024
}

// ---------------------------------------------------------------------------
// Fixed-size aggregate-transition states (numeric.c). These are alloc-free, so
// they live in this `no_std` ABI module; the Vec-bearing states are in `var`.
// ---------------------------------------------------------------------------

/// `Int128AggState` (numeric.c:5586-5592) -- 128-bit transition state used by
/// the `numeric_poly_*` / `int*_accum` fast paths (PolyNumAggState on 128-bit
/// platforms).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Int128AggState {
    pub calc_sum_x2: bool,
    pub n: i64,
    pub sum_x: i128,
    pub sum_x2: i128,
}

/// `Int8TransTypeData` -- the 2-element int8 array transition value
/// (count, sum) used by avg(int2)/avg(int4) and moving sum(int2)/sum(int4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Int8TransTypeData {
    pub count: i64,
    pub sum: i64,
}

/// `NumericSortSupport` (numeric.c:340-347) -- the `ssup_extra` payload for the
/// numeric abbreviated-key sort, minus the HyperLogLog estimator/scratch buffer
/// (those live behind the sort-support seams). Carries only the in-crate
/// computation fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NumericSortSupport {
    /// Number of non-null values seen.
    pub input_count: i64,
    /// True while cardinality is still being estimated.
    pub estimating: bool,
}
