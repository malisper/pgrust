//! On-disk ABI for the PostgreSQL `numeric` type.
//!
//! These types are lifted byte-for-byte from PostgreSQL's `numeric.c` (the
//! `NumericData`/`NumericChoice`/`NumericLong`/`NumericShort` structs, see
//! `postgres-18.3/src/backend/utils/adt/numeric.c` ~lines 130-160) and the
//! associated `NUMERIC_*` macros.  The `#[repr(C)]` layout MUST match the C
//! storage format exactly: a `numeric` value is a varlena whose payload is a
//! `NumericChoice` union.  Layout is verified at compile time by the
//! const-assert gates at the bottom of this module.
//!
//! There is NO `extern "C"` here; the byte-view accessors operate over the
//! varlena payload as a `&[u8]` so they are safe to call from pure-Rust code.

#![allow(non_upper_case_globals)]

use core::mem::{align_of, offset_of, size_of};

// ---------------------------------------------------------------------------
// Digit type and base.
// ---------------------------------------------------------------------------

/// A single base-NBASE digit.  Signed and wide enough to hold a digit; the
/// canonical PostgreSQL build uses `int16`.
pub type NumericDigit = i16;

/// `Numeric` is an opaque pointer to on-disk `NumericData` (matches the C
/// `typedef struct NumericData *Numeric`).  Prefer the byte-view accessors;
/// this alias exists for ABI parity only.
pub type Numeric = *mut NumericData;

/// Base for the digit representation.  Values other than 10000 are historical
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
// On-disk ABI types (#[repr(C)], lifted from numeric.c).
// ---------------------------------------------------------------------------

/// Short form: a 2-byte header (sign + display scale + weight) followed by the
/// flexible digit array.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct NumericShort {
    /// Sign + display scale + weight.
    pub n_header: u16,
    /// Flexible array member (`NumericDigit n_data[]`).
    pub n_data: [NumericDigit; 0],
}

/// Long form: a 2-byte sign/dscale word and a separate 2-byte weight, followed
/// by the flexible digit array.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct NumericLong {
    /// Sign + display scale.
    pub n_sign_dscale: u16,
    /// Weight of the first digit.
    pub n_weight: i16,
    /// Flexible array member (`NumericDigit n_data[]`).
    pub n_data: [NumericDigit; 0],
}

/// Union over the header-word, long form, and short form.  Which variant is
/// active is determined by the high bits of the first word (see the
/// `NUMERIC_*` flag constants).
#[derive(Copy, Clone)]
#[repr(C)]
pub union NumericChoice {
    /// Raw header word.
    pub n_header: u16,
    /// Long form (4-byte header).
    pub n_long: NumericLong,
    /// Short form (2-byte header).
    pub n_short: NumericShort,
}

/// The `numeric` type as stored on disk: a varlena header followed by a
/// `NumericChoice`.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct NumericData {
    /// Varlena length header.  Do not touch directly.
    pub vl_len_: i32,
    /// Choice of storage format.
    pub choice: NumericChoice,
}

// ---------------------------------------------------------------------------
// Header bit-packing constants.
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

// Special-value definitions (NaN, +Inf, -Inf).
/// High bits plus NaN/Inf flag bits.
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

// Typmod/precision/scale limits (from numeric.h).
pub const NUMERIC_MAX_PRECISION: i32 = 1000;
pub const NUMERIC_MIN_SCALE: i32 = -1000;
pub const NUMERIC_MAX_SCALE: i32 = 1000;
pub const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;
pub const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
pub const NUMERIC_MAX_RESULT_SCALE: i32 = NUMERIC_MAX_PRECISION * 2;
pub const NUMERIC_MIN_SIG_DIGITS: i32 = 16;

/// `VARHDRSZ`, the varlena length-header size in bytes.  Re-uses the
/// crate-level definition (`heaptuple::VARHDRSZ`) to keep a single source of
/// truth.
pub use crate::VARHDRSZ;

// ---------------------------------------------------------------------------
// Safe byte-view accessors over the varlena payload (`&[u8]`).
//
// The slice is the entire on-disk `numeric` value, i.e. it starts at the
// varlena header.  The first header word (at byte offset VARHDRSZ) determines
// the format.  These mirror the `NUMERIC_*` macros from numeric.c but read the
// header word directly from bytes, so no `unsafe` or raw pointers are needed.
// ---------------------------------------------------------------------------

/// Read the 16-bit header word (`choice.n_header`) from a numeric byte slice.
///
/// The header word follows the 4-byte varlena length header.  In native (host)
/// byte order, matching the on-disk representation produced by the C struct.
#[inline]
fn header_word(num: &[u8]) -> u16 {
    debug_assert!(num.len() >= VARHDRSZ + 2);
    u16::from_ne_bytes([num[VARHDRSZ], num[VARHDRSZ + 1]])
}

/// Read the long-form weight word (`choice.n_long.n_weight`).
#[inline]
fn long_weight_word(num: &[u8]) -> i16 {
    debug_assert!(num.len() >= NUMERIC_HDRSZ);
    i16::from_ne_bytes([num[VARHDRSZ + 2], num[VARHDRSZ + 3]])
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

/// `NUMERIC_SIGN`: returns one of `NUMERIC_POS`, `NUMERIC_NEG`, `NUMERIC_NAN`,
/// `NUMERIC_PINF`, or `NUMERIC_NINF`.
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
/// sign bit and must be sign-extended (matching the C macro which ORs in
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
#[inline]
pub fn numeric_header_size(num: &[u8]) -> usize {
    VARHDRSZ
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
/// payload following the header.
///
/// The bytes are native-endian pairs forming `NumericDigit` values; callers
/// that need decoded `i16` values can iterate `chunks_exact(2)` and apply
/// [`NumericDigit::from_ne_bytes`].  We hand back bytes (not `&[NumericDigit]`)
/// because the on-disk bytes are not guaranteed to be `NumericDigit`-aligned
/// inside an arbitrary `&[u8]`, and this keeps the crate `no_std`/alloc-free.
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
// Typmod pack/unpack helpers (see make_numeric_typmod et al. in numeric.c).
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

/// `numeric_typmod_scale`: extract scale from a typmod.
///
/// The scale may be negative, so we sign-extend the 11-bit two's-complement
/// field using the `(x ^ 1024) - 1024` bit hack.
#[inline]
pub fn numeric_typmod_scale(typmod: i32) -> i32 {
    (((typmod - VARHDRSZ as i32) & 0x7ff) ^ 1024) - 1024
}

// ---------------------------------------------------------------------------
// Aggregate-transition working state (numeric.c).
//
// These are computation-time (per-call / per-aggcontext) states, NOT on-disk
// storage and NOT shared/cross-process state.  The Vec-bearing accumulators
// (`NumericSumAccum`/`NumericAggState`) need heap allocation and so live in the
// backend crate (`adt_numeric::aggregate`), keeping this no_std
// ABI crate alloc-free; only the fixed-size `Copy` states/consts are here.
// ---------------------------------------------------------------------------

/// `Int128AggState` (numeric.c:5586-5592) -- 128-bit transition state used by
/// the `numeric_poly_*` / `int*_accum` fast paths (PolyNumAggState alias on
/// 128-bit platforms).
#[derive(Clone, Copy, Debug, Default)]
pub struct Int128AggState {
    pub calc_sum_x2: bool,
    pub n: i64,
    pub sum_x: i128,
    pub sum_x2: i128,
}

/// `Int8TransTypeData` (numeric.c, near int8_avg) -- the 2-element int8 array
/// transition value (count, sum) used by avg(int2)/avg(int4) and the
/// moving-aggregate sum(int2)/sum(int4).
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct Int8TransTypeData {
    pub count: i64,
    pub sum: i64,
}

// ---------------------------------------------------------------------------
// Sort-support abbreviation constants (numeric.c:404-415).
//
// On a 64-bit Datum the abbreviation is a 64-bit signed integer; the special
// values use the int64 extremes.  The abbreviation is negated relative to the
// original value (see numeric_abbrev_convert_var), so NaN sorts last.
// ---------------------------------------------------------------------------

/// Number of bits in the abbreviated key (64 on a 64-bit-Datum build).
pub const NUMERIC_ABBREV_BITS: i32 = 64;
/// Abbreviation for NaN (`PG_INT64_MIN`).
pub const NUMERIC_ABBREV_NAN: i64 = i64::MIN;
/// Abbreviation for +Infinity (`-PG_INT64_MAX`).
pub const NUMERIC_ABBREV_PINF: i64 = -i64::MAX;
/// Abbreviation for -Infinity (`PG_INT64_MAX`).
pub const NUMERIC_ABBREV_NINF: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// Compile-time layout gates.
// ---------------------------------------------------------------------------

const _: () = {
    assert!(size_of::<NumericData>() == 8);
    assert!(align_of::<NumericData>() == 4);
    assert!(offset_of!(NumericData, choice) == 4);

    assert!(size_of::<NumericShort>() == 2);
    assert!(size_of::<NumericLong>() == 4);
    assert!(size_of::<NumericChoice>() == 4);

    assert!(size_of::<NumericDigit>() == 2);

    // Header-size constants must agree with the C macros.
    assert!(NUMERIC_HDRSZ == VARHDRSZ + size_of::<u16>() + size_of::<i16>());
    assert!(NUMERIC_HDRSZ_SHORT == VARHDRSZ + size_of::<u16>());
};

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::{vec, vec::Vec};

    /// Build a minimal short-format numeric byte vector: 4-byte varlena header
    /// (value ignored by accessors here) + 2-byte short header + digits.
    fn short_numeric(header: u16, digits: &[NumericDigit]) -> Vec<u8> {
        let mut v = vec![0u8; VARHDRSZ];
        v.extend_from_slice(&header.to_ne_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_ne_bytes());
        }
        v
    }

    fn long_numeric(sign_dscale: u16, weight: i16, digits: &[NumericDigit]) -> Vec<u8> {
        let mut v = vec![0u8; VARHDRSZ];
        v.extend_from_slice(&sign_dscale.to_ne_bytes());
        v.extend_from_slice(&weight.to_ne_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_ne_bytes());
        }
        v
    }

    #[test]
    fn short_weight_sign_extends() {
        // weight field = 0x3F with sign bit 0x40 set -> -1
        let n = short_numeric(NUMERIC_SHORT | NUMERIC_SHORT_WEIGHT_SIGN_MASK | 0x003F, &[]);
        assert!(numeric_is_short(&n));
        assert_eq!(numeric_weight(&n), -1);

        // weight field = 0x00 with sign bit set -> -64 (the min)
        let n = short_numeric(NUMERIC_SHORT | NUMERIC_SHORT_WEIGHT_SIGN_MASK, &[]);
        assert_eq!(numeric_weight(&n), NUMERIC_SHORT_WEIGHT_MIN);

        // positive weight, no sign bit -> 0x3F = 63 (the max)
        let n = short_numeric(NUMERIC_SHORT | 0x003F, &[]);
        assert_eq!(numeric_weight(&n), NUMERIC_SHORT_WEIGHT_MAX);
    }

    #[test]
    fn short_dscale_and_sign() {
        // dscale = 5 packed into bits 0x1F80, negative sign.
        let header = NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK | (5u16 << NUMERIC_SHORT_DSCALE_SHIFT);
        let n = short_numeric(header, &[]);
        assert_eq!(numeric_dscale(&n), 5);
        assert_eq!(numeric_sign(&n), NUMERIC_NEG);
    }

    #[test]
    fn long_format_fields() {
        // sign = NEG, dscale = 1000, weight = -3, two digits.
        let n = long_numeric(NUMERIC_NEG | 1000, -3, &[1234, 5678]);
        assert!(!numeric_is_short(&n));
        assert!(!numeric_header_is_short(&n));
        assert_eq!(numeric_sign(&n), NUMERIC_NEG);
        assert_eq!(numeric_dscale(&n), 1000);
        assert_eq!(numeric_weight(&n), -3);
        assert_eq!(numeric_header_size(&n), NUMERIC_HDRSZ);
        assert_eq!(numeric_ndigits(&n, n.len()), 2);
        let digits = numeric_digits(&n);
        assert_eq!(numeric_digit_at(digits, 0), 1234);
        assert_eq!(numeric_digit_at(digits, 1), 5678);
    }

    #[test]
    fn special_values() {
        let nan = short_numeric(NUMERIC_NAN, &[]);
        assert!(numeric_is_special(&nan));
        assert!(numeric_is_nan(&nan));
        assert!(!numeric_is_inf(&nan));
        assert_eq!(numeric_sign(&nan), NUMERIC_NAN);

        let pinf = short_numeric(NUMERIC_PINF, &[]);
        assert!(numeric_is_pinf(&pinf));
        assert!(numeric_is_inf(&pinf));
        assert_eq!(numeric_sign(&pinf), NUMERIC_PINF);

        let ninf = short_numeric(NUMERIC_NINF, &[]);
        assert!(numeric_is_ninf(&ninf));
        assert!(numeric_is_inf(&ninf));
        assert_eq!(numeric_sign(&ninf), NUMERIC_NINF);
    }

    #[test]
    fn typmod_round_trip() {
        let tm = make_numeric_typmod(10, 2);
        assert!(is_valid_numeric_typmod(tm));
        assert_eq!(numeric_typmod_precision(tm), 10);
        assert_eq!(numeric_typmod_scale(tm), 2);

        // Negative scale must sign-extend.
        let tm = make_numeric_typmod(5, -3);
        assert_eq!(numeric_typmod_precision(tm), 5);
        assert_eq!(numeric_typmod_scale(tm), -3);
    }
}
