//! On-disk byte-view vocabulary for the PostgreSQL `numeric` type.
//!
//! Mirrors the `NumericData`/`NumericChoice`/`NumericLong`/`NumericShort` layout
//! and the `NUMERIC_*` macros of `postgres-18.3/src/backend/utils/adt/numeric.c`
//! (~lines 130-260). Only the safe byte-view accessors needed by consumers that
//! read an on-disk `numeric` image (e.g. the `jsonb` hash path) live here; the
//! arithmetic engine is the `backend-utils-adt-numeric` unit's. The slice passed
//! to every accessor is the entire on-disk value, starting at its varlena
//! header.

#![no_std]
#![allow(non_upper_case_globals)]

use core::mem::size_of;
use types_datum::VARHDRSZ;

/// A single base-NBASE digit (`int16` in the canonical build).
pub type NumericDigit = i16;

// ---------------------------------------------------------------------------
// Header bit-packing constants (numeric.c).
// ---------------------------------------------------------------------------

/// Mask selecting the two high (sign/format) bits.
pub const NUMERIC_SIGN_MASK: u16 = 0xC000;
pub const NUMERIC_POS: u16 = 0x0000;
pub const NUMERIC_NEG: u16 = 0x4000;
pub const NUMERIC_SHORT: u16 = 0x8000;
pub const NUMERIC_SPECIAL: u16 = 0xC000;

/// Special-value sign/format bits.
pub const NUMERIC_EXT_SIGN_MASK: u16 = 0xF000;
pub const NUMERIC_NAN: u16 = 0xC000;
pub const NUMERIC_PINF: u16 = 0xD000;
pub const NUMERIC_NINF: u16 = 0xF000;
pub const NUMERIC_INF_SIGN_MASK: u16 = 0x2000;

// Short-format field definitions.
pub const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
pub const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
pub const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
pub const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
pub const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;

// Long-format field definitions.
pub const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;

// ---------------------------------------------------------------------------
// Safe byte-view accessors over the varlena payload (`&[u8]`).
// ---------------------------------------------------------------------------

/// Read the 16-bit header word (`choice.n_header`) from a numeric byte slice.
#[inline]
fn header_word(num: &[u8]) -> u16 {
    debug_assert!(num.len() >= VARHDRSZ + 2);
    u16::from_ne_bytes([num[VARHDRSZ], num[VARHDRSZ + 1]])
}

/// Read the long-form weight word (`choice.n_long.n_weight`).
#[inline]
fn long_weight_word(num: &[u8]) -> i16 {
    debug_assert!(num.len() >= VARHDRSZ + 4);
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
