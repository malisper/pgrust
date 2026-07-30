//! nbtcompare.c: btree comparison builtins for the trivial by-value types,
//! plus the skip-support increment/decrement kernels (consumed by the
//! skipsupport crate's dispatcher, never via fmgr). sortsupport rows
//! (3129-3134) stay unregistered — that substrate is unported, so fmgr
//! resolve of those OIDs panics loudly by design.
//! STRESS_SORT_INT_MIN is a C debug build option; production returns -1/+1.

pub mod builtins;

#[cfg(test)]
mod tests;

use ::array::oidvector;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};

const A_LESS_THAN_B: i32 = -1;
const A_GREATER_THAN_B: i32 = 1;
const OIDOID: Oid = 26;

macro_rules! threeway {
    ($($fname:ident: $a:ty, $b:ty;)*) => {$(
        #[inline]
        pub fn $fname(a: $a, b: $b) -> i32 {
            if (a as i64) > (b as i64) {
                A_GREATER_THAN_B
            } else if (a as i64) == (b as i64) {
                0
            } else {
                A_LESS_THAN_B
            }
        }
    )*};
}

threeway! {
    btint4cmp: i32, i32;
    btint8cmp: i64, i64;
    btint48cmp: i32, i64;
    btint84cmp: i64, i32;
    btint24cmp: i16, i32;
    btint42cmp: i32, i16;
    btint28cmp: i16, i64;
    btint82cmp: i64, i16;
}

#[inline]
pub fn btboolcmp(a: bool, b: bool) -> i32 {
    a as i32 - b as i32
}

#[inline]
pub fn btint2cmp(a: i16, b: i16) -> i32 {
    a as i32 - b as i32
}

#[inline]
pub fn btoidcmp(a: Oid, b: Oid) -> i32 {
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

// C compares chars as unsigned.
#[inline]
pub fn btcharcmp(a: u8, b: u8) -> i32 {
    a as i32 - b as i32
}

macro_rules! skip_incdec {
    ($($dec:ident, $inc:ident: $as_:ident, $from:ident, $ty:ty, $min:expr, $max:expr;)*) => {$(
        pub fn $dec(existing: ::datum::Datum, underflow: &mut bool) -> ::datum::Datum {
            let v: $ty = existing.$as_();
            if v == $min {
                *underflow = true;
                return ::datum::Datum::null();
            }
            *underflow = false;
            ::datum::Datum::$from(v - 1)
        }
        pub fn $inc(existing: ::datum::Datum, overflow: &mut bool) -> ::datum::Datum {
            let v: $ty = existing.$as_();
            if v == $max {
                *overflow = true;
                return ::datum::Datum::null();
            }
            *overflow = false;
            ::datum::Datum::$from(v + 1)
        }
    )*};
}

skip_incdec! {
    int2_decrement, int2_increment: as_i16, from_i16, i16, i16::MIN, i16::MAX;
    int4_decrement, int4_increment: as_i32, from_i32, i32, i32::MIN, i32::MAX;
    int8_decrement, int8_increment: as_i64, from_i64, i64, i64::MIN, i64::MAX;
    oid_decrement, oid_increment: as_u32, from_u32, u32, 0u32, u32::MAX;
    char_decrement, char_increment: as_u8, from_u8, u8, 0u8, u8::MAX;
}

pub fn bool_decrement(existing: ::datum::Datum, underflow: &mut bool) -> ::datum::Datum {
    if !existing.as_bool() {
        *underflow = true;
        return ::datum::Datum::null();
    }
    *underflow = false;
    ::datum::Datum::from_bool(false)
}

pub fn bool_increment(existing: ::datum::Datum, overflow: &mut bool) -> ::datum::Datum {
    if existing.as_bool() {
        *overflow = true;
        return ::datum::Datum::null();
    }
    *overflow = false;
    ::datum::Datum::from_bool(true)
}

/// check_valid_oidvector (oid.c): general oid[] casts to oidvector can
/// violate the type's layout restrictions; every SQL-boundary caller checks.
pub fn check_valid_oidvector(v: &oidvector) -> PgResult<()> {
    if v.ndim != 1 || v.dataoffset != 0 || v.elemtype != OIDOID {
        return Err(not_valid_oidvector());
    }
    Ok(())
}

pub fn btoidvectorcmp(a: &oidvector, a_values: &[Oid], b: &oidvector, b_values: &[Oid]) -> i32 {
    if a.dim1 != b.dim1 {
        return a.dim1 - b.dim1;
    }
    for i in 0..a.dim1 as usize {
        if a_values[i] != b_values[i] {
            return if a_values[i] > b_values[i] {
                A_GREATER_THAN_B
            } else {
                A_LESS_THAN_B
            };
        }
    }
    0
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_valid_oidvector() -> Box<PgError> {
    Box::new(
        PgError::error("array is not a valid oidvector")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}
