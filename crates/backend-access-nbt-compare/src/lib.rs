//! Port of `src/backend/access/nbtree/nbtcompare.c` (PostgreSQL 18.3).
//!
//! Comparison functions for the btree access method, plus the sortsupport and
//! skipsupport routines registered for the in-core trivial
//! integer/oid/char/bool/oidvector opclasses.
//!
//! These functions are reached through the fmgr calling convention in C. The
//! fmgr boundary is a seam, so here they are expressed as idiomatic Rust
//! functions over the concrete datatypes; the three-way comparison logic,
//! branch order and overflow handling are ported 1:1 from C.
//!
//! `STRESS_SORT_INT_MIN` is a debugging-only build option in C; production
//! builds use `-1` / `+1`, which is what we reproduce here.
//!
//! ## Sortsupport / skipsupport
//!
//! C's `*sortsupport` / `*skipsupport` strategy routines mutate a live
//! [`SortSupportData`] / [`SkipSupportData`] node by storing C function pointers
//! into its `comparator` / `increment` / `decrement` slots. In this repo those
//! slots are `Copy` tokens that only the sort/skip substrate knows how to mint
//! and interpret, so — mirroring the `SortComparatorId` precedent — the *field
//! write* is delegated to a
//! [`backend_access_nbt_compare_seams`] install seam (OUTWARD, owned by the
//! unported substrate; `nbtcompare` calls but never installs it), while the
//! comparison / increment / decrement *kernels* and the boundary `Datum`s
//! remain pure and in-crate.
//!
//! The skipsupport increment/decrement kernels are exposed as pure functions
//! returning `(Datum, bool)` — the produced value paired with C's
//! `*overflow` / `*underflow` flag. On the overflow/underflow path the returned
//! `Datum` is undefined (C returns `(Datum) 0`), exactly as C documents.

// NB: not `#![no_std]` — the fmgr builtin registration layer
// (`fmgr_builtins`) registers the btree comparison functions into the
// fmgr-core builtin table (C: `fmgr_builtins[]`), which uses `String`/`std`.
#![allow(non_snake_case)]

use types_core::primitive::{InvalidOid, Oid};
use types_datum::Datum;
use types_sortsupport::{SkipSupportData, SortSupportData};

use backend_access_nbt_compare_seams as sort;

mod fmgr_builtins;

/// `A_LESS_THAN_B` in the non-`STRESS_SORT_INT_MIN` (production) build.
const A_LESS_THAN_B: i32 = -1;
/// `A_GREATER_THAN_B` in the non-`STRESS_SORT_INT_MIN` (production) build.
const A_GREATER_THAN_B: i32 = 1;

/// `OID_MAX` (`postgres_ext.h`): the largest valid `Oid`.
const OID_MAX: Oid = Oid::MAX;
/// `UCHAR_MAX` (`<limits.h>`).
const UCHAR_MAX: u8 = u8::MAX;

// OIDs of the in-core btree `*sortsupport` functions (catalog/pg_proc.dat).
/// `Oid` of `btint2sortsupport`.
const F_BTINT2SORTSUPPORT: Oid = 3129;
/// `Oid` of `btint4sortsupport`.
const F_BTINT4SORTSUPPORT: Oid = 3130;
/// `Oid` of `btint8sortsupport`.
const F_BTINT8SORTSUPPORT: Oid = 3131;
/// `Oid` of `btoidsortsupport`.
const F_BTOIDSORTSUPPORT: Oid = 3134;

// OIDs of the in-core btree `*skipsupport` functions (catalog/pg_proc.dat).
/// `Oid` of `btint2skipsupport`.
const F_BTINT2SKIPSUPPORT: Oid = 6402;
/// `Oid` of `btint4skipsupport`.
const F_BTINT4SKIPSUPPORT: Oid = 6403;
/// `Oid` of `btint8skipsupport`.
const F_BTINT8SKIPSUPPORT: Oid = 6404;
/// `Oid` of `btoidskipsupport`.
const F_BTOIDSKIPSUPPORT: Oid = 6405;
/// `Oid` of `btcharskipsupport`.
const F_BTCHARSKIPSUPPORT: Oid = 6406;
/// `Oid` of `btboolskipsupport`.
const F_BTBOOLSKIPSUPPORT: Oid = 6408;

/// The owned-model stand-in for `OidFunctionCall1(sortfunc,
/// PointerGetDatum(ssup))` over an in-core btree `*sortsupport` routine: an
/// owned `Datum` cannot carry the `SortSupport` pointer, so the dispatch crosses
/// as `&mut SortSupportData`. Returns whether `sortfunc` named one of this
/// crate's sortsupport functions (so the caller knows whether to fall through to
/// its fmgr path for an as-yet-unported sortsupport builtin).
fn run_sortsupport(sortfunc: Oid, ssup: &mut SortSupportData) -> bool {
    match sortfunc {
        F_BTINT2SORTSUPPORT => btint2sortsupport(ssup),
        F_BTINT4SORTSUPPORT => btint4sortsupport(ssup),
        F_BTINT8SORTSUPPORT => btint8sortsupport(ssup),
        F_BTOIDSORTSUPPORT => btoidsortsupport(ssup),
        _ => return false,
    }
    true
}

/// The owned-model stand-in for `OidFunctionCall1(skipSupportFunction,
/// PointerGetDatum(sksup))` over an in-core btree `*skipsupport` routine: an
/// owned `Datum` cannot carry the `SkipSupport` pointer, so the dispatch crosses
/// as `&mut SkipSupportData`. Returns whether `skipfunc` named one of this
/// crate's skipsupport functions (so the caller knows whether to fall through to
/// its fmgr path for an as-yet-unported skipsupport builtin).
fn run_skipsupport(skipfunc: Oid, sksup: &mut SkipSupportData) -> bool {
    match skipfunc {
        F_BTBOOLSKIPSUPPORT => btboolskipsupport(sksup),
        F_BTINT2SKIPSUPPORT => btint2skipsupport(sksup),
        F_BTINT4SKIPSUPPORT => btint4skipsupport(sksup),
        F_BTINT8SKIPSUPPORT => btint8skipsupport(sksup),
        F_BTOIDSKIPSUPPORT => btoidskipsupport(sksup),
        F_BTCHARSKIPSUPPORT => btcharskipsupport(sksup),
        _ => return false,
    }
    true
}

/// This crate owns two inward seams ([`run_sortsupport`](sort::run_sortsupport)
/// and [`run_skipsupport`](sort::run_skipsupport)), the by-OID sortsupport /
/// skipsupport dispatch the sort/skip substrate calls in lieu of the
/// pointer-carrying `OidFunctionCall1`. The comparison kernels are reached
/// through fmgr dispatch (not a cross-crate seam call), and the sort/skip
/// install seams are OUTWARD (owned by the substrate).
pub fn init_seams() {
    sort::run_sortsupport::set(run_sortsupport);
    sort::run_skipsupport::set(run_skipsupport);
    // Register the btree comparison builtins into the fmgr fast-path table so
    // `fmgr_isbuiltin` resolves the catalog-index `BTORDER_PROC`s at boot
    // without recursing into the not-yet-built syscache.
    fmgr_builtins::register_nbtcompare_builtins();
}

// ---------------------------------------------------------------------------
// Datum (un)packing helpers, mirroring DatumGetXxx / XxxGetDatum.  The
// `Datum` is a newtype over a machine word with explicit typed conversions;
// integers are stored exactly as the fmgr macros do.
// ---------------------------------------------------------------------------

#[inline]
fn datum_get_bool(d: Datum) -> bool {
    // `DatumGetBool(X)` is `((bool) ((X) != 0))`.
    d.as_bool()
}
#[inline]
fn bool_get_datum(b: bool) -> Datum {
    Datum::from_bool(b)
}
#[inline]
fn datum_get_int16(d: Datum) -> i16 {
    d.as_i16()
}
#[inline]
fn int16_get_datum(v: i16) -> Datum {
    Datum::from_i16(v)
}
#[inline]
fn datum_get_int32(d: Datum) -> i32 {
    d.as_i32()
}
#[inline]
fn int32_get_datum(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn datum_get_int64(d: Datum) -> i64 {
    d.as_i64()
}
#[inline]
fn int64_get_datum(v: i64) -> Datum {
    Datum::from_i64(v)
}
#[inline]
fn datum_get_object_id(d: Datum) -> Oid {
    d.as_oid()
}
#[inline]
fn object_id_get_datum(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn datum_get_uint8(d: Datum) -> u8 {
    d.as_u8()
}
#[inline]
fn uint8_get_datum(v: u8) -> Datum {
    Datum::from_u8(v)
}
#[inline]
fn char_get_datum(v: u8) -> Datum {
    // C `CharGetDatum((uint8) cexisting +/- 1)` — store the byte value.
    Datum::from_u8(v)
}

// ===========================================================================
// bool
// ===========================================================================

/// `btboolcmp`
pub fn btboolcmp(a: bool, b: bool) -> i32 {
    (a as i32) - (b as i32)
}

/// `bool_decrement` — returns the decremented value paired with `*underflow`.
pub fn bool_decrement(existing: Datum) -> (Datum, bool) {
    let bexisting = datum_get_bool(existing);

    if !bexisting {
        // return value is undefined
        return (Datum::null(), true);
    }

    (bool_get_datum(false), false)
}

/// `bool_increment` — returns the incremented value paired with `*overflow`.
pub fn bool_increment(existing: Datum) -> (Datum, bool) {
    let bexisting = datum_get_bool(existing);

    if bexisting {
        // return value is undefined
        return (Datum::null(), true);
    }

    (bool_get_datum(true), false)
}

/// `btboolskipsupport`
pub fn btboolskipsupport(sksup: &mut SkipSupportData) {
    // sksup->low_elem = BoolGetDatum(false);
    // sksup->high_elem = BoolGetDatum(true);
    sksup.low_elem = bool_get_datum(false);
    sksup.high_elem = bool_get_datum(true);
    // sksup->decrement = bool_decrement; sksup->increment = bool_increment;
    sort::install_skipsupport_bool::call(sksup, bool_increment, bool_decrement);
}

// ===========================================================================
// int2 (int16)
// ===========================================================================

/// `btint2cmp`
pub fn btint2cmp(a: i16, b: i16) -> i32 {
    (a as i32) - (b as i32)
}

/// `btint2fastcmp` — SortSupport fast comparator over packed `Datum`s.
pub fn btint2fastcmp(x: Datum, y: Datum) -> i32 {
    let a = datum_get_int16(x);
    let b = datum_get_int16(y);

    (a as i32) - (b as i32)
}

/// `btint2sortsupport`
pub fn btint2sortsupport(ssup: &mut SortSupportData) {
    // ssup->comparator = btint2fastcmp;
    sort::install_sortsupport_int2::call(ssup, btint2fastcmp);
}

/// `int2_decrement` — returns the decremented value paired with `*underflow`.
pub fn int2_decrement(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int16(existing);

    if iexisting == i16::MIN {
        return (Datum::null(), true);
    }

    (int16_get_datum(iexisting - 1), false)
}

/// `int2_increment` — returns the incremented value paired with `*overflow`.
pub fn int2_increment(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int16(existing);

    if iexisting == i16::MAX {
        return (Datum::null(), true);
    }

    (int16_get_datum(iexisting + 1), false)
}

/// `btint2skipsupport`
pub fn btint2skipsupport(sksup: &mut SkipSupportData) {
    // sksup->low_elem = Int16GetDatum(PG_INT16_MIN);
    // sksup->high_elem = Int16GetDatum(PG_INT16_MAX);
    sksup.low_elem = int16_get_datum(i16::MIN);
    sksup.high_elem = int16_get_datum(i16::MAX);
    // sksup->decrement = int2_decrement; sksup->increment = int2_increment;
    sort::install_skipsupport_int2::call(sksup, int2_increment, int2_decrement);
}

// ===========================================================================
// int4 (int32)
// ===========================================================================

/// `btint4cmp`
pub fn btint4cmp(a: i32, b: i32) -> i32 {
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `ssup_datum_int32_cmp` is the shared signed-int32 fast comparator; mirror it
/// here so `btint4sortsupport` can register it.
pub fn ssup_datum_int32_cmp(x: Datum, y: Datum) -> i32 {
    let a = datum_get_int32(x);
    let b = datum_get_int32(y);

    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint4sortsupport`
pub fn btint4sortsupport(ssup: &mut SortSupportData) {
    // ssup->comparator = ssup_datum_int32_cmp;
    sort::install_sortsupport_int4::call(ssup, ssup_datum_int32_cmp);
}

/// `int4_decrement` — returns the decremented value paired with `*underflow`.
pub fn int4_decrement(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int32(existing);

    if iexisting == i32::MIN {
        return (Datum::null(), true);
    }

    (int32_get_datum(iexisting - 1), false)
}

/// `int4_increment` — returns the incremented value paired with `*overflow`.
pub fn int4_increment(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int32(existing);

    if iexisting == i32::MAX {
        return (Datum::null(), true);
    }

    (int32_get_datum(iexisting + 1), false)
}

/// `btint4skipsupport`
pub fn btint4skipsupport(sksup: &mut SkipSupportData) {
    // sksup->low_elem = Int32GetDatum(PG_INT32_MIN);
    // sksup->high_elem = Int32GetDatum(PG_INT32_MAX);
    sksup.low_elem = int32_get_datum(i32::MIN);
    sksup.high_elem = int32_get_datum(i32::MAX);
    // sksup->decrement = int4_decrement; sksup->increment = int4_increment;
    sort::install_skipsupport_int4::call(sksup, int4_increment, int4_decrement);
}

// ===========================================================================
// int8 (int64)
// ===========================================================================

/// `btint8cmp`
pub fn btint8cmp(a: i64, b: i64) -> i32 {
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `ssup_datum_signed_cmp` is the shared signed-`Datum` fast comparator used on
/// 64-bit-`Datum` platforms (`SIZEOF_DATUM >= 8`); mirror it here. (On a
/// `SIZEOF_DATUM < 8` build C uses `btint8fastcmp` instead, but this repo
/// targets `SIZEOF_DATUM == 8`, so that branch is unreachable.)
pub fn ssup_datum_signed_cmp(x: Datum, y: Datum) -> i32 {
    let a = datum_get_int64(x);
    let b = datum_get_int64(y);

    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint8sortsupport` -- on `SIZEOF_DATUM >= 8` platforms (the only ones we
/// target) this installs `ssup_datum_signed_cmp`.
pub fn btint8sortsupport(ssup: &mut SortSupportData) {
    // #if SIZEOF_DATUM >= 8: ssup->comparator = ssup_datum_signed_cmp;
    sort::install_sortsupport_int8::call(ssup, ssup_datum_signed_cmp);
}

/// `int8_decrement` — returns the decremented value paired with `*underflow`.
pub fn int8_decrement(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int64(existing);

    if iexisting == i64::MIN {
        return (Datum::null(), true);
    }

    (int64_get_datum(iexisting - 1), false)
}

/// `int8_increment` — returns the incremented value paired with `*overflow`.
pub fn int8_increment(existing: Datum) -> (Datum, bool) {
    let iexisting = datum_get_int64(existing);

    if iexisting == i64::MAX {
        return (Datum::null(), true);
    }

    (int64_get_datum(iexisting + 1), false)
}

/// `btint8skipsupport`
pub fn btint8skipsupport(sksup: &mut SkipSupportData) {
    // sksup->low_elem = Int64GetDatum(PG_INT64_MIN);
    // sksup->high_elem = Int64GetDatum(PG_INT64_MAX);
    sksup.low_elem = int64_get_datum(i64::MIN);
    sksup.high_elem = int64_get_datum(i64::MAX);
    // sksup->decrement = int8_decrement; sksup->increment = int8_increment;
    sort::install_skipsupport_int8::call(sksup, int8_increment, int8_decrement);
}

// ===========================================================================
// cross-width integer comparisons
// ===========================================================================

/// `btint48cmp`
pub fn btint48cmp(a: i32, b: i64) -> i32 {
    let a = a as i64;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint84cmp`
pub fn btint84cmp(a: i64, b: i32) -> i32 {
    let b = b as i64;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint24cmp`
pub fn btint24cmp(a: i16, b: i32) -> i32 {
    let a = a as i32;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint42cmp`
pub fn btint42cmp(a: i32, b: i16) -> i32 {
    let b = b as i32;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint28cmp`
pub fn btint28cmp(a: i16, b: i64) -> i32 {
    let a = a as i64;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btint82cmp`
pub fn btint82cmp(a: i64, b: i16) -> i32 {
    let b = b as i64;
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

// ===========================================================================
// oid
// ===========================================================================

/// `btoidcmp`
pub fn btoidcmp(a: Oid, b: Oid) -> i32 {
    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btoidfastcmp` — SortSupport fast comparator over packed `Datum`s.
pub fn btoidfastcmp(x: Datum, y: Datum) -> i32 {
    let a = datum_get_object_id(x);
    let b = datum_get_object_id(y);

    if a > b {
        A_GREATER_THAN_B
    } else if a == b {
        0
    } else {
        A_LESS_THAN_B
    }
}

/// `btoidsortsupport`
pub fn btoidsortsupport(ssup: &mut SortSupportData) {
    // ssup->comparator = btoidfastcmp;
    sort::install_sortsupport_oid::call(ssup, btoidfastcmp);
}

/// `oid_decrement` — returns the decremented value paired with `*underflow`.
pub fn oid_decrement(existing: Datum) -> (Datum, bool) {
    let oexisting = datum_get_object_id(existing);

    if oexisting == InvalidOid {
        return (Datum::null(), true);
    }

    (object_id_get_datum(oexisting - 1), false)
}

/// `oid_increment` — returns the incremented value paired with `*overflow`.
pub fn oid_increment(existing: Datum) -> (Datum, bool) {
    let oexisting = datum_get_object_id(existing);

    if oexisting == OID_MAX {
        return (Datum::null(), true);
    }

    (object_id_get_datum(oexisting + 1), false)
}

/// `btoidskipsupport`
pub fn btoidskipsupport(sksup: &mut SkipSupportData) {
    // sksup->low_elem = ObjectIdGetDatum(InvalidOid);
    // sksup->high_elem = ObjectIdGetDatum(OID_MAX);
    sksup.low_elem = object_id_get_datum(InvalidOid);
    sksup.high_elem = object_id_get_datum(OID_MAX);
    // sksup->decrement = oid_decrement; sksup->increment = oid_increment;
    sort::install_skipsupport_oid::call(sksup, oid_increment, oid_decrement);
}

/// `btoidvectorcmp`
///
/// `a` and `b` are the `values` slices of two `oidvector`s; the caller (the
/// fmgr seam) is responsible for `check_valid_oidvector()` on the headers.
/// We arbitrarily choose to sort first by vector length.
pub fn btoidvectorcmp(a: &[Oid], b: &[Oid]) -> i32 {
    // We arbitrarily choose to sort first by vector length
    if a.len() != b.len() {
        // C: `PG_RETURN_INT32(a->dim1 - b->dim1)` (dim1 is int32).
        return a.len() as i32 - b.len() as i32;
    }

    for i in 0..a.len() {
        if a[i] != b[i] {
            if a[i] > b[i] {
                return A_GREATER_THAN_B;
            } else {
                return A_LESS_THAN_B;
            }
        }
    }
    0
}

// ===========================================================================
// char
// ===========================================================================

/// `btcharcmp`
///
/// Be careful to compare chars as unsigned.
pub fn btcharcmp(a: i8, b: i8) -> i32 {
    (a as u8 as i32) - (b as u8 as i32)
}

/// `char_decrement` — returns the decremented value paired with `*underflow`.
pub fn char_decrement(existing: Datum) -> (Datum, bool) {
    let cexisting = datum_get_uint8(existing);

    if cexisting == 0 {
        return (Datum::null(), true);
    }

    (char_get_datum(cexisting - 1), false)
}

/// `char_increment` — returns the incremented value paired with `*overflow`.
pub fn char_increment(existing: Datum) -> (Datum, bool) {
    let cexisting = datum_get_uint8(existing);

    if cexisting == UCHAR_MAX {
        return (Datum::null(), true);
    }

    (char_get_datum(cexisting + 1), false)
}

/// `btcharskipsupport`
pub fn btcharskipsupport(sksup: &mut SkipSupportData) {
    // btcharcmp compares chars as unsigned
    // sksup->low_elem = UInt8GetDatum(0);
    // sksup->high_elem = UInt8GetDatum(UCHAR_MAX);
    sksup.low_elem = uint8_get_datum(0);
    sksup.high_elem = uint8_get_datum(UCHAR_MAX);
    // sksup->decrement = char_decrement; sksup->increment = char_increment;
    sort::install_skipsupport_char::call(sksup, char_increment, char_decrement);
}

#[cfg(test)]
mod tests;
