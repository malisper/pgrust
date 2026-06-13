//! Operator family: equality / ordering comparison (`array_eq`, `array_ne`,
//! `array_lt`, `array_gt`, `array_le`, `array_ge`, `btarraycmp`, `array_cmp`),
//! hashing (`hash_array`, `hash_array_extended`), and containment
//! (`arrayoverlap`, `arraycontains`, `arraycontained` via
//! `array_contain_compare`).
//!
//! Element equality / comparison / hashing dispatch through the fmgr owner's
//! `element_eq` / `element_cmp` / `element_hash` / `element_hash_extended`
//! seams (the cached typcache support-proc finfos); element storage metadata
//! comes from the lsyscache owner's `get_typlenbyvalalign`.

use types_core::Oid;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Comparison (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_eq(array1, array2)` (arrayfuncs.c), under the given `collation`.
pub fn array_eq(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_eq")
}

/// `array_ne(array1, array2)` (arrayfuncs.c) — `!array_eq`.
pub fn array_ne(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_ne")
}

/// `array_lt(array1, array2)` (arrayfuncs.c) — `array_cmp < 0`.
pub fn array_lt(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_lt")
}

/// `array_gt(array1, array2)` (arrayfuncs.c) — `array_cmp > 0`.
pub fn array_gt(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_gt")
}

/// `array_le(array1, array2)` (arrayfuncs.c) — `array_cmp <= 0`.
pub fn array_le(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_le")
}

/// `array_ge(array1, array2)` (arrayfuncs.c) — `array_cmp >= 0`.
pub fn array_ge(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: array_ge")
}

/// `btarraycmp(array1, array2)` (arrayfuncs.c) — the btree 3-way comparator
/// wrapper over `array_cmp`.
pub fn btarraycmp(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<i32> {
    todo!("ops: btarraycmp")
}

/// `array_cmp(fcinfo)` (arrayfuncs.c): the 3-way element-wise comparison that
/// backs all the ordering operators.
pub fn array_cmp(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<i32> {
    todo!("ops: array_cmp")
}

// ---------------------------------------------------------------------------
// Hashing (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `hash_array(array)` (arrayfuncs.c).
pub fn hash_array(array: &[u8], collation: Oid) -> PgResult<u32> {
    todo!("ops: hash_array")
}

/// `hash_array_extended(array, seed)` (arrayfuncs.c).
pub fn hash_array_extended(array: &[u8], collation: Oid, seed: u64) -> PgResult<u64> {
    todo!("ops: hash_array_extended")
}

// ---------------------------------------------------------------------------
// Containment (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `arrayoverlap(array1, array2)` (arrayfuncs.c): whether the two arrays share
/// any element.
pub fn arrayoverlap(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: arrayoverlap")
}

/// `arraycontains(array1, array2)` (arrayfuncs.c): whether `array1` contains
/// every element of `array2`.
pub fn arraycontains(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: arraycontains")
}

/// `arraycontained(array1, array2)` (arrayfuncs.c): `arraycontains(array2,
/// array1)`.
pub fn arraycontained(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    todo!("ops: arraycontained")
}

/// `array_contain_compare(array1, array2, collation, matchall, fn_extra)`
/// (arrayfuncs.c): the shared engine behind overlap / contains / contained.
pub fn array_contain_compare(
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
    matchall: bool,
) -> PgResult<bool> {
    todo!("ops: array_contain_compare")
}
