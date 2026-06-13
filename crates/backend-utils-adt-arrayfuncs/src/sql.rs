//! SQL family: `array_larger` / `array_smaller`, `generate_subscripts`,
//! `array_fill` / `array_remove` / `array_replace`, `width_bucket_array`,
//! `trim_array`, the array iterator (`array_create_iterator` /
//! `array_iterate` / `array_free_iterator`), and `array_map`.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Larger/smaller (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_larger(array1, array2)` (arrayfuncs.c): the greater per `array_cmp`.
pub fn array_larger<'mcx>(
    mcx: Mcx<'mcx>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_larger")
}

/// `array_smaller(array1, array2)` (arrayfuncs.c): the lesser per `array_cmp`.
pub fn array_smaller<'mcx>(
    mcx: Mcx<'mcx>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_smaller")
}

// ---------------------------------------------------------------------------
// generate_subscripts (arrayfuncs.c) — set-returning.
// ---------------------------------------------------------------------------

/// `generate_subscripts(array, dim [, reverse])` (arrayfuncs.c): the subscript
/// range of dimension `dim`, materialized in order.
pub fn generate_subscripts<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    dim: i32,
    reverse: bool,
) -> PgResult<PgVec<'mcx, i32>> {
    todo!("sql: generate_subscripts")
}

// ---------------------------------------------------------------------------
// fill / remove / replace (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_fill(value, dims [, lbs])` (arrayfuncs.c): an array of `value`s with
/// the given dimensions.
pub fn array_fill<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    is_null: bool,
    elmtype: Oid,
    dims: &[i32],
    lbs: &[i32],
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_fill")
}

/// `array_remove(array, search)` (arrayfuncs.c): the array with every element
/// equal to `search` removed.
pub fn array_remove<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_remove")
}

/// `array_replace(array, search, replace)` (arrayfuncs.c).
pub fn array_replace<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    replace: Datum,
    replace_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_replace")
}

/// `array_replace_internal(...)` (arrayfuncs.c): the shared remove/replace
/// engine (`replace=None` means remove).
pub fn array_replace_internal<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    replace: Option<(Datum, bool)>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_replace_internal")
}

// ---------------------------------------------------------------------------
// width_bucket / trim (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `width_bucket_array(operand, thresholds)` (arrayfuncs.c).
pub fn width_bucket_array(
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
) -> PgResult<i32> {
    todo!("sql: width_bucket_array")
}

/// `trim_array(array, n)` (arrayfuncs.c): drop the last `n` elements of a
/// one-dimensional array.
pub fn trim_array<'mcx>(mcx: Mcx<'mcx>, array: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: trim_array")
}

// ---------------------------------------------------------------------------
// Iterator (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_create_iterator(arr, slice_ndim, mstate)` (arrayfuncs.c).
pub fn array_create_iterator<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    slice_ndim: i32,
) -> PgResult<()> {
    todo!("sql: array_create_iterator")
}

/// `array_iterate(iterator, &value, &isnull)` (arrayfuncs.c): yield the next
/// element (or slice); `Ok(None)` at exhaustion (C: returns `false`).
pub fn array_iterate(/* iterator: &mut ArrayIterator */) -> PgResult<Option<(Datum, bool)>> {
    todo!("sql: array_iterate")
}

/// `array_free_iterator(iterator)` (arrayfuncs.c).
pub fn array_free_iterator() {
    todo!("sql: array_free_iterator")
}

// ---------------------------------------------------------------------------
// array_map (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_map(arrayd, exprstate, econtext, retType, amstate)` (arrayfuncs.c):
/// apply a per-element expression to produce a new array. The element
/// expression evaluation crosses the executor boundary.
pub fn array_map<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    ret_type: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("sql: array_map")
}
