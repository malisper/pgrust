//! Element/slice family: scalar element get/set (`array_get_element` /
//! `array_set_element` and their `_expanded` forms, `array_ref` /
//! `array_set`), slice get/set (`array_get_slice` / `array_set_slice`), and
//! the dimension introspection functions (`array_ndims`, `array_dims`,
//! `array_lower`, `array_upper`, `array_length`, `array_cardinality`).

use mcx::{Mcx, PgVec};
use types_datum::datum::Datum;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Scalar element get/set (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_get_element(arraydatum, nSubscripts, indx, arraytyplen, elmlen,
/// elmbyval, elmalign, &isNull)` (arrayfuncs.c).
pub fn array_get_element<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    todo!("element_slice: array_get_element")
}

/// `array_set_element(arraydatum, nSubscripts, indx, dataValue, isNull,
/// arraytyplen, elmlen, elmbyval, elmalign)` (arrayfuncs.c).
pub fn array_set_element<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("element_slice: array_set_element")
}

/// `array_get_element_expanded(...)` (arrayfuncs.c).
pub fn array_get_element_expanded<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    todo!("element_slice: array_get_element_expanded")
}

/// `array_set_element_expanded(...)` (arrayfuncs.c).
pub fn array_set_element_expanded<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("element_slice: array_set_element_expanded")
}

/// `array_ref(array, nSubscripts, indx, arraytyplen, elmlen, elmbyval,
/// elmalign, &isNull)` (arrayfuncs.c) — thin wrapper over `array_get_element`.
pub fn array_ref<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    todo!("element_slice: array_ref")
}

/// `array_set(array, nSubscripts, indx, dataValue, isNull, arraytyplen,
/// elmlen, elmbyval, elmalign)` (arrayfuncs.c) — wrapper over
/// `array_set_element`.
pub fn array_set<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("element_slice: array_set")
}

// ---------------------------------------------------------------------------
// Slice get/set (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_get_slice(arraydatum, nSubscripts, upperIndx, lowerIndx,
/// upperProvided, lowerProvided, arraytyplen, elmlen, elmbyval, elmalign)`
/// (arrayfuncs.c).
pub fn array_get_slice<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    upper_indx: &[i32],
    lower_indx: &[i32],
    upper_provided: &[bool],
    lower_provided: &[bool],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("element_slice: array_get_slice")
}

/// `array_set_slice(arraydatum, nSubscripts, upperIndx, lowerIndx,
/// upperProvided, lowerProvided, srcArrayDatum, isNull, arraytyplen, elmlen,
/// elmbyval, elmalign)` (arrayfuncs.c).
pub fn array_set_slice<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    upper_indx: &[i32],
    lower_indx: &[i32],
    upper_provided: &[bool],
    lower_provided: &[bool],
    src_array: &[u8],
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("element_slice: array_set_slice")
}

// ---------------------------------------------------------------------------
// Dimension introspection (arrayfuncs.c) — SQL functions.
// ---------------------------------------------------------------------------

/// `array_ndims(arr)` (arrayfuncs.c): the dimension count, or `None` for a
/// zero-dimensional array (C: SQL NULL).
pub fn array_ndims(array: &[u8]) -> Option<i32> {
    todo!("element_slice: array_ndims")
}

/// `array_dims(arr)` (arrayfuncs.c): the `[lb:ub]...` text form, or `None`.
pub fn array_dims<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    todo!("element_slice: array_dims")
}

/// `array_lower(arr, reqdim)` (arrayfuncs.c): the lower bound of dimension
/// `reqdim`, or `None`.
pub fn array_lower(array: &[u8], reqdim: i32) -> Option<i32> {
    todo!("element_slice: array_lower")
}

/// `array_upper(arr, reqdim)` (arrayfuncs.c): the upper bound of dimension
/// `reqdim`, or `None`.
pub fn array_upper(array: &[u8], reqdim: i32) -> Option<i32> {
    todo!("element_slice: array_upper")
}

/// `array_length(arr, reqdim)` (arrayfuncs.c): the length of dimension
/// `reqdim`, or `None`.
pub fn array_length(array: &[u8], reqdim: i32) -> Option<i32> {
    todo!("element_slice: array_length")
}

/// `array_cardinality(arr)` (arrayfuncs.c): total element count.
pub fn array_cardinality(array: &[u8]) -> i32 {
    todo!("element_slice: array_cardinality")
}
