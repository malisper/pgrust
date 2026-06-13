//! Construct family: `construct_array` / `construct_md_array` /
//! `construct_empty_array` / `deconstruct_array` plus the
//! `initArrayResult*` / `accumArrayResult*` / `makeArrayResult*` build-state
//! accumulators.
//!
//! This family OWNS the inward `backend-utils-adt-arrayfuncs-seams` and is the
//! source of the functions [`crate::init_seams`] installs. The public function
//! signatures below match those seam signatures exactly.

use mcx::{Mcx, PgString, PgVec};
use types_array::ArrayType;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_nodes::{EStateData, EcxtId};
use types_tuple::heaptuple::ItemPointerData;

use backend_utils_adt_arrayfuncs_seams::{ArrayBuildCtx, ArrayBuildStateAnyHandle};

// ---------------------------------------------------------------------------
// construct_* / deconstruct_* (arrayfuncs.c) — in-process API (no seam).
// ---------------------------------------------------------------------------

/// `construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign)`
/// (arrayfuncs.c): build a one-dimensional array from element `Datum`s.
pub fn construct_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("construct: construct_array")
}

/// `construct_md_array(elems, nulls, ndims, dims, lbs, elmtype, elmlen,
/// elmbyval, elmalign)` (arrayfuncs.c): build a multi-dimensional array.
pub fn construct_md_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    nulls: Option<&[bool]>,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("construct: construct_md_array")
}

/// `construct_empty_array(elmtype)` (arrayfuncs.c): a zero-dimensional array.
pub fn construct_empty_array<'mcx>(mcx: Mcx<'mcx>, elmtype: Oid) -> PgResult<PgVec<'mcx, u8>> {
    todo!("construct: construct_empty_array")
}

/// `deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elemsp,
/// &nullsp, &nelemsp)` (arrayfuncs.c): split an array buffer into per-element
/// `(Datum, isnull)` pairs.
pub fn deconstruct_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, (Datum, bool)>> {
    todo!("construct: deconstruct_array")
}

/// `array_contains_nulls(array)` (arrayfuncs.c): whether any element is null.
pub fn array_contains_nulls(array: &[u8]) -> bool {
    todo!("construct: array_contains_nulls")
}

// ---------------------------------------------------------------------------
// ArrayBuildState (single-element accumulator), arrayfuncs.c.
//
// `initArrayResult` / `accumArrayResult` / `makeArrayResult` (the non-`Any`
// variants over a concrete `ArrayBuildState`) land with the build-state types
// (`ArrayBuildState`, `ArrayBuildStateArr`), which `types-datum::array_build`
// will gain alongside the existing `ArrayBuildStateAny` when this family is
// implemented. They are intentionally not declared at scaffold stage to avoid
// inventing types ahead of their definition.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Inward seam implementations (installed by crate::init_seams). The
// signatures below MUST match `backend-utils-adt-arrayfuncs-seams`.
// ---------------------------------------------------------------------------

/// Seam `init_array_result_any` — `initArrayResultAny` (arrayfuncs.c).
pub fn init_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    input_type: Oid,
) -> PgResult<ArrayBuildStateAnyHandle<'mcx>> {
    todo!("construct: initArrayResultAny (inward seam)")
}

/// Seam `accum_array_result_any` — `accumArrayResultAny` (arrayfuncs.c).
pub fn accum_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
    dvalue: Datum,
    disnull: bool,
    input_type: Oid,
) -> PgResult<ArrayBuildStateAnyHandle<'mcx>> {
    todo!("construct: accumArrayResultAny (inward seam)")
}

/// Seam `make_array_result_any` — `makeArrayResultAny` (arrayfuncs.c).
pub fn make_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
) -> PgResult<Datum> {
    todo!("construct: makeArrayResultAny (inward seam)")
}

/// Seam `pfree_array_datum` — free a previously built array `Datum`.
pub fn pfree_array_datum(curarray: Datum) {
    todo!("construct: pfree_array_datum (inward seam)")
}

/// Seam `construct_array_builtin` — `construct_array_builtin` (arrayfuncs.c).
pub fn construct_array_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
) -> PgResult<Datum> {
    todo!("construct: construct_array_builtin (inward seam)")
}

/// Seam `deconstruct_text_array` — `deconstruct_array_builtin(..., TEXTOID)`.
pub fn deconstruct_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    todo!("construct: deconstruct_text_array (inward seam)")
}

/// Seam `deconstruct_tid_array` — `deconstruct_array_builtin(..., TIDOID)`.
pub fn deconstruct_tid_array<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
) -> PgResult<PgVec<'mcx, (ItemPointerData, bool)>> {
    todo!("construct: deconstruct_tid_array (inward seam)")
}

/// Seam `construct_text_array` — `accumArrayResult`/`makeArrayResult` over
/// `TEXTOID`.
pub fn construct_text_array<'mcx>(mcx: Mcx<'mcx>, elems: &[&str]) -> PgResult<Datum> {
    todo!("construct: construct_text_array (inward seam)")
}

/// Re-export of the on-disk header type for build-state finalizers.
pub type Header = ArrayType;
