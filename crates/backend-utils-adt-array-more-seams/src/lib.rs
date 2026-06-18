//! Seam declarations for the `backend-utils-adt-array-more` unit
//! (`utils/adt/arrayfuncs.c`), the array varlena subsystem.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;

pub use backend_utils_adt_tsvector_ext_seams::ArrayElem;

seam_core::seam!(
    /// Deconstruct a 1-D `float4[]` weight array (`win`, the detoasted array
    /// varlena bytes) into its element values, allocating the result in `mcx`.
    ///
    /// Mirrors the `utils/array.h` macros `getWeights` uses: error with
    /// `ERRCODE_ARRAY_SUBSCRIPT_ERROR` / "array of weight must be
    /// one-dimensional" when `ARR_NDIM != 1`, then with
    /// `ERRCODE_NULL_VALUE_NOT_ALLOWED` / "array of weight must not contain
    /// nulls" when `array_contains_nulls`, otherwise yield the `ARR_DATA_PTR`
    /// element values. (`getWeights` itself performs the "too short" and
    /// "weight out of range" checks on the returned values.)
    pub fn deconstruct_float4_array<'mcx>(
        mcx: Mcx<'mcx>,
        win: &[u8],
    ) -> PgResult<PgVec<'mcx, f32>>
);

seam_core::seam!(
    /// `deconstruct_array_builtin(arr, TEXTOID, ...)` (tsvector_op.c) — explode
    /// a 1-D `text[]` datum into its elements (with NULL flags).
    pub fn deconstruct_text_array(arr: &[u8]) -> PgResult<Vec<ArrayElem>>
);

seam_core::seam!(
    /// `deconstruct_array_builtin(arr, CHAROID, ...)` (tsvector_op.c) — explode
    /// a 1-D `"char"[]` datum into its elements (each a single byte).
    pub fn deconstruct_char_array(arr: &[u8]) -> PgResult<Vec<ArrayElem>>
);

seam_core::seam!(
    /// Like [`deconstruct_text_array`], but also returns `ARR_NDIM(arr)`
    /// alongside the flattened elements. The jsonfuncs path operators
    /// (`jsonb_set` / `jsonb_insert` / `jsonb_delete` over `text[]` /
    /// `jsonb_extract_path`) need the dimension count to reproduce the C
    /// `ARR_NDIM(path) > 1` "wrong number of array subscripts" guard before
    /// consuming the flat element list. `(ndim, elems)`.
    pub fn deconstruct_text_array_with_ndim(arr: &[u8]) -> PgResult<(i32, Vec<ArrayElem>)>
);

seam_core::seam!(
    /// `construct_array_builtin(elems, n, TEXTOID)` (tsvector_op.c) — build a
    /// `text[]` datum from owned element byte strings.
    pub fn construct_text_array(elems: &[Vec<u8>]) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// `construct_array_builtin(elems, n, INT2OID)` (tsvector_op.c) — build an
    /// `int2[]` datum.
    pub fn construct_int2_array(elems: &[i16]) -> PgResult<Vec<u8>>
);
