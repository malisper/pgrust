//! Seam declarations for the `backend-utils-adt-array-more` unit
//! (`utils/adt/arrayfuncs.c`), the array varlena subsystem.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;

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
