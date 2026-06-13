//! Seam declarations for the `backend-utils-adt-numeric` unit
//! (`utils/adt/numeric.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `numeric_maximum_size(typmod)` (numeric.c): the maximum on-disk size of
    /// a `numeric` value with the given typmod, or -1 if indeterminate. Pure
    /// arithmetic on the typmod-encoded precision/scale; no allocation, no
    /// error path.
    pub fn numeric_maximum_size(typmod: i32) -> i32
);

seam_core::seam!(
    /// `DatumGetFloat8(DirectFunctionCall1(numeric_float8,
    /// DirectFunctionCall2(numeric_sub, v1, v2)))` — the `numrange_subdiff`
    /// body (rangetypes.c:1703): the `numeric` subtype distance `v1 - v2` as a
    /// `float8`. `v1` / `v2` are `numeric` `Datum`s. `Err` carries the
    /// `numeric_sub` / `numeric_float8` `ereport(ERROR)`s (e.g. overflow).
    pub fn numeric_subdiff(
        v1: types_datum::Datum,
        v2: types_datum::Datum,
    ) -> types_error::PgResult<f64>
);
