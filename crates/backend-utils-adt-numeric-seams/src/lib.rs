//! Seam declarations for the `backend-utils-adt-numeric` unit
//! (`utils/adt/numeric.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `DatumGetBool(DirectFunctionCall2(numeric_eq, a, b))` over two on-disk
    /// `numeric` varlenas — value equality (scale-insensitive). `a`/`b` are
    /// the whole on-disk `numeric` images (varlena header included). Reached
    /// from `jsonb_util.c`'s `equalsJsonbScalarValue` `jbvNumeric` arm. Pure
    /// computation; infallible.
    pub fn numeric_eq(a: &[u8], b: &[u8]) -> bool
);

seam_core::seam!(
    /// `DatumGetInt32(DirectFunctionCall2(numeric_cmp, a, b))` over two on-disk
    /// `numeric` varlenas — the 3-way B-tree comparison (`-1`/`0`/`1`, with
    /// full special-value ordering). Reached from `jsonb_util.c`'s
    /// `compareJsonbScalarValue` `jbvNumeric` arm. Pure computation;
    /// infallible.
    pub fn numeric_cmp(a: &[u8], b: &[u8]) -> i32
);

seam_core::seam!(
    /// `DatumGetFloat8(DirectFunctionCall1(numeric_float8,
    /// DirectFunctionCall2(numeric_sub, a, b)))` over two on-disk `numeric`
    /// varlenas — the `numeric` distance `a - b` as a `float8`. This is the
    /// owned-bytes counterpart of [`numeric_subdiff`] (which takes
    /// pointer-bearing `Datum`s); reached from `brin_minmax_multi`'s
    /// `brin_minmax_multi_distance_numeric`, whose arguments arrive as detoasted
    /// `numeric` byte images on the fmgr by-reference lane. `Err` carries the
    /// `numeric_sub` / `numeric_float8` `ereport(ERROR)`s (e.g. overflow). Pure
    /// computation otherwise.
    pub fn numeric_subdiff_bytes(a: &[u8], b: &[u8]) -> types_error::PgResult<f64>
);

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
    ///
    /// Datum-unification: `v1` / `v2` are the canonical unified value type
    /// (`types_tuple::Datum<'mcx>`). They are pointer-bearing `numeric` Datums
    /// (detoasted by the caller, exactly as C's `DirectFunctionCall2` treats
    /// the `Datum` as a `Numeric` pointer), so they ride the by-value arm; the
    /// owner reads each image's varlena bytes off the carried machine word.
    pub fn numeric_subdiff<'mcx>(
        v1: types_tuple::Datum<'mcx>,
        v2: types_tuple::Datum<'mcx>,
    ) -> types_error::PgResult<f64>
);

// The `addHyperLogLog`/`estimateHyperLogLog` calls of `numeric_abbrev_convert_var`
// / `numeric_abbrev_abort` (numeric.c) are NOT seams: the HyperLogLog counter
// (`NumericSortSupport.abbr_card`) is the numeric unit's own state, held by value
// in the abbreviated-key sort state and operated on directly via
// `backend-lib-hyperloglog` (mirroring varlena's `VarStringSortSupport`).
