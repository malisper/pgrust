//! Seam declarations for `src/backend/utils/adt/arrayutils.c` — the pure
//! integer/subscript math (`ArrayGetOffset`, `ArrayGetNItems`,
//! `ArrayCheckBounds`, and the `mda_*` multi-dimensional helpers) that
//! `arrayfuncs.c` calls.
//!
//! `arrayutils.c` is owned by the (unported) `backend-utils-adt-next` /
//! `probe-adt-arrayutils` neighbor; its `init_seams()` will install these.
//! Until then a call panics loudly.
//!
//! `ArrayGetNItems` and `ArrayCheckBounds` `ereport(ERROR)` on overflow /
//! out-of-range, so they return [`PgResult`]; `ArrayGetOffset` and the `mda_*`
//! helpers are pure and infallible.

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;

seam_core::seam!(
    /// `ArrayGetOffset(n, dim, lb, indx)` (arrayutils.c): linearize an
    /// `n`-dimensional subscript tuple to a flat element offset.
    pub fn array_get_offset(n: i32, dim: &[i32], lb: &[i32], indx: &[i32]) -> i32
);

seam_core::seam!(
    /// `ArrayGetNItems(ndim, dims)` (arrayutils.c): total element count;
    /// `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED)` if it overflows
    /// `MaxArraySize`.
    pub fn array_get_n_items(ndim: i32, dims: &[i32]) -> PgResult<i32>
);

seam_core::seam!(
    /// `ArrayCheckBounds(ndim, dims, lb)` (arrayutils.c): verify `dim[i] +
    /// lb[i]` does not overflow `INT_MAX`; `ereport(ERROR)` otherwise.
    pub fn array_check_bounds(ndim: i32, dims: &[i32], lb: &[i32]) -> PgResult<()>
);

seam_core::seam!(
    /// `mda_get_range(n, span, st, endp)` (arrayutils.c): `span[i] = endp[i] -
    /// st[i] + 1` for each dimension. Writes into `span`.
    pub fn mda_get_range(n: i32, span: &mut [i32], st: &[i32], endp: &[i32])
);

seam_core::seam!(
    /// `mda_get_prod(n, range, prod)` (arrayutils.c): products of dimension
    /// ranges for offset arithmetic. Writes into `prod`.
    pub fn mda_get_prod(n: i32, range: &[i32], prod: &mut [i32])
);

seam_core::seam!(
    /// `mda_get_offset_values(n, dist, prod, span)` (arrayutils.c): per-axis
    /// distance increments for stepping a sub-array. Writes into `dist`.
    pub fn mda_get_offset_values(n: i32, dist: &mut [i32], prod: &[i32], span: &[i32])
);

seam_core::seam!(
    /// `mda_next_tuple(n, curr, span)` (arrayutils.c): advance the subscript
    /// cursor `curr` to the next tuple within `span`; returns the highest
    /// changed dimension, or `-1` when the iteration is exhausted.
    pub fn mda_next_tuple(n: i32, curr: &mut [i32], span: &[i32]) -> i32
);

seam_core::seam!(
    /// `ArrayGetIntegerTypmods(arr, &n)` (arrayutils.c): decode a `cstring[]`
    /// typmod array varlena into its integer typmod list (each element parsed via
    /// `pg_strtoint32`). `ereport(ERROR)` if the array is not 1-D `cstring[]` or
    /// contains nulls. The element strings are deconstructed and parsed; the
    /// result is allocated in `mcx`.
    pub fn array_get_integer_typmods<'mcx>(
        mcx: Mcx<'mcx>,
        arr: &[u8],
    ) -> PgResult<PgVec<'mcx, i32>>
);
