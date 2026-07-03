use mcx::{Mcx, PgVec};
use types_error::PgResult;

// Owner: arrayfuncs (hosts ArrayGetIntegerTypmods, arrayutils.c). Seam
// because a direct dep from adt_numeric would cycle: arrayfuncs ->
// lsyscache -> format_type -> adt_numeric.

seam_core::seam!(
    pub fn array_get_integer_typmods<'mcx>(
        mcx: Mcx<'mcx>,
        arr: &[u8],
    ) -> PgResult<PgVec<'mcx, i32>>
);
