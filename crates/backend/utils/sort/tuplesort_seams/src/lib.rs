use ::datum::NullableDatum;
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    pub fn tuplesort_datums<'mcx>(
        mcx: Mcx<'mcx>,
        datum_type: Oid,
        sort_operator: Oid,
        collation: Oid,
        nulls_first: bool,
        work_mem: i32,
        values: &[NullableDatum],
    ) -> PgResult<PgVec<'mcx, NullableDatum>>
);
