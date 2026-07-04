use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::parsenodes::Query;

seam_core::seam!(
    pub fn query_rewrite<'mcx>(
        mcx: Mcx<'mcx>,
        query: Query<'mcx>,
    ) -> PgResult<PgVec<'mcx, Query<'mcx>>>
);

seam_core::seam!(
    pub fn acquire_rewrite_locks<'mcx>(
        mcx: Mcx<'mcx>,
        query: &Query<'mcx>,
        for_execute: bool,
        for_update_pushed_down: bool,
    ) -> PgResult<()>
);
