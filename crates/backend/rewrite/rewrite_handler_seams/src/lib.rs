use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::parsenodes::Query;

seam_core::seam!(
    // QueryRewrite (rewrite/rewriteHandler.c); the query moves in (C scribbles
    // on it in place) and the rewritten list comes back arena-owned.
    pub fn query_rewrite<'mcx>(
        mcx: Mcx<'mcx>,
        query: Query<'mcx>,
    ) -> PgResult<PgVec<'mcx, Query<'mcx>>>
);
