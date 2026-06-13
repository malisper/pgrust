//! Seam declarations for the `backend-rewrite-rewriteHandler` unit
//! (`rewrite/rewriteHandler.c`): the rule rewriter portalcmds runs over the
//! analyzed cursor query.

use types_error::PgResult;
use types_nodes::portalcmds::Query;

seam_core::seam!(
    /// `QueryRewrite(query)` (rewriteHandler.c) — apply the rule system to the
    /// (already analyzed) query, returning the list of resulting `Query`s
    /// (allocated in `mcx`). For a `SELECT` this is exactly one element. Runs
    /// the rule rewriter; can `ereport(ERROR)`.
    pub fn query_rewrite<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query: Query,
    ) -> PgResult<mcx::PgVec<'mcx, Query>>
);
