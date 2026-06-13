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

seam_core::seam!(
    /// `build_column_default(rel, attrno)` (rewriteHandler.c): build the
    /// default-value expression tree for the 1-based column `attrno` of `rel`,
    /// or `None` (the C `NULL`) when the column has no default. For a generated
    /// column this is the GENERATED-AS expression. The result expression is
    /// allocated in `mcx`; reading the catalog default can `ereport(ERROR)`.
    pub fn build_column_default<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: types_rel::Relation<'mcx>,
        attrno: i32,
    ) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Expr>>>
);
