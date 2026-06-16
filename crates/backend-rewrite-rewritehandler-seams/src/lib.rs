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

seam_core::seam!(
    /// `get_view_query(view)` (rewriteHandler.c): return the `Query` from a
    /// view's `_RETURN` rule (the `ON SELECT` rewrite action). The C returns a
    /// read-only pointer into the relcache's `rd_rules`; this seam returns the
    /// canonical owned [`types_nodes::copy_query::Query`] image, allocated in
    /// `mcx`. The caller must have verified the relation is a view. C
    /// `elog(ERROR)`s on a missing/malformed `_RETURN` rule, carried on `Err`.
    ///
    /// rewriteHandler.c's `get_view_query` reads `view->rd_rules`, the relcache
    /// rewrite-rule array, which the ported `RelationData` does not yet model —
    /// so this stays a seam-and-panic until rewriteHandler's view-rule access
    /// lands. (Owner dir `backend-rewrite-rewritehandler` does not resolve to a
    /// crate, so the seam-install guard does not require it to be installed.)
    pub fn get_view_query<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        view: &types_rel::Relation<'mcx>,
    ) -> PgResult<types_nodes::copy_query::Query<'mcx>>
);

seam_core::seam!(
    /// `RelationHasSecurityInvoker(relation)` (`utils/rel.h`): whether the
    /// view's parsed reloptions have the `security_invoker` flag set (default
    /// `false` when no reloptions). The repo's `RelationData::rd_options` carries
    /// the *heap* `StdRdOptions`, which does not model the *view* `StdRdOptions`
    /// where `security_invoker` lives, so this stays a seam-and-panic until view
    /// reloptions are carried on the relcache entry. Homed alongside
    /// `get_view_query` (lockcmds reads both off the same opened view relcache
    /// handle); the owner dir `backend-rewrite-rewritehandler` does not resolve
    /// to a crate, so the seam-install guard does not require an installer.
    pub fn relation_has_security_invoker(relation: &types_rel::Relation<'_>) -> bool
);

seam_core::seam!(
    /// `relation_is_updatable(reloid, outer_reloids, include_triggers,
    /// include_cols)` (rewriteHandler.c): the bitmask of `CMD_*` events the
    /// relation supports for auto-updatable-view purposes. `include_cols` is
    /// `None` for the relation-level probe (C `NULL`) and `Some(col)` for the
    /// single-column probe (C `bms_make_singleton(col)`); `outer_reloids` is the
    /// C `NIL` recursion guard, always empty for the SQL-callable entry points.
    /// Walks the view rewrite tree; can `ereport(ERROR)`. Owner unported, so this
    /// panics until rewriteHandler lands.
    pub fn relation_is_updatable(
        reloid: types_core::Oid,
        include_triggers: bool,
        include_col: Option<i32>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `expand_generated_columns_in_expr(node, rel, rt_index)`
    /// (rewriteHandler.c:4494) — expression-level: replace references to STORED
    /// generated columns of `rel` (range-table index `rt_index`) within the
    /// expression with their generation expressions. Returns the (possibly
    /// rewritten) expression. Reached by `publicationcmds.c`
    /// `TransformPubWhereClauses`; the rewriteHandler.c owner installs it. `rel`
    /// is passed by OID (the seam re-opens / consults the relation).
    pub fn expand_generated_columns_in_expr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: Option<types_nodes::primnodes::Expr>,
        rel_oid: types_core::Oid,
        rt_index: i32,
    ) -> PgResult<Option<types_nodes::primnodes::Expr>>
);
