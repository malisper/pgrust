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
    ///
    /// This is the LEGACY shape over the **opaque** [`portalcmds::Query`] token
    /// (its sole consumer is `backend-commands-portalcmds`'s `PerformCursorOpen`).
    /// It is documented K1/planner debt (DESIGN_DEBT TD-REWRITEHANDLER-RULELOCK):
    /// the opaque-token contract collapse onto the canonical
    /// [`types_nodes::copy_query::Query`] lands with the parser/planner value
    /// producers. New callers use [`query_rewrite_canonical`] below.
    pub fn query_rewrite<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query: Query,
    ) -> PgResult<mcx::PgVec<'mcx, Query>>
);

seam_core::seam!(
    /// `QueryRewrite(parsetree)` (rewriteHandler.c:4566) — the canonical top-level
    /// rule-rewriter entry over the value-typed
    /// [`types_nodes::copy_query::Query`]: apply all non-SELECT rules, then the
    /// RIR (`ON SELECT`/view) rules, then assign the command tag, returning the
    /// list of rewritten queries (each allocated in `mcx`). For a plain `SELECT`
    /// with no rules this is exactly one element (the input). This is the
    /// **additive** entry that the top-level callers (`tcop/postgres.c`
    /// `pg_rewrite_query`, SPI, `plancache.c` `RevalidateCachedQuery`) call as the
    /// parser/planner value-typed waves land; it is installed by the
    /// rewriteHandler owner. Runs the rewriter engine; can `ereport(ERROR)`.
    pub fn query_rewrite_canonical<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        parsetree: types_nodes::copy_query::Query<'mcx>,
    ) -> PgResult<mcx::PgVec<'mcx, types_nodes::copy_query::Query<'mcx>>>
);

seam_core::seam!(
    /// `AcquireRewriteLocks(parsetree, forExecute, forUpdatePushedDown)`
    /// (rewriteHandler.c:148) over the value-typed
    /// [`types_nodes::copy_query::Query`] — acquire the appropriate relation
    /// locks for every relation in the query, fix up dropped JOIN alias vars,
    /// and refresh RTE relkinds, recursing through subquery RTEs and CTEs. The
    /// `Query` is taken by value, mutated in place (locks taken, relkinds /
    /// joinaliasvars updated), and returned. `plancache.c`'s
    /// `RevalidateCachedQuery` analyzed branch calls this **standalone** (i.e.
    /// separately from `QueryRewrite`) to re-lock the cached analyzed querytree
    /// before re-planning. Runs the locker engine; can `ereport(ERROR)`. The
    /// sub-link descent remains the documented `'static`-SubLink keystone panic
    /// (rare on the SELECT/DML spine).
    pub fn acquire_rewrite_locks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        parsetree: types_nodes::copy_query::Query<'mcx>,
        for_execute: bool,
        for_update_pushed_down: bool,
    ) -> PgResult<types_nodes::copy_query::Query<'mcx>>
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
    /// rewrite-rule array; the ported owner installs this over the relcache
    /// `relation_rules` projection.
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
    /// Walks the view rewrite tree; can `ereport(ERROR)`.
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
