//! Seam declarations for the `backend-optimizer-prep-prepjointree` unit
//! (`optimizer/prep/prepjointree.c`).
//!
//! prepjointree.c is the optimizer's jointree-preprocessing pass, called from
//! `subquery_planner` (planner.c). It is being ported family-by-family; this
//! seam crate declares the three top-level entry points `subquery_planner`
//! invokes, so the (still-unported) planner driver can be wired to them as each
//! family lands.
//!
//! ## Family status
//!
//! * `reduce_outer_joins` — **FAMILY 4, ported now** (installed by the owner's
//!   `init_seams`).
//! * `pull_up_sublinks` — **FAMILY 1, deferred** to the SubLink owned-Query
//!   carrier keystone (#273). Declared here as seam-and-panic until the keystone
//!   lands, because `subquery_planner` calls it.
//! * `pull_up_subqueries` — **FAMILY 2, ported now** (installed by the owner's
//!   `init_seams`), now that #273/#274 have landed; `subquery_planner` calls it.
//! * `preprocess_function_rtes` / `expand_virtual_generated_columns` —
//!   **FAMILY 6, ported now** (installed by the owner's `init_seams`);
//!   `subquery_planner` calls both. The SRF-inline (`inline_set_returning_function`,
//!   clauses.c) and the relcache/rewriter virtual-generated-column tlist
//!   (`build_virtual_generated_columns_tlist`) legs ride seams into their real
//!   owners (still unported), which seam-and-panic until they land.
//!
//! ## Model
//!
//! The C entry points take `PlannerInfo *root` and read/mutate `root->parse`
//! (the top `Query`), `root->append_rel_list`, and the planner-arena nodes. In
//! this repo `PlannerInfo` is lifetime-free; the top `Query` lives in the
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store behind the
//! `PlannerInfo.parse` [`QueryId`](types_pathnodes::QueryId) handle. The caller
//! resolves it (`run.resolve_mut(root.parse)`) and threads the `&mut Query`
//! alongside the `&mut PlannerInfo` (whose `append_rel_list` /
//! `node_arena` the pass also mutates) — the two are distinct objects, so no
//! aliasing conflict. `mcx` is the planner-run context new nodes allocate in.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `reduce_outer_joins(root)` (prepjointree.c): if the query contains outer
    /// joins, attempt to reduce them to plain inner joins (or a FULL join to a
    /// LEFT/RIGHT join) using strictness information from upper-level quals.
    /// Mutates `parse` (jointree `JoinExpr.jointype`s, `RangeTblEntry.jointype`s,
    /// and `Var`/`PlaceHolderVar` nullingrels via `remove_nulling_relids`) and
    /// `root.append_rel_list`. `Err` carries the `find_nonnullable_*` walk
    /// `ereport(ERROR)` surface. The planner only calls this when
    /// `parse->hasJoinRTEs` indicates outer joins are present.
    pub fn reduce_outer_joins<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pull_up_sublinks(root)` (prepjointree.c): replace top-level `ANY`/`EXISTS`
    /// SubLinks appearing in the query's WHERE/JOIN quals with semijoins /
    /// antijoins (via `convert_ANY_sublink_to_join` /
    /// `convert_EXISTS_sublink_to_join`). Mutates `parse`'s jointree. **FAMILY 1,
    /// deferred to the SubLink owned-Query carrier keystone (#273)** — the owner
    /// installs a seam-and-panic body until then.
    pub fn pull_up_sublinks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `remove_useless_result_rtes(root)` (prepjointree.c): remove `RTE_RESULT`
    /// RTEs from the join tree and elide single-child `FromExpr`s, fixing up
    /// `PlaceHolderVar` phrels (`substitute_phv_relids`) and dropped-outer-join
    /// nulling refs (`remove_nulling_relids`). Mutates `parse` (jointree, PHVs)
    /// and `root.append_rel_list`; also filters `root.rowMarks`, dropping any
    /// `PlanRowMark` whose `rti` resolves to an `RTE_RESULT` RTE. **FAMILY 5,
    /// ported now** (installed by the owner's `init_seams`). `Err` carries the
    /// bitmapset-allocation `ereport(ERROR)` surface.
    ///
    /// `rowmark_rtis` is the `rti` of each `root.rowMarks[i]`, resolved by the
    /// caller from the [`PlannerRun`](types_pathnodes::planner_run::PlannerRun)
    /// rowmark store (the crate's deliberate model: `parse` and the rowmark
    /// values are resolved out of the run by the planner driver and threaded in,
    /// so this owner never holds `run` itself). It is parallel to `root.rowMarks`
    /// (same length, same order); the C `foreach(cell, root->rowMarks)` reads
    /// `rc->rti` from each, which these values carry.
    pub fn remove_useless_result_rtes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
        rowmark_rtis: &[types_core::Index],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pull_up_subqueries(root)` (prepjointree.c): pull up simple subqueries,
    /// simple UNION ALLs, constant functions, and VALUES that appear as
    /// range-table entries of the query's jointree, flattening them into the
    /// parent. Mutates `parse` (jointree, rtable, targetList) and
    /// `root.append_rel_list`. **FAMILY 2, deferred to FAMILY 5 +
    /// the `AppendRelInfo.translated_vars` walkable-carrier keystone (#274)** —
    /// the owner installs a seam-and-panic body until then.
    pub fn pull_up_subqueries<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `preprocess_function_rtes(root)` (prepjointree.c:914): const-simplify every
    /// `RTE_FUNCTION`'s `functions` list (`eval_const_expressions`) and, where
    /// possible, inline a set-returning function into a subquery RTE
    /// (`inline_set_returning_function`). Mutates `parse.rtable`. **FAMILY 6,
    /// ported now** (installed by the owner's `init_seams`). `subquery_planner`
    /// calls this. `Err` carries the const-fold / inline `ereport(ERROR)`.
    pub fn preprocess_function_rtes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `expand_virtual_generated_columns(root)` (prepjointree.c:969): scan the
    /// rangetable for relations with virtual generated columns and replace all
    /// `Var` nodes that reference such columns with the generation expressions.
    /// Returns a (possibly) modified `Query` (the C returns `parse`, replaced
    /// wholesale by `pullup_replace_vars` if any expansion happened). **FAMILY 6,
    /// ported now** (installed by the owner's `init_seams`). `subquery_planner`
    /// calls this. `Err` carries the relcache / replace `ereport(ERROR)`.
    pub fn expand_virtual_generated_columns<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<types_nodes::copy_query::Query<'mcx>>
);

seam_core::seam!(
    /// The relcache + rewriter leg of `expand_virtual_generated_columns`
    /// (prepjointree.c:993-1023): for the `RTE_RELATION` at one-based `rt_index`
    /// with OID `relid`, `table_open` it, read its `TupleDesc`, and — only if
    /// `tupdesc->constr->has_generated_virtual` — build the per-attribute
    /// targetlist of `build_generation_expression(rel, attno)` (run through
    /// `ChangeVarNodes` for generated attrs) / `makeVar` (for normal attrs),
    /// returning `Ok(Some(tlist))`. Returns `Ok(None)` when the relation has no
    /// virtual generated columns (the common no-op path). **Owner: the
    /// relcache + rewriteHandler leg (`table_open` / `RelationGetDescr` /
    /// `build_generation_expression`); seam-and-panics until it lands** — the
    /// `build_generation_expression` rewriter is unported, and the bare loop in
    /// the owner cannot prove the no-op without opening the relation.
    pub fn build_virtual_generated_columns_tlist<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        relid: types_core::primitive::Oid,
        rt_index: i32,
    ) -> types_error::PgResult<
        Option<mcx::PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>>,
    >
);

seam_core::seam!(
    /// `flatten_simple_union_all(root)` (prepjointree.c): if the query's
    /// `setOperations` tree consists entirely of simple UNION ALL operations,
    /// flatten it into an append relation; otherwise do nothing. Mutates `parse`
    /// (rtable, jointree, setOperations) and `root.append_rel_list`. **FAMILY 3,
    /// ported** (installed by the owner's `init_seams`).
    pub fn flatten_simple_union_all<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_relids_for_join(query, joinrelid)` (prepjointree.c:4191): find the
    /// jointree node for the join RTE `joinrelid` (`find_jointree_node_for_rel`)
    /// and return the set of base+OJ relids present underneath it
    /// (`get_relids_in_jointree(jtnode, true, false)`). Errors with
    /// `elog(ERROR, "could not find join node %d")` if the join RTE is not in the
    /// jointree. Consumed by `alias_relid_set` / `add_nullingrels_if_needed`
    /// (optimizer/util/var.c, `flatten_join_alias_vars`). The result is returned
    /// as the lifetime-free [`ExprRelids`] word storage so the var.c owner can
    /// assign it directly into a `PlaceHolderVar.phrels`. Installed by the owner's
    /// `init_seams`.
    pub fn get_relids_for_join<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query: &types_nodes::copy_query::Query<'mcx>,
        joinrelid: i32,
    ) -> types_error::PgResult<types_nodes::primnodes::ExprRelids>
);

seam_core::seam!(
    /// `get_relids_in_jointree((Node *) query->jointree, true, false)`
    /// (prepjointree.c:4234), applied to the whole query jointree: the set of
    /// base+OJ RT indexes present in the query. Consumed by
    /// `mark_nullable_by_grouping` (optimizer/util/var.c) to compute the
    /// syntactic `phrels` of a PlaceHolderVar wrapping a variable-free grouping
    /// expression. Returned as the lifetime-free [`ExprRelids`] word storage.
    /// Installed by the owner's `init_seams`.
    pub fn get_relids_in_query_jointree<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query: &types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<types_nodes::primnodes::ExprRelids>
);
