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
//! * `pull_up_subqueries` — **FAMILY 2, deferred** to FAMILY 5
//!   (`remove_useless_result_rtes`/`get_nullingrels`) and the
//!   `AppendRelInfo.translated_vars` walkable-carrier keystone (#274). Declared
//!   here as seam-and-panic; `subquery_planner` calls it.
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
    /// nulling refs (`remove_nulling_relids`). Mutates `parse` (jointree, PHVs,
    /// rowMarks) and `root.append_rel_list`. **FAMILY 5, ported now** (installed
    /// by the owner's `init_seams`). `Err` carries the bitmapset-allocation
    /// `ereport(ERROR)` surface.
    pub fn remove_useless_result_rtes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
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
