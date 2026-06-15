//! Seam declarations for the `backend-optimizer-plan-subselect-pullup` unit —
//! the `prepjointree`-facing half of `optimizer/plan/subselect.c`.
//!
//! These are the two SubLink → join conversions that
//! `pull_up_sublinks_qual_recurse` (in the still-unported `prepjointree.c`)
//! invokes. The owning unit installs them from its `init_seams()`; until the
//! caller lands they have no consumer, but they panic loudly if called before
//! installation (the seam-and-panic contract).
//!
//! ## Model
//!
//! The C calls take `PlannerInfo *root` and read/mutate `root->parse` (the top
//! `Query`). In this repo `PlannerInfo.parse` is a lifetime-free
//! [`QueryId`](types_pathnodes::QueryId) handle into the
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store. The caller
//! resolves the top `Query` (`run.resolve_mut(root.parse)`) and threads the
//! `&mut Query` plus the `&PlannerInfo` in directly, exactly as
//! [[plan-layer-route-to-159]] step 8 prescribes (walk the embedded owned
//! sub-`Query` by deref; the resolver is only for the top `root.parse`).
//!
//! The `available_rels` argument is C's `Relids` =
//! [`types_pathnodes::Relids`] (`Option<Box<Bitmapset>>`). The `mcx` argument
//! is the planner-run context the new RTEs / Vars / JoinExpr are allocated in.
//!
//! `convert_VALUES_to_ANY`, the SubPlan-building half (`make_subplan` /
//! `build_subplan`), and `convert_EXISTS_to_ANY` are NOT here: they need path
//! construction and are deferred to the planmain stage.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `convert_ANY_sublink_to_join(root, sublink, available_rels)`
    /// (subselect.c): try to convert a top-level `ANY` SubLink in a qual into a
    /// semijoin. On success returns a `JoinExpr` with `larg = NULL` (the caller
    /// fills it) and `rarg` = the pulled-up subquery `RangeTblRef`; the
    /// subselect is appended to `parse->rtable` as a side effect. Returns `None`
    /// if the SubLink cannot be converted. `Err` carries the walk/lookup
    /// `ereport(ERROR)` surface (`pull_varnos`, `contain_volatile_functions`).
    pub fn convert_ANY_sublink_to_join<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
        sublink: &types_nodes::rawexprnodes::SubLink<'mcx>,
        available_rels: &types_pathnodes::Relids,
    ) -> types_error::PgResult<Option<types_nodes::rawnodes::JoinExpr<'mcx>>>
);

seam_core::seam!(
    /// `convert_EXISTS_sublink_to_join(root, sublink, under_not, available_rels)`
    /// (subselect.c): try to convert a top-level `EXISTS` (or `NOT EXISTS`,
    /// via `under_not`) SubLink into a semijoin / antijoin. On success returns a
    /// `JoinExpr` with `larg = NULL`, `rarg` = the flattened subquery jointree
    /// and `quals` = the pulled-up WHERE clause; the subselect's rtable is
    /// merged into `parse->rtable` (`CombineRangeTables`). Returns `None` if the
    /// SubLink cannot be converted. `Err` carries the walk/lookup
    /// `ereport(ERROR)` surface.
    pub fn convert_EXISTS_sublink_to_join<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
        sublink: &types_nodes::rawexprnodes::SubLink<'mcx>,
        under_not: bool,
        available_rels: &types_pathnodes::Relids,
    ) -> types_error::PgResult<Option<types_nodes::rawnodes::JoinExpr<'mcx>>>
);
