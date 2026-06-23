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
//! [`QueryId`](pathnodes::QueryId) handle into the
//! [`PlannerRun`](pathnodes::planner_run::PlannerRun) store. The caller
//! resolves the top `Query` (`run.resolve_mut(root.parse)`) and threads the
//! `&mut Query` plus the `&PlannerInfo` in directly, exactly as
//! [[plan-layer-route-to-159]] step 8 prescribes (walk the embedded owned
//! sub-`Query` by deref; the resolver is only for the top `root.parse`).
//!
//! The `available_rels` argument is C's `Relids` =
//! [`pathnodes::Relids`] (`Option<Box<Bitmapset>>`). The `mcx` argument
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
        root: &pathnodes::PlannerInfo,
        parse: &mut nodes::copy_query::Query<'mcx>,
        sublink: &nodes::primnodes::SubLink,
        available_rels: &pathnodes::Relids,
    ) -> types_error::PgResult<Option<nodes::rawnodes::JoinExpr<'mcx>>>
);

seam_core::seam!(
    /// `convert_VALUES_to_ANY(root, testexpr, values)` (subselect.c): the
    /// `pull_up_sublinks_qual_recurse` fast path that tries to rewrite
    /// `x op (VALUES (a), (b), ...)` (an `ANY` SubLink over a constant
    /// single-column VALUES list of ≥2 rows) directly into a
    /// `ScalarArrayOpExpr` (`x op ANY (ARRAY[...])`), avoiding a semijoin
    /// entirely. Returns `None` when the SubLink isn't a simplifiable VALUES
    /// sequence (the common case, where the caller then falls through to
    /// `convert_ANY_sublink_to_join`).
    ///
    /// **Deferred to the planmain stage** alongside `make_subplan` /
    /// `build_subplan` / `convert_EXISTS_to_ANY`: its body needs
    /// `convert_testexpr` + `eval_const_expressions` + `make_SAOP_expr`, which
    /// are part of the still-unported SubPlan-building half of subselect.c. The
    /// owner installs a seam-and-panic body until then; `pull_up_sublinks` (its
    /// only caller) is itself reachable only from the unported
    /// `subquery_planner`, so this panic is latent.
    ///
    /// C takes `(root, testexpr, values)` extracted from the `ANY` SubLink; here
    /// the whole `&SubLink` is handed in so the owner derefs its embedded-owned
    /// `subselect` / `testexpr` itself (matching the sibling conversions and
    /// avoiding a `'static`-subselect lifetime dance at the call site).
    pub fn convert_VALUES_to_ANY<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &pathnodes::PlannerInfo,
        sublink: &nodes::primnodes::SubLink<'mcx>,
    ) -> types_error::PgResult<Option<nodes::primnodes::Expr<'mcx>>>
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
        root: &pathnodes::PlannerInfo,
        parse: &mut nodes::copy_query::Query<'mcx>,
        sublink: &nodes::primnodes::SubLink,
        under_not: bool,
        available_rels: &pathnodes::Relids,
    ) -> types_error::PgResult<Option<nodes::rawnodes::JoinExpr<'mcx>>>
);
