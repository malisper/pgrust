//! Outward seam declarations for the not-yet-ported (or not-acyclically-
//! reachable) externals that `optimizer/plan/initsplan.c` calls and that have
//! no existing seam home elsewhere:
//!
//!   * `lookup_type_cache_hasheq` (lookup_type_cache.c / typcache.c) — the
//!     `TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR` lookup used by
//!     `check_memoizable`; returns `(hash_proc, eq_opr)`.
//!   * `phinfo_add_needed` (placeholder.c) — add `where_needed` to a
//!     PlaceHolderVar's `PlaceHolderInfo::ph_needed`. The owner placeholder.c
//!     does not expose this (it is a static helper there); homed here so
//!     `add_vars_to_targetlist`/`add_vars_to_attr_needed` can update PHV needs.
//!   * `expand_inherited_rtentry` (inherit.c) — UNPORTED owner; only reached for
//!     inheritance parents from `add_other_rels_to_query`.
//!   * `preprocess_phv_expression` (subselect.c) — UNPORTED owner; only reached
//!     for LATERAL PlaceHolderVars with `phlevelsup > 0` in
//!     `extract_lateral_references`.
//!   * `increment_var_sublevels_up` (rewriteManip.c) over an owned arena `Expr`
//!     — used by `extract_lateral_references` for upper-level PHVs.
//!
//! Each call panics loudly until the real owner installs it ("mirror PG and
//! panic"). This crate has NO owner directory, so the
//! `every_declared_seam_is_installed_by_its_owner` guard skips it.

#![allow(non_snake_case)]

extern crate alloc;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, PlaceHolderVar};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, Relids};

seam_core::seam!(
    /// `lookup_type_cache(type, TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR)`
    /// (typcache.c) projected to `(hash_proc, eq_opr)` for `check_memoizable`.
    pub fn lookup_type_cache_hasheq(typid: Oid) -> (Oid, Oid)
);

seam_core::seam!(
    /// `find_placeholder_info(root, phv); phinfo->ph_needed = bms_add_members(
    /// phinfo->ph_needed, where_needed)` (placeholder.c / initsplan.c) — update
    /// a PlaceHolderVar's `ph_needed` set.
    pub fn phinfo_add_needed(root: &mut PlannerInfo, phv: &PlaceHolderVar, where_needed: &Relids) -> PgResult<()>
);

seam_core::seam!(
    /// `expand_inherited_rtentry(root, rel, rte, rti)` (inherit.c) — expand an
    /// inheritance/partition parent into its child "otherrels". UNPORTED owner.
    pub fn expand_inherited_rtentry(root: &mut PlannerInfo, rti: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `preprocess_phv_expression(root, expr)` (subselect.c) — run
    /// SS_process_sublinks / SS_replace_correlation_vars over an upper-level
    /// PlaceHolderVar's expression. UNPORTED owner; only reached for
    /// LATERAL PHVs with `phlevelsup > 0`.
    pub fn preprocess_phv_expression(root: &mut PlannerInfo, expr: Expr) -> PgResult<Expr>
);

seam_core::seam!(
    /// `IncrementVarSublevelsUp((Node *) expr, -((int) phlevelsup), 0)`
    /// (rewriteManip.c) over an owned arena `Expr`. Only reached for upper-level
    /// LATERAL PlaceHolderVars in `extract_lateral_references`.
    pub fn increment_var_sublevels_up_expr(expr: Expr, delta_sublevels_up: i32, min_sublevels_up: i32) -> PgResult<Expr>
);

seam_core::seam!(
    /// `pull_vars_of_level((Node *) node, levelsup)` (var.c) over a borrowed
    /// `Node` subtree. Used by `extract_lateral_references` to gather the
    /// level-`levelsup` Vars/PHVs referenced by a LATERAL RTE's
    /// `functions`/`tablefunc`/`values_lists`/`tablesample` parse subtrees
    /// (each a `Node *` in C). var.c is ported but its installed `&Expr`/`NodeId`
    /// var seams cannot name a whole parse `Node`; this per-`Node` seam is the
    /// home, loud-panic until var.c installs it.
    pub fn pull_vars_of_level_node<'mcx>(node: &Node<'mcx>, levelsup: i32) -> alloc::vec::Vec<Expr>
);

seam_core::seam!(
    /// `pull_vars_of_level((Node *) rte->subquery, levelsup)` (var.c) over a
    /// borrowed `Query`. The RTE_SUBQUERY arm of `extract_lateral_references`
    /// needs to walk the sub-`Query` (which enters one more query level); the
    /// owned `Query<'mcx>` is not a `Node` value and is not `Clone`, so it rides
    /// its own seam. Loud-panic until var.c installs it.
    pub fn pull_vars_of_level_query<'mcx>(query: &Query<'mcx>, levelsup: i32) -> alloc::vec::Vec<Expr>
);

seam_core::seam!(
    /// `add_nulling_relids((Node *) quals, target, added)` (rewriteManip.c) over
    /// an owned arena `Expr` (a single implicit-AND conjunct). The rewrite-core
    /// owner works over `&mut Node`, a model mismatch — homed here as a per-`Expr`
    /// seam. Only reached for outer-join clone quals in
    /// `deconstruct_distribute_oj_quals`.
    pub fn add_nulling_relids_expr(expr: Expr, target: Relids, added: Relids) -> Expr
);

seam_core::seam!(
    /// `eval_const_expressions(root, node)` (clauses.c) over an owned arena
    /// `Expr`. Used by `process_implied_equality` when both operands are
    /// pseudo-constant, to fold a derived `item1 op item2` clause to a boolean
    /// `Const` where possible. clauses.c is ported but works over `&Node`/`Mcx`;
    /// this per-`Expr` seam is the cycle break.
    pub fn eval_const_expressions_expr(root: &mut PlannerInfo, node: Expr) -> PgResult<Expr>
);

seam_core::seam!(
    /// `find_nonnullable_rels((Node *) expr)` (clauses.c) over an owned arena
    /// `Expr`. clauses.c is ported but works over `&Node`; this is the cycle
    /// break used by `distribute_qual_to_rels`/`expr_is_nonnullable` callers.
    pub fn find_nonnullable_rels_expr(expr: &Expr) -> Relids
);

seam_core::seam!(
    /// `find_forced_null_var((Node *) clause)` (clauses.c) over an owned arena
    /// `Expr`, returning the `Var` (as an owned `Expr::Var`) forced to NULL by an
    /// IS NULL test, or `None`. The cycle break for
    /// `check_redundant_nullability_qual`.
    pub fn find_forced_null_var_expr(clause: &Expr) -> Option<Expr>
);

seam_core::seam!(
    /// Project the per-RTE fields `add_base_clause_to_rel` reads from
    /// `root->simple_rte_array[rti]`: `(rtekind, rte->inh, rte->relkind)`.
    /// `PlannerInfo.simple_rte_array` is the opaque `RangeTblEntryId` handle, so
    /// these are resolved via this seam (see the .port-ref reachability note).
    /// `relkind` is the `char` pg_class relkind (`'p'` =
    /// `RELKIND_PARTITIONED_TABLE`).
    pub fn rte_kind_inh_relkind(root: &PlannerInfo, rti: i32) -> (i32, bool, i8)
);

/* ==========================================================================
 * subselect.c outward seams — the not-yet-ported planner externals that
 * `make_subplan`/`build_subplan`/`SS_process_ctes`/`finalize_plan` call.
 * Each panics loudly until its real owner installs it ("mirror PG and panic").
 * ======================================================================== */

seam_core::seam!(
    /// `subquery_planner(glob, subquery, parent_root, hasRecursion,
    /// tuple_fraction, NULL)` (planner.c) — recursively plan a SubLink's /
    /// CTE's sub-`Query`, returning the sub-`PlannerInfo` (subroot) plus the
    /// chosen final `Plan` tree and its source `Path`.
    ///
    /// This is THE one true subselect producer dependency: the C planner entry
    /// that turns an owned sub-`Query<'mcx>` into a planned subtree. Its owner
    /// (`subquery_planner`/`standard_planner` in planner.c) is unported, so the
    /// call is seamed here precisely (the carrier + interning is the
    /// subselect.c deliverable). The seam runs the whole lower planner —
    /// `subquery_planner` → `fetch_upper_rel(UPPERREL_FINAL)` →
    /// `get_cheapest_fractional_path` / `cheapest_total_path` → `create_plan` —
    /// and hands back the finished triple `(subroot, plan, path)` ready for
    /// `intern_subplan`. `path` is returned as the [`PathId`] in the returned
    /// subroot's path arena.
    ///
    /// `tuple_fraction` is the retrieval fraction; `has_recursion` mirrors the C
    /// `hasRecursion` flag. The returned `Node<'mcx>` embeds the subplan's
    /// `Plan` base.
    pub fn plan_sublink_subquery<'mcx>(
        root: &mut PlannerInfo,
        run: &mut PlannerRun<'mcx>,
        subquery: Query<'mcx>,
        has_recursion: bool,
        tuple_fraction: f64,
    ) -> PgResult<SublinkPlanResult<'mcx>>
);

seam_core::seam!(
    /// `cost_subplan(root, subplan, plan)` (costsize.c) — fill in a `SubPlan`'s
    /// `startup_cost` / `per_call_cost` from the child plan's costs and the
    /// `subLinkType`. Owner costsize.c does not yet expose this entry over the
    /// owned `SubPlan`/`Plan` model. Mutates `subplan` in place.
    pub fn cost_subplan<'a, 'mcx>(
        root: &PlannerInfo,
        subplan: &mut types_nodes::primnodes::SubPlan<'a>,
        plan: &Node<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `materialize_finished_plan(subplan)` (createplan.c) — wrap a finished
    /// `Plan` in a `Material` node (used by `build_subplan` for an
    /// uncorrelated non-init subplan when `enable_material` and the top node
    /// does not already materialize). Returns the new owned plan tree (a
    /// `Material` node whose `lefttree` is the input). Owner createplan.c does
    /// not yet expose this over the owned `Node` model.
    pub fn materialize_finished_plan<'mcx>(plan: Node<'mcx>) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `ExecMaterializesOutput(nodeTag(plan))` (execAmi.c) — true if the plan
    /// node type already materializes its output (so `build_subplan` need not
    /// add a `Material`). Pure node-tag classification; owner execAmi.c is
    /// unported.
    pub fn exec_materializes_output(tag: types_nodes::nodes::NodeTag) -> bool
);

seam_core::seam!(
    /// `(oprcanhash, oprcode)` of `pg_operator` row `opno`
    /// (`SearchSysCache1(OPEROID)` in `hash_ok_operator`). `OperRow` does not
    /// project `oprcanhash`, so this two-field projection is homed here.
    /// `Ok(None)` would be a cache miss; C `elog(ERROR)`s, so we return the
    /// pair directly and let an absent row be the seam owner's error.
    pub fn oper_canhash_code(opno: Oid) -> PgResult<(bool, Oid)>
);

seam_core::seam!(
    /// `find_minmax_agg_replacement_param(root, aggref)` (planagg.c) — if the
    /// `Aggref` will be replaced by a Param referencing a MIN/MAX-optimization
    /// initplan output during setrefs.c, return that Param's id; else `None`.
    /// Used by `finalize_primnode`'s Aggref arm. Owner planagg.c is unported.
    pub fn find_minmax_agg_replacement_param(
        root: &PlannerInfo,
        aggref: &types_nodes::primnodes::Aggref,
    ) -> Option<i32>
);

seam_core::seam!(
    /// `find_base_rel(root, scanrelid)->subroot->outer_params` (relnode.c +
    /// pathnodes.h) — for `finalize_plan`'s `T_SubqueryScan` arm: the
    /// sub-`PlannerInfo`'s `outer_params` set and a clone usable to recurse
    /// into the subplan with the sub-root. Returns the [`PlanId`]-keyed subroot
    /// plus its `outer_params` `Relids`. relnode.c is ported but does not yet
    /// expose `subroot` retrieval over this model.
    pub fn base_rel_subroot_outer_params(root: &PlannerInfo, scanrelid: i32) -> Relids
);

/// The triple a planned SubLink/CTE sub-`Query` yields from
/// [`plan_sublink_subquery`] — the analogue of C's
/// `subroot = subquery_planner(...); best_path = ...; plan = create_plan(...)`.
#[derive(Debug)]
pub struct SublinkPlanResult<'mcx> {
    /// The sub-query's `PlannerInfo` (C `subroot`). Lifetime-free arena root.
    pub subroot: PlannerInfo,
    /// The finished `Plan` tree (`create_plan(subroot, best_path)`).
    pub plan: Node<'mcx>,
    /// The chosen `Path`'s handle in `subroot`'s path arena (C `best_path`),
    /// or `None` for the dummy-path init case.
    pub subpath: Option<types_pathnodes::PathId>,
    /// The sub-query's interned `QueryId` (so the caller can read its
    /// `targetList` back through the run for `generate_subquery_params`).
    pub subquery_id: types_pathnodes::QueryId,
}
