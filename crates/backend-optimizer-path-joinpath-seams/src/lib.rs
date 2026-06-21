//! Seam declarations for the join-path enumerator (`optimizer/path/joinpath.c`),
//! arena-shaped over [`types_pathnodes::PlannerInfo`] (`RelId`/`PathId`/`RinfoId`
//! handles + the `rel()`/`path()`/`rinfo()` accessors).
//!
//! joinpath.c generates the candidate join paths for a pair of relations. The
//! enumeration *structure* (which join methods to consider for which clause
//! shapes) is the real crate body over the arena; everything it reaches across a
//! subsystem boundary crosses here. All path inputs are `PathId` arena handles
//! (the C `Path *`); the path constructors allocate into the arena and return
//! the new `PathId`. The `add_path` family mutates the joinrel through
//! `&mut PlannerInfo` + a `RelId`.
//!
//! **Failure surface.** Each seam mirrors the C function's failure surface
//! (AGENTS.md): a constructor / cost estimator / pathkey builder whose C path
//! `palloc`s (and so can `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`) returns
//! [`PgResult`]; pure predicates, scalar reads, and GUC getters return bare
//! values. Each seam defaults to a loud panic until the owning crate installs a
//! real implementation at single-threaded startup.
//!
//! Cross-crate owners: pathnode.c (`create_*_path`, `add_path*`,
//! `calc_*_required_outer`, `path_is_reparameterizable_by_child`), costsize.c
//! (`initial_cost_*`, `compute_semi_anti_join_factors`, `compare_path_costs`,
//! join-method enable GUCs), pathkeys.c / equivclass.c (pathkey
//! building/matching + eclass redundancy), joininfo.c (`innerrel_is_unique`),
//! restrictinfo.c (`clause_sides_match_join`), clauses.c/typcache.c (the bundled
//! memoize cache-key analysis), lsyscache.c (`get_commutator`), execAmi.c
//! (`ExecMaterializesOutput`), and the FDW + extension hooks.

extern crate alloc;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodes::NodeTag;
use types_pathnodes::optimizer_plan::{
    CostSelector, JoinCostWorkspace, JoinPathExtraData, SemiAntiJoinFactors,
};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinType, NodeId, PathId, PathKey, PhInfoId, PlannerInfo, RelId, Relids, RinfoId,
    SpecialJoinInfo,
};

/* ======================================================================
 * nodes/bitmapset.c — the `bms_difference` op the relnode seam set does not
 * expose (the rest reuse `backend-optimizer-util-relnode-seams`).
 * ==================================================================== */

seam_core::seam!(
    /// `bms_difference(a, b)` — fresh set `a - b`.
    pub fn bms_difference(a: &Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_equal(a, b)` — true iff the two sets have exactly the same members.
    pub fn bms_equal(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_membership(a) == BMS_MULTIPLE` — true iff `a` has two or more members.
    pub fn bms_membership_is_multiple(a: &Relids) -> bool
);

/* ======================================================================
 * optimizer/util/pathnode.c — path constructors (alloc into the arena,
 * return the new `PathId`) + the add_path family.
 * ==================================================================== */

seam_core::seam!(
    /// `create_nestloop_path(...)` — allocate the nestloop `Path` into the arena.
    pub fn create_nestloop_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        joinrel: RelId,
        jointype: JoinType,
        workspace: &JoinCostWorkspace,
        extra: &JoinPathExtraData,
        outer_path: PathId,
        inner_path: PathId,
        restrict_clauses: &[RinfoId],
        pathkeys: &[PathKey],
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_mergejoin_path(...)` — allocate the mergejoin `Path`.
    pub fn create_mergejoin_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        joinrel: RelId,
        jointype: JoinType,
        workspace: &JoinCostWorkspace,
        extra: &JoinPathExtraData,
        outer_path: PathId,
        inner_path: PathId,
        restrict_clauses: &[RinfoId],
        pathkeys: &[PathKey],
        required_outer: &Relids,
        mergeclauses: &[RinfoId],
        outersortkeys: &[PathKey],
        innersortkeys: &[PathKey],
        outer_presorted_keys: i32,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_hashjoin_path(...)` — allocate the hashjoin `Path`.
    pub fn create_hashjoin_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        joinrel: RelId,
        jointype: JoinType,
        workspace: &JoinCostWorkspace,
        extra: &JoinPathExtraData,
        outer_path: PathId,
        inner_path: PathId,
        parallel_hash: bool,
        restrict_clauses: &[RinfoId],
        required_outer: &Relids,
        hashclauses: &[RinfoId],
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_unique_path(root, rel, subpath, sjinfo)` — allocate the unique
    /// `Path`; `None` when unique-ification is impossible (C `NULL`).
    pub fn create_unique_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        sjinfo: &SpecialJoinInfo,
    ) -> PgResult<Option<PathId>>
);
seam_core::seam!(
    /// `create_material_path(rel, subpath)` — allocate the material `Path`.
    pub fn create_material_path(root: &mut PlannerInfo, rel: RelId, subpath: PathId) -> PgResult<PathId>
);
seam_core::seam!(
    /// `add_path(parent_rel, new_path)` — consider the new path for the
    /// joinrel's pathlist (may evict dominated paths). Allocates while inserting.
    pub fn add_path(root: &mut PlannerInfo, parent_rel: RelId, new_path: PathId) -> PgResult<()>
);
seam_core::seam!(
    /// `add_path_precheck(...)` — cheap dominated-path precheck.
    pub fn add_path_precheck(
        root: &PlannerInfo,
        parent_rel: RelId,
        disabled_nodes: i32,
        startup_cost: f64,
        total_cost: f64,
        pathkeys: &[PathKey],
        required_outer: &Relids,
    ) -> bool
);
seam_core::seam!(
    /// `add_partial_path(parent_rel, new_path)`.
    pub fn add_partial_path(root: &mut PlannerInfo, parent_rel: RelId, new_path: PathId) -> PgResult<()>
);
seam_core::seam!(
    /// `add_partial_path_precheck(...)`.
    pub fn add_partial_path_precheck(
        root: &PlannerInfo,
        parent_rel: RelId,
        disabled_nodes: i32,
        total_cost: f64,
        pathkeys: &[PathKey],
    ) -> bool
);
seam_core::seam!(
    /// `calc_nestloop_required_outer(...)`.
    pub fn calc_nestloop_required_outer(
        outerrelids: &Relids,
        outer_paramrels: &Relids,
        innerrelids: &Relids,
        inner_paramrels: &Relids,
    ) -> Relids
);
seam_core::seam!(
    /// `calc_non_nestloop_required_outer(outer_path, inner_path)`.
    pub fn calc_non_nestloop_required_outer(
        root: &PlannerInfo,
        outer_path: PathId,
        inner_path: PathId,
    ) -> Relids
);
seam_core::seam!(
    /// `path_is_reparameterizable_by_child(path, child_rel)`.
    pub fn path_is_reparameterizable_by_child(
        root: &PlannerInfo,
        path: PathId,
        child_rel: RelId,
    ) -> bool
);

/* ======================================================================
 * optimizer/path/costsize.c — preliminary cost estimators + cost GUCs.
 * ==================================================================== */

seam_core::seam!(
    /// `initial_cost_nestloop(...)`. Threads `run` + `&mut root` (the rescan
    /// cost path estimates the Memoize distinct-param count via
    /// `estimate_num_groups`, which examines exprs through the [`PlannerRun`]).
    pub fn initial_cost_nestloop<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        jointype: JoinType,
        outer_path: PathId,
        inner_path: PathId,
        extra: &JoinPathExtraData,
    ) -> types_error::PgResult<JoinCostWorkspace>
);
seam_core::seam!(
    /// `initial_cost_mergejoin(...)`. Threads `run` + `&mut root` (the
    /// incremental-sort source-cost path estimates group counts via
    /// `estimate_num_groups`).
    pub fn initial_cost_mergejoin<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        jointype: JoinType,
        mergeclauses: &[RinfoId],
        outer_path: PathId,
        inner_path: PathId,
        outersortkeys: &[PathKey],
        innersortkeys: &[PathKey],
        outer_presorted_keys: i32,
        extra: &JoinPathExtraData,
    ) -> types_error::PgResult<JoinCostWorkspace>
);
seam_core::seam!(
    /// `initial_cost_hashjoin(...)`.
    pub fn initial_cost_hashjoin(
        root: &PlannerInfo,
        jointype: JoinType,
        hashclauses: &[RinfoId],
        outer_path: PathId,
        inner_path: PathId,
        extra: &JoinPathExtraData,
        parallel_hash: bool,
    ) -> JoinCostWorkspace
);
seam_core::seam!(
    /// `compute_semi_anti_join_factors(...)`.
    pub fn compute_semi_anti_join_factors<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        joinrel: RelId,
        outerrel: RelId,
        innerrel: RelId,
        jointype: JoinType,
        sjinfo: &SpecialJoinInfo,
        restrictlist: &[RinfoId],
    ) -> SemiAntiJoinFactors
);
seam_core::seam!(
    /// `compare_path_costs(path1, path2, criterion)` — -1/0/1.
    pub fn compare_path_costs(
        root: &PlannerInfo,
        path1: PathId,
        path2: PathId,
        criterion: CostSelector,
    ) -> i32
);

// The join-method enable GUCs (`enable_mergejoin`/`enable_hashjoin`/
// `enable_material`/`enable_parallel_hash`/`enable_memoize`, `optimizer/cost.h`)
// are NOT seams: per the no-ambient-global-seams rule a per-backend GUC knob is
// passed as an explicit parameter, not modeled as a zero-arg getter. The
// enumerator takes them as a `JoinEnableFlags` value off the caller's planner
// facet (see the owning crate's public entry point).

/* ======================================================================
 * optimizer/path/pathkeys.c — ordering helpers (allocate pathkey/clause lists).
 * ==================================================================== */

seam_core::seam!(
    /// `build_join_pathkeys(root, joinrel, jointype, outer_pathkeys)`.
    pub fn build_join_pathkeys(
        root: &mut PlannerInfo,
        joinrel: RelId,
        jointype: JoinType,
        outer_pathkeys: &[PathKey],
    ) -> PgResult<alloc::vec::Vec<PathKey>>
);
seam_core::seam!(
    /// `find_mergeclauses_for_outer_pathkeys(root, pathkeys, restrictinfos)`.
    pub fn find_mergeclauses_for_outer_pathkeys(
        root: &mut PlannerInfo,
        pathkeys: &[PathKey],
        restrictinfos: &[RinfoId],
    ) -> PgResult<alloc::vec::Vec<RinfoId>>
);
seam_core::seam!(
    /// `select_outer_pathkeys_for_merge(root, mergeclauses, joinrel)`.
    pub fn select_outer_pathkeys_for_merge(
        root: &mut PlannerInfo,
        mergeclauses: &[RinfoId],
        joinrel: RelId,
    ) -> PgResult<alloc::vec::Vec<PathKey>>
);
seam_core::seam!(
    /// `make_inner_pathkeys_for_merge(root, mergeclauses, outer_pathkeys)`.
    pub fn make_inner_pathkeys_for_merge(
        root: &mut PlannerInfo,
        mergeclauses: &[RinfoId],
        outer_pathkeys: &[PathKey],
    ) -> PgResult<alloc::vec::Vec<PathKey>>
);
seam_core::seam!(
    /// `trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, pathkeys)`.
    pub fn trim_mergeclauses_for_inner_pathkeys(
        root: &mut PlannerInfo,
        mergeclauses: &[RinfoId],
        pathkeys: &[PathKey],
    ) -> PgResult<alloc::vec::Vec<RinfoId>>
);
seam_core::seam!(
    /// `pathkeys_contained_in(keys1, keys2)`.
    pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool
);
seam_core::seam!(
    /// `pathkeys_count_contained_in(keys1, keys2)` — `(contained, n_common)`.
    pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, i32)
);
seam_core::seam!(
    /// `update_mergeclause_eclasses(root, restrictinfo)` — fills
    /// `left_ec`/`right_ec` on the arena `RestrictInfo` in place.
    pub fn update_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: RinfoId) -> PgResult<()>
);
seam_core::seam!(
    /// `EC_MUST_BE_REDUNDANT(restrictinfo->left_ec)`.
    pub fn ec_must_be_redundant_left(root: &PlannerInfo, restrictinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `EC_MUST_BE_REDUNDANT(restrictinfo->right_ec)`.
    pub fn ec_must_be_redundant_right(root: &PlannerInfo, restrictinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `get_cheapest_path_for_pathkeys(...)`.
    pub fn get_cheapest_path_for_pathkeys(
        root: &PlannerInfo,
        paths: &[PathId],
        pathkeys: &[PathKey],
        required_outer: &Relids,
        cost_criterion: CostSelector,
        require_parallel_safe: bool,
    ) -> Option<PathId>
);
seam_core::seam!(
    /// `get_cheapest_parallel_safe_total_inner(paths)`.
    pub fn get_cheapest_parallel_safe_total_inner(
        root: &PlannerInfo,
        paths: &[PathId],
    ) -> Option<PathId>
);

/* ======================================================================
 * optimizer/util/joininfo.c — innerrel_is_unique.
 * ==================================================================== */

seam_core::seam!(
    /// `innerrel_is_unique(...)`.
    ///
    /// Threads the planner-run resolver (`run`): for a subquery innerrel the
    /// distinctness proof resolves the subquery `Query` from its RTE
    /// (`simple_rte_array[relid]` → `RangeTblEntryId` → `&RangeTblEntry`) and
    /// reads its `distinctClause`/`groupClause`/… via `&PlannerRun`.
    pub fn innerrel_is_unique<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        joinrelids: &Relids,
        outerrelids: &Relids,
        innerrel: RelId,
        jointype: JoinType,
        restrictlist: &[RinfoId],
        force_cache: bool,
    ) -> bool
);

/* ======================================================================
 * optimizer/util/restrictinfo.c — clause_sides_match_join (sets
 * `rinfo->outer_is_left` as a side effect, hence `&mut PlannerInfo`).
 * ==================================================================== */

seam_core::seam!(
    /// `clause_sides_match_join(rinfo, outerrelids, innerrelids)` — also sets
    /// `rinfo->outer_is_left`; returns whether the clause matches.
    pub fn clause_sides_match_join(
        root: &mut PlannerInfo,
        rinfo: RinfoId,
        outerrelids: &Relids,
        innerrelids: &Relids,
    ) -> bool
);

/* ======================================================================
 * RestrictInfo clause-payload reads: `IsA(clause, Const)` and
 * `castNode(OpExpr, clause)->opno`. The clause node lives in the optimizer
 * arena and is reached only by `RinfoId` handle from this crate.
 * ==================================================================== */

seam_core::seam!(
    /// `restrictinfo->clause && IsA(restrictinfo->clause, Const)`.
    pub fn clause_is_const(root: &PlannerInfo, rinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `castNode(OpExpr, restrictinfo->clause)->opno` — the operator OID of a
    /// hash/merge-joinable clause (known to be an `OpExpr`).
    pub fn clause_opexpr_opno(root: &PlannerInfo, rinfo: RinfoId) -> Oid
);
seam_core::seam!(
    /// `IsA(rinfo->clause, OpExpr) && list_length(opexpr->args) == 2` — the
    /// paraminfo cache-key form check.
    pub fn clause_is_opexpr_with_two_args(root: &PlannerInfo, rinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `list_nth(castNode(OpExpr, rinfo->clause)->args, n)` — the n-th argument
    /// expression of a 2-arg `OpExpr` clause, interned into the node arena and
    /// returned as a node handle (the owned-arena `OpExpr` stores its args as
    /// inline `Expr` values, so producing a handle requires `&mut` to intern).
    pub fn opexpr_arg<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, rinfo: RinfoId, n: i32) -> PgResult<NodeId>
);

/* ======================================================================
 * executor/execAmi.c — ExecMaterializesOutput(pathtype) over a NodeTag.
 * ==================================================================== */

seam_core::seam!(
    /// `ExecMaterializesOutput(plantype)`.
    pub fn exec_materializes_output(plantype: NodeTag) -> bool
);

/* ======================================================================
 * get_memoize_path callees. The memoize cache-key *orchestration* lives in
 * the joinpath crate (the C `get_memoize_path` /
 * `extract_lateral_vars_from_PHVs` / `paraminfo_get_equal_hashops`); only the
 * genuine cross-subsystem callees — node-payload walks (clauses.c / var.c /
 * placeholder.c / nodeFuncs.c / typcache.c) and the `create_memoize_path`
 * constructor (pathnode.c) — cross here, each as a thin marshal+delegate.
 * Expression nodes are `NodeId` handles into the optimizer/parse arena.
 * ==================================================================== */

seam_core::seam!(
    /// `contain_volatile_functions((Node *) expr)` (clauses.c) for an expr node.
    pub fn contain_volatile_functions_node(root: &PlannerInfo, node: NodeId) -> bool
);
seam_core::seam!(
    /// `contain_volatile_functions((Node *) reltarget)` (clauses.c) — the rel's
    /// PathTarget expression list.
    pub fn contain_volatile_functions_reltarget(root: &PlannerInfo, rel: RelId) -> bool
);
seam_core::seam!(
    /// `contain_volatile_functions((Node *) rinfo)` (clauses.c) — a RestrictInfo's
    /// clause tree.
    pub fn contain_volatile_functions_rinfo(root: &PlannerInfo, rinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `pull_varnos(root, (Node *) expr)` (var.c) — relids referenced by `expr`.
    pub fn pull_varnos(root: &PlannerInfo, node: NodeId) -> Relids
);
seam_core::seam!(
    /// `pull_vars_of_level((Node *) expr, 0)` (var.c) — the level-0 Vars/PHVs in
    /// `expr`, as a fresh list of node handles (allocates). A collected
    /// `PlaceHolderVar` is deep-copied into `mcx` (`copyObject` shape; its
    /// `'mcx`-tagged children would otherwise alias a freed transient), so `mcx`
    /// must outlive the returned handles' use.
    pub fn pull_vars_of_level<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, node: NodeId, levelsup: i32)
        -> PgResult<alloc::vec::Vec<NodeId>>
);
seam_core::seam!(
    /// `IsA(node, Var)` — true iff `node` is a `Var`.
    pub fn node_is_var(root: &PlannerInfo, node: NodeId) -> bool
);
seam_core::seam!(
    /// `IsA(node, PlaceHolderVar)` — true iff `node` is a `PlaceHolderVar`.
    pub fn node_is_placeholdervar(root: &PlannerInfo, node: NodeId) -> bool
);
seam_core::seam!(
    /// `((Var *) node)->varno` — the range-table index of a `Var` node.
    pub fn var_varno(root: &PlannerInfo, node: NodeId) -> i32
);
seam_core::seam!(
    /// `find_placeholder_info(root, (PlaceHolderVar *) node)` (placeholder.c) —
    /// the PlaceHolderInfo for a `PlaceHolderVar` node.
    pub fn find_placeholder_info(root: &mut PlannerInfo, node: NodeId) -> PhInfoId
);
seam_core::seam!(
    /// `lookup_type_cache(exprType((Node*) expr), TYPECACHE_HASH_PROC |
    /// TYPECACHE_EQ_OPR)` (nodeFuncs.c + typcache.c) — returns `Some(eq_opr)`
    /// when both `hash_proc` and `eq_opr` are valid OIDs, else `None`.
    pub fn expr_hash_eq_operator(root: &PlannerInfo, node: NodeId) -> Option<Oid>
);
seam_core::seam!(
    /// `create_memoize_path(...)` (pathnode.c) — allocate the Memoize `Path` atop
    /// `inner_path` and return its handle. `param_exprs` are the cache-key expr
    /// handles, `hash_operators` their hash equality operators (parallel lists).
    pub fn create_memoize_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        param_exprs: &[NodeId],
        hash_operators: &[Oid],
        singlerow: bool,
        binary_mode: bool,
        calls: f64,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * FDW join-pushdown hook + the set_join_pathlist_hook extension hook. Both
 * consult routine pointers not modelled here, so the whole invocation crosses;
 * they may append paths to the joinrel (and so may ereport).
 * ==================================================================== */

seam_core::seam!(
    /// `joinrel->fdwroutine->GetForeignJoinPaths(...)` if set; else no-op.
    pub fn fdw_get_foreign_join_paths(
        root: &mut PlannerInfo,
        joinrel: RelId,
        outerrel: RelId,
        innerrel: RelId,
        jointype: JoinType,
        extra: &JoinPathExtraData,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `set_join_pathlist_hook(...)` if installed; else no-op.
    pub fn set_join_pathlist_hook(
        root: &mut PlannerInfo,
        joinrel: RelId,
        outerrel: RelId,
        innerrel: RelId,
        jointype: JoinType,
        extra: &JoinPathExtraData,
    ) -> PgResult<()>
);

/* ======================================================================
 * NOTE: `add_paths_to_joinrel` is NOT a seam. The join-relation enumerator
 * (joinrels.c:populate_joinrel_with_paths) calls the crate-level
 * `backend_optimizer_path_joinpath::add_paths_to_joinrel` directly — joinrels
 * depends on joinpath (no cycle: joinpath does not depend on joinrels), and
 * threads the `Mcx` plus the `JoinEnableFlags` GUC snapshot itself. No inward
 * seam is needed for that entry point.
 * ==================================================================== */
