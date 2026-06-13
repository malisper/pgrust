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

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodes::NodeTag;
use types_pathnodes::optimizer_plan::{
    CostSelector, JoinCostWorkspace, JoinPathExtraData, SemiAntiJoinFactors,
};
use types_pathnodes::{JoinType, PathId, PathKey, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};

/* ======================================================================
 * nodes/bitmapset.c — the `bms_difference` op the relnode seam set does not
 * expose (the rest reuse `backend-optimizer-util-relnode-seams`).
 * ==================================================================== */

seam_core::seam!(
    /// `bms_difference(a, b)` — fresh set `a - b`.
    pub fn bms_difference(a: &Relids, b: &Relids) -> Relids
);

/* ======================================================================
 * optimizer/util/pathnode.c — path constructors (alloc into the arena,
 * return the new `PathId`) + the add_path family.
 * ==================================================================== */

seam_core::seam!(
    /// `create_nestloop_path(...)` — allocate the nestloop `Path` into the arena.
    pub fn create_nestloop_path(
        root: &mut PlannerInfo,
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
    pub fn create_mergejoin_path(
        root: &mut PlannerInfo,
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
    pub fn create_hashjoin_path(
        root: &mut PlannerInfo,
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
    pub fn create_unique_path(
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
    /// `initial_cost_nestloop(...)`.
    pub fn initial_cost_nestloop(
        root: &PlannerInfo,
        jointype: JoinType,
        outer_path: PathId,
        inner_path: PathId,
        extra: &JoinPathExtraData,
    ) -> JoinCostWorkspace
);
seam_core::seam!(
    /// `initial_cost_mergejoin(...)`.
    pub fn initial_cost_mergejoin(
        root: &PlannerInfo,
        jointype: JoinType,
        mergeclauses: &[RinfoId],
        outer_path: PathId,
        inner_path: PathId,
        outersortkeys: &[PathKey],
        innersortkeys: &[PathKey],
        outer_presorted_keys: i32,
        extra: &JoinPathExtraData,
    ) -> JoinCostWorkspace
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
    pub fn compute_semi_anti_join_factors(
        root: &PlannerInfo,
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
    pub fn innerrel_is_unique(
        root: &mut PlannerInfo,
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

/* ======================================================================
 * utils/cache/lsyscache.c — get_commutator (syscache lookup; ereport-capable).
 * ==================================================================== */

seam_core::seam!(
    /// `get_commutator(opno)`.
    pub fn get_commutator(opno: Oid) -> PgResult<Oid>
);

/* ======================================================================
 * executor/execAmi.c — ExecMaterializesOutput(pathtype) over a NodeTag.
 * ==================================================================== */

seam_core::seam!(
    /// `ExecMaterializesOutput(plantype)`.
    pub fn exec_materializes_output(plantype: NodeTag) -> bool
);

/* ======================================================================
 * get_memoize_path bundle (clauses.c / nodeFuncs.c / typcache.c +
 * pathnode.c create_memoize_path). The whole cache-key analysis walks node
 * payloads and the type cache, so it crosses as one seam returning the new
 * Memoize `PathId` or `None`.
 * ==================================================================== */

seam_core::seam!(
    /// `get_memoize_path(...)` — if a Memoize node atop `inner_path` is possible,
    /// allocate it into the arena and return its handle; else `None`.
    pub fn get_memoize_path(
        root: &mut PlannerInfo,
        innerrel: RelId,
        outerrel: RelId,
        inner_path: PathId,
        outer_path: PathId,
        jointype: JoinType,
        extra: &JoinPathExtraData,
    ) -> PgResult<Option<PathId>>
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
