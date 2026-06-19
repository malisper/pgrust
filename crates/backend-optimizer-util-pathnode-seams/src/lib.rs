//! Inward seam declarations owned by `optimizer/util/pathnode.c`, arena-shaped
//! over [`types_pathnodes::PlannerInfo`] (`RelId`/`PathId`/`RinfoId`/`EcId`
//! handles + the `rel()`/`path()`/`rinfo()` accessors).
//!
//! pathnode.c is the planner's path cost-domination engine and the
//! `create_*_path` factory family. Every other optimizer crate (allpaths.c,
//! indxpath.c, planner.c, createplan.c, geqo, …) reaches a path constructor or
//! the `add_path`/`set_cheapest` machinery through these seams; the join-path
//! enumerator's pathnode seams live in `backend-optimizer-path-joinpath-seams`
//! (this crate declares the *rest* of the public `create_*_path` surface so the
//! other callers have a contract to bind to). The owning crate
//! (`backend-optimizer-util-pathnode`) installs every seam from its
//! `init_seams()` at single-threaded startup; until then a call panics loudly.
//!
//! **Failure surface.** A constructor / cost-bearing routine whose C path
//! `palloc`s (and so can `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`) returns
//! [`PgResult`]; pure predicates / scalar reads return bare values.
//!
//! Every path input is a [`PathId`] arena handle (the C `Path *`); the
//! constructors allocate into the arena and return the new `PathId`. Node-payload
//! lists (tlists, quals, group clauses, …) are carried as `NodeId` handles into
//! the optimizer/parse arena, matching the `types_pathnodes` path-subtype field
//! model.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::{AttrNumber, Cost, Index, Oid};
use types_error::PgResult;
use types_pathnodes::optimizer_plan::CostSelector;
use types_pathnodes::{
    AggSplit, AggStrategy, CmdType, IndexClause, IndexOptInfo, LimitOption, NodeId, PathId, PathKey,
    PathTarget, PlannerInfo, RelId, Relids, RinfoId, ScanDirection, SetOpCmd, SetOpStrategy,
    SpecialJoinInfo,
};

/// `AggClauseCosts` (nodes/pathnodes.h), trimmed to the field pathnode.c reads
/// directly (`transitionSpace`) — the full struct lives with the agg-cost
/// producer (clauses.c/cost.c). pathnode passes the value straight through to the
/// `cost_agg` seam; only `transitionSpace` is consumed in-crate.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AggClauseCostsLite {
    /// `Cost transCost.startup`.
    pub trans_startup: Cost,
    /// `Cost transCost.per_tuple`.
    pub trans_per_tuple: Cost,
    /// `Cost finalCost.startup`.
    pub final_startup: Cost,
    /// `Cost finalCost.per_tuple`.
    pub final_per_tuple: Cost,
    /// `int transitionSpace` — estimated size of the transition state.
    pub transition_space: i32,
}

/* ======================================================================
 * MISC. PATH UTILITIES — comparison + cheapest selection.
 * ==================================================================== */

seam_core::seam!(
    /// `compare_path_costs(path1, path2, criterion)` (pathnode.c:71) — -1/0/+1
    /// per whether `path1` is cheaper/equal/more-expensive than `path2`.
    pub fn compare_path_costs(
        root: &PlannerInfo,
        path1: PathId,
        path2: PathId,
        criterion: CostSelector,
    ) -> i32
);
seam_core::seam!(
    /// `compare_fractional_path_costs(path1, path2, fraction)` (pathnode.c:126).
    pub fn compare_fractional_path_costs(
        root: &PlannerInfo,
        path1: PathId,
        path2: PathId,
        fraction: f64,
    ) -> i32
);
seam_core::seam!(
    /// `set_cheapest(parent_rel)` (pathnode.c:271).
    pub fn set_cheapest(root: &mut PlannerInfo, parent_rel: RelId) -> PgResult<()>
);

/* ======================================================================
 * add_path family (pathnode.c:463-967).
 * ==================================================================== */

seam_core::seam!(
    /// `add_path(parent_rel, new_path)` (pathnode.c:463) — consider `new_path`
    /// (an already-allocated arena `PathId`) for the rel's pathlist.
    pub fn add_path(root: &mut PlannerInfo, parent_rel: RelId, new_path: PathId) -> PgResult<()>
);
seam_core::seam!(
    /// `add_path_precheck(...)` (pathnode.c:690).
    pub fn add_path_precheck(
        root: &PlannerInfo,
        parent_rel: RelId,
        disabled_nodes: i32,
        startup_cost: Cost,
        total_cost: Cost,
        pathkeys: &[PathKey],
        required_outer: &Relids,
    ) -> bool
);
seam_core::seam!(
    /// `add_partial_path(parent_rel, new_path)` (pathnode.c:797).
    pub fn add_partial_path(
        root: &mut PlannerInfo,
        parent_rel: RelId,
        new_path: PathId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `add_partial_path_precheck(...)` (pathnode.c:923).
    pub fn add_partial_path_precheck(
        root: &PlannerInfo,
        parent_rel: RelId,
        disabled_nodes: i32,
        total_cost: Cost,
        pathkeys: &[PathKey],
    ) -> bool
);

/* ======================================================================
 * Scan-path constructors (pathnode.c:985-2429).
 * ==================================================================== */

seam_core::seam!(
    /// `create_seqscan_path(root, rel, required_outer, parallel_workers)`.
    pub fn create_seqscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
        parallel_workers: i32,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_samplescan_path(root, rel, required_outer)`.
    pub fn create_samplescan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_index_path(...)` (pathnode.c:1051).
    pub fn create_index_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        index: Box<IndexOptInfo>,
        indexclauses: Vec<IndexClause>,
        indexorderbys: Vec<NodeId>,
        indexorderbycols: Vec<i32>,
        pathkeys: Vec<PathKey>,
        indexscandir: ScanDirection,
        indexonly: bool,
        required_outer: &Relids,
        loop_count: f64,
        partial_path: bool,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_bitmap_heap_path(...)` (pathnode.c:1100).
    pub fn create_bitmap_heap_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        bitmapqual: PathId,
        required_outer: &Relids,
        loop_count: f64,
        parallel_degree: i32,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_bitmap_and_path(root, rel, bitmapquals)` (pathnode.c:1133).
    pub fn create_bitmap_and_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        bitmapquals: Vec<PathId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_bitmap_or_path(root, rel, bitmapquals)` (pathnode.c:1185).
    pub fn create_bitmap_or_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        bitmapquals: Vec<PathId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_tidscan_path(root, rel, tidquals, required_outer)`.
    pub fn create_tidscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        tidquals: Vec<NodeId>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_tidrangescan_path(root, rel, tidrangequals, required_outer)`.
    pub fn create_tidrangescan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        tidrangequals: Vec<NodeId>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `import_path_from_subroot(root, subroot, sub_path_id)` — deep-copy a
    /// subroot path and its whole subtree into `root`'s arenas, remapping every
    /// `PathId`/`RelId`/`RinfoId`/`NodeId` handle, returning a `root`-arena
    /// `PathId`. The cross-root path-tree import primitive set-op planning and
    /// `set_subquery_pathlist` use to feed a subroot's final-rel paths into
    /// `create_subqueryscan_path(ROOT, …)`. Lives in the concrete pathnode unit;
    /// crossed as a seam so consumers (allpaths) need not take a concrete dep.
    pub fn import_path_from_subroot<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        subroot: &PlannerInfo,
        sub_path_id: PathId,
    ) -> PathId
);
seam_core::seam!(
    /// `create_subqueryscan_path(...)` (pathnode.c:2223).
    pub fn create_subqueryscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        subpath: PathId,
        subroot_subpath: Option<PathId>,
        trivial_pathtarget: bool,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_functionscan_path(root, rel, pathkeys, required_outer)`.
    pub fn create_functionscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_tablefuncscan_path(root, rel, required_outer)`.
    pub fn create_tablefuncscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_valuesscan_path(root, rel, required_outer)`.
    pub fn create_valuesscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_ctescan_path(root, rel, pathkeys, required_outer)`.
    pub fn create_ctescan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_namedtuplestorescan_path(root, rel, required_outer)`.
    pub fn create_namedtuplestorescan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_resultscan_path(root, rel, required_outer)`.
    pub fn create_resultscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_worktablescan_path(root, rel, required_outer)`.
    pub fn create_worktablescan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * Append / MergeAppend (pathnode.c:1302-1579).
 * ==================================================================== */

seam_core::seam!(
    /// `create_append_path(...)` (pathnode.c:1302). `have_root=false` is the C
    /// `root == NULL` dummy-path case; `rows < 0` means "compute from subpaths".
    pub fn create_append_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        have_root: bool,
        rel: RelId,
        subpaths: Vec<PathId>,
        partial_subpaths: Vec<PathId>,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
        parallel_workers: i32,
        parallel_aware: bool,
        rows: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_merge_append_path(...)` (pathnode.c:1473).
    pub fn create_merge_append_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpaths: Vec<PathId>,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * GroupResult / Material / Memoize / Gather (pathnode.c:1588-2211).
 * ==================================================================== */

seam_core::seam!(
    /// `create_group_result_path(root, rel, target, havingqual)`.
    pub fn create_group_result_path(
        root: &mut PlannerInfo,
        rel: RelId,
        target: Box<PathTarget>,
        havingqual: Vec<NodeId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_material_path(rel, subpath)` (pathnode.c:1636).
    pub fn create_material_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_memoize_path(...)` (pathnode.c:1669).
    pub fn create_memoize_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        param_exprs: Vec<NodeId>,
        hash_operators: Vec<Oid>,
        singlerow: bool,
        binary_mode: bool,
        calls: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_gather_merge_path(...)` (pathnode.c:2097).
    pub fn create_gather_merge_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        subpath: PathId,
        target: Option<Box<PathTarget>>,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
        rows: Option<f64>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_gather_path(...)` (pathnode.c:2179).
    pub fn create_gather_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        subpath: PathId,
        target: Option<Box<PathTarget>>,
        required_outer: &Relids,
        rows: Option<f64>,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * Foreign paths (pathnode.c:2442-2580).
 * ==================================================================== */

seam_core::seam!(
    /// `create_foreignscan_path(...)` (pathnode.c:2442).
    pub fn create_foreignscan_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        target: Option<Box<PathTarget>>,
        rows: f64,
        disabled_nodes: i32,
        startup_cost: Cost,
        total_cost: Cost,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
        fdw_outerpath: Option<PathId>,
        fdw_restrictinfo: Vec<RinfoId>,
        fdw_private: Vec<NodeId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_foreign_join_path(...)` (pathnode.c:2490).
    pub fn create_foreign_join_path(
        root: &mut PlannerInfo,
        rel: RelId,
        target: Option<Box<PathTarget>>,
        rows: f64,
        disabled_nodes: i32,
        startup_cost: Cost,
        total_cost: Cost,
        pathkeys: Vec<PathKey>,
        required_outer: &Relids,
        fdw_outerpath: Option<PathId>,
        fdw_restrictinfo: Vec<RinfoId>,
        fdw_private: Vec<NodeId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_foreign_upper_path(...)` (pathnode.c:2544).
    pub fn create_foreign_upper_path(
        root: &mut PlannerInfo,
        rel: RelId,
        target: Option<Box<PathTarget>>,
        rows: f64,
        disabled_nodes: i32,
        startup_cost: Cost,
        total_cost: Cost,
        pathkeys: Vec<PathKey>,
        fdw_outerpath: Option<PathId>,
        fdw_restrictinfo: Vec<RinfoId>,
        fdw_private: Vec<NodeId>,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * Join required_outer helpers + join-path constructors (pathnode.c:2591-2891).
 * ==================================================================== */

seam_core::seam!(
    /// `calc_nestloop_required_outer(...)` (pathnode.c:2591).
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

/* ======================================================================
 * Upper-rel path constructors (pathnode.c:2901-4239).
 * ==================================================================== */

seam_core::seam!(
    /// `create_projection_path(root, rel, subpath, target)` (pathnode.c:2901).
    pub fn create_projection_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        target: Box<PathTarget>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `apply_projection_to_path(root, rel, path, target)` (pathnode.c:3011).
    pub fn apply_projection_to_path(
        root: &mut PlannerInfo,
        rel: RelId,
        path: PathId,
        target: Box<PathTarget>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_set_projection_path(root, rel, subpath, target)`.
    pub fn create_set_projection_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        target: Box<PathTarget>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_incremental_sort_path(...)` (pathnode.c:3170).
    pub fn create_incremental_sort_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
        subpath: PathId,
        pathkeys: Vec<PathKey>,
        presorted_keys: i32,
        limit_tuples: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_sort_path(root, rel, subpath, pathkeys, limit_tuples)`.
    pub fn create_sort_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        pathkeys: Vec<PathKey>,
        limit_tuples: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_group_path(...)` (pathnode.c:3265).
    pub fn create_group_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        group_clause: Vec<NodeId>,
        qual: Vec<NodeId>,
        num_groups: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_upper_unique_path(...)` (pathnode.c:3325).
    pub fn create_upper_unique_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        num_cols: i32,
        num_groups: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_agg_path(...)` (pathnode.c:3378).
    pub fn create_agg_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        target: Box<PathTarget>,
        aggstrategy: AggStrategy,
        aggsplit: AggSplit,
        group_clause: Vec<NodeId>,
        qual: Vec<NodeId>,
        aggcosts: Option<AggClauseCostsLite>,
        num_groups: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_minmaxagg_path(...)` (pathnode.c:3624).
    pub fn create_minmaxagg_path(
        root: &mut PlannerInfo,
        rel: RelId,
        target: Box<PathTarget>,
        mmaggregates: Vec<types_pathnodes::MinMaxAggInfo>,
        quals: Vec<NodeId>,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_windowagg_path(...)` (pathnode.c:3715).
    pub fn create_windowagg_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        target: Box<PathTarget>,
        window_funcs: Vec<NodeId>,
        run_condition: Vec<NodeId>,
        winclause: NodeId,
        qual: Vec<NodeId>,
        topwindow: bool,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_setop_path(...)` (pathnode.c:3788).
    pub fn create_setop_path(
        root: &mut PlannerInfo,
        rel: RelId,
        leftpath: PathId,
        rightpath: PathId,
        cmd: SetOpCmd,
        strategy: SetOpStrategy,
        group_list: Vec<NodeId>,
        num_groups: f64,
        output_rows: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_recursiveunion_path(...)` (pathnode.c:3906).
    pub fn create_recursiveunion_path(
        root: &mut PlannerInfo,
        rel: RelId,
        leftpath: PathId,
        rightpath: PathId,
        target: Box<PathTarget>,
        distinct_list: Vec<NodeId>,
        wt_param: i32,
        num_groups: f64,
    ) -> PgResult<PathId>
);
seam_core::seam!(
    /// `create_lockrows_path(...)` (pathnode.c:3951).
    pub fn create_lockrows_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        row_marks: Vec<NodeId>,
        epq_param: i32,
    ) -> PgResult<PathId>
);
#[allow(clippy::type_complexity)]
mod modifytable {
    use super::*;
    seam_core::seam!(
        /// `create_modifytable_path(...)` (pathnode.c:4015).
        pub fn create_modifytable_path(
            root: &mut PlannerInfo,
            rel: RelId,
            subpath: PathId,
            operation: CmdType,
            can_set_tag: bool,
            nominal_relation: Index,
            root_relation: Index,
            part_cols_updated: bool,
            result_relations: Vec<i32>,
            update_colnos_lists: Vec<Vec<AttrNumber>>,
            with_check_option_lists: Vec<Vec<NodeId>>,
            returning_lists: Vec<Vec<NodeId>>,
            row_marks: Vec<NodeId>,
            onconflict: Option<NodeId>,
            merge_action_lists: Vec<Vec<NodeId>>,
            merge_join_conditions: Vec<Vec<NodeId>>,
            epq_param: i32,
        ) -> PgResult<PathId>
    );
}
pub use modifytable::create_modifytable_path;

seam_core::seam!(
    /// `create_limit_path(...)` (pathnode.c:4117).
    pub fn create_limit_path(
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        limit_offset: Option<NodeId>,
        limit_count: Option<NodeId>,
        limit_option: LimitOption,
        offset_est: i64,
        count_est: i64,
    ) -> PgResult<PathId>
);

/* ======================================================================
 * Unique / reparameterization (pathnode.c:1730 + 4242-4884).
 * ==================================================================== */

seam_core::seam!(
    /// `create_unique_path(root, rel, subpath, sjinfo)` (pathnode.c:1730).
    pub fn create_unique_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        subpath: PathId,
        sjinfo: &SpecialJoinInfo,
    ) -> PgResult<Option<PathId>>
);
seam_core::seam!(
    /// `reparameterize_path(root, path, required_outer, loop_count)`
    /// (pathnode.c:4242).
    pub fn reparameterize_path(
        root: &mut PlannerInfo,
        path: PathId,
        required_outer: &Relids,
        loop_count: f64,
    ) -> PgResult<Option<PathId>>
);

/* ----------------------------------------------------------------------
 * Cross-subsystem helpers `create_unique_path` (pathnode.c:1730) consumes.
 *
 * pathnode.c is below pathkeys.c / indxpath.c / analyzejoins.c / selfuncs.c
 * in the build DAG, so it cannot name those crates directly. These outward
 * seams give it a contract; the owning unit installs the body from its
 * `init_seams()`. (lsyscache's `get_ordering_op_for_equality_op` /
 * `get_equality_op_for_ordering_op` are *below* pathnode and reached via the
 * lsyscache-seams crate directly, so they are not redeclared here.)
 * -------------------------------------------------------------------- */

seam_core::seam!(
    /// `make_pathkeys_for_sortclauses(root, sortclauses, tlist)` (pathkeys.c).
    /// Returns the canonical pathkey list for the given (arena `NodeId`)
    /// SortGroupClause / TargetEntry lists. Installed by pathkeys.c.
    pub fn make_pathkeys_for_sortclauses(
        root: &mut PlannerInfo,
        sortclauses: &[NodeId],
        tlist: &[NodeId],
    ) -> Vec<PathKey>
);
seam_core::seam!(
    /// `relation_has_unique_index_for(root, rel, NIL, exprlist, oprlist)`
    /// (plancat.c via indxpath.c). True when the relation has a unique index that
    /// proves `exprlist` is unique under `oprlist`. Installed by indxpath.c.
    pub fn relation_has_unique_index_for(
        root: &mut PlannerInfo,
        rel: RelId,
        exprlist: &[NodeId],
        oprlist: &[Oid],
    ) -> bool
);
seam_core::seam!(
    /// The `RTE_SUBQUERY` distinctness probe from `create_unique_path`
    /// (pathnode.c:1959): `query_supports_distinctness(rte->subquery)` &&
    /// `translate_sub_tlist(uniq_exprs, rel->relid)` non-NIL &&
    /// `query_is_distinct_for(rte->subquery, colnos, in_operators)`. Installed by
    /// analyzejoins.c (which owns the `Query`-distinctness machinery + the
    /// `PlannerRun` sub-`Query` resolver). `uniq_exprs` are arena `Var` handles.
    pub fn subquery_is_distinct_for<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
        uniq_exprs: &[NodeId],
        in_operators: &[Oid],
    ) -> bool
);
seam_core::seam!(
    /// `estimate_num_groups(root, group_exprs, input_rows, NULL, NULL)`
    /// (selfuncs.c). Estimates the number of distinct groups. Installed by
    /// selfuncs.c.
    pub fn estimate_num_groups_simple<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        group_exprs: &[NodeId],
        input_rows: f64,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `reparameterize_path_by_child(root, path, child_rel)` (pathnode.c:4408).
    pub fn reparameterize_path_by_child(
        root: &mut PlannerInfo,
        path: PathId,
        child_rel: RelId,
    ) -> PgResult<Option<PathId>>
);
seam_core::seam!(
    /// `path_is_reparameterizable_by_child(path, child_rel)` (pathnode.c:4704).
    pub fn path_is_reparameterizable_by_child(
        root: &PlannerInfo,
        path: PathId,
        child_rel: RelId,
    ) -> bool
);

/* ======================================================================
 * OUTWARD seams — cross-subsystem callees pathnode.c reaches that have no
 * existing seam-crate decl in this repo. Each panics until its owner lands.
 *
 *  - costsize.c: the cost_X / final_cost_X estimators + cost GUC getters. These
 *    take the new path by its arena `PathId` and write its base `Path`
 *    cost/row fields in place (the C cost_X(path, root, ...) that fills
 *    `path->startup_cost`/`total_cost`/`rows`/`disabled_nodes`).
 *  - relnode.c: get_{baserel,appendrel,joinrel}_parampathinfo +
 *    get_param_path_clause_serials (param_info construction).
 *  - clauses.c/tlist.c/createplan.c: expression walks (equal/is_parallel_safe/
 *    is_projection_capable_path/expression_returns_set_rows).
 *  - bms set algebra not exposed by relnode-seams (union/del_members/compare/
 *    equal/subset_compare/equal_set) + pathkeys.c (compare_pathkeys/
 *    pathkeys_contained_in) + miscadmin.c CHECK_FOR_INTERRUPTS.
 * ==================================================================== */

use types_pathnodes::QualCost;

/// `PathKeysComparison` — `compare_pathkeys` result (pathkeys.h `PathKeysComparison`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathKeysComparison {
    /// `PATHKEYS_EQUAL`.
    Equal,
    /// `PATHKEYS_BETTER1` — keys1 is a superset of keys2.
    Better1,
    /// `PATHKEYS_BETTER2` — keys2 is a superset of keys1.
    Better2,
    /// `PATHKEYS_DIFFERENT`.
    Different,
}

/// `BMS_Comparison` (bitmapset.h) — `bms_subset_compare` result.
pub use types_nodes::bitmapset::BMS_Comparison;

/* --- miscadmin.c --------------------------------------------------------- */
seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` — process a pending query cancel/die.
    pub fn check_for_interrupts()
);

/* --- pathkeys.c ---------------------------------------------------------- */
seam_core::seam!(
    /// `compare_pathkeys(keys1, keys2)`.
    pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison
);
seam_core::seam!(
    /// `pathkeys_contained_in(keys1, keys2)`.
    pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool
);

/* --- nodes/bitmapset.c (the ops relnode-seams does not expose) ----------- */
seam_core::seam!(
    /// `bms_union(a, b)`.
    pub fn relids_union(a: &Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_del_members(a, b)` — `a` minus `b` (consumes `a`).
    pub fn relids_del_members(a: Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_add_members(a, b)` — union into `a` (consumes `a`).
    pub fn relids_add_members(a: Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_equal(a, b)`.
    pub fn relids_equal(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_equal(a, b)` — used where C compares a rel's relids to a query-rels
    /// set (`create_append_path` LIMIT applicability).
    pub fn relids_equal_set(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_subset_compare(a, b)`.
    pub fn relids_subset_compare(a: &Relids, b: &Relids) -> BMS_Comparison
);
seam_core::seam!(
    /// `bms_compare(a, b)` — total order over bitmapsets (-1/0/1).
    pub fn relids_compare(a: &Relids, b: &Relids) -> i32
);

/* --- relnode.c (param_info construction) -------------------------------- */
seam_core::seam!(
    /// `get_baserel_parampathinfo(root, baserel, required_outer)`.
    pub fn get_baserel_parampathinfo<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        baserel: RelId,
        required_outer: &Relids,
    ) -> Option<Box<types_pathnodes::ParamPathInfo>>
);
seam_core::seam!(
    /// `get_appendrel_parampathinfo(appendrel, required_outer)`.
    pub fn get_appendrel_parampathinfo(
        root: &mut PlannerInfo,
        appendrel: RelId,
        required_outer: &Relids,
    ) -> Option<Box<types_pathnodes::ParamPathInfo>>
);
seam_core::seam!(
    /// `get_joinrel_parampathinfo(root, joinrel, outer_path, inner_path, sjinfo,
    /// required_outer, *restrict_clauses)` — returns the param_info plus the
    /// (possibly trimmed) restrict-clause list.
    pub fn get_joinrel_parampathinfo<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        joinrel: RelId,
        outer_path: PathId,
        inner_path: PathId,
        sjinfo: &SpecialJoinInfo,
        required_outer: &Relids,
        restrict_clauses: Vec<RinfoId>,
    ) -> (Option<Box<types_pathnodes::ParamPathInfo>>, Vec<RinfoId>)
);
seam_core::seam!(
    /// `path->param_info->ppi_clauses` serial set — the `Bitmapset` of clause
    /// serials already enforced inside a parameterized inner path.
    pub fn get_param_path_clause_serials(root: &PlannerInfo, path: PathId) -> Relids
);

/* --- clauses.c / tlist.c / createplan.c (expression walks) -------------- */
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) exprs)` over a tlist of expr handles.
    pub fn is_parallel_safe(root: &PlannerInfo, exprs: &[NodeId]) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) quals)` over a qual list of expr handles.
    pub fn is_parallel_safe_quals(root: &PlannerInfo, quals: &[NodeId]) -> bool
);
seam_core::seam!(
    /// `is_projection_capable_path(path)`.
    pub fn is_projection_capable_path(root: &PlannerInfo, path: PathId) -> bool
);
seam_core::seam!(
    /// `equal((Node *) a, (Node *) b)` over two tlists of expr handles.
    pub fn equal_exprs(root: &PlannerInfo, a: &[NodeId], b: &[NodeId]) -> bool
);
seam_core::seam!(
    /// `expression_returns_set_rows(root, (Node *) expr)`.
    pub fn expression_returns_set_rows(root: &PlannerInfo, node: NodeId) -> f64
);
seam_core::seam!(
    /// `cost_qual_eval(&cost, quals, root)` — eval cost of a list of expr quals.
    pub fn cost_qual_eval(root: &PlannerInfo, quals: &[NodeId]) -> QualCost
);

/* --- costsize.c: cost GUC getters + sizing helpers ---------------------- */
seam_core::seam!(
    /// `cpu_tuple_cost` GUC.
    pub fn cpu_tuple_cost() -> f64
);
seam_core::seam!(
    /// `cpu_operator_cost` GUC.
    pub fn cpu_operator_cost() -> f64
);
seam_core::seam!(
    /// `work_mem` GUC (KB).
    pub fn work_mem() -> i32
);
seam_core::seam!(
    /// `enable_hashagg` GUC.
    pub fn enable_hashagg() -> bool
);
seam_core::seam!(
    /// `get_hash_memory_limit()` (nodeHash.c) — bytes.
    pub fn get_hash_memory_limit() -> f64
);
seam_core::seam!(
    /// `sizeof(MinimalTupleData)` header (htup_details.h `MAXALIGN(SizeofMinimalTupleHeader)`).
    pub fn sizeof_minimal_tuple_header() -> usize
);

/* --- costsize.c: the cost estimators. Each fills the new path's base Path
 *     cost/row fields in the arena (by `PathId`). The non-base subtype fields a
 *     few estimators also fill (IndexPath selectivity, GatherMergePath, etc.)
 *     are written through the same `PathId` (the estimator downcasts the arena
 *     PathNode). ------------------------------------------------------------ */
seam_core::seam!(pub fn cost_seqscan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_samplescan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(
    /// `cost_index(IndexPath *path, root, loop_count, partial_path)`.
    ///
    /// `run` threads the planner-run RTE/Query store so the index-AM
    /// `amcostestimate` callback can reach `examine_variable` /
    /// `clauselist_selectivity` (selfuncs.c) — exactly as C's `root` carries
    /// the parse/range-table the cost estimator walks.
    pub fn cost_index<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        path: PathId,
        loop_count: f64,
        partial_path: bool,
    )
);
seam_core::seam!(
    /// `cost_bitmap_heap_scan(path, root, rel, bitmapqual, loop_count)`.
    pub fn cost_bitmap_heap_scan(
        root: &mut PlannerInfo,
        path: PathId,
        rel: RelId,
        bitmapqual: PathId,
        loop_count: f64,
    )
);
seam_core::seam!(pub fn cost_bitmap_and_node(root: &mut PlannerInfo, path: PathId));
seam_core::seam!(pub fn cost_bitmap_or_node(root: &mut PlannerInfo, path: PathId));
seam_core::seam!(
    /// `cost_tidscan(path, root, rel, tidquals, param_info)`.
    pub fn cost_tidscan(root: &mut PlannerInfo, path: PathId, rel: RelId, tidquals: &[NodeId])
);
seam_core::seam!(
    pub fn cost_tidrangescan<'mcx>(run: &types_pathnodes::planner_run::PlannerRun<'mcx>, root: &mut PlannerInfo, path: PathId, rel: RelId, tidrangequals: &[NodeId])
);
seam_core::seam!(pub fn cost_subqueryscan<'mcx>(run: &types_pathnodes::planner_run::PlannerRun<'mcx>, root: &mut PlannerInfo, path: PathId, rel: RelId, subpath: PathId, trivial_pathtarget: bool));
seam_core::seam!(pub fn cost_functionscan<'mcx>(run: &types_pathnodes::planner_run::PlannerRun<'mcx>, root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_tablefuncscan<'mcx>(run: &types_pathnodes::planner_run::PlannerRun<'mcx>, root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_valuesscan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_ctescan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_namedtuplestorescan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_resultscan(root: &mut PlannerInfo, path: PathId, rel: RelId));
seam_core::seam!(pub fn cost_append(root: &mut PlannerInfo, path: PathId));
seam_core::seam!(
    /// `cost_merge_append(path, root, pathkeys, n_streams, input_disabled_nodes,
    /// input_startup_cost, input_total_cost, tuples)`.
    pub fn cost_merge_append(
        root: &mut PlannerInfo,
        path: PathId,
        pathkeys: &[PathKey],
        n_streams: i32,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        tuples: f64,
    )
);
seam_core::seam!(
    /// `cost_material(path, input_disabled_nodes, input_startup_cost,
    /// input_total_cost, tuples, width)`.
    pub fn cost_material(
        root: &mut PlannerInfo,
        path: PathId,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        tuples: f64,
        width: i32,
    )
);
seam_core::seam!(
    /// `cost_gather(GatherPath *path, root, rel, param_info, rows)`.
    pub fn cost_gather(root: &mut PlannerInfo, path: PathId, rel: RelId, rows: Option<f64>)
);
seam_core::seam!(
    /// `cost_gather_merge(GatherMergePath *path, root, rel, param_info,
    /// input_disabled_nodes, input_startup_cost, input_total_cost, rows)`.
    pub fn cost_gather_merge(
        root: &mut PlannerInfo,
        path: PathId,
        rel: RelId,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        rows: Option<f64>,
    )
);
seam_core::seam!(
    /// `cost_sort(path, root, pathkeys, input_disabled_nodes, input_cost, tuples,
    /// width, comparison_cost, sort_mem, limit_tuples)`.
    pub fn cost_sort(
        root: &mut PlannerInfo,
        path: PathId,
        pathkeys: &[PathKey],
        input_disabled_nodes: i32,
        input_cost: Cost,
        tuples: f64,
        width: i32,
        comparison_cost: Cost,
        sort_mem: i32,
        limit_tuples: f64,
    )
);
seam_core::seam!(
    /// `cost_incremental_sort(path, root, pathkeys, presorted_keys,
    /// input_disabled_nodes, input_startup_cost, input_total_cost, input_tuples,
    /// width, comparison_cost, sort_mem, limit_tuples)`.
    pub fn cost_incremental_sort<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        pathkeys: &[PathKey],
        presorted_keys: i32,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        input_tuples: f64,
        width: i32,
        comparison_cost: Cost,
        sort_mem: i32,
        limit_tuples: f64,
    ) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `cost_group(path, root, numGroupCols, numGroups, quals,
    /// input_disabled_nodes, input_startup_cost, input_total_cost,
    /// input_tuples)`.
    pub fn cost_group<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        num_group_cols: i32,
        num_groups: f64,
        quals: &[NodeId],
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        input_tuples: f64,
    )
);
seam_core::seam!(
    /// `cost_agg(path, root, aggstrategy, aggcosts, numGroupCols, numGroups,
    /// quals, input_disabled_nodes, input_startup_cost, input_total_cost,
    /// input_tuples, input_width)`.
    pub fn cost_agg<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        aggstrategy: AggStrategy,
        aggcosts: Option<AggClauseCostsLite>,
        num_group_cols: i32,
        num_groups: f64,
        quals: &[NodeId],
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        input_tuples: f64,
        input_width: i32,
    )
);
seam_core::seam!(
    /// `cost_windowagg(path, root, windowFuncs, winclause, input_disabled_nodes,
    /// input_startup_cost, input_total_cost, input_tuples)`.
    ///
    /// `run` + `&mut root` thread the `get_windowclause_startup_tuples`
    /// estimate (which calls `estimate_num_groups`, examining `pg_statistic`
    /// through the [`PlannerRun`] RTE store and re-interning stripped grouping
    /// expressions into the arena), so the seam is fallible (OOM + the examine
    /// path's `ereport`s).
    pub fn cost_windowagg<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        window_funcs: &[NodeId],
        winclause: NodeId,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        input_tuples: f64,
    ) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `cost_recursive_union(runion, nrterm, rterm)` — fills the RecursiveUnion
    /// path from its left/right subpaths (both by `PathId`).
    pub fn cost_recursive_union(
        root: &mut PlannerInfo,
        path: PathId,
        nrterm: PathId,
        rterm: PathId,
    )
);
seam_core::seam!(
    /// `final_cost_nestloop(root, path, workspace, extra)`.
    pub fn final_cost_nestloop(
        root: &mut PlannerInfo,
        path: PathId,
        workspace: &types_pathnodes::optimizer_plan::JoinCostWorkspace,
        extra: &types_pathnodes::optimizer_plan::JoinPathExtraData,
    )
);
seam_core::seam!(
    /// `final_cost_mergejoin(root, path, workspace, extra)`.
    pub fn final_cost_mergejoin<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        workspace: &types_pathnodes::optimizer_plan::JoinCostWorkspace,
        extra: &types_pathnodes::optimizer_plan::JoinPathExtraData,
    )
);
seam_core::seam!(
    /// `final_cost_hashjoin(root, path, workspace, extra)`.
    pub fn final_cost_hashjoin<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        workspace: &types_pathnodes::optimizer_plan::JoinCostWorkspace,
        extra: &types_pathnodes::optimizer_plan::JoinPathExtraData,
    )
);

/* ======================================================================
 * pathnode.c routines the join-relation enumerator (joinrels.c) drives:
 * the semijoin RHS unique-ifiability test and the dummy childless-Append
 * installer for `mark_dummy_rel`. (Additive — appended for joinrels.)
 * ==================================================================== */

seam_core::seam!(
    /// `create_unique_path(root, rel, rel->cheapest_total_path, sjinfo) != NULL`
    /// (pathnode.c) — can the relation's RHS be unique-ified for a semijoin?
    /// Returns whether a `UniquePath` could be created (the C non-NULL test);
    /// the path itself is cached on the rel by the owner.
    pub fn can_create_unique_path<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        sjinfo: &SpecialJoinInfo,
    ) -> bool
);
seam_core::seam!(
    /// `mark_dummy_rel(rel)` body (joinrels.c:1324, pathnode-side): evict the
    /// rel's paths, set `rows = 0`, install a childless dummy `create_append_path`
    /// in the rel's own memory context, and `set_cheapest`. Owned by pathnode.c
    /// because it `create_append_path`/`add_path`/`set_cheapest`s.
    pub fn install_dummy_append_path<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: RelId,
    ) -> PgResult<()>
);
