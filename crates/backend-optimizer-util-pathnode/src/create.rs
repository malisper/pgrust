//! `create_*_path` factory family (pathnode.c:985-4884) + the join required_outer
//! helpers + reparameterization.
//!
//! Each constructor builds the concrete [`PathNode`] subtype, allocates it into
//! the planner arena (`root.alloc_path`, the C freshly-`palloc`'d `Path *`) and
//! returns its [`PathId`]. The cost model fills the new path's cost/row fields
//! *after* allocation, addressed by `PathId` through the `cost_*` seams (the C
//! `cost_X(path, root, …)` mutating `path->startup_cost`/`total_cost`/`rows`).
//! Subpaths are already-installed paths referenced by their `PathId`.

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::{AttrNumber, Cost, Index, InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_pathnodes::optimizer_plan::{JoinCostWorkspace, JoinPathExtraData};
use types_pathnodes::{
    AggPath, AggSplit, AggStrategy, AppendPath, BitmapAndPath, BitmapHeapPath, BitmapOrPath,
    CmdType, ForeignPath, GatherMergePath, GatherPath, GroupPath, GroupResultPath, GroupingSetsPath,
    HashPath, IncrementalSortPath, IndexClause, IndexOptInfo, IndexPath, JoinPath, JoinType,
    LimitOption, LimitPath, LockRowsPath, MaterialPath, MemoizePath, MergeAppendPath, MergePath,
    MinMaxAggInfo, MinMaxAggPath, ModifyTablePath, NestPath, NodeId, NodeTag, Path, PathId, PathKey,
    PathNode, PathTarget, PlannerInfo, ProjectSetPath, ProjectionPath, QualCost, RecursiveUnionPath,
    RelId, Relids, RinfoId, ScanDirection, SetOpCmd, SetOpPath, SetOpStrategy, SortPath,
    SpecialJoinInfo, SubqueryScanPath, TidPath, TidRangePath, UpperUniquePath, WindowAggPath,
};

use backend_optimizer_util_pathnode_seams as seam;
use backend_optimizer_util_pathnode_seams::AggClauseCostsLite;
use backend_optimizer_util_relnode_seams as bms;

use crate::{clamp_row_est, compare_path_costs, oom, CostSelector};

/* --------------------------------------------------------------------------
 * makeNode path-node tags (nodetags.h) — `Path.type_`.
 * ------------------------------------------------------------------------ */
const T_PATH: NodeTag = NodeTag(279);
const T_INDEX_PATH: NodeTag = NodeTag(280);
const T_BITMAP_HEAP_PATH: NodeTag = NodeTag(282);
const T_BITMAP_AND_PATH: NodeTag = NodeTag(283);
const T_BITMAP_OR_PATH: NodeTag = NodeTag(284);
const T_TID_PATH: NodeTag = NodeTag(285);
const T_TID_RANGE_PATH: NodeTag = NodeTag(286);
const T_SUBQUERY_SCAN_PATH: NodeTag = NodeTag(287);
const T_FOREIGN_PATH: NodeTag = NodeTag(288);
const T_APPEND_PATH: NodeTag = NodeTag(290);
const T_MERGE_APPEND_PATH: NodeTag = NodeTag(291);
const T_GROUP_RESULT_PATH: NodeTag = NodeTag(292);
const T_MATERIAL_PATH: NodeTag = NodeTag(293);
const T_MEMOIZE_PATH: NodeTag = NodeTag(294);
const T_GATHER_PATH: NodeTag = NodeTag(296);
const T_GATHER_MERGE_PATH: NodeTag = NodeTag(297);
const T_NEST_PATH: NodeTag = NodeTag(298);
const T_MERGE_PATH: NodeTag = NodeTag(299);
const T_HASH_PATH: NodeTag = NodeTag(300);
const T_PROJECT_SET_PATH: NodeTag = NodeTag(302);
const T_SORT_PATH: NodeTag = NodeTag(303);
const T_INCREMENTAL_SORT_PATH: NodeTag = NodeTag(304);
const T_GROUP_PATH: NodeTag = NodeTag(305);
const T_UPPER_UNIQUE_PATH: NodeTag = NodeTag(306);
const T_AGG_PATH: NodeTag = NodeTag(307);
const T_GROUPING_SETS_PATH: NodeTag = NodeTag(310);
const T_MIN_MAX_AGG_PATH: NodeTag = NodeTag(311);
const T_WINDOW_AGG_PATH: NodeTag = NodeTag(312);
const T_SET_OP_PATH: NodeTag = NodeTag(313);
const T_RECURSIVE_UNION_PATH: NodeTag = NodeTag(314);
const T_LOCK_ROWS_PATH: NodeTag = NodeTag(315);
const T_MODIFY_TABLE_PATH: NodeTag = NodeTag(316);
const T_LIMIT_PATH: NodeTag = NodeTag(317);

/* --------------------------------------------------------------------------
 * Plan-node tags (nodetags.h) — `Path.pathtype`.
 * ------------------------------------------------------------------------ */
const T_RESULT: NodeTag = NodeTag(331);
const T_PROJECT_SET: NodeTag = NodeTag(332);
const T_MODIFY_TABLE: NodeTag = NodeTag(333);
const T_APPEND: NodeTag = NodeTag(334);
const T_MERGE_APPEND: NodeTag = NodeTag(335);
const T_RECURSIVE_UNION: NodeTag = NodeTag(336);
const T_BITMAP_AND: NodeTag = NodeTag(337);
const T_BITMAP_OR: NodeTag = NodeTag(338);
const T_SEQ_SCAN: NodeTag = NodeTag(339);
const T_SAMPLE_SCAN: NodeTag = NodeTag(340);
const T_INDEX_SCAN: NodeTag = NodeTag(341);
const T_INDEX_ONLY_SCAN: NodeTag = NodeTag(342);
const T_BITMAP_HEAP_SCAN: NodeTag = NodeTag(344);
const T_TID_SCAN: NodeTag = NodeTag(345);
const T_TID_RANGE_SCAN: NodeTag = NodeTag(346);
const T_SUBQUERY_SCAN: NodeTag = NodeTag(347);
const T_FUNCTION_SCAN: NodeTag = NodeTag(348);
const T_VALUES_SCAN: NodeTag = NodeTag(349);
const T_TABLE_FUNC_SCAN: NodeTag = NodeTag(350);
const T_CTE_SCAN: NodeTag = NodeTag(351);
const T_NAMED_TUPLESTORE_SCAN: NodeTag = NodeTag(352);
const T_WORK_TABLE_SCAN: NodeTag = NodeTag(353);
const T_FOREIGN_SCAN: NodeTag = NodeTag(354);
const T_NEST_LOOP: NodeTag = NodeTag(356);
const T_MERGE_JOIN: NodeTag = NodeTag(358);
const T_HASH_JOIN: NodeTag = NodeTag(359);
const T_MATERIAL: NodeTag = NodeTag(360);
const T_MEMOIZE: NodeTag = NodeTag(361);
const T_SORT: NodeTag = NodeTag(362);
const T_INCREMENTAL_SORT: NodeTag = NodeTag(363);
const T_GROUP: NodeTag = NodeTag(364);
const T_AGG: NodeTag = NodeTag(365);
const T_WINDOW_AGG: NodeTag = NodeTag(366);
const T_UNIQUE: NodeTag = NodeTag(367);
const T_GATHER: NodeTag = NodeTag(368);
const T_GATHER_MERGE: NodeTag = NodeTag(369);
const T_SET_OP: NodeTag = NodeTag(371);
const T_LOCK_ROWS: NodeTag = NodeTag(372);
const T_LIMIT: NodeTag = NodeTag(373);

const AGG_PLAIN: AggStrategy = types_pathnodes::AGG_PLAIN;
const AGG_SORTED: AggStrategy = types_pathnodes::AGG_SORTED;
const AGG_HASHED: AggStrategy = types_pathnodes::AGG_HASHED;
const AGG_MIXED: AggStrategy = types_pathnodes::AGG_MIXED;
const SETOP_SORTED: SetOpStrategy = types_pathnodes::SETOP_SORTED;
const RELOPT_BASEREL: u32 = 1;
const CMD_UPDATE: CmdType = types_pathnodes::CMD_UPDATE;
const CMD_MERGE: CmdType = types_pathnodes::CMD_MERGE;

/// `PATH_REQ_OUTER` of a borrowed [`Path`] — `param_info ? ppi_req_outer : NULL`.
#[inline]
fn path_req_outer(path: &Path) -> Relids {
    match &path.param_info {
        Some(ppi) => seam::relids_union::call(&ppi.ppi_req_outer, &None),
        None => None,
    }
}

/// `MAXALIGN(len)` — round up to `MAXIMUM_ALIGNOF` (8 on a 64-bit build).
#[inline]
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// Build the common base [`Path`] prologue fields every constructor shares.
fn base_path(
    type_: NodeTag,
    pathtype: NodeTag,
    parent: RelId,
    pathtarget: Option<Box<PathTarget>>,
) -> Path {
    Path {
        type_,
        pathtype,
        parent,
        pathtarget,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: Vec::new(),
    }
}

/// `pathnode->pathtarget = rel->reltarget` — owned clone (cost model only reads
/// `width`/`cost`).
#[inline]
fn rel_reltarget(root: &PlannerInfo, rel: RelId) -> Option<Box<PathTarget>> {
    root.rel(rel).reltarget.clone()
}

/// Width of a path's pathtarget (`pathtarget->width`, 0 if none).
#[inline]
fn pathtarget_width(p: &Path) -> i32 {
    p.pathtarget.as_ref().map(|t| t.width).unwrap_or(0)
}

/* ===========================================================================
 * Scan-path constructors (pathnode.c:985-1289).
 * ======================================================================== */

/// `create_seqscan_path(root, rel, required_outer, parallel_workers)`.
pub fn create_seqscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
    parallel_workers: i32,
) -> PgResult<PathId> {
    let mut pathnode = base_path(T_PATH, T_SEQ_SCAN, rel, rel_reltarget(root, rel));
    pathnode.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    pathnode.parallel_aware = parallel_workers > 0;
    pathnode.parallel_safe = root.rel(rel).consider_parallel;
    pathnode.parallel_workers = parallel_workers;
    pathnode.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(pathnode));
    seam::cost_seqscan::call(root, id, rel);
    Ok(id)
}

/// `create_samplescan_path(root, rel, required_outer)` (pathnode.c:1010).
pub fn create_samplescan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut pathnode = base_path(T_PATH, T_SAMPLE_SCAN, rel, rel_reltarget(root, rel));
    pathnode.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    pathnode.parallel_aware = false;
    pathnode.parallel_safe = root.rel(rel).consider_parallel;
    pathnode.parallel_workers = 0;
    pathnode.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(pathnode));
    seam::cost_samplescan::call(root, id, rel);
    Ok(id)
}

/// `create_index_path(...)` (pathnode.c:1051).
pub fn create_index_path(
    root: &mut PlannerInfo,
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
) -> PgResult<PathId> {
    let rel = index.rel.ok_or_else(|| PgError::error("IndexOptInfo.rel must be set"))?;
    let pathtype = if indexonly { T_INDEX_ONLY_SCAN } else { T_INDEX_SCAN };
    let mut path = base_path(T_INDEX_PATH, pathtype, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = pathkeys;
    let pathnode = IndexPath {
        path,
        indexinfo: Some(index),
        indexclauses,
        indexorderbys,
        indexorderbycols,
        indexscandir,
        indextotalcost: 0.0,
        indexselectivity: 0.0,
    };
    let id = root.alloc_path(PathNode::IndexPath(pathnode));
    seam::cost_index::call(root, id, loop_count, partial_path);
    Ok(id)
}

/// `create_bitmap_heap_path(...)` (pathnode.c:1100).
pub fn create_bitmap_heap_path(
    root: &mut PlannerInfo,
    rel: RelId,
    bitmapqual: PathId,
    required_outer: &Relids,
    loop_count: f64,
    parallel_degree: i32,
) -> PgResult<PathId> {
    let mut path = base_path(T_BITMAP_HEAP_PATH, T_BITMAP_HEAP_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = parallel_degree > 0;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = parallel_degree;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::BitmapHeapPath(BitmapHeapPath {
        path,
        bitmapqual: Some(bitmapqual),
    }));
    seam::cost_bitmap_heap_scan::call(root, id, rel, bitmapqual, loop_count);
    Ok(id)
}

/// `create_bitmap_and_path(root, rel, bitmapquals)` (pathnode.c:1133).
pub fn create_bitmap_and_path(
    root: &mut PlannerInfo,
    rel: RelId,
    bitmapquals: Vec<PathId>,
) -> PgResult<PathId> {
    let mut path = base_path(T_BITMAP_AND_PATH, T_BITMAP_AND, rel, rel_reltarget(root, rel));
    let mut required_outer: Relids = None;
    for &bq in &bitmapquals {
        let child = root.path(bq).base();
        required_outer = seam::relids_add_members::call(required_outer, &path_req_outer(child));
    }
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, &required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let pathnode = BitmapAndPath {
        path,
        bitmapquals,
        bitmapselectivity: 0.0,
    };
    let id = root.alloc_path(PathNode::BitmapAndPath(pathnode));
    seam::cost_bitmap_and_node::call(root, id);
    Ok(id)
}

/// `create_bitmap_or_path(root, rel, bitmapquals)` (pathnode.c:1185).
pub fn create_bitmap_or_path(
    root: &mut PlannerInfo,
    rel: RelId,
    bitmapquals: Vec<PathId>,
) -> PgResult<PathId> {
    let mut path = base_path(T_BITMAP_OR_PATH, T_BITMAP_OR, rel, rel_reltarget(root, rel));
    let mut required_outer: Relids = None;
    for &bq in &bitmapquals {
        let child = root.path(bq).base();
        required_outer = seam::relids_add_members::call(required_outer, &path_req_outer(child));
    }
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, &required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let pathnode = BitmapOrPath {
        path,
        bitmapquals,
        bitmapselectivity: 0.0,
    };
    let id = root.alloc_path(PathNode::BitmapOrPath(pathnode));
    seam::cost_bitmap_or_node::call(root, id);
    Ok(id)
}

/// `create_tidscan_path(root, rel, tidquals, required_outer)` (pathnode.c:1237).
pub fn create_tidscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    tidquals: Vec<NodeId>,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_TID_PATH, T_TID_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::TidPath(TidPath { path, tidquals }));
    let tq = tidquals_of(root, id);
    seam::cost_tidscan::call(root, id, rel, &tq);
    Ok(id)
}

fn tidquals_of(root: &PlannerInfo, id: PathId) -> Vec<NodeId> {
    match root.path(id) {
        PathNode::TidPath(p) => p.tidquals.clone(),
        _ => Vec::new(),
    }
}

/// `create_tidrangescan_path(...)` (pathnode.c:1266).
pub fn create_tidrangescan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    tidrangequals: Vec<NodeId>,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_TID_RANGE_PATH, T_TID_RANGE_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::TidRangePath(TidRangePath { path, tidrangequals }));
    let trq = match root.path(id) {
        PathNode::TidRangePath(p) => p.tidrangequals.clone(),
        _ => Vec::new(),
    };
    seam::cost_tidrangescan::call(root, id, rel, &trq);
    Ok(id)
}

/* ===========================================================================
 * Append / MergeAppend (pathnode.c:1302-1579).
 * ======================================================================== */

/// `create_append_path(...)` (pathnode.c:1302). `have_root=false` is C
/// `root == NULL`; `rows < 0` means "compute from subpaths".
pub fn create_append_path(
    root: &mut PlannerInfo,
    have_root: bool,
    rel: RelId,
    subpaths: Vec<PathId>,
    partial_subpaths: Vec<PathId>,
    pathkeys: Vec<PathKey>,
    required_outer: &Relids,
    parallel_workers: i32,
    parallel_aware: bool,
    rows: f64,
) -> PgResult<PathId> {
    debug_assert!(!parallel_aware || parallel_workers > 0);

    let mut path = base_path(T_APPEND_PATH, T_APPEND, rel, rel_reltarget(root, rel));

    let reloptkind = root.rel(rel).reloptkind;
    if reloptkind == RELOPT_BASEREL && have_root && !subpaths.is_empty() {
        path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    } else {
        path.param_info = seam::get_appendrel_parampathinfo::call(root, rel, required_outer);
    }

    path.parallel_aware = parallel_aware;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = parallel_workers;
    path.pathkeys = pathkeys.clone();

    let mut subpaths = subpaths;
    let mut partial_subpaths = partial_subpaths;
    if path.parallel_aware {
        debug_assert!(pathkeys.is_empty());
        sort_append_subpaths(root, &mut subpaths, true);
        sort_append_subpaths(root, &mut partial_subpaths, false);
    }

    let first_partial_path = subpaths.len() as i32;
    subpaths.try_reserve(partial_subpaths.len()).map_err(oom)?;
    subpaths.extend(partial_subpaths);

    let limit_tuples = if have_root
        && seam::relids_equal_set::call(&root.rel(rel).relids, &root.all_query_rels)
    {
        root.limit_tuples
    } else {
        -1.0
    };

    for &sp in &subpaths {
        let child = root.path(sp).base();
        path.parallel_safe = path.parallel_safe && child.parallel_safe;
    }

    let pathnode = AppendPath {
        path,
        subpaths,
        first_partial_path,
        limit_tuples,
    };
    let id = root.alloc_path(PathNode::AppendPath(pathnode));

    // Single-child Append may be a no-op (inherits the child); else cost_append.
    let n = match root.path(id) {
        PathNode::AppendPath(p) => p.subpaths.len(),
        _ => 0,
    };
    if n == 1 {
        let child_id = match root.path(id) {
            PathNode::AppendPath(p) => p.subpaths[0],
            _ => unreachable!(),
        };
        let child: Path = root.path(child_id).base().clone();
        if child.parallel_aware == parallel_aware {
            let p = root.path_mut(id).base_mut();
            p.rows = child.rows;
            p.startup_cost = child.startup_cost;
            p.total_cost = child.total_cost;
        } else {
            seam::cost_append::call(root, id);
        }
        // Must do this last, else cost_append complains.
        root.path_mut(id).base_mut().pathkeys = child.pathkeys;
    } else {
        seam::cost_append::call(root, id);
    }

    if rows >= 0.0 {
        root.path_mut(id).base_mut().rows = rows;
    }
    Ok(id)
}

/// `list_sort` of Append subpaths by descending total/startup cost, relids
/// tiebreak via `bms_compare`.
fn sort_append_subpaths(root: &PlannerInfo, subpaths: &mut [PathId], by_total: bool) {
    let crit = if by_total {
        CostSelector::TOTAL_COST
    } else {
        CostSelector::STARTUP_COST
    };
    subpaths.sort_by(|&a, &b| {
        let pa = root.path(a).base();
        let pb = root.path(b).base();
        let cmp = compare_path_costs(pa, pb, crit);
        let ord = if cmp != 0 {
            -cmp
        } else {
            seam::relids_compare::call(&root.rel(pa.parent).relids, &root.rel(pb.parent).relids)
        };
        ord.cmp(&0)
    });
}

/// `create_merge_append_path(...)` (pathnode.c:1473).
pub fn create_merge_append_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpaths: Vec<PathId>,
    pathkeys: Vec<PathKey>,
    _required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_MERGE_APPEND_PATH, T_MERGE_APPEND, rel, rel_reltarget(root, rel));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = pathkeys.clone();

    let limit_tuples =
        if seam::relids_equal_set::call(&root.rel(rel).relids, &root.all_query_rels) {
            root.limit_tuples
        } else {
            -1.0
        };

    path.rows = 0.0;
    let mut input_disabled_nodes: i32 = 0;
    let mut input_startup_cost: Cost = 0.0;
    let mut input_total_cost: Cost = 0.0;
    for &sp in &subpaths {
        let subpath: Path = root.path(sp).base().clone();
        path.rows += subpath.rows;
        path.parallel_safe = path.parallel_safe && subpath.parallel_safe;

        if seam::pathkeys_contained_in::call(&pathkeys, &subpath.pathkeys) {
            input_disabled_nodes += subpath.disabled_nodes;
            input_startup_cost += subpath.startup_cost;
            input_total_cost += subpath.total_cost;
        } else {
            // Need a Sort node; cost it via a dummy arena Path.
            let sort_dummy = base_path(T_PATH, T_SORT, rel, None);
            let width = pathtarget_width(&subpath);
            let sub_disabled = subpath.disabled_nodes;
            let sub_total = subpath.total_cost;
            let sub_rows = subpath.rows;
            let dummy_id = root.alloc_path(PathNode::Path(sort_dummy));
            let wm = seam::work_mem::call();
            seam::cost_sort::call(
                root, dummy_id, &pathkeys, sub_disabled, sub_total, sub_rows, width, 0.0, wm,
                limit_tuples,
            );
            let d = root.path(dummy_id).base();
            input_disabled_nodes += d.disabled_nodes;
            input_startup_cost += d.startup_cost;
            input_total_cost += d.total_cost;
        }
    }

    let one_noop = subpaths.len() == 1
        && root.path(subpaths[0]).base().parallel_aware == path.parallel_aware;
    let tuples = path.rows;
    let n = subpaths.len() as i32;

    let id = root.alloc_path(PathNode::MergeAppendPath(MergeAppendPath {
        path,
        subpaths,
        limit_tuples,
    }));

    if one_noop {
        let p = root.path_mut(id).base_mut();
        p.disabled_nodes = input_disabled_nodes;
        p.startup_cost = input_startup_cost;
        p.total_cost = input_total_cost;
    } else {
        let pk = match root.path(id) {
            PathNode::MergeAppendPath(p) => p.path.pathkeys.clone(),
            _ => Vec::new(),
        };
        seam::cost_merge_append::call(
            root, id, &pk, n, input_disabled_nodes, input_startup_cost, input_total_cost, tuples,
        );
    }

    Ok(id)
}

/* ===========================================================================
 * GroupResult / Material / Memoize (pathnode.c:1588-1716).
 * ======================================================================== */

/// `create_group_result_path(root, rel, target, havingqual)` (pathnode.c:1588).
pub fn create_group_result_path(
    root: &mut PlannerInfo,
    rel: RelId,
    target: Box<PathTarget>,
    havingqual: Vec<NodeId>,
) -> PgResult<PathId> {
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;
    let mut path = base_path(T_GROUP_RESULT_PATH, T_RESULT, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();

    path.rows = 1.0;
    path.startup_cost = target_startup;
    path.total_cost = target_startup + seam::cpu_tuple_cost::call() + target_per_tuple;

    if !havingqual.is_empty() {
        let qc = seam::cost_qual_eval::call(root, &havingqual);
        path.startup_cost += qc.startup + qc.per_tuple;
        path.total_cost += qc.startup + qc.per_tuple;
    }

    Ok(root.alloc_path(PathNode::GroupResultPath(GroupResultPath {
        path,
        quals: havingqual,
    })))
}

/// `create_material_path(rel, subpath)` (pathnode.c:1636).
pub fn create_material_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_MATERIAL_PATH, T_MATERIAL, rel, rel_reltarget(root, rel));
    path.param_info = sp.param_info.clone();
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();
    let width = pathtarget_width(&sp);
    let id = root.alloc_path(PathNode::MaterialPath(MaterialPath {
        path,
        subpath: Some(subpath),
    }));
    seam::cost_material::call(
        root,
        id,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        sp.rows,
        width,
    );
    Ok(id)
}

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
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_MEMOIZE_PATH, T_MEMOIZE, rel, rel_reltarget(root, rel));
    path.param_info = sp.param_info.clone();
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    path.disabled_nodes = sp.disabled_nodes;
    let cpu_tuple_cost = seam::cpu_tuple_cost::call();
    path.startup_cost = sp.startup_cost + cpu_tuple_cost;
    path.total_cost = sp.total_cost + cpu_tuple_cost;
    path.rows = sp.rows;

    Ok(root.alloc_path(PathNode::MemoizePath(MemoizePath {
        path,
        subpath: Some(subpath),
        hash_operators,
        param_exprs,
        singlerow,
        binary_mode,
        calls: clamp_row_est(calls),
        est_entries: 0,
    })))
}

/* ===========================================================================
 * Gather / GatherMerge (pathnode.c:2098-2211).
 * ======================================================================== */

/// `create_gather_merge_path(...)` (pathnode.c:2097).
pub fn create_gather_merge_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    target: Option<Box<PathTarget>>,
    pathkeys: Vec<PathKey>,
    required_outer: &Relids,
    rows: Option<f64>,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    debug_assert!(sp.parallel_safe);
    debug_assert!(!pathkeys.is_empty());

    if !seam::pathkeys_contained_in::call(&pathkeys, &sp.pathkeys) {
        return Err(PgError::error("gather merge input not sufficiently sorted"));
    }

    let pathtarget = target.or_else(|| rel_reltarget(root, rel));
    let mut path = base_path(T_GATHER_MERGE_PATH, T_GATHER_MERGE, rel, pathtarget);
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.pathkeys = pathkeys;

    let pathnode = GatherMergePath {
        path,
        subpath: Some(subpath),
        num_workers: sp.parallel_workers,
    };
    let id = root.alloc_path(PathNode::GatherMergePath(pathnode));

    seam::cost_gather_merge::call(
        root,
        id,
        rel,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        rows,
    );

    Ok(id)
}

/// `create_gather_path(...)` (pathnode.c:2179).
pub fn create_gather_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    target: Option<Box<PathTarget>>,
    required_outer: &Relids,
    rows: Option<f64>,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    debug_assert!(sp.parallel_safe);

    let mut path = base_path(T_GATHER_PATH, T_GATHER, rel, target);
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = false;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();

    let mut num_workers = sp.parallel_workers;
    let mut single_copy = false;
    if num_workers == 0 {
        path.pathkeys = sp.pathkeys.clone();
        num_workers = 1;
        single_copy = true;
    }

    let pathnode = GatherPath {
        path,
        subpath: Some(subpath),
        single_copy,
        num_workers,
    };
    let id = root.alloc_path(PathNode::GatherPath(pathnode));
    seam::cost_gather::call(root, id, rel, rows);
    Ok(id)
}

/* ===========================================================================
 * Remaining scan-path constructors (pathnode.c:2223-2429).
 * ======================================================================== */

/// `create_subqueryscan_path(...)` (pathnode.c:2223).
pub fn create_subqueryscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    trivial_pathtarget: bool,
    pathkeys: Vec<PathKey>,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_SUBQUERY_SCAN_PATH, T_SUBQUERY_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = pathkeys;
    let id = root.alloc_path(PathNode::SubqueryScanPath(SubqueryScanPath {
        path,
        subpath: Some(subpath),
    }));
    seam::cost_subqueryscan::call(root, id, rel, subpath, trivial_pathtarget);
    Ok(id)
}

/// `create_functionscan_path(...)` (pathnode.c:2253).
pub fn create_functionscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    pathkeys: Vec<PathKey>,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_FUNCTION_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = pathkeys;
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_functionscan::call(root, id, rel);
    Ok(id)
}

/// `create_tablefuncscan_path(...)` (pathnode.c:2279).
pub fn create_tablefuncscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_TABLE_FUNC_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_tablefuncscan::call(root, id, rel);
    Ok(id)
}

/// `create_valuesscan_path(...)` (pathnode.c:2305).
pub fn create_valuesscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_VALUES_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_valuesscan::call(root, id, rel);
    Ok(id)
}

/// `create_ctescan_path(...)` (pathnode.c:2331).
pub fn create_ctescan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    pathkeys: Vec<PathKey>,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_CTE_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = pathkeys;
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_ctescan::call(root, id, rel);
    Ok(id)
}

/// `create_namedtuplestorescan_path(...)` (pathnode.c:2357).
pub fn create_namedtuplestorescan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_NAMED_TUPLESTORE_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_namedtuplestorescan::call(root, id, rel);
    Ok(id)
}

/// `create_resultscan_path(...)` (pathnode.c:2383).
pub fn create_resultscan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_RESULT, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_resultscan::call(root, id, rel);
    Ok(id)
}

/// `create_worktablescan_path(...)` (pathnode.c:2409). Cost = CTE scan.
pub fn create_worktablescan_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<PathId> {
    let mut path = base_path(T_PATH, T_WORK_TABLE_SCAN, rel, rel_reltarget(root, rel));
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();
    let id = root.alloc_path(PathNode::Path(path));
    seam::cost_ctescan::call(root, id, rel);
    Ok(id)
}

/* ===========================================================================
 * Foreign paths (pathnode.c:2442-2580). FDW-supplied cost; no cost seam.
 * ======================================================================== */

/// `create_foreignscan_path(...)` (pathnode.c:2442).
pub fn create_foreignscan_path(
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
) -> PgResult<PathId> {
    let pathtarget = target.or_else(|| rel_reltarget(root, rel));
    let mut path = base_path(T_FOREIGN_PATH, T_FOREIGN_SCAN, rel, pathtarget);
    path.param_info = seam::get_baserel_parampathinfo::call(root, rel, required_outer);
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.rows = rows;
    path.disabled_nodes = disabled_nodes;
    path.startup_cost = startup_cost;
    path.total_cost = total_cost;
    path.pathkeys = pathkeys;
    Ok(root.alloc_path(PathNode::ForeignPath(ForeignPath {
        path,
        fdw_outerpath,
        fdw_restrictinfo,
        fdw_private,
    })))
}

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
) -> PgResult<PathId> {
    if required_outer.is_some() || root.rel(rel).lateral_relids.is_some() {
        return Err(PgError::error(
            "parameterized foreign joins are not supported yet",
        ));
    }
    let pathtarget = target.or_else(|| rel_reltarget(root, rel));
    let mut path = base_path(T_FOREIGN_PATH, T_FOREIGN_SCAN, rel, pathtarget);
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.rows = rows;
    path.disabled_nodes = disabled_nodes;
    path.startup_cost = startup_cost;
    path.total_cost = total_cost;
    path.pathkeys = pathkeys;
    Ok(root.alloc_path(PathNode::ForeignPath(ForeignPath {
        path,
        fdw_outerpath,
        fdw_restrictinfo,
        fdw_private,
    })))
}

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
) -> PgResult<PathId> {
    let pathtarget = target.or_else(|| rel_reltarget(root, rel));
    let mut path = base_path(T_FOREIGN_PATH, T_FOREIGN_SCAN, rel, pathtarget);
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel;
    path.parallel_workers = 0;
    path.rows = rows;
    path.disabled_nodes = disabled_nodes;
    path.startup_cost = startup_cost;
    path.total_cost = total_cost;
    path.pathkeys = pathkeys;
    Ok(root.alloc_path(PathNode::ForeignPath(ForeignPath {
        path,
        fdw_outerpath,
        fdw_restrictinfo,
        fdw_private,
    })))
}

/* ===========================================================================
 * Join required_outer helpers (pathnode.c:2591-2651).
 * ======================================================================== */

/// `calc_nestloop_required_outer(...)` (pathnode.c:2591).
pub fn calc_nestloop_required_outer(
    outerrelids: &Relids,
    outer_paramrels: &Relids,
    _innerrelids: &Relids,
    inner_paramrels: &Relids,
) -> Relids {
    if inner_paramrels.is_none() {
        return seam::relids_union::call(outer_paramrels, &None);
    }
    let required_outer = seam::relids_union::call(outer_paramrels, inner_paramrels);
    seam::relids_del_members::call(required_outer, outerrelids)
}

/// `calc_non_nestloop_required_outer(outer_path, inner_path)` (pathnode.c:2618).
pub fn calc_non_nestloop_required_outer(
    root: &PlannerInfo,
    outer_path: PathId,
    inner_path: PathId,
) -> Relids {
    let outer_paramrels = path_req_outer(root.path(outer_path).base());
    let inner_paramrels = path_req_outer(root.path(inner_path).base());
    seam::relids_union::call(&outer_paramrels, &inner_paramrels)
}

/* ===========================================================================
 * Join-path constructors (pathnode.c:2670-2891).
 * ======================================================================== */

/// `create_nestloop_path(...)` (pathnode.c:2670).
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
) -> PgResult<PathId> {
    let outer_base: Path = root.path(outer_path).base().clone();
    let inner_base: Path = root.path(inner_path).base().clone();
    let inner_req_outer = path_req_outer(&inner_base);

    let outer_top = root.rel(outer_base.parent).top_parent_relids.clone();
    let outerrelids = if outer_top.is_some() {
        outer_top
    } else {
        root.rel(outer_base.parent).relids.clone()
    };

    let mut restrict_clauses: Vec<RinfoId> = restrict_clauses.to_vec();

    if bms::relids_overlap::call(&inner_req_outer, &outerrelids) {
        let enforced_serials = seam::get_param_path_clause_serials::call(root, inner_path);
        let mut jclauses: Vec<RinfoId> = Vec::new();
        for &rid in &restrict_clauses {
            let serial = root.rinfo(rid).rinfo_serial;
            if !bms::relids_is_member::call(serial, &enforced_serials) {
                jclauses.push(rid);
            }
        }
        restrict_clauses = jclauses;
    }

    let mut path = base_path(T_NEST_PATH, T_NEST_LOOP, joinrel, rel_reltarget(root, joinrel));
    let sjinfo = extra
        .sjinfo
        .as_deref()
        .cloned()
        .unwrap_or_else(default_sjinfo);
    let (ppi, restrict_clauses) = seam::get_joinrel_parampathinfo::call(
        root,
        joinrel,
        outer_path,
        inner_path,
        &sjinfo,
        required_outer,
        restrict_clauses,
    );
    path.param_info = ppi;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(joinrel).consider_parallel
        && outer_base.parallel_safe
        && inner_base.parallel_safe;
    path.parallel_workers = outer_base.parallel_workers;
    path.pathkeys = pathkeys.to_vec();

    let pathnode = NestPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique: extra.inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo: restrict_clauses,
        },
    };
    let id = root.alloc_path(PathNode::NestPath(pathnode));
    seam::final_cost_nestloop::call(root, id, workspace, extra);
    Ok(id)
}

/// `create_mergejoin_path(...)` (pathnode.c:2767).
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
    path_mergeclauses: &[RinfoId],
    outersortkeys: &[PathKey],
    innersortkeys: &[PathKey],
    outer_presorted_keys: i32,
) -> PgResult<PathId> {
    let outer_base: Path = root.path(outer_path).base().clone();
    let inner_base: Path = root.path(inner_path).base().clone();

    let mut path = base_path(T_MERGE_PATH, T_MERGE_JOIN, joinrel, rel_reltarget(root, joinrel));
    let sjinfo = extra
        .sjinfo
        .as_deref()
        .cloned()
        .unwrap_or_else(default_sjinfo);
    let (ppi, restrict_clauses) = seam::get_joinrel_parampathinfo::call(
        root,
        joinrel,
        outer_path,
        inner_path,
        &sjinfo,
        required_outer,
        restrict_clauses.to_vec(),
    );
    path.param_info = ppi;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(joinrel).consider_parallel
        && outer_base.parallel_safe
        && inner_base.parallel_safe;
    path.parallel_workers = outer_base.parallel_workers;
    path.pathkeys = pathkeys.to_vec();

    let pathnode = MergePath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique: extra.inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo: restrict_clauses,
        },
        path_mergeclauses: path_mergeclauses.to_vec(),
        outersortkeys: outersortkeys.to_vec(),
        innersortkeys: innersortkeys.to_vec(),
        outer_presorted_keys,
        skip_mark_restore: false,
        materialize_inner: false,
    };
    let id = root.alloc_path(PathNode::MergePath(pathnode));
    seam::final_cost_mergejoin::call(root, id, workspace, extra);
    Ok(id)
}

/// `create_hashjoin_path(...)` (pathnode.c:2835).
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
    path_hashclauses: &[RinfoId],
) -> PgResult<PathId> {
    let outer_base: Path = root.path(outer_path).base().clone();
    let inner_base: Path = root.path(inner_path).base().clone();

    let mut path = base_path(T_HASH_PATH, T_HASH_JOIN, joinrel, rel_reltarget(root, joinrel));
    let sjinfo = extra
        .sjinfo
        .as_deref()
        .cloned()
        .unwrap_or_else(default_sjinfo);
    let (ppi, restrict_clauses) = seam::get_joinrel_parampathinfo::call(
        root,
        joinrel,
        outer_path,
        inner_path,
        &sjinfo,
        required_outer,
        restrict_clauses.to_vec(),
    );
    path.param_info = ppi;
    path.parallel_aware = root.rel(joinrel).consider_parallel && parallel_hash;
    path.parallel_safe = root.rel(joinrel).consider_parallel
        && outer_base.parallel_safe
        && inner_base.parallel_safe;
    path.parallel_workers = outer_base.parallel_workers;
    path.pathkeys = Vec::new(); // hashjoin output ordering is unpredictable

    let pathnode = HashPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique: extra.inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo: restrict_clauses,
        },
        path_hashclauses: path_hashclauses.to_vec(),
        num_batches: 0,
        inner_rows_total: 0.0,
    };
    let id = root.alloc_path(PathNode::HashPath(pathnode));
    seam::final_cost_hashjoin::call(root, id, workspace, extra);
    Ok(id)
}

/// A zeroed [`SpecialJoinInfo`] for the `extra.sjinfo == NULL` plain-join case
/// (the C passes `extra->sjinfo`, which for a non-special join is a synthetic
/// JOIN_INNER sjinfo; the param-info seam only reads it for special-join shapes).
fn default_sjinfo() -> SpecialJoinInfo {
    SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: None,
        syn_righthand: None,
        jointype: types_pathnodes::JOIN_INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    }
}

/* ===========================================================================
 * Upper-rel path constructors (pathnode.c:3011-4239).
 * ======================================================================== */

/// `create_projection_path(root, rel, subpath, target)` (pathnode.c:2901).
pub fn create_projection_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    target: Box<PathTarget>,
) -> PgResult<PathId> {
    // Strip off any ProjectionPath in what we're given.
    let subpath = match root.path(subpath) {
        PathNode::ProjectionPath(subpp) => {
            debug_assert_eq!(subpp.path.parent, rel);
            let inner = subpp.subpath.ok_or_else(|| {
                PgError::error("create_projection_path: ProjectionPath::subpath must be set")
            })?;
            debug_assert!(!matches!(root.path(inner), PathNode::ProjectionPath(_)));
            inner
        }
        _ => subpath,
    };

    let sp: Path = root.path(subpath).base().clone();
    let subpath_projection_capable = seam::is_projection_capable_path::call(root, subpath);

    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;
    let target_exprs = target.exprs.clone();

    let old_startup = sp.pathtarget.as_ref().map_or(0.0, |t| t.cost.startup);
    let old_per_tuple = sp.pathtarget.as_ref().map_or(0.0, |t| t.cost.per_tuple);
    let exprs_equal = match &sp.pathtarget {
        Some(old) => seam::equal_exprs::call(root, &old.exprs, &target_exprs),
        None => target_exprs.is_empty(),
    };

    let parallel_safe = root.rel(rel).consider_parallel
        && sp.parallel_safe
        && seam::is_parallel_safe::call(root, &target_exprs);

    let mut path = base_path(T_PATH, T_RESULT, rel, Some(target));
    path.parent = rel;
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    let dummypp;
    if subpath_projection_capable || exprs_equal {
        dummypp = true;
        path.rows = sp.rows;
        path.disabled_nodes = sp.disabled_nodes;
        path.startup_cost = sp.startup_cost + (target_startup - old_startup);
        path.total_cost = sp.total_cost
            + (target_startup - old_startup)
            + (target_per_tuple - old_per_tuple) * sp.rows;
    } else {
        dummypp = false;
        let cpu_tuple_cost = seam::cpu_tuple_cost::call();
        path.rows = sp.rows;
        path.disabled_nodes = sp.disabled_nodes;
        path.startup_cost = sp.startup_cost + target_startup;
        path.total_cost =
            sp.total_cost + target_startup + (cpu_tuple_cost + target_per_tuple) * sp.rows;
    }

    Ok(root.alloc_path(PathNode::ProjectionPath(ProjectionPath {
        path,
        subpath: Some(subpath),
        dummypp,
    })))
}

/// `apply_projection_to_path(root, rel, path, target)` (pathnode.c:3011).
pub fn apply_projection_to_path(
    root: &mut PlannerInfo,
    rel: RelId,
    path: PathId,
    target: Box<PathTarget>,
) -> PgResult<PathId> {
    let base: Path = root.path(path).base().clone();
    if !seam::is_projection_capable_path::call(root, path) {
        return create_projection_path(root, rel, path, target);
    }

    let oldcost = base.pathtarget.as_ref().map_or(QualCost::default(), |t| t.cost);
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;
    let target_exprs = target.exprs.clone();
    let path_rows = base.rows;
    let target_for_pushdown = target.clone();

    {
        let p = root.path_mut(path).base_mut();
        p.pathtarget = Some(target);
        p.startup_cost += target_startup - oldcost.startup;
        p.total_cost +=
            target_startup - oldcost.startup + (target_per_tuple - oldcost.per_tuple) * path_rows;
    }

    let is_gather = matches!(root.path(path), PathNode::GatherPath(_));
    let is_gather_merge = matches!(root.path(path), PathNode::GatherMergePath(_));
    if (is_gather || is_gather_merge) && seam::is_parallel_safe::call(root, &target_exprs) {
        if is_gather {
            let gsub = match root.path(path) {
                PathNode::GatherPath(g) => g.subpath.ok_or_else(|| {
                    PgError::error("apply_projection_to_path: GatherPath::subpath must be set")
                })?,
                _ => unreachable!(),
            };
            let gsub_parent = root.path(gsub).base().parent;
            let new_sub = create_projection_path(root, gsub_parent, gsub, target_for_pushdown)?;
            if let PathNode::GatherPath(g) = root.path_mut(path) {
                g.subpath = Some(new_sub);
            }
        } else {
            let gsub = match root.path(path) {
                PathNode::GatherMergePath(g) => g.subpath.ok_or_else(|| {
                    PgError::error(
                        "apply_projection_to_path: GatherMergePath::subpath must be set",
                    )
                })?,
                _ => unreachable!(),
            };
            let gsub_parent = root.path(gsub).base().parent;
            let new_sub = create_projection_path(root, gsub_parent, gsub, target_for_pushdown)?;
            if let PathNode::GatherMergePath(g) = root.path_mut(path) {
                g.subpath = Some(new_sub);
            }
        }
    } else if base.parallel_safe && !seam::is_parallel_safe::call(root, &target_exprs) {
        root.path_mut(path).base_mut().parallel_safe = false;
    }

    Ok(path)
}

/// `create_set_projection_path(root, rel, subpath, target)` (pathnode.c:3100).
pub fn create_set_projection_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    target: Box<PathTarget>,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;
    let target_exprs = target.exprs.clone();

    let mut path = base_path(T_PROJECT_SET_PATH, T_PROJECT_SET, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel
        && sp.parallel_safe
        && seam::is_parallel_safe::call(root, &target_exprs);
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    let mut tlist_rows: f64 = 1.0;
    for node in target_exprs.iter() {
        let itemrows = seam::expression_returns_set_rows::call(root, *node);
        if tlist_rows < itemrows {
            tlist_rows = itemrows;
        }
    }

    let cpu_tuple_cost = seam::cpu_tuple_cost::call();
    path.disabled_nodes = sp.disabled_nodes;
    path.rows = sp.rows * tlist_rows;
    path.startup_cost = sp.startup_cost + target_startup;
    path.total_cost = sp.total_cost
        + target_startup
        + (cpu_tuple_cost + target_per_tuple) * sp.rows
        + (path.rows - sp.rows) * cpu_tuple_cost / 2.0;

    Ok(root.alloc_path(PathNode::ProjectSetPath(ProjectSetPath {
        path,
        subpath: Some(subpath),
    })))
}

/// `create_incremental_sort_path(...)` (pathnode.c:3170).
pub fn create_incremental_sort_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    pathkeys: Vec<PathKey>,
    presorted_keys: i32,
    limit_tuples: f64,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_INCREMENTAL_SORT_PATH, T_INCREMENTAL_SORT, rel, sp.pathtarget.clone());
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = pathkeys.clone();
    let width = pathtarget_width(&sp);
    let id = root.alloc_path(PathNode::IncrementalSortPath(IncrementalSortPath {
        spath: SortPath {
            path,
            subpath: Some(subpath),
        },
        nPresortedCols: presorted_keys,
    }));
    let wm = seam::work_mem::call();
    seam::cost_incremental_sort::call(
        root,
        id,
        &pathkeys,
        presorted_keys,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        sp.rows,
        width,
        0.0,
        wm,
        limit_tuples,
    );
    Ok(id)
}

/// `create_sort_path(...)` (pathnode.c:3220).
pub fn create_sort_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    pathkeys: Vec<PathKey>,
    limit_tuples: f64,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_SORT_PATH, T_SORT, rel, sp.pathtarget.clone());
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = pathkeys.clone();
    let width = pathtarget_width(&sp);
    let id = root.alloc_path(PathNode::SortPath(SortPath {
        path,
        subpath: Some(subpath),
    }));
    let wm = seam::work_mem::call();
    seam::cost_sort::call(
        root,
        id,
        &pathkeys,
        sp.disabled_nodes,
        sp.total_cost,
        sp.rows,
        width,
        0.0,
        wm,
        limit_tuples,
    );
    Ok(id)
}

/// `create_group_path(...)` (pathnode.c:3265).
pub fn create_group_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    group_clause: Vec<NodeId>,
    qual: Vec<NodeId>,
    num_groups: f64,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let target = rel_reltarget(root, rel);
    let target_startup = target.as_ref().map(|t| t.cost.startup).unwrap_or(0.0);
    let target_per_tuple = target.as_ref().map(|t| t.cost.per_tuple).unwrap_or(0.0);

    let mut path = base_path(T_GROUP_PATH, T_GROUP, rel, target);
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    let num_group_cols = group_clause.len() as i32;
    let id = root.alloc_path(PathNode::GroupPath(GroupPath {
        path,
        subpath: Some(subpath),
        groupClause: group_clause,
        qual: qual.clone(),
    }));
    seam::cost_group::call(
        root,
        id,
        num_group_cols,
        num_groups,
        &qual,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        sp.rows,
    );

    {
        let p = root.path_mut(id).base_mut();
        p.startup_cost += target_startup;
        p.total_cost += target_startup + target_per_tuple * p.rows;
    }

    Ok(id)
}

/// `create_upper_unique_path(...)` (pathnode.c:3325).
pub fn create_upper_unique_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    num_cols: i32,
    num_groups: f64,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_UPPER_UNIQUE_PATH, T_UNIQUE, rel, sp.pathtarget.clone());
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    path.disabled_nodes = sp.disabled_nodes;
    path.startup_cost = sp.startup_cost;
    path.total_cost =
        sp.total_cost + seam::cpu_operator_cost::call() * sp.rows * num_cols as f64;
    path.rows = num_groups;

    Ok(root.alloc_path(PathNode::UpperUniquePath(UpperUniquePath {
        path,
        subpath: Some(subpath),
        numkeys: num_cols,
    })))
}

/// `create_agg_path(...)` (pathnode.c:3378).
pub fn create_agg_path(
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
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;

    let mut path = base_path(T_AGG_PATH, T_AGG, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;

    if aggstrategy == AGG_SORTED {
        if sp.pathkeys.len() as i32 > root.num_groupby_pathkeys {
            let n = root.num_groupby_pathkeys.max(0) as usize;
            path.pathkeys = sp.pathkeys[..n].to_vec();
        } else {
            path.pathkeys = sp.pathkeys.clone();
        }
    } else {
        path.pathkeys = Vec::new();
    }

    let transition_space = aggcosts.map(|a| a.transition_space as u64).unwrap_or(0);
    let num_group_cols = group_clause.len() as i32;
    let width = pathtarget_width(&sp);
    let id = root.alloc_path(PathNode::AggPath(AggPath {
        path,
        subpath: Some(subpath),
        aggstrategy,
        aggsplit,
        numGroups: num_groups,
        transitionSpace: transition_space,
        groupClause: group_clause,
        qual: qual.clone(),
    }));
    seam::cost_agg::call(
        root,
        id,
        aggstrategy,
        aggcosts,
        num_group_cols,
        num_groups,
        &qual,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        sp.rows,
        width,
    );

    {
        let p = root.path_mut(id).base_mut();
        p.startup_cost += target_startup;
        p.total_cost += target_startup + target_per_tuple * p.rows;
    }

    Ok(id)
}

/// `create_groupingsets_path(...)` (pathnode.c:3461). Not seam-exported (no
/// joinpath/util caller binds it yet) but ported in full for completeness.
pub fn create_groupingsets_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    having_qual: Vec<NodeId>,
    mut aggstrategy: AggStrategy,
    rollups: Vec<types_pathnodes::RollupData>,
    agg_costs: Option<AggClauseCostsLite>,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let target = rel_reltarget(root, rel);
    let target_startup = target.as_ref().map(|t| t.cost.startup).unwrap_or(0.0);
    let target_per_tuple = target.as_ref().map(|t| t.cost.per_tuple).unwrap_or(0.0);
    let width = pathtarget_width(&sp);

    let mut path = base_path(T_GROUPING_SETS_PATH, T_AGG, rel, target);
    path.param_info = sp.param_info.clone();
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;

    if aggstrategy == AGG_SORTED && rollups.len() == 1 && rollups[0].groupClause.is_empty() {
        aggstrategy = AGG_PLAIN;
    }
    if aggstrategy == AGG_MIXED && rollups.len() == 1 {
        aggstrategy = AGG_HASHED;
    }

    if aggstrategy == AGG_SORTED && rollups.len() == 1 {
        path.pathkeys = root.group_pathkeys.clone();
    } else {
        path.pathkeys = Vec::new();
    }

    let transition_space = agg_costs.map(|a| a.transition_space as u64).unwrap_or(0);
    debug_assert!(!rollups.is_empty());

    let id = root.alloc_path(PathNode::GroupingSetsPath(GroupingSetsPath {
        path,
        subpath: Some(subpath),
        aggstrategy,
        rollups: rollups.clone(),
        qual: having_qual.clone(),
        transitionSpace: transition_space,
    }));

    let mut is_first = true;
    let mut is_first_sort = true;
    for rollup in &rollups {
        let num_group_cols = rollup.gsets.first().map(|g| g.len()).unwrap_or(0) as i32;

        if is_first {
            let (dn, sc, tc, rw) = (sp.disabled_nodes, sp.startup_cost, sp.total_cost, sp.rows);
            seam::cost_agg::call(
                root, id, aggstrategy, agg_costs, num_group_cols, rollup.numGroups, &having_qual,
                dn, sc, tc, rw, width,
            );
            is_first = false;
            if !rollup.is_hashed {
                is_first_sort = false;
            }
        } else {
            let agg_dummy = base_path(T_PATH, T_AGG, rel, None);
            let agg_id = root.alloc_path(PathNode::Path(agg_dummy));
            if rollup.is_hashed || is_first_sort {
                let strat = if rollup.is_hashed { AGG_HASHED } else { AGG_SORTED };
                seam::cost_agg::call(
                    root, agg_id, strat, agg_costs, num_group_cols, rollup.numGroups, &having_qual,
                    0, 0.0, 0.0, sp.rows, width,
                );
                if !rollup.is_hashed {
                    is_first_sort = false;
                }
            } else {
                let sort_dummy = base_path(T_PATH, T_SORT, rel, None);
                let sort_id = root.alloc_path(PathNode::Path(sort_dummy));
                let wm = seam::work_mem::call();
                seam::cost_sort::call(
                    root, sort_id, &[], 0, 0.0, sp.rows, width, 0.0, wm, -1.0,
                );
                let s = root.path(sort_id).base().clone();
                seam::cost_agg::call(
                    root, agg_id, AGG_SORTED, agg_costs, num_group_cols, rollup.numGroups,
                    &having_qual, s.disabled_nodes, s.startup_cost, s.total_cost, s.rows, width,
                );
            }
            let a = root.path(agg_id).base().clone();
            let p = root.path_mut(id).base_mut();
            p.disabled_nodes += a.disabled_nodes;
            p.total_cost += a.total_cost;
            p.rows += a.rows;
        }
    }

    {
        let p = root.path_mut(id).base_mut();
        p.startup_cost += target_startup;
        p.total_cost += target_startup + target_per_tuple * p.rows;
    }

    Ok(id)
}

/// `create_minmaxagg_path(...)` (pathnode.c:3624).
pub fn create_minmaxagg_path(
    root: &mut PlannerInfo,
    rel: RelId,
    target: Box<PathTarget>,
    mmaggregates: Vec<MinMaxAggInfo>,
    quals: Vec<NodeId>,
) -> PgResult<PathId> {
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;
    let target_exprs = target.exprs.clone();

    let mut path = base_path(T_MIN_MAX_AGG_PATH, T_RESULT, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = true;
    path.parallel_workers = 0;
    path.rows = 1.0;
    path.pathkeys = Vec::new();

    let mut initplan_cost: Cost = 0.0;
    let mut initplan_disabled_nodes: i32 = 0;
    for mminfo in &mmaggregates {
        if let Some(pid) = mminfo.path {
            let mp = root.path(pid).base();
            initplan_disabled_nodes += mp.disabled_nodes;
            if !mp.parallel_safe {
                path.parallel_safe = false;
            }
        }
        initplan_cost += mminfo.pathcost;
    }

    let cpu_tuple_cost = seam::cpu_tuple_cost::call();
    path.disabled_nodes = initplan_disabled_nodes;
    path.startup_cost = initplan_cost + target_startup;
    path.total_cost = initplan_cost + target_startup + target_per_tuple + cpu_tuple_cost;

    if !quals.is_empty() {
        let qc = seam::cost_qual_eval::call(root, &quals);
        path.startup_cost += qc.startup;
        path.total_cost += qc.startup + qc.per_tuple;
    }

    if path.parallel_safe {
        path.parallel_safe = seam::is_parallel_safe::call(root, &target_exprs)
            && seam::is_parallel_safe_quals::call(root, &quals);
    }

    Ok(root.alloc_path(PathNode::MinMaxAggPath(MinMaxAggPath {
        path,
        mmaggregates,
        quals,
    })))
}

/// `create_windowagg_path(...)` (pathnode.c:3715).
pub fn create_windowagg_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    target: Box<PathTarget>,
    window_funcs: Vec<NodeId>,
    run_condition: Vec<NodeId>,
    winclause: NodeId,
    qual: Vec<NodeId>,
    topwindow: bool,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let target_startup = target.cost.startup;
    let target_per_tuple = target.cost.per_tuple;

    let mut path = base_path(T_WINDOW_AGG_PATH, T_WINDOW_AGG, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.pathkeys = sp.pathkeys.clone();

    let id = root.alloc_path(PathNode::WindowAggPath(WindowAggPath {
        path,
        subpath: Some(subpath),
        winclause,
        qual,
        runCondition: run_condition,
        topwindow,
    }));
    seam::cost_windowagg::call(
        root,
        id,
        &window_funcs,
        winclause,
        sp.disabled_nodes,
        sp.startup_cost,
        sp.total_cost,
        sp.rows,
    );

    {
        let p = root.path_mut(id).base_mut();
        p.startup_cost += target_startup;
        p.total_cost += target_startup + target_per_tuple * p.rows;
    }

    Ok(id)
}

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
) -> PgResult<PathId> {
    let lp: Path = root.path(leftpath).base().clone();
    let rp: Path = root.path(rightpath).base().clone();

    let mut path = base_path(T_SET_OP_PATH, T_SET_OP, rel, rel_reltarget(root, rel));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe =
        root.rel(rel).consider_parallel && lp.parallel_safe && rp.parallel_safe;
    path.parallel_workers = lp.parallel_workers + rp.parallel_workers;
    path.pathkeys = if strategy == SETOP_SORTED {
        lp.pathkeys.clone()
    } else {
        Vec::new()
    };

    let group_len = group_list.len() as f64;
    let cpu_operator_cost = seam::cpu_operator_cost::call();
    path.disabled_nodes = lp.disabled_nodes + rp.disabled_nodes;
    if strategy == SETOP_SORTED {
        path.startup_cost = lp.startup_cost + rp.startup_cost;
        path.total_cost =
            lp.total_cost + rp.total_cost + cpu_operator_cost * (lp.rows + rp.rows) * group_len;
        path.total_cost += cpu_operator_cost * output_rows;
    } else {
        path.startup_cost =
            lp.total_cost + rp.total_cost + cpu_operator_cost * (lp.rows + rp.rows) * group_len;
        path.total_cost = path.startup_cost;
        path.total_cost += cpu_operator_cost * output_rows;

        if !seam::enable_hashagg::call() {
            path.disabled_nodes += 1;
        }
        let lwidth = pathtarget_width(&lp);
        let hashentrysize = maxalign(lwidth as usize)
            + maxalign(seam::sizeof_minimal_tuple_header::call());
        if hashentrysize as f64 * num_groups > seam::get_hash_memory_limit::call() {
            path.disabled_nodes += 1;
        }
    }
    path.rows = output_rows;

    Ok(root.alloc_path(PathNode::SetOpPath(SetOpPath {
        path,
        leftpath: Some(leftpath),
        rightpath: Some(rightpath),
        cmd,
        strategy,
        groupList: group_list,
        numGroups: num_groups,
    })))
}

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
) -> PgResult<PathId> {
    let lp: Path = root.path(leftpath).base().clone();
    let rp: Path = root.path(rightpath).base().clone();

    let mut path = base_path(T_RECURSIVE_UNION_PATH, T_RECURSIVE_UNION, rel, Some(target));
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe =
        root.rel(rel).consider_parallel && lp.parallel_safe && rp.parallel_safe;
    path.parallel_workers = lp.parallel_workers;
    path.pathkeys = Vec::new();

    let id = root.alloc_path(PathNode::RecursiveUnionPath(RecursiveUnionPath {
        path,
        leftpath: Some(leftpath),
        rightpath: Some(rightpath),
        distinctList: distinct_list,
        wtParam: wt_param,
        numGroups: num_groups,
    }));
    seam::cost_recursive_union::call(root, id, leftpath, rightpath);

    Ok(id)
}

/// `create_lockrows_path(...)` (pathnode.c:3951).
pub fn create_lockrows_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    row_marks: Vec<NodeId>,
    epq_param: i32,
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_LOCK_ROWS_PATH, T_LOCK_ROWS, rel, sp.pathtarget.clone());
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = false;
    path.parallel_workers = 0;
    path.rows = sp.rows;
    path.pathkeys = Vec::new();

    path.disabled_nodes = sp.disabled_nodes;
    path.startup_cost = sp.startup_cost;
    path.total_cost = sp.total_cost + seam::cpu_tuple_cost::call() * sp.rows;

    Ok(root.alloc_path(PathNode::LockRowsPath(LockRowsPath {
        path,
        subpath: Some(subpath),
        rowMarks: row_marks,
        epqParam: epq_param,
    })))
}

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
) -> PgResult<PathId> {
    debug_assert!(
        operation == CMD_MERGE
            || if operation == CMD_UPDATE {
                result_relations.len() == update_colnos_lists.len()
            } else {
                update_colnos_lists.is_empty()
            }
    );
    debug_assert!(
        with_check_option_lists.is_empty()
            || result_relations.len() == with_check_option_lists.len()
    );
    debug_assert!(
        returning_lists.is_empty() || result_relations.len() == returning_lists.len()
    );

    let sp: Path = root.path(subpath).base().clone();
    let mut pathtarget = rel_reltarget(root, rel);

    let mut path = base_path(T_MODIFY_TABLE_PATH, T_MODIFY_TABLE, rel, None);
    path.parallel_aware = false;
    path.parallel_safe = false;
    path.parallel_workers = 0;
    path.pathkeys = Vec::new();

    path.disabled_nodes = sp.disabled_nodes;
    path.startup_cost = sp.startup_cost;
    path.total_cost = sp.total_cost;
    let returning = !returning_lists.is_empty();
    if returning {
        path.rows = sp.rows;
        let sub_width = pathtarget_width(&sp);
        if let Some(t) = pathtarget.as_mut() {
            t.width = sub_width;
        }
    } else {
        path.rows = 0.0;
        if let Some(t) = pathtarget.as_mut() {
            t.width = 0;
        }
    }
    path.pathtarget = pathtarget;

    Ok(root.alloc_path(PathNode::ModifyTablePath(ModifyTablePath {
        path,
        subpath: Some(subpath),
        operation,
        canSetTag: can_set_tag,
        nominalRelation: nominal_relation,
        rootRelation: root_relation,
        partColsUpdated: part_cols_updated,
        resultRelations: result_relations,
        updateColnosLists: update_colnos_lists,
        withCheckOptionLists: with_check_option_lists,
        returningLists: returning_lists,
        rowMarks: row_marks,
        onconflict,
        epqParam: epq_param,
        mergeActionLists: merge_action_lists,
        mergeJoinConditions: merge_join_conditions,
    })))
}

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
) -> PgResult<PathId> {
    let sp: Path = root.path(subpath).base().clone();
    let mut path = base_path(T_LIMIT_PATH, T_LIMIT, rel, sp.pathtarget.clone());
    path.param_info = None;
    path.parallel_aware = false;
    path.parallel_safe = root.rel(rel).consider_parallel && sp.parallel_safe;
    path.parallel_workers = sp.parallel_workers;
    path.rows = sp.rows;
    path.disabled_nodes = sp.disabled_nodes;
    path.startup_cost = sp.startup_cost;
    path.total_cost = sp.total_cost;
    path.pathkeys = sp.pathkeys.clone();

    let (rows, startup, total) = adjust_limit_rows_costs(
        path.rows,
        path.startup_cost,
        path.total_cost,
        offset_est,
        count_est,
    );
    path.rows = rows;
    path.startup_cost = startup;
    path.total_cost = total;

    Ok(root.alloc_path(PathNode::LimitPath(LimitPath {
        path,
        subpath: Some(subpath),
        limitOffset: limit_offset,
        limitCount: limit_count,
        limitOption: limit_option,
    })))
}

/// `adjust_limit_rows_costs(...)` (pathnode.c:4173) — returns the adjusted
/// `(rows, startup, total)`.
pub fn adjust_limit_rows_costs(
    mut rows: f64,
    mut startup_cost: Cost,
    mut total_cost: Cost,
    offset_est: i64,
    count_est: i64,
) -> (f64, Cost, Cost) {
    let input_rows = rows;
    let input_startup_cost = startup_cost;
    let input_total_cost = total_cost;

    if offset_est != 0 {
        let mut offset_rows = if offset_est > 0 {
            offset_est as f64
        } else {
            clamp_row_est(input_rows * 0.10)
        };
        if offset_rows > rows {
            offset_rows = rows;
        }
        if input_rows > 0.0 {
            startup_cost += (input_total_cost - input_startup_cost) * offset_rows / input_rows;
        }
        rows -= offset_rows;
        if rows < 1.0 {
            rows = 1.0;
        }
    }

    if count_est != 0 {
        let mut count_rows = if count_est > 0 {
            count_est as f64
        } else {
            clamp_row_est(input_rows * 0.10)
        };
        if count_rows > rows {
            count_rows = rows;
        }
        if input_rows > 0.0 {
            total_cost =
                startup_cost + (input_total_cost - input_startup_cost) * count_rows / input_rows;
        }
        rows = count_rows;
        if rows < 1.0 {
            rows = 1.0;
        }
    }

    (rows, startup_cost, total_cost)
}

/* ===========================================================================
 * create_unique_path + reparameterization (pathnode.c:1730 + 4242-4884).
 *
 * create_unique_path / reparameterize_path / reparameterize_path_by_child reach
 * deep into catalog (lsyscache), plancat, analyzejoins, pathkeys.c, and the
 * `adjust_appendrel_attrs` expression mutator — none of which are ported in this
 * wave. Their cross-subsystem bodies are seam-and-panic'd through the dedicated
 * mutator seams below (declared here as outward seams so a faithful `panic!`
 * fires only when the missing owner is actually invoked, never a silent stub).
 *
 * `path_is_reparameterizable_by_child` + the pathlist helper are pure structural
 * walks over the arena `PathNode` variants and are ported 1:1.
 * ======================================================================== */

/// `create_unique_path(root, rel, subpath, sjinfo)` (pathnode.c:1730).
///
/// Reaches `get_ordering_op_for_equality_op` / `get_equality_op_for_ordering_op`
/// (lsyscache), `relation_has_unique_index_for` (plancat), `query_is_distinct_for`
/// (analyzejoins), `make_pathkeys_for_sortclauses` (pathkeys.c), and constructs
/// `TargetEntry`/`SortGroupClause` over `semi_rhs_exprs`. None ported in this
/// wave — the whole body crosses the dedicated unique seam (panics until the
/// pathkeys/catalog owners land).
pub fn create_unique_path(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<Option<PathId>> {
    unique_seam::create_unique_path::call(root, rel, subpath, sjinfo)
}

/// `install_dummy_append_path(root, rel)` — the pathnode-side body of joinrels.c's
/// `mark_dummy_rel` (joinrels.c:1324), minus the already-marked early-out (ported
/// in the joinrels consumer). Evicts the rel's paths, sets `rows = 0`, installs a
/// childless dummy `create_append_path` (C `create_append_path(NULL, rel, NIL, NIL,
/// NIL, rel->lateral_relids, 0, false, -1)`), and re-runs `set_cheapest`.
///
/// The `MemoryContextSwitchTo(GetMemoryChunkContext(rel))` dance around the C body
/// is a no-op in the arena/`PlannerInfo` model (paths live in the planner arena,
/// not a per-rel chunk context), so it is dropped behaviour-preservingly.
pub fn install_dummy_append_path(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    // Set dummy size estimate.
    root.rel_mut(rel).rows = 0.0;

    // Evict any previously chosen paths.
    root.rel_mut(rel).pathlist.clear();
    root.rel_mut(rel).partial_pathlist.clear();

    // Set up the dummy path: a childless Append over the rel's lateral_relids.
    let lateral_relids = root.rel(rel).lateral_relids.clone();
    let dummy = create_append_path(
        root,
        /* have_root = */ false,
        rel,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &lateral_relids,
        0,
        false,
        -1.0,
    )?;
    crate::add_path(root, rel, dummy)?;

    // Set or update cheapest_total_path and related fields.
    crate::set_cheapest(root, rel)
}

/// `reparameterize_path(root, path, required_outer, loop_count)`
/// (pathnode.c:4242). Re-derives a path's clauses + re-runs the cost model over
/// real RestrictInfo/expression nodes — crosses the reparam seam (panics until
/// the indxpath/cost/clause owners land).
pub fn reparameterize_path(
    root: &mut PlannerInfo,
    path: PathId,
    required_outer: &Relids,
    loop_count: f64,
) -> PgResult<Option<PathId>> {
    unique_seam::reparameterize_path::call(root, path, required_outer, loop_count)
}

/// `reparameterize_path_by_child(root, path, child_rel)` (pathnode.c:4408). The
/// heart is `adjust_appendrel_attrs` over the path's clauses/quals/orderbys (an
/// `expression_tree_mutator` over the whole node vocabulary) + FDW/Custom
/// reparameterization callbacks — crosses the reparam seam.
pub fn reparameterize_path_by_child(
    root: &mut PlannerInfo,
    path: PathId,
    child_rel: RelId,
) -> PgResult<Option<PathId>> {
    unique_seam::reparameterize_path_by_child::call(root, path, child_rel)
}

/// `path_is_reparameterizable_by_child(path, child_rel)` (pathnode.c:4704) — a
/// pure structural pre-check mirroring `reparameterize_path_by_child`'s node
/// walk. Ported 1:1 over the arena.
pub fn path_is_reparameterizable_by_child(
    root: &PlannerInfo,
    path: PathId,
    child_rel: RelId,
) -> bool {
    let base = root.path(path).base();

    // If not parameterized by the parent of the given relation, no reparam needed.
    if base.param_info.is_none()
        || !bms::relids_overlap::call(
            &path_req_outer(base),
            &root.rel(child_rel).top_parent_relids,
        )
    {
        return true;
    }

    match root.path(path) {
        PathNode::Path(_) | PathNode::IndexPath(_) => true,
        PathNode::BitmapHeapPath(bhpath) => match bhpath.bitmapqual {
            Some(bq) => path_is_reparameterizable_by_child(root, bq, child_rel),
            None => true,
        },
        PathNode::BitmapAndPath(bapath) => {
            pathlist_is_reparameterizable_by_child(root, &bapath.bitmapquals, child_rel)
        }
        PathNode::BitmapOrPath(bopath) => {
            pathlist_is_reparameterizable_by_child(root, &bopath.bitmapquals, child_rel)
        }
        PathNode::ForeignPath(fpath) => match fpath.fdw_outerpath {
            Some(op) => path_is_reparameterizable_by_child(root, op, child_rel),
            None => true,
        },
        PathNode::CustomPath(cpath) => {
            pathlist_is_reparameterizable_by_child(root, &cpath.custom_paths, child_rel)
        }
        PathNode::NestPath(jp) => {
            jpath_reparameterizable(root, &jp.jpath, child_rel)
        }
        PathNode::MergePath(jp) => jpath_reparameterizable(root, &jp.jpath, child_rel),
        PathNode::HashPath(jp) => jpath_reparameterizable(root, &jp.jpath, child_rel),
        PathNode::AppendPath(apath) => {
            pathlist_is_reparameterizable_by_child(root, &apath.subpaths, child_rel)
        }
        PathNode::MaterialPath(mpath) => match mpath.subpath {
            Some(sp) => path_is_reparameterizable_by_child(root, sp, child_rel),
            None => true,
        },
        PathNode::MemoizePath(mpath) => match mpath.subpath {
            Some(sp) => path_is_reparameterizable_by_child(root, sp, child_rel),
            None => true,
        },
        PathNode::GatherPath(gpath) => match gpath.subpath {
            Some(sp) => path_is_reparameterizable_by_child(root, sp, child_rel),
            None => true,
        },
        // We don't know how to reparameterize this path.
        _ => false,
    }
}

/// `REJECT_IF_PATH_NOT_REPARAMETERIZABLE` for both join subpaths.
fn jpath_reparameterizable(root: &PlannerInfo, jpath: &JoinPath, child_rel: RelId) -> bool {
    match jpath.outerjoinpath {
        Some(op) if !path_is_reparameterizable_by_child(root, op, child_rel) => return false,
        _ => {}
    }
    match jpath.innerjoinpath {
        Some(ip) if !path_is_reparameterizable_by_child(root, ip, child_rel) => return false,
        _ => {}
    }
    true
}

/// `pathlist_is_reparameterizable_by_child(pathlist, child_rel)` (pathnode.c:4864).
pub fn pathlist_is_reparameterizable_by_child(
    root: &PlannerInfo,
    pathlist: &[PathId],
    child_rel: RelId,
) -> bool {
    for &p in pathlist {
        if !path_is_reparameterizable_by_child(root, p, child_rel) {
            return false;
        }
    }
    true
}

/// `reparameterize_pathlist_by_child(root, pathlist, child_rel)`
/// (pathnode.c:4835). Maps `reparameterize_path_by_child` over a list; returns
/// `None` to indicate failure (the C `NIL`).
pub fn reparameterize_pathlist_by_child(
    root: &mut PlannerInfo,
    pathlist: &[PathId],
    child_rel: RelId,
) -> PgResult<Option<Vec<PathId>>> {
    let mut result: Vec<PathId> = Vec::new();
    for &p in pathlist {
        match reparameterize_path_by_child(root, p, child_rel)? {
            Some(np) => result.push(np),
            None => return Ok(None),
        }
    }
    Ok(Some(result))
}

/// Outward seams for the genuinely-unported cross-subsystem bodies of
/// `create_unique_path` / `reparameterize_path{,_by_child}` (lsyscache / plancat
/// / analyzejoins / pathkeys.c / `adjust_appendrel_attrs`). Declared here (not in
/// the inward `-seams` crate) because pathnode is their *consumer* for these
/// pieces; each panics until the owning unit installs it.
mod unique_seam {
    use super::*;

    seam_core::seam!(
        /// pathnode.c:1730 cross-subsystem body of `create_unique_path`.
        pub fn create_unique_path(
            root: &mut PlannerInfo,
            rel: RelId,
            subpath: PathId,
            sjinfo: &SpecialJoinInfo,
        ) -> PgResult<Option<PathId>>
    );
    seam_core::seam!(
        /// pathnode.c:4242 cross-subsystem body of `reparameterize_path`.
        pub fn reparameterize_path(
            root: &mut PlannerInfo,
            path: PathId,
            required_outer: &Relids,
            loop_count: f64,
        ) -> PgResult<Option<PathId>>
    );
    seam_core::seam!(
        /// pathnode.c:4408 cross-subsystem body of `reparameterize_path_by_child`.
        pub fn reparameterize_path_by_child(
            root: &mut PlannerInfo,
            path: PathId,
            child_rel: RelId,
        ) -> PgResult<Option<PathId>>
    );
}

// Keep InvalidOid referenced (used by future unique-path equality-op handling
// once the catalog seams land; documents the OID-validity domain).
#[allow(dead_code)]
const _INVALID_OID: Oid = InvalidOid;
