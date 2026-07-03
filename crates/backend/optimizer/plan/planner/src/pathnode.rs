use std::rc::Rc;

use mcx::PgVec;
use types_error::{PgError, PgResult};
use types_nodes::list::NodeList;
use types_nodes::NodeTag;
use types_pathnodes::{
    GroupResultPath, IndexClause, IndexOptInfo, IndexPath, Path, PathId, PathKey, PathNode,
    PathTarget, ProjectionPath, PtId, RelId, ScanDirection,
};

use crate::costsize::{clamp_width_est, cost_qual_eval_node};
use crate::gucs;
use crate::run::PlannerRun;

pub fn tag16(tag: NodeTag) -> u16 {
    tag as u16
}

// create_pathtarget (tlist.c): make_pathtarget_from_tlist +
// set_pathtarget_cost_width (costsize.c).
pub fn create_pathtarget<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<PtId> {
    let mcx = run.mcx;
    let mut target = PathTarget::new(mcx);
    let mut any_sortgroupref = false;

    for tle_node in tlist {
        let tle = tle_node
            .as_target_entry()
            .expect("targetList cell is a TargetEntry");
        if tle.expr.node_tag() != NodeTag::T_Var {
            let cost = cost_qual_eval_node(tle.expr)?;
            target.cost.startup += cost.startup;
            target.cost.per_tuple += cost.per_tuple;
        }
        let id = run.intern_expr(tle.expr);
        target.exprs.push(id);
        target.sortgrouprefs.push(tle.ressortgroupref);
        any_sortgroupref |= tle.ressortgroupref != 0;
    }
    if !any_sortgroupref {
        target.sortgrouprefs.clear();
    }
    let id = run.root.alloc_pathtarget(target);
    let mut tuple_width: i64 = 0;
    for i in 0..run.root.pathtarget(id).exprs.len() {
        let expr = run.root.pathtarget(id).exprs[i];
        tuple_width += crate::costsize::get_expr_width(run, expr)? as i64;
    }
    run.root.pathtarget_mut(id).width = clamp_width_est(tuple_width);
    Ok(id)
}

// create_group_result_path (pathnode.c); quals arm needs cost_qual_eval.
pub fn create_group_result_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    target_id: PtId,
    havingqual: PgVec<'mcx, types_pathnodes::NodeId>,
) -> PathNode<'mcx> {
    if !havingqual.is_empty() {
        panic!("create_group_result_path (pathnode.c): quals cost; M2 qual lane");
    }
    let target = run.root.pathtarget(target_id);
    let (t_startup, t_per_tuple) = (target.cost.startup, target.cost.per_tuple);
    let rel = run.root.rel(rel_id);
    PathNode::GroupResultPath(GroupResultPath {
        path: Path {
            type_: tag16(NodeTag::T_GroupResultPath),
            pathtype: tag16(NodeTag::T_Result),
            parent: rel_id,
            pathtarget_id: Some(target_id),
            param_info: None,
            parallel_aware: false,
            parallel_safe: rel.consider_parallel,
            parallel_workers: 0,
            rows: 1.0,
            disabled_nodes: 0,
            startup_cost: t_startup,
            total_cost: t_startup + gucs::cpu_tuple_cost() + t_per_tuple,
            pathkeys: PgVec::new_in(run.mcx),
        },
        quals: havingqual,
    })
}

// is_projection_capable_path (createplan.c), keyed on pathtype like C.
pub fn is_projection_capable_pathtype(pathtype: u16) -> bool {
    match pathtype {
        t if t == tag16(NodeTag::T_Result) => true,
        t if t == tag16(NodeTag::T_SeqScan) => true,
        t if t == tag16(NodeTag::T_IndexScan) => true,
        t if t == tag16(NodeTag::T_IndexOnlyScan) => true,
        t if t == tag16(NodeTag::T_NestLoop) => true,
        _ => panic!(
            "is_projection_capable_path (createplan.c): pathtype {pathtype}; \
             M2 plan lane"
        ),
    }
}

pub fn exprs_same(
    run: &PlannerRun<'_>,
    a: &PgVec<'_, types_pathnodes::NodeId>,
    b: &PgVec<'_, types_pathnodes::NodeId>,
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if a.as_slice() == b.as_slice() {
        return true;
    }
    a.iter()
        .zip(b.iter())
        .all(|(&x, &y)| types_nodes::equal(*run.root.expr_node(x), *run.root.expr_node(y)))
}

// create_projection_path (pathnode.c).
pub fn create_projection_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    target_id: PtId,
    target_parallel_safe: bool,
) -> PathNode<'mcx> {
    if matches!(run.root.path(subpath_id), PathNode::ProjectionPath(_)) {
        panic!("create_projection_path (pathnode.c): ProjectionPath stripping; unreachable pre-M2");
    }
    let sub = run.root.path(subpath_id).base();
    let old_target_id = sub.pathtarget_id.expect("subpath has a pathtarget");
    let dummypp = is_projection_capable_pathtype(sub.pathtype)
        || exprs_same(
            run,
            &run.root.pathtarget(old_target_id).exprs,
            &run.root.pathtarget(target_id).exprs,
        );

    let sub = run.root.path(subpath_id).base();
    let oldt = run.root.pathtarget(old_target_id);
    let newt = run.root.pathtarget(target_id);
    let rel = run.root.rel(rel_id);
    let mut path = Path {
        type_: tag16(NodeTag::T_ProjectionPath),
        pathtype: tag16(NodeTag::T_Result),
        parent: rel_id,
        pathtarget_id: Some(target_id),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe && target_parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows: sub.rows,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: crate::relnode::pgvec_clone_shallow(run.mcx, &sub.pathkeys),
    };
    if dummypp {
        path.startup_cost = sub.startup_cost + (newt.cost.startup - oldt.cost.startup);
        path.total_cost = sub.total_cost
            + (newt.cost.startup - oldt.cost.startup)
            + (newt.cost.per_tuple - oldt.cost.per_tuple) * sub.rows;
    } else {
        path.startup_cost = sub.startup_cost + newt.cost.startup;
        path.total_cost = sub.total_cost
            + newt.cost.startup
            + (gucs::cpu_tuple_cost() + newt.cost.per_tuple) * sub.rows;
    }
    PathNode::ProjectionPath(ProjectionPath { path, subpath: Some(subpath_id), dummypp })
}

// create_modifytable_path (pathnode.c), single-relation INSERT/UPDATE/DELETE
// arm: no RETURNING/WCO/rowmarks/ON CONFLICT/MERGE lists (loud upstream),
// rows = 0.
pub fn create_modifytable_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    operation: types_nodes::nodes_enums::CmdType,
    can_set_tag: bool,
    // (operation is stored as the C CmdType value; types_pathnodes uses u32)
    result_relation: u32,
    update_colnos: Option<PgVec<'mcx, i16>>,
) -> PathNode<'mcx> {
    let sub = run.root.path(subpath_id).base();
    let path = Path {
        type_: tag16(NodeTag::T_ModifyTablePath),
        pathtype: tag16(NodeTag::T_ModifyTable),
        parent: rel_id,
        // C reuses rel->reltarget and zeroes its width; the upper rel target
        // is unset here and copy_generic_path_info reads width 0 from None.
        pathtarget_id: run.root.rel(rel_id).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: sub.startup_cost,
        total_cost: sub.total_cost,
        pathkeys: PgVec::new_in(run.mcx),
    };
    let mut result_relations = PgVec::new_in(run.mcx);
    result_relations.push(result_relation as i32);
    PathNode::ModifyTablePath(types_pathnodes::ModifyTablePath {
        path,
        subpath: Some(subpath_id),
        operation: operation as u32,
        canSetTag: can_set_tag,
        nominalRelation: result_relation,
        rootRelation: 0,
        partColsUpdated: false,
        resultRelations: result_relations,
        updateColnosLists: match update_colnos {
            Some(colnos) => {
                let mut lists = PgVec::new_in(run.mcx);
                lists.push(colnos);
                lists
            }
            None => PgVec::new_in(run.mcx),
        },
        withCheckOptionLists: PgVec::new_in(run.mcx),
        returningLists: PgVec::new_in(run.mcx),
        rowMarks: PgVec::new_in(run.mcx),
        onconflict: None,
        epqParam: 0,
        mergeActionLists: PgVec::new_in(run.mcx),
        mergeJoinConditions: PgVec::new_in(run.mcx),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CostSelector {
    Startup,
    Total,
}
pub fn compare_path_costs(path1: &Path<'_>, path2: &Path<'_>, criterion: CostSelector) -> i32 {
    if path1.disabled_nodes != path2.disabled_nodes {
        return if path1.disabled_nodes < path2.disabled_nodes { -1 } else { 1 };
    }
    let (a1, b1, a2, b2) = match criterion {
        CostSelector::Startup => {
            (path1.startup_cost, path2.startup_cost, path1.total_cost, path2.total_cost)
        }
        CostSelector::Total => {
            (path1.total_cost, path2.total_cost, path1.startup_cost, path2.startup_cost)
        }
    };
    if a1 < b1 {
        return -1;
    }
    if a1 > b1 {
        return 1;
    }
    if a2 < b2 {
        return -1;
    }
    if a2 > b2 {
        return 1;
    }
    0
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PathCostComparison {
    Equal,
    Better1,
    Better2,
    Different,
}

const STD_FUZZ_FACTOR: f64 = 1.01;

// compare_path_costs_fuzzily (pathnode.c); the parent rel's two consider
// flags arrive by value.
fn compare_path_costs_fuzzily(
    path1: &Path<'_>,
    path2: &Path<'_>,
    fuzz_factor: f64,
    consider_startup: bool,
    consider_param_startup: bool,
) -> PathCostComparison {
    let consider = |p: &Path<'_>| {
        if p.param_info.is_none() {
            consider_startup
        } else {
            consider_param_startup
        }
    };
    if path1.disabled_nodes != path2.disabled_nodes {
        return if path1.disabled_nodes < path2.disabled_nodes {
            PathCostComparison::Better1
        } else {
            PathCostComparison::Better2
        };
    }
    if path1.total_cost > path2.total_cost * fuzz_factor {
        if consider(path1) && path2.startup_cost > path1.startup_cost * fuzz_factor {
            return PathCostComparison::Different;
        }
        return PathCostComparison::Better2;
    }
    if path2.total_cost > path1.total_cost * fuzz_factor {
        if consider(path2) && path1.startup_cost > path2.startup_cost * fuzz_factor {
            return PathCostComparison::Different;
        }
        return PathCostComparison::Better1;
    }
    if path1.startup_cost > path2.startup_cost * fuzz_factor {
        return PathCostComparison::Better2;
    }
    if path2.startup_cost > path1.startup_cost * fuzz_factor {
        return PathCostComparison::Better1;
    }
    PathCostComparison::Equal
}

// add_path (pathnode.c); with no parameterized paths, PATH_REQ_OUTER
// comparisons collapse to BMS_EQUAL.
pub fn add_path<'mcx>(run: &mut PlannerRun<'mcx>, rel_id: RelId, new_id: PathId) -> PathId {
    let mut accept_new = true;
    let mut insert_at = 0usize;

    let consider_startup = run.root.rel(rel_id).consider_startup;
    let consider_param_startup = run.root.rel(rel_id).consider_param_startup;

    let empty: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    let mut working = core::mem::replace(&mut run.root.rel_mut(rel_id).pathlist, empty);

    debug_assert!(run.root.path(new_id).base().param_info.is_none());

    let mut i = 0usize;
    while i < working.len() {
        let new_path = run.root.path(new_id).base();
        let old_path = run.root.path(working[i]).base();
        let mut remove_old = false;

        let costcmp = compare_path_costs_fuzzily(
            new_path,
            old_path,
            STD_FUZZ_FACTOR,
            consider_startup,
            consider_param_startup,
        );

        if costcmp != PathCostComparison::Different {
            use crate::pathkeys::{compare_pathkeys, PathKeysComparison};
            let keyscmp = compare_pathkeys(&new_path.pathkeys, &old_path.pathkeys);
            if keyscmp != PathKeysComparison::Different {
                match costcmp {
                    PathCostComparison::Equal => match keyscmp {
                        // Outer relids are BMS_EQUAL (no parameterized paths).
                        PathKeysComparison::Better1 => {
                            if new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            }
                        }
                        PathKeysComparison::Better2 => {
                            if new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
                        }
                        PathKeysComparison::Equal => {
                            if new_path.parallel_safe > old_path.parallel_safe {
                                remove_old = true;
                            } else if new_path.parallel_safe < old_path.parallel_safe {
                                accept_new = false;
                            } else if new_path.rows < old_path.rows {
                                remove_old = true;
                            } else if new_path.rows > old_path.rows {
                                accept_new = false;
                            } else if compare_path_costs_fuzzily(
                                new_path,
                                old_path,
                                1.0000000001,
                                consider_startup,
                                consider_param_startup,
                            ) == PathCostComparison::Better1
                            {
                                remove_old = true;
                            } else {
                                accept_new = false;
                            }
                        }
                        PathKeysComparison::Different => unreachable!(),
                    },
                    PathCostComparison::Better1 => {
                        if keyscmp != PathKeysComparison::Better2
                            && new_path.rows <= old_path.rows
                            && new_path.parallel_safe >= old_path.parallel_safe
                        {
                            remove_old = true;
                        }
                    }
                    PathCostComparison::Better2 => {
                        if keyscmp != PathKeysComparison::Better1
                            && new_path.rows >= old_path.rows
                            && new_path.parallel_safe <= old_path.parallel_safe
                        {
                            accept_new = false;
                        }
                    }
                    PathCostComparison::Different => unreachable!(),
                }
            }
        }

        if remove_old {
            working.remove(i);
        } else {
            let new_path = run.root.path(new_id).base();
            let old_path = run.root.path(working[i]).base();
            if new_path.disabled_nodes > old_path.disabled_nodes
                || (new_path.disabled_nodes == old_path.disabled_nodes
                    && new_path.total_cost >= old_path.total_cost)
            {
                insert_at = i + 1;
            }
            i += 1;
        }

        if !accept_new {
            break;
        }
    }

    if accept_new {
        let at = insert_at.min(working.len());
        working.insert(at, new_id);
    }
    run.root.rel_mut(rel_id).pathlist = working;
    new_id
}

pub fn add_existing_path(run: &mut PlannerRun<'_>, rel_id: RelId, path_id: PathId) {
    add_path(run, rel_id, path_id);
}

#[cold]
#[inline(never)]
fn no_plan_error() -> PgError {
    PgError::error("could not devise a query plan for the given query".to_string())
}

// set_cheapest (pathnode.c); parameterized paths can't be built on this lane.
pub fn set_cheapest(run: &mut PlannerRun<'_>, rel_id: RelId) -> PgResult<()> {
    if run.root.rel(rel_id).pathlist.is_empty() {
        return Err(no_plan_error().into());
    }
    let mut cheapest_startup_path: Option<PathId> = None;
    let mut cheapest_total_path: Option<PathId> = None;

    let npaths = run.root.rel(rel_id).pathlist.len();
    for i in 0..npaths {
        let pid = run.root.rel(rel_id).pathlist[i];
        let path = run.root.path(pid).base();
        assert!(path.param_info.is_none(), "set_cheapest (pathnode.c): parameterized path; M2 lane");
        let (Some(s), Some(t)) = (cheapest_startup_path, cheapest_total_path) else {
            cheapest_startup_path = Some(pid);
            cheapest_total_path = Some(pid);
            continue;
        };
        use crate::pathkeys::{compare_pathkeys, PathKeysComparison};
        let cmp = compare_path_costs(run.root.path(s).base(), path, CostSelector::Startup);
        if cmp > 0
            || (cmp == 0
                && compare_pathkeys(&run.root.path(s).base().pathkeys, &path.pathkeys)
                    == PathKeysComparison::Better2)
        {
            cheapest_startup_path = Some(pid);
        }
        let cmp = compare_path_costs(run.root.path(t).base(), path, CostSelector::Total);
        if cmp > 0
            || (cmp == 0
                && compare_pathkeys(&run.root.path(t).base().pathkeys, &path.pathkeys)
                    == PathKeysComparison::Better2)
        {
            cheapest_total_path = Some(pid);
        }
    }

    let cheapest_total = cheapest_total_path.expect("nonempty pathlist");
    let mcx = run.mcx;
    let rel = run.root.rel_mut(rel_id);
    rel.cheapest_startup_path = cheapest_startup_path;
    rel.cheapest_total_path = Some(cheapest_total);
    rel.cheapest_unique_path = None;
    rel.cheapest_parameterized_paths = PgVec::new_in(mcx);
    rel.cheapest_parameterized_paths.push(cheapest_total);
    Ok(())
}

fn base_path<'mcx>(
    run: &PlannerRun<'mcx>,
    type_: NodeTag,
    pathtype: NodeTag,
    rel_id: RelId,
) -> Path<'mcx> {
    Path {
        type_: tag16(type_),
        pathtype: tag16(pathtype),
        parent: rel_id,
        pathtarget_id: run.root.rel(rel_id).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: PgVec::new_in(run.mcx),
    }
}

// create_seqscan_path (pathnode.c); required_outer is empty on this lane.
pub fn create_seqscan_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    parallel_workers: i32,
) -> PgResult<PathId> {
    assert!(parallel_workers == 0, "create_seqscan_path: partial path; M3 parallel lane");
    let mut path = base_path(run, NodeTag::T_Path, NodeTag::T_SeqScan, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let id = run.root.alloc_path(PathNode::Path(path));
    crate::costsize::cost_seqscan(run, id, rel_id);
    Ok(id)
}

// choose_bitmap_and (indxpath.c), single-candidate arm: with one input path
// there is nothing to AND.
pub fn choose_bitmap_and(_run: &PlannerRun<'_>, _rel_id: RelId, paths: &[PathId]) -> PathId {
    debug_assert!(!paths.is_empty());
    if paths.len() == 1 {
        return paths[0];
    }
    panic!("choose_bitmap_and (indxpath.c): AND/OR path reduction; M2 bitmap-combine lane");
}

// create_bitmap_heap_path (pathnode.c); required_outer/parallel loud upstream.
pub fn create_bitmap_heap_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    bitmapqual: PathId,
    loop_count: f64,
) -> PgResult<PathId> {
    let mut path = base_path(run, NodeTag::T_BitmapHeapPath, NodeTag::T_BitmapHeapScan, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let node = types_pathnodes::BitmapHeapPath { path, bitmapqual: Some(bitmapqual) };
    let id = run.root.alloc_path(PathNode::BitmapHeapPath(node));
    crate::costsize::cost_bitmap_heap_scan(run, id, rel_id, bitmapqual, loop_count);
    Ok(id)
}

// create_index_path (pathnode.c); indexorderbys/partial paths loud upstream.
pub fn create_index_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: Rc<IndexOptInfo<'mcx>>,
    indexclauses: PgVec<'mcx, IndexClause<'mcx>>,
    pathkeys: PgVec<'mcx, PathKey>,
    indexscandir: ScanDirection,
    indexonly: bool,
    loop_count: f64,
) -> PgResult<PathId> {
    let rel_id = index.rel.expect("IndexOptInfo rel set");
    let pathtype = if indexonly { NodeTag::T_IndexOnlyScan } else { NodeTag::T_IndexScan };
    let mut path = base_path(run, NodeTag::T_IndexPath, pathtype, rel_id);
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    path.pathkeys = pathkeys;
    let mcx = run.mcx;
    let node = IndexPath {
        path,
        indexinfo: Some(index),
        indexclauses,
        indexorderbys: PgVec::new_in(mcx),
        indexorderbycols: PgVec::new_in(mcx),
        indexscandir,
        indextotalcost: 0.0,
        indexselectivity: 0.0,
    };
    let id = run.root.alloc_path(PathNode::IndexPath(node));
    crate::costsize::cost_index(run, id, loop_count)?;
    Ok(id)
}

// create_agg_path (pathnode.c).
#[allow(clippy::too_many_arguments)]
pub fn create_agg_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    target_id: PtId,
    aggstrategy: u32,
    aggsplit: u32,
    group_clause: PgVec<'mcx, types_pathnodes::NodeId>,
    qual: PgVec<'mcx, types_pathnodes::NodeId>,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
) -> PgResult<PathId> {
    assert!(
        aggstrategy != types_pathnodes::AGG_MIXED,
        "create_agg_path (pathnode.c): AGG_MIXED; M3 grouping-sets lane"
    );
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let pathkeys = if aggstrategy == types_pathnodes::AGG_SORTED {
        crate::relnode::pgvec_clone_shallow(run.mcx, &sub.pathkeys)
    } else {
        // AGG_HASHED/AGG_PLAIN output is unordered.
        PgVec::new_in(run.mcx)
    };
    let path = Path {
        type_: tag16(NodeTag::T_AggPath),
        pathtype: tag16(NodeTag::T_Agg),
        parent: rel_id,
        pathtarget_id: Some(target_id),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys,
    };
    let (sub_disabled, sub_startup, sub_total, sub_rows) =
        (sub.disabled_nodes, sub.startup_cost, sub.total_cost, sub.rows);
    let sub_width = sub.pathtarget_id.map_or(0, |pt| run.root.pathtarget(pt).width);
    let num_group_cols = group_clause.len() as i32;
    let quals = crate::relnode::pgvec_clone_shallow(run.mcx, &qual);
    let transition_space = aggcosts.transitionSpace as u64;

    let id = run.root.alloc_path(PathNode::AggPath(types_pathnodes::AggPath {
        path,
        subpath: Some(subpath_id),
        aggstrategy,
        aggsplit,
        numGroups: num_groups,
        transitionSpace: transition_space,
        groupClause: group_clause,
        qual,
    }));
    crate::costsize::cost_agg(
        run,
        id,
        aggstrategy,
        aggcosts,
        num_group_cols,
        num_groups,
        &quals,
        sub_disabled,
        sub_startup,
        sub_total,
        sub_rows,
        sub_width,
    )?;

    let target = run.root.pathtarget(target_id);
    let (t_startup, t_per_tuple) = (target.cost.startup, target.cost.per_tuple);
    let p = run.root.path_mut(id).base_mut();
    let rows = p.rows;
    p.startup_cost += t_startup;
    p.total_cost += t_startup + t_per_tuple * rows;
    Ok(id)
}

/// C `create_upper_unique_path` (pathnode.c): one cpu_operator_cost per
/// compared column per input tuple; input ordering preserved.
pub fn create_upper_unique_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    num_cols: i32,
    num_groups: f64,
) -> PathId {
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let path = Path {
        type_: tag16(NodeTag::T_UpperUniquePath),
        pathtype: tag16(NodeTag::T_Unique),
        parent: rel_id,
        // Unique doesn't project, so use the source path's pathtarget.
        pathtarget_id: sub.pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows: num_groups,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: sub.startup_cost,
        total_cost: sub.total_cost
            + gucs::cpu_operator_cost() * sub.rows * num_cols as f64,
        pathkeys: crate::relnode::pgvec_clone_shallow(run.mcx, &sub.pathkeys),
    };
    run.root.alloc_path(PathNode::UpperUniquePath(types_pathnodes::UpperUniquePath {
        path,
        subpath: Some(subpath_id),
        numkeys: num_cols,
    }))
}

// get_cheapest_fractional_path (planner.c).
pub fn get_cheapest_fractional_path(run: &PlannerRun<'_>, rel_id: RelId, tuple_fraction: f64) -> PathId {
    let mut best = run
        .root
        .rel(rel_id)
        .cheapest_total_path
        .expect("set_cheapest ran");
    if tuple_fraction <= 0.0 {
        return best;
    }
    let mut tuple_fraction = tuple_fraction;
    if tuple_fraction >= 1.0 && run.root.path(best).base().rows > 0.0 {
        tuple_fraction /= run.root.path(best).base().rows;
    }
    let npaths = run.root.rel(rel_id).pathlist.len();
    for i in 0..npaths {
        let pid = run.root.rel(rel_id).pathlist[i];
        let path = run.root.path(pid).base();
        if path.param_info.is_some() {
            continue;
        }
        if Some(pid) == run.root.rel(rel_id).cheapest_total_path
            || compare_fractional_path_costs(run.root.path(best).base(), path, tuple_fraction) <= 0
        {
            continue;
        }
        best = pid;
    }
    best
}

fn compare_fractional_path_costs(path1: &Path<'_>, path2: &Path<'_>, fraction: f64) -> i32 {
    if path1.disabled_nodes != path2.disabled_nodes {
        return if path1.disabled_nodes < path2.disabled_nodes { -1 } else { 1 };
    }
    if fraction <= 0.0 || fraction >= 1.0 {
        return compare_path_costs(path1, path2, CostSelector::Total);
    }
    let cost1 = path1.startup_cost + fraction * (path1.total_cost - path1.startup_cost);
    let cost2 = path2.startup_cost + fraction * (path2.total_cost - path2.startup_cost);
    if cost1 < cost2 {
        return -1;
    }
    if cost1 > cost2 {
        return 1;
    }
    0
}

pub fn create_sort_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    pathkeys: PgVec<'mcx, PathKey>,
    limit_tuples: f64,
) -> PathId {
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let path = Path {
        type_: tag16(NodeTag::T_SortPath),
        pathtype: tag16(NodeTag::T_Sort),
        parent: rel_id,
        // Sort doesn't project, so use the source path's pathtarget.
        pathtarget_id: sub.pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys,
    };
    let (sub_disabled, sub_total, sub_rows) = (sub.disabled_nodes, sub.total_cost, sub.rows);
    let width = sub.pathtarget_id.map_or(0, |pt| run.root.pathtarget(pt).width);
    let id = run
        .root
        .alloc_path(PathNode::SortPath(types_pathnodes::SortPath {
            path,
            subpath: Some(subpath_id),
        }));
    crate::costsize::cost_sort(
        run,
        id,
        sub_disabled,
        sub_total,
        sub_rows,
        width,
        0.0,
        init_small::globals::work_mem(),
        limit_tuples,
    );
    id
}

#[allow(clippy::too_many_arguments)]
pub fn create_limit_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    limit_offset: Option<types_nodes::Node<'mcx>>,
    limit_count: Option<types_nodes::Node<'mcx>>,
    limit_option: types_nodes::nodes_enums::LimitOption,
    offset_est: i64,
    count_est: i64,
) -> PathId {
    let mcx = run.mcx;
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let mut path = Path {
        type_: tag16(NodeTag::T_LimitPath),
        pathtype: tag16(NodeTag::T_Limit),
        parent: rel_id,
        // Limit doesn't project, so use the source path's pathtarget.
        pathtarget_id: sub.pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows: sub.rows,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: sub.startup_cost,
        total_cost: sub.total_cost,
        pathkeys: crate::relnode::pgvec_clone_shallow(mcx, &sub.pathkeys),
    };
    adjust_limit_rows_costs(
        &mut path.rows,
        &mut path.startup_cost,
        &mut path.total_cost,
        offset_est,
        count_est,
    );
    let limit_offset_id = limit_offset.map(|n| run.intern_expr(n));
    let limit_count_id = limit_count.map(|n| run.intern_expr(n));
    run.root.alloc_path(PathNode::LimitPath(types_pathnodes::LimitPath {
        path,
        subpath: Some(subpath_id),
        limitOffset: limit_offset_id,
        limitCount: limit_count_id,
        limitOption: limit_option as u32,
    }))
}

pub fn adjust_limit_rows_costs(
    rows: &mut f64,
    startup_cost: &mut f64,
    total_cost: &mut f64,
    offset_est: i64,
    count_est: i64,
) {
    let input_rows = *rows;
    let input_startup_cost = *startup_cost;
    let input_total_cost = *total_cost;

    if offset_est != 0 {
        let mut offset_rows = if offset_est > 0 {
            offset_est as f64
        } else {
            crate::costsize::clamp_row_est(input_rows * 0.10)
        };
        if offset_rows > *rows {
            offset_rows = *rows;
        }
        if input_rows > 0.0 {
            *startup_cost += (input_total_cost - input_startup_cost) * offset_rows / input_rows;
        }
        *rows -= offset_rows;
        if *rows < 1.0 {
            *rows = 1.0;
        }
    }

    if count_est != 0 {
        let mut count_rows = if count_est > 0 {
            count_est as f64
        } else {
            crate::costsize::clamp_row_est(input_rows * 0.10)
        };
        if count_rows > *rows {
            count_rows = *rows;
        }
        if input_rows > 0.0 {
            *total_cost = *startup_cost
                + (input_total_cost - input_startup_cost) * count_rows / input_rows;
        }
        *rows = count_rows;
        if *rows < 1.0 {
            *rows = 1.0;
        }
    }
}
