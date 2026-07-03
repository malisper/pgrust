//! pathnode.c: Path constructors + add_path dominance. Cross-cycle callbacks
//! (estimate_num_groups) ride planner_seams.

use mcx::PgVec;
use types_error::{PgError, PgResult};
use types_nodes::list::NodeList;
use types_nodes::NodeTag;
use types_pathnodes::{
    GroupResultPath, IndexClause, IndexOptInfo, IndexPath, Path, PathId, PathKey, PathNode,
    PathTarget, ProjectionPath, PtId, RelId, Relids, ScanDirection,
};

use costsize::{clamp_width_est, cost_qual_eval_node, gucs, JoinCostWorkspace};
use types_pathnodes::run::PlannerRun;
use types_pathnodes::{
    compare_pathkeys, tag16, HashPath, JoinPath, MaterialPath, MergePath, NestPath,
    PathKeysComparison, SemiAntiJoinFactors, SpecialJoinInfo, RinfoId,
};

pub use costsize::SubqueryScanInfo;

pub use types_pathnodes::tag16;

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
        tuple_width += costsize::get_expr_width(run, expr)? as i64;
    }
    run.root.pathtarget_mut(id).width = clamp_width_est(tuple_width);
    Ok(id)
}

// create_group_result_path (pathnode.c). The qual cost lands on startup:
// C evaluates havingqual once, not per tuple.
pub fn create_group_result_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    target_id: PtId,
    havingqual: PgVec<'mcx, types_pathnodes::NodeId>,
) -> PgResult<PathNode<'mcx>> {
    let mut qual_cost = 0.0f64;
    for &id in havingqual.iter() {
        let qc = costsize::cost_qual_eval_node(*run.root.expr_node(id))?;
        qual_cost += qc.startup + qc.per_tuple;
    }
    let target = run.root.pathtarget(target_id);
    let (t_startup, t_per_tuple) = (target.cost.startup, target.cost.per_tuple);
    let rel = run.root.rel(rel_id);
    Ok(PathNode::GroupResultPath(GroupResultPath {
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
            startup_cost: t_startup + qual_cost,
            total_cost: t_startup + gucs::cpu_tuple_cost() + t_per_tuple + qual_cost,
            pathkeys: PgVec::new_in(run.mcx),
        },
        quals: havingqual,
    }))
}

// is_projection_capable_path (createplan.c), keyed on pathtype like C.
pub fn is_projection_capable_pathtype(pathtype: u16) -> bool {
    match pathtype {
        t if t == tag16(NodeTag::T_Result) => true,
        t if t == tag16(NodeTag::T_SeqScan) => true,
        t if t == tag16(NodeTag::T_IndexScan) => true,
        t if t == tag16(NodeTag::T_IndexOnlyScan) => true,
        t if t == tag16(NodeTag::T_CteScan) => true,
        t if t == tag16(NodeTag::T_SubqueryScan) => true,
        t if t == tag16(NodeTag::T_ValuesScan) => true,
        t if t == tag16(NodeTag::T_FunctionScan) => true,
        t if t == tag16(NodeTag::T_SetOp) => false,
        t if t == tag16(NodeTag::T_Append) => false,
        t if t == tag16(NodeTag::T_MergeAppend) => false,
        t if t == tag16(NodeTag::T_ProjectSet) => false,
        t if t == tag16(NodeTag::T_NestLoop) => true,
        t if t == tag16(NodeTag::T_MergeJoin) => true,
        t if t == tag16(NodeTag::T_HashJoin) => true,
        t if t == tag16(NodeTag::T_ValuesScan) => true,
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
        pathkeys: types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys),
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

// create_set_projection_path (pathnode.c).
pub fn create_set_projection_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    target_id: PtId,
) -> PgResult<PathNode<'mcx>> {
    let mut tlist_rows = 1.0f64;
    for i in 0..run.root.pathtarget(target_id).exprs.len() {
        let id = run.root.pathtarget(target_id).exprs[i];
        let itemrows = costsize::expression_returns_set_rows(*run.root.expr_node(id))?;
        if tlist_rows < itemrows {
            tlist_rows = itemrows;
        }
    }
    let target_parallel_safe = clauses::is_parallel_safe_exprs(run, target_id)?;
    let sub = run.root.path(subpath_id).base();
    let target = run.root.pathtarget(target_id);
    let rel = run.root.rel(rel_id);
    let rows = sub.rows * tlist_rows;
    let path = Path {
        type_: tag16(NodeTag::T_ProjectSetPath),
        pathtype: tag16(NodeTag::T_ProjectSet),
        parent: rel_id,
        pathtarget_id: Some(target_id),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && sub.parallel_safe && target_parallel_safe,
        parallel_workers: sub.parallel_workers,
        rows,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: sub.startup_cost + target.cost.startup,
        total_cost: sub.total_cost
            + target.cost.startup
            + (gucs::cpu_tuple_cost() + target.cost.per_tuple) * sub.rows
            + (rows - sub.rows) * gucs::cpu_tuple_cost() / 2.0,
        pathkeys: types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys),
    };
    Ok(PathNode::ProjectSetPath(types_pathnodes::ProjectSetPath {
        path,
        subpath: Some(subpath_id),
    }))
}

// create_modifytable_path (pathnode.c), single-relation INSERT/UPDATE/DELETE
// arm: no WCO/rowmarks/MERGE lists (loud upstream), rows = 0.
pub fn create_modifytable_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    operation: types_nodes::nodes_enums::CmdType,
    can_set_tag: bool,
    // (operation is stored as the C CmdType value; types_pathnodes uses u32)
    result_relation: u32,
    update_colnos: Option<PgVec<'mcx, i16>>,
    returning_list: Option<PgVec<'mcx, types_pathnodes::NodeId>>,
    onconflict: Option<types_pathnodes::NodeId>,
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
        returningLists: match returning_list {
            Some(rlist) => {
                let mut lists = PgVec::new_in(run.mcx);
                lists.push(rlist);
                lists
            }
            None => PgVec::new_in(run.mcx),
        },
        rowMarks: PgVec::new_in(run.mcx),
        onconflict,
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

// PATH_REQ_OUTER (pathnodes.h).
pub fn path_req_outer<'p, 'mcx>(path: &'p Path<'mcx>) -> &'p Relids<'mcx> {
    match &path.param_info {
        Some(ppi) => &ppi.ppi_req_outer,
        None => &None,
    }
}

// add_path (pathnode.c).
pub fn add_path<'mcx>(run: &mut PlannerRun<'mcx>, rel_id: RelId, new_id: PathId) -> PathId {
    use types_pathnodes::relids::{relids_subset_compare, SubsetCmp};
    let mut accept_new = true;
    let mut insert_at = 0usize;

    let consider_startup = run.root.rel(rel_id).consider_startup;
    let consider_param_startup = run.root.rel(rel_id).consider_param_startup;

    let empty: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    let mut working = core::mem::replace(&mut run.root.rel_mut(rel_id).pathlist, empty);

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
            let keyscmp = compare_pathkeys(&new_path.pathkeys, &old_path.pathkeys);
            if keyscmp != PathKeysComparison::Different {
                let outercmp = || {
                    relids_subset_compare(path_req_outer(new_path), path_req_outer(old_path))
                };
                match costcmp {
                    PathCostComparison::Equal => match keyscmp {
                        PathKeysComparison::Better1 => {
                            let oc = outercmp();
                            if (oc == SubsetCmp::Equal || oc == SubsetCmp::Subset1)
                                && new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            }
                        }
                        PathKeysComparison::Better2 => {
                            let oc = outercmp();
                            if (oc == SubsetCmp::Equal || oc == SubsetCmp::Subset2)
                                && new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
                        }
                        PathKeysComparison::Equal => match outercmp() {
                            SubsetCmp::Equal => {
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
                            SubsetCmp::Subset1 => {
                                if new_path.rows <= old_path.rows
                                    && new_path.parallel_safe >= old_path.parallel_safe
                                {
                                    remove_old = true;
                                }
                            }
                            SubsetCmp::Subset2 => {
                                if new_path.rows >= old_path.rows
                                    && new_path.parallel_safe <= old_path.parallel_safe
                                {
                                    accept_new = false;
                                }
                            }
                            SubsetCmp::Different => {}
                        },
                        PathKeysComparison::Different => unreachable!(),
                    },
                    PathCostComparison::Better1 => {
                        if keyscmp != PathKeysComparison::Better2 {
                            let oc = outercmp();
                            if (oc == SubsetCmp::Equal || oc == SubsetCmp::Subset1)
                                && new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            }
                        }
                    }
                    PathCostComparison::Better2 => {
                        if keyscmp != PathKeysComparison::Better1 {
                            let oc = outercmp();
                            if (oc == SubsetCmp::Equal || oc == SubsetCmp::Subset2)
                                && new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
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

// set_cheapest (pathnode.c).
pub fn set_cheapest(run: &mut PlannerRun<'_>, rel_id: RelId) -> PgResult<()> {
    use types_pathnodes::relids::{relids_subset_compare, SubsetCmp};
    if run.root.rel(rel_id).pathlist.is_empty() {
        return Err(no_plan_error().into());
    }
    let mut cheapest_startup_path: Option<PathId> = None;
    let mut cheapest_total_path: Option<PathId> = None;
    let mut best_param_path: Option<PathId> = None;
    let mcx = run.mcx;
    let mut parameterized_paths: PgVec<'_, PathId> = PgVec::new_in(mcx);

    let npaths = run.root.rel(rel_id).pathlist.len();
    for i in 0..npaths {
        let pid = run.root.rel(rel_id).pathlist[i];
        let path = run.root.path(pid).base();
        if path.param_info.is_some() {
            parameterized_paths.push(pid);
            if cheapest_total_path.is_some() {
                continue;
            }
            match best_param_path {
                None => best_param_path = Some(pid),
                Some(bp) => {
                    let best = run.root.path(bp).base();
                    match relids_subset_compare(path_req_outer(path), path_req_outer(best)) {
                        SubsetCmp::Equal => {
                            if compare_path_costs(path, best, CostSelector::Total) < 0 {
                                best_param_path = Some(pid);
                            }
                        }
                        SubsetCmp::Subset1 => best_param_path = Some(pid),
                        SubsetCmp::Subset2 | SubsetCmp::Different => {}
                    }
                }
            }
            continue;
        }
        let (Some(s), Some(t)) = (cheapest_startup_path, cheapest_total_path) else {
            cheapest_startup_path = Some(pid);
            cheapest_total_path = Some(pid);
            continue;
        };
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

    if let Some(t) = cheapest_total_path {
        parameterized_paths.insert(0, t);
    }
    let cheapest_total = match cheapest_total_path {
        Some(t) => t,
        None => best_param_path.expect("nonempty pathlist"),
    };
    let rel = run.root.rel_mut(rel_id);
    rel.cheapest_startup_path = cheapest_startup_path;
    rel.cheapest_total_path = Some(cheapest_total);
    rel.cheapest_unique_path = None;
    rel.cheapest_parameterized_paths = parameterized_paths;
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

// join_clause_is_movable_into (restrictinfo.c).
pub fn join_clause_is_movable_into(
    run: &PlannerRun<'_>,
    rid: types_pathnodes::RinfoId,
    currentrelids: &types_pathnodes::Relids<'_>,
    current_and_outer: &types_pathnodes::Relids<'_>,
) -> bool {
    use types_pathnodes::relids::{relids_is_subset, relids_overlap};
    let ri = run.root.rinfo(rid);
    if !relids_is_subset(&ri.clause_relids, current_and_outer) {
        return false;
    }
    if !relids_overlap(currentrelids, &ri.clause_relids) {
        return false;
    }
    if relids_overlap(currentrelids, &ri.outer_relids) {
        return false;
    }
    true
}

// get_baserel_parampathinfo (relnode.c). The path holds a copy of the cached
// PPI (C shares the pointer; nothing compares PPIs by identity here).
pub fn get_baserel_parampathinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_pathnodes::ParamPathInfo<'mcx>>>> {
    use types_pathnodes::relids::{
        relids_add_member, relids_copy, relids_equal, relids_is_empty, relids_is_subset,
        relids_overlap, relids_union,
    };
    let mcx = run.mcx;
    debug_assert!(relids_is_subset(&run.root.rel(rel_id).lateral_relids, required_outer));
    if relids_is_empty(required_outer) {
        return Ok(None);
    }
    debug_assert!(!relids_overlap(&run.root.rel(rel_id).relids, required_outer));
    for i in 0..run.root.rel(rel_id).ppilist.len() {
        if relids_equal(&run.root.rel(rel_id).ppilist[i].ppi_req_outer, required_outer) {
            let ppi = run.root.rel(rel_id).ppilist[i].clone();
            return Ok(Some(mcx::box_new_in(mcx, ppi)));
        }
    }
    let joinrelids = relids_union(mcx, &run.root.rel(rel_id).relids, required_outer);
    let joininfo =
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &run.root.rel(rel_id).joininfo);
    let mut pclauses: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(mcx);
    let rel_relids = relids_copy(mcx, &run.root.rel(rel_id).relids);
    for &rid in joininfo.iter() {
        if join_clause_is_movable_into(run, rid, &rel_relids, &joinrelids) {
            // generate_join_implied_equalities divergence: eclass-lite keeps
            // join equalities in joininfo, but C's EC leg emits them at a
            // parameterized scan as outer_em = inner_em; commute so the
            // scanned rel sits on the right (EXPLAIN parity).
            pclauses.push(commute_ec_clause_for_scan(run, rid, &rel_relids)?);
        }
    }
    let mut pserials: types_pathnodes::Relids<'mcx> = None;
    for &rid in pclauses.iter() {
        pserials = relids_add_member(mcx, &pserials, run.root.rinfo(rid).rinfo_serial as u32);
    }
    let rows = costsize::get_parameterized_baserel_size(run, rel_id, &pclauses)?;
    let ppi = types_pathnodes::ParamPathInfo {
        ppi_req_outer: relids_copy(mcx, required_outer),
        ppi_rows: rows,
        ppi_clauses: pclauses,
        ppi_serials: pserials,
    };
    run.root.rel_mut(rel_id).ppilist.push(ppi.clone());
    Ok(Some(mcx::box_new_in(mcx, ppi)))
}

// The EC-orientation shim for get_baserel_parampathinfo (see call site).
fn commute_ec_clause_for_scan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: types_pathnodes::RinfoId,
    rel_relids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<types_pathnodes::RinfoId> {
    use types_pathnodes::relids::{relids_is_empty, relids_is_subset};
    let mcx = run.mcx;
    let (clause_id, ok, left_in_rel) = {
        let ri = run.root.rinfo(rid);
        (
            ri.clause,
            ri.can_join
                && ri.is_pushed_down
                && !ri.pseudoconstant
                && !ri.mergeopfamilies.is_empty(),
            !relids_is_empty(&ri.left_relids) && relids_is_subset(&ri.left_relids, rel_relids),
        )
    };
    if !ok || !left_in_rel {
        return Ok(rid);
    }
    let clause = *run.root.expr_node(clause_id);
    let Some(op) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return Ok(rid);
    };
    let comm = lsyscache::get_commutator(op.opno)?;
    if comm == 0 {
        return Ok(rid);
    }
    let mut args = types_nodes::NodeList::nil();
    args.lappend(mcx, op.args.nth(1))?;
    args.lappend(mcx, op.args.nth(0))?;
    let commuted = types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: comm,
            opfuncid: lsyscache::get_opcode(comm)?,
            opresulttype: op.opresulttype,
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            args,
            location: op.location,
        },
    )?;
    planner_seams::make_restrictinfo::call(
        run, commuted, true, false, false, false, 0, None, None, None,
    )
}

// get_joinrel_parampathinfo (relnode.c): only the unparameterized-join arm is
// live (parameterized join paths need the movable-clause split).
pub fn get_joinrel_parampathinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    _rel_id: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
    restrict_clauses: &mut PgVec<'mcx, types_pathnodes::RinfoId>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_pathnodes::ParamPathInfo<'mcx>>>> {
    let _ = restrict_clauses;
    if types_pathnodes::relids::relids_is_empty(required_outer) {
        return Ok(None);
    }
    panic!(
        "get_joinrel_parampathinfo (relnode.c): parameterized join path; \
         multi-level lateral/param lane unported"
    );
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
    costsize::cost_seqscan(run, id, rel_id);
    Ok(id)
}

// create_functionscan_path (pathnode.c); pathkeys empty (ORDINALITY is loud
// in the parser).
pub fn create_functionscan_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<PathId> {
    let param_info = get_baserel_parampathinfo(run, rel_id, required_outer)?;
    let mut path = base_path(run, NodeTag::T_Path, NodeTag::T_FunctionScan, rel_id);
    path.param_info = param_info;
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let id = run.root.alloc_path(PathNode::Path(path));
    costsize::cost_functionscan(run, id, rel_id)?;
    Ok(id)
}

// create_valuesscan_path (pathnode.c); result is always unordered.
pub fn create_valuesscan_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<PathId> {
    let param_info = get_baserel_parampathinfo(run, rel_id, required_outer)?;
    let mut path = base_path(run, NodeTag::T_Path, NodeTag::T_ValuesScan, rel_id);
    path.param_info = param_info;
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let id = run.root.alloc_path(PathNode::Path(path));
    costsize::cost_valuesscan(run, id, rel_id)?;
    Ok(id)
}

// create_resultscan_path (pathnode.c).
pub fn create_resultscan_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<PathId> {
    let param_info = get_baserel_parampathinfo(run, rel_id, required_outer)?;
    let mut path = base_path(run, NodeTag::T_Path, NodeTag::T_Result, rel_id);
    path.param_info = param_info;
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let id = run.root.alloc_path(PathNode::Path(path));
    costsize::cost_resultscan(run, id, rel_id)?;
    Ok(id)
}

// create_ctescan_path (pathnode.c); pathkeys/required_outer empty on this lane.
pub fn create_ctescan_path<'mcx>(run: &mut PlannerRun<'mcx>, rel_id: RelId) -> PgResult<PathId> {
    let mut path = base_path(run, NodeTag::T_Path, NodeTag::T_CteScan, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let id = run.root.alloc_path(PathNode::Path(path));
    costsize::cost_ctescan(run, id, rel_id)?;
    Ok(id)
}

// create_bitmap_and_path (pathnode.c); required_outer empty on this lane
// (children are unparameterized, asserted).
pub fn create_bitmap_and_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    bitmapquals: PgVec<'mcx, PathId>,
) -> PgResult<PathId> {
    for &q in bitmapquals.iter() {
        assert!(
            run.root.path(q).base().param_info.is_none(),
            "create_bitmap_and_path (pathnode.c): parameterized subpath; M2 join lane"
        );
    }
    let mut path = base_path(run, NodeTag::T_BitmapAndPath, NodeTag::T_BitmapAnd, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let node = types_pathnodes::BitmapAndPath { path, bitmapquals, bitmapselectivity: 0.0 };
    let id = run.root.alloc_path(PathNode::BitmapAndPath(node));
    costsize::cost_bitmap_and_node(run, id);
    Ok(id)
}

// create_bitmap_or_path (pathnode.c); required_outer empty on this lane.
pub fn create_bitmap_or_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    bitmapquals: PgVec<'mcx, PathId>,
) -> PgResult<PathId> {
    for &q in bitmapquals.iter() {
        assert!(
            run.root.path(q).base().param_info.is_none(),
            "create_bitmap_or_path (pathnode.c): parameterized subpath; M2 join lane"
        );
    }
    let mut path = base_path(run, NodeTag::T_BitmapOrPath, NodeTag::T_BitmapOr, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let node = types_pathnodes::BitmapOrPath { path, bitmapquals, bitmapselectivity: 0.0 };
    let id = run.root.alloc_path(PathNode::BitmapOrPath(node));
    costsize::cost_bitmap_or_node(run, id);
    Ok(id)
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
    costsize::cost_bitmap_heap_scan(run, id, rel_id, bitmapqual, loop_count);
    Ok(id)
}

// create_index_path (pathnode.c); indexorderbys/partial paths loud upstream.
pub fn create_index_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &'mcx IndexOptInfo<'mcx>,
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
    costsize::cost_index(run, id, loop_count)?;
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
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys)
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
    let quals = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &qual);
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
    costsize::cost_agg(
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

// create_groupingsets_path (pathnode.c); hashed/AGG_MIXED rollups are loud
// upstream, so the is_hashed cost legs are unreachable panics.
#[allow(clippy::too_many_arguments)]
pub fn create_groupingsets_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    having_qual: PgVec<'mcx, types_pathnodes::NodeId>,
    aggstrategy: u32,
    rollups: PgVec<'mcx, types_pathnodes::RollupData<'mcx>>,
    agg_costs: &types_pathnodes::AggClauseCosts,
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let target_id = rel.pathtarget_id.expect("grouped rel has a reltarget");

    debug_assert!(!rollups.is_empty());
    let aggstrategy = if aggstrategy == types_pathnodes::AGG_SORTED
        && rollups.len() == 1
        && rollups[0].groupClause.is_empty()
    {
        types_pathnodes::AGG_PLAIN
    } else {
        aggstrategy
    };
    debug_assert!(
        aggstrategy == types_pathnodes::AGG_SORTED || aggstrategy == types_pathnodes::AGG_PLAIN,
        "create_groupingsets_path (pathnode.c): hashed/AGG_MIXED strategy; grouping-sets lane"
    );
    debug_assert!(aggstrategy != types_pathnodes::AGG_PLAIN || rollups.len() == 1);

    let pathkeys = if aggstrategy == types_pathnodes::AGG_SORTED && rollups.len() == 1 {
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &run.root.group_pathkeys)
    } else {
        PgVec::new_in(mcx)
    };

    let path = Path {
        type_: tag16(NodeTag::T_GroupingSetsPath),
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
    let quals = types_pathnodes::relids::pgvec_clone_shallow(mcx, &having_qual);

    let id = run
        .root
        .alloc_path(PathNode::GroupingSetsPath(types_pathnodes::GroupingSetsPath {
            path,
            subpath: Some(subpath_id),
            aggstrategy,
            rollups,
            qual: having_qual,
            transitionSpace: agg_costs.transitionSpace as u64,
        }));

    let nrollups = match run.root.path(id) {
        PathNode::GroupingSetsPath(p) => p.rollups.len(),
        _ => unreachable!(),
    };
    let mut is_first = true;
    let mut is_first_sort = true;
    for i in 0..nrollups {
        let (num_group_cols, num_groups, is_hashed) = match run.root.path(id) {
            PathNode::GroupingSetsPath(p) => (
                p.rollups[i].gsets[0].len() as i32,
                p.rollups[i].numGroups,
                p.rollups[i].is_hashed,
            ),
            _ => unreachable!(),
        };
        assert!(
            !is_hashed,
            "create_groupingsets_path (pathnode.c): hashed rollup; grouping-sets lane"
        );
        if is_first {
            let (rows, disabled, startup, total) = costsize::cost_agg_shape(
                run,
                aggstrategy,
                agg_costs,
                num_group_cols,
                num_groups,
                &quals,
                sub_disabled,
                sub_startup,
                sub_total,
                sub_rows,
                sub_width,
            )?;
            let p = run.root.path_mut(id).base_mut();
            p.rows = rows;
            p.disabled_nodes = disabled;
            p.startup_cost = startup;
            p.total_cost = total;
            is_first = false;
            is_first_sort = false;
        } else {
            // Later rollups sort the subpath themselves; input cost is not
            // re-charged.
            debug_assert!(!is_first_sort);
            let (sort_disabled, sort_startup, sort_total) = costsize::cost_sort_shape(
                0,
                0.0,
                sub_rows,
                sub_width,
                0.0,
                init_small::globals::work_mem(),
                -1.0,
            );
            let _ = sort_startup;
            let (agg_rows, agg_disabled, _agg_startup, agg_total) =
                costsize::cost_agg_shape(
                    run,
                    types_pathnodes::AGG_SORTED,
                    agg_costs,
                    num_group_cols,
                    num_groups,
                    &quals,
                    sort_disabled,
                    sort_startup,
                    sort_total,
                    sub_rows,
                    sub_width,
                )?;
            let p = run.root.path_mut(id).base_mut();
            p.disabled_nodes += agg_disabled;
            p.total_cost += agg_total;
            p.rows += agg_rows;
        }
    }

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
        pathkeys: types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys),
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

pub fn compare_fractional_path_costs(path1: &Path<'_>, path2: &Path<'_>, fraction: f64) -> i32 {
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
    costsize::cost_sort(
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

pub fn create_incremental_sort_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    pathkeys: PgVec<'mcx, PathKey>,
    presorted_keys: usize,
    limit_tuples: f64,
) -> PgResult<PathId> {
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    let path = Path {
        type_: tag16(NodeTag::T_IncrementalSortPath),
        pathtype: tag16(NodeTag::T_IncrementalSort),
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
    let (sub_disabled, sub_startup, sub_total, sub_rows) =
        (sub.disabled_nodes, sub.startup_cost, sub.total_cost, sub.rows);
    let width = sub.pathtarget_id.map_or(0, |pt| run.root.pathtarget(pt).width);
    let keys = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.pathkeys);
    let id = run.root.alloc_path(PathNode::IncrementalSortPath(
        types_pathnodes::IncrementalSortPath {
            spath: types_pathnodes::SortPath { path, subpath: Some(subpath_id) },
            nPresortedCols: presorted_keys as i32,
        },
    ));
    costsize::cost_incremental_sort(
        run,
        id,
        &keys,
        presorted_keys,
        sub_disabled,
        sub_startup,
        sub_total,
        sub_rows,
        width,
        0.0,
        init_small::globals::work_mem(),
        limit_tuples,
    )?;
    Ok(id)
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
        pathkeys: types_pathnodes::relids::pgvec_clone_shallow(mcx, &sub.pathkeys),
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

// create_lockrows_path (pathnode.c): pathkeys NIL (locking can reorder).
pub fn create_lockrows_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    row_marks: PgVec<'mcx, types_pathnodes::PlanRowMarkId>,
    epq_param: i32,
) -> PathId {
    let mcx = run.mcx;
    let sub = run.root.path(subpath_id).base();
    let path = Path {
        type_: tag16(NodeTag::T_LockRowsPath),
        pathtype: tag16(NodeTag::T_LockRows),
        parent: rel_id,
        pathtarget_id: sub.pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: sub.rows,
        disabled_nodes: sub.disabled_nodes,
        startup_cost: sub.startup_cost,
        total_cost: sub.total_cost + gucs::cpu_tuple_cost() * sub.rows,
        pathkeys: PgVec::new_in(mcx),
    };
    run.root.alloc_path(PathNode::LockRowsPath(types_pathnodes::LockRowsPath {
        path,
        subpath: Some(subpath_id),
        rowMarks: row_marks,
        epqParam: epq_param,
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
            costsize::clamp_row_est(input_rows * 0.10)
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
            costsize::clamp_row_est(input_rows * 0.10)
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

// create_windowagg_path (pathnode.c); runCondition/qual legs dead
// (WindowFuncRunCondition loud upstream, topqual only feeds runconditions).
pub fn create_windowagg_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    target_id: PtId,
    window_funcs: &[types_nodes::Node<'mcx>],
    winclause_node: types_nodes::Node<'mcx>,
    topwindow: bool,
) -> PgResult<PathId> {
    let _ = topwindow;
    let sub = run.root.path(subpath_id).base();
    let rel = run.root.rel(rel_id);
    // WindowAgg preserves the input sort order.
    let pathkeys = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys);
    let path = Path {
        type_: tag16(NodeTag::T_WindowAggPath),
        pathtype: tag16(NodeTag::T_WindowAgg),
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
    let winclause = run.intern_expr(winclause_node);
    let id = run.root.alloc_path(PathNode::WindowAggPath(types_pathnodes::WindowAggPath {
        path,
        subpath: Some(subpath_id),
        winclause,
        qual: PgVec::new_in(run.mcx),
        runCondition: PgVec::new_in(run.mcx),
        topwindow,
    }));
    costsize::cost_windowagg(
        run,
        id,
        window_funcs,
        winclause_node,
        sub_disabled,
        sub_startup,
        sub_total,
        sub_rows,
    )?;
    let target = run.root.pathtarget(target_id);
    let (t_startup, t_per_tuple) = (target.cost.startup, target.cost.per_tuple);
    let p = run.root.path_mut(id).base_mut();
    let rows = p.rows;
    p.startup_cost += t_startup;
    p.total_cost += t_startup + t_per_tuple * rows;
    Ok(id)
}

// create_unique_path (pathnode.c). The make_pathkeys_for_sortclauses
// redundancy probe reduces under eclass-lite to dropping duplicate exprs
// (no EC ever carries a const). C's semi_can_hash write-back on oversized
// hash entries is skipped: recomputation reaches the same verdict.
pub fn create_unique_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpath_id: PathId,
    sjinfo: &types_pathnodes::SpecialJoinInfo<'mcx>,
) -> PgResult<Option<PathId>> {
    debug_assert!(Some(subpath_id) == run.root.rel(rel_id).cheapest_total_path);
    debug_assert!(sjinfo.jointype == types_pathnodes::JOIN_SEMI);
    debug_assert!(types_pathnodes::relids::relids_equal(
        &run.root.rel(rel_id).relids,
        &sjinfo.syn_righthand
    ));

    if let Some(cached) = run.root.rel(rel_id).cheapest_unique_path {
        return Ok(Some(cached));
    }
    if !(sjinfo.semi_can_btree || sjinfo.semi_can_hash) {
        return Ok(None);
    }

    let mcx = run.mcx;
    let mut uniq_exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let mut in_operators: PgVec<'mcx, types_pathnodes::Oid> = PgVec::new_in(mcx);
    for (i, &expr_id) in sjinfo.semi_rhs_exprs.iter().enumerate() {
        let in_oper = sjinfo.semi_operators[i];
        let sortop = lsyscache::amop::get_ordering_op_for_equality_op(in_oper, false)?;
        if sortop != 0 {
            let eqop = lsyscache::amop::get_equality_op_for_ordering_op(sortop)?
                .map(|(op, _)| op)
                .unwrap_or(0);
            assert!(
                eqop != 0,
                "could not find equality operator for ordering operator {sortop}"
            );
            let expr = *run.root.expr_node(expr_id);
            let dup = uniq_exprs.iter().any(|&kept| {
                types_nodes::equal(*run.root.expr_node(kept), expr)
            });
            if dup {
                continue;
            }
        } else {
            assert!(
                !sjinfo.semi_can_btree,
                "could not find ordering operator for equality operator {in_oper}"
            );
        }
        uniq_exprs.push(expr_id);
        in_operators.push(in_oper);
    }
    if uniq_exprs.is_empty() {
        return Ok(None);
    }

    let sub = run.root.path(subpath_id).base();
    let (sub_disabled, sub_startup, sub_total, sub_parallel_safe, sub_parallel_workers) = (
        sub.disabled_nodes,
        sub.startup_cost,
        sub.total_cost,
        sub.parallel_safe,
        sub.parallel_workers,
    );
    let sub_pathkeys = types_pathnodes::relids::pgvec_clone_shallow(mcx, &sub.pathkeys);
    let sub_width = run.root.path_pathtarget(subpath_id).width;
    let rel_rows = run.root.rel(rel_id).rows;
    let rel_rtekind = run.root.rel(rel_id).rtekind;

    let mut path = Path {
        type_: tag16(NodeTag::T_UniquePath),
        pathtype: tag16(NodeTag::T_Unique),
        parent: rel_id,
        pathtarget_id: run.root.rel(rel_id).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: run.root.rel(rel_id).consider_parallel && sub_parallel_safe,
        parallel_workers: sub_parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: PgVec::new_in(mcx),
    };

    if rel_rtekind == types_pathnodes::RTE_RELATION
        && sjinfo.semi_can_btree
        && relation_has_unique_index_for(run, rel_id, &[], &uniq_exprs, &in_operators)?
    {
        path.rows = rel_rows;
        path.disabled_nodes = sub_disabled;
        path.startup_cost = sub_startup;
        path.total_cost = sub_total;
        path.pathkeys = sub_pathkeys;
        let id = run.root.alloc_path(PathNode::UniquePath(types_pathnodes::UniquePath {
            path,
            subpath: Some(subpath_id),
            umethod: types_pathnodes::UNIQUE_PATH_NOOP,
            in_operators,
            uniq_exprs,
        }));
        run.root.rel_mut(rel_id).cheapest_unique_path = Some(id);
        return Ok(Some(id));
    }
    assert!(
        rel_rtekind != types_nodes::parsenodes::RTEKind::RTE_SUBQUERY as u32,
        "create_unique_path (pathnode.c): un-flattened subquery RHS; subquery-scan lane"
    );

    let group_exprs: PgVec<'mcx, (types_pathnodes::NodeId, types_nodes::Node<'mcx>)> = {
        let mut v = PgVec::new_in(mcx);
        for &id in uniq_exprs.iter() {
            v.push((id, *run.root.expr_node(id)));
        }
        v
    };
    path.rows = planner_seams::estimate_num_groups::call(run, &group_exprs, rel_rows)?;
    let num_cols = uniq_exprs.len();

    let sort_cost = if sjinfo.semi_can_btree {
        let (d, s, mut t) = costsize::cost_sort_shape(
            sub_disabled,
            sub_total,
            rel_rows,
            sub_width,
            0.0,
            init_small::globals::work_mem(),
            -1.0,
        );
        t += gucs::cpu_operator_cost() * rel_rows * num_cols as f64;
        Some((d, s, t))
    } else {
        None
    };

    let mut semi_can_hash = sjinfo.semi_can_hash;
    let mut agg_cost: Option<(i32, f64, f64)> = None;
    if semi_can_hash {
        let hashentrysize = (sub_width + 64) as f64;
        if hashentrysize * path.rows > ::nodehash::get_hash_memory_limit() as f64 {
            semi_can_hash = false;
        } else {
            let scratch = Path {
                type_: path.type_,
                pathtype: path.pathtype,
                parent: path.parent,
                pathtarget_id: path.pathtarget_id,
                param_info: None,
                parallel_aware: path.parallel_aware,
                parallel_safe: path.parallel_safe,
                parallel_workers: path.parallel_workers,
                rows: path.rows,
                disabled_nodes: path.disabled_nodes,
                startup_cost: path.startup_cost,
                total_cost: path.total_cost,
                pathkeys: PgVec::new_in(mcx),
            };
            let id = run.root.alloc_path(PathNode::UniquePath(types_pathnodes::UniquePath {
                path: scratch,
                subpath: Some(subpath_id),
                umethod: types_pathnodes::UNIQUE_PATH_HASH,
                in_operators: types_pathnodes::relids::pgvec_clone_shallow(mcx, &in_operators),
                uniq_exprs: types_pathnodes::relids::pgvec_clone_shallow(mcx, &uniq_exprs),
            }));
            costsize::cost_agg(
                run,
                id,
                types_pathnodes::AGG_HASHED,
                &types_pathnodes::AggClauseCosts::default(),
                num_cols as i32,
                path.rows,
                &[],
                sub_disabled,
                sub_startup,
                sub_total,
                rel_rows,
                sub_width,
            )?;
            let p = run.root.path(id).base();
            agg_cost = Some((p.disabled_nodes, p.startup_cost, p.total_cost));
        }
    }

    let umethod = match (sort_cost, agg_cost) {
        (Some(sc), Some(ac)) => {
            if ac.0 < sc.0 || (ac.0 == sc.0 && ac.2 < sc.2) {
                types_pathnodes::UNIQUE_PATH_HASH
            } else {
                types_pathnodes::UNIQUE_PATH_SORT
            }
        }
        (Some(_), None) => types_pathnodes::UNIQUE_PATH_SORT,
        (None, Some(_)) => types_pathnodes::UNIQUE_PATH_HASH,
        (None, None) => {
            debug_assert!(!semi_can_hash);
            return Ok(None);
        }
    };
    let (d, s, t) = if umethod == types_pathnodes::UNIQUE_PATH_HASH {
        agg_cost.unwrap()
    } else {
        sort_cost.unwrap()
    };
    path.disabled_nodes = d;
    path.startup_cost = s;
    path.total_cost = t;
    let id = run.root.alloc_path(PathNode::UniquePath(types_pathnodes::UniquePath {
        path,
        subpath: Some(subpath_id),
        umethod,
        in_operators,
        uniq_exprs,
    }));
    run.root.rel_mut(rel_id).cheapest_unique_path = Some(id);
    Ok(Some(id))
}

// relation_has_unique_index_for (indxpath.c): the caller's join clauses
// (outer_is_left already set) plus the rel's mergejoinable
// var-op-pseudoconstant baserestrictinfo clauses plus expr/operator pairs.
pub fn relation_has_unique_index_for<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    restrictlist: &[types_pathnodes::RinfoId],
    exprlist: &[types_pathnodes::NodeId],
    oprlist: &[types_pathnodes::Oid],
) -> PgResult<bool> {
    debug_assert!(exprlist.len() == oprlist.len());
    if run.root.rel(rel_id).indexlist.is_empty() {
        return Ok(false);
    }
    let mut restrict_rids: PgVec<'_, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    restrict_rids.extend(restrictlist.iter().copied());
    for i in 0..run.root.rel(rel_id).baserestrictinfo.len() {
        let rid = run.root.rel(rel_id).baserestrictinfo[i];
        let ri = run.root.rinfo(rid);
        if ri.mergeopfamilies.is_empty() {
            continue;
        }
        let left_empty = types_pathnodes::relids::relids_is_empty(&ri.left_relids);
        let right_empty = types_pathnodes::relids::relids_is_empty(&ri.right_relids);
        if left_empty {
            run.root.rinfo_mut(rid).outer_is_left = true;
        } else if right_empty {
            run.root.rinfo_mut(rid).outer_is_left = false;
        } else {
            continue;
        }
        restrict_rids.push(rid);
    }
    if restrict_rids.is_empty() && exprlist.is_empty() {
        return Ok(false);
    }

    let strip_relabel = |mut n: types_nodes::Node<'mcx>| {
        while let Some(r) = n.as_relabel_type() {
            n = r.arg;
        }
        n
    };
    let n_indexes = run.root.rel(rel_id).indexlist.len();
    for i in 0..n_indexes {
        let ind = run.root.rel(rel_id).indexlist[i];
        if !ind.unique || !ind.immediate || !ind.indpred.is_empty() {
            continue;
        }
        let mut all_matched = true;
        for c in 0..ind.nkeycolumns as usize {
            let mut matched = false;
            for &rid in restrict_rids.iter() {
                let ri = run.root.rinfo(rid);
                if !ri.mergeopfamilies.iter().any(|&f| f == ind.opfamily[c]) {
                    continue;
                }
                let clause = *run.root.expr_node(ri.clause);
                let o = clause.as_op_expr().expect("mergejoinable clause is an OpExpr");
                let rexpr = strip_relabel(if ri.outer_is_left {
                    o.args.nth(1)
                } else {
                    o.args.nth(0)
                });
                if planner_seams::match_index_to_operand::call(run, rexpr, c, ind) {
                    matched = true;
                    break;
                }
            }
            if !matched {
                for (j, &expr_id) in exprlist.iter().enumerate() {
                    let expr = strip_relabel(*run.root.expr_node(expr_id));
                    if !planner_seams::match_index_to_operand::call(run, expr, c, ind) {
                        continue;
                    }
                    if !lsyscache::amop::op_in_opfamily(oprlist[j], ind.opfamily[c])? {
                        continue;
                    }
                    matched = true;
                    break;
                }
            }
            if !matched {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            return Ok(true);
        }
    }
    Ok(false)
}

// create_subqueryscan_path (pathnode.c).
pub fn create_subqueryscan_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subroot_subpath: PathId,
    trivial_pathtarget: bool,
    pathkeys: PgVec<'mcx, PathKey>,
    required_outer: &types_pathnodes::Relids<'mcx>,
    sub: &SubqueryScanInfo,
) -> PgResult<PathId> {
    let param_info = get_baserel_parampathinfo(run, rel_id, required_outer)?;
    let mut path = base_path(run, NodeTag::T_SubqueryScanPath, NodeTag::T_SubqueryScan, rel_id);
    path.param_info = param_info;
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel && sub.parallel_safe;
    path.parallel_workers = sub.parallel_workers;
    path.pathkeys = pathkeys;
    let id = run.root.alloc_path(PathNode::SubqueryScanPath(types_pathnodes::SubqueryScanPath {
        path,
        subpath: None,
        subroot_subpath: Some(subroot_subpath),
    }));
    costsize::cost_subqueryscan(run, id, rel_id, sub, trivial_pathtarget)?;
    Ok(id)
}

// create_append_path (pathnode.c), serial arm: no partial subpaths, no
// parallel append, no required_outer, pathkeys always NIL (ordered-append is
// the MergeAppend/set-ops ordered lane).
pub fn create_append_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    subpaths: PgVec<'mcx, PathId>,
    rows: f64,
) -> PgResult<PathId> {
    let mut path = base_path(run, NodeTag::T_AppendPath, NodeTag::T_Append, rel_id);
    path.parallel_aware = false;
    path.parallel_safe = run.root.rel(rel_id).consider_parallel;
    let first_partial_path = subpaths.len() as i32;
    let limit_tuples = if types_pathnodes::relids::relids_equal(
        &run.root.rel(rel_id).relids,
        &run.root.all_query_rels,
    ) {
        run.root.limit_tuples
    } else {
        -1.0
    };
    for &sp in subpaths.iter() {
        let s = run.root.path(sp).base();
        debug_assert!(s.param_info.is_none());
        path.parallel_safe = path.parallel_safe && s.parallel_safe;
    }
    let single = (subpaths.len() == 1).then(|| subpaths[0]);
    let id = run.root.alloc_path(PathNode::AppendPath(types_pathnodes::AppendPath {
        path,
        subpaths,
        first_partial_path,
        limit_tuples,
    }));
    match single {
        // A single non-parallel-aware child makes the Append a no-op that
        // setrefs removes: inherit its size, cost and pathkeys.
        Some(child_id) => {
            let (c_rows, c_startup, c_total, c_keys) = {
                let c = run.root.path(child_id).base();
                debug_assert!(!c.parallel_aware);
                (
                    c.rows,
                    c.startup_cost,
                    c.total_cost,
                    types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &c.pathkeys),
                )
            };
            let p = run.root.path_mut(id).base_mut();
            p.rows = c_rows;
            p.startup_cost = c_startup;
            p.total_cost = c_total;
            p.pathkeys = c_keys;
        }
        None => costsize::cost_append(run, id),
    }
    if rows >= 0.0 {
        run.root.path_mut(id).base_mut().rows = rows;
    }
    Ok(id)
}

// create_setop_path (pathnode.c).
#[allow(clippy::too_many_arguments)]
pub fn create_setop_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    leftpath: PathId,
    rightpath: PathId,
    cmd: types_pathnodes::SetOpCmd,
    strategy: types_pathnodes::SetOpStrategy,
    group_list: PgVec<'mcx, types_pathnodes::NodeId>,
    num_groups: f64,
    output_rows: f64,
) -> PathId {
    let mcx = run.mcx;
    let n_group_cols = group_list.len() as f64;
    let (l_startup, l_total, l_rows, l_disabled, l_safe, l_workers, l_keys, l_width) = {
        let l = run.root.path(leftpath).base();
        (
            l.startup_cost,
            l.total_cost,
            l.rows,
            l.disabled_nodes,
            l.parallel_safe,
            l.parallel_workers,
            types_pathnodes::relids::pgvec_clone_shallow(mcx, &l.pathkeys),
            run.root.path_pathtarget(leftpath).width,
        )
    };
    let (r_startup, r_total, r_rows, r_disabled, r_safe, r_workers) = {
        let r = run.root.path(rightpath).base();
        (r.startup_cost, r.total_cost, r.rows, r.disabled_nodes, r.parallel_safe, r.parallel_workers)
    };
    let rel = run.root.rel(rel_id);
    let mut path = Path {
        type_: tag16(NodeTag::T_SetOpPath),
        pathtype: tag16(NodeTag::T_SetOp),
        parent: rel_id,
        pathtarget_id: rel.pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel && l_safe && r_safe,
        parallel_workers: l_workers + r_workers,
        rows: output_rows,
        disabled_nodes: l_disabled + r_disabled,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: if strategy == types_pathnodes::SETOP_SORTED {
            l_keys
        } else {
            PgVec::new_in(mcx)
        },
    };
    if strategy == types_pathnodes::SETOP_SORTED {
        // Sorted mode emits incrementally: one comparison per column per
        // input tuple.
        path.startup_cost = l_startup + r_startup;
        path.total_cost = l_total
            + r_total
            + gucs::cpu_operator_cost() * (l_rows + r_rows) * n_group_cols
            + gucs::cpu_operator_cost() * output_rows;
    } else {
        path.startup_cost = l_total
            + r_total
            + gucs::cpu_operator_cost() * (l_rows + r_rows) * n_group_cols;
        path.total_cost = path.startup_cost + gucs::cpu_operator_cost() * output_rows;
        if !gucs::enable_hashagg() {
            path.disabled_nodes += 1;
        }
        let hashentrysize = maxalign8(l_width.max(0) as usize)
            + maxalign8(types_tuple::SizeofMinimalTupleHeader);
        if hashentrysize as f64 * num_groups > ::nodehash::get_hash_memory_limit() as f64 {
            path.disabled_nodes += 1;
        }
    }
    run.root.alloc_path(PathNode::SetOpPath(types_pathnodes::SetOpPath {
        path,
        leftpath: Some(leftpath),
        rightpath: Some(rightpath),
        cmd,
        strategy,
        groupList: group_list,
        numGroups: num_groups,
    }))
}

const fn maxalign8(n: usize) -> usize {
    (n + 7) & !7
}

// apply_projection_to_path (pathnode.c); Gather/GatherMerge pushdown arms are
// dead (loud upstream).
pub fn apply_projection_to_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    path_id: PathId,
    target_id: PtId,
) -> PgResult<PathId> {
    let pathtype = run.root.path(path_id).base().pathtype;
    if !is_projection_capable_pathtype(pathtype) {
        let safe = clauses::is_parallel_safe_exprs(run, target_id)?;
        let pn = create_projection_path(run, rel_id, path_id, target_id, safe);
        return Ok(run.root.alloc_path(pn));
    }
    let old_target = run.root.path(path_id).base().pathtarget_id.expect("path has a pathtarget");
    let oldcost = run.root.pathtarget(old_target).cost;
    let newcost = run.root.pathtarget(target_id).cost;
    let p = run.root.path_mut(path_id).base_mut();
    p.pathtarget_id = Some(target_id);
    p.startup_cost += newcost.startup - oldcost.startup;
    p.total_cost += newcost.startup - oldcost.startup
        + (newcost.per_tuple - oldcost.per_tuple) * p.rows;
    Ok(path_id)
}

// create_minmaxagg_path (pathnode.c).
pub fn create_minmaxagg_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    target_id: PtId,
    mmaggregates: PgVec<'mcx, types_pathnodes::MinMaxAggInfo>,
    quals: PgVec<'mcx, types_pathnodes::NodeId>,
) -> PgResult<PathId> {
    let mut path = base_path(run, NodeTag::T_MinMaxAggPath, NodeTag::T_Result, rel_id);
    path.pathtarget_id = Some(target_id);
    path.parallel_safe = true;
    path.rows = 1.0;

    let mut initplan_cost = 0.0;
    let mut initplan_disabled_nodes = 0;
    for mminfo in mmaggregates.iter() {
        let sub = types_pathnodes::run::subroot_path_base(run, mminfo);
        initplan_disabled_nodes += sub.disabled_nodes;
        initplan_cost += mminfo.pathcost;
        if !sub.parallel_safe {
            path.parallel_safe = false;
        }
    }

    let tcost = run.root.pathtarget(target_id).cost;
    path.disabled_nodes = initplan_disabled_nodes;
    path.startup_cost = initplan_cost + tcost.startup;
    path.total_cost =
        initplan_cost + tcost.startup + tcost.per_tuple + gucs::cpu_tuple_cost();

    if !quals.is_empty() {
        let mut qual_cost = types_pathnodes::QualCost::default();
        for &qid in quals.iter() {
            let c = costsize::cost_qual_eval_node(*run.root.expr_node(qid))?;
            qual_cost.startup += c.startup;
            qual_cost.per_tuple += c.per_tuple;
        }
        path.startup_cost += qual_cost.startup;
        path.total_cost += qual_cost.startup + qual_cost.per_tuple;
    }

    if path.parallel_safe {
        path.parallel_safe = clauses::is_parallel_safe_exprs(run, target_id)?;
        if path.parallel_safe {
            for &qid in quals.iter() {
                if !clauses::is_parallel_safe_opt(run, Some(*run.root.expr_node(qid)))? {
                    path.parallel_safe = false;
                    break;
                }
            }
        }
    }

    Ok(run.root.alloc_path(PathNode::MinMaxAggPath(types_pathnodes::MinMaxAggPath {
        path,
        mmaggregates,
        quals,
    })))
}

// add_path_precheck (pathnode.c); required_outer is NULL on every path this
// lane can build.
#[allow(clippy::too_many_arguments)]
pub fn add_path_precheck(
    run: &PlannerRun<'_>,
    joinrel: RelId,
    disabled_nodes: i32,
    startup_cost: f64,
    total_cost: f64,
    pathkeys: &[PathKey],
    required_outer: &Relids<'_>,
) -> bool {
    let consider_startup = if types_pathnodes::relids::relids_is_empty(required_outer) {
        run.root.rel(joinrel).consider_startup
    } else {
        run.root.rel(joinrel).consider_param_startup
    };
    for &old_id in run.root.rel(joinrel).pathlist.iter() {
        let old = run.root.path(old_id).base();
        if old.disabled_nodes != disabled_nodes {
            if disabled_nodes < old.disabled_nodes {
                break;
            }
        } else if total_cost <= old.total_cost * STD_FUZZ_FACTOR {
            break;
        }
        if startup_cost > old.startup_cost * STD_FUZZ_FACTOR || !consider_startup {
            let keyscmp = compare_pathkeys(pathkeys, &old.pathkeys);
            if (keyscmp == PathKeysComparison::Equal || keyscmp == PathKeysComparison::Better2)
                && types_pathnodes::relids::relids_equal(required_outer, path_req_outer(old))
            {
                return false;
            }
        }
    }
    true
}

pub fn create_material_path(run: &mut PlannerRun<'_>, rel: RelId, subpath: PathId) -> PathId {
    let sub = run.root.path(subpath).base();
    debug_assert!(sub.parent == rel);
    let (sub_disabled, sub_startup, sub_total, sub_rows) =
        (sub.disabled_nodes, sub.startup_cost, sub.total_cost, sub.rows);
    let sub_parallel_safe = sub.parallel_safe;
    let sub_parallel_workers = sub.parallel_workers;
    debug_assert!(sub.param_info.is_none());
    let sub_pathkeys = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &sub.pathkeys);
    let width = run.root.path_pathtarget(subpath).width;

    let startup_cost = sub_startup;
    let mut run_cost = sub_total - sub_startup;
    run_cost += 2.0 * gucs::cpu_operator_cost() * sub_rows;
    let nbytes = costsize::relation_byte_size(sub_rows, width);
    let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
    if nbytes > work_mem_bytes {
        let npages = (nbytes / 8192.0).ceil();
        run_cost += gucs::seq_page_cost() * npages;
    }

    let path = Path {
        type_: tag16(NodeTag::T_MaterialPath),
        pathtype: tag16(NodeTag::T_Material),
        parent: rel,
        pathtarget_id: run.root.rel(rel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: run.root.rel(rel).consider_parallel && sub_parallel_safe,
        parallel_workers: sub_parallel_workers,
        rows: sub_rows,
        disabled_nodes: sub_disabled + if gucs::enable_material() { 0 } else { 1 },
        startup_cost,
        total_cost: startup_cost + run_cost,
        pathkeys: sub_pathkeys,
    };
    run.root
        .alloc_path(types_pathnodes::PathNode::MaterialPath(MaterialPath {
            path,
            subpath: Some(subpath),
        }))
}

// create_nestloop_path (pathnode.c).
#[allow(clippy::too_many_arguments)]
pub fn create_nestloop_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    restrict_clauses: &[RinfoId],
    required_outer: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<PathId> {
    use types_pathnodes::relids::{relids_copy, relids_overlap, relids_union};
    let mcx = run.mcx;

    // Clauses that get enforced at the parameterized inner scan are removed
    // from the join's own restrict list.
    let mut restrict_vec: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    restrict_vec.extend(restrict_clauses.iter().copied());
    let inner_req_outer = relids_copy(mcx, path_req_outer(run.root.path(inner_path).base()));
    let outer_parent_relids = {
        let r = run.root.path(outer_path).base().parent;
        relids_copy(mcx, &run.root.rel(r).relids)
    };
    if relids_overlap(&inner_req_outer, &outer_parent_relids) {
        let inner_parent_relids = {
            let r = run.root.path(inner_path).base().parent;
            relids_copy(mcx, &run.root.rel(r).relids)
        };
        let inner_and_outer = relids_union(mcx, &inner_parent_relids, &inner_req_outer);
        let mut jclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        for &rid in restrict_vec.iter() {
            if !join_clause_is_movable_into(run, rid, &inner_parent_relids, &inner_and_outer) {
                jclauses.push(rid);
            }
        }
        restrict_vec = jclauses;
    }

    let mut restrict_vec_for_ppi =
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &restrict_vec);
    let param_info =
        get_joinrel_parampathinfo(run, joinrel, required_outer, &mut restrict_vec_for_ppi)?;
    debug_assert!(param_info.is_none() || restrict_vec_for_ppi.len() == restrict_vec.len());

    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let joinrestrictinfo = restrict_vec;
    let mut pks: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    pks.extend(pathkeys.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_NestPath),
        pathtype: tag16(NodeTag::T_NestLoop),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: pks,
    };
    let mut node = NestPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
    };
    costsize::final_cost_nestloop(run, &mut node, workspace, semifactors)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::NestPath(node)))
}


#[allow(clippy::too_many_arguments)]
pub fn create_hashjoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    restrict_clauses: &[RinfoId],
    hashclauses: &[RinfoId],
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let mut joinrestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    joinrestrictinfo.extend(restrict_clauses.iter().copied());
    let mut path_hashclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    path_hashclauses.extend(hashclauses.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_HashPath),
        pathtype: tag16(NodeTag::T_HashJoin),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: PgVec::new_in(mcx),
    };
    let mut node = HashPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
        path_hashclauses,
        num_batches: workspace.numbatches,
        inner_rows_total: workspace.inner_rows_total,
    };
    costsize::final_cost_hashjoin(run, &mut node, workspace, semifactors)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::HashPath(node)))
}


// create_mergejoin_path (pathnode.c); required_outer is always NULL.
#[allow(clippy::too_many_arguments)]
pub fn create_mergejoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    restrict_clauses: &[RinfoId],
    pathkeys: PgVec<'mcx, PathKey>,
    mergeclauses: PgVec<'mcx, RinfoId>,
    outersortkeys: PgVec<'mcx, PathKey>,
    innersortkeys: PgVec<'mcx, PathKey>,
    outer_presorted_keys: usize,
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let mut joinrestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    joinrestrictinfo.extend(restrict_clauses.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_MergePath),
        pathtype: tag16(NodeTag::T_MergeJoin),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys,
    };
    let mut node = MergePath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
        path_mergeclauses: mergeclauses,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys: outer_presorted_keys as i32,
        skip_mark_restore: false,
        materialize_inner: false,
    };
    costsize::final_cost_mergejoin(run, &mut node, workspace, inner_unique)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::MergePath(node)))
}
