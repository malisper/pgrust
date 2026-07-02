use mcx::PgVec;
use types_error::{PgError, PgResult};
use types_nodes::list::NodeList;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    GroupResultPath, Path, PathId, PathNode, PathTarget, ProjectionPath, PtId, QualCost, RelId,
};

use crate::gucs;
use crate::run::PlannerRun;

pub fn tag16(tag: NodeTag) -> u16 {
    tag as u16
}

// cost_qual_eval_node (costsize.c): Consts cost nothing; everything else in
// the walker is deferred.
fn cost_qual_eval_node(node: Node<'_>) -> QualCost {
    match node.node_tag() {
        NodeTag::T_Const => QualCost::default(),
        other => panic!(
            "cost_qual_eval_walker (costsize.c): {other:?}; \
             backend-optimizer-path-costsize M2 lane"
        ),
    }
}

// get_expr_width (costsize.c); Var arms need rel attr_widths (M2 scan lane).
fn get_expr_width(node: Node<'_>) -> PgResult<i32> {
    let (typid, typmod) = match node.node_tag() {
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            (c.consttype, c.consttypmod)
        }
        other => panic!(
            "get_expr_width/exprType (costsize.c/nodeFuncs.c): {other:?}; \
             M2 expression lane"
        ),
    };
    let width = lsyscache::get_typavgwidth(typid, typmod)?;
    debug_assert!(width > 0);
    Ok(width)
}

const MAX_ALLOC_SIZE: i64 = 0x3fffffff;

fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > MAX_ALLOC_SIZE {
        return MAX_ALLOC_SIZE as i32;
    }
    debug_assert!(tuple_width >= 0);
    tuple_width as i32
}

// create_pathtarget (tlist.c): make_pathtarget_from_tlist +
// set_pathtarget_cost_width (costsize.c).
pub fn create_pathtarget<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<PtId> {
    let mcx = run.mcx;
    let mut target = PathTarget::new(mcx);
    let mut tuple_width: i64 = 0;
    let mut any_sortgroupref = false;

    for tle_node in tlist {
        let tle = tle_node
            .as_target_entry()
            .expect("targetList cell is a TargetEntry");
        tuple_width += get_expr_width(tle.expr)? as i64;
        if tle.expr.node_tag() != NodeTag::T_Var {
            let cost = cost_qual_eval_node(tle.expr);
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
    target.width = clamp_width_est(tuple_width);
    Ok(run.root.alloc_pathtarget(target))
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
        _ => panic!(
            "is_projection_capable_path (createplan.c): pathtype {pathtype}; \
             M2 plan lane"
        ),
    }
}

fn exprs_same(a: &PgVec<'_, types_pathnodes::NodeId>, b: &PgVec<'_, types_pathnodes::NodeId>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if a.as_slice() == b.as_slice() {
        return true;
    }
    panic!("equal() over expression trees (equalfuncs.c): M2 lane");
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

// add_path (pathnode.c): the cost-dominance pruning arms only matter once a
// rel can hold competing paths; every trivial-lane rel receives exactly one.
pub fn add_path<'mcx>(run: &mut PlannerRun<'mcx>, rel_id: RelId, path: PathNode<'mcx>) -> PathId {
    if !run.root.rel(rel_id).pathlist.is_empty() {
        panic!("add_path (pathnode.c): competing paths (compare_path_costs); M2 lane");
    }
    let id = run.root.alloc_path(path);
    run.root.rel_mut(rel_id).pathlist.push(id);
    id
}

pub fn add_existing_path(run: &mut PlannerRun<'_>, rel_id: RelId, path_id: PathId) {
    if !run.root.rel(rel_id).pathlist.is_empty() {
        panic!("add_path (pathnode.c): competing paths (compare_path_costs); M2 lane");
    }
    run.root.rel_mut(rel_id).pathlist.push(path_id);
}

#[cold]
#[inline(never)]
fn no_plan_error() -> PgError {
    PgError::error("could not devise a query plan for the given query".to_string())
}

// set_cheapest (pathnode.c).
pub fn set_cheapest(run: &mut PlannerRun<'_>, rel_id: RelId) -> PgResult<()> {
    let rel = run.root.rel(rel_id);
    if rel.pathlist.is_empty() {
        return Err(no_plan_error().into());
    }
    if rel.pathlist.len() > 1 {
        panic!("set_cheapest (pathnode.c): compare_path_costs over multiple paths; M2 lane");
    }
    let only = rel.pathlist[0];
    if run.root.path(only).base().param_info.is_some() {
        panic!("set_cheapest (pathnode.c): parameterized paths; M2 lane");
    }
    let rel = run.root.rel_mut(rel_id);
    rel.cheapest_startup_path = Some(only);
    rel.cheapest_total_path = Some(only);
    rel.cheapest_unique_path = None;
    rel.cheapest_parameterized_paths.clear();
    rel.cheapest_parameterized_paths.push(only);
    Ok(())
}

// get_cheapest_fractional_path (planner.c).
pub fn get_cheapest_fractional_path(run: &PlannerRun<'_>, rel_id: RelId, tuple_fraction: f64) -> PathId {
    let best = run
        .root
        .rel(rel_id)
        .cheapest_total_path
        .expect("set_cheapest ran");
    if tuple_fraction <= 0.0 {
        return best;
    }
    panic!(
        "get_cheapest_fractional_path (planner.c): compare_fractional_path_costs; \
         M2 cursor lane"
    );
}
