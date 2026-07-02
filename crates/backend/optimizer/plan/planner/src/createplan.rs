use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::plannodes::Result as ResultPlan;
use types_nodes::primnodes::TargetEntry;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{PathId, PathNode, PtId};

use crate::pathnode::is_projection_capable_pathtype;
use crate::run::PlannerRun;

pub const CP_EXACT_TLIST: i32 = 0x0001;
pub const CP_IGNORE_TLIST: i32 = 0x0008;

pub fn create_plan<'mcx>(run: &mut PlannerRun<'mcx>, best_path: PathId) -> PgResult<Node<'mcx>> {
    debug_assert!(run.root.plan_params.is_empty());
    run.root.curOuterRels = None;
    run.root.curOuterParams.clear();

    let plan = create_plan_recurse(run, best_path, CP_EXACT_TLIST)?;

    if plan.node_tag() != NodeTag::T_ModifyTable {
        apply_tlist_labeling(plan, run.processed_tlist());
    }
    // SS_attach_initplans: no initplans exist on this lane.
    debug_assert!(run.root.init_plans.is_empty());
    assert!(run.root.curOuterParams.is_empty(), "unassigned NestLoopParams");
    run.root.plan_params.clear();
    Ok(plan)
}

fn create_plan_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    match run.root.path(path_id) {
        PathNode::ProjectionPath(_) => create_projection_plan(run, path_id, flags),
        PathNode::GroupResultPath(_) => create_group_result_plan(run, path_id),
        other => panic!(
            "create_plan_recurse (createplan.c): pathtype {}; M2 plan lane",
            other.base().pathtype
        ),
    }
}

// use_physical_tlist is false on every reachable input: CP_EXACT_TLIST is
// always demanded and the parent rel is never a physical scan rel here.
fn create_projection_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(flags & CP_EXACT_TLIST != 0);
    let (subpath_id, target_id, path_costs) = match run.root.path(path_id) {
        PathNode::ProjectionPath(pp) => (
            pp.subpath.expect("projection has a subpath"),
            pp.path.pathtarget_id.unwrap(),
            (
                pp.path.startup_cost,
                pp.path.total_cost,
                pp.path.rows,
                pp.path.parallel_safe,
            ),
        ),
        _ => unreachable!(),
    };
    if !is_projection_capable_pathtype(run.root.path(subpath_id).base().pathtype) {
        panic!("create_projection_plan (createplan.c): separate Result arm; M2 lane");
    }

    let subplan = create_plan_recurse(run, subpath_id, CP_IGNORE_TLIST)?;
    let tlist = build_path_tlist(run, target_id)?;
    let width = run.root.pathtarget(target_id).width;

    // C scribbles the new tlist and label costs onto the just-built subplan.
    // SAFETY: subplan was created above; no other handle to it exists yet.
    unsafe {
        subplan.with_plan_mut(|p| {
            p.targetlist = tlist;
            p.startup_cost = path_costs.0;
            p.total_cost = path_costs.1;
            p.plan_rows = path_costs.2;
            p.plan_width = width;
            p.parallel_safe = path_costs.3;
        })
    }
    .expect("subplan embeds a Plan base");
    Ok(subplan)
}

fn create_group_result_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let (target_id, costs) = match run.root.path(path_id) {
        PathNode::GroupResultPath(grp) => {
            if !grp.quals.is_empty() {
                panic!("order_qual_clauses (createplan.c): M2 qual lane");
            }
            (
                grp.path.pathtarget_id.unwrap(),
                (
                    grp.path.startup_cost,
                    grp.path.total_cost,
                    grp.path.rows,
                    grp.path.parallel_safe,
                ),
            )
        }
        _ => unreachable!(),
    };
    let tlist = build_path_tlist(run, target_id)?;
    let width = run.root.pathtarget(target_id).width;

    // make_result + copy_generic_path_info.
    let mut plan = Node::build::<ResultPlan>(run.mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.startup_cost = costs.0;
    plan.plan.total_cost = costs.1;
    plan.plan.plan_rows = costs.2;
    plan.plan.plan_width = width;
    plan.plan.parallel_safe = costs.3;
    Ok(plan.seal())
}

// build_path_tlist; parameterized paths can't reach here.
fn build_path_tlist<'mcx>(run: &mut PlannerRun<'mcx>, target_id: PtId) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let n = run.root.pathtarget(target_id).exprs.len();
    let mut tlist = NodeList::nil();
    for i in 0..n {
        let target = run.root.pathtarget(target_id);
        let expr = *run.root.expr_node(target.exprs[i]);
        let ressortgroupref = target.sortgrouprefs.get(i).copied().unwrap_or(0);
        let tle = Node::mk(
            mcx,
            TargetEntry {
                expr,
                resno: (i + 1) as i16,
                resname: None,
                ressortgroupref,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )?;
        tlist.lappend(mcx, tle)?;
    }
    Ok(tlist)
}

// Copies the querytree tlist's decoration onto the plan tlist, in place as C.
fn apply_tlist_labeling<'mcx>(plan: Node<'mcx>, src_tlist: &NodeList<'mcx>) {
    let dest_tlist = &plan.as_plan().expect("plan node").targetlist;
    assert_eq!(dest_tlist.len(), src_tlist.len());
    for (dest_node, src_node) in dest_tlist.iter().zip(src_tlist.iter()) {
        let src = src_node.as_target_entry().expect("TargetEntry");
        // SAFETY: dest tlist entries were freshly built by build_path_tlist;
        // no reference derived from them is live across this mutation.
        unsafe {
            dest_node.with_mut::<TargetEntry, _>(|dest| {
                debug_assert_eq!(dest.resno, src.resno);
                dest.resname = src.resname;
                dest.ressortgroupref = src.ressortgroupref;
                dest.resorigtbl = src.resorigtbl;
                dest.resorigcol = src.resorigcol;
                dest.resjunk = src.resjunk;
            })
        }
        .expect("dest tlist cell is a TargetEntry");
    }
}
