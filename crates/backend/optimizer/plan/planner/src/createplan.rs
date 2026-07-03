use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::plannodes::{IndexScan, Plan, Result as ResultPlan, SeqScan};
use types_nodes::primnodes::{OpExpr, TargetEntry};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{IndexOptInfo, PathId, PathNode, PtId, RinfoId};

use crate::pathnode::is_projection_capable_pathtype;
use crate::run::PlannerRun;

pub const CP_EXACT_TLIST: i32 = 0x0001;
pub const CP_SMALL_TLIST: i32 = 0x0002;
pub const CP_LABEL_TLIST: i32 = 0x0004;
pub const CP_IGNORE_TLIST: i32 = 0x0008;

const INDEX_VAR: i32 = -3;

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
        PathNode::Path(p) if p.pathtype == crate::pathnode::tag16(NodeTag::T_SeqScan) => {
            create_scan_plan(run, path_id, flags)
        }
        PathNode::IndexPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::ProjectionPath(_) => create_projection_plan(run, path_id, flags),
        PathNode::GroupResultPath(_) => create_group_result_plan(run, path_id),
        other => panic!(
            "create_plan_recurse (createplan.c): pathtype {}; M2 plan lane",
            other.base().pathtype
        ),
    }
}

// use_physical_tlist (createplan.c): live callers demand exact/ignored.
fn use_physical_tlist(flags: i32) -> bool {
    if flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        return false;
    }
    panic!("build_physical_tlist (plancat.c): M2 physical-tlist lane");
}

// create_scan_plan (createplan.c).
fn create_scan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let pathtype = run.root.path(best_path).base().pathtype;

    let scan_clauses: mcx::PgVec<'mcx, RinfoId> = {
        let mut v = mcx::PgVec::new_in(mcx);
        if let PathNode::IndexPath(ip) = run.root.path(best_path) {
            v.extend(
                ip.indexinfo
                    .as_ref()
                    .expect("indexinfo set")
                    .indrestrictinfo
                    .borrow()
                    .iter()
                    .copied(),
            );
        } else {
            v.extend(run.root.rel(rel_id).baserestrictinfo.iter().copied());
        }
        v
    };
    debug_assert!(run.root.path(best_path).base().param_info.is_none());

    // get_gating_quals: pseudoconstant quals panicked in the qual lane.
    debug_assert!(scan_clauses.iter().all(|&r| !run.root.rinfo(r).pseudoconstant));

    let tlist = if flags == CP_IGNORE_TLIST {
        NodeList::nil()
    } else if use_physical_tlist(flags) {
        unreachable!()
    } else {
        let target_id = run.root.path(best_path).base().pathtarget_id.unwrap();
        build_path_tlist(run, target_id)?
    };

    match pathtype {
        t if t == crate::pathnode::tag16(NodeTag::T_SeqScan) => {
            create_seqscan_plan(run, best_path, tlist, scan_clauses)
        }
        t if t == crate::pathnode::tag16(NodeTag::T_IndexScan) => {
            create_indexscan_plan(run, best_path, tlist, scan_clauses)
        }
        t if t == crate::pathnode::tag16(NodeTag::T_IndexOnlyScan) => {
            panic!("create_indexscan_plan (createplan.c): index-only scan; M2 IOS lane")
        }
        other => panic!("create_scan_plan (createplan.c): pathtype {other}; M2 scan lane"),
    }
}

// order_qual_clauses (createplan.c): stable sort by ascending eval cost.
fn order_qual_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
) -> PgResult<mcx::PgVec<'mcx, RinfoId>> {
    let mut items: mcx::PgVec<'_, (RinfoId, f64, u32)> = mcx::PgVec::new_in(run.mcx);
    items.reserve(clauses.len());
    for &rid in clauses {
        debug_assert!(run.root.rinfo(rid).security_level == 0);
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let cost = crate::costsize::cost_qual_eval_node(clause)?;
        items.push((rid, cost.per_tuple, run.root.rinfo(rid).security_level));
    }
    if items.len() > 1 {
        items.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.partial_cmp(&b.1).unwrap()));
    }
    let mut out = mcx::PgVec::new_in(run.mcx);
    out.extend(items.iter().map(|x| x.0));
    Ok(out)
}
fn extract_actual_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    rinfos: &[RinfoId],
) -> NodeList<'mcx> {
    let mut out = NodeList::nil();
    for &rid in rinfos {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        out.lappend(run.mcx, *run.root.expr_node(run.root.rinfo(rid).clause))
            .expect("lappend");
    }
    out
}

fn copy_generic_path_info(run: &PlannerRun<'_>, plan: &mut Plan<'_>, path_id: PathId) {
    let p = run.root.path(path_id).base();
    plan.disabled_nodes = p.disabled_nodes;
    plan.startup_cost = p.startup_cost;
    plan.total_cost = p.total_cost;
    plan.plan_rows = p.rows;
    plan.plan_width = p
        .pathtarget_id
        .map(|id| run.root.pathtarget(id).width)
        .unwrap_or(0);
    plan.parallel_aware = p.parallel_aware;
    plan.parallel_safe = p.parallel_safe;
}

// create_seqscan_plan (createplan.c).
fn create_seqscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let scan_relid = run.root.rel(run.root.path(best_path).base().parent).relid;
    debug_assert!(scan_relid > 0);

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<SeqScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_indexscan_plan (createplan.c), plain-IndexScan arm.
fn create_indexscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (indexoid, indexscandir, baserelid, indexclause_rinfos) = {
        let PathNode::IndexPath(p) = run.root.path(best_path) else {
            panic!("create_indexscan_plan: not an IndexPath")
        };
        debug_assert!(p.indexorderbys.is_empty());
        let mut rids = mcx::PgVec::new_in(mcx);
        for ic in p.indexclauses.iter() {
            rids.push(ic.rinfo.expect("IndexClause rinfo"));
        }
        (
            p.indexinfo.as_ref().expect("indexinfo set").indexoid,
            p.indexscandir,
            p.path.parent,
            rids,
        )
    };
    let scan_relid = run.root.rel(baserelid).relid;
    debug_assert!(scan_relid > 0);
    debug_assert!(indexscandir == 1 || indexscandir == -1);

    let (stripped_indexquals, fixed_indexquals) = fix_indexqual_references(run, best_path)?;

    let mut qpqual_rinfos: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(mcx);
    for &rid in scan_clauses.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        // is_redundant_with_indexclauses: no EC parents, so rinfo identity.
        if indexclause_rinfos.iter().any(|&c| c == rid) {
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if !clauses::contain_mutable_functions(clause)? {
            panic!("predicate_implied_by (predtest.c): M2 predicate lane");
        }
        qpqual_rinfos.push(rid);
    }
    let ordered = order_qual_clauses(run, &qpqual_rinfos)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<IndexScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.indexid = indexoid;
    plan.indexqual = fixed_indexquals;
    plan.indexqualorig = stripped_indexquals;
    plan.indexorderby = NodeList::nil();
    plan.indexorderbyorig = NodeList::nil();
    plan.indexorderbyops = types_nodes::list::OidList::nil();
    plan.indexorderdir = indexscandir;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// fix_indexqual_references (createplan.c) -> (stripped, fixed) qual lists.
fn fix_indexqual_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let mcx = run.mcx;
    let (index, iclauses) = {
        let PathNode::IndexPath(p) = run.root.path(best_path) else { unreachable!() };
        (
            std::rc::Rc::clone(p.indexinfo.as_ref().expect("indexinfo set")),
            p.indexclauses.clone(),
        )
    };
    let mut stripped = NodeList::nil();
    let mut fixed = NodeList::nil();
    for ic in iclauses.iter() {
        for &rid in ic.indexquals.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            stripped.lappend(mcx, clause)?;
            fixed.lappend(mcx, fix_indexqual_clause(run, &index, ic.indexcol as i32, clause)?)?;
        }
    }
    Ok((stripped, fixed))
}

// fix_indexqual_clause (createplan.c); C's in-place scribble becomes a
// rebuilt OpExpr (the original stays shared as indexqualorig).
fn fix_indexqual_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    indexcol: i32,
    clause: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    match clause.node_tag() {
        NodeTag::T_OpExpr => {
            let o = clause.as_op_expr().unwrap();
            debug_assert!(o.args.len() == 2);
            let fixed_arg = fix_indexqual_operand(run, index, indexcol, o.args.nth(0))?;
            Node::mk(
                mcx,
                OpExpr {
                    opno: o.opno,
                    opfuncid: o.opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args: NodeList::make2(mcx, fixed_arg, o.args.nth(1))?,
                    location: o.location,
                },
            )
        }
        other => panic!("fix_indexqual_clause (createplan.c): {other:?}; M2 lane"),
    }
}

// fix_indexqual_operand (createplan.c), simple-column arm.
fn fix_indexqual_operand<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    indexcol: i32,
    mut node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    while node.node_tag() == NodeTag::T_RelabelType {
        node = node.as_relabel_type().unwrap().arg;
    }
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    if let Some(var) = node.as_var() {
        if var.varno as u32 == index_relid && index.indexkeys[indexcol as usize] != 0 {
            if index.indexkeys[indexcol as usize] == var.varattno as i32 {
                return Node::mk_var(
                    mcx,
                    INDEX_VAR,
                    (indexcol + 1) as i16,
                    var.vartype,
                    var.vartypmod,
                    var.varcollid,
                    0,
                );
            }
            panic!("index key does not match expected index column");
        }
    }
    panic!("fix_indexqual_operand (createplan.c): expression column; M2 lane");
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
