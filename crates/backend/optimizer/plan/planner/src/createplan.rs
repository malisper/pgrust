use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::plannodes::{Agg, IndexScan, Plan, Result as ResultPlan, SeqScan};
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
        PathNode::BitmapHeapPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::ProjectionPath(_) => create_projection_plan(run, path_id, flags),
        PathNode::GroupResultPath(_) => create_group_result_plan(run, path_id),
        PathNode::AggPath(_) => create_agg_plan(run, path_id),
        PathNode::SortPath(_) => create_sort_plan(run, path_id, flags),
        PathNode::NestPath(_) => create_join_plan(run, path_id),
        PathNode::LimitPath(_) => create_limit_plan(run, path_id, flags),
        PathNode::ModifyTablePath(_) => create_modifytable_plan(run, path_id),
        other => panic!(
            "create_plan_recurse (createplan.c): pathtype {}; M2 plan lane",
            other.base().pathtype
        ),
    }
}

// use_physical_tlist (createplan.c), plain-baserel arm.
fn use_physical_tlist(run: &PlannerRun<'_>, best_path: PathId, flags: i32) -> bool {
    if flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        return false;
    }
    let base = run.root.path(best_path).base();
    let rel_id = base.parent;
    let rel = run.root.rel(rel_id);
    if rel.rtekind != types_pathnodes::RTE_RELATION
        || rel.reloptkind != types_pathnodes::RELOPT_BASEREL
    {
        return false;
    }
    for attno in rel.min_attr..=0 {
        let ndx = (attno - rel.min_attr) as usize;
        if !crate::relnode::relids_is_empty(&rel.attr_needed[ndx]) {
            return false;
        }
    }
    debug_assert!(run.root.placeholder_list.is_empty());
    if base.pathtype == crate::pathnode::tag16(NodeTag::T_IndexOnlyScan) {
        panic!("use_physical_tlist (createplan.c): index-only indextlist; M2 IOS lane");
    }
    if flags & CP_LABEL_TLIST != 0 {
        let target = run.root.pathtarget(base.pathtarget_id.unwrap());
        if target.sortgrouprefs.iter().any(|&s| s != 0) {
            return false;
        }
    }
    true
}

// build_physical_tlist (plancat.c), heap-relation arm.
fn build_physical_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: types_pathnodes::RelId,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel_id).relid;
    let reloid = run.rte(varno as usize).relid;
    let relation = table::table_open(mcx, reloid, 0)?;
    let mut tlist = NodeList::nil();
    for att in relation.rd_att.attrs.iter() {
        assert!(
            !att.attisdropped,
            "build_physical_tlist (plancat.c): dropped column NULL Const; M2 lane"
        );
        let var = Node::mk_var(
            mcx,
            varno as i32,
            att.attnum,
            att.atttypid,
            att.atttypmod,
            att.attcollation,
            0,
        )?;
        let tle = Node::mk_target_entry(mcx, var, att.attnum, None, false)?;
        tlist.lappend(mcx, tle)?;
    }
    Ok(tlist)
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
    } else if use_physical_tlist(run, best_path, flags) {
        let physical = build_physical_tlist(run, rel_id)?;
        if flags & CP_LABEL_TLIST != 0 {
            // apply_pathtarget_labeling_to_tlist: no sortgrouprefs to copy
            // (use_physical_tlist refused any labeled target).
        }
        physical
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
        t if t == crate::pathnode::tag16(NodeTag::T_BitmapHeapScan) => {
            create_bitmap_scan_plan(run, best_path, tlist, scan_clauses)
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

// create_bitmap_scan_plan (createplan.c). indexECs bookkeeping is dead while
// eq_classes are empty; nestloop-param replacement loud upstream (param_info).
fn create_bitmap_scan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (baserelid, bitmapqual) = {
        let PathNode::BitmapHeapPath(p) = run.root.path(best_path) else {
            panic!("create_bitmap_scan_plan: not a BitmapHeapPath")
        };
        debug_assert!(!p.path.parallel_aware);
        (p.path.parent, p.bitmapqual.expect("BitmapHeapPath bitmapqual"))
    };
    let scan_relid = run.root.rel(baserelid).relid;
    debug_assert!(scan_relid > 0);

    let (bitmapqualplan, indexquals, mut bitmapqualorig) =
        create_bitmap_subplan(run, bitmapqual)?;

    // scan_clauses minus indexquals (C list_member -> equal()).
    let mut qpqual_rinfos: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(mcx);
    for &rid in scan_clauses.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if indexquals.iter().any(|q| types_nodes::equal(q, clause)) {
            continue;
        }
        if !clauses::contain_mutable_functions(clause)? {
            panic!("predicate_implied_by (predtest.c): M2 predicate lane");
        }
        qpqual_rinfos.push(rid);
    }
    let ordered = order_qual_clauses(run, &qpqual_rinfos)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    // list_difference_ptr(bitmapqualorig, qpqual): drop double-tested clauses.
    if !qpqual.is_nil() {
        let mut kept = NodeList::nil();
        for orig in bitmapqualorig.iter() {
            if !qpqual.iter().any(|q| q.ptr_eq(orig)) {
                kept.lappend(mcx, orig)?;
            }
        }
        bitmapqualorig = kept;
    }

    let mut plan = Node::build::<types_nodes::plannodes::BitmapHeapScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.plan.lefttree = Some(bitmapqualplan);
    plan.scan.scanrelid = scan_relid;
    plan.bitmapqualorig = bitmapqualorig;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_bitmap_subplan (createplan.c), IndexPath arm -> (plan, indexquals,
// bitmapqualorig); the BitmapAnd/OrPath arms ride the bitmap-combine lane.
// indexECs output is dropped (eq_classes empty on this lane).
fn create_bitmap_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    bitmapqual: PathId,
) -> PgResult<(Node<'mcx>, NodeList<'mcx>, NodeList<'mcx>)> {
    let mcx = run.mcx;
    let (indexclauses, indexselectivity, parent, parallel_safe, has_indpred) = {
        match run.root.path(bitmapqual) {
            PathNode::IndexPath(ip) => (
                ip.indexclauses.clone(),
                ip.indexselectivity,
                ip.path.parent,
                ip.path.parallel_safe,
                !ip.indexinfo.as_ref().expect("indexinfo set").indpred.is_empty(),
            ),
            PathNode::BitmapAndPath(_) | PathNode::BitmapOrPath(_) => panic!(
                "create_bitmap_subplan (createplan.c): BitmapAnd/Or; M2 bitmap-combine lane"
            ),
            other => panic!(
                "create_bitmap_subplan (createplan.c): pathtype {}",
                other.base().pathtype
            ),
        }
    };
    assert!(
        !has_indpred,
        "create_bitmap_subplan (createplan.c): partial-index indpred; M2 predicate lane"
    );

    // C builds a throwaway IndexScan via create_indexscan_plan and moves its
    // qual lists over; the direct fix_indexqual_references call is the same
    // computation without the discarded node.
    let (stripped_indexquals, fixed_indexquals) = fix_indexqual_references(run, bitmapqual)?;

    let (indexoid, indextotalcost, tuples) = {
        let PathNode::IndexPath(ip) = run.root.path(bitmapqual) else { unreachable!() };
        (
            ip.indexinfo.as_ref().expect("indexinfo set").indexoid,
            ip.indextotalcost,
            run.root.rel(parent).tuples,
        )
    };
    let mut plan = Node::build::<types_nodes::plannodes::BitmapIndexScan<'mcx>>(mcx)?;
    plan.scan.scanrelid = run.root.rel(parent).relid;
    plan.indexid = indexoid;
    plan.isshared = false;
    plan.indexqual = fixed_indexquals;
    plan.indexqualorig = stripped_indexquals;
    plan.scan.plan.startup_cost = 0.0;
    plan.scan.plan.total_cost = indextotalcost;
    plan.scan.plan.plan_rows =
        crate::costsize::clamp_row_est(indexselectivity * tuples);
    plan.scan.plan.plan_width = 0;
    plan.scan.plan.parallel_aware = false;
    plan.scan.plan.parallel_safe = parallel_safe;

    let mut subquals = NodeList::nil();
    let mut subindexquals = NodeList::nil();
    for ic in indexclauses.iter() {
        let rid = ic.rinfo.expect("IndexClause rinfo");
        debug_assert!(!run.root.rinfo(rid).pseudoconstant);
        subquals.lappend(mcx, *run.root.expr_node(run.root.rinfo(rid).clause))?;
        for &qid in ic.indexquals.iter() {
            subindexquals.lappend(mcx, *run.root.expr_node(run.root.rinfo(qid).clause))?;
        }
    }
    Ok((plan.seal(), subindexquals, subquals))
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

// create_modifytable_plan + make_modifytable (createplan.c), single-relation
// INSERT/UPDATE/DELETE arm: no FDW result rels, no ON CONFLICT/MERGE/
// RETURNING lists.
fn create_modifytable_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, operation, can_set_tag, nominal, root_rel, result_relations, epq_param) = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        debug_assert!(
            p.withCheckOptionLists.is_empty()
                && p.returningLists.is_empty()
                && p.rowMarks.is_empty()
                && p.onconflict.is_none()
                && p.mergeActionLists.is_empty()
        );
        (
            p.subpath.expect("ModifyTablePath has a subpath"),
            p.operation,
            p.canSetTag,
            p.nominalRelation,
            p.rootRelation,
            crate::relnode::pgvec_clone_shallow(mcx, &p.resultRelations),
            p.epqParam,
        )
    };
    use types_nodes::nodes_enums::CmdType;
    let operation = match operation {
        x if x == CmdType::CMD_INSERT as u32 => CmdType::CMD_INSERT,
        x if x == CmdType::CMD_UPDATE as u32 => CmdType::CMD_UPDATE,
        x if x == CmdType::CMD_DELETE as u32 => CmdType::CMD_DELETE,
        other => panic!("make_modifytable (createplan.c): operation {other}; M4 MERGE lane"),
    };

    let subplan = create_plan_recurse(run, subpath_id, CP_EXACT_TLIST)?;
    apply_tlist_labeling(subplan, run.processed_tlist());

    let update_colnos_lists = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        debug_assert!(p.updateColnosLists.len() <= 1);
        let mut lists = types_nodes::list::NodeList::nil();
        for colnos in p.updateColnosLists.iter() {
            let mut il = types_nodes::list::IntList::nil();
            for &c in colnos.iter() {
                il.lappend(mcx, c as i32)?;
            }
            lists.lappend(mcx, Node::mk_int_list(mcx, il)?)?;
        }
        lists
    };

    let mut plan = Node::build::<types_nodes::plannodes::ModifyTable>(mcx)?;
    plan.plan.lefttree = Some(subplan);
    plan.operation = operation;
    plan.updateColnosLists = update_colnos_lists;
    plan.canSetTag = can_set_tag;
    plan.nominalRelation = nominal;
    plan.rootRelation = root_rel;
    let mut rr = types_nodes::list::IntList::nil();
    for &rti in result_relations.iter() {
        rr.lappend(mcx, rti)?;
    }
    plan.resultRelations = rr;
    plan.epqParam = epq_param;
    plan.returningOldAlias = run.parse().returningOldAlias;
    plan.returningNewAlias = run.parse().returningNewAlias;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
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

// create_agg_plan + make_agg (createplan.c); HAVING quals are the M3 lane.
fn create_agg_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id, aggstrategy, aggsplit, num_groups, transition_space) =
        match run.root.path(path_id) {
            PathNode::AggPath(ap) => {
                assert!(
                    ap.qual.is_empty(),
                    "create_agg_plan (createplan.c): HAVING quals; M3 having lane"
                );
                (
                    ap.subpath.expect("AggPath has a subpath"),
                    ap.path.pathtarget_id.unwrap(),
                    ap.aggstrategy,
                    ap.aggsplit,
                    ap.numGroups,
                    ap.transitionSpace,
                )
            }
            _ => unreachable!(),
        };

    // Agg can project, so no need to be picky about the child tlist, but the
    // grouping columns must be available (CP_LABEL_TLIST).
    let subplan = create_plan_recurse(run, subpath_id, CP_LABEL_TLIST)?;
    let tlist = build_path_tlist(run, target_id)?;

    // extract_grouping_cols/ops/collations (tlist.c) against the subplan tlist.
    let group_clause = match run.root.path(path_id) {
        PathNode::AggPath(ap) => {
            crate::relnode::pgvec_clone_shallow(run.mcx, &ap.groupClause)
        }
        _ => unreachable!(),
    };
    let num_cols = group_clause.len();
    let mut grp_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(run.mcx);
    let mut grp_operators: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(run.mcx);
    let mut grp_collations: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(run.mcx);
    let subplan_tlist = &subplan.as_plan().expect("plan node").targetlist;
    for i in 0..num_cols {
        let (sgref, eqop) = {
            let scl = run
                .root
                .expr_node(group_clause[i])
                .as_sort_group_clause()
                .expect("AggPath.groupClause cell");
            (scl.tleSortGroupRef, scl.eqop)
        };
        // get_sortgroupclause_tle (tlist.c); a miss is C's elog(ERROR).
        let tle_node = subplan_tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sgref)
            .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
        let tle = tle_node.as_target_entry().unwrap();
        grp_col_idx.push(tle.resno);
        grp_operators.push(eqop);
        grp_collations.push(expr_collation(tle.expr));
    }

    let mut plan = Node::build::<Agg>(run.mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.aggstrategy = aggstrategy;
    plan.aggsplit = aggsplit;
    plan.numCols = num_cols as i32;
    plan.grpColIdx = mcx::vec_borrow_in(run.mcx, grp_col_idx)?;
    plan.grpOperators = mcx::vec_borrow_in(run.mcx, grp_operators)?;
    plan.grpCollations = mcx::vec_borrow_in(run.mcx, grp_collations)?;
    plan.numGroups = clamp_cardinality_to_long(num_groups);
    plan.transitionSpace = transition_space;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// exprCollation (nodeFuncs.c) over the grouping-column families.
fn expr_collation(node: Node<'_>) -> types_core::Oid {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        tag => panic!("exprCollation (nodeFuncs.c): node family {tag:?} not ported here"),
    }
}

fn clamp_cardinality_to_long(x: f64) -> i64 {
    if x < i64::MAX as f64 {
        x as i64
    } else {
        i64::MAX
    }
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

// create_sort_plan + make_sort_from_pathkeys + make_sort (createplan.c).
fn create_sort_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath_id, pathkeys) = {
        let PathNode::SortPath(sp) = run.root.path(path_id) else { unreachable!() };
        (
            sp.subpath.expect("SortPath has a subpath"),
            crate::relnode::pgvec_clone_shallow(run.mcx, &sp.path.pathkeys),
        )
    };
    // Sort can't project: request a tlist without excess columns.
    let subplan = create_plan_recurse(run, subpath_id, flags | CP_SMALL_TLIST)?;
    // IS_OTHER_REL child sorts can't arise (append lanes are loud).
    let plan = make_sort_from_pathkeys(run, subplan, &pathkeys)?;
    // SAFETY: plan was freshly built above; no other handle exists yet.
    unsafe {
        plan.with_plan_mut(|p| {
            let base = run.root.path(path_id).base();
            p.disabled_nodes = base.disabled_nodes;
            p.startup_cost = base.startup_cost;
            p.total_cost = base.total_cost;
            p.plan_rows = base.rows;
            p.plan_width = base
                .pathtarget_id
                .map(|id| run.root.pathtarget(id).width)
                .unwrap_or(0);
            p.parallel_aware = base.parallel_aware;
            p.parallel_safe = base.parallel_safe;
        })
    }
    .expect("Sort embeds a Plan base");
    Ok(plan)
}

// find_ec_member_matching_expr (equivclass.c); relids=NULL so child members
// are skipped.
fn find_ec_member_matching_expr<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: types_pathnodes::EcId,
    expr: Node<'mcx>,
) -> Option<types_pathnodes::EmId> {
    let mut expr = expr;
    while let Some(r) = expr.as_relabel_type() {
        expr = r.arg;
    }
    for &em_id in run.root.ec(ec).ec_members.iter() {
        let em = run.root.em(em_id);
        if em.em_is_child || em.em_is_const {
            continue;
        }
        let mut em_expr = *run.root.expr_node(em.em_expr);
        while let Some(r) = em_expr.as_relabel_type() {
            em_expr = r.arg;
        }
        if types_nodes::equal(em_expr, expr) {
            return Some(em_id);
        }
    }
    None
}

// prepare_sort_from_pathkeys + make_sort (createplan.c): every pathkey must
// match an existing tlist column (the resjunk-entry-injection leg is loud).
fn make_sort_from_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    lefttree: Node<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    // C shares lefttree->targetlist by pointer; flat cell copy, shared nodes.
    let tlist = NodeList::from_slice(mcx, lefttree.as_plan().expect("plan node").targetlist.as_slice())?;
    let mut sort_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut sort_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);

    for pathkey in pathkeys {
        let ec = pathkey.pk_eclass.expect("canonical pathkey has an eclass");
        assert!(
            !run.root.ec(ec).ec_has_volatile,
            "prepare_sort_from_pathkeys (createplan.c): volatile sortref leg; M2 lane"
        );
        let mut found: Option<(i16, u32)> = None;
        for tle_node in &tlist {
            let tle = tle_node.as_target_entry().expect("TargetEntry");
            if let Some(em_id) = find_ec_member_matching_expr(run, ec, tle.expr) {
                found = Some((tle.resno, run.root.em(em_id).em_datatype));
                break;
            }
        }
        let Some((resno, pk_datatype)) = found else {
            panic!(
                "prepare_sort_from_pathkeys (createplan.c): resjunk sort-column injection; \
                 M2 lane"
            );
        };
        let sortop = lsyscache::amop::get_opfamily_member_for_cmptype(
            pathkey.pk_opfamily,
            pk_datatype,
            pk_datatype,
            pathkey.pk_cmptype,
        )?;
        assert!(
            sortop != 0,
            "missing operator {}({},{}) in opfamily {}",
            pathkey.pk_cmptype,
            pk_datatype,
            pk_datatype,
            pathkey.pk_opfamily
        );
        sort_col_idx.push(resno);
        sort_operators.push(sortop);
        collations.push(run.root.ec(ec).ec_collation);
        nulls_first.push(pathkey.pk_nulls_first);
    }

    let mut plan = Node::build::<types_nodes::plannodes::Sort>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.disabled_nodes = lefttree.as_plan().unwrap().disabled_nodes
        + if crate::gucs::enable_sort() { 0 } else { 1 };
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(lefttree);
    plan.numCols = sort_col_idx.len() as i32;
    plan.sortColIdx = mcx::slice_borrow_in(mcx, &sort_col_idx)?;
    plan.sortOperators = mcx::slice_borrow_in(mcx, &sort_operators)?;
    plan.collations = mcx::slice_borrow_in(mcx, &collations)?;
    plan.nullsFirst = mcx::slice_borrow_in(mcx, &nulls_first)?;
    Ok(plan.seal())
}

// create_limit_plan + make_limit (createplan.c); WITH TIES is loud.
fn create_limit_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, limit_offset, limit_count, limit_option) = {
        let PathNode::LimitPath(lp) = run.root.path(path_id) else { unreachable!() };
        (
            lp.subpath.expect("LimitPath has a subpath"),
            lp.limitOffset.map(|id| *run.root.expr_node(id)),
            lp.limitCount.map(|id| *run.root.expr_node(id)),
            lp.limitOption,
        )
    };
    assert!(
        limit_option == types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT as u32,
        "create_limit_plan (createplan.c): WITH TIES uniq keys; M2 ties lane"
    );
    // Limit doesn't project, so tlist requirements pass through.
    let subplan = create_plan_recurse(run, subpath_id, flags)?;

    let mut plan = Node::build::<types_nodes::plannodes::Limit>(mcx)?;
    plan.plan.targetlist =
        NodeList::from_slice(mcx, subplan.as_plan().expect("plan node").targetlist.as_slice())?;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.limitOffset = limit_offset;
    plan.limitCount = limit_count;
    plan.limitOption = types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_join_plan (createplan.c), T_NestLoop arm -> create_nestloop_plan +
// make_nestloop. Gating (pseudoconstant) clauses are loud upstream; the
// reparameterize/nestParams legs are dead while param_info is always None.
fn create_join_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (outer_path, inner_path, jointype, inner_unique, restrict, target_id) =
        match run.root.path(path_id) {
            PathNode::NestPath(np) => {
                debug_assert!(np.jpath.path.param_info.is_none());
                (
                    np.jpath.outerjoinpath.expect("nestloop outer path"),
                    np.jpath.innerjoinpath.expect("nestloop inner path"),
                    np.jpath.jointype,
                    np.jpath.inner_unique,
                    crate::relnode::pgvec_clone_shallow(mcx, &np.jpath.joinrestrictinfo),
                    np.jpath.path.pathtarget_id.unwrap(),
                )
            }
            other => panic!(
                "create_join_plan (createplan.c): pathtype {}; M2 merge/hash lane",
                other.base().pathtype
            ),
        };
    assert!(
        jointype == types_pathnodes::JOIN_INNER,
        "create_nestloop_plan (createplan.c): jointype {jointype}; M2 outer-join lane"
    );
    debug_assert!(restrict.iter().all(|&r| !run.root.rinfo(r).pseudoconstant));

    let tlist = build_path_tlist(run, target_id)?;
    // NestLoop can project, so no need to be picky about child tlists.
    let outer_plan = create_plan_recurse(run, outer_path, 0)?;
    debug_assert!(run.root.curOuterRels.is_none() && run.root.curOuterParams.is_empty());
    let inner_plan = create_plan_recurse(run, inner_path, 0)?;

    let ordered = order_qual_clauses(run, &restrict)?;
    // Inner join: all clauses alike become joinquals, otherclauses stay NIL.
    let joinclauses = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::NestLoop>(mcx)?;
    plan.join.plan.targetlist = tlist;
    plan.join.plan.qual = NodeList::nil();
    plan.join.plan.lefttree = Some(outer_plan);
    plan.join.plan.righttree = Some(inner_plan);
    plan.join.jointype = types_nodes::JoinType::JOIN_INNER;
    plan.join.inner_unique = inner_unique;
    plan.join.joinqual = joinclauses;
    plan.nestParams = NodeList::nil();
    copy_generic_path_info(run, &mut plan.join.plan, path_id);
    Ok(plan.seal())
}
