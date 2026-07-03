use types_error::PgResult;
use types_nodes::list::{NodeList, OidList};
use types_nodes::plannodes::{
    Agg, Hash, HashJoin, IndexScan, Plan, Result as ResultPlan, SeqScan, WindowAgg,
};
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
    crate::subselect::ss_attach_initplans(run, plan)?;
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
        PathNode::Path(p)
            if p.pathtype == crate::pathnode::tag16(NodeTag::T_SeqScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_FunctionScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_ValuesScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_CteScan) =>
        {
            create_scan_plan(run, path_id, flags)
        }
        PathNode::IndexPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::BitmapHeapPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::ProjectionPath(_) => create_projection_plan(run, path_id, flags),
        PathNode::GroupResultPath(_) => create_group_result_plan(run, path_id),
        PathNode::AggPath(_) => create_agg_plan(run, path_id),
        PathNode::WindowAggPath(_) => create_windowagg_plan(run, path_id),
        PathNode::UpperUniquePath(_) => create_upper_unique_plan(run, path_id, flags),
        PathNode::SortPath(_) => create_sort_plan(run, path_id, flags),
        PathNode::IncrementalSortPath(_) => create_incremental_sort_plan(run, path_id, flags),
        PathNode::MaterialPath(_) => create_material_plan(run, path_id, flags),
        PathNode::NestPath(_) => create_join_plan(run, path_id),
        PathNode::MergePath(_) => create_mergejoin_plan(run, path_id),
        PathNode::HashPath(_) => create_hashjoin_plan(run, path_id),
        PathNode::LimitPath(_) => create_limit_plan(run, path_id, flags),
        PathNode::UniquePath(_) => panic!(
            "create_unique_plan (createplan.c): unique-ified semijoin won the cost \
             competition; unique-plan lane unported"
        ),
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
        let PathNode::IndexPath(ip) = run.root.path(best_path) else { unreachable!() };
        let info = ip.indexinfo.as_ref().expect("indexinfo set");
        for i in 0..info.ncolumns as usize {
            if !info.canreturn[i] {
                return false;
            }
        }
    }
    let base = run.root.path(best_path).base();
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

    let gating_clauses = get_gating_quals(run, &scan_clauses)?;
    // A gating Result can project, so the scan needn't honor tlist flags.
    let flags = if gating_clauses.is_nil() { flags } else { 0 };

    let tlist = if flags == CP_IGNORE_TLIST {
        NodeList::nil()
    } else if use_physical_tlist(run, best_path, flags) {
        let physical = if pathtype == crate::pathnode::tag16(NodeTag::T_IndexOnlyScan) {
            // copyObject(indexinfo->indextlist): fresh TLE nodes so the
            // plan tlist stays independent of the plan's own indextlist.
            ios_indextlist_copy(run, best_path, false)?
        } else {
            build_physical_tlist(run, rel_id)?
        };
        if flags & CP_LABEL_TLIST != 0 {
            // apply_pathtarget_labeling_to_tlist: no sortgrouprefs to copy
            // (use_physical_tlist refused any labeled target).
        }
        physical
    } else {
        let target_id = run.root.path(best_path).base().pathtarget_id.unwrap();
        build_path_tlist(run, target_id)?
    };

    let plan = match pathtype {
        t if t == crate::pathnode::tag16(NodeTag::T_SeqScan) => {
            create_seqscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_IndexScan) => {
            create_indexscan_plan(run, best_path, tlist, scan_clauses, false)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_IndexOnlyScan) => {
            create_indexscan_plan(run, best_path, tlist, scan_clauses, true)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_BitmapHeapScan) => {
            create_bitmap_scan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_FunctionScan) => {
            create_functionscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_ValuesScan) => {
            create_valuesscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_CteScan) => {
            create_ctescan_plan(run, best_path, tlist, scan_clauses)?
        }
        other => panic!("create_scan_plan (createplan.c): pathtype {other}; M2 scan lane"),
    };

    if !gating_clauses.is_nil() {
        return create_gating_plan(run, best_path, plan, gating_clauses);
    }
    Ok(plan)
}

// get_gating_quals (createplan.c).
fn get_gating_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    quals: &[RinfoId],
) -> PgResult<NodeList<'mcx>> {
    if !run.root.hasPseudoConstantQuals {
        return Ok(NodeList::nil());
    }
    let ordered = order_qual_clauses(run, quals)?;
    let mut out = NodeList::nil();
    for &rid in ordered.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            out.lappend(run.mcx, *run.root.expr_node(run.root.rinfo(rid).clause))?;
        }
    }
    Ok(out)
}

// create_gating_plan (createplan.c): a Result node evaluating the
// pseudoconstant quals as one-time quals atop the scan plan.
fn create_gating_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    plan: Node<'mcx>,
    gating_quals: NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    debug_assert!(!gating_quals.is_nil());
    let splan = match plan.as_result() {
        Some(r) if r.plan.lefttree.is_none() && r.resconstantqual.is_none() => None,
        _ => Some(plan),
    };
    let target_id = run.root.path(path_id).base().pathtarget_id.unwrap();
    let tlist = build_path_tlist(run, target_id)?;

    let mut gplan = Node::build::<ResultPlan<'mcx>>(mcx)?;
    gplan.plan.targetlist = tlist;
    gplan.resconstantqual = Some(Node::mk_list(mcx, gating_quals)?);
    gplan.plan.lefttree = splan;
    // copy_plan_costsize: gating changes no cost or size estimates.
    let child = plan.as_plan().expect("plan node");
    gplan.plan.disabled_nodes = child.disabled_nodes;
    gplan.plan.startup_cost = child.startup_cost;
    gplan.plan.total_cost = child.total_cost;
    gplan.plan.plan_rows = child.plan_rows;
    gplan.plan.plan_width = child.plan_width;
    gplan.plan.parallel_aware = false;
    gplan.plan.parallel_safe = run.root.path(path_id).base().parallel_safe;
    Ok(gplan.seal())
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

// extract_actual_join_clauses (restrictinfo.c): joinquals vs pushed-down
// otherquals for outer joins; pseudoconstants are gating quals.
fn extract_actual_join_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    rinfos: &[RinfoId],
    joinrelids: &types_pathnodes::Relids<'mcx>,
) -> (NodeList<'mcx>, NodeList<'mcx>) {
    let mut joinquals = NodeList::nil();
    let mut otherquals = NodeList::nil();
    for &rid in rinfos {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if crate::joinrels::rinfo_is_pushed_down(run, rid, joinrelids) {
            otherquals.lappend(run.mcx, clause).expect("lappend");
        } else {
            joinquals.lappend(run.mcx, clause).expect("lappend");
        }
    }
    (joinquals, otherquals)
}

fn jointype_enum(jointype: u32) -> types_nodes::JoinType {
    match jointype {
        types_pathnodes::JOIN_INNER => types_nodes::JoinType::JOIN_INNER,
        types_pathnodes::JOIN_LEFT => types_nodes::JoinType::JOIN_LEFT,
        types_pathnodes::JOIN_RIGHT => types_nodes::JoinType::JOIN_RIGHT,
        types_pathnodes::JOIN_SEMI => types_nodes::JoinType::JOIN_SEMI,
        types_pathnodes::JOIN_ANTI => types_nodes::JoinType::JOIN_ANTI,
        types_pathnodes::JOIN_RIGHT_SEMI => types_nodes::JoinType::JOIN_RIGHT_SEMI,
        types_pathnodes::JOIN_RIGHT_ANTI => types_nodes::JoinType::JOIN_RIGHT_ANTI,
        other => panic!("create_join_plan (createplan.c): jointype {other} unported"),
    }
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

// create_functionscan_plan (createplan.c); replace_nestloop_params is dead
// (param_info asserted None upstream).
fn create_functionscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    let rte = run.rte(scan_relid as usize);
    debug_assert!(rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_FUNCTION);
    // C shares the list pointer; the header is re-copied cell-by-cell (the
    // RangeTblFunction nodes stay shared).
    let mut functions = NodeList::nil();
    for f in &rte.functions {
        functions.lappend(mcx, f)?;
    }
    let funcordinality = rte.funcordinality;

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::FunctionScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.functions = functions;
    plan.funcordinality = funcordinality;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_valuesscan_plan (createplan.c); replace_nestloop_params is dead
// (param_info asserted None upstream).
fn create_valuesscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    let rte = run.rte(scan_relid as usize);
    debug_assert!(rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_VALUES);
    // C shares the list pointer; the header is re-copied cell-by-cell (the
    // per-row expression lists stay shared).
    let mut values_lists = NodeList::nil();
    for row in &rte.values_lists {
        values_lists.lappend(mcx, row)?;
    }

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::ValuesScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.values_lists = values_lists;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_ctescan_plan (createplan.c).
fn create_ctescan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    let rte = run.rte(scan_relid as usize);
    debug_assert!(rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE);
    let (plan_id, cte_param_id) = crate::cte::cte_plan_id_and_param(run, rte);

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::CteScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.ctePlanId = plan_id;
    plan.cteParam = cte_param_id;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_indexscan_plan (createplan.c), plain-IndexScan arm.
fn create_indexscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
    indexonly: bool,
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

    if indexonly {
        let indextlist = ios_indextlist_copy(run, best_path, true)?;
        let mut plan = Node::build::<types_nodes::plannodes::IndexOnlyScan<'mcx>>(mcx)?;
        plan.scan.plan.targetlist = tlist;
        plan.scan.plan.qual = qpqual;
        plan.scan.scanrelid = scan_relid;
        plan.indexid = indexoid;
        plan.indexqual = fixed_indexquals;
        plan.recheckqual = stripped_indexquals;
        plan.indexorderby = NodeList::nil();
        plan.indextlist = indextlist;
        plan.indexorderdir = indexscandir;
        copy_generic_path_info(run, &mut plan.scan.plan, best_path);
        return Ok(plan.seal());
    }

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

// Fresh TLE copies of indexinfo->indextlist. mark_returnable = the C
// scribble `indextle->resjunk = !indexinfo->canreturn[i]` applied to the
// copy that becomes the plan's indextlist (setrefs drops resjunk entries).
fn ios_indextlist_copy<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    mark_returnable: bool,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let index = {
        let PathNode::IndexPath(p) = run.root.path(best_path) else { unreachable!() };
        *p.indexinfo.as_ref().expect("indexinfo set")
    };
    let mut tlist = NodeList::nil();
    for (i, &tle_id) in index.indextlist.iter().enumerate() {
        let tle = run
            .root
            .expr_node(tle_id)
            .as_target_entry()
            .expect("indextlist holds TargetEntries");
        let resjunk = if mark_returnable { !index.canreturn[i] } else { tle.resjunk };
        let new_tle = Node::mk_target_entry(mcx, tle.expr, tle.resno, tle.resname, resjunk)?;
        tlist.lappend(mcx, new_tle)?;
    }
    Ok(tlist)
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
            p.indexinfo.expect("indexinfo set"),
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

// use_physical_tlist is false on every reachable input: the parent rel is
// never a physical scan rel here (C ignores flags in this function too).
fn create_projection_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(flags & (CP_EXACT_TLIST | CP_SMALL_TLIST | CP_LABEL_TLIST) != 0);
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
// INSERT/UPDATE/DELETE arm: no FDW result rels, no ON CONFLICT/MERGE lists.
fn create_modifytable_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, operation, can_set_tag, nominal, root_rel, result_relations, epq_param) = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        debug_assert!(
            p.withCheckOptionLists.is_empty()
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

    let returning_lists = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        debug_assert!(p.returningLists.len() <= 1);
        let mut ids: mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::NodeId>> =
            mcx::PgVec::new_in(mcx);
        for rlist in p.returningLists.iter() {
            ids.push(crate::relnode::pgvec_clone_shallow(mcx, rlist));
        }
        let mut lists = types_nodes::list::NodeList::nil();
        for rlist in ids.iter() {
            let mut nl = types_nodes::list::NodeList::nil();
            for &id in rlist.iter() {
                nl.lappend(mcx, *run.root.expr_node(id))?;
            }
            lists.lappend(mcx, Node::mk_list(mcx, nl)?)?;
        }
        lists
    };

    let mut plan = Node::build::<types_nodes::plannodes::ModifyTable>(mcx)?;
    plan.plan.lefttree = Some(subplan);
    plan.operation = operation;
    plan.updateColnosLists = update_colnos_lists;
    plan.returningLists = returning_lists;
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
    let (target_id, quals, costs) = match run.root.path(path_id) {
        PathNode::GroupResultPath(grp) => (
            grp.path.pathtarget_id.unwrap(),
            crate::relnode::pgvec_clone_shallow(run.mcx, &grp.quals),
            (
                grp.path.startup_cost,
                grp.path.total_cost,
                grp.path.rows,
                grp.path.parallel_safe,
            ),
        ),
        _ => unreachable!(),
    };
    let tlist = build_path_tlist(run, target_id)?;
    let width = run.root.pathtarget(target_id).width;

    // order_qual_clauses over bare clauses: stable sort by per-tuple cost
    // (security_level is 0 for bare quals).
    let mut items: mcx::PgVec<'_, (types_pathnodes::NodeId, f64)> = mcx::PgVec::new_in(run.mcx);
    items.reserve(quals.len());
    for &id in quals.iter() {
        let cost = crate::costsize::cost_qual_eval_node(*run.root.expr_node(id))?;
        items.push((id, cost.per_tuple));
    }
    if items.len() > 1 {
        items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    }
    let mut qual_list = NodeList::nil();
    for &(id, _) in items.iter() {
        qual_list.lappend(run.mcx, *run.root.expr_node(id))?;
    }

    // make_result + copy_generic_path_info.
    let mut plan = Node::build::<ResultPlan>(run.mcx)?;
    plan.plan.targetlist = tlist;
    if !qual_list.is_nil() {
        plan.resconstantqual = Some(Node::mk_list(run.mcx, qual_list)?);
    }
    plan.plan.startup_cost = costs.0;
    plan.plan.total_cost = costs.1;
    plan.plan.plan_rows = costs.2;
    plan.plan.plan_width = width;
    plan.plan.parallel_safe = costs.3;
    Ok(plan.seal())
}

// create_agg_plan + make_agg (createplan.c).
fn create_agg_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id, aggstrategy, aggsplit, num_groups, transition_space, qual_ids) =
        match run.root.path(path_id) {
            PathNode::AggPath(ap) => (
                ap.subpath.expect("AggPath has a subpath"),
                ap.path.pathtarget_id.unwrap(),
                ap.aggstrategy,
                ap.aggsplit,
                ap.numGroups,
                ap.transitionSpace,
                crate::relnode::pgvec_clone_shallow(run.mcx, &ap.qual),
            ),
            _ => unreachable!(),
        };
    let qual = order_bare_qual_clauses(run, &qual_ids)?;

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
    plan.plan.qual = qual;
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


// create_windowagg_plan (createplan.c); runCondition/qual/frame-offset legs
// dead (loud upstream), startOffset/endOffset always None (default frame).
fn create_windowagg_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id, winclause_id, topwindow) = match run.root.path(path_id) {
        PathNode::WindowAggPath(wp) => {
            debug_assert!(wp.qual.is_empty() && wp.runCondition.is_empty());
            (
                wp.subpath.expect("WindowAggPath has a subpath"),
                wp.path.pathtarget_id.unwrap(),
                wp.winclause,
                wp.topwindow,
            )
        }
        _ => unreachable!(),
    };
    let wc_node = *run.root.expr_node(winclause_id);

    // WindowAgg spools its input into a tuplestore: request a small tlist,
    // with grouping columns labeled.
    let subplan = create_plan_recurse(run, subpath_id, CP_LABEL_TLIST | CP_SMALL_TLIST)?;
    let tlist = build_path_tlist(run, target_id)?;

    let wc = wc_node.as_window_clause().expect("WindowClause");
    assert!(
        wc.startOffset.is_none() && wc.endOffset.is_none(),
        "create_windowagg_plan (createplan.c): frame offsets unported"
    );
    let subplan_tlist = &subplan.as_plan().expect("plan node").targetlist;
    let mut cols = |clause: &NodeList<'mcx>| -> PgResult<(
        &'mcx [i16],
        &'mcx [types_core::Oid],
        &'mcx [types_core::Oid],
    )> {
        let mut idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(run.mcx);
        let mut ops: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(run.mcx);
        let mut colls: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(run.mcx);
        for sgc_node in clause {
            let sgc = sgc_node.as_sort_group_clause().expect("SortGroupClause");
            debug_assert!(sgc.eqop != 0);
            let tle_node = subplan_tlist
                .iter()
                .find(|n| {
                    n.as_target_entry().expect("tlist cell").ressortgroupref
                        == sgc.tleSortGroupRef
                })
                .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
            let tle = tle_node.as_target_entry().unwrap();
            idx.push(tle.resno);
            ops.push(sgc.eqop);
            colls.push(expr_collation(tle.expr));
        }
        Ok((
            mcx::vec_borrow_in(run.mcx, idx)?,
            mcx::vec_borrow_in(run.mcx, ops)?,
            mcx::vec_borrow_in(run.mcx, colls)?,
        ))
    };
    let (part_idx, part_ops, part_colls) = cols(&wc.partitionClause)?;
    let (ord_idx, ord_ops, ord_colls) = cols(&wc.orderClause)?;

    let mut plan = Node::build::<WindowAgg>(run.mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.lefttree = Some(subplan);
    plan.winname = wc.name;
    plan.winref = wc.winref;
    plan.partNumCols = part_idx.len() as i32;
    plan.partColIdx = part_idx;
    plan.partOperators = part_ops;
    plan.partCollations = part_colls;
    plan.ordNumCols = ord_idx.len() as i32;
    plan.ordColIdx = ord_idx;
    plan.ordOperators = ord_ops;
    plan.ordCollations = ord_colls;
    plan.frameOptions = wc.frameOptions;
    plan.startOffset = None;
    plan.endOffset = None;
    plan.startInRangeFunc = wc.startInRangeFunc;
    plan.endInRangeFunc = wc.endInRangeFunc;
    plan.inRangeColl = wc.inRangeColl;
    plan.inRangeAsc = wc.inRangeAsc;
    plan.inRangeNullsFirst = wc.inRangeNullsFirst;
    plan.topWindow = topwindow;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// order_qual_clauses (createplan.c) over bare expressions (AggPath.qual
// carries no RestrictInfos, as C).
fn order_bare_qual_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    quals: &[types_pathnodes::NodeId],
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut items: mcx::PgVec<'_, (Node<'mcx>, f64)> = mcx::PgVec::new_in(mcx);
    for &q in quals {
        let clause = *run.root.expr_node(q);
        let cost = crate::costsize::cost_qual_eval_node(clause)?;
        items.push((clause, cost.per_tuple));
    }
    if items.len() > 1 {
        items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    }
    let mut out = NodeList::nil();
    for (clause, _) in items.iter() {
        out.lappend(mcx, *clause)?;
    }
    Ok(out)
}

// create_upper_unique_plan + make_unique_from_pathkeys (createplan.c).
fn create_upper_unique_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, pathkeys, numkeys) = {
        let PathNode::UpperUniquePath(up) = run.root.path(path_id) else { unreachable!() };
        (
            up.subpath.expect("UpperUniquePath has a subpath"),
            crate::relnode::pgvec_clone_shallow(mcx, &up.path.pathkeys),
            up.numkeys,
        )
    };
    // Unique doesn't project; grouping columns must be labeled.
    let subplan = create_plan_recurse(run, subpath_id, flags | CP_LABEL_TLIST)?;

    let tlist = NodeList::from_slice(
        mcx,
        subplan.as_plan().expect("plan node").targetlist.as_slice(),
    )?;
    let mut uniq_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut uniq_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut uniq_collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    for pathkey in pathkeys.iter() {
        if uniq_col_idx.len() >= numkeys as usize {
            break;
        }
        let ec = pathkey.pk_eclass.expect("canonical pathkey has an eclass");
        assert!(
            !run.root.ec(ec).ec_has_volatile,
            "make_unique_from_pathkeys (createplan.c): volatile sortref leg; M2 lane"
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
            panic!("could not find pathkey item to sort");
        };
        let eqop = lsyscache::amop::get_opfamily_member_for_cmptype(
            pathkey.pk_opfamily,
            pk_datatype,
            pk_datatype,
            types_pathnodes::COMPARE_EQ,
        )?;
        assert!(
            eqop != 0,
            "missing operator {}({},{}) in opfamily {}",
            types_pathnodes::COMPARE_EQ,
            pk_datatype,
            pk_datatype,
            pathkey.pk_opfamily
        );
        uniq_col_idx.push(resno);
        uniq_operators.push(eqop);
        uniq_collations.push(run.root.ec(ec).ec_collation);
    }
    assert_eq!(uniq_col_idx.len(), numkeys as usize);

    let mut plan = Node::build::<types_nodes::plannodes::Unique>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.numCols = numkeys;
    plan.uniqColIdx = mcx::slice_borrow_in(mcx, &uniq_col_idx)?;
    plan.uniqOperators = mcx::slice_borrow_in(mcx, &uniq_operators)?;
    plan.uniqCollations = mcx::slice_borrow_in(mcx, &uniq_collations)?;
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
    copy_generic_path_info_node(run, plan, path_id);
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

struct SortColumns<'mcx> {
    tlist: NodeList<'mcx>,
    sort_col_idx: mcx::PgVec<'mcx, i16>,
    sort_operators: mcx::PgVec<'mcx, u32>,
    collations: mcx::PgVec<'mcx, u32>,
    nulls_first: mcx::PgVec<'mcx, bool>,
}

// prepare_sort_from_pathkeys (createplan.c): every pathkey must match an
// existing tlist column (the resjunk-entry-injection leg is loud).
fn prepare_sort_from_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    lefttree: Node<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
) -> PgResult<SortColumns<'mcx>> {
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
    Ok(SortColumns { tlist, sort_col_idx, sort_operators, collations, nulls_first })
}

fn fill_sort_fields<'mcx>(
    run: &PlannerRun<'mcx>,
    plan: &mut types_nodes::plannodes::Sort<'mcx>,
    lefttree: Node<'mcx>,
    cols: SortColumns<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    plan.plan.targetlist = cols.tlist;
    plan.plan.disabled_nodes = lefttree.as_plan().unwrap().disabled_nodes
        + if crate::gucs::enable_sort() { 0 } else { 1 };
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(lefttree);
    plan.numCols = cols.sort_col_idx.len() as i32;
    plan.sortColIdx = mcx::slice_borrow_in(mcx, &cols.sort_col_idx)?;
    plan.sortOperators = mcx::slice_borrow_in(mcx, &cols.sort_operators)?;
    plan.collations = mcx::slice_borrow_in(mcx, &cols.collations)?;
    plan.nullsFirst = mcx::slice_borrow_in(mcx, &cols.nulls_first)?;
    Ok(())
}

fn make_sort_from_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    lefttree: Node<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
) -> PgResult<Node<'mcx>> {
    let cols = prepare_sort_from_pathkeys(run, lefttree, pathkeys)?;
    let mut plan = Node::build::<types_nodes::plannodes::Sort>(run.mcx)?;
    fill_sort_fields(run, &mut plan, lefttree, cols)?;
    Ok(plan.seal())
}

// make_incrementalsort_from_pathkeys (createplan.c); C's make_incrementalsort
// leaves disabled_nodes at makeNode's zero (no enable_sort penalty), so the
// fill_sort_fields value is zeroed back out.
fn make_incrementalsort_from_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    lefttree: Node<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
    n_presorted_cols: i32,
) -> PgResult<Node<'mcx>> {
    let cols = prepare_sort_from_pathkeys(run, lefttree, pathkeys)?;
    let mut plan = Node::build::<types_nodes::plannodes::IncrementalSort>(run.mcx)?;
    fill_sort_fields(run, &mut plan.sort, lefttree, cols)?;
    plan.sort.plan.disabled_nodes = 0;
    plan.nPresortedCols = n_presorted_cols;
    Ok(plan.seal())
}

// create_incrementalsort_plan (createplan.c).
fn create_incremental_sort_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath_id, pathkeys, n_presorted) = {
        let PathNode::IncrementalSortPath(sp) = run.root.path(path_id) else { unreachable!() };
        (
            sp.spath.subpath.expect("IncrementalSortPath has a subpath"),
            crate::relnode::pgvec_clone_shallow(run.mcx, &sp.spath.path.pathkeys),
            sp.nPresortedCols,
        )
    };
    let subplan = create_plan_recurse(run, subpath_id, flags | CP_SMALL_TLIST)?;
    let plan = make_incrementalsort_from_pathkeys(run, subplan, &pathkeys, n_presorted)?;
    copy_generic_path_info_node(run, plan, path_id);
    Ok(plan)
}

fn copy_generic_path_info_node<'mcx>(run: &PlannerRun<'mcx>, plan: Node<'mcx>, path_id: PathId) {
    // SAFETY: plan was freshly built by the caller; no other handle exists yet.
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
    .expect("plan node embeds a Plan base");
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
// create_material_plan + make_material (createplan.c): the tlist shares the
// child's (Material never projects).
fn create_material_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let subpath = match run.root.path(path_id) {
        PathNode::MaterialPath(mp) => mp.subpath.expect("Material subpath"),
        other => panic!("create_material_plan (createplan.c): pathtype {}", other.base().pathtype),
    };
    let subplan = create_plan_recurse(run, subpath, flags | CP_SMALL_TLIST)?;
    let mut tlist = NodeList::nil();
    for te in subplan.as_plan().expect("subplan").targetlist.iter() {
        tlist.lappend(mcx, te)?;
    }
    let mut plan = Node::build::<types_nodes::plannodes::Material>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.plan.righttree = None;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

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
    debug_assert!(restrict.iter().all(|&r| !run.root.rinfo(r).pseudoconstant));

    let tlist = build_path_tlist(run, target_id)?;
    // NestLoop can project, so no need to be picky about child tlists.
    let outer_plan = create_plan_recurse(run, outer_path, 0)?;
    debug_assert!(run.root.curOuterRels.is_none() && run.root.curOuterParams.is_empty());
    let inner_plan = create_plan_recurse(run, inner_path, 0)?;

    let ordered = order_qual_clauses(run, &restrict)?;
    let (joinclauses, otherclauses) = if crate::joinpath::is_outer_join(jointype) {
        let joinrelids = crate::relnode::relids_copy(
            mcx,
            &run.root.rel(run.root.path(path_id).base().parent).relids,
        );
        extract_actual_join_clauses(run, &ordered, &joinrelids)
    } else {
        (extract_actual_clauses(run, &ordered), NodeList::nil())
    };

    let mut plan = Node::build::<types_nodes::plannodes::NestLoop>(mcx)?;
    plan.join.plan.targetlist = tlist;
    plan.join.plan.qual = otherclauses;
    plan.join.plan.lefttree = Some(outer_plan);
    plan.join.plan.righttree = Some(inner_plan);
    plan.join.jointype = jointype_enum(jointype);
    plan.join.inner_unique = inner_unique;
    plan.join.joinqual = joinclauses;
    plan.nestParams = NodeList::nil();
    copy_generic_path_info(run, &mut plan.join.plan, path_id);
    Ok(plan.seal())
}

// create_hashjoin_plan (createplan.c), JOIN_INNER arm. Skew fields default to
// invalid (the executor's skew fast path is loud, so they are never consumed);
// otherclauses is NIL for inner joins.
fn create_hashjoin_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (outer_path, inner_path, jointype, inner_unique, restrict, hash_rinfos, target_id, num_batches) =
        match run.root.path(path_id) {
            PathNode::HashPath(hp) => {
                debug_assert!(hp.jpath.path.param_info.is_none());
                (
                    hp.jpath.outerjoinpath.expect("hashjoin outer path"),
                    hp.jpath.innerjoinpath.expect("hashjoin inner path"),
                    hp.jpath.jointype,
                    hp.jpath.inner_unique,
                    crate::relnode::pgvec_clone_shallow(mcx, &hp.jpath.joinrestrictinfo),
                    crate::relnode::pgvec_clone_shallow(mcx, &hp.path_hashclauses),
                    hp.jpath.path.pathtarget_id.unwrap(),
                    hp.num_batches,
                )
            }
            other => panic!(
                "create_hashjoin_plan (createplan.c): pathtype {}",
                other.base().pathtype
            ),
        };
    let tlist = build_path_tlist(run, target_id)?;
    let outer_flags = if num_batches > 1 { CP_SMALL_TLIST } else { 0 };
    let outer_plan = create_plan_recurse(run, outer_path, outer_flags)?;
    let inner_plan = create_plan_recurse(run, inner_path, CP_SMALL_TLIST)?;

    let ordered = order_qual_clauses(run, &restrict)?;
    let (joinclauses, otherclauses) = if crate::joinpath::is_outer_join(jointype) {
        let joinrelids = crate::relnode::relids_copy(
            mcx,
            &run.root.rel(run.root.path(path_id).base().parent).relids,
        );
        extract_actual_join_clauses(run, &ordered, &joinrelids)
    } else {
        (extract_actual_clauses(run, &ordered), NodeList::nil())
    };
    // hashclauses (plain OpExpr form) removed from joinclauses (no double eval).
    let hashclauses_actual = get_actual_clauses(run, &hash_rinfos);
    let joinclauses = list_difference(mcx, &joinclauses, &hashclauses_actual);

    // Rearrange so the outer variable is on the left, per outer rel relids.
    let outer_relids =
        crate::relnode::relids_copy(mcx, &run.root.rel(run.root.path(outer_path).base().parent).relids);
    let switched = get_switched_clauses(run, &hash_rinfos, &outer_relids)?;

    let mut hashoperators: OidList<'mcx> = OidList::nil();
    let mut hashcollations: OidList<'mcx> = OidList::nil();
    let mut outer_hashkeys = NodeList::nil();
    let mut inner_hashkeys = NodeList::nil();
    for clause_node in switched.iter() {
        let op = clause_node.as_op_expr().expect("switched hashclause is an OpExpr");
        hashoperators.lappend(mcx, op.opno)?;
        hashcollations.lappend(mcx, op.inputcollid)?;
        outer_hashkeys.lappend(mcx, op.args.nth(0))?;
        inner_hashkeys.lappend(mcx, op.args.nth(1))?;
    }

    // make_hash: tlist shares the inner plan's, hashkeys are the inner keys.
    let mut hash_plan = Node::build::<Hash>(mcx)?;
    let mut inner_tlist = NodeList::nil();
    for te in inner_plan.as_plan().expect("inner plan").targetlist.iter() {
        inner_tlist.lappend(mcx, te)?;
    }
    let (i_startup, i_total, i_rows, i_width) = {
        let p = inner_plan.as_plan().unwrap();
        (p.startup_cost, p.total_cost, p.plan_rows, p.plan_width)
    };
    hash_plan.plan.targetlist = inner_tlist;
    hash_plan.plan.qual = NodeList::nil();
    hash_plan.plan.lefttree = Some(inner_plan);
    hash_plan.plan.righttree = None;
    hash_plan.hashkeys = inner_hashkeys;
    // copy_plan_costsize + Hash startup == total (EXPLAIN-only).
    hash_plan.plan.plan_rows = i_rows;
    hash_plan.plan.plan_width = i_width;
    hash_plan.plan.total_cost = i_total;
    hash_plan.plan.startup_cost = i_total;
    let _ = i_startup;
    let hash_node = hash_plan.seal();

    // make_hashjoin.
    let mut join_plan = Node::build::<HashJoin>(mcx)?;
    join_plan.join.plan.targetlist = tlist;
    join_plan.join.plan.qual = otherclauses;
    join_plan.join.plan.lefttree = Some(outer_plan);
    join_plan.join.plan.righttree = Some(hash_node);
    join_plan.hashclauses = switched;
    join_plan.hashoperators = hashoperators;
    join_plan.hashcollations = hashcollations;
    join_plan.hashkeys = outer_hashkeys;
    join_plan.join.jointype = jointype_enum(jointype);
    join_plan.join.inner_unique = inner_unique;
    join_plan.join.joinqual = joinclauses;
    copy_generic_path_info(run, &mut join_plan.join.plan, path_id);
    Ok(join_plan.seal())
}

// get_actual_clauses (clauses.c): the clause of each non-pseudoconstant rinfo.
fn get_actual_clauses<'mcx>(run: &PlannerRun<'mcx>, rinfos: &[RinfoId]) -> NodeList<'mcx> {
    let mut out = NodeList::nil();
    for &rid in rinfos {
        debug_assert!(!run.root.rinfo(rid).pseudoconstant);
        out.lappend(run.mcx, *run.root.expr_node(run.root.rinfo(rid).clause))
            .expect("lappend");
    }
    out
}

// list_difference over shared arena nodes (Node pointer identity).
fn list_difference<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &NodeList<'mcx>,
    b: &NodeList<'mcx>,
) -> NodeList<'mcx> {
    let mut out = NodeList::nil();
    for n in a.iter() {
        if !b.iter().any(|m| Node::ptr_eq(n, m)) {
            out.lappend(mcx, n).expect("lappend");
        }
    }
    out
}

// get_switched_clauses (createplan.c): commute so the outer var is on the left,
// setting outer_is_left. CommuteOpExpr swaps args + opno->commutator.
fn get_switched_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    hash_rinfos: &[RinfoId],
    outer_relids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut out = NodeList::nil();
    for &rid in hash_rinfos {
        let right_relids = run.root.rinfo(rid).right_relids.clone();
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let op = clause.as_op_expr().expect("hashclause is an OpExpr");
        if crate::relnode::relids_is_subset(&right_relids, outer_relids) {
            let commutator = lsyscache::get_commutator(op.opno)?;
            assert!(commutator != 0, "get_switched_clauses: no commutator for {}", op.opno);
            let mut temp = Node::build::<OpExpr>(mcx)?;
            temp.opno = commutator;
            temp.opfuncid = 0;
            temp.opresulttype = op.opresulttype;
            temp.opretset = op.opretset;
            temp.opcollid = op.opcollid;
            temp.inputcollid = op.inputcollid;
            temp.args = NodeList::make2(mcx, op.args.nth(1), op.args.nth(0))?;
            temp.location = op.location;
            out.lappend(mcx, temp.seal())?;
            run.root.rinfo_mut(rid).outer_is_left = false;
        } else {
            out.lappend(mcx, clause)?;
            run.root.rinfo_mut(rid).outer_is_left = true;
        }
    }
    Ok(out)
}

// label_sort_with_costsize (createplan.c): explicit merge-input sorts get
// their own cost labels (the path cost already includes them).
fn label_sort_with_costsize<'mcx>(
    run: &PlannerRun<'mcx>,
    sort_plan: Node<'mcx>,
    limit_tuples: f64,
) {
    let _ = run;
    let lefttree = sort_plan
        .as_plan()
        .expect("Sort embeds a Plan base")
        .lefttree
        .expect("Sort has a child");
    let child = lefttree.as_plan().expect("plan node");
    let (disabled, rows, width, total, parallel_safe) = (
        sort_plan.as_plan().unwrap().disabled_nodes,
        child.plan_rows,
        child.plan_width,
        child.total_cost,
        child.parallel_safe,
    );
    let (_, startup_cost, total_cost) = crate::costsize::cost_sort_shape(
        disabled,
        total,
        rows,
        width,
        0.0,
        init_small::globals::work_mem(),
        limit_tuples,
    );
    // SAFETY: sort_plan was freshly built by make_sort_from_pathkeys; no other
    // handle to it exists yet.
    unsafe {
        sort_plan.with_plan_mut(|p| {
            p.startup_cost = startup_cost;
            p.total_cost = total_cost;
            p.plan_rows = rows;
            p.plan_width = width;
            p.parallel_aware = false;
            p.parallel_safe = parallel_safe;
        })
    }
    .expect("Sort embeds a Plan base");
}

// label_incrementalsort_with_costsize (createplan.c).
fn label_incrementalsort_with_costsize<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sort_plan: Node<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
    limit_tuples: f64,
) -> PgResult<()> {
    let isort = sort_plan.as_incremental_sort().expect("IncrementalSort plan");
    let lefttree = isort.sort.plan.lefttree.expect("IncrementalSort has a child");
    let child = lefttree.as_plan().expect("plan node");
    let (disabled, startup, total, rows, width, parallel_safe) = (
        isort.sort.plan.disabled_nodes,
        child.startup_cost,
        child.total_cost,
        child.plan_rows,
        child.plan_width,
        child.parallel_safe,
    );
    let (_, startup_cost, total_cost, _) = crate::costsize::cost_incremental_sort_shape(
        run,
        pathkeys,
        isort.nPresortedCols as usize,
        disabled,
        startup,
        total,
        rows,
        width,
        0.0,
        init_small::globals::work_mem(),
        limit_tuples,
    )?;
    // SAFETY: sort_plan was freshly built by make_incrementalsort_from_pathkeys;
    // no other handle to it exists yet.
    unsafe {
        sort_plan.with_plan_mut(|p| {
            p.startup_cost = startup_cost;
            p.total_cost = total_cost;
            p.plan_rows = rows;
            p.plan_width = width;
            p.parallel_aware = false;
            p.parallel_safe = parallel_safe;
        })
    }
    .expect("IncrementalSort embeds a Plan base");
    Ok(())
}

// create_mergejoin_plan + make_mergejoin (createplan.c), JOIN_INNER arm:
// otherclauses is NIL; the materialize_inner arm is loud (nodeMaterial
// unported); replace_nestloop_params is dead (param_info always None).
fn create_mergejoin_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (
        outer_path,
        inner_path,
        jointype,
        inner_unique,
        restrict,
        merge_rinfos,
        outersortkeys,
        innersortkeys,
        skip_mark_restore,
        materialize_inner,
        outer_presorted_keys,
        target_id,
    ) = match run.root.path(path_id) {
        PathNode::MergePath(mp) => {
            debug_assert!(mp.jpath.path.param_info.is_none());
            (
                mp.jpath.outerjoinpath.expect("mergejoin outer path"),
                mp.jpath.innerjoinpath.expect("mergejoin inner path"),
                mp.jpath.jointype,
                mp.jpath.inner_unique,
                crate::relnode::pgvec_clone_shallow(mcx, &mp.jpath.joinrestrictinfo),
                crate::relnode::pgvec_clone_shallow(mcx, &mp.path_mergeclauses),
                crate::relnode::pgvec_clone_shallow(mcx, &mp.outersortkeys),
                crate::relnode::pgvec_clone_shallow(mcx, &mp.innersortkeys),
                mp.skip_mark_restore,
                mp.materialize_inner,
                mp.outer_presorted_keys,
                mp.jpath.path.pathtarget_id.unwrap(),
            )
        }
        other => panic!(
            "create_mergejoin_plan (createplan.c): pathtype {}",
            other.base().pathtype
        ),
    };
    debug_assert!(restrict.iter().all(|&r| !run.root.rinfo(r).pseudoconstant));

    let tlist = build_path_tlist(run, target_id)?;
    let outer_flags = if outersortkeys.is_empty() { 0 } else { CP_SMALL_TLIST };
    let inner_flags = if innersortkeys.is_empty() { 0 } else { CP_SMALL_TLIST };
    let mut outer_plan = create_plan_recurse(run, outer_path, outer_flags)?;
    let mut inner_plan = create_plan_recurse(run, inner_path, inner_flags)?;

    let ordered = order_qual_clauses(run, &restrict)?;
    let (joinclauses, otherclauses) = if crate::joinpath::is_outer_join(jointype) {
        let joinrelids = crate::relnode::relids_copy(
            mcx,
            &run.root.rel(run.root.path(path_id).base().parent).relids,
        );
        extract_actual_join_clauses(run, &ordered, &joinrelids)
    } else {
        (extract_actual_clauses(run, &ordered), NodeList::nil())
    };
    // NB: mergeclauses keep RestrictInfo order (never reordered by cost).
    let merge_actual = get_actual_clauses(run, &merge_rinfos);
    let joinclauses = list_difference(mcx, &joinclauses, &merge_actual);

    let outer_relids = crate::relnode::relids_copy(
        mcx,
        &run.root.rel(run.root.path(outer_path).base().parent).relids,
    );
    let mergeclauses = get_switched_clauses(run, &merge_rinfos, &outer_relids)?;

    let outerpathkeys: mcx::PgVec<'mcx, types_pathnodes::PathKey>;
    if !outersortkeys.is_empty() {
        if crate::gucs::enable_incremental_sort() && outer_presorted_keys > 0 {
            let sort_plan = make_incrementalsort_from_pathkeys(
                run,
                outer_plan,
                &outersortkeys,
                outer_presorted_keys as i32,
            )?;
            label_incrementalsort_with_costsize(run, sort_plan, &outersortkeys, -1.0)?;
            outer_plan = sort_plan;
        } else {
            let sort_plan = make_sort_from_pathkeys(run, outer_plan, &outersortkeys)?;
            label_sort_with_costsize(run, sort_plan, -1.0);
            outer_plan = sort_plan;
        }
        outerpathkeys = outersortkeys;
    } else {
        outerpathkeys = crate::relnode::pgvec_clone_shallow(
            mcx,
            &run.root.path(outer_path).base().pathkeys,
        );
    }

    let innerpathkeys: mcx::PgVec<'mcx, types_pathnodes::PathKey>;
    if !innersortkeys.is_empty() {
        let sort_plan = make_sort_from_pathkeys(run, inner_plan, &innersortkeys)?;
        label_sort_with_costsize(run, sort_plan, -1.0);
        inner_plan = sort_plan;
        innerpathkeys = innersortkeys;
    } else {
        innerpathkeys = crate::relnode::pgvec_clone_shallow(
            mcx,
            &run.root.path(inner_path).base().pathkeys,
        );
    }

    assert!(
        !materialize_inner,
        "create_mergejoin_plan (createplan.c): materialize_inner; nodeMaterial.c unported"
    );

    let n_clauses = merge_rinfos.len();
    let mut merge_families: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut merge_collations: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut merge_reversals: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    let mut merge_nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);

    let mut opathkey: Option<types_pathnodes::PathKey> = None;
    let mut opeclass: Option<types_pathnodes::EcId> = None;
    let mut lop = 0usize;
    let mut lip = 0usize;
    for &rid in merge_rinfos.iter() {
        let (oeclass, ieclass) = {
            let ri = run.root.rinfo(rid);
            if ri.outer_is_left {
                (ri.left_ec, ri.right_ec)
            } else {
                (ri.right_ec, ri.left_ec)
            }
        };
        debug_assert!(oeclass.is_some() && ieclass.is_some());

        if oeclass != opeclass {
            assert!(lop < outerpathkeys.len(), "outer pathkeys do not match mergeclauses");
            let opk = outerpathkeys[lop];
            lop += 1;
            opathkey = Some(opk);
            opeclass = opk.pk_eclass;
            assert!(oeclass == opeclass, "outer pathkeys do not match mergeclauses");
        }

        let mut ipathkey: Option<types_pathnodes::PathKey> = None;
        let mut ipeclass: Option<types_pathnodes::EcId> = None;
        let mut first_inner_match = false;
        if lip < innerpathkeys.len() {
            let ipk = innerpathkeys[lip];
            if ieclass == ipk.pk_eclass {
                lip += 1;
                ipathkey = Some(ipk);
                ipeclass = ipk.pk_eclass;
                first_inner_match = true;
            }
        }
        if !first_inner_match {
            for &ipk in innerpathkeys[..lip].iter() {
                ipathkey = Some(ipk);
                ipeclass = ipk.pk_eclass;
                if ieclass == ipeclass {
                    break;
                }
            }
            assert!(ieclass == ipeclass, "inner pathkeys do not match mergeclauses");
        }

        let opk = opathkey.unwrap();
        let ipk = ipathkey.unwrap();
        assert!(
            opk.pk_opfamily == ipk.pk_opfamily
                && run.root.ec(opk.pk_eclass.unwrap()).ec_collation
                    == run.root.ec(ipk.pk_eclass.unwrap()).ec_collation,
            "left and right pathkeys do not match in mergejoin"
        );
        assert!(
            !first_inner_match
                || (opk.pk_cmptype == ipk.pk_cmptype
                    && opk.pk_nulls_first == ipk.pk_nulls_first),
            "left and right pathkeys do not match in mergejoin"
        );

        merge_families.push(opk.pk_opfamily);
        merge_collations.push(run.root.ec(opk.pk_eclass.unwrap()).ec_collation);
        merge_reversals.push(opk.pk_cmptype == types_pathnodes::COMPARE_GT);
        merge_nulls_first.push(opk.pk_nulls_first);
    }
    debug_assert_eq!(merge_families.len(), n_clauses);

    let mut join_plan = Node::build::<types_nodes::plannodes::MergeJoin>(mcx)?;
    join_plan.join.plan.targetlist = tlist;
    join_plan.join.plan.qual = otherclauses;
    join_plan.join.plan.lefttree = Some(outer_plan);
    join_plan.join.plan.righttree = Some(inner_plan);
    join_plan.skip_mark_restore = skip_mark_restore;
    join_plan.mergeclauses = mergeclauses;
    join_plan.mergeFamilies = mcx::vec_borrow_in(mcx, merge_families)?;
    join_plan.mergeCollations = mcx::vec_borrow_in(mcx, merge_collations)?;
    join_plan.mergeReversals = mcx::vec_borrow_in(mcx, merge_reversals)?;
    join_plan.mergeNullsFirst = mcx::vec_borrow_in(mcx, merge_nulls_first)?;
    join_plan.join.jointype = jointype_enum(jointype);
    join_plan.join.inner_unique = inner_unique;
    join_plan.join.joinqual = joinclauses;
    copy_generic_path_info(run, &mut join_plan.join.plan, path_id);
    Ok(join_plan.seal())
}
