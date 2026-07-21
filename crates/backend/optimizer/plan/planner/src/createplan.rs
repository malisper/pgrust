use types_error::PgResult;
use types_nodes::list::{NodeList, OidList};
use types_nodes::plannodes::{
    Agg, Append, Group, Hash, HashJoin, IndexScan, Plan, Result as ResultPlan, SampleScan,
    SeqScan, SetOp, SubqueryScan, WindowAgg,
};
use types_nodes::primnodes::{OpExpr, TargetEntry};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{IndexOptInfo, PathId, PathNode, PtId, RelId, RinfoId};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

use crate::pathnode::is_projection_capable_pathtype;
use crate::run::PlannerRun;

pub const CP_EXACT_TLIST: i32 = 0x0001;
pub const CP_SMALL_TLIST: i32 = 0x0002;
pub const CP_LABEL_TLIST: i32 = 0x0004;
pub const CP_IGNORE_TLIST: i32 = 0x0008;

const INDEX_VAR: i32 = -3;

pub fn create_plan<'mcx>(run: &mut PlannerRun<'mcx>, best_path: PathId) -> PgResult<Node<'mcx>> {
    debug_assert!(run.root.plan_params.is_empty());
    run.root.curOuterRels = types_pathnodes::relids::relids_empty();
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
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_SampleScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_FunctionScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_ValuesScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_CteScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_NamedTuplestoreScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_WorkTableScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_TableFuncScan)
                || p.pathtype == crate::pathnode::tag16(NodeTag::T_Result) =>
        {
            create_scan_plan(run, path_id, flags)
        }
        PathNode::IndexPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::TidPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::TidRangePath(_) => create_scan_plan(run, path_id, flags),
        PathNode::BitmapHeapPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::SubqueryScanPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::ForeignPath(_) => create_scan_plan(run, path_id, flags),
        PathNode::AppendPath(_) => create_append_plan(run, path_id, flags),
        PathNode::MergeAppendPath(_) => create_merge_append_plan(run, path_id, flags),
        PathNode::SetOpPath(_) => create_setop_plan(run, path_id, flags),
        PathNode::RecursiveUnionPath(_) => create_recursiveunion_plan(run, path_id),
        PathNode::ProjectionPath(_) => create_projection_plan(run, path_id, flags),
        PathNode::ProjectSetPath(_) => create_project_set_plan(run, path_id),
        PathNode::GroupResultPath(_) => create_group_result_plan(run, path_id),
        PathNode::GroupPath(_) => create_group_plan(run, path_id),
        PathNode::AggPath(_) => create_agg_plan(run, path_id),
        PathNode::MinMaxAggPath(_) => create_minmaxagg_plan(run, path_id),
        PathNode::GroupingSetsPath(_) => create_groupingsets_plan(run, path_id),
        PathNode::WindowAggPath(_) => create_windowagg_plan(run, path_id),
        PathNode::UpperUniquePath(_) => create_upper_unique_plan(run, path_id, flags),
        PathNode::SortPath(_) => create_sort_plan(run, path_id, flags),
        PathNode::IncrementalSortPath(_) => create_incremental_sort_plan(run, path_id, flags),
        PathNode::MaterialPath(_) => create_material_plan(run, path_id, flags),
        PathNode::MemoizePath(_) => create_memoize_plan(run, path_id, flags),
        PathNode::NestPath(_) | PathNode::MergePath(_) | PathNode::HashPath(_) => {
            create_join_plan(run, path_id)
        }
        PathNode::GatherPath(_) => create_gather_plan(run, path_id),
        PathNode::GatherMergePath(_) => create_gather_merge_plan(run, path_id),
        PathNode::LimitPath(_) => create_limit_plan(run, path_id, flags),
        PathNode::LockRowsPath(_) => create_lockrows_plan(run, path_id, flags),
        PathNode::UniquePath(_) => create_unique_plan(run, path_id, flags),
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
    if (rel.rtekind != types_pathnodes::RTE_RELATION
        && rel.rtekind != types_pathnodes::RTE_SUBQUERY
        && rel.rtekind != types_pathnodes::RTE_FUNCTION
        && rel.rtekind != types_pathnodes::RTE_TABLEFUNC
        && rel.rtekind != types_pathnodes::RTE_VALUES
        && rel.rtekind != types_pathnodes::RTE_CTE)
        || rel.reloptkind != types_pathnodes::RELOPT_BASEREL
    {
        return false;
    }
    // An empty-tlist bitmap scan stays as-is (may skip heap fetches).
    if base.pathtype == crate::pathnode::tag16(NodeTag::T_BitmapHeapScan)
        && base
            .pathtarget_id
            .is_some_and(|id| run.root.pathtarget(id).exprs.is_empty())
    {
        return false;
    }
    for attno in rel.min_attr..=0 {
        let ndx = (attno - rel.min_attr) as usize;
        if !crate::relnode::relids_is_empty(&rel.attr_needed[ndx]) {
            return false;
        }
    }
    for i in 0..run.root.placeholder_list.len() {
        let phinfo = run.root.phinfo(run.root.placeholder_list[i]);
        if !crate::relnode::relids_is_subset(&phinfo.ph_needed, &rel.relids)
            && crate::relnode::relids_is_subset(&phinfo.ph_eval_at, &rel.relids)
        {
            return false;
        }
    }
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
    // CP_LABEL_TLIST: labeled sort/group columns must be distinct simple Vars
    // (they appear in the physical tlist and are relabeled by
    // apply_pathtarget_labeling_to_tlist); anything else forces the path tlist.
    if flags & CP_LABEL_TLIST != 0 {
        let target = run.root.pathtarget(base.pathtarget_id.unwrap());
        let mut sortgroupatts: mcx::PgVec<'_, i16> = mcx::PgVec::new_in(run.mcx);
        for (i, &sgr) in target.sortgrouprefs.iter().enumerate() {
            if sgr == 0 {
                continue;
            }
            let expr = *run.root.expr_node(target.exprs[i]);
            let Some(var) = expr.as_var() else {
                return false;
            };
            if sortgroupatts.contains(&var.varattno) {
                return false;
            }
            sortgroupatts.push(var.varattno);
        }
    }
    true
}

// apply_pathtarget_labeling_to_tlist (tlist.c), Var-only leg —
// use_physical_tlist admitted only distinct simple-Var labels.
fn apply_pathtarget_labeling_to_tlist(
    run: &PlannerRun<'_>,
    tlist: &NodeList<'_>,
    target_id: PtId,
) {
    let target = run.root.pathtarget(target_id);
    for (i, &sgr) in target.sortgrouprefs.iter().enumerate() {
        if sgr == 0 {
            continue;
        }
        let expr = *run.root.expr_node(target.exprs[i]);
        let var = expr.as_var().expect("use_physical_tlist admitted only Var labels");
        let tle_node = tlist
            .iter()
            .find(|n| {
                let tle = n.as_target_entry().expect("TargetEntry");
                tle.expr
                    .as_var()
                    .is_some_and(|v| v.varno == var.varno && v.varattno == var.varattno)
            })
            .expect("ORDER/GROUP BY expression not found in targetlist");
        // SAFETY: physical-tlist entries were freshly built; no reference
        // derived from them is live across this mutation.
        unsafe {
            tle_node.with_mut::<TargetEntry, _>(|tle| {
                debug_assert!(tle.ressortgroupref == 0 || tle.ressortgroupref == sgr);
                tle.ressortgroupref = sgr;
            })
        }
        .expect("tlist cell is a TargetEntry");
    }
}

// build_physical_tlist (plancat.c), heap-relation + subquery + CTE arms.
fn build_physical_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: types_pathnodes::RelId,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel_id).relid;
    let rte = run.rte(varno as usize);
    if run.root.rel(rel_id).rtekind == types_pathnodes::RTE_SUBQUERY {
        // One Var per subquery output; resjunk columns stay resjunk.
        let sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");
        let mut tlist = NodeList::nil();
        for tle_node in &sub.targetList {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            let (vartype, vartypmod) = crate::costsize::expr_type_typmod(tle.expr);
            let varcollid = crate::pathkeys::expr_collation(tle.expr);
            let var =
                Node::mk_var(mcx, varno as i32, tle.resno, vartype, vartypmod, varcollid, 0)?;
            tlist.lappend(
                mcx,
                Node::mk_target_entry(mcx, var, tle.resno, None, tle.resjunk)?,
            )?;
        }
        return Ok(tlist);
    }
    if run.root.rel(rel_id).rtekind == types_pathnodes::RTE_CTE {
        // expandRTE's CTE leg: one Var per output column.
        let mut tlist = NodeList::nil();
        for (i, (typid, typmod)) in rte.coltypes.iter().zip(rte.coltypmods.iter()).enumerate() {
            let coll = rte.colcollations.iter().nth(i).unwrap_or(0);
            let var =
                Node::mk_var(mcx, varno as i32, (i + 1) as i16, typid, typmod, coll, 0)?;
            let tle = Node::mk_target_entry(mcx, var, (i + 1) as i16, None, false)?;
            tlist.lappend(mcx, tle)?;
        }
        return Ok(tlist);
    }
    if matches!(
        run.root.rel(rel_id).rtekind,
        types_pathnodes::RTE_FUNCTION | types_pathnodes::RTE_TABLEFUNC | types_pathnodes::RTE_VALUES
    ) {
        let (_, colvars) = parse_relation::expandRTE(
            mcx,
            rte,
            varno as i32,
            0,
            types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            -1,
            true,
        )?;
        let mut tlist = NodeList::nil();
        for var_node in &colvars {
            let Some(var) = var_node.as_var() else {
                // A non-Var in expandRTE's output means a dropped column.
                return Ok(NodeList::nil());
            };
            tlist.lappend(
                mcx,
                Node::mk_target_entry(mcx, var_node, var.varattno, None, false)?,
            )?;
        }
        return Ok(tlist);
    }
    let reloid = rte.relid;
    let relation = table::table_open(mcx, reloid, 0)?;
    let mut tlist = NodeList::nil();
    for att in relation.rd_att.attrs.iter() {
        if att.attisdropped || att.atthasmissing {
            // found a dropped or missing col, so punt
            return Ok(NodeList::nil());
        }
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
// replace_nestloop_params + _mutator (createplan.c), Var arm (PHVs loud).
fn replace_nestloop_params<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Ok(replace_nestloop_params_mutator(run, node)?.unwrap_or(node))
}

fn replace_nestloop_params_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    if let Some(v) = node.as_var() {
        debug_assert!(v.varlevelsup == 0);
        if v.varno <= 0 || !crate::relnode::relids_is_member(v.varno, &run.root.curOuterRels) {
            return Ok(None);
        }
        return Ok(Some(crate::paramassign::replace_nestloop_param_var(run, v, node)?));
    }
    if node.node_tag() == NodeTag::T_PlaceHolderVar {
        let phv = node.as_place_holder_var().unwrap();
        debug_assert!(phv.phlevelsup == 0);
        let id = crate::placeholder::find_placeholder_info(run, phv)?;
        let eval_at = crate::relnode::relids_copy(run.mcx, &run.root.phinfo(id).ph_eval_at);
        if !crate::relnode::relids_is_subset(&eval_at, &run.root.curOuterRels) {
            let new_expr = replace_nestloop_params_mutator(run, phv.phexpr)?;
            match new_expr {
                None => return Ok(None),
                Some(e) => {
                    return Ok(Some(Node::mk(
                        run.mcx,
                        types_nodes::primnodes::PlaceHolderVar {
                            phexpr: e,
                            phrels: phv.phrels.clone_in(run.mcx)?,
                            phnullingrels: phv.phnullingrels.clone_in(run.mcx)?,
                            phid: phv.phid,
                            phlevelsup: phv.phlevelsup,
                        },
                    )?));
                }
            }
        }
        return Ok(Some(crate::paramassign::replace_nestloop_param_placeholdervar(
            run, phv, node,
        )?));
    }
    clauses::walker::expression_tree_mutator(run.mcx, node, &mut |n| {
        replace_nestloop_params_mutator(run, n)
    })
}

fn replace_nestloop_params_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    match clauses::walker::mutate_list(run.mcx, list, &mut |n| {
        replace_nestloop_params_mutator(run, n)
    })? {
        Some(l) => Ok(l),
        None => Ok(list.clone_in(run.mcx)?),
    }
}

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
        // Parameterized paths enforce their movable join clauses at the scan.
        if let Some(ppi) = run.root.path(best_path).base().param_info.as_deref() {
            for &rid in ppi.ppi_clauses.iter() {
                if !v.iter().any(|&x| x == rid) {
                    v.push(rid);
                }
            }
        }
        v
    };

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
        if physical.is_nil() {
            // Failed because of dropped cols, so use regular method
            let target_id = run.root.path(best_path).base().pathtarget_id.unwrap();
            build_path_tlist(run, target_id, best_path)?
        } else {
            if flags & CP_LABEL_TLIST != 0 {
                let target_id = run.root.path(best_path).base().pathtarget_id.unwrap();
                apply_pathtarget_labeling_to_tlist(run, &physical, target_id);
            }
            physical
        }
    } else {
        let target_id = run.root.path(best_path).base().pathtarget_id.unwrap();
        build_path_tlist(run, target_id, best_path)?
    };

    let plan = match pathtype {
        t if t == crate::pathnode::tag16(NodeTag::T_SeqScan) => {
            create_seqscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_SampleScan) => {
            create_samplescan_plan(run, best_path, tlist, scan_clauses)?
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
        t if t == crate::pathnode::tag16(NodeTag::T_TidScan) => {
            create_tidscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_TidRangeScan) => {
            create_tidrangescan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_FunctionScan) => {
            create_functionscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_TableFuncScan) => {
            create_tablefuncscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_ValuesScan) => {
            create_valuesscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_CteScan) => {
            create_ctescan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_NamedTuplestoreScan) => {
            create_namedtuplestorescan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_WorkTableScan) => {
            create_worktablescan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_SubqueryScan) => {
            create_subqueryscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_Result) => {
            create_resultscan_plan(run, best_path, tlist, scan_clauses)?
        }
        t if t == crate::pathnode::tag16(NodeTag::T_ForeignScan) => {
            create_foreignscan_plan(run, best_path, tlist, scan_clauses)?
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
    let tlist = build_path_tlist(run, target_id, path_id)?;

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

// order_qual_clauses (createplan.c): stable sort (C insertion sort) by
// security_level then eval cost; a cheap (<10x cpu_operator_cost) leakproof
// qual is demoted to level 0 so it can run ahead of pricier low-level quals.
fn order_qual_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
) -> PgResult<mcx::PgVec<'mcx, RinfoId>> {
    let mut items: mcx::PgVec<'_, (RinfoId, f64, u32)> = mcx::PgVec::new_in(run.mcx);
    items.reserve(clauses.len());
    for &rid in clauses {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), clause)?;
        let r = run.root.rinfo(rid);
        let security_level =
            if r.leakproof && cost.per_tuple < 10.0 * crate::gucs::cpu_operator_cost() {
                0
            } else {
                r.security_level
            };
        items.push((rid, cost.per_tuple, security_level));
    }
    if items.len() > 1 {
        items.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.partial_cmp(&b.1).unwrap()));
    }
    let mut out = mcx::PgVec::new_in(run.mcx);
    out.extend(items.iter().map(|x| x.0));
    Ok(out)
}
pub fn extract_actual_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    rinfos: &[RinfoId],
) -> NodeList<'mcx> {
    let mut out = NodeList::nil();
    for &rid in rinfos {
        if run.root.rinfo(rid).pseudoconstant || rinfo_is_constant_true(run, rid) {
            continue;
        }
        out.lappend(run.mcx, *run.root.expr_node(run.root.rinfo(rid).clause))
            .expect("lappend");
    }
    out
}

// rinfo_is_constant_true (restrictinfo.c:444): reconsider_outer_join_clauses
// throws back dummy constant-TRUE clauses; they must not reach the plan.
fn rinfo_is_constant_true(run: &PlannerRun<'_>, rid: RinfoId) -> bool {
    match run.root.expr_node(run.root.rinfo(rid).clause).as_const() {
        Some(c) => !c.constisnull && c.constvalue.as_bool(),
        None => false,
    }
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
        if run.root.rinfo(rid).pseudoconstant || rinfo_is_constant_true(run, rid) {
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
        types_pathnodes::JOIN_FULL => types_nodes::JoinType::JOIN_FULL,
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
    let mut qpqual = extract_actual_clauses(run, &ordered);
    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
    }

    let mut plan = Node::build::<SeqScan<'mcx>>(mcx)?;
    plan.cb_scan_cols = seqscan_consumed_cols(run, best_path, &qpqual)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// pgrust-only (SeqScan::cb_scan_cols): the exact 1-based attno set this scan
// must deliver — the path's OWN target (the rel's reltarget: exactly the
// columns anything above the scan reads, per attr_needed bookkeeping) plus
// the scan clauses' Vars. Captured HERE, before `use_physical_tlist` may
// swap the plan tlist for the whole-row physical one, because the executor
// cannot tell an inflated tlist from a genuine whole-row read. None =
// wholerow/system-column reference (fall back to the plan-tlist walk).
fn seqscan_consumed_cols<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    qpqual: &NodeList<'mcx>,
) -> PgResult<Option<types_nodes::bitmapset::Bitmapset<'mcx>>> {
    use nodes_core::NodeWalker as _;

    let base = run.root.path(best_path).base();
    let scanrelid = run.root.rel(base.parent).relid as i32;

    struct Cx<'mcx> {
        mcx: mcx::Mcx<'mcx>,
        scanrelid: i32,
        cols: types_nodes::bitmapset::Bitmapset<'mcx>,
        opaque: bool,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for Cx<'mcx> {
        fn visit(&mut self, n: Node<'mcx>) -> PgResult<bool> {
            if n.node_tag() == NodeTag::T_Var {
                let v = n.as_var().unwrap();
                if v.varno == self.scanrelid && v.varlevelsup == 0 {
                    if v.varattno <= 0 {
                        // Wholerow (0) / system column (<0): the exact set is
                        // not expressible — report unknown.
                        self.opaque = true;
                    } else {
                        self.cols.add_member(self.mcx, v.varattno as i32)?;
                    }
                }
                return Ok(false);
            }
            nodes_core::expression_tree_walker(n, self)
        }
    }
    let mut cx =
        Cx { mcx: run.mcx, scanrelid, cols: types_nodes::bitmapset::Bitmapset::empty(), opaque: false };
    if let Some(pt) = base.pathtarget_id {
        // Walk a snapshot of the expr ids: expr_node borrows root immutably
        // while the walk only reads.
        let n = run.root.pathtarget(pt).exprs.len();
        for i in 0..n {
            let eid = run.root.pathtarget(pt).exprs[i];
            let node = *run.root.expr_node(eid);
            cx.visit(node)?;
        }
    }
    for n in qpqual.iter() {
        cx.visit(n)?;
    }
    Ok(if cx.opaque { None } else { Some(cx.cols) })
}

// create_samplescan_plan (createplan.c): parameterized paths get
// replace_nestloop_params over the quals and the tablesample clause (the
// clause is rebuilt by hand — the expression walker has no
// TableSampleClause arm).
fn create_samplescan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let scan_relid = run.root.rel(run.root.path(best_path).base().parent).relid;
    debug_assert!(scan_relid > 0);
    let rte = run.rte(scan_relid as usize);
    debug_assert!(rte.rtekind == types_nodes::RTEKind::RTE_RELATION);
    let mut tsc = rte.tablesample.expect("sampled rel has a tablesample clause");

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        let t = tsc.as_table_sample_clause().expect("TableSampleClause");
        let args = replace_nestloop_params_list(run, &t.args)?;
        let repeatable = match t.repeatable {
            Some(r) => Some(replace_nestloop_params(run, r)?),
            None => None,
        };
        tsc = Node::mk(
            mcx,
            types_nodes::parsenodes::TableSampleClause {
                tsmhandler: t.tsmhandler,
                args,
                repeatable,
            },
        )?;
    }

    let mut plan = Node::build::<SampleScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.tablesample = Some(tsc);
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// is_redundant_derived_clause (equivclass.c).
fn is_redundant_derived_clause(run: &PlannerRun<'_>, rid: RinfoId, others: &[RinfoId]) -> bool {
    let Some(parent_ec) = run.root.rinfo(rid).parent_ec else {
        return false;
    };
    others.iter().any(|&o| run.root.rinfo(o).parent_ec == Some(parent_ec))
}

// create_tidscan_plan (createplan.c). tidquals has OR semantics, so qpqual
// dedup differs by arity: single tidqual dedups in RestrictInfo form,
// multiple dedup as an explicit OR clause via equal() after stripping.
fn create_tidscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    debug_assert!(run.root.rel(rel_id).rtekind == types_pathnodes::RTE_RELATION);

    let tidqual_rids: mcx::PgVec<'mcx, RinfoId> = {
        let PathNode::TidPath(tp) = run.root.path(best_path) else {
            unreachable!("TidPath");
        };
        let mut v = mcx::PgVec::new_in(mcx);
        v.extend(tp.tidquals.iter().copied());
        v
    };

    let scan_clauses = if tidqual_rids.len() == 1 {
        let mut qpqual = mcx::PgVec::new_in(mcx);
        for &rid in scan_clauses.iter() {
            if run.root.rinfo(rid).pseudoconstant {
                continue;
            }
            if tidqual_rids.iter().any(|&t| t == rid) {
                continue;
            }
            if is_redundant_derived_clause(run, rid, &tidqual_rids) {
                continue;
            }
            qpqual.push(rid);
        }
        qpqual
    } else {
        scan_clauses
    };

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut tidquals = extract_actual_clauses(run, &tidqual_rids);
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if tidquals.len() > 1 {
        // list_difference(scan_clauses, list_make1(make_orclause(tidquals)))
        let orclause = clauses::make_orclause(mcx, tidquals.clone_in(mcx)?)?;
        let mut kept = NodeList::nil();
        for c in &qpqual {
            if !types_nodes::equal::equal(c, orclause) {
                kept.lappend(mcx, c)?;
            }
        }
        qpqual = kept;
    }

    if run.root.path(best_path).base().param_info.is_some() {
        tidquals = replace_nestloop_params_list(run, &tidquals)?;
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
    }

    let mut plan = Node::build::<types_nodes::TidScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.tidquals = tidquals;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_tidrangescan_plan (createplan.c). tidrangequals has AND semantics.
fn create_tidrangescan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    debug_assert!(run.root.rel(rel_id).rtekind == types_pathnodes::RTE_RELATION);

    let tidrangequal_rids: mcx::PgVec<'mcx, RinfoId> = {
        let PathNode::TidRangePath(tp) = run.root.path(best_path) else {
            unreachable!("TidRangePath");
        };
        let mut v = mcx::PgVec::new_in(mcx);
        v.extend(tp.tidrangequals.iter().copied());
        v
    };

    let mut qpqual_rids = mcx::PgVec::new_in(mcx);
    for &rid in scan_clauses.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        if tidrangequal_rids.iter().any(|&t| t == rid) {
            continue;
        }
        qpqual_rids.push(rid);
    }

    let ordered = order_qual_clauses(run, &qpqual_rids)?;
    let mut tidrangequals = extract_actual_clauses(run, &tidrangequal_rids);
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        tidrangequals = replace_nestloop_params_list(run, &tidrangequals)?;
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
    }

    let mut plan = Node::build::<types_nodes::TidRangeScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.tidrangequals = tidrangequals;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_functionscan_plan (createplan.c).
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
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        let mut new_functions = NodeList::nil();
        for f_node in &functions {
            let f = f_node.as_range_tbl_function().expect("functions cell");
            let funcexpr = match f.funcexpr {
                Some(e) => Some(replace_nestloop_params(run, e)?),
                None => None,
            };
            new_functions.lappend(
                mcx,
                Node::mk(
                    mcx,
                    types_nodes::parsenodes::RangeTblFunction {
                        funcexpr,
                        funccolcount: f.funccolcount,
                        funccolnames: f.funccolnames.clone_in(mcx)?,
                        funccoltypes: f.funccoltypes.clone_in(mcx)?,
                        funccoltypmods: f.funccoltypmods.clone_in(mcx)?,
                        funccolcollations: f.funccolcollations.clone_in(mcx)?,
                        funcparams: f.funcparams.clone_in(mcx)?,
                    },
                )?,
            )?;
        }
        functions = new_functions;
    }

    let mut plan = Node::build::<types_nodes::plannodes::FunctionScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.functions = functions;
    plan.funcordinality = funcordinality;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_tablefuncscan_plan (createplan.c).
fn create_tablefuncscan_plan<'mcx>(
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
    debug_assert!(rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_TABLEFUNC);
    let mut tablefunc = rte.tablefunc.expect("TABLEFUNC RTE has tablefunc");

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        tablefunc = replace_nestloop_params(run, tablefunc)?;
    }

    let mut plan = Node::build::<types_nodes::plannodes::TableFuncScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.tablefunc = Some(tablefunc);
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_valuesscan_plan (createplan.c).
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
    let mut qpqual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        values_lists = replace_nestloop_params_list(run, &values_lists)?;
    }

    let mut plan = Node::build::<types_nodes::plannodes::ValuesScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.values_lists = values_lists;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_resultscan_plan (createplan.c): an RTE_RESULT scan is a childless
// Result whose quals ride resconstantqual (C's make_result second arg).
fn create_resultscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    debug_assert!(run.root.rel(rel_id).relid > 0);
    debug_assert!(
        run.root.rel(rel_id).rtekind == types_nodes::parsenodes::RTEKind::RTE_RESULT as u32
    );

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut quals = extract_actual_clauses(run, &ordered);
    if run.root.path(best_path).base().param_info.is_some() {
        quals = replace_nestloop_params_list(run, &quals)?;
    }

    let mut plan = Node::build::<types_nodes::plannodes::Result<'mcx>>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.resconstantqual =
        if quals.is_nil() { None } else { Some(Node::mk_list(mcx, quals)?) };
    copy_generic_path_info(run, &mut plan.plan, best_path);
    Ok(plan.seal())
}

// create_foreignscan_plan (createplan.c): the FDW builds the node (it may
// move restriction clauses to remote execution); core fills in the rest.
fn create_foreignscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rel_id = run.root.path(best_path).base().parent;
    let scan_relid = run.root.rel(rel_id).relid;
    let kind = run
        .root
        .rel(rel_id)
        .fdwroutine
        .expect("create_foreignscan_plan: rel has fdwroutine");
    let fdw_outerpath = match run.root.path(best_path) {
        PathNode::ForeignPath(fp) => fp.fdw_outerpath,
        other => panic!(
            "create_foreignscan_plan (createplan.c): pathtype {}",
            other.base().pathtype
        ),
    };

    let outer_plan = match fdw_outerpath {
        Some(p) => Some(create_plan_recurse(run, p, CP_EXACT_TLIST)?),
        None => None,
    };

    let rel_oid = if scan_relid > 0 {
        debug_assert_eq!(run.root.rel(rel_id).rtekind, types_pathnodes::RTE_RELATION);
        let rte = run.rte(scan_relid as usize);
        debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_RELATION);
        rte.relid
    } else {
        0
    };

    let ordered = order_qual_clauses(run, &scan_clauses)?;

    let plan = (crate::fdwplan::fdw_plan_routine(kind).get_foreign_plan)(
        run, rel_id, rel_oid, best_path, tlist, ordered, outer_plan,
    )?;
    debug_assert_eq!(plan.node_tag(), NodeTag::T_ForeignScan);

    if run.root.rel(rel_id).reloptkind == types_pathnodes::RELOPT_UPPER_REL {
        panic!("create_foreignscan_plan (createplan.c): upper-rel ForeignPath unported");
    }
    let mut fs_relids = types_nodes::bitmapset::Bitmapset::empty();
    for m in types_pathnodes::relids::relids_members(&run.root.rel(rel_id).relids) {
        fs_relids.add_member(mcx, m)?;
    }
    let mut fs_base_relids = fs_relids.clone_in(mcx)?;
    for m in types_pathnodes::relids::relids_members(&run.root.outer_join_rels) {
        fs_base_relids.del_member(m);
    }

    let (check_as_user, fs_server) = {
        let rel = run.root.rel(rel_id);
        if rel.useridiscurrent {
            run.glob.depends_on_role = true;
        }
        (run.root.rel(rel_id).userid, run.root.rel(rel_id).serverid)
    };

    // replace_nestloop_params runs after the FDW callback because parts of
    // fdw_exprs/fdw_recheck_quals may have come from join clauses.
    let replaced = if run.root.path(best_path).base().param_info.is_some() {
        let fs = plan.as_foreign_scan().expect("ForeignScan node");
        let qual = replace_nestloop_params_list(run, &fs.scan.plan.qual)?;
        let fdw_exprs = replace_nestloop_params_list(run, &fs.fdw_exprs)?;
        let fdw_recheck_quals = replace_nestloop_params_list(run, &fs.fdw_recheck_quals)?;
        Some((qual, fdw_exprs, fdw_recheck_quals))
    } else {
        None
    };

    let mut fs_system_col = false;
    if scan_relid > 0 {
        let mut attrs_used = types_nodes::bitmapset::Bitmapset::empty();
        // rel's targetlist, not attr_needed (unset for inheritance children).
        let exprs =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel_reltarget(rel_id).exprs);
        for &eid in exprs.iter() {
            vars::pull_varattnos(mcx, *run.root.expr_node(eid), scan_relid as i32, &mut attrs_used)?;
        }
        let rids = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel_id).baserestrictinfo);
        for &rid in rids.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            vars::pull_varattnos(mcx, clause, scan_relid as i32, &mut attrs_used)?;
        }
        for i in (FirstLowInvalidHeapAttributeNumber + 1)..0 {
            if attrs_used.is_member(i - FirstLowInvalidHeapAttributeNumber) {
                fs_system_col = true;
                break;
            }
        }
    }

    // SAFETY: exclusive plan-tree ownership (just built by the FDW callback).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::ForeignScan, _>(|fs| {
            copy_generic_path_info(run, &mut fs.scan.plan, best_path);
            fs.checkAsUser = check_as_user;
            fs.fs_server = fs_server;
            fs.fs_relids = fs_relids;
            fs.fs_base_relids = fs_base_relids;
            if let Some((qual, fdw_exprs, fdw_recheck_quals)) = replaced {
                fs.scan.plan.qual = qual;
                fs.fdw_exprs = fdw_exprs;
                fs.fdw_recheck_quals = fdw_recheck_quals;
            }
            fs.fsSystemCol = fs_system_col;
        })
    }
    .expect("ForeignScan node");

    Ok(plan)
}

// make_foreignscan (createplan.c), the constructor an FDW's GetForeignPlan
// calls; costs, checkAsUser/fs_server, relid sets fill in afterwards.
#[allow(clippy::too_many_arguments)]
pub fn make_foreignscan<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    qptlist: NodeList<'mcx>,
    qpqual: NodeList<'mcx>,
    scanrelid: u32,
    fdw_exprs: NodeList<'mcx>,
    fdw_private: NodeList<'mcx>,
    fdw_scan_tlist: NodeList<'mcx>,
    fdw_recheck_quals: NodeList<'mcx>,
    outer_plan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mut plan = Node::build::<types_nodes::plannodes::ForeignScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = qptlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.plan.lefttree = outer_plan;
    plan.scan.scanrelid = scanrelid;
    plan.fdw_exprs = fdw_exprs;
    plan.fdw_private = fdw_private;
    plan.fdw_scan_tlist = fdw_scan_tlist;
    plan.fdw_recheck_quals = fdw_recheck_quals;
    Ok(plan.seal())
}

// create_worktablescan_plan (createplan.c): the wt param ID comes from the
// cteroot, resolved at set_worktable_pathlist time (the parent-root chain is
// detached here; see PlannerInfo.self_ref_wt_param).
fn create_worktablescan_plan<'mcx>(
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
    debug_assert!(rte.self_reference);
    let wt_param = run.root.self_ref_wt_param;
    assert!(
        wt_param >= 0,
        "could not find param ID for CTE \"{}\"",
        rte.ctename.unwrap_or("?")
    );
    debug_assert!(run.root.path(best_path).base().param_info.is_none());

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::WorkTableScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.wtParam = wt_param;
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
    let (plan_id, cte_param_id) = crate::cte::cte_plan_id_and_param(run, scan_relid as usize);

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut qpqual = extract_actual_clauses(run, &ordered);
    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
    }

    let mut plan = Node::build::<types_nodes::plannodes::CteScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.ctePlanId = plan_id;
    plan.cteParam = cte_param_id;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// create_namedtuplestorescan_plan (createplan.c); param_info empty on this
// lane (asserted at costing), so no nestloop-param replacement.
fn create_namedtuplestorescan_plan<'mcx>(
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
    debug_assert!(rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_NAMEDTUPLESTORE);
    let enrname = rte.enrname;

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let qpqual = extract_actual_clauses(run, &ordered);

    let mut plan = Node::build::<types_nodes::plannodes::NamedTuplestoreScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qpqual;
    plan.scan.scanrelid = scan_relid;
    plan.enrname = enrname;
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
    let (indexoid, indexscandir, baserelid, indexclause_rinfos, orderby_ids, orderby_cols, orderby_pathkeys) = {
        let PathNode::IndexPath(p) = run.root.path(best_path) else {
            panic!("create_indexscan_plan: not an IndexPath")
        };
        let mut rids = mcx::PgVec::new_in(mcx);
        for ic in p.indexclauses.iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            rids.push((rid, ic.lossy, run.root.rinfo(rid).parent_ec));
        }
        let mut ob_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
        ob_ids.extend(p.indexorderbys.iter().copied());
        let mut ob_cols: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
        ob_cols.extend(p.indexorderbycols.iter().copied());
        let mut ob_pks: mcx::PgVec<'mcx, types_pathnodes::PathKey> = mcx::PgVec::new_in(mcx);
        ob_pks.extend(p.path.pathkeys.iter().copied());
        (
            p.indexinfo.as_ref().expect("indexinfo set").indexoid,
            p.indexscandir,
            p.path.parent,
            rids,
            ob_ids,
            ob_cols,
            ob_pks,
        )
    };
    let scan_relid = run.root.rel(baserelid).relid;
    debug_assert!(scan_relid > 0);
    debug_assert!(indexscandir == 1 || indexscandir == -1);

    let (stripped_indexquals, fixed_indexquals) = fix_indexqual_references(run, best_path)?;

    // fix_indexorderby_references (createplan.c).
    let mut indexorderbys = NodeList::nil();
    let mut fixed_indexorderbys = NodeList::nil();
    {
        let index = {
            let PathNode::IndexPath(p) = run.root.path(best_path) else { unreachable!() };
            p.indexinfo.expect("indexinfo set")
        };
        debug_assert!(orderby_ids.len() == orderby_cols.len());
        // ORDER BY clauses are never RowCompare: no per-member indexcolnos.
        let no_indexcolnos: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
        for (&nid, &col) in orderby_ids.iter().zip(orderby_cols.iter()) {
            let clause = *run.root.expr_node(nid);
            indexorderbys.lappend(mcx, clause)?;
            fixed_indexorderbys
                .lappend(mcx, fix_indexqual_clause(run, &index, col, clause, &no_indexcolnos)?)?;
        }
    }

    let mut qpqual_rinfos: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(mcx);
    for &rid in scan_clauses.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        if indexclause_rinfos
            .iter()
            .any(|&(c, lossy, parent_ec)| {
                !lossy
                    && (c == rid
                        || (parent_ec.is_some()
                            && run.root.rinfo(rid).parent_ec == parent_ec))
            })
        {
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if !clauses::contain_mutable_functions(clause)?
            && predicate_implied_by_indexquals(mcx, clause, &stripped_indexquals)?
        {
            continue;
        }
        qpqual_rinfos.push(rid);
    }
    let ordered = order_qual_clauses(run, &qpqual_rinfos)?;
    let mut qpqual = extract_actual_clauses(run, &ordered);

    let mut stripped_indexquals = stripped_indexquals;
    let mut indexorderbys = indexorderbys;
    if run.root.path(best_path).base().param_info.is_some() {
        stripped_indexquals = replace_nestloop_params_list(run, &stripped_indexquals)?;
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        indexorderbys = replace_nestloop_params_list(run, &indexorderbys)?;
    }

    // Sort operators for the ORDER BY expressions' result types: the pathkey
    // has the btree opfamily; the datatype completes the lookup.
    let mut indexorderbyops = types_nodes::list::OidList::nil();
    if !indexorderbys.is_nil() {
        debug_assert!(orderby_pathkeys.len() == indexorderbys.len());
        for (pathkey, expr) in orderby_pathkeys.iter().zip(indexorderbys.iter()) {
            let exprtype = nodes_core::node_funcs::expr_type(expr);
            let sortop = lsyscache::get_opfamily_member_for_cmptype(
                pathkey.pk_opfamily,
                exprtype,
                exprtype,
                pathkey.pk_cmptype,
            )?;
            assert!(
                sortop != 0,
                "missing operator {}({exprtype},{exprtype}) in opfamily {}",
                pathkey.pk_cmptype,
                pathkey.pk_opfamily
            );
            indexorderbyops.lappend(mcx, sortop)?;
        }
    }

    if indexonly {
        let indextlist = ios_indextlist_copy(run, best_path, true)?;
        let mut plan = Node::build::<types_nodes::plannodes::IndexOnlyScan<'mcx>>(mcx)?;
        plan.scan.plan.targetlist = tlist;
        plan.scan.plan.qual = qpqual;
        plan.scan.scanrelid = scan_relid;
        plan.indexid = indexoid;
        plan.indexqual = fixed_indexquals;
        plan.recheckqual = stripped_indexquals;
        plan.indexorderby = fixed_indexorderbys;
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
    plan.indexorderby = fixed_indexorderbys;
    plan.indexorderbyorig = indexorderbys;
    plan.indexorderbyops = indexorderbyops;
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

// create_bitmap_scan_plan (createplan.c).
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
        (p.path.parent, p.bitmapqual.expect("BitmapHeapPath bitmapqual"))
    };
    let scan_relid = run.root.rel(baserelid).relid;
    debug_assert!(scan_relid > 0);

    let (bitmapqualplan, indexquals, mut bitmapqualorig, _indexecs) =
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
        if !clauses::contain_mutable_functions(clause)?
            && predicate_implied_by_indexquals(mcx, clause, &indexquals)?
        {
            continue;
        }
        qpqual_rinfos.push(rid);
    }
    let ordered = order_qual_clauses(run, &qpqual_rinfos)?;
    let mut qpqual = extract_actual_clauses(run, &ordered);

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

    if run.root.path(best_path).base().param_info.is_some() {
        qpqual = replace_nestloop_params_list(run, &qpqual)?;
        bitmapqualorig = replace_nestloop_params_list(run, &bitmapqualorig)?;
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

// create_bitmap_subplan (createplan.c)
// -> (plan, indexquals, bitmapqualorig, indexECs).
fn create_bitmap_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    bitmapqual: PathId,
) -> PgResult<(
    Node<'mcx>,
    NodeList<'mcx>,
    NodeList<'mcx>,
    mcx::PgVec<'mcx, types_pathnodes::EcId>,
)> {
    let mcx = run.mcx;
    match run.root.path(bitmapqual) {
        PathNode::BitmapAndPath(ap) => {
            let subs = ap.bitmapquals.clone();
            let (startup_cost, total_cost, selectivity, parent, parallel_safe) = (
                ap.path.startup_cost,
                ap.path.total_cost,
                ap.bitmapselectivity,
                ap.path.parent,
                ap.path.parallel_safe,
            );
            let mut subplans = NodeList::nil();
            let mut subquals = NodeList::nil();
            let mut subindexquals = NodeList::nil();
            let mut indexecs: mcx::PgVec<'mcx, types_pathnodes::EcId> = mcx::PgVec::new_in(mcx);
            for &sub in subs.iter() {
                let (subplan, subindexqual, subqual, subindexec) =
                    create_bitmap_subplan(run, sub)?;
                subplans.lappend(mcx, subplan)?;
                list_concat_unique(mcx, &mut subquals, &subqual)?;
                list_concat_unique(mcx, &mut subindexquals, &subindexqual)?;
                indexecs.extend(subindexec.iter().copied());
            }
            let mut plan = Node::build::<types_nodes::plannodes::BitmapAnd<'mcx>>(mcx)?;
            plan.bitmapplans = subplans;
            plan.plan.startup_cost = startup_cost;
            plan.plan.total_cost = total_cost;
            plan.plan.plan_rows = crate::costsize::clamp_row_est(
                selectivity * run.root.rel(parent).tuples,
            );
            plan.plan.plan_width = 0;
            plan.plan.parallel_aware = false;
            plan.plan.parallel_safe = parallel_safe;
            return Ok((plan.seal(), subindexquals, subquals, indexecs));
        }
        PathNode::BitmapOrPath(op) => {
            let subs = op.bitmapquals.clone();
            let (startup_cost, total_cost, selectivity, parent, parallel_safe) = (
                op.path.startup_cost,
                op.path.total_cost,
                op.bitmapselectivity,
                op.path.parent,
                op.path.parallel_safe,
            );
            let mut subplans = NodeList::nil();
            let mut subquals = NodeList::nil();
            let mut subindexquals = NodeList::nil();
            let mut const_true_subqual = false;
            let mut const_true_subindexqual = false;
            for &sub in subs.iter() {
                // Per-arm indexECs are dropped: EC-derived quals can't be
                // redundant across OR arms.
                let (subplan, subindexqual, subqual, _) = create_bitmap_subplan(run, sub)?;
                subplans.lappend(mcx, subplan)?;
                if subqual.is_nil() {
                    const_true_subqual = true;
                } else if !const_true_subqual {
                    subquals.lappend(mcx, clauses::make_ands_explicit(mcx, &subqual)?)?;
                }
                if subindexqual.is_nil() {
                    const_true_subindexqual = true;
                } else if !const_true_subindexqual {
                    subindexquals
                        .lappend(mcx, clauses::make_ands_explicit(mcx, &subindexqual)?)?;
                }
            }
            // SAOP-built single-subpath ORs skip the OR step.
            let plan = if subplans.len() == 1 {
                subplans.nth(0)
            } else {
                let mut plan = Node::build::<types_nodes::plannodes::BitmapOr<'mcx>>(mcx)?;
                plan.isshared = false;
                plan.bitmapplans = subplans;
                plan.plan.startup_cost = startup_cost;
                plan.plan.total_cost = total_cost;
                plan.plan.plan_rows = crate::costsize::clamp_row_est(
                    selectivity * run.root.rel(parent).tuples,
                );
                plan.plan.plan_width = 0;
                plan.plan.parallel_aware = false;
                plan.plan.parallel_safe = parallel_safe;
                plan.seal()
            };
            let qual = if const_true_subqual {
                NodeList::nil()
            } else if subquals.len() <= 1 {
                subquals
            } else {
                let mut l = NodeList::nil();
                l.lappend(mcx, clauses::make_orclause(mcx, subquals)?)?;
                l
            };
            let indexqual = if const_true_subindexqual {
                NodeList::nil()
            } else if subindexquals.len() <= 1 {
                subindexquals
            } else {
                let mut l = NodeList::nil();
                l.lappend(mcx, clauses::make_orclause(mcx, subindexquals)?)?;
                l
            };
            return Ok((plan, indexqual, qual, mcx::PgVec::new_in(mcx)));
        }
        _ => {}
    }
    let (indexclauses, indexselectivity, parent, parallel_safe, indpred) = {
        match run.root.path(bitmapqual) {
            PathNode::IndexPath(ip) => (
                ip.indexclauses.clone(),
                ip.indexselectivity,
                ip.path.parent,
                ip.path.parallel_safe,
                ip.indexinfo.as_ref().expect("indexinfo set").indpred.clone(),
            ),
            other => panic!(
                "create_bitmap_subplan (createplan.c): pathtype {}",
                other.base().pathtype
            ),
        }
    };

    // C builds a throwaway IndexScan via create_indexscan_plan and moves its
    // qual lists over; the direct fix_indexqual_references call is the same
    // computation without the discarded node, so it must also replicate
    // create_indexscan_plan's nestloop-param replacement of the stripped
    // quals (fixed quals get theirs inside fix_indexqual_clause).
    let (mut stripped_indexquals, fixed_indexquals) =
        fix_indexqual_references(run, bitmapqual)?;
    if run.root.path(bitmapqual).base().param_info.is_some() {
        stripped_indexquals = replace_nestloop_params_list(run, &stripped_indexquals)?;
    }

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
    let mut indexecs: mcx::PgVec<'mcx, types_pathnodes::EcId> = mcx::PgVec::new_in(mcx);
    for ic in indexclauses.iter() {
        let rid = ic.rinfo.expect("IndexClause rinfo");
        debug_assert!(!run.root.rinfo(rid).pseudoconstant);
        if let Some(pec) = run.root.rinfo(rid).parent_ec {
            // Derived from the same EC as an already-included clause.
            if indexecs.contains(&pec) {
                continue;
            }
        }
        subquals.lappend(mcx, *run.root.expr_node(run.root.rinfo(rid).clause))?;
        for &qid in ic.indexquals.iter() {
            subindexquals.lappend(mcx, *run.root.expr_node(run.root.rinfo(qid).clause))?;
        }
        if let Some(pec) = run.root.rinfo(rid).parent_ec {
            indexecs.push(pec);
        }
    }
    // Index predicate conditions not implied by the pushed-down quals must be
    // rechecked (C: "We can add any index predicate conditions, too").
    for &pid in indpred.iter() {
        let pred = *run.root.expr_node(pid);
        if !crate::predtest::predicate_implied_by(mcx, &[pred], subquals.as_slice(), false)? {
            subquals.lappend(mcx, pred)?;
            subindexquals.lappend(mcx, pred)?;
        }
    }
    Ok((plan.seal(), subindexquals, subquals, indexecs))
}

// predicate_implied_by (predtest.c), strong form: one restriction clause vs
// the implicit-AND indexqual list.
fn predicate_implied_by_indexquals<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pred: Node<'mcx>,
    indexquals: &NodeList<'mcx>,
) -> PgResult<bool> {
    crate::predtest::predicate_implied_by(mcx, &[pred], indexquals.as_slice(), false)
}

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
            fixed.lappend(
                mcx,
                fix_indexqual_clause(run, &index, ic.indexcol as i32, clause, &ic.indexcols)?,
            )?;
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
    indexcolnos: &mcx::PgVec<'mcx, i16>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let clause = replace_nestloop_params(run, clause)?;
    match clause.node_tag() {
        NodeTag::T_OpExpr => {
            let o = clause.as_op_expr().unwrap();
            debug_assert!(o.args.len() == 2);
            let fixed_arg = fix_indexqual_operand(run, index, indexcol, o.args.nth(0))?;
            Node::mk(
                mcx,
                OpExpr {
                    opno: o.opno,
                    // C's later fix_expr_common fills InvalidOid via
                    // set_opfuncid (setrefs.c); our read-only walker cannot,
                    // so resolve at the rebuild (ordering ops arrive unset).
                    opfuncid: if o.opfuncid != 0 {
                        o.opfuncid
                    } else {
                        lsyscache::get_opcode(o.opno)?
                    },
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args: NodeList::make2(mcx, fixed_arg, o.args.nth(1))?,
                    location: o.location,
                },
            )
        }
        NodeTag::T_RowCompareExpr => {
            let rc = clause.as_row_compare_expr().unwrap();
            debug_assert!(rc.largs.len() == indexcolnos.len());
            let mut largs = NodeList::nil();
            for (arg, &col) in rc.largs.iter().zip(indexcolnos.iter()) {
                largs.lappend(mcx, fix_indexqual_operand(run, index, col as i32, arg)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(mcx)?,
                    opfamilies: rc.opfamilies.clone_in(mcx)?,
                    inputcollids: rc.inputcollids.clone_in(mcx)?,
                    largs,
                    rargs: rc.rargs.clone_in(mcx)?,
                },
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let saop = clause.as_scalar_array_op_expr().unwrap();
            debug_assert!(saop.args.len() == 2);
            let fixed_arg = fix_indexqual_operand(run, index, indexcol, saop.args.nth(0))?;
            Node::mk(
                mcx,
                types_nodes::primnodes::ScalarArrayOpExpr {
                    opno: saop.opno,
                    opfuncid: saop.opfuncid,
                    hashfuncid: saop.hashfuncid,
                    negfuncid: saop.negfuncid,
                    useOr: saop.useOr,
                    inputcollid: saop.inputcollid,
                    args: NodeList::make2(mcx, fixed_arg, saop.args.nth(1))?,
                    location: saop.location,
                },
            )
        }
        NodeTag::T_NullTest => {
            let nt = clause.as_null_test().unwrap();
            let fixed_arg =
                fix_indexqual_operand(run, index, indexcol, nt.arg.expect("NullTest.arg"))?;
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(fixed_arg),
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        other => panic!("fix_indexqual_clause (createplan.c): {other:?}; M2 lane"),
    }
}

// fix_indexqual_operand (createplan.c).
fn fix_indexqual_operand<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    indexcol: i32,
    mut node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    // strip_noop_phvs before Relabel stripping (createplan.c:5274).
    node = vars::strip_noop_phvs(mcx, node)?;
    while node.node_tag() == NodeTag::T_RelabelType {
        node = node.as_relabel_type().unwrap().arg;
    }
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    if index.indexkeys[indexcol as usize] != 0 {
        if let Some(var) = node.as_var() {
            if var.varno as u32 == index_relid
                && index.indexkeys[indexcol as usize] == var.varattno as i32
            {
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
        }
        panic!("index key does not match expected index column");
    }
    let mut pos = 0usize;
    for i in 0..indexcol as usize {
        if index.indexkeys[i] == 0 {
            pos += 1;
        }
    }
    let id = *index.indexprs.get(pos).expect("too few entries in indexprs list");
    let raw = *run.root.expr_node(id);
    let mut indexkey = raw;
    if indexkey.node_tag() == NodeTag::T_RelabelType {
        indexkey = indexkey.as_relabel_type().unwrap().arg;
    }
    if types_nodes::equal(node, indexkey) {
        return Node::mk_var(
            mcx,
            INDEX_VAR,
            (indexcol + 1) as i16,
            nodes_core::expr_type(raw),
            -1,
            nodes_core::expr_collation(raw),
            0,
        );
    }
    panic!("index key does not match expected index column");
}

// create_projection_plan (createplan.c).
fn create_projection_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    // flags == 0 arrives from create_project_set_plan (C passes 0 there too).
    debug_assert!(flags == 0 || flags & (CP_EXACT_TLIST | CP_SMALL_TLIST | CP_LABEL_TLIST) != 0);
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
    // The flags check is hoisted out of use_physical_tlist so the dominant
    // CP_EXACT_TLIST callers skip the call.
    if flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) == 0 && use_physical_tlist(run, path_id, flags) {
        // C: the caller doesn't care what tlist comes back, so don't
        // project — the subplan keeps its own (physical) tlist and only the
        // costs are relabeled.
        let subplan = create_plan_recurse(run, subpath_id, 0)?;
        if flags & CP_LABEL_TLIST != 0 {
            apply_pathtarget_labeling_to_tlist(
                run,
                &subplan.as_plan().expect("plan node").targetlist,
                target_id,
            );
        }
        let width = run.root.pathtarget(target_id).width;
        // SAFETY: subplan was created above; no other handle to it exists yet.
        unsafe {
            subplan.with_plan_mut(|p| {
                p.startup_cost = path_costs.0;
                p.total_cost = path_costs.1;
                p.plan_rows = path_costs.2;
                p.plan_width = width;
                p.parallel_safe = path_costs.3;
            })
        }
        .expect("subplan embeds a Plan base");
        return Ok(subplan);
    }
    if !is_projection_capable_pathtype(run.root.path(subpath_id).base().pathtype) {
        // Result arm (projection-incapable subplan, e.g. Append).
        let subplan = create_plan_recurse(run, subpath_id, 0)?;
        let tlist = build_path_tlist(run, target_id, path_id)?;
        if !tlist_same_exprs(&tlist, &subplan.as_plan().expect("plan node").targetlist) {
            let mut plan = Node::build::<ResultPlan<'mcx>>(run.mcx)?;
            plan.plan.targetlist = tlist;
            plan.plan.lefttree = Some(subplan);
            copy_generic_path_info(run, &mut plan.plan, path_id);
            return Ok(plan.seal());
        }
        let width = run.root.pathtarget(target_id).width;
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
        return Ok(subplan);
    }

    let subplan = create_plan_recurse(run, subpath_id, CP_IGNORE_TLIST)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;
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

// create_project_set_plan (createplan.c).
fn create_project_set_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id) = match run.root.path(path_id) {
        PathNode::ProjectSetPath(p) => (
            p.subpath.expect("ProjectSetPath has a subpath"),
            p.path.pathtarget_id.unwrap(),
        ),
        _ => unreachable!(),
    };
    let subplan = create_plan_recurse(run, subpath_id, 0)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;
    let mut plan = Node::build::<types_nodes::plannodes::ProjectSet>(run.mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.lefttree = Some(subplan);
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_modifytable_plan + make_modifytable (createplan.c), single-relation
// INSERT/UPDATE/DELETE arm: no FDW result rels, no MERGE lists.
fn create_modifytable_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, operation, can_set_tag, nominal, root_rel, part_cols_updated, result_relations, epq_param, onconflict_id, row_mark_ids) = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        (
            p.subpath.expect("ModifyTablePath has a subpath"),
            p.operation,
            p.canSetTag,
            p.nominalRelation,
            p.rootRelation,
            p.partColsUpdated,
            crate::relnode::pgvec_clone_shallow(mcx, &p.resultRelations),
            p.epqParam,
            p.onconflict,
            crate::relnode::pgvec_clone_shallow(mcx, &p.rowMarks),
        )
    };
    use types_nodes::nodes_enums::CmdType;
    let operation = match operation {
        x if x == CmdType::CMD_INSERT as u32 => CmdType::CMD_INSERT,
        x if x == CmdType::CMD_UPDATE as u32 => CmdType::CMD_UPDATE,
        x if x == CmdType::CMD_DELETE as u32 => CmdType::CMD_DELETE,
        x if x == CmdType::CMD_MERGE as u32 => CmdType::CMD_MERGE,
        other => panic!("make_modifytable (createplan.c): operation {other} unported"),
    };

    let subplan = create_plan_recurse(run, subpath_id, CP_EXACT_TLIST)?;
    apply_tlist_labeling(subplan, run.processed_tlist());

    let update_colnos_lists = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
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

    let with_check_option_lists = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
        let mut ids: mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::NodeId>> =
            mcx::PgVec::new_in(mcx);
        for wlist in p.withCheckOptionLists.iter() {
            ids.push(crate::relnode::pgvec_clone_shallow(mcx, wlist));
        }
        let mut lists = types_nodes::list::NodeList::nil();
        for wlist in ids.iter() {
            let mut nl = types_nodes::list::NodeList::nil();
            for &id in wlist.iter() {
                nl.lappend(mcx, *run.root.expr_node(id))?;
            }
            lists.lappend(mcx, Node::mk_list(mcx, nl)?)?;
        }
        lists
    };

    let returning_lists = {
        let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
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
    plan.withCheckOptionLists = with_check_option_lists;
    plan.returningLists = returning_lists;
    plan.canSetTag = can_set_tag;
    plan.nominalRelation = nominal;
    plan.rootRelation = root_rel;
    plan.partColsUpdated = part_cols_updated;
    let mut rr = types_nodes::list::IntList::nil();
    for &rti in result_relations.iter() {
        rr.lappend(mcx, rti)?;
    }
    plan.resultRelations = rr;
    plan.epqParam = epq_param;
    // make_modifytable: PlanRowMark nodes materialize from the run store
    // (C shares root->rowMarks pointers) — the executor's EPQ aux rowmarks.
    for &id in row_mark_ids.iter() {
        plan.rowMarks.lappend(mcx, Node::mk(mcx, *run.rowmark(id))?)?;
    }
    plan.returningOldAlias = run.parse().returningOldAlias;
    plan.returningNewAlias = run.parse().returningNewAlias;
    if let Some(ocid) = onconflict_id {
        let oc = run
            .root
            .expr_node(ocid)
            .as_on_conflict_expr()
            .expect("ModifyTablePath onconflict is OnConflictExpr");
        plan.onConflictAction = oc.action as u32;
        // The executor wants consecutive resnos in onConflictSet; the real
        // target column numbers move to onConflictCols (C make_modifytable).
        plan.onConflictSet = oc.onConflictSet.clone_in(mcx)?;
        let colnos = crate::prep::extract_update_targetlist_colnos(mcx, &plan.onConflictSet);
        let mut cols = types_nodes::list::IntList::nil();
        for &c in colnos.iter() {
            cols.lappend(mcx, c as i32)?;
        }
        plan.onConflictCols = cols;
        plan.onConflictWhere = oc.onConflictWhere;
        plan.arbiterIndexes = crate::plancat::infer_arbiter_indexes(run, oc)?;
        plan.exclRelRTI = oc.exclRelIndex as u32;
        plan.exclRelTlist = oc.exclRelTlist.clone_in(mcx)?;
    }
    {
        let (action_lists, join_conds) = {
            let PathNode::ModifyTablePath(p) = run.root.path(path_id) else { unreachable!() };
            let mut lists: mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::NodeId>> =
                mcx::PgVec::new_in(mcx);
            for al in p.mergeActionLists.iter() {
                lists.push(crate::relnode::pgvec_clone_shallow(mcx, al));
            }
            let mut conds: mcx::PgVec<'mcx, Option<types_pathnodes::NodeId>> =
                mcx::PgVec::new_in(mcx);
            for &c in p.mergeJoinConditions.iter() {
                conds.push(c);
            }
            (lists, conds)
        };
        let mut mal = types_nodes::list::NodeList::nil();
        for al in action_lists.iter() {
            let mut nl = types_nodes::list::NodeList::nil();
            for &id in al.iter() {
                nl.lappend(mcx, *run.root.expr_node(id))?;
            }
            mal.lappend(mcx, Node::mk_list(mcx, nl)?)?;
        }
        plan.mergeActionLists = mal;
        let mut mjc = types_nodes::list::NodeList::nil();
        for &c in join_conds.iter() {
            // A None condition (no BY SOURCE actions) rides as an empty
            // implicit-AND list: ExecQual over it is constant true, matching
            // C's NULL-condition semantics.
            let cell = match c {
                Some(id) => *run.root.expr_node(id),
                None => Node::mk_list(mcx, types_nodes::list::NodeList::nil())?,
            };
            mjc.lappend(mcx, cell)?;
        }
        plan.mergeJoinConditions = mjc;
    }
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
    let tlist = build_path_tlist(run, target_id, path_id)?;
    let width = run.root.pathtarget(target_id).width;

    // order_qual_clauses over bare clauses: stable sort by per-tuple cost
    // (security_level is 0 for bare quals).
    let mut items: mcx::PgVec<'_, (types_pathnodes::NodeId, f64)> = mcx::PgVec::new_in(run.mcx);
    items.reserve(quals.len());
    for &id in quals.iter() {
        let node = *run.root.expr_node(id);
        let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
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
fn create_group_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id, qual_ids, group_clause) = match run.root.path(path_id) {
        PathNode::GroupPath(gp) => (
            gp.subpath.expect("GroupPath has a subpath"),
            gp.path.pathtarget_id.unwrap(),
            crate::relnode::pgvec_clone_shallow(run.mcx, &gp.qual),
            crate::relnode::pgvec_clone_shallow(run.mcx, &gp.groupClause),
        ),
        _ => unreachable!(),
    };
    let qual = order_bare_qual_clauses(run, &qual_ids)?;

    // Group can project; grouping columns must be available (CP_LABEL_TLIST).
    let subplan = create_plan_recurse(run, subpath_id, CP_LABEL_TLIST)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;

    // extract_grouping_cols/ops/collations (tlist.c) against the subplan tlist.
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
                .expect("GroupPath.groupClause cell");
            (scl.tleSortGroupRef, scl.eqop)
        };
        let tle_node = subplan_tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sgref)
            .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
        let tle = tle_node.as_target_entry().unwrap();
        grp_col_idx.push(tle.resno);
        grp_operators.push(eqop);
        grp_collations.push(expr_collation(tle.expr));
    }

    let mut plan = Node::build::<Group>(run.mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = qual;
    plan.plan.lefttree = Some(subplan);
    plan.numCols = num_cols as i32;
    plan.grpColIdx = mcx::vec_borrow_in(run.mcx, grp_col_idx)?;
    plan.grpOperators = mcx::vec_borrow_in(run.mcx, grp_operators)?;
    plan.grpCollations = mcx::vec_borrow_in(run.mcx, grp_collations)?;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

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
    let tlist = build_path_tlist(run, target_id, path_id)?;

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


// create_unique_plan (createplan.c).
fn create_unique_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, umethod, in_operators, uniq_expr_ids, target_id, parallel_safe) =
        match run.root.path(path_id) {
            PathNode::UniquePath(up) => (
                up.subpath.expect("UniquePath has a subpath"),
                up.umethod,
                crate::relnode::pgvec_clone_shallow(mcx, &up.in_operators),
                crate::relnode::pgvec_clone_shallow(mcx, &up.uniq_exprs),
                up.path.pathtarget_id.unwrap(),
                up.path.parallel_safe,
            ),
            _ => unreachable!(),
        };
    let mut subplan = create_plan_recurse(run, subpath_id, flags)?;
    if umethod == types_pathnodes::UNIQUE_PATH_NOOP {
        return Ok(subplan);
    }

    let mut newtlist = build_path_tlist(run, target_id, path_id)?;
    let mut newitems = false;
    for &uid in uniq_expr_ids.iter() {
        let uniqexpr = *run.root.expr_node(uid);
        let found = newtlist
            .iter()
            .any(|n| types_nodes::equal(n.as_target_entry().expect("tlist cell").expr, uniqexpr));
        if !found {
            let resno = newtlist.len() as i16 + 1;
            newtlist.lappend(mcx, Node::mk_target_entry(mcx, uniqexpr, resno, None, false)?)?;
            newitems = true;
        }
    }
    if newitems || umethod == types_pathnodes::UNIQUE_PATH_SORT {
        // change_plan_targetlist (createplan.c).
        if !crate::pathnode::is_projection_capable_pathtype(subplan.node_tag() as u16)
            && !tlist_same_exprs(&newtlist, &subplan.as_plan().expect("plan node").targetlist)
        {
            let sub = subplan.as_plan().expect("plan node");
            let mut result = Node::build::<ResultPlan>(mcx)?;
            result.plan.targetlist = newtlist.clone_in(mcx)?;
            result.plan.qual = NodeList::nil();
            result.plan.lefttree = Some(subplan);
            result.plan.disabled_nodes = sub.disabled_nodes;
            result.plan.startup_cost = sub.startup_cost;
            result.plan.total_cost = sub.total_cost;
            result.plan.plan_rows = sub.plan_rows;
            result.plan.plan_width = sub.plan_width;
            result.plan.parallel_aware = false;
            result.plan.parallel_safe = sub.parallel_safe && parallel_safe;
            subplan = result.seal();
        } else {
            // SAFETY: subplan was freshly built by create_plan_recurse; no
            // reference derived from its tlist is live across this write.
            unsafe {
                subplan.with_plan_mut(|p| {
                    p.targetlist = newtlist;
                    p.parallel_safe = p.parallel_safe && parallel_safe;
                })
            }
            .expect("plan node");
        }
    }

    let subplan_tlist =
        NodeList::from_slice(mcx, subplan.as_plan().expect("plan node").targetlist.as_slice())?;
    let mut grp_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut grp_collations: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    for &uid in uniq_expr_ids.iter() {
        let uniqexpr = *run.root.expr_node(uid);
        let tle = subplan_tlist
            .iter()
            .find(|n| {
                types_nodes::equal(n.as_target_entry().expect("tlist cell").expr, uniqexpr)
            })
            .unwrap_or_else(|| panic!("failed to find unique expression in subplan tlist"))
            .as_target_entry()
            .unwrap();
        grp_col_idx.push(tle.resno);
        grp_collations.push(expr_collation(tle.expr));
    }
    let num_cols = uniq_expr_ids.len();
    let rows = run.root.path(path_id).base().rows;
    if umethod == types_pathnodes::UNIQUE_PATH_HASH {
        let mut grp_operators: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
        for &in_oper in in_operators.iter() {
            let (_, eq_oper) = lsyscache::get_compatible_hash_operators(in_oper)?
                .unwrap_or_else(|| {
                    panic!("could not find compatible hash operator for operator {in_oper}")
                });
            grp_operators.push(eq_oper);
        }

        let mut plan = Node::build::<Agg>(mcx)?;
        plan.plan.targetlist = build_path_tlist(run, target_id, path_id)?;
        plan.plan.qual = NodeList::nil();
        plan.plan.lefttree = Some(subplan);
        plan.aggstrategy = types_pathnodes::AGG_HASHED;
        plan.aggsplit = types_pathnodes::AGGSPLIT_SIMPLE;
        plan.numCols = num_cols as i32;
        plan.grpColIdx = mcx::vec_borrow_in(mcx, grp_col_idx)?;
        plan.grpOperators = mcx::vec_borrow_in(mcx, grp_operators)?;
        plan.grpCollations = mcx::vec_borrow_in(mcx, grp_collations)?;
        plan.numGroups = clamp_cardinality_to_long(rows);
        plan.transitionSpace = 0;
        copy_generic_path_info(run, &mut plan.plan, path_id);
        return Ok(plan.seal());
    }

    debug_assert!(umethod == types_pathnodes::UNIQUE_PATH_SORT);
    let mut sort_operators: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut uniq_operators: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    for (pos, &in_oper) in in_operators.iter().enumerate() {
        let sortop = lsyscache::amop::get_ordering_op_for_equality_op(in_oper, false)?;
        assert!(
            sortop != 0,
            "could not find ordering operator for equality operator {in_oper}"
        );
        let eqop = lsyscache::amop::get_equality_op_for_ordering_op(sortop)?
            .map(|(op, _)| op)
            .unwrap_or(0);
        assert!(
            eqop != 0,
            "could not find equality operator for ordering operator {sortop}"
        );
        let tle_node = subplan_tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").resno == grp_col_idx[pos])
            .expect("grouping column is in the subplan tlist");
        crate::prepunion::assign_sort_group_ref(tle_node, &subplan_tlist);
        sort_operators.push(sortop);
        uniq_operators.push(eqop);
        nulls_first.push(false);
    }

    // make_sort_from_sortclauses (createplan.c).
    let mut sort = Node::build::<types_nodes::plannodes::Sort>(mcx)?;
    sort.plan.targetlist = NodeList::from_slice(
        mcx,
        subplan.as_plan().expect("plan node").targetlist.as_slice(),
    )?;
    sort.plan.disabled_nodes = subplan.as_plan().unwrap().disabled_nodes
        + if crate::gucs::enable_sort() { 0 } else { 1 };
    sort.plan.qual = NodeList::nil();
    sort.plan.lefttree = Some(subplan);
    sort.numCols = num_cols as i32;
    sort.sortColIdx = mcx::slice_borrow_in(mcx, &grp_col_idx)?;
    sort.sortOperators = mcx::slice_borrow_in(mcx, &sort_operators)?;
    sort.collations = mcx::slice_borrow_in(mcx, &grp_collations)?;
    sort.nullsFirst = mcx::slice_borrow_in(mcx, &nulls_first)?;
    let sort_plan = sort.seal();
    label_sort_with_costsize(run, sort_plan, -1.0);

    // make_unique_from_sortclauses (createplan.c).
    let mut plan = Node::build::<types_nodes::plannodes::Unique>(mcx)?;
    plan.plan.targetlist = NodeList::from_slice(
        mcx,
        sort_plan.as_plan().expect("plan node").targetlist.as_slice(),
    )?;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(sort_plan);
    plan.numCols = num_cols as i32;
    plan.uniqColIdx = mcx::slice_borrow_in(mcx, &grp_col_idx)?;
    plan.uniqOperators = mcx::slice_borrow_in(mcx, &uniq_operators)?;
    plan.uniqCollations = mcx::slice_borrow_in(mcx, &grp_collations)?;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_minmaxagg_plan (createplan.c/planagg.c): one InitPlan per agg, then
// a Param-fed Result.
fn create_minmaxagg_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (mmaggregates, qual_ids, target_id) = match run.root.path(path_id) {
        PathNode::MinMaxAggPath(p) => (
            crate::relnode::pgvec_clone_shallow(mcx, &p.mmaggregates),
            crate::relnode::pgvec_clone_shallow(mcx, &p.quals),
            p.path.pathtarget_id.unwrap(),
        ),
        _ => unreachable!(),
    };

    for mminfo in mmaggregates.iter() {
        let idx = mminfo.subroot_idx.expect("minmax agg has a subroot");
        let sub_path = mminfo.subroot_path.expect("minmax agg has a path");
        let sub_state = run.minmax_subroots[idx].take().expect("minmax subroot taken once");
        let saved_root = core::mem::replace(&mut run.root, sub_state.root);
        let saved_tlist =
            core::mem::replace(&mut run.processed_tlist, sub_state.processed_tlist);

        // create_plan, not create_plan_recurse: a different planner context.
        let subplan = create_plan(run, sub_path)?;
        let subparse = run.parse();

        let mut lim = Node::build::<types_nodes::plannodes::Limit>(mcx)?;
        lim.plan.targetlist = NodeList::from_slice(
            mcx,
            subplan.as_plan().expect("plan node").targetlist.as_slice(),
        )?;
        lim.plan.qual = NodeList::nil();
        lim.plan.lefttree = Some(subplan);
        lim.limitOffset = subparse.limitOffset;
        lim.limitCount = subparse.limitCount;
        lim.limitOption = types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT;
        {
            let p = run.root.path(sub_path).base();
            let width = p.pathtarget_id.map_or(0, |pt| run.root.pathtarget(pt).width);
            lim.plan.disabled_nodes = p.disabled_nodes;
            lim.plan.startup_cost = p.startup_cost;
            lim.plan.total_cost = mminfo.pathcost;
            lim.plan.plan_rows = 1.0;
            lim.plan.plan_width = width;
            lim.plan.parallel_aware = false;
            lim.plan.parallel_safe = p.parallel_safe;
        }
        let limit_plan = lim.seal();

        let sub_root = core::mem::replace(&mut run.root, saved_root);
        let sub_tlist = core::mem::replace(&mut run.processed_tlist, saved_tlist);
        crate::subselect::ss_make_initplan_from_plan(
            run,
            crate::run::SubrootState { root: sub_root, processed_tlist: sub_tlist },
            limit_plan,
            mminfo.param,
        )?;
    }

    let tlist = build_path_tlist(run, target_id, path_id)?;
    let qual_list = order_bare_qual_clauses(run, &qual_ids)?;

    let mut plan = Node::build::<ResultPlan>(mcx)?;
    plan.plan.targetlist = tlist;
    if !qual_list.is_nil() {
        plan.resconstantqual = Some(Node::mk_list(mcx, qual_list)?);
    }
    copy_generic_path_info(run, &mut plan.plan, path_id);

    // setrefs swaps the residual Aggrefs for the InitPlan output Params.
    debug_assert!(run.root.minmax_aggs.is_empty());
    for mm in mmaggregates.iter() {
        let id = run.root.alloc_minmax_agg_info(*mm);
        run.root.minmax_aggs.push(id);
    }
    Ok(plan.seal())
}
fn sortgroupref_tle<'mcx>(sgref: u32, tlist: &NodeList<'mcx>) -> &'mcx TargetEntry<'mcx> {
    tlist
        .iter()
        .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sgref)
        .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"))
        .as_target_entry()
        .unwrap()
}

// remap_groupColIdx (createplan.c).
fn remap_group_col_idx<'mcx>(
    run: &PlannerRun<'mcx>,
    group_clause: &[types_pathnodes::NodeId],
) -> mcx::PgVec<'mcx, i16> {
    debug_assert!(!run.root.grouping_map.is_empty());
    let mut idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(run.mcx);
    for &gc_id in group_clause {
        let gc = run.root.expr_node(gc_id).as_sort_group_clause().expect("group clause cell");
        idx.push(run.root.grouping_map[gc.tleSortGroupRef as usize]);
    }
    idx
}

// make_sort_from_groupcols (createplan.c): keys located by grpColIdx resno,
// not ressortgroupref; only ordering info comes from the clauses.
fn make_sort_from_groupcols<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_clause: &[types_pathnodes::NodeId],
    grp_col_idx: &[i16],
    lefttree: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let sub_tlist = &lefttree.as_plan().expect("plan node").targetlist;
    let mut sort_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut sort_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    for (i, &gc_id) in group_clause.iter().enumerate() {
        let gc = run.root.expr_node(gc_id).as_sort_group_clause().expect("group clause cell");
        let tle = sub_tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").resno == grp_col_idx[i])
            .unwrap_or_else(|| panic!("could not retrieve tle for sort-from-groupcols"))
            .as_target_entry()
            .unwrap();
        sort_col_idx.push(tle.resno);
        sort_operators.push(gc.sortop);
        collations.push(expr_collation(tle.expr));
        nulls_first.push(gc.nulls_first);
    }
    let mut plan = Node::build::<types_nodes::plannodes::Sort>(mcx)?;
    plan.plan.targetlist =
        NodeList::from_slice(mcx, lefttree.as_plan().unwrap().targetlist.as_slice())?;
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

// create_groupingsets_plan (createplan.c): a top Agg for the first rollup
// plus vestigial chain Aggs (each with a stripped Sort) for the rest;
// grouping_map is stashed on the root for setrefs' GroupingFunc fixing.
fn create_groupingsets_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    debug_assert!(!run.parse().groupingSets.is_nil());
    let (subpath_id, target_id, aggstrategy, transition_space, qual_ids, rollups) =
        match run.root.path(path_id) {
            PathNode::GroupingSetsPath(p) => (
                p.subpath.expect("GroupingSetsPath has a subpath"),
                p.path.pathtarget_id.unwrap(),
                p.aggstrategy,
                p.transitionSpace,
                crate::relnode::pgvec_clone_shallow(mcx, &p.qual),
                p.rollups.clone(),
            ),
            _ => unreachable!(),
        };
    debug_assert!(!rollups.is_empty());

    let subplan = create_plan_recurse(run, subpath_id, CP_LABEL_TLIST)?;
    let subplan_tlist =
        NodeList::from_slice(mcx, subplan.as_plan().expect("plan node").targetlist.as_slice())?;

    let mut maxref: u32 = 0;
    for &gc_id in run.root.processed_groupClause.iter() {
        let gc = run.root.expr_node(gc_id).as_sort_group_clause().expect("group clause cell");
        maxref = maxref.max(gc.tleSortGroupRef);
    }
    let mut grouping_map: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    grouping_map.resize(maxref as usize + 1, 0);
    for i in 0..run.root.processed_groupClause.len() {
        let gc_id = run.root.processed_groupClause[i];
        let sgref = run
            .root
            .expr_node(gc_id)
            .as_sort_group_clause()
            .expect("group clause cell")
            .tleSortGroupRef;
        grouping_map[sgref as usize] = sortgroupref_tle(sgref, &subplan_tlist).resno;
    }
    debug_assert!(run.root.grouping_map.is_empty());
    run.root.grouping_map = grouping_map;

    let gsets_node_list = |gsets: &[mcx::PgVec<'mcx, i32>]| -> PgResult<NodeList<'mcx>> {
        let mut out = NodeList::nil();
        for set in gsets {
            let mut il = types_nodes::list::IntList::nil();
            for &x in set.iter() {
                il.lappend(mcx, x)?;
            }
            out.lappend(mcx, Node::mk_int_list(mcx, il)?)?;
        }
        Ok(out)
    };
    let grouping_arrays = |run: &PlannerRun<'mcx>,
                           group_clause: &[types_pathnodes::NodeId]|
     -> PgResult<(&'mcx [u32], &'mcx [u32])> {
        let mut ops: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
        let mut colls: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
        for &gc_id in group_clause {
            let gc =
                run.root.expr_node(gc_id).as_sort_group_clause().expect("group clause cell");
            ops.push(gc.eqop);
            colls.push(expr_collation(sortgroupref_tle(gc.tleSortGroupRef, &subplan_tlist).expr));
        }
        Ok((mcx::slice_borrow_in(mcx, &ops)?, mcx::slice_borrow_in(mcx, &colls)?))
    };

    let mut chain = NodeList::nil();
    if rollups.len() > 1 {
        let mut is_first_sort = rollups[0].is_hashed;
        for rollup in rollups[1..].iter() {
            let new_grp_col_idx = remap_group_col_idx(run, &rollup.groupClause);
            let sort_plan = if !rollup.is_hashed && !is_first_sort {
                Some(make_sort_from_groupcols(
                    run,
                    &rollup.groupClause,
                    &new_grp_col_idx,
                    subplan,
                )?)
            } else {
                None
            };
            if !rollup.is_hashed {
                is_first_sort = false;
            }
            let strat = if rollup.is_hashed {
                types_pathnodes::AGG_HASHED
            } else if rollup.gsets[0].is_empty() {
                types_pathnodes::AGG_PLAIN
            } else {
                types_pathnodes::AGG_SORTED
            };
            let (ops, colls) = grouping_arrays(run, &rollup.groupClause)?;
            let mut agg = Node::build::<Agg>(mcx)?;
            agg.plan.targetlist = NodeList::nil();
            agg.plan.qual = NodeList::nil();
            agg.plan.lefttree = sort_plan;
            agg.aggstrategy = strat;
            agg.aggsplit = types_pathnodes::AGGSPLIT_SIMPLE;
            agg.numCols = rollup.gsets[0].len() as i32;
            agg.grpColIdx = mcx::vec_borrow_in(mcx, new_grp_col_idx)?;
            agg.grpOperators = ops;
            agg.grpCollations = colls;
            agg.groupingSets = gsets_node_list(&rollup.gsets)?;
            agg.numGroups = clamp_cardinality_to_long(rollup.numGroups);
            agg.transitionSpace = transition_space;
            // C strips the vestigial Sort after make_agg.
            // SAFETY: sort_plan was freshly built above; no other handle.
            if let Some(sp) = sort_plan {
                unsafe {
                    sp.with_plan_mut(|p| {
                        p.targetlist = NodeList::nil();
                        p.lefttree = None;
                    })
                }
                .expect("Sort embeds a Plan base");
            }
            chain.lappend(mcx, agg.seal())?;
        }
    }

    let rollup = &rollups[0];
    let top_grp_col_idx = remap_group_col_idx(run, &rollup.groupClause);
    let (ops, colls) = grouping_arrays(run, &rollup.groupClause)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;
    let mut qual = NodeList::nil();
    for &q in qual_ids.iter() {
        qual.lappend(mcx, *run.root.expr_node(q))?;
    }
    let mut plan = Node::build::<Agg>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = qual;
    plan.plan.lefttree = Some(subplan);
    plan.aggstrategy = aggstrategy;
    plan.aggsplit = types_pathnodes::AGGSPLIT_SIMPLE;
    plan.numCols = rollup.gsets[0].len() as i32;
    plan.grpColIdx = mcx::vec_borrow_in(mcx, top_grp_col_idx)?;
    plan.grpOperators = ops;
    plan.grpCollations = colls;
    plan.groupingSets = gsets_node_list(&rollup.gsets)?;
    plan.chain = chain;
    plan.numGroups = clamp_cardinality_to_long(rollup.numGroups);
    plan.transitionSpace = transition_space;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}


// create_windowagg_plan (createplan.c).
fn create_windowagg_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let (subpath_id, target_id, winclause_id, topwindow, rc_ids, qual_ids) =
        match run.root.path(path_id) {
            PathNode::WindowAggPath(wp) => (
                wp.subpath.expect("WindowAggPath has a subpath"),
                wp.path.pathtarget_id.unwrap(),
                wp.winclause,
                wp.topwindow,
                crate::relnode::pgvec_clone_shallow(run.mcx, &wp.runCondition),
                crate::relnode::pgvec_clone_shallow(run.mcx, &wp.qual),
            ),
            _ => unreachable!(),
        };
    let wc_node = *run.root.expr_node(winclause_id);
    let mut run_condition = NodeList::nil();
    for &id in rc_ids.iter() {
        run_condition.lappend(run.mcx, *run.root.expr_node(id))?;
    }
    let mut qual = NodeList::nil();
    for &id in qual_ids.iter() {
        qual.lappend(run.mcx, *run.root.expr_node(id))?;
    }

    // WindowAgg spools its input into a tuplestore: request a small tlist,
    // with grouping columns labeled.
    let subplan = create_plan_recurse(run, subpath_id, CP_LABEL_TLIST | CP_SMALL_TLIST)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;

    let wc = wc_node.as_window_clause().expect("WindowClause");
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
    plan.startOffset = wc.startOffset;
    plan.endOffset = wc.endOffset;
    plan.startInRangeFunc = wc.startInRangeFunc;
    plan.endInRangeFunc = wc.endInRangeFunc;
    plan.inRangeColl = wc.inRangeColl;
    plan.inRangeAsc = wc.inRangeAsc;
    plan.inRangeNullsFirst = wc.inRangeNullsFirst;
    plan.runCondition = run_condition.clone_in(run.mcx)?;
    plan.runConditionOrig = run_condition;
    plan.plan.qual = qual;
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
        let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), clause)?;
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
        let mut found: Option<(i16, u32)> = None;
        if run.root.ec(ec).ec_has_volatile {
            // A volatile EC came from an ORDER BY clause: match that same
            // targetlist entry via its sortref (get_sortgroupref_tle).
            let sortref = run.root.ec(ec).ec_sortref;
            assert!(sortref != 0, "volatile EquivalenceClass has no sortref");
            let tle = tlist
                .iter()
                .map(|n| n.as_target_entry().expect("TargetEntry"))
                .find(|tle| tle.ressortgroupref == sortref)
                .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
            debug_assert_eq!(run.root.ec(ec).ec_members.len(), 1);
            let em_id = run.root.ec(ec).ec_members[0];
            found = Some((tle.resno, run.root.em(em_id).em_datatype));
        } else {
            for tle_node in &tlist {
                let tle = tle_node.as_target_entry().expect("TargetEntry");
                if let Some(em_id) = find_ec_member_matching_expr(run, ec, tle.expr, &types_pathnodes::relids::RELIDS_UNSET) {
                    found = Some((tle.resno, run.root.em(em_id).em_datatype));
                    break;
                }
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
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescecollid,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxcollid,
        _ => nodes_core::expr_collation(node),
    }
}

fn clamp_cardinality_to_long(x: f64) -> i64 {
    if x < i64::MAX as f64 {
        x as i64
    } else {
        i64::MAX
    }
}

// build_path_tlist (createplan.c).
fn build_path_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    target_id: PtId,
    path_id: PathId,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let has_param = run.root.path(path_id).base().param_info.is_some();
    let n = run.root.pathtarget(target_id).exprs.len();
    let mut tlist = NodeList::nil();
    for i in 0..n {
        let target = run.root.pathtarget(target_id);
        let expr = *run.root.expr_node(target.exprs[i]);
        let ressortgroupref = target.sortgrouprefs.get(i).copied().unwrap_or(0);
        // Parameterized path: lateral references become nestloop Params.
        let expr = if has_param { replace_nestloop_params(run, expr)? } else { expr };
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
pub(crate) fn apply_tlist_labeling<'mcx>(plan: Node<'mcx>, src_tlist: &NodeList<'mcx>) {
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

// IS_OTHER_REL(best_path->subpath->parent) ? best_path->path.parent->relids
// : NULL (create_sort_plan / create_incrementalsort_plan, createplan.c).
fn sort_relids_for_child_ec<'mcx>(
    run: &PlannerRun<'mcx>,
    path_id: PathId,
    subpath_id: PathId,
) -> types_pathnodes::Relids<'mcx> {
    let subparent = run.root.path(subpath_id).base().parent;
    match run.root.rel(subparent).reloptkind {
        types_pathnodes::RELOPT_OTHER_MEMBER_REL
        | types_pathnodes::RELOPT_OTHER_JOINREL
        | types_pathnodes::RELOPT_OTHER_UPPER_REL => {
            let parent = run.root.path(path_id).base().parent;
            types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(parent).relids)
        }
        _ => types_pathnodes::relids::relids_empty(),
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
    // Child sorts resolve pathkey EC members through the child rel's relids;
    // IS_OTHER_REL covers other-upper child grouping rels (reloptkind, not
    // top_parent_relids, which upper rels never set).
    let relids = sort_relids_for_child_ec(run, path_id, subpath_id);
    let plan = make_sort_from_pathkeys(run, subplan, &pathkeys, &relids)?;
    copy_generic_path_info_node(run, plan, path_id);
    Ok(plan)
}

// create_gather_plan (createplan.c).
fn create_gather_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, num_workers, single_copy, target_id) = {
        let PathNode::GatherPath(g) = run.root.path(path_id) else { unreachable!() };
        (
            g.subpath.expect("Gather subpath"),
            g.num_workers,
            g.single_copy,
            g.path.pathtarget_id.expect("Gather path has a pathtarget"),
        )
    };
    // Projection pushes down to the child: the work parallelizes, and no
    // system column can ride the tuple queue (MinimalTuple representation).
    let subplan = create_plan_recurse(run, subpath_id, CP_EXACT_TLIST)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;

    let mut plan = Node::build::<types_nodes::plannodes::Gather>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.num_workers = num_workers;
    plan.rescan_param = crate::cte::assign_special_exec_param(run)?;
    plan.single_copy = single_copy;
    plan.invisible = false;
    copy_generic_path_info(run, &mut plan.plan, path_id);

    run.glob.parallel_mode_needed = true;
    Ok(plan.seal())
}

// create_gather_merge_plan (createplan.c).
fn create_gather_merge_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, num_workers, target_id, pathkeys) = {
        let PathNode::GatherMergePath(g) = run.root.path(path_id) else { unreachable!() };
        (
            g.subpath.expect("GatherMerge subpath"),
            g.num_workers,
            g.path.pathtarget_id.expect("GatherMerge path has a pathtarget"),
            crate::relnode::pgvec_clone_shallow(run.mcx, &g.path.pathkeys),
        )
    };
    assert!(!pathkeys.is_empty());
    let tlist = build_path_tlist(run, target_id, path_id)?;
    // As with Gather, project away columns in the workers.
    let subplan = create_plan_recurse(run, subpath_id, CP_EXACT_TLIST)?;

    let mut plan = Node::build::<types_nodes::plannodes::GatherMerge>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.num_workers = num_workers;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    plan.rescan_param = crate::cte::assign_special_exec_param(run)?;

    let relids = {
        let subparent = run.root.path(subpath_id).base().parent;
        types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(subparent).relids)
    };
    let (subplan, cols) = {
        let (new_lt, cols) = prepare_sort_from_pathkeys(
            run,
            Some(subplan),
            &subplan.as_plan().expect("plan node").targetlist,
            &pathkeys,
            &relids,
            None,
            false,
        )?;
        (new_lt.expect("lefttree"), cols)
    };
    plan.numCols = cols.sort_col_idx.len() as i32;
    plan.sortColIdx = mcx::slice_borrow_in(mcx, &cols.sort_col_idx)?;
    plan.sortOperators = mcx::slice_borrow_in(mcx, &cols.sort_operators)?;
    plan.collations = mcx::slice_borrow_in(mcx, &cols.collations)?;
    plan.nullsFirst = mcx::slice_borrow_in(mcx, &cols.nulls_first)?;
    plan.plan.lefttree = Some(subplan);

    run.glob.parallel_mode_needed = true;
    Ok(plan.seal())
}

// find_ec_member_matching_expr (equivclass.c): child members match only when
// computable from relids.
pub(crate) fn find_ec_member_matching_expr<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: types_pathnodes::EcId,
    expr: Node<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
) -> Option<types_pathnodes::EmId> {
    use types_pathnodes::relids::{relids_is_subset, relids_members};
    let mut expr = expr;
    while let Some(r) = expr.as_relabel_type() {
        expr = r.arg;
    }
    let e = run.root.ec(ec);
    let mut candidates: mcx::PgVec<'mcx, types_pathnodes::EmId> = mcx::PgVec::new_in(run.mcx);
    candidates.extend(e.ec_members.iter().copied());
    if !e.ec_childmembers.is_empty() {
        for r in relids_members(relids) {
            if let Some(list) = e.ec_childmembers.get(r as usize) {
                candidates.extend(list.iter().copied());
            }
        }
    }
    for &em_id in candidates.iter() {
        let em = run.root.em(em_id);
        if em.em_is_const {
            continue;
        }
        if em.em_is_child && !relids_is_subset(&em.em_relids, relids) {
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

// find_computable_ec_member (equivclass.c) over a plan tlist; the allpaths
// variant is pathtarget-shaped. require_parallel_safe is always false here.
fn find_computable_ec_member_tlist<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: types_pathnodes::EcId,
    tlist: &NodeList<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Option<types_pathnodes::EmId>> {
    use types_pathnodes::relids::{relids_is_subset, relids_members};
    use vars::{PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_INCLUDE_WINDOWFUNCS};
    let mcx = run.mcx;
    let flags = PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS;

    let mut exprvars: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        for v in &vars::pull_var_clause(mcx, tle.expr, flags)? {
            exprvars.push(v);
        }
    }

    let candidates = {
        let e = run.root.ec(ec);
        let mut out: mcx::PgVec<'mcx, types_pathnodes::EmId> = mcx::PgVec::new_in(mcx);
        out.extend(e.ec_members.iter().copied());
        if !e.ec_childmembers.is_empty() {
            for r in relids_members(relids) {
                if let Some(list) = e.ec_childmembers.get(r as usize) {
                    out.extend(list.iter().copied());
                }
            }
        }
        out
    };
    'candidate: for &em_id in candidates.iter() {
        let em = run.root.em(em_id);
        if em.em_is_const {
            continue;
        }
        if em.em_is_child && !relids_is_subset(&em.em_relids, relids) {
            continue;
        }
        let em_expr = *run.root.expr_node(em.em_expr);
        for emv in &vars::pull_var_clause(mcx, em_expr, flags)? {
            if !exprvars.iter().any(|&x| types_nodes::equal(x, emv)) {
                continue 'candidate;
            }
        }
        return Ok(Some(em_id));
    }
    Ok(None)
}

// inject_projection_plan (createplan.c).
fn inject_projection_plan<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    subplan: Node<'mcx>,
    tlist: NodeList<'mcx>,
    parallel_safe: bool,
) -> PgResult<Node<'mcx>> {
    let sub = subplan.as_plan().expect("plan node");
    let mut result = Node::build::<ResultPlan>(mcx)?;
    result.plan.targetlist = tlist;
    result.plan.qual = NodeList::nil();
    result.plan.lefttree = Some(subplan);
    result.plan.disabled_nodes = sub.disabled_nodes;
    result.plan.startup_cost = sub.startup_cost;
    result.plan.total_cost = sub.total_cost;
    result.plan.plan_rows = sub.plan_rows;
    result.plan.plan_width = sub.plan_width;
    result.plan.parallel_aware = false;
    result.plan.parallel_safe = parallel_safe;
    Ok(result.seal())
}

// prepare_sort_from_pathkeys (createplan.c). req_col_idx pins each key to the
// given column, as when building MergeAppend children. lefttree is None for
// the Append/MergeAppend node-level calls (C passes the node itself with
// adjust_tlist_in_place=true); those callers read the adjusted tlist out of
// SortColumns. A pathkey with no tlist match gets a resjunk entry appended,
// injecting a Result when the lefttree can't project.
fn prepare_sort_from_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    lefttree: Option<Node<'mcx>>,
    input_tlist: &NodeList<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
    relids: &types_pathnodes::Relids<'mcx>,
    req_col_idx: Option<&[i16]>,
    adjust_tlist_in_place: bool,
) -> PgResult<(Option<Node<'mcx>>, SortColumns<'mcx>)> {
    let mcx = run.mcx;
    let mut lefttree = lefttree;
    let mut adjust_tlist_in_place = adjust_tlist_in_place;
    // C shares lefttree->targetlist by pointer; flat cell copy, shared nodes.
    let mut tlist = NodeList::from_slice(mcx, input_tlist.as_slice())?;
    let mut tlist_changed = false;
    let mut sort_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut sort_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);

    for pathkey in pathkeys {
        let ec = pathkey.pk_eclass.expect("canonical pathkey has an eclass");
        let mut found: Option<(i16, u32)> = None;
        if run.root.ec(ec).ec_has_volatile {
            // A volatile EC came from an ORDER BY clause: match that same
            // targetlist entry via its sortref (get_sortgroupref_tle).
            let sortref = run.root.ec(ec).ec_sortref;
            assert!(sortref != 0, "volatile EquivalenceClass has no sortref");
            let tle = tlist
                .iter()
                .map(|n| n.as_target_entry().expect("TargetEntry"))
                .find(|tle| tle.ressortgroupref == sortref)
                .unwrap_or_else(|| {
                    panic!("ORDER/GROUP BY expression not found in targetlist")
                });
            debug_assert_eq!(run.root.ec(ec).ec_members.len(), 1);
            let em_id = run.root.ec(ec).ec_members[0];
            found = Some((tle.resno, run.root.em(em_id).em_datatype));
        } else if let Some(req) = req_col_idx {
            let want = req[sort_col_idx.len()];
            for tle_node in &tlist {
                let tle = tle_node.as_target_entry().expect("TargetEntry");
                if tle.resno != want {
                    continue;
                }
                if let Some(em_id) = find_ec_member_matching_expr(run, ec, tle.expr, relids) {
                    found = Some((tle.resno, run.root.em(em_id).em_datatype));
                }
                break;
            }
        } else {
            for tle_node in &tlist {
                let tle = tle_node.as_target_entry().expect("TargetEntry");
                if let Some(em_id) = find_ec_member_matching_expr(run, ec, tle.expr, relids) {
                    found = Some((tle.resno, run.root.em(em_id).em_datatype));
                    break;
                }
            }
        }
        let (resno, pk_datatype) = match found {
            Some(f) => f,
            None => {
                let em_id = find_computable_ec_member_tlist(run, ec, &tlist, relids)?
                    .unwrap_or_else(|| panic!("could not find pathkey item to sort"));
                let pk_datatype = run.root.em(em_id).em_datatype;
                let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
                if !adjust_tlist_in_place {
                    let lt = lefttree.expect("resjunk injection requires a lefttree");
                    if !crate::pathnode::is_projection_capable_pathtype(lt.node_tag() as u16) {
                        let ps = lt.as_plan().expect("plan node").parallel_safe;
                        let copy = NodeList::from_slice(mcx, tlist.as_slice())?;
                        lefttree = Some(inject_projection_plan(mcx, lt, copy, ps)?);
                    }
                }
                adjust_tlist_in_place = true;
                let resno = tlist.len() as i16 + 1;
                // The TLE shares em_expr (C copyObject; planner arena nodes are shared).
                tlist.lappend(mcx, Node::mk_target_entry(mcx, em_expr, resno, None, true)?)?;
                tlist_changed = true;
                (resno, pk_datatype)
            }
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
    if tlist_changed {
        if let Some(lt) = lefttree {
            let newt = NodeList::from_slice(mcx, tlist.as_slice())?;
            // SAFETY: lt was freshly built by create_plan_recurse (or is the
            // Result injected above); no reference derived from its tlist is
            // live across this write.
            unsafe { lt.with_plan_mut(|p| p.targetlist = newt) }.expect("plan node");
        }
    }
    Ok((lefttree, SortColumns { tlist, sort_col_idx, sort_operators, collations, nulls_first }))
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
    relids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Node<'mcx>> {
    let (lefttree, cols) = {
        let (new_lt, cols) = prepare_sort_from_pathkeys(
            run,
            Some(lefttree),
            &lefttree.as_plan().expect("plan node").targetlist,
            pathkeys,
            relids,
            None,
            false,
        )?;
        (new_lt.expect("lefttree"), cols)
    };
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
    relids: &types_pathnodes::Relids<'mcx>,
    n_presorted_cols: i32,
) -> PgResult<Node<'mcx>> {
    let (lefttree, cols) = {
        let (new_lt, cols) = prepare_sort_from_pathkeys(
            run,
            Some(lefttree),
            &lefttree.as_plan().expect("plan node").targetlist,
            pathkeys,
            relids,
            None,
            false,
        )?;
        (new_lt.expect("lefttree"), cols)
    };
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
    let relids = sort_relids_for_child_ec(run, path_id, subpath_id);
    let plan = make_incrementalsort_from_pathkeys(run, subplan, &pathkeys, &relids, n_presorted)?;
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

// create_limit_plan + make_limit (createplan.c): WITH TIES pulls its tie
// columns from parse->sortClause resolved against parse->targetList.
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
    // Limit doesn't project, so tlist requirements pass through.
    let subplan = create_plan_recurse(run, subpath_id, flags)?;

    let with_ties =
        limit_option == types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_WITH_TIES as u32;
    let (uniq_num_cols, uniq_col_idx, uniq_operators, uniq_collations) = if with_ties {
        let parse = run.parse();
        let mut idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
        let mut ops: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
        let mut colls: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
        for sgc_node in &parse.sortClause {
            let sgc = sgc_node.as_sort_group_clause().expect("SortGroupClause");
            let tle = parse
                .targetList
                .iter()
                .map(|n| n.as_target_entry().expect("tlist cell"))
                .find(|tle| tle.ressortgroupref == sgc.tleSortGroupRef)
                .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
            idx.push(tle.resno);
            ops.push(sgc.eqop);
            colls.push(expr_collation(tle.expr));
        }
        (
            idx.len() as i32,
            mcx::vec_borrow_in(mcx, idx)?,
            mcx::vec_borrow_in(mcx, ops)?,
            mcx::vec_borrow_in(mcx, colls)?,
        )
    } else {
        (0, &[][..], &[][..], &[][..])
    };

    let mut plan = Node::build::<types_nodes::plannodes::Limit>(mcx)?;
    plan.plan.targetlist =
        NodeList::from_slice(mcx, subplan.as_plan().expect("plan node").targetlist.as_slice())?;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.limitOffset = limit_offset;
    plan.limitCount = limit_count;
    plan.limitOption = if with_ties {
        types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_WITH_TIES
    } else {
        types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT
    };
    plan.uniqNumCols = uniq_num_cols;
    plan.uniqColIdx = uniq_col_idx;
    plan.uniqOperators = uniq_operators;
    plan.uniqCollations = uniq_collations;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_lockrows_plan + make_lockrows (createplan.c); PlanRowMark nodes
// materialize from the run store (C shares root->rowMarks pointers).
fn create_lockrows_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath_id, marks, epq_param) = {
        let PathNode::LockRowsPath(lp) = run.root.path(path_id) else { unreachable!() };
        (
            lp.subpath.expect("LockRowsPath has a subpath"),
            crate::relnode::pgvec_clone_shallow(mcx, &lp.rowMarks),
            lp.epqParam,
        )
    };
    // LockRows doesn't project, so tlist requirements pass through.
    let subplan = create_plan_recurse(run, subpath_id, flags)?;

    let mut plan = Node::build::<types_nodes::plannodes::LockRows>(mcx)?;
    plan.plan.targetlist =
        NodeList::from_slice(mcx, subplan.as_plan().expect("plan node").targetlist.as_slice())?;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    for &id in marks.iter() {
        plan.rowMarks.lappend(mcx, Node::mk(mcx, *run.rowmark(id))?)?;
    }
    plan.epqParam = epq_param;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

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

// create_memoize_plan + make_memoize (createplan.c).
fn create_memoize_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (subpath, hash_ops, param_expr_ids, singlerow, binary_mode, est_entries) =
        match run.root.path(path_id) {
            PathNode::MemoizePath(mp) => (
                mp.subpath.expect("Memoize subpath"),
                crate::relnode::pgvec_clone_shallow(mcx, &mp.hash_operators),
                crate::relnode::pgvec_clone_shallow(mcx, &mp.param_exprs),
                mp.singlerow,
                mp.binary_mode,
                mp.est_entries,
            ),
            other => {
                panic!("create_memoize_plan (createplan.c): pathtype {}", other.base().pathtype)
            }
        };
    let subplan = create_plan_recurse(run, subpath, flags | CP_SMALL_TLIST)?;

    let mut param_exprs = NodeList::nil();
    for &e in param_expr_ids.iter() {
        let node = *run.root.expr_node(e);
        param_exprs.lappend(mcx, replace_nestloop_params(run, node)?)?;
    }
    let nkeys = param_exprs.len();
    debug_assert!(nkeys > 0 && hash_ops.len() == nkeys);

    let mut collations: mcx::PgVec<'mcx, types_core::Oid> = mcx::vec_with_capacity_in(mcx, nkeys)?;
    for e in &param_exprs {
        collations.push(crate::pathkeys::expr_collation(e));
    }
    let mut keyparamids = types_nodes::bitmapset::Bitmapset::empty();
    for e in &param_exprs {
        pull_paramids(run.mcx, e, &mut keyparamids)?;
    }

    let mut tlist = NodeList::nil();
    for te in subplan.as_plan().expect("subplan").targetlist.iter() {
        tlist.lappend(mcx, te)?;
    }
    let mut plan = Node::build::<types_nodes::plannodes::Memoize>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.plan.righttree = None;
    plan.numKeys = nkeys as i32;
    plan.hashOperators = mcx::slice_borrow_in(mcx, &hash_ops)?;
    plan.collations = mcx::slice_borrow_in(mcx, &collations)?;
    plan.param_exprs = param_exprs;
    plan.singlerow = singlerow;
    plan.binary_mode = binary_mode;
    plan.est_entries = est_entries;
    plan.keyparamids = keyparamids;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// pull_paramids (createplan.c) over the replaced (plan-side) exprs.
fn pull_paramids<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    out: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    if let Some(p) = node.as_param() {
        out.add_member(mcx, p.paramid)?;
        return Ok(());
    }
    clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
        pull_paramids(mcx, n, out)?;
        Ok(None)
    })?;
    Ok(())
}

// create_join_plan (createplan.c): build the join node, then gate any
// pseudoconstant joinrestrictinfo clauses with a one-time-filter Result.
fn create_join_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let plan = match run.root.path(path_id) {
        PathNode::NestPath(_) => create_nestloop_plan(run, path_id)?,
        PathNode::MergePath(_) => create_mergejoin_plan(run, path_id)?,
        PathNode::HashPath(_) => create_hashjoin_plan(run, path_id)?,
        other => panic!(
            "create_join_plan (createplan.c): pathtype {}",
            other.base().pathtype
        ),
    };
    let restrict = match run.root.path(path_id) {
        PathNode::NestPath(np) => {
            crate::relnode::pgvec_clone_shallow(run.mcx, &np.jpath.joinrestrictinfo)
        }
        PathNode::MergePath(mp) => {
            crate::relnode::pgvec_clone_shallow(run.mcx, &mp.jpath.joinrestrictinfo)
        }
        PathNode::HashPath(hp) => {
            crate::relnode::pgvec_clone_shallow(run.mcx, &hp.jpath.joinrestrictinfo)
        }
        _ => unreachable!(),
    };
    let gating_clauses = get_gating_quals(run, &restrict)?;
    if !gating_clauses.is_nil() {
        return create_gating_plan(run, path_id, plan, gating_clauses);
    }
    Ok(plan)
}

fn create_nestloop_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (outer_path, inner_path, jointype, inner_unique, restrict, target_id, has_param) =
        match run.root.path(path_id) {
            PathNode::NestPath(np) => (
                np.jpath.outerjoinpath.expect("nestloop outer path"),
                np.jpath.innerjoinpath.expect("nestloop inner path"),
                np.jpath.jointype,
                np.jpath.inner_unique,
                crate::relnode::pgvec_clone_shallow(mcx, &np.jpath.joinrestrictinfo),
                np.jpath.path.pathtarget_id.unwrap(),
                np.jpath.path.param_info.is_some(),
            ),
            other => panic!(
                "create_nestloop_plan (createplan.c): pathtype {}",
                other.base().pathtype
            ),
        };

    // An inner parameterized by the topmost parent of the outer rel gets its
    // parameterization translated to the child now (partitionwise join).
    {
        let outer_parent = run.root.path(outer_path).base().parent;
        if crate::joinpath::path_param_by_parent(run, inner_path, outer_parent) {
            reparameterize_path_by_child(run, inner_path, outer_parent)?
                .expect("could not reparameterize a subpath");
        }
    }

    let tlist = build_path_tlist(run, target_id, path_id)?;
    // NestLoop can project, so no need to be picky about child tlists.
    let outer_plan = create_plan_recurse(run, outer_path, 0)?;

    // The inner side sees the outer rels as nestloop-param sources.
    let save_outer_rels =
        crate::relnode::relids_copy(mcx, &run.root.curOuterRels);
    let outerrelids = {
        let r = run.root.path(outer_path).base().parent;
        crate::relnode::relids_copy(mcx, &run.root.rel(r).relids)
    };
    run.root.curOuterRels =
        crate::relnode::relids_union(mcx, &run.root.curOuterRels, &outerrelids);

    let inner_plan = create_plan_recurse(run, inner_path, 0)?;

    run.root.curOuterRels = save_outer_rels;

    let ordered = order_qual_clauses(run, &restrict)?;
    let (mut joinclauses, mut otherclauses) = if crate::joinpath::is_outer_join(jointype) {
        let joinrelids = crate::relnode::relids_copy(
            mcx,
            &run.root.rel(run.root.path(path_id).base().parent).relids,
        );
        extract_actual_join_clauses(run, &ordered, &joinrelids)
    } else {
        (extract_actual_clauses(run, &ordered), NodeList::nil())
    };

    if has_param {
        joinclauses = replace_nestloop_params_list(run, &joinclauses)?;
        otherclauses = replace_nestloop_params_list(run, &otherclauses)?;
    }

    let req_outer =
        crate::relnode::relids_copy(mcx, crate::pathnode::path_req_outer(run.root.path(path_id).base()));
    let nest_params =
        crate::paramassign::identify_current_nestloop_params(run, &outerrelids, &req_outer)?;

    // PHV nestloop params may be missing from the outer plan's tlist (the
    // executor's NestLoopParam machinery needs simple outer-Var references);
    // append them as resjunk entries. Surviving curOuterParams references
    // inside such a PHV become Params now.
    let mut outer_plan = outer_plan;
    {
        let mut new_tlist: Option<NodeList<'mcx>> = None;
        let mut outer_parallel_safe = outer_plan.as_plan().expect("plan node").parallel_safe;
        for nlp_node in &nest_params {
            let nlp = nlp_node.as_nest_loop_param().expect("nestParams cell");
            if nlp.paramval.as_var().is_some() {
                continue;
            }
            let phv_node = nlp.paramval;
            let phv = phv_node.as_place_holder_var().expect("non-Var nestParam is a PHV");
            let already = match &new_tlist {
                Some(tl) => tl.iter(),
                None => outer_plan.as_plan().unwrap().targetlist.iter(),
            }
            .any(|n| types_nodes::equal(n.as_target_entry().expect("tlist cell").expr, phv_node));
            if already {
                continue;
            }
            // Safe after the membership check: equal() ignores phexpr.
            let new_phexpr = replace_nestloop_params(run, phv.phexpr)?;
            // SAFETY: exclusive plan-tree ownership (C rewrites in place).
            unsafe {
                phv_node.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|p| {
                    p.phexpr = new_phexpr;
                })
            }
            .expect("PlaceHolderVar");
            let mut tl = match new_tlist.take() {
                Some(tl) => tl,
                None => NodeList::from_slice(
                    mcx,
                    outer_plan.as_plan().unwrap().targetlist.as_slice(),
                )?,
            };
            let resno = tl.len() as i16 + 1;
            let copy = rewrite_manip::copy_node(mcx, phv_node)?;
            tl.lappend(mcx, Node::mk_target_entry(mcx, copy, resno, None, true)?)?;
            new_tlist = Some(tl);
            if outer_parallel_safe {
                outer_parallel_safe = clauses::is_parallel_safe_opt(run, Some(phv_node))?;
            }
        }
        if let Some(tl) = new_tlist {
            outer_plan = change_plan_targetlist(mcx, outer_plan, tl, outer_parallel_safe)?;
        }
    }

    let mut plan = Node::build::<types_nodes::plannodes::NestLoop>(mcx)?;
    plan.join.plan.targetlist = tlist;
    plan.join.plan.qual = otherclauses;
    plan.join.plan.lefttree = Some(outer_plan);
    plan.join.plan.righttree = Some(inner_plan);
    plan.join.jointype = jointype_enum(jointype);
    plan.join.inner_unique = inner_unique;
    plan.join.joinqual = joinclauses;
    plan.nestParams = nest_params;
    copy_generic_path_info(run, &mut plan.join.plan, path_id);
    Ok(plan.seal())
}

// change_plan_targetlist (createplan.c): a non-projecting plan node whose
// tlist must change gets a Result on top.
fn change_plan_targetlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    subplan: Node<'mcx>,
    tlist: NodeList<'mcx>,
    tlist_parallel_safe: bool,
) -> PgResult<Node<'mcx>> {
    if !is_projection_capable_pathtype(subplan.node_tag() as u16)
        && !tlist_same_exprs(&tlist, &subplan.as_plan().expect("plan node").targetlist)
    {
        let sub = subplan.as_plan().expect("plan node");
        let mut result = Node::build::<ResultPlan>(mcx)?;
        result.plan.targetlist = tlist;
        result.plan.qual = NodeList::nil();
        result.plan.lefttree = Some(subplan);
        result.plan.disabled_nodes = sub.disabled_nodes;
        result.plan.startup_cost = sub.startup_cost;
        result.plan.total_cost = sub.total_cost;
        result.plan.plan_rows = sub.plan_rows;
        result.plan.plan_width = sub.plan_width;
        result.plan.parallel_aware = false;
        result.plan.parallel_safe = sub.parallel_safe && tlist_parallel_safe;
        Ok(result.seal())
    } else {
        // SAFETY: freshly built subplan; exclusive plan-tree ownership.
        unsafe {
            subplan.with_plan_mut(|p| {
                p.targetlist = tlist;
                p.parallel_safe = p.parallel_safe && tlist_parallel_safe;
            })
        }
        .expect("plan node");
        Ok(subplan)
    }
}

// create_hashjoin_plan (createplan.c), JOIN_INNER arm. Skew fields default to
// invalid (the executor's skew fast path is loud, so they are never consumed);
// otherclauses is NIL for inner joins.
fn create_hashjoin_plan<'mcx>(run: &mut PlannerRun<'mcx>, path_id: PathId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (has_param, outer_path, inner_path, jointype, inner_unique, restrict, hash_rinfos, target_id, num_batches) =
        match run.root.path(path_id) {
            PathNode::HashPath(hp) => {
                (
                    hp.jpath.path.param_info.is_some(),
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
    let tlist = build_path_tlist(run, target_id, path_id)?;
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

    // Parameterized path: outer-relation Vars become nestloop Params (there
    // are none in the hashclauses).
    let (joinclauses, otherclauses) = if has_param {
        (
            replace_nestloop_params_list(run, &joinclauses)?,
            replace_nestloop_params_list(run, &otherclauses)?,
        )
    } else {
        (joinclauses, otherclauses)
    };

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
    // Parallel-aware: the executor sizes the shared table from the total rows
    // expected across all participants.
    if run.root.path(path_id).base().parallel_aware {
        hash_plan.plan.parallel_aware = true;
        hash_plan.rows_total = match run.root.path(path_id) {
            PathNode::HashPath(hp) => hp.inner_rows_total,
            _ => unreachable!(),
        };
    }
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
// list_difference (list.c) membership is equal()-based: a child-translated
// restrictlist and hashclause list carry content-equal but distinct nodes
// (reparameterize_path_by_child adjusts them separately), and the hash/merge
// clauses must still be removed from the joinquals.
fn list_difference<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &NodeList<'mcx>,
    b: &NodeList<'mcx>,
) -> NodeList<'mcx> {
    let mut out = NodeList::nil();
    for n in a.iter() {
        if !b.iter().any(|m| types_nodes::equal(n, m)) {
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
// otherclauses is NIL; replace_nestloop_params is dead (param_info always
// None).
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
        has_param,
    ) = match run.root.path(path_id) {
        PathNode::MergePath(mp) => {
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
                mp.jpath.path.param_info.is_some(),
            )
        }
        other => panic!(
            "create_mergejoin_plan (createplan.c): pathtype {}",
            other.base().pathtype
        ),
    };

    let tlist = build_path_tlist(run, target_id, path_id)?;
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

    // Parameterized path: outer-relation Vars become nestloop Params (there
    // are none in the mergeclauses).
    let (joinclauses, otherclauses) = if has_param {
        (
            replace_nestloop_params_list(run, &joinclauses)?,
            replace_nestloop_params_list(run, &otherclauses)?,
        )
    } else {
        (joinclauses, otherclauses)
    };

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
                &outer_relids,
                outer_presorted_keys as i32,
            )?;
            label_incrementalsort_with_costsize(run, sort_plan, &outersortkeys, -1.0)?;
            outer_plan = sort_plan;
        } else {
            let sort_plan =
                make_sort_from_pathkeys(run, outer_plan, &outersortkeys, &outer_relids)?;
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
        let inner_relids = crate::relnode::relids_copy(
            mcx,
            &run.root.rel(run.root.path(inner_path).base().parent).relids,
        );
        let sort_plan =
            make_sort_from_pathkeys(run, inner_plan, &innersortkeys, &inner_relids)?;
        label_sort_with_costsize(run, sort_plan, -1.0);
        inner_plan = sort_plan;
        innerpathkeys = innersortkeys;
    } else {
        innerpathkeys = crate::relnode::pgvec_clone_shallow(
            mcx,
            &run.root.path(inner_path).base().pathkeys,
        );
    }

    if materialize_inner {
        // make_material shielding the inner from mark/restore; costed as
        // never spilling — cpu_operator_cost per tuple, in sync with
        // final_cost_mergejoin.
        let mut tlist = NodeList::nil();
        for te in inner_plan.as_plan().expect("inner plan").targetlist.iter() {
            tlist.lappend(mcx, te)?;
        }
        let mut mat = Node::build::<types_nodes::plannodes::Material>(mcx)?;
        mat.plan.targetlist = tlist;
        mat.plan.qual = NodeList::nil();
        mat.plan.lefttree = Some(inner_plan);
        mat.plan.righttree = None;
        {
            let inner = inner_plan.as_plan().expect("inner plan");
            mat.plan.disabled_nodes = inner.disabled_nodes;
            mat.plan.startup_cost = inner.startup_cost;
            mat.plan.total_cost =
                inner.total_cost + crate::gucs::cpu_operator_cost() * inner.plan_rows;
            mat.plan.plan_rows = inner.plan_rows;
            mat.plan.plan_width = inner.plan_width;
            mat.plan.parallel_aware = false;
            mat.plan.parallel_safe = inner.parallel_safe;
        }
        inner_plan = mat.seal();
    }

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

// create_append_plan (createplan.c), serial arm; async legs have no lane.
fn create_append_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (rel_id, target_id, subpaths, first_partial, pathkeys, limit_tuples, has_param_info) =
        match run.root.path(path_id) {
            PathNode::AppendPath(a) => (
                a.path.parent,
                a.path.pathtarget_id.expect("Append path has a pathtarget"),
                crate::relnode::pgvec_clone_shallow(mcx, &a.subpaths),
                a.first_partial_path,
                crate::relnode::pgvec_clone_shallow(mcx, &a.path.pathkeys),
                a.limit_tuples,
                // C folds param_info->ppi_clauses (via replace_nestloop_params)
                // into the prunequal; appendrel ParamPathInfos carry no
                // clauses, so only a clause-bearing one is loud below.
                a.path.param_info.as_ref().is_some_and(|ppi| !ppi.ppi_clauses.is_empty()),
            ),
            _ => unreachable!(),
        };
    let tlist = build_path_tlist(run, target_id, path_id)?;

    if subpaths.is_empty() {
        // Dummy rel: a Result plan with a constant-FALSE gating qual.
        let konst = clauses::make_bool_const(mcx, false, false)?;
        let mut plan = Node::build::<ResultPlan<'mcx>>(mcx)?;
        plan.plan.targetlist = tlist;
        plan.resconstantqual = Some(Node::mk_list(mcx, NodeList::make1(mcx, konst)?)?);
        copy_generic_path_info(run, &mut plan.plan, path_id);
        return Ok(plan.seal());
    }

    let orig_tlist_len = tlist.len();
    let node_cols = if pathkeys.is_empty() {
        None
    } else {
        let relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel_id).relids);
        let (_, cols) =
            prepare_sort_from_pathkeys(run, None, &tlist, &pathkeys, &relids, None, true)?;
        Some(cols)
    };

    let mut appendplans = NodeList::nil();
    for &sp in subpaths.iter() {
        let mut subplan = create_plan_recurse(run, sp, CP_EXACT_TLIST)?;
        if let Some(cols) = &node_cols {
            subplan = prepare_ordered_append_child(
                run, subplan, sp, &pathkeys, cols, limit_tuples, "Append",
            )?;
        }
        appendplans.lappend(mcx, subplan)?;
    }

    let mut apprelids = types_nodes::bitmapset::Bitmapset::empty();
    for m in crate::relnode::relids_members(&run.root.rel(rel_id).relids) {
        apprelids.add_member(mcx, m)?;
    }

    let mut part_prune_index = -1;
    if crate::gucs::enable_partition_pruning() {
        let rinfos = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel_id).baserestrictinfo);
        let mut prunequal: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for &rid in rinfos.iter() {
            if run.root.rinfo(rid).pseudoconstant {
                continue;
            }
            prunequal.push(*run.root.expr_node(run.root.rinfo(rid).clause));
        }
        if has_param_info {
            // A parameterized Append checks its ppi_clauses in the children;
            // outer-rel Vars therein become nestloop Params for pruning.
            let prmquals = match &run.root.path(path_id).base().param_info {
                Some(ppi) => crate::relnode::pgvec_clone_shallow(mcx, &ppi.ppi_clauses),
                None => unreachable!(),
            };
            for &rid in prmquals.iter() {
                if run.root.rinfo(rid).pseudoconstant {
                    continue;
                }
                let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
                prunequal.push(replace_nestloop_params(run, clause)?);
            }
        }
        if !prunequal.is_empty() {
            part_prune_index =
                crate::partprune::make_partition_pruneinfo(run, rel_id, &subpaths, &prunequal)?;
        }
    }

    let tlist_was_changed =
        node_cols.as_ref().is_some_and(|c| c.tlist.len() != orig_tlist_len);
    let node_tlist = match node_cols {
        Some(c) => c.tlist,
        None => tlist,
    };

    let mut plan = Node::build::<Append<'mcx>>(mcx)?;
    plan.plan.targetlist = node_tlist;
    plan.apprelids = apprelids;
    plan.appendplans = appendplans;
    plan.nasyncplans = 0;
    plan.first_partial_plan = first_partial;
    plan.part_prune_index = part_prune_index;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    let plan = plan.seal();
    // Strip prepare_sort_from_pathkeys' added sort columns when the caller
    // asked for the exact or a narrow tlist (C tail of create_append_plan).
    if tlist_was_changed && flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        let p = plan.as_plan().expect("plan node");
        let ps = p.parallel_safe;
        let head = NodeList::from_slice(mcx, &p.targetlist.as_slice()[..orig_tlist_len])?;
        return inject_projection_plan(mcx, plan, head, ps);
    }
    Ok(plan)
}

// Ordered Append/MergeAppend child (create_append_plan / create_merge_append_
// plan, createplan.c): pin the child's sort columns to the parent's and add a
// Sort when the subpath isn't sufficiently ordered.
fn prepare_ordered_append_child<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subplan: Node<'mcx>,
    subpath_id: PathId,
    pathkeys: &[types_pathnodes::PathKey],
    node_cols: &SortColumns<'mcx>,
    limit_tuples: f64,
    parent_label: &str,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let child_relids = {
        let parent = run.root.path(subpath_id).base().parent;
        types_pathnodes::relids::relids_copy(mcx, &run.root.rel(parent).relids)
    };
    let (subplan, sub_cols) = {
        let (new_sp, cols) = prepare_sort_from_pathkeys(
            run,
            Some(subplan),
            &subplan.as_plan().expect("plan node").targetlist,
            pathkeys,
            &child_relids,
            Some(&node_cols.sort_col_idx),
            false,
        )?;
        (new_sp.expect("lefttree"), cols)
    };
    debug_assert_eq!(sub_cols.sort_col_idx.len(), node_cols.sort_col_idx.len());
    assert!(
        sub_cols.sort_col_idx.as_slice() == node_cols.sort_col_idx.as_slice(),
        "{parent_label} child's targetlist doesn't match {parent_label}"
    );
    debug_assert!(sub_cols.sort_operators.as_slice() == node_cols.sort_operators.as_slice());
    debug_assert!(sub_cols.collations.as_slice() == node_cols.collations.as_slice());
    debug_assert!(sub_cols.nulls_first.as_slice() == node_cols.nulls_first.as_slice());

    if crate::pathkeys::pathkeys_contained_in(
        pathkeys,
        &run.root.path(subpath_id).base().pathkeys,
    ) {
        return Ok(subplan);
    }
    let mut sort = Node::build::<types_nodes::plannodes::Sort>(mcx)?;
    fill_sort_fields(run, &mut sort, subplan, sub_cols)?;
    let sort = sort.seal();
    label_sort_with_costsize(run, sort, limit_tuples);
    Ok(sort)
}

// create_merge_append_plan (createplan.c).
fn create_merge_append_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (rel_id, target_id, subpaths, pathkeys, limit_tuples, has_param_info) =
        match run.root.path(path_id) {
            PathNode::MergeAppendPath(m) => (
                m.path.parent,
                m.path.pathtarget_id.expect("MergeAppend path has a pathtarget"),
                crate::relnode::pgvec_clone_shallow(mcx, &m.subpaths),
                crate::relnode::pgvec_clone_shallow(mcx, &m.path.pathkeys),
                m.limit_tuples,
                m.path.param_info.is_some(),
            ),
            _ => unreachable!(),
        };
    let tlist = build_path_tlist(run, target_id, path_id)?;
    let orig_tlist_len = tlist.len();
    let relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel_id).relids);
    let (_, node_cols) =
        prepare_sort_from_pathkeys(run, None, &tlist, &pathkeys, &relids, None, true)?;

    let mut mergeplans = NodeList::nil();
    for &sp in subpaths.iter() {
        let subplan = create_plan_recurse(run, sp, CP_EXACT_TLIST)?;
        let subplan = prepare_ordered_append_child(
            run, subplan, sp, &pathkeys, &node_cols, limit_tuples, "MergeAppend",
        )?;
        mergeplans.lappend(mcx, subplan)?;
    }

    let mut apprelids = types_nodes::bitmapset::Bitmapset::empty();
    for m in crate::relnode::relids_members(&run.root.rel(rel_id).relids) {
        apprelids.add_member(mcx, m)?;
    }

    let mut part_prune_index = -1;
    if crate::gucs::enable_partition_pruning() {
        debug_assert!(!has_param_info);
        let rinfos =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel_id).baserestrictinfo);
        let mut prunequal: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for &rid in rinfos.iter() {
            if run.root.rinfo(rid).pseudoconstant {
                continue;
            }
            prunequal.push(*run.root.expr_node(run.root.rinfo(rid).clause));
        }
        if !prunequal.is_empty() {
            part_prune_index =
                crate::partprune::make_partition_pruneinfo(run, rel_id, &subpaths, &prunequal)?;
        }
    }

    let tlist_was_changed = node_cols.tlist.len() != orig_tlist_len;
    let _ = tlist;
    let mut plan = Node::build::<types_nodes::plannodes::MergeAppend<'mcx>>(mcx)?;
    plan.plan.targetlist = node_cols.tlist;
    plan.apprelids = apprelids;
    plan.mergeplans = mergeplans;
    plan.numCols = node_cols.sort_col_idx.len() as i32;
    plan.sortColIdx = mcx::slice_borrow_in(mcx, &node_cols.sort_col_idx)?;
    plan.sortOperators = mcx::slice_borrow_in(mcx, &node_cols.sort_operators)?;
    plan.collations = mcx::slice_borrow_in(mcx, &node_cols.collations)?;
    plan.nullsFirst = mcx::slice_borrow_in(mcx, &node_cols.nulls_first)?;
    plan.part_prune_index = part_prune_index;
    copy_generic_path_info(run, &mut plan.plan, path_id);
    let plan = plan.seal();
    // Strip prepare_sort_from_pathkeys' added sort columns when the caller
    // asked for the exact or a narrow tlist (C tail of create_merge_append_plan).
    if tlist_was_changed && flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        let p = plan.as_plan().expect("plan node");
        let ps = p.parallel_safe;
        let head = NodeList::from_slice(mcx, &p.targetlist.as_slice()[..orig_tlist_len])?;
        return inject_projection_plan(mcx, plan, head, ps);
    }
    Ok(plan)
}

// create_setop_plan + make_setop (createplan.c).
fn create_setop_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (target_id, leftpath, rightpath, cmd, strategy, group_ids, num_groups) =
        match run.root.path(path_id) {
            PathNode::SetOpPath(s) => (
                s.path.pathtarget_id.expect("SetOp path has a pathtarget"),
                s.leftpath.expect("SetOp leftpath"),
                s.rightpath.expect("SetOp rightpath"),
                s.cmd,
                s.strategy,
                crate::relnode::pgvec_clone_shallow(mcx, &s.groupList),
                s.numGroups,
            ),
            _ => unreachable!(),
        };
    let tlist = build_path_tlist(run, target_id, path_id)?;
    // SetOp doesn't project: tlist requirements pass through, and the
    // grouping columns must be labeled.
    let leftplan = create_plan_recurse(run, leftpath, flags | CP_LABEL_TLIST)?;
    let rightplan = create_plan_recurse(run, rightpath, flags | CP_LABEL_TLIST)?;

    let mut cmp_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut cmp_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut cmp_collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut cmp_nulls_first: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    for &gid in group_ids.iter() {
        let sortcl = *run
            .root
            .expr_node(gid)
            .as_sort_group_clause()
            .expect("groupList holds SortGroupClauses");
        let tle = tlist
            .iter()
            .map(|n| n.as_target_entry().expect("tlist cell"))
            .find(|t| t.ressortgroupref == sortcl.tleSortGroupRef)
            .expect("grouping column matches a tlist entry");
        cmp_col_idx.push(tle.resno);
        let op = if strategy == types_pathnodes::SETOP_HASHED {
            sortcl.eqop
        } else {
            sortcl.sortop
        };
        debug_assert!(op != 0);
        cmp_operators.push(op);
        cmp_collations.push(expr_collation(tle.expr));
        cmp_nulls_first.push(sortcl.nulls_first);
    }

    let mut plan = Node::build::<SetOp<'mcx>>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.lefttree = Some(leftplan);
    plan.plan.righttree = Some(rightplan);
    plan.cmd = cmd;
    plan.strategy = strategy;
    plan.numCols = cmp_col_idx.len() as i32;
    plan.cmpColIdx = mcx::slice_borrow_in(mcx, &cmp_col_idx)?;
    plan.cmpOperators = mcx::slice_borrow_in(mcx, &cmp_operators)?;
    plan.cmpCollations = mcx::slice_borrow_in(mcx, &cmp_collations)?;
    plan.cmpNullsFirst = mcx::slice_borrow_in(mcx, &cmp_nulls_first)?;
    plan.numGroups = clamp_cardinality_to_long(num_groups);
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_recursiveunion_plan + make_recursive_union (createplan.c).
fn create_recursiveunion_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: PathId,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (target_id, leftpath, rightpath, wt_param, distinct_ids, num_groups) =
        match run.root.path(path_id) {
            PathNode::RecursiveUnionPath(p) => (
                p.path.pathtarget_id.expect("RecursiveUnion path has a pathtarget"),
                p.leftpath.expect("RecursiveUnion leftpath"),
                p.rightpath.expect("RecursiveUnion rightpath"),
                p.wtParam,
                crate::relnode::pgvec_clone_shallow(mcx, &p.distinctList),
                p.numGroups,
            ),
            _ => unreachable!(),
        };
    // Both children must produce the same tlist.
    let leftplan = create_plan_recurse(run, leftpath, CP_EXACT_TLIST)?;
    let rightplan = create_plan_recurse(run, rightpath, CP_EXACT_TLIST)?;
    let tlist = build_path_tlist(run, target_id, path_id)?;

    let mut dup_col_idx: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut dup_operators: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    let mut dup_collations: mcx::PgVec<'mcx, u32> = mcx::PgVec::new_in(mcx);
    for &gid in distinct_ids.iter() {
        let sortcl = *run
            .root
            .expr_node(gid)
            .as_sort_group_clause()
            .expect("distinctList holds SortGroupClauses");
        let tle = tlist
            .iter()
            .map(|n| n.as_target_entry().expect("tlist cell"))
            .find(|t| t.ressortgroupref == sortcl.tleSortGroupRef)
            .expect("grouping column matches a tlist entry");
        dup_col_idx.push(tle.resno);
        debug_assert!(sortcl.eqop != 0);
        dup_operators.push(sortcl.eqop);
        dup_collations.push(expr_collation(tle.expr));
    }

    let mut plan = Node::build::<types_nodes::plannodes::RecursiveUnion<'mcx>>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.lefttree = Some(leftplan);
    plan.plan.righttree = Some(rightplan);
    plan.wtParam = wt_param;
    plan.numCols = dup_col_idx.len() as i32;
    plan.dupColIdx = mcx::slice_borrow_in(mcx, &dup_col_idx)?;
    plan.dupOperators = mcx::slice_borrow_in(mcx, &dup_operators)?;
    plan.dupCollations = mcx::slice_borrow_in(mcx, &dup_collations)?;
    plan.numGroups = clamp_cardinality_to_long(num_groups);
    copy_generic_path_info(run, &mut plan.plan, path_id);
    Ok(plan.seal())
}

// create_subqueryscan_plan (createplan.c); the subplan is created under the
// rel's subroot (C: create_plan(rel->subroot, best_path->subpath)).
fn create_subqueryscan_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: mcx::PgVec<'mcx, RinfoId>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (rel_id, sub_pid) = match run.root.path(best_path) {
        PathNode::SubqueryScanPath(p) => {
            (p.path.parent, p.subroot_subpath.expect("SubqueryScanPath subpath"))
        }
        _ => unreachable!(),
    };
    let scan_relid = run.root.rel(rel_id).relid;
    debug_assert!(scan_relid > 0);
    debug_assert!(
        run.root.rel(rel_id).rtekind
            == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY as u32
    );
    let idx = run.root.rel(rel_id).subroot_idx.expect("subquery rel has a subroot");

    run.swap_with_rel_subroot(idx);
    let subplan = crate::createplan::create_plan(run, sub_pid);
    run.swap_with_rel_subroot(idx);
    let subplan = subplan?;

    let ordered = order_qual_clauses(run, &scan_clauses)?;
    let mut qual = extract_actual_clauses(run, &ordered);

    if run.root.path(best_path).base().param_info.is_some() {
        // Subquery lateral params first (createplan.c): they carry fixed
        // param IDs, and replace_nestloop_params then reuses them for the
        // same Var in scan_clauses — a second ID for one Var would land in
        // the child's chgParam but not Memoize's keyparamids, purging the
        // cache on every rescan.
        let subplan_params = crate::relnode::pgvec_clone_shallow(
            mcx,
            &run.root.rel(rel_id).subplan_params,
        );
        crate::paramassign::process_subquery_nestloop_params(run, &subplan_params)?;
        qual = replace_nestloop_params_list(run, &qual)?;
    }

    let mut plan = Node::build::<SubqueryScan<'mcx>>(mcx)?;
    plan.scan.plan.targetlist = tlist;
    plan.scan.plan.qual = qual;
    plan.scan.scanrelid = scan_relid;
    plan.subplan = Some(subplan);
    plan.scanstatus = 0;
    copy_generic_path_info(run, &mut plan.scan.plan, best_path);
    Ok(plan.seal())
}

// list_concat_unique (list.c): append members of `add` not already equal()-
// present in `dest`.
fn list_concat_unique<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    dest: &mut NodeList<'mcx>,
    add: &NodeList<'mcx>,
) -> PgResult<()> {
    for n in add {
        if !dest.iter().any(|d| types_nodes::equal(d, n)) {
            dest.lappend(mcx, n)?;
        }
    }
    Ok(())
}

// tlist_same_exprs (tlist.c).
fn tlist_same_exprs(tlist1: &NodeList<'_>, tlist2: &NodeList<'_>) -> bool {
    if tlist1.len() != tlist2.len() {
        return false;
    }
    for (a, b) in tlist1.iter().zip(tlist2.iter()) {
        let ta = a.as_target_entry().expect("tlist cell");
        let tb = b.as_target_entry().expect("tlist cell");
        if !types_nodes::equal::equal(ta.expr, tb.expr) {
            return false;
        }
    }
    true
}

fn path_needs_child_reparam(run: &PlannerRun<'_>, path: PathId, child_rel: RelId) -> bool {
    run.root.path(path).base().param_info.is_some()
        && crate::relnode::relids_overlap(
            crate::pathnode::path_req_outer(run.root.path(path).base()),
            &run.root.rel(child_rel).top_parent_relids,
        )
}

// path_is_reparameterizable_by_child (pathnode.c), restricted to the path
// types reparameterize_path_by_child below handles.
pub(crate) fn path_is_reparameterizable_by_child(
    run: &PlannerRun<'_>,
    path: PathId,
    child_rel: RelId,
) -> bool {
    if !path_needs_child_reparam(run, path, child_rel) {
        return true;
    }
    match run.root.path(path) {
        PathNode::Path(_) | PathNode::IndexPath(_) => true,
        PathNode::BitmapHeapPath(p) => p
            .bitmapqual
            .is_none_or(|q| path_is_reparameterizable_by_child(run, q, child_rel)),
        PathNode::BitmapAndPath(p) => p
            .bitmapquals
            .iter()
            .all(|&q| path_is_reparameterizable_by_child(run, q, child_rel)),
        PathNode::BitmapOrPath(p) => p
            .bitmapquals
            .iter()
            .all(|&q| path_is_reparameterizable_by_child(run, q, child_rel)),
        PathNode::NestPath(p) => {
            path_is_reparameterizable_by_child(run, p.jpath.outerjoinpath.unwrap(), child_rel)
                && path_is_reparameterizable_by_child(run, p.jpath.innerjoinpath.unwrap(), child_rel)
        }
        PathNode::MergePath(p) => {
            path_is_reparameterizable_by_child(run, p.jpath.outerjoinpath.unwrap(), child_rel)
                && path_is_reparameterizable_by_child(run, p.jpath.innerjoinpath.unwrap(), child_rel)
        }
        PathNode::HashPath(p) => {
            path_is_reparameterizable_by_child(run, p.jpath.outerjoinpath.unwrap(), child_rel)
                && path_is_reparameterizable_by_child(run, p.jpath.innerjoinpath.unwrap(), child_rel)
        }
        PathNode::AppendPath(p) => p
            .subpaths
            .iter()
            .all(|&q| path_is_reparameterizable_by_child(run, q, child_rel)),
        PathNode::MaterialPath(p) => {
            path_is_reparameterizable_by_child(run, p.subpath.unwrap(), child_rel)
        }
        PathNode::MemoizePath(p) => {
            path_is_reparameterizable_by_child(run, p.subpath.unwrap(), child_rel)
        }
        _ => false,
    }
}

// reparameterize_path_by_child (pathnode.c): translate a parent-rel
// parameterization down to the child that create_plan is building for.
// Mutates the winning path in place (create_plan visits it exactly once).
fn reparameterize_path_by_child<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path: PathId,
    child_rel: RelId,
) -> PgResult<Option<PathId>> {
    enum Snap<'m> {
        Scan,
        Index {
            indexinfo: &'m types_pathnodes::IndexOptInfo<'m>,
            indexclauses: mcx::PgVec<'m, types_pathnodes::IndexClause<'m>>,
        },
        BitmapHeap(Option<PathId>),
        BitmapList(mcx::PgVec<'m, PathId>),
        Join {
            outer: PathId,
            inner: PathId,
            restrictinfo: mcx::PgVec<'m, RinfoId>,
            clauses: Option<mcx::PgVec<'m, RinfoId>>,
        },
        Append(mcx::PgVec<'m, PathId>),
        Sub {
            subpath: PathId,
            param_exprs: Option<mcx::PgVec<'m, types_pathnodes::NodeId>>,
        },
        Unsupported,
    }

    let mcx = run.mcx;
    if !path_needs_child_reparam(run, path, child_rel) {
        return Ok(Some(path));
    }
    let top_parent = run.root.rel(child_rel).top_parent.expect("child rel has a top parent");

    fn adjust_rinfos<'mcx>(
        run: &mut PlannerRun<'mcx>,
        rids: &[RinfoId],
        child_rel: RelId,
        top_parent: RelId,
    ) -> PgResult<mcx::PgVec<'mcx, RinfoId>> {
        let mut out: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(run.mcx);
        for &rid in rids {
            out.push(crate::inherit::adjust_child_rinfo_multilevel(
                run, rid, child_rel, top_parent,
            )?);
        }
        Ok(out)
    }

    let parent = run.root.path(path).base().parent;
    let snap = match run.root.path(path) {
        PathNode::Path(_) => Snap::Scan,
        PathNode::IndexPath(ip) => Snap::Index {
            indexinfo: ip.indexinfo.expect("indexinfo set"),
            indexclauses: {
                let mut v = mcx::PgVec::new_in(mcx);
                for ic in ip.indexclauses.iter() {
                    v.push(ic.clone());
                }
                v
            },
        },
        PathNode::BitmapHeapPath(p) => Snap::BitmapHeap(p.bitmapqual),
        PathNode::BitmapAndPath(p) => {
            Snap::BitmapList(crate::relnode::pgvec_clone_shallow(mcx, &p.bitmapquals))
        }
        PathNode::BitmapOrPath(p) => {
            Snap::BitmapList(crate::relnode::pgvec_clone_shallow(mcx, &p.bitmapquals))
        }
        PathNode::NestPath(p) => Snap::Join {
            outer: p.jpath.outerjoinpath.unwrap(),
            inner: p.jpath.innerjoinpath.unwrap(),
            restrictinfo: crate::relnode::pgvec_clone_shallow(mcx, &p.jpath.joinrestrictinfo),
            clauses: None,
        },
        PathNode::MergePath(p) => Snap::Join {
            outer: p.jpath.outerjoinpath.unwrap(),
            inner: p.jpath.innerjoinpath.unwrap(),
            restrictinfo: crate::relnode::pgvec_clone_shallow(mcx, &p.jpath.joinrestrictinfo),
            clauses: Some(crate::relnode::pgvec_clone_shallow(mcx, &p.path_mergeclauses)),
        },
        PathNode::HashPath(p) => Snap::Join {
            outer: p.jpath.outerjoinpath.unwrap(),
            inner: p.jpath.innerjoinpath.unwrap(),
            restrictinfo: crate::relnode::pgvec_clone_shallow(mcx, &p.jpath.joinrestrictinfo),
            clauses: Some(crate::relnode::pgvec_clone_shallow(mcx, &p.path_hashclauses)),
        },
        PathNode::AppendPath(p) => {
            Snap::Append(crate::relnode::pgvec_clone_shallow(mcx, &p.subpaths))
        }
        PathNode::MaterialPath(p) => {
            Snap::Sub { subpath: p.subpath.unwrap(), param_exprs: None }
        }
        PathNode::MemoizePath(p) => Snap::Sub {
            subpath: p.subpath.unwrap(),
            param_exprs: Some(crate::relnode::pgvec_clone_shallow(mcx, &p.param_exprs)),
        },
        _ => Snap::Unsupported,
    };

    match snap {
        Snap::Scan => {
            let list = crate::relnode::pgvec_clone_shallow(
                mcx,
                &run.root.rel(parent).baserestrictinfo,
            );
            let new = adjust_rinfos(run, &list, child_rel, top_parent)?;
            run.root.rel_mut(parent).baserestrictinfo = new;
            // SampleScan: the sampled rel's RTE carries the tablesample
            // clause whose args may reference the joining rel's parent;
            // translate them to the child (pathnode.c:4464-4477).
            if run.root.path(path).base().pathtype
                == crate::pathnode::tag16(NodeTag::T_SampleScan)
            {
                let scan_relid = run.root.rel(parent).relid;
                debug_assert!(scan_relid > 0);
                let rte_cell = run.rte_cell(scan_relid as usize);
                let rte =
                    rte_cell.as_range_tbl_entry().expect("rtable cell is a RangeTblEntry");
                debug_assert!(rte.rtekind == types_nodes::RTEKind::RTE_RELATION);
                let tsc = rte.tablesample.expect("sampled rel has a tablesample clause");
                let adjusted = crate::inherit::adjust_appendrel_attrs_multilevel(
                    run, tsc, child_rel, top_parent,
                )?;
                // SAFETY: planning is single-threaded and no derived ref to
                // this RTE's tablesample outlives the write (C mutates the
                // RTE in place at the same point, pathnode.c:4476).
                unsafe {
                    rte_cell
                        .with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.tablesample = Some(adjusted)
                        })
                        .expect("RangeTblEntry");
                }
            }
        }
        Snap::Index { indexinfo, indexclauses } => {
            let old = indexinfo.indrestrictinfo.borrow().clone();
            let new = adjust_rinfos(run, &old, child_rel, top_parent)?;
            *indexinfo.indrestrictinfo.borrow_mut() = new;
            let mut new_clauses: mcx::PgVec<'mcx, types_pathnodes::IndexClause<'mcx>> =
                mcx::PgVec::new_in(mcx);
            for ic in indexclauses.iter() {
                let rinfo = match ic.rinfo {
                    Some(r) => Some(crate::inherit::adjust_child_rinfo_multilevel(
                        run, r, child_rel, top_parent,
                    )?),
                    None => None,
                };
                let indexquals = adjust_rinfos(run, &ic.indexquals, child_rel, top_parent)?;
                new_clauses.push(types_pathnodes::IndexClause {
                    rinfo,
                    indexquals,
                    lossy: ic.lossy,
                    indexcol: ic.indexcol,
                    indexcols: crate::relnode::pgvec_clone_shallow(mcx, &ic.indexcols),
                });
            }
            if let PathNode::IndexPath(p) = run.root.path_mut(path) {
                p.indexclauses = new_clauses;
            }
        }
        Snap::BitmapHeap(bq) => {
            let list = crate::relnode::pgvec_clone_shallow(
                mcx,
                &run.root.rel(parent).baserestrictinfo,
            );
            let new = adjust_rinfos(run, &list, child_rel, top_parent)?;
            run.root.rel_mut(parent).baserestrictinfo = new;
            if let Some(bq) = bq {
                if reparameterize_path_by_child(run, bq, child_rel)?.is_none() {
                    return Ok(None);
                }
            }
        }
        Snap::BitmapList(quals) => {
            for &q in quals.iter() {
                if reparameterize_path_by_child(run, q, child_rel)?.is_none() {
                    return Ok(None);
                }
            }
        }
        Snap::Join { outer, inner, restrictinfo, clauses } => {
            if reparameterize_path_by_child(run, outer, child_rel)?.is_none()
                || reparameterize_path_by_child(run, inner, child_rel)?.is_none()
            {
                return Ok(None);
            }
            let new_rl = adjust_rinfos(run, &restrictinfo, child_rel, top_parent)?;
            let new_cl = match clauses {
                Some(cl) => Some(adjust_rinfos(run, &cl, child_rel, top_parent)?),
                None => None,
            };
            match run.root.path_mut(path) {
                PathNode::NestPath(p) => p.jpath.joinrestrictinfo = new_rl,
                PathNode::MergePath(p) => {
                    p.jpath.joinrestrictinfo = new_rl;
                    p.path_mergeclauses = new_cl.unwrap();
                }
                PathNode::HashPath(p) => {
                    p.jpath.joinrestrictinfo = new_rl;
                    p.path_hashclauses = new_cl.unwrap();
                }
                _ => unreachable!(),
            }
        }
        Snap::Append(subs) => {
            for &q in subs.iter() {
                if reparameterize_path_by_child(run, q, child_rel)?.is_none() {
                    return Ok(None);
                }
            }
        }
        Snap::Sub { subpath, param_exprs } => {
            if reparameterize_path_by_child(run, subpath, child_rel)?.is_none() {
                return Ok(None);
            }
            if let Some(pe) = param_exprs {
                let mut new_pe: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                    mcx::PgVec::new_in(mcx);
                for &e in pe.iter() {
                    new_pe.push(crate::inherit::adjust_child_expr_multilevel(
                        run, e, child_rel, top_parent,
                    )?);
                }
                if let PathNode::MemoizePath(p) = run.root.path_mut(path) {
                    p.param_exprs = new_pe;
                }
            }
        }
        Snap::Unsupported => return Ok(None),
    }

    // Translate the ParamPathInfo, which refers to the topmost parent.
    let (old_rows, old_clauses, old_serials, old_req_outer) = {
        let ppi = run.root.path(path).base().param_info.as_ref().expect("parameterized path");
        (
            ppi.ppi_rows,
            crate::relnode::pgvec_clone_shallow(mcx, &ppi.ppi_clauses),
            crate::relnode::relids_copy(mcx, &ppi.ppi_serials),
            crate::relnode::relids_copy(mcx, &ppi.ppi_req_outer),
        )
    };
    let required_outer =
        crate::inherit::adjust_child_relids_multilevel(run, &old_req_outer, child_rel, top_parent);
    let existing = run
        .root
        .rel(parent)
        .ppilist
        .iter()
        .position(|p| crate::relnode::relids_equal(&p.ppi_req_outer, &required_outer));
    let new_ppi = match existing {
        Some(i) => run.root.rel(parent).ppilist[i].clone(),
        None => {
            let clauses = adjust_rinfos(run, &old_clauses, child_rel, top_parent)?;
            let ppi = types_pathnodes::ParamPathInfo {
                ppi_req_outer: crate::relnode::relids_copy(mcx, &required_outer),
                ppi_rows: old_rows,
                ppi_clauses: clauses,
                ppi_serials: old_serials,
            };
            run.root.rel_mut(parent).ppilist.push(ppi.clone());
            ppi
        }
    };
    run.root.path_mut(path).base_mut().param_info = Some(mcx::box_new_in(mcx, new_ppi));

    // A lateral reference to only the parent of the outer rel shows up in the
    // pathtarget; translate it too.
    if crate::relnode::relids_overlap(
        &run.root.rel(parent).lateral_relids,
        &run.root.rel(child_rel).top_parent_relids,
    ) {
        let pt = run.root.path(path).base().pathtarget_id.expect("path has a pathtarget");
        let src_exprs = crate::relnode::pgvec_clone_shallow(mcx, &run.root.pathtarget(pt).exprs);
        let mut exprs: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
        for &e in src_exprs.iter() {
            exprs.push(crate::inherit::adjust_child_expr_multilevel(
                run, e, child_rel, top_parent,
            )?);
        }
        let copy = {
            let src = run.root.pathtarget(pt);
            types_pathnodes::PathTarget {
                exprs,
                sortgrouprefs: crate::relnode::pgvec_clone_shallow(mcx, &src.sortgrouprefs),
                cost: src.cost,
                width: src.width,
                has_volatile_expr: src.has_volatile_expr,
            }
        };
        let new_pt = run.root.alloc_pathtarget(copy);
        run.root.path_mut(path).base_mut().pathtarget_id = Some(new_pt);
    }
    Ok(Some(path))
}
