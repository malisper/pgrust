//! Plan-tree deparse contexts (ruleutils.c): deparse_context_for_plan_tree,
//! set_deparse_plan, push/pop child and ancestor plans, resolve_special_varno,
//! appendrels/appendparents child-Var mapping, PARAM_EXEC referent/generator
//! search.

use std::rc::Rc;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::{Plan, PlannedStmt};
use types_nodes::primnodes::{Param, ParamKind, SubPlan, Var};
use types_nodes::{Node, NodeList, NodeTag, RangeTblEntry};

use crate::deparse::{get_rule_expr, DeparseContext};
use crate::gap;
use crate::query::{self, DeparseNamespace};

#[derive(Clone, Copy)]
pub enum AncestorEntry<'mcx> {
    Plan(Node<'mcx>),
    Sub(&'mcx SubPlan<'mcx>),
}

#[derive(Clone, Default)]
pub(crate) struct DpnsPlan<'mcx> {
    pub plan: Option<Node<'mcx>>,
    pub ancestors: Vec<AncestorEntry<'mcx>>,
    pub outer_plan: Option<Node<'mcx>>,
    pub inner_plan: Option<Node<'mcx>>,
    pub outer_tlist: Option<&'mcx NodeList<'mcx>>,
    pub inner_tlist: Option<&'mcx NodeList<'mcx>>,
    pub index_tlist: Option<&'mcx NodeList<'mcx>>,
    pub ret_old_alias: Option<&'mcx str>,
    pub ret_new_alias: Option<&'mcx str>,
}

pub struct PlanDeparse<'mcx> {
    pub(crate) ns: Rc<DeparseNamespace<'mcx>>,
}

fn plan_of(node: Node<'_>) -> &Plan<'_> {
    node.as_plan()
        .unwrap_or_else(|| gap("set_deparse_plan", &format!("{:?} plan vocabulary", node.node_tag())))
}

pub fn deparse_context_for_plan_tree<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
    rtable_names: Vec<Option<String>>,
) -> PgResult<PlanDeparse<'mcx>> {
    let rtable: Vec<&RangeTblEntry<'_>> = pstmt
        .rtable
        .iter()
        .map(|n| n.as_range_tbl_entry().expect("rtable entry"))
        .collect();
    let ntables = rtable.len();
    let mut dpns = DeparseNamespace::empty(rtable);
    dpns.rtable_names = rtable_names;
    dpns.subplans = Some(&pstmt.subplans);
    if !pstmt.appendRelations.is_nil() {
        let mut appendrels: Vec<Option<&'mcx types_nodes::plannodes::AppendRelInfo<'mcx>>> =
            vec![None; ntables + 1];
        for n in pstmt.appendRelations.iter() {
            let appinfo = n.as_append_rel_info().expect("appendRelations cell");
            let crelid = appinfo.child_relid as usize;
            assert!(crelid > 0 && crelid <= ntables);
            assert!(appendrels[crelid].is_none());
            appendrels[crelid] = Some(appinfo);
        }
        dpns.appendrels = Some(appendrels);
    }
    query::set_simple_column_names(mcx, &mut dpns)?;
    Ok(PlanDeparse { ns: Rc::new(dpns) })
}

pub fn set_deparse_context_plan<'mcx>(
    ctx: &PlanDeparse<'mcx>,
    plan: Node<'mcx>,
    ancestors: Vec<AncestorEntry<'mcx>>,
) {
    let mut ps = ctx.ns.plan.borrow_mut();
    ps.ancestors = ancestors;
    set_deparse_plan(&mut ps, plan, ctx.ns.subplans);
    if let Some(mt) = plan.as_modify_table() {
        ps.ret_old_alias = mt.returningOldAlias;
        ps.ret_new_alias = mt.returningNewAlias;
    }
}

pub fn select_rtable_names_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    rels_used: &Bitmapset<'mcx>,
) -> PgResult<Vec<Option<String>>> {
    let rt: Vec<&RangeTblEntry<'_>> = rtable
        .iter()
        .map(|n| n.as_range_tbl_entry().expect("rtable entry"))
        .collect();
    let mut dpns = DeparseNamespace::empty(rt);
    // C's rels_used is NULL when no plan node referenced any RTE (an empty
    // Bitmapset has no representation), and set_rtable_names treats NULL as
    // "no filter": every RTE keeps its name (scanless plans, e.g. a dummy
    // join under GroupAggregate, still deparse qualified Vars).
    let rels_used = if rels_used.is_empty() { None } else { Some(rels_used) };
    query::set_rtable_names(mcx, &mut dpns, &[], rels_used)?;
    Ok(std::mem::take(&mut dpns.rtable_names))
}

pub fn deparse_expression<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Node<'mcx>,
    ctx: &PlanDeparse<'mcx>,
    forceprefix: bool,
    showimplicit: bool,
) -> PgResult<String> {
    let mut dctx = DeparseContext::new(mcx, 0);
    dctx.namespaces = vec![Rc::clone(&ctx.ns)];
    dctx.varprefix = forceprefix;
    get_rule_expr(expr, &mut dctx, showimplicit)?;
    Ok(dctx.buf)
}

pub(crate) fn set_deparse_plan<'mcx>(
    ps: &mut DpnsPlan<'mcx>,
    plan: Node<'mcx>,
    subplans: Option<&'mcx types_nodes::list::OptNodeList<'mcx>>,
) {
    ps.plan = Some(plan);

    ps.outer_plan = if let Some(a) = plan.as_append() {
        Some(a.appendplans.nth(0))
    } else if let Some(m) = plan.as_merge_append() {
        Some(m.mergeplans.nth(0))
    } else {
        plan_of(plan).lefttree
    };
    ps.outer_tlist = ps.outer_plan.map(|o| &plan_of(o).targetlist);

    ps.inner_plan = if let Some(sq) = plan.as_subquery_scan() {
        sq.subplan
    } else if let Some(cs) = plan.as_cte_scan() {
        Some(
            subplans
                .expect("plan deparse context has subplans")
                .nth(cs.ctePlanId as usize - 1)
                // CTE subplans are parallel-restricted; never a NULL hole.
                .expect("CteScan subplan cell present"),
        )
    } else if let Some(wts) = plan.as_work_table_scan() {
        Some(find_recursive_union(ps, wts))
    } else if let Some(mt) = plan.as_modify_table() {
        if mt.operation == CmdType::CMD_MERGE {
            mt.plan.lefttree
        } else {
            Some(plan)
        }
    } else {
        plan_of(plan).righttree
    };
    ps.inner_tlist = match plan.as_modify_table() {
        Some(mt) if mt.operation == CmdType::CMD_INSERT => Some(&mt.exclRelTlist),
        _ => ps.inner_plan.map(|i| &plan_of(i).targetlist),
    };

    ps.index_tlist = plan.as_index_only_scan().map(|ios| &ios.indextlist);
}


// find_recursive_union (ruleutils.c): the parent RecursiveUnion supplying a
// WorkTableScan's rows, located by wtParam among the ancestor plans.
fn find_recursive_union<'mcx>(
    ps: &DpnsPlan<'mcx>,
    wts: &types_nodes::plannodes::WorkTableScan<'_>,
) -> Node<'mcx> {
    for a in &ps.ancestors {
        if let AncestorEntry::Plan(p) = a {
            if let Some(ru) = p.as_recursive_union() {
                if ru.wtParam == wts.wtParam {
                    return *p;
                }
            }
        }
    }
    panic!(
        "could not find RecursiveUnion for WorkTableScan with wtParam {}",
        wts.wtParam
    )
}

pub(crate) fn push_child_plan<'mcx>(
    dpns: &DeparseNamespace<'mcx>,
    plan: Node<'mcx>,
) -> DpnsPlan<'mcx> {
    let mut ps = dpns.plan.borrow_mut();
    let save = ps.clone();
    let cur = ps.plan.expect("push_child_plan from an active plan node");
    ps.ancestors.insert(0, AncestorEntry::Plan(cur));
    set_deparse_plan(&mut ps, plan, dpns.subplans);
    save
}

pub(crate) fn pop_child_plan<'mcx>(dpns: &DeparseNamespace<'mcx>, save: DpnsPlan<'mcx>) {
    *dpns.plan.borrow_mut() = save;
}

pub(crate) fn push_ancestor_plan<'mcx>(
    dpns: &DeparseNamespace<'mcx>,
    ancestor_idx: usize,
) -> DpnsPlan<'mcx> {
    let mut ps = dpns.plan.borrow_mut();
    let save = ps.clone();
    let AncestorEntry::Plan(plan) = ps.ancestors[ancestor_idx] else {
        panic!("push_ancestor_plan: SubPlan ancestor cell");
    };
    ps.ancestors = ps.ancestors[ancestor_idx + 1..].to_vec();
    set_deparse_plan(&mut ps, plan, dpns.subplans);
    save
}

pub(crate) fn pop_ancestor_plan<'mcx>(dpns: &DeparseNamespace<'mcx>, save: DpnsPlan<'mcx>) {
    *dpns.plan.borrow_mut() = save;
}

pub(crate) fn get_tle_by_resno<'mcx>(
    tlist: &NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx types_nodes::primnodes::TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("targetlist holds TargetEntries"))
        .find(|tle| tle.resno == resno)
}

pub(crate) fn resolve_special_varno<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    callback: &mut dyn FnMut(Node<'mcx>, &mut DeparseContext<'mcx>) -> PgResult<()>,
) -> PgResult<()> {
    let Some(var) = node.as_var() else {
        return callback(node, ctx);
    };
    let dpns = Rc::clone(&ctx.namespaces[var.varlevelsup as usize]);
    let ps = dpns.plan.borrow();

    if var.varno == types_nodes::primnodes::OUTER_VAR && ps.outer_tlist.is_some() {
        let tle = get_tle_by_resno(ps.outer_tlist.unwrap(), var.varattno)
            .unwrap_or_else(|| panic!("bogus varattno for OUTER_VAR var: {}", var.varattno));
        // Descending to an Append/MergeAppend first child: union apprelids
        // into appendparents for every Var in the resolved subexpression.
        let node_apprelids = match ps.plan {
            Some(p) => match p.as_append() {
                Some(a) => Some(&a.apprelids),
                None => p.as_merge_append().map(|m| &m.apprelids),
            },
            None => None,
        };
        let save_appendparents = if let Some(apprelids) = node_apprelids {
            let cur = core::mem::replace(&mut ctx.appendparents, Bitmapset::empty());
            ctx.appendparents = cur.union(apprelids, ctx.mcx)?;
            Some(cur)
        } else {
            None
        };
        let outer = ps.outer_plan.unwrap();
        drop(ps);
        let save = push_child_plan(&dpns, outer);
        let r = resolve_special_varno(tle.expr, ctx, callback);
        pop_child_plan(&dpns, save);
        if let Some(s) = save_appendparents {
            ctx.appendparents = s;
        }
        return r;
    }
    if var.varno == types_nodes::primnodes::INNER_VAR && ps.inner_tlist.is_some() {
        let tle = get_tle_by_resno(ps.inner_tlist.unwrap(), var.varattno)
            .unwrap_or_else(|| panic!("bogus varattno for INNER_VAR var: {}", var.varattno));
        let inner = ps.inner_plan.unwrap();
        drop(ps);
        let save = push_child_plan(&dpns, inner);
        let r = resolve_special_varno(tle.expr, ctx, callback);
        pop_child_plan(&dpns, save);
        return r;
    }
    if var.varno == types_nodes::primnodes::INDEX_VAR && ps.index_tlist.is_some() {
        let tle = get_tle_by_resno(ps.index_tlist.unwrap(), var.varattno)
            .unwrap_or_else(|| panic!("bogus varattno for INDEX_VAR var: {}", var.varattno));
        drop(ps);
        return resolve_special_varno(tle.expr, ctx, callback);
    }
    if var.varno < 1 || var.varno as usize > dpns.rtable.len() {
        panic!("bogus varno: {}", var.varno);
    }
    drop(ps);
    callback(node, ctx)
}

pub(crate) fn get_special_variable<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let need_paren = node.node_tag() != NodeTag::T_Var;
    if need_paren {
        ctx.buf.push('(');
    }
    get_rule_expr(node, ctx, true)?;
    if need_paren {
        ctx.buf.push(')');
    }
    Ok(())
}

pub(crate) fn find_param_referent<'mcx>(
    param: &Param,
    ctx: &DeparseContext<'mcx>,
) -> Option<(Node<'mcx>, Rc<DeparseNamespace<'mcx>>, usize)> {
    if param.paramkind != ParamKind::PARAM_EXEC {
        return None;
    }
    let dpns = Rc::clone(ctx.namespaces.first()?);
    let (mut child_plan, ancestors) = {
        let ps = dpns.plan.borrow();
        (ps.plan, ps.ancestors.clone())
    };

    for (idx, entry) in ancestors.iter().enumerate() {
        match *entry {
            AncestorEntry::Plan(ancestor) => {
                if let Some(nl) = ancestor.as_nest_loop() {
                    let inner = nl.join.plan.righttree;
                    if child_plan.is_some() && inner.is_some_and(|i| i.ptr_eq(child_plan.unwrap())) {
                        for nlp_node in &nl.nestParams {
                            let nlp = nlp_node.as_nest_loop_param().expect("nestParams cell");
                            if nlp.paramno == param.paramid {
                                return Some((nlp.paramval, dpns, idx));
                            }
                        }
                    }
                }
                child_plan = Some(ancestor);
            }
            AncestorEntry::Sub(subplan) => {
                for (i, paramid) in subplan.parParam.iter().enumerate() {
                    if paramid == param.paramid {
                        let arg = subplan.args.nth(i);
                        for (j, rest) in ancestors.iter().enumerate().skip(idx + 1) {
                            if matches!(rest, AncestorEntry::Plan(_)) {
                                return Some((arg, dpns, j));
                            }
                        }
                        panic!("SubPlan cannot be outermost ancestor");
                    }
                }
            }
        }
    }
    None
}

pub(crate) fn find_param_generator<'mcx>(
    param: &Param,
    ctx: &DeparseContext<'mcx>,
) -> Option<(&'mcx SubPlan<'mcx>, usize)> {
    if param.paramkind != ParamKind::PARAM_EXEC {
        return None;
    }
    let dpns = ctx.namespaces.first()?;
    let ps = dpns.plan.borrow();
    let plan = ps.plan?;

    if let Some(hit) = find_param_generator_initplan(param, plan) {
        return Some(hit);
    }
    for tle_node in plan_of(plan).targetlist.iter() {
        let tle = tle_node.as_target_entry().expect("targetlist holds TargetEntries");
        if let Some(subplan) = tle.expr.as_sub_plan() {
            if subplan.subLinkType == types_nodes::primnodes::SubLinkType::MULTIEXPR_SUBLINK {
                if let Some(i) = subplan.setParam.iter().position(|id| id == param.paramid) {
                    return Some((subplan, i));
                }
            }
        }
    }
    for entry in &ps.ancestors {
        match *entry {
            AncestorEntry::Sub(subplan) => {
                if let Some(i) = subplan.paramIds.iter().position(|id| id == param.paramid) {
                    return Some((subplan, i));
                }
            }
            AncestorEntry::Plan(ancestor) => {
                if let Some(hit) = find_param_generator_initplan(param, ancestor) {
                    return Some(hit);
                }
            }
        }
    }
    None
}

fn find_param_generator_initplan<'mcx>(
    param: &Param,
    plan: Node<'mcx>,
) -> Option<(&'mcx SubPlan<'mcx>, usize)> {
    for sp_node in plan_of(plan).initPlan.iter() {
        let subplan = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
        if let Some(i) = subplan.setParam.iter().position(|id| id == param.paramid) {
            return Some((subplan, i));
        }
    }
    None
}

pub(crate) fn get_variable_special<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    resolve_special_varno(node, ctx, &mut get_special_variable)
}

pub(crate) fn inner_plan_drilldown<'mcx>(
    var: &Var<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    dpns: &Rc<DeparseNamespace<'mcx>>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<bool> {
    let attnum = var.varattno;
    let colnames_len = rte.eref.map_or(0, |e| e.colnames.len());
    let ps = dpns.plan.borrow();
    if !matches!(rte.rtekind, types_nodes::RTEKind::RTE_SUBQUERY | types_nodes::RTEKind::RTE_CTE)
        || attnum as i32 <= colnames_len as i32
        || ps.inner_plan.is_none()
    {
        return Ok(false);
    }
    let tle = get_tle_by_resno(ps.inner_tlist.expect("inner_plan implies inner_tlist"), attnum)
        .unwrap_or_else(|| {
            panic!(
                "invalid attnum {attnum} for relation \"{}\"",
                rte.eref.and_then(|e| e.aliasname).unwrap_or("")
            )
        });
    let inner = ps.inner_plan.unwrap();
    drop(ps);
    let save = push_child_plan(dpns, inner);
    let need_paren = tle.expr.node_tag() != NodeTag::T_Var;
    if need_paren {
        ctx.buf.push('(');
    }
    let r = get_rule_expr(tle.expr, ctx, true);
    if need_paren {
        ctx.buf.push(')');
    }
    pop_child_plan(dpns, save);
    r.map(|()| true)
}
