//! subselect.c uncorrelated-initplan slice (EXISTS/EXPR sublinks) plus the
//! pull_up_sublinks decision walk (prepjointree.c); every other sublink shape
//! is a named panic.

use clauses::NodeWalker;
use mcx::Mcx;
use types_core::catalog::{BOOLOID, VOIDOID};
use types_error::PgResult;
use types_nodes::list::{IntList, NodeList};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{Param, ParamKind, SubLink, SubLinkType, SubPlan};
use types_nodes::{Node, NodeTag};
use types_pathnodes::RelId;

use crate::createplan::create_plan;
use crate::pathnode::get_cheapest_fractional_path;
use crate::planmain::fetch_final_rel;
use crate::run::PlannerRun;

// pull_up_sublinks (prepjointree.c), decision-only: nothing uncorrelated
// converts to a join, so the walk proves that and panics where C would build one.
pub fn pull_up_sublinks<'mcx>(run: &mut PlannerRun<'mcx>, parse: &Query<'mcx>) -> PgResult<()> {
    let f = parse.jointree.expect("jointree is a FromExpr");
    for child in &f.fromlist {
        assert!(
            child.node_tag() == NodeTag::T_RangeTblRef,
            "pull_up_sublinks_jointree_recurse (prepjointree.c): {:?}; M2 join lane",
            child.node_tag()
        );
    }
    match f.quals {
        Some(quals) => pull_up_sublinks_qual_recurse(run, quals),
        None => Ok(()),
    }
}

fn pull_up_sublinks_qual_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            match sl.subLinkType {
                SubLinkType::ANY_SUBLINK => panic!(
                    "convert_ANY_sublink_to_join (subselect.c): M2 join lane"
                ),
                SubLinkType::EXISTS_SUBLINK => {
                    if vars::contain_vars_of_level(sl.subselect, 1)? {
                        panic!(
                            "convert_EXISTS_sublink_to_join (subselect.c): correlated \
                             EXISTS; M2 join lane"
                        );
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match b.boolop {
                types_nodes::BoolExprType::AND_EXPR => {
                    for arg in &b.args {
                        pull_up_sublinks_qual_recurse(run, arg)?;
                    }
                    Ok(())
                }
                types_nodes::BoolExprType::NOT_EXPR => {
                    let arg = b.args.first().expect("NOT has one arg");
                    if let Some(sl) = arg.as_sub_link() {
                        if sl.subLinkType == SubLinkType::EXISTS_SUBLINK
                            && vars::contain_vars_of_level(sl.subselect, 1)?
                        {
                            panic!(
                                "convert_EXISTS_sublink_to_join (subselect.c): correlated \
                                 NOT EXISTS; M2 join lane"
                            );
                        }
                    }
                    Ok(())
                }
                types_nodes::BoolExprType::OR_EXPR => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

/// SS_process_sublinks (subselect.c).
pub fn ss_process_sublinks<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    is_qual: bool,
) -> PgResult<Node<'mcx>> {
    Ok(process_sublinks_mutator(run, expr, is_qual)?.unwrap_or(expr))
}

// C's AND/OR-flatness arms are unreachable: BoolExpr panicked upstream in
// eval_const_expressions/canonicalize_qual.
fn process_sublinks_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    is_top_qual: bool,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_SubLink {
        let sl = node.as_sub_link().unwrap();
        debug_assert!(sl.testexpr.is_none(), "testexpr-bearing sublinks are loud upstream");
        return Ok(Some(make_subplan(run, sl, is_top_qual)?));
    }
    debug_assert!(!matches!(
        node.node_tag(),
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_Query
    ));
    clauses::expression_tree_mutator(run.mcx, node, &mut |n| {
        process_sublinks_mutator(run, n, false)
    })
}

// make_subplan (subselect.c). C copyObject's the sub-Query because rules can
// alias one Query from several SubLinks; parser-built SubLinks hold the only
// reference, so a list-cell-level copy is the scribble target.
fn make_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sublink: &SubLink<'mcx>,
    is_top_qual: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let orig = sublink
        .subselect
        .as_query()
        .expect("make_subplan on an untransformed sublink");
    let mut subquery = query_cells_copy(mcx, orig)?;

    let tuple_fraction = match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            simplify_exists_query(run, &mut subquery)?;
            1.0
        }
        SubLinkType::EXPR_SUBLINK => 0.0,
        other => panic!(
            "make_subplan (subselect.c): {other:?} sublink; M2 sublink lane"
        ),
    };

    debug_assert!(run.root.plan_params.is_empty());
    run.push_root()?;
    crate::subquery::subquery_planner(run, subquery, tuple_fraction)?;

    let final_rel = fetch_final_rel(run);
    let best_path = get_cheapest_fractional_path(run, final_rel, tuple_fraction);
    let plan = create_plan(run, best_path)?;
    run.pop_root_to_subroot();
    // Correlated references park plan_params on the parent root (loud upstream).
    debug_assert!(run.root.plan_params.is_empty());

    build_subplan(run, plan, sublink.subLinkType, is_top_qual)
}

// build_subplan (subselect.c), parParam==NIL EXISTS/EXPR initplan arms only.
fn build_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    sub_link_type: SubLinkType,
    unknown_eq_false: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(plan);
    let parallel_safe = plan.as_plan().expect("plan node").parallel_safe;

    let (prm, prm_node) = match sub_link_type {
        SubLinkType::EXISTS_SUBLINK => generate_new_exec_param(run, BOOLOID, -1, 0)?,
        SubLinkType::EXPR_SUBLINK => {
            let te = plan
                .as_plan()
                .unwrap()
                .targetlist
                .first()
                .expect("EXPR subplan tlist")
                .as_target_entry()
                .expect("tlist entry");
            debug_assert!(!te.resjunk);
            let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
            generate_new_exec_param(run, ty, tm, crate::pathkeys::expr_collation(te.expr))?
        }
        other => panic!("build_subplan (subselect.c): {other:?}; M2 sublink lane"),
    };

    run.glob.subplans.lappend(mcx, plan)?;
    let plan_id = run.glob.subplans.len() as i32;
    debug_assert_eq!(run.subroots.len(), run.glob.subplans.len());

    let mut splan = SubPlan {
        subLinkType: sub_link_type,
        testexpr: None,
        paramIds: IntList::nil(),
        plan_id,
        plan_name: Some(str_in(mcx, &format!("InitPlan {plan_id}"))?),
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: unknown_eq_false,
        parallel_safe,
        setParam: IntList::make1(mcx, prm.paramid)?,
        parParam: IntList::nil(),
        args: NodeList::nil(),
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };
    cost_subplan(&mut splan, plan);
    let splan_node = Node::mk(mcx, splan)?;
    let splan_id = run.intern_expr(splan_node);
    run.root.init_plans.push(splan_id);

    Ok(prm_node)
}

/// generate_new_exec_param (paramassign.c).
pub fn generate_new_exec_param<'mcx>(
    run: &mut PlannerRun<'mcx>,
    paramtype: types_core::Oid,
    paramtypmod: i32,
    paramcollation: types_core::Oid,
) -> PgResult<(Param, Node<'mcx>)> {
    let paramid = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(run.mcx, paramtype)?;
    let prm = Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid,
        paramtype,
        paramtypmod,
        paramcollid: paramcollation,
        location: -1,
    };
    Ok((prm, Node::mk(run.mcx, prm)?))
}

pub(crate) fn get_first_col_type(plan: Node<'_>) -> (types_core::Oid, i32, types_core::Oid) {
    if let Some(first) = plan.as_plan().expect("plan node").targetlist.first() {
        let tent = first.as_target_entry().expect("tlist entry");
        if !tent.resjunk {
            let (ty, tm) = crate::costsize::expr_type_typmod(tent.expr);
            return (ty, tm, crate::pathkeys::expr_collation(tent.expr));
        }
    }
    (VOIDOID, -1, 0)
}

// cost_subplan (costsize.c), initplan slice (NULL testexpr: qual costs drop out).
pub(crate) fn cost_subplan<'mcx>(splan: &mut SubPlan<'mcx>, plan: Node<'mcx>) {
    let p = plan.as_plan().expect("plan node");
    let mut startup = 0.0;
    let mut per_tuple = 0.0;
    let plan_run_cost = p.total_cost - p.startup_cost;
    match splan.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            per_tuple += plan_run_cost / crate::costsize::clamp_row_est(p.plan_rows);
        }
        SubLinkType::ALL_SUBLINK | SubLinkType::ANY_SUBLINK => {
            unreachable!("ALL/ANY subplans are loud upstream")
        }
        _ => per_tuple += plan_run_cost,
    }
    if splan.parParam.is_nil() && exec_materializes_output(plan.node_tag()) {
        startup += p.startup_cost;
    } else {
        per_tuple += p.startup_cost;
    }
    splan.startup_cost = startup;
    splan.per_call_cost = per_tuple;
}

// ExecMaterializesOutput (execAmi.c) over the ported node set.
fn exec_materializes_output(tag: NodeTag) -> bool {
    matches!(tag, NodeTag::T_Sort | NodeTag::T_Material)
}

// simplify_EXISTS_query (subselect.c).
fn simplify_exists_query<'mcx>(run: &mut PlannerRun<'mcx>, query: &mut Query<'mcx>) -> PgResult<bool> {
    if query.commandType != types_nodes::CmdType::CMD_SELECT
        || query.setOperations.is_some()
        || query.hasAggs
        || !query.groupingSets.is_nil()
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasModifyingCTE
        || query.havingQual.is_some()
        || query.limitOffset.is_some()
        || !query.rowMarks.is_nil()
    {
        return Ok(false);
    }
    if let Some(limit) = query.limitCount {
        let node = clauses::eval_const_expressions_with_params(
            run.mcx,
            limit,
            run.glob.bound_params,
        )?;
        query.limitCount = Some(node);
        let Some(c) = node.as_const() else { return Ok(false) };
        debug_assert_eq!(c.consttype, types_core::catalog::INT8OID);
        if !c.constisnull && c.constvalue.as_i64() <= 0 {
            return Ok(false);
        }
        query.limitCount = None;
    }
    query.targetList = NodeList::nil();
    query.groupClause = NodeList::nil();
    query.windowClause = NodeList::nil();
    query.distinctClause = NodeList::nil();
    query.sortClause = NodeList::nil();
    query.hasDistinctOn = false;
    if query.hasGroupRTE {
        panic!("simplify_EXISTS_query (subselect.c): RTE_GROUP removal; M2 grouping lane");
    }
    Ok(true)
}

// The scribble copy for make_subplan: struct fields plus list cells; nodes
// stay shared (see make_subplan comment).
pub(crate) fn query_cells_copy<'mcx>(mcx: Mcx<'mcx>, q: &Query<'mcx>) -> PgResult<Query<'mcx>> {
    Ok(Query {
        commandType: q.commandType,
        querySource: q.querySource,
        queryId: q.queryId,
        canSetTag: q.canSetTag,
        utilityStmt: q.utilityStmt,
        resultRelation: q.resultRelation,
        hasAggs: q.hasAggs,
        hasWindowFuncs: q.hasWindowFuncs,
        hasTargetSRFs: q.hasTargetSRFs,
        hasSubLinks: q.hasSubLinks,
        hasDistinctOn: q.hasDistinctOn,
        hasRecursive: q.hasRecursive,
        hasModifyingCTE: q.hasModifyingCTE,
        hasForUpdate: q.hasForUpdate,
        hasRowSecurity: q.hasRowSecurity,
        hasGroupRTE: q.hasGroupRTE,
        isReturn: q.isReturn,
        cteList: q.cteList.clone_in(mcx)?,
        rtable: q.rtable.clone_in(mcx)?,
        rteperminfos: q.rteperminfos.clone_in(mcx)?,
        jointree: q.jointree,
        mergeActionList: q.mergeActionList.clone_in(mcx)?,
        mergeTargetRelation: q.mergeTargetRelation,
        mergeJoinCondition: q.mergeJoinCondition,
        targetList: q.targetList.clone_in(mcx)?,
        r#override: q.r#override,
        onConflict: q.onConflict,
        returningOldAlias: q.returningOldAlias,
        returningNewAlias: q.returningNewAlias,
        returningList: q.returningList.clone_in(mcx)?,
        groupClause: q.groupClause.clone_in(mcx)?,
        groupDistinct: q.groupDistinct,
        groupingSets: q.groupingSets.clone_in(mcx)?,
        havingQual: q.havingQual,
        windowClause: q.windowClause.clone_in(mcx)?,
        distinctClause: q.distinctClause.clone_in(mcx)?,
        sortClause: q.sortClause.clone_in(mcx)?,
        limitOffset: q.limitOffset,
        limitCount: q.limitCount,
        limitOption: q.limitOption,
        rowMarks: q.rowMarks.clone_in(mcx)?,
        setOperations: q.setOperations,
        constraintDeps: q.constraintDeps.clone_in(mcx)?,
        withCheckOptions: q.withCheckOptions.clone_in(mcx)?,
        stmt_location: q.stmt_location,
        stmt_len: q.stmt_len,
    })
}

/// SS_replace_correlation_vars (subselect.c): the uncorrelated lane proves no
/// uplevel Var exists (replace_outer_var parks correlation on the parent's
/// plan_params — M2 correlated-subquery lane).
pub fn ss_replace_correlation_vars<'mcx>(expr: Node<'mcx>) -> PgResult<Node<'mcx>> {
    if contains_uplevel_var(expr)? {
        panic!("replace_outer_var (paramassign.c): correlated subquery; M2 sublink lane");
    }
    Ok(expr)
}

struct ContainsUplevel;
impl<'mcx> clauses::NodeWalker<'mcx> for ContainsUplevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(v) = node.as_var() {
            return Ok(v.varlevelsup > 0);
        }
        if let Some(a) = node.as_aggref() {
            if a.agglevelsup > 0 {
                panic!("replace_outer_agg (paramassign.c): uplevel Aggref; M2 sublink lane");
            }
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn contains_uplevel_var(expr: Node<'_>) -> PgResult<bool> {
    ContainsUplevel.visit(expr)
}

/// SS_charge_for_initplans (subselect.c).
pub fn ss_charge_for_initplans(run: &mut PlannerRun<'_>, final_rel: RelId) -> PgResult<()> {
    if run.root.init_plans.is_empty() {
        return Ok(());
    }
    let mut initplan_cost = 0.0;
    let mut unsafe_initplans = false;
    for &ipid in run.root.init_plans.iter() {
        let sp = run
            .root
            .expr_node(ipid)
            .as_sub_plan()
            .expect("init_plans holds SubPlan nodes");
        initplan_cost += sp.startup_cost + sp.per_call_cost;
        if !sp.parallel_safe {
            unsafe_initplans = true;
        }
    }
    let path_ids: mcx::PgVec<'_, types_pathnodes::PathId> = {
        let mut v = mcx::PgVec::new_in(run.mcx);
        v.extend(run.root.rel(final_rel).pathlist.iter().copied());
        v
    };
    for pid in path_ids.iter() {
        let p = run.root.path_mut(*pid).base_mut();
        p.startup_cost += initplan_cost;
        p.total_cost += initplan_cost;
        if unsafe_initplans {
            p.parallel_safe = false;
        }
    }
    if unsafe_initplans {
        let rel = run.root.rel_mut(final_rel);
        rel.partial_pathlist.clear();
        rel.consider_parallel = false;
    } else {
        let partial: mcx::PgVec<'_, types_pathnodes::PathId> = {
            let mut v = mcx::PgVec::new_in(run.mcx);
            v.extend(run.root.rel(final_rel).partial_pathlist.iter().copied());
            v
        };
        for pid in partial.iter() {
            let p = run.root.path_mut(*pid).base_mut();
            p.startup_cost += initplan_cost;
            p.total_cost += initplan_cost;
        }
    }
    Ok(())
}

/// SS_attach_initplans (subselect.c): the current level's initplans move onto
/// the topmost plan node.
pub fn ss_attach_initplans<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<()> {
    if run.root.init_plans.is_empty() {
        return Ok(());
    }
    let mut list = NodeList::nil();
    for &ipid in run.root.init_plans.iter() {
        list.lappend(run.mcx, *run.root.expr_node(ipid))?;
    }
    // SAFETY: createplan exclusively owns the just-built tree (C assigns
    // plan->initPlan in place).
    unsafe { plan.with_plan_mut(|p| p.initPlan = list) }.expect("plan node");
    Ok(())
}

/// SS_finalize_plan (subselect.c): compute extParam/allParam for every node.
pub fn ss_finalize_plan<'mcx>(
    run: &PlannerRun<'mcx>,
    plan: Node<'mcx>,
    outer_params: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    // Planner-arena set -> nodes-side bitmapset, converted once at the boundary.
    let mut valid = types_nodes::bitmapset::Bitmapset::empty();
    if let Some(b) = outer_params {
        for (i, w) in b.words.iter().enumerate() {
            let mut w = *w;
            while w != 0 {
                let bit = w.trailing_zeros();
                valid.add_member(run.mcx, (i as i32) * 64 + bit as i32)?;
                w &= w - 1;
            }
        }
    }
    finalize_plan(run, plan, &valid)?;
    Ok(())
}

// finalize_plan (subselect.c) over the ported node set; gather_param and
// scan_params legs (parallel, EPQ) are dead here.
fn finalize_plan<'mcx>(
    run: &PlannerRun<'mcx>,
    plan: Node<'mcx>,
    valid_params: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<types_nodes::bitmapset::Bitmapset<'mcx>> {
    let mcx = run.mcx;
    let mut paramids = types_nodes::bitmapset::Bitmapset::empty();
    let base = plan.as_plan().expect("plan node");

    let mut init_ext_param = types_nodes::bitmapset::Bitmapset::empty();
    let mut init_set_param = types_nodes::bitmapset::Bitmapset::empty();
    for ip in &base.initPlan {
        let sp = ip.as_sub_plan().expect("initPlan cell is a SubPlan");
        let initplan = run
            .glob
            .subplans
            .nth((sp.plan_id - 1) as usize);
        init_ext_param.add_members(mcx, &initplan.as_plan().expect("plan node").extParam)?;
        for id in sp.setParam.iter() {
            init_set_param.add_member(mcx, id)?;
        }
    }
    let mut valid = valid_params.clone_in(mcx)?;
    valid.add_members(mcx, &init_set_param)?;

    finalize_primnode_list(run, &base.targetlist, &mut paramids)?;
    finalize_primnode_list(run, &base.qual, &mut paramids)?;
    debug_assert!(!base.parallel_aware, "gather_param leg; M3 parallel lane");

    match plan.node_tag() {
        NodeTag::T_Result => {
            if let Some(rcq) = plan.as_result().unwrap().resconstantqual {
                finalize_primnode(run, rcq, &mut paramids)?;
            }
        }
        NodeTag::T_SeqScan | NodeTag::T_Sort | NodeTag::T_Agg | NodeTag::T_Material => {}
        // cteParam is linkage only; the CTE plan's extParam matters (C bug #4902).
        NodeTag::T_CteScan => {
            let plan_id = plan.as_cte_scan().unwrap().ctePlanId;
            assert!(
                plan_id >= 1 && plan_id as usize <= run.glob.subplans.len(),
                "could not find plan for CteScan referencing plan ID {plan_id}"
            );
            let cteplan = run.glob.subplans.nth((plan_id - 1) as usize);
            paramids.add_members(mcx, &cteplan.as_plan().expect("plan node").extParam)?;
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            finalize_primnode_list(run, &s.indexqual, &mut paramids)?;
            finalize_primnode_list(run, &s.indexorderby, &mut paramids)?;
        }
        NodeTag::T_BitmapIndexScan => {
            finalize_primnode_list(
                run,
                &plan.as_bitmap_index_scan().unwrap().indexqual,
                &mut paramids,
            )?;
        }
        NodeTag::T_BitmapHeapScan => {
            finalize_primnode_list(
                run,
                &plan.as_bitmap_heap_scan().unwrap().bitmapqualorig,
                &mut paramids,
            )?;
        }
        NodeTag::T_BitmapAnd => {
            for sub in &plan.as_bitmap_and().unwrap().bitmapplans {
                let child = finalize_plan(run, sub, &valid)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_BitmapOr => {
            for sub in &plan.as_bitmap_or().unwrap().bitmapplans {
                let child = finalize_plan(run, sub, &valid)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            if let Some(off) = l.limitOffset {
                finalize_primnode(run, off, &mut paramids)?;
            }
            if let Some(cnt) = l.limitCount {
                finalize_primnode(run, cnt, &mut paramids)?;
            }
        }
        NodeTag::T_NestLoop => {
            let nl = plan.as_nest_loop().unwrap();
            debug_assert!(nl.nestParams.is_nil());
            finalize_primnode_list(run, &nl.join.joinqual, &mut paramids)?;
        }
        NodeTag::T_ModifyTable => {
            panic!("finalize_plan (subselect.c): ModifyTable with exec params; M2 DML lane")
        }
        other => panic!("finalize_plan (subselect.c): {other:?}; M2 plan lane"),
    }

    if let Some(child) = base.lefttree {
        let child_params = finalize_plan(run, child, &valid)?;
        paramids.add_members(mcx, &child_params)?;
    }
    if let Some(child) = base.righttree {
        let child_params = finalize_plan(run, child, &valid)?;
        paramids.add_members(mcx, &child_params)?;
    }

    assert!(
        paramids.is_subset(&valid),
        "plan should not reference subplan's variable"
    );

    let mut all_param = paramids.clone_in(mcx)?;
    all_param.add_members(mcx, &init_ext_param)?;
    all_param.add_members(mcx, &init_set_param)?;
    let mut ext_param = paramids.clone_in(mcx)?;
    ext_param.add_members(mcx, &init_ext_param)?;
    ext_param.del_members(&init_set_param);
    // SAFETY: the plan tree is exclusively owned by this planning invocation
    // (C writes the same fields in place).
    unsafe {
        plan.with_plan_mut(|p| {
            p.extParam = ext_param;
            p.allParam = all_param;
        })
    }
    .expect("plan node");
    Ok(paramids)
}

fn finalize_primnode_list<'mcx>(
    run: &PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    for node in list {
        finalize_primnode(run, node, paramids)?;
    }
    Ok(())
}

struct FinalizePrimnode<'a, 'mcx> {
    run: &'a PlannerRun<'mcx>,
    paramids: &'a mut types_nodes::bitmapset::Bitmapset<'mcx>,
}

impl<'a, 'mcx> clauses::NodeWalker<'mcx> for FinalizePrimnode<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            if p.paramkind == ParamKind::PARAM_EXEC {
                self.paramids.add_member(self.run.mcx, p.paramid)?;
            }
            return Ok(false);
        }
        if node.node_tag() == NodeTag::T_SubPlan {
            panic!("finalize_primnode (subselect.c): in-expression SubPlan; M2 sublink lane");
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn finalize_primnode<'mcx>(
    run: &PlannerRun<'mcx>,
    node: Node<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    FinalizePrimnode { run, paramids }.visit(node)?;
    Ok(())
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}
