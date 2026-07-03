//! costsize.c slice: cost_seqscan/cost_index, cost_qual_eval,
//! set_baserel_size_estimates/set_rel_width, index_pages_fetched.

use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{NodeId, PathNode, QualCost, RelId, RinfoId, RTE_RELATION};

use crate::gucs;
use crate::run::PlannerRun;

pub const MAXIMUM_ROWCOUNT: f64 = 1e100;
const MAX_ALLOC_SIZE: i64 = 0x3fffffff;

pub fn clamp_row_est(nrows: f64) -> f64 {
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round_ties_even()
    }
}

pub fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > MAX_ALLOC_SIZE {
        return MAX_ALLOC_SIZE as i32;
    }
    debug_assert!(tuple_width >= 0);
    tuple_width as i32
}

// get_tablespace_page_costs (spccache.c): reloptions unported, so every
// tablespace reads the GUC defaults (divergence owned by the spccache unit).
pub fn get_tablespace_page_costs(_spcid: u32) -> (f64, f64) {
    (gucs::random_page_cost(), gucs::seq_page_cost())
}

// cost_qual_eval (costsize.c) with the rinfo->eval_cost cache (lesson 10).
pub fn cost_qual_eval(run: &mut PlannerRun<'_>, quals: &[RinfoId]) -> PgResult<QualCost> {
    let mut total = QualCost::default();
    for &rid in quals {
        let cached = run.root.rinfo(rid).eval_cost;
        let cost = if cached.startup >= 0.0 {
            cached
        } else {
            if run.root.rinfo(rid).orclause.is_some() {
                panic!("cost_qual_eval_walker (costsize.c): orclause; M2 OR lane");
            }
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let mut cost = QualCost::default();
            cost_qual_eval_walker(clause, &mut cost)?;
            if run.root.rinfo(rid).pseudoconstant {
                cost.startup += cost.per_tuple;
                cost.per_tuple = 0.0;
            }
            run.root.rinfo_mut(rid).eval_cost = cost;
            cost
        };
        total.startup += cost.startup;
        total.per_tuple += cost.per_tuple;
    }
    Ok(total)
}
pub fn cost_qual_eval_node(node: Node<'_>) -> PgResult<QualCost> {
    let mut cost = QualCost::default();
    cost_qual_eval_walker(node, &mut cost)?;
    Ok(cost)
}

fn cost_qual_eval_walker(node: Node<'_>, cost: &mut QualCost) -> PgResult<()> {
    match node.node_tag() {
        // SQLValueFunction: no explicit C case; childless leaf, no charge.
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr => Ok(()),
        // C charges nothing for Aggref/WindowFunc themselves and does not
        // descend: their costs are get_agg_clause_costs'/cost_windowagg's job.
        NodeTag::T_Aggref | NodeTag::T_WindowFunc => Ok(()),
        NodeTag::T_GroupingFunc => {
            cost.per_tuple += gucs::cpu_operator_cost();
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            crate::plancat::add_function_cost(f.funcid, cost)?;
            for arg in &f.args {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // set_opfuncid memo write-back is unmodeled (walker.rs note).
            let opfuncid = if o.opfuncid != 0 { o.opfuncid } else { lsyscache::get_opcode(o.opno)? };
            crate::plancat::add_function_cost(opfuncid, cost)?;
            for arg in &o.args {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            cost_qual_eval_walker(node.as_relabel_type().unwrap().arg, cost)
        }
        // C charges both I/O functions of the coercion.
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            let (infunc, _) = lsyscache::getTypeInputInfo(c.resulttype)?;
            crate::plancat::add_function_cost(infunc, cost)?;
            let (outfunc, _) = lsyscache::getTypeOutputInfo(expr_type_typmod(c.arg).0)?;
            crate::plancat::add_function_cost(outfunc, cost)?;
            cost_qual_eval_walker(c.arg, cost)
        }
        NodeTag::T_CoerceToDomain => {
            cost_qual_eval_walker(node.as_coerce_to_domain().unwrap().arg, cost)
        }
        // Boolean connectives are free in C; NullTest is "cheap" (no charge).
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for arg in node.as_list().unwrap() {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            let opfuncid = if sa.opfuncid == 0 {
                lsyscache::get_opcode(sa.opno)?
            } else {
                sa.opfuncid
            };
            if sa.hashfuncid != 0 {
                panic!("cost_qual_eval_walker (costsize.c): hashed SAOP; M2 lane");
            }
            let arraynode = sa.args.nth(1);
            let mut sacosts = QualCost { startup: 0.0, per_tuple: 0.0 };
            crate::plancat::add_function_cost(opfuncid, &mut sacosts)?;
            // C: the operator runs against about half the array elements.
            cost.startup += sacosts.startup;
            cost.per_tuple +=
                sacosts.per_tuple * crate::selfuncs::estimate_array_length(arraynode) * 0.5;
            for arg in sa.args.iter() {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                cost_qual_eval_walker(e, cost)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => cost_qual_eval_walker(arg, cost),
            None => Ok(()),
        },
        // C charges DistinctExpr like OpExpr; BooleanTest itself is free.
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid = if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            crate::plancat::add_function_cost(opfuncid, cost)?;
            for arg in &d.args {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => cost_qual_eval_walker(arg, cost),
            None => Ok(()),
        },
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                cost_qual_eval_walker(arg, cost)?;
            }
            Ok(())
        }
        // C arbitrarily uses the first alternative's cost.
        NodeTag::T_AlternativeSubPlan => {
            let asp = node.as_alternative_sub_plan().unwrap();
            cost_qual_eval_walker(asp.subplans.first().expect("alternatives"), cost)
        }
        // The SubPlan's own costs, precomputed by cost_subplan; C does not
        // descend into the testexpr (already included) or args.
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            cost.startup += sp.startup_cost;
            cost.per_tuple += sp.per_call_cost;
            Ok(())
        }
        // C's default arm: CASE itself is free, children are charged.
        NodeTag::T_CaseTestExpr => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                cost_qual_eval_walker(a, cost)?;
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                cost_qual_eval_walker(cw.expr.expect("CaseWhen.expr"), cost)?;
                cost_qual_eval_walker(cw.result.expect("CaseWhen.result"), cost)?;
            }
            match c.defresult {
                Some(d) => cost_qual_eval_walker(d, cost),
                None => Ok(()),
            }
        }
        NodeTag::T_CoerceToDomainValue => Ok(()),
        other => panic!("cost_qual_eval_walker (costsize.c): {other:?}; M2 expression lane"),
    }
}
fn get_restriction_qual_cost(run: &PlannerRun<'_>, rel: RelId) -> QualCost {
    run.root.rel(rel).baserestrictcost
}
pub fn cost_seqscan(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId, rel: RelId) {
    let (relid, rtekind, reltablespace, pages, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.reltablespace, baserel.pages, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_seqscan (costsize.c): parameterized path; M2 lateral lane"
    );
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let (_, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    let disk_run_cost = spc_seq_page_cost * pages as f64;

    let qpqual_cost = get_restriction_qual_cost(run, rel);
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut cpu_run_cost = cpu_per_tuple * tuples;

    // tlist eval costs are paid per output row, not per scanned tuple.
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    cpu_run_cost += target.cost.per_tuple * rows;
    debug_assert!(run.root.path(path_id).base().parallel_workers == 0);

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if gucs::enable_seqscan() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

// cost_functionscan (costsize.c): function eval is all startup cost (the
// executor materializes into a tuplestore before returning rows).
pub fn cost_functionscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_FUNCTION);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_functionscan (costsize.c): parameterized path; M2 lateral lane"
    );
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let mut exprcost = QualCost::default();
    for rtfunc_node in &run.rte(relid as usize).functions {
        let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
        if let Some(fexpr) = rtfunc.funcexpr {
            cost_qual_eval_walker(fexpr, &mut exprcost)?;
        }
    }
    startup_cost += exprcost.startup + exprcost.per_tuple;

    let qpqual_cost = get_restriction_qual_cost(run, rel);
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_ctescan (costsize.c): 2× cpu_tuple_cost per scanned tuple (scan +
// tuplestore); the CTE query itself is charged as initplan cost, not here.
pub fn cost_ctescan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_CTE);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_ctescan (costsize.c): parameterized path; M2 lateral lane"
    );
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let mut cpu_per_tuple = gucs::cpu_tuple_cost();

    let qpqual_cost = get_restriction_qual_cost(run, rel);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_cte_size_estimates (costsize.c); self-reference worktable arm loud upstream.
pub fn set_cte_size_estimates(run: &mut PlannerRun<'_>, rel: RelId, cte_rows: f64) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    run.root.rel_mut(rel).tuples = cte_rows;
    set_baserel_size_estimates(run, rel)
}

// set_values_size_estimates (costsize.c): tuples = row count of the list.
pub fn set_values_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    debug_assert!(rti > 0);
    debug_assert_eq!(run.rte(rti).rtekind, types_nodes::parsenodes::RTEKind::RTE_VALUES);
    run.root.rel_mut(rel).tuples = run.rte(rti).values_lists.len() as f64;
    set_baserel_size_estimates(run, rel)
}

// cost_valuesscan (costsize.c): one cpu_operator_cost per list evaluation.
pub fn cost_valuesscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_VALUES);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_valuesscan (costsize.c): parameterized path; M2 lateral lane"
    );
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let mut cpu_per_tuple = gucs::cpu_operator_cost();

    let qpqual_cost = get_restriction_qual_cost(run, rel);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_function_size_estimates (costsize.c).
pub fn set_function_size_estimates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    let mut funcexprs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(run.mcx);
    for rtfunc_node in &run.rte(rti).functions {
        let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
        if let Some(fexpr) = rtfunc.funcexpr {
            funcexprs.push(fexpr);
        }
    }
    let mut tuples = 0.0f64;
    for &fexpr in funcexprs.iter() {
        let ntup = expression_returns_set_rows(fexpr)?;
        if ntup > tuples {
            tuples = ntup;
        }
    }
    run.root.rel_mut(rel).tuples = tuples;
    set_baserel_size_estimates(run, rel)
}

// expression_returns_set_rows (clauses.c); the OpExpr opretset arm is dead
// (no set-returning operators resolve on this lane).
pub(crate) fn expression_returns_set_rows(clause: Node<'_>) -> PgResult<f64> {
    if let Some(fe) = clause.as_func_expr() {
        if fe.funcretset {
            return Ok(clamp_row_est(crate::plancat::get_function_rows(
                fe.funcid,
                Some(clause),
            )?));
        }
    }
    Ok(1.0)
}

// cost_index (costsize.c); nestloop loop_count and partial paths are loud.
pub fn cost_index(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId, loop_count: f64) -> PgResult<()> {
    assert!(loop_count == 1.0, "cost_index (costsize.c): loop_count > 1; M2 join lane");

    let (baserel_id, indexonly, index_total_pages, indrestrictinfo) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("cost_index: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        (
            index.rel.expect("index rel set"),
            ip.path.pathtype == crate::pathnode::tag16(NodeTag::T_IndexOnlyScan),
            index.pages,
            index.indrestrictinfo.borrow().clone(),
        )
    };
    {
        let baserel = run.root.rel(baserel_id);
        debug_assert!(baserel.relid > 0 && baserel.rtekind == RTE_RELATION);
    }
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_index (costsize.c): parameterized path; M2 join lane"
    );

    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;
    let mut cpu_run_cost = 0.0;

    // qpquals: restrictions not redundant with the index clauses.
    let indexclause_rinfos: mcx::PgVec<'_, (RinfoId, bool)> = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let mut v = mcx::PgVec::new_in(run.mcx);
        for ic in ip.indexclauses.iter() {
            v.push((ic.rinfo.expect("IndexClause rinfo"), ic.lossy));
        }
        v
    };
    let mut qpquals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    for &rid in indrestrictinfo.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        // is_redundant_with_indexclauses: no EC parents, so rinfo identity;
        // a lossy indexclause does not enforce the condition exactly.
        if indexclause_rinfos.iter().any(|&(c, lossy)| c == rid && !lossy) {
            continue;
        }
        qpquals.push(rid);
    }

    let new_rows = run.root.rel(baserel_id).rows;
    run.root.path_mut(path_id).base_mut().rows = new_rows;
    run.root.path_mut(path_id).base_mut().disabled_nodes =
        if gucs::enable_indexscan() { 0 } else { 1 };

    let am = crate::selfuncs::amcostestimate(run, path_id, loop_count)?;
    if let PathNode::IndexPath(ip) = run.root.path_mut(path_id) {
        ip.indextotalcost = am.index_total_cost;
        ip.indexselectivity = am.index_selectivity;
    }
    startup_cost += am.index_startup_cost;
    run_cost += am.index_total_cost - am.index_startup_cost;

    let (baserel_tuples, baserel_pages, baserel_allvisfrac, reltablespace) = {
        let baserel = run.root.rel(baserel_id);
        (baserel.tuples, baserel.pages, baserel.allvisfrac, baserel.reltablespace)
    };
    let tuples_fetched = clamp_row_est(am.index_selectivity * baserel_tuples);
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);

    let mut pages_fetched =
        index_pages_fetched(run, tuples_fetched, baserel_pages, index_total_pages as f64);
    if indexonly {
        pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
    }
    let max_io_cost = pages_fetched * spc_random_page_cost;

    pages_fetched = (am.index_selectivity * baserel_pages as f64).ceil();
    if indexonly {
        pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
    }
    let min_io_cost = if pages_fetched > 0.0 {
        let mut m = spc_random_page_cost;
        if pages_fetched > 1.0 {
            m += (pages_fetched - 1.0) * spc_seq_page_cost;
        }
        m
    } else {
        0.0
    };

    let csquared = am.index_correlation * am.index_correlation;
    run_cost += max_io_cost + csquared * (min_io_cost - max_io_cost);

    let qpqual_cost = cost_qual_eval(run, &qpquals)?;
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    cpu_run_cost += cpu_per_tuple * tuples_fetched;

    let path_rows = run.root.path(path_id).base().rows;
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    cpu_run_cost += target.cost.per_tuple * path_rows;

    debug_assert!(run.root.path(path_id).base().parallel_workers == 0);
    run_cost += cpu_run_cost;

    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_bitmap_tree_node (costsize.c): (cost, selectivity) of a bitmapqual.
pub fn cost_bitmap_tree_node(
    run: &PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
) -> (f64, f64) {
    match run.root.path(path_id) {
        PathNode::IndexPath(ip) => (
            // Per-tuple bitmap-manipulation charge: a one-tuple bitmap scan
            // must not tie the plain indexscan.
            ip.indextotalcost + 0.1 * gucs::cpu_operator_cost() * ip.path.rows,
            ip.indexselectivity,
        ),
        PathNode::BitmapAndPath(ap) => (ap.path.total_cost, ap.bitmapselectivity),
        PathNode::BitmapOrPath(op) => (op.path.total_cost, op.bitmapselectivity),
        other => panic!(
            "cost_bitmap_tree_node (costsize.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

// cost_bitmap_and_node (costsize.c): AND selectivity assumes independent
// inputs; 100x cpu_operator_cost per tbm_intersect.
pub fn cost_bitmap_and_node(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let subs = {
        let PathNode::BitmapAndPath(p) = run.root.path(path_id) else { unreachable!() };
        p.bitmapquals.clone()
    };
    let mut total_cost = 0.0;
    let mut selec = 1.0;
    for (i, &sub) in subs.iter().enumerate() {
        let (sub_cost, sub_selec) = cost_bitmap_tree_node(run, sub);
        selec *= sub_selec;
        total_cost += sub_cost;
        if i > 0 {
            total_cost += 100.0 * gucs::cpu_operator_cost();
        }
    }
    let PathNode::BitmapAndPath(p) = run.root.path_mut(path_id) else { unreachable!() };
    p.bitmapselectivity = selec;
    p.path.rows = 0.0;
    p.path.disabled_nodes = 0;
    p.path.startup_cost = total_cost;
    p.path.total_cost = total_cost;
}

// cost_bitmap_or_node (costsize.c): OR selectivity assumes non-overlapping
// inputs, clamped to 1; tbm_unions are free when the input is an IndexPath.
pub fn cost_bitmap_or_node(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let subs = {
        let PathNode::BitmapOrPath(p) = run.root.path(path_id) else { unreachable!() };
        p.bitmapquals.clone()
    };
    let mut total_cost = 0.0;
    let mut selec = 0.0;
    for (i, &sub) in subs.iter().enumerate() {
        let (sub_cost, sub_selec) = cost_bitmap_tree_node(run, sub);
        selec += sub_selec;
        total_cost += sub_cost;
        if i > 0 && !matches!(run.root.path(sub), PathNode::IndexPath(_)) {
            total_cost += 100.0 * gucs::cpu_operator_cost();
        }
    }
    let PathNode::BitmapOrPath(p) = run.root.path_mut(path_id) else { unreachable!() };
    p.bitmapselectivity = selec.min(1.0);
    p.path.rows = 0.0;
    p.path.startup_cost = total_cost;
    p.path.total_cost = total_cost;
}

fn get_indexpath_pages(run: &PlannerRun<'_>, path_id: types_pathnodes::PathId) -> f64 {
    match run.root.path(path_id) {
        PathNode::IndexPath(ip) => {
            ip.indexinfo.as_ref().expect("indexinfo set").pages as f64
        }
        PathNode::BitmapAndPath(p) => {
            p.bitmapquals.clone().iter().map(|&q| get_indexpath_pages(run, q)).sum()
        }
        PathNode::BitmapOrPath(p) => {
            p.bitmapquals.clone().iter().map(|&q| get_indexpath_pages(run, q)).sum()
        }
        other => panic!(
            "get_indexpath_pages (costsize.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

// compute_bitmap_pages (costsize.c) -> (pages_fetched, cost, tuples_fetched).
pub fn compute_bitmap_pages(
    run: &PlannerRun<'_>,
    rel: RelId,
    bitmapqual: types_pathnodes::PathId,
    loop_count: f64,
) -> (f64, f64, f64) {
    let (index_total_cost, index_selectivity) = cost_bitmap_tree_node(run, bitmapqual);
    let (pages, tuples) = {
        let baserel = run.root.rel(rel);
        (baserel.pages, baserel.tuples)
    };
    let mut tuples_fetched = clamp_row_est(index_selectivity * tuples);
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let mut pages_fetched = (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched);
    let heap_pages = pages_fetched.min(pages as f64);
    let maxentries =
        tidbitmap::tbm_calculate_entries(init_small::globals::work_mem() as usize * 1024) as f64;
    if loop_count > 1.0 {
        pages_fetched = index_pages_fetched(
            run,
            tuples_fetched * loop_count,
            pages,
            get_indexpath_pages(run, bitmapqual),
        );
        pages_fetched /= loop_count;
    }
    pages_fetched = if pages_fetched >= t { t } else { pages_fetched.ceil() };
    if maxentries < heap_pages {
        // tbm_lossify() sheds pages sharply once memory runs short; this
        // matches C's crude estimate of that shape.
        let lossy_pages = (heap_pages - maxentries / 2.0).max(0.0);
        let exact_pages = heap_pages - lossy_pages;
        if lossy_pages > 0.0 {
            tuples_fetched = clamp_row_est(
                index_selectivity * (exact_pages / heap_pages) * tuples
                    + (lossy_pages / heap_pages) * tuples,
            );
        }
    }
    (pages_fetched, index_total_cost, tuples_fetched)
}

// cost_bitmap_heap_scan (costsize.c); loop_count > 1 rides the join lane.
pub fn cost_bitmap_heap_scan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
    bitmapqual: types_pathnodes::PathId,
    loop_count: f64,
) {
    let (relid, rtekind, reltablespace, pages, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.reltablespace, baserel.pages, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_bitmap_heap_scan (costsize.c): parameterized path; M2 join lane"
    );
    let rows = base_rows;

    let (pages_fetched, index_total_cost, tuples_fetched) =
        compute_bitmap_pages(run, rel, bitmapqual, loop_count);

    let mut startup_cost = index_total_cost;
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    // Interpolate between random (few pages) and sequential (most of the
    // table) per-page cost, nonlinearly, as C.
    let cost_per_page = if pages_fetched >= 2.0 {
        spc_random_page_cost
            - (spc_random_page_cost - spc_seq_page_cost) * (pages_fetched / t).sqrt()
    } else {
        spc_random_page_cost
    };
    let mut run_cost = pages_fetched * cost_per_page;

    // Indexquals are assumed rechecked at every tuple (lossy bitmaps), so the
    // full scan-clause freight is charged.
    let qpqual_cost = get_restriction_qual_cost(run, rel);
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let cpu_run_cost = cpu_per_tuple * tuples_fetched;
    debug_assert!(run.root.path(path_id).base().parallel_workers == 0);
    run_cost += cpu_run_cost;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if gucs::enable_bitmapscan() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

// cost_agg (costsize.c), AGG_PLAIN/AGG_SORTED/AGG_HASHED arms.
#[allow(clippy::too_many_arguments)]
pub fn cost_agg(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    aggstrategy: u32,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[types_pathnodes::NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    input_width: i32,
) -> PgResult<()> {
    let (rows, disabled_nodes, startup_cost, total_cost) = cost_agg_shape(
        run,
        aggstrategy,
        aggcosts,
        num_group_cols,
        num_groups,
        quals,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        input_width,
    )?;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
    Ok(())
}

/// cost_agg without a Path to write into (C's dummy `Path agg_path` callers);
/// returns (rows, disabled_nodes, startup, total).
#[allow(clippy::too_many_arguments)]
pub fn cost_agg_shape(
    run: &mut PlannerRun<'_>,
    aggstrategy: u32,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[types_pathnodes::NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    input_width: i32,
) -> PgResult<(f64, i32, f64, f64)> {
    let mut disabled_nodes = input_disabled_nodes;

    let (mut startup_cost, mut total_cost, mut output_tuples);
    if aggstrategy == types_pathnodes::AGG_PLAIN {
        debug_assert!(num_group_cols == 0);
        startup_cost = input_total_cost;
        startup_cost += aggcosts.transCost.startup;
        startup_cost += aggcosts.transCost.per_tuple * input_tuples;
        startup_cost += aggcosts.finalCost.startup;
        startup_cost += aggcosts.finalCost.per_tuple;
        total_cost = startup_cost + gucs::cpu_tuple_cost();
        output_tuples = 1.0;
    } else if aggstrategy == types_pathnodes::AGG_SORTED {
        // Output is delivered on-the-fly, one group at a time.
        startup_cost = input_startup_cost;
        total_cost = input_total_cost;
        total_cost += aggcosts.transCost.startup;
        total_cost += aggcosts.transCost.per_tuple * input_tuples;
        total_cost += gucs::cpu_operator_cost() * num_group_cols as f64 * input_tuples;
        total_cost += aggcosts.finalCost.startup;
        total_cost += aggcosts.finalCost.per_tuple * num_groups;
        total_cost += gucs::cpu_tuple_cost() * num_groups;
        output_tuples = num_groups;
    } else if aggstrategy == types_pathnodes::AGG_HASHED {
        startup_cost = input_total_cost;
        if !gucs::enable_hashagg() {
            disabled_nodes += 1;
        }
        startup_cost += aggcosts.transCost.startup;
        startup_cost += aggcosts.transCost.per_tuple * input_tuples;
        startup_cost += gucs::cpu_operator_cost() * num_group_cols as f64 * input_tuples;
        startup_cost += aggcosts.finalCost.startup;
        total_cost = startup_cost;
        total_cost += aggcosts.finalCost.per_tuple * num_groups;
        total_cost += gucs::cpu_tuple_cost() * num_groups;
        output_tuples = num_groups;
    } else {
        panic!("cost_agg (costsize.c): AGG_MIXED; M3 grouping-sets lane");
    }

    if aggstrategy == types_pathnodes::AGG_HASHED {
        let hashentrysize = ::nodeagg::hash_agg_entry_size(
            run.root.aggtransinfos.len(),
            input_width.max(0) as usize,
            aggcosts.transitionSpace as usize,
        );
        let (mem_limit, ngroups_limit, num_partitions) =
            ::nodeagg::hash_agg_set_limits(hashentrysize, num_groups, 0);
        let nbatches = ((num_groups * hashentrysize) / mem_limit as f64)
            .max(num_groups / ngroups_limit as f64)
            .ceil()
            .max(1.0);
        let num_partitions = (num_partitions.max(2)) as f64;
        let depth = (nbatches.ln() / num_partitions.ln()).ceil();
        let pages = relation_byte_size(input_tuples, input_width) / BLCKSZ as f64;
        let pages_written = pages * depth * 2.0;
        let pages_read = pages_written;
        startup_cost += pages_written * gucs::random_page_cost();
        total_cost += pages_written * gucs::random_page_cost();
        total_cost += pages_read * gucs::seq_page_cost();
        let spill_cost = depth * input_tuples * 2.0 * gucs::cpu_tuple_cost();
        startup_cost += spill_cost;
        total_cost += spill_cost;
    }

    // HAVING quals: charged per output tuple, then filter selectivity.
    if !quals.is_empty() {
        let mut qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
        for &q in quals {
            let c = cost_qual_eval_node(*run.root.expr_node(q))?;
            qual_cost.startup += c.startup;
            qual_cost.per_tuple += c.per_tuple;
        }
        startup_cost += qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        // C passes the bare clauses; the transient RestrictInfo wrap feeds
        // the same restriction_selectivity legs.
        let mut rids: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
        for &q in quals {
            let clause = *run.root.expr_node(q);
            rids.push(crate::initsplan::make_restrictinfo(
                run, clause, true, false, false, false, 0, None, None, None,
            )?);
        }
        let sel = crate::clausesel::clauselist_selectivity(
            run,
            &rids,
            0,
            types_pathnodes::JOIN_INNER,
            None,
        )?;
        output_tuples = clamp_row_est(output_tuples * sel);
    }

    Ok((output_tuples, disabled_nodes, startup_cost, total_cost))
}

const BLCKSZ: usize = 8192;
const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

// relation_byte_size (costsize.c).
pub(crate) fn relation_byte_size(tuples: f64, width: i32) -> f64 {
    tuples * ((maxalign(width.max(0) as usize) + maxalign(SIZEOF_HEAP_TUPLE_HEADER)) as f64)
}

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// index_pages_fetched (costsize.c): the Mackert-Lohman formula.
pub fn index_pages_fetched(
    run: &PlannerRun<'_>,
    tuples_fetched: f64,
    pages: u32,
    index_pages: f64,
) -> f64 {
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let total_pages = (run.root.total_table_pages + index_pages).max(1.0);
    debug_assert!(t <= total_pages);

    let mut b = gucs::effective_cache_size() as f64 * t / total_pages;
    b = if b <= 1.0 { 1.0 } else { b.ceil() };

    if t <= b {
        let pf = (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched);
        if pf >= t {
            t
        } else {
            pf.ceil()
        }
    } else {
        let lim = (2.0 * t * b) / (2.0 * t - b);
        let pf = if tuples_fetched <= lim {
            (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched)
        } else {
            b + (tuples_fetched - lim) * (t - b) / t
        };
        pf.ceil()
    }
}

// set_baserel_size_estimates (costsize.c).
pub fn set_baserel_size_estimates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    let quals = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).baserestrictinfo);
    let selec = crate::clausesel::clauselist_selectivity(run, &quals, 0, types_pathnodes::JOIN_INNER, None)?;
    let nrows = run.root.rel(rel).tuples * selec;
    run.root.rel_mut(rel).rows = clamp_row_est(nrows);
    let qcost = cost_qual_eval(run, &quals)?;
    run.root.rel_mut(rel).baserestrictcost = qcost;
    set_rel_width(run, rel)?;
    Ok(())
}

// get_expr_width (costsize.c).
pub fn get_expr_width(run: &PlannerRun<'_>, expr: NodeId) -> PgResult<i32> {
    let node = *run.root.expr_node(expr);
    if let Some(var) = node.as_var() {
        debug_assert!(var.varlevelsup == 0);
        if var.varno >= 0 && var.varno < run.root.simple_rel_array_size {
            if let Some(rel_id) = run.root.simple_rel_array.get(var.varno as usize).copied().flatten() {
                let rel = run.root.rel(rel_id);
                if var.varattno >= rel.min_attr && var.varattno <= rel.max_attr {
                    let ndx = (var.varattno - rel.min_attr) as usize;
                    if rel.attr_widths[ndx] > 0 {
                        return Ok(rel.attr_widths[ndx]);
                    }
                }
            }
        }
        let width = lsyscache::get_typavgwidth(var.vartype, var.vartypmod)?;
        debug_assert!(width > 0);
        return Ok(width);
    }
    let (typid, typmod) = expr_type_typmod(node);
    let width = lsyscache::get_typavgwidth(typid, typmod)?;
    debug_assert!(width > 0);
    Ok(width)
}

// exprType/exprTypmod (nodeFuncs.c), the arms this lane can carry.
pub fn expr_type_typmod(node: Node<'_>) -> (u32, i32) {
    match node.node_tag() {
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            (c.consttype, c.consttypmod)
        }
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            (v.vartype, v.vartypmod)
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            (r.resulttype, r.resulttypmod)
        }
        NodeTag::T_CoerceToDomain => {
            let cd = node.as_coerce_to_domain().unwrap();
            (cd.resulttype, cd.resulttypmod)
        }
        NodeTag::T_OpExpr => (node.as_op_expr().unwrap().opresulttype, -1),
        NodeTag::T_DistinctExpr => (node.as_distinct_expr().unwrap().opresulttype, -1),
        NodeTag::T_BooleanTest
        | NodeTag::T_BoolExpr
        | NodeTag::T_NullTest => (types_core::catalog::BOOLOID, -1),
        NodeTag::T_RowExpr => (node.as_row_expr().unwrap().row_typeid, -1),
        NodeTag::T_FuncExpr => (node.as_func_expr().unwrap().funcresulttype, -1),
        NodeTag::T_Aggref => (node.as_aggref().unwrap().aggtype, -1),
        NodeTag::T_GroupingFunc => (23, -1),
        NodeTag::T_WindowFunc => (node.as_window_func().unwrap().wintype, -1),
        NodeTag::T_Param => {
            let p = node.as_param().unwrap();
            (p.paramtype, p.paramtypmod)
        }
        NodeTag::T_SQLValueFunction => {
            let svf = node.as_sql_value_function().unwrap();
            (svf.r#type, svf.typmod)
        }
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => {
                    (sp.firstColType, sp.firstColTypmod)
                }
                SubLinkType::MULTIEXPR_SUBLINK => {
                    panic!("exprType (nodeFuncs.c): MULTIEXPR SubPlan not ported")
                }
                _ => (types_core::catalog::BOOLOID, -1),
            }
        }
        NodeTag::T_AlternativeSubPlan => expr_type_typmod(
            node.as_alternative_sub_plan().unwrap().subplans.first().expect("alternatives"),
        ),
        NodeTag::T_CaseTestExpr => {
            let ct = node.as_case_test_expr().unwrap();
            (ct.typeId, ct.typeMod)
        }
        // C exprTypmod CaseExpr: typmod only when every result agrees.
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            (c.casetype, case_expr_typmod(c))
        }
        NodeTag::T_CoerceViaIO => (node.as_coerce_via_io().unwrap().resulttype, -1),
        NodeTag::T_NextValueExpr => (
            node.as_variant::<types_nodes::primnodes::NextValueExpr>().unwrap().typeId,
            -1,
        ),
        _ => (nodes_core::expr_type(node), nodes_core::expr_typmod(node)),
    }
}

fn case_expr_typmod(c: &types_nodes::primnodes::CaseExpr<'_>) -> i32 {
    let Some(defresult) = c.defresult else { return -1 };
    let (dtype, typmod) = expr_type_typmod(defresult);
    if dtype != c.casetype || typmod < 0 {
        return -1;
    }
    for w in &c.args {
        let result = w.as_case_when().expect("CaseWhen").result.expect("CaseWhen.result");
        let (rtype, rtypmod) = expr_type_typmod(result);
        if rtype != c.casetype || rtypmod != typmod {
            return -1;
        }
    }
    typmod
}

// set_rel_width (costsize.c); PlaceHolderVars are the M2 subquery lane.
pub fn set_rel_width<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let relid_idx = run.root.rel(rel).relid;
    let reloid = run.rte(relid_idx as usize).relid;
    let min_attr = run.root.rel(rel).min_attr;
    let max_attr = run.root.rel(rel).max_attr;
    let mut tuple_width: i64 = 0;
    let mut have_wholerow_var = false;

    {
        let rt = run.root.rel_reltarget_mut(rel);
        rt.cost.startup = 0.0;
        rt.cost.per_tuple = 0.0;
    }

    let exprs = match run.root.rel(rel).pathtarget_id {
        Some(id) => crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(id).exprs),
        None => mcx::PgVec::new_in(run.mcx),
    };

    for &node_id in exprs.iter() {
        let node = *run.root.expr_node(node_id);
        let var = match node.as_var() {
            Some(v) if v.varno as u32 == relid_idx => Some((v.varattno, v.vartype, v.vartypmod)),
            _ => None,
        };
        if let Some((varattno, vartype, vartypmod)) = var {
            debug_assert!(varattno >= min_attr && varattno <= max_attr);
            let ndx = (varattno - min_attr) as usize;
            if varattno == 0 {
                have_wholerow_var = true;
                continue;
            }
            let cached = run.root.rel(rel).attr_widths[ndx];
            if cached > 0 {
                tuple_width += cached as i64;
                continue;
            }
            if reloid != 0 && varattno > 0 {
                let item_width = lsyscache::get_attavgwidth(reloid, varattno)?;
                if item_width > 0 {
                    run.root.rel_mut(rel).attr_widths[ndx] = item_width;
                    tuple_width += item_width as i64;
                    continue;
                }
            }
            let item_width = lsyscache::get_typavgwidth(vartype, vartypmod)?;
            debug_assert!(item_width > 0);
            run.root.rel_mut(rel).attr_widths[ndx] = item_width;
            tuple_width += item_width as i64;
        } else {
            if node.node_tag() == NodeTag::T_Var {
                panic!("set_rel_width (costsize.c): foreign Var in reltarget; M2 join lane");
            }
            let (typid, typmod) = expr_type_typmod(node);
            let item_width = lsyscache::get_typavgwidth(typid, typmod)?;
            debug_assert!(item_width > 0);
            tuple_width += item_width as i64;
            let cost = cost_qual_eval_node(node)?;
            let rt = run.root.rel_reltarget_mut(rel);
            rt.cost.startup += cost.startup;
            rt.cost.per_tuple += cost.per_tuple;
        }
    }

    if have_wholerow_var {
        let mut wholerow_width: i64 =
            types_tuple::MAXALIGN(types_tuple::SizeofHeapTupleHeader) as i64;
        if reloid != 0 {
            let relation = table::table_open(run.mcx, reloid, types_rel::NoLock)?;
            let empty = mcx::PgVec::new_in(run.mcx);
            let mut widths = core::mem::replace(&mut run.root.rel_mut(rel).attr_widths, empty);
            wholerow_width += crate::plancat::get_rel_data_width(
                &relation,
                Some(&mut widths),
                min_attr,
            )? as i64;
            run.root.rel_mut(rel).attr_widths = widths;
            relation.close(types_rel::NoLock)?;
        } else {
            for i in 1..=max_attr {
                wholerow_width += run.root.rel(rel).attr_widths[(i - min_attr) as usize] as i64;
            }
        }
        let clamped = clamp_width_est(wholerow_width);
        run.root.rel_mut(rel).attr_widths[(0 - min_attr) as usize] = clamped;
        tuple_width += wholerow_width;
    }

    let width = clamp_width_est(tuple_width);
    run.root.rel_reltarget_mut(rel).width = width;
    Ok(())
}

const LOG2_DIVISOR: f64 = 0.693147180559945;
fn log2(x: f64) -> f64 {
    x.ln() / LOG2_DIVISOR
}

// tuplesort_merge_order (tuplesort.c); consts pinned to tuplesort.c:176-179.
fn tuplesort_merge_order(allowed_mem: i64) -> f64 {
    const MINORDER: i64 = 6;
    const MAXORDER: i64 = 500;
    const TAPE_BUFFER_OVERHEAD: i64 = BLCKSZ as i64;
    const MERGE_BUFFER_SIZE: i64 = BLCKSZ as i64 * 32;
    (allowed_mem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)).clamp(MINORDER, MAXORDER) as f64
}

fn cost_tuplesort(
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> (f64, f64) {
    let input_bytes = relation_byte_size(tuples, width);
    let sort_mem_bytes = sort_mem as i64 * 1024;
    let tuples = tuples.max(2.0);
    let comparison_cost = comparison_cost + 2.0 * gucs::cpu_operator_cost();

    let (output_tuples, output_bytes) = if limit_tuples > 0.0 && limit_tuples < tuples {
        (limit_tuples, relation_byte_size(limit_tuples, width))
    } else {
        (tuples, input_bytes)
    };

    let startup_cost = if output_bytes > sort_mem_bytes as f64 {
        let npages = (input_bytes / BLCKSZ as f64).ceil();
        let nruns = input_bytes / sort_mem_bytes as f64;
        let mergeorder = tuplesort_merge_order(sort_mem_bytes);
        let log_runs =
            if nruns > mergeorder { (nruns.ln() / mergeorder.ln()).ceil() } else { 1.0 };
        let npageaccesses = 2.0 * npages * log_runs;
        comparison_cost * tuples * log2(tuples)
            + npageaccesses * (gucs::seq_page_cost() * 0.75 + gucs::random_page_cost() * 0.25)
    } else if tuples > 2.0 * output_tuples || input_bytes > sort_mem_bytes as f64 {
        comparison_cost * tuples * log2(2.0 * output_tuples)
    } else {
        comparison_cost * tuples * log2(tuples)
    };
    (startup_cost, gucs::cpu_operator_cost() * tuples)
}

/// The cost_sort computation without a Path to write into (C's dummy
/// `Path sort_path` callers); returns (disabled_nodes, startup, total).
#[allow(clippy::too_many_arguments)]
pub fn cost_sort_shape(
    input_disabled_nodes: i32,
    input_cost: f64,
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> (i32, f64, f64) {
    let (startup, run_cost) =
        cost_tuplesort(tuples, width, comparison_cost, sort_mem, limit_tuples);
    let startup_cost = startup + input_cost;
    (
        input_disabled_nodes + if gucs::enable_sort() { 0 } else { 1 },
        startup_cost,
        startup_cost + run_cost,
    )
}

/// cost_incremental_sort (costsize.c) without a Path to write into; returns
/// (disabled_nodes, startup, total, rows).
#[allow(clippy::too_many_arguments)]
pub fn cost_incremental_sort_shape<'mcx>(
    run: &mut PlannerRun<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
    presorted_keys: usize,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> PgResult<(i32, f64, f64, f64)> {
    debug_assert!(presorted_keys > 0 && presorted_keys < pathkeys.len());
    let input_run_cost = input_total_cost - input_startup_cost;
    let input_tuples = input_tuples.max(2.0);
    let mut input_groups = input_tuples.min(crate::selfuncs::DEFAULT_NUM_DISTINCT);

    let mcx = run.mcx;
    let mut presorted_exprs: mcx::PgVec<'_, (types_pathnodes::NodeId, Node<'mcx>)> =
        mcx::PgVec::new_in(mcx);
    let mut unknown_varno = false;
    for (i, key) in pathkeys.iter().enumerate() {
        let ec = key.pk_eclass.expect("canonical pathkey has an eclass");
        let em_id = run.root.ec(ec).ec_members[0];
        let em_expr = run.root.em(em_id).em_expr;
        let expr = *run.root.expr_node(em_expr);
        // Vars with varno 0 (generate_append_tlist) confuse estimate_num_groups.
        if vars::pull_varnos(mcx, expr)?.is_member(0) {
            unknown_varno = true;
            break;
        }
        presorted_exprs.push((em_expr, expr));
        if i + 1 >= presorted_keys {
            break;
        }
    }
    if !unknown_varno {
        input_groups = crate::selfuncs::estimate_num_groups(run, &presorted_exprs, input_tuples)?;
    }

    let group_tuples = input_tuples / input_groups;
    let group_input_run_cost = input_run_cost / input_groups;
    let (group_startup_cost, group_run_cost) =
        cost_tuplesort(group_tuples, width, comparison_cost, sort_mem, limit_tuples);

    let startup_cost = group_startup_cost + input_startup_cost + group_input_run_cost;
    let mut run_cost = group_run_cost
        + (group_run_cost + group_startup_cost) * (input_groups - 1.0)
        + group_input_run_cost * (input_groups - 1.0);
    run_cost += (gucs::cpu_tuple_cost() + comparison_cost) * input_tuples;
    run_cost += 2.0 * gucs::cpu_tuple_cost() * input_groups;

    debug_assert!(gucs::enable_incremental_sort());
    Ok((input_disabled_nodes, startup_cost, startup_cost + run_cost, input_tuples))
}

#[allow(clippy::too_many_arguments)]
pub fn cost_incremental_sort<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
    pathkeys: &[types_pathnodes::PathKey],
    presorted_keys: usize,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> PgResult<()> {
    let (disabled_nodes, startup_cost, total_cost, rows) = cost_incremental_sort_shape(
        run,
        pathkeys,
        presorted_keys,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    )?;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn cost_sort(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    input_disabled_nodes: i32,
    input_cost: f64,
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (disabled_nodes, startup_cost, total_cost) = cost_sort_shape(
        input_disabled_nodes,
        input_cost,
        tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = tuples;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
}

// cost_windowagg (costsize.c).
#[allow(clippy::too_many_arguments)]
pub fn cost_windowagg<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
    window_funcs: &[Node<'mcx>],
    wc_node: Node<'mcx>,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
) -> PgResult<()> {
    let wc = wc_node.as_window_clause().expect("WindowClause");
    let num_part_cols = wc.partitionClause.len();
    let num_order_cols = wc.orderClause.len();

    let mut startup_cost = input_startup_cost;
    let mut total_cost = input_total_cost;
    for wf_node in window_funcs {
        let wf = wf_node.as_window_func().expect("WindowFunc");
        let mut argcosts = QualCost::default();
        crate::plancat::add_function_cost(wf.winfnoid, &mut argcosts)?;
        startup_cost += argcosts.startup;
        let mut wfunccost = argcosts.per_tuple;
        let mut argcosts = QualCost::default();
        for arg in &wf.args {
            let c = cost_qual_eval_node(arg)?;
            argcosts.startup += c.startup;
            argcosts.per_tuple += c.per_tuple;
        }
        startup_cost += argcosts.startup;
        wfunccost += argcosts.per_tuple;
        if let Some(f) = wf.aggfilter {
            let c = cost_qual_eval_node(f)?;
            startup_cost += c.startup;
            wfunccost += c.per_tuple;
        }
        total_cost += wfunccost * input_tuples;
    }

    total_cost +=
        crate::gucs::cpu_operator_cost() * (num_part_cols + num_order_cols) as f64 * input_tuples;
    total_cost += crate::gucs::cpu_tuple_cost() * input_tuples;

    {
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = input_tuples;
        p.disabled_nodes = input_disabled_nodes;
        p.startup_cost = startup_cost;
        p.total_cost = total_cost;
    }

    let startup_tuples = get_windowclause_startup_tuples(run, wc_node, input_tuples)?;
    if startup_tuples > 1.0 {
        let p = run.root.path_mut(path_id).base_mut();
        p.startup_cost += (total_cost - startup_cost) / input_tuples * (startup_tuples - 1.0);
    }
    Ok(())
}

// get_windowclause_startup_tuples (costsize.c).
fn get_windowclause_startup_tuples<'mcx>(
    run: &mut PlannerRun<'mcx>,
    wc_node: Node<'mcx>,
    input_tuples: f64,
) -> PgResult<f64> {
    use types_nodes::rawnodes::{
        FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET_FOLLOWING,
        FRAMEOPTION_END_OFFSET_PRECEDING, FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
        FRAMEOPTION_GROUPS, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
    };
    let wc = wc_node.as_window_clause().expect("WindowClause");
    let frame_options = wc.frameOptions;

    let partition_tuples = if !wc.partitionClause.is_nil() {
        let mut clause_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
            mcx::PgVec::new_in(run.mcx);
        for n in &wc.partitionClause {
            clause_ids.push(run.intern_expr(n));
        }
        let exprs =
            crate::grouping::sortgrouplist_exprs(run, &clause_ids, &run.parse().targetList);
        let num_partitions = crate::selfuncs::estimate_num_groups(run, &exprs, input_tuples)?;
        input_tuples / num_partitions
    } else {
        input_tuples
    };

    let wc = wc_node.as_window_clause().expect("WindowClause");
    let peer_tuples = if !wc.orderClause.is_nil() {
        let mut clause_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
            mcx::PgVec::new_in(run.mcx);
        for n in &wc.orderClause {
            clause_ids.push(run.intern_expr(n));
        }
        let exprs =
            crate::grouping::sortgrouplist_exprs(run, &clause_ids, &run.parse().targetList);
        let num_groups = crate::selfuncs::estimate_num_groups(run, &exprs, partition_tuples)?;
        partition_tuples / num_groups
    } else {
        1.0
    };

    let wc = wc_node.as_window_clause().expect("WindowClause");
    let return_tuples = if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        partition_tuples
    } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
        if frame_options & FRAMEOPTION_ROWS != 0 {
            1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            if wc.orderClause.is_nil() { partition_tuples } else { peer_tuples }
        } else {
            unreachable!()
        }
    } else if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        1.0
    } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
        let end_offset_value = match wc.endOffset.and_then(|n| n.as_const()) {
            Some(c) => {
                if c.constisnull {
                    // NULL errors at execution; assume one row/range/group.
                    1.0
                } else {
                    match c.consttype {
                        types_core::catalog::INT2OID => c.constvalue.as_i16() as f64,
                        types_core::catalog::INT4OID => c.constvalue.as_i32() as f64,
                        types_core::catalog::INT8OID => c.constvalue.as_i64() as f64,
                        _ => partition_tuples / peer_tuples * crate::selfuncs::DEFAULT_INEQ_SEL,
                    }
                }
            }
            None => partition_tuples / peer_tuples * crate::selfuncs::DEFAULT_INEQ_SEL,
        };
        if frame_options & FRAMEOPTION_ROWS != 0 {
            end_offset_value + 1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            peer_tuples * (end_offset_value + 1.0)
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    };

    let return_tuples = if !wc.partitionClause.is_nil() || !wc.orderClause.is_nil() {
        f64::min(return_tuples + 1.0, partition_tuples)
    } else {
        f64::min(return_tuples, partition_tuples)
    };
    Ok(clamp_row_est(return_tuples))
}

const APPEND_CPU_COST_MULTIPLIER: f64 = 0.5;

// cost_append (costsize.c), serial unordered arm; the ordered arm belongs to
// the MergeAppend/set-ops ordered lane and parallel append has no lane.
pub fn cost_append(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let (subpaths, parallel_aware, pathkeys_empty) = match run.root.path(path_id) {
        types_pathnodes::PathNode::AppendPath(a) => (
            crate::relnode::pgvec_clone_shallow(run.mcx, &a.subpaths),
            a.path.parallel_aware,
            a.path.pathkeys.is_empty(),
        ),
        _ => panic!("cost_append: not an AppendPath"),
    };
    assert!(!parallel_aware, "cost_append (costsize.c): parallel append; M3 parallel lane");
    {
        let p = run.root.path_mut(path_id).base_mut();
        p.disabled_nodes = 0;
        p.startup_cost = 0.0;
        p.total_cost = 0.0;
        p.rows = 0.0;
    }
    if subpaths.is_empty() {
        return;
    }
    assert!(
        pathkeys_empty,
        "cost_append (costsize.c): ordered append; MergeAppend lane unported (set-ops lane)"
    );
    let mut rows = 0.0;
    let mut disabled = 0;
    let mut total = 0.0;
    let startup = run.root.path(subpaths[0]).base().startup_cost;
    for &sp in subpaths.iter() {
        let s = run.root.path(sp).base();
        rows += s.rows;
        disabled += s.disabled_nodes;
        total += s.total_cost;
    }
    total += gucs::cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * rows;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled;
    p.startup_cost = startup;
    p.total_cost = total;
}

// cost_subqueryscan (costsize.c); param_info is always None on this lane.
pub fn cost_subqueryscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
    sub: &crate::pathnode::SubqueryScanInfo,
    trivial_pathtarget: bool,
) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    debug_assert!(
        run.root.rel(rel).rtekind
            == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY as u32
    );
    let qpquals =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).baserestrictinfo);
    let selec = crate::clausesel::clauselist_selectivity(
        run,
        &qpquals,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let rows = clamp_row_est(sub.rows * selec);
    {
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = rows;
        p.disabled_nodes = sub.disabled_nodes;
        p.startup_cost = sub.startup_cost;
        p.total_cost = sub.total_cost;
    }
    // With no quals and a trivial target, setrefs elides the SubqueryScan.
    if qpquals.is_empty() && trivial_pathtarget {
        return Ok(());
    }

    let qpqual_cost = get_restriction_qual_cost(run, rel);
    let mut startup_cost = qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * sub.rows;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost += startup_cost;
    p.total_cost += startup_cost + run_cost;
    Ok(())
}

// set_subquery_size_estimates (costsize.c).
pub fn set_subquery_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    let idx = run.root.rel(rel).subroot_idx.expect("subquery rel has a subroot");

    run.swap_with_rel_subroot(idx);
    let (tuples, widths) = {
        let final_rel = crate::planmain::fetch_final_rel(run);
        let cheapest = run
            .root
            .rel(final_rel)
            .cheapest_total_path
            .expect("subquery final rel has a cheapest path");
        let tuples = run.root.path(cheapest).base().rows;
        let sub_parse = run.parse();
        let mut widths: mcx::PgVec<'_, (i16, i32)> = mcx::PgVec::new_in(run.mcx);
        for tle_node in &sub_parse.targetList {
            let te = tle_node.as_target_entry().expect("tlist cell");
            if te.resjunk {
                continue;
            }
            let mut item_width = 0;
            if let Some(v) = te.expr.as_var() {
                if sub_parse.setOperations.is_none() {
                    let subrel_id = crate::relnode::find_base_rel(&run.root, v.varno);
                    let subrel = run.root.rel(subrel_id);
                    item_width = subrel.attr_widths[(v.varattno - subrel.min_attr) as usize];
                }
            }
            widths.push((te.resno, item_width));
        }
        (tuples, widths)
    };
    run.swap_with_rel_subroot(idx);

    run.root.rel_mut(rel).tuples = tuples;
    let (min_attr, max_attr) = {
        let r = run.root.rel(rel);
        (r.min_attr, r.max_attr)
    };
    for &(resno, w) in widths.iter() {
        if resno < min_attr || resno > max_attr {
            continue;
        }
        run.root.rel_mut(rel).attr_widths[(resno - min_attr) as usize] = w;
    }
    set_baserel_size_estimates(run, rel)
}
