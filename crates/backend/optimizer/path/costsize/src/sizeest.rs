//! Per-RTE-kind size estimators (costsize.c:5328-6248) + `set_rel_width` /
//! `set_pathtarget_cost_width` / `get_expr_width`.

use types_core::primitive::{Cost, Oid};
use pathnodes::planner_run::PlannerRun;
use pathnodes::{NodeId, PathTarget, PlannerInfo, RelId};

use costsize_seams as cz;

use crate::{
    clamp_row_est, clamp_width_est, cost_qual_eval, cost_qual_eval_node, rinfo_clause_nodes,
    recursive_worktable_factor, SizeofHeapTupleHeader, RTE_FUNCTION, RTE_SUBQUERY, RTE_VALUES,
};

/// `MAXALIGN(LEN)` — round up to the platform max alignment (8).
#[inline]
fn maxalign(len: i64) -> i64 {
    (len + 7) & !7
}

/// `IS_SPECIAL_VARNO` (primnodes.h) — `((int) (varno) < 0)`. The special varnos
/// are the C negative sentinels (-1..-4); real range-table indices are >= 1.
#[inline]
fn is_special_varno(varno: i32) -> bool {
    varno < 0
}

/// `set_baserel_size_estimates` (costsize.c:5328).
pub fn set_baserel_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);

    let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
    let clause_nodes = rinfo_clause_nodes(root, &baserestrictinfo);

    // Pass the RestrictInfo list (not bare nodes) so the RestrictInfo
    // superstructure survives for find_single_rel_for_clauses + extended stats,
    // exactly as C's set_baserel_size_estimates passes rel->baserestrictinfo.
    let nrows = root.rel(rel).tuples
        * cz::clauselist_selectivity_rinfos::call(
            run,
            root,
            &baserestrictinfo,
            0,
            super::JOIN_INNER as i32,
            None,
        );

    root.rel_mut(rel).rows = clamp_row_est(nrows);

    let qcost = cost_qual_eval(root, &clause_nodes);
    root.rel_mut(rel).baserestrictcost = qcost;

    set_rel_width(run, root, rel);
}

/// `set_function_size_estimates` (costsize.c:6066). The rel's rowcount is the
/// largest `expression_returns_set_rows` over the FUNCTION RTE's `functions`
/// list. `cost_functionscan` (no `run`) cannot reach the RTE's funcexprs, so
/// the max-rows reduction rides the `rte_function_max_set_rows` seam (installed
/// by `backend-optimizer-rte-seams`, which has the `PlannerRun` RTE store).
pub fn set_function_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);
    let tuples = cz::rte_function_max_set_rows::call(run, root, rel);
    debug_assert!(rt_rtekind(root, rel) == RTE_FUNCTION);
    root.rel_mut(rel).tuples = tuples;
    set_baserel_size_estimates(run, root, rel);
}

/// `set_values_size_estimates` (costsize.c:6043). The row count is exactly the
/// length of the VALUES RTE's `values_lists`.
pub fn set_values_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);
    let relid = root.rel(rel).relid as types_core::primitive::Index;
    let rte = pathnodes::planner_run::planner_rt_fetch(run, root, relid);
    debug_assert!(rte.rtekind as u32 == RTE_VALUES);
    let tuples = rte.values_lists.len() as f64;
    root.rel_mut(rel).tuples = tuples;
    set_baserel_size_estimates(run, root, rel);
}

/// `set_tablefunc_size_estimates` (costsize.c:6104) — hardwired 100 rows.
pub fn set_tablefunc_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);
    root.rel_mut(rel).tuples = 100.0;
    set_baserel_size_estimates(run, root, rel);
}

/// `set_cte_size_estimates` (costsize.c:6151).
pub fn set_cte_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId, cte_rows: f64) {
    debug_assert!(root.rel(rel).relid > 0);
    let self_reference = cz::rte_cte_self_reference::call(run, root, rel);
    if self_reference {
        root.rel_mut(rel).tuples = clamp_row_est(recursive_worktable_factor() * cte_rows);
    } else {
        root.rel_mut(rel).tuples = cte_rows;
    }
    set_baserel_size_estimates(run, root, rel);
}

/// `set_namedtuplestore_size_estimates` (costsize.c:6192).
pub fn set_namedtuplestore_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);
    let enrtuples = cz::rte_enrtuples::call(run, root, rel);
    root.rel_mut(rel).tuples = enrtuples;
    if root.rel(rel).tuples < 0.0 {
        root.rel_mut(rel).tuples = 1000.0;
    }
    set_baserel_size_estimates(run, root, rel);
}

/// `set_result_size_estimates` (costsize.c:6224).
pub fn set_result_size_estimates<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    debug_assert!(root.rel(rel).relid > 0);
    root.rel_mut(rel).tuples = 1.0;
    set_baserel_size_estimates(run, root, rel);
}

/// `set_subquery_size_estimates` (costsize.c:5902).
///
/// Set the size estimates for a base relation that is a subquery. The rel's
/// targetlist and restrictinfo list must already be built and the subquery's
/// Paths completed; we read the subquery's `PlannerInfo` (`rel.subroot`) for the
/// raw output rowcount and per-column width estimates, then defer to
/// `set_baserel_size_estimates`.
pub fn set_subquery_size_estimates<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) {
    debug_assert!(root.rel(rel).relid > 0);
    debug_assert!(rt_rtekind(root, rel) == RTE_SUBQUERY);

    // The subroot lives inside the rel; move it out so we can borrow it while
    // mutating `root` (fetch_upper_rel needs `&mut subroot`). Restored below.
    let mut subroot: alloc::boxed::Box<PlannerInfo> = root
        .rel_mut(rel)
        .subroot
        .0
        .take()
        .expect("set_subquery_size_estimates: rel.subroot is NULL");

    // Copy raw number of output rows from subquery: all of its paths share the
    // same rowcount, so look at cheapest-total of the subroot's FINAL rel. The
    // FINAL upper rel was created while the subquery was planned, so read it
    // directly off `subroot.upper_rels` (a costsize->relnode dep would cycle
    // through rte-seams).
    let sub_final_rel = subroot.upper_rels[pathnodes::UPPERREL_FINAL as usize]
        .first()
        .copied()
        .expect("set_subquery_size_estimates: subroot has no UPPERREL_FINAL rel");
    let cheapest = subroot
        .rel(sub_final_rel)
        .cheapest_total_path
        .expect("set_subquery_size_estimates: subroot FINAL rel has no cheapest_total_path");
    root.rel_mut(rel).tuples = subroot.path(cheapest).base().rows;

    // Per-output-column width estimates from the subquery's targetlist. For a
    // plain Var (and only when the subquery is not itself a setop), use the
    // width estimate made while planning the subquery; otherwise leave it to
    // set_rel_width to fill a datatype-based default.
    let min_attr = root.rel(rel).min_attr;
    let max_attr = root.rel(rel).max_attr;
    let sub_has_setops = run.resolve(subroot.parse).setOperations.is_some();

    // Snapshot the (resjunk, resno, item_width) for each tlist entry; the Var
    // width read borrows `subroot` immutably, disjoint from `root`.
    let mut updates: alloc::vec::Vec<(i16, i32)> = alloc::vec::Vec::new();
    {
        let parse = run.resolve(subroot.parse);
        for te in parse.targetList.iter() {
            // junk columns aren't visible to upper query
            if te.resjunk {
                continue;
            }
            // ignore tlist columns not visible at our query level
            if te.resno < min_attr || te.resno > max_attr {
                continue;
            }
            let mut item_width: i32 = 0;
            if !sub_has_setops {
                if let Some(texpr) = te.expr.as_deref() {
                    if let nodes::primnodes::Expr::Var(var) = texpr {
                        let subrel = relnode_seams::find_base_rel::call(
                            &subroot,
                            var.varno,
                        );
                        let sr = subroot.rel(subrel);
                        let ndx = (var.varattno - sr.min_attr) as usize;
                        item_width = sr.attr_widths[ndx];
                    }
                }
            }
            updates.push((te.resno, item_width));
        }
    }
    for (resno, item_width) in updates {
        let ndx = (resno - min_attr) as usize;
        root.rel_mut(rel).attr_widths[ndx] = item_width;
    }

    // Restore the subroot into the rel.
    root.rel_mut(rel).subroot.0 = Some(subroot);

    // Now estimate number of output rows, etc.
    set_baserel_size_estimates(run, root, rel);
}

/// `get_expr_width` (costsize.c:6404).
pub fn get_expr_width(root: &PlannerInfo, expr: NodeId) -> i32 {
    use nodes::primnodes::Expr;
    if let Expr::Var(var) = root.node(expr) {
        debug_assert!(var.varlevelsup == 0);

        if !is_special_varno(var.varno) && var.varno < root.simple_rel_array_size {
            if let Some(rel_id) = root
                .simple_rel_array
                .get(var.varno as usize)
                .copied()
                .flatten()
            {
                let rel = root.rel(rel_id);
                if var.varattno >= rel.min_attr && var.varattno <= rel.max_attr {
                    let ndx = (var.varattno - rel.min_attr) as usize;
                    if rel.attr_widths[ndx] > 0 {
                        return rel.attr_widths[ndx];
                    }
                }
            }
        }

        let width = cz::get_typavgwidth::call(var.vartype, var.vartypmod);
        debug_assert!(width > 0);
        return width;
    }

    let width = cz::get_typavgwidth::call(
        cz::expr_type::call(root, expr),
        cz::expr_typmod::call(root, expr),
    );
    debug_assert!(width > 0);
    width
}

/// `set_pathtarget_cost_width` (costsize.c:6366).
pub fn set_pathtarget_cost_width(root: &PlannerInfo, target: &mut PathTarget) {
    let mut tuple_width: i64 = 0;
    target.cost.startup = 0.0;
    target.cost.per_tuple = 0.0;

    let mut acc_startup: Cost = 0.0;
    let mut acc_per_tuple: Cost = 0.0;
    for &node in target.exprs.iter() {
        tuple_width += get_expr_width(root, node) as i64;
        if !is_var(root, node) {
            let cost = cost_qual_eval_node(root, node);
            acc_startup += cost.startup;
            acc_per_tuple += cost.per_tuple;
        }
    }
    target.cost.startup = acc_startup;
    target.cost.per_tuple = acc_per_tuple;
    target.width = clamp_width_est(tuple_width);
}

/// `set_rel_width` (costsize.c:6480).
pub fn set_rel_width<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) {
    let relid_idx = root.rel(rel).relid;
    // rte->relid (underlying table OID, 0 for phony rel) — RTE unreachable → seam.
    let reloid: Oid = cz::rte_relid::call(run, root, rel);

    let min_attr = root.rel(rel).min_attr;
    let max_attr = root.rel(rel).max_attr;
    let mut tuple_width: i64 = 0;
    let mut have_wholerow_var = false;

    if let Some(rt) = root.rel_mut(rel).reltarget.as_deref_mut() {
        rt.cost.startup = 0.0;
        rt.cost.per_tuple = 0.0;
    }

    let exprs: alloc::vec::Vec<NodeId> = match root.rel(rel).reltarget.as_deref() {
        Some(rt) => rt.exprs.clone(),
        None => alloc::vec::Vec::new(),
    };

    for &node in exprs.iter() {
        let belongs = var_belongs(root, node, relid_idx);
        if belongs {
            let (varattno, vartype, vartypmod) = var_fields(root, node);

            debug_assert!(varattno >= min_attr);
            debug_assert!(varattno <= max_attr);

            let ndx = (varattno - min_attr) as usize;

            if varattno == 0 {
                have_wholerow_var = true;
                continue;
            }

            let cached = root.rel(rel).attr_widths.get(ndx).copied().unwrap_or(0);
            if cached > 0 {
                tuple_width += cached as i64;
                continue;
            }

            if reloid != 0 && varattno > 0 {
                let item_width = cz::get_attavgwidth::call(reloid, varattno);
                if item_width > 0 {
                    if let Some(w) = root.rel_mut(rel).attr_widths.get_mut(ndx) {
                        *w = item_width;
                    }
                    tuple_width += item_width as i64;
                    continue;
                }
            }

            let item_width = cz::get_typavgwidth::call(vartype, vartypmod);
            debug_assert!(item_width > 0);
            if let Some(w) = root.rel_mut(rel).attr_widths.get_mut(ndx) {
                *w = item_width;
            }
            tuple_width += item_width as i64;
        } else if is_placeholdervar(root, node) {
            let (ph_width, cost_startup, cost_per_tuple) =
                cz::find_placeholder_info_width::call(run.mcx(), root, node);
            tuple_width += ph_width as i64;
            if let Some(rt) = root.rel_mut(rel).reltarget.as_deref_mut() {
                rt.cost.startup += cost_startup;
                rt.cost.per_tuple += cost_per_tuple;
            }
        } else {
            let item_width = cz::get_typavgwidth::call(
                cz::expr_type::call(root, node),
                cz::expr_typmod::call(root, node),
            );
            debug_assert!(item_width > 0);
            tuple_width += item_width as i64;
            let cost = cost_qual_eval_node(root, node);
            if let Some(rt) = root.rel_mut(rel).reltarget.as_deref_mut() {
                rt.cost.startup += cost.startup;
                rt.cost.per_tuple += cost.per_tuple;
            }
        }
    }

    if have_wholerow_var {
        let mut wholerow_width: i64 = maxalign(SizeofHeapTupleHeader as i64);

        if reloid != 0 {
            // C: get_relation_data_width(reloid, rel->attr_widths - rel->min_attr)
            // — pass the unshifted rel cache plus its min_attr so the callee
            // indexes attr_widths[attno - min_attr] (costsize.c:6330).
            wholerow_width += cz::get_relation_data_width::call(
                reloid,
                &root.rel(rel).attr_widths,
                min_attr,
            ) as i64;
        } else {
            let mut i: i32 = 1;
            while i <= max_attr as i32 {
                let ndx = (i - min_attr as i32) as usize;
                wholerow_width += root.rel(rel).attr_widths.get(ndx).copied().unwrap_or(0) as i64;
                i += 1;
            }
        }

        let clamped = clamp_width_est(wholerow_width);
        let zero_ndx = (0 - min_attr as i32) as usize;
        if let Some(w) = root.rel_mut(rel).attr_widths.get_mut(zero_ndx) {
            *w = clamped;
        }
        tuple_width += wholerow_width;
    }

    let width = clamp_width_est(tuple_width);
    if let Some(rt) = root.rel_mut(rel).reltarget.as_deref_mut() {
        rt.width = width;
    }
}

/* --------------------------------------------------------------------------
 * Node-shape helpers over the arena's `Expr` enum.
 * ------------------------------------------------------------------------ */

fn is_var(root: &PlannerInfo, node: NodeId) -> bool {
    matches!(root.node(node), nodes::primnodes::Expr::Var(_))
}

fn var_belongs(root: &PlannerInfo, node: NodeId, relid_idx: u32) -> bool {
    matches!(root.node(node), nodes::primnodes::Expr::Var(v) if v.varno as u32 == relid_idx)
}

fn var_fields(root: &PlannerInfo, node: NodeId) -> (i16, Oid, i32) {
    match root.node(node) {
        nodes::primnodes::Expr::Var(v) => (v.varattno, v.vartype, v.vartypmod),
        _ => panic!("set_rel_width: expected Var"),
    }
}

fn is_placeholdervar(root: &PlannerInfo, node: NodeId) -> bool {
    matches!(
        root.node(node),
        nodes::primnodes::Expr::PlaceHolderVar(_)
    )
}

fn rt_rtekind(root: &PlannerInfo, rel: RelId) -> u32 {
    root.rel(rel).rtekind
}
