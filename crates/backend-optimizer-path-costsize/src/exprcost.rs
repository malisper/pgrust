//! Agg / Group / WindowAgg cost (costsize.c:2682-3184), `cost_rescan` +
//! `cost_memoize_rescan` (costsize.c:4640/2541), and `cost_subplan`
//! (costsize.c:4534).


use types_core::primitive::Cost;
use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    AggStrategy, NodeId, PathId, PathNode, PlannerInfo, AGG_HASHED, AGG_MIXED,
    AGG_PLAIN, AGG_SORTED,
};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_util_pathnode_seams as ps;
use backend_utils_adt_selfuncs_seams as selfuncs;
use types_selfuncs::{EstimationInfo, SELFLAG_USED_DEFAULT};

use crate::{
    ceil, clamp_row_est, cost_qual_eval, cpu_operator_cost, cpu_tuple_cost,
    libm_log, random_page_cost, relation_byte_size, seq_page_cost, work_mem,
    Max, Min, BLCKSZ, ENABLE_HASHAGG,
};
use crate::sizeest::get_expr_width;

/// Per-aggregate cost carrier (matches the pathnode-seams `AggClauseCostsLite`).
pub use backend_optimizer_util_pathnode_seams::AggClauseCostsLite;

/* ==========================================================================
 * cost_agg (costsize.c:2682)
 * ========================================================================== */

/// `cost_agg` — fills an Agg path (by `PathId`). `quals` are the HAVING-qual
/// expression handles.
pub fn cost_agg<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    aggstrategy: AggStrategy,
    aggcosts: Option<AggClauseCostsLite>,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[NodeId],
    mut disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    input_width: i32,
) {
    let mut output_tuples: f64;
    let mut startup_cost: Cost;
    let mut total_cost: Cost;

    let aggcosts = aggcosts.unwrap_or_else(|| {
        debug_assert!(aggstrategy == AGG_HASHED);
        AggClauseCostsLite::default()
    });

    let cpu_op = cpu_operator_cost();
    let cpu_tup = cpu_tuple_cost();

    if aggstrategy == AGG_PLAIN {
        startup_cost = input_total_cost;
        startup_cost += aggcosts.trans_startup;
        startup_cost += aggcosts.trans_per_tuple * input_tuples;
        startup_cost += aggcosts.final_startup;
        startup_cost += aggcosts.final_per_tuple;
        total_cost = startup_cost + cpu_tup;
        output_tuples = 1.0;
    } else if aggstrategy == AGG_SORTED || aggstrategy == AGG_MIXED {
        startup_cost = input_startup_cost;
        total_cost = input_total_cost;
        if aggstrategy == AGG_MIXED && !ENABLE_HASHAGG() {
            disabled_nodes += 1;
        }
        total_cost += aggcosts.trans_startup;
        total_cost += aggcosts.trans_per_tuple * input_tuples;
        total_cost += (cpu_op * num_group_cols as f64) * input_tuples;
        total_cost += aggcosts.final_startup;
        total_cost += aggcosts.final_per_tuple * num_groups;
        total_cost += cpu_tup * num_groups;
        output_tuples = num_groups;
    } else {
        // AGG_HASHED.
        startup_cost = input_total_cost;
        if !ENABLE_HASHAGG() {
            disabled_nodes += 1;
        }
        startup_cost += aggcosts.trans_startup;
        startup_cost += aggcosts.trans_per_tuple * input_tuples;
        startup_cost += (cpu_op * num_group_cols as f64) * input_tuples;
        startup_cost += aggcosts.final_startup;

        total_cost = startup_cost;
        total_cost += aggcosts.final_per_tuple * num_groups;
        total_cost += cpu_tup * num_groups;
        output_tuples = num_groups;
    }

    // Disk costs of hash aggregation that spills to disk.
    if aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED {
        let pages: f64;
        let mut pages_written: f64;
        let mut pages_read: f64;
        let spill_cost: f64;
        let hashentrysize: f64;
        let mut nbatches: f64;
        let mem_limit: usize;
        let ngroups_limit: u64;
        let mut num_partitions: i32;
        let depth: f64;

        hashentrysize = cz::hash_agg_entry_size::call(
            root.aggtransinfos.len() as i32,
            input_width as f64,
            aggcosts.transition_space as u64,
        );
        let limits = cz::hash_agg_set_limits::call(hashentrysize, num_groups, 0);
        mem_limit = limits.mem_limit;
        ngroups_limit = limits.ngroups_limit;
        num_partitions = limits.num_partitions;

        nbatches = Max(
            (num_groups * hashentrysize) / mem_limit as f64,
            num_groups / ngroups_limit as f64,
        );

        nbatches = Max(ceil(nbatches), 1.0);
        num_partitions = Max(num_partitions as f64, 2.0) as i32;

        depth = ceil(libm_log(nbatches) / libm_log(num_partitions as f64));

        pages = relation_byte_size(input_tuples, input_width) / BLCKSZ;
        pages_written = pages * depth;
        pages_read = pages * depth;

        pages_read *= 2.0;
        pages_written *= 2.0;

        let random_pc = random_page_cost();
        let seq_pc = seq_page_cost();
        startup_cost += pages_written * random_pc;
        total_cost += pages_written * random_pc;
        total_cost += pages_read * seq_pc;

        spill_cost = depth * input_tuples * 2.0 * cpu_tup;
        startup_cost += spill_cost;
        total_cost += spill_cost;
    }

    if !quals.is_empty() {
        let qual_cost = cost_qual_eval(root, quals);
        startup_cost += qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        output_tuples = clamp_row_est(
            output_tuples
                * cz::clauselist_selectivity::call(run, root, quals, 0, super::JOIN_INNER as i32, None),
        );
    }

    let p = root.path_mut(path_id).base_mut();
    p.rows = output_tuples;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
}

/* ==========================================================================
 * cost_group (costsize.c:3194)
 * ========================================================================== */

/// `cost_group` — fills a Group path (by `PathId`). `quals` are HAVING-quals.
pub fn cost_group<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    let mut output_tuples: f64 = num_groups;
    let mut startup_cost: Cost = input_startup_cost;
    let mut total_cost: Cost = input_total_cost;

    total_cost += cpu_operator_cost() * input_tuples * num_group_cols as f64;

    if !quals.is_empty() {
        let qual_cost = cost_qual_eval(root, quals);
        startup_cost += qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        output_tuples = clamp_row_est(
            output_tuples
                * cz::clauselist_selectivity::call(run, root, quals, 0, super::JOIN_INNER as i32, None),
        );
    }

    let p = root.path_mut(path_id).base_mut();
    p.rows = output_tuples;
    p.disabled_nodes = input_disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
}

/* ==========================================================================
 * cost_windowagg (costsize.c:3097)
 * ========================================================================== */

/// `cost_windowagg` — fills a WindowAgg path (by `PathId`). `window_funcs` are
/// the WindowFunc node handles; `winclause` is the WindowClause node handle.
pub fn cost_windowagg<'mcx>(
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    window_funcs: &[NodeId],
    winclause: NodeId,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) -> types_error::PgResult<()> {
    // The WindowClause column counts + startup-tuples estimate (the C reads
    // winclause->partitionClause/orderClause + get_windowclause_startup_tuples,
    // which also touches root->parse->targetList) are unreachable in the fabled
    // arena, so they cross a focused seam. The window-function eval costs (over
    // the reachable WindowFunc node handles) and all arithmetic stay in-crate.
    let wc = cz::windowclause_cost_info::call(run, root, winclause, input_tuples)?;
    let num_part_cols = wc.num_part_cols;
    let num_order_cols = wc.num_order_cols;

    let mut startup_cost: Cost = input_startup_cost;
    let mut total_cost: Cost = input_total_cost;

    // Window functions are charged their stated execution cost, plus the cost of
    // evaluating their input expressions + filter, per tuple.
    for &wfn in window_funcs {
        let (fn_startup, wfunccost) = cz::windowfunc_cost::call(run, root, wfn);
        startup_cost += fn_startup;
        total_cost += wfunccost * input_tuples;
    }

    // cpu_operator_cost per grouping column per tuple + cpu_tuple_cost per tuple.
    total_cost +=
        cpu_operator_cost() * (num_part_cols + num_order_cols) as f64 * input_tuples;
    total_cost += cpu_tuple_cost() * input_tuples;

    {
        let p = root.path_mut(path_id).base_mut();
        p.rows = input_tuples;
        p.disabled_nodes = input_disabled_nodes;
        p.startup_cost = startup_cost;
        p.total_cost = total_cost;
    }

    // Account for tuples we must read before the first output row.
    let startup_tuples = wc.startup_tuples;
    if startup_tuples > 1.0 {
        let p = root.path_mut(path_id).base_mut();
        p.startup_cost += (total_cost - startup_cost) / input_tuples * (startup_tuples - 1.0);
    }

    Ok(())
}

/* ==========================================================================
 * cost_rescan (costsize.c:4640) + cost_memoize_rescan (costsize.c:2541)
 * ========================================================================== */

/// `cost_rescan` (costsize.c:4640) — returns `(rescan_startup, rescan_total)`.
///
/// `run` is threaded for the Memoize arm, whose `cost_memoize_rescan` estimates
/// the distinct parameter count via `estimate_num_groups` (which examines the
/// param expressions through the [`PlannerRun`] RTE store). `root` is `&mut` for
/// the same reason (the examine path re-interns stripped expressions).
pub fn cost_rescan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
) -> PgResult<(Cost, Cost)> {
    use types_nodes::nodes;
    let node = root.path(path_id);
    let base = node.base();
    let result = match base.pathtype {
        x if x == nodes::T_FunctionScan => (0.0, base.total_cost - base.startup_cost),
        x if x == types_nodes::nodehashjoin::T_HashJoin => {
            let num_batches = match node {
                PathNode::HashPath(hp) => hp.num_batches,
                _ => 1,
            };
            if num_batches == 1 {
                (0.0, base.total_cost - base.startup_cost)
            } else {
                (base.startup_cost, base.total_cost)
            }
        }
        x if x == types_nodes::nodectescan::T_CteScan || x == nodes::T_WorkTableScan => {
            let pt_width = base
                .pathtarget
                .as_ref()
                .expect("cost_rescan: pathtarget must be set")
                .width;
            let mut run_cost = cpu_tuple_cost() * base.rows;
            let nbytes = relation_byte_size(base.rows, pt_width);
            let work_mem_bytes = work_mem() as f64 * 1024.0;
            if nbytes > work_mem_bytes {
                let npages = ceil(nbytes / BLCKSZ);
                run_cost += seq_page_cost() * npages;
            }
            (0.0, run_cost)
        }
        x if x == nodes::T_Material || x == nodes::T_Sort => {
            let pt_width = base
                .pathtarget
                .as_ref()
                .expect("cost_rescan: pathtarget must be set")
                .width;
            let mut run_cost = cpu_operator_cost() * base.rows;
            let nbytes = relation_byte_size(base.rows, pt_width);
            let work_mem_bytes = work_mem() as f64 * 1024.0;
            if nbytes > work_mem_bytes {
                let npages = ceil(nbytes / BLCKSZ);
                run_cost += seq_page_cost() * npages;
            }
            (0.0, run_cost)
        }
        x if x == types_nodes::nodememoize::T_Memoize => {
            return cost_memoize_rescan(run, root, path_id)
        }
        _ => (base.startup_cost, base.total_cost),
    };
    Ok(result)
}

/// `cost_memoize_rescan` (costsize.c:2541) — returns
/// `(rescan_startup_cost, rescan_total_cost)` and records
/// `MemoizePath.est_entries`. Now that the cost-model rescan path threads `run`
/// + `&mut root` (for the `estimate_num_groups` distinct-param estimate), this
/// matches the C 1:1, including the `mpath->est_entries` write.
pub fn cost_memoize_rescan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mpath_id: PathId,
) -> PgResult<(Cost, Cost)> {
    let (subpath, calls, param_exprs) = match root.path(mpath_id) {
        PathNode::MemoizePath(mp) => (
            mp.subpath.expect("cost_memoize_rescan: subpath must be set"),
            mp.calls,
            mp.param_exprs.clone(),
        ),
        _ => panic!("backend-optimizer-path-costsize::cost_memoize_rescan: path is not a MemoizePath"),
    };

    let (input_startup_cost, input_total_cost, tuples, width) = {
        let sp = root.path(subpath).base();
        (
            sp.startup_cost,
            sp.total_cost,
            sp.rows,
            sp.pathtarget
                .as_ref()
                .expect("cost_memoize_rescan: subpath pathtarget must be set")
                .width,
        )
    };

    let hash_mem_bytes = ps::get_hash_memory_limit::call();

    let mut est_entry_bytes =
        relation_byte_size(tuples, width) + cz::exec_estimate_cache_entry_overhead_bytes::call(tuples);

    for &pe in &param_exprs {
        est_entry_bytes += get_expr_width(root, pe) as f64;
    }

    let est_cache_entries = (hash_mem_bytes / est_entry_bytes).floor();

    // estimate on the distinct number of parameter values
    let mut estinfo = EstimationInfo::default();
    let mut ndistinct =
        selfuncs::estimate_num_groups::call(run, root, &param_exprs, calls, Some(&mut estinfo))?;

    // When the estimation fell back on using a default value, it's a bit too
    // risky to assume that it's ok to use a Memoize node.  The use of a default
    // could cause us to use a Memoize node when it's really inappropriate to do
    // so.  If we see that this has been done, then we'll assume that every call
    // will have unique parameters, which will almost certainly mean a
    // MemoizePath will never survive add_path(). (costsize.c:2589-2592)
    if (estinfo.flags & SELFLAG_USED_DEFAULT) != 0 {
        ndistinct = calls;
    }

    let pg_uint32_max = u32::MAX as f64;
    let est_entries = Min(Min(ndistinct, est_cache_entries), pg_uint32_max);

    // Store the number of entries -- 0 means unknown (C: mpath->est_entries).
    if let PathNode::MemoizePath(mp) = root.path_mut(mpath_id) {
        mp.est_entries = est_entries as u32;
    }

    let evict_ratio = 1.0 - Min(est_cache_entries, ndistinct) / ndistinct;

    let hit_ratio =
        ((calls - ndistinct) / calls) * (est_cache_entries / Max(ndistinct, est_cache_entries));

    debug_assert!(hit_ratio >= 0.0 && hit_ratio <= 1.0);

    let mut total_cost = input_total_cost * (1.0 - hit_ratio) + cpu_operator_cost();
    total_cost += cpu_tuple_cost() * evict_ratio;
    total_cost += cpu_operator_cost() / 10.0 * evict_ratio * tuples;
    total_cost += cpu_tuple_cost() + cpu_operator_cost() * tuples;

    let mut startup_cost = input_startup_cost * (1.0 - hit_ratio);
    startup_cost += cpu_tuple_cost();

    Ok((startup_cost, total_cost))
}

/* ==========================================================================
 * cost_subplan (costsize.c:4534)
 * ========================================================================== */

/// `SubLinkType` discriminants (primnodes.h) used by `cost_subplan`.
pub const EXISTS_SUBLINK: i32 = 0;
pub const ALL_SUBLINK: i32 = 1;
pub const ANY_SUBLINK: i32 = 2;

/// `cost_subplan` (costsize.c:4534). The C reads a `SubPlan*` and its child
/// `Plan*`; neither has an arena representation in the fabled model, so the
/// numeric plan fields and flags are passed in by value and the result is
/// returned as `(startup_cost, per_call_cost)`. `testexpr_quals` are the
/// implicit-AND'ed testexpr clause handles (`make_ands_implicit(subplan->testexpr)`).
///
/// `plan_materializes_output` is `ExecMaterializesOutput(nodeTag(plan))`,
/// computed by the caller (it crosses into the executor's node-tag table).
pub fn cost_subplan(
    root: &PlannerInfo,
    use_hash_table: bool,
    sublink_type: i32,
    par_param_is_nil: bool,
    plan_materializes_output: bool,
    plan_startup_cost: Cost,
    plan_total_cost: Cost,
    plan_rows: f64,
    testexpr_quals: &[NodeId],
) -> (Cost, Cost) {
    // cost_qual_eval(testexpr, NULL): the C passes root as NULL. We still pass
    // root since the walker tolerates it (consults no stats for these nodes).
    let mut sp_cost = cost_qual_eval(root, testexpr_quals);

    if use_hash_table {
        sp_cost.startup += plan_total_cost + cpu_operator_cost() * plan_rows;
    } else {
        let plan_run_cost = plan_total_cost - plan_startup_cost;

        if sublink_type == EXISTS_SUBLINK {
            sp_cost.per_tuple += plan_run_cost / clamp_row_est(plan_rows);
        } else if sublink_type == ALL_SUBLINK || sublink_type == ANY_SUBLINK {
            sp_cost.per_tuple += 0.50 * plan_run_cost;
            sp_cost.per_tuple += 0.50 * plan_rows * cpu_operator_cost();
        } else {
            sp_cost.per_tuple += plan_run_cost;
        }

        if par_param_is_nil && plan_materializes_output {
            sp_cost.startup += plan_startup_cost;
        } else {
            sp_cost.per_tuple += plan_startup_cost;
        }
    }

    (sp_cost.startup, sp_cost.per_tuple)
}

/// `cost_subplan(root, subplan, plan)` (costsize.c:4534) over the owned model:
/// reads the owned `SubPlan` and its finished `Plan` `Node`, fills in
/// `subplan.startup_cost` / `per_call_cost` in place. The testexpr cost is
/// computed over the owned `Expr` (the C `make_ands_implicit(subplan->testexpr)`
/// then `cost_qual_eval`); per-element costing folds into `cost_qual_eval_expr`,
/// which iterates the implicit-AND clauses.
pub fn cost_subplan_owned(
    root: &PlannerInfo,
    subplan: &mut types_nodes::primnodes::SubPlan<'_>,
    plan: &types_nodes::nodes::Node<'_>,
) -> PgResult<()> {
    use types_nodes::primnodes::SubLinkType;

    let head = plan.plan_head();
    let plan_startup = head.startup_cost;
    let plan_total = head.total_cost;
    let plan_rows = head.plan_rows;

    // cost_qual_eval(&sp_cost, make_ands_implicit(subplan->testexpr), NULL).
    // The owned testexpr is an Expr; cost_qual_eval_expr walks it (treating the
    // top-level AND as the implicit-AND clause list internally).
    let (mut sp_startup, mut sp_per_tuple) = match &subplan.testexpr {
        Some(te) => crate::qualcost::cost_qual_eval_expr(Some(root), te),
        None => (0.0, 0.0),
    };

    if subplan.useHashTable {
        sp_startup += plan_total + cpu_operator_cost() * plan_rows;
    } else {
        let plan_run_cost = plan_total - plan_startup;

        if subplan.subLinkType == SubLinkType::Exists {
            sp_per_tuple += plan_run_cost / clamp_row_est(plan_rows);
        } else if subplan.subLinkType == SubLinkType::All
            || subplan.subLinkType == SubLinkType::Any
        {
            sp_per_tuple += 0.50 * plan_run_cost;
            sp_per_tuple += 0.50 * plan_rows * cpu_operator_cost();
        } else {
            sp_per_tuple += plan_run_cost;
        }

        // parParam == NIL && ExecMaterializesOutput(nodeTag(plan)).
        let materializes = backend_optimizer_path_joinpath_seams::exec_materializes_output::call(
            plan.node_tag(),
        );
        if subplan.parParam.is_empty() && materializes {
            sp_startup += plan_startup;
        } else {
            sp_per_tuple += plan_startup;
        }
    }

    subplan.startup_cost = sp_startup;
    subplan.per_call_cost = sp_per_tuple;
    Ok(())
}

