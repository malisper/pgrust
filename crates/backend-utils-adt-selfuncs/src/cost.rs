//! The index-AM cost-estimation family of `utils/adt/selfuncs.c`:
//! `genericcostestimate`, `btcostestimate`, and their helpers
//! (`get_quals_from_indexclauses`, `index_other_operands_eval_cost`,
//! `add_predicate_to_index_quals`, `btcost_correlation`,
//! `examine_indexcol_variable`).
//!
//! These are reached across the dependency cycle through the costsize-owned
//! `amcostestimate` seam ([`backend_optimizer_path_costsize_seams::amcostestimate`]),
//! which `cost_index` (costsize.c) invokes for the index AM, and which each
//! AM's vtable cost slot (e.g. nbtree's `btcostestimate_am`) delegates to.
//! The dispatch on `index->relam` (here, btree) is done by the seam body.
//!
//! Faithfulness: 1:1 with the C, over the value-typed planner model. The
//! `clauselist_selectivity` over the mixed RestrictInfo/predicate list crosses
//! to clausesel.c through the `clauselist_selectivity_mixed` seam; the
//! Mackert-Lohman `index_pages_fetched`, `get_tablespace_page_costs`,
//! `cost_qual_eval_node`, and the `cpu_*_cost` GUC globals cross to costsize.c.

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    IndexOptInfo, NodeId, PathId, PathNode, PlannerInfo, RinfoId, JOIN_INNER,
};
use types_pathnodes::planner_run::PlannerRun;
use types_selfuncs::{VariableStatData, ATTSTATSSLOT_NUMBERS};
use types_statistics::STATISTIC_KIND_CORRELATION;
use types_scan::scankey::{BTEqualStrategyNumber, BTLessStrategyNumber};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_path_costsize_seams::AmCostEstimate;
use backend_optimizer_path_small_seams as sel;
use backend_optimizer_path_small_seams::ClauseListEntry;
use backend_optimizer_util_predtest_seams as predtest;
use backend_utils_cache_lsyscache_seams as lsc;

use crate::examine::examine_indexcol_variable;
use crate::scalar::get_variable_numdistinct;

/// `DEFAULT_PAGE_CPU_MULTIPLIER` (selfuncs.c) — the per-page CPU cost charge
/// for descending an index, as a multiple of `cpu_operator_cost`.
const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;

/// `DEFAULT_RANGE_INEQ_SEL` (selfuncs.h) — `0.005`; the default selectivity for
/// a range-style inequality pair.
const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

/// `GenericCosts` (selfuncs.h) — the intermediate/final values shared between
/// `genericcostestimate` and a specific AM's cost estimator (e.g.
/// `btcostestimate`). Inputs the AM may set are `numIndexTuples` /
/// `num_sa_scans`; the rest are filled by `genericcostestimate`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GenericCosts {
    /// Estimated cost to fetch and use one index tuple, startup component.
    pub index_startup_cost: f64,
    /// Estimated total cost.
    pub index_total_cost: f64,
    /// Estimated selectivity (fraction of main-table tuples visited).
    pub index_selectivity: f64,
    /// Estimated index-order correlation.
    pub index_correlation: f64,
    /// Number of leaf index pages visited.
    pub num_index_pages: f64,
    /// Number of index tuples visited.
    pub num_index_tuples: f64,
    /// `spc_random_page_cost` for the index's tablespace.
    pub spc_random_page_cost: f64,
    /// Number of ScalarArrayOp index scans (1 if none).
    pub num_sa_scans: f64,
}

/// `get_quals_from_indexclauses(indexclauses)` (selfuncs.c) — get the
/// implicitly-ANDed list of index qual RestrictInfos from an `IndexPath`'s
/// `indexclauses`. Returns the [`RinfoId`] list (the C `RestrictInfo *` list).
fn get_quals_from_indexclauses(path: &types_pathnodes::IndexPath) -> Vec<RinfoId> {
    let mut result: Vec<RinfoId> = Vec::new();
    for iclause in &path.indexclauses {
        for &rinfo in &iclause.indexquals {
            result.push(rinfo);
        }
    }
    result
}

/// `index_other_operands_eval_cost(root, indexquals)` (selfuncs.c) — total cost
/// to evaluate the "other operands" (the non-index-column inputs) of a list of
/// index quals. The list elements are RestrictInfos (`indexQuals`) or bare
/// ORDER BY expressions (`indexOrderBys`); pass each through [`ClauseRef`].
fn index_other_operands_eval_cost(
    root: &mut PlannerInfo,
    quals: &[ClauseRef],
) -> f64 {
    let mut qual_arg_cost = 0.0f64;

    for q in quals {
        // Index quals will have RestrictInfos, indexorderbys won't. Look through
        // RestrictInfo if present.
        let clause: Expr = match q {
            ClauseRef::Rinfo(rid) => root.node(root.rinfo(*rid).clause).clone(),
            ClauseRef::Bare(node) => root.node(*node).clone(),
        };

        let other_operand: Option<Expr> = match &clause {
            Expr::OpExpr(op) => op.args.get(1).cloned(),
            Expr::RowCompareExpr(rc) => rc.rargs.first().cloned(),
            Expr::ScalarArrayOpExpr(saop) => saop.args.get(1).cloned(),
            Expr::NullTest(_) => None,
            other => panic!(
                "unsupported indexqual type: {:?}",
                core::mem::discriminant(other)
            ),
        };

        let (startup, per_tuple) = match other_operand {
            Some(operand) => {
                let node_id = root.alloc_node(operand);
                cz::cost_qual_eval_walker::call(root, node_id)
            }
            None => (0.0, 0.0),
        };
        qual_arg_cost += startup + per_tuple;
    }
    qual_arg_cost
}

/// An element of a qual list `index_other_operands_eval_cost` walks — either a
/// RestrictInfo (index qual) or a bare expression node (ORDER BY).
enum ClauseRef {
    Rinfo(RinfoId),
    Bare(NodeId),
}

/// `add_predicate_to_index_quals(index, indexQuals)` (selfuncs.c) — if the index
/// is partial, AND its predicate clauses (those not implied by the indexquals)
/// with the given index quals to produce a more accurate selectivity estimate.
/// Returns the mixed RestrictInfo/bare-predicate list for
/// `clauselist_selectivity`. `predicate_implied_by` (predtest.c) takes bare
/// `Node *` lists, so the indexqual RestrictInfos' clause nodes are resolved to
/// NodeIds for the implication test.
fn add_predicate_to_index_quals(
    root: &PlannerInfo,
    index: &IndexOptInfo,
    index_quals: &[RinfoId],
) -> PgResult<Vec<ClauseListEntry>> {
    let mut result: Vec<ClauseListEntry> = Vec::new();

    if index.indpred.is_empty() {
        for &r in index_quals {
            result.push(ClauseListEntry::Rinfo(r));
        }
        return Ok(result);
    }

    // The indexqual RestrictInfos' clause nodes, for the implication test
    // (predicate_implied_by's `restriction_clauses` is the C indexQuals list,
    // where it reads each RestrictInfo through `predicate_classify`).
    let restriction_nodes: Vec<NodeId> =
        index_quals.iter().map(|&r| root.rinfo(r).clause).collect();

    let mut pred_extra: Vec<ClauseListEntry> = Vec::new();
    for &pred_qual in &index.indpred {
        let one_qual = [pred_qual];
        if !predtest::predicate_implied_by::call(root, &one_qual, &restriction_nodes, false) {
            pred_extra.push(ClauseListEntry::Bare(root.node(pred_qual).clone()));
        }
    }
    result.extend(pred_extra);
    for &r in index_quals {
        result.push(ClauseListEntry::Rinfo(r));
    }
    Ok(result)
}

/// `genericcostestimate(root, path, loop_count, costs)` (selfuncs.c) — the
/// AM-independent core of index cost estimation. The caller (an AM-specific
/// estimator like [`btcostestimate`]) may preset `costs.num_index_tuples` and
/// `costs.num_sa_scans`; this fills the rest. 1:1 with the C body.
pub(crate) fn genericcostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
    costs: &mut GenericCosts,
) -> PgResult<()> {
    // Pull the fields we need out of the IndexPath / IndexOptInfo (cloned to
    // release the borrow before the &mut root calls below).
    let (index, index_quals, index_order_bys): (IndexOptInfo, Vec<RinfoId>, Vec<NodeId>) = {
        let path = expect_index_path(root, path_id);
        let index = (**path
            .indexinfo
            .as_ref()
            .expect("genericcostestimate: indexinfo must be set"))
        .clone();
        let quals = get_quals_from_indexclauses(path);
        let order_bys = path.indexorderbys.clone();
        (index, quals, order_bys)
    };
    let index_rel = index
        .rel
        .expect("genericcostestimate: index.rel must be set");
    let index_rel_relid = root.rel(index_rel).relid as i32;
    let index_rel_tuples = root.rel(index_rel).tuples;

    // If the index is partial, AND the index predicate with the explicitly
    // given indexquals to produce a more accurate idea of the index selectivity.
    let selectivity_quals = add_predicate_to_index_quals(root, &index, &index_quals)?;

    // If caller didn't give us an estimate for ScalarArrayOpExpr index scans,
    // just assume that the number of index descents is the number of distinct
    // combinations of array elements from all of the scan's SAOP clauses.
    let mut num_sa_scans = costs.num_sa_scans;
    if num_sa_scans < 1.0 {
        num_sa_scans = 1.0;
        for &rid in &index_quals {
            let clause = root.node(root.rinfo(rid).clause).clone();
            if let Some(saop) = clause.as_scalararrayopexpr() {
                if let Some(arr) = saop.args.get(1).cloned() {
                    let node_id = root.alloc_node(arr);
                    let alength = crate::misc::estimate_array_length(mcx, root, node_id)?;
                    if alength > 1.0 {
                        num_sa_scans *= alength;
                    }
                }
            }
        }
    }

    // Estimate the fraction of main-table tuples that will be visited.
    let index_selectivity = sel::clauselist_selectivity_mixed::call(
        run,
        root,
        &selectivity_quals,
        index_rel_relid,
        JOIN_INNER,
        None,
    )?;

    // If caller didn't give us an estimate, estimate the number of index tuples
    // that will be visited (the peculiar form is for partial indexes).
    let mut num_index_tuples = costs.num_index_tuples;
    if num_index_tuples <= 0.0 {
        num_index_tuples = index_selectivity * index_rel_tuples;
        num_index_tuples = (num_index_tuples / num_sa_scans).round();
    }

    // We can bound the number of tuples by the index size in any case. Also,
    // always estimate at least one tuple is touched.
    if num_index_tuples > index.tuples {
        num_index_tuples = index.tuples;
    }
    if num_index_tuples < 1.0 {
        num_index_tuples = 1.0;
    }

    // Estimate the number of index pages that will be retrieved (pro-rata).
    let num_index_pages = if index.pages > 1 && index.tuples > 1.0 {
        (num_index_tuples * index.pages as f64 / index.tuples).ceil()
    } else {
        1.0
    };

    // Fetch estimated page cost for tablespace containing index.
    let spc = cz::get_tablespace_page_costs::call(index.reltablespace);
    let spc_random_page_cost = spc.spc_random_page_cost;

    // Disk access costs (per-index-scan; adjust for nestloop/SAOP repeats via
    // the Mackert-Lohman formula).
    let num_outer_scans = loop_count;
    let num_scans = num_sa_scans * num_outer_scans;

    let index_total_cost: f64 = if num_scans > 1.0 {
        let mut pages_fetched = num_index_pages * num_scans;
        pages_fetched = cz::index_pages_fetched::call(
            pages_fetched,
            index.pages,
            index.pages as f64,
            root,
        );
        (pages_fetched * spc_random_page_cost) / num_outer_scans
    } else {
        num_index_pages * spc_random_page_cost
    };

    // CPU cost: complex expressions in the indexquals are evaluated once at scan
    // start; per-tuple cost is cpu_index_tuple_cost + one cpu_operator_cost per
    // indexqual operator (multiplied by num_sa_scans for SAOP cases). Similarly
    // add costs for any index ORDER BY expressions.
    let qual_refs: Vec<ClauseRef> =
        index_quals.iter().map(|&r| ClauseRef::Rinfo(r)).collect();
    let orderby_refs: Vec<ClauseRef> =
        index_order_bys.iter().map(|&n| ClauseRef::Bare(n)).collect();
    let qual_arg_cost = index_other_operands_eval_cost(root, &qual_refs)
        + index_other_operands_eval_cost(root, &orderby_refs);
    let cpu_operator_cost = cz::cpu_operator_cost::call();
    let cpu_index_tuple_cost = cz::cpu_index_tuple_cost::call();
    let qual_op_cost = cpu_operator_cost
        * (index_quals.len() as f64 + index_order_bys.len() as f64);

    let index_startup_cost = qual_arg_cost;
    let index_total_cost = index_total_cost
        + qual_arg_cost
        + num_index_tuples * num_sa_scans * (cpu_index_tuple_cost + qual_op_cost);

    // Generic assumption about index correlation: there isn't any.
    let index_correlation = 0.0;

    costs.index_startup_cost = index_startup_cost;
    costs.index_total_cost = index_total_cost;
    costs.index_selectivity = index_selectivity;
    costs.index_correlation = index_correlation;
    costs.num_index_pages = num_index_pages;
    costs.num_index_tuples = num_index_tuples;
    costs.spc_random_page_cost = spc_random_page_cost;
    costs.num_sa_scans = num_sa_scans;
    Ok(())
}

/// `btcost_correlation(index, vardata)` (selfuncs.c) — estimate the correlation
/// of a btree index's first column from its `pg_statistic` correlation slot.
/// The caller has already filled `vardata.statsTuple`.
fn btcost_correlation<'mcx>(
    mcx: Mcx<'mcx>,
    index: &IndexOptInfo,
    vardata: &VariableStatData,
) -> PgResult<f64> {
    debug_assert!(vardata.stats_tuple.is_some());

    let mut index_correlation = 0.0f64;

    let sortop = lsc::get_opfamily_member::call(
        index.opfamily[0],
        index.opcintype[0],
        index.opcintype[0],
        BTLessStrategyNumber as i16,
    )?;
    if OidIsValid(sortop) {
        if let Some(stats_tuple) = vardata.stats_tuple {
            if let Some(sslot) = lsc::get_attstatsslot::call(
                mcx,
                stats_tuple,
                STATISTIC_KIND_CORRELATION as i32,
                sortop,
                ATTSTATSSLOT_NUMBERS,
            )? {
                debug_assert!(sslot.numbers.len() == 1);
                let mut var_correlation = sslot.numbers[0] as f64;

                if index.reverse_sort[0] {
                    var_correlation = -var_correlation;
                }

                if index.nkeycolumns > 1 {
                    index_correlation = var_correlation * 0.75;
                } else {
                    index_correlation = var_correlation;
                }
            }
        }
    }

    Ok(index_correlation)
}

/// `btcostestimate(root, path, loop_count, ...)` (selfuncs.c) — the btree index
/// AM's cost estimator. Determines the btree boundary quals, computes
/// `numIndexTuples`, then calls [`genericcostestimate`] and adds btree-specific
/// descent costs and correlation. 1:1 with the C body. Returns the five output
/// estimates as an [`AmCostEstimate`].
pub(crate) fn btcostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index, indexclauses): (IndexOptInfo, Vec<types_pathnodes::IndexClause>) = {
        let path = expect_index_path(root, path_id);
        let index = (**path
            .indexinfo
            .as_ref()
            .expect("btcostestimate: indexinfo must be set"))
        .clone();
        (index, path.indexclauses.clone())
    };
    let index_rel = index.rel.expect("btcostestimate: index.rel must be set");
    let index_rel_relid = root.rel(index_rel).relid as i32;
    let index_rel_tuples = root.rel(index_rel).tuples;

    let mut costs = GenericCosts::default();
    let num_index_tuples: f64;

    // Only leading '=' quals plus inequality quals for the immediately next
    // attribute contribute to index selectivity ("boundary quals"). Walk the
    // index clauses (given in index column order) to find them.
    let mut index_bound_quals: Vec<RinfoId> = Vec::new();
    let mut index_skip_quals: Vec<RinfoId> = Vec::new();
    let mut indexcol: i32 = 0;
    let mut eq_qual_here = false;
    let mut found_row_compare = false;
    let mut found_array = false;
    let mut found_is_null_op = false;
    let mut num_sa_scans = 1.0f64;
    let mut correlation = 0.0f64;
    let mut have_correlation = false;

    'outer: for iclause in &indexclauses {
        if indexcol < iclause.indexcol as i32 {
            let num_sa_scans_prev_cols = num_sa_scans;

            // Beginning of a new column's quals. Consider how nbtree will
            // backfill skip arrays for index columns that lacked an '=' qual.
            if found_row_compare {
                // Skip arrays can't be added after a RowCompare input qual.
                break;
            }
            if eq_qual_here {
                // Don't need a skip array for an indexcol that already has '='.
                indexcol += 1;
                index_skip_quals.clear();
            }
            eq_qual_here = false;

            while indexcol < iclause.indexcol as i32 {
                found_array = true;

                // A skipped attribute's ndistinct forms the basis of our
                // estimate of the total number of "array elements".
                let mut vardata = VariableStatData::zeroed(NodeId::default());
                examine_indexcol_variable(mcx, run, root, &index, indexcol as usize, &mut vardata)?;
                let (mut ndistinct, isdefault) = get_variable_numdistinct(root, &vardata);

                if indexcol == 0 {
                    // Get an estimate of the leading column's correlation in
                    // passing (avoids rereading variable stats below).
                    if vardata.stats_tuple.is_some() {
                        correlation = btcost_correlation(mcx, &index, &vardata)?;
                    }
                    have_correlation = true;
                }

                crate::examine::release_variable_stats(vardata);

                // If ndistinct is a default estimate, conservatively assume that
                // no skipping will happen at runtime.
                if isdefault {
                    num_sa_scans = num_sa_scans_prev_cols;
                    break;
                }

                // Apply indexcol's indexSkipQuals selectivity to ndistinct.
                if !index_skip_quals.is_empty() {
                    let partial_skip_quals =
                        add_predicate_to_index_quals(root, &index, &index_skip_quals)?;
                    let ndistinctfrac = sel::clauselist_selectivity_mixed::call(
                        run,
                        root,
                        &partial_skip_quals,
                        index_rel_relid,
                        JOIN_INNER,
                        None,
                    )?;

                    // If ndistinctfrac is selective on its own, the scan is
                    // unlikely to benefit from repositioning using later quals.
                    if ndistinctfrac < DEFAULT_RANGE_INEQ_SEL {
                        num_sa_scans = num_sa_scans_prev_cols;
                        break;
                    }

                    ndistinct = (ndistinct * ndistinctfrac).round();
                    ndistinct = ndistinct.max(1.0);
                }

                // When there's no inequality quals, account for the need to find
                // an initial value by counting -inf/+inf as a value.
                if index_skip_quals.is_empty() {
                    ndistinct += 1.0;
                }

                // Update num_sa_scans estimate by multiplying by ndistinct.
                num_sa_scans *= ndistinct;

                // ...but back out of adding this latest group of skip arrays when
                // num_sa_scans exceeds the total number of index pages.
                if (index.pages as f64) < num_sa_scans {
                    num_sa_scans = num_sa_scans_prev_cols;
                    break;
                }

                indexcol += 1;
                index_skip_quals.clear();
            }

            // If an initial gap could not be bridged, this column's quals won't
            // go into indexBoundQuals (and so won't affect numIndexTuples).
            if indexcol != iclause.indexcol as i32 {
                break;
            }
        }

        debug_assert!(indexcol == iclause.indexcol as i32);

        // Examine each indexqual associated with this index clause.
        for &rid in &iclause.indexquals {
            let clause = root.node(root.rinfo(rid).clause).clone();
            let mut clause_op: Oid = InvalidOid;

            match &clause {
                Expr::OpExpr(op) => {
                    clause_op = op.opno;
                }
                Expr::RowCompareExpr(rc) => {
                    clause_op = *rc.opnos.first().expect("RowCompareExpr opnos empty");
                    found_row_compare = true;
                }
                Expr::ScalarArrayOpExpr(saop) => {
                    if let Some(other_operand) = saop.args.get(1).cloned() {
                        let node_id = root.alloc_node(other_operand);
                        let alength = crate::misc::estimate_array_length(mcx, root, node_id)?;
                        clause_op = saop.opno;
                        found_array = true;
                        if alength > 1.0 {
                            num_sa_scans *= alength;
                        }
                    } else {
                        clause_op = saop.opno;
                        found_array = true;
                    }
                }
                Expr::NullTest(nt) => {
                    if nt.nulltesttype == types_nodes::primnodes::NullTestType::IS_NULL {
                        found_is_null_op = true;
                        // IS NULL is like = for selectivity/skip scan purposes.
                        eq_qual_here = true;
                    }
                }
                other => panic!(
                    "unsupported indexqual type: {:?}",
                    core::mem::discriminant(other)
                ),
            }

            // Check for equality operator.
            if OidIsValid(clause_op) {
                let op_strategy = lsc::get_op_opfamily_strategy::call(
                    clause_op,
                    index.opfamily[indexcol as usize],
                )?;
                debug_assert!(op_strategy != 0);
                if op_strategy == BTEqualStrategyNumber as i32 {
                    eq_qual_here = true;
                }
            }

            index_bound_quals.push(rid);

            // We apply inequality selectivities to estimate index descent costs
            // with scans that use skip arrays. Save this indexcol's RestrictInfos
            // if it looks like they'll be needed for that.
            if !eq_qual_here
                && !found_row_compare
                && indexcol < index.nkeycolumns - 1
            {
                index_skip_quals.push(rid);
            }
        }
        // (loop label only to satisfy the borrow checker's break targets above)
        #[allow(clippy::never_loop)]
        if false {
            break 'outer;
        }
    }

    // If index is unique and we found an '=' clause for each column, we can just
    // assume numIndexTuples = 1 (unless an array or NullTest invalidates that).
    if index.unique
        && indexcol == index.nkeycolumns - 1
        && eq_qual_here
        && !found_array
        && !found_is_null_op
    {
        num_index_tuples = 1.0;
    } else {
        // If the index is partial, AND the index predicate with the index-bound
        // quals to produce a more accurate idea of the rows covered.
        let selectivity_quals = add_predicate_to_index_quals(root, &index, &index_bound_quals)?;
        let btree_selectivity = sel::clauselist_selectivity_mixed::call(
            run,
            root,
            &selectivity_quals,
            index_rel_relid,
            JOIN_INNER,
            None,
        )?;
        let mut nit = btree_selectivity * index_rel_tuples;

        // Clamp the number of descents to at most 1/3 the number of index pages.
        num_sa_scans = num_sa_scans.min((index.pages as f64 * 0.3333333).ceil());
        num_sa_scans = num_sa_scans.max(1.0);

        // As in genericcostestimate(), adjust for array quals and round.
        nit = (nit / num_sa_scans).round();
        num_index_tuples = nit;
    }

    // Now do generic index cost estimation.
    costs.num_index_tuples = num_index_tuples;
    costs.num_sa_scans = num_sa_scans;

    genericcostestimate(mcx, run, root, path_id, loop_count, &mut costs)?;

    let cpu_operator_cost = cz::cpu_operator_cost::call();

    // Add a CPU-cost component for the initial btree descent: about log2(N)
    // comparisons. Charge once per estimated index descent.
    if index.tuples > 1.0 {
        let descent_cost = (index.tuples.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }

    // A per-page CPU cost for each page descended through (tree height + 1).
    let descent_cost =
        (index.tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    if !have_correlation {
        let mut vardata = VariableStatData::zeroed(NodeId::default());
        examine_indexcol_variable(mcx, run, root, &index, 0, &mut vardata)?;
        if vardata.stats_tuple.is_some() {
            costs.index_correlation = btcost_correlation(mcx, &index, &vardata)?;
        }
        crate::examine::release_variable_stats(vardata);
    } else {
        costs.index_correlation = correlation;
    }

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

/// `hashcostestimate(root, path, loop_count, ...)` (selfuncs.c) — the hash index
/// AM's cost estimator. A hash index has no descent costs (the AM goes directly
/// to the target bucket after computing the hash value), and the C body
/// deliberately adds no other hash-specific costs, so it simply runs
/// [`genericcostestimate`] and returns its results unmodified. 1:1 with the C
/// body.
pub(crate) fn hashcostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let mut costs = GenericCosts::default();
    genericcostestimate(mcx, run, root, path_id, loop_count, &mut costs)?;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

/// `&mut`-borrow the `IndexPath` for a `PathId`, panicking if it is not one.
fn expect_index_path(root: &PlannerInfo, path_id: PathId) -> &types_pathnodes::IndexPath {
    match root.path(path_id) {
        PathNode::IndexPath(ip) => ip,
        _ => panic!("expect_index_path: path is not an IndexPath"),
    }
}

/// The `amcostestimate` seam body (costsize.c dispatch into the index AM's cost
/// routine). Dispatches on the index's access method. For btree this is
/// [`btcostestimate`]. The owner crate installs this from [`crate::init_seams`].
pub fn seam_amcostestimate<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    path_id: PathId,
    loop_count: f64,
) -> AmCostEstimate {
    let relam = {
        let path = expect_index_path(root, path_id);
        path.indexinfo
            .as_ref()
            .expect("amcostestimate: indexinfo must be set")
            .relam
    };
    let mcx = run.mcx();
    match relam {
        // BTREE_AM_OID (pg_am.h) — the btree access method.
        403 => btcostestimate(mcx, run, root, path_id, loop_count)
            .expect("btcostestimate"),
        // HASH_AM_OID (pg_am.h) — the hash access method.
        405 => hashcostestimate(mcx, run, root, path_id, loop_count)
            .expect("hashcostestimate"),
        other => panic!(
            "amcostestimate: no cost estimator ported for index AM oid {}",
            other
        ),
    }
}
