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
use types_error::PgError;
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    IndexOptInfo, NodeId, PathId, PathNode, PlannerInfo, RinfoId, JOIN_INNER,
};
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_selfuncs::{VariableStatData, ATTSTATSSLOT_NUMBERS};
use types_statistics::STATISTIC_KIND_CORRELATION;
use types_scan::scankey::{BTEqualStrategyNumber, BTLessStrategyNumber};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_path_costsize_seams::AmCostEstimate;
use backend_optimizer_path_small_seams as sel;
use backend_optimizer_path_small_seams::ClauseListEntry;
use backend_optimizer_util_predtest_seams as predtest;
use backend_utils_cache_lsyscache_seams as lsc;
use backend_access_brin_insert_vacuum_seams as brin_iv;
use backend_access_index_indexam_seams as indexam;
use backend_access_gin_ginutil_seams as gin;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_adt_arrayfuncs_seams as arr;

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
fn index_other_operands_eval_cost<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    quals: &[ClauseRef],
) -> PgResult<f64> {
    let mut qual_arg_cost = 0.0f64;

    for q in quals {
        // Index quals will have RestrictInfos, indexorderbys won't. Look through
        // RestrictInfo if present. copyObject shape: the clause may carry a
        // correlated SubPlan operand whose derived `Expr::clone` panics, so
        // deep-copy through `clone_in` (it also drops the `&root` borrow so the
        // operand can `alloc_node` below).
        let clause: Expr = match q {
            ClauseRef::Rinfo(rid) => root.node(root.rinfo(*rid).clause).clone_in(mcx)?,
            ClauseRef::Bare(node) => root.node(*node).clone_in(mcx)?,
        };

        // `clause` is an owned deep copy, so MOVE the second operand out of it
        // (C reads `get_rightop(clause)` as a borrowed pointer). A derived
        // `.cloned()` here would recurse into the panicking `Expr::clone` arm
        // when the operand is a correlated SubPlan.
        let other_operand: Option<Expr> = match clause {
            Expr::OpExpr(mut op) => {
                if op.args.len() > 1 {
                    Some(op.args.swap_remove(1))
                } else {
                    None
                }
            }
            Expr::RowCompareExpr(mut rc) => {
                if rc.rargs.is_empty() {
                    None
                } else {
                    Some(rc.rargs.swap_remove(0))
                }
            }
            Expr::ScalarArrayOpExpr(mut saop) => {
                if saop.args.len() > 1 {
                    Some(saop.args.swap_remove(1))
                } else {
                    None
                }
            }
            Expr::NullTest(_) => None,
            other => panic!(
                "unsupported indexqual type: {:?}",
                core::mem::discriminant(&other)
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
    Ok(qual_arg_cost)
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
fn add_predicate_to_index_quals<'mcx>(
    mcx: Mcx<'mcx>,
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
            // `ClauseListEntry::Bare` carries the planner-arena `'static` form
            // (the seam owns that intern slot); clone the predicate clause into
            // `mcx` then erase to the arena lifetime at this intern boundary.
            pred_extra.push(ClauseListEntry::Bare(
                root.node(pred_qual).clone_in(mcx)?.erase_lifetime(),
            ));
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
    let selectivity_quals = add_predicate_to_index_quals(mcx, root, &index, &index_quals)?;

    // If caller didn't give us an estimate for ScalarArrayOpExpr index scans,
    // just assume that the number of index descents is the number of distinct
    // combinations of array elements from all of the scan's SAOP clauses.
    let mut num_sa_scans = costs.num_sa_scans;
    if num_sa_scans < 1.0 {
        num_sa_scans = 1.0;
        for &rid in &index_quals {
            // copyObject shape: deep-copy via `clone_in` (a SAOP operand may be a
            // SubPlan; also drops the `&root` borrow for the `alloc_node` below).
            let clause = root.node(root.rinfo(rid).clause).clone_in(mcx)?;
            if let Some(saop) = clause.as_scalararrayopexpr() {
                // copyObject shape: deep-copy the array operand via `clone_in`.
                if let Some(arr) = saop.args.get(1).map(|e| e.clone_in(mcx)).transpose()? {
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
    let qual_arg_cost = index_other_operands_eval_cost(mcx, root, &qual_refs)?
        + index_other_operands_eval_cost(mcx, root, &orderby_refs)?;
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
                        add_predicate_to_index_quals(mcx, root, &index, &index_skip_quals)?;
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
            // copyObject shape: an OpExpr/SAOP indexqual operand may be a
            // correlated SubPlan whose derived `Expr::clone` panics; deep-copy
            // through `clone_in` (the clone also drops the `&root` borrow so the
            // SAOP arm can `alloc_node` below).
            let clause = root.node(root.rinfo(rid).clause).clone_in(mcx)?;
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
                    // copyObject shape: deep-copy the array operand via `clone_in`
                    // (a `= ANY (subquery)` operand can be a SubPlan, whose derived
                    // `Expr::clone` panics).
                    if let Some(other_operand) =
                        saop.args.get(1).map(|e| e.clone_in(mcx)).transpose()?
                    {
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
        let selectivity_quals = add_predicate_to_index_quals(mcx, root, &index, &index_bound_quals)?;
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

/// `gistcostestimate(root, path, loop_count, ...)` (gistutil.c) — the GiST index
/// AM's cost estimator. Runs the AM-independent [`genericcostestimate`], then
/// adds the same two CPU descent-cost components btree charges: a `log2(N)`
/// comparison cost for the initial descent (when the index has more than one
/// tuple), and a per-page CPU cost for each page descended through
/// (`tree_height + 1`). GiST presets neither `num_index_tuples` nor
/// `num_sa_scans` (it has no ScalarArrayOp special-casing), so `num_sa_scans`
/// stays at `genericcostestimate`'s default of 1. 1:1 with the C body.
pub(crate) fn gistcostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let index: IndexOptInfo = {
        let path = expect_index_path(root, path_id);
        (**path
            .indexinfo
            .as_ref()
            .expect("gistcostestimate: indexinfo must be set"))
        .clone()
    };

    let mut costs = GenericCosts::default();
    genericcostestimate(mcx, run, root, path_id, loop_count, &mut costs)?;

    let cpu_operator_cost = cz::cpu_operator_cost::call();

    // We model index descent costs similarly to those for btree. Add a CPU-cost
    // component to represent the initial descent: about log2(N) comparisons.
    if index.tuples > 1.0 {
        let descent_cost = (index.tuples.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }

    // Add a CPU-cost component for each page descended through (tree_height + 1).
    let descent_cost =
        (index.tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

/// `spgcostestimate(root, path, loop_count, ...)` (selfuncs.c) — the SP-GiST
/// index AM's cost estimator. Runs the AM-independent [`genericcostestimate`],
/// then adds btree-style descent costs. Unlike btree/GiST, SP-GiST first derives
/// an estimate of the tree height when it is unknown (`tree_height < 0`) by
/// assuming a fanout of 100, i.e. `log100(pages)`, and caches it back onto the
/// `IndexOptInfo`. The initial-descent CPU cost uses `ceil(log(N))` (natural log,
/// since the branching factor isn't necessarily two), and the per-page charge is
/// computed the same as for btrees (`tree_height + 1`). SP-GiST presets neither
/// `num_index_tuples` nor `num_sa_scans`, so `num_sa_scans` stays at
/// `genericcostestimate`'s default of 1. 1:1 with the C body.
pub(crate) fn spgcostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let mut costs = GenericCosts::default();
    genericcostestimate(mcx, run, root, path_id, loop_count, &mut costs)?;

    // We model index descent costs similarly to those for btree, but to do that
    // we first need an idea of the tree height. We somewhat arbitrarily assume
    // that the fanout is 100, meaning the tree height is at most
    // log100(index->pages). Cache the result back onto the IndexOptInfo via
    // index->tree_height, as the C body does.
    let index = {
        let path = expect_index_path_mut(root, path_id);
        let index = path
            .indexinfo
            .as_mut()
            .expect("spgcostestimate: indexinfo must be set");
        if index.tree_height < 0 {
            // unknown?
            index.tree_height = if index.pages > 1 {
                // avoid computing log(0)
                (((index.pages as f64).ln()) / 100.0f64.ln()) as i32
            } else {
                0
            };
        }
        (**index).clone()
    };

    let cpu_operator_cost = cz::cpu_operator_cost::call();

    // Add a CPU-cost component to represent the costs of initial descent. We just
    // use log(N) here not log2(N) since the branching factor isn't necessarily
    // two anyway. As for btree, charge once per SA scan.
    if index.tuples > 1.0 {
        // avoid computing log(0)
        let descent_cost = index.tuples.ln().ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }

    // Likewise add a per-page charge, calculated the same as for btrees.
    let descent_cost =
        (index.tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

/// `brincostestimate(root, path, loop_count, ...)` (selfuncs.c) — the BRIN
/// index AM's cost estimator. Estimates the number of block-ranges the scan
/// touches from the index correlation and qual selectivity, then prices the
/// revmap (sequential) + regular-page (random) reads plus a per-range bitmap
/// charge. 1:1 with the C body. Returns the five output cost components.
pub(crate) fn brincostestimate<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    /// `BRIN_DEFAULT_PAGES_PER_RANGE` (brin.h).
    const BRIN_DEFAULT_PAGES_PER_RANGE: f64 = 128.0;
    /// `REVMAP_PAGE_MAXITEMS` (brin_page.h) for `BLCKSZ == 8192`:
    /// `(8192 - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(SizeOfBrinSpecial)) /
    /// sizeof(ItemPointerData)` == `(8192 - 24 - 4) / 6` == `1360`. Only the
    /// hypothetical-index branch reads it.
    const REVMAP_PAGE_MAXITEMS: f64 = 1360.0;

    let (index, indexclauses): (IndexOptInfo, Vec<types_pathnodes::IndexClause>) = {
        let path = expect_index_path(root, path_id);
        let index = (**path
            .indexinfo
            .as_ref()
            .expect("brincostestimate: indexinfo must be set"))
        .clone();
        (index, path.indexclauses.clone())
    };

    let index_quals: Vec<RinfoId> = {
        let path = expect_index_path(root, path_id);
        get_quals_from_indexclauses(path)
    };

    let num_pages = index.pages as f64;
    let baserel = index.rel.expect("brincostestimate: index.rel must be set");
    let baserel_relid = root.rel(baserel).relid as i32;
    let baserel_pages = root.rel(baserel).pages as f64;

    // RTE must be a plain relation (Assert(rte->rtekind == RTE_RELATION)).
    {
        let rte = planner_rt_fetch(run, root, root.rel(baserel).relid);
        debug_assert_eq!(
            rte.rtekind,
            types_nodes::parsenodes::RTEKind::RTE_RELATION
        );
    }

    // Fetch estimated page cost for the tablespace containing the index.
    let spc = cz::get_tablespace_page_costs::call(index.reltablespace);
    let spc_seq_page_cost = spc.spc_seq_page_cost;
    let spc_random_page_cost = spc.spc_random_page_cost;

    // Obtain some data from the index itself, if possible. Otherwise invent some
    // plausible internal statistics based on the relation page count.
    let (index_ranges, stats_pages_per_range, stats_revmap_num_pages) = if !index.hypothetical {
        // A lock should have already been obtained on the index in plancat.c.
        let stats = brin_iv::brin_get_stats::call(mcx, index.indexoid)?;
        let index_ranges =
            (baserel_pages / stats.pages_per_range as f64).ceil().max(1.0);
        (index_ranges, stats.pages_per_range as f64, stats.revmap_num_pages as f64)
    } else {
        // Assume default number of pages per range.
        let index_ranges =
            (baserel_pages / BRIN_DEFAULT_PAGES_PER_RANGE).ceil().max(1.0);
        let revmap_num_pages = (index_ranges / REVMAP_PAGE_MAXITEMS) + 1.0;
        (index_ranges, BRIN_DEFAULT_PAGES_PER_RANGE, revmap_num_pages)
    };

    // Compute index correlation. Because we can use all index quals equally when
    // scanning, use the largest absolute correlation among columns used by the
    // query. Start at the worst case (0); if no stats are found, keep it 0.
    let mut index_correlation = 0.0f64;
    for iclause in &indexclauses {
        let attnum = index.indexkeys[iclause.indexcol as usize];

        // Look up stats for this index column (simple var -> table stats,
        // expression column -> index stats). `examine_indexcol_variable` handles
        // both legs (the `indexkeys[col] != 0` vs `== 0` split), exactly as the C
        // `attnum != 0` branch does.
        let _ = attnum;
        let mut vardata = VariableStatData::zeroed(NodeId::default());
        examine_indexcol_variable(mcx, run, root, &index, iclause.indexcol as usize, &mut vardata)?;

        if let Some(stats_tuple) = vardata.stats_tuple {
            if let Some(sslot) = lsc::get_attstatsslot::call(
                mcx,
                stats_tuple,
                STATISTIC_KIND_CORRELATION as i32,
                InvalidOid,
                ATTSTATSSLOT_NUMBERS,
            )? {
                let var_correlation = if sslot.numbers.is_empty() {
                    0.0f64
                } else {
                    (sslot.numbers[0] as f64).abs()
                };
                if var_correlation > index_correlation {
                    index_correlation = var_correlation;
                }
            }
        }

        crate::examine::release_variable_stats(vardata);
    }

    // Estimate the fraction of main-table tuples matched by the index quals.
    let qual_selectivity = {
        let entries: Vec<ClauseListEntry> =
            index_quals.iter().map(|&r| ClauseListEntry::Rinfo(r)).collect();
        sel::clauselist_selectivity_mixed::call(
            run,
            root,
            &entries,
            baserel_relid,
            JOIN_INNER,
            None,
        )?
    };

    // The minimum possible ranges we could match if all rows were in perfect
    // order in the table's heap.
    let minimal_ranges = (index_ranges * qual_selectivity).ceil();

    // Estimate the number of ranges we'll touch using the correlation. Careful
    // not to divide by zero (we use the absolute value of the correlation).
    let estimated_ranges = if index_correlation < 1.0e-10 {
        index_ranges
    } else {
        (minimal_ranges / index_correlation).min(index_ranges)
    };

    // We expect to visit this portion of the table.
    let selec = crate::clamp_probability(estimated_ranges / index_ranges);
    let index_selectivity = selec;

    // Compute the index qual costs, much as in genericcostestimate, to add to the
    // index costs. We can disregard indexorderbys (BRIN doesn't support those).
    let qual_arg_cost = {
        let refs: Vec<ClauseRef> =
            index_quals.iter().map(|&r| ClauseRef::Rinfo(r)).collect();
        index_other_operands_eval_cost(mcx, root, &refs)?
    };

    let cpu_operator_cost = cz::cpu_operator_cost::call();

    // Startup cost: read the whole revmap sequentially, including the index quals.
    let mut index_startup_cost = spc_seq_page_cost * stats_revmap_num_pages * loop_count;
    index_startup_cost += qual_arg_cost;

    // Total cost: reading a BRIN index involves back-and-forth over regular pages
    // (revmap can point to them out of sequential order), so price the regular
    // pages as random reads.
    let mut index_total_cost = index_startup_cost
        + spc_random_page_cost * (num_pages - stats_revmap_num_pages) * loop_count;

    // Charge a small amount per range tuple we expect to match, reflecting the
    // bitmap-manipulation cost (the BRIN scan sets a bit for each page in a
    // matching range, so multiply by the pages-per-range).
    index_total_cost += 0.1 * cpu_operator_cost * estimated_ranges * stats_pages_per_range;

    Ok(AmCostEstimate {
        index_startup_cost,
        index_total_cost,
        index_selectivity,
        index_correlation,
        index_pages: num_pages,
    })
}

// ===========================================================================
// GIN cost estimation (gincostestimate + gincost_pattern / gincost_opexpr /
// gincost_scalararrayopexpr, selfuncs.c). GIN has search behaviour completely
// different from the other index AMs, so it does not go through
// genericcostestimate.
// ===========================================================================

/// `INDEX_MAX_KEYS` (pg_config_manual.h) — the per-attribute scan-flag array
/// bound. GIN indexes never have more key columns than this.
const INDEX_MAX_KEYS: usize = 32;

/// `GIN_EXTRACTQUERY_PROC` (access/gin.h) — the GIN opclass support function
/// number for `extractQuery`.
const GIN_EXTRACTQUERY_PROC: i16 = 3;

/// `DEFAULT_COLLATION_OID` (pg_collation_d.h) — the collation `extractProc` is
/// called with when the index column has no collation (matches `initGinState`).
const DEFAULT_COLLATION_OID: Oid = 100;

/// `BLCKSZ` (pg_config.h) — the page size, used in the item-pointer cross-check
/// (`numTuples / (BLCKSZ / 3)`).
const BLCKSZ: f64 = 8192.0;

/// `GinQualCounts` (selfuncs.c) — accumulated counts of the index terms that a
/// GIN query needs to search for, used to drive `gincostestimate`.
#[derive(Clone, Debug)]
struct GinQualCounts {
    /// `bool attHasFullScan[INDEX_MAX_KEYS]` — per-attribute: a full-index scan
    /// was requested (a `GIN_SEARCH_MODE_ALL` qual).
    att_has_full_scan: [bool; INDEX_MAX_KEYS],
    /// `bool attHasNormalScan[INDEX_MAX_KEYS]` — per-attribute: a normal
    /// (default / include-empty) scan was requested.
    att_has_normal_scan: [bool; INDEX_MAX_KEYS],
    /// `double partialEntries` — estimated number of partial-match entries.
    partial_entries: f64,
    /// `double exactEntries` — estimated number of exact-match entries.
    exact_entries: f64,
    /// `double searchEntries` — total estimated number of entries searched.
    search_entries: f64,
    /// `double arrayScans` — multiplicative count of ScalarArrayOp sub-scans.
    array_scans: f64,
}

impl GinQualCounts {
    /// `memset(&counts, 0, sizeof(counts))` — all-zero, including `arrayScans`
    /// (the caller sets `arrayScans = 1` separately, as the C does).
    fn zeroed() -> Self {
        GinQualCounts {
            att_has_full_scan: [false; INDEX_MAX_KEYS],
            att_has_normal_scan: [false; INDEX_MAX_KEYS],
            partial_entries: 0.0,
            exact_entries: 0.0,
            search_entries: 0.0,
            array_scans: 0.0,
        }
    }
}

/// `gincost_pattern(index, indexcol, clause_op, query, counts)` (selfuncs.c) —
/// estimate the number of index terms that need to be searched while testing
/// the given GIN query (`query` is a single key `Datum`), and increment
/// `*counts`. Returns `Ok(false)` if the query is provably unsatisfiable.
fn gincost_pattern<'mcx>(
    mcx: Mcx<'mcx>,
    index: &IndexOptInfo,
    indexcol: usize,
    clause_op: Oid,
    query: types_tuple::Datum<'mcx>,
    counts: &mut GinQualCounts,
) -> PgResult<bool> {
    debug_assert!((indexcol as i32) < index.nkeycolumns);

    // Get the operator's strategy number within the index opfamily (we don't
    // need the declared input types, but get_op_opfamily_properties throws if
    // it fails to find a matching pg_amop entry, which we want).
    let strategy_op = match lsc::get_op_opfamily_properties::call(
        clause_op,
        index.opfamily[indexcol],
        false,
        false,
    )? {
        Some((strategy, _lefttype, _righttype)) => strategy,
        // missing_ok = false elog(ERROR)s in C; the seam carries it on Err.
        None => unreachable!("get_op_opfamily_properties(missing_ok=false) returned None"),
    };

    // GIN always uses the "default" support functions, which are those with
    // lefttype == righttype == the opclass' opcintype (see
    // IndexSupportInitialize in relcache.c).
    let extract_proc_oid = lsc::get_opfamily_proc::call(
        index.opfamily[indexcol],
        index.opcintype[indexcol],
        index.opcintype[indexcol],
        GIN_EXTRACTQUERY_PROC,
    )?;

    if !OidIsValid(extract_proc_oid) {
        // should not happen; throw same error as index_getprocinfo.
        return Err(PgError::error(alloc::format!(
            "missing support function {} for attribute {} of index",
            GIN_EXTRACTQUERY_PROC,
            indexcol + 1,
        )));
    }

    // Choose collation to pass to extractProc (should match initGinState).
    let collation = if OidIsValid(index.indexcollations[indexcol]) {
        index.indexcollations[indexcol]
    } else {
        DEFAULT_COLLATION_OID
    };

    let flinfo = fmgr::fmgr_info::call(mcx, extract_proc_oid)?;

    // set_fn_opclass_options(&flinfo, index->opclassoptions[indexcol]) — the
    // IndexOptInfo value model does not carry per-column opclass options
    // (none of the built-in GIN opclasses register any), so this is a no-op
    // here; see the gincostestimate doc-comment for the deferred bit.

    let result = gin::gin_extract_query::call(
        mcx,
        &flinfo,
        collation,
        query,
        strategy_op as u16,
    )?;

    let nentries = result.query_values.len();
    let search_mode = result.search_mode;

    if nentries == 0 && search_mode == types_gin::GIN_SEARCH_MODE_DEFAULT {
        // No match is possible.
        return Ok(false);
    }

    for i in 0..nentries {
        // For partial match we haven't any information to estimate the number
        // of matched entries in the index, so we just estimate it as 100.
        if !result.partial_matches.is_empty() && result.partial_matches[i] {
            counts.partial_entries += 100.0;
        } else {
            counts.exact_entries += 1.0;
        }
        counts.search_entries += 1.0;
    }

    if search_mode == types_gin::GIN_SEARCH_MODE_DEFAULT {
        counts.att_has_normal_scan[indexcol] = true;
    } else if search_mode == types_gin::GIN_SEARCH_MODE_INCLUDE_EMPTY {
        // Treat "include empty" like an exact-match item.
        counts.att_has_normal_scan[indexcol] = true;
        counts.exact_entries += 1.0;
        counts.search_entries += 1.0;
    } else {
        // It's GIN_SEARCH_MODE_ALL.
        counts.att_has_full_scan[indexcol] = true;
    }

    Ok(true)
}

/// `gincost_opexpr(root, index, indexcol, clause, counts)` (selfuncs.c) —
/// estimate the search-term counts for a single `OpExpr` GIN index clause.
fn gincost_opexpr<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexcol: usize,
    clause: &types_nodes::primnodes::OpExpr,
    counts: &mut GinQualCounts,
) -> PgResult<bool> {
    let clause_op = clause.opno;
    // copyObject shape: deep-copy the operand via `clone_in` (it may be a SubPlan).
    let operand = clause
        .args
        .get(1)
        .map(|e| e.clone_in(mcx))
        .transpose()?
        .expect("gincost_opexpr: OpExpr must have a second argument");

    // Aggressively reduce to a constant, and look through relabeling.
    let mut operand = sel::estimate_expression_value::call(run, root, &operand)?;
    if let Expr::RelabelType(r) = &operand {
        if let Some(arg) = &r.arg {
            operand = (**arg).clone();
        }
    }

    // It's impossible to call the extractQuery method for an unknown operand.
    // So unless the operand is a Const we can't do much; just assume there will
    // be one ordinary search entry from the operand at runtime.
    let c = match &operand {
        Expr::Const(c) => c,
        _ => {
            counts.exact_entries += 1.0;
            counts.search_entries += 1.0;
            return Ok(true);
        }
    };

    // If the Const is null, there can be no matches.
    if c.constisnull {
        return Ok(false);
    }

    // Otherwise, apply extractQuery and get the actual term counts.
    gincost_pattern(mcx, index, indexcol, clause_op, c.constvalue.clone(), counts)
}

/// `gincost_scalararrayopexpr(root, index, indexcol, clause, numIndexEntries,
/// counts)` (selfuncs.c) — estimate the search-term counts for a single
/// `ScalarArrayOpExpr` GIN index clause. Each RHS array element gives rise to a
/// separate indexscan at runtime; we average the counts across the elements and
/// multiply `counts.arrayScans` by the number of satisfiable elements.
fn gincost_scalararrayopexpr<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexcol: usize,
    clause: &types_nodes::primnodes::ScalarArrayOpExpr,
    num_index_entries: f64,
    counts: &mut GinQualCounts,
) -> PgResult<bool> {
    let clause_op = clause.opno;
    debug_assert!(clause.useOr);

    // copyObject shape: deep-copy the operand via `clone_in` (it may be a SubPlan).
    let rightop = clause
        .args
        .get(1)
        .map(|e| e.clone_in(mcx))
        .transpose()?
        .expect("gincost_scalararrayopexpr: SAOP must have a second argument");

    // Aggressively reduce to a constant, and look through relabeling. The fold
    // seam yields the planner-arena `'static` form; bring it into `mcx`.
    let mut rightop: Expr<'mcx> =
        sel::estimate_expression_value::call(run, root, &rightop)?.clone_in(mcx)?;
    if let Expr::RelabelType(r) = &rightop {
        if let Some(arg) = &r.arg {
            rightop = (**arg).clone_in(mcx)?;
        }
    }

    // It's impossible to call the extractQuery method for an unknown operand.
    // So unless the operand is a Const we can't do much; just assume there will
    // be one ordinary search entry from each array entry at runtime, and fall
    // back on a probably-bad estimate of the number of array entries.
    let c = match &rightop {
        Expr::Const(c) => c,
        _ => {
            let node_id = root.alloc_node(rightop.clone_in(mcx)?);
            counts.exact_entries += 1.0;
            counts.search_entries += 1.0;
            counts.array_scans *= cz::estimate_array_length::call(root, node_id);
            return Ok(true);
        }
    };

    // If the Const is null, there can be no matches.
    if c.constisnull {
        return Ok(false);
    }

    // Otherwise, extract the array elements and iterate over them.
    let arraydatum = c.constvalue.clone();
    // ARR_ELEMTYPE(arrayval): the element type of the RHS array constant.
    let elem_type = lsc::get_base_element_type::call(c.consttype)?;
    let s = lsc::get_typlenbyvalalign::call(elem_type)?;
    let elems = arr::deconstruct_array_v::call(
        mcx,
        arraydatum,
        elem_type,
        s.typlen,
        s.typbyval,
        s.typalign as core::ffi::c_char,
    )?;

    let mut arraycounts = GinQualCounts::zeroed();
    let mut num_possible: i32 = 0;

    for (elem_value, elem_isnull) in elems.iter() {
        // NULL can't match anything, so ignore, as the executor will.
        if *elem_isnull {
            continue;
        }

        // Otherwise, apply extractQuery and get the actual term counts.
        let mut elemcounts = GinQualCounts::zeroed();

        if gincost_pattern(
            mcx,
            index,
            indexcol,
            clause_op,
            elem_value.clone(),
            &mut elemcounts,
        )? {
            // We ignore array elements that are unsatisfiable patterns.
            num_possible += 1;

            if elemcounts.att_has_full_scan[indexcol]
                && !elemcounts.att_has_normal_scan[indexcol]
            {
                // Full index scan will be required. We treat this as if every
                // key in the index had been listed in the query.
                elemcounts.partial_entries = 0.0;
                elemcounts.exact_entries = num_index_entries;
                elemcounts.search_entries = num_index_entries;
            }
            arraycounts.partial_entries += elemcounts.partial_entries;
            arraycounts.exact_entries += elemcounts.exact_entries;
            arraycounts.search_entries += elemcounts.search_entries;
        }
    }

    if num_possible == 0 {
        // No satisfiable patterns in the array.
        return Ok(false);
    }

    // Now add the averages to the global counts. This gives an estimate of the
    // average number of terms searched for in each indexscan, including both
    // array and non-array qual contributions.
    let np = num_possible as f64;
    counts.partial_entries += arraycounts.partial_entries / np;
    counts.exact_entries += arraycounts.exact_entries / np;
    counts.search_entries += arraycounts.search_entries / np;

    counts.array_scans *= np;

    Ok(true)
}

/// `gincostestimate(root, path, loop_count, ...)` (selfuncs.c) — the GIN index
/// AM's cost estimator. GIN has search behaviour completely different from the
/// other index types, so it does not use `genericcostestimate`: it reads the
/// metapage stats (`ginGetStats`), runs the opclass `extractQuery` for each
/// index qual to count search keys, then prices the pending-list scan, the
/// entry-tree descent, and the data-page reads. 1:1 with the C body.
///
/// Deferred bit: the C `set_fn_opclass_options(&flinfo, opclassoptions[col])`
/// step is omitted because the `IndexOptInfo` value model does not carry
/// per-column opclass options; none of the built-in GIN opclasses register any,
/// so this does not affect the cost for the standard opclasses (array_ops,
/// tsvector_ops, jsonb_ops, jsonb_path_ops). The faithful `extractQuery` term
/// counting (exact/partial/searchEntries, ScalarArrayOp averaging) is ported.
pub(crate) fn gincostestimate<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index, indexclauses): (IndexOptInfo, Vec<types_pathnodes::IndexClause>) = {
        let path = expect_index_path(root, path_id);
        let index = (**path
            .indexinfo
            .as_ref()
            .expect("gincostestimate: indexinfo must be set"))
        .clone();
        (index, path.indexclauses.clone())
    };

    let index_quals: Vec<RinfoId> = {
        let path = expect_index_path(root, path_id);
        get_quals_from_indexclauses(path)
    };

    let mut num_pages = index.pages as f64;
    let num_tuples = index.tuples;

    // Obtain statistical information from the meta page, if possible. Else set
    // ginStats to zeroes, and we'll cope below.
    let gin_stats: types_gin::GinStatsData = if !index.hypothetical {
        // Lock should have already been obtained in plancat.c.
        let index_rel = indexam::index_open::call(
            mcx,
            index.indexoid,
            types_storage::lock::NoLock,
        )?;
        let stats = gin::gin_get_stats::call(&index_rel)?;
        index_rel.close(types_storage::lock::NoLock)?;
        types_gin::GinStatsData {
            nPendingPages: stats.nPendingPages,
            nTotalPages: stats.nTotalPages,
            nEntryPages: stats.nEntryPages,
            nDataPages: stats.nDataPages,
            nEntries: stats.nEntries,
            ginVersion: stats.ginVersion,
        }
    } else {
        types_gin::GinStatsData::default()
    };

    // Assuming we got valid (nonzero) stats at all, nPendingPages can be
    // trusted, but the other fields are data as of the last VACUUM. We can
    // scale them up to account for growth since then, but only to 4X; beyond
    // that, fall back to estimating from the assumed-accurate index size.
    let num_pending_pages = if (gin_stats.nPendingPages as f64) < num_pages {
        gin_stats.nPendingPages as f64
    } else {
        0.0
    };

    let num_entry_pages;
    let num_data_pages;
    let mut num_entries;

    if num_pages > 0.0
        && (gin_stats.nTotalPages as f64) <= num_pages
        && (gin_stats.nTotalPages as f64) > num_pages / 4.0
        && gin_stats.nEntryPages > 0
        && gin_stats.nEntries > 0
    {
        // The stats seem close enough to sane to be trusted. Scale them by
        // numPages / nTotalPages to account for growth since the last VACUUM.
        let scale = num_pages / gin_stats.nTotalPages as f64;

        let mut nep = (gin_stats.nEntryPages as f64 * scale).ceil();
        let mut ndp = (gin_stats.nDataPages as f64 * scale).ceil();
        num_entries = (gin_stats.nEntries as f64 * scale).ceil();
        // Ensure we didn't round up too much.
        nep = nep.min(num_pages - num_pending_pages);
        ndp = ndp.min(num_pages - num_pending_pages - nep);
        num_entry_pages = nep;
        num_data_pages = ndp;
    } else {
        // Hypothetical index, never-vacuumed pre-9.1 index (zero stats), or
        // grown too much since the last VACUUM. Invent plausible internal
        // statistics from the index page count (clamped to >= 10 pages):
        // estimate 90% entry pages, the rest data pages, 100 entries/page.
        num_pages = num_pages.max(10.0);
        num_entry_pages = ((num_pages - num_pending_pages) * 0.90).floor();
        num_data_pages = num_pages - num_pending_pages - num_entry_pages;
        num_entries = (num_entry_pages * 100.0).floor();
    }

    // In an empty index, numEntries could be zero. Avoid divide-by-zero.
    if num_entries < 1.0 {
        num_entries = 1.0;
    }

    // If the index is partial, AND the index predicate with the index-bound
    // quals to produce a more accurate idea of the rows covered.
    let selectivity_quals = add_predicate_to_index_quals(mcx, root, &index, &index_quals)?;

    let baserel = index.rel.expect("gincostestimate: index.rel must be set");
    let baserel_relid = root.rel(baserel).relid as i32;

    // Estimate the fraction of main-table tuples that will be visited.
    let index_selectivity = sel::clauselist_selectivity_mixed::call(
        run,
        root,
        &selectivity_quals,
        baserel_relid,
        JOIN_INNER,
        None,
    )?;

    // Fetch estimated page cost for the tablespace containing the index.
    let spc = cz::get_tablespace_page_costs::call(index.reltablespace);
    let spc_random_page_cost = spc.spc_random_page_cost;

    // Generic assumption about index correlation: there isn't any.
    let index_correlation = 0.0;

    // Examine quals to estimate the number of search entries & partial matches.
    let mut counts = GinQualCounts::zeroed();
    counts.array_scans = 1.0;
    let mut match_possible = true;

    'outer: for iclause in &indexclauses {
        let indexcol = iclause.indexcol as usize;
        for &rid in &iclause.indexquals {
            let clause: Expr = {
                // copyObject shape: deep-copy via `clone_in` (an OpExpr/SAOP
                // operand may carry a correlated SubPlan whose derived
                // `Expr::clone` panics).
                let clause_node = root.rinfo(rid).clause;
                root.node(clause_node).clone_in(mcx)?
            };

            match &clause {
                Expr::OpExpr(op) => {
                    match_possible =
                        gincost_opexpr(mcx, run, root, &index, indexcol, op, &mut counts)?;
                    if !match_possible {
                        break 'outer;
                    }
                }
                Expr::ScalarArrayOpExpr(saop) => {
                    match_possible = gincost_scalararrayopexpr(
                        mcx,
                        run,
                        root,
                        &index,
                        indexcol,
                        saop,
                        num_entries,
                        &mut counts,
                    )?;
                    if !match_possible {
                        break 'outer;
                    }
                }
                other => {
                    // shouldn't be anything else for a GIN index.
                    return Err(PgError::error(alloc::format!(
                        "unsupported GIN indexqual type: {:?}",
                        core::mem::discriminant(other)
                    )));
                }
            }
        }
    }

    // Fall out if there were any provably-unsatisfiable quals.
    if !match_possible {
        return Ok(AmCostEstimate {
            index_startup_cost: 0.0,
            index_total_cost: 0.0,
            index_selectivity: 0.0,
            index_correlation,
            index_pages: num_pages,
        });
    }

    // If an attribute has a full scan but at the same time doesn't have a normal
    // scan, we'll have to scan all non-null entries of that attribute. We don't
    // have per-attribute statistics for GIN, so assume the whole index must be
    // scanned.
    let mut full_index_scan = false;
    for i in 0..index.nkeycolumns as usize {
        if counts.att_has_full_scan[i] && !counts.att_has_normal_scan[i] {
            full_index_scan = true;
            break;
        }
    }

    if full_index_scan || index_quals.is_empty() {
        // Full index scan will be required. Treat this as if every key in the
        // index had been listed in the query.
        counts.partial_entries = 0.0;
        counts.exact_entries = num_entries;
        counts.search_entries = num_entries;
    }

    // Will we have more than one iteration of a nestloop scan?
    let outer_scans = loop_count;

    let cpu_operator_cost = cz::cpu_operator_cost::call();
    let cpu_index_tuple_cost = cz::cpu_index_tuple_cost::call();

    // Compute cost to begin scan; first of all, pay attention to pending list.
    let mut entry_pages_fetched = num_pending_pages;

    // Estimate the number of entry pages read. We need to do
    // counts.searchEntries searches. Use a power function; tuples on leaf pages
    // is usually much greater. Includes all searches in the entry tree,
    // including the first entry in the partial-match algorithm.
    entry_pages_fetched += (counts.search_entries * num_entry_pages.powf(0.15).round()).ceil();

    // Add an estimate of the entry pages read by the partial-match algorithm
    // (a scan over leaf pages in the entry tree). counts.partialEntries is
    // pretty bogus, so it might exceed numEntries; clamp the proportion.
    let mut partial_scale = counts.partial_entries / num_entries;
    partial_scale = partial_scale.min(1.0);

    entry_pages_fetched += (num_entry_pages * partial_scale).ceil();

    // The partial-match algorithm reads all data pages before the actual scan,
    // so it's a startup cost. Again, no useful stats, so estimate as proportion.
    let mut data_pages_fetched = (num_data_pages * partial_scale).ceil();

    let mut index_startup_cost = 0.0f64;
    let mut index_total_cost = 0.0f64;

    // Add a CPU-cost component for the initial entry-btree descent. We don't
    // charge I/O for upper btree levels (they stay in cache), but we still do
    // about log2(N) comparisons; charge one cpu_operator_cost per comparison.
    // With ScalarArrayOpExprs, charge this once per SA scan; the ones after the
    // first are not startup cost for the overall plan, so add only to total.
    if num_entries > 1.0 {
        // avoid computing log(0)
        let descent_cost =
            (num_entries.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        index_startup_cost += descent_cost * counts.search_entries;
        index_total_cost += counts.array_scans * descent_cost * counts.search_entries;
    }

    // Add a cpu cost per entry-page fetched. Not amortized over a loop.
    index_startup_cost +=
        entry_pages_fetched * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    index_total_cost += entry_pages_fetched
        * counts.array_scans
        * DEFAULT_PAGE_CPU_MULTIPLIER
        * cpu_operator_cost;

    // Add a cpu cost per data-page fetched (partial-match data pages), as a
    // startup cost. Also not amortized over a loop.
    index_startup_cost +=
        DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * data_pages_fetched;

    // Since we add the startup cost to the total cost later on, remove the
    // initial arrayscan from the total.
    index_total_cost += data_pages_fetched
        * (counts.array_scans - 1.0)
        * DEFAULT_PAGE_CPU_MULTIPLIER
        * cpu_operator_cost;

    // Cache effects if more than one scan due to nestloops or array quals.
    // Pro-rated per nestloop scan, but the array qual factor isn't pro-rated.
    if outer_scans > 1.0 || counts.array_scans > 1.0 {
        entry_pages_fetched *= outer_scans * counts.array_scans;
        entry_pages_fetched = cz::index_pages_fetched::call(
            entry_pages_fetched,
            num_entry_pages as u32,
            num_entry_pages,
            root,
        );
        entry_pages_fetched /= outer_scans;
        data_pages_fetched *= outer_scans * counts.array_scans;
        data_pages_fetched = cz::index_pages_fetched::call(
            data_pages_fetched,
            num_data_pages as u32,
            num_data_pages,
            root,
        );
        data_pages_fetched /= outer_scans;
    }

    // Use random page cost because logically-close pages could be far apart on
    // disk.
    index_startup_cost += (entry_pages_fetched + data_pages_fetched) * spc_random_page_cost;

    // Compute the number of data pages fetched during the scan. Assume every
    // entry has the same number of items with no overlap.
    let mut data_pages_fetched =
        (num_data_pages * counts.exact_entries / num_entries).ceil();

    // If there's a lot of overlap among the entries (one very frequent entry),
    // the above can grossly under-estimate. Cross-check against the overall
    // selectivity: at a minimum, read one item pointer per matching entry.
    // Average ~3 bytes per item pointer.
    let data_pages_fetched_by_sel = (index_selectivity * (num_tuples / (BLCKSZ / 3.0))).ceil();
    if data_pages_fetched_by_sel > data_pages_fetched {
        data_pages_fetched = data_pages_fetched_by_sel;
    }

    // Add one page cpu-cost to the startup cost.
    index_startup_cost +=
        DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * counts.search_entries;

    // Add once again a CPU-cost for those data pages, before amortizing for
    // cache.
    index_total_cost +=
        data_pages_fetched * counts.array_scans * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;

    // Account for cache effects, the same as above.
    if outer_scans > 1.0 || counts.array_scans > 1.0 {
        data_pages_fetched *= outer_scans * counts.array_scans;
        data_pages_fetched = cz::index_pages_fetched::call(
            data_pages_fetched,
            num_data_pages as u32,
            num_data_pages,
            root,
        );
        data_pages_fetched /= outer_scans;
    }

    // Apply random_page_cost as the cost per page.
    index_total_cost += index_startup_cost + data_pages_fetched * spc_random_page_cost;

    // Add on index qual eval costs, much as in genericcostestimate. We charge
    // cpu but can disregard indexorderbys (GIN doesn't support those).
    let qual_arg_cost = {
        let refs: Vec<ClauseRef> =
            index_quals.iter().map(|&r| ClauseRef::Rinfo(r)).collect();
        index_other_operands_eval_cost(mcx, root, &refs)?
    };
    let qual_op_cost = cpu_operator_cost * index_quals.len() as f64;

    index_startup_cost += qual_arg_cost;
    index_total_cost += qual_arg_cost;

    // Add a cpu cost per search entry, corresponding to the actual visited
    // entries.
    index_total_cost += (counts.search_entries * counts.array_scans) * qual_op_cost;
    // Now add a cpu cost per tuple in the posting lists / trees.
    index_total_cost += (num_tuples * index_selectivity) * cpu_index_tuple_cost;

    Ok(AmCostEstimate {
        index_startup_cost,
        index_total_cost,
        index_selectivity,
        index_correlation,
        index_pages: data_pages_fetched,
    })
}

/// `&mut`-borrow the `IndexPath` for a `PathId`, panicking if it is not one.
fn expect_index_path(root: &PlannerInfo, path_id: PathId) -> &types_pathnodes::IndexPath {
    match root.path(path_id) {
        PathNode::IndexPath(ip) => ip,
        _ => panic!("expect_index_path: path is not an IndexPath"),
    }
}

/// `&mut`-borrow the `IndexPath` for a `PathId`, panicking if it is not one.
fn expect_index_path_mut(root: &mut PlannerInfo, path_id: PathId) -> &mut types_pathnodes::IndexPath {
    match root.path_mut(path_id) {
        PathNode::IndexPath(ip) => ip,
        _ => panic!("expect_index_path_mut: path is not an IndexPath"),
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
    // In C, `info->amcostestimate` is the `amcostestimate` function pointer
    // copied from the AM's `IndexAmRoutine` (plancat.c, resolved through the
    // AM's `amhandler`). Dispatch must therefore key on the AM's *handler*
    // function, not on the AM's OID: a user-created access method (e.g. the
    // `create_am` test's `gist2`, defined `HANDLER gisthandler`) has its own
    // `pg_am.oid` but shares a built-in handler, and must reach that handler's
    // cost estimator. Resolve `relam -> amhandler` (SearchSysCache AMOID,
    // amform->amhandler) and dispatch on the well-known built-in handler OID,
    // mirroring `GetIndexAmRoutine`'s handler-OID dispatch in amapi.c.
    let amhandler = backend_utils_cache_syscache_seams::search_am_handler::call(relam)
        .expect("amcostestimate: search_am_handler")
        .unwrap_or_else(|| {
            panic!("amcostestimate: cache lookup failed for access method {relam}")
        });
    // F_*HANDLER (pg_proc.dat): bthandler=330, hashhandler=331, gisthandler=332,
    // ginhandler=333, spghandler=334, brinhandler=335.
    match amhandler {
        330 => btcostestimate(mcx, run, root, path_id, loop_count)
            .expect("btcostestimate"),
        331 => hashcostestimate(mcx, run, root, path_id, loop_count)
            .expect("hashcostestimate"),
        332 => gistcostestimate(mcx, run, root, path_id, loop_count)
            .expect("gistcostestimate"),
        334 => spgcostestimate(mcx, run, root, path_id, loop_count)
            .expect("spgcostestimate"),
        335 => brincostestimate(mcx, run, root, path_id, loop_count)
            .expect("brincostestimate"),
        333 => gincostestimate(mcx, run, root, path_id, loop_count)
            .expect("gincostestimate"),
        other => panic!(
            "amcostestimate: no cost estimator ported for index AM handler oid \
             {} (am oid {})",
            other, relam
        ),
    }
}
