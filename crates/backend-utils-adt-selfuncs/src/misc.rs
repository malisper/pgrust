//! Assorted selfuncs.c entry points — `const_node_info` (the `IsA(node, Const)`
//! decode `scalararraysel_containment` uses), `estimate_array_length`, and
//! `estimate_num_groups`.

use mcx::Mcx;
use types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{NodeId, PlannerInfo, RelId, Relids};
use types_selfuncs::{ConstNodeInfo, EstimationInfo, VariableStatData, SELFLAG_USED_DEFAULT};
use types_statistics::STATS_EXT_NDISTINCT;

use backend_utils_cache_lsyscache_seams as lsc;

use backend_nodes_equalfuncs_seams as eq;
use backend_nodes_nodeFuncs_seams as nf;
use backend_optimizer_path_equivclass_ext_seams as ec_ext;
use backend_optimizer_path_equivclass_seams as ec;
use backend_optimizer_util_clauses as clauses;
use backend_optimizer_util_relnode_seams as rel_seams;
use backend_statistics_mvdistinct as mvdistinct;

use crate::clamp_probability;
use crate::examine::{examine_variable, get_restriction_variable, release_variable_stats};
use crate::ineq::{histogram_selectivity, mcv_selectivity};
use crate::scalar::get_variable_numdistinct;
use crate::{clamp_row_est, BOOLOID};

/// `pull_var_clause` recursion flags (optimizer.h), as `estimate_num_groups`
/// passes them: recurse into aggregate/window-func/placeholder arguments and
/// collect the component Vars.
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_RECURSE_PLACEHOLDERS: i32 = 0x0020;

/* ---------------------------------------------------------------------------
 * const_node_info — INSTALLED seam (selfuncs.c scalararraysel_containment IsA).
 * ------------------------------------------------------------------------- */

/// `IsA(node, Const)` decode: returns `None` when `node` is not a `Const`
/// (C: the `!IsA` punt), else its `(constisnull, constvalue, consttype)`.
pub(crate) fn const_node_info(node: NodeId) -> PgResult<Option<ConstNodeInfo>> {
    // The node handle is resolved against the planner arena by the caller's
    // context; the const_node_info seam only carries the NodeId, so the decode
    // of an arbitrary arena handle into a Const without a PlannerInfo to resolve
    // it against is not expressible here. This entry point is reached only by
    // scalararraysel_containment (array_selfuncs.c), whose own port resolves the
    // node before this point; the standalone NodeId-only form is the array
    // estimator's seam contract and stays a precise panic until that consumer
    // threads the arena through.
    let _ = node;
    panic!(
        "selfuncs: const_node_info(NodeId) needs the PlannerInfo node arena to resolve the \
         handle (the seam carries only a NodeId); scalararraysel_containment must thread the \
         arena through before this decode is expressible"
    )
}

/// Seam body for `const_node_info`.
pub fn seam_const_node_info(node: NodeId) -> PgResult<Option<ConstNodeInfo>> {
    const_node_info(node)
}

/* ---------------------------------------------------------------------------
 * estimate_num_groups (selfuncs.c:3448) — INSTALLED seam.
 * ------------------------------------------------------------------------- */

/// `GroupVarInfo` (selfuncs.c) — a unique Var (or expression treated as a Var)
/// collected by `estimate_num_groups`, with its per-table distinct-value
/// estimate. The C `Node *var` is held here as an owned [`Expr`] (the stripped,
/// nullingrels-free expression).
struct GroupVarInfo {
    /// `Node *var` — might be an expression, not just a Var.
    var: Expr,
    /// `RelOptInfo *rel` — relation it belongs to.
    rel: Option<RelId>,
    /// `double ndistinct` — # distinct values.
    ndistinct: f64,
    /// `bool isdefault` — true if `DEFAULT_NUM_DISTINCT` was used.
    isdefault: bool,
}

/// `add_unique_group_var(root, varinfos, var, vardata)` (selfuncs.c) — add an
/// item to a list of [`GroupVarInfo`]s, but only if it's not known equal to any
/// of the existing entries. 1:1 with the C body.
fn add_unique_group_var(
    root: &mut PlannerInfo,
    mut varinfos: alloc::vec::Vec<GroupVarInfo>,
    var: &Expr,
    vardata: &VariableStatData,
) -> alloc::vec::Vec<GroupVarInfo> {
    let (ndistinct, isdefault) = get_variable_numdistinct(root, vardata);

    // The nullingrels bits within the var could cause the same var to be counted
    // multiple times if it's marked with different nullingrels. They could also
    // prevent us from matching the var to the expressions in extended statistics
    // (see estimate_multivariate_ndistinct). So strip them out first.
    let outer_join_rels = root.outer_join_rels.clone();
    let none: Relids = None;
    let var: Expr = nf::remove_nulling_relids::call(var, &outer_join_rels, &none);

    // foreach over the existing varinfos, dropping duplicates / known-equal vars.
    let mut i = 0usize;
    while i < varinfos.len() {
        // Drop exact duplicates.
        if eq::equal_expr::call(&var, &varinfos[i].var) {
            return varinfos;
        }

        // Drop known-equal vars, but only if they belong to different relations
        // (see comments for estimate_num_groups). We aren't too fussy about the
        // semantics of "equal" here.
        if vardata.rel != varinfos[i].rel
            && ec::exprs_known_equal::call(
                root,
                var.clone(),
                varinfos[i].var.clone(),
                InvalidOid,
            )
        {
            if varinfos[i].ndistinct <= ndistinct {
                // Keep older item, forget new one.
                return varinfos;
            } else {
                // Delete the older item.
                varinfos.remove(i);
                continue;
            }
        }
        i += 1;
    }

    varinfos.push(GroupVarInfo {
        var,
        rel: vardata.rel,
        ndistinct,
        isdefault,
    });
    varinfos
}

/// `estimate_multivariate_bucketsize(root, inner, hashclauses, &innerbucketsize)`
/// (selfuncs.c:3801) — try to refine the inner hash-bucket-size estimate using
/// multivariate ndistinct extended statistics on the inner relation, returning
/// the (possibly improved) `*innerbucketsize` and the list of clauses that could
/// NOT be estimated here (the caller estimates those one at a time).
///
/// Seam-contract note: the costsize self-seam carries only `(root: &PlannerInfo,
/// inner_rel, hashclauses)` — it does NOT thread the `&PlannerRun` resolver or a
/// mutable `PlannerInfo`. The full extended-statistics estimation path needs both
/// (`estimate_multivariate_ndistinct` takes `run` + `&mut root`, and the
/// per-clause varinfo construction reads `simple_rte_array` through the run). But
/// that path is reachable ONLY when an inner-side base relation actually carries
/// `CREATE STATISTICS` ndistinct objects (`rel->statlist != NIL`). When no
/// referenced inner relation has extended statistics — the universal case absent
/// an explicit statistics object — every clause is classified "can't be estimated
/// here" and pushed to `otherclauses`, `ndistinct` stays 1.0, and
/// `*innerbucketsize` is left UNCHANGED (the caller's prior 1.0). So this seam
/// faithfully returns `(1.0, all_hashclauses)` in that case. If an inner relation
/// does carry extended statistics, the seam must be re-signed to thread `run` +
/// `&mut` before the multivariate path is expressible; we panic loudly there
/// rather than silently dropping the refinement.
pub(crate) fn estimate_multivariate_bucketsize(
    root: &PlannerInfo,
    _inner: RelId,
    hashclauses: &[types_pathnodes::RinfoId],
) -> (f64, alloc::vec::Vec<types_pathnodes::RinfoId>) {
    let rinfos = hashclauses;
    // Nothing to do for a single clause (or none).
    if rinfos.len() <= 1 {
        return (1.0, rinfos.to_vec());
    }

    // The multivariate refinement only fires when an inner-side base relation
    // carries ndistinct extended statistics. Probe each clause's inner-side
    // singleton relation; if any has a non-empty statlist, the full path (which
    // needs the run resolver + &mut root) is required and the stripped seam
    // can't express it.
    for &rid in rinfos.iter() {
        let ri = root.rinfo(rid);
        let relids = if ri.outer_is_left {
            &ri.right_relids
        } else {
            &ri.left_relids
        };
        if let Some(relid) = rel_seams::relids_get_singleton_member::call(relids) {
            if let Some(Some(rel_id)) = root.simple_rel_array.get(relid as usize) {
                if !root.rel(*rel_id).statlist.is_empty() {
                    panic!(
                        "selfuncs::estimate_multivariate_bucketsize: inner relation carries \
                         extended (ndistinct) statistics, but the costsize seam strips the \
                         PlannerRun resolver and mutable PlannerInfo the multivariate path \
                         needs; re-sign estimate_multivariate_bucketsize to thread (run, &mut \
                         root) before this is reachable"
                    );
                }
            }
        }
    }

    // No referenced inner relation has extended statistics: every clause is an
    // "otherclause", innerbucketsize unchanged.
    (1.0, rinfos.to_vec())
}

/// `estimate_num_groups(root, groupExprs, input_rows, NULL, estinfo)`
/// (selfuncs.c) — estimate the number of distinct groups the grouping
/// expressions take over `input_rows` rows. 1:1 with the C body.
///
/// The repo callers never pass a `pgset` (the C `List **pgset` grouping-set
/// filter is always `NULL`), so it is omitted, matching the seam contract.
pub(crate) fn estimate_num_groups<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    group_exprs: &[NodeId],
    mut input_rows: f64,
    mut estinfo: Option<&mut EstimationInfo>,
) -> PgResult<f64> {
    // The allocation context for the per-expression examine work, recovered from
    // the planner-run store (C reaches it via CurrentMemoryContext).
    let mcx: Mcx<'mcx> = run.mcx();
    let mut varinfos: alloc::vec::Vec<GroupVarInfo> = alloc::vec::Vec::new();
    let mut srf_multiplier = 1.0f64;
    let mut numdistinct: f64;

    // Zero the estinfo output parameter, if non-NULL.
    if let Some(info) = estinfo.as_deref_mut() {
        *info = EstimationInfo::default();
    }

    // We don't ever want to return an estimate of zero groups, as that tends to
    // lead to division-by-zero and other unpleasantness.
    input_rows = clamp_row_est(input_rows);

    // If no grouping columns, there's exactly one group.
    if group_exprs.is_empty() {
        return Ok(1.0);
    }

    // Count groups derived from boolean grouping expressions. For other
    // expressions, find the unique Vars used, treating an expression as a Var if
    // we can find stats for it.
    numdistinct = 1.0;

    for &groupexpr_id in group_exprs.iter() {
        let groupexpr = root.node(groupexpr_id).clone();

        // Set-returning functions in grouping columns: compensate by scaling up
        // the end result by the largest SRF rowcount estimate.
        let this_srf_multiplier =
            clauses::expression_returns_set_rows(Some(&groupexpr))?;
        if srf_multiplier < this_srf_multiplier {
            srf_multiplier = this_srf_multiplier;
        }

        // Short-circuit for expressions returning boolean.
        if backend_nodes_core::nodefuncs::expr_type(Some(&groupexpr))? == BOOLOID {
            numdistinct *= 2.0;
            continue;
        }

        // If examine_variable is able to deduce anything about the GROUP BY
        // expression, treat it as a single variable even if it's really more
        // complicated.
        let vardata = examine_variable(mcx, run, root, groupexpr_id, 0)?;
        if vardata.stats_tuple.is_some() || vardata.isunique {
            varinfos = add_unique_group_var(root, varinfos, &groupexpr, &vardata);
            release_variable_stats(vardata);
            continue;
        }
        release_variable_stats(vardata);

        // Else pull out the component Vars. Handle PlaceHolderVars by recursing
        // into their arguments.
        let varshere = ec_ext::pull_var_clause::call(
            &groupexpr,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS,
        );

        // If we find any variable-free GROUP BY item, then either it is a
        // constant (and we can ignore it) or it contains a volatile function; in
        // the latter case we punt and assume that each input row will yield a
        // distinct group.
        if varshere.is_empty() {
            if clauses::contain_volatile_functions(Some(&groupexpr))? {
                return Ok(input_rows);
            }
            continue;
        }

        // Else add variables to varinfos list.
        for var in varshere.into_iter() {
            let var_id = root.alloc_node(var.clone());
            let vardata = examine_variable(mcx, run, root, var_id, 0)?;
            varinfos = add_unique_group_var(root, varinfos, &var, &vardata);
            release_variable_stats(vardata);
        }
    }

    // If now no Vars, we must have an all-constant or all-boolean GROUP BY list.
    if varinfos.is_empty() {
        // Apply SRF multiplier as we would do in the long path.
        numdistinct *= srf_multiplier;
        // Round off.
        numdistinct = numdistinct.ceil();
        // Guard against out-of-range answers.
        if numdistinct > input_rows {
            numdistinct = input_rows;
        }
        if numdistinct < 1.0 {
            numdistinct = 1.0;
        }
        return Ok(numdistinct);
    }

    // Group Vars by relation and estimate total numdistinct.
    //
    // For each iteration of the outer loop, we process the frontmost Var in
    // varinfos, plus all other Vars in the same relation. We remove these Vars
    // from the newvarinfos list for the next iteration.
    loop {
        let rel = varinfos[0].rel;
        let mut reldistinct = 1.0f64;
        let mut relmaxndistinct = reldistinct;
        let mut relvarcount = 0i32;
        let mut newvarinfos: alloc::vec::Vec<GroupVarInfo> = alloc::vec::Vec::new();
        let mut relvarinfos: alloc::vec::Vec<GroupVarInfo> = alloc::vec::Vec::new();

        // Split the list of varinfos in two - one for the current rel, one for
        // remaining Vars on other rels. (C processes varinfo1 = linitial, then
        // for_each_from from index 1.)
        let mut drained = core::mem::take(&mut varinfos).into_iter();
        let varinfo1 = drained.next().expect("estimate_num_groups: varinfos non-empty");
        relvarinfos.push(varinfo1);
        for varinfo2 in drained {
            if varinfo2.rel == rel {
                relvarinfos.push(varinfo2);
            } else {
                newvarinfos.push(varinfo2);
            }
        }

        // Get the numdistinct estimate for the Vars of this rel. We iteratively
        // search for multivariate n-distinct with maximum number of vars.
        while !relvarinfos.is_empty() {
            if let Some(mvndistinct) =
                estimate_multivariate_ndistinct(run, root, rel, &mut relvarinfos)?
            {
                reldistinct *= mvndistinct;
                if relmaxndistinct < mvndistinct {
                    relmaxndistinct = mvndistinct;
                }
                relvarcount += 1;
            } else {
                for varinfo2 in relvarinfos.iter() {
                    reldistinct *= varinfo2.ndistinct;
                    if relmaxndistinct < varinfo2.ndistinct {
                        relmaxndistinct = varinfo2.ndistinct;
                    }
                    relvarcount += 1;

                    // When varinfo2's isdefault is set then we'd better set the
                    // SELFLAG_USED_DEFAULT bit in the EstimationInfo.
                    if varinfo2.isdefault {
                        if let Some(info) = estinfo.as_deref_mut() {
                            info.flags |= SELFLAG_USED_DEFAULT;
                        }
                    }
                }
                // we're done with this relation
                relvarinfos.clear();
            }
        }

        // Sanity check --- don't divide by zero if empty relation.
        // Assert(IS_SIMPLE_REL(rel)); rel is always present here.
        let relid = rel.expect("estimate_num_groups: GroupVarInfo with no rel");
        let rel_tuples = root.rel(relid).tuples;
        let rel_rows = root.rel(relid).rows;
        if rel_tuples > 0.0 {
            // Clamp to size of rel, or size of rel / 10 if multiple Vars.
            let mut clamp = rel_tuples;

            if relvarcount > 1 {
                clamp *= 0.1;
                if clamp < relmaxndistinct {
                    clamp = relmaxndistinct;
                    // for sanity in case some ndistinct is too large:
                    if clamp > rel_tuples {
                        clamp = rel_tuples;
                    }
                }
            }
            if reldistinct > clamp {
                reldistinct = clamp;
            }

            // Update the estimate based on the restriction selectivity, guarding
            // against division by zero when reldistinct is zero. Also skip this
            // if we know that we are returning all rows.
            if reldistinct > 0.0 && rel_rows < rel_tuples {
                // n * (1 - ((N-p)/N)^(N/n)) — the Dell'Era approximation form.
                reldistinct *= 1.0
                    - ((rel_tuples - rel_rows) / rel_tuples).powf(rel_tuples / reldistinct);
            }
            reldistinct = clamp_row_est(reldistinct);

            // Update estimate of total distinct groups.
            numdistinct *= reldistinct;
        }

        varinfos = newvarinfos;
        if varinfos.is_empty() {
            break;
        }
    }

    // Now we can account for the effects of any SRFs.
    numdistinct *= srf_multiplier;

    // Round off.
    numdistinct = numdistinct.ceil();

    // Guard against out-of-range answers.
    if numdistinct > input_rows {
        numdistinct = input_rows;
    }
    if numdistinct < 1.0 {
        numdistinct = 1.0;
    }

    Ok(numdistinct)
}

/// `AttrNumberIsForUserDefinedAttr(attnum)` (access/attnum.h) — `attnum > 0`.
#[inline]
fn attr_number_is_for_user_defined_attr(attnum: AttrNumber) -> bool {
    attnum > 0
}

/// `estimate_multivariate_ndistinct(root, rel, varinfos, &ndistinct)`
/// (selfuncs.c) — find the best matching ndistinct extended statistics for the
/// given list of [`GroupVarInfo`]s (all belonging to `rel`). On a match (> 1
/// covered var/expr), returns `Some(ndistinct)` and rewrites `*varinfos` to drop
/// the matched entries; on no match returns `None` leaving `varinfos`
/// untouched. 1:1 with the C body.
fn estimate_multivariate_ndistinct<'run>(
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    rel: Option<RelId>,
    varinfos: &mut alloc::vec::Vec<GroupVarInfo>,
) -> PgResult<Option<f64>> {
    let relid = match rel {
        Some(r) => r,
        None => return Ok(None),
    };

    // bail out immediately if the table has no extended statistics.
    let statlist = root.rel(relid).statlist.clone();
    if statlist.is_empty() {
        return Ok(None);
    }

    let rte_inh = planner_rt_fetch(run, root, root.rel(relid).relid).inh;

    // look for the ndistinct statistics object matching the most vars.
    let mut nmatches_vars = 0i32; // we require at least two matches
    let mut nmatches_exprs = 0i32;
    let mut stat_oid: Oid = InvalidOid;
    let mut matched_info: Option<NodeId> = None;

    for &stat_id in statlist.iter() {
        let info_kind = root.statistic_ext(stat_id).kind;
        let info_inherit = root.statistic_ext(stat_id).inherit;

        // skip statistics of other kinds.
        if info_kind != STATS_EXT_NDISTINCT {
            continue;
        }
        // skip statistics with mismatching stxdinherit value.
        if info_inherit != rte_inh {
            continue;
        }

        let info_exprs = root.statistic_ext(stat_id).exprs.clone();
        let info_keys = root.statistic_ext(stat_id).keys.clone();

        let mut nshared_vars = 0i32;
        let mut nshared_exprs = 0i32;

        for varinfo in varinfos.iter() {
            // simple Var, search in statistics keys directly.
            if let Some(var) = varinfo.var.as_var() {
                let attnum = var.varattno;
                // Ignore system attributes.
                if !attr_number_is_for_user_defined_attr(attnum) {
                    continue;
                }
                if rel_seams::relids_is_member::call(attnum as i32, &info_keys) {
                    nshared_vars += 1;
                }
                continue;
            }

            // expression - see if it's in the statistics object.
            for &expr_id in info_exprs.iter() {
                let expr = root.node(expr_id).clone();
                if eq::equal_expr::call(&varinfo.var, &expr) {
                    nshared_exprs += 1;
                    break;
                }
            }
        }

        // The ndistinct statistics need at least two columns to match.
        if nshared_vars + nshared_exprs < 2 {
            continue;
        }

        // Check if these statistics are a better match than the previous best.
        if (nshared_exprs > nmatches_exprs)
            || (nshared_exprs == nmatches_exprs && nshared_vars > nmatches_vars)
        {
            stat_oid = root.statistic_ext(stat_id).stat_oid;
            nmatches_vars = nshared_vars;
            nmatches_exprs = nshared_exprs;
            matched_info = Some(stat_id);
        }
    }

    // No match?
    if stat_oid == InvalidOid {
        return Ok(None);
    }
    let matched_id = matched_info.expect("estimate_multivariate_ndistinct: matched stat");

    let stats = mvdistinct::statext_ndistinct_load(stat_oid, rte_inh)?;

    // If we have a match, search it for the specific item that matches and
    // construct the output values.
    let matched_exprs = root.statistic_ext(matched_id).exprs.clone();
    let matched_keys = root.statistic_ext(matched_id).keys.clone();

    // How much we need to offset the attnums? If there are no expressions, no
    // offset is needed. Otherwise offset enough to move the lowest one (which is
    // equal to number of expressions) to 1.
    let attnum_offset: i32 = if !matched_exprs.is_empty() {
        matched_exprs.len() as i32 + 1
    } else {
        0
    };

    // see what actually matched.
    let mut matched: Relids = None;
    for varinfo in varinfos.iter() {
        let mut found = false;

        if let Some(var) = varinfo.var.as_var() {
            let attnum = var.varattno;
            if !attr_number_is_for_user_defined_attr(attnum) {
                continue;
            }
            // Is the variable covered by the statistics object?
            if !rel_seams::relids_is_member::call(attnum as i32, &matched_keys) {
                continue;
            }
            let attnum = attnum as i32 + attnum_offset;
            matched = rel_seams::relids_add_member::call(matched, attnum);
            found = true;
        }

        if found {
            continue;
        }

        // expression - see if it's in the statistics object.
        let mut idx = 0i32;
        for &expr_id in matched_exprs.iter() {
            let expr = root.node(expr_id).clone();
            if eq::equal_expr::call(&varinfo.var, &expr) {
                let attnum = -(idx + 1) + attnum_offset;
                matched = rel_seams::relids_add_member::call(matched, attnum);
                // there should be just one matching expression.
                break;
            }
            idx += 1;
        }
    }

    // Find the specific item that exactly matches the combination.
    let matched_nmembers = rel_seams::relids_num_members::call(&matched);
    let mut item_ndistinct: Option<f64> = None;
    for tmpitem in stats.items.iter() {
        if tmpitem.attributes.len() as i32 != matched_nmembers {
            continue;
        }
        // assume it's the right item; check that all item attributes fit.
        let mut ok = true;
        for &attr in tmpitem.attributes.iter() {
            let attnum = attr as i32 + attnum_offset;
            if !rel_seams::relids_is_member::call(attnum, &matched) {
                ok = false;
                break;
            }
        }
        if ok {
            item_ndistinct = Some(tmpitem.ndistinct);
            break;
        }
    }

    // Make sure we found an item.
    let item_ndistinct = item_ndistinct
        .ok_or_else(|| types_error::PgError::error("corrupt MVNDistinct entry"))?;

    // Form the output varinfo list, keeping only unmatched ones.
    let old = core::mem::take(varinfos);
    let mut newlist: alloc::vec::Vec<GroupVarInfo> = alloc::vec::Vec::new();
    for varinfo in old.into_iter() {
        if let Some(var) = varinfo.var.as_var() {
            let attnum = var.varattno;
            if !attr_number_is_for_user_defined_attr(attnum) {
                newlist.push(varinfo);
                continue;
            }
            let attnum = attnum as i32 + attnum_offset;
            // if it's not matched, keep the varinfo.
            if !rel_seams::relids_is_member::call(attnum, &matched) {
                newlist.push(varinfo);
            }
            continue;
        }

        // Process complex expressions: search for an exact match.
        let mut found = false;
        for &expr_id in matched_exprs.iter() {
            let expr = root.node(expr_id).clone();
            if eq::equal_expr::call(&varinfo.var, &expr) {
                found = true;
                break;
            }
        }
        if found {
            continue;
        }
        newlist.push(varinfo);
    }
    *varinfos = newlist;

    Ok(Some(item_ndistinct))
}

/// Seam body for `estimate_num_groups`.
pub fn seam_estimate_num_groups<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    group_exprs: &[NodeId],
    input_rows: f64,
    estinfo: Option<&mut EstimationInfo>,
) -> PgResult<f64> {
    estimate_num_groups(run, root, group_exprs, input_rows, estinfo)
}

/* ---------------------------------------------------------------------------
 * generic_restriction_selectivity (selfuncs.c:921)
 * ------------------------------------------------------------------------- */

/// `generic_restriction_selectivity(root, oproid, collation, args, varRelid,
/// default_selectivity)` (selfuncs.c) — selectivity for an operator we have no
/// special knowledge of, by applying it to the column's MCV and/or histogram
/// stats. 1:1 with the C body. Reaches the keystone-blocked
/// `get_restriction_variable`; the MCV/histogram merge math is fully ported.
pub fn generic_restriction_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    oproid: Oid,
    collation: Oid,
    args: &[NodeId],
    var_relid: i32,
    default_selectivity: f64,
) -> PgResult<f64> {
    // If not var OP something or something OP var, punt.
    let (vardata, other, varonleft) =
        match get_restriction_variable(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(default_selectivity),
        };

    // If the something is a NULL constant, assume operator is strict.
    if let Some(c) = other.as_const() {
        if c.constisnull {
            release_variable_stats(vardata);
            return Ok(0.0);
        }
    }

    let mut selec;
    if let Some(c) = other.as_const() {
        // Variable is being compared to a known non-null constant.
        let constval = types_datum::datum::Datum::from_usize(c.constvalue.as_usize());
        let opproc_oid = lsc::get_opcode::call(oproid)?;

        // Selectivity for the column's most common values.
        let (mcvsel, mcvsum) =
            mcv_selectivity(mcx, &vardata, opproc_oid, collation, constval, varonleft)?;

        // If the histogram is large enough, use it; else fall back on default.
        let (mut sel, hist_size) = histogram_selectivity(
            mcx, &vardata, opproc_oid, collation, constval, varonleft, 10, 1,
        )?;
        if sel < 0.0 {
            sel = default_selectivity;
        } else if hist_size < 100 {
            // Combine histogram and default for sizes 10..100.
            let hist_weight = hist_size as f64 / 100.0;
            sel = sel * hist_weight + default_selectivity * (1.0 - hist_weight);
        }

        // Don't believe extremely small or large estimates.
        if sel < 0.0001 {
            sel = 0.0001;
        } else if sel > 0.9999 {
            sel = 0.9999;
        }

        // Account for nulls.
        let nullfrac = match vardata.stats_tuple {
            Some(t) => crate::scalar::stats_tuple_stanullfrac(t) as f64,
            None => 0.0,
        };

        // Merge MCV and histogram (histogram covers non-null non-MCV values).
        sel *= 1.0 - nullfrac - mcvsum;
        sel += mcvsel;
        selec = sel;
    } else {
        // Comparison value is not constant, so we can't do anything.
        selec = default_selectivity;
    }

    release_variable_stats(vardata);

    selec = clamp_probability(selec);
    Ok(selec)
}

/* ---------------------------------------------------------------------------
 * estimate_array_length (selfuncs.c:2146)
 * ------------------------------------------------------------------------- */

/// `estimate_array_length(root, arrayexpr)` (selfuncs.c) — estimate the number
/// of elements in an array-valued expression.
///
/// The `strip_array_coercion` peel and the `Const` / `ArrayExpr` fast paths
/// require resolving the arena node and decoding an array varlena
/// (`DatumGetArrayTypeP` / `ArrayGetNItems`), which crosses into the unported
/// arrayfuncs varlena envelope; the statistics fallback uses the
/// keystone-blocked `examine_variable`. The default guess of `10` (matching
/// `scalararraysel`) is the live tail. Kept structurally as a precise panic for
/// the non-default paths.
pub fn estimate_array_length<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    arrayexpr: NodeId,
) -> PgResult<f64> {
    // C `estimate_array_length` peels `strip_array_coercion`, then:
    //   * a `Const` array -> `ArrayGetNItems(DatumGetArrayTypeP(...))`;
    //   * an `ArrayExpr` -> `list_length(arrayexpr->elements)`;
    //   * otherwise examines the variable's stats (DECHIST / element stats).
    // The Const path needs the array-varlena decode (`DatumGetArrayTypeP` /
    // `ArrayGetNItems`, the unported arrayfuncs varlena envelope), and the
    // stats path needs `examine_variable`, whose seam carries `&PlannerRun` +
    // `&mut PlannerInfo` that this `estimate_array_length` seam (consumed by
    // costsize with a shared `&PlannerInfo` and no run) does not have. The
    // `ArrayExpr` element-count fast path is the live tail; everything else
    // falls back to the default guess of 10 (matching `scalararraysel`), which
    // is what C returns when no recognized form yields a count.
    let _ = mcx;
    if let Some(ae) = root.node(arrayexpr).as_arrayexpr() {
        return Ok(ae.elements.len() as f64);
    }
    Ok(10.0)
}
