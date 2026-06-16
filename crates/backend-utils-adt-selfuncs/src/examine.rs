//! The variable-recognition layer of selfuncs.c — `examine_variable`,
//! `examine_simple_variable`, `get_restriction_variable`, `get_join_variables`,
//! `all_rows_selectable`, and `ReleaseVariableStats`.
//!
//! ## What this layer does
//!
//! These functions locate the `pg_statistic` data for an expression. The
//! statistics-acquisition core, `examine_simple_variable`, reads
//! `planner_rt_fetch(varno, root)` (resolved through the [`PlannerRun`] RTE
//! store), dispatches on `rte->rtekind`, and for `RTE_RELATION` runs
//! `SearchSysCache3(STATRELATTINH, ...)` to pin a `pg_statistic` `HeapTuple`;
//! for `RTE_SUBQUERY`/`RTE_CTE` it recurses into the subquery's `subroot`.
//!
//! The PlannerRun-through-costing keystone threads `&PlannerRun<'mcx>` to the
//! restriction/join-selectivity call sites, so `examine_simple_variable` can
//! call [`planner_rt_fetch`] over `simple_rte_array` and resolve each
//! `RangeTblEntry`. The arena-mutating `&mut PlannerInfo` is threaded here too
//! (from `call_oprrest`/`call_oprjoin`), so the PlaceHolderVar/RelabelType
//! stripping can re-intern the stripped expression into the planner node arena.
//!
//! ## Remaining seam-and-panic boundaries (genuinely unported owners)
//!
//! * `SearchSysCache3(STATRELATTINH, ...)` — the `pg_statistic` catcache probe
//!   ([`search_statrelattinh`](backend_utils_cache_syscache_seams::search_statrelattinh)).
//!   The seam is declared by the (ported) syscache owner but is not installed
//!   yet, so a relation-column stats lookup raises the owner's loud panic until
//!   the catcache wiring lands (mirror-PG-and-panic). With no live `statsTuple`,
//!   the stats-absent / default-estimate paths are the live common case.
//! * `statext_expressions_load` — extended-statistics per-expression tuple load
//!   (unported; reached only after an `equal()` match on an EXPRESSIONS stat).
//! * the CTE-subroot recursion (`cte_plan_ids` / `glob->subroots`) requires the
//!   unported CTE planner.

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::{Expr, Var};
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{NodeId, PlannerInfo, RelId, Relids, SpecialJoinInfo};
use types_selfuncs::{StatsTuple, StatsTupleFreeFunc, VariableStatData};

use backend_catalog_aclchk_seams as aclchk;
use backend_nodes_core::bitmapset as colbms;
use backend_nodes_equalfuncs_seams as eq;
use backend_nodes_nodeFuncs_seams as nf;
use backend_optimizer_path_joinpath_seams as jp;
use backend_optimizer_util_relnode_seams as rel_seams;
use backend_utils_cache_syscache_seams as syscache;

use crate::FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER;

/// `STATS_EXT_EXPRESSIONS` (pg_statistic_ext.h) — the per-expression extended
/// statistics kind, `'e'`, matched against [`StatisticExtInfo::kind`].
const STATS_EXT_EXPRESSIONS: i8 = b'e' as i8;

/* ===========================================================================
 * strip_all_phvs_deep (selfuncs.c) — deeply strip all PlaceHolderVars.
 * ========================================================================= */

/// `contain_placeholder_walker(node)` (selfuncs.c) — lightweight check for any
/// `PlaceHolderVar` anywhere in the expression. `PlaceHolderVar` is an `Expr`
/// variant in this model but is not handled by the generic
/// [`expression_tree_walker`](backend_nodes_core::nodefuncs::expression_tree_walker),
/// so the PHV test is done here and the walker recurses into the rest.
fn contain_placeholder(node: &Expr) -> bool {
    if matches!(node, Expr::PlaceHolderVar(_)) {
        return true;
    }
    let mut found = false;
    backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |child: &Expr| {
        if contain_placeholder(child) {
            found = true;
            return true; // abort
        }
        false
    });
    found
}

/// `strip_all_phvs_mutator(node)` (selfuncs.c) — replace every `PlaceHolderVar`
/// with its contained `phexpr`, recursively. Operates on an owned [`Expr`]
/// (matching the C mutator returning a fresh `Node *`).
fn strip_all_phvs_mutator(node: Expr) -> Expr {
    if let Expr::PlaceHolderVar(phv) = node {
        let inner = phv
            .phexpr
            .map(|b| *b)
            .expect("strip_all_phvs_mutator: PlaceHolderVar has no phexpr");
        return strip_all_phvs_mutator(inner);
    }
    backend_nodes_core::nodefuncs::expression_tree_mutator(node, &mut strip_all_phvs_mutator)
}

/// `strip_all_phvs_deep(root, node)` (selfuncs.c) — deeply strip all
/// PlaceHolderVars. The lightweight walker first checks for any PHV (avoiding a
/// tree copy in the common case); the expensive mutator runs only when one is
/// present. Returns an owned [`Expr`] (a clone of `node` when nothing changed).
fn strip_all_phvs_deep(root: &PlannerInfo, node: &Expr) -> Expr {
    let last_ph_id = root.glob.as_ref().map(|g| g.last_ph_id).unwrap_or(0);
    if last_ph_id == 0 {
        return node.clone();
    }
    if !contain_placeholder(node) {
        return node.clone();
    }
    strip_all_phvs_mutator(node.clone())
}

/* ===========================================================================
 * examine_variable (selfuncs.c)
 * ========================================================================= */

/// `examine_variable(root, node, varRelid, &vardata)` (selfuncs.c) — look up
/// statistical data about an expression. Strips PlaceHolderVars and
/// binary-compatible RelabelTypes, takes the fast path for a simple `Var`, and
/// otherwise determines variable membership to match index expressions /
/// extended statistics. 1:1 with the C body.
pub(crate) fn examine_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    node_id: NodeId,
    var_relid: i32,
) -> PgResult<VariableStatData> {
    // Make sure we don't return dangling pointers in vardata.
    let mut vardata = VariableStatData::zeroed(node_id);

    // Save the exposed type of the expression.
    let node_expr = root.node(node_id).clone();
    vardata.vartype = backend_nodes_core::nodefuncs::expr_type(Some(&node_expr))?;

    // PlaceHolderVars are transparent for statistics lookup; strip them first.
    let mut basenode = strip_all_phvs_deep(root, &node_expr);

    // Look inside any binary-compatible relabeling (handle nested RelabelTypes).
    while let Expr::RelabelType(rt) = &basenode {
        let arg = rt
            .arg
            .clone()
            .expect("examine_variable: RelabelType has no arg");
        basenode = *arg;
    }

    // Fast path for a simple Var.
    if let Expr::Var(var) = &basenode {
        if var_relid == 0 || var_relid == var.varno {
            let var = var.clone();
            // Re-intern the stripped Var so vardata->var is a handle to the Var
            // without phvs or relabeling (C: vardata->var = basenode).
            vardata.var = root.alloc_node(Expr::Var(var.clone()));
            vardata.rel = Some(rel_seams::find_base_rel::call(root, var.varno));
            vardata.atttype = var.vartype;
            vardata.atttypmod = var.vartypmod;
            vardata.isunique = has_unique_index(root, vardata.rel.unwrap(), var.varattno);

            // Try to locate some stats.
            examine_simple_variable(mcx, run, root, &var, &mut vardata)?;

            return Ok(vardata);
        }
    }

    // A more complicated expression. Determine variable membership; when
    // varRelid isn't zero, only vars of that relation are considered "real".
    // pull_varnos(root, basenode): intern the stripped node and use the
    // root-aware NodeId form (varlevelsup-correct).
    let basenode_id = root.alloc_node(basenode.clone());
    let varnos: Relids = jp::pull_varnos::call(root, basenode_id);
    let outer_join_rels = root.outer_join_rels.clone();
    let basevarnos: Relids = rel_seams::relids_difference::call(&varnos, &outer_join_rels);

    let mut onerel: Option<RelId> = None;
    // The expression vardata->var ends up pointing at (the stripped basenode,
    // when recognized to a relation).
    let mut node_for_var: Option<Expr> = None;

    if rel_seams::relids_is_empty::call(&basevarnos) {
        // No Vars at all ... must be pseudo-constant clause.
    } else if let Some(relid) = rel_seams::relids_get_singleton_member::call(&basevarnos) {
        if var_relid == 0 || var_relid == relid {
            let r = rel_seams::find_base_rel::call(root, relid);
            onerel = Some(r);
            vardata.rel = Some(r);
            node_for_var = Some(basenode.clone());
        }
        // else treat it as a constant
    } else if var_relid == 0 {
        // Treat it as a variable of a join relation.
        vardata.rel = rel_seams::find_join_rel::call(root, &varnos);
        node_for_var = Some(basenode.clone());
    } else if rel_seams::relids_is_member::call(var_relid, &varnos) {
        // Ignore the vars belonging to other relations.
        vardata.rel = Some(rel_seams::find_base_rel::call(root, var_relid));
        node_for_var = Some(basenode.clone());
        // note: no point in expressional-index search here
    }
    // else treat it as a constant

    // vardata->var = node (the recognized stripped node, else the original).
    let final_node = node_for_var.clone().unwrap_or_else(|| node_expr.clone());
    vardata.var = root.alloc_node(final_node.clone());
    vardata.atttype = backend_nodes_core::nodefuncs::expr_type(Some(&final_node))?;
    vardata.atttypmod = backend_nodes_core::nodefuncs::expr_typmod(Some(&final_node))?;

    if let Some(onerel) = onerel {
        // We have an expression in vars of a single relation. Try to match it
        // to expressional index columns / extended statistics.
        examine_expression_stats(mcx, run, root, onerel, &varnos, &basenode, &mut vardata)?;
    }

    Ok(vardata)
}

/// The "expression in vars of a single relation" tail of `examine_variable`:
/// match `node` against expressional index columns and extended-statistics
/// expressions, pinning a `pg_statistic` tuple when an expression matches.
fn examine_expression_stats<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    onerel: RelId,
    varnos: &Relids,
    basenode: &Expr,
    vardata: &mut VariableStatData,
) -> PgResult<()> {
    // The nullingrels bits could prevent matching; strip them when the
    // expression overlaps any outer join.
    let mut node = basenode.clone();
    let outer_join_rels = root.outer_join_rels.clone();
    if rel_seams::relids_overlap::call(varnos, &outer_join_rels) {
        let none: Relids = None;
        node = nf::remove_nulling_relids::call(&node, &outer_join_rels, &none);
    }

    // Snapshot the per-rel index + stat lists (immutable reads) so we can match
    // and then acquire stats through &mut root without aliasing.
    let rel = root.rel(onerel);
    let onerel_relid = rel.relid;
    let indexlist = rel.indexlist.clone();
    let statlist = rel.statlist.clone();

    // --- expressional index columns ---
    for index in indexlist.iter() {
        if index.indexprs.is_empty() {
            continue; // no expressions here...
        }
        let mut indexpr_iter = index.indexprs.iter();
        for pos in 0..(index.ncolumns as usize) {
            if index.indexkeys.get(pos).copied() != Some(0) {
                continue;
            }
            let indexkey_id = *indexpr_iter
                .next()
                .ok_or_else(|| PgError::error("too few entries in indexprs list"))?;
            let mut indexkey = root.node(indexkey_id).clone();
            if let Expr::RelabelType(rt) = &indexkey {
                indexkey = *rt
                    .arg
                    .clone()
                    .expect("examine_variable: index RelabelType has no arg");
            }
            if eq::equal_expr::call(&node, &indexkey) {
                // Found a match ... is it a unique index?
                if index.unique
                    && index.nkeycolumns == 1
                    && pos == 0
                    && (index.indpred.is_empty() || index.predOK)
                {
                    vardata.isunique = true;
                }
                // Has it got stats? Only for non-partial indexes.
                if index.indpred.is_empty() {
                    vardata.stats_tuple =
                        search_statrelattinh(mcx, index.indexoid, (pos + 1) as AttrNumber, false)?;
                    vardata.freefunc = Some(StatsTupleFreeFunc::ReleaseSysCache);

                    if vardata.stats_tuple.is_some() {
                        // SELECT privilege on the whole index's table.
                        let index_rel_relid = index
                            .rel
                            .map(|r| root.rel(r).relid as i32)
                            .unwrap_or(onerel_relid as i32);
                        vardata.acl_ok =
                            all_rows_selectable(mcx, run, root, index_rel_relid as u32, None)?;
                    } else {
                        vardata.acl_ok = true; // suppress leakproofness checks later
                    }
                }
                if vardata.stats_tuple.is_some() {
                    break;
                }
            }
        }
        if vardata.stats_tuple.is_some() {
            break;
        }
    }

    // --- extended statistics with a matching expression ---
    for &stat_id in statlist.iter() {
        if vardata.stats_tuple.is_some() {
            break;
        }
        let info = root.statistic_ext(stat_id);
        if info.kind != STATS_EXT_EXPRESSIONS {
            continue; // skip stats without per-expression stats
        }
        let rte_inh = planner_rt_fetch(run, root, onerel_relid).inh;
        if info.inherit != rte_inh {
            continue; // skip stats with mismatching stxdinherit value
        }
        let stat_oid = info.stat_oid;
        let exprs = info.exprs.clone();
        for (pos, &expr_id) in exprs.iter().enumerate() {
            let mut expr = root.node(expr_id).clone();
            if let Expr::RelabelType(rt) = &expr {
                expr = *rt
                    .arg
                    .clone()
                    .expect("examine_variable: extstats RelabelType has no arg");
            }
            if eq::equal_expr::call(&node, &expr) {
                vardata.stats_tuple = statext_expressions_load(mcx, stat_oid, rte_inh, pos)?;
                vardata.freefunc = Some(StatsTupleFreeFunc::ReleaseDummy);
                vardata.acl_ok = all_rows_selectable(mcx, run, root, onerel_relid, None)?;
                break;
            }
        }
    }

    Ok(())
}

/* ===========================================================================
 * examine_simple_variable (selfuncs.c)
 * ========================================================================= */

/// `examine_simple_variable(root, var, vardata)` (selfuncs.c) — handle a simple
/// `Var` for `examine_variable`, recursing to deal with Vars referencing
/// subqueries (sub-SELECT-in-FROM or CTE style). 1:1 with the C body.
fn examine_simple_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &PlannerInfo,
    var: &Var,
    vardata: &mut VariableStatData,
) -> PgResult<()> {
    let rte = planner_rt_fetch(run, root, var.varno as Index);
    let rtekind = rte.rtekind;
    let rte_relid = rte.relid;
    let rte_inh = rte.inh;
    let rte_self_reference = rte.self_reference;

    if rtekind == RTEKind::RTE_RELATION {
        // Plain table or parent of an inheritance appendrel: look up the column
        // in pg_statistic.
        vardata.stats_tuple = search_statrelattinh(mcx, rte_relid, var.varattno, rte_inh)?;
        vardata.freefunc = Some(StatsTupleFreeFunc::ReleaseSysCache);

        if vardata.stats_tuple.is_some() {
            // Test if user has permission to read all rows from this column.
            let attset = colbms::bms_make_singleton(
                mcx,
                (var.varattno - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER) as i32,
            )?;
            vardata.acl_ok = all_rows_selectable(mcx, run, root, var.varno as u32, Some(&attset))?;
        } else {
            vardata.acl_ok = true; // suppress any leakproofness checks later
        }
    } else if (rtekind == RTEKind::RTE_SUBQUERY && !rte_inh)
        || (rtekind == RTEKind::RTE_CTE && !rte_self_reference)
    {
        examine_subquery_variable(mcx, run, root, var, rtekind, vardata)?;
    } else {
        // Otherwise the Var comes from a FUNCTION or VALUES RTE; nothing to do.
    }
    Ok(())
}

/// The `RTE_SUBQUERY` / `RTE_CTE` recursion branch of [`examine_simple_variable`].
fn examine_subquery_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &PlannerInfo,
    var: &Var,
    rtekind: RTEKind,
    vardata: &mut VariableStatData,
) -> PgResult<()> {
    use types_core::primitive::InvalidAttrNumber;

    // Punt if it's a whole-row var rather than a plain column reference.
    if var.varattno == InvalidAttrNumber {
        return Ok(());
    }

    // Find the subquery's planner subroot.
    let subroot: &PlannerInfo = if rtekind == RTEKind::RTE_SUBQUERY {
        let relid = rel_seams::find_base_rel::call(root, var.varno);
        match root.rel(relid).subroot.0.as_deref() {
            Some(sr) => sr,
            None => return Ok(()), // subquery hasn't been planned yet
        }
    } else {
        // CTE case (see examine_cte_variable's seam-and-panic rationale).
        return examine_cte_variable(mcx, run, root, var, vardata);
    };

    // Use the subquery parsetree as mangled by the planner (subroot->parse).
    let subquery = run.resolve(subroot.parse);

    // Punt if subquery uses set operations or grouping sets.
    if subquery.setOperations.is_some() || !subquery.groupingSets.is_empty() {
        return Ok(());
    }

    // Get the subquery output expression referenced by the upper Var.
    let subtlist = if !subquery.returningList.is_empty() {
        subquery.returningList.as_slice()
    } else {
        subquery.targetList.as_slice()
    };
    let ste = match backend_parser_relation::get_tle_by_resno(subtlist, var.varattno) {
        Some(t) if !t.resjunk => t,
        _ => {
            return Err(PgError::error(alloc::format!(
                "subquery does not have attribute {}",
                var.varattno
            )))
        }
    };
    let ste_expr = ste.expr.clone();

    // DISTINCT: can't use stats; but the only DISTINCT column is unique.
    if !subquery.distinctClause.is_empty() {
        let distinct = sort_group_list(&subquery.distinctClause);
        if distinct.len() == 1
            && backend_parser_clause::targetIsInSortList(ste, InvalidOid, &distinct)
        {
            vardata.isunique = true;
        }
        return Ok(());
    }

    // The same idea works for a GROUP-BY too.
    if !subquery.groupClause.is_empty() {
        let group = sort_group_list(&subquery.groupClause);
        if group.len() == 1 && backend_parser_clause::targetIsInSortList(ste, InvalidOid, &group) {
            vardata.isunique = true;
        }
        return Ok(());
    }

    // If the sub-query originated from a security_barrier view, don't dig down.
    if planner_rt_fetch(run, root, var.varno as Index).security_barrier {
        return Ok(());
    }

    // Can only handle a simple Var of subquery's query level.
    if let Some(inner_var) = ste_expr.as_ref().and_then(|e| e.as_var()) {
        if inner_var.varlevelsup == 0 {
            let inner_var = inner_var.clone();
            examine_simple_variable(mcx, run, subroot, &inner_var, vardata)?;
        }
    }

    Ok(())
}

/// The CTE arm of [`examine_subquery_variable`].
fn examine_cte_variable<'mcx, 'run>(
    _mcx: Mcx<'mcx>,
    _run: &PlannerRun<'run>,
    _root: &PlannerInfo,
    _var: &Var,
    _vardata: &mut VariableStatData,
) -> PgResult<()> {
    // The CTE subroot lookup walks `cteroot->parse->cteList` to match the CTE by
    // `rte->ctename`, indexes `cteroot->cte_plan_ids`, and resolves
    // `glob->subroots[plan_id - 1]`. The parent_root chain + per-run CTE
    // planning that populate `cte_plan_ids` and the run's subroot store are
    // produced only by the unported CTE planner (SS_process_ctes /
    // subquery_planner). Until that lands, recursing into a CTE column's stats
    // is seam-and-panic (mirror-PG-and-panic) rather than silently returning
    // stats-free vardata.
    panic!(
        "selfuncs: examine_simple_variable CTE recursion is blocked — locating the CTE's planner \
         subroot via parent_root walk + cte_plan_ids -> glob->subroots requires the unported CTE \
         planner to have populated cte_plan_ids and the run's subroot store"
    )
}

/* ===========================================================================
 * all_rows_selectable (selfuncs.c)
 * ========================================================================= */

/// `all_rows_selectable(root, varno, varattnos)` (selfuncs.c) — whether the user
/// may read all rows for the given relation/columns (table-level or per-column
/// SELECT privilege, and no security barrier / RLS quals). 1:1 with the C body.
fn all_rows_selectable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &PlannerInfo,
    varno: u32,
    varattnos: Option<&Bitmapset<'mcx>>,
) -> PgResult<bool> {
    use types_acl::acl::{AclMaskHow, AclResult, ACL_SELECT};

    // find_base_rel_noerr(root, varno) == simple_rel_array[varno].
    let rel = root
        .simple_rel_array
        .get(varno as usize)
        .copied()
        .flatten();

    // Determine the user ID for privilege checks.
    let userid = if let Some(relid) = rel {
        root.rel(relid).userid
    } else {
        // RETURNING Var for an INSERT target relation: use the RTEPermissionInfo
        // associated with the RTE.
        let perminfoindex = planner_rt_fetch(run, root, varno).perminfoindex;
        getrte_perminfo_checkasuser(run, root, perminfoindex)?
    };
    let userid = if OidIsValid(userid) {
        userid
    } else {
        backend_utils_init_miscinit::GetUserId()
    };

    // Navigate to the inheritance root parent if this is a child.
    let mut cur_varno = varno;
    // varattnos is owned locally because the inheritance walk rewrites it.
    let mut cur_attnos: Option<Bitmapset<'mcx>> = match colbms::bms_copy(mcx, varattnos)? {
        Some(b) => Some((*b).clone_in(mcx)?),
        None => None,
    };
    if !root.append_rel_array.is_empty() {
        match navigate_to_inh_root(mcx, run, root, cur_varno, cur_attnos)? {
            None => return Ok(false), // attr is local to child
            Some((vn, an)) => {
                cur_varno = vn;
                cur_attnos = an;
            }
        }
    }

    let rte = planner_rt_fetch(run, root, cur_varno);
    let rte_relid = rte.relid;

    // No securityQuals from security barrier views or RLS policies.
    if !rte.securityQuals.is_empty() {
        return Ok(false);
    }

    // Table-level SELECT privilege is sufficient for the requested attributes.
    if aclchk::pg_class_aclcheck::call(rte_relid, userid, ACL_SELECT)? == AclResult::AclcheckOk {
        return Ok(true);
    }

    if cur_attnos.is_none() {
        return Ok(false); // whole-table access requested
    }

    // Check per-column privileges.
    let attset = cur_attnos.as_ref();
    let mut varattno = -1i32;
    loop {
        varattno = colbms::bms_next_member(attset, varattno);
        if varattno < 0 {
            break;
        }
        let attno = varattno + FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER as i32;
        if attno == 0 {
            // Whole-row reference: must have access to all columns.
            if aclchk::pg_attribute_aclcheck_all::call(
                rte_relid,
                userid,
                ACL_SELECT,
                AclMaskHow::AclmaskAll,
            )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }
        } else if aclchk::pg_attribute_aclcheck::call(
            rte_relid,
            attno as AttrNumber,
            userid,
            ACL_SELECT,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }

    Ok(true)
}

/// The inheritance-root navigation loop of `all_rows_selectable`. Returns
/// `None` when a required attribute is local to the child (C `return false`),
/// else `Some((final_varno, mapped_attnos))`.
fn navigate_to_inh_root<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &PlannerInfo,
    mut varno: u32,
    mut attnos: Option<Bitmapset<'mcx>>,
) -> PgResult<Option<(u32, Option<Bitmapset<'mcx>>)>> {
    loop {
        let appinfo = match root
            .append_rel_array
            .get(varno as usize)
            .and_then(|a| a.as_ref())
        {
            Some(a) => a,
            None => break,
        };
        if planner_rt_fetch(run, root, appinfo.parent_relid).rtekind != RTEKind::RTE_RELATION {
            break;
        }
        let num_child_cols = appinfo.num_child_cols;
        let parent_colnos = appinfo.parent_colnos.clone();
        let parent_relid = appinfo.parent_relid;

        let mut parent_varattnos: Option<mcx::PgBox<'mcx, Bitmapset<'mcx>>> = None;
        let mut varattno = -1i32;
        loop {
            varattno = colbms::bms_next_member(attnos.as_ref(), varattno);
            if varattno < 0 {
                break;
            }
            let attno = varattno + FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER as i32;
            if attno == 0 {
                // Whole-row reference: map each child column to the parent.
                for a in 1..=num_child_cols {
                    let parent_attno = parent_colnos[(a - 1) as usize];
                    if parent_attno == 0 {
                        return Ok(None); // attr is local to child
                    }
                    parent_varattnos = Some(colbms::bms_add_member(
                        mcx,
                        parent_varattnos,
                        parent_attno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER as i32,
                    )?);
                }
            } else {
                let parent_attno = if attno < 0 {
                    attno // system attnos are the same in all tables
                } else {
                    if attno > num_child_cols {
                        return Ok(None); // safety check
                    }
                    let pa = parent_colnos[(attno - 1) as usize] as i32;
                    if pa == 0 {
                        return Ok(None); // attr is local to child
                    }
                    pa
                };
                parent_varattnos = Some(colbms::bms_add_member(
                    mcx,
                    parent_varattnos,
                    parent_attno - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER as i32,
                )?);
            }
        }

        varno = parent_relid;
        attnos = match parent_varattnos {
            Some(b) => Some((*b).clone_in(mcx)?),
            None => None,
        };
    }
    Ok(Some((varno, attnos)))
}

/* ===========================================================================
 * get_restriction_variable / get_join_variables / ReleaseVariableStats
 * ========================================================================= */

/// `get_restriction_variable(root, args, varRelid, &vardata, &other,
/// &varonleft)` (selfuncs.c) — recognize a `(var op const)` / `(const op var)`
/// restriction clause. Returns `None` when the clause has the wrong structure
/// (C: `false`), else `(vardata, other, varonleft)`. 1:1 with the C body.
pub(crate) fn get_restriction_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<Option<(VariableStatData, Expr, bool)>> {
    // Fail if not a binary opclause (probably shouldn't happen).
    if args.len() != 2 {
        return Ok(None);
    }
    let left = args[0];
    let right = args[1];

    let vardata = examine_variable(mcx, run, root, left, var_relid)?;
    let rdata = examine_variable(mcx, run, root, right, var_relid)?;

    // If one side is a variable and the other not, we win.
    if vardata.rel.is_some() && rdata.rel.is_none() {
        let other = estimate_other(mcx, root, rdata.var)?;
        return Ok(Some((vardata, other, true)));
    }
    if vardata.rel.is_none() && rdata.rel.is_some() {
        let other = estimate_other(mcx, root, vardata.var)?;
        release_variable_stats(vardata);
        return Ok(Some((rdata, other, false)));
    }

    // Oops, clause has wrong structure (probably var op var).
    release_variable_stats(vardata);
    release_variable_stats(rdata);
    Ok(None)
}

/// `estimate_expression_value(root, node)` over an arena node handle — fold the
/// node to a `Const` if possible (the ported clauses.c folder).
fn estimate_other<'mcx>(mcx: Mcx<'mcx>, root: &PlannerInfo, node: NodeId) -> PgResult<Expr> {
    let expr = root.node(node).clone();
    backend_optimizer_util_clauses::estimate_expression_value(mcx, expr)
}

/// `get_join_variables(root, args, sjinfo, &vardata1, &vardata2,
/// &join_is_reversed)` (selfuncs.c) — examine the two operands of a join
/// clause. 1:1 with the C body.
pub(crate) fn get_join_variables<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    args: &[NodeId],
    sjinfo: &SpecialJoinInfo,
) -> PgResult<(VariableStatData, VariableStatData, bool)> {
    if args.len() != 2 {
        return Err(PgError::error("join operator should take two arguments"));
    }
    let left = args[0];
    let right = args[1];

    let vardata1 = examine_variable(mcx, run, root, left, 0)?;
    let vardata2 = examine_variable(mcx, run, root, right, 0)?;

    let reversed_1 = match vardata1.rel {
        Some(rel1) => {
            let relids1 = root.rel(rel1).relids.clone();
            rel_seams::relids_is_subset::call(&relids1, &sjinfo.syn_righthand)
        }
        None => false,
    };
    let join_is_reversed = if reversed_1 {
        true
    } else {
        match vardata2.rel {
            Some(rel2) => {
                let relids2 = root.rel(rel2).relids.clone();
                rel_seams::relids_is_subset::call(&relids2, &sjinfo.syn_lefthand)
            }
            None => false,
        }
    };

    Ok((vardata1, vardata2, join_is_reversed))
}

/// `ReleaseVariableStats(vardata)` (selfuncs.h) — run `vardata.freefunc` on the
/// pinned `statsTuple`. A no-op when there is no tuple, else dispatches the
/// closed `freefunc` enum.
pub(crate) fn release_variable_stats(vardata: VariableStatData) {
    let stats_tuple = match vardata.stats_tuple {
        None => return, // HeapTupleIsValid(statsTuple) is false — nothing to free.
        Some(t) => t,
    };
    match vardata.freefunc {
        Some(StatsTupleFreeFunc::ReleaseSysCache) => {
            syscache::release_stats_tuple::call(stats_tuple);
        }
        Some(StatsTupleFreeFunc::ReleaseDummy) => release_dummy_stats_tuple(stats_tuple),
        None => { /* freefunc == NULL but a tuple is present: C requires a freefunc. */ }
    }
}

/* ===========================================================================
 * Seam-and-panic shims into genuinely-unported owners.
 * ========================================================================= */

/// `SearchSysCache3(STATRELATTINH, ...)` — the `pg_statistic` catcache probe.
/// Owned by the (ported) syscache unit but not installed yet, so `::call`
/// raises the owner's panic until the catcache wiring lands.
fn search_statrelattinh<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    inherit: bool,
) -> PgResult<Option<StatsTuple>> {
    syscache::search_statrelattinh::call(mcx, relid, attnum, inherit)
}

/// `statext_expressions_load(statOid, inh, pos)` — load the per-expression
/// `pg_statistic` tuple for an extended-statistics expression (unported owner).
fn statext_expressions_load<'mcx>(
    _mcx: Mcx<'mcx>,
    stat_oid: Oid,
    inh: bool,
    pos: usize,
) -> PgResult<Option<StatsTuple>> {
    let _ = (stat_oid, inh, pos);
    panic!(
        "selfuncs: statext_expressions_load is unported — loading a per-expression pg_statistic \
         tuple for an extended-statistics object (statistics/extended_stats.c) has no owner; \
         reached only after an equal() match against an EXPRESSIONS stat"
    )
}

/// C `ReleaseDummy(tuple)` = `pfree(tuple)`; only applied to a copied tuple from
/// `statext_expressions_load`, whose owner is unported (so unreachable).
fn release_dummy_stats_tuple(stats_tuple: StatsTuple) {
    let _ = stats_tuple;
    panic!(
        "selfuncs: ReleaseDummy on a statext_expressions_load tuple is unreachable — its producer \
         is unported, so no ReleaseDummy-tagged statsTuple can be created"
    )
}

/// `getRTEPermissionInfo(root->parse->rteperminfos, rte)->checkAsUser` — the
/// RETURNING-Var user fallback in `all_rows_selectable`.
fn getrte_perminfo_checkasuser<'run>(
    run: &PlannerRun<'run>,
    root: &PlannerInfo,
    perminfoindex: u32,
) -> PgResult<Oid> {
    let parse = run.resolve(root.parse);
    let idx = (perminfoindex as usize)
        .checked_sub(1)
        .expect("getRTEPermissionInfo: invalid perminfoindex 0");
    let perminfo = parse
        .rteperminfos
        .get(idx)
        .expect("getRTEPermissionInfo: perminfoindex out of range");
    Ok(perminfo.checkAsUser)
}

/* ===========================================================================
 * Local helpers.
 * ========================================================================= */

/// Extract the `SortGroupClause` list from a `Query`'s
/// `distinctClause`/`groupClause` (stored as boxed `Node`s in this model) so it
/// can be passed to `targetIsInSortList(&[SortGroupClause])`. C carries these as
/// `List *` of `SortGroupClause` directly; here each entry is a
/// `Node::SortGroupClause`.
fn sort_group_list(
    nodes: &[mcx::PgBox<'_, types_nodes::nodes::Node<'_>>],
) -> alloc::vec::Vec<types_nodes::rawnodes::SortGroupClause> {
    nodes
        .iter()
        .map(|n| match &**n {
            types_nodes::nodes::Node::SortGroupClause(s) => *s,
            other => panic!("expected SortGroupClause in group/distinct clause, got {other:?}"),
        })
        .collect()
}

/// `has_unique_index(rel, attno)` (plancat.c) — is there a single-column unique
/// index on the attribute? Re-implemented over the planner's per-rel
/// `indexlist` (pulling plancat into the selectivity deps would form a cycle).
fn has_unique_index(root: &PlannerInfo, rel: RelId, attno: AttrNumber) -> bool {
    for index in root.rel(rel).indexlist.iter() {
        if index.unique
            && index.nkeycolumns == 1
            && index.indexkeys.first().copied() == Some(attno as i32)
            && (index.indpred.is_empty() || index.predOK)
        {
            return true;
        }
    }
    false
}

/* ===========================================================================
 * Seam bodies.
 * ========================================================================= */

/// Seam body for `examine_variable`.
pub fn seam_examine_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    node: NodeId,
    var_relid: i32,
) -> PgResult<VariableStatData> {
    examine_variable(mcx, run, root, node, var_relid)
}

/// Seam body for `get_restriction_variable`.
pub fn seam_get_restriction_variable<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<Option<(VariableStatData, Expr, bool)>> {
    get_restriction_variable(mcx, run, root, args, var_relid)
}

/// Seam body for `get_join_variables`.
pub fn seam_get_join_variables<'mcx, 'run>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'run>,
    root: &mut PlannerInfo,
    args: &[NodeId],
    sjinfo: &SpecialJoinInfo,
) -> PgResult<(VariableStatData, VariableStatData, bool)> {
    get_join_variables(mcx, run, root, args, sjinfo)
}

/// Seam body for `release_variable_stats`.
pub fn seam_release_variable_stats(vardata: VariableStatData) {
    release_variable_stats(vardata)
}
