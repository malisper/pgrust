//! `backend-optimizer-plan-analyzejoins` — `src/backend/optimizer/plan/analyzejoins.c`.
//!
//! Join-removal optimizations consulted by `query_planner` (planmain.c):
//! left-join removal (`remove_useless_joins`), semijoin → inner-join reduction
//! (`reduce_unique_semijoins`), and self-join elimination
//! (`remove_useless_self_joins`).
//!
//! # The #295 keystone landed here
//!
//! analyzejoins.c casts the *planner* `RestrictInfo *` / `EquivalenceMember *`
//! to `Node *` and runs `ChangeVarNodesExtended(…, replace_relid_callback)` to
//! adjust their relids for a change of RT index. In this repo those are arena
//! handles (`RinfoId` / `EmId`), not `Node` variants, so they are not directly
//! walkable. The [`change_relids`] module reproduces that walker faithfully over
//! the arena handle model (see its docs), and [`types_pathnodes::UniqueRelInfo`]
//! is the carrier the uniqueness cache (`RelOptInfo::unique_for_rels`) needs.
//!
//! # What is built vs seam-and-panic in this pass (#294)
//!
//! Built end to end and installed as seams:
//! * `reduce_unique_semijoins` and its support chain (`innerrel_is_unique[_ext]`,
//!   `is_innerrel_unique_for`, `rel_is_distinct_for`, `rel_supports_distinctness`)
//!   for **both** the `RTE_RELATION` case (proof via unique indexes) **and** the
//!   `RTE_SUBQUERY` case. The subquery distinctness legs
//!   (`query_supports_distinctness` / `query_is_distinct_for` /
//!   `distinct_col_search`, in [`query_distinct`]) are now ported: the
//!   distinctness chain threads `&PlannerRun` and resolves the sub-`Query` off
//!   the rel's RTE (`simple_rte_array[relid]` → `RangeTblEntryId` →
//!   `RangeTblEntry::subquery`).
//! * `innerrel_is_unique` (the joinpath-seams entry the join enumerator calls),
//!   re-signed to carry `&PlannerRun` so the subquery proof is reachable from the
//!   join-search path too.
//! * `remove_useless_joins` (left-join removal, [`remove_joins`]) and its
//!   surgery: `join_is_removable`, `remove_leftjoinrel_from_query`,
//!   `remove_rel_from_query` (the `subst == -1`, `sjinfo != NULL` specialization),
//!   `remove_rel_from_restrictinfo`, `remove_rel_from_eclass`,
//!   `remove_rel_from_joinlist`. The left-join path never walks `root->parse`, so
//!   it is fully portable over the arena handle model.
//!
//! * `remove_useless_self_joins` (self-join elimination, [`remove_joins`]) and
//!   its surgery: `remove_self_joins_recurse`, `remove_self_joins_one_group`,
//!   `remove_self_join_rel`, `split_selfjoin_quals`, `match_unique_clauses`,
//!   `add_non_redundant_clauses`, `restrict_infos_logically_equal`, and
//!   `update_eclasses`. The `ChangeVarNodesExtended((Node *) root->parse, …)`
//!   substitution is reachable because `root->parse` resolves off its `QueryId`
//!   to a real owned `Query` `Node` (via the `PlannerRun` store), walked by
//!   [`change_relids::change_relids_in_query`]; the arena `RestrictInfo` / `EM`
//!   relid walk is [`change_relids`]. The seam is re-signed to take
//!   `&mut PlannerRun` so the parse-tree and row-mark mutations are expressible.
//!
//! `rebuild_joinclause_attr_needed` (initsplan.c:3559) is owned by
//! `backend-optimizer-plan-init-subselect` and now ported+installed there
//! (`targetlist::rebuild_joinclause_attr_needed`); the self-join and left-join
//! surgery both call it via the `backend-optimizer-plan-small-seams` seam.

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinType, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL,
    JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_SEMI, RELOPT_BASEREL, RTE_RELATION,
};

use backend_optimizer_util_relnode as relnode;
use backend_optimizer_path_equivclass as equivclass;
use backend_nodes_core::list as pg_list;

/// Backing storage for the `enable_self_join_elimination` GUC.
///
/// analyzejoins.c declares the GUC's `conf->variable` backing as a plain
/// process-global `bool enable_self_join_elimination;` (analyzejoins.c:53),
/// read directly at plan time in `remove_useless_self_joins` (analyzejoins.c:2493).
/// It is not derived from the ControlFile. We mirror that C global with a
/// process-global `AtomicBool`, seeded to the `boot_val` (`true`) declared in
/// `guc_tables.c`, and expose it to the GUC engine via [`init_seams`].
static ENABLE_SELF_JOIN_ELIMINATION: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(true);

/// Reader mirroring the C global `enable_self_join_elimination`.
pub fn enable_self_join_elimination() -> bool {
    ENABLE_SELF_JOIN_ELIMINATION.load(core::sync::atomic::Ordering::Relaxed)
}

/// Writer the GUC engine calls when `enable_self_join_elimination` is assigned.
pub fn set_enable_self_join_elimination(value: bool) {
    ENABLE_SELF_JOIN_ELIMINATION.store(value, core::sync::atomic::Ordering::Relaxed);
}

pub mod change_relids;
pub mod query_distinct;
pub mod relids;
pub mod remove_joins;

#[cfg(test)]
mod tests;

/// `RTE_SUBQUERY` (`parsenodes.h` `RTEKind`).
const RTE_SUBQUERY: types_pathnodes::RTEKind = 1;

/// `IS_OUTER_JOIN(jointype)` (nodes.h): LEFT/FULL/RIGHT/ANTI/RIGHT_ANTI.
#[inline]
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | JOIN_RIGHT_ANTI
    )
}

/// `RINFO_IS_PUSHED_DOWN(rinfo, joinrelids)` (restrictinfo.h): the clause is
/// "pushed down" relative to a join if it is not a join clause for that join.
#[inline]
pub(crate) fn rinfo_is_pushed_down(
    root: &PlannerInfo,
    rinfo: RinfoId,
    joinrelids: &Relids,
) -> bool {
    let r = root.rinfo(rinfo);
    r.is_pushed_down || !relids::is_subset(&r.required_relids, joinrelids)
}

/* ======================================================================
 * join_is_removable
 * ==================================================================== */

/// `join_is_removable(root, sjinfo)` (analyzejoins.c:154) — can we skip this
/// special join because it just duplicates its left input?
///
/// True for a LEFT join whose condition cannot match more than one inner-side
/// row, provided the inner side produces no variable needed above the join.
pub fn join_is_removable<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<bool> {
    /* Must be a left join to a single baserel. */
    if sjinfo.jointype != JOIN_LEFT {
        return Ok(false);
    }
    let innerrelid = match relids::get_singleton_member(&sjinfo.min_righthand) {
        Some(id) => id,
        None => return Ok(false),
    };

    /*
     * Never try to eliminate a left join to the query result rel. (MERGE builds
     * such a join tree.)
     */
    let result_relation = run.resolve(root.parse).resultRelation;
    if innerrelid == result_relation {
        return Ok(false);
    }

    let innerrel = relnode::find_base_rel(root, innerrelid);

    /*
     * Quick check to eliminate cases where we surely can't prove uniqueness of
     * the innerrel.
     */
    if !rel_supports_distinctness(root, run, innerrel) {
        return Ok(false);
    }

    /* Compute the relid set for the join we are considering. */
    let inputrelids = relids::union(&sjinfo.min_lefthand, &sjinfo.min_righthand);
    debug_assert!(sjinfo.ojrelid != 0);
    let mut joinrelids = relids::copy(&inputrelids);
    joinrelids = relids::add_member(joinrelids, sjinfo.ojrelid as i32);

    /*
     * We can't remove the join if any inner-rel attributes are used above the
     * join (compare to inputrelids, not joinrelids). Count down from max_attr.
     */
    let (min_attr, max_attr) = {
        let r = root.rel(innerrel);
        (r.min_attr, r.max_attr)
    };
    let mut attroff = (max_attr - min_attr) as i32;
    while attroff >= 0 {
        let needed = relids::copy(&root.rel(innerrel).attr_needed[attroff as usize]);
        if !relids::is_subset(&needed, &inputrelids) {
            return Ok(false);
        }
        attroff -= 1;
    }

    /*
     * Similarly check that the inner rel isn't needed by any PlaceHolderVars
     * used above the join.
     */
    let innerrel_relids = relids::copy(&root.rel(innerrel).relids);
    let ph_ids: Vec<types_pathnodes::PhInfoId> = root.placeholder_list.clone();
    for ph_id in ph_ids {
        let (ph_lateral, ph_eval_at, ph_needed) = {
            let phinfo = root.phinfo(ph_id);
            (
                relids::copy(&phinfo.ph_lateral),
                relids::copy(&phinfo.ph_eval_at),
                relids::copy(&phinfo.ph_needed),
            )
        };

        if relids::overlap(&ph_lateral, &innerrel_relids) {
            return Ok(false); /* references innerrel laterally */
        }
        if !relids::overlap(&ph_eval_at, &innerrel_relids) {
            continue; /* definitely doesn't reference innerrel */
        }
        if relids::is_subset(&ph_needed, &inputrelids) {
            continue; /* PHV is not used above the join */
        }
        if !relids::is_member(sjinfo.ojrelid as i32, &ph_eval_at) {
            return Ok(false); /* has to be evaluated below the join */
        }

        /* There must still be a place to evaluate the PHV if we remove the join. */
        if !relids::overlap(&sjinfo.min_lefthand, &ph_eval_at) {
            return Ok(false); /* no other place to eval the PHV */
        }
        /* Check the contained expression last (a bit expensive). */
        let phexpr_id = root.phinfo(ph_id).ph_var_phexpr;
        // Borrow the PHV expr (only inspected by pull_varnos_expr); a derived
        // `.clone()` would panic on a context-allocated child.
        let phexpr: &Expr = root.node(phexpr_id);
        let varnos = backend_optimizer_util_joininfo_ext_seams::pull_varnos_expr::call(root, phexpr);
        if relids::overlap(&varnos, &innerrel_relids) {
            return Ok(false); /* contained expression references innerrel */
        }
    }

    /*
     * Search for mergejoinable clauses that constrain the inner rel against the
     * outer rel or a pseudoconstant.
     */
    let mut clause_list: Vec<RinfoId> = Vec::new();
    let joininfo: Vec<RinfoId> = root.rel(innerrel).joininfo.clone();
    let min_lefthand = relids::copy(&sjinfo.min_lefthand);
    for restrictinfo in joininfo {
        /* Consider only the has_clone form of cloned clauses. */
        if root.rinfo(restrictinfo).is_clone {
            continue;
        }
        /* If it's not a join clause for this outer join, we can't use it. */
        if rinfo_is_pushed_down(root, restrictinfo, &joinrelids) {
            continue;
        }
        /* Ignore if it's not a mergejoinable clause. */
        {
            let r = root.rinfo(restrictinfo);
            if !r.can_join || r.mergeopfamilies.is_empty() {
                continue;
            }
        }
        /* Check "outer op inner" / "inner op outer", marking which side is inner. */
        if !backend_optimizer_path_joinpath_seams::clause_sides_match_join::call(
            root,
            restrictinfo,
            &min_lefthand,
            &innerrel_relids,
        ) {
            continue;
        }
        clause_list.push(restrictinfo);
    }

    /* Try to prove the innerrel distinct for the relevant equality clauses. */
    Ok(rel_is_distinct_for(root, run, innerrel, &clause_list, None))
}

/* ======================================================================
 * rel_supports_distinctness / rel_is_distinct_for
 * ==================================================================== */

/// `rel_supports_distinctness(root, rel)` (analyzejoins.c:919) — could the
/// relation possibly be proven distinct on some set of columns? A cheap
/// pre-check for [`rel_is_distinct_for`].
///
/// Threads `&PlannerRun` (#294) so the `RTE_SUBQUERY` leg can resolve the
/// sub-`Query` off the rel's RTE and call
/// [`query_distinct::query_supports_distinctness`].
pub fn rel_supports_distinctness<'mcx>(
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
) -> bool {
    let r = root.rel(rel);
    /* We only know about baserels ... */
    if r.reloptkind != RELOPT_BASEREL {
        return false;
    }
    if r.rtekind == RTE_RELATION {
        /*
         * For a plain relation, we only know how to prove uniqueness by
         * reference to unique indexes.  Make sure there's at least one
         * suitable unique index. It must be immediately enforced, and not a
         * partial index.
         */
        for ind in &r.indexlist {
            if ind.unique && ind.immediate && ind.indpred.is_empty() {
                return true;
            }
        }
    } else if r.rtekind == RTE_SUBQUERY {
        /*
         * `Query *subquery = root->simple_rte_array[rel->relid]->subquery;`
         * The sub-Query is carried inline on the RTE; resolve the RTE handle
         * through the planner-run store.
         */
        let rte_id = root.simple_rte_array[r.relid as usize];
        let rte = run.resolve_rte(rte_id);
        if let Some(subquery) = rte.subquery.as_deref() {
            if query_distinct::query_supports_distinctness(subquery) {
                return true;
            }
        }
    }
    /* We have no proof rules for any other rtekinds. */
    false
}

/// The `RTE_SUBQUERY` distinctness probe from `create_unique_path`
/// (pathnode.c:1959): for a subquery rel whose `semi_rhs_exprs` (`uniq_exprs`)
/// are simple Vars referencing subquery outputs, is the subquery already
/// guaranteed distinct over those columns under `in_operators`?
///
/// `query_supports_distinctness(rte->subquery)` &&
/// `translate_sub_tlist(uniq_exprs, rel->relid)` non-NIL &&
/// `query_is_distinct_for(rte->subquery, colnos, in_operators)`. `translate_sub_tlist`
/// (a pathnode.c static) is inlined: each `uniq_expr` must be a `Var` with
/// `varno == rel->relid`; otherwise punt (NIL → false).
pub fn subquery_is_distinct_for<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rel: RelId,
    uniq_exprs: &[types_pathnodes::NodeId],
    in_operators: &[types_core::primitive::Oid],
) -> bool {
    let relid = root.rel(rel).relid;

    let rte_id = root.simple_rte_array[relid as usize];
    let rte = run.resolve_rte(rte_id);
    let subquery = match rte.subquery.as_deref() {
        Some(q) => q,
        None => return false,
    };

    if !query_distinct::query_supports_distinctness(subquery) {
        return false;
    }

    // translate_sub_tlist(uniq_exprs, relid): each element must be a Var of this
    // rel; build the list of output column numbers, else punt.
    let mut colnos: Vec<i32> = Vec::new();
    for &nid in uniq_exprs {
        match root.node(nid) {
            Expr::Var(v) if v.varno == relid as i32 => colnos.push(v.varattno as i32),
            _ => return false, /* punt */
        }
    }
    if colnos.is_empty() {
        return false;
    }

    query_distinct::query_is_distinct_for(subquery, &colnos, in_operators)
}

/// `rel_is_distinct_for(root, rel, clause_list, extra_clauses)` (analyzejoins.c:980)
/// — does the relation return only distinct rows according to `clause_list`?
///
/// `clause_list` is the list of mergejoinable equality join clauses (the caller
/// has already verified each is an equality with an expression in this rel on one
/// side). `extra_clauses` (when `Some`) is set to the baserestrictinfo clauses
/// used to prove uniqueness (used by the self-join checker).
///
/// Threads `&PlannerRun` (#294) for the `RTE_SUBQUERY` leg.
pub fn rel_is_distinct_for<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    clause_list: &[RinfoId],
    extra_clauses: Option<&mut Vec<RinfoId>>,
) -> bool {
    let (reloptkind, rtekind, relid) = {
        let r = root.rel(rel);
        (r.reloptkind, r.rtekind, r.relid)
    };
    if reloptkind != RELOPT_BASEREL {
        return false;
    }
    if rtekind == RTE_RELATION {
        /*
         * Examine the indexes to see if we have a matching unique index.
         * relation_has_unique_index_ext automatically adds any usable
         * restriction clauses for the rel, so we needn't do that here.
         */
        backend_optimizer_path_indxpath::relation_has_unique_index_ext(
            root,
            rel,
            clause_list,
            &[],
            &[],
            extra_clauses,
        )
    } else if rtekind == RTE_SUBQUERY {
        /*
         * Build the argument lists for query_is_distinct_for: the output column
         * numbers the query must be distinct over and the equality operators.
         * (XXX we are not considering restriction clauses attached to the
         * subquery.)
         */
        let mut colnos: Vec<i32> = Vec::new();
        let mut opids: Vec<types_core::primitive::Oid> = Vec::new();

        for &ri in clause_list {
            let r = root.rinfo(ri);
            /*
             * Get the equality operator we need uniqueness according to. The
             * caller's mergejoinability test should have selected only OpExprs.
             */
            // Borrow the clause (only inspected here); a derived `.clone()`
            // would panic on a context-allocated child.
            let clause_expr: &Expr = root.node(r.clause);
            let op = match clause_expr {
                Expr::OpExpr(op) => op.opno,
                other => panic!("rel_is_distinct_for: clause is not an OpExpr: {other:?}"),
            };

            /* caller identified the inner side for us */
            let outer_is_left = r.outer_is_left;
            let var: Option<&Expr> = match clause_expr {
                Expr::OpExpr(op) => {
                    if outer_is_left {
                        op.args.get(1) /* get_rightop */
                    } else {
                        op.args.first() /* get_leftop */
                    }
                }
                _ => None,
            };

            /*
             * We may ignore any RelabelType node above the operand (there won't
             * be more than one after eval_const_expressions).
             */
            let var: Option<&Expr> = match var {
                Some(Expr::RelabelType(rt)) => rt.arg.as_deref(),
                other => other,
            };

            /*
             * If inner side isn't a Var referencing a subquery output column,
             * this clause doesn't help us.
             */
            match var {
                Some(Expr::Var(v)) if v.varno == relid as i32 && v.varlevelsup == 0 => {
                    colnos.push(v.varattno as i32);
                    opids.push(op);
                }
                _ => continue,
            }
        }

        let rte_id = root.simple_rte_array[relid as usize];
        let rte = run.resolve_rte(rte_id);
        match rte.subquery.as_deref() {
            Some(subquery) => query_distinct::query_is_distinct_for(subquery, &colnos, &opids),
            None => false,
        }
    } else {
        false
    }
}

/* ======================================================================
 * innerrel_is_unique family
 * ==================================================================== */

/// `is_innerrel_unique_for(root, joinrelids, outerrelids, innerrel, jointype,
/// restrictlist, extra_clauses)` (analyzejoins.c:1457) — the actual uniqueness
/// proof. Selects mergejoinable clauses constraining the inner rel against the
/// outer rel, then defers to [`rel_is_distinct_for`].
fn is_innerrel_unique_for<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids,
    outerrelids: &Relids,
    innerrel: RelId,
    jointype: JoinType,
    restrictlist: &[RinfoId],
    extra_clauses: Option<&mut Vec<RinfoId>>,
) -> bool {
    let innerrel_relids = root.rel(innerrel).relids.clone();
    let mut clause_list: Vec<RinfoId> = Vec::new();

    /*
     * Search for mergejoinable clauses that constrain the inner rel against the
     * outer rel.
     */
    for &restrictinfo in restrictlist {
        /*
         * If it's a pushed-down clause and we're at an outer join, we can't use
         * it.
         */
        if is_outer_join(jointype) && rinfo_is_pushed_down(root, restrictinfo, joinrelids) {
            continue;
        }

        /* Ignore if it's not a mergejoinable clause */
        {
            let r = root.rinfo(restrictinfo);
            if !r.can_join || r.mergeopfamilies.is_empty() {
                continue; /* not mergejoinable */
            }
        }

        /*
         * Check if the clause has the form "outer op inner" or "inner op outer",
         * and if so mark which side is inner. (Sets rinfo->outer_is_left.)
         */
        if !backend_optimizer_path_joinpath_seams::clause_sides_match_join::call(
            root,
            restrictinfo,
            outerrelids,
            &innerrel_relids,
        ) {
            continue; /* no good for these input relations */
        }

        clause_list.push(restrictinfo);
    }

    /* Let rel_is_distinct_for() do the hard work */
    rel_is_distinct_for(root, run, innerrel, &clause_list, extra_clauses)
}

/// `innerrel_is_unique_ext(...)` (analyzejoins.c:1328). Caches answers in
/// `innerrel.unique_for_rels` / `non_unique_for_rels`.
///
/// `self_join` is `extra_clauses.is_some()` in C; here it is passed explicitly
/// alongside the out-param to keep the borrow shape clean. When `self_join` is
/// true the returned proof clauses are written into `out_extra_clauses`.
pub fn innerrel_is_unique_ext<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids,
    outerrelids: &Relids,
    innerrel: RelId,
    jointype: JoinType,
    restrictlist: &[RinfoId],
    force_cache: bool,
    self_join: bool,
    out_extra_clauses: Option<&mut Vec<RinfoId>>,
) -> bool {
    /* Certainly can't prove uniqueness when there are no joinclauses */
    if restrictlist.is_empty() {
        return false;
    }

    /*
     * Make a quick check to eliminate cases in which we will surely be unable to
     * prove uniqueness of the innerrel.
     */
    if !rel_supports_distinctness(root, run, innerrel) {
        return false;
    }

    /*
     * Query the cache to see if we've already proven (or disproven) uniqueness
     * for this outerrel (or a relevant sub/superset).
     */
    {
        let rel = root.rel(innerrel);
        for u in &rel.unique_for_rels {
            if (!self_join && relids::is_subset(&u.outerrelids, outerrelids))
                || (self_join && relids::equal(&u.outerrelids, outerrelids) && u.self_join)
            {
                if let Some(out) = out_extra_clauses {
                    *out = u.extra_clauses.clone();
                }
                return true; /* Success! */
            }
        }
        for nu in &rel.non_unique_for_rels {
            if relids::is_subset(outerrelids, nu) {
                return false;
            }
        }
    }

    /* No cached information, so try to make the proof. */
    let mut outer_exprs: Vec<RinfoId> = Vec::new();
    let proved = is_innerrel_unique_for(
        root,
        run,
        joinrelids,
        outerrelids,
        innerrel,
        jointype,
        restrictlist,
        if self_join {
            Some(&mut outer_exprs)
        } else {
            None
        },
    );

    if proved {
        /* Cache the positive result for future probes. */
        let uri = types_pathnodes::UniqueRelInfo {
            outerrelids: relids::copy(outerrelids),
            self_join,
            extra_clauses: outer_exprs.clone(),
        };
        root.rel_mut(innerrel).unique_for_rels.push(uri);

        if let Some(out) = out_extra_clauses {
            *out = outer_exprs;
        }
        true /* Success! */
    } else {
        /*
         * None of the join conditions for outerrel proved innerrel unique. In
         * normal planning mode caching this is pointless (joinrels are built
         * smaller-to-larger), so we only cache when forced or in GEQO /
         * join-search-plugin mode (root.join_search_private != NULL).
         */
        if force_cache || join_search_private_is_set(root) {
            root.rel_mut(innerrel)
                .non_unique_for_rels
                .push(relids::copy(outerrelids));
        }
        false
    }
}

/// `root->join_search_private != NULL` — true in GEQO / join-search-plugin mode.
/// The planner mirror does not carry that opaque pointer; in normal bottom-up
/// planning it is always NULL, so this returns false (the behaviour-preserving
/// value for `reduce_unique_semijoins`, which always passes `force_cache=true`).
#[inline]
fn join_search_private_is_set(_root: &PlannerInfo) -> bool {
    false
}

/// `innerrel_is_unique(...)` (analyzejoins.c:1306) — the cached-frontend the join
/// enumerator (joinpath.c) calls. Installed as the `innerrel_is_unique` seam.
pub fn innerrel_is_unique<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids,
    outerrelids: &Relids,
    innerrel: RelId,
    jointype: JoinType,
    restrictlist: &[RinfoId],
    force_cache: bool,
) -> bool {
    innerrel_is_unique_ext(
        root,
        run,
        joinrelids,
        outerrelids,
        innerrel,
        jointype,
        restrictlist,
        force_cache,
        false,
        None,
    )
}

/* ======================================================================
 * reduce_unique_semijoins
 * ==================================================================== */

/// `reduce_unique_semijoins(root)` (analyzejoins.c:844) — reduce semijoins whose
/// inner rel is provably unique for the join clauses to plain inner joins, by
/// deleting their `SpecialJoinInfo` from `root.join_info_list`.
pub fn reduce_unique_semijoins<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>) -> PgResult<()> {
    /*
     * Scan join_info_list for semijoins. We collect the indices to delete after
     * the scan, since proving uniqueness re-borrows root mutably.  (C uses
     * foreach_delete_current; deletion does not affect the indices of the
     * not-yet-visited entries because we delete from the tail when reindexing,
     * so we record absolute SpecialJoinInfo identities by value-copy of the
     * needed fields, mirroring the per-iteration snapshot.)
     */
    let mut to_delete: Vec<usize> = Vec::new();

    let n = root.join_info_list.len();
    for i in 0..n {
        let (jointype, min_lefthand, min_righthand) = {
            let sj = &root.join_info_list[i];
            (
                sj.jointype,
                sj.min_lefthand.clone(),
                sj.min_righthand.clone(),
            )
        };

        /* Must be a semijoin to a single baserel. */
        if jointype != JOIN_SEMI {
            continue;
        }
        let innerrelid = match relids::get_singleton_member(&min_righthand) {
            Some(id) => id,
            None => continue,
        };

        let innerrel = relnode::find_base_rel(root, innerrelid);

        /*
         * Before we trouble to run generate_join_implied_equalities, make a quick
         * check to eliminate cases in which we will surely be unable to prove
         * uniqueness of the innerrel.
         */
        if !rel_supports_distinctness(root, run, innerrel) {
            continue;
        }

        /* Compute the relid set for the join we are considering. */
        let joinrelids = relids::union(&min_lefthand, &min_righthand);

        /*
         * Since we're only considering a single-rel RHS, any join clauses it has
         * must link it to the semijoin's min_lefthand. We can also consider
         * EC-derived join clauses.
         */
        let ec_clauses = equivclass::generate_join_implied_equalities(
            root,
            run,
            relids::copy(&joinrelids),
            relids::copy(&min_lefthand),
            innerrel,
            None,
        )?;
        let mut restrictlist = ec_clauses;
        restrictlist.extend_from_slice(&root.rel(innerrel).joininfo);

        /* Test whether the innerrel is unique for those clauses. */
        if !innerrel_is_unique(
            root,
            run,
            &joinrelids,
            &min_lefthand,
            innerrel,
            JOIN_SEMI,
            &restrictlist,
            true,
        ) {
            continue;
        }

        /* OK, mark the SpecialJoinInfo for removal. */
        to_delete.push(i);
    }

    /* Delete the marked entries (high-to-low to keep indices valid). */
    for &i in to_delete.iter().rev() {
        root.join_info_list.remove(i);
    }
    Ok(())
}

// Keep `pg_list` referenced (list_concat parity is expressed via Vec::extend
// above; the import documents the analyzejoins.c list_concat call site).
#[allow(unused_imports)]
use pg_list as _pg_list_doc;

/* ======================================================================
 * Seam installation (inward seams owned by analyzejoins.c)
 * ==================================================================== */

/// Install the analyzejoins seams this crate backs. Wired into
/// `seams-init::init_all()`.
///
/// Installed: `reduce_unique_semijoins` + `remove_useless_joins` (planmain.c
/// upcalls) and `innerrel_is_unique` (joinpath.c upcall). NOT installed here:
/// `remove_useless_self_joins` — genuinely carrier-blocked on a `root->parse`
/// Query-tree relid walker (see crate docs); left at its panicking default.
pub fn init_seams() {
    backend_optimizer_plan_init_subselect_seams::reduce_unique_semijoins::set(
        |root, run| {
            // The seam is `void`; analyzejoins.c's reduce_unique_semijoins cannot
            // ereport on this path. The EC-implied-equalities generation returns
            // a PgResult; an error there is a planner bug, so surface it.
            reduce_unique_semijoins(root, run).expect("reduce_unique_semijoins")
        },
    );

    backend_optimizer_plan_init_subselect_seams::remove_useless_joins::set(
        |root, run, joinlist| {
            remove_joins::remove_useless_joins(root, run, joinlist)
                .expect("remove_useless_joins")
        },
    );

    // remove_useless_self_joins: full self-join elimination including the
    // `root->parse` relid substitution. The seam carries `&mut PlannerRun`.
    backend_optimizer_plan_init_subselect_seams::remove_useless_self_joins::set(
        |root, run, joinlist| remove_joins::remove_useless_self_joins(root, run, joinlist),
    );

    backend_optimizer_path_joinpath_seams::innerrel_is_unique::set(
        |root, run, joinrelids, outerrelids, innerrel, jointype, restrictlist, force_cache| {
            innerrel_is_unique(
                root,
                run,
                joinrelids,
                outerrelids,
                innerrel,
                jointype,
                restrictlist,
                force_cache,
            )
        },
    );

    // pathnode.c `create_unique_path`'s RTE_SUBQUERY distinctness probe.
    backend_optimizer_util_pathnode_seams::subquery_is_distinct_for::set(
        |run, root, rel, uniq_exprs, in_operators| {
            subquery_is_distinct_for(run, root, rel, uniq_exprs, in_operators)
        },
    );

    // Expose the `enable_self_join_elimination` GUC's backing storage to the
    // GUC engine. analyzejoins.c owns the `conf->variable` for this GUC (the
    // file-scope `bool enable_self_join_elimination;`, analyzejoins.c:53); the
    // engine reads/writes it through these accessors.
    backend_utils_misc_guc_tables::vars::enable_self_join_elimination.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: enable_self_join_elimination,
            set: set_enable_self_join_elimination,
        },
    );
}
