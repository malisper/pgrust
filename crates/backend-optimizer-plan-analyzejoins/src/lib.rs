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
//! # What is built vs seam-and-panic in this pass
//!
//! Built end to end and installed as seams:
//! * `reduce_unique_semijoins` and its support chain (`innerrel_is_unique[_ext]`,
//!   `is_innerrel_unique_for`, `rel_is_distinct_for`, `rel_supports_distinctness`)
//!   for the `RTE_RELATION` case (proof via unique indexes), plus
//!   `innerrel_is_unique` (the joinpath-seams entry the join enumerator calls).
//!
//! Still seam-and-panic (left as the precise follow-up decomp — see the crate's
//! task report):
//! * `remove_useless_joins` / `remove_useless_self_joins` — the heavy in-place
//!   PlannerInfo surgery legs (PHV array / commute sets / EC fixup / joinlist
//!   rebuild + the two absent initsplan `rebuild_{joinclause,lateral}_attr_needed`
//!   seams). These remain uninstalled in `backend-optimizer-plan-init-subselect-seams`
//!   and panic loudly until filled — mirror-pg-and-panic.
//! * The `RTE_SUBQUERY` distinctness legs (`query_supports_distinctness` /
//!   `query_is_distinct_for`) — blocked on the planner `Query` carrier, which is
//!   trimmed (no `distinctClause`/`groupClause`/`groupingSets`/`havingQual`/
//!   `setOperations`/`targetList`). They panic via [`subquery_distinctness_unported`].

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_pathnodes::{
    JoinType, PlannerInfo, RelId, Relids, RinfoId, JOIN_ANTI, JOIN_FULL, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_SEMI, RELOPT_BASEREL, RTE_RELATION,
};

use backend_optimizer_util_relnode as relnode;
use backend_optimizer_path_equivclass as equivclass;
use backend_nodes_core::list as pg_list;

pub mod change_relids;
pub mod relids;

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
fn rinfo_is_pushed_down(root: &PlannerInfo, rinfo: RinfoId, joinrelids: &Relids) -> bool {
    let r = root.rinfo(rinfo);
    r.is_pushed_down || !relids::is_subset(&r.required_relids, joinrelids)
}

/// Diverging panic for the still-unported `RTE_SUBQUERY` distinctness legs
/// (`query_supports_distinctness` / `query_is_distinct_for`). The planner
/// `Query` carrier is trimmed of the clauses these need; this mirror-pg-and-panic
/// marks the exact boundary rather than silently returning a wrong answer.
fn subquery_distinctness_unported() -> ! {
    panic!(
        "analyzejoins: RTE_SUBQUERY distinctness (query_supports_distinctness / \
         query_is_distinct_for) needs the full planner Query carrier \
         (distinctClause/groupClause/groupingSets/havingQual/setOperations/targetList), \
         which is not yet modeled"
    )
}

/* ======================================================================
 * rel_supports_distinctness / rel_is_distinct_for
 * ==================================================================== */

/// `rel_supports_distinctness(root, rel)` (analyzejoins.c:919) — could the
/// relation possibly be proven distinct on some set of columns? A cheap
/// pre-check for [`rel_is_distinct_for`].
pub fn rel_supports_distinctness(root: &PlannerInfo, rel: RelId) -> bool {
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
        /* query_supports_distinctness(subquery) — carrier-blocked. */
        subquery_distinctness_unported();
    }
    /* We have no proof rules for any other rtekinds. */
    false
}

/// `rel_is_distinct_for(root, rel, clause_list, extra_clauses)` (analyzejoins.c:980)
/// — does the relation return only distinct rows according to `clause_list`?
///
/// `clause_list` is the list of mergejoinable equality join clauses (the caller
/// has already verified each is an equality with an expression in this rel on one
/// side). `extra_clauses` (when `Some`) is set to the baserestrictinfo clauses
/// used to prove uniqueness (used by the self-join checker).
pub fn rel_is_distinct_for(
    root: &mut PlannerInfo,
    rel: RelId,
    clause_list: &[RinfoId],
    extra_clauses: Option<&mut Vec<RinfoId>>,
) -> bool {
    let (reloptkind, rtekind) = {
        let r = root.rel(rel);
        (r.reloptkind, r.rtekind)
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
        /* query_is_distinct_for(subquery, colnos, opids) — carrier-blocked. */
        subquery_distinctness_unported();
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
fn is_innerrel_unique_for(
    root: &mut PlannerInfo,
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
    rel_is_distinct_for(root, innerrel, &clause_list, extra_clauses)
}

/// `innerrel_is_unique_ext(...)` (analyzejoins.c:1328). Caches answers in
/// `innerrel.unique_for_rels` / `non_unique_for_rels`.
///
/// `self_join` is `extra_clauses.is_some()` in C; here it is passed explicitly
/// alongside the out-param to keep the borrow shape clean. When `self_join` is
/// true the returned proof clauses are written into `out_extra_clauses`.
pub fn innerrel_is_unique_ext(
    root: &mut PlannerInfo,
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
    if !rel_supports_distinctness(root, innerrel) {
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
pub fn innerrel_is_unique(
    root: &mut PlannerInfo,
    joinrelids: &Relids,
    outerrelids: &Relids,
    innerrel: RelId,
    jointype: JoinType,
    restrictlist: &[RinfoId],
    force_cache: bool,
) -> bool {
    innerrel_is_unique_ext(
        root,
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
pub fn reduce_unique_semijoins(root: &mut PlannerInfo) -> PgResult<()> {
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
        if !rel_supports_distinctness(root, innerrel) {
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

/// Install the analyzejoins seams this crate currently backs. Wired into
/// `seams-init::init_all()`.
///
/// Installed: `reduce_unique_semijoins` (planmain.c upcall) and
/// `innerrel_is_unique` (joinpath.c upcall). NOT installed here:
/// `remove_useless_joins` / `remove_useless_self_joins` — still seam-and-panic
/// pending the heavy in-place-surgery follow-up (see crate docs).
pub fn init_seams() {
    backend_optimizer_plan_init_subselect_seams::reduce_unique_semijoins::set(
        |root| {
            // The seam is `void`; analyzejoins.c's reduce_unique_semijoins cannot
            // ereport on this path. The EC-implied-equalities generation returns
            // a PgResult; an error there is a planner bug, so surface it.
            reduce_unique_semijoins(root).expect("reduce_unique_semijoins")
        },
    );

    backend_optimizer_path_joinpath_seams::innerrel_is_unique::set(
        |root, joinrelids, outerrelids, innerrel, jointype, restrictlist, force_cache| {
            innerrel_is_unique(
                root,
                joinrelids,
                outerrelids,
                innerrel,
                jointype,
                restrictlist,
                force_cache,
            )
        },
    );
}
