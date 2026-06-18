//! `remove_useless_joins` and its supporting surgery (analyzejoins.c:89-827) —
//! left-join removal.
//!
//! When a left join's condition cannot match more than one inner-side row and no
//! inner-side variable is needed above the join, the join just duplicates its
//! left input and can be removed. `remove_useless_joins` scans
//! `root->join_info_list` for such joins, proves removability via
//! [`crate::join_is_removable`], and rips the inner rel out of every planner data
//! structure (`remove_leftjoinrel_from_query`).
//!
//! # Carrier note (vs the self-join leg)
//!
//! Left-join removal calls `remove_rel_from_query` with `subst == -1` and a
//! non-NULL `sjinfo`. On that path C never runs
//! `ChangeVarNodesExtended((Node *) root->parse, …)` — only the per-PHV
//! `phexpr` walk and the EC `remove_rel_from_restrictinfo` cleanups, both of
//! which operate over arena `Expr`/`RestrictInfo` handles this repo *can* walk
//! ([`crate::change_relids`]). So the whole left-join leg is portable. The
//! `subst > 0` self-join path (which walks the entire `root->parse` `Query` tree
//! for a relid change) needs a `Query`-tree relid walker the planner model does
//! not yet carry, so `remove_useless_self_joins` stays seam-and-panic.

use alloc::vec::Vec;

use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{JoinlistNode, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};

use backend_optimizer_util_relnode as relnode;
use backend_optimizer_rte_seams as rte;
use backend_optimizer_util_plancat_ext_seams as px;

use crate::change_relids::{change_relids_in_node, ReplaceRelidContext};
use crate::relids;
use crate::rinfo_is_pushed_down;

/// `remove_useless_joins(root, joinlist)` (analyzejoins.c:89) — check for
/// relations that don't actually need to be joined, and remove them.
pub fn remove_useless_joins<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    mut joinlist: Vec<JoinlistNode>,
) -> PgResult<Vec<JoinlistNode>> {
    /*
     * We are only interested in relations that are left-joined to, so we scan
     * the join_info_list to find them easily. The C code restarts the whole
     * foreach on every removal (a `goto restart`) because removing attr_needed
     * bits may make a previously-unremovable join removable; we mirror that with
     * an outer loop that restarts the scan after each removal.
     */
    'restart: loop {
        let n = root.join_info_list.len();
        for i in 0..n {
            let sjinfo = root.join_info_list[i].clone();

            /* Skip if not removable */
            if !crate::join_is_removable(root, run, &sjinfo)? {
                continue;
            }

            /*
             * Currently join_is_removable can only succeed when the sjinfo's
             * righthand is a single baserel. Remove that rel from the query and
             * joinlist.
             */
            let innerrelid = relids::get_singleton_member(&sjinfo.min_righthand)
                .expect("join_is_removable guarantees a singleton min_righthand");

            remove_leftjoinrel_from_query(root, run, innerrelid, &sjinfo)?;

            /* Verify that exactly one reference gets removed from joinlist */
            let mut nremoved = 0;
            joinlist = remove_rel_from_joinlist(joinlist, innerrelid, &mut nremoved);
            if nremoved != 1 {
                panic!("failed to find relation {innerrelid} in joinlist");
            }

            /*
             * Delete this SpecialJoinInfo from the list too — it's no longer of
             * interest. (We restart the scan immediately, so a plain index
             * removal is fine.)
             */
            root.join_info_list.remove(i);

            /* Restart the scan. */
            continue 'restart;
        }
        break;
    }

    Ok(joinlist)
}

/// `remove_rel_from_query(root, rel, subst=-1, sjinfo, joinrelids)`
/// (analyzejoins.c:324) — the LEFT-JOIN-removal specialization (`subst < 0`,
/// `sjinfo != NULL`). Updates only the parts needed for left-join removal.
fn remove_rel_from_query<'mcx>(
    root: &mut PlannerInfo,
    _run: &PlannerRun<'mcx>,
    rel: RelId,
    sjinfo: &SpecialJoinInfo,
    joinrelids: &Relids,
) -> PgResult<()> {
    let relid = root.rel(rel).relid as i32;
    let subst = -1; /* left-join removal: delete only */
    let ojrelid = sjinfo.ojrelid as i32;

    /* Update all_baserels and related relid sets. */
    root.all_baserels = relids::adjust_relid_set(&root.all_baserels, relid, subst);
    root.all_query_rels = relids::adjust_relid_set(&root.all_query_rels, relid, subst);

    root.outer_join_rels = relids::del_member(relids::copy(&root.outer_join_rels), ojrelid);
    root.all_query_rels = relids::del_member(relids::copy(&root.all_query_rels), ojrelid);

    /*
     * Likewise remove references from SpecialJoinInfo data structures. Upper
     * joins' relid sets have to be adjusted. (We always make a private copy of
     * the relid sets before modifying, as initsplan.c shares them.)
     */
    for idx in 0..root.join_info_list.len() {
        let sjinf = &mut root.join_info_list[idx];
        sjinf.min_lefthand = relids::adjust_relid_set(
            &relids::copy(&sjinf.min_lefthand),
            relid,
            subst,
        );
        sjinf.min_righthand =
            relids::adjust_relid_set(&relids::copy(&sjinf.min_righthand), relid, subst);
        sjinf.syn_lefthand =
            relids::adjust_relid_set(&relids::copy(&sjinf.syn_lefthand), relid, subst);
        sjinf.syn_righthand =
            relids::adjust_relid_set(&relids::copy(&sjinf.syn_righthand), relid, subst);

        /* Remove sjinfo->ojrelid bits from the sets (sjinfo != NULL path). */
        sjinf.min_lefthand = relids::del_member(sjinf.min_lefthand.take(), ojrelid);
        sjinf.min_righthand = relids::del_member(sjinf.min_righthand.take(), ojrelid);
        sjinf.syn_lefthand = relids::del_member(sjinf.syn_lefthand.take(), ojrelid);
        sjinf.syn_righthand = relids::del_member(sjinf.syn_righthand.take(), ojrelid);
        /* relid cannot appear in these fields, but ojrelid can: */
        sjinf.commute_above_l = relids::del_member(sjinf.commute_above_l.take(), ojrelid);
        sjinf.commute_above_r = relids::del_member(sjinf.commute_above_r.take(), ojrelid);
        sjinf.commute_below_l = relids::del_member(sjinf.commute_below_l.take(), ojrelid);
        sjinf.commute_below_r = relids::del_member(sjinf.commute_below_r.take(), ojrelid);
    }

    /*
     * Likewise remove references from PlaceHolderVar data structures, removing
     * any no-longer-needed placeholders entirely (left-join removal only).
     */
    let ph_ids: Vec<types_pathnodes::PhInfoId> = root.placeholder_list.clone();
    let mut kept: Vec<types_pathnodes::PhInfoId> = Vec::with_capacity(ph_ids.len());
    for ph_id in ph_ids {
        let (ph_needed, ph_eval_at, phid) = {
            let phinfo = root.phinfo(ph_id);
            (
                relids::copy(&phinfo.ph_needed),
                relids::copy(&phinfo.ph_eval_at),
                phinfo.phid,
            )
        };

        if relids::is_subset(&ph_needed, joinrelids)
            && relids::is_member(relid, &ph_eval_at)
            && !relids::is_member(ojrelid, &ph_eval_at)
        {
            // Remove the PHV (foreach_delete_current + null the array slot).
            root.placeholder_array[phid as usize] = None;
            // (Drop from placeholder_list by not keeping it.)
            continue;
        }

        // Otherwise, update the PHV.
        {
            let phinfo = root.phinfo_mut(ph_id);
            phinfo.ph_eval_at = relids::adjust_relid_set(&phinfo.ph_eval_at, relid, subst);
            phinfo.ph_eval_at = relids::adjust_relid_set(&phinfo.ph_eval_at, ojrelid, subst);
            debug_assert!(!relids::is_empty(&phinfo.ph_eval_at));
            /* Reduce ph_needed to contain only "relation 0". */
            if relids::is_member(0, &phinfo.ph_needed) {
                phinfo.ph_needed = relids::make_singleton(0);
            } else {
                phinfo.ph_needed = None;
            }
            phinfo.ph_lateral = relids::adjust_relid_set(&phinfo.ph_lateral, relid, subst);
            /* ph_lateral might overlap ph_eval_at after replacement; remove. */
            phinfo.ph_lateral = relids::difference(&phinfo.ph_lateral, &phinfo.ph_eval_at);

            /* phv->phrels (the carrier mirror + the inline PlaceHolderVar). */
            phinfo.ph_var_phrels =
                relids::adjust_relid_set(&phinfo.ph_var_phrels, relid, subst);
            phinfo.ph_var_phrels =
                relids::adjust_relid_set(&phinfo.ph_var_phrels, ojrelid, subst);
            phinfo.ph_var.phrels =
                adjust_expr_relids(&phinfo.ph_var.phrels, relid, subst);
            phinfo.ph_var.phrels =
                adjust_expr_relids(&phinfo.ph_var.phrels, ojrelid, subst);
            debug_assert!(!phinfo.ph_var.phrels.words.iter().all(|&w| w == 0));
        }

        /* ChangeVarNodesExtended((Node *) phv->phexpr, relid, subst, …). */
        let phexpr_id = root.phinfo(ph_id).ph_var_phexpr;
        change_relids_in_node(
            root,
            phexpr_id,
            ReplaceRelidContext {
                rt_index: relid,
                new_index: subst,
            },
        );
        kept.push(ph_id);
    }
    root.placeholder_list = kept;

    /*
     * Likewise remove references from EquivalenceClasses.
     */
    let n_ecs = root.eq_classes.len();
    for ec_i in 0..n_ecs {
        let ec_id = types_pathnodes::EcId(ec_i as u32);
        let touched = {
            let ec = root.ec(ec_id);
            relids::is_member(relid, &ec.ec_relids)
                || relids::is_member(ojrelid, &ec.ec_relids)
        };
        // C: `if (member(relid) || (sjinfo==NULL || member(ojrelid)))`. With
        // sjinfo != NULL this is `member(relid) || member(ojrelid)`.
        if touched {
            remove_rel_from_eclass(root, ec_id, sjinfo, relid, subst);
        }
    }

    /*
     * Recompute per-Var attr_needed (and lateral_vars) relid sets. Strip all
     * bits other than "relation 0" from attr_needed sets (ph_needed handled
     * above). subst < 0, so no lateral_vars ChangeVarNodes.
     */
    for rti in 1..root.simple_rel_array_size {
        let otherrel = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };
        let (min_attr, max_attr) = {
            let r = root.rel(otherrel);
            (r.min_attr, r.max_attr)
        };
        let mut attroff = (max_attr - min_attr) as i32;
        while attroff >= 0 {
            let r = root.rel_mut(otherrel);
            let cur = &r.attr_needed[attroff as usize];
            if relids::is_member(0, cur) {
                r.attr_needed[attroff as usize] = relids::make_singleton(0);
            } else {
                r.attr_needed[attroff as usize] = None;
            }
            attroff -= 1;
        }
    }
    Ok(())
}

/// `remove_leftjoinrel_from_query(root, relid, sjinfo)` (analyzejoins.c:543).
fn remove_leftjoinrel_from_query<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    relid: i32,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<()> {
    let rel = relnode::find_base_rel(root, relid);
    let ojrelid = sjinfo.ojrelid as i32;

    /* Compute the relid set for the join we are considering. */
    let mut joinrelids = relids::union(&sjinfo.min_lefthand, &sjinfo.min_righthand);
    joinrelids = relids::add_member(joinrelids, ojrelid);

    remove_rel_from_query(root, run, rel, sjinfo, &joinrelids)?;

    /*
     * Remove any joinquals referencing the rel from the joininfo lists. Add the
     * relids of all OJs commutable with this one to the pushed-down test set so
     * we get rid of clones too.
     */
    let mut join_plus_commute = relids::union(&joinrelids, &sjinfo.commute_above_r);
    join_plus_commute = relids::add_members(join_plus_commute, &sjinfo.commute_below_l);

    /*
     * Copy the rel's old joininfo before the loop — remove_join_clause_from_rels
     * would otherwise destroy the list while we scan it.
     */
    let joininfos: Vec<RinfoId> = root.rel(rel).joininfo.clone();
    for rinfo in joininfos {
        let required_relids = relids::copy(&root.rinfo(rinfo).required_relids);
        backend_optimizer_util_joininfo::remove_join_clause_from_rels(
            run,
            root,
            rinfo,
            &required_relids,
        );

        if rinfo_is_pushed_down(root, rinfo, &join_plus_commute) {
            /*
             * Drop references to relid or ojrelid in the RestrictInfo's relid
             * sets (we already verified any such PHV is safe).
             */
            remove_rel_from_restrictinfo(root, rinfo, relid, ojrelid);

            /* Throw it back into the joininfo lists. */
            backend_optimizer_path_equivclass_ext_seams::distribute_restrictinfo_to_rels::call(
                run, root, rinfo,
            )?;
        }
    }

    /*
     * Now remove the rel from the baserel array to prevent further references.
     */
    root.simple_rel_array[relid as usize] = None;
    // (simple_rte_array slot left in place: it is a handle into the run store;
    // C nulls the `RangeTblEntry *` pointer. The slot is never re-read for a
    // removed baserel because simple_rel_array[relid] is now None.)

    /*
     * Repeat construction of attr_needed bits coming from all other sources.
     */
    backend_optimizer_util_joininfo::rebuild_placeholder_attr_needed(root)?;
    backend_optimizer_plan_small_seams::rebuild_joinclause_attr_needed::call(root, run);
    backend_optimizer_path_equivclass::rebuild_eclass_attr_needed(root)?;
    backend_optimizer_plan_small_seams::rebuild_lateral_attr_needed::call(root, run);
    Ok(())
}

/// `remove_rel_from_restrictinfo(rinfo, relid, ojrelid)` (analyzejoins.c:657).
/// Clean out relid/ojrelid bits from `clause_relids`/`required_relids`; recurse
/// through OR sub-clauses.
fn remove_rel_from_restrictinfo(
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    relid: i32,
    ojrelid: i32,
) {
    {
        let r = root.rinfo_mut(rinfo);
        r.clause_relids = relids::del_member(relids::copy(&r.clause_relids), relid);
        r.clause_relids = relids::del_member(r.clause_relids.take(), ojrelid);
        r.required_relids = relids::del_member(relids::copy(&r.required_relids), relid);
        r.required_relids = relids::del_member(r.required_relids.take(), ojrelid);
    }

    /* If it's an OR, recurse to clean up sub-clauses. */
    let orclause = root.rinfo(rinfo).orclause;
    if let Some(orclause_id) = orclause {
        // restriction_is_or_clause(rinfo): the orclause is an OR of ANDs /
        // sub-RestrictInfos (make_sub_restrictinfos output). Collect the nested
        // RestrictInfo handles and recurse, mirroring the C foreach over the OR
        // args (the AND vs. sub-RestrictInfo distinction collapses to "every
        // embedded RestrictInfo" in the arena model).
        let mut nested: Vec<RinfoId> = Vec::new();
        crate::change_relids::collect_nested_rinfos(root, orclause_id, &mut nested);
        for rid in nested {
            remove_rel_from_restrictinfo(root, rid, relid, ojrelid);
        }
    }
}

/// `remove_rel_from_eclass(ec, sjinfo, relid, subst)` (analyzejoins.c:718) for
/// the LEFT-JOIN case (`sjinfo != NULL`). Fixes up the EC and EM relid sets and
/// drops empty members; updates the source clauses' relid bits.
fn remove_rel_from_eclass(
    root: &mut PlannerInfo,
    ec_id: types_pathnodes::EcId,
    sjinfo: &SpecialJoinInfo,
    relid: i32,
    subst: i32,
) {
    let ojrelid = sjinfo.ojrelid as i32;

    /* Fix up the EC's overall relids. */
    {
        let ec = root.ec_mut(ec_id);
        ec.ec_relids = relids::adjust_relid_set(&ec.ec_relids, relid, subst);
        ec.ec_relids = relids::adjust_relid_set(&ec.ec_relids, ojrelid, subst);
        debug_assert!(ec.ec_childmembers.is_empty());
    }

    /*
     * Fix up the member expressions. Any non-const member that ends with empty
     * em_relids must be a Var/PHV of the removed relation; drop it.
     */
    let members: Vec<types_pathnodes::EmId> = root.ec(ec_id).ec_members.clone();
    let mut new_members: Vec<types_pathnodes::EmId> = Vec::with_capacity(members.len());
    for em_id in members {
        let touched = {
            let em = root.em(em_id);
            relids::is_member(relid, &em.em_relids) || relids::is_member(ojrelid, &em.em_relids)
        };
        if touched {
            debug_assert!(!root.em(em_id).em_is_const);
            {
                let em = root.em_mut(em_id);
                em.em_relids = relids::adjust_relid_set(&em.em_relids, relid, subst);
                em.em_relids = relids::adjust_relid_set(&em.em_relids, ojrelid, subst);
            }
            if relids::is_empty(&root.em(em_id).em_relids) {
                /* foreach_delete_current: drop it. */
                continue;
            }
        }
        new_members.push(em_id);
    }
    root.ec_mut(ec_id).ec_members = new_members;

    /* Fix up the source clauses, in case we can re-use them later. */
    let sources: Vec<RinfoId> = root.ec(ec_id).ec_sources.clone();
    for rinfo in sources {
        /* sjinfo != NULL path: remove_rel_from_restrictinfo. */
        remove_rel_from_restrictinfo(root, rinfo, relid, ojrelid);
    }

    /* Drop already-derived clauses (base restriction clauses we don't need). */
    backend_optimizer_path_equivclass::ec_clear_derived_clauses(root, ec_id);
}

/// `remove_rel_from_joinlist(joinlist, relid, nremoved)` (analyzejoins.c:789).
/// Build a fresh joinlist with the target relid removed (recursively).
fn remove_rel_from_joinlist(
    joinlist: Vec<JoinlistNode>,
    relid: i32,
    nremoved: &mut i32,
) -> Vec<JoinlistNode> {
    let mut result: Vec<JoinlistNode> = Vec::new();
    for jlnode in joinlist {
        match jlnode {
            JoinlistNode::Rel(varno) => {
                if varno == relid {
                    *nremoved += 1;
                } else {
                    result.push(JoinlistNode::Rel(varno));
                }
            }
            JoinlistNode::Sub(sublist) => {
                let sub = remove_rel_from_joinlist(sublist, relid, nremoved);
                /* Avoid including empty sub-lists in the result. */
                if !sub.is_empty() {
                    result.push(JoinlistNode::Sub(sub));
                }
            }
        }
    }
    result
}

/// `adjust_relid_set` (rewriteManip.c:760) over an [`ExprRelids`]
/// (`{ words: Vec<u64> }`, the PHV's inline `phrels` storage). Mirrors
/// [`crate::relids::adjust_relid_set`] but on the lifetime-free word vector.
fn adjust_expr_relids(
    set: &types_nodes::primnodes::ExprRelids,
    oldrelid: i32,
    newrelid: i32,
) -> types_nodes::primnodes::ExprRelids {
    const INNER_VAR: i32 = 65000;
    let old_is_special = oldrelid >= INNER_VAR;
    let new_is_special = newrelid < 0 || newrelid >= INNER_VAR;
    let is_member = |x: i32| -> bool {
        if x < 0 {
            return false;
        }
        let wn = (x / 64) as usize;
        wn < set.words.len() && (set.words[wn] >> (x % 64)) & 1 == 1
    };
    let mut words = set.words.clone();
    if !old_is_special && is_member(oldrelid) {
        let wn = (oldrelid / 64) as usize;
        words[wn] &= !(1u64 << (oldrelid % 64));
        if !new_is_special {
            let wn2 = (newrelid / 64) as usize;
            if wn2 >= words.len() {
                words.resize(wn2 + 1, 0);
            }
            words[wn2] |= 1u64 << (newrelid % 64);
        }
        // Trim trailing zero words.
        while let Some(&last) = words.last() {
            if last == 0 {
                words.pop();
            } else {
                break;
            }
        }
    }
    types_nodes::primnodes::ExprRelids { words }
}

/// `remove_useless_self_joins(root, joinlist)` (analyzejoins.c:2488) — remove
/// self-joins on a unique column, returning the (possibly trimmed) joinlist.
///
/// The early-exit (GUC off, empty joinlist, or a single non-`List` joinlist
/// element — the common single-relation query) is ported faithfully and returns
/// the joinlist unchanged. The deep `remove_self_joins_recurse` surgery walks
/// the whole `root->parse` `Query` tree for a relid substitution
/// (`ChangeVarNodesExtended`), which the planner model does not yet carry, so it
/// remains seam-and-panic (`crate::change_relids` only walks arena `Expr`/
/// `RestrictInfo` handles, not the `Query` tree). Reaching it is a genuine
/// keystone, surfaced loudly rather than silently mis-planned.
pub fn remove_useless_self_joins<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinlist: Vec<JoinlistNode>,
) -> Vec<JoinlistNode> {
    // C: `if (!enable_self_join_elimination || joinlist == NIL ||
    //        (list_length(joinlist) == 1 && !IsA(linitial(joinlist), List)))
    //         return joinlist;`
    // A single-element joinlist whose sole entry is a RangeTblRef (not a nested
    // `List`) has no self-join potential.
    if !crate::enable_self_join_elimination()
        || joinlist.is_empty()
        || (joinlist.len() == 1 && matches!(joinlist[0], JoinlistNode::Rel(_)))
    {
        return joinlist;
    }

    // C: `toRemove = remove_self_joins_recurse(root, joinlist, toRemove);`
    // remove_self_joins_recurse only performs surgery when it finds a GROUP of
    // 2+ range-table entries over the SAME base-table OID (a potential
    // self-join). Collect the candidate relids exactly as the C does (ordinary
    // base relations, no TABLESAMPLE, not the UPDATE/DELETE/MERGE target), group
    // by relid OID, and decide:
    //   * no same-OID group of >= 2  =>  toRemove is empty, return joinlist
    //     unchanged (the overwhelmingly common case: distinct tables);
    //   * a same-OID group of >= 2   =>  genuine self-join elimination, whose
    //     remove_self_joins_one_group surgery + ChangeVarNodesExtended over
    //     root->parse is not modeled yet — surface loudly.
    let result_relation = px::parse_result_relation::call(run, root);
    let mut candidate_relids: Vec<i32> = Vec::new();
    collect_self_join_candidates(root, run, &joinlist, result_relation, &mut candidate_relids);

    // Group by reloid; any group of >= 2 is a self-join candidate.
    let mut reloids: Vec<types_core::primitive::Oid> = candidate_relids
        .iter()
        .map(|&varno| rte::rte_relid::call(run, root, varno as types_core::Index))
        .collect();
    reloids.sort_unstable();
    let has_self_join_group = reloids
        .windows(2)
        .any(|w| w[0] == w[1]);

    if !has_self_join_group {
        // toRemove stayed empty — nothing to do.
        return joinlist;
    }

    // The merge-pairs surgery + orphaned-relation removal needs the Query-tree
    // relid walker the planner model does not carry yet.
    panic!(
        "remove_useless_self_joins: self-join surgery (remove_self_joins_one_group / \
         ChangeVarNodesExtended over root->parse) not modeled"
    );
}

/// `remove_self_joins_recurse`'s candidate-collection half (analyzejoins.c): walk
/// the joinlist (recursing into nested sub-joinlists) and append every
/// `RangeTblRef` that is an ordinary base relation eligible for self-join removal
/// — `rtekind == RTE_RELATION && relkind == RELKIND_RELATION && tablesample ==
/// NULL && varno != resultRelation` (the `mergeTargetRelation` exclusion is
/// subsumed: a MERGE target is a result relation). We collect only the relids;
/// the grouping/removal is decided by the caller.
fn collect_self_join_candidates<'mcx>(
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinlist: &[JoinlistNode],
    result_relation: i32,
    out: &mut Vec<i32>,
) {
    for node in joinlist {
        match node {
            JoinlistNode::Rel(varno) => {
                let rti = *varno as types_core::Index;
                if rte::rte_rtekind::call(run, root, rti) == types_pathnodes::RTE_RELATION
                    && rte::rte_relkind::call(run, root, rti)
                        == types_tuple::access::RELKIND_RELATION as i8
                    && !rte::rte_has_tablesample::call(run, root, rti)
                    && *varno != result_relation
                {
                    out.push(*varno);
                }
            }
            JoinlistNode::Sub(sublist) => {
                collect_self_join_candidates(root, run, sublist, result_relation, out);
            }
        }
    }
}
