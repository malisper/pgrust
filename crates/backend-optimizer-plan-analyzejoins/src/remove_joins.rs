//! `remove_useless_joins` (left-join removal) and `remove_useless_self_joins`
//! (self-join elimination) with their supporting surgery (analyzejoins.c).
//!
//! ## Left-join removal
//!
//! When a left join's condition cannot match more than one inner-side row and no
//! inner-side variable is needed above the join, the join just duplicates its
//! left input and can be removed. `remove_useless_joins` scans
//! `root->join_info_list` for such joins, proves removability via
//! [`crate::join_is_removable`], and rips the inner rel out of every planner data
//! structure (`remove_leftjoinrel_from_query`). `remove_rel_from_query` runs with
//! `subst == -1` (delete only).
//!
//! ## Self-join elimination
//!
//! `remove_useless_self_joins` groups the joinlist's base relations by Oid and,
//! within each same-Oid group, proves a pair is a unique self-join
//! (`innerrel_is_unique_ext` over the self-join quals) and rewrites every
//! reference to the removed relid into the kept one (`remove_self_join_rel` →
//! `update_eclasses`, clause/targetlist/attr_needed transfer, row-mark transfer,
//! and a `ChangeVarNodesExtended` substitution over `root->parse`, the
//! `processed_tlist`/`processed_groupClause`, and the planner relid sets). The
//! `root->parse` `Query` is resolved off its [`types_pathnodes::QueryId`] through
//! the [`PlannerRun`] store and walked as a real `Node::Query` value; the
//! callback skips `RangeTblRef`s so the trailing `remove_rel_from_joinlist` can
//! still find them by their original relid. The arena `RestrictInfo`/`EM` relid
//! walk is [`crate::change_relids`].

use alloc::vec::Vec;

use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinlistNode, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo, JOIN_INNER,
};

use backend_optimizer_util_relnode as relnode;
use backend_optimizer_rte_seams as rte;
use backend_optimizer_path_equivclass as equivclass;

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

/// `remove_rel_from_query(root, rel, subst, sjinfo, joinrelids)`
/// (analyzejoins.c:324). Rip every reference to `rel`'s relid out of the planner
/// data structures, substituting `subst` for it. `sjinfo == Some` is the
/// left-join (`subst <= 0`, delete) path; `sjinfo == None` is the self-join
/// (`subst > 0`, rename) path.
fn remove_rel_from_query<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    subst: i32,
    sjinfo: Option<&SpecialJoinInfo>,
    joinrelids: Option<&Relids>,
) -> PgResult<()> {
    let mcx = run.mcx();
    let relid = root.rel(rel).relid as i32;
    let ojrelid = sjinfo.map(|s| s.ojrelid as i32);

    /* Update all_baserels and related relid sets. */
    root.all_baserels = relids::adjust_relid_set(&root.all_baserels, relid, subst);
    root.all_query_rels = relids::adjust_relid_set(&root.all_query_rels, relid, subst);

    if let Some(oj) = ojrelid {
        root.outer_join_rels = relids::del_member(relids::copy(&root.outer_join_rels), oj);
        root.all_query_rels = relids::del_member(relids::copy(&root.all_query_rels), oj);
    }

    /*
     * Likewise remove references from SpecialJoinInfo data structures. Upper
     * joins' relid sets have to be adjusted. (We always make a private copy of
     * the relid sets before modifying, as initsplan.c shares them.)
     */
    let n_sjinfos = root.join_info_list.len();
    for idx in 0..n_sjinfos {
        {
            let sjinf = &mut root.join_info_list[idx];
            sjinf.min_lefthand =
                relids::adjust_relid_set(&relids::copy(&sjinf.min_lefthand), relid, subst);
            sjinf.min_righthand =
                relids::adjust_relid_set(&relids::copy(&sjinf.min_righthand), relid, subst);
            sjinf.syn_lefthand =
                relids::adjust_relid_set(&relids::copy(&sjinf.syn_lefthand), relid, subst);
            sjinf.syn_righthand =
                relids::adjust_relid_set(&relids::copy(&sjinf.syn_righthand), relid, subst);
        }

        if let Some(oj) = ojrelid {
            /* sjinfo != NULL (subst <= 0): remove ojrelid bits from the sets. */
            let sjinf = &mut root.join_info_list[idx];
            sjinf.min_lefthand = relids::del_member(sjinf.min_lefthand.take(), oj);
            sjinf.min_righthand = relids::del_member(sjinf.min_righthand.take(), oj);
            sjinf.syn_lefthand = relids::del_member(sjinf.syn_lefthand.take(), oj);
            sjinf.syn_righthand = relids::del_member(sjinf.syn_righthand.take(), oj);
            /* relid cannot appear in these fields, but ojrelid can: */
            sjinf.commute_above_l = relids::del_member(sjinf.commute_above_l.take(), oj);
            sjinf.commute_above_r = relids::del_member(sjinf.commute_above_r.take(), oj);
            sjinf.commute_below_l = relids::del_member(sjinf.commute_below_l.take(), oj);
            sjinf.commute_below_r = relids::del_member(sjinf.commute_below_r.take(), oj);
        } else {
            /* sjinfo == NULL (subst > 0, self-join): rename in semi_rhs_exprs. */
            let semi_rhs: Vec<types_pathnodes::NodeId> =
                root.join_info_list[idx].semi_rhs_exprs.clone();
            crate::change_relids::change_relids_in_node_list(
                mcx,
                root,
                &semi_rhs,
                ReplaceRelidContext { rt_index: relid, new_index: subst },
            )?;
        }
    }

    /*
     * Likewise remove references from PlaceHolderVar data structures, removing
     * any no-longer-needed placeholders entirely (left-join removal only — for
     * self-join elimination the PHV is renamed onto the remaining relation).
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

        // sjinfo != NULL removal branch only.
        if let (Some(s), Some(jr)) = (sjinfo, joinrelids) {
            let oj = s.ojrelid as i32;
            debug_assert!(!relids::is_member(relid, &root.phinfo(ph_id).ph_lateral));
            if relids::is_subset(&ph_needed, jr)
                && relids::is_member(relid, &ph_eval_at)
                && !relids::is_member(oj, &ph_eval_at)
            {
                // Remove the PHV (foreach_delete_current + null the array slot).
                root.placeholder_array[phid as usize] = None;
                continue;
            }
        }

        // Otherwise, update the PHV.
        {
            let phinfo = root.phinfo_mut(ph_id);
            phinfo.ph_eval_at = relids::adjust_relid_set(&phinfo.ph_eval_at, relid, subst);
            if let Some(oj) = ojrelid {
                phinfo.ph_eval_at = relids::adjust_relid_set(&phinfo.ph_eval_at, oj, subst);
            }
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
            phinfo.ph_var_phrels = relids::adjust_relid_set(&phinfo.ph_var_phrels, relid, subst);
            phinfo.ph_var.phrels = adjust_expr_relids(&phinfo.ph_var.phrels, relid, subst);
            if let Some(oj) = ojrelid {
                phinfo.ph_var_phrels = relids::adjust_relid_set(&phinfo.ph_var_phrels, oj, subst);
                phinfo.ph_var.phrels = adjust_expr_relids(&phinfo.ph_var.phrels, oj, subst);
            }
            debug_assert!(!phinfo.ph_var.phrels.words.iter().all(|&w| w == 0));
        }

        /* ChangeVarNodesExtended((Node *) phv->phexpr, relid, subst, …). */
        let phexpr_id = root.phinfo(ph_id).ph_var_phexpr;
        change_relids_in_node(
            mcx,
            root,
            phexpr_id,
            ReplaceRelidContext { rt_index: relid, new_index: subst },
        )?;
        kept.push(ph_id);
    }
    root.placeholder_list = kept;

    /*
     * Likewise remove references from EquivalenceClasses.
     */
    let n_ecs = root.eq_classes.len();
    for ec_i in 0..n_ecs {
        let ec_id = types_pathnodes::EcId(ec_i as u32);
        // C: `if (member(relid) || (sjinfo == NULL || member(ojrelid)))`.
        let touched = relids::is_member(relid, &root.ec(ec_id).ec_relids)
            || sjinfo.is_none()
            || ojrelid.map_or(false, |oj| relids::is_member(oj, &root.ec(ec_id).ec_relids));
        if touched {
            remove_rel_from_eclass(mcx, root, ec_id, sjinfo, relid, subst)?;
        }
    }

    /*
     * Recompute per-Var attr_needed (and lateral_vars) relid sets. Strip all
     * bits other than "relation 0" from attr_needed sets (ph_needed handled
     * above); for the self-join rename (subst > 0) also rename lateral_vars.
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

        if subst > 0 {
            let lateral_vars: Vec<types_pathnodes::NodeId> =
                root.rel(otherrel).lateral_vars.clone();
            crate::change_relids::change_relids_in_node_list(
                mcx,
                root,
                &lateral_vars,
                ReplaceRelidContext { rt_index: relid, new_index: subst },
            )?;
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

    /* Left-join removal: subst = -1 (delete), sjinfo != NULL. */
    remove_rel_from_query(root, run, rel, -1, Some(sjinfo), Some(&joinrelids))?;

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
    backend_optimizer_util_joininfo::rebuild_placeholder_attr_needed(run.mcx(), root)?;
    backend_optimizer_plan_small_seams::rebuild_joinclause_attr_needed::call(root, run);
    backend_optimizer_path_equivclass::rebuild_eclass_attr_needed(run.mcx(), root)?;
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

/// `remove_rel_from_eclass(ec, sjinfo, relid, subst)` (analyzejoins.c:718).
/// Fixes up the EC and EM relid sets and drops empty members; updates the source
/// clauses' relid bits. `sjinfo == None` is the self-join (`subst > 0`) rename
/// path; `Some` is the left-join (`subst <= 0`) deletion path.
fn remove_rel_from_eclass<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    ec_id: types_pathnodes::EcId,
    sjinfo: Option<&SpecialJoinInfo>,
    relid: i32,
    subst: i32,
) -> types_error::PgResult<()> {
    let ojrelid = sjinfo.map(|s| s.ojrelid as i32);

    /* Fix up the EC's overall relids. */
    {
        let ec = root.ec_mut(ec_id);
        ec.ec_relids = relids::adjust_relid_set(&ec.ec_relids, relid, subst);
        if let Some(oj) = ojrelid {
            ec.ec_relids = relids::adjust_relid_set(&ec.ec_relids, oj, subst);
        }
        debug_assert!(ec.ec_childmembers.iter().all(|v| v.is_empty()));
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
            relids::is_member(relid, &em.em_relids)
                || ojrelid.map_or(false, |oj| relids::is_member(oj, &em.em_relids))
        };
        if touched {
            debug_assert!(!root.em(em_id).em_is_const);
            {
                let em = root.em_mut(em_id);
                em.em_relids = relids::adjust_relid_set(&em.em_relids, relid, subst);
                if let Some(oj) = ojrelid {
                    em.em_relids = relids::adjust_relid_set(&em.em_relids, oj, subst);
                }
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
        if let Some(s) = sjinfo {
            /* sjinfo != NULL path: remove_rel_from_restrictinfo. */
            remove_rel_from_restrictinfo(root, rinfo, relid, s.ojrelid as i32);
        } else {
            /* sjinfo == NULL (self-join): ChangeVarNodesExtended(rinfo, ...). */
            crate::change_relids::change_relids_in_rinfo(
                mcx,
                root,
                rinfo,
                crate::change_relids::ReplaceRelidContext { rt_index: relid, new_index: subst },
            )?;
        }
    }

    /* Drop already-derived clauses (base restriction clauses we don't need). */
    backend_optimizer_path_equivclass::ec_clear_derived_clauses(root, ec_id);
    Ok(())
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
    // IS_SPECIAL_VARNO(varno) == ((int) varno < 0): the special varnos
    // (INNER_VAR/OUTER_VAR/INDEX_VAR/ROWID_VAR == -1..-4) and the left-join
    // removal "delete only" sentinel are all negative; real RT indices are >= 1.
    let old_is_special = oldrelid < 0;
    let new_is_special = newrelid < 0;
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
/// Search for joins where a relation is joined to itself: if the join clause for
/// each tuple from one side is proven to match the same physical row (or
/// nothing) on the other side, the self-join is eliminated. Suitable join
/// clauses are of the form `X = X` and become `X IS NOT NULL` clauses.
pub fn remove_useless_self_joins<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    joinlist: Vec<JoinlistNode>,
) -> types_error::PgResult<Vec<JoinlistNode>> {
    // C: `if (!enable_self_join_elimination || joinlist == NIL ||
    //        (list_length(joinlist) == 1 && !IsA(linitial(joinlist), List)))
    //         return joinlist;`
    if !crate::enable_self_join_elimination()
        || joinlist.is_empty()
        || (joinlist.len() == 1 && matches!(joinlist[0], JoinlistNode::Rel(_)))
    {
        return Ok(joinlist);
    }

    // toRemove = remove_self_joins_recurse(root, joinlist, toRemove);
    let mut to_remove: Relids = None;
    to_remove = remove_self_joins_recurse(root, run, &joinlist, to_remove)?;

    if relids::is_empty(&to_remove) {
        return Ok(joinlist);
    }

    // Restore the removed relations' RangeTblRefs to the joinlist (delete them).
    let mut joinlist = joinlist;
    let mut relid: i32 = -1;
    loop {
        relid = bms_next_member(&to_remove, relid);
        if relid < 0 {
            break;
        }
        let mut nremoved = 0;
        joinlist = remove_rel_from_joinlist(joinlist, relid, &mut nremoved);
    }

    Ok(joinlist)
}

/// `bms_next_member` over a [`Relids`] (signed-relid iterator, start at -1).
fn bms_next_member(set: &Relids, prev: i32) -> i32 {
    let Some(bms) = set else {
        return -2;
    };
    let mut i = prev + 1;
    let nbits = (bms.words.len() * 64) as i32;
    while i < nbits {
        let w = (i / 64) as usize;
        if (bms.words[w] >> (i % 64)) & 1 == 1 {
            return i;
        }
        i += 1;
    }
    -2
}

/// `remove_self_joins_recurse(root, joinlist, toRemove)` (analyzejoins.c:2307) —
/// gather base-relation indexes from the joinlist, group them by Oid, and try to
/// eliminate self-joins within each same-Oid group.
fn remove_self_joins_recurse<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    joinlist: &[JoinlistNode],
    mut to_remove: Relids,
) -> types_error::PgResult<Relids> {
    let result_relation = run.resolve(root.parse).resultRelation;
    let merge_target = run.resolve(root.parse).mergeTargetRelation;

    // Collect indexes of base relations of this join-tree level.
    let mut relids: Relids = None;
    for node in joinlist {
        match node {
            JoinlistNode::Rel(varno) => {
                let rti = *varno as types_core::Index;
                if rte::rte_rtekind::call(run, root, rti) == types_pathnodes::RTE_RELATION
                    && rte::rte_relkind::call(run, root, rti)
                        == types_tuple::access::RELKIND_RELATION as i8
                    && !rte::rte_has_tablesample::call(run, root, rti)
                    && *varno != result_relation
                    && *varno != merge_target
                {
                    debug_assert!(!relids::is_member(*varno, &relids));
                    relids = relids::add_member(relids, *varno);
                }
            }
            JoinlistNode::Sub(sublist) => {
                to_remove = remove_self_joins_recurse(root, run, sublist, to_remove)?;
            }
        }
    }

    let num_rels = relids::num_members(&relids);
    // Need at least two relations for the join.
    if num_rels < 2 {
        return Ok(to_remove);
    }

    // Build a candidate array of (relid, reloid) and sort it by reloid.
    let mut candidates: Vec<(i32, types_core::primitive::Oid)> = Vec::with_capacity(num_rels as usize);
    let mut i: i32 = -1;
    loop {
        i = bms_next_member(&relids, i);
        if i < 0 {
            break;
        }
        let reloid = rte::rte_relid::call(run, root, i as types_core::Index);
        candidates.push((i, reloid));
    }
    // self_join_candidates_cmp — stable order by reloid.
    candidates.sort_by(|a, b| a.1.cmp(&b.1));

    // Iteratively form same-Oid groups and remove self-joins within each.
    let n = candidates.len();
    let mut i: usize = 0;
    let mut j: usize = 1;
    while j < n + 1 {
        if j == n || candidates[j].1 != candidates[i].1 {
            if j - i >= 2 {
                // Group of >= 2 same-Oid relations.
                let mut group: Relids = None;
                while i < j {
                    group = relids::add_member(group, candidates[i].0);
                    i += 1;
                }
                relids = relids::difference(&relids, &group);

                // Iterate while the group keeps shrinking and stays multiple.
                loop {
                    debug_assert!(!relids::overlap(&group, &to_remove));
                    let removed = remove_self_joins_one_group(root, run, &group)?;
                    to_remove = relids::add_members(to_remove, &removed);
                    group = relids::difference(&group, &removed);
                    if relids::is_empty(&removed) || !relids::membership_is_multiple(&group) {
                        break;
                    }
                }
            } else {
                // Single relation; just remove it from the set.
                relids = relids::del_member(relids, candidates[i].0);
                i = j;
            }
        }
        j += 1;
    }

    debug_assert!(relids::is_empty(&relids));
    Ok(to_remove)
}

/// `remove_self_joins_one_group(root, relids)` (analyzejoins.c:2140) — find and
/// remove unique self-joins within a group of same-Oid base relations.
fn remove_self_joins_one_group<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    group: &Relids,
) -> types_error::PgResult<Relids> {
    let mut result: Relids = None;

    let mut r: i32 = -1;
    loop {
        r = bms_next_member(group, r);
        if r <= 0 {
            break;
        }
        let rrel = match root.simple_rel_array[r as usize] {
            Some(rel) => rel,
            None => continue,
        };

        let mut k: i32 = r;
        loop {
            k = bms_next_member(group, k);
            if k <= 0 {
                break;
            }
            let krel = match root.simple_rel_array[k as usize] {
                Some(rel) => rel,
                None => continue,
            };

            // Sanity: same Oid.
            debug_assert_eq!(
                rte::rte_relid::call(run, root, k as types_core::Index),
                rte::rte_relid::call(run, root, r as types_core::Index)
            );

            // Can't eliminate if the two rels belong to different special-join
            // ordering rules.
            let mut jinfo_check = true;
            for info in &root.join_info_list {
                if (relids::is_member(k, &info.syn_lefthand)
                    ^ relids::is_member(r, &info.syn_lefthand))
                    || (relids::is_member(k, &info.syn_righthand)
                        ^ relids::is_member(r, &info.syn_righthand))
                {
                    jinfo_check = false;
                    break;
                }
            }
            if !jinfo_check {
                continue;
            }

            // Row-mark equivalence: can't remove if the rels have row marks of
            // different strength.
            let mut kmark: Option<types_pathnodes::PlanRowMarkId> = None;
            let mut rmark: Option<types_pathnodes::PlanRowMarkId> = None;
            for &rm_id in &root.rowMarks {
                let rti = run.resolve_rowmark(rm_id).rti as i32;
                if rti == r {
                    debug_assert!(rmark.is_none());
                    rmark = Some(rm_id);
                } else if rti == k {
                    debug_assert!(kmark.is_none());
                    kmark = Some(rm_id);
                }
                if kmark.is_some() && rmark.is_some() {
                    break;
                }
            }
            if let (Some(km), Some(rm)) = (kmark, rmark) {
                if run.resolve_rowmark(km).markType != run.resolve_rowmark(rm).markType {
                    continue;
                }
            }

            // Base rels: relids bitset has only one member, their relid.
            let mut joinrelids: Relids = None;
            joinrelids = relids::add_member(joinrelids, r);
            joinrelids = relids::add_member(joinrelids, k);

            // restrictlist = generate_join_implied_equalities(...). At this stage
            // joininfo lists can only contain clauses for a superior outer join,
            // so we can skip build_joinrel_restrictlist.
            let rrel_relids = relids::copy(&root.rel(rrel).relids);
            let restrictlist = match equivclass::generate_join_implied_equalities(
                root,
                run,
                relids::copy(&joinrelids),
                rrel_relids.clone(),
                krel,
                None,
            ) {
                Ok(l) => l,
                Err(_) => continue,
            };
            if restrictlist.is_empty() {
                continue;
            }

            // Separate self-join quals ("x = x") from other quals ("a = b").
            let (mut selfjoinquals, otherjoinquals) =
                split_selfjoin_quals(root, run, &restrictlist)?;

            debug_assert_eq!(
                restrictlist.len(),
                selfjoinquals.len() + otherjoinquals.len()
            );

            // Add krel's baserestrictinfo to selfjoinquals to enable SJE for the
            // degenerate case without any self-join clauses.
            selfjoinquals.extend(root.rel(krel).baserestrictinfo.iter().copied());

            // Prove the outer (rrel) can't duplicate inner rows.
            let mut uclauses: Vec<RinfoId> = Vec::new();
            let force_cache = otherjoinquals.is_empty();
            if !crate::innerrel_is_unique_ext(
                root,
                run,
                &joinrelids,
                &rrel_relids,
                krel,
                JOIN_INNER,
                &selfjoinquals,
                force_cache,
                /* self_join = */ true,
                Some(&mut uclauses),
            ) {
                continue;
            }

            // Validate inner baserestrictinfo matches the unique-index clauses.
            let krel_relid = root.rel(krel).relid as i32;
            if !match_unique_clauses(root, run, rrel, &uclauses, krel_relid)? {
                continue;
            }

            // Remove rrel from the planner structures and the corresponding mark.
            remove_self_join_rel(root, run, kmark, rmark, krel, rrel, &restrictlist)?;

            result = relids::add_member(result, r);

            // We have removed the outer relation, try the next one.
            break;
        }
    }

    Ok(result)
}

/// `split_selfjoin_quals(root, joinquals, &selfjoinquals, &otherjoinquals, from,
/// to)` (analyzejoins.c:2008) — partition `joinquals` into the quals whose two
/// sides match under the self-join relid change (`x = x`) and the rest.
fn split_selfjoin_quals<'mcx>(
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinquals: &[RinfoId],
) -> types_error::PgResult<(Vec<RinfoId>, Vec<RinfoId>)> {
    let mcx = run.mcx();
    let mut sjoinquals: Vec<RinfoId> = Vec::new();
    let mut ojoinquals: Vec<RinfoId> = Vec::new();

    for &rinfo_id in joinquals {
        let rinfo = root.rinfo(rinfo_id);

        // Clause looks like F(arg1) = G(arg2): require a mergejoinable OpExpr
        // with exactly two relids and singleton left/right relids.
        if rinfo.mergeopfamilies.is_empty()
            || relids::num_members(&rinfo.clause_relids) != 2
            || !relids::membership_is_singleton(&rinfo.left_relids)
            || !relids::membership_is_singleton(&rinfo.right_relids)
        {
            ojoinquals.push(rinfo_id);
            continue;
        }

        let clause = root.node(rinfo.clause);
        let (left_arg, right_arg) = match clause {
            // Deep-copy the operands via `Expr::clone_in` (they are taken by
            // value into strip_relabel and mutated; a derived `Expr::clone`
            // panics on a context-allocated child).
            Expr::OpExpr(op) if op.args.len() == 2 => (
                op.args[0].clone_in(mcx).expect("clone_in"),
                op.args[1].clone_in(mcx).expect("clone_in"),
            ),
            _ => {
                ojoinquals.push(rinfo_id);
                continue;
            }
        };

        // leftexpr = get_leftop(clause), rightexpr = copyObject(get_rightop).
        let leftexpr = strip_relabel(left_arg);
        let mut rightexpr = strip_relabel(right_arg);

        // ChangeVarNodesExtended(rightexpr, right_singleton, left_singleton, 0).
        let from = relids::get_singleton_member(&rinfo.right_relids).unwrap_or(-1);
        let to = relids::get_singleton_member(&rinfo.left_relids).unwrap_or(-1);
        change_expr_relids_standalone(mcx, &mut rightexpr, from, to)?;

        if backend_optimizer_path_equivclass_ext_seams::equal::call(&leftexpr, &rightexpr) {
            sjoinquals.push(rinfo_id);
        } else {
            ojoinquals.push(rinfo_id);
        }
    }

    Ok((sjoinquals, ojoinquals))
}

/// `RelabelType` strip — `if (IsA(x, RelabelType)) x = ((RelabelType *)x)->arg`.
fn strip_relabel(mut e: Expr) -> Expr {
    if let Expr::RelabelType(r) = &mut e {
        if let Some(arg) = r.arg.take() {
            // Move the contained arg out by value (no derived `Expr::clone`,
            // which would panic on a context-allocated child).
            return *arg;
        }
    }
    e
}

/// `ChangeVarNodesExtended((Node *) expr, from, to, 0, replace_relid_callback)`
/// over a standalone leaf [`Expr`] (no embedded planner RestrictInfo): the
/// callback's only relevant branch is the RangeTblRef skip, which a leaf clause
/// expr never contains, so plain `ChangeVarNodes` suffices.
fn change_expr_relids_standalone<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    expr: &mut Expr<'mcx>,
    from: i32,
    to: i32,
) -> types_error::PgResult<()> {
    let owned = core::mem::replace(expr, dummy_expr());
    let mut node = types_nodes::nodes::Node::mk_expr(mcx, owned)?;
    backend_rewrite_core::change::ChangeVarNodes(&mut node, from, to, 0, mcx);
    *expr = node
        .into_expr()
        .unwrap_or_else(|| unreachable!("ChangeVarNodes returned a non-Expr for an Expr input"));
    Ok(())
}

/// A throwaway [`Expr`] used as the `mem::replace` placeholder; immediately
/// overwritten and never observed.
fn dummy_expr<'mcx>() -> Expr<'mcx> {
    Expr::Const(types_nodes::primnodes::Const::default())
}

/// `match_unique_clauses(root, outer, uclauses, relid)` (analyzejoins.c:2074) —
/// validate that the inner relation's baserestrictinfo contains the same
/// expressions as the unique-index `uclauses` from the outer relation.
fn match_unique_clauses<'mcx>(
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    outer: RelId,
    uclauses: &[RinfoId],
    relid: i32,
) -> types_error::PgResult<bool> {
    let outer_relid = root.rel(outer).relid as i32;
    debug_assert!(outer_relid > 0 && relid > 0);
    let mcx = run.mcx();

    for &rinfo_id in uclauses {
        let rinfo = root.rinfo(rinfo_id);
        // Only filters like f(R.x1,...,R.xN) == expr: exactly one of
        // left_relids/right_relids is empty.
        debug_assert!(
            relids::is_empty(&rinfo.left_relids) ^ relids::is_empty(&rinfo.right_relids)
        );

        // clause = copyObject(rinfo->clause); ChangeVarNodes(clause, relid, outer->relid).
        // Deep-copy via `Expr::clone_in` (a derived `Expr::clone` panics on a
        // context-allocated child).
        let mut clause = root.node(rinfo.clause).clone_in(mcx)?;
        change_expr_relids_standalone(mcx, &mut clause, relid, outer_relid)?;

        let left_empty = relids::is_empty(&rinfo.left_relids);
        let (iclause, c1) = op_sides(&clause, left_empty);
        let (Some(iclause), Some(c1)) = (iclause, c1) else {
            return Ok(false);
        };

        let mut matched = false;
        for &orinfo_id in &root.rel(outer).baserestrictinfo {
            let orinfo = root.rinfo(orinfo_id);
            if orinfo.mergeopfamilies.is_empty() {
                // Don't consider clauses not similar to 'F(X) = G(Y)'.
                continue;
            }
            let oclause_node = root.node(orinfo.clause);
            let o_left_empty = relids::is_empty(&orinfo.left_relids);
            let (oclause, c2) = op_sides(oclause_node, o_left_empty);
            let (Some(oclause), Some(c2)) = (oclause, c2) else {
                continue;
            };
            if backend_optimizer_path_equivclass_ext_seams::equal::call(&iclause, &oclause)
                && backend_optimizer_path_equivclass_ext_seams::equal::call(&c1, &c2)
            {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

/// For an OpExpr clause, return `(iclause, c1)`:
///   when `left_empty`: `(get_rightop, get_leftop)` else `(get_leftop, get_rightop)`.
fn op_sides<'mcx>(clause: &Expr<'mcx>, left_empty: bool) -> (Option<Expr<'mcx>>, Option<Expr<'mcx>>) {
    if let Expr::OpExpr(op) = clause {
        let left = op.args.first().cloned();
        let right = op.args.get(1).cloned();
        if left_empty {
            (right, left)
        } else {
            (left, right)
        }
    } else {
        (None, None)
    }
}

/// `restrict_infos_logically_equal(a, b)` (analyzejoins.c:1632) — compare two
/// RestrictInfos for logical equality, ignoring their `rinfo_serial`.
fn restrict_infos_logically_equal(root: &PlannerInfo, a: RinfoId, b: RinfoId) -> bool {
    // C neutralizes `rinfo_serial` (sets a's to b's), then `equal(a, b)`. The
    // RestrictInfo `equal` (`_equalRestrictInfo`, equalfuncs.funcs.c:3257) compares
    // exactly: clause, is_pushed_down, has_clone, is_clone, security_level,
    // required_relids, incompatible_relids, outer_relids, rinfo_serial — so with
    // the serial neutralized we replicate that field set here (the RestrictInfo
    // is an arena handle, not a `Node`, so we compare the modeled fields rather
    // than feeding the wrapper to the generic `equal`).
    let ra = root.rinfo(a);
    let rb = root.rinfo(b);
    ra.is_pushed_down == rb.is_pushed_down
        && ra.has_clone == rb.has_clone
        && ra.is_clone == rb.is_clone
        && ra.security_level == rb.security_level
        && relids::equal(&ra.required_relids, &rb.required_relids)
        && relids::equal(&ra.incompatible_relids, &rb.incompatible_relids)
        && relids::equal(&ra.outer_relids, &rb.outer_relids)
        && backend_optimizer_path_equivclass_ext_seams::equal::call(
            root.node(ra.clause),
            root.node(rb.clause),
        )
}

/// Which `RelOptInfo` clause list `add_non_redundant_clauses` keeps against. In
/// C this is the `List **keep_rinfo_list` pointer (`&toKeep->baserestrictinfo`
/// or `&toKeep->joininfo`); the port re-reads the live list from the keep rel
/// each iteration so that clauses just distributed in this same loop are seen by
/// later candidates (C passes the list by pointer precisely because
/// `distribute_restrictinfo_to_rels()` mutates it mid-loop).
#[derive(Clone, Copy)]
enum KeepList {
    Base,
    Join,
}

/// `add_non_redundant_clauses(root, rinfo_candidates, &keep_rinfo_list,
/// removed_relid)` (analyzejoins.c:1658) — distribute each non-redundant
/// candidate RestrictInfo to its rels, skipping ones logically equal to a clause
/// already in the keep rel's clause list.
fn add_non_redundant_clauses<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rinfo_candidates: &[RinfoId],
    keep_rel: RelId,
    keep_list: KeepList,
    _removed_relid: i32,
) -> PgResult<()> {
    for &rinfo_id in rinfo_candidates {
        debug_assert!(!relids::is_member(
            _removed_relid,
            &root.rinfo(rinfo_id).required_relids
        ));

        let mut is_redundant = false;
        let cand_clause_relids = relids::copy(&root.rinfo(rinfo_id).clause_relids);
        let cand_parent_ec = root.rinfo(rinfo_id).parent_ec;

        // Re-read the live keep list each iteration: `distribute_restrictinfo_to_rels`
        // appends to it, and (matching C's pass-by-pointer) later candidates must
        // see clauses distributed earlier in this same loop.
        let keep_rinfo_list: Vec<RinfoId> = match keep_list {
            KeepList::Base => root.rel(keep_rel).baserestrictinfo.clone(),
            KeepList::Join => root.rel(keep_rel).joininfo.clone(),
        };

        for &src_id in &keep_rinfo_list {
            if !relids::equal(&root.rinfo(src_id).clause_relids, &cand_clause_relids) {
                continue;
            }
            let src_parent_ec = root.rinfo(src_id).parent_ec;
            if src_id == rinfo_id
                || (cand_parent_ec.is_some() && src_parent_ec == cand_parent_ec)
                || restrict_infos_logically_equal(root, rinfo_id, src_id)
            {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            backend_optimizer_path_equivclass_ext_seams::distribute_restrictinfo_to_rels::call(
                run, root, rinfo_id,
            )?;
        }
    }
    Ok(())
}

/// `update_eclasses(ec, from, to)` (analyzejoins.c:1532) — rewrite the varno
/// `from`→`to` in an EquivalenceClass's members and source clauses, dropping
/// duplicates created by the replacement.
fn update_eclasses<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    ec: types_pathnodes::EcId,
    from: i32,
    to: i32,
) -> types_error::PgResult<()> {
    let mcx = run.mcx();
    debug_assert!(root.ec(ec).ec_childmembers.iter().all(|v| v.is_empty()));

    // --- members ---
    let members = root.ec(ec).ec_members.clone();
    let mut new_members: Vec<types_pathnodes::EmId> = Vec::new();
    for em_id in members {
        if !relids::is_member(from, &root.em(em_id).em_relids) {
            new_members.push(em_id);
            continue;
        }

        {
            let em = root.em_mut(em_id);
            em.em_relids = relids::adjust_relid_set(&em.em_relids, from, to);
            if let Some(jd) = em.em_jdomain.as_deref_mut() {
                jd.jd_relids = relids::adjust_relid_set(&jd.jd_relids, from, to);
            }
        }
        // ChangeVarNodesExtended((Node *) em->em_expr, from, to, 0, ...).
        crate::change_relids::change_relids_in_em(
            mcx,
            root,
            em_id,
            crate::change_relids::ReplaceRelidContext { rt_index: from, new_index: to },
        )?;

        // Drop if redundant with an already-kept member.
        let mut is_redundant = false;
        let em_relids = relids::copy(&root.em(em_id).em_relids);
        // Borrow the EM expr (only inspected by `equal`); a derived `.clone()`
        // would panic on a context-allocated child.
        let em_expr_id = root.em(em_id).em_expr;
        let em_expr: &Expr = root.node(em_expr_id);
        for &other_id in &new_members {
            if !relids::equal(&root.em(other_id).em_relids, &em_relids) {
                continue;
            }
            if backend_optimizer_path_equivclass_ext_seams::equal::call(
                root.node(root.em(other_id).em_expr),
                em_expr,
            ) {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            new_members.push(em_id);
        }
    }
    root.ec_mut(ec).ec_members = new_members;

    // ec_clear_derived_clauses(ec).
    backend_optimizer_path_equivclass::derives::ec_clear_derived_clauses(root, ec);

    // --- sources ---
    let sources = root.ec(ec).ec_sources.clone();
    let mut new_sources: Vec<RinfoId> = Vec::new();
    for rinfo_id in sources {
        if !relids::is_member(from, &root.rinfo(rinfo_id).required_relids) {
            new_sources.push(rinfo_id);
            continue;
        }

        // ChangeVarNodesExtended((Node *) rinfo, from, to, 0, replace_relid_callback).
        crate::change_relids::change_relids_in_rinfo(
            mcx,
            root,
            rinfo_id,
            crate::change_relids::ReplaceRelidContext { rt_index: from, new_index: to },
        )?;

        let mut is_redundant = false;
        let clause_relids = relids::copy(&root.rinfo(rinfo_id).clause_relids);
        // Borrow the clause (only inspected by `equal`); a derived `.clone()`
        // would panic on a context-allocated child.
        let clause_id = root.rinfo(rinfo_id).clause;
        let clause: &Expr = root.node(clause_id);
        for &other_id in &new_sources {
            if !relids::equal(&root.rinfo(other_id).clause_relids, &clause_relids) {
                continue;
            }
            if backend_optimizer_path_equivclass_ext_seams::equal::call(
                root.node(root.rinfo(other_id).clause),
                clause,
            ) {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            new_sources.push(rinfo_id);
        }
    }
    root.ec_mut(ec).ec_sources = new_sources;
    let new_relids = relids::adjust_relid_set(&root.ec(ec).ec_relids, from, to);
    root.ec_mut(ec).ec_relids = new_relids;
    Ok(())
}

/// `remove_self_join_rel(root, kmark, rmark, toKeep, toRemove, restrictlist)`
/// (analyzejoins.c:1826) — remove `toRemove` after proving it participates only
/// in an unneeded unique self-join: transfer its clauses, ECs, targetlist,
/// attr_needed, and row mark to `toKeep`, then rewrite every reference (varno
/// `toRemove->relid` → `toKeep->relid`) across the query and planner structures.
fn remove_self_join_rel<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    kmark: Option<types_pathnodes::PlanRowMarkId>,
    rmark: Option<types_pathnodes::PlanRowMarkId>,
    to_keep: RelId,
    to_remove: RelId,
    restrictlist: &[RinfoId],
) -> types_error::PgResult<()> {
    let mcx = run.mcx();
    let keep_relid = root.rel(to_keep).relid as i32;
    let remove_relid = root.rel(to_remove).relid as i32;
    debug_assert!(keep_relid > 0);
    debug_assert!(remove_relid > 0);

    let ctx = crate::change_relids::ReplaceRelidContext {
        rt_index: remove_relid,
        new_index: keep_relid,
    };

    let mut jinfo_candidates: Vec<RinfoId> = Vec::new();
    let mut binfo_candidates: Vec<RinfoId> = Vec::new();

    // Process toRemove->joininfo: detach from rels, rewrite relids, then sort
    // into join vs base candidates.
    let joininfos = root.rel(to_remove).joininfo.clone();
    for rinfo_id in joininfos {
        let req = relids::copy(&root.rinfo(rinfo_id).required_relids);
        backend_optimizer_util_joininfo::remove_join_clause_from_rels(run, root, rinfo_id, &req);
        crate::change_relids::change_relids_in_rinfo(mcx, root, rinfo_id, ctx)?;

        if relids::membership_is_multiple(&root.rinfo(rinfo_id).required_relids) {
            jinfo_candidates.push(rinfo_id);
        } else {
            binfo_candidates.push(rinfo_id);
        }
    }

    // Concatenate restrictlist to toRemove->baserestrictinfo, then rewrite all.
    {
        let bri = &mut root.rel_mut(to_remove).baserestrictinfo;
        bri.extend(restrictlist.iter().copied());
    }
    let baserestricts = root.rel(to_remove).baserestrictinfo.clone();
    for rinfo_id in baserestricts {
        crate::change_relids::change_relids_in_rinfo(mcx, root, rinfo_id, ctx)?;
        if relids::membership_is_multiple(&root.rinfo(rinfo_id).required_relids) {
            jinfo_candidates.push(rinfo_id);
        } else {
            binfo_candidates.push(rinfo_id);
        }
    }

    // Add all non-redundant clauses to toKeep. The keep list is re-read live
    // inside add_non_redundant_clauses (C passes `&toKeep->baserestrictinfo` /
    // `&toKeep->joininfo` by pointer because distribute_restrictinfo_to_rels
    // mutates it mid-loop).
    add_non_redundant_clauses(
        root,
        run,
        &binfo_candidates,
        to_keep,
        KeepList::Base,
        remove_relid,
    )
    .expect("add_non_redundant_clauses (base)");
    add_non_redundant_clauses(
        root,
        run,
        &jinfo_candidates,
        to_keep,
        KeepList::Join,
        remove_relid,
    )
    .expect("add_non_redundant_clauses (join)");

    // Arrange equivalence classes: replace remove_relid with keep_relid.
    let mut ei: i32 = -1;
    loop {
        ei = bms_next_member(&root.rel(to_remove).eclass_indexes, ei);
        if ei < 0 {
            break;
        }
        let ec_id = types_pathnodes::EcId(ei as u32);
        update_eclasses(root, run, ec_id, remove_relid, keep_relid)?;
        let cur = relids::copy(&root.rel(to_keep).eclass_indexes);
        root.rel_mut(to_keep).eclass_indexes = relids::add_member(cur, ei);
    }

    // Transfer the targetlist (reltarget->exprs) and attr_needed flags.
    let remove_exprs: Vec<types_pathnodes::NodeId> = root
        .rel(to_remove)
        .reltarget
        .as_ref()
        .map(|t| t.exprs.clone())
        .unwrap_or_default();
    for node_id in remove_exprs {
        crate::change_relids::change_relids_in_node(mcx, root, node_id, ctx)?;
        let keep_exprs = root
            .rel(to_keep)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.clone())
            .unwrap_or_default();
        // C uses `list_member` here (analyzejoins.c:1913), which compares with
        // `equal()` (structural), not pointer identity. After relid replacement a
        // `toRemove` expr (e.g. `t2.c`) becomes structurally identical to an
        // existing `toKeep` expr (`t3.c`) but is a distinct arena node, so an
        // identity `contains` would miss the duplicate. Compare structurally.
        let already_present = keep_exprs.iter().any(|&keep_id| {
            backend_optimizer_path_equivclass_ext_seams::equal::call(
                root.node(keep_id),
                root.node(node_id),
            )
        });
        if !already_present {
            if let Some(t) = root.rel_mut(to_keep).reltarget.as_deref_mut() {
                t.exprs.push(node_id);
            }
        }
    }

    let (keep_min, keep_max) = {
        let krel = root.rel(to_keep);
        (krel.min_attr as i32, krel.max_attr as i32)
    };
    // C indexes BOTH toRemove->attr_needed and toKeep->attr_needed with the
    // keep-relative offset `attno = i - toKeep->min_attr` (analyzejoins.c:1917).
    // For a same-OID self-join the rels share min_attr, so this is also the
    // remove-relative offset.
    for i in keep_min..=keep_max {
        let attno = (i - keep_min) as usize;
        let adjusted = relids::adjust_relid_set(
            &root.rel(to_remove).attr_needed[attno],
            remove_relid,
            keep_relid,
        );
        root.rel_mut(to_remove).attr_needed[attno] = relids::copy(&adjusted);
        let cur = root.rel(to_keep).attr_needed[attno].clone();
        root.rel_mut(to_keep).attr_needed[attno] = relids::add_members(cur, &adjusted);
    }

    // Transfer / drop the row mark.
    if let Some(rm) = rmark {
        if let Some(km) = kmark {
            debug_assert_eq!(
                run.resolve_rowmark(km).markType,
                run.resolve_rowmark(rm).markType
            );
            root.rowMarks.retain(|&m| m != rm);
        } else {
            // Shouldn't have inheritance children here: rti == prti.
            debug_assert_eq!(run.resolve_rowmark(rm).rti, run.resolve_rowmark(rm).prti);
            let nm = run.resolve_rowmark_mut(rm);
            nm.rti = keep_relid as types_core::Index;
            nm.prti = keep_relid as types_core::Index;
        }
    }

    // Replace varno in all the query structures (except RangeTblRef).
    crate::change_relids::change_relids_in_query(run, root.parse, ctx)?;

    // C's `simple_rte_array[rti]` aliases `parse->rtable` pointers, so the walk
    // above is automatically reflected there. In this repo `simple_rte_array`
    // holds *cloned* RTEs in a separate run store; re-apply the same relid
    // substitution to those copies so a LATERAL subquery/function/values RTE that
    // referenced the removed relation is rewritten before `set_subquery_pathlist`
    // reads it to derive nestloop params (else: `non-LATERAL parameter required`).
    {
        let simple_rte_array = root.simple_rte_array.clone();
        crate::change_relids::change_relids_in_simple_rte_array(run, &simple_rte_array, ctx);
    }

    // Replace links in the planner info: full relid rename across SpecialJoinInfo
    // sets / semi_rhs_exprs, PlaceHolderVars, all EquivalenceClasses, the
    // attr_needed reset-to-relation-0 floor, and lateral_vars (subst > 0,
    // sjinfo = NULL path of remove_rel_from_query).
    remove_rel_from_query(root, run, to_remove, keep_relid, None, None)
        .expect("remove_rel_from_query (self-join rename)");

    // Replace varno in root targetlist and HAVING (grouping) clause.
    let tlist = root.processed_tlist.clone();
    crate::change_relids::change_relids_in_node_list(mcx, root, &tlist, ctx)?;
    let gclause = root.processed_groupClause.clone();
    crate::change_relids::change_relids_in_node_list(mcx, root, &gclause, ctx)?;

    root.all_result_relids =
        relids::adjust_relid_set(&root.all_result_relids, remove_relid, keep_relid);
    root.leaf_result_relids =
        relids::adjust_relid_set(&root.leaf_result_relids, remove_relid, keep_relid);

    // Remove the rel from the baserel array.
    root.simple_rel_array[remove_relid as usize] = None;

    // Rebuild attr_needed bits from all other sources.
    backend_optimizer_util_joininfo::placeholder::rebuild_placeholder_attr_needed(run.mcx(), root)
        .expect("rebuild_placeholder_attr_needed");
    backend_optimizer_plan_small_seams::rebuild_joinclause_attr_needed::call(root, run);
    backend_optimizer_path_equivclass::rebuild_eclass_attr_needed(run.mcx(), root)
        .expect("rebuild_eclass_attr_needed");
    backend_optimizer_plan_small_seams::rebuild_lateral_attr_needed::call(root, run);
    Ok(())
}

