//! remove_useless_joins / reduce_unique_semijoins / innerrel_is_unique
//! (analyzejoins.c); self-join elimination is the SJE lane.

use mcx::PgVec;
use types_error::PgResult;
use types_pathnodes::{
    JoinlistNode, RelId, Relids, RinfoId, SpecialJoinInfo, UniqueRelInfo, JOIN_LEFT, JOIN_SEMI,
    RELOPT_BASEREL, RTE_RELATION,
};

use crate::relnode::{
    find_base_rel, pgvec_clone_shallow, relids_add_member, relids_copy, relids_del_member,
    relids_intersect, relids_is_member, relids_is_subset, relids_num_members,
    relids_singleton, relids_singleton_member, relids_union,
};
use crate::run::PlannerRun;

pub fn remove_useless_joins<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut joinlist: PgVec<'mcx, JoinlistNode<'mcx>>,
) -> PgResult<PgVec<'mcx, JoinlistNode<'mcx>>> {
    'restart: loop {
        for i in 0..run.root.join_info_list.len() {
            let sjinfo = run.root.join_info_list[i].clone();
            if !join_is_removable(run, &sjinfo)? {
                continue;
            }
            let innerrelid =
                relids_singleton_member(&sjinfo.min_righthand).expect("single baserel");
            remove_leftjoinrel_from_query(run, innerrelid, &sjinfo)?;
            let mut nremoved = 0;
            joinlist = remove_rel_from_joinlist(run, joinlist, innerrelid, &mut nremoved);
            assert!(nremoved == 1, "failed to find relation {innerrelid} in joinlist");
            run.root.join_info_list.remove(i);
            continue 'restart;
        }
        return Ok(joinlist);
    }
}

fn join_is_removable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<bool> {
    if sjinfo.jointype != JOIN_LEFT {
        return Ok(false);
    }
    let Some(innerrelid) = relids_singleton_member(&sjinfo.min_righthand) else {
        return Ok(false);
    };
    // MERGE can left-join to the query result rel.
    if innerrelid == run.parse().resultRelation {
        return Ok(false);
    }
    let innerrel = find_base_rel(&run.root, innerrelid);
    if !rel_supports_distinctness(run, innerrel) {
        return Ok(false);
    }
    let mcx = run.mcx;
    let inputrelids = relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand);
    debug_assert!(sjinfo.ojrelid != 0);
    let joinrelids = relids_add_member(mcx, &inputrelids, sjinfo.ojrelid);

    // "Above" includes pushed-down conditions: compare against inputrelids
    // (without ojrelid), not joinrelids.
    {
        let rel = run.root.rel(innerrel);
        if rel.attr_needed.iter().any(|a| !relids_is_subset(a, &inputrelids)) {
            return Ok(false);
        }
    }
    debug_assert!(run.root.placeholder_list.is_empty());

    let joininfo = pgvec_clone_shallow(mcx, &run.root.rel(innerrel).joininfo);
    let inner_relids = relids_copy(mcx, &run.root.rel(innerrel).relids);
    let mut clause_list: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    for &rid in joininfo.iter() {
        if run.root.rinfo(rid).is_clone {
            continue;
        }
        if crate::joinrels::rinfo_is_pushed_down(run, rid, &joinrelids) {
            continue;
        }
        {
            let ri = run.root.rinfo(rid);
            if !ri.can_join || ri.mergeopfamilies.is_empty() {
                continue;
            }
        }
        if !clause_sides_match_join(run, rid, &sjinfo.min_lefthand, &inner_relids) {
            continue;
        }
        clause_list.push(rid);
    }
    rel_is_distinct_for(run, innerrel, &clause_list)
}

fn remove_leftjoinrel_from_query<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relid: i32,
    sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let rel = find_base_rel(&run.root, relid);
    let ojrelid = sjinfo.ojrelid as i32;
    debug_assert!(ojrelid != 0);
    let inputrelids = relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand);
    let joinrelids = relids_add_member(mcx, &inputrelids, sjinfo.ojrelid);

    // remove_rel_from_query, subst = -1 (left-join removal) arm.
    run.root.all_baserels = relids_del_member(mcx, &run.root.all_baserels, relid);
    run.root.all_query_rels = relids_del_member(mcx, &run.root.all_query_rels, relid);
    run.root.outer_join_rels = relids_del_member(mcx, &run.root.outer_join_rels, ojrelid);
    run.root.all_query_rels = relids_del_member(mcx, &run.root.all_query_rels, ojrelid);

    for j in 0..run.root.join_info_list.len() {
        macro_rules! strip {
            ($field:ident, $x:expr) => {
                let v = relids_del_member(mcx, &run.root.join_info_list[j].$field, $x);
                run.root.join_info_list[j].$field = v;
            };
        }
        strip!(min_lefthand, relid);
        strip!(min_righthand, relid);
        strip!(syn_lefthand, relid);
        strip!(syn_righthand, relid);
        strip!(min_lefthand, ojrelid);
        strip!(min_righthand, ojrelid);
        strip!(syn_lefthand, ojrelid);
        strip!(syn_righthand, ojrelid);
        // relid cannot appear in the commute sets, but ojrelid can.
        strip!(commute_above_l, ojrelid);
        strip!(commute_above_r, ojrelid);
        strip!(commute_below_l, ojrelid);
        strip!(commute_below_r, ojrelid);
    }

    debug_assert!(run.root.placeholder_list.is_empty());
    // Pathkey ECs (single-member, from qp_callback) never reference a
    // removable rel or its ojrelid; a referencing EC is the unported
    // remove_rel_from_eclass arm.
    for ec in run.root.eq_classes.iter() {
        assert!(
            !relids_is_member(relid, &ec.ec_relids)
                && !relids_is_member(ojrelid, &ec.ec_relids),
            "remove_rel_from_eclass (analyzejoins.c): EC references removed rel; eclass lane"
        );
    }

    // Reset attr_needed to only the "relation 0" bits; rebuilt below.
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(other) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(other).relid as usize, rti);
        let n = run.root.rel(other).attr_needed.len();
        for ndx in 0..n {
            let keep = relids_is_member(0, &run.root.rel(other).attr_needed[ndx]);
            run.root.rel_mut(other).attr_needed[ndx] =
                if keep { relids_singleton(mcx, 0) } else { None };
        }
        debug_assert!(run.root.rel(other).lateral_vars.is_empty());
    }

    // Clones of deletable quals carry commutable OJs' relids; test
    // pushed-down-ness against the commute-augmented set to drop them too.
    let join_plus_commute = {
        let t = relids_union(mcx, &joinrelids, &sjinfo.commute_above_r);
        relids_union(mcx, &t, &sjinfo.commute_below_l)
    };
    let joininfos = pgvec_clone_shallow(mcx, &run.root.rel(rel).joininfo);
    for &rid in joininfos.iter() {
        remove_join_clause_from_rels(run, rid);
        if crate::joinrels::rinfo_is_pushed_down(run, rid, &join_plus_commute) {
            remove_rel_from_restrictinfo(run, rid, relid, ojrelid);
            crate::initsplan::distribute_restrictinfo_to_rels(run, rid)?;
        }
    }

    run.root.simple_rel_array[relid as usize] = None;
    run.root.simple_rte_array[relid as usize] = types_pathnodes::RangeTblEntryId::Invalid;

    // rebuild_placeholder/eclass/lateral_attr_needed: no PHVs, no ECs, no
    // lateral refs exist on this lane (asserted above).
    rebuild_joinclause_attr_needed(run);
    Ok(())
}

// remove_join_clause_from_rels (joininfo.c).
fn remove_join_clause_from_rels(run: &mut PlannerRun<'_>, rid: RinfoId) {
    let required = relids_copy(run.mcx, &run.root.rinfo(rid).required_relids);
    for cur_relid in crate::relnode::relids_members(&required) {
        let Some(rel) = run
            .root
            .simple_rel_array
            .get(cur_relid as usize)
            .copied()
            .flatten()
        else {
            continue;
        };
        let pos = run.root.rel(rel).joininfo.iter().position(|&x| x == rid);
        if let Some(pos) = pos {
            run.root.rel_mut(rel).joininfo.remove(pos);
        }
    }
}

fn remove_rel_from_restrictinfo(run: &mut PlannerRun<'_>, rid: RinfoId, relid: i32, ojrelid: i32) {
    let mcx = run.mcx;
    let mut v = relids_del_member(mcx, &run.root.rinfo(rid).clause_relids, relid);
    v = relids_del_member(mcx, &v, ojrelid);
    run.root.rinfo_mut(rid).clause_relids = v;
    let mut v = relids_del_member(mcx, &run.root.rinfo(rid).required_relids, relid);
    v = relids_del_member(mcx, &v, ojrelid);
    run.root.rinfo_mut(rid).required_relids = v;
    // OR clauses carry no sub-RestrictInfos here (make_restrictinfo
    // divergence: orclause stays None), so C's recursion has nothing to fix.
    debug_assert!(run.root.rinfo(rid).orclause.is_none());
}

fn remove_rel_from_joinlist<'mcx>(
    run: &PlannerRun<'mcx>,
    joinlist: PgVec<'mcx, JoinlistNode<'mcx>>,
    relid: i32,
    nremoved: &mut i32,
) -> PgVec<'mcx, JoinlistNode<'mcx>> {
    let mut result: PgVec<'mcx, JoinlistNode<'mcx>> = PgVec::new_in(run.mcx);
    for jl in joinlist {
        match jl {
            JoinlistNode::Rel(varno) => {
                if varno == relid {
                    *nremoved += 1;
                } else {
                    result.push(JoinlistNode::Rel(varno));
                }
            }
            JoinlistNode::Sub(sub) => {
                let sublist = remove_rel_from_joinlist(run, sub, relid, nremoved);
                if !sublist.is_empty() {
                    result.push(JoinlistNode::Sub(sublist));
                }
            }
        }
    }
    result
}

// rebuild_joinclause_attr_needed (initsplan.c): repeat the attr_needed
// construction from all surviving join clauses.
fn rebuild_joinclause_attr_needed(run: &mut PlannerRun<'_>) {
    let mcx = run.mcx;
    let mut seen_serials: PgVec<'_, i32> = PgVec::new_in(mcx);
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(brel) = run.root.simple_rel_array[rti] else { continue };
        if run.root.rel(brel).reloptkind != RELOPT_BASEREL {
            continue;
        }
        let joininfo = pgvec_clone_shallow(mcx, &run.root.rel(brel).joininfo);
        for &rid in joininfo.iter() {
            let (serial, is_clone) = {
                let ri = run.root.rinfo(rid);
                (ri.rinfo_serial, ri.is_clone)
            };
            if !is_clone {
                if seen_serials.contains(&serial) {
                    continue;
                }
                seen_serials.push(serial);
            }
            let relids = relids_copy(mcx, &run.root.rinfo(rid).required_relids);
            if relids_num_members(&relids) > 1 {
                let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
                let mut vars: PgVec<'_, types_nodes::Node<'_>> = PgVec::new_in(mcx);
                crate::initsplan::pull_var_nodes(clause, &mut vars);
                let where_needed = if is_clone {
                    relids_intersect(mcx, &relids, &run.root.all_baserels)
                } else {
                    relids
                };
                crate::initsplan::add_vars_to_attr_needed(run, &vars, &where_needed);
            }
        }
    }
}

pub fn reduce_unique_semijoins(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mut i = 0;
    while i < run.root.join_info_list.len() {
        let sjinfo = run.root.join_info_list[i].clone();
        if sjinfo.jointype != JOIN_SEMI {
            i += 1;
            continue;
        }
        let Some(innerrelid) = relids_singleton_member(&sjinfo.min_righthand) else {
            i += 1;
            continue;
        };
        let innerrel = find_base_rel(&run.root, innerrelid);
        if !rel_supports_distinctness(run, innerrel) {
            i += 1;
            continue;
        }
        let joinrelids = relids_union(run.mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand);
        debug_assert!(sjinfo.ojrelid == 0);
        // Eclass-lite keeps join equalities in joininfo, so
        // generate_join_implied_equalities contributes nothing extra.
        let restrictlist = pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).joininfo);
        if !innerrel_is_unique(
            run,
            &joinrelids,
            &sjinfo.min_lefthand,
            innerrel,
            JOIN_SEMI,
            &restrictlist,
            true,
        )? {
            i += 1;
            continue;
        }
        run.root.join_info_list.remove(i);
    }
    Ok(())
}

fn rel_supports_distinctness(run: &PlannerRun<'_>, rel: RelId) -> bool {
    let rel = run.root.rel(rel);
    if rel.reloptkind != RELOPT_BASEREL {
        return false;
    }
    // C divergence: the RTE_SUBQUERY arm (query_supports_distinctness) is
    // unported — a provably-distinct subquery RHS keeps its join / stays
    // inner_unique=false where C could prove it.
    rel.rtekind == RTE_RELATION
        && rel
            .indexlist
            .iter()
            .any(|ind| ind.unique && ind.immediate && ind.indpred.is_empty())
}

fn rel_is_distinct_for(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    clause_list: &[RinfoId],
) -> PgResult<bool> {
    if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
        return Ok(false);
    }
    if run.root.rel(rel).rtekind != RTE_RELATION {
        return Ok(false);
    }
    crate::pathnode::relation_has_unique_index_for(run, rel, clause_list, &[], &[])
}

#[allow(clippy::too_many_arguments)]
pub fn innerrel_is_unique<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outerrelids: &Relids<'mcx>,
    innerrel: RelId,
    jointype: u32,
    restrictlist: &[RinfoId],
    force_cache: bool,
) -> PgResult<bool> {
    if restrictlist.is_empty() {
        return Ok(false);
    }
    if !rel_supports_distinctness(run, innerrel) {
        return Ok(false);
    }
    // A proof for any subset of the outerrel holds for supersets too.
    for u in run.root.rel(innerrel).unique_for_rels.iter() {
        if !u.self_join && relids_is_subset(&u.outerrelids, outerrelids) {
            return Ok(true);
        }
    }
    for cached in run.root.rel(innerrel).non_unique_for_rels.iter() {
        if relids_is_subset(outerrelids, cached) {
            return Ok(false);
        }
    }
    if is_innerrel_unique_for(run, joinrelids, outerrelids, innerrel, jointype, restrictlist)? {
        let info = UniqueRelInfo {
            outerrelids: relids_copy(run.mcx, outerrelids),
            self_join: false,
            extra_clauses: PgVec::new_in(run.mcx),
        };
        run.root.rel_mut(innerrel).unique_for_rels.push(info);
        Ok(true)
    } else {
        // Negative caching pays only outside the bottom-up join search
        // (join_search_private is always None here).
        if force_cache {
            let c = relids_copy(run.mcx, outerrelids);
            run.root.rel_mut(innerrel).non_unique_for_rels.push(c);
        }
        Ok(false)
    }
}

fn is_innerrel_unique_for<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outerrelids: &Relids<'mcx>,
    innerrel: RelId,
    jointype: u32,
    restrictlist: &[RinfoId],
) -> PgResult<bool> {
    let mcx = run.mcx;
    let inner_relids = relids_copy(mcx, &run.root.rel(innerrel).relids);
    let mut clause_list: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    for &rid in restrictlist {
        if crate::joinpath::is_outer_join(jointype)
            && crate::joinrels::rinfo_is_pushed_down(run, rid, joinrelids)
        {
            continue;
        }
        {
            let ri = run.root.rinfo(rid);
            if !ri.can_join || ri.mergeopfamilies.is_empty() {
                continue;
            }
        }
        if !clause_sides_match_join(run, rid, outerrelids, &inner_relids) {
            continue;
        }
        clause_list.push(rid);
    }
    rel_is_distinct_for(run, innerrel, &clause_list)
}

// clause_sides_match_join (paths.h): sets the transient outer_is_left flag.
pub(crate) fn clause_sides_match_join(
    run: &mut PlannerRun<'_>,
    rid: RinfoId,
    outerrelids: &Relids<'_>,
    innerrelids: &Relids<'_>,
) -> bool {
    let (left, right) = {
        let ri = run.root.rinfo(rid);
        (ri.left_relids.clone(), ri.right_relids.clone())
    };
    if relids_is_subset(&left, outerrelids) && relids_is_subset(&right, innerrelids) {
        run.root.rinfo_mut(rid).outer_is_left = true;
        true
    } else if relids_is_subset(&left, innerrelids) && relids_is_subset(&right, outerrelids) {
        run.root.rinfo_mut(rid).outer_is_left = false;
        true
    } else {
        false
    }
}
