//! remove_useless_joins / reduce_unique_semijoins / innerrel_is_unique /
//! self-join elimination (analyzejoins.c).

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_pathnodes::{
    EcId, JoinlistNode, RelId, Relids, RinfoId, SpecialJoinInfo, UniqueRelInfo, JOIN_INNER,
    JOIN_LEFT, JOIN_SEMI, RELOPT_BASEREL, RTE_RELATION, RTE_SUBQUERY,
};

use crate::relnode::{
    find_base_rel, pgvec_clone_shallow, relids_add_member, relids_copy, relids_del_member,
    relids_equal, relids_intersect, relids_is_empty, relids_is_member, relids_is_subset,
    relids_members, relids_num_members, relids_singleton, relids_singleton_member, relids_union,
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
    // A PHV needed above the join, or evaluated only at the inner rel, or
    // referencing it (laterally or in its expression), blocks removal.
    let phids = pgvec_clone_shallow(mcx, &run.root.placeholder_list);
    for &phid in phids.iter() {
        let inner_relids = relids_copy(mcx, &run.root.rel(innerrel).relids);
        {
            let ph = run.root.phinfo(phid);
            if crate::relnode::relids_overlap(&ph.ph_lateral, &inner_relids) {
                return Ok(false);
            }
            if !crate::relnode::relids_overlap(&ph.ph_eval_at, &inner_relids) {
                continue;
            }
            if relids_is_subset(&ph.ph_needed, &inputrelids) {
                continue;
            }
            if !relids_is_member(sjinfo.ojrelid as i32, &ph.ph_eval_at) {
                return Ok(false);
            }
            if !crate::relnode::relids_overlap(&sjinfo.min_lefthand, &ph.ph_eval_at) {
                return Ok(false);
            }
        }
        let phexpr = *run.root.expr_node(run.root.phinfo(phid).ph_var_phexpr);
        // C pull_varnos(root, phexpr): a nested PHV contributes its
        // ph_eval_at, not its syntactic phrels.
        let varnos = crate::initsplan::pull_varnos_relids(run, phexpr)?;
        if crate::relnode::relids_overlap(&varnos, &inner_relids) {
            return Ok(false);
        }
    }

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
    rel_is_distinct_for(run, innerrel, &clause_list, None)
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

    // PHVs used at the target rel and/or in the join qual are removed; ones
    // used at partner rels or above the join are updated in place (a partner-
    // rel PHV cannot have the target rel in ph_eval_at).
    {
        let phids = pgvec_clone_shallow(mcx, &run.root.placeholder_list);
        let mut kept: PgVec<'mcx, types_pathnodes::PhInfoId> = PgVec::new_in(mcx);
        for &phid in phids.iter() {
            let (removable, phid_index) = {
                let ph = run.root.phinfo(phid);
                debug_assert!(!relids_is_member(relid, &ph.ph_lateral));
                (
                    relids_is_subset(&ph.ph_needed, &joinrelids)
                        && relids_is_member(relid, &ph.ph_eval_at)
                        && !relids_is_member(ojrelid, &ph.ph_eval_at),
                    ph.phid as usize,
                )
            };
            if removable {
                run.root.placeholder_array[phid_index] = None;
                continue;
            }
            let mut eval_at = adjust_relid_set(run, &run.root.phinfo(phid).ph_eval_at, relid, -1);
            eval_at = adjust_relid_set(run, &eval_at, ojrelid, -1);
            debug_assert!(!relids_is_empty(&eval_at));
            run.root.phinfo_mut(phid).ph_eval_at = eval_at;
            let needed = if relids_is_member(0, &run.root.phinfo(phid).ph_needed) {
                relids_singleton(mcx, 0)
            } else {
                crate::relnode::relids_empty()
            };
            run.root.phinfo_mut(phid).ph_needed = needed;
            let mut lateral = adjust_relid_set(run, &run.root.phinfo(phid).ph_lateral, relid, -1);
            lateral = crate::relnode::relids_difference(
                mcx,
                &lateral,
                &run.root.phinfo(phid).ph_eval_at,
            );
            run.root.phinfo_mut(phid).ph_lateral = lateral;
            let mut phrels = adjust_relid_set(run, &run.root.phinfo(phid).ph_var_phrels, relid, -1);
            phrels = adjust_relid_set(run, &phrels, ojrelid, -1);
            debug_assert!(!relids_is_empty(&phrels));
            run.root.phinfo_mut(phid).ph_var_phrels = phrels;
            let phexpr = *run.root.expr_node(run.root.phinfo(phid).ph_var_phexpr);
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, phexpr, relid, -1, 0)?;
            kept.push(phid);
        }
        run.root.placeholder_list = kept;
    }
    crate::equivclass::remove_rel_from_eclasses(run, relid, ojrelid);

    // Reset attr_needed to only the "relation 0" bits; rebuilt below.
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(other) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(other).relid as usize, rti);
        let n = run.root.rel(other).attr_needed.len();
        for ndx in 0..n {
            let keep = relids_is_member(0, &run.root.rel(other).attr_needed[ndx]);
            run.root.rel_mut(other).attr_needed[ndx] =
                if keep { relids_singleton(mcx, 0) } else { crate::relnode::relids_empty() };
        }
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

    rebuild_placeholder_attr_needed(run);
    rebuild_joinclause_attr_needed(run);
    crate::equivclass::rebuild_eclass_attr_needed(run)?;
    crate::initsplan::rebuild_lateral_attr_needed(run)?;
    Ok(())
}

// rebuild_placeholder_attr_needed (placeholder.c).
fn rebuild_placeholder_attr_needed(run: &mut PlannerRun<'_>) {
    let mcx = run.mcx;
    let phids = pgvec_clone_shallow(mcx, &run.root.placeholder_list);
    for &phid in phids.iter() {
        let phexpr = *run.root.expr_node(run.root.phinfo(phid).ph_var_phexpr);
        let eval_at = relids_copy(mcx, &run.root.phinfo(phid).ph_eval_at);
        let mut vars: PgVec<'_, types_nodes::Node<'_>> = PgVec::new_in(mcx);
        crate::initsplan::pull_var_nodes(phexpr, &mut vars);
        if !vars.is_empty() {
            crate::initsplan::add_vars_to_attr_needed(run, &vars, &eval_at);
        }
    }
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

pub(crate) fn remove_rel_from_restrictinfo(run: &mut PlannerRun<'_>, rid: RinfoId, relid: i32, ojrelid: i32) {
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
        let mut restrictlist = crate::equivclass::generate_join_implied_equalities(
            run,
            &joinrelids,
            &sjinfo.min_lefthand,
            innerrel,
            None,
        )?;
        restrictlist.extend(run.root.rel(innerrel).joininfo.iter().copied());
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
    if rel.rtekind == RTE_RELATION {
        return rel
            .indexlist
            .iter()
            .any(|ind| ind.unique && ind.immediate && ind.indpred.is_empty());
    }
    if rel.rtekind == RTE_SUBQUERY {
        let rte = run.rte(rel.relid as usize);
        return rte.subquery.is_some_and(query_supports_distinctness);
    }
    false
}

pub fn query_supports_distinctness(query: &Query<'_>) -> bool {
    if query.hasTargetSRFs && query.distinctClause.is_nil() {
        return false;
    }
    !query.distinctClause.is_nil()
        || !query.groupClause.is_nil()
        || !query.groupingSets.is_nil()
        || query.hasAggs
        || query.havingQual.is_some()
        || query.setOperations.is_some()
}

fn rel_is_distinct_for<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clause_list: &[RinfoId],
    extra_clauses: Option<&mut PgVec<'mcx, RinfoId>>,
) -> PgResult<bool> {
    if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
        return Ok(false);
    }
    let rtekind = run.root.rel(rel).rtekind;
    if rtekind == RTE_RELATION {
        return crate::indxpath::relation_has_unique_index_ext(
            run,
            rel,
            clause_list,
            &[],
            &[],
            extra_clauses,
        );
    }
    if rtekind == RTE_SUBQUERY {
        let relid = run.root.rel(rel).relid;
        let Some(subquery) = run.rte(relid as usize).subquery else {
            return Ok(false);
        };
        let mut distinct_cols: PgVec<'_, DistinctColInfo> = PgVec::new_in(run.mcx);
        for &rid in clause_list {
            let ri = run.root.rinfo(rid);
            let clause = *run.root.expr_node(ri.clause);
            // The caller's mergejoinability test selected only OpExprs.
            let op = clause.as_op_expr().expect("mergejoinable clause is an OpExpr");
            let side = if ri.outer_is_left { op.args.nth(1) } else { op.args.nth(0) };
            let side = side.as_relabel_type().map_or(side, |r| r.arg);
            let Some(v) = side.as_var() else { continue };
            if v.varno != relid as i32 || v.varlevelsup != 0 {
                continue;
            }
            distinct_cols.push(DistinctColInfo {
                colno: v.varattno,
                opid: op.opno,
                collid: op.inputcollid,
            });
        }
        return query_is_distinct_for_with_collations(subquery, &distinct_cols);
    }
    Ok(false)
}

fn get_sortgroupclause_tle<'mcx>(
    sortgroupref: types_core::Index,
    tlist: &types_nodes::NodeList<'mcx>,
) -> &'mcx types_nodes::primnodes::TargetEntry<'mcx> {
    tlist
        .iter()
        .find_map(|n| {
            let t = n.as_target_entry().expect("tlist cell");
            (t.ressortgroupref == sortgroupref).then_some(t)
        })
        .expect("ORDER/GROUP BY expression not found in targetlist")
}

// DistinctColInfo (analyzejoins.c): a subquery output column the caller needs
// distinctness over, the upper-level equality operator, and its input
// collation.
#[derive(Clone, Copy)]
struct DistinctColInfo {
    colno: i16,
    opid: types_core::Oid,
    collid: types_core::Oid,
}

fn distinct_col_search(colno: i16, distinct_cols: &[DistinctColInfo]) -> Option<DistinctColInfo> {
    distinct_cols.iter().copied().find(|d| d.colno == colno)
}

// query_is_distinct_for (analyzejoins.c): collation-blind wrapper retained
// for external callers; forwards with InvalidOid collations.
pub fn query_is_distinct_for(
    query: &Query<'_>,
    colnos: &[i16],
    opids: &[types_core::Oid],
) -> PgResult<bool> {
    debug_assert!(colnos.len() == opids.len());
    let distinct_cols: Vec<DistinctColInfo> = colnos
        .iter()
        .zip(opids.iter())
        .map(|(&colno, &opid)| DistinctColInfo { colno, opid, collid: types_core::InvalidOid })
        .collect();
    query_is_distinct_for_with_collations(query, &distinct_cols)
}

fn query_is_distinct_for_with_collations(
    query: &Query<'_>,
    distinct_cols: &[DistinctColInfo],
) -> PgResult<bool> {
    // The clause's collation must agree on equality with the collation the
    // subquery deduplicates under, else its distinctness does not carry over.
    let all_match = |clauses: &types_nodes::NodeList<'_>| -> PgResult<bool> {
        for n in clauses {
            let sgc = n.as_sort_group_clause().expect("SortGroupClause cell");
            let tle = get_sortgroupclause_tle(sgc.tleSortGroupRef, &query.targetList);
            let Some(dcinfo) = distinct_col_search(tle.resno, distinct_cols) else {
                return Ok(false);
            };
            if !lsyscache::equality_ops_are_compatible(dcinfo.opid, sgc.eqop)?
                || !lsyscache::collations_agree_on_equality(
                    dcinfo.collid,
                    nodes_core::node_funcs::expr_collation(tle.expr),
                )?
            {
                return Ok(false);
            }
        }
        Ok(true)
    };

    // DISTINCT (including DISTINCT ON) proves uniqueness even with SRFs in
    // the tlist.
    if !query.distinctClause.is_nil() && all_match(&query.distinctClause)? {
        return Ok(true);
    }
    // A tlist SRF can duplicate rows after grouping.
    if query.hasTargetSRFs {
        return Ok(false);
    }
    if !query.groupClause.is_nil() && query.groupingSets.is_nil() {
        if all_match(&query.groupClause)? {
            return Ok(true);
        }
    } else if !query.groupingSets.is_nil() {
        if !query.groupClause.is_nil() {
            return Ok(false);
        }
        // A single empty grouping set returns exactly one row.
        return Ok(query.groupingSets.len() == 1
            && query
                .groupingSets
                .nth(0)
                .as_grouping_set()
                .expect("groupingSets cell")
                .kind
                == types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_EMPTY);
    } else if query.hasAggs || query.havingQual.is_some() {
        return Ok(true);
    }

    if let Some(setop_node) = query.setOperations {
        let topop = setop_node.as_set_operation_stmt().expect("setOperations stmt");
        if !topop.all {
            let mut lg = 0usize;
            let mut matched = true;
            for n in &query.targetList {
                let tle = n.as_target_entry().expect("tlist cell");
                if tle.resjunk {
                    continue;
                }
                let sgc = topop
                    .groupClauses
                    .nth(lg)
                    .as_sort_group_clause()
                    .expect("setop groupClauses cell");
                lg += 1;
                let Some(dcinfo) = distinct_col_search(tle.resno, distinct_cols) else {
                    matched = false;
                    break;
                };
                if !lsyscache::equality_ops_are_compatible(dcinfo.opid, sgc.eqop)?
                    || !lsyscache::collations_agree_on_equality(
                        dcinfo.collid,
                        nodes_core::node_funcs::expr_collation(tle.expr),
                    )?
                {
                    matched = false;
                    break;
                }
            }
            if matched {
                return Ok(true);
            }
        }
    }
    Ok(false)
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
    innerrel_is_unique_ext(
        run,
        joinrelids,
        outerrelids,
        innerrel,
        jointype,
        restrictlist,
        force_cache,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn innerrel_is_unique_ext<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outerrelids: &Relids<'mcx>,
    innerrel: RelId,
    jointype: u32,
    restrictlist: &[RinfoId],
    force_cache: bool,
    mut extra_clauses: Option<&mut PgVec<'mcx, RinfoId>>,
) -> PgResult<bool> {
    let self_join = extra_clauses.is_some();
    if restrictlist.is_empty() {
        return Ok(false);
    }
    if !rel_supports_distinctness(run, innerrel) {
        return Ok(false);
    }
    // A proof for any subset of the outerrel holds for supersets too; a
    // self-join probe used filtered clauses and needs an exact self-join hit.
    for u in run.root.rel(innerrel).unique_for_rels.iter() {
        if (!self_join && relids_is_subset(&u.outerrelids, outerrelids))
            || (self_join && u.self_join && relids_equal(&u.outerrelids, outerrelids))
        {
            if let Some(out) = extra_clauses.as_deref_mut() {
                out.clear();
                out.extend(u.extra_clauses.iter().copied());
            }
            return Ok(true);
        }
    }
    for cached in run.root.rel(innerrel).non_unique_for_rels.iter() {
        if relids_is_subset(outerrelids, cached) {
            return Ok(false);
        }
    }
    let mut outer_exprs: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    if is_innerrel_unique_for(
        run,
        joinrelids,
        outerrelids,
        innerrel,
        jointype,
        restrictlist,
        self_join.then_some(&mut outer_exprs),
    )? {
        let info = UniqueRelInfo {
            outerrelids: relids_copy(run.mcx, outerrelids),
            self_join,
            extra_clauses: pgvec_clone_shallow(run.mcx, &outer_exprs),
        };
        run.root.rel_mut(innerrel).unique_for_rels.push(info);
        if let Some(out) = extra_clauses.as_deref_mut() {
            *out = outer_exprs;
        }
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
    extra_clauses: Option<&mut PgVec<'mcx, RinfoId>>,
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
    rel_is_distinct_for(run, innerrel, &clause_list, extra_clauses)
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

// adjust_relid_set (rewriteManip.c) over planner Relids; subst <= 0 deletes.
fn adjust_relid_set<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &Relids<'mcx>,
    oldrelid: i32,
    subst: i32,
) -> Relids<'mcx> {
    if oldrelid > 0 && relids_is_member(oldrelid, relids) {
        let s = relids_del_member(run.mcx, relids, oldrelid);
        if subst > 0 {
            return relids_add_member(run.mcx, &s, subst as u32);
        }
        return s;
    }
    relids_copy(run.mcx, relids)
}

// replace_relid_callback (analyzejoins.c), RestrictInfo arm. Expression
// sub-walks ride rewrite_manip's SJE walker.
fn replace_relid_rinfo(run: &mut PlannerRun<'_>, rid: RinfoId, from: i32, to: i32) -> PgResult<()> {
    let mcx = run.mcx;
    let (is_req_equal, clause_was_multiple) = {
        let ri = run.root.rinfo(rid);
        (
            relids_equal(&ri.required_relids, &ri.clause_relids),
            relids_num_members(&ri.clause_relids) > 1,
        )
    };
    let touched = {
        let ri = run.root.rinfo(rid);
        relids_is_member(from, &ri.clause_relids) || relids_is_member(from, &ri.required_relids)
    };
    if touched {
        let (clause_id, orclause_id) = {
            let ri = run.root.rinfo(rid);
            (ri.clause, ri.orclause)
        };
        rewrite_manip::ChangeVarNodesExtendedSJE(mcx, *run.root.expr_node(clause_id), from, to, 0)?;
        if let Some(oc) = orclause_id {
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, *run.root.expr_node(oc), from, to, 0)?;
        }
        let new_clause_relids = adjust_relid_set(run, &run.root.rinfo(rid).clause_relids, from, to);
        let delta = relids_num_members(&run.root.rinfo(rid).clause_relids)
            - relids_num_members(&new_clause_relids);
        let new_left = adjust_relid_set(run, &run.root.rinfo(rid).left_relids, from, to);
        let new_right = adjust_relid_set(run, &run.root.rinfo(rid).right_relids, from, to);
        let ri = run.root.rinfo_mut(rid);
        ri.num_base_rels -= delta;
        ri.clause_relids = new_clause_relids;
        ri.left_relids = new_left;
        ri.right_relids = new_right;
    }
    if is_req_equal {
        let c = relids_copy(mcx, &run.root.rinfo(rid).clause_relids);
        run.root.rinfo_mut(rid).required_relids = c;
    } else {
        let v = adjust_relid_set(run, &run.root.rinfo(rid).required_relids, from, to);
        run.root.rinfo_mut(rid).required_relids = v;
    }
    let v = adjust_relid_set(run, &run.root.rinfo(rid).outer_relids, from, to);
    run.root.rinfo_mut(rid).outer_relids = v;
    let v = adjust_relid_set(run, &run.root.rinfo(rid).incompatible_relids, from, to);
    run.root.rinfo_mut(rid).incompatible_relids = v;

    let rewrite_to_nulltest = {
        let ri = run.root.rinfo(rid);
        !ri.mergeopfamilies.is_empty()
            && clause_was_multiple
            && relids_singleton_member(&ri.clause_relids) == Some(to)
            && run.root.expr_node(ri.clause).node_tag() == types_nodes::NodeTag::T_OpExpr
    };
    if rewrite_to_nulltest {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let op = clause.as_op_expr().expect("OpExpr");
        if op.args.len() == 2 {
            let (l, r) = (op.args.nth(0), op.args.nth(1));
            // "t1.a = t2.a" became "t1.a = t1.a": always true where t1.a is
            // not null; replace with a NullTest.
            if types_nodes::equal(l, r) {
                let ntest = types_nodes::Node::mk(
                    mcx,
                    types_nodes::primnodes::NullTest {
                        arg: Some(l),
                        nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                        argisrow: false,
                        location: -1,
                    },
                )?;
                let ntest_id = run.intern_expr(ntest);
                let ri = run.root.rinfo_mut(rid);
                ri.clause = ntest_id;
                ri.mergeopfamilies = PgVec::new_in(mcx);
                ri.left_em = None;
                ri.right_em = None;
                debug_assert!(run.root.rinfo(rid).orclause.is_none());
            }
        }
    }
    Ok(())
}

// restrict_infos_logically_equal (analyzejoins.c): equal() minus rinfo_serial.
fn restrict_infos_logically_equal(run: &PlannerRun<'_>, a: RinfoId, b: RinfoId) -> bool {
    let (ra, rb) = (run.root.rinfo(a), run.root.rinfo(b));
    ra.is_pushed_down == rb.is_pushed_down
        && ra.has_clone == rb.has_clone
        && ra.is_clone == rb.is_clone
        && ra.security_level == rb.security_level
        && relids_equal(&ra.required_relids, &rb.required_relids)
        && relids_equal(&ra.incompatible_relids, &rb.incompatible_relids)
        && relids_equal(&ra.outer_relids, &rb.outer_relids)
        && types_nodes::equal(*run.root.expr_node(ra.clause), *run.root.expr_node(rb.clause))
}

enum KeepList {
    BaseRestrictInfo,
    JoinInfo,
}

// add_non_redundant_clauses (analyzejoins.c): the keep list is re-read from
// the rel each iteration because distribute_restrictinfo_to_rels appends.
fn add_non_redundant_clauses(
    run: &mut PlannerRun<'_>,
    candidates: &[RinfoId],
    to_keep: RelId,
    which: KeepList,
    removed_relid: i32,
) -> PgResult<()> {
    for &rid in candidates {
        debug_assert!(!relids_is_member(removed_relid, &run.root.rinfo(rid).required_relids));
        let mut is_redundant = false;
        let n = match which {
            KeepList::BaseRestrictInfo => run.root.rel(to_keep).baserestrictinfo.len(),
            KeepList::JoinInfo => run.root.rel(to_keep).joininfo.len(),
        };
        for i in 0..n {
            let src = match which {
                KeepList::BaseRestrictInfo => run.root.rel(to_keep).baserestrictinfo[i],
                KeepList::JoinInfo => run.root.rel(to_keep).joininfo[i],
            };
            if !relids_equal(
                &run.root.rinfo(src).clause_relids,
                &run.root.rinfo(rid).clause_relids,
            ) {
                continue;
            }
            let same_parent_ec = {
                let (rs, rr) = (run.root.rinfo(src), run.root.rinfo(rid));
                rr.parent_ec.is_some() && rs.parent_ec == rr.parent_ec
            };
            if src == rid || same_parent_ec || restrict_infos_logically_equal(run, rid, src) {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            crate::initsplan::distribute_restrictinfo_to_rels(run, rid)?;
        }
    }
    Ok(())
}

// update_eclasses (analyzejoins.c).
fn update_eclasses(run: &mut PlannerRun<'_>, ec: EcId, from: i32, to: i32) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(run.root.ec(ec).ec_childmembers.is_empty());

    let members = pgvec_clone_shallow(mcx, &run.root.ec(ec).ec_members);
    let mut new_members: PgVec<'_, types_pathnodes::EmId> = PgVec::new_in(mcx);
    for &em_id in members.iter() {
        if !relids_is_member(from, &run.root.em(em_id).em_relids) {
            new_members.push(em_id);
            continue;
        }
        let new_relids = adjust_relid_set(run, &run.root.em(em_id).em_relids, from, to);
        run.root.em_mut(em_id).em_relids = new_relids;
        let jd = run.root.em(em_id).em_jdomain;
        let new_jd_relids = adjust_relid_set(run, &run.root.join_domains[jd].jd_relids, from, to);
        run.root.join_domains[jd].jd_relids = new_jd_relids;
        let expr = *run.root.expr_node(run.root.em(em_id).em_expr);
        rewrite_manip::ChangeVarNodesExtendedSJE(mcx, expr, from, to, 0)?;

        let mut is_redundant = false;
        for &other in new_members.iter() {
            if !relids_equal(&run.root.em(em_id).em_relids, &run.root.em(other).em_relids) {
                continue;
            }
            let other_expr = *run.root.expr_node(run.root.em(other).em_expr);
            if types_nodes::equal(expr, other_expr) {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            new_members.push(em_id);
        }
    }
    run.root.ec_mut(ec).ec_members = new_members;

    crate::equivclass::ec_clear_derived_clauses(run, ec);

    let sources = pgvec_clone_shallow(mcx, &run.root.ec(ec).ec_sources);
    let mut new_sources: PgVec<'_, RinfoId> = PgVec::new_in(mcx);
    for &rid in sources.iter() {
        if !relids_is_member(from, &run.root.rinfo(rid).required_relids) {
            new_sources.push(rid);
            continue;
        }
        replace_relid_rinfo(run, rid, from, to)?;
        let mut is_redundant = false;
        for &other in new_sources.iter() {
            if !relids_equal(
                &run.root.rinfo(rid).clause_relids,
                &run.root.rinfo(other).clause_relids,
            ) {
                continue;
            }
            if types_nodes::equal(
                *run.root.expr_node(run.root.rinfo(rid).clause),
                *run.root.expr_node(run.root.rinfo(other).clause),
            ) {
                is_redundant = true;
                break;
            }
        }
        if !is_redundant {
            new_sources.push(rid);
        }
    }
    run.root.ec_mut(ec).ec_sources = new_sources;
    let v = adjust_relid_set(run, &run.root.ec(ec).ec_relids, from, to);
    run.root.ec_mut(ec).ec_relids = v;
    Ok(())
}

// remove_rel_from_query (analyzejoins.c), subst > 0 (self-join) form; the
// subst = -1 left-join form is folded into remove_leftjoinrel_from_query.
fn remove_rel_from_query_subst(
    run: &mut PlannerRun<'_>,
    to_remove: RelId,
    subst: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let relid = run.root.rel(to_remove).relid as i32;

    let v = adjust_relid_set(run, &run.root.all_baserels, relid, subst);
    run.root.all_baserels = v;
    let v = adjust_relid_set(run, &run.root.all_query_rels, relid, subst);
    run.root.all_query_rels = v;

    for j in 0..run.root.join_info_list.len() {
        macro_rules! adj {
            ($field:ident) => {
                let v = adjust_relid_set(run, &run.root.join_info_list[j].$field, relid, subst);
                run.root.join_info_list[j].$field = v;
            };
        }
        adj!(min_lefthand);
        adj!(min_righthand);
        adj!(syn_lefthand);
        adj!(syn_righthand);
        let exprs = pgvec_clone_shallow(mcx, &run.root.join_info_list[j].semi_rhs_exprs);
        for &eid in exprs.iter() {
            let e = *run.root.expr_node(eid);
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, e, relid, subst, 0)?;
        }
    }

    // PHVs move to the remaining relation (no removal on the subst path).
    let phids = pgvec_clone_shallow(mcx, &run.root.placeholder_list);
    for &phid in phids.iter() {
        let v = adjust_relid_set(run, &run.root.phinfo(phid).ph_eval_at, relid, subst);
        debug_assert!(!relids_is_empty(&v));
        run.root.phinfo_mut(phid).ph_eval_at = v;
        let needed = if relids_is_member(0, &run.root.phinfo(phid).ph_needed) {
            relids_singleton(mcx, 0)
        } else {
            crate::relnode::relids_empty()
        };
        run.root.phinfo_mut(phid).ph_needed = needed;
        let mut lateral = adjust_relid_set(run, &run.root.phinfo(phid).ph_lateral, relid, subst);
        lateral = crate::relnode::relids_difference(
            mcx,
            &lateral,
            &run.root.phinfo(phid).ph_eval_at,
        );
        run.root.phinfo_mut(phid).ph_lateral = lateral;
        let v = adjust_relid_set(run, &run.root.phinfo(phid).ph_var_phrels, relid, subst);
        run.root.phinfo_mut(phid).ph_var_phrels = v;
        let phexpr = *run.root.expr_node(run.root.phinfo(phid).ph_var_phexpr);
        rewrite_manip::ChangeVarNodesExtendedSJE(mcx, phexpr, relid, subst, 0)?;
    }

    for i in 0..run.root.eq_classes.len() {
        remove_rel_from_eclass_subst(run, EcId(i as u32), relid, subst)?;
    }

    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(other) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(other).relid as usize, rti);
        let n = run.root.rel(other).attr_needed.len();
        for ndx in 0..n {
            let keep = relids_is_member(0, &run.root.rel(other).attr_needed[ndx]);
            run.root.rel_mut(other).attr_needed[ndx] =
                if keep { relids_singleton(mcx, 0) } else { crate::relnode::relids_empty() };
        }
        let lvars = pgvec_clone_shallow(mcx, &run.root.rel(other).lateral_vars);
        for &lv in lvars.iter() {
            let e = *run.root.expr_node(lv);
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, e, relid, subst, 0)?;
        }
    }
    Ok(())
}

// remove_rel_from_eclass (analyzejoins.c), subst form (sjinfo == NULL).
fn remove_rel_from_eclass_subst(
    run: &mut PlannerRun<'_>,
    ec: EcId,
    relid: i32,
    subst: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let v = adjust_relid_set(run, &run.root.ec(ec).ec_relids, relid, subst);
    run.root.ec_mut(ec).ec_relids = v;
    debug_assert!(run.root.ec(ec).ec_childmembers.is_empty());
    let members = pgvec_clone_shallow(mcx, &run.root.ec(ec).ec_members);
    let mut new_members: PgVec<'_, types_pathnodes::EmId> = PgVec::new_in(mcx);
    for &em_id in members.iter() {
        if relids_is_member(relid, &run.root.em(em_id).em_relids) {
            debug_assert!(!run.root.em(em_id).em_is_const);
            let v = adjust_relid_set(run, &run.root.em(em_id).em_relids, relid, subst);
            run.root.em_mut(em_id).em_relids = v;
            if relids_is_empty(&run.root.em(em_id).em_relids) {
                continue;
            }
        }
        new_members.push(em_id);
    }
    run.root.ec_mut(ec).ec_members = new_members;
    let sources = pgvec_clone_shallow(mcx, &run.root.ec(ec).ec_sources);
    for &rid in sources.iter() {
        replace_relid_rinfo(run, rid, relid, subst)?;
    }
    crate::equivclass::ec_clear_derived_clauses(run, ec);
    Ok(())
}

// remove_self_join_rel (analyzejoins.c).
#[allow(clippy::too_many_arguments)]
fn remove_self_join_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    kmark: Option<usize>,
    rmark: Option<usize>,
    to_keep: RelId,
    to_remove: RelId,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    let mcx = run.mcx;
    let keep_relid = run.root.rel(to_keep).relid as i32;
    let remove_relid = run.root.rel(to_remove).relid as i32;
    debug_assert!(keep_relid > 0 && remove_relid > 0);

    let mut jinfo_candidates: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let mut binfo_candidates: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);

    let joininfos = pgvec_clone_shallow(mcx, &run.root.rel(to_remove).joininfo);
    for &rid in joininfos.iter() {
        remove_join_clause_from_rels(run, rid);
        replace_relid_rinfo(run, rid, remove_relid, keep_relid)?;
        if relids_num_members(&run.root.rinfo(rid).required_relids) > 1 {
            jinfo_candidates.push(rid);
        } else {
            binfo_candidates.push(rid);
        }
    }

    let mut brestrict = pgvec_clone_shallow(mcx, &run.root.rel(to_remove).baserestrictinfo);
    brestrict.extend(restrictlist.iter().copied());
    for &rid in brestrict.iter() {
        replace_relid_rinfo(run, rid, remove_relid, keep_relid)?;
        if relids_num_members(&run.root.rinfo(rid).required_relids) > 1 {
            jinfo_candidates.push(rid);
        } else {
            binfo_candidates.push(rid);
        }
    }

    add_non_redundant_clauses(
        run,
        &binfo_candidates,
        to_keep,
        KeepList::BaseRestrictInfo,
        remove_relid,
    )?;
    add_non_redundant_clauses(run, &jinfo_candidates, to_keep, KeepList::JoinInfo, remove_relid)?;

    let ec_indexes = relids_copy(mcx, &run.root.rel(to_remove).eclass_indexes);
    for i in relids_members(&ec_indexes) {
        update_eclasses(run, EcId(i as u32), remove_relid, keep_relid)?;
        let v = relids_add_member(mcx, &run.root.rel(to_keep).eclass_indexes, i as u32);
        run.root.rel_mut(to_keep).eclass_indexes = v;
    }

    // Transfer the targetlist and attr_needed flags.
    {
        let remove_pt = run.rel_reltarget_id(to_remove);
        let keep_pt = run.rel_reltarget_id(to_keep);
        let exprs = pgvec_clone_shallow(mcx, &run.root.pathtarget(remove_pt).exprs);
        for &eid in exprs.iter() {
            let e = *run.root.expr_node(eid);
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, e, remove_relid, keep_relid, 0)?;
            let member = run
                .root
                .pathtarget(keep_pt)
                .exprs
                .iter()
                .any(|&k| types_nodes::equal(*run.root.expr_node(k), e));
            if !member {
                debug_assert!(run.root.pathtarget(keep_pt).sortgrouprefs.is_empty());
                run.root.pathtarget_mut(keep_pt).exprs.push(eid);
            }
        }
    }
    {
        debug_assert_eq!(
            run.root.rel(to_keep).attr_needed.len(),
            run.root.rel(to_remove).attr_needed.len()
        );
        let n = run.root.rel(to_keep).attr_needed.len();
        for ndx in 0..n {
            let adjusted = adjust_relid_set(
                run,
                &run.root.rel(to_remove).attr_needed[ndx],
                remove_relid,
                keep_relid,
            );
            run.root.rel_mut(to_remove).attr_needed[ndx] = relids_copy(mcx, &adjusted);
            let merged = relids_union(mcx, &run.root.rel(to_keep).attr_needed[ndx], &adjusted);
            run.root.rel_mut(to_keep).attr_needed[ndx] = merged;
        }
    }

    if let Some(rpos) = rmark {
        if kmark.is_some() {
            let rid = run.root.rowMarks[rpos];
            let kid = run.root.rowMarks[kmark.unwrap()];
            debug_assert!(
                run.rowmarks[kid.0 as usize].markType == run.rowmarks[rid.0 as usize].markType
            );
            run.root.rowMarks.remove(rpos);
        } else {
            let rid = run.root.rowMarks[rpos];
            let rm = &mut run.rowmarks[rid.0 as usize];
            debug_assert_eq!(rm.rti, rm.prti);
            rm.rti = keep_relid as u32;
            rm.prti = keep_relid as u32;
        }
    }

    // Replace varno in the parse tree (RangeTblRefs excluded so
    // remove_rel_from_joinlist can still find them).
    rewrite_manip::ChangeVarNodesExtendedSJEQueryRef(mcx, run.parse(), remove_relid, keep_relid)?;

    remove_rel_from_query_subst(run, to_remove, keep_relid)?;

    if let Some(tlist) = run.processed_tlist {
        for n in tlist {
            rewrite_manip::ChangeVarNodesExtendedSJE(mcx, n, remove_relid, keep_relid, 0)?;
        }
    }
    // processed_groupClause: SortGroupClauses carry no rangetable indexes
    // (C's walk over them is structurally a no-op).
    // C calls adjust_relid_set on all_result_relids/leaf_result_relids but
    // discards the results; mirrored as a no-op.

    run.root.simple_rel_array[remove_relid as usize] = None;
    run.root.simple_rte_array[remove_relid as usize] = types_pathnodes::RangeTblEntryId::Invalid;

    crate::placeholder::rebuild_placeholder_attr_needed(run)?;
    rebuild_joinclause_attr_needed(run);
    crate::equivclass::rebuild_eclass_attr_needed(run)?;
    crate::initsplan::rebuild_lateral_attr_needed(run)?;
    Ok(())
}

// split_selfjoin_quals (analyzejoins.c).
fn split_selfjoin_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinquals: &[RinfoId],
) -> PgResult<(PgVec<'mcx, RinfoId>, PgVec<'mcx, RinfoId>)> {
    let mcx = run.mcx;
    let mut sjoinquals: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let mut ojoinquals: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    for &rid in joinquals {
        let (mergeable, two_rels, left_single, right_single) = {
            let ri = run.root.rinfo(rid);
            (
                !ri.mergeopfamilies.is_empty(),
                relids_num_members(&ri.clause_relids) == 2,
                relids_num_members(&ri.left_relids) == 1,
                relids_num_members(&ri.right_relids) == 1,
            )
        };
        if !mergeable || !two_rels || !left_single || !right_single {
            ojoinquals.push(rid);
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let Some(op) = clause.as_op_expr() else {
            ojoinquals.push(rid);
            continue;
        };
        if op.args.len() != 2 {
            ojoinquals.push(rid);
            continue;
        }
        let mut leftexpr = op.args.nth(0);
        let mut rightexpr = copyfuncs::copy_object(mcx, op.args.nth(1))?;
        if let Some(r) = leftexpr.as_relabel_type() {
            leftexpr = r.arg;
        }
        if let Some(r) = rightexpr.as_relabel_type() {
            rightexpr = r.arg;
        }
        let (from, to) = {
            let ri = run.root.rinfo(rid);
            (
                relids_singleton_member(&ri.right_relids).expect("singleton"),
                relids_singleton_member(&ri.left_relids).expect("singleton"),
            )
        };
        rewrite_manip::ChangeVarNodesExtendedSJE(mcx, rightexpr, from, to, 0)?;
        if types_nodes::equal(leftexpr, rightexpr) {
            sjoinquals.push(rid);
        } else {
            ojoinquals.push(rid);
        }
    }
    Ok((sjoinquals, ojoinquals))
}

// match_unique_clauses (analyzejoins.c): baserestrictinfo-derived uniqueness
// clauses must match equivalently on both sides of the self join.
fn match_unique_clauses(
    run: &mut PlannerRun<'_>,
    outer: RelId,
    uclauses: &[RinfoId],
    relid: i32,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let outer_relid = run.root.rel(outer).relid as i32;
    for &rid in uclauses {
        debug_assert!(outer_relid > 0 && relid > 0);
        let left_empty = relids_is_empty(&run.root.rinfo(rid).left_relids);
        debug_assert!(left_empty ^ relids_is_empty(&run.root.rinfo(rid).right_relids));

        let clause =
            copyfuncs::copy_object(mcx, *run.root.expr_node(run.root.rinfo(rid).clause))?;
        rewrite_manip::ChangeVarNodesExtendedSJE(mcx, clause, relid, outer_relid, 0)?;
        let op = clause.as_op_expr().expect("mergejoinable clause is an OpExpr");
        let (iclause, c1) = if left_empty {
            (op.args.nth(1), op.args.nth(0))
        } else {
            (op.args.nth(0), op.args.nth(1))
        };

        let mut matched = false;
        let brestrict = pgvec_clone_shallow(mcx, &run.root.rel(outer).baserestrictinfo);
        for &orid in brestrict.iter() {
            if run.root.rinfo(orid).mergeopfamilies.is_empty() {
                continue;
            }
            let oclause_node = *run.root.expr_node(run.root.rinfo(orid).clause);
            let oop = oclause_node.as_op_expr().expect("mergejoinable clause is an OpExpr");
            let oleft_empty = relids_is_empty(&run.root.rinfo(orid).left_relids);
            let (oclause, c2) = if oleft_empty {
                (oop.args.nth(1), oop.args.nth(0))
            } else {
                (oop.args.nth(0), oop.args.nth(1))
            };
            if types_nodes::equal(iclause, oclause) && types_nodes::equal(c1, c2) {
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

// remove_self_joins_one_group (analyzejoins.c): try each (removed, kept) pair
// of same-Oid baserels.
fn remove_self_joins_one_group<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relids: &Relids<'mcx>,
) -> PgResult<Relids<'mcx>> {
    let mcx = run.mcx;
    let mut result: Relids<'mcx> = crate::relnode::relids_empty();
    let members: PgVec<'_, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend(relids_members(relids));
        v
    };
    'outer: for (ri_, &r) in members.iter().enumerate() {
        let rrel = find_base_rel(&run.root, r);
        for &k in members[ri_ + 1..].iter() {
            let krel = find_base_rel(&run.root, k);
            debug_assert_eq!(run.rte(k as usize).relid, run.rte(r as usize).relid);

            // Different join-order rules for the pair defeat the merge.
            let mut jinfo_check = true;
            for info in run.root.join_info_list.iter() {
                if (relids_is_member(k, &info.syn_lefthand)
                    != relids_is_member(r, &info.syn_lefthand))
                    || (relids_is_member(k, &info.syn_righthand)
                        != relids_is_member(r, &info.syn_righthand))
                {
                    jinfo_check = false;
                    break;
                }
            }
            if !jinfo_check {
                continue;
            }

            let mut kmark: Option<usize> = None;
            let mut rmark: Option<usize> = None;
            for (pos, &rmid) in run.root.rowMarks.iter().enumerate() {
                let rti = run.rowmarks[rmid.0 as usize].rti as i32;
                if rti == r {
                    debug_assert!(rmark.is_none());
                    rmark = Some(pos);
                } else if rti == k {
                    debug_assert!(kmark.is_none());
                    kmark = Some(pos);
                }
                if kmark.is_some() && rmark.is_some() {
                    break;
                }
            }
            if let (Some(kp), Some(rp)) = (kmark, rmark) {
                let (kid, rid) = (run.root.rowMarks[kp], run.root.rowMarks[rp]);
                if run.rowmarks[kid.0 as usize].markType != run.rowmarks[rid.0 as usize].markType
                {
                    continue;
                }
            }

            let mut joinrelids: Relids<'mcx> = crate::relnode::relids_empty();
            joinrelids = relids_add_member(mcx, &joinrelids, r as u32);
            joinrelids = relids_add_member(mcx, &joinrelids, k as u32);

            let rrel_relids = relids_copy(mcx, &run.root.rel(rrel).relids);
            let restrictlist = crate::equivclass::generate_join_implied_equalities(
                run,
                &joinrelids,
                &rrel_relids,
                krel,
                None,
            )?;
            if restrictlist.is_empty() {
                continue;
            }

            let (mut selfjoinquals, otherjoinquals) = split_selfjoin_quals(run, &restrictlist)?;
            debug_assert_eq!(
                restrictlist.len(),
                selfjoinquals.len() + otherjoinquals.len()
            );
            // The degenerate case (no self-join quals) still works if both
            // sides bear the same baserestrictinfo clause.
            selfjoinquals.extend(run.root.rel(krel).baserestrictinfo.iter().copied());

            let mut uclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
            if !innerrel_is_unique_ext(
                run,
                &joinrelids,
                &rrel_relids,
                krel,
                JOIN_INNER,
                &selfjoinquals,
                otherjoinquals.is_empty(),
                Some(&mut uclauses),
            )? {
                continue;
            }

            if !match_unique_clauses(run, rrel, &uclauses, run.root.rel(krel).relid as i32)? {
                continue;
            }

            remove_self_join_rel(run, kmark, rmark, krel, rrel, &restrictlist)?;
            result = relids_add_member(mcx, &result, r as u32);
            continue 'outer;
        }
    }
    Ok(result)
}

// remove_self_joins_recurse (analyzejoins.c).
fn remove_self_joins_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinlist: &[JoinlistNode<'mcx>],
    mut to_remove: Relids<'mcx>,
) -> PgResult<Relids<'mcx>> {
    let mcx = run.mcx;
    let mut relids: Relids<'mcx> = crate::relnode::relids_empty();
    for jl in joinlist {
        match jl {
            JoinlistNode::Rel(varno) => {
                let varno = *varno;
                let rte = run.rte(varno as usize);
                let parse = run.parse();
                if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
                    && rte.relkind == types_rel::RELKIND_RELATION
                    && rte.tablesample.is_none()
                    && varno != parse.resultRelation
                    && varno != parse.mergeTargetRelation
                {
                    debug_assert!(!relids_is_member(varno, &relids));
                    relids = relids_add_member(mcx, &relids, varno as u32);
                }
            }
            JoinlistNode::Sub(sub) => {
                to_remove = remove_self_joins_recurse(run, sub, to_remove)?;
            }
        }
    }

    let num_rels = relids_num_members(&relids) as usize;
    if num_rels < 2 {
        return Ok(to_remove);
    }

    let mut candidates: PgVec<'_, (i32, types_core::Oid)> = PgVec::new_in(mcx);
    for i in relids_members(&relids) {
        candidates.push((i, run.rte(i as usize).relid));
    }
    candidates.sort_by_key(|&(_, reloid)| reloid);

    let mut i = 0usize;
    for j in 1..=num_rels {
        if j == num_rels || candidates[j].1 != candidates[i].1 {
            if j - i >= 2 {
                let mut group: Relids<'mcx> = crate::relnode::relids_empty();
                while i < j {
                    group = relids_add_member(mcx, &group, candidates[i].0 as u32);
                    i += 1;
                }
                loop {
                    debug_assert!(!crate::relnode::relids_overlap(&group, &to_remove));
                    let removed = remove_self_joins_one_group(run, &group)?;
                    to_remove = relids_union(mcx, &to_remove, &removed);
                    group = crate::relnode::relids_difference(mcx, &group, &removed);
                    if relids_is_empty(&removed) || relids_num_members(&group) <= 1 {
                        break;
                    }
                }
            } else {
                i = j;
            }
        }
    }
    Ok(to_remove)
}

// remove_useless_self_joins (analyzejoins.c).
pub fn remove_useless_self_joins<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinlist: PgVec<'mcx, JoinlistNode<'mcx>>,
) -> PgResult<PgVec<'mcx, JoinlistNode<'mcx>>> {
    if !crate::gucs::enable_self_join_elimination()
        || joinlist.is_empty()
        || (joinlist.len() == 1 && !matches!(joinlist[0], JoinlistNode::Sub(_)))
    {
        return Ok(joinlist);
    }

    let to_remove = remove_self_joins_recurse(run, &joinlist, crate::relnode::relids_empty())?;

    let mut joinlist = joinlist;
    for relid in relids_members(&to_remove) {
        let mut nremoved = 0;
        joinlist = remove_rel_from_joinlist(run, joinlist, relid, &mut nremoved);
        assert!(nremoved == 1, "failed to find relation {relid} in joinlist");
    }
    Ok(joinlist)
}
