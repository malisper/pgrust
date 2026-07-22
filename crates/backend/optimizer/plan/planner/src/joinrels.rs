//! joinrels.c + allpaths.c join-search slice: two-baserel INNER/LEFT/SEMI/
//! ANTI joins.

use mcx::PgVec;
use types_error::PgResult;
use types_pathnodes::{
    JoinlistNode, RelId, Relids, SpecialJoinInfo, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    RELOPT_JOINREL,
};

use crate::relnode::{
    find_base_rel, relids_add_member, relids_copy, relids_equal, relids_is_member,
    relids_is_subset, relids_overlap, relids_union,
};
use crate::run::PlannerRun;
pub use types_pathnodes::run::{init_dummy_sjinfo, rinfo_is_pushed_down};
use crate::costsize::set_joinrel_size_estimates;

const RTE_JOIN: u32 = types_nodes::parsenodes::RTEKind::RTE_JOIN as u32;

pub fn make_rel_from_joinlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinlist: &[JoinlistNode<'mcx>],
) -> PgResult<RelId> {
    let levels_needed = joinlist.len();
    debug_assert!(levels_needed > 0);
    let mut initial_rels: PgVec<'mcx, RelId> = PgVec::new_in(run.mcx);
    for jl in joinlist {
        match jl {
            JoinlistNode::Rel(varno) => initial_rels.push(find_base_rel(&run.root, *varno)),
            // A sub-joinlist (a forced FULL-join pair, or collapse-limit
            // grouping) is planned as its own subproblem.
            JoinlistNode::Sub(sub) => initial_rels.push(make_rel_from_joinlist(run, sub)?),
        }
    }
    if levels_needed == 1 {
        return Ok(initial_rels[0]);
    }
    run.root.initial_rels = crate::relnode::pgvec_clone_shallow(run.mcx, &initial_rels);
    // Route to the genetic optimizer when enabled and the join is large enough
    // (allpaths.c make_rel_from_joinlist). geqo_threshold's min is 2 so the
    // cast is safe; the join_search_hook lane is not implemented.
    if crate::geqo::enable_geqo() && levels_needed >= crate::geqo::geqo_threshold() as usize {
        return crate::geqo::geqo(run, levels_needed, &initial_rels);
    }
    standard_join_search(run, levels_needed, initial_rels)
}

pub fn standard_join_search<'mcx>(
    run: &mut PlannerRun<'mcx>,
    levels_needed: usize,
    initial_rels: PgVec<'mcx, RelId>,
) -> PgResult<RelId> {
    debug_assert!(run.root.join_rel_level.is_empty());
    run.root.join_rel_level.push(PgVec::new_in(run.mcx));
    run.root.join_rel_level.push(initial_rels);
    for _ in 2..=levels_needed {
        run.root.join_rel_level.push(PgVec::new_in(run.mcx));
    }

    for lev in 2..=levels_needed {
        join_search_one_level(run, lev)?;
        let rels = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.join_rel_level[lev],
        );
        for &rel in rels.iter() {
            crate::allpaths::generate_partitionwise_join_paths(run, rel)?;
            // No partial join paths exist yet (partial-join lane), so this
            // gathers nothing today; the call keeps C's shape.
            if !crate::relnode::relids_equal(&run.root.rel(rel).relids, &run.root.all_query_rels) {
                crate::allpaths::generate_useful_gather_paths(run, rel, false)?;
            }
            crate::pathnode::set_cheapest(run, rel)?;
        }
    }

    if run.root.join_rel_level[levels_needed].is_empty() {
        panic!("failed to build any {levels_needed}-way joins");
    }
    debug_assert!(run.root.join_rel_level[levels_needed].len() == 1);
    let rel = run.root.join_rel_level[levels_needed][0];
    run.root.join_rel_level.clear();
    Ok(rel)
}

fn join_search_one_level(run: &mut PlannerRun<'_>, level: usize) -> PgResult<()> {
    debug_assert!(run.root.join_rel_level[level].is_empty());
    run.root.join_cur_level = level as i32;
    let prev = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.join_rel_level[level - 1]);
    let ones = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.join_rel_level[1]);

    for (i, &old_rel) in prev.iter().enumerate() {
        if !run.root.rel(old_rel).joininfo.is_empty()
            || run.root.rel(old_rel).has_eclass_joins
            || has_join_restriction(run, old_rel)
        {
            let first_rel = if level == 2 { i + 1 } else { 0 };
            make_rels_by_clause_joins(run, old_rel, &ones, first_rel)?;
        } else {
            make_rels_by_clauseless_joins(run, old_rel, &ones)?;
        }
    }

    // Bushy plans: join k-level rels to (level-k)-level rels.
    for k in 2.. {
        let other_level = level - k;
        if k > other_level {
            break;
        }
        let krels = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.join_rel_level[k]);
        let orels =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.join_rel_level[other_level]);
        for (i, &old_rel) in krels.iter().enumerate() {
            if run.root.rel(old_rel).joininfo.is_empty()
                && !run.root.rel(old_rel).has_eclass_joins
                && !has_join_restriction(run, old_rel)
            {
                continue;
            }
            let others: &[RelId] = if k == other_level { &orels[i + 1..] } else { &orels[..] };
            for &new_rel in others {
                if relids_overlap(&run.root.rel(old_rel).relids, &run.root.rel(new_rel).relids) {
                    continue;
                }
                if have_relevant_joinclause(run, old_rel, new_rel)
                    || have_join_order_restriction(run, old_rel, new_rel)?
                {
                    make_join_rel(run, old_rel, new_rel)?;
                }
            }
        }
    }

    // Last-ditch: Cartesian products against the initial rels.
    if run.root.join_rel_level[level].is_empty() {
        for &old_rel in prev.iter() {
            make_rels_by_clauseless_joins(run, old_rel, &ones)?;
        }
        // With special joins or lateral refs some levels can legally be
        // empty; a workable bushy plan appears at a higher level.
        if run.root.join_rel_level[level].is_empty()
            && run.root.join_info_list.is_empty()
            && !run.root.hasLateralRTEs
        {
            panic!("failed to build any {level}-way joins");
        }
    }
    Ok(())
}

fn make_rels_by_clause_joins(
    run: &mut PlannerRun<'_>,
    old_rel: RelId,
    other_rels: &[RelId],
    first_rel: usize,
) -> PgResult<()> {
    for &other_rel in &other_rels[first_rel..] {
        if relids_overlap(&run.root.rel(old_rel).relids, &run.root.rel(other_rel).relids) {
            continue;
        }
        if have_relevant_joinclause(run, old_rel, other_rel)
            || have_join_order_restriction(run, old_rel, other_rel)?
        {
            make_join_rel(run, old_rel, other_rel)?;
        }
    }
    Ok(())
}

// make_rels_by_clauseless_joins (joinrels.c): iterates the full initial-rels
// list, so at level 2 a clauseless pair is attempted in both directions
// (make_join_rel dedupes the joinrel; repeated path population matches C).
fn make_rels_by_clauseless_joins(
    run: &mut PlannerRun<'_>,
    old_rel: RelId,
    other_rels: &[RelId],
) -> PgResult<()> {
    for &other_rel in other_rels {
        if !relids_overlap(&run.root.rel(old_rel).relids, &run.root.rel(other_rel).relids) {
            make_join_rel(run, old_rel, other_rel)?;
        }
    }
    Ok(())
}

// has_join_restriction (joinrels.c).
fn has_join_restriction(run: &PlannerRun<'_>, rel: RelId) -> bool {
    if !crate::relnode::relids_is_empty(&run.root.rel(rel).lateral_relids)
        || !crate::relnode::relids_is_empty(&run.root.rel(rel).lateral_referencers)
    {
        return true;
    }
    let relids = &run.root.rel(rel).relids;
    for &phid in run.root.placeholder_list.iter() {
        let phinfo = run.root.phinfo(phid);
        if relids_is_subset(relids, &phinfo.ph_eval_at)
            && !relids_equal(relids, &phinfo.ph_eval_at)
        {
            return true;
        }
    }
    run.root.join_info_list.iter().any(|sj| {
        // Full joins' ordering is preserved by other mechanisms.
        if sj.jointype == types_pathnodes::JOIN_FULL {
            return false;
        }
        if relids_is_subset(&sj.min_lefthand, relids) && relids_is_subset(&sj.min_righthand, relids)
        {
            return false;
        }
        relids_overlap(&sj.min_lefthand, relids) || relids_overlap(&sj.min_righthand, relids)
    })
}

// have_join_order_restriction (joinrels.c).
pub(crate) fn have_join_order_restriction(
    run: &mut PlannerRun<'_>,
    rel1: RelId,
    rel2: RelId,
) -> PgResult<bool> {
    // A direct lateral reference either way makes the pair worth attempting.
    if relids_overlap(&run.root.rel(rel1).relids, &run.root.rel(rel2).direct_lateral_relids)
        || relids_overlap(&run.root.rel(rel2).relids, &run.root.rel(rel1).direct_lateral_relids)
    {
        return Ok(true);
    }
    // Likewise when both rels are needed to compute some PlaceHolderVar.
    for &phid in run.root.placeholder_list.iter() {
        let phinfo = run.root.phinfo(phid);
        if relids_is_subset(&run.root.rel(rel1).relids, &phinfo.ph_eval_at)
            && relids_is_subset(&run.root.rel(rel2).relids, &phinfo.ph_eval_at)
        {
            return Ok(true);
        }
    }
    let mut result = false;
    for i in 0..run.root.join_info_list.len() {
        let sj = &run.root.join_info_list[i];
        if sj.jointype == types_pathnodes::JOIN_FULL {
            continue;
        }
        let r1 = &run.root.rel(rel1).relids;
        let r2 = &run.root.rel(rel2).relids;
        if (relids_is_subset(&sj.min_lefthand, r1) && relids_is_subset(&sj.min_righthand, r2))
            || (relids_is_subset(&sj.min_lefthand, r2) && relids_is_subset(&sj.min_righthand, r1))
            || (relids_overlap(&sj.min_righthand, r1) && relids_overlap(&sj.min_righthand, r2))
            || (relids_overlap(&sj.min_lefthand, r1) && relids_overlap(&sj.min_lefthand, r2))
        {
            result = true;
            break;
        }
    }
    // Clauseless bushy joins are put off as long as either input can legally
    // join by clause; without this veto a high join-order restriction
    // explodes the search inside its LHS/RHS.
    if result && (has_legal_joinclause(run, rel1)? || has_legal_joinclause(run, rel2)?) {
        result = false;
    }
    Ok(result)
}

// has_legal_joinclause (joinrels.c): probes single initial rels only — a
// heuristic, so an occasional wrong "false" only costs planning time.
fn has_legal_joinclause(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<bool> {
    let others = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.initial_rels);
    for &rel2 in others.iter() {
        if relids_overlap(&run.root.rel(rel).relids, &run.root.rel(rel2).relids) {
            continue;
        }
        if have_relevant_joinclause(run, rel, rel2) {
            let joinrelids = relids_union(
                run.mcx,
                &run.root.rel(rel).relids,
                &run.root.rel(rel2).relids,
            );
            if join_is_legal(run, rel, rel2, &joinrelids)?.is_some() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn have_relevant_joinclause(run: &PlannerRun<'_>, rel1: RelId, rel2: RelId) -> bool {
    let (probe, other) = if run.root.rel(rel1).joininfo.len() <= run.root.rel(rel2).joininfo.len()
    {
        (rel1, rel2)
    } else {
        (rel2, rel1)
    };
    let other_relids = &run.root.rel(other).relids;
    let result = run.root.rel(probe).joininfo.iter().any(|&rid| {
        relids_overlap(other_relids, &run.root.rinfo(rid).required_relids)
    });
    if !result
        && run.root.rel(rel1).has_eclass_joins
        && run.root.rel(rel2).has_eclass_joins
    {
        return crate::equivclass::have_relevant_eclass_joinclause(run, rel1, rel2);
    }
    result
}

// is_dummy_rel (joinrels.c). C's dummy marker is a childless Append; ours is
// a GroupResultPath whose single qual is constant FALSE (allpaths.rs
// set_dummy_rel_pathlist). A quals-free GroupResultPath fronts a live no-FROM
// result rel (set_subquery_pathlist's final rel under an unflattenable SRF
// subquery) and must not read as dummy.
pub fn is_dummy_rel(root: &types_pathnodes::PlannerInfo<'_>, rel: RelId) -> bool {
    let Some(&first) = root.rel(rel).pathlist.first() else { return false };
    let mut path = root.path(first);
    loop {
        match path {
            types_pathnodes::PathNode::ProjectionPath(p) => {
                path = root.path(p.subpath.expect("ProjectionPath has a subpath"))
            }
            types_pathnodes::PathNode::ProjectSetPath(p) => {
                path = root.path(p.subpath.expect("ProjectSetPath has a subpath"))
            }
            _ => break,
        }
    }
    let types_pathnodes::PathNode::GroupResultPath(g) = path else {
        return false;
    };
    g.quals.len() == 1
        && matches!(
            root.expr_node(g.quals[0]).as_const(),
            Some(c) if !c.constisnull && !c.constvalue.as_bool()
        )
}

pub fn make_join_rel(
    run: &mut PlannerRun<'_>,
    rel1: RelId,
    rel2: RelId,
) -> PgResult<Option<RelId>> {
    debug_assert!(!relids_overlap(&run.root.rel(rel1).relids, &run.root.rel(rel2).relids));
    let mut joinrelids = relids_union(
        run.mcx,
        &run.root.rel(rel1).relids,
        &run.root.rel(rel2).relids,
    );
    // C returns NULL for an illegal pair; the search loops just skip it.
    let Some((match_sjinfo, reversed)) = join_is_legal(run, rel1, rel2, &joinrelids)? else {
        return Ok(None);
    };

    let mut pushed_down_joins: PgVec<'_, SpecialJoinInfo<'_>> = PgVec::new_in(run.mcx);
    joinrelids =
        add_outer_joins_to_relids(run, joinrelids, &match_sjinfo, &mut pushed_down_joins);
    let (rel1, rel2) = if reversed { (rel2, rel1) } else { (rel1, rel2) };

    let sjinfo = match match_sjinfo {
        Some(sj) => sj,
        None => init_dummy_sjinfo(
            run,
            relids_copy(run.mcx, &run.root.rel(rel1).relids),
            relids_copy(run.mcx, &run.root.rel(rel2).relids),
        ),
    };
    let (joinrel, restrictlist) =
        build_join_rel(run, joinrelids, rel1, rel2, &sjinfo, &pushed_down_joins)?;
    // If we've already proven this join is empty, we needn't consider paths.
    if is_dummy_rel(&run.root, joinrel) {
        return Ok(Some(joinrel));
    }
    populate_joinrel_with_paths(run, rel1, rel2, joinrel, &sjinfo, &restrictlist)?;
    Ok(Some(joinrel))
}

// add_outer_joins_to_relids (joinrels.c): canonical joinrel relids include
// the OJ's own relid plus any identity-3-pushed-down joins completed here.
fn add_outer_joins_to_relids<'mcx>(
    run: &PlannerRun<'mcx>,
    input_relids: Relids<'mcx>,
    sjinfo: &Option<SpecialJoinInfo<'mcx>>,
    pushed_down_joins: &mut PgVec<'mcx, SpecialJoinInfo<'mcx>>,
) -> Relids<'mcx> {
    let mcx = run.mcx;
    let Some(sj) = sjinfo else { return input_relids };
    if sj.ojrelid == 0 {
        return input_relids;
    }
    if sj.jointype != JOIN_LEFT {
        return relids_add_member(mcx, &input_relids, sj.ojrelid);
    }
    // Pushed into a lower join's RHS per identity 3: our outputs are not the
    // final state of our RHS yet.
    if !relids_is_subset(&sj.commute_below_l, &input_relids) {
        return input_relids;
    }
    let mut input_relids = relids_add_member(mcx, &input_relids, sj.ojrelid);
    if !crate::relnode::relids_is_empty(&sj.commute_above_l) {
        let mut commute_above_rels = relids_copy(mcx, &sj.commute_above_l);
        // join_info_list is bottom-up, so one pass suffices.
        for i in 0..run.root.join_info_list.len() {
            let othersj = &run.root.join_info_list[i];
            if othersj.ojrelid == sj.ojrelid
                || othersj.ojrelid == 0
                || othersj.jointype != JOIN_LEFT
            {
                continue;
            }
            if !relids_is_member(othersj.ojrelid as i32, &commute_above_rels) {
                continue;
            }
            if !relids_is_member(othersj.ojrelid as i32, &input_relids)
                && relids_is_subset(&othersj.min_lefthand, &input_relids)
                && relids_is_subset(&othersj.min_righthand, &input_relids)
                && relids_is_subset(&othersj.commute_below_l, &input_relids)
            {
                input_relids = relids_add_member(mcx, &input_relids, othersj.ojrelid);
                let othersj = run.root.join_info_list[i].clone();
                commute_above_rels =
                    relids_union(mcx, &commute_above_rels, &othersj.commute_above_l);
                pushed_down_joins.push(othersj);
            }
        }
    }
    input_relids
}

// restriction_is_constant_false (joinrels.c).
fn restriction_is_constant_false(
    run: &PlannerRun<'_>,
    restrictlist: &[types_pathnodes::RinfoId],
    joinrel: RelId,
    only_pushed_down: bool,
) -> bool {
    let joinrelids = &run.root.rel(joinrel).relids;
    for &rid in restrictlist {
        if only_pushed_down && !rinfo_is_pushed_down(run, rid, joinrelids) {
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if let Some(c) = clause.as_const() {
            // Constant NULL is as good as constant FALSE for our purposes.
            if c.constisnull || !c.constvalue.as_bool() {
                return true;
            }
        }
    }
    false
}

// mark_dummy_rel (joinrels.c); the dummy marker is a GroupResultPath (see
// set_dummy_rel_pathlist), and unlike that function the reltarget width is
// left alone.
pub fn mark_dummy_rel(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    if is_dummy_rel(&run.root, rel) {
        return Ok(());
    }
    let required_outer =
        crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    crate::allpaths::add_dummy_path(run, rel, &required_outer)
}

// find_join_rel (relnode.c): linear probe while the list is short, relids-
// keyed hash past 32 entries. The hash is built eagerly by add_join_rel when
// the list crosses the threshold (C builds it lazily on the next lookup);
// same lookups, same results.
pub fn find_join_rel(
    root: &types_pathnodes::PlannerInfo<'_>,
    relids: &Relids<'_>,
) -> Option<RelId> {
    if let Some(hash) = &root.join_rel_hash {
        return hash.get(&join_rel_hash_key(root.mcx, relids)).copied();
    }
    root.join_rel_list.iter().copied().find(|&jr| relids_equal(&root.rel(jr).relids, relids))
}

// build_join_rel_hash (relnode.c).
fn build_join_rel_hash<'mcx>(root: &mut types_pathnodes::PlannerInfo<'mcx>) {
    debug_assert!(root.join_rel_hash.is_none());
    let mcx = root.mcx;
    let mut hash = mcx::PgFxHashMap::with_capacity_and_hasher_in(256, Default::default(), mcx);
    for &jr in root.join_rel_list.iter() {
        hash.insert(join_rel_hash_key(mcx, &root.rel(jr).relids), jr);
    }
    root.join_rel_hash = Some(hash);
}

fn join_rel_hash_key<'mcx>(mcx: mcx::Mcx<'mcx>, relids: &Relids<'_>) -> PgVec<'mcx, u64> {
    let mut key: PgVec<'mcx, u64> = PgVec::new_in(mcx);
    key.extend_from_slice(crate::relnode::relids_word_slice(relids));
    key
}

// add_join_rel (relnode.c): append to join_rel_list and the hash once the
// list has outgrown the linear probe.
pub(crate) fn add_join_rel<'mcx>(
    root: &mut types_pathnodes::PlannerInfo<'mcx>,
    joinrel: RelId,
) {
    let mcx = root.mcx;
    root.join_rel_list.push(joinrel);
    if root.join_rel_hash.is_none() && root.join_rel_list.len() > 32 {
        build_join_rel_hash(root);
    } else if root.join_rel_hash.is_some() {
        let key = join_rel_hash_key(mcx, &root.rel(joinrel).relids);
        root.join_rel_hash.as_mut().unwrap().insert(key, joinrel);
    }
}

pub(crate) fn populate_joinrel_with_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &PgVec<'mcx, types_pathnodes::RinfoId>,
) -> PgResult<()> {
    'switch: {
        match sjinfo.jointype {
            JOIN_INNER => {
                if is_dummy_rel(&run.root, rel1)
                    || is_dummy_rel(&run.root, rel2)
                    || restriction_is_constant_false(run, restrictlist, joinrel, false)
                {
                    mark_dummy_rel(run, joinrel)?;
                    break 'switch;
                }
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, JOIN_INNER, sjinfo, restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, JOIN_INNER, sjinfo, restrictlist)?;
            }
            JOIN_LEFT => {
                if is_dummy_rel(&run.root, rel1)
                    || restriction_is_constant_false(run, restrictlist, joinrel, true)
                {
                    mark_dummy_rel(run, joinrel)?;
                    break 'switch;
                }
                if restriction_is_constant_false(run, restrictlist, joinrel, false)
                    && relids_is_subset(&run.root.rel(rel2).relids, &sjinfo.syn_righthand)
                {
                    mark_dummy_rel(run, rel2)?;
                }
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, JOIN_LEFT, sjinfo, restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, JOIN_RIGHT, sjinfo, restrictlist)?;
            }
            types_pathnodes::JOIN_FULL => {
                if (is_dummy_rel(&run.root, rel1) && is_dummy_rel(&run.root, rel2))
                    || restriction_is_constant_false(run, restrictlist, joinrel, true)
                {
                    mark_dummy_rel(run, joinrel)?;
                    break 'switch;
                }
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_FULL, sjinfo, restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_FULL, sjinfo, restrictlist)?;
                if run.root.rel(joinrel).pathlist.is_empty() {
                    return Err(full_join_unsupported());
                }
            }
            types_pathnodes::JOIN_SEMI => {
                if relids_is_subset(&sjinfo.min_lefthand, &run.root.rel(rel1).relids)
                    && relids_is_subset(&sjinfo.min_righthand, &run.root.rel(rel2).relids)
                {
                    if is_dummy_rel(&run.root, rel1)
                        || is_dummy_rel(&run.root, rel2)
                        || restriction_is_constant_false(run, restrictlist, joinrel, false)
                    {
                        mark_dummy_rel(run, joinrel)?;
                        break 'switch;
                    }
                    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_SEMI, sjinfo, restrictlist)?;
                    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_RIGHT_SEMI, sjinfo, restrictlist)?;
                }
                let unique_ok = relids_equal(&sjinfo.syn_righthand, &run.root.rel(rel2).relids)
                    && {
                        let cheapest =
                            run.root.rel(rel2).cheapest_total_path.expect("cheapest path");
                        crate::pathnode::create_unique_path(run, rel2, cheapest, sjinfo)?
                            .is_some()
                    };
                if unique_ok {
                    if is_dummy_rel(&run.root, rel1)
                        || is_dummy_rel(&run.root, rel2)
                        || restriction_is_constant_false(run, restrictlist, joinrel, false)
                    {
                        mark_dummy_rel(run, joinrel)?;
                        break 'switch;
                    }
                    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_UNIQUE_INNER, sjinfo, restrictlist)?;
                    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_UNIQUE_OUTER, sjinfo, restrictlist)?;
                }
            }
            types_pathnodes::JOIN_ANTI => {
                if is_dummy_rel(&run.root, rel1)
                    || restriction_is_constant_false(run, restrictlist, joinrel, true)
                {
                    mark_dummy_rel(run, joinrel)?;
                    break 'switch;
                }
                if restriction_is_constant_false(run, restrictlist, joinrel, false)
                    && relids_is_subset(&run.root.rel(rel2).relids, &sjinfo.syn_righthand)
                {
                    mark_dummy_rel(run, rel2)?;
                }
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_ANTI, sjinfo, restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_RIGHT_ANTI, sjinfo, restrictlist)?;
            }
            other => panic!("populate_joinrel_with_paths (joinrels.c): jointype {other}"),
        }
    }
    try_partitionwise_join(run, rel1, rel2, joinrel, sjinfo, restrictlist)
}

#[cold]
#[inline(never)]
fn full_join_unsupported() -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::new(
            types_error::ERROR,
            "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions"
                .to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// join_is_legal (joinrels.c): None = illegal.
fn join_is_legal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrelids: &Relids<'mcx>,
) -> PgResult<Option<(Option<SpecialJoinInfo<'mcx>>, bool)>> {
    let r1 = relids_copy(run.mcx, &run.root.rel(rel1).relids);
    let r2 = relids_copy(run.mcx, &run.root.rel(rel2).relids);
    let mut match_sjinfo: Option<SpecialJoinInfo<'mcx>> = None;
    let mut reversed = false;
    let mut unique_ified = false;
    let mut must_be_leftjoin = false;

    for i in 0..run.root.join_info_list.len() {
        let sj = run.root.join_info_list[i].clone();
        if !relids_overlap(&sj.min_righthand, joinrelids) {
            continue;
        }
        if relids_is_subset(joinrelids, &sj.min_righthand) {
            continue;
        }
        if relids_is_subset(&sj.min_lefthand, &r1) && relids_is_subset(&sj.min_righthand, &r1) {
            continue;
        }
        if relids_is_subset(&sj.min_lefthand, &r2) && relids_is_subset(&sj.min_righthand, &r2) {
            continue;
        }
        debug_assert!(matches!(
            sj.jointype,
            JOIN_LEFT
                | types_pathnodes::JOIN_SEMI
                | types_pathnodes::JOIN_ANTI
                | types_pathnodes::JOIN_FULL
        ));
        // A semijoin whose RHS was already joined to other rels inside an
        // input must have been unique-ified there; it's no longer relevant.
        if sj.jointype == types_pathnodes::JOIN_SEMI {
            if relids_is_subset(&sj.syn_righthand, &r1) && !relids_equal(&sj.syn_righthand, &r1)
            {
                continue;
            }
            if relids_is_subset(&sj.syn_righthand, &r2) && !relids_equal(&sj.syn_righthand, &r2)
            {
                continue;
            }
        }
        if relids_is_subset(&sj.min_lefthand, &r1) && relids_is_subset(&sj.min_righthand, &r2) {
            if match_sjinfo.is_some() {
                return Ok(None);
            }
            match_sjinfo = Some(sj.clone());
            reversed = false;
        } else if relids_is_subset(&sj.min_lefthand, &r2)
            && relids_is_subset(&sj.min_righthand, &r1)
        {
            if match_sjinfo.is_some() {
                return Ok(None);
            }
            match_sjinfo = Some(sj.clone());
            reversed = true;
        } else if sj.jointype == types_pathnodes::JOIN_SEMI
            && relids_equal(&sj.syn_righthand, &r2)
            && {
                let cheapest = run.root.rel(rel2).cheapest_total_path.expect("cheapest path");
                crate::pathnode::create_unique_path(run, rel2, cheapest, &sj)?.is_some()
            }
        {
            if match_sjinfo.is_some() {
                return Ok(None);
            }
            match_sjinfo = Some(sj.clone());
            reversed = false;
            unique_ified = true;
        } else if sj.jointype == types_pathnodes::JOIN_SEMI
            && relids_equal(&sj.syn_righthand, &r1)
            && {
                let cheapest = run.root.rel(rel1).cheapest_total_path.expect("cheapest path");
                crate::pathnode::create_unique_path(run, rel1, cheapest, &sj)?.is_some()
            }
        {
            if match_sjinfo.is_some() {
                return Ok(None);
            }
            match_sjinfo = Some(sj.clone());
            reversed = true;
            unique_ified = true;
        } else {
            // Both inputs overlapping the RHS means any violation already
            // happened below (a previously-allowed commutation).
            if relids_overlap(&r1, &sj.min_righthand) && relids_overlap(&r2, &sj.min_righthand) {
                continue;
            }
            // Associating into this SJ's RHS (identity 3) demands a LEFT SJ
            // and no LHS overlap.
            if sj.jointype != JOIN_LEFT || relids_overlap(joinrelids, &sj.min_lefthand) {
                return Ok(None);
            }
            must_be_leftjoin = true;
        }
    }

    // Identity 3 also demands the proposed join be a strict-LHS LEFT join.
    if must_be_leftjoin
        && match_sjinfo
            .as_ref()
            .is_none_or(|sj| sj.jointype != JOIN_LEFT || !sj.lhs_strict)
    {
        return Ok(None);
    }

    if run.root.hasLateralRTEs {
        let mcx = run.mcx;
        // Lateral refs in both directions are unjoinable; one direction
        // forces a nestloop with the referencer inside, and only direct
        // references qualify at this join level.
        let lateral_fwd = relids_overlap(&r1, &run.root.rel(rel2).lateral_relids);
        let lateral_rev = relids_overlap(&r2, &run.root.rel(rel1).lateral_relids);
        if lateral_fwd && lateral_rev {
            return Ok(None);
        }
        if lateral_fwd {
            if let Some(sj) = &match_sjinfo {
                if reversed || unique_ified || sj.jointype == types_pathnodes::JOIN_FULL {
                    return Ok(None);
                }
            }
            if !relids_overlap(&r1, &run.root.rel(rel2).direct_lateral_relids) {
                return Ok(None);
            }
        } else if lateral_rev {
            if let Some(sj) = &match_sjinfo {
                if !reversed || unique_ified || sj.jointype == types_pathnodes::JOIN_FULL {
                    return Ok(None);
                }
            }
            if !relids_overlap(&r2, &run.root.rel(rel1).direct_lateral_relids) {
                return Ok(None);
            }
        }

        // Reject if the join's minimum parameterization overlaps rels forced
        // to the inner side of an outer join with this joinrel.
        let join_lateral_rels = min_join_parameterization(run, joinrelids, rel1, rel2);
        if !crate::relnode::relids_is_empty(&join_lateral_rels) {
            let mut join_plus_rhs = relids_copy(mcx, joinrelids);
            let mut more = true;
            while more {
                more = false;
                for i in 0..run.root.join_info_list.len() {
                    let (min_l, min_r, jt) = {
                        let sj = &run.root.join_info_list[i];
                        (
                            relids_copy(mcx, &sj.min_lefthand),
                            relids_copy(mcx, &sj.min_righthand),
                            sj.jointype,
                        )
                    };
                    if jt == types_pathnodes::JOIN_FULL {
                        continue;
                    }
                    if relids_overlap(&min_l, &join_plus_rhs)
                        && !relids_is_subset(&min_r, &join_plus_rhs)
                    {
                        join_plus_rhs = relids_union(mcx, &join_plus_rhs, &min_r);
                        more = true;
                    }
                }
            }
            if relids_overlap(&join_plus_rhs, &join_lateral_rels) {
                return Ok(None);
            }
        }
    }
    Ok(Some((match_sjinfo, reversed)))
}

// min_join_parameterization (relnode.c).
pub fn min_join_parameterization<'mcx>(
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
) -> Relids<'mcx> {
    let mcx = run.mcx;
    let u = relids_union(
        mcx,
        &run.root.rel(outer_rel).lateral_relids,
        &run.root.rel(inner_rel).lateral_relids,
    );
    let r = crate::relnode::relids_difference(mcx, &u, joinrelids);
    if crate::relnode::relids_is_empty(&r) {
        crate::relnode::relids_empty()
    } else {
        r
    }
}

fn build_join_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrelids: Relids<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    pushed_down_joins: &PgVec<'mcx, SpecialJoinInfo<'mcx>>,
) -> PgResult<(RelId, PgVec<'mcx, types_pathnodes::RinfoId>)> {
    let mcx = run.mcx;
    if let Some(jr) = find_join_rel(&run.root, &joinrelids) {
        let restrictlist =
            build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel, sjinfo)?;
        return Ok((jr, restrictlist));
    }

    let mut joinrel = types_pathnodes::RelOptInfo::new(mcx);
    joinrel.reloptkind = RELOPT_JOINREL;
    joinrel.relids = relids_copy(mcx, &joinrelids);
    joinrel.consider_startup = run.root.tuple_fraction > 0.0;
    joinrel.rtekind = RTE_JOIN;
    joinrel.rel_parallel_workers = -1;
    joinrel.nparts = -1;
    joinrel.baserestrict_min_security = u32::MAX;
    joinrel.direct_lateral_relids = relids_union(
        mcx,
        &run.root.rel(outer_rel).direct_lateral_relids,
        &run.root.rel(inner_rel).direct_lateral_relids,
    );
    joinrel.lateral_relids = min_join_parameterization(run, &joinrelids, outer_rel, inner_rel);
    joinrel.pathtarget_id =
        Some(run.root.alloc_pathtarget(types_pathnodes::PathTarget::new(mcx)));
    let joinrel = run.root.alloc_rel(joinrel);

    build_joinrel_tlist(
        run,
        joinrel,
        outer_rel,
        sjinfo,
        pushed_down_joins,
        sjinfo.jointype == types_pathnodes::JOIN_FULL,
    )?;
    build_joinrel_tlist(
        run,
        joinrel,
        inner_rel,
        sjinfo,
        pushed_down_joins,
        sjinfo.jointype != JOIN_INNER,
    )?;
    crate::placeholder::add_placeholders_to_joinrel(run, joinrel, outer_rel, inner_rel)?;

    {
        let d = crate::relnode::relids_difference(
            mcx,
            &run.root.rel(joinrel).direct_lateral_relids,
            &run.root.rel(joinrel).relids,
        );
        run.root.rel_mut(joinrel).direct_lateral_relids =
            if crate::relnode::relids_is_empty(&d) { crate::relnode::relids_empty() } else { d };
    }

    let restrictlist =
        build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel, sjinfo)?;
    build_joinrel_joinlist(run, joinrel, outer_rel, inner_rel);

    let he = crate::equivclass::has_relevant_eclass_joinclause(run, joinrel);
    run.root.rel_mut(joinrel).has_eclass_joins = he;

    crate::relnode::build_joinrel_partition_info(
        run, joinrel, outer_rel, inner_rel, sjinfo, &restrictlist,
    )?;

    set_joinrel_size_estimates(run, joinrel, outer_rel, inner_rel, sjinfo, &restrictlist)?;

    if run.root.rel(inner_rel).consider_parallel && run.root.rel(outer_rel).consider_parallel {
        let mut safe = true;
        for &rid in restrictlist.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            if !crate::is_parallel_safe_opt(run, Some(clause))? {
                safe = false;
                break;
            }
        }
        if safe {
            let target = run.rel_reltarget_id(joinrel);
            safe = crate::is_parallel_safe_exprs(run, target)?;
        }
        if safe {
            run.root.rel_mut(joinrel).consider_parallel = true;
        }
    }

    add_join_rel(&mut run.root, joinrel);
    if !run.root.join_rel_level.is_empty() {
        let lev = run.root.join_cur_level;
        debug_assert!(lev > 0);
        run.root.join_rel_level[lev as usize].push(joinrel);
    }
    Ok((joinrel, restrictlist))
}

// build_joinrel_tlist (relnode.c), Var-only arm. Under can_null the output
// Var carries the nulling bits it would have in syntactic join order: this
// join's relid, the relids of identity-3 joins completed here, and any
// commute_above_r relids already inside the joinrel.
fn build_joinrel_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    input_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    pushed_down_joins: &PgVec<'mcx, SpecialJoinInfo<'mcx>>,
    can_null: bool,
) -> PgResult<()> {
    let mcx = run.mcx;
    let relids = relids_copy(mcx, &run.root.rel(joinrel).relids);
    let mut tuple_width = run.root.rel_reltarget(joinrel).width as i64;
    let exprs = crate::relnode::pgvec_clone_shallow(
        mcx,
        &run.root.rel_reltarget(input_rel).exprs,
    );
    for &id in exprs.iter() {
        let node = *run.root.expr_node(id);
        if let Some(phv) = node.as_place_holder_var() {
            // relnode.c:1116-1163: keep the PHV if still needed above, adding
            // this join's nulling bits under can_null; width from its phinfo.
            let phinfo_id = crate::placeholder::find_placeholder_info(run, phv)?;
            let (ph_needed, ph_width) = {
                let phinfo = run.root.phinfo(phinfo_id);
                (relids_copy(mcx, &phinfo.ph_needed), phinfo.ph_width)
            };
            if crate::relnode::relids_is_subset(&ph_needed, &relids) {
                continue;
            }
            let phv = node.as_place_holder_var().expect("PlaceHolderVar");
            let out_id = if can_null {
                let mut nulling = phv.phnullingrels.clone_in(mcx)?;
                let mut changed = false;
                let phrels_subset = |side: &types_pathnodes::Relids<'mcx>| {
                    phv.phrels.iter().all(|m| relids_is_member(m, side))
                };
                if sjinfo.ojrelid != 0
                    && relids_is_member(sjinfo.ojrelid as i32, &relids)
                    && (phrels_subset(&sjinfo.syn_righthand)
                        || (sjinfo.jointype == types_pathnodes::JOIN_FULL
                            && phrels_subset(&sjinfo.syn_lefthand)))
                {
                    nulling.add_member(mcx, sjinfo.ojrelid as i32)?;
                    changed = true;
                }
                for othersj in pushed_down_joins.iter() {
                    debug_assert!(relids_is_member(othersj.ojrelid as i32, &relids));
                    if phrels_subset(&othersj.syn_righthand) {
                        nulling.add_member(mcx, othersj.ojrelid as i32)?;
                        changed = true;
                    }
                }
                for m in crate::relnode::relids_members(&sjinfo.commute_above_r) {
                    if relids_is_member(m, &relids) {
                        nulling.add_member(mcx, m)?;
                        changed = true;
                    }
                }
                if changed {
                    let copy = types_nodes::primnodes::PlaceHolderVar {
                        phexpr: phv.phexpr,
                        phrels: phv.phrels.clone_in(mcx)?,
                        phnullingrels: nulling,
                        phid: phv.phid,
                        phlevelsup: phv.phlevelsup,
                    };
                    run.intern_expr(types_nodes::Node::mk(mcx, copy)?)
                } else {
                    id
                }
            } else {
                id
            };
            run.root.rel_reltarget_mut(joinrel).exprs.push(out_id);
            tuple_width += ph_width as i64;
            continue;
        }
        let var = node
            .as_var()
            .unwrap_or_else(|| panic!("unexpected node type in rel targetlist: {:?}", node.node_tag()));
        if var.varno == types_nodes::primnodes::ROWID_VAR {
            // relnode.c:1176-1184: UPDATE/DELETE/MERGE row identity vars are
            // always needed; width from the RowIdentityVarInfo. Never nulled
            // (cf. setrefs.c comments), so the interned Var passes through.
            let ridinfo = &run.root.row_identity_vars[(var.varattno - 1) as usize];
            tuple_width += ridinfo.rowidwidth as i64;
            run.root.rel_reltarget_mut(joinrel).exprs.push(id);
            continue;
        }
        debug_assert!(var.varno > 0 && relids_is_member(var.varno, &relids));
        let baserel = find_base_rel(&run.root, var.varno);
        let ndx = (var.varattno - run.root.rel(baserel).min_attr) as usize;
        if relids_is_subset(&run.root.rel(baserel).attr_needed[ndx], &relids) {
            continue;
        }
        tuple_width += run.root.rel(baserel).attr_widths[ndx] as i64;
        let out_id = if can_null {
            let mut nulling = var.varnullingrels.clone_in(mcx)?;
            let mut changed = false;
            if sjinfo.ojrelid != 0
                && relids_is_member(sjinfo.ojrelid as i32, &relids)
                && (relids_is_member(var.varno, &sjinfo.syn_righthand)
                    || (sjinfo.jointype == types_pathnodes::JOIN_FULL
                        && relids_is_member(var.varno, &sjinfo.syn_lefthand)))
            {
                nulling.add_member(mcx, sjinfo.ojrelid as i32)?;
                changed = true;
            }
            for othersj in pushed_down_joins.iter() {
                debug_assert!(relids_is_member(othersj.ojrelid as i32, &relids));
                if relids_is_member(var.varno, &othersj.syn_righthand) {
                    nulling.add_member(mcx, othersj.ojrelid as i32)?;
                    changed = true;
                }
            }
            for m in crate::relnode::relids_members(&sjinfo.commute_above_r) {
                if relids_is_member(m, &relids) {
                    nulling.add_member(mcx, m)?;
                    changed = true;
                }
            }
            if changed {
                let nulled = types_nodes::primnodes::Var { varnullingrels: nulling, ..*var };
                run.intern_expr(types_nodes::Node::mk(mcx, nulled)?)
            } else {
                id
            }
        } else {
            id
        };
        run.root.rel_reltarget_mut(joinrel).exprs.push(out_id);
    }
    run.root.rel_reltarget_mut(joinrel).width = crate::costsize::clamp_width_est(tuple_width);
    Ok(())
}

// build_joinrel_restrictlist (relnode.c).
fn build_joinrel_restrictlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<PgVec<'mcx, types_pathnodes::RinfoId>> {
    let both_input_relids = relids_union(
        run.mcx,
        &run.root.rel(outer_rel).relids,
        &run.root.rel(inner_rel).relids,
    );
    let mut result: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    for &input in [outer_rel, inner_rel].iter() {
        for &rid in run.root.rel(input).joininfo.iter() {
            if !relids_is_subset(&run.root.rinfo(rid).required_relids, joinrelids) {
                continue;
            }
            // A clone variant may be too late (or wrong) to evaluate here;
            // skipping trusts that another clone was or will be selected.
            let ri = run.root.rinfo(rid);
            if ri.has_clone || ri.is_clone {
                debug_assert!(!rinfo_is_pushed_down(run, rid, joinrelids));
                if !relids_is_subset(&ri.required_relids, &both_input_relids) {
                    continue;
                }
                if relids_overlap(&ri.incompatible_relids, &both_input_relids) {
                    continue;
                }
            }
            if !result.iter().any(|&r| r == rid) {
                result.push(rid);
            }
        }
    }
    // EC-derived clauses cannot duplicate the joininfo ones.
    let outer_relids = relids_copy(run.mcx, &run.root.rel(outer_rel).relids);
    let eq = crate::equivclass::generate_join_implied_equalities(
        run,
        joinrelids,
        &outer_relids,
        inner_rel,
        Some(sjinfo),
    )?;
    result.extend(eq.iter().copied());
    Ok(result)
}

fn build_joinrel_joinlist(run: &mut PlannerRun<'_>, joinrel: RelId, outer_rel: RelId, inner_rel: RelId) {
    let joinrelids = relids_copy(run.mcx, &run.root.rel(joinrel).relids);
    let mut result: PgVec<'_, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    for &input in [outer_rel, inner_rel].iter() {
        for &rid in run.root.rel(input).joininfo.iter() {
            if relids_is_subset(&run.root.rinfo(rid).required_relids, &joinrelids) {
                continue;
            }
            if !result.iter().any(|&r| r == rid) {
                result.push(rid);
            }
        }
    }
    run.root.rel_mut(joinrel).joininfo = result;
}


fn is_simple_rel(root: &types_pathnodes::PlannerInfo<'_>, rel: RelId) -> bool {
    matches!(
        root.rel(rel).reloptkind,
        types_pathnodes::RELOPT_BASEREL | types_pathnodes::RELOPT_OTHER_MEMBER_REL
    )
}

// try_partitionwise_join (joinrels.c).
fn try_partitionwise_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    parent_sjinfo: &SpecialJoinInfo<'mcx>,
    parent_restrictlist: &PgVec<'mcx, types_pathnodes::RinfoId>,
) -> PgResult<()> {
    use types_pathnodes::{JOIN_ANTI, JOIN_FULL, JOIN_SEMI};
    let mcx = run.mcx;
    let rel1_is_simple = is_simple_rel(&run.root, rel1);
    let rel2_is_simple = is_simple_rel(&run.root, rel2);

    if run.root.rel(joinrel).part_scheme.is_none() || run.root.rel(joinrel).nparts == 0 {
        return Ok(());
    }
    debug_assert!(run.root.rel(joinrel).consider_partitionwise_join);
    if !crate::relnode::rel_is_partitioned(&run.root, rel1)
        || !crate::relnode::rel_is_partitioned(&run.root, rel2)
    {
        return Ok(());
    }
    debug_assert!(
        run.root.rel(rel1).consider_partitionwise_join
            && run.root.rel(rel2).consider_partitionwise_join
    );
    debug_assert!(
        !(run.root.rel(joinrel).partbounds_merged && run.root.rel(joinrel).nparts <= 0)
    );

    let merged_parts = compute_partition_bounds(run, rel1, rel2, joinrel, parent_sjinfo)?;

    let nparts = run.root.rel(joinrel).nparts;
    for cnt_parts in 0..nparts.max(0) as usize {
        let (child_rel1, child_rel2) = match &merged_parts {
            Some((p1, p2)) => (p1[cnt_parts], p2[cnt_parts]),
            None => (
                run.root.rel(rel1).part_rels[cnt_parts],
                run.root.rel(rel2).part_rels[cnt_parts],
            ),
        };
        let rel1_empty =
            child_rel1.is_none_or(|r| is_dummy_rel(&run.root, r));
        let rel2_empty =
            child_rel2.is_none_or(|r| is_dummy_rel(&run.root, r));

        // Provably empty join segments are skipped outright; these rules
        // mirror populate_joinrel_with_paths's dummy-input rules.
        match parent_sjinfo.jointype {
            JOIN_INNER | JOIN_SEMI => {
                if rel1_empty || rel2_empty {
                    continue;
                }
            }
            JOIN_LEFT | JOIN_ANTI => {
                if rel1_empty {
                    continue;
                }
            }
            JOIN_FULL => {
                if rel1_empty && rel2_empty {
                    continue;
                }
            }
            other => panic!("try_partitionwise_join (joinrels.c): jointype {other}"),
        }

        // An entirely-pruned child has no paths: reject partitionwise join
        // unless the segment was eliminated above.
        let (Some(child_rel1), Some(child_rel2)) = (child_rel1, child_rel2) else {
            run.root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        };

        // A leaf child with consider_partitionwise_join=false is a dummy rel
        // whose tlist translation was skipped in set_append_rel_size.
        if rel1_is_simple && !run.root.rel(child_rel1).consider_partitionwise_join {
            debug_assert!(
                run.root.rel(child_rel1).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL
            );
            debug_assert!(is_dummy_rel(&run.root, child_rel1));
            run.root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        }
        if rel2_is_simple && !run.root.rel(child_rel2).consider_partitionwise_join {
            debug_assert!(
                run.root.rel(child_rel2).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL
            );
            debug_assert!(is_dummy_rel(&run.root, child_rel2));
            run.root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        }

        debug_assert!(!relids_overlap(
            &run.root.rel(child_rel1).relids,
            &run.root.rel(child_rel2).relids
        ));

        let child_sjinfo = build_child_join_sjinfo(run, parent_sjinfo, child_rel1, child_rel2)?;

        let child_relids = relids_union(
            mcx,
            &run.root.rel(child_rel1).relids,
            &run.root.rel(child_rel2).relids,
        );
        let appinfos = crate::inherit::find_appinfos_by_relids(run, &child_relids);

        let mut child_restrictlist: PgVec<'mcx, types_pathnodes::RinfoId> =
            PgVec::new_in(mcx);
        for &rid in parent_restrictlist.iter() {
            child_restrictlist.push(crate::inherit::adjust_child_rinfo(run, rid, &appinfos)?);
        }

        let child_joinrel = match run.root.rel(joinrel).part_rels[cnt_parts] {
            Some(cj) => cj,
            None => {
                let cj = crate::relnode::build_child_join_rel(
                    run,
                    child_rel1,
                    child_rel2,
                    joinrel,
                    &child_restrictlist,
                    &child_sjinfo,
                    &appinfos,
                )?;
                run.root.rel_mut(joinrel).part_rels[cnt_parts] = Some(cj);
                {
                    let lp = crate::relnode::relids_take(&mut run.root.rel_mut(joinrel).live_parts);
                    run.root.rel_mut(joinrel).live_parts =
                        crate::relnode::relids_add_member(mcx, &lp, cnt_parts as u32);
                }
                {
                    let cj_relids = relids_copy(mcx, &run.root.rel(cj).relids);
                    let cur = crate::relnode::relids_take(&mut run.root.rel_mut(joinrel).all_partrels);
                    run.root.rel_mut(joinrel).all_partrels =
                        relids_union(mcx, &cur, &cj_relids);
                }
                cj
            }
        };
        debug_assert!(relids_equal(
            &run.root.rel(child_joinrel).relids,
            &crate::inherit::adjust_child_relids(mcx, &run.root.rel(joinrel).relids, &appinfos)
        ));

        populate_joinrel_with_paths(
            run,
            child_rel1,
            child_rel2,
            child_joinrel,
            &child_sjinfo,
            &child_restrictlist,
        )?;
        // C frees appinfos/child_relids/child_sjinfo per iteration; ours are
        // arena-owned and dropped as handles here.
    }
    Ok(())
}

// build_child_join_sjinfo (joinrels.c); free_child_join_sjinfo is subsumed by
// dropping the returned value.
fn build_child_join_sjinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parent_sjinfo: &SpecialJoinInfo<'mcx>,
    left_rel: RelId,
    right_rel: RelId,
) -> PgResult<SpecialJoinInfo<'mcx>> {
    let mcx = run.mcx;
    let left_relids = relids_copy(mcx, &run.root.rel(left_rel).relids);
    let right_relids = relids_copy(mcx, &run.root.rel(right_rel).relids);
    if parent_sjinfo.jointype == JOIN_INNER {
        debug_assert!(parent_sjinfo.ojrelid == 0);
        return Ok(init_dummy_sjinfo(run, left_relids, right_relids));
    }
    let left_appinfos = crate::inherit::find_appinfos_by_relids(run, &left_relids);
    let right_appinfos = crate::inherit::find_appinfos_by_relids(run, &right_relids);
    let mut sjinfo = parent_sjinfo.clone();
    sjinfo.min_lefthand =
        crate::inherit::adjust_child_relids(mcx, &sjinfo.min_lefthand, &left_appinfos);
    sjinfo.min_righthand =
        crate::inherit::adjust_child_relids(mcx, &sjinfo.min_righthand, &right_appinfos);
    sjinfo.syn_lefthand =
        crate::inherit::adjust_child_relids(mcx, &sjinfo.syn_lefthand, &left_appinfos);
    sjinfo.syn_righthand =
        crate::inherit::adjust_child_relids(mcx, &sjinfo.syn_righthand, &right_appinfos);
    // Outer-join relids need no adjustment.
    let parent_rhs_exprs = sjinfo.semi_rhs_exprs;
    let mut rhs_exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    for &eid in parent_rhs_exprs.iter() {
        let e = *run.root.expr_node(eid);
        let tr = crate::inherit::adjust_appendrel_attrs_multi(run, e, &right_appinfos)?;
        rhs_exprs.push(run.intern_expr(tr));
    }
    sjinfo.semi_rhs_exprs = rhs_exprs;
    Ok(sjinfo)
}

// compute_partition_bounds (joinrels.c). Returns the pairs lists only when
// the joinrel's bounds were merged (Some ⇔ partbounds_merged).
#[allow(clippy::type_complexity)]
fn compute_partition_bounds<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    parent_sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<Option<(PgVec<'mcx, Option<RelId>>, PgVec<'mcx, Option<RelId>>)>> {
    let mcx = run.mcx;
    if run.root.rel(joinrel).nparts == -1 {
        debug_assert!(
            run.root.rel(joinrel).boundinfo.is_none()
                && run.root.rel(joinrel).part_rels.is_empty()
        );
        let partnatts =
            run.root.rel(joinrel).part_scheme.as_ref().unwrap().partnatts as i32;
        let bounds_equal = !run.root.rel(rel1).partbounds_merged
            && !run.root.rel(rel2).partbounds_merged
            && run.root.rel(rel1).nparts == run.root.rel(rel2).nparts
            && partbounds::partition_bounds_equal(
                partnatts,
                &**run.root.rel(rel1).boundinfo.as_ref().unwrap(),
                &**run.root.rel(rel2).boundinfo.as_ref().unwrap(),
            );
        if bounds_equal {
            let bcopy = (**run.root.rel(rel1).boundinfo.as_ref().unwrap()).clone();
            let nparts = run.root.rel(rel1).nparts;
            debug_assert!(nparts > 0);
            let j = run.root.rel_mut(joinrel);
            j.boundinfo = Some(mcx::alloc_in(mcx, bcopy)?);
            j.nparts = nparts;
            j.part_rels = mcx::vec_from_elem_in(mcx, None, nparts as usize);
            return Ok(None);
        }

        let merged = {
            let scheme = run.root.rel(joinrel).part_scheme.as_ref().unwrap();
            let mut supfuncs: Vec<types_fmgr::FmgrInfo> =
                Vec::with_capacity(partnatts as usize);
            let mut collations: Vec<types_core::Oid> = Vec::with_capacity(partnatts as usize);
            for i in 0..partnatts as usize {
                supfuncs.push(fmgr_core::fmgr_info(scheme.partsupfunc[i].fn_oid)?);
                collations.push(scheme.partcollation[i]);
            }
            let image_datum = |img: &types_pathnodes::DatumImage<'_>| match img {
                types_pathnodes::DatumImage::ByVal(w) => datum::Datum::from_u64(*w),
                types_pathnodes::DatumImage::Bytes(b) => {
                    datum::Datum::from_usize(b.as_ptr() as usize)
                }
            };
            let mut cmp = |keycol: usize,
                           a: &types_pathnodes::DatumImage<'mcx>,
                           b: &types_pathnodes::DatumImage<'mcx>|
             -> PgResult<i32> {
                let mut fcinfo = types_fmgr::LocalFcinfo::<2>::new(collations[keycol]);
                fcinfo.set_arg(0, image_datum(a));
                fcinfo.set_arg(1, image_datum(b));
                let r = supfuncs[keycol].invoke(&mut fcinfo)?;
                assert!(!fcinfo.isnull, "partition comparison function returned NULL");
                Ok(r.as_i32())
            };
            let dummy_flags = |run: &PlannerRun<'mcx>, rel: RelId| {
                let n = run.root.rel(rel).nparts as usize;
                let mut v: Vec<bool> = Vec::with_capacity(n);
                for i in 0..n {
                    v.push(match run.root.rel(rel).part_rels[i] {
                        Some(p) => is_dummy_rel(&run.root, p),
                        None => true,
                    });
                }
                v
            };
            let outer_dummy = dummy_flags(run, rel1);
            let inner_dummy = dummy_flags(run, rel2);
            let outer = partbounds::MergeRel {
                nparts: run.root.rel(rel1).nparts,
                boundinfo: &**run.root.rel(rel1).boundinfo.as_ref().unwrap(),
                is_dummy: &outer_dummy,
            };
            let inner = partbounds::MergeRel {
                nparts: run.root.rel(rel2).nparts,
                boundinfo: &**run.root.rel(rel2).boundinfo.as_ref().unwrap(),
                is_dummy: &inner_dummy,
            };
            partbounds::partition_bounds_merge(
                mcx,
                partnatts,
                &mut cmp,
                &outer,
                &inner,
                parent_sjinfo.jointype,
            )?
        };
        let Some(m) = merged else {
            run.root.rel_mut(joinrel).nparts = 0;
            return Ok(None);
        };
        let nparts = m.outer_parts.len();
        debug_assert!(nparts > 0);
        let mut parts1: PgVec<'mcx, Option<RelId>> = PgVec::new_in(mcx);
        let mut parts2: PgVec<'mcx, Option<RelId>> = PgVec::new_in(mcx);
        for i in 0..nparts {
            parts1.push(match m.outer_parts[i] {
                -1 => None,
                ix => run.root.rel(rel1).part_rels[ix as usize],
            });
            parts2.push(match m.inner_parts[i] {
                -1 => None,
                ix => run.root.rel(rel2).part_rels[ix as usize],
            });
        }
        let j = run.root.rel_mut(joinrel);
        j.boundinfo = Some(mcx::alloc_in(mcx, m.merged_bounds)?);
        j.nparts = nparts as i32;
        j.partbounds_merged = true;
        j.part_rels = mcx::vec_from_elem_in(mcx, None, nparts);
        Ok(Some((parts1, parts2)))
    } else {
        debug_assert!(
            run.root.rel(joinrel).nparts > 0
                && run.root.rel(joinrel).boundinfo.is_some()
                && !run.root.rel(joinrel).part_rels.is_empty()
        );
        if run.root.rel(joinrel).partbounds_merged {
            let pairs = get_matching_part_pairs(run, joinrel, rel1, rel2);
            debug_assert!(
                pairs.0.len() == run.root.rel(joinrel).nparts as usize
                    && pairs.1.len() == run.root.rel(joinrel).nparts as usize
            );
            Ok(Some(pairs))
        } else {
            Ok(None)
        }
    }
}

// get_matching_part_pairs (joinrels.c).
#[allow(clippy::type_complexity)]
fn get_matching_part_pairs<'mcx>(
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    rel1: RelId,
    rel2: RelId,
) -> (PgVec<'mcx, Option<RelId>>, PgVec<'mcx, Option<RelId>>) {
    let mcx = run.mcx;
    let rel1_is_simple = is_simple_rel(&run.root, rel1);
    let rel2_is_simple = is_simple_rel(&run.root, rel2);
    let mut parts1: PgVec<'mcx, Option<RelId>> = PgVec::new_in(mcx);
    let mut parts2: PgVec<'mcx, Option<RelId>> = PgVec::new_in(mcx);
    for cnt_parts in 0..run.root.rel(joinrel).nparts as usize {
        let Some(child_joinrel) = run.root.rel(joinrel).part_rels[cnt_parts] else {
            // Previously-ignored empty segment: keep it ignored.
            parts1.push(None);
            parts2.push(None);
            continue;
        };
        let resolve = |rel: RelId, simple: bool| -> RelId {
            let child_relids = crate::relnode::relids_intersect(
                mcx,
                &run.root.rel(child_joinrel).relids,
                &run.root.rel(rel).all_partrels,
            );
            debug_assert_eq!(
                crate::relnode::relids_num_members(&child_relids),
                crate::relnode::relids_num_members(&run.root.rel(rel).relids)
            );
            if simple {
                let varno = crate::relnode::relids_singleton_member(&child_relids)
                    .expect("simple rel side is a single partition");
                find_base_rel(&run.root, varno)
            } else {
                find_join_rel(&run.root, &child_relids)
                    .expect("child join rel was built when planning the input join")
            }
        };
        parts1.push(Some(resolve(rel1, rel1_is_simple)));
        parts2.push(Some(resolve(rel2, rel2_is_simple)));
    }
    (parts1, parts2)
}
