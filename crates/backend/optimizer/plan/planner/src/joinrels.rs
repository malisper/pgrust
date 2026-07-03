//! joinrels.c + allpaths.c join-search slice: two-baserel INNER/LEFT/SEMI/
//! ANTI joins.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
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
            debug_assert!(run.root.rel(rel).part_scheme.is_none());
            debug_assert!(run.root.rel(rel).partial_pathlist.is_empty());
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
        let has_clauses = !run.root.rel(old_rel).joininfo.is_empty()
            || run.root.rel(old_rel).has_eclass_joins
            || has_join_restriction(run, old_rel);
        let others: &[RelId] = if level == 2 { &ones[i + 1..] } else { &ones[..] };
        for &other_rel in others {
            if relids_overlap(&run.root.rel(old_rel).relids, &run.root.rel(other_rel).relids) {
                continue;
            }
            if !has_clauses
                || have_relevant_joinclause(run, old_rel, other_rel)
                || have_join_order_restriction(run, old_rel, other_rel)
            {
                make_join_rel(run, old_rel, other_rel)?;
            }
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
                    || have_join_order_restriction(run, old_rel, new_rel)
                {
                    make_join_rel(run, old_rel, new_rel)?;
                }
            }
        }
    }

    // Last-ditch: Cartesian products against the initial rels.
    if run.root.join_rel_level[level].is_empty() {
        for &old_rel in prev.iter() {
            for &other_rel in ones.iter() {
                if !relids_overlap(&run.root.rel(old_rel).relids, &run.root.rel(other_rel).relids)
                {
                    make_join_rel(run, old_rel, other_rel)?;
                }
            }
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
    run.root.join_info_list.iter().any(|sj| {
        if relids_is_subset(&sj.min_lefthand, relids) && relids_is_subset(&sj.min_righthand, relids)
        {
            return false;
        }
        relids_overlap(&sj.min_lefthand, relids) || relids_overlap(&sj.min_righthand, relids)
    })
}

// have_join_order_restriction (joinrels.c); the has_legal_joinclause veto is
// dead at exactly two baserels (the only partner is the pair itself, and a
// relevant joinclause short-circuits before this check).
fn have_join_order_restriction(run: &PlannerRun<'_>, rel1: RelId, rel2: RelId) -> bool {
    // A direct lateral reference either way makes the pair worth attempting.
    if relids_overlap(&run.root.rel(rel1).relids, &run.root.rel(rel2).direct_lateral_relids)
        || relids_overlap(&run.root.rel(rel2).relids, &run.root.rel(rel1).direct_lateral_relids)
    {
        return true;
    }
    let r1 = &run.root.rel(rel1).relids;
    let r2 = &run.root.rel(rel2).relids;
    run.root.join_info_list.iter().any(|sj| {
        (relids_is_subset(&sj.min_lefthand, r1) && relids_is_subset(&sj.min_righthand, r2))
            || (relids_is_subset(&sj.min_lefthand, r2) && relids_is_subset(&sj.min_righthand, r1))
            || (relids_overlap(&sj.min_righthand, r1) && relids_overlap(&sj.min_righthand, r2))
            || (relids_overlap(&sj.min_lefthand, r1) && relids_overlap(&sj.min_lefthand, r2))
    })
}

fn have_relevant_joinclause(run: &PlannerRun<'_>, rel1: RelId, rel2: RelId) -> bool {
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
// a GroupResultPath (allpaths.rs set_dummy_rel_pathlist), which also fronts
// the trivial RTE_RESULT rel — no current caller can see that rel.
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
    matches!(path, types_pathnodes::PathNode::GroupResultPath(_))
}

pub fn make_join_rel(
    run: &mut PlannerRun<'_>,
    rel1: RelId,
    rel2: RelId,
) -> PgResult<Option<RelId>> {
    debug_assert!(!relids_overlap(&run.root.rel(rel1).relids, &run.root.rel(rel2).relids));
    if is_dummy_rel(&run.root, rel1) || is_dummy_rel(&run.root, rel2) {
        panic!(
            "populate_joinrel_with_paths (joinrels.c): dummy input rel; \
             mark_dummy_rel join lane unported"
        );
    }
    let mut joinrelids = relids_union(
        run.mcx,
        &run.root.rel(rel1).relids,
        &run.root.rel(rel2).relids,
    );
    // C returns NULL for an illegal pair; the search loops just skip it.
    let Some((match_sjinfo, reversed)) = join_is_legal(run, rel1, rel2, &joinrelids)? else {
        return Ok(None);
    };

    if let Some(sj) = &match_sjinfo {
        if sj.ojrelid != 0 {
            debug_assert!(sj.commute_below_l.is_none() && sj.commute_above_l.is_none());
            joinrelids = relids_add_member(run.mcx, &joinrelids, sj.ojrelid);
        }
    }
    let (rel1, rel2) = if reversed { (rel2, rel1) } else { (rel1, rel2) };

    let sjinfo = match match_sjinfo {
        Some(sj) => sj,
        None => init_dummy_sjinfo(
            run,
            relids_copy(run.mcx, &run.root.rel(rel1).relids),
            relids_copy(run.mcx, &run.root.rel(rel2).relids),
        ),
    };
    let (joinrel, restrictlist) = build_join_rel(run, joinrelids, rel1, rel2, &sjinfo)?;
    for &rid in restrictlist.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if clause.node_tag() == NodeTag::T_Const {
            panic!(
                "restriction_is_constant_false (joinrels.c): constant join qual; M2 lane"
            );
        }
    }
    match sjinfo.jointype {
        JOIN_INNER => {
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, JOIN_INNER, &sjinfo, &restrictlist)?;
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, JOIN_INNER, &sjinfo, &restrictlist)?;
        }
        JOIN_LEFT => {
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, JOIN_LEFT, &sjinfo, &restrictlist)?;
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, JOIN_RIGHT, &sjinfo, &restrictlist)?;
        }
        types_pathnodes::JOIN_SEMI => {
            if relids_is_subset(&sjinfo.min_lefthand, &run.root.rel(rel1).relids)
                && relids_is_subset(&sjinfo.min_righthand, &run.root.rel(rel2).relids)
            {
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_SEMI, &sjinfo, &restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_RIGHT_SEMI, &sjinfo, &restrictlist)?;
            }
            let unique_ok = relids_equal(&sjinfo.syn_righthand, &run.root.rel(rel2).relids) && {
                let cheapest = run.root.rel(rel2).cheapest_total_path.expect("cheapest path");
                crate::pathnode::create_unique_path(run, rel2, cheapest, &sjinfo)?.is_some()
            };
            if unique_ok {
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_UNIQUE_INNER, &sjinfo, &restrictlist)?;
                crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_UNIQUE_OUTER, &sjinfo, &restrictlist)?;
            }
        }
        types_pathnodes::JOIN_ANTI => {
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_ANTI, &sjinfo, &restrictlist)?;
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_RIGHT_ANTI, &sjinfo, &restrictlist)?;
        }
        types_pathnodes::JOIN_FULL => {
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, types_pathnodes::JOIN_FULL, &sjinfo, &restrictlist)?;
            crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, types_pathnodes::JOIN_FULL, &sjinfo, &restrictlist)?;
            // C errors here when neither mergeable nor hashable clauses
            // exist; add_paths_to_joinrel's !mergejoin_allowed arm is loud
            // upstream of that.
        }
        other => panic!(
            "populate_joinrel_with_paths (joinrels.c): jointype {other}; join-outer lane \
             covers INNER/LEFT/SEMI/ANTI"
        ),
    }
    Ok(Some(joinrel))
}

// join_is_legal (joinrels.c): None = illegal. must_be_leftjoin commutation
// cannot arise while make_outerjoininfo panics on identity-3.
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
            if relids_overlap(&r1, &sj.min_righthand) && relids_overlap(&r2, &sj.min_righthand) {
                continue;
            }
            // Associating into an SJ's RHS needs identity 3, which is loud
            // in make_outerjoininfo — nothing can legalize this join.
            return Ok(None);
        }
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
        None
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
) -> PgResult<(RelId, PgVec<'mcx, types_pathnodes::RinfoId>)> {
    let mcx = run.mcx;
    for i in 0..run.root.join_rel_list.len() {
        let jr = run.root.join_rel_list[i];
        if relids_equal(&run.root.rel(jr).relids, &joinrelids) {
            let restrictlist =
                build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel, sjinfo)?;
            return Ok((jr, restrictlist));
        }
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

    debug_assert!(run.root.placeholder_list.is_empty());
    build_joinrel_tlist(run, joinrel, outer_rel, sjinfo, sjinfo.jointype == types_pathnodes::JOIN_FULL)?;
    build_joinrel_tlist(run, joinrel, inner_rel, sjinfo, sjinfo.jointype != JOIN_INNER)?;

    {
        let d = crate::relnode::relids_difference(
            mcx,
            &run.root.rel(joinrel).direct_lateral_relids,
            &run.root.rel(joinrel).relids,
        );
        run.root.rel_mut(joinrel).direct_lateral_relids =
            if crate::relnode::relids_is_empty(&d) { None } else { d };
    }

    let restrictlist =
        build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel, sjinfo)?;
    build_joinrel_joinlist(run, joinrel, outer_rel, inner_rel);

    let he = crate::equivclass::has_relevant_eclass_joinclause(run, joinrel);
    run.root.rel_mut(joinrel).has_eclass_joins = he;

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

    run.root.join_rel_list.push(joinrel);
    debug_assert!(run.root.join_rel_hash.is_none());
    if !run.root.join_rel_level.is_empty() {
        let lev = run.root.join_cur_level;
        debug_assert!(lev > 0);
        run.root.join_rel_level[lev as usize].push(joinrel);
    }
    Ok((joinrel, restrictlist))
}

// build_joinrel_tlist (relnode.c), Var-only arm. can_null adds the OJ's
// relid to nullable-side Vars (pushed_down_joins/commute legs are dead).
fn build_joinrel_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    input_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
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
        let var = node
            .as_var()
            .unwrap_or_else(|| panic!("unexpected node type in rel targetlist: {:?}", node.node_tag()));
        debug_assert!(var.varno > 0 && relids_is_member(var.varno, &relids));
        let baserel = find_base_rel(&run.root, var.varno);
        let ndx = (var.varattno - run.root.rel(baserel).min_attr) as usize;
        if relids_is_subset(&run.root.rel(baserel).attr_needed[ndx], &relids) {
            continue;
        }
        tuple_width += run.root.rel(baserel).attr_widths[ndx] as i64;
        let in_oj = can_null
            && sjinfo.ojrelid != 0
            && relids_is_member(sjinfo.ojrelid as i32, &relids);
        let nullable_side = in_oj
            && (relids_is_member(var.varno, &sjinfo.syn_righthand)
                || (sjinfo.jointype == types_pathnodes::JOIN_FULL
                    && relids_is_member(var.varno, &sjinfo.syn_lefthand)));
        let out_id = if nullable_side {
            debug_assert!(
                sjinfo.commute_above_r.is_none() && sjinfo.commute_above_l.is_none()
            );
            let mut nulled = types_nodes::primnodes::Var {
                varnullingrels: var.varnullingrels.clone_in(mcx)?,
                ..*var
            };
            nulled.varnullingrels.add_member(mcx, sjinfo.ojrelid as i32)?;
            run.intern_expr(types_nodes::Node::mk(mcx, nulled)?)
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
    let mut result: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    for &input in [outer_rel, inner_rel].iter() {
        for &rid in run.root.rel(input).joininfo.iter() {
            if !relids_is_subset(&run.root.rinfo(rid).required_relids, joinrelids) {
                continue;
            }
            debug_assert!(!run.root.rinfo(rid).has_clone && !run.root.rinfo(rid).is_clone);
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

