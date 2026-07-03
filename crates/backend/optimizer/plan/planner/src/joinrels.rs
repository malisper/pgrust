//! joinrels.c + allpaths.c join-search slice: two-baserel inner joins only.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
use types_pathnodes::{
    JoinlistNode, RelId, Relids, SpecialJoinInfo, JOIN_INNER, RELOPT_JOINREL,
};

use crate::relnode::{
    find_base_rel, relids_copy, relids_equal, relids_is_member, relids_is_subset,
    relids_overlap, relids_union,
};
use crate::run::PlannerRun;

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
            JoinlistNode::Sub(_) => panic!(
                "make_rel_from_joinlist (allpaths.c): sub-joinlist; M2 collapse-limit lane"
            ),
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
    assert!(
        levels_needed == 2,
        "standard_join_search (allpaths.c): {levels_needed}-way join; M2 multi-join lane"
    );
    debug_assert!(run.root.join_rel_level.is_empty());
    run.root.join_rel_level.push(PgVec::new_in(run.mcx));
    run.root.join_rel_level.push(initial_rels);
    run.root.join_rel_level.push(PgVec::new_in(run.mcx));

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
    debug_assert!(level == 2);
    run.root.join_cur_level = level as i32;
    let prev = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.join_rel_level[1]);
    for (i, &old_rel) in prev.iter().enumerate() {
        debug_assert!(run.root.join_info_list.is_empty());
        if run.root.rel(old_rel).joininfo.is_empty() && !run.root.rel(old_rel).has_eclass_joins {
            panic!(
                "make_rels_by_clauseless_joins (joinrels.c): cartesian product; M2 join lane"
            );
        }
        for &other_rel in prev[i + 1..].iter() {
            let overlap = relids_overlap(
                &run.root.rel(old_rel).relids,
                &run.root.rel(other_rel).relids,
            );
            if !overlap && have_relevant_joinclause(run, old_rel, other_rel) {
                make_join_rel(run, old_rel, other_rel)?;
            }
        }
    }
    Ok(())
}

fn have_relevant_joinclause(run: &PlannerRun<'_>, rel1: RelId, rel2: RelId) -> bool {
    let (probe, other) = if run.root.rel(rel1).joininfo.len() <= run.root.rel(rel2).joininfo.len()
    {
        (rel1, rel2)
    } else {
        (rel2, rel1)
    };
    let other_relids = &run.root.rel(other).relids;
    run.root.rel(probe).joininfo.iter().any(|&rid| {
        relids_overlap(other_relids, &run.root.rinfo(rid).required_relids)
    })
}

pub fn init_dummy_sjinfo<'mcx>(
    run: &PlannerRun<'mcx>,
    left_relids: Relids<'mcx>,
    right_relids: Relids<'mcx>,
) -> SpecialJoinInfo<'mcx> {
    SpecialJoinInfo {
        min_lefthand: relids_copy(run.mcx, &left_relids),
        min_righthand: relids_copy(run.mcx, &right_relids),
        syn_lefthand: left_relids,
        syn_righthand: right_relids,
        jointype: JOIN_INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: PgVec::new_in(run.mcx),
        semi_rhs_exprs: PgVec::new_in(run.mcx),
    }
}

pub fn make_join_rel(run: &mut PlannerRun<'_>, rel1: RelId, rel2: RelId) -> PgResult<RelId> {
    debug_assert!(!relids_overlap(&run.root.rel(rel1).relids, &run.root.rel(rel2).relids));
    let joinrelids = relids_union(
        run.mcx,
        &run.root.rel(rel1).relids,
        &run.root.rel(rel2).relids,
    );
    // join_is_legal always yields (sjinfo NULL, reversed false) with no
    // special joins; add_outer_joins_to_relids is then the identity.
    assert!(
        run.root.join_info_list.is_empty(),
        "join_is_legal (joinrels.c): special joins; M2 outer-join lane"
    );
    let sjinfo = init_dummy_sjinfo(
        run,
        relids_copy(run.mcx, &run.root.rel(rel1).relids),
        relids_copy(run.mcx, &run.root.rel(rel2).relids),
    );
    let (joinrel, restrictlist) = build_join_rel(run, joinrelids, rel1, rel2, &sjinfo)?;
    for &rid in restrictlist.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if clause.node_tag() == NodeTag::T_Const {
            panic!(
                "restriction_is_constant_false (joinrels.c): constant join qual; M2 lane"
            );
        }
    }
    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel1, rel2, JOIN_INNER, &sjinfo, &restrictlist)?;
    crate::joinpath::add_paths_to_joinrel(run, joinrel, rel2, rel1, JOIN_INNER, &sjinfo, &restrictlist)?;
    Ok(joinrel)
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
                build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel);
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
    joinrel.pathtarget_id =
        Some(run.root.alloc_pathtarget(types_pathnodes::PathTarget::new(mcx)));
    let joinrel = run.root.alloc_rel(joinrel);

    debug_assert!(run.root.placeholder_list.is_empty());
    build_joinrel_tlist(run, joinrel, outer_rel);
    build_joinrel_tlist(run, joinrel, inner_rel);

    let restrictlist = build_joinrel_restrictlist(run, &joinrelids, outer_rel, inner_rel);
    build_joinrel_joinlist(run, joinrel, outer_rel, inner_rel);

    debug_assert!(run.root.eq_classes.is_empty());

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

// build_joinrel_tlist (relnode.c), Var-only inner-join arm (can_null=false).
fn build_joinrel_tlist(run: &mut PlannerRun<'_>, joinrel: RelId, input_rel: RelId) {
    let relids = relids_copy(run.mcx, &run.root.rel(joinrel).relids);
    let mut tuple_width = run.root.rel_reltarget(joinrel).width as i64;
    let exprs = crate::relnode::pgvec_clone_shallow(
        run.mcx,
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
        run.root.rel_reltarget_mut(joinrel).exprs.push(id);
    }
    run.root.rel_reltarget_mut(joinrel).width = crate::costsize::clamp_width_est(tuple_width);
}

// build_joinrel_restrictlist (relnode.c); the generate_join_implied_equalities
// leg is dead while eq_classes stay empty (initsplan.rs EC divergence keeps
// join equality clauses in the joininfo lists instead).
fn build_joinrel_restrictlist<'mcx>(
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
) -> PgVec<'mcx, types_pathnodes::RinfoId> {
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
    result
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

// set_joinrel_size_estimates + calc_joinrel_size_estimate (costsize.c),
// JOIN_INNER arm; FK selectivity is 1.0 while fkey_list stays empty.
fn set_joinrel_size_estimates<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[types_pathnodes::RinfoId],
) -> PgResult<()> {
    debug_assert!(sjinfo.jointype == JOIN_INNER);
    debug_assert!(run.root.fkey_list.is_empty());
    let jselec = crate::clausesel::clauselist_selectivity(
        run,
        restrictlist,
        0,
        sjinfo.jointype,
        Some(sjinfo),
    )?;
    let nrows = run.root.rel(outer_rel).rows * run.root.rel(inner_rel).rows * jselec;
    run.root.rel_mut(joinrel).rows = crate::costsize::clamp_row_est(nrows);
    Ok(())
}
