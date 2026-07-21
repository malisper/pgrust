//! joinpath.c nestloop + mergejoin + hashjoin arms (incl. SEMI/ANTI and
//! unique-ified semijoin inputs) with their pathnode.c/costsize.c join-cost
//! slices. Lateral-driven parameterized inners and Memoize are live, as are
//! the partial (parallel) nestloop/merge/hash arms.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
use types_pathnodes::{
    HashPath, JoinPath, MaterialPath, MergePath, MergeScanSelCache, NestPath, Path,
    PathId, PathKey, RelId, Relids, RinfoId, SpecialJoinInfo, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
};

use crate::gucs;
use crate::pathkeys::{
    build_join_pathkeys, compare_pathkeys, find_mergeclauses_for_outer_pathkeys,
    get_cheapest_path_for_pathkeys, make_inner_pathkeys_for_merge, pathkeys_contained_in,
    pathkeys_count_contained_in, select_outer_pathkeys_for_merge,
    trim_mergeclauses_for_inner_pathkeys, update_mergeclause_eclasses, PathKeysComparison,
};
use crate::costsize::{
    initial_cost_hashjoin, initial_cost_mergejoin, initial_cost_nestloop, JoinCostWorkspace,
};
use crate::pathnode::{
    add_partial_path, add_partial_path_precheck, add_path_precheck, compare_path_costs,
    create_hashjoin_path, create_material_path, create_memoize_path, create_mergejoin_path,
    create_nestloop_path, tag16, CostSelector,
};
pub use types_pathnodes::{is_outer_join, SemiAntiJoinFactors};
use crate::pathkeys::get_cheapest_parallel_safe_total_inner;
use crate::run::PlannerRun;

// PATH_PARAM_BY_PARENT (joinpath.c): paths parameterized by a parent rel
// count as parameterized by any of its children during partitionwise joins.
pub(crate) fn path_param_by_parent(run: &PlannerRun<'_>, path: PathId, rel: RelId) -> bool {
    crate::relnode::relids_overlap(
        crate::pathnode::path_req_outer(run.root.path(path).base()),
        &run.root.rel(rel).top_parent_relids,
    )
}

// PATH_PARAM_BY_REL (joinpath.c).
fn path_param_by_rel(run: &PlannerRun<'_>, path: PathId, rel: RelId) -> bool {
    crate::relnode::relids_overlap(
        crate::pathnode::path_req_outer(run.root.path(path).base()),
        &run.root.rel(rel).relids,
    ) || path_param_by_parent(run, path, rel)
}

#[allow(clippy::too_many_arguments)]
pub fn add_paths_to_joinrel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    use types_pathnodes::{
        JOIN_ANTI, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER,
        JOIN_UNIQUE_OUTER,
    };
    assert!(
        matches!(
            jointype,
            JOIN_INNER
                | JOIN_LEFT
                | JOIN_RIGHT
                | types_pathnodes::JOIN_FULL
                | JOIN_SEMI
                | JOIN_ANTI
                | JOIN_RIGHT_SEMI
                | JOIN_RIGHT_ANTI
                | JOIN_UNIQUE_INNER
                | JOIN_UNIQUE_OUTER
        ),
        "add_paths_to_joinrel (joinpath.c): unrecognized jointype {jointype}"
    );
    let inner_unique = match jointype {
        JOIN_SEMI | JOIN_ANTI => false,
        JOIN_UNIQUE_INNER => crate::relnode::relids_is_subset(
            &sjinfo.min_lefthand,
            &run.root.rel(outerrel).relids,
        ),
        _ => {
            let joinrelids = crate::relnode::relids_copy(run.mcx, &run.root.rel(joinrel).relids);
            let outerrelids = crate::relnode::relids_copy(run.mcx, &run.root.rel(outerrel).relids);
            let jt = if jointype == JOIN_UNIQUE_OUTER { JOIN_INNER } else { jointype };
            crate::analyzejoins::innerrel_is_unique(
                run, &joinrelids, &outerrelids, innerrel, jt, restrictlist, false,
            )?
        }
    };
    // FULL joins compute mergeclauses regardless of the GUCs: there may be
    // no other alternative (joinpath.c:210, 321).
    let (mergeclause_list, mergejoin_allowed) =
        if gucs::enable_mergejoin() || jointype == types_pathnodes::JOIN_FULL {
            select_mergejoin_clauses(run, joinrel, outerrel, innerrel, jointype, restrictlist)?
        } else {
            (PgVec::new_in(run.mcx), true)
        };
    let semifactors = if matches!(jointype, JOIN_SEMI | JOIN_ANTI) || inner_unique {
        Some(compute_semi_anti_join_factors(
            run, joinrel, outerrel, innerrel, jointype, sjinfo, restrictlist,
        )?)
    } else {
        None
    };
    // param_source_rels: rels an added parameterized path may require. Child
    // joins have no SpecialJoinInfos of their own: test the topmost parent's
    // relids against join_info_list.
    let param_source_rels = {
        use crate::relnode::{relids_difference, relids_overlap, relids_union};
        let mcx = run.mcx;
        let joinrelids = if run.root.rel(joinrel).reloptkind == types_pathnodes::RELOPT_OTHER_JOINREL
        {
            crate::relnode::relids_copy(mcx, &run.root.rel(joinrel).top_parent_relids)
        } else {
            crate::relnode::relids_copy(mcx, &run.root.rel(joinrel).relids)
        };
        let mut psr: Relids<'mcx> = crate::relnode::relids_empty();
        for i in 0..run.root.join_info_list.len() {
            let (min_l, min_r, jt) = {
                let sj = &run.root.join_info_list[i];
                (
                    crate::relnode::relids_copy(mcx, &sj.min_lefthand),
                    crate::relnode::relids_copy(mcx, &sj.min_righthand),
                    sj.jointype,
                )
            };
            if relids_overlap(&joinrelids, &min_r) && !relids_overlap(&joinrelids, &min_l) {
                psr = relids_union(
                    mcx,
                    &psr,
                    &relids_difference(mcx, &run.root.all_baserels, &min_r),
                );
            }
            if jt == types_pathnodes::JOIN_FULL
                && relids_overlap(&joinrelids, &min_l)
                && !relids_overlap(&joinrelids, &min_r)
            {
                psr = relids_union(
                    mcx,
                    &psr,
                    &relids_difference(mcx, &run.root.all_baserels, &min_l),
                );
            }
        }
        relids_union(mcx, &psr, &run.root.rel(joinrel).lateral_relids)
    };
    if mergejoin_allowed {
        sort_inner_and_outer(
            run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist,
            &mergeclause_list, &param_source_rels, semifactors,
        )?;
        // Nestloop can't do right/right-anti/right-semi/full joins, so
        // skipping here suppresses nothing legal (joinpath.c:286-296).
        match_unsorted_outer(run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist, &mergeclause_list, &param_source_rels, semifactors)?;
    }
    if gucs::enable_hashjoin() || jointype == types_pathnodes::JOIN_FULL {
        hash_inner_and_outer(run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist, &param_source_rels, semifactors)?;
    }
    Ok(())
}

// compute_semi_anti_join_factors (costsize.c).
#[allow(clippy::too_many_arguments)]
fn compute_semi_anti_join_factors<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
) -> PgResult<SemiAntiJoinFactors> {
    use types_pathnodes::{JOIN_ANTI, JOIN_SEMI};
    let mcx = run.mcx;
    let joinquals: PgVec<'mcx, RinfoId> = if is_outer_join(jointype) {
        let joinrelids = crate::relnode::relids_copy(mcx, &run.root.rel(joinrel).relids);
        let mut v = PgVec::new_in(mcx);
        for &rid in restrictlist {
            if !crate::joinrels::rinfo_is_pushed_down(run, rid, &joinrelids) {
                v.push(rid);
            }
        }
        v
    } else {
        let mut v = PgVec::new_in(mcx);
        v.extend(restrictlist.iter().copied());
        v
    };
    let jselec = crate::clausesel::clauselist_selectivity(
        run,
        &joinquals,
        0,
        if jointype == JOIN_ANTI { JOIN_ANTI } else { JOIN_SEMI },
        Some(sjinfo),
    )?;
    let norm_sjinfo = crate::joinrels::init_dummy_sjinfo(
        run,
        crate::relnode::relids_copy(mcx, &run.root.rel(outerrel).relids),
        crate::relnode::relids_copy(mcx, &run.root.rel(innerrel).relids),
    );
    let nselec = crate::clausesel::clauselist_selectivity(
        run,
        &joinquals,
        0,
        JOIN_INNER,
        Some(&norm_sjinfo),
    )?;
    let inner_rows = run.root.rel(innerrel).rows;
    let avgmatch = if jselec > 0.0 {
        (nselec * inner_rows / jselec).max(1.0)
    } else {
        1.0
    };
    Ok(SemiAntiJoinFactors { outer_match_frac: jselec, match_count: avgmatch })
}

// select_mergejoin_clauses (joinpath.c). For outer joins only the join's own
// clauses (not pushed-down ones) participate, and a non-mergeable joinclause
// forbids mergejoin for right/full joins (have_nonmergeable_joinclause).
fn select_mergejoin_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    restrictlist: &[RinfoId],
) -> PgResult<(PgVec<'mcx, RinfoId>, bool)> {
    let isouterjoin = is_outer_join(jointype);
    let joinrelids = crate::relnode::relids_copy(run.mcx, &run.root.rel(joinrel).relids);
    let mut have_nonmergeable_joinclause = false;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    for &rid in restrictlist {
        if isouterjoin && crate::joinrels::rinfo_is_pushed_down(run, rid, &joinrelids) {
            continue;
        }
        {
            let ri = run.root.rinfo(rid);
            if !ri.can_join || ri.mergeopfamilies.is_empty() {
                // Constant extra joinquals stay mergeable: the executor
                // handles them in right/full merge joins (FULL JOIN ON FALSE).
                let clause = *run.root.expr_node(ri.clause);
                if clause.node_tag() != NodeTag::T_Const {
                    have_nonmergeable_joinclause = true;
                }
                continue;
            }
        }
        if !clause_sides_match_join(run, rid, outerrel, innerrel) {
            have_nonmergeable_joinclause = true;
            continue;
        }
        if !run.root.rinfo(rid).outer_is_left {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let opno = clause.as_op_expr().expect("mergeclause is an OpExpr").opno;
            if lsyscache::get_commutator(opno)? == 0 {
                have_nonmergeable_joinclause = true;
                continue;
            }
        }
        update_mergeclause_eclasses(run, rid)?;
        // EC_MUST_BE_REDUNDANT: a const EC can't appear in canonical sort
        // orderings, so the clause is unusable for merging.
        if run.root.ec(run.root.rinfo(rid).left_ec.unwrap()).ec_has_const
            || run.root.ec(run.root.rinfo(rid).right_ec.unwrap()).ec_has_const
        {
            have_nonmergeable_joinclause = true;
            continue;
        }
        result.push(rid);
    }
    let mergejoin_allowed = match jointype {
        JOIN_RIGHT | types_pathnodes::JOIN_RIGHT_ANTI | types_pathnodes::JOIN_FULL => {
            !have_nonmergeable_joinclause
        }
        _ => true,
    };
    Ok((result, mergejoin_allowed))
}

// sort_inner_and_outer (joinpath.c); the partial legs are dead upstream.
#[allow(clippy::too_many_arguments)]
fn sort_inner_and_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    let save_jointype = jointype;
    if jointype == types_pathnodes::JOIN_RIGHT_SEMI {
        return Ok(());
    }
    if mergeclause_list.is_empty() {
        return Ok(());
    }
    let mut outer_path = run
        .root
        .rel(outerrel)
        .cheapest_total_path
        .expect("outer rel has a cheapest path");
    let mut inner_path = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest path");
    if path_param_by_rel(run, outer_path, innerrel)
        || path_param_by_rel(run, inner_path, outerrel)
    {
        return Ok(());
    }
    if jointype == types_pathnodes::JOIN_UNIQUE_OUTER {
        outer_path = crate::pathnode::create_unique_path(run, outerrel, outer_path, sjinfo)?
            .expect("unique-ify was proven possible");
        jointype = JOIN_INNER;
    } else if jointype == types_pathnodes::JOIN_UNIQUE_INNER {
        inner_path = crate::pathnode::create_unique_path(run, innerrel, inner_path, sjinfo)?
            .expect("unique-ify was proven possible");
        jointype = JOIN_INNER;
    }

    // A parallel-safe joinrel can carry a partial merge join: cheapest partial
    // outer joined to a parallel-safe complete inner. UNIQUE_OUTER/FULL/RIGHT/
    // RIGHT_ANTI can't (false null-extended rows or lost uniqueness).
    let (cheapest_partial_outer, cheapest_safe_inner) = if run.root.rel(joinrel).consider_parallel
        && save_jointype != types_pathnodes::JOIN_UNIQUE_OUTER
        && save_jointype != types_pathnodes::JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != types_pathnodes::JOIN_RIGHT_ANTI
        && !run.root.rel(outerrel).partial_pathlist.is_empty()
        && crate::relnode::relids_is_empty(&run.root.rel(joinrel).lateral_relids)
    {
        let cpo = run.root.rel(outerrel).partial_pathlist[0];
        let csi = if run.root.path(inner_path).base().parallel_safe {
            Some(inner_path)
        } else if save_jointype != types_pathnodes::JOIN_UNIQUE_INNER {
            let pl = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).pathlist);
            get_cheapest_parallel_safe_total_inner(run, &pl)
        } else {
            None
        };
        (Some(cpo), csi)
    } else {
        (None, None)
    };

    let all_pathkeys = select_outer_pathkeys_for_merge(run, mergeclause_list, joinrel)?;

    for i in 0..all_pathkeys.len() {
        let mut outerkeys: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
        if i == 0 {
            outerkeys.extend(all_pathkeys.iter().copied());
        } else {
            outerkeys.push(all_pathkeys[i]);
            outerkeys.extend(
                all_pathkeys
                    .iter()
                    .enumerate()
                    .filter(|&(j, _)| j != i)
                    .map(|(_, &pk)| pk),
            );
        }
        let cur_mergeclauses =
            find_mergeclauses_for_outer_pathkeys(run, &outerkeys, mergeclause_list)?;
        debug_assert_eq!(cur_mergeclauses.len(), mergeclause_list.len());
        let innerkeys = make_inner_pathkeys_for_merge(run, &cur_mergeclauses, &outerkeys)?;
        let merge_pathkeys = build_join_pathkeys(run, joinrel, jointype, &outerkeys)?;
        let want_partial = cheapest_partial_outer.is_some() && cheapest_safe_inner.is_some();
        let (pk2, mc2, ok2, ik2) = if want_partial {
            (
                crate::relnode::pgvec_clone_shallow(run.mcx, &merge_pathkeys),
                crate::relnode::pgvec_clone_shallow(run.mcx, &cur_mergeclauses),
                crate::relnode::pgvec_clone_shallow(run.mcx, &outerkeys),
                crate::relnode::pgvec_clone_shallow(run.mcx, &innerkeys),
            )
        } else {
            (PgVec::new_in(run.mcx), PgVec::new_in(run.mcx), PgVec::new_in(run.mcx), PgVec::new_in(run.mcx))
        };
        try_mergejoin_path(
            run,
            joinrel,
            outer_path,
            inner_path,
            merge_pathkeys,
            cur_mergeclauses,
            outerkeys,
            innerkeys,
            jointype,
            inner_unique,
            sjinfo,
            restrictlist,
            param_source_rels,
            semifactors,
            false,
        )?;
        if want_partial {
            try_partial_mergejoin_path(
                run,
                joinrel,
                cheapest_partial_outer.unwrap(),
                cheapest_safe_inner.unwrap(),
                pk2,
                mc2,
                ok2,
                ik2,
                jointype,
                inner_unique,
                sjinfo,
                restrictlist,
            )?;
        }
    }
    Ok(())
}

// generate_mergejoin_paths (joinpath.c).
#[allow(clippy::too_many_arguments)]
fn generate_mergejoin_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    innerrel: RelId,
    outerpath: PathId,
    mut jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
    useallclauses: bool,
    inner_cheapest_total: PathId,
    merge_pathkeys: &[PathKey],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
    is_partial: bool,
) -> PgResult<()> {
    let mcx = run.mcx;
    let save_jointype = jointype;
    if matches!(
        jointype,
        types_pathnodes::JOIN_UNIQUE_OUTER | types_pathnodes::JOIN_UNIQUE_INNER
    ) {
        jointype = JOIN_INNER;
    }
    let outer_pathkeys =
        crate::relnode::pgvec_clone_shallow(mcx, &run.root.path(outerpath).base().pathkeys);
    let mergeclauses =
        find_mergeclauses_for_outer_pathkeys(run, &outer_pathkeys, mergeclause_list)?;
    // FULL may try a clauseless merge join (its only legal plan for e.g.
    // FULL JOIN ON FALSE), per C.
    if mergeclauses.is_empty() && jointype != types_pathnodes::JOIN_FULL {
        return Ok(());
    }
    if useallclauses && mergeclauses.len() != mergeclause_list.len() {
        return Ok(());
    }

    let innersortkeys = make_inner_pathkeys_for_merge(run, &mergeclauses, &outer_pathkeys)?;

    let mut mpk: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    mpk.extend(merge_pathkeys.iter().copied());
    try_mergejoin_path(
        run,
        joinrel,
        outerpath,
        inner_cheapest_total,
        mpk,
        crate::relnode::pgvec_clone_shallow(mcx, &mergeclauses),
        PgVec::new_in(mcx),
        crate::relnode::pgvec_clone_shallow(mcx, &innersortkeys),
        jointype,
        inner_unique,
        sjinfo,
        restrictlist,
        param_source_rels,
        semifactors,
        is_partial,
    )?;

    if save_jointype == types_pathnodes::JOIN_UNIQUE_INNER {
        return Ok(());
    }

    let mut cheapest_startup_inner: Option<PathId>;
    let mut cheapest_total_inner: Option<PathId>;
    if pathkeys_contained_in(
        &innersortkeys,
        &run.root.path(inner_cheapest_total).base().pathkeys,
    ) {
        // inner_cheapest_total didn't require a sort above.
        cheapest_startup_inner = Some(inner_cheapest_total);
        cheapest_total_inner = Some(inner_cheapest_total);
    } else {
        cheapest_startup_inner = None;
        cheapest_total_inner = None;
    }
    let num_sortkeys = innersortkeys.len();

    for sortkeycnt in (1..=num_sortkeys).rev() {
        let trialsortkeys = &innersortkeys[..sortkeycnt];
        let inner_pathlist =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(innerrel).pathlist);
        let mut newclauses: Option<PgVec<'mcx, RinfoId>> = None;

        let innerpath = get_cheapest_path_for_pathkeys(
            run,
            &inner_pathlist,
            trialsortkeys,
            &crate::relnode::RELIDS_UNSET,
            CostSelector::Total,
            is_partial,
        );
        if let Some(ip) = innerpath {
            let cheaper = match cheapest_total_inner {
                None => true,
                Some(ct) => {
                    compare_path_costs(
                        run.root.path(ip).base(),
                        run.root.path(ct).base(),
                        CostSelector::Total,
                    ) < 0
                }
            };
            if cheaper {
                let clauses = if sortkeycnt < num_sortkeys {
                    let t = trim_mergeclauses_for_inner_pathkeys(run, &mergeclauses, trialsortkeys);
                    debug_assert!(!t.is_empty());
                    t
                } else {
                    crate::relnode::pgvec_clone_shallow(mcx, &mergeclauses)
                };
                newclauses = Some(crate::relnode::pgvec_clone_shallow(mcx, &clauses));
                let mut mpk: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
                mpk.extend(merge_pathkeys.iter().copied());
                try_mergejoin_path(
                    run,
                    joinrel,
                    outerpath,
                    ip,
                    mpk,
                    clauses,
                    PgVec::new_in(mcx),
                    PgVec::new_in(mcx),
                    jointype,
                    inner_unique,
                    sjinfo,
                    restrictlist,
                    param_source_rels,
                    semifactors,
                    is_partial,
                )?;
                cheapest_total_inner = Some(ip);
            }
        }

        let innerpath = get_cheapest_path_for_pathkeys(
            run,
            &inner_pathlist,
            trialsortkeys,
            &crate::relnode::RELIDS_UNSET,
            CostSelector::Startup,
            is_partial,
        );
        if let Some(ip) = innerpath {
            let cheaper = match cheapest_startup_inner {
                None => true,
                Some(cs) => {
                    compare_path_costs(
                        run.root.path(ip).base(),
                        run.root.path(cs).base(),
                        CostSelector::Startup,
                    ) < 0
                }
            };
            if cheaper {
                if Some(ip) != cheapest_total_inner {
                    let clauses = match newclauses {
                        Some(ref c) => crate::relnode::pgvec_clone_shallow(mcx, c),
                        None => {
                            if sortkeycnt < num_sortkeys {
                                let t = trim_mergeclauses_for_inner_pathkeys(
                                    run,
                                    &mergeclauses,
                                    trialsortkeys,
                                );
                                debug_assert!(!t.is_empty());
                                t
                            } else {
                                crate::relnode::pgvec_clone_shallow(mcx, &mergeclauses)
                            }
                        }
                    };
                    let mut mpk: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
                    mpk.extend(merge_pathkeys.iter().copied());
                    try_mergejoin_path(
                        run,
                        joinrel,
                        outerpath,
                        ip,
                        mpk,
                        clauses,
                        PgVec::new_in(mcx),
                        PgVec::new_in(mcx),
                        jointype,
                        inner_unique,
                        sjinfo,
                        restrictlist,
                        param_source_rels,
                        semifactors,
                        is_partial,
                    )?;
                }
                cheapest_startup_inner = Some(ip);
            }
        }
        if useallclauses {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn match_unsorted_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use types_pathnodes::{
        JOIN_ANTI, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER,
        JOIN_UNIQUE_OUTER,
    };
    let save_jointype = jointype;
    if jointype == JOIN_RIGHT_SEMI {
        return Ok(());
    }
    let (nestjoin_ok, useallclauses) = match jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_SEMI | JOIN_ANTI => (true, false),
        JOIN_RIGHT | JOIN_RIGHT_ANTI | types_pathnodes::JOIN_FULL => (false, true),
        JOIN_UNIQUE_OUTER | JOIN_UNIQUE_INNER => {
            jointype = JOIN_INNER;
            (true, false)
        }
        other => panic!("match_unsorted_outer (joinpath.c): jointype {other}"),
    };
    // A cheapest-total inner parameterized by the outer rel is only usable
    // via cheapest_parameterized_paths below.
    let mut inner_cheapest_total = run.root.rel(innerrel).cheapest_total_path;
    if let Some(ict) = inner_cheapest_total {
        if path_param_by_rel(run, ict, outerrel) {
            inner_cheapest_total = None;
        }
    }

    let mut matpath = None;
    if save_jointype == JOIN_UNIQUE_INNER {
        let Some(ict) = inner_cheapest_total else { return Ok(()) };
        inner_cheapest_total = Some(
            crate::pathnode::create_unique_path(run, innerrel, ict, sjinfo)?
                .expect("unique-ify was proven possible"),
        );
    } else if nestjoin_ok && gucs::enable_material() {
        if let Some(ict) = inner_cheapest_total {
            if !exec_materializes_output(run.root.path(ict).base().pathtype) {
                matpath = Some(create_material_path(run, innerrel, ict));
            }
        }
    }

    let outer_paths =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).pathlist);
    for &raw_outerpath in outer_paths.iter() {
        let mut outerpath = raw_outerpath;
        if path_param_by_rel(run, outerpath, innerrel) {
            continue;
        }
        if save_jointype == JOIN_UNIQUE_OUTER {
            if Some(outerpath) != run.root.rel(outerrel).cheapest_total_path {
                continue;
            }
            outerpath = crate::pathnode::create_unique_path(run, outerrel, outerpath, sjinfo)?
                .expect("unique-ify was proven possible");
        }
        let outer_pathkeys = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.path(outerpath).base().pathkeys,
        );
        let merge_pathkeys = build_join_pathkeys(run, joinrel, jointype, &outer_pathkeys)?;
        if save_jointype == JOIN_UNIQUE_INNER {
            let ict = inner_cheapest_total.expect("checked above");
            try_nestloop_path(run, joinrel, outerpath, ict, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist, param_source_rels, semifactors)?;
        } else if nestjoin_ok {
            let inner_candidates = crate::relnode::pgvec_clone_shallow(
                run.mcx,
                &run.root.rel(innerrel).cheapest_parameterized_paths,
            );
            for &innerpath in inner_candidates.iter() {
                try_nestloop_path(run, joinrel, outerpath, innerpath, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist, param_source_rels, semifactors)?;
                if let Some(mpath) = get_memoize_path(
                    run,
                    innerrel,
                    outerrel,
                    innerpath,
                    outerpath,
                    jointype,
                    inner_unique,
                    restrictlist,
                )? {
                    try_nestloop_path(run, joinrel, outerpath, mpath, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist, param_source_rels, semifactors)?;
                }
            }
            if let Some(mp) = matpath {
                try_nestloop_path(run, joinrel, outerpath, mp, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist, param_source_rels, semifactors)?;
            }
        }
        if save_jointype == JOIN_UNIQUE_OUTER {
            continue;
        }
        let Some(ict_for_merge) = inner_cheapest_total else { continue };
        // FULL may try a clauseless merge join (its only legal plan for
        // FULL JOIN ON FALSE), per C; enable_mergejoin=off is a disabled-cost
        // matter (costsize), not a generation gate.
        if !mergeclause_list.is_empty() || save_jointype == types_pathnodes::JOIN_FULL {
            generate_mergejoin_paths(
                run,
                joinrel,
                innerrel,
                outerpath,
                save_jointype,
                inner_unique,
                sjinfo,
                restrictlist,
                mergeclause_list,
                useallclauses,
                ict_for_merge,
                &merge_pathkeys,
                param_source_rels,
                semifactors,
                false,
            )?;
        }
    }

    // Partial nestloop/mergejoin over the outer rel's partial paths. Excluded
    // for the same reasons as sort_inner_and_outer's partial merge leg, plus
    // partial paths must not be parameterized (lateral_relids empty).
    if run.root.rel(joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != types_pathnodes::JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != JOIN_RIGHT_ANTI
        && !run.root.rel(outerrel).partial_pathlist.is_empty()
        && crate::relnode::relids_is_empty(&run.root.rel(joinrel).lateral_relids)
    {
        if nestjoin_ok {
            consider_parallel_nestloop(
                run, joinrel, outerrel, innerrel, save_jointype, inner_unique, sjinfo,
                restrictlist, param_source_rels, semifactors,
            )?;
        }
        let mut pict = inner_cheapest_total;
        let need_alt = match pict {
            None => true,
            Some(p) => !run.root.path(p).base().parallel_safe,
        };
        if need_alt {
            if save_jointype == JOIN_UNIQUE_INNER {
                return Ok(());
            }
            let pl = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).pathlist);
            pict = get_cheapest_parallel_safe_total_inner(run, &pl);
        }
        if let Some(ict) = pict {
            consider_parallel_mergejoin(
                run, joinrel, outerrel, innerrel, save_jointype, inner_unique, sjinfo,
                restrictlist, mergeclause_list, ict, semifactors,
            )?;
        }
    }
    Ok(())
}

// consider_parallel_nestloop (joinpath.c): partial nestloops joining a partial
// outer path to a complete inner.
#[allow(clippy::too_many_arguments)]
fn consider_parallel_nestloop<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use types_pathnodes::JOIN_UNIQUE_INNER;
    let save_jointype = jointype;
    let inner_cheapest_total = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest total path");
    if jointype == JOIN_UNIQUE_INNER {
        jointype = JOIN_INNER;
    }
    let mut matpath = None;
    if save_jointype != JOIN_UNIQUE_INNER
        && gucs::enable_material()
        && run.root.path(inner_cheapest_total).base().parallel_safe
        && !path_param_by_rel(run, inner_cheapest_total, outerrel)
        && !exec_materializes_output(run.root.path(inner_cheapest_total).base().pathtype)
    {
        matpath = Some(create_material_path(run, innerrel, inner_cheapest_total));
    }

    let outer_paths =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).partial_pathlist);
    for &outerpath in outer_paths.iter() {
        let outer_pathkeys =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.path(outerpath).base().pathkeys);
        let pathkeys = build_join_pathkeys(run, joinrel, jointype, &outer_pathkeys)?;
        let inner_candidates = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(innerrel).cheapest_parameterized_paths,
        );
        for &raw_inner in inner_candidates.iter() {
            if !run.root.path(raw_inner).base().parallel_safe {
                continue;
            }
            let innerpath = if save_jointype == JOIN_UNIQUE_INNER {
                if Some(raw_inner) != run.root.rel(innerrel).cheapest_total_path {
                    continue;
                }
                crate::pathnode::create_unique_path(run, innerrel, raw_inner, sjinfo)?
                    .expect("unique-ify was proven possible")
            } else {
                raw_inner
            };
            try_partial_nestloop_path(
                run, joinrel, outerpath, innerpath, &pathkeys, jointype, inner_unique,
                sjinfo, restrictlist, param_source_rels, semifactors,
            )?;
            if let Some(mpath) = get_memoize_path(
                run, innerrel, outerrel, innerpath, outerpath, jointype, inner_unique,
                restrictlist,
            )? {
                try_partial_nestloop_path(
                    run, joinrel, outerpath, mpath, &pathkeys, jointype, inner_unique,
                    sjinfo, restrictlist, param_source_rels, semifactors,
                )?;
            }
        }
        if let Some(mp) = matpath {
            try_partial_nestloop_path(
                run, joinrel, outerpath, mp, &pathkeys, jointype, inner_unique,
                sjinfo, restrictlist, param_source_rels, semifactors,
            )?;
        }
    }
    Ok(())
}

// consider_parallel_mergejoin (joinpath.c): partial mergejoins for each partial
// outer path joined to a complete inner.
#[allow(clippy::too_many_arguments)]
fn consider_parallel_mergejoin<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
    inner_cheapest_total: PathId,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    let outer_paths =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).partial_pathlist);
    for &outerpath in outer_paths.iter() {
        let outer_pathkeys =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.path(outerpath).base().pathkeys);
        let merge_pathkeys = build_join_pathkeys(run, joinrel, jointype, &outer_pathkeys)?;
        generate_mergejoin_paths(
            run,
            joinrel,
            innerrel,
            outerpath,
            jointype,
            inner_unique,
            sjinfo,
            restrictlist,
            mergeclause_list,
            false,
            inner_cheapest_total,
            &merge_pathkeys,
            &crate::relnode::RELIDS_UNSET,
            semifactors,
            true,
        )?;
    }
    Ok(())
}

fn exec_materializes_output(pathtype: u16) -> bool {
    pathtype == tag16(NodeTag::T_Material)
        || pathtype == tag16(NodeTag::T_Sort)
        || pathtype == tag16(NodeTag::T_FunctionScan)
        || pathtype == tag16(NodeTag::T_CteScan)
        || pathtype == tag16(NodeTag::T_NamedTuplestoreScan)
        || pathtype == tag16(NodeTag::T_WorkTableScan)
}

// extract_lateral_vars_from_PHVs (joinpath.c): lateral references within
// PHVs due to be evaluated at innerrelids, usable as memoize cache keys.
fn extract_lateral_vars_from_phvs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    innerrelids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<PgVec<'mcx, types_pathnodes::NodeId>> {
    let mcx = run.mcx;
    let mut ph_lateral_vars: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    if !run.root.hasLateralRTEs {
        return Ok(ph_lateral_vars);
    }
    // Memoize never sits atop joinrel paths, so multi-member eval sites are
    // uninteresting.
    if crate::relnode::relids_num_members(innerrelids) > 1 {
        return Ok(ph_lateral_vars);
    }
    let phids = crate::relnode::pgvec_clone_shallow(mcx, &run.root.placeholder_list);
    for &phid in phids.iter() {
        {
            let ph = run.root.phinfo(phid);
            if crate::relnode::relids_is_empty(&ph.ph_lateral) {
                continue;
            }
            if !crate::relnode::relids_equal(&ph.ph_eval_at, innerrelids) {
                continue;
            }
        }
        let phexpr_id = run.root.phinfo(phid).ph_var_phexpr;
        let phexpr = *run.root.expr_node(phexpr_id);
        // A PHV expression not referencing innerrelids caches on the whole
        // expression (fewer distinct values than its input Vars).
        let expr_varnos = crate::initsplan::pull_varnos_relids(run, phexpr)?;
        if !crate::relnode::relids_overlap(&expr_varnos, innerrelids) {
            ph_lateral_vars.push(phexpr_id);
            continue;
        }
        let ph_lateral = crate::relnode::relids_copy(mcx, &run.root.phinfo(phid).ph_lateral);
        for node in &vars::pull_vars_of_level(mcx, phexpr, 0)? {
            if let Some(v) = node.as_var() {
                debug_assert!(v.varlevelsup == 0);
                if crate::relnode::relids_is_member(v.varno, &ph_lateral) {
                    ph_lateral_vars.push(run.intern_expr(node));
                }
            } else {
                let phv = node.as_place_holder_var().expect("pull_vars_of_level node");
                debug_assert!(phv.phlevelsup == 0);
                let sub = crate::placeholder::find_placeholder_info(run, phv)?;
                if crate::relnode::relids_is_subset(
                    &run.root.phinfo(sub).ph_eval_at,
                    &ph_lateral,
                ) {
                    ph_lateral_vars.push(run.intern_expr(node));
                }
            }
        }
    }
    Ok(ph_lateral_vars)
}

// paraminfo_get_equal_hashops (joinpath.c). None = not hashable.
#[allow(clippy::type_complexity)]
fn paraminfo_get_equal_hashops<'mcx>(
    run: &mut PlannerRun<'mcx>,
    inner_path: PathId,
    outerrel: RelId,
    innerrel: RelId,
    ph_lateral_vars: &[types_pathnodes::NodeId],
) -> PgResult<Option<(PgVec<'mcx, types_pathnodes::NodeId>, PgVec<'mcx, u32>, bool)>> {
    let mcx = run.mcx;
    let mut param_exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let mut operators: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    let mut binary_mode = false;

    let ppi_clauses: PgVec<'mcx, RinfoId> =
        match &run.root.path(inner_path).base().param_info {
            Some(pi) => crate::relnode::pgvec_clone_shallow(mcx, &pi.ppi_clauses),
            None => PgVec::new_in(mcx),
        };
    for &rid in ppi_clauses.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let Some(opexpr) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
            return Ok(None);
        };
        if !clause_sides_match_join(run, rid, outerrel, innerrel) {
            return Ok(None);
        }
        let ri = run.root.rinfo(rid);
        let (expr, hasheqoperator) = if ri.outer_is_left {
            (opexpr.args.nth(0), ri.left_hasheqoperator)
        } else {
            (opexpr.args.nth(1), ri.right_hasheqoperator)
        };
        if hasheqoperator == 0 {
            return Ok(None);
        }
        if !param_exprs.iter().any(|&e| types_nodes::equal(*run.root.expr_node(e), expr)) {
            operators.push(hasheqoperator);
            param_exprs.push(run.intern_expr(expr));
        }
        // A non-hashable join operator may distinguish values the hash
        // equality operator cannot (-0.0 vs +0.0): compare bit by bit.
        if run.root.rinfo(rid).hashjoinoperator == 0 {
            binary_mode = true;
        }
    }

    // C: lateral_vars = list_concat(ph_lateral_vars, innerrel->lateral_vars)
    // — PHV-extracted keys first; order shapes the cache-key display.
    let mut lateral_vars: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    lateral_vars.extend(ph_lateral_vars.iter().copied());
    lateral_vars.extend(run.root.rel(innerrel).lateral_vars.iter().copied());
    for &id in lateral_vars.iter() {
        let expr = *run.root.expr_node(id);
        if clauses::contain_volatile_functions(expr)? {
            return Ok(None);
        }
        let typ = crate::costsize::expr_type_typmod(expr).0;
        let entry = typcache::lookup_type_cache(
            typ,
            typcache::TYPECACHE_HASH_PROC | typcache::TYPECACHE_EQ_OPR,
        )?;
        if entry.hash_proc() == 0 || entry.eq_opr() == 0 {
            return Ok(None);
        }
        if !param_exprs.iter().any(|&e| types_nodes::equal(*run.root.expr_node(e), expr)) {
            operators.push(entry.eq_opr());
            param_exprs.push(id);
        }
        // Lateral Vars flow into opaque expressions: binary mode always.
        binary_mode = true;
    }
    Ok(Some((param_exprs, operators, binary_mode)))
}

// get_memoize_path (joinpath.c).
#[allow(clippy::too_many_arguments)]
fn get_memoize_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    innerrel: RelId,
    outerrel: RelId,
    inner_path: PathId,
    outer_path: PathId,
    jointype: u32,
    inner_unique: bool,
    restrictlist: &[RinfoId],
) -> PgResult<Option<PathId>> {
    use types_pathnodes::{JOIN_ANTI, JOIN_SEMI};
    if !gucs::enable_memoize() {
        return Ok(None);
    }
    // A single expected outer row can never repeat a parameter value.
    if run.root.rel(run.root.path(outer_path).base().parent).rows < 2.0 {
        return Ok(None);
    }
    let ph_lateral_vars = {
        let inner_relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(innerrel).relids);
        extract_lateral_vars_from_phvs(run, &inner_relids)?
    };
    let has_ppi_clauses = run
        .root
        .path(inner_path)
        .base()
        .param_info
        .as_ref()
        .is_some_and(|pi| !pi.ppi_clauses.is_empty());
    // No cache key at all sounds more like a job for Material.
    if !has_ppi_clauses
        && run.root.rel(innerrel).lateral_vars.is_empty()
        && ph_lateral_vars.is_empty()
    {
        return Ok(None);
    }
    // Non-unique SEMI/ANTI nestloops don't scan the inner to completion, so
    // cache entries could never be marked complete.
    if !inner_unique && (jointype == JOIN_SEMI || jointype == JOIN_ANTI) {
        return Ok(None);
    }
    // Unique joins skip to the next outer tuple on the first match; singlerow
    // caching only works when the whole join condition is parameterized.
    if inner_unique {
        let serials = {
            let Some(pi) = &run.root.path(inner_path).base().param_info else {
                return Ok(None);
            };
            crate::relnode::relids_copy(run.mcx, &pi.ppi_serials)
        };
        for &rid in restrictlist {
            if !crate::relnode::relids_is_member(run.root.rinfo(rid).rinfo_serial, &serials) {
                return Ok(None);
            }
        }
    }
    // A cache hit would skip volatile-function calls the query expects.
    {
        let exprs = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel_reltarget(innerrel).exprs,
        );
        for &e in exprs.iter() {
            if clauses::contain_volatile_functions(*run.root.expr_node(e))? {
                return Ok(None);
            }
        }
        let base_rinfos = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(innerrel).baserestrictinfo,
        );
        for &rid in base_rinfos.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            if clauses::contain_volatile_functions(clause)? {
                return Ok(None);
            }
        }
        let ppi_clauses: PgVec<'mcx, RinfoId> =
            match &run.root.path(inner_path).base().param_info {
                Some(pi) => crate::relnode::pgvec_clone_shallow(run.mcx, &pi.ppi_clauses),
                None => PgVec::new_in(run.mcx),
            };
        for &rid in ppi_clauses.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            if clauses::contain_volatile_functions(clause)? {
                return Ok(None);
            }
        }
    }
    // Parameterization refers to the topmost parent of the outer rel.
    let outerrel = run.root.rel(outerrel).top_parent.unwrap_or(outerrel);
    let Some((param_exprs, hash_operators, binary_mode)) =
        paraminfo_get_equal_hashops(run, inner_path, outerrel, innerrel, &ph_lateral_vars)?
    else {
        return Ok(None);
    };
    let calls = run.root.path(outer_path).base().rows;
    Ok(Some(create_memoize_path(
        run,
        innerrel,
        inner_path,
        param_exprs,
        hash_operators,
        inner_unique,
        binary_mode,
        calls,
    )))
}

#[allow(clippy::too_many_arguments)]
fn try_nestloop_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use crate::relnode::{
        relids_copy, relids_difference, relids_is_empty, relids_is_member, relids_is_subset,
        relids_overlap, relids_union,
    };
    let mcx = run.mcx;
    let inner_paramrels =
        relids_copy(mcx, crate::pathnode::path_req_outer(run.root.path(inner_path).base()));
    let outer_paramrels =
        relids_copy(mcx, crate::pathnode::path_req_outer(run.root.path(outer_path).base()));
    if sjinfo.ojrelid != 0
        && (relids_is_member(sjinfo.ojrelid as i32, &inner_paramrels)
            || relids_is_member(sjinfo.ojrelid as i32, &outer_paramrels))
    {
        return Ok(());
    }
    // Input-path parameterizations refer to topmost parents
    // (reparameterize_path_by_child runs at create_plan), so the tests below
    // use topmost-parent relids too.
    let top_or_self = |r: RelId| {
        if relids_is_empty(&run.root.rel(r).top_parent_relids) {
            relids_copy(mcx, &run.root.rel(r).relids)
        } else {
            relids_copy(mcx, &run.root.rel(r).top_parent_relids)
        }
    };
    let innerrelids = top_or_self(run.root.path(inner_path).base().parent);
    let outerrelids = top_or_self(run.root.path(outer_path).base().parent);

    // calc_nestloop_required_outer (pathnode.c).
    debug_assert!(!relids_overlap(&outer_paramrels, &innerrelids));
    let required_outer = if relids_is_empty(&inner_paramrels) {
        relids_copy(mcx, &outer_paramrels)
    } else {
        let u = relids_union(mcx, &outer_paramrels, &inner_paramrels);
        let d = relids_difference(mcx, &u, &outerrelids);
        if relids_is_empty(&d) {
            crate::relnode::relids_empty()
        } else {
            d
        }
    };
    if !relids_is_empty(&required_outer)
        && !relids_overlap(&required_outer, param_source_rels)
    {
        // allow_star_schema_join (joinpath.c).
        let star = relids_overlap(&inner_paramrels, &outerrelids)
            && !relids_is_subset(&inner_paramrels, &outerrelids);
        if !star {
            return Ok(());
        }
    }

    // A parent-parameterized inner is translated at create_plan; skip if the
    // translation would fail.
    {
        let outer_parent = run.root.path(outer_path).base().parent;
        if path_param_by_parent(run, inner_path, outer_parent)
            && !crate::createplan::path_is_reparameterizable_by_child(run, inner_path, outer_parent)
        {
            return Ok(());
        }
    }

    let workspace = initial_cost_nestloop(run, jointype, inner_unique, outer_path, inner_path)?;

    if add_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.startup_cost, workspace.total_cost, pathkeys, &required_outer) {
        let path = create_nestloop_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            sjinfo,
            outer_path,
            inner_path,
            pathkeys,
            restrictlist,
            &required_outer,
            semifactors,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// try_partial_nestloop_path (joinpath.c). Partial paths are never
// parameterized: the joinrel has no lateral rels and the outer path no
// required_outer; a parameterized inner must be fully satisfied by the outer.
#[allow(clippy::too_many_arguments)]
fn try_partial_nestloop_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    _param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use crate::relnode::{relids_copy, relids_is_empty, relids_is_subset};
    let mcx = run.mcx;
    debug_assert!(relids_is_empty(&run.root.rel(joinrel).lateral_relids));
    debug_assert!(relids_is_empty(crate::pathnode::path_req_outer(
        run.root.path(outer_path).base()
    )));
    let inner_paramrels =
        relids_copy(mcx, crate::pathnode::path_req_outer(run.root.path(inner_path).base()));
    if !relids_is_empty(&inner_paramrels) {
        let outer_parent = run.root.path(outer_path).base().parent;
        let outerrelids = if relids_is_empty(&run.root.rel(outer_parent).top_parent_relids) {
            relids_copy(mcx, &run.root.rel(outer_parent).relids)
        } else {
            relids_copy(mcx, &run.root.rel(outer_parent).top_parent_relids)
        };
        if !relids_is_subset(&inner_paramrels, &outerrelids) {
            return Ok(());
        }
    }
    {
        let outer_parent = run.root.path(outer_path).base().parent;
        if path_param_by_parent(run, inner_path, outer_parent)
            && !crate::createplan::path_is_reparameterizable_by_child(run, inner_path, outer_parent)
        {
            return Ok(());
        }
    }

    let workspace = initial_cost_nestloop(run, jointype, inner_unique, outer_path, inner_path)?;
    if !add_partial_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.total_cost, pathkeys) {
        return Ok(());
    }
    let path = create_nestloop_path(
        run, joinrel, jointype, &workspace, inner_unique, sjinfo, outer_path, inner_path,
        pathkeys, restrictlist, &crate::relnode::RELIDS_UNSET, semifactors,
    )?;
    add_partial_path(run, joinrel, path);
    Ok(())
}

// try_mergejoin_path (joinpath.c); is_partial delegates to the partial arm.
#[allow(clippy::too_many_arguments)]
fn try_mergejoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: PgVec<'mcx, PathKey>,
    mergeclauses: PgVec<'mcx, RinfoId>,
    mut outersortkeys: PgVec<'mcx, PathKey>,
    mut innersortkeys: PgVec<'mcx, PathKey>,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    _semifactors: Option<SemiAntiJoinFactors>,
    is_partial: bool,
) -> PgResult<()> {
    use crate::relnode::{relids_is_empty, relids_is_member, relids_overlap};
    if is_partial {
        return try_partial_mergejoin_path(
            run, joinrel, outer_path, inner_path, pathkeys, mergeclauses, outersortkeys,
            innersortkeys, jointype, inner_unique, sjinfo, restrictlist,
        );
    }
    if sjinfo.ojrelid != 0
        && (relids_is_member(
            sjinfo.ojrelid as i32,
            crate::pathnode::path_req_outer(run.root.path(inner_path).base()),
        ) || relids_is_member(
            sjinfo.ojrelid as i32,
            crate::pathnode::path_req_outer(run.root.path(outer_path).base()),
        ))
    {
        return Ok(());
    }
    let required_outer = crate::pathnode::calc_non_nestloop_required_outer(
        run,
        run.mcx,
        outer_path,
        inner_path,
    );
    if !relids_is_empty(&required_outer) && !relids_overlap(&required_outer, param_source_rels) {
        return Ok(());
    }

    let mut outer_presorted_keys = 0usize;
    if !outersortkeys.is_empty() {
        let (contained, n) = pathkeys_count_contained_in(
            &outersortkeys,
            &run.root.path(outer_path).base().pathkeys,
        );
        if contained {
            outersortkeys.clear();
        } else {
            outer_presorted_keys = n;
        }
    }
    if !innersortkeys.is_empty()
        && pathkeys_contained_in(&innersortkeys, &run.root.path(inner_path).base().pathkeys)
    {
        innersortkeys.clear();
    }

    let workspace = initial_cost_mergejoin(
        run,
        jointype,
        &mergeclauses,
        outer_path,
        inner_path,
        &outersortkeys,
        &innersortkeys,
        outer_presorted_keys,
    )?;

    if add_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.startup_cost, workspace.total_cost, &pathkeys, &required_outer) {
        let path = create_mergejoin_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            sjinfo,
            outer_path,
            inner_path,
            restrictlist,
            pathkeys,
            &required_outer,
            mergeclauses,
            outersortkeys,
            innersortkeys,
            outer_presorted_keys,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// try_partial_mergejoin_path (joinpath.c). required_outer is always empty here.
#[allow(clippy::too_many_arguments)]
fn try_partial_mergejoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: PgVec<'mcx, PathKey>,
    mergeclauses: PgVec<'mcx, RinfoId>,
    mut outersortkeys: PgVec<'mcx, PathKey>,
    mut innersortkeys: PgVec<'mcx, PathKey>,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    use crate::relnode::relids_is_empty;
    debug_assert!(relids_is_empty(&run.root.rel(joinrel).lateral_relids));
    debug_assert!(relids_is_empty(crate::pathnode::path_req_outer(
        run.root.path(outer_path).base()
    )));
    if !relids_is_empty(crate::pathnode::path_req_outer(run.root.path(inner_path).base())) {
        return Ok(());
    }

    let mut outer_presorted_keys = 0usize;
    if !outersortkeys.is_empty() {
        let (contained, n) = pathkeys_count_contained_in(
            &outersortkeys,
            &run.root.path(outer_path).base().pathkeys,
        );
        if contained {
            outersortkeys.clear();
        } else {
            outer_presorted_keys = n;
        }
    }
    if !innersortkeys.is_empty()
        && pathkeys_contained_in(&innersortkeys, &run.root.path(inner_path).base().pathkeys)
    {
        innersortkeys.clear();
    }

    let workspace = initial_cost_mergejoin(
        run,
        jointype,
        &mergeclauses,
        outer_path,
        inner_path,
        &outersortkeys,
        &innersortkeys,
        outer_presorted_keys,
    )?;
    if !add_partial_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.total_cost, &pathkeys) {
        return Ok(());
    }
    let path = create_mergejoin_path(
        run, joinrel, jointype, &workspace, inner_unique, sjinfo, outer_path, inner_path,
        restrictlist, pathkeys, &crate::relnode::RELIDS_UNSET, mergeclauses, outersortkeys, innersortkeys,
        outer_presorted_keys,
    )?;
    add_partial_path(run, joinrel, path);
    Ok(())
}

// hash_inner_and_outer (joinpath.c).
#[allow(clippy::too_many_arguments)]
fn hash_inner_and_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use types_pathnodes::{JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER};
    let save_jointype = jointype;
    let isouterjoin = is_outer_join(jointype);
    let joinrelids = crate::relnode::relids_copy(run.mcx, &run.root.rel(joinrel).relids);
    let mut hashclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    for &ri in restrictlist {
        if isouterjoin && crate::joinrels::rinfo_is_pushed_down(run, ri, &joinrelids) {
            continue;
        }
        let r = run.root.rinfo(ri);
        if !r.can_join || r.hashjoinoperator == 0 {
            continue;
        }
        if !clause_sides_match_join(run, ri, outerrel, innerrel) {
            continue;
        }
        if !run.root.rinfo(ri).outer_is_left {
            let clause = *run.root.expr_node(run.root.rinfo(ri).clause);
            let opno = clause.as_op_expr().expect("hashclause is an OpExpr").opno;
            if lsyscache::get_commutator(opno)? == 0 {
                continue;
            }
        }
        hashclauses.push(ri);
    }
    if hashclauses.is_empty() {
        return Ok(());
    }

    let cheapest_startup_outer = run.root.rel(outerrel).cheapest_startup_path;
    let mut cheapest_total_outer = run
        .root
        .rel(outerrel)
        .cheapest_total_path
        .expect("outer rel has a cheapest total path");
    let mut cheapest_total_inner = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest total path");

    // A cheapest-total path parameterized by the other rel rules hashjoin out.
    if path_param_by_rel(run, cheapest_total_outer, innerrel)
        || path_param_by_rel(run, cheapest_total_inner, outerrel)
    {
        return Ok(());
    }

    if jointype == JOIN_UNIQUE_OUTER {
        cheapest_total_outer =
            crate::pathnode::create_unique_path(run, outerrel, cheapest_total_outer, sjinfo)?
                .expect("unique-ify was proven possible");
        jointype = JOIN_INNER;
        try_hashjoin_path(
            run, joinrel, cheapest_total_outer, cheapest_total_inner, &hashclauses, jointype,
            inner_unique, sjinfo, restrictlist, param_source_rels, semifactors,
        )?;
        return Ok(());
    }
    if jointype == JOIN_UNIQUE_INNER {
        cheapest_total_inner =
            crate::pathnode::create_unique_path(run, innerrel, cheapest_total_inner, sjinfo)?
                .expect("unique-ify was proven possible");
        jointype = JOIN_INNER;
        try_hashjoin_path(
            run, joinrel, cheapest_total_outer, cheapest_total_inner, &hashclauses, jointype,
            inner_unique, sjinfo, restrictlist, param_source_rels, semifactors,
        )?;
        if let Some(cso) = cheapest_startup_outer {
            if cso != cheapest_total_outer {
                try_hashjoin_path(
                    run, joinrel, cso, cheapest_total_inner, &hashclauses, jointype,
                    inner_unique, sjinfo, restrictlist, param_source_rels, semifactors,
                )?;
            }
        }
        return Ok(());
    }

    if let Some(cso) = cheapest_startup_outer {
        try_hashjoin_path(
            run, joinrel, cso, cheapest_total_inner, &hashclauses, jointype, inner_unique, sjinfo,
            restrictlist, param_source_rels, semifactors,
        )?;
    }

    let outer_params =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).cheapest_parameterized_paths);
    let inner_params =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).cheapest_parameterized_paths);
    for &op in outer_params.iter() {
        if path_param_by_rel(run, op, innerrel) {
            continue;
        }
        for &ip in inner_params.iter() {
            if path_param_by_rel(run, ip, outerrel) {
                continue;
            }
            if Some(op) == cheapest_startup_outer && ip == cheapest_total_inner {
                continue;
            }
            try_hashjoin_path(
                run, joinrel, op, ip, &hashclauses, jointype, inner_unique, sjinfo, restrictlist,
                param_source_rels, semifactors,
            )?;
        }
    }

    // Partial hash join. A partial inner (shared table built in parallel) needs
    // enable_parallel_hash; otherwise a parallel-safe complete inner is copied
    // per worker. RIGHT_SEMI is excluded (no cross-worker match-flag coherence);
    // FULL/RIGHT/RIGHT_ANTI can't share match bits across backends.
    if run.root.rel(joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != types_pathnodes::JOIN_RIGHT_SEMI
        && !run.root.rel(outerrel).partial_pathlist.is_empty()
        && crate::relnode::relids_is_empty(&run.root.rel(joinrel).lateral_relids)
    {
        let cheapest_partial_outer = run.root.rel(outerrel).partial_pathlist[0];
        if !run.root.rel(innerrel).partial_pathlist.is_empty()
            && save_jointype != JOIN_UNIQUE_INNER
            && gucs::enable_parallel_hash()
        {
            let cheapest_partial_inner = run.root.rel(innerrel).partial_pathlist[0];
            try_partial_hashjoin_path(
                run, joinrel, cheapest_partial_outer, cheapest_partial_inner, &hashclauses,
                jointype, inner_unique, sjinfo, restrictlist, semifactors, true,
            )?;
        }
        let cheapest_safe_inner = if save_jointype == types_pathnodes::JOIN_FULL
            || save_jointype == JOIN_RIGHT
            || save_jointype == types_pathnodes::JOIN_RIGHT_ANTI
        {
            None
        } else if run.root.path(cheapest_total_inner).base().parallel_safe {
            Some(cheapest_total_inner)
        } else if save_jointype != JOIN_UNIQUE_INNER {
            let pl = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).pathlist);
            get_cheapest_parallel_safe_total_inner(run, &pl)
        } else {
            None
        };
        if let Some(csi) = cheapest_safe_inner {
            try_partial_hashjoin_path(
                run, joinrel, cheapest_partial_outer, csi, &hashclauses, jointype, inner_unique,
                sjinfo, restrictlist, semifactors, false,
            )?;
        }
    }
    Ok(())
}

// clause_sides_match_join (joinpath.c): sets outer_is_left as a side effect.
fn clause_sides_match_join(
    run: &mut PlannerRun<'_>,
    ri: RinfoId,
    outerrel: RelId,
    innerrel: RelId,
) -> bool {
    let (left, right) = {
        let r = run.root.rinfo(ri);
        (r.left_relids.clone(), r.right_relids.clone())
    };
    let outer_relids = run.root.rel(outerrel).relids.clone();
    let inner_relids = run.root.rel(innerrel).relids.clone();
    if crate::relnode::relids_is_subset(&left, &outer_relids)
        && crate::relnode::relids_is_subset(&right, &inner_relids)
    {
        run.root.rinfo_mut(ri).outer_is_left = true;
        true
    } else if crate::relnode::relids_is_subset(&left, &inner_relids)
        && crate::relnode::relids_is_subset(&right, &outer_relids)
    {
        run.root.rinfo_mut(ri).outer_is_left = false;
        true
    } else {
        false
    }
}

#[allow(clippy::too_many_arguments)]
fn try_hashjoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    hashclauses: &[RinfoId],
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    param_source_rels: &Relids<'mcx>,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    use crate::relnode::{relids_is_empty, relids_is_member, relids_overlap};
    if sjinfo.ojrelid != 0
        && (relids_is_member(
            sjinfo.ojrelid as i32,
            crate::pathnode::path_req_outer(run.root.path(inner_path).base()),
        ) || relids_is_member(
            sjinfo.ojrelid as i32,
            crate::pathnode::path_req_outer(run.root.path(outer_path).base()),
        ))
    {
        return Ok(());
    }
    let required_outer = crate::pathnode::calc_non_nestloop_required_outer(
        run,
        run.mcx,
        outer_path,
        inner_path,
    );
    if !relids_is_empty(&required_outer) && !relids_overlap(&required_outer, param_source_rels) {
        return Ok(());
    }
    let workspace = initial_cost_hashjoin(run, hashclauses, outer_path, inner_path, false);
    if add_path_precheck(
        run,
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        &[],
        &required_outer,
    ) {
        let path = create_hashjoin_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            sjinfo,
            outer_path,
            inner_path,
            false,
            restrictlist,
            &required_outer,
            hashclauses,
            semifactors,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// try_partial_hashjoin_path (joinpath.c). parallel_hash => shared table built
// from a partial inner; otherwise the complete inner is copied per worker.
#[allow(clippy::too_many_arguments)]
fn try_partial_hashjoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    hashclauses: &[RinfoId],
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    semifactors: Option<SemiAntiJoinFactors>,
    parallel_hash: bool,
) -> PgResult<()> {
    use crate::relnode::relids_is_empty;
    debug_assert!(relids_is_empty(&run.root.rel(joinrel).lateral_relids));
    debug_assert!(relids_is_empty(crate::pathnode::path_req_outer(
        run.root.path(outer_path).base()
    )));
    if !relids_is_empty(crate::pathnode::path_req_outer(run.root.path(inner_path).base())) {
        return Ok(());
    }
    let workspace = initial_cost_hashjoin(run, hashclauses, outer_path, inner_path, parallel_hash);
    if !add_partial_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.total_cost, &[]) {
        return Ok(());
    }
    let path = create_hashjoin_path(
        run, joinrel, jointype, &workspace, inner_unique, sjinfo, outer_path, inner_path,
        parallel_hash, restrictlist, &crate::relnode::RELIDS_UNSET, hashclauses, semifactors,
    )?;
    add_partial_path(run, joinrel, path);
    Ok(())
}

