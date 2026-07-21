//! prepunion.c: plan_set_operations and its subroutines.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::list::{NodeList, OidList};
use types_nodes::parsenodes::{RTEKind, SetOperation, SetOperationStmt, SortGroupClause};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    NodeId, PathId, PathKey, RelId, Relids, AGGSPLIT_SIMPLE, AGG_HASHED, SETOPCMD_EXCEPT,
    SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL, SETOP_HASHED, SETOP_SORTED,
    UPPERREL_SETOP,
};

use crate::pathnode::{add_path, create_pathtarget, set_cheapest, SubqueryScanInfo};
use crate::run::PlannerRun;

pub fn plan_set_operations<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<RelId> {
    let parse = run.parse();
    let top_node = parse.setOperations.expect("plan_set_operations without setOperations");
    let topop = top_node.as_set_operation_stmt().expect("setOperations is a SetOperationStmt");

    let jt = parse.jointree.expect("jointree is a FromExpr");
    debug_assert!(jt.fromlist.is_nil() && jt.quals.is_none());
    debug_assert!(parse.groupClause.is_nil() && parse.havingQual.is_none());
    debug_assert!(parse.windowClause.is_nil() && parse.distinctClause.is_nil());

    debug_assert!(run.root.eq_classes.is_empty());
    run.root.ec_merging_done = true;

    crate::relnode::setup_simple_rel_arrays(&mut run.root, parse.rtable.len());

    let mut node = topop.larg.expect("setop larg");
    while let Some(op) = node.as_set_operation_stmt() {
        node = op.larg.expect("setop larg");
    }
    let leftmost_rti = node.as_range_tbl_ref().expect("setop leaf is a RangeTblRef").rtindex;
    let leftmost_query =
        run.rte(leftmost_rti as usize).subquery.expect("setop leaf RTE has a subquery");
    let refnames_tlist = &leftmost_query.targetList;

    if run.root.hasRecursion {
        let (setop_rel, top_tlist) = generate_recursion_path(run, topop, refnames_tlist)?;
        run.processed_tlist = Some(mcx::leak_in(mcx::alloc_in(run.mcx, top_tlist)?));
        return Ok(setop_rel);
    }
    let (setop_rel, top_tlist, _trivial) = recurse_set_operations(
        run,
        top_node,
        None,
        &topop.colTypes,
        &topop.colCollations,
        refnames_tlist,
    )?;

    run.processed_tlist = Some(mcx::leak_in(mcx::alloc_in(run.mcx, top_tlist)?));
    Ok(setop_rel)
}

fn recurse_set_operations<'mcx>(
    run: &mut PlannerRun<'mcx>,
    set_op: Node<'mcx>,
    parent_op: Option<&'mcx SetOperationStmt<'mcx>>,
    col_types: &OidList<'mcx>,
    col_collations: &OidList<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<(RelId, NodeList<'mcx>, bool)> {
    if let Some(rtr) = set_op.as_range_tbl_ref() {
        let rti = rtr.rtindex;
        let rte = run.rte(rti as usize);
        let subquery = rte.subquery.expect("setop leaf RTE has a subquery");

        let rel = crate::relnode::build_simple_rel(run, rti as u32, RTEKind::RTE_SUBQUERY)?;
        debug_assert!(run.root.plan_params.is_empty());

        let tuple_fraction = run.root.tuple_fraction;
        let sub_parse =
            mcx::leak_in(mcx::alloc_in(run.mcx, crate::subselect::query_cells_copy(run.mcx, subquery)?)?);
        run.push_root()?;
        crate::subquery::subquery_planner(run, sub_parse, false, tuple_fraction, parent_op)?;
        let child_tlist = run.processed_tlist();
        let idx = run.pop_root_to_rel_subroot();
        run.root.rel_mut(rel).subroot_idx = Some(idx);
        assert!(
            run.root.plan_params.is_empty(),
            "unexpected outer reference in set operation subquery"
        );

        let (tlist, trivial) = generate_setop_tlist(
            run,
            col_types,
            col_collations,
            rti,
            true,
            child_tlist,
            refnames_tlist,
        )?;
        let pt = create_pathtarget(run, &tlist)?;
        run.root.rel_mut(rel).pathtarget_id = Some(pt);
        Ok((rel, tlist, trivial))
    } else if let Some(op) = set_op.as_set_operation_stmt() {
        let (rel, mut tlist) = if op.op == SetOperation::SETOP_UNION {
            generate_union_paths(run, op, refnames_tlist)?
        } else {
            generate_nonunion_paths(run, op, refnames_tlist)?
        };
        let mut trivial = true;
        if !tlist_same_datatypes(&tlist, col_types) || !tlist_same_collations(&tlist, col_collations)
        {
            // Vars use varno 0 so setrefs can match them against the setop
            // subplan tlist (recurse_set_operations, prepunion.c).
            (tlist, trivial) = generate_setop_tlist(
                run,
                col_types,
                col_collations,
                0,
                false,
                &tlist,
                refnames_tlist,
            )?;
            let target_id = create_pathtarget(run, &tlist)?;
            let paths =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).pathlist);
            for (i, &subpath) in paths.iter().enumerate() {
                let path =
                    crate::pathnode::apply_projection_to_path(run, rel, subpath, target_id)?;
                if path != subpath {
                    run.root.rel_mut(rel).pathlist[i] = path;
                }
            }
            // prepunion.c:326-338: partial paths get an unconditional
            // ProjectionPath (never in-place — multiple refs possible).
            let partials =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).partial_pathlist);
            for (i, &subpath) in partials.iter().enumerate() {
                debug_assert!(run.root.path(subpath).base().param_info.is_none());
                let safe = ::clauses::is_parallel_safe_exprs(run, target_id)?;
                let pn =
                    crate::pathnode::create_projection_path(run, rel, subpath, target_id, safe);
                let pid = run.root.alloc_path(pn);
                run.root.rel_mut(rel).partial_pathlist[i] = pid;
            }
        }
        postprocess_setop_rel(run, rel)?;
        Ok((rel, tlist, trivial))
    } else {
        panic!("recurse_set_operations (prepunion.c): unrecognized node {:?}", set_op.node_tag());
    }
}

// Metadata copied out of the subroot arena so the outer path can be costed
// and its pathkeys converted without holding a cross-root borrow.
struct ChildCandidate<'mcx> {
    pid: PathId,
    info: SubqueryScanInfo,
    pathkey_descs: PgVec<'mcx, crate::pathkeys::SubPathKeyDesc<'mcx>>,
    sub_tlist: PgVec<'mcx, crate::pathkeys::SubTle<'mcx>>,
}

fn build_setop_child_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    trivial_tlist: bool,
    child_tlist: &NodeList<'mcx>,
    interesting_pathkeys: &[PathKey],
    want_num_groups: bool,
) -> PgResult<Option<f64>> {
    debug_assert!(run.root.rel(rel).rtekind == RTEKind::RTE_SUBQUERY as u32);
    let want_sorted = !interesting_pathkeys.is_empty();

    if want_sorted {
        crate::equivclass::add_setop_child_rel_equivalences(
            run,
            rel,
            child_tlist,
            interesting_pathkeys,
        );
    }

    crate::costsize::set_subquery_size_estimates(run, rel)?;

    let idx = run.root.rel(rel).subroot_idx.expect("subquery rel has a subroot");
    run.swap_with_rel_subroot(idx);
    let mut candidates: PgVec<'mcx, ChildCandidate> = PgVec::new_in(run.mcx);
    let mut partial_candidate: Option<ChildCandidate> = None;
    // Swap back before propagating errors (num_groups block pattern below).
    let subroot_result = (|| -> PgResult<bool> {
        let final_rel = crate::planmain::fetch_final_rel(run);
        let consider_parallel = run.root.rel(final_rel).consider_parallel;
        debug_assert!(
            consider_parallel || run.root.rel(final_rel).partial_pathlist.is_empty()
        );
        // prepunion.c: only the cheapest partial subpath is worth a partial
        // SubqueryScan.
        if let Some(&psp) = run.root.rel(final_rel).partial_pathlist.first() {
            partial_candidate = Some(ChildCandidate {
                pid: psp,
                info: child_info(run, psp),
                pathkey_descs: crate::pathkeys::extract_subquery_pathkey_descs(run, psp),
                sub_tlist: crate::pathkeys::extract_subquery_tlist(run, psp),
            });
        }
        let cheapest = run
            .root
            .rel(final_rel)
            .cheapest_total_path
            .expect("subquery final rel has a cheapest path");
        let setop_pathkeys =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.setop_pathkeys);
        let limittuples = run.root.limit_tuples;
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(final_rel).pathlist);
        for &subpath in paths.iter() {
            if subpath == cheapest {
                candidates.push(ChildCandidate {
                    pid: subpath,
                    info: child_info(run, subpath),
                    pathkey_descs: crate::pathkeys::extract_subquery_pathkey_descs(run, subpath),
                    sub_tlist: crate::pathkeys::extract_subquery_tlist(run, subpath),
                });
            }
            if !want_sorted {
                continue;
            }
            let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
                &setop_pathkeys,
                &run.root.path(subpath).base().pathkeys,
            );
            let mut sp = subpath;
            if !is_sorted {
                if sp != cheapest
                    && (presorted_keys == 0 || !crate::gucs::enable_incremental_sort())
                {
                    continue;
                }
                let keys = crate::relnode::pgvec_clone_shallow(run.mcx, &setop_pathkeys);
                if presorted_keys == 0 || !crate::gucs::enable_incremental_sort() {
                    sp = crate::pathnode::create_sort_path(run, final_rel, sp, keys, limittuples);
                } else {
                    sp = crate::pathnode::create_incremental_sort_path(
                        run,
                        final_rel,
                        sp,
                        keys,
                        presorted_keys,
                        limittuples,
                    )?;
                }
            }
            if sp != cheapest {
                candidates.push(ChildCandidate {
                    pid: sp,
                    info: child_info(run, sp),
                    pathkey_descs: crate::pathkeys::extract_subquery_pathkey_descs(run, sp),
                    sub_tlist: crate::pathkeys::extract_subquery_tlist(run, sp),
                });
            }
        }
        Ok(consider_parallel)
    })();
    run.swap_with_rel_subroot(idx);
    let consider_parallel = subroot_result?;

    run.root.rel_mut(rel).consider_parallel = consider_parallel;
    for c in candidates.iter() {
        let pathkeys = crate::pathkeys::convert_subquery_pathkeys(
            run,
            rel,
            &c.pathkey_descs,
            &c.sub_tlist,
        )?;
        let id = crate::pathnode::create_subqueryscan_path(
            run,
            rel,
            c.pid,
            trivial_tlist,
            pathkeys,
            &crate::relnode::RELIDS_UNSET,
            &c.info,
        )?;
        add_path(run, rel, id);
    }
    // prepunion.c: partial SubqueryScan over the child's cheapest partial path.
    if consider_parallel
        && types_pathnodes::relids::relids_is_empty(&run.root.rel(rel).lateral_relids)
    {
        if let Some(c) = partial_candidate {
            let id = crate::pathnode::create_subqueryscan_path(
                run,
                rel,
                c.pid,
                trivial_tlist,
                PgVec::new_in(run.mcx),
                &crate::relnode::RELIDS_UNSET,
                &c.info,
            )?;
            crate::pathnode::add_partial_path(run, rel, id);
        }
    }

    postprocess_setop_rel(run, rel)?;

    if !want_num_groups {
        return Ok(None);
    }
    let input_rows = {
        let cheapest = run.root.rel(rel).cheapest_total_path.expect("set_cheapest ran");
        run.root.path(cheapest).base().rows
    };
    run.swap_with_rel_subroot(idx);
    // C: the child subroot's parent_root chain stays live here; selfuncs
    // climbs it for the child's uplevel CTE references.
    run.swapped_parent_subroot = Some(idx);
    let result = (|| -> PgResult<f64> {
        let sub_parse = run.parse();
        if !sub_parse.groupClause.is_nil()
            || !sub_parse.groupingSets.is_nil()
            || !sub_parse.distinctClause.is_nil()
            || run.root.hasHavingQual
            || sub_parse.hasAggs
        {
            return Ok(input_rows);
        }
        let mut exprs: PgVec<'mcx, (NodeId, Node<'mcx>)> = PgVec::new_in(run.mcx);
        for tle_node in &sub_parse.targetList {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            if tle.resjunk {
                continue;
            }
            let id = run.intern_expr(tle.expr);
            exprs.push((id, tle.expr));
        }
        crate::selfuncs::estimate_num_groups(run, &exprs, input_rows)
    })();
    run.swapped_parent_subroot = None;
    run.swap_with_rel_subroot(idx);
    Ok(Some(result?))
}

pub(crate) fn child_info(run: &PlannerRun<'_>, pid: PathId) -> SubqueryScanInfo {
    let p = run.root.path(pid).base();
    SubqueryScanInfo {
        rows: p.rows,
        disabled_nodes: p.disabled_nodes,
        startup_cost: p.startup_cost,
        total_cost: p.total_cost,
        parallel_safe: p.parallel_safe,
        parallel_workers: p.parallel_workers,
    }
}

fn generate_union_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    op: &'mcx SetOperationStmt<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<(RelId, NodeList<'mcx>)> {
    let mcx = run.mcx;
    let rellist = plan_union_children(run, op, refnames_tlist)?;

    let input_tlists: PgVec<'mcx, &NodeList<'mcx>> = {
        let mut v = PgVec::new_in(mcx);
        for (_, tl, _) in rellist.iter() {
            v.push(tl);
        }
        v
    };
    let tlist = generate_append_tlist(run, &op.colTypes, &op.colCollations, &input_tlists, refnames_tlist)?;

    let mut group_list = NodeList::nil();
    let mut try_sorted = false;
    let mut union_pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    if !op.all {
        group_list = generate_setop_grouplist(run, op, &tlist)?;
        if grouping_is_sortable_nodes(&op.groupClauses) {
            try_sorted = true;
            union_pathkeys =
                crate::pathkeys::make_pathkeys_for_sortclauses(run, &group_list, &tlist)?;
            run.root.query_pathkeys =
                crate::relnode::pgvec_clone_shallow(mcx, &union_pathkeys);
        }
    }

    for &(rel, ref child_tlist, trivial) in rellist.iter() {
        if run.root.rel(rel).rtekind == RTEKind::RTE_SUBQUERY as u32 {
            build_setop_child_paths(run, rel, trivial, child_tlist, &union_pathkeys, false)?;
        }
    }

    let mut cheapest_pathlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut ordered_pathlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut partial_pathlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut partial_paths_valid = true;
    let mut consider_parallel = true;
    let mut relids: Relids<'mcx> = crate::relnode::relids_empty();
    for &(rel, _, _) in rellist.iter() {
        cheapest_pathlist
            .push(run.root.rel(rel).cheapest_total_path.expect("union child has cheapest path"));
        if try_sorted {
            let paths =
                crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel).pathlist);
            match crate::pathkeys::get_cheapest_path_for_pathkeys(
                run,
                &paths,
                &union_pathkeys,
                &crate::relnode::RELIDS_UNSET,
                crate::pathnode::CostSelector::Total,
                false,
            ) {
                Some(ordered_path) => ordered_pathlist.push(ordered_path),
                // Type coercion in the union tlist can defeat child sorting.
                None => try_sorted = false,
            }
        }
        if consider_parallel {
            if !run.root.rel(rel).consider_parallel {
                consider_parallel = false;
                partial_paths_valid = false;
            } else if run.root.rel(rel).partial_pathlist.is_empty() {
                partial_paths_valid = false;
            } else {
                partial_pathlist.push(run.root.rel(rel).partial_pathlist[0]);
            }
        }
        relids = crate::relnode::relids_union(mcx, &relids, &run.root.rel(rel).relids);
    }

    let result_rel =
        crate::relnode::fetch_upper_rel_with_relids(&mut run.root, UPPERREL_SETOP, relids);
    let target_id = create_pathtarget(run, &tlist)?;
    {
        let r = run.root.rel_mut(result_rel);
        r.pathtarget_id = Some(target_id);
        r.consider_parallel = consider_parallel;
    }
    run.root.rel_mut(result_rel).consider_startup = run.root.tuple_fraction > 0.0;

    let apath = crate::pathnode::create_append_path(
        run,
        result_rel,
        cheapest_pathlist,
        PgVec::new_in(mcx),
        PgVec::new_in(mcx),
        &crate::relnode::RELIDS_UNSET,
        0,
        false,
        -1.0,
    )?;
    let apath_rows = run.root.path(apath).base().rows;
    run.root.rel_mut(result_rel).rows = apath_rows;

    // prepunion.c: the same append from the children's cheapest partial
    // paths, under a Gather.
    let gpath = if partial_paths_valid && !partial_pathlist.is_empty() {
        let mut parallel_workers = 0;
        for &sp in partial_pathlist.iter() {
            parallel_workers = parallel_workers.max(run.root.path(sp).base().parallel_workers);
        }
        debug_assert!(parallel_workers > 0);
        if crate::gucs::enable_parallel_append() {
            parallel_workers =
                parallel_workers.max((partial_pathlist.len() as u32).ilog2() as i32 + 1);
            parallel_workers =
                parallel_workers.min(crate::gucs::max_parallel_workers_per_gather());
        }
        let papath = crate::pathnode::create_append_path(
            run,
            result_rel,
            PgVec::new_in(mcx),
            partial_pathlist,
            PgVec::new_in(mcx),
            &crate::relnode::RELIDS_UNSET,
            parallel_workers,
            crate::gucs::enable_parallel_append(),
            -1.0,
        )?;
        Some(crate::pathnode::create_gather_path(run, result_rel, papath, Some(target_id), None))
    } else {
        None
    };

    if !op.all {
        let d_num_groups = apath_rows;
        let can_sort = grouping_is_sortable_nodes(&group_list);
        let can_hash = grouping_is_hashable_nodes(&group_list);

        if can_hash {
            for src in [Some(apath), gpath].into_iter().flatten() {
                let hash_target = create_pathtarget(run, &tlist)?;
                let group_ids = intern_clause_list(run, &group_list);
                let path = crate::pathnode::create_agg_path(
                    run,
                    result_rel,
                    src,
                    hash_target,
                    AGG_HASHED,
                    AGGSPLIT_SIMPLE,
                    group_ids,
                    PgVec::new_in(mcx),
                    &types_pathnodes::AggClauseCosts::default(),
                    d_num_groups,
                )?;
                add_path(run, result_rel, path);
            }
        }

        if can_sort {
            for src in [Some(apath), gpath].into_iter().flatten() {
                let mut path = src;
                // C sorts the Gather source unconditionally; the Append
                // source only under a nonempty groupList.
                if !group_list.is_nil() || path != apath {
                    let keys =
                        crate::pathkeys::make_pathkeys_for_sortclauses(run, &group_list, &tlist)?;
                    path = crate::pathnode::create_sort_path(run, result_rel, path, keys, -1.0);
                }
                let numkeys = run.root.path(path).base().pathkeys.len() as i32;
                let unique = crate::pathnode::create_upper_unique_path(
                    run,
                    result_rel,
                    path,
                    numkeys,
                    d_num_groups,
                );
                add_path(run, result_rel, unique);
            }
        }

        if try_sorted && !group_list.is_nil() {
            let path = crate::pathnode::create_merge_append_path(
                run,
                result_rel,
                ordered_pathlist,
                crate::relnode::pgvec_clone_shallow(mcx, &union_pathkeys),
            )?;
            let unique = crate::pathnode::create_upper_unique_path(
                run,
                result_rel,
                path,
                tlist.len() as i32,
                d_num_groups,
            );
            add_path(run, result_rel, unique);
        }
    } else {
        add_path(run, result_rel, apath);
        if let Some(g) = gpath {
            add_path(run, result_rel, g);
        }
    }

    Ok((result_rel, tlist))
}

// generate_recursion_path (prepunion.c).
fn generate_recursion_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    op: &'mcx SetOperationStmt<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<(RelId, NodeList<'mcx>)> {
    let mcx = run.mcx;
    assert!(op.op == SetOperation::SETOP_UNION, "only UNION queries can be recursive");
    debug_assert!(run.root.wt_param_id >= 0);

    let (lrel, lpath_tlist, ltrivial) = recurse_set_operations(
        run,
        op.larg.expect("setop larg"),
        None,
        &op.colTypes,
        &op.colCollations,
        refnames_tlist,
    )?;
    if run.root.rel(lrel).rtekind == RTEKind::RTE_SUBQUERY as u32 {
        build_setop_child_paths(run, lrel, ltrivial, &lpath_tlist, &[], false)?;
    }
    let lpath = run.root.rel(lrel).cheapest_total_path.expect("non-recursive term has a path");
    // The recursive term's worktable scans read this (set_worktable_pathlist).
    run.root.non_recursive_path = Some(lpath);
    let (rrel, rpath_tlist, rtrivial) = recurse_set_operations(
        run,
        op.rarg.expect("setop rarg"),
        None,
        &op.colTypes,
        &op.colCollations,
        refnames_tlist,
    )?;
    if run.root.rel(rrel).rtekind == RTEKind::RTE_SUBQUERY as u32 {
        build_setop_child_paths(run, rrel, rtrivial, &rpath_tlist, &[], false)?;
    }
    let rpath = run.root.rel(rrel).cheapest_total_path.expect("recursive term has a path");
    run.root.non_recursive_path = None;

    let input_tlists: PgVec<'mcx, &NodeList<'mcx>> = {
        let mut v = PgVec::new_in(mcx);
        v.push(&lpath_tlist);
        v.push(&rpath_tlist);
        v
    };
    let tlist =
        generate_append_tlist(run, &op.colTypes, &op.colCollations, &input_tlists, refnames_tlist)?;

    let relids = crate::relnode::relids_union(
        mcx,
        &run.root.rel(lrel).relids,
        &run.root.rel(rrel).relids,
    );
    let result_rel =
        crate::relnode::fetch_upper_rel_with_relids(&mut run.root, UPPERREL_SETOP, relids);
    let target_id = create_pathtarget(run, &tlist)?;
    run.root.rel_mut(result_rel).pathtarget_id = Some(target_id);

    let (group_ids, d_num_groups) = if op.all {
        (PgVec::new_in(mcx), 0.0)
    } else {
        let group_list = generate_setop_grouplist(run, op, &tlist)?;
        if !grouping_is_hashable_nodes(&group_list) {
            return Err(Box::new(
                types_error::PgError::error("could not implement recursive UNION")
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail("All column datatypes must be hashable."),
            ));
        }
        let (lrows, rrows) =
            (run.root.path(lpath).base().rows, run.root.path(rpath).base().rows);
        (intern_clause_list(run, &group_list), lrows + rrows * 10.0)
    };

    let wt_param = run.root.wt_param_id;
    let path = crate::pathnode::create_recursiveunion_path(
        run,
        result_rel,
        lpath,
        rpath,
        target_id,
        group_ids,
        wt_param,
        d_num_groups,
    );
    add_path(run, result_rel, path);
    postprocess_setop_rel(run, result_rel)?;
    Ok((result_rel, tlist))
}

fn generate_nonunion_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    op: &'mcx SetOperationStmt<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<(RelId, NodeList<'mcx>)> {
    let mcx = run.mcx;
    let save_fraction = run.root.tuple_fraction;
    run.root.tuple_fraction = 0.0;

    let (mut lrel, mut lpath_tlist, ltrivial) = recurse_set_operations(
        run,
        op.larg.expect("setop larg"),
        Some(op),
        &op.colTypes,
        &op.colCollations,
        refnames_tlist,
    )?;
    let (mut rrel, mut rpath_tlist, rtrivial) = recurse_set_operations(
        run,
        op.rarg.expect("setop rarg"),
        Some(op),
        &op.colTypes,
        &op.colCollations,
        refnames_tlist,
    )?;

    let (tlist, result_trivial) = generate_setop_tlist(
        run,
        &op.colTypes,
        &op.colCollations,
        0,
        false,
        &lpath_tlist,
        refnames_tlist,
    )?;
    debug_assert!(result_trivial);

    let group_list = generate_setop_grouplist(run, op, &tlist)?;
    let can_sort = grouping_is_sortable_nodes(&group_list);
    let can_hash = grouping_is_hashable_nodes(&group_list);
    if !can_sort && !can_hash {
        return Err(crate::grouping::could_not_implement(
            if op.op == SetOperation::SETOP_INTERSECT { "INTERSECT" } else { "EXCEPT" },
        ));
    }

    let mut nonunion_pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    if can_sort {
        nonunion_pathkeys =
            crate::pathkeys::make_pathkeys_for_sortclauses(run, &group_list, &tlist)?;
        run.root.query_pathkeys =
            crate::relnode::pgvec_clone_shallow(mcx, &nonunion_pathkeys);
    }

    let mut d_left_groups =
        match run.root.rel(lrel).rtekind == RTEKind::RTE_SUBQUERY as u32 {
            true => build_setop_child_paths(run, lrel, ltrivial, &lpath_tlist, &nonunion_pathkeys, true)?
                .expect("num groups requested"),
            false => run.root.rel(lrel).rows,
        };
    let mut d_right_groups =
        match run.root.rel(rrel).rtekind == RTEKind::RTE_SUBQUERY as u32 {
            true => build_setop_child_paths(run, rrel, rtrivial, &rpath_tlist, &nonunion_pathkeys, true)?
                .expect("num groups requested"),
            false => run.root.rel(rrel).rows,
        };

    run.root.tuple_fraction = save_fraction;

    if op.op != SetOperation::SETOP_EXCEPT && d_left_groups > d_right_groups {
        core::mem::swap(&mut lrel, &mut rrel);
        core::mem::swap(&mut lpath_tlist, &mut rpath_tlist);
        core::mem::swap(&mut d_left_groups, &mut d_right_groups);
    }

    let lpath = run.root.rel(lrel).cheapest_total_path.expect("left child has cheapest path");
    let rpath = run.root.rel(rrel).cheapest_total_path.expect("right child has cheapest path");

    let relids = crate::relnode::relids_union(
        mcx,
        &run.root.rel(lrel).relids,
        &run.root.rel(rrel).relids,
    );
    let result_rel =
        crate::relnode::fetch_upper_rel_with_relids(&mut run.root, UPPERREL_SETOP, relids);
    let target_id = create_pathtarget(run, &tlist)?;
    run.root.rel_mut(result_rel).pathtarget_id = Some(target_id);

    let (lrows, rrows) =
        (run.root.path(lpath).base().rows, run.root.path(rpath).base().rows);
    let d_num_groups = d_left_groups;
    let d_num_output_rows = if op.op == SetOperation::SETOP_EXCEPT {
        if op.all { lrows } else { d_num_groups }
    } else if op.all {
        lrows.min(rrows)
    } else {
        d_num_groups
    };
    run.root.rel_mut(result_rel).rows = d_num_output_rows;

    let cmd = match (op.op, op.all) {
        (SetOperation::SETOP_INTERSECT, false) => SETOPCMD_INTERSECT,
        (SetOperation::SETOP_INTERSECT, true) => SETOPCMD_INTERSECT_ALL,
        (SetOperation::SETOP_EXCEPT, false) => SETOPCMD_EXCEPT,
        (SetOperation::SETOP_EXCEPT, true) => SETOPCMD_EXCEPT_ALL,
        (other, _) => panic!("unrecognized set op: {other:?}"),
    };

    if can_hash {
        let group_ids = intern_clause_list(run, &group_list);
        let path = crate::pathnode::create_setop_path(
            run,
            result_rel,
            lpath,
            rpath,
            cmd,
            SETOP_HASHED,
            group_ids,
            d_num_groups,
            d_num_output_rows,
        );
        add_path(run, result_rel, path);
    }

    if can_sort {
        let slpath = sorted_nonunion_input(
            run,
            lrel,
            lpath,
            &group_list,
            &lpath_tlist,
            &nonunion_pathkeys,
        )?;
        let srpath = sorted_nonunion_input(
            run,
            rrel,
            rpath,
            &group_list,
            &rpath_tlist,
            &nonunion_pathkeys,
        )?;
        let group_ids = intern_clause_list(run, &group_list);
        let path = crate::pathnode::create_setop_path(
            run,
            result_rel,
            slpath,
            srpath,
            cmd,
            SETOP_SORTED,
            group_ids,
            d_num_groups,
            d_num_output_rows,
        );
        add_path(run, result_rel, path);
    }

    Ok((result_rel, tlist))
}

fn sorted_nonunion_input<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    cheapest: PathId,
    group_list: &NodeList<'mcx>,
    input_tlist: &NodeList<'mcx>,
    nonunion_pathkeys: &[PathKey],
) -> PgResult<PathId> {
    let pathkeys = crate::pathkeys::make_pathkeys_for_sortclauses(run, group_list, input_tlist)?;
    if crate::pathkeys::pathkeys_contained_in(
        &pathkeys,
        &run.root.path(cheapest).base().pathkeys,
    ) {
        return Ok(cheapest);
    }
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).pathlist);
    if let Some(p) = crate::pathkeys::get_cheapest_path_for_pathkeys(
        run,
        &paths,
        nonunion_pathkeys,
        &crate::relnode::RELIDS_UNSET,
        crate::pathnode::CostSelector::Total,
        false,
    ) {
        return Ok(p);
    }
    let parent = run.root.path(cheapest).base().parent;
    Ok(crate::pathnode::create_sort_path(run, parent, cheapest, pathkeys, -1.0))
}

type UnionChild<'mcx> = (RelId, NodeList<'mcx>, bool);

fn plan_union_children<'mcx>(
    run: &mut PlannerRun<'mcx>,
    top_union: &'mcx SetOperationStmt<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, UnionChild<'mcx>>> {
    let mcx = run.mcx;
    let mut pending: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    pending.push(top_union.larg.expect("setop larg"));
    pending.push(top_union.rarg.expect("setop rarg"));
    let mut result: PgVec<'mcx, UnionChild<'mcx>> = PgVec::new_in(mcx);

    while !pending.is_empty() {
        let set_op = pending.remove(0);
        if let Some(op) = set_op.as_set_operation_stmt() {
            if op.op == top_union.op
                && (op.all == top_union.all || op.all)
                && oid_list_equal(&op.colTypes, &top_union.colTypes)
                && oid_list_equal(&op.colCollations, &top_union.colCollations)
            {
                pending.insert(0, op.rarg.expect("setop rarg"));
                pending.insert(0, op.larg.expect("setop larg"));
                continue;
            }
        }
        let parent = if top_union.all { None } else { Some(top_union) };
        result.push(recurse_set_operations(
            run,
            set_op,
            parent,
            &top_union.colTypes,
            &top_union.colCollations,
            refnames_tlist,
        )?);
    }
    Ok(result)
}

fn postprocess_setop_rel(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    set_cheapest(run, rel)
}

#[allow(clippy::too_many_arguments)]
fn generate_setop_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    col_types: &OidList<'mcx>,
    col_collations: &OidList<'mcx>,
    varno: i32,
    hack_constants: bool,
    input_tlist: &NodeList<'mcx>,
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<(NodeList<'mcx>, bool)> {
    let mcx = run.mcx;
    let mut tlist = NodeList::nil();
    let mut trivial = true;
    let pstate = parser_small1::parse_node::make_parsestate(mcx, None);

    debug_assert_eq!(col_types.len(), col_collations.len());
    let mut resno: i16 = 1;
    let mut it = input_tlist.iter();
    let mut rt = refnames_tlist.iter();
    for (i, col_type) in col_types.iter().enumerate() {
        let col_coll = col_collations.nth(i);
        let input_tle = it.next().expect("input tlist matches colTypes").as_target_entry().expect("tlist cell");
        let ref_tle = rt.next().expect("refnames tlist matches colTypes").as_target_entry().expect("tlist cell");
        debug_assert!(input_tle.resno == resno && ref_tle.resno == resno);
        debug_assert!(!input_tle.resjunk && !ref_tle.resjunk);

        let (in_type, in_typmod) = crate::costsize::expr_type_typmod(input_tle.expr);
        let mut expr = if hack_constants && input_tle.expr.node_tag() == NodeTag::T_Const {
            input_tle.expr
        } else {
            Node::mk_var(
                mcx,
                varno,
                input_tle.resno,
                in_type,
                in_typmod,
                crate::pathkeys::expr_collation(input_tle.expr),
                0,
            )?
        };

        let (expr_type, _) = crate::costsize::expr_type_typmod(expr);
        if expr_type != col_type {
            expr = coerce::coerce_to_common_type(
                mcx,
                &pstate,
                expr,
                expr_type,
                -1,
                col_type,
                "UNION/INTERSECT/EXCEPT",
            )?;
            trivial = false;
        }
        if crate::pathkeys::expr_collation(expr) != col_coll {
            let (ety, etypmod) = crate::costsize::expr_type_typmod(expr);
            expr = nodes_core::node_funcs::apply_relabel_type(
                mcx,
                expr,
                ety,
                etypmod,
                col_coll,
                types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            trivial = false;
        }

        let tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr,
                resno,
                resname: ref_tle.resname,
                // Setop-tree convention: every output column's sortgroupref
                // equals its resno.
                ressortgroupref: resno as u32,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )?;
        tlist.lappend(mcx, tle)?;
        resno += 1;
    }
    debug_assert!(it.next().is_none());
    Ok((tlist, trivial))
}

fn generate_append_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    col_types: &OidList<'mcx>,
    col_collations: &OidList<'mcx>,
    input_tlists: &[&NodeList<'mcx>],
    refnames_tlist: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let ncols = col_types.len();
    let mut col_typmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    col_typmods.extend(core::iter::repeat(-1).take(ncols));

    for (tlist_no, subtlist) in input_tlists.iter().enumerate() {
        for (colindex, subtle_node) in subtlist.iter().enumerate() {
            let subtle = subtle_node.as_target_entry().expect("tlist cell");
            debug_assert!(!subtle.resjunk);
            let (ty, typmod) = crate::costsize::expr_type_typmod(subtle.expr);
            if ty == col_types.nth(colindex) {
                if tlist_no == 0 {
                    col_typmods[colindex] = typmod;
                } else if typmod != col_typmods[colindex] {
                    col_typmods[colindex] = -1;
                }
            } else {
                col_typmods[colindex] = -1;
            }
        }
    }

    let mut tlist = NodeList::nil();
    let mut rt = refnames_tlist.iter();
    for i in 0..ncols {
        let resno = (i + 1) as i16;
        let ref_tle = rt.next().expect("refnames tlist matches colTypes").as_target_entry().expect("tlist cell");
        debug_assert!(ref_tle.resno == resno && !ref_tle.resjunk);
        let expr = Node::mk_var(
            mcx,
            0,
            resno,
            col_types.nth(i),
            col_typmods[i],
            col_collations.nth(i),
            0,
        )?;
        let tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr,
                resno,
                resname: ref_tle.resname,
                ressortgroupref: resno as u32,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )?;
        tlist.lappend(mcx, tle)?;
    }
    let _ = run;
    Ok(tlist)
}

fn generate_setop_grouplist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    op: &SetOperationStmt<'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut grouplist = NodeList::nil();
    let mut gc = op.groupClauses.iter();
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        debug_assert!(!tle.resjunk);
        debug_assert_eq!(tle.ressortgroupref, tle.resno as u32);
        let sgc = gc
            .next()
            .expect("groupClauses matches targetlist")
            .as_sort_group_clause()
            .expect("groupClauses cell");
        debug_assert_eq!(sgc.tleSortGroupRef, 0);
        grouplist.lappend(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: tle.ressortgroupref,
                    eqop: sgc.eqop,
                    sortop: sgc.sortop,
                    reverse_sort: sgc.reverse_sort,
                    nulls_first: sgc.nulls_first,
                    hashable: sgc.hashable,
                },
            )?,
        )?;
    }
    debug_assert!(gc.next().is_none());
    Ok(grouplist)
}

// generate_setop_child_grouplist (prepunion.c): NIL when a child column type
// diverges from the setop output type.
pub(crate) fn generate_setop_child_grouplist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    op: &SetOperationStmt<'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, NodeId>> {
    let mut clauses: PgVec<'mcx, NodeId> = PgVec::new_in(run.mcx);
    let mut gc = op.groupClauses.iter();
    let mut ct = 0usize;
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            continue;
        }
        let sgc_node = gc.next().expect("groupClauses matches non-junk targetlist");
        let sgc = sgc_node.as_sort_group_clause().expect("groupClauses cell");
        let coltype = op.colTypes.nth(ct);
        ct += 1;
        let (tle_type, _) = crate::costsize::expr_type_typmod(tle.expr);
        if coltype != tle_type {
            return Ok(PgVec::new_in(run.mcx));
        }
        let sortgroupref = assign_sort_group_ref(tle_node, targetlist);
        let clause = Node::mk(
            run.mcx,
            SortGroupClause {
                tleSortGroupRef: sortgroupref,
                eqop: sgc.eqop,
                sortop: sgc.sortop,
                reverse_sort: sgc.reverse_sort,
                nulls_first: sgc.nulls_first,
                hashable: sgc.hashable,
            },
        )?;
        clauses.push(run.intern_expr(clause));
    }
    debug_assert!(gc.next().is_none());
    Ok(clauses)
}

// assignSortGroupRef (parse_clause.c).
pub(crate) fn assign_sort_group_ref<'mcx>(tle_node: Node<'mcx>, tlist: &NodeList<'mcx>) -> u32 {
    let tle = tle_node.as_target_entry().expect("tlist cell");
    if tle.ressortgroupref != 0 {
        return tle.ressortgroupref;
    }
    let max_ref = tlist
        .iter()
        .map(|n| n.as_target_entry().expect("tlist cell").ressortgroupref)
        .max()
        .unwrap_or(0);
    // SAFETY: C assigns the ref in place on the shared processed_tlist entry;
    // no borrow of this TargetEntry is read across the write.
    unsafe {
        tle_node.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| {
            t.ressortgroupref = max_ref + 1
        })
    }
    .expect("tlist cell");
    max_ref + 1
}

fn intern_clause_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
) -> PgVec<'mcx, NodeId> {
    let mut out = PgVec::new_in(run.mcx);
    for n in list {
        out.push(run.intern_expr(n));
    }
    out
}

fn grouping_is_sortable_nodes(clauses: &NodeList<'_>) -> bool {
    clauses
        .iter()
        .all(|n| n.as_sort_group_clause().expect("group clause cell").sortop != 0)
}

fn grouping_is_hashable_nodes(clauses: &NodeList<'_>) -> bool {
    clauses
        .iter()
        .all(|n| n.as_sort_group_clause().expect("group clause cell").hashable)
}

fn oid_list_equal(a: &OidList<'_>, b: &OidList<'_>) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

fn tlist_same_datatypes(tlist: &NodeList<'_>, col_types: &OidList<'_>) -> bool {
    let mut ct = col_types.iter();
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            return false;
        }
        let Some(t) = ct.next() else { return false };
        if crate::costsize::expr_type_typmod(tle.expr).0 != t {
            return false;
        }
    }
    ct.next().is_none()
}

fn tlist_same_collations(tlist: &NodeList<'_>, col_collations: &OidList<'_>) -> bool {
    let mut cc = col_collations.iter();
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            return false;
        }
        let Some(c) = cc.next() else { return false };
        if crate::pathkeys::expr_collation(tle.expr) != c {
            return false;
        }
    }
    cc.next().is_none()
}
