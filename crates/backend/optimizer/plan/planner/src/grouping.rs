use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_pathnodes::{
    RelId, UPPERREL_DISTINCT, UPPERREL_FINAL, UPPERREL_GROUP_AGG, UPPERREL_ORDERED,
    UPPERREL_PARTIAL_DISTINCT, UPPERREL_WINDOW,
};

use crate::pathnode::{add_existing_path, create_pathtarget, create_projection_path};
use crate::planmain::{fetch_final_rel, query_planner};
use crate::prep::preprocess_targetlist;
use crate::run::PlannerRun;
pub(crate) use types_pathnodes::run::sortgrouplist_exprs;
use crate::{is_parallel_safe_exprs, is_parallel_safe_opt};

pub fn grouping_planner<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tuple_fraction: f64,
    setops: Option<&'mcx types_nodes::parsenodes::SetOperationStmt<'mcx>>,
) -> PgResult<()> {
    let parse = run.parse();
    let mut tuple_fraction = tuple_fraction;
    let mut offset_est: i64 = 0;
    let mut count_est: i64 = 0;
    let mut limit_tuples = -1.0f64;
    if parse.limitCount.is_some() || parse.limitOffset.is_some() {
        tuple_fraction = preprocess_limit(run, tuple_fraction, &mut offset_est, &mut count_est)?;
        if count_est > 0 && offset_est >= 0 {
            limit_tuples = count_est as f64 + offset_est as f64;
        }
    }
    run.root.tuple_fraction = tuple_fraction;

    if parse.setOperations.is_some() {
        let current_rel = crate::prepunion::plan_set_operations(run)?;
        assert_eq!(parse.commandType, CmdType::CMD_SELECT);
        let fixed = postprocess_setop_tlist(run, run.processed_tlist(), &parse.targetList)?;
        run.processed_tlist = Some(fixed);
        let cheapest = run
            .root
            .rel(current_rel)
            .cheapest_total_path
            .expect("setop rel has a cheapest path");
        let final_target = run
            .root
            .path(cheapest)
            .base()
            .pathtarget_id
            .expect("setop path has a pathtarget");
        let final_target_parallel_safe = is_parallel_safe_exprs(run, final_target)?;
        debug_assert!(!parse.hasTargetSRFs);
        debug_assert!(parse.rowMarks.is_nil() && parse.distinctClause.is_nil());
        run.root.sort_pathkeys = crate::pathkeys::make_pathkeys_for_sortclauses(
            run,
            &parse.sortClause,
            run.processed_tlist(),
        )?;
        // The setop result tlist couldn't contain any SRFs (C planner.c:1500).
        return grouping_planner_tail(
            run,
            current_rel,
            final_target,
            final_target_parallel_safe,
            &mcx::PgVec::new_in(run.mcx),
            &mcx::PgVec::new_in(run.mcx),
            false,
            limit_tuples,
            offset_est,
            count_est,
        );
    }
    // A recursive query always has setOperations.
    debug_assert!(!run.root.hasRecursion);
    if !parse.groupingSets.is_nil() {
        run.gset_data = Some(crate::groupingsets::preprocess_grouping_sets(run)?);
    } else {
        run.gset_data = None;
        if !parse.groupClause.is_nil() {
            run.root.processed_groupClause = preprocess_groupclause(run, None)?;
        }
    }

    preprocess_targetlist(run)?;

    if parse.hasAggs {
        crate::prepagg::preprocess_aggrefs(run, run.processed_tlist())?;
        if let Some(having) = parse.havingQual {
            crate::prepagg::preprocess_aggrefs_node(run, having)?;
        }
    }
    run.active_windows = mcx::PgVec::new_in(run.mcx);
    let mut wflists = None;
    if parse.hasWindowFuncs {
        let tlist_node = types_nodes::Node::mk_list(
            run.mcx,
            run.processed_tlist().clone_in(run.mcx)?,
        )?;
        let wfl = clauses::classify::find_window_functions(
            run.mcx,
            tlist_node,
            parse.windowClause.len() as u32,
        )?;
        if wfl.num_window_funcs > 0 {
            let mut wfl = wfl;
            crate::window::optimize_window_clauses(run, &mut wfl)?;
            let active = crate::window::select_active_windows(run, &wfl)?;
            crate::window::name_active_windows(run.mcx, &active)?;
            run.active_windows = active;
            wflists = Some(wfl);
        }
        // C clears parse->hasWindowFuncs when every WindowFunc const-folded
        // away; limit_tuples below reads the original flag (unreachable
        // difference on this lane: nothing folds a WindowFunc).
    }
    if parse.hasAggs {
        crate::planagg::preprocess_minmax_aggregates(run)?;
    }
    run.root.limit_tuples = if !parse.groupClause.is_nil()
        || !parse.groupingSets.is_nil()
        || !parse.distinctClause.is_nil()
        || parse.hasAggs
        || parse.hasWindowFuncs
        || parse.hasTargetSRFs
        || run.root.hasHavingQual
    {
        -1.0
    } else {
        limit_tuples
    };

    run.qp_setop = setops;
    let current_rel = query_planner(run, standard_qp_callback)?;

    let final_target = create_pathtarget(run, run.processed_tlist())?;
    let final_target_parallel_safe = is_parallel_safe_exprs(run, final_target)?;

    let mut have_postponed_srfs = false;
    let (sort_input_target, sort_input_target_parallel_safe) = if !parse.sortClause.is_nil() {
        let (t, postponed) = make_sort_input_target(run, final_target)?;
        have_postponed_srfs = postponed;
        let safe = if t == final_target {
            final_target_parallel_safe
        } else {
            is_parallel_safe_exprs(run, t)?
        };
        (t, safe)
    } else {
        (final_target, final_target_parallel_safe)
    };
    let (grouping_target, grouping_target_parallel_safe) =
        if !run.active_windows.is_empty() {
            let t = crate::window::make_window_input_target(run, final_target)?;
            (t, is_parallel_safe_exprs(run, t)?)
        } else {
            (sort_input_target, sort_input_target_parallel_safe)
        };
    let have_grouping = parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.groupingSets.is_nil()
        || run.root.hasHavingQual;
    let (scanjoin_target, scanjoin_target_parallel_safe) = if have_grouping {
        let t = make_group_input_target(run, final_target)?;
        (t, is_parallel_safe_exprs(run, t)?)
    } else {
        (grouping_target, grouping_target_parallel_safe)
    };

    // Split each level's target into SRF-computing and SRF-free versions;
    // each split treats the next-lower target's exprs as already computed.
    let (
        final_target,
        final_targets,
        final_targets_contain_srfs,
        sort_input_target,
        sort_input_targets,
        sort_input_targets_contain_srfs,
        grouping_target,
        grouping_targets,
        grouping_targets_contain_srfs,
        scanjoin_target,
        scanjoin_targets,
        scanjoin_targets_contain_srfs,
    ) = if parse.hasTargetSRFs {
        let (final_targets, final_contain) =
            crate::srf::split_pathtarget_at_srfs(run, final_target, Some(sort_input_target))?;
        debug_assert!(!final_contain[0]);
        let (sort_input_targets, sort_input_contain) =
            crate::srf::split_pathtarget_at_srfs(run, sort_input_target, Some(grouping_target))?;
        debug_assert!(!sort_input_contain[0]);
        let (grouping_targets, grouping_contain) = crate::srf::split_pathtarget_at_srfs_grouping(
            run,
            grouping_target,
            Some(scanjoin_target),
        )?;
        debug_assert!(!grouping_contain[0]);
        let (scanjoin_targets, scanjoin_contain) =
            crate::srf::split_pathtarget_at_srfs(run, scanjoin_target, None)?;
        debug_assert!(!scanjoin_contain[0]);
        (
            final_targets[0],
            final_targets,
            final_contain,
            sort_input_targets[0],
            sort_input_targets,
            sort_input_contain,
            grouping_targets[0],
            grouping_targets,
            grouping_contain,
            scanjoin_targets[0],
            scanjoin_targets,
            scanjoin_contain,
        )
    } else {
        let mut ts = mcx::PgVec::new_in(run.mcx);
        ts.push(scanjoin_target);
        let mut cs = mcx::PgVec::new_in(run.mcx);
        cs.push(false);
        (
            final_target,
            mcx::PgVec::new_in(run.mcx),
            mcx::PgVec::new_in(run.mcx),
            sort_input_target,
            mcx::PgVec::new_in(run.mcx),
            mcx::PgVec::new_in(run.mcx),
            grouping_target,
            mcx::PgVec::new_in(run.mcx),
            mcx::PgVec::new_in(run.mcx),
            scanjoin_target,
            ts,
            cs,
        )
    };
    let reltarget = run.rel_reltarget_id(current_rel);
    let same_exprs = scanjoin_targets.len() == 1
        && crate::pathnode::exprs_same(
            run,
            &run.root.pathtarget(scanjoin_target).exprs,
            &run.root.pathtarget(reltarget).exprs,
        );
    apply_scanjoin_target_to_paths(
        run,
        current_rel,
        &scanjoin_targets,
        &scanjoin_targets_contain_srfs,
        scanjoin_target_parallel_safe,
        same_exprs,
    )?;

    run.root.upper_targets[UPPERREL_FINAL as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_ORDERED as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_WINDOW as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_GROUP_AGG as usize] = Some(grouping_target);

    let mut current_rel = current_rel;
    if have_grouping {
        current_rel = create_grouping_paths(
            run,
            current_rel,
            grouping_target,
            grouping_target_parallel_safe,
        )?;
        if parse.hasTargetSRFs {
            crate::srf::adjust_paths_for_srfs(
                run,
                current_rel,
                &grouping_targets,
                &grouping_targets_contain_srfs,
            )?;
        }
    }

    if let Some(wfl) = &wflists {
        if !run.active_windows.is_empty() {
            current_rel = crate::window::create_window_paths(
                run,
                current_rel,
                grouping_target,
                sort_input_target,
                sort_input_target_parallel_safe,
                wfl,
            )?;
            if parse.hasTargetSRFs {
                crate::srf::adjust_paths_for_srfs(
                    run,
                    current_rel,
                    &sort_input_targets,
                    &sort_input_targets_contain_srfs,
                )?;
            }
        }
    }

    if !parse.distinctClause.is_nil() {
        current_rel = create_distinct_paths(run, current_rel, sort_input_target)?;
    }

    grouping_planner_tail(
        run,
        current_rel,
        final_target,
        final_target_parallel_safe,
        &final_targets,
        &final_targets_contain_srfs,
        have_postponed_srfs,
        limit_tuples,
        offset_est,
        count_est,
    )
}

#[allow(clippy::too_many_arguments)]
fn grouping_planner_tail<'mcx>(
    run: &mut PlannerRun<'mcx>,
    current_rel: RelId,
    final_target: types_pathnodes::PtId,
    final_target_parallel_safe: bool,
    final_targets: &mcx::PgVec<'mcx, types_pathnodes::PtId>,
    final_targets_contain_srfs: &mcx::PgVec<'mcx, bool>,
    have_postponed_srfs: bool,
    limit_tuples: f64,
    offset_est: i64,
    count_est: i64,
) -> PgResult<()> {
    let parse = run.parse();
    let mut current_rel = current_rel;
    if !parse.sortClause.is_nil() {
        current_rel = create_ordered_paths(
            run,
            current_rel,
            final_target,
            final_target_parallel_safe,
            if have_postponed_srfs { -1.0 } else { limit_tuples },
        )?;
        if parse.hasTargetSRFs {
            crate::srf::adjust_paths_for_srfs(
                run,
                current_rel,
                final_targets,
                final_targets_contain_srfs,
            )?;
        }
    }

    let final_rel = fetch_final_rel(run);
    if run.root.rel(current_rel).consider_parallel
        && is_parallel_safe_opt(run, parse.limitOffset)?
        && is_parallel_safe_opt(run, parse.limitCount)?
    {
        run.root.rel_mut(final_rel).consider_parallel = true;
    }
    {
        let (serverid, userid, useridiscurrent, has_fdw) = {
            let cur = run.root.rel(current_rel);
            (cur.serverid, cur.userid, cur.useridiscurrent, cur.fdwroutine)
        };
        let f = run.root.rel_mut(final_rel);
        f.serverid = serverid;
        f.userid = userid;
        f.useridiscurrent = useridiscurrent;
        f.fdwroutine = has_fdw;
    }

    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(current_rel).pathlist);
    for path_id in paths.iter() {
        let mut path_id = *path_id;
        // parse->rowMarks (not root->rowMarks) gates the LockRows node:
        // non-locking marks belong to ModifyTable.
        if !parse.rowMarks.is_nil() {
            let epq_param = crate::cte::assign_special_exec_param(run)?;
            let marks = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rowMarks);
            path_id =
                crate::pathnode::create_lockrows_path(run, final_rel, path_id, marks, epq_param);
        }
        if limit_needed(parse) {
            path_id = crate::pathnode::create_limit_path(
                run,
                final_rel,
                path_id,
                parse.limitOffset,
                parse.limitCount,
                parse.limitOption,
                offset_est,
                count_est,
            );
        }
        if parse.commandType != CmdType::CMD_SELECT {
            // Non-locking auto-marks (preprocess_rowmarks) may exist; C hands
            // them to ModifyTable for EPQ mark fetches — this lane's EPQ
            // rescans instead (divergence note in preprocess_rowmarks).
            debug_assert!(run.root.rowMarks.iter().all(|&id| {
                run.rowmark(id).strength == types_nodes::LockClauseStrength::LCS_NONE
            }));
            let mcx = run.mcx;
            let onconflict = parse.onConflict.map(|oc| run.root.alloc_expr_node(oc));
            let mut root_relation: u32 = 0;
            let mut result_relations: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
            let mut update_colnos_lists: mcx::PgVec<'mcx, mcx::PgVec<'mcx, i16>> =
                mcx::PgVec::new_in(mcx);
            let mut wco_lists: mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::NodeId>> =
                mcx::PgVec::new_in(mcx);
            let mut returning_lists: mcx::PgVec<
                'mcx,
                mcx::PgVec<'mcx, types_pathnodes::NodeId>,
            > = mcx::PgVec::new_in(mcx);
            let mut merge_action_lists: mcx::PgVec<
                'mcx,
                mcx::PgVec<'mcx, types_pathnodes::NodeId>,
            > = mcx::PgVec::new_in(mcx);
            let mut merge_join_conditions: mcx::PgVec<
                'mcx,
                Option<types_pathnodes::NodeId>,
            > = mcx::PgVec::new_in(mcx);

            if crate::relnode::relids_num_members(&run.root.all_result_relids) > 1 {
                // Inherited UPDATE/DELETE/MERGE: only surviving leaf children
                // become result relations, with per-leaf translated lists.
                root_relation = parse.resultRelation as u32;
                let top_rel = run.root.simple_rel_array[parse.resultRelation as usize]
                    .expect("target rel built");
                let leaf = crate::relnode::relids_copy(mcx, &run.root.leaf_result_relids);
                for rti in crate::relnode::relids_members(&leaf) {
                    let this_rel = run.root.simple_rel_array[rti as usize]
                        .expect("leaf result rel built");
                    if crate::joinrels::is_dummy_rel(&run.root, this_rel) {
                        continue;
                    }
                    result_relations.push(rti);
                    let is_top = this_rel == top_rel;
                    if parse.commandType == CmdType::CMD_UPDATE {
                        let src =
                            crate::relnode::pgvec_clone_shallow(mcx, &run.root.update_colnos);
                        let colnos = if is_top {
                            src
                        } else {
                            crate::inherit::adjust_inherited_attnums_multilevel(
                                run,
                                src.as_slice(),
                                rti as u32,
                                parse.resultRelation as u32,
                            )
                        };
                        update_colnos_lists.push(colnos);
                    }
                    if !parse.withCheckOptions.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for wco in &parse.withCheckOptions {
                            let n = if is_top {
                                wco
                            } else {
                                crate::inherit::adjust_appendrel_attrs_multilevel(
                                    run, wco, this_rel, top_rel,
                                )?
                            };
                            ids.push(run.root.alloc_expr_node(n));
                        }
                        wco_lists.push(ids);
                    }
                    if !parse.returningList.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for tle in &parse.returningList {
                            let n = if is_top {
                                tle
                            } else {
                                crate::inherit::adjust_appendrel_attrs_multilevel(
                                    run, tle, this_rel, top_rel,
                                )?
                            };
                            ids.push(run.root.alloc_expr_node(n));
                        }
                        returning_lists.push(ids);
                    }
                    if !parse.mergeActionList.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for action_node in &parse.mergeActionList {
                            let action =
                                action_node.as_merge_action().expect("mergeActionList cell");
                            let qual = match action.qual {
                                None => None,
                                Some(q) => Some(crate::inherit::adjust_appendrel_attrs_multilevel(
                                    run, q, this_rel, top_rel,
                                )?),
                            };
                            let src_tl = action.targetList.clone_in(mcx)?;
                            let mut new_tl = types_nodes::list::NodeList::nil();
                            for tle in &src_tl {
                                new_tl.lappend(
                                    mcx,
                                    crate::inherit::adjust_appendrel_attrs_multilevel(
                                        run, tle, this_rel, top_rel,
                                    )?,
                                )?;
                            }
                            let action =
                                action_node.as_merge_action().expect("mergeActionList cell");
                            let update_colnos = if action.commandType == CmdType::CMD_UPDATE {
                                let mut src: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
                                for c in action.updateColnos.iter() {
                                    src.push(c as i16);
                                }
                                let tr = crate::inherit::adjust_inherited_attnums_multilevel(
                                    run,
                                    src.as_slice(),
                                    rti as u32,
                                    parse.resultRelation as u32,
                                );
                                let mut il = types_nodes::list::IntList::nil();
                                for &c in tr.iter() {
                                    il.lappend(mcx, c as i32)?;
                                }
                                il
                            } else {
                                action.updateColnos.clone_in(mcx)?
                            };
                            let leaf_action = types_nodes::Node::mk(
                                mcx,
                                types_nodes::primnodes::MergeAction {
                                    matchKind: action.matchKind,
                                    commandType: action.commandType,
                                    r#override: action.r#override,
                                    qual,
                                    targetList: new_tl,
                                    updateColnos: update_colnos,
                                },
                            )?;
                            ids.push(run.root.alloc_expr_node(leaf_action));
                        }
                        merge_action_lists.push(ids);
                    }
                    if parse.commandType == CmdType::CMD_MERGE {
                        let cond = match parse.mergeJoinCondition {
                            None => None,
                            Some(jc) => {
                                let n = if is_top {
                                    jc
                                } else {
                                    crate::inherit::adjust_appendrel_attrs_multilevel(
                                        run, jc, this_rel, top_rel,
                                    )?
                                };
                                Some(run.root.alloc_expr_node(n))
                            }
                        };
                        merge_join_conditions.push(cond);
                    }
                }
                if result_relations.is_empty() {
                    // Every child excluded: dummy one-relation plan over the
                    // top target rel so statement triggers still fire.
                    result_relations.push(parse.resultRelation);
                    if parse.commandType == CmdType::CMD_UPDATE {
                        update_colnos_lists.push(crate::relnode::pgvec_clone_shallow(
                            mcx,
                            &run.root.update_colnos,
                        ));
                    }
                    if !parse.withCheckOptions.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for wco in &parse.withCheckOptions {
                            ids.push(run.root.alloc_expr_node(wco));
                        }
                        wco_lists.push(ids);
                    }
                    if !parse.returningList.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for tle in &parse.returningList {
                            ids.push(run.root.alloc_expr_node(tle));
                        }
                        returning_lists.push(ids);
                    }
                    if !parse.mergeActionList.is_nil() {
                        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                            mcx::PgVec::new_in(mcx);
                        for action in &parse.mergeActionList {
                            ids.push(run.root.alloc_expr_node(action));
                        }
                        merge_action_lists.push(ids);
                    }
                    if parse.commandType == CmdType::CMD_MERGE {
                        merge_join_conditions
                            .push(parse.mergeJoinCondition.map(|jc| run.root.alloc_expr_node(jc)));
                    }
                }
            } else {
                result_relations.push(parse.resultRelation);
                if parse.commandType == CmdType::CMD_UPDATE {
                    update_colnos_lists.push(crate::relnode::pgvec_clone_shallow(
                        mcx,
                        &run.root.update_colnos,
                    ));
                }
                if !parse.withCheckOptions.is_nil() {
                    let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                        mcx::PgVec::new_in(mcx);
                    for wco in &parse.withCheckOptions {
                        ids.push(run.root.alloc_expr_node(wco));
                    }
                    wco_lists.push(ids);
                }
                if !parse.returningList.is_nil() {
                    let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                        mcx::PgVec::new_in(mcx);
                    for tle in &parse.returningList {
                        ids.push(run.root.alloc_expr_node(tle));
                    }
                    returning_lists.push(ids);
                }
                if !parse.mergeActionList.is_nil() {
                    let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                        mcx::PgVec::new_in(mcx);
                    for action in &parse.mergeActionList {
                        ids.push(run.root.alloc_expr_node(action));
                    }
                    merge_action_lists.push(ids);
                }
                if parse.commandType == CmdType::CMD_MERGE {
                    merge_join_conditions
                        .push(parse.mergeJoinCondition.map(|jc| run.root.alloc_expr_node(jc)));
                }
            }

            let part_cols_updated = run.root.partColsUpdated;
            // C passes root->rowMarks: the non-locking source-rel marks feed
            // the executor's EPQ aux rowmarks (EvalPlanQualFetchRowMark).
            let row_marks = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rowMarks);
            let epq_param = crate::cte::assign_special_exec_param(run)?;
            let mtpath = crate::pathnode::create_modifytable_path(
                run,
                final_rel,
                path_id,
                parse.commandType,
                parse.canSetTag,
                parse.resultRelation as u32,
                root_relation,
                part_cols_updated,
                result_relations,
                update_colnos_lists,
                wco_lists,
                returning_lists,
                onconflict,
                merge_action_lists,
                merge_join_conditions,
                row_marks,
                epq_param,
            );
            path_id = run.root.alloc_path(mtpath);
        }
        add_existing_path(run, final_rel, path_id);
    }
    // planner.c: hand current_rel's partial paths to final_rel when an outer
    // query level can use them (set_subquery_pathlist's partial SubqueryScan
    // loop is the consumer).
    if run.root.rel(final_rel).consider_parallel
        && run.root.query_level > 1
        && !limit_needed(parse)
    {
        debug_assert!(parse.rowMarks.is_nil() && parse.commandType == CmdType::CMD_SELECT);
        let partials = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(current_rel).partial_pathlist,
        );
        for &pid in partials.iter() {
            crate::pathnode::add_partial_path(run, final_rel, pid);
        }
    }
    // FDW upper paths, create_upper_paths_hook: absent.
    Ok(())
}

// preprocess_groupclause (planner.c); interned ids share the parse nodes,
// as C. `force` (grouping sets): sortgrouprefs whose order the result must
// follow.
pub(crate) fn preprocess_groupclause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    force: Option<&[i32]>,
) -> PgResult<mcx::PgVec<'mcx, types_pathnodes::NodeId>> {
    let mcx = run.mcx;
    let parse = run.parse();
    let mut new_groupclause: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> =
        mcx::PgVec::new_in(mcx);
    if let Some(refs) = force {
        for &r in refs {
            let cl = parse
                .groupClause
                .iter()
                .find(|n| {
                    n.as_sort_group_clause().expect("groupClause cell").tleSortGroupRef
                        == r as u32
                })
                .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in list"));
            new_groupclause.push(cl);
        }
        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
        for &n in new_groupclause.iter() {
            ids.push(run.intern_expr(n));
        }
        return Ok(ids);
    }
    if !parse.sortClause.is_nil() {
        for sc_node in &parse.sortClause {
            let sc = sc_node.as_sort_group_clause().expect("sortClause cell");
            let mut matched = false;
            for gc_node in &parse.groupClause {
                let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
                if sortgroupclause_equal(gc, sc) {
                    new_groupclause.push(gc_node);
                    matched = true;
                    break;
                }
            }
            if !matched {
                break;
            }
        }
    }
    if new_groupclause.is_empty() {
        new_groupclause.clear();
        for gc_node in &parse.groupClause {
            new_groupclause.push(gc_node);
        }
    } else {
        let mut give_up = false;
        for gc_node in &parse.groupClause {
            if new_groupclause.iter().any(|&n| n.ptr_eq(gc_node)) {
                continue;
            }
            let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
            if gc.sortop == 0 {
                give_up = true;
                break;
            }
            new_groupclause.push(gc_node);
        }
        if give_up {
            new_groupclause.clear();
            for gc_node in &parse.groupClause {
                new_groupclause.push(gc_node);
            }
        }
    }
    let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
    for &n in new_groupclause.iter() {
        ids.push(run.intern_expr(n));
    }
    Ok(ids)
}

fn sortgroupclause_equal(
    a: &types_nodes::parsenodes::SortGroupClause,
    b: &types_nodes::parsenodes::SortGroupClause,
) -> bool {
    a.tleSortGroupRef == b.tleSortGroupRef
        && a.eqop == b.eqop
        && a.sortop == b.sortop
        && a.reverse_sort == b.reverse_sort
        && a.nulls_first == b.nulls_first
        && a.hashable == b.hashable
}

// C consults processed_groupClause, not parse->groupClause (planner.c:5550):
// a GROUP BY item removed as pathkey-redundant is a non-group column here,
// which also fixes its position in the group input target.
fn target_sgref_in_group_clause(run: &PlannerRun<'_>, sgref: u32) -> bool {
    sgref != 0
        && run.root.processed_groupClause.iter().any(|&id| {
            run.root
                .expr_node(id)
                .as_sort_group_clause()
                .expect("processed_groupClause cell")
                .tleSortGroupRef
                == sgref
        })
}

// make_group_input_target (planner.c).
fn make_group_input_target<'mcx>(
    run: &mut PlannerRun<'mcx>,
    final_target: types_pathnodes::PtId,
) -> PgResult<types_pathnodes::PtId> {
    let mcx = run.mcx;

    let mut tlist = types_nodes::list::NodeList::nil();
    let mut group_exprs: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut vars: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> = mcx::PgVec::new_in(mcx);
    // The input target sits logically below the grouping step: under
    // grouping sets its expressions shed the grouping RT index
    // (planner.c:5558-5568, 5601-5612).
    let strip_rt = if run.parse().hasGroupRTE && !run.parse().groupingSets.is_nil() {
        debug_assert!(run.root.group_rtindex > 0);
        Some(run.root.group_rtindex)
    } else {
        None
    };
    let n = run.root.pathtarget(final_target).exprs.len();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let mut expr = *run.root.expr_node(ft.exprs[i]);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        if target_sgref_in_group_clause(run, sgref) {
            if let Some(rt) = strip_rt {
                expr = crate::flatten_group::strip_group_nulling(mcx, expr, rt)?.unwrap_or(expr);
            }
            let tle = types_nodes::Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr,
                    resno: (tlist.len() + 1) as i16,
                    resname: None,
                    ressortgroupref: sgref,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?;
            tlist.lappend(mcx, tle)?;
            group_exprs.push(expr);
        } else {
            pull_agg_input_vars(expr, &mut vars);
        }
    }
    if let Some(having) = run.parse().havingQual {
        pull_agg_input_vars(having, &mut vars);
    }
    if let Some(rt) = strip_rt {
        for v in vars.iter_mut() {
            if let Some(new) = crate::flatten_group::strip_group_nulling(mcx, *v, rt)? {
                *v = new;
            }
        }
    }

    // add_new_columns_to_pathtarget: dedupe by equal().
    let mut uniq: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> = mcx::PgVec::new_in(mcx);
    for &v in vars.iter() {
        if group_exprs.iter().chain(uniq.iter()).any(|&u| types_nodes::equal(u, v)) {
            continue;
        }
        uniq.push(v);
        let tle = types_nodes::Node::mk_target_entry(
            mcx,
            v,
            (tlist.len() + 1) as i16,
            None,
            false,
        )?;
        tlist.lappend(mcx, tle)?;
    }
    crate::pathnode::create_pathtarget(run, &tlist)
}

// pull_var_clause with PVC_RECURSE_AGGREGATES over the agg-lane shapes.
fn pull_agg_input_vars<'mcx>(
    node: types_nodes::Node<'mcx>,
    out: &mut mcx::PgVec<'mcx, types_nodes::Node<'mcx>>,
) {
    use types_nodes::NodeTag;
    match node.node_tag() {
        NodeTag::T_Var => out.push(node),
        NodeTag::T_Const => {}
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            for d in &a.aggdirectargs {
                pull_agg_input_vars(d, out);
            }
            for arg in &a.args {
                pull_agg_input_vars(arg, out);
            }
            if let Some(f) = a.aggfilter {
                pull_agg_input_vars(f, out);
            }
        }
        // PVC_RECURSE_WINDOWFUNCS: window args feed the grouped input target.
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                pull_agg_input_vars(arg, out);
            }
            if let Some(f) = wf.aggfilter {
                pull_agg_input_vars(f, out);
            }
        }
        // PVC_RECURSE_AGGREGATES treats GroupingFunc like Aggref.
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            debug_assert!(g.agglevelsup == 0);
            for arg in &g.args {
                pull_agg_input_vars(arg, out);
            }
        }
        NodeTag::T_TargetEntry => {
            pull_agg_input_vars(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_List => {
            for a in node.as_list().unwrap() {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_RelabelType => {
            pull_agg_input_vars(node.as_relabel_type().unwrap().arg, out)
        }
        NodeTag::T_FieldSelect => {
            pull_agg_input_vars(node.as_field_select().unwrap().arg, out)
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for a in sr.refupperindexpr.iter().flatten() {
                pull_agg_input_vars(a, out);
            }
            for a in sr.reflowerindexpr.iter().flatten() {
                pull_agg_input_vars(a, out);
            }
            if let Some(a) = sr.refexpr {
                pull_agg_input_vars(a, out);
            }
            if let Some(a) = sr.refassgnexpr {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_Param => {}
        NodeTag::T_NullTest => {
            if let Some(arg) = node.as_null_test().unwrap().arg {
                pull_agg_input_vars(arg, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(arg) = node.as_boolean_test().unwrap().arg {
                pull_agg_input_vars(arg, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_NullIfExpr => {
            for a in &node.as_null_if_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            pull_agg_input_vars(fs.arg, out);
            for a in &fs.newvals {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in &node.as_row_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_AlternativeSubPlan => {
            for a in &node.as_alternative_sub_plan().unwrap().subplans {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                pull_agg_input_vars(te, out);
            }
            for a in &sp.args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                pull_agg_input_vars(arg, out);
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                pull_agg_input_vars(cw.expr.expect("CaseWhen.expr"), out);
                pull_agg_input_vars(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                pull_agg_input_vars(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in &node.as_coalesce_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in &node.as_min_max_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for a in &node.as_array_expr().unwrap().elements {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                pull_agg_input_vars(a, out);
            }
        }
        NodeTag::T_CoerceViaIO => pull_agg_input_vars(node.as_coerce_via_io().unwrap().arg, out),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            pull_agg_input_vars(a.arg, out);
            if let Some(e) = a.elemexpr {
                pull_agg_input_vars(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            pull_agg_input_vars(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            pull_agg_input_vars(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                pull_agg_input_vars(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in &c.args {
                pull_agg_input_vars(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                pull_agg_input_vars(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                pull_agg_input_vars(e, out);
            }
        }
        NodeTag::T_JsonBehavior => {
            if let Some(e) = node.as_json_behavior().unwrap().expr {
                pull_agg_input_vars(e, out);
            }
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                pull_agg_input_vars(e, out);
            }
            for v in &j.passing_values {
                pull_agg_input_vars(v, out);
            }
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter().chain(x.args.iter()) {
                pull_agg_input_vars(a, out);
            }
        }
        // PVC_INCLUDE_PLACEHOLDERS: the PHV joins the input target whole.
        NodeTag::T_PlaceHolderVar => out.push(node),
        other => panic!("pull_var_clause (var.c): {other:?}; M3 expression lane"),
    }
}

// grouping_is_sortable/grouping_is_hashable (tlist.c) over interned clauses.
fn grouping_is_sortable(run: &PlannerRun<'_>, clauses: &[types_pathnodes::NodeId]) -> bool {
    clauses.iter().all(|&id| {
        run.root.expr_node(id).as_sort_group_clause().expect("group clause cell").sortop != 0
    })
}

fn grouping_is_hashable(run: &PlannerRun<'_>, clauses: &[types_pathnodes::NodeId]) -> bool {
    clauses.iter().all(|&id| {
        run.root.expr_node(id).as_sort_group_clause().expect("group clause cell").hashable
    })
}

// make_ordered_path (planner.c).
fn make_ordered_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    path: types_pathnodes::PathId,
    cheapest_path: types_pathnodes::PathId,
    pathkeys: &mcx::PgVec<'mcx, types_pathnodes::PathKey>,
    limit_tuples: f64,
) -> PgResult<Option<types_pathnodes::PathId>> {
    let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
        pathkeys,
        &run.root.path(path).base().pathkeys,
    );
    if is_sorted {
        return Ok(Some(path));
    }
    let use_full_sort = presorted_keys == 0 || !crate::gucs::enable_incremental_sort();
    if path != cheapest_path && use_full_sort {
        return Ok(None);
    }
    let keys = crate::relnode::pgvec_clone_shallow(run.mcx, pathkeys);
    Ok(Some(if use_full_sort {
        crate::pathnode::create_sort_path(run, rel, path, keys, limit_tuples)
    } else {
        crate::pathnode::create_incremental_sort_path(
            run,
            rel,
            path,
            keys,
            presorted_keys,
            limit_tuples,
        )?
    }))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionwiseAggregateType {
    Full,
    Partial,
    None,
}

struct GroupPathExtra<'mcx> {
    can_sort: bool,
    can_hash: bool,
    can_partial_agg: bool,
    target_parallel_safe: bool,
    having_qual: Option<types_nodes::Node<'mcx>>,
    target_list: &'mcx types_nodes::list::NodeList<'mcx>,
    patype: PartitionwiseAggregateType,
    agg_partial_costs: types_pathnodes::AggClauseCosts,
    agg_final_costs: types_pathnodes::AggClauseCosts,
    partial_costs_set: bool,
}

// make_grouping_rel (planner.c).
fn make_grouping_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    target: types_pathnodes::PtId,
    target_parallel_safe: bool,
    having_qual: Option<types_nodes::Node<'mcx>>,
) -> PgResult<RelId> {
    let input_is_other = matches!(
        run.root.rel(input_rel).reloptkind,
        types_pathnodes::RELOPT_OTHER_MEMBER_REL
            | types_pathnodes::RELOPT_OTHER_JOINREL
            | types_pathnodes::RELOPT_OTHER_UPPER_REL
    );
    let grouped_rel = if input_is_other {
        let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(input_rel).relids);
        let id =
            crate::relnode::fetch_upper_rel_with_relids(&mut run.root, UPPERREL_GROUP_AGG, relids);
        run.root.rel_mut(id).reloptkind = types_pathnodes::RELOPT_OTHER_UPPER_REL;
        id
    } else {
        crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_GROUP_AGG)
    };
    let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
        let input = run.root.rel(input_rel);
        (
            input.serverid,
            input.userid,
            input.useridiscurrent,
            input.fdwroutine,
            input.consider_parallel,
        )
    };
    let having_safe = is_parallel_safe_opt(run, having_qual)?;
    let g = run.root.rel_mut(grouped_rel);
    g.serverid = serverid;
    g.userid = userid;
    g.useridiscurrent = useridiscurrent;
    g.fdwroutine = has_fdw;
    g.consider_parallel = in_parallel && target_parallel_safe && having_safe;
    g.pathtarget_id = Some(target);
    Ok(grouped_rel)
}

// can_partial_agg (planner.c).
fn can_partial_agg(run: &PlannerRun<'_>) -> bool {
    let parse = run.parse();
    if !parse.hasAggs && parse.groupClause.is_nil() {
        false
    } else if !parse.groupingSets.is_nil() {
        false
    } else {
        !(run.root.hasNonPartialAggs || run.root.hasNonSerialAggs)
    }
}

// create_grouping_paths (planner.c), single grouping set.
fn create_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouping_target: types_pathnodes::PtId,
    target_parallel_safe: bool,
) -> PgResult<RelId> {
    let parse = run.parse();

    let mut agg_costs = types_pathnodes::AggClauseCosts::default();
    crate::prepagg::get_agg_clause_costs(
        run,
        types_pathnodes::AGGSPLIT_SIMPLE,
        &mut agg_costs,
    )?;
    let grouped_rel =
        make_grouping_rel(run, input_rel, grouping_target, target_parallel_safe, parse.havingQual)?;

    // is_degenerate_grouping: HAVING with no aggs and no GROUP BY yields one
    // Result row per grouping set (create_degenerate_grouping_paths,
    // planner.c:3966-4013), filtered by HAVING as gating quals.
    if (run.root.hasHavingQual || !parse.groupingSets.is_nil())
        && !parse.hasAggs
        && parse.groupClause.is_nil()
    {
        let nrows = parse.groupingSets.len().max(1);
        let mut quals: mcx::PgVec<'_, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
        if let Some(hq) = parse.havingQual {
            for clause in hq.as_list().expect("preprocessed havingQual is a list") {
                quals.push(run.intern_expr(clause));
            }
        }
        let mut qual_cost = types_pathnodes::QualCost::default();
        if let Some(hq) = parse.havingQual {
            qual_cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), hq)?;
        }
        let target_id = run.rel_reltarget_id(grouped_rel);
        let parallel_safe = run.root.rel(grouped_rel).consider_parallel;
        let tcost = run.root.pathtarget(target_id).cost;
        let startup_cost =
            tcost.startup + if parse.havingQual.is_some() { qual_cost.startup + qual_cost.per_tuple } else { 0.0 };
        let total_cost = tcost.startup + crate::gucs::cpu_tuple_cost() + tcost.per_tuple
            + if parse.havingQual.is_some() { qual_cost.startup + qual_cost.per_tuple } else { 0.0 };
        let mk_result = |run: &mut PlannerRun<'mcx>, quals: mcx::PgVec<'mcx, types_pathnodes::NodeId>| {
            let path = types_pathnodes::PathNode::GroupResultPath(types_pathnodes::GroupResultPath {
                path: types_pathnodes::Path {
                    type_: crate::pathnode::tag16(types_nodes::NodeTag::T_GroupResultPath),
                    pathtype: crate::pathnode::tag16(types_nodes::NodeTag::T_Result),
                    parent: grouped_rel,
                    pathtarget_id: Some(target_id),
                    param_info: None,
                    parallel_aware: false,
                    parallel_safe,
                    parallel_workers: 0,
                    rows: 1.0,
                    disabled_nodes: 0,
                    startup_cost,
                    total_cost,
                    pathkeys: mcx::PgVec::new_in(run.mcx),
                },
                quals,
            });
            run.root.alloc_path(path)
        };
        let pid = if nrows > 1 {
            let mut subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(run.mcx);
            for _ in 0..nrows {
                let q2 = {
                    let mut v: mcx::PgVec<'_, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
                    v.extend_from_slice(&quals);
                    v
                };
                subpaths.push(mk_result(run, q2));
            }
            crate::pathnode::create_append_path(
                run,
                grouped_rel,
                subpaths,
                mcx::PgVec::new_in(run.mcx),
                mcx::PgVec::new_in(run.mcx),
                &crate::relnode::RELIDS_UNSET,
                0,
                false,
                -1.0,
            )?
        } else {
            mk_result(run, quals)
        };
        crate::pathnode::add_path(run, grouped_rel, pid);
        crate::pathnode::set_cheapest(run, grouped_rel)?;
        return Ok(grouped_rel);
    }

    let can_sort = run.gset_data.as_ref().is_some_and(|gd| !gd.rollups.is_empty())
        || grouping_is_sortable(run, &run.root.processed_groupClause);
    let can_hash = !parse.groupClause.is_nil()
        && run.root.numOrderedAggs == 0
        && match &run.gset_data {
            Some(gd) => gd.any_hashable,
            None => grouping_is_hashable(run, &run.root.processed_groupClause),
        };
    let mut extra = GroupPathExtra {
        can_sort,
        can_hash,
        can_partial_agg: can_partial_agg(run),
        target_parallel_safe,
        having_qual: parse.havingQual,
        target_list: &parse.targetList,
        patype: if crate::gucs::enable_partitionwise_aggregate() && parse.groupingSets.is_nil() {
            PartitionwiseAggregateType::Full
        } else {
            PartitionwiseAggregateType::None
        },
        agg_partial_costs: types_pathnodes::AggClauseCosts::default(),
        agg_final_costs: types_pathnodes::AggClauseCosts::default(),
        partial_costs_set: false,
    };

    create_ordinary_grouping_paths(run, input_rel, grouped_rel, &agg_costs, &mut extra)?;
    crate::pathnode::set_cheapest(run, grouped_rel)?;
    Ok(grouped_rel)
}

// create_ordinary_grouping_paths (planner.c); returns the partially grouped
// rel (C's out-parameter) for the partitionwise child recursion.
fn create_ordinary_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouped_rel: RelId,
    agg_costs: &types_pathnodes::AggClauseCosts,
    extra: &mut GroupPathExtra<'mcx>,
) -> PgResult<Option<RelId>> {
    let mut patype = PartitionwiseAggregateType::None;
    if extra.patype != PartitionwiseAggregateType::None
        && crate::relnode::rel_is_partitioned(&run.root, input_rel)
    {
        patype = if extra.patype == PartitionwiseAggregateType::Full
            && group_by_has_partkey(run, input_rel, extra.target_list)?
        {
            PartitionwiseAggregateType::Full
        } else if extra.can_partial_agg {
            PartitionwiseAggregateType::Partial
        } else {
            PartitionwiseAggregateType::None
        };
    }
    let partially_grouped_rel = if extra.can_partial_agg {
        // Partitionwise partial aggregation needs the rel even without any
        // partial input paths, so the per-child appends have a home.
        let force_rel_creation = patype == PartitionwiseAggregateType::Partial;
        create_partial_grouping_paths(run, grouped_rel, input_rel, extra, force_rel_creation)?
    } else {
        None
    };

    if patype != PartitionwiseAggregateType::None {
        create_partitionwise_grouping_paths(
            run,
            input_rel,
            grouped_rel,
            partially_grouped_rel,
            agg_costs,
            patype,
            extra,
        )?;
    }

    // If we are doing partial aggregation only (a child under a parent doing
    // partitionwise PARTIAL), the parent finalizes; stop here.
    if extra.patype == PartitionwiseAggregateType::Partial {
        let pgr = partially_grouped_rel.expect("partial patype forces the rel");
        if !run.root.rel(pgr).pathlist.is_empty() {
            crate::pathnode::set_cheapest(run, pgr)?;
        }
        return Ok(partially_grouped_rel);
    }

    if let Some(pgr) = partially_grouped_rel {
        if !run.root.rel(pgr).partial_pathlist.is_empty() {
            gather_grouping_paths(run, pgr)?;
            crate::pathnode::set_cheapest(run, pgr)?;
        }
    }

    let cheapest_rows = {
        let cheapest = run.root.rel(input_rel).cheapest_total_path.unwrap();
        run.root.path(cheapest).base().rows
    };
    let num_groups = get_number_of_groups(run, cheapest_rows, extra.target_list)?;
    add_paths_to_grouping_rel(
        run,
        input_rel,
        grouped_rel,
        partially_grouped_rel,
        agg_costs,
        num_groups,
        extra,
    )?;

    if run.root.rel(grouped_rel).pathlist.is_empty() {
        return Err(could_not_implement("GROUP BY"));
    }
    Ok(partially_grouped_rel)
}

// make_partial_grouping_target (planner.c): grouping columns as-is, then the
// Vars/Aggrefs of non-group columns + HAVING, with top-level Aggrefs flat-
// copied into AGGSPLIT_INITIAL_SERIAL mode.
fn make_partial_grouping_target<'mcx>(
    run: &mut PlannerRun<'mcx>,
    grouping_target: types_pathnodes::PtId,
    having_qual: Option<types_nodes::Node<'mcx>>,
) -> PgResult<types_pathnodes::PtId> {
    let mcx = run.mcx;
    let mut tlist = types_nodes::list::NodeList::nil();
    let mut kept_exprs: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut non_group_cols = types_nodes::list::NodeList::nil();
    let n = run.root.pathtarget(grouping_target).exprs.len();
    for i in 0..n {
        let gt = run.root.pathtarget(grouping_target);
        let expr = *run.root.expr_node(gt.exprs[i]);
        let sgref = gt.sortgrouprefs.get(i).copied().unwrap_or(0);
        let in_group = sgref != 0
            && run.root.processed_groupClause.iter().any(|&id| {
                run.root.expr_node(id).as_sort_group_clause().expect("groupClause cell").tleSortGroupRef
                    == sgref
            });
        if in_group {
            let tle = types_nodes::Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr,
                    resno: (tlist.len() + 1) as i16,
                    resname: None,
                    ressortgroupref: sgref,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?;
            tlist.lappend(mcx, tle)?;
            kept_exprs.push(expr);
        } else {
            non_group_cols.lappend(mcx, expr)?;
        }
    }
    if let Some(h) = having_qual {
        non_group_cols.lappend(mcx, h)?;
    }
    let non_group_vars = vars::pull_var_clause(
        mcx,
        types_nodes::Node::mk_list(mcx, non_group_cols)?,
        vars::PVC_INCLUDE_AGGREGATES
            | vars::PVC_RECURSE_WINDOWFUNCS
            | vars::PVC_INCLUDE_PLACEHOLDERS,
    )?;

    // add_new_columns_to_pathtarget: dedupe by equal().
    let mut uniq: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> = mcx::PgVec::new_in(mcx);
    for v in &non_group_vars {
        if kept_exprs.iter().chain(uniq.iter()).any(|&u| types_nodes::equal(u, v)) {
            continue;
        }
        uniq.push(v);
        let tle =
            types_nodes::Node::mk_target_entry(mcx, v, (tlist.len() + 1) as i16, None, false)?;
        tlist.lappend(mcx, tle)?;
    }

    // Adjust top-level Aggrefs into partial mode (flat copy per C).
    let mut marked = types_nodes::list::NodeList::nil();
    for tle_node in &tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let new_expr = if tle.expr.node_tag() == types_nodes::NodeTag::T_Aggref {
            mark_partial_aggref_copy(run, tle.expr, types_pathnodes::AGGSPLIT_INITIAL_SERIAL)?
        } else {
            tle.expr
        };
        let new_tle = types_nodes::Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: new_expr,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        marked.lappend(mcx, new_tle)?;
    }
    crate::pathnode::create_pathtarget(run, &marked)
}

// mark_partial_aggref (planner.c) on a flat copy of the Aggref.
fn mark_partial_aggref_copy<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: types_nodes::Node<'mcx>,
    aggsplit: types_pathnodes::AggSplit,
) -> PgResult<types_nodes::Node<'mcx>> {
    let a = node.as_aggref().expect("Aggref");
    let args = a.args.clone_in(run.mcx)?;
    make_marked_aggref(run, a, aggsplit, args, a.aggfilter)
}

// Flat-copy + mark_partial_aggref, with args/aggfilter overridable
// (convert_combining_aggrefs builds the parent Aggref this way).
pub(crate) fn make_marked_aggref<'mcx>(
    run: &mut PlannerRun<'mcx>,
    a: &types_nodes::primnodes::Aggref<'mcx>,
    aggsplit: types_pathnodes::AggSplit,
    args: types_nodes::list::NodeList<'mcx>,
    aggfilter: Option<types_nodes::Node<'mcx>>,
) -> PgResult<types_nodes::Node<'mcx>> {
    const INTERNALOID: u32 = 2281;
    const BYTEAOID: u32 = 17;
    let mcx = run.mcx;
    assert!(a.aggtranstype != 0, "mark_partial_aggref: aggtranstype unresolved");
    assert!(
        a.aggsplit == types_pathnodes::AGGSPLIT_SIMPLE,
        "mark_partial_aggref: aggsplit already set"
    );
    let skip_final = aggsplit & types_pathnodes::AGGSPLITOP_SKIPFINAL != 0;
    let serialize = aggsplit & types_pathnodes::AGGSPLITOP_SERIALIZE != 0;
    let aggtype = if skip_final {
        if a.aggtranstype == INTERNALOID && serialize { BYTEAOID } else { a.aggtranstype }
    } else {
        a.aggtype
    };
    types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::Aggref {
            aggfnoid: a.aggfnoid,
            aggtype,
            aggcollid: a.aggcollid,
            inputcollid: a.inputcollid,
            aggtranstype: a.aggtranstype,
            aggargtypes: a.aggargtypes.clone_in(mcx)?,
            aggdirectargs: a.aggdirectargs.clone_in(mcx)?,
            args,
            aggorder: a.aggorder.clone_in(mcx)?,
            aggdistinct: a.aggdistinct.clone_in(mcx)?,
            aggfilter,
            aggstar: a.aggstar,
            aggvariadic: a.aggvariadic,
            aggkind: a.aggkind,
            aggpresorted: a.aggpresorted,
            agglevelsup: a.agglevelsup,
            aggsplit,
            aggno: a.aggno,
            aggtransno: a.aggtransno,
            location: a.location,
        },
    )
}

// create_partial_grouping_paths (planner.c), parallel (partial-input) legs
// only: the non-partial leg exists solely under partitionwise PARTIAL, which
// is loud in create_ordinary_grouping_paths, so force_rel_creation never
// arises and NULL returns stand in for both gates.
fn create_partial_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    grouped_rel: RelId,
    input_rel: RelId,
    extra: &mut GroupPathExtra<'mcx>,
    force_rel_creation: bool,
) -> PgResult<Option<RelId>> {
    // M5-3 (§2.3): a covered shape gets NO partial-aggregation machinery at
    // all under engine=runtime — no partially-grouped rel, hence no grouped
    // Gather Merges and no finalize-over-Gather paths; the serial Agg over
    // the serial scan is the plan the runtime router picks up. Returning
    // None here is the ordinary "partial grouping not possible" answer the
    // caller already handles. force_rel_creation (a partitionwise parent's
    // requirement) wins — but partitioned shapes never classify covered.
    if !force_rel_creation && crate::m5_suppress::m5_suppress_gather(run)? {
        return Ok(None);
    }
    // NLIDX (GL-NLIDX-2, rel-aware — input_rel is the final joinrel with
    // its serial cheapest set): a keyed shape gets NO partial-aggregation
    // machinery, so no Finalize/Gather/Partial forms exist and the serial
    // NL plan reaches the executor arm.
    if !force_rel_creation && crate::m5_suppress::m5_suppress_gather_nlidx(run, input_rel)? {
        return Ok(None);
    }
    let parse = run.parse();

    // Partially aggregated NON-partial paths exist only under a parent doing
    // partitionwise PARTIAL aggregation (extra.patype is the parent's type).
    let cheapest_total_path = if !run.root.rel(input_rel).pathlist.is_empty()
        && extra.patype == PartitionwiseAggregateType::Partial
    {
        run.root.rel(input_rel).cheapest_total_path
    } else {
        None
    };

    let cheapest_partial_path = if run.root.rel(grouped_rel).consider_parallel
        && !run.root.rel(input_rel).partial_pathlist.is_empty()
    {
        Some(run.root.rel(input_rel).partial_pathlist[0])
    } else {
        None
    };

    if cheapest_total_path.is_none() && cheapest_partial_path.is_none() && !force_rel_creation {
        return Ok(None);
    }

    let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(grouped_rel).relids);
    let partially_grouped_rel = crate::relnode::fetch_upper_rel_with_relids(
        &mut run.root,
        types_pathnodes::UPPERREL_PARTIAL_GROUP_AGG,
        relids,
    );
    {
        let (cp, rk, sid, uid, uidc, fdw) = {
            let g = run.root.rel(grouped_rel);
            (
                g.consider_parallel,
                g.reloptkind,
                g.serverid,
                g.userid,
                g.useridiscurrent,
                g.fdwroutine,
            )
        };
        let p = run.root.rel_mut(partially_grouped_rel);
        p.consider_parallel = cp;
        p.reloptkind = rk;
        p.serverid = sid;
        p.userid = uid;
        p.useridiscurrent = uidc;
        p.fdwroutine = fdw;
    }
    let grouping_target =
        run.root.rel(grouped_rel).pathtarget_id.expect("grouped rel has a target");
    let partial_target = make_partial_grouping_target(run, grouping_target, extra.having_qual)?;
    run.root.rel_mut(partially_grouped_rel).pathtarget_id = Some(partial_target);

    if !extra.partial_costs_set {
        extra.agg_partial_costs = types_pathnodes::AggClauseCosts::default();
        extra.agg_final_costs = types_pathnodes::AggClauseCosts::default();
        if parse.hasAggs {
            crate::prepagg::get_agg_clause_costs(
                run,
                types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
                &mut extra.agg_partial_costs,
            )?;
            crate::prepagg::get_agg_clause_costs(
                run,
                types_pathnodes::AGGSPLIT_FINAL_DESERIAL,
                &mut extra.agg_final_costs,
            )?;
        }
        extra.partial_costs_set = true;
    }

    let num_partial_groups = match cheapest_total_path {
        Some(p) => {
            let rows = run.root.path(p).base().rows;
            get_number_of_groups(run, rows, extra.target_list)?
        }
        None => 0.0,
    };
    let num_partial_partial_groups = match cheapest_partial_path {
        Some(p) => {
            let rows = run.root.path(p).base().rows;
            get_number_of_groups(run, rows, extra.target_list)?
        }
        None => 0.0,
    };

    // Partially aggregated non-partial paths (parent finalizes over Append).
    if extra.can_sort {
        if let Some(cheapest_total) = cheapest_total_path {
            let paths =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
            for &path_id in paths.iter() {
                let path_keys = crate::relnode::pgvec_clone_shallow(
                    run.mcx,
                    &run.root.path(path_id).base().pathkeys,
                );
                let orderings = crate::pathkeys::get_useful_group_keys_orderings(run, &path_keys);
                for info in orderings {
                    let Some(sorted) = make_ordered_path(
                        run,
                        partially_grouped_rel,
                        path_id,
                        cheapest_total,
                        &info.pathkeys,
                        -1.0,
                    )?
                    else {
                        continue;
                    };
                    if parse.hasAggs {
                        let strategy = if parse.groupClause.is_nil() {
                            types_pathnodes::AGG_PLAIN
                        } else {
                            types_pathnodes::AGG_SORTED
                        };
                        let agg_costs_partial = extra.agg_partial_costs;
                        let agg_path = crate::pathnode::create_agg_path(
                            run,
                            partially_grouped_rel,
                            sorted,
                            partial_target,
                            strategy,
                            types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
                            info.clauses,
                            mcx::PgVec::new_in(run.mcx),
                            &agg_costs_partial,
                            num_partial_groups,
                        )?;
                        crate::pathnode::add_path(run, partially_grouped_rel, agg_path);
                    } else {
                        let group_path = crate::pathnode::create_group_path(
                            run,
                            partially_grouped_rel,
                            sorted,
                            info.clauses,
                            mcx::PgVec::new_in(run.mcx),
                            num_partial_groups,
                        )?;
                        crate::pathnode::add_path(run, partially_grouped_rel, group_path);
                    }
                }
            }
        }
    }

    if extra.can_sort && cheapest_partial_path.is_some() {
        let cheapest_partial_path = cheapest_partial_path.expect("just checked");
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).partial_pathlist);
        for &path_id in paths.iter() {
            let path_keys = crate::relnode::pgvec_clone_shallow(
                run.mcx,
                &run.root.path(path_id).base().pathkeys,
            );
            let orderings = crate::pathkeys::get_useful_group_keys_orderings(run, &path_keys);
            for info in orderings {
                let Some(sorted) = make_ordered_path(
                    run,
                    partially_grouped_rel,
                    path_id,
                    cheapest_partial_path,
                    &info.pathkeys,
                    -1.0,
                )?
                else {
                    continue;
                };
                if parse.hasAggs {
                    let strategy = if parse.groupClause.is_nil() {
                        types_pathnodes::AGG_PLAIN
                    } else {
                        types_pathnodes::AGG_SORTED
                    };
                    let agg_costs_partial = extra.agg_partial_costs;
                    let agg_path = crate::pathnode::create_agg_path(
                        run,
                        partially_grouped_rel,
                        sorted,
                        partial_target,
                        strategy,
                        types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
                        info.clauses,
                        mcx::PgVec::new_in(run.mcx),
                        &agg_costs_partial,
                        num_partial_partial_groups,
                    )?;
                    crate::pathnode::add_partial_path(run, partially_grouped_rel, agg_path);
                } else {
                    let group_path = crate::pathnode::create_group_path(
                        run,
                        partially_grouped_rel,
                        sorted,
                        info.clauses,
                        mcx::PgVec::new_in(run.mcx),
                        num_partial_partial_groups,
                    )?;
                    crate::pathnode::add_partial_path(run, partially_grouped_rel, group_path);
                }
            }
        }
    }

    if extra.can_hash {
        if let Some(cheapest_total) = cheapest_total_path {
            let group_clause =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
            let agg_costs_partial = extra.agg_partial_costs;
            let agg_path = crate::pathnode::create_agg_path(
                run,
                partially_grouped_rel,
                cheapest_total,
                partial_target,
                types_pathnodes::AGG_HASHED,
                types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
                group_clause,
                mcx::PgVec::new_in(run.mcx),
                &agg_costs_partial,
                num_partial_groups,
            )?;
            crate::pathnode::add_path(run, partially_grouped_rel, agg_path);
        }
    }

    if extra.can_hash {
        if let Some(cheapest_partial_path) = cheapest_partial_path {
            let group_clause =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
            let agg_costs_partial = extra.agg_partial_costs;
            let agg_path = crate::pathnode::create_agg_path(
                run,
                partially_grouped_rel,
                cheapest_partial_path,
                partial_target,
                types_pathnodes::AGG_HASHED,
                types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
                group_clause,
                mcx::PgVec::new_in(run.mcx),
                &agg_costs_partial,
                num_partial_partial_groups,
            )?;
            crate::pathnode::add_partial_path(run, partially_grouped_rel, agg_path);
        }
    }

    Ok(Some(partially_grouped_rel))
}

// gather_grouping_paths (planner.c).
// M5-3 note: NO suppression gate here — a partially-grouped rel's MAIN
// pathlist is populated exclusively by these gathers (its own paths are
// partial), and set_cheapest right after would find an empty pathlist
// ("could not devise a query plan"). Covered shapes are suppressed
// UPSTREAM instead: create_partial_grouping_paths returns None, so this
// function is never reached for them.
fn gather_grouping_paths<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let groupby_pathkeys: mcx::PgVec<'mcx, types_pathnodes::PathKey> = {
        let n = run.root.num_groupby_pathkeys as usize;
        let take = if run.root.group_pathkeys.len() > n { n } else { run.root.group_pathkeys.len() };
        let mut keys = mcx::PgVec::new_in(run.mcx);
        keys.extend(run.root.group_pathkeys.iter().take(take).copied());
        keys
    };

    crate::allpaths::generate_useful_gather_paths(run, rel, true)?;

    let cheapest_partial_path = run.root.rel(rel).partial_pathlist[0];
    let target = run.rel_reltarget_id(rel);
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).partial_pathlist);
    for &path_id in paths.iter() {
        let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
            &groupby_pathkeys,
            &run.root.path(path_id).base().pathkeys,
        );
        if is_sorted {
            continue;
        }
        let use_full_sort = presorted_keys == 0 || !crate::gucs::enable_incremental_sort();
        if path_id != cheapest_partial_path && use_full_sort {
            continue;
        }
        let keys = crate::relnode::pgvec_clone_shallow(run.mcx, &groupby_pathkeys);
        let sorted = if use_full_sort {
            crate::pathnode::create_sort_path(run, rel, path_id, keys, -1.0)
        } else {
            crate::pathnode::create_incremental_sort_path(
                run,
                rel,
                path_id,
                keys,
                presorted_keys,
                -1.0,
            )?
        };
        let (total_groups, gm_keys) = {
            let sb = run.root.path(sorted).base();
            (
                ::costsize::compute_gather_rows(sb.rows, sb.parallel_workers),
                crate::relnode::pgvec_clone_shallow(run.mcx, &sb.pathkeys),
            )
        };
        let gm = crate::pathnode::create_gather_merge_path(
            run,
            rel,
            sorted,
            Some(target),
            gm_keys,
            Some(total_groups),
        );
        crate::pathnode::add_path(run, rel, gm);
    }
    Ok(())
}

// add_paths_to_grouping_rel (planner.c).
#[allow(clippy::too_many_arguments)]
fn add_paths_to_grouping_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouped_rel: RelId,
    partially_grouped_rel: Option<RelId>,
    agg_costs: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
    extra: &GroupPathExtra<'mcx>,
) -> PgResult<()> {
    let parse = run.parse();
    let grouping_target =
        run.root.rel(grouped_rel).pathtarget_id.expect("grouped rel has a target");
    let can_sort = extra.can_sort;
    let can_hash = extra.can_hash;
    let cheapest = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
    let having_qual: mcx::PgVec<'mcx, types_pathnodes::NodeId> = {
        let mut v = mcx::PgVec::new_in(run.mcx);
        if let Some(h) = extra.having_qual {
            for hc in h.as_list().expect("preprocessed havingQual is a list") {
                v.push(run.intern_expr(hc));
            }
        }
        v
    };

    if can_sort {
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
        for &path_id in paths.iter() {
            let path_keys =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.path(path_id).base().pathkeys);
            let orderings = crate::pathkeys::get_useful_group_keys_orderings(run, &path_keys);
            for info in orderings {
                let Some(sorted) =
                    make_ordered_path(run, grouped_rel, path_id, cheapest, &info.pathkeys, -1.0)?
                else {
                    continue;
                };
                if !parse.groupingSets.is_nil() {
                    crate::groupingsets::consider_groupingsets_paths(
                        run,
                        grouped_rel,
                        sorted,
                        true,
                        can_hash,
                        agg_costs,
                        &having_qual,
                        num_groups,
                    )?;
                    continue;
                }
                if !parse.hasAggs {
                    assert!(
                        !parse.groupClause.is_nil(),
                        "add_paths_to_grouping_rel (planner.c): no aggs and no GROUP BY"
                    );
                    let group_path = crate::pathnode::create_group_path(
                        run,
                        grouped_rel,
                        sorted,
                        info.clauses,
                        crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                        num_groups,
                    )?;
                    crate::pathnode::add_path(run, grouped_rel, group_path);
                    continue;
                }
                let strategy = if parse.groupClause.is_nil() {
                    types_pathnodes::AGG_PLAIN
                } else {
                    types_pathnodes::AGG_SORTED
                };
                let agg_path = crate::pathnode::create_agg_path(
                    run,
                    grouped_rel,
                    sorted,
                    grouping_target,
                    strategy,
                    types_pathnodes::AGGSPLIT_SIMPLE,
                    info.clauses,
                    crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                    agg_costs,
                    num_groups,
                )?;
                crate::pathnode::add_path(run, grouped_rel, agg_path);
            }
        }

        // Finalize partially aggregated paths (sorted flavor).
        if let Some(pgr) = partially_grouped_rel.filter(|&p| !run.root.rel(p).pathlist.is_empty())
        {
            let pgr_cheapest =
                run.root.rel(pgr).cheapest_total_path.expect("partially grouped rel has paths");
            let paths =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(pgr).pathlist);
            for &path_id in paths.iter() {
                let path_keys = crate::relnode::pgvec_clone_shallow(
                    run.mcx,
                    &run.root.path(path_id).base().pathkeys,
                );
                let orderings = crate::pathkeys::get_useful_group_keys_orderings(run, &path_keys);
                for info in orderings {
                    let Some(sorted) = make_ordered_path(
                        run,
                        grouped_rel,
                        path_id,
                        pgr_cheapest,
                        &info.pathkeys,
                        -1.0,
                    )?
                    else {
                        continue;
                    };
                    if parse.hasAggs {
                        let strategy = if parse.groupClause.is_nil() {
                            types_pathnodes::AGG_PLAIN
                        } else {
                            types_pathnodes::AGG_SORTED
                        };
                        let final_costs = extra.agg_final_costs;
                        let agg_path = crate::pathnode::create_agg_path(
                            run,
                            grouped_rel,
                            sorted,
                            grouping_target,
                            strategy,
                            types_pathnodes::AGGSPLIT_FINAL_DESERIAL,
                            info.clauses,
                            crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                            &final_costs,
                            num_groups,
                        )?;
                        crate::pathnode::add_path(run, grouped_rel, agg_path);
                    } else {
                        let group_path = crate::pathnode::create_group_path(
                            run,
                            grouped_rel,
                            sorted,
                            info.clauses,
                            crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                            num_groups,
                        )?;
                        crate::pathnode::add_path(run, grouped_rel, group_path);
                    }
                }
            }
        }
    }

    if can_hash {
        if !parse.groupingSets.is_nil() {
            crate::groupingsets::consider_groupingsets_paths(
                run,
                grouped_rel,
                cheapest,
                false,
                true,
                agg_costs,
                &having_qual,
                num_groups,
            )?;
        } else {
            let group_clause =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
            let agg_path = crate::pathnode::create_agg_path(
                run,
                grouped_rel,
                cheapest,
                grouping_target,
                types_pathnodes::AGG_HASHED,
                types_pathnodes::AGGSPLIT_SIMPLE,
                group_clause,
                crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                agg_costs,
                num_groups,
            )?;
            crate::pathnode::add_path(run, grouped_rel, agg_path);
        }

        // Finalize HashAgg atop the cheapest partially grouped path.
        if let Some(pgr) = partially_grouped_rel {
            if !run.root.rel(pgr).pathlist.is_empty() {
                let path = run
                    .root
                    .rel(pgr)
                    .cheapest_total_path
                    .expect("partially grouped rel has a cheapest path");
                let group_clause =
                    crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
                let final_costs = extra.agg_final_costs;
                let agg_path = crate::pathnode::create_agg_path(
                    run,
                    grouped_rel,
                    path,
                    grouping_target,
                    types_pathnodes::AGG_HASHED,
                    types_pathnodes::AGGSPLIT_FINAL_DESERIAL,
                    group_clause,
                    crate::relnode::pgvec_clone_shallow(run.mcx, &having_qual),
                    &final_costs,
                    num_groups,
                )?;
                crate::pathnode::add_path(run, grouped_rel, agg_path);
            }
        }
    }

    // Partitionwise aggregation can leave fully aggregated partial paths
    // (Parallel Append of per-child paths); unreachable while partitionwise
    // PARTIAL stays loud, but the C tail is one gather call.
    if !run.root.rel(grouped_rel).partial_pathlist.is_empty() {
        gather_grouping_paths(run, grouped_rel)?;
    }

    Ok(())
}

// create_partitionwise_grouping_paths (planner.c).
#[allow(clippy::too_many_arguments)]
fn create_partitionwise_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouped_rel: RelId,
    partially_grouped_rel: Option<RelId>,
    agg_costs: &types_pathnodes::AggClauseCosts,
    patype: PartitionwiseAggregateType,
    extra: &GroupPathExtra<'mcx>,
) -> PgResult<()> {
    debug_assert!(patype != PartitionwiseAggregateType::None);
    debug_assert!(
        patype != PartitionwiseAggregateType::Partial || partially_grouped_rel.is_some()
    );
    let target = run.root.rel(grouped_rel).pathtarget_id.expect("grouped rel has a target");
    let mut live_children: mcx::PgVec<'mcx, RelId> = mcx::PgVec::new_in(run.mcx);
    let mut partially_grouped_live_children: mcx::PgVec<'mcx, RelId> =
        mcx::PgVec::new_in(run.mcx);
    let mut partial_grouping_valid = true;
    let live = crate::relnode::relids_copy(run.mcx, &run.root.rel(input_rel).live_parts);
    for i in crate::relnode::relids_members(&live) {
        let child_input = run.root.rel(input_rel).part_rels[i as usize]
            .expect("live partition has a RelOptInfo");
        if crate::joinrels::is_dummy_rel(&run.root, child_input) {
            continue;
        }
        let child_relids =
            crate::relnode::relids_copy(run.mcx, &run.root.rel(child_input).relids);
        let appinfos = crate::inherit::find_appinfos_by_relids(run, &child_relids);

        let src_exprs =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(target).exprs);
        let mut exprs: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
        for &eid in src_exprs.iter() {
            let e = *run.root.expr_node(eid);
            let tr = crate::inherit::adjust_appendrel_attrs_multi(run, e, &appinfos)?;
            exprs.push(run.intern_expr(tr));
        }
        let child_target = {
            let src = run.root.pathtarget(target);
            let copy = types_pathnodes::PathTarget {
                exprs,
                sortgrouprefs: crate::relnode::pgvec_clone_shallow(run.mcx, &src.sortgrouprefs),
                cost: src.cost,
                width: src.width,
                has_volatile_expr: src.has_volatile_expr,
            };
            run.root.alloc_pathtarget(copy)
        };
        let child_having = match extra.having_qual {
            Some(h) => Some(crate::inherit::adjust_appendrel_attrs_multi(run, h, &appinfos)?),
            None => None,
        };
        let tl_node =
            types_nodes::Node::mk_list(run.mcx, extra.target_list.clone_in(run.mcx)?)?;
        let child_tlist = crate::inherit::adjust_appendrel_attrs_multi(run, tl_node, &appinfos)?
            .as_list()
            .expect("translated targetlist is a list");
        let mut child_extra = GroupPathExtra {
            can_sort: extra.can_sort,
            can_hash: extra.can_hash,
            can_partial_agg: extra.can_partial_agg,
            target_parallel_safe: extra.target_parallel_safe,
            having_qual: child_having,
            target_list: child_tlist,
            // extra.patype was the parent's value; for the child, our value
            // is its parent's value.
            patype,
            agg_partial_costs: extra.agg_partial_costs,
            agg_final_costs: extra.agg_final_costs,
            partial_costs_set: extra.partial_costs_set,
        };

        let child_grouped_rel = make_grouping_rel(
            run,
            child_input,
            child_target,
            extra.target_parallel_safe,
            child_having,
        )?;
        let child_partially_grouped_rel = create_ordinary_grouping_paths(
            run,
            child_input,
            child_grouped_rel,
            agg_costs,
            &mut child_extra,
        )?;

        match child_partially_grouped_rel {
            Some(cpgr) => partially_grouped_live_children.push(cpgr),
            None => partial_grouping_valid = false,
        }

        if patype == PartitionwiseAggregateType::Full {
            crate::pathnode::set_cheapest(run, child_grouped_rel)?;
            live_children.push(child_grouped_rel);
        }
    }

    // A partially grouped path must exist for EVERY child to append them.
    if let Some(pgr) = partially_grouped_rel {
        if partial_grouping_valid {
            assert!(
                !partially_grouped_live_children.is_empty(),
                "partitioned input rel has no live children"
            );
            crate::allpaths::add_paths_to_append_rel(run, pgr, &partially_grouped_live_children)?;
            // The finalization step uses the rel's cheapest path.
            if !run.root.rel(pgr).pathlist.is_empty() {
                crate::pathnode::set_cheapest(run, pgr)?;
            }
        }
    }

    if patype == PartitionwiseAggregateType::Full {
        assert!(!live_children.is_empty(), "partitioned input rel has no live children");
        crate::allpaths::add_paths_to_append_rel(run, grouped_rel, &live_children)?;
    }
    Ok(())
}

// group_by_has_partkey (planner.c); checks parse->groupClause, not
// processed_groupClause (partkey columns proved redundant still count).
fn group_by_has_partkey<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    target_list: &types_nodes::list::NodeList<'mcx>,
) -> PgResult<bool> {
    let parse = run.parse();
    let mut clause_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
    for gc in &parse.groupClause {
        clause_ids.push(run.intern_expr(gc));
    }
    let groupexprs = sortgrouplist_exprs(run, &clause_ids, target_list);

    let rel = run.root.rel(input_rel);
    if rel.partexprs.is_empty() {
        return Ok(false);
    }
    let scheme = rel.part_scheme.as_ref().expect("partitioned rel has a scheme");
    for cnt in 0..scheme.partnatts as usize {
        let partcoll = scheme.partcollation[cnt];
        let mut found = false;
        'partexpr: for &pid in rel.partexprs[cnt].iter() {
            let partexpr = *run.root.expr_node(pid);
            for &(_, ge) in groupexprs.iter() {
                let groupcoll = crate::pathkeys::expr_collation(ge);
                // At most one RelabelType survives eval_const_expressions.
                let g = match ge.as_relabel_type() {
                    Some(r) => r.arg,
                    None => ge,
                };
                if types_nodes::equal(g, partexpr) {
                    if partcoll != 0 && groupcoll != 0 && partcoll != groupcoll {
                        return Ok(false);
                    }
                    found = true;
                    break 'partexpr;
                }
            }
        }
        if !found {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cold]
#[inline(never)]
pub(crate) fn could_not_implement(what: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(format!("could not implement {what}"))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "Some of the datatypes only support hashing, while others only support sorting.",
            ),
    )
}

// get_number_of_groups (planner.c).
fn get_number_of_groups<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_rows: f64,
    target_list: &'mcx types_nodes::list::NodeList<'mcx>,
) -> PgResult<f64> {
    let parse = run.parse();
    if !parse.groupClause.is_nil() {
        if !parse.groupingSets.is_nil() {
            let mut gd = run.gset_data.take().expect("grouping sets preprocessed");
            let tlist = target_list;
            let mut dnum_groups = 0.0;
            for rollup in gd.rollups.iter_mut() {
                let clauses =
                    crate::relnode::pgvec_clone_shallow(run.mcx, &rollup.groupClause);
                let group_exprs = sortgrouplist_exprs(run, &clauses, tlist);
                rollup.numGroups = 0.0;
                for (gset, gs) in rollup.gsets.iter().zip(rollup.gsets_data.iter_mut()) {
                    let num_groups = crate::selfuncs::estimate_num_groups_pgset(
                        run,
                        &group_exprs,
                        path_rows,
                        Some(gset),
                    )?;
                    gs.numGroups = num_groups;
                    rollup.numGroups += num_groups;
                }
                dnum_groups += rollup.numGroups;
            }
            if !gd.hash_sets_idx.is_empty() {
                gd.dNumHashGroups = 0.0;
                let clauses = crate::relnode::pgvec_clone_shallow(
                    run.mcx,
                    &run.root.processed_groupClause,
                );
                let group_exprs = sortgrouplist_exprs(run, &clauses, tlist);
                for (gset, gs) in gd.hash_sets_idx.iter().zip(gd.unsortable_sets.iter_mut()) {
                    let num_groups = crate::selfuncs::estimate_num_groups_pgset(
                        run,
                        &group_exprs,
                        path_rows,
                        Some(gset),
                    )?;
                    gs.numGroups = num_groups;
                    gd.dNumHashGroups += num_groups;
                }
                dnum_groups += gd.dNumHashGroups;
            }
            run.gset_data = Some(gd);
            return Ok(dnum_groups);
        }
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
        let group_exprs = sortgrouplist_exprs(run, &clauses, target_list);
        return crate::selfuncs::estimate_num_groups(run, &group_exprs, path_rows);
    }
    if !parse.groupingSets.is_nil() {
        return Ok(parse.groupingSets.len() as f64);
    }
    Ok(1.0)
}

// create_distinct_paths (planner.c).
fn create_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    target: types_pathnodes::PtId,
) -> PgResult<RelId> {
    let distinct_rel = crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_DISTINCT);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (
                input.serverid,
                input.userid,
                input.useridiscurrent,
                input.fdwroutine,
                input.consider_parallel,
            )
        };
        let d = run.root.rel_mut(distinct_rel);
        d.serverid = serverid;
        d.userid = userid;
        d.useridiscurrent = useridiscurrent;
        d.fdwroutine = has_fdw;
        d.consider_parallel = in_parallel;
        d.pathtarget_id = Some(target);
    }

    create_final_distinct_paths(run, input_rel, distinct_rel)?;
    create_partial_distinct_paths(run, input_rel, distinct_rel, target)?;

    if run.root.rel(distinct_rel).pathlist.is_empty() {
        return Err(could_not_implement("DISTINCT"));
    }
    crate::pathnode::set_cheapest(run, distinct_rel)?;
    Ok(distinct_rel)
}

// create_partial_distinct_paths (planner.c): distinctify each worker's
// stream, gather, then run the final distinctification over the gathered
// paths.
fn create_partial_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    final_distinct_rel: RelId,
    target: types_pathnodes::PtId,
) -> PgResult<()> {
    {
        let input = run.root.rel(input_rel);
        if !input.consider_parallel || input.partial_pathlist.is_empty() {
            return Ok(());
        }
    }
    // M5-3 (§2.3): covered shapes build no partial-distinct machinery at
    // all (the partial-distinct rel's main pathlist is fed only by its own
    // gathers — suppressing those alone would leave it path-less); the
    // serial Unique/Agg plan is what the runtime distinct sink admits.
    if crate::m5_suppress::m5_suppress_gather(run)? {
        return Ok(());
    }
    let parse = run.parse();
    // Parallel DISTINCT ON would lose the deterministic row choice.
    if parse.hasDistinctOn {
        return Ok(());
    }

    let partial_distinct_rel =
        crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_PARTIAL_DISTINCT);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (
                input.serverid,
                input.userid,
                input.useridiscurrent,
                input.fdwroutine,
                input.consider_parallel,
            )
        };
        let d = run.root.rel_mut(partial_distinct_rel);
        d.serverid = serverid;
        d.userid = userid;
        d.useridiscurrent = useridiscurrent;
        d.fdwroutine = has_fdw;
        d.consider_parallel = in_parallel;
        d.pathtarget_id = Some(target);
    }

    let cheapest_partial = run.root.rel(input_rel).partial_pathlist[0];
    let num_distinct_rows = {
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_distinctClause);
        let exprs = sortgrouplist_exprs(run, &clauses, &parse.targetList);
        let rows = run.root.path(cheapest_partial).base().rows;
        crate::selfuncs::estimate_num_groups(run, &exprs, rows)?
    };

    if grouping_is_sortable(run, &run.root.processed_distinctClause) {
        let distinct_pathkeys =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.distinct_pathkeys);
        let paths = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(input_rel).partial_pathlist,
        );
        for &input_path in paths.iter() {
            let useful_list = get_useful_pathkeys_for_distinct(
                run,
                &distinct_pathkeys,
                &crate::relnode::pgvec_clone_shallow(
                    run.mcx,
                    &run.root.path(input_path).base().pathkeys,
                ),
            );
            for useful in useful_list {
                let Some(sorted) = make_ordered_path(
                    run,
                    partial_distinct_rel,
                    input_path,
                    cheapest_partial,
                    &useful,
                    -1.0,
                )? else {
                    continue;
                };
                if run.root.distinct_pathkeys.is_empty() {
                    // All DISTINCT keys redundant: each worker contributes at
                    // most one row; the final step limits again post-Gather.
                    let limit =
                        limit_one_path(run, partial_distinct_rel, sorted)?;
                    crate::pathnode::add_partial_path(run, partial_distinct_rel, limit);
                } else {
                    let numkeys = run.root.distinct_pathkeys.len() as i32;
                    let unique = crate::pathnode::create_upper_unique_path(
                        run,
                        partial_distinct_rel,
                        sorted,
                        numkeys,
                        num_distinct_rows,
                    );
                    crate::pathnode::add_partial_path(run, partial_distinct_rel, unique);
                }
            }
        }
    }

    // Hash arm: enable_hashagg is a hard off-switch here (no must-hash
    // fallback; the final step still has its own alternatives).
    if crate::gucs::enable_hashagg()
        && grouping_is_hashable(run, &run.root.processed_distinctClause)
    {
        let distinct_clause =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_distinctClause);
        let input_target = run
            .root
            .path(cheapest_partial)
            .base()
            .pathtarget_id
            .expect("input path has a pathtarget");
        let agg_path = crate::pathnode::create_agg_path(
            run,
            partial_distinct_rel,
            cheapest_partial,
            input_target,
            types_pathnodes::AGG_HASHED,
            types_pathnodes::AGGSPLIT_SIMPLE,
            distinct_clause,
            mcx::PgVec::new_in(run.mcx),
            &types_pathnodes::AggClauseCosts::default(),
            num_distinct_rows,
        )?;
        crate::pathnode::add_partial_path(run, partial_distinct_rel, agg_path);
    }

    if !run.root.rel(partial_distinct_rel).partial_pathlist.is_empty() {
        crate::allpaths::generate_useful_gather_paths(run, partial_distinct_rel, true)?;
        crate::pathnode::set_cheapest(run, partial_distinct_rel)?;
        // Re-distinctify to remove duplicates across workers.
        create_final_distinct_paths(run, partial_distinct_rel, final_distinct_rel)?;
    }
    Ok(())
}

// LIMIT 1 as a Const int8 (makeConst in the C arms of planner.c).
fn limit_one_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    subpath: types_pathnodes::PathId,
) -> PgResult<types_pathnodes::PathId> {
    let limit_count = types_nodes::Node::mk_const(
        run.mcx,
        types_core::catalog::INT8OID,
        -1,
        0,
        8,
        datum::Datum::from_i64(1),
        false,
        true,
    )?;
    Ok(crate::pathnode::create_limit_path(
        run,
        rel,
        subpath,
        None,
        Some(limit_count),
        types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT,
        0,
        1,
    ))
}

// create_final_distinct_paths (planner.c).
fn create_final_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    distinct_rel: RelId,
) -> PgResult<()> {
    let parse = run.parse();
    let has_distinct_on = parse.hasDistinctOn;

    let cheapest = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
    let num_distinct_rows = if !parse.groupClause.is_nil()
        || !parse.groupingSets.is_nil()
        || parse.hasAggs
        || run.root.hasHavingQual
    {
        run.root.path(cheapest).base().rows
    } else {
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_distinctClause);
        let exprs = sortgrouplist_exprs(run, &clauses, &parse.targetList);
        let rows = run.root.path(cheapest).base().rows;
        crate::selfuncs::estimate_num_groups(run, &exprs, rows)?
    };

    if grouping_is_sortable(run, &run.root.processed_distinctClause) {
        let limittuples = if run.root.distinct_pathkeys.is_empty() { 1.0 } else { -1.0 };
        // DISTINCT ON sorts by the more rigorous of DISTINCT and ORDER BY
        // (the parser ensured one is a prefix of the other).
        let needed_pathkeys = if has_distinct_on
            && run.root.distinct_pathkeys.len() < run.root.sort_pathkeys.len()
        {
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys)
        } else {
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.distinct_pathkeys)
        };
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
        for &input_path in paths.iter() {
            let useful_list = get_useful_pathkeys_for_distinct(
                run,
                &needed_pathkeys,
                &crate::relnode::pgvec_clone_shallow(
                    run.mcx,
                    &run.root.path(input_path).base().pathkeys,
                ),
            );
            for useful in useful_list {
                let Some(sorted) = make_ordered_path(
                    run,
                    distinct_rel,
                    input_path,
                    cheapest,
                    &useful,
                    limittuples,
                )? else {
                    continue;
                };
                if run.root.distinct_pathkeys.is_empty() {
                    // All DISTINCT keys redundant: every retained tuple is
                    // indistinguishable, so LIMIT 1 over the sorted path
                    // uniquifies (a pre-existing LIMIT may duplicate, as C).
                    let limit = limit_one_path(run, distinct_rel, sorted)?;
                    crate::pathnode::add_path(run, distinct_rel, limit);
                } else {
                    let numkeys = run.root.distinct_pathkeys.len() as i32;
                    let unique = crate::pathnode::create_upper_unique_path(
                        run,
                        distinct_rel,
                        sorted,
                        numkeys,
                        num_distinct_rows,
                    );
                    crate::pathnode::add_path(run, distinct_rel, unique);
                }
            }
        }
    }

    let allow_hash = if run.root.rel(distinct_rel).pathlist.is_empty() {
        true
    } else {
        // Hashing loses DISTINCT ON's row-choice semantics.
        !has_distinct_on && crate::gucs::enable_hashagg()
    };
    if allow_hash && grouping_is_hashable(run, &run.root.processed_distinctClause) {
        let distinct_clause =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_distinctClause);
        let input_target = run
            .root
            .path(cheapest)
            .base()
            .pathtarget_id
            .expect("input path has a pathtarget");
        let agg_path = crate::pathnode::create_agg_path(
            run,
            distinct_rel,
            cheapest,
            input_target,
            types_pathnodes::AGG_HASHED,
            types_pathnodes::AGGSPLIT_SIMPLE,
            distinct_clause,
            mcx::PgVec::new_in(run.mcx),
            &types_pathnodes::AggClauseCosts::default(),
            num_distinct_rows,
        )?;
        crate::pathnode::add_path(run, distinct_rel, agg_path);
    }

    Ok(())
}

// get_useful_pathkeys_for_distinct (planner.c).
fn get_useful_pathkeys_for_distinct<'mcx>(
    run: &PlannerRun<'mcx>,
    needed_pathkeys: &mcx::PgVec<'mcx, types_pathnodes::PathKey>,
    path_pathkeys: &[types_pathnodes::PathKey],
) -> mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::PathKey>> {
    let mcx = run.mcx;
    let mut list: mcx::PgVec<'mcx, mcx::PgVec<'mcx, types_pathnodes::PathKey>> =
        mcx::PgVec::new_in(mcx);
    list.push(crate::relnode::pgvec_clone_shallow(mcx, needed_pathkeys));
    if !crate::gucs::enable_distinct_reordering() {
        return list;
    }
    let has_distinct_on = run.parse().hasDistinctOn;
    let mut useful: mcx::PgVec<'mcx, types_pathnodes::PathKey> = mcx::PgVec::new_in(mcx);
    for pk in path_pathkeys {
        if !needed_pathkeys.contains(pk) {
            break;
        }
        // A reordering must keep matching the initial distinctClause pathkeys
        // under DISTINCT ON.
        if has_distinct_on && !run.root.distinct_pathkeys.contains(pk) {
            break;
        }
        useful.push(*pk);
    }
    if useful.is_empty() {
        return list;
    }
    if useful.len() < needed_pathkeys.len() && !crate::gucs::enable_incremental_sort() {
        return list;
    }
    for pk in needed_pathkeys.iter() {
        if !useful.contains(pk) {
            useful.push(*pk);
        }
    }
    if crate::pathkeys::compare_pathkeys(&useful, needed_pathkeys)
        == crate::pathkeys::PathKeysComparison::Equal
    {
        return list;
    }
    list.push(useful);
    list
}

pub fn limit_needed(parse: &types_nodes::parsenodes::Query<'_>) -> bool {
    if let Some(node) = parse.limitCount {
        match node.as_const() {
            // NULL indicates LIMIT ALL, ie, no limit.
            Some(c) => {
                if !c.constisnull {
                    return true;
                }
            }
            None => return true,
        }
    }
    if let Some(node) = parse.limitOffset {
        match node.as_const() {
            Some(c) => {
                if !c.constisnull && c.constvalue.as_i64() != 0 {
                    return true;
                }
            }
            None => return true,
        }
    }
    false
}

fn preprocess_limit<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tuple_fraction: f64,
    offset_est: &mut i64,
    count_est: &mut i64,
) -> PgResult<f64> {
    let mcx = run.mcx;
    let parse = run.parse();
    debug_assert!(parse.limitCount.is_some() || parse.limitOffset.is_some());

    let estimate = |node: types_nodes::Node<'mcx>| -> PgResult<Option<i64>> {
        let est = clauses::estimate_expression_value(mcx, node)?;
        Ok(est.as_const().map(|c| {
            if c.constisnull {
                i64::MIN // NULL sentinel; both callers special-case it
            } else {
                c.constvalue.as_i64()
            }
        }))
    };

    *count_est = match parse.limitCount {
        None => 0,
        Some(node) => match estimate(node)? {
            // NULL indicates LIMIT ALL, ie, no limit.
            Some(i64::MIN) => 0,
            Some(v) => {
                if v <= 0 {
                    1
                } else {
                    v
                }
            }
            None => -1,
        },
    };
    *offset_est = match parse.limitOffset {
        None => 0,
        Some(node) => match estimate(node)? {
            // Treat NULL as no offset; the executor will too.
            Some(i64::MIN) => 0,
            Some(v) => {
                if v < 0 {
                    0
                } else {
                    v
                }
            }
            None => -1,
        },
    };

    let mut tuple_fraction = tuple_fraction;
    if *count_est != 0 {
        let limit_fraction = if *count_est < 0 || *offset_est < 0 {
            0.10
        } else {
            *count_est as f64 + *offset_est as f64
        };
        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction = tuple_fraction.min(limit_fraction);
            }
        } else if tuple_fraction > 0.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction = limit_fraction;
            } else {
                tuple_fraction = tuple_fraction.min(limit_fraction);
            }
        } else {
            tuple_fraction = limit_fraction;
        }
    } else if *offset_est != 0 && tuple_fraction > 0.0 {
        let limit_fraction = if *offset_est < 0 { 0.10 } else { *offset_est as f64 };
        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction += limit_fraction;
            } else {
                tuple_fraction = limit_fraction;
            }
        } else if limit_fraction < 1.0 {
            tuple_fraction += limit_fraction;
            if tuple_fraction >= 1.0 {
                tuple_fraction = 0.0; // assume fetch all
            }
        }
    }
    Ok(tuple_fraction)
}

// make_sort_input_target (planner.c); returns (target, have_postponed_srfs).
fn make_sort_input_target<'mcx>(
    run: &mut PlannerRun<'mcx>,
    final_target: types_pathnodes::PtId,
) -> PgResult<(types_pathnodes::PtId, bool)> {
    let mcx = run.mcx;
    let parse = run.parse();
    debug_assert!(!parse.sortClause.is_nil());
    let mut have_srf = false;
    let mut have_srf_sortcols = false;
    let mut have_volatile = false;
    let mut have_expensive = false;
    let n = run.root.pathtarget(final_target).exprs.len();
    let mut col_is_srf: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    let mut postpone_col: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        let expr = *run.root.expr_node(ft.exprs[i]);
        let mut is_srf = false;
        let mut postpone = false;
        if sgref != 0 {
            if !have_srf_sortcols
                && parse.hasTargetSRFs
                && coerce::expression_returns_set(expr)
            {
                have_srf_sortcols = true;
            }
        } else if parse.hasTargetSRFs && coerce::expression_returns_set(expr) {
            is_srf = true;
            have_srf = true;
        } else if clauses::contain_volatile_functions(expr)? {
            postpone = true;
            have_volatile = true;
        } else {
            let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), expr)?;
            if cost.per_tuple > 10.0 * crate::gucs::cpu_operator_cost() {
                postpone = true;
                have_expensive = true;
            }
        }
        col_is_srf.push(is_srf);
        postpone_col.push(postpone);
    }
    // SRFs are postponable only when none appear in sortgroupref columns.
    let postpone_srfs = have_srf && !have_srf_sortcols;
    if !(postpone_srfs
        || have_volatile
        || (have_expensive && (parse.limitCount.is_some() || run.root.tuple_fraction > 0.0)))
    {
        return Ok((final_target, false));
    }

    let mut input = types_pathnodes::PathTarget::new(mcx);
    let mut postponable = types_nodes::list::NodeList::nil();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        let eid = ft.exprs[i];
        if postpone_col[i] || (postpone_srfs && col_is_srf[i]) {
            postponable.lappend(mcx, *run.root.expr_node(eid))?;
        } else {
            input.exprs.push(eid);
            input.sortgrouprefs.push(sgref);
        }
    }
    let postponable_vars = vars::pull_var_clause(
        mcx,
        types_nodes::Node::mk_list(mcx, postponable)?,
        vars::PVC_INCLUDE_AGGREGATES
            | vars::PVC_INCLUDE_WINDOWFUNCS
            | vars::PVC_INCLUDE_PLACEHOLDERS,
    )?;
    for v in &postponable_vars {
        let dup = input
            .exprs
            .iter()
            .any(|&eid| types_nodes::equal(*run.root.expr_node(eid), v));
        if !dup {
            input.exprs.push(run.intern_expr(v));
            input.sortgrouprefs.push(0);
        }
    }
    if input.sortgrouprefs.iter().all(|&r| r == 0) {
        input.sortgrouprefs.clear();
    }

    // set_pathtarget_cost_width (costsize.c), as create_pathtarget.
    for i in 0..input.exprs.len() {
        let expr = *run.root.expr_node(input.exprs[i]);
        if expr.node_tag() != types_nodes::NodeTag::T_Var {
            let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), expr)?;
            input.cost.startup += cost.startup;
            input.cost.per_tuple += cost.per_tuple;
        }
    }
    let id = run.root.alloc_pathtarget(input);
    let mut tuple_width: i64 = 0;
    for i in 0..run.root.pathtarget(id).exprs.len() {
        let expr = run.root.pathtarget(id).exprs[i];
        tuple_width += crate::costsize::get_expr_width(run, expr)? as i64;
    }
    run.root.pathtarget_mut(id).width = crate::costsize::clamp_width_est(tuple_width);
    Ok((id, postpone_srfs))
}

// Incremental sort and partial paths are loud/absent.
fn create_ordered_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    target: types_pathnodes::PtId,
    target_parallel_safe: bool,
    limit_tuples: f64,
) -> PgResult<RelId> {
    let cheapest_input = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
    let ordered_rel = crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_ORDERED);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (input.serverid, input.userid, input.useridiscurrent, input.fdwroutine,
             input.consider_parallel)
        };
        let o = run.root.rel_mut(ordered_rel);
        o.serverid = serverid;
        o.userid = userid;
        o.useridiscurrent = useridiscurrent;
        o.fdwroutine = has_fdw;
        o.consider_parallel = in_parallel && target_parallel_safe;
    }

    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
    for &input_path in paths.iter() {
        let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
            &run.root.sort_pathkeys,
            &run.root.path(input_path).base().pathkeys,
        );
        let sorted_path = if is_sorted {
            input_path
        } else {
            if input_path != cheapest_input
                && (presorted_keys == 0 || !crate::gucs::enable_incremental_sort())
            {
                continue;
            }
            let sort_pathkeys =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
            if presorted_keys == 0 || !crate::gucs::enable_incremental_sort() {
                crate::pathnode::create_sort_path(
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys,
                    limit_tuples,
                )
            } else {
                crate::pathnode::create_incremental_sort_path(
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys,
                    presorted_keys,
                    limit_tuples,
                )?
            }
        };

        let sorted_target = run
            .root
            .path(sorted_path)
            .base()
            .pathtarget_id
            .expect("sorted path has a pathtarget");
        let sorted_path = if !crate::pathnode::exprs_same(
            run,
            &run.root.pathtarget(sorted_target).exprs,
            &run.root.pathtarget(target).exprs,
        ) {
            crate::pathnode::apply_projection_to_path(run, ordered_rel, sorted_path, target)?
        } else {
            sorted_path
        };
        crate::pathnode::add_path(run, ordered_rel, sorted_path);
    }
    // generate_gather_paths made a plain Gather and order-preserving Gather
    // Merges already; what remains is sorting a partial path (fully or
    // incrementally) and putting a Gather Merge on top.
    // M5-3 (§2.3): suppressed under engine=runtime for covered shapes,
    // like every other Gather/Gather Merge construction site.
    let m5_suppress = crate::m5_suppress::m5_suppress_gather(run)?;
    if run.root.rel(ordered_rel).consider_parallel
        && !run.root.sort_pathkeys.is_empty()
        && !run.root.rel(input_rel).partial_pathlist.is_empty()
        && !m5_suppress
    {
        let cheapest_partial_path = run.root.rel(input_rel).partial_pathlist[0];
        let partials =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).partial_pathlist);
        for &input_path in partials.iter() {
            let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
                &run.root.sort_pathkeys,
                &run.root.path(input_path).base().pathkeys,
            );
            if is_sorted {
                continue;
            }
            if input_path != cheapest_partial_path
                && (presorted_keys == 0 || !crate::gucs::enable_incremental_sort())
            {
                continue;
            }
            let sort_pathkeys =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
            let sorted_path = if presorted_keys == 0 || !crate::gucs::enable_incremental_sort() {
                crate::pathnode::create_sort_path(
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys,
                    limit_tuples,
                )
            } else {
                crate::pathnode::create_incremental_sort_path(
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys,
                    presorted_keys,
                    limit_tuples,
                )?
            };
            let (total_groups, sorted_target) = {
                let sb = run.root.path(sorted_path).base();
                (
                    ::costsize::compute_gather_rows(sb.rows, sb.parallel_workers),
                    sb.pathtarget_id,
                )
            };
            let gm_keys = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
            let sorted_path = crate::pathnode::create_gather_merge_path(
                run,
                ordered_rel,
                sorted_path,
                sorted_target,
                gm_keys,
                Some(total_groups),
            );

            let sorted_target = run
                .root
                .path(sorted_path)
                .base()
                .pathtarget_id
                .expect("gather merge path has a pathtarget");
            let sorted_path = if !crate::pathnode::exprs_same(
                run,
                &run.root.pathtarget(sorted_target).exprs,
                &run.root.pathtarget(target).exprs,
            ) {
                crate::pathnode::apply_projection_to_path(run, ordered_rel, sorted_path, target)?
            } else {
                sorted_path
            };
            crate::pathnode::add_path(run, ordered_rel, sorted_path);
        }
    }

    assert!(!run.root.rel(ordered_rel).pathlist.is_empty(), "failed to generate ORDER BY paths");
    Ok(ordered_rel)
}

// has_volatile_pathkey (planner.c).
fn has_volatile_pathkey(run: &PlannerRun<'_>, keys: &[types_pathnodes::PathKey]) -> bool {
    keys.iter().any(|pk| {
        let ec = pk.pk_eclass.expect("canonical pathkey has an eclass");
        run.root.ec(ec).ec_has_volatile
    })
}

// adjust_group_pathkeys_for_groupagg (planner.c): extend group_pathkeys with
// the pathkeys suiting the largest set of DISTINCT/ORDER BY aggregates, and
// mark those Aggrefs aggpresorted so nodeagg skips its per-group sorts.
fn adjust_group_pathkeys_for_groupagg<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    debug_assert!(run.parse().groupingSets.is_nil());
    debug_assert!(run.root.numOrderedAggs > 0);
    if !crate::gucs::enable_presorted_aggregate() {
        return Ok(());
    }
    let mcx = run.mcx;
    let grouppathkeys = crate::relnode::pgvec_clone_shallow(mcx, &run.root.group_pathkeys);
    let agginfo_ids = crate::relnode::pgvec_clone_shallow(mcx, &run.root.agginfos);
    let naggs = agginfo_ids.len();
    let aggref_of = |run: &PlannerRun<'mcx>, i: usize| -> &'mcx types_nodes::primnodes::Aggref<'mcx> {
        let aggref_id = run.root.agg_info(agginfo_ids[i]).aggrefs[0];
        run.root
            .expr_node(aggref_id)
            .as_aggref()
            .expect("AggInfo.aggrefs holds Aggrefs")
    };

    // C's unprocessed_aggs Bitmapset over agginfo indexes.
    let mut unprocessed: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    unprocessed.resize(naggs, false);
    let mut n_unprocessed = 0usize;
    for i in 0..naggs {
        let aggref = aggref_of(run, i);
        // AGGKIND_IS_ORDERED_SET covers both ordered-set and hypothetical.
        if aggref.aggkind != types_nodes::primnodes::AGGKIND_NORMAL {
            continue;
        }
        if aggref.aggdistinct.is_nil() && aggref.aggorder.is_nil() {
            continue;
        }
        if aggref.aggfilter.is_some() {
            // Presorting evaluates the sort expressions before the FILTER can
            // remove rows; only error-free arguments (Vars/Consts) qualify.
            let mut allow_presort = true;
            for tle_node in &aggref.args {
                let tle = tle_node.as_target_entry().expect("Aggref.args cell");
                let mut expr = tle.expr;
                while let Some(r) = expr.as_relabel_type() {
                    expr = r.arg;
                }
                match expr.node_tag() {
                    types_nodes::NodeTag::T_Var | types_nodes::NodeTag::T_Const => {}
                    _ => {
                        allow_presort = false;
                        break;
                    }
                }
            }
            if !allow_presort {
                continue;
            }
        }
        unprocessed[i] = true;
        n_unprocessed += 1;
    }

    let mut bestpathkeys: mcx::PgVec<'mcx, types_pathnodes::PathKey> = mcx::PgVec::new_in(mcx);
    let mut bestaggs: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    bestaggs.resize(naggs, false);
    let mut n_best = 0usize;
    while n_unprocessed > n_best {
        let mut aggindexes: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
        aggindexes.resize(naggs, false);
        let mut n_agg = 0usize;
        let mut currpathkeys: Option<mcx::PgVec<'mcx, types_pathnodes::PathKey>> = None;
        for i in 0..naggs {
            if !unprocessed[i] {
                continue;
            }
            let aggref = aggref_of(run, i);
            let sortlist = if !aggref.aggdistinct.is_nil() {
                &aggref.aggdistinct
            } else {
                &aggref.aggorder
            };
            let pathkeys =
                crate::pathkeys::make_pathkeys_for_sortclauses(run, sortlist, &aggref.args)?;
            // Aggrefs whose ORDER BY/DISTINCT contains volatile functions
            // always sort on their own (result consistency; planner.c note).
            if has_volatile_pathkey(run, &pathkeys) {
                unprocessed[i] = false;
                n_unprocessed -= 1;
                continue;
            }
            match currpathkeys.as_mut() {
                None => {
                    let cur = if !grouppathkeys.is_empty() {
                        let mut cur =
                            crate::relnode::pgvec_clone_shallow(mcx, &grouppathkeys);
                        crate::pathkeys::append_pathkeys(run, &mut cur, &pathkeys);
                        cur
                    } else {
                        pathkeys
                    };
                    currpathkeys = Some(cur);
                    aggindexes[i] = true;
                    n_agg += 1;
                }
                Some(cur) => {
                    let pathkeys = if !grouppathkeys.is_empty() {
                        let mut pk =
                            crate::relnode::pgvec_clone_shallow(mcx, &grouppathkeys);
                        crate::pathkeys::append_pathkeys(run, &mut pk, &pathkeys);
                        pk
                    } else {
                        pathkeys
                    };
                    match crate::pathkeys::compare_pathkeys(cur, &pathkeys) {
                        crate::pathkeys::PathKeysComparison::Better2 => {
                            *cur = pathkeys;
                            aggindexes[i] = true;
                            n_agg += 1;
                        }
                        crate::pathkeys::PathKeysComparison::Better1
                        | crate::pathkeys::PathKeysComparison::Equal => {
                            aggindexes[i] = true;
                            n_agg += 1;
                        }
                        crate::pathkeys::PathKeysComparison::Different => {}
                    }
                }
            }
        }
        for i in 0..naggs {
            if aggindexes[i] {
                debug_assert!(unprocessed[i]);
                unprocessed[i] = false;
                n_unprocessed -= 1;
            }
        }
        if n_agg > n_best {
            bestaggs = aggindexes;
            n_best = n_agg;
            bestpathkeys = currpathkeys.expect("n_agg > 0 implies pathkeys were chosen");
        }
    }

    // bestpathkeys already includes the original GROUP BY pathkeys.
    if !bestpathkeys.is_empty() {
        run.root.group_pathkeys = bestpathkeys;
    }

    // No Hash Aggregate risk: create_grouping_paths never allows hashing with
    // ordered aggregates, so aggpresorted is honored by AGG_SORTED/AGG_PLAIN.
    for i in 0..naggs {
        if !bestaggs[i] {
            continue;
        }
        let aggref_ids =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.agg_info(agginfo_ids[i]).aggrefs);
        for &aggref_id in aggref_ids.iter() {
            let node = *run.root.expr_node(aggref_id);
            // SAFETY: the planner exclusively owns the sealed parse tree
            // during planning (C scribbles aggpresorted through shared
            // pointers); no reference derived from this node is live here.
            unsafe {
                node.with_mut::<types_nodes::primnodes::Aggref, _>(|a| {
                    a.aggpresorted = true;
                });
            }
        }
    }
    Ok(())
}

// standard_qp_callback (planner.c); qp_extra arrives as run.qp_setop /
// run.active_windows.
fn standard_qp_callback<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let parse = run.parse();
    let tlist = run.processed_tlist();

    if run.gset_data.is_some() {
        // Grouping sets: the first RollupData's groupClause, with C's
        // remove_redundant=false, set_ec_sortref=false.
        let mut clauses = match run.gset_data.as_ref().unwrap().rollups.first() {
            Some(r) => crate::relnode::pgvec_clone_shallow(run.mcx, &r.groupClause),
            None => mcx::PgVec::new_in(run.mcx),
        };
        let has_group_rte = parse.hasGroupRTE;
        if grouping_is_sortable(run, &clauses) {
            let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
                run,
                &mut clauses,
                tlist,
                false,
                has_group_rte,
                false,
            )?;
            assert!(sortable);
            run.root.num_groupby_pathkeys = pathkeys.len() as i32;
            run.root.group_pathkeys = pathkeys;
        } else {
            run.root.group_pathkeys = mcx::PgVec::new_in(run.mcx);
            run.root.num_groupby_pathkeys = 0;
        }
    } else if !parse.groupClause.is_nil() || run.root.numOrderedAggs > 0 {
        let mut clauses = core::mem::replace(
            &mut run.root.processed_groupClause,
            mcx::PgVec::new_in(run.mcx),
        );
        let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
            run, &mut clauses, tlist, true, false, true,
        )?;
        run.root.processed_groupClause = clauses;
        if sortable {
            run.root.num_groupby_pathkeys = pathkeys.len() as i32;
            run.root.group_pathkeys = pathkeys;
            if run.root.numOrderedAggs > 0 {
                adjust_group_pathkeys_for_groupagg(run)?;
            }
        } else {
            // Can't sort; no point in considering aggregate ordering either.
            run.root.group_pathkeys = mcx::PgVec::new_in(run.mcx);
            run.root.num_groupby_pathkeys = 0;
        }
    } else {
        run.root.group_pathkeys = mcx::PgVec::new_in(run.mcx);
        run.root.num_groupby_pathkeys = 0;
    }

    if !parse.distinctClause.is_nil() {
        let mut clauses: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
            mcx::PgVec::new_in(run.mcx);
        for n in &parse.distinctClause {
            clauses.push(run.intern_expr(n));
        }
        let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
            run, &mut clauses, tlist, true, false, false,
        )?;
        run.root.processed_distinctClause = clauses;
        run.root.distinct_pathkeys =
            if sortable { pathkeys } else { mcx::PgVec::new_in(run.mcx) };
    } else {
        run.root.distinct_pathkeys = mcx::PgVec::new_in(run.mcx);
    }

    if !run.active_windows.is_empty() {
        let wc = run.active_windows[0];
        let pk = crate::window::make_pathkeys_for_window(run, wc, tlist)?;
        run.root.window_pathkeys = pk;
    } else {
        run.root.window_pathkeys = mcx::PgVec::new_in(run.mcx);
    }

    run.root.sort_pathkeys =
        crate::pathkeys::make_pathkeys_for_sortclauses(run, &parse.sortClause, tlist)?;

    run.root.setop_pathkeys = mcx::PgVec::new_in(run.mcx);
    if let Some(op) = run.qp_setop {
        let mut group_clauses = crate::prepunion::generate_setop_child_grouplist(run, op, tlist)?;
        if !group_clauses.is_empty() {
            let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
                run,
                &mut group_clauses,
                tlist,
                false,
                false,
                false,
            )?;
            if sortable {
                run.root.setop_pathkeys = pathkeys;
            }
        }
    }

    run.root.query_pathkeys = if !run.root.group_pathkeys.is_empty() {
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.group_pathkeys)
    } else if !run.root.window_pathkeys.is_empty() {
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.window_pathkeys)
    } else if run.root.distinct_pathkeys.len() > run.root.sort_pathkeys.len() {
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.distinct_pathkeys)
    } else if !run.root.sort_pathkeys.is_empty() {
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys)
    } else {
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.setop_pathkeys)
    };
    Ok(())
}

// postprocess_setop_tlist (planner.c): transpose sort-key refs from the parse
// tlist onto flat copies of the setop tlist.
fn postprocess_setop_tlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    new_tlist: &types_nodes::list::NodeList<'mcx>,
    orig_tlist: &types_nodes::list::NodeList<'mcx>,
) -> PgResult<&'mcx types_nodes::list::NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut out = types_nodes::list::NodeList::nil();
    let mut orig = orig_tlist.iter();
    for new_node in new_tlist {
        let new_tle = new_node.as_target_entry().expect("tlist cell");
        debug_assert!(!new_tle.resjunk);
        let orig_tle = orig
            .next()
            .expect("setop tlist longer than parse tlist")
            .as_target_entry()
            .expect("tlist cell");
        assert!(!orig_tle.resjunk, "resjunk output columns are not implemented");
        debug_assert_eq!(new_tle.resno, orig_tle.resno);
        out.lappend(
            mcx,
            types_nodes::Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: new_tle.expr,
                    resno: new_tle.resno,
                    resname: new_tle.resname,
                    ressortgroupref: orig_tle.ressortgroupref,
                    resorigtbl: new_tle.resorigtbl,
                    resorigcol: new_tle.resorigcol,
                    resjunk: new_tle.resjunk,
                },
            )?,
        )?;
    }
    assert!(orig.next().is_none(), "resjunk output columns are not implemented");
    Ok(mcx::leak_in(mcx::alloc_in(mcx, out)?))
}

// Unpartitioned, SRF-free arm.
fn apply_scanjoin_target_to_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    scanjoin_targets: &mcx::PgVec<'mcx, types_pathnodes::PtId>,
    scanjoin_targets_contain_srfs: &mcx::PgVec<'mcx, bool>,
    scanjoin_target_parallel_safe: bool,
    tlist_same_exprs: bool,
) -> PgResult<()> {
    let scanjoin_target = scanjoin_targets[0];
    let rel_is_partitioned = {
        let r = run.root.rel(rel_id);
        r.part_scheme.is_some()
            && r.boundinfo.is_some()
            && r.nparts > 0
            && !r.part_rels.is_empty()
            && !crate::joinrels::is_dummy_rel(&run.root, rel_id)
    };

    // Partitioned rels: drop the whole-rel paths and rebuild from retargeted
    // child paths below (C keeps neither; the below-Append target is never
    // costlier). The main pathlist goes first: the stanza below must still
    // see the old partial paths.
    if rel_is_partitioned {
        run.root.rel_mut(rel_id).pathlist = mcx::PgVec::new_in(run.mcx);
    }

    if !scanjoin_target_parallel_safe {
        // Workers can't compute this target: last chance to use the partial
        // paths, emitting the current reltarget under a Gather.
        crate::allpaths::generate_useful_gather_paths(run, rel_id, false)?;
        run.root.rel_mut(rel_id).partial_pathlist = mcx::PgVec::new_in(run.mcx);
        run.root.rel_mut(rel_id).consider_parallel = false;
    }

    if rel_is_partitioned {
        run.root.rel_mut(rel_id).partial_pathlist = mcx::PgVec::new_in(run.mcx);
    }

    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel_id).pathlist);
    for (i, path_id) in paths.iter().enumerate() {
        debug_assert!(run.root.path(*path_id).base().param_info.is_none());
        if tlist_same_exprs {
            let sortgrouprefs = crate::relnode::pgvec_clone_shallow(
                run.mcx,
                &run.root.pathtarget(scanjoin_target).sortgrouprefs,
            );
            let pt = run.root.path(*path_id).base().pathtarget_id.unwrap();
            run.root.pathtarget_mut(pt).sortgrouprefs = sortgrouprefs;
        } else {
            let newpath = create_projection_path(
                run,
                rel_id,
                *path_id,
                scanjoin_target,
                scanjoin_target_parallel_safe,
            );
            let new_id = run.root.alloc_path(newpath);
            run.root.rel_mut(rel_id).pathlist[i] = new_id;
        }
    }
    let partials = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel_id).partial_pathlist);
    for (i, path_id) in partials.iter().enumerate() {
        debug_assert!(run.root.path(*path_id).base().param_info.is_none());
        if tlist_same_exprs {
            let sortgrouprefs = crate::relnode::pgvec_clone_shallow(
                run.mcx,
                &run.root.pathtarget(scanjoin_target).sortgrouprefs,
            );
            let pt = run.root.path(*path_id).base().pathtarget_id.unwrap();
            run.root.pathtarget_mut(pt).sortgrouprefs = sortgrouprefs;
        } else {
            let newpath = create_projection_path(
                run,
                rel_id,
                *path_id,
                scanjoin_target,
                scanjoin_target_parallel_safe,
            );
            let new_id = run.root.alloc_path(newpath);
            run.root.rel_mut(rel_id).partial_pathlist[i] = new_id;
        }
    }
    crate::srf::adjust_paths_for_srfs(
        run,
        rel_id,
        scanjoin_targets,
        scanjoin_targets_contain_srfs,
    )?;
    run.root.rel_mut(rel_id).pathtarget_id = Some(*scanjoin_targets.last().unwrap());

    if rel_is_partitioned {
        let mut live_children: Vec<types_pathnodes::RelId> = Vec::new();
        let live = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel_id).live_parts);
        for i in crate::relnode::relids_members(&live) {
            let child = run.root.rel(rel_id).part_rels[i as usize]
                .expect("live partition has a RelOptInfo");
            let child_relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(child).relids);
            let appinfos = crate::inherit::find_appinfos_by_relids(run, &child_relids);
            let mut child_targets: mcx::PgVec<'mcx, types_pathnodes::PtId> =
                mcx::PgVec::new_in(run.mcx);
            for &t in scanjoin_targets.iter() {
                let src_exprs =
                    crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(t).exprs);
                let mut exprs: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                    mcx::PgVec::new_in(run.mcx);
                for &eid in src_exprs.iter() {
                    let e = *run.root.expr_node(eid);
                    let tr = crate::inherit::adjust_appendrel_attrs_multi(run, e, &appinfos)?;
                    exprs.push(run.intern_expr(tr));
                }
                let src = run.root.pathtarget(t);
                let copy = types_pathnodes::PathTarget {
                    exprs,
                    sortgrouprefs: crate::relnode::pgvec_clone_shallow(
                        run.mcx,
                        &src.sortgrouprefs,
                    ),
                    cost: src.cost,
                    width: src.width,
                    has_volatile_expr: src.has_volatile_expr,
                };
                child_targets.push(run.root.alloc_pathtarget(copy));
            }
            apply_scanjoin_target_to_paths(
                run,
                child,
                &child_targets,
                scanjoin_targets_contain_srfs,
                scanjoin_target_parallel_safe,
                tlist_same_exprs,
            )?;
            if !crate::joinrels::is_dummy_rel(&run.root, child) {
                live_children.push(child);
            }
        }
        crate::allpaths::add_paths_to_append_rel(run, rel_id, &live_children)?;
    }

    // Gather/Gather Merge over the retargeted partial paths — only for the
    // parallel-safe non-child rel, after all paths and before set_cheapest.
    let is_other_rel = matches!(
        run.root.rel(rel_id).reloptkind,
        types_pathnodes::RELOPT_OTHER_MEMBER_REL
            | types_pathnodes::RELOPT_OTHER_JOINREL
            | types_pathnodes::RELOPT_OTHER_UPPER_REL
    );
    if run.root.rel(rel_id).consider_parallel && !is_other_rel {
        crate::allpaths::generate_useful_gather_paths(run, rel_id, false)?;
    }

    // Reassess the cheapest paths now that costs may have changed.
    crate::pathnode::set_cheapest(run, rel_id)?;
    Ok(())
}
