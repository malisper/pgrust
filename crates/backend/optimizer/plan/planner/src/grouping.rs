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
        return grouping_planner_tail(
            run,
            current_rel,
            final_target,
            final_target_parallel_safe,
            limit_tuples,
            offset_est,
            count_est,
        );
    }
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

    let (sort_input_target, sort_input_target_parallel_safe) = if !parse.sortClause.is_nil() {
        let t = make_sort_input_target(run, final_target)?;
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
    let scanjoin_target = if have_grouping {
        make_group_input_target(run, final_target)?
    } else {
        grouping_target
    };

    let (scanjoin_targets, scanjoin_targets_contain_srfs) = if parse.hasTargetSRFs {
        if have_grouping || !run.active_windows.is_empty() {
            panic!(
                "grouping_planner (planner.c): targetlist SRFs above grouping/window \
                 levels (split_pathtarget_at_srfs_grouping) unported"
            );
        }
        debug_assert!(scanjoin_target == grouping_target && grouping_target == sort_input_target);
        crate::srf::split_pathtarget_at_srfs(run, scanjoin_target)?
    } else {
        let mut ts = mcx::PgVec::new_in(run.mcx);
        ts.push(scanjoin_target);
        let mut cs = mcx::PgVec::new_in(run.mcx);
        cs.push(false);
        (ts, cs)
    };
    let scanjoin_target = scanjoin_targets[0];
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
        final_target_parallel_safe,
        same_exprs,
    )?;

    run.root.upper_targets[UPPERREL_FINAL as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_ORDERED as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_WINDOW as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_GROUP_AGG as usize] = Some(grouping_target);

    let mut current_rel = if have_grouping {
        let _ = grouping_target_parallel_safe;
        create_grouping_paths(run, current_rel, grouping_target)?
    } else {
        current_rel
    };

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
            limit_tuples,
        )?;
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
            (cur.serverid, cur.userid, cur.useridiscurrent, cur.has_fdwroutine)
        };
        let f = run.root.rel_mut(final_rel);
        f.serverid = serverid;
        f.userid = userid;
        f.useridiscurrent = useridiscurrent;
        f.has_fdwroutine = has_fdw;
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
            if parse.commandType == CmdType::CMD_MERGE {
                panic!("create_modifytable_path (pathnode.c): MERGE; M4 MERGE lane");
            }
            debug_assert!(parse.withCheckOptions.is_nil());
            debug_assert!(run.root.rowMarks.is_empty());
            let onconflict = parse.onConflict.map(|oc| run.root.alloc_expr_node(oc));
            let update_colnos = (parse.commandType == CmdType::CMD_UPDATE).then(|| {
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.update_colnos)
            });
            let returning_list = (!parse.returningList.is_nil()).then(|| {
                let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                    mcx::PgVec::new_in(run.mcx);
                for tle in &parse.returningList {
                    ids.push(run.root.alloc_expr_node(tle));
                }
                ids
            });
            let mtpath = crate::pathnode::create_modifytable_path(
                run,
                final_rel,
                path_id,
                parse.commandType,
                parse.canSetTag,
                parse.resultRelation as u32,
                update_colnos,
                returning_list,
                onconflict,
            );
            path_id = run.root.alloc_path(mtpath);
        }
        add_existing_path(run, final_rel, path_id);
    }
    // Partial paths, FDW upper paths, create_upper_paths_hook: all absent.
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

// get_sortgroupref_tle (tlist.c) over the final pathtarget's sortgrouprefs.
fn target_sgref_in_group_clause(run: &PlannerRun<'_>, sgref: u32) -> bool {
    sgref != 0
        && run
            .parse()
            .groupClause
            .iter()
            .any(|n| n.as_sort_group_clause().expect("groupClause cell").tleSortGroupRef == sgref)
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
    let n = run.root.pathtarget(final_target).exprs.len();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let expr = *run.root.expr_node(ft.exprs[i]);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        if target_sgref_in_group_clause(run, sgref) {
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
            debug_assert!(a.aggdirectargs.is_nil());
            for arg in &a.args {
                pull_agg_input_vars(arg, out);
            }
            if let Some(f) = a.aggfilter {
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

// create_grouping_paths + make_grouping_rel + create_ordinary_grouping_paths
// + add_paths_to_grouping_rel (planner.c), single grouping set.
fn create_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouping_target: types_pathnodes::PtId,
) -> PgResult<RelId> {
    let parse = run.parse();

    let grouped_rel =
        crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_GROUP_AGG);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (
                input.serverid,
                input.userid,
                input.useridiscurrent,
                input.has_fdwroutine,
                input.consider_parallel,
            )
        };
        let target_safe = is_parallel_safe_exprs(run, grouping_target)?
            && is_parallel_safe_opt(run, parse.havingQual)?;
        let g = run.root.rel_mut(grouped_rel);
        g.serverid = serverid;
        g.userid = userid;
        g.useridiscurrent = useridiscurrent;
        g.has_fdwroutine = has_fdw;
        g.consider_parallel = in_parallel && target_safe;
        g.pathtarget_id = Some(grouping_target);
    }

    let mut agg_costs = types_pathnodes::AggClauseCosts::default();
    crate::prepagg::get_agg_clause_costs(
        run,
        types_pathnodes::AGGSPLIT_SIMPLE,
        &mut agg_costs,
    )?;
    debug_assert!(run.root.rel(input_rel).partial_pathlist.is_empty());

    // is_degenerate_grouping: HAVING with no aggs and no GROUP BY.
    if (run.root.hasHavingQual || !parse.groupingSets.is_nil())
        && !parse.hasAggs
        && parse.groupClause.is_nil()
    {
        panic!(
            "create_degenerate_grouping_paths (planner.c): HAVING without \
             aggregates/GROUP BY (GroupResultPath quals); M3 lane"
        );
    }

    let can_sort = run.gset_data.as_ref().is_some_and(|gd| !gd.rollups.is_empty())
        || grouping_is_sortable(run, &run.root.processed_groupClause);
    let can_hash = !parse.groupClause.is_nil()
        && run.root.numOrderedAggs == 0
        && match &run.gset_data {
            Some(gd) => gd.any_hashable,
            None => grouping_is_hashable(run, &run.root.processed_groupClause),
        };

    let num_groups = get_number_of_groups(run, input_rel)?;
    let cheapest = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
    let having_qual: mcx::PgVec<'mcx, types_pathnodes::NodeId> = {
        let mut v = mcx::PgVec::new_in(run.mcx);
        if let Some(h) = parse.havingQual {
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
                        &agg_costs,
                        &having_qual,
                        num_groups,
                    )?;
                    continue;
                }
                if !parse.hasAggs {
                    panic!(
                        "create_group_path (pathnode.c): GROUP BY without aggregates \
                         (nodeGroup); M3 group lane"
                    );
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
                    &agg_costs,
                    num_groups,
                )?;
                crate::pathnode::add_path(run, grouped_rel, agg_path);
            }
        }
    }

    if can_hash {
        if !parse.groupingSets.is_nil() {
            // C's hash-only consider_groupingsets_paths(is_sorted=false) arm.
            if crate::gucs::enable_hashagg() {
                panic!(
                    "consider_groupingsets_paths (planner.c): hashed/AGG_MIXED grouping-sets \
                     strategy unported — set enable_hashagg=off; grouping-sets lane"
                );
            }
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
                &agg_costs,
                num_groups,
            )?;
            crate::pathnode::add_path(run, grouped_rel, agg_path);
        }
    }

    if run.root.rel(grouped_rel).pathlist.is_empty() {
        return Err(could_not_implement("GROUP BY"));
    }
    crate::pathnode::set_cheapest(run, grouped_rel)?;
    Ok(grouped_rel)
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

// get_sortgrouplist_exprs (tlist.c) into estimate_num_groups' input shape.
pub(crate) fn sortgrouplist_exprs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[types_pathnodes::NodeId],
    tlist: &types_nodes::list::NodeList<'mcx>,
) -> mcx::PgVec<'mcx, (types_pathnodes::NodeId, types_nodes::Node<'mcx>)> {
    let mut exprs = mcx::PgVec::new_in(run.mcx);
    for &gc_id in clauses {
        let sgref = run
            .root
            .expr_node(gc_id)
            .as_sort_group_clause()
            .expect("sortgroup clause cell")
            .tleSortGroupRef;
        let tle_node = tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sgref)
            .expect("sortgroupref has a tlist entry");
        let expr = tle_node.as_target_entry().unwrap().expr;
        let id = run.intern_expr(expr);
        exprs.push((id, expr));
    }
    exprs
}

// get_number_of_groups (planner.c); the hash_sets leg needs the unported
// hashed strategy and stays loud.
fn get_number_of_groups<'mcx>(run: &mut PlannerRun<'mcx>, input_rel: RelId) -> PgResult<f64> {
    let parse = run.parse();
    let path_rows = {
        let cheapest = run.root.rel(input_rel).cheapest_total_path.unwrap();
        run.root.path(cheapest).base().rows
    };
    if !parse.groupClause.is_nil() {
        if !parse.groupingSets.is_nil() {
            let mut gd = run.gset_data.take().expect("grouping sets preprocessed");
            if !gd.unsortable_sets.is_empty() {
                panic!(
                    "get_number_of_groups (planner.c): unsortable grouping sets need the \
                     hashed strategy (unported); grouping-sets lane"
                );
            }
            let tlist = run.processed_tlist();
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
            run.gset_data = Some(gd);
            return Ok(dnum_groups);
        }
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
        let tlist = run.processed_tlist();
        let group_exprs = sortgrouplist_exprs(run, &clauses, tlist);
        return crate::selfuncs::estimate_num_groups(run, &group_exprs, path_rows);
    }
    if !parse.groupingSets.is_nil() {
        return Ok(parse.groupingSets.len() as f64);
    }
    Ok(1.0)
}

// create_distinct_paths + create_final_distinct_paths (planner.c); partial
// paths are always empty so create_partial_distinct_paths is a no-op.
fn create_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    target: types_pathnodes::PtId,
) -> PgResult<RelId> {
    let parse = run.parse();
    assert!(!parse.hasDistinctOn, "DISTINCT ON is loud upstream");

    let distinct_rel = crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_DISTINCT);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (
                input.serverid,
                input.userid,
                input.useridiscurrent,
                input.has_fdwroutine,
                input.consider_parallel,
            )
        };
        let d = run.root.rel_mut(distinct_rel);
        d.serverid = serverid;
        d.userid = userid;
        d.useridiscurrent = useridiscurrent;
        d.has_fdwroutine = has_fdw;
        d.consider_parallel = in_parallel;
        d.pathtarget_id = Some(target);
    }

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
        let needed_pathkeys =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.distinct_pathkeys);
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
                    panic!(
                        "create_final_distinct_paths (planner.c): all-redundant \
                         DISTINCT keys (LIMIT 1 uniqification); M2 const-EC lane"
                    );
                }
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

    let allow_hash = if run.root.rel(distinct_rel).pathlist.is_empty() {
        true
    } else {
        crate::gucs::enable_hashagg()
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

    if run.root.rel(distinct_rel).pathlist.is_empty() {
        return Err(could_not_implement("DISTINCT"));
    }
    crate::pathnode::set_cheapest(run, distinct_rel)?;
    Ok(distinct_rel)
}

// get_useful_pathkeys_for_distinct (planner.c), hasDistinctOn loud upstream.
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
    let mut useful: mcx::PgVec<'mcx, types_pathnodes::PathKey> = mcx::PgVec::new_in(mcx);
    for pk in path_pathkeys {
        if !needed_pathkeys.contains(pk) {
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

// The no-postponable-columns arm returns final_target; the projection-
// building arm is loud.
fn make_sort_input_target<'mcx>(
    run: &mut PlannerRun<'mcx>,
    final_target: types_pathnodes::PtId,
) -> PgResult<types_pathnodes::PtId> {
    let parse = run.parse();
    debug_assert!(!parse.sortClause.is_nil());
    let mut have_srf = false;
    let mut have_srf_sortcols = false;
    let mut have_volatile = false;
    let mut have_expensive = false;
    let n = run.root.pathtarget(final_target).exprs.len();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        let expr = *run.root.expr_node(ft.exprs[i]);
        if sgref != 0 {
            if !have_srf_sortcols
                && parse.hasTargetSRFs
                && coerce::expression_returns_set(expr)
            {
                have_srf_sortcols = true;
            }
            continue;
        }
        if parse.hasTargetSRFs && coerce::expression_returns_set(expr) {
            have_srf = true;
        } else if clauses::contain_volatile_functions(expr)? {
            have_volatile = true;
        } else {
            let cost = crate::costsize::cost_qual_eval_node(expr)?;
            if cost.per_tuple > 10.0 * crate::gucs::cpu_operator_cost() {
                have_expensive = true;
            }
        }
    }
    let postpone_srfs = have_srf && !have_srf_sortcols;
    if !(postpone_srfs
        || have_volatile
        || (have_expensive && (parse.limitCount.is_some() || run.root.tuple_fraction > 0.0)))
    {
        return Ok(final_target);
    }
    panic!("make_sort_input_target (planner.c): postponed-column projection; M2 sort lane");
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
            (input.serverid, input.userid, input.useridiscurrent, input.has_fdwroutine,
             input.consider_parallel)
        };
        let o = run.root.rel_mut(ordered_rel);
        o.serverid = serverid;
        o.userid = userid;
        o.useridiscurrent = useridiscurrent;
        o.has_fdwroutine = has_fdw;
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
        if !crate::pathnode::exprs_same(
            run,
            &run.root.pathtarget(sorted_target).exprs,
            &run.root.pathtarget(target).exprs,
        ) {
            panic!("apply_projection_to_path (pathnode.c): post-sort projection; M2 sort lane");
        }
        crate::pathnode::add_path(run, ordered_rel, sorted_path);
    }
    debug_assert!(run.root.rel(input_rel).partial_pathlist.is_empty());
    assert!(!run.root.rel(ordered_rel).pathlist.is_empty(), "failed to generate ORDER BY paths");
    Ok(ordered_rel)
}

// standard_qp_callback (planner.c); qp_extra arrives as run.qp_setop /
// run.active_windows.
fn standard_qp_callback<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let parse = run.parse();
    let tlist = run.processed_tlist();
    // DIVERGENCE: adjust_group_pathkeys_for_groupagg (planner.c) unported —
    // aggpresorted never set, matching C under enable_presorted_aggregate=off;
    // ordered aggs sort inside nodeagg (presorted-aggregate lane).

    if run.gset_data.is_some() {
        // Grouping sets: the first RollupData's groupClause, with C's
        // remove_redundant=false, set_ec_sortref=false (hasGroupRTE is always
        // false on this port, so remove_group_rtindex is dead).
        let mut clauses = match run.gset_data.as_ref().unwrap().rollups.first() {
            Some(r) => crate::relnode::pgvec_clone_shallow(run.mcx, &r.groupClause),
            None => mcx::PgVec::new_in(run.mcx),
        };
        if grouping_is_sortable(run, &clauses) {
            let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
                run, &mut clauses, tlist, false, false,
            )?;
            assert!(sortable);
            run.root.num_groupby_pathkeys = pathkeys.len() as i32;
            run.root.group_pathkeys = pathkeys;
        } else {
            run.root.group_pathkeys = mcx::PgVec::new_in(run.mcx);
            run.root.num_groupby_pathkeys = 0;
        }
    } else if !parse.groupClause.is_nil() {
        let mut clauses = core::mem::replace(
            &mut run.root.processed_groupClause,
            mcx::PgVec::new_in(run.mcx),
        );
        let (pathkeys, sortable) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
            run, &mut clauses, tlist, true, true,
        )?;
        run.root.processed_groupClause = clauses;
        if sortable {
            run.root.num_groupby_pathkeys = pathkeys.len() as i32;
            run.root.group_pathkeys = pathkeys;
        } else {
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
            run, &mut clauses, tlist, true, false,
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

    if !scanjoin_target_parallel_safe {
        // generate_useful_gather_paths is a no-op with no partial paths.
        debug_assert!(run.root.rel(rel_id).partial_pathlist.is_empty());
        run.root.rel_mut(rel_id).consider_parallel = false;
    }

    // Partitioned rels: drop the whole-rel paths and rebuild from retargeted
    // child paths below (C keeps neither; the below-Append target is never
    // costlier).
    if rel_is_partitioned {
        run.root.rel_mut(rel_id).pathlist = mcx::PgVec::new_in(run.mcx);
        debug_assert!(run.root.rel(rel_id).partial_pathlist.is_empty());
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
    debug_assert!(run.root.rel(rel_id).partial_pathlist.is_empty());
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
            let child_rti = run.root.rel(child).relid as usize;
            let appinfo = run.root.append_rel_array[child_rti]
                .clone()
                .expect("child rel has an AppendRelInfo");
            let mut child_targets: mcx::PgVec<'mcx, types_pathnodes::PtId> =
                mcx::PgVec::new_in(run.mcx);
            for &t in scanjoin_targets.iter() {
                let src_exprs =
                    crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(t).exprs);
                let mut exprs: mcx::PgVec<'mcx, types_pathnodes::NodeId> =
                    mcx::PgVec::new_in(run.mcx);
                for &eid in src_exprs.iter() {
                    let e = *run.root.expr_node(eid);
                    let tr = crate::inherit::adjust_appendrel_attrs(run, e, &appinfo)?;
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

    // Reassess the cheapest paths now that costs may have changed.
    crate::pathnode::set_cheapest(run, rel_id)?;
    Ok(())
}
