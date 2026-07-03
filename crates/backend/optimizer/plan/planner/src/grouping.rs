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

pub fn grouping_planner<'mcx>(run: &mut PlannerRun<'mcx>, tuple_fraction: f64) -> PgResult<()> {
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
        panic!("plan_set_operations (prepunion.c): M2 setop lane");
    }
    if !parse.groupingSets.is_nil() {
        panic!("preprocess_grouping_sets (planner.c): M3 grouping-sets lane");
    }
    if !parse.groupClause.is_nil() {
        run.root.processed_groupClause = preprocess_groupclause(run)?;
    }

    preprocess_targetlist(run)?;

    if parse.hasAggs {
        crate::prepagg::preprocess_aggrefs(run, run.processed_tlist())?;
        debug_assert!(parse.havingQual.is_none());
    }
    if parse.hasWindowFuncs {
        panic!("find_window_functions (clauses.c): M2 window lane");
    }
    debug_assert!(!parse.hasTargetSRFs);
    run.root.limit_tuples = if !parse.groupClause.is_nil()
        || !parse.groupingSets.is_nil()
        || !parse.distinctClause.is_nil()
        || parse.hasAggs
        || parse.hasWindowFuncs
        || run.root.hasHavingQual
    {
        -1.0
    } else {
        limit_tuples
    };

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
    if !parse.distinctClause.is_nil() || parse.havingQual.is_some() {
        panic!("create_distinct_paths/HAVING (planner.c): M2 lane");
    }
    // No window functions: grouping_target = sort_input_target.
    let grouping_target = sort_input_target;
    let grouping_target_parallel_safe = sort_input_target_parallel_safe;
    let scanjoin_target = if parse.hasAggs || !parse.groupClause.is_nil() {
        make_group_input_target(run, final_target)?
    } else {
        grouping_target
    };

    let reltarget = run.rel_reltarget_id(current_rel);
    let same_exprs = crate::pathnode::exprs_same(
        run,
        &run.root.pathtarget(scanjoin_target).exprs,
        &run.root.pathtarget(reltarget).exprs,
    );
    apply_scanjoin_target_to_paths(
        run,
        current_rel,
        scanjoin_target,
        final_target_parallel_safe,
        same_exprs,
    )?;

    run.root.upper_targets[UPPERREL_FINAL as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_ORDERED as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_WINDOW as usize] = Some(sort_input_target);
    run.root.upper_targets[UPPERREL_GROUP_AGG as usize] = Some(grouping_target);

    let mut current_rel = if parse.hasAggs || !parse.groupClause.is_nil() {
        let _ = grouping_target_parallel_safe;
        create_grouping_paths(run, current_rel, grouping_target)?
    } else {
        current_rel
    };

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

    debug_assert!(parse.rowMarks.is_nil());
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(current_rel).pathlist);
    for path_id in paths.iter() {
        let mut path_id = *path_id;
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
            debug_assert!(parse.withCheckOptions.is_nil() && parse.returningList.is_nil());
            debug_assert!(parse.onConflict.is_none() && run.root.rowMarks.is_empty());
            let update_colnos = (parse.commandType == CmdType::CMD_UPDATE).then(|| {
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.update_colnos)
            });
            let mtpath = crate::pathnode::create_modifytable_path(
                run,
                final_rel,
                path_id,
                parse.commandType,
                parse.canSetTag,
                parse.resultRelation as u32,
                update_colnos,
            );
            path_id = run.root.alloc_path(mtpath);
        }
        add_existing_path(run, final_rel, path_id);
    }
    // Partial paths, FDW upper paths, create_upper_paths_hook: all absent.
    Ok(())
}

// preprocess_groupclause (planner.c); interned ids share the parse nodes,
// as C.
fn preprocess_groupclause<'mcx>(
    run: &mut PlannerRun<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, types_pathnodes::NodeId>> {
    let mcx = run.mcx;
    let parse = run.parse();
    let mut new_groupclause: mcx::PgVec<'mcx, types_nodes::Node<'mcx>> =
        mcx::PgVec::new_in(mcx);
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
    debug_assert!(run.parse().havingQual.is_none());

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
            debug_assert!(a.aggdirectargs.is_nil() && a.aggfilter.is_none());
            for arg in &a.args {
                pull_agg_input_vars(arg, out);
            }
        }
        NodeTag::T_TargetEntry => {
            pull_agg_input_vars(node.as_target_entry().unwrap().expr, out)
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
        other => panic!("pull_var_clause (var.c): {other:?}; M3 expression lane"),
    }
}

// create_grouping_paths + create_ordinary_grouping_paths +
// add_paths_to_grouping_rel (planner.c). DIVERGENCE: sorted-grouping
// candidates (can_sort arm) are not generated, so plan choice (not results)
// can differ from C where a sorted plan would win.
fn create_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouping_target: types_pathnodes::PtId,
) -> PgResult<RelId> {
    let parse = run.parse();
    debug_assert!(parse.hasAggs || !parse.groupClause.is_nil());
    debug_assert!(parse.groupingSets.is_nil() && parse.havingQual.is_none());

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
        let target_safe = is_parallel_safe_exprs(run, grouping_target)?;
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

    if parse.groupClause.is_nil() {
        let num_groups = 1.0;
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
        for path_id in paths.iter() {
            let agg_path = crate::pathnode::create_agg_path(
                run,
                grouped_rel,
                *path_id,
                grouping_target,
                types_pathnodes::AGG_PLAIN,
                types_pathnodes::AGGSPLIT_SIMPLE,
                mcx::PgVec::new_in(run.mcx),
                mcx::PgVec::new_in(run.mcx),
                &agg_costs,
                num_groups,
            );
            crate::pathnode::add_path(run, grouped_rel, agg_path);
        }
        crate::pathnode::set_cheapest(run, grouped_rel)?;
        return Ok(grouped_rel);
    }

    if !parse.hasAggs {
        panic!("create_group_path (pathnode.c): GROUP BY without aggregates; M3 group lane");
    }
    let can_hash = run.root.numOrderedAggs == 0
        && parse
            .groupClause
            .iter()
            .all(|n| n.as_sort_group_clause().expect("groupClause cell").hashable);
    if !can_hash {
        panic!(
            "add_paths_to_grouping_rel (planner.c): grouping not hashable \
             (AGG_SORTED); M3 sorted-grouping lane"
        );
    }

    let num_groups = get_number_of_groups(run, input_rel)?;
    let cheapest = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
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
        mcx::PgVec::new_in(run.mcx),
        &agg_costs,
        num_groups,
    );
    crate::pathnode::add_path(run, grouped_rel, agg_path);
    crate::pathnode::set_cheapest(run, grouped_rel)?;
    Ok(grouped_rel)
}

// get_number_of_groups (planner.c), plain-GROUP-BY leg.
fn get_number_of_groups<'mcx>(run: &mut PlannerRun<'mcx>, input_rel: RelId) -> PgResult<f64> {
    let path_rows = {
        let cheapest = run.root.rel(input_rel).cheapest_total_path.unwrap();
        run.root.path(cheapest).base().rows
    };
    let mut group_exprs: mcx::PgVec<'mcx, (types_pathnodes::NodeId, types_nodes::Node<'mcx>)> =
        mcx::PgVec::new_in(run.mcx);
    for i in 0..run.root.processed_groupClause.len() {
        let gc_id = run.root.processed_groupClause[i];
        let sgref = run
            .root
            .expr_node(gc_id)
            .as_sort_group_clause()
            .expect("processed_groupClause cell")
            .tleSortGroupRef;
        let tle_node = run
            .processed_tlist()
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == sgref
            })
            .expect("groupClause sortgroupref has a tlist entry");
        let expr = tle_node.as_target_entry().unwrap().expr;
        let id = run.intern_expr(expr);
        group_exprs.push((id, expr));
    }
    crate::selfuncs::estimate_num_groups(run, &group_exprs, path_rows)
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
    debug_assert!(!parse.sortClause.is_nil() && !parse.hasTargetSRFs);
    let mut have_volatile = false;
    let mut have_expensive = false;
    let n = run.root.pathtarget(final_target).exprs.len();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        if sgref != 0 {
            continue;
        }
        let expr = *run.root.expr_node(ft.exprs[i]);
        if clauses::contain_volatile_functions(expr)? {
            have_volatile = true;
        } else {
            let cost = crate::costsize::cost_qual_eval_node(expr)?;
            if cost.per_tuple > 10.0 * crate::gucs::cpu_operator_cost() {
                have_expensive = true;
            }
        }
    }
    if !(have_volatile
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
            if presorted_keys == 0 || !crate::gucs::enable_incremental_sort() {
                let sort_pathkeys =
                    crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
                crate::pathnode::create_sort_path(
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys,
                    limit_tuples,
                )
            } else {
                panic!(
                    "create_incremental_sort_path (pathnode.c): M2 incremental-sort lane"
                );
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

// DIVERGENCE: group_pathkeys are not built (sorted-grouping lane); only
// hashed grouping paths are generated, so input order is never consumed.
fn standard_qp_callback<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let parse = run.parse();
    if run.root.numOrderedAggs > 0 {
        panic!("make_pathkeys_for_sortclauses_extended (pathkeys.c): M3 ordered-agg lane");
    }
    if !parse.distinctClause.is_nil() {
        panic!("make_pathkeys_for_sortclauses (pathkeys.c): M2 distinct lane");
    }
    run.root.num_groupby_pathkeys = 0;
    run.root.sort_pathkeys = crate::pathkeys::make_pathkeys_for_sortclauses(
        run,
        &parse.sortClause,
        run.processed_tlist(),
    )?;
    // group/window/distinct/setop pathkeys are all NIL here.
    run.root.query_pathkeys =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
    Ok(())
}

// Unpartitioned, SRF-free arm.
fn apply_scanjoin_target_to_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    scanjoin_target: types_pathnodes::PtId,
    scanjoin_target_parallel_safe: bool,
    tlist_same_exprs: bool,
) -> PgResult<()> {
    debug_assert!(run.root.rel(rel_id).part_scheme.is_none());

    if !scanjoin_target_parallel_safe {
        // generate_useful_gather_paths is a no-op with no partial paths.
        debug_assert!(run.root.rel(rel_id).partial_pathlist.is_empty());
        run.root.rel_mut(rel_id).consider_parallel = false;
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
    run.root.rel_mut(rel_id).pathtarget_id = Some(scanjoin_target);
    // Reassess the cheapest paths now that costs may have changed.
    crate::pathnode::set_cheapest(run, rel_id)?;
    Ok(())
}
