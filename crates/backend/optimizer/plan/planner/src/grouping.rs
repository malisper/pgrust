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

// Plain-SELECT arm: no setops/limit/grouping/aggs/windows/distinct/sort.
pub fn grouping_planner<'mcx>(run: &mut PlannerRun<'mcx>, tuple_fraction: f64) -> PgResult<()> {
    let parse = run.parse();
    if parse.limitCount.is_some() || parse.limitOffset.is_some() {
        panic!("preprocess_limit (planner.c): LIMIT/OFFSET estimation; M2 limit lane");
    }
    let limit_tuples = -1.0f64;
    run.root.tuple_fraction = tuple_fraction;

    if parse.setOperations.is_some() {
        panic!("plan_set_operations (prepunion.c): M2 setop lane");
    }
    if !parse.groupingSets.is_nil() || !parse.groupClause.is_nil() {
        panic!("preprocess_grouping_sets/preprocess_groupclause (planner.c): M2 grouping lane");
    }

    preprocess_targetlist(run);

    if parse.hasAggs {
        panic!("preprocess_aggrefs (prepagg.c): M2 aggregate lane");
    }
    if parse.hasWindowFuncs {
        panic!("find_window_functions (clauses.c): M2 window lane");
    }
    debug_assert!(!parse.hasTargetSRFs);
    run.root.limit_tuples = limit_tuples;

    let current_rel = query_planner(run, standard_qp_callback)?;

    let final_target = create_pathtarget(run, run.processed_tlist())?;
    let final_target_parallel_safe = is_parallel_safe_exprs(run, final_target)?;

    if !parse.sortClause.is_nil() {
        panic!("make_sort_input_target (planner.c): M2 sort lane");
    }
    if parse.distinctClause.is_nil() && parse.havingQual.is_none() {
    } else {
        panic!("create_distinct_paths/HAVING (planner.c): M2 lane");
    }
    // sort_input_target = grouping_target = scanjoin_target = final_target.
    let scanjoin_target = final_target;

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
    run.root.upper_targets[UPPERREL_DISTINCT as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_WINDOW as usize] = Some(final_target);
    run.root.upper_targets[UPPERREL_GROUP_AGG as usize] = Some(final_target);

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
    if limit_needed(parse) {
        panic!("create_limit_path (pathnode.c): M2 limit lane");
    }
    if parse.commandType != CmdType::CMD_SELECT {
        panic!("create_modifytable_path (pathnode.c): M2 DML lane");
    }
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(current_rel).pathlist);
    for path_id in paths.iter() {
        add_existing_path(run, final_rel, *path_id);
    }
    // Partial paths, FDW upper paths, create_upper_paths_hook: all absent.
    Ok(())
}

// limit_needed: non-Const arms unreachable behind the preprocess_limit panic.
fn limit_needed(parse: &types_nodes::parsenodes::Query<'_>) -> bool {
    debug_assert!(parse.limitCount.is_none() && parse.limitOffset.is_none());
    false
}

// With no group/window/distinct/sort/setop clauses every pathkey list is NIL.
fn standard_qp_callback<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let parse = run.parse();
    if !parse.groupClause.is_nil() || run.root.numOrderedAggs > 0 {
        panic!("make_pathkeys_for_sortclauses_extended (pathkeys.c): M2 grouping lane");
    }
    if !parse.distinctClause.is_nil() || !parse.sortClause.is_nil() {
        panic!("make_pathkeys_for_sortclauses (pathkeys.c): M2 sort lane");
    }
    run.root.num_groupby_pathkeys = 0;
    debug_assert!(run.root.query_pathkeys.is_empty());
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
