//! joinpath.c nestloop + mergejoin + hashjoin arms with their pathnode.c/
//! costsize.c join-cost slices. Parameterized paths are loud.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
use types_pathnodes::{
    HashPath, JoinPath, MaterialPath, MergePath, MergeScanSelCache, NestPath, Path, PathId,
    PathKey, RelId, RinfoId, SpecialJoinInfo, JOIN_INNER,
};

use crate::gucs;
use crate::pathkeys::{
    build_join_pathkeys, compare_pathkeys, find_mergeclauses_for_outer_pathkeys,
    get_cheapest_path_for_pathkeys, make_inner_pathkeys_for_merge, pathkeys_contained_in,
    pathkeys_count_contained_in, select_outer_pathkeys_for_merge,
    trim_mergeclauses_for_inner_pathkeys, update_mergeclause_eclasses, PathKeysComparison,
};
use crate::pathnode::{compare_path_costs, tag16, CostSelector};
use crate::run::PlannerRun;

#[derive(Default)]
struct JoinCostWorkspace {
    startup_cost: f64,
    total_cost: f64,
    run_cost: f64,
    disabled_nodes: i32,
    // hashjoin-only (ExecChooseHashTableSize outputs)
    numbuckets: i32,
    numbatches: i32,
    inner_rows_total: f64,
    // mergejoin-only (initial_cost_mergejoin outputs)
    inner_run_cost: f64,
    outer_rows: f64,
    inner_rows: f64,
    outer_skip_rows: f64,
    inner_skip_rows: f64,
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
    assert!(
        jointype == JOIN_INNER,
        "add_paths_to_joinrel (joinpath.c): jointype {jointype}; M2 outer/semi-join lane"
    );
    let inner_unique = innerrel_is_unique(run, outerrel, innerrel, restrictlist);
    debug_assert!(run.root.join_info_list.is_empty());
    // mergejoin_allowed is always true for inner joins.
    let mergeclause_list = select_mergejoin_clauses(run, outerrel, innerrel, restrictlist)?;
    if gucs::enable_mergejoin() {
        sort_inner_and_outer(
            run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist,
            &mergeclause_list,
        )?;
    }
    match_unsorted_outer(run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist, &mergeclause_list)?;
    hash_inner_and_outer(run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist)
}

// select_mergejoin_clauses (joinpath.c), inner-join arm: pushed-down clauses
// are usable and mergejoin_allowed is unconditionally true.
fn select_mergejoin_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    outerrel: RelId,
    innerrel: RelId,
    restrictlist: &[RinfoId],
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    for &rid in restrictlist {
        {
            let ri = run.root.rinfo(rid);
            if !ri.can_join || ri.mergeopfamilies.is_empty() {
                continue;
            }
        }
        if !clause_sides_match_join(run, rid, outerrel, innerrel) {
            continue;
        }
        if !run.root.rinfo(rid).outer_is_left {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let opno = clause.as_op_expr().expect("mergeclause is an OpExpr").opno;
            if lsyscache::get_commutator(opno)? == 0 {
                continue;
            }
        }
        update_mergeclause_eclasses(run, rid)?;
        // EC_MUST_BE_REDUNDANT: eclass-lite ECs never carry consts.
        debug_assert!(!run.root.ec(run.root.rinfo(rid).left_ec.unwrap()).ec_has_const);
        debug_assert!(!run.root.ec(run.root.rinfo(rid).right_ec.unwrap()).ec_has_const);
        result.push(rid);
    }
    Ok(result)
}

// sort_inner_and_outer (joinpath.c); the unique-ify and partial legs are loud
// or dead upstream.
#[allow(clippy::too_many_arguments)]
fn sort_inner_and_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
) -> PgResult<()> {
    if mergeclause_list.is_empty() {
        return Ok(());
    }
    let outer_path = run
        .root
        .rel(outerrel)
        .cheapest_total_path
        .expect("outer rel has a cheapest path");
    let inner_path = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest path");
    debug_assert!(run.root.path(outer_path).base().param_info.is_none());
    debug_assert!(run.root.path(inner_path).base().param_info.is_none());
    debug_assert!(run.root.rel(outerrel).partial_pathlist.is_empty());

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
        )?;
    }
    Ok(())
}

// generate_mergejoin_paths (joinpath.c); useallclauses is false (inner join)
// and partial paths are dead.
#[allow(clippy::too_many_arguments)]
fn generate_mergejoin_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    innerrel: RelId,
    outerpath: PathId,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
    inner_cheapest_total: PathId,
    merge_pathkeys: &[PathKey],
) -> PgResult<()> {
    let mcx = run.mcx;
    let outer_pathkeys =
        crate::relnode::pgvec_clone_shallow(mcx, &run.root.path(outerpath).base().pathkeys);
    let mergeclauses =
        find_mergeclauses_for_outer_pathkeys(run, &outer_pathkeys, mergeclause_list)?;
    if mergeclauses.is_empty() {
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
    )?;

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
            CostSelector::Total,
            false,
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
                )?;
                cheapest_total_inner = Some(ip);
            }
        }

        let innerpath = get_cheapest_path_for_pathkeys(
            run,
            &inner_pathlist,
            trialsortkeys,
            CostSelector::Startup,
            false,
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
                    )?;
                }
                cheapest_startup_inner = Some(ip);
            }
        }
    }
    Ok(())
}

// innerrel_is_unique -> rel_is_distinct_for -> relation_has_unique_index_for
// (analyzejoins.c/indxpath.c): a PROVEN unique inner routes to the loud lane
// (inner_unique costing/exec semantics are the M2 join-uniqueness lane); a
// non-matching unique index correctly proves nothing. The unique_for_rels
// cache is skipped (per-join-level, cold).
fn innerrel_is_unique(
    run: &mut PlannerRun<'_>,
    outerrel: RelId,
    innerrel: RelId,
    restrictlist: &[RinfoId],
) -> bool {
    if restrictlist.is_empty() {
        return false;
    }
    // rel_supports_distinctness over the innerrel's indexlist.
    {
        let rel = run.root.rel(innerrel);
        if rel.reloptkind != types_pathnodes::RELOPT_BASEREL
            || rel.rtekind != types_pathnodes::RTE_RELATION
        {
            return false;
        }
        if !rel
            .indexlist
            .iter()
            .any(|ind| ind.unique && ind.immediate && ind.indpred.is_empty())
        {
            return false;
        }
    }

    let mut clause_list: PgVec<'_, RinfoId> = PgVec::new_in(run.mcx);
    for &rid in restrictlist {
        if !clause_sides_match_join(run, rid, outerrel, innerrel) {
            continue;
        }
        clause_list.push(rid);
    }

    let inner_relid = run.root.rel(innerrel).relid;
    let n_indexes = run.root.rel(innerrel).indexlist.len();
    for i in 0..n_indexes {
        let ind = std::rc::Rc::clone(&run.root.rel(innerrel).indexlist[i]);
        if !ind.unique || !ind.immediate || !ind.indpred.is_empty() {
            continue;
        }
        let mut all_matched = true;
        for c in 0..ind.nkeycolumns as usize {
            let mut matched = false;
            for &rid in clause_list.iter() {
                let ri = run.root.rinfo(rid);
                if !ri.mergeopfamilies.iter().any(|&f| f == ind.opfamily[c]) {
                    continue;
                }
                let clause = *run.root.expr_node(ri.clause);
                let o = clause.as_op_expr().expect("mergeclause is an OpExpr");
                let mut rexpr = if ri.outer_is_left { o.args.nth(1) } else { o.args.nth(0) };
                while let Some(r) = rexpr.as_relabel_type() {
                    rexpr = r.arg;
                }
                // match_index_to_operand, simple-column arm (expression
                // columns never match a Var).
                if let Some(var) = rexpr.as_var() {
                    if var.varno as u32 == inner_relid
                        && var.varattno != 0
                        && ind.indexkeys[c] == var.varattno as i32
                    {
                        matched = true;
                        break;
                    }
                }
            }
            if !matched {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            panic!(
                "innerrel_is_unique (analyzejoins.c): unique-index proof; M2 join-uniqueness lane"
            );
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn match_unsorted_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
    mergeclause_list: &[RinfoId],
) -> PgResult<()> {
    let inner_cheapest_total = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest path");
    debug_assert!(run.root.path(inner_cheapest_total).base().param_info.is_none());

    let matpath = if gucs::enable_material()
        && !exec_materializes_output(run.root.path(inner_cheapest_total).base().pathtype)
    {
        Some(create_material_path(run, innerrel, inner_cheapest_total))
    } else {
        None
    };

    let outer_paths =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).pathlist);
    for &outerpath in outer_paths.iter() {
        debug_assert!(run.root.path(outerpath).base().param_info.is_none());
        let outer_pathkeys = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.path(outerpath).base().pathkeys,
        );
        let merge_pathkeys = build_join_pathkeys(run, joinrel, jointype, &outer_pathkeys)?;
        let inner_candidates = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(innerrel).cheapest_parameterized_paths,
        );
        for &innerpath in inner_candidates.iter() {
            try_nestloop_path(run, joinrel, outerpath, innerpath, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist)?;
        }
        if let Some(mp) = matpath {
            try_nestloop_path(run, joinrel, outerpath, mp, &merge_pathkeys, jointype, inner_unique, sjinfo, restrictlist)?;
        }
        if !mergeclause_list.is_empty() && gucs::enable_mergejoin() {
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
                inner_cheapest_total,
                &merge_pathkeys,
            )?;
        }
    }
    debug_assert!(run.root.rel(outerrel).partial_pathlist.is_empty());
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
) -> PgResult<()> {
    debug_assert!(sjinfo.ojrelid == 0);
    assert!(
        run.root.path(outer_path).base().param_info.is_none()
            && run.root.path(inner_path).base().param_info.is_none(),
        "try_nestloop_path (joinpath.c): parameterized input; M2 param-path lane"
    );

    let workspace = initial_cost_nestloop(run, jointype, inner_unique, outer_path, inner_path);

    if add_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.startup_cost, workspace.total_cost, pathkeys) {
        let path = create_nestloop_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            outer_path,
            inner_path,
            pathkeys,
            restrictlist,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// try_mergejoin_path (joinpath.c); required_outer/partial legs are dead.
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
) -> PgResult<()> {
    debug_assert!(sjinfo.ojrelid == 0);
    assert!(
        run.root.path(outer_path).base().param_info.is_none()
            && run.root.path(inner_path).base().param_info.is_none(),
        "try_mergejoin_path (joinpath.c): parameterized input; M2 param-path lane"
    );

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

    if add_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.startup_cost, workspace.total_cost, &pathkeys) {
        let path = create_mergejoin_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            outer_path,
            inner_path,
            restrictlist,
            pathkeys,
            mergeclauses,
            outersortkeys,
            innersortkeys,
            outer_presorted_keys,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// add_path_precheck (pathnode.c); required_outer is NULL on every path this
// lane can build.
fn add_path_precheck(
    run: &PlannerRun<'_>,
    joinrel: RelId,
    disabled_nodes: i32,
    startup_cost: f64,
    total_cost: f64,
    pathkeys: &[PathKey],
) -> bool {
    const STD_FUZZ_FACTOR: f64 = 1.01;
    let consider_startup = run.root.rel(joinrel).consider_startup;
    for &old_id in run.root.rel(joinrel).pathlist.iter() {
        let old = run.root.path(old_id).base();
        if old.disabled_nodes != disabled_nodes {
            if disabled_nodes < old.disabled_nodes {
                break;
            }
        } else if total_cost <= old.total_cost * STD_FUZZ_FACTOR {
            break;
        }
        if startup_cost > old.startup_cost * STD_FUZZ_FACTOR || !consider_startup {
            debug_assert!(old.param_info.is_none());
            let keyscmp = compare_pathkeys(pathkeys, &old.pathkeys);
            if keyscmp == PathKeysComparison::Equal || keyscmp == PathKeysComparison::Better2 {
                return false;
            }
        }
    }
    true
}

pub fn create_material_path(run: &mut PlannerRun<'_>, rel: RelId, subpath: PathId) -> PathId {
    let sub = run.root.path(subpath).base();
    debug_assert!(sub.parent == rel);
    let (sub_disabled, sub_startup, sub_total, sub_rows) =
        (sub.disabled_nodes, sub.startup_cost, sub.total_cost, sub.rows);
    let sub_parallel_safe = sub.parallel_safe;
    let sub_parallel_workers = sub.parallel_workers;
    debug_assert!(sub.pathkeys.is_empty() && sub.param_info.is_none());
    let width = run.root.path_pathtarget(subpath).width;

    let startup_cost = sub_startup;
    let mut run_cost = sub_total - sub_startup;
    run_cost += 2.0 * gucs::cpu_operator_cost() * sub_rows;
    let nbytes = crate::costsize::relation_byte_size(sub_rows, width);
    let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
    if nbytes > work_mem_bytes {
        let npages = (nbytes / 8192.0).ceil();
        run_cost += gucs::seq_page_cost() * npages;
    }

    let path = Path {
        type_: tag16(NodeTag::T_MaterialPath),
        pathtype: tag16(NodeTag::T_Material),
        parent: rel,
        pathtarget_id: run.root.rel(rel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe: run.root.rel(rel).consider_parallel && sub_parallel_safe,
        parallel_workers: sub_parallel_workers,
        rows: sub_rows,
        disabled_nodes: sub_disabled + if gucs::enable_material() { 0 } else { 1 },
        startup_cost,
        total_cost: startup_cost + run_cost,
        pathkeys: PgVec::new_in(run.mcx),
    };
    run.root
        .alloc_path(types_pathnodes::PathNode::MaterialPath(MaterialPath {
            path,
            subpath: Some(subpath),
        }))
}

fn cost_rescan(run: &PlannerRun<'_>, path: PathId) -> (f64, f64) {
    let p = run.root.path(path).base();
    let pathtype = p.pathtype;
    if pathtype == tag16(NodeTag::T_Material) || pathtype == tag16(NodeTag::T_Sort) {
        let mut run_cost = gucs::cpu_operator_cost() * p.rows;
        let width = run.root.path_pathtarget(path).width;
        let nbytes = crate::costsize::relation_byte_size(p.rows, width);
        let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
        if nbytes > work_mem_bytes {
            let npages = (nbytes / 8192.0).ceil();
            run_cost += gucs::seq_page_cost() * npages;
        }
        (0.0, run_cost)
    } else if pathtype == tag16(NodeTag::T_FunctionScan)
        || pathtype == tag16(NodeTag::T_HashJoin)
        || pathtype == tag16(NodeTag::T_CteScan)
        || pathtype == tag16(NodeTag::T_WorkTableScan)
        || pathtype == tag16(NodeTag::T_Memoize)
    {
        panic!("cost_rescan (costsize.c): pathtype {pathtype}; M2 lane");
    } else {
        (p.startup_cost, p.total_cost)
    }
}

fn initial_cost_nestloop(
    run: &PlannerRun<'_>,
    jointype: u32,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
) -> JoinCostWorkspace {
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let mut disabled_nodes = if gucs::enable_nestloop() { 0 } else { 1 };
    disabled_nodes += inner.disabled_nodes + outer.disabled_nodes;

    let (inner_rescan_start, inner_rescan_total) = cost_rescan(run, inner_path);

    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;
    let outer_path_rows = outer.rows;
    startup_cost += outer.startup_cost + inner.startup_cost;
    run_cost += outer.total_cost - outer.startup_cost;
    if outer_path_rows > 1.0 {
        run_cost += (outer_path_rows - 1.0) * inner_rescan_start;
    }
    let inner_run_cost = inner.total_cost - inner.startup_cost;
    let inner_rescan_run_cost = inner_rescan_total - inner_rescan_start;

    debug_assert!(jointype == JOIN_INNER && !inner_unique);
    run_cost += inner_run_cost;
    if outer_path_rows > 1.0 {
        run_cost += (outer_path_rows - 1.0) * inner_rescan_run_cost;
    }

    JoinCostWorkspace {
        startup_cost,
        total_cost: startup_cost + run_cost,
        run_cost,
        disabled_nodes,
        numbatches: 1,
        ..Default::default()
    }
}

fn final_cost_nestloop(
    run: &mut PlannerRun<'_>,
    path: &mut NestPath<'_>,
    workspace: &JoinCostWorkspace,
) -> PgResult<()> {
    let outer = run.root.path(path.jpath.outerjoinpath.unwrap()).base();
    let inner = run.root.path(path.jpath.innerjoinpath.unwrap()).base();
    let outer_path_rows = if outer.rows <= 0.0 { 1.0 } else { outer.rows };
    let inner_path_rows = if inner.rows <= 0.0 { 1.0 } else { inner.rows };
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    debug_assert!(path.jpath.path.param_info.is_none());
    path.jpath.path.rows = run.root.rel(path.jpath.path.parent).rows;
    debug_assert!(path.jpath.path.parallel_workers == 0);
    debug_assert!(path.jpath.jointype == JOIN_INNER && !path.jpath.inner_unique);

    let ntuples = outer_path_rows * inner_path_rows;

    let quals = crate::relnode::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let restrict_qual_cost = crate::costsize::cost_qual_eval(run, &quals)?;
    startup_cost += restrict_qual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + restrict_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path.jpath.path.rows;

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

// create_nestloop_path (pathnode.c); required_outer is always NULL here, so
// the moved-clause and parampathinfo legs are dead.
#[allow(clippy::too_many_arguments)]
fn create_nestloop_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    restrict_clauses: &[RinfoId],
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let mut joinrestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    joinrestrictinfo.extend(restrict_clauses.iter().copied());
    let mut pks: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    pks.extend(pathkeys.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_NestPath),
        pathtype: tag16(NodeTag::T_NestLoop),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: pks,
    };
    let mut node = NestPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
    };
    final_cost_nestloop(run, &mut node, workspace)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::NestPath(node)))
}

// hash_inner_and_outer (joinpath.c), JOIN_INNER non-parallel arm. param_info is
// always None on this lane, so PATH_PARAM_BY_REL never skips and the
// cheapest-parameterized loops fold to the cheapest total paths.
#[allow(clippy::too_many_arguments)]
fn hash_inner_and_outer<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: u32,
    inner_unique: bool,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    debug_assert!(jointype == JOIN_INNER);
    let mut hashclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    for &ri in restrictlist {
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
    let cheapest_total_inner = run
        .root
        .rel(innerrel)
        .cheapest_total_path
        .expect("inner rel has a cheapest total path");

    if let Some(cso) = cheapest_startup_outer {
        try_hashjoin_path(
            run, joinrel, cso, cheapest_total_inner, &hashclauses, jointype, inner_unique, sjinfo,
            restrictlist,
        )?;
    }

    let outer_params =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(outerrel).cheapest_parameterized_paths);
    let inner_params =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(innerrel).cheapest_parameterized_paths);
    for &op in outer_params.iter() {
        debug_assert!(run.root.path(op).base().param_info.is_none());
        for &ip in inner_params.iter() {
            debug_assert!(run.root.path(ip).base().param_info.is_none());
            if Some(op) == cheapest_startup_outer && ip == cheapest_total_inner {
                continue;
            }
            try_hashjoin_path(
                run, joinrel, op, ip, &hashclauses, jointype, inner_unique, sjinfo, restrictlist,
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
) -> PgResult<()> {
    debug_assert!(sjinfo.ojrelid == 0);
    assert!(
        run.root.path(outer_path).base().param_info.is_none()
            && run.root.path(inner_path).base().param_info.is_none(),
        "try_hashjoin_path (joinpath.c): parameterized input; M2 param-path lane"
    );
    let workspace = initial_cost_hashjoin(run, hashclauses, outer_path, inner_path);
    if add_path_precheck(
        run,
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        &[],
    ) {
        let path = create_hashjoin_path(
            run, joinrel, jointype, &workspace, inner_unique, outer_path, inner_path, restrictlist,
            hashclauses,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

fn page_size(tuples: f64, width: i32) -> f64 {
    (crate::costsize::relation_byte_size(tuples, width) / 8192.0).ceil()
}

fn initial_cost_hashjoin(
    run: &PlannerRun<'_>,
    hashclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
) -> JoinCostWorkspace {
    let (o_rows, o_startup, o_total, o_disabled) = {
        let o = run.root.path(outer_path).base();
        (o.rows, o.startup_cost, o.total_cost, o.disabled_nodes)
    };
    let (i_rows, i_startup, i_total, i_disabled) = {
        let i = run.root.path(inner_path).base();
        (i.rows, i.startup_cost, i.total_cost, i.disabled_nodes)
    };
    let _ = i_startup;
    let mut disabled_nodes = if gucs::enable_hashjoin() { 0 } else { 1 };
    disabled_nodes += i_disabled + o_disabled;

    let num_hashclauses = hashclauses.len() as f64;
    let mut startup_cost = o_startup;
    let mut run_cost = o_total - o_startup;
    startup_cost += i_total;
    startup_cost += (gucs::cpu_operator_cost() * num_hashclauses + gucs::cpu_tuple_cost()) * i_rows;
    run_cost += gucs::cpu_operator_cost() * num_hashclauses * o_rows;

    let inner_width = run.root.path_pathtarget(inner_path).width;
    let (numbuckets, numbatches, _skew) =
        ::nodehash::exec_choose_hash_table_size(i_rows, inner_width, true);

    if numbatches > 1 {
        let outer_width = run.root.path_pathtarget(outer_path).width;
        let outerpages = page_size(o_rows, outer_width);
        let innerpages = page_size(i_rows, inner_width);
        startup_cost += gucs::seq_page_cost() * innerpages;
        run_cost += gucs::seq_page_cost() * (innerpages + 2.0 * outerpages);
    }

    JoinCostWorkspace {
        startup_cost,
        total_cost: startup_cost + run_cost,
        run_cost,
        disabled_nodes,
        numbuckets: numbuckets as i32,
        numbatches,
        inner_rows_total: i_rows,
        ..Default::default()
    }
}

const DISABLE_COST: f64 = 1.0e10;

fn final_cost_hashjoin(
    run: &mut PlannerRun<'_>,
    path: &mut HashPath<'_>,
    workspace: &JoinCostWorkspace,
) -> PgResult<()> {
    let outer_path = path.jpath.outerjoinpath.unwrap();
    let inner_path = path.jpath.innerjoinpath.unwrap();
    let outer_path_rows = run.root.path(outer_path).base().rows;
    let inner_path_rows = run.root.path(inner_path).base().rows;
    let inner_width = run.root.path_pathtarget(inner_path).width;
    let inner_parent = run.root.path(inner_path).base().parent;

    let numbuckets = workspace.numbuckets;
    let numbatches = workspace.numbatches;
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    debug_assert!(path.jpath.path.param_info.is_none());
    path.jpath.path.rows = run.root.rel(path.jpath.path.parent).rows;
    debug_assert!(path.jpath.path.parallel_workers == 0);
    assert!(
        path.jpath.jointype == JOIN_INNER && !path.jpath.inner_unique,
        "final_cost_hashjoin (costsize.c): SEMI/ANTI/inner_unique branch; M2 lane"
    );

    path.num_batches = numbatches;
    path.inner_rows_total = workspace.inner_rows_total;

    let virtualbuckets = numbuckets as f64 * numbatches as f64;

    // No UniquePath / extended stats on this lane: estimate_multivariate_
    // bucketsize is the identity (returns every hashclause) and each clause's
    // bucketsize comes from estimate_hash_bucket_stats.
    let mut innerbucketsize = 1.0f64;
    let mut innermcvfreq = 1.0f64;
    let inner_relids = run.root.rel(inner_parent).relids.clone();
    let hcls = crate::relnode::pgvec_clone_shallow(run.mcx, &path.path_hashclauses);
    for &hcl in hcls.iter() {
        let right_is_inner = {
            let r = run.root.rinfo(hcl);
            crate::relnode::relids_is_subset(&r.right_relids, &inner_relids)
        };
        let (thisbucketsize, thismcvfreq) = if right_is_inner {
            let cached = run.root.rinfo(hcl).right_bucketsize;
            if cached < 0.0 {
                let clause = *run.root.expr_node(run.root.rinfo(hcl).clause);
                let rightop = clause.as_op_expr().unwrap().args.nth(1);
                let (mcv, bs) =
                    crate::selfuncs::estimate_hash_bucket_stats(run, rightop, virtualbuckets)?;
                let r = run.root.rinfo_mut(hcl);
                r.right_mcvfreq = mcv;
                r.right_bucketsize = bs;
                (bs, mcv)
            } else {
                (cached, run.root.rinfo(hcl).right_mcvfreq)
            }
        } else {
            let cached = run.root.rinfo(hcl).left_bucketsize;
            if cached < 0.0 {
                let clause = *run.root.expr_node(run.root.rinfo(hcl).clause);
                let leftop = clause.as_op_expr().unwrap().args.nth(0);
                let (mcv, bs) =
                    crate::selfuncs::estimate_hash_bucket_stats(run, leftop, virtualbuckets)?;
                let r = run.root.rinfo_mut(hcl);
                r.left_mcvfreq = mcv;
                r.left_bucketsize = bs;
                (bs, mcv)
            } else {
                (cached, run.root.rinfo(hcl).left_mcvfreq)
            }
        };
        if innerbucketsize > thisbucketsize {
            innerbucketsize = thisbucketsize;
        }
        if innermcvfreq > thismcvfreq {
            innermcvfreq = thismcvfreq;
        }
    }

    if crate::costsize::relation_byte_size(
        crate::costsize::clamp_row_est(inner_path_rows * innermcvfreq),
        inner_width,
    ) > ::nodehash::get_hash_memory_limit() as f64
    {
        startup_cost += DISABLE_COST;
    }

    let hash_qual_cost = crate::costsize::cost_qual_eval(run, &hcls)?;
    let joinrestrict = crate::relnode::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let qp_qual_cost = crate::costsize::cost_qual_eval(run, &joinrestrict)?;
    let qp_startup = qp_qual_cost.startup - hash_qual_cost.startup;
    let qp_per_tuple = qp_qual_cost.per_tuple - hash_qual_cost.per_tuple;

    startup_cost += hash_qual_cost.startup;
    run_cost += hash_qual_cost.per_tuple
        * outer_path_rows
        * crate::costsize::clamp_row_est(inner_path_rows * innerbucketsize)
        * 0.5;

    // approx_tuple_count divergence: the joinrel size estimate already applies
    // the (equijoin) hashclause selectivity, so reuse it for the CPU term.
    let hashjointuples = path.jpath.path.rows;

    startup_cost += qp_startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qp_per_tuple;
    run_cost += cpu_per_tuple * hashjointuples;

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path.jpath.path.rows;

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_hashjoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    restrict_clauses: &[RinfoId],
    hashclauses: &[RinfoId],
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let mut joinrestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    joinrestrictinfo.extend(restrict_clauses.iter().copied());
    let mut path_hashclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    path_hashclauses.extend(hashclauses.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_HashPath),
        pathtype: tag16(NodeTag::T_HashJoin),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: PgVec::new_in(mcx),
    };
    let mut node = HashPath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
        path_hashclauses,
        num_batches: workspace.numbatches,
        inner_rows_total: workspace.inner_rows_total,
    };
    final_cost_hashjoin(run, &mut node, workspace)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::HashPath(node)))
}

// ExecSupportsMarkRestore (execAmi.c), keyed on pathtype like C.
fn exec_supports_mark_restore(run: &PlannerRun<'_>, path_id: PathId) -> bool {
    let node = run.root.path(path_id);
    let pathtype = node.base().pathtype;
    if pathtype == tag16(NodeTag::T_IndexScan) || pathtype == tag16(NodeTag::T_IndexOnlyScan) {
        let types_pathnodes::PathNode::IndexPath(ip) = node else {
            panic!("index pathtype on a non-IndexPath")
        };
        return ip.indexinfo.as_ref().expect("indexinfo set").amcanmarkpos;
    }
    if pathtype == tag16(NodeTag::T_Material) || pathtype == tag16(NodeTag::T_Sort) {
        return true;
    }
    if pathtype == tag16(NodeTag::T_Result) {
        return match node {
            types_pathnodes::PathNode::ProjectionPath(pp) => {
                exec_supports_mark_restore(run, pp.subpath.expect("projection has a subpath"))
            }
            _ => false,
        };
    }
    false
}

// cached_scansel (costsize.c): mergejoinscansel memoized on the RestrictInfo
// (leaving scansel_cache unwritten cost fabled 53x on joinplan).
fn cached_scansel(
    run: &mut PlannerRun<'_>,
    rinfo: RinfoId,
    pathkey: &PathKey,
) -> PgResult<MergeScanSelCache> {
    let collation = run
        .root
        .ec(pathkey.pk_eclass.expect("canonical pathkey has an eclass"))
        .ec_collation;
    for cache in run.root.rinfo(rinfo).scansel_cache.iter() {
        if cache.opfamily == pathkey.pk_opfamily
            && cache.collation == collation
            && cache.cmptype == pathkey.pk_cmptype
            && cache.nulls_first == pathkey.pk_nulls_first
        {
            return Ok(*cache);
        }
    }
    let (leftstartsel, leftendsel, rightstartsel, rightendsel) = crate::selfuncs::mergejoinscansel(
        run,
        rinfo,
        pathkey.pk_opfamily,
        pathkey.pk_cmptype,
        pathkey.pk_nulls_first,
    )?;
    let cache = MergeScanSelCache {
        opfamily: pathkey.pk_opfamily,
        collation,
        cmptype: pathkey.pk_cmptype,
        nulls_first: pathkey.pk_nulls_first,
        leftstartsel,
        leftendsel,
        rightstartsel,
        rightendsel,
    };
    run.root.rinfo_mut(rinfo).scansel_cache.push(cache);
    Ok(cache)
}

#[allow(clippy::too_many_arguments)]
fn initial_cost_mergejoin(
    run: &mut PlannerRun<'_>,
    jointype: u32,
    mergeclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
    outersortkeys: &[PathKey],
    innersortkeys: &[PathKey],
    outer_presorted_keys: usize,
) -> PgResult<JoinCostWorkspace> {
    let mut startup_cost = 0.0f64;
    let mut run_cost = 0.0f64;
    let outer_path_rows = run.root.path(outer_path).base().rows.max(1.0);
    let inner_path_rows = run.root.path(inner_path).base().rows.max(1.0);

    let (outerstartsel, outerendsel, innerstartsel, innerendsel);
    if !mergeclauses.is_empty() {
        debug_assert!(jointype == JOIN_INNER);
        let firstclause = mergeclauses[0];
        let opathkey = if !outersortkeys.is_empty() {
            outersortkeys[0]
        } else {
            run.root.path(outer_path).base().pathkeys[0]
        };
        let ipathkey = if !innersortkeys.is_empty() {
            innersortkeys[0]
        } else {
            run.root.path(inner_path).base().pathkeys[0]
        };
        assert!(
            opathkey.pk_opfamily == ipathkey.pk_opfamily
                && run.root.ec(opathkey.pk_eclass.unwrap()).ec_collation
                    == run.root.ec(ipathkey.pk_eclass.unwrap()).ec_collation
                && opathkey.pk_cmptype == ipathkey.pk_cmptype
                && opathkey.pk_nulls_first == ipathkey.pk_nulls_first,
            "left and right pathkeys do not match in mergejoin"
        );

        let cache = cached_scansel(run, firstclause, &opathkey)?;
        let left_is_outer = crate::relnode::relids_is_subset(
            &run.root.rinfo(firstclause).left_relids,
            &run.root.rel(run.root.path(outer_path).base().parent).relids,
        );
        if left_is_outer {
            outerstartsel = cache.leftstartsel;
            outerendsel = cache.leftendsel;
            innerstartsel = cache.rightstartsel;
            innerendsel = cache.rightendsel;
        } else {
            outerstartsel = cache.rightstartsel;
            outerendsel = cache.rightendsel;
            innerstartsel = cache.leftstartsel;
            innerendsel = cache.leftendsel;
        }
    } else {
        outerstartsel = 0.0;
        innerstartsel = 0.0;
        outerendsel = 1.0;
        innerendsel = 1.0;
    }

    let outer_skip_rows = (outer_path_rows * outerstartsel).round_ties_even();
    let inner_skip_rows = (inner_path_rows * innerstartsel).round_ties_even();
    let outer_rows = crate::costsize::clamp_row_est(outer_path_rows * outerendsel);
    let inner_rows = crate::costsize::clamp_row_est(inner_path_rows * innerendsel);
    debug_assert!(outer_skip_rows <= outer_rows);
    debug_assert!(inner_skip_rows <= inner_rows);

    let outerstartsel = outer_skip_rows / outer_path_rows;
    let innerstartsel = inner_skip_rows / inner_path_rows;
    let outerendsel = outer_rows / outer_path_rows;
    let innerendsel = inner_rows / inner_path_rows;

    let mut disabled_nodes = if gucs::enable_mergejoin() { 0 } else { 1 };

    let work_mem = init_small::globals::work_mem();
    if !outersortkeys.is_empty() {
        debug_assert!(!pathkeys_contained_in(
            outersortkeys,
            &run.root.path(outer_path).base().pathkeys
        ));
        assert!(
            !(gucs::enable_incremental_sort() && outer_presorted_keys > 0),
            "initial_cost_mergejoin (costsize.c): incremental-sort outer; M2 incsort lane"
        );
        let outer = run.root.path(outer_path).base();
        let (o_disabled, o_total) = (outer.disabled_nodes, outer.total_cost);
        let width = run.root.path_pathtarget(outer_path).width;
        let (sort_disabled, sort_startup, sort_total) = crate::costsize::cost_sort_shape(
            o_disabled,
            o_total,
            outer_path_rows,
            width,
            0.0,
            work_mem,
            -1.0,
        );
        disabled_nodes += sort_disabled;
        startup_cost += sort_startup;
        startup_cost += (sort_total - sort_startup) * outerstartsel;
        run_cost += (sort_total - sort_startup) * (outerendsel - outerstartsel);
    } else {
        let outer = run.root.path(outer_path).base();
        disabled_nodes += outer.disabled_nodes;
        startup_cost += outer.startup_cost;
        startup_cost += (outer.total_cost - outer.startup_cost) * outerstartsel;
        run_cost += (outer.total_cost - outer.startup_cost) * (outerendsel - outerstartsel);
    }

    let inner_run_cost;
    if !innersortkeys.is_empty() {
        debug_assert!(!pathkeys_contained_in(
            innersortkeys,
            &run.root.path(inner_path).base().pathkeys
        ));
        let inner = run.root.path(inner_path).base();
        let (i_disabled, i_total) = (inner.disabled_nodes, inner.total_cost);
        let width = run.root.path_pathtarget(inner_path).width;
        let (sort_disabled, sort_startup, sort_total) = crate::costsize::cost_sort_shape(
            i_disabled,
            i_total,
            inner_path_rows,
            width,
            0.0,
            work_mem,
            -1.0,
        );
        disabled_nodes += sort_disabled;
        startup_cost += sort_startup;
        startup_cost += (sort_total - sort_startup) * innerstartsel;
        inner_run_cost = (sort_total - sort_startup) * (innerendsel - innerstartsel);
    } else {
        let inner = run.root.path(inner_path).base();
        disabled_nodes += inner.disabled_nodes;
        startup_cost += inner.startup_cost;
        startup_cost += (inner.total_cost - inner.startup_cost) * innerstartsel;
        inner_run_cost = (inner.total_cost - inner.startup_cost) * (innerendsel - innerstartsel);
    }

    Ok(JoinCostWorkspace {
        disabled_nodes,
        startup_cost,
        total_cost: startup_cost + run_cost + inner_run_cost,
        run_cost,
        inner_run_cost,
        outer_rows,
        inner_rows,
        outer_skip_rows,
        inner_skip_rows,
        ..Default::default()
    })
}

// approx_tuple_count (costsize.c).
fn approx_tuple_count(
    run: &mut PlannerRun<'_>,
    outer_path: PathId,
    inner_path: PathId,
    quals: &[RinfoId],
) -> PgResult<f64> {
    let outer_tuples = run.root.path(outer_path).base().rows;
    let inner_tuples = run.root.path(inner_path).base().rows;
    let outer_relids = crate::relnode::relids_copy(
        run.mcx,
        &run.root.rel(run.root.path(outer_path).base().parent).relids,
    );
    let inner_relids = crate::relnode::relids_copy(
        run.mcx,
        &run.root.rel(run.root.path(inner_path).base().parent).relids,
    );
    let sjinfo = crate::joinrels::init_dummy_sjinfo(run, outer_relids, inner_relids);
    let mut selec = 1.0f64;
    for &q in quals {
        selec *= crate::clausesel::clause_selectivity(run, q, 0, JOIN_INNER, Some(&sjinfo))?;
    }
    Ok(crate::costsize::clamp_row_est(selec * outer_tuples * inner_tuples))
}

fn final_cost_mergejoin(
    run: &mut PlannerRun<'_>,
    path: &mut MergePath<'_>,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
) -> PgResult<()> {
    let outer_path = path.jpath.outerjoinpath.unwrap();
    let inner_path = path.jpath.innerjoinpath.unwrap();
    let inner_path_rows = run.root.path(inner_path).base().rows.max(1.0);
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;
    let inner_run_cost = workspace.inner_run_cost;
    let outer_rows = workspace.outer_rows;
    let inner_rows = workspace.inner_rows;
    let outer_skip_rows = workspace.outer_skip_rows;
    let inner_skip_rows = workspace.inner_skip_rows;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    debug_assert!(path.jpath.path.param_info.is_none());
    path.jpath.path.rows = run.root.rel(path.jpath.path.parent).rows;
    debug_assert!(path.jpath.path.parallel_workers == 0);

    let mergeclauses = crate::relnode::pgvec_clone_shallow(run.mcx, &path.path_mergeclauses);
    let restrictinfos = crate::relnode::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let merge_qual_cost = crate::costsize::cost_qual_eval(run, &mergeclauses)?;
    let mut qp_qual_cost = crate::costsize::cost_qual_eval(run, &restrictinfos)?;
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    debug_assert!(path.jpath.jointype == JOIN_INNER);
    path.skip_mark_restore =
        inner_unique && path.jpath.joinrestrictinfo.len() == path.path_mergeclauses.len();

    let mergejointuples = approx_tuple_count(run, outer_path, inner_path, &mergeclauses)?;

    let rescannedtuples = if path.skip_mark_restore {
        0.0
    } else {
        (mergejointuples - inner_path_rows).max(0.0)
    };
    let rescanratio = 1.0 + rescannedtuples / inner_rows;

    let bare_inner_cost = inner_run_cost * rescanratio;
    let mat_inner_cost = inner_run_cost + gucs::cpu_operator_cost() * inner_rows * rescanratio;

    let inner_width = run.root.path_pathtarget(inner_path).width;
    path.materialize_inner = if path.skip_mark_restore {
        false
    } else if gucs::enable_material() && mat_inner_cost < bare_inner_cost {
        true
    } else if path.innersortkeys.is_empty() && !exec_supports_mark_restore(run, inner_path) {
        true
    } else if gucs::enable_material()
        && !path.innersortkeys.is_empty()
        && crate::costsize::relation_byte_size(inner_path_rows, inner_width)
            > init_small::globals::work_mem() as f64 * 1024.0
    {
        true
    } else {
        false
    };

    run_cost += if path.materialize_inner { mat_inner_cost } else { bare_inner_cost };

    startup_cost += merge_qual_cost.startup;
    startup_cost += merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost += merge_qual_cost.per_tuple
        * ((outer_rows - outer_skip_rows) + (inner_rows - inner_skip_rows) * rescanratio);

    startup_cost += qp_qual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path.jpath.path.rows;

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

// create_mergejoin_path (pathnode.c); required_outer is always NULL.
#[allow(clippy::too_many_arguments)]
fn create_mergejoin_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: u32,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
    restrict_clauses: &[RinfoId],
    pathkeys: PgVec<'mcx, PathKey>,
    mergeclauses: PgVec<'mcx, RinfoId>,
    outersortkeys: PgVec<'mcx, PathKey>,
    innersortkeys: PgVec<'mcx, PathKey>,
    outer_presorted_keys: usize,
) -> PgResult<PathId> {
    let mcx = run.mcx;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let parallel_safe =
        run.root.rel(joinrel).consider_parallel && outer.parallel_safe && inner.parallel_safe;
    let parallel_workers = outer.parallel_workers;

    let mut joinrestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    joinrestrictinfo.extend(restrict_clauses.iter().copied());

    let path = Path {
        type_: tag16(NodeTag::T_MergePath),
        pathtype: tag16(NodeTag::T_MergeJoin),
        parent: joinrel,
        pathtarget_id: run.root.rel(joinrel).pathtarget_id,
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys,
    };
    let mut node = MergePath {
        jpath: JoinPath {
            path,
            jointype,
            inner_unique,
            outerjoinpath: Some(outer_path),
            innerjoinpath: Some(inner_path),
            joinrestrictinfo,
        },
        path_mergeclauses: mergeclauses,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys: outer_presorted_keys as i32,
        skip_mark_restore: false,
        materialize_inner: false,
    };
    final_cost_mergejoin(run, &mut node, workspace, inner_unique)?;
    Ok(run.root.alloc_path(types_pathnodes::PathNode::MergePath(node)))
}
