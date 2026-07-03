//! joinpath.c nestloop arm + its pathnode.c/costsize.c join-cost slice.
//! DIVERGENCE: merge/hash join candidates (sort_inner_and_outer/
//! hash_inner_and_outer) are not generated -- plan choice (not results) can
//! differ from C where one would win; parameterized nestloops are loud.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
use types_pathnodes::{
    JoinPath, MaterialPath, NestPath, Path, PathId, RelId, RinfoId, SpecialJoinInfo,
    JOIN_INNER,
};

use crate::gucs;
use crate::pathnode::tag16;
use crate::run::PlannerRun;

struct JoinCostWorkspace {
    startup_cost: f64,
    total_cost: f64,
    run_cost: f64,
    disabled_nodes: i32,
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
    let inner_unique = innerrel_is_unique(run, innerrel, restrictlist);
    debug_assert!(run.root.join_info_list.is_empty());
    match_unsorted_outer(run, joinrel, outerrel, innerrel, jointype, inner_unique, sjinfo, restrictlist)
}

// innerrel_is_unique (analyzejoins.c): the no-unique-index quick exit is the
// only live leg; a provable-uniqueness candidate routes to the loud lane.
fn innerrel_is_unique(run: &PlannerRun<'_>, innerrel: RelId, restrictlist: &[RinfoId]) -> bool {
    if restrictlist.is_empty() {
        return false;
    }
    // rel_supports_distinctness over the innerrel's indexlist.
    let rel = run.root.rel(innerrel);
    if rel.reloptkind != types_pathnodes::RELOPT_BASEREL
        || rel.rtekind != types_pathnodes::RTE_RELATION
    {
        return false;
    }
    for ind in rel.indexlist.iter() {
        if ind.unique && ind.immediate && ind.indpred.is_empty() {
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
        assert!(
            run.root.path(outerpath).base().pathkeys.is_empty(),
            "build_join_pathkeys (pathkeys.c): ordered outer path; M2 pathkey lane"
        );
        let inner_candidates = crate::relnode::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(innerrel).cheapest_parameterized_paths,
        );
        for &innerpath in inner_candidates.iter() {
            try_nestloop_path(run, joinrel, outerpath, innerpath, jointype, inner_unique, sjinfo, restrictlist)?;
        }
        if let Some(mp) = matpath {
            try_nestloop_path(run, joinrel, outerpath, mp, jointype, inner_unique, sjinfo, restrictlist)?;
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

    if add_path_precheck(run, joinrel, workspace.disabled_nodes, workspace.startup_cost, workspace.total_cost) {
        let path = create_nestloop_path(
            run,
            joinrel,
            jointype,
            &workspace,
            inner_unique,
            outer_path,
            inner_path,
            restrictlist,
        )?;
        crate::pathnode::add_path(run, joinrel, path);
    }
    Ok(())
}

// add_path_precheck (pathnode.c); pathkeys are NIL and required_outer NULL on
// every path this lane can build, so the pathkey comparison is always EQUAL.
fn add_path_precheck(
    run: &PlannerRun<'_>,
    joinrel: RelId,
    disabled_nodes: i32,
    startup_cost: f64,
    total_cost: f64,
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
            debug_assert!(old.pathkeys.is_empty() && old.param_info.is_none());
            return false;
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
        pathkeys: PgVec::new_in(mcx),
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
