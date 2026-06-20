//! Join cost estimators (costsize.c:3266-4523) + the join-size estimate family
//! (costsize.c:5106-5633).

use alloc::vec::Vec;

use types_core::primitive::{Cost, Oid, Selectivity};
use types_error::PgResult;
use types_pathnodes::optimizer_plan::{JoinCostWorkspace, JoinPathExtraData, SemiAntiJoinFactors};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    HashPath, JoinType, MergePath, NestPath, Path, PathId, PathKey, PathNode, PlannerInfo, RelId,
    Relids, RinfoId, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_SEMI,
};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_util_pathnode_seams as ps;

use crate::{
    bms_is_subset, clamp_row_est, cost_incremental_sort_owned, cost_qual_eval_rinfos,
    cost_sort_owned, cpu_operator_cost, cpu_tuple_cost, disable_cost, get_parallel_divisor,
    page_size, relation_byte_size, rint, work_mem, Max, Min, ENABLE_HASHJOIN, ENABLE_MATERIAL,
    ENABLE_MERGEJOIN, ENABLE_NESTLOOP, ENABLE_INCREMENTAL_SORT,
};

/* --------------------------------------------------------------------------
 * Small helpers over the owned-tree model.
 * ------------------------------------------------------------------------ */

/// `IS_OUTER_JOIN(jointype)` (nodes.h).
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | JOIN_RIGHT_ANTI
    )
}

/// `ExecSupportsMarkRestore(pathnode)` (execAmi.c) — does a Path support
/// mark/restore?
///
/// This is used during planning (here, `final_cost_mergejoin` deciding whether
/// to materialize the inner side) and so must accept a Path, not a Plan. The
/// owned-tree analogue: the Path is identified by its `PathId` arena handle and
/// resolved through `root.path(..)`. We examine the `pathtype` (the Plan node
/// type the Path would produce), not the `PathNode` tag, exactly like C.
pub fn exec_supports_mark_restore(root: &PlannerInfo, path: PathId) -> bool {
    use types_nodes::nodes;
    let pathnode = root.path(path);
    let pathtype = pathnode.base().pathtype;

    if pathtype == nodes::T_IndexScan || pathtype == nodes::T_IndexOnlyScan {
        // Not all index types support mark/restore.
        //   return castNode(IndexPath, pathnode)->indexinfo->amcanmarkpos;
        match pathnode {
            PathNode::IndexPath(ip) => ip
                .indexinfo
                .as_ref()
                .expect("ExecSupportsMarkRestore: IndexPath->indexinfo must be set")
                .amcanmarkpos,
            other => panic!("castNode(IndexPath, pathnode) failed: {other:?}"),
        }
    } else if pathtype == nodes::T_Material || pathtype == nodes::T_Sort {
        true
    } else if pathtype == nodes::T_CustomScan {
        match pathnode {
            PathNode::CustomPath(cp) => {
                cp.flags & types_pathnodes::CUSTOMPATH_SUPPORT_MARK_RESTORE != 0
            }
            other => panic!("castNode(CustomPath, pathnode) failed: {other:?}"),
        }
    } else if pathtype == nodes::T_Result {
        // Result supports mark/restore iff it has a child plan that does.
        // There is more than one Path type that can produce a Result plan node.
        match pathnode {
            PathNode::ProjectionPath(pp) => match pp.subpath {
                Some(sub) => exec_supports_mark_restore(root, sub),
                None => false,
            },
            // MinMaxAggPath / GroupResultPath produce childless Result nodes.
            PathNode::MinMaxAggPath(_) | PathNode::GroupResultPath(_) => false,
            // Simple RTE_RESULT base relation: Assert(IsA(pathnode, Path)) in C;
            // childless Result.
            PathNode::Path(_) => false,
            other => {
                debug_assert!(
                    false,
                    "T_Result pathtype on unexpected Path node \
                     (C: Assert(IsA(pathnode, Path))): {other:?}"
                );
                false
            }
        }
    } else if pathtype == nodes::T_Append {
        match pathnode {
            PathNode::AppendPath(ap) => {
                // If there's exactly one child, there will be no Append in the
                // final plan, so mark/restore follows the child's ability.
                if ap.subpaths.len() == 1 {
                    exec_supports_mark_restore(root, ap.subpaths[0])
                } else {
                    false
                }
            }
            other => panic!("castNode(AppendPath, pathnode) failed: {other:?}"),
        }
    } else if pathtype == nodes::T_MergeAppend {
        match pathnode {
            PathNode::MergeAppendPath(mapath) => {
                // Like the Append case: single-subpath MergeAppends won't be in
                // the final plan, so just return the child's ability.
                if mapath.subpaths.len() == 1 {
                    exec_supports_mark_restore(root, mapath.subpaths[0])
                } else {
                    false
                }
            }
            other => panic!("castNode(MergeAppendPath, pathnode) failed: {other:?}"),
        }
    } else {
        false
    }
}

/// `CLAMP_PROBABILITY(p)` (optimizer.h). Used by the C
/// `get_foreign_key_join_selectivity`.
fn clamp_probability(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

/// `RINFO_IS_PUSHED_DOWN(rinfo, joinrelids)` (pathnodes.h).
fn rinfo_is_pushed_down(root: &PlannerInfo, rinfo: RinfoId, joinrelids: &Relids) -> bool {
    let r = root.rinfo(rinfo);
    r.is_pushed_down || !bms_is_subset(&r.required_relids, joinrelids)
}

/// Read a pathkey's EquivalenceClass collation (`pk_eclass->ec_collation`).
fn pathkey_collation(root: &PlannerInfo, pathkey: &PathKey) -> Oid {
    let ec = pathkey
        .pk_eclass
        .expect("pathkey_collation: pk_eclass must be set");
    root.ec(ec).ec_collation
}

/// `MergeScanSelCache` result of `mergejoinscansel` for one clause/pathkey.
#[derive(Clone, Copy)]
struct ScanSel {
    leftstartsel: Selectivity,
    leftendsel: Selectivity,
    rightstartsel: Selectivity,
    rightendsel: Selectivity,
}

/// `cached_scansel` (costsize.c:4080). The fabled `RestrictInfo.scansel_cache`
/// carries opaque `NodeId` handles (not real `MergeScanSelCache` structs), so
/// the cache lookup is not field-resolvable here; we compute via the
/// `mergejoinscansel` seam each time (behaviour-preserving: same computed value,
/// only the rinfo-internal cache hit differs).
fn cached_scansel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    pathkey: &PathKey,
) -> ScanSel {
    let clause = root.rinfo(rinfo).clause;
    let (ls, le, rs, re) = cz::mergejoinscansel::call(
        run,
        root,
        clause,
        pathkey.pk_opfamily,
        pathkey.pk_cmptype,
        pathkey.pk_nulls_first,
    )
    .expect("mergejoinscansel");
    ScanSel {
        leftstartsel: ls,
        leftendsel: le,
        rightstartsel: rs,
        rightendsel: re,
    }
}

/* ==========================================================================
 * nestloop (costsize.c:3266-3517)
 * ========================================================================== */

/// `initial_cost_nestloop` (costsize.c:3266) — returns the filled workspace.
pub fn initial_cost_nestloop<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    jointype: JoinType,
    outer_path: PathId,
    inner_path: PathId,
    extra: &JoinPathExtraData,
) -> PgResult<JoinCostWorkspace> {
    let mut workspace = JoinCostWorkspace_default();

    let mut disabled_nodes: i32;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (outer_path_rows, outer_startup, outer_total, outer_disabled) = {
        let o = root.path(outer_path).base();
        (o.rows, o.startup_cost, o.total_cost, o.disabled_nodes)
    };
    let (inner_startup, inner_total, inner_disabled) = {
        let i = root.path(inner_path).base();
        (i.startup_cost, i.total_cost, i.disabled_nodes)
    };

    disabled_nodes = if ENABLE_NESTLOOP { 0 } else { 1 };
    disabled_nodes += inner_disabled;
    disabled_nodes += outer_disabled;

    let (inner_rescan_start_cost, inner_rescan_total_cost) =
        crate::exprcost::cost_rescan(run, root, inner_path)?;

    startup_cost += outer_startup + inner_startup;
    run_cost += outer_total - outer_startup;
    if outer_path_rows > 1.0 {
        run_cost += (outer_path_rows - 1.0) * inner_rescan_start_cost;
    }

    let inner_run_cost = inner_total - inner_startup;
    let inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost;

    if jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique {
        workspace.inner_run_cost = inner_run_cost;
        workspace.inner_rescan_run_cost = inner_rescan_run_cost;
    } else {
        run_cost += inner_run_cost;
        if outer_path_rows > 1.0 {
            run_cost += (outer_path_rows - 1.0) * inner_rescan_run_cost;
        }
    }

    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost;
    workspace.run_cost = run_cost;
    Ok(workspace)
}

/// `final_cost_nestloop` (costsize.c:3389) — fills a `NestPath` (by `PathId`).
pub fn final_cost_nestloop(
    root: &mut PlannerInfo,
    path_id: PathId,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    let (outer_id, inner_id, jointype, joinrestrictinfo, parent, parallel_workers, param_rows) = {
        let np = expect_nest(root, path_id);
        (
            np.jpath.outerjoinpath.expect("final_cost_nestloop: outer"),
            np.jpath.innerjoinpath.expect("final_cost_nestloop: inner"),
            np.jpath.jointype,
            np.jpath.joinrestrictinfo.clone(),
            np.jpath.path.parent,
            np.jpath.path.parallel_workers,
            np.jpath.path.param_info.as_deref().map(|pi| pi.ppi_rows),
        )
    };

    let mut outer_path_rows = root.path(outer_id).base().rows;
    let mut inner_path_rows = root.path(inner_id).base().rows;
    let mut startup_cost: Cost = workspace.startup_cost;
    let mut run_cost: Cost = workspace.run_cost;
    let cpu_per_tuple: Cost;
    let mut ntuples: f64;

    set_join_disabled(root, path_id, workspace.disabled_nodes);

    if outer_path_rows <= 0.0 {
        outer_path_rows = 1.0;
    }
    if inner_path_rows <= 0.0 {
        inner_path_rows = 1.0;
    }

    let mut rows = match param_rows {
        Some(r) => r,
        None => root.rel(parent).rows,
    };
    if parallel_workers > 0 {
        let pd = get_parallel_divisor(root.path(path_id).base());
        rows = clamp_row_est(rows / pd);
    }
    set_join_rows(root, path_id, rows);

    if jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique {
        let inner_run_cost = workspace.inner_run_cost;
        let inner_rescan_run_cost = workspace.inner_rescan_run_cost;

        let mut outer_matched_rows = rint(outer_path_rows * extra.semifactors.outer_match_frac);
        let mut outer_unmatched_rows = outer_path_rows - outer_matched_rows;
        let inner_scan_frac = 2.0 / (extra.semifactors.match_count + 1.0);

        ntuples = outer_matched_rows * inner_path_rows * inner_scan_frac;

        if has_indexed_join_quals(root, path_id) {
            run_cost += inner_run_cost * inner_scan_frac;
            if outer_matched_rows > 1.0 {
                run_cost += (outer_matched_rows - 1.0) * inner_rescan_run_cost * inner_scan_frac;
            }
            run_cost += outer_unmatched_rows * inner_rescan_run_cost / inner_path_rows;
        } else {
            ntuples += outer_unmatched_rows * inner_path_rows;

            run_cost += inner_run_cost;
            if outer_unmatched_rows >= 1.0 {
                outer_unmatched_rows -= 1.0;
            } else {
                outer_matched_rows -= 1.0;
            }

            if outer_matched_rows > 0.0 {
                run_cost += outer_matched_rows * inner_rescan_run_cost * inner_scan_frac;
            }
            if outer_unmatched_rows > 0.0 {
                run_cost += outer_unmatched_rows * inner_rescan_run_cost;
            }
        }
    } else {
        ntuples = outer_path_rows * inner_path_rows;
    }

    let restrict_qual_cost = cost_qual_eval_rinfos(root, &joinrestrictinfo);
    startup_cost += restrict_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + restrict_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let path_rows = root.path(path_id).base().rows;
    let (pt_startup, pt_per_tuple) = join_pathtarget_cost(root, path_id, "final_cost_nestloop");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * path_rows;

    let p = root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `has_indexed_join_quals` (costsize.c:5210).
pub fn has_indexed_join_quals(root: &PlannerInfo, path_id: PathId) -> bool {
    let (joinrestrictinfo, inner_id, joinrel_parent) = {
        let np = expect_nest(root, path_id);
        (
            np.jpath.joinrestrictinfo.clone(),
            np.jpath.innerjoinpath,
            np.jpath.path.parent,
        )
    };

    if !joinrestrictinfo.is_empty() {
        return false;
    }

    let inner_id = match inner_id {
        Some(id) => id,
        None => return false,
    };

    let inner_param = match root.path(inner_id).base().param_info.as_deref() {
        Some(pi) => pi.ppi_clauses.clone(),
        None => return false,
    };

    // Find the indexclauses list for the inner scan: a plain IndexPath, or a
    // BitmapHeapPath whose bitmapqual is a plain IndexPath.
    let index_path_id: PathId = match root.path(inner_id) {
        PathNode::IndexPath(_) => inner_id,
        PathNode::BitmapHeapPath(bp) => match bp.bitmapqual {
            Some(q) => match root.path(q) {
                PathNode::IndexPath(_) => q,
                _ => return false,
            },
            None => return false,
        },
        _ => return false,
    };

    let inner_parent = root.path(inner_id).base().parent;
    let joinrelids = root.rel(joinrel_parent).relids.clone();
    let inner_relids = root.rel(inner_parent).relids.clone();
    let _ = (&joinrelids, &inner_relids);

    let mut found_one = false;
    for &rid in &inner_param {
        if cz::join_clause_is_movable_into::call(root, rid, inner_parent, joinrel_parent) {
            if !cz::is_redundant_with_indexclauses::call(root, rid, index_path_id) {
                return false;
            }
            found_one = true;
        }
    }
    found_one
}

/* ==========================================================================
 * merge join (costsize.c:3551-4131)
 * ========================================================================== */

/// `initial_cost_mergejoin` (costsize.c:3551) — returns the filled workspace.
pub fn initial_cost_mergejoin<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    jointype: JoinType,
    mergeclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
    outersortkeys: &[PathKey],
    innersortkeys: &[PathKey],
    outer_presorted_keys: i32,
    _extra: &JoinPathExtraData,
) -> PgResult<JoinCostWorkspace> {
    let mut workspace = JoinCostWorkspace_default();

    let mut disabled_nodes: i32;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let outer = root.path(outer_path).base().clone();
    let inner = root.path(inner_path).base().clone();
    let mut outer_path_rows = outer.rows;
    let mut inner_path_rows = inner.rows;
    let inner_run_cost: Cost;
    let outer_rows: f64;
    let inner_rows: f64;
    let outer_skip_rows: f64;
    let inner_skip_rows: f64;
    let mut outerstartsel: Selectivity;
    let mut outerendsel: Selectivity;
    let mut innerstartsel: Selectivity;
    let mut innerendsel: Selectivity;

    if outer_path_rows <= 0.0 {
        outer_path_rows = 1.0;
    }
    if inner_path_rows <= 0.0 {
        inner_path_rows = 1.0;
    }

    if !mergeclauses.is_empty() && jointype != JOIN_FULL {
        let firstclause = mergeclauses[0];
        let opathkeys: &[PathKey] = if !outersortkeys.is_empty() {
            outersortkeys
        } else {
            &outer.pathkeys
        };
        let ipathkeys: &[PathKey] = if !innersortkeys.is_empty() {
            innersortkeys
        } else {
            &inner.pathkeys
        };
        debug_assert!(!opathkeys.is_empty());
        debug_assert!(!ipathkeys.is_empty());
        let opathkey = &opathkeys[0];
        let ipathkey = &ipathkeys[0];
        if opathkey.pk_opfamily != ipathkey.pk_opfamily
            || pathkey_collation(root, opathkey) != pathkey_collation(root, ipathkey)
            || opathkey.pk_cmptype != ipathkey.pk_cmptype
            || opathkey.pk_nulls_first != ipathkey.pk_nulls_first
        {
            panic!(
                "backend-optimizer-path-costsize::initial_cost_mergejoin: \
                 left and right pathkeys do not match in mergejoin"
            );
        }

        let cache = cached_scansel(run, root, firstclause, opathkey);

        let first_left_relids = root.rinfo(firstclause).left_relids.clone();
        if bms_is_subset(&first_left_relids, &root.rel(outer.parent).relids) {
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
        if jointype == JOIN_LEFT || jointype == JOIN_ANTI {
            outerstartsel = 0.0;
            outerendsel = 1.0;
        } else if jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI {
            innerstartsel = 0.0;
            innerendsel = 1.0;
        }
    } else {
        outerstartsel = 0.0;
        innerstartsel = 0.0;
        outerendsel = 1.0;
        innerendsel = 1.0;
    }

    outer_skip_rows = rint(outer_path_rows * outerstartsel);
    inner_skip_rows = rint(inner_path_rows * innerstartsel);
    outer_rows = clamp_row_est(outer_path_rows * outerendsel);
    inner_rows = clamp_row_est(inner_path_rows * innerendsel);

    debug_assert!(outer_skip_rows <= outer_rows);
    debug_assert!(inner_skip_rows <= inner_rows);

    outerstartsel = outer_skip_rows / outer_path_rows;
    innerstartsel = inner_skip_rows / inner_path_rows;
    outerendsel = outer_rows / outer_path_rows;
    innerendsel = inner_rows / inner_path_rows;

    debug_assert!(outerstartsel <= outerendsel);
    debug_assert!(innerstartsel <= innerendsel);

    disabled_nodes = if ENABLE_MERGEJOIN { 0 } else { 1 };

    // cost of source data — outer side.
    if !outersortkeys.is_empty() {
        debug_assert!(!ps::pathkeys_contained_in::call(outersortkeys, &outer.pathkeys));

        let mut sort_path = make_dummy_sort_path(&outer);
        if ENABLE_INCREMENTAL_SORT && outer_presorted_keys > 0 {
            cost_incremental_sort_owned(
                &mut sort_path,
                run,
                root,
                outersortkeys,
                outer_presorted_keys,
                outer.disabled_nodes,
                outer.startup_cost,
                outer.total_cost,
                outer_path_rows,
                outer_pathtarget_width(&outer),
                0.0,
                work_mem(),
                -1.0,
            )?;
        } else {
            cost_sort_owned(
                &mut sort_path,
                outersortkeys,
                outer.disabled_nodes,
                outer.total_cost,
                outer_path_rows,
                outer_pathtarget_width(&outer),
                0.0,
                work_mem(),
                -1.0,
            );
        }

        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * outerstartsel;
        run_cost += (sort_path.total_cost - sort_path.startup_cost) * (outerendsel - outerstartsel);
    } else {
        disabled_nodes += outer.disabled_nodes;
        startup_cost += outer.startup_cost;
        startup_cost += (outer.total_cost - outer.startup_cost) * outerstartsel;
        run_cost += (outer.total_cost - outer.startup_cost) * (outerendsel - outerstartsel);
    }

    // cost of source data — inner side.
    if !innersortkeys.is_empty() {
        debug_assert!(!ps::pathkeys_contained_in::call(innersortkeys, &inner.pathkeys));

        let mut sort_path = make_dummy_sort_path(&inner);
        cost_sort_owned(
            &mut sort_path,
            innersortkeys,
            inner.disabled_nodes,
            inner.total_cost,
            inner_path_rows,
            outer_pathtarget_width(&inner),
            0.0,
            work_mem(),
            -1.0,
        );
        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * innerstartsel;
        inner_run_cost =
            (sort_path.total_cost - sort_path.startup_cost) * (innerendsel - innerstartsel);
    } else {
        disabled_nodes += inner.disabled_nodes;
        startup_cost += inner.startup_cost;
        startup_cost += (inner.total_cost - inner.startup_cost) * innerstartsel;
        inner_run_cost = (inner.total_cost - inner.startup_cost) * (innerendsel - innerstartsel);
    }

    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost + inner_run_cost;
    workspace.run_cost = run_cost;
    workspace.inner_run_cost = inner_run_cost;
    workspace.outer_rows = outer_rows;
    workspace.inner_rows = inner_rows;
    workspace.outer_skip_rows = outer_skip_rows;
    workspace.inner_skip_rows = inner_skip_rows;
    Ok(workspace)
}

/// Build a dummy `Path` for `cost_sort` (C uses a stack `Path`).
fn make_dummy_sort_path(template: &Path) -> Path {
    let mut p = template.clone();
    p.pathkeys = Vec::new();
    p
}

fn outer_pathtarget_width(p: &Path) -> i32 {
    p.pathtarget
        .as_ref()
        .expect("mergejoin: input path pathtarget must be set")
        .width
}

/// `approx_tuple_count` (costsize.c:5303).
fn approx_tuple_count<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_id: PathId,
    inner_id: PathId,
    quals: &[RinfoId],
) -> f64 {
    let outer = root.path(outer_id).base();
    let inner = root.path(inner_id).base();
    let outer_tuples = outer.rows;
    let inner_tuples = inner.rows;
    let mut selec: Selectivity = 1.0;

    let sjinfo = cz::init_dummy_sjinfo::call(root, outer.parent, inner.parent);

    for &id in quals {
        let clause = root.rinfo(id).clause;
        selec *= cz::clause_selectivity::call(run, root, clause, 0, JOIN_INNER as i32, Some(&sjinfo));
    }

    clamp_row_est(selec * outer_tuples * inner_tuples)
}

/// `final_cost_mergejoin` (costsize.c:3792) — fills a `MergePath` (by `PathId`).
pub fn final_cost_mergejoin<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    let (
        outer_id,
        inner_id,
        jointype,
        joinrestrictinfo,
        mergeclauses,
        innersortkeys_empty,
        parent,
        parallel_workers,
        param_rows,
    ) = {
        let mp = expect_merge(root, path_id);
        (
            mp.jpath.outerjoinpath.expect("final_cost_mergejoin: outer"),
            mp.jpath.innerjoinpath.expect("final_cost_mergejoin: inner"),
            mp.jpath.jointype,
            mp.jpath.joinrestrictinfo.clone(),
            mp.path_mergeclauses.clone(),
            mp.innersortkeys.is_empty(),
            mp.jpath.path.parent,
            mp.jpath.path.parallel_workers,
            mp.jpath.path.param_info.as_deref().map(|pi| pi.ppi_rows),
        )
    };

    let mut inner_path_rows = root.path(inner_id).base().rows;
    let mut startup_cost: Cost = workspace.startup_cost;
    let mut run_cost: Cost = workspace.run_cost;
    let inner_run_cost: Cost = workspace.inner_run_cost;
    let outer_rows = workspace.outer_rows;
    let inner_rows = workspace.inner_rows;
    let outer_skip_rows = workspace.outer_skip_rows;
    let inner_skip_rows = workspace.inner_skip_rows;
    let cpu_per_tuple: Cost;
    let bare_inner_cost: Cost;
    let mat_inner_cost: Cost;
    let mergejointuples: f64;
    let rescannedtuples: f64;
    let rescanratio: f64;

    set_join_disabled(root, path_id, workspace.disabled_nodes);

    if inner_path_rows <= 0.0 {
        inner_path_rows = 1.0;
    }

    let mut rows = match param_rows {
        Some(r) => r,
        None => root.rel(parent).rows,
    };
    if parallel_workers > 0 {
        let pd = get_parallel_divisor(root.path(path_id).base());
        rows = clamp_row_est(rows / pd);
    }
    set_join_rows(root, path_id, rows);

    let merge_qual_cost = cost_qual_eval_rinfos(root, &mergeclauses);
    let mut qp_qual_cost = cost_qual_eval_rinfos(root, &joinrestrictinfo);
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    let skip_mark_restore = (jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique)
        && (joinrestrictinfo.len() == mergeclauses.len());
    if let PathNode::MergePath(mp) = root.path_mut(path_id) {
        mp.skip_mark_restore = skip_mark_restore;
    }

    mergejointuples = approx_tuple_count(run, root, outer_id, inner_id, &mergeclauses);

    let outer_is_unique = matches!(root.path(outer_id), PathNode::UniquePath(_));
    if outer_is_unique || skip_mark_restore {
        rescannedtuples = 0.0;
    } else {
        let r = mergejointuples - inner_path_rows;
        rescannedtuples = if r < 0.0 { 0.0 } else { r };
    }

    rescanratio = 1.0 + (rescannedtuples / inner_rows);

    bare_inner_cost = inner_run_cost * rescanratio;
    mat_inner_cost = inner_run_cost + cpu_operator_cost() * inner_rows * rescanratio;

    let materialize_inner = if skip_mark_restore {
        false
    } else if ENABLE_MATERIAL && mat_inner_cost < bare_inner_cost {
        true
    } else if innersortkeys_empty && !cz::exec_supports_mark_restore::call(root, inner_id) {
        true
    } else if ENABLE_MATERIAL
        && !innersortkeys_empty
        && relation_byte_size(inner_path_rows, inner_pathtarget_width(root, inner_id))
            > work_mem() as f64 * 1024.0
    {
        true
    } else {
        false
    };
    if let PathNode::MergePath(mp) = root.path_mut(path_id) {
        mp.materialize_inner = materialize_inner;
    }

    if materialize_inner {
        run_cost += mat_inner_cost;
    } else {
        run_cost += bare_inner_cost;
    }

    startup_cost += merge_qual_cost.startup;
    startup_cost += merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost += merge_qual_cost.per_tuple
        * ((outer_rows - outer_skip_rows) + (inner_rows - inner_skip_rows) * rescanratio);

    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    let path_rows = root.path(path_id).base().rows;
    let (pt_startup, pt_per_tuple) = join_pathtarget_cost(root, path_id, "final_cost_mergejoin");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * path_rows;

    let p = root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

fn inner_pathtarget_width(root: &PlannerInfo, inner_id: PathId) -> i32 {
    root.path(inner_id)
        .base()
        .pathtarget
        .as_ref()
        .expect("final_cost_mergejoin: inner pathtarget must be set")
        .width
}

/* ==========================================================================
 * hash join (costsize.c:4159-4523)
 * ========================================================================== */

/// `initial_cost_hashjoin` (costsize.c:4159) — returns the filled workspace.
pub fn initial_cost_hashjoin(
    root: &PlannerInfo,
    _jointype: JoinType,
    hashclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
    _extra: &JoinPathExtraData,
    parallel_hash: bool,
) -> JoinCostWorkspace {
    let mut workspace = JoinCostWorkspace_default();

    let mut disabled_nodes: i32;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let outer = root.path(outer_path).base().clone();
    let inner = root.path(inner_path).base().clone();
    let outer_path_rows = outer.rows;
    let inner_path_rows = inner.rows;
    let mut inner_path_rows_total = inner_path_rows;
    let num_hashclauses = hashclauses.len() as i32;
    let numbuckets: i32;
    let numbatches: i32;

    disabled_nodes = if ENABLE_HASHJOIN { 0 } else { 1 };
    disabled_nodes += inner.disabled_nodes;
    disabled_nodes += outer.disabled_nodes;

    startup_cost += outer.startup_cost;
    run_cost += outer.total_cost - outer.startup_cost;
    startup_cost += inner.total_cost;

    let cpu_op = cpu_operator_cost();
    let cpu_tup = cpu_tuple_cost();
    startup_cost += (cpu_op * num_hashclauses as f64 + cpu_tup) * inner_path_rows;
    run_cost += cpu_op * num_hashclauses as f64 * outer_path_rows;

    if parallel_hash {
        inner_path_rows_total *= get_parallel_divisor(&inner);
    }

    let hts = cz::exec_choose_hash_table_size::call(
        inner_path_rows_total,
        outer_pathtarget_width(&inner),
        true,
        parallel_hash,
        outer.parallel_workers,
    );
    numbuckets = hts.numbuckets;
    numbatches = hts.numbatches;

    if numbatches > 1 {
        let outerpages = page_size(outer_path_rows, outer_pathtarget_width(&outer));
        let innerpages = page_size(inner_path_rows, outer_pathtarget_width(&inner));
        let seq_pc = crate::seq_page_cost();
        startup_cost += seq_pc * innerpages;
        run_cost += seq_pc * (innerpages + 2.0 * outerpages);
    }

    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost;
    workspace.run_cost = run_cost;
    workspace.numbuckets = numbuckets;
    workspace.numbatches = numbatches;
    workspace.inner_rows_total = inner_path_rows_total;
    workspace
}

/// `final_cost_hashjoin` (costsize.c:4286) — fills a `HashPath` (by `PathId`).
pub fn final_cost_hashjoin<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    let (outer_id, inner_id, jointype, joinrestrictinfo, hashclauses, parent, parallel_workers, param_rows) = {
        let hp = expect_hash(root, path_id);
        (
            hp.jpath.outerjoinpath.expect("final_cost_hashjoin: outer"),
            hp.jpath.innerjoinpath.expect("final_cost_hashjoin: inner"),
            hp.jpath.jointype,
            hp.jpath.joinrestrictinfo.clone(),
            hp.path_hashclauses.clone(),
            hp.jpath.path.parent,
            hp.jpath.path.parallel_workers,
            hp.jpath.path.param_info.as_deref().map(|pi| pi.ppi_rows),
        )
    };

    let outer_path_rows = root.path(outer_id).base().rows;
    let inner_path_rows = root.path(inner_id).base().rows;
    let inner_path_rows_total = workspace.inner_rows_total;
    let mut startup_cost: Cost = workspace.startup_cost;
    let mut run_cost: Cost = workspace.run_cost;
    let numbuckets = workspace.numbuckets;
    let numbatches = workspace.numbatches;
    let cpu_per_tuple: Cost;
    let hashjointuples: f64;
    let virtualbuckets: f64;
    let mut innerbucketsize: Selectivity;
    let mut innermcvfreq: Selectivity;

    set_join_disabled(root, path_id, workspace.disabled_nodes);

    let mut rows = match param_rows {
        Some(r) => r,
        None => root.rel(parent).rows,
    };
    if parallel_workers > 0 {
        let pd = get_parallel_divisor(root.path(path_id).base());
        rows = clamp_row_est(rows / pd);
    }
    set_join_rows(root, path_id, rows);

    if let PathNode::HashPath(hp) = root.path_mut(path_id) {
        hp.num_batches = numbatches;
        hp.inner_rows_total = inner_path_rows_total;
    }

    virtualbuckets = numbuckets as f64 * numbatches as f64;

    let inner_is_unique = matches!(root.path(inner_id), PathNode::UniquePath(_));
    if inner_is_unique {
        innerbucketsize = 1.0 / virtualbuckets;
        innermcvfreq = 0.0;
    } else {
        innerbucketsize = 1.0;
        innermcvfreq = 1.0;

        let inner_parent_id = root.path(inner_id).base().parent;

        let (mv_bucketsize, otherclauses) =
            cz::estimate_multivariate_bucketsize::call(root, inner_parent_id, &hashclauses);
        innerbucketsize = Min(innerbucketsize, mv_bucketsize);

        let inner_relids = root.rel(inner_parent_id).relids.clone();
        for &restrict_id in &otherclauses {
            let thisbucketsize: Selectivity;
            let thismcvfreq: Selectivity;

            let (right_relids, left_relids, right_bucketsize, left_bucketsize, right_mcvfreq, left_mcvfreq, clause) = {
                let r = root.rinfo(restrict_id);
                (
                    r.right_relids.clone(),
                    r.left_relids.clone(),
                    r.right_bucketsize,
                    r.left_bucketsize,
                    r.right_mcvfreq,
                    r.left_mcvfreq,
                    r.clause,
                )
            };

            if bms_is_subset(&right_relids, &inner_relids) {
                if right_bucketsize < 0.0 {
                    let hashkey = get_rightop(root, clause);
                    let (mcv, bs) =
                        cz::estimate_hash_bucket_stats::call(run, root, hashkey, virtualbuckets)
                            .expect("estimate_hash_bucket_stats");
                    thismcvfreq = mcv;
                    thisbucketsize = bs;
                } else {
                    thisbucketsize = right_bucketsize;
                    thismcvfreq = right_mcvfreq;
                }
            } else {
                debug_assert!(bms_is_subset(&left_relids, &inner_relids));
                if left_bucketsize < 0.0 {
                    let hashkey = get_leftop(root, clause);
                    let (mcv, bs) =
                        cz::estimate_hash_bucket_stats::call(run, root, hashkey, virtualbuckets)
                            .expect("estimate_hash_bucket_stats");
                    thismcvfreq = mcv;
                    thisbucketsize = bs;
                } else {
                    thisbucketsize = left_bucketsize;
                    thismcvfreq = left_mcvfreq;
                }
            }

            if innerbucketsize > thisbucketsize {
                innerbucketsize = thisbucketsize;
            }
            if innermcvfreq > thismcvfreq {
                innermcvfreq = thismcvfreq;
            }
        }
    }

    if relation_byte_size(
        clamp_row_est(inner_path_rows * innermcvfreq),
        outer_pathtarget_width(root.path(inner_id).base()),
    ) > ps::get_hash_memory_limit::call()
    {
        startup_cost += disable_cost();
    }

    let hash_qual_cost = cost_qual_eval_rinfos(root, &hashclauses);
    let mut qp_qual_cost = cost_qual_eval_rinfos(root, &joinrestrictinfo);
    qp_qual_cost.startup -= hash_qual_cost.startup;
    qp_qual_cost.per_tuple -= hash_qual_cost.per_tuple;

    if jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique {
        let outer_matched_rows = rint(outer_path_rows * extra.semifactors.outer_match_frac);
        let inner_scan_frac = 2.0 / (extra.semifactors.match_count + 1.0);

        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_matched_rows
            * clamp_row_est(inner_path_rows * innerbucketsize * inner_scan_frac)
            * 0.5;

        run_cost += hash_qual_cost.per_tuple
            * (outer_path_rows - outer_matched_rows)
            * clamp_row_est(inner_path_rows / virtualbuckets)
            * 0.05;

        if jointype == JOIN_ANTI {
            hashjointuples = outer_path_rows - outer_matched_rows;
        } else {
            hashjointuples = outer_matched_rows;
        }
    } else {
        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_path_rows
            * clamp_row_est(inner_path_rows * innerbucketsize)
            * 0.5;

        hashjointuples = approx_tuple_count(run, root, outer_id, inner_id, &hashclauses);
    }

    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * hashjointuples;

    let path_rows = root.path(path_id).base().rows;
    let (pt_startup, pt_per_tuple) = join_pathtarget_cost(root, path_id, "final_cost_hashjoin");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * path_rows;

    let p = root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `get_leftop((Expr *) clause)` (nodeFuncs.c) — the OpExpr hashclause's left
/// operand (`linitial(op->args)`). Interns the inline `Expr` arg into the
/// planner node arena and returns its `NodeId` (what `estimate_hash_bucket_stats`'
/// `examine_variable` looks up stats for).
fn get_leftop(root: &mut PlannerInfo, clause: types_pathnodes::NodeId) -> types_pathnodes::NodeId {
    op_arg(root, clause, 0)
}
/// `get_rightop((Expr *) clause)` (nodeFuncs.c) — analogous, `lsecond(op->args)`.
fn get_rightop(root: &mut PlannerInfo, clause: types_pathnodes::NodeId) -> types_pathnodes::NodeId {
    op_arg(root, clause, 1)
}
/// Intern `op->args[i]` of the OpExpr `clause` into the node arena.
fn op_arg(
    root: &mut PlannerInfo,
    clause: types_pathnodes::NodeId,
    i: usize,
) -> types_pathnodes::NodeId {
    use types_nodes::primnodes::Expr;
    let arg = match root.node(clause) {
        Expr::OpExpr(op) => op
            .args
            .get(i)
            .cloned()
            .expect("get_leftop/get_rightop: OpExpr missing operand"),
        other => panic!(
            "get_leftop/get_rightop: hash clause is not an OpExpr: {:?}",
            core::mem::discriminant(other)
        ),
    };
    root.alloc_node(arg)
}

/* ==========================================================================
 * compute_semi_anti_join_factors (costsize.c:5106)
 * ========================================================================== */

/// `compute_semi_anti_join_factors`.
pub fn compute_semi_anti_join_factors<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) -> SemiAntiJoinFactors {
    // No precondition assert here: upstream `compute_semi_anti_join_factors`
    // (costsize.c:5114) has none, and the caller (joinpath.c:223) invokes it for
    // *any* `inner_unique` join — including a plain JOIN_INNER, whose dummy
    // SpecialJoinInfo carries `jointype == JOIN_INNER`. Asserting otherwise
    // (the prior `jointype == JOIN_SEMI || JOIN_ANTI || sjinfo.jointype !=
    // JOIN_INNER`) wrongly aborted unique inner joins in debug builds.

    // Build the joinquals list (drop pushed-down clauses for outer joins).
    let joinquals_ids: Vec<RinfoId> = if is_outer_join(jointype) {
        let joinrel_relids = root.rel(joinrel).relids.clone();
        restrictlist
            .iter()
            .copied()
            .filter(|&id| !rinfo_is_pushed_down(root, id, &joinrel_relids))
            .collect()
    } else {
        restrictlist.to_vec()
    };
    let joinqual_nodes: Vec<types_pathnodes::NodeId> =
        joinquals_ids.iter().map(|&id| root.rinfo(id).clause).collect();

    let jselec = cz::clauselist_selectivity::call(
        run,
        root,
        &joinqual_nodes,
        0,
        (if jointype == JOIN_ANTI { JOIN_ANTI } else { JOIN_SEMI }) as i32,
        Some(sjinfo),
    );

    let norm_sjinfo = cz::init_dummy_sjinfo::call(root, outerrel, innerrel);

    let nselec = cz::clauselist_selectivity::call(
        run,
        root,
        &joinqual_nodes,
        0,
        JOIN_INNER as i32,
        Some(&norm_sjinfo),
    );

    let avgmatch = if jselec > 0.0 {
        let m = nselec * root.rel(innerrel).rows / jselec;
        Max(1.0, m)
    } else {
        1.0
    };

    SemiAntiJoinFactors {
        outer_match_frac: jselec,
        match_count: avgmatch,
    }
}

/* ==========================================================================
 * join-size estimate family (costsize.c:5378-5633)
 * ========================================================================== */

/// `get_parameterized_baserel_size` (costsize.c:5378).
pub fn get_parameterized_baserel_size<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    param_clauses: &[RinfoId],
) -> f64 {
    let baserel_tuples = root.rel(rel).tuples;
    let baserel_relid = root.rel(rel).relid;
    let baserel_rows = root.rel(rel).rows;
    let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();

    let mut all_nodes: Vec<types_pathnodes::NodeId> =
        param_clauses.iter().map(|&id| root.rinfo(id).clause).collect();
    for &id in baserestrictinfo.iter() {
        all_nodes.push(root.rinfo(id).clause);
    }

    let mut nrows = baserel_tuples
        * cz::clauselist_selectivity::call(run, root, &all_nodes, baserel_relid as i32, JOIN_INNER as i32, None);
    nrows = clamp_row_est(nrows);
    if nrows > baserel_rows {
        nrows = baserel_rows;
    }
    nrows
}

/// `get_foreign_key_join_selectivity(root, outer_relids, inner_relids, sjinfo,
/// &restrictlist)` (costsize.c:5650). Considers each FK in `root->fkey_list`
/// that connects a baserel on one side of this join to a baserel on the other;
/// removes the FK-matched clauses from the restrictlist and multiplies in the
/// FK-implied selectivity. Returns `(fkselec, remaining_clause_handles)`.
///
/// Carrier note: the fabled `ForeignKeyOptInfo` is trimmed to the columns
/// `match_foreign_keys_to_quals` (initsplan.c, not yet ported) would fill. Until
/// that pass lands, `eclass`/`fk_eclass_member` stay `None` for every FK, so the
/// EC-derived clause-removal below never matches and every FK chickens out with
/// `removedlist` empty — leaving `fkselec = 1.0` and the restrictlist unchanged,
/// which is the correct faithful result for the un-matched state. The "loose
/// clause" (`fkinfo->rinfos[i]`) match path and the `nconst_ec`/ec_has_const
/// correction need carrier fields that don't exist yet; they panic loudly if a
/// future `match_foreign_keys_to_quals` ever makes them reachable, rather than
/// silently diverging.
pub fn get_foreign_key_join_selectivity(
    root: &PlannerInfo,
    outer_relids: &Relids,
    inner_relids: &Relids,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) -> (Selectivity, Vec<RinfoId>) {
    use backend_optimizer_util_relnode_seams as bms;

    const BMS_SINGLETON: i32 = 1;

    let mut fkselec: Selectivity = 1.0;
    let jointype = sjinfo.jointype;
    // worklist starts as the passed restrictlist; we only clone it on the first
    // clause removal (mirroring C's `worklist == *restrictlist` shallow-copy
    // guard). Track whether we've copied with `worklist_copied`.
    let mut worklist: Vec<RinfoId> = restrictlist.to_vec();

    for &fk_nid in root.fkey_list.iter() {
        let fkinfo = root.foreign_key(fk_nid).clone();

        // This FK is relevant only if it connects a baserel on one side to a
        // baserel on the other side.
        let ref_is_outer: bool;
        if bms::relids_is_member::call(fkinfo.con_relid as i32, outer_relids)
            && bms::relids_is_member::call(fkinfo.ref_relid as i32, inner_relids)
        {
            ref_is_outer = false;
        } else if bms::relids_is_member::call(fkinfo.ref_relid as i32, outer_relids)
            && bms::relids_is_member::call(fkinfo.con_relid as i32, inner_relids)
        {
            ref_is_outer = true;
        } else {
            continue;
        }

        // For semi/anti joins, if the referenced rel is outer, or the inner side
        // isn't a singleton, the FK doesn't help; punt.
        if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
            && (ref_is_outer || bms::relids_membership::call(inner_relids) != BMS_SINGLETON)
        {
            continue;
        }

        // Modify the restrictlist by removing clauses that match the FK.
        // `worklist` is already an owned copy of the passed restrictlist, so the
        // C shallow-copy-on-first-removal guard is implicit here.
        let mut removedlist: Vec<RinfoId> = Vec::new();
        let mut kept: Vec<RinfoId> = Vec::with_capacity(worklist.len());
        for &rid in worklist.iter() {
            let rinfo = root.rinfo(rid);
            let mut remove_it = false;
            for i in 0..fkinfo.nkeys as usize {
                if let Some(parent_ec) = rinfo.parent_ec {
                    // EC-derived clauses can only match by EC.
                    if fkinfo.eclass[i] == Some(parent_ec) {
                        remove_it = true;
                        break;
                    }
                } else {
                    // The "loose clause" path matches `rinfo` against
                    // `fkinfo->rinfos[i]`, a per-column clause list that the
                    // trimmed carrier does not model. It is only ever populated
                    // by the unported `match_foreign_keys_to_quals`; reaching
                    // here with that pass ported means the carrier must be
                    // widened first.
                    if !fkinfo.eclass.is_empty() && fkinfo.eclass[i].is_some() {
                        panic!(
                            "backend-optimizer-path-costsize::get_foreign_key_join_selectivity: \
                             FK loose-clause matching needs ForeignKeyOptInfo.rinfos, which the \
                             trimmed carrier does not model"
                        );
                    }
                }
            }
            if remove_it {
                removedlist.push(rid);
            } else {
                kept.push(rid);
            }
        }
        worklist = kept;

        // If we failed to remove all expected matching clauses, chicken out and
        // ignore this FK; put back any clauses we removed.
        //
        // The expected count is `nmatched_ec - nconst_ec + nmatched_ri`, fields
        // that `match_foreign_keys_to_quals` fills and the trimmed carrier omits.
        // With that pass unported, `removedlist` is always empty here, so the
        // first disjunct (`removedlist == NIL`) settles the check and we never
        // need the count. If a removal ever happens, the count is required.
        if removedlist.is_empty() {
            // chicken out: nothing removed, worklist unchanged.
            continue;
        } else {
            panic!(
                "backend-optimizer-path-costsize::get_foreign_key_join_selectivity: FK matched \
                 clauses but the chicken-out count (nmatched_ec - nconst_ec + nmatched_ri) and \
                 ec_has_const correction need ForeignKeyOptInfo fields the trimmed carrier omits"
            );
        }
    }

    clamp_probability(&mut fkselec);
    (fkselec, worklist)
}

/// `set_joinrel_size_estimates` (costsize.c:5427).
pub fn set_joinrel_size_estimates<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) {
    let outer_rows = root.rel(outer_rel).rows;
    let inner_rows = root.rel(inner_rel).rows;
    let rows = calc_joinrel_size_estimate(
        run,
        root,
        rel,
        outer_rel,
        inner_rel,
        outer_rows,
        inner_rows,
        sjinfo,
        restrictlist,
    );
    root.rel_mut(rel).rows = rows;
}

/// `get_parameterized_joinrel_size` (costsize.c:5459).
pub fn get_parameterized_joinrel_size<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    sjinfo: &SpecialJoinInfo,
    restrict_clauses: &[RinfoId],
) -> f64 {
    let outer = root.path(outer_path).base();
    let inner = root.path(inner_path).base();

    let mut nrows = calc_joinrel_size_estimate(
        run,
        root,
        rel,
        outer.parent,
        inner.parent,
        outer.rows,
        inner.rows,
        sjinfo,
        restrict_clauses,
    );
    if nrows > root.rel(rel).rows {
        nrows = root.rel(rel).rows;
    }
    nrows
}

/// `calc_joinrel_size_estimate` (costsize.c:5500).
pub fn calc_joinrel_size_estimate<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    outer_rows: f64,
    inner_rows: f64,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) -> f64 {
    let jointype = sjinfo.jointype;
    let jselec: Selectivity;
    let pselec: Selectivity;
    let mut nrows: f64;

    // FK matching: the ForeignKeyOptInfo structs are opaque in the fabled arena,
    // so the whole FK pass (and its clause-removal) is routed to the owner.
    let (fkselec, worklist) =
        cz::get_foreign_key_join_selectivity::call(root, outer_rel, inner_rel, sjinfo, restrictlist);

    if is_outer_join(jointype) {
        let joinrel_relids = root.rel(joinrel).relids.clone();
        let mut joinqual_nodes: Vec<types_pathnodes::NodeId> = Vec::new();
        let mut pushedqual_nodes: Vec<types_pathnodes::NodeId> = Vec::new();
        for &rid in &worklist {
            if rinfo_is_pushed_down(root, rid, &joinrel_relids) {
                pushedqual_nodes.push(root.rinfo(rid).clause);
            } else {
                joinqual_nodes.push(root.rinfo(rid).clause);
            }
        }

        jselec = cz::clauselist_selectivity::call(run, root, &joinqual_nodes, 0, jointype as i32, Some(sjinfo));
        pselec = cz::clauselist_selectivity::call(run, root, &pushedqual_nodes, 0, jointype as i32, Some(sjinfo));
    } else {
        let quals: Vec<types_pathnodes::NodeId> =
            worklist.iter().map(|&id| root.rinfo(id).clause).collect();
        jselec = cz::clauselist_selectivity::call(run, root, &quals, 0, jointype as i32, Some(sjinfo));
        pselec = 0.0;
    }

    match jointype {
        JOIN_INNER => {
            nrows = outer_rows * inner_rows * fkselec * jselec;
        }
        JOIN_LEFT => {
            nrows = outer_rows * inner_rows * fkselec * jselec;
            if nrows < outer_rows {
                nrows = outer_rows;
            }
            nrows *= pselec;
        }
        JOIN_FULL => {
            nrows = outer_rows * inner_rows * fkselec * jselec;
            if nrows < outer_rows {
                nrows = outer_rows;
            }
            if nrows < inner_rows {
                nrows = inner_rows;
            }
            nrows *= pselec;
        }
        JOIN_SEMI => {
            nrows = outer_rows * fkselec * jselec;
        }
        JOIN_ANTI => {
            nrows = outer_rows * (1.0 - fkselec * jselec);
            nrows *= pselec;
        }
        _ => panic!(
            "backend-optimizer-path-costsize::calc_joinrel_size_estimate: unrecognized join \
             type: {jointype}"
        ),
    }

    clamp_row_est(nrows)
}

/* --------------------------------------------------------------------------
 * Arena accessors.
 * ------------------------------------------------------------------------ */

fn expect_nest(root: &PlannerInfo, id: PathId) -> &NestPath {
    match root.path(id) {
        PathNode::NestPath(np) => np,
        _ => panic!("backend-optimizer-path-costsize: path is not a NestPath"),
    }
}
fn expect_merge(root: &PlannerInfo, id: PathId) -> &MergePath {
    match root.path(id) {
        PathNode::MergePath(mp) => mp,
        _ => panic!("backend-optimizer-path-costsize: path is not a MergePath"),
    }
}
fn expect_hash(root: &PlannerInfo, id: PathId) -> &HashPath {
    match root.path(id) {
        PathNode::HashPath(hp) => hp,
        _ => panic!("backend-optimizer-path-costsize: path is not a HashPath"),
    }
}

fn set_join_disabled(root: &mut PlannerInfo, id: PathId, n: i32) {
    root.path_mut(id).base_mut().disabled_nodes = n;
}
fn set_join_rows(root: &mut PlannerInfo, id: PathId, rows: f64) {
    root.path_mut(id).base_mut().rows = rows;
}
fn join_pathtarget_cost(root: &PlannerInfo, id: PathId, who: &str) -> (Cost, Cost) {
    let pt = root
        .path(id)
        .base()
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| panic!("backend-optimizer-path-costsize::{who}: pathtarget must be set"));
    (pt.cost.startup, pt.cost.per_tuple)
}

/// Construct a default `JoinCostWorkspace` (the type is `Copy` but has no
/// `Default`; build it field-by-field as C leaves it uninitialized then fills).
#[allow(non_snake_case)]
fn JoinCostWorkspace_default() -> JoinCostWorkspace {
    JoinCostWorkspace {
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        run_cost: 0.0,
        inner_run_cost: 0.0,
        inner_rescan_run_cost: 0.0,
        outer_rows: 0.0,
        inner_rows: 0.0,
        outer_skip_rows: 0.0,
        inner_skip_rows: 0.0,
        numbuckets: 0,
        numbatches: 0,
        inner_rows_total: 0.0,
    }
}

#[allow(unused_imports)]
use types_pathnodes::Relids as _Relids;
