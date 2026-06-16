//! Append-relation size/path machinery:
//! `set_append_rel_size` (allpaths.c:955), `set_append_rel_pathlist` (1250),
//! `add_paths_to_append_rel` (1320), `generate_orderedappend_paths` (1748),
//! `get_cheapest_parameterized_child_path` (2047), `accumulate_append_subpath`
//! (2135), `get_singleton_append_subpath` (2180).

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{Index, InvalidAttrNumber};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    BackwardScanDirection, ForwardScanDirection, PathId, PathKey, PathNode, PlannerInfo, RelId,
    Relids, RELOPT_BASEREL,
};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_path_costsize_seams as costsize;
use backend_optimizer_util_appendinfo_seams as appendinfo;

use crate::{
    enable_partitionwise_join, max_parallel_workers_per_gather, path_req_outer, set_dummy_rel_pathlist,
    set_rel_size, set_rel_pathlist, set_rel_consider_parallel, parallel_mode_ok, enable_parallel_append,
};

/// `RELKIND_PARTITIONED_TABLE`.
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

/// `set_append_rel_size` (allpaths.c:955) — size estimates for an "append
/// relation" (the parent of an inheritance/partition tree). Computes whole-rel
/// tuples/rows/width by accumulating over the live children.
pub fn set_append_rel_size<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    let parent_rtindex = rti;

    // Guard against stack overflow due to overly deep inheritance.
    backend_tcop_postgres_seams::check_stack_depth::call()?;

    debug_assert!(crate::is_simple_rel(root.rel(rel)));

    // Partitioned baserel with a whole-row-Var-free targetlist: enable
    // partitionwise join consideration.
    if enable_partitionwise_join()
        && root.rel(rel).reloptkind == RELOPT_BASEREL
        && crate::rte::rte_relkind::call(run, root, rti) == RELKIND_PARTITIONED_TABLE
    {
        let min_attr = root.rel(rel).min_attr;
        let idx = (InvalidAttrNumber - min_attr) as usize;
        let whole_row_empty = root
            .rel(rel)
            .attr_needed
            .get(idx)
            .map(|s| s.is_none())
            .unwrap_or(true);
        if whole_row_empty {
            root.rel_mut(rel).consider_partitionwise_join = true;
        }
    }

    // Initialize accumulators.
    let mut has_live_children = false;
    let mut parent_tuples: f64 = 0.0;
    let mut parent_rows: f64 = 0.0;
    let mut parent_size: f64 = 0.0;
    let nattrs = (root.rel(rel).max_attr - root.rel(rel).min_attr + 1) as usize;
    let mut parent_attrsizes: Vec<f64> = alloc::vec![0.0; nattrs];

    // Walk the append_rel_list for children of this parent.
    let appinfos: Vec<types_pathnodes::AppendRelInfo> = root
        .append_rel_list
        .iter()
        .filter(|a| a.parent_relid == parent_rtindex)
        .cloned()
        .collect();

    for appinfo in &appinfos {
        let child_rtindex = appinfo.child_relid;
        let childrel = bms::find_base_rel::call(root, child_rtindex as i32);
        debug_assert!(root.rel(childrel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL);

        // We may already have proven the child dummy.
        if crate::is_dummy_rel(root, childrel) {
            continue;
        }

        // Constraint exclusion on the child (its baserestrictinfo was already
        // substituted when its RelOptInfo was built).
        if crate::relation_excluded_by_constraints(run, root, childrel, child_rtindex) {
            set_dummy_rel_pathlist(root, run, childrel)?;
            continue;
        }

        // Copy the parent's join quals to the child with Var substitution,
        // skipping quals from above outer joins that can null this rel.
        let nulling = bms::relids_copy::call(&root.rel(rel).nulling_relids);
        let parent_joininfo = root.rel(rel).joininfo.clone();
        let mut kept: Vec<types_pathnodes::RinfoId> = Vec::new();
        for rinfo in parent_joininfo {
            let clause_relids = root.rinfo(rinfo).clause_relids.clone();
            if !bms::relids_overlap::call(&clause_relids, &nulling) {
                kept.push(rinfo);
            }
        }
        let childrinfos =
            appendinfo::adjust_appendrel_attrs_restrictlist::call(root, &kept, core::slice::from_ref(appinfo))?;
        root.rel_mut(childrel).joininfo = childrinfos;

        // Translate the parent's reltarget exprs to the child (1-to-1).
        let parent_exprs = root
            .rel(rel)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.clone())
            .unwrap_or_default();
        let mut child_exprs: Vec<types_pathnodes::NodeId> = Vec::with_capacity(parent_exprs.len());
        for nid in &parent_exprs {
            let parent_expr = root.node(*nid).clone();
            let translated = backend_optimizer_path_equivclass_ext_seams::adjust_appendrel_attrs::call(
                root,
                parent_expr,
                alloc::vec![childrel],
            )?;
            let new_nid = root.alloc_node(translated);
            child_exprs.push(new_nid);
        }
        if let Some(t) = root.rel_mut(childrel).reltarget.as_mut() {
            t.exprs = child_exprs.clone();
        }

        // Make child entries in the EquivalenceClasses if needed.
        let need_ec = root.rel(rel).has_eclass_joins
            || backend_optimizer_path_pathkeys::has_useful_pathkeys(root, root.rel(rel));
        if need_ec {
            backend_optimizer_path_equivclass_seams::add_child_rel_equivalences::call(
                root,
                childrel, // appinfo carried by relid handle: see seam contract
                rel,
                childrel,
            )?;
        }
        let parent_has_ecj = root.rel(rel).has_eclass_joins;
        root.rel_mut(childrel).has_eclass_joins = parent_has_ecj;

        // (attr_needed for the child's variables is intentionally left empty;
        // attr_needed is only examined for base rels, not otherrels.)

        // Propagate partitionwise-join consideration to the child.
        if root.rel(rel).consider_partitionwise_join {
            root.rel_mut(childrel).consider_partitionwise_join = true;
        }

        // Parallel-safety for the child (only if the appendrel is still safe).
        if parallel_mode_ok(root) && root.rel(rel).consider_parallel {
            set_rel_consider_parallel(run, root, childrel, child_rtindex);
        }

        // Compute the child's size.
        set_rel_size(mcx, run, root, childrel, child_rtindex)?;

        // Constraint exclusion may have detected a contradiction in a child
        // subquery even though we didn't prove one above.
        if crate::is_dummy_rel(root, childrel) {
            continue;
        }

        // We have at least one live child.
        has_live_children = true;

        // If any live child is not parallel-safe, the whole appendrel isn't.
        if !root.rel(childrel).consider_parallel {
            root.rel_mut(rel).consider_parallel = false;
        }

        // Accumulate size information.
        debug_assert!(root.rel(childrel).rows > 0.0);
        let child_tuples = root.rel(childrel).tuples;
        let child_rows = root.rel(childrel).rows;
        let child_width = root
            .rel(childrel)
            .reltarget
            .as_ref()
            .map(|t| t.width)
            .unwrap_or(0);
        parent_tuples += child_tuples;
        parent_rows += child_rows;
        parent_size += child_width as f64 * child_rows;

        // Per-column estimates: parent var -> child expr, 1-to-1.
        let parent_exprs2 = root
            .rel(rel)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.clone())
            .unwrap_or_default();
        let child_exprs2 = root
            .rel(childrel)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.clone())
            .unwrap_or_default();
        let rel_min_attr = root.rel(rel).min_attr;
        let child_min_attr = root.rel(childrel).min_attr;
        let child_relid = root.rel(childrel).relid;
        let child_attr_widths = root.rel(childrel).attr_widths.clone();

        for (pv, cv) in parent_exprs2.iter().zip(child_exprs2.iter()) {
            if let Expr::Var(parentvar) = root.node(*pv).clone() {
                if parentvar.varno == parent_rtindex as i32 {
                    let pndx = (parentvar.varattno - rel_min_attr) as usize;
                    let mut width: i32 = 0;
                    if let Expr::Var(childvar) = root.node(*cv).clone() {
                        if childvar.varno == child_relid as i32 {
                            let cndx = (childvar.varattno - child_min_attr) as usize;
                            width = child_attr_widths.get(cndx).copied().unwrap_or(0);
                        }
                    }
                    if width <= 0 {
                        let t = costsize::expr_type::call(root, *cv);
                        let tm = costsize::expr_typmod::call(root, *cv);
                        width = costsize::get_typavgwidth::call(t, tm);
                    }
                    debug_assert!(width > 0);
                    if pndx < parent_attrsizes.len() {
                        parent_attrsizes[pndx] += width as f64 * child_rows;
                    }
                }
            }
        }
    }

    if has_live_children {
        debug_assert!(parent_rows > 0.0);
        root.rel_mut(rel).tuples = parent_tuples;
        root.rel_mut(rel).rows = parent_rows;
        let new_width = (parent_size / parent_rows).round() as i32;
        if let Some(t) = root.rel_mut(rel).reltarget.as_mut() {
            t.width = new_width;
        }
        for i in 0..nattrs {
            let w = (parent_attrsizes[i] / parent_rows).round() as i32;
            if i < root.rel(rel).attr_widths.len() {
                root.rel_mut(rel).attr_widths[i] = w;
            }
        }
        // rel->pages stays zero (avoid double-counting in total_table_pages).
    } else {
        // All children excluded by constraints: mark the whole appendrel dummy.
        set_dummy_rel_pathlist(root, run, rel)?;
    }
    Ok(())
}

/// `set_append_rel_pathlist` (allpaths.c:1250) — access paths for an append
/// relation: generate child paths, remember the non-dummy children, then build
/// the parent's append paths.
pub fn set_append_rel_pathlist<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    let parent_rtindex = rti;
    let mut live_childrels: Vec<RelId> = Vec::new();

    let appinfos: Vec<(i32,)> = root
        .append_rel_list
        .iter()
        .filter(|a| a.parent_relid == parent_rtindex)
        .map(|a| (a.child_relid as i32,))
        .collect();

    for (child_rtindex,) in appinfos {
        let childrel = root.simple_rel_array[child_rtindex as usize]
            .expect("set_append_rel_pathlist: child rel slot is empty");

        // Propagate parallel-unsafety down to the child if the parent became
        // unsafe after visiting this child in set_append_rel_size.
        if !root.rel(rel).consider_parallel {
            root.rel_mut(childrel).consider_parallel = false;
        }

        // Compute the child's access paths.
        set_rel_pathlist(mcx, run, root, childrel, child_rtindex as Index)?;

        if crate::is_dummy_rel(root, childrel) {
            continue;
        }
        live_childrels.push(childrel);
    }

    add_paths_to_append_rel(mcx, root, run, rel, &live_childrels)?;
    Ok(())
}

/// `add_paths_to_append_rel` (allpaths.c:1320) — generate Append/MergeAppend
/// paths for the given append relation from its non-dummy child rels.
pub fn add_paths_to_append_rel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    live_childrels: &[RelId],
) -> PgResult<()> {
    let mut subpaths: Vec<PathId> = Vec::new();
    let mut subpaths_valid = true;
    let mut startup_subpaths: Vec<PathId> = Vec::new();
    let mut startup_subpaths_valid = true;
    let mut partial_subpaths: Vec<PathId> = Vec::new();
    let mut pa_partial_subpaths: Vec<PathId> = Vec::new();
    let mut pa_nonpartial_subpaths: Vec<PathId> = Vec::new();
    let mut partial_subpaths_valid = true;
    let mut all_child_pathkeys: Vec<Vec<PathKey>> = Vec::new();
    let mut all_child_outers: Vec<Relids> = Vec::new();
    let mut partial_rows: f64 = -1.0;

    // If appropriate, consider parallel append.
    let mut pa_subpaths_valid = enable_parallel_append() && root.rel(rel).consider_parallel;
    let consider_startup = root.rel(rel).consider_startup;
    let tuple_fraction = root.tuple_fraction;

    for &childrel in live_childrels {
        let mut cheapest_partial_path: Option<PathId> = None;

        // Unparameterized cheapest-total path -> the unparameterized Append.
        let ct = root.rel(childrel).cheapest_total_path;
        let ct_unparam = ct
            .map(|p| root.path(p).base().param_info.is_none())
            .unwrap_or(false);
        if !root.rel(childrel).pathlist.is_empty() && ct_unparam {
            accumulate_append_subpath(root, ct.unwrap(), &mut subpaths, None);
        } else {
            subpaths_valid = false;
        }

        // Cheapest startup paths (when considering cheap startup plans).
        if consider_startup && root.rel(childrel).cheapest_startup_path.is_some() {
            let cheapest_path = if tuple_fraction > 0.0 {
                get_cheapest_fractional_path(root, childrel, tuple_fraction)
            } else {
                root.rel(childrel).cheapest_startup_path.unwrap()
            };
            debug_assert!(root.path(cheapest_path).base().param_info.is_none());
            accumulate_append_subpath(root, cheapest_path, &mut startup_subpaths, None);
        } else {
            startup_subpaths_valid = false;
        }

        // Partial plan.
        if !root.rel(childrel).partial_pathlist.is_empty() {
            let cpp = root.rel(childrel).partial_pathlist[0];
            cheapest_partial_path = Some(cpp);
            accumulate_append_subpath(root, cpp, &mut partial_subpaths, None);
        } else {
            partial_subpaths_valid = false;
        }

        // Parallel append mixing partial and non-partial paths.
        if pa_subpaths_valid {
            let nppath = get_cheapest_parallel_safe_total_inner(root, childrel);
            if cheapest_partial_path.is_none() && nppath.is_none() {
                // Neither partial nor parallel-safe: forget it.
                pa_subpaths_valid = false;
            } else if nppath.is_none()
                || (cheapest_partial_path.is_some()
                    && root.path(cheapest_partial_path.unwrap()).base().total_cost
                        < root.path(nppath.unwrap()).base().total_cost)
            {
                // Partial path is cheaper or the only option.
                let cpp = cheapest_partial_path.unwrap();
                accumulate_append_subpath(
                    root,
                    cpp,
                    &mut pa_partial_subpaths,
                    Some(&mut pa_nonpartial_subpaths),
                );
            } else {
                // Only a non-partial path, or it's cheaper.
                accumulate_append_subpath(root, nppath.unwrap(), &mut pa_nonpartial_subpaths, None);
            }
        }

        // Collect all available orderings and parameterizations of the children.
        let child_paths: Vec<PathId> = root.rel(childrel).pathlist.clone();
        for childpath in child_paths {
            let childkeys = root.path(childpath).base().pathkeys.clone();
            let childouter = path_req_outer(root, childpath);

            if !childkeys.is_empty() {
                let mut found = false;
                for existing in &all_child_pathkeys {
                    if backend_optimizer_path_pathkeys::compare_pathkeys(existing, &childkeys)
                        == backend_optimizer_util_pathnode_seams::PathKeysComparison::Equal
                    {
                        found = true;
                        break;
                    }
                }
                if !found {
                    all_child_pathkeys.push(childkeys);
                }
            }

            if childouter.is_some() {
                let mut found = false;
                for existing in &all_child_outers {
                    if bms::relids_equal::call(existing, &childouter) {
                        found = true;
                        break;
                    }
                }
                if !found {
                    all_child_outers.push(childouter);
                }
            }
        }
    }

    // Unparameterized unordered Append.
    if subpaths_valid {
        let p = pathnode::create_append_path::call(
            root, run, true, rel, subpaths.clone(), Vec::new(), Vec::new(), &None, 0, false, -1.0,
        )?;
        pathnode::add_path::call(root, rel, p)?;
    }

    // AppendPath for the cheap startup paths.
    if startup_subpaths_valid {
        let p = pathnode::create_append_path::call(
            root, run, true, rel, startup_subpaths, Vec::new(), Vec::new(), &None, 0, false, -1.0,
        )?;
        pathnode::add_path::call(root, rel, p)?;
    }

    // Append of unordered unparameterized partial paths (parallel-aware if able).
    if partial_subpaths_valid && !partial_subpaths.is_empty() {
        let mut parallel_workers = 0;
        for &p in &partial_subpaths {
            parallel_workers = parallel_workers.max(root.path(p).base().parallel_workers);
        }
        debug_assert!(parallel_workers > 0);

        if enable_parallel_append() {
            parallel_workers = parallel_workers
                .max(pg_leftmost_one_pos32(live_childrels.len() as u32) + 1);
            parallel_workers = parallel_workers.min(max_parallel_workers_per_gather());
        }
        debug_assert!(parallel_workers > 0);

        let appendpath = pathnode::create_append_path::call(
            root,
            run,
            true,
            rel,
            Vec::new(),
            partial_subpaths,
            Vec::new(),
            &None,
            parallel_workers,
            enable_parallel_append(),
            -1.0,
        )?;
        partial_rows = root.path(appendpath).base().rows;
        pathnode::add_partial_path::call(root, rel, appendpath)?;
    }

    // Parallel-aware append mixing partial and non-partial paths.
    if pa_subpaths_valid && !pa_nonpartial_subpaths.is_empty() {
        let mut parallel_workers = 0;
        for &p in &pa_partial_subpaths {
            parallel_workers = parallel_workers.max(root.path(p).base().parallel_workers);
        }
        parallel_workers =
            parallel_workers.max(pg_leftmost_one_pos32(live_childrels.len() as u32) + 1);
        parallel_workers = parallel_workers.min(max_parallel_workers_per_gather());
        debug_assert!(parallel_workers > 0);

        let appendpath = pathnode::create_append_path::call(
            root,
            run,
            true,
            rel,
            pa_nonpartial_subpaths,
            pa_partial_subpaths,
            Vec::new(),
            &None,
            parallel_workers,
            true,
            partial_rows,
        )?;
        pathnode::add_partial_path::call(root, rel, appendpath)?;
    }

    // Unparameterized ordered append paths.
    if subpaths_valid {
        generate_orderedappend_paths(mcx, root, run, rel, live_childrels, &all_child_pathkeys)?;
    }

    // Append paths for each child parameterization.
    let outers = all_child_outers.clone();
    for required_outer in outers {
        let mut sp: Vec<PathId> = Vec::new();
        let mut sp_valid = true;
        for &childrel in live_childrels {
            if root.rel(childrel).pathlist.is_empty() {
                sp_valid = false;
                break;
            }
            match get_cheapest_parameterized_child_path(root, childrel, &required_outer)? {
                Some(subpath) => accumulate_append_subpath(root, subpath, &mut sp, None),
                None => {
                    sp_valid = false;
                    break;
                }
            }
        }
        if sp_valid {
            let p = pathnode::create_append_path::call(
                root, run, true, rel, sp, Vec::new(), Vec::new(), &required_outer, 0, false, -1.0,
            )?;
            pathnode::add_path::call(root, rel, p)?;
        }
    }

    // Single-child appendrel: also consider ordered partial paths.
    if live_childrels.len() == 1 {
        let childrel = live_childrels[0];
        let partials: Vec<PathId> = root.rel(childrel).partial_pathlist.clone();
        // skip the cheapest partial path (index 0; already used above)
        for path in partials.into_iter().skip(1) {
            if root.path(path).base().pathkeys.is_empty() {
                continue;
            }
            let workers = root.path(path).base().parallel_workers;
            let appendpath = pathnode::create_append_path::call(
                root,
                run,
                true,
                rel,
                Vec::new(),
                alloc::vec![path],
                Vec::new(),
                &None,
                workers,
                true,
                partial_rows,
            )?;
            pathnode::add_partial_path::call(root, rel, appendpath)?;
        }
    }
    Ok(())
}

/// `get_cheapest_parameterized_child_path` (allpaths.c:2047) — the cheapest path
/// for `rel` with exactly the requested parameterization (reparameterizing an
/// existing path if needed). `None` if none can be made.
pub fn get_cheapest_parameterized_child_path(
    root: &mut PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> PgResult<Option<PathId>> {
    // Cheapest existing path with no more than the needed parameterization.
    let cheapest0 = backend_optimizer_path_pathkeys::get_cheapest_path_for_pathkeys(
        root,
        &root.rel(rel).pathlist.clone(),
        &[],
        required_outer,
        types_pathnodes::optimizer_plan::CostSelector::TOTAL_COST,
        false,
    );
    let cheapest0 = cheapest0.expect("get_cheapest_parameterized_child_path: no candidate");
    if bms::relids_equal::call(&path_req_outer(root, cheapest0), required_outer) {
        return Ok(Some(cheapest0));
    }

    // Otherwise reparameterize candidates and find the cheapest.
    let mut cheapest: Option<PathId> = None;
    let pathlist: Vec<PathId> = root.rel(rel).pathlist.clone();
    for mut path in pathlist {
        // Can't use it if it needs more than the requested parameterization.
        if !bms::relids_is_subset::call(&path_req_outer(root, path), required_outer) {
            continue;
        }
        // Reparameterization only increases cost.
        if let Some(c) = cheapest {
            if pathnode::compare_path_costs::call(
                root,
                c,
                path,
                types_pathnodes::optimizer_plan::CostSelector::TOTAL_COST,
            ) <= 0
            {
                continue;
            }
        }
        // Reparameterize if needed, then recheck cost.
        if !bms::relids_equal::call(&path_req_outer(root, path), required_outer) {
            match pathnode::reparameterize_path::call(root, path, required_outer, 1.0)? {
                Some(rp) => path = rp,
                None => continue, // failed to reparameterize
            }
            debug_assert!(bms::relids_equal::call(&path_req_outer(root, path), required_outer));
            if let Some(c) = cheapest {
                if pathnode::compare_path_costs::call(
                    root,
                    c,
                    path,
                    types_pathnodes::optimizer_plan::CostSelector::TOTAL_COST,
                ) <= 0
                {
                    continue;
                }
            }
        }
        cheapest = Some(path);
    }
    Ok(cheapest)
}

/// `accumulate_append_subpath` (allpaths.c:2135) — add a subpath to an
/// Append/MergeAppend subpath list, flattening child Append/MergeAppends.
pub fn accumulate_append_subpath(
    root: &PlannerInfo,
    path: PathId,
    subpaths: &mut Vec<PathId>,
    special_subpaths: Option<&mut Vec<PathId>>,
) {
    match root.path(path) {
        PathNode::AppendPath(apath) => {
            if !apath.path.parallel_aware || apath.first_partial_path == 0 {
                subpaths.extend(apath.subpaths.iter().copied());
                return;
            } else if let Some(special) = special_subpaths {
                // Split Parallel Append into partial and non-partial subpaths.
                let fpp = apath.first_partial_path as usize;
                subpaths.extend(apath.subpaths[fpp..].iter().copied());
                special.extend(apath.subpaths[..fpp].iter().copied());
                return;
            }
        }
        PathNode::MergeAppendPath(mpath) => {
            subpaths.extend(mpath.subpaths.iter().copied());
            return;
        }
        _ => {}
    }
    subpaths.push(path);
}

/// `get_singleton_append_subpath` (allpaths.c:2180) — the single subpath of an
/// Append/MergeAppend, else `path`. (`path` must not be parallel-aware.)
pub fn get_singleton_append_subpath(root: &PlannerInfo, path: PathId) -> PathId {
    debug_assert!(!root.path(path).base().parallel_aware);
    match root.path(path) {
        PathNode::AppendPath(apath) if apath.subpaths.len() == 1 => apath.subpaths[0],
        PathNode::MergeAppendPath(mpath) if mpath.subpaths.len() == 1 => mpath.subpaths[0],
        _ => path,
    }
}

/// `generate_orderedappend_paths` (allpaths.c:1748) — ordered Append/MergeAppend
/// paths for each interesting child ordering.
pub fn generate_orderedappend_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    live_childrels: &[RelId],
    all_child_pathkeys: &[Vec<PathKey>],
) -> PgResult<()> {
    let _ = mcx;
    let mut partition_pathkeys: Vec<PathKey> = Vec::new();
    let mut partition_pathkeys_desc: Vec<PathKey> = Vec::new();
    let mut partition_pathkeys_partial = true;
    let mut partition_pathkeys_desc_partial = true;

    // RANGE-ordered partitioned setups may permit a plain Append.
    if root.rel(rel).part_scheme.is_some()
        && crate::is_simple_rel(root.rel(rel))
        && partitions_are_ordered(root, rel)
    {
        let (pk, partial) = backend_optimizer_path_pathkeys::build_partition_pathkeys(
            root,
            rel,
            ForwardScanDirection,
        );
        partition_pathkeys = pk;
        partition_pathkeys_partial = partial;
        let (pkd, partial_d) = backend_optimizer_path_pathkeys::build_partition_pathkeys(
            root,
            rel,
            BackwardScanDirection,
        );
        partition_pathkeys_desc = pkd;
        partition_pathkeys_desc_partial = partial_d;
    }

    for pathkeys in all_child_pathkeys {
        let mut startup_subpaths: Vec<PathId> = Vec::new();
        let mut total_subpaths: Vec<PathId> = Vec::new();
        let mut fractional_subpaths: Vec<PathId> = Vec::new();
        let mut startup_neq_total = false;

        let mut match_partition_order = pathkeys_contained_in(pathkeys, &partition_pathkeys)
            || (!partition_pathkeys_partial
                && pathkeys_contained_in(&partition_pathkeys, pathkeys));

        let match_partition_order_desc = !match_partition_order
            && (pathkeys_contained_in(pathkeys, &partition_pathkeys_desc)
                || (!partition_pathkeys_desc_partial
                    && pathkeys_contained_in(&partition_pathkeys_desc, pathkeys)));

        // Iteration order: reverse for descending partition order.
        let (first_index, end_index, direction): (i64, i64, i64) = if match_partition_order_desc {
            match_partition_order = true;
            ((live_childrels.len() as i64) - 1, -1, -1)
        } else {
            (0, live_childrels.len() as i64, 1)
        };

        let mut i = first_index;
        while i != end_index {
            let childrel = live_childrels[i as usize];

            let mut cheapest_startup = backend_optimizer_path_pathkeys::get_cheapest_path_for_pathkeys(
                root,
                &root.rel(childrel).pathlist.clone(),
                pathkeys,
                &None,
                types_pathnodes::optimizer_plan::CostSelector::STARTUP_COST,
                false,
            );
            let mut cheapest_total = backend_optimizer_path_pathkeys::get_cheapest_path_for_pathkeys(
                root,
                &root.rel(childrel).pathlist.clone(),
                pathkeys,
                &None,
                types_pathnodes::optimizer_plan::CostSelector::TOTAL_COST,
                false,
            );

            if cheapest_startup.is_none() || cheapest_total.is_none() {
                let ct = root.rel(childrel).cheapest_total_path.expect("no unparam total path");
                cheapest_startup = Some(ct);
                cheapest_total = Some(ct);
                debug_assert!(root.path(ct).base().param_info.is_none());
            }
            let cheapest_startup = cheapest_startup.unwrap();
            let cheapest_total = cheapest_total.unwrap();

            let mut cheapest_fractional: Option<PathId> = None;
            if root.tuple_fraction > 0.0 {
                let mut path_fraction = root.tuple_fraction;
                debug_assert!(root.path(cheapest_total).base().rows > 0.0);
                if path_fraction >= 1.0 {
                    path_fraction /= root.path(cheapest_total).base().rows;
                }
                let cf = backend_optimizer_path_pathkeys::get_cheapest_fractional_path_for_pathkeys(
                    root,
                    &root.rel(childrel).pathlist.clone(),
                    pathkeys,
                    &None,
                    path_fraction,
                );
                cheapest_fractional = Some(cf.unwrap_or(cheapest_total));
            }

            if cheapest_startup != cheapest_total {
                startup_neq_total = true;
            }

            if match_partition_order {
                // Plain Append: cut out single-subpath child Append/MergeAppends.
                let cs = get_singleton_append_subpath(root, cheapest_startup);
                let ct = get_singleton_append_subpath(root, cheapest_total);
                startup_subpaths.push(cs);
                total_subpaths.push(ct);
                if let Some(cf) = cheapest_fractional {
                    let cf = get_singleton_append_subpath(root, cf);
                    fractional_subpaths.push(cf);
                }
            } else {
                accumulate_append_subpath(root, cheapest_startup, &mut startup_subpaths, None);
                accumulate_append_subpath(root, cheapest_total, &mut total_subpaths, None);
                if let Some(cf) = cheapest_fractional {
                    accumulate_append_subpath(root, cf, &mut fractional_subpaths, None);
                }
            }

            i += direction;
        }

        // Build the Append or MergeAppend paths.
        if match_partition_order {
            let p = pathnode::create_append_path::call(
                root, run, true, rel, startup_subpaths, Vec::new(), pathkeys.clone(), &None, 0, false, -1.0,
            )?;
            pathnode::add_path::call(root, rel, p)?;
            if startup_neq_total {
                let p = pathnode::create_append_path::call(
                    root, run, true, rel, total_subpaths, Vec::new(), pathkeys.clone(), &None, 0, false, -1.0,
                )?;
                pathnode::add_path::call(root, rel, p)?;
            }
            if !fractional_subpaths.is_empty() {
                let p = pathnode::create_append_path::call(
                    root, run, true, rel, fractional_subpaths, Vec::new(), pathkeys.clone(), &None, 0, false, -1.0,
                )?;
                pathnode::add_path::call(root, rel, p)?;
            }
        } else {
            let p = pathnode::create_merge_append_path::call(
                root, rel, startup_subpaths, pathkeys.clone(), &None,
            )?;
            pathnode::add_path::call(root, rel, p)?;
            if startup_neq_total {
                let p = pathnode::create_merge_append_path::call(
                    root, rel, total_subpaths, pathkeys.clone(), &None,
                )?;
                pathnode::add_path::call(root, rel, p)?;
            }
            if !fractional_subpaths.is_empty() {
                let p = pathnode::create_merge_append_path::call(
                    root, rel, fractional_subpaths, pathkeys.clone(), &None,
                )?;
                pathnode::add_path::call(root, rel, p)?;
            }
        }
    }
    Ok(())
}

/* ----- helpers crossing sibling owners ----- */

/// `pathkeys_contained_in(keys1, keys2)` (pathkeys.c).
fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    backend_optimizer_path_pathkeys::pathkeys_contained_in(keys1, keys2)
}

/// `partitions_are_ordered(rel->boundinfo, rel->live_parts)` (partbounds.c).
fn partitions_are_ordered(root: &PlannerInfo, rel: RelId) -> bool {
    crate::seams::partitions_are_ordered::call(root, rel)
}

/// `get_cheapest_fractional_path(childrel, tuple_fraction)` (pathkeys.c).
fn get_cheapest_fractional_path(root: &PlannerInfo, rel: RelId, tuple_fraction: f64) -> PathId {
    crate::seams::get_cheapest_fractional_path::call(root, rel, tuple_fraction)
}

/// `get_cheapest_parallel_safe_total_inner(childrel->pathlist)` (pathkeys.c).
fn get_cheapest_parallel_safe_total_inner(root: &PlannerInfo, rel: RelId) -> Option<PathId> {
    backend_optimizer_path_pathkeys::get_cheapest_parallel_safe_total_inner(
        root,
        &root.rel(rel).pathlist,
    )
}

/// `pg_leftmost_one_pos32(word)` (pg_bitutils.h) — index of the leftmost set bit.
fn pg_leftmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    (31 - word.leading_zeros()) as i32
}

extern crate alloc;
