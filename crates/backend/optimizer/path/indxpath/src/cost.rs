//! Loop-count / joinrel-size estimators and the bitmap-AND cost helpers
//! (indxpath.c). Ported 1:1 over the planner arena.

use core::cmp::Ordering;

use types_core::primitive::{Cost, Index};
use types_error::PgResult;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{PathId, PlannerInfo, RelId, Relids, JOIN_SEMI};

use costsize_seams as costsize;
// joinrels.c is ported; is_dummy_rel is called directly on its owner.
use joinrels as joinrels;
use pathnode_seams as pathnode;
use relnode_seams as relids_seam;
use selfuncs_seams as selfuncs;

use crate::util::relids_next_member;

/// `approximate_joinrel_size(root, relids)` (indxpath.c:2425) — a crude estimate
/// of a joinrel's size: the product of the (non-dummy) base relations' row
/// estimates.
pub fn approximate_joinrel_size(root: &PlannerInfo, relids: &Relids) -> f64 {
    let mut rowcount = 1.0_f64;
    let mut relid: i32 = -1;
    loop {
        relid = relids_next_member(relids, relid);
        if relid < 0 {
            break;
        }
        // Paranoia: ignore bogus relid indexes.
        if relid >= root.simple_rel_array_size {
            continue;
        }
        let rel_id = match root.simple_rel_array[relid as usize] {
            Some(id) => id,
            None => continue,
        };
        // Relation could be proven empty, if so ignore.
        if joinrels::is_dummy_rel(root, rel_id) {
            continue;
        }
        // Otherwise, rel's rows estimate should be valid by now. Accumulate.
        rowcount *= root.rel(rel_id).rows;
    }
    rowcount
}

/// `adjust_rowcount_for_semijoins(root, cur_relid, outer_relid, rowcount)`
/// (indxpath.c:2381) — if `outer_relid` is on the inside of any semijoin that
/// `cur_relid` is on the outside of, replace `rowcount` with the estimated
/// number of unique rows from the semijoin RHS (when that's smaller).
pub fn adjust_rowcount_for_semijoins<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    cur_relid: Index,
    outer_relid: Index,
    mut rowcount: f64,
) -> PgResult<f64> {
    // Snapshot the matching semijoins first: estimate_num_groups needs `&mut
    // root` (it re-interns stripped grouping expressions into the node arena),
    // which can't be borrowed while iterating `root.join_info_list`.
    let mut candidates: alloc::vec::Vec<(Relids, alloc::vec::Vec<pathnodes::NodeId>)> =
        alloc::vec::Vec::new();
    for sjinfo in &root.join_info_list {
        if sjinfo.jointype == JOIN_SEMI
            && relids_seam::relids_is_member::call(cur_relid as i32, &sjinfo.syn_lefthand)
            && relids_seam::relids_is_member::call(outer_relid as i32, &sjinfo.syn_righthand)
        {
            candidates.push((sjinfo.syn_righthand.clone(), sjinfo.semi_rhs_exprs.clone()));
        }
    }

    for (syn_righthand, semi_rhs_exprs) in candidates {
        // Estimate number of unique-ified rows.
        let nraw = approximate_joinrel_size(root, &syn_righthand);
        let nunique =
            selfuncs::estimate_num_groups::call(run, root, &semi_rhs_exprs, nraw, None)?;
        if rowcount > nunique {
            rowcount = nunique;
        }
    }
    Ok(rowcount)
}

/// `get_loop_count(root, cur_relid, outer_relids)` (indxpath.c:2328) — estimate
/// the number of times an inner indexscan parameterized by `outer_relids` will
/// be re-executed (the smallest outer-rel row count, semijoin-adjusted).
pub fn get_loop_count<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    cur_relid: Index,
    outer_relids: &Relids,
) -> PgResult<f64> {
    // For a non-parameterized path, just return 1.0 quickly.
    if outer_relids.is_none() {
        return Ok(1.0);
    }

    let mut result = 0.0_f64;
    let mut outer_relid: i32 = -1;
    loop {
        outer_relid = relids_next_member(outer_relids, outer_relid);
        if outer_relid < 0 {
            break;
        }
        // Paranoia: ignore bogus relid indexes.
        if outer_relid >= root.simple_rel_array_size {
            continue;
        }
        let outer_rel_id = match root.simple_rel_array[outer_relid as usize] {
            Some(id) => id,
            None => continue,
        };
        // Other relation could be proven empty, if so ignore.
        if joinrels::is_dummy_rel(root, outer_rel_id) {
            continue;
        }
        // Otherwise, rel's rows estimate should be valid by now.
        let outer_rows = root.rel(outer_rel_id).rows;

        // Check to see if rel is on the inside of any semijoins.
        let rowcount =
            adjust_rowcount_for_semijoins(run, root, cur_relid, outer_relid as Index, outer_rows)?;

        // Remember smallest row count estimate among the outer rels.
        if result == 0.0 || result > rowcount {
            result = rowcount;
        }
    }
    // Return 1.0 if we found no valid relations (shouldn't happen).
    if result > 0.0 {
        Ok(result)
    } else {
        Ok(1.0)
    }
}

/// `path_usage_comparator(a, b)` (indxpath.c:1992) — qsort comparator ordering
/// bitmap candidate paths by increasing index access cost, breaking ties by
/// selectivity. Returns [`Ordering`] for `sort_by`.
pub fn path_usage_comparator(root: &PlannerInfo, a: PathId, b: PathId) -> Ordering {
    let (acost, aselec) = costsize::cost_bitmap_tree_node::call(root, a);
    let (bcost, bselec) = costsize::cost_bitmap_tree_node::call(root, b);

    // If costs are the same, sort by selectivity.
    if acost < bcost {
        return Ordering::Less;
    }
    if acost > bcost {
        return Ordering::Greater;
    }
    if aselec < bselec {
        return Ordering::Less;
    }
    if aselec > bselec {
        return Ordering::Greater;
    }
    Ordering::Equal
}

/// `bitmap_scan_cost_est(root, rel, ipath)` (indxpath.c:2025) — estimate the
/// cost of actually executing a bitmap scan whose bitmap-producing path is
/// `ipath` (which could be a BitmapAnd or BitmapOr node).
///
/// The C stack-local dummy `BitmapHeapPath` is realized here as a transient
/// path allocated into the arena (the planner arena is a plain `Vec`, so the
/// extra node is cheap and behaviorally identical); we copy `ipath`'s
/// `param_info`, force `parallel_workers = 0`, cost it via
/// `cost_bitmap_heap_scan` (told the loop count via `get_loop_count`), and
/// return the resulting `total_cost`.
pub fn bitmap_scan_cost_est<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    ipath: PathId,
) -> PgResult<Cost> {
    use ::nodes::nodes::NodeTag;
    use pathnodes::{BitmapHeapPath, Path, PathNode};

    // PATH_REQ_OUTER(ipath): the required-outer relids of the bitmapqual.
    let req_outer: Relids = root
        .path(ipath)
        .base()
        .param_info
        .as_ref()
        .map(|ppi| relids_seam::relids_copy::call(&ppi.ppi_req_outer))
        .unwrap_or(None);
    let relid = root.rel(rel).relid;
    let loop_count = get_loop_count(run, root, relid, &req_outer)?;

    // Set up a (throwaway) BitmapHeapPath.
    let reltarget = root.rel(rel).reltarget.clone();
    let param_info = root.path(ipath).base().param_info.clone();
    // T_BitmapHeapPath / T_BitmapHeapScan: the path/plan node tags. These
    // planner/path tags are not modeled as `NodeTag` constants in types-nodes;
    // the dummy path is never installed in the rel's pathlist and is only fed to
    // cost_bitmap_heap_scan, which reads param_info/parent/pathtarget, not the
    // tag, so a default tag is faithful here.
    let bpath = BitmapHeapPath {
        path: Path {
            type_: NodeTag(0),
            pathtype: NodeTag(0),
            parent: rel,
            pathtarget: reltarget,
            param_info,
            parallel_aware: false,
            parallel_safe: false,
            // Check the cost of temporary path without considering parallelism.
            parallel_workers: 0,
            rows: 0.0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            pathkeys: alloc::vec::Vec::new(),
        },
        bitmapqual: Some(ipath),
    };
    let bpath_id = root.alloc_path(PathNode::BitmapHeapPath(bpath));

    // Now we can do cost_bitmap_heap_scan.
    pathnode::cost_bitmap_heap_scan::call(root, bpath_id, rel, ipath, loop_count);

    Ok(root.path(bpath_id).base().total_cost)
}

/// `bitmap_and_cost_est(root, rel, paths)` (indxpath.c:2059) — estimate the cost
/// of executing a BitmapAnd scan over the given component bitmap paths.
///
/// Mirrors C: build a real `BitmapAndPath` via the pathnode constructor (it is
/// installed in the arena and gets a `PathId`), then route it through
/// `bitmap_scan_cost_est`. `paths` are the component bitmapqual handles.
pub fn bitmap_and_cost_est<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    paths: alloc::vec::Vec<PathId>,
) -> Result<Cost, types_error::PgError> {
    let apath_id = pathnode::create_bitmap_and_path::call(root, run, rel, paths)?;
    bitmap_scan_cost_est(run, root, rel, apath_id)
}
