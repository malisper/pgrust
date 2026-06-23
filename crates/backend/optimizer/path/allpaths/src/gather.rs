//! Gather / Gather-Merge path generation:
//! `generate_gather_paths` (allpaths.c:3098), `get_useful_pathkeys_for_relation`
//! (3167), `generate_useful_gather_paths` (3235).

use alloc::vec::Vec;

use types_error::PgResult;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{PathId, PathKey, PlannerInfo, RelId};

use pathnode_seams as pathnode;

use crate::enable_incremental_sort;

/// `generate_gather_paths` (allpaths.c:3098) — push a Gather (or order-preserving
/// Gather Merge) on top of the relation's partial paths.
///
/// Must not be called until all partial paths for the rel exist. `override_rows`
/// overrides the rel's rowcount estimate (used for partially-grouped paths).
pub fn generate_gather_paths<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    override_rows: bool,
) -> PgResult<()> {
    // Nothing to do if there are no partial paths.
    if root.rel(rel).partial_pathlist.is_empty() {
        return Ok(());
    }

    // The Gather output is unsorted, so only the cheapest partial path (front of
    // partial_pathlist) matters.
    let cheapest_partial_path = root.rel(rel).partial_pathlist[0];
    let rows = compute_gather_rows(root, cheapest_partial_path);
    let rowsp = if override_rows { Some(rows) } else { None };

    let reltarget = root.rel(rel).reltarget.clone();
    let simple_gather_path = pathnode::create_gather_path::call(
        root,
        run,
        rel,
        cheapest_partial_path,
        reltarget,
        &None,
        rowsp,
    )?;
    pathnode::add_path::call(root, rel, simple_gather_path)?;

    // For each useful ordering, consider an order-preserving Gather Merge.
    let partials: Vec<PathId> = root.rel(rel).partial_pathlist.clone();
    for subpath in partials {
        if root.path(subpath).base().pathkeys.is_empty() {
            continue;
        }
        let rows = compute_gather_rows(root, subpath);
        let rowsp = if override_rows { Some(rows) } else { None };
        let reltarget = root.rel(rel).reltarget.clone();
        let pk = root.path(subpath).base().pathkeys.clone();
        let path = pathnode::create_gather_merge_path::call(
            root, run, rel, subpath, reltarget, pk, &None, rowsp,
        )?;
        pathnode::add_path::call(root, rel, path)?;
    }
    Ok(())
}

/// `compute_gather_rows(path)` (costsize.c) — `clamp_row_est(path->rows *
/// get_parallel_divisor(path))`. Lives in the costsize crate as a pub fn.
fn compute_gather_rows(root: &PlannerInfo, path: PathId) -> f64 {
    costsize::compute_gather_rows(root.path(path).base())
}

/// `get_useful_pathkeys_for_relation` (allpaths.c:3167) — which orderings of a
/// relation might be useful (currently the `query_pathkeys`, truncated at the
/// first key whose EC has no early-computable, optionally parallel-safe member).
fn get_useful_pathkeys_for_relation(
    root: &PlannerInfo,
    rel: RelId,
    require_parallel_safe: bool,
) -> Vec<Vec<PathKey>> {
    let mut useful_pathkeys_list: Vec<Vec<PathKey>> = Vec::new();

    if !root.query_pathkeys.is_empty() {
        let mut npathkeys: usize = 0;
        for pathkey in &root.query_pathkeys {
            // pathkey->pk_eclass is never NULL in C.
            let pathkey_ec = pathkey.pk_eclass.expect("PathKey with NULL pk_eclass");
            // Stop at the first pathkey with no safe-to-compute-early EC member.
            if !equivclass::relation_can_be_sorted_early(
                root,
                rel,
                pathkey_ec,
                require_parallel_safe,
            ) {
                break;
            }
            npathkeys += 1;
        }

        if npathkeys == root.query_pathkeys.len() {
            useful_pathkeys_list.push(root.query_pathkeys.clone());
        } else if npathkeys > 0 {
            useful_pathkeys_list.push(root.query_pathkeys[..npathkeys].to_vec());
        }
    }

    useful_pathkeys_list
}

/// `generate_useful_gather_paths` (allpaths.c:3235) — like `generate_gather_paths`
/// but also considers useful orderings for nodes above the Gather Merge, adding
/// a regular or incremental sort to provide them.
pub fn generate_useful_gather_paths<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    override_rows: bool,
) -> PgResult<()> {
    if root.rel(rel).partial_pathlist.is_empty() {
        return Ok(());
    }

    // Generate the regular gather (merge) paths.
    generate_gather_paths(root, run, rel, override_rows)?;

    // Consider incremental sort for interesting orderings.
    let useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel, true);

    // Used for explicit (full) sort paths.
    let cheapest_partial_path = root.rel(rel).partial_pathlist[0];

    let inc_sort = enable_incremental_sort();

    for useful_pathkeys in useful_pathkeys_list {
        let partials: Vec<PathId> = root.rel(rel).partial_pathlist.clone();
        for mut subpath in partials {
            let mut presorted_keys: i32 = 0;
            let is_sorted = pathkeys_count_contained_in(
                &useful_pathkeys,
                &root.path(subpath).base().pathkeys,
                &mut presorted_keys,
            );

            // generate_gather_paths already made a GM path for sorted subpaths.
            if is_sorted {
                continue;
            }

            // Sort the cheapest path; incrementally sort any partially-sorted
            // path (only when incremental sort is enabled, unless it's the
            // cheapest input path).
            if subpath != cheapest_partial_path && (presorted_keys == 0 || !inc_sort) {
                continue;
            }

            // Regular sort for non-presorted paths (or when inc sort disabled);
            // else incremental sort.
            if presorted_keys == 0 || !inc_sort {
                subpath = pathnode::create_sort_path::call(
                    root,
                    rel,
                    subpath,
                    useful_pathkeys.clone(),
                    -1.0,
                )?;
            } else {
                subpath = pathnode::create_incremental_sort_path::call(
                    root,
                    run,
                    rel,
                    subpath,
                    useful_pathkeys.clone(),
                    presorted_keys,
                    -1.0,
                )?;
            }

            let rows = compute_gather_rows(root, subpath);
            let rowsp = if override_rows { Some(rows) } else { None };
            let reltarget = root.rel(rel).reltarget.clone();
            let pk = root.path(subpath).base().pathkeys.clone();
            let path = pathnode::create_gather_merge_path::call(
                root, run, rel, subpath, reltarget, pk, &None, rowsp,
            )?;
            pathnode::add_path::call(root, rel, path)?;
        }
    }
    Ok(())
}

/// `pathkeys_count_contained_in(keys1, keys2, &n_common)` (pathkeys.c) — does
/// `keys1` form a (possibly partial) prefix of `keys2`? Sets `*n_common` to the
/// number of leading matched keys.
fn pathkeys_count_contained_in(
    keys1: &[PathKey],
    keys2: &[PathKey],
    n_common: &mut i32,
) -> bool {
    let (contained, common) =
        pathkeys::pathkeys_count_contained_in(keys1, keys2);
    *n_common = common;
    contained
}

extern crate alloc;
