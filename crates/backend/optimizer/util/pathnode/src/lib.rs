#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::result_large_err)]
// The port mirrors pathnode.c's control flow verbatim: the parallel-safety
// dominance checks compare booleans by magnitude (`a >= b` / `a < b` on bools),
// exactly as the C tournament does. Kept for 1:1 parity.
#![allow(clippy::bool_comparison)]

//! Safe-Rust port of `src/backend/optimizer/util/pathnode.c` (postgres-18.3):
//! the planner's path cost-domination engine and the `create_*_path` factory
//! family.
//!
//! # Arena model
//!
//! Paths live in the [`PlannerInfo`](::pathnodes::PlannerInfo) arena
//! (`root.path_arena`, keyed by [`PathId`]). A constructor allocates its new
//! [`PathNode`](::pathnodes::PathNode) into the arena via
//! [`PlannerInfo::alloc_path`] and returns the [`PathId`] (the C constructor's
//! freshly-`palloc`'d `Path *`); subpaths are referenced by their `PathId`. The
//! rel's `pathlist`/`partial_pathlist`/cheapest-path fields hold the same
//! `PathId` handles (identity preserved exactly as the C `Path *` aliasing).
//!
//! The dominance tournament ([`add_path`]/[`add_partial_path`]) reads each
//! candidate's [`Path`] fields through the immutable arena accessor and consumes
//! an already-allocated `PathId`; the cost model (`costsize.c`),
//! parameterization layer (`relnode.c`), pathkeys/relids algebra, and the
//! expression walks (`clauses.c`/`tlist.c`) are reached through the seams in
//! [`pathnode_seams`].
//!
//! Allocating functions return [`PgResult`](::types_error::PgResult): in C every
//! `palloc` can `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::Cost;
use ::types_error::{PgError, PgResult};
use ::pathnodes::optimizer_plan::CostSelector;
use ::pathnodes::{Path, PathId, PathKey, PathNode, PlannerInfo, RelId, Relids};

use pathnode_seams as seam;
use relnode_seams as bms;
use seam::{BMS_Comparison, PathKeysComparison};

pub mod create;
pub mod import;

pub use import::{import_path_from_subroot, import_pathkey_eclasses};

/// `STD_FUZZ_FACTOR` (pathnode.c:50) — the normal fuzz factor for
/// [`compare_path_costs_fuzzily`]: `1.0 + delta`, `delta` = 1% considered
/// significant.
pub const STD_FUZZ_FACTOR: f64 = 1.01;

/// `PathCostComparison` (pathnode.c:37-43) — the fuzzy cost relationship.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PathCostComparison {
    /// `COSTS_EQUAL`.
    Equal,
    /// `COSTS_BETTER1`.
    Better1,
    /// `COSTS_BETTER2`.
    Better2,
    /// `COSTS_DIFFERENT`.
    Different,
}

/// Borrow a path's `PATH_REQ_OUTER` without cloning (for seam args). `NULL` is
/// the empty set (`None`).
#[inline]
fn path_req_outer_ref(path: &Path) -> &Relids {
    const EMPTY: Relids = None;
    match &path.param_info {
        Some(ppi) => &ppi.ppi_req_outer,
        None => &EMPTY,
    }
}

/* ===========================================================================
 * MISC. PATH UTILITIES
 * ======================================================================== */

/// `compare_path_costs(path1, path2, criterion)` (pathnode.c:71).
pub fn compare_path_costs(path1: &Path, path2: &Path, criterion: CostSelector) -> i32 {
    if path1.disabled_nodes != path2.disabled_nodes {
        if path1.disabled_nodes < path2.disabled_nodes {
            return -1;
        } else {
            return 1;
        }
    }

    if criterion == CostSelector::STARTUP_COST {
        if path1.startup_cost < path2.startup_cost {
            return -1;
        }
        if path1.startup_cost > path2.startup_cost {
            return 1;
        }
        if path1.total_cost < path2.total_cost {
            return -1;
        }
        if path1.total_cost > path2.total_cost {
            return 1;
        }
    } else {
        if path1.total_cost < path2.total_cost {
            return -1;
        }
        if path1.total_cost > path2.total_cost {
            return 1;
        }
        if path1.startup_cost < path2.startup_cost {
            return -1;
        }
        if path1.startup_cost > path2.startup_cost {
            return 1;
        }
    }
    0
}

/// `compare_fractional_path_costs(path1, path2, fraction)` (pathnode.c:126).
pub fn compare_fractional_path_costs(path1: &Path, path2: &Path, fraction: f64) -> i32 {
    if path1.disabled_nodes != path2.disabled_nodes {
        if path1.disabled_nodes < path2.disabled_nodes {
            return -1;
        } else {
            return 1;
        }
    }
    if fraction <= 0.0 || fraction >= 1.0 {
        return compare_path_costs(path1, path2, CostSelector::TOTAL_COST);
    }
    let cost1: Cost = path1.startup_cost + fraction * (path1.total_cost - path1.startup_cost);
    let cost2: Cost = path2.startup_cost + fraction * (path2.total_cost - path2.startup_cost);
    if cost1 < cost2 {
        return -1;
    }
    if cost1 > cost2 {
        return 1;
    }
    0
}

/// `compare_path_costs_fuzzily(path1, path2, fuzz_factor)` (pathnode.c:184).
///
/// `CONSIDER_PATH_STARTUP_COST(p)` dereferences `p->parent`; both paths in
/// [`add_path`] share the one `parent_rel`, so the parent's two consider flags
/// are passed in directly.
pub fn compare_path_costs_fuzzily(
    path1: &Path,
    path2: &Path,
    fuzz_factor: f64,
    consider_startup: bool,
    consider_param_startup: bool,
) -> PathCostComparison {
    let consider_path_startup_cost = |p: &Path| -> bool {
        if p.param_info.is_none() {
            consider_startup
        } else {
            consider_param_startup
        }
    };

    if path1.disabled_nodes != path2.disabled_nodes {
        if path1.disabled_nodes < path2.disabled_nodes {
            return PathCostComparison::Better1;
        } else {
            return PathCostComparison::Better2;
        }
    }

    if path1.total_cost > path2.total_cost * fuzz_factor {
        if consider_path_startup_cost(path1) && path2.startup_cost > path1.startup_cost * fuzz_factor
        {
            return PathCostComparison::Different;
        }
        return PathCostComparison::Better2;
    }
    if path2.total_cost > path1.total_cost * fuzz_factor {
        if consider_path_startup_cost(path2) && path1.startup_cost > path2.startup_cost * fuzz_factor
        {
            return PathCostComparison::Different;
        }
        return PathCostComparison::Better1;
    }
    if path1.startup_cost > path2.startup_cost * fuzz_factor {
        return PathCostComparison::Better2;
    }
    if path2.startup_cost > path1.startup_cost * fuzz_factor {
        return PathCostComparison::Better1;
    }
    PathCostComparison::Equal
}

/// `clamp_row_est(nrows)` (costsize.c) — round + clamp to `[1, MAXIMUM_ROWCOUNT]`.
/// Pure math, grounded here (needed by the constructors).
pub fn clamp_row_est(nrows: f64) -> f64 {
    const MAXIMUM_ROWCOUNT: f64 = 1e100;
    let mut nrows = nrows;
    if nrows > MAXIMUM_ROWCOUNT {
        nrows = MAXIMUM_ROWCOUNT;
    } else if nrows <= 1.0 {
        nrows = 1.0;
    } else {
        nrows = nrows.round();
    }
    nrows
}

/* ===========================================================================
 * set_cheapest (pathnode.c:271)
 * ======================================================================== */

/// `set_cheapest(parent_rel)` (pathnode.c:271).
pub fn set_cheapest(root: &mut PlannerInfo, parent_rel: RelId) -> PgResult<()> {
    if root.rel(parent_rel).pathlist.is_empty() {
        return Err(PgError::error(
            "could not devise a query plan for the given query",
        ));
    }

    let pathlist: Vec<PathId> = root.rel(parent_rel).pathlist.clone();

    let mut cheapest_startup_path: Option<PathId> = None;
    let mut cheapest_total_path: Option<PathId> = None;
    let mut best_param_path: Option<PathId> = None;
    let mut parameterized_paths: Vec<PathId> = Vec::new();

    for &pid in &pathlist {
        let path = root.path(pid).base();

        if path.param_info.is_some() {
            parameterized_paths.push(pid);

            if cheapest_total_path.is_some() {
                continue;
            }

            match best_param_path {
                None => best_param_path = Some(pid),
                Some(best) => {
                    let best_path = root.path(best).base();
                    match seam::relids_subset_compare::call(
                        path_req_outer_ref(path),
                        path_req_outer_ref(best_path),
                    ) {
                        BMS_Comparison::BMS_EQUAL => {
                            if compare_path_costs(path, best_path, CostSelector::TOTAL_COST) < 0 {
                                best_param_path = Some(pid);
                            }
                        }
                        BMS_Comparison::BMS_SUBSET1 => {
                            best_param_path = Some(pid);
                        }
                        BMS_Comparison::BMS_SUBSET2 => {}
                        BMS_Comparison::BMS_DIFFERENT => {}
                    }
                }
            }
        } else {
            if cheapest_total_path.is_none() {
                cheapest_startup_path = Some(pid);
                cheapest_total_path = Some(pid);
                continue;
            }

            let s = cheapest_startup_path
                .ok_or_else(|| PgError::error("set_cheapest: cheapest_startup_path is NULL"))?;
            let s_path = root.path(s).base();
            let cmp = compare_path_costs(s_path, path, CostSelector::STARTUP_COST);
            if cmp > 0
                || (cmp == 0
                    && seam::compare_pathkeys::call(&s_path.pathkeys, &path.pathkeys)
                        == PathKeysComparison::Better2)
            {
                cheapest_startup_path = Some(pid);
            }

            let t = cheapest_total_path
                .ok_or_else(|| PgError::error("set_cheapest: cheapest_total_path is NULL"))?;
            let t_path = root.path(t).base();
            let cmp = compare_path_costs(t_path, path, CostSelector::TOTAL_COST);
            if cmp > 0
                || (cmp == 0
                    && seam::compare_pathkeys::call(&t_path.pathkeys, &path.pathkeys)
                        == PathKeysComparison::Better2)
            {
                cheapest_total_path = Some(pid);
            }
        }
    }

    // lcons the cheapest unparameterized path onto the front.
    if let Some(t) = cheapest_total_path {
        parameterized_paths.insert(0, t);
    }

    let cheapest_total_path = cheapest_total_path.or(best_param_path);
    let cheapest_total_path = cheapest_total_path
        .ok_or_else(|| PgError::error("could not devise a query plan for the given query"))?;

    let rel = root.rel_mut(parent_rel);
    rel.cheapest_startup_path = cheapest_startup_path;
    rel.cheapest_total_path = Some(cheapest_total_path);
    rel.cheapest_unique_path = None;
    rel.cheapest_parameterized_paths = parameterized_paths;
    Ok(())
}

/* ===========================================================================
 * add_path (pathnode.c:463)
 * ======================================================================== */

/// `add_path(parent_rel, new_path)` (pathnode.c:463). `new_id` is an
/// already-allocated arena handle (the C freshly-`palloc`'d `Path *`).
pub fn add_path(root: &mut PlannerInfo, parent_rel: RelId, new_id: PathId) -> PgResult<()> {
    let mut accept_new = true;
    let mut insert_at = 0usize;

    seam::check_for_interrupts::call();

    let new_path: Path = root.path(new_id).base().clone();

    let consider_startup = root.rel(parent_rel).consider_startup;
    let consider_param_startup = root.rel(parent_rel).consider_param_startup;

    let new_path_pathkeys: &[PathKey] = if new_path.param_info.is_some() {
        &[]
    } else {
        &new_path.pathkeys
    };

    let mut working: Vec<PathId> = root.rel(parent_rel).pathlist.clone();

    let mut i = 0usize;
    while i < working.len() {
        let old_path: Path = root.path(working[i]).base().clone();
        let old_path = &old_path;
        let mut remove_old = false;

        let costcmp = compare_path_costs_fuzzily(
            &new_path,
            old_path,
            STD_FUZZ_FACTOR,
            consider_startup,
            consider_param_startup,
        );

        if costcmp != PathCostComparison::Different {
            let old_path_pathkeys: &[PathKey] = if old_path.param_info.is_some() {
                &[]
            } else {
                &old_path.pathkeys
            };
            let keyscmp = seam::compare_pathkeys::call(new_path_pathkeys, old_path_pathkeys);
            if keyscmp != PathKeysComparison::Different {
                match costcmp {
                    PathCostComparison::Equal => {
                        let outercmp = seam::relids_subset_compare::call(
                            path_req_outer_ref(&new_path),
                            path_req_outer_ref(old_path),
                        );
                        if keyscmp == PathKeysComparison::Better1 {
                            if (outercmp == BMS_Comparison::BMS_EQUAL
                                || outercmp == BMS_Comparison::BMS_SUBSET1)
                                && new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            }
                        } else if keyscmp == PathKeysComparison::Better2 {
                            if (outercmp == BMS_Comparison::BMS_EQUAL
                                || outercmp == BMS_Comparison::BMS_SUBSET2)
                                && new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
                        } else {
                            // PATHKEYS_EQUAL
                            if outercmp == BMS_Comparison::BMS_EQUAL {
                                if new_path.parallel_safe > old_path.parallel_safe {
                                    remove_old = true;
                                } else if new_path.parallel_safe < old_path.parallel_safe {
                                    accept_new = false;
                                } else if new_path.rows < old_path.rows {
                                    remove_old = true;
                                } else if new_path.rows > old_path.rows {
                                    accept_new = false;
                                } else if compare_path_costs_fuzzily(
                                    &new_path,
                                    old_path,
                                    1.0000000001,
                                    consider_startup,
                                    consider_param_startup,
                                ) == PathCostComparison::Better1
                                {
                                    remove_old = true;
                                } else {
                                    accept_new = false;
                                }
                            } else if outercmp == BMS_Comparison::BMS_SUBSET1
                                && new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            } else if outercmp == BMS_Comparison::BMS_SUBSET2
                                && new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
                        }
                    }
                    PathCostComparison::Better1 => {
                        if keyscmp != PathKeysComparison::Better2 {
                            let outercmp = seam::relids_subset_compare::call(
                                path_req_outer_ref(&new_path),
                                path_req_outer_ref(old_path),
                            );
                            if (outercmp == BMS_Comparison::BMS_EQUAL
                                || outercmp == BMS_Comparison::BMS_SUBSET1)
                                && new_path.rows <= old_path.rows
                                && new_path.parallel_safe >= old_path.parallel_safe
                            {
                                remove_old = true;
                            }
                        }
                    }
                    PathCostComparison::Better2 => {
                        if keyscmp != PathKeysComparison::Better1 {
                            let outercmp = seam::relids_subset_compare::call(
                                path_req_outer_ref(&new_path),
                                path_req_outer_ref(old_path),
                            );
                            if (outercmp == BMS_Comparison::BMS_EQUAL
                                || outercmp == BMS_Comparison::BMS_SUBSET2)
                                && new_path.rows >= old_path.rows
                                && new_path.parallel_safe <= old_path.parallel_safe
                            {
                                accept_new = false;
                            }
                        }
                    }
                    PathCostComparison::Different => {}
                }
            }
        }

        if remove_old {
            working.remove(i);
        } else {
            if new_path.disabled_nodes > old_path.disabled_nodes
                || (new_path.disabled_nodes == old_path.disabled_nodes
                    && new_path.total_cost >= old_path.total_cost)
            {
                insert_at = i + 1;
            }
            i += 1;
        }

        if !accept_new {
            break;
        }
    }

    if accept_new {
        if insert_at > working.len() {
            insert_at = working.len();
        }
        working.insert(insert_at, new_id);
    }

    root.rel_mut(parent_rel).pathlist = working;
    Ok(())
}

/// `add_path_precheck(...)` (pathnode.c:690).
pub fn add_path_precheck(
    root: &PlannerInfo,
    parent_rel: RelId,
    disabled_nodes: i32,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: &[PathKey],
    required_outer: &Relids,
) -> bool {
    let required_outer_present = !bms::relids_is_empty::call(required_outer);

    let new_path_pathkeys: &[PathKey] = if required_outer_present { &[] } else { pathkeys };

    let consider_startup = if required_outer_present {
        root.rel(parent_rel).consider_param_startup
    } else {
        root.rel(parent_rel).consider_startup
    };

    for &pid in &root.rel(parent_rel).pathlist {
        let old_path = root.path(pid).base();
        if old_path.disabled_nodes != disabled_nodes {
            if disabled_nodes < old_path.disabled_nodes {
                break;
            }
        } else if total_cost <= old_path.total_cost * STD_FUZZ_FACTOR {
            break;
        }

        if startup_cost > old_path.startup_cost * STD_FUZZ_FACTOR || !consider_startup {
            let old_path_pathkeys: &[PathKey] = if old_path.param_info.is_some() {
                &[]
            } else {
                &old_path.pathkeys
            };
            let keyscmp = seam::compare_pathkeys::call(new_path_pathkeys, old_path_pathkeys);
            if keyscmp == PathKeysComparison::Equal || keyscmp == PathKeysComparison::Better2 {
                if seam::relids_equal::call(required_outer, path_req_outer_ref(old_path)) {
                    return false;
                }
            }
        }
    }

    true
}

/* ===========================================================================
 * add_partial_path (pathnode.c:797)
 * ======================================================================== */

/// `add_partial_path(parent_rel, new_path)` (pathnode.c:797).
pub fn add_partial_path(root: &mut PlannerInfo, parent_rel: RelId, new_id: PathId) -> PgResult<()> {
    let mut accept_new = true;
    let mut insert_at = 0usize;

    seam::check_for_interrupts::call();

    let new_path: Path = root.path(new_id).base().clone();

    let mut working: Vec<PathId> = root.rel(parent_rel).partial_pathlist.clone();

    let mut i = 0usize;
    while i < working.len() {
        let old_path: Path = root.path(working[i]).base().clone();
        let old_path = &old_path;
        let mut remove_old = false;

        let keyscmp = seam::compare_pathkeys::call(&new_path.pathkeys, &old_path.pathkeys);

        if keyscmp != PathKeysComparison::Different {
            if new_path.disabled_nodes != old_path.disabled_nodes {
                if new_path.disabled_nodes > old_path.disabled_nodes {
                    accept_new = false;
                } else {
                    remove_old = true;
                }
            } else if new_path.total_cost > old_path.total_cost * STD_FUZZ_FACTOR {
                if keyscmp != PathKeysComparison::Better1 {
                    accept_new = false;
                }
            } else if old_path.total_cost > new_path.total_cost * STD_FUZZ_FACTOR {
                if keyscmp != PathKeysComparison::Better2 {
                    remove_old = true;
                }
            } else if keyscmp == PathKeysComparison::Better1 {
                remove_old = true;
            } else if keyscmp == PathKeysComparison::Better2 {
                accept_new = false;
            } else if old_path.total_cost > new_path.total_cost * 1.0000000001 {
                remove_old = true;
            } else {
                accept_new = false;
            }
        }

        if remove_old {
            working.remove(i);
        } else {
            if new_path.total_cost >= old_path.total_cost {
                insert_at = i + 1;
            }
            i += 1;
        }

        if !accept_new {
            break;
        }
    }

    if accept_new {
        if insert_at > working.len() {
            insert_at = working.len();
        }
        working.insert(insert_at, new_id);
    }

    root.rel_mut(parent_rel).partial_pathlist = working;
    Ok(())
}

/// `add_partial_path_precheck(...)` (pathnode.c:923).
pub fn add_partial_path_precheck(
    root: &PlannerInfo,
    parent_rel: RelId,
    disabled_nodes: i32,
    total_cost: Cost,
    pathkeys: &[PathKey],
) -> bool {
    for &pid in &root.rel(parent_rel).partial_pathlist {
        let old_path = root.path(pid).base();
        let keyscmp = seam::compare_pathkeys::call(pathkeys, &old_path.pathkeys);
        if keyscmp != PathKeysComparison::Different {
            if total_cost > old_path.total_cost * STD_FUZZ_FACTOR
                && keyscmp != PathKeysComparison::Better1
            {
                return false;
            }
            if old_path.total_cost > total_cost * STD_FUZZ_FACTOR
                && keyscmp != PathKeysComparison::Better2
            {
                return true;
            }
        }
    }

    let no_outer: Relids = None;
    add_path_precheck(
        root,
        parent_rel,
        disabled_nodes,
        total_cost,
        total_cost,
        pathkeys,
        &no_outer,
    )
}

/* ===========================================================================
 * create_append_path list-sort comparators (pathnode.c:1433/1456)
 * ======================================================================== */

/// `append_total_cost_compare(a, b)` (pathnode.c:1433) — descending total cost.
pub fn append_total_cost_compare(path1: &Path, path2: &Path) -> i32 {
    let cmp = -compare_path_costs(path1, path2, CostSelector::TOTAL_COST);
    if cmp != 0 {
        return cmp;
    }
    0
}

/// `append_startup_cost_compare(a, b)` (pathnode.c:1456) — descending startup.
pub fn append_startup_cost_compare(path1: &Path, path2: &Path) -> i32 {
    let cmp = -compare_path_costs(path1, path2, CostSelector::STARTUP_COST);
    if cmp != 0 {
        return cmp;
    }
    0
}

/// Map a `TryReserveError` onto the project's owned OOM `PgError`.
#[inline]
pub(crate) fn oom(_: alloc::collections::TryReserveError) -> PgError {
    PgError::error("out of memory")
}

/* ===========================================================================
 * Seam installation
 * ======================================================================== */

/// Install this crate's inward seams (pathnode-owned) — both the joinpath-facing
/// constructors (`backend-optimizer-path-joinpath-seams`) and the general
/// `create_*_path`/`add_*` surface (`backend-optimizer-util-pathnode-seams`).
pub fn init_seams() {
    use joinpath_seams as jp;
    use pathnode_seams as ps;

    /* --- joinpath-facing seams (owned by pathnode) --- */
    jp::create_nestloop_path::set(create::create_nestloop_path);
    jp::create_mergejoin_path::set(create::create_mergejoin_path);
    jp::create_hashjoin_path::set(create::create_hashjoin_path);
    jp::create_unique_path::set(create::create_unique_path);
    jp::create_material_path::set(create::create_material_path);
    jp::create_memoize_path::set(create_memoize_path_jp);
    jp::add_path::set(add_path);
    jp::add_path_precheck::set(add_path_precheck);
    jp::add_partial_path::set(add_partial_path);
    jp::add_partial_path_precheck::set(add_partial_path_precheck);
    jp::calc_nestloop_required_outer::set(create::calc_nestloop_required_outer);
    jp::calc_non_nestloop_required_outer::set(create::calc_non_nestloop_required_outer);
    jp::compare_path_costs::set(compare_path_costs_seam);
    jp::path_is_reparameterizable_by_child::set(create::path_is_reparameterizable_by_child);

    /* --- general pathnode inward seams --- */
    ps::compare_path_costs::set(compare_path_costs_seam);
    ps::compare_fractional_path_costs::set(compare_fractional_path_costs_seam);
    ps::set_cheapest::set(set_cheapest);
    ps::add_path::set(add_path);
    ps::add_path_precheck::set(add_path_precheck);
    ps::add_partial_path::set(add_partial_path);
    ps::add_partial_path_precheck::set(add_partial_path_precheck);

    ps::create_seqscan_path::set(create::create_seqscan_path);
    ps::create_samplescan_path::set(create::create_samplescan_path);
    ps::create_index_path::set(create::create_index_path);
    ps::create_bitmap_heap_path::set(create::create_bitmap_heap_path);
    ps::create_bitmap_and_path::set(create::create_bitmap_and_path);
    ps::create_bitmap_or_path::set(create::create_bitmap_or_path);
    ps::create_tidscan_path::set(create::create_tidscan_path);
    ps::create_tidrangescan_path::set(create::create_tidrangescan_path);
    ps::create_subqueryscan_path::set(create::create_subqueryscan_path);
    ps::import_path_from_subroot::set(import::import_path_from_subroot);
    ps::import_pathkey_eclasses::set(import::import_pathkey_eclasses);
    ps::create_functionscan_path::set(create::create_functionscan_path);
    ps::create_tablefuncscan_path::set(create::create_tablefuncscan_path);
    ps::create_valuesscan_path::set(create::create_valuesscan_path);
    ps::create_ctescan_path::set(create::create_ctescan_path);
    ps::create_namedtuplestorescan_path::set(create::create_namedtuplestorescan_path);
    ps::create_resultscan_path::set(create::create_resultscan_path);
    ps::create_worktablescan_path::set(create::create_worktablescan_path);
    ps::create_append_path::set(create::create_append_path);
    ps::create_merge_append_path::set(create::create_merge_append_path);
    ps::create_group_result_path::set(create::create_group_result_path);
    ps::create_material_path::set(create::create_material_path);
    ps::create_memoize_path::set(create::create_memoize_path);
    ps::create_gather_merge_path::set(create::create_gather_merge_path);
    ps::create_gather_path::set(create::create_gather_path);
    ps::create_foreignscan_path::set(create::create_foreignscan_path);
    ps::create_foreign_join_path::set(create::create_foreign_join_path);
    ps::create_foreign_upper_path::set(create::create_foreign_upper_path);
    ps::calc_nestloop_required_outer::set(create::calc_nestloop_required_outer);
    ps::calc_non_nestloop_required_outer::set(create::calc_non_nestloop_required_outer);
    ps::create_projection_path::set(create::create_projection_path);
    ps::apply_projection_to_path::set(create::apply_projection_to_path);
    ps::create_set_projection_path::set(create::create_set_projection_path);
    ps::create_incremental_sort_path::set(create::create_incremental_sort_path);
    ps::create_sort_path::set(create::create_sort_path);
    ps::create_group_path::set(create::create_group_path);
    ps::create_upper_unique_path::set(create::create_upper_unique_path);
    ps::create_agg_path::set(create::create_agg_path);
    ps::create_minmaxagg_path::set(create::create_minmaxagg_path);
    ps::create_windowagg_path::set(create::create_windowagg_path);
    ps::create_setop_path::set(create::create_setop_path);
    ps::create_recursiveunion_path::set(create::create_recursiveunion_path);
    ps::create_lockrows_path::set(create::create_lockrows_path);
    ps::create_modifytable_path::set(create::create_modifytable_path);
    ps::create_limit_path::set(create::create_limit_path);
    ps::create_unique_path::set(create::create_unique_path);
    ps::can_create_unique_path::set(create::can_create_unique_path);
    ps::install_dummy_append_path::set(create::install_dummy_append_path);
    ps::reparameterize_path::set(create::reparameterize_path);
    ps::reparameterize_path_by_child::set(create::reparameterize_path_by_child);
    ps::path_is_reparameterizable_by_child::set(create::path_is_reparameterizable_by_child);
    ps::equal_exprs::set(equal_exprs);
}

/// `equal((Node *) a, (Node *) b)` over two tlists of expression handles
/// (`apply_projection_to_path`, pathnode.c). `equal()` on two Lists compares
/// length first, then element-wise; here each element is an `Expr` resolved
/// from the planner node arena and compared via the equalfuncs.c `equal` seam.
fn equal_exprs(root: &PlannerInfo, a: &[::pathnodes::NodeId], b: &[::pathnodes::NodeId]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(&x, &y)| equalfuncs_seams::equal_expr::call(root.node(x), root.node(y)))
}

/// Seam wrapper: resolve the two `PathId`s and run [`compare_path_costs`].
fn compare_path_costs_seam(
    root: &PlannerInfo,
    path1: PathId,
    path2: PathId,
    criterion: CostSelector,
) -> i32 {
    let p1 = root.path(path1).base();
    let p2 = root.path(path2).base();
    compare_path_costs(p1, p2, criterion)
}

/// Seam wrapper: the joinpath-seams `create_memoize_path` passes the cache-key
/// `param_exprs`/`hash_operators` as slices; the constructor owns `Vec`s.
fn create_memoize_path_jp(
    root: &mut PlannerInfo,
    rel: RelId,
    subpath: PathId,
    param_exprs: &[::pathnodes::NodeId],
    hash_operators: &[::types_core::primitive::Oid],
    singlerow: bool,
    binary_mode: bool,
    calls: f64,
) -> PgResult<PathId> {
    create::create_memoize_path(
        root,
        rel,
        subpath,
        param_exprs.to_vec(),
        hash_operators.to_vec(),
        singlerow,
        binary_mode,
        calls,
    )
}

/// Seam wrapper for [`compare_fractional_path_costs`].
fn compare_fractional_path_costs_seam(
    root: &PlannerInfo,
    path1: PathId,
    path2: PathId,
    fraction: f64,
) -> i32 {
    let p1 = root.path(path1).base();
    let p2 = root.path(path2).base();
    compare_fractional_path_costs(p1, p2, fraction)
}

// Reference PathNode so the import is documented (constructors live in create).
#[allow(dead_code)]
type _PathNode = PathNode;
