#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

//! Safe-Rust port of `src/backend/optimizer/path/allpaths.c` (postgres-18.3):
//! the top-level access-path search driver.
//!
//! # Scope and the subquery keystone
//!
//! The path-construction spine ports 1:1 over the
//! [`types_pathnodes`] arena model (`PlannerInfo`/`RelId`/`PathId`): every
//! `create_*_path`/`add_path`/`add_partial_path`/`set_cheapest` crosses the
//! `backend-optimizer-util-pathnode-seams`; the size estimators cross the
//! costsize/pathkeys/equivclass/appendinfo/relnode owners' seams; the RTE/Query
//! scalar field reads cross `backend-optimizer-rte-seams`.
//!
//! The **subquery pushdown vertical** (`set_subquery_pathlist` +
//! `subquery_is_pushdown_safe` and the whole `qual_is_pushdown_safe` /
//! `remove_unused_subquery_outputs` / window-runcondition cluster) operates on
//! `Query` *subtrees* (`targetList`, `setOperations`, `windowClause`,
//! `distinctClause`, …). `types_pathnodes` carries no `Query` value — only the
//! opaque [`types_pathnodes::QueryId`] handle and the scalar RTE projections —
//! because the real `Query<'mcx>` is owned by the (unported) planner-entry crate
//! that runs `subquery_planner`. So that whole vertical, plus `set_cte_pathlist`
//! / `set_worktable_pathlist` (which resolve a CTE by name out of
//! `parse->cteList`), routes through seam-and-panic until the planner lands. See
//! [`subquery`].
//!
//! # Owner seams installed here
//!
//! allpaths.c owns three already-declared-but-uninstalled seams its consumers
//! wait on:
//! * `costsize-seams::compute_parallel_worker` (costsize.c `cost_*` callers),
//! * `costsize-seams::create_partial_bitmap_paths` (indxpath driver),
//! * `geqo-all-seams::build_and_cost_join_rel` (geqo `merge_clump`).
//!
//! These are wired in [`init_seams`].

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    PathId, PlannerInfo, RelId, Relids, JOIN_ANTI, JOIN_SEMI, RELOPT_BASEREL, RTE_RELATION,
};

/* RTEKind discriminants (parsenodes.h). `types_pathnodes::RTEKind` is a bare
 * `u32`; only `RTE_RELATION` is exported there, so mirror the rest here to match
 * the values the `rte_rtekind` seam returns. */
use types_pathnodes::RTEKind;
const RTE_SUBQUERY: RTEKind = 1;
const RTE_FUNCTION: RTEKind = 3;
const RTE_TABLEFUNC: RTEKind = 4;
const RTE_VALUES: RTEKind = 5;
const RTE_CTE: RTEKind = 6;
const RTE_NAMEDTUPLESTORE: RTEKind = 7;
const RTE_RESULT: RTEKind = 8;

use backend_optimizer_path_joinrels::{is_dummy_rel, is_simple_rel, make_join_rel};
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_path_costsize_seams as costsize;
use backend_optimizer_rte_seams as rte;

pub mod subquery;

/* ==========================================================================
 * Well-known catalog OID constant (pg_proc/pg_operator).
 * ======================================================================== */

/// `Int8LessOperator` — the `int8 < int8` operator OID (pg_operator.dat: 412),
/// used to build the ordinality pathkey for a `WITH ORDINALITY` function scan.
const INT8_LESS_OPERATOR: Oid = 412;

/// `RELKIND_FOREIGN_TABLE` (pg_class.h).
const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
/// `RELKIND_PARTITIONED_TABLE` (pg_class.h).
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

/// `BMS_SINGLETON` (bitmapset.h) — `bms_membership` result for a one-member set.
const BMS_SINGLETON: i32 = 1;

/* ==========================================================================
 * GUC reads (file-scope globals in C; read live here, behaviour-identical).
 * ======================================================================== */

#[inline]
fn max_parallel_workers_per_gather() -> i32 {
    backend_utils_misc_guc_tables::vars::max_parallel_workers_per_gather.read()
}
#[inline]
fn min_parallel_table_scan_size() -> i32 {
    backend_utils_misc_guc_tables::vars::min_parallel_table_scan_size.read()
}
#[inline]
fn min_parallel_index_scan_size() -> i32 {
    backend_utils_misc_guc_tables::vars::min_parallel_index_scan_size.read()
}
#[inline]
fn enable_parallel_append() -> bool {
    backend_utils_misc_guc_tables::vars::enable_parallel_append.read()
}
#[inline]
fn enable_incremental_sort() -> bool {
    backend_utils_misc_guc_tables::vars::enable_incremental_sort.read()
}
#[inline]
fn enable_partitionwise_join() -> bool {
    backend_utils_misc_guc_tables::vars::enable_partitionwise_join.read()
}
#[inline]
fn enable_geqo() -> bool {
    backend_utils_misc_guc_tables::vars::enable_geqo.read()
}
#[inline]
fn geqo_threshold() -> i32 {
    backend_utils_misc_guc_tables::vars::geqo_threshold.read()
}

/// `root->glob->parallelModeOK`.
#[inline]
fn parallel_mode_ok(root: &PlannerInfo) -> bool {
    root.glob.as_ref().map(|g| g.parallel_mode_ok).unwrap_or(false)
}

/// `PATH_REQ_OUTER(path)` (pathnodes.h) — `param_info ? ppi_req_outer : NULL`.
#[inline]
fn path_req_outer(root: &PlannerInfo, path: PathId) -> Relids {
    match &root.path(path).base().param_info {
        Some(ppi) => bms::relids_copy::call(&ppi.ppi_req_outer),
        None => None,
    }
}

/* ==========================================================================
 * make_one_rel (allpaths.c:170)
 * ======================================================================== */

/// `make_one_rel` (allpaths.c:170) — find all access paths for executing a
/// query, returning the single rel that joins all base rels.
///
/// In the arena model the C `RelOptInfo *` result is the final join rel's
/// [`RelId`].
pub fn make_one_rel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    joinlist: &[JoinlistNode],
) -> PgResult<RelId> {
    // Mark base rels as to whether we care about fast-start plans.
    set_base_rel_consider_startup(root);

    // Compute size estimates and consider_parallel flags for each base rel.
    set_base_rel_sizes(mcx, root)?;

    // Now compute total_table_pages (appendrels not double-counted: parents
    // have pages = 0).
    let mut total_pages: f64 = 0.0;
    let mut rti: usize = 1;
    while rti < root.simple_rel_array_size as usize {
        let brel = match root.simple_rel_array[rti] {
            Some(id) => id,
            None => {
                rti += 1;
                continue;
            }
        };
        debug_assert_eq!(root.rel(brel).relid as usize, rti);
        if is_dummy_rel(root, brel) {
            rti += 1;
            continue;
        }
        if is_simple_rel(root.rel(brel)) {
            total_pages += root.rel(brel).pages as f64;
        }
        rti += 1;
    }
    root.total_table_pages = total_pages;

    // Generate access paths for each base rel.
    set_base_rel_pathlists(mcx, root)?;

    // Generate access paths for the entire join tree.
    let rel = make_rel_from_joinlist(mcx, root, joinlist)?
        .expect("make_one_rel: empty joinlist");

    // The result should join all and only the query's base + outer-join rels.
    debug_assert!(bms::relids_equal::call(&root.rel(rel).relids, &root.all_query_rels));

    Ok(rel)
}

/* ==========================================================================
 * set_base_rel_consider_startup (allpaths.c:246)
 * ======================================================================== */

/// `set_base_rel_consider_startup` (allpaths.c:246) — set the
/// `consider_param_startup` flag for base rels on the RHS of a single-base-rel
/// SEMI/ANTI join.
pub fn set_base_rel_consider_startup(root: &mut PlannerInfo) {
    // Collect the singleton members first to avoid borrowing join_info_list
    // while mutating the rel arena.
    let mut singletons: Vec<i32> = Vec::new();
    for sjinfo in &root.join_info_list {
        if sjinfo.jointype == JOIN_SEMI || sjinfo.jointype == JOIN_ANTI {
            if let Some(varno) = bms::relids_get_singleton_member::call(&sjinfo.syn_righthand) {
                singletons.push(varno);
            }
        }
    }
    for varno in singletons {
        let rel = bms::find_base_rel::call(root, varno);
        root.rel_mut(rel).consider_param_startup = true;
    }
}

/* ==========================================================================
 * set_base_rel_sizes (allpaths.c:289)
 * ======================================================================== */

/// `set_base_rel_sizes` (allpaths.c:289) — set the size estimates and
/// `consider_parallel` flag for each base-relation entry.
pub fn set_base_rel_sizes<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo) -> PgResult<()> {
    let mut rti: usize = 1;
    while rti < root.simple_rel_array_size as usize {
        let rel = match root.simple_rel_array[rti] {
            Some(id) => id,
            None => {
                rti += 1;
                continue;
            }
        };
        debug_assert_eq!(root.rel(rel).relid as usize, rti);

        // ignore RTEs that are "other rels"
        if root.rel(rel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        // If parallelism is allowable for this query in general, see whether
        // it's allowable for this rel in particular (must precede set_rel_size).
        if parallel_mode_ok(root) {
            set_rel_consider_parallel(root, rel, rti as Index);
        }

        set_rel_size(mcx, root, rel, rti as Index)?;

        rti += 1;
    }
    Ok(())
}

/* ==========================================================================
 * set_base_rel_pathlists (allpaths.c:332)
 * ======================================================================== */

/// `set_base_rel_pathlists` (allpaths.c:332) — find all paths for scanning each
/// base-relation entry.
pub fn set_base_rel_pathlists<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo) -> PgResult<()> {
    let mut rti: usize = 1;
    while rti < root.simple_rel_array_size as usize {
        let rel = match root.simple_rel_array[rti] {
            Some(id) => id,
            None => {
                rti += 1;
                continue;
            }
        };
        debug_assert_eq!(root.rel(rel).relid as usize, rti);
        if root.rel(rel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }
        set_rel_pathlist(mcx, root, rel, rti as Index)?;
        rti += 1;
    }
    Ok(())
}

/* ==========================================================================
 * set_rel_size (allpaths.c:359)
 * ======================================================================== */

/// `set_rel_size` (allpaths.c:359) — set size estimates for a base relation
/// (the RTE-kind dispatcher).
pub fn set_rel_size<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    if root.rel(rel).reloptkind == RELOPT_BASEREL
        && relation_excluded_by_constraints(root, rel, rti)
    {
        // Proven empty by constraint exclusion: install a dummy path now. (Only
        // for regular baserels; otherrels had CE checked in set_append_rel_size.)
        set_dummy_rel_pathlist(root, rel)?;
    } else if rte::rte_inh::call(root, rti) {
        // It's an "append relation".
        set_append_rel_size(mcx, root, rel, rti)?;
    } else {
        match rte::rte_rtekind::call(root, rti) {
            RTE_RELATION => {
                let relkind = rte::rte_relkind::call(root, rti);
                if relkind == RELKIND_FOREIGN_TABLE {
                    set_foreign_size(root, rel, rti)?;
                } else if relkind == RELKIND_PARTITIONED_TABLE {
                    // Partitioned table scanned with ONLY: no partitions, dummy.
                    set_dummy_rel_pathlist(root, rel)?;
                } else if rte::rte_has_tablesample::call(root, rti) {
                    set_tablesample_rel_size(mcx, root, rel, rti)?;
                } else {
                    set_plain_rel_size(mcx, root, rel)?;
                }
            }
            RTE_SUBQUERY => {
                // Subqueries build their paths immediately (no param choice).
                subquery::set_subquery_pathlist(root, rel, rti)?;
            }
            RTE_FUNCTION => backend_optimizer_path_costsize::sizeest::set_function_size_estimates(root, rel),
            RTE_TABLEFUNC => {
                backend_optimizer_path_costsize::sizeest::set_tablefunc_size_estimates(root, rel)
            }
            RTE_VALUES => seams::set_values_size_estimates::call(root, rel),
            RTE_CTE => {
                if rte::rte_self_reference::call(root, rti) {
                    subquery::set_worktable_pathlist(root, rel, rti)?;
                } else {
                    subquery::set_cte_pathlist(root, rel, rti)?;
                }
            }
            RTE_NAMEDTUPLESTORE => set_namedtuplestore_pathlist(root, rel)?,
            RTE_RESULT => set_result_pathlist(root, rel)?,
            other => {
                return Err(PgError::error(alloc::format!("unexpected rtekind: {other}")));
            }
        }
    }

    // All non-dummy rels must have a nonzero rowcount estimate.
    debug_assert!(root.rel(rel).rows > 0.0 || is_dummy_rel(root, rel));
    Ok(())
}

/* ==========================================================================
 * set_rel_pathlist (allpaths.c:472)
 * ======================================================================== */

/// `set_rel_pathlist` (allpaths.c:472) — build access paths for a base relation
/// (the RTE-kind dispatcher) plus the post-dispatch finishing steps.
pub fn set_rel_pathlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    if is_dummy_rel(root, rel) {
        // Already proven empty; nothing more to do.
    } else if rte::rte_inh::call(root, rti) {
        set_append_rel_pathlist(mcx, root, rel, rti)?;
    } else {
        match rte::rte_rtekind::call(root, rti) {
            RTE_RELATION => {
                if rte::rte_relkind::call(root, rti) == RELKIND_FOREIGN_TABLE {
                    set_foreign_pathlist(root, rel, rti)?;
                } else if rte::rte_has_tablesample::call(root, rti) {
                    set_tablesample_rel_pathlist(root, rel, rti)?;
                } else {
                    set_plain_rel_pathlist(mcx, root, rel)?;
                }
            }
            RTE_SUBQUERY => {}        // fully handled during set_rel_size
            RTE_FUNCTION => set_function_pathlist(root, rel, rti)?,
            RTE_TABLEFUNC => set_tablefunc_pathlist(root, rel)?,
            RTE_VALUES => set_values_pathlist(root, rel)?,
            RTE_CTE => {}            // fully handled during set_rel_size
            RTE_NAMEDTUPLESTORE => {} // fully handled during set_rel_size
            RTE_RESULT => {}        // fully handled during set_rel_size
            other => {
                return Err(PgError::error(alloc::format!("unexpected rtekind: {other}")));
            }
        }
    }

    // The set_rel_pathlist_hook plugin point is NULL in core PostgreSQL; nothing
    // to invoke here (no extension is loaded in this build).

    // If this is a baserel (not an inheritance child) and not the topmost
    // scan/join rel, consider gathering partial paths. (The topmost rel is
    // postponed to grouping_planner.)
    if root.rel(rel).reloptkind == RELOPT_BASEREL
        && !bms::relids_equal::call(&root.rel(rel).relids, &root.all_query_rels)
    {
        generate_useful_gather_paths(root, rel, false)?;
    }

    // Find the cheapest of the paths for this rel.
    pathnode::set_cheapest::call(root, rel)?;
    Ok(())
}

/* ==========================================================================
 * set_plain_rel_size (allpaths.c:571)
 * ======================================================================== */

/// `set_plain_rel_size` (allpaths.c:571) — size estimates for a plain relation.
pub fn set_plain_rel_size<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    // Test partial indexes first (partial unique indexes can affect estimates).
    check_index_predicates(mcx, root, rel)?;
    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_baserel_size_estimates(root, rel);
    Ok(())
}

/* ==========================================================================
 * set_rel_consider_parallel (allpaths.c:588)
 * ======================================================================== */

/// `set_rel_consider_parallel` (allpaths.c:588) — set the rel's
/// `consider_parallel` flag if it can safely be scanned from within a worker.
pub fn set_rel_consider_parallel(root: &mut PlannerInfo, rel: RelId, rti: Index) {
    debug_assert!(!root.rel(rel).consider_parallel);
    debug_assert!(parallel_mode_ok(root));
    debug_assert!(is_simple_rel(root.rel(rel)));

    // Assorted checks based on rtekind.
    match rte::rte_rtekind::call(root, rti) {
        RTE_RELATION => {
            // Temp tables can't be accessed by workers.
            if get_rel_persistence(rte::rte_relid::call(root, rti)) == RELPERSISTENCE_TEMP {
                return;
            }
            // TABLESAMPLE pushdown safety (the sample function + args).
            if rte::rte_has_tablesample::call(root, rti) {
                if !tablesample_is_parallel_safe(root, rti) {
                    return;
                }
            }
            // FDW parallel-safety dispatch.
            if rte::rte_relkind::call(root, rti) == RELKIND_FOREIGN_TABLE {
                if !foreign_scan_parallel_safe(root, rel, rti) {
                    return;
                }
            }
            // Appendrel-specific considerations are handled in
            // set_append_rel_{size,pathlist}; nothing more here.
        }
        RTE_SUBQUERY => {
            // Subquery-in-FROM is fine, except LIMIT/OFFSET.
            if subquery_limit_needed(root, rti) {
                return;
            }
        }
        RTE_FUNCTION => {
            if !rel_functions_parallel_safe(root, rti) {
                return;
            }
        }
        RTE_TABLEFUNC => return, // not parallel safe
        RTE_VALUES => {
            if !rel_values_parallel_safe(root, rti) {
                return;
            }
        }
        RTE_CTE => return,           // CTE tuplestores aren't shared
        RTE_NAMEDTUPLESTORE => return, // tuplestore cannot be shared
        RTE_RESULT => {}            // RESULT RTEs are fine
        _ => {
            // RTE_JOIN / RTE_GROUP: shouldn't happen for baserels.
            return;
        }
    }

    // If anything in baserestrictinfo is parallel-restricted, give up.
    if !baserestrictinfo_parallel_safe(root, rel) {
        return;
    }
    // If the rel's outputs are not parallel-safe, give up.
    if !reltarget_exprs_parallel_safe(root, rel) {
        return;
    }

    // We have a winner.
    root.rel_mut(rel).consider_parallel = true;
}

/* ==========================================================================
 * set_plain_rel_pathlist (allpaths.c:767)
 * ======================================================================== */

/// `set_plain_rel_pathlist` (allpaths.c:767) — access paths for a plain relation.
pub fn set_plain_rel_pathlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<()> {
    // Seqscan can't take join clauses, but may be parameterized by LATERAL refs.
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    // Consider TID scans. If create_tidscan_paths returns true, a TID scan is
    // forced (CurrentOfExpr); add no other paths.
    if create_tidscan_paths(root, rel)? {
        return Ok(());
    }

    // Consider sequential scan.
    let seqscan = pathnode::create_seqscan_path::call(root, rel, &required_outer, 0)?;
    pathnode::add_path::call(root, rel, seqscan)?;

    // If appropriate, consider parallel sequential scan.
    if root.rel(rel).consider_parallel && required_outer.is_none() {
        create_plain_partial_paths(root, rel)?;
    }

    // Consider index scans.
    create_index_paths(mcx, root, rel)?;
    Ok(())
}

/* ==========================================================================
 * create_plain_partial_paths (allpaths.c:805)
 * ======================================================================== */

/// `create_plain_partial_paths` (allpaths.c:805) — partial paths for parallel
/// scan of a plain relation.
pub fn create_plain_partial_paths(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    let pages = root.rel(rel).pages as f64;
    let parallel_workers = compute_parallel_worker(
        root,
        rel,
        pages,
        -1.0,
        max_parallel_workers_per_gather(),
    );
    // If any limit was set to zero, the user doesn't want a parallel scan.
    if parallel_workers <= 0 {
        return Ok(());
    }
    // Add an unordered partial path based on a parallel sequential scan.
    let path = pathnode::create_seqscan_path::call(root, rel, &None, parallel_workers)?;
    pathnode::add_partial_path::call(root, rel, path)?;
    Ok(())
}

/* ==========================================================================
 * set_tablesample_rel_size (allpaths.c:826) / pathlist (allpaths.c:866)
 * ======================================================================== */

/// `set_tablesample_rel_size` (allpaths.c:826) — size estimates for a sampled
/// relation. The sample method's `SampleScanGetSampleSize` dispatch is seamed.
pub fn set_tablesample_rel_size<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    // Test partial indexes first.
    check_index_predicates(mcx, root, rel)?;

    // Call the sampling method's estimation function. Unported (TSM dispatch).
    let (pages, tuples) = tablesample_get_sample_size(root, rel, rti)?;

    root.rel_mut(rel).pages = pages;
    root.rel_mut(rel).tuples = tuples;

    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_baserel_size_estimates(root, rel);
    Ok(())
}

/// `set_tablesample_rel_pathlist` (allpaths.c:866) — access paths for a sampled
/// relation.
pub fn set_tablesample_rel_pathlist(
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    // Samplescan can't take join clauses, but may be parameterized by LATERAL.
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    let mut path = pathnode::create_samplescan_path::call(root, rel, &required_outer)?;

    // If the sampling method does not support repeatable scans and a join might
    // occur, wrap the SampleScan in a Materialize node.
    let multi = root.query_level > 1
        || bms::relids_membership::call(&root.all_query_rels) != BMS_SINGLETON;
    if multi && !tablesample_repeatable_across_scans(root, rti)? {
        path = pathnode::create_material_path::call(root, rel, path)?;
    }

    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/* ==========================================================================
 * set_foreign_size (allpaths.c:913) / pathlist (allpaths.c:937)
 * ======================================================================== */

/// `set_foreign_size` (allpaths.c:913) — size estimates for a foreign table RTE.
pub fn set_foreign_size(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    // Mark rel with estimated output rows, width, etc.
    set_foreign_size_estimates(root, rel);
    // Let the FDW adjust the size estimates (FDW dispatch — seamed).
    foreign_get_rel_size(root, rel, rti)?;
    // But do not let it set the rows estimate to zero.
    let rows = costsize::clamp_row_est::call(root.rel(rel).rows);
    root.rel_mut(rel).rows = rows;
    // Make sure tuples is not insane relative to rows.
    let tuples = root.rel(rel).tuples.max(rows);
    root.rel_mut(rel).tuples = tuples;
    Ok(())
}

/// `set_foreign_pathlist` (allpaths.c:937) — access paths for a foreign table RTE.
pub fn set_foreign_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    // Call the FDW's GetForeignPaths (FDW dispatch — seamed).
    foreign_get_paths(root, rel, rti)
}

mod append;
pub use append::{
    accumulate_append_subpath, add_paths_to_append_rel, generate_orderedappend_paths,
    get_cheapest_parameterized_child_path, get_singleton_append_subpath, set_append_rel_pathlist,
    set_append_rel_size,
};

mod gather;
pub use gather::{generate_gather_paths, generate_useful_gather_paths};

mod rte_simple;
pub use rte_simple::{
    set_function_pathlist, set_namedtuplestore_pathlist, set_result_pathlist,
    set_tablefunc_pathlist, set_values_pathlist,
};

mod joinsearch;
pub use joinsearch::{make_rel_from_joinlist, standard_join_search, JoinlistNode};

mod parallel_workers;
pub use parallel_workers::compute_parallel_worker;

mod partwise;
pub use partwise::generate_partitionwise_join_paths;

mod bitmap;
pub use bitmap::create_partial_bitmap_paths;

mod dummy;
pub use dummy::set_dummy_rel_pathlist;

/* ==========================================================================
 * Cross-crate seam wrappers for owner-absent / not-yet-wired dependencies.
 *
 * Each of these crosses an owner whose crate is not yet ported in this repo;
 * they seam-and-panic (CONTRACT_RECONCILE_PENDING in seams-init) until the
 * owner lands, mirroring the C call exactly.
 * ======================================================================== */

/// `RELPERSISTENCE_TEMP` (pg_class.h).
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `relation_excluded_by_constraints(root, rel, rte)` (plancat.c) — plancat
/// owner crate is not yet ported; route through the rte-seams placeholder.
fn relation_excluded_by_constraints(root: &PlannerInfo, rel: RelId, rti: Index) -> bool {
    seams::relation_excluded_by_constraints::call(root, rel, rti)
}

/// `check_index_predicates(root, rel)` (indxpath.c).
fn check_index_predicates<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    backend_optimizer_path_indxpath::check_index_predicates(mcx, root, rel)
}

/// `create_index_paths(root, rel)` (indxpath.c).
fn create_index_paths<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    backend_optimizer_path_indxpath::create_index_paths(mcx, root, rel)
}

/// `create_tidscan_paths(root, rel)` (tidpath.c, here via path-small). The C
/// reads `enable_tidscan` as a GUC global inside `create_tidscan_paths`; the
/// path-small port lifts it to an explicit parameter, so we pass the live value.
fn create_tidscan_paths(root: &mut PlannerInfo, rel: RelId) -> PgResult<bool> {
    let enable_tidscan = backend_utils_misc_guc_tables::vars::enable_tidscan.read();
    backend_optimizer_path_small::create_tidscan_paths(root, rel, enable_tidscan)
}

/// `get_rel_persistence(relid)` (lsyscache.c).
fn get_rel_persistence(relid: Oid) -> i8 {
    seams::get_rel_persistence::call(relid)
}

/// `set_foreign_size_estimates(root, rel)` (costsize.c).
fn set_foreign_size_estimates(root: &mut PlannerInfo, rel: RelId) {
    seams::set_foreign_size_estimates::call(root, rel)
}

/// FDW `GetForeignRelSize` dispatch (fdwapi.h).
fn foreign_get_rel_size(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::fdw_get_foreign_rel_size::call(root, rel, rte::rte_relid::call(root, rti))
}
/// FDW `GetForeignPaths` dispatch (fdwapi.h).
fn foreign_get_paths(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::fdw_get_foreign_paths::call(root, rel, rte::rte_relid::call(root, rti))
}
/// FDW `IsForeignScanParallelSafe` dispatch (fdwapi.h).
fn foreign_scan_parallel_safe(root: &PlannerInfo, rel: RelId, rti: Index) -> bool {
    seams::fdw_is_foreign_scan_parallel_safe::call(root, rel, rti)
}

/// TABLESAMPLE method `SampleScanGetSampleSize` dispatch (tsmapi.h).
fn tablesample_get_sample_size(
    root: &PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<(u32, f64)> {
    seams::tsm_get_sample_size::call(root, rel, rti)
}
/// TABLESAMPLE `GetTsmRoutine(...)->repeatable_across_scans` (tsmapi.h).
fn tablesample_repeatable_across_scans(root: &PlannerInfo, rti: Index) -> PgResult<bool> {
    seams::tsm_repeatable_across_scans::call(root, rti)
}
/// TABLESAMPLE function + args parallel-safety (`func_parallel` + `is_parallel_safe`).
fn tablesample_is_parallel_safe(root: &PlannerInfo, rti: Index) -> bool {
    seams::tsm_is_parallel_safe::call(root, rti)
}

/// `limit_needed(subquery)` over the subquery RTE (limit.c / planner.c).
fn subquery_limit_needed(root: &PlannerInfo, rti: Index) -> bool {
    seams::subquery_limit_needed::call(root, rti)
}
/// `is_parallel_safe(root, (Node *) rte->functions)` for a function RTE.
fn rel_functions_parallel_safe(root: &PlannerInfo, rti: Index) -> bool {
    seams::rte_functions_parallel_safe::call(root, rti)
}
/// `is_parallel_safe(root, (Node *) rte->values_lists)` for a VALUES RTE.
fn rel_values_parallel_safe(root: &PlannerInfo, rti: Index) -> bool {
    seams::rte_values_lists_parallel_safe::call(root, rti)
}
/// `is_parallel_safe(root, (Node *) rel->baserestrictinfo)`.
fn baserestrictinfo_parallel_safe(root: &PlannerInfo, rel: RelId) -> bool {
    seams::rel_baserestrictinfo_parallel_safe::call(root, rel)
}
/// `is_parallel_safe(root, (Node *) rel->reltarget->exprs)`.
fn reltarget_exprs_parallel_safe(root: &PlannerInfo, rel: RelId) -> bool {
    seams::rel_reltarget_parallel_safe::call(root, rel)
}

/// Build the ordinality pathkey for a `WITH ORDINALITY` function scan: the
/// `Var` for the ordinality column with int8 ascending sort, only if it is
/// already mentioned in some EquivalenceClass.
fn build_ordinality_pathkeys(
    root: &mut PlannerInfo,
    rel: RelId,
) -> Vec<types_pathnodes::PathKey> {
    let ordattno: AttrNumber = root.rel(rel).max_attr;
    let relid = root.rel(rel).relid;

    // Is there a Var for the ordinality column in rel's targetlist?
    let mut found_var: Option<Expr> = None;
    let exprs = root.rel(rel).reltarget.as_ref().map(|t| t.exprs.clone()).unwrap_or_default();
    for nid in exprs {
        if let Expr::Var(v) = root.node(nid) {
            if v.varattno == ordattno && v.varno == relid as i32 && v.varlevelsup == 0 {
                found_var = Some(Expr::Var(v.clone()));
                break;
            }
        }
    }

    match found_var {
        Some(var) => {
            let relids = bms::relids_copy::call(&root.rel(rel).relids);
            backend_optimizer_path_pathkeys::build_expression_pathkey(
                root,
                &var,
                INT8_LESS_OPERATOR,
                &relids,
                false,
            )
        }
        None => Vec::new(),
    }
}

/* ==========================================================================
 * init_seams (install allpaths-owned seams)
 * ======================================================================== */

/// Install the seams allpaths.c owns: `compute_parallel_worker` and
/// `create_partial_bitmap_paths` (declared in costsize-seams; consumed by
/// costsize/indxpath), and `build_and_cost_join_rel` (declared in geqo-all-seams;
/// consumed by geqo).
pub fn init_seams() {
    costsize::compute_parallel_worker::set(compute_parallel_worker_seam);
    costsize::create_partial_bitmap_paths::set(create_partial_bitmap_paths);
    backend_geqo_all_seams::build_and_cost_join_rel::set(build_and_cost_join_rel_seam);
}

/// Seam adapter for `compute_parallel_worker` (the seam takes `&PlannerInfo`;
/// the body needs no mutation).
fn compute_parallel_worker_seam(
    root: &PlannerInfo,
    rel: RelId,
    heap_pages: f64,
    index_pages: f64,
    max_workers: i32,
) -> i32 {
    compute_parallel_worker(root, rel, heap_pages, index_pages, max_workers)
}

/// `geqo_eval.c:merge_clump` body — `make_join_rel` then (on success)
/// `generate_partitionwise_join_paths` + topmost-guarded
/// `generate_useful_gather_paths` + `set_cheapest`. The seam carries no `Mcx`,
/// so we run the join build in a fresh local planner memory context (the path
/// work allocates into the `PlannerInfo` arena; the `Mcx` is only the OOM
/// channel for `make_join_rel`'s fallible reserves).
fn build_and_cost_join_rel_seam(
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
) -> Option<RelId> {
    let cx = mcx::MemoryContext::new("geqo merge_clump");
    let mcx = cx.mcx();
    build_and_cost_join_rel(mcx, root, rel1, rel2)
        .unwrap_or_else(|e| panic!("build_and_cost_join_rel: {e:?}"))
}

/// `build_and_cost_join_rel` — the join-build-plus-finishing body shared by GEQO
/// `merge_clump`. Returns the joinrel handle, or `None` if the join is invalid.
pub fn build_and_cost_join_rel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
) -> PgResult<Option<RelId>> {
    let joinrel = match make_join_rel(mcx, root, rel1, rel2)? {
        Some(r) => r,
        None => return Ok(None),
    };

    // Create paths for partitionwise joins.
    generate_partitionwise_join_paths(mcx, root, joinrel)?;

    // Except for the topmost scan/join rel, consider gathering partial paths.
    if !bms::relids_equal::call(&root.rel(joinrel).relids, &root.all_query_rels) {
        generate_useful_gather_paths(root, joinrel, false)?;
    }

    // Find and save the cheapest paths for this rel.
    pathnode::set_cheapest::call(root, joinrel)?;

    Ok(Some(joinrel))
}

/* ==========================================================================
 * Owner-absent dependency seams (declared here, installed by owners on landing;
 * registered in seams-init's CONTRACT_RECONCILE_PENDING until then).
 * ======================================================================== */

pub mod seams;
