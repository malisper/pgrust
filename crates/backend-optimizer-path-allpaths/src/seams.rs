//! Owner-absent dependency seams for allpaths.c.
//!
//! These cross owners whose crates are not yet ported in this repo:
//! * **plancat.c** — `relation_excluded_by_constraints`;
//! * **lsyscache.c** — `get_rel_persistence`;
//! * **costsize.c** `set_foreign_size_estimates` (the foreign-table variant is
//!   not yet a pub fn on the costsize crate);
//! * **FDW dispatch** (fdwapi.h) — `GetForeignRelSize`/`GetForeignPaths`/
//!   `IsForeignScanParallelSafe`;
//! * **TABLESAMPLE method dispatch** (tsmapi.h) — `SampleScanGetSampleSize`,
//!   `GetTsmRoutine(...)->repeatable_across_scans`, the sample-fn parallel
//!   safety check;
//! * **planner.c / clauses.c** parallel-safety probes over Query/RTE subtrees
//!   (`limit_needed`, `is_parallel_safe` over the RTE's functions/values_lists
//!   and the rel's baserestrictinfo/reltarget).
//!
//! Each is declared with `seam!` and `set` by its real owner once it lands;
//! until then they are registered (uninstalled, loud-panic-on-call) in
//! seams-init's `CONTRACT_RECONCILE_PENDING`. Every signature is lifetime-free
//! over the arena handles, mirroring the C call exactly.

use types_core::primitive::{Index, Oid};
use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PathId, PlannerInfo, RelId};

seam_core::seam!(
    /// `relation_excluded_by_constraints(root, rel, rte)` (plancat.c). The RTE
    /// is identified by its 1-based range-table index. Takes `&mut PlannerInfo`
    /// because the C body allocates constraint expressions into the arena and may
    /// set `rel->partition_qual` via `set_baserel_partition_constraint`.
    ///
    /// Threads the planner-run resolver (`run`): the body reads RTE fields
    /// through the re-signed `rte_*` seams that now take `&PlannerRun<'mcx>`.
    pub fn relation_excluded_by_constraints<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
        rti: Index,
    ) -> bool
);

seam_core::seam!(
    /// `get_rel_persistence(relid)` (lsyscache.c) — the relation persistence
    /// char (`RELPERSISTENCE_TEMP`/`PERMANENT`/`UNLOGGED`).
    pub fn get_rel_persistence(relid: Oid) -> i8
);

seam_core::seam!(
    /// `set_foreign_size_estimates(root, rel)` (costsize.c). Not yet a pub fn on
    /// the landed costsize crate.
    pub fn set_foreign_size_estimates(root: &mut PlannerInfo, rel: RelId)
);
seam_core::seam!(
    /// `set_values_size_estimates(root, rel)` (costsize.c). Not yet a pub fn on
    /// the landed costsize crate.
    pub fn set_values_size_estimates(root: &mut PlannerInfo, rel: RelId)
);
seam_core::seam!(
    /// `set_subquery_size_estimates(root, rel)` (costsize.c). Used by the
    /// (keystone-blocked) subquery vertical; declared here for completeness.
    pub fn set_subquery_size_estimates(root: &mut PlannerInfo, rel: RelId)
);

seam_core::seam!(
    /// FDW `GetForeignRelSize(root, rel, relid)` (fdwapi.h dispatch).
    pub fn fdw_get_foreign_rel_size(
        root: &mut PlannerInfo,
        rel: RelId,
        relid: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// FDW `GetForeignPaths(root, rel, relid)` (fdwapi.h dispatch).
    pub fn fdw_get_foreign_paths(
        root: &mut PlannerInfo,
        rel: RelId,
        relid: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// FDW `IsForeignScanParallelSafe(root, rel, rte)` (fdwapi.h dispatch).
    pub fn fdw_is_foreign_scan_parallel_safe(
        root: &PlannerInfo,
        rel: RelId,
        rti: Index,
    ) -> bool
);

// NOTE: TABLESAMPLE `SampleScanGetSampleSize` / `repeatable_across_scans`
// dispatch is NOT a seam here. The estimation bodies + the GetTsmRoutine
// registry live in `backend-access-tablesample-core` (the access AM owner),
// which `set_tablesample_rel_{size,pathlist}` call directly after navigating
// `rte->tablesample->{tsmhandler,args}` through the rte-seams.
seam_core::seam!(
    /// TABLESAMPLE sample-function + args parallel safety
    /// (`func_parallel(tsc->tsmhandler) == PROPARALLEL_SAFE` and
    /// `is_parallel_safe(root, tsc->args)`).
    pub fn tsm_is_parallel_safe(root: &PlannerInfo, rti: Index) -> bool
);

seam_core::seam!(
    /// `limit_needed(rte->subquery)` (planner.c) over a subquery RTE.
    pub fn subquery_limit_needed(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) rte->functions)` (clauses.c) over a
    /// function RTE's `RangeTblFunction` list.
    pub fn rte_functions_parallel_safe(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) rte->values_lists)` (clauses.c) over a
    /// VALUES RTE.
    pub fn rte_values_lists_parallel_safe(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) rel->baserestrictinfo)` (clauses.c).
    pub fn rel_baserestrictinfo_parallel_safe(root: &PlannerInfo, rel: RelId) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) rel->reltarget->exprs)` (clauses.c).
    pub fn rel_reltarget_parallel_safe(root: &PlannerInfo, rel: RelId) -> bool
);

seam_core::seam!(
    /// `partitions_are_ordered(rel->boundinfo, rel->live_parts)` (partbounds.c)
    /// — can the partitions be read in a guaranteed sort order (allowing an
    /// Append instead of a MergeAppend)?
    pub fn partitions_are_ordered(root: &PlannerInfo, rel: RelId) -> bool
);
seam_core::seam!(
    /// `get_cheapest_fractional_path(rel, tuple_fraction)` (pathkeys.c) — the
    /// cheapest path of `rel` for retrieving `tuple_fraction` of its rows.
    pub fn get_cheapest_fractional_path(
        root: &PlannerInfo,
        rel: RelId,
        tuple_fraction: f64,
    ) -> PathId
);

extern crate alloc;
