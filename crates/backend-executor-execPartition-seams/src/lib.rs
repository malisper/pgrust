//! Seam declarations for the `backend-executor-execPartition` unit
//! (`executor/execPartition.c`): the run-time partition-pruning entry points
//! the Append/MergeAppend nodes call.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. `PartitionPruneState` lives in
//! [`types_nodes::nodeappend`] (trimmed to what the Append node reads); the
//! pruning structures allocate in the executor's per-query context, so the
//! allocating entry point takes the target `Mcx` and is fallible on OOM.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitPartitionExecPruning(planstate, n_total_subplans,
    /// part_prune_index, relids, &initially_valid_subplans)`
    /// (execPartition.c): set up the run-time pruning data structure for an
    /// Append/MergeAppend and compute the initially-valid subplan set (the C
    /// out-parameter `initially_valid_subplans`, returned here as the tuple's
    /// second element). Allocates in `mcx`; can `ereport(ERROR)` while
    /// evaluating initial-prune expressions.
    pub fn exec_init_partition_exec_pruning<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        n_total_subplans: i32,
        part_prune_index: i32,
        relids: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<(
        mcx::PgBox<'mcx, types_nodes::PartitionPruneState<'mcx>>,
        Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
    )>
);

seam_core::seam!(
    /// `ExecFindMatchingSubPlans(prunestate, initial_prune,
    /// validsubplan_rtis)` (execPartition.c): determine the set of subplans
    /// that match the current parameter values (the `validsubplan_rtis`
    /// out-parameter is unused by the Append callers, so it is not modeled).
    /// Allocates the result set in `mcx`; evaluating prune expressions can
    /// `ereport(ERROR)`.
    pub fn exec_find_matching_subplans<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        prunestate: &mut types_nodes::PartitionPruneState<'mcx>,
        initial_prune: bool,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);
