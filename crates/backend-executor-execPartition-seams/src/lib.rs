//! Seam declarations for the `backend-executor-execPartition` unit
//! (`executor/execPartition.c`): the run-time partition-pruning entry points
//! that `Append`/`MergeAppend` nodes consult.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The C `PlanState.state` back-pointer is the
//! owned-tree's explicit `estate`; allocation in the per-query context makes the
//! constructors fallible.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitPartitionExecPruning(planstate, n_total_subplans,
    /// part_prune_index, relids, &initially_valid_subplans)` (execPartition.c):
    /// set up the run-time pruning data structure for `planstate`, returning the
    /// new `PartitionPruneState` and, via the second tuple element, the set of
    /// subplans that survived initial pruning (the C out-parameter
    /// `initially_valid_subplans`). Allocates the prune state and the result set
    /// in the per-query context, hence `mcx` and `PgResult`.
    pub fn exec_init_partition_exec_pruning<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        n_total_subplans: i32,
        part_prune_index: i32,
        relids: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<(
        mcx::PgBox<'mcx, types_nodes::PartitionPruneState<'mcx>>,
        Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
    )>
);

seam_core::seam!(
    /// `ExecFindMatchingSubPlans(prunestate, initial_prune, &validsubplan_rtis)`
    /// (execPartition.c): determine the minimum set of subplans matching the
    /// current parameter values, returning the surviving subplan indices. The
    /// MergeAppend caller passes `NULL` for `validsubplan_rtis`, so the RT-index
    /// out-parameter is not modeled. Building the result set allocates in `mcx`.
    pub fn exec_find_matching_subplans<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        prunestate: &mut types_nodes::PartitionPruneState<'mcx>,
        initial_prune: bool,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);
