//! Seam declarations for the `backend-executor-execPartition` unit
//! (`executor/execPartition.c`): the run-time partition-pruning entry points
//! that `Append`/`MergeAppend` nodes consult.
//!
//! The owning unit installs these from its `init_seams()`. The C
//! `PlanState.state` back-pointer is the owned-tree's explicit `estate` param
//! (the owned model threads `EState` rather than carrying the back-pointer);
//! allocation in the per-query context makes the constructors fallible.

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
    /// `ExecFindMatchingSubPlans(prunestate, initial_prune, &validsubplan_rtis)`
    /// (execPartition.c): determine the minimum set of subplans matching the
    /// current parameter values, returning the surviving subplan indices. The
    /// MergeAppend caller passes `NULL` for `validsubplan_rtis`, so the RT-index
    /// out-parameter is not modeled. Building the result set allocates in `mcx`.
    pub fn exec_find_matching_subplans<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        prunestate: &mut types_nodes::PartitionPruneState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        initial_prune: bool,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `ExecSetupPartitionTupleRouting(estate, rel)` (execPartition.c): build
    /// the `PartitionTupleRouting` for a partitioned target relation `rel`
    /// (an open-relation alias). Allocates the routing structure in the
    /// EState's per-query context; fallible on OOM and on catalog
    /// `ereport(ERROR)`.
    pub fn exec_setup_partition_tuple_routing<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        rel: types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::PartitionTupleRouting<'mcx>>>
);

seam_core::seam!(
    /// `ExecFindPartition(mtstate, rootResultRelInfo, proute, slot, estate)`
    /// (execPartition.c): find or initialize the leaf-partition
    /// `ResultRelInfo` that `slot`'s tuple routes to, returning its id in the
    /// EState result-rel pool. May initialize a new partition's
    /// `ResultRelInfo` (allocating + firing relcache/trigger setup), so
    /// fallible.
    pub fn exec_find_partition<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        root_result_rel_info: types_nodes::RriId,
        proute: &mut types_nodes::PartitionTupleRouting<'mcx>,
        slot: types_nodes::SlotId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_nodes::RriId>
);

seam_core::seam!(
    /// `ExecCleanupTupleRouting(mtstate, proute)` (execPartition.c): tear down
    /// the tuple-routing state at executor shutdown (close partitions, free
    /// per-partition resources). Closing relations can `ereport(ERROR)`.
    pub fn exec_cleanup_tuple_routing<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        proute: &mut types_nodes::PartitionTupleRouting<'mcx>,
    ) -> types_error::PgResult<()>
);
