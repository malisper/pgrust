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
        planstate: &mut nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        n_total_subplans: i32,
        part_prune_index: i32,
        relids: Option<&nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<(
        mcx::PgBox<'mcx, nodes::PartitionPruneState<'mcx>>,
        Option<mcx::PgBox<'mcx, nodes::Bitmapset<'mcx>>>,
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
        prunestate: &mut nodes::PartitionPruneState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        initial_prune: bool,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `ExecSetupPartitionTupleRouting(estate, rel)` (execPartition.c): build
    /// the `PartitionTupleRouting` for a partitioned target relation `rel`
    /// (an open-relation alias). Allocates the routing structure in the
    /// EState's per-query context; fallible on OOM and on catalog
    /// `ereport(ERROR)`.
    pub fn exec_setup_partition_tuple_routing<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        rel: rel::Relation<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, nodes::PartitionTupleRouting<'mcx>>>
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
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        root_result_rel_info: nodes::RriId,
        proute: &mut nodes::PartitionTupleRouting<'mcx>,
        slot: nodes::SlotId,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<nodes::RriId>
);

seam_core::seam!(
    /// `ExecCleanupTupleRouting(mtstate, proute)` (execPartition.c): tear down
    /// the tuple-routing state at executor shutdown (close partitions, free
    /// per-partition resources). Closing relations can `ereport(ERROR)`.
    pub fn exec_cleanup_tuple_routing<'mcx>(
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        proute: &mut nodes::PartitionTupleRouting<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `adjust_partition_colnos(colnos, leaf_part_rri)` (execPartition.c): adjust
    /// an UPDATE target column-number list for the attribute differences between
    /// the parent and the partition, using the leaf's child→root conversion map.
    /// `ExecInitPartitionInfo`'s ON CONFLICT DO UPDATE leg calls it on
    /// `node->onConflictCols` when the partition rowtype differs from the root.
    /// Must not be called when no adjustment is required (the C `Assert(map !=
    /// NULL)`). Returns the freshly-mapped colno list allocated in `mcx`. Fallible
    /// (`elog(ERROR)` on an unexpected attno, OOM).
    pub fn adjust_partition_colnos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        colnos: &[i32],
        leaf_part_rri: nodes::RriId,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, i32>>
);

seam_core::seam!(
    /// `adjust_partition_colnos_using_map(colnos, attrMap)` (execPartition.c):
    /// like [`adjust_partition_colnos`], but with a caller-supplied attribute
    /// map (the `attmap->attnums` slice) instead of the leaf's child→root map.
    /// `ExecInitPartitionInfo`'s MERGE leg calls it on each UPDATE action's
    /// `updateColnos` using the freshly-built `build_attrmap_by_name(partrel,
    /// firstResultRel)` map. Must not be called when no adjustment is required
    /// (the C `Assert(attrMap != NULL)`). Returns the freshly-mapped colno list
    /// allocated in `mcx`. Fallible (`elog(ERROR)` on an unexpected attno, OOM).
    pub fn adjust_partition_colnos_using_map<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        colnos: &[i32],
        attnums: &[i16],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, i32>>
);

seam_core::seam!(
    /// `ExecDoInitialPruning(estate)` (execPartition.c): perform run-time
    /// "initial" (executor-startup) partition pruning for every
    /// `PartitionPruneInfo` in `estate->es_part_prune_infos`, building each
    /// `PartitionPruneState` (appended to `es_part_prune_states`), storing the
    /// surviving-subplan bitmapset (or `None`) in `es_part_prune_results`, and
    /// accumulating the surviving leaf RT indexes into `es_unpruned_relids`.
    /// `InitPlan` calls it right after installing `es_part_prune_infos`.
    /// Allocates in the per-query context and can `ereport(ERROR)` (pruning
    /// evaluation), hence `mcx` and `PgResult`.
    pub fn exec_do_initial_pruning<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
