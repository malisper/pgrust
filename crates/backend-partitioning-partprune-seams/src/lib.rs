//! Seam declarations for the `backend-partitioning-core` unit's
//! `partitioning/partprune.c` boundary — `get_matching_partitions`, the
//! runtime pruning evaluator `execPartition.c`'s
//! `find_matching_subplans_recurse` calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

extern crate alloc;

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::partition::PartitionPruneContext;
use types_nodes::{Bitmapset, EStateData};

seam_core::seam!(
    /// `prune_append_rel_partitions(rel)` (partprune.c:723): perform
    /// compile-time partition pruning of a partitioned baserel using its
    /// `baserestrictinfo`, returning the `PartitionDesc` indexes of the
    /// surviving partitions (the C `Bitmapset *`). inherit.c's
    /// `expand_partitioned_rtentry` calls it to initialize `rel->live_parts`.
    /// Owned by `partprune.c`, which is keystone-blocked on the
    /// `PartitionPruneStep` carrier (see the partprune-blocked memory note), so
    /// this currently panics when reached.
    pub fn prune_append_rel_partitions<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        rel: types_pathnodes::RelId,
    ) -> PgResult<types_pathnodes::Relids>
);

seam_core::seam!(
    /// `make_partition_pruneinfo(root, parentrel, subpaths, prunequal)`
    /// (partprune.c:226): build a `PartitionPruneInfo` describing how the
    /// executor should prune `subpaths` of the partitioned `parentrel` using
    /// `prunequal`, append it to `root->glob->partPruneInfos`, and return its
    /// list index (stored into the Append/MergeAppend `part_prune_index`).
    /// `subpaths` are `PathId` handles into the planner path arena; `prunequal`
    /// are bare-clause expression-node handles. Owned by `partprune.c`, which is
    /// keystone-blocked on the `PartitionPruneStep` carrier (see the
    /// partprune-blocked memory note), so this currently panics when reached —
    /// the planner only reaches it for a partitioned rel with a non-empty
    /// prunequal.
    pub fn make_partition_pruneinfo(
        root: &mut types_pathnodes::PlannerInfo,
        parentrel: types_pathnodes::RelId,
        subpaths: &[types_pathnodes::PathId],
        prunequal: &[types_pathnodes::NodeId],
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_matching_partitions(context, pruning_steps)` (partprune.c): run the
    /// pruning steps against the current comparison values and return the set
    /// of surviving partition indexes (a `None` result is the C NULL/empty
    /// set). The context's lazily-resolved `stepcmpfuncs` are filled in place,
    /// and pruning-expression evaluation reads the EState (the owned model
    /// threads it where C reaches it via `context->exprcontext->ecxt_estate`);
    /// the result allocates in `mcx` (C: `context->ppccontext`). `Err` carries
    /// the comparison/eval `ereport(ERROR)`s and OOM.
    pub fn get_matching_partitions<'mcx>(
        mcx: Mcx<'mcx>,
        context: &mut PartitionPruneContext<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>
);
