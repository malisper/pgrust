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
use ::nodes::partition::PartitionPruneContext;
use nodes::{Bitmapset, EStateData};

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
        run: &pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut pathnodes::PlannerInfo,
        rel: pathnodes::RelId,
    ) -> PgResult<pathnodes::Relids>
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
    pub fn make_partition_pruneinfo<'mcx>(
        run: &pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut pathnodes::PlannerInfo,
        parentrel: pathnodes::RelId,
        subpaths: &[pathnodes::PathId],
        prunequal: &[pathnodes::NodeId],
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_matching_partitions(context, pruning_steps)` (partprune.c): run the
    /// pruning steps against the current comparison values and return the set
    /// of surviving partition indexes (a `None` result is the C NULL/empty
    /// set). `pruning_steps` is the executor's per-partrel step list (the
    /// `Opaque`-carried `PartitionPruneStep` payload, downcast by the caller);
    /// the context's lazily-resolved `stepcmpfuncs` are filled in place, and
    /// non-Const step expressions are evaluated via `context.exprstates` over
    /// `context.exprcontext` (`partkey_datum_from_expr`'s ExprState leg), reading
    /// the EState where C reaches it via `context->exprcontext->ecxt_estate`; the
    /// result allocates in `mcx` (C: `context->ppccontext`). `Err` carries the
    /// comparison/eval `ereport(ERROR)`s and OOM.
    ///
    /// NOT YET INSTALLED — the run-time kernel-evaluation leg (the
    /// `partkey_datum_from_expr` ExprState evaluation + the bound-math over the
    /// executor's `PartitionPruneContext` for all three strategies) is the
    /// remaining follow-on. The whole planner -> setrefs -> execMain pipeline is
    /// wired and reaches this seam; a generic-plan `WHERE a = $1` errors here
    /// until the body lands.
    pub fn get_matching_partitions<'mcx>(
        mcx: Mcx<'mcx>,
        context: &mut PartitionPruneContext<'mcx>,
        pruning_steps: &[::nodes::partprune_carrier::PartitionPruneStep<'mcx>],
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>
);
