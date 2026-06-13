//! Seam declarations for the `backend-partitioning-core` unit's
//! `partitioning/partprune.c` boundary — `get_matching_partitions`, the
//! runtime pruning evaluator `execPartition.c`'s
//! `find_matching_subplans_recurse` calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::partition::PartitionPruneContext;
use types_nodes::{Bitmapset, EStateData};

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
