//! Seam declarations for the `backend-partitioning-partdesc` unit
//! (`partitioning/partdesc.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

use mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::nodes::partition::PartitionDescData;
use ::rel::Relation;

seam_core::seam!(
    /// `RelationGetPartitionDesc(rel, omit_detached)` (partdesc.c:70): build (or
    /// reuse) the partition descriptor for a partitioned relation. Reached from
    /// the partition-constraint builder (`get_qual_for_{list,range}`'s DEFAULT
    /// branch in partbounds.c) across the partbounds → partdesc dependency edge
    /// that a direct dep would cycle. Allocates the descriptor in `mcx`; the
    /// catalog scan / bound build can `ereport(ERROR)`, carried on `Err`.
    pub fn relation_get_partition_desc<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        omit_detached: bool,
    ) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>>
);
