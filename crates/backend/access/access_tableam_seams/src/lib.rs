//! Seam declarations for the `backend-access-tableam` unit (`access/table/
//! tableam.c`, `tableam.h`). The owning unit installs these from its
//! `init_seams()` when the cross-crate-cycle paths land; until then a call
//! panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use ::types_cluster::CopyForClusterResult;
use types_core::{MultiXactId, TransactionId};
use ::types_error::PgResult;
use ::rel::Relation;

seam_core::seam!(
    /// `table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
    /// OldestXmin, &FreezeXid, &MultiXactCutoff, &num_tuples, &tups_vacuumed,
    /// &tups_recently_dead)` (tableam.h): AM-specific heap rewrite.
    pub fn table_relation_copy_for_cluster<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        old_heap: &Relation<'mcx>,
        new_heap: &Relation<'mcx>,
        old_index: Option<&Relation<'mcx>>,
        use_sort: bool,
        oldest_xmin: TransactionId,
        freeze_xid: TransactionId,
        multixact_cutoff: MultiXactId,
    ) -> PgResult<CopyForClusterResult>
);
