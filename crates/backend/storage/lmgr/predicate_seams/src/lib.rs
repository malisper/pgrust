use types_core::{Buffer, TransactionId};
use types_error::PgResult;
use types_rel::RelationData;
use types_snapshot::SnapshotData;
use types_tuple::ItemPointerData;

seam_core::seam!(
    pub fn predicate_lock_relation<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        snapshot: &'a SnapshotData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn predicate_lock_tid<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        tid: ItemPointerData,
        snapshot: &'a SnapshotData<'mcx>,
        tuple_xid: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    // The open Relation crosses whole so the impl reads rd_id/persistence off
    // the pointer in hand (no relcache re-open per page).
    pub fn check_for_serializable_conflict_out_needed<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        snapshot: &'a SnapshotData<'mcx>,
    ) -> bool
);

seam_core::seam!(
    pub fn check_for_serializable_conflict_out<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        xid: TransactionId,
        snapshot: &'a SnapshotData<'mcx>,
    ) -> PgResult<()>
);
