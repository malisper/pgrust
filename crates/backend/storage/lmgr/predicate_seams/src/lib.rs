use types_core::TransactionId;
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
    pub fn predicate_lock_page<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        blkno: types_core::BlockNumber,
        snapshot: &'a SnapshotData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn predicate_lock_page_split<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        oldblkno: types_core::BlockNumber,
        newblkno: types_core::BlockNumber,
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

seam_core::seam!(
    pub fn pre_commit_check_for_serialization_failure() -> PgResult<()>
);

seam_core::seam!(
    pub fn register_predicate_locking_xid(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_prepare_predicate_locks() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_predicate_locks(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    // CheckPointPredicate() (predicate.c).
    pub fn check_point_predicate() -> PgResult<()>
);

seam_core::seam!(
    // CheckForSerializableConflictIn (predicate.c); InvalidBlockNumber = relation-level only.
    pub fn check_for_serializable_conflict_in<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        tid: Option<&'a ItemPointerData>,
        blkno: types_core::BlockNumber,
    ) -> PgResult<()>
);
