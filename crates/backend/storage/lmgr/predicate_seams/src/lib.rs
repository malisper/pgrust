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
    pub fn predicate_lock_page_combine<'a, 'mcx>(
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
    pub fn check_for_serializable_conflict_out_needed<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        snapshot: &'a SnapshotData<'mcx>,
    ) -> PgResult<bool>
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
    pub fn check_point_predicate() -> PgResult<()>
);

seam_core::seam!(
    pub fn check_for_serializable_conflict_in<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        tid: Option<&'a ItemPointerData>,
        blkno: types_core::BlockNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn get_serializable_transaction_snapshot<'a>(
        snapshot: &'a mut SnapshotData<'static>,
        mcx: mcx::Mcx<'static>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn release_predicate_locks(is_commit: bool, is_read_only_safe: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn check_table_for_serializable_conflict_in<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // C gates inside on PredXact->SxactGlobalXmin, not caller isolation — call unconditionally.
    pub fn transfer_predicate_locks_to_heap_relation<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ShareSerializableXact (predicate.c): C's SerializableXactHandle is a
    // SERIALIZABLEXACT* into shared memory; threads share the address space,
    // so it crosses to workers as a usize (0 = InvalidSerializableXact).
    pub fn share_serializable_xact() -> usize
);

seam_core::seam!(
    pub fn attach_serializable_xact(handle: usize) -> PgResult<()>
);

seam_core::seam!(
    // SetSerializableTransactionSnapshot (predicate.c): the parallel-worker
    // no-op arm and READ ONLY DEFERRABLE rejection are ported; the
    // snapshot-import arm (SET TRANSACTION SNAPSHOT) stays loud.
    pub fn set_serializable_transaction_snapshot() -> PgResult<()>
);
