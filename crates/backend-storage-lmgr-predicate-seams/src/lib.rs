//! Seam declarations for the `backend-storage-lmgr-predicate` unit
//! (`storage/lmgr/predicate.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `PredicateLockPage(relation, blkno, snapshot)` (predicate.c): acquire a
    /// page-level predicate (SIREAD) lock, as an index-only scan must when it
    /// returns a tuple without visiting the heap. The snapshot is the
    /// active-snapshot token owned by snapmgr. Can `ereport(ERROR)`.
    pub fn predicate_lock_page<'mcx>(
        relation: types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PredicateLockPageSplit(relation, oldblkno, newblkno)` (predicate.c):
    /// copy the predicate (SIREAD) locks from `oldblkno` to `newblkno` when a
    /// hash bucket page splits, so serializable conflicts are preserved across
    /// the split. Keyed on the relation by OID; can `ereport(ERROR)`.
    pub fn predicate_lock_page_split(
        index_oid: types_core::primitive::Oid,
        old_blkno: types_core::primitive::BlockNumber,
        new_blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckForSerializableConflictIn(relation, NULL, blkno)` (predicate.c):
    /// the page-granularity rw-conflict check the hash AM performs on the
    /// primary bucket page before inserting. `Err` carries the
    /// serialization-failure `ereport(ERROR)`.
    pub fn check_for_serializable_conflict_in_page(
        index_oid: types_core::primitive::Oid,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetSerializableTransactionSnapshot(snapshot)` (predicate.c) — get a
    /// snapshot for a serializable transaction, registering it with the
    /// predicate-locking machinery. C fills the caller's `SnapshotData`; here
    /// the seam takes the freshly-fetched snapshot and returns the registered
    /// (possibly adjusted) one. Allocates / can `ereport(ERROR)`.
    pub fn get_serializable_transaction_snapshot(
        snapshot: types_snapshot::SnapshotData,
    ) -> types_error::PgResult<types_snapshot::SnapshotData>
);

seam_core::seam!(
    /// `SetSerializableTransactionSnapshot(snapshot, sourcevxid, sourcepid)`
    /// (predicate.c) — adopt an imported snapshot into the serializable
    /// machinery (used by `SET TRANSACTION SNAPSHOT`).
    pub fn set_serializable_transaction_snapshot(
        snapshot: types_snapshot::SnapshotData,
        sourcevxid: types_storage::VirtualTransactionId,
        sourcepid: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PredicateLockRelation(relation, snapshot)` (predicate.c): take a
    /// predicate (SIREAD) lock on the whole index relation for a serializable
    /// transaction. Called by `index_beginscan_internal` when the AM does not
    /// handle predicate locks itself (`!ampredlocks`). The owner keys on the
    /// relation by OID; `Err` carries the C `ereport(ERROR)` (e.g. out of
    /// shared memory for the lock).
    pub fn predicate_lock_relation(
        index_oid: types_core::primitive::Oid,
        snapshot: &types_snapshot::SnapshotData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber)`
    /// (predicate.c): the relation-granularity rw-conflict check
    /// `index_insert` performs when the AM does not handle predicate locks
    /// itself. `Err` carries the serialization-failure `ereport(ERROR)`.
    pub fn check_for_serializable_conflict_in(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `predicatelock_twophase_recover(xid, info, recdata, len)` — restore a
    /// prepared transaction's SIREAD predicate locks at recovery (slot
    /// `TWOPHASE_RM_PREDICATELOCK_ID` of `twophase_recover_callbacks`).
    pub fn predicatelock_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RegisterPredicateLockingXid(xid)` — tell the predicate locking system
    /// the top-level transaction's XID.
    pub fn register_predicate_locking_xid(
        xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PreCommit_CheckForSerializationFailure()` — raise a serialization
    /// failure detected at commit time.
    pub fn pre_commit_check_for_serialization_failure() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_PredicateLocks()`.
    pub fn at_prepare_predicate_locks() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_PredicateLocks(xid)`.
    pub fn post_prepare_predicate_locks(
        xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PredicateLockTwoPhaseFinish(xid, isCommit)` (predicate.c) — release the
    /// SIREAD predicate locks held by a finishing prepared transaction. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn predicate_lock_twophase_finish(
        xid: types_core::primitive::TransactionId,
        is_commit: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `TransferPredicateLocksToHeapRelation(relation)` (predicate.c): promote
    /// tuple/page predicate locks to a relation lock before the rewrite.
    pub fn transfer_predicate_locks_to_heap_relation(relid: types_core::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PredicateLockShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn predicate_lock_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `PredicateLockShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn predicate_lock_shmem_init() -> types_error::PgResult<()>
);
