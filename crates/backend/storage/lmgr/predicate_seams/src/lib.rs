//! Seam declarations for the `backend-storage-lmgr-predicate` unit
//! (`storage/lmgr/predicate.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `ShareSerializableXact(void)` (predicate.c:5046): return
    /// `MySerializableXact`, the backend's `SERIALIZABLEXACT *` handle, so a
    /// parallel leader can share it with its workers. Carried as the raw machine
    /// word (`usize`); `InvalidSerializableXact` (NULL) is `0`, the value outside
    /// a serializable transaction. Pure read of backend-local predicate state.
    pub fn share_serializable_xact() -> usize
);

seam_core::seam!(
    /// `PredicateLockPage(relation, blkno, snapshot)` (predicate.c): acquire a
    /// page-level predicate (SIREAD) lock, as an index-only scan must when it
    /// returns a tuple without visiting the heap. The snapshot is the
    /// active-snapshot token owned by snapmgr. Can `ereport(ERROR)`.
    pub fn predicate_lock_page<'mcx>(
        relation: rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
        snapshot: Option<std::rc::Rc<snapshot::SnapshotData>>,
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
    /// `PredicateLockPageCombine(relation, oldblkno, newblkno)` (predicate.c):
    /// transfer the predicate (SIREAD) locks from a page about to be unlinked
    /// (`oldblkno`) onto its right sibling (`newblkno`), so any insert that
    /// would have landed on the removed page is still covered. Reached by GIN
    /// `ginDeletePage`. Keyed on the relation by OID; can `ereport(ERROR)`.
    pub fn predicate_lock_page_combine(
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
        snapshot: snapshot::SnapshotData,
    ) -> types_error::PgResult<snapshot::SnapshotData>
);

seam_core::seam!(
    /// `SetSerializableTransactionSnapshot(snapshot, sourcevxid, sourcepid)`
    /// (predicate.c) — adopt an imported snapshot into the serializable
    /// machinery (used by `SET TRANSACTION SNAPSHOT`).
    pub fn set_serializable_transaction_snapshot(
        snapshot: snapshot::SnapshotData,
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
        snapshot: &snapshot::SnapshotData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckForSerializableConflictOutNeeded(relation, snapshot)` (predicate.c):
    /// whether a `HeapCheckForSerializableConflictOut` call would do anything for
    /// this relation under `snapshot` (true only for a serializable transaction
    /// scanning a non-catalog, non-temp relation). Keyed by the relation OID;
    /// the predicate never errors.
    pub fn check_for_serializable_conflict_out_needed(
        relation_oid: types_core::primitive::Oid,
        snapshot: &snapshot::SnapshotData,
    ) -> bool
);

seam_core::seam!(
    /// `HeapCheckForSerializableConflictOut(visible, relation, tuple, buffer,
    /// snapshot)` (predicate.c): the read-side rw-conflict check a heap scan
    /// performs per tuple in a serializable transaction. `visible` is the tuple's
    /// visibility under `snapshot`; the owner inspects the tuple's xmin/xmax to
    /// register or raise the conflict. Keyed by the relation OID; `Err` carries
    /// the serialization-failure `ereport(ERROR)`.
    pub fn heap_check_for_serializable_conflict_out(
        visible: bool,
        relation_oid: types_core::primitive::Oid,
        tuple: &types_tuple::heaptuple::HeapTupleData<'_>,
        buffer: types_storage::Buffer,
        snapshot: &snapshot::SnapshotData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckForSerializableConflictOut(relation, xid, snapshot)` (predicate.c):
    /// the predicate.c engine entry that registers the read-side rw-conflict for
    /// a tuple written by transaction `xid`. `HeapCheckForSerializableConflictOut`
    /// (heapam.c) resolves the conflicting `xid` off the tuple's visibility and
    /// calls this. Keyed by the relation OID; `Err` carries the
    /// serialization-failure `ereport(ERROR)`.
    pub fn check_for_serializable_conflict_out(
        relation_oid: types_core::primitive::Oid,
        xid: types_core::primitive::TransactionId,
        snapshot: &snapshot::SnapshotData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckForSerializableConflictIn(relation, tid, blkno)` (predicate.c):
    /// the rw-conflict check a write performs against the predicate (SIREAD)
    /// locks held on a tuple/page/relation. The engine checks, in order, the
    /// tuple tag (when `tid` is `Some`), the page tag (when `blkno !=
    /// InvalidBlockNumber`), then always the relation tag — a reader's
    /// finer-grained SIREAD lock is only seen by the matching tuple/page check,
    /// so writes must pass the precise target:
    ///   * `index_insert` passes `(None, InvalidBlockNumber)` (relation-only,
    ///     when the AM does not handle predicate locks itself);
    ///   * nbtree `_bt_doinsert`/`_bt_check_unique` pass `(None, leaf_blkno)`
    ///     so a page-level reader lock masked by a unique violation is reported;
    ///   * `heap_delete`/`heap_update` pass `(Some(old_tid), old_blkno)` so a
    ///     concurrent reader's tuple-level SIREAD lock yields the write-skew
    ///     serialization failure.
    /// `Err` carries the serialization-failure `ereport(ERROR)`.
    pub fn check_for_serializable_conflict_in(
        index_oid: types_core::primitive::Oid,
        tid: Option<(
            types_core::primitive::BlockNumber,
            types_core::primitive::OffsetNumber,
        )>,
        blkno: types_core::primitive::BlockNumber,
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

seam_core::seam!(
    /// `PredicateLockTID(relation, tid, snapshot, tuple_xid)` (predicate.c):
    /// the tuple-granularity SIREAD predicate lock a heap fetch / HOT-chain
    /// search takes after finding a visible tuple, so a serializable
    /// transaction registers the read for rw-conflict detection. The owner
    /// short-circuits when `!SerializationNeededForRead(relation, snapshot)`,
    /// so this is a no-op outside serializable transactions. Keyed by the
    /// relation OID; `Err` carries the out-of-shared-memory `ereport(ERROR)`.
    /// Owner unported; scaffolded slot (panics until predicate.c lands).
    pub fn predicate_lock_tid(
        relation_oid: types_core::primitive::Oid,
        tid: types_tuple::heaptuple::ItemPointerData,
        snapshot: &snapshot::SnapshotData,
        tuple_xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckTableForSerializableConflictIn(relation)` (predicate.c): when a
    /// whole table is deleted/truncated in a serializable transaction, record
    /// an rw-conflict in to this transaction from each transaction holding a
    /// predicate lock on the table (a relation-granularity sweep). Called by
    /// `heap_drop_with_catalog`. `Err` carries the serialization-failure
    /// `ereport(ERROR)`.
    pub fn check_table_for_serializable_conflict_in(
        relation: &rel::Relation<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetPredicateLockStatusData()` projected to lockfuncs.c's `pg_locks` rows
    /// (the SIREAD predicate leg of `pg_lock_status`). The target-tag decode
    /// (`GET_PREDICATELOCKTARGETTAG_*`) and the holder-`SERIALIZABLEXACT` fields
    /// are predicate.c-internal, so the seam yields each row already projected to
    /// the scalar [`types_storage::lock::PredLockStatusRow`] columns the listing
    /// function emits. The vector lives in `mcx` (the SRF result context). `Err`
    /// carries OOM / lock-acquisition failure.
    pub fn predicate_lock_status_rows<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::lock::PredLockStatusRow>>
);
