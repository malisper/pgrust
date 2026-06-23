//! External dependencies of `replication/logical/origin.c` that origin.c only
//! *calls into*: the `pg_replication_origin` catalog/syscache/heapam/genam
//! tuple machinery, the lmgr object/relation locks, WAL insertion +
//! `XLogFlush`, the transaction/recovery predicates, `on_shmem_exit`
//! registration, the set-returning-function / tuplestore plumbing, and the
//! checkpoint-file transient I/O + CRC32C.
//!
//! None of these owners is ported yet, and origin.c's use of each is a tight
//! composite (e.g. the `replorigin_create` free-id scan loop interleaves
//! `systable_beginscan`/`systable_getnext`/`systable_endscan` with a
//! `heap_form_tuple`/`CatalogTupleInsert`/`CommandCounterIncrement`). Until
//! the owners land with a vocabulary rich enough to express each leg
//! (`Relation`, `HeapTuple`, `SysScanDesc`, `Form_pg_replication_origin`,
//! transient-file descriptors, CRC32C), origin reaches them through these
//! batched seams, which panic loudly until installed (mirror-PG-and-panic).
//!
//! Each seam signature mirrors the C failure surface: every operation that
//! can `ereport` at ERROR/PANIC returns [`types_error::PgResult`].

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::{Oid, RepOriginId, XLogRecPtr};
use types_error::PgResult;
use types_storage::LWLockMode;

// ---------------------------------------------------------------------------
// ReplicationOriginLock — one of the fixed individual LWLocks in the shared
// MainLWLockArray (owned by lwlock.c). origin.c only acquires/releases it.
// Modeled as acquire/release seams (vs. a `LWLockAcquireMain` guard) because
// origin.c's protocol releases it explicitly mid-function and re-acquires it
// across a `goto restart` loop, which a scope guard cannot express.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `LWLockAcquire(ReplicationOriginLock, mode)`. Can `ereport(ERROR)` via
    /// `CHECK_FOR_INTERRUPTS` on the wait path.
    pub fn LWLockAcquireReplicationOriginLock(mode: LWLockMode) -> PgResult<()>
);
seam_core::seam!(
    /// `LWLockRelease(ReplicationOriginLock)`.
    pub fn LWLockReleaseReplicationOriginLock() -> PgResult<()>
);

// ---------------------------------------------------------------------------
// catalog / syscache / heapam / genam — pg_replication_origin tuple machinery.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `SearchSysCache1(REPLORIGNAME, CStringGetTextDatum(roname))` →
    /// `ident->roident`, or `None` when no tuple is cached.
    pub fn syscache_roident_by_name(roname: &str) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(REPLORIGIDENT, ObjectIdGetDatum(roident))` →
    /// `text_to_cstring(&ric->roname)`, or `None` when no tuple is cached.
    pub fn syscache_roname_by_oid(roident: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `replorigin_create`'s catalog work (origin.c lines 300-361):
    /// `table_open(ReplicationOriginRelationId, ExclusiveLock)`, the dirty-
    /// snapshot `systable_beginscan`/`systable_getnext`/`systable_endscan`
    /// scan over `roident = InvalidOid+1 .. PG_UINT16_MAX` for the first
    /// unused id (with `CHECK_FOR_INTERRUPTS` each iteration), the
    /// `heap_form_tuple` + `CatalogTupleInsert` + `CommandCounterIncrement`
    /// of the new row, and `table_close(rel, ExclusiveLock)`. Returns the
    /// chosen roident, or `None` when every id collided (origin.c's
    /// `tuple == NULL`).
    pub fn create_catalog_insert(roname: &str) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `table_open(ReplicationOriginRelationId, RowExclusiveLock)`.
    pub fn drop_open_relation() -> PgResult<()>
);
seam_core::seam!(
    /// Whether the `SearchSysCache1(REPLORIGIDENT, roident)` tuple exists
    /// (`HeapTupleIsValid`).
    pub fn drop_tuple_exists(roident: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `CatalogTupleDelete(rel, &tuple->t_self)` + `ReleaseSysCache(tuple)` +
    /// `CommandCounterIncrement()` for the located REPLORIGIDENT tuple.
    pub fn drop_delete_tuple(roident: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `table_close(rel, RowExclusiveLock)` — the already-dropped
    /// early-return close (origin.c line 467).
    pub fn drop_close_relation_keep_unlocked() -> PgResult<()>
);
seam_core::seam!(
    /// `table_close(rel, NoLock)` — success-path close, keeping the lock to
    /// commit (origin.c line 482).
    pub fn drop_close_relation_nolock() -> PgResult<()>
);

// ---------------------------------------------------------------------------
// lock manager (lmgr) — object & relation locks on ReplicationOriginRelationId.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `LockSharedObject(ReplicationOriginRelationId, roident, 0,
    /// AccessExclusiveLock)`.
    pub fn LockSharedObjectOrigin(roident: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `UnlockSharedObject(ReplicationOriginRelationId, roident, 0,
    /// AccessExclusiveLock)`.
    pub fn UnlockSharedObjectOrigin(roident: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock)`.
    pub fn LockRelationOidOrigin() -> PgResult<()>
);
seam_core::seam!(
    /// `UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock)`.
    pub fn UnlockRelationOidOrigin() -> PgResult<()>
);

// ---------------------------------------------------------------------------
// WAL insertion (xloginsert.c / xlog.c).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xlrec));
    /// XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET)` for an
    /// `xl_replorigin_set { remote_lsn, node_id, force }`.
    pub fn wal_insert_replorigin_set(
        remote_lsn: XLogRecPtr,
        node_id: RepOriginId,
        force: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xlrec));
    /// XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_DROP)` for an
    /// `xl_replorigin_drop { node_id }`.
    pub fn wal_insert_replorigin_drop(node_id: RepOriginId) -> PgResult<()>
);
seam_core::seam!(
    /// `XLogFlush(record)` — make sure WAL up to `record` is on disk.
    pub fn XLogFlush(record: XLogRecPtr) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// transaction / recovery predicates (xact.c / xlog.c).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `IsTransactionState()`.
    pub fn IsTransactionState() -> PgResult<bool>
);
seam_core::seam!(
    /// `RecoveryInProgress()`.
    pub fn RecoveryInProgress() -> PgResult<bool>
);

// ---------------------------------------------------------------------------
// ipc.c — process-exit callback registration.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `on_shmem_exit(ReplicationOriginExitCleanup, 0)`.
    pub fn register_origin_exit_cleanup() -> PgResult<()>
);

// ---------------------------------------------------------------------------
// funcapi.c / tuplestore.c — set-returning-function plumbing.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `InitMaterializedSRF(fcinfo, 0)` for `pg_show_replication_origin_status`.
    pub fn InitMaterializedSRF() -> PgResult<()>
);
seam_core::seam!(
    /// `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values,
    /// nulls)` for one `pg_show_replication_origin_status` row. `external_id`
    /// is `None` for the C `nulls[1] = true` case (origin concurrently
    /// dropped).
    pub fn put_replication_origin_status_row(
        local_id: Oid,
        external_id: Option<String>,
        remote_lsn: XLogRecPtr,
        local_lsn: XLogRecPtr,
    ) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// checkpoint file I/O + CRC32C (storage/fd.c, storage/file.c, port/pg_crc32c).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `CheckPointReplicationOrigin`'s file write (origin.c lines 598-710):
    /// unlink stale temp, `OpenTransientFile` the temp, write the magic, write
    /// each `ReplicationStateOnDisk { roident, remote_lsn }`, write the CRC32C,
    /// `CloseTransientFile`, and `durable_rename` to the permanent path. Any
    /// I/O error is the C `ereport(PANIC)`.
    pub fn checkpoint_write(
        states: Vec<(RepOriginId, XLogRecPtr)>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `StartupReplicationOrigin`'s file read (origin.c lines 747-846):
    /// `OpenTransientFile` the checkpoint path (`Ok(None)` is the ENOENT
    /// early-return — no checkpoint yet / fresh standby), verify the magic and
    /// the trailing CRC32C, and decode each `ReplicationStateOnDisk` into a
    /// `(roident, remote_lsn)` pair in file order. Magic mismatch / short read
    /// / checksum failure is the C `ereport(PANIC)`.
    pub fn checkpoint_read() -> PgResult<Option<Vec<(RepOriginId, XLogRecPtr)>>>
);
