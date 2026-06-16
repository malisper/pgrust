//! Seam declarations for the `backend-catalog-storage` unit (`storage.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::RelFileLocator;

/// `CreateDBRelInfo` (`dbcommands.c`): one relation to be copied when creating
/// a database — its physical identifier, oid, and permanence. Produced by the
/// cross-database `pg_class` scan ([`scan_source_database_pg_class`]) and
/// consumed by [`create_and_copy_relation_data`].
#[derive(Clone, Copy, Debug)]
pub struct CreateDBRelInfo {
    /// `RelFileLocator rlocator` — physical relation identifier.
    pub rlocator: RelFileLocator,
    /// `Oid reloid` — relation oid.
    pub reloid: Oid,
    /// `bool permanent` — relation is permanent (vs. unlogged).
    pub permanent: bool,
}

seam_core::seam!(
    /// `ScanSourceDatabasePgClass(tbid, dbid, srcpath)` (dbcommands.c): the
    /// cross-database raw buffered scan of the source database's `pg_class`
    /// relation (`RelationMapOidToFilenumberForDatabase` + `LockRelationId` +
    /// `smgropen`/`smgrnblocks` + `GetAccessStrategy(BAS_BULKREAD)` +
    /// `RegisterSnapshot(GetLatestSnapshot())` + the block-by-block
    /// `ReadBufferWithoutRelcache` / `LockBuffer` / page-item walk gated by
    /// `HeapTupleSatisfiesVisibility`, with `ScanSourceDatabasePgClassTuple`'s
    /// shared/storage/temp filter folded in). Returns the list of relations to
    /// copy. The whole buffer/smgr/snapshot/visibility engine — none of which
    /// the command layer owns — stays behind this seam (storage.c's domain),
    /// exactly as `src-idiomatic` collapses it. Can `ereport(ERROR)`.
    pub fn scan_source_database_pg_class<'mcx>(
        mcx: Mcx<'mcx>,
        tbid: Oid,
        dbid: Oid,
        srcpath: &str,
    ) -> PgResult<PgVec<'mcx, CreateDBRelInfo>>
);

seam_core::seam!(
    /// `CreateAndCopyRelationData(src_rlocator, dst_rlocator, permanent_data)`
    /// (storage.c): create destination relation storage and copy every fork
    /// block-by-block from the source, WAL-logging each block
    /// (`RelationCreateStorage` + `RelationCopyStorageUsingBuffer` per fork +
    /// `smgrimmedsync` for permanent rels). Owned by storage.c. Can
    /// `ereport(ERROR)`.
    pub fn create_and_copy_relation_data(
        src_rlocator: RelFileLocator,
        dst_rlocator: RelFileLocator,
        permanent: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `smgr_redo(record)` (storage.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn smgr_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelFileLocatorSkippingWAL(rlocator)` (storage.c): true if the relation
    /// is in the `pendingDeletes`/`pendingSyncs` set such that WAL is being
    /// skipped for its current relfilenode this transaction
    /// (`wal_skip_threshold`). Pure in-memory hash lookup; cannot `ereport`.
    pub fn rel_file_locator_skipping_wal(rlocator: RelFileLocator) -> bool
);

seam_core::seam!(
    /// `smgrDoPendingSyncs(isCommit, isParallelWorker)` — fsync files created
    /// and not WAL-logged in this transaction.
    pub fn smgr_do_pending_syncs(is_commit: bool, is_parallel_worker: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `smgrDoPendingDeletes(isCommit)` — drop files scheduled for deletion.
    pub fn smgr_do_pending_deletes(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `smgrGetPendingDeletes(forCommit, &ptr)` — list the non-temp relation
    /// files this transaction will delete; allocated in `mcx` (C: palloc in
    /// the caller's context).
    pub fn smgr_get_pending_deletes<'mcx>(
        mcx: Mcx<'mcx>,
        for_commit: bool,
    ) -> PgResult<PgVec<'mcx, RelFileLocator>>
);

seam_core::seam!(
    /// `AtSubCommit_smgr()` — reparent pending deletes to the parent subxact.
    pub fn at_subcommit_smgr()
);

seam_core::seam!(
    /// `AtSubAbort_smgr()` — delete files created in the aborted subxact.
    pub fn at_subabort_smgr() -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_smgr()` — forget pending deletes (2PC takes over).
    pub fn post_prepare_smgr()
);

seam_core::seam!(
    /// `DropRelationFiles(delrels, ndelrels, isRedo=false)` (storage.c) — drop
    /// the physical files a finished prepared transaction was supposed to
    /// delete. Can `ereport(ERROR)`, carried on `Err`.
    pub fn drop_relation_files(rels: &[types_wal::RelFileLocator]) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationDropStorage(relation)` (storage.c): schedule the relation's
    /// current main-fork storage (named by `rlocator`/`backend`) for unlink at
    /// transaction commit (adds it to `pendingDeletes`). Used by
    /// `RelationSetNewRelfilenumber` when *not* in binary-upgrade mode. The
    /// relcache owns the relation entry, so its physical identity is passed
    /// explicitly. `Err` carries its `ereport(ERROR)`s.
    pub fn relation_drop_storage(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<()>
);

seam_core::seam!(
    /// The binary-upgrade old-storage drop in `RelationSetNewRelfilenumber`
    /// (relcache.c): `srel = smgropen(rlocator, backend);
    /// smgrdounlinkall(&srel, 1, false); smgrclose(srel)` — immediately (not at
    /// commit) unlink the relation's existing files, as required during
    /// `pg_upgrade`. The relcache owns the relation entry, so its physical
    /// identity is passed explicitly. `Err` carries its `ereport(ERROR)`s.
    pub fn smgr_unlink_relation_now(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `srel = RelationCreateStorage(newrlocator, persistence, true);
    /// smgrclose(srel)` (storage.c), as called by `RelationSetNewRelfilenumber`
    /// for a `RELKIND_HAS_STORAGE` but non-table-AM relation: create the main
    /// fork of the new relfilenumber's storage and close the transient smgr
    /// handle. `Err` carries its `ereport(ERROR)`s.
    pub fn relation_create_storage_main_fork(
        newrlocator: RelFileLocator,
        relpersistence: i8,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The pg_class-update leg of `RelationSetNewRelfilenumber` (relcache.c) for
    /// a non-mapped relation: `table_open(pg_class)`,
    /// `SearchSysCacheLockedCopy1(RELOID, relid)`, set
    /// `relfilenode = new_relfilenumber` and (for non-sequence relkinds) reset
    /// `relpages/reltuples/relallvisible/relallfrozen`, set
    /// `relfrozenxid/relminmxid/relpersistence`, then `CatalogTupleUpdate` +
    /// `UnlockTuple` + `heap_freetuple` + `table_close`. The whole pg_class
    /// tuple lifecycle is the catalog owner's; the relcache passes the already
    /// dispatched-on values. `Err` carries its `ereport(ERROR)`s.
    pub fn update_pg_class_relfilenumber(
        relid: Oid,
        new_relfilenumber: Oid,
        relpersistence: i8,
        relkind: i8,
        freeze_xid: u32,
        minmulti: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationPreserveStorage(rlocator, atCommit)` (storage.c) — protect the
    /// physical file named by `rlocator` from deletion at transaction
    /// end/abort. relmapper calls this with `atCommit=false` for each mapped
    /// file when committing a relmap update, inside a critical section.
    pub fn relation_preserve_storage(
        rlocator: RelFileLocator,
        at_commit: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The storage-creation leg of `heapam_relation_set_new_filelocator`
    /// (heapam_handler.c): `srel = RelationCreateStorage(newrlocator,
    /// persistence, true)`, then for an unlogged relation
    /// (`RELPERSISTENCE_UNLOGGED`) `smgrcreate(srel, INIT_FORKNUM, false)` +
    /// `log_smgrcreate(newrlocator, INIT_FORKNUM)`, finally `smgrclose(srel)`.
    /// Owned by `storage.c`; the transient `SMgrRelation` handle never crosses
    /// the boundary (it is created and closed entirely inside the owner), so the
    /// heap AM only supplies the locator + persistence. `Err` carries the
    /// `ereport(ERROR)`s of storage creation / WAL logging.
    pub fn relation_set_new_filelocator_storage(
        newrlocator: RelFileLocator,
        relpersistence: i8,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The init-fork creation leg of `fill_seq_with_data` (sequence.c) for an
    /// unlogged sequence: `srel = smgropen(rlocator, INVALID_PROC_NUMBER);
    /// smgrcreate(srel, INIT_FORKNUM, false); log_smgrcreate(&rlocator,
    /// INIT_FORKNUM)`. `log_smgrcreate` is owned by `storage.c`; the transient
    /// `SMgrRelation` handle never crosses the boundary (smgropen is idempotent
    /// over the shared smgr cache, and the matching `smgrclose` is done by the
    /// caller through `relation_close_smgr` after the fork is filled+flushed).
    /// `Err` carries the `ereport(ERROR)`s of fork creation / WAL logging.
    pub fn smgr_create_init_fork_and_log(rlocator: RelFileLocator) -> PgResult<()>
);
