//! Seam declarations for reading and mutating `pg_database` rows, owned by
//! `backend-catalog-pg-database`.
//!
//! READ seams (postinit.c): `GetDatabaseTuple`/`GetDatabaseTupleByOid` open
//! `pg_database` and scan it (`table_open` + `systable_beginscan` +
//! `systable_getnext` + `heap_copytuple` + `systable_endscan` + `table_close`);
//! `CheckMyDatabase` re-reads the same row via `SearchSysCache1(DATABASEOID,
//! ...)`. Decoding the variable-length locale columns
//! (`datcollate`/`datctype`/`datlocale`/`datcollversion`) requires the catalog
//! read + fmgr/varlena layer the consuming crate does not own, so each read
//! crosses as one batched call that returns a decoded [`FormPgDatabase`] (the
//! `heap_copytuple` analog: an owned copy in `mcx`). `None` is the C
//! invalid-tuple / cache-miss result.
//!
//! MUTATE seams (the read-modify-write surface dbcommands.c needs): createdb
//! forms one row from a [`NewDbRecord`] and `CatalogTupleInsert`s it; dropdb
//! marks the row invalid with an in-place update then `CatalogTupleDelete`s it
//! transactionally; the `ALTER DATABASE` family takes the inplace-update tuple
//! lock, re-forms the row with one or more columns changed, and
//! `CatalogTupleUpdate`s it. The owner forms/deforms the `pg_database` tuple
//! against the relation descriptor (the consumer never touches the on-disk
//! layout), exactly the per-catalog typed pattern `backend-catalog-indexing`'s
//! F1 family uses.
//!
//! The owning unit (`backend-catalog-pg-database`) installs these from its
//! `init_seams()`; until then a call panics loudly.

use mcx::Mcx;
use types_catalog::pg_database::{FormPgDatabase, NewDbRecord};
use types_core::primitive::Oid;
use types_error::PgResult;
use rel::Relation;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `GetDatabaseTuple(dbname)` (postinit.c): scan `pg_database` by name
    /// (`Anum_pg_database_datname` = `dbname`, index `DatabaseNameIndexId`
    /// when the critical shared relcache is built, else seqscan). Returns the
    /// decoded row, or `None` if no such database. `Err` carries the
    /// scan/catalog-open `ereport(ERROR)` surface plus OOM from the copy.
    pub fn get_database_tuple_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        dbname: &str,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);

seam_core::seam!(
    /// `GetDatabaseTupleByOid(dboid)` (postinit.c): as above, scanning by OID
    /// (`Anum_pg_database_oid` = `dboid`, index `DatabaseOidIndexId`).
    pub fn get_database_tuple_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid))` +
    /// `GETSTRUCT`/`SysCacheGetAttr*` decode (postinit.c `CheckMyDatabase`):
    /// read our own `pg_database` row through the syscache. Returns the
    /// decoded row, or `None` on a cache miss. `Err` carries the syscache
    /// lookup's `ereport(ERROR)` surface plus OOM.
    pub fn search_database_syscache<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);

/* ==========================================================================
 * MUTATE seams — the read-modify-write surface dbcommands.c needs.
 * ========================================================================== */

seam_core::seam!(
    /// `heap_form_tuple(RelationGetDescr(pg_database_rel), new_record,
    /// new_record_nulls)` + `CatalogTupleInsert(pg_database_rel, tuple)`
    /// (createdb, dbcommands.c). The owner forms the `pg_database` tuple from
    /// the [`NewDbRecord`] column values against the relation descriptor
    /// (`datacl` is always NULL at create time), allocates the row OIDs through
    /// `CatalogIndexInsert`, and stamps the heap. The caller has already chosen
    /// `record.oid` (createdb's `GetNewOidWithIndex` + file-conflict retry loop
    /// is dbcommands logic) and opened `rel` RowExclusiveLock. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s plus OOM.
    pub fn insert_pg_database<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        record: &NewDbRecord<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleDelete(pg_database_rel, &tup->t_self)` (dropdb,
    /// dbcommands.c): delete the addressed `pg_database` row transactionally.
    /// The caller has opened `rel` RowExclusiveLock and holds the row's TID
    /// (from the in-place invalidate that precedes the delete). `Err` carries
    /// the heap-mutation `ereport(ERROR)`s.
    pub fn delete_pg_database<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: ItemPointerData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_modify_tuple` / `GETSTRUCT`-write + `CatalogTupleUpdate(rel, otid,
    /// newtuple)` + `UnlockTuple(rel, &otid, InplaceUpdateTupleLock)` (the
    /// `ALTER DATABASE` family, dbcommands.c): re-form the `pg_database` row at
    /// `otid` from the (caller-modified) decoded [`FormPgDatabase`], update it
    /// transactionally with index maintenance, and release the inplace-update
    /// tuple lock that [`scan_pg_database_locked_for_update`] acquired. Every C
    /// `ALTER DATABASE` path releases the lock on the line immediately after
    /// `CatalogTupleUpdate`, so the two are folded into one seam (no lock is
    /// held across the consumer's `?`). A full re-form from the decoded row is
    /// behaviour-identical to C's `heap_modify_tuple` over the changed columns.
    /// `Err` carries the heap/index-mutation `ereport(ERROR)`s plus OOM.
    pub fn update_pg_database<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        otid: ItemPointerData,
        form: &FormPgDatabase<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `systable_beginscan(DatabaseNameIndexId/DatabaseOidIndexId, key)` +
    /// `LockTuple(rel, &tup->t_self, InplaceUpdateTupleLock)` +
    /// `heap_copytuple` read that opens every `ALTER DATABASE` read-modify-write
    /// (and the `SearchSysCacheLockedCopy1(DATABASEOID, db_id)` rename path,
    /// dbcommands.c). Scans `pg_database` by OID (when `by_oid`) or by name,
    /// acquires the `InplaceUpdateTupleLock` (`LockTuple`, held until the
    /// matching [`update_pg_database`] releases it — the lmgr models this
    /// heavyweight tuple lock imperatively, released by the transaction's
    /// resource owner, never a scope guard), and returns the row's `t_self`
    /// (passed back to [`update_pg_database`]) plus the decoded modifiable
    /// [`FormPgDatabase`]. `None` when no such row exists (no lock acquired).
    /// The caller has opened `rel` RowExclusiveLock; `my_database_id` is C's
    /// `MyDatabaseId`, passed explicitly (no ambient-global seam). `Err`
    /// carries the scan/lock `ereport(ERROR)`s plus OOM.
    pub fn scan_pg_database_locked_for_update<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        my_database_id: Oid,
        by_oid: bool,
        dboid: Oid,
        dbname: &str,
    ) -> PgResult<Option<(ItemPointerData, FormPgDatabase<'mcx>)>>
);

seam_core::seam!(
    /// The dropdb in-place invalidate (dbcommands.c): `ScanKeyInit(datname =
    /// dbname)` → `systable_inplace_update_begin(pgdbrel, DatabaseNameIndexId,
    /// true, NULL, 1, &scankey, &tup, &state)` → `datform->datconnlimit =
    /// DATCONNLIMIT_INVALID_DB` → `systable_inplace_update_finish(state, tup)`
    /// → `XLogFlush(XactLastRecEnd)`. Marks the named `pg_database` row invalid
    /// without an MVCC update (so an interrupted DROP leaves the row unusable
    /// but droppable again), returning the row's `t_self` for the transactional
    /// `CatalogTupleDelete` that follows, or `None` if the name found no live
    /// row. The owner runs the whole genam in-place flow (the buffer lock never
    /// crosses the seam). `Err` carries the in-place-update `ereport(ERROR)`s.
    pub fn set_pg_database_invalid_inplace<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        dbname: &str,
    ) -> PgResult<Option<ItemPointerData>>
);

seam_core::seam!(
    /// `aclnewowner(DatumGetAclP(datacl), olddba, newowner)` (AlterDatabaseOwner,
    /// dbcommands.c): re-assign ownership of the existing `datacl` aclitem[] from
    /// `old_owner_id` to `new_owner_id`, returning the re-encoded aclitem[]
    /// varlena bytes for the updated `pg_database` row. `datacl` crosses the
    /// pg_database carrier as opaque varlena bytes by design; decoding the
    /// aclitem[] and re-encoding the result needs the catalog/fmgr/varlena layer
    /// the command layer does not own, so this whole `aclnewowner` step is one
    /// owner call. `Err` carries the decode/`ereport(ERROR)` surface plus OOM.
    pub fn aclnewowner_datacl<'mcx>(
        mcx: Mcx<'mcx>,
        old_datacl: &[u8],
        old_owner_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `SetDatabaseHasLoginEventTriggers()` (event_trigger.c:389-421): set
    /// `pg_database.dathasloginevt` for the current database (`MyDatabaseId`).
    /// `table_open(DatabaseRelationId, RowExclusiveLock)` +
    /// `LockSharedObject(DatabaseRelationId, MyDatabaseId, 0,
    /// AccessExclusiveLock)` + `SearchSysCacheLockedCopy1(DATABASEOID,
    /// MyDatabaseId)`; if `!dathasloginevt` set it + `CatalogTupleUpdate` +
    /// `CommandCounterIncrement`; `UnlockTuple(InplaceUpdateTupleLock)` +
    /// `table_close`. `Err` carries the catalog-mutation `ereport(ERROR)`s.
    pub fn set_database_has_login_event_triggers(mcx: Mcx<'_>) -> PgResult<()>
);

seam_core::seam!(
    /// `EventTriggerOnLogin`'s stale-flag reset (event_trigger.c:951-985): clear
    /// `pg_database.dathasloginevt` for the current database in place. The caller
    /// already holds `ConditionalLockSharedObject(DatabaseRelationId,
    /// MyDatabaseId, 0, AccessExclusiveLock)` and rechecked the login run-list is
    /// empty. `table_open(RowExclusiveLock)` +
    /// `systable_inplace_update_begin/finish/cancel(DatabaseOidIndexId)` (set the
    /// flag false iff currently true) + `table_close`. `Err` carries the catalog
    /// `ereport(ERROR)`s.
    pub fn reset_database_has_login_event_triggers(mcx: Mcx<'_>) -> PgResult<()>
);
