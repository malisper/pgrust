//! Seam declarations for the `backend-access-table-table` unit
//! (`access/table/table.c`).
//!
//! The owner (`backend-access-table-table`) installs these from its
//! `init_seams()`. The C `Relation` crosses as an owned
//! [`types_tuple::rel::RelationData`] carrier: `table_open` copies the
//! consumed relcache fields into the caller's `mcx`; `table_close` consumes
//! the carrier (the C pointer is dead after close).

seam_core::seam!(
    /// `table_open(relationId, lockmode)` (access/table/table.c): open a table
    /// relation by OID — `relation_open` plus a check that the relation is not
    /// an index nor a composite type. The returned carrier is allocated in
    /// `mcx`. `Err` carries the C `ereport(ERROR)`s: a relation that cannot
    /// be opened (`could not open relation with OID %u`), a wrong relation
    /// kind, a lock-acquisition error, or OOM copying the entry.
    pub fn table_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_tuple::rel::RelationData<'mcx>>
);

seam_core::seam!(
    /// `table_close(relation, lockmode)` (access/table/table.c): close a table
    /// previously opened with `table_open`, consuming the carrier. If
    /// `lockmode` is not `NoLock`, the specified lock is then released;
    /// `LockRelease` can `elog(ERROR)` (`unrecognized lock mode`, `failed to
    /// re-find shared lock object`), carried on `Err`.
    pub fn table_close(
        relation: types_tuple::rel::RelationData<'_>,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);
