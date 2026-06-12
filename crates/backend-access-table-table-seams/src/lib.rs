//! Seam declarations for the `backend-access-table-table` unit
//! (`access/table/table.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The C `Relation` (`struct RelationData *`) crosses these seams as the
//! relation's `Oid`: the relcache store owns the open-relation state and
//! re-resolves the OID; field reads go through the relcache owner's seams.

seam_core::seam!(
    /// `table_open(relationId, lockmode)` (access/table/table.c): open a table
    /// relation by OID — `relation_open` plus a check that the relation is not
    /// an index nor a composite type. A relation that cannot be opened raises
    /// `ereport(ERROR)` inside the owner.
    pub fn table_open(
        relation_id: types_core::primitive::Oid,
        lockmode: types_tuple::access::LOCKMODE,
    ) -> types_core::primitive::Oid
);

seam_core::seam!(
    /// `table_close(relation, lockmode)` (access/table/table.c): close a table
    /// previously opened with `table_open`. If `lockmode` is not `NoLock`, the
    /// specified lock is then released.
    pub fn table_close(
        relation: types_core::primitive::Oid,
        lockmode: types_tuple::access::LOCKMODE,
    )
);
