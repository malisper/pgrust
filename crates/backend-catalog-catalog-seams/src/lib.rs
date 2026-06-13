//! Seam declarations for the `backend-catalog-catalog` unit
//! (`catalog/catalog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `IsPinnedObject(classId, objectId)` (catalog/catalog.c): is the object
    /// required for basic database functionality? Pure OID-range arithmetic —
    /// infallible.
    pub fn is_pinned_object(class_id: Oid, object_id: Oid) -> bool
);

seam_core::seam!(
    /// `IsSharedRelation(relationId)` (catalog/catalog.c): is the relation a
    /// shared catalog (lives in the global tablespace, visible from every
    /// database)? Lookup against a fixed OID set — infallible.
    pub fn is_shared_relation(relation_id: Oid) -> bool
);

seam_core::seam!(
    /// `GetDatabasePath(dbOid, spcOid)` (catalog/catalog.c): build the
    /// filesystem path to the directory holding `dbOid`'s relations in
    /// tablespace `spcOid`. relmapper's `relmap_redo` calls it during WAL
    /// replay, uses the path transiently, then `pfree`s it (the returned owned
    /// `String` is dropped). Path construction allocates, so the result is
    /// fallible (OOM).
    pub fn get_database_path(db_oid: Oid, spc_oid: Oid) -> PgResult<String>
);
