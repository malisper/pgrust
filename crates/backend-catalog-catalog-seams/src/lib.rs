//! Seam declarations for the `backend-catalog-catalog` unit
//! (`catalog/catalog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::{Oid, RelFileNumber};
use types_error::PgResult;

seam_core::seam!(
    /// `IsPinnedObject(classId, objectId)` (catalog/catalog.c): is the object
    /// required for basic database functionality? Pure OID-range arithmetic —
    /// infallible.
    pub fn is_pinned_object(class_id: Oid, object_id: Oid) -> bool
);

seam_core::seam!(
    /// `IsCatalogRelationOid(relid)` (catalog/catalog.c): a relation is a
    /// system catalog iff it has a pinned OID (`relid < FirstUnpinnedObjectId`).
    /// Pure OID-range arithmetic — infallible.
    pub fn is_catalog_relation_oid(relid: Oid) -> bool
);

seam_core::seam!(
    /// `IsCatalogRelation(relation)` (catalog/catalog.c): is this relation a
    /// system catalog (`IsCatalogRelationOid(RelationGetRelid(relation))`)?
    /// Pure OID-range check; infallible.
    pub fn is_catalog_relation(relation: &types_rel::RelationData<'_>) -> bool
);

seam_core::seam!(
    /// `IsToastRelation(relation)` (catalog/catalog.c): is this relation in the
    /// `pg_toast` namespace (`relation->rd_rel->relnamespace ==
    /// PG_TOAST_NAMESPACE`)? Pure field read; infallible.
    pub fn is_toast_relation(relation: &types_rel::RelationData<'_>) -> bool
);

seam_core::seam!(
    /// `IsSharedRelation(relationId)` (catalog/catalog.c): is the relation a
    /// shared catalog (lives in the global tablespace, visible from every
    /// database)? Lookup against a fixed OID set — infallible.
    pub fn is_shared_relation(relation_id: Oid) -> bool
);

seam_core::seam!(
    /// `IsCatalogNamespace(namespaceId)` (catalog/catalog.c): true iff the
    /// namespace is `pg_catalog` (`namespaceId == PG_CATALOG_NAMESPACE`). No
    /// catalog access — infallible.
    pub fn is_catalog_namespace(namespace_id: Oid) -> bool
);

seam_core::seam!(
    /// `RelationInvalidatesSnapshotsOnly(relid)` (catalog/catalog.c): for the
    /// few catalogs whose tuples affect only saved snapshots (not catcache or
    /// relcache), this returns true so inval.c queues a snapshot inval instead.
    /// Pure OID compare; infallible.
    pub fn relation_invalidates_snapshots_only(relation_id: Oid) -> bool
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

seam_core::seam!(
    /// `GetNewRelFileNumber(reltablespace, pg_class, relpersistence)`
    /// (catalog/catalog.c): allocate a brand-new relfilenumber unused by any
    /// existing relation in the target tablespace, probing the filesystem to
    /// avoid collisions. The relcache caller passes `NULL` for the `pg_class`
    /// argument (it isn't used for the search), so only the tablespace and
    /// persistence cross. `Err` carries its `ereport(ERROR)`s.
    pub fn get_new_relfilenumber(reltablespace: Oid, relpersistence: i8) -> PgResult<RelFileNumber>
);

seam_core::seam!(
    /// `IsSystemRelation(rel)` (catalog.c).
    pub fn is_system_relation(rel: &types_rel::Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `IsSystemClass(relid, reltuple)` (catalog.c).
    pub fn is_system_class(relid: Oid, form: &types_cluster::PgClassForm) -> PgResult<bool>
);
