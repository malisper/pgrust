//! Seam declarations for the `backend-catalog-catalog` unit
//! (`catalog/catalog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `GetDatabasePath(dbOid, spcOid)` (catalog/catalog.c): build the
    /// filesystem path of the directory holding a database's relations,
    /// allocated in `mcx` (C: `palloc` in the current context). `Err` carries
    /// the allocation failure.
    pub fn get_database_path<'mcx>(
        mcx: Mcx<'mcx>,
        db_oid: Oid,
        spc_oid: Oid,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `IsPinnedObject(classId, objectId)` (catalog/catalog.c): is the object
    /// required for basic database functionality? Pure OID-range arithmetic —
    /// infallible.
    pub fn is_pinned_object(class_id: Oid, object_id: Oid) -> bool
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
    /// `IsSharedRelation(relationId)` (catalog/catalog.c): is the relation one
    /// of the shared catalogs (or their indexes/toast)? Pure OID compare;
    /// infallible.
    pub fn is_shared_relation(relation_id: Oid) -> bool
);

seam_core::seam!(
    /// `RelationInvalidatesSnapshotsOnly(relid)` (catalog/catalog.c): for the
    /// few catalogs whose tuples affect only saved snapshots (not catcache or
    /// relcache), this returns true so inval.c queues a snapshot inval instead.
    /// Pure OID compare; infallible.
    pub fn relation_invalidates_snapshots_only(relation_id: Oid) -> bool
);
