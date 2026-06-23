//! Seam declarations for the `backend-catalog-pg-largeobject` unit
//! (`catalog/pg_largeobject.c`): large-object metadata existence checks.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::{Mcx, PgVec};
use ::types_acl::AclItem;
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `pg_largeobject_aclmask_snapshot`'s catalog read (aclchk.c): open
    /// `pg_largeobject_metadata` with `AccessShareLock`,
    /// `systable_beginscan(LargeObjectMetadataOidIndexId, snapshot, oid = loid)`,
    /// take the first row, then `GETSTRUCT(lomowner)` +
    /// `heap_getattr(Anum_pg_largeobject_metadata_lomacl)` (detoasted +
    /// decoded to `aclitem[]`). pg_largeobject_metadata has *no* syscache, so
    /// this lives in the merged pg_largeobject domain (snapshot systable scan),
    /// not in syscache like the other ACL projections. Returns the owner OID
    /// plus the decoded `lomacl` (`None` = SQL-null column -> aclchk builds the
    /// hardwired `acldefault(OBJECT_LARGEOBJECT, ownerId)`). `Ok(None)` on a
    /// missing object (the caller raises "large object %u does not exist").
    /// `snapshot == None` is the C `NULL` (instantaneous MVCC snapshot). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn largeobject_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        lobj_oid: Oid,
        snapshot: Option<std::rc::Rc<snapshot::SnapshotData>>,
    ) -> PgResult<Option<(Oid, Option<PgVec<'mcx, AclItem>>)>>
);

seam_core::seam!(
    /// `LargeObjectExistsWithSnapshot(loid, snapshot)` (pg_largeobject.c):
    /// whether `pg_largeobject_metadata` has a row for `loid`, scanned under
    /// the given snapshot (`None` = the C `NULL`, i.e. the latest catalog
    /// state). Opens/scans/closes `pg_largeobject_metadata`, which can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn large_object_exists_with_snapshot(
        loid: Oid,
        snapshot: Option<std::rc::Rc<snapshot::SnapshotData>>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `LargeObjectExists(loid)` (pg_largeobject.c): whether
    /// `pg_largeobject_metadata` has a row for `loid` under the latest catalog
    /// state (the `LargeObjectExistsWithSnapshot(loid, NULL)` wrapper). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn large_object_exists(loid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `LargeObjectDrop(loid)` (catalog/pg_largeobject.c): the per-class
    /// `OCLASS_LARGEOBJECT` drop handler dependency.c's `doDeletion` invokes for
    /// a large-object metadata object. Removes the `pg_largeobject_metadata`
    /// row and the object's data pages. Can `ereport(ERROR)`, carried on `Err`.
    pub fn LargeObjectDrop(loid: Oid) -> PgResult<()>
);
