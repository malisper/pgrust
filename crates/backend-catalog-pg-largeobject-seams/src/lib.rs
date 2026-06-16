//! Seam declarations for the `backend-catalog-pg-largeobject` unit
//! (`catalog/pg_largeobject.c`): large-object metadata existence checks.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `LargeObjectExistsWithSnapshot(loid, snapshot)` (pg_largeobject.c):
    /// whether `pg_largeobject_metadata` has a row for `loid`, scanned under
    /// the given snapshot (`None` = the C `NULL`, i.e. the latest catalog
    /// state). Opens/scans/closes `pg_largeobject_metadata`, which can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn large_object_exists_with_snapshot(
        loid: Oid,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
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
