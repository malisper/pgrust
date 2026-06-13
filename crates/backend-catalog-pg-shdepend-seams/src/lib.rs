//! Seam declarations for the `backend-catalog-pg-shdepend` unit
//! (`catalog/pg_shdepend.c`): shared-dependency (owner) records.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `recordDependencyOnOwner(classId, objectId, owner)` (pg_shdepend.c):
    /// record `objectId`'s ownership dependency on the role `owner`. Catalog
    /// DML; can `ereport(ERROR)`, carried on `Err`.
    pub fn recordDependencyOnOwner(class_id: Oid, object_id: Oid, owner: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `changeDependencyOnOwner(classId, objectId, newOwnerId)`
    /// (pg_shdepend.c): update the owner shared-dependency record for an
    /// object whose owner changed. Catalog DML; can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn changeDependencyOnOwner(class_id: Oid, object_id: Oid, new_owner_id: Oid) -> PgResult<()>
);
