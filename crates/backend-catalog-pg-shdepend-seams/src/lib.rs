//! Seam declarations for the `backend-catalog-pg-shdepend` unit
//! (`catalog/pg_shdepend.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `recordDependencyOnOwner(classId, objectId, owner)` (pg_shdepend.c):
    /// record a `SHARED_DEPENDENCY_OWNER` shared dependency from the object on
    /// its owning role. `Err` carries the catalog-mutation `ereport(ERROR)`s.
    pub fn record_dependency_on_owner(
        class_id: Oid,
        object_id: Oid,
        owner: Oid,
    ) -> PgResult<()>
);
