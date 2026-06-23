//! Seam declarations for the `backend-commands-alter` unit
//! (`commands/alter.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterObjectOwner_internal(classId, objectId, new_ownerId)` (alter.c):
    /// the generic ALTER OWNER path used by REASSIGN OWNED for object classes
    /// that have no bespoke owner-change routine (collations, conversions,
    /// operators, functions, languages, large objects, opfamilies, opclasses,
    /// extensions, extended statistics, tablespaces, databases, text-search
    /// configs/dicts). Can `ereport(ERROR)`, carried on `Err`.
    pub fn alter_object_owner_internal(
        class_id: Oid,
        object_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
