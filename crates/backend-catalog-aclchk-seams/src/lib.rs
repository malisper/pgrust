//! Seam declarations for the `backend-catalog-aclchk` unit
//! (`catalog/aclchk.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_acl::{AclMode, AclResult};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;

seam_core::seam!(
    /// `object_aclcheck(classid, objectid, roleid, mode)` (aclchk.c): check
    /// privilege bits on a catalog object. Can `ereport(ERROR)` (e.g. cache
    /// lookup failure for a dropped object), carried on `Err`.
    pub fn object_aclcheck(
        classid: Oid,
        objectid: Oid,
        roleid: Oid,
        mode: AclMode,
    ) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `object_ownercheck(classid, objectid, roleid)` (aclchk.c): is `roleid`
    /// owner of (or member of the owning role of) the object? Can
    /// `ereport(ERROR)` on cache lookup failure, carried on `Err`.
    pub fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_class_aclcheck(table_oid, roleid, mode)` (aclchk.c): check
    /// table-level privilege bits. Returns `ACLCHECK_OK` / `ACLCHECK_NO_PRIV`
    /// (no ereport on a privilege miss); a syscache lookup failure can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `pg_attribute_aclcheck(table_oid, attnum, roleid, mode)` (aclchk.c):
    /// check column-level privilege bits. Returns `ACLCHECK_OK` /
    /// `ACLCHECK_NO_PRIV`; a syscache lookup failure can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn pg_attribute_aclcheck(
        table_oid: Oid,
        attnum: i16,
        roleid: Oid,
        mode: AclMode,
    ) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `aclcheck_error(aclerr, objtype, objectname)` (aclchk.c): raise the
    /// standard permission-denied / must-be-owner error. Always raises for a
    /// non-OK `aclerr` (the only way callers reach it), carried on `Err`.
    /// `objectname` mirrors the C `const char *` (nullable in principle).
    pub fn aclcheck_error(
        aclerr: AclResult,
        objtype: ObjectType,
        objectname: Option<String>,
    ) -> PgResult<()>
);
