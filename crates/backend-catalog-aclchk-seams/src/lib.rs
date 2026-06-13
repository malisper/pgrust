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

seam_core::seam!(
    /// `errorConflictingDefElem(defel, pstate)` (aclchk.c): always raises
    /// `ERRCODE_SYNTAX_ERROR` ("conflicting or redundant options") at the
    /// `DefElem`'s parse location. `defname` carries the conflicting option
    /// name for the message.
    pub fn error_conflicting_def_elem(defname: String) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveRoleFromObjectACL(roleid, classid, objid)` (aclchk.c): during
    /// DROP OWNED, revoke any privileges the role holds on the object — a
    /// REVOKE-equivalent that rewrites the object's ACL. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn remove_role_from_object_acl(roleid: Oid, classid: Oid, objid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveRoleFromInitPriv(roleid, classid, objid, objsubid)` (aclchk.c):
    /// during DROP OWNED, remove all mentions of the role from the object's
    /// pg_init_privs entry. Can `ereport(ERROR)`, carried on `Err`.
    pub fn remove_role_from_init_priv(
        roleid: Oid,
        classid: Oid,
        objid: Oid,
        objsubid: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ReplaceRoleInInitPriv(oldroleid, newroleid, classid, objid, objsubid)`
    /// (aclchk.c): during REASSIGN OWNED, substitute newrole for oldrole in
    /// the object's pg_init_privs entry. Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn replace_role_in_init_priv(
        oldroleid: Oid,
        newroleid: Oid,
        classid: Oid,
        objid: Oid,
        objsubid: i32,
    ) -> PgResult<()>
>>>>>>> main
);
