//! Seam declarations for the `backend-catalog-aclchk` unit
//! (`catalog/aclchk.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_acl::{AclMaskHow, AclMode, AclResult};
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
    /// `object_aclcheck_ext(classid, objectid, roleid, mode, &is_missing)`
    /// (aclchk.c): like [`object_aclcheck`] but, when the object is missing,
    /// returns `ACLCHECK_NO_PRIV` with the missing flag set rather than
    /// raising. Result is `(aclresult, is_missing)`.
    pub fn object_aclcheck_ext(
        classid: Oid,
        objectid: Oid,
        roleid: Oid,
        mode: AclMode,
    ) -> PgResult<(AclResult, bool)>
);

seam_core::seam!(
    /// `pg_class_aclcheck(table_oid, roleid, mode)` (aclchk.c): check
    /// privilege bits on a relation. Can `ereport(ERROR)` on cache lookup
    /// failure, carried on `Err`.
    pub fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `pg_class_aclcheck_ext(table_oid, roleid, mode, &is_missing)`
    /// (aclchk.c): like [`pg_class_aclcheck`] but, when the relation is
    /// missing, returns `ACLCHECK_NO_PRIV` with the missing flag set rather
    /// than raising. Result is `(aclresult, is_missing)`.
    pub fn pg_class_aclcheck_ext(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
    ) -> PgResult<(AclResult, bool)>
);

seam_core::seam!(
    /// `pg_attribute_aclcheck_ext(table_oid, attnum, roleid, mode,
    /// &is_missing)` (aclchk.c): per-column privilege check; on a missing
    /// table/column returns `ACLCHECK_NO_PRIV` with the missing flag set.
    /// Result is `(aclresult, is_missing)`.
    pub fn pg_attribute_aclcheck_ext(
        table_oid: Oid,
        attnum: types_core::AttrNumber,
        roleid: Oid,
        mode: AclMode,
    ) -> PgResult<(AclResult, bool)>
);

seam_core::seam!(
    /// `pg_attribute_aclcheck_all(table_oid, roleid, mode, how)` (aclchk.c):
    /// check the privilege against every column of the relation, combining
    /// per `how`. Can `ereport(ERROR)`, carried on `Err`.
    pub fn pg_attribute_aclcheck_all(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        how: AclMaskHow,
    ) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `pg_attribute_aclcheck_all_ext(table_oid, roleid, mode, how,
    /// &is_missing)` (aclchk.c): like [`pg_attribute_aclcheck_all`] but, on a
    /// missing relation, returns `ACLCHECK_NO_PRIV` with the missing flag set.
    /// Result is `(aclresult, is_missing)`.
    pub fn pg_attribute_aclcheck_all_ext(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        how: AclMaskHow,
    ) -> PgResult<(AclResult, bool)>
);

seam_core::seam!(
    /// `pg_parameter_aclcheck(name, roleid, mode)` (aclchk.c): privilege
    /// check on a configuration parameter (by name). Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn pg_parameter_aclcheck(name: &str, roleid: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `pg_largeobject_aclcheck_snapshot(lobj_oid, roleid, mode, snapshot)`
    /// (aclchk.c): privilege check on a large object using the given snapshot
    /// (`None` = the C `NULL`, i.e. the latest catalog state). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pg_largeobject_aclcheck_snapshot(
        lobj_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
    ) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `pg_attribute_aclcheck(table_oid, attnum, roleid, mode)` (aclchk.c):
    /// check privilege bits on a single relation column. Can `ereport(ERROR)`
    /// on cache lookup failure, carried on `Err`.
    pub fn pg_attribute_aclcheck(
        table_oid: Oid,
        attnum: types_core::AttrNumber,
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
);
