seam_core::seam!(
    // pg_parameter_aclcheck(name, roleid, ACL_SET) == ACLCHECK_OK (aclchk.c).
    pub fn pg_parameter_aclcheck_set(
        name: &str,
        roleid: types_core::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // object_aclcheck(classid, objectid, roleid, mode) (aclchk.c); AclMode is
    // uint64 (parsenodes.h), 0 == ACLCHECK_OK (acl.h AclResult).
    pub fn object_aclcheck(
        classid: types_core::Oid,
        objectid: types_core::Oid,
        roleid: types_core::Oid,
        mode: u64,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    // aclcheck_error(aclresult, objtype, objectname) (aclchk.c); objtype is
    // the parsenodes.h ObjectType discriminant. Always ereport(ERROR)s, so a
    // call only ever returns Err.
    pub fn aclcheck_error(
        aclresult: i32,
        objtype: i32,
        objectname: &str,
    ) -> types_error::PgResult<()>
);
