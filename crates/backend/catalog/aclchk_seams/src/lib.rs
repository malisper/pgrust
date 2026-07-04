seam_core::seam!(
    // object_aclcheck_ext (aclchk.c): (AclResult, is_missing).
    pub fn object_aclcheck_ext(
        classid: types_core::Oid,
        objectid: types_core::Oid,
        roleid: types_core::Oid,
        mode: u64,
    ) -> types_error::PgResult<(i32, bool)>
);

seam_core::seam!(
    // has_lo_priv_byid (acl.c): (result, is_missing); snapshot selection
    // (active vs NULL for ACL_UPDATE) happens on the aclchk side.
    pub fn has_lo_priv_byid(
        roleid: types_core::Oid,
        lobj_oid: types_core::Oid,
        priv_mode: u64,
    ) -> types_error::PgResult<(bool, bool)>
);

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

seam_core::seam!(
    // pg_class_aclcheck_ext (aclchk.c): (AclResult, is_missing).
    pub fn pg_class_aclcheck_ext(
        table_oid: types_core::Oid,
        roleid: types_core::Oid,
        mode: u64,
    ) -> types_error::PgResult<(i32, bool)>
);

seam_core::seam!(
    // pg_class_aclmask (aclchk.c); how_all selects ACLMASK_ALL vs ACLMASK_ANY.
    pub fn pg_class_aclmask(
        table_oid: types_core::Oid,
        roleid: types_core::Oid,
        mask: u64,
        how_all: bool,
    ) -> types_error::PgResult<u64>
);

seam_core::seam!(
    // pg_attribute_aclcheck (aclchk.c).
    pub fn pg_attribute_aclcheck(
        table_oid: types_core::Oid,
        attnum: i16,
        roleid: types_core::Oid,
        mode: u64,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    // pg_attribute_aclcheck_all (aclchk.c); how_all as above.
    pub fn pg_attribute_aclcheck_all(
        table_oid: types_core::Oid,
        roleid: types_core::Oid,
        mode: u64,
        how_all: bool,
    ) -> types_error::PgResult<i32>
);
