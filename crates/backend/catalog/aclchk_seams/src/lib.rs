seam_core::seam!(
    // pg_parameter_aclcheck(name, roleid, ACL_SET) == ACLCHECK_OK (aclchk.c).
    pub fn pg_parameter_aclcheck_set(
        name: &str,
        roleid: types_core::Oid,
    ) -> types_error::PgResult<bool>
);
