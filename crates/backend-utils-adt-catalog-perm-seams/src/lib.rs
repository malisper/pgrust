//! Seam declarations for the `backend-utils-adt-catalog-perm` unit
//! (`utils/adt/acl.c`). The owning unit installs these from its `init_seams()`
//! when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK` (acl.c).
    pub fn pg_class_aclcheck_maintain_ok(relid: Oid, userid: Oid) -> PgResult<bool>
);
