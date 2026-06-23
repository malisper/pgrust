//! Seam declarations for the `backend-catalog-pg-parameter-acl` unit
//! (`catalog/pg_parameter_acl.c`): configuration-parameter ACL lookups.
//!
//! The owning unit (`backend-catalog-pg-parameter-acl`) installs these from its
//! `init_seams()`; until that runs a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `ParameterAclLookup(parameter, missing_ok)` (pg_parameter_acl.c): the
    /// OID of the `pg_parameter_acl` row for the named configuration parameter
    /// (canonicalized to lower case), or `InvalidOid` with `missing_ok = true`.
    /// With `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`
    /// (`Err`). Used by `get_object_address`'s `OBJECT_PARAMETER_ACL` arm.
    pub fn parameter_acl_lookup(parameter: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `ParameterAclCreate(parameter)` (pg_parameter_acl.c): add a new
    /// `pg_parameter_acl` row with a null ACL for the named configuration
    /// parameter (canonicalized to lower case via
    /// `convert_GUC_name_for_parameter_acl`), returning the new entry's OID.
    /// Validates the name with `check_GUC_name_for_parameter_acl` first.
    /// Consumed by aclchk.c's parameter-ACL GRANT/REVOKE path.
    pub fn parameter_acl_create(parameter: &str) -> PgResult<Oid>
);
