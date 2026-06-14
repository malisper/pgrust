//! Seam declarations for the `backend-catalog-pg-parameter-acl` unit
//! (`catalog/pg_parameter_acl.c`): configuration-parameter ACL lookups.
//!
//! The owning unit (pg_parameter_acl.c, not yet ported) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

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
