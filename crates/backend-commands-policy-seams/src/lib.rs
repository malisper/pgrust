//! Seam declarations for the `backend-commands-policy` unit
//! (`commands/policy.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `RemoveRoleFromObjectPolicy(roleid, classid, objid)` (policy.c): during
    /// DROP OWNED, try to remove the role from any row-security policy on the
    /// object. Returns `false` (so the caller deletes the policy instead) when
    /// the role is the policy's only remaining role. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn remove_role_from_object_policy(roleid: Oid, classid: Oid, objid: Oid) -> PgResult<bool>
);
