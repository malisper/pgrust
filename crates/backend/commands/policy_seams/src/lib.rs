//! Seam declarations for the `backend-commands-policy` unit
//! (`commands/policy.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `RemoveRoleFromObjectPolicy(roleid, classid, objid)` (policy.c): during
    /// DROP OWNED, try to remove the role from any row-security policy on the
    /// object. Returns `false` (so the caller deletes the policy instead) when
    /// the role is the policy's only remaining role. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn remove_role_from_object_policy(roleid: Oid, classid: Oid, objid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_relation_policy_oid(relid, policy_name, missing_ok)` (policy.c): the
    /// OID of the named row-security policy on relation `relid`, or `InvalidOid`
    /// with `missing_ok = true`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_relation_policy_oid(
        relid: Oid,
        policy_name: &str,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RemovePolicyById(policy_id)` (commands/policy.c): the per-class
    /// `OCLASS_POLICY` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_policy` object. Removes the row-security policy's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemovePolicyById(policy_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `rename_policy(RenameStmt *stmt)` (policy.c) — ALTER POLICY ... RENAME TO.
    pub fn rename_policy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);
