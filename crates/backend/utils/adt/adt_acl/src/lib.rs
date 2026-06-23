//! Scaffold of the `backend-utils-adt-acl` unit (`utils/adt/acl.c`, ~5700 LOC).
//!
//! Families re-derived from the C structure of `acl.c`:
//!
//! - [`aclitem_io`]: the `aclitem` SQL type's in/out plumbing and hashing
//!   (`aclparse`, `getid`, `putid`, `is_safe_acl_char`, `aclitemin`,
//!   `aclitemout`, `hash_aclitem*`, `aclitem_eq`/`_match`/`Comparator`).
//! - [`acldefault`]: built-in default ACLs (`acldefault`, `acldefault_sql`).
//! - [`acl_ops`]: the `Acl` array constructors and mask algebra
//!   (`allocacl`/`make_empty_acl`/`aclcopy`/`aclconcat`/`aclmerge`/
//!   `aclitemsort`/`aclequal`/`check_acl`/`aclupdate`/`aclnewowner`/
//!   `check_circularity`/`recursive_revoke`/`aclmask`/`aclmask_direct`/
//!   `aclmembers`, the SQL `aclinsert`/`aclremove`/`aclcontains`/`makeaclitem`/
//!   `aclexplode`, and `convert_aclright_to_string`/`convert_any_priv_string`).
//! - [`has_privilege`]: the `has_*_privilege` SQL families and `pg_has_role`,
//!   their per-object `convert_*_name`/`convert_*_priv_string` helpers,
//!   `column_privilege_check`, `has_param_priv_byname`, `has_lo_priv_byid`,
//!   and `pg_role_aclcheck`.
//! - [`role_membership`]: the role-membership cache and queries
//!   (`initialize_acl`, `RoleMembershipCacheCallback`, `roles_list_append`,
//!   `roles_is_member_of`, `has_privs_of_role`, `member_can_set_role`,
//!   `check_can_set_role`, `is_member_of_role`/`_nosuper`, `is_admin_of_role`,
//!   `select_best_admin`, `select_best_grantor`, `get_role_oid`/`_or_public`,
//!   `get_rolespec_oid`/`_tuple`/`_name`, `check_rolespec_name`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use types_acl::AclMode;

pub mod acl_ops;
pub mod acldefault;
pub mod aclitem_io;
pub mod fmgr_builtins;
pub mod has_privilege;
pub mod role_membership;
mod user_seam_wiring;

/// C: `typedef struct { const char *name; AclMode value; } priv_map` —
/// one entry of a privilege-name → privilege-bit table.
pub struct PrivMap {
    /// `name` — the SQL privilege keyword (e.g. `"SELECT"`).
    pub name: &'static str,
    /// `value` — the corresponding [`AclMode`] bit(s).
    pub value: AclMode,
}

/// Install this unit's seams (`backend-utils-adt-acl-seams`).
pub fn init_seams() {
    acl_seams::member_can_set_role::set(role_membership::member_can_set_role);
    acl_seams::check_can_set_role::set(role_membership::check_can_set_role);
    acl_seams::has_privs_of_role::set(role_membership::has_privs_of_role);
    // guc_funcs.c's ConfigOptionIsVisible reaches has_privs_of_role through its
    // own outward seam crate. C: `bool has_privs_of_role(Oid, Oid)`; the owner's
    // body carries the per-owner error channel (PgResult) for the catalog
    // membership scan, so unwrap to the bare bool the SHOW-visibility check uses.
    guc_funcs_seams::has_privs_of_role::set(|member, role| {
        role_membership::has_privs_of_role(member, role)
            .expect("has_privs_of_role catalog lookup failed")
    });
    // ProcessUtility's CHECKPOINT privilege check (utility.c:951) reaches
    // has_privs_of_role through the tcop utility-out-seams copy. C:
    // `bool has_privs_of_role(Oid, Oid)`; the owner body carries the per-owner
    // error channel (PgResult) for the catalog membership scan, so unwrap to the
    // bare bool the predicate uses (mirrors the guc_funcs install above).
    utility_out_seams::has_privs_of_role::set(|member, role| {
        role_membership::has_privs_of_role(member, role)
            .expect("has_privs_of_role catalog lookup failed")
    });
    // `pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK` (acl.c) —
    // the CLUSTER "may I maintain this relation?" predicate (cluster.c). The
    // underlying `pg_class_aclcheck` body lives in aclchk and is already
    // installed onto its own seam; this is the thin acl.c convenience wrapper,
    // declared in catalog-perm-seams. Install it here so the cluster caller
    // stops hitting an uninstalled seam.
    catalog_perm_seams::pg_class_aclcheck_maintain_ok::set(|relid, userid| {
        let res = aclchk_seams::pg_class_aclcheck::call(
            relid,
            userid,
            types_acl::ACL_MAINTAIN,
        )?;
        Ok(res == types_acl::ACLCHECK_OK)
    });
    acl_seams::get_rolespec_oid::set(role_membership::get_rolespec_oid);
    acl_seams::get_role_oid::set(role_membership::get_role_oid);
    acl_seams::initialize_acl::set(role_membership::initialize_acl);
    acl_seams::has_bypassrls_privilege::set(
        role_membership::has_bypassrls_privilege,
    );

    // Register acl.c's SQL-callable functions into the fmgr-core builtin table
    // (C: their `fmgr_builtins[]` rows): the aclitem type I/O + the
    // has_*_privilege / pg_has_role read families.
    fmgr_builtins::register_acl_builtins();

    // commands/user.c role-membership / role-resolution seams. `has_privs_of_role`
    // is intentionally NOT installed here: backend-commands-user installs it
    // (via the acl-seams delegation) to avoid a duplicate-install panic.
    user_seam_wiring::install();
}
