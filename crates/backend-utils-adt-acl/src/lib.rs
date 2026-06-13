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

pub mod acl_ops;
pub mod acldefault;
pub mod aclitem_io;
pub mod has_privilege;
pub mod role_membership;

/// Install this unit's seams (`backend-utils-adt-acl-seams`).
pub fn init_seams() {
    backend_utils_adt_acl_seams::member_can_set_role::set(role_membership::member_can_set_role);
    backend_utils_adt_acl_seams::check_can_set_role::set(role_membership::check_can_set_role);
    backend_utils_adt_acl_seams::has_privs_of_role::set(role_membership::has_privs_of_role);
    backend_utils_adt_acl_seams::get_rolespec_oid::set(role_membership::get_rolespec_oid);
    backend_utils_adt_acl_seams::initialize_acl::set(role_membership::initialize_acl);
}
