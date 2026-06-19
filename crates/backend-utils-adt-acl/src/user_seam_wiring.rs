//! Installs the `commands/user.c` role-membership / role-resolution seams
//! (`backend_commands_user_seams`) onto acl.c's real implementations.
//!
//! The seam vocabulary uses the owned `types_parsenodes::RoleSpec` (the
//! command driver's parse-node model); acl.c's resolvers take the canonical
//! arena `types_nodes::parsenodes::RoleSpec`. The two meet here: a `RoleSpec`
//! converter (allocating `rolename` into the target context) bridges them, and
//! thin adapters wrap the bare-value / `String` results to the seam surface.

use mcx::{Mcx, MemoryContext, PgString};
use types_authid::{AuthIdForm, AuthMemForm};
use types_cache::{AuthIdRow, AuthMembersFullRow};
use types_core::primitive::Oid;
use types_error::PgResult;

use crate::role_membership;

use types_nodes::parsenodes::RoleSpec as NRoleSpec;
use types_nodes::parsenodes::RoleSpecType as NRoleSpecType;
use types_parsenodes::RoleSpec as PRoleSpec;
use types_parsenodes::RoleSpecType as PRoleSpecType;

/// `AuthIdRow` (the syscache value projection) → `AuthIdForm` (the user.c
/// vocabulary). Re-homes the owned strings; both carry the same columns.
fn authid_row_to_form(row: &AuthIdRow<'_>) -> AuthIdForm {
    AuthIdForm {
        oid: row.oid,
        rolname: row.rolname.as_str().to_string(),
        rolsuper: row.rolsuper,
        rolinherit: row.rolinherit,
        rolpassword: row.rolpassword.as_ref().map(|s| s.as_str().to_string()),
        rolvaliduntil: row.rolvaliduntil,
    }
}

fn authmem_row_to_form(row: &AuthMembersFullRow) -> AuthMemForm {
    AuthMemForm {
        oid: row.oid,
        roleid: row.roleid,
        member: row.member,
        grantor: row.grantor,
        admin_option: row.admin_option,
        inherit_option: row.inherit_option,
        set_option: row.set_option,
    }
}

/// `get_rolespec_tuple(role)` — resolve the RoleSpec and project the
/// `Form_pg_authid` view (raises if not found, like the C).
fn get_rolespec_tuple_seam<'mcx>(mcx: Mcx<'mcx>, role: PRoleSpec) -> PgResult<AuthIdForm> {
    let arena = role_spec_to_arena(mcx, &role)?;
    let row = role_membership::get_rolespec_tuple(mcx, &arena)?;
    Ok(authid_row_to_form(&row))
}

fn authid_by_name_seam<'mcx>(mcx: Mcx<'mcx>, rolename: String) -> PgResult<Option<AuthIdForm>> {
    Ok(
        backend_utils_cache_syscache_seams::lookup_authid_by_name::call(mcx, &rolename)?
            .as_ref()
            .map(authid_row_to_form),
    )
}

fn authid_by_oid_seam<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<Option<AuthIdForm>> {
    Ok(
        backend_utils_cache_syscache_seams::lookup_authid_by_oid::call(mcx, roleid)?
            .as_ref()
            .map(authid_row_to_form),
    )
}

fn authmem_by_keys_seam<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    member: Oid,
    grantor: Oid,
) -> PgResult<Option<AuthMemForm>> {
    Ok(
        backend_utils_cache_syscache_seams::lookup_authmem_by_keys::call(
            mcx, roleid, member, grantor,
        )?
        .as_ref()
        .map(authmem_row_to_form),
    )
}

fn authmem_list_by_role_seam<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<Vec<AuthMemForm>> {
    let rows = backend_utils_cache_syscache_seams::lookup_authmem_list_by_role::call(mcx, roleid)?;
    Ok(rows.iter().map(authmem_row_to_form).collect())
}

/// Convert an owned `types_parsenodes::RoleSpec` into the canonical arena
/// `types_nodes::parsenodes::RoleSpec`, allocating `rolename` in `mcx`.
fn role_spec_to_arena<'mcx>(mcx: Mcx<'mcx>, role: &PRoleSpec) -> PgResult<NRoleSpec<'mcx>> {
    let roletype = match role.roletype {
        PRoleSpecType::ROLESPEC_CSTRING => NRoleSpecType::Cstring,
        PRoleSpecType::ROLESPEC_CURRENT_ROLE => NRoleSpecType::CurrentRole,
        PRoleSpecType::ROLESPEC_CURRENT_USER => NRoleSpecType::CurrentUser,
        PRoleSpecType::ROLESPEC_SESSION_USER => NRoleSpecType::SessionUser,
        PRoleSpecType::ROLESPEC_PUBLIC => NRoleSpecType::Public,
    };
    let rolename = match role.rolename.as_deref() {
        Some(s) => Some(PgString::from_str_in(s, mcx)?),
        None => None,
    };
    Ok(NRoleSpec { roletype, rolename })
}

fn get_rolespec_oid_seam(role: PRoleSpec, missing_ok: bool) -> PgResult<Oid> {
    let cx = MemoryContext::new("acl get_rolespec_oid seam");
    let mcx = cx.mcx();
    let arena = role_spec_to_arena(mcx, &role)?;
    role_membership::get_rolespec_oid(&arena, missing_ok)
}

fn get_rolespec_name_seam<'mcx>(mcx: Mcx<'mcx>, role: PRoleSpec) -> PgResult<PgString<'mcx>> {
    let arena = role_spec_to_arena(mcx, &role)?;
    let name = role_membership::get_rolespec_name(mcx, &arena)?;
    PgString::from_str_in(&name, mcx)
}

fn check_rolespec_name_seam(role: PRoleSpec, detail_msg: String) -> PgResult<()> {
    let cx = MemoryContext::new("acl check_rolespec_name seam");
    let mcx = cx.mcx();
    let arena = role_spec_to_arena(mcx, &role)?;
    role_membership::check_rolespec_name(Some(&arena), Some(&detail_msg))
}

pub fn install() {
    use backend_commands_user_seams as user;

    user::is_admin_of_role::set(role_membership::is_admin_of_role);
    user::is_member_of_role_nosuper::set(role_membership::is_member_of_role_nosuper);
    user::select_best_admin::set(role_membership::select_best_admin);
    user::has_bypassrls_privilege::set(role_membership::has_bypassrls_privilege);
    user::get_role_oid::set(|rolename, missing_ok| {
        role_membership::get_role_oid(&rolename, missing_ok)
    });
    user::get_rolespec_oid::set(get_rolespec_oid_seam);
    user::get_rolespec_name::set(get_rolespec_name_seam);
    user::check_rolespec_name::set(check_rolespec_name_seam);

    // Value-projected syscache reads (no opaque tuple handle): the
    // `SearchSysCache`/`GETSTRUCT`/`ReleaseSysCache` lifecycle collapses into a
    // single projection returning the `Form_pg_authid`/`Form_pg_auth_members`
    // value.
    user::get_rolespec_tuple::set(get_rolespec_tuple_seam);
    user::authid_by_name::set(authid_by_name_seam);
    user::authid_by_oid::set(authid_by_oid_seam);
    user::authmem_by_keys::set(authmem_by_keys_seam);
    user::authmem_list_by_role::set(authmem_list_by_role_seam);
}
