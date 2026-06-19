//! Installs the `commands/user.c` role-membership / role-resolution seams
//! (`backend_commands_user_seams`) onto acl.c's real implementations.
//!
//! The seam vocabulary uses the owned `types_parsenodes::RoleSpec` (the
//! command driver's parse-node model); acl.c's resolvers take the canonical
//! arena `types_nodes::parsenodes::RoleSpec`. The two meet here: a `RoleSpec`
//! converter (allocating `rolename` into the target context) bridges them, and
//! thin adapters wrap the bare-value / `String` results to the seam surface.

use mcx::{Mcx, MemoryContext, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

use crate::role_membership;

use types_nodes::parsenodes::RoleSpec as NRoleSpec;
use types_nodes::parsenodes::RoleSpecType as NRoleSpecType;
use types_parsenodes::RoleSpec as PRoleSpec;
use types_parsenodes::RoleSpecType as PRoleSpecType;

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
}
