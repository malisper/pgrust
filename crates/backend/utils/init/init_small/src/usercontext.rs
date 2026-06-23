//! `src/backend/utils/init/usercontext.c` — convenience functions for
//! running code as a different database user.
//!
//! The session state this file reads and mutates is owned elsewhere
//! (`miscinit.c`, `acl.c`, `guc.c`), and direct dependencies on those units
//! would cycle, so the calls go through the owners' seam crates.

use ::utils_error::ereport;
use ::mcx::Mcx;
use types_core::{Oid, UserContext, SECURITY_RESTRICTED_OPERATION, USER_CONTEXT_NO_NEST_LEVEL};
use types_error::{ErrorLocation, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};

/// `SwitchToUntrustedUser(Oid userid, UserContext *context)` — temporarily
/// switch to a new user ID.
///
/// If the current user doesn't have permission to SET ROLE to the new user,
/// an ERROR occurs.
///
/// If the new user doesn't have permission to SET ROLE to the current user,
/// SECURITY_RESTRICTED_OPERATION is imposed and a new GUC nest level is
/// created so that any settings changes can be rolled back.
///
/// `mcx` receives the role-name strings of the refusal message (in C,
/// `GetUserNameFromId` pstrdups them in the caller's current context).
pub fn SwitchToUntrustedUser(mcx: Mcx<'_>, userid: Oid, context: &mut UserContext) -> PgResult<()> {
    // Get the current user ID and security context.
    let (save_userid, save_sec_context) =
        miscinit_seams::get_user_id_and_sec_context::call();
    context.save_userid = save_userid;
    context.save_sec_context = save_sec_context;

    // Check that we have sufficient privileges to assume the target role.
    if !acl_seams::member_can_set_role::call(save_userid, userid)? {
        let save_user_name = miscinit_seams::get_user_name_from_id::call(
            mcx,
            save_userid,
            false,
        )?
        .expect("GetUserNameFromId(noerr = false) returns a name");
        let target_user_name = miscinit_seams::get_user_name_from_id::call(
            mcx, userid, false,
        )?
        .expect("GetUserNameFromId(noerr = false) returns a name");

        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "role \"{}\" cannot SET ROLE to \"{}\"",
                save_user_name.as_str(),
                target_user_name.as_str()
            ))
            .finish(ErrorLocation::new(
                "usercontext.c",
                45,
                "SwitchToUntrustedUser",
            ));
    }

    // Try to prevent the user to which we're switching from assuming the
    // privileges of the current user, unless they can SET ROLE to that user
    // anyway.
    if acl_seams::member_can_set_role::call(userid, save_userid)? {
        // Each user can SET ROLE to the other, so there's no point in
        // imposing any security restrictions. Just let the user do whatever
        // they want.
        miscinit_seams::set_user_id_and_sec_context::call(
            userid,
            context.save_sec_context,
        );
        context.save_nestlevel = USER_CONTEXT_NO_NEST_LEVEL;
    } else {
        // This user can SET ROLE to the target user, but not the other way
        // around, so protect ourselves against the target user by setting
        // SECURITY_RESTRICTED_OPERATION to prevent certain changes to the
        // session state. Also set up a new GUC nest level, so that we can
        // roll back any GUC changes that may be made by code running as the
        // target user, inasmuch as they could be malicious.
        let sec_context = context.save_sec_context | SECURITY_RESTRICTED_OPERATION;
        miscinit_seams::set_user_id_and_sec_context::call(userid, sec_context);
        context.save_nestlevel = guc_seams::new_guc_nest_level::call();
    }

    Ok(())
}

/// `RestoreUserContext(UserContext *context)` — switch back to the original
/// user ID.
///
/// If we created a new GUC nest level, also roll back any changes that were
/// made within it.
pub fn RestoreUserContext(context: &UserContext) -> PgResult<()> {
    if context.save_nestlevel != USER_CONTEXT_NO_NEST_LEVEL {
        guc_seams::at_eoxact_guc::call(false, context.save_nestlevel)?;
    }
    miscinit_seams::set_user_id_and_sec_context::call(
        context.save_userid,
        context.save_sec_context,
    );
    Ok(())
}
