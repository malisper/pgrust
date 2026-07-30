#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{Oid, UserContext, SECURITY_RESTRICTED_OPERATION, USER_CONTEXT_NO_NEST_LEVEL};
use types_error::{PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE};

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_set_role(mcx: Mcx<'_>, save_userid: Oid, userid: Oid) -> Box<PgError> {
    let name = |roleid| -> Result<_, Box<PgError>> {
        Ok(miscinit_seams::get_user_name_from_id::call(mcx, roleid, false)?
            .expect("GetUserNameFromId(noerr = false) returns a name"))
    };
    let (save_name, target_name) = match (name(save_userid), name(userid)) {
        (Ok(s), Ok(t)) => (s, t),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        PgError::error(format!(
            "role \"{}\" cannot SET ROLE to \"{}\"",
            save_name.as_str(),
            target_name.as_str()
        ))
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

pub fn SwitchToUntrustedUser(mcx: Mcx<'_>, userid: Oid, context: &mut UserContext) -> PgResult<()> {
    let (save_userid, save_sec_context) = miscinit_seams::get_user_id_and_sec_context::call();
    context.save_userid = save_userid;
    context.save_sec_context = save_sec_context;

    if !acl_seams::member_can_set_role::call(save_userid, userid)? {
        return Err(cannot_set_role(mcx, save_userid, userid));
    }

    if acl_seams::member_can_set_role::call(userid, save_userid)? {
        // Each user can SET ROLE to the other: no security restrictions.
        miscinit_seams::set_user_id_and_sec_context::call(userid, context.save_sec_context);
        context.save_nestlevel = USER_CONTEXT_NO_NEST_LEVEL;
    } else {
        // One-way trust: restrict session-state changes by the target user and
        // open a GUC nest level so its settings changes can be rolled back.
        let sec_context = context.save_sec_context | SECURITY_RESTRICTED_OPERATION;
        miscinit_seams::set_user_id_and_sec_context::call(userid, sec_context);
        context.save_nestlevel = guc_seams::new_guc_nest_level::call();
    }

    Ok(())
}

pub fn RestoreUserContext(context: &UserContext) -> PgResult<()> {
    if context.save_nestlevel != USER_CONTEXT_NO_NEST_LEVEL {
        guc_seams::at_eoxact_guc::call(false, context.save_nestlevel)?;
    }
    miscinit_seams::set_user_id_and_sec_context::call(context.save_userid, context.save_sec_context);
    Ok(())
}
