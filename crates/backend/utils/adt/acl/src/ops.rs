use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_INVALID_PARAMETER_VALUE,
};

use crate::membership::roles_is_member_of_list;
use crate::{
    acl_grant_option_for, acl_option_to_privs, aclitem_get_goptions, aclitem_get_privs,
    aclitem_get_rights, aclitem_set_privs_goptions, aclitem_set_rights, aclmask, AclItem,
    AclMaskHow, RoleRecurseType, ACL_ID_PUBLIC, ACL_NO_RIGHTS,
};

pub const ACL_MODECHG_ADD: i32 = 1;
pub const ACL_MODECHG_DEL: i32 = 2;
pub const ACL_MODECHG_EQL: i32 = 3;

pub const DROP_RESTRICT: i32 = 0;
pub const DROP_CASCADE: i32 = 1;

#[inline]
pub fn aclitem_match(a1: &AclItem, a2: &AclItem) -> bool {
    a1.ai_grantee == a2.ai_grantee && a1.ai_grantor == a2.ai_grantor
}

pub fn aclitem_comparator(a1: &AclItem, a2: &AclItem) -> core::cmp::Ordering {
    (a1.ai_grantee, a1.ai_grantor, a1.ai_privs).cmp(&(a2.ai_grantee, a2.ai_grantor, a2.ai_privs))
}

pub fn aclcopy<'mcx>(mcx: Mcx<'mcx>, orig: &[AclItem]) -> PgResult<PgVec<'mcx, AclItem>> {
    let mut v = mcx::vec_with_capacity_in(mcx, orig.len())?;
    v.extend_from_slice(orig);
    Ok(v)
}

pub fn aclconcat<'mcx>(
    mcx: Mcx<'mcx>,
    left: &[AclItem],
    right: &[AclItem],
) -> PgResult<PgVec<'mcx, AclItem>> {
    let mut v = mcx::vec_with_capacity_in(mcx, left.len() + right.len())?;
    v.extend_from_slice(left);
    v.extend_from_slice(right);
    Ok(v)
}

pub fn aclmerge<'mcx>(
    mcx: Mcx<'mcx>,
    left: &[AclItem],
    right: &[AclItem],
    owner_id: Oid,
) -> PgResult<PgVec<'mcx, AclItem>> {
    if left.is_empty() {
        return aclcopy(mcx, right);
    }
    if right.is_empty() {
        return aclcopy(mcx, left);
    }
    let mut result = aclcopy(mcx, left)?;
    for aip in right {
        result = aclupdate(mcx, &result, aip, ACL_MODECHG_ADD, owner_id, DROP_RESTRICT)?;
    }
    Ok(result)
}

pub fn aclitemsort(acl: &mut [AclItem]) {
    acl.sort_by(aclitem_comparator);
}

pub fn aclequal(left: &[AclItem], right: &[AclItem]) -> bool {
    left == right
}

pub fn aclupdate<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    mod_aip: &AclItem,
    modechg: i32,
    owner_id: Oid,
    behavior: i32,
) -> PgResult<PgVec<'mcx, AclItem>> {
    if modechg != ACL_MODECHG_DEL && aclitem_get_goptions(mod_aip) != ACL_NO_RIGHTS {
        check_circularity(mcx, old_acl, mod_aip, owner_id)?;
    }

    let mut new_acl = aclcopy(mcx, old_acl)?;
    let dst = match old_acl.iter().position(|item| aclitem_match(mod_aip, item)) {
        Some(d) => d,
        None => {
            new_acl.push(AclItem {
                ai_grantee: mod_aip.ai_grantee,
                ai_grantor: mod_aip.ai_grantor,
                ai_privs: 0,
            });
            new_acl.len() - 1
        }
    };

    let old_rights = aclitem_get_rights(&new_acl[dst]);
    let old_goptions = aclitem_get_goptions(&new_acl[dst]);

    let updated = match modechg {
        ACL_MODECHG_ADD => old_rights | aclitem_get_rights(mod_aip),
        ACL_MODECHG_DEL => old_rights & !aclitem_get_rights(mod_aip),
        ACL_MODECHG_EQL => aclitem_get_rights(mod_aip),
        _ => old_rights,
    };
    aclitem_set_rights(&mut new_acl[dst], updated);

    let new_goptions = aclitem_get_goptions(&new_acl[dst]);

    if updated == ACL_NO_RIGHTS {
        new_acl.remove(dst);
    }

    if (old_goptions & !new_goptions) != 0 {
        debug_assert!(mod_aip.ai_grantee != ACL_ID_PUBLIC);
        new_acl = recursive_revoke(
            mcx,
            new_acl,
            mod_aip.ai_grantee,
            old_goptions & !new_goptions,
            owner_id,
            behavior,
        )?;
    }

    Ok(new_acl)
}

pub fn aclnewowner<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    old_owner_id: Oid,
    new_owner_id: Oid,
) -> PgResult<PgVec<'mcx, AclItem>> {
    let mut new_acl = aclcopy(mcx, old_acl)?;
    let mut newpresent = false;
    for aip in new_acl.iter_mut() {
        if aip.ai_grantor == old_owner_id {
            aip.ai_grantor = new_owner_id;
        } else if aip.ai_grantor == new_owner_id {
            newpresent = true;
        }
        if aip.ai_grantee == old_owner_id {
            aip.ai_grantee = new_owner_id;
        } else if aip.ai_grantee == new_owner_id {
            newpresent = true;
        }
    }

    // Merge duplicates the substitution may have created (C's O(N^2) walk):
    // a merged-away entry is tombstoned with zero rights, then skipped.
    if newpresent {
        let num = new_acl.len();
        let mut dst = 0usize;
        for targ in 0..num {
            if aclitem_get_rights(&new_acl[targ]) == ACL_NO_RIGHTS {
                continue;
            }
            for src in (targ + 1)..num {
                if aclitem_get_rights(&new_acl[src]) == ACL_NO_RIGHTS {
                    continue;
                }
                if aclitem_match(&new_acl[targ], &new_acl[src]) {
                    let merged =
                        aclitem_get_rights(&new_acl[targ]) | aclitem_get_rights(&new_acl[src]);
                    aclitem_set_rights(&mut new_acl[targ], merged);
                    aclitem_set_rights(&mut new_acl[src], ACL_NO_RIGHTS);
                }
            }
            let item = new_acl[targ];
            new_acl[dst] = item;
            dst += 1;
        }
        new_acl.truncate(dst);
    }

    Ok(new_acl)
}

fn check_circularity(
    mcx: Mcx<'_>,
    old_acl: &[AclItem],
    mod_aip: &AclItem,
    owner_id: Oid,
) -> PgResult<()> {
    debug_assert!(mod_aip.ai_grantee != ACL_ID_PUBLIC);

    if mod_aip.ai_grantor == owner_id {
        return Ok(());
    }

    // Zap all grant options of the target grantee (and what depends on them),
    // then see whether the would-be grantor independently retains the option.
    let mut acl = aclcopy(mcx, old_acl)?;
    'restart: loop {
        for i in 0..acl.len() {
            if acl[i].ai_grantee == mod_aip.ai_grantee
                && aclitem_get_goptions(&acl[i]) != ACL_NO_RIGHTS
            {
                let item = acl[i];
                acl = aclupdate(mcx, &acl, &item, ACL_MODECHG_DEL, owner_id, DROP_CASCADE)?;
                continue 'restart;
            }
        }
        break;
    }

    let own_privs = acl_option_to_privs(aclmask(
        &acl,
        mod_aip.ai_grantor,
        owner_id,
        acl_grant_option_for(aclitem_get_goptions(mod_aip)),
        AclMaskHow::AclmaskAll,
    )?);

    if (aclitem_get_goptions(mod_aip) & !own_privs) != 0 {
        return Err(Box::new(
            PgError::error("grant options cannot be granted back to your own grantor")
                .with_sqlstate(ERRCODE_INVALID_GRANT_OPERATION),
        ));
    }
    Ok(())
}

fn recursive_revoke<'mcx>(
    mcx: Mcx<'mcx>,
    mut acl: PgVec<'mcx, AclItem>,
    grantee: Oid,
    mut revoke_privs: u64,
    owner_id: Oid,
    behavior: i32,
) -> PgResult<PgVec<'mcx, AclItem>> {
    if grantee == owner_id {
        return Ok(acl);
    }

    let still_has = aclmask(
        &acl,
        grantee,
        owner_id,
        acl_grant_option_for(revoke_privs),
        AclMaskHow::AclmaskAll,
    )?;
    revoke_privs &= !acl_option_to_privs(still_has);
    if revoke_privs == ACL_NO_RIGHTS {
        return Ok(acl);
    }

    'restart: loop {
        for i in 0..acl.len() {
            if acl[i].ai_grantor == grantee && (aclitem_get_privs(&acl[i]) & revoke_privs) != 0 {
                if behavior == DROP_RESTRICT {
                    return Err(Box::new(
                        PgError::error("dependent privileges exist")
                            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                            .with_hint("Use CASCADE to revoke them too."),
                    ));
                }
                let mut mod_acl = AclItem {
                    ai_grantee: acl[i].ai_grantee,
                    ai_grantor: grantee,
                    ai_privs: 0,
                };
                aclitem_set_privs_goptions(&mut mod_acl, revoke_privs, revoke_privs);
                acl = aclupdate(mcx, &acl, &mod_acl, ACL_MODECHG_DEL, owner_id, behavior)?;
                continue 'restart;
            }
        }
        break;
    }
    Ok(acl)
}

// aclmask_direct (acl.c): no membership recursion, no owner special case
// beyond exact match.
pub fn aclmask_direct(acl: &[AclItem], roleid: Oid, owner_id: Oid, mask: u64, how: AclMaskHow) -> u64 {
    if mask == 0 {
        return 0;
    }
    let done = |result: u64| match how {
        AclMaskHow::AclmaskAll => result == mask,
        AclMaskHow::AclmaskAny => result != 0,
    };
    let mut result = 0u64;
    if mask & crate::ACLITEM_ALL_GOPTION_BITS != 0 && roleid == owner_id {
        result = mask & crate::ACLITEM_ALL_GOPTION_BITS;
        if done(result) {
            return result;
        }
    }
    for item in acl {
        if item.ai_grantee == roleid {
            result |= item.ai_privs & mask;
            if done(result) {
                return result;
            }
        }
    }
    result
}

pub fn aclmembers<'mcx>(mcx: Mcx<'mcx>, acl: &[AclItem]) -> PgResult<PgVec<'mcx, Oid>> {
    let mut list: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, acl.len() * 2)?;
    for ai in acl {
        if ai.ai_grantee != ACL_ID_PUBLIC {
            list.push(ai.ai_grantee);
        }
        if ai.ai_grantor != ACL_ID_PUBLIC {
            list.push(ai.ai_grantor);
        }
    }
    list.sort_unstable();
    list.dedup();
    Ok(list)
}

pub fn aclcontains(acl: &[AclItem], aip: &AclItem) -> bool {
    acl.iter().any(|it| {
        aip.ai_grantee == it.ai_grantee
            && aip.ai_grantor == it.ai_grantor
            && (aclitem_get_rights(aip) & aclitem_get_rights(it)) == aclitem_get_rights(aip)
    })
}

pub fn select_best_grantor(
    role_id: Oid,
    privileges: u64,
    acl: &[AclItem],
    owner_id: Oid,
) -> PgResult<(Oid, u64)> {
    let needed_goptions = acl_grant_option_for(privileges);

    // The owner is treated as having all grant options; a superuser is
    // implicitly a member of every role and so acts as the owner.
    if role_id == owner_id || superuser::superuser_arg(role_id)? {
        return Ok((owner_id, needed_goptions));
    }

    let roles_list = roles_is_member_of_list(role_id, RoleRecurseType::Privs)?;

    let mut grantor_id = role_id;
    let mut grant_options = ACL_NO_RIGHTS;
    let mut nrights = 0u32;

    for otherrole in roles_list {
        let otherprivs =
            aclmask_direct(acl, otherrole, owner_id, needed_goptions, AclMaskHow::AclmaskAll);
        if otherprivs == needed_goptions {
            return Ok((otherrole, otherprivs));
        }
        if otherprivs != ACL_NO_RIGHTS {
            let nnewrights = otherprivs.count_ones();
            if nnewrights > nrights {
                grantor_id = otherrole;
                grant_options = otherprivs;
                nrights = nnewrights;
            }
        }
    }
    Ok((grantor_id, grant_options))
}

pub struct PrivMapEntry {
    pub name: &'static str,
    pub value: u64,
}

pub fn convert_any_priv_string(priv_type: &str, privileges: &[PrivMapEntry]) -> PgResult<u64> {
    let mut result = 0u64;
    for chunk in priv_type.split(',') {
        let chunk = chunk.trim_matches(|c: char| c.is_ascii_whitespace());
        match privileges.iter().find(|p| p.name.eq_ignore_ascii_case(chunk)) {
            Some(p) => result |= p.value,
            None => {
                return Err(Box::new(
                    PgError::error(format!("unrecognized privilege type: \"{chunk}\""))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                ))
            }
        }
    }
    Ok(result)
}
