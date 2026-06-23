//! `Acl` array construction and the privilege-mask algebra (`utils/adt/acl.c`).
//!
//! Covers the C-API `Acl` operations (`allocacl`, `make_empty_acl`, `aclcopy`,
//! `aclconcat`, `aclmerge`, `aclitemsort`, `aclequal`, `check_acl`,
//! `aclupdate`, `aclnewowner`, `check_circularity`, `recursive_revoke`,
//! `aclmask`, `aclmask_direct`, `aclmembers`), the SQL operators
//! (`aclinsert`, `aclremove`, `aclcontains`, `makeaclitem`, `aclexplode`),
//! and the priv-string conversion helpers shared with `has_privilege`
//! (`convert_aclright_to_string`, `convert_any_priv_string`).
//!
//! In C an `Acl` is a varlena `ArrayType` of fixed-size `AclItem`s: a
//! one-dimensional, no-nulls array (`ARR_ELEMTYPE == ACLITEMOID`). At this
//! family's representation the *container* header is gone — the value is just
//! the item slice (`ACL_DAT(acl)`), and `ACL_NUM(acl)` is its length. The
//! `check_acl` validation (`ndim == 1`, no nulls, `elemtype == ACLITEMOID`) is
//! a property of that absent header, so it has nothing to validate here.

extern crate alloc;

use mcx::{Mcx, PgVec};
use types_acl::{
    AclItem, AclMaskHow, AclMode, ACLITEM_ALL_GOPTION_BITS, ACL_ID_PUBLIC, ACL_INSERT,
    ACL_NO_RIGHTS, ACL_SELECT, ACL_UPDATE, ACL_DELETE, ACL_TRUNCATE, ACL_REFERENCES, ACL_TRIGGER,
    ACL_EXECUTE, ACL_USAGE, ACL_CREATE, ACL_CREATE_TEMP, ACL_CONNECT, ACL_SET, ACL_ALTER_SYSTEM,
    ACL_MAINTAIN,
};
use types_acl::AclMaskHow::{AclmaskAll as ACLMASK_ALL, AclmaskAny as ACLMASK_ANY};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_GRANT_OPERATION, ERRCODE_INVALID_PARAMETER_VALUE,
};

use acl_seams::has_privs_of_role;

/// `ACL_MODECHG_ADD` (`utils/acl.h`).
pub const ACL_MODECHG_ADD: i32 = 1;
/// `ACL_MODECHG_DEL` (`utils/acl.h`).
pub const ACL_MODECHG_DEL: i32 = 2;
/// `ACL_MODECHG_EQL` (`utils/acl.h`).
pub const ACL_MODECHG_EQL: i32 = 3;

/// `DROP_RESTRICT` (`nodes/parsenodes.h`) — the first `DropBehavior` variant.
pub const DROP_RESTRICT: i32 = 0;
/// `DROP_CASCADE` (`nodes/parsenodes.h`).
pub const DROP_CASCADE: i32 = 1;

// --- AclItem bit accessors (utils/acl.h macros) ---

/// `ACLITEM_GET_PRIVS(item)` — the lower 32 privilege bits.
#[inline]
fn aclitem_get_privs(item: AclItem) -> AclMode {
    item.ai_privs & 0xFFFF_FFFF
}

/// `ACLITEM_GET_GOPTIONS(item)` — the upper 32 grant-option bits, shifted down.
#[inline]
fn aclitem_get_goptions(item: AclItem) -> AclMode {
    (item.ai_privs >> 32) & 0xFFFF_FFFF
}

/// `ACLITEM_GET_RIGHTS(item)` — the full 64-bit privs+goptions word.
#[inline]
fn aclitem_get_rights(item: AclItem) -> AclMode {
    item.ai_privs
}

/// `ACLITEM_SET_RIGHTS(item, rights)`.
#[inline]
fn aclitem_set_rights(item: &mut AclItem, rights: AclMode) {
    item.ai_privs = rights;
}

/// `ACLITEM_SET_PRIVS_GOPTIONS(item, privs, goptions)`.
#[inline]
fn aclitem_set_privs_goptions(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    item.ai_privs = (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32);
}

/// `ACL_GRANT_OPTION_FOR(privs)`.
#[inline]
fn acl_grant_option_for(privs: AclMode) -> AclMode {
    (privs & 0xFFFF_FFFF) << 32
}

/// `ACL_OPTION_TO_PRIVS(privs)`.
#[inline]
fn acl_option_to_privs(privs: AclMode) -> AclMode {
    (privs >> 32) & 0xFFFF_FFFF
}

/// `aclitem_match` (acl.c) — two AclItems match iff same grantee and grantor.
#[inline]
fn aclitem_match(a1: &AclItem, a2: &AclItem) -> bool {
    a1.ai_grantee == a2.ai_grantee && a1.ai_grantor == a2.ai_grantor
}

/// `aclitemComparator` (acl.c) — the qsort ordering for AclItems.
fn aclitem_comparator(a1: &AclItem, a2: &AclItem) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if a1.ai_grantee > a2.ai_grantee {
        return Ordering::Greater;
    }
    if a1.ai_grantee < a2.ai_grantee {
        return Ordering::Less;
    }
    if a1.ai_grantor > a2.ai_grantor {
        return Ordering::Greater;
    }
    if a1.ai_grantor < a2.ai_grantor {
        return Ordering::Less;
    }
    if a1.ai_privs > a2.ai_privs {
        return Ordering::Greater;
    }
    if a1.ai_privs < a2.ai_privs {
        return Ordering::Less;
    }
    Ordering::Equal
}

/// `(how == ACLMASK_ALL) ? (result == mask) : (result != 0)`.
#[inline]
fn mask_done(how: AclMaskHow, result: AclMode, mask: AclMode) -> bool {
    match how {
        ACLMASK_ALL => result == mask,
        ACLMASK_ANY => result != 0,
    }
}

/// `allocacl` (acl.c) — allocate a zeroed `Acl` array of `n` items in `mcx`.
///
/// C's `elog(ERROR, "invalid size: %d", n)` for `n < 0` is preserved against
/// the signed `i32` count; otherwise the only failure is an over-large or OOM
/// allocation, surfaced through `mcx`'s OOM `PgError`.
pub fn allocacl<'mcx>(mcx: Mcx<'mcx>, n: i32) -> PgResult<&'mcx mut [AclItem]> {
    if n < 0 {
        return Err(PgError::error(alloc::format!("invalid size: {n}")));
    }
    let n = n as usize;
    let mut items: PgVec<'mcx, AclItem> = mcx::vec_with_capacity_in(mcx, n)?;
    items.resize(
        n,
        AclItem { ai_grantee: 0, ai_grantor: 0, ai_privs: 0 },
    );
    Ok(items.leak())
}

/// `make_empty_acl` (acl.c) — allocate an empty `Acl` array in `mcx`.
pub fn make_empty_acl<'mcx>(mcx: Mcx<'mcx>) -> PgResult<&'mcx mut [AclItem]> {
    allocacl(mcx, 0)
}

/// `aclcopy` (acl.c) — duplicate an `Acl` array into `mcx`.
pub fn aclcopy<'mcx>(mcx: Mcx<'mcx>, orig: &[AclItem]) -> PgResult<&'mcx mut [AclItem]> {
    let result = allocacl(mcx, orig.len() as i32)?;
    result.copy_from_slice(orig);
    Ok(result)
}

/// `aclconcat` (acl.c) — concatenate two `Acl` arrays into `mcx`.
///
/// This may produce redundant entries; be careful what the result is used for.
pub fn aclconcat<'mcx>(
    mcx: Mcx<'mcx>,
    left: &[AclItem],
    right: &[AclItem],
) -> PgResult<&'mcx mut [AclItem]> {
    let result = allocacl(mcx, (left.len() + right.len()) as i32)?;
    result[..left.len()].copy_from_slice(left);
    result[left.len()..].copy_from_slice(right);
    Ok(result)
}

/// `aclmerge` (acl.c) — merge two `Acl` arrays, OR-ing rights per grantee.
///
/// Produces a properly merged ACL with no redundant entries. C returns NULL on
/// the all-empty case; here that is the empty slice.
pub fn aclmerge<'mcx>(
    mcx: Mcx<'mcx>,
    left: &[AclItem],
    right: &[AclItem],
    owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    // Check for cases where one or both are empty.
    if left.is_empty() {
        if right.is_empty() {
            return allocacl(mcx, 0);
        } else {
            return aclcopy(mcx, right);
        }
    } else if right.is_empty() {
        return aclcopy(mcx, left);
    }

    // Merge them the hard way, one item at a time.
    let mut result_acl: &mut [AclItem] = aclcopy(mcx, left)?;

    for aip in right.iter() {
        result_acl = aclupdate(mcx, result_acl, aip, ACL_MODECHG_ADD, owner_id, DROP_RESTRICT)?;
    }

    Ok(result_acl)
}

/// `aclitemsort` (acl.c) — sort an `Acl` array in place into canonical order.
pub fn aclitemsort(acl: &mut [AclItem]) {
    if acl.len() > 1 {
        acl.sort_by(aclitem_comparator);
    }
}

/// `aclequal` (acl.c) — are two `Acl` arrays equal as sets?
///
/// Order-sensitive (sort both inputs first with `aclitemsort` to compare as
/// sets, exactly as C documents).
pub fn aclequal(left: &[AclItem], right: &[AclItem]) -> bool {
    // Check for cases where one or both are empty.
    if left.is_empty() {
        return right.is_empty();
    } else if right.is_empty() {
        return false;
    }

    if left.len() != right.len() {
        return false;
    }

    left == right
}

/// `check_acl` (acl.c) — validate an `Acl` array's varlena shape; errors on bad.
///
/// C checks the `ArrayType` header (`elemtype == ACLITEMOID`, `ndim == 1`, no
/// nulls). At this representation the header is gone — the value is just the
/// item slice — so there is nothing left to validate, and a slice that exists
/// is by construction a well-formed one-dimensional no-nulls aclitem array.
pub fn check_acl(_acl: &[AclItem]) -> PgResult<()> {
    Ok(())
}

/// `aclupdate` (acl.c) — apply `mod_aip` (with `modechg`) to `old_acl`,
/// producing a new array in `mcx`. `modechg` is acl.c's `int` mode-change code.
pub fn aclupdate<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    mod_aip: &AclItem,
    modechg: i32,
    owner_id: Oid,
    behavior: i32,
) -> PgResult<&'mcx mut [AclItem]> {
    // Caller probably already checked old_acl, but be safe.
    check_acl(old_acl)?;

    // If granting grant options, check for circularity.
    if modechg != ACL_MODECHG_DEL && aclitem_get_goptions(*mod_aip) != ACL_NO_RIGHTS {
        check_circularity(mcx, old_acl, mod_aip, owner_id)?;
    }

    let num = old_acl.len();

    // Search the ACL for an existing entry for this grantee and grantor. If one
    // exists, modify the entry in the same position (of a copy); otherwise
    // insert the new entry at the end.
    let found = old_acl.iter().position(|item| aclitem_match(mod_aip, item));

    let new_acl: &mut [AclItem];
    let dst: usize;
    let mut num = num;

    match found {
        Some(d) => {
            // found a match, so modify existing item
            new_acl = allocacl(mcx, num as i32)?;
            new_acl.copy_from_slice(old_acl);
            dst = d;
        }
        None => {
            // need to append a new item
            new_acl = allocacl(mcx, (num + 1) as i32)?;
            new_acl[..num].copy_from_slice(old_acl);
            dst = num;
            // initialize the new entry with no permissions
            new_acl[dst].ai_grantee = mod_aip.ai_grantee;
            new_acl[dst].ai_grantor = mod_aip.ai_grantor;
            aclitem_set_privs_goptions(&mut new_acl[dst], ACL_NO_RIGHTS, ACL_NO_RIGHTS);
            num += 1; // set num to the size of new_acl
        }
    }

    let old_rights = aclitem_get_rights(new_acl[dst]);
    let old_goptions = aclitem_get_goptions(new_acl[dst]);

    // apply the specified permissions change
    match modechg {
        ACL_MODECHG_ADD => {
            aclitem_set_rights(&mut new_acl[dst], old_rights | aclitem_get_rights(*mod_aip));
        }
        ACL_MODECHG_DEL => {
            aclitem_set_rights(&mut new_acl[dst], old_rights & !aclitem_get_rights(*mod_aip));
        }
        ACL_MODECHG_EQL => {
            aclitem_set_rights(&mut new_acl[dst], aclitem_get_rights(*mod_aip));
        }
        _ => {}
    }

    let new_rights = aclitem_get_rights(new_acl[dst]);
    let new_goptions = aclitem_get_goptions(new_acl[dst]);

    // If the adjusted entry has no permissions, delete it from the list.
    let mut new_acl: &mut [AclItem] = new_acl;
    if new_rights == ACL_NO_RIGHTS {
        // memmove(new_aip + dst, new_aip + dst + 1, ...) then shrink by one.
        new_acl.copy_within(dst + 1..num, dst);
        let len = new_acl.len() - 1;
        new_acl = &mut new_acl[..len];
    }

    // Remove abandoned privileges (cascading revoke). Currently we can only
    // handle this when the grantee is not PUBLIC.
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

/// `aclnewowner` (acl.c) — rewrite an `Acl` array for an ownership change.
///
/// Substitutes `new_owner_id` for `old_owner_id` wherever it appears as either
/// grantor or grantee, then merges any resulting duplicates.
pub fn aclnewowner<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    old_owner_id: Oid,
    new_owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    check_acl(old_acl)?;

    // Make a copy, substituting new owner ID for old wherever it appears as
    // either grantor or grantee. Also note if the new owner ID is already
    // present.
    let num = old_acl.len();
    let new_acl = allocacl(mcx, num as i32)?;
    new_acl.copy_from_slice(old_acl);

    let mut newpresent = false;
    for dst_aip in new_acl.iter_mut() {
        if dst_aip.ai_grantor == old_owner_id {
            dst_aip.ai_grantor = new_owner_id;
        } else if dst_aip.ai_grantor == new_owner_id {
            newpresent = true;
        }
        if dst_aip.ai_grantee == old_owner_id {
            dst_aip.ai_grantee = new_owner_id;
        } else if dst_aip.ai_grantee == new_owner_id {
            newpresent = true;
        }
    }

    // If the old ACL contained references to the new owner, merge any resulting
    // duplicate entries (the O(N^2) algorithm C uses). To simplify deletion we
    // temporarily zero a duplicate's privilege mask; such an entry is skipped.
    // dst is the next output slot, targ the currently considered input slot
    // (always >= dst), and src scans entries to the right of targ.
    if newpresent {
        let mut dst = 0usize;
        for targ in 0..num {
            // ignore if deleted in an earlier pass
            if aclitem_get_rights(new_acl[targ]) == ACL_NO_RIGHTS {
                continue;
            }
            // find and merge any duplicates
            for src in (targ + 1)..num {
                if aclitem_get_rights(new_acl[src]) == ACL_NO_RIGHTS {
                    continue;
                }
                if aclitem_match(&new_acl[targ], &new_acl[src]) {
                    let merged =
                        aclitem_get_rights(new_acl[targ]) | aclitem_get_rights(new_acl[src]);
                    aclitem_set_rights(&mut new_acl[targ], merged);
                    // mark the duplicate deleted
                    aclitem_set_rights(&mut new_acl[src], ACL_NO_RIGHTS);
                }
            }
            // and emit to output
            new_acl[dst] = new_acl[targ];
            dst += 1;
        }
        // Adjust array size to be 'dst' items.
        return Ok(&mut new_acl[..dst]);
    }

    Ok(new_acl)
}

/// `check_circularity` (acl.c) — guard against grant cycles before an update.
///
/// We recursively delete all grant options belonging to the target grantee,
/// then check whether the would-be grantor still has the grant option.
pub fn check_circularity<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    mod_aip: &AclItem,
    owner_id: Oid,
) -> PgResult<()> {
    check_acl(old_acl)?;

    // For now, grant options can only be granted to roles, not PUBLIC.
    debug_assert!(mod_aip.ai_grantee != ACL_ID_PUBLIC);

    // The owner always has grant options, no need to check.
    if mod_aip.ai_grantor == owner_id {
        return Ok(());
    }

    // Make a working copy.
    let mut acl: &mut [AclItem] = allocacl(mcx, old_acl.len() as i32)?;
    acl.copy_from_slice(old_acl);

    // Zap all grant options of target grantee, plus what depends on 'em.
    'cc_restart: loop {
        let num = acl.len();
        for i in 0..num {
            if acl[i].ai_grantee == mod_aip.ai_grantee
                && aclitem_get_goptions(acl[i]) != ACL_NO_RIGHTS
            {
                let item = acl[i];
                // We'll actually zap ordinary privs too, but no matter.
                acl = aclupdate(mcx, acl, &item, ACL_MODECHG_DEL, owner_id, DROP_CASCADE)?;
                continue 'cc_restart;
            }
        }
        break;
    }

    // Now compute grantor's independently-derived privileges.
    let mut own_privs = aclmask(
        acl,
        mod_aip.ai_grantor,
        owner_id,
        acl_grant_option_for(aclitem_get_goptions(*mod_aip)),
        ACLMASK_ALL,
    )?;
    own_privs = acl_option_to_privs(own_privs);

    if (aclitem_get_goptions(*mod_aip) & !own_privs) != 0 {
        return Err(
            PgError::error("grant options cannot be granted back to your own grantor")
                .with_sqlstate(ERRCODE_INVALID_GRANT_OPERATION),
        );
    }

    Ok(())
}

/// `recursive_revoke` (acl.c) — cascade-revoke privileges no longer grantable.
/// `behavior` is the C `DropBehavior`.
pub fn recursive_revoke<'mcx>(
    mcx: Mcx<'mcx>,
    acl: &'mcx mut [AclItem],
    grantee: Oid,
    revoke_privs: AclMode,
    owner_id: Oid,
    behavior: i32,
) -> PgResult<&'mcx mut [AclItem]> {
    check_acl(acl)?;

    // The owner can never truly lose grant options, so short-circuit.
    if grantee == owner_id {
        return Ok(acl);
    }

    // The grantee might still have some grant options via another grantor.
    let still_has = aclmask(
        acl,
        grantee,
        owner_id,
        acl_grant_option_for(revoke_privs),
        ACLMASK_ALL,
    )?;
    let mut revoke_privs = revoke_privs;
    revoke_privs &= !acl_option_to_privs(still_has);
    if revoke_privs == ACL_NO_RIGHTS {
        return Ok(acl);
    }

    let mut acl: &mut [AclItem] = acl;
    'restart: loop {
        let num = acl.len();
        for i in 0..num {
            if acl[i].ai_grantor == grantee
                && (aclitem_get_privs(acl[i]) & revoke_privs) != 0
            {
                if behavior == DROP_RESTRICT {
                    return Err(PgError::error("dependent privileges exist")
                        .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                        .with_hint("Use CASCADE to revoke them too."));
                }

                let mut mod_acl = AclItem {
                    ai_grantee: acl[i].ai_grantee,
                    ai_grantor: grantee,
                    ai_privs: 0,
                };
                aclitem_set_privs_goptions(&mut mod_acl, revoke_privs, revoke_privs);

                acl = aclupdate(mcx, acl, &mod_acl, ACL_MODECHG_DEL, owner_id, behavior)?;
                continue 'restart;
            }
        }
        break;
    }

    Ok(acl)
}

/// `aclmask` (acl.c) — privilege bits in `acl` available to `roleid`.
///
/// With `ACLMASK_ALL` returns the held bits ANDed with `mask`; with
/// `ACLMASK_ANY` returns as soon as any bit in the mask is known held.
pub fn aclmask(
    acl: &[AclItem],
    roleid: Oid,
    owner_id: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    check_acl(acl)?;

    // Quick exit for mask == 0.
    if mask == 0 {
        return Ok(0);
    }

    let mut result: AclMode = 0;

    // Owner always implicitly has all grant options.
    if (mask & ACLITEM_ALL_GOPTION_BITS) != 0 && has_privs_of_role::call(roleid, owner_id)? {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if mask_done(how, result, mask) {
            return Ok(result);
        }
    }

    // Check privileges granted directly to roleid or to public.
    for aidata in acl.iter() {
        if aidata.ai_grantee == ACL_ID_PUBLIC || aidata.ai_grantee == roleid {
            result |= aidata.ai_privs & mask;
            if mask_done(how, result, mask) {
                return Ok(result);
            }
        }
    }

    // Check privileges granted indirectly via role memberships. Done in a
    // separate pass to minimize expensive indirect membership tests: test
    // whether an entry grants any still-interesting privileges before the
    // has_privs_of_role test.
    let mut remaining = mask & !result;
    for aidata in acl.iter() {
        if aidata.ai_grantee == ACL_ID_PUBLIC || aidata.ai_grantee == roleid {
            continue; // already checked it
        }

        if (aidata.ai_privs & remaining) != 0
            && has_privs_of_role::call(roleid, aidata.ai_grantee)?
        {
            result |= aidata.ai_privs & mask;
            if mask_done(how, result, mask) {
                return Ok(result);
            }
            remaining = mask & !result;
        }
    }

    Ok(result)
}

/// `aclmask_direct` (acl.c) — like `aclmask` but without role-membership
/// expansion (direct grants to `roleid` only).
pub fn aclmask_direct(
    acl: &[AclItem],
    roleid: Oid,
    owner_id: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> AclMode {
    // C calls check_acl here; at this representation it is a no-op (Ok), so
    // there is no error path to surface — matching the infallible signature.

    // Quick exit for mask == 0.
    if mask == 0 {
        return 0;
    }

    let mut result: AclMode = 0;

    // Owner always implicitly has all grant options.
    if (mask & ACLITEM_ALL_GOPTION_BITS) != 0 && roleid == owner_id {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if mask_done(how, result, mask) {
            return result;
        }
    }

    // Check privileges granted directly to roleid (and not to public).
    for aidata in acl.iter() {
        if aidata.ai_grantee == roleid {
            result |= aidata.ai_privs & mask;
            if mask_done(how, result, mask) {
                return result;
            }
        }
    }

    result
}

/// `aclmembers` (acl.c) — distinct role OIDs mentioned in `acl`, into `mcx`.
///
/// Collects every grantee and grantor (not distinguishing the two), returning
/// the distinct OIDs in sorted order.
pub fn aclmembers<'mcx>(mcx: Mcx<'mcx>, acl: &[AclItem]) -> PgResult<&'mcx mut [Oid]> {
    if acl.is_empty() {
        return Ok(mcx::vec_with_capacity_in::<Oid>(mcx, 0)?.leak());
    }

    check_acl(acl)?;

    // Allocate the worst-case space requirement (2 OIDs per item).
    let mut list: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, acl.len() * 2)?;

    // Walk the ACL collecting mentioned RoleIds.
    for ai in acl.iter() {
        if ai.ai_grantee != ACL_ID_PUBLIC {
            list.push(ai.ai_grantee);
        }
        // grantor is currently never PUBLIC, but let's check anyway
        if ai.ai_grantor != ACL_ID_PUBLIC {
            list.push(ai.ai_grantor);
        }
    }

    // Sort the array (C: qsort(oid_cmp)).
    list.sort_unstable();

    // Remove duplicates (C: qunique keeps the first of each run).
    let mut w = 0usize;
    for r in 0..list.len() {
        if r == 0 || list[r] != list[w - 1] {
            list[w] = list[r];
            w += 1;
        }
    }
    list.truncate(w);

    Ok(list.leak())
}

/// `aclinsert` (acl.c) — deprecated SQL stub (`PG_FUNCTION_ARGS`).
pub fn aclinsert() -> PgResult<()> {
    Err(PgError::error("aclinsert is no longer supported")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `aclremove` (acl.c) — deprecated SQL stub (`PG_FUNCTION_ARGS`).
pub fn aclremove() -> PgResult<()> {
    Err(PgError::error("aclremove is no longer supported")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `aclcontains` (acl.c) — SQL: is an aclitem present in an acl array?
///
/// The fmgr `PG_FUNCTION_ARGS` marshaling (`PG_GETARG_ACL_P` / `_ACLITEM_P`,
/// `PG_RETURN_BOOL`) is the not-yet-ported fmgr/SRF layer; the argless scaffold
/// signature carries none of those inputs, so this panics loudly until that
/// boundary lands. The contained `aclcontains_impl` holds the faithful logic.
pub fn aclcontains() -> PgResult<bool> {
    panic!(
        "backend-utils-adt-acl::acl_ops::aclcontains: fmgr PG_FUNCTION_ARGS \
         marshaling not yet ported"
    )
}

/// The faithful body of `aclcontains` (acl.c) once its arguments are marshaled:
/// true iff some entry of `acl` has the same grantee and grantor as `aip` and
/// includes all of `aip`'s rights.
pub fn aclcontains_impl(acl: &[AclItem], aip: &AclItem) -> PgResult<bool> {
    check_acl(acl)?;
    for item in acl.iter() {
        if aip.ai_grantee == item.ai_grantee
            && aip.ai_grantor == item.ai_grantor
            && (aclitem_get_rights(*aip) & aclitem_get_rights(*item)) == aclitem_get_rights(*aip)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `makeaclitem` (acl.c) — SQL: build an aclitem from grantee/grantor/privs.
///
/// The fmgr `PG_FUNCTION_ARGS` marshaling (`PG_GETARG_OID`/`_TEXT_PP`/`_BOOL`,
/// `PG_RETURN_ACLITEM_P`) is the not-yet-ported fmgr layer; the argless
/// scaffold signature carries none of those inputs, so this panics loudly until
/// that boundary lands. The contained `makeaclitem_impl` holds the faithful
/// logic, including the `any_priv_map` privilege table.
pub fn makeaclitem() -> PgResult<AclItem> {
    panic!(
        "backend-utils-adt-acl::acl_ops::makeaclitem: fmgr PG_FUNCTION_ARGS \
         marshaling not yet ported"
    )
}

/// The faithful body of `makeaclitem` (acl.c) once its arguments are marshaled:
/// builds an `AclItem` from grantee/grantor and a parsed privilege string,
/// setting grant options to the same bits when `goption` is true.
pub fn makeaclitem_impl(
    grantee: Oid,
    grantor: Oid,
    privtext: &str,
    goption: bool,
) -> PgResult<AclItem> {
    // static const priv_map any_priv_map[] (acl.c:1643).
    let any_priv_map: &[(&str, AclMode)] = &[
        ("SELECT", ACL_SELECT),
        ("INSERT", ACL_INSERT),
        ("UPDATE", ACL_UPDATE),
        ("DELETE", ACL_DELETE),
        ("TRUNCATE", ACL_TRUNCATE),
        ("REFERENCES", ACL_REFERENCES),
        ("TRIGGER", ACL_TRIGGER),
        ("EXECUTE", ACL_EXECUTE),
        ("USAGE", ACL_USAGE),
        ("CREATE", ACL_CREATE),
        ("TEMP", ACL_CREATE_TEMP),
        ("TEMPORARY", ACL_CREATE_TEMP),
        ("CONNECT", ACL_CONNECT),
        ("SET", ACL_SET),
        ("ALTER SYSTEM", ACL_ALTER_SYSTEM),
        ("MAINTAIN", ACL_MAINTAIN),
    ];

    let priv_ = convert_any_priv_string_str(privtext, any_priv_map)?;

    let mut result = AclItem { ai_grantee: grantee, ai_grantor: grantor, ai_privs: 0 };
    aclitem_set_privs_goptions(
        &mut result,
        priv_,
        if goption { priv_ } else { ACL_NO_RIGHTS },
    );

    Ok(result)
}

/// Decode an `AclItem` from its 16-byte `repr(C)` image — the per-element
/// window an `aclitem[]` array deconstruction (`array_unnest`) hands back, or
/// `PG_GETARG_ACLITEM_P`'s by-reference pointer image. Mirrors the C in-memory
/// `AclItem` layout: `ai_grantee` (Oid/u32), `ai_grantor` (Oid/u32), `ai_privs`
/// (u64), all native-endian.
pub fn aclitem_from_image(bytes: &[u8]) -> AclItem {
    assert!(bytes.len() >= 16, "aclitem image too short");
    let grantee = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
    let grantor = u32::from_ne_bytes(bytes[4..8].try_into().unwrap());
    let privs = u64::from_ne_bytes(bytes[8..16].try_into().unwrap());
    AclItem { ai_grantee: grantee, ai_grantor: grantor, ai_privs: privs }
}

/// One expanded `aclexplode` row: `(grantor oid, grantee oid, privilege_type
/// text, is_grantable bool)` — the C `values[0..4]` of a single emitted tuple.
pub struct AclExplodeRow {
    /// `values[0] = ObjectIdGetDatum(aidata->ai_grantor)`.
    pub grantor: Oid,
    /// `values[1] = ObjectIdGetDatum(aidata->ai_grantee)`.
    pub grantee: Oid,
    /// `values[2] = convert_aclright_to_string(priv_bit)` — a static keyword.
    pub privilege_type: &'static str,
    /// `values[3] = (ACLITEM_GET_GOPTIONS(*aidata) & priv_bit) != 0`.
    pub is_grantable: bool,
}

/// `aclexplode` (acl.c) — SQL SRF: expand an acl array into rows.
///
/// The C function is a value-per-call SRF that walks the `AclItem` array
/// (`ACL_DAT(acl)`/`ACL_NUM(acl)`), and for each item scans the
/// `N_ACL_RIGHTS` privilege bits, emitting one `(grantor, grantee,
/// privilege_type, is_grantable)` row per privilege bit that is set in the
/// item's lower-32 privilege word. The emitted set is fully determined by the
/// input array, so this pure-data core renders the whole row series up front
/// (the executor-frame SRF driver then materializes it); the per-call
/// `FuncCallContext`/`SRF_*`/`heap_form_tuple` protocol is the executor-frame
/// driver's concern, not this unit's.
///
/// `acl` is the deconstructed `AclItem` slice (`ACL_DAT(acl)`); the C
/// `check_acl(acl)` header validation (`ndim == 1`, no nulls, `elemtype ==
/// ACLITEMOID`) is a property of the absent varlena/array header, so there is
/// nothing to validate here.
pub fn aclexplode(acl: &[AclItem]) -> alloc::vec::Vec<AclExplodeRow> {
    // C: aidat = ACL_DAT(acl); while (idx[0] < ACL_NUM(acl)) { for each of the
    // N_ACL_RIGHTS privilege bits: if the priv bit is set, emit a row }.
    let mut rows: alloc::vec::Vec<AclExplodeRow> = alloc::vec::Vec::new();
    for aidata in acl {
        for bit in 0..types_acl::N_ACL_RIGHTS {
            // priv_bit = UINT64CONST(1) << idx[1].
            let priv_bit: AclMode = 1u64 << bit;
            if aclitem_get_privs(*aidata) & priv_bit != 0 {
                rows.push(AclExplodeRow {
                    grantor: aidata.ai_grantor,
                    grantee: aidata.ai_grantee,
                    // convert_aclright_to_string takes the bit as an i32 (the C
                    // arg is `int`); priv_bit fits in 32 bits here.
                    privilege_type: convert_aclright_to_string(priv_bit as i32),
                    is_grantable: aclitem_get_goptions(*aidata) & priv_bit != 0,
                });
            }
        }
    }
    rows
}

/// `convert_aclright_to_string` (acl.c) — privilege bit to its keyword text.
pub fn convert_aclright_to_string(aclright: i32) -> &'static str {
    match aclright as AclMode {
        ACL_INSERT => "INSERT",
        ACL_SELECT => "SELECT",
        ACL_UPDATE => "UPDATE",
        ACL_DELETE => "DELETE",
        ACL_TRUNCATE => "TRUNCATE",
        ACL_REFERENCES => "REFERENCES",
        ACL_TRIGGER => "TRIGGER",
        ACL_EXECUTE => "EXECUTE",
        ACL_USAGE => "USAGE",
        ACL_CREATE => "CREATE",
        ACL_CREATE_TEMP => "TEMPORARY",
        ACL_CONNECT => "CONNECT",
        ACL_SET => "SET",
        ACL_ALTER_SYSTEM => "ALTER SYSTEM",
        ACL_MAINTAIN => "MAINTAIN",
        _ => panic!("unrecognized aclright: {aclright}"),
    }
}

/// `convert_any_priv_string` (acl.c) — parse a comma-separated privilege list
/// against a `priv_map` table into an `AclMode`.
///
/// The C signature takes a `text *` first argument; here the priv-string is
/// carried inside the `priv_map`-style slice's caller. This entry point keeps
/// the scaffold signature (the priv table) and is exercised via
/// `convert_any_priv_string_str` which adds the input string.
pub fn convert_any_priv_string(_privileges: &[(&str, AclMode)]) -> PgResult<AclMode> {
    panic!(
        "backend-utils-adt-acl::acl_ops::convert_any_priv_string: caller must \
         supply the privilege text (use convert_any_priv_string_str)"
    )
}

/// `convert_any_priv_string(priv_type_text, privileges)` (acl.c) — the faithful
/// parse: split `priv_type` at commas, trim each chunk, and OR in the matched
/// (case-insensitive) privilege's value, erroring on an unrecognized name.
pub fn convert_any_priv_string_str(
    priv_type: &str,
    privileges: &[(&str, AclMode)],
) -> PgResult<AclMode> {
    let mut result: AclMode = 0;

    // Split string at commas (C walks the modifiable copy via strchr).
    for chunk in priv_type.split(',') {
        // Drop leading/trailing whitespace in this chunk. C uses isspace() on
        // unsigned char; trim ASCII whitespace to match (space, \t, \n, \r,
        // \x0b, \x0c).
        let chunk = chunk.trim_matches(|c: char| c.is_ascii_whitespace());

        // Match (case-insensitively) to the privileges list.
        let matched = privileges
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(chunk));

        match matched {
            Some((_, value)) => result |= *value,
            None => {
                return Err(PgError::error(alloc::format!(
                    "unrecognized privilege type: \"{chunk}\""
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
    }

    Ok(result)
}
