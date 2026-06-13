//! Role-membership cache and queries (`utils/adt/acl.c`).
//!
//! `initialize_acl`/`RoleMembershipCacheCallback` set up and invalidate the
//! per-backend `cached_roles` lists; `roles_list_append`/`roles_is_member_of`
//! build the transitive membership set (with a Bloom-filter fast path past
//! `ROLES_LIST_BLOOM_THRESHOLD`). The public predicates (`has_privs_of_role`,
//! `member_can_set_role`, `check_can_set_role`, `is_member_of_role`,
//! `is_member_of_role_nosuper`, `is_admin_of_role`, `select_best_admin`,
//! `select_best_grantor`) and the rolespec resolvers (`get_role_oid`,
//! `get_role_oid_or_public`, `get_rolespec_oid`, `get_rolespec_tuple`,
//! `get_rolespec_name`, `check_rolespec_name`) sit on top.

use std::cell::RefCell;
use std::ptr;

use backend_lib_bloomfilter_seams::{self as bloom_seams, BloomFilter};
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit::{GetSessionUserId, GetUserId, GetUserNameFromId};
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as globals_seams;
use backend_utils_misc_superuser_seams as superuser_seams;
use mcx::{Mcx, MemoryContext};
use types_acl::{
    AclItem, AclMode, RoleRecurseType, ACL_GRANT_OPTION_FOR, ACL_ID_PUBLIC, ACL_NO_RIGHTS,
    ACLMASK_ALL, ROLERECURSE_MEMBERS, ROLERECURSE_PRIVS, ROLERECURSE_SETROLE,
};
use types_core::{InvalidOid, Oid, OidIsValid, ROLE_PG_DATABASE_OWNER};
use types_datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_RESERVED_NAME,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::{
    RoleSpec, ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE, ROLESPEC_CURRENT_USER, ROLESPEC_PUBLIC,
    ROLESPEC_SESSION_USER,
};
use types_syscache::{AUTHMEMROLEMEM, AUTHOID, DATABASEOID};

use crate::acl_ops::aclmask_direct;

/// `ROLES_LIST_BLOOM_THRESHOLD` (acl.c) — membership-list size past which a
/// Bloom filter is built to speed up membership tests.
pub const ROLES_LIST_BLOOM_THRESHOLD: i32 = 1024;

/// Run `f` with a transient `MemoryContext` for syscache projections that copy
/// strings/tuples out of the cache (the C `CurrentMemoryContext` of these
/// call sites). The context is dropped on return.
fn with_transient<R>(f: impl for<'mcx> FnOnce(Mcx<'mcx>) -> PgResult<R>) -> PgResult<R> {
    let cx = MemoryContext::new("acl role-membership transient");
    f(cx.mcx())
}

/* ---------------------------------------------------------------------------
 * Per-backend file-scope statics (acl.c:70-91).
 *
 * The C `cached_roles[]` lists live in `TopMemoryContext` and persist for the
 * life of the backend, so each is modeled here by a leaked `Box<[Oid]>`
 * exposed as `&'static [Oid]`; recomputation reclaims (drops, the C
 * `list_free`) the previous slice before installing the new one.
 * ------------------------------------------------------------------------- */

struct RoleCache {
    /// `static Oid cached_role[] = {InvalidOid, InvalidOid, InvalidOid}`.
    cached_role: [Oid; 3],
    /// `static List *cached_roles[] = {NIL, NIL, NIL}` — leaked, persistent.
    cached_roles: [&'static [Oid]; 3],
    /// `static uint32 cached_db_hash`.
    cached_db_hash: u32,
}

thread_local! {
    static ROLE_CACHE: RefCell<RoleCache> = const {
        RefCell::new(RoleCache {
            cached_role: [InvalidOid, InvalidOid, InvalidOid],
            cached_roles: [&[], &[], &[]],
            cached_db_hash: 0,
        })
    };
}

/// `initialize_acl` (acl.c) — register the syscache invalidation callback and
/// reset the per-backend membership cache.
pub fn initialize_acl() -> PgResult<()> {
    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        let hash = syscache_seams::database_syscache_hash_value::call(
            globals_seams::my_database_id::call(),
        )?;
        ROLE_CACHE.with(|c| c.borrow_mut().cached_db_hash = hash);

        /*
         * In normal mode, set a callback on any syscache invalidation of rows
         * of pg_auth_members (for roles_is_member_of()) pg_database (for
         * roles_is_member_of())
         */
        inval_seams::cache_register_syscache_callback::call(
            AUTHMEMROLEMEM,
            role_membership_cache_callback,
            Datum::null(),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            AUTHOID,
            role_membership_cache_callback,
            Datum::null(),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            DATABASEOID,
            role_membership_cache_callback,
            Datum::null(),
        )?;
    }
    Ok(())
}

/// `RoleMembershipCacheCallback` (acl.c) — syscache invalidation callback that
/// flushes the cached membership lists.
pub fn role_membership_cache_callback(_arg: Datum, cacheid: i32, hashvalue: u32) {
    ROLE_CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if cacheid == DATABASEOID && hashvalue != c.cached_db_hash && hashvalue != 0 {
            return; /* ignore pg_database changes for other DBs */
        }

        /* Force membership caches to be recomputed on next use */
        c.cached_role[ROLERECURSE_MEMBERS as usize] = InvalidOid;
        c.cached_role[ROLERECURSE_PRIVS as usize] = InvalidOid;
        c.cached_role[ROLERECURSE_SETROLE as usize] = InvalidOid;
    });
}

/// `roles_list_append` (acl.c) — a helper for `roles_is_member_of` providing an
/// optimized `list_append_unique_oid()` via a Bloom filter. The caller owns
/// freeing `*bf` once done.
fn roles_list_append(roles_list: &mut Vec<Oid>, bf: &mut *mut BloomFilter, role: Oid) {
    let roleptr = (&role as *const Oid).cast::<u8>();

    /*
     * If there is a previously-created Bloom filter, use it to try to
     * determine whether the role is missing from the list. If it says yes,
     * that's a hard fact and we can go ahead and add the role. If it says no,
     * that's only probabilistic and we'd better search the list. Without a
     * filter, we must always do an ordinary linear search through the
     * existing list.
     */
    if (!(*bf).is_null()
        && bloom_seams::bloom_lacks_element::call(*bf, roleptr, std::mem::size_of::<Oid>()))
        || !list_member_oid(roles_list, role)
    {
        /*
         * If the list is large, we take on the overhead of creating and
         * populating a Bloom filter to speed up future calls to this
         * function.
         */
        if (*bf).is_null() && roles_list.len() as i32 > ROLES_LIST_BLOOM_THRESHOLD {
            *bf = bloom_seams::bloom_create::call(
                (ROLES_LIST_BLOOM_THRESHOLD * 10) as i64,
                globals_seams::work_mem::call(),
                0,
            )
            .expect("bloom_create");
            for roleid in roles_list.iter() {
                bloom_seams::bloom_add_element::call(
                    *bf,
                    (roleid as *const Oid).cast::<u8>(),
                    std::mem::size_of::<Oid>(),
                );
            }
        }

        /*
         * Finally, add the role to the list and the Bloom filter, if it
         * exists.
         */
        roles_list.push(role);
        if !(*bf).is_null() {
            bloom_seams::bloom_add_element::call(*bf, roleptr, std::mem::size_of::<Oid>());
        }
    }
}

/// `roles_is_member_of` (acl.c) — the transitive set of roles `roleid` belongs
/// to under `type`. `admin_of`/`admin_role` is the optional admin-search out
/// param. Returns the cached OID list.
///
/// The C cached result lives in `TopMemoryContext`; here it is a persistent
/// leaked slice (see [`RoleCache`]).
fn roles_is_member_of(
    roleid: Oid,
    ty: RoleRecurseType,
    admin_of: Oid,
    mut admin_role: Option<&mut Oid>,
) -> PgResult<&'static [Oid]> {
    debug_assert_eq!(OidIsValid(admin_of), admin_role.is_some());
    if let Some(ar) = admin_role.as_deref_mut() {
        *ar = InvalidOid;
    }

    /* If cache is valid and ADMIN OPTION not sought, just return the list */
    if let Some(cached) = ROLE_CACHE.with(|c| {
        let c = c.borrow();
        if c.cached_role[ty as usize] == roleid
            && !OidIsValid(admin_of)
            && OidIsValid(c.cached_role[ty as usize])
        {
            Some(c.cached_roles[ty as usize])
        } else {
            None
        }
    }) {
        return Ok(cached);
    }

    /*
     * Role expansion happens in a non-database backend when guc.c checks
     * ROLE_PG_READ_ALL_SETTINGS for a physical walsender SHOW command. In that
     * case, no role gets pg_database_owner.
     */
    let my_db = globals_seams::my_database_id::call();
    let dba: Oid = if !OidIsValid(my_db) {
        InvalidOid
    } else {
        match syscache_seams::database_datdba::call(my_db)? {
            Some(datdba) => datdba,
            None => {
                return Err(PgError::new(
                    ERROR,
                    format!("cache lookup failed for database {my_db}"),
                ));
            }
        }
    };

    /*
     * Find all the roles that roleid is a member of, including multi-level
     * recursion. The role itself will always be the first element of the
     * resulting list.
     *
     * Each element of the list is scanned to see if it adds any indirect
     * memberships. We can use a single list as both the record of
     * already-found memberships and the agenda of roles yet to be scanned.
     */
    let mut roles_list: Vec<Oid> = vec![roleid];
    let mut bf: *mut BloomFilter = ptr::null_mut();

    let mut idx = 0;
    while idx < roles_list.len() {
        let memberid = roles_list[idx];

        /* Find roles that memberid is directly a member of */
        let memlist = syscache_seams::auth_members_of_member::call(memberid)?;
        for form in memlist.iter() {
            let otherid = form.roleid;

            /*
             * While otherid==InvalidOid shouldn't appear in the catalog, the
             * OidIsValid() avoids crashing if that arises.
             */
            if otherid == admin_of && form.admin_option && OidIsValid(admin_of) {
                if let Some(ar) = admin_role.as_deref_mut() {
                    if !OidIsValid(*ar) {
                        *ar = memberid;
                    }
                }
            }

            /* If we're supposed to ignore non-heritable grants, do so. */
            if ty == ROLERECURSE_PRIVS && !form.inherit_option {
                continue;
            }

            /* If we're supposed to ignore non-SET grants, do so. */
            if ty == ROLERECURSE_SETROLE && !form.set_option {
                continue;
            }

            /*
             * Even though there shouldn't be any loops in the membership
             * graph, we must test for having already seen this role. It is
             * legal for instance to have both A->B and A->C->B.
             */
            roles_list_append(&mut roles_list, &mut bf, otherid);
        }

        /* implement pg_database_owner implicit membership */
        if memberid == dba && OidIsValid(dba) {
            roles_list_append(&mut roles_list, &mut bf, ROLE_PG_DATABASE_OWNER);
        }

        idx += 1;
    }

    /*
     * Free the Bloom filter created by roles_list_append(), if there is one.
     */
    if !bf.is_null() {
        bloom_seams::bloom_free::call(bf);
    }

    /*
     * Copy the completed list into TopMemoryContext so it will persist (here:
     * a leaked persistent slice), then assign to the state variable, freeing
     * (dropping) the prior list.
     */
    let new_cached_roles: &'static [Oid] = Box::leak(roles_list.into_boxed_slice());

    ROLE_CACHE.with(|c| {
        let mut c = c.borrow_mut();
        c.cached_role[ty as usize] = InvalidOid; /* just paranoia */
        let old = c.cached_roles[ty as usize];
        /* list_free(cached_roles[type]) — reclaim the previous leaked slice */
        if !old.is_empty() {
            // SAFETY: every non-empty cached_roles slot was installed by a
            // prior Box::leak here; reclaim it to mirror the C list_free.
            unsafe {
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                    old.as_ptr() as *mut Oid,
                    old.len(),
                )));
            }
        }
        c.cached_roles[ty as usize] = new_cached_roles;
        c.cached_role[ty as usize] = roleid;
    });

    /* And now we can return the answer */
    Ok(new_cached_roles)
}

/// `list_member_oid(list, datum)` — linear membership test over an OID list.
fn list_member_oid(list: &[Oid], oid: Oid) -> bool {
    list.iter().any(|&x| x == oid)
}

/// `has_privs_of_role` (acl.c) — does `member` inherit privileges of `role`?
pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    /* Fast path for simple case */
    if member == role {
        return Ok(true);
    }

    /* Superusers have every privilege, so are part of every role */
    if superuser_seams::superuser_arg::call(member)? {
        return Ok(true);
    }

    /*
     * Find all the roles that member has the privileges of, including
     * multi-level recursion, then see if target role is any one of them.
     */
    let list = roles_is_member_of(member, ROLERECURSE_PRIVS, InvalidOid, None)?;
    Ok(list_member_oid(list, role))
}

/// `member_can_set_role` (acl.c) — may `member` `SET ROLE` to `role`?
pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool> {
    /* Fast path for simple case */
    if member == role {
        return Ok(true);
    }

    /* Superusers have every privilege, so can always SET ROLE */
    if superuser_seams::superuser_arg::call(member)? {
        return Ok(true);
    }

    /*
     * Find all the roles that member can access via SET ROLE, including
     * multi-level recursion, then see if target role is any one of them.
     */
    let list = roles_is_member_of(member, ROLERECURSE_SETROLE, InvalidOid, None)?;
    Ok(list_member_oid(list, role))
}

/// `check_can_set_role` (acl.c) — error unless `member` may `SET ROLE` to
/// `role`.
pub fn check_can_set_role(member: Oid, role: Oid) -> PgResult<()> {
    if !member_can_set_role(member, role)? {
        let name = with_transient(|mcx| {
            Ok(GetUserNameFromId(mcx, role, false)?
                .map(|s| s.to_string())
                .unwrap_or_default())
        })?;
        return Err(PgError::new(
            ERROR,
            format!("must be able to SET ROLE \"{name}\""),
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }
    Ok(())
}

/// `is_member_of_role` (acl.c) — is `member` a member of `role` (any path)?
pub fn is_member_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    /* Fast path for simple case */
    if member == role {
        return Ok(true);
    }

    /* Superusers have every privilege, so are part of every role */
    if superuser_seams::superuser_arg::call(member)? {
        return Ok(true);
    }

    /*
     * Find all the roles that member is a member of, including multi-level
     * recursion, then see if target role is any one of them.
     */
    let list = roles_is_member_of(member, ROLERECURSE_MEMBERS, InvalidOid, None)?;
    Ok(list_member_oid(list, role))
}

/// `is_member_of_role_nosuper` (acl.c) — membership test ignoring superuser.
pub fn is_member_of_role_nosuper(member: Oid, role: Oid) -> PgResult<bool> {
    /* Fast path for simple case */
    if member == role {
        return Ok(true);
    }

    /*
     * Find all the roles that member is a member of, including multi-level
     * recursion, then see if target role is any one of them.
     */
    let list = roles_is_member_of(member, ROLERECURSE_MEMBERS, InvalidOid, None)?;
    Ok(list_member_oid(list, role))
}

/// `is_admin_of_role` (acl.c) — may `member` administer membership in `role`?
pub fn is_admin_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    if superuser_seams::superuser_arg::call(member)? {
        return Ok(true);
    }

    /* By policy, a role cannot have WITH ADMIN OPTION on itself. */
    if member == role {
        return Ok(false);
    }

    let mut admin_role = InvalidOid;
    roles_is_member_of(member, ROLERECURSE_MEMBERS, role, Some(&mut admin_role))?;
    Ok(OidIsValid(admin_role))
}

/// `select_best_admin` (acl.c) — pick an admin role through which `member` can
/// administer `role`, or `InvalidOid`.
pub fn select_best_admin(member: Oid, role: Oid) -> PgResult<Oid> {
    /* By policy, a role cannot have WITH ADMIN OPTION on itself. */
    if member == role {
        return Ok(InvalidOid);
    }

    let mut admin_role = InvalidOid;
    roles_is_member_of(member, ROLERECURSE_PRIVS, role, Some(&mut admin_role))?;
    Ok(admin_role)
}

/// `select_best_grantor` (acl.c) — choose the grantor role and grantable
/// privileges for a GRANT performed by `role_id`. `acl` is the ACL of the
/// object in question. Returns `(grantor_id, grant_options)`.
///
/// If no grant options exist, `grantor_id` is `role_id` and `grant_options`
/// is `ACL_NO_RIGHTS`.
pub fn select_best_grantor(
    role_id: Oid,
    privileges: AclMode,
    acl: &[AclItem],
    owner_id: Oid,
) -> PgResult<(Oid, AclMode)> {
    let needed_goptions = ACL_GRANT_OPTION_FOR(privileges);

    /*
     * The object owner is always treated as having all grant options, so if
     * roleId is the owner it's easy. Also, if roleId is a superuser it's easy:
     * superusers are implicitly members of every role, so they act as the
     * object owner.
     */
    if role_id == owner_id || superuser_seams::superuser_arg::call(role_id)? {
        return Ok((owner_id, needed_goptions));
    }

    /*
     * Otherwise we have to do a careful search to see if roleId has the
     * privileges of any suitable role. Note: we can hang onto the result of
     * roles_is_member_of() throughout this loop, because aclmask_direct()
     * doesn't query any role memberships.
     */
    let roles_list = roles_is_member_of(role_id, ROLERECURSE_PRIVS, InvalidOid, None)?;

    /* initialize candidate result as default */
    let mut grantor_id = role_id;
    let mut grant_options = ACL_NO_RIGHTS;
    let mut nrights = 0;

    for &otherrole in roles_list.iter() {
        let otherprivs = aclmask_direct(acl, otherrole, owner_id, needed_goptions, ACLMASK_ALL);
        if otherprivs == needed_goptions {
            /* Found a suitable grantor */
            return Ok((otherrole, otherprivs));
        }

        /*
         * If it has just some of the needed privileges, remember best
         * candidate.
         */
        if otherprivs != ACL_NO_RIGHTS {
            let nnewrights = otherprivs.count_ones() as i32;

            if nnewrights > nrights {
                grantor_id = otherrole;
                grant_options = otherprivs;
                nrights = nnewrights;
            }
        }
    }

    Ok((grantor_id, grant_options))
}

/// `get_role_oid` (acl.c) — resolve a role name to its OID, honoring
/// `missing_ok`.
pub fn get_role_oid(rolname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = with_transient(|mcx| {
        Ok(syscache_seams::lookup_authid_by_name::call(mcx, rolname)?.map(|r| r.oid))
    })?
    .unwrap_or(InvalidOid);

    if !OidIsValid(oid) && !missing_ok {
        return Err(PgError::new(
            ERROR,
            format!("role \"{rolname}\" does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `get_role_oid_or_public` (acl.c) — like `get_role_oid`, mapping "public" to
/// `ACL_ID_PUBLIC`.
pub fn get_role_oid_or_public(rolname: &str) -> PgResult<Oid> {
    if rolname == "public" {
        return Ok(ACL_ID_PUBLIC);
    }

    get_role_oid(rolname, false)
}

/// `get_rolespec_oid` (acl.c) — resolve a parser `RoleSpec` to a role OID.
///
/// PUBLIC is always disallowed here. Routines wanting to handle the PUBLIC
/// case must check the case separately.
pub fn get_rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid> {
    let oid = match role.roletype {
        ROLESPEC_CSTRING => {
            debug_assert!(role.rolename.is_some());
            get_role_oid(
                role.rolename
                    .as_deref()
                    .expect("ROLESPEC_CSTRING requires rolename"),
                missing_ok,
            )?
        }

        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => GetUserId(),

        ROLESPEC_SESSION_USER => GetSessionUserId(),

        ROLESPEC_PUBLIC => {
            return Err(PgError::new(ERROR, "role \"public\" does not exist".to_string())
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    };

    Ok(oid)
}

/// `get_rolespec_tuple` (acl.c) — fetch the `pg_authid` row for a `RoleSpec`.
///
/// In C this returns a `HeapTuple` the caller must `ReleaseSysCache`; here the
/// projected [`AuthIdRow`](types_cache::AuthIdRow) is returned by value into
/// `mcx`, subsuming the release.
pub fn get_rolespec_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    role: &RoleSpec<'_>,
) -> PgResult<types_cache::AuthIdRow<'mcx>> {
    let tuple = match role.roletype {
        ROLESPEC_CSTRING => {
            debug_assert!(role.rolename.is_some());
            let name = role
                .rolename
                .as_deref()
                .expect("ROLESPEC_CSTRING requires rolename");
            match syscache_seams::lookup_authid_by_name::call(mcx, name)? {
                Some(t) => t,
                None => {
                    return Err(PgError::new(
                        ERROR,
                        format!("role \"{name}\" does not exist"),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
                }
            }
        }

        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => {
            let uid = GetUserId();
            match syscache_seams::lookup_authid_by_oid::call(mcx, uid)? {
                Some(t) => t,
                None => {
                    return Err(PgError::new(
                        ERROR,
                        format!("cache lookup failed for role {uid}"),
                    ));
                }
            }
        }

        ROLESPEC_SESSION_USER => {
            let uid = GetSessionUserId();
            match syscache_seams::lookup_authid_by_oid::call(mcx, uid)? {
                Some(t) => t,
                None => {
                    return Err(PgError::new(
                        ERROR,
                        format!("cache lookup failed for role {uid}"),
                    ));
                }
            }
        }

        ROLESPEC_PUBLIC => {
            return Err(PgError::new(ERROR, "role \"public\" does not exist".to_string())
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    };

    Ok(tuple)
}

/// `get_rolespec_name` (acl.c) — the role name a `RoleSpec` resolves to (a
/// `pstrdup`'d copy of `NameStr(authForm->rolname)`).
pub fn get_rolespec_name<'mcx>(mcx: Mcx<'mcx>, role: &RoleSpec<'_>) -> PgResult<String> {
    let tp = get_rolespec_tuple(mcx, role)?;
    Ok(tp.rolname.to_string())
}

/// `check_rolespec_name` (acl.c) — reject reserved role names with
/// `detail_msg`.
///
/// If `role` is `None`, no error is thrown. If `detail_msg` is `None` then no
/// detail message is provided.
pub fn check_rolespec_name(role: Option<&RoleSpec<'_>>, detail_msg: Option<&str>) -> PgResult<()> {
    let role = match role {
        Some(r) => r,
        None => return Ok(()),
    };

    if role.roletype != ROLESPEC_CSTRING {
        return Ok(());
    }

    let rolename = role
        .rolename
        .as_deref()
        .expect("ROLESPEC_CSTRING requires rolename");

    if backend_commands_user_seams::is_reserved_name::call(rolename.to_string())? {
        let err = PgError::new(ERROR, format!("role name \"{rolename}\" is reserved"))
            .with_sqlstate(ERRCODE_RESERVED_NAME);
        let err = match detail_msg {
            Some(detail) => err.with_detail(detail.to_string()),
            None => err,
        };
        return Err(err);
    }
    Ok(())
}
