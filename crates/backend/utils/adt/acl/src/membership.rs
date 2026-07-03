use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;

use cache_syscache::cacheinfo::{AUTHMEMMEMROLE, AUTHMEMROLEMEM, AUTHNAME, AUTHOID, DATABASEOID};
use cache_syscache::{
    GetSysCacheHashValue, GetSysCacheOid, ReleaseSysCache, ReleaseSysCacheList, SearchSysCache1,
    SearchSysCacheList1, SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use types_core::{catalog::ROLE_PG_DATABASE_OWNER, InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_tuple::HeapTupleData;

const ANUM_PG_AUTHID_OID: i32 = 1;
const ANUM_PG_DATABASE_DATDBA: i32 = 3;
const ANUM_PG_AUTH_MEMBERS_ROLEID: i32 = 2;
const ANUM_PG_AUTH_MEMBERS_INHERIT_OPTION: i32 = 6;
const ANUM_PG_AUTH_MEMBERS_SET_OPTION: i32 = 7;

const ROLES_LIST_BLOOM_THRESHOLD: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleRecurseType {
    Members = 0,
    Privs = 1,
    SetRole = 2,
}

struct MembershipCache {
    role: [Oid; 3],
    roles: [Vec<Oid>; 3],
}

thread_local! {
    // C: TopMemoryContext lists living for the backend; never dropped.
    static CACHE: RefCell<ManuallyDrop<MembershipCache>> = const {
        RefCell::new(ManuallyDrop::new(MembershipCache {
            role: [InvalidOid; 3],
            roles: [Vec::new(), Vec::new(), Vec::new()],
        }))
    };
    static CACHED_DB_HASH: Cell<u32> = const { Cell::new(0) };
}

pub fn initialize_acl() -> PgResult<()> {
    if !miscinit::IsBootstrapProcessingMode() {
        let dbid = init_small::globals::MyDatabaseId();
        CACHED_DB_HASH.set(GetSysCacheHashValue(
            DATABASEOID,
            SysCacheKey::Value(Datum::from_oid(dbid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?);
        for cacheid in [AUTHMEMROLEMEM, AUTHOID, DATABASEOID] {
            inval::invalidate::CacheRegisterSyscacheCallback(
                cacheid,
                RoleMembershipCacheCallback,
                Datum::null(),
            )?;
        }
    }
    Ok(())
}

pub fn RoleMembershipCacheCallback(_arg: Datum, cacheid: i32, hashvalue: u32) {
    if cacheid == DATABASEOID && hashvalue != CACHED_DB_HASH.get() && hashvalue != 0 {
        return; // ignore pg_database changes for other DBs
    }
    CACHE.with(|c| c.borrow_mut().role = [InvalidOid; 3]);
}

fn roles_list_append(roles_list: &mut Vec<Oid>, role: Oid) {
    if roles_list.contains(&role) {
        return;
    }
    assert!(
        roles_list.len() <= ROLES_LIST_BLOOM_THRESHOLD,
        "roles_list_append: Bloom-filter fastpath unported past {ROLES_LIST_BLOOM_THRESHOLD} roles"
    );
    roles_list.push(role);
}

fn getattr(tuple: &HeapTupleData<'_>, attnum: i32) -> Datum {
    let td = match catcache::cache_tupdesc(AUTHMEMMEMROLE) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(AUTHMEMMEMROLE, false)
                .expect("catcache phase-2 init for pg_auth_members");
            catcache::cache_tupdesc(AUTHMEMMEMROLE).expect("phase-2 init left no tupdesc")
        }
    };
    let mut isnull = false;
    // SAFETY: caller passes a pg_auth_members tuple; the read columns are
    // fixed-width NOT NULL leading columns.
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, td, &mut isnull) };
    debug_assert!(!isnull);
    d
}

// C returns the cached List*; every ported caller runs list_member_oid on it,
// so the containment test is fused here.
pub(crate) fn roles_is_member_of_contains(
    roleid: Oid,
    rtype: RoleRecurseType,
    target: Oid,
) -> PgResult<bool> {
    let t = rtype as usize;

    let hit = CACHE.with(|c| {
        let cache = c.borrow();
        if cache.role[t] == roleid && cache.role[t] != InvalidOid {
            Some(cache.roles[t].contains(&target))
        } else {
            None
        }
    });
    if let Some(found) = hit {
        return Ok(found);
    }

    // A non-database backend (walsender SHOW) expands roles with no
    // pg_database_owner membership.
    let my_db = init_small::globals::MyDatabaseId();
    let dba = if my_db == InvalidOid {
        InvalidOid
    } else {
        let Some(tuple) = SearchSysCache1(DATABASEOID, SysCacheKey::Value(Datum::from_oid(my_db)))?
        else {
            return Err(PgError::error(format!("cache lookup failed for database {my_db}")).into());
        };
        let dba = SysCacheGetAttrNotNull(DATABASEOID, &tuple, ANUM_PG_DATABASE_DATDBA)?.as_oid();
        ReleaseSysCache(tuple);
        dba
    };

    // Breadth-first: the list is both the found-set and the agenda.
    let mut roles_list: Vec<Oid> = Vec::with_capacity(8);
    roles_list.push(roleid);
    let mut i = 0;
    while i < roles_list.len() {
        let memberid = roles_list[i];
        let memlist =
            SearchSysCacheList1(AUTHMEMMEMROLE, SysCacheKey::Value(Datum::from_oid(memberid)))?;
        for m in 0..memlist.n_members() as usize {
            let member = memlist.member(m);
            let tuple = member.tuple();
            let otherid = getattr(&tuple, ANUM_PG_AUTH_MEMBERS_ROLEID).as_oid();
            if rtype == RoleRecurseType::Privs
                && !getattr(&tuple, ANUM_PG_AUTH_MEMBERS_INHERIT_OPTION).as_bool()
            {
                continue;
            }
            if rtype == RoleRecurseType::SetRole
                && !getattr(&tuple, ANUM_PG_AUTH_MEMBERS_SET_OPTION).as_bool()
            {
                continue;
            }
            roles_list_append(&mut roles_list, otherid);
        }
        ReleaseSysCacheList(memlist);

        if memberid == dba && dba != InvalidOid {
            roles_list_append(&mut roles_list, ROLE_PG_DATABASE_OWNER);
        }
        i += 1;
    }

    let found = roles_list.contains(&target);
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        cache.role[t] = InvalidOid;
        cache.roles[t] = roles_list;
        cache.role[t] = roleid;
    });
    Ok(found)
}

/// Recurses only through inheritable grants; use for privilege checks.
pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    if member == role {
        return Ok(true);
    }
    if superuser::superuser_arg(member)? {
        return Ok(true);
    }
    roles_is_member_of_contains(member, RoleRecurseType::Privs, role)
}

pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool> {
    if member == role {
        return Ok(true);
    }
    if superuser::superuser_arg(member)? {
        return Ok(true);
    }
    roles_is_member_of_contains(member, RoleRecurseType::SetRole, role)
}

pub fn is_member_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    if member == role {
        return Ok(true);
    }
    if superuser::superuser_arg(member)? {
        return Ok(true);
    }
    roles_is_member_of_contains(member, RoleRecurseType::Members, role)
}

pub fn is_member_of_role_nosuper(member: Oid, role: Oid) -> PgResult<bool> {
    if member == role {
        return Ok(true);
    }
    roles_is_member_of_contains(member, RoleRecurseType::Members, role)
}

// roles_is_member_of (acl.c) list form: snapshot of the cached expansion.
pub(crate) fn roles_is_member_of_list(
    roleid: Oid,
    rtype: RoleRecurseType,
) -> PgResult<Vec<Oid>> {
    roles_is_member_of_contains(roleid, rtype, InvalidOid)?;
    let t = rtype as usize;
    Ok(CACHE.with(|c| {
        let cache = c.borrow();
        debug_assert_eq!(cache.role[t], roleid);
        cache.roles[t].clone()
    }))
}

pub fn get_role_oid_or_public(rolname: &str) -> PgResult<Oid> {
    if rolname == "public" {
        return Ok(crate::ACL_ID_PUBLIC);
    }
    get_role_oid(rolname, false)
}

pub fn get_role_oid(rolname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = GetSysCacheOid(
        AUTHNAME,
        ANUM_PG_AUTHID_OID,
        SysCacheKey::Str(rolname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if oid == InvalidOid && !missing_ok {
        return Err(
            PgError::new(ERROR, format!("role \"{rolname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
                .into(),
        );
    }
    Ok(oid)
}

#[cfg(test)]
pub(crate) fn seed_membership_cache(rtype_idx: usize, roleid: Oid, roles: Vec<Oid>) {
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        cache.role[rtype_idx] = roleid;
        cache.roles[rtype_idx] = roles;
    });
}

#[cfg(test)]
pub(crate) fn cached_role(rtype_idx: usize) -> Oid {
    CACHE.with(|c| c.borrow().role[rtype_idx])
}

#[cfg(test)]
pub(crate) fn seed_db_hash(h: u32) {
    CACHED_DB_HASH.set(h);
}
