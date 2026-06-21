//! `backend-commands-subscriptioncmds` — `commands/subscriptioncmds.c`.
//!
//! This unit's owner-change surface: [`AlterSubscriptionOwner_internal`],
//! [`AlterSubscriptionOwner`] (by name, ALTER SUBSCRIPTION ... OWNER TO) and
//! [`AlterSubscriptionOwner_oid`] (by OID, reached from REASSIGN OWNED via
//! `backend-catalog-pg-shdepend`). Ported 1:1 from
//! `subscriptioncmds.c:1966-2085`: the owner-equality short-circuit, the
//! `object_ownercheck` / password_required superuser gate / `check_can_set_role`
//! / database `ACL_CREATE` permission checks, the `CatalogTupleUpdate` of
//! `subowner`, the `changeDependencyOnOwner` owner-dependency rewrite, the
//! post-alter hook, and the launcher / apply-worker wakeups.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::ToString;

use mcx::Mcx;

use backend_utils_error::{ereport, PgResult};
use types_error::{PgError, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERROR};

use types_acl::acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_CREATE, AclResult};
use types_catalog::catalog::DATABASE_RELATION_ID;
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_subscription::{
    Anum_pg_subscription_oid, Anum_pg_subscription_subname, Anum_pg_subscription_subowner,
    Anum_pg_subscription_subpasswordrequired, Natts_pg_subscription, SubscriptionRelationId,
};
use types_core::primitive::{InvalidOid, Oid};
use types_nodes::parsenodes::ObjectType;
use types_storage::lock::RowExclusiveLock;
use types_syscache::syscache_ids::SUBSCRIPTIONOID;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use backend_access_common_heaptuple::{heap_deform_tuple, heap_modify_tuple};
use backend_catalog_aclchk::{object_aclcheck, object_ownercheck};
use backend_catalog_indexing::keystone::CatalogTupleUpdate;
use backend_catalog_pg_shdepend::changeDependencyOnOwner;
use backend_utils_cache_syscache::cacheinfo::SUBSCRIPTIONNAME;
use backend_utils_cache_syscache::{ReleaseSysCache, SearchSysCache1, SearchSysCache2};
use backend_utils_init_miscinit::GetUserId;

use types_cache::syscache::SysCacheKey;
use types_datum::Datum as KeyDatum;

use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_objectaccess_seams as objaccess;
use backend_commands_dbcommands_seams as dbcommands_seams;
use backend_commands_tablespace_globals_seams as globals_seams;
use backend_replication_logical_launcher_seams as launcher_seams;
use backend_replication_logical_worker_seams as worker_seams;
use backend_utils_adt_acl_seams as acl_seams;
use backend_utils_misc_superuser_seams as superuser_seams;

mod cmds;
mod inward;

pub use cmds::{AlterSubscription, CreateSubscription, DropSubscription};

/// `ObjectIdGetDatum(value)` as a syscache key word.
pub(crate) fn oid_cache_key(value: Oid) -> SysCacheKey<'static> {
    SysCacheKey::Value(KeyDatum::from_oid(value))
}

/// `CStringGetTextDatum(s)` — a `text` varlena Datum (4-byte header + payload).
pub(crate) fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const VARHDRSZ: usize = 4;
    let payload = s.as_bytes();
    let total = VARHDRSZ + payload.len();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[VARHDRSZ..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// `namein(s)` image — a `NAMEDATALEN`-byte NUL-padded `NameData` Datum.
pub(crate) fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    use types_core::fmgr::NAMEDATALEN;
    let mut image: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN as usize)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), (NAMEDATALEN as usize) - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < NAMEDATALEN as usize {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// `ObjectAddressSet(addr, class, object)`.
pub(crate) fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `NameStr(...)` — the bytes up to the first NUL of a `NameData` image.
pub(crate) fn name_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// `aclcheck_error(aclerr, objtype, objectname)` (aclchk.c) — the C is
/// `pg_noreturn`; this helper always returns the resulting `PgError`.
pub(crate) fn aclcheck_error_str(aclerr: AclResult, objtype: ObjectType, objectname: &str) -> PgError {
    match aclchk_seams::aclcheck_error::call(aclerr, objtype, Some(objectname.to_string())) {
        Ok(()) => ereport(ERROR)
            .errmsg_internal("aclcheck_error seam returned without raising")
            .into_error(),
        Err(e) => e,
    }
}

/// `(Form_pg_subscription) GETSTRUCT(tup)` — deform the cached tuple's fixed
/// columns. The fixed-width prefix columns (`oid`, `subowner`,
/// `subpasswordrequired`, `subname`) read here are never NULL.
pub(crate) fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, (Datum<'mcx>, bool)>> {
    let desc = rel.rd_att_clone_in(mcx)?;
    heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)
}

/// Internal workhorse for changing a subscription owner
/// (`subscriptioncmds.c:1969-2024`).
fn AlterSubscriptionOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<()> {
    let cols = deform(mcx, rel, tup)?;
    let form_oid = cols[(Anum_pg_subscription_oid - 1) as usize].0.as_oid();
    let form_subowner = cols[(Anum_pg_subscription_subowner - 1) as usize].0.as_oid();
    let form_subpasswordrequired =
        cols[(Anum_pg_subscription_subpasswordrequired - 1) as usize].0.as_bool();
    let subname = name_str(cols[(Anum_pg_subscription_subname - 1) as usize].0.as_ref_bytes())
        .to_string();

    if form_subowner == new_owner_id {
        return Ok(());
    }

    if !object_ownercheck(mcx, SubscriptionRelationId, form_oid, GetUserId())? {
        return Err(aclcheck_error_str(
            ACLCHECK_NOT_OWNER,
            ObjectType::Subscription,
            &subname,
        ));
    }

    /*
     * Don't allow non-superuser modification of a subscription with
     * password_required=false.
     */
    if !form_subpasswordrequired && !superuser_seams::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("password_required=false is superuser-only")
            .errhint(
                "Subscriptions with the password_required option set to false may only be created or modified by the superuser.",
            )
            .into_error());
    }

    /* Must be able to become new owner */
    acl_seams::check_can_set_role::call(GetUserId(), new_owner_id)?;

    /*
     * current owner must have CREATE on database
     *
     * This is consistent with how ALTER SCHEMA ... OWNER TO works, but some
     * other object types behave differently (e.g. you can't give a table to a
     * user who lacks CREATE privileges on a schema).
     */
    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let aclresult =
        object_aclcheck(mcx, DATABASE_RELATION_ID, my_database_id, GetUserId(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let dbname = dbcommands_seams::get_database_name::call(mcx, my_database_id)?;
        return Err(aclcheck_error_str(
            aclresult,
            ObjectType::Database,
            dbname.as_deref().unwrap_or(""),
        ));
    }

    /* form->subowner = newOwnerId; CatalogTupleUpdate(rel, &tup->t_self, tup); */
    let mut repl_values: [Datum<'mcx>; Natts_pg_subscription] =
        core::array::from_fn(|_| Datum::null());
    let repl_nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    let idx = (Anum_pg_subscription_subowner - 1) as usize;
    repl_values[idx] = Datum::from_oid(new_owner_id);
    replaces[idx] = true;

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut newtup = heap_modify_tuple(mcx, tup, &tupdesc, &repl_values, &repl_nulls, &replaces)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, rel, otid, &mut newtup)?;

    /* Update owner dependency reference */
    changeDependencyOnOwner(SubscriptionRelationId, form_oid, new_owner_id)?;

    objaccess::invoke_object_post_alter_hook::call(SubscriptionRelationId, form_oid, 0)?;

    /* Wake up related background processes to handle this change quickly. */
    launcher_seams::ApplyLauncherWakeupAtCommit::call();
    worker_seams::LogicalRepWorkersWakeupAtCommit::call(form_oid)?;

    Ok(())
}

/// Change subscription owner -- by name (`subscriptioncmds.c:2029-2060`).
pub fn AlterSubscriptionOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = backend_access_table_table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let tup = match SearchSysCache2(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(KeyDatum::from_oid(my_database_id)),
        SysCacheKey::Str(name),
    )? {
        Some(t) => t,
        None => {
            backend_access_table_table::table_close(rel, RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("subscription \"{name}\" does not exist"))
                .into_error());
        }
    };

    let cols = deform(mcx, &rel, &tup)?;
    let subid = cols[(Anum_pg_subscription_oid - 1) as usize].0.as_oid();

    AlterSubscriptionOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    let address = object_address_set(SubscriptionRelationId, subid);

    ReleaseSysCache(tup);

    backend_access_table_table::table_close(rel, RowExclusiveLock)?;

    Ok(address)
}

/// Change subscription owner -- by OID (`subscriptioncmds.c:2065-2085`).
pub fn AlterSubscriptionOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = backend_access_table_table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let tup = match SearchSysCache1(mcx, SUBSCRIPTIONOID, oid_cache_key(subid))? {
        Some(t) => t,
        None => {
            backend_access_table_table::table_close(rel, RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("subscription with OID {subid} does not exist"))
                .into_error());
        }
    };

    AlterSubscriptionOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    ReleaseSysCache(tup);

    backend_access_table_table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/// `OidIsValid(oid)`.
#[allow(dead_code)]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// Install this unit's two inward seams.
pub fn init_seams() {
    inward::init_seams()
}
