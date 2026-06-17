#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `commands/user.c`'s `pg_authid` / `pg_auth_members` catalog-write
//! orchestration owner.
//!
//! The role-DDL spine (`backend-commands-user`) crosses the
//! `backend-commands-user-seams` catalog-mutation contracts (`insert_authid`,
//! `update_authid`, `delete_authid`, `rename_authid`, `insert_authmem`,
//! `update_authmem_by_oid`, `delete_authmem_by_oid`, the two by-column delete
//! scans, and `get_new_oid_with_index`). This crate installs them: it re-opens
//! the catalog by the OID identity the user seam's `table_open` returned, runs
//! the value-layer `heap_form_tuple`/`heap_modify_tuple` +
//! `CatalogTuple{Insert,Update,Delete}` machinery through
//! `backend-catalog-indexing`, and records / removes the shared dependencies
//! through `backend-catalog-pg-shdepend` (`updateAclDependencies` after a
//! membership insert, `deleteSharedDependencyRecordsFor` before each silent
//! membership removal).

use mcx::{Mcx, MemoryContext};
use types_authid::{AuthIdUpdate, AuthMemUpdate, NewAuthMemRecord, NewAuthRecord};
use types_catalog::pg_authid as pa;
use types_core::primitive::{InvalidOid, Oid};
use types_error::PgResult;
use types_storage::lock::RowExclusiveLock;

use backend_access_table_table::table_open;
use backend_catalog_indexing_seams as idx;
use backend_catalog_pg_shdepend_seams as shdep;
use backend_commands_user_seams as user;

/// `AuthIdRelationId` / `AuthMemRelationId` (the user seam's `table_open` rel
/// OID) re-opened in `mcx` at `RowExclusiveLock` — the lock the user.c
/// `table_open(...)` already holds (re-entrant; released at transaction end).
fn open<'mcx>(mcx: Mcx<'mcx>, rel: Oid) -> PgResult<types_rel::Relation<'mcx>> {
    table_open(mcx, rel, RowExclusiveLock)
}

/// `GetNewOidWithIndex(rel, indexId, oidColumn)` dispatched by the catalog's OID
/// index (`AuthIdOidIndexId` ⇒ pg_authid, `AuthMemOidIndexId` ⇒ pg_auth_members).
fn get_new_oid_with_index(rel: Oid, index_id: Oid) -> PgResult<Oid> {
    let ctx = MemoryContext::new("get_new_oid_with_index_authid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    if index_id == pa::AuthIdOidIndexId {
        idx::get_new_oid_with_index_pg_authid::call(&r)
    } else if index_id == pa::AuthMemOidIndexId {
        idx::get_new_oid_with_index_pg_auth_members::call(&r)
    } else {
        Err(types_error::PgError::error(
            "get_new_oid_with_index: unexpected index for role catalog",
        ))
    }
}

fn insert_authid(rel: Oid, rec: NewAuthRecord) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_authid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    idx::catalog_tuple_insert_pg_authid::call(&r, &rec)
}

fn update_authid(rel: Oid, roleid: Oid, upd: AuthIdUpdate) -> PgResult<()> {
    let ctx = MemoryContext::new("update_authid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    idx::catalog_tuple_update_pg_authid::call(&r, roleid, &upd)
}

fn delete_authid(rel: Oid, roleid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_authid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    idx::delete_tuple_pg_authid::call(&r, roleid)
}

fn rename_authid(rel: Oid, roleid: Oid, newname: String, clear_md5: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("rename_authid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    idx::rename_tuple_pg_authid::call(&r, roleid, &newname, clear_md5)
}

/// AddRoleMems insert path: `CatalogTupleInsert` then
/// `updateAclDependencies(AuthMemRelationId, objectId, 0, InvalidOid, 0, NULL,
/// 1, {grantorId})`.
fn insert_authmem(rel: Oid, rec: NewAuthMemRecord) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_authmem");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    let object_id = rec.oid;
    let grantor = rec.grantor;
    idx::catalog_tuple_insert_pg_auth_members::call(&r, &rec)?;

    let oldmembers = mcx::vec_with_capacity_in::<Oid>(mcx, 0)?;
    let mut newmembers = mcx::vec_with_capacity_in::<Oid>(mcx, 1)?;
    newmembers.push(grantor);
    shdep::updateAclDependencies::call(
        mcx,
        pa::AuthMemRelationId,
        object_id,
        0,
        InvalidOid,
        oldmembers,
        newmembers,
    )
}

fn update_authmem_by_oid(rel: Oid, authmem_oid: Oid, upd: AuthMemUpdate) -> PgResult<()> {
    let ctx = MemoryContext::new("update_authmem_by_oid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    idx::catalog_tuple_update_pg_auth_members::call(&r, authmem_oid, &upd)
}

/// DelRoleMems delete path: `deleteSharedDependencyRecordsFor(AuthMemRelationId,
/// authmem_oid, 0)` then `CatalogTupleDelete`.
fn delete_authmem_by_oid(rel: Oid, authmem_oid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_authmem_by_oid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    shdep::deleteSharedDependencyRecordsFor::call(pa::AuthMemRelationId, authmem_oid, 0)?;
    idx::delete_tuple_pg_auth_members::call(&r, authmem_oid)
}

/// DropRole's first silent-removal scan: every `pg_auth_members` row with
/// `roleid == role` → `deleteSharedDependencyRecordsFor` + `CatalogTupleDelete`.
/// Returns the count removed.
fn delete_authmem_by_roleid(rel: Oid, roleid: Oid) -> PgResult<usize> {
    let ctx = MemoryContext::new("delete_authmem_by_roleid");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    let oids = idx::authmem_oids_by_roleid::call(&r, roleid)?;
    for oid in &oids {
        shdep::deleteSharedDependencyRecordsFor::call(pa::AuthMemRelationId, *oid, 0)?;
        idx::delete_tuple_pg_auth_members::call(&r, *oid)?;
    }
    Ok(oids.len())
}

/// DropRole's second silent-removal scan: every `pg_auth_members` row with
/// `member == role`.
fn delete_authmem_by_member(rel: Oid, memberid: Oid) -> PgResult<usize> {
    let ctx = MemoryContext::new("delete_authmem_by_member");
    let mcx = ctx.mcx();
    let r = open(mcx, rel)?;
    let oids = idx::authmem_oids_by_member::call(&r, memberid)?;
    for oid in &oids {
        shdep::deleteSharedDependencyRecordsFor::call(pa::AuthMemRelationId, *oid, 0)?;
        idx::delete_tuple_pg_auth_members::call(&r, *oid)?;
    }
    Ok(oids.len())
}

/// Install the `commands/user.c` catalog-mutation seams this crate owns.
pub fn init_seams() {
    user::get_new_oid_with_index::set(get_new_oid_with_index);
    user::insert_authid::set(insert_authid);
    user::update_authid::set(update_authid);
    user::delete_authid::set(delete_authid);
    user::rename_authid::set(rename_authid);
    user::insert_authmem::set(insert_authmem);
    user::update_authmem_by_oid::set(update_authmem_by_oid);
    user::delete_authmem_by_oid::set(delete_authmem_by_oid);
    user::delete_authmem_by_roleid::set(delete_authmem_by_roleid);
    user::delete_authmem_by_member::set(delete_authmem_by_member);
}
