//! `pg_authid` / `pg_auth_members` catalog-write value layer
//! (`commands/user.c`'s `heap_form_tuple`/`heap_modify_tuple` +
//! `CatalogTuple{Insert,Update,Delete}` machinery).
//!
//! The `commands/user.c` orchestration (in `backend-catalog-pg-authid`) opens
//! the relation by OID and supplies the value-typed row structs; these bodies
//! form the heap tuple against the relation's descriptor and run the catalog
//! mutation. Tuples located for update/delete are re-fetched by OID inside the
//! open transaction (the OID-keyed contract for the syscache-located rows the
//! C holds directly).

use mcx::{Mcx, MemoryContext};
use authid::{AuthIdUpdate, AuthMemUpdate, NewAuthMemRecord, NewAuthRecord};
use types_catalog::pg_authid as pa;
use types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use rel::{Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::{Datum, FormedTuple};

use heaptuple::{heap_deform_tuple, heap_form_tuple, heap_modify_tuple};
use scankey::ScanKeyInit;
use genam_seams as genam;
use table::table_open;

use crate::keystone::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};

/* ---- shared helpers (mirrors of family2's private helpers) ---- */

/// `namestrcpy(&name, src)` — a zero-filled 64-byte `NameData` image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    let mut name = [0u8; 64];
    for (i, &b) in src.as_bytes().iter().take(64).enumerate() {
        name[i] = b;
    }
    name[63] = 0;
    Ok(Datum::ByRef(mcx::slice_in(mcx, &name[..])?))
}

/// `CStringGetTextDatum(s)` — a `text` varlena image carried as `Datum::ByRef`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let payload = s.as_bytes();
    let total = 4 + payload.len();
    let word = (total as u32) << 2;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.extend_from_slice(&word.to_ne_bytes());
    buf.extend_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// `table_open(rel->rd_id, RowExclusiveLock)` — re-open the catalog in `mcx`.
fn reopen<'mcx>(mcx: Mcx<'mcx>, rel: &RelationData<'_>) -> PgResult<Relation<'mcx>> {
    table_open(mcx, rel.rd_id, RowExclusiveLock)
}

/// An OID-equality scan key on `attno`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(value))?;
    Ok(key)
}

/// Fetch the single catalog tuple whose `oidcol == oid` by an OID-keyed
/// `systable` heap scan (the genam fallback `index_ok = false` path).
fn fetch_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oidcol: i16,
    oid: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let keys = [oid_key(oidcol, oid)?];
    let mut scan = genam::systable_beginscan::call(rel, InvalidOid, false, None, &keys)?;
    let tup = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    scan.end()?;
    Ok(tup)
}

/// Deform every column of `tup` against `rel`'s descriptor.
fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<(Vec<Datum<'mcx>>, Vec<bool>)> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &tupdesc, &tup.data)?;
    let mut values = Vec::with_capacity(cols.len());
    let mut nulls = Vec::with_capacity(cols.len());
    for (v, n) in cols.iter() {
        values.push(v.clone());
        nulls.push(*n);
    }
    Ok((values, nulls))
}

fn set_col<'mcx>(
    values: &mut [Datum<'mcx>],
    nulls: &mut [bool],
    replaces: &mut [bool],
    anum: i16,
    datum: Datum<'mcx>,
) {
    let i = (anum - 1) as usize;
    values[i] = datum;
    nulls[i] = false;
    replaces[i] = true;
}

fn set_col_null(nulls: &mut [bool], replaces: &mut [bool], anum: i16) {
    let i = (anum - 1) as usize;
    nulls[i] = true;
    replaces[i] = true;
}

/// `heap_form_tuple(rel->rd_att, values, nulls)` + `CatalogTupleInsert`.
fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/// `heap_modify_tuple` + `CatalogTupleUpdate(rel, oldtup->t_self, newtup)`.
fn modify_and_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    replaces: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, values, nulls, replaces)?;
    CatalogTupleUpdate(mcx, rel, oldtup.tuple.t_self, &mut tup)
}

/* ======================================================================== *
 * pg_authid
 * ======================================================================== */

/// CreateRole: form the full `pg_authid` row from `rec` and `CatalogTupleInsert`.
fn insert_pg_authid(rel: &RelationData<'_>, rec: &NewAuthRecord) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_pg_authid");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    let mut values: Vec<Datum> = vec![Datum::default(); pa::Natts_pg_authid];
    let mut nulls: Vec<bool> = vec![false; pa::Natts_pg_authid];

    values[(pa::Anum_pg_authid_oid - 1) as usize] = Datum::from_oid(rec.oid);
    values[(pa::Anum_pg_authid_rolname - 1) as usize] = name_datum(mcx, &rec.rolname)?;
    values[(pa::Anum_pg_authid_rolsuper - 1) as usize] = Datum::from_bool(rec.rolsuper);
    values[(pa::Anum_pg_authid_rolinherit - 1) as usize] = Datum::from_bool(rec.rolinherit);
    values[(pa::Anum_pg_authid_rolcreaterole - 1) as usize] = Datum::from_bool(rec.rolcreaterole);
    values[(pa::Anum_pg_authid_rolcreatedb - 1) as usize] = Datum::from_bool(rec.rolcreatedb);
    values[(pa::Anum_pg_authid_rolcanlogin - 1) as usize] = Datum::from_bool(rec.rolcanlogin);
    values[(pa::Anum_pg_authid_rolreplication - 1) as usize] = Datum::from_bool(rec.rolreplication);
    values[(pa::Anum_pg_authid_rolbypassrls - 1) as usize] = Datum::from_bool(rec.rolbypassrls);
    values[(pa::Anum_pg_authid_rolconnlimit - 1) as usize] = Datum::from_i32(rec.rolconnlimit);

    match &rec.rolpassword {
        Some(pw) => {
            values[(pa::Anum_pg_authid_rolpassword - 1) as usize] = text_datum(mcx, pw)?;
        }
        None => nulls[(pa::Anum_pg_authid_rolpassword - 1) as usize] = true,
    }
    match rec.rolvaliduntil {
        Some(ts) => {
            values[(pa::Anum_pg_authid_rolvaliduntil - 1) as usize] = Datum::from_i64(ts);
        }
        None => nulls[(pa::Anum_pg_authid_rolvaliduntil - 1) as usize] = true,
    }

    form_and_insert(mcx, &r, &values, &nulls)
}

/// AlterRole: re-fetch the `pg_authid` row for `roleid`, apply the deltas,
/// `heap_modify_tuple` + `CatalogTupleUpdate`.
fn update_pg_authid(rel: &RelationData<'_>, roleid: Oid, upd: &AuthIdUpdate) -> PgResult<()> {
    let ctx = MemoryContext::new("update_pg_authid");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pa::Anum_pg_authid_oid, roleid)?
        .ok_or_else(|| PgError::error("cache lookup failed for role"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    if let Some(v) = upd.rolsuper {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolsuper, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolinherit {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolinherit, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolcreaterole {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolcreaterole, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolcreatedb {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolcreatedb, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolcanlogin {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolcanlogin, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolreplication {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolreplication, Datum::from_bool(v));
    }
    if let Some(v) = upd.rolconnlimit {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolconnlimit, Datum::from_i32(v));
    }
    if let Some(pw) = &upd.rolpassword {
        match pw {
            Some(p) => set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolpassword, text_datum(mcx, p)?),
            None => set_col_null(&mut nulls, &mut replaces, pa::Anum_pg_authid_rolpassword),
        }
    }
    if let Some(vu) = &upd.rolvaliduntil {
        match vu {
            Some(ts) => set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolvaliduntil, Datum::from_i64(*ts)),
            None => set_col_null(&mut nulls, &mut replaces, pa::Anum_pg_authid_rolvaliduntil),
        }
    }
    if let Some(v) = upd.rolbypassrls {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolbypassrls, Datum::from_bool(v));
    }

    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// RenameRole: re-fetch by `roleid`, write `rolname` (and clear `rolpassword`
/// when `clear_md5`), `CatalogTupleUpdate`.
fn rename_tuple_pg_authid(
    rel: &RelationData<'_>,
    roleid: Oid,
    newname: &str,
    clear_md5: bool,
) -> PgResult<()> {
    let ctx = MemoryContext::new("rename_tuple_pg_authid");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pa::Anum_pg_authid_oid, roleid)?
        .ok_or_else(|| PgError::error("cache lookup failed for role"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_authid_rolname, name_datum(mcx, newname)?);
    if clear_md5 {
        set_col_null(&mut nulls, &mut replaces, pa::Anum_pg_authid_rolpassword);
    }

    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// DropRole: re-fetch by `roleid`, `CatalogTupleDelete(rel, &tuple->t_self)`.
fn delete_tuple_pg_authid(rel: &RelationData<'_>, roleid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_tuple_pg_authid");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pa::Anum_pg_authid_oid, roleid)?
        .ok_or_else(|| PgError::error("cache lookup failed for role"))?;
    CatalogTupleDelete(mcx, &r, oldtup.tuple.t_self)
}

fn get_new_oid_with_index_pg_authid<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid> {
    catalog_catalog::GetNewOidWithIndex(rel, pa::AuthIdOidIndexId, pa::Anum_pg_authid_oid)
}

/* ======================================================================== *
 * pg_auth_members
 * ======================================================================== */

fn insert_pg_auth_members(rel: &RelationData<'_>, rec: &NewAuthMemRecord) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_pg_auth_members");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    let mut values: Vec<Datum> = vec![Datum::default(); pa::Natts_pg_auth_members];
    let nulls: Vec<bool> = vec![false; pa::Natts_pg_auth_members];

    values[(pa::Anum_pg_auth_members_oid - 1) as usize] = Datum::from_oid(rec.oid);
    values[(pa::Anum_pg_auth_members_roleid - 1) as usize] = Datum::from_oid(rec.roleid);
    values[(pa::Anum_pg_auth_members_member - 1) as usize] = Datum::from_oid(rec.member);
    values[(pa::Anum_pg_auth_members_grantor - 1) as usize] = Datum::from_oid(rec.grantor);
    values[(pa::Anum_pg_auth_members_admin_option - 1) as usize] = Datum::from_bool(rec.admin_option);
    values[(pa::Anum_pg_auth_members_inherit_option - 1) as usize] = Datum::from_bool(rec.inherit_option);
    values[(pa::Anum_pg_auth_members_set_option - 1) as usize] = Datum::from_bool(rec.set_option);

    form_and_insert(mcx, &r, &values, &nulls)
}

fn update_pg_auth_members(
    rel: &RelationData<'_>,
    authmem_oid: Oid,
    upd: &AuthMemUpdate,
) -> PgResult<()> {
    let ctx = MemoryContext::new("update_pg_auth_members");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pa::Anum_pg_auth_members_oid, authmem_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for pg_auth_members entry"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    if let Some(v) = upd.admin_option {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_auth_members_admin_option, Datum::from_bool(v));
    }
    if let Some(v) = upd.inherit_option {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_auth_members_inherit_option, Datum::from_bool(v));
    }
    if let Some(v) = upd.set_option {
        set_col(&mut values, &mut nulls, &mut replaces, pa::Anum_pg_auth_members_set_option, Datum::from_bool(v));
    }

    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn delete_tuple_pg_auth_members(rel: &RelationData<'_>, authmem_oid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_tuple_pg_auth_members");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pa::Anum_pg_auth_members_oid, authmem_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for pg_auth_members entry"))?;
    CatalogTupleDelete(mcx, &r, oldtup.tuple.t_self)
}

fn get_new_oid_with_index_pg_auth_members<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid> {
    catalog_catalog::GetNewOidWithIndex(
        rel,
        pa::AuthMemOidIndexId,
        pa::Anum_pg_auth_members_oid,
    )
}

/// `systable` scan over `pg_auth_members` keyed on `keycol == role`, returning
/// every matching row's `oid` (the `commands/user.c` DropRole silent-removal
/// scans, which then `deleteSharedDependencyRecordsFor` + `CatalogTupleDelete`
/// in the orchestration layer).
fn authmem_oids_by_col(rel: &RelationData<'_>, keycol: i16, role: Oid) -> PgResult<Vec<Oid>> {
    let ctx = MemoryContext::new("authmem_oids_by_col");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let keys = [oid_key(keycol, role)?];
    let mut scan = genam::systable_beginscan::call(&r, InvalidOid, false, None, &keys)?;
    let mut oids = Vec::new();
    while let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        let (values, nulls) = deform(mcx, &r, &tup)?;
        let i = (pa::Anum_pg_auth_members_oid - 1) as usize;
        if nulls[i] {
            scan.end()?;
            return Err(PgError::error("null oid in pg_auth_members"));
        }
        oids.push(values[i].as_oid());
    }
    scan.end()?;
    Ok(oids)
}

fn authmem_oids_by_roleid(rel: &RelationData<'_>, role: Oid) -> PgResult<Vec<Oid>> {
    authmem_oids_by_col(rel, pa::Anum_pg_auth_members_roleid, role)
}

fn authmem_oids_by_member(rel: &RelationData<'_>, role: Oid) -> PgResult<Vec<Oid>> {
    authmem_oids_by_col(rel, pa::Anum_pg_auth_members_member, role)
}

pub(crate) fn install() {
    use indexing_seams as s;
    s::get_new_oid_with_index_pg_authid::set(get_new_oid_with_index_pg_authid);
    s::get_new_oid_with_index_pg_auth_members::set(get_new_oid_with_index_pg_auth_members);
    s::catalog_tuple_insert_pg_authid::set(insert_pg_authid);
    s::catalog_tuple_update_pg_authid::set(update_pg_authid);
    s::rename_tuple_pg_authid::set(rename_tuple_pg_authid);
    s::delete_tuple_pg_authid::set(delete_tuple_pg_authid);
    s::catalog_tuple_insert_pg_auth_members::set(insert_pg_auth_members);
    s::catalog_tuple_update_pg_auth_members::set(update_pg_auth_members);
    s::delete_tuple_pg_auth_members::set(delete_tuple_pg_auth_members);
    s::authmem_oids_by_roleid::set(authmem_oids_by_roleid);
    s::authmem_oids_by_member::set(authmem_oids_by_member);
}
