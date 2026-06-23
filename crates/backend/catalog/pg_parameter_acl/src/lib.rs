//! `backend-catalog-pg-parameter-acl` — routines to support manipulation of the
//! `pg_parameter_acl` relation (`backend/catalog/pg_parameter_acl.c`).
//!
//! Two routines:
//!   * [`ParameterAclLookup`] — given a configuration parameter name, look up
//!     the associated `pg_parameter_acl` row's OID (canonicalizing the name via
//!     `convert_GUC_name_for_parameter_acl`, then a `PARAMETERACLNAME` syscache
//!     OID probe). With `missing_ok = false` a miss raises
//!     `ERRCODE_UNDEFINED_OBJECT`.
//!   * [`ParameterAclCreate`] — add a new `pg_parameter_acl` tuple with a null
//!     ACL for the named parameter, returning the new OID. Validates the name
//!     (`check_GUC_name_for_parameter_acl`), canonicalizes it, then
//!     `GetNewOidWithIndex` → `heap_form_tuple` → `CatalogTupleInsert` (the
//!     `catalog/indexing.c` carrier precedent shared with pg_collation /
//!     pg_database). It takes only `RowExclusiveLock`, relying on the unique
//!     index — not a duplicate probe — to reject collisions, exactly as the C
//!     does.
//!
//! The inward seams this crate installs are `parameter_acl_lookup` and
//! `parameter_acl_create` (declared in `backend-catalog-pg-parameter-acl-seams`,
//! consumed by `get_object_address`'s `OBJECT_PARAMETER_ACL` arm and aclchk.c's
//! parameter-ACL GRANT/REVOKE path). The seams thread no `Mcx`, so each handler
//! runs its work in a scratch `MemoryContext` it owns for the call (the
//! pg_collation idiom) — the OID it returns is a scalar the caller keeps.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// PgResult's PgError variant is large; boxing it would diverge from the rest of
// the workspace's vocabulary and from the C (which throws by value).
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};
use ::types_catalog::pg_parameter_acl as cat;
use ::types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use ::types_storage::lock::{NoLock, RowExclusiveLock};
use types_tuple::heaptuple::Datum;

use ::utils_error::ereport;
use ::types_error::pg_error::ErrorLocation;
use types_error::{PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};

use ::heaptuple::heap_form_tuple;
use ::catalog_catalog::GetNewOidWithIndex;
use ::indexing::keystone::CatalogTupleInsert;
use cache_syscache::{GetSysCacheOid, PARAMETERACLNAME};
use ::misc_guc::convert_guc_name_for_parameter_acl;
use ::cache::syscache::SysCacheKey;

use table_seams as table_seams;
use pg_parameter_acl_seams::{parameter_acl_create, parameter_acl_lookup};
use varlena_seams as varlena_seams;
use guc_seams as guc_seams;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module. The C line
/// number is not tracked (it is `0`).
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/catalog/pg_parameter_acl.c", 0, funcname)
}

/// `CStringGetTextDatum(s)` — build a `text` varlena `Datum` (varlena.c).
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// ParameterAclLookup (pg_parameter_acl.c)
///
/// Given a configuration parameter name, look up the associated configuration
/// parameter ACL's OID.
///
/// If `missing_ok` is false, throw an error if the ACL entry is not found. If
/// true, just return `InvalidOid`.
pub fn ParameterAclLookup(mcx: Mcx<'_>, parameter: &str, missing_ok: bool) -> PgResult<Oid> {
    /* Convert name to the form it should have in pg_parameter_acl... */
    let parname = convert_guc_name_for_parameter_acl(parameter);

    /* ... and look it up */
    // GetSysCacheOid1(PARAMETERACLNAME, Anum_pg_parameter_acl_oid,
    //                 PointerGetDatum(cstring_to_text(parname)))
    // The PARAMETERACLNAME cache keys on the parname text column; the syscache
    // takes the bare string key (CStringGetTextDatum is applied internally).
    let oid = GetSysCacheOid(
        mcx,
        PARAMETERACLNAME,
        cat::Anum_pg_parameter_acl_oid as AttrNumber,
        SysCacheKey::Str(&parname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;

    if !OidIsValid(oid) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("parameter ACL \"{parameter}\" does not exist"))
            .into_error());
    }

    /* pfree(parname): the owned `String` is dropped at end of scope. */

    Ok(oid)
}

/// ParameterAclCreate (pg_parameter_acl.c)
///
/// Add a new tuple to pg_parameter_acl.
///
/// `parameter`: the parameter name to create an entry for. Caller should have
/// verified that there's no such entry already.
///
/// Returns the new entry's OID.
pub fn ParameterAclCreate(mcx: Mcx<'_>, parameter: &str) -> PgResult<Oid> {
    /*
     * To prevent cluttering pg_parameter_acl with useless entries, insist that
     * the name be valid.
     */
    guc_seams::check_guc_name_for_parameter_acl::call(parameter)?;

    /* Convert name to the form it should have in pg_parameter_acl. */
    let parname = convert_guc_name_for_parameter_acl(parameter);

    /*
     * Create and insert a new record containing a null ACL.
     *
     * We don't take a strong enough lock to prevent concurrent insertions,
     * relying instead on the unique index.
     */
    let rel = table_seams::table_open::call(mcx, cat::ParameterAclRelationId, RowExclusiveLock)?;
    let tupdesc = rel.rd_att_clone_in(mcx)?;

    // Datum values[Natts_pg_parameter_acl] = {0};
    // bool  nulls[Natts_pg_parameter_acl]  = {0};
    let mut values: [Datum<'_>; cat::Natts_pg_parameter_acl] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; cat::Natts_pg_parameter_acl];
    let idx = |attno: i32| (attno - 1) as usize;

    let parameterId = GetNewOidWithIndex(
        &rel,
        cat::ParameterAclOidIndexId,
        cat::Anum_pg_parameter_acl_oid as AttrNumber,
    )?;
    values[idx(cat::Anum_pg_parameter_acl_oid)] = Datum::from_oid(parameterId);
    values[idx(cat::Anum_pg_parameter_acl_parname)] = text_datum(mcx, &parname)?;
    nulls[idx(cat::Anum_pg_parameter_acl_paracl)] = true;

    let mut tuple = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    /* Close pg_parameter_acl, but keep lock till commit. */
    /* heap_freetuple(tuple): the formed tuple is dropped at end of scope. */
    drop(tuple);
    rel.close(NoLock)?;

    Ok(parameterId)
}

/// The inward `parameter_acl_lookup` seam handler. The seam threads no `Mcx`, so
/// the handler runs the syscache probe in a scratch `MemoryContext` it owns for
/// the call; the resulting OID is a scalar the caller keeps.
fn parameter_acl_lookup_handler(parameter: &str, missing_ok: bool) -> PgResult<Oid> {
    let scratch = MemoryContext::new("ParameterAclLookup");
    ParameterAclLookup(scratch.mcx(), parameter, missing_ok)
}

/// The inward `parameter_acl_create` seam handler. Runs the tuple-forming +
/// catalog-mutation work in a scratch `MemoryContext`; returns the new OID.
fn parameter_acl_create_handler(parameter: &str) -> PgResult<Oid> {
    let scratch = MemoryContext::new("ParameterAclCreate");
    ParameterAclCreate(scratch.mcx(), parameter)
}

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    parameter_acl_lookup::set(parameter_acl_lookup_handler);
    parameter_acl_create::set(parameter_acl_create_handler);
}
