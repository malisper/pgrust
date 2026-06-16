//! `src/backend/catalog/pg_cast.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_cast` relation.
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the
//! `backend-catalog-pg-constraint` precedent: `table_open`/`close` guard the
//! relation, the duplicate check is a `systable` scan on the
//! `CastSourceTargetIndexId` unique index (the C uses `SearchSysCache2(
//! CASTSOURCETARGET, …)` purely for a friendlier error — the unique index
//! catches it either way, per the C comment), the tuple build + insert crosses
//! the `catalog_tuple_insert_pg_cast` heapam seam, and dependency recording +
//! the post-create hook go through the dependency / objectaccess seams.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};

use types_catalog::catalog::{PROCEDURE_RELATION_ID, TYPE_RELATION_ID};
use types_catalog::catalog_dependency::{DependencyType, ObjectAddress};
use types_catalog::pg_cast::{
    Anum_pg_cast_castsource, Anum_pg_cast_casttarget, CastRelationId, CastSourceTargetIndexId,
    PgCastInsertRow,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;
use backend_utils_adt_format_type_seams as format_type_seams;

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ObjectAddressSet(addr, class, object)` (`catalog/objectaddress.h`) — set the
/// classId/objectId, with objectSubId = 0.
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/* ===========================================================================
 * CastCreate (pg_cast.c:48-138)
 * ========================================================================= */

/// CastCreate
///
/// Forms and inserts catalog tuples for a new cast being created. Caller must
/// have already checked privileges, and done consistency checks on the given
/// datatypes and cast function (if applicable).
///
/// Since we allow binary coercibility of the datatypes to the cast function's
/// input and result, there could be one or two `WITHOUT FUNCTION` casts that
/// this one depends on. We don't record that explicitly in pg_cast, but we
/// still need to make dependencies on those casts.
///
/// `behavior` indicates the types of the dependencies that the new cast will
/// have on its input and output types, the cast function, and the other casts
/// if any.
pub fn CastCreate(
    mcx: Mcx<'_>,
    sourcetypeid: Oid,
    targettypeid: Oid,
    funcid: Oid,
    incastid: Oid,
    outcastid: Oid,
    castcontext: i8,
    castmethod: i8,
    behavior: DependencyType,
) -> PgResult<ObjectAddress> {
    let cast_ctx = MemoryContext::new("pg_cast");
    let relation = table::table_open(cast_ctx.mcx(), CastRelationId, RowExclusiveLock)?;

    /*
     * Check for duplicate.  This is just to give a friendly error message, the
     * unique index would catch it anyway (so no need to sweat about race
     * conditions).
     *
     *   tuple = SearchSysCache2(CASTSOURCETARGET,
     *                           ObjectIdGetDatum(sourcetypeid),
     *                           ObjectIdGetDatum(targettypeid));
     *   if (HeapTupleIsValid(tuple)) ereport(ERROR, ...);
     */
    let skey = [
        oid_key(Anum_pg_cast_castsource, sourcetypeid)?,
        oid_key(Anum_pg_cast_casttarget, targettypeid)?,
    ];
    let mut found = false;
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &relation,
            CastSourceTargetIndexId,
            true,
            None,
            &skey,
        )?;
        let scratch = MemoryContext::new("pg_cast dup scan");
        if genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())?.is_some() {
            found = true;
        }
        scan.end()?;
    }
    if found {
        return Err(PgError::new(
            ERROR,
            format!(
                "cast from type {} to type {} already exists",
                format_type_seams::format_type_be::call(mcx, sourcetypeid)?.as_str(),
                format_type_seams::format_type_be::call(mcx, targettypeid)?.as_str(),
            ),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /* ready to go */
    // castid = GetNewOidWithIndex(relation, CastOidIndexId, Anum_pg_cast_oid);
    // values[...] = ...;  tuple = heap_form_tuple(...);  CatalogTupleInsert(...);
    let row = PgCastInsertRow {
        castsource: sourcetypeid,
        casttarget: targettypeid,
        castfunc: funcid,
        castcontext,
        castmethod,
    };
    let castid: Oid = indexing_seams::catalog_tuple_insert_pg_cast::call(cast_ctx.mcx(), &relation, &row)?;

    // addrs = new_object_addresses();
    let mut addrs = dependency_seams::new_object_addresses::call()?;

    /* make dependency entries */
    // ObjectAddressSet(myself, CastRelationId, castid);
    let myself = ObjectAddressSet(CastRelationId, castid);

    /* dependency on source type */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, sourcetypeid),
        &mut addrs,
    )?;

    /* dependency on target type */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, targettypeid),
        &mut addrs,
    )?;

    /* dependency on function */
    if OidIsValid(funcid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, funcid),
            &mut addrs,
        )?;
    }

    /* dependencies on casts required for function */
    if OidIsValid(incastid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(CastRelationId, incastid),
            &mut addrs,
        )?;
    }
    if OidIsValid(outcastid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(CastRelationId, outcastid),
            &mut addrs,
        )?;
    }

    // record_object_address_dependencies(&myself, addrs, behavior);
    dependency_seams::record_object_address_dependencies::call(myself, &mut addrs, behavior)?;
    // free_object_addresses(addrs);
    dependency_seams::free_object_addresses::call(addrs)?;

    /* dependency on extension */
    // recordDependencyOnCurrentExtension(&myself, false);
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new cast */
    // InvokeObjectPostCreateHook(CastRelationId, castid, 0);
    objectaccess_seams::invoke_object_post_create_hook::call(CastRelationId, castid, 0)?;

    // heap_freetuple(tuple);  (the formed tuple is owned by the insert seam)
    // table_close(relation, RowExclusiveLock);
    relation.close(RowExclusiveLock)?;

    Ok(myself)
}

/// `pg_cast.c` owns no inward seam — `CastCreate` is invoked directly by the
/// (unported) DDL command code, not across a cycle. The empty body keeps the
/// crate uniform with its siblings.
pub fn init_seams() {}
