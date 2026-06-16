//! `src/backend/catalog/pg_range.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_range` relation.
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the
//! `backend-catalog-pg-constraint` precedent: `table_open`/`close` guard the
//! relation, the tuple build + insert crosses the `catalog_tuple_insert_pg_range`
//! heapam seam, `RangeDelete` scans `RangeTypidIndexId` and deletes each match
//! via the generic `catalog_tuple_delete` seam, and dependency recording goes
//! through the dependency / pg_depend seams.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};

use types_catalog::catalog::{
    COLLATION_RELATION_ID, OPERATOR_CLASS_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL};
use types_catalog::pg_range::{
    Anum_pg_range_rngtypid, PgRangeInsertRow, RangeRelationId, RangeTypidIndexId,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{Oid, OidIsValid};
use types_error::PgResult;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;

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

/// `ObjectAddressSet(addr, class, object)` (`catalog/objectaddress.h`).
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/* ===========================================================================
 * RangeCreate (pg_range.c:34-103)
 * ========================================================================= */

/// RangeCreate — create an entry in pg_range.
pub fn RangeCreate(
    mcx: Mcx<'_>,
    rangeTypeOid: Oid,
    rangeSubType: Oid,
    rangeCollation: Oid,
    rangeSubOpclass: Oid,
    rangeCanonical: Oid,
    rangeSubDiff: Oid,
    multirangeTypeOid: Oid,
) -> PgResult<()> {
    let range_ctx = MemoryContext::new("pg_range");
    let pg_range = table::table_open(range_ctx.mcx(), RangeRelationId, RowExclusiveLock)?;

    // values[...] = ...;  tup = heap_form_tuple(...);  CatalogTupleInsert(...);
    let row = PgRangeInsertRow {
        rngtypid: rangeTypeOid,
        rngsubtype: rangeSubType,
        rngmultitypid: multirangeTypeOid,
        rngcollation: rangeCollation,
        rngsubopc: rangeSubOpclass,
        rngcanonical: rangeCanonical,
        rngsubdiff: rangeSubDiff,
    };
    indexing_seams::catalog_tuple_insert_pg_range::call(range_ctx.mcx(), &pg_range, &row)?;

    /* record type's dependencies on range-related items */
    let mut addrs = dependency_seams::new_object_addresses::call()?;

    // ObjectAddressSet(myself, TypeRelationId, rangeTypeOid);
    let myself = ObjectAddressSet(TYPE_RELATION_ID, rangeTypeOid);

    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, rangeSubType),
        &mut addrs,
    )?;

    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(OPERATOR_CLASS_RELATION_ID, rangeSubOpclass),
        &mut addrs,
    )?;

    if OidIsValid(rangeCollation) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(COLLATION_RELATION_ID, rangeCollation),
            &mut addrs,
        )?;
    }

    if OidIsValid(rangeCanonical) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, rangeCanonical),
            &mut addrs,
        )?;
    }

    if OidIsValid(rangeSubDiff) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, rangeSubDiff),
            &mut addrs,
        )?;
    }

    dependency_seams::record_object_address_dependencies::call(myself, &mut addrs, DEPENDENCY_NORMAL)?;
    dependency_seams::free_object_addresses::call(addrs)?;

    /* record multirange type's dependency on the range type */
    let referencing = ObjectAddressSet(TYPE_RELATION_ID, multirangeTypeOid);
    pg_depend_seams::recordDependencyOn::call(mcx, &referencing, &myself, DEPENDENCY_INTERNAL)?;

    pg_range.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * RangeDelete (pg_range.c:109-138)
 * ========================================================================= */

/// RangeDelete — remove the pg_range entry for the specified type.
pub fn RangeDelete(rangeTypeOid: Oid) -> PgResult<()> {
    let range_ctx = MemoryContext::new("pg_range");
    let pg_range = table::table_open(range_ctx.mcx(), RangeRelationId, RowExclusiveLock)?;

    let key = [oid_key(Anum_pg_range_rngtypid, rangeTypeOid)?];

    let mut scan =
        genam_seams::systable_beginscan::call(&pg_range, RangeTypidIndexId, true, None, &key)?;
    loop {
        let scratch = MemoryContext::new("pg_range delete scan row");
        let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? else {
            break;
        };
        // CatalogTupleDelete(pg_range, &tup->t_self);
        indexing_seams::catalog_tuple_delete::call(&pg_range, tup.tuple.t_self)?;
    }
    scan.end()?;

    pg_range.close(RowExclusiveLock)?;

    Ok(())
}

/// `pg_range.c` owns no inward seam — `RangeCreate`/`RangeDelete` are invoked
/// directly by the (unported) DDL command code, not across a cycle. The empty
/// body keeps the crate uniform with its siblings.
pub fn init_seams() {}
