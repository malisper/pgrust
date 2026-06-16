//! The catalog-row delete family of `catalog/heap.c`: `DeleteRelationTuple`,
//! `DeleteAttributeTuples`, `DeleteSystemAttributeTuples`, plus the
//! `RemoveAttributeById` STOP delegation.

#![allow(non_snake_case)]

extern crate alloc;

use backend_access_common_scankey::ScanKeyInit;
use backend_utils_error::elog;
use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid};
use types_core::fmgr::F_OIDEQ;
use types_error::{PgResult, ERROR};
use types_scan::scankey::{
    ScanKeyData, BTEqualStrategyNumber, BTLessEqualStrategyNumber,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::{
    AttributeRelationId, InheritsRelationId, RelationRelationId,
};

/* genbki index OIDs the scans use. */
const AttributeRelidNumIndexId: Oid = 2659;

/* pg_attribute attribute numbers (genbki). */
const Anum_pg_attribute_attrelid: AttrNumber = 1;
const Anum_pg_attribute_attnum: AttrNumber = 6;
/* pg_inherits attribute number. */
const Anum_pg_inherits_inhrelid: AttrNumber = 1;

/* int2le builtin (utils/fmgroids.h). */
const F_INT2LE: types_core::primitive::RegProcedure = 148;

/*
 *		DeleteRelationTuple
 *
 * Remove pg_class row for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
pub fn DeleteRelationTuple<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    /* Grab an appropriate lock on the pg_class relation */
    let pg_class_desc = backend_access_table_table::table_open(
        mcx,
        RelationRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    let Some((tid, _form)) =
        backend_utils_cache_syscache_seams::search_syscache_copy_pg_class::call(mcx, relid)?
    else {
        pg_class_desc.close(types_storage::lock::RowExclusiveLock)?;
        return elog(ERROR, &format!("cache lookup failed for relation {relid}"));
    };

    /* delete the relation tuple from pg_class, and finish up */
    backend_catalog_indexing_seams::catalog_tuple_delete::call(&pg_class_desc, tid)?;

    pg_class_desc.close(types_storage::lock::RowExclusiveLock)
}

/*
 *		DeleteAttributeTuples
 *
 * Remove pg_attribute rows for the given relid.
 */
pub fn DeleteAttributeTuples<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    /* Grab an appropriate lock on the pg_attribute relation */
    let attrel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* Use the index to scan only attributes of the target relation */
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attrel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key,
    )?;

    /* Delete all the matching tuples */
    loop {
        let Some(atttup) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };
        backend_catalog_indexing_seams::catalog_tuple_delete::call(&attrel, atttup.tuple.t_self)?;
    }

    /* Clean up after the scan */
    scan.end()?;
    attrel.close(types_storage::lock::RowExclusiveLock)
}

/*
 *		DeleteSystemAttributeTuples
 *
 * Remove pg_attribute rows for system columns of the given relid.
 *
 * Note: this is only used when converting a table to a view.  Views don't
 * have system columns, so we should remove them from pg_attribute.
 */
pub fn DeleteSystemAttributeTuples<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    /* Grab an appropriate lock on the pg_attribute relation */
    let attrel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* Use the index to scan only system attributes of the target relation */
    let mut key = [ScanKeyData::empty(), ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;
    ScanKeyInit(
        &mut key[1],
        Anum_pg_attribute_attnum,
        BTLessEqualStrategyNumber,
        F_INT2LE,
        Datum::from_i16(0),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attrel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key,
    )?;

    /* Delete all the matching tuples */
    loop {
        let Some(atttup) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };
        backend_catalog_indexing_seams::catalog_tuple_delete::call(&attrel, atttup.tuple.t_self)?;
    }

    /* Clean up after the scan */
    scan.end()?;
    attrel.close(types_storage::lock::RowExclusiveLock)
}

/*
 *		RelationRemoveInheritance
 *
 * Remove any pg_inherits rows linking this relation to its parent(s). Used by
 * `heap_drop_with_catalog`. By the time we get here there are no children.
 */
pub fn RelationRemoveInheritance<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    const InheritsRelidSeqnoIndexId: Oid = 2680;

    let catalog_relation = backend_access_table_table::table_open(
        mcx,
        InheritsRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &catalog_relation,
        InheritsRelidSeqnoIndexId,
        true,
        None,
        &key,
    )?;

    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };
        backend_catalog_indexing_seams::catalog_tuple_delete::call(
            &catalog_relation,
            tuple.tuple.t_self,
        )?;
    }

    scan.end()?;
    catalog_relation.close(types_storage::lock::RowExclusiveLock)
}

/*
 *		RemoveAttributeById
 *
 * This is the guts of ALTER TABLE DROP COLUMN: actually mark the attribute
 * deleted in pg_attribute.  We also remove pg_statistic entries for it.
 *
 * STOP — deeper-keystone-blocked. Faithful porting needs a `SearchSysCacheCopy2(
 * ATTNUM)` returning a *writable full* `pg_attribute` row + its `t_self`, a
 * `CatalogTupleUpdate` seam for pg_attribute, and `RemoveStatistics(relid,
 * attnum)`. None of these primitives exist yet (the syscache crate exposes only
 * narrow `pg_attribute` projections; there is no full-row ATTNUM copy seam or
 * pg_attribute `catalog_tuple_update` seam, and `RemoveStatistics`-by-relid has
 * no owner crate). The inward `RemoveAttributeById` seam (backend-catalog-heap-
 * seams) is therefore left declared-but-uninstalled (a loud panic = the
 * mirror-and-panic posture), not stubbed here.
 */
