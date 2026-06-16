//! The pg_statistic maintenance of `catalog/heap.c`: `RemoveStatistics`.
//!
//! `CopyStatistics` (heap.c) is NOT landed here: it scans `pg_statistic`,
//! `heap_copytuple`s each row, rewrites `starelid`, and re-inserts the modified
//! tuple via `CatalogTupleInsertWithInfo`. The typed catalog-write model has no
//! `pg_statistic` insert carrier (`catalog_tuple_insert*_pg_statistic`) nor a
//! generic "insert a column-mutated `FormedTuple`" path, so a faithful
//! `CopyStatistics` is blocked on a pg_statistic INSERT carrier keystone in
//! `backend-catalog-indexing`. It is left unported (no stub).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_access_common_scankey::ScanKeyInit;
use mcx::Mcx;
use types_core::fmgr::{F_INT2EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::backend_access_common_heaptuple::Datum;

/* genbki catalog + index OIDs (catalog/pg_statistic.h, catalog/indexing.h). */
const StatisticRelationId: Oid = 2619;
const StatisticRelidAttnumInhIndexId: Oid = 2696;

/* pg_statistic attribute numbers (catalog/pg_statistic.h). */
const Anum_pg_statistic_starelid: AttrNumber = 1;
const Anum_pg_statistic_staattnum: AttrNumber = 2;

/*
 * RemoveStatistics --- remove entries in pg_statistic for a rel or column
 *
 * If attnum is zero, remove all entries for rel; else remove only the one(s)
 * for that column.
 */
pub fn RemoveStatistics<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<()> {
    let pgstatistic = backend_access_table_table::table_open(
        mcx,
        StatisticRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    let mut key = [ScanKeyData::empty(), ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_statistic_starelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let nkeys = if attnum == 0 {
        1
    } else {
        ScanKeyInit(
            &mut key[1],
            Anum_pg_statistic_staattnum,
            BTEqualStrategyNumber,
            F_INT2EQ,
            Datum::from_i16(attnum),
        )?;
        2
    };

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &pgstatistic,
        StatisticRelidAttnumInhIndexId,
        true,
        None,
        &key[..nkeys],
    )?;

    /* we must loop even when attnum != 0, in case of inherited stats */
    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };
        backend_catalog_indexing_seams::catalog_tuple_delete::call(&pgstatistic, tuple.tuple.t_self)?;
    }

    scan.end()?;
    pgstatistic.close(types_storage::lock::RowExclusiveLock)
}
