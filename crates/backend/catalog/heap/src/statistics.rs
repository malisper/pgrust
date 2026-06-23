//! The pg_statistic maintenance of `catalog/heap.c`: `RemoveStatistics` and
//! `CopyStatistics`. `CopyStatistics` scans `pg_statistic`, `heap_copytuple`s
//! each row, rewrites `starelid` (the fixed-width column 1) via
//! `heap_modify_tuple`, and re-inserts the modified tuple through
//! `CatalogTupleInsertWithInfo` (the `catalog/indexing.c` keystone).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use alloc::vec;

use ::scankey::ScanKeyInit;
use ::mcx::Mcx;
use ::types_core::fmgr::{F_INT2EQ, F_OIDEQ};
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::heaptuple::Datum;

extern crate alloc;

/* genbki catalog + index OIDs (catalog/pg_statistic.h, catalog/indexing.h). */
const StatisticRelationId: Oid = 2619;
const StatisticRelidAttnumInhIndexId: Oid = 2696;

/* pg_statistic attribute numbers (catalog/pg_statistic.h). */
const Anum_pg_statistic_starelid: AttrNumber = 1;
const Anum_pg_statistic_staattnum: AttrNumber = 2;

/*
 * CopyStatistics --- copy entries in pg_statistic from one rel to another
 */
pub fn CopyStatistics<'mcx>(mcx: Mcx<'mcx>, fromrelid: Oid, torelid: Oid) -> PgResult<()> {
    let statrel = table::table_open(
        mcx,
        StatisticRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* Now search for stat records */
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_statistic_starelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(fromrelid),
    )?;

    let mut scan = genam_seams::systable_beginscan::call(
        &statrel,
        StatisticRelidAttnumInhIndexId,
        true,
        None,
        &key[..1],
    )?;

    // CatalogIndexState indstate = NULL; — opened lazily on the first row.
    let mut indstate: Option<
        indexing::keystone::CatalogIndexState<'mcx>,
    > = None;

    let natts = statrel.rd_att.natts as usize;
    loop {
        let Some(tup) =
            genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };

        /* make a modifiable copy and update the copy of the tuple */
        /* statform->starelid = torelid; — column 1 is the only one rewritten. */
        let mut repl_values = vec![Datum::ByVal(0); natts];
        let repl_isnull = vec![false; natts];
        let mut do_replace = vec![false; natts];
        repl_values[(Anum_pg_statistic_starelid - 1) as usize] = Datum::from_oid(torelid);
        do_replace[(Anum_pg_statistic_starelid - 1) as usize] = true;

        let mut newtup = heaptuple::heap_modify_tuple(
            mcx,
            &tup,
            &statrel.rd_att,
            &repl_values,
            &repl_isnull,
            &do_replace,
        )
        .map_err(|e| {
            utils_error::ereport(::types_error::ERROR)
                .errmsg_internal(format!("heap_modify_tuple failed in CopyStatistics: {e:?}"))
                .into_error()
        })?;

        /* fetch index information when we know we need it */
        if indstate.is_none() {
            indstate =
                Some(indexing::keystone::CatalogOpenIndexes(mcx, &statrel)?);
        }

        indexing::keystone::CatalogTupleInsertWithInfo(
            mcx,
            &statrel,
            &mut newtup,
            indstate.as_mut().unwrap(),
        )?;
    }

    scan.end()?;

    if let Some(indstate) = indstate {
        indexing::keystone::CatalogCloseIndexes(indstate)?;
    }
    statrel.close(types_storage::lock::RowExclusiveLock)
}

/*
 * RemoveStatistics --- remove entries in pg_statistic for a rel or column
 *
 * If attnum is zero, remove all entries for rel; else remove only the one(s)
 * for that column.
 */
pub fn RemoveStatistics<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<()> {
    let pgstatistic = table::table_open(
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

    let mut scan = genam_seams::systable_beginscan::call(
        &pgstatistic,
        StatisticRelidAttnumInhIndexId,
        true,
        None,
        &key[..nkeys],
    )?;

    /* we must loop even when attnum != 0, in case of inherited stats */
    loop {
        let Some(tuple) =
            genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };
        indexing_seams::catalog_tuple_delete::call(&pgstatistic, tuple.tuple.t_self)?;
    }

    scan.end()?;
    pgstatistic.close(types_storage::lock::RowExclusiveLock)
}
