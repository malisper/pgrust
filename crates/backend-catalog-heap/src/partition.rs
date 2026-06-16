//! The partition catalog maintenance of `catalog/heap.c`:
//! `RemovePartitionKeyByRelId`.
//!
//! `StorePartitionKey` / `StorePartitionBound` are NOT landed here. They form
//! and insert `pg_partitioned_table` rows / rewrite the `pg_class.relpartbound`
//! column from `int2vector`/`oidvector` (`buildint2vector`/`buildoidvector`,
//! still private to `backend-catalog-indexing`) and a `PartitionBoundSpec`
//! `pg_node_tree`, and depend on a `pg_partitioned_table` INSERT carrier and a
//! `pg_class.relpartbound` UPDATE carrier that the typed catalog-write model
//! has not assembled. They remain unported (no stub).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid};
use types_core::fmgr::F_OIDEQ;
use types_error::PgResult;
use backend_access_common_scankey::ScanKeyInit;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::backend_access_common_heaptuple::Datum;

/* genbki catalog + index OIDs (catalog/pg_partitioned_table.h, indexing.h). */
const PartitionedRelationId: Oid = 3350;
const PartitionedRelidIndexId: Oid = 5040;

/* pg_partitioned_table attribute number (catalog/pg_partitioned_table.h). */
const Anum_pg_partitioned_table_partrelid: AttrNumber = 1;

/*
 *	RemovePartitionKeyByRelId
 *		Remove pg_partitioned_table entry for a relation
 *
 * The C reads the row through `SearchSysCache1(PARTRELID)` and deletes its
 * `t_self`. With no PARTRELID copy-with-TID syscache seam, the row's TID is
 * recovered by a keyed `systable_beginscan` on the partrelid index — the same
 * "scan-to-get-t_self-then-CatalogTupleDelete" shape as `DeleteRelationTuple`'s
 * sibling delete routines. The unique index yields at most one row; an empty
 * scan reproduces the C `!HeapTupleIsValid` `elog(ERROR)`.
 */
pub fn RemovePartitionKeyByRelId<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let rel = backend_access_table_table::table_open(
        mcx,
        PartitionedRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_partitioned_table_partrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &rel,
        PartitionedRelidIndexId,
        true,
        None,
        &key,
    )?;

    let tuple = backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?;
    let Some(tuple) = tuple else {
        scan.end()?;
        rel.close(types_storage::lock::RowExclusiveLock)?;
        return backend_utils_error::elog(
            types_error::ERROR,
            &format!("cache lookup failed for partition key of relation {relid}"),
        );
    };

    backend_catalog_indexing_seams::catalog_tuple_delete::call(&rel, tuple.tuple.t_self)?;

    scan.end()?;
    rel.close(types_storage::lock::RowExclusiveLock)
}
