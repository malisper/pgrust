//! The partition catalog maintenance of `catalog/heap.c`:
//! `RemovePartitionKeyByRelId` and `StorePartitionKey`.
//!
//! `StorePartitionKey` forms and inserts the `pg_partitioned_table` row (the
//! `partattrs`/`partclass`/`partcollation` `int2vector`/`oidvector` images and
//! the `partexprs` `pg_node_tree`), records the opclass/collation/column
//! dependencies, and invalidates the relcache. The `int2vector`/`oidvector`
//! and `text` varlena images are built inline (the same self-contained byte
//! layout `backend-catalog-indexing` uses, avoiding an adt-int/oid dependency).
//!
//! `StorePartitionBound` is NOT landed here: it rewrites `pg_class.relpartbound`
//! from a transformed `PartitionBoundSpec`, which depends on the partition-bound
//! transform/validation machinery (`transformPartitionBound` /
//! `check_new_partition_bound` in `partitioning/partbounds.c`) and the partition
//! descriptor (`partitioning/partdesc.c`), neither of which is ported yet.

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
const PartitionedRelidIndexId: Oid = 3351;

/* genbki catalog OIDs used for dependency recording (catalog/pg_*.h). */
const RelationRelationId: Oid = 1259;
const OperatorClassRelationId: Oid = 2616;
const CollationRelationId: Oid = 3456;
const DEFAULT_COLLATION_OID: Oid = 100;
const INT2OID: Oid = 21;
const OIDOID: Oid = 26;

/* pg_partitioned_table column count + attribute numbers
 * (catalog/pg_partitioned_table.h). */
const Natts_pg_partitioned_table: usize = 8;
const Anum_pg_partitioned_table_partrelid: AttrNumber = 1;
const Anum_pg_partitioned_table_partstrat: AttrNumber = 2;
const Anum_pg_partitioned_table_partnatts: AttrNumber = 3;
const Anum_pg_partitioned_table_partdefid: AttrNumber = 4;
const Anum_pg_partitioned_table_partattrs: AttrNumber = 5;
const Anum_pg_partitioned_table_partclass: AttrNumber = 6;
const Anum_pg_partitioned_table_partcollation: AttrNumber = 7;
const Anum_pg_partitioned_table_partexprs: AttrNumber = 8;

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

/// `buildint2vector(int2s, n)` (utils/adt/int.c): the on-disk `int2vector`
/// image — a varlena whose header (`vl_len_` via `SET_VARSIZE`, then `ndim=1`,
/// `dataoffset=0`, `elemtype=INT2OID`, `dim1=n`, `lbound1=0`) is followed by the
/// `n` `int16` values. Returned as the verbatim `Datum::ByRef` bytes (header
/// included), exactly what `heap_form_tuple` reads via `VARSIZE_ANY`. Built
/// inline (the same image `backend-catalog-indexing` builds for pg_index),
/// avoiding an adt-int crate dependency from catalog-heap.
fn buildint2vector<'mcx>(mcx: Mcx<'mcx>, int2s: &[i16]) -> PgResult<Datum<'mcx>> {
    const HEADER: usize = 24; // offsetof(int2vector, values)
    let n = int2s.len();
    let total = HEADER + n * core::mem::size_of::<i16>();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2; // SET_VARSIZE
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim = 1
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes()); // dataoffset = 0
    buf[12..16].copy_from_slice(&INT2OID.to_ne_bytes()); // elemtype
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes()); // dim1
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes()); // lbound1 = 0
    for (i, v) in int2s.iter().enumerate() {
        let off = HEADER + i * 2;
        buf[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `buildoidvector(oids, n)` (utils/adt/oid.c): the on-disk `oidvector` image
/// (the `int2vector`-shaped header, `elemtype=OIDOID`, then `n` `Oid` values).
fn buildoidvector<'mcx>(mcx: Mcx<'mcx>, oids: &[Oid]) -> PgResult<Datum<'mcx>> {
    const HEADER: usize = 24; // offsetof(oidvector, values)
    let n = oids.len();
    let total = HEADER + n * core::mem::size_of::<Oid>();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2; // SET_VARSIZE
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim = 1
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes()); // dataoffset = 0
    buf[12..16].copy_from_slice(&OIDOID.to_ne_bytes()); // elemtype
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes()); // dim1
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes()); // lbound1 = 0
    for (i, v) in oids.iter().enumerate() {
        let off = HEADER + i * 4;
        buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `CStringGetTextDatum(s)` (postgres.h → `cstring_to_text`): a `text` varlena
/// with the standard 4-byte header followed by the payload bytes.
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const VARHDRSZ: usize = 4;
    let payload = s.as_bytes();
    let total = VARHDRSZ + payload.len();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2; // SET_VARSIZE
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[VARHDRSZ..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/*
 *	StorePartitionKey
 *		Store information about the partition key rel into the catalog
 *
 * Faithful port of catalog/heap.c:StorePartitionKey. `partexprs` is the list of
 * partition-key expressions (each an `Expr` node), already `nodeToString`-able;
 * `None`/empty means the column-only case (SQL NULL `partexprs`).
 */
#[allow(clippy::too_many_arguments)]
pub fn StorePartitionKey<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    strategy: i8,
    partnatts: i16,
    partattrs: &[AttrNumber],
    partexprs: Option<&types_nodes::nodes::Node<'mcx>>,
    partopclass: &[Oid],
    partcollation: &[Oid],
) -> PgResult<()> {
    use types_catalog::catalog_dependency::{
        ObjectAddress, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
    };

    debug_assert_eq!(
        rel.rd_rel.relkind,
        types_tuple::access::RELKIND_PARTITIONED_TABLE
    );

    let relid = rel.rd_id;
    let n = partnatts as usize;

    /* Copy the partition attribute numbers, opclass / collation OIDs into the
     * on-disk vector images. */
    let partattrs_vec = buildint2vector(mcx, &partattrs[..n])?;
    let partopclass_vec = buildoidvector(mcx, &partopclass[..n])?;
    let partcollation_vec = buildoidvector(mcx, &partcollation[..n])?;

    /* Convert the expressions (if any) to a text datum. */
    let partexpr_datum: Option<Datum> = match partexprs {
        Some(node) => {
            let s = backend_nodes_outfuncs::nodeToString(mcx, node)?;
            Some(cstring_to_text_datum(mcx, s.as_str())?)
        }
        None => None,
    };

    let pg_partitioned_table = backend_access_table_table::table_open(
        mcx,
        PartitionedRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_partitioned_table];
    let mut nulls: Vec<bool> = vec![false; Natts_pg_partitioned_table];

    values[Anum_pg_partitioned_table_partrelid as usize - 1] = Datum::from_oid(relid);
    values[Anum_pg_partitioned_table_partstrat as usize - 1] = Datum::from_char(strategy);
    values[Anum_pg_partitioned_table_partnatts as usize - 1] = Datum::from_i16(partnatts);
    values[Anum_pg_partitioned_table_partdefid as usize - 1] =
        Datum::from_oid(types_core::primitive::InvalidOid);
    values[Anum_pg_partitioned_table_partattrs as usize - 1] = partattrs_vec;
    values[Anum_pg_partitioned_table_partclass as usize - 1] = partopclass_vec;
    values[Anum_pg_partitioned_table_partcollation as usize - 1] = partcollation_vec;
    match partexpr_datum {
        Some(d) => values[Anum_pg_partitioned_table_partexprs as usize - 1] = d,
        None => nulls[Anum_pg_partitioned_table_partexprs as usize - 1] = true,
    }

    let tupdesc = pg_partitioned_table.rd_att_clone_in(mcx)?;
    let mut tuple = backend_access_common_heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    backend_catalog_indexing::keystone::CatalogTupleInsert(mcx, &pg_partitioned_table, &mut tuple)?;
    pg_partitioned_table.close(types_storage::lock::RowExclusiveLock)?;

    /* Mark this relation as dependent on a few things as follows. */
    let myself = ObjectAddress {
        classId: RelationRelationId,
        objectId: relid,
        objectSubId: 0,
    };

    let mut addrs = backend_catalog_dependency::new_object_addresses();

    /* Operator class and collation per key column. */
    for i in 0..n {
        let referenced = ObjectAddress {
            classId: OperatorClassRelationId,
            objectId: partopclass[i],
            objectSubId: 0,
        };
        backend_catalog_dependency::add_exact_object_address(&referenced, &mut addrs);

        /* The default collation is pinned, so don't bother recording it. */
        if types_core::primitive::OidIsValid(partcollation[i])
            && partcollation[i] != DEFAULT_COLLATION_OID
        {
            let referenced = ObjectAddress {
                classId: CollationRelationId,
                objectId: partcollation[i],
                objectSubId: 0,
            };
            backend_catalog_dependency::add_exact_object_address(&referenced, &mut addrs);
        }
    }

    backend_catalog_dependency::record_object_address_dependencies(
        &myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;

    /* The partitioning columns are made internally dependent on the table. */
    for i in 0..n {
        if partattrs[i] == 0 {
            continue; /* ignore expressions here */
        }
        let referenced = ObjectAddress {
            classId: RelationRelationId,
            objectId: relid,
            objectSubId: partattrs[i] as i32,
        };
        backend_catalog_pg_depend::recordDependencyOn(
            mcx,
            &referenced,
            &myself,
            DEPENDENCY_INTERNAL,
        )?;
    }

    /* Anything mentioned in partition expressions. */
    if let Some(node) = partexprs {
        backend_catalog_dependency::recordDependencyOnSingleRelExpr(
            &myself,
            node,
            relid,
            DEPENDENCY_NORMAL,
            DEPENDENCY_INTERNAL,
            true, /* reverse the self-deps */
        )?;
    }

    /* Invalidate the relcache so the next CCI rebuilds the partition key. */
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(rel)?;

    Ok(())
}
