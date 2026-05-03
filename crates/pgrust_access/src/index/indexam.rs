use pgrust_catalog_data::{BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::{ColumnDesc, RelationDesc};

use crate::access::amapi::{
    IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult,
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::access::itemptr::ItemPointerData;
use crate::access::relscan::{IndexScanDesc, ScanDirection};
use crate::access::scankey::ScanKeyData;
use crate::access::tidbitmap::TidBitmap;
use crate::gist::GistBuildRowSource;
use crate::{
    AccessError, AccessHeapServices, AccessIndexServices, AccessInterruptServices, AccessResult,
    AccessScalarServices, AccessTransactionServices, AccessWalServices,
};

pub fn supports_index_am(am_oid: u32) -> bool {
    matches!(
        am_oid,
        BTREE_AM_OID | GIN_AM_OID | GIST_AM_OID | HASH_AM_OID | SPGIST_AM_OID
    )
}

fn unknown_index_am() -> AccessError {
    AccessError::Corrupt("unknown index access method")
}

fn materialize_heap_row_values(
    heap_desc: &RelationDesc,
    datums: &[Option<&[u8]>],
    scalar: &dyn AccessScalarServices,
) -> AccessResult<Vec<Value>> {
    let mut row_values = Vec::with_capacity(heap_desc.columns.len());
    for (index, column) in heap_desc.columns.iter().enumerate() {
        row_values.push(if let Some(datum) = datums.get(index) {
            scalar.decode_value(column, *datum)?
        } else {
            missing_column_value(column)
        });
    }
    Ok(row_values)
}

fn missing_column_value(column: &ColumnDesc) -> Value {
    if column.generated.is_some() {
        return Value::Null;
    }
    column.missing_default_value.clone().unwrap_or(Value::Null)
}

fn collect_projected_rows(
    ctx: &IndexBuildContext,
    heap: &dyn AccessHeapServices,
    index: &mut dyn AccessIndexServices,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<(u64, Vec<(ItemPointerData, Vec<Value>)>)> {
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut projected = Vec::new();
    let heap_tuples = heap.for_each_visible_heap_tuple(
        ctx.heap_relation,
        ctx.snapshot.clone(),
        &mut |tid, tuple| {
            let datums = tuple
                .deform(&attr_descs)
                .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
            let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums, scalar)?;
            if let Some(key_values) = index.project_index_row(&ctx.index_meta, &row_values, tid)? {
                projected.push((tid, key_values));
            }
            Ok(())
        },
    )?;
    Ok((heap_tuples, projected))
}

struct ProjectedBuildSource<'a> {
    ctx: &'a IndexBuildContext,
    heap: &'a dyn AccessHeapServices,
    index: &'a mut dyn AccessIndexServices,
    scalar: &'a dyn AccessScalarServices,
}

impl GistBuildRowSource for ProjectedBuildSource<'_> {
    fn for_each_projected(
        &mut self,
        visit: &mut dyn FnMut(ItemPointerData, Vec<Value>) -> AccessResult<()>,
    ) -> AccessResult<u64> {
        let attr_descs = self.ctx.heap_desc.attribute_descs();
        self.heap.for_each_visible_heap_tuple(
            self.ctx.heap_relation,
            self.ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple
                    .deform(&attr_descs)
                    .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
                let row_values =
                    materialize_heap_row_values(&self.ctx.heap_desc, &datums, self.scalar)?;
                if let Some(key_values) =
                    self.index
                        .project_index_row(&self.ctx.index_meta, &row_values, tid)?
                {
                    visit(tid, key_values)?;
                }
                Ok(())
            },
        )
    }
}

pub fn index_build_stub(
    ctx: &IndexBuildContext,
    am_oid: u32,
    heap: &dyn AccessHeapServices,
    index: &mut dyn AccessIndexServices,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBuildResult> {
    match am_oid {
        BTREE_AM_OID => {
            let (heap_tuples, projected) = collect_projected_rows(ctx, heap, index, scalar)?;
            crate::nbtree::btbuild_projected(ctx, heap_tuples, projected, scalar, wal)
        }
        GIN_AM_OID => {
            let (heap_tuples, projected) = collect_projected_rows(ctx, heap, index, scalar)?;
            crate::gin::ginbuild_projected(ctx, heap_tuples, projected, interrupts, scalar, wal)
        }
        GIST_AM_OID => {
            let mut source = ProjectedBuildSource {
                ctx,
                heap,
                index,
                scalar,
            };
            crate::gist::gistbuild(ctx, &mut source, interrupts, scalar, wal)
        }
        HASH_AM_OID => {
            let (heap_tuples, projected) = collect_projected_rows(ctx, heap, index, scalar)?;
            crate::hash::hashbuild_projected(ctx, heap_tuples, projected, interrupts, scalar, wal)
        }
        SPGIST_AM_OID => {
            let (heap_tuples, projected) = collect_projected_rows(ctx, heap, index, scalar)?;
            crate::spgist::spgbuild_projected(ctx, heap_tuples, projected, scalar, wal)
        }
        _ => Err(unknown_index_am()),
    }
}

pub fn index_insert_stub<R>(
    ctx: &IndexInsertContext,
    am_oid: u32,
    runtime: &R,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<bool>
where
    R: AccessHeapServices + AccessTransactionServices + AccessInterruptServices,
{
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btinsert(ctx, runtime, scalar, wal),
        GIN_AM_OID => crate::gin::gininsert(ctx, scalar, wal),
        GIST_AM_OID => crate::gist::gistinsert(ctx, scalar, wal),
        HASH_AM_OID => crate::hash::hashinsert(ctx, scalar, wal),
        SPGIST_AM_OID => crate::spgist::spginsert(ctx, scalar, wal),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_build_empty_stub(
    ctx: &IndexBuildEmptyContext,
    am_oid: u32,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btbuildempty(ctx, wal),
        GIN_AM_OID => crate::gin::ginbuildempty(ctx, wal),
        GIST_AM_OID => crate::gist::gistbuildempty(ctx, wal),
        HASH_AM_OID => crate::hash::hashbuildempty(ctx, wal),
        SPGIST_AM_OID => crate::spgist::spgbuildempty(ctx, wal),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_beginscan(
    ctx: &IndexBeginScanContext,
    am_oid: u32,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<IndexScanDesc> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btbeginscan(ctx, scalar),
        GIN_AM_OID => crate::gin::ginbeginscan(ctx),
        GIST_AM_OID => crate::gist::gistbeginscan(ctx),
        HASH_AM_OID => crate::hash::hashbeginscan(ctx, scalar),
        SPGIST_AM_OID => crate::spgist::spgbeginscan(ctx, scalar),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_rescan(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    keys: &[ScanKeyData],
    direction: ScanDirection,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<()> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btrescan(scan, keys, direction, scalar),
        GIN_AM_OID => crate::gin::ginrescan(scan, keys, direction),
        GIST_AM_OID => crate::gist::gistrescan(scan, keys, direction),
        HASH_AM_OID => crate::hash::hashrescan(scan, keys, direction, scalar),
        SPGIST_AM_OID => crate::spgist::spgrescan(scan, keys, direction, scalar),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_getnext(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btgettuple(scan, scalar),
        GIN_AM_OID => Err(AccessError::Corrupt("missing index gettuple callback")),
        GIST_AM_OID => crate::gist::gistgettuple(scan, scalar),
        HASH_AM_OID => crate::hash::hashgettuple(scan, scalar),
        SPGIST_AM_OID => crate::spgist::spggettuple(scan),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_getbitmap(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    bitmap: &mut TidBitmap,
    heap: &dyn AccessHeapServices,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<i64> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btgetbitmap(scan, bitmap, scalar),
        GIN_AM_OID => crate::gin::gingetbitmap(scan, bitmap, heap, scalar),
        GIST_AM_OID => crate::gist::gistgetbitmap(scan, bitmap, scalar),
        HASH_AM_OID => crate::hash::hashgetbitmap(scan, bitmap, scalar),
        SPGIST_AM_OID => crate::spgist::spggetbitmap(scan, bitmap),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_endscan(scan: IndexScanDesc, am_oid: u32) -> AccessResult<()> {
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btendscan(scan),
        GIN_AM_OID => crate::gin::ginendscan(scan),
        GIST_AM_OID => crate::gist::gistendscan(scan),
        HASH_AM_OID => crate::hash::hashendscan(scan),
        SPGIST_AM_OID => crate::spgist::spgendscan(scan),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_bulk_delete<R>(
    ctx: &IndexVacuumContext,
    am_oid: u32,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
    runtime: &R,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBulkDeleteResult>
where
    R: AccessTransactionServices + AccessInterruptServices,
{
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btbulkdelete(ctx, callback, stats, runtime, wal),
        GIN_AM_OID => crate::gin::ginbulkdelete(ctx, callback, stats, wal),
        GIST_AM_OID => crate::gist::gistbulkdelete(ctx, callback, stats, wal),
        HASH_AM_OID => crate::hash::hashbulkdelete(ctx, callback, stats, runtime, wal),
        SPGIST_AM_OID => crate::spgist::spgbulkdelete(ctx, callback, stats, wal),
        _ => Err(unknown_index_am()),
    }
}

pub fn index_vacuum_cleanup<R>(
    ctx: &IndexVacuumContext,
    am_oid: u32,
    stats: Option<IndexBulkDeleteResult>,
    runtime: &R,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBulkDeleteResult>
where
    R: AccessTransactionServices,
{
    match am_oid {
        BTREE_AM_OID => crate::nbtree::btvacuumcleanup(ctx, stats, runtime, wal),
        GIN_AM_OID => crate::gin::ginvacuumcleanup(ctx, stats, wal),
        GIST_AM_OID => crate::gist::gistvacuumcleanup(ctx, stats),
        HASH_AM_OID => crate::hash::hashvacuumcleanup(ctx, stats, wal),
        SPGIST_AM_OID => crate::spgist::spgvacuumcleanup(ctx, stats),
        _ => Err(unknown_index_am()),
    }
}
