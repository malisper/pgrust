use std::path::Path;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{
    heap_delete_with_waiter, heap_flush, heap_insert, heap_insert_mvcc_with_cid, heap_scan_begin,
    heap_scan_next, heap_update_with_waiter,
};
use crate::backend::access::transam::xact::Snapshot;
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, CatalogError};
use crate::backend::catalog::indexing::{
    maintain_catalog_indexes_for_insert, rebuild_system_catalog_indexes,
};
use crate::backend::catalog::rowcodec::{catalog_row_values_for_kind, decode_catalog_tuple_values};
use crate::backend::catalog::rows::{PhysicalCatalogRows, physical_catalog_rows_for_catalog_entry};
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::catalog::{
    BootstrapCatalogKind, bootstrap_catalog_kinds, bootstrap_relation_desc,
};
use crate::include::nodes::datum::Value;

pub(crate) fn sync_catalog_rows(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
) -> Result<(), CatalogError> {
    sync_catalog_rows_subset(base_dir, rows, db_oid, &bootstrap_catalog_kinds())
}

pub(crate) fn sync_catalog_rows_subset(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        };
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    for &kind in kinds {
        insert_catalog_rows(
            &pool,
            RelFileLocator {
                spc_oid: 0,
                db_oid,
                rel_number: kind.relation_oid(),
            },
            &bootstrap_relation_desc(kind),
            catalog_row_values_for_kind(rows, kind),
        )?;
    }
    rebuild_system_catalog_indexes(base_dir)?;
    Ok(())
}

pub(crate) fn append_catalog_entry_rows(
    base_dir: &Path,
    catalog: &Catalog,
    relation_name: &str,
    entry: &CatalogEntry,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let rows = physical_catalog_rows_for_catalog_entry(catalog, relation_name, entry);
    append_catalog_rows_subset(base_dir, &rows, 1, kinds)
}

pub(crate) fn insert_catalog_rows_subset_mvcc(
    ctx: &CatalogWriteContext,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    for &kind in kinds {
        let desc = bootstrap_relation_desc(kind);
        for values in catalog_row_values_for_kind(rows, kind) {
            let tid = catalog_tuple_insert(
                ctx,
                RelFileLocator {
                    spc_oid: 0,
                    db_oid,
                    rel_number: kind.relation_oid(),
                },
                &desc,
                &values,
            )?;
            maintain_catalog_indexes_for_insert(ctx, kind, tid, &values)?;
        }
    }
    Ok(())
}

pub(crate) fn delete_catalog_rows_subset_mvcc(
    ctx: &CatalogWriteContext,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
    for &kind in kinds {
        let desc = bootstrap_relation_desc(kind);
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        };
        for values in catalog_row_values_for_kind(rows, kind) {
            catalog_tuple_delete_matching(ctx, kind, rel, &desc, &values, &snapshot).map_err(
                |err| CatalogError::Io(format!("catalog delete for {kind:?} failed: {err:?}")),
            )?;
        }
    }
    Ok(())
}

fn append_catalog_rows_subset(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        };
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.create(rel, ForkNumber::Main, true)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    for &kind in kinds {
        insert_catalog_rows(
            &pool,
            RelFileLocator {
                spc_oid: 0,
                db_oid,
                rel_number: kind.relation_oid(),
            },
            &bootstrap_relation_desc(kind),
            catalog_row_values_for_kind(rows, kind),
        )?;
    }
    rebuild_system_catalog_indexes(base_dir)?;
    Ok(())
}

fn insert_catalog_rows(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
    rows: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    for values in rows {
        let tuple = tuple_from_values(desc, &values)
            .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
        heap_insert(pool, 0, rel, &tuple)
            .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))?;
    }
    let nblocks = pool
        .with_storage_mut(|s| s.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|e| CatalogError::Io(e.to_string()))?;
    for block in 0..nblocks {
        heap_flush(pool, 0, rel, block)
            .map_err(|e| CatalogError::Io(format!("catalog flush failed: {e:?}")))?;
    }
    Ok(())
}

fn catalog_tuple_insert(
    ctx: &CatalogWriteContext,
    rel: RelFileLocator,
    desc: &RelationDesc,
    values: &[Value],
) -> Result<ItemPointerData, CatalogError> {
    let tuple = tuple_from_values(desc, values)
        .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
    heap_insert_mvcc_with_cid(&ctx.pool, ctx.client_id, rel, ctx.xid, ctx.cid, &tuple)
        .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))
}

#[allow(dead_code)]
fn catalog_tuple_update_matching(
    ctx: &CatalogWriteContext,
    kind: BootstrapCatalogKind,
    rel: RelFileLocator,
    desc: &RelationDesc,
    old_values: &[Value],
    new_values: &[Value],
    snapshot: &Snapshot,
) -> Result<(), CatalogError> {
    let tid = find_catalog_tuple_tid(ctx, kind, rel, desc, old_values, snapshot)?
        .ok_or(CatalogError::Corrupt("missing catalog tuple for update"))?;
    let replacement = tuple_from_values(desc, new_values)
        .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
    let waiter = ctx
        .waiter
        .as_deref()
        .map(|waiter| (&*ctx.txns, waiter));
    heap_update_with_waiter(
        &ctx.pool,
        ctx.client_id,
        rel,
        &ctx.txns,
        ctx.xid,
        ctx.cid,
        tid,
        &replacement,
        waiter,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple update failed: {e:?}")))?;
    Ok(())
}

fn catalog_tuple_delete_matching(
    ctx: &CatalogWriteContext,
    kind: BootstrapCatalogKind,
    rel: RelFileLocator,
    desc: &RelationDesc,
    values: &[Value],
    snapshot: &Snapshot,
) -> Result<(), CatalogError> {
    let tid = find_catalog_tuple_tid(ctx, kind, rel, desc, values, snapshot)?
        .ok_or(CatalogError::Corrupt("missing catalog tuple for delete"))?;
    let waiter = ctx
        .waiter
        .as_deref()
        .map(|waiter| (&*ctx.txns, waiter));
    heap_delete_with_waiter(
        &ctx.pool,
        ctx.client_id,
        rel,
        &ctx.txns,
        ctx.xid,
        tid,
        snapshot,
        waiter,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple delete failed: {e:?}")))?;
    Ok(())
}

fn find_catalog_tuple_tid(
    ctx: &CatalogWriteContext,
    kind: BootstrapCatalogKind,
    rel: RelFileLocator,
    desc: &RelationDesc,
    values: &[Value],
    snapshot: &Snapshot,
) -> Result<Option<ItemPointerData>, CatalogError> {
    let txns = ctx.txns.read();
    let mut scan = heap_scan_begin(&ctx.pool, rel)
        .map_err(|e| CatalogError::Io(format!("catalog scan begin failed: {e:?}")))?;
    while let Some((tid, tuple)) = heap_scan_next(&ctx.pool, ctx.client_id, &mut scan)
        .map_err(|e| CatalogError::Io(format!("catalog scan failed: {e:?}")))?
    {
        if !snapshot.tuple_visible(&txns, &tuple) {
            continue;
        }
        let decoded = decode_catalog_tuple_values(desc, &tuple)?;
        if catalog_row_identity_matches(kind, &decoded, values) {
            return Ok(Some(tid));
        }
    }
    Ok(None)
}

fn catalog_row_identity_matches(
    kind: BootstrapCatalogKind,
    left: &[Value],
    right: &[Value],
) -> bool {
    match kind {
        BootstrapCatalogKind::PgClass
        | BootstrapCatalogKind::PgType
        | BootstrapCatalogKind::PgAttrdef => catalog_value_eq(left.first(), right.first()),
        BootstrapCatalogKind::PgAttribute => {
            catalog_value_eq(left.first(), right.first())
                && catalog_value_eq(left.get(3), right.get(3))
        }
        BootstrapCatalogKind::PgConstraint => catalog_value_eq(left.first(), right.first()),
        BootstrapCatalogKind::PgDepend => {
            catalog_value_eq(left.get(0), right.get(0))
                && catalog_value_eq(left.get(1), right.get(1))
                && catalog_value_eq(left.get(2), right.get(2))
                && catalog_value_eq(left.get(3), right.get(3))
                && catalog_value_eq(left.get(4), right.get(4))
                && catalog_value_eq(left.get(5), right.get(5))
                && catalog_value_eq(left.get(6), right.get(6))
        }
        BootstrapCatalogKind::PgIndex => catalog_value_eq(left.first(), right.first()),
        _ => left == right,
    }
}

fn catalog_value_eq(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (Some(Value::Int16(a)), Some(Value::Int16(b))) => a == b,
        (Some(Value::Int16(a)), Some(Value::Int32(b))) => i32::from(*a) == *b,
        (Some(Value::Int16(a)), Some(Value::Int64(b))) => i64::from(*a) == *b,
        (Some(Value::Int32(a)), Some(Value::Int16(b))) => *a == i32::from(*b),
        (Some(Value::Int32(a)), Some(Value::Int32(b))) => a == b,
        (Some(Value::Int32(a)), Some(Value::Int64(b))) => i64::from(*a) == *b,
        (Some(Value::Int64(a)), Some(Value::Int16(b))) => *a == i64::from(*b),
        (Some(Value::Int64(a)), Some(Value::Int32(b))) => *a == i64::from(*b),
        (Some(Value::Int64(a)), Some(Value::Int64(b))) => a == b,
        (Some(Value::Text(a)), Some(Value::Text(b))) => a == b,
        (Some(Value::Text(a)), Some(Value::InternalChar(b))) => {
            a.chars().next() == Some(char::from(*b))
        }
        (Some(Value::InternalChar(a)), Some(Value::Text(b))) => {
            Some(char::from(*a)) == b.chars().next()
        }
        (Some(Value::InternalChar(a)), Some(Value::InternalChar(b))) => a == b,
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a == b,
        (Some(a), Some(b)) => a == b,
        (None, None) => true,
        _ => false,
    }
}
