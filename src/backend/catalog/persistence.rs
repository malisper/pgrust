use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{
    heap_delete_with_waiter, heap_flush, heap_insert, heap_insert_mvcc_with_cid, heap_scan_begin,
    heap_scan_next, heap_update_with_waiter,
};
use crate::backend::access::transam::xact::{Snapshot, TransactionManager};
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::indexing::{
    maintain_catalog_indexes_for_insert_in_db, rebuild_system_catalog_indexes_for_db,
};
use crate::backend::catalog::rowcodec::{catalog_row_values_for_kind, decode_catalog_tuple_values};
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::catalog::{
    BootstrapCatalogKind, bootstrap_catalog_kinds, bootstrap_relation_desc,
};
use crate::include::nodes::datum::Value;

#[allow(dead_code)]
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
    // Bootstrap/template-copy path only. Steady-state durable catalog writes
    // should use row-level incremental maintenance instead of heap rewrites.
    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    // Bulk bootstrap rewrites can safely defer fsync until the relation is
    // fully written because no concurrent readers exist yet and we force a
    // final relation sync below.
    let pool = BufferPool::new_without_wal_skip_fsync(SmgrStorageBackend::new(smgr), 16);
    sync_catalog_rows_subset_in_pool(&pool, rows, db_oid, kinds)?;
    rebuild_system_catalog_indexes_for_db(base_dir, db_oid)?;
    Ok(())
}

pub(crate) fn sync_catalog_rows_subset_in_pool(
    pool: &BufferPool<SmgrStorageBackend>,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    for &kind in kinds {
        insert_catalog_rows(
            pool,
            bootstrap_catalog_rel(kind, db_oid),
            &bootstrap_relation_desc(kind),
            catalog_row_values_for_kind(rows, kind),
        )?;
    }
    Ok(())
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
            let tid =
                catalog_tuple_insert(ctx, bootstrap_catalog_rel(kind, db_oid), &desc, &values)?;
            maintain_catalog_indexes_for_insert_in_db(ctx, kind, db_oid, tid, &values)?;
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
        let rel = bootstrap_catalog_rel(kind, db_oid);
        for values in catalog_row_values_for_kind(rows, kind) {
            catalog_tuple_delete_matching(ctx, kind, rel, &desc, &values, &snapshot).map_err(
                |err| CatalogError::Io(format!("catalog delete for {kind:?} failed: {err:?}")),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn apply_catalog_row_changes_subset_incremental(
    base_dir: &Path,
    rows_to_delete: &PhysicalCatalogRows,
    rows_to_insert: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    if physical_catalog_rows_empty(rows_to_delete) && physical_catalog_rows_empty(rows_to_insert) {
        return Ok(());
    }

    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 16));
    let txns = Arc::new(RwLock::new(
        TransactionManager::new_durable(base_dir.to_path_buf())
            .map_err(|e| CatalogError::Io(format!("catalog transaction load failed: {e:?}")))?,
    ));
    let xid = txns.write().begin();
    let ctx = CatalogWriteContext {
        pool,
        txns: Arc::clone(&txns),
        xid,
        cid: 0,
        client_id: 0,
        waiter: None,
        interrupts: Arc::new(InterruptState::new()),
    };

    let mut committed = false;
    let result = (|| {
        delete_catalog_rows_subset_mvcc(&ctx, rows_to_delete, db_oid, kinds)?;
        insert_catalog_rows_subset_mvcc(&ctx, rows_to_insert, db_oid, kinds)?;
        txns.write()
            .commit(xid)
            .map_err(|e| CatalogError::Io(format!("catalog transaction commit failed: {e:?}")))?;
        // :HACK: Steady-state direct catalog writers still use a standalone
        // durable transaction manager here. Flush its CLOG before returning so
        // fresh durable readers immediately observe the committed xid instead
        // of depending on drop-time writeback timing.
        txns.write()
            .flush_clog()
            .map_err(|e| CatalogError::Io(format!("catalog transaction flush failed: {e:?}")))?;
        committed = true;
        // PostgreSQL leaves dead catalog index tuples behind here and expects a
        // later VACUUM to reclaim them once their deleting xid is old enough.
        Ok(())
    })();

    if result.is_err() && !committed {
        let _ = txns.write().abort(xid);
    }
    result
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
    pool.with_storage_mut(|s| s.smgr.immedsync(rel, ForkNumber::Main))
        .map_err(|e| CatalogError::Io(format!("catalog sync failed: {e}")))?;
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
        .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref()));
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
        .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref()));
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
        let decoded = decode_catalog_tuple_values(desc, &tuple)?;
        if !catalog_row_identity_matches(kind, &decoded, values) {
            continue;
        }
        if snapshot.tuple_visible(&txns, &tuple) {
            return Ok(Some(tid));
        }
    }
    Ok(None)
}

fn physical_catalog_rows_empty(rows: &PhysicalCatalogRows) -> bool {
    rows.namespaces.is_empty()
        && rows.classes.is_empty()
        && rows.attributes.is_empty()
        && rows.attrdefs.is_empty()
        && rows.depends.is_empty()
        && rows.inherits.is_empty()
        && rows.descriptions.is_empty()
        && rows.foreign_data_wrappers.is_empty()
        && rows.indexes.is_empty()
        && rows.rewrites.is_empty()
        && rows.triggers.is_empty()
        && rows.ams.is_empty()
        && rows.amops.is_empty()
        && rows.amprocs.is_empty()
        && rows.authids.is_empty()
        && rows.auth_members.is_empty()
        && rows.languages.is_empty()
        && rows.ts_parsers.is_empty()
        && rows.ts_templates.is_empty()
        && rows.ts_dicts.is_empty()
        && rows.ts_configs.is_empty()
        && rows.ts_config_maps.is_empty()
        && rows.constraints.is_empty()
        && rows.operators.is_empty()
        && rows.opclasses.is_empty()
        && rows.opfamilies.is_empty()
        && rows.procs.is_empty()
        && rows.aggregates.is_empty()
        && rows.casts.is_empty()
        && rows.collations.is_empty()
        && rows.databases.is_empty()
        && rows.tablespaces.is_empty()
        && rows.statistics.is_empty()
        && rows.types.is_empty()
}

fn catalog_row_identity_matches(
    kind: BootstrapCatalogKind,
    left: &[Value],
    right: &[Value],
) -> bool {
    match kind {
        BootstrapCatalogKind::PgClass
        | BootstrapCatalogKind::PgType
        | BootstrapCatalogKind::PgAttrdef
        | BootstrapCatalogKind::PgTrigger
        | BootstrapCatalogKind::PgPolicy
        | BootstrapCatalogKind::PgAggregate => catalog_value_eq(left.first(), right.first()),
        BootstrapCatalogKind::PgAttribute => {
            catalog_value_eq(left.first(), right.first())
                && catalog_value_eq(left.get(4), right.get(4))
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
        BootstrapCatalogKind::PgStatistic => {
            catalog_value_eq(left.first(), right.first())
                && catalog_value_eq(left.get(1), right.get(1))
                && catalog_value_eq(left.get(2), right.get(2))
        }
        BootstrapCatalogKind::PgDescription => {
            catalog_value_eq(left.first(), right.first())
                && catalog_value_eq(left.get(1), right.get(1))
                && catalog_value_eq(left.get(2), right.get(2))
        }
        _ => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(lhs, rhs)| catalog_value_eq(Some(lhs), Some(rhs)))
        }
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
