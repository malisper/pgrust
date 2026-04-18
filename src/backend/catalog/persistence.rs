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
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, CatalogError};
use crate::backend::catalog::indexing::{
    maintain_catalog_indexes_for_insert_in_db, rebuild_system_catalog_indexes_for_db,
};
use crate::backend::catalog::rowcodec::{catalog_row_values_for_kind, decode_catalog_tuple_values};
use crate::backend::catalog::rows::{PhysicalCatalogRows, physical_catalog_rows_for_catalog_entry};
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
    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
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

pub(crate) fn append_catalog_entry_rows(
    base_dir: &Path,
    catalog: &Catalog,
    relation_name: &str,
    entry: &CatalogEntry,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let rows = physical_catalog_rows_for_catalog_entry(catalog, relation_name, entry);
    append_catalog_rows_subset_incremental(base_dir, &rows, entry.rel.db_oid, kinds)
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

fn append_catalog_rows_subset(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.create(rel, ForkNumber::Main, true)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    for &kind in kinds {
        insert_catalog_rows(
            &pool,
            bootstrap_catalog_rel(kind, db_oid),
            &bootstrap_relation_desc(kind),
            catalog_row_values_for_kind(rows, kind),
        )?;
    }
    rebuild_system_catalog_indexes_for_db(base_dir, db_oid)?;
    Ok(())
}

pub(crate) fn sync_catalog_rows_subset_incremental(
    base_dir: &Path,
    current_rows: &PhysicalCatalogRows,
    target_rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let (rows_to_delete, rows_to_insert) =
        diff_catalog_rows_subset(current_rows, target_rows, kinds);
    if physical_catalog_rows_empty(&rows_to_delete) && physical_catalog_rows_empty(&rows_to_insert)
    {
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

    let result = (|| {
        // :HACK: This transitional path only inserts new system catalog index tuples.
        // Deletes/updates rely on heap visibility checks to ignore stale index entries
        // until we add true system-catalog index delete/update maintenance.
        delete_catalog_rows_subset_mvcc(&ctx, &rows_to_delete, db_oid, kinds)?;
        insert_catalog_rows_subset_mvcc(&ctx, &rows_to_insert, db_oid, kinds)?;
        txns.write()
            .commit(xid)
            .map_err(|e| CatalogError::Io(format!("catalog transaction commit failed: {e:?}")))?;
        Ok(())
    })();

    if result.is_err() {
        let _ = txns.write().abort(xid);
    }
    result
}

fn append_catalog_rows_subset_incremental(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    sync_catalog_rows_subset_incremental(
        base_dir,
        &PhysicalCatalogRows::default(),
        rows,
        db_oid,
        kinds,
    )
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

fn diff_catalog_rows_subset(
    current_rows: &PhysicalCatalogRows,
    target_rows: &PhysicalCatalogRows,
    kinds: &[BootstrapCatalogKind],
) -> (PhysicalCatalogRows, PhysicalCatalogRows) {
    let mut rows_to_delete = PhysicalCatalogRows::default();
    let mut rows_to_insert = PhysicalCatalogRows::default();

    macro_rules! diff_kind {
        ($field:ident) => {{
            let (deletes, inserts) =
                diff_catalog_row_list(&current_rows.$field, &target_rows.$field);
            rows_to_delete.$field = deletes;
            rows_to_insert.$field = inserts;
        }};
    }

    for &kind in kinds {
        match kind {
            BootstrapCatalogKind::PgNamespace => diff_kind!(namespaces),
            BootstrapCatalogKind::PgClass => diff_kind!(classes),
            BootstrapCatalogKind::PgAttribute => diff_kind!(attributes),
            BootstrapCatalogKind::PgType => diff_kind!(types),
            BootstrapCatalogKind::PgProc => diff_kind!(procs),
            BootstrapCatalogKind::PgTsParser => diff_kind!(ts_parsers),
            BootstrapCatalogKind::PgTsTemplate => diff_kind!(ts_templates),
            BootstrapCatalogKind::PgTsDict => diff_kind!(ts_dicts),
            BootstrapCatalogKind::PgTsConfig => diff_kind!(ts_configs),
            BootstrapCatalogKind::PgTsConfigMap => diff_kind!(ts_config_maps),
            BootstrapCatalogKind::PgLanguage => diff_kind!(languages),
            BootstrapCatalogKind::PgOperator => diff_kind!(operators),
            BootstrapCatalogKind::PgDatabase => diff_kind!(databases),
            BootstrapCatalogKind::PgAuthId => diff_kind!(authids),
            BootstrapCatalogKind::PgAuthMembers => diff_kind!(auth_members),
            BootstrapCatalogKind::PgCollation => diff_kind!(collations),
            BootstrapCatalogKind::PgLargeobjectMetadata => {}
            BootstrapCatalogKind::PgTablespace => diff_kind!(tablespaces),
            BootstrapCatalogKind::PgAm => diff_kind!(ams),
            BootstrapCatalogKind::PgAmop => diff_kind!(amops),
            BootstrapCatalogKind::PgAmproc => diff_kind!(amprocs),
            BootstrapCatalogKind::PgAttrdef => diff_kind!(attrdefs),
            BootstrapCatalogKind::PgCast => diff_kind!(casts),
            BootstrapCatalogKind::PgConstraint => diff_kind!(constraints),
            BootstrapCatalogKind::PgDepend => diff_kind!(depends),
            BootstrapCatalogKind::PgDescription => diff_kind!(descriptions),
            BootstrapCatalogKind::PgIndex => diff_kind!(indexes),
            BootstrapCatalogKind::PgInherits => diff_kind!(inherits),
            BootstrapCatalogKind::PgRewrite => diff_kind!(rewrites),
            BootstrapCatalogKind::PgStatistic => diff_kind!(statistics),
            BootstrapCatalogKind::PgTrigger => diff_kind!(triggers),
            BootstrapCatalogKind::PgOpclass => diff_kind!(opclasses),
            BootstrapCatalogKind::PgOpfamily => diff_kind!(opfamilies),
        }
    }

    (rows_to_delete, rows_to_insert)
}

fn diff_catalog_row_list<T: Clone + PartialEq>(current: &[T], target: &[T]) -> (Vec<T>, Vec<T>) {
    let mut remaining_target = target.to_vec();
    let mut rows_to_delete = Vec::new();
    for row in current {
        if let Some(idx) = remaining_target
            .iter()
            .position(|candidate| candidate == row)
        {
            remaining_target.remove(idx);
        } else {
            rows_to_delete.push(row.clone());
        }
    }
    (rows_to_delete, remaining_target)
}

fn physical_catalog_rows_empty(rows: &PhysicalCatalogRows) -> bool {
    rows.namespaces.is_empty()
        && rows.classes.is_empty()
        && rows.attributes.is_empty()
        && rows.attrdefs.is_empty()
        && rows.depends.is_empty()
        && rows.inherits.is_empty()
        && rows.descriptions.is_empty()
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
        | BootstrapCatalogKind::PgTrigger => catalog_value_eq(left.first(), right.first()),
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
