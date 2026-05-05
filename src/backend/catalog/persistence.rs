use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{
    heap_delete_with_waiter, heap_fetch_visible_with_txns, heap_flush, heap_insert,
    heap_insert_mvcc_with_cid, heap_scan_begin, heap_scan_next, heap_update_with_waiter,
};
use crate::backend::access::heap::heapam_visibility::SnapshotVisibility;
use crate::backend::access::transam::xact::{Snapshot, TransactionManager};
use crate::backend::catalog::bootstrap::{bootstrap_catalog_rel, bootstrap_catalog_toast_rel};
use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::indexing::{
    CatalogTupleIdentity, catalog_index_insert_state_for_db, rebuild_system_catalog_indexes_for_db,
};
use crate::backend::catalog::rowcodec::catalog_row_values_for_kind;
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::{encode_tuple_values, tuple_from_values};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::heaptoast::{
    ExternalToastValueInput, StoredToastValue, TOAST_MAX_CHUNK_SIZE,
};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::catalog::{
    BootstrapCatalogKind, bootstrap_catalog_kinds, bootstrap_relation_desc,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::ToastRelationRef;

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
        smgr.unlink(rel, None, false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        if let Some(toast_rel) = bootstrap_catalog_toast_rel(kind, db_oid) {
            smgr.open(toast_rel)
                .map_err(|e| CatalogError::Io(e.to_string()))?;
            smgr.unlink(toast_rel, None, false);
            smgr.create(toast_rel, ForkNumber::Main, false)
                .map_err(|e| CatalogError::Io(e.to_string()))?;
        }
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
            kind,
            db_oid,
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
        let index_state = catalog_index_insert_state_for_db(ctx, kind, db_oid)?;
        for values in catalog_row_values_for_kind(rows, kind) {
            let tid = catalog_tuple_insert(
                ctx,
                kind,
                db_oid,
                bootstrap_catalog_rel(kind, db_oid),
                &desc,
                &values,
            )?;
            index_state.insert(tid, &values)?;
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
    let snapshot = ctx.snapshot_for_command()?;
    for &kind in kinds {
        let desc = bootstrap_relation_desc(kind);
        let rel = bootstrap_catalog_rel(kind, db_oid);
        let values = catalog_row_values_for_kind(rows, kind);
        if !values.is_empty() {
            catalog_tuples_delete_matching(ctx, kind, rel, &desc, &values, &snapshot).map_err(
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
    kind: BootstrapCatalogKind,
    db_oid: u32,
    rel: RelFileLocator,
    desc: &RelationDesc,
    rows: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    for values in rows {
        let tuple = catalog_tuple_from_values(pool, 0, kind, db_oid, desc, &values, None)?;
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
    kind: BootstrapCatalogKind,
    db_oid: u32,
    rel: RelFileLocator,
    desc: &RelationDesc,
    values: &[Value],
) -> Result<ItemPointerData, CatalogError> {
    let tuple = catalog_tuple_from_values(
        &ctx.pool,
        ctx.client_id,
        kind,
        db_oid,
        desc,
        values,
        Some(ctx),
    )?;
    heap_insert_mvcc_with_cid(&ctx.pool, ctx.client_id, rel, ctx.xid, ctx.cid, &tuple)
        .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))
}

fn catalog_tuple_from_values(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: crate::ClientId,
    kind: BootstrapCatalogKind,
    db_oid: u32,
    desc: &RelationDesc,
    values: &[Value],
    mvcc: Option<&CatalogWriteContext>,
) -> Result<HeapTuple, CatalogError> {
    let mut tuple_values = encode_tuple_values(desc, values)
        .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
    if let Some(toast_rel) = catalog_toast_relation(kind, db_oid) {
        pool.ensure_relation_fork(toast_rel.rel, ForkNumber::Main)
            .map_err(|e| CatalogError::Io(format!("catalog toast fork create failed: {e:?}")))?;
        let mut store_external = |value: ExternalToastValueInput| {
            store_catalog_external_value(pool, client_id, toast_rel, &value, mvcc)
                .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))
        };
        pgrust_access::table::toast_helper::toast_tuple_values_for_write_external_only_with_store(
            desc,
            &mut tuple_values,
            toast_rel.relation_oid,
            &mut store_external,
        )
        .map_err(|e| CatalogError::Io(format!("catalog tuple toast failed: {e:?}")))?;
    }
    HeapTuple::from_values(&desc.attribute_descs(), &tuple_values)
        .map_err(|e| CatalogError::Io(format!("catalog tuple build failed: {e:?}")))
}

fn catalog_toast_relation(kind: BootstrapCatalogKind, db_oid: u32) -> Option<ToastRelationRef> {
    bootstrap_catalog_toast_rel(kind, db_oid).map(|rel| ToastRelationRef {
        rel,
        relation_oid: kind.toast_relation_oid(),
    })
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogTupleUpdateResult {
    pub(crate) old_values: Vec<Value>,
    pub(crate) new_values: Vec<Value>,
    pub(crate) old_tid: ItemPointerData,
    pub(crate) new_tid: ItemPointerData,
}

pub(crate) fn catalog_tuple_update_by_identity(
    ctx: &CatalogWriteContext,
    identity: &CatalogTupleIdentity,
    replacements: &[(&str, Value)],
) -> Result<CatalogTupleUpdateResult, CatalogError> {
    let snapshot = ctx.snapshot_for_command()?;
    let tuple = heap_fetch_visible_with_txns(
        &ctx.pool,
        ctx.client_id,
        identity.heap_rel,
        identity.tid,
        &ctx.txns,
        &snapshot,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple fetch failed: {e:?}")))?
    .ok_or(CatalogError::Corrupt("missing catalog tuple for update"))?;
    let desc = bootstrap_relation_desc(identity.kind);
    let txns_guard = ctx.txns.read();
    let old_values = crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
        &ctx.pool,
        &txns_guard,
        &snapshot,
        ctx.client_id,
        identity.kind,
        identity.db_oid,
        &desc,
        &tuple,
    )?;
    drop(txns_guard);

    let mut new_values = old_values.clone();
    for (column_name, value) in replacements {
        let Some(index) = desc
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(CatalogError::UnknownColumn((*column_name).to_string()));
        };
        new_values[index] = value.clone();
    }

    let replacement = catalog_tuple_from_values(
        &ctx.pool,
        ctx.client_id,
        identity.kind,
        identity.db_oid,
        &desc,
        &new_values,
        Some(ctx),
    )?;
    let waiter = ctx
        .waiter
        .as_deref()
        .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref()));
    let new_tid = heap_update_with_waiter(
        &ctx.pool,
        ctx.client_id,
        identity.heap_rel,
        &ctx.txns,
        ctx.xid,
        ctx.cid,
        identity.tid,
        &replacement,
        waiter,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple update failed: {e:?}")))?;
    catalog_index_insert_state_for_db(ctx, identity.kind, identity.db_oid)?.insert_with_old_tid(
        new_tid,
        Some(identity.tid),
        &new_values,
    )?;

    Ok(CatalogTupleUpdateResult {
        old_values,
        new_values,
        old_tid: identity.tid,
        new_tid,
    })
}

pub(crate) fn catalog_tuple_delete_by_identity(
    ctx: &CatalogWriteContext,
    identity: &CatalogTupleIdentity,
) -> Result<Vec<Value>, CatalogError> {
    let snapshot = ctx.snapshot_for_command()?;
    let tuple = heap_fetch_visible_with_txns(
        &ctx.pool,
        ctx.client_id,
        identity.heap_rel,
        identity.tid,
        &ctx.txns,
        &snapshot,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple fetch failed: {e:?}")))?
    .ok_or(CatalogError::Corrupt("missing catalog tuple for delete"))?;
    let desc = bootstrap_relation_desc(identity.kind);
    let txns_guard = ctx.txns.read();
    let old_values = crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
        &ctx.pool,
        &txns_guard,
        &snapshot,
        ctx.client_id,
        identity.kind,
        identity.db_oid,
        &desc,
        &tuple,
    )?;
    drop(txns_guard);

    let waiter = ctx
        .waiter
        .as_deref()
        .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref()));
    heap_delete_with_waiter(
        &ctx.pool,
        ctx.client_id,
        identity.heap_rel,
        &ctx.txns,
        ctx.xid,
        identity.tid,
        &snapshot,
        waiter,
    )
    .map_err(|e| CatalogError::Io(format!("catalog tuple delete failed: {e:?}")))?;

    Ok(old_values)
}

fn next_catalog_toast_value_id(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: crate::ClientId,
    toast: ToastRelationRef,
) -> Result<u32, CatalogError> {
    let mut scan = heap_scan_begin(pool, toast.rel)
        .map_err(|e| CatalogError::Io(format!("catalog toast scan begin failed: {e:?}")))?;
    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    let mut max_value_id = 0u32;
    while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
        .map_err(|e| CatalogError::Io(format!("catalog toast scan failed: {e:?}")))?
    {
        let values = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("catalog toast deform failed: {e:?}")))?;
        let Some(chunk_id) = pgrust_access::access::heaptoast::toast_chunk_id_from_values(&values)
            .map_err(|e| CatalogError::Io(format!("catalog toast chunk decode failed: {e:?}")))?
        else {
            continue;
        };
        max_value_id = max_value_id.max(chunk_id);
    }
    Ok(max_value_id.saturating_add(1))
}

fn store_catalog_external_value(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: crate::ClientId,
    toast: ToastRelationRef,
    value: &ExternalToastValueInput,
    mvcc: Option<&CatalogWriteContext>,
) -> Result<StoredToastValue, CatalogError> {
    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let value_id = next_catalog_toast_value_id(pool, client_id, toast)?;
    let mut chunk_tids = Vec::new();

    for (chunk_seq, chunk) in value.data.chunks(TOAST_MAX_CHUNK_SIZE).enumerate() {
        let row = pgrust_access::access::heaptoast::toast_chunk_row_values(
            value_id,
            chunk_seq as i32,
            chunk,
        );
        let tuple = tuple_from_values(&desc, &row)
            .map_err(|e| CatalogError::Io(format!("catalog toast tuple encode failed: {e:?}")))?;
        let tid = if let Some(ctx) = mvcc {
            heap_insert_mvcc_with_cid(pool, client_id, toast.rel, ctx.xid, ctx.cid, &tuple)
        } else {
            heap_insert(pool, client_id, toast.rel, &tuple)
        }
        .map_err(|e| CatalogError::Io(format!("catalog toast chunk insert failed: {e:?}")))?;
        chunk_tids.push(tid);
    }

    // :HACK: Catalog detoast readers scan the toast heap directly for now, so
    // catalog toast index maintenance is not required for correctness.
    Ok(StoredToastValue {
        pointer: pgrust_access::varatt::VarattExternal {
            va_rawsize: value.rawsize,
            va_extinfo: if value.compression_id
                == pgrust_access::access::toast_compression::ToastCompressionId::Invalid
            {
                value.data.len() as u32
            } else {
                pgrust_access::varatt::varatt_external_set_size_and_compression_method(
                    value.data.len() as u32,
                    value.compression_id as u32,
                )
            },
            va_valueid: value_id,
            va_toastrelid: toast.relation_oid,
        },
        chunk_tids,
    })
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

fn catalog_tuples_delete_matching(
    ctx: &CatalogWriteContext,
    kind: BootstrapCatalogKind,
    rel: RelFileLocator,
    desc: &RelationDesc,
    values: &[Vec<Value>],
    snapshot: &Snapshot,
) -> Result<(), CatalogError> {
    let mut wanted = values
        .iter()
        .map(|row| WantedCatalogDelete {
            values: row,
            key: catalog_row_identity_key(kind, row),
            tid: None,
        })
        .collect::<Vec<_>>();
    let mut remaining = wanted.iter().fold(
        BTreeMap::<CatalogIdentityKey, usize>::new(),
        |mut acc, row| {
            *acc.entry(row.key.clone()).or_default() += 1;
            acc
        },
    );
    let mut fallback_tids = BTreeMap::<CatalogIdentityKey, Vec<ItemPointerData>>::new();
    {
        let txns = ctx.txns.read();
        let mut scan = heap_scan_begin(&ctx.pool, rel)
            .map_err(|e| CatalogError::Io(format!("catalog scan begin failed: {e:?}")))?;
        while let Some((tid, tuple)) = heap_scan_next(&ctx.pool, ctx.client_id, &mut scan)
            .map_err(|e| CatalogError::Io(format!("catalog scan failed: {e:?}")))?
        {
            if remaining.is_empty() {
                break;
            }
            if !snapshot.tuple_visible(&*txns, &tuple) {
                continue;
            }
            let decoded =
                crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
                    &ctx.pool,
                    &txns,
                    snapshot,
                    ctx.client_id,
                    kind,
                    rel.db_oid,
                    desc,
                    &tuple,
                )?;
            let key = catalog_row_identity_key(kind, &decoded);
            if !remaining.contains_key(&key) {
                continue;
            };
            if let Some(wanted_row) = wanted.iter_mut().find(|wanted| {
                wanted.tid.is_none()
                    && wanted.key == key
                    && wanted.values.as_slice() == decoded.as_slice()
            }) {
                wanted_row.tid = Some(tid);
                decrement_remaining_catalog_delete(&mut remaining, &key);
            } else {
                fallback_tids.entry(key).or_default().push(tid);
            }
        }
    }

    for wanted_row in wanted.iter_mut().filter(|wanted| wanted.tid.is_none()) {
        let Some(tids) = fallback_tids.get_mut(&wanted_row.key) else {
            continue;
        };
        if let Some(tid) = tids.pop() {
            wanted_row.tid = Some(tid);
            decrement_remaining_catalog_delete(&mut remaining, &wanted_row.key);
        }
    }

    if !remaining.is_empty() {
        return Err(CatalogError::Corrupt("missing catalog tuple for delete"));
    }

    let waiter = ctx
        .waiter
        .as_deref()
        .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref()));
    for tid in wanted.into_iter().filter_map(|wanted| wanted.tid) {
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
    }
    Ok(())
}

struct WantedCatalogDelete<'a> {
    values: &'a Vec<Value>,
    key: CatalogIdentityKey,
    tid: Option<ItemPointerData>,
}

fn decrement_remaining_catalog_delete(
    remaining: &mut BTreeMap<CatalogIdentityKey, usize>,
    key: &CatalogIdentityKey,
) {
    let Some(count) = remaining.get_mut(key) else {
        return;
    };
    *count -= 1;
    if *count == 0 {
        remaining.remove(key);
    }
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
        if !snapshot.tuple_visible(&*txns, &tuple) {
            continue;
        }
        let decoded = crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
            &ctx.pool,
            &txns,
            snapshot,
            ctx.client_id,
            kind,
            rel.db_oid,
            desc,
            &tuple,
        )?;
        if !catalog_row_identity_matches(kind, &decoded, values) {
            continue;
        }
        return Ok(Some(tid));
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CatalogIdentityValue {
    Null,
    Int(i64),
    UInt(u64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
    Other(String),
}

type CatalogIdentityKey = Vec<CatalogIdentityValue>;

fn catalog_row_identity_key(kind: BootstrapCatalogKind, values: &[Value]) -> CatalogIdentityKey {
    match kind {
        BootstrapCatalogKind::PgClass
        | BootstrapCatalogKind::PgNamespace
        | BootstrapCatalogKind::PgType
        | BootstrapCatalogKind::PgAttrdef
        | BootstrapCatalogKind::PgTrigger
        | BootstrapCatalogKind::PgEventTrigger
        | BootstrapCatalogKind::PgPolicy
        | BootstrapCatalogKind::PgStatisticExt
        | BootstrapCatalogKind::PgConversion
        | BootstrapCatalogKind::PgProc
        | BootstrapCatalogKind::PgAggregate
        | BootstrapCatalogKind::PgConstraint
        | BootstrapCatalogKind::PgIndex
        | BootstrapCatalogKind::PgPartitionedTable
        | BootstrapCatalogKind::PgSequence
        | BootstrapCatalogKind::PgPublication
        | BootstrapCatalogKind::PgPublicationRel
        | BootstrapCatalogKind::PgPublicationNamespace
        | BootstrapCatalogKind::PgDefaultAcl
        | BootstrapCatalogKind::PgLargeobjectMetadata => {
            catalog_identity_key_from_indexes(values, &[0])
        }
        BootstrapCatalogKind::PgLargeobject => catalog_identity_key_from_indexes(values, &[0, 1]),
        BootstrapCatalogKind::PgAttribute => catalog_identity_key_from_indexes(values, &[0, 4]),
        BootstrapCatalogKind::PgDepend => {
            catalog_identity_key_from_indexes(values, &[0, 1, 2, 3, 4, 5, 6])
        }
        BootstrapCatalogKind::PgStatistic | BootstrapCatalogKind::PgDescription => {
            catalog_identity_key_from_indexes(values, &[0, 1, 2])
        }
        _ => values.iter().map(catalog_identity_value).collect(),
    }
}

fn catalog_identity_key_from_indexes(values: &[Value], indexes: &[usize]) -> CatalogIdentityKey {
    indexes
        .iter()
        .map(|index| {
            values
                .get(*index)
                .map(catalog_identity_value)
                .unwrap_or(CatalogIdentityValue::Null)
        })
        .collect()
}

fn catalog_identity_value(value: &Value) -> CatalogIdentityValue {
    match value {
        Value::Int16(value) => CatalogIdentityValue::Int(i64::from(*value)),
        Value::Int32(value) => CatalogIdentityValue::Int(i64::from(*value)),
        Value::Int64(value) => CatalogIdentityValue::Int(*value),
        Value::Xid8(value) | Value::PgLsn(value) => CatalogIdentityValue::UInt(*value),
        Value::EnumOid(value) => CatalogIdentityValue::UInt(u64::from(*value)),
        Value::Text(value) => CatalogIdentityValue::Text(value.to_string()),
        Value::InternalChar(value) => CatalogIdentityValue::Text(char::from(*value).to_string()),
        Value::Bool(value) => CatalogIdentityValue::Bool(*value),
        Value::Bytea(value) => CatalogIdentityValue::Bytes(value.clone()),
        Value::Uuid(value) => CatalogIdentityValue::Bytes(value.to_vec()),
        Value::MacAddr(value) => CatalogIdentityValue::Bytes(value.to_vec()),
        Value::MacAddr8(value) => CatalogIdentityValue::Bytes(value.to_vec()),
        Value::Null => CatalogIdentityValue::Null,
        other => CatalogIdentityValue::Other(format!("{other:?}")),
    }
}

fn physical_catalog_rows_empty(rows: &PhysicalCatalogRows) -> bool {
    rows.namespaces.is_empty()
        && rows.classes.is_empty()
        && rows.attributes.is_empty()
        && rows.attrdefs.is_empty()
        && rows.depends.is_empty()
        && rows.shdepends.is_empty()
        && rows.inherits.is_empty()
        && rows.descriptions.is_empty()
        && rows.foreign_data_wrappers.is_empty()
        && rows.foreign_servers.is_empty()
        && rows.foreign_tables.is_empty()
        && rows.user_mappings.is_empty()
        && rows.indexes.is_empty()
        && rows.rewrites.is_empty()
        && rows.sequences.is_empty()
        && rows.triggers.is_empty()
        && rows.event_triggers.is_empty()
        && rows.ams.is_empty()
        && rows.amops.is_empty()
        && rows.amprocs.is_empty()
        && rows.authids.is_empty()
        && rows.auth_members.is_empty()
        && rows.languages.is_empty()
        && rows.largeobjects.is_empty()
        && rows.largeobject_metadata.is_empty()
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
        && rows.conversions.is_empty()
        && rows.collations.is_empty()
        && rows.default_acls.is_empty()
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
        | BootstrapCatalogKind::PgNamespace
        | BootstrapCatalogKind::PgType
        | BootstrapCatalogKind::PgAttrdef
        | BootstrapCatalogKind::PgTrigger
        | BootstrapCatalogKind::PgEventTrigger
        | BootstrapCatalogKind::PgPolicy
        | BootstrapCatalogKind::PgStatisticExt
        | BootstrapCatalogKind::PgConversion
        | BootstrapCatalogKind::PgProc
        | BootstrapCatalogKind::PgAggregate
        | BootstrapCatalogKind::PgSequence => catalog_value_eq(left.first(), right.first()),
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
        BootstrapCatalogKind::PgPartitionedTable => catalog_value_eq(left.first(), right.first()),
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
