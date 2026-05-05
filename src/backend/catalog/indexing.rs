use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::heap::vacuumlazy::{vacuum_relation_pages, vacuum_relation_scan};
use crate::backend::access::index::indexam::{
    index_beginscan, index_build_stub, index_bulk_delete, index_endscan, index_getnext,
    index_vacuum_cleanup,
};
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::backend::utils::time::snapmgr::Snapshot;
use crate::include::access::amapi::{
    IndexBeginScanContext, IndexBuildContext, IndexInsertContext, IndexUniqueCheck,
    IndexVacuumContext,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    BTREE_AM_OID, BootstrapCatalogKind, CatalogIndexDescriptor, system_catalog_index_by_oid,
    system_catalog_indexes, system_catalog_indexes_for_heap,
};
use crate::include::nodes::datum::Value;
pub use pgrust_catalog_store::indexing::{
    insert_bootstrap_system_indexes, system_catalog_index_desc, system_catalog_index_entry,
    system_catalog_index_entry_for_db, system_catalog_index_meta, system_catalog_index_rel_for_db,
    system_catalog_index_relcache,
};

const SYSTEM_CATALOG_INDEX_SHADOW_REL_NUMBER_BASE: u32 = 0xF000_0000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogTupleIdentity {
    pub(crate) db_oid: u32,
    pub(crate) kind: BootstrapCatalogKind,
    pub(crate) heap_rel: RelFileLocator,
    pub(crate) tid: ItemPointerData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogScannedTuple {
    pub(crate) identity: CatalogTupleIdentity,
    pub(crate) tuple: crate::include::access::htup::HeapTuple,
}

fn system_catalog_index_shadow_rel(
    descriptor: CatalogIndexDescriptor,
    db_oid: u32,
    ordinal: usize,
) -> RelFileLocator {
    let target = system_catalog_index_rel_for_db(descriptor, db_oid);
    RelFileLocator {
        rel_number: SYSTEM_CATALOG_INDEX_SHADOW_REL_NUMBER_BASE.saturating_add(ordinal as u32),
        ..target
    }
}

pub fn rebuild_system_catalog_indexes(base_dir: &Path) -> Result<(), CatalogError> {
    rebuild_system_catalog_indexes_for_db(base_dir, 1)
}

pub fn rebuild_system_catalog_indexes_for_db(
    base_dir: &Path,
    db_oid: u32,
) -> Result<(), CatalogError> {
    rebuild_system_catalog_indexes_for_db_with_hook(base_dir, db_oid, |_, _, _| Ok(()))
}

fn rebuild_system_catalog_indexes_for_db_with_hook(
    base_dir: &Path,
    db_oid: u32,
    mut before_swap: impl FnMut(
        &CatalogIndexDescriptor,
        RelFileLocator,
        RelFileLocator,
    ) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    // Bootstrap/template-copy path only. Normal catalog writes should preserve
    // existing index relfiles and maintain them incrementally.
    let smgr = MdStorageManager::new(base_dir);
    let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 64));
    let txns = Arc::new(RwLock::new(
        TransactionManager::new_durable(base_dir.to_path_buf()).unwrap_or_default(),
    ));
    let snapshot = txns
        .read()
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|err| CatalogError::Io(format!("system catalog snapshot failed: {err:?}")))?;
    let interrupts = Arc::new(InterruptState::new());
    for (ordinal, descriptor) in system_catalog_indexes().iter().enumerate() {
        let target_rel = system_catalog_index_rel_for_db(*descriptor, db_oid);
        let shadow_rel = system_catalog_index_shadow_rel(*descriptor, db_oid, ordinal);
        pool.with_storage_mut(|storage| {
            storage.smgr.unlink(shadow_rel, None, false);
            Ok::<(), crate::backend::storage::smgr::SmgrError>(())
        })
        .map_err(|err| {
            CatalogError::Io(format!(
                "remove stale shadow system index relfile failed for {}: {err}",
                descriptor.relation_name
            ))
        })?;
        let build_ctx = system_catalog_index_build_context(
            &pool,
            &txns,
            &snapshot,
            &interrupts,
            *descriptor,
            db_oid,
            shadow_rel,
        );
        index_build_stub(&build_ctx, BTREE_AM_OID).map_err(|err| {
            CatalogError::Io(format!(
                "system catalog index build failed for {}: {err:?}",
                descriptor.relation_name
            ))
        })?;
        pool.checkpoint_flush_all(true).map_err(|err| {
            CatalogError::Io(format!(
                "system catalog index shadow flush failed for {}: {err:?}",
                descriptor.relation_name
            ))
        })?;
        before_swap(descriptor, shadow_rel, target_rel)?;
        let _ = pool.invalidate_relation(shadow_rel);
        pool.with_storage_mut(|storage| {
            storage.smgr.immedsync(shadow_rel, ForkNumber::Main)?;
            storage
                .smgr
                .replace_relation_main_fork_from_shadow(shadow_rel, target_rel)
        })
        .map_err(|err| {
            CatalogError::Io(format!(
                "system catalog index shadow swap failed for {}: {err}",
                descriptor.relation_name
            ))
        })?;
        let _ = pool.invalidate_relation(shadow_rel);
        let _ = pool.invalidate_relation(target_rel);
    }
    Ok(())
}

pub fn rebuild_system_catalog_indexes_in_pool(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
) -> Result<(), CatalogError> {
    rebuild_system_catalog_indexes_in_pool_for_db(pool, txns, 1)
}

pub fn rebuild_system_catalog_indexes_in_pool_for_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    db_oid: u32,
) -> Result<(), CatalogError> {
    let snapshot = txns
        .read()
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|err| CatalogError::Io(format!("system catalog snapshot failed: {err:?}")))?;
    let interrupts = Arc::new(InterruptState::new());
    for descriptor in system_catalog_indexes() {
        let build_ctx = system_catalog_index_build_context(
            pool,
            txns,
            &snapshot,
            &interrupts,
            *descriptor,
            db_oid,
            system_catalog_index_rel_for_db(*descriptor, db_oid),
        );
        index_build_stub(&build_ctx, BTREE_AM_OID).map_err(|err| {
            CatalogError::Io(format!(
                "system catalog index build failed for {}: {err:?}",
                descriptor.relation_name
            ))
        })?;
    }
    Ok(())
}

pub fn rebuild_system_catalog_index_in_pool_for_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    db_oid: u32,
    descriptor: CatalogIndexDescriptor,
) -> Result<(), CatalogError> {
    let snapshot = txns
        .read()
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|err| CatalogError::Io(format!("system catalog snapshot failed: {err:?}")))?;
    let interrupts = Arc::new(InterruptState::new());
    let build_ctx = system_catalog_index_build_context(
        pool,
        txns,
        &snapshot,
        &interrupts,
        descriptor,
        db_oid,
        system_catalog_index_rel_for_db(descriptor, db_oid),
    );
    index_build_stub(&build_ctx, BTREE_AM_OID).map_err(|err| {
        CatalogError::Io(format!(
            "system catalog index build failed for {}: {err:?}",
            descriptor.relation_name
        ))
    })?;
    let _ = pool.invalidate_relation(system_catalog_index_rel_for_db(descriptor, db_oid));
    Ok(())
}

fn system_catalog_index_build_context(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    snapshot: &Snapshot,
    interrupts: &Arc<InterruptState>,
    descriptor: CatalogIndexDescriptor,
    db_oid: u32,
    index_relation: RelFileLocator,
) -> IndexBuildContext {
    let heap_relation = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
    IndexBuildContext {
        pool: Arc::clone(pool),
        txns: Arc::clone(txns),
        client_id: 0,
        interrupts: Arc::clone(interrupts),
        snapshot: snapshot.clone(),
        heap_relation,
        heap_desc: crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind),
        heap_toast: None,
        index_relation,
        index_name: descriptor.relation_name.to_string(),
        index_desc: system_catalog_index_desc(descriptor),
        index_meta: system_catalog_index_relcache(descriptor),
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        maintenance_work_mem_kb: 65_536,
        expr_eval: None,
    }
}

pub fn maintain_catalog_indexes_for_insert(
    ctx: &CatalogWriteContext,
    heap_kind: BootstrapCatalogKind,
    heap_tid: ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    maintain_catalog_indexes_for_insert_in_db(ctx, heap_kind, 1, heap_tid, values)
}

pub(crate) struct CatalogIndexInsertState {
    contexts: Vec<IndexInsertContext>,
}

impl CatalogIndexInsertState {
    pub(crate) fn insert(
        &self,
        heap_tid: ItemPointerData,
        values: &[Value],
    ) -> Result<(), CatalogError> {
        self.insert_with_old_tid(heap_tid, None, values)
    }

    pub(crate) fn insert_with_old_tid(
        &self,
        heap_tid: ItemPointerData,
        old_heap_tid: Option<ItemPointerData>,
        values: &[Value],
    ) -> Result<(), CatalogError> {
        for template in &self.contexts {
            let mut insert_ctx = template.clone();
            insert_ctx.heap_tid = heap_tid;
            insert_ctx.old_heap_tid = old_heap_tid;
            insert_ctx.values = values.to_vec();
            crate::backend::access::index::indexam::index_insert_stub(&insert_ctx, BTREE_AM_OID)?;
        }
        Ok(())
    }
}

pub(crate) fn catalog_index_insert_state_for_db(
    ctx: &CatalogWriteContext,
    heap_kind: BootstrapCatalogKind,
    db_oid: u32,
) -> Result<CatalogIndexInsertState, CatalogError> {
    let snapshot = ctx.snapshot_for_command()?;
    let heap_relation = bootstrap_catalog_rel(heap_kind, db_oid);
    let heap_desc = crate::include::catalog::bootstrap_relation_desc(heap_kind);
    let contexts = system_catalog_indexes_for_heap(heap_kind)
        .map(|descriptor| IndexInsertContext {
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            txn_waiter: ctx.waiter.clone(),
            client_id: ctx.client_id,
            interrupts: ctx.interrupts.clone(),
            snapshot: snapshot.clone(),
            heap_relation,
            heap_desc: heap_desc.clone(),
            index_relation: system_catalog_index_rel_for_db(*descriptor, db_oid),
            index_name: descriptor.relation_name.to_string(),
            index_desc: system_catalog_index_desc(*descriptor),
            index_meta: system_catalog_index_relcache(*descriptor),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            heap_tid: ItemPointerData::default(),
            old_heap_tid: None,
            values: Vec::new(),
            unique_check: if descriptor.unique {
                IndexUniqueCheck::Yes
            } else {
                IndexUniqueCheck::No
            },
        })
        .collect();
    Ok(CatalogIndexInsertState { contexts })
}

pub fn maintain_catalog_indexes_for_insert_in_db(
    ctx: &CatalogWriteContext,
    heap_kind: BootstrapCatalogKind,
    db_oid: u32,
    heap_tid: ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    catalog_index_insert_state_for_db(ctx, heap_kind, db_oid)?.insert(heap_tid, values)
}

pub fn probe_system_catalog_rows_visible(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    probe_system_catalog_rows_visible_in_db(
        pool,
        txns,
        snapshot,
        client_id,
        1,
        index_relation_oid,
        key_data,
    )
}

pub(crate) fn probe_system_catalog_tuples_visible_in_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    db_oid: u32,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
) -> Result<Vec<CatalogScannedTuple>, CatalogError> {
    let descriptor = *system_catalog_index_by_oid(index_relation_oid)
        .ok_or(CatalogError::Corrupt("unknown system catalog index"))?;
    let index_desc = system_catalog_index_desc(descriptor);
    let index_meta = system_catalog_index_relcache(descriptor);
    let heap_relation = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
    let scan_ctx = IndexBeginScanContext {
        pool: Arc::clone(pool),
        client_id,
        snapshot: snapshot.clone(),
        heap_relation,
        index_relation: system_catalog_index_rel_for_db(descriptor, db_oid),
        index_desc,
        index_meta,
        key_data,
        order_by_data: Vec::new(),
        direction: ScanDirection::Forward,
        want_itup: false,
    };
    let mut scan = index_beginscan(&scan_ctx, BTREE_AM_OID)?;
    let mut tuples = Vec::new();
    while index_getnext(&mut scan, BTREE_AM_OID)? {
        let Some(tid) = scan.xs_heaptid else {
            continue;
        };
        let Some(tuple) = crate::backend::access::heap::heapam::heap_fetch_visible_with_txns(
            pool,
            client_id,
            scan_ctx.heap_relation,
            tid,
            txns,
            snapshot,
        )
        .map_err(|err| CatalogError::Io(format!("catalog heap fetch failed: {err:?}")))?
        else {
            continue;
        };
        tuples.push(CatalogScannedTuple {
            identity: CatalogTupleIdentity {
                db_oid,
                kind: descriptor.heap_kind,
                heap_rel: heap_relation,
                tid,
            },
            tuple,
        });
    }
    index_endscan(scan, BTREE_AM_OID)?;
    Ok(tuples)
}

pub fn probe_system_catalog_rows_visible_in_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    db_oid: u32,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let tuples = probe_system_catalog_tuples_visible_in_db(
        pool,
        txns,
        snapshot,
        client_id,
        db_oid,
        index_relation_oid,
        key_data,
    )?;
    let mut rows = Vec::with_capacity(tuples.len());
    for scanned in tuples {
        let heap_desc = crate::include::catalog::bootstrap_relation_desc(scanned.identity.kind);
        let txns_guard = txns.read();
        rows.push(
            crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
                pool,
                &txns_guard,
                snapshot,
                client_id,
                scanned.identity.kind,
                scanned.identity.db_oid,
                &heap_desc,
                &scanned.tuple,
            )?,
        );
    }
    Ok(rows)
}

pub fn vacuum_system_catalog_indexes_for_kinds(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    vacuum_system_catalog_indexes_for_kinds_in_db(pool, txns, 1, kinds)
}

pub fn vacuum_system_catalog_indexes_for_kinds_in_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let interrupts = Arc::new(InterruptState::new());
    let mut visited_index_oids = std::collections::BTreeSet::new();
    for &kind in kinds {
        for descriptor in system_catalog_indexes_for_heap(kind) {
            if !visited_index_oids.insert(descriptor.relation_oid) {
                continue;
            }
            let ctx = IndexVacuumContext {
                pool: Arc::clone(pool),
                txns: Arc::clone(txns),
                client_id: 0,
                interrupts: Arc::clone(&interrupts),
                heap_relation: bootstrap_catalog_rel(descriptor.heap_kind, db_oid),
                heap_desc: crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind),
                heap_toast: None,
                index_relation: system_catalog_index_rel_for_db(*descriptor, db_oid),
                index_name: descriptor.relation_name.to_string(),
                index_desc: system_catalog_index_desc(*descriptor),
                index_meta: system_catalog_index_relcache(*descriptor),
                expr_eval: None,
            };
            let scan =
                vacuum_relation_scan(pool, 0, ctx.heap_relation, txns, false).map_err(|err| {
                    CatalogError::Io(format!("catalog heap vacuum scan failed: {err:?}"))
                })?;
            let dead_item_callback = |tid| scan.dead_tids.contains(&tid);
            let stats = index_bulk_delete(&ctx, BTREE_AM_OID, &dead_item_callback, None)?;
            let _ = index_vacuum_cleanup(&ctx, BTREE_AM_OID, Some(stats))?;
        }
    }
    Ok(())
}

pub fn vacuum_system_catalog_heaps_and_indexes_for_kinds_in_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let interrupts = Arc::new(InterruptState::new());
    let mut visited_kinds = std::collections::BTreeSet::new();
    for &kind in kinds {
        if !visited_kinds.insert(kind) {
            continue;
        }

        let heap_relation = bootstrap_catalog_rel(kind, db_oid);
        let scan = vacuum_relation_scan(pool, 0, heap_relation, txns, false)
            .map_err(|err| CatalogError::Io(format!("catalog heap vacuum scan failed: {err:?}")))?;
        if scan.dead_tids.is_empty() {
            continue;
        }

        let mut vacuumed_all_indexes = true;
        for descriptor in system_catalog_indexes_for_heap(kind) {
            let index_relation = system_catalog_index_rel_for_db(*descriptor, db_oid);
            let index_blocks = pool
                .with_storage_mut(|storage| storage.smgr.nblocks(index_relation, ForkNumber::Main))
                .unwrap_or(0);
            if index_blocks == 0 {
                continue;
            }
            let ctx = IndexVacuumContext {
                pool: Arc::clone(pool),
                txns: Arc::clone(txns),
                client_id: 0,
                interrupts: Arc::clone(&interrupts),
                heap_relation,
                heap_desc: crate::include::catalog::bootstrap_relation_desc(kind),
                heap_toast: None,
                index_relation,
                index_name: descriptor.relation_name.to_string(),
                index_desc: system_catalog_index_desc(*descriptor),
                index_meta: system_catalog_index_relcache(*descriptor),
                expr_eval: None,
            };
            let dead_item_callback = |tid| scan.dead_tids.contains(&tid);
            let Ok(stats) = index_bulk_delete(&ctx, BTREE_AM_OID, &dead_item_callback, None) else {
                vacuumed_all_indexes = false;
                break;
            };
            if index_vacuum_cleanup(&ctx, BTREE_AM_OID, Some(stats)).is_err() {
                vacuumed_all_indexes = false;
                break;
            }
        }
        if !vacuumed_all_indexes {
            continue;
        }

        vacuum_relation_pages(
            pool,
            0,
            heap_relation,
            kind.relation_oid(),
            txns,
            &scan,
            None,
            true,
        )
        .map_err(|err| CatalogError::Io(format!("catalog heap vacuum failed: {err:?}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend::catalog::store::CatalogStore;
    use crate::backend::storage::smgr::segment_path;
    use crate::include::catalog::C_COLLATION_OID;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pgrust_catalog_indexing_{label}_{nanos}"))
    }

    #[test]
    fn durable_rebuild_failure_keeps_existing_index_and_cleans_shadow_on_retry() {
        let base = temp_dir("shadow_failure");
        let _store = CatalogStore::load_database(&base, 1).unwrap();
        let descriptor = system_catalog_indexes()[0];
        let target_rel = system_catalog_index_rel_for_db(descriptor, 1);
        let shadow_rel = system_catalog_index_shadow_rel(descriptor, 1, 0);
        let target_path = segment_path(&base, target_rel, ForkNumber::Main, 0);
        let shadow_path = segment_path(&base, shadow_rel, ForkNumber::Main, 0);
        let before = fs::read(&target_path).unwrap();

        let err =
            rebuild_system_catalog_indexes_for_db_with_hook(&base, 1, |current, shadow, target| {
                if current.relation_oid == descriptor.relation_oid {
                    assert_eq!(shadow, shadow_rel);
                    assert_eq!(target, target_rel);
                    Err(CatalogError::Io("injected rebuild failure".into()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(matches!(err, CatalogError::Io(_)));
        assert_eq!(fs::read(&target_path).unwrap(), before);
        assert!(
            shadow_path.exists(),
            "failed rebuild should leave the shadow relfile for retry cleanup"
        );

        CatalogStore::load_database(&base, 1).unwrap();
        rebuild_system_catalog_indexes_for_db(&base, 1).unwrap();
        assert_eq!(fs::read(&target_path).unwrap(), before);
        assert!(
            !shadow_path.exists(),
            "successful retry should consume the stale shadow relfile"
        );
    }

    #[test]
    fn system_catalog_name_indexes_use_c_collation() {
        let descriptor = *system_catalog_indexes()
            .iter()
            .find(|descriptor| descriptor.relation_name == "pg_class_relname_nsp_index")
            .unwrap();

        let desc = system_catalog_index_desc(descriptor);
        assert_eq!(desc.columns[0].collation_oid, C_COLLATION_OID);

        let meta = system_catalog_index_meta(descriptor);
        assert_eq!(meta.indcollation, vec![C_COLLATION_OID, 0]);
    }
}
