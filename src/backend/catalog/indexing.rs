use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::heap::vacuumlazy::vacuum_relation_scan;
use crate::backend::access::index::indexam::{
    index_beginscan, index_build_stub, index_bulk_delete, index_endscan, index_getnext,
    index_vacuum_cleanup,
};
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, CatalogError, CatalogIndexMeta};
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::executor::RelationDesc;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::backend::utils::time::snapmgr::Snapshot;
use crate::include::access::amapi::{
    IndexBeginScanContext, IndexBuildContext, IndexInsertContext, IndexUniqueCheck,
    IndexVacuumContext,
};
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, BootstrapCatalogKind, CatalogIndexDescriptor,
    PG_CATALOG_NAMESPACE_OID, system_catalog_index_by_oid, system_catalog_indexes,
    system_catalog_indexes_for_heap,
};
use crate::include::nodes::datum::Value;

const SYSTEM_CATALOG_INDEX_SHADOW_REL_NUMBER_BASE: u32 = 0xF000_0000;

pub fn insert_bootstrap_system_indexes(catalog: &mut Catalog) {
    for descriptor in system_catalog_indexes() {
        if catalog.get_by_oid(descriptor.relation_oid).is_some() {
            continue;
        }
        let entry = system_catalog_index_entry(*descriptor);
        catalog.insert(descriptor.relation_name, entry);
    }
}

pub fn system_catalog_index_entry(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
) -> CatalogEntry {
    system_catalog_index_entry_for_db(descriptor, 1)
}

pub fn system_catalog_index_entry_for_db(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
    db_oid: u32,
) -> CatalogEntry {
    CatalogEntry {
        rel: system_catalog_index_rel(descriptor, db_oid),
        relation_oid: descriptor.relation_oid,
        namespace_oid: PG_CATALOG_NAMESPACE_OID,
        owner_oid: BOOTSTRAP_SUPERUSER_OID,
        relacl: None,
        reloptions: None,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'i',
        am_oid: BTREE_AM_OID,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: system_catalog_index_desc(descriptor),
        partitioned_table: None,
        index_meta: Some(system_catalog_index_meta(descriptor)),
    }
}

fn system_catalog_index_rel(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
    db_oid: u32,
) -> RelFileLocator {
    let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
    RelFileLocator {
        spc_oid: heap_rel.spc_oid,
        db_oid: heap_rel.db_oid,
        rel_number: descriptor.relation_oid,
    }
}

fn system_catalog_index_shadow_rel(
    descriptor: CatalogIndexDescriptor,
    db_oid: u32,
    ordinal: usize,
) -> RelFileLocator {
    let target = system_catalog_index_rel(descriptor, db_oid);
    RelFileLocator {
        rel_number: SYSTEM_CATALOG_INDEX_SHADOW_REL_NUMBER_BASE.saturating_add(ordinal as u32),
        ..target
    }
}

/// Public accessor so that the wasm ephemeral bootstrap path can resolve
/// system-catalog index locators with the correct shared/db scope.
pub fn system_catalog_index_rel_for_db(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
    db_oid: u32,
) -> RelFileLocator {
    system_catalog_index_rel(descriptor, db_oid)
}

pub fn system_catalog_index_desc(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
) -> RelationDesc {
    let heap_desc = crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind);
    let columns = descriptor
        .key_attnums
        .iter()
        .map(|attnum| {
            heap_desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .cloned()
                .ok_or(CatalogError::Corrupt(
                    "system catalog index key out of range",
                ))
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("valid system catalog index descriptors");
    RelationDesc { columns }
}

pub fn system_catalog_index_meta(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
) -> CatalogIndexMeta {
    CatalogIndexMeta {
        indrelid: descriptor.heap_kind.relation_oid(),
        indkey: descriptor.key_attnums.to_vec(),
        indisunique: descriptor.unique,
        indnullsnotdistinct: false,
        indisprimary: false,
        indisexclusion: false,
        indimmediate: true,
        indisvalid: true,
        indisready: true,
        indislive: true,
        indclass: descriptor.opclass_oids.to_vec(),
        indcollation: vec![0; descriptor.key_attnums.len()],
        indoption: vec![0; descriptor.key_attnums.len()],
        indexprs: None,
        indpred: None,
        brin_options: None,
        gin_options: None,
        hash_options: None,
    }
}

pub fn system_catalog_index_relcache(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
) -> IndexRelCacheEntry {
    let meta = system_catalog_index_meta(descriptor);
    IndexRelCacheEntry {
        indexrelid: descriptor.relation_oid,
        indrelid: meta.indrelid,
        indnatts: meta.indkey.len() as i16,
        indnkeyatts: meta.indclass.len() as i16,
        indisunique: meta.indisunique,
        indnullsnotdistinct: false,
        indisprimary: false,
        indisexclusion: false,
        indimmediate: true,
        indisclustered: false,
        indisvalid: true,
        indcheckxmin: false,
        indisready: true,
        indislive: true,
        indisreplident: false,
        am_oid: BTREE_AM_OID,
        am_handler_oid: None,
        indkey: meta.indkey,
        indclass: meta.indclass,
        indcollation: meta.indcollation,
        indoption: meta.indoption,
        opfamily_oids: Vec::new(),
        opcintype_oids: Vec::new(),
        opckeytype_oids: Vec::new(),
        amop_entries: Vec::new(),
        amproc_entries: Vec::new(),
        indexprs: None,
        indpred: None,
        rd_indexprs: None,
        rd_indpred: None,
        brin_options: None,
        gin_options: None,
        hash_options: None,
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
        let target_rel = system_catalog_index_rel(*descriptor, db_oid);
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
            system_catalog_index_rel(*descriptor, db_oid),
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
    heap_tid: crate::include::access::itemptr::ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    maintain_catalog_indexes_for_insert_in_db(ctx, heap_kind, 1, heap_tid, values)
}

pub fn maintain_catalog_indexes_for_insert_in_db(
    ctx: &CatalogWriteContext,
    heap_kind: BootstrapCatalogKind,
    db_oid: u32,
    heap_tid: crate::include::access::itemptr::ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|err| CatalogError::Io(format!("catalog snapshot failed: {err:?}")))?;
    for descriptor in system_catalog_indexes_for_heap(heap_kind) {
        let heap_relation = bootstrap_catalog_rel(heap_kind, db_oid);
        let insert_ctx = IndexInsertContext {
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            txn_waiter: ctx.waiter.clone(),
            client_id: ctx.client_id,
            interrupts: ctx.interrupts.clone(),
            snapshot: snapshot.clone(),
            heap_relation,
            heap_desc: crate::include::catalog::bootstrap_relation_desc(heap_kind),
            index_relation: system_catalog_index_rel(*descriptor, db_oid),
            index_name: descriptor.relation_name.to_string(),
            index_desc: system_catalog_index_desc(*descriptor),
            index_meta: system_catalog_index_relcache(*descriptor),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            heap_tid,
            values: values.to_vec(),
            unique_check: if descriptor.unique {
                IndexUniqueCheck::Yes
            } else {
                IndexUniqueCheck::No
            },
        };
        crate::backend::access::index::indexam::index_insert_stub(&insert_ctx, BTREE_AM_OID)?;
    }
    Ok(())
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

pub fn probe_system_catalog_rows_visible_in_db(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    db_oid: u32,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let descriptor = *system_catalog_index_by_oid(index_relation_oid)
        .ok_or(CatalogError::Corrupt("unknown system catalog index"))?;
    let heap_desc = crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind);
    let index_desc = system_catalog_index_desc(descriptor);
    let index_meta = system_catalog_index_relcache(descriptor);
    let scan_ctx = IndexBeginScanContext {
        pool: Arc::clone(pool),
        client_id,
        snapshot: snapshot.clone(),
        heap_relation: bootstrap_catalog_rel(descriptor.heap_kind, db_oid),
        index_relation: system_catalog_index_rel(descriptor, db_oid),
        index_desc,
        index_meta,
        key_data,
        order_by_data: Vec::new(),
        direction: ScanDirection::Forward,
        want_itup: false,
    };
    let mut scan = index_beginscan(&scan_ctx, BTREE_AM_OID)?;
    let mut rows = Vec::new();
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
        rows.push(
            crate::backend::catalog::rowcodec::decode_catalog_tuple_values(&heap_desc, &tuple)?,
        );
    }
    index_endscan(scan, BTREE_AM_OID)?;
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
                index_relation: system_catalog_index_rel(*descriptor, db_oid),
                index_name: descriptor.relation_name.to_string(),
                index_desc: system_catalog_index_desc(*descriptor),
                index_meta: system_catalog_index_relcache(*descriptor),
            };
            let scan = vacuum_relation_scan(pool, 0, ctx.heap_relation, txns).map_err(|err| {
                CatalogError::Io(format!("catalog heap vacuum scan failed: {err:?}"))
            })?;
            let dead_item_callback = |tid| scan.dead_tids.contains(&tid);
            let stats = index_bulk_delete(&ctx, BTREE_AM_OID, &dead_item_callback, None)?;
            let _ = index_vacuum_cleanup(&ctx, BTREE_AM_OID, Some(stats))?;
        }
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
        let target_rel = system_catalog_index_rel(descriptor, 1);
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
}
