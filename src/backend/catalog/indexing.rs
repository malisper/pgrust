use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::index::indexam::{
    index_beginscan, index_build_stub, index_endscan, index_getnext,
};
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
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
};
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, BootstrapCatalogKind, PG_CATALOG_NAMESPACE_OID,
    system_catalog_index_by_oid, system_catalog_indexes, system_catalog_indexes_for_heap,
};
use crate::include::nodes::datum::Value;

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
    CatalogEntry {
        rel: RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: descriptor.relation_oid,
        },
        relation_oid: descriptor.relation_oid,
        namespace_oid: PG_CATALOG_NAMESPACE_OID,
        owner_oid: BOOTSTRAP_SUPERUSER_OID,
        row_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'i',
        relpages: 0,
        reltuples: 0.0,
        desc: system_catalog_index_desc(descriptor),
        index_meta: Some(system_catalog_index_meta(descriptor)),
    }
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
        indisprimary: false,
        indisvalid: true,
        indisready: true,
        indislive: true,
        indclass: descriptor.opclass_oids.to_vec(),
        indcollation: vec![0; descriptor.key_attnums.len()],
        indoption: vec![0; descriptor.key_attnums.len()],
        indexprs: None,
        indpred: None,
    }
}

pub fn system_catalog_index_relcache(
    descriptor: crate::include::catalog::CatalogIndexDescriptor,
) -> IndexRelCacheEntry {
    let meta = system_catalog_index_meta(descriptor);
    IndexRelCacheEntry {
        indrelid: meta.indrelid,
        indnatts: meta.indkey.len() as i16,
        indnkeyatts: meta.indkey.len() as i16,
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
        indexprs: None,
        indpred: None,
    }
}

pub fn rebuild_system_catalog_indexes(base_dir: &Path) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for descriptor in system_catalog_indexes() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: descriptor.relation_oid,
        };
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(format!("open system index relfile failed: {e}")))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(format!("create system index relfile failed: {e}")))?;
    }

    let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 64));
    let txns = Arc::new(RwLock::new(
        TransactionManager::new_durable(base_dir.to_path_buf()).unwrap_or_default(),
    ));
    let snapshot = txns
        .read()
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|err| CatalogError::Io(format!("system catalog snapshot failed: {err:?}")))?;
    let interrupts = Arc::new(InterruptState::new());
    for descriptor in system_catalog_indexes() {
        let build_ctx = IndexBuildContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            client_id: 0,
            interrupts: Arc::clone(&interrupts),
            snapshot: snapshot.clone(),
            heap_relation: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: descriptor.heap_kind.relation_oid(),
            },
            heap_desc: crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind),
            index_relation: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: descriptor.relation_oid,
            },
            index_name: descriptor.relation_name.to_string(),
            index_desc: system_catalog_index_desc(*descriptor),
            index_meta: system_catalog_index_relcache(*descriptor),
            maintenance_work_mem_kb: 65_536,
        };
        index_build_stub(&build_ctx, BTREE_AM_OID).map_err(|err| {
            CatalogError::Io(format!(
                "system catalog index build failed for {}: {err:?}",
                descriptor.relation_name
            ))
        })?;
    }
    Ok(())
}

pub fn maintain_catalog_indexes_for_insert(
    ctx: &CatalogWriteContext,
    heap_kind: BootstrapCatalogKind,
    heap_tid: crate::include::access::itemptr::ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|err| CatalogError::Io(format!("catalog snapshot failed: {err:?}")))?;
    for descriptor in system_catalog_indexes_for_heap(heap_kind) {
        let insert_ctx = IndexInsertContext {
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            txn_waiter: ctx.waiter.clone(),
            client_id: ctx.client_id,
            interrupts: ctx.interrupts.clone(),
            snapshot: snapshot.clone(),
            heap_relation: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: heap_kind.relation_oid(),
            },
            heap_desc: crate::include::catalog::bootstrap_relation_desc(heap_kind),
            index_relation: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: descriptor.relation_oid,
            },
            index_name: descriptor.relation_name.to_string(),
            index_desc: system_catalog_index_desc(*descriptor),
            index_meta: system_catalog_index_relcache(*descriptor),
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
    let descriptor = *system_catalog_index_by_oid(index_relation_oid)
        .ok_or(CatalogError::Corrupt("unknown system catalog index"))?;
    let heap_desc = crate::include::catalog::bootstrap_relation_desc(descriptor.heap_kind);
    let index_desc = system_catalog_index_desc(descriptor);
    let index_meta = system_catalog_index_relcache(descriptor);
    let scan_ctx = IndexBeginScanContext {
        pool: Arc::clone(pool),
        client_id,
        snapshot: snapshot.clone(),
        heap_relation: RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: descriptor.heap_kind.relation_oid(),
        },
        index_relation: RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: descriptor.relation_oid,
        },
        index_desc,
        index_meta,
        key_data,
        direction: ScanDirection::Forward,
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
