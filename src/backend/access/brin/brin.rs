use crate::backend::catalog::CatalogError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{BLCKSZ, ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::include::access::relscan::{
    BrinIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::{BufferPool, ClientId, PinnedBuffer};

use super::pageops::{brin_metapage_data, brin_metapage_init};

fn pin_brin_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("brin pin block failed: {err:?}")))
}

fn read_brin_metapage(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<crate::include::access::brin_page::BrinMetaPageData, CatalogError> {
    let pin = pin_brin_block(
        pool,
        client_id,
        rel,
        crate::include::access::brin_page::BRIN_METAPAGE_BLKNO,
    )?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("brin shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    brin_metapage_data(&page)
}

fn brin_pages_per_range_from_meta(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Result<u32, CatalogError> {
    let pages_per_range = index_meta
        .brin_options
        .as_ref()
        .map(|options| options.pages_per_range)
        .ok_or(CatalogError::Corrupt("BRIN index metadata missing brin_options"))?;
    if pages_per_range == 0 {
        return Err(CatalogError::Corrupt(
            "BRIN index metadata has invalid pages_per_range",
        ));
    }
    Ok(pages_per_range)
}

pub fn brin_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 5,
        amsupport: 4,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: false,
        amsummarizing: true,
        ambuild: Some(brinbuild),
        ambuildempty: Some(brinbuildempty),
        aminsert: Some(brininsert),
        ambeginscan: Some(brinbeginscan),
        amrescan: Some(brinrescan),
        amgettuple: None,
        amgetbitmap: Some(bringetbitmap),
        amendscan: Some(brinendscan),
        ambulkdelete: Some(brinbulkdelete),
        amvacuumcleanup: Some(brinvacuumcleanup),
    }
}

pub(crate) fn brinbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    brinbuildempty(&IndexBuildEmptyContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        xid: ctx.snapshot.current_xid,
        index_relation: ctx.index_relation,
        index_desc: ctx.index_desc.clone(),
        index_meta: ctx.index_meta.clone(),
    })?;
    let heap_blocks = ctx
        .pool
        .with_storage_mut(|storage| storage.smgr.nblocks(ctx.heap_relation, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(format!("brin heap nblocks failed: {err:?}")))?;
    if heap_blocks == 0 {
        return Ok(IndexBuildResult::default());
    }
    Err(CatalogError::Io(
        "BRIN build implementation not yet wired".into(),
    ))
}

pub(crate) fn brinbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    let pages_per_range = brin_pages_per_range_from_meta(&ctx.index_meta)?;
    ctx.pool
        .ensure_relation_fork(ctx.index_relation, ForkNumber::Main)
        .map_err(|err| CatalogError::Io(format!("brin ensure relation failed: {err:?}")))?;
    ctx.pool
        .with_storage_mut(|storage| {
            storage.smgr.truncate(ctx.index_relation, ForkNumber::Main, 0)?;
            let mut metapage = [0u8; BLCKSZ];
            brin_metapage_init(
                &mut metapage,
                pages_per_range,
                crate::include::access::brin_page::BRIN_CURRENT_VERSION,
            )
            .map_err(|err| {
                crate::backend::storage::smgr::SmgrError::Io(std::io::Error::other(format!(
                    "{err:?}"
                )))
            })?;
            storage
                .smgr
                .extend(ctx.index_relation, ForkNumber::Main, 0, &metapage, true)?;
            Ok::<(), crate::backend::storage::smgr::SmgrError>(())
        })
        .map_err(|err| CatalogError::Io(format!("brin buildempty failed: {err:?}")))
}

pub(crate) fn brininsert(_ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    Err(CatalogError::Io(
        "BRIN insert implementation not yet wired".into(),
    ))
}

pub(crate) fn brinbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    // :HACK: Durable BRIN reloptions are not persisted separately yet, so scan
    // setup reads the metapage first and only falls back to relcache metadata
    // immediately after a local build.
    let pages_per_range = read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?;
    Ok(IndexScanDesc {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: Some(ctx.heap_relation),
        index_relation: ctx.index_relation,
        index_desc: ctx.index_desc.clone(),
        index_meta: ctx.index_meta.clone(),
        indoption: ctx.index_meta.indoption.clone(),
        number_of_keys: ctx.key_data.len(),
        key_data: ctx.key_data.clone(),
        number_of_order_bys: ctx.order_by_data.len(),
        order_by_data: ctx.order_by_data.clone(),
        direction: ctx.direction,
        xs_want_itup: ctx.want_itup,
        xs_itup: None,
        xs_heaptid: None,
        xs_recheck: false,
        xs_recheck_order_by: false,
        xs_orderby_values: vec![None; ctx.order_by_data.len()],
        opaque: IndexScanOpaque::Brin(BrinIndexScanOpaque {
            pages_per_range,
            current_range_start: None,
            next_revmap_page: 1,
            next_revmap_index: 0,
            scan_started: false,
        }),
    })
}

pub(crate) fn brinrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    scan.xs_recheck = false;
    scan.xs_recheck_order_by = false;
    for value in &mut scan.xs_orderby_values {
        *value = None;
    }
    let IndexScanOpaque::Brin(state) = &mut scan.opaque else {
        return Err(CatalogError::Corrupt("BRIN scan state missing opaque"));
    };
    state.current_range_start = None;
    state.next_revmap_page = 1;
    state.next_revmap_index = 0;
    state.scan_started = false;
    Ok(())
}

pub(crate) fn bringetbitmap(
    _scan: &mut IndexScanDesc,
    _bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    Err(CatalogError::Io(
        "BRIN getbitmap implementation not yet wired".into(),
    ))
}

pub(crate) fn brinendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}

pub(crate) fn brinbulkdelete(
    _ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

pub(crate) fn brinvacuumcleanup(
    _ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend::storage::smgr::md::MdStorageManager;
    use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
    use crate::include::access::brin::BrinOptions;
    use crate::include::catalog::BRIN_AM_OID;
    use crate::include::nodes::primnodes::RelationDesc;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pgrust_brin_{label}_{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn test_rel(rel_number: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number,
        }
    }

    fn test_index_meta(pages_per_range: u32) -> IndexRelCacheEntry {
        IndexRelCacheEntry {
            indexrelid: 42,
            indrelid: 41,
            indnatts: 0,
            indnkeyatts: 0,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: false,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            am_oid: BRIN_AM_OID,
            am_handler_oid: None,
            indkey: Vec::new(),
            indclass: Vec::new(),
            indcollation: Vec::new(),
            indoption: Vec::new(),
            opfamily_oids: Vec::new(),
            opcintype_oids: Vec::new(),
            opckeytype_oids: Vec::new(),
            amop_entries: Vec::new(),
            amproc_entries: Vec::new(),
            indexprs: None,
            indpred: None,
            brin_options: Some(BrinOptions { pages_per_range }),
        }
    }

    #[test]
    fn brinbuildempty_writes_postgres_shaped_metapage() {
        let base = temp_dir("buildempty_metapage");
        let pool = Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(&base)),
            4,
        ));
        let rel = test_rel(7000);

        brinbuildempty(&IndexBuildEmptyContext {
            pool: Arc::clone(&pool),
            client_id: 0,
            xid: 1,
            index_relation: rel,
            index_desc: RelationDesc {
                columns: Vec::new(),
            },
            index_meta: test_index_meta(32),
        })
        .unwrap();

        let meta = read_brin_metapage(&pool, 0, rel).unwrap();
        assert_eq!(meta.pages_per_range, 32);
        assert_eq!(meta.last_revmap_page, 0);
        assert_eq!(meta.brin_magic, crate::include::access::brin_page::BRIN_META_MAGIC);
        assert_eq!(
            meta.brin_version,
            crate::include::access::brin_page::BRIN_CURRENT_VERSION
        );
    }

    #[test]
    fn brinbeginscan_prefers_metapage_pages_per_range() {
        let base = temp_dir("beginscan_metapage");
        let pool = Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(&base)),
            4,
        ));
        let rel = test_rel(7001);

        brinbuildempty(&IndexBuildEmptyContext {
            pool: Arc::clone(&pool),
            client_id: 0,
            xid: 1,
            index_relation: rel,
            index_desc: RelationDesc {
                columns: Vec::new(),
            },
            index_meta: test_index_meta(32),
        })
        .unwrap();

        let scan = brinbeginscan(&IndexBeginScanContext {
            pool,
            client_id: 0,
            snapshot: crate::backend::utils::time::snapmgr::Snapshot::bootstrap(),
            heap_relation: test_rel(8000),
            index_relation: rel,
            index_desc: RelationDesc {
                columns: Vec::new(),
            },
            index_meta: test_index_meta(64),
            key_data: Vec::new(),
            order_by_data: Vec::new(),
            direction: ScanDirection::Forward,
            want_itup: false,
        })
        .unwrap();

        let IndexScanOpaque::Brin(opaque) = scan.opaque else {
            panic!("expected BRIN opaque state");
        };
        assert_eq!(opaque.pages_per_range, 32);
        assert_eq!(opaque.next_revmap_page, 1);
        assert!(!opaque.scan_started);
    }
}
