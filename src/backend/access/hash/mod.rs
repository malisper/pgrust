pub(crate) mod support;
pub mod wal;

use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values,
};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::access::transam::xlog::{
    XLOG_HASH_ADD_OVFL_PAGE, XLOG_HASH_DELETE, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_INSERT,
    XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_PAGE, XLOG_HASH_VACUUM,
};
use crate::backend::access::{RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::fsm::{get_free_index_page, record_free_index_page};
use crate::backend::storage::page::bufpage::{
    ItemIdFlags, PageError, page_add_item, page_get_item, page_get_item_id,
    page_get_max_offset_number, page_remove_item,
};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::check_for_interrupts;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::include::access::hash::{
    HASH_INVALID_BLOCK, HASH_MAX_BUCKETS, HASH_METAPAGE, HashMetaPageData, HashPageError,
    LH_BUCKET_PAGE, LH_OVERFLOW_PAGE, LH_UNUSED_PAGE, hash_build_bucket_count,
    hash_fillfactor_from_meta, hash_metapage_data, hash_metapage_init, hash_metapage_set,
    hash_opclass_for_first_key, hash_page_get_opaque, hash_page_has_items, hash_page_has_space,
    hash_page_init, hash_page_items, hash_page_set_opaque, hash_split_needed, hash_tuple_hash,
    hash_tuple_key_values,
};
use crate::include::access::htup::AttributeCompression;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::access::relscan::{
    HashIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use crate::{BufferPool, ClientId, PinnedBuffer, SmgrStorageBackend};
use pgrust_access::{
    AccessError, AccessHeapServices, AccessIndexServices, AccessScalarServices, AccessWalBlockRef,
    AccessWalRecord, AccessWalServices,
};

pub(crate) use support::{
    HASH_PARTITION_SEED, hash_bytes_extended, hash_combine64, hash_value_extended,
};

fn hash_insert_mutex() -> &'static parking_lot::Mutex<()> {
    static HASH_INSERT_MUTEX: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    HASH_INSERT_MUTEX.get_or_init(|| parking_lot::Mutex::new(()))
}

fn check_catalog_interrupts(
    interrupts: &crate::backend::utils::misc::interrupts::InterruptState,
) -> Result<(), CatalogError> {
    check_for_interrupts(interrupts).map_err(CatalogError::Interrupted)
}

fn page_error(err: HashPageError) -> CatalogError {
    CatalogError::Io(format!("hash page error: {err:?}"))
}

fn slotted_page_error(err: PageError) -> CatalogError {
    CatalogError::Io(format!("hash slotted page error: {err:?}"))
}

fn pin_hash_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("hash pin block failed: {err:?}")))
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(format!("hash nblocks failed: {err:?}")))
}

fn read_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    let pin = pin_hash_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("hash shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

struct HashPageImage {
    block: u32,
    page: [u8; crate::backend::storage::smgr::BLCKSZ],
    wal_info: u8,
    will_init: bool,
}

const HASH_MAX_PAGES_PER_WAL_RECORD: usize = 200;

fn write_hash_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    pages: &[HashPageImage],
) -> Result<(), CatalogError> {
    if pages.len() > HASH_MAX_PAGES_PER_WAL_RECORD {
        for chunk in pages.chunks(HASH_MAX_PAGES_PER_WAL_RECORD) {
            write_hash_pages(pool, client_id, xid, rel, chunk)?;
        }
        return Ok(());
    }

    for page in pages {
        pool.ensure_block_exists(rel, ForkNumber::Main, page.block)
            .map_err(|err| CatalogError::Io(format!("hash extend failed: {err:?}")))?;
    }
    let blocks = pages
        .iter()
        .map(|page| {
            let mut flags = pgrust_core::REGBUF_STANDARD | pgrust_core::REGBUF_FORCE_IMAGE;
            if page.will_init {
                flags |= pgrust_core::REGBUF_WILL_INIT;
            }
            AccessWalBlockRef {
                tag: crate::backend::storage::buffer::BufferTag {
                    rel,
                    fork: ForkNumber::Main,
                    block: page.block,
                },
                flags,
                data: page.page.to_vec(),
            }
        })
        .collect::<Vec<_>>();
    let wal_info = pages
        .first()
        .map(|page| page.wal_info)
        .unwrap_or(XLOG_HASH_INSERT);
    let lsn = RootAccessWal { pool }
        .log_access_record(AccessWalRecord {
            xid,
            rmid: pgrust_core::RM_HASH_ID,
            info: wal_info,
            payload: Vec::new(),
            blocks,
        })
        .map_err(map_access_error)?;
    for page in pages {
        let pin = pin_hash_block(pool, client_id, rel, page.block)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("hash exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), &page.page, lsn, &mut guard)
            .map_err(|err| CatalogError::Io(format!("hash buffered write failed: {err:?}")))?;
    }
    Ok(())
}

fn init_hash_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    bucket_count: u32,
    fillfactor: u16,
) -> Result<(), CatalogError> {
    let meta = HashMetaPageData::new(bucket_count, fillfactor);
    let mut pages = Vec::new();
    let mut meta_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    hash_metapage_init(&mut meta_page, &meta);
    pages.push(HashPageImage {
        block: HASH_METAPAGE,
        page: meta_page,
        wal_info: XLOG_HASH_INIT_META_PAGE,
        will_init: true,
    });
    for bucket in 0..meta.bucket_count() {
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        hash_page_init(&mut page, bucket, LH_BUCKET_PAGE);
        pages.push(HashPageImage {
            block: bucket + 1,
            page,
            wal_info: XLOG_HASH_INIT_META_PAGE,
            will_init: true,
        });
    }
    write_hash_pages(pool, client_id, xid, rel, &pages)
}

fn read_meta(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<HashMetaPageData, CatalogError> {
    let page = read_page(pool, client_id, rel, HASH_METAPAGE)?;
    hash_metapage_data(&page).map_err(page_error)
}

fn write_meta(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &HashMetaPageData,
    wal_info: u8,
) -> Result<(), CatalogError> {
    let mut page = read_page(pool, client_id, rel, HASH_METAPAGE)?;
    hash_metapage_set(&mut page, meta).map_err(page_error)?;
    write_hash_pages(
        pool,
        client_id,
        xid,
        rel,
        &[HashPageImage {
            block: HASH_METAPAGE,
            page,
            wal_info,
            will_init: false,
        }],
    )
}

fn bulk_load_hash_index(
    ctx: &IndexBuildContext,
    fillfactor: u16,
    pending: Vec<(ItemPointerData, Vec<Value>)>,
) -> Result<IndexBuildResult, CatalogError> {
    let mut result = IndexBuildResult {
        heap_tuples: pending.len() as u64,
        ..IndexBuildResult::default()
    };
    let mut meta = HashMetaPageData::new(
        hash_build_bucket_count(pending.len(), fillfactor),
        fillfactor,
    );
    let mut buckets = vec![Vec::new(); meta.bucket_count() as usize];

    for (tid, key_values) in pending {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        let Some(first) = key_values.first() else {
            return Err(CatalogError::Corrupt("hash index missing key value"));
        };
        let Some(hash) = RootAccessServices
            .hash_index_value(first, hash_opclass_for_first_key(&ctx.index_meta))
            .map_err(map_access_error)?
        else {
            continue;
        };
        let payload = encode_hash_tuple_payload(
            &ctx.index_desc,
            &key_values,
            hash,
            ctx.default_toast_compression,
        )?;
        let bucket = meta.bucket_for_hash(hash) as usize;
        buckets[bucket].push(IndexTupleData::new_raw(tid, false, true, false, payload));
        result.index_tuples += 1;
    }

    meta.hashm_ntuples = result.index_tuples;
    init_hash_relation(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        meta.bucket_count(),
        fillfactor,
    )?;

    let mut reserved_blocks = (HASH_METAPAGE..=meta.bucket_count()).collect::<BTreeSet<_>>();
    let mut images = Vec::new();
    let mut meta_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    hash_metapage_init(&mut meta_page, &meta);
    images.push(HashPageImage {
        block: HASH_METAPAGE,
        page: meta_page,
        wal_info: XLOG_HASH_INIT_META_PAGE,
        will_init: false,
    });

    for (bucket, items) in buckets.iter().enumerate() {
        if items.is_empty() {
            continue;
        }
        let primary_block = meta
            .bucket_block(bucket as u32)
            .ok_or(CatalogError::Corrupt("hash bucket block missing"))?;
        let (bucket_images, _) = build_bucket_chain_images(
            &ctx.pool,
            ctx.index_relation,
            bucket as u32,
            primary_block,
            false,
            &[],
            items,
            XLOG_HASH_INSERT,
            &mut reserved_blocks,
        )?;
        images.extend(bucket_images);
    }

    write_hash_pages(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        &images,
    )?;
    Ok(result)
}

fn encode_hash_tuple_payload(
    desc: &RelationDesc,
    key_values: &[Value],
    hash: u32,
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    crate::include::access::hash::encode_hash_tuple_payload(
        desc,
        key_values,
        hash,
        default_toast_compression,
        &RootAccessServices,
    )
    .map_err(map_access_error)
}

fn tuple_hash(tuple: &IndexTupleData) -> Result<u32, CatalogError> {
    hash_tuple_hash(tuple).map_err(map_access_error)
}

fn tuple_key_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    hash_tuple_key_values(desc, tuple, &RootAccessServices).map_err(map_access_error)
}

fn allocate_hash_block(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(u32, bool), CatalogError> {
    loop {
        if let Some(block) = get_free_index_page(pool, rel).map_err(CatalogError::Io)? {
            if block > HASH_METAPAGE {
                return Ok((block, false));
            }
            continue;
        }
        return Ok((relation_nblocks(pool, rel)?, true));
    }
}

fn allocate_hash_block_reserved(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    reserved: &mut BTreeSet<u32>,
) -> Result<(u32, bool), CatalogError> {
    loop {
        if let Some(block) = get_free_index_page(pool, rel).map_err(CatalogError::Io)? {
            if block > HASH_METAPAGE && reserved.insert(block) {
                return Ok((block, false));
            }
            continue;
        }
        let mut block = relation_nblocks(pool, rel)?;
        while !reserved.insert(block) {
            block = block.saturating_add(1);
        }
        return Ok((block, true));
    }
}

fn build_bucket_chain_images(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    bucket: u32,
    primary_block: u32,
    primary_will_init: bool,
    reusable_overflows: &[u32],
    items: &[IndexTupleData],
    wal_info: u8,
    reserved: &mut BTreeSet<u32>,
) -> Result<(Vec<HashPageImage>, Vec<u32>), CatalogError> {
    let mut images = Vec::new();
    let mut reuse_index = 0usize;
    let mut current_block = primary_block;
    let mut current_will_init = primary_will_init;
    let mut current_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    hash_page_init(&mut current_page, bucket, LH_BUCKET_PAGE);

    for tuple in items {
        if !hash_page_has_space(&current_page, tuple).map_err(map_access_error)?
            && hash_page_has_items(&current_page).map_err(map_access_error)?
        {
            let (next_block, next_will_init) =
                if let Some(block) = reusable_overflows.get(reuse_index).copied() {
                    reuse_index += 1;
                    reserved.insert(block);
                    (block, false)
                } else {
                    allocate_hash_block_reserved(pool, rel, reserved)?
                };
            let mut current_opaque = hash_page_get_opaque(&current_page).map_err(page_error)?;
            current_opaque.hasho_nextblkno = next_block;
            hash_page_set_opaque(&mut current_page, current_opaque).map_err(page_error)?;
            images.push(HashPageImage {
                block: current_block,
                page: current_page,
                wal_info,
                will_init: current_will_init,
            });

            let mut next_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
            hash_page_init(&mut next_page, bucket, LH_OVERFLOW_PAGE);
            let mut next_opaque = hash_page_get_opaque(&next_page).map_err(page_error)?;
            next_opaque.hasho_prevblkno = current_block;
            hash_page_set_opaque(&mut next_page, next_opaque).map_err(page_error)?;
            current_block = next_block;
            current_will_init = next_will_init;
            current_page = next_page;
        }
        page_add_item(&mut current_page, &tuple.serialize()).map_err(slotted_page_error)?;
    }

    images.push(HashPageImage {
        block: current_block,
        page: current_page,
        wal_info,
        will_init: current_will_init,
    });
    Ok((images, reusable_overflows[reuse_index..].to_vec()))
}

fn append_tuple_to_bucket(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &HashMetaPageData,
    bucket: u32,
    tuple: &IndexTupleData,
    wal_info: u8,
) -> Result<(), CatalogError> {
    let bucket_block = meta
        .bucket_block(bucket)
        .ok_or(CatalogError::Corrupt("hash bucket block missing"))?;
    let mut block = bucket_block;
    loop {
        let mut page = read_page(pool, client_id, rel, block)?;
        let mut opaque = hash_page_get_opaque(&page).map_err(page_error)?;
        if hash_page_has_space(&page, tuple).map_err(map_access_error)? {
            page_add_item(&mut page, &tuple.serialize()).map_err(slotted_page_error)?;
            write_hash_pages(
                pool,
                client_id,
                xid,
                rel,
                &[HashPageImage {
                    block,
                    page,
                    wal_info,
                    will_init: false,
                }],
            )?;
            return Ok(());
        }
        if opaque.hasho_nextblkno != HASH_INVALID_BLOCK {
            block = opaque.hasho_nextblkno;
            continue;
        }

        let (new_block, will_init) = allocate_hash_block(pool, rel)?;
        let mut overflow = [0u8; crate::backend::storage::smgr::BLCKSZ];
        hash_page_init(&mut overflow, bucket, LH_OVERFLOW_PAGE);
        let mut overflow_opaque = hash_page_get_opaque(&overflow).map_err(page_error)?;
        overflow_opaque.hasho_prevblkno = block;
        hash_page_set_opaque(&mut overflow, overflow_opaque).map_err(page_error)?;
        page_add_item(&mut overflow, &tuple.serialize()).map_err(slotted_page_error)?;
        opaque.hasho_nextblkno = new_block;
        hash_page_set_opaque(&mut page, opaque).map_err(page_error)?;
        write_hash_pages(
            pool,
            client_id,
            xid,
            rel,
            &[
                HashPageImage {
                    block,
                    page,
                    wal_info: XLOG_HASH_ADD_OVFL_PAGE,
                    will_init: false,
                },
                HashPageImage {
                    block: new_block,
                    page: overflow,
                    wal_info: XLOG_HASH_ADD_OVFL_PAGE,
                    will_init,
                },
            ],
        )?;
        return Ok(());
    }
}

fn collect_bucket_chain(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    start_block: u32,
) -> Result<Vec<(u32, [u8; crate::backend::storage::smgr::BLCKSZ])>, CatalogError> {
    let mut chain = Vec::new();
    let mut block = start_block;
    loop {
        let page = read_page(pool, client_id, rel, block)?;
        let opaque = hash_page_get_opaque(&page).map_err(page_error)?;
        let next = opaque.hasho_nextblkno;
        chain.push((block, page));
        if next == HASH_INVALID_BLOCK {
            return Ok(chain);
        }
        block = next;
    }
}

fn maybe_split_bucket(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &mut HashMetaPageData,
) -> Result<(), CatalogError> {
    if !hash_split_needed(meta) {
        return Ok(());
    }
    let new_bucket = meta.hashm_maxbucket.saturating_add(1);
    if new_bucket as usize >= HASH_MAX_BUCKETS {
        return Ok(());
    }
    if new_bucket > meta.hashm_highmask {
        meta.hashm_lowmask = meta.hashm_highmask;
        meta.hashm_highmask = (meta.hashm_highmask << 1) | 1;
    }
    let old_bucket = new_bucket & meta.hashm_lowmask;
    let old_block = meta
        .bucket_block(old_bucket)
        .ok_or(CatalogError::Corrupt("hash split bucket block missing"))?;
    let (new_block, new_will_init) = allocate_hash_block(pool, rel)?;
    let mut reserved_blocks = BTreeSet::from([old_block, new_block]);
    meta.hashm_maxbucket = new_bucket;
    meta.hashm_bucket_blocks[new_bucket as usize] = new_block;
    let spare_index = usize::try_from((new_bucket + 1).ilog2()).unwrap_or(0);
    if let Some(spare) = meta.hashm_spares.get_mut(spare_index) {
        *spare = spare.saturating_add(1);
    }

    let old_chain = collect_bucket_chain(pool, client_id, rel, old_block)?;
    let mut old_items = Vec::new();
    let mut new_items = Vec::new();
    let mut old_overflows = Vec::new();
    for (idx, (block, page)) in old_chain.iter().enumerate() {
        if idx > 0 {
            old_overflows.push(*block);
        }
        for tuple in hash_page_items(page).map_err(map_access_error)? {
            if meta.bucket_for_hash(tuple_hash(&tuple)?) == new_bucket {
                new_items.push(tuple);
            } else {
                old_items.push(tuple);
            }
        }
    }

    let (old_images, unused_old_overflows) = build_bucket_chain_images(
        pool,
        rel,
        old_bucket,
        old_block,
        false,
        &old_overflows,
        &old_items,
        XLOG_HASH_SPLIT_PAGE,
        &mut reserved_blocks,
    )?;
    let (new_images, _) = build_bucket_chain_images(
        pool,
        rel,
        new_bucket,
        new_block,
        new_will_init,
        &[],
        &new_items,
        XLOG_HASH_SPLIT_PAGE,
        &mut reserved_blocks,
    )?;
    let mut meta_page = read_page(pool, client_id, rel, HASH_METAPAGE)?;
    hash_metapage_set(&mut meta_page, meta).map_err(page_error)?;

    let mut images = vec![HashPageImage {
        block: HASH_METAPAGE,
        page: meta_page,
        wal_info: XLOG_HASH_SPLIT_ALLOCATE_PAGE,
        will_init: false,
    }];
    images.extend(old_images);
    images.extend(new_images);
    for block in unused_old_overflows {
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        hash_page_init(&mut page, 0, LH_UNUSED_PAGE);
        images.push(HashPageImage {
            block,
            page,
            wal_info: XLOG_HASH_SPLIT_PAGE,
            will_init: false,
        });
        record_free_index_page(pool, rel, block).map_err(CatalogError::Io)?;
    }
    write_hash_pages(pool, client_id, xid, rel, &images)?;
    Ok(())
}

fn hashbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = crate::backend::access::RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let mut heap_tuples = 0;
    let mut pending = Vec::new();
    heap_services
        .for_each_visible_heap_tuple(
            ctx.heap_relation,
            ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple
                    .deform(&attr_descs)
                    .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
                let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)
                    .map_err(map_catalog_error_to_access)?;
                heap_tuples += 1;
                if let Some(key_values) =
                    index_services.project_index_row(&ctx.index_meta, &row_values, tid)?
                {
                    pending.push((tid, key_values));
                }
                Ok(())
            },
        )
        .map_err(map_access_error)?;

    let fillfactor = hash_fillfactor_from_meta(&ctx.index_meta);
    if relation_nblocks(&ctx.pool, ctx.index_relation)? == 0 {
        let mut result = bulk_load_hash_index(ctx, fillfactor, pending)?;
        result.heap_tuples = heap_tuples;
        return Ok(result);
    }

    let mut result = IndexBuildResult {
        heap_tuples,
        ..IndexBuildResult::default()
    };
    for (tid, key_values) in pending {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        if insert_hash_key_values(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            &ctx.index_desc,
            &ctx.index_meta,
            ctx.default_toast_compression,
            tid,
            &key_values,
        )? {
            result.index_tuples += 1;
        }
    }
    Ok(result)
}

fn hashbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    init_hash_relation(
        &ctx.pool,
        ctx.client_id,
        ctx.xid,
        ctx.index_relation,
        2,
        hash_fillfactor_from_meta(&ctx.index_meta),
    )
}

fn insert_hash_key_values(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    index_relation: RelFileLocator,
    index_desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    default_toast_compression: AttributeCompression,
    heap_tid: ItemPointerData,
    key_values: &[Value],
) -> Result<bool, CatalogError> {
    let Some(first) = key_values.first() else {
        return Err(CatalogError::Corrupt("hash index missing key value"));
    };
    let Some(hash) = RootAccessServices
        .hash_index_value(first, hash_opclass_for_first_key(index_meta))
        .map_err(map_access_error)?
    else {
        return Ok(false);
    };
    let payload =
        encode_hash_tuple_payload(index_desc, key_values, hash, default_toast_compression)?;
    let tuple = IndexTupleData::new_raw(heap_tid, false, true, false, payload);
    let _guard = hash_insert_mutex().lock();
    let mut meta = read_meta(pool, client_id, index_relation)?;
    let bucket = meta.bucket_for_hash(hash);
    append_tuple_to_bucket(
        pool,
        client_id,
        xid,
        index_relation,
        &meta,
        bucket,
        &tuple,
        XLOG_HASH_INSERT,
    )?;
    meta.hashm_ntuples = meta.hashm_ntuples.saturating_add(1);
    maybe_split_bucket(pool, client_id, xid, index_relation, &mut meta)?;
    write_meta(
        pool,
        client_id,
        xid,
        index_relation,
        &meta,
        XLOG_HASH_INSERT,
    )?;
    Ok(true)
}

fn hashinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    insert_hash_key_values(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        &ctx.index_desc,
        &ctx.index_meta,
        ctx.default_toast_compression,
        ctx.heap_tid,
        &ctx.values,
    )
}

fn scan_key_argument(scan: &IndexScanDesc) -> Option<&Value> {
    scan.key_data
        .iter()
        .find(|key| key.attribute_number == 1 && matches!(key.strategy, 1 | 3))
        .map(|key| &key.argument)
}

fn hashbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    let mut scan = crate::backend::access::index::genam::index_beginscan_stub(ctx)?;
    scan.opaque = IndexScanOpaque::Hash(HashIndexScanOpaque::default());
    hashrescan(&mut scan, &ctx.key_data, ctx.direction)?;
    Ok(scan)
}

fn hashrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    crate::backend::access::index::genam::index_rescan_stub(scan, keys, direction)?;
    let mut state = HashIndexScanOpaque::default();
    if let Some(argument) = scan_key_argument(scan)
        && let Some(hash) = RootAccessServices
            .hash_index_value(argument, hash_opclass_for_first_key(&scan.index_meta))
            .map_err(map_access_error)?
    {
        let meta = read_meta(&scan.pool, scan.client_id, scan.index_relation)?;
        let bucket = meta.bucket_for_hash(hash);
        state.scan_hash = Some(hash);
        state.scan_key = Some(argument.to_owned_value());
        state.current_block = meta.bucket_block(bucket);
    }
    scan.opaque = IndexScanOpaque::Hash(state);
    Ok(())
}

fn load_hash_page_items(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    let Some(block) = scan
        .opaque
        .as_hash_mut()
        .and_then(|state| state.current_block)
    else {
        return Ok(false);
    };
    let page = read_page(&scan.pool, scan.client_id, scan.index_relation, block)?;
    let opaque = hash_page_get_opaque(&page).map_err(page_error)?;
    let scan_hash = scan
        .opaque
        .as_hash_mut()
        .and_then(|state| state.scan_hash)
        .ok_or(CatalogError::Corrupt("hash scan missing hash key"))?;
    let scan_key = scan
        .opaque
        .as_hash_mut()
        .and_then(|state| state.scan_key.clone())
        .ok_or(CatalogError::Corrupt("hash scan missing key value"))?;
    let opclass = hash_opclass_for_first_key(&scan.index_meta);
    let filtered = hash_page_items(&page)
        .map_err(map_access_error)?
        .into_iter()
        .filter(|tuple| {
            tuple_hash(tuple).ok() == Some(scan_hash)
                && tuple_key_values(&scan.index_desc, tuple)
                    .ok()
                    .is_some_and(|values| {
                        values.first().is_some_and(|value| {
                            RootAccessServices.hash_values_equal(value, &scan_key, opclass)
                        })
                    })
        })
        .collect::<Vec<_>>();
    let state = scan
        .opaque
        .as_hash_mut()
        .ok_or(CatalogError::Corrupt("hash scan state missing opaque"))?;
    state.current_block = if opaque.hasho_nextblkno == HASH_INVALID_BLOCK {
        None
    } else {
        Some(opaque.hasho_nextblkno)
    };
    state.current_items = filtered;
    state.next_offset = match scan.direction {
        ScanDirection::Forward => 0,
        ScanDirection::Backward => state.current_items.len().saturating_sub(1),
    };
    Ok(true)
}

fn hashgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    loop {
        let needs_load = scan
            .opaque
            .as_hash_mut()
            .is_none_or(|state| state.current_items.is_empty());
        if needs_load {
            if !load_hash_page_items(scan)? {
                return Ok(false);
            }
            continue;
        }
        let next = {
            let state = scan
                .opaque
                .as_hash_mut()
                .ok_or(CatalogError::Corrupt("hash scan state missing opaque"))?;
            match scan.direction {
                ScanDirection::Forward => {
                    if state.next_offset >= state.current_items.len() {
                        state.current_items.clear();
                        None
                    } else {
                        let idx = state.next_offset;
                        state.next_offset += 1;
                        Some(state.current_items[idx].clone())
                    }
                }
                ScanDirection::Backward => {
                    if state.current_items.is_empty()
                        || state.next_offset >= state.current_items.len()
                    {
                        state.current_items.clear();
                        None
                    } else {
                        let idx = state.next_offset;
                        let tuple = state.current_items[idx].clone();
                        if idx == 0 {
                            state.current_items.clear();
                        } else {
                            state.next_offset -= 1;
                        }
                        Some(tuple)
                    }
                }
            }
        };
        let Some(tuple) = next else {
            continue;
        };
        scan.xs_heaptid = Some(tuple.t_tid);
        scan.xs_itup = scan.xs_want_itup.then_some(tuple);
        scan.xs_recheck = true;
        return Ok(true);
    }
}

fn hashgetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, CatalogError> {
    let mut count = 0_i64;
    while hashgettuple(scan)? {
        if let Some(tid) = scan.xs_heaptid {
            bitmap.add_tid(tid);
            count += 1;
        }
    }
    Ok(count)
}

fn hashendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    crate::backend::access::index::genam::index_endscan_stub(scan)
}

fn hashbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let mut result = stats.unwrap_or_default();
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    result.num_pages = u64::from(nblocks);
    for block in 1..nblocks {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        let mut page = read_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
        let Ok(opaque) = hash_page_get_opaque(&page) else {
            continue;
        };
        if opaque.is_unused() || opaque.is_meta() {
            continue;
        }
        let max_offset = page_get_max_offset_number(&page).map_err(slotted_page_error)?;
        let mut removed = 0_u64;
        for offset in (1..=max_offset).rev() {
            let item_id = page_get_item_id(&page, offset).map_err(slotted_page_error)?;
            if item_id.lp_flags != ItemIdFlags::Normal {
                continue;
            }
            let bytes = page_get_item(&page, offset).map_err(slotted_page_error)?;
            let tuple = IndexTupleData::parse(bytes).map_err(|err| {
                CatalogError::Io(format!("hash index tuple parse failed: {err:?}"))
            })?;
            result.num_index_tuples = result.num_index_tuples.saturating_add(1);
            if callback(tuple.t_tid) {
                page_remove_item(&mut page, offset).map_err(slotted_page_error)?;
                removed = removed.saturating_add(1);
            }
        }
        if removed > 0 {
            result.num_removed_tuples = result.num_removed_tuples.saturating_add(removed);
            write_hash_pages(
                &ctx.pool,
                ctx.client_id,
                INVALID_TRANSACTION_ID,
                ctx.index_relation,
                &[HashPageImage {
                    block,
                    page,
                    wal_info: XLOG_HASH_DELETE,
                    will_init: false,
                }],
            )?;
        }
    }
    Ok(result)
}

fn hashvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let mut result = stats.unwrap_or_default();
    let mut meta = read_meta(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    meta.hashm_ntuples = meta.hashm_ntuples.saturating_sub(result.num_removed_tuples);
    for bucket in 0..meta.bucket_count() {
        let Some(mut prev_block) = meta.bucket_block(bucket) else {
            continue;
        };
        let mut prev_page = read_page(&ctx.pool, ctx.client_id, ctx.index_relation, prev_block)?;
        let mut prev_opaque = hash_page_get_opaque(&prev_page).map_err(page_error)?;
        let mut next_block = prev_opaque.hasho_nextblkno;
        while next_block != HASH_INVALID_BLOCK {
            let block = next_block;
            let page = read_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
            let opaque = hash_page_get_opaque(&page).map_err(page_error)?;
            next_block = opaque.hasho_nextblkno;
            if !hash_page_items(&page).map_err(map_access_error)?.is_empty() {
                prev_block = block;
                prev_page = page;
                prev_opaque = opaque;
                continue;
            }
            prev_opaque.hasho_nextblkno = opaque.hasho_nextblkno;
            hash_page_set_opaque(&mut prev_page, prev_opaque).map_err(page_error)?;
            let mut unused = [0u8; crate::backend::storage::smgr::BLCKSZ];
            hash_page_init(&mut unused, 0, LH_UNUSED_PAGE);
            write_hash_pages(
                &ctx.pool,
                ctx.client_id,
                INVALID_TRANSACTION_ID,
                ctx.index_relation,
                &[
                    HashPageImage {
                        block: prev_block,
                        page: prev_page,
                        wal_info: XLOG_HASH_VACUUM,
                        will_init: false,
                    },
                    HashPageImage {
                        block,
                        page: unused,
                        wal_info: XLOG_HASH_VACUUM,
                        will_init: false,
                    },
                ],
            )?;
            record_free_index_page(&ctx.pool, ctx.index_relation, block)
                .map_err(CatalogError::Io)?;
            result.num_deleted_pages = result.num_deleted_pages.saturating_add(1);
        }
    }
    write_meta(
        &ctx.pool,
        ctx.client_id,
        INVALID_TRANSACTION_ID,
        ctx.index_relation,
        &meta,
        XLOG_HASH_VACUUM,
    )?;
    Ok(result)
}

trait HashScanOpaqueExt {
    fn as_hash_mut(&mut self) -> Option<&mut HashIndexScanOpaque>;
}

impl HashScanOpaqueExt for IndexScanOpaque {
    fn as_hash_mut(&mut self) -> Option<&mut HashIndexScanOpaque> {
        match self {
            IndexScanOpaque::Hash(state) => Some(state),
            _ => None,
        }
    }
}

pub fn hash_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 1,
        amsupport: 1,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: true,
        amconsistentordering: false,
        amcanbackward: true,
        amcanunique: false,
        amcanmulticol: false,
        amoptionalkey: false,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: true,
        amsummarizing: false,
        ambuild: Some(hashbuild),
        ambuildempty: Some(hashbuildempty),
        aminsert: Some(hashinsert),
        ambeginscan: Some(hashbeginscan),
        amrescan: Some(hashrescan),
        amgettuple: Some(hashgettuple),
        amgetbitmap: Some(hashgetbitmap),
        amendscan: Some(hashendscan),
        ambulkdelete: Some(hashbulkdelete),
        amvacuumcleanup: Some(hashvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn hash_handler_advertises_postgres_like_capabilities() {
        let am = hash_am_handler();

        assert_eq!(am.amstrategies, 1);
        assert_eq!(am.amsupport, 1);
        assert!(am.amcanhash);
        assert!(!am.amcanunique);
        assert!(!am.amcanmulticol);
        assert!(!am.amoptionalkey);
        assert!(am.amgetbitmap.is_some());
    }

    #[test]
    fn hash_tuple_payload_roundtrips_hash_and_key() {
        let desc = RelationDesc {
            columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
        };
        let payload =
            encode_hash_tuple_payload(&desc, &[Value::Int32(10)], 123, AttributeCompression::Pglz)
                .unwrap();
        let tuple =
            IndexTupleData::new_raw(ItemPointerData::default(), false, true, false, payload);

        assert_eq!(tuple_hash(&tuple).unwrap(), 123);
        assert_eq!(
            tuple_key_values(&desc, &tuple).unwrap(),
            vec![Value::Int32(10)]
        );
    }
}
