use std::collections::BTreeSet;
use std::sync::{Mutex, OnceLock};

use pgrust_core::{
    INVALID_TRANSACTION_ID, REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_HASH_ID,
    XLOG_HASH_ADD_OVFL_PAGE, XLOG_HASH_DELETE, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_INSERT,
    XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_PAGE, XLOG_HASH_VACUUM,
};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_storage::{
    BLCKSZ, BufferPool, BufferTag, ForkNumber, PinnedBuffer, RelFileLocator, SmgrStorageBackend,
    fsm::{get_free_index_page, record_free_index_page},
    page::bufpage::{
        ItemIdFlags, PageError, page_add_item, page_get_item, page_get_item_id,
        page_get_max_offset_number, page_remove_item,
    },
    smgr::StorageManager,
};

use crate::access::amapi::{
    IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult,
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::access::hash::{
    HASH_INVALID_BLOCK, HASH_MAX_BUCKETS, HASH_METAPAGE, HashMetaPageData, HashPageError,
    LH_BUCKET_PAGE, LH_OVERFLOW_PAGE, LH_UNUSED_PAGE, encode_hash_tuple_payload,
    hash_build_bucket_count, hash_fillfactor_from_meta, hash_metapage_data, hash_metapage_init,
    hash_metapage_set, hash_opclass_for_first_key, hash_page_get_opaque, hash_page_has_items,
    hash_page_has_space, hash_page_init, hash_page_items, hash_page_set_opaque, hash_split_needed,
    hash_tuple_hash, hash_tuple_key_values,
};
use crate::access::htup::AttributeCompression;
use crate::access::itemptr::ItemPointerData;
use crate::access::itup::IndexTupleData;
use crate::access::relscan::{HashIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::access::scankey::ScanKeyData;
use crate::access::tidbitmap::TidBitmap;
use crate::index::genam::{index_beginscan_stub, index_endscan_stub, index_rescan_stub};
use crate::{
    AccessError, AccessInterruptServices, AccessResult, AccessScalarServices, AccessWalBlockRef,
    AccessWalRecord, AccessWalServices,
};

fn hash_insert_mutex() -> &'static Mutex<()> {
    static HASH_INSERT_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    HASH_INSERT_MUTEX.get_or_init(|| Mutex::new(()))
}

fn page_error(err: HashPageError) -> AccessError {
    AccessError::Scalar(format!("hash page error: {err:?}"))
}

fn slotted_page_error(err: PageError) -> AccessError {
    AccessError::Scalar(format!("hash slotted page error: {err:?}"))
}

fn check_interrupts(interrupts: &dyn AccessInterruptServices) -> AccessResult<()> {
    interrupts
        .check_interrupts()
        .map_err(AccessError::Interrupted)
}

fn pin_hash_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: RelFileLocator,
    block: u32,
) -> AccessResult<PinnedBuffer<'a, SmgrStorageBackend>> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| AccessError::Scalar(format!("hash pin block failed: {err:?}")))
}

fn read_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: RelFileLocator,
    block: u32,
) -> AccessResult<[u8; BLCKSZ]> {
    let pin = pin_hash_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| AccessError::Scalar(format!("hash shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> AccessResult<u32> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| AccessError::Scalar(format!("hash nblocks failed: {err:?}")))
}

struct HashPageImage {
    block: u32,
    page: [u8; BLCKSZ],
    wal_info: u8,
    will_init: bool,
}

const HASH_MAX_PAGES_PER_WAL_RECORD: usize = 200;

fn write_hash_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    xid: u32,
    rel: RelFileLocator,
    pages: &[HashPageImage],
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
    if pages.len() > HASH_MAX_PAGES_PER_WAL_RECORD {
        for chunk in pages.chunks(HASH_MAX_PAGES_PER_WAL_RECORD) {
            write_hash_pages(pool, client_id, xid, rel, chunk, wal)?;
        }
        return Ok(());
    }

    for page in pages {
        pool.ensure_block_exists(rel, ForkNumber::Main, page.block)
            .map_err(|err| AccessError::Scalar(format!("hash extend failed: {err:?}")))?;
    }
    let blocks = pages
        .iter()
        .map(|page| {
            let mut flags = REGBUF_STANDARD | REGBUF_FORCE_IMAGE;
            if page.will_init {
                flags |= REGBUF_WILL_INIT;
            }
            AccessWalBlockRef {
                tag: BufferTag {
                    rel,
                    fork: ForkNumber::Main,
                    block: page.block,
                },
                flags,
                data: page.page.to_vec(),
                buffer_data: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    let wal_info = pages
        .first()
        .map(|page| page.wal_info)
        .unwrap_or(XLOG_HASH_INSERT);
    let lsn = wal.log_access_record(AccessWalRecord {
        xid,
        rmid: RM_HASH_ID,
        info: wal_info,
        payload: Vec::new(),
        blocks,
    })?;
    for page in pages {
        let pin = pin_hash_block(pool, client_id, rel, page.block)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| AccessError::Scalar(format!("hash exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), &page.page, lsn, &mut guard)
            .map_err(|err| AccessError::Scalar(format!("hash buffered write failed: {err:?}")))?;
    }
    Ok(())
}

fn init_hash_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    xid: u32,
    rel: RelFileLocator,
    bucket_count: u32,
    fillfactor: u16,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
    let meta = HashMetaPageData::new(bucket_count, fillfactor);
    let mut pages = Vec::new();
    let mut meta_page = [0u8; BLCKSZ];
    hash_metapage_init(&mut meta_page, &meta);
    pages.push(HashPageImage {
        block: HASH_METAPAGE,
        page: meta_page,
        wal_info: XLOG_HASH_INIT_META_PAGE,
        will_init: true,
    });
    for bucket in 0..meta.bucket_count() {
        let mut page = [0u8; BLCKSZ];
        hash_page_init(&mut page, bucket, LH_BUCKET_PAGE);
        pages.push(HashPageImage {
            block: bucket + 1,
            page,
            wal_info: XLOG_HASH_INIT_META_PAGE,
            will_init: true,
        });
    }
    write_hash_pages(pool, client_id, xid, rel, &pages, wal)
}

fn read_meta(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: RelFileLocator,
) -> AccessResult<HashMetaPageData> {
    let page = read_page(pool, client_id, rel, HASH_METAPAGE)?;
    hash_metapage_data(&page).map_err(page_error)
}

fn write_meta(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &HashMetaPageData,
    wal_info: u8,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
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
        wal,
    )
}

fn encode_hash_key_payload(
    desc: &RelationDesc,
    key_values: &[Value],
    hash: u32,
    default_toast_compression: AttributeCompression,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<Vec<u8>> {
    encode_hash_tuple_payload(desc, key_values, hash, default_toast_compression, scalar)
}

fn allocate_hash_block(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> AccessResult<(u32, bool)> {
    loop {
        if let Some(block) = get_free_index_page(pool, rel).map_err(AccessError::Scalar)? {
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
) -> AccessResult<(u32, bool)> {
    loop {
        if let Some(block) = get_free_index_page(pool, rel).map_err(AccessError::Scalar)? {
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
) -> AccessResult<(Vec<HashPageImage>, Vec<u32>)> {
    let mut images = Vec::new();
    let mut reuse_index = 0usize;
    let mut current_block = primary_block;
    let mut current_will_init = primary_will_init;
    let mut current_page = [0u8; BLCKSZ];
    hash_page_init(&mut current_page, bucket, LH_BUCKET_PAGE);

    for tuple in items {
        if !hash_page_has_space(&current_page, tuple)? && hash_page_has_items(&current_page)? {
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

            let mut next_page = [0u8; BLCKSZ];
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
    client_id: pgrust_core::ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &HashMetaPageData,
    bucket: u32,
    tuple: &IndexTupleData,
    wal_info: u8,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
    let bucket_block = meta
        .bucket_block(bucket)
        .ok_or(AccessError::Corrupt("hash bucket block missing"))?;
    let mut block = bucket_block;
    loop {
        let mut page = read_page(pool, client_id, rel, block)?;
        let mut opaque = hash_page_get_opaque(&page).map_err(page_error)?;
        if hash_page_has_space(&page, tuple)? {
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
                wal,
            )?;
            return Ok(());
        }
        if opaque.hasho_nextblkno != HASH_INVALID_BLOCK {
            block = opaque.hasho_nextblkno;
            continue;
        }

        let (new_block, will_init) = allocate_hash_block(pool, rel)?;
        let mut overflow = [0u8; BLCKSZ];
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
            wal,
        )?;
        return Ok(());
    }
}

fn collect_bucket_chain(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: RelFileLocator,
    start_block: u32,
) -> AccessResult<Vec<(u32, [u8; BLCKSZ])>> {
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
    client_id: pgrust_core::ClientId,
    xid: u32,
    rel: RelFileLocator,
    meta: &mut HashMetaPageData,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
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
        .ok_or(AccessError::Corrupt("hash split bucket block missing"))?;
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
        for tuple in hash_page_items(page)? {
            if meta.bucket_for_hash(hash_tuple_hash(&tuple)?) == new_bucket {
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
        let mut page = [0u8; BLCKSZ];
        hash_page_init(&mut page, 0, LH_UNUSED_PAGE);
        images.push(HashPageImage {
            block,
            page,
            wal_info: XLOG_HASH_SPLIT_PAGE,
            will_init: false,
        });
        record_free_index_page(pool, rel, block).map_err(AccessError::Scalar)?;
    }
    write_hash_pages(pool, client_id, xid, rel, &images, wal)?;
    Ok(())
}

fn bulk_load_hash_index(
    ctx: &IndexBuildContext,
    fillfactor: u16,
    heap_tuples: u64,
    pending: Vec<(ItemPointerData, Vec<Value>)>,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBuildResult> {
    let mut result = IndexBuildResult {
        heap_tuples,
        ..IndexBuildResult::default()
    };
    let mut meta = HashMetaPageData::new(
        hash_build_bucket_count(pending.len(), fillfactor),
        fillfactor,
    );
    let mut buckets = vec![Vec::new(); meta.bucket_count() as usize];

    for (tid, key_values) in pending {
        check_interrupts(interrupts)?;
        let Some(first) = key_values.first() else {
            return Err(AccessError::Corrupt("hash index missing key value"));
        };
        let Some(hash) =
            scalar.hash_index_value(first, hash_opclass_for_first_key(&ctx.index_meta))?
        else {
            continue;
        };
        let payload = encode_hash_key_payload(
            &ctx.index_desc,
            &key_values,
            hash,
            ctx.default_toast_compression,
            scalar,
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
        wal,
    )?;

    let mut reserved_blocks = (HASH_METAPAGE..=meta.bucket_count()).collect::<BTreeSet<_>>();
    let mut images = Vec::new();
    let mut meta_page = [0u8; BLCKSZ];
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
            .ok_or(AccessError::Corrupt("hash bucket block missing"))?;
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
        wal,
    )?;
    Ok(result)
}

pub fn hashbuild_projected(
    ctx: &IndexBuildContext,
    heap_tuples: u64,
    pending: Vec<(ItemPointerData, Vec<Value>)>,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBuildResult> {
    let fillfactor = hash_fillfactor_from_meta(&ctx.index_meta);
    if relation_nblocks(&ctx.pool, ctx.index_relation)? == 0 {
        return bulk_load_hash_index(
            ctx,
            fillfactor,
            heap_tuples,
            pending,
            interrupts,
            scalar,
            wal,
        );
    }

    let mut result = IndexBuildResult {
        heap_tuples,
        ..IndexBuildResult::default()
    };
    for (tid, key_values) in pending {
        check_interrupts(interrupts)?;
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
            scalar,
            wal,
        )? {
            result.index_tuples += 1;
        }
    }
    Ok(result)
}

pub fn hashbuildempty(
    ctx: &IndexBuildEmptyContext,
    wal: &dyn AccessWalServices,
) -> AccessResult<()> {
    init_hash_relation(
        &ctx.pool,
        ctx.client_id,
        ctx.xid,
        ctx.index_relation,
        2,
        hash_fillfactor_from_meta(&ctx.index_meta),
        wal,
    )
}

pub fn insert_hash_key_values(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    xid: u32,
    index_relation: RelFileLocator,
    index_desc: &RelationDesc,
    index_meta: &pgrust_nodes::relcache::IndexRelCacheEntry,
    default_toast_compression: AttributeCompression,
    heap_tid: ItemPointerData,
    key_values: &[Value],
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<bool> {
    let Some(first) = key_values.first() else {
        return Err(AccessError::Corrupt("hash index missing key value"));
    };
    let Some(hash) = scalar.hash_index_value(first, hash_opclass_for_first_key(index_meta))? else {
        return Ok(false);
    };
    let payload = encode_hash_key_payload(
        index_desc,
        key_values,
        hash,
        default_toast_compression,
        scalar,
    )?;
    let tuple = IndexTupleData::new_raw(heap_tid, false, true, false, payload);
    let _guard = hash_insert_mutex()
        .lock()
        .map_err(|_| AccessError::Scalar("hash insert mutex poisoned".into()))?;
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
        wal,
    )?;
    meta.hashm_ntuples = meta.hashm_ntuples.saturating_add(1);
    maybe_split_bucket(pool, client_id, xid, index_relation, &mut meta, wal)?;
    write_meta(
        pool,
        client_id,
        xid,
        index_relation,
        &meta,
        XLOG_HASH_INSERT,
        wal,
    )?;
    Ok(true)
}

pub fn hashinsert(
    ctx: &IndexInsertContext,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<bool> {
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
        scalar,
        wal,
    )
}

fn scan_key_argument(scan: &IndexScanDesc) -> Option<&Value> {
    scan.key_data
        .iter()
        .find(|key| key.attribute_number == 1 && matches!(key.strategy, 1 | 3))
        .map(|key| &key.argument)
}

fn hash_state(scan: &IndexScanDesc) -> AccessResult<&HashIndexScanOpaque> {
    match &scan.opaque {
        IndexScanOpaque::Hash(state) => Ok(state),
        _ => Err(AccessError::Corrupt("hash scan state missing opaque")),
    }
}

fn hash_state_mut(scan: &mut IndexScanDesc) -> AccessResult<&mut HashIndexScanOpaque> {
    match &mut scan.opaque {
        IndexScanOpaque::Hash(state) => Ok(state),
        _ => Err(AccessError::Corrupt("hash scan state missing opaque")),
    }
}

pub fn hashbeginscan(
    ctx: &IndexBeginScanContext,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<IndexScanDesc> {
    let mut scan = index_beginscan_stub(ctx)?;
    scan.opaque = IndexScanOpaque::Hash(HashIndexScanOpaque::default());
    hashrescan(&mut scan, &ctx.key_data, ctx.direction, scalar)?;
    Ok(scan)
}

pub fn hashrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<()> {
    index_rescan_stub(scan, keys, direction)?;
    let mut state = HashIndexScanOpaque::default();
    if let Some(argument) = scan_key_argument(scan)
        && let Some(hash) =
            scalar.hash_index_value(argument, hash_opclass_for_first_key(&scan.index_meta))?
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

fn load_hash_page_items(
    scan: &mut IndexScanDesc,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    let Some(block) = hash_state(scan)?.current_block else {
        return Ok(false);
    };
    let page = read_page(&scan.pool, scan.client_id, scan.index_relation, block)?;
    let opaque = hash_page_get_opaque(&page).map_err(page_error)?;
    let scan_hash = hash_state(scan)?
        .scan_hash
        .ok_or(AccessError::Corrupt("hash scan missing hash key"))?;
    let scan_key = hash_state(scan)?
        .scan_key
        .clone()
        .ok_or(AccessError::Corrupt("hash scan missing key value"))?;
    let opclass = hash_opclass_for_first_key(&scan.index_meta);
    let filtered = hash_page_items(&page)?
        .into_iter()
        .filter(|tuple| {
            hash_tuple_hash(tuple).ok() == Some(scan_hash)
                && hash_tuple_key_values(&scan.index_desc, tuple, scalar)
                    .ok()
                    .is_some_and(|values| {
                        values.first().is_some_and(|value| {
                            scalar.hash_values_equal(value, &scan_key, opclass)
                        })
                    })
        })
        .collect::<Vec<_>>();
    let direction = scan.direction;
    let state = hash_state_mut(scan)?;
    state.current_block = if opaque.hasho_nextblkno == HASH_INVALID_BLOCK {
        None
    } else {
        Some(opaque.hasho_nextblkno)
    };
    state.current_items = filtered;
    state.next_offset = match direction {
        ScanDirection::Forward => 0,
        ScanDirection::Backward => state.current_items.len().saturating_sub(1),
    };
    Ok(true)
}

pub fn hashgettuple(
    scan: &mut IndexScanDesc,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    loop {
        let needs_load = hash_state(scan)
            .map(|state| state.current_items.is_empty())
            .unwrap_or(true);
        if needs_load {
            if !load_hash_page_items(scan, scalar)? {
                return Ok(false);
            }
            continue;
        }
        let direction = scan.direction;
        let next = {
            let state = hash_state_mut(scan)?;
            match direction {
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

pub fn hashgetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<i64> {
    let mut count = 0_i64;
    while hashgettuple(scan, scalar)? {
        if let Some(tid) = scan.xs_heaptid {
            bitmap.add_tid(tid);
            count += 1;
        }
    }
    Ok(count)
}

pub fn hashendscan(scan: IndexScanDesc) -> AccessResult<()> {
    index_endscan_stub(scan)
}

pub fn hashbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
    interrupts: &dyn AccessInterruptServices,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBulkDeleteResult> {
    let mut result = stats.unwrap_or_default();
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    result.num_pages = u64::from(nblocks);
    for block in 1..nblocks {
        check_interrupts(interrupts)?;
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
                AccessError::Scalar(format!("hash index tuple parse failed: {err:?}"))
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
                wal,
            )?;
        }
    }
    Ok(result)
}

pub fn hashvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
    wal: &dyn AccessWalServices,
) -> AccessResult<IndexBulkDeleteResult> {
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
            if !hash_page_items(&page)?.is_empty() {
                prev_block = block;
                prev_page = page;
                prev_opaque = opaque;
                continue;
            }
            prev_opaque.hasho_nextblkno = opaque.hasho_nextblkno;
            hash_page_set_opaque(&mut prev_page, prev_opaque).map_err(page_error)?;
            let mut unused = [0u8; BLCKSZ];
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
                wal,
            )?;
            record_free_index_page(&ctx.pool, ctx.index_relation, block)
                .map_err(AccessError::Scalar)?;
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
        wal,
    )?;
    Ok(result)
}
