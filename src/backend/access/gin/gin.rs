use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::backend::access::gin::jsonb_ops::{self, GinJsonbQuery};
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::page::bufpage::{PageError, page_header};
use crate::backend::storage::smgr::{BLCKSZ, ForkNumber, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::{InterruptState, check_for_interrupts};
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::include::access::gin::{
    GIN_DATA, GIN_DELETED, GIN_ENTRY, GIN_INVALID_BLOCKNO, GIN_LEAF, GIN_LIST, GIN_METAPAGE_BLKNO,
    GIN_ROOT_BLKNO, GinEntryKey, GinEntryTupleData, GinMetaPageData, GinOptions, GinPageError,
    GinPendingTupleData, GinPostingTupleData, gin_metapage_data, gin_metapage_init,
    gin_metapage_set_data, gin_page_append_item, gin_page_get_opaque, gin_page_init,
    gin_page_items, gin_page_set_opaque,
};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use crate::{BufferPool, ClientId, PinnedBuffer};

const INLINE_POSTING_LIMIT: usize = 256;
const POSTING_PAGE_TID_LIMIT: usize = 900;

#[derive(Debug, Clone, Default)]
struct GinIndexImage {
    entries: BTreeMap<GinEntryKey, BTreeSet<ItemPointerData>>,
    pending: Vec<GinPendingTupleData>,
}

pub fn gin_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 0,
        amsupport: 7,
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
        amstorage: true,
        amclusterable: false,
        ampredlocks: false,
        amsummarizing: false,
        ambuild: Some(ginbuild),
        ambuildempty: Some(ginbuildempty),
        aminsert: Some(gininsert),
        ambeginscan: Some(ginbeginscan),
        amrescan: Some(ginrescan),
        amgettuple: None,
        amgetbitmap: Some(gingetbitmap),
        amendscan: Some(ginendscan),
        ambulkdelete: Some(ginbulkdelete),
        amvacuumcleanup: Some(ginvacuumcleanup),
    }
}

fn page_error(err: GinPageError) -> CatalogError {
    CatalogError::Io(format!("GIN page error: {err:?}"))
}

fn raw_page_error(err: PageError) -> CatalogError {
    CatalogError::Io(format!("GIN page error: {err:?}"))
}

fn pin_gin_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("gin pin block failed: {err:?}")))
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(format!("gin nblocks failed: {err:?}")))
}

fn read_gin_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; BLCKSZ], CatalogError> {
    let pin = pin_gin_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("gin shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

fn read_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut pages = Vec::with_capacity(nblocks as usize);
    for block in 0..nblocks {
        pages.push(read_gin_block(pool, client_id, rel, block)?);
    }
    Ok(pages)
}

fn write_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    pages: &[[u8; BLCKSZ]],
) -> Result<(), CatalogError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| CatalogError::Io(format!("gin ensure relation failed: {err:?}")))?;
    pool.with_storage_mut(|storage| {
        storage.smgr.truncate(rel, ForkNumber::Main, 0)?;
        Ok::<(), crate::backend::storage::smgr::SmgrError>(())
    })
    .map_err(|err| CatalogError::Io(format!("gin truncate failed: {err:?}")))?;
    for block in 0..pages.len() as u32 {
        pool.ensure_block_exists(rel, ForkNumber::Main, block)
            .map_err(|err| CatalogError::Io(format!("gin extend failed: {err:?}")))?;
    }
    for (block, page) in pages.iter().enumerate() {
        let pin = pin_gin_block(pool, client_id, rel, block as u32)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("gin exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), page, 0, &mut guard)
            .map_err(|err| CatalogError::Io(format!("gin buffered write failed: {err:?}")))?;
    }
    Ok(())
}

fn gin_options_from_meta(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Result<GinOptions, CatalogError> {
    index_meta.gin_options.clone().ok_or(CatalogError::Corrupt(
        "GIN index metadata missing gin_options",
    ))
}

fn read_gin_metapage(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<GinMetaPageData, CatalogError> {
    let page = read_gin_block(pool, client_id, rel, GIN_METAPAGE_BLKNO)?;
    gin_metapage_data(&page).map_err(page_error)
}

pub(crate) fn ginbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    let options = gin_options_from_meta(&ctx.index_meta)?;
    let mut metapage = [0u8; BLCKSZ];
    gin_metapage_init(&mut metapage, &options).map_err(page_error)?;
    let mut root = [0u8; BLCKSZ];
    gin_page_init(&mut root, GIN_ENTRY | GIN_LEAF).map_err(page_error)?;
    write_index_pages(
        &ctx.pool,
        ctx.client_id,
        ctx.index_relation,
        &[metapage, root],
    )
}

pub(crate) fn ginbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let options = gin_options_from_meta(&ctx.index_meta)?;
    let mut image = GinIndexImage::default();
    let mut projector = IndexBuildKeyProjector::new(ctx)?;
    let heap_tuples = scan_visible_heap_entries(
        &ctx.pool,
        &ctx.txns,
        ctx.client_id,
        &ctx.interrupts,
        ctx.snapshot.clone(),
        ctx.heap_relation,
        &ctx.heap_desc,
        ctx,
        &mut projector,
        &mut image.entries,
    )?;
    let pages = form_index_pages(&image, &options)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &pages)?;
    Ok(IndexBuildResult {
        heap_tuples,
        index_tuples: image.entries.len() as u64,
    })
}

fn scan_visible_heap_entries(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<crate::backend::access::transam::xact::TransactionManager>>,
    client_id: ClientId,
    interrupts: &Arc<InterruptState>,
    snapshot: crate::backend::access::transam::xact::Snapshot,
    heap_relation: RelFileLocator,
    heap_desc: &RelationDesc,
    ctx: &IndexBuildContext,
    projector: &mut IndexBuildKeyProjector,
    entries: &mut BTreeMap<GinEntryKey, BTreeSet<ItemPointerData>>,
) -> Result<u64, CatalogError> {
    let mut scan = heap_scan_begin_visible(pool, client_id, heap_relation, snapshot)
        .map_err(|err| CatalogError::Io(format!("gin heap scan begin failed: {err:?}")))?;
    let attr_descs = heap_desc.attribute_descs();
    let mut visible = 0u64;
    loop {
        check_for_interrupts(interrupts.as_ref()).map_err(CatalogError::Interrupted)?;
        let next = {
            let txns = txns.read();
            heap_scan_next_visible(pool, client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)): Option<(ItemPointerData, HeapTuple)> =
            next.map_err(|err| CatalogError::Io(format!("gin heap scan failed: {err:?}")))?
        else {
            break;
        };
        visible += 1;
        let row_values = materialize_heap_row_values(
            heap_desc,
            &tuple
                .deform(&attr_descs)
                .map_err(|err| CatalogError::Io(format!("gin heap deform failed: {err:?}")))?,
        )?;
        let Some(index_values) = projector.project(ctx, &row_values, tid)? else {
            continue;
        };
        insert_index_values(entries, tid, &index_values)?;
    }
    Ok(visible)
}

pub(crate) fn gininsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    let mut image = read_index_image(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.options())
        .or_else(|_| gin_options_from_meta(&ctx.index_meta))?;
    let row_entries = extract_row_entries(&ctx.values, ctx.heap_tid)?;
    if options.fastupdate {
        image.pending.push(GinPendingTupleData {
            tid: ctx.heap_tid,
            entries: row_entries.into_iter().collect(),
        });
        if pending_bytes(&image.pending) > options.pending_list_limit_bytes() {
            cleanup_pending_into_entries(&mut image);
        }
    } else {
        for entry in row_entries {
            image.entries.entry(entry).or_default().insert(ctx.heap_tid);
        }
    }
    let pages = form_index_pages(&image, &options)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &pages)?;
    Ok(false)
}

pub(crate) fn ginbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
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
        number_of_order_bys: 0,
        order_by_data: Vec::new(),
        direction: ctx.direction,
        xs_want_itup: false,
        xs_itup: None,
        xs_heaptid: None,
        xs_recheck: true,
        xs_recheck_order_by: false,
        xs_orderby_values: Vec::new(),
        opaque: IndexScanOpaque::Gin(crate::include::access::relscan::GinIndexScanOpaque {
            scan_started: false,
        }),
    })
}

pub(crate) fn ginrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    scan.xs_recheck = true;
    if let IndexScanOpaque::Gin(state) = &mut scan.opaque {
        state.scan_started = false;
    }
    Ok(())
}

pub(crate) fn gingetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    let image = read_index_image(&scan.pool, scan.client_id, scan.index_relation)?;
    if let IndexScanOpaque::Gin(state) = &mut scan.opaque {
        state.scan_started = true;
    }
    let mut result: Option<BTreeSet<ItemPointerData>> = None;
    for key in &scan.key_data {
        let attnum = u16::try_from(key.attribute_number)
            .map_err(|_| CatalogError::Corrupt("GIN scan key attnum out of range"))?;
        let query = jsonb_ops::extract_query(attnum, key.strategy, &key.argument)?;
        let _search_mode = jsonb_ops::query_search_mode(&query);
        let tids = match query {
            GinJsonbQuery::All => all_tids(&image),
            GinJsonbQuery::None => BTreeSet::new(),
            GinJsonbQuery::Any(entries) if jsonb_ops::strategy_requires_all(key.strategy) => {
                intersect_entry_tids(&image, &entries)
            }
            GinJsonbQuery::Any(entries) => union_entry_tids(&image, &entries),
        };
        result = Some(match result.take() {
            Some(existing) => existing.intersection(&tids).copied().collect(),
            None => tids,
        });
    }
    let tids = result.unwrap_or_else(|| all_tids(&image));
    for tid in &tids {
        bitmap.add_tid(*tid);
    }
    Ok(tids.len() as i64)
}

pub(crate) fn ginendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}

pub(crate) fn ginbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.options())
        .or_else(|_| gin_options_from_meta(&ctx.index_meta))?;
    let mut image = read_index_image(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    cleanup_pending_into_entries(&mut image);
    let mut out = stats.unwrap_or_default();
    let mut removed = 0u64;
    image.entries.retain(|_, tids| {
        let before = tids.len();
        tids.retain(|tid| !callback(*tid));
        removed += (before - tids.len()) as u64;
        !tids.is_empty()
    });
    out.num_removed_tuples += removed;
    out.num_index_tuples = image.entries.values().map(|tids| tids.len() as u64).sum();
    let pages = form_index_pages(&image, &options)?;
    out.num_pages = pages.len() as u64;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &pages)?;
    Ok(out)
}

pub(crate) fn ginvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.options())
        .or_else(|_| gin_options_from_meta(&ctx.index_meta))?;
    let mut image = read_index_image(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    cleanup_pending_into_entries(&mut image);
    let pages = form_index_pages(&image, &options)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &pages)?;
    let mut out = stats.unwrap_or_default();
    out.num_pages = pages.len() as u64;
    out.num_index_tuples = image.entries.values().map(|tids| tids.len() as u64).sum();
    Ok(out)
}

fn insert_index_values(
    entries: &mut BTreeMap<GinEntryKey, BTreeSet<ItemPointerData>>,
    tid: ItemPointerData,
    values: &[Value],
) -> Result<(), CatalogError> {
    for entry in extract_row_entries(values, tid)? {
        entries.entry(entry).or_default().insert(tid);
    }
    Ok(())
}

fn extract_row_entries(
    values: &[Value],
    _tid: ItemPointerData,
) -> Result<BTreeSet<GinEntryKey>, CatalogError> {
    let mut entries = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        let attnum = u16::try_from(index + 1)
            .map_err(|_| CatalogError::Corrupt("GIN key attnum out of range"))?;
        entries.extend(extract_value_entries(attnum, value)?);
    }
    Ok(entries)
}

fn extract_value_entries(attnum: u16, value: &Value) -> Result<Vec<GinEntryKey>, CatalogError> {
    jsonb_ops::extract_value(attnum, value)
}

fn cleanup_pending_into_entries(image: &mut GinIndexImage) {
    for pending in image.pending.drain(..) {
        for entry in pending.entries {
            image.entries.entry(entry).or_default().insert(pending.tid);
        }
    }
}

fn union_entry_tids(image: &GinIndexImage, keys: &[GinEntryKey]) -> BTreeSet<ItemPointerData> {
    let mut out = BTreeSet::new();
    for key in keys {
        if let Some(tids) = image.entries.get(key) {
            out.extend(tids.iter().copied());
        }
        for pending in &image.pending {
            if pending.entries.iter().any(|entry| entry == key) {
                out.insert(pending.tid);
            }
        }
    }
    out
}

fn intersect_entry_tids(image: &GinIndexImage, keys: &[GinEntryKey]) -> BTreeSet<ItemPointerData> {
    let mut iter = keys.iter();
    let Some(first) = iter.next() else {
        return all_tids(image);
    };
    let mut out = union_entry_tids(image, std::slice::from_ref(first));
    for key in iter {
        let tids = union_entry_tids(image, std::slice::from_ref(key));
        out = out.intersection(&tids).copied().collect();
        if out.is_empty() {
            break;
        }
    }
    out
}

fn all_tids(image: &GinIndexImage) -> BTreeSet<ItemPointerData> {
    let mut out = BTreeSet::new();
    for tids in image.entries.values() {
        out.extend(tids.iter().copied());
    }
    for pending in &image.pending {
        out.insert(pending.tid);
    }
    out
}

fn pending_bytes(pending: &[GinPendingTupleData]) -> usize {
    pending
        .iter()
        .map(|tuple| tuple.serialize().len())
        .sum::<usize>()
}

fn read_index_image(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<GinIndexImage, CatalogError> {
    let pages = read_index_pages(pool, client_id, rel)?;
    let mut image = GinIndexImage::default();
    for (block, page) in pages.iter().enumerate() {
        if block == GIN_METAPAGE_BLKNO as usize {
            continue;
        }
        let opaque = gin_page_get_opaque(page).map_err(page_error)?;
        if opaque.flags & GIN_DELETED != 0 {
            continue;
        }
        if opaque.flags & GIN_ENTRY != 0 {
            for item in gin_page_items(page).map_err(page_error)? {
                let tuple = GinEntryTupleData::parse(item).map_err(page_error)?;
                let tids = if let Some(root) = tuple.posting_root {
                    read_posting_tids(&pages, root)?
                } else {
                    tuple.tids
                };
                image
                    .entries
                    .entry(tuple.key)
                    .or_default()
                    .extend(tids.into_iter());
            }
        } else if opaque.flags & GIN_LIST != 0 {
            for item in gin_page_items(page).map_err(page_error)? {
                image
                    .pending
                    .push(GinPendingTupleData::parse(item).map_err(page_error)?);
            }
        }
    }
    Ok(image)
}

fn read_posting_tids(
    pages: &[[u8; BLCKSZ]],
    root: u32,
) -> Result<Vec<ItemPointerData>, CatalogError> {
    let mut block = root;
    let mut tids = Vec::new();
    while block != GIN_INVALID_BLOCKNO {
        let page = pages
            .get(block as usize)
            .ok_or(CatalogError::Corrupt("GIN posting root out of range"))?;
        let opaque = gin_page_get_opaque(page).map_err(page_error)?;
        if opaque.flags & GIN_DATA == 0 {
            return Err(CatalogError::Corrupt("GIN posting page expected"));
        }
        for item in gin_page_items(page).map_err(page_error)? {
            tids.extend(
                GinPostingTupleData::parse(item)
                    .map_err(page_error)?
                    .tids
                    .into_iter(),
            );
        }
        block = opaque.rightlink;
    }
    Ok(tids)
}

fn form_index_pages(
    image: &GinIndexImage,
    options: &GinOptions,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let entries = image
        .entries
        .iter()
        .map(|(key, tids)| (key.clone(), tids.iter().copied().collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    let placeholder_tuples = entries
        .iter()
        .map(|(key, tids)| GinEntryTupleData {
            key: key.clone(),
            posting_root: (tids.len() > INLINE_POSTING_LIMIT).then_some(0),
            tids: if tids.len() <= INLINE_POSTING_LIMIT {
                tids.clone()
            } else {
                Vec::new()
            },
        })
        .collect::<Vec<_>>();
    let placeholder_entry_pages = pack_entry_pages(&placeholder_tuples, GIN_ROOT_BLKNO)?;
    let mut posting_roots = BTreeMap::new();
    let mut posting_pages = Vec::new();
    let mut next_block = GIN_ROOT_BLKNO + placeholder_entry_pages.len() as u32;
    for (key, tids) in &entries {
        if tids.len() <= INLINE_POSTING_LIMIT {
            continue;
        }
        posting_roots.insert(key.clone(), next_block);
        let pages = pack_posting_pages(tids, next_block)?;
        next_block = next_block.saturating_add(pages.len() as u32);
        posting_pages.extend(pages);
    }
    let entry_tuples = entries
        .iter()
        .map(|(key, tids)| GinEntryTupleData {
            key: key.clone(),
            posting_root: posting_roots.get(key).copied(),
            tids: if tids.len() <= INLINE_POSTING_LIMIT {
                tids.clone()
            } else {
                Vec::new()
            },
        })
        .collect::<Vec<_>>();
    let entry_pages = pack_entry_pages(&entry_tuples, GIN_ROOT_BLKNO)?;
    if entry_pages.len() != placeholder_entry_pages.len() {
        return Err(CatalogError::Corrupt(
            "GIN entry page count changed during form",
        ));
    }
    let pending_start = GIN_ROOT_BLKNO + entry_pages.len() as u32 + posting_pages.len() as u32;
    let pending_pages = pack_pending_pages(&image.pending, pending_start)?;

    let mut meta = GinMetaPageData::new(options);
    meta.pending_head = if pending_pages.is_empty() {
        GIN_INVALID_BLOCKNO
    } else {
        pending_start
    };
    meta.pending_tail = if pending_pages.is_empty() {
        GIN_INVALID_BLOCKNO
    } else {
        pending_start + pending_pages.len() as u32 - 1
    };
    meta.n_pending_pages = pending_pages.len() as u32;
    meta.n_pending_heap_tuples = image.pending.len() as u64;
    meta.n_total_pages =
        1 + entry_pages.len() as u32 + posting_pages.len() as u32 + pending_pages.len() as u32;
    meta.n_entry_pages = entry_pages.len() as u32;
    meta.n_data_pages = posting_pages.len() as u32;
    meta.n_entries = image.entries.len() as u64;

    let mut metapage = [0u8; BLCKSZ];
    gin_metapage_init(&mut metapage, options).map_err(page_error)?;
    gin_metapage_set_data(&mut metapage, &meta).map_err(page_error)?;
    let mut pages = vec![metapage];
    pages.extend(entry_pages);
    pages.extend(posting_pages);
    pages.extend(pending_pages);
    Ok(pages)
}

fn pack_entry_pages(
    tuples: &[GinEntryTupleData],
    start_block: u32,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let mut pages = Vec::new();
    let mut page = empty_page(GIN_ENTRY | GIN_LEAF)?;
    for tuple in tuples {
        let bytes = tuple.serialize();
        if gin_page_append_item(&mut page, &bytes).is_err() {
            pages.push(page);
            page = empty_page(GIN_ENTRY | GIN_LEAF)?;
            gin_page_append_item(&mut page, &bytes).map_err(page_error)?;
        }
    }
    pages.push(page);
    set_page_rightlinks(&mut pages, start_block)?;
    Ok(pages)
}

fn pack_posting_pages(
    tids: &[ItemPointerData],
    start_block: u32,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let mut pages = Vec::new();
    for chunk in tids.chunks(POSTING_PAGE_TID_LIMIT) {
        let mut page = empty_page(GIN_DATA | GIN_LEAF)?;
        let tuple = GinPostingTupleData {
            tids: chunk.to_vec(),
        };
        gin_page_append_item(&mut page, &tuple.serialize()).map_err(page_error)?;
        pages.push(page);
    }
    set_page_rightlinks(&mut pages, start_block)?;
    Ok(pages)
}

fn pack_pending_pages(
    pending: &[GinPendingTupleData],
    start_block: u32,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let mut pages = Vec::new();
    if pending.is_empty() {
        return Ok(pages);
    }
    let mut page = empty_page(GIN_LIST | GIN_LEAF)?;
    for tuple in pending {
        let bytes = tuple.serialize();
        if gin_page_append_item(&mut page, &bytes).is_err() {
            pages.push(page);
            page = empty_page(GIN_LIST | GIN_LEAF)?;
            gin_page_append_item(&mut page, &bytes).map_err(page_error)?;
        }
    }
    pages.push(page);
    set_page_rightlinks(&mut pages, start_block)?;
    Ok(pages)
}

fn empty_page(flags: u16) -> Result<[u8; BLCKSZ], CatalogError> {
    let mut page = [0u8; BLCKSZ];
    gin_page_init(&mut page, flags).map_err(page_error)?;
    Ok(page)
}

fn set_page_rightlinks(pages: &mut [[u8; BLCKSZ]], start_block: u32) -> Result<(), CatalogError> {
    let len = pages.len();
    for (index, page) in pages.iter_mut().enumerate() {
        let mut opaque = gin_page_get_opaque(page).map_err(page_error)?;
        opaque.rightlink = if index + 1 == len {
            GIN_INVALID_BLOCKNO
        } else {
            start_block + index as u32 + 1
        };
        gin_page_set_opaque(page, opaque).map_err(page_error)?;
    }
    Ok(())
}

#[allow(dead_code)]
fn page_free_space(page: &[u8; BLCKSZ]) -> Result<usize, CatalogError> {
    Ok(page_header(page).map_err(raw_page_error)?.free_space())
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::jsonb::parse_jsonb_text;

    use super::*;

    #[test]
    fn form_and_read_index_image_roundtrips_entry_pages() {
        let mut image = GinIndexImage::default();
        let value = Value::Jsonb(parse_jsonb_text(r#"{"a": 1}"#).unwrap());
        insert_index_values(
            &mut image.entries,
            ItemPointerData {
                block_number: 1,
                offset_number: 2,
            },
            &[value],
        )
        .unwrap();

        let pages = form_index_pages(&image, &GinOptions::default()).unwrap();
        assert!(pages.len() >= 2);
        let meta = gin_metapage_data(&pages[0]).unwrap();
        assert_eq!(meta.n_entries as usize, image.entries.len());
    }
}
