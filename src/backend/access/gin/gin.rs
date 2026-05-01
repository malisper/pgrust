use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::backend::access::gin::jsonb_ops::{self, GinJsonbQuery};
use crate::backend::access::heap::heapam::{
    heap_scan_begin, heap_scan_begin_visible, heap_scan_next, heap_scan_next_visible,
};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::access::transam::xlog::RM_GIN_ID;
use crate::backend::catalog::CatalogError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::fsm::{
    clear_free_index_pages, get_free_index_page, record_free_index_page,
};
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
    GIN_ROOT_BLKNO, GinEntryKey, GinEntryTupleData, GinMetaPageData, GinNullCategory, GinOptions,
    GinPageError, GinPageOpaqueData, GinPendingTupleData, GinPostingTupleData, gin_metapage_data,
    gin_metapage_init, gin_metapage_set_data, gin_page_append_item, gin_page_get_opaque,
    gin_page_init, gin_page_items, gin_page_set_opaque,
};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use crate::include::nodes::tsearch::{TsQuery, TsQueryNode};
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
    clear_free_index_pages(pool, rel).map_err(CatalogError::Io)?;
    for block in 0..pages.len() as u32 {
        pool.ensure_block_exists(rel, ForkNumber::Main, block)
            .map_err(|err| CatalogError::Io(format!("gin extend failed: {err:?}")))?;
    }
    for (block, page) in pages.iter().enumerate() {
        write_gin_block(pool, client_id, rel, block as u32, page)?;
    }
    Ok(())
}

fn write_gin_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; BLCKSZ],
) -> Result<(), CatalogError> {
    pool.ensure_block_exists(rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("gin extend failed: {err:?}")))?;
    let pin = pin_gin_block(pool, client_id, rel, block)?;
    let mut guard = pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("gin exclusive lock failed: {err:?}")))?;
    pool.write_page_image_locked_with_rmgr(pin.buffer_id(), 0, page, &mut guard, RM_GIN_ID)
        .map_err(|err| CatalogError::Io(format!("gin buffered write failed: {err:?}")))?;
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
    let metapage_options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .ok()
        .map(|meta| meta.options());
    let options = if let Some(options) = &metapage_options {
        options.clone()
    } else {
        gin_options_from_meta(&ctx.index_meta)?
    };
    let row_entries = extract_row_entries(&ctx.values, ctx.heap_tid)?;
    if options.fastupdate {
        if metapage_options.is_some() {
            append_fastupdate_pending_tuple(
                &ctx.pool,
                ctx.client_id,
                ctx.index_relation,
                GinPendingTupleData {
                    tid: ctx.heap_tid,
                    entries: row_entries.into_iter().collect(),
                },
            )?;
            return Ok(false);
        }
    }

    insert_entries_into_main(
        &ctx.pool,
        ctx.client_id,
        ctx.index_relation,
        row_entries,
        ctx.heap_tid,
        true,
    )?;
    Ok(false)
}

fn append_fastupdate_pending_tuple(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: GinPendingTupleData,
) -> Result<(), CatalogError> {
    let mut tuple_bytes = Vec::with_capacity(tuple.serialized_len());
    tuple.serialize_into(&mut tuple_bytes);

    let mut metapage = read_gin_block(pool, client_id, rel, GIN_METAPAGE_BLKNO)?;
    let mut meta = gin_metapage_data(&metapage).map_err(page_error)?;

    if meta.pending_tail == GIN_INVALID_BLOCKNO {
        if meta.pending_head != GIN_INVALID_BLOCKNO {
            return Err(CatalogError::Corrupt("GIN pending head without tail"));
        }
        append_pending_tuple_on_new_page(pool, client_id, rel, &mut meta, &tuple_bytes)?;
    } else {
        let mut tail_page = read_gin_block(pool, client_id, rel, meta.pending_tail)?;
        validate_pending_page(&tail_page)?;
        match append_pending_tuple_to_page(&mut tail_page, &tuple_bytes) {
            Ok(tail_free_size) => {
                meta.tail_free_size = tail_free_size;
                meta.n_pending_heap_tuples = meta.n_pending_heap_tuples.saturating_add(1);
                write_gin_block(pool, client_id, rel, meta.pending_tail, &tail_page)?;
            }
            Err(GinPageError::Page(PageError::NoSpace)) => {
                append_pending_tuple_on_new_page(pool, client_id, rel, &mut meta, &tuple_bytes)?;
            }
            Err(err) => return Err(page_error(err)),
        }
    }

    gin_metapage_set_data(&mut metapage, &meta).map_err(page_error)?;
    write_gin_block(pool, client_id, rel, GIN_METAPAGE_BLKNO, &metapage)?;

    if pending_cleanup_needed(&meta) {
        gin_insert_cleanup_relation(
            pool,
            client_id,
            rel,
            GinCleanupMode {
                full_clean: false,
                force_cleanup: false,
                fill_fsm: true,
            },
        )?;
    }

    Ok(())
}

fn append_pending_tuple_on_new_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    meta: &mut GinMetaPageData,
    tuple_bytes: &[u8],
) -> Result<(), CatalogError> {
    let new_block = relation_nblocks(pool, rel)?;
    let (new_page, tail_free_size) = new_pending_page(tuple_bytes)?;
    write_gin_block(pool, client_id, rel, new_block, &new_page)?;

    if meta.pending_tail == GIN_INVALID_BLOCKNO {
        meta.pending_head = new_block;
    } else {
        let mut old_tail_page = read_gin_block(pool, client_id, rel, meta.pending_tail)?;
        let mut old_tail_opaque = validate_pending_page(&old_tail_page)?;
        if old_tail_opaque.rightlink != GIN_INVALID_BLOCKNO {
            return Err(CatalogError::Corrupt("GIN pending tail has rightlink"));
        }
        old_tail_opaque.rightlink = new_block;
        gin_page_set_opaque(&mut old_tail_page, old_tail_opaque).map_err(page_error)?;
        write_gin_block(pool, client_id, rel, meta.pending_tail, &old_tail_page)?;
    }

    meta.pending_tail = new_block;
    meta.tail_free_size = tail_free_size;
    meta.n_pending_pages = meta.n_pending_pages.saturating_add(1);
    meta.n_pending_heap_tuples = meta.n_pending_heap_tuples.saturating_add(1);
    meta.n_total_pages = meta.n_total_pages.max(new_block.saturating_add(1));
    Ok(())
}

fn validate_pending_page(page: &[u8; BLCKSZ]) -> Result<GinPageOpaqueData, CatalogError> {
    let opaque = gin_page_get_opaque(page).map_err(page_error)?;
    if opaque.flags & GIN_LIST == 0 {
        return Err(CatalogError::Corrupt("GIN pending page expected"));
    }
    Ok(opaque)
}

fn new_pending_page(tuple_bytes: &[u8]) -> Result<([u8; BLCKSZ], u32), CatalogError> {
    let mut page = empty_page(GIN_LIST | GIN_LEAF)?;
    let tail_free_size =
        append_pending_tuple_to_page(&mut page, tuple_bytes).map_err(page_error)?;
    Ok((page, tail_free_size))
}

fn append_pending_tuple_to_page(
    page: &mut [u8; BLCKSZ],
    tuple_bytes: &[u8],
) -> Result<u32, GinPageError> {
    gin_page_append_item(page, tuple_bytes)?;
    Ok(page_header(page)?.free_space() as u32)
}

fn pending_cleanup_needed(meta: &GinMetaPageData) -> bool {
    (meta.n_pending_pages as usize).saturating_mul(BLCKSZ)
        > meta.options().pending_list_limit_bytes()
}

#[derive(Debug, Clone, Copy)]
struct GinCleanupMode {
    full_clean: bool,
    force_cleanup: bool,
    fill_fsm: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct GinCleanupStats {
    pages_deleted: i64,
    tuples_moved: u64,
}

pub(crate) fn gin_clean_pending_list(ctx: &IndexVacuumContext) -> Result<i64, CatalogError> {
    gin_insert_cleanup(
        ctx,
        GinCleanupMode {
            full_clean: true,
            force_cleanup: true,
            fill_fsm: true,
        },
    )
    .map(|stats| stats.pages_deleted)
}

fn gin_insert_cleanup(
    ctx: &IndexVacuumContext,
    mode: GinCleanupMode,
) -> Result<GinCleanupStats, CatalogError> {
    gin_insert_cleanup_relation(&ctx.pool, ctx.client_id, ctx.index_relation, mode)
}

fn gin_insert_cleanup_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    mode: GinCleanupMode,
) -> Result<GinCleanupStats, CatalogError> {
    let _force_cleanup = mode.force_cleanup;
    let meta = read_gin_metapage(pool, client_id, rel)?;
    if meta.pending_head == GIN_INVALID_BLOCKNO {
        return Ok(GinCleanupStats::default());
    }

    let finish_tail = (!mode.full_clean).then_some(meta.pending_tail);
    let mut block = meta.pending_head;
    let mut pending = Vec::new();
    let mut processed_pages = Vec::new();
    let mut new_head = GIN_INVALID_BLOCKNO;

    while block != GIN_INVALID_BLOCKNO {
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = validate_pending_page(&page)?;
        for item in gin_page_items(&page).map_err(page_error)? {
            pending.push(GinPendingTupleData::parse(item).map_err(page_error)?);
        }
        processed_pages.push(block);
        new_head = opaque.rightlink;
        if finish_tail == Some(block) {
            break;
        }
        block = opaque.rightlink;
    }

    let mut pending_entries: BTreeMap<GinEntryKey, BTreeSet<ItemPointerData>> = BTreeMap::new();
    for tuple in &pending {
        for entry in &tuple.entries {
            pending_entries
                .entry(entry.clone())
                .or_default()
                .insert(tuple.tid);
        }
    }
    if new_head == GIN_INVALID_BLOCKNO && gin_main_index_empty(pool, client_id, rel)? {
        let image = GinIndexImage {
            entries: pending_entries,
            pending: Vec::new(),
        };
        let pages = form_index_pages(&image, &meta.options())?;
        write_index_pages(pool, client_id, rel, &pages)?;
        return Ok(GinCleanupStats {
            pages_deleted: processed_pages.len() as i64,
            tuples_moved: pending.len() as u64,
        });
    }
    insert_entry_tids_batch(pool, client_id, rel, pending_entries, mode.fill_fsm, true)?;

    for block in &processed_pages {
        mark_gin_page_deleted(pool, client_id, rel, *block, mode.fill_fsm)?;
    }

    let mut meta = read_gin_metapage(pool, client_id, rel)?;
    meta.pending_head = new_head;
    if new_head == GIN_INVALID_BLOCKNO {
        meta.pending_tail = GIN_INVALID_BLOCKNO;
        meta.tail_free_size = 0;
    }
    refresh_gin_metapage_counts(pool, client_id, rel, &mut meta)?;
    write_gin_metapage(pool, client_id, rel, &meta)?;

    Ok(GinCleanupStats {
        pages_deleted: processed_pages.len() as i64,
        tuples_moved: pending.len() as u64,
    })
}

pub(crate) fn gin_update_options(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    options: &GinOptions,
) -> Result<(), CatalogError> {
    let mut meta = read_gin_metapage(pool, client_id, rel)?;
    meta.fastupdate = u8::from(options.fastupdate);
    meta.pending_list_limit_kb = options.pending_list_limit_kb;
    write_gin_metapage(pool, client_id, rel, &meta)
}

fn insert_entries_into_main(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    entries: BTreeSet<GinEntryKey>,
    tid: ItemPointerData,
    fill_fsm: bool,
) -> Result<(), CatalogError> {
    let entries = entries
        .into_iter()
        .map(|entry| (entry, BTreeSet::from([tid])))
        .collect();
    insert_entry_tids_batch(pool, client_id, rel, entries, fill_fsm, false)
}

fn gin_main_index_empty(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<bool, CatalogError> {
    let page = read_gin_block(pool, client_id, rel, GIN_ROOT_BLKNO)?;
    let opaque = validate_entry_page(&page)?;
    Ok(opaque.rightlink == GIN_INVALID_BLOCKNO
        && gin_page_items(&page).map_err(page_error)?.is_empty())
}

fn insert_entry_tids_batch(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    mut entries: BTreeMap<GinEntryKey, BTreeSet<ItemPointerData>>,
    fill_fsm: bool,
    refresh_meta: bool,
) -> Result<(), CatalogError> {
    let pages = entry_page_chain(pool, client_id, rel)?;
    for page in pages {
        if entries.is_empty() {
            break;
        }
        let page_tuples = entry_page_tuples(pool, client_id, rel, page.block)?;
        let upper_bound = page_tuples.last().map(|tuple| tuple.key.clone());
        let mut page_entries = Vec::new();
        while let Some((key, _)) = entries.first_key_value() {
            let belongs_on_page = page.rightlink == GIN_INVALID_BLOCKNO
                || upper_bound.as_ref().is_none_or(|upper| key <= upper);
            if !belongs_on_page {
                break;
            }
            page_entries.push(
                entries
                    .pop_first()
                    .expect("pending GIN entry must still be present"),
            );
        }
        if page_entries.is_empty() {
            continue;
        }
        merge_entry_tids_into_page(
            pool,
            client_id,
            rel,
            page,
            page_tuples,
            page_entries,
            fill_fsm,
        )?;
    }
    if refresh_meta {
        let mut meta = read_gin_metapage(pool, client_id, rel)?;
        refresh_gin_metapage_counts(pool, client_id, rel, &mut meta)?;
        write_gin_metapage(pool, client_id, rel, &meta)?;
    }
    Ok(())
}

fn entry_page_chain(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<Vec<EntryPageTarget>, CatalogError> {
    let mut block = GIN_ROOT_BLKNO;
    let mut pages = Vec::new();
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(block) {
            return Err(CatalogError::Corrupt("GIN entry page cycle"));
        }
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = validate_entry_page(&page)?;
        pages.push(EntryPageTarget {
            block,
            rightlink: opaque.rightlink,
        });
        if opaque.rightlink == GIN_INVALID_BLOCKNO {
            return Ok(pages);
        }
        block = opaque.rightlink;
    }
}

fn merge_entry_tids_into_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    page: EntryPageTarget,
    page_tuples: Vec<GinEntryTupleData>,
    entries: Vec<(GinEntryKey, BTreeSet<ItemPointerData>)>,
    fill_fsm: bool,
) -> Result<(), CatalogError> {
    let mut tuples = page_tuples
        .into_iter()
        .map(|tuple| (tuple.key.clone(), tuple))
        .collect::<BTreeMap<_, _>>();
    let mut changed = false;
    for (entry, new_tids) in entries {
        if new_tids.is_empty() {
            continue;
        }
        if let Some(tuple) = tuples.remove(&entry) {
            let old_posting_root = tuple.posting_root;
            let mut tids = entry_tuple_tids(pool, client_id, rel, &tuple)?;
            let old_len = tids.len();
            tids.extend(new_tids);
            if tids.len() == old_len {
                tuples.insert(entry, tuple);
                continue;
            }
            let merged = entry_tuple_for_tids(
                pool,
                client_id,
                rel,
                entry.clone(),
                tids,
                old_posting_root,
                fill_fsm,
            )?;
            tuples.insert(entry, merged);
            changed = true;
        } else {
            let tuple = entry_tuple_for_tids(
                pool,
                client_id,
                rel,
                entry.clone(),
                new_tids,
                None,
                fill_fsm,
            )?;
            tuples.insert(entry, tuple);
            changed = true;
        }
    }
    if changed {
        rewrite_entry_page_chain_segment(
            pool,
            client_id,
            rel,
            page.block,
            page.rightlink,
            tuples.into_values().collect(),
        )?;
    }
    Ok(())
}

fn insert_entry_tids(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    entry: GinEntryKey,
    new_tids: BTreeSet<ItemPointerData>,
    fill_fsm: bool,
) -> Result<(), CatalogError> {
    if new_tids.is_empty() {
        return Ok(());
    }
    let target = find_entry_target_page(pool, client_id, rel, &entry)?;
    let mut tuples = entry_page_tuples(pool, client_id, rel, target.block)?;
    if let Some(position) = tuples.iter().position(|tuple| tuple.key == entry) {
        let mut tids = entry_tuple_tids(pool, client_id, rel, &tuples[position])?;
        let old_len = tids.len();
        tids.extend(new_tids);
        if tids.len() == old_len {
            return Ok(());
        }
        tuples[position] = entry_tuple_for_tids(
            pool,
            client_id,
            rel,
            entry,
            tids,
            tuples[position].posting_root,
            fill_fsm,
        )?;
    } else {
        tuples.push(entry_tuple_for_tids(
            pool, client_id, rel, entry, new_tids, None, fill_fsm,
        )?);
    }
    rewrite_entry_page_chain_segment(pool, client_id, rel, target.block, target.rightlink, tuples)
}

#[derive(Debug, Clone, Copy)]
struct EntryPageTarget {
    block: u32,
    rightlink: u32,
}

fn find_entry_target_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    key: &GinEntryKey,
) -> Result<EntryPageTarget, CatalogError> {
    let mut block = GIN_ROOT_BLKNO;
    let mut last = None;
    loop {
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = validate_entry_page(&page)?;
        let tuples = entry_page_tuples_from_page(&page)?;
        if tuples.iter().any(|tuple| &tuple.key == key)
            || tuples.last().is_none_or(|tuple| key <= &tuple.key)
            || opaque.rightlink == GIN_INVALID_BLOCKNO
        {
            return Ok(EntryPageTarget {
                block,
                rightlink: opaque.rightlink,
            });
        }
        if Some(block) == last {
            return Err(CatalogError::Corrupt("GIN entry page self-link"));
        }
        last = Some(block);
        block = opaque.rightlink;
    }
}

fn validate_entry_page(page: &[u8; BLCKSZ]) -> Result<GinPageOpaqueData, CatalogError> {
    let opaque = gin_page_get_opaque(page).map_err(page_error)?;
    if opaque.flags & GIN_ENTRY == 0 || opaque.flags & GIN_DELETED != 0 {
        return Err(CatalogError::Corrupt("GIN entry page expected"));
    }
    Ok(opaque)
}

fn entry_page_tuples(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<Vec<GinEntryTupleData>, CatalogError> {
    let page = read_gin_block(pool, client_id, rel, block)?;
    validate_entry_page(&page)?;
    entry_page_tuples_from_page(&page)
}

fn entry_page_tuples_from_page(
    page: &[u8; BLCKSZ],
) -> Result<Vec<GinEntryTupleData>, CatalogError> {
    gin_page_items(page)
        .map_err(page_error)?
        .into_iter()
        .map(|item| GinEntryTupleData::parse(item).map_err(page_error))
        .collect()
}

fn entry_tuple_tids(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &GinEntryTupleData,
) -> Result<BTreeSet<ItemPointerData>, CatalogError> {
    let tids = if let Some(root) = tuple.posting_root {
        read_posting_tids_from_relation(pool, client_id, rel, root)?
    } else {
        tuple.tids.clone()
    };
    Ok(tids.into_iter().collect())
}

fn entry_tuple_for_tids(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    key: GinEntryKey,
    tids: BTreeSet<ItemPointerData>,
    old_posting_root: Option<u32>,
    fill_fsm: bool,
) -> Result<GinEntryTupleData, CatalogError> {
    let tids = tids.into_iter().collect::<Vec<_>>();
    if tids.len() <= INLINE_POSTING_LIMIT {
        if let Some(root) = old_posting_root {
            mark_posting_chain_deleted(pool, client_id, rel, root, fill_fsm)?;
        }
        return Ok(GinEntryTupleData {
            key,
            posting_root: None,
            tids,
        });
    }

    if let Some(root) = old_posting_root {
        mark_posting_chain_deleted(pool, client_id, rel, root, fill_fsm)?;
    }
    let posting_root = write_posting_chain(pool, client_id, rel, &tids)?;
    Ok(GinEntryTupleData {
        key,
        posting_root: Some(posting_root),
        tids: Vec::new(),
    })
}

fn rewrite_entry_page_chain_segment(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    first_block: u32,
    old_rightlink: u32,
    mut tuples: Vec<GinEntryTupleData>,
) -> Result<(), CatalogError> {
    tuples.sort_by(|left, right| left.key.cmp(&right.key));
    let mut pages = pack_entry_tuple_pages_without_links(&tuples)?;
    let mut blocks = vec![first_block];
    while blocks.len() < pages.len() {
        blocks.push(allocate_gin_block(pool, client_id, rel)?);
    }
    for index in 0..pages.len() {
        let rightlink = if index + 1 < pages.len() {
            blocks[index + 1]
        } else {
            old_rightlink
        };
        set_page_rightlink(&mut pages[index], rightlink)?;
        write_gin_block(pool, client_id, rel, blocks[index], &pages[index])?;
    }
    Ok(())
}

fn pack_entry_tuple_pages_without_links(
    tuples: &[GinEntryTupleData],
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
    Ok(pages)
}

fn write_posting_chain(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tids: &[ItemPointerData],
) -> Result<u32, CatalogError> {
    let mut pages = Vec::new();
    for chunk in tids.chunks(POSTING_PAGE_TID_LIMIT) {
        let mut page = empty_page(GIN_DATA | GIN_LEAF)?;
        let tuple = GinPostingTupleData {
            tids: chunk.to_vec(),
        };
        gin_page_append_item(&mut page, &tuple.serialize()).map_err(page_error)?;
        pages.push(page);
    }
    let mut blocks = Vec::with_capacity(pages.len());
    for _ in &pages {
        blocks.push(allocate_gin_block(pool, client_id, rel)?);
    }
    for index in 0..pages.len() {
        let rightlink = blocks
            .get(index + 1)
            .copied()
            .unwrap_or(GIN_INVALID_BLOCKNO);
        set_page_rightlink(&mut pages[index], rightlink)?;
        write_gin_block(pool, client_id, rel, blocks[index], &pages[index])?;
    }
    blocks
        .first()
        .copied()
        .ok_or(CatalogError::Corrupt("GIN posting chain cannot be empty"))
}

fn read_posting_tids_from_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    root: u32,
) -> Result<Vec<ItemPointerData>, CatalogError> {
    let mut block = root;
    let mut tids = Vec::new();
    let mut seen = BTreeSet::new();
    while block != GIN_INVALID_BLOCKNO {
        if !seen.insert(block) {
            return Err(CatalogError::Corrupt("GIN posting chain cycle"));
        }
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = gin_page_get_opaque(&page).map_err(page_error)?;
        if opaque.flags & GIN_DATA == 0 || opaque.flags & GIN_DELETED != 0 {
            return Err(CatalogError::Corrupt("GIN posting page expected"));
        }
        for item in gin_page_items(&page).map_err(page_error)? {
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

fn mark_posting_chain_deleted(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    root: u32,
    fill_fsm: bool,
) -> Result<(), CatalogError> {
    let mut block = root;
    let mut seen = BTreeSet::new();
    while block != GIN_INVALID_BLOCKNO {
        if !seen.insert(block) {
            return Err(CatalogError::Corrupt("GIN posting chain cycle"));
        }
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = gin_page_get_opaque(&page).map_err(page_error)?;
        let next = opaque.rightlink;
        mark_gin_page_deleted(pool, client_id, rel, block, fill_fsm)?;
        block = next;
    }
    Ok(())
}

fn mark_gin_page_deleted(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
    fill_fsm: bool,
) -> Result<(), CatalogError> {
    let mut page = read_gin_block(pool, client_id, rel, block)?;
    let mut opaque = gin_page_get_opaque(&page).map_err(page_error)?;
    if opaque.flags & GIN_DELETED == 0 {
        opaque.flags = GIN_DELETED;
        opaque.rightlink = GIN_INVALID_BLOCKNO;
        gin_page_set_opaque(&mut page, opaque).map_err(page_error)?;
        write_gin_block(pool, client_id, rel, block, &page)?;
    }
    if fill_fsm {
        record_free_index_page(pool, rel, block).map_err(CatalogError::Io)?;
    }
    Ok(())
}

fn allocate_gin_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    if let Some(block) = get_free_index_page(pool, rel).map_err(CatalogError::Io)? {
        return Ok(block);
    }
    let block = relation_nblocks(pool, rel)?;
    let reserved = empty_page(GIN_DELETED)?;
    write_gin_block(pool, client_id, rel, block, &reserved)?;
    Ok(block)
}

fn refresh_gin_metapage_counts(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    meta: &mut GinMetaPageData,
) -> Result<(), CatalogError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut entry_pages = 0u32;
    let mut data_pages = 0u32;
    let mut entries = 0u64;
    for block in 1..nblocks {
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = gin_page_get_opaque(&page).map_err(page_error)?;
        if opaque.flags & GIN_DELETED != 0 {
            continue;
        }
        if opaque.flags & GIN_ENTRY != 0 {
            entry_pages = entry_pages.saturating_add(1);
            entries =
                entries.saturating_add(gin_page_items(&page).map_err(page_error)?.len() as u64);
        } else if opaque.flags & GIN_DATA != 0 {
            data_pages = data_pages.saturating_add(1);
        }
    }

    let (pending_pages, pending_tuples, tail, tail_free_size) =
        pending_chain_stats(pool, client_id, rel, meta.pending_head)?;
    meta.n_entry_pages = entry_pages;
    meta.n_data_pages = data_pages;
    meta.n_entries = entries;
    meta.n_pending_pages = pending_pages;
    meta.n_pending_heap_tuples = pending_tuples;
    meta.pending_tail = tail;
    meta.tail_free_size = tail_free_size;
    meta.n_total_pages = nblocks;
    Ok(())
}

fn pending_chain_stats(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    head: u32,
) -> Result<(u32, u64, u32, u32), CatalogError> {
    if head == GIN_INVALID_BLOCKNO {
        return Ok((0, 0, GIN_INVALID_BLOCKNO, 0));
    }
    let mut block = head;
    let mut pages = 0u32;
    let mut tuples = 0u64;
    let mut tail = GIN_INVALID_BLOCKNO;
    let mut tail_free_size = 0u32;
    let mut seen = BTreeSet::new();
    while block != GIN_INVALID_BLOCKNO {
        if !seen.insert(block) {
            return Err(CatalogError::Corrupt("GIN pending list cycle"));
        }
        let page = read_gin_block(pool, client_id, rel, block)?;
        let opaque = validate_pending_page(&page)?;
        pages = pages.saturating_add(1);
        tuples = tuples.saturating_add(gin_page_items(&page).map_err(page_error)?.len() as u64);
        tail = block;
        tail_free_size = page_free_space(&page)? as u32;
        block = opaque.rightlink;
    }
    Ok((pages, tuples, tail, tail_free_size))
}

fn write_gin_metapage(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    meta: &GinMetaPageData,
) -> Result<(), CatalogError> {
    let mut metapage = read_gin_block(pool, client_id, rel, GIN_METAPAGE_BLKNO)?;
    gin_metapage_set_data(&mut metapage, meta).map_err(page_error)?;
    write_gin_block(pool, client_id, rel, GIN_METAPAGE_BLKNO, &metapage)
}

fn set_page_rightlink(page: &mut [u8; BLCKSZ], rightlink: u32) -> Result<(), CatalogError> {
    let mut opaque = gin_page_get_opaque(page).map_err(page_error)?;
    opaque.rightlink = rightlink;
    gin_page_set_opaque(page, opaque).map_err(page_error)
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
        let tids = if let Value::TsQuery(query) = &key.argument {
            // :HACK: tsvector GIN scans are deliberately lossy for now. The
            // executor rechecks @@ against the heap row, so correctness comes
            // from the recheck while this path provides index visibility.
            match extract_tsvector_query(attnum, query) {
                GinTsvectorQuery::All => all_attribute_tids_or_heap(scan, &image, attnum)?,
                GinTsvectorQuery::Any(entries) => union_entry_tids(&image, &entries),
            }
        } else {
            let opfamily = attnum
                .checked_sub(1)
                .and_then(|idx| scan.index_meta.opfamily_oids.get(idx as usize))
                .copied();
            let query = jsonb_ops::extract_query(attnum, key.strategy, opfamily, &key.argument)?;
            let _search_mode = jsonb_ops::query_search_mode(&query);
            match query {
                GinJsonbQuery::All => all_attribute_tids_or_heap(scan, &image, attnum)?,
                GinJsonbQuery::None => BTreeSet::new(),
                GinJsonbQuery::Any(entries) if jsonb_ops::strategy_requires_all(key.strategy) => {
                    intersect_entry_tids(&image, &entries)
                }
                GinJsonbQuery::Any(entries) => union_entry_tids(&image, &entries),
            }
        };
        result = Some(match result.take() {
            Some(existing) => existing.intersection(&tids).copied().collect(),
            None => tids,
        });
    }
    let tids = match result {
        Some(tids) => tids,
        None => all_heap_tids(scan)?,
    };
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
    let cleanup = gin_insert_cleanup(
        ctx,
        GinCleanupMode {
            full_clean: true,
            force_cleanup: true,
            fill_fsm: true,
        },
    )?;
    let options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.options())
        .or_else(|_| gin_options_from_meta(&ctx.index_meta))?;
    let mut image = read_index_image(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut out = stats.unwrap_or_default();
    out.num_deleted_pages += cleanup.pages_deleted as u64;
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
    let cleanup = gin_insert_cleanup(
        ctx,
        GinCleanupMode {
            full_clean: true,
            force_cleanup: true,
            fill_fsm: true,
        },
    )?;
    let options = read_gin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.options())
        .or_else(|_| gin_options_from_meta(&ctx.index_meta))?;
    let image = read_index_image(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let pages = form_index_pages(&image, &options)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &pages)?;
    let mut out = stats.unwrap_or_default();
    out.num_deleted_pages += cleanup.pages_deleted as u64;
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
    match value {
        Value::TsVector(vector) => {
            let mut entries = Vec::new();
            for lexeme in &vector.lexemes {
                entries.push(GinEntryKey {
                    attnum,
                    category: crate::include::access::gin::GinNullCategory::NormalKey,
                    bytes: lexeme.text.as_str().as_bytes().to_vec(),
                });
            }
            if vector.lexemes.is_empty() {
                entries.push(GinEntryKey {
                    attnum,
                    category: crate::include::access::gin::GinNullCategory::EmptyItem,
                    bytes: Vec::new(),
                });
            }
            Ok(entries)
        }
        _ => jsonb_ops::extract_value(attnum, value),
    }
}

enum GinTsvectorQuery {
    All,
    Any(Vec<GinEntryKey>),
}

fn extract_tsvector_query(attnum: u16, query: &TsQuery) -> GinTsvectorQuery {
    if tsquery_has_prefix_operand(&query.root) || tsquery_has_not_operand(&query.root) {
        return GinTsvectorQuery::All;
    }
    let mut entries = Vec::new();
    collect_tsquery_entries(attnum, &query.root, &mut entries);
    entries.sort();
    entries.dedup();
    if entries.is_empty() {
        GinTsvectorQuery::All
    } else {
        GinTsvectorQuery::Any(entries)
    }
}

fn tsquery_has_not_operand(node: &TsQueryNode) -> bool {
    match node {
        TsQueryNode::Operand(_) => false,
        TsQueryNode::Not(_) => true,
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            tsquery_has_not_operand(left) || tsquery_has_not_operand(right)
        }
        TsQueryNode::Phrase { left, right, .. } => {
            tsquery_has_not_operand(left) || tsquery_has_not_operand(right)
        }
    }
}

fn collect_tsquery_entries(attnum: u16, node: &TsQueryNode, out: &mut Vec<GinEntryKey>) {
    match node {
        TsQueryNode::Operand(operand) => out.push(GinEntryKey {
            attnum,
            category: crate::include::access::gin::GinNullCategory::NormalKey,
            bytes: operand.lexeme.as_str().as_bytes().to_vec(),
        }),
        TsQueryNode::Not(inner) => collect_tsquery_entries(attnum, inner, out),
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            collect_tsquery_entries(attnum, left, out);
            collect_tsquery_entries(attnum, right, out);
        }
        TsQueryNode::Phrase { left, right, .. } => {
            collect_tsquery_entries(attnum, left, out);
            collect_tsquery_entries(attnum, right, out);
        }
    }
}

fn tsquery_has_prefix_operand(node: &TsQueryNode) -> bool {
    match node {
        TsQueryNode::Operand(operand) => operand.prefix,
        TsQueryNode::Not(inner) => tsquery_has_prefix_operand(inner),
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            tsquery_has_prefix_operand(left) || tsquery_has_prefix_operand(right)
        }
        TsQueryNode::Phrase { left, right, .. } => {
            tsquery_has_prefix_operand(left) || tsquery_has_prefix_operand(right)
        }
    }
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

fn all_attribute_tids(image: &GinIndexImage, attnum: u16) -> BTreeSet<ItemPointerData> {
    let mut out = BTreeSet::new();
    for (key, tids) in &image.entries {
        if key.attnum == attnum && key.category != GinNullCategory::NullItem {
            out.extend(tids.iter().copied());
        }
    }
    for pending in &image.pending {
        if pending
            .entries
            .iter()
            .any(|key| key.attnum == attnum && key.category != GinNullCategory::NullItem)
        {
            out.insert(pending.tid);
        }
    }
    out
}

fn all_attribute_tids_or_heap(
    scan: &IndexScanDesc,
    image: &GinIndexImage,
    attnum: u16,
) -> Result<BTreeSet<ItemPointerData>, CatalogError> {
    let mut tids = all_attribute_tids(image, attnum);
    if tids.is_empty() {
        all_heap_tids(scan)
    } else {
        let heap_tids = all_heap_tids(scan)?;
        // :HACK: pgrust's simplified GIN vacuum can leave scan-all posting
        // sets with stale TIDs after heap pruning. Treat disjoint scan-all
        // probes as lossy and let BitmapHeap recheck preserve PostgreSQL-
        // visible semantics until GIN vacuum rewrites affected posting trees
        // with full heap-TID remapping.
        if !heap_tids.is_empty() && tids.is_disjoint(&heap_tids) {
            tids.extend(heap_tids);
        }
        Ok(tids)
    }
}

fn all_heap_tids(scan: &IndexScanDesc) -> Result<BTreeSet<ItemPointerData>, CatalogError> {
    let rel = scan
        .heap_relation
        .ok_or(CatalogError::Corrupt("GIN scan missing heap relation"))?;
    let mut out = BTreeSet::new();
    let mut heap_scan = heap_scan_begin(&scan.pool, rel)
        .map_err(|err| CatalogError::Io(format!("gin heap scan begin failed: {err:?}")))?;
    while let Some((tid, _tuple)) = heap_scan_next(&scan.pool, scan.client_id, &mut heap_scan)
        .map_err(|err| CatalogError::Io(format!("gin heap scan failed: {err:?}")))?
    {
        out.insert(tid);
    }
    Ok(out)
}

fn pending_bytes(pending: &[GinPendingTupleData]) -> usize {
    pending
        .iter()
        .map(GinPendingTupleData::serialized_len)
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
    let pending_tail_free_size = pending_pages
        .last()
        .map(page_free_space)
        .transpose()?
        .unwrap_or(0);

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
    meta.tail_free_size = pending_tail_free_size as u32;
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

    #[test]
    fn form_index_pages_records_pending_tail_free_space() {
        let mut image = GinIndexImage::default();
        image.pending.push(GinPendingTupleData {
            tid: ItemPointerData {
                block_number: 3,
                offset_number: 7,
            },
            entries: vec![GinEntryKey {
                attnum: 1,
                category: crate::include::access::gin::GinNullCategory::NormalKey,
                bytes: b"queued".to_vec(),
            }],
        });

        let pages = form_index_pages(&image, &GinOptions::default()).unwrap();
        let meta = gin_metapage_data(&pages[0]).unwrap();

        assert_ne!(meta.pending_head, GIN_INVALID_BLOCKNO);
        assert_eq!(meta.n_pending_pages, 1);
        assert_eq!(meta.n_pending_heap_tuples, 1);
        assert_eq!(
            meta.tail_free_size as usize,
            page_free_space(&pages[meta.pending_tail as usize]).unwrap()
        );
        assert!(meta.tail_free_size > 0);
    }
}
