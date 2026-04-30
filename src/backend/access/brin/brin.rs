use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    materialize_heap_row_values, project_index_key_values,
};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::page::bufpage::{PageError, max_align, page_get_item, page_header};
use crate::backend::storage::smgr::{BLCKSZ, ForkNumber, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::{InterruptState, check_for_interrupts};
use crate::backend::utils::time::snapmgr::Snapshot;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::include::access::brin_internal::{
    BRIN_PROCNUM_ADDVALUE, BRIN_PROCNUM_CONSISTENT, BRIN_PROCNUM_UNION,
};
use crate::include::access::brin_internal::{BrinDesc, BrinMemTuple, BrinTupleLocation};
use crate::include::access::brin_page::{BRIN_PAGETYPE_REGULAR, brin_page_type};
use crate::include::access::brin_revmap::normalize_range_start;
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{
    BrinIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use crate::{BufferPool, ClientId, PinnedBuffer};

use super::minmax::{BrinMinmaxStrategy, minmax_add_value, minmax_consistent, minmax_union};
use super::pageops::{
    brin_can_do_samepage_update, brin_page_get_freespace, brin_page_init,
    brin_regular_page_add_item, page_index_tuple_delete_no_compact, page_index_tuple_overwrite,
};
use super::pageops::{brin_metapage_data, brin_metapage_init};
use super::revmap::{
    BrinRevmap, brin_revmap_extend, brin_revmap_get_location, brin_revmap_get_tuple_bytes,
    brin_revmap_initialize, brin_revmap_set_location,
};
use super::tuple::{
    brin_build_desc, brin_deform_tuple, brin_form_placeholder_tuple, brin_form_tuple,
};

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

fn read_brin_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; BLCKSZ], CatalogError> {
    let pin = pin_brin_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("brin shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(format!("brin nblocks failed: {err:?}")))
}

fn read_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<Vec<[u8; BLCKSZ]>, CatalogError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut pages = Vec::with_capacity(nblocks as usize);
    for block in 0..nblocks {
        pages.push(read_brin_block(pool, client_id, rel, block)?);
    }
    Ok(pages)
}

fn write_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    pages: &[[u8; BLCKSZ]],
) -> Result<(), CatalogError> {
    for block in 0..pages.len() as u32 {
        pool.ensure_block_exists(rel, ForkNumber::Main, block)
            .map_err(|err| CatalogError::Io(format!("brin extend failed: {err:?}")))?;
    }
    for (block, page) in pages.iter().enumerate() {
        let pin = pin_brin_block(pool, client_id, rel, block as u32)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("brin exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), page, 0, &mut guard)
            .map_err(|err| CatalogError::Io(format!("brin buffered write failed: {err:?}")))?;
    }
    Ok(())
}

fn brin_pages_per_range_from_meta(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Result<u32, CatalogError> {
    let pages_per_range = index_meta
        .brin_options
        .as_ref()
        .map(|options| options.pages_per_range)
        .ok_or(CatalogError::Corrupt(
            "BRIN index metadata missing brin_options",
        ))?;
    if pages_per_range == 0 {
        return Err(CatalogError::Corrupt(
            "BRIN index metadata has invalid pages_per_range",
        ));
    }
    Ok(pages_per_range)
}

fn page_error(err: PageError) -> CatalogError {
    CatalogError::Io(format!("BRIN page error: {err:?}"))
}

fn required_amproc(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
    label: &str,
) -> Result<u32, CatalogError> {
    index_meta
        .amproc_oid(index_desc, column_index, procnum)
        .ok_or_else(|| {
            CatalogError::Io(format!(
                "missing BRIN {label} support proc for column {}",
                column_index + 1
            ))
        })
}

fn optional_amproc(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    index_meta.amproc_oid(index_desc, column_index, procnum)
}

fn add_values_to_summary(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_desc: &RelationDesc,
    desc: &BrinDesc,
    tuple: &mut BrinMemTuple,
    values: &[Value],
) -> Result<bool, CatalogError> {
    let mut modified = tuple.empty_range;
    for (column_index, value) in values.iter().enumerate() {
        let column = tuple
            .columns
            .get_mut(column_index)
            .ok_or(CatalogError::Corrupt("BRIN tuple column out of range"))?;
        let had_nulls = !tuple.empty_range && (column.has_nulls || column.all_nulls);
        if matches!(value, Value::Null) {
            if desc.info[column_index].regular_nulls && !column.has_nulls {
                column.has_nulls = true;
                modified = true;
            }
            continue;
        }

        let Some(add_value_proc) =
            optional_amproc(index_meta, index_desc, column_index, BRIN_PROCNUM_ADDVALUE)
        else {
            // :HACK: Catalog-visible BRIN opclasses outpace pgrust's BRIN
            // runtime, which currently implements minmax summaries only. Keep
            // unsupported columns all-null so catalog/property tests can build
            // mixed-opclass indexes without inventing incorrect semantics.
            continue;
        };
        modified |= minmax_add_value(add_value_proc, column, value, false)?;
        if had_nulls && !(column.has_nulls || column.all_nulls) {
            column.has_nulls = true;
        }
    }
    tuple.empty_range = false;
    Ok(modified)
}

fn union_memtuples(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_desc: &RelationDesc,
    left: &mut BrinMemTuple,
    right: &BrinMemTuple,
) -> Result<(), CatalogError> {
    if right.empty_range {
        return Ok(());
    }
    if left.empty_range {
        *left = right.clone();
        return Ok(());
    }
    for column_index in 0..left.columns.len() {
        let Some(union_proc) =
            optional_amproc(index_meta, index_desc, column_index, BRIN_PROCNUM_UNION)
        else {
            continue;
        };
        minmax_union(
            union_proc,
            &mut left.columns[column_index],
            &right.columns[column_index],
        )?;
    }
    left.empty_range = false;
    Ok(())
}

fn scan_visible_heap_summaries(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<crate::backend::access::transam::xact::TransactionManager>>,
    client_id: ClientId,
    interrupts: &Arc<InterruptState>,
    snapshot: Snapshot,
    heap_relation: RelFileLocator,
    heap_desc: &RelationDesc,
    index_desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    pages_per_range: u32,
    target_ranges: Option<&BTreeSet<u32>>,
    summaries: &mut BTreeMap<u32, BrinMemTuple>,
    desc: &BrinDesc,
) -> Result<u64, CatalogError> {
    let mut scan = heap_scan_begin_visible(pool, client_id, heap_relation, snapshot)
        .map_err(|err| CatalogError::Io(format!("brin heap scan begin failed: {err:?}")))?;
    let attr_descs = heap_desc.attribute_descs();
    let mut visible = 0u64;
    loop {
        check_for_interrupts(interrupts.as_ref()).map_err(CatalogError::Interrupted)?;
        let next = {
            let txns = txns.read();
            heap_scan_next_visible(pool, client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)): Option<(ItemPointerData, HeapTuple)> =
            next.map_err(|err| CatalogError::Io(format!("brin heap scan failed: {err:?}")))?
        else {
            break;
        };
        visible += 1;
        let range_start = normalize_range_start(pages_per_range, tid.block_number);
        if target_ranges.is_some_and(|ranges| !ranges.contains(&range_start)) {
            continue;
        }
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("brin heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(heap_desc, &datums)?;
        let key_values =
            project_index_key_values(index_desc, &index_meta.indkey, &row_values, &[])?;
        let summary = summaries
            .entry(range_start)
            .or_insert_with(|| BrinMemTuple::new(desc, range_start));
        add_values_to_summary(index_meta, index_desc, desc, summary, &key_values)?;
    }
    Ok(visible)
}

fn range_page_count(range_start: u32, pages_per_range: u32, heap_blocks: u32) -> u32 {
    heap_blocks.saturating_sub(range_start).min(pages_per_range)
}

fn store_regular_tuple(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    minimum_block: u32,
    bytes: &[u8],
) -> Result<BrinTupleLocation, CatalogError> {
    let required_size = max_align(bytes.len());
    let mut block = minimum_block.max(1);
    loop {
        while index_pages.len() <= block as usize {
            index_pages.push([0u8; BLCKSZ]);
        }
        if let Err(PageError::NotInitialized) = page_header(&index_pages[block as usize]) {
            brin_page_init(&mut index_pages[block as usize], BRIN_PAGETYPE_REGULAR)?;
        }
        if brin_page_type(&index_pages[block as usize]).map_err(page_error)?
            != BRIN_PAGETYPE_REGULAR
        {
            block += 1;
            continue;
        }
        if brin_page_get_freespace(&index_pages[block as usize])? < required_size {
            block += 1;
            continue;
        }
        let offset = brin_regular_page_add_item(&mut index_pages[block as usize], bytes)?;
        return Ok(BrinTupleLocation { block, offset });
    }
}

fn upsert_summary_tuple(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    range_start: u32,
    bytes: &[u8],
) -> Result<(), CatalogError> {
    brin_revmap_extend(index_pages, revmap, range_start)?;
    let location = brin_revmap_get_location(index_pages, revmap, range_start)?;
    if !location.is_valid() {
        let new_location = store_regular_tuple(index_pages, revmap.last_revmap_page + 1, bytes)?;
        brin_revmap_set_location(index_pages, revmap, range_start, new_location)?;
        return Ok(());
    }

    let page = index_pages
        .get_mut(location.block as usize)
        .ok_or(CatalogError::Corrupt("missing BRIN summary page"))?;
    let old_bytes = page_get_item(page, location.offset)
        .map_err(page_error)?
        .to_vec();
    if brin_can_do_samepage_update(page, old_bytes.len(), bytes.len())?
        && page_index_tuple_overwrite(page, location.offset, bytes)?
    {
        return Ok(());
    }

    let new_location = store_regular_tuple(index_pages, revmap.last_revmap_page + 1, bytes)?;
    brin_revmap_set_location(index_pages, revmap, range_start, new_location)?;
    let old_page = index_pages
        .get_mut(location.block as usize)
        .ok_or(CatalogError::Corrupt("missing BRIN old summary page"))?;
    page_index_tuple_delete_no_compact(old_page, location.offset)?;
    Ok(())
}

fn summarize_unsummarized_ranges(
    ctx: &IndexVacuumContext,
    pages_per_range: u32,
) -> Result<(Vec<[u8; BLCKSZ]>, u64), CatalogError> {
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    let desc = brin_build_desc(&ctx.index_desc);
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    let mut target_ranges = BTreeSet::new();
    for range_start in (0..heap_blocks).step_by(pages_per_range as usize) {
        if !brin_revmap_get_location(&index_pages, &revmap, range_start)?.is_valid() {
            target_ranges.insert(range_start);
        }
    }
    if target_ranges.is_empty() {
        return Ok((index_pages, 0));
    }

    for range_start in &target_ranges {
        let placeholder = brin_form_placeholder_tuple(&desc, *range_start)?;
        upsert_summary_tuple(
            &mut index_pages,
            &mut revmap,
            *range_start,
            &placeholder.bytes,
        )?;
    }

    let mut summaries = target_ranges
        .iter()
        .map(|range_start| (*range_start, BrinMemTuple::new(&desc, *range_start)))
        .collect::<BTreeMap<_, _>>();
    let visible = scan_visible_heap_summaries(
        &ctx.pool,
        &ctx.txns,
        ctx.client_id,
        &ctx.interrupts,
        Snapshot::bootstrap(),
        ctx.heap_relation,
        &ctx.heap_desc,
        &ctx.index_desc,
        &ctx.index_meta,
        pages_per_range,
        Some(&target_ranges),
        &mut summaries,
        &desc,
    )?;
    for (range_start, summary) in summaries {
        let tuple = brin_form_tuple(&desc, &summary)?;
        upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    }
    Ok((index_pages, visible))
}

fn range_matches_scan(
    scan: &IndexScanDesc,
    desc: &BrinDesc,
    tuple: &BrinMemTuple,
) -> Result<bool, CatalogError> {
    if scan.key_data.is_empty() {
        return Ok(true);
    }
    if tuple.empty_range {
        return Ok(false);
    }
    for key in &scan.key_data {
        let column_index = usize::try_from(key.attribute_number.saturating_sub(1))
            .map_err(|_| CatalogError::Corrupt("invalid BRIN scan key attribute"))?;
        let column = tuple
            .columns
            .get(column_index)
            .ok_or(CatalogError::Corrupt(
                "BRIN scan key attribute out of range",
            ))?;
        if column.all_nulls || matches!(key.argument, Value::Null) {
            return Ok(false);
        }
        let strategy = BrinMinmaxStrategy::try_from(key.strategy as i16)?;
        let Some(consistent_proc) = optional_amproc(
            &scan.index_meta,
            &scan.index_desc,
            column_index,
            BRIN_PROCNUM_CONSISTENT,
        ) else {
            return Ok(true);
        };
        if !minmax_consistent(consistent_proc, column, strategy, &key.argument)? {
            return Ok(false);
        }
    }
    let _ = desc;
    Ok(true)
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
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    if heap_blocks == 0 {
        return Ok(IndexBuildResult::default());
    }
    let pages_per_range = read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?;
    let desc = brin_build_desc(&ctx.index_desc);
    let mut summaries = (0..heap_blocks)
        .step_by(pages_per_range as usize)
        .map(|range_start| (range_start, BrinMemTuple::new(&desc, range_start)))
        .collect::<BTreeMap<_, _>>();
    let heap_tuples = scan_visible_heap_summaries(
        &ctx.pool,
        &ctx.txns,
        ctx.client_id,
        &ctx.interrupts,
        ctx.snapshot.clone(),
        ctx.heap_relation,
        &ctx.heap_desc,
        &ctx.index_desc,
        &ctx.index_meta,
        pages_per_range,
        None,
        &mut summaries,
        &desc,
    )?;

    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    for (range_start, summary) in summaries.values().map(|tuple| (tuple.blkno, tuple)) {
        let tuple = brin_form_tuple(&desc, summary)?;
        upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    }
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;

    Ok(IndexBuildResult {
        heap_tuples,
        index_tuples: summaries.len() as u64,
    })
}

pub(crate) fn brinbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    let pages_per_range = brin_pages_per_range_from_meta(&ctx.index_meta)?;
    ctx.pool
        .ensure_relation_fork(ctx.index_relation, ForkNumber::Main)
        .map_err(|err| CatalogError::Io(format!("brin ensure relation failed: {err:?}")))?;
    ctx.pool
        .with_storage_mut(|storage| {
            storage
                .smgr
                .truncate(ctx.index_relation, ForkNumber::Main, 0)?;
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

pub(crate) fn brininsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    let range_start = normalize_range_start(
        read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
            .map(|meta| meta.pages_per_range)
            .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?,
        ctx.heap_tid.block_number,
    );
    let desc = brin_build_desc(&ctx.index_desc);
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    let Some((_location, bytes)) = brin_revmap_get_tuple_bytes(&index_pages, &revmap, range_start)?
    else {
        return Ok(false);
    };
    let mut summary = brin_deform_tuple(&desc, bytes)?;
    if summary.placeholder {
        return Ok(false);
    }
    if !add_values_to_summary(
        &ctx.index_meta,
        &ctx.index_desc,
        &desc,
        &mut summary,
        &ctx.values,
    )? {
        return Ok(false);
    }
    let tuple = brin_form_tuple(&desc, &summary)?;
    upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
    Ok(false)
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
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    let heap_relation = scan.heap_relation.ok_or(CatalogError::Corrupt(
        "BRIN bitmap scan missing heap relation",
    ))?;
    let heap_blocks = relation_nblocks(&scan.pool, heap_relation)?;
    let index_pages = read_index_pages(&scan.pool, scan.client_id, scan.index_relation)?;
    let revmap = brin_revmap_initialize(&index_pages)?;
    let desc = brin_build_desc(&scan.index_desc);
    let pages_per_range = {
        let IndexScanOpaque::Brin(state) = &mut scan.opaque else {
            return Err(CatalogError::Corrupt("BRIN scan state missing opaque"));
        };
        state.scan_started = true;
        state.pages_per_range
    };

    let mut total_pages = 0i64;
    for range_start in (0..heap_blocks).step_by(pages_per_range as usize) {
        let add_range = match brin_revmap_get_tuple_bytes(&index_pages, &revmap, range_start)? {
            None => true,
            Some((_location, bytes)) => {
                let tuple = brin_deform_tuple(&desc, bytes)?;
                tuple.placeholder || range_matches_scan(scan, &desc, &tuple)?
            }
        };
        if add_range {
            let page_count = range_page_count(range_start, pages_per_range, heap_blocks);
            bitmap.add_range(range_start, page_count);
            total_pages += i64::from(page_count);
        }
    }
    Ok(total_pages.saturating_mul(10))
}

pub(crate) fn brinendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}

pub(crate) fn brinbulkdelete(
    _ctx: &IndexVacuumContext,
    _callback: &crate::include::access::amapi::IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

pub(crate) fn brinvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let pages_per_range = read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?;
    let (index_pages, _visible) = summarize_unsummarized_ranges(ctx, pages_per_range)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    let mut out = stats.unwrap_or_default();
    out.num_pages = index_pages.len() as u64;
    out.num_index_tuples = if heap_blocks == 0 {
        0
    } else {
        ((heap_blocks + pages_per_range - 1) / pages_per_range) as u64
    };
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend::access::heap::heapam::heap_insert;
    use crate::backend::access::transam::xact::TransactionManager;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::value_io::encode_tuple_values;
    use crate::backend::storage::smgr::md::MdStorageManager;
    use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
    use crate::backend::utils::misc::interrupts::InterruptState;
    use crate::include::access::brin::BrinOptions;
    use crate::include::catalog::{
        BRIN_AM_OID, BRIN_MINMAX_ADD_VALUE_PROC_OID, BRIN_MINMAX_CONSISTENT_PROC_OID,
        BRIN_MINMAX_OPCINFO_PROC_OID, BRIN_MINMAX_UNION_PROC_OID, INT4_TYPE_OID,
    };
    use crate::include::nodes::datum::Value;
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
            indnatts: 1,
            indnkeyatts: 1,
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
            indkey: vec![1],
            indclass: vec![crate::include::catalog::INT4_BRIN_MINMAX_OPCLASS_OID],
            indclass_options: vec![Vec::new()],
            indcollation: Vec::new(),
            indoption: Vec::new(),
            opfamily_oids: vec![crate::include::catalog::BRIN_INTEGER_MINMAX_FAMILY_OID],
            opcintype_oids: vec![INT4_TYPE_OID],
            opckeytype_oids: vec![0],
            amop_entries: Vec::new(),
            amproc_entries: vec![vec![
                crate::backend::utils::cache::relcache::IndexAmProcEntry {
                    procnum: 1,
                    lefttype: INT4_TYPE_OID,
                    righttype: INT4_TYPE_OID,
                    proc_oid: BRIN_MINMAX_OPCINFO_PROC_OID,
                },
                crate::backend::utils::cache::relcache::IndexAmProcEntry {
                    procnum: 2,
                    lefttype: INT4_TYPE_OID,
                    righttype: INT4_TYPE_OID,
                    proc_oid: BRIN_MINMAX_ADD_VALUE_PROC_OID,
                },
                crate::backend::utils::cache::relcache::IndexAmProcEntry {
                    procnum: 3,
                    lefttype: INT4_TYPE_OID,
                    righttype: INT4_TYPE_OID,
                    proc_oid: BRIN_MINMAX_CONSISTENT_PROC_OID,
                },
                crate::backend::utils::cache::relcache::IndexAmProcEntry {
                    procnum: 4,
                    lefttype: INT4_TYPE_OID,
                    righttype: INT4_TYPE_OID,
                    proc_oid: BRIN_MINMAX_UNION_PROC_OID,
                },
            ]],
            indexprs: None,
            indpred: None,
            rd_indexprs: None,
            rd_indpred: None,
            btree_options: None,
            brin_options: Some(BrinOptions { pages_per_range }),
            gist_options: None,
            gin_options: None,
            hash_options: None,
        }
    }

    fn heap_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                column_desc(
                    "a",
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                    false,
                ),
                column_desc(
                    "pad",
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                    false,
                ),
            ],
        }
    }

    fn index_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![column_desc(
                "a",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            )],
        }
    }

    fn heap_tuple(values: &[Value]) -> HeapTuple {
        let desc = heap_desc();
        let encoded = encode_tuple_values(&desc, values).unwrap();
        HeapTuple::from_values(&desc.attribute_descs(), &encoded).unwrap()
    }

    fn insert_heap_row(
        pool: &BufferPool<SmgrStorageBackend>,
        rel: RelFileLocator,
        value: i32,
        pad_bytes: usize,
    ) -> crate::include::access::itemptr::ItemPointerData {
        pool.ensure_relation_fork(rel, ForkNumber::Main).unwrap();
        heap_insert(
            pool,
            0,
            rel,
            &heap_tuple(&[
                Value::Int32(value),
                Value::Text("x".repeat(pad_bytes).into()),
            ]),
        )
        .unwrap()
    }

    fn summary_for_range(
        pool: &Arc<BufferPool<SmgrStorageBackend>>,
        index_rel: RelFileLocator,
        _index_meta: &IndexRelCacheEntry,
        range_start: u32,
    ) -> Option<BrinMemTuple> {
        let pages = read_index_pages(pool, 0, index_rel).unwrap();
        let revmap = brin_revmap_initialize(&pages).unwrap();
        let desc = brin_build_desc(&index_desc());
        brin_revmap_get_tuple_bytes(&pages, &revmap, range_start)
            .unwrap()
            .map(|(_, bytes)| brin_deform_tuple(&desc, bytes).unwrap())
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
        assert_eq!(
            meta.brin_magic,
            crate::include::access::brin_page::BRIN_META_MAGIC
        );
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

    #[test]
    fn brinbuild_summarizes_ranges_and_bitmap_scan_matches() {
        let base = temp_dir("build_bitmap");
        let pool = Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(&base)),
            16,
        ));
        let txns = Arc::new(RwLock::new(TransactionManager::new_ephemeral()));
        let interrupts = Arc::new(InterruptState::new());
        let heap_rel = test_rel(8100);
        let index_rel = test_rel(8101);
        let meta = test_index_meta(1);

        for value in [10, 11] {
            insert_heap_row(&pool, heap_rel, value, 3000);
        }
        for value in [100, 101] {
            insert_heap_row(&pool, heap_rel, value, 3000);
        }
        for value in [1000, 1001] {
            insert_heap_row(&pool, heap_rel, value, 3000);
        }

        let result = brinbuild(&IndexBuildContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            client_id: 0,
            interrupts: Arc::clone(&interrupts),
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            heap_desc: heap_desc(),
            index_relation: index_rel,
            index_name: "brin_idx".into(),
            index_desc: index_desc(),
            index_meta: meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            maintenance_work_mem_kb: 64,
            expr_eval: None,
        })
        .unwrap();
        assert_eq!(result.heap_tuples, 6);
        assert!(result.index_tuples >= 3);

        let first = summary_for_range(&pool, index_rel, &meta, 0).unwrap();
        assert_eq!(
            first.columns[0].values,
            vec![Value::Int32(10), Value::Int32(11)]
        );
        let second = summary_for_range(&pool, index_rel, &meta, 1).unwrap();
        assert_eq!(
            second.columns[0].values,
            vec![Value::Int32(100), Value::Int32(101)]
        );
        let third = summary_for_range(&pool, index_rel, &meta, 2).unwrap();
        assert_eq!(
            third.columns[0].values,
            vec![Value::Int32(1000), Value::Int32(1001)]
        );

        let mut scan = brinbeginscan(&IndexBeginScanContext {
            pool: Arc::clone(&pool),
            client_id: 0,
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            index_relation: index_rel,
            index_desc: index_desc(),
            index_meta: meta,
            key_data: vec![
                ScanKeyData {
                    attribute_number: 1,
                    strategy: 4,
                    argument: Value::Int32(100),
                },
                ScanKeyData {
                    attribute_number: 1,
                    strategy: 1,
                    argument: Value::Int32(200),
                },
            ],
            order_by_data: Vec::new(),
            direction: ScanDirection::Forward,
            want_itup: false,
        })
        .unwrap();
        let mut bitmap = TidBitmap::new();
        bringetbitmap(&mut scan, &mut bitmap).unwrap();
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn brininsert_updates_existing_summary_range() {
        let base = temp_dir("insert_updates_range");
        let pool = Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(&base)),
            16,
        ));
        let txns = Arc::new(RwLock::new(TransactionManager::new_ephemeral()));
        let interrupts = Arc::new(InterruptState::new());
        let heap_rel = test_rel(8200);
        let index_rel = test_rel(8201);
        let meta = test_index_meta(4);

        insert_heap_row(&pool, heap_rel, 10, 200);
        brinbuild(&IndexBuildContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            client_id: 0,
            interrupts: Arc::clone(&interrupts),
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            heap_desc: heap_desc(),
            index_relation: index_rel,
            index_name: "brin_idx".into(),
            index_desc: index_desc(),
            index_meta: meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            maintenance_work_mem_kb: 64,
            expr_eval: None,
        })
        .unwrap();

        let tid = insert_heap_row(&pool, heap_rel, 50, 200);
        brininsert(&IndexInsertContext {
            pool: Arc::clone(&pool),
            txns,
            txn_waiter: None,
            client_id: 0,
            interrupts,
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            heap_desc: heap_desc(),
            index_relation: index_rel,
            index_name: "brin_idx".into(),
            index_desc: index_desc(),
            index_meta: meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            heap_tid: tid,
            old_heap_tid: None,
            values: vec![Value::Int32(50)],
            unique_check: crate::include::access::amapi::IndexUniqueCheck::No,
        })
        .unwrap();

        let summary = summary_for_range(&pool, index_rel, &meta, 0).unwrap();
        assert_eq!(
            summary.columns[0].values,
            vec![Value::Int32(10), Value::Int32(50)]
        );
    }

    #[test]
    fn brinvacuumcleanup_summarizes_unsummarized_tail_ranges() {
        let base = temp_dir("vacuum_summarizes_tail");
        let pool = Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(&base)),
            16,
        ));
        let txns = Arc::new(RwLock::new(TransactionManager::new_ephemeral()));
        let interrupts = Arc::new(InterruptState::new());
        let heap_rel = test_rel(8300);
        let index_rel = test_rel(8301);
        let meta = test_index_meta(1);

        for value in [10, 11] {
            insert_heap_row(&pool, heap_rel, value, 3000);
        }
        brinbuild(&IndexBuildContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            client_id: 0,
            interrupts: Arc::clone(&interrupts),
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            heap_desc: heap_desc(),
            index_relation: index_rel,
            index_name: "brin_idx".into(),
            index_desc: index_desc(),
            index_meta: meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            maintenance_work_mem_kb: 64,
            expr_eval: None,
        })
        .unwrap();

        let tid = insert_heap_row(&pool, heap_rel, 1000, 3000);
        let tail_range = normalize_range_start(1, tid.block_number);
        brininsert(&IndexInsertContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            txn_waiter: None,
            client_id: 0,
            interrupts: Arc::clone(&interrupts),
            snapshot: Snapshot::bootstrap(),
            heap_relation: heap_rel,
            heap_desc: heap_desc(),
            index_relation: index_rel,
            index_name: "brin_idx".into(),
            index_desc: index_desc(),
            index_meta: meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            heap_tid: tid,
            old_heap_tid: None,
            values: vec![Value::Int32(1000)],
            unique_check: crate::include::access::amapi::IndexUniqueCheck::No,
        })
        .unwrap();

        assert!(summary_for_range(&pool, index_rel, &meta, tail_range).is_none());

        let stats = brinvacuumcleanup(
            &IndexVacuumContext {
                pool: Arc::clone(&pool),
                txns,
                client_id: 0,
                interrupts,
                heap_relation: heap_rel,
                heap_desc: heap_desc(),
                index_relation: index_rel,
                index_name: "brin_idx".into(),
                index_desc: index_desc(),
                index_meta: meta.clone(),
            },
            None,
        )
        .unwrap();
        let heap_blocks = relation_nblocks(&pool, heap_rel).unwrap();
        let expected_ranges = u64::from(heap_blocks);
        assert_eq!(stats.num_index_tuples, expected_ranges);
        let summary = summary_for_range(&pool, index_rel, &meta, tail_range).unwrap();
        assert_eq!(
            summary.columns[0].values,
            vec![Value::Int32(1000), Value::Int32(1000)]
        );
    }
}
