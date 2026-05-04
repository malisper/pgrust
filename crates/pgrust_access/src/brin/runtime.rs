use std::collections::{BTreeMap, BTreeSet};

use pgrust_catalog_data::{
    BRIN_BOX_INCLUSION_FAMILY_OID, BRIN_BPCHAR_BLOOM_FAMILY_OID, BRIN_BYTEA_BLOOM_FAMILY_OID,
    BRIN_CHAR_BLOOM_FAMILY_OID, BRIN_DATETIME_BLOOM_FAMILY_OID, BRIN_FLOAT_BLOOM_FAMILY_OID,
    BRIN_INTEGER_BLOOM_FAMILY_OID, BRIN_INTERVAL_BLOOM_FAMILY_OID, BRIN_MACADDR_BLOOM_FAMILY_OID,
    BRIN_MACADDR8_BLOOM_FAMILY_OID, BRIN_NAME_BLOOM_FAMILY_OID, BRIN_NETWORK_BLOOM_FAMILY_OID,
    BRIN_NETWORK_INCLUSION_FAMILY_OID, BRIN_NUMERIC_BLOOM_FAMILY_OID, BRIN_OID_BLOOM_FAMILY_OID,
    BRIN_PG_LSN_BLOOM_FAMILY_OID, BRIN_RANGE_INCLUSION_FAMILY_OID, BRIN_TEXT_BLOOM_FAMILY_OID,
    BRIN_TID_BLOOM_FAMILY_OID, BRIN_TIME_BLOOM_FAMILY_OID, BRIN_TIMETZ_BLOOM_FAMILY_OID,
    BRIN_UUID_BLOOM_FAMILY_OID,
};
use pgrust_core::{ClientId, RelFileLocator, Snapshot};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::{ColumnDesc, RelationDesc, ToastRelationRef};
use pgrust_nodes::relcache::IndexRelCacheEntry;
use pgrust_storage::page::bufpage::{PageError, max_align, page_get_item, page_header};
use pgrust_storage::smgr::{ForkNumber, StorageManager};
use pgrust_storage::{BLCKSZ, BufferPool, PinnedBuffer, SmgrStorageBackend};

use crate::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::access::brin::BRIN_DEFAULT_PAGES_PER_RANGE;
use crate::access::brin_internal::{
    BRIN_PROCNUM_ADDVALUE, BRIN_PROCNUM_CONSISTENT, BrinDesc, BrinMemTuple, BrinTupleLocation,
};
use crate::access::brin_page::{
    BRIN_CURRENT_VERSION, BRIN_METAPAGE_BLKNO, BRIN_PAGETYPE_REGULAR, BrinMetaPageData,
    brin_page_type,
};
use crate::access::brin_revmap::normalize_range_start;
use crate::access::relscan::{BrinIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::access::scankey::ScanKeyData;
use crate::access::tidbitmap::TidBitmap;
use crate::{
    AccessError, AccessHeapServices, AccessIndexServices, AccessResult, AccessScalarServices,
    AccessWalServices,
};

use super::minmax::{
    BrinMinmaxStrategy, minmax_add_value, minmax_consistent, minmax_multi_add_value,
    minmax_multi_consistent,
};
use super::pageops::{
    brin_can_do_samepage_update, brin_metapage_data, brin_metapage_init, brin_page_get_freespace,
    brin_page_init, brin_regular_page_add_item, page_index_tuple_delete_no_compact,
    page_index_tuple_overwrite,
};
use super::revmap::{
    BrinRevmap, brin_revmap_desummarize_range, brin_revmap_extend, brin_revmap_get_location,
    brin_revmap_get_tuple_bytes, brin_revmap_initialize, brin_revmap_set_location,
};
use super::tuple::{
    brin_build_desc_with_meta, brin_deform_tuple, brin_form_placeholder_tuple, brin_form_tuple,
    brin_opfamily_is_minmax_multi,
};

type CatalogError = AccessError;

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
) -> Result<BrinMetaPageData, CatalogError> {
    let pin = pin_brin_block(pool, client_id, rel, BRIN_METAPAGE_BLKNO)?;
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

fn brin_pages_per_range_from_meta(index_meta: &IndexRelCacheEntry) -> Result<u32, CatalogError> {
    let pages_per_range = index_meta
        .brin_options
        .as_ref()
        .map(|options| options.pages_per_range)
        // :HACK: Some rebuild paths currently reconstruct relcache metadata
        // without persisted BRIN reloptions before the new metapage exists.
        // PostgreSQL falls back to the access method default in that case.
        .unwrap_or(BRIN_DEFAULT_PAGES_PER_RANGE);
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

fn index_amproc_oid(
    index_meta: &IndexRelCacheEntry,
    _index_desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    index_meta
        .amproc_entries
        .get(column_index)?
        .iter()
        .find(|entry| entry.procnum == procnum)
        .map(|entry| entry.proc_oid)
}

fn optional_amproc(
    index_meta: &IndexRelCacheEntry,
    index_desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    index_amproc_oid(index_meta, index_desc, column_index, procnum)
}

fn add_values_to_summary(
    index_meta: &IndexRelCacheEntry,
    index_desc: &RelationDesc,
    desc: &BrinDesc,
    tuple: &mut BrinMemTuple,
    values: &[Value],
    scalar: &dyn AccessScalarServices,
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

        if brin_opfamily_is_minmax_multi(index_meta.opfamily_oids.get(column_index).copied()) {
            modified |= minmax_multi_add_value(column, value, false, scalar)?;
            if had_nulls && !(column.has_nulls || column.all_nulls) {
                column.has_nulls = true;
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
        modified |= minmax_add_value(add_value_proc, column, value, false, scalar)?;
        if had_nulls && !(column.has_nulls || column.all_nulls) {
            column.has_nulls = true;
        }
    }
    tuple.empty_range = false;
    Ok(modified)
}

fn missing_column_value(column: &ColumnDesc) -> Value {
    if column.generated.is_some() {
        return Value::Null;
    }
    column.missing_default_value.clone().unwrap_or(Value::Null)
}

fn materialize_heap_row_values(
    heap_desc: &RelationDesc,
    heap: &dyn AccessHeapServices,
    snapshot: &Snapshot,
    heap_toast: Option<ToastRelationRef>,
    datums: &[Option<&[u8]>],
    scalar: &dyn AccessScalarServices,
) -> AccessResult<Vec<Value>> {
    let mut row_values = Vec::with_capacity(heap_desc.columns.len());
    for (index, column) in heap_desc.columns.iter().enumerate() {
        row_values.push(if let Some(datum) = datums.get(index) {
            if let Some(toast) = heap_toast {
                let mut fetch_external =
                    |raw: &[u8]| heap.fetch_toast_value_bytes(toast, snapshot, raw);
                scalar.decode_value_with_external_toast(
                    column,
                    *datum,
                    Some(&mut fetch_external),
                )?
            } else {
                scalar.decode_value_with_external_toast(column, *datum, None)?
            }
        } else {
            missing_column_value(column)
        });
    }
    Ok(row_values)
}

fn project_plain_index_key_values(
    index_desc: &RelationDesc,
    indkey: &[i16],
    row_values: &[Value],
) -> AccessResult<Vec<Value>> {
    let mut values = Vec::with_capacity(index_desc.columns.len());
    for attnum in indkey.iter().copied().take(index_desc.columns.len()) {
        if attnum <= 0 {
            return Err(AccessError::Unsupported(
                "BRIN expression index projection requires index services".into(),
            ));
        }
        let index = usize::try_from(attnum - 1)
            .map_err(|_| AccessError::Corrupt("invalid BRIN index key attnum"))?;
        values.push(row_values.get(index).cloned().unwrap_or(Value::Null));
    }
    Ok(values)
}

fn scan_visible_heap_summaries(
    heap: &dyn AccessHeapServices,
    mut index: Option<&mut dyn AccessIndexServices>,
    scalar: &dyn AccessScalarServices,
    snapshot: Snapshot,
    heap_toast: Option<ToastRelationRef>,
    heap_relation: RelFileLocator,
    heap_desc: &RelationDesc,
    index_desc: &RelationDesc,
    index_meta: &IndexRelCacheEntry,
    pages_per_range: u32,
    target_ranges: Option<&BTreeSet<u32>>,
    summaries: &mut BTreeMap<u32, BrinMemTuple>,
    desc: &BrinDesc,
) -> AccessResult<u64> {
    let attr_descs = heap_desc.attribute_descs();
    let decode_snapshot = snapshot.clone();
    let mut visible = 0u64;
    heap.for_each_visible_heap_tuple(heap_relation, snapshot, &mut |tid, tuple| {
        visible += 1;
        let range_start = normalize_range_start(pages_per_range, tid.block_number);
        if target_ranges.is_some_and(|ranges| !ranges.contains(&range_start)) {
            return Ok(());
        }
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| AccessError::Scalar(format!("brin heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(
            heap_desc,
            heap,
            &decode_snapshot,
            heap_toast,
            &datums,
            scalar,
        )?;
        let key_values = if let Some(index) = index.as_deref_mut() {
            let Some(key_values) = index.project_index_row(index_meta, &row_values, tid)? else {
                return Ok(());
            };
            key_values
        } else {
            project_plain_index_key_values(index_desc, &index_meta.indkey, &row_values)?
        };
        let summary = summaries
            .entry(range_start)
            .or_insert_with(|| BrinMemTuple::new(desc, range_start));
        add_values_to_summary(index_meta, index_desc, desc, summary, &key_values, scalar)?;
        Ok(())
    })?;
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
    heap: &dyn AccessHeapServices,
    index: Option<&mut dyn AccessIndexServices>,
    scalar: &dyn AccessScalarServices,
) -> Result<(Vec<[u8; BLCKSZ]>, u64), CatalogError> {
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    let desc = brin_build_desc_with_meta(&ctx.index_desc, Some(&ctx.index_meta));
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
        let placeholder = brin_form_placeholder_tuple(&desc, *range_start, scalar)?;
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
        heap,
        index,
        scalar,
        Snapshot::bootstrap(),
        ctx.heap_toast,
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
        let tuple = brin_form_tuple(&desc, &summary, scalar)?;
        upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    }
    Ok((index_pages, visible))
}

fn brin_pages_per_range_for_vacuum_context(ctx: &IndexVacuumContext) -> Result<u32, CatalogError> {
    read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))
}

pub fn brin_summarize_new_values(
    ctx: &IndexVacuumContext,
    heap: &dyn AccessHeapServices,
    index: Option<&mut dyn AccessIndexServices>,
    scalar: &dyn AccessScalarServices,
) -> Result<i32, CatalogError> {
    let pages_per_range = brin_pages_per_range_for_vacuum_context(ctx)?;
    let index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let revmap = brin_revmap_initialize(&index_pages)?;
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    let mut summarized_ranges = 0i32;
    for range_start in (0..heap_blocks).step_by(pages_per_range as usize) {
        if !brin_revmap_get_location(&index_pages, &revmap, range_start)?.is_valid() {
            summarized_ranges = summarized_ranges.saturating_add(1);
        }
    }
    if summarized_ranges == 0 {
        return Ok(0);
    }

    let (index_pages, _visible) =
        summarize_unsummarized_ranges(ctx, pages_per_range, heap, index, scalar)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
    Ok(summarized_ranges)
}

pub fn brin_summarize_range(
    ctx: &IndexVacuumContext,
    heap_block: u32,
    heap: &dyn AccessHeapServices,
    index: Option<&mut dyn AccessIndexServices>,
    scalar: &dyn AccessScalarServices,
) -> Result<i32, CatalogError> {
    let pages_per_range = brin_pages_per_range_for_vacuum_context(ctx)?;
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    if heap_block >= heap_blocks {
        return Ok(0);
    }
    let range_start = normalize_range_start(pages_per_range, heap_block);
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    if brin_revmap_get_location(&index_pages, &revmap, range_start)?.is_valid() {
        return Ok(0);
    }

    let desc = brin_build_desc_with_meta(&ctx.index_desc, Some(&ctx.index_meta));
    let placeholder = brin_form_placeholder_tuple(&desc, range_start, scalar)?;
    upsert_summary_tuple(
        &mut index_pages,
        &mut revmap,
        range_start,
        &placeholder.bytes,
    )?;

    let mut target_ranges = BTreeSet::new();
    target_ranges.insert(range_start);
    let mut summaries = BTreeMap::from([(range_start, BrinMemTuple::new(&desc, range_start))]);
    let _visible = scan_visible_heap_summaries(
        heap,
        index,
        scalar,
        Snapshot::bootstrap(),
        ctx.heap_toast,
        ctx.heap_relation,
        &ctx.heap_desc,
        &ctx.index_desc,
        &ctx.index_meta,
        pages_per_range,
        Some(&target_ranges),
        &mut summaries,
        &desc,
    )?;
    let summary = summaries
        .remove(&range_start)
        .ok_or(CatalogError::Corrupt("BRIN summary range missing"))?;
    let tuple = brin_form_tuple(&desc, &summary, scalar)?;
    upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
    Ok(1)
}

pub fn brin_desummarize_range(
    ctx: &IndexVacuumContext,
    heap_block: u32,
) -> Result<(), CatalogError> {
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    brin_revmap_desummarize_range(&mut index_pages, &mut revmap, heap_block)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)
}

fn range_matches_scan(
    scan: &IndexScanDesc,
    desc: &BrinDesc,
    tuple: &BrinMemTuple,
    scalar: &dyn AccessScalarServices,
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
        let opfamily_oid = scan.index_meta.opfamily_oids.get(column_index).copied();
        if is_lossy_brin_family(opfamily_oid) {
            // :HACK: pgrust only implements BRIN minmax consistency today.
            // Inclusion and bloom opclasses still build catalog-compatible
            // indexes, but scans must include the range and let heap recheck
            // decide until their native summaries are implemented.
            continue;
        }
        let strategy = BrinMinmaxStrategy::try_from(key.strategy as i16)?;
        if brin_opfamily_is_minmax_multi(opfamily_oid) {
            if !minmax_multi_consistent(column, strategy, &key.argument, scalar)? {
                return Ok(false);
            }
            continue;
        }
        let Some(consistent_proc) = optional_amproc(
            &scan.index_meta,
            &scan.index_desc,
            column_index,
            BRIN_PROCNUM_CONSISTENT,
        ) else {
            return Ok(true);
        };
        if !minmax_consistent(consistent_proc, column, strategy, &key.argument, scalar)? {
            return Ok(false);
        }
    }
    let _ = desc;
    Ok(true)
}

fn is_lossy_brin_family(opfamily_oid: Option<u32>) -> bool {
    matches!(
        opfamily_oid,
        Some(
            BRIN_NETWORK_INCLUSION_FAMILY_OID
                | BRIN_RANGE_INCLUSION_FAMILY_OID
                | BRIN_BOX_INCLUSION_FAMILY_OID
                | BRIN_BYTEA_BLOOM_FAMILY_OID
                | BRIN_CHAR_BLOOM_FAMILY_OID
                | BRIN_NAME_BLOOM_FAMILY_OID
                | BRIN_INTEGER_BLOOM_FAMILY_OID
                | BRIN_TEXT_BLOOM_FAMILY_OID
                | BRIN_OID_BLOOM_FAMILY_OID
                | BRIN_TID_BLOOM_FAMILY_OID
                | BRIN_FLOAT_BLOOM_FAMILY_OID
                | BRIN_MACADDR_BLOOM_FAMILY_OID
                | BRIN_MACADDR8_BLOOM_FAMILY_OID
                | BRIN_NETWORK_BLOOM_FAMILY_OID
                | BRIN_BPCHAR_BLOOM_FAMILY_OID
                | BRIN_TIME_BLOOM_FAMILY_OID
                | BRIN_DATETIME_BLOOM_FAMILY_OID
                | BRIN_INTERVAL_BLOOM_FAMILY_OID
                | BRIN_TIMETZ_BLOOM_FAMILY_OID
                | BRIN_NUMERIC_BLOOM_FAMILY_OID
                | BRIN_UUID_BLOOM_FAMILY_OID
                | BRIN_PG_LSN_BLOOM_FAMILY_OID
        )
    )
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
        ambuild: None,
        ambuildempty: None,
        aminsert: None,
        ambeginscan: None,
        amrescan: None,
        amgettuple: None,
        amgetbitmap: None,
        amendscan: None,
        ambulkdelete: None,
        amvacuumcleanup: None,
    }
}

pub fn brinbuild(
    ctx: &IndexBuildContext,
    heap: &dyn AccessHeapServices,
    index: &mut dyn AccessIndexServices,
    scalar: &dyn AccessScalarServices,
    _wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, CatalogError> {
    brinbuildempty(
        &IndexBuildEmptyContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            xid: ctx.snapshot.current_xid,
            index_relation: ctx.index_relation,
            index_desc: ctx.index_desc.clone(),
            index_meta: ctx.index_meta.clone(),
        },
        _wal,
    )?;
    let heap_blocks = relation_nblocks(&ctx.pool, ctx.heap_relation)?;
    if heap_blocks == 0 {
        return Ok(IndexBuildResult::default());
    }
    let pages_per_range = read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?;
    let desc = brin_build_desc_with_meta(&ctx.index_desc, Some(&ctx.index_meta));
    let mut summaries = (0..heap_blocks)
        .step_by(pages_per_range as usize)
        .map(|range_start| (range_start, BrinMemTuple::new(&desc, range_start)))
        .collect::<BTreeMap<_, _>>();
    let heap_tuples = scan_visible_heap_summaries(
        heap,
        Some(index),
        scalar,
        ctx.snapshot.clone(),
        ctx.heap_toast,
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
        let tuple = brin_form_tuple(&desc, summary, scalar)?;
        upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    }
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;

    Ok(IndexBuildResult {
        heap_tuples,
        index_tuples: summaries.len() as u64,
    })
}

pub fn brinbuildempty(
    ctx: &IndexBuildEmptyContext,
    _wal: &dyn AccessWalServices,
) -> Result<(), CatalogError> {
    let pages_per_range = brin_pages_per_range_from_meta(&ctx.index_meta)?;
    let mut metapage = [0u8; BLCKSZ];
    brin_metapage_init(&mut metapage, pages_per_range, BRIN_CURRENT_VERSION)
        .map_err(|err| CatalogError::Io(format!("brin metapage init failed: {err:?}")))?;
    ctx.pool
        .with_storage_mut(|storage| {
            storage
                .smgr
                .truncate(ctx.index_relation, ForkNumber::Main, 0)?;
            Ok::<(), pgrust_storage::smgr::SmgrError>(())
        })
        .map_err(|err| CatalogError::Io(format!("brin buildempty truncate failed: {err:?}")))?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &[metapage])
}

pub fn brininsert(
    ctx: &IndexInsertContext,
    scalar: &dyn AccessScalarServices,
    _wal: &dyn AccessWalServices,
) -> Result<bool, CatalogError> {
    let range_start = normalize_range_start(
        read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
            .map(|meta| meta.pages_per_range)
            .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?,
        ctx.heap_tid.block_number,
    );
    let desc = brin_build_desc_with_meta(&ctx.index_desc, Some(&ctx.index_meta));
    let mut index_pages = read_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation)?;
    let mut revmap = brin_revmap_initialize(&index_pages)?;
    let Some((_location, bytes)) = brin_revmap_get_tuple_bytes(&index_pages, &revmap, range_start)?
    else {
        if range_start == 0 || ctx.old_heap_tid.is_some() {
            let mut summary = BrinMemTuple::new(&desc, range_start);
            add_values_to_summary(
                &ctx.index_meta,
                &ctx.index_desc,
                &desc,
                &mut summary,
                &ctx.values,
                scalar,
            )?;
            let tuple = brin_form_tuple(&desc, &summary, scalar)?;
            upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
            write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
        }
        return Ok(false);
    };
    let mut summary = brin_deform_tuple(&desc, bytes, scalar)?;
    if summary.placeholder {
        return Ok(false);
    }
    if !add_values_to_summary(
        &ctx.index_meta,
        &ctx.index_desc,
        &desc,
        &mut summary,
        &ctx.values,
        scalar,
    )? {
        return Ok(false);
    }
    let tuple = brin_form_tuple(&desc, &summary, scalar)?;
    upsert_summary_tuple(&mut index_pages, &mut revmap, range_start, &tuple.bytes)?;
    write_index_pages(&ctx.pool, ctx.client_id, ctx.index_relation, &index_pages)?;
    Ok(false)
}

pub fn brinbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
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

pub fn brinrescan(
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

pub fn bringetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
    scalar: &dyn AccessScalarServices,
) -> Result<i64, CatalogError> {
    let heap_relation = scan.heap_relation.ok_or(CatalogError::Corrupt(
        "BRIN bitmap scan missing heap relation",
    ))?;
    let heap_blocks = relation_nblocks(&scan.pool, heap_relation)?;
    let index_pages = read_index_pages(&scan.pool, scan.client_id, scan.index_relation)?;
    let revmap = brin_revmap_initialize(&index_pages)?;
    let desc = brin_build_desc_with_meta(&scan.index_desc, Some(&scan.index_meta));
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
                let tuple = brin_deform_tuple(&desc, bytes, scalar)?;
                tuple.placeholder || range_matches_scan(scan, &desc, &tuple, scalar)?
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

pub fn brinendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}

pub fn brinbulkdelete(
    _ctx: &IndexVacuumContext,
    _callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

pub fn brinvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
    heap: &dyn AccessHeapServices,
    index: Option<&mut dyn AccessIndexServices>,
    scalar: &dyn AccessScalarServices,
    _wal: &dyn AccessWalServices,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let pages_per_range = read_brin_metapage(&ctx.pool, ctx.client_id, ctx.index_relation)
        .map(|meta| meta.pages_per_range)
        .or_else(|_| brin_pages_per_range_from_meta(&ctx.index_meta))?;
    let (index_pages, _visible) =
        summarize_unsummarized_ranges(ctx, pages_per_range, heap, index, scalar)?;
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
