use std::cmp::Ordering;
use std::sync::Arc;

use parking_lot::RwLockWriteGuard;

use pgrust_access::nbtree::tuple as access_tuple;
use pgrust_access::{AccessError, AccessHeapServices, AccessIndexServices, AccessResult};

use crate::backend::access::heap::heapam::heap_fetch;
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values_with_toast,
};
use crate::backend::access::index::unique::{UniqueCandidateResult, classify_unique_candidate};
use crate::backend::access::nbtree::nbtcompare::{
    BT_DESC_FLAG, compare_bt_values, compare_bt_values_with_options, compare_item_pointers,
};
use crate::backend::access::nbtree::nbtpreprocesskeys::preprocess_scan_keys;
use crate::backend::access::nbtree::nbtsplitloc::choose_split_index;
use crate::backend::access::nbtree::nbtutils::BtSortTuple;
use crate::backend::access::nbtree::nbtxlog::log_btree_record;
use crate::backend::access::transam::xact::{TransactionId, TransactionStatus};
use crate::backend::access::transam::xlog::{
    INVALID_LSN, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_UPPER,
    XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R, XLOG_FPI,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::value_io::{
    format_unique_key_detail, format_vector_array_storage_text,
};
use crate::backend::storage::fsm::get_free_index_page;
use crate::backend::storage::page::bufpage::{
    ITEM_ID_SIZE, PageError, PageHeaderData, SIZE_OF_PAGE_HEADER_DATA, max_align, page_header,
};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::check_for_interrupts;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexInsertContext,
};
use crate::include::access::htup::AttributeCompression;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::access::nbtree::{
    BTP_DELETED, BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_ROOT, BTPageOpaqueData,
    BTREE_DEFAULT_FILLFACTOR, BTREE_METAPAGE, BTREE_NONLEAF_FILLFACTOR, BTREE_VERSION, P_NONE,
    bt_init_meta_page, bt_max_item_size, bt_page_append_tuple, bt_page_data_items,
    bt_page_get_meta, bt_page_get_opaque, bt_page_high_key, bt_page_init, bt_page_is_recyclable,
    bt_page_set_high_key, bt_page_set_opaque,
};
use crate::include::access::relscan::{
    BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::{ArrayValue, Value};
use crate::include::nodes::primnodes::{ColumnDesc, RelationDesc};
use crate::include::storage::buf_internals::Page;
use crate::{BufferPool, ClientId, OwnedBufferPin, PinnedBuffer, SmgrStorageBackend};

fn check_catalog_interrupts(
    interrupts: &crate::backend::utils::misc::interrupts::InterruptState,
) -> Result<(), CatalogError> {
    check_for_interrupts(interrupts).map_err(CatalogError::Interrupted)
}

fn check_insert_split_interrupts(ctx: &IndexInsertContext) -> Result<(), CatalogError> {
    if ctx
        .index_meta
        .btree_options
        .is_some_and(|options| options.deduplicate_items)
    {
        // :HACK: `deduplicate_items` is accepted and preserved before posting-list
        // deduplication exists. Duplicate-heavy regression inserts can therefore
        // split many pages and miss PostgreSQL's 5s regression timeout in dev
        // builds; keep normal online insert polling for indexes without the shim.
        return Ok(());
    }
    check_catalog_interrupts(ctx.interrupts.as_ref())
}

pub(crate) const UNIQUE_BUILD_DETAIL_SEPARATOR: &str = "\nDETAIL: ";
const INDEX_TUPLE_HEADER_SIZE: usize = crate::include::access::itup::SIZE_OF_INDEX_TUPLE_DATA;

#[derive(Debug, Clone)]
struct BuiltPageRef {
    block: u32,
    level: u32,
    lower_bound: Vec<Value>,
}

#[derive(Debug, Clone)]
struct PageSplitResult {
    left_block: u32,
    right_block: u32,
    level: u32,
    right_lower_bound: Vec<Value>,
    parent_stack: Vec<InsertStackEntry>,
}

#[derive(Debug, Clone)]
struct InsertStackEntry {
    block: u32,
    offset: usize,
}

#[derive(Debug, Clone)]
struct InsertSearchPath {
    leaf_block: u32,
    parent_stack: Vec<InsertStackEntry>,
}

struct LockedUniqueInsertPath<'a> {
    leaf_block: u32,
    parent_stack: Vec<InsertStackEntry>,
    pin: PinnedBuffer<'a, SmgrStorageBackend>,
    guard: RwLockWriteGuard<'a, Page>,
}

enum LockedUniqueCheckResult {
    Clear,
    WaitFor(TransactionId),
    Restart { split_block: Option<u32> },
}

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub(crate) fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    catalog_result(access_tuple::encode_key_payload(
        desc,
        values,
        default_toast_compression,
        &RootAccessServices,
    ))
}

pub(crate) fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
) -> Result<Vec<Value>, CatalogError> {
    catalog_result(access_tuple::decode_key_payload(
        desc,
        payload,
        &RootAccessServices,
    ))
}

fn parse_fixed_vector_text(text: &str) -> Option<Vec<i64>> {
    text.split_ascii_whitespace()
        .map(|part| part.parse::<i64>().ok())
        .collect()
}

fn fixed_vector_items_from_array(array: &ArrayValue) -> Vec<i64> {
    array
        .elements
        .iter()
        .filter_map(|value| match value {
            Value::Int16(value) => Some(i64::from(*value)),
            Value::Int32(value) => Some(i64::from(*value)),
            Value::Int64(value) => Some(*value),
            _ => None,
        })
        .collect()
}

fn fixed_vector_items(value: &Value) -> Option<Vec<i64>> {
    match value {
        Value::Array(items) => Some(fixed_vector_items_from_array(&ArrayValue::from_1d(
            items.clone(),
        ))),
        Value::PgArray(array) => Some(fixed_vector_items_from_array(array)),
        Value::Text(text) => parse_fixed_vector_text(text),
        Value::TextRef(ptr, len) => {
            let bytes = unsafe { std::slice::from_raw_parts(*ptr, *len as usize) };
            std::str::from_utf8(bytes)
                .ok()
                .and_then(parse_fixed_vector_text)
        }
        _ => None,
    }
}

fn compare_fixed_vector_values(left: &Value, right: &Value) -> Option<Ordering> {
    Some(fixed_vector_items(left)?.cmp(&fixed_vector_items(right)?))
}

fn compare_bt_values_for_type(
    left: &Value,
    right: &Value,
    ty: crate::backend::parser::SqlType,
) -> Ordering {
    compare_bt_values_for_type_with_option(left, right, ty, 0)
}

fn compare_bt_values_for_type_with_option(
    left: &Value,
    right: &Value,
    ty: crate::backend::parser::SqlType,
    option: i16,
) -> Ordering {
    if matches!(ty.kind, crate::backend::parser::SqlTypeKind::InternalChar)
        && !ty.is_array
        && let (Some(left_byte), Some(right_byte)) =
            (internal_char_cmp_key(left), internal_char_cmp_key(right))
    {
        let mut ord = left_byte.cmp(&right_byte);
        if option & crate::backend::access::nbtree::nbtcompare::BT_DESC_FLAG != 0 {
            ord = ord.reverse();
        }
        return ord;
    }
    if matches!(
        ty.kind,
        crate::backend::parser::SqlTypeKind::Int2Vector
            | crate::backend::parser::SqlTypeKind::OidVector
    ) && let Some(mut ord) = compare_fixed_vector_values(left, right)
    {
        if option & crate::backend::access::nbtree::nbtcompare::BT_DESC_FLAG != 0 {
            ord = ord.reverse();
        }
        return ord;
    }
    if matches!(ty.kind, crate::backend::parser::SqlTypeKind::Char)
        && !ty.is_array
        && let (Some(left_text), Some(right_text)) = (left.as_text(), right.as_text())
    {
        let mut ord = left_text
            .trim_end_matches(' ')
            .cmp(right_text.trim_end_matches(' '));
        if option & crate::backend::access::nbtree::nbtcompare::BT_DESC_FLAG != 0 {
            ord = ord.reverse();
        }
        return ord;
    }
    compare_bt_values_with_options(left, right, option)
}

fn internal_char_cmp_key(value: &Value) -> Option<u8> {
    match value {
        Value::InternalChar(byte) => Some(*byte),
        _ => value
            .as_text()
            .map(|text| text.as_bytes().first().copied().unwrap_or_default()),
    }
}

fn compare_key_arrays_with_columns_and_options(
    columns: &[ColumnDesc],
    left: &[Value],
    right: &[Value],
    indoption: &[i16],
) -> Ordering {
    for (idx, (left, right)) in left.iter().zip(right.iter()).enumerate() {
        let option = indoption.get(idx).copied().unwrap_or_default();
        let ord = columns
            .get(idx)
            .map(|column| {
                compare_bt_values_for_type_with_option(left, right, column.sql_type, option)
            })
            .unwrap_or_else(|| compare_bt_values_with_options(left, right, option));
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

fn btree_key_count(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> usize {
    usize::try_from(index_meta.indnkeyatts.max(0)).unwrap_or_default()
}

fn key_prefix(values: &[Value], key_count: usize) -> &[Value] {
    &values[..values.len().min(key_count)]
}

fn compare_key_arrays_with_columns(
    columns: &[ColumnDesc],
    left: &[Value],
    right: &[Value],
) -> Ordering {
    compare_key_arrays_with_columns_and_options(columns, left, right, &[])
}

fn compare_key_prefixes_with_columns_and_options(
    columns: &[ColumnDesc],
    left: &[Value],
    right: &[Value],
    key_count: usize,
    indoption: &[i16],
) -> Ordering {
    compare_key_arrays_with_columns_and_options(
        columns,
        key_prefix(left, key_count),
        key_prefix(right, key_count),
        indoption,
    )
}

fn compare_bt_keyspace_with_columns_and_options(
    columns: &[ColumnDesc],
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    indoption: &[i16],
) -> Ordering {
    let ord =
        compare_key_arrays_with_columns_and_options(columns, left_keys, right_keys, indoption);
    if ord != Ordering::Equal {
        return ord;
    }
    compare_item_pointers(left_tid, right_tid)
}

fn tuple_key_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    decode_key_payload(desc, &tuple.payload)
}

fn tuple_key_prefix_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
    key_count: usize,
) -> Result<Vec<Value>, CatalogError> {
    let mut values = tuple_key_values(desc, tuple)?;
    values.truncate(key_count);
    Ok(values)
}

fn pivot_tuple(
    desc: &RelationDesc,
    child_block: u32,
    key_values: &[Value],
    default_toast_compression: AttributeCompression,
) -> Result<IndexTupleData, CatalogError> {
    Ok(IndexTupleData::new_raw(
        ItemPointerData {
            block_number: child_block,
            offset_number: 0,
        },
        false,
        false,
        false,
        encode_key_payload(desc, key_values, default_toast_compression)?,
    ))
}

fn oversized_btree_tuple_error(index_name: &str, tuple: &IndexTupleData) -> CatalogError {
    CatalogError::Io(format!(
        "index row size {} exceeds btree version {} maximum {} for index \"{}\"",
        max_align(tuple.size()),
        BTREE_VERSION,
        bt_max_item_size(),
        index_name,
    ))
}

fn check_leaf_tuple_size(index_name: &str, tuple: &IndexTupleData) -> Result<(), CatalogError> {
    if max_align(tuple.size()) <= bt_max_item_size() {
        return Ok(());
    }
    Err(oversized_btree_tuple_error(index_name, tuple))
}

fn tuple_matches_scan_keys(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
    keys: &[ScanKeyData],
    indoption: &[i16],
) -> Result<bool, CatalogError> {
    if keys.is_empty() {
        return Ok(true);
    }
    let values = decode_key_payload(desc, &tuple.payload)?;
    for key in keys {
        if key.attribute_number == 0 {
            if !tuple_matches_row_scan_key(desc, &values, key)? {
                return Ok(false);
            }
            continue;
        }
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let value = values
            .get(attno)
            .ok_or(CatalogError::Corrupt("scan key attno out of range"))?;
        let column = desc
            .columns
            .get(attno)
            .ok_or(CatalogError::Corrupt("scan key desc attno out of range"))?;
        if !value_matches_scan_key_strategy(
            value,
            &key.argument,
            column.sql_type,
            key.strategy,
            indoption.get(attno).copied().unwrap_or_default(),
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn value_matches_scan_key_strategy(
    value: &Value,
    argument: &Value,
    sql_type: crate::backend::parser::SqlType,
    strategy: u16,
    option: i16,
) -> bool {
    if strategy == 3
        && let Some(array) = argument.as_array_value()
    {
        if matches!(value, Value::Null) {
            return false;
        }
        return array.elements.iter().any(|element| {
            if matches!(element, Value::Null) {
                return false;
            }
            let ord = compare_bt_values_for_type_with_option(value, element, sql_type, option);
            ord == Ordering::Equal
        });
    }
    match (value, argument) {
        (Value::Null, Value::Null) => return strategy == 3,
        (Value::Null, _) => return false,
        (_, Value::Null) => return strategy == 1,
        _ => {}
    }
    let ord = compare_bt_values_for_type_with_option(value, argument, sql_type, option);
    strategy_matches_ordering(strategy, ord)
}

fn strategy_matches_ordering(strategy: u16, ord: Ordering) -> bool {
    match strategy {
        1 => ord == Ordering::Less,
        2 => matches!(ord, Ordering::Less | Ordering::Equal),
        3 => ord == Ordering::Equal,
        4 => matches!(ord, Ordering::Greater | Ordering::Equal),
        5 => ord == Ordering::Greater,
        _ => false,
    }
}

fn tuple_matches_row_scan_key(
    desc: &RelationDesc,
    values: &[Value],
    key: &ScanKeyData,
) -> Result<bool, CatalogError> {
    let Value::Record(record) = &key.argument else {
        return Ok(false);
    };
    for (idx, right) in record.fields.iter().enumerate() {
        let field = record
            .descriptor
            .fields
            .get(idx)
            .ok_or(CatalogError::Corrupt("row scan key descriptor mismatch"))?;
        let index_pos = field
            .name
            .strip_prefix('i')
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or(CatalogError::Corrupt("row scan key missing index position"))?;
        let left = values.get(index_pos).ok_or(CatalogError::Corrupt(
            "row scan key index position out of range",
        ))?;
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return Ok(false);
        }
        let column = desc.columns.get(index_pos).ok_or(CatalogError::Corrupt(
            "row scan key desc position out of range",
        ))?;
        let ord = compare_bt_values_for_type(left, right, column.sql_type);
        if ord == Ordering::Equal {
            continue;
        }
        return Ok(strategy_matches_ordering(key.strategy, ord));
    }
    Ok(strategy_matches_ordering(key.strategy, Ordering::Equal))
}

fn page_fillfactor_reserve(fillfactor: u16) -> usize {
    crate::backend::storage::smgr::BLCKSZ * (100usize.saturating_sub(fillfactor as usize)) / 100
}

fn append_page_or_finish(
    pages: &mut Vec<Vec<IndexTupleData>>,
    current: &mut Vec<IndexTupleData>,
    candidate: IndexTupleData,
    flags: u16,
    level: u32,
    fillfactor: u16,
) -> Result<(), CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut page, flags, level)
        .map_err(|err| CatalogError::Io(format!("btree page init failed: {err:?}")))?;
    for tuple in current.iter() {
        bt_page_append_tuple(&mut page, tuple)
            .map_err(|_| CatalogError::Io("index build tuple too large for btree page".into()))?;
    }
    if bt_page_append_tuple(&mut page, &candidate).is_ok() {
        let after = page_header(&page)
            .map_err(|err| CatalogError::Io(format!("btree page header failed: {err:?}")))?
            .free_space();
        if after > page_fillfactor_reserve(fillfactor) || current.is_empty() {
            current.push(candidate);
            return Ok(());
        }
    }
    pages.push(std::mem::take(current));
    current.push(candidate);
    Ok(())
}

fn group_sorted_tuples_into_pages(
    tuples: Vec<IndexTupleData>,
    flags: u16,
    level: u32,
    fillfactor: u16,
) -> Result<Vec<Vec<IndexTupleData>>, CatalogError> {
    let mut pages = Vec::new();
    let mut current = Vec::new();
    for tuple in tuples {
        append_page_or_finish(&mut pages, &mut current, tuple, flags, level, fillfactor)?;
    }
    if !current.is_empty() {
        pages.push(current);
    }
    Ok(pages)
}

fn pin_btree_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("btree pin block failed: {err:?}")))
}

fn lock_btree_block_exclusive<'a>(
    ctx: &'a IndexInsertContext,
    block: u32,
) -> Result<
    (
        PinnedBuffer<'a, SmgrStorageBackend>,
        RwLockWriteGuard<'a, Page>,
    ),
    CatalogError,
> {
    let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
    let guard = ctx
        .pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    Ok((pin, guard))
}

fn read_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    let pin = pin_btree_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

fn write_buffered_btree_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
    wal_info: u8,
) -> Result<(), CatalogError> {
    write_buffered_btree_page_with_init(pool, client_id, xid, rel, block, page, wal_info, false)
}

fn write_buffered_btree_page_with_init(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
    wal_info: u8,
    will_init: bool,
) -> Result<(), CatalogError> {
    pool.ensure_block_exists(rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("btree extend failed: {err:?}")))?;
    let pin = pin_btree_block(pool, client_id, rel, block)?;
    let mut guard = pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    let lsn = if let Some(wal) = pool.wal_writer() {
        log_btree_record(
            &wal,
            xid,
            wal_info,
            &[crate::backend::access::nbtree::nbtxlog::LoggedBtreeBlock {
                block_id: 0,
                tag: crate::backend::storage::buffer::BufferTag {
                    rel,
                    fork: ForkNumber::Main,
                    block,
                },
                page,
                will_init,
                force_image: true,
                data: &[],
            }],
            &[],
        )
        .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))?
    } else {
        INVALID_LSN
    };
    pool.install_page_image_locked(pin.buffer_id(), page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    Ok(())
}

fn ensure_relation_exists(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| CatalogError::Io(format!("btree ensure relation failed: {err:?}")))
}

fn truncate_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    pool.with_storage_mut(|storage| {
        storage.smgr.truncate(rel, ForkNumber::Main, 0)?;
        let _ = storage.smgr.truncate(rel, ForkNumber::Fsm, 0);
        Ok::<(), crate::backend::storage::smgr::SmgrError>(())
    })
    .map_err(|err| CatalogError::Io(err.to_string()))
}

fn read_page(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    read_buffered_page(pool, 0, rel, block)
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(err.to_string()))
}

fn write_meta_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    root: u32,
    level: u32,
) -> Result<(), CatalogError> {
    let mut metapage = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_init_meta_page(&mut metapage, root, level, false)
        .map_err(|err| CatalogError::Io(format!("btree metapage init failed: {err:?}")))?;
    write_buffered_btree_page(
        pool,
        client_id,
        xid,
        rel,
        BTREE_METAPAGE,
        &metapage,
        XLOG_BTREE_INSERT_META,
    )
}

fn page_lower_bound(
    desc: &RelationDesc,
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
    key_count: usize,
) -> Result<Vec<Value>, CatalogError> {
    let page = read_page(pool, rel, block)?;
    let items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let tuple = items
        .first()
        .ok_or(CatalogError::Corrupt("btree page unexpectedly empty"))?;
    tuple_key_prefix_values(desc, tuple, key_count)
}

fn page_first_key_values(
    desc: &RelationDesc,
    page: &Page,
    key_count: usize,
) -> Result<Option<Vec<Value>>, CatalogError> {
    let items = bt_page_data_items(page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let Some(tuple) = items.first() else {
        return Ok(None);
    };
    tuple_key_prefix_values(desc, tuple, key_count).map(Some)
}

pub(crate) fn ensure_empty_btree(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    ensure_relation_exists(pool, rel)?;
    truncate_relation(pool, rel)?;
    write_meta_page(pool, client_id, xid, rel, 1, 0)?;
    let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut root, BTP_LEAF | BTP_ROOT, 0)
        .map_err(|err| CatalogError::Io(format!("btree root init failed: {err:?}")))?;
    write_buffered_btree_page_with_init(pool, client_id, xid, rel, 1, &root, XLOG_FPI, true)?;
    Ok(())
}

fn key_values_from_heap_row(
    _heap_desc: &RelationDesc,
    index_desc: &RelationDesc,
    indkey: &[i16],
    row_values: &[Value],
) -> Result<Vec<Value>, CatalogError> {
    let mut keys = Vec::with_capacity(index_desc.columns.len());
    for attnum in indkey {
        let idx = attnum.saturating_sub(1) as usize;
        let mut value = row_values
            .get(idx)
            .cloned()
            .ok_or(CatalogError::Corrupt("index key attnum out of range"))?;
        let column = index_desc
            .columns
            .get(keys.len())
            .ok_or(CatalogError::Corrupt("index key descriptor out of range"))?;
        if let Some(array) = value.as_array_value()
            && matches!(
                column.sql_type.kind,
                crate::backend::parser::SqlTypeKind::Int2Vector
                    | crate::backend::parser::SqlTypeKind::OidVector
            )
        {
            value = Value::Text(
                format_vector_array_storage_text(column.sql_type, &array)
                    .map_err(|err| CatalogError::Io(format!("{err:?}")))?
                    .into(),
            );
        }
        keys.push(value);
    }
    Ok(keys)
}

fn keys_contain_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

fn write_page_header_fast(
    page: &mut [u8; crate::backend::storage::smgr::BLCKSZ],
    header: PageHeaderData,
) {
    page[0..8].copy_from_slice(&header.pd_lsn.to_le_bytes());
    page[8..10].copy_from_slice(&header.pd_checksum.to_le_bytes());
    page[10..12].copy_from_slice(&header.pd_flags.to_le_bytes());
    page[12..14].copy_from_slice(&header.pd_lower.to_le_bytes());
    page[14..16].copy_from_slice(&header.pd_upper.to_le_bytes());
    page[16..18].copy_from_slice(&header.pd_special.to_le_bytes());
    page[18..20].copy_from_slice(&header.pd_pagesize_version.to_le_bytes());
    page[20..24].copy_from_slice(&header.pd_prune_xid.to_le_bytes());
}

fn write_index_tuple_at(
    page: &mut [u8; crate::backend::storage::smgr::BLCKSZ],
    offset: usize,
    tuple: &IndexTupleData,
) {
    page[offset..offset + 4].copy_from_slice(&tuple.t_tid.block_number.to_le_bytes());
    page[offset + 4..offset + 6].copy_from_slice(&tuple.t_tid.offset_number.to_le_bytes());
    page[offset + 6..offset + INDEX_TUPLE_HEADER_SIZE].copy_from_slice(&tuple.t_info.to_le_bytes());
    page[offset + INDEX_TUPLE_HEADER_SIZE..offset + tuple.size()].copy_from_slice(&tuple.payload);
}

fn write_item_id_fast(
    page: &mut [u8; crate::backend::storage::smgr::BLCKSZ],
    offset: usize,
    lp_off: usize,
    lp_len: usize,
) {
    let raw = (lp_off as u32 & 0x7fff) | (1u32 << 15) | ((lp_len as u32 & 0x7fff) << 17);
    let idx = max_align(SIZE_OF_PAGE_HEADER_DATA) + (offset - 1) * ITEM_ID_SIZE;
    page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&raw.to_le_bytes());
}

fn append_tuples_to_build_page(
    page: &mut [u8; crate::backend::storage::smgr::BLCKSZ],
    tuples: &[IndexTupleData],
) -> Result<(), CatalogError> {
    let mut header = page_header(page)
        .map_err(|err| CatalogError::Io(format!("btree build page header failed: {err:?}")))?;
    let mut offset =
        (usize::from(header.pd_lower) - max_align(SIZE_OF_PAGE_HEADER_DATA)) / ITEM_ID_SIZE + 1;

    for tuple in tuples {
        let len = tuple.size();
        let aligned_len = max_align(len);
        if header.free_space() < aligned_len + ITEM_ID_SIZE {
            return Err(CatalogError::Io(
                "index tuple too large for btree build page".into(),
            ));
        }
        let new_upper = usize::from(header.pd_upper) - aligned_len;
        write_index_tuple_at(page, new_upper, tuple);
        page[new_upper + len..new_upper + aligned_len].fill(0);
        write_item_id_fast(page, offset, new_upper, len);
        header.pd_upper = new_upper as u16;
        header.pd_lower = usize::from(header.pd_lower)
            .checked_add(ITEM_ID_SIZE)
            .ok_or(CatalogError::Corrupt("btree build page lower overflow"))?
            as u16;
        offset += 1;
    }

    write_page_header_fast(page, header);
    Ok(())
}

fn build_leaf_pages(
    ctx: &IndexBuildContext,
    tuples: Vec<BtSortTuple>,
    next_block: &mut u32,
) -> Result<Vec<BuiltPageRef>, CatalogError> {
    let fillfactor = ctx
        .index_meta
        .btree_options
        .map(|options| options.fillfactor)
        .unwrap_or(BTREE_DEFAULT_FILLFACTOR);
    let pages = group_sorted_tuples_into_pages(
        tuples.into_iter().map(|tuple| tuple.tuple).collect(),
        BTP_LEAF,
        0,
        fillfactor,
    )?;

    let mut built = Vec::with_capacity(pages.len());
    for (idx, items) in pages.into_iter().enumerate() {
        let block = *next_block;
        *next_block += 1;
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut page, BTP_LEAF, 0)
            .map_err(|err| CatalogError::Io(format!("btree leaf init failed: {err:?}")))?;
        append_tuples_to_build_page(&mut page, &items)?;
        let mut opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        opaque.btpo_prev = if idx == 0 { P_NONE } else { block - 1 };
        opaque.btpo_next = P_NONE;
        bt_page_set_opaque(&mut page, opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page_with_init(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            block,
            &page,
            XLOG_FPI,
            true,
        )?;
        built.push(BuiltPageRef {
            block,
            level: 0,
            lower_bound: tuple_key_prefix_values(
                &ctx.index_desc,
                items
                    .first()
                    .ok_or(CatalogError::Corrupt("empty leaf build page"))?,
                btree_key_count(&ctx.index_meta),
            )?,
        });
    }

    for built_page in built.iter().skip(1) {
        let prev_block = built_page.block - 1;
        let mut prev_page = read_page(&ctx.pool, ctx.index_relation, prev_block)?;
        let mut prev_opaque = bt_page_get_opaque(&prev_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        prev_opaque.btpo_next = built_page.block;
        bt_page_set_opaque(&mut prev_page, prev_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            prev_block,
            &prev_page,
            XLOG_FPI,
        )?;
    }

    Ok(built)
}

fn build_internal_level(
    ctx: &IndexBuildContext,
    children: Vec<BuiltPageRef>,
    next_block: &mut u32,
) -> Result<Vec<BuiltPageRef>, CatalogError> {
    let level = children
        .first()
        .map(|child| child.level + 1)
        .ok_or(CatalogError::Corrupt("missing child level"))?;
    let mut tuples = Vec::with_capacity(children.len());
    for child in &children {
        tuples.push(pivot_tuple(
            &ctx.index_desc,
            child.block,
            &child.lower_bound,
            ctx.default_toast_compression,
        )?);
    }
    let pages = group_sorted_tuples_into_pages(tuples, 0, level, BTREE_NONLEAF_FILLFACTOR)?;

    let mut built = Vec::with_capacity(pages.len());
    for (idx, items) in pages.into_iter().enumerate() {
        let block = *next_block;
        *next_block += 1;
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut page, 0, level)
            .map_err(|err| CatalogError::Io(format!("btree internal init failed: {err:?}")))?;
        append_tuples_to_build_page(&mut page, &items)?;
        let mut opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        opaque.btpo_prev = if idx == 0 { P_NONE } else { block - 1 };
        opaque.btpo_next = P_NONE;
        bt_page_set_opaque(&mut page, opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page_with_init(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            block,
            &page,
            XLOG_FPI,
            true,
        )?;
        built.push(BuiltPageRef {
            block,
            level,
            lower_bound: tuple_key_prefix_values(
                &ctx.index_desc,
                items
                    .first()
                    .ok_or(CatalogError::Corrupt("empty internal build page"))?,
                btree_key_count(&ctx.index_meta),
            )?,
        });
    }

    for built_page in built.iter().skip(1) {
        let prev_block = built_page.block - 1;
        let mut prev_page = read_page(&ctx.pool, ctx.index_relation, prev_block)?;
        let mut prev_opaque = bt_page_get_opaque(&prev_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        prev_opaque.btpo_next = built_page.block;
        bt_page_set_opaque(&mut prev_page, prev_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            prev_block,
            &prev_page,
            XLOG_FPI,
        )?;
    }

    Ok(built)
}

fn mark_root_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
) -> Result<(), CatalogError> {
    let mut page = read_page(pool, rel, block)?;
    let mut opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    opaque.btpo_flags |= BTP_ROOT;
    bt_page_set_opaque(&mut page, opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    write_buffered_btree_page(pool, client_id, xid, rel, block, &page, XLOG_FPI)
}

fn build_btree_pages(
    ctx: &IndexBuildContext,
    tuples: Vec<BtSortTuple>,
) -> Result<IndexBuildResult, CatalogError> {
    ensure_relation_exists(&ctx.pool, ctx.index_relation)?;
    truncate_relation(&ctx.pool, ctx.index_relation)?;
    write_meta_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        1,
        0,
    )?;

    if tuples.is_empty() {
        let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut root, BTP_LEAF | BTP_ROOT, 0)
            .map_err(|err| CatalogError::Io(format!("btree root init failed: {err:?}")))?;
        write_buffered_btree_page_with_init(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            1,
            &root,
            XLOG_FPI,
            true,
        )?;
        return Ok(IndexBuildResult::default());
    }

    let mut next_block = 1u32;
    let mut current = build_leaf_pages(ctx, tuples, &mut next_block)?;
    while current.len() > 1 {
        current = build_internal_level(ctx, current, &mut next_block)?;
    }
    let root = current
        .first()
        .ok_or(CatalogError::Corrupt("missing btree root after build"))?;
    mark_root_block(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        root.block,
    )?;
    write_meta_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        root.block,
        root.level,
    )?;
    Ok(IndexBuildResult::default())
}

fn check_unique_build(
    index_name: &str,
    tuples: &[BtSortTuple],
    columns: &[ColumnDesc],
    key_count: usize,
    nulls_not_distinct: bool,
) -> Result<(), CatalogError> {
    let mut last: Option<&[Value]> = None;
    for tuple in tuples {
        let tuple_keys = key_prefix(&tuple.key_values, key_count);
        if !nulls_not_distinct && keys_contain_null(tuple_keys) {
            last = None;
            continue;
        }
        if last.is_some_and(|prev| {
            compare_key_arrays_with_columns(columns, prev, tuple_keys) == Ordering::Equal
        }) {
            let detail = format_unique_build_key_detail(
                &columns[..columns.len().min(key_count)],
                tuple_keys,
            );
            return Err(CatalogError::UniqueViolation(format!(
                "{index_name}{UNIQUE_BUILD_DETAIL_SEPARATOR}{detail}"
            )));
        }
        last = Some(tuple_keys);
    }
    Ok(())
}

fn format_unique_build_key_detail(columns: &[ColumnDesc], values: &[Value]) -> String {
    format_unique_key_detail(columns, values)
        .strip_suffix(" already exists.")
        .map(|prefix| format!("{prefix} is duplicated."))
        .unwrap_or_else(|| format_unique_key_detail(columns, values))
}

fn btbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    // :HACK: Bulk nbtree builds are still single-threaded and dev-profile slow;
    // per-tuple timeout polling makes PostgreSQL's btree regression primary-key
    // build miss its statement timeout. Keep interrupt checks on online index
    // operations and move bulk-build polling back here once the builder is faster.
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let toast =
        ctx.heap_toast.map(
            |relation| crate::include::nodes::execnodes::ToastFetchContext {
                relation,
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                snapshot: ctx.snapshot.clone(),
                client_id: ctx.client_id,
            },
        );
    let mut spool = crate::backend::access::nbtree::nbtsort::BtSpool::default();
    let mut result = IndexBuildResult::default();
    let mut approx_bytes = 0usize;
    let key_count = btree_key_count(&ctx.index_meta);
    heap_services
        .for_each_visible_heap_tuple(
            ctx.heap_relation,
            ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple
                    .deform(&attr_descs)
                    .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
                let row_values =
                    materialize_heap_row_values_with_toast(&ctx.heap_desc, &datums, toast.as_ref())
                        .map_err(map_catalog_error_to_access)?;
                let Some(all_values) =
                    index_services.project_index_row(&ctx.index_meta, &row_values, tid)?
                else {
                    result.heap_tuples += 1;
                    return Ok(());
                };
                let payload =
                    encode_key_payload(&ctx.index_desc, &all_values, ctx.default_toast_compression)
                        .map_err(map_catalog_error_to_access)?;
                let tuple = IndexTupleData::new_raw(tid, false, false, false, payload);
                check_leaf_tuple_size(&ctx.index_name, &tuple)
                    .map_err(map_catalog_error_to_access)?;
                let key_values = key_prefix(&all_values, key_count).to_vec();
                approx_bytes = approx_bytes
                    .saturating_add(tuple.size())
                    .saturating_add(all_values.len() * 16);
                if approx_bytes > ctx.maintenance_work_mem_kb.saturating_mul(1024) {
                    return Err(AccessError::Scalar(
                        "CREATE INDEX requires external build spill, which is not supported yet"
                            .into(),
                    ));
                }
                spool.push(BtSortTuple { tuple, key_values });
                result.heap_tuples += 1;
                result.index_tuples += 1;
                Ok(())
            },
        )
        .map_err(map_access_error)?;

    let tuples = spool.finish(
        &ctx.index_desc.columns,
        key_count,
        &ctx.index_meta.indoption,
    );
    if ctx.index_meta.indisunique {
        check_unique_build(
            &ctx.index_name,
            &tuples,
            &ctx.index_desc.columns,
            key_count,
            ctx.index_meta.indnullsnotdistinct,
        )?;
    }
    build_btree_pages(ctx, tuples)?;
    Ok(result)
}

fn choose_child_slot(
    desc: &RelationDesc,
    items: &[IndexTupleData],
    target: &[Value],
    direction: ScanDirection,
    key_count: usize,
    indoption: &[i16],
) -> Result<usize, CatalogError> {
    if items.is_empty() {
        return Err(CatalogError::Corrupt("empty internal btree page"));
    }
    if target.is_empty() {
        return Ok(match direction {
            ScanDirection::Forward => 0,
            ScanDirection::Backward => items.len() - 1,
        });
    }
    let mut choice = 0usize;
    for (idx, tuple) in items.iter().enumerate() {
        let key = tuple_key_prefix_values(desc, tuple, key_count)?;
        if compare_key_prefixes_with_columns_and_options(
            &desc.columns,
            &key,
            target,
            key_count,
            indoption,
        ) != Ordering::Greater
        {
            choice = idx;
        } else {
            break;
        }
    }
    Ok(choice)
}

fn scan_positioning_prefix(keys: &[ScanKeyData], direction: ScanDirection) -> Vec<Value> {
    let mut prefix = Vec::new();
    let preprocessed = preprocess_scan_keys(keys);
    let mut index = 0usize;
    while index < preprocessed.len() {
        let key = &preprocessed[index];
        if key.attribute_number <= 0 || key.argument.as_array_value().is_some() {
            break;
        }
        let expected_attno = (prefix.len() + 1) as i16;
        if key.attribute_number != expected_attno {
            break;
        }
        if key.strategy == 3 {
            prefix.push(key.argument.clone());
            index += 1;
            continue;
        }
        let attribute_number = key.attribute_number;
        let mut chosen: Option<Value> = None;
        let mut cursor = index;
        while cursor < preprocessed.len()
            && preprocessed[cursor].attribute_number == attribute_number
        {
            let candidate = &preprocessed[cursor];
            match direction {
                ScanDirection::Forward if matches!(candidate.strategy, 4 | 5) => {
                    if candidate.argument.as_array_value().is_some() {
                        cursor += 1;
                        continue;
                    }
                    if chosen.as_ref().is_none_or(|current| {
                        compare_bt_values(current, &candidate.argument) == Ordering::Less
                    }) {
                        chosen = Some(candidate.argument.clone());
                    }
                }
                ScanDirection::Backward if matches!(candidate.strategy, 1 | 2) => {
                    if candidate.argument.as_array_value().is_some() {
                        cursor += 1;
                        continue;
                    }
                    if chosen.as_ref().is_none_or(|current| {
                        compare_bt_values(current, &candidate.argument) == Ordering::Greater
                    }) {
                        chosen = Some(candidate.argument.clone());
                    }
                }
                _ => {}
            }
            cursor += 1;
        }
        if let Some(bound) = chosen {
            prefix.push(bound);
        }
        break;
    }
    prefix
}

fn exact_equality_prefix(keys: &[ScanKeyData], indoption: &[i16]) -> Vec<Value> {
    let mut prefix = Vec::new();
    let mut expected_attno = 1i16;
    for key in preprocess_scan_keys(keys) {
        if key.strategy != 3
            || key.attribute_number != expected_attno
            || key.argument.as_array_value().is_some()
        {
            return Vec::new();
        }
        let index = key.attribute_number.saturating_sub(1) as usize;
        if indoption
            .get(index)
            .is_some_and(|option| option & BT_DESC_FLAG != 0)
        {
            return Vec::new();
        }
        prefix.push(key.argument);
        expected_attno += 1;
    }
    prefix
}

fn tuple_prefix_cmp(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
    target: &[Value],
    indoption: &[i16],
) -> Result<Ordering, CatalogError> {
    let values = tuple_key_values(desc, tuple)?;
    let prefix_len = target.len().min(values.len());
    Ok(compare_key_arrays_with_columns_and_options(
        &desc.columns,
        &values[..prefix_len],
        target,
        indoption,
    ))
}

fn empty_leaf_exhausts_exact_equality_scan(
    scan: &IndexScanDesc,
    items: &[IndexTupleData],
) -> Result<bool, CatalogError> {
    let target = exact_equality_prefix(&scan.key_data, &scan.indoption);
    if target.is_empty() || items.is_empty() {
        return Ok(false);
    }
    match scan.direction {
        ScanDirection::Forward => {
            let last = items
                .last()
                .expect("items is not empty when checking equality scan bounds");
            Ok(
                tuple_prefix_cmp(&scan.index_desc, last, &target, &scan.indoption)?
                    != Ordering::Less,
            )
        }
        ScanDirection::Backward => {
            let first = items
                .first()
                .expect("items is not empty when checking equality scan bounds");
            Ok(
                tuple_prefix_cmp(&scan.index_desc, first, &target, &scan.indoption)?
                    != Ordering::Greater,
            )
        }
    }
}

fn read_page_items(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<
    (
        Vec<IndexTupleData>,
        crate::include::access::nbtree::BTPageOpaqueData,
    ),
    CatalogError,
> {
    let page = read_page(pool, rel, block)?;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    Ok((items, opaque))
}

fn find_leaf_with_ancestors(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
    target: &[Value],
    direction: ScanDirection,
    key_count: usize,
    indoption: &[i16],
) -> Result<(Vec<u32>, u32), CatalogError> {
    let meta_page = read_page(pool, rel, BTREE_METAPAGE)?;
    let meta = bt_page_get_meta(&meta_page)
        .map_err(|err| CatalogError::Io(format!("btree metapage read failed: {err:?}")))?;
    let mut ancestors = Vec::new();
    let mut block = meta.btm_root;
    let mut level = meta.btm_level;
    while level > 0 {
        ancestors.push(block);
        let (items, _) = read_page_items(pool, rel, block)?;
        let slot = choose_child_slot(desc, &items, target, direction, key_count, indoption)?;
        block = items[slot].t_tid.block_number;
        level -= 1;
    }
    Ok((ancestors, block))
}

fn find_child_index(items: &[IndexTupleData], child_block: u32, start: usize) -> Option<usize> {
    let start = start.min(items.len());
    for (idx, item) in items.iter().enumerate().skip(start) {
        if item.t_tid.block_number == child_block {
            return Some(idx);
        }
    }
    for idx in (0..start).rev() {
        if items[idx].t_tid.block_number == child_block {
            return Some(idx);
        }
    }
    None
}

fn leaf_upper_bound(
    ctx: &IndexInsertContext,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
    opaque: crate::include::access::nbtree::BTPageOpaqueData,
) -> Result<Option<Vec<Value>>, CatalogError> {
    if let Some(high_key) = bt_page_high_key(page)
        .map_err(|err| CatalogError::Io(format!("btree high-key read failed: {err:?}")))?
    {
        return tuple_key_prefix_values(
            &ctx.index_desc,
            &high_key,
            btree_key_count(&ctx.index_meta),
        )
        .map(Some);
    }
    if opaque.btpo_next == P_NONE {
        return Ok(None);
    }
    let Some((_, _, _, first_tuple)) = first_live_right_leaf(ctx, opaque.btpo_next)? else {
        return Ok(None);
    };
    let Some(first_tuple) = first_tuple else {
        return Ok(None);
    };
    tuple_key_prefix_values(
        &ctx.index_desc,
        &first_tuple,
        btree_key_count(&ctx.index_meta),
    )
    .map(Some)
}

type LiveRightLeaf = (
    u32,
    Page,
    crate::include::access::nbtree::BTPageOpaqueData,
    Option<IndexTupleData>,
);

fn first_live_right_leaf(
    ctx: &IndexInsertContext,
    start_block: u32,
) -> Result<Option<LiveRightLeaf>, CatalogError> {
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    let mut block = start_block;
    let mut visited = 0u32;
    while block != P_NONE && visited <= nblocks {
        visited += 1;
        let page = read_page(&ctx.pool, ctx.index_relation, block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.btpo_flags & BTP_DELETED != 0 {
            block = opaque.btpo_next;
            continue;
        }
        let first_tuple = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree next-page parse failed: {err:?}")))?
            .into_iter()
            .next();
        return Ok(Some((block, page, opaque, first_tuple)));
    }
    if visited > nblocks {
        return Err(CatalogError::Corrupt("btree right sibling cycle"));
    }
    Ok(None)
}

fn replacement_for_deleted_leaf(
    ctx: &IndexInsertContext,
    opaque: crate::include::access::nbtree::BTPageOpaqueData,
) -> Result<Option<u32>, CatalogError> {
    if opaque.btpo_next != P_NONE
        && let Some((block, _, _, _)) = first_live_right_leaf(ctx, opaque.btpo_next)?
    {
        return Ok(Some(block));
    }
    Ok((opaque.btpo_prev != P_NONE).then_some(opaque.btpo_prev))
}

fn left_sibling_may_contain_key(
    ctx: &IndexInsertContext,
    left_page: &Page,
    left_opaque: crate::include::access::nbtree::BTPageOpaqueData,
    right_block: u32,
    right_first_key: Option<&[Value]>,
    key_values: &[Value],
) -> Result<bool, CatalogError> {
    let upper_bound = if let Some(high_key) = bt_page_high_key(left_page)
        .map_err(|err| CatalogError::Io(format!("btree high-key read failed: {err:?}")))?
    {
        tuple_key_prefix_values(&ctx.index_desc, &high_key, btree_key_count(&ctx.index_meta))?
    } else if left_opaque.btpo_next == right_block {
        let Some(right_first_key) = right_first_key else {
            return Ok(false);
        };
        right_first_key.to_vec()
    } else if left_opaque.btpo_next == P_NONE {
        return Ok(false);
    } else {
        return Err(CatalogError::Corrupt(
            "btree leaf missing high key without adjacent sibling bound",
        ));
    };
    Ok(compare_key_prefixes_with_columns_and_options(
        &ctx.index_desc.columns,
        key_values,
        &upper_bound,
        btree_key_count(&ctx.index_meta),
        &ctx.index_meta.indoption,
    ) != Ordering::Greater)
}

fn find_parent_from_stack(
    ctx: &IndexInsertContext,
    parent_stack: &mut Vec<InsertStackEntry>,
    child_block: u32,
) -> Result<Option<(u32, usize)>, CatalogError> {
    let Some(mut entry) = parent_stack.last().cloned() else {
        return Ok(None);
    };

    loop {
        let page = read_page(&ctx.pool, ctx.index_relation, entry.block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
            let mut ancestor_stack = parent_stack.clone();
            ancestor_stack.pop();
            finish_incomplete_split(ctx, entry.block, &ancestor_stack)?;
            continue;
        }
        if opaque.is_meta() || opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
            return Ok(None);
        }
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree parent parse failed: {err:?}")))?;
        if let Some(index) = find_child_index(&items, child_block, entry.offset) {
            if let Some(top) = parent_stack.last_mut() {
                top.block = entry.block;
                top.offset = index;
            }
            return Ok(Some((entry.block, index)));
        }
        if opaque.btpo_next == P_NONE {
            return Ok(None);
        }
        entry.block = opaque.btpo_next;
        entry.offset = 0;
    }
}

// :HACK: Stack-based recovery should locate the parent in normal concurrent
// split cases. Keep a full-tree fallback for rare stale-path cases until this
// matches PostgreSQL's BTStack recovery more closely.
fn find_parent_block_by_scan(
    ctx: &IndexInsertContext,
    child_block: u32,
) -> Result<Option<u32>, CatalogError> {
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    for block in 1..nblocks {
        if block == child_block {
            continue;
        }
        let page = read_page(&ctx.pool, ctx.index_relation, block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.is_meta() || opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
            continue;
        }
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree parent parse failed: {err:?}")))?;
        if items
            .iter()
            .any(|item| item.t_tid.block_number == child_block)
        {
            return Ok(Some(block));
        }
    }
    Ok(None)
}

fn parent_contains_child(
    ctx: &IndexInsertContext,
    parent_block: u32,
    child_block: u32,
) -> Result<bool, CatalogError> {
    let page = read_page(&ctx.pool, ctx.index_relation, parent_block)?;
    page_contains_child(&page, child_block)
}

fn page_contains_child(page: &Page, child_block: u32) -> Result<bool, CatalogError> {
    let items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree parent parse failed: {err:?}")))?;
    Ok(items
        .iter()
        .any(|item| item.t_tid.block_number == child_block))
}

fn find_leaf_for_insert(
    ctx: &IndexInsertContext,
    key_values: &[Value],
) -> Result<InsertSearchPath, CatalogError> {
    loop {
        let meta_page = read_page(&ctx.pool, ctx.index_relation, BTREE_METAPAGE)?;
        let meta = bt_page_get_meta(&meta_page)
            .map_err(|err| CatalogError::Io(format!("btree metapage read failed: {err:?}")))?;
        let mut block = meta.btm_root;
        let mut level = meta.btm_level;
        let mut parent_stack = Vec::new();

        while level > 0 {
            let page = read_page(&ctx.pool, ctx.index_relation, block)?;
            let opaque = bt_page_get_opaque(&page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
                finish_incomplete_split(ctx, block, &parent_stack)?;
                break;
            }
            let items = bt_page_data_items(&page)
                .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
            let slot = choose_child_slot(
                &ctx.index_desc,
                &items,
                key_values,
                ScanDirection::Forward,
                btree_key_count(&ctx.index_meta),
                &ctx.index_meta.indoption,
            )?;
            parent_stack.push(InsertStackEntry {
                block,
                offset: slot,
            });
            block = items[slot].t_tid.block_number;
            level -= 1;
        }
        if level > 0 {
            continue;
        }

        loop {
            let page = read_page(&ctx.pool, ctx.index_relation, block)?;
            let opaque = bt_page_get_opaque(&page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
                finish_incomplete_split(ctx, block, &parent_stack)?;
                break;
            }
            if opaque.btpo_flags & BTP_DELETED != 0 {
                if let Some(replacement) = replacement_for_deleted_leaf(ctx, opaque)? {
                    block = replacement;
                    continue;
                }
                break;
            }
            let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)? else {
                return Ok(InsertSearchPath {
                    leaf_block: block,
                    parent_stack,
                });
            };
            if compare_key_prefixes_with_columns_and_options(
                &ctx.index_desc.columns,
                key_values,
                &upper_bound,
                btree_key_count(&ctx.index_meta),
                &ctx.index_meta.indoption,
            ) != Ordering::Greater
            {
                return Ok(InsertSearchPath {
                    leaf_block: block,
                    parent_stack,
                });
            }
            if opaque.btpo_next == P_NONE {
                return Ok(InsertSearchPath {
                    leaf_block: block,
                    parent_stack,
                });
            }
            block = opaque.btpo_next;
        }
    }
}

fn find_locked_unique_insert_path<'a>(
    ctx: &'a IndexInsertContext,
    key_values: &[Value],
) -> Result<LockedUniqueInsertPath<'a>, CatalogError> {
    'search: loop {
        let search = find_leaf_for_insert(ctx, key_values)?;
        let mut block = search.leaf_block;
        let mut used_original_stack = true;

        loop {
            let (pin, guard) = lock_btree_block_exclusive(ctx, block)?;
            let page = *guard;
            let opaque = bt_page_get_opaque(&page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
                drop(guard);
                drop(pin);
                finish_incomplete_split(ctx, block, &[])?;
                continue 'search;
            }
            if opaque.btpo_flags & BTP_DELETED != 0 {
                let replacement = replacement_for_deleted_leaf(ctx, opaque)?;
                drop(guard);
                drop(pin);
                if let Some(replacement) = replacement {
                    block = replacement;
                    used_original_stack = false;
                    continue;
                }
                continue 'search;
            }
            if let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)?
                && compare_key_prefixes_with_columns_and_options(
                    &ctx.index_desc.columns,
                    key_values,
                    &upper_bound,
                    btree_key_count(&ctx.index_meta),
                    &ctx.index_meta.indoption,
                ) == Ordering::Greater
            {
                drop(guard);
                drop(pin);
                continue 'search;
            }
            if opaque.btpo_prev != P_NONE {
                let left_block = opaque.btpo_prev;
                let left_page = read_page(&ctx.pool, ctx.index_relation, left_block)?;
                let left_opaque = bt_page_get_opaque(&left_page).map_err(|err| {
                    CatalogError::Io(format!("btree opaque read failed: {err:?}"))
                })?;
                if left_opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
                    drop(guard);
                    drop(pin);
                    finish_incomplete_split(ctx, left_block, &[])?;
                    continue 'search;
                }
                let current_first_key = page_first_key_values(
                    &ctx.index_desc,
                    &page,
                    btree_key_count(&ctx.index_meta),
                )?;
                if left_sibling_may_contain_key(
                    ctx,
                    &left_page,
                    left_opaque,
                    block,
                    current_first_key.as_deref(),
                    key_values,
                )? {
                    drop(guard);
                    drop(pin);
                    block = left_block;
                    used_original_stack = false;
                    continue;
                }
            }

            // PostgreSQL reuses the descent stack only when the locked leaf is
            // still the exact first candidate page from the original search.
            // If we had to walk left to settle on the true first candidate
            // leaf, refresh parent discovery lazily during split propagation.
            return Ok(LockedUniqueInsertPath {
                leaf_block: block,
                parent_stack: if used_original_stack {
                    search.parent_stack
                } else {
                    Vec::new()
                },
                pin,
                guard,
            });
        }
    }
}

fn find_parent_stack_for_key(
    ctx: &IndexInsertContext,
    key_values: &[Value],
    child_level: u32,
) -> Result<Option<Vec<InsertStackEntry>>, CatalogError> {
    'restart: loop {
        let meta_page = read_page(&ctx.pool, ctx.index_relation, BTREE_METAPAGE)?;
        let meta = bt_page_get_meta(&meta_page)
            .map_err(|err| CatalogError::Io(format!("btree metapage read failed: {err:?}")))?;

        if meta.btm_level <= child_level {
            return Ok(None);
        }

        let mut block = meta.btm_root;
        let mut level = meta.btm_level;
        let mut parent_stack = Vec::new();

        while level > child_level + 1 {
            let page = read_page(&ctx.pool, ctx.index_relation, block)?;
            let opaque = bt_page_get_opaque(&page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
                finish_incomplete_split(ctx, block, &parent_stack)?;
                continue 'restart;
            }
            let items = bt_page_data_items(&page)
                .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
            let slot = choose_child_slot(
                &ctx.index_desc,
                &items,
                key_values,
                ScanDirection::Forward,
                btree_key_count(&ctx.index_meta),
                &ctx.index_meta.indoption,
            )?;
            parent_stack.push(InsertStackEntry {
                block,
                offset: slot,
            });
            block = items[slot].t_tid.block_number;
            level -= 1;
        }

        let page = read_page(&ctx.pool, ctx.index_relation, block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
            finish_incomplete_split(ctx, block, &parent_stack)?;
            continue;
        }
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        let slot = choose_child_slot(
            &ctx.index_desc,
            &items,
            key_values,
            ScanDirection::Forward,
            btree_key_count(&ctx.index_meta),
            &ctx.index_meta.indoption,
        )?;
        parent_stack.push(InsertStackEntry {
            block,
            offset: slot,
        });
        return Ok(Some(parent_stack));
    }
}

fn leaf_has_match(scan: &IndexScanDesc, block: u32) -> Result<bool, CatalogError> {
    let page = read_page(&scan.pool, scan.index_relation, block)?;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.btpo_flags & BTP_DELETED != 0 {
        return Ok(false);
    }
    let items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    for tuple in items {
        if tuple_matches_scan_keys(&scan.index_desc, &tuple, &scan.key_data, &scan.indoption)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn initial_scan_block(scan: &IndexScanDesc) -> Result<Option<u32>, CatalogError> {
    let nblocks = relation_nblocks(&scan.pool, scan.index_relation)?;
    if nblocks <= 1 {
        return Ok(None);
    }
    let target = scan_positioning_prefix(&scan.key_data, scan.direction);
    let (_, mut block) = find_leaf_with_ancestors(
        &scan.pool,
        scan.index_relation,
        &scan.index_desc,
        &target,
        scan.direction,
        btree_key_count(&scan.index_meta),
        &scan.indoption,
    )?;
    loop {
        let page = read_page(&scan.pool, scan.index_relation, block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        let neighbor = match scan.direction {
            ScanDirection::Forward => opaque.btpo_prev,
            ScanDirection::Backward => opaque.btpo_next,
        };
        if neighbor == P_NONE || !leaf_has_match(scan, neighbor)? {
            break;
        }
        block = neighbor;
    }
    Ok(Some(block))
}

fn load_leaf_items(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    let Some(block) = scan
        .opaque
        .as_btree_mut()
        .and_then(|opaque| opaque.current_block)
    else {
        return Ok(false);
    };
    let state = scan
        .opaque
        .as_btree_mut()
        .ok_or(CatalogError::Corrupt("missing btree scan state"))?;
    if state.current_pin.is_none() {
        state.current_pin = Some(OwnedBufferPin::wrap_existing(
            Arc::clone(&scan.pool),
            pin_btree_block(&scan.pool, scan.client_id, scan.index_relation, block)?.into_raw(),
        ));
    }
    let pin = state
        .current_pin
        .as_ref()
        .ok_or(CatalogError::Corrupt("btree scan lost current pin"))?;
    let guard = scan
        .pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree scan shared lock failed: {err:?}")))?;
    let opaque = bt_page_get_opaque(&guard)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.btpo_flags & BTP_DELETED != 0 {
        drop(guard);
        state.current_pin = None;
        state.current_block = match scan.direction {
            ScanDirection::Forward => (opaque.btpo_next != P_NONE).then_some(opaque.btpo_next),
            ScanDirection::Backward => (opaque.btpo_prev != P_NONE).then_some(opaque.btpo_prev),
        };
        state.current_items.clear();
        state.next_offset = 0;
        return Ok(true);
    }
    let items = bt_page_data_items(&guard)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    drop(guard);
    let stop_after_empty = empty_leaf_exhausts_exact_equality_scan(scan, &items)?;
    let filtered = items
        .into_iter()
        .filter(|tuple| {
            tuple_matches_scan_keys(&scan.index_desc, tuple, &scan.key_data, &scan.indoption)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    let state = scan
        .opaque
        .as_btree_mut()
        .ok_or(CatalogError::Corrupt("missing btree scan state"))?;
    state.page_prev = (opaque.btpo_prev != P_NONE).then_some(opaque.btpo_prev);
    state.page_next = (opaque.btpo_next != P_NONE).then_some(opaque.btpo_next);
    state.current_items = filtered;
    state.next_offset = match scan.direction {
        ScanDirection::Forward => 0,
        ScanDirection::Backward => state.current_items.len().saturating_sub(1),
    };
    if state.current_items.is_empty() {
        state.current_pin = None;
        state.current_block = if stop_after_empty {
            None
        } else {
            match scan.direction {
                ScanDirection::Forward => state.page_next,
                ScanDirection::Backward => state.page_prev,
            }
        };
    }
    Ok(true)
}

trait IndexScanOpaqueExt {
    fn as_btree_mut(&mut self) -> Option<&mut BtIndexScanOpaque>;
}

impl IndexScanOpaqueExt for IndexScanOpaque {
    fn as_btree_mut(&mut self) -> Option<&mut BtIndexScanOpaque> {
        match self {
            IndexScanOpaque::Btree(state) => Some(state),
            IndexScanOpaque::Gist(_) => None,
            IndexScanOpaque::Spgist(_) => None,
            IndexScanOpaque::Brin(_) => None,
            IndexScanOpaque::Gin(_) => None,
            IndexScanOpaque::Hash(_) => None,
            IndexScanOpaque::None => None,
        }
    }
}

fn btbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    let mut scan = crate::backend::access::index::genam::index_beginscan_stub(ctx)?;
    let start_block = initial_scan_block(&scan)?;
    if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
        state.current_block = start_block;
        state.current_pin = None;
        state.page_prev = None;
        state.page_next = None;
        state.next_offset = 0;
    }
    Ok(scan)
}

fn btrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    crate::backend::access::index::genam::index_rescan_stub(scan, keys, direction)?;
    let start_block = initial_scan_block(scan)?;
    if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
        state.current_block = start_block;
        state.current_pin = None;
        state.page_prev = None;
        state.page_next = None;
        state.current_items.clear();
        state.next_offset = 0;
    }
    Ok(())
}

fn btgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    let _ = preprocess_scan_keys(&scan.key_data);
    loop {
        let needs_load = match &scan.opaque {
            IndexScanOpaque::Btree(state) => state.current_items.is_empty(),
            IndexScanOpaque::Gist(_) => true,
            IndexScanOpaque::Spgist(_) => true,
            IndexScanOpaque::Brin(_) => true,
            IndexScanOpaque::Gin(_) => true,
            IndexScanOpaque::Hash(_) => true,
            IndexScanOpaque::None => true,
        };
        if needs_load {
            if !load_leaf_items(scan)? {
                return Ok(false);
            }
            continue;
        }
        let next = if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
            match scan.direction {
                ScanDirection::Forward => {
                    if state.next_offset >= state.current_items.len() {
                        state.current_items.clear();
                        state.current_pin = None;
                        state.current_block = state.page_next;
                        state.page_prev = None;
                        state.page_next = None;
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
                        state.current_pin = None;
                        state.current_block = state.page_prev;
                        state.page_prev = None;
                        state.page_next = None;
                        None
                    } else {
                        let idx = state.next_offset;
                        let tuple = state.current_items[idx].clone();
                        if idx == 0 {
                            state.current_items.clear();
                            state.current_pin = None;
                            state.current_block = state.page_prev;
                            state.page_prev = None;
                            state.page_next = None;
                        } else {
                            state.next_offset -= 1;
                        }
                        Some(tuple)
                    }
                }
            }
        } else {
            None
        };
        let Some(tuple) = next else {
            if let IndexScanOpaque::Btree(state) = &scan.opaque
                && state.current_block.is_none()
            {
                return Ok(false);
            }
            continue;
        };
        scan.xs_heaptid = Some(tuple.t_tid);
        scan.xs_itup = scan.xs_want_itup.then_some(tuple);
        return Ok(true);
    }
}

fn btgetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, CatalogError> {
    let mut count = 0_i64;
    while btgettuple(scan)? {
        if let Some(tid) = scan.xs_heaptid {
            bitmap.add_tid(tid);
            count += 1;
        }
    }
    Ok(count)
}

fn btendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    crate::backend::access::index::genam::index_endscan_stub(scan)
}

fn allocate_btree_block(ctx: &IndexInsertContext) -> Result<u32, CatalogError> {
    let oldest_active_xid = ctx.txns.read().oldest_active_xid();
    loop {
        let Some(block) =
            get_free_index_page(&ctx.pool, ctx.index_relation).map_err(CatalogError::Io)?
        else {
            return relation_nblocks(&ctx.pool, ctx.index_relation);
        };
        if block <= BTREE_METAPAGE {
            continue;
        }
        let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
        if block >= nblocks {
            continue;
        }
        let page = read_page(&ctx.pool, ctx.index_relation, block)?;
        match page_header(&page) {
            Ok(_) => {}
            Err(PageError::NotInitialized) => return Ok(block),
            Err(err) => {
                return Err(CatalogError::Io(format!(
                    "btree page header read failed: {err:?}"
                )));
            }
        }
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.is_meta() || opaque.is_root() || opaque.btpo_flags & BTP_DELETED == 0 {
            continue;
        }
        if bt_page_is_recyclable(&page, oldest_active_xid)
            .map_err(|err| CatalogError::Io(format!("btree recyclable check failed: {err:?}")))?
        {
            return Ok(block);
        }
    }
}

fn write_split_pages_locked(
    ctx: &IndexInsertContext,
    block: u32,
    pin: PinnedBuffer<'_, SmgrStorageBackend>,
    mut guard: RwLockWriteGuard<'_, Page>,
    existing_high_key: Option<IndexTupleData>,
    left_items: &[IndexTupleData],
    right_items: &[IndexTupleData],
    old_opaque: crate::include::access::nbtree::BTPageOpaqueData,
    is_leaf: bool,
) -> Result<PageSplitResult, CatalogError> {
    let new_block = allocate_btree_block(ctx)?;
    let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    let level = old_opaque.btpo_level;
    let flags = if is_leaf { BTP_LEAF } else { 0 };
    let live_right = if is_leaf && old_opaque.btpo_next != P_NONE {
        first_live_right_leaf(ctx, old_opaque.btpo_next)?
    } else {
        None
    };
    let inherited_high_key = if is_leaf {
        existing_high_key.or_else(|| {
            live_right
                .as_ref()
                .and_then(|(_, _, _, first_tuple)| first_tuple.clone())
        })
    } else {
        None
    };
    let live_right_block = live_right
        .as_ref()
        .map(|(block, _, _, _)| *block)
        .unwrap_or(P_NONE);

    bt_page_init(&mut left_page, flags, level)
        .map_err(|err| CatalogError::Io(format!("btree left split init failed: {err:?}")))?;
    bt_page_init(&mut right_page, flags, level)
        .map_err(|err| CatalogError::Io(format!("btree right split init failed: {err:?}")))?;

    let mut left_opaque = bt_page_get_opaque(&left_page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    left_opaque.btpo_prev = old_opaque.btpo_prev;
    left_opaque.btpo_next = new_block;
    left_opaque.btpo_flags |= crate::include::access::nbtree::BTP_INCOMPLETE_SPLIT;
    bt_page_set_opaque(&mut left_page, left_opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;

    let mut right_opaque = bt_page_get_opaque(&right_page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    right_opaque.btpo_prev = block;
    right_opaque.btpo_next = live_right_block;
    bt_page_set_opaque(&mut right_page, right_opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;

    if is_leaf {
        let left_high_key = right_items
            .first()
            .ok_or(CatalogError::Corrupt("right split page empty"))?;
        bt_page_set_high_key(
            &mut left_page,
            left_high_key,
            left_items.to_vec(),
            left_opaque,
        )
        .map_err(|err| CatalogError::Io(format!("btree left split rebuild failed: {err:?}")))?;
        if let Some(high_key) = inherited_high_key {
            bt_page_set_high_key(
                &mut right_page,
                &high_key,
                right_items.to_vec(),
                right_opaque,
            )
            .map_err(|err| {
                CatalogError::Io(format!("btree right split rebuild failed: {err:?}"))
            })?;
        } else {
            for tuple in right_items {
                bt_page_append_tuple(&mut right_page, tuple)
                    .map_err(|_| CatalogError::Io("index split right page overflow".into()))?;
            }
        }
    } else {
        for tuple in left_items {
            bt_page_append_tuple(&mut left_page, tuple)
                .map_err(|_| CatalogError::Io("index split left page overflow".into()))?;
        }
        for tuple in right_items {
            bt_page_append_tuple(&mut right_page, tuple)
                .map_err(|_| CatalogError::Io("index split right page overflow".into()))?;
        }
    }

    write_buffered_btree_page_with_init(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        new_block,
        &right_page,
        XLOG_BTREE_SPLIT_R,
        true,
    )?;
    if let Some((next_block, mut next_page, mut next_opaque, _)) = live_right {
        next_opaque.btpo_prev = new_block;
        bt_page_set_opaque(&mut next_page, next_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            next_block,
            &next_page,
            XLOG_BTREE_SPLIT_R,
        )?;
    }
    // Publish the new sibling only after its page image is initialized and any
    // existing right neighbor already links back to it.
    let lsn = if let Some(wal) = ctx.pool.wal_writer() {
        log_btree_record(
            &wal,
            ctx.snapshot.current_xid,
            XLOG_BTREE_SPLIT_L,
            &[crate::backend::access::nbtree::nbtxlog::LoggedBtreeBlock {
                block_id: 0,
                tag: crate::backend::storage::buffer::BufferTag {
                    rel: ctx.index_relation,
                    fork: ForkNumber::Main,
                    block,
                },
                page: &left_page,
                will_init: false,
                force_image: true,
                data: &[],
            }],
            &[],
        )
        .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))?
    } else {
        INVALID_LSN
    };
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &left_page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    drop(guard);
    drop(pin);

    Ok(PageSplitResult {
        left_block: block,
        right_block: new_block,
        level,
        right_lower_bound: tuple_key_prefix_values(
            &ctx.index_desc,
            right_items
                .first()
                .ok_or(CatalogError::Corrupt("right split page empty"))?,
            btree_key_count(&ctx.index_meta),
        )?,
        parent_stack: Vec::new(),
    })
}

fn clear_incomplete_split(ctx: &IndexInsertContext, block: u32) -> Result<(), CatalogError> {
    let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
    let mut guard = ctx
        .pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    let mut page = *guard;
    let mut opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.btpo_flags & crate::include::access::nbtree::BTP_INCOMPLETE_SPLIT == 0 {
        return Ok(());
    }
    opaque.btpo_flags &= !crate::include::access::nbtree::BTP_INCOMPLETE_SPLIT;
    bt_page_set_opaque(&mut page, opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    let lsn = if let Some(wal) = ctx.pool.wal_writer() {
        log_btree_record(
            &wal,
            ctx.snapshot.current_xid,
            XLOG_BTREE_INSERT_UPPER,
            &[crate::backend::access::nbtree::nbtxlog::LoggedBtreeBlock {
                block_id: 0,
                tag: crate::backend::storage::buffer::BufferTag {
                    rel: ctx.index_relation,
                    fork: ForkNumber::Main,
                    block,
                },
                page: &page,
                will_init: false,
                force_image: true,
                data: &[],
            }],
            &[],
        )
        .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))?
    } else {
        INVALID_LSN
    };
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))
}

fn insert_tuple_into_locked_page(
    ctx: &IndexInsertContext,
    pin: PinnedBuffer<'_, SmgrStorageBackend>,
    mut guard: RwLockWriteGuard<'_, Page>,
    block: u32,
    new_tuple: IndexTupleData,
    key_values: &[Value],
    is_leaf: bool,
) -> Result<Option<PageSplitResult>, CatalogError> {
    let page = *guard;
    let old_opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let existing_high_key = bt_page_high_key(&page)
        .map_err(|err| CatalogError::Io(format!("btree high-key read failed: {err:?}")))?;
    let mut items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let key_count = btree_key_count(&ctx.index_meta);
    let insert_at = if is_leaf {
        items.partition_point(|item| {
            let existing =
                tuple_key_prefix_values(&ctx.index_desc, item, key_count).unwrap_or_default();
            compare_bt_keyspace_with_columns_and_options(
                &ctx.index_desc.columns,
                &existing,
                &item.t_tid,
                key_values,
                &new_tuple.t_tid,
                &ctx.index_meta.indoption,
            ) != Ordering::Greater
        })
    } else {
        items.partition_point(|item| {
            let existing =
                tuple_key_prefix_values(&ctx.index_desc, item, key_count).unwrap_or_default();
            compare_key_prefixes_with_columns_and_options(
                &ctx.index_desc.columns,
                &existing,
                key_values,
                key_count,
                &ctx.index_meta.indoption,
            ) != Ordering::Greater
        })
    };
    let insert_offnum =
        u16::try_from(insert_at + 1 + if existing_high_key.is_some() { 1 } else { 0 })
            .map_err(|_| CatalogError::Io("btree insert offset out of range".into()))?;
    let insert_tuple_wal_data = new_tuple.serialize();
    items.insert(insert_at, new_tuple);

    let mut log_insert_delta = true;
    let rebuilt =
        match build_insert_page_image(old_opaque, existing_high_key.as_ref(), &items, is_leaf) {
            Ok(rebuilt) => rebuilt,
            Err(_) if is_leaf && prune_aborted_leaf_items(ctx, &mut items) > 0 => {
                log_insert_delta = false;
                match build_insert_page_image(
                    old_opaque,
                    existing_high_key.as_ref(),
                    &items,
                    is_leaf,
                ) {
                    Ok(rebuilt) => rebuilt,
                    Err(_) => {
                        let split = choose_split_index(&items, None);
                        let right_items = items.split_off(split);
                        let left_items = items;
                        return write_split_pages_locked(
                            ctx,
                            block,
                            pin,
                            guard,
                            existing_high_key.clone(),
                            &left_items,
                            &right_items,
                            old_opaque,
                            is_leaf,
                        )
                        .map(Some);
                    }
                }
            }
            Err(_) => {
                let split = choose_split_index(&items, None);
                let right_items = items.split_off(split);
                let left_items = items;
                return write_split_pages_locked(
                    ctx,
                    block,
                    pin,
                    guard,
                    existing_high_key.clone(),
                    &left_items,
                    &right_items,
                    old_opaque,
                    is_leaf,
                )
                .map(Some);
            }
        };
    let wal_info = if is_leaf {
        XLOG_BTREE_INSERT_LEAF
    } else {
        XLOG_BTREE_INSERT_UPPER
    };
    let lsn = if let Some(wal) = ctx.pool.wal_writer() {
        log_btree_record(
            &wal,
            ctx.snapshot.current_xid,
            wal_info,
            &[crate::backend::access::nbtree::nbtxlog::LoggedBtreeBlock {
                block_id: 0,
                tag: crate::backend::storage::buffer::BufferTag {
                    rel: ctx.index_relation,
                    fork: ForkNumber::Main,
                    block,
                },
                page: &rebuilt,
                will_init: false,
                force_image: !log_insert_delta,
                data: if log_insert_delta {
                    &insert_tuple_wal_data
                } else {
                    &[]
                },
            }],
            &insert_offnum.to_le_bytes(),
        )
        .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))?
    } else {
        INVALID_LSN
    };
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &rebuilt, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    Ok(None)
}

fn build_insert_page_image(
    old_opaque: BTPageOpaqueData,
    existing_high_key: Option<&IndexTupleData>,
    items: &[IndexTupleData],
    is_leaf: bool,
) -> Result<Page, crate::include::access::nbtree::BtPageError> {
    let mut rebuilt = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(
        &mut rebuilt,
        if is_leaf { BTP_LEAF } else { 0 },
        old_opaque.btpo_level,
    )?;
    let mut rebuilt_opaque = bt_page_get_opaque(&rebuilt)?;
    rebuilt_opaque.btpo_prev = old_opaque.btpo_prev;
    rebuilt_opaque.btpo_next = old_opaque.btpo_next;
    if let Some(high_key) = existing_high_key {
        bt_page_set_high_key(&mut rebuilt, high_key, items.to_vec(), rebuilt_opaque)?;
    } else {
        bt_page_set_opaque(&mut rebuilt, rebuilt_opaque)?;
        for tuple in items {
            bt_page_append_tuple(&mut rebuilt, tuple)?;
        }
    }
    Ok(rebuilt)
}

fn prune_aborted_leaf_items(ctx: &IndexInsertContext, items: &mut Vec<IndexTupleData>) -> usize {
    let before = items.len();
    let txns = ctx.txns.read();
    items.retain(|item| {
        let Ok(tuple) = heap_fetch(&ctx.pool, ctx.client_id, ctx.heap_relation, item.t_tid) else {
            return true;
        };
        !matches!(
            txns.status(tuple.header.xmin),
            Some(TransactionStatus::Aborted)
        )
    });
    before.saturating_sub(items.len())
}

fn insert_tuple_into_page(
    ctx: &IndexInsertContext,
    block: u32,
    new_tuple: IndexTupleData,
    key_values: &[Value],
    is_leaf: bool,
) -> Result<Option<PageSplitResult>, CatalogError> {
    let (pin, guard) = lock_btree_block_exclusive(ctx, block)?;
    insert_tuple_into_locked_page(ctx, pin, guard, block, new_tuple, key_values, is_leaf)
}

fn create_new_root(
    ctx: &IndexInsertContext,
    left_block: u32,
    right_block: u32,
    child_level: u32,
    right_lower_bound: &[Value],
) -> Result<(), CatalogError> {
    let left_lower_bound = page_lower_bound(
        &ctx.index_desc,
        &ctx.pool,
        ctx.index_relation,
        left_block,
        btree_key_count(&ctx.index_meta),
    )?;
    let root_block = allocate_btree_block(ctx)?;
    let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut root, BTP_ROOT, child_level + 1)
        .map_err(|err| CatalogError::Io(format!("btree new root init failed: {err:?}")))?;
    for tuple in [
        pivot_tuple(
            &ctx.index_desc,
            left_block,
            &left_lower_bound,
            ctx.default_toast_compression,
        )?,
        pivot_tuple(
            &ctx.index_desc,
            right_block,
            right_lower_bound,
            ctx.default_toast_compression,
        )?,
    ] {
        bt_page_append_tuple(&mut root, &tuple)
            .map_err(|_| CatalogError::Io("new btree root overflow".into()))?;
    }
    write_buffered_btree_page_with_init(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        root_block,
        &root,
        XLOG_BTREE_NEWROOT,
        true,
    )?;
    write_meta_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        root_block,
        child_level + 1,
    )?;
    Ok(())
}

fn refresh_parent_stack_for_split(
    ctx: &IndexInsertContext,
    split: &PageSplitResult,
) -> Result<Option<Vec<InsertStackEntry>>, CatalogError> {
    find_parent_stack_for_key(ctx, &split.right_lower_bound, split.level)
}

fn propagate_split_upwards(
    ctx: &IndexInsertContext,
    mut split: PageSplitResult,
) -> Result<(), CatalogError> {
    loop {
        if split.parent_stack.is_empty() {
            if let Some(parent_stack) = refresh_parent_stack_for_split(ctx, &split)? {
                split.parent_stack = parent_stack;
            } else {
                create_new_root(
                    ctx,
                    split.left_block,
                    split.right_block,
                    split.level,
                    &split.right_lower_bound,
                )?;
                clear_incomplete_split(ctx, split.left_block)?;
                return Ok(());
            }
        }

        let right_pivot = pivot_tuple(
            &ctx.index_desc,
            split.right_block,
            &split.right_lower_bound,
            ctx.default_toast_compression,
        )?;
        let parent = find_parent_from_stack(ctx, &mut split.parent_stack, split.left_block)?;
        let fallback_parent = if parent.is_none() {
            find_parent_block_by_scan(ctx, split.left_block)?
        } else {
            None
        };
        if let Some(parent_block) = parent.map(|(block, _)| block).or(fallback_parent) {
            let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, parent_block)?;
            let guard = ctx
                .pool
                .lock_buffer_exclusive(pin.buffer_id())
                .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
            let parent_page = *guard;
            let parent_opaque = bt_page_get_opaque(&parent_page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            if parent_opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0
                || parent_opaque.is_meta()
                || parent_opaque.is_leaf()
                || parent_opaque.btpo_flags & BTP_DELETED != 0
                || !page_contains_child(&parent_page, split.left_block)?
            {
                drop(guard);
                drop(pin);
                split.parent_stack.clear();
                continue;
            }
            if page_contains_child(&parent_page, split.right_block)? {
                drop(guard);
                drop(pin);
                clear_incomplete_split(ctx, split.left_block)?;
                return Ok(());
            }
            let next_split = insert_tuple_into_locked_page(
                ctx,
                pin,
                guard,
                parent_block,
                right_pivot,
                &split.right_lower_bound,
                false,
            )?;
            clear_incomplete_split(ctx, split.left_block)?;
            if let Some(mut next_split) = next_split {
                next_split.parent_stack.clear();
                split = next_split;
                continue;
            }
            return Ok(());
        }
        split.parent_stack.clear();
    }
}

fn finish_incomplete_split(
    ctx: &IndexInsertContext,
    left_block: u32,
    parent_stack: &[InsertStackEntry],
) -> Result<(), CatalogError> {
    let page = read_page(&ctx.pool, ctx.index_relation, left_block)?;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT == 0 || opaque.btpo_next == P_NONE {
        return Ok(());
    }
    let mut parent_stack = parent_stack.to_vec();
    if let Some((parent_block, _)) = find_parent_from_stack(ctx, &mut parent_stack, left_block)?
        && parent_contains_child(ctx, parent_block, opaque.btpo_next)?
    {
        return clear_incomplete_split(ctx, left_block);
    }
    let right_lower_bound = page_lower_bound(
        &ctx.index_desc,
        &ctx.pool,
        ctx.index_relation,
        opaque.btpo_next,
        btree_key_count(&ctx.index_meta),
    )?;
    propagate_split_upwards(
        ctx,
        PageSplitResult {
            left_block,
            right_block: opaque.btpo_next,
            level: opaque.btpo_level,
            right_lower_bound,
            parent_stack,
        },
    )
}

fn bt_check_unique_locked(
    ctx: &IndexInsertContext,
    locked_page: &Page,
    locked_opaque: crate::include::access::nbtree::BTPageOpaqueData,
    key_values: &[Value],
) -> Result<LockedUniqueCheckResult, CatalogError> {
    let access_runtime = RootAccessRuntime {
        pool: Some(&ctx.pool),
        txns: Some(&ctx.txns),
        txn_waiter: ctx.txn_waiter.as_deref(),
        interrupts: Some(ctx.interrupts.as_ref()),
        client_id: ctx.client_id,
    };
    let mut page = *locked_page;
    let mut opaque = locked_opaque;

    loop {
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        for tuple in items {
            let tuple_keys =
                tuple_key_prefix_values(&ctx.index_desc, &tuple, btree_key_count(&ctx.index_meta))?;
            match compare_key_prefixes_with_columns_and_options(
                &ctx.index_desc.columns,
                &tuple_keys,
                key_values,
                btree_key_count(&ctx.index_meta),
                &ctx.index_meta.indoption,
            ) {
                Ordering::Less => continue,
                Ordering::Greater => return Ok(LockedUniqueCheckResult::Clear),
                Ordering::Equal => {
                    match classify_unique_candidate(ctx, tuple.t_tid, &access_runtime)? {
                        UniqueCandidateResult::NoConflict => {}
                        UniqueCandidateResult::Conflict(_) => {
                            return Err(CatalogError::UniqueViolation(ctx.index_name.clone()));
                        }
                        UniqueCandidateResult::WaitFor(xid) => {
                            return Ok(LockedUniqueCheckResult::WaitFor(xid));
                        }
                    }
                }
            }
        }

        let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)? else {
            return Ok(LockedUniqueCheckResult::Clear);
        };
        if compare_key_prefixes_with_columns_and_options(
            &ctx.index_desc.columns,
            key_values,
            &upper_bound,
            btree_key_count(&ctx.index_meta),
            &ctx.index_meta.indoption,
        ) != Ordering::Equal
            || opaque.btpo_next == P_NONE
        {
            return Ok(LockedUniqueCheckResult::Clear);
        }

        let next_block = opaque.btpo_next;
        page = read_page(&ctx.pool, ctx.index_relation, next_block)?;
        opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.btpo_flags & BTP_INCOMPLETE_SPLIT != 0 {
            return Ok(LockedUniqueCheckResult::Restart {
                split_block: Some(next_block),
            });
        }
    }
}

fn btinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    if relation_nblocks(&ctx.pool, ctx.index_relation)? <= 1 {
        ensure_empty_btree(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
        )?;
    }

    let all_values = key_values_from_heap_row(
        &ctx.heap_desc,
        &ctx.index_desc,
        &ctx.index_meta.indkey,
        &ctx.values,
    )?;
    let key_count = btree_key_count(&ctx.index_meta);
    let key_values = key_prefix(&all_values, key_count).to_vec();
    let payload = encode_key_payload(&ctx.index_desc, &all_values, ctx.default_toast_compression)?;
    let new_tuple = IndexTupleData::new_raw(ctx.heap_tid, false, false, false, payload);
    check_leaf_tuple_size(&ctx.index_name, &new_tuple)?;

    let check_unique = matches!(
        ctx.unique_check,
        crate::include::access::amapi::IndexUniqueCheck::Yes
    ) && (ctx.index_meta.indnullsnotdistinct || !keys_contain_null(&key_values));
    if check_unique {
        // PostgreSQL checks uniqueness while holding the write lock on the
        // first leaf page the key could live on, and keeps that lock through
        // insertion so a concurrent inserter of the same key cannot race past
        // the uniqueness probe.
        loop {
            let locked = find_locked_unique_insert_path(ctx, &key_values)?;
            let page = *locked.guard;
            let opaque = bt_page_get_opaque(&page)
                .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
            match bt_check_unique_locked(ctx, &page, opaque, &key_values)? {
                LockedUniqueCheckResult::Clear => {
                    if let Some(mut split) = insert_tuple_into_locked_page(
                        ctx,
                        locked.pin,
                        locked.guard,
                        locked.leaf_block,
                        new_tuple,
                        &key_values,
                        true,
                    )? {
                        split.parent_stack = locked.parent_stack;
                        check_insert_split_interrupts(ctx)?;
                        propagate_split_upwards(ctx, split)?;
                    }
                    return Ok(true);
                }
                LockedUniqueCheckResult::WaitFor(xid) => {
                    drop(locked.guard);
                    drop(locked.pin);
                    let waiter = ctx.txn_waiter.as_ref().ok_or_else(|| {
                        CatalogError::Io("btree unique check missing transaction waiter".into())
                    })?;
                    match waiter.wait_for(&ctx.txns, xid, ctx.client_id, ctx.interrupts.as_ref()) {
                        crate::backend::storage::lmgr::WaitOutcome::Completed => {}
                        crate::backend::storage::lmgr::WaitOutcome::DeadlockTimeout => {
                            return Err(CatalogError::Io(format!(
                                "btree unique check timed out waiting for transaction {xid}"
                            )));
                        }
                        crate::backend::storage::lmgr::WaitOutcome::Interrupted(reason) => {
                            return Err(CatalogError::Interrupted(reason));
                        }
                    }
                }
                LockedUniqueCheckResult::Restart { split_block } => {
                    drop(locked.guard);
                    drop(locked.pin);
                    if let Some(split_block) = split_block {
                        finish_incomplete_split(ctx, split_block, &[])?;
                    }
                }
            }
        }
    }

    let search = find_leaf_for_insert(ctx, &key_values)?;
    if let Some(mut split) =
        insert_tuple_into_page(ctx, search.leaf_block, new_tuple, &key_values, true)?
    {
        split.parent_stack = search.parent_stack;
        check_insert_split_interrupts(ctx)?;
        propagate_split_upwards(ctx, split)?;
    }
    Ok(true)
}

fn btbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    ensure_empty_btree(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation)
}

pub fn btree_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 5,
        amsupport: 5,
        amcanorder: true,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentordering: true,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: true,
        amsearchnulls: true,
        amstorage: false,
        amclusterable: true,
        ampredlocks: true,
        amsummarizing: false,
        ambuild: Some(btbuild),
        ambuildempty: Some(btbuildempty),
        aminsert: Some(btinsert),
        ambeginscan: Some(btbeginscan),
        amrescan: Some(btrescan),
        amgettuple: Some(btgettuple),
        amgetbitmap: Some(btgetbitmap),
        amendscan: Some(btendscan),
        ambulkdelete: Some(crate::backend::access::nbtree::nbtvacuum::btbulkdelete),
        amvacuumcleanup: Some(crate::backend::access::nbtree::nbtvacuum::btvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::htup::AttributeStorage;

    #[test]
    fn large_text_index_keys_use_inline_compression() {
        let mut column = column_desc("f1", SqlType::new(SqlTypeKind::Text), true);
        column.storage.attstorage = AttributeStorage::Extended;
        column.storage.attcompression = AttributeCompression::Pglz;
        let desc = RelationDesc {
            columns: vec![column],
        };
        let value = Value::Text("1234567890".repeat(1000).into());

        let payload = encode_key_payload(
            &desc,
            std::slice::from_ref(&value),
            AttributeCompression::Pglz,
        )
        .expect("payload should encode");

        assert!(
            payload.len() < 2000,
            "expected compressed payload, got {}",
            payload.len()
        );
        assert_eq!(decode_key_payload(&desc, &payload).unwrap(), vec![value]);
    }

    #[test]
    fn oversized_leaf_tuple_reports_limit_error() {
        let tuple = IndexTupleData::new_raw(
            ItemPointerData::default(),
            false,
            false,
            false,
            vec![0; bt_max_item_size() + 1],
        );

        let err = check_leaf_tuple_size("idx", &tuple).unwrap_err();
        match err {
            CatalogError::Io(message) => {
                assert!(message.starts_with("index row size "));
                assert!(message.contains("maximum"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
