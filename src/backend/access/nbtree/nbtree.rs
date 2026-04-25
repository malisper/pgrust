use std::cmp::Ordering;
use std::sync::Arc;

use parking_lot::RwLockWriteGuard;

use crate::backend::access::common::toast_compression::compress_inline_datum;
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::access::index::unique::{UniqueCandidateResult, classify_unique_candidate};
use crate::backend::access::nbtree::nbtcompare::{compare_bt_keyspace, compare_bt_values};
use crate::backend::access::nbtree::nbtpreprocesskeys::preprocess_scan_keys;
use crate::backend::access::nbtree::nbtsplitloc::choose_split_index;
use crate::backend::access::nbtree::nbtutils::BtSortTuple;
use crate::backend::access::nbtree::nbtxlog::log_btree_record;
use crate::backend::access::transam::xact::TransactionId;
use crate::backend::access::transam::xlog::{
    INVALID_LSN, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_UPPER,
    XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R, XLOG_FPI,
};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::render_datetime_value_text;
use crate::backend::executor::value_io::{decode_value, encode_anyarray_bytes, encode_array_bytes};
use crate::backend::storage::fsm::get_free_index_page;
use crate::backend::storage::page::bufpage::{MAX_HEAP_TUPLE_SIZE, max_align, page_header};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::backend::utils::misc::interrupts::check_for_interrupts;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexInsertContext,
};
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::access::nbtree::{
    BTP_DELETED, BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_ROOT, BTREE_DEFAULT_FILLFACTOR,
    BTREE_METAPAGE, BTREE_NONLEAF_FILLFACTOR, BTREE_VERSION, P_NONE, bt_init_meta_page,
    bt_max_item_size, bt_page_append_tuple, bt_page_data_items, bt_page_get_meta,
    bt_page_get_opaque, bt_page_high_key, bt_page_init, bt_page_is_recyclable,
    bt_page_set_high_key, bt_page_set_opaque,
};
use crate::include::access::relscan::{
    BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{ColumnDesc, RelationDesc};
use crate::include::storage::buf_internals::Page;
use crate::{BufferPool, ClientId, OwnedBufferPin, PinnedBuffer, SmgrStorageBackend};

fn check_catalog_interrupts(
    interrupts: &crate::backend::utils::misc::interrupts::InterruptState,
) -> Result<(), CatalogError> {
    check_for_interrupts(interrupts).map_err(CatalogError::Interrupted)
}

const BT_DESC_FLAG: i16 = 0x0001;
const TOAST_INDEX_TARGET: usize = MAX_HEAP_TUPLE_SIZE / 16;

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

fn encode_index_value(
    sql_type: crate::backend::parser::SqlType,
    value: &Value,
) -> Result<Vec<u8>, CatalogError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int64(v)
            if matches!(
                sql_type.kind,
                crate::backend::parser::SqlTypeKind::Oid
                    | crate::backend::parser::SqlTypeKind::RegConfig
                    | crate::backend::parser::SqlTypeKind::RegDictionary
            ) =>
        {
            let oid = u32::try_from(*v)
                .map_err(|_| CatalogError::Io(format!("oid index key out of range: {v}")))?;
            Ok(oid.to_le_bytes().to_vec())
        }
        Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Money(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Bool(v) => Ok(vec![u8::from(*v)]),
        Value::Text(v) => Ok(v.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(value
            .as_text()
            .ok_or(CatalogError::Corrupt("text ref must materialize"))?
            .to_owned()
            .into_bytes()),
        Value::Xml(v) => Ok(v.as_bytes().to_vec()),
        Value::Numeric(v) => Ok(v.render().into_bytes()),
        Value::Bytea(v) => Ok(v.clone()),
        Value::Inet(v) => Ok(v.render_inet().into_bytes()),
        Value::Cidr(v) => Ok(v.render_cidr().into_bytes()),
        Value::Bit(v) => {
            let mut bytes = Vec::with_capacity(4 + v.bytes.len());
            bytes.extend_from_slice(&(v.bit_len as u32).to_le_bytes());
            bytes.extend_from_slice(&v.bytes);
            Ok(bytes)
        }
        Value::Float64(v)
            if matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::Float4) =>
        {
            Ok((*v as f32).to_le_bytes().to_vec())
        }
        Value::Float64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Json(v) => Ok(v.as_bytes().to_vec()),
        Value::Jsonb(v) => Ok(v.clone()),
        Value::JsonPath(v) => Ok(v.as_bytes().to_vec()),
        Value::TsVector(v) => Ok(crate::backend::executor::render_tsvector_text(v).into_bytes()),
        Value::TsQuery(v) => Ok(crate::backend::executor::render_tsquery_text(v).into_bytes()),
        Value::InternalChar(v) => Ok(vec![*v]),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => Ok(render_datetime_value_text(value)
            .expect("datetime values must render")
            .into_bytes()),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_)
        | Value::Range(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
        Value::Multirange(v) => crate::backend::executor::encode_multirange_bytes(v)
            .map_err(|err| CatalogError::Io(format!("{err:?}"))),
        Value::Array(_) | Value::PgArray(_)
            if sql_type.kind == crate::backend::parser::SqlTypeKind::AnyArray =>
        {
            let array = value
                .as_array_value()
                .ok_or_else(|| CatalogError::Io("array index key must materialize".into()))?;
            encode_anyarray_bytes(&array).map_err(|err| CatalogError::Io(format!("{err:?}")))
        }
        Value::Array(_) | Value::PgArray(_) if sql_type.is_array => {
            let array = value
                .as_array_value()
                .ok_or_else(|| CatalogError::Io("array index key must materialize".into()))?;
            encode_array_bytes(sql_type.element_type(), &array)
                .map_err(|err| CatalogError::Io(format!("{err:?}")))
        }
        Value::Array(_) | Value::PgArray(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
        Value::Record(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
    }
}

fn decode_index_value(column: &ColumnDesc, bytes: &[u8]) -> Result<Value, CatalogError> {
    decode_value(column, Some(bytes)).map_err(|err| CatalogError::Io(format!("{err:?}")))
}

fn maybe_compress_index_value(
    column: &ColumnDesc,
    bytes: Vec<u8>,
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    if column.storage.attlen != -1
        || bytes.len() <= TOAST_INDEX_TARGET
        || !matches!(
            column.storage.attstorage,
            AttributeStorage::Extended | AttributeStorage::Main
        )
    {
        return Ok(bytes);
    }

    match compress_inline_datum(
        &bytes,
        column.storage.attcompression,
        default_toast_compression,
    ) {
        Ok(Some(compressed)) => Ok(compressed.encoded),
        Ok(None) => Ok(bytes),
        Err(err) => Err(CatalogError::Io(format!(
            "btree index key compression failed: {err:?}"
        ))),
    }
}

pub(crate) fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for (column, value) in desc.columns.iter().zip(values.iter()) {
        match value {
            Value::Null => {
                payload.push(1);
                payload.extend_from_slice(&0u32.to_le_bytes());
            }
            _ => {
                payload.push(0);
                let bytes = maybe_compress_index_value(
                    column,
                    encode_index_value(column.sql_type, &value)?,
                    default_toast_compression,
                )?;
                payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                payload.extend_from_slice(&bytes);
            }
        }
    }
    Ok(payload)
}

pub(crate) fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
) -> Result<Vec<Value>, CatalogError> {
    if payload.len() < 2 {
        return Err(CatalogError::Corrupt("index tuple payload too short"));
    }
    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut values = Vec::with_capacity(count);
    for column in desc.columns.iter().take(count) {
        if offset + 5 > payload.len() {
            return Err(CatalogError::Corrupt("index tuple payload truncated"));
        }
        let is_null = payload[offset] != 0;
        offset += 1;
        let len = u32::from_le_bytes(
            payload[offset..offset + 4]
                .try_into()
                .map_err(|_| CatalogError::Corrupt("index tuple payload length"))?,
        ) as usize;
        offset += 4;
        if is_null {
            values.push(Value::Null);
            continue;
        }
        if offset + len > payload.len() {
            return Err(CatalogError::Corrupt("index tuple payload overflow"));
        }
        values.push(decode_index_value(column, &payload[offset..offset + len])?);
        offset += len;
    }
    Ok(values)
}

fn compare_key_arrays(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let ord = compare_bt_values(left, right);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

fn tuple_key_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    decode_key_payload(desc, &tuple.payload)
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
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let mut ord = compare_bt_keyspace(
            &[values
                .get(attno)
                .cloned()
                .ok_or(CatalogError::Corrupt("scan key attno out of range"))?],
            &tuple.t_tid,
            std::slice::from_ref(&key.argument),
            &tuple.t_tid,
        );
        if indoption
            .get(attno)
            .is_some_and(|opt| opt & BT_DESC_FLAG != 0)
        {
            ord = ord.reverse();
        }
        let ok = match key.strategy {
            1 => ord == Ordering::Less,
            2 => matches!(ord, Ordering::Less | Ordering::Equal),
            3 => ord == Ordering::Equal,
            4 => matches!(ord, Ordering::Greater | Ordering::Equal),
            5 => ord == Ordering::Greater,
            _ => false,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
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
) -> Result<Vec<Value>, CatalogError> {
    let page = read_page(pool, rel, block)?;
    let items = bt_page_data_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let tuple = items
        .first()
        .ok_or(CatalogError::Corrupt("btree page unexpectedly empty"))?;
    tuple_key_values(desc, tuple)
}

fn page_first_key_values(
    desc: &RelationDesc,
    page: &Page,
) -> Result<Option<Vec<Value>>, CatalogError> {
    let items = bt_page_data_items(page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let Some(tuple) = items.first() else {
        return Ok(None);
    };
    tuple_key_values(desc, tuple).map(Some)
}

fn ensure_empty_btree(
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
        let value = row_values
            .get(idx)
            .cloned()
            .ok_or(CatalogError::Corrupt("index key attnum out of range"))?;
        keys.push(value);
    }
    Ok(keys)
}

fn keys_contain_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

fn build_leaf_pages(
    ctx: &IndexBuildContext,
    tuples: Vec<BtSortTuple>,
    next_block: &mut u32,
) -> Result<Vec<BuiltPageRef>, CatalogError> {
    let pages = group_sorted_tuples_into_pages(
        tuples.into_iter().map(|tuple| tuple.tuple).collect(),
        BTP_LEAF,
        0,
        BTREE_DEFAULT_FILLFACTOR,
    )?;

    let mut built = Vec::with_capacity(pages.len());
    for (idx, items) in pages.into_iter().enumerate() {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        let block = *next_block;
        *next_block += 1;
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut page, BTP_LEAF, 0)
            .map_err(|err| CatalogError::Io(format!("btree leaf init failed: {err:?}")))?;
        for tuple in &items {
            bt_page_append_tuple(&mut page, tuple)
                .map_err(|_| CatalogError::Io("index tuple too large for leaf page".into()))?;
        }
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
            lower_bound: tuple_key_values(
                &ctx.index_desc,
                items
                    .first()
                    .ok_or(CatalogError::Corrupt("empty leaf build page"))?,
            )?,
        });
    }

    for built_page in built.iter().skip(1) {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
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
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        let block = *next_block;
        *next_block += 1;
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut page, 0, level)
            .map_err(|err| CatalogError::Io(format!("btree internal init failed: {err:?}")))?;
        for tuple in &items {
            bt_page_append_tuple(&mut page, tuple)
                .map_err(|_| CatalogError::Io("index tuple too large for internal page".into()))?;
        }
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
            lower_bound: tuple_key_values(
                &ctx.index_desc,
                items
                    .first()
                    .ok_or(CatalogError::Corrupt("empty internal build page"))?,
            )?,
        });
    }

    for built_page in built.iter().skip(1) {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
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
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
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

fn check_unique_build(index_name: &str, tuples: &[BtSortTuple]) -> Result<(), CatalogError> {
    let mut last: Option<&[Value]> = None;
    for tuple in tuples {
        if keys_contain_null(&tuple.key_values) {
            last = None;
            continue;
        }
        if last.is_some_and(|prev| compare_key_arrays(prev, &tuple.key_values) == Ordering::Equal) {
            return Err(CatalogError::UniqueViolation(index_name.to_string()));
        }
        last = Some(&tuple.key_values);
    }
    Ok(())
}

fn btbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        ctx.heap_relation,
        ctx.snapshot.clone(),
    )
    .map_err(|err| CatalogError::Io(format!("heap scan begin failed: {err:?}")))?;
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut spool = crate::backend::access::nbtree::nbtsort::BtSpool::default();
    let mut result = IndexBuildResult::default();
    let mut approx_bytes = 0usize;

    loop {
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)) =
            next.map_err(|err| CatalogError::Io(format!("heap scan failed: {err:?}")))?
        else {
            break;
        };
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)?;
        let Some(key_values) = key_projector.project(ctx, &row_values, tid)? else {
            result.heap_tuples += 1;
            continue;
        };
        let payload =
            encode_key_payload(&ctx.index_desc, &key_values, ctx.default_toast_compression)?;
        let tuple = IndexTupleData::new_raw(tid, false, false, false, payload);
        check_leaf_tuple_size(&ctx.index_name, &tuple)?;
        approx_bytes = approx_bytes
            .saturating_add(tuple.size())
            .saturating_add(key_values.len() * 16);
        if approx_bytes > ctx.maintenance_work_mem_kb.saturating_mul(1024) {
            return Err(CatalogError::Io(
                "CREATE INDEX requires external build spill, which is not supported yet".into(),
            ));
        }
        spool.push(BtSortTuple { tuple, key_values });
        result.heap_tuples += 1;
        result.index_tuples += 1;
    }

    let tuples = spool.finish();
    if ctx.index_meta.indisunique {
        check_unique_build(&ctx.index_name, &tuples)?;
    }
    build_btree_pages(ctx, tuples)?;
    Ok(result)
}

fn choose_child_slot(
    desc: &RelationDesc,
    items: &[IndexTupleData],
    target: &[Value],
    direction: ScanDirection,
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
        let key = tuple_key_values(desc, tuple)?;
        if compare_key_arrays(&key, target) != Ordering::Greater {
            choice = idx;
        } else {
            break;
        }
    }
    Ok(choice)
}

fn scan_positioning_prefix(keys: &[ScanKeyData], direction: ScanDirection) -> Vec<Value> {
    let mut prefix = Vec::new();
    for key in preprocess_scan_keys(keys) {
        if key.strategy == 3 {
            prefix.push(key.argument);
            continue;
        }
        let use_bound = match direction {
            ScanDirection::Forward => matches!(key.strategy, 4 | 5),
            ScanDirection::Backward => matches!(key.strategy, 1 | 2),
        };
        if use_bound {
            prefix.push(key.argument);
        }
        break;
    }
    prefix
}

fn exact_equality_prefix(keys: &[ScanKeyData], indoption: &[i16]) -> Vec<Value> {
    let mut prefix = Vec::new();
    let mut expected_attno = 1i16;
    for key in preprocess_scan_keys(keys) {
        if key.strategy != 3 || key.attribute_number != expected_attno {
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
) -> Result<Ordering, CatalogError> {
    let values = tuple_key_values(desc, tuple)?;
    let prefix_len = target.len().min(values.len());
    Ok(compare_key_arrays(&values[..prefix_len], target))
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
            Ok(tuple_prefix_cmp(&scan.index_desc, last, &target)? != Ordering::Less)
        }
        ScanDirection::Backward => {
            let first = items
                .first()
                .expect("items is not empty when checking equality scan bounds");
            Ok(tuple_prefix_cmp(&scan.index_desc, first, &target)? != Ordering::Greater)
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
        let slot = choose_child_slot(desc, &items, target, direction)?;
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
        return tuple_key_values(&ctx.index_desc, &high_key).map(Some);
    }
    if opaque.btpo_next == P_NONE {
        return Ok(None);
    }
    let next_page = read_page(&ctx.pool, ctx.index_relation, opaque.btpo_next)?;
    let next_items = bt_page_data_items(&next_page)
        .map_err(|err| CatalogError::Io(format!("btree next-page parse failed: {err:?}")))?;
    let Some(first_tuple) = next_items.first() else {
        return Ok(None);
    };
    tuple_key_values(&ctx.index_desc, first_tuple).map(Some)
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
        tuple_key_values(&ctx.index_desc, &high_key)?
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
    Ok(compare_key_arrays(key_values, &upper_bound) != Ordering::Greater)
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
            let slot =
                choose_child_slot(&ctx.index_desc, &items, key_values, ScanDirection::Forward)?;
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
            let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)? else {
                return Ok(InsertSearchPath {
                    leaf_block: block,
                    parent_stack,
                });
            };
            if compare_key_arrays(key_values, &upper_bound) != Ordering::Greater {
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
            if let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)?
                && compare_key_arrays(key_values, &upper_bound) == Ordering::Greater
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
                let current_first_key = page_first_key_values(&ctx.index_desc, &page)?;
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
            let slot =
                choose_child_slot(&ctx.index_desc, &items, key_values, ScanDirection::Forward)?;
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
        let slot = choose_child_slot(&ctx.index_desc, &items, key_values, ScanDirection::Forward)?;
        parent_stack.push(InsertStackEntry {
            block,
            offset: slot,
        });
        return Ok(Some(parent_stack));
    }
}

fn leaf_has_match(scan: &IndexScanDesc, block: u32) -> Result<bool, CatalogError> {
    let (items, _) = read_page_items(&scan.pool, scan.index_relation, block)?;
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
    let inherited_high_key = if is_leaf {
        if let Some(high_key) = existing_high_key {
            Some(high_key)
        } else if old_opaque.btpo_next != P_NONE {
            let next_page = read_page(&ctx.pool, ctx.index_relation, old_opaque.btpo_next)?;
            bt_page_data_items(&next_page)
                .map_err(|err| CatalogError::Io(format!("btree next-page parse failed: {err:?}")))?
                .into_iter()
                .next()
        } else {
            None
        }
    } else {
        None
    };

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
    right_opaque.btpo_next = old_opaque.btpo_next;
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
    if old_opaque.btpo_next != P_NONE {
        let mut next_page = read_page(&ctx.pool, ctx.index_relation, old_opaque.btpo_next)?;
        let mut next_opaque = bt_page_get_opaque(&next_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        next_opaque.btpo_prev = new_block;
        bt_page_set_opaque(&mut next_page, next_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            old_opaque.btpo_next,
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
        right_lower_bound: tuple_key_values(
            &ctx.index_desc,
            right_items
                .first()
                .ok_or(CatalogError::Corrupt("right split page empty"))?,
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
    let insert_at = if is_leaf {
        items.partition_point(|item| {
            let existing = tuple_key_values(&ctx.index_desc, item).unwrap_or_default();
            compare_bt_keyspace(&existing, &item.t_tid, key_values, &new_tuple.t_tid)
                != Ordering::Greater
        })
    } else {
        items.partition_point(|item| {
            let existing = tuple_key_values(&ctx.index_desc, item).unwrap_or_default();
            compare_key_arrays(&existing, key_values) != Ordering::Greater
        })
    };
    items.insert(insert_at, new_tuple);

    let mut rebuilt = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(
        &mut rebuilt,
        if is_leaf { BTP_LEAF } else { 0 },
        old_opaque.btpo_level,
    )
    .map_err(|err| CatalogError::Io(format!("btree page init failed: {err:?}")))?;
    let mut rebuilt_opaque = bt_page_get_opaque(&rebuilt)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    rebuilt_opaque.btpo_prev = old_opaque.btpo_prev;
    rebuilt_opaque.btpo_next = old_opaque.btpo_next;
    let append_result = if let Some(high_key) = existing_high_key.as_ref() {
        bt_page_set_high_key(&mut rebuilt, high_key, items.clone(), rebuilt_opaque)
    } else {
        if let Err(err) = bt_page_set_opaque(&mut rebuilt, rebuilt_opaque) {
            Err(err)
        } else {
            let mut result = Ok(());
            for tuple in &items {
                if let Err(err) = bt_page_append_tuple(&mut rebuilt, tuple) {
                    result = Err(err);
                    break;
                }
            }
            result
        }
    };
    if append_result.is_err() {
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
                data: &[],
            }],
            &[],
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
    let left_lower_bound =
        page_lower_bound(&ctx.index_desc, &ctx.pool, ctx.index_relation, left_block)?;
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
    let mut page = *locked_page;
    let mut opaque = locked_opaque;

    loop {
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        for tuple in items {
            let tuple_keys = tuple_key_values(&ctx.index_desc, &tuple)?;
            match compare_key_arrays(&tuple_keys, key_values) {
                Ordering::Less => continue,
                Ordering::Greater => return Ok(LockedUniqueCheckResult::Clear),
                Ordering::Equal => match classify_unique_candidate(ctx, tuple.t_tid)? {
                    UniqueCandidateResult::NoConflict => {}
                    UniqueCandidateResult::Conflict(_) => {
                        return Err(CatalogError::UniqueViolation(ctx.index_name.clone()));
                    }
                    UniqueCandidateResult::WaitFor(xid) => {
                        return Ok(LockedUniqueCheckResult::WaitFor(xid));
                    }
                },
            }
        }

        let Some(upper_bound) = leaf_upper_bound(ctx, &page, opaque)? else {
            return Ok(LockedUniqueCheckResult::Clear);
        };
        if compare_key_arrays(key_values, &upper_bound) != Ordering::Equal
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

    let key_values = key_values_from_heap_row(
        &ctx.heap_desc,
        &ctx.index_desc,
        &ctx.index_meta.indkey,
        &ctx.values,
    )?;
    let payload = encode_key_payload(&ctx.index_desc, &key_values, ctx.default_toast_compression)?;
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
                        check_catalog_interrupts(ctx.interrupts.as_ref())?;
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
        check_catalog_interrupts(ctx.interrupts.as_ref())?;
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
        amgetbitmap: None,
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
