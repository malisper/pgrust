use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::backend::access::heap::heapam::{
    heap_fetch, heap_scan_begin_visible, heap_scan_next_visible,
};
use crate::backend::access::index::indexam;
use crate::backend::access::nbtree::nbtcompare::{compare_bt_keyspace, compare_bt_values};
use crate::backend::access::nbtree::nbtpreprocesskeys::preprocess_scan_keys;
use crate::backend::access::nbtree::nbtsplitloc::choose_split_index;
use crate::backend::access::nbtree::nbtutils::BtSortTuple;
use crate::backend::access::transam::xact::{
    INVALID_TRANSACTION_ID, TransactionId, TransactionStatus,
};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::render_datetime_value_text;
use crate::backend::executor::value_io::{decode_value, missing_column_value};
use crate::backend::storage::page::bufpage::page_header;
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexInsertContext, IndexUniqueCheck,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::access::nbtree::{
    BTP_LEAF, BTP_ROOT, BTREE_DEFAULT_FILLFACTOR, BTREE_METAPAGE, BTREE_NONLEAF_FILLFACTOR, P_NONE,
    bt_init_meta_page, bt_page_append_tuple, bt_page_get_meta, bt_page_get_opaque, bt_page_init,
    bt_page_items, bt_page_set_opaque,
};
use crate::include::access::relscan::{
    BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::{ColumnDesc, RelationDesc};
use crate::{BufferPool, ClientId, OwnedBufferPin, PinnedBuffer, SmgrStorageBackend};

type WriteLockMap = parking_lot::Mutex<HashMap<RelFileLocator, Arc<parking_lot::Mutex<()>>>>;

const BT_DESC_FLAG: i16 = 0x0001;

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
        Value::Bool(v) => Ok(vec![u8::from(*v)]),
        Value::Text(v) => Ok(v.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(value
            .as_text()
            .ok_or(CatalogError::Corrupt("text ref must materialize"))?
            .to_owned()
            .into_bytes()),
        Value::Numeric(v) => Ok(v.render().into_bytes()),
        Value::Bytea(v) => Ok(v.clone()),
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
        | Value::Circle(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
        Value::Array(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
        Value::PgArray(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
    }
}

fn decode_index_value(column: &ColumnDesc, bytes: &[u8]) -> Result<Value, CatalogError> {
    decode_value(column, Some(bytes)).map_err(|err| CatalogError::Io(format!("{err:?}")))
}

fn encode_key_payload(desc: &RelationDesc, values: &[Value]) -> Result<Vec<u8>, CatalogError> {
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
                let bytes = encode_index_value(column.sql_type, value)?;
                payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                payload.extend_from_slice(&bytes);
            }
        }
    }
    Ok(payload)
}

fn decode_key_payload(desc: &RelationDesc, payload: &[u8]) -> Result<Vec<Value>, CatalogError> {
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
) -> Result<IndexTupleData, CatalogError> {
    Ok(IndexTupleData::new_raw(
        ItemPointerData {
            block_number: child_block,
            offset_number: 0,
        },
        false,
        false,
        false,
        encode_key_payload(desc, key_values)?,
    ))
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

fn btree_write_locks() -> &'static WriteLockMap {
    static LOCKS: OnceLock<WriteLockMap> = OnceLock::new();
    LOCKS.get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
}

fn btree_relation_write_lock(rel: RelFileLocator) -> Arc<parking_lot::Mutex<()>> {
    let mut locks = btree_write_locks().lock();
    Arc::clone(
        locks
            .entry(rel)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(()))),
    )
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
) -> Result<(), CatalogError> {
    pool.ensure_block_exists(rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("btree extend failed: {err:?}")))?;
    let pin = pin_btree_block(pool, client_id, rel, block)?;
    let mut guard = pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    pool.write_btree_page_image_locked(pin.buffer_id(), xid, page, &mut guard)
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
    pool.with_storage_mut(|storage| storage.smgr.truncate(rel, ForkNumber::Main, 0))
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
    write_buffered_btree_page(pool, client_id, xid, rel, BTREE_METAPAGE, &metapage)
}

fn page_lower_bound(
    desc: &RelationDesc,
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<Vec<Value>, CatalogError> {
    let page = read_page(pool, rel, block)?;
    let items = bt_page_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let tuple = items
        .first()
        .ok_or(CatalogError::Corrupt("btree page unexpectedly empty"))?;
    tuple_key_values(desc, tuple)
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
    write_buffered_btree_page(pool, client_id, xid, rel, 1, &root)?;
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
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            block,
            &page,
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
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            block,
            &page,
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
    write_buffered_btree_page(pool, client_id, xid, rel, block, &page)
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
        write_buffered_btree_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            1,
            &root,
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
    let write_lock = btree_relation_write_lock(ctx.index_relation);
    let _guard = write_lock.lock();

    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        ctx.heap_relation,
        ctx.snapshot.clone(),
    )
    .map_err(|err| CatalogError::Io(format!("heap scan begin failed: {err:?}")))?;
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut spool = crate::backend::access::nbtree::nbtsort::BtSpool::default();
    let mut heap_tuples = 0u64;
    let mut approx_bytes = 0usize;

    loop {
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
        let mut row_values = Vec::with_capacity(ctx.heap_desc.columns.len());
        for (index, column) in ctx.heap_desc.columns.iter().enumerate() {
            row_values.push(if let Some(datum) = datums.get(index) {
                decode_value(column, *datum)
                    .map_err(|err| CatalogError::Io(format!("heap decode failed: {err:?}")))?
            } else {
                missing_column_value(column)
            });
        }
        let key_values = key_values_from_heap_row(
            &ctx.heap_desc,
            &ctx.index_desc,
            &ctx.index_meta.indkey,
            &row_values,
        )?;
        let payload = encode_key_payload(&ctx.index_desc, &key_values)?;
        let tuple = IndexTupleData::new_raw(tid, false, false, false, payload);
        approx_bytes = approx_bytes
            .saturating_add(tuple.size())
            .saturating_add(key_values.len() * 16);
        if approx_bytes > ctx.maintenance_work_mem_kb.saturating_mul(1024) {
            return Err(CatalogError::Io(
                "CREATE INDEX requires external build spill, which is not supported yet".into(),
            ));
        }
        spool.push(BtSortTuple { tuple, key_values });
        heap_tuples += 1;
    }

    let tuples = spool.finish();
    if ctx.index_meta.indisunique {
        check_unique_build(&ctx.index_name, &tuples)?;
    }
    let mut result = build_btree_pages(ctx, tuples)?;
    result.heap_tuples = heap_tuples;
    result.index_tuples = heap_tuples;
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
    let items = bt_page_items(&page)
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

fn find_leaf_for_insert(
    ctx: &IndexInsertContext,
    key_values: &[Value],
) -> Result<(Vec<u32>, u32), CatalogError> {
    let (ancestors, mut block) = find_leaf_with_ancestors(
        &ctx.pool,
        ctx.index_relation,
        &ctx.index_desc,
        key_values,
        ScanDirection::Forward,
    )?;
    loop {
        let (items, opaque) = read_page_items(&ctx.pool, ctx.index_relation, block)?;
        if opaque.btpo_next == P_NONE {
            break;
        }
        let next_lower = items
            .get(0)
            .map(|tuple| tuple_key_values(&ctx.index_desc, tuple))
            .transpose()?
            .unwrap_or_default();
        if compare_key_arrays(&next_lower, key_values) == Ordering::Greater {
            break;
        }
        block = opaque.btpo_next;
    }
    Ok((ancestors, block))
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
    let items = bt_page_items(&guard)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    drop(guard);
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
        state.current_block = match scan.direction {
            ScanDirection::Forward => state.page_next,
            ScanDirection::Backward => state.page_prev,
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

fn next_block_number(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    relation_nblocks(pool, rel)
}

fn write_split_pages(
    ctx: &IndexInsertContext,
    block: u32,
    left_items: &[IndexTupleData],
    right_items: &[IndexTupleData],
    old_opaque: crate::include::access::nbtree::BTPageOpaqueData,
    is_leaf: bool,
) -> Result<PageSplitResult, CatalogError> {
    let new_block = next_block_number(&ctx.pool, ctx.index_relation)?;
    let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    let level = old_opaque.btpo_level;
    let flags = if is_leaf { BTP_LEAF } else { 0 };

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

    for tuple in left_items {
        bt_page_append_tuple(&mut left_page, tuple)
            .map_err(|_| CatalogError::Io("index split left page overflow".into()))?;
    }
    for tuple in right_items {
        bt_page_append_tuple(&mut right_page, tuple)
            .map_err(|_| CatalogError::Io("index split right page overflow".into()))?;
    }

    write_buffered_btree_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        block,
        &left_page,
    )?;
    write_buffered_btree_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        new_block,
        &right_page,
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
        )?;
    }

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
    })
}

fn clear_incomplete_split(ctx: &IndexInsertContext, block: u32) -> Result<(), CatalogError> {
    let mut page = read_page(&ctx.pool, ctx.index_relation, block)?;
    let mut opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.btpo_flags & crate::include::access::nbtree::BTP_INCOMPLETE_SPLIT == 0 {
        return Ok(());
    }
    opaque.btpo_flags &= !crate::include::access::nbtree::BTP_INCOMPLETE_SPLIT;
    bt_page_set_opaque(&mut page, opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    write_buffered_btree_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        block,
        &page,
    )
}

fn insert_tuple_into_page(
    ctx: &IndexInsertContext,
    block: u32,
    new_tuple: IndexTupleData,
    key_values: &[Value],
    is_leaf: bool,
) -> Result<Option<PageSplitResult>, CatalogError> {
    let page = read_page(&ctx.pool, ctx.index_relation, block)?;
    let old_opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let mut items = bt_page_items(&page)
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
    bt_page_set_opaque(&mut rebuilt, rebuilt_opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    for tuple in &items {
        if bt_page_append_tuple(&mut rebuilt, tuple).is_err() {
            let split = choose_split_index(&items, None);
            let right_items = items.split_off(split);
            let left_items = items;
            return write_split_pages(ctx, block, &left_items, &right_items, old_opaque, is_leaf)
                .map(Some);
        }
    }
    write_buffered_btree_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        block,
        &rebuilt,
    )?;
    Ok(None)
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
    let root_block = next_block_number(&ctx.pool, ctx.index_relation)?;
    let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut root, BTP_ROOT, child_level + 1)
        .map_err(|err| CatalogError::Io(format!("btree new root init failed: {err:?}")))?;
    for tuple in [
        pivot_tuple(&ctx.index_desc, left_block, &left_lower_bound)?,
        pivot_tuple(&ctx.index_desc, right_block, right_lower_bound)?,
    ] {
        bt_page_append_tuple(&mut root, &tuple)
            .map_err(|_| CatalogError::Io("new btree root overflow".into()))?;
    }
    write_buffered_btree_page(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        root_block,
        &root,
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

fn bt_check_unique(ctx: &IndexInsertContext, key_values: &[Value]) -> Result<(), CatalogError> {
    if !matches!(ctx.unique_check, IndexUniqueCheck::Yes) || keys_contain_null(key_values) {
        return Ok(());
    }
    loop {
        let begin = IndexBeginScanContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            snapshot: ctx.snapshot.clone(),
            heap_relation: ctx.heap_relation,
            index_relation: ctx.index_relation,
            index_desc: ctx.index_desc.clone(),
            index_meta: ctx.index_meta.clone(),
            key_data: key_values
                .iter()
                .enumerate()
                .map(|(idx, value)| ScanKeyData {
                    attribute_number: idx as i16 + 1,
                    strategy: 3,
                    argument: value.clone(),
                })
                .collect(),
            direction: ScanDirection::Forward,
        };
        let mut scan = indexam::index_beginscan(&begin, ctx.index_meta.am_oid)?;
        let mut wait_for_xid = None;
        while indexam::index_getnext(&mut scan, ctx.index_meta.am_oid)? {
            let tid = scan
                .xs_heaptid
                .ok_or(CatalogError::Corrupt("index scan tuple missing heap tid"))?;
            match classify_unique_candidate(ctx, tid)? {
                UniqueCandidateResult::NoConflict => {}
                UniqueCandidateResult::Conflict => {
                    let _ = indexam::index_endscan(scan, ctx.index_meta.am_oid);
                    return Err(CatalogError::UniqueViolation(ctx.index_name.clone()));
                }
                UniqueCandidateResult::WaitFor(xid) => {
                    wait_for_xid = Some(xid);
                    break;
                }
            }
        }
        indexam::index_endscan(scan, ctx.index_meta.am_oid)?;
        let Some(xid) = wait_for_xid else {
            return Ok(());
        };
        let waiter = ctx.txn_waiter.as_ref().ok_or_else(|| {
            CatalogError::Io("btree unique check missing transaction waiter".into())
        })?;
        if !waiter.wait_for(&ctx.txns, xid) {
            return Err(CatalogError::Io(format!(
                "btree unique check timed out waiting for transaction {xid}"
            )));
        }
    }
}

enum UniqueCandidateResult {
    NoConflict,
    Conflict,
    WaitFor(TransactionId),
}

fn classify_unique_candidate(
    ctx: &IndexInsertContext,
    tid: ItemPointerData,
) -> Result<UniqueCandidateResult, CatalogError> {
    let tuple = heap_fetch(&ctx.pool, ctx.client_id, ctx.heap_relation, tid)
        .map_err(|err| CatalogError::Io(format!("heap unique probe failed: {err:?}")))?;
    let txns = ctx.txns.read();
    let xmin = tuple.header.xmin;
    let xmax = tuple.header.xmax;

    if xmin == INVALID_TRANSACTION_ID {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    if xmin != ctx.snapshot.current_xid {
        match txns.status(xmin) {
            Some(TransactionStatus::Committed) => {}
            Some(TransactionStatus::Aborted) => return Ok(UniqueCandidateResult::NoConflict),
            Some(TransactionStatus::InProgress) | None => {
                return Ok(UniqueCandidateResult::WaitFor(xmin));
            }
        }
    }

    if xmax == INVALID_TRANSACTION_ID {
        return Ok(UniqueCandidateResult::Conflict);
    }
    if xmax == ctx.snapshot.current_xid {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    match txns.status(xmax) {
        Some(TransactionStatus::Committed) => Ok(UniqueCandidateResult::NoConflict),
        Some(TransactionStatus::Aborted) => Ok(UniqueCandidateResult::Conflict),
        Some(TransactionStatus::InProgress) | None => Ok(UniqueCandidateResult::WaitFor(xmax)),
    }
}

fn btinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    let write_lock = btree_relation_write_lock(ctx.index_relation);
    let _guard = write_lock.lock();

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
    bt_check_unique(ctx, &key_values)?;

    let payload = encode_key_payload(&ctx.index_desc, &key_values)?;
    let new_tuple = IndexTupleData::new_raw(ctx.heap_tid, false, false, false, payload);
    let (mut ancestors, leaf_block) = find_leaf_for_insert(ctx, &key_values)?;

    let mut split = insert_tuple_into_page(ctx, leaf_block, new_tuple, &key_values, true)?;
    while let Some(result) = split {
        let right_pivot = pivot_tuple(
            &ctx.index_desc,
            result.right_block,
            &result.right_lower_bound,
        )?;
        let Some(parent_block) = ancestors.pop() else {
            create_new_root(
                ctx,
                result.left_block,
                result.right_block,
                result.level,
                &result.right_lower_bound,
            )?;
            clear_incomplete_split(ctx, result.left_block)?;
            return Ok(true);
        };
        split = insert_tuple_into_page(
            ctx,
            parent_block,
            right_pivot,
            &result.right_lower_bound,
            false,
        )?;
        clear_incomplete_split(ctx, result.left_block)?;
    }

    Ok(true)
}

fn btbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    let write_lock = btree_relation_write_lock(ctx.index_relation);
    let _guard = write_lock.lock();
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
        ambuild: Some(btbuild),
        ambuildempty: Some(btbuildempty),
        aminsert: Some(btinsert),
        ambeginscan: Some(btbeginscan),
        amrescan: Some(btrescan),
        amgettuple: Some(btgettuple),
        amendscan: Some(btendscan),
    }
}
