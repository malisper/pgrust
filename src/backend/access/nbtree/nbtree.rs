use std::cmp::Ordering;

use crate::backend::access::heap::heapam::{
    heap_scan_begin_visible, heap_scan_next_visible,
};
use crate::backend::access::nbtree::nbtcompare::compare_bt_keyspace;
use crate::backend::access::nbtree::nbtpreprocesskeys::preprocess_scan_keys;
use crate::backend::access::nbtree::nbtsearch::first_greater_or_equal_by;
use crate::backend::access::nbtree::nbtutils::BtSortTuple;
use crate::backend::catalog::CatalogError;
use crate::backend::executor::value_io::decode_value;
use crate::backend::storage::page::bufpage::page_header;
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildResult, IndexInsertContext,
};
use crate::include::access::itup::IndexTupleData;
use crate::include::access::nbtree::{
    BTP_LEAF, BTP_ROOT, BTREE_DEFAULT_FILLFACTOR, BTREE_METAPAGE, bt_init_meta_page,
    bt_page_append_tuple, bt_page_get_opaque, bt_page_init, bt_page_items, bt_page_replace_items,
    bt_page_set_opaque,
};
use crate::include::access::relscan::{
    BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::RelationDesc;

const BT_DESC_FLAG: i16 = 0x0001;

fn encode_index_value(
    sql_type: crate::backend::parser::SqlType,
    value: &Value,
) -> Result<Vec<u8>, CatalogError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
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
        Value::Float64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Json(v) => Ok(v.as_bytes().to_vec()),
        Value::Jsonb(v) => Ok(v.clone()),
        Value::JsonPath(v) => Ok(v.as_bytes().to_vec()),
        Value::InternalChar(v) => Ok(vec![*v]),
        Value::Array(_) => Err(CatalogError::Io(format!(
            "unsupported index key type {:?}",
            sql_type.kind
        ))),
    }
}

fn decode_index_value(
    column: &crate::include::nodes::plannodes::ColumnDesc,
    bytes: &[u8],
) -> Result<Value, CatalogError> {
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
        if indoption.get(attno).is_some_and(|opt| opt & BT_DESC_FLAG != 0) {
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

fn append_page_or_finish(
    pages: &mut Vec<Vec<IndexTupleData>>,
    current: &mut Vec<IndexTupleData>,
    candidate: IndexTupleData,
) -> Result<(), CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut page, BTP_LEAF, 0)
        .map_err(|err| CatalogError::Io(format!("btree page init failed: {err:?}")))?;
    for tuple in current.iter() {
        bt_page_append_tuple(&mut page, tuple)
            .map_err(|_| CatalogError::Io("index build tuple too large for leaf page".into()))?;
    }
    let before = page_header(&page)
        .map_err(|err| CatalogError::Io(format!("btree page header failed: {err:?}")))?
        .free_space();
    if bt_page_append_tuple(&mut page, &candidate).is_ok() {
        let after = page_header(&page)
            .map_err(|err| CatalogError::Io(format!("btree page header failed: {err:?}")))?
            .free_space();
        let reserved = crate::backend::storage::smgr::BLCKSZ
            * (100usize.saturating_sub(BTREE_DEFAULT_FILLFACTOR as usize))
            / 100;
        if after > reserved || current.is_empty() {
            current.push(candidate);
            return Ok(());
        }
    }
    let _ = before;
    pages.push(std::mem::take(current));
    current.push(candidate);
    Ok(())
}

fn write_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
) -> Result<(), CatalogError> {
    pool.with_storage_mut(|storage| {
        storage
            .smgr
            .write_block(rel, ForkNumber::Main, block, page, true)
    })
    .map_err(|err| CatalogError::Io(err.to_string()))
}

fn extend_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
) -> Result<(), CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.extend(rel, ForkNumber::Main, block, page, true))
        .map_err(|err| CatalogError::Io(err.to_string()))
}

fn ensure_relation_exists(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    pool.with_storage_mut(|storage| {
        let _ = storage.smgr.open(rel);
        storage.smgr.create(rel, ForkNumber::Main, true)
    })
    .map_err(|err| CatalogError::Io(err.to_string()))
}

fn build_leaf_chain(
    ctx: &IndexBuildContext,
    tuples: Vec<BtSortTuple>,
) -> Result<IndexBuildResult, CatalogError> {
    ensure_relation_exists(&ctx.pool, ctx.index_relation)?;

    let mut pages: Vec<Vec<IndexTupleData>> = Vec::new();
    let mut current = Vec::new();
    for sort_tuple in tuples {
        append_page_or_finish(&mut pages, &mut current, sort_tuple.tuple)?;
    }
    if !current.is_empty() {
        pages.push(current);
    }

    let mut metapage = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_init_meta_page(&mut metapage, 1, 0, false)
        .map_err(|err| CatalogError::Io(format!("btree metapage init failed: {err:?}")))?;
    extend_page(&ctx.pool, ctx.index_relation, BTREE_METAPAGE, &metapage)?;

    if pages.is_empty() {
        let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut root, BTP_LEAF | BTP_ROOT, 0)
            .map_err(|err| CatalogError::Io(format!("btree root init failed: {err:?}")))?;
        extend_page(&ctx.pool, ctx.index_relation, 1, &root)?;
        return Ok(IndexBuildResult::default());
    }

    for (idx, items) in pages.iter().enumerate() {
        let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        let block = idx as u32 + 1;
        let mut flags = BTP_LEAF;
        if idx == 0 {
            flags |= BTP_ROOT;
        }
        bt_page_init(&mut page, flags, 0)
            .map_err(|err| CatalogError::Io(format!("btree leaf init failed: {err:?}")))?;
        for tuple in items {
            bt_page_append_tuple(&mut page, tuple)
                .map_err(|_| CatalogError::Io("index tuple too large for leaf page".into()))?;
        }
        let mut opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        opaque.btpo_prev = if idx == 0 { 0 } else { block - 1 };
        opaque.btpo_next = if idx + 1 == pages.len() { 0 } else { block + 1 };
        bt_page_set_opaque(&mut page, opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        extend_page(&ctx.pool, ctx.index_relation, block, &page)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: pages.iter().map(|page| page.len() as u64).sum(),
        index_tuples: pages.iter().map(|page| page.len() as u64).sum(),
    })
}

fn key_values_from_heap_row(
    heap_desc: &RelationDesc,
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
        let _ = heap_desc;
        keys.push(value);
    }
    Ok(keys)
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
    let mut spool = crate::backend::access::nbtree::nbtsort::BtSpool::default();
    let mut heap_tuples = 0u64;
    let mut approx_bytes = 0usize;

    loop {
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)) = next.map_err(|err| CatalogError::Io(format!("heap scan failed: {err:?}")))? else {
            break;
        };
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("heap deform failed: {err:?}")))?;
        let mut row_values = Vec::with_capacity(ctx.heap_desc.columns.len());
        for (column, datum) in ctx.heap_desc.columns.iter().zip(datums.into_iter()) {
            row_values.push(
                decode_value(column, datum)
                    .map_err(|err| CatalogError::Io(format!("heap decode failed: {err:?}")))?,
            );
        }
        let key_values =
            key_values_from_heap_row(&ctx.heap_desc, &ctx.index_desc, &ctx.index_meta.indkey, &row_values)?;
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

    let mut result = build_leaf_chain(ctx, spool.finish())?;
    result.heap_tuples = heap_tuples;
    result.index_tuples = heap_tuples;
    Ok(result)
}

fn read_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    pool.with_storage_mut(|storage| {
        storage
            .smgr
            .read_block(rel, ForkNumber::Main, block, &mut page)
    })
    .map_err(|err| CatalogError::Io(err.to_string()))?;
    Ok(page)
}

fn relation_nblocks(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(err.to_string()))
}

fn load_leaf_items(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    let Some(block) = scan
        .opaque
        .as_btree_mut()
        .and_then(|opaque| opaque.current_block)
    else {
        return Ok(false);
    };
    let page = read_page(&scan.pool, scan.index_relation, block)?;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let items = bt_page_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let filtered = items
        .into_iter()
        .filter(|tuple| {
            tuple_matches_scan_keys(&scan.index_desc, tuple, &scan.key_data, &scan.indoption)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
        state.current_items = filtered;
        state.next_offset = match scan.direction {
            ScanDirection::Forward => 0,
            ScanDirection::Backward => state.current_items.len().saturating_sub(1),
        };
        state.current_block = match scan.direction {
            ScanDirection::Forward => {
                if state.current_items.is_empty() {
                    (opaque.btpo_next != 0).then_some(opaque.btpo_next)
                } else {
                    Some(block)
                }
            }
            ScanDirection::Backward => {
                if state.current_items.is_empty() {
                    (opaque.btpo_prev != 0).then_some(opaque.btpo_prev)
                } else {
                    Some(block)
                }
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
            IndexScanOpaque::None => None,
        }
    }
}

fn btbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    let mut scan = crate::backend::access::index::genam::index_beginscan_stub(ctx)?;
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
        state.current_block = match ctx.direction {
            ScanDirection::Forward => (nblocks > 1).then_some(1),
            ScanDirection::Backward => (nblocks > 1).then_some(nblocks - 1),
        };
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
    let nblocks = relation_nblocks(&scan.pool, scan.index_relation)?;
    if let IndexScanOpaque::Btree(state) = &mut scan.opaque {
        state.current_block = match direction {
            ScanDirection::Forward => (nblocks > 1).then_some(1),
            ScanDirection::Backward => (nblocks > 1).then_some(nblocks - 1),
        };
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
                        let page = read_page(&scan.pool, scan.index_relation, state.current_block.unwrap())?;
                        let opaque = bt_page_get_opaque(&page)
                            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
                        state.current_items.clear();
                        state.current_block = (opaque.btpo_next != 0).then_some(opaque.btpo_next);
                        None
                    } else {
                        let idx = state.next_offset;
                        state.next_offset += 1;
                        Some(state.current_items[idx].clone())
                    }
                }
                ScanDirection::Backward => {
                    if state.current_items.is_empty() || state.next_offset >= state.current_items.len() {
                        let page = read_page(&scan.pool, scan.index_relation, state.current_block.unwrap())?;
                        let opaque = bt_page_get_opaque(&page)
                            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
                        state.current_items.clear();
                        state.current_block = (opaque.btpo_prev != 0).then_some(opaque.btpo_prev);
                        None
                    } else {
                        let idx = state.next_offset;
                        let tuple = state.current_items[idx].clone();
                        if idx == 0 {
                            let page =
                                read_page(&scan.pool, scan.index_relation, state.current_block.unwrap())?;
                            let opaque = bt_page_get_opaque(&page).map_err(|err| {
                                CatalogError::Io(format!("btree opaque read failed: {err:?}"))
                            })?;
                            state.current_items.clear();
                            state.current_block = (opaque.btpo_prev != 0).then_some(opaque.btpo_prev);
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
            if let IndexScanOpaque::Btree(state) = &scan.opaque {
                if state.current_block.is_none() {
                    return Ok(false);
                }
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

fn btinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    ensure_relation_exists(&ctx.pool, ctx.index_relation)?;
    let key_values =
        key_values_from_heap_row(&ctx.heap_desc, &ctx.index_desc, &ctx.index_meta.indkey, &ctx.values)?;
    let payload = encode_key_payload(&ctx.index_desc, &key_values)?;
    let new_tuple = IndexTupleData::new_raw(ctx.heap_tid, false, false, false, payload);
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    if nblocks <= 1 {
        ensure_relation_exists(&ctx.pool, ctx.index_relation)?;
        let mut metapage = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_init_meta_page(&mut metapage, 1, 0, false)
            .map_err(|err| CatalogError::Io(format!("btree metapage init failed: {err:?}")))?;
        extend_page(&ctx.pool, ctx.index_relation, BTREE_METAPAGE, &metapage)?;
        let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
        bt_page_init(&mut root, BTP_LEAF | BTP_ROOT, 0)
            .map_err(|err| CatalogError::Io(format!("btree root init failed: {err:?}")))?;
        bt_page_append_tuple(&mut root, &new_tuple)
            .map_err(|_| CatalogError::Io("index tuple too large for root page".into()))?;
        extend_page(&ctx.pool, ctx.index_relation, 1, &root)?;
        return Ok(true);
    }

    let mut target_block = 1u32;
    for block in 1..nblocks {
        let page = read_page(&ctx.pool, ctx.index_relation, block)?;
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        let items = bt_page_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        let is_last = opaque.btpo_next == 0;
        let should_use = items
            .last()
            .map(|tuple| {
                let existing = decode_key_payload(&ctx.index_desc, &tuple.payload).unwrap_or_default();
                compare_bt_keyspace(&key_values, &ctx.heap_tid, &existing, &tuple.t_tid)
                    != Ordering::Greater
            })
            .unwrap_or(true);
        if should_use || is_last {
            target_block = block;
            break;
        }
    }

    let mut page = read_page(&ctx.pool, ctx.index_relation, target_block)?;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let mut items = bt_page_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let insert_at = first_greater_or_equal_by(&items, |item| {
        let existing = decode_key_payload(&ctx.index_desc, &item.payload).unwrap_or_default();
        compare_bt_keyspace(&existing, &item.t_tid, &key_values, &ctx.heap_tid)
    });
    items.insert(insert_at, new_tuple);

    if bt_page_replace_items(&mut page, &items, opaque).is_ok() {
        write_page(&ctx.pool, ctx.index_relation, target_block, &page)?;
        return Ok(true);
    }

    let split = crate::backend::access::nbtree::nbtsplitloc::choose_split_index(&items, None);
    let right_items = items.split_off(split);
    let left_items = items;
    let new_block = nblocks;

    let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut left_page, opaque.btpo_flags, 0)
        .map_err(|err| CatalogError::Io(format!("btree left split init failed: {err:?}")))?;
    let mut left_opaque = bt_page_get_opaque(&left_page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    left_opaque.btpo_prev = opaque.btpo_prev;
    left_opaque.btpo_next = new_block;
    bt_page_set_opaque(&mut left_page, left_opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    for tuple in &left_items {
        bt_page_append_tuple(&mut left_page, tuple)
            .map_err(|_| CatalogError::Io("index split left page overflow".into()))?;
    }

    let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_page_init(&mut right_page, BTP_LEAF, 0)
        .map_err(|err| CatalogError::Io(format!("btree right split init failed: {err:?}")))?;
    let mut right_opaque = bt_page_get_opaque(&right_page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    right_opaque.btpo_prev = target_block;
    right_opaque.btpo_next = opaque.btpo_next;
    bt_page_set_opaque(&mut right_page, right_opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    for tuple in &right_items {
        bt_page_append_tuple(&mut right_page, tuple)
            .map_err(|_| CatalogError::Io("index split right page overflow".into()))?;
    }

    write_page(&ctx.pool, ctx.index_relation, target_block, &left_page)?;
    ctx.pool
        .with_storage_mut(|storage| {
            storage
                .smgr
                .extend(ctx.index_relation, ForkNumber::Main, new_block, &right_page, true)
        })
        .map_err(|err| CatalogError::Io(err.to_string()))?;
    if opaque.btpo_next != 0 {
        let mut next_page = read_page(&ctx.pool, ctx.index_relation, opaque.btpo_next)?;
        let mut next_opaque = bt_page_get_opaque(&next_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        next_opaque.btpo_prev = new_block;
        bt_page_set_opaque(&mut next_page, next_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        write_page(&ctx.pool, ctx.index_relation, opaque.btpo_next, &next_page)?;
    }
    Ok(true)
}

fn btbuildempty(index_relation: RelFileLocator) -> Result<(), CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    bt_init_meta_page(&mut page, 1, 0, false)
        .map_err(|err| CatalogError::Io(format!("btree metapage init failed: {err:?}")))?;
    let _ = index_relation;
    Ok(())
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
