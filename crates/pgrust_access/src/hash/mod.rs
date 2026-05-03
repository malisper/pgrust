use pgrust_nodes::datum::Value;
use pgrust_storage::{
    BLCKSZ, BufferPool, ForkNumber, PinnedBuffer, RelFileLocator, SmgrStorageBackend,
};

use crate::access::amapi::IndexBeginScanContext;
use crate::access::hash::{
    HASH_INVALID_BLOCK, HASH_METAPAGE, HashMetaPageData, HashPageError, hash_metapage_data,
    hash_opclass_for_first_key, hash_page_get_opaque, hash_page_items, hash_tuple_hash,
    hash_tuple_key_values,
};
use crate::access::relscan::{HashIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::access::scankey::ScanKeyData;
use crate::access::tidbitmap::TidBitmap;
use crate::index::genam::{index_beginscan_stub, index_endscan_stub, index_rescan_stub};
use crate::{AccessError, AccessResult, AccessScalarServices};

fn page_error(err: HashPageError) -> AccessError {
    AccessError::Scalar(format!("hash page error: {err:?}"))
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

fn read_meta(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: RelFileLocator,
) -> AccessResult<HashMetaPageData> {
    let page = read_page(pool, client_id, rel, HASH_METAPAGE)?;
    hash_metapage_data(&page).map_err(page_error)
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
