use parking_lot::RwLock;

use crate::access::heap::mvcc::{
    CommandId, MvccError, Snapshot, TransactionId, TransactionManager, TransactionStatus,
};
use crate::access::heap::tuple::{
    HeapTuple, ItemPointerData, TupleError, heap_page_add_tuple, heap_page_get_tuple,
    heap_page_init, heap_page_replace_tuple,
};
use crate::database::TransactionWaiter;
use crate::storage::page::{ItemIdFlags, PageError, page_get_item, page_get_item_id, page_get_max_offset_number};
use crate::storage::smgr::{ForkNumber, RelFileLocator, SmgrError, StorageManager};
use crate::{BufferPool, ClientId, Error, RequestPageResult, SmgrStorageBackend};

#[derive(Debug)]
pub enum HeapError {
    Buffer(Error),
    Tuple(TupleError),
    Storage(SmgrError),
    Mvcc(MvccError),
    NoBufferAvailable,
    TupleNotVisible(ItemPointerData),
    TupleAlreadyModified(ItemPointerData),
    TupleUpdated(ItemPointerData, ItemPointerData),
}

/// Result of a heap modification that encountered a concurrent modification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeapModifyResult {
    Ok,
    Deleted,
    Updated { new_ctid: ItemPointerData },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapScan {
    rel: RelFileLocator,
    nblocks: u32,
    current_block: u32,
    current_offset: u16,
}

impl From<Error> for HeapError {
    fn from(value: Error) -> Self {
        Self::Buffer(value)
    }
}

impl From<TupleError> for HeapError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

impl From<SmgrError> for HeapError {
    fn from(value: SmgrError) -> Self {
        Self::Storage(value)
    }
}

impl From<MvccError> for HeapError {
    fn from(value: MvccError) -> Self {
        Self::Mvcc(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleHeapScan {
    scan: HeapScan,
    snapshot: Snapshot,
}

pub fn heap_scan_begin(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<HeapScan, HeapError> {
    let nblocks = pool.with_storage_mut(|s| s.smgr.nblocks(rel, ForkNumber::Main))?;
    Ok(HeapScan {
        rel,
        nblocks,
        current_block: 0,
        current_offset: 1,
    })
}

pub fn heap_scan_next(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    scan: &mut HeapScan,
) -> Result<Option<(ItemPointerData, HeapTuple)>, HeapError> {
    while scan.current_block < scan.nblocks {
        let block = scan.current_block;
        let buffer_id = pin_existing_block(pool, client_id, scan.rel, block)?;
        let page = {
            let _content_lock = pool.lock_buffer_shared(buffer_id)?;
            pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?
        };
        let max_offset = page_get_max_offset_number(&page).map_err(TupleError::from)?;

        while scan.current_offset <= max_offset {
            let off = scan.current_offset;
            scan.current_offset += 1;

            let item_id = page_get_item_id(&page, off).map_err(TupleError::from)?;
            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }

            let tuple = heap_page_get_tuple(&page, off)?;
            pool.unpin(client_id, buffer_id)?;
            return Ok(Some((
                ItemPointerData {
                    block_number: block,
                    offset_number: off,
                },
                tuple,
            )));
        }

        pool.unpin(client_id, buffer_id)?;
        scan.current_block += 1;
        scan.current_offset = 1;
    }

    Ok(None)
}

pub fn heap_scan_begin_visible(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    snapshot: Snapshot,
) -> Result<VisibleHeapScan, HeapError> {
    Ok(VisibleHeapScan {
        scan: heap_scan_begin(pool, rel)?,
        snapshot,
    })
}

pub fn heap_scan_next_visible(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &TransactionManager,
    scan: &mut VisibleHeapScan,
) -> Result<Option<(ItemPointerData, HeapTuple)>, HeapError> {
    while let Some((tid, tuple)) = heap_scan_next(pool, client_id, &mut scan.scan)? {
        if scan.snapshot.tuple_visible(txns, &tuple) {
            return Ok(Some((tid, tuple)));
        }
    }
    Ok(None)
}

/// Scan for the next visible tuple without copying tuple data.
/// Calls `process` with the raw tuple bytes (borrowing from the page buffer)
/// and returns its result. The tuple bytes are only valid during the callback.
pub fn heap_scan_next_visible_raw<T, E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &TransactionManager,
    scan: &mut VisibleHeapScan,
    mut process: impl FnMut(ItemPointerData, &[u8]) -> Result<T, E>,
) -> Result<Option<T>, E> {
    while scan.scan.current_block < scan.scan.nblocks {
        let block = scan.scan.current_block;
        let buffer_id = pin_existing_block(pool, client_id, scan.scan.rel, block).map_err(E::from)?;

        // Borrow the page in-place to avoid copying 8KB.
        let _content_lock = pool.lock_buffer_shared(buffer_id).map_err(|e| E::from(HeapError::Buffer(e)))?;
        let found = pool.with_page(buffer_id, |page| -> Result<Option<T>, E> {
            let max_offset = page_get_max_offset_number(page).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

            while scan.scan.current_offset <= max_offset {
                let off = scan.scan.current_offset;
                scan.scan.current_offset += 1;

                let item_id = page_get_item_id(page, off).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                    continue;
                }

                let tuple_bytes = page_get_item(page, off).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if !scan.snapshot.tuple_bytes_visible(txns, tuple_bytes) {
                    continue;
                }

                let tid = ItemPointerData {
                    block_number: block,
                    offset_number: off,
                };
                return Ok(Some(process(tid, tuple_bytes)?));
            }
            Ok(None)
        }).ok_or_else(|| E::from(HeapError::Buffer(Error::InvalidBuffer)))?;
        drop(_content_lock);

        pool.unpin(client_id, buffer_id).map_err(|e| E::from(HeapError::Buffer(e)))?;

        if let Some(result) = found? {
            return Ok(Some(result));
        }

        scan.scan.current_block += 1;
        scan.scan.current_offset = 1;
    }

    Ok(None)
}

/// Scan ALL remaining visible tuples, calling `process` for each one.
/// Processes all tuples on each page before moving to the next, avoiding
/// repeated pin/unpin per tuple.
pub fn heap_scan_all_visible_raw<E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &TransactionManager,
    scan: &mut VisibleHeapScan,
    mut process: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<usize, E> {
    let mut count = 0usize;
    while scan.scan.current_block < scan.scan.nblocks {
        let block = scan.scan.current_block;
        let buffer_id = pin_existing_block(pool, client_id, scan.scan.rel, block).map_err(E::from)?;

        let _content_lock = pool.lock_buffer_shared(buffer_id).map_err(|e| E::from(HeapError::Buffer(e)))?;
        pool.with_page(buffer_id, |page| -> Result<(), E> {
            let max_offset = page_get_max_offset_number(page).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

            while scan.scan.current_offset <= max_offset {
                let off = scan.scan.current_offset;
                scan.scan.current_offset += 1;

                let item_id = page_get_item_id(page, off).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                    continue;
                }

                let tuple_bytes = page_get_item(page, off).map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if !scan.snapshot.tuple_bytes_visible(txns, tuple_bytes) {
                    continue;
                }

                process(tuple_bytes)?;
                count += 1;
            }
            Ok(())
        }).ok_or_else(|| E::from(HeapError::Buffer(Error::InvalidBuffer)))??;
        drop(_content_lock);

        pool.unpin(client_id, buffer_id).map_err(|e| E::from(HeapError::Buffer(e)))?;
        scan.scan.current_block += 1;
        scan.scan.current_offset = 1;
    }

    Ok(count)
}

pub fn heap_insert(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_version(pool, client_id, rel, tuple, 0, 0)
}

pub fn heap_insert_mvcc(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    xid: TransactionId,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_mvcc_with_cid(pool, client_id, rel, xid, 0, tuple)
}

pub fn heap_insert_mvcc_with_cid(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    xid: TransactionId,
    cid: CommandId,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_version(pool, client_id, rel, tuple, xid, cid)
}

pub fn heap_fetch(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
) -> Result<HeapTuple, HeapError> {
    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let page = {
        let _content_lock = pool.lock_buffer_shared(buffer_id)?;
        pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?
    };
    let tuple = heap_page_get_tuple(&page, tid.offset_number)?;
    pool.unpin(client_id, buffer_id)?;
    Ok(tuple)
}

pub fn heap_fetch_visible(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    txns: &TransactionManager,
    snapshot: &Snapshot,
) -> Result<Option<HeapTuple>, HeapError> {
    let tuple = heap_fetch(pool, client_id, rel, tid)?;
    if snapshot.tuple_visible(txns, &tuple) {
        Ok(Some(tuple))
    } else {
        Ok(None)
    }
}

pub fn heap_delete(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
) -> Result<(), HeapError> {
    let snapshot = txns.snapshot(xid)?;

    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;

    let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
    let mut new_page = page;
    let mut tuple = heap_page_get_tuple(&new_page, tid.offset_number)?;

    if !snapshot.tuple_visible(txns, &tuple) {
        drop(_content_lock);
        pool.unpin(client_id, buffer_id)?;
        return Err(HeapError::TupleNotVisible(tid));
    }

    if tuple.header.xmax != 0 {
        let xmax_status = txns.status(tuple.header.xmax);
        if !matches!(xmax_status, Some(TransactionStatus::Aborted) | None) {
            drop(_content_lock);
            pool.unpin(client_id, buffer_id)?;
            return Err(HeapError::TupleAlreadyModified(tid));
        }
    }

    tuple.header.xmax = xid;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &tuple)?;
    pool.write_page_image(buffer_id, xid, &new_page)?;
    drop(_content_lock);
    pool.unpin(client_id, buffer_id)?;
    Ok(())
}

pub fn heap_delete_with_waiter(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    tid: ItemPointerData,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<(), HeapError> {
    let snapshot = txns.read().snapshot(xid)?;

    loop {
        let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
        let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;

        let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let mut new_page = page;
        let mut tuple = heap_page_get_tuple(&new_page, tid.offset_number)?;

        {
            let txns_guard = txns.read();
            if !snapshot.tuple_visible(&txns_guard, &tuple) {
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;
                return Err(HeapError::TupleNotVisible(tid));
            }
        }

        let xmax = tuple.header.xmax;
        if xmax == 0 {
            tuple.header.xmax = xid;
            heap_page_replace_tuple(&mut new_page, tid.offset_number, &tuple)?;
            pool.write_page_image(buffer_id, xid, &new_page)?;
            drop(_content_lock);
            pool.unpin(client_id, buffer_id)?;
            return Ok(());
        }

        drop(_content_lock);
        pool.unpin(client_id, buffer_id)?;

        let xmax_status = txns.read().status(xmax);

        match xmax_status {
            Some(TransactionStatus::InProgress) | None => {
                if let Some((txns_lock, txn_waiter)) = waiter {
                    txn_waiter.wait_for(txns_lock, xmax);
                    continue;
                }
                return Err(HeapError::TupleAlreadyModified(tid));
            }
            Some(TransactionStatus::Aborted) => {
                // Re-acquire lock and claim: retry will re-read the tuple;
                // if xmax is still the aborted xid, we treat it as claimable.
                let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
                let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;
                let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
                let mut new_page = page;
                let mut recheck = heap_page_get_tuple(&new_page, tid.offset_number)?;
                if recheck.header.xmax != xmax {
                    drop(_content_lock);
                    pool.unpin(client_id, buffer_id)?;
                    continue;
                }
                recheck.header.xmax = xid;
                heap_page_replace_tuple(&mut new_page, tid.offset_number, &recheck)?;
                pool.write_page_image(buffer_id, xid, &new_page)?;
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;
                return Ok(());
            }
            Some(TransactionStatus::Committed) => {
                return Err(HeapError::TupleAlreadyModified(tid));
            }
        }
    }
}

pub fn heap_update(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_update_with_cid(pool, client_id, rel, txns, xid, 0, tid, replacement)
}

pub fn heap_update_with_cid(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    let snapshot = txns.snapshot_for_command(xid, cid)?;
    let old = heap_fetch(pool, client_id, rel, tid)?;
    if !snapshot.tuple_visible(txns, &old) {
        return Err(HeapError::TupleNotVisible(tid));
    }
    if old.header.xmax != 0 {
        let xmax_status = txns.status(old.header.xmax);
        if !matches!(xmax_status, Some(TransactionStatus::Aborted) | None) {
            return Err(HeapError::TupleAlreadyModified(tid));
        }
    }

    let new_tid = heap_insert_version(pool, client_id, rel, replacement, xid, cid)?;

    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;
    let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
    let mut new_page = page;
    let mut old_version = heap_page_get_tuple(&new_page, tid.offset_number)?;
    old_version.header.xmax = xid;
    old_version.header.ctid = new_tid;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &old_version)?;
    pool.write_page_image(buffer_id, xid, &new_page)?;
    drop(_content_lock);
    pool.unpin(client_id, buffer_id)?;

    Ok(new_tid)
}

/// Result of attempting to claim a tuple for update under concurrency.
enum ClaimResult {
    Claimed,
    WaitFor(TransactionId),
    Updated { new_ctid: ItemPointerData },
    Deleted,
}

fn try_claim_tuple(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    target_tid: ItemPointerData,
) -> Result<(ClaimResult, ItemPointerData), HeapError> {
    let buffer_id = pin_existing_block(pool, client_id, rel, target_tid.block_number)?;
    let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;

    let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
    let tuple = heap_page_get_tuple(&page, target_tid.offset_number)?;

    if tuple.header.xmax == 0 {
        let mut new_page = page;
        let mut modified = tuple;
        modified.header.xmax = xid;
        heap_page_replace_tuple(&mut new_page, target_tid.offset_number, &modified)?;
        pool.write_page_image(buffer_id, xid, &new_page)?;
        drop(_content_lock);
        pool.unpin(client_id, buffer_id)?;
        return Ok((ClaimResult::Claimed, target_tid));
    }

    let xmax = tuple.header.xmax;
    let ctid = tuple.header.ctid;

    // Check xmax status while still holding the content lock. We use
    // try_read to avoid deadlock with parking_lot's write-preferring
    // RwLock: a pending txns writer would block a blocking read() call
    // while we hold the content lock, but try_read fails gracefully.
    // If we can't get the lock, treat as InProgress (conservative).
    let xmax_status = {
        let mut status = None;
        for _ in 0..10 {
            if let Some(guard) = txns.try_read() {
                status = guard.status(xmax);
                break;
            }
            std::thread::yield_now();
        }
        status
    };

    match xmax_status {
        Some(TransactionStatus::InProgress) | None => {
            drop(_content_lock);
            pool.unpin(client_id, buffer_id)?;
            Ok((ClaimResult::WaitFor(xmax), target_tid))
        }
        Some(TransactionStatus::Aborted) => {
            let mut new_page = page;
            let mut modified = tuple;
            modified.header.xmax = xid;
            heap_page_replace_tuple(&mut new_page, target_tid.offset_number, &modified)?;
            pool.write_page_image(buffer_id, xid, &new_page)?;
            drop(_content_lock);
            pool.unpin(client_id, buffer_id)?;
            Ok((ClaimResult::Claimed, target_tid))
        }
        Some(TransactionStatus::Committed) => {
            drop(_content_lock);
            pool.unpin(client_id, buffer_id)?;
            if ctid == target_tid {
                Ok((ClaimResult::Deleted, target_tid))
            } else {
                Ok((ClaimResult::Updated { new_ctid: ctid }, target_tid))
            }
        }
    }
}

pub fn heap_update_with_waiter(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<ItemPointerData, HeapError> {
    loop {
        let (result, _) = try_claim_tuple(pool, client_id, rel, txns, xid, tid)?;

        match result {
            ClaimResult::Claimed => {
                let new_tid = heap_insert_version(pool, client_id, rel, replacement, xid, cid)?;

                let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
                let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;
                let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
                let mut new_page = page;
                let mut old_version =
                    heap_page_get_tuple(&new_page, tid.offset_number)?;
                old_version.header.ctid = new_tid;
                heap_page_replace_tuple(
                    &mut new_page,
                    tid.offset_number,
                    &old_version,
                )?;
                pool.write_page_image(buffer_id, xid, &new_page)?;
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;

                return Ok(new_tid);
            }
            ClaimResult::WaitFor(xwait) => {
                if let Some((txns_lock, txn_waiter)) = waiter {
                    txn_waiter.wait_for(txns_lock, xwait);
                    continue;
                }
                return Err(HeapError::TupleAlreadyModified(tid));
            }
            ClaimResult::Deleted => {
                return Err(HeapError::TupleAlreadyModified(tid));
            }
            ClaimResult::Updated { new_ctid } => {
                return Err(HeapError::TupleUpdated(tid, new_ctid));
            }
        }
    }
}

pub fn heap_flush(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<(), HeapError> {
    use crate::FlushResult;
    let buffer_id = pin_existing_block(pool, client_id, rel, block_number)?;
    if let FlushResult::WriteIssued = pool.flush_buffer(buffer_id)? {
        pool.complete_write(buffer_id)?;
    }
    pool.unpin(client_id, buffer_id)?;
    Ok(())
}

fn ensure_relation_exists(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), HeapError> {
    pool.with_storage_mut(|s| s.smgr.open(rel))?;
    match pool.with_storage_mut(|s| s.smgr.create(rel, ForkNumber::Main, false)) {
        Ok(()) => {}
        Err(SmgrError::AlreadyExists { .. }) => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn heap_insert_version(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &HeapTuple,
    xmin: TransactionId,
    cid: CommandId,
) -> Result<ItemPointerData, HeapError> {
    ensure_relation_exists(pool, rel)?;

    loop {
        let target_block = pool.with_storage_mut(|s| -> Result<u32, HeapError> {
            let nblocks = s.smgr.nblocks(rel, ForkNumber::Main)?;
            if nblocks == 0 {
                let mut page = [0u8; crate::BLCKSZ];
                heap_page_init(&mut page);
                s.smgr.extend(rel, ForkNumber::Main, 0, &page, true)?;
                Ok(0)
            } else {
                Ok(nblocks - 1)
            }
        })?;

        let buffer_id = pin_existing_block(pool, client_id, rel, target_block)?;
        let _content_lock = pool.lock_buffer_exclusive(buffer_id)?;
        let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let mut new_page = page;
        let mut stored = tuple.clone();
        stored.header.xmin = xmin;
        stored.header.xmax = 0;
        stored.header.cid_or_xvac = cid;

        match heap_page_add_tuple(&mut new_page, target_block, &stored) {
            Ok(offset_number) => {
                pool.write_page_image(buffer_id, xmin, &new_page)?;
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;
                return Ok(ItemPointerData {
                    block_number: target_block,
                    offset_number,
                });
            }
            Err(TupleError::Page(PageError::NoSpace)) => {
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;
                pool.with_storage_mut(|s| -> Result<(), HeapError> {
                    let current_nblocks = s.smgr.nblocks(rel, ForkNumber::Main)?;
                    let mut page = [0u8; crate::BLCKSZ];
                    heap_page_init(&mut page);
                    s.smgr.extend(rel, ForkNumber::Main, current_nblocks, &page, true)?;
                    Ok(())
                })?;
            }
            Err(e) => {
                drop(_content_lock);
                pool.unpin(client_id, buffer_id)?;
                return Err(e.into());
            }
        }
    }
}


fn pin_existing_block(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<usize, HeapError> {
    let tag = crate::BufferTag {
        rel,
        fork: ForkNumber::Main,
        block: block_number,
    };
    match pool.request_page(client_id, tag)? {
        RequestPageResult::Hit { buffer_id } => Ok(buffer_id),
        RequestPageResult::ReadIssued { buffer_id } => {
            pool.complete_read(buffer_id)?;
            Ok(buffer_id)
        }
        RequestPageResult::WaitingOnRead { buffer_id } => {
            pool.wait_for_io(buffer_id)?;
            Ok(buffer_id)
        }
        RequestPageResult::AllBuffersPinned => Err(HeapError::NoBufferAvailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SmgrStorageBackend;
    use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_heapam_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: n,
        }
    }

    fn visible_tuple_payloads(
        base: &std::path::Path,
        rel: RelFileLocator,
        txns: &TransactionManager,
        snapshot: Snapshot,
    ) -> Vec<Vec<u8>> {
        let smgr = crate::storage::smgr::MdStorageManager::new(base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let mut scan = heap_scan_begin_visible(&pool, rel, snapshot).unwrap();
        let mut rows = Vec::new();
        while let Some((_tid, tuple)) =
            heap_scan_next_visible(&pool, 1, txns, &mut scan).unwrap()
        {
            rows.push(tuple.data);
        }
        rows
    }

    #[test]
    fn heap_insert_and_fetch_roundtrip() {
        let base = temp_dir("insert_fetch_roundtrip");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let tuple = HeapTuple::new_raw(2, b"hello|heap".to_vec());

        let tid = heap_insert(&pool, 1, rel(5000), &tuple).unwrap();
        let fetched = heap_fetch(&pool, 2, rel(5000), tid).unwrap();

        assert_eq!(fetched.data, tuple.data);
        assert_eq!(fetched.header.ctid, tid);
    }

    #[test]
    fn heap_insert_persists_after_flush_and_reload() {
        let base = temp_dir("persist_after_flush");
        let rel = rel(5001);
        let tid = {
            let smgr = crate::storage::smgr::MdStorageManager::new(&base);
            let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
            let tuple = HeapTuple::new_raw(2, b"persisted-tuple".to_vec());
            let tid = heap_insert(&pool, 1, rel, &tuple).unwrap();
            heap_flush(&pool, 1, rel, tid.block_number).unwrap();
            tid
        };

        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let fetched = heap_fetch(&pool, 2, rel, tid).unwrap();
        assert_eq!(fetched.data, b"persisted-tuple".to_vec());
    }

    #[test]
    fn heap_insert_spills_to_new_page_when_full() {
        let base = temp_dir("spill_to_new_page");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5002);

        let large = HeapTuple::new_raw(1, vec![0xAB; 7000]);
        let first = heap_insert(&pool, 1, rel, &large).unwrap();
        let second = heap_insert(&pool, 1, rel, &large).unwrap();
        let third = heap_insert(&pool, 1, rel, &large).unwrap();

        assert_eq!(first.block_number, 0);
        assert!(second.block_number > first.block_number);
        assert!(third.block_number > second.block_number);
    }

    #[test]
    fn heap_scan_returns_inserted_tuples_in_physical_order() {
        let base = temp_dir("scan_physical_order");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5003);

        let large = HeapTuple::new_raw(1, vec![0xAA; 7000]);
        let small = HeapTuple::new_raw(1, b"tail".to_vec());

        let t1 = heap_insert(&pool, 1, rel, &large).unwrap();
        let t2 = heap_insert(&pool, 1, rel, &large).unwrap();
        let t3 = heap_insert(&pool, 1, rel, &small).unwrap();

        let mut scan = heap_scan_begin(&pool, rel).unwrap();
        let mut seen = Vec::new();
        while let Some((tid, tuple)) = heap_scan_next(&pool, 2, &mut scan).unwrap() {
            seen.push((tid, tuple.data));
        }

        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].0, t1);
        assert_eq!(seen[1].0, t2);
        assert_eq!(seen[2].0, t3);
        assert_eq!(seen[2].1, b"tail".to_vec());
    }

    #[test]
    fn heap_scan_skips_unused_line_pointers() {
        let base = temp_dir("scan_skips_unused");
        let rel = rel(5004);
        let mut smgr = crate::storage::smgr::MdStorageManager::new(&base);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut page = [0u8; crate::BLCKSZ];
        heap_page_init(&mut page);
        let _ =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"first".to_vec())).unwrap();
        let off2 =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"second".to_vec())).unwrap();

        // Mark the second line pointer unused to simulate a hole on the page.
        let idx = crate::storage::page::max_align(crate::storage::page::SIZE_OF_PAGE_HEADER_DATA)
            + (usize::from(off2) - 1) * crate::storage::page::ITEM_ID_SIZE;
        page[idx..idx + crate::storage::page::ITEM_ID_SIZE]
            .copy_from_slice(&crate::storage::page::ItemIdData::unused().encode());

        smgr.extend(rel, ForkNumber::Main, 0, &page, true).unwrap();

        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let mut scan = heap_scan_begin(&pool, rel).unwrap();

        let first = heap_scan_next(&pool, 1, &mut scan).unwrap().unwrap();
        assert_eq!(first.1.data, b"first".to_vec());
        assert!(heap_scan_next(&pool, 1, &mut scan).unwrap().is_none());
    }

    #[test]
    fn heap_delete_hides_tuple_after_commit() {
        let base = temp_dir("mvcc_delete");
        let rel = rel(5005);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let inserter = txns.begin();
        let tid = heap_insert_mvcc(
            &pool,
            1,
            rel,
            inserter,
            &HeapTuple::new_raw(1, b"row".to_vec()),
        )
        .unwrap();
        txns.commit(inserter).unwrap();

        let deleter = txns.begin();
        heap_delete(&pool, 2, rel, &txns, deleter, tid).unwrap();

        let other = txns.begin();
        let other_snapshot = txns.snapshot(other).unwrap();
        let before_commit =
            heap_fetch_visible(&pool, 3, rel, tid, &txns, &other_snapshot).unwrap();
        assert!(before_commit.is_some());

        txns.commit(deleter).unwrap();
        let after_commit = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(
            heap_fetch_visible(&pool, 4, rel, tid, &txns, &after_commit)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn heap_update_preserves_old_version_until_commit_and_new_version_after() {
        let base = temp_dir("mvcc_update");
        let rel = rel(5006);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let inserter = txns.begin();
        let old_tid = heap_insert_mvcc(
            &pool,
            1,
            rel,
            inserter,
            &HeapTuple::new_raw(1, b"old".to_vec()),
        )
        .unwrap();
        txns.commit(inserter).unwrap();

        let updater = txns.begin();
        let new_tid = heap_update(
            &pool,
            2,
            rel,
            &txns,
            updater,
            old_tid,
            &HeapTuple::new_raw(1, b"new".to_vec()),
        )
        .unwrap();

        let concurrent = txns.begin();
        let concurrent_snapshot = txns.snapshot(concurrent).unwrap();
        let old_visible =
            heap_fetch_visible(&pool, 3, rel, old_tid, &txns, &concurrent_snapshot)
                .unwrap()
                .unwrap();
        assert_eq!(old_visible.data, b"old".to_vec());
        assert!(
            heap_fetch_visible(&pool, 3, rel, new_tid, &txns, &concurrent_snapshot)
                .unwrap()
                .is_none()
        );

        txns.commit(updater).unwrap();
        let committed_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(
            heap_fetch_visible(&pool, 4, rel, old_tid, &txns, &committed_snapshot)
                .unwrap()
                .is_none()
        );
        let new_visible =
            heap_fetch_visible(&pool, 4, rel, new_tid, &txns, &committed_snapshot)
                .unwrap()
                .unwrap();
        assert_eq!(new_visible.data, b"new".to_vec());

        let old_stored = heap_fetch(&pool, 5, rel, old_tid).unwrap();
        assert_eq!(old_stored.header.xmax, updater);
        assert_eq!(old_stored.header.ctid, new_tid);
    }

    #[test]
    fn visible_scan_filters_dead_versions() {
        let base = temp_dir("mvcc_scan");
        let rel = rel(5007);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let xid1 = txns.begin();
        let tid1 = heap_insert_mvcc(
            &pool,
            1,
            rel,
            xid1,
            &HeapTuple::new_raw(1, b"first".to_vec()),
        )
        .unwrap();
        txns.commit(xid1).unwrap();

        let xid2 = txns.begin();
        let _tid2 = heap_update(
            &pool,
            2,
            rel,
            &txns,
            xid2,
            tid1,
            &HeapTuple::new_raw(1, b"second".to_vec()),
        )
        .unwrap();
        txns.commit(xid2).unwrap();

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let mut scan = heap_scan_begin_visible(&pool, rel, snapshot).unwrap();
        let mut rows = Vec::new();
        while let Some((_tid, tuple)) =
            heap_scan_next_visible(&pool, 3, &txns, &mut scan).unwrap()
        {
            rows.push(tuple.data);
        }

        assert_eq!(rows, vec![b"second".to_vec()]);
    }

    #[test]
    fn mvcc_changes_can_live_in_buffer_cache_until_late_flush() {
        let base = temp_dir("mvcc_buffer_cache");
        let rel = rel(5008);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let insert_xid = txns.begin();
        let original_tid = heap_insert_mvcc(
            &pool,
            1,
            rel,
            insert_xid,
            &HeapTuple::new_raw(1, b"old".to_vec()),
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();
        heap_flush(&pool, 1, rel, original_tid.block_number).unwrap();

        let committed_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert_eq!(
            visible_tuple_payloads(&base, rel, &txns, committed_snapshot.clone()),
            vec![b"old".to_vec()]
        );

        let update_xid = txns.begin();
        let updated_tid = heap_update(
            &pool,
            1,
            rel,
            &txns,
            update_xid,
            original_tid,
            &HeapTuple::new_raw(1, b"new".to_vec()),
        )
        .unwrap();
        txns.commit(update_xid).unwrap();

        let delete_xid = txns.begin();
        heap_delete(&pool, 1, rel, &txns, delete_xid, updated_tid).unwrap();
        txns.commit(delete_xid).unwrap();

        // The writer's pool sees both committed changes immediately because it is
        // reading the dirty page out of shared buffers, not reloading from disk.
        let writer_view = heap_fetch_visible(
            &pool,
            2,
            rel,
            original_tid,
            &txns,
            &txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        )
        .unwrap();
        assert!(writer_view.is_none());

        let mut writer_scan = heap_scan_begin_visible(
            &pool,
            rel,
            txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        )
        .unwrap();
        assert!(
            heap_scan_next_visible(&pool, 2, &txns, &mut writer_scan)
                .unwrap()
                .is_none()
        );

        // With write-through caching, every write_page_image flushes to disk
        // immediately. A fresh pool sees the fully up-to-date disk image: the
        // update and delete are already durable, so no rows are visible.
        assert_eq!(
            visible_tuple_payloads(
                &base,
                rel,
                &txns,
                txns.snapshot(INVALID_TRANSACTION_ID).unwrap()
            ),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn durable_transaction_status_survives_restart_for_visibility() {
        let base = temp_dir("durable_mvcc_visibility");
        let rel = rel(5009);

        let tid = {
            let smgr = crate::storage::smgr::MdStorageManager::new(&base);
            let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
            let mut txns = TransactionManager::new_durable(&base).unwrap();

            let xid = txns.begin();
            let tid = heap_insert_mvcc(
                &pool,
                1,
                rel,
                xid,
                &HeapTuple::new_raw(1, b"row".to_vec()),
            )
            .unwrap();
            txns.commit(xid).unwrap();
            heap_flush(&pool, 1, rel, tid.block_number).unwrap();
            tid
        };

        let mut reopened_txns = TransactionManager::new_durable(&base).unwrap();
        let snapshot = reopened_txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let visible = heap_fetch_visible(&pool, 2, rel, tid, &reopened_txns, &snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(visible.data, b"row".to_vec());

        let deleting_xid = reopened_txns.begin();
        heap_delete(&pool, 2, rel, &reopened_txns, deleting_xid, tid).unwrap();
        reopened_txns.commit(deleting_xid).unwrap();
        heap_flush(&pool, 2, rel, tid.block_number).unwrap();
        drop(pool);

        let final_txns = TransactionManager::new_durable(&base).unwrap();
        let final_snapshot = final_txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        assert!(
            heap_fetch_visible(&pool, 3, rel, tid, &final_txns, &final_snapshot)
                .unwrap()
                .is_none()
        );
    }
}
