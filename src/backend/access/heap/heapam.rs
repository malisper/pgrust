use parking_lot::RwLock;

use super::HeapWalPolicy;
use super::visibilitymap::{
    VisibilityMapBuffer, VisibilityMapError, visibilitymap_clear_with_wal_policy, visibilitymap_pin,
};
use crate::backend::access::transam::xact::{
    CommandId, MvccError, Snapshot, TransactionId, TransactionManager, TransactionStatus,
};
use crate::backend::storage::buffer::Page;
use crate::backend::storage::page::bufpage::{
    ITEM_ID_SIZE, ItemIdFlags, MAX_HEAP_TUPLE_SIZE, PageError, max_align, page_clear_all_visible,
    page_get_item, page_get_item_id, page_get_item_id_unchecked, page_get_item_unchecked,
    page_get_max_offset_number, page_header, page_is_all_visible,
};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, SmgrError, StorageManager};
use crate::backend::utils::misc::interrupts::InterruptReason;
use crate::include::access::htup::{
    HeapTuple, ItemPointerData, TupleError, heap_page_add_tuple, heap_page_get_ctid,
    heap_page_get_tuple, heap_page_init, heap_page_replace_tuple,
};
use crate::include::access::visibilitymapdefs::{
    VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
};
use crate::include::catalog::is_bootstrap_catalog_storage_oid;
use crate::include::storage::buf_internals::BufferId;
use crate::pgrust::database::TransactionWaiter;
use crate::{
    BufferPool, ClientId, Error, OwnedBufferPin, PinnedBuffer, RequestPageResult,
    SmgrStorageBackend,
};
use std::rc::Rc;

#[derive(Debug)]
pub enum HeapError {
    Buffer(Error),
    Tuple(TupleError),
    Storage(SmgrError),
    Mvcc(MvccError),
    VisibilityMap(VisibilityMapError),
    NoBufferAvailable,
    TupleNotVisible(ItemPointerData),
    TupleAlreadyModified(ItemPointerData),
    TupleUpdated(ItemPointerData, ItemPointerData),
    DeadlockDetected,
    Interrupted(InterruptReason),
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

impl From<PageError> for HeapError {
    fn from(value: PageError) -> Self {
        Self::Tuple(TupleError::from(value))
    }
}

fn clear_page_visibility_if_needed(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
    page: &mut Page,
    vmbuf: &Option<VisibilityMapBuffer>,
) -> Result<bool, HeapError> {
    clear_page_visibility_if_needed_with_wal_policy(
        pool,
        client_id,
        rel,
        block,
        page,
        vmbuf,
        HeapWalPolicy::Wal,
    )
}

fn clear_page_visibility_if_needed_with_wal_policy(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
    page: &mut Page,
    vmbuf: &Option<VisibilityMapBuffer>,
    wal_policy: HeapWalPolicy,
) -> Result<bool, HeapError> {
    if !page_is_all_visible(page)? {
        return Ok(false);
    }
    page_clear_all_visible(page)?;
    let _ = visibilitymap_clear_with_wal_policy(
        pool,
        client_id,
        rel,
        block,
        vmbuf,
        VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
        wal_policy,
    )?;
    Ok(true)
}

fn write_heap_page_locked(
    pool: &BufferPool<SmgrStorageBackend>,
    buffer_id: BufferId,
    xid: TransactionId,
    page: &Page,
    guard: &mut parking_lot::RwLockWriteGuard<'_, Page>,
    wal_policy: HeapWalPolicy,
) -> Result<(), HeapError> {
    match wal_policy {
        HeapWalPolicy::Wal => pool.write_page_image_locked(buffer_id, xid, page, guard)?,
        HeapWalPolicy::NoWal => pool.write_page_no_wal_locked(buffer_id, page, guard)?,
    }
    Ok(())
}

/// Maximum tuples per 8kB page: 8160 usable / 28 min per tuple = 291.
const MAX_HEAP_TUPLES_PER_PAGE: usize = 291;

pub struct VisibleHeapScan {
    pub(crate) scan: HeapScan,
    pub(crate) snapshot: Snapshot,
    /// Shared pin on the currently pinned page, if any. The scan and any
    /// outstanding `BufferHeap` tuple slots share this pin via `Rc`. The
    /// buffer is unpinned only when the last reference is dropped — so a
    /// slot returned to the caller keeps the page alive even after the scan
    /// advances.
    pinned_buffer: Option<(u32, Rc<OwnedBufferPin<SmgrStorageBackend>>)>,
    /// Pool needed for wrapping pins.
    pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    /// Page-mode visibility: offsets of visible tuples on the current page.
    /// Populated once per page by `prepare_page_tuples`, then iterated without
    /// further visibility checks (like PostgreSQL's rs_vistuples).
    vis_tuples: [u16; MAX_HEAP_TUPLES_PER_PAGE],
    vis_count: u16,
    vis_index: u16,
}

impl VisibleHeapScan {
    /// True if there are remaining visible tuples on the current page.
    pub fn has_page_tuples(&self) -> bool {
        self.vis_index < self.vis_count
    }

    /// Return the buffer_id of the currently pinned page, if any.
    pub fn pinned_buffer_id(&self) -> Option<usize> {
        self.pinned_buffer.as_ref().map(|(_, pin)| pin.buffer_id())
    }

    /// Return a clone of the current page's shared pin (cheap Rc clone).
    pub fn pinned_buffer_rc(&self) -> Option<Rc<OwnedBufferPin<SmgrStorageBackend>>> {
        self.pinned_buffer.as_ref().map(|(_, pin)| Rc::clone(pin))
    }

    pub fn nblocks(&self) -> u32 {
        self.scan.nblocks
    }
}

impl std::fmt::Debug for VisibleHeapScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VisibleHeapScan")
            .field("scan", &self.scan)
            .field("snapshot", &self.snapshot)
            .field("pinned_buffer", &self.pinned_buffer)
            .finish_non_exhaustive()
    }
}

// No manual Drop needed: the Rc<OwnedBufferPin> unpins the buffer when
// the last reference (scan or slot) is dropped.

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
        let pin = pin_existing_block(pool, client_id, scan.rel, block)?;
        let buffer_id = pin.buffer_id();
        let page = pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let max_offset = page_get_max_offset_number(&page).map_err(TupleError::from)?;

        while scan.current_offset <= max_offset {
            let off = scan.current_offset;
            scan.current_offset += 1;

            let item_id = page_get_item_id(&page, off).map_err(TupleError::from)?;
            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }

            let tuple = heap_page_get_tuple(&page, off)?;
            drop(pin);
            return Ok(Some((
                ItemPointerData {
                    block_number: block,
                    offset_number: off,
                },
                tuple,
            )));
        }

        drop(pin);
        scan.current_block += 1;
        scan.current_offset = 1;
    }

    Ok(None)
}

pub fn heap_scan_begin_visible(
    pool: &std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    _client_id: ClientId,
    rel: RelFileLocator,
    snapshot: Snapshot,
) -> Result<VisibleHeapScan, HeapError> {
    Ok(VisibleHeapScan {
        scan: heap_scan_begin(pool, rel)?,
        snapshot: relation_snapshot(&snapshot, rel),
        pinned_buffer: None,
        pool: std::sync::Arc::clone(pool),
        vis_tuples: [0; MAX_HEAP_TUPLES_PER_PAGE],
        vis_count: 0,
        vis_index: 0,
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
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    mut process: impl FnMut(ItemPointerData, &[u8]) -> Result<T, E>,
) -> Result<Option<T>, E> {
    use crate::include::access::htup::INFOMASK_OFFSET;

    while scan.scan.current_block < scan.scan.nblocks {
        let block = scan.scan.current_block;

        // Reuse pinned buffer if we're still on the same page.
        let buffer_id = match &scan.pinned_buffer {
            Some((pinned_block, pin)) if *pinned_block == block => pin.buffer_id(),
            _ => {
                // Drop previous pin (Rc<OwnedBufferPin> handles unpin).
                drop(scan.pinned_buffer.take());
                let pin =
                    pin_existing_block(pool, client_id, scan.scan.rel, block).map_err(E::from)?;
                let bid = pin.into_raw();
                let owned = OwnedBufferPin::wrap_existing(std::sync::Arc::clone(&scan.pool), bid);
                scan.pinned_buffer = Some((block, Rc::new(owned)));
                bid
            }
        };

        // Acquire txns.read() before the page lock so readers never hold a
        // content lock while blocked behind a pending transaction writer.
        let txns_guard = txns.read();

        // Acquire shared content lock briefly for this tuple batch.
        // Released after processing — does NOT block writers across exec_next calls.
        let guard = pool
            .lock_buffer_shared(buffer_id)
            .map_err(|e| E::from(HeapError::Buffer(e)))?;
        let page: &Page = &*guard;
        let mut any_hints_written = false;

        let found: Result<Option<T>, E> = (|| {
            let max_offset = page_get_max_offset_number(page)
                .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

            while scan.scan.current_offset <= max_offset {
                let off = scan.scan.current_offset;
                scan.scan.current_offset += 1;

                let item_id = page_get_item_id(page, off)
                    .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                    continue;
                }

                let tuple_bytes = page_get_item(page, off)
                    .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

                // Fast path: check hint bits without acquiring the txns lock.
                if let Some(visible) = scan
                    .snapshot
                    .tuple_bytes_try_visible_from_hints(tuple_bytes)
                {
                    if !visible {
                        continue;
                    }
                    let tid = ItemPointerData {
                        block_number: block,
                        offset_number: off,
                    };
                    return Ok(Some(process(tid, tuple_bytes)?));
                }

                let (visible, hints) = scan
                    .snapshot
                    .tuple_bytes_visible_with_hints(&txns_guard, tuple_bytes);

                if hints != 0 {
                    unsafe {
                        let hint_off = item_id.lp_off as usize + INFOMASK_OFFSET;
                        let page_ptr = page as *const Page as *mut u8;
                        let current = u16::from_le_bytes([
                            *page_ptr.add(hint_off),
                            *page_ptr.add(hint_off + 1),
                        ]);
                        let updated = (current | hints).to_le_bytes();
                        *page_ptr.add(hint_off) = updated[0];
                        *page_ptr.add(hint_off + 1) = updated[1];
                    }
                    any_hints_written = true;
                }

                if !visible {
                    continue;
                }

                let tid = ItemPointerData {
                    block_number: block,
                    offset_number: off,
                };
                return Ok(Some(process(tid, tuple_bytes)?));
            }
            Ok(None)
        })();

        if any_hints_written {
            pool.mark_buffer_dirty_hint(buffer_id);
        }
        drop(guard);
        drop(txns_guard);

        if let Some(result) = found? {
            // Keep buffer pinned — next call will likely need the same page.
            return Ok(Some(result));
        }

        // Page exhausted — drop pin and move to next block.
        drop(scan.pinned_buffer.take());
        scan.scan.current_block += 1;
        scan.scan.current_offset = 1;
    }

    // Scan complete — drop any remaining pin.
    drop(scan.pinned_buffer.take());

    Ok(None)
}

/// Advance to the next page and collect visible tuple offsets. The caller
/// must hold no lock; this function acquires the shared lock, runs visibility
/// checks, records offsets in `scan.vis_tuples[]`, and releases the lock.
/// Returns the buffer_id of the pinned page (for the caller to lock), or
/// None if the scan is complete.
pub fn heap_scan_prepare_next_page<E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
) -> Result<Option<usize>, E> {
    // Drop previous pin and advance to next block.
    if scan.pinned_buffer.is_some() {
        drop(scan.pinned_buffer.take());
        scan.scan.current_block += 1;
    }
    // If no pinned buffer, this is the first call — start at current_block (0).

    heap_scan_prepare_current_page(pool, client_id, txns, scan, true)
}

pub fn heap_scan_prepare_page_at<E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    block: u32,
) -> Result<Option<usize>, E> {
    drop(scan.pinned_buffer.take());
    scan.scan.current_block = block;
    heap_scan_prepare_current_page(pool, client_id, txns, scan, false)
}

fn heap_scan_prepare_current_page<E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    advance_past_empty: bool,
) -> Result<Option<usize>, E> {
    while scan.scan.current_block < scan.scan.nblocks {
        let block = scan.scan.current_block;
        let pin = pin_existing_block(pool, client_id, scan.scan.rel, block).map_err(E::from)?;
        let buffer_id = pin.into_raw();
        let owned = OwnedBufferPin::wrap_existing(std::sync::Arc::clone(&scan.pool), buffer_id);
        scan.pinned_buffer = Some((block, Rc::new(owned)));

        // Collect visible tuple offsets under a single lock.
        let guard = pool
            .lock_buffer_shared(buffer_id)
            .map_err(|e| E::from(HeapError::Buffer(e)))?;
        let page: &Page = &*guard;
        let max_offset = page_get_max_offset_number(page)
            .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

        let mut ntup: u16 = 0;
        let mut pending_offsets = Vec::new();

        for off in 1..=max_offset {
            // Safe: off is in 1..=max_offset from page_get_max_offset_number.
            let item_id = page_get_item_id_unchecked(page, off);
            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }

            let tuple_bytes = page_get_item_unchecked(page, off);

            let visible = if let Some(vis) = scan
                .snapshot
                .tuple_bytes_try_visible_from_hints(tuple_bytes)
            {
                vis
            } else {
                pending_offsets.push((off, tuple_bytes.to_vec()));
                continue;
            };

            if visible {
                scan.vis_tuples[ntup as usize] = off;
                ntup += 1;
            }
        }
        drop(guard);

        if !pending_offsets.is_empty() {
            let txns_guard = txns.read();
            for (off, tuple_bytes) in pending_offsets {
                if scan.snapshot.tuple_bytes_visible(&txns_guard, &tuple_bytes) {
                    scan.vis_tuples[ntup as usize] = off;
                    ntup += 1;
                }
            }
        }

        scan.vis_count = ntup;
        scan.vis_index = 0;

        if ntup > 0 {
            return Ok(Some(buffer_id));
        }

        // Empty page — drop pin and try next.
        drop(scan.pinned_buffer.take());
        scan.scan.current_block += 1;
        if !advance_past_empty {
            return Ok(None);
        }
    }

    Ok(None)
}

fn relation_snapshot(snapshot: &Snapshot, rel: RelFileLocator) -> Snapshot {
    let mut snapshot = snapshot.clone();
    if !is_bootstrap_catalog_storage_oid(rel.rel_number)
        && let Some(cid) = snapshot.heap_current_cid()
    {
        snapshot.current_cid = cid;
    }
    snapshot
}

/// Return the next visible tuple on the current page. The caller must hold
/// the shared content lock on the page (via `pool.lock_buffer_shared`).
/// Returns None when all visible tuples on this page have been consumed.
pub fn heap_scan_page_next_tuple<'a>(
    page: &'a Page,
    scan: &mut VisibleHeapScan,
) -> Option<(ItemPointerData, &'a [u8])> {
    if scan.vis_index >= scan.vis_count {
        return None;
    }
    let off = scan.vis_tuples[scan.vis_index as usize];
    scan.vis_index += 1;
    // Safe: offset was validated during heap_scan_prepare_next_page.
    let tuple_bytes = page_get_item_unchecked(page, off);
    let tid = ItemPointerData {
        block_number: scan.scan.current_block,
        offset_number: off,
    };
    Some((tid, tuple_bytes))
}

/// Clean up a pagemode scan — drop any remaining pin.
pub fn heap_scan_end<E: From<HeapError>>(
    _pool: &BufferPool<SmgrStorageBackend>,
    _client_id: ClientId,
    scan: &mut VisibleHeapScan,
) -> Result<(), E> {
    drop(scan.pinned_buffer.take());
    Ok(())
}

/// Scan ALL remaining visible tuples, calling `process` for each one.
/// Holds a single shared content lock per page, avoiding per-tuple lock
/// acquire/release. Sets hint bits via unsafe under the shared lock.
pub fn heap_scan_all_visible_raw<E: From<HeapError>>(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    mut process: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<usize, E> {
    use crate::include::access::htup::INFOMASK_OFFSET;

    let mut count = 0usize;
    while scan.scan.current_block < scan.scan.nblocks {
        let block = scan.scan.current_block;
        let pin = pin_existing_block(pool, client_id, scan.scan.rel, block).map_err(E::from)?;
        let buffer_id = pin.buffer_id();

        let txns_guard = txns.read();
        let guard = pool
            .lock_buffer_shared(buffer_id)
            .map_err(|e| E::from(HeapError::Buffer(e)))?;
        let page: &Page = &*guard;
        let mut any_hints_written = false;

        let result: Result<(), E> = (|| {
            let max_offset = page_get_max_offset_number(page)
                .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

            while scan.scan.current_offset <= max_offset {
                let off = scan.scan.current_offset;
                scan.scan.current_offset += 1;

                let item_id = page_get_item_id(page, off)
                    .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;
                if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                    continue;
                }

                let tuple_bytes = page_get_item(page, off)
                    .map_err(|e| E::from(HeapError::Tuple(TupleError::from(e))))?;

                // Fast path: check hint bits without acquiring the txns lock.
                if let Some(visible) = scan
                    .snapshot
                    .tuple_bytes_try_visible_from_hints(tuple_bytes)
                {
                    if visible {
                        process(tuple_bytes)?;
                        count += 1;
                    }
                    continue;
                }

                let (visible, hints) = scan
                    .snapshot
                    .tuple_bytes_visible_with_hints(&txns_guard, tuple_bytes);

                if hints != 0 {
                    unsafe {
                        let hint_off = item_id.lp_off as usize + INFOMASK_OFFSET;
                        let page_ptr = page as *const Page as *mut u8;
                        let current = u16::from_le_bytes([
                            *page_ptr.add(hint_off),
                            *page_ptr.add(hint_off + 1),
                        ]);
                        let updated = (current | hints).to_le_bytes();
                        *page_ptr.add(hint_off) = updated[0];
                        *page_ptr.add(hint_off + 1) = updated[1];
                    }
                    any_hints_written = true;
                }

                if visible {
                    process(tuple_bytes)?;
                    count += 1;
                }
            }
            Ok(())
        })();

        if any_hints_written {
            pool.mark_buffer_dirty_hint(buffer_id);
        }
        drop(guard);
        drop(txns_guard);
        drop(pin);

        result?;
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
    heap_insert_version(pool, client_id, rel, tuple, 0, 0, 100)
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
    heap_insert_version(pool, client_id, rel, tuple, xid, cid, 100)
}

pub fn heap_insert_mvcc_with_cid_and_fillfactor(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    xid: TransactionId,
    cid: CommandId,
    tuple: &HeapTuple,
    fillfactor: u16,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_version(pool, client_id, rel, tuple, xid, cid, fillfactor)
}

pub fn heap_fetch(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
) -> Result<HeapTuple, HeapError> {
    let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let page = pool
        .read_page(pin.buffer_id())
        .ok_or(Error::InvalidBuffer)?;
    let tuple = heap_page_get_tuple(&page, tid.offset_number)?;
    drop(pin);
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
    let snapshot = relation_snapshot(snapshot, rel);
    heap_fetch_visible_impl(pool, client_id, rel, tid, |tuple| {
        snapshot.tuple_visible(txns, tuple)
    })
}

pub fn heap_fetch_visible_with_txns(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
) -> Result<Option<HeapTuple>, HeapError> {
    let txns_guard = txns.read();
    let snapshot = relation_snapshot(snapshot, rel);
    let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let buffer_id = pin.buffer_id();
    let guard = pool.lock_buffer_shared(buffer_id)?;
    let tuple = heap_page_get_tuple(&guard, tid.offset_number)?;
    let visible = snapshot.tuple_visible(&txns_guard, &tuple);
    drop(guard);
    drop(pin);
    if visible { Ok(Some(tuple)) } else { Ok(None) }
}

fn heap_fetch_visible_impl(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    is_visible: impl FnOnce(&HeapTuple) -> bool,
) -> Result<Option<HeapTuple>, HeapError> {
    let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let buffer_id = pin.buffer_id();
    let guard = pool.lock_buffer_shared(buffer_id)?;
    let tuple = heap_page_get_tuple(&guard, tid.offset_number)?;
    let visible = is_visible(&tuple);
    drop(guard);
    drop(pin);

    if visible { Ok(Some(tuple)) } else { Ok(None) }
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

    let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let buffer_id = pin.buffer_id();
    let mut vmbuf = None;
    visibilitymap_pin(pool, rel, tid.block_number, &mut vmbuf)?;

    let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
    let mut new_page = *guard;
    let mut tuple = heap_page_get_tuple(&new_page, tid.offset_number)?;

    if !snapshot.tuple_visible(txns, &tuple) {
        return Err(HeapError::TupleNotVisible(tid));
    }

    if tuple.header.xmax != 0 {
        let xmax_status = txns.status(tuple.header.xmax);
        if !matches!(xmax_status, Some(TransactionStatus::Aborted) | None) {
            return Err(HeapError::TupleAlreadyModified(tid));
        }
    }

    let _ = clear_page_visibility_if_needed(
        pool,
        client_id,
        rel,
        tid.block_number,
        &mut new_page,
        &vmbuf,
    )?;
    tuple.header.xmax = xid;
    tuple.header.cid_or_xvac = delete_command_id(txns, &snapshot, &tuple, snapshot.current_cid);
    // Clear HEAP_XMAX_INVALID — xmax is now a real transaction.
    tuple.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &tuple)?;
    pool.write_page_image_locked(buffer_id, xid, &new_page, &mut guard)?;
    Ok(())
}

fn delete_command_id(
    txns: &TransactionManager,
    snapshot: &Snapshot,
    tuple: &HeapTuple,
    cmax: CommandId,
) -> CommandId {
    if snapshot.transaction_is_own(tuple.header.xmin) {
        txns.combo_command_id(tuple.header.xmin, tuple.header.cid_or_xvac, cmax)
    } else {
        cmax
    }
}

pub fn heap_delete_with_waiter(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    tid: ItemPointerData,
    snapshot: &Snapshot,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<(), HeapError> {
    heap_delete_with_waiter_with_wal_policy(
        pool,
        client_id,
        rel,
        txns,
        xid,
        tid,
        snapshot,
        waiter,
        HeapWalPolicy::Wal,
    )
}

pub fn heap_delete_with_waiter_with_wal_policy(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    tid: ItemPointerData,
    snapshot: &Snapshot,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
    wal_policy: HeapWalPolicy,
) -> Result<(), HeapError> {
    loop {
        let txns_guard = txns.read();
        let snapshot = relation_snapshot(snapshot, rel);
        let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
        let buffer_id = pin.buffer_id();
        let mut vmbuf = None;
        visibilitymap_pin(pool, rel, tid.block_number, &mut vmbuf)?;

        let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
        let mut new_page = *guard;
        let mut tuple = heap_page_get_tuple(&new_page, tid.offset_number)?;

        if !snapshot.tuple_visible(&txns_guard, &tuple) {
            return Err(HeapError::TupleNotVisible(tid));
        }

        let xmax = tuple.header.xmax;
        if xmax == 0 {
            let _ = clear_page_visibility_if_needed_with_wal_policy(
                pool,
                client_id,
                rel,
                tid.block_number,
                &mut new_page,
                &vmbuf,
                wal_policy,
            )?;
            let cmax = delete_command_id(&txns_guard, &snapshot, &tuple, snapshot.current_cid);
            drop(txns_guard);
            tuple.header.xmax = xid;
            tuple.header.cid_or_xvac = cmax;
            tuple.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
            heap_page_replace_tuple(&mut new_page, tid.offset_number, &tuple)?;
            write_heap_page_locked(pool, buffer_id, xid, &new_page, &mut guard, wal_policy)?;
            return Ok(());
        }
        drop(txns_guard);
        if xmax == xid {
            return Err(HeapError::TupleAlreadyModified(tid));
        }

        drop(guard);
        drop(pin);

        let xmax_status = txns.read().status(xmax);

        match xmax_status {
            Some(TransactionStatus::InProgress) | None => {
                if let Some((txns_lock, txn_waiter, interrupts)) = waiter {
                    match txn_waiter.wait_for(txns_lock, xmax, client_id, interrupts) {
                        crate::backend::storage::lmgr::WaitOutcome::Completed => continue,
                        crate::backend::storage::lmgr::WaitOutcome::DeadlockTimeout => {
                            return Err(HeapError::DeadlockDetected);
                        }
                        crate::backend::storage::lmgr::WaitOutcome::Interrupted(reason) => {
                            return Err(HeapError::Interrupted(reason));
                        }
                    }
                }
                return Err(HeapError::TupleAlreadyModified(tid));
            }
            Some(TransactionStatus::Aborted) => {
                // Re-acquire lock and claim: retry will re-read the tuple;
                // if xmax is still the aborted xid, we treat it as claimable.
                let pin2 = pin_existing_block(pool, client_id, rel, tid.block_number)?;
                let buffer_id2 = pin2.buffer_id();
                let mut vmbuf2 = None;
                visibilitymap_pin(pool, rel, tid.block_number, &mut vmbuf2)?;
                let mut guard = pool.lock_buffer_exclusive(buffer_id2)?;
                let mut new_page = *guard;
                let mut recheck = heap_page_get_tuple(&new_page, tid.offset_number)?;
                if recheck.header.xmax != xmax {
                    drop(guard);
                    drop(pin2);
                    continue;
                }
                let _ = clear_page_visibility_if_needed_with_wal_policy(
                    pool,
                    client_id,
                    rel,
                    tid.block_number,
                    &mut new_page,
                    &vmbuf2,
                    wal_policy,
                )?;
                let cmax = {
                    let txns_guard = txns.read();
                    delete_command_id(&txns_guard, &snapshot, &recheck, snapshot.current_cid)
                };
                recheck.header.xmax = xid;
                recheck.header.cid_or_xvac = cmax;
                recheck.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
                heap_page_replace_tuple(&mut new_page, tid.offset_number, &recheck)?;
                write_heap_page_locked(pool, buffer_id2, xid, &new_page, &mut guard, wal_policy)?;
                return Ok(());
            }
            Some(TransactionStatus::Committed) => {
                // Read just the ctid under a shared lock — no page copy.
                let pin2 = pin_existing_block(pool, client_id, rel, tid.block_number)?;
                let buffer_id2 = pin2.buffer_id();
                let guard2 = pool.lock_buffer_shared(buffer_id2)?;
                let current_ctid =
                    heap_page_get_ctid(&*guard2, tid.offset_number).map_err(HeapError::from)?;
                drop(guard2);
                drop(pin2);
                if current_ctid == tid {
                    return Err(HeapError::TupleAlreadyModified(tid));
                }
                return Err(HeapError::TupleUpdated(tid, current_ctid));
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

    let new_tid = heap_insert_version(pool, client_id, rel, replacement, xid, cid, 100)?;

    let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let buffer_id = pin.buffer_id();
    let mut vmbuf = None;
    visibilitymap_pin(pool, rel, tid.block_number, &mut vmbuf)?;
    let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
    let mut new_page = *guard;
    let mut old_version = heap_page_get_tuple(&new_page, tid.offset_number)?;
    let _ = clear_page_visibility_if_needed(
        pool,
        client_id,
        rel,
        tid.block_number,
        &mut new_page,
        &vmbuf,
    )?;
    old_version.header.xmax = xid;
    old_version.header.cid_or_xvac = delete_command_id(txns, &snapshot, &old_version, cid);
    old_version.header.ctid = new_tid;
    // Clear HEAP_XMAX_INVALID — xmax is now a real transaction, not invalid.
    old_version.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &old_version)?;
    pool.write_page_image_locked(buffer_id, xid, &new_page, &mut guard)?;

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
    cid: CommandId,
    snapshot: &Snapshot,
    target_tid: ItemPointerData,
) -> Result<(ClaimResult, ItemPointerData), HeapError> {
    let pin = pin_existing_block(pool, client_id, rel, target_tid.block_number)?;
    let buffer_id = pin.buffer_id();
    let mut vmbuf = None;
    visibilitymap_pin(pool, rel, target_tid.block_number, &mut vmbuf)?;

    let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
    let mut new_page = *guard;
    let tuple = heap_page_get_tuple(&new_page, target_tid.offset_number)?;

    if tuple.header.xmax == 0 {
        let mut modified = tuple;
        let _ = clear_page_visibility_if_needed(
            pool,
            client_id,
            rel,
            target_tid.block_number,
            &mut new_page,
            &vmbuf,
        )?;
        let cmax = {
            let txns_guard = txns.read();
            delete_command_id(&txns_guard, snapshot, &modified, cid)
        };
        modified.header.xmax = xid;
        modified.header.cid_or_xvac = cmax;
        modified.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
        heap_page_replace_tuple(&mut new_page, target_tid.offset_number, &modified)?;
        pool.write_page_image_locked(buffer_id, xid, &new_page, &mut guard)?;
        return Ok((ClaimResult::Claimed, target_tid));
    }

    let xmax = tuple.header.xmax;
    if xmax == xid {
        return Ok((ClaimResult::Deleted, target_tid));
    }

    drop(guard);
    drop(pin);

    // Check xmax status after releasing the buffer lock. Safe to use
    // blocking read() here because the buffer lock is already dropped
    // (lines 784-785), so there's no deadlock risk with the write-preferring
    // RwLock. Previous code used try_read() with fallback to None, which
    // caused an infinite busy-loop under contention (see bugs/005).
    let xmax_status = txns.read().status(xmax);

    match xmax_status {
        Some(TransactionStatus::InProgress) | None => Ok((ClaimResult::WaitFor(xmax), target_tid)),
        Some(TransactionStatus::Aborted) => {
            let pin2 = pin_existing_block(pool, client_id, rel, target_tid.block_number)?;
            let buffer_id2 = pin2.buffer_id();
            let mut vmbuf2 = None;
            visibilitymap_pin(pool, rel, target_tid.block_number, &mut vmbuf2)?;
            let mut guard = pool.lock_buffer_exclusive(buffer_id2)?;
            let mut new_page = *guard;
            let mut modified = heap_page_get_tuple(&new_page, target_tid.offset_number)?;
            let _ = clear_page_visibility_if_needed(
                pool,
                client_id,
                rel,
                target_tid.block_number,
                &mut new_page,
                &vmbuf2,
            )?;
            let cmax = {
                let txns_guard = txns.read();
                delete_command_id(&txns_guard, snapshot, &modified, cid)
            };
            modified.header.xmax = xid;
            modified.header.cid_or_xvac = cmax;
            modified.header.infomask &= !crate::include::access::htup::HEAP_XMAX_INVALID;
            heap_page_replace_tuple(&mut new_page, target_tid.offset_number, &modified)?;
            pool.write_page_image_locked(buffer_id2, xid, &new_page, &mut guard)?;
            Ok((ClaimResult::Claimed, target_tid))
        }
        Some(TransactionStatus::Committed) => {
            // Re-read the tuple to get the current ctid. The ctid captured
            // before we dropped the lock may be stale: the committer writes
            // ctid (pointing to the new version) AFTER setting xmax but
            // BEFORE committing. If we read the tuple before ctid was
            // written, we'd see ctid == self and incorrectly return Deleted
            // instead of Updated.
            // Read just the ctid under a shared lock — no page copy.
            let pin2 = pin_existing_block(pool, client_id, rel, target_tid.block_number)?;
            let buffer_id2 = pin2.buffer_id();
            let guard = pool.lock_buffer_shared(buffer_id2)?;
            let current_ctid =
                heap_page_get_ctid(&*guard, target_tid.offset_number).map_err(HeapError::from)?;
            drop(guard);
            drop(pin2);
            if current_ctid == target_tid {
                Ok((ClaimResult::Deleted, target_tid))
            } else {
                Ok((
                    ClaimResult::Updated {
                        new_ctid: current_ctid,
                    },
                    target_tid,
                ))
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
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<ItemPointerData, HeapError> {
    let snapshot = txns.read().snapshot_for_command(xid, cid)?;
    heap_update_with_waiter_with_snapshot(
        pool,
        client_id,
        rel,
        txns,
        xid,
        cid,
        tid,
        replacement,
        &snapshot,
        waiter,
    )
}

pub fn heap_update_with_waiter_with_snapshot(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
    snapshot: &Snapshot,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<ItemPointerData, HeapError> {
    loop {
        let (result, _) = try_claim_tuple(pool, client_id, rel, txns, xid, cid, snapshot, tid)?;

        match result {
            ClaimResult::Claimed => {
                let new_tid =
                    heap_insert_version(pool, client_id, rel, replacement, xid, cid, 100)?;

                let pin = pin_existing_block(pool, client_id, rel, tid.block_number)?;
                let buffer_id = pin.buffer_id();
                let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
                let mut new_page = *guard;
                let mut old_version = heap_page_get_tuple(&new_page, tid.offset_number)?;
                old_version.header.ctid = new_tid;
                // xmax was already set by try_claim_tuple, and HEAP_XMAX_INVALID cleared there.
                heap_page_replace_tuple(&mut new_page, tid.offset_number, &old_version)?;
                pool.write_page_image_locked(buffer_id, xid, &new_page, &mut guard)?;

                return Ok(new_tid);
            }
            ClaimResult::WaitFor(xwait) => {
                if let Some((txns_lock, txn_waiter, interrupts)) = waiter {
                    match txn_waiter.wait_for(txns_lock, xwait, client_id, interrupts) {
                        crate::backend::storage::lmgr::WaitOutcome::Completed => continue,
                        crate::backend::storage::lmgr::WaitOutcome::DeadlockTimeout => {
                            return Err(HeapError::DeadlockDetected);
                        }
                        crate::backend::storage::lmgr::WaitOutcome::Interrupted(reason) => {
                            return Err(HeapError::Interrupted(reason));
                        }
                    }
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
    let pin = pin_existing_block(pool, client_id, rel, block_number)?;
    let buffer_id = pin.buffer_id();
    if let FlushResult::WriteIssued = pool.flush_buffer(buffer_id)? {
        pool.complete_write(buffer_id)?;
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
    fillfactor: u16,
) -> Result<ItemPointerData, HeapError> {
    if tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE {
        return Err(HeapError::Tuple(TupleError::Oversized {
            size: tuple.serialized_len(),
            max_size: MAX_HEAP_TUPLE_SIZE,
        }));
    }

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

        let pin = pin_existing_block(pool, client_id, rel, target_block)?;
        let buffer_id = pin.buffer_id();
        let mut vmbuf = None;
        visibilitymap_pin(pool, rel, target_block, &mut vmbuf)?;
        let mut guard = pool.lock_buffer_exclusive(buffer_id)?;
        let mut new_page = *guard;
        let mut stored = tuple.clone();
        stored.header.xmin = xmin;
        stored.header.xmax = 0;
        stored.header.cid_or_xvac = cid;
        stored.header.infomask |= crate::include::access::htup::HEAP_XMAX_INVALID;

        let serialized_tuple = stored.serialize();
        let page_was_all_visible = page_is_all_visible(&new_page)?;
        if !heap_page_has_fillfactor_space(&new_page, serialized_tuple.len(), fillfactor)? {
            drop(guard);
            drop(pin);
            extend_heap_relation(pool, rel)?;
            continue;
        }
        match heap_page_add_tuple(&mut new_page, target_block, &stored) {
            Ok(offset_number) => {
                if page_was_all_visible {
                    let _ = clear_page_visibility_if_needed(
                        pool,
                        client_id,
                        rel,
                        target_block,
                        &mut new_page,
                        &vmbuf,
                    )?;
                }
                pool.write_page_insert_locked(
                    buffer_id,
                    xmin,
                    &new_page,
                    &mut guard,
                    offset_number,
                    &serialized_tuple,
                )?;
                return Ok(ItemPointerData {
                    block_number: target_block,
                    offset_number,
                });
            }
            Err(TupleError::Page(PageError::NoSpace)) => {
                drop(guard);
                drop(pin);
                extend_heap_relation(pool, rel)?;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

fn heap_page_has_fillfactor_space(
    page: &Page,
    tuple_len: usize,
    fillfactor: u16,
) -> Result<bool, HeapError> {
    let fillfactor = fillfactor.clamp(10, 100);
    if fillfactor == 100 {
        return Ok(true);
    }
    let header = page_header(page).map_err(TupleError::from)?;
    let required = max_align(tuple_len) + ITEM_ID_SIZE;
    if header.free_space() < required {
        return Ok(false);
    }
    if page_get_max_offset_number(page).map_err(TupleError::from)? == 0 {
        return Ok(true);
    }
    let free_after = header.free_space() - required;
    let reserved = crate::BLCKSZ * usize::from(100 - fillfactor) / 100;
    Ok(free_after >= reserved)
}

fn extend_heap_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), HeapError> {
    pool.with_storage_mut(|s| -> Result<(), HeapError> {
        let current_nblocks = s.smgr.nblocks(rel, ForkNumber::Main)?;
        let mut page = [0u8; crate::BLCKSZ];
        heap_page_init(&mut page);
        s.smgr
            .extend(rel, ForkNumber::Main, current_nblocks, &page, true)?;
        Ok(())
    })
}

fn pin_existing_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, HeapError> {
    let tag = crate::BufferTag {
        rel,
        fork: ForkNumber::Main,
        block: block_number,
    };
    let buffer_id = match pool.request_page(client_id, tag)? {
        RequestPageResult::Hit { buffer_id } => buffer_id,
        RequestPageResult::ReadIssued { buffer_id } => {
            if let Err(e) = pool.complete_read(buffer_id) {
                let _ = pool.fail_read(buffer_id);
                return Err(e.into());
            }
            buffer_id
        }
        RequestPageResult::WaitingOnRead { buffer_id } => {
            pool.wait_for_io(buffer_id)?;
            buffer_id
        }
        RequestPageResult::AllBuffersPinned => return Err(HeapError::NoBufferAvailable),
    };
    // request_page already pinned the buffer; wrap it in an RAII guard
    // that will unpin on drop without incrementing the pin count again.
    Ok(pool.wrap_pinned(client_id, buffer_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SmgrStorageBackend;
    use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
    use crate::backend::storage::page::bufpage::MAX_HEAP_TUPLE_SIZE;
    use crate::include::access::htup::{AttributeAlign, AttributeDesc, TupleValue};
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

    /// Test-only: create the storage fork for a relation. In production,
    /// forks are created at startup by `Database::open` and by `CREATE TABLE`.
    /// Tests that use raw buffer pools must call this before inserting.
    fn create_fork(pool: &BufferPool<SmgrStorageBackend>, rel: RelFileLocator) {
        pool.with_storage_mut(|s| {
            s.smgr.open(rel).unwrap();
            match s.smgr.create(rel, ForkNumber::Main, false) {
                Ok(()) => {}
                Err(SmgrError::AlreadyExists { .. }) => {}
                Err(e) => panic!("create_fork failed: {e:?}"),
            }
        });
    }

    fn visible_tuple_payloads(
        base: &std::path::Path,
        rel: RelFileLocator,
        txns: &TransactionManager,
        snapshot: Snapshot,
    ) -> Vec<Vec<u8>> {
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 4));
        let mut scan = heap_scan_begin_visible(&pool, 1, rel, snapshot).unwrap();
        let mut rows = Vec::new();
        while let Some((_tid, tuple)) = heap_scan_next_visible(&*pool, 1, txns, &mut scan).unwrap()
        {
            rows.push(tuple.data);
        }
        rows
    }

    #[test]
    fn heap_insert_and_fetch_roundtrip() {
        let base = temp_dir("insert_fetch_roundtrip");
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        create_fork(&pool, rel(5000));
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
            let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
            let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
            create_fork(&pool, rel);
            let tuple = HeapTuple::new_raw(2, b"persisted-tuple".to_vec());
            let tid = heap_insert(&pool, 1, rel, &tuple).unwrap();
            heap_flush(&pool, 1, rel, tid.block_number).unwrap();
            tid
        };

        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let fetched = heap_fetch(&pool, 2, rel, tid).unwrap();
        assert_eq!(fetched.data, b"persisted-tuple".to_vec());
    }

    #[test]
    fn heap_insert_spills_to_new_page_when_full() {
        let base = temp_dir("spill_to_new_page");
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5002);
        create_fork(&pool, rel);

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
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5003);
        create_fork(&pool, rel);

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
        let mut smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut page = [0u8; crate::BLCKSZ];
        heap_page_init(&mut page);
        let _ =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"first".to_vec())).unwrap();
        let off2 =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"second".to_vec())).unwrap();

        // Mark the second line pointer unused to simulate a hole on the page.
        let idx = crate::backend::storage::page::bufpage::max_align(
            crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
        ) + (usize::from(off2) - 1)
            * crate::backend::storage::page::bufpage::ITEM_ID_SIZE;
        page[idx..idx + crate::backend::storage::page::bufpage::ITEM_ID_SIZE].copy_from_slice(
            &crate::backend::storage::page::bufpage::ItemIdData::unused().encode(),
        );

        smgr.extend(rel, ForkNumber::Main, 0, &page, true).unwrap();

        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let mut scan = heap_scan_begin(&pool, rel).unwrap();

        let first = heap_scan_next(&pool, 1, &mut scan).unwrap().unwrap();
        assert_eq!(first.1.data, b"first".to_vec());
        assert!(heap_scan_next(&pool, 1, &mut scan).unwrap().is_none());
    }

    #[test]
    fn heap_insert_rejects_oversized_tuple() {
        let base = temp_dir("reject_oversized_tuple");
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let rel = rel(5010);
        create_fork(&pool, rel);

        let desc = vec![AttributeDesc {
            name: "payload".into(),
            attlen: -1,
            attalign: AttributeAlign::Int,
            attstorage: crate::include::access::htup::AttributeStorage::Extended,
            attcompression: crate::include::access::htup::AttributeCompression::Default,
            nullable: false,
        }];
        let tuple =
            HeapTuple::from_values(&desc, &[TupleValue::Bytes(vec![b'x'; MAX_HEAP_TUPLE_SIZE])])
                .unwrap();

        assert!(tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE);
        match heap_insert(&pool, 1, rel, &tuple) {
            Err(HeapError::Tuple(TupleError::Oversized { size, max_size })) => {
                assert_eq!(size, tuple.serialized_len());
                assert_eq!(max_size, MAX_HEAP_TUPLE_SIZE);
            }
            other => panic!("expected oversized tuple error, got {other:?}"),
        }
    }

    #[test]
    fn heap_delete_hides_tuple_after_commit() {
        let base = temp_dir("mvcc_delete");
        let rel = rel(5005);
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        create_fork(&pool, rel);
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
        let before_commit = heap_fetch_visible(&pool, 3, rel, tid, &txns, &other_snapshot).unwrap();
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
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        create_fork(&pool, rel);
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
        let old_visible = heap_fetch_visible(&pool, 3, rel, old_tid, &txns, &concurrent_snapshot)
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
        let new_visible = heap_fetch_visible(&pool, 4, rel, new_tid, &txns, &committed_snapshot)
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
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        create_fork(&*pool, rel);
        let mut txns = TransactionManager::default();

        let xid1 = txns.begin();
        let tid1 = heap_insert_mvcc(
            &*pool,
            1,
            rel,
            xid1,
            &HeapTuple::new_raw(1, b"first".to_vec()),
        )
        .unwrap();
        txns.commit(xid1).unwrap();

        let xid2 = txns.begin();
        let _tid2 = heap_update(
            &*pool,
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
        let mut scan = heap_scan_begin_visible(&pool, 3, rel, snapshot).unwrap();
        let mut rows = Vec::new();
        while let Some((_tid, tuple)) = heap_scan_next_visible(&*pool, 3, &txns, &mut scan).unwrap()
        {
            rows.push(tuple.data);
        }

        assert_eq!(rows, vec![b"second".to_vec()]);
    }

    #[test]
    fn mvcc_changes_can_live_in_buffer_cache_until_late_flush() {
        let base = temp_dir("mvcc_buffer_cache");
        let rel = rel(5008);
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        create_fork(&*pool, rel);
        let mut txns = TransactionManager::default();

        let insert_xid = txns.begin();
        let original_tid = heap_insert_mvcc(
            &*pool,
            1,
            rel,
            insert_xid,
            &HeapTuple::new_raw(1, b"old".to_vec()),
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();
        heap_flush(&*pool, 1, rel, original_tid.block_number).unwrap();

        let committed_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert_eq!(
            visible_tuple_payloads(&base, rel, &txns, committed_snapshot.clone()),
            vec![b"old".to_vec()]
        );

        let update_xid = txns.begin();
        let updated_tid = heap_update(
            &*pool,
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
        heap_delete(&*pool, 1, rel, &txns, delete_xid, updated_tid).unwrap();
        txns.commit(delete_xid).unwrap();

        // The writer's pool sees both committed changes immediately because it is
        // reading the dirty page out of shared buffers, not reloading from disk.
        let writer_view = heap_fetch_visible(
            &*pool,
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
            2,
            rel,
            txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        )
        .unwrap();
        assert!(
            heap_scan_next_visible(&*pool, 2, &txns, &mut writer_scan)
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
            let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
            let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
            create_fork(&pool, rel);
            let mut txns = TransactionManager::new_durable(&base).unwrap();

            let xid = txns.begin();
            let tid = heap_insert_mvcc(&pool, 1, rel, xid, &HeapTuple::new_raw(1, b"row".to_vec()))
                .unwrap();
            txns.commit(xid).unwrap();
            heap_flush(&pool, 1, rel, tid.block_number).unwrap();
            tid
        };

        let mut reopened_txns = TransactionManager::new_durable(&base).unwrap();
        let snapshot = reopened_txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
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
        drop(reopened_txns);

        let final_txns = TransactionManager::new_durable(&base).unwrap();
        let final_snapshot = final_txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let smgr = crate::backend::storage::smgr::MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        assert!(
            heap_fetch_visible(&pool, 3, rel, tid, &final_txns, &final_snapshot)
                .unwrap()
                .is_none()
        );
    }
}
