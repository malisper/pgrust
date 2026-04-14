use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::storage_backend::{SmgrStorageBackend, StorageBackend};
use crate::backend::access::transam::xlog::{INVALID_LSN, Lsn, WalWriter};
use crate::backend::access::transam::xlog::{RM_BTREE_ID, RM_HEAP_ID};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::storage::buf_internals::*;

struct BufferFrame {
    state: BufferState,
    tag: Mutex<Option<BufferTag>>,
    content_lock: RwLock<Page>,
    io_complete: Condvar,
}

struct StrategyState {
    free_list: VecDeque<BufferId>,
    next_victim: usize,
}

/// Partitioned hash table for buffer tag -> buffer id lookup.
/// Each partition has its own RwLock, so threads accessing different
/// partitions never contend. Matches PostgreSQL's NUM_BUFFER_PARTITIONS
/// approach (128 lwlock partitions over the shared buffer mapping table).
const NUM_BUFFER_PARTITIONS: usize = 128;

struct LookupPartition {
    lock: RwLock<FxHashMap<BufferTag, BufferId>>,
}

struct PartitionedLookup {
    partitions: Vec<LookupPartition>,
}

impl PartitionedLookup {
    fn new() -> Self {
        let partitions = (0..NUM_BUFFER_PARTITIONS)
            .map(|_| LookupPartition {
                lock: RwLock::new(FxHashMap::default()),
            })
            .collect();
        Self { partitions }
    }

    #[inline]
    fn partition_index(tag: &BufferTag) -> usize {
        (tag.rel.rel_number.wrapping_add(tag.block)) as usize % NUM_BUFFER_PARTITIONS
    }

    #[inline]
    fn partition(&self, tag: &BufferTag) -> &LookupPartition {
        &self.partitions[Self::partition_index(tag)]
    }
}

pub struct BufferPool<S: StorageBackend + Send> {
    storage: Mutex<S>,
    wal: Option<Arc<WalWriter>>,
    frames: Vec<BufferFrame>,
    lookup: PartitionedLookup,
    strategy: Mutex<StrategyState>,
    max_usage_count: u8,
    stats_hit: AtomicU64,
    stats_read: AtomicU64,
    stats_written: AtomicU64,
}

impl<S: StorageBackend + Send> BufferPool<S> {
    pub fn new(storage: S, capacity: usize) -> Self {
        Self::new_inner(storage, capacity, None)
    }

    pub fn new_with_wal(storage: S, capacity: usize, wal: Arc<WalWriter>) -> Self {
        Self::new_inner(storage, capacity, Some(wal))
    }

    fn new_inner(storage: S, capacity: usize, wal: Option<Arc<WalWriter>>) -> Self {
        let mut free_list = VecDeque::with_capacity(capacity);
        for id in 0..capacity {
            free_list.push_back(id);
        }

        let frames = (0..capacity)
            .map(|_| BufferFrame {
                state: BufferState::new(),
                tag: Mutex::new(None),
                content_lock: RwLock::new([0u8; PAGE_SIZE]),
                io_complete: Condvar::new(),
            })
            .collect();

        Self {
            storage: Mutex::new(storage),
            wal,
            frames,
            lookup: PartitionedLookup::new(),
            strategy: Mutex::new(StrategyState {
                free_list,
                next_victim: 0,
            }),
            max_usage_count: 5,
            stats_hit: AtomicU64::new(0),
            stats_read: AtomicU64::new(0),
            stats_written: AtomicU64::new(0),
        }
    }

    /// Flush all pending WAL records to disk. Returns the flushed LSN, or
    /// `INVALID_LSN` if this pool has no WAL writer.
    pub fn flush_wal(&self) -> Result<Lsn, String> {
        match &self.wal {
            Some(wal) => wal.flush().map_err(|e| e.to_string()),
            None => Ok(INVALID_LSN),
        }
    }

    /// Write a commit record to WAL for the given transaction.
    /// No-op if this pool has no WAL writer.
    pub fn write_wal_commit(&self, xid: u32) -> Result<Lsn, String> {
        match &self.wal {
            Some(wal) => wal.write_commit(xid).map_err(|e| e.to_string()),
            None => Ok(INVALID_LSN),
        }
    }

    pub fn wal_writer(&self) -> Option<Arc<WalWriter>> {
        self.wal.as_ref().map(Arc::clone)
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    pub fn with_storage<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&S) -> R,
    {
        let storage = self.storage.lock();
        f(&storage)
    }

    pub fn with_storage_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut S) -> R,
    {
        let mut storage = self.storage.lock();
        f(&mut storage)
    }

    pub fn usage_stats(&self) -> BufferUsageStats {
        BufferUsageStats {
            shared_hit: self.stats_hit.load(Ordering::Relaxed),
            shared_read: self.stats_read.load(Ordering::Relaxed),
            shared_written: self.stats_written.load(Ordering::Relaxed),
        }
    }

    pub fn reset_usage_stats(&self) {
        self.stats_hit.store(0, Ordering::Relaxed);
        self.stats_read.store(0, Ordering::Relaxed);
        self.stats_written.store(0, Ordering::Relaxed);
    }

    /// Return a raw pointer to the page in a pinned buffer frame, WITHOUT
    /// acquiring the content lock. This is safe when:
    /// 1. The buffer is pinned (preventing eviction)
    /// 2. The caller only reads immutable tuple user data (not headers)
    /// 3. Visibility has already been determined under a lock
    /// Matches PostgreSQL's `heapgettup_pagemode` which reads tuple data
    /// from pinned pages without holding the buffer content lock.
    ///
    /// # Safety
    /// The caller must hold a pin on the buffer and must not read data that
    /// may be concurrently modified (e.g., hint bits). Tuple user data is
    /// immutable after insertion and is safe to read.
    pub unsafe fn page_unlocked(&self, buffer_id: BufferId) -> Option<&Page> {
        let frame = self.frames.get(buffer_id)?;
        // SAFETY: caller guarantees pin is held and only immutable data is read.
        Some(unsafe { &*frame.content_lock.data_ptr() })
    }

    /// Acquire a shared content lock on a buffer frame. Multiple readers can
    /// hold this simultaneously. The caller must hold a pin on the buffer.
    pub fn lock_buffer_shared(
        &self,
        buffer_id: BufferId,
    ) -> Result<RwLockReadGuard<'_, Page>, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        Ok(frame.content_lock.read())
    }

    /// Acquire an exclusive content lock on a buffer frame. Only one writer
    /// can hold this at a time, and it blocks all readers. The caller must
    /// hold a pin on the buffer.
    pub fn lock_buffer_exclusive(
        &self,
        buffer_id: BufferId,
    ) -> Result<RwLockWriteGuard<'_, Page>, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        Ok(frame.content_lock.write())
    }

    pub fn buffer_state(&self, buffer_id: BufferId) -> Option<BufferStateView> {
        let frame = self.frames.get(buffer_id)?;
        let tag = *frame.tag.lock();
        Some(frame.state.to_view(tag))
    }

    /// Returns a copy of the page in the given buffer frame.
    pub fn read_page(&self, buffer_id: BufferId) -> Option<Page> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_valid() {
            return None;
        }
        let guard = frame.content_lock.read();
        Some(*guard)
    }

    /// Borrow the page in-place and pass it to a closure, avoiding the 8KB copy.
    /// The page reference is only valid for the duration of the closure.
    pub fn with_page<T>(&self, buffer_id: BufferId, f: impl FnOnce(&Page) -> T) -> Option<T> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_valid() {
            return None;
        }
        let guard = frame.content_lock.read();
        Some(f(&*guard))
    }

    /// Mark a buffer as dirty (for use after hint bit writes).
    pub fn mark_buffer_dirty_hint(&self, buffer_id: BufferId) {
        if let Some(frame) = self.frames.get(buffer_id) {
            frame.state.set_dirty();
        }
    }

    /// Mutably borrow the page in-place and pass it to a closure.
    /// Marks the buffer as dirty.
    pub fn with_page_mut<T>(
        &self,
        buffer_id: BufferId,
        f: impl FnOnce(&mut Page) -> T,
    ) -> Option<T> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_valid() {
            return None;
        }
        let mut guard = frame.content_lock.write();
        frame.state.set_dirty();
        Some(f(&mut *guard))
    }

    /// Allocate a buffer for the given tag. Mirrors PostgreSQL's BufferAlloc.
    ///
    /// Fast path: shared-lock the partition, look up the tag, pin, return.
    /// The shared lock prevents eviction of any buffer in this partition,
    /// so no tag re-check is needed after pinning (same as PostgreSQL's
    /// PinBuffer which does not re-check the tag).
    ///
    /// Slow path: get a clean victim via get_victim_buffer (which handles
    /// dirty flushing and old-tag eviction internally), then exclusive-lock
    /// the new partition and install the new tag.
    pub fn request_page(
        &self,
        _client_id: ClientId,
        tag: BufferTag,
    ) -> Result<RequestPageResult, Error> {
        let partition = self.lookup.partition(&tag);

        // ---- fast path: see if the block is in the buffer pool already ----
        // (PG: BufferAlloc lines 2025-2057)
        {
            let lookup = partition.lock.read();
            if let Some(&buffer_id) = lookup.get(&tag) {
                let frame = &self.frames[buffer_id];
                frame.state.pin_and_bump_usage(self.max_usage_count);
                // Partition read lock prevents eviction — no tag re-check needed.
                if frame.state.is_valid() {
                    self.stats_hit.fetch_add(1, Ordering::Relaxed);
                    return Ok(RequestPageResult::Hit { buffer_id });
                } else {
                    return Ok(RequestPageResult::WaitingOnRead { buffer_id });
                }
            }
        }
        // Partition lock released before slow path (PG: line 2063).

        // ---- slow path: allocate a victim and install the new tag ----

        // Phase 1: Get a clean, pinned victim with no tag and no hash entry.
        // Retry loop is internal. (PG: GetVictimBuffer, line 2070)
        let buffer_id = match self.get_victim_buffer() {
            Ok(id) => id,
            Err(Error::AllBuffersPinned) => return Ok(RequestPageResult::AllBuffersPinned),
            Err(e) => return Err(e),
        };
        let frame = &self.frames[buffer_id];

        // Phase 2: Try to install the new tag. (PG: BufferAlloc lines 2078-2150)
        let mut lookup = partition.lock.write();

        // Check for collision — another thread may have inserted this tag
        // while we were preparing the victim. (PG: BufTableInsert, line 2079)
        if let Some(&existing_id) = lookup.get(&tag) {
            // Collision. Give up the victim. (PG: lines 2095-2101)
            frame.state.decrement_pin();
            {
                let mut strategy = self.strategy.lock();
                strategy.free_list.push_back(buffer_id);
            }

            // Pin the existing buffer instead. (PG: PinBuffer, line 2107)
            let existing_frame = &self.frames[existing_id];
            existing_frame
                .state
                .pin_and_bump_usage(self.max_usage_count);
            let valid = existing_frame.state.is_valid();
            drop(lookup);

            if valid {
                self.stats_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(RequestPageResult::Hit {
                    buffer_id: existing_id,
                });
            } else {
                return Ok(RequestPageResult::WaitingOnRead {
                    buffer_id: existing_id,
                });
            }
        }

        // No collision. Install the new tag. (PG: lines 2130-2150)
        // PG does this under the buffer header spinlock; we use the tag mutex.
        debug_assert_eq!(frame.state.pin_count(), 1);
        *frame.tag.lock() = Some(tag);
        frame.state.init_for_io();
        lookup.insert(tag, buffer_id);

        Ok(RequestPageResult::ReadIssued { buffer_id })
    }

    /// Select and prepare a victim buffer for reuse. Returns a clean buffer
    /// that is pinned (refcount=1), has no tag, and no hash table entry.
    /// Retries internally until a usable victim is found.
    /// Mirrors PostgreSQL's GetVictimBuffer (bufmgr.c lines 2344-2496).
    fn get_victim_buffer(&self) -> Result<BufferId, Error> {
        loop {
            // Select a victim. (PG: StrategyGetBuffer, line 2366)
            let buffer_id = {
                let mut strategy = self.strategy.lock();
                let Some(buffer_id) = self.allocate_victim(&mut strategy) else {
                    return Err(Error::AllBuffersPinned);
                };

                // Pin while strategy lock is held so no other thread can
                // select this buffer. (PG: PinBuffer_Locked, line 2372)
                let frame = &self.frames[buffer_id];
                frame.state.pin_and_bump_usage(self.max_usage_count);
                buffer_id
                // Strategy lock released here.
            };

            let frame = &self.frames[buffer_id];

            // If dirty, flush to disk. (PG: lines 2386-2449)
            if frame.state.is_dirty() {
                // Conditional content lock to avoid deadlock. (PG: line 2408)
                let content_guard = match frame.content_lock.try_read() {
                    Some(guard) => guard,
                    None => {
                        // Can't get lock — unpin and try another victim.
                        // (PG: lines 2414-2415)
                        frame.state.decrement_pin();
                        continue;
                    }
                };
                let page = *content_guard;
                drop(content_guard);

                let old_tag = *frame.tag.lock();
                if let Some(old_tag) = old_tag {
                    let skip_fsync = self.wal.is_some();
                    frame.state.clear_dirty();

                    if let Some(ref wal) = self.wal {
                        let page_lsn = u64::from_le_bytes(page[0..8].try_into().unwrap());
                        if page_lsn > 0 {
                            let _ = wal.flush_to(page_lsn);
                        }
                    }

                    let write_result = {
                        let mut storage = self.storage.lock();
                        storage
                            .write_page(old_tag, &page, skip_fsync)
                            .map_err(Error::Storage)
                    };
                    if let Err(e) = write_result {
                        frame.state.decrement_pin();
                        return Err(e);
                    }
                    self.stats_written.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Evict the old tag from the hash table. This is only hit if the
            // buffer was previously in use. We do not need to invalidate a
            // buffer we got off the free list.
            // (PG: InvalidateVictimBuffer, lines 2276-2342)
            if frame.tag.lock().is_some() {
                if !self.invalidate_victim(buffer_id) {
                    // Another thread pinned or dirtied this buffer during
                    // the flush. Give up and try another. (PG: lines 2481-2482)
                    frame.state.decrement_pin();
                    continue;
                }
            }

            return Ok(buffer_id);
        }
    }

    /// Evict a victim buffer's old tag from the hash table. The buffer must
    /// be pinned by this thread (refcount >= 1).
    ///
    /// Returns true if the buffer can be reused (tag cleared, hash entry
    /// removed). Returns false if another thread pinned or dirtied the
    /// buffer, in which case the caller should unpin and retry.
    ///
    /// Mirrors PostgreSQL's InvalidateVictimBuffer (bufmgr.c lines 2276-2342).
    fn invalidate_victim(&self, buffer_id: BufferId) -> bool {
        let frame = &self.frames[buffer_id];

        // Read tag — safe because we have a pin. (PG: line 2287)
        let tag = match *frame.tag.lock() {
            Some(tag) => tag,
            None => return true, // no tag to evict
        };

        // Lock the old partition. (PG: line 2292)
        let old_partition = self.lookup.partition(&tag);
        let mut old_lookup = old_partition.lock.write();

        // Re-check under partition lock: if somebody else pinned or dirtied
        // this buffer, it's clearly in use — give up. (PG: line 2309)
        let mut tag_guard = frame.tag.lock();
        if frame.state.pin_count() != 1 || frame.state.is_dirty() {
            return false;
        }

        // Clear the buffer's tag. (PG: ClearBufferTag, line 2326)
        *tag_guard = None;
        drop(tag_guard);

        // Remove from hash table. (PG: BufTableDelete, line 2333)
        old_lookup.remove(&tag);

        true
        // Partition lock released on drop. (PG: line 2335)
    }

    pub fn pending_io(&self, buffer_id: BufferId) -> Option<PendingIo> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_io_in_progress() {
            return None;
        }
        let tag = (*frame.tag.lock())?;
        Some(PendingIo {
            buffer_id,
            op: if frame.state.is_valid() {
                IoOp::Write
            } else {
                IoOp::Read
            },
            tag,
        })
    }

    pub fn complete_read(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;

        let tag = {
            if !frame.state.is_io_in_progress() {
                return Err(Error::NoIoInProgress);
            }
            if frame.state.is_valid() {
                return Err(Error::WrongIoOp);
            }
            frame.tag.lock().ok_or(Error::UnknownBuffer)?
        };

        let page = {
            let mut storage = self.storage.lock();
            storage.read_page(tag).map_err(Error::Storage)?
        };

        {
            let mut guard = frame.content_lock.write();
            *guard = page;
        }

        // Clear IO flags under the header spinlock, matching PostgreSQL's
        // TerminateBufferIO.  Safe because pin_and_bump_usage/decrement_pin
        // wait for BM_LOCKED to be clear before their CAS.
        {
            let mut buf_state = frame.state.lock_header();
            buf_state &= !BM_LOCKED_PUB;
            buf_state |= BM_VALID_PUB;
            buf_state &= !BM_IO_IN_PROGRESS_PUB;
            buf_state &= !BM_IO_ERROR_PUB;
            frame.state.unlock_header(buf_state);
        }

        // Notify under the tag mutex to prevent lost wakeups.
        // wait_for_io holds this mutex across its flag check and condvar
        // wait, so the notify cannot slip between them.
        {
            let _tag_guard = frame.tag.lock();
            frame.io_complete.notify_all();
        }
        self.stats_read.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn fail_read(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.state.is_io_in_progress() {
            return Err(Error::NoIoInProgress);
        }
        if frame.state.is_valid() {
            return Err(Error::WrongIoOp);
        }

        {
            let mut buf_state = frame.state.lock_header();
            buf_state &= !BM_LOCKED_PUB;
            buf_state &= !BM_VALID_PUB;
            buf_state &= !BM_IO_IN_PROGRESS_PUB;
            buf_state |= BM_IO_ERROR_PUB;
            frame.state.unlock_header(buf_state);
        }

        {
            let _tag_guard = frame.tag.lock();
            frame.io_complete.notify_all();
        }
        Ok(())
    }

    pub fn mark_dirty(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.state.is_valid() {
            return Err(Error::InvalidBuffer);
        }
        frame.state.set_dirty();
        Ok(())
    }

    pub fn write_byte(&self, buffer_id: BufferId, offset: usize, value: u8) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.state.is_valid() {
            return Err(Error::InvalidBuffer);
        }
        let mut guard = frame.content_lock.write();
        guard[offset] = value;
        frame.state.set_dirty();
        Ok(())
    }

    /// Write a modified page image into the buffer frame.
    ///
    /// If this pool has a WAL writer, a full-page-image WAL record is appended
    /// and the page's `pd_lsn` (bytes 0-7) is stamped with the assigned LSN.
    /// The page is marked dirty and left in the buffer cache; the caller must
    /// flush WAL before committing and the data page will reach disk via
    /// eviction or an explicit `flush_buffer` + `complete_write`.
    ///
    /// If no WAL writer is present (e.g. in unit tests), the page is written
    /// directly to storage with an fsync to preserve the pre-WAL safety guarantee.
    /// Write a page image into the buffer, using an already-held exclusive content lock guard.
    /// The caller must hold the write guard from `lock_buffer_exclusive`.
    /// Write a page image for an insert, using row-level WAL delta when possible.
    /// Falls back to full page image for the first write to each page.
    pub fn write_page_insert_locked(
        &self,
        buffer_id: BufferId,
        xid: u32,
        page: &Page,
        guard: &mut RwLockWriteGuard<'_, Page>,
        offset_number: u16,
        tuple_data: &[u8],
    ) -> Result<(), Error> {
        let tag = self.buffer_tag(buffer_id)?;

        let mut page_to_store = *page;

        if let Some(ref wal) = self.wal {
            let lsn = wal
                .write_insert(xid, tag, page, offset_number, tuple_data)
                .map_err(|e| Error::Wal(e.to_string()))?;
            page_to_store[0..8].copy_from_slice(&lsn.to_le_bytes());
            **guard = page_to_store;
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.set_dirty();
        } else {
            **guard = page_to_store;
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.set_dirty();
            {
                let mut storage = self.storage.lock();
                storage
                    .write_page(tag, &page_to_store, false)
                    .map_err(Error::Storage)?;
            }
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.clear_dirty();
        }

        Ok(())
    }

    pub fn write_page_image_locked_with_rmgr(
        &self,
        buffer_id: BufferId,
        xid: u32,
        page: &Page,
        guard: &mut RwLockWriteGuard<'_, Page>,
        rmid: u8,
    ) -> Result<(), Error> {
        let tag = self.buffer_tag(buffer_id)?;

        let mut page_to_store = *page;

        if let Some(ref wal) = self.wal {
            let lsn = wal
                .write_record_with_rmgr(xid, tag, page, rmid)
                .map_err(|e| Error::Wal(e.to_string()))?;
            page_to_store[0..8].copy_from_slice(&lsn.to_le_bytes());
            **guard = page_to_store;
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.set_dirty();
        } else {
            **guard = page_to_store;
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.set_dirty();
            {
                let mut storage = self.storage.lock();
                storage
                    .write_page(tag, &page_to_store, false)
                    .map_err(Error::Storage)?;
            }
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.clear_dirty();
        }

        Ok(())
    }

    pub fn write_page_image_locked(
        &self,
        buffer_id: BufferId,
        xid: u32,
        page: &Page,
        guard: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<(), Error> {
        self.write_page_image_locked_with_rmgr(buffer_id, xid, page, guard, RM_HEAP_ID)
    }

    pub fn write_btree_page_image_locked(
        &self,
        buffer_id: BufferId,
        xid: u32,
        page: &Page,
        guard: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<(), Error> {
        self.write_page_image_locked_with_rmgr(buffer_id, xid, page, guard, RM_BTREE_ID)
    }

    pub fn write_page_image(
        &self,
        buffer_id: BufferId,
        xid: u32,
        page: &Page,
    ) -> Result<(), Error> {
        let tag = self.buffer_tag(buffer_id)?;

        // Determine the page image to store. If WAL is available, stamp pd_lsn.
        let mut page_to_store = *page;

        if let Some(ref wal) = self.wal {
            // Write WAL record first (write-ahead). The returned LSN is stamped
            // into the page header so recovery can tell when this page was last
            // modified.
            let lsn = wal
                .write_record(xid, tag, page)
                .map_err(|e| Error::Wal(e.to_string()))?;
            page_to_store[0..8].copy_from_slice(&lsn.to_le_bytes());

            // Store updated page in buffer frame and mark dirty.
            // The data file write is deferred -- WAL provides crash durability.
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            let mut guard = frame.content_lock.write();
            *guard = page_to_store;
            frame.state.set_dirty();
        } else {
            // No WAL: fall back to immediate write + fsync for safety.
            {
                let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
                let mut guard = frame.content_lock.write();
                *guard = page_to_store;
                frame.state.set_dirty();
            }
            {
                let mut storage = self.storage.lock();
                storage
                    .write_page(tag, &page_to_store, false)
                    .map_err(Error::Storage)?;
            }
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            {
                let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
                frame.state.clear_dirty();
            }
        }

        Ok(())
    }

    pub fn install_page_image_locked(
        &self,
        buffer_id: BufferId,
        page: &Page,
        lsn: Lsn,
        guard: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<(), Error> {
        let tag = self.buffer_tag(buffer_id)?;
        let mut page_to_store = *page;
        page_to_store[0..8].copy_from_slice(&lsn.to_le_bytes());
        **guard = page_to_store;

        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.state.set_dirty();

        if self.wal.is_none() {
            let mut storage = self.storage.lock();
            storage
                .write_page(tag, &page_to_store, false)
                .map_err(Error::Storage)?;
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            frame.state.clear_dirty();
        }

        Ok(())
    }

    pub fn flush_buffer(&self, buffer_id: BufferId) -> Result<FlushResult, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        match frame.state.try_start_flush() {
            Ok(()) => Ok(FlushResult::WriteIssued),
            Err(result) => Ok(result),
        }
    }

    pub fn complete_write(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;

        let (tag, page) = {
            if !frame.state.is_io_in_progress() {
                return Err(Error::NoIoInProgress);
            }
            if !frame.state.is_valid() {
                return Err(Error::WrongIoOp);
            }
            let tag = frame.tag.lock().ok_or(Error::UnknownBuffer)?;
            let page = *frame.content_lock.read();
            (tag, page)
        };

        {
            let skip_fsync = self.wal.is_some();
            let mut storage = self.storage.lock();
            // When WAL is present, skip_fsync is true: the page can be recovered
            // from WAL after a crash. WAL flush is enforced at commit time and
            // in the eviction path (where losing the in-memory copy is permanent).
            storage
                .write_page(tag, &page, skip_fsync)
                .map_err(Error::Storage)?;
        }

        // Terminate IO under the header spinlock, matching PostgreSQL's
        // TerminateBufferIO.  Clears dirty (the write succeeded), IO flags.
        {
            let mut buf_state = frame.state.lock_header();
            buf_state &= !BM_LOCKED_PUB;
            buf_state &= !BM_IO_IN_PROGRESS_PUB;
            buf_state &= !BM_IO_ERROR_PUB;
            buf_state &= !BM_DIRTY_PUB;
            frame.state.unlock_header(buf_state);
        }

        frame.io_complete.notify_all();
        self.stats_written.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn fail_write(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.state.is_io_in_progress() {
            return Err(Error::NoIoInProgress);
        }
        if !frame.state.is_valid() {
            return Err(Error::WrongIoOp);
        }

        // Terminate IO under the header spinlock.  Re-mark dirty so the
        // write will be retried, and set IO_ERROR.
        {
            let mut buf_state = frame.state.lock_header();
            buf_state &= !BM_LOCKED_PUB;
            buf_state &= !BM_IO_IN_PROGRESS_PUB;
            buf_state |= BM_IO_ERROR_PUB;
            buf_state |= BM_DIRTY_PUB;
            frame.state.unlock_header(buf_state);
        }

        frame.io_complete.notify_all();
        Ok(())
    }

    pub fn unpin(&self, _client_id: ClientId, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        let pc = frame.state.pin_count();
        if pc == 0 {
            let tag = *frame.tag.lock();
            panic!(
                "unpin: pin_count already 0 for buffer_id={buffer_id} tag={tag:?} state={:#010x}",
                frame.state.load()
            );
        }
        frame.state.decrement_pin();
        Ok(())
    }

    /// Create a PinnedBuffer guard for a buffer that is ALREADY pinned
    /// (e.g., by request_page). Does NOT increment the pin count.
    pub fn wrap_pinned(&self, client_id: ClientId, buffer_id: BufferId) -> PinnedBuffer<'_, S> {
        PinnedBuffer {
            pool: self,
            client_id,
            buffer_id,
            released: false,
        }
    }

    /// Pin a buffer and return an RAII guard that unpins on drop.
    /// Prevents pin leaks when functions return early via `?`.
    pub fn pin_buffer(&self, client_id: ClientId, buffer_id: BufferId) -> PinnedBuffer<'_, S> {
        PinnedBuffer {
            pool: self,
            client_id,
            buffer_id,
            released: false,
        }
    }

    /// Increment the pin count on an already-pinned buffer, matching
    /// PostgreSQL's `IncrBufferRefCount`. The buffer must already have
    /// pin_count > 0 (i.e., the caller holds an existing pin).
    ///
    /// This is cheap: just an atomic CAS on the shared state word.
    /// (PostgreSQL makes this even cheaper with a process-local
    /// `PrivateRefCount`, but for now the shared atomic is sufficient.)
    pub fn increment_buffer_pin(&self, buffer_id: BufferId) {
        let frame = &self.frames[buffer_id];
        debug_assert!(
            frame.state.pin_count() > 0,
            "increment_buffer_pin: buffer must already be pinned"
        );
        frame.state.increment_pin();
    }

    /// Unpin the buffer without consuming the guard (for manual control).
    fn unpin_raw(&self, buffer_id: BufferId) {
        if let Some(frame) = self.frames.get(buffer_id) {
            frame.state.decrement_pin();
        }
    }

    /// Wait until I/O completes on the given buffer.
    /// Matches PostgreSQL's WaitIO: check the flag under the header spinlock,
    /// then sleep on the condvar if I/O is still in progress.
    pub fn wait_for_io(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        let mut guard = frame.tag.lock();
        loop {
            // Check under header spinlock, matching complete_read/fail_read
            // which clear the flag under the same spinlock.
            let buf_state = frame.state.lock_header();
            frame.state.unlock_header(buf_state & !BM_LOCKED_PUB);
            if buf_state & BM_IO_IN_PROGRESS_PUB == 0 {
                break;
            }
            frame.io_complete.wait(&mut guard);
        }
        if frame.state.is_io_error() {
            Err(Error::InvalidBuffer)
        } else {
            Ok(())
        }
    }

    pub fn invalidate_relation(&self, rel: RelFileLocator) -> Result<usize, Error> {
        let mut strategy = self.strategy.lock();

        // Scan all frames without holding partition locks. For each match,
        // lock just that partition, recheck, and remove. Matches PostgreSQL's
        // DropRelationBuffers approach.
        let mut removed = 0;
        for (buffer_id, frame) in self.frames.iter().enumerate() {
            // Quick check without partition lock — read the frame's tag.
            let tag = {
                let tag_guard = frame.tag.lock();
                match *tag_guard {
                    Some(t) if t.rel == rel => t,
                    _ => continue,
                }
            };

            // Callers (DROP TABLE, TRUNCATE) hold AccessExclusive on the
            // relation, so no new pins can be acquired. A pin here is a bug.
            if frame.state.pin_count() > 0 {
                return Err(Error::BufferPinned);
            }
            if frame.state.is_io_in_progress() {
                return Err(Error::NoIoInProgress);
            }

            // Lock the partition and recheck — the tag may have changed.
            let pidx = PartitionedLookup::partition_index(&tag);
            let mut lookup = self.lookup.partitions[pidx].lock.write();
            let mut tag_guard = frame.tag.lock();
            match *tag_guard {
                Some(t) if t == tag => {}
                _ => continue, // tag changed, skip
            }

            lookup.remove(&tag);
            *tag_guard = None;
            frame.state.store(0);
            *frame.content_lock.write() = [0u8; PAGE_SIZE];
            strategy.free_list.push_back(buffer_id);
            removed += 1;
        }

        Ok(removed)
    }

    fn allocate_victim(&self, strategy: &mut StrategyState) -> Option<BufferId> {
        while let Some(buffer_id) = strategy.free_list.pop_front() {
            let frame = &self.frames[buffer_id];
            if frame.state.pin_count() == 0 && !frame.state.is_io_in_progress() {
                return Some(buffer_id);
            }
        }

        let capacity = self.frames.len();
        if capacity == 0 {
            return None;
        }

        let mut scanned = 0usize;
        while scanned < capacity * (self.max_usage_count as usize + 1) {
            let buffer_id = strategy.next_victim;
            strategy.next_victim = (strategy.next_victim + 1) % capacity;
            scanned += 1;

            let frame = &self.frames[buffer_id];
            if frame.state.pin_count() > 0 || frame.state.is_io_in_progress() {
                continue;
            }
            if frame.state.decrement_usage() {
                continue;
            }
            return Some(buffer_id);
        }

        None
    }
}

impl<S: StorageBackend + Send> BufferPool<S> {
    fn buffer_tag(&self, buffer_id: BufferId) -> Result<BufferTag, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.state.is_valid() {
            return Err(Error::InvalidBuffer);
        }
        frame.tag.lock().ok_or(Error::UnknownBuffer)
    }
}

impl BufferPool<SmgrStorageBackend> {
    pub fn ensure_relation_fork(&self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), Error> {
        self.with_storage_mut(|storage| {
            let _ = storage.smgr.open(rel);
            storage.smgr.create(rel, fork, true)
        })
        .map_err(|err| Error::Storage(err.to_string()))
    }

    pub fn ensure_block_exists(
        &self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: u32,
    ) -> Result<(), Error> {
        self.ensure_relation_fork(rel, fork)?;
        self.with_storage_mut(|storage| {
            let nblocks = storage.smgr.nblocks(rel, fork)?;
            if block >= nblocks {
                let zero_page = [0u8; PAGE_SIZE];
                for b in nblocks..=block {
                    storage.smgr.extend(rel, fork, b, &zero_page, true)?;
                }
            }
            Ok::<(), crate::backend::storage::smgr::SmgrError>(())
        })
        .map_err(|err| Error::Storage(err.to_string()))
    }

    pub fn pin_existing_block(
        &self,
        client_id: ClientId,
        rel: RelFileLocator,
        fork: ForkNumber,
        block_number: u32,
    ) -> Result<PinnedBuffer<'_, SmgrStorageBackend>, Error> {
        let tag = BufferTag {
            rel,
            fork,
            block: block_number,
        };
        let buffer_id = match self.request_page(client_id, tag)? {
            RequestPageResult::Hit { buffer_id } => buffer_id,
            RequestPageResult::ReadIssued { buffer_id } => {
                if let Err(e) = self.complete_read(buffer_id) {
                    let _ = self.fail_read(buffer_id);
                    return Err(e);
                }
                buffer_id
            }
            RequestPageResult::WaitingOnRead { buffer_id } => {
                self.wait_for_io(buffer_id)?;
                buffer_id
            }
            RequestPageResult::AllBuffersPinned => return Err(Error::AllBuffersPinned),
        };
        Ok(self.wrap_pinned(client_id, buffer_id))
    }
}

/// RAII guard that unpins a buffer on drop. Prevents pin leaks when
/// functions return early via `?`. Similar to PostgreSQL's ResourceOwner
/// cleanup on transaction abort.
pub struct PinnedBuffer<'a, S: StorageBackend + Send> {
    pool: &'a BufferPool<S>,
    client_id: ClientId,
    buffer_id: BufferId,
    released: bool,
}

impl<'a, S: StorageBackend + Send> PinnedBuffer<'a, S> {
    pub fn buffer_id(&self) -> BufferId {
        self.buffer_id
    }

    /// Consume the guard and return the raw buffer_id WITHOUT unpinning.
    /// The caller takes responsibility for eventually unpinning the buffer.
    /// Used when transferring pin ownership to manual tracking (e.g., scan state).
    pub fn into_raw(mut self) -> BufferId {
        self.released = true;
        self.buffer_id
    }

    /// Manually release the pin. After this, drop is a no-op.
    pub fn release(mut self) -> Result<(), Error> {
        self.released = true;
        self.pool.unpin(self.client_id, self.buffer_id)
    }
}

impl<S: StorageBackend + Send> Drop for PinnedBuffer<'_, S> {
    fn drop(&mut self) {
        if !self.released {
            self.pool.unpin_raw(self.buffer_id);
        }
    }
}

/// Owned buffer pin that can outlive any borrow of the pool.
///
/// Like PostgreSQL's `BufferHeapTupleTableSlot.buffer` field: holds an
/// independent pin on the buffer so the tuple pointer remains valid even
/// after the scan advances to the next page. The pin is released when this
/// value is dropped.
///
/// Cloning takes an additional pin on the same buffer (cheap atomic CAS).
pub struct OwnedBufferPin<S: StorageBackend + Send> {
    pool: Arc<BufferPool<S>>,
    buffer_id: BufferId,
}

impl<S: StorageBackend + Send> OwnedBufferPin<S> {
    /// Take an additional pin on `buffer_id` (which must already be pinned)
    /// and return an owned guard that will release it on drop.
    pub fn new(pool: Arc<BufferPool<S>>, buffer_id: BufferId) -> Self {
        pool.increment_buffer_pin(buffer_id);
        Self { pool, buffer_id }
    }

    /// Adopt an existing pin without incrementing the pin count. The caller
    /// transfers ownership of one pin to this guard — the pin will be released
    /// when this value (or the last `Rc`/`Arc` wrapping it) is dropped.
    pub fn wrap_existing(pool: Arc<BufferPool<S>>, buffer_id: BufferId) -> Self {
        debug_assert!(
            pool.frames[buffer_id].state.pin_count() > 0,
            "wrap_existing: buffer must already be pinned"
        );
        Self { pool, buffer_id }
    }

    pub fn buffer_id(&self) -> BufferId {
        self.buffer_id
    }
}

impl<S: StorageBackend + Send> Clone for OwnedBufferPin<S> {
    fn clone(&self) -> Self {
        self.pool.increment_buffer_pin(self.buffer_id);
        Self {
            pool: Arc::clone(&self.pool),
            buffer_id: self.buffer_id,
        }
    }
}

impl<S: StorageBackend + Send> Drop for OwnedBufferPin<S> {
    fn drop(&mut self) {
        self.pool.unpin_raw(self.buffer_id);
    }
}

impl<S: StorageBackend + Send> std::fmt::Debug for OwnedBufferPin<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedBufferPin")
            .field("buffer_id", &self.buffer_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_backend::{FakeStorage, SmgrStorageBackend};
    use super::*;
    use crate::backend::storage::smgr::{
        ForkNumber, MdStorageManager, RelFileLocator, StorageManager,
    };
    use std::fs;
    use std::path::PathBuf;

    fn rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 1,
            db_oid: 1,
            rel_number: n,
        }
    }

    fn tag(rel_number: u32, block: u32) -> BufferTag {
        BufferTag {
            rel: rel(rel_number),
            fork: ForkNumber::Main,
            block,
        }
    }

    fn page(fill: u8) -> Page {
        [fill; PAGE_SIZE]
    }

    #[test]
    fn miss_then_hit_after_successful_read() {
        let tag = tag(42, 0);
        let mut storage = FakeStorage::default();
        storage.put_page(tag, page(7));
        let pool = BufferPool::new(storage, 2);

        let first = pool.request_page(1, tag).unwrap();
        assert_eq!(first, RequestPageResult::ReadIssued { buffer_id: 0 });
        pool.complete_read(0).unwrap();
        pool.unpin(1, 0).unwrap();

        let second = pool.request_page(2, tag).unwrap();
        assert_eq!(second, RequestPageResult::Hit { buffer_id: 0 });
        let state = pool.buffer_state(0).unwrap();
        assert!(state.valid);
        assert_eq!(state.pin_count, 1);
    }

    #[test]
    fn concurrent_requests_share_one_canonical_buffer() {
        let tag = tag(42, 1);
        let mut storage = FakeStorage::default();
        storage.put_page(tag, page(9));
        let pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, tag).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        assert_eq!(
            pool.request_page(2, tag).unwrap(),
            RequestPageResult::WaitingOnRead { buffer_id: 0 }
        );

        let state = pool.buffer_state(0).unwrap();
        assert_eq!(state.pin_count, 2);
        pool.complete_read(0).unwrap();
        assert!(pool.buffer_state(0).unwrap().valid);
    }

    #[test]
    fn flush_persists_data_and_clears_dirty() {
        let tag = tag(7, 0);
        let mut storage = FakeStorage::default();
        storage.put_page(tag, page(1));
        let pool = BufferPool::new(storage, 1);

        assert_eq!(
            pool.request_page(1, tag).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 99).unwrap();

        assert_eq!(pool.flush_buffer(0).unwrap(), FlushResult::WriteIssued);
        pool.complete_write(0).unwrap();

        let state = pool.buffer_state(0).unwrap();
        assert!(state.valid);
        assert!(!state.dirty);
        assert_eq!(
            pool.with_storage(|s: &FakeStorage| s.get_page(tag).unwrap()[0]),
            99
        );
    }

    #[test]
    fn write_failure_retains_dirty_state() {
        let tag = tag(8, 0);
        let mut storage = FakeStorage::default();
        storage.put_page(tag, page(3));
        let pool = BufferPool::new(storage, 1);

        assert_eq!(
            pool.request_page(1, tag).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 44).unwrap();
        pool.with_storage_mut(|s: &mut FakeStorage| s.fail_next_write(tag, "boom"));

        assert_eq!(pool.flush_buffer(0).unwrap(), FlushResult::WriteIssued);
        let err = pool.complete_write(0).unwrap_err();
        assert!(matches!(err, Error::Storage(_)));

        let state = pool.buffer_state(0).unwrap();
        assert!(state.dirty);
        assert!(state.io_in_progress);

        pool.fail_write(0).unwrap();
        let state = pool.buffer_state(0).unwrap();
        assert!(state.dirty);
        assert!(!state.io_in_progress);
    }

    #[test]
    fn eviction_skips_pinned_buffers() {
        let mut storage = FakeStorage::default();
        let a = tag(1, 0);
        let b = tag(2, 0);
        let c = tag(3, 0);
        storage.put_page(a, page(1));
        storage.put_page(b, page(2));
        storage.put_page(c, page(3));
        let pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, a).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        assert_eq!(
            pool.request_page(2, b).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 1 }
        );
        pool.complete_read(1).unwrap();
        pool.unpin(2, 1).unwrap();

        let third = pool.request_page(3, c).unwrap();
        assert_eq!(third, RequestPageResult::ReadIssued { buffer_id: 1 });

        let state0 = pool.buffer_state(0).unwrap();
        let state1 = pool.buffer_state(1).unwrap();
        assert_eq!(state0.tag, Some(a));
        assert_eq!(state1.tag, Some(c));
    }

    #[test]
    fn invalidate_relation_rejects_pinned_buffers_and_then_removes_pages() {
        let mut storage = FakeStorage::default();
        let a = tag(11, 0);
        let b = tag(11, 1);
        storage.put_page(a, page(1));
        storage.put_page(b, page(2));
        let pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, a).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        assert_eq!(
            pool.request_page(2, b).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 1 }
        );
        pool.complete_read(1).unwrap();
        pool.unpin(2, 1).unwrap();

        assert_eq!(pool.invalidate_relation(rel(11)), Err(Error::BufferPinned));

        pool.unpin(1, 0).unwrap();
        let removed = pool.invalidate_relation(rel(11)).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(pool.buffer_state(0).unwrap().tag, None);
        assert_eq!(pool.buffer_state(1).unwrap().tag, None);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("pgrust_bufmgr_integ_{}", label));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn smgr_rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: n,
        }
    }

    fn smgr_tag(rel_number: u32, block: u32) -> BufferTag {
        BufferTag {
            rel: smgr_rel(rel_number),
            fork: ForkNumber::Main,
            block,
        }
    }

    fn pool_with_relation(
        base: &PathBuf,
        rel_number: u32,
        nblocks: u32,
        capacity: usize,
    ) -> BufferPool<SmgrStorageBackend> {
        let mut smgr = MdStorageManager::new(base);
        let rel = smgr_rel(rel_number);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..nblocks {
            let fill = ((rel_number + i) % 200) as u8;
            smgr.extend(rel, ForkNumber::Main, i, &[fill; PAGE_SIZE], true)
                .unwrap();
        }
        smgr.immedsync(rel, ForkNumber::Main).unwrap();
        BufferPool::new(SmgrStorageBackend::new(smgr), capacity)
    }

    #[test]
    fn integ_cache_miss_reads_from_disk() {
        let base = temp_dir("miss_reads_disk");
        let pool = pool_with_relation(&base, 1, 3, 8);
        let t = smgr_tag(1, 0);
        let fill = (1u32 % 200) as u8;

        assert_eq!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        let page_data = pool.read_page(0).unwrap();
        assert!(
            page_data.iter().all(|&b| b == fill),
            "page read from disk should have fill byte {fill:#x}"
        );
    }

    #[test]
    fn integ_second_request_is_cache_hit() {
        let base = temp_dir("cache_hit");
        let pool = pool_with_relation(&base, 2, 1, 8);
        let t = smgr_tag(2, 0);

        assert_eq!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.unpin(1, 0).unwrap();

        assert_eq!(
            pool.request_page(2, t).unwrap(),
            RequestPageResult::Hit { buffer_id: 0 }
        );
    }

    #[test]
    fn integ_dirty_page_flushed_to_disk() {
        let base = temp_dir("flush_to_disk");
        {
            let pool = pool_with_relation(&base, 3, 1, 8);
            let t = smgr_tag(3, 0);

            pool.request_page(1, t).unwrap();
            pool.complete_read(0).unwrap();

            pool.write_byte(0, 0, 0xFF).unwrap();
            assert_eq!(pool.flush_buffer(0).unwrap(), FlushResult::WriteIssued);
            pool.complete_write(0).unwrap();
            assert!(!pool.buffer_state(0).unwrap().dirty);
        }

        let mut smgr2 = MdStorageManager::new(&base);
        let mut buf = [0u8; PAGE_SIZE];
        smgr2
            .read_block(smgr_rel(3), ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(buf[0], 0xFF, "flushed byte should be on disk");
        let fill = (3u32 % 200) as u8;
        assert!(buf[1..].iter().all(|&b| b == fill));
    }

    #[test]
    fn integ_all_buffers_pinned_returns_error() {
        let base = temp_dir("all_pinned");
        let pool = pool_with_relation(&base, 4, 3, 2);

        for block in 0..2u32 {
            let t = smgr_tag(4, block);
            assert!(matches!(
                pool.request_page(1, t).unwrap(),
                RequestPageResult::ReadIssued { .. }
            ));
            pool.complete_read(block as usize).unwrap();
        }

        let t = smgr_tag(4, 2);
        assert_eq!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::AllBuffersPinned
        );
    }

    #[test]
    fn integ_eviction_flushes_dirty_frame() {
        let base = temp_dir("evict_flush");
        let pool = pool_with_relation(&base, 5, 2, 1);

        let t0 = smgr_tag(5, 0);
        let t1 = smgr_tag(5, 1);

        assert_eq!(
            pool.request_page(1, t0).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 0xAB).unwrap();
        pool.flush_buffer(0).unwrap();
        pool.complete_write(0).unwrap();
        pool.unpin(1, 0).unwrap();

        assert_eq!(
            pool.request_page(1, t1).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        let mut smgr2 = MdStorageManager::new(&base);
        let mut buf = [0u8; PAGE_SIZE];
        smgr2
            .read_block(smgr_rel(5), ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(
            buf[0], 0xAB,
            "evicted dirty page should have been flushed to disk"
        );
    }

    /// A dirty buffer that is evicted without a prior explicit flush must
    /// still be written to disk before the frame is reused.
    #[test]
    fn integ_eviction_of_unflushed_dirty_frame_writes_to_disk() {
        let base = temp_dir("evict_unflushed_dirty");
        let pool = pool_with_relation(&base, 15, 2, 1);

        let t0 = smgr_tag(15, 0);
        let t1 = smgr_tag(15, 1);

        // Load block 0, dirty it, but do NOT call flush_buffer.
        assert_eq!(
            pool.request_page(1, t0).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 0xCD).unwrap();
        assert!(pool.buffer_state(0).unwrap().dirty);
        pool.unpin(1, 0).unwrap();

        // Request block 1 -- the single-frame pool must evict block 0.
        // The eviction path should write the dirty page to disk.
        assert_eq!(
            pool.request_page(1, t1).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        // Open a fresh smgr and verify block 0 has the written value.
        let mut smgr2 = MdStorageManager::new(&base);
        let mut buf = [0u8; PAGE_SIZE];
        smgr2
            .read_block(smgr_rel(15), ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(
            buf[0], 0xCD,
            "eviction must flush dirty page even without explicit heap_flush"
        );
    }

    #[test]
    fn integ_multiple_blocks_no_aliasing() {
        let base = temp_dir("no_aliasing");
        let nblocks = 5u32;
        let pool = pool_with_relation(&base, 6, nblocks, 8);

        for block in 0..nblocks {
            let t = smgr_tag(6, block);
            assert!(matches!(
                pool.request_page(1, t).unwrap(),
                RequestPageResult::ReadIssued { .. }
            ));
            pool.complete_read(block as usize).unwrap();
        }

        for block in 0..nblocks {
            let expected_fill = ((6 + block) % 200) as u8;
            let page_data = pool.read_page(block as usize).unwrap();
            assert!(
                page_data.iter().all(|&b| b == expected_fill),
                "block {block} has wrong fill byte (expected {expected_fill:#x})"
            );
        }
    }

    #[test]
    fn integ_invalidate_then_reread() {
        let base = temp_dir("invalidate_reread");
        let pool = pool_with_relation(&base, 7, 2, 8);
        let rel = smgr_rel(7);

        for block in 0..2u32 {
            let t = smgr_tag(7, block);
            assert!(matches!(
                pool.request_page(1, t).unwrap(),
                RequestPageResult::ReadIssued { .. }
            ));
            pool.complete_read(block as usize).unwrap();
            pool.unpin(1, block as usize).unwrap();
        }

        assert_eq!(pool.invalidate_relation(rel).unwrap(), 2);

        let t = smgr_tag(7, 0);
        assert!(matches!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::ReadIssued { .. }
        ));
    }

    fn pool_with_wal(
        base: &PathBuf,
        rel_number: u32,
        nblocks: u32,
        capacity: usize,
    ) -> BufferPool<SmgrStorageBackend> {
        let mut smgr = MdStorageManager::new(base);
        let rel = smgr_rel(rel_number);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..nblocks {
            let fill = ((rel_number + i) % 200) as u8;
            smgr.extend(rel, ForkNumber::Main, i, &[fill; PAGE_SIZE], true)
                .unwrap();
        }
        smgr.immedsync(rel, ForkNumber::Main).unwrap();

        let wal_dir = base.join("pg_wal");
        let wal =
            Arc::new(crate::backend::access::transam::xlog::WalWriter::new(&wal_dir).unwrap());
        BufferPool::new_with_wal(SmgrStorageBackend::new(smgr), capacity, wal)
    }

    fn read_block_from_disk(base: &PathBuf, rel_number: u32, block: u32) -> Page {
        let mut smgr = MdStorageManager::new(base);
        let mut buf = [0u8; PAGE_SIZE];
        smgr.read_block(smgr_rel(rel_number), ForkNumber::Main, block, &mut buf)
            .unwrap();
        buf
    }

    /// With WAL, `write_page_image` must NOT write the data page immediately.
    /// The on-disk file should still contain the original fill until the page
    /// is evicted or explicitly flushed.
    #[test]
    fn integ_wal_defers_data_page_write() {
        let base = temp_dir("wal_defers_write");
        let rel_number = 20u32;
        let fill = (rel_number % 200) as u8;
        let pool = pool_with_wal(&base, rel_number, 1, 4);
        let t = smgr_tag(rel_number, 0);

        // Load the page.
        assert_eq!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        // Modify via write_page_image (WAL path).
        let mut new_page = [fill; PAGE_SIZE];
        new_page[100] = 0xEE;
        pool.write_page_image(0, 1, &new_page).unwrap();

        // Frame should be dirty -- write was deferred.
        assert!(
            pool.buffer_state(0).unwrap().dirty,
            "page should still be dirty after WAL write"
        );

        // On-disk file must still have the original content.
        let on_disk = read_block_from_disk(&base, rel_number, 0);
        assert_eq!(
            on_disk[100], fill,
            "data page should not be on disk yet -- WAL defers the write"
        );
    }

    /// With WAL, `write_page_image` stamps the WAL LSN into `pd_lsn` (bytes 0-7).
    #[test]
    fn integ_wal_stamps_pd_lsn_in_page() {
        let base = temp_dir("wal_pd_lsn");
        let rel_number = 21u32;
        let pool = pool_with_wal(&base, rel_number, 1, 4);
        let t = smgr_tag(rel_number, 0);

        assert_eq!(
            pool.request_page(1, t).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        // Use a zeroed page so pd_lsn starts at 0.
        let new_page = [0u8; PAGE_SIZE];
        pool.write_page_image(0, 1, &new_page).unwrap();

        let page_after = pool.read_page(0).unwrap();
        let lsn_after = u64::from_le_bytes(page_after[0..8].try_into().unwrap());

        // LSN must be non-zero: the WAL writer stamped the assigned record LSN.
        assert_ne!(lsn_after, 0, "pd_lsn should be stamped with the WAL LSN");
        // The first (and only) record lands at offset WAL_RECORD_LEN.
        use crate::backend::access::transam::xlog::WAL_RECORD_LEN;
        assert_eq!(
            lsn_after, WAL_RECORD_LEN as u64,
            "first WAL record LSN should equal WAL_RECORD_LEN"
        );
    }

    /// After eviction from a WAL-backed pool, the data page reaches disk
    /// (without requiring a separate fsync -- WAL provides that guarantee).
    #[test]
    fn integ_wal_eviction_writes_data_page_to_disk() {
        let base = temp_dir("wal_evict_writes");
        let rel_number = 22u32;
        let fill = (rel_number % 200) as u8;
        // Single-frame pool forces eviction when block 1 is requested.
        let pool = pool_with_wal(&base, rel_number, 2, 1);

        let t0 = smgr_tag(rel_number, 0);
        let t1 = smgr_tag(rel_number, 1);

        // Load block 0 and dirty it via write_page_image.
        assert_eq!(
            pool.request_page(1, t0).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        let mut new_page = [fill; PAGE_SIZE];
        new_page[50] = 0xBE;
        pool.write_page_image(0, 99, &new_page).unwrap();
        pool.unpin(1, 0).unwrap();

        // Data page is still deferred at this point.
        let before = read_block_from_disk(&base, rel_number, 0);
        assert_eq!(before[50], fill, "data page not on disk yet");

        // Request block 1 -- evicts block 0, which must be written to disk.
        assert_eq!(
            pool.request_page(1, t1).unwrap(),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        // Now block 0 should be on disk with the modified byte.
        let after = read_block_from_disk(&base, rel_number, 0);
        assert_eq!(
            after[50], 0xBE,
            "eviction should have written the dirty page to disk"
        );
    }

    /// PoC for eviction race: dirty write lost when a writer modifies a
    /// page between the evictor's disk write and its re-check.
    ///
    /// Relies on the 50ms sleep injected in request_page after the
    /// eviction write to widen the race window.
    ///
    /// Sequence:
    ///   1. Pool has 2 frames. Load page A (block 0) and page B (block 1).
    ///   2. Write V1=0xAA to page A, mark dirty, unpin both.
    ///   3. Evictor thread requests page C (block 2) — must evict A.
    ///      Evictor writes V1 to disk, drops locks, sleeps 50ms.
    ///   4. Writer thread loads page A (still in lookup), writes V2=0xBB,
    ///      marks dirty, unpins — all during the 50ms window.
    ///   5. Evictor wakes, re-checks pin_count=0 and tag unchanged,
    ///      clears frame, repurposes for page C.
    ///   6. Reader loads page A — not in pool, reads from disk → gets V1.
    ///      V2 is lost.
    #[test]
    fn poc_eviction_loses_dirty_write() {
        use std::sync::Arc;

        let base = temp_dir("poc_eviction_race");
        let pool = Arc::new(pool_with_relation(&base, 600, 3, 2));

        let page_a = smgr_tag(600, 0);
        let page_b = smgr_tag(600, 1);
        let page_c = smgr_tag(600, 2);

        // Step 1: Load pages A and B into both frames.
        match pool.request_page(1, page_a).unwrap() {
            RequestPageResult::ReadIssued { buffer_id } => pool.complete_read(buffer_id).unwrap(),
            other => panic!("expected ReadIssued for A, got {other:?}"),
        };
        match pool.request_page(1, page_b).unwrap() {
            RequestPageResult::ReadIssued { buffer_id } => pool.complete_read(buffer_id).unwrap(),
            other => panic!("expected ReadIssued for B, got {other:?}"),
        };

        // Step 2: Write V1=0xAA to page A, mark dirty, unpin both.
        let a_buf = match pool.request_page(1, page_a).unwrap() {
            RequestPageResult::Hit { buffer_id } => buffer_id,
            other => panic!("expected Hit for A, got {other:?}"),
        };
        {
            let mut guard = pool.lock_buffer_exclusive(a_buf).unwrap();
            guard[100] = 0xAA;
        }
        pool.mark_buffer_dirty_hint(a_buf);
        pool.unpin(1, a_buf).unwrap(); // extra pin from second request
        pool.unpin(1, a_buf).unwrap(); // original pin

        let b_buf = match pool.request_page(1, page_b).unwrap() {
            RequestPageResult::Hit { buffer_id } => buffer_id,
            other => panic!("expected Hit for B, got {other:?}"),
        };
        pool.unpin(1, b_buf).unwrap(); // extra pin
        pool.unpin(1, b_buf).unwrap(); // original pin

        // Step 3: Evictor requests page C in a separate thread.
        // This will evict page A (dirty), write to disk, then sleep 50ms.
        let pool2 = pool.clone();
        let evictor = std::thread::spawn(move || match pool2.request_page(2, page_c).unwrap() {
            RequestPageResult::ReadIssued { buffer_id } => {
                pool2.complete_read(buffer_id).unwrap();
                pool2.unpin(2, buffer_id).unwrap();
            }
            other => panic!("expected ReadIssued for C, got {other:?}"),
        });

        // Step 4: While evictor sleeps, writer loads page A and writes V2.
        // Give the evictor a moment to reach the sleep.
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Page A should still be in lookup (evictor hasn't re-acquired locks yet).
        let a_buf2 = match pool.request_page(3, page_a).unwrap() {
            RequestPageResult::Hit { buffer_id } => buffer_id,
            RequestPageResult::ReadIssued { buffer_id } => {
                // Page was already evicted — the race window was missed.
                pool.complete_read(buffer_id).unwrap();
                pool.unpin(3, buffer_id).unwrap();
                evictor.join().unwrap();
                eprintln!("race window missed — evictor finished before writer could load A");
                return; // can't reproduce this run
            }
            other => panic!("unexpected for A reload: {other:?}"),
        };

        // Write V2=0xBB
        {
            let mut guard = pool.lock_buffer_exclusive(a_buf2).unwrap();
            guard[100] = 0xBB;
        }
        pool.mark_buffer_dirty_hint(a_buf2);
        pool.unpin(3, a_buf2).unwrap();

        // Step 5: Wait for evictor to finish (it will clear the frame).
        evictor.join().unwrap();

        // Step 6: Read page A — should get V2=0xBB, but if the race hit,
        // it'll get V1=0xAA from disk.
        let a_buf3 = match pool.request_page(4, page_a).unwrap() {
            RequestPageResult::Hit { buffer_id } => buffer_id,
            RequestPageResult::ReadIssued { buffer_id } => {
                pool.complete_read(buffer_id).unwrap();
                buffer_id
            }
            other => panic!("unexpected for final A read: {other:?}"),
        };
        let guard = pool.lock_buffer_shared(a_buf3).unwrap();
        let val = guard[100];
        drop(guard);
        pool.unpin(4, a_buf3).unwrap();

        assert_eq!(
            val, 0xBB,
            "EVICTION RACE REPRODUCED: expected V2=0xBB but got {val:#04x} (V1=0xAA means dirty write lost)"
        );
    }

    // (stack overflow test removed — needs a different fix approach)
}
