mod types;
mod backend;

pub use types::*;
pub use backend::*;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;

use crate::storage::smgr::RelFileLocator;
use crate::storage::wal::{INVALID_LSN, Lsn, WalWriter};

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

pub struct BufferPool<S: StorageBackend + Send> {
    storage: Mutex<S>,
    wal: Option<Arc<WalWriter>>,
    frames: Vec<BufferFrame>,
    lookup: RwLock<FxHashMap<BufferTag, BufferId>>,
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
            lookup: RwLock::new(FxHashMap::default()),
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
    pub fn with_page_mut<T>(&self, buffer_id: BufferId, f: impl FnOnce(&mut Page) -> T) -> Option<T> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_valid() {
            return None;
        }
        let mut guard = frame.content_lock.write();
        frame.state.set_dirty();
        Some(f(&mut *guard))
    }

    pub fn request_page(&self, client_id: ClientId, tag: BufferTag) -> Result<RequestPageResult, Error> {
        // Fast path: check if the tag is already in the lookup table.
        {
            let lookup = self.lookup.read();
            if let Some(&buffer_id) = lookup.get(&tag) {
                let frame = &self.frames[buffer_id];
                frame.state.pin_and_bump_usage(self.max_usage_count);
                if frame.state.is_valid() {
                    self.stats_hit.fetch_add(1, Ordering::Relaxed);
                    return Ok(RequestPageResult::Hit { buffer_id });
                } else {
                    return Ok(RequestPageResult::WaitingOnRead { buffer_id });
                }
            }
        }

        // Slow path: allocate a victim and install the new tag.
        let mut strategy = self.strategy.lock();

        let Some(buffer_id) = self.allocate_victim(&mut strategy) else {
            return Ok(RequestPageResult::AllBuffersPinned);
        };

        let frame = &self.frames[buffer_id];
        let mut lookup = self.lookup.write();

        // Re-check: while we were waiting for the mapping lock, another
        // reader that already held a shared lookup lock could have pinned
        // this candidate buffer. In that case, restart victim selection.
        if frame.state.pin_count() > 0 || frame.state.is_io_in_progress() {
            drop(lookup);
            drop(strategy);
            return self.request_page(client_id, tag);
        }

        // Re-check: another thread may have inserted this tag while we waited.
        if let Some(&existing_id) = lookup.get(&tag) {
            strategy.free_list.push_back(buffer_id);
            drop(lookup);
            drop(strategy);

            let existing_frame = &self.frames[existing_id];
            existing_frame.state.pin_and_bump_usage(self.max_usage_count);
            if existing_frame.state.is_valid() {
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

        // If the victim holds a dirty page, write it out before reusing.
        // WAL rule: flush WAL up to the page's LSN before writing the data page.
        if frame.state.is_dirty() {
            let old_tag = *frame.tag.lock();
            if let Some(old_tag) = old_tag {
                let page = *frame.content_lock.read();
                let skip_fsync = self.wal.is_some();

                // Ensure WAL is flushed up to this page's LSN (write-ahead rule).
                if let Some(ref wal) = self.wal {
                    let page_lsn = u64::from_le_bytes(page[0..8].try_into().unwrap());
                    if page_lsn > 0 {
                        let _ = wal.flush_to(page_lsn);
                    }
                }
                drop(lookup);
                drop(strategy);

                {
                    let mut storage = self.storage.lock();
                    storage.write_page(old_tag, &page, skip_fsync).map_err(Error::Storage)?;
                }
                self.stats_written.fetch_add(1, Ordering::Relaxed);

                // Re-acquire all locks in the same order as above.
                strategy = self.strategy.lock();
                lookup = self.lookup.write();

                // Re-check: another thread may have pinned or replaced this
                // buffer while we dropped locks. If so, start over.
                if frame.state.pin_count() > 0 || *frame.tag.lock() != Some(old_tag) {
                    drop(lookup);
                    drop(strategy);
                    return self.request_page(client_id, tag);
                }
            }
        }

        {
            let mut tag_guard = frame.tag.lock();
            if let Some(old_tag) = tag_guard.take() {
                lookup.remove(&old_tag);
            }
            *tag_guard = Some(tag);
        }

        // Reset page data
        *frame.content_lock.write() = [0u8; PAGE_SIZE];

        // Reset atomic state: pin_count=1, usage_count=1, io_in_progress=true
        frame.state.init_for_io();

        lookup.insert(tag, buffer_id);

        Ok(RequestPageResult::ReadIssued { buffer_id })
    }

    pub fn pending_io(&self, buffer_id: BufferId) -> Option<PendingIo> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.state.is_io_in_progress() {
            return None;
        }
        let tag = (*frame.tag.lock())?;
        Some(PendingIo {
            buffer_id,
            op: if frame.state.is_valid() { IoOp::Write } else { IoOp::Read },
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
        frame.state.set_valid();
        frame.state.clear_io_in_progress();
        frame.state.clear_io_error();

        frame.io_complete.notify_all();
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

        frame.state.clear_valid();
        frame.state.clear_io_in_progress();
        frame.state.set_io_error();

        frame.io_complete.notify_all();
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

    pub fn write_byte(
        &self,
        buffer_id: BufferId,
        offset: usize,
        value: u8,
    ) -> Result<(), Error> {
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
        let tag = {
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            if !frame.state.is_valid() {
                return Err(Error::InvalidBuffer);
            }
            frame.tag.lock().ok_or(Error::UnknownBuffer)?
        };

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
                storage.write_page(tag, &page_to_store, false).map_err(Error::Storage)?;
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
        let tag = {
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            if !frame.state.is_valid() {
                return Err(Error::InvalidBuffer);
            }
            frame.tag.lock().ok_or(Error::UnknownBuffer)?
        };

        let mut page_to_store = *page;

        if let Some(ref wal) = self.wal {
            let lsn = wal
                .write_record(xid, tag, page)
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
                storage.write_page(tag, &page_to_store, false).map_err(Error::Storage)?;
            }
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            frame.state.clear_dirty();
        }

        Ok(())
    }

    pub fn write_page_image(&self, buffer_id: BufferId, xid: u32, page: &Page) -> Result<(), Error> {
        let tag = {
            let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
            if !frame.state.is_valid() {
                return Err(Error::InvalidBuffer);
            }
            frame.tag.lock().ok_or(Error::UnknownBuffer)?
        };

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
                storage.write_page(tag, &page_to_store, false).map_err(Error::Storage)?;
            }
            self.stats_written.fetch_add(1, Ordering::Relaxed);
            {
                let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
                frame.state.clear_dirty();
            }
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
            storage.write_page(tag, &page, skip_fsync).map_err(Error::Storage)?;
        }

        frame.state.clear_dirty();
        frame.state.clear_io_in_progress();
        frame.state.clear_io_error();

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

        frame.state.clear_io_in_progress();
        frame.state.set_io_error();
        frame.state.set_dirty();

        frame.io_complete.notify_all();
        Ok(())
    }

    pub fn unpin(&self, _client_id: ClientId, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        debug_assert!(frame.state.pin_count() > 0, "unpin on buffer with pin_count=0");
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

    /// Unpin the buffer without consuming the guard (for manual control).
    fn unpin_raw(&self, buffer_id: BufferId) {
        if let Some(frame) = self.frames.get(buffer_id) {
            frame.state.decrement_pin();
        }
    }

    /// Wait until I/O completes on the given buffer.
    pub fn wait_for_io(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        let mut guard = frame.tag.lock();
        while frame.state.is_io_in_progress() {
            frame.io_complete.wait(&mut guard);
        }
        if frame.state.is_io_error() {
            Err(Error::InvalidBuffer)
        } else {
            Ok(())
        }
    }

    pub fn invalidate_relation(&self, rel: RelFileLocator) -> Result<usize, Error> {
        let mut lookup = self.lookup.write();
        let mut strategy = self.strategy.lock();

        // First pass: verify no frames are pinned or have I/O in progress.
        for frame in &self.frames {
            let tag_guard = frame.tag.lock();
            if !matches!(*tag_guard, Some(tag) if tag.rel == rel) {
                continue;
            }
            if frame.state.pin_count() > 0 {
                return Err(Error::BufferPinned);
            }
            if frame.state.is_io_in_progress() {
                return Err(Error::NoIoInProgress);
            }
        }

        // Second pass: clear matching frames.
        let mut removed = 0;
        for (buffer_id, frame) in self.frames.iter().enumerate() {
            let mut tag_guard = frame.tag.lock();
            let Some(tag) = *tag_guard else { continue };
            if tag.rel != rel {
                continue;
            }

            lookup.remove(&tag);
            // Reset the frame to default state
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
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
        assert_eq!(pool.with_storage(|s| s.get_page(tag).unwrap()[0]), 99);
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
        pool.with_storage_mut(|s| s.fail_next_write(tag, "boom"));

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
        assert_eq!(pool.request_page(1, t).unwrap(), RequestPageResult::AllBuffersPinned);
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
        smgr2.read_block(smgr_rel(15), ForkNumber::Main, 0, &mut buf).unwrap();
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
        let wal = Arc::new(crate::storage::wal::WalWriter::new(&wal_dir).unwrap());
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
        assert!(pool.buffer_state(0).unwrap().dirty, "page should still be dirty after WAL write");

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
        use crate::storage::wal::WAL_RECORD_LEN;
        assert_eq!(lsn_after, WAL_RECORD_LEN as u64,
            "first WAL record LSN should equal WAL_RECORD_LEN");
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
}
