use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::bufmgr::BufferPool;
use super::storage_backend::{SmgrStorageBackend, StorageBackend};
use crate::buf_internals::{BufferId, BufferState, BufferTag, ClientId, Error, PAGE_SIZE, Page};
use crate::smgr::{ForkNumber, RelFileLocator, StorageManager};

const LOCAL_MAX_USAGE_COUNT: u8 = 5;

struct LocalBufferFrame {
    state: BufferState,
    tag: Mutex<Option<BufferTag>>,
    content_lock: RwLock<Page>,
}

struct LocalStrategyState {
    free_list: VecDeque<BufferId>,
    next_victim: usize,
}

pub struct LocalBufferManager<S: StorageBackend + Send> {
    backing_pool: Arc<BufferPool<S>>,
    frames: Vec<LocalBufferFrame>,
    lookup: Mutex<FxHashMap<BufferTag, BufferId>>,
    strategy: Mutex<LocalStrategyState>,
    storage_read_count: AtomicU64,
}

impl<S: StorageBackend + Send> LocalBufferManager<S> {
    pub fn new(backing_pool: Arc<BufferPool<S>>, capacity: usize) -> Self {
        let mut free_list = VecDeque::with_capacity(capacity);
        for id in 0..capacity {
            free_list.push_back(id);
        }
        Self {
            backing_pool,
            frames: (0..capacity)
                .map(|_| LocalBufferFrame {
                    state: BufferState::new(),
                    tag: Mutex::new(None),
                    content_lock: RwLock::new([0u8; PAGE_SIZE]),
                })
                .collect(),
            lookup: Mutex::new(FxHashMap::default()),
            strategy: Mutex::new(LocalStrategyState {
                free_list,
                next_victim: 0,
            }),
            storage_read_count: AtomicU64::new(0),
        }
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    pub fn pinned_count(&self) -> usize {
        self.frames
            .iter()
            .filter(|frame| frame.state.pin_count() > 0)
            .count()
    }

    pub fn backing_pool(&self) -> &Arc<BufferPool<S>> {
        &self.backing_pool
    }

    pub fn storage_read_count(&self) -> u64 {
        self.storage_read_count.load(Ordering::Relaxed)
    }

    pub fn lock_buffer_shared(
        &self,
        buffer_id: BufferId,
    ) -> Result<RwLockReadGuard<'_, Page>, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        Ok(frame.content_lock.read())
    }

    pub fn lock_buffer_exclusive(
        &self,
        buffer_id: BufferId,
    ) -> Result<RwLockWriteGuard<'_, Page>, Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        Ok(frame.content_lock.write())
    }

    pub fn read_page(&self, buffer_id: BufferId) -> Option<Page> {
        self.frames
            .get(buffer_id)
            .map(|frame| *frame.content_lock.read())
    }

    pub fn mark_buffer_dirty(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.state.set_dirty();
        Ok(())
    }

    pub fn mark_buffer_dirty_hint(&self, buffer_id: BufferId) {
        if let Some(frame) = self.frames.get(buffer_id) {
            frame.state.set_dirty();
        }
    }

    pub fn pin_existing_block(
        &self,
        client_id: ClientId,
        rel: RelFileLocator,
        fork: ForkNumber,
        block_number: u32,
    ) -> Result<LocalPinnedBuffer<'_, S>, Error> {
        let tag = BufferTag {
            rel,
            fork,
            block: block_number,
        };
        let buffer_id = self.request_page(tag)?;
        Ok(LocalPinnedBuffer {
            local: self,
            client_id,
            buffer_id,
            released: false,
        })
    }

    pub fn wrap_pinned(
        &self,
        client_id: ClientId,
        buffer_id: BufferId,
    ) -> LocalPinnedBuffer<'_, S> {
        LocalPinnedBuffer {
            local: self,
            client_id,
            buffer_id,
            released: false,
        }
    }

    pub fn increment_buffer_pin(&self, buffer_id: BufferId) {
        let frame = self
            .frames
            .get(buffer_id)
            .expect("increment local buffer pin: unknown buffer");
        assert!(
            frame.state.pin_count() > 0,
            "increment local buffer pin: buffer must already be pinned"
        );
        frame.state.increment_pin();
    }

    fn request_page(&self, tag: BufferTag) -> Result<BufferId, Error> {
        {
            let lookup = self.lookup.lock();
            if let Some(&buffer_id) = lookup.get(&tag) {
                let frame = &self.frames[buffer_id];
                frame.state.pin_and_bump_usage(LOCAL_MAX_USAGE_COUNT);
                return Ok(buffer_id);
            }
        }

        let buffer_id = self.allocate_victim().ok_or(Error::AllBuffersPinned)?;
        let frame = &self.frames[buffer_id];
        if frame.state.is_dirty() {
            self.flush_buffer(buffer_id)?;
        }
        if let Some(old_tag) = *frame.tag.lock() {
            self.lookup.lock().remove(&old_tag);
        }

        let page = self
            .backing_pool
            .with_storage_mut(|storage| storage.read_page(tag).map_err(Error::Storage))?;
        self.storage_read_count.fetch_add(1, Ordering::Relaxed);
        *frame.content_lock.write() = page;
        *frame.tag.lock() = Some(tag);
        frame
            .state
            .store(1 | (1u32 << 14) | crate::buf_internals::BM_VALID_PUB);
        self.lookup.lock().insert(tag, buffer_id);
        Ok(buffer_id)
    }

    fn allocate_victim(&self) -> Option<BufferId> {
        {
            let mut strategy = self.strategy.lock();
            while let Some(buffer_id) = strategy.free_list.pop_front() {
                let frame = &self.frames[buffer_id];
                if frame.state.pin_count() == 0 {
                    frame.state.pin_and_bump_usage(LOCAL_MAX_USAGE_COUNT);
                    return Some(buffer_id);
                }
            }
        }

        let capacity = self.frames.len();
        if capacity == 0 {
            return None;
        }
        let mut strategy = self.strategy.lock();
        let mut scanned = 0usize;
        while scanned < capacity * (usize::from(LOCAL_MAX_USAGE_COUNT) + 1) {
            let buffer_id = strategy.next_victim;
            strategy.next_victim = (strategy.next_victim + 1) % capacity;
            scanned += 1;
            let frame = &self.frames[buffer_id];
            if frame.state.pin_count() > 0 {
                continue;
            }
            if frame.state.decrement_usage() {
                continue;
            }
            frame.state.pin_and_bump_usage(LOCAL_MAX_USAGE_COUNT);
            return Some(buffer_id);
        }
        None
    }

    pub fn flush_buffer(&self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?;
        let tag = match *frame.tag.lock() {
            Some(tag) => tag,
            None => return Ok(()),
        };
        if !frame.state.is_dirty() {
            return Ok(());
        }
        let page = *frame.content_lock.read();
        self.backing_pool.with_storage_mut(|storage| {
            storage.write_page(tag, &page, true).map_err(Error::Storage)
        })?;
        frame.state.clear_dirty();
        Ok(())
    }

    pub fn flush_relation(&self, rel: RelFileLocator) -> Result<(), Error> {
        for buffer_id in 0..self.frames.len() {
            let matches = {
                let tag = *self.frames[buffer_id].tag.lock();
                tag.is_some_and(|tag| tag.rel == rel)
            };
            if matches {
                self.flush_buffer(buffer_id)?;
            }
        }
        Ok(())
    }

    pub fn invalidate_relation(&self, rel: RelFileLocator) -> Result<(), Error> {
        let mut lookup = self.lookup.lock();
        for buffer_id in 0..self.frames.len() {
            let frame = &self.frames[buffer_id];
            let Some(tag) = *frame.tag.lock() else {
                continue;
            };
            if tag.rel != rel {
                continue;
            }
            if frame.state.pin_count() > 0 {
                return Err(Error::BufferPinned);
            }
            lookup.remove(&tag);
            *frame.tag.lock() = None;
            frame.state.store(0);
            self.strategy.lock().free_list.push_back(buffer_id);
        }
        Ok(())
    }

    fn unpin_raw(&self, buffer_id: BufferId) {
        let frame = self
            .frames
            .get(buffer_id)
            .expect("unpin local buffer: unknown buffer");
        frame.state.decrement_pin();
    }
}

impl LocalBufferManager<SmgrStorageBackend> {
    pub fn nblocks(&self, rel: RelFileLocator, fork: ForkNumber) -> Result<u32, Error> {
        self.backing_pool.with_storage_mut(|storage| {
            storage
                .smgr
                .nblocks(rel, fork)
                .map_err(|err| Error::Storage(err.to_string()))
        })
    }

    pub fn extend_zeroed(
        &self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: u32,
    ) -> Result<(), Error> {
        self.backing_pool
            .with_storage_mut(|storage| {
                let nblocks = storage
                    .smgr
                    .nblocks(rel, fork)
                    .map_err(|err| err.to_string())?;
                if block >= nblocks {
                    let zero_page = [0u8; PAGE_SIZE];
                    for b in nblocks..=block {
                        storage
                            .smgr
                            .extend(rel, fork, b, &zero_page, true)
                            .map_err(|err| err.to_string())?;
                    }
                }
                Ok::<(), String>(())
            })
            .map_err(Error::Storage)
    }
}

pub struct LocalPinnedBuffer<'a, S: StorageBackend + Send> {
    local: &'a LocalBufferManager<S>,
    client_id: ClientId,
    buffer_id: BufferId,
    released: bool,
}

impl<'a, S: StorageBackend + Send> LocalPinnedBuffer<'a, S> {
    pub fn buffer_id(&self) -> BufferId {
        self.buffer_id
    }

    pub fn into_raw(mut self) -> BufferId {
        self.released = true;
        self.buffer_id
    }

    pub fn release(mut self) -> Result<(), Error> {
        self.local.unpin_raw(self.buffer_id);
        self.released = true;
        Ok(())
    }
}

impl<S: StorageBackend + Send> Drop for LocalPinnedBuffer<'_, S> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.client_id;
            self.local.unpin_raw(self.buffer_id);
            self.released = true;
        }
    }
}

pub struct OwnedLocalBufferPin<S: StorageBackend + Send> {
    local: Arc<LocalBufferManager<S>>,
    buffer_id: BufferId,
    released: bool,
}

impl<S: StorageBackend + Send> OwnedLocalBufferPin<S> {
    pub fn wrap_existing(local: Arc<LocalBufferManager<S>>, buffer_id: BufferId) -> Self {
        Self {
            local,
            buffer_id,
            released: false,
        }
    }

    pub fn new(local: Arc<LocalBufferManager<S>>, buffer_id: BufferId) -> Self {
        local.increment_buffer_pin(buffer_id);
        Self {
            local,
            buffer_id,
            released: false,
        }
    }

    pub fn buffer_id(&self) -> BufferId {
        self.buffer_id
    }
}

impl<S: StorageBackend + Send> Clone for OwnedLocalBufferPin<S> {
    fn clone(&self) -> Self {
        self.local.increment_buffer_pin(self.buffer_id);
        Self {
            local: Arc::clone(&self.local),
            buffer_id: self.buffer_id,
            released: false,
        }
    }
}

impl<S: StorageBackend + Send> Drop for OwnedLocalBufferPin<S> {
    fn drop(&mut self) {
        if !self.released {
            self.local.unpin_raw(self.buffer_id);
            self.released = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FakeStorage;

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

    #[test]
    fn local_buffer_hit_reuses_slot() {
        let mut storage = FakeStorage::default();
        storage.put_page(tag(1, 0), [7; PAGE_SIZE]);
        let pool = Arc::new(BufferPool::new(storage, 2));
        let local = LocalBufferManager::new(pool, 2);

        let pin = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 0)
            .unwrap();
        let first = pin.buffer_id();
        drop(pin);
        let pin = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 0)
            .unwrap();
        assert_eq!(pin.buffer_id(), first);
        assert_eq!(local.read_page(first).unwrap()[0], 7);
    }

    #[test]
    fn local_buffer_dirty_victim_is_written() {
        let mut storage = FakeStorage::default();
        storage.put_page(tag(1, 0), [1; PAGE_SIZE]);
        storage.put_page(tag(1, 1), [2; PAGE_SIZE]);
        let pool = Arc::new(BufferPool::new(storage, 2));
        let local = LocalBufferManager::new(Arc::clone(&pool), 1);

        let pin = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 0)
            .unwrap();
        let buffer_id = pin.buffer_id();
        {
            let mut page = local.lock_buffer_exclusive(buffer_id).unwrap();
            page[0] = 9;
        }
        local.mark_buffer_dirty(buffer_id).unwrap();
        drop(pin);

        let _pin = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 1)
            .unwrap();
        assert_eq!(
            pool.with_storage(|storage| storage.get_page(tag(1, 0)).unwrap()[0]),
            9
        );
    }

    #[test]
    fn local_buffer_all_pinned_fails() {
        let mut storage = FakeStorage::default();
        storage.put_page(tag(1, 0), [1; PAGE_SIZE]);
        storage.put_page(tag(1, 1), [2; PAGE_SIZE]);
        let pool = Arc::new(BufferPool::new(storage, 2));
        let local = LocalBufferManager::new(pool, 1);

        let _pin = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 0)
            .unwrap();
        let err = local
            .pin_existing_block(1, rel(1), ForkNumber::Main, 1)
            .err()
            .unwrap();
        assert_eq!(err, Error::AllBuffersPinned);
    }
}
