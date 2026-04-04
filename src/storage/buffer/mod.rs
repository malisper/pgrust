mod types;
mod backend;

pub use types::*;
pub use backend::*;

use std::collections::{HashMap, VecDeque};

use crate::storage::smgr::RelFileLocator;

#[derive(Debug, Clone)]
struct BufferFrame {
    tag: Option<BufferTag>,
    page: Page,
    valid: bool,
    dirty: bool,
    io_in_progress: bool,
    io_error: bool,
    usage_count: u8,
    pin_count: usize,
    pins_by_client: HashMap<ClientId, usize>,
}

impl Default for BufferFrame {
    fn default() -> Self {
        Self {
            tag: None,
            page: [0; PAGE_SIZE],
            valid: false,
            dirty: false,
            io_in_progress: false,
            io_error: false,
            usage_count: 0,
            pin_count: 0,
            pins_by_client: HashMap::new(),
        }
    }
}

impl BufferFrame {
    fn state_view(&self) -> BufferStateView {
        BufferStateView {
            tag: self.tag,
            valid: self.valid,
            dirty: self.dirty,
            io_in_progress: self.io_in_progress,
            io_error: self.io_error,
            pin_count: self.pin_count,
            usage_count: self.usage_count,
        }
    }

    fn pin(&mut self, client_id: ClientId) {
        self.pin_count += 1;
        *self.pins_by_client.entry(client_id).or_insert(0) += 1;
    }

    fn unpin(&mut self, client_id: ClientId) -> Result<(), Error> {
        let entry = self
            .pins_by_client
            .get_mut(&client_id)
            .ok_or(Error::BufferPinned)?;

        *entry -= 1;
        if *entry == 0 {
            self.pins_by_client.remove(&client_id);
        }
        self.pin_count -= 1;
        Ok(())
    }
}

pub struct BufferPool<S: StorageBackend> {
    storage: S,
    frames: Vec<BufferFrame>,
    lookup: HashMap<BufferTag, BufferId>,
    free_list: VecDeque<BufferId>,
    next_victim: usize,
    max_usage_count: u8,
    usage_stats: BufferUsageStats,
}

impl<S: StorageBackend> BufferPool<S> {
    pub fn new(storage: S, capacity: usize) -> Self {
        let mut free_list = VecDeque::with_capacity(capacity);
        for id in 0..capacity {
            free_list.push_back(id);
        }

        Self {
            storage,
            frames: vec![BufferFrame::default(); capacity],
            lookup: HashMap::new(),
            free_list,
            next_victim: 0,
            max_usage_count: 5,
            usage_stats: BufferUsageStats::default(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn usage_stats(&self) -> BufferUsageStats {
        self.usage_stats
    }

    pub fn reset_usage_stats(&mut self) {
        self.usage_stats = BufferUsageStats::default();
    }

    pub fn buffer_state(&self, buffer_id: BufferId) -> Option<BufferStateView> {
        self.frames.get(buffer_id).map(BufferFrame::state_view)
    }

    pub fn read_page(&self, buffer_id: BufferId) -> Option<&Page> {
        let frame = self.frames.get(buffer_id)?;
        if frame.valid { Some(&frame.page) } else { None }
    }

    pub fn request_page(&mut self, client_id: ClientId, tag: BufferTag) -> RequestPageResult {
        if let Some(&buffer_id) = self.lookup.get(&tag) {
            let frame = &mut self.frames[buffer_id];
            frame.pin(client_id);
            if frame.usage_count < self.max_usage_count {
                frame.usage_count += 1;
            }

            if frame.valid {
                self.usage_stats.shared_hit += 1;
                RequestPageResult::Hit { buffer_id }
            } else {
                RequestPageResult::WaitingOnRead { buffer_id }
            }
        } else {
            let Some(buffer_id) = self.allocate_victim() else {
                return RequestPageResult::AllBuffersPinned;
            };

            let frame = &mut self.frames[buffer_id];
            if let Some(old_tag) = frame.tag.take() {
                self.lookup.remove(&old_tag);
            }

            frame.tag = Some(tag);
            frame.page = [0; PAGE_SIZE];
            frame.valid = false;
            frame.dirty = false;
            frame.io_in_progress = true;
            frame.io_error = false;
            frame.usage_count = 1;
            frame.pin_count = 0;
            frame.pins_by_client.clear();
            frame.pin(client_id);

            self.lookup.insert(tag, buffer_id);
            RequestPageResult::ReadIssued { buffer_id }
        }
    }

    pub fn pending_io(&self, buffer_id: BufferId) -> Option<PendingIo> {
        let frame = self.frames.get(buffer_id)?;
        if !frame.io_in_progress {
            return None;
        }
        Some(PendingIo {
            buffer_id,
            op: if frame.valid { IoOp::Write } else { IoOp::Read },
            tag: frame.tag?,
        })
    }

    pub fn complete_read(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let tag = self.require_pending(buffer_id, IoOp::Read)?;
        let page = self.storage.read_page(tag).map_err(Error::Storage)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.page = page;
        frame.valid = true;
        frame.io_in_progress = false;
        frame.io_error = false;
        self.usage_stats.shared_read += 1;
        Ok(())
    }

    pub fn fail_read(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let _ = self.require_pending(buffer_id, IoOp::Read)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.valid = false;
        frame.io_in_progress = false;
        frame.io_error = true;
        Ok(())
    }

    pub fn mark_dirty(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.valid {
            return Err(Error::InvalidBuffer);
        }
        frame.dirty = true;
        Ok(())
    }

    pub fn write_byte(
        &mut self,
        buffer_id: BufferId,
        offset: usize,
        value: u8,
    ) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.valid {
            return Err(Error::InvalidBuffer);
        }
        frame.page[offset] = value;
        frame.dirty = true;
        Ok(())
    }

    pub fn write_page_image(&mut self, buffer_id: BufferId, page: &Page) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.valid {
            return Err(Error::InvalidBuffer);
        }
        frame.page = *page;
        frame.dirty = true;
        Ok(())
    }

    pub fn flush_buffer(&mut self, buffer_id: BufferId) -> Result<FlushResult, Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        if frame.io_in_progress {
            return Ok(FlushResult::InProgress);
        }
        if !frame.valid {
            return Ok(FlushResult::Invalid);
        }
        if !frame.dirty {
            return Ok(FlushResult::AlreadyClean);
        }
        frame.io_in_progress = true;
        frame.io_error = false;
        Ok(FlushResult::WriteIssued)
    }

    pub fn complete_write(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let tag = self.require_pending(buffer_id, IoOp::Write)?;
        let page = self.frames.get(buffer_id).ok_or(Error::UnknownBuffer)?.page;
        self.storage
            .write_page(tag, &page)
            .map_err(Error::Storage)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.dirty = false;
        frame.io_in_progress = false;
        frame.io_error = false;
        self.usage_stats.shared_written += 1;
        Ok(())
    }

    pub fn fail_write(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let _ = self.require_pending(buffer_id, IoOp::Write)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.io_in_progress = false;
        frame.io_error = true;
        frame.dirty = true;
        Ok(())
    }

    pub fn unpin(&mut self, client_id: ClientId, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.unpin(client_id)
    }

    pub fn invalidate_relation(&mut self, rel: RelFileLocator) -> Result<usize, Error> {
        let mut removed = 0;

        for buffer_id in 0..self.frames.len() {
            let frame = &self.frames[buffer_id];
            if !matches!(frame.tag, Some(tag) if tag.rel == rel) {
                continue;
            }
            if frame.pin_count > 0 {
                return Err(Error::BufferPinned);
            }
            if frame.io_in_progress {
                return Err(Error::NoIoInProgress);
            }
        }

        for buffer_id in 0..self.frames.len() {
            let frame = &mut self.frames[buffer_id];
            let Some(tag) = frame.tag else {
                continue;
            };
            if tag.rel != rel {
                continue;
            }

            self.lookup.remove(&tag);
            *frame = BufferFrame::default();
            self.free_list.push_back(buffer_id);
            removed += 1;
        }

        Ok(removed)
    }

    fn allocate_victim(&mut self) -> Option<BufferId> {
        while let Some(buffer_id) = self.free_list.pop_front() {
            let frame = &self.frames[buffer_id];
            if frame.pin_count == 0 && !frame.io_in_progress {
                return Some(buffer_id);
            }
        }

        let capacity = self.frames.len();
        if capacity == 0 {
            return None;
        }

        let mut scanned = 0usize;
        while scanned < capacity * (self.max_usage_count as usize + 1) {
            let buffer_id = self.next_victim;
            self.next_victim = (self.next_victim + 1) % capacity;
            scanned += 1;

            let frame = &mut self.frames[buffer_id];
            if frame.pin_count > 0 || frame.io_in_progress {
                continue;
            }
            if frame.usage_count > 0 {
                frame.usage_count -= 1;
                continue;
            }
            return Some(buffer_id);
        }

        None
    }

    fn require_pending(&self, buffer_id: BufferId, op: IoOp) -> Result<BufferTag, Error> {
        let pending = self.pending_io(buffer_id).ok_or(Error::NoIoInProgress)?;
        if pending.op != op {
            return Err(Error::WrongIoOp);
        }
        Ok(pending.tag)
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
        let mut pool = BufferPool::new(storage, 2);

        let first = pool.request_page(1, tag);
        assert_eq!(first, RequestPageResult::ReadIssued { buffer_id: 0 });
        pool.complete_read(0).unwrap();
        pool.unpin(1, 0).unwrap();

        let second = pool.request_page(2, tag);
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
        let mut pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, tag),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        assert_eq!(
            pool.request_page(2, tag),
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
        let mut pool = BufferPool::new(storage, 1);

        assert_eq!(
            pool.request_page(1, tag),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 99).unwrap();

        assert_eq!(pool.flush_buffer(0).unwrap(), FlushResult::WriteIssued);
        pool.complete_write(0).unwrap();

        let state = pool.buffer_state(0).unwrap();
        assert!(state.valid);
        assert!(!state.dirty);
        assert_eq!(pool.storage().get_page(tag).unwrap()[0], 99);
    }

    #[test]
    fn write_failure_retains_dirty_state() {
        let tag = tag(8, 0);
        let mut storage = FakeStorage::default();
        storage.put_page(tag, page(3));
        let mut pool = BufferPool::new(storage, 1);

        assert_eq!(
            pool.request_page(1, tag),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 44).unwrap();
        pool.storage_mut().fail_next_write(tag, "boom");

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
        let mut pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, a),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();

        assert_eq!(
            pool.request_page(2, b),
            RequestPageResult::ReadIssued { buffer_id: 1 }
        );
        pool.complete_read(1).unwrap();
        pool.unpin(2, 1).unwrap();

        let third = pool.request_page(3, c);
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
        let mut pool = BufferPool::new(storage, 2);

        assert_eq!(
            pool.request_page(1, a),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        assert_eq!(
            pool.request_page(2, b),
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
        let mut pool = pool_with_relation(&base, 1, 3, 8);
        let t = smgr_tag(1, 0);
        let fill = (1u32 % 200) as u8;

        assert_eq!(
            pool.request_page(1, t),
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
        let mut pool = pool_with_relation(&base, 2, 1, 8);
        let t = smgr_tag(2, 0);

        assert_eq!(
            pool.request_page(1, t),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.unpin(1, 0).unwrap();

        assert_eq!(
            pool.request_page(2, t),
            RequestPageResult::Hit { buffer_id: 0 }
        );
    }

    #[test]
    fn integ_dirty_page_flushed_to_disk() {
        let base = temp_dir("flush_to_disk");
        {
            let mut pool = pool_with_relation(&base, 3, 1, 8);
            let t = smgr_tag(3, 0);

            pool.request_page(1, t);
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
        let mut pool = pool_with_relation(&base, 4, 3, 2);

        for block in 0..2u32 {
            let t = smgr_tag(4, block);
            assert!(matches!(
                pool.request_page(1, t),
                RequestPageResult::ReadIssued { .. }
            ));
            pool.complete_read(block as usize).unwrap();
        }

        let t = smgr_tag(4, 2);
        assert_eq!(pool.request_page(1, t), RequestPageResult::AllBuffersPinned);
    }

    #[test]
    fn integ_eviction_flushes_dirty_frame() {
        let base = temp_dir("evict_flush");
        let mut pool = pool_with_relation(&base, 5, 2, 1);

        let t0 = smgr_tag(5, 0);
        let t1 = smgr_tag(5, 1);

        assert_eq!(
            pool.request_page(1, t0),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.write_byte(0, 0, 0xAB).unwrap();
        pool.flush_buffer(0).unwrap();
        pool.complete_write(0).unwrap();
        pool.unpin(1, 0).unwrap();

        assert_eq!(
            pool.request_page(1, t1),
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

    #[test]
    fn integ_multiple_blocks_no_aliasing() {
        let base = temp_dir("no_aliasing");
        let nblocks = 5u32;
        let mut pool = pool_with_relation(&base, 6, nblocks, 8);

        for block in 0..nblocks {
            let t = smgr_tag(6, block);
            assert!(matches!(
                pool.request_page(1, t),
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
        let mut pool = pool_with_relation(&base, 7, 2, 8);
        let rel = smgr_rel(7);

        for block in 0..2u32 {
            let t = smgr_tag(7, block);
            assert!(matches!(
                pool.request_page(1, t),
                RequestPageResult::ReadIssued { .. }
            ));
            pool.complete_read(block as usize).unwrap();
            pool.unpin(1, block as usize).unwrap();
        }

        assert_eq!(pool.invalidate_relation(rel).unwrap(), 2);

        let t = smgr_tag(7, 0);
        assert!(matches!(
            pool.request_page(1, t),
            RequestPageResult::ReadIssued { .. }
        ));
    }
}
