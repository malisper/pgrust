pub mod smgr;

use std::collections::{BTreeMap, HashMap, VecDeque};

// A "client" in this model is a deterministic stand-in for a PostgreSQL
// backend/session. We use it only to track who holds pins.
pub type ClientId = u32;

// Buffer IDs are stable frame indexes in the in-memory pool.
pub type BufferId = usize;

// PostgreSQL pages are normally BLCKSZ bytes. The default build value is 8192.
// This model fixes that to 8KiB so the tests can reason in page units.
pub const PAGE_SIZE: usize = 8192;
pub type Page = [u8; PAGE_SIZE];

// This mirrors PostgreSQL's RelFileLocator at the level we need for buffer
// identity. We deliberately omit backend-local and temp-specific details in
// this first-pass model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelFileLocator {
    pub spc_oid: u32,
    pub db_oid: u32,
    pub rel_number: u32,
}

// PostgreSQL relations can have multiple forks. Shared buffers are keyed by
// relation + fork + block number, not just by relation/block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ForkNumber {
    Main,
    Fsm,
    VisibilityMap,
    Init,
    Other(u8),
}

// This is the logical page identity, equivalent to PostgreSQL's BufferTag.
// In the real system this is used as the hash table key for the shared buffer
// mapping table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferTag {
    pub rel: RelFileLocator,
    pub fork: ForkNumber,
    pub block: u32,
}

// The model only exposes two I/O kinds: page read and page write. PostgreSQL
// has more nuance around AIO plumbing and writeback, but this is the minimal
// distinction needed to test shared-buffer behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOp {
    Read,
    Write,
}

// Tests can query "what I/O is currently outstanding for this buffer?".
// That lets them drive completion deterministically rather than relying on
// background threads or callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingIo {
    pub buffer_id: BufferId,
    pub op: IoOp,
    pub tag: BufferTag,
}

// This is the observable result of requesting a page. It intentionally
// compresses a lot of PostgreSQL internal detail into the outcomes a caller
// actually cares about:
// - hit in cache
// - miss that started a read
// - miss that attached to an already in-flight read
// - failure because there was no reusable buffer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPageResult {
    Hit { buffer_id: BufferId },
    ReadIssued { buffer_id: BufferId },
    WaitingOnRead { buffer_id: BufferId },
    AllBuffersPinned,
}

// Flush requests are modeled as a two-step process:
// 1. request a flush
// 2. complete or fail the write
//
// This mirrors the read path and makes I/O state transitions explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    WriteIssued,
    AlreadyClean,
    InProgress,
    Invalid,
}

// A small read-only snapshot of a buffer frame's state. This is the main tool
// tests use to assert behavior without depending on internal implementation
// details such as HashMap layout or free-list ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferStateView {
    pub tag: Option<BufferTag>,
    pub valid: bool,
    pub dirty: bool,
    pub io_in_progress: bool,
    pub io_error: bool,
    pub pin_count: usize,
    pub usage_count: u8,
}

// These are model-level errors. They are not meant to replicate PostgreSQL's
// exact error messages; they exist so tests can assert precondition failures
// and illegal state transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnknownBuffer,
    WrongIoOp,
    NoIoInProgress,
    BufferPinned,
    InvalidBuffer,
    NotDirty,
    Storage(String),
}

// The storage manager boundary for this v1 rewrite.
//
// In PostgreSQL, bufmgr talks to smgr, and smgr dispatches to md.c for actual
// filesystem I/O. Here we replace all of that with a small trait so the
// buffer-manager behavior can be tested independently of the real filesystem.
pub trait StorageBackend {
    fn read_page(&self, tag: BufferTag) -> Result<Page, String>;
    fn write_page(&mut self, tag: BufferTag, page: &Page) -> Result<(), String>;
}

// A deterministic in-memory fake for the storage layer.
//
// This does two jobs:
// - stores pages by BufferTag
// - lets tests inject one-shot failures
#[derive(Debug, Default, Clone)]
pub struct FakeStorage {
    pages: BTreeMap<BufferTag, Page>,
    fail_reads: HashMap<BufferTag, String>,
    fail_writes: HashMap<BufferTag, String>,
}

impl FakeStorage {
    // Seed a page in fake storage. This is the equivalent of pre-existing
    // relation contents on disk.
    pub fn put_page(&mut self, tag: BufferTag, page: Page) {
        self.pages.insert(tag, page);
    }

    // Read back the current fake-disk contents for assertions.
    pub fn get_page(&self, tag: BufferTag) -> Option<Page> {
        self.pages.get(&tag).copied()
    }

    // Configure the next read of this page to fail.
    pub fn fail_next_read(&mut self, tag: BufferTag, message: impl Into<String>) {
        self.fail_reads.insert(tag, message.into());
    }

    // Configure the next write of this page to fail.
    pub fn fail_next_write(&mut self, tag: BufferTag, message: impl Into<String>) {
        self.fail_writes.insert(tag, message.into());
    }
}

impl StorageBackend for FakeStorage {
    fn read_page(&self, tag: BufferTag) -> Result<Page, String> {
        // This intentionally leaves read failures "sticky" for now. The tests
        // only need injected failures, not exact md.c retry semantics.
        if let Some(err) = self.fail_reads.get(&tag) {
            return Err(err.clone());
        }

        // Uninitialized pages read back as zeroed pages. That's sufficient for
        // the current model and avoids forcing every test to seed every page.
        Ok(self.pages.get(&tag).copied().unwrap_or([0; PAGE_SIZE]))
    }

    fn write_page(&mut self, tag: BufferTag, page: &Page) -> Result<(), String> {
        // Writes fail once and then revert to normal behavior, which is
        // usually what tests want when validating retry semantics.
        if let Some(err) = self.fail_writes.remove(&tag) {
            return Err(err);
        }

        self.pages.insert(tag, *page);
        Ok(())
    }
}

// This is the model's equivalent of PostgreSQL's BufferDesc + page storage.
// We store explicit fields instead of bit-packing flags, because the goal here
// is readability and testability rather than exact memory layout fidelity.
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
        // A default frame is "completely free":
        // - no tag assigned
        // - no page data considered valid
        // - no dirty state
        // - no I/O in progress
        // - no pins
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
    // Expose a stable, read-only view to tests and callers.
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

    // Pinning prevents the frame from being reused. PostgreSQL has both a
    // shared pin count and backend-private pin tracking; we model both as an
    // aggregate count plus a per-client map.
    fn pin(&mut self, client_id: ClientId) {
        self.pin_count += 1;
        *self.pins_by_client.entry(client_id).or_insert(0) += 1;
    }

    // Unpin one reference owned by this client.
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

// BufferPool is the heart of the model.
//
// Important pieces:
// - `frames`: fixed-size pool of buffer frames
// - `lookup`: BufferTag -> BufferId mapping table
// - `free_list`: buffers that have never been used or were invalidated
// - `next_victim`: the clock hand for default eviction
pub struct BufferPool<S: StorageBackend> {
    storage: S,
    frames: Vec<BufferFrame>,
    lookup: HashMap<BufferTag, BufferId>,
    free_list: VecDeque<BufferId>,
    next_victim: usize,
    max_usage_count: u8,
}

impl<S: StorageBackend> BufferPool<S> {
    // Create a fixed-size buffer pool. All frames start on the freelist.
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
        }
    }

    // Number of frames in the pool.
    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    // Expose the backing storage for assertions.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    // Mutable access is used by tests to inject failures or seed pages.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    // Inspect one frame's state.
    pub fn buffer_state(&self, buffer_id: BufferId) -> Option<BufferStateView> {
        self.frames.get(buffer_id).map(BufferFrame::state_view)
    }

    // Request a page on behalf of a client.
    //
    // Behavior:
    // - If the page already has a canonical frame, pin that frame.
    // - If the frame is valid, this is a hit.
    // - If the frame exists but is still being read, the caller attaches to
    //   the same frame and waits on the existing read.
    // - Otherwise allocate a victim, install the new tag, pin it, and mark
    //   read I/O as in progress.
    pub fn request_page(&mut self, client_id: ClientId, tag: BufferTag) -> RequestPageResult {
        if let Some(&buffer_id) = self.lookup.get(&tag) {
            let frame = &mut self.frames[buffer_id];

            // This mirrors the "page already in buffer pool" path in
            // PostgreSQL's BufferAlloc/PinBuffer logic.
            frame.pin(client_id);
            if frame.usage_count < self.max_usage_count {
                frame.usage_count += 1;
            }

            // `valid=false` means there is a canonical frame for the page, but
            // it is not yet usable. In PostgreSQL this corresponds to cases
            // like an in-progress read.
            if frame.valid {
                RequestPageResult::Hit { buffer_id }
            } else {
                RequestPageResult::WaitingOnRead { buffer_id }
            }
        } else {
            // There is no existing canonical frame for this tag, so we need to
            // find a victim frame to recycle.
            let Some(buffer_id) = self.allocate_victim() else {
                return RequestPageResult::AllBuffersPinned;
            };

            let frame = &mut self.frames[buffer_id];

            // If the victim frame used to belong to some other page, remove the
            // old tag from the mapping table before assigning the new one.
            if let Some(old_tag) = frame.tag.take() {
                self.lookup.remove(&old_tag);
            }

            // Install the new page identity and mark the frame as an in-flight
            // read. The data is not valid until `complete_read()` succeeds.
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

    // Report the currently pending I/O for a frame, if any.
    //
    // We infer the operation type from validity:
    // - invalid + io_in_progress => read
    // - valid + io_in_progress => write
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

    // Complete a read that was previously issued for this frame.
    pub fn complete_read(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let tag = self.require_pending(buffer_id, IoOp::Read)?;
        let page = self.storage.read_page(tag).map_err(Error::Storage)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.page = page;
        frame.valid = true;
        frame.io_in_progress = false;
        frame.io_error = false;
        Ok(())
    }

    // Mark a read as failed. The frame remains the canonical mapping for the
    // tag, but it is still invalid and now advertises an I/O error.
    pub fn fail_read(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let _ = self.require_pending(buffer_id, IoOp::Read)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.valid = false;
        frame.io_in_progress = false;
        frame.io_error = true;
        Ok(())
    }

    // Mark a valid page dirty without changing its contents.
    pub fn mark_dirty(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        if !frame.valid {
            return Err(Error::InvalidBuffer);
        }
        frame.dirty = true;
        Ok(())
    }

    // Convenience helper for tests: modify one byte and mark the page dirty.
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

    // Begin a flush if the page is valid, dirty, and not already doing I/O.
    // The actual storage write is performed by `complete_write()`.
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

    // Complete a previously started write. On success the page becomes clean.
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
        Ok(())
    }

    // Mark a write as failed. The key semantic we want to preserve is that the
    // page remains dirty so the caller can retry later.
    pub fn fail_write(&mut self, buffer_id: BufferId) -> Result<(), Error> {
        let _ = self.require_pending(buffer_id, IoOp::Write)?;
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.io_in_progress = false;
        frame.io_error = true;
        frame.dirty = true;
        Ok(())
    }

    // Release one pin owned by a client.
    pub fn unpin(&mut self, client_id: ClientId, buffer_id: BufferId) -> Result<(), Error> {
        let frame = self.frames.get_mut(buffer_id).ok_or(Error::UnknownBuffer)?;
        frame.unpin(client_id)
    }

    // Drop all frames belonging to one relation, but only if they are not
    // currently pinned and do not have in-flight I/O. This mirrors the "be
    // conservative around invalidation" rule from PostgreSQL.
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

    // Victim selection:
    //
    // 1. Prefer buffers on the freelist.
    // 2. Fall back to a simplified clock sweep.
    //
    // The clock sweep matches the important behavior of PostgreSQL's default
    // strategy: skip pinned/in-use buffers and gradually age usage counts down
    // before reusing a frame.
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

    // Small helper to assert that a given frame is currently performing the
    // expected kind of I/O.
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

    // Short helpers keep the tests readable. The important part of these
    // tests is the scenario, not the mechanics of constructing identifiers.
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
        // This is the basic cache lifecycle:
        // 1. first request misses and issues read I/O
        // 2. read completes
        // 3. later request hits the same frame
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
        // Two clients requesting the same page while the first read is still
        // in progress should end up attached to the same frame, not two
        // separate copies.
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
        // A successful flush must persist data to storage and clear the dirty
        // flag in the in-memory frame.
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
        // The key failure semantic we want is "dirty survives write failure".
        // That gives callers a clear retry path.
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
        // If one frame is still pinned, the eviction policy must recycle some
        // other eligible frame instead.
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
        // Relation invalidation is deliberately conservative: fail if any page
        // of the relation is pinned, then succeed once all pins are released.
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
}
