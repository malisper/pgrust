//! `buf_init.c` — the shared buffer-manager descriptor array
//! (`BufferDescriptors`), the page bytes (`BufferBlocks`), the per-buffer
//! content-lock array, and the per-buffer I/O condition-variable array
//! (`BufferIOCVArray`), plus the buffer-header spinlock primitives
//! (`LockBufHdr` / `UnlockBufHdr` / `WaitBufHdrUnlocked`) and the `freeNext`
//! accessors from bufmgr.c / buf_internals.h.
//!
//! Ambient per-backend handle: in C the pool is reached through the process
//! globals `BufferDescriptors`/`BufferBlocks`; a backend is a thread here, so
//! the ambient handle is a thread-local `&'static BufferManager` published by
//! [`BufferManager::register_global`] (the C "one pool view per process"
//! posture, keeping the backend-local `RefCell`/`Cell` state correct without
//! forcing `Sync`).

use std::cell::{Cell, RefCell, UnsafeCell};
use std::sync::atomic::Ordering;

use backend_storage_buffer_support::{BufTable, BufferStrategyControl, StrategyShmemSize};
use types_condvar::ConditionVariable;
use types_core::Size;
use types_core::primitive::{Buffer, BLCKSZ, INVALID_PROC_NUMBER};
use types_storage::buf::{buftag, PgAioWaitRef, BM_LOCKED, FREENEXT_END_OF_LIST, FREENEXT_NOT_IN_LIST};
use types_storage::storage::{
    pg_atomic_uint32, LWLock, LWLockMode, LWTRANCHE_BUFFER_CONTENT, BUFFER_MAPPING_LWLOCK_OFFSET,
    NUM_BUFFER_PARTITIONS,
};

use crate::refcount::PrivateRefCount;

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

thread_local! {
    /// THIS backend's ambient buffer-manager handle (the `BufferDescriptors`
    /// analog), published by [`BufferManager::register_global`]. A backend is a
    /// thread in this engine, so the ambient pool is thread-local — matching C's
    /// "one pool view per process" while keeping the pool's backend-local
    /// `RefCell`/`Cell` state correct (no forced `Sync`).
    static BACKEND_MGR: Cell<Option<&'static BufferManager>> = const { Cell::new(None) };
}

/// One per-buffer descriptor's mutable, NON-atomic fields
/// (buf_internals.h `BufferDesc` minus `state`, `buf_id`, and the embedded
/// `content_lock`). `state` lives separately in [`BufferManager::states`] as an
/// atomic so the header spinlock + lock-free pin CAS operate on it without any
/// Rust lock; `content_lock` lives in [`BufferManager::content_locks`] (so it
/// can be reached by `&LWLock` directly); `buf_id == index`.
///
/// `tag` / `io_wref` / `wait_backend_pgprocno` are protected by the header
/// spinlock; `free_next` by the strategy spinlock — exactly as in C.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DescFields {
    /// `BufferTag tag` — valid when `BM_TAG_VALID`.
    pub(crate) tag: buftag,
    /// `int freeNext` — freelist link (strategy-spinlock-protected).
    pub(crate) free_next: i32,
    /// `int wait_backend_pgprocno` — backend of the pin-count waiter.
    pub(crate) wait_backend_pgprocno: i32,
    /// `PgAioWaitRef io_wref` (buf_internals.h:269) — set iff AIO is in
    /// progress on this buffer (header-spinlock-protected).
    pub(crate) io_wref: PgAioWaitRef,
}

impl Default for DescFields {
    fn default() -> Self {
        Self {
            tag: buftag::default(),
            free_next: FREENEXT_NOT_IN_LIST,
            wait_backend_pgprocno: INVALID_PROC_NUMBER,
            io_wref: PgAioWaitRef::default(),
        }
    }
}

/// The buffer manager — the shared descriptor array, the page bytes, the
/// per-buffer content-lock and I/O-condvar arrays, plus the per-backend private
/// pin counts and this backend's pin-count-waiter record.
///
/// In a real multi-backend server these arrays live in the `MAP_SHARED`
/// segment; here a backend is a thread and the per-backend state is modelled
/// with `RefCell`/`Cell`, so the manager owns the arrays directly (the shmem
/// carve is a later concern). Exclusion is the real C lock discipline:
///   * `states[i]` — the per-buffer header spinlock (`BM_LOCKED` CAS) + the
///     lock-free pin CAS, exactly as bufmgr.c `LockBufHdr`/`UnlockBufHdr`.
///   * `fields[i]` — protected by the header spinlock (`tag`/`io_wref`/
///     `wait_backend_pgprocno`) or the strategy spinlock (`free_next`).
///   * `content_locks[i]` — a real `LWLock` (`LWTRANCHE_BUFFER_CONTENT`),
///     acquired by `LockBuffer` via the lwlock dep directly.
///   * `io_cvs[i]` — the `BufferIOCVArray` condition variable for waiting on
///     I/O completion on buffer `i`.
pub struct BufferManager {
    /// `GetBufferDescriptor(i)->state` — the packed atomic word per buffer. The
    /// substrate for the header spinlock + the lock-free pin CAS.
    states: Vec<pg_atomic_uint32>,
    /// The remaining (spinlock/strategy-protected) descriptor fields. Reached
    /// only under the header spinlock / strategy spinlock.
    fields: RefCell<Vec<DescFields>>,
    /// `BufferBlocks` — `nbuffers * BLCKSZ` page bytes.
    ///
    /// Modelled as an `UnsafeCell` rather than a `RefCell` to match C's bare
    /// `char *BufferBlocks` shared array: in C, reading one page's header (e.g.
    /// `BufferGetLSNAtomic`) while another page is being written, or reading the
    /// LSN of the very page a hint-bit write is updating, is legal because
    /// synchronisation is provided by the per-buffer content lock and the header
    /// spinlock — not by an interpreter-level borrow. A single `RefCell` over the
    /// whole pool would spuriously conflict on these legal disjoint/same-page
    /// re-entrant accesses (e.g. `with_buffer_page` holding the page while the
    /// per-tuple visibility check calls `SetHintBits`→`MarkBufferDirtyHint`→
    /// `BufferGetLSNAtomic`). The content lock is the real serialiser.
    blocks: UnsafeCell<Vec<u8>>,
    /// The per-buffer content `LWLock` array
    /// (`BufferDescriptorGetContentLock`). One real lock per buffer.
    content_locks: Vec<LWLock>,
    /// `BufferIOCVArray` — the per-buffer I/O `ConditionVariable` array
    /// (`BufferDescriptorGetIOCV`).
    io_cvs: Vec<ConditionVariable>,
    /// The per-backend private pin counts (NOT shmem).
    private_refcount: PrivateRefCount,
    /// `SharedBufHash` (buf_table.c) — the buffer-mapping hash table
    /// (`BufferTag -> buf_id`). Reached under a `BufferMappingLock` partition
    /// lock, exactly as in C. Owned by the manager (`InitBufTable`).
    buf_table: BufTable,
    /// `StrategyControl` (freelist.c) — the freelist head + clock-sweep hand +
    /// allocation counters. Drives victim selection through [`ClockSweep`].
    strategy_control: BufferStrategyControl,
    /// `NBuffers`.
    nbuffers: u32,
    /// `PinCountWaitBuf` (bufmgr.c:183) — the single buffer this backend
    /// registered as the `BM_PIN_COUNT_WAITER` on while parked in
    /// `LockBufferForCleanup`. `-1` == NULL. BACKEND-LOCAL.
    pin_count_wait_buf: Cell<i32>,
}

impl BufferManager {
    // -- construction (buf_init.c) -----------------------------------------

    /// `BufferManagerShmemInit(NBuffers)` (buf_init.c) — place + initialise the
    /// descriptor array, the data pages, the per-buffer content locks, and the
    /// I/O-condvar array. Faithful to the per-descriptor init loop: `buf_id = i`,
    /// `state = 0`, `wait_backend_pgprocno = INVALID_PROC_NUMBER`, freelist
    /// `freeNext = i+1`, last `FREENEXT_END_OF_LIST`,
    /// `LWLockInitialize(content_lock, LWTRANCHE_BUFFER_CONTENT)`,
    /// `ConditionVariableInit(BufferDescriptorGetIOCV(buf))`.
    pub fn BufferManagerShmemInit(nbuffers: u32) -> Self {
        let n = nbuffers as usize;

        // states[i] — zeroed (state == 0).
        let mut states = Vec::with_capacity(n);
        for _ in 0..n {
            states.push(pg_atomic_uint32::new(0));
        }

        // fields[i] — the buf_init.c per-descriptor init loop.
        let mut fields = Vec::with_capacity(n);
        for i in 0..n {
            fields.push(DescFields {
                tag: buftag::default(),
                free_next: if i + 1 < n {
                    (i + 1) as i32
                } else {
                    FREENEXT_END_OF_LIST
                },
                wait_backend_pgprocno: INVALID_PROC_NUMBER,
                io_wref: PgAioWaitRef::default(),
            });
        }

        // BufferBlocks — n * BLCKSZ zeroed page bytes.
        let blocks = vec![0u8; n.saturating_mul(BLCKSZ)];

        // content_locks[i] — LWLockInitialize(.., LWTRANCHE_BUFFER_CONTENT).
        let mut content_locks = Vec::with_capacity(n);
        for _ in 0..n {
            let mut lock = LWLock::default();
            backend_storage_lmgr_lwlock::LWLockInitialize(&mut lock, LWTRANCHE_BUFFER_CONTENT);
            content_locks.push(lock);
        }

        // io_cvs[i] — ConditionVariableInit(BufferDescriptorGetIOCV(buf)).
        let mut io_cvs = Vec::with_capacity(n);
        for _ in 0..n {
            let cv = ConditionVariable::new();
            backend_storage_lmgr_condition_variable::ConditionVariableInit(&cv);
            io_cvs.push(cv);
        }

        // SharedBufHash — InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS) so a
        // backend can hold the new entry's slot while still holding the old
        // entry's slot during a buffer reassignment (buf_init.c:127 /
        // shmem.c sizing rationale).
        let buf_table = BufTable::InitBufTable(nbuffers as i32 + NUM_BUFFER_PARTITIONS)
            .expect("InitBufTable failed in BufferManagerShmemInit");

        // StrategyControl — StrategyInitialize(NBuffers). The "init once" path
        // seeds the freelist 0..NBuffers and the clock hand at 0 (freelist.c).
        let strategy_control = BufferStrategyControl::StrategyInitialize(nbuffers)
            .expect("StrategyInitialize failed in BufferManagerShmemInit");

        Self {
            states,
            fields: RefCell::new(fields),
            blocks: UnsafeCell::new(blocks),
            content_locks,
            io_cvs,
            private_refcount: PrivateRefCount::default(),
            buf_table,
            strategy_control,
            nbuffers,
            pin_count_wait_buf: Cell::new(-1),
        }
    }

    /// `InitBufferPool` — back-compat constructor name.
    pub fn new(nbuffers: u32) -> Self {
        Self::BufferManagerShmemInit(nbuffers)
    }

    // -- ambient (per-backend) manager handle ------------------------------

    /// Publish this manager as THIS backend's ambient buffer manager, returning
    /// a `'static` reference to it (the C `BufferManagerShmemInit` establishing
    /// the process-global descriptor array). Calling more than once for the same
    /// backend returns the FIRST-published manager.
    pub fn register_global(self) -> &'static BufferManager {
        BACKEND_MGR.with(|slot| {
            if let Some(existing) = slot.get() {
                return existing;
            }
            let leaked: &'static BufferManager = Box::leak(Box::new(self));
            slot.set(Some(leaked));
            leaked
        })
    }

    /// THIS backend's ambient buffer manager, or `None` if not yet published.
    pub fn global() -> Option<&'static BufferManager> {
        BACKEND_MGR.with(|slot| slot.get())
    }

    /// THIS backend's ambient buffer manager, panicking with a clear message if
    /// it has not been published (a programming error — the installed seams can
    /// only run after `register_global`).
    pub(crate) fn global_expect() -> &'static BufferManager {
        Self::global().expect(
            "buffer manager not initialised: call BufferManager::register_global \
             (BufferManagerShmemInit) before using the buffer-manager seams",
        )
    }

    #[allow(dead_code)]
    pub fn nbuffers(&self) -> u32 {
        self.nbuffers
    }

    /// The per-backend private pin map (used by the pin/unpin family in F1b).
    #[allow(dead_code)]
    pub(crate) fn private_refcount(&self) -> &PrivateRefCount {
        &self.private_refcount
    }

    /// `BufferDescriptorGetContentLock(buf)` — the content `LWLock` for buffer
    /// `buf_id` (used by `LockBuffer` in F1c, direct lwlock dep).
    #[allow(dead_code)]
    pub(crate) fn content_lock(&self, buf_id: usize) -> &LWLock {
        &self.content_locks[buf_id]
    }

    /// `BufferDescriptorGetIOCV(buf)` — the I/O condition variable for buffer
    /// `buf_id` (used by the I/O wait family).
    #[allow(dead_code)]
    pub(crate) fn io_cv(&self, buf_id: usize) -> &ConditionVariable {
        &self.io_cvs[buf_id]
    }

    /// `PinCountWaitBuf` accessor (F1c `LockBufferForCleanup`).
    #[allow(dead_code)]
    pub(crate) fn pin_count_wait_buf(&self) -> &Cell<i32> {
        &self.pin_count_wait_buf
    }

    // -- header spinlock (bufmgr.c LockBufHdr/UnlockBufHdr/WaitBufHdrUnlocked)

    /// `LockBufHdr(buf)` — acquire a buffer header's spinlock by setting
    /// `BM_LOCKED` via a `pg_atomic_fetch_or_u32` spin, returning the state with
    /// `BM_LOCKED` set. IN-CRATE (the spin loop is the algorithm).
    pub fn lock_buf_hdr(&self, buf_id: usize) -> u32 {
        let state = &self.states[buf_id].value;
        loop {
            // C `pg_atomic_fetch_or_u32` has FULL barrier semantics
            // (atomics.h). `SeqCst` is the Rust ordering that matches a full
            // (StoreLoad-inclusive) barrier.
            let old = state.fetch_or(BM_LOCKED, Ordering::SeqCst);
            if old & BM_LOCKED == 0 {
                return old | BM_LOCKED;
            }
            std::hint::spin_loop();
        }
    }

    /// `UnlockBufHdr(buf, buf_state)` — clear `BM_LOCKED`, writing back the
    /// (possibly modified) state.
    pub fn unlock_buf_hdr(&self, buf_id: usize, buf_state: u32) {
        self.states[buf_id]
            .value
            .store(buf_state & !BM_LOCKED, Ordering::Release);
    }

    /// `WaitBufHdrUnlocked(buf)` — spin until `BM_LOCKED` is clear, returning the
    /// observed state.
    #[allow(dead_code)]
    pub(crate) fn wait_buf_hdr_unlocked(&self, buf_id: usize) -> u32 {
        let state = &self.states[buf_id].value;
        let mut buf_state = state.load(Ordering::Acquire);
        while buf_state & BM_LOCKED != 0 {
            std::hint::spin_loop();
            buf_state = state.load(Ordering::Acquire);
        }
        buf_state
    }

    /// Read a descriptor's `state` atom without the header lock.
    #[allow(dead_code)]
    pub(crate) fn read_state(&self, buf_id: usize) -> u32 {
        self.states[buf_id].value.load(Ordering::Acquire)
    }

    /// `&GetBufferDescriptor(buf_id)->state` — the raw atomic word, for the
    /// lock-free CAS loops (`MarkBufferDirty`) that drive their own
    /// `compare_exchange_weak`.
    #[allow(dead_code)]
    pub(crate) fn states_atomic(&self, buf_id: usize) -> &std::sync::atomic::AtomicU32 {
        &self.states[buf_id].value
    }

    /// `pg_atomic_compare_exchange_u32(&buf->state, &expected, new)` — the
    /// lock-free pin/unpin/mark CAS substrate. C `pg_atomic_compare_exchange_u32`
    /// has FULL barrier semantics (atomics.h:370); `SeqCst` on both the success
    /// and failure orderings is the Rust match (`AcqRel`/`Acquire` would be
    /// genuinely weaker). Returns `Ok(())` on success or `Err(actual)` with the
    /// observed value on failure, mirroring the C in/out `expected` pointer.
    #[allow(dead_code)]
    pub(crate) fn state_compare_exchange(
        &self,
        buf_id: usize,
        expected: u32,
        new: u32,
    ) -> Result<u32, u32> {
        self.states[buf_id].value.compare_exchange_weak(
            expected,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }

    /// `GetBufferDescriptor(buf_id)->wait_backend_pgprocno` — the backend parked
    /// as the `BM_PIN_COUNT_WAITER`. Read under the header spinlock by the caller
    /// (`WakePinCountWaiter`).
    #[allow(dead_code)]
    pub(crate) fn wait_backend_pgprocno(&self, buf_id: usize) -> i32 {
        self.fields.borrow()[buf_id].wait_backend_pgprocno
    }

    /// `GetBufferDescriptor(buf_id)->wait_backend_pgprocno = procno` — record the
    /// backend parked as the `BM_PIN_COUNT_WAITER`. Written under the header
    /// spinlock by `LockBufferForCleanup`.
    #[allow(dead_code)]
    pub(crate) fn set_wait_backend_pgprocno(&self, buf_id: usize, procno: i32) {
        self.fields.borrow_mut()[buf_id].wait_backend_pgprocno = procno;
    }

    // -- buffer-id <-> Buffer helpers --------------------------------------

    /// `BufferIsValid` — true iff `buffer` is a valid shared (1..=NBuffers)
    /// buffer number. (Local/temp buffers are out of this core.)
    #[allow(dead_code)]
    pub fn buffer_is_valid(&self, buffer: Buffer) -> bool {
        buffer != INVALID_BUFFER && buffer > 0 && (buffer as i64) <= self.nbuffers as i64
    }

    /// `buffer - 1` for a valid shared buffer, with the `BufferIsValid`
    /// `elog(ERROR, "bad buffer ID")` surface. Crate-internal helper shared by
    /// the lock family (F1c).
    #[allow(dead_code)]
    pub(crate) fn buffer_to_buf_id_pub(&self, buffer: Buffer) -> types_error::PgResult<usize> {
        if !self.buffer_is_valid(buffer) {
            return Err(types_error::PgError::error(format!("bad buffer ID: {buffer}")));
        }
        Ok((buffer - 1) as usize)
    }

    /// Test-only: take one pin on a buffer via the F1b `pin_buffer`. Mirrors the
    /// F1b test wiring (the resource-owner stubs are installed by the caller).
    #[cfg(test)]
    pub(crate) fn pin_buffer_for_test(&self, buf_id: usize, has_strategy: bool) -> bool {
        self.pin_buffer(buf_id, has_strategy)
    }

    /// `GetBufferDescriptor(buf_id)->freeNext` (buf_internals.h). Raw read —
    /// the caller holds the strategy spinlock where it matters, exactly as the
    /// C freelist.c readers do.
    pub fn free_next(&self, buf_id: i32) -> i32 {
        self.fields.borrow()[buf_id as usize].free_next
    }

    /// `GetBufferDescriptor(buf_id)->freeNext = value`. Raw write under the
    /// caller-held strategy spinlock (the C freelist.c writers' contract).
    pub fn set_free_next(&self, buf_id: i32, value: i32) {
        self.fields.borrow_mut()[buf_id as usize].free_next = value;
    }

    /// `GetBufferDescriptor(buf_id)->tag` — header-spinlock-protected `Copy`
    /// read (callers hold the header lock or partition lock where it matters).
    #[allow(dead_code)]
    pub(crate) fn desc_tag(&self, buf_id: usize) -> buftag {
        self.fields.borrow()[buf_id].tag
    }

    /// Raw view of buffer `buf_id`'s page bytes for in-place read/write under a
    /// caller-held content lock (F1d `with_buffer_page`); also used by F1c
    /// `MarkBufferDirtyHint` to stamp the page LSN under the header lock.
    #[allow(dead_code)]
    pub(crate) fn with_block_mut<R>(&self, buf_id: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let start = buf_id * BLCKSZ;
        // SAFETY: faithful to C's bare `char *BufferBlocks` pointer arithmetic.
        // The caller holds this buffer's content lock (exclusive for writes), so
        // the `[start, start+BLCKSZ)` slice is exclusively this backend's to
        // mutate. Reads of *other* pages, or a re-entrant read of *this* page's
        // header LSN (the `MarkBufferDirtyHint`/`BufferGetLSNAtomic` hint-bit
        // path), are governed by the same locks as in C, not by Rust borrows.
        let blocks = unsafe { &mut *self.blocks.get() };
        f(&mut blocks[start..start + BLCKSZ])
    }

    /// `MemSet(BufHdrGetBlock(buf), 0, BLCKSZ)` (bufmgr.c
    /// `ExtendBufferedRelShared`) — zero-fill a freshly-acquired victim buffer's
    /// page bytes before the extension lock is taken. The page is owned by this
    /// backend's pin and not yet valid, so no content lock is needed.
    #[allow(dead_code)]
    pub(crate) fn zero_block(&self, buf_id: usize) {
        let start = buf_id * BLCKSZ;
        // SAFETY: see `with_block_mut`. The freshly-acquired victim buffer is
        // pinned exclusively by this backend; the page is not yet valid.
        let blocks = unsafe { &mut *self.blocks.get() };
        blocks[start..start + BLCKSZ].fill(0);
    }

    /// Read-only view of buffer `buf_id`'s page bytes under a caller-held content
    /// lock (F1d `BufferGetPage` read / `PageGetLSN` / `PageIsNew`).
    #[allow(dead_code)]
    pub(crate) fn with_block<R>(&self, buf_id: usize, f: impl FnOnce(&[u8]) -> R) -> R {
        let start = buf_id * BLCKSZ;
        // SAFETY: see `with_block_mut`. A shared read under the caller-held
        // content lock / header spinlock, faithful to C's bare-pointer read.
        let blocks = unsafe { &*self.blocks.get() };
        f(&blocks[start..start + BLCKSZ])
    }

    // -- F2a: buffer-mapping table + strategy control + mapping locks ------

    /// `SharedBufHash` (buf_table.c) — the buffer-mapping hash table, reached
    /// under the partition's `BufferMappingLock` by the alloc/invalidate paths.
    #[allow(dead_code)]
    pub(crate) fn buf_table(&self) -> &BufTable {
        &self.buf_table
    }

    /// `StrategyControl` (freelist.c) — the freelist/clock-sweep control block.
    #[allow(dead_code)]
    pub(crate) fn strategy_control(&self) -> &BufferStrategyControl {
        &self.strategy_control
    }

    /// `StrategyNotifyBgWriter(bgwprocno)` (freelist.c) — set (or clear, with
    /// `-1`) the bgwriter proc number the next `StrategyGetBuffer` will wake.
    /// Forwards to the strategy control block. The background writer calls this
    /// to register for a next-allocation wakeup before hibernating.
    pub fn StrategyNotifyBgWriter(&self, bgwprocno: i32) -> types_error::PgResult<()> {
        self.strategy_control.notify_bgwriter(bgwprocno)
    }

    /// `GetBufferDescriptor(buf_id)->tag = tag` — set a victim's new tag under
    /// the caller-held header spinlock (`BufferAlloc` / `InvalidateVictimBuffer`).
    #[allow(dead_code)]
    pub(crate) fn set_desc_tag(&self, buf_id: usize, tag: buftag) {
        self.fields.borrow_mut()[buf_id].tag = tag;
    }

    /// `GetBufferDescriptor(buf_id)->io_wref = io_wref` — stamp / clear the
    /// AIO wait reference under the caller-held header spinlock
    /// (`StartBufferIO` staging / `TerminateBufferIO` release; `io_wref` is a
    /// spinlock-protected field like `tag`).
    #[allow(dead_code)]
    pub(crate) fn set_io_wref(&self, buf_id: usize, io_wref: PgAioWaitRef) {
        self.fields.borrow_mut()[buf_id].io_wref = io_wref;
    }

    /// `LWLockAcquire(BufMappingPartitionLock(partition), mode)` — take the
    /// `BufferMappingLock` for `partition` (the `MainLWLockArray` slot at
    /// `BUFFER_MAPPING_LWLOCK_OFFSET + partition`). Returns the RAII guard whose
    /// drop is `LWLockRelease`. Direct lwlock dep (no central seam).
    #[allow(dead_code)]
    pub(crate) fn map_acquire(
        &self,
        partition: u32,
        mode: LWLockMode,
    ) -> types_error::PgResult<backend_storage_lmgr_lwlock::MainLWLockGuard> {
        let my = backend_storage_lmgr_proc_seams::my_proc_number::call();
        backend_storage_lmgr_lwlock::LWLockAcquireMain(
            (BUFFER_MAPPING_LWLOCK_OFFSET + partition as i32) as usize,
            mode,
            my,
        )
    }
}

// ---------------------------------------------------------------------------
// Shared-memory sizing + placement (buf_init.c BufferManagerShmemSize /
// BufferManagerShmemInit). These are the `CalculateShmemSize` accumulator +
// `CreateOrAttachShmemStructs` entry points called from ipci.c.
// ---------------------------------------------------------------------------

/// `sizeof(BufferDescPadded)` (buf_internals.h) — `BUFFERDESC_PAD_TO_SIZE` is
/// 64 on the 64-bit (`SIZEOF_VOID_P == 8`) migration profile.
const SIZEOF_BUFFER_DESC_PADDED: Size = 64;
/// `sizeof(ConditionVariableMinimallyPadded)` (condition_variable.h):
/// `CV_MINIMAL_SIZE = (sizeof(ConditionVariable) <= 16 ? 16 : 32)`. The C
/// `ConditionVariable` is `slock_t mutex` (4) + `proclist_head wakeup` (two
/// `int32`, 8) = 12 bytes <= 16, so the padded size is 16.
const SIZEOF_CV_MINIMALLY_PADDED: Size = 16;
/// `sizeof(CkptSortItem)` (buf_internals.h) — `Oid tsId` (4) +
/// `RelFileNumber relNumber` (4) + `ForkNumber forkNum` (4) +
/// `BlockNumber blockNum` (4) + `int buf_id` (4) = 20 (alignment 4, no padding).
const SIZEOF_CKPT_SORT_ITEM: Size = 20;
/// `PG_CACHE_LINE_SIZE` (pg_config_manual.h).
const PG_CACHE_LINE_SIZE: Size = 128;
/// `PG_IO_ALIGN_SIZE` (c.h).
const PG_IO_ALIGN_SIZE: Size = types_storage::bufpage::PG_IO_ALIGN_SIZE;

/// `BufferManagerShmemSize(void)` (buf_init.c) — shared-memory bytes the buffer
/// pool needs: descriptors, data pages (+ I/O alignment padding), the freelist
/// strategy control + buffer lookup hash, the I/O condition variables, and the
/// checkpoint sort array. Mirrors the C `add_size`/`mul_size` overflow-checked
/// accumulation (carried on `Err`). `NBuffers` is the GUC global the C reads.
pub fn BufferManagerShmemSize() -> types_error::PgResult<Size> {
    use backend_storage_ipc_shmem_seams as shmem;

    let nbuffers = backend_utils_misc_guc_tables::vars::NBuffers.read() as Size;
    let nbuffers_i32 = backend_utils_misc_guc_tables::vars::NBuffers.read();

    let mut size: Size = 0;

    // size of buffer descriptors + cacheline alignment slack.
    size = shmem::add_size::call(size, shmem::mul_size::call(nbuffers, SIZEOF_BUFFER_DESC_PADDED)?)?;
    size = shmem::add_size::call(size, PG_CACHE_LINE_SIZE)?;

    // size of data pages, plus I/O alignment padding.
    size = shmem::add_size::call(size, PG_IO_ALIGN_SIZE)?;
    size = shmem::add_size::call(size, shmem::mul_size::call(nbuffers, BLCKSZ as Size)?)?;

    // size of stuff controlled by freelist.c (buf lookup hash + control block).
    size = shmem::add_size::call(size, StrategyShmemSize(nbuffers_i32))?;

    // size of I/O condition variables + cacheline alignment slack.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(nbuffers, SIZEOF_CV_MINIMALLY_PADDED)?,
    )?;
    size = shmem::add_size::call(size, PG_CACHE_LINE_SIZE)?;

    // size of checkpoint sort array in bufmgr.c.
    size = shmem::add_size::call(size, shmem::mul_size::call(nbuffers, SIZEOF_CKPT_SORT_ITEM)?)?;

    Ok(size)
}

/// `BufferManagerShmemInit(void)` (buf_init.c) — allocate-or-attach the buffer
/// pool's shared structures and stand up the process-local manager view.
///
/// The C carves four named regions (`Buffer Descriptors`, `Buffer Blocks`,
/// `Buffer IO Condition Variables`, `Checkpoint BufferIds`) from the shared
/// segment via `ShmemInitStruct`, then initialises the descriptor headers in
/// the first-creator backend. This engine models the genuinely-shared payload
/// (descriptors, page bytes, content locks, I/O condvars, buf table, strategy
/// control) as the owned [`BufferManager`] published process-locally through
/// [`BufferManager::register_global`] — the same posture procarray uses for the
/// ProcArray header. We still call `ShmemInitStruct` for each named region so
/// the cross-backend allocate-or-attach + `found` bookkeeping (the shmem index
/// entries surfaced by `pg_get_shmem_allocations`) is honoured exactly as C
/// does, and so the `found`/not-found "first creator initialises" decision is
/// driven by the real shmem index.
pub fn BufferManagerShmemInit() -> types_error::PgResult<()> {
    use backend_storage_ipc_shmem_seams as shmem;

    let nbuffers = backend_utils_misc_guc_tables::vars::NBuffers.read() as u32;
    let n = nbuffers as Size;

    // Align descriptors to a cacheline boundary.
    let (_descs, found_descs) = shmem::shmem_init_struct::call(
        "Buffer Descriptors",
        shmem::mul_size::call(n, SIZEOF_BUFFER_DESC_PADDED)?,
    )?;

    // Align buffer pool on the I/O page-size boundary.
    let (_blocks, found_bufs) = shmem::shmem_init_struct::call(
        "Buffer Blocks",
        shmem::add_size::call(shmem::mul_size::call(n, BLCKSZ as Size)?, PG_IO_ALIGN_SIZE)?,
    )?;

    // Align condition variables to a cacheline boundary.
    let (_iocv, found_iocv) = shmem::shmem_init_struct::call(
        "Buffer IO Condition Variables",
        shmem::mul_size::call(n, SIZEOF_CV_MINIMALLY_PADDED)?,
    )?;

    // Checkpoint sort array (allocated in shmem to avoid runtime allocation
    // during a checkpoint).
    let (_ckpt, found_ckpt) = shmem::shmem_init_struct::call(
        "Checkpoint BufferIds",
        shmem::mul_size::call(n, SIZEOF_CKPT_SORT_ITEM)?,
    )?;

    if found_descs || found_bufs || found_iocv || found_ckpt {
        // Should find all of these, or none of them (EXEC_BACKEND only). When
        // attaching, the genuinely-shared payload is already initialised by the
        // first creator; this backend just re-publishes its process-local view.
        debug_assert!(found_descs && found_bufs && found_iocv && found_ckpt);
    }

    // Stand up (or re-publish) this backend's view of the pool. The descriptor
    // headers, freelist links, content locks, and I/O condvars are initialised
    // inside `BufferManagerShmemInit(nbuffers)` exactly as the C first-creator
    // init loop does (`StrategyInitialize` is invoked there too).
    BufferManager::BufferManagerShmemInit(nbuffers).register_global();

    Ok(())
}

#[cfg(test)]
pub(crate) mod test_seams {
    use std::sync::Once;

    static ONCE: Once = Once::new();

    /// Install every outward seam the F1b/F1c unit tests reach, exactly ONCE for
    /// the whole test binary (`<fn>::set` panics on a second install, and the
    /// test harness runs tests in parallel within one process). The resource
    /// owner pin bookkeeping is a no-op; `my_proc_number` is a lone test backend
    /// (0); `nbuffers` matters only for the strategy path (unused here).
    pub(crate) fn install() {
        ONCE.call_once(|| {
            use backend_storage_buffer_bufmgr_seams as sb;
            sb::remember_buffer::set(|_b| {});
            sb::forget_buffer::set(|_b| {});
            sb::resowner_enlarge::set(|| Ok(()));
            backend_storage_lmgr_proc_seams::my_proc_number::set(|| 0);
            // `BufferManagerShmemInit` now also stands up the buffer-support
            // BufTable + StrategyControl (`InitBufTable` / `StrategyInitialize`),
            // both of which carve their backing store via `ShmemInitStruct`.
            // In tests there is no real shmem segment, so return "(null, first
            // creation)" exactly like the buffer-support test harness does.
            backend_storage_ipc_shmem_seams::shmem_init_struct::set(|_name, _size| {
                Ok((core::ptr::null_mut(), false))
            });
            // The direct LWLock content-lock path brackets each acquire/release
            // with HOLD_INTERRUPTS/RESUME_INTERRUPTS (globals.c); stub them.
            backend_utils_init_small_seams::hold_interrupts::set(|| {});
            backend_utils_init_small_seams::resume_interrupts::set(|| {});
        });
    }
}
