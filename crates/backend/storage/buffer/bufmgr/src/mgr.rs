//! `buf_init.c` — the shared buffer-manager descriptor array
//! (`BufferDescriptors`), the page bytes (`BufferBlocks`), the per-buffer
//! content-lock array, and the per-buffer I/O condition-variable array
//! (`BufferIOCVArray`), plus the buffer-header spinlock primitives
//! (`LockBufHdr` / `UnlockBufHdr` / `WaitBufHdrUnlocked`) and the `freeNext`
//! accessors from bufmgr.c / buf_internals.h.
//!
//! Ambient per-backend handle: in C the pool is reached through the process
//! globals `BufferDescriptors`/`BufferBlocks`; here each backend caches a
//! `&'static BufferManager` "view" in a thread-local, published by
//! [`BufferManager::register_global`]. The view is process-local — exactly like
//! C's per-process pointer globals — but the POOL CONTENTS it points at (the
//! descriptor `state` atomics, the spinlock-protected descriptor fields, the
//! page bytes, the per-buffer content locks and I/O condvars) live in the
//! `MAP_SHARED` shared-memory segment carved by `ShmemInitStruct`. Because the
//! anonymous `MAP_SHARED` mapping is inherited at the same virtual address by
//! every forked backend, those raw pointers resolve to the same shared bytes in
//! every process, so a page one backend dirties is visible to all others — the
//! real bufmgr.c / buf_init.c posture. Only the per-backend cursor state (the
//! private pin counts, the pin-count-waiter record) stays process-local.

use std::cell::Cell;
use std::sync::atomic::Ordering;

use ::support::{BufTable, BufferStrategyControl, StrategyShmemSize};
use ::condvar::ConditionVariable;
use ::types_core::Size;
use ::types_core::primitive::{Buffer, BLCKSZ, INVALID_PROC_NUMBER};
use ::types_storage::buf::{buftag, PgAioWaitRef, BM_LOCKED, FREENEXT_END_OF_LIST, FREENEXT_NOT_IN_LIST};
use ::types_storage::storage::{
    pg_atomic_uint32, LWLock, LWLockMode, LWTRANCHE_BUFFER_CONTENT, BUFFER_MAPPING_LWLOCK_OFFSET,
    NUM_BUFFER_PARTITIONS,
};

use crate::refcount::PrivateRefCount;

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

/// A fixed-length array resident in the `MAP_SHARED` shared-memory segment,
/// reached through a raw base pointer (the address returned by
/// `ShmemInitStruct`, identical in every forked backend). This is the Rust
/// stand-in for C's bare in-shmem array pointers (`BufferDescriptors`,
/// `BufferBlocks`, the content-lock and I/O-condvar arrays): all backends share
/// the same bytes, and cross-backend exclusion is the buffer manager's lock
/// discipline (the header spinlock / content lock / partition lock), not a Rust
/// borrow.
///
/// The `BufferManager` holding these is itself process-local and `'static`
/// (leaked in `register_global`), so the raw pointers it carries never dangle.
struct ShmemArray<T> {
    base: *mut T,
    len: usize,
}

// SAFETY: the pointed-to bytes live in the shared segment for the life of the
// server; cross-backend access is serialized by the buffer manager's locks
// (exactly as the C bare-pointer arrays are). The `BufferManager` is published
// `'static` per backend, so the pointer is valid for the whole process.
unsafe impl<T> Send for ShmemArray<T> {}
unsafe impl<T> Sync for ShmemArray<T> {}

impl<T> ShmemArray<T> {
    /// Wrap a base pointer + element count. The region must hold at least `len`
    /// properly-aligned, initialized `T`s.
    fn new(base: *mut T, len: usize) -> Self {
        Self { base, len }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    /// `&array[i]` — a shared reference to element `i`. The caller provides the
    /// real serialization (the relevant lock), exactly as the C readers do.
    #[inline]
    fn get(&self, i: usize) -> &T {
        debug_assert!(i < self.len, "ShmemArray index {i} out of bounds {}", self.len);
        // SAFETY: `i < len`, the region holds `len` initialized `T`s, and
        // cross-backend access is serialized by the caller-held buffer-manager
        // lock (the same discipline C relies on for these bare-pointer arrays).
        unsafe { &*self.base.add(i) }
    }

    /// `&mut array[i]` — an exclusive reference to element `i`. The caller must
    /// hold the lock that makes the access exclusive (header spinlock for the
    /// descriptor fields, content lock for the page bytes), exactly as in C.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self, i: usize) -> &mut T {
        debug_assert!(i < self.len, "ShmemArray index {i} out of bounds {}", self.len);
        // SAFETY: as `get`, plus the caller holds the exclusive lock for `i`.
        unsafe { &mut *self.base.add(i) }
    }

    /// Raw slice `[start, start+count)` of the backing region for in-place
    /// byte access (the page bytes), under the caller-held content lock.
    #[inline]
    fn slice(&self, start: usize, count: usize) -> &[T] {
        debug_assert!(start + count <= self.len);
        // SAFETY: bounded by `len`; the caller holds the content lock for these
        // pages, so the shared read is sound (C's `char *BufferBlocks` read).
        unsafe { core::slice::from_raw_parts(self.base.add(start), count) }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn slice_mut(&self, start: usize, count: usize) -> &mut [T] {
        debug_assert!(start + count <= self.len);
        // SAFETY: as `slice`, plus the caller holds the exclusive content lock.
        unsafe { core::slice::from_raw_parts_mut(self.base.add(start), count) }
    }
}

/// Carve a `count`-element `T` array out of the `MAP_SHARED` shared-memory
/// segment via the `ShmemInitStruct` seam (the C `ShmemInitStruct(name,
/// count*sizeof(T), &found)`), returning the base pointer and the `found` flag
/// (true == another backend already created+initialized it).
///
/// In production the seam returns a real pointer into the shared segment, the
/// same address in every forked backend. In the crate's unit tests (and any
/// caller that has not stood up a real segment) the seam returns a NULL
/// pointer; there `carve_shmem` falls back to a leaked, zeroed process-heap
/// allocation so the owned `BufferManager` is still usable as a single-process
/// pool. The leak is bounded (one pool per test process) and matches the
/// `register_global` leak posture.
fn carve_shmem<T>(name: &str, count: usize) -> (*mut T, bool) {
    let bytes = count.saturating_mul(core::mem::size_of::<T>());
    let (raw, found) = ipc_shmem_seams::shmem_init_struct::call(name, bytes)
        .expect("ShmemInitStruct failed in BufferManagerShmemInit");
    if !raw.is_null() {
        debug_assert_eq!(
            raw as usize % core::mem::align_of::<T>(),
            0,
            "shmem region {name} is misaligned for its element type"
        );
        return (raw.cast::<T>(), found);
    }
    // No real segment (test / standalone harness): leak a zeroed heap region.
    let layout = core::alloc::Layout::array::<T>(count.max(1)).expect("layout");
    // SAFETY: non-zero layout; the zeroed bytes are a valid bit pattern for the
    // plain-data / atomic element types placed here (they are re-initialized in
    // place by the `!found` init loop, which always runs because we report
    // `found == false`).
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) }.cast::<T>();
    assert!(!ptr.is_null(), "out of memory carving {name} fallback");
    (ptr, false)
}

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

/// The buffer manager — a process-local *view* onto the shared descriptor
/// array, the page bytes, and the per-buffer content-lock and I/O-condvar
/// arrays (all resident in the `MAP_SHARED` shmem segment), plus the
/// per-backend private pin counts and this backend's pin-count-waiter record
/// (genuinely process-local).
///
/// The five shared arrays are reached through raw base pointers into the shmem
/// regions carved by `ShmemInitStruct`; the same anonymous `MAP_SHARED` mapping
/// (and thus the same addresses) is inherited by every forked backend, so the
/// pointers resolve to the same shared bytes process-wide. Exclusion is the
/// real C lock discipline:
///   * `states[i]` — the per-buffer header spinlock (`BM_LOCKED` CAS) + the
///     lock-free pin CAS, exactly as bufmgr.c `LockBufHdr`/`UnlockBufHdr`.
///   * `fields[i]` — protected by the header spinlock (`tag`/`io_wref`/
///     `wait_backend_pgprocno`) or the strategy spinlock (`free_next`).
///   * `content_locks[i]` — a real `LWLock` (`LWTRANCHE_BUFFER_CONTENT`),
///     acquired by `LockBuffer` via the lwlock dep directly.
///   * `io_cvs[i]` — the `BufferIOCVArray` condition variable for waiting on
///     I/O completion on buffer `i`.
pub struct BufferManager {
    /// `BufferDescriptors[i].state` — the packed atomic word per buffer, the
    /// substrate for the header spinlock + the lock-free pin CAS. Base pointer
    /// into the "Buffer Descriptors" shmem region.
    states: ShmemArray<pg_atomic_uint32>,
    /// The remaining (spinlock/strategy-protected) descriptor fields, reached
    /// only under the header spinlock / strategy spinlock. Base pointer into
    /// the "Buffer Descriptors" shmem region (alongside `states`). C's bare
    /// `char *`-style pointer access matches the per-buffer-lock discipline; no
    /// `RefCell` (which would impose an interpreter borrow flag that is not the
    /// real serialiser and would not be coherent cross-process).
    fields: ShmemArray<DescFields>,
    /// `BufferBlocks` — `nbuffers * BLCKSZ` page bytes, base pointer into the
    /// "Buffer Blocks" shmem region. Bare-pointer access matches C's
    /// `char *BufferBlocks`; reads of disjoint pages, or a re-entrant read of
    /// this page's header LSN (`SetHintBits`→`MarkBufferDirtyHint`→
    /// `BufferGetLSNAtomic`), are governed by the per-buffer content lock and
    /// header spinlock, exactly as in C.
    blocks: ShmemArray<u8>,
    /// `BufferDescriptorGetContentLock(buf)` — the per-buffer content `LWLock`
    /// array. Base pointer into the "Buffer Content Locks" shmem region.
    content_locks: ShmemArray<LWLock>,
    /// `BufferIOCVArray` — the per-buffer I/O `ConditionVariable` array
    /// (`BufferDescriptorGetIOCV`). Base pointer into the "Buffer IO Condition
    /// Variables" shmem region.
    io_cvs: ShmemArray<ConditionVariable>,
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

    /// `BufferManagerShmemInit(NBuffers)` (buf_init.c) — place the descriptor
    /// array, the data pages, the per-buffer content locks, and the I/O-condvar
    /// array IN THE `MAP_SHARED` SHARED-MEMORY SEGMENT (via `ShmemInitStruct`),
    /// then — only on first creation (`found == false`, the postmaster) — run
    /// the per-descriptor init loop in place: `buf_id = i`, `state = 0`,
    /// `wait_backend_pgprocno = INVALID_PROC_NUMBER`, freelist `freeNext = i+1`,
    /// last `FREENEXT_END_OF_LIST`,
    /// `LWLockInitialize(content_lock, LWTRANCHE_BUFFER_CONTENT)`,
    /// `ConditionVariableInit(BufferDescriptorGetIOCV(buf))`. A forked child
    /// re-publishes the same view onto the already-initialized shared bytes.
    pub fn BufferManagerShmemInit(nbuffers: u32) -> Self {
        let n = nbuffers as usize;

        // Carve each genuinely-shared array from the MAP_SHARED segment. The
        // returned base pointer is the same address in every forked backend.
        let (states, found_s) =
            carve_shmem::<pg_atomic_uint32>("Buffer Descriptors", n);
        // The spinlock-protected descriptor fields share the descriptor region
        // conceptually but are a distinct C field set; give them their own
        // named region for a clean layout (still all in the shared segment).
        let (fields, found_f) = carve_shmem::<DescFields>("Buffer Desc Fields", n);
        let (blocks, found_b) = carve_shmem::<u8>("Buffer Blocks", n.saturating_mul(BLCKSZ));
        let (content_locks, found_c) = carve_shmem::<LWLock>("Buffer Content Locks", n);
        let (io_cvs, found_i) = carve_shmem::<ConditionVariable>("Buffer IO Condition Variables", n);

        let states = ShmemArray::new(states, n);
        let fields = ShmemArray::new(fields, n);
        let blocks = ShmemArray::new(blocks, n.saturating_mul(BLCKSZ));
        let content_locks = ShmemArray::new(content_locks, n);
        let io_cvs = ShmemArray::new(io_cvs, n);

        // First creator initializes the shared bytes in place; attachers reuse
        // them (mirrors C's `if (!foundDescs) { for (i...) ... }`).
        if !found_s {
            for i in 0..n {
                // state == 0: pg_atomic_init_u32(&state, 0).
                states.get_mut(i).value.store(0, Ordering::Relaxed);
            }
        }
        if !found_f {
            for i in 0..n {
                *fields.get_mut(i) = DescFields {
                    tag: buftag::default(),
                    free_next: if i + 1 < n {
                        (i + 1) as i32
                    } else {
                        FREENEXT_END_OF_LIST
                    },
                    wait_backend_pgprocno: INVALID_PROC_NUMBER,
                    io_wref: PgAioWaitRef::default(),
                };
            }
        }
        if !found_b {
            // MemSet(BufferBlocks, 0, NBuffers * BLCKSZ).
            blocks.slice_mut(0, blocks.len()).fill(0);
        }
        if !found_c {
            for i in 0..n {
                // LWLockInitialize(content_lock, LWTRANCHE_BUFFER_CONTENT).
                lwlock::LWLockInitialize(
                    content_locks.get_mut(i),
                    LWTRANCHE_BUFFER_CONTENT,
                );
            }
        }
        if !found_i {
            for i in 0..n {
                // ConditionVariableInit(BufferDescriptorGetIOCV(buf)).
                *io_cvs.get_mut(i) = ConditionVariable::new();
                condition_variable::ConditionVariableInit(io_cvs.get(i));
            }
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
            fields,
            blocks,
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
        self.content_locks.get(buf_id)
    }

    /// `BufferDescriptorGetIOCV(buf)` — the I/O condition variable for buffer
    /// `buf_id` (used by the I/O wait family).
    #[allow(dead_code)]
    pub(crate) fn io_cv(&self, buf_id: usize) -> &ConditionVariable {
        self.io_cvs.get(buf_id)
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
        let state = &self.states.get(buf_id).value;
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
        self.states.get(buf_id)
            .value
            .store(buf_state & !BM_LOCKED, Ordering::Release);
    }

    /// `WaitBufHdrUnlocked(buf)` — spin until `BM_LOCKED` is clear, returning the
    /// observed state.
    #[allow(dead_code)]
    pub(crate) fn wait_buf_hdr_unlocked(&self, buf_id: usize) -> u32 {
        let state = &self.states.get(buf_id).value;
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
        self.states.get(buf_id).value.load(Ordering::Acquire)
    }

    /// `&GetBufferDescriptor(buf_id)->state` — the raw atomic word, for the
    /// lock-free CAS loops (`MarkBufferDirty`) that drive their own
    /// `compare_exchange_weak`.
    #[allow(dead_code)]
    pub(crate) fn states_atomic(&self, buf_id: usize) -> &std::sync::atomic::AtomicU32 {
        &self.states.get(buf_id).value
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
        self.states.get(buf_id).value.compare_exchange_weak(
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
        self.fields.get(buf_id).wait_backend_pgprocno
    }

    /// `GetBufferDescriptor(buf_id)->wait_backend_pgprocno = procno` — record the
    /// backend parked as the `BM_PIN_COUNT_WAITER`. Written under the header
    /// spinlock by `LockBufferForCleanup`.
    #[allow(dead_code)]
    pub(crate) fn set_wait_backend_pgprocno(&self, buf_id: usize, procno: i32) {
        self.fields.get_mut(buf_id).wait_backend_pgprocno = procno;
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
        self.fields.get(buf_id as usize).free_next
    }

    /// `GetBufferDescriptor(buf_id)->freeNext = value`. Raw write under the
    /// caller-held strategy spinlock (the C freelist.c writers' contract).
    pub fn set_free_next(&self, buf_id: i32, value: i32) {
        self.fields.get_mut(buf_id as usize).free_next = value;
    }

    /// `GetBufferDescriptor(buf_id)->tag` — header-spinlock-protected `Copy`
    /// read (callers hold the header lock or partition lock where it matters).
    #[allow(dead_code)]
    pub(crate) fn desc_tag(&self, buf_id: usize) -> buftag {
        self.fields.get(buf_id).tag
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
        f(self.blocks.slice_mut(start, BLCKSZ))
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
        self.blocks.slice_mut(start, BLCKSZ).fill(0);
    }

    /// Read-only view of buffer `buf_id`'s page bytes under a caller-held content
    /// lock (F1d `BufferGetPage` read / `PageGetLSN` / `PageIsNew`).
    #[allow(dead_code)]
    pub(crate) fn with_block<R>(&self, buf_id: usize, f: impl FnOnce(&[u8]) -> R) -> R {
        let start = buf_id * BLCKSZ;
        // SAFETY: see `with_block_mut`. A shared read under the caller-held
        // content lock / header spinlock, faithful to C's bare-pointer read.
        f(self.blocks.slice(start, BLCKSZ))
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
        self.fields.get_mut(buf_id).tag = tag;
    }

    /// `GetBufferDescriptor(buf_id)->io_wref = io_wref` — stamp / clear the
    /// AIO wait reference under the caller-held header spinlock
    /// (`StartBufferIO` staging / `TerminateBufferIO` release; `io_wref` is a
    /// spinlock-protected field like `tag`).
    #[allow(dead_code)]
    pub(crate) fn set_io_wref(&self, buf_id: usize, io_wref: PgAioWaitRef) {
        self.fields.get_mut(buf_id).io_wref = io_wref;
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
    ) -> types_error::PgResult<lwlock::MainLWLockGuard> {
        let my = lmgr_proc_seams::my_proc_number::call();
        lwlock::LWLockAcquireMain(
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
const PG_IO_ALIGN_SIZE: Size = ::types_storage::bufpage::PG_IO_ALIGN_SIZE;

/// `BufferManagerShmemSize(void)` (buf_init.c) — shared-memory bytes the buffer
/// pool needs: descriptors, data pages (+ I/O alignment padding), the freelist
/// strategy control + buffer lookup hash, the I/O condition variables, and the
/// checkpoint sort array. Mirrors the C `add_size`/`mul_size` overflow-checked
/// accumulation (carried on `Err`). `NBuffers` is the GUC global the C reads.
pub fn BufferManagerShmemSize() -> types_error::PgResult<Size> {
    use ipc_shmem_seams as shmem;

    let nbuffers = guc_tables::vars::NBuffers.read() as Size;
    let nbuffers_i32 = guc_tables::vars::NBuffers.read();

    let mut size: Size = 0;

    // size of buffer descriptors (state atoms) + cacheline alignment slack.
    size = shmem::add_size::call(size, shmem::mul_size::call(nbuffers, SIZEOF_BUFFER_DESC_PADDED)?)?;
    size = shmem::add_size::call(size, PG_CACHE_LINE_SIZE)?;

    // size of the spinlock-protected descriptor fields region (carved
    // separately from the state atoms here) + cacheline slack.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(nbuffers, core::mem::size_of::<DescFields>() as Size)?,
    )?;
    size = shmem::add_size::call(size, PG_CACHE_LINE_SIZE)?;

    // size of the per-buffer content-lock array (LWLockPadded-sized) + slack.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(
            nbuffers,
            ::types_storage::storage::LWLOCK_PADDED_SIZE as Size,
        )?,
    )?;
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
/// pool's shared structures and stand up this backend's manager view.
///
/// The genuinely-shared payload (descriptors, page bytes, content locks, I/O
/// condvars, buf table, strategy control) is carved from the `MAP_SHARED`
/// segment via `ShmemInitStruct` inside [`BufferManager::BufferManagerShmemInit`]
/// (the named regions `Buffer Descriptors`, `Buffer Desc Fields`,
/// `Buffer Blocks`, `Buffer Content Locks`, `Buffer IO Condition Variables`,
/// plus `Shared Buffer Lookup Table` and `Buffer Strategy Status` from the
/// buffer-support crate). The first creator initialises the bytes in place; a
/// forked child attaches and re-publishes the same shared pointers as its
/// process-local `&'static` view. The descriptor headers are NOT process-heap
/// copies — they are the shared bytes, so a page one backend dirties is visible
/// to every other backend. The `Checkpoint BufferIds` sort array (used only by
/// `BufferSync`) is carved here for shmem-index parity.
pub fn BufferManagerShmemInit() -> types_error::PgResult<()> {
    use ipc_shmem_seams as shmem;

    let nbuffers = guc_tables::vars::NBuffers.read() as u32;
    let n = nbuffers as Size;

    // Stand up (or re-publish) this backend's view of the pool. The descriptor
    // states/fields, page bytes, content locks, and I/O condvars are carved from
    // the shared segment and (on first creation) initialised in place inside the
    // constructor exactly as the C first-creator init loop does
    // (`StrategyInitialize` / `InitBufTable` are invoked there too).
    BufferManager::BufferManagerShmemInit(nbuffers).register_global();

    // Checkpoint sort array (allocated in shmem to avoid runtime allocation
    // during a checkpoint). Carved for shmem-index parity; the checkpoint code
    // that consumes it is reached separately.
    let (_ckpt, _found_ckpt) = shmem::shmem_init_struct::call(
        "Checkpoint BufferIds",
        shmem::mul_size::call(n, SIZEOF_CKPT_SORT_ITEM)?,
    )?;

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
            use bufmgr_seams as sb;
            sb::remember_buffer::set(|_b| {});
            sb::forget_buffer::set(|_b| {});
            sb::resowner_enlarge::set(|| Ok(()));
            lmgr_proc_seams::my_proc_number::set(|| 0);
            // `BufferManagerShmemInit` now also stands up the buffer-support
            // BufTable + StrategyControl (`InitBufTable` / `StrategyInitialize`),
            // both of which carve their backing store via `ShmemInitStruct`.
            // In tests there is no real shmem segment, so return "(null, first
            // creation)" exactly like the buffer-support test harness does.
            ipc_shmem_seams::shmem_init_struct::set(|_name, _size| {
                Ok((core::ptr::null_mut(), false))
            });
            // The direct LWLock content-lock path brackets each acquire/release
            // with HOLD_INTERRUPTS/RESUME_INTERRUPTS (globals.c); stub them.
            init_small_seams::hold_interrupts::set(|| {});
            init_small_seams::resume_interrupts::set(|| {});
        });
    }
}
