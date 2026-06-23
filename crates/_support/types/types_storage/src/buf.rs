//! Shared-buffer-pool handle vocabulary (`storage/buf.h`).

use types_core::{uint32, BlockNumber, ForkNumber, Oid, RelFileNumber};

use crate::storage::{pg_atomic_uint32, LWLock};

/// `typedef int Buffer;` (storage/buf.h). A nonzero value is a 1-based index
/// into the shared buffer descriptors (positive) or local buffers (negative);
/// 0 is the invalid handle.
pub type Buffer = i32;

/// `#define InvalidBuffer 0` (storage/buf.h).
pub const InvalidBuffer: Buffer = 0;

/// `#define BufferIsInvalid(buffer) ((buffer) == InvalidBuffer)` (storage/buf.h).
#[inline]
pub const fn BufferIsInvalid(buffer: Buffer) -> bool {
    buffer == InvalidBuffer
}

/// `#define BufferIsValid(bufnum)` (storage/buf.h) — true for any non-invalid
/// buffer handle.
#[inline]
pub const fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `#define BUFFER_LOCK_UNLOCK 0` (storage/bufmgr.h) — release the buffer's
/// content lock.
pub const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `#define BUFFER_LOCK_SHARE 1` (storage/bufmgr.h) — acquire a shared
/// content lock.
pub const BUFFER_LOCK_SHARE: i32 = 1;

/// `#define BUFFER_LOCK_EXCLUSIVE 2` (storage/bufmgr.h) — acquire an exclusive
/// content lock.
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

/// `RBM_NORMAL` (storage/bufmgr.h) — read the page normally.
pub const RBM_NORMAL: i32 = 0;
/// `RBM_ZERO_AND_LOCK` (storage/bufmgr.h) — don't read, zero the page and
/// return it exclusive-locked.
pub const RBM_ZERO_AND_LOCK: i32 = 1;
/// `RBM_ZERO_AND_CLEANUP_LOCK` (storage/bufmgr.h) — as `RBM_ZERO_AND_LOCK`
/// but acquire a cleanup lock.
pub const RBM_ZERO_AND_CLEANUP_LOCK: i32 = 2;

/// `BufferAccessStrategyType` (`storage/bufmgr.h`): the kind of ring buffer to
/// create with `GetAccessStrategy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BufferAccessStrategyType {
    /// `BAS_NORMAL` — Normal random access.
    BasNormal = 0,
    /// `BAS_BULKREAD` — Large read-only scan (hint bit updates are okay).
    BasBulkread,
    /// `BAS_BULKWRITE` — Large multi-block write (e.g. COPY IN).
    BasBulkwrite,
    /// `BAS_VACUUM` — VACUUM.
    BasVacuum,
}

/// `BufferAccessStrategyData` (freelist.c) — the backend-private ring of shared
/// buffers a bulk operation re-uses instead of trashing the whole cache. This
/// is BACKEND-LOCAL state, NOT shared memory: the ring is an owned `Vec<Buffer>`
/// touched only by the owning backend, exactly as `freelist.c` documents. The
/// ring merely *names* shared buffers (by `Buffer` number); the buffers
/// themselves live in the shared pool.
///
/// In C the fixed fields and the flexible `buffers[]` array are `palloc0`'d
/// together and the object is handed out BY POINTER (`typedef struct
/// BufferAccessStrategyData *BufferAccessStrategy`). Callers hold that pointer
/// directly, mutate the ring through it, and `pfree` it with
/// `FreeAccessStrategy`. The faithful Rust model of that backend-private heap
/// object shared/mutated by pointer is an `Rc<RefCell<_>>`; see the
/// [`BufferAccessStrategy`] alias, whose `None` is the C `NULL` (default,
/// no-ring) strategy.
///
/// The ring algorithms (sizing, slot reuse, reject) live in
/// `backend-storage-buffer-support` (`strategy.rs`); the fields are exposed here
/// because this is the shared vocabulary crate every consumer that threads the
/// pointer can reach, just as `storage/buf.h` declares the typedef.
#[derive(Clone, Debug)]
pub struct BufferAccessStrategyData {
    /// `BufferAccessStrategyType btype` — overall strategy type.
    pub btype: BufferAccessStrategyType,
    /// `int nbuffers` — number of elements in `buffers`.
    pub nbuffers: i32,
    /// `int current` — index of the "current" slot, i.e. the one most recently
    /// returned by `GetBufferFromRing`.
    pub current: i32,
    /// `Buffer buffers[]` — ring slots; `InvalidBuffer` (0) = empty slot.
    pub buffers: alloc::vec::Vec<Buffer>,
}

/// `typedef struct BufferAccessStrategyData *BufferAccessStrategy;`
/// (storage/buf.h). A backend-private ring handed out by pointer; the C `NULL`
/// (default, no-ring) strategy is `None`. The object is shared and mutated
/// through the pointer (an [`alloc::rc::Rc`] of an interior-mutable cell here),
/// mirroring C's single palloc'd object reused across the bulk operation, and
/// dropped by `FreeAccessStrategy`.
pub type BufferAccessStrategy = Option<alloc::rc::Rc<core::cell::RefCell<BufferAccessStrategyData>>>;

/// The C `NULL` strategy as a value, for sites that built `BufferAccessStrategy`
/// from the old `::NONE` constant. (`Option::None` is the canonical spelling;
/// this exists to keep call sites that named a single constant readable.)
#[inline]
pub fn buffer_access_strategy_none() -> BufferAccessStrategy {
    None
}

// ---------------------------------------------------------------------------
// Buffer-manager internals vocabulary (`storage/buf_internals.h`): the buffer
// tag, the packed `BufferDesc.state` masks, the `BM_*` flags, and the
// `BufferDesc` header itself. Verified against buf_internals.h.
// ---------------------------------------------------------------------------

/// `slock_t` (`storage/s_lock.h`) — the spinlock word, an `int` on the
/// int-based platforms.
pub type slock_t = i32;

/// `BufferTag` (buf_internals.h): identifies the block held in a buffer.
/// Mirrors the canonical PostgreSQL field order.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct buftag {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
    pub forkNum: ForkNumber,
    pub blockNum: BlockNumber,
}

/// `BUF_REFCOUNT_BITS` (buf_internals.h): bits in `state` for the shared
/// refcount.
pub const BUF_REFCOUNT_BITS: u32 = 18;
/// `BUF_REFCOUNT_ONE` (buf_internals.h): one shared pin = the lowest bit.
pub const BUF_REFCOUNT_ONE: u32 = 1;
/// `BUF_USAGECOUNT_ONE` (buf_internals.h).
pub const BUF_USAGECOUNT_ONE: u32 = 1 << BUF_REFCOUNT_BITS;
/// `BUF_USAGECOUNT_MASK` (buf_internals.h).
pub const BUF_USAGECOUNT_MASK: u32 = 0x003C_0000;
/// `BUF_REFCOUNT_MASK` (buf_internals.h).
pub const BUF_REFCOUNT_MASK: u32 = (1 << BUF_REFCOUNT_BITS) - 1;
/// `BUF_FLAG_MASK` (buf_internals.h).
pub const BUF_FLAG_MASK: u32 = 0xFFC0_0000;
/// `BM_MAX_USAGE_COUNT` (buf_internals.h).
pub const BM_MAX_USAGE_COUNT: u32 = 5;

/// `BM_LOCKED` (buf_internals.h) — buffer header is locked.
pub const BM_LOCKED: u32 = 1 << 22;
/// `BM_DIRTY` (buf_internals.h) — data needs writing.
pub const BM_DIRTY: u32 = 1 << 23;
/// `BM_VALID` (buf_internals.h) — data is valid.
pub const BM_VALID: u32 = 1 << 24;
/// `BM_TAG_VALID` (buf_internals.h) — tag is assigned.
pub const BM_TAG_VALID: u32 = 1 << 25;
/// `BM_IO_IN_PROGRESS` (buf_internals.h) — read or write in progress.
pub const BM_IO_IN_PROGRESS: u32 = 1 << 26;
/// `BM_IO_ERROR` (buf_internals.h) — previous I/O failed.
pub const BM_IO_ERROR: u32 = 1 << 27;
/// `BM_JUST_DIRTIED` (buf_internals.h) — dirtied since write started.
pub const BM_JUST_DIRTIED: u32 = 1 << 28;
/// `BM_PIN_COUNT_WAITER` (buf_internals.h) — have waiter for sole pin.
pub const BM_PIN_COUNT_WAITER: u32 = 1 << 29;
/// `BM_CHECKPOINT_NEEDED` (buf_internals.h) — must write for checkpoint.
pub const BM_CHECKPOINT_NEEDED: u32 = 1 << 30;
/// `BM_PERMANENT` (buf_internals.h) — permanent buffer (not unlogged, or init
/// fork).
pub const BM_PERMANENT: u32 = 1 << 31;

/// `PgAioWaitRef` (aio_types.h): a reference to an in-flight async I/O handle.
/// Three `uint32`s (the generation is split to avoid int64 alignment). Carried
/// in [`BufferDesc`] for layout parity; the AIO machinery itself is deferred.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgAioWaitRef {
    pub aio_index: uint32,
    pub generation_upper: uint32,
    pub generation_lower: uint32,
}

/// `BufferDesc` (buf_internals.h): the per-buffer header. `state` is the packed
/// `pg_atomic_uint32` (flags | usagecount | shared refcount).
///
/// `io_wref` and `content_lock` are carried even though the AIO and
/// content-lock machinery are facaded/deferred in the buffer manager port.
/// Not `Copy`/`Clone`: it embeds the shmem-resident atomic `state` and an
/// `LWLock`, whose identity (not value) is meaningful — exactly as in C, where
/// a `BufferDesc` is always reached through a pointer and never copied.
#[derive(Debug, Default)]
pub struct BufferDesc {
    /// `BufferTag tag` — ID of page contained in buffer; valid when
    /// `BM_TAG_VALID`.
    pub tag: buftag,
    /// `int buf_id` — buffer's index number (from 0); never changes.
    pub buf_id: i32,
    /// `pg_atomic_uint32 state` — flags | usagecount | shared refcount.
    pub state: pg_atomic_uint32,
    /// `int wait_backend_pgprocno` — backend of pin-count waiter.
    pub wait_backend_pgprocno: i32,
    /// `int freeNext` — link in freelist chain (protected by
    /// `buffer_strategy_lock`, not the header lock).
    pub freeNext: i32,
    /// `PgAioWaitRef io_wref` — set iff AIO is in progress.
    pub io_wref: PgAioWaitRef,
    /// `LWLock content_lock` — to lock access to buffer contents.
    pub content_lock: LWLock,
}

// ---------------------------------------------------------------------------
// `freelist.c` / `localbuf.c` signature types.
// ---------------------------------------------------------------------------

/// `FREENEXT_END_OF_LIST` (buf_internals.h) — sentinel `freeNext` value marking
/// the tail of the free list.
pub const FREENEXT_END_OF_LIST: i32 = -1;
/// `FREENEXT_NOT_IN_LIST` (buf_internals.h) — sentinel `freeNext` value marking
/// a buffer that is not currently on the free list.
pub const FREENEXT_NOT_IN_LIST: i32 = -2;

/// `IOContext` (pgstat.h) — the I/O-statistics bucket a strategy ring's reads
/// and writes are attributed to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IOContext {
    /// `IOCONTEXT_BULKREAD`.
    IOCONTEXT_BULKREAD = 0,
    /// `IOCONTEXT_BULKWRITE`.
    IOCONTEXT_BULKWRITE = 1,
    /// `IOCONTEXT_INIT`.
    IOCONTEXT_INIT = 2,
    /// `IOCONTEXT_NORMAL`.
    IOCONTEXT_NORMAL = 3,
    /// `IOCONTEXT_VACUUM`.
    IOCONTEXT_VACUUM = 4,
}

/// `LocalBufferLookupEnt` (localbuf.c) — entry for the per-backend local-buffer
/// lookup hash (`LocalBufHash`). BACKEND-LOCAL, not shared memory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalBufferLookupEnt {
    /// `key` — the buffer tag (disk page identity).
    pub key: buftag,
    /// `id` — associated local buffer index.
    pub id: i32,
}

/// The victim returned by the shared clock sweep (`StrategyGetBuffer`) and by
/// the backend-private ring (`GetBufferFromRing`). Faithful to the C contract:
/// the chosen buffer's header spinlock is STILL HELD when the victim is
/// returned, and the observed `buf_state` (with `BM_LOCKED` set) is carried
/// alongside the buffer id. The caller must `PinBuffer_Locked` (bump the
/// refcount) and then unlock the header before any other backend can pin the
/// victim.
///
/// The contract is enforced by the type: the only way to read the held state
/// out is [`Victim::into_parts`], so the caller cannot accidentally drop the
/// lock between selection and pin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "the victim's buffer header lock is held; pin or unlock it"]
pub struct Victim {
    /// The selected buffer index (0-based `BufferDesc` index).
    pub buf_id: i32,
    /// `*buf_state` as observed under `LockBufHdr` (`BM_LOCKED` is set).
    pub buf_state: u32,
}

impl Victim {
    /// Decompose into `(buf_id, buf_state)` so the caller can run
    /// `PinBuffer_Locked` and finally `UnlockBufHdr`.
    pub fn into_parts(self) -> (i32, u32) {
        (self.buf_id, self.buf_state)
    }
}

/// `MaxBlockNumber` (block.h) — the block-number sentinel one less than
/// `InvalidBlockNumber`. A relation may not be extended to or beyond this
/// length.
pub const MAX_BLOCK_NUMBER: BlockNumber = 0xFFFF_FFFE;

/// Result of `ExtendBufferedRelBy` (`storage/buffer/bufmgr.c`): the first
/// newly-extended block, the (pinned) victim buffers for the extended pages,
/// and the actual number of pages extended (the C call writes that back through
/// its `&extend_by` out-parameter).
#[derive(Clone, Debug, Default)]
pub struct ExtendedRelation {
    /// The first newly-extended block.
    pub first_block: types_core::BlockNumber,
    /// `victim_buffers[0 .. extended_by]`; index 0 is the page that
    /// `RelationAddBlocks` returns (exclusive-locked).
    pub victim_buffers: alloc::vec::Vec<Buffer>,
    /// The actual number of pages extended (`>= 1`, `<= extend_by`).
    pub extended_by: u32,
}
