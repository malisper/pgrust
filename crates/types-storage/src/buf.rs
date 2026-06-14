//! Shared-buffer-pool handle vocabulary (`storage/buf.h`).

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

/// A `BufferAccessStrategy` ring (`storage/buf.h`). `id == 0` is the C `NULL`
/// strategy (use the shared buffer pool with no ring); a nonzero id selects a
/// ring the buffer manager owns. The opaque `BufferAccessStrategyData` lives in
/// the buffer manager; callers only thread this handle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BufferAccessStrategy {
    pub id: u32,
}

impl BufferAccessStrategy {
    /// The C `NULL` strategy.
    pub const NONE: BufferAccessStrategy = BufferAccessStrategy { id: 0 };

    /// Whether a (non-NULL) strategy is set.
    #[inline]
    pub const fn is_set(self) -> bool {
        self.id != 0
    }
}

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
