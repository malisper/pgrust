use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use ::types_core::{uint32, BlockNumber, ForkNumber, Oid, RelFileNumber};

pub use ::types_core::{Buffer, BufferIsValid, InvalidBuffer};

#[inline]
pub const fn BufferIsInvalid(buffer: Buffer) -> bool {
    buffer == InvalidBuffer
}

pub const BUFFER_LOCK_UNLOCK: i32 = 0;
pub const BUFFER_LOCK_SHARE: i32 = 1;
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

pub const RBM_NORMAL: i32 = 0;
pub const RBM_ZERO_AND_LOCK: i32 = 1;
pub const RBM_ZERO_AND_CLEANUP_LOCK: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BufferAccessStrategyType {
    BasNormal = 0,
    BasBulkread,
    BasBulkwrite,
    BasVacuum,
}

// Backend-local ring naming shared buffers; C pallocs one object and hands it
// out by pointer (`BufferAccessStrategy`), mutated through that pointer.
#[derive(Clone, Debug)]
pub struct BufferAccessStrategyData {
    pub btype: BufferAccessStrategyType,
    pub nbuffers: i32,
    pub current: i32,
    pub buffers: Vec<Buffer>, // std Vec + Rc justified: owner state sized once, not per-row
}

pub type BufferAccessStrategy = Option<Rc<RefCell<BufferAccessStrategyData>>>;

#[inline]
pub fn buffer_access_strategy_none() -> BufferAccessStrategy {
    None
}

pub type slock_t = i32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct buftag {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
    pub forkNum: ForkNumber,
    pub blockNum: BlockNumber,
}

const _: () = assert!(core::mem::size_of::<buftag>() == 20);

pub const BUF_REFCOUNT_BITS: u32 = 18;
pub const BUF_REFCOUNT_ONE: u32 = 1;
pub const BUF_REFCOUNT_MASK: u32 = (1 << BUF_REFCOUNT_BITS) - 1;
pub const BUF_USAGECOUNT_ONE: u32 = 1 << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_MASK: u32 = 0x003C_0000;
pub const BUF_FLAG_MASK: u32 = 0xFFC0_0000;
pub const BM_MAX_USAGE_COUNT: u32 = 5;

pub const BM_LOCKED: u32 = 1 << 22;
pub const BM_DIRTY: u32 = 1 << 23;
pub const BM_VALID: u32 = 1 << 24;
pub const BM_TAG_VALID: u32 = 1 << 25;
pub const BM_IO_IN_PROGRESS: u32 = 1 << 26;
pub const BM_IO_ERROR: u32 = 1 << 27;
pub const BM_JUST_DIRTIED: u32 = 1 << 28;
pub const BM_PIN_COUNT_WAITER: u32 = 1 << 29;
pub const BM_CHECKPOINT_NEEDED: u32 = 1 << 30;
pub const BM_PERMANENT: u32 = 1 << 31;

// Generation split into two uint32s to avoid int64 alignment (aio_types.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgAioWaitRef {
    pub aio_index: uint32,
    pub generation_upper: uint32,
    pub generation_lower: uint32,
}

pub const FREENEXT_END_OF_LIST: i32 = -1;
pub const FREENEXT_NOT_IN_LIST: i32 = -2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IOContext {
    IOCONTEXT_BULKREAD = 0,
    IOCONTEXT_BULKWRITE = 1,
    IOCONTEXT_INIT = 2,
    IOCONTEXT_NORMAL = 3,
    IOCONTEXT_VACUUM = 4,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalBufferLookupEnt {
    pub key: buftag,
    pub id: i32,
}

// C contract: the victim's buffer-header spinlock is STILL HELD (BM_LOCKED set
// in buf_state); into_parts is the only way out, so the caller can't drop the
// lock between selection and PinBuffer_Locked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "the victim's buffer header lock is held; pin or unlock it"]
pub struct Victim {
    pub buf_id: i32,
    pub buf_state: u32,
}

impl Victim {
    pub fn into_parts(self) -> (i32, u32) {
        (self.buf_id, self.buf_state)
    }
}

pub const MAX_BLOCK_NUMBER: BlockNumber = 0xFFFF_FFFE;

#[derive(Clone, Debug, Default)]
pub struct ExtendedRelation {
    pub first_block: BlockNumber,
    pub victim_buffers: Vec<Buffer>,
    pub extended_by: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buf_state_masks_match_buf_internals_h() {
        assert_eq!(BUF_REFCOUNT_MASK, 0x0003_FFFF);
        assert_eq!(BUF_USAGECOUNT_MASK, ((1u32 << 4) - 1) << BUF_REFCOUNT_BITS);
        assert_eq!(BUF_FLAG_MASK, ((1u32 << 10) - 1) << (BUF_REFCOUNT_BITS + 4));
        assert_eq!(
            BUF_REFCOUNT_MASK | BUF_USAGECOUNT_MASK | BUF_FLAG_MASK,
            u32::MAX
        );
        assert_eq!(BM_LOCKED & BUF_FLAG_MASK, BM_LOCKED);
    }
}
