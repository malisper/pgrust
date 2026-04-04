use crate::storage::smgr::{ForkNumber, RelFileLocator, BLCKSZ};

pub const PAGE_SIZE: usize = BLCKSZ;
pub type Page = [u8; PAGE_SIZE];

pub type ClientId = u32;
pub type BufferId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferTag {
    pub rel: RelFileLocator,
    pub fork: ForkNumber,
    pub block: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOp {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingIo {
    pub buffer_id: BufferId,
    pub op: IoOp,
    pub tag: BufferTag,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPageResult {
    Hit { buffer_id: BufferId },
    ReadIssued { buffer_id: BufferId },
    WaitingOnRead { buffer_id: BufferId },
    AllBuffersPinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    WriteIssued,
    AlreadyClean,
    InProgress,
    Invalid,
}

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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BufferUsageStats {
    pub shared_hit: u64,
    pub shared_read: u64,
    pub shared_written: u64,
}

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
