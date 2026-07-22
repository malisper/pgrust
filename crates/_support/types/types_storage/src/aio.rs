//! aio_types.h + the aio.h enums that cross crate seams.

use types_core::{BlockNumber, ForkNumber};

use crate::storage::RelFileLocator;

pub const PGAIO_SUBMIT_BATCH_SIZE: usize = 32;
pub const PGAIO_HANDLE_MAX_CALLBACKS: usize = 4;

pub const PGAIO_HF_SYNCHRONOUS: u8 = 1 << 0;
pub const PGAIO_HF_REFERENCES_LOCAL: u8 = 1 << 1;
pub const PGAIO_HF_BUFFERED: u8 = 1 << 2;

pub const PGAIO_OP_INVALID: u8 = 0;
pub const PGAIO_OP_READV: u8 = 1;
pub const PGAIO_OP_WRITEV: u8 = 2;

pub const PGAIO_TID_INVALID: u8 = 0;
pub const PGAIO_TID_SMGR: u8 = 1;

pub const PGAIO_HCB_INVALID: u8 = 0;
pub const PGAIO_HCB_MD_READV: u8 = 1;
pub const PGAIO_HCB_SHARED_BUFFER_READV: u8 = 2;
pub const PGAIO_HCB_LOCAL_BUFFER_READV: u8 = 3;

pub const PGAIO_RESULT_ERROR_BITS: u32 = 23;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PgAioResultStatus {
    #[default]
    Unknown = 0,
    Ok,
    Partial,
    Warning,
    Error,
}

// C packs id:6/status:3/error_data:23 + result into 8 bytes; the unpacked
// form trades 4 bytes per handle for direct field access.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioResult {
    pub id: u8,
    pub status: PgAioResultStatus,
    // Only the low PGAIO_RESULT_ERROR_BITS may be used (asserted at encode).
    pub error_data: u32,
    pub result: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioTargetSmgr {
    pub rlocator: RelFileLocator,
    pub blockNum: BlockNumber,
    pub nblocks: BlockNumber,
    pub forkNum: ForkNumber,
    pub is_temp: bool,
    pub skip_fsync: bool,
}

// Single-variant union in C (PgAioTargetData.smgr).
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioTargetData {
    pub smgr: PgAioTargetSmgr,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioReturn {
    pub result: PgAioResult,
    pub target_data: PgAioTargetData,
}

// The read/write op's shared descriptor (PgAioOpData); the fd is only valid
// in the defining backend unless reopened (aio.h NB on PgAioOpData).
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioOpDataRw {
    pub fd: i32,
    pub iov_length: u16,
    pub offset: u64,
}

/// The tag bufmgr stamps into `BufferDesc.io_wref.aio_index` to route WaitIO
/// between the pgaio engine and the divergent uring prefetch lane (both arm
/// the same field; see bufmgr read.rs).
pub const PGAIO_WREF_TAG: u32 = 1 << 30;
