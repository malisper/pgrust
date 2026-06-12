//! `enum BackendType` (`miscadmin.h`) — process-type discriminants. The
//! values match the C enum order exactly (parity matters: they appear in
//! protocol/launch plumbing and stats indexing).

pub type BackendType = u32;

pub const B_INVALID: BackendType = 0;
/* Backends and other backend-like processes */
pub const B_BACKEND: BackendType = 1;
pub const B_DEAD_END_BACKEND: BackendType = 2;
pub const B_AUTOVAC_LAUNCHER: BackendType = 3;
pub const B_AUTOVAC_WORKER: BackendType = 4;
pub const B_BG_WORKER: BackendType = 5;
pub const B_WAL_SENDER: BackendType = 6;
pub const B_SLOTSYNC_WORKER: BackendType = 7;
pub const B_STANDALONE_BACKEND: BackendType = 8;
/* Auxiliary processes */
pub const B_ARCHIVER: BackendType = 9;
pub const B_BG_WRITER: BackendType = 10;
pub const B_CHECKPOINTER: BackendType = 11;
pub const B_IO_WORKER: BackendType = 12;
pub const B_STARTUP: BackendType = 13;
pub const B_WAL_RECEIVER: BackendType = 14;
pub const B_WAL_SUMMARIZER: BackendType = 15;
pub const B_WAL_WRITER: BackendType = 16;
/* Logger is not connected to shared memory and has no PGPROC entry */
pub const B_LOGGER: BackendType = 17;

/// `BACKEND_NUM_TYPES` (`miscadmin.h`).
pub const BACKEND_NUM_TYPES: usize = (B_LOGGER + 1) as usize;
