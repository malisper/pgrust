//! ABI vocabulary for `backend/commands/async.c` — LISTEN / NOTIFY.
//!
//! These `#[repr(C)]` structs / enums / constants mirror the (file-private but
//! ABI-load-bearing) definitions in `src/backend/commands/async.c` plus a few
//! header constants the file references:
//!   * `AsyncQueueEntry`, `QueuePosition`, `QueueBackendStatus`,
//!     `AsyncQueueControl` — the shared-memory queue layout (async.c lines
//!     177-292).  `AsyncQueueControl.backend[]` is a `FLEXIBLE_ARRAY_MEMBER`, so
//!     the struct modeled here is its fixed header only; the per-backend
//!     `QueueBackendStatus` array is carved into the same shmem segment after it.
//!   * `ListenActionKind` / `ListenAction` — pending-action records (async.c
//!     lines 332-343).  Backend-local, but mirrored here for completeness.
//!   * `NOTIFY_PAYLOAD_MAX_LENGTH`, `QUEUE_CLEANUP_DELAY`, `QUEUE_PAGESIZE`,
//!     `QUEUE_FULL_WARN_INTERVAL`, `MIN_HASHABLE_NOTIFIES` — the queue tuning
//!     constants.
//!   * `DatabaseRelationId` (catalog/pg_database_d.h) and `InvalidPid`
//!     (miscadmin.h) — referenced directly by async.c.
//!
//! `QUEUEALIGN` / `AsyncQueueEntryEmptySize` are size helpers expressed as
//! `const fn` / `const` so callers compute identical values to the C macros.

use core::ffi::c_int;

use crate::types::{Oid, ProcNumber, TimestampTz, TransactionId};
use crate::BLCKSZ;

/// `NAMEDATALEN` (c.f. `fmgr.rs`), re-stated here so the queue-entry layout is
/// self-describing.  Identical to `crate::fmgr::NAMEDATALEN`.
pub const NAMEDATALEN: usize = 64;

/// `InvalidPid` — `(-1)` (miscadmin.h line 32).
pub const InvalidPid: i32 = -1;

/// `DatabaseRelationId` — OID of `pg_database` (catalog/pg_database_d.h line 23).
/// async.c's `PreCommit_Notify` takes `LockSharedObject(DatabaseRelationId, ...)`
/// (the lock on "database 0") to serialize queue writers.
pub const DatabaseRelationId: Oid = 1262;

/// Maximum size of a NOTIFY payload, including terminating NUL (async.c line
/// 163): `BLCKSZ - NAMEDATALEN - 128`.
pub const NOTIFY_PAYLOAD_MAX_LENGTH: usize = BLCKSZ - NAMEDATALEN - 128;

/// `QUEUE_PAGESIZE` — `BLCKSZ` (async.c line 311).
pub const QUEUE_PAGESIZE: usize = BLCKSZ;

/// `QUEUE_CLEANUP_DELAY` — try to advance the tail every this-many pages
/// (async.c line 238).
pub const QUEUE_CLEANUP_DELAY: i64 = 4;

/// `QUEUE_FULL_WARN_INTERVAL` — warn at most once every 5 s (async.c line 313).
pub const QUEUE_FULL_WARN_INTERVAL: c_int = 5000;

/// `MIN_HASHABLE_NOTIFIES` — threshold to build the per-(sub)xact dedup hashtab
/// (async.c line 397).
pub const MIN_HASHABLE_NOTIFIES: i32 = 16;

/// `QUEUEALIGN(len)` — `INTALIGN(len)` (async.c line 187): round `len` up to the
/// next multiple of 4 (`ALIGNOF_INT`).
#[inline]
pub const fn QUEUEALIGN(len: usize) -> usize {
    (len + (4 - 1)) & !(4 - 1)
}

/// Struct representing an entry in the global notify queue (async.c lines
/// 177-184).  The declaration has maximal length; a real queue entry's `data`
/// area is only big enough for the actual channel + payload strings.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AsyncQueueEntry {
    /// total allocated length of entry
    pub length: c_int,
    /// sender's database OID
    pub dboid: Oid,
    /// sender's XID
    pub xid: TransactionId,
    /// sender's PID
    pub srcPid: i32,
    /// `char data[NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH]`
    pub data: [u8; NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH],
}

/// `AsyncQueueEntryEmptySize` — `offsetof(AsyncQueueEntry, data) + 2` (async.c
/// line 189): the minimum possible entry size (empty channel + payload), not
/// counting alignment padding.  `offsetof(AsyncQueueEntry, data)` is 16 (four
/// 4-byte fields), so this is 18.
pub const AsyncQueueEntryEmptySize: usize = 16 + 2;

/// Struct describing a queue position (async.c lines 194-198).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueuePosition {
    /// SLRU page number
    pub page: i64,
    /// byte offset within page
    pub offset: c_int,
}

/// Struct describing a listening backend's status (async.c lines 243-249).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct QueueBackendStatus {
    /// either a PID or `InvalidPid`
    pub pid: i32,
    /// backend's database OID, or `InvalidOid`
    pub dboid: Oid,
    /// id of next listener, or `INVALID_PROC_NUMBER`
    pub nextListener: ProcNumber,
    /// backend has read queue up to here
    pub pos: QueuePosition,
}

/// Shared-memory state for LISTEN/NOTIFY excluding its SLRU stuff (async.c lines
/// 281-292).  This is the **fixed header** of `AsyncQueueControl`; the
/// `QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER]` array is carved into the
/// same `ShmemInitStruct` segment immediately after this header.
///
/// Protected by `NotifyQueueLock` / `NotifyQueueTailLock`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AsyncQueueControl {
    /// head points to the next free location
    pub head: QueuePosition,
    /// tail must be <= the queue position of every listening backend
    pub tail: QueuePosition,
    /// oldest unrecycled page; must be <= `tail.page`
    pub stopPage: i64,
    /// id of first listener, or `INVALID_PROC_NUMBER`
    pub firstListener: ProcNumber,
    /// time of last queue-full msg
    pub lastQueueFillWarn: TimestampTz,
    // `QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER]` follows in shmem.
}

/// `offsetof(AsyncQueueControl, backend)` — the size of the fixed header above,
/// used by `AsyncShmemSize`/`AsyncShmemInit` (async.c lines 490, 510).
pub const ASYNC_QUEUE_CONTROL_HEADER_SIZE: usize = core::mem::size_of::<AsyncQueueControl>();

/// `ListenActionKind` (async.c lines 332-337) — the pending LISTEN/UNLISTEN/
/// UNLISTEN_ALL action discriminator.  Backend-local.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenActionKind {
    LISTEN_LISTEN = 0,
    LISTEN_UNLISTEN = 1,
    LISTEN_UNLISTEN_ALL = 2,
}
