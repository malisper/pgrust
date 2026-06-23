//! Vocabulary for `backend/commands/async.c` — LISTEN / NOTIFY.
//!
//! These structs / enums / constants mirror the (file-private but load-bearing)
//! definitions in `src/backend/commands/async.c` plus a few header constants the
//! file references.

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use ::types_core::{Oid, ProcNumber, TimestampTz, TransactionId, BLCKSZ};

/// `NAMEDATALEN` — identical to `::types_core::fmgr::NAMEDATALEN`; re-stated here so
/// the queue-entry layout is self-describing.
pub const NAMEDATALEN: usize = 64;

/// `InvalidPid` — `(-1)` (miscadmin.h).
pub const InvalidPid: i32 = -1;

/// `DatabaseRelationId` — OID of `pg_database` (catalog/pg_database_d.h).
/// async.c's `PreCommit_Notify` takes `LockSharedObject(DatabaseRelationId, ...)`
/// (the lock on "database 0") to serialize queue writers.
pub const DatabaseRelationId: Oid = 1262;

/// Maximum size of a NOTIFY payload, including terminating NUL (async.c):
/// `BLCKSZ - NAMEDATALEN - 128`.
pub const NOTIFY_PAYLOAD_MAX_LENGTH: usize = BLCKSZ - NAMEDATALEN - 128;

/// `QUEUE_PAGESIZE` — `BLCKSZ` (async.c).
pub const QUEUE_PAGESIZE: usize = BLCKSZ;

/// `QUEUE_CLEANUP_DELAY` — try to advance the tail every this-many pages.
pub const QUEUE_CLEANUP_DELAY: i64 = 4;

/// `QUEUE_FULL_WARN_INTERVAL` — warn at most once every 5 s.
pub const QUEUE_FULL_WARN_INTERVAL: i32 = 5000;

/// `MIN_HASHABLE_NOTIFIES` — threshold to build the per-(sub)xact dedup hashtab.
pub const MIN_HASHABLE_NOTIFIES: i32 = 16;

/// `QUEUEALIGN(len)` — `INTALIGN(len)`: round `len` up to the next multiple of 4.
#[inline]
pub const fn QUEUEALIGN(len: usize) -> usize {
    (len + (4 - 1)) & !(4 - 1)
}

/// Struct representing an entry in the global notify queue (async.c).  The
/// declaration has maximal length; a real queue entry's `data` area is only big
/// enough for the actual channel + payload strings.
#[derive(Clone, Debug)]
pub struct AsyncQueueEntry {
    /// total allocated length of entry
    pub length: i32,
    /// sender's database OID
    pub dboid: Oid,
    /// sender's XID
    pub xid: TransactionId,
    /// sender's PID
    pub srcPid: i32,
    /// `char data[NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH]`
    pub data: [u8; NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH],
}

/// `AsyncQueueEntryEmptySize` — `offsetof(AsyncQueueEntry, data) + 2`: the
/// minimum possible entry size (empty channel + payload), not counting alignment
/// padding.  `offsetof(AsyncQueueEntry, data)` is 16 (four 4-byte fields).
pub const AsyncQueueEntryEmptySize: usize = 16 + 2;

/// Struct describing a queue position (async.c).
///
/// `#[repr(C)]` — overlaid on the "Async Queue Control" shmem segment (inside
/// [`AsyncQueueControl`] / [`QueueBackendStatus`]); the layout must be
/// deterministic for the cross-backend shared-memory image.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueuePosition {
    /// SLRU page number
    pub page: i64,
    /// byte offset within page
    pub offset: i32,
}

/// Struct describing a listening backend's status (async.c).
///
/// `#[repr(C)]` — carved as the per-backend array in the "Async Queue Control"
/// shmem segment immediately after the [`AsyncQueueControl`] header.
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

/// Shared-memory state for LISTEN/NOTIFY excluding its SLRU stuff (async.c).
/// This is the **fixed header** of `AsyncQueueControl`; the
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
/// used by `AsyncShmemSize`/`AsyncShmemInit`.
pub const ASYNC_QUEUE_CONTROL_HEADER_SIZE: usize = core::mem::size_of::<AsyncQueueControl>();

/// `ListenActionKind` (async.c) — the pending LISTEN/UNLISTEN/UNLISTEN_ALL action
/// discriminator.  Backend-local.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenActionKind {
    LISTEN_LISTEN = 0,
    LISTEN_UNLISTEN = 1,
    LISTEN_UNLISTEN_ALL = 2,
}

/// LWLock identity for the two NOTIFY-queue locks (`NotifyQueueLock`,
/// `NotifyQueueTailLock`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotifyLock {
    /// `NotifyQueueLock` — protects head/tail/backend[] and the listener list.
    Queue,
    /// `NotifyQueueTailLock` — single-truncator lock for `asyncQueueAdvanceTail`.
    Tail,
}

/// LWLock acquire mode (`LW_SHARED` / `LW_EXCLUSIVE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotifyLockMode {
    Shared,
    Exclusive,
}
