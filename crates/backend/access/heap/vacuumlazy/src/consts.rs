//! Header-derived constants `vacuumlazy.c` reaches through `#include`s.
//!
//! These are plain integer/scalar constants transcribed 1:1 from the PostgreSQL
//! headers (`storage/block.h`, `storage/off.h`, `storage/bufmgr.h`,
//! `access/visibilitymapdefs.h`, `access/heapam.h`, `commands/progress.h`,
//! `common/relpath.h`, `storage/latch.h`, `utils/wait_event.h`, …). They are
//! file-scope to this crate so the algorithm modules can reference them by their
//! canonical names.

use types_core::{BlockNumber, Buffer, MultiXactId, OffsetNumber, TransactionId, XLogRecPtr};

// ---- storage/block.h, storage/buf.h, storage/off.h ----

/// `InvalidBlockNumber` (storage/block.h).
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `InvalidBuffer` (storage/buf.h).
pub const InvalidBuffer: Buffer = 0;
/// `InvalidOffsetNumber` (storage/off.h).
pub const InvalidOffsetNumber: OffsetNumber = 0;
/// `FirstOffsetNumber` (storage/off.h).
pub const FirstOffsetNumber: OffsetNumber = 1;
/// `MaxOffsetNumber` (storage/off.h).
pub const MaxOffsetNumber: OffsetNumber = types_storage::bufpage::MaxOffsetNumber;

/// `InvalidTransactionId` (access/transam.h).
pub const InvalidTransactionId: TransactionId = 0;
/// `InvalidMultiXactId` (access/multixact.h).
pub const InvalidMultiXactId: MultiXactId = 0;
/// `InvalidXLogRecPtr` (access/xlogdefs.h).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// ---- common/relpath.h ----

/// `MAIN_FORKNUM` (common/relpath.h) — the main relation fork.
pub const MAIN_FORKNUM: i32 = 0;

// ---- storage/bufmgr.h ----

/// `BUFFER_LOCK_UNLOCK` (storage/bufmgr.h).
pub const BUFFER_LOCK_UNLOCK: i32 = 0;
/// `BUFFER_LOCK_SHARE` (storage/bufmgr.h).
pub const BUFFER_LOCK_SHARE: i32 = 1;
/// `BUFFER_LOCK_EXCLUSIVE` (storage/bufmgr.h).
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

// ---- storage/lmgr.h lock modes (storage/lockdefs.h) ----

/// `NoLock` (storage/lockdefs.h).
pub const NoLock: i32 = 0;
/// `RowExclusiveLock` (storage/lockdefs.h).
pub const RowExclusiveLock: i32 = 3;
/// `AccessExclusiveLock` (storage/lockdefs.h).
pub const AccessExclusiveLock: i32 = 8;

// ---- access/visibilitymapdefs.h ----

/// `VISIBILITYMAP_ALL_VISIBLE`.
pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
/// `VISIBILITYMAP_ALL_FROZEN`.
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
/// `VISIBILITYMAP_VALID_BITS`.
pub const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

// ---- access/heapam.h prune options (access/heapam.h:42-43) ----

/// `HEAP_PAGE_PRUNE_MARK_UNUSED_NOW`.
pub const HEAP_PAGE_PRUNE_MARK_UNUSED_NOW: i32 = 1 << 0;
/// `HEAP_PAGE_PRUNE_FREEZE`.
pub const HEAP_PAGE_PRUNE_FREEZE: i32 = 1 << 1;

// ---- access/heapam.h PruneReason ----

/// `PRUNE_VACUUM_SCAN`.
pub const PRUNE_VACUUM_SCAN: i32 = 1;
/// `PRUNE_VACUUM_CLEANUP`.
pub const PRUNE_VACUUM_CLEANUP: i32 = 2;

// ---- access/heapam.h HTSV_Result ----

pub const HEAPTUPLE_DEAD: i32 = 0;
pub const HEAPTUPLE_LIVE: i32 = 1;
pub const HEAPTUPLE_RECENTLY_DEAD: i32 = 2;
pub const HEAPTUPLE_INSERT_IN_PROGRESS: i32 = 3;
pub const HEAPTUPLE_DELETE_IN_PROGRESS: i32 = 4;

// ---- storage/read_stream.h ----

/// `READ_STREAM_MAINTENANCE`.
pub const READ_STREAM_MAINTENANCE: i32 = 0x01;
/// `READ_STREAM_USE_BATCHING`.
pub const READ_STREAM_USE_BATCHING: i32 = 0x08;

// ---- storage/bufmgr.h ReadBufferMode ----

/// `RBM_NORMAL`.
pub const RBM_NORMAL: i32 = 0;

// ---- commands/progress.h ----

pub const PROGRESS_VACUUM_PHASE: i32 = 0;
pub const PROGRESS_VACUUM_TOTAL_HEAP_BLKS: i32 = 1;
pub const PROGRESS_VACUUM_HEAP_BLKS_SCANNED: i32 = 2;
pub const PROGRESS_VACUUM_HEAP_BLKS_VACUUMED: i32 = 3;
pub const PROGRESS_VACUUM_NUM_INDEX_VACUUMS: i32 = 4;
pub const PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES: i32 = 5;
pub const PROGRESS_VACUUM_DEAD_TUPLE_BYTES: i32 = 6;
pub const PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS: i32 = 7;
pub const PROGRESS_VACUUM_INDEXES_TOTAL: i32 = 8;
pub const PROGRESS_VACUUM_INDEXES_PROCESSED: i32 = 9;
pub const PROGRESS_VACUUM_DELAY_TIME: i32 = 10;

pub const PROGRESS_VACUUM_PHASE_SCAN_HEAP: i64 = 1;
pub const PROGRESS_VACUUM_PHASE_VACUUM_INDEX: i64 = 2;
pub const PROGRESS_VACUUM_PHASE_VACUUM_HEAP: i64 = 3;
pub const PROGRESS_VACUUM_PHASE_INDEX_CLEANUP: i64 = 4;
pub const PROGRESS_VACUUM_PHASE_TRUNCATE: i64 = 5;
pub const PROGRESS_VACUUM_PHASE_FINAL_CLEANUP: i64 = 6;

/// `PROGRESS_COMMAND_VACUUM` (utils/backend_progress.h).
pub const PROGRESS_COMMAND_VACUUM: i32 = 1;

// ---- commands/vacuum.h options (bits32) ----

/// `VACOPT_VERBOSE`.
pub const VACOPT_VERBOSE: u32 = 0x04;
/// `VACOPT_DISABLE_PAGE_SKIPPING`.
pub const VACOPT_DISABLE_PAGE_SKIPPING: u32 = 0x100;

// ---- storage/latch.h wait events ----

/// `WL_LATCH_SET`.
pub const WL_LATCH_SET: i32 = 1 << 0;
/// `WL_TIMEOUT`.
pub const WL_TIMEOUT: i32 = 1 << 3;
/// `WL_EXIT_ON_PM_DEATH`.
pub const WL_EXIT_ON_PM_DEATH: i32 = 1 << 5;

/// `PG_WAIT_TIMEOUT` (utils/wait_event.h).
pub const PG_WAIT_TIMEOUT: u32 = 0x0A00_0000;
/// `WAIT_EVENT_VACUUM_TRUNCATE` — the `WaitEventTimeout` member (`PG_WAIT_TIMEOUT | 8`).
pub const WAIT_EVENT_VACUUM_TRUNCATE: u32 = PG_WAIT_TIMEOUT | 8;

// ===========================================================================
// transaction-id wraparound comparisons (access/transam.h).
// ===========================================================================

/// `TransactionIdIsValid(xid)`.
#[inline]
pub fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)`.
#[inline]
pub fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= ::types_core::FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` — modular "id1 < id2".
#[inline]
pub fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `TransactionIdPrecedesOrEquals(id1, id2)` — modular "id1 <= id2".
#[inline]
pub fn transaction_id_precedes_or_equals(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1 <= id2;
    }
    (id1.wrapping_sub(id2) as i32) <= 0
}

/// `TransactionIdFollows(id1, id2)` — modular "id1 > id2".
#[inline]
pub fn transaction_id_follows(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1 > id2;
    }
    (id1.wrapping_sub(id2) as i32) > 0
}

/// `MultiXactIdIsValid(multi)`.
#[inline]
pub fn multi_xact_id_is_valid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

/// `MultiXactIdPrecedes(multi1, multi2)` — modular "multi1 < multi2".
#[inline]
pub fn multi_xact_id_precedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

/// `MultiXactIdPrecedesOrEquals(multi1, multi2)` — modular "multi1 <= multi2".
#[inline]
pub fn multi_xact_id_precedes_or_equals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) <= 0
}

/// `BufferIsValid(buffer)`.
#[inline]
pub fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `OffsetNumberNext(offsetNumber)` (storage/off.h:52).
#[inline]
pub fn offset_number_next(offset_number: OffsetNumber) -> OffsetNumber {
    1u16.wrapping_add(offset_number)
}

/// `pg_cmp_u16(a, b)` (common/int.h) — `(int32) a - (int32) b`.
#[inline]
pub fn pg_cmp_u16(a: u16, b: u16) -> i32 {
    (a as i32) - (b as i32)
}
