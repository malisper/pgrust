//! Port of `src/backend/access/transam/xlogprefetcher.c` (PostgreSQL 18.3):
//! prefetching support for recovery.
//!
//! This module provides a drop-in replacement for an XLogReader that tries to
//! minimize I/O stalls by looking ahead in the WAL. If blocks that will be
//! accessed in the near future are not already in the buffer pool, it
//! initiates I/Os that might complete before the caller eventually needs the
//! data. When referenced blocks are found in the buffer pool already, the
//! buffer is recorded in the decoded record so that XLogReadBufferForRedo()
//! can try to avoid a second buffer mapping table lookup.
//!
//! Currently, only the main fork is considered for prefetching. Currently,
//! prefetching is only effective on systems where PrefetchBuffer() does
//! something useful (mainly Linux).
//!
//! # Model notes
//!
//! - The wrapped `XLogReaderState` is a `&mut` the prefetcher holds, exactly
//!   like the C `prefetcher->reader` pointer; the reader's operations cross
//!   `backend-access-transam-xlogreader-seams` until xlogreader lands. The
//!   prefetcher's "current" decoded record (C `prefetcher->record`, a pointer
//!   into the reader's decode buffer) is held as the `Copy` header facts
//!   ([`ReadAheadRecordInfo`]); block references and main data of that record
//!   (the reader's decode-queue tail while the reference is held) are re-read
//!   through the `read_ahead_record_*` seams. Record identity (the C
//!   `record == prefetcher->record` pointer comparison) is by start LSN,
//!   which is unique per record.
//! - `recovery_prefetch` (a GUC) and `XLogPrefetchReconfigureCount` are
//!   per-backend C globals → `thread_local!`.
//! - `SharedStats` lives in shared memory via `ShmemInitStruct`; a backend is
//!   a thread here, so the slot is a process-global `OnceLock` and the first
//!   `XLogPrefetchShmemInit` call is the C `!found` arm (the procsignal
//!   model). The C `int` gauges become `AtomicI32` (the struct is genuinely
//!   shared); the counters keep their C `pg_atomic_uint64` shape.
//! - `maintenance_io_concurrency` (bufmgr.c) and `io_direct_flags` (fd.c) are
//!   foreign per-backend globals: per the no-ambient-global-seams rule the
//!   public entry points take them as explicit parameters.
//! - The filter table is the C `HASH_BLOBS` hash keyed by `RelFileLocator`;
//!   the `dlist` ordering (most recently updated at the head) is a `PgVec`
//!   with front == head, drained from the back like `dlist_tail_element`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

use backend_access_transam_xlogreader_seams as xlogreader;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_smgr_seams as smgr;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_error::elog;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_utils_init_small_seams as init_small;
use mcx::{vec_with_capacity_in, Mcx, PgHashMap, PgVec};
use types_core::init::BackendType;
use types_core::{
    uint8, BlockNumber, BufferIsValid, ForkNumber::MAIN_FORKNUM, InvalidBuffer, InvalidOid,
    InvalidRelFileNumber, InvalidXLogRecPtr, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER,
};
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERROR};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_wal::rmgr::XLogReaderState;
use types_wal::{
    xl_dbase_create_file_copy_rec, xl_smgr_create, xl_smgr_truncate, ReadAheadRecordInfo,
    RelFileLocator, XLogNextRecordResult, BKPBLOCK_WILL_INIT, RM_DBASE_ID, RM_SMGR_ID, RM_XLOG_ID,
    XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// Constants (xlogprefetcher.c macros + the record opcodes it tests).
// ---------------------------------------------------------------------------

/// `XLOGPREFETCHER_STATS_DISTANCE` (xlogprefetcher.c) — every time we process
/// this much WAL, we'll update the values in pg_stat_recovery_prefetch.
const XLOGPREFETCHER_STATS_DISTANCE: XLogRecPtr = BLCKSZ as XLogRecPtr;

/// `XLOGPREFETCHER_SEQ_WINDOW_SIZE` (xlogprefetcher.c) — to detect repeated
/// access to the same block and skip useless extra system calls, we remember
/// a small window of recently prefetched blocks.
const XLOGPREFETCHER_SEQ_WINDOW_SIZE: usize = 4;

/// `XLOGPREFETCHER_DISTANCE_MULTIPLIER` (xlogprefetcher.c) — when
/// maintenance_io_concurrency is not saturated, we're prepared to look ahead
/// up to N times that number of block references.
const XLOGPREFETCHER_DISTANCE_MULTIPLIER: u32 = 4;

/// `XLOG_CHECKPOINT_SHUTDOWN` (catalog/pg_control.h).
const XLOG_CHECKPOINT_SHUTDOWN: uint8 = 0x00;
/// `XLOG_END_OF_RECOVERY` (catalog/pg_control.h).
const XLOG_END_OF_RECOVERY: uint8 = 0x90;
/// `XLOG_SMGR_CREATE` (catalog/storage_xlog.h).
const XLOG_SMGR_CREATE: uint8 = 0x10;
/// `XLOG_SMGR_TRUNCATE` (catalog/storage_xlog.h).
const XLOG_SMGR_TRUNCATE: uint8 = 0x20;
/// `XLOG_DBASE_CREATE_FILE_COPY` (commands/dbcommands_xlog.h).
const XLOG_DBASE_CREATE_FILE_COPY: uint8 = 0x00;

/// `IO_DIRECT_DATA` (storage/fd.h) — bit of fd.c's `io_direct_flags`.
pub const IO_DIRECT_DATA: i32 = 0x01;

/// Possible values for the `recovery_prefetch` GUC
/// (`RecoveryPrefetchValue`, access/xlogprefetcher.h).
pub const RECOVERY_PREFETCH_OFF: i32 = 0;
pub const RECOVERY_PREFETCH_ON: i32 = 1;
pub const RECOVERY_PREFETCH_TRY: i32 = 2;

/// `USE_PREFETCH` (pg_config_manual.h) — defined when `posix_fadvise` is
/// available (`USE_POSIX_FADVISE`). macOS/OpenBSD/Windows lack it.
const USE_PREFETCH: bool = cfg!(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
));

// ---------------------------------------------------------------------------
// GUCs / per-backend globals (xlogprefetcher.c:68-78).
// ---------------------------------------------------------------------------

thread_local! {
    /// `int recovery_prefetch = RECOVERY_PREFETCH_TRY;` — the GUC's
    /// per-backend variable, written by [`assign_recovery_prefetch`].
    static RECOVERY_PREFETCH: Cell<i32> = const { Cell::new(RECOVERY_PREFETCH_TRY) };

    /// `static int XLogPrefetchReconfigureCount = 0;`
    static XLOG_PREFETCH_RECONFIGURE_COUNT: Cell<i32> = const { Cell::new(0) };
}

/// Read the current `recovery_prefetch` GUC value.
pub fn recovery_prefetch() -> i32 {
    RECOVERY_PREFETCH.with(Cell::get)
}

/// `RecoveryPrefetchEnabled()` (xlogprefetcher.c) — `#ifdef USE_PREFETCH` it
/// is `recovery_prefetch != RECOVERY_PREFETCH_OFF && maintenance_io_concurrency
/// > 0`, otherwise `false`. `maintenance_io_concurrency` is bufmgr.c's GUC,
/// passed by the caller.
#[inline]
fn RecoveryPrefetchEnabled(maintenance_io_concurrency: i32) -> bool {
    USE_PREFETCH && recovery_prefetch() != RECOVERY_PREFETCH_OFF && maintenance_io_concurrency > 0
}

/// `AmStartupProcess()` (miscadmin.h) — `MyBackendType == B_STARTUP`.
fn AmStartupProcess() -> bool {
    init_small::my_backend_type::call() == BackendType::Startup
}

// ---------------------------------------------------------------------------
// LsnReadQueue (xlogprefetcher.c:83-291).
// ---------------------------------------------------------------------------

/// `LsnReadQueueNextStatus` (xlogprefetcher.c) — whether an IO should be
/// started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LsnReadQueueNextStatus {
    /// `LRQ_NEXT_NO_IO`.
    NoIo,
    /// `LRQ_NEXT_IO`.
    Io,
    /// `LRQ_NEXT_AGAIN`.
    Again,
}

/// One slot of the circular queue: `struct { bool io; XLogRecPtr lsn; }`.
#[derive(Clone, Copy, Debug, Default)]
struct LsnReadQueueEntry {
    io: bool,
    lsn: XLogRecPtr,
}

/// `LsnReadQueue` (xlogprefetcher.c) — a simple circular queue of LSNs, used
/// to control the number of (potentially) inflight IOs.
///
/// In C the queue carries a `next` callback pointer plus a `lrq_private`
/// value; here the owner passes the callback (`XLogPrefetcherNextBlock`) into
/// the `lrq_*` operators, preserving the exact ring/admission semantics
/// without the self-referential pointer.
struct LsnReadQueue<'mcx> {
    max_inflight: u32,
    inflight: u32,
    completed: u32,
    head: u32,
    tail: u32,
    size: u32,
    /// `queue[FLEXIBLE_ARRAY_MEMBER]` — the ring slots, palloc'd in the
    /// caller's context.
    queue: PgVec<'mcx, LsnReadQueueEntry>,
}

/// `lrq_alloc(max_distance, max_inflight, lrq_private, next)`
/// (xlogprefetcher.c:201). The `palloc` OOM `ereport(ERROR)` is the `Err`.
fn lrq_alloc<'mcx>(
    mcx: Mcx<'mcx>,
    max_distance: u32,
    max_inflight: u32,
) -> PgResult<LsnReadQueue<'mcx>> {
    debug_assert!(max_distance >= max_inflight);

    let size = max_distance + 1; // full ring buffer has a gap
    let mut queue = vec_with_capacity_in(mcx, size as usize)?;
    queue.resize(size as usize, LsnReadQueueEntry::default());

    Ok(LsnReadQueue {
        max_inflight,
        inflight: 0,
        completed: 0,
        head: 0,
        tail: 0,
        size,
        queue,
    })
}

// `lrq_free(lrq)` (xlogprefetcher.c:226) is `pfree(lrq)`: dropping the
// `LsnReadQueue` returns the ring's charge to its context.

/// `lrq_inflight(lrq)` (xlogprefetcher.c:232).
fn lrq_inflight(lrq: &LsnReadQueue<'_>) -> u32 {
    lrq.inflight
}

/// `lrq_completed(lrq)` (xlogprefetcher.c:238).
fn lrq_completed(lrq: &LsnReadQueue<'_>) -> u32 {
    lrq.completed
}

/// `lrq_prefetch(lrq)` (xlogprefetcher.c:244) — try to start as many IOs as
/// we can within our limits. `next` is the `XLogPrefetcherNextBlock` callback
/// (it writes the slot's LSN through its out-param); its `elog(ERROR)` arm is
/// the `Err`.
fn lrq_prefetch(
    lrq: &mut LsnReadQueue<'_>,
    mut next: impl FnMut(&mut XLogRecPtr) -> PgResult<LsnReadQueueNextStatus>,
) -> PgResult<()> {
    // Try to start as many IOs as we can within our limits.
    while lrq.inflight < lrq.max_inflight && lrq.inflight + lrq.completed < lrq.size - 1 {
        debug_assert!((lrq.head + 1) % lrq.size != lrq.tail);
        let head = lrq.head as usize;
        match next(&mut lrq.queue[head].lsn)? {
            LsnReadQueueNextStatus::Again => return Ok(()),
            LsnReadQueueNextStatus::Io => {
                lrq.queue[head].io = true;
                lrq.inflight += 1;
            }
            LsnReadQueueNextStatus::NoIo => {
                lrq.queue[head].io = false;
                lrq.completed += 1;
            }
        }
        lrq.head += 1;
        if lrq.head == lrq.size {
            lrq.head = 0;
        }
    }
    Ok(())
}

/// `lrq_complete_lsn(lrq, lsn)` (xlogprefetcher.c:271). The C function reads
/// `RecoveryPrefetchEnabled()` itself; the caller passes the result as
/// `enabled` (both inputs are stable across the call).
fn lrq_complete_lsn(
    lrq: &mut LsnReadQueue<'_>,
    lsn: XLogRecPtr,
    enabled: bool,
    next: impl FnMut(&mut XLogRecPtr) -> PgResult<LsnReadQueueNextStatus>,
) -> PgResult<()> {
    // We know that LSNs before 'lsn' have been replayed, so we can now assume
    // that any IOs that were started before then have finished.
    while lrq.tail != lrq.head && lrq.queue[lrq.tail as usize].lsn < lsn {
        if lrq.queue[lrq.tail as usize].io {
            lrq.inflight -= 1;
        } else {
            lrq.completed -= 1;
        }
        lrq.tail += 1;
        if lrq.tail == lrq.size {
            lrq.tail = 0;
        }
    }
    if enabled {
        lrq_prefetch(lrq, next)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// XLogPrefetcher / XLogPrefetcherFilter (xlogprefetcher.c:124-166).
// ---------------------------------------------------------------------------

/// `XLogPrefetcherFilter` (xlogprefetcher.c) — a temporary filter used to
/// track block ranges that haven't been created yet, whole relations that
/// haven't been created yet, and whole relations that (we assume) have
/// already been dropped, or will be created by bulk WAL operators.
///
/// The C struct embeds the hash key (`rlocator`) and a `dlist_node link`; the
/// key is the map key and the ordering lives in
/// [`XLogPrefetcher::filter_queue`].
#[derive(Clone, Copy, Debug)]
struct XLogPrefetcherFilter {
    filter_until_replayed: XLogRecPtr,
    filter_from_block: BlockNumber,
}

/// `struct XLogPrefetcher` (xlogprefetcher.c) — a mechanism that wraps an
/// XLogReader, prefetching blocks that will soon be referenced, to try to
/// avoid IO stalls.
pub struct XLogPrefetcher<'mcx, 'r, 'rdr> {
    /// The context the C `palloc`s charge (the ring buffer, the filter hash
    /// table and queue) — `CurrentMemoryContext` at `XLogPrefetcherAllocate`.
    mcx: Mcx<'mcx>,

    /* WAL reader and current reading state. */
    /// `XLogReaderState *reader`.
    reader: &'r mut XLogReaderState<'rdr>,
    /// `DecodedXLogRecord *record` — the header facts of the read-ahead
    /// record we hold a reference to (`None` is the C NULL).
    record: Option<ReadAheadRecordInfo>,
    /// `int next_block_id`.
    next_block_id: i32,

    /* When to publish stats. */
    next_stats_shm_lsn: XLogRecPtr,

    /* Book-keeping to avoid accessing blocks that don't exist yet. */
    /// `HTAB *filter_table` (`HASH_BLOBS`, key = `RelFileLocator`).
    filter_table: PgHashMap<'mcx, RelFileLocator, XLogPrefetcherFilter>,
    /// `dlist_head filter_queue` — front == dlist head (most recently
    /// updated).
    filter_queue: PgVec<'mcx, RelFileLocator>,

    /* Book-keeping to avoid repeat prefetches. */
    recent_rlocator: [RelFileLocator; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    recent_block: [BlockNumber; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    recent_idx: i32,

    /* Book-keeping to disable prefetching temporarily. */
    no_readahead_until: XLogRecPtr,

    /* IO depth manager. */
    /// `LsnReadQueue *streaming_read` — allocated on first
    /// `XLogPrefetcherReadRecord` (the reconfigure path).
    streaming_read: Option<LsnReadQueue<'mcx>>,

    begin_ptr: XLogRecPtr,

    reconfigure_count: i32,
}

// ---------------------------------------------------------------------------
// XLogPrefetchStats / SharedStats (xlogprefetcher.c:171-199).
// ---------------------------------------------------------------------------

/// `XLogPrefetchStats` (xlogprefetcher.c) — counters exposed in shared memory
/// for pg_stat_recovery_prefetch. The `pg_atomic_uint64` counters keep their
/// shape; the C plain-`int` gauges become `AtomicI32` because the struct is
/// genuinely cross-thread shared state here.
struct XLogPrefetchStats {
    /// Time of last reset (a `TimestampTz` stored in the C `uint64` atomic).
    reset_time: AtomicU64,
    /// Prefetches initiated.
    prefetch: AtomicU64,
    /// Blocks already in cache.
    hit: AtomicU64,
    /// Zero-inited blocks skipped.
    skip_init: AtomicU64,
    /// New/missing blocks filtered.
    skip_new: AtomicU64,
    /// FPWs skipped.
    skip_fpw: AtomicU64,
    /// Repeat accesses skipped.
    skip_rep: AtomicU64,

    /* Dynamic values */
    /// Number of WAL bytes ahead.
    wal_distance: AtomicI32,
    /// Number of block references ahead.
    block_distance: AtomicI32,
    /// Number of I/Os in progress.
    io_depth: AtomicI32,
}

/// `static XLogPrefetchStats *SharedStats;` — the `ShmemInitStruct` slot. A
/// backend is a thread, so the shared slot is a process-global installed by
/// [`XLogPrefetchShmemInit`].
static SHARED_STATS: OnceLock<XLogPrefetchStats> = OnceLock::new();

/// Dereference `SharedStats`; panics (the C NULL deref) if
/// `XLogPrefetchShmemInit` has not run.
fn shared_stats() -> &'static XLogPrefetchStats {
    SHARED_STATS
        .get()
        .expect("XLogPrefetchShmemInit must run before SharedStats is used")
}

/// `XLogPrefetchShmemSize(void)` (xlogprefetcher.c:293).
pub fn XLogPrefetchShmemSize() -> usize {
    core::mem::size_of::<XLogPrefetchStats>()
}

/// `XLogPrefetchResetStats(void)` (xlogprefetcher.c:302) — reset all counters
/// to zero.
pub fn XLogPrefetchResetStats() {
    let s = shared_stats();
    s.reset_time
        .store(timestamp::get_current_timestamp::call() as u64, Relaxed);
    s.prefetch.store(0, Relaxed);
    s.hit.store(0, Relaxed);
    s.skip_init.store(0, Relaxed);
    s.skip_new.store(0, Relaxed);
    s.skip_fpw.store(0, Relaxed);
    s.skip_rep.store(0, Relaxed);
}

/// `XLogPrefetchShmemInit(void)` (xlogprefetcher.c:314).
///
/// The first call is the C `!found` arm (initialize every field); later calls
/// attach. `PgResult` mirrors `ShmemInitStruct`'s out-of-shared-memory
/// `ereport(ERROR)` surface; the host-allocation model cannot fail here.
pub fn XLogPrefetchShmemInit() -> PgResult<()> {
    SHARED_STATS.get_or_init(|| XLogPrefetchStats {
        // pg_atomic_init_u64(&SharedStats->reset_time, GetCurrentTimestamp());
        reset_time: AtomicU64::new(timestamp::get_current_timestamp::call() as u64),
        prefetch: AtomicU64::new(0),
        hit: AtomicU64::new(0),
        skip_init: AtomicU64::new(0),
        skip_new: AtomicU64::new(0),
        skip_fpw: AtomicU64::new(0),
        skip_rep: AtomicU64::new(0),
        wal_distance: AtomicI32::new(0),
        block_distance: AtomicI32::new(0),
        io_depth: AtomicI32::new(0),
    });
    Ok(())
}

/// `XLogPrefetchReconfigure(void)` (xlogprefetcher.c:339) — called when any
/// GUC is changed that affects prefetching.
pub fn XLogPrefetchReconfigure() {
    XLOG_PREFETCH_RECONFIGURE_COUNT.with(|c| c.set(c.get() + 1));
}

/// `XLogPrefetchIncrement(counter)` (xlogprefetcher.c:350) — increment a
/// counter in shared memory. This is equivalent to `*counter++` on a plain
/// uint64 without any memory barrier or locking, except on platforms where
/// readers can't read uint64 without possibly observing a torn value.
fn XLogPrefetchIncrement(counter: &AtomicU64) {
    debug_assert!(AmStartupProcess() || !init_small::is_under_postmaster::call());
    counter.store(counter.load(Relaxed) + 1, Relaxed);
}

// ---------------------------------------------------------------------------
// Allocate / free / accessor (xlogprefetcher.c:361-404).
// ---------------------------------------------------------------------------

impl<'mcx, 'r, 'rdr> XLogPrefetcher<'mcx, 'r, 'rdr> {
    /// `XLogPrefetcherAllocate(reader)` (xlogprefetcher.c:361) — create a
    /// prefetcher that is ready to begin prefetching blocks referenced by WAL
    /// records. `mcx` is the C `CurrentMemoryContext` the `palloc0` /
    /// `hash_create` charge; their OOM `ereport(ERROR)` is the `Err`.
    pub fn XLogPrefetcherAllocate(
        mcx: Mcx<'mcx>,
        reader: &'r mut XLogReaderState<'rdr>,
    ) -> PgResult<XLogPrefetcher<'mcx, 'r, 'rdr>> {
        // ctl.keysize = sizeof(RelFileLocator);
        // ctl.entrysize = sizeof(XLogPrefetcherFilter);
        // hash_create("XLogPrefetcherFilterTable", 1024, &ctl,
        //             HASH_ELEM | HASH_BLOBS);
        let mut filter_table = PgHashMap::new_in(mcx);
        filter_table.try_reserve(1024).map_err(|_| {
            mcx.oom(1024 * core::mem::size_of::<(RelFileLocator, XLogPrefetcherFilter)>())
        })?;

        let prefetcher = XLogPrefetcher {
            mcx,
            reader,
            record: None,
            next_block_id: 0,
            next_stats_shm_lsn: 0,
            filter_table,
            // dlist_init(&prefetcher->filter_queue);
            filter_queue: PgVec::new_in(mcx),
            recent_rlocator: [RelFileLocator::default(); XLOGPREFETCHER_SEQ_WINDOW_SIZE],
            recent_block: [0; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
            recent_idx: 0,
            no_readahead_until: 0,
            streaming_read: None,
            begin_ptr: 0,
            // First usage will cause streaming_read to be allocated.
            reconfigure_count: XLOG_PREFETCH_RECONFIGURE_COUNT.with(Cell::get) - 1,
        };

        let s = shared_stats();
        s.wal_distance.store(0, Relaxed);
        s.block_distance.store(0, Relaxed);
        s.io_depth.store(0, Relaxed);

        Ok(prefetcher)
    }

    /// `XLogPrefetcherFree(prefetcher)` (xlogprefetcher.c:389) — destroy a
    /// prefetcher and release all resources (`lrq_free` + `hash_destroy` +
    /// `pfree` are the owned collections' `Drop`).
    pub fn XLogPrefetcherFree(self) {}

    /// `XLogPrefetcherGetReader(prefetcher)` (xlogprefetcher.c:400) — provide
    /// access to the reader.
    pub fn XLogPrefetcherGetReader(&mut self) -> &mut XLogReaderState<'rdr> {
        self.reader
    }

    /// `XLogPrefetcherComputeStats(prefetcher)` (xlogprefetcher.c:409) —
    /// update the statistics visible in the pg_stat_recovery_prefetch view.
    pub fn XLogPrefetcherComputeStats(&mut self) {
        // How far ahead of replay are we now?
        let wal_distance: i64 = match xlogreader::decode_queue_tail_lsn::call(&*self.reader) {
            Some(tail_lsn) => {
                let head_lsn = xlogreader::decode_queue_head_lsn::call(&*self.reader)
                    .expect("decode_queue_head is non-NULL when decode_queue_tail is");
                tail_lsn as i64 - head_lsn as i64
            }
            None => 0,
        };

        // How many IOs are currently in flight and completed?
        let lrq = self
            .streaming_read
            .as_ref()
            .expect("streaming_read allocated (C dereferences it unconditionally)");
        let io_depth = lrq_inflight(lrq);
        let completed = lrq_completed(lrq);

        // Update the instantaneous stats visible in pg_stat_recovery_prefetch.
        let s = shared_stats();
        s.io_depth.store(io_depth as i32, Relaxed);
        s.block_distance.store((io_depth + completed) as i32, Relaxed);
        s.wal_distance.store(wal_distance as i32, Relaxed);

        self.next_stats_shm_lsn = self.reader.ReadRecPtr + XLOGPREFETCHER_STATS_DISTANCE;
    }
}

// ---------------------------------------------------------------------------
// XLogPrefetcherNextBlock (xlogprefetcher.c:458).
// ---------------------------------------------------------------------------

impl XLogPrefetcher<'_, '_, '_> {
    /// `XLogPrefetcherNextBlock(pgsr_private, lsn)` (xlogprefetcher.c:458) —
    /// a callback that examines the next block reference in the WAL, and
    /// possibly starts an IO so that a later read will be fast.
    ///
    /// Returns the status plus the LSN the C writes through `*lsn`
    /// (meaningful on the IO / NO_IO returns):
    ///
    /// - `Again` if no more WAL data is available yet;
    /// - `Io` if the next block reference is for a main fork block that isn't
    ///   in the buffer pool, and the kernel has been asked to start reading
    ///   it; the I/O is considered done once the returned LSN is replayed;
    /// - `NoIo` if we examined the next block reference and found that it was
    ///   already in the buffer pool, or we decided for various reasons not to
    ///   prefetch.
    fn XLogPrefetcherNextBlock(
        &mut self,
        maintenance_io_concurrency: i32,
        io_direct_flags: i32,
    ) -> PgResult<(LsnReadQueueNextStatus, XLogRecPtr)> {
        let replaying_lsn = self.reader.ReadRecPtr;
        // The C out-param `*lsn`.
        let mut out_lsn: XLogRecPtr = 0;

        // We keep track of the record and block we're up to between calls
        // with prefetcher->record and prefetcher->next_block_id.
        loop {
            let record: ReadAheadRecordInfo;

            // Try to read a new future record, if we don't already have one.
            if self.record.is_none() {
                // If there are already records or an error queued up that
                // could be replayed, we don't want to block here. Otherwise,
                // it's OK to block waiting for more data: presumably the
                // caller has nothing else to do.
                let nonblocking =
                    xlogreader::xlog_reader_has_queued_record_or_error::call(&*self.reader);

                // Readahead is disabled until we replay past a certain point.
                if nonblocking && replaying_lsn <= self.no_readahead_until {
                    return Ok((LsnReadQueueNextStatus::Again, out_lsn));
                }

                let decoded =
                    match xlogreader::xlog_read_ahead::call(&mut *self.reader, nonblocking)? {
                        Some(decoded) => decoded,
                        None => {
                            // We can't read any more, due to an error or lack
                            // of data in nonblocking mode. Don't try to read
                            // ahead again until we've replayed everything
                            // already decoded.
                            if nonblocking {
                                if let Some(tail_lsn) =
                                    xlogreader::decode_queue_tail_lsn::call(&*self.reader)
                                {
                                    self.no_readahead_until = tail_lsn;
                                }
                            }
                            return Ok((LsnReadQueueNextStatus::Again, out_lsn));
                        }
                    };

                // If prefetching is disabled, we don't need to analyze the
                // record or issue any prefetches. We just need to cause one
                // record to be decoded.
                if !RecoveryPrefetchEnabled(maintenance_io_concurrency) {
                    out_lsn = InvalidXLogRecPtr;
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // We have a new record to process.
                self.record = Some(decoded);
                self.next_block_id = 0;
                record = decoded;
            } else {
                // Continue to process from last call, or last loop.
                record = self.record.expect("checked is_some above");
            }

            // Check for operations that require us to filter out block
            // ranges, or pause readahead completely.
            if replaying_lsn < record.lsn {
                let rmid = record.xl_rmid;
                let record_type = record.xl_info & !XLR_INFO_MASK;

                if rmid == RM_XLOG_ID {
                    if record_type == XLOG_CHECKPOINT_SHUTDOWN
                        || record_type == XLOG_END_OF_RECOVERY
                    {
                        // These records might change the TLI. Avoid potential
                        // bugs if we were to allow "read TLI" and "replay
                        // TLI" to differ without more analysis.
                        self.no_readahead_until = record.lsn;

                        // Fall through so we move past this record.
                    }
                } else if rmid == RM_DBASE_ID {
                    // When databases are created with the file-copy strategy,
                    // there are no WAL records to tell us about the creation
                    // of individual relations.
                    if record_type == XLOG_DBASE_CREATE_FILE_COPY {
                        let xlrec = {
                            let main_data =
                                xlogreader::read_ahead_record_main_data::call(&*self.reader);
                            xl_dbase_create_file_copy_rec::from_bytes(main_data)
                        }
                        .ok_or_else(short_main_data)?;
                        let rlocator =
                            RelFileLocator::new(InvalidOid, xlrec.db_id(), InvalidRelFileNumber);

                        // Don't try to prefetch anything in this database
                        // until it has been created, or we might confuse the
                        // blocks of different generations, if a database OID
                        // or relfilenumber is reused. It's also more
                        // efficient than discovering that relations don't
                        // exist on disk yet with ENOENT errors.
                        self.XLogPrefetcherAddFilter(rlocator, 0, record.lsn)?;
                    }
                } else if rmid == RM_SMGR_ID {
                    if record_type == XLOG_SMGR_CREATE {
                        let xlrec = {
                            let main_data =
                                xlogreader::read_ahead_record_main_data::call(&*self.reader);
                            xl_smgr_create::from_bytes(main_data)
                        }
                        .ok_or_else(short_main_data)?;

                        if xlrec.fork_num() == MAIN_FORKNUM {
                            // Don't prefetch anything for this whole relation
                            // until it has been created. Otherwise we might
                            // confuse the blocks of different generations, if
                            // a relfilenumber is reused. This also avoids the
                            // need to discover the problem via extra syscalls
                            // that report ENOENT.
                            self.XLogPrefetcherAddFilter(xlrec.rlocator(), 0, record.lsn)?;
                        }
                    } else if record_type == XLOG_SMGR_TRUNCATE {
                        let xlrec = {
                            let main_data =
                                xlogreader::read_ahead_record_main_data::call(&*self.reader);
                            xl_smgr_truncate::from_bytes(main_data)
                        }
                        .ok_or_else(short_main_data)?;

                        // Don't consider prefetching anything in the
                        // truncated range until the truncation has been
                        // performed.
                        self.XLogPrefetcherAddFilter(
                            xlrec.rlocator(),
                            xlrec.blkno(),
                            record.lsn,
                        )?;
                    }
                }
            }

            // Scan the block references, starting where we left off last
            // time.
            while self.next_block_id <= record.max_block_id {
                let block_id = self.next_block_id;
                self.next_block_id += 1;

                // DecodedBkpBlock *block = &record->blocks[block_id]; copy
                // out the consumed fields so the reader borrow ends here.
                let (in_use, rlocator, forknum, blkno, flags, has_image, prefetch_buffer) = {
                    let block = xlogreader::read_ahead_record_block::call(&*self.reader, block_id);
                    (
                        block.in_use(),
                        block.rlocator(),
                        block.forknum(),
                        block.blkno(),
                        block.flags(),
                        block.has_image(),
                        block.prefetch_buffer(),
                    )
                };

                if !in_use {
                    continue;
                }

                debug_assert!(!BufferIsValid(prefetch_buffer));

                // Record the LSN of this record. When it's replayed,
                // LsnReadQueue will consider any IOs submitted for earlier
                // LSNs to be finished.
                out_lsn = record.lsn;

                // We don't try to prefetch anything but the main fork for
                // now.
                if forknum != MAIN_FORKNUM {
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // If there is a full page image attached, we won't be reading
                // the page, so don't bother trying to prefetch.
                if has_image {
                    XLogPrefetchIncrement(&shared_stats().skip_fpw);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // There is no point in reading a page that will be zeroed.
                if flags & BKPBLOCK_WILL_INIT != 0 {
                    XLogPrefetchIncrement(&shared_stats().skip_init);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // Should we skip prefetching this block due to a filter?
                if self.XLogPrefetcherIsFiltered(rlocator, blkno) {
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // There is no point in repeatedly prefetching the same block.
                for i in 0..XLOGPREFETCHER_SEQ_WINDOW_SIZE {
                    if blkno == self.recent_block[i] && rlocator == self.recent_rlocator[i] {
                        // XXX If we also remembered where it was, we could
                        // set recent_buffer so that recovery could skip
                        // smgropen() and a buffer table lookup.
                        XLogPrefetchIncrement(&shared_stats().skip_rep);
                        return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                    }
                }
                self.recent_rlocator[self.recent_idx as usize] = rlocator;
                self.recent_block[self.recent_idx as usize] = blkno;
                self.recent_idx = (self.recent_idx + 1) % XLOGPREFETCHER_SEQ_WINDOW_SIZE as i32;

                // We could try to have a fast path for repeated references to
                // the same relation (with some scheme to handle invalidations
                // safely), but for now we'll call smgropen() every time (the
                // flattened smgr seams take the locator + backend pair).
                let smgr_rlocator = storage_locator(rlocator);

                // If the relation file doesn't exist on disk, for example
                // because we're replaying after a crash and the file will be
                // created and then unlinked by WAL that hasn't been replayed
                // yet, suppress further prefetching in the relation until
                // this record is replayed.
                if !smgr::smgrexists::call(smgr_rlocator, INVALID_PROC_NUMBER, MAIN_FORKNUM)? {
                    self.XLogPrefetcherAddFilter(rlocator, 0, record.lsn)?;
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // If the relation isn't big enough to contain the referenced
                // block yet, suppress prefetching of this block and higher
                // until this record is replayed.
                if blkno >= smgr::smgrnblocks::call(smgr_rlocator, INVALID_PROC_NUMBER, forknum)? {
                    self.XLogPrefetcherAddFilter(rlocator, blkno, record.lsn)?;
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                // Try to initiate prefetching.
                let result = bufmgr::prefetch_shared_buffer::call(
                    smgr_rlocator,
                    INVALID_PROC_NUMBER,
                    forknum,
                    blkno,
                )?;
                if BufferIsValid(result.recent_buffer) {
                    // Cache hit, nothing to do.
                    XLogPrefetchIncrement(&shared_stats().hit);
                    xlogreader::set_read_ahead_record_prefetch_buffer::call(
                        &mut *self.reader,
                        block_id,
                        result.recent_buffer,
                    );
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                } else if result.initiated_io {
                    // Cache miss, I/O (presumably) started.
                    XLogPrefetchIncrement(&shared_stats().prefetch);
                    xlogreader::set_read_ahead_record_prefetch_buffer::call(
                        &mut *self.reader,
                        block_id,
                        InvalidBuffer,
                    );
                    return Ok((LsnReadQueueNextStatus::Io, out_lsn));
                } else if io_direct_flags & IO_DIRECT_DATA == 0 {
                    // This shouldn't be possible, because we already
                    // determined that the relation exists on disk and is big
                    // enough. Something is wrong with the cache invalidation
                    // for smgrexists(), smgrnblocks(), or the file was
                    // unlinked or truncated beneath our feet?
                    elog(
                        ERROR,
                        format!(
                            "could not prefetch relation {}/{}/{} block {}",
                            rlocator.spc_oid(),
                            rlocator.db_oid(),
                            rlocator.rel_number(),
                            blkno
                        ),
                    )?;
                    unreachable!("elog(ERROR) always returns Err");
                }
            }

            // Several callsites need to be able to read exactly one record
            // without any internal readahead. Examples: xlog.c reading
            // checkpoint records with emode set to PANIC, which might
            // otherwise cause XLogPageRead() to panic on some future page,
            // and xlog.c determining where to start writing WAL next, which
            // depends on the contents of the reader's internal buffer after
            // reading one record. Therefore, don't even think about
            // prefetching until the first record after
            // XLogPrefetcherBeginRead() has been consumed.
            if let Some(tail_lsn) = xlogreader::decode_queue_tail_lsn::call(&*self.reader) {
                if tail_lsn == self.begin_ptr {
                    return Ok((LsnReadQueueNextStatus::Again, out_lsn));
                }
            }

            // Advance to the next record.
            self.record = None;
        }
    }
}

/// The `from_bytes` `None` arm: the C cast would read past the record's main
/// data — data corruption.
fn short_main_data() -> PgError {
    PgError::error("WAL record main_data shorter than the record struct it must hold")
        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

/// Convert the WAL-side locator (types-wal, what decoded blocks carry) to the
/// storage-side locator (types-storage, what the smgr/bufmgr seams take).
/// Same C struct (`storage/relfilelocator.h`); the two trimmed copies predate
/// this port (see DESIGN_DEBT.md).
fn storage_locator(rlocator: RelFileLocator) -> types_storage::RelFileLocator {
    types_storage::RelFileLocator {
        spcOid: rlocator.spc_oid(),
        dbOid: rlocator.db_oid(),
        relNumber: rlocator.rel_number(),
    }
}

// ---------------------------------------------------------------------------
// pg_stat_get_recovery_prefetch (xlogprefetcher.c:823).
// ---------------------------------------------------------------------------

/// `pg_stat_get_recovery_prefetch(PG_FUNCTION_ARGS)` (xlogprefetcher.c:823) —
/// expose statistics about recovery prefetching.
pub fn pg_stat_get_recovery_prefetch(fcinfo: &mut FunctionCallInfoBaseData<'_>) -> PgResult<Datum> {
    const PG_STAT_GET_RECOVERY_PREFETCH_COLS: usize = 10;

    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    let s = shared_stats();
    let values: [Datum; PG_STAT_GET_RECOVERY_PREFETCH_COLS] = [
        // TimestampTzGetDatum(pg_atomic_read_u64(&SharedStats->reset_time))
        Datum::from_i64(s.reset_time.load(Relaxed) as i64),
        Datum::from_i64(s.prefetch.load(Relaxed) as i64),
        Datum::from_i64(s.hit.load(Relaxed) as i64),
        Datum::from_i64(s.skip_init.load(Relaxed) as i64),
        Datum::from_i64(s.skip_new.load(Relaxed) as i64),
        Datum::from_i64(s.skip_fpw.load(Relaxed) as i64),
        Datum::from_i64(s.skip_rep.load(Relaxed) as i64),
        Datum::from_i32(s.wal_distance.load(Relaxed)),
        Datum::from_i32(s.block_distance.load(Relaxed)),
        Datum::from_i32(s.io_depth.load(Relaxed)),
    ];
    let nulls = [false; PG_STAT_GET_RECOVERY_PREFETCH_COLS];

    funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;

    // return (Datum) 0;
    Ok(Datum::null())
}

// ---------------------------------------------------------------------------
// Filter helpers (xlogprefetcher.c:855-956).
// ---------------------------------------------------------------------------

impl XLogPrefetcher<'_, '_, '_> {
    /// `XLogPrefetcherAddFilter(prefetcher, rlocator, blockno, lsn)`
    /// (xlogprefetcher.c:855) — don't prefetch any blocks >= 'blockno' from a
    /// given 'rlocator', until 'lsn' has been replayed.
    fn XLogPrefetcherAddFilter(
        &mut self,
        rlocator: RelFileLocator,
        blockno: BlockNumber,
        lsn: XLogRecPtr,
    ) -> PgResult<()> {
        // filter = hash_search(filter_table, &rlocator, HASH_ENTER, &found);
        let found = self.filter_table.contains_key(&rlocator);
        if !found {
            // Don't allow any prefetching of this block or higher until
            // replayed. The hash_search(HASH_ENTER) palloc OOM is the `Err`;
            // reserve both growths first so the table and queue stay
            // consistent.
            self.filter_table.try_reserve(1).map_err(|_| {
                self.mcx
                    .oom(core::mem::size_of::<(RelFileLocator, XLogPrefetcherFilter)>())
            })?;
            self.filter_queue
                .try_reserve(1)
                .map_err(|_| self.mcx.oom(core::mem::size_of::<RelFileLocator>()))?;
            self.filter_table.insert(
                rlocator,
                XLogPrefetcherFilter {
                    filter_until_replayed: lsn,
                    filter_from_block: blockno,
                },
            );
            // dlist_push_head(&prefetcher->filter_queue, &filter->link):
            // newest at the front.
            self.filter_queue.push(rlocator);
            self.filter_queue.rotate_right(1);
        } else {
            // We were already filtering this rlocator. Extend the filter's
            // lifetime to cover this WAL record, but leave the lower of the
            // block numbers there because we don't want to have to track
            // individual blocks.
            let filter = self
                .filter_table
                .get_mut(&rlocator)
                .expect("contains_key checked above");
            filter.filter_until_replayed = lsn;
            // filter->filter_from_block = Min(filter->filter_from_block,
            //                                 blockno);
            filter.filter_from_block = filter.filter_from_block.min(blockno);
            // dlist_delete(&filter->link);
            // dlist_push_head(&prefetcher->filter_queue, &filter->link);
            if let Some(pos) = self.filter_queue.iter().position(|&r| r == rlocator) {
                self.filter_queue[..=pos].rotate_right(1);
            }
        }
        Ok(())
    }

    /// `XLogPrefetcherCompleteFilters(prefetcher, replaying_lsn)`
    /// (xlogprefetcher.c:893) — have we replayed any records that caused us
    /// to begin filtering a block range? That means that relations should
    /// have been created, extended or dropped as required, so we can stop
    /// filtering out accesses to a given relfilenumber.
    fn XLogPrefetcherCompleteFilters(&mut self, replaying_lsn: XLogRecPtr) {
        // filter = dlist_tail_element(...) — the least recently updated
        // filter, at the back of the queue.
        while let Some(&tail_key) = self.filter_queue.last() {
            let filter = self
                .filter_table
                .get(&tail_key)
                .expect("queued filter present in table");

            if filter.filter_until_replayed >= replaying_lsn {
                break;
            }

            // dlist_delete(&filter->link);
            self.filter_queue.pop();
            // hash_search(filter_table, filter, HASH_REMOVE, NULL);
            self.filter_table.remove(&tail_key);
        }
    }

    /// `XLogPrefetcherIsFiltered(prefetcher, rlocator, blockno)`
    /// (xlogprefetcher.c:913) — check if a given block should be skipped due
    /// to a filter.
    fn XLogPrefetcherIsFiltered(&self, rlocator: RelFileLocator, blockno: BlockNumber) -> bool {
        // Test for empty queue first, because we expect it to be empty most
        // of the time and we can avoid the hash table lookup in that case.
        if !self.filter_queue.is_empty() {
            // See if the block range is filtered.
            if let Some(filter) = self.filter_table.get(&rlocator) {
                if filter.filter_from_block <= blockno {
                    return true;
                }
            }

            // See if the whole database is filtered.
            //   rlocator.relNumber = InvalidRelFileNumber;
            //   rlocator.spcOid = InvalidOid;
            let db_rlocator =
                RelFileLocator::new(InvalidOid, rlocator.db_oid(), InvalidRelFileNumber);
            if self.filter_table.contains_key(&db_rlocator) {
                return true;
            }
        }

        false
    }
}

// ---------------------------------------------------------------------------
// XLogPrefetcherBeginRead / XLogPrefetcherReadRecord
// (xlogprefetcher.c:961-1078).
// ---------------------------------------------------------------------------

impl XLogPrefetcher<'_, '_, '_> {
    /// `XLogPrefetcherBeginRead(prefetcher, recPtr)` (xlogprefetcher.c:961) —
    /// a wrapper for `XLogBeginRead()` that also resets the prefetcher.
    pub fn XLogPrefetcherBeginRead(&mut self, rec_ptr: XLogRecPtr) {
        // This will forget about any in-flight IO.
        self.reconfigure_count -= 1;

        // Book-keeping to avoid readahead on first read.
        self.begin_ptr = rec_ptr;

        self.no_readahead_until = 0;

        // This will forget about any queued up records in the decoder.
        xlogreader::xlog_begin_read::call(&mut *self.reader, rec_ptr);
    }

    /// `XLogPrefetcherReadRecord(prefetcher, errmsg)` (xlogprefetcher.c:980)
    /// — a wrapper for `XLogReadRecord()` that provides the same interface,
    /// but also tries to initiate I/O for blocks referenced in future WAL
    /// records.
    ///
    /// The C function returns `&record->header` or NULL with `*errmsg` set;
    /// the [`XLogNextRecordResult`] carries the same outcome (on `Record` the
    /// record itself is the reader's current record, readable there).
    /// `maintenance_io_concurrency` (bufmgr.c GUC) and `io_direct_flags`
    /// (fd.c) are this backend's current values, passed explicitly.
    pub fn XLogPrefetcherReadRecord(
        &mut self,
        maintenance_io_concurrency: i32,
        io_direct_flags: i32,
    ) -> PgResult<XLogNextRecordResult<'_>> {
        // See if it's time to reset the prefetching machinery, because a
        // relevant GUC was changed.
        let reconfigure_count = XLOG_PREFETCH_RECONFIGURE_COUNT.with(Cell::get);
        if reconfigure_count != self.reconfigure_count {
            let max_distance: u32;
            let max_inflight: u32;

            // if (prefetcher->streaming_read) lrq_free(...);
            drop(self.streaming_read.take());

            if RecoveryPrefetchEnabled(maintenance_io_concurrency) {
                debug_assert!(maintenance_io_concurrency > 0);
                max_inflight = maintenance_io_concurrency as u32;
                max_distance = max_inflight * XLOGPREFETCHER_DISTANCE_MULTIPLIER;
            } else {
                max_inflight = 1;
                max_distance = 1;
            }

            self.streaming_read = Some(lrq_alloc(self.mcx, max_distance, max_inflight)?);

            self.reconfigure_count = reconfigure_count;
        }

        // Release last returned record, if there is one, as it's now been
        // replayed.
        let replayed_up_to = xlogreader::xlog_release_previous_record::call(&mut *self.reader);

        // Can we drop any filters yet? If we were waiting for a relation to
        // be created or extended, it is now OK to access blocks in the
        // covered range.
        self.XLogPrefetcherCompleteFilters(replayed_up_to);

        // All IO initiated by earlier WAL is now completed. This might
        // trigger further prefetching.
        let enabled = RecoveryPrefetchEnabled(maintenance_io_concurrency);
        self.lrq_complete_lsn_self(
            replayed_up_to,
            enabled,
            maintenance_io_concurrency,
            io_direct_flags,
        )?;

        // If there's nothing queued yet, then start prefetching to cause at
        // least one record to be queued.
        if !xlogreader::xlog_reader_has_queued_record_or_error::call(&*self.reader) {
            {
                let lrq = self
                    .streaming_read
                    .as_ref()
                    .expect("streaming_read allocated by the reconfigure path");
                debug_assert_eq!(lrq_inflight(lrq), 0);
                debug_assert_eq!(lrq_completed(lrq), 0);
            }
            self.lrq_prefetch_self(maintenance_io_concurrency, io_direct_flags)?;
        }

        // Read the next record.
        let lsn = match xlogreader::xlog_next_record::call(&mut *self.reader) {
            None => {
                // The C NULL return: *errmsg points into the reader's
                // errormsg_buf.
                let errmsg = xlogreader::xlog_reader_deferred_errmsg::call(&*self.reader);
                return Ok(XLogNextRecordResult::NoRecord { errmsg });
            }
            Some(lsn) => lsn,
        };

        // The record we just got is the "current" one, for the benefit of
        // the XLogRecXXX() macros: it is reader->record, reachable through
        // the reader.

        // If maintenance_io_concurrency is set very low, we might have
        // started prefetching some but not all of the blocks referenced in
        // the record we're about to return. Forget about the rest of the
        // blocks in this record by dropping the prefetcher's reference to
        // it. (C compares the record pointers; record start LSNs are
        // unique.)
        if self.record.map(|r| r.lsn) == Some(lsn) {
            self.record = None;
        }

        // See if it's time to compute some statistics, because enough WAL
        // has been processed.
        if lsn >= self.next_stats_shm_lsn {
            self.XLogPrefetcherComputeStats();
        }

        Ok(XLogNextRecordResult::Record { lsn })
    }

    /// Bridge [`lrq_prefetch`] to the [`Self::XLogPrefetcherNextBlock`]
    /// callback: the queue is taken out of `self` so the callback can borrow
    /// the rest (the C callback reaches the prefetcher through
    /// `lrq_private`).
    fn lrq_prefetch_self(
        &mut self,
        maintenance_io_concurrency: i32,
        io_direct_flags: i32,
    ) -> PgResult<()> {
        let mut lrq = self
            .streaming_read
            .take()
            .expect("streaming_read allocated (C dereferences it unconditionally)");
        let result = lrq_prefetch(&mut lrq, |lsn| {
            let (status, out_lsn) =
                self.XLogPrefetcherNextBlock(maintenance_io_concurrency, io_direct_flags)?;
            *lsn = out_lsn;
            Ok(status)
        });
        self.streaming_read = Some(lrq);
        result
    }

    /// Bridge [`lrq_complete_lsn`] to the callback (see
    /// [`Self::lrq_prefetch_self`]).
    fn lrq_complete_lsn_self(
        &mut self,
        lsn: XLogRecPtr,
        enabled: bool,
        maintenance_io_concurrency: i32,
        io_direct_flags: i32,
    ) -> PgResult<()> {
        let mut lrq = self
            .streaming_read
            .take()
            .expect("streaming_read allocated (C dereferences it unconditionally)");
        let result = lrq_complete_lsn(&mut lrq, lsn, enabled, |out| {
            let (status, out_lsn) =
                self.XLogPrefetcherNextBlock(maintenance_io_concurrency, io_direct_flags)?;
            *out = out_lsn;
            Ok(status)
        });
        self.streaming_read = Some(lrq);
        result
    }
}

// ---------------------------------------------------------------------------
// GUC hooks (xlogprefetcher.c:1080-1101).
// ---------------------------------------------------------------------------

/// `check_recovery_prefetch(new_value, extra, source)` (xlogprefetcher.c:1080)
/// — GUC check hook. The `Err` carries the `GUC_check_errdetail` text
/// (`#ifndef USE_PREFETCH`: `RECOVERY_PREFETCH_ON` is rejected).
pub fn check_recovery_prefetch(new_value: i32) -> PgResult<()> {
    if !USE_PREFETCH && new_value == RECOVERY_PREFETCH_ON {
        return Err(PgError::error(
            "\"recovery_prefetch\" is not supported on platforms that lack support for issuing read-ahead advice.",
        ));
    }

    Ok(())
}

/// `assign_recovery_prefetch(new_value, extra)` (xlogprefetcher.c:1094) — GUC
/// assign hook: reconfigure prefetching, because a setting it depends on
/// changed.
pub fn assign_recovery_prefetch(new_value: i32) {
    RECOVERY_PREFETCH.with(|c| c.set(new_value));
    if AmStartupProcess() {
        XLogPrefetchReconfigure();
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// This unit's functions are reachable by direct dependency (xlogrecovery,
/// ipci, bufmgr's GUC hooks, guc all sit above it acyclically); there is no
/// `backend-access-transam-xlogprefetcher-seams` crate and nothing to
/// install.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
