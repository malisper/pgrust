//! xlogprefetcher.c — recovery WAL read-ahead: decode-ahead over the reader's
//! queue, filter table for not-yet-created block ranges, and a distance-driven
//! LSN ring gating prefetch issuance.
//!
//! Divergence from C: issuance goes through this repo's PrefetchSharedBuffer
//! (buffer-table probe, then io_uring pool read or posix_fadvise; DIO = no-op)
//! while recovery reads themselves stay on the sync buffered path — see
//! docs/optimizations/2026-07-04-recovery-prefetch-divergence.md.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

use datum::Datum;
use mcx::{vec_with_capacity_in, Mcx, PgFxHashMap, PgVec};
use types_core::{
    BackendType, BlockNumber, BufferIsValid, ForkNumber, InvalidBuffer, InvalidOid,
    InvalidXLogRecPtr, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_storage::{RelFileLocator, RelFileLocatorBackend, IO_DIRECT_DATA};
use xlogreader::{ReadAheadRecordInfo, XLogReaderRoutine, XLogReaderState};
use xlogreader_seams::BKPBLOCK_WILL_INIT;

#[cfg(test)]
mod tests;

const XLOGPREFETCHER_STATS_DISTANCE: XLogRecPtr = BLCKSZ as XLogRecPtr;
const XLOGPREFETCHER_SEQ_WINDOW_SIZE: usize = 4;
const XLOGPREFETCHER_DISTANCE_MULTIPLIER: u32 = 4;

// Rmgr rows 2/4 (rmgrlist.h); pinned against rmgr::RmgrTable by test.
const RM_SMGR_ID: u8 = 2;
const RM_DBASE_ID: u8 = 4;
const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;

pub use guc_tables::consts::{RECOVERY_PREFETCH_OFF, RECOVERY_PREFETCH_ON, RECOVERY_PREFETCH_TRY};

// C USE_PREFETCH = posix_fadvise; this repo's FilePrefetch also has a macOS
// F_RDADVISE arm, so macOS counts (divergence, see crate doc).
const USE_PREFETCH: bool = cfg!(any(target_os = "linux", target_os = "macos"));

thread_local! {
    static RECOVERY_PREFETCH: Cell<i32> = const { Cell::new(RECOVERY_PREFETCH_TRY) };
    static RECONFIGURE_COUNT: Cell<i32> = const { Cell::new(0) };
}

pub fn recovery_prefetch() -> i32 {
    RECOVERY_PREFETCH.with(Cell::get)
}

fn maintenance_io_concurrency() -> i32 {
    guc_tables::vars::maintenance_io_concurrency.read()
}

fn RecoveryPrefetchEnabled() -> bool {
    USE_PREFETCH && recovery_prefetch() != RECOVERY_PREFETCH_OFF && maintenance_io_concurrency() > 0
}

fn AmStartupProcess() -> bool {
    miscinit::GetMyBackendType() == BackendType::Startup
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LsnReadQueueNextStatus {
    NoIo,
    Io,
    Again,
}

#[derive(Clone, Copy, Debug, Default)]
struct LsnReadQueueEntry {
    io: bool,
    lsn: XLogRecPtr,
}

struct LsnReadQueue<'mcx> {
    max_inflight: u32,
    inflight: u32,
    completed: u32,
    head: u32,
    tail: u32,
    size: u32,
    queue: PgVec<'mcx, LsnReadQueueEntry>,
}

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

fn lrq_prefetch(
    lrq: &mut LsnReadQueue<'_>,
    mut next: impl FnMut(&mut XLogRecPtr) -> PgResult<LsnReadQueueNextStatus>,
) -> PgResult<()> {
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

fn lrq_complete_lsn(
    lrq: &mut LsnReadQueue<'_>,
    lsn: XLogRecPtr,
    enabled: bool,
    next: impl FnMut(&mut XLogRecPtr) -> PgResult<LsnReadQueueNextStatus>,
) -> PgResult<()> {
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

#[derive(Clone, Copy, Debug)]
struct XLogPrefetcherFilter {
    filter_until_replayed: XLogRecPtr,
    filter_from_block: BlockNumber,
}

pub struct XLogPrefetcher<'mcx> {
    mcx: Mcx<'mcx>,
    record: Option<ReadAheadRecordInfo>,
    next_block_id: i32,
    next_stats_shm_lsn: XLogRecPtr,
    filter_table: PgFxHashMap<'mcx, RelFileLocator, XLogPrefetcherFilter>,
    // C dlist: front = most recently updated; completed from the back.
    filter_queue: PgVec<'mcx, RelFileLocator>,
    recent_rlocator: [RelFileLocator; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    recent_block: [BlockNumber; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    recent_idx: usize,
    no_readahead_until: XLogRecPtr,
    streaming_read: Option<LsnReadQueue<'mcx>>,
    begin_ptr: XLogRecPtr,
    reconfigure_count: i32,
}

struct XLogPrefetchStats {
    reset_time: AtomicU64,
    prefetch: AtomicU64,
    hit: AtomicU64,
    skip_init: AtomicU64,
    skip_new: AtomicU64,
    skip_fpw: AtomicU64,
    skip_rep: AtomicU64,
    wal_distance: AtomicI32,
    block_distance: AtomicI32,
    io_depth: AtomicI32,
}

impl XLogPrefetchStats {
    fn zeroed_now() -> XLogPrefetchStats {
        XLogPrefetchStats {
            reset_time: AtomicU64::new(adt_timestamp::GetCurrentTimestamp() as u64),
            prefetch: AtomicU64::new(0),
            hit: AtomicU64::new(0),
            skip_init: AtomicU64::new(0),
            skip_new: AtomicU64::new(0),
            skip_fpw: AtomicU64::new(0),
            skip_rep: AtomicU64::new(0),
            wal_distance: AtomicI32::new(0),
            block_distance: AtomicI32::new(0),
            io_depth: AtomicI32::new(0),
        }
    }
}

static SHARED_STATS: OnceLock<XLogPrefetchStats> = OnceLock::new();

fn shared_stats() -> &'static XLogPrefetchStats {
    SHARED_STATS
        .get()
        .expect("XLogPrefetchShmemInit must run before SharedStats is used")
}

pub fn XLogPrefetchShmemSize() -> usize {
    core::mem::size_of::<XLogPrefetchStats>()
}

pub fn XLogPrefetchShmemInit() {
    const {
        assert!(!core::mem::needs_drop::<XLogPrefetchStats>());
    }
    let _ = SHARED_STATS.set(XLogPrefetchStats::zeroed_now());
}

pub fn XLogPrefetchShmemResetAfterCrash() {
    XLogPrefetchResetStats();
    let s = shared_stats();
    s.wal_distance.store(0, Relaxed);
    s.block_distance.store(0, Relaxed);
    s.io_depth.store(0, Relaxed);
}

pub fn XLogPrefetchResetStats() {
    let s = shared_stats();
    s.reset_time.store(adt_timestamp::GetCurrentTimestamp() as u64, Relaxed);
    s.prefetch.store(0, Relaxed);
    s.hit.store(0, Relaxed);
    s.skip_init.store(0, Relaxed);
    s.skip_new.store(0, Relaxed);
    s.skip_fpw.store(0, Relaxed);
    s.skip_rep.store(0, Relaxed);
}

pub fn XLogPrefetchReconfigure() {
    RECONFIGURE_COUNT.with(|c| c.set(c.get() + 1));
}

// C: plain increment through pg_atomic_write_u64 — single writer (startup).
fn XLogPrefetchIncrement(counter: &AtomicU64) {
    debug_assert!(AmStartupProcess() || !init_small::globals::IsUnderPostmaster());
    counter.store(counter.load(Relaxed) + 1, Relaxed);
}

impl<'mcx> XLogPrefetcher<'mcx> {
    pub fn XLogPrefetcherAllocate(mcx: Mcx<'mcx>) -> XLogPrefetcher<'mcx> {
        let s = shared_stats();
        s.wal_distance.store(0, Relaxed);
        s.block_distance.store(0, Relaxed);
        s.io_depth.store(0, Relaxed);

        XLogPrefetcher {
            mcx,
            record: None,
            next_block_id: 0,
            next_stats_shm_lsn: 0,
            filter_table: PgFxHashMap::with_hasher_in(Default::default(), mcx),
            filter_queue: PgVec::new_in(mcx),
            recent_rlocator: [RelFileLocator::default(); XLOGPREFETCHER_SEQ_WINDOW_SIZE],
            recent_block: [0; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
            recent_idx: 0,
            no_readahead_until: 0,
            streaming_read: None,
            begin_ptr: 0,
            // First use hits the reconfigure path, allocating streaming_read.
            reconfigure_count: RECONFIGURE_COUNT.with(Cell::get) - 1,
        }
    }

    pub fn XLogPrefetcherComputeStats(&mut self, reader: &XLogReaderState<'_>) {
        let wal_distance: i64 =
            match (reader.decode_queue_tail_lsn(), reader.decode_queue_head_lsn()) {
                (Some(tail), Some(head)) => tail as i64 - head as i64,
                _ => 0,
            };
        let lrq = self.streaming_read.as_ref().expect("streaming_read allocated");
        let io_depth = lrq.inflight;
        let completed = lrq.completed;

        let s = shared_stats();
        s.io_depth.store(io_depth as i32, Relaxed);
        s.block_distance.store((io_depth + completed) as i32, Relaxed);
        s.wal_distance.store(wal_distance as i32, Relaxed);

        self.next_stats_shm_lsn = reader.v.ReadRecPtr + XLOGPREFETCHER_STATS_DISTANCE;
    }

    fn XLogPrefetcherNextBlock<R: XLogReaderRoutine>(
        &mut self,
        reader: &mut XLogReaderState<'_>,
        routine: &mut R,
    ) -> PgResult<(LsnReadQueueNextStatus, XLogRecPtr)> {
        let replaying_lsn = reader.v.ReadRecPtr;
        let mut out_lsn: XLogRecPtr = 0;

        loop {
            let record: ReadAheadRecordInfo;
            if self.record.is_none() {
                // Blocking read-ahead only when nothing is queued to replay.
                let nonblocking = reader.XLogReaderHasQueuedRecordOrError();
                if nonblocking && replaying_lsn <= self.no_readahead_until {
                    return Ok((LsnReadQueueNextStatus::Again, out_lsn));
                }
                if reader.XLogReadAhead(routine, nonblocking)?.is_none() {
                    if nonblocking {
                        if let Some(tail) = reader.decode_queue_tail_lsn() {
                            self.no_readahead_until = tail;
                        }
                    }
                    return Ok((LsnReadQueueNextStatus::Again, out_lsn));
                }
                if !RecoveryPrefetchEnabled() {
                    return Ok((LsnReadQueueNextStatus::NoIo, InvalidXLogRecPtr));
                }
                record = reader.read_ahead_record_info().expect("XLogReadAhead queued a record");
                self.record = Some(record);
                self.next_block_id = 0;
            } else {
                record = self.record.expect("checked is_none above");
                // Invariant: the held record is always the decode-queue tail
                // (read-ahead only advances via this function).
                debug_assert_eq!(reader.decode_queue_tail_lsn(), Some(record.lsn));
            }

            if replaying_lsn < record.lsn {
                let rmid = record.xl_rmid;
                let record_type = record.xl_info & !transam_xlog::XLR_INFO_MASK;

                if rmid == transam_xlog::RM_XLOG_ID {
                    if record_type == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN
                        || record_type == transam_xlog::XLOG_END_OF_RECOVERY
                    {
                        // TLI may change at these records; pause all readahead.
                        self.no_readahead_until = record.lsn;
                    }
                } else if rmid == RM_DBASE_ID {
                    if record_type == XLOG_DBASE_CREATE_FILE_COPY {
                        // File-copy DB creation emits no per-relation WAL.
                        let md = reader.read_ahead_main_data();
                        if md.len() < 4 {
                            return Err(short_main_data());
                        }
                        let db_id = u32::from_ne_bytes(md[0..4].try_into().unwrap());
                        let rlocator = RelFileLocator::new(InvalidOid, db_id, InvalidOid);
                        self.XLogPrefetcherAddFilter(rlocator, 0, record.lsn)?;
                    }
                } else if rmid == RM_SMGR_ID {
                    if record_type == storage_xlog::XLOG_SMGR_CREATE {
                        let md = reader.read_ahead_main_data();
                        if md.len() < 16 {
                            return Err(short_main_data());
                        }
                        let rlocator = RelFileLocator::new(
                            u32::from_ne_bytes(md[0..4].try_into().unwrap()),
                            u32::from_ne_bytes(md[4..8].try_into().unwrap()),
                            u32::from_ne_bytes(md[8..12].try_into().unwrap()),
                        );
                        let forknum = i32::from_ne_bytes(md[12..16].try_into().unwrap());
                        if forknum == ForkNumber::MAIN_FORKNUM as i32 {
                            self.XLogPrefetcherAddFilter(rlocator, 0, record.lsn)?;
                        }
                    } else if record_type == storage_xlog::XLOG_SMGR_TRUNCATE {
                        let md = reader.read_ahead_main_data();
                        if md.len() < 16 {
                            return Err(short_main_data());
                        }
                        let blkno = u32::from_ne_bytes(md[0..4].try_into().unwrap());
                        let rlocator = RelFileLocator::new(
                            u32::from_ne_bytes(md[4..8].try_into().unwrap()),
                            u32::from_ne_bytes(md[8..12].try_into().unwrap()),
                            u32::from_ne_bytes(md[12..16].try_into().unwrap()),
                        );
                        self.XLogPrefetcherAddFilter(rlocator, blkno, record.lsn)?;
                    }
                }
            }

            while self.next_block_id <= record.max_block_id {
                let block_id = self.next_block_id;
                self.next_block_id += 1;

                let block = reader.read_ahead_block(block_id);
                if !block.in_use {
                    continue;
                }
                debug_assert!(!BufferIsValid(block.prefetch_buffer));

                // The IO (if issued) counts as done once this record replays.
                out_lsn = record.lsn;

                if block.forknum != ForkNumber::MAIN_FORKNUM {
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                if block.has_image {
                    XLogPrefetchIncrement(&shared_stats().skip_fpw);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                if block.flags & BKPBLOCK_WILL_INIT != 0 {
                    XLogPrefetchIncrement(&shared_stats().skip_init);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                if self.XLogPrefetcherIsFiltered(block.rlocator, block.blkno) {
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                if (0..XLOGPREFETCHER_SEQ_WINDOW_SIZE).any(|i| {
                    block.blkno == self.recent_block[i] && block.rlocator == self.recent_rlocator[i]
                }) {
                    XLogPrefetchIncrement(&shared_stats().skip_rep);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                self.recent_rlocator[self.recent_idx] = block.rlocator;
                self.recent_block[self.recent_idx] = block.blkno;
                self.recent_idx = (self.recent_idx + 1) % XLOGPREFETCHER_SEQ_WINDOW_SIZE;

                let key = RelFileLocatorBackend {
                    locator: block.rlocator,
                    backend: INVALID_PROC_NUMBER,
                };
                // C: smgropen() every time (no fast path for repeats yet).
                smgr::smgropen(block.rlocator, INVALID_PROC_NUMBER)?;

                if !smgr::smgrexists(key, ForkNumber::MAIN_FORKNUM)? {
                    self.XLogPrefetcherAddFilter(block.rlocator, 0, record.lsn)?;
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }
                if block.blkno >= smgr::smgrnblocks(key, block.forknum)? {
                    self.XLogPrefetcherAddFilter(block.rlocator, block.blkno, record.lsn)?;
                    XLogPrefetchIncrement(&shared_stats().skip_new);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                }

                let result = bufmgr::PrefetchSharedBuffer(
                    key,
                    RELPERSISTENCE_PERMANENT,
                    block.forknum,
                    block.blkno,
                )?;
                if BufferIsValid(result.recent_buffer) {
                    XLogPrefetchIncrement(&shared_stats().hit);
                    reader.set_read_ahead_block_prefetch_buffer(block_id, result.recent_buffer);
                    return Ok((LsnReadQueueNextStatus::NoIo, out_lsn));
                } else if result.initiated_io {
                    XLogPrefetchIncrement(&shared_stats().prefetch);
                    reader.set_read_ahead_block_prefetch_buffer(block_id, InvalidBuffer);
                    return Ok((LsnReadQueueNextStatus::Io, out_lsn));
                } else if fd::io_direct_flags() & IO_DIRECT_DATA == 0 {
                    // Exists + big enough yet nothing resident or issuable:
                    // smgr cache invalidation broke, or the file vanished.
                    elog::elog(
                        ERROR,
                        format!(
                            "could not prefetch relation {}/{}/{} block {}",
                            block.rlocator.spcOid,
                            block.rlocator.dbOid,
                            block.rlocator.relNumber,
                            block.blkno
                        ),
                    )?;
                    unreachable!("elog(ERROR) returns Err");
                }
            }

            // No readahead past the first record after BeginRead: callers like
            // checkpoint reads at PANIC emode must see exactly one record.
            if reader.decode_queue_tail_lsn() == Some(self.begin_ptr) {
                return Ok((LsnReadQueueNextStatus::Again, out_lsn));
            }
            self.record = None;
        }
    }

    fn XLogPrefetcherAddFilter(
        &mut self,
        rlocator: RelFileLocator,
        blockno: BlockNumber,
        lsn: XLogRecPtr,
    ) -> PgResult<()> {
        if let Some(filter) = self.filter_table.get_mut(&rlocator) {
            // Extend the lifetime; keep the lower block bound.
            filter.filter_until_replayed = lsn;
            filter.filter_from_block = filter.filter_from_block.min(blockno);
            if let Some(pos) = self.filter_queue.iter().position(|&r| r == rlocator) {
                self.filter_queue[..=pos].rotate_right(1);
            }
        } else {
            self.filter_table
                .try_reserve(1)
                .map_err(|_| self.mcx.oom(core::mem::size_of::<XLogPrefetcherFilter>()))?;
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
            self.filter_queue.push(rlocator);
            self.filter_queue.rotate_right(1);
        }
        Ok(())
    }

    fn XLogPrefetcherCompleteFilters(&mut self, replaying_lsn: XLogRecPtr) {
        while let Some(&tail_key) = self.filter_queue.last() {
            let filter = self
                .filter_table
                .get(&tail_key)
                .expect("queued filter present in table");
            if filter.filter_until_replayed >= replaying_lsn {
                break;
            }
            self.filter_queue.pop();
            self.filter_table.remove(&tail_key);
        }
    }

    fn XLogPrefetcherIsFiltered(&self, rlocator: RelFileLocator, blockno: BlockNumber) -> bool {
        if self.filter_queue.is_empty() {
            return false;
        }
        if let Some(filter) = self.filter_table.get(&rlocator) {
            if filter.filter_from_block <= blockno {
                return true;
            }
        }
        let db_locator = RelFileLocator::new(InvalidOid, rlocator.dbOid, InvalidOid);
        self.filter_table.contains_key(&db_locator)
    }

    pub fn XLogPrefetcherBeginRead(
        &mut self,
        reader: &mut XLogReaderState<'_>,
        rec_ptr: XLogRecPtr,
    ) {
        // Forces lrq re-allocation, forgetting any in-flight IO.
        self.reconfigure_count -= 1;
        self.begin_ptr = rec_ptr;
        self.no_readahead_until = 0;
        // C keeps its (now dangling) record pointer; identity here is the
        // decode-queue tail, which BeginRead resets.
        self.record = None;
        reader.XLogBeginRead(rec_ptr);
    }

    /// XLogReadRecord's interface (`Ok(None)` = failure, message via
    /// `reader.errormsg()`), plus read-ahead prefetch for future records.
    pub fn XLogPrefetcherReadRecord<R: XLogReaderRoutine>(
        &mut self,
        reader: &mut XLogReaderState<'_>,
        routine: &mut R,
    ) -> PgResult<Option<XLogRecPtr>> {
        let reconfigure_count = RECONFIGURE_COUNT.with(Cell::get);
        if reconfigure_count != self.reconfigure_count {
            drop(self.streaming_read.take());
            let (max_inflight, max_distance) = if RecoveryPrefetchEnabled() {
                let m = maintenance_io_concurrency() as u32;
                (m, m * XLOGPREFETCHER_DISTANCE_MULTIPLIER)
            } else {
                (1, 1)
            };
            self.streaming_read = Some(lrq_alloc(self.mcx, max_distance, max_inflight)?);
            self.reconfigure_count = reconfigure_count;
        }

        let replayed_up_to = reader.XLogReleasePreviousRecord();
        self.XLogPrefetcherCompleteFilters(replayed_up_to);

        // IO initiated by earlier WAL is done; this may prefetch further.
        let enabled = RecoveryPrefetchEnabled();
        {
            let mut lrq = self.streaming_read.take().expect("allocated above");
            let result = lrq_complete_lsn(&mut lrq, replayed_up_to, enabled, |out| {
                let (status, lsn) = self.XLogPrefetcherNextBlock(reader, routine)?;
                *out = lsn;
                Ok(status)
            });
            self.streaming_read = Some(lrq);
            result?;
        }

        if !reader.XLogReaderHasQueuedRecordOrError() {
            let mut lrq = self.streaming_read.take().expect("allocated above");
            debug_assert_eq!(lrq.inflight, 0);
            debug_assert_eq!(lrq.completed, 0);
            let result = lrq_prefetch(&mut lrq, |out| {
                let (status, lsn) = self.XLogPrefetcherNextBlock(reader, routine)?;
                *out = lsn;
                Ok(status)
            });
            self.streaming_read = Some(lrq);
            result?;
        }

        let Some(lsn) = reader.XLogNextRecord() else {
            return Ok(None);
        };

        // With tiny maintenance_io_concurrency this record may be partially
        // scanned; drop the reference (the C record-pointer comparison).
        if self.record.map(|r| r.lsn) == Some(lsn) {
            self.record = None;
        }

        if lsn >= self.next_stats_shm_lsn {
            self.XLogPrefetcherComputeStats(reader);
        }

        Ok(Some(lsn))
    }
}

#[track_caller]
#[cold]
fn short_main_data() -> Box<PgError> {
    Box::new(
        PgError::error("WAL record main_data shorter than the record struct it must hold")
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

pub fn fc_pg_stat_get_recovery_prefetch(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_recovery_prefetch: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    let s = shared_stats();
    let values = [
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
    srf.putvalues(&values, &[false; 10])?;
    Ok(srf.finish(fcinfo))
}

static XLOGPREFETCHER_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 6248,
    name: "pg_stat_get_recovery_prefetch",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_stat_get_recovery_prefetch,
}];

pub fn check_recovery_prefetch(new_value: i32) -> Result<(), &'static str> {
    if !USE_PREFETCH && new_value == RECOVERY_PREFETCH_ON {
        return Err(
            "\"recovery_prefetch\" is not supported on platforms that lack support for issuing read-ahead advice.",
        );
    }
    Ok(())
}

pub fn assign_recovery_prefetch(new_value: i32) {
    RECOVERY_PREFETCH.with(|c| c.set(new_value));
    if AmStartupProcess() {
        XLogPrefetchReconfigure();
    }
}

pub fn init_seams() {
    xlogprefetcher_seams::xlog_prefetch_reconfigure::set(XLogPrefetchReconfigure);
    xlogprefetcher_seams::xlog_prefetch_reset_stats::set(XLogPrefetchResetStats);
    guc_tables::vars::recovery_prefetch.install(guc_tables::GucVarAccessors {
        get: recovery_prefetch,
        set: |v| RECOVERY_PREFETCH.with(|c| c.set(v)),
    });
    guc_tables::hooks::check_recovery_prefetch.install(|new_value, _extra, _source| {
        match check_recovery_prefetch(*new_value) {
            Ok(()) => Ok(true),
            Err(detail) => {
                guc_seams::guc_check_errdetail::call(detail.to_string());
                Ok(false)
            }
        }
    });
    guc_tables::hooks::assign_recovery_prefetch
        .install(|new_value, _extra| assign_recovery_prefetch(new_value));
    fmgr_core::register_late_builtins(XLOGPREFETCHER_BUILTINS);
}
