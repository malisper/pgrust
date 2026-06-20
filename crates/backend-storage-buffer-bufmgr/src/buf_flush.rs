//! The flush / checkpoint / background-writer path of `bufmgr.c`.
//!
//! F5 (this stage): the relation/database/checkpoint write sweeps and the
//! background-writer pacing built on top of the in-crate write core
//! ([`BufferManager::flush_buffer`]). Ported function-by-function from
//! `src/backend/storage/buffer/bufmgr.c`:
//!
//!  * `FlushBuffer` (4284) — write one dirty buffer to storage (the write core:
//!    `StartBufferIO` / WAL flush / `smgrwrite` / `TerminateBufferIO`).
//!  * `FlushOneBuffer` (5346) — flush a single pinned, exclusive-locked buffer.
//!  * `FlushRelationBuffers` (4936) — flush every dirty buffer of one relation.
//!  * `FlushRelationsAllBuffers` (5026) — flush dirty buffers for a set of rels.
//!  * `FlushDatabaseBuffers` (5304) — flush every dirty buffer of one database.
//!  * `BufferSync` (3344) — the checkpoint write pass (sort + throttled write).
//!  * `SyncOneBuffer` (3918) — write one candidate buffer during sync/bgwriter.
//!  * `BgBufferSync` (3620) — the background-writer LRU write pass.
//!  * `CheckPointBuffers` (4210) — checkpoint entry: `BufferSync`.
//!  * `WritebackContextInit` (6394) — initialise a writeback accumulator.
//!  * `ScheduleBufferTagForWriteback` (6420) / `IssuePendingWritebacks` (6469) —
//!    the kernel write-back hint accumulator + drain (over `smgrwriteback`).
//!  * `ckpt_buforder_comparator` (6340) / `buffertag_comparator` (6306) — the
//!    checkpoint-sort orderings (pure comparators).
//!
//! The per-buffer content lock for the share-locked flush is acquired DIRECTLY
//! through the lwlock dep (no central content-lock seam), and the actual disk
//! write rides the landed [`backend_storage_smgr_smgr::smgrwrite`] /
//! `smgrwriteback`. The checkpointer/bgwriter throttling + pgstat accounting
//! call out to per-owner seams installed when those subsystems port
//! (panic-until-owner — sanctioned, those callers do not exist yet).
#![allow(dead_code)]

use core::cmp::Ordering as CmpOrdering;

use types_core::primitive::{BlockNumber, Buffer, ForkNumber, Oid, BLCKSZ};
use types_error::{PgError, PgResult};
use types_storage::buf::{
    buftag, BM_CHECKPOINT_NEEDED, BM_DIRTY, BM_IO_IN_PROGRESS, BM_JUST_DIRTIED, BM_PERMANENT,
    BM_VALID,
};
use types_storage::storage::LWLockMode;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use backend_storage_buffer_bufmgr_seams as sb;
use backend_storage_lmgr_lwlock as lwlock;
use backend_storage_smgr_smgr as smgr;

use crate::mgr::BufferManager;

/// `BUF_WRITTEN` (bufmgr.c:80) — SyncOneBuffer wrote the buffer.
const BUF_WRITTEN: i32 = 0x01;
/// `BUF_REUSABLE` (bufmgr.c:81) — the buffer is a replacement candidate.
const BUF_REUSABLE: i32 = 0x02;
/// `RELS_BSEARCH_THRESHOLD` (bufmgr.c:83) — switch the rel match to bsearch
/// above this many relations.
const RELS_BSEARCH_THRESHOLD: usize = 20;
/// `WRITEBACK_MAX_PENDING_FLUSHES` (buf_internals.h) — the hard cap on a
/// writeback context's pending high-water mark.
const WRITEBACK_MAX_PENDING_FLUSHES: i32 = 256;

/// `CHECKPOINT_IS_SHUTDOWN` (xlog.h) — a shutdown checkpoint.
const CHECKPOINT_IS_SHUTDOWN: i32 = 1 << 0;
/// `CHECKPOINT_END_OF_RECOVERY` (xlog.h) — an end-of-recovery checkpoint.
const CHECKPOINT_END_OF_RECOVERY: i32 = 1 << 1;
/// `CHECKPOINT_FLUSH_ALL` (xlog.h) — flush all pages, including unlogged.
const CHECKPOINT_FLUSH_ALL: i32 = 1 << 5;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BUF_STATE_GET_USAGECOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_usagecount(buf_state: u32) -> u32 {
    (buf_state & types_storage::buf::BUF_USAGECOUNT_MASK) / types_storage::buf::BUF_USAGECOUNT_ONE
}

/// `BufferDescriptorGetBuffer(buf)` — the 1-based [`Buffer`] for a 0-based id.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

/// `BufTagGetRelFileLocator(tag)` — recover the relfilelocator a tag names.
#[inline]
fn tag_to_rlocator(tag: &buftag) -> RelFileLocator {
    RelFileLocator {
        spcOid: tag.spcOid,
        dbOid: tag.dbOid,
        relNumber: tag.relNumber,
    }
}

/// `RelFileLocatorEquals` over a buffer tag's relfilelocator part — does this
/// buffer belong to `rlocator`?
#[inline]
fn tag_matches_rlocator(tag: &buftag, rlocator: &RelFileLocator) -> bool {
    tag.spcOid == rlocator.spcOid
        && tag.dbOid == rlocator.dbOid
        && tag.relNumber == rlocator.relNumber
}

/// `rlocator_comparator(p1, p2)` (bufmgr.c:6258) — order relfilelocators by
/// relNumber, then dbOid, then spcOid (the field order the C `bsearch`/`qsort`
/// uses for the drop/flush membership tests).
pub(crate) fn rlocator_comparator(a: &RelFileLocator, b: &RelFileLocator) -> CmpOrdering {
    if a.relNumber < b.relNumber {
        return CmpOrdering::Less;
    } else if a.relNumber > b.relNumber {
        return CmpOrdering::Greater;
    }
    if a.dbOid < b.dbOid {
        return CmpOrdering::Less;
    } else if a.dbOid > b.dbOid {
        return CmpOrdering::Greater;
    }
    if a.spcOid < b.spcOid {
        return CmpOrdering::Less;
    } else if a.spcOid > b.spcOid {
        return CmpOrdering::Greater;
    }
    CmpOrdering::Equal
}

// ---------------------------------------------------------------------------
// One per-buffer writeback tag and the writeback accumulator (buf_internals.h /
// bufmgr.c). These are bufmgr-private POD; the C `WritebackContext` is a stack
// local threaded through the sync paths.
// ---------------------------------------------------------------------------

/// `PendingWriteback` (buf_internals.h) — one accumulated kernel-writeback hint.
#[derive(Clone, Copy, Debug, Default)]
pub struct PendingWriteback {
    /// The buffer tag whose backing block should be hinted for writeback.
    pub tag: buftag,
}

/// `WritebackContext` (buf_internals.h) — accumulates per-buffer writeback hints
/// (`sync_file_range`/`smgrwriteback`) issued during a checkpoint/bgwriter pass,
/// so they can be coalesced and dispatched in physical order.
#[derive(Clone, Debug, Default)]
pub struct WritebackContext {
    /// `*max_pending` — high-water mark before the context auto-flushes. 0
    /// disables writeback control.
    pub max_pending: i32,
    /// `nr_pending` — number of accumulated hints.
    pub nr_pending: i32,
    /// `pending_writebacks[]` — the accumulated hints.
    pub pending_writebacks: Vec<PendingWriteback>,
}

/// `CkptSortItem` (bufmgr.c) — one checkpoint-sort entry (the dirty buffers to
/// write, sortable into physical order).
#[derive(Clone, Copy, Debug, Default)]
pub struct CkptSortItem {
    /// `tsId` — tablespace OID (sort key 1, also the write-balancing key).
    pub ts_id: Oid,
    /// `relNumber` — relation file number (sort key 2).
    pub rel_number: Oid,
    /// `forkNum` — fork (sort key 3).
    pub fork_num: ForkNumber,
    /// `blockNum` — block number (sort key 4).
    pub block_num: BlockNumber,
    /// `buf_id` — the 0-based buffer id this entry names.
    pub buf_id: i32,
}

impl BufferManager {
    // -----------------------------------------------------------------------
    // FlushBuffer write core (bufmgr.c:4284)
    // -----------------------------------------------------------------------

    /// `FlushBuffer(buf, reln, io_object, io_context)` (bufmgr.c:4284) —
    /// physically write out a shared buffer. The caller must hold a pin on the
    /// buffer and have at least share-locked the buffer contents.
    ///
    /// The write core: `StartBufferIO(buf, false, false)` (return on a lost
    /// race), read `PageGetLSN` + clear `BM_JUST_DIRTIED` under the header lock,
    /// `XLogFlush(recptr)` for `BM_PERMANENT` buffers, the checksum copy +
    /// `smgrwrite`, then `TerminateBufferIO(buf, true, 0, true, false)`.
    pub(crate) fn flush_buffer(&self, buf_id: usize) -> PgResult<()> {
        // StartBufferIO(buf, false, false): false if someone else flushed it.
        if !self.start_flush_io(buf_id)? {
            return Ok(());
        }

        let tag = self.desc_tag(buf_id);

        // Run PageGetLSN + clear BM_JUST_DIRTIED while holding the header lock,
        // since we don't have the buffer locked exclusively in all cases. The
        // cleared BM_JUST_DIRTIED lets TerminateBufferIO detect a concurrent
        // re-dirty: if the page is dirtied again during the write,
        // BM_JUST_DIRTIED is re-set and TerminateBufferIO will leave BM_DIRTY
        // on. (bufmgr.c:4324.)
        let mut buf_state = self.lock_buf_hdr(buf_id);
        let recptr = self.with_block(buf_id, |block| {
            let page = backend_storage_page::PageRef::new(block).expect("buffer block is BLCKSZ");
            backend_storage_page::PageGetLSN(&page)
        });
        buf_state &= !BM_JUST_DIRTIED;
        self.unlock_buf_hdr(buf_id, buf_state);

        // WAL: flush WAL up to the page LSN for permanent buffers (bufmgr.c:4344).
        if buf_state & BM_PERMANENT != 0 {
            backend_access_transam_xlog_seams::xlog_flush::call(recptr)?;
        }

        // Copy the page to private storage and stamp the checksum, since we hold
        // only a shared lock and other processes might be updating hint bits in
        // it. (PageSetChecksumCopy, bufmgr.c:4363.)
        let mut page = [0u8; BLCKSZ];
        self.with_block(buf_id, |block| page.copy_from_slice(block));
        {
            let mut p = backend_storage_page::PageMut::new(&mut page).expect("page is BLCKSZ");
            backend_storage_page::PageSetChecksumInplace(&mut p, tag.blockNum);
        }

        // smgrwrite the page out (bufmgr.c:4377).
        let backend = RelFileLocatorBackend {
            locator: tag_to_rlocator(&tag),
            backend: types_core::primitive::INVALID_PROC_NUMBER,
        };
        // `FlushBuffer(buf, reln, ...)` (bufmgr.c:4332): when no `reln`
        // (SMgrRelation) is supplied — the bgwriter/checkpointer flush path
        // never has one — C does `reln = smgropen(BufTagGetRelFileLocator(&tag),
        // INVALID_PROC_NUMBER)` before the smgr write. This port's smgr surface
        // is keyed by `RelFileLocatorBackend` with no threaded handle, so the
        // SMgrRelation cache entry must already exist in THIS process's
        // thread-local smgr cache. A bgwriter/checkpointer flushing a relation it
        // has not itself opened would otherwise reach the md layer with no entry
        // ("md operation on an unopened SMgrRelation"). `smgropen` is idempotent.
        smgr::smgropen(backend.locator, backend.backend)?;
        smgr::smgrwrite(backend, tag.forkNum, tag.blockNum, &page, false)?;

        // pgBufferUsage.shared_blks_written++ (bufmgr.c:4397).
        sb::count_buffer_write::call();

        // TerminateBufferIO(buf, true, 0, true, false): clear dirty unless the
        // page was re-dirtied during the write (BM_JUST_DIRTIED re-set); forget
        // the I/O from the resource owner (bufmgr.c:4403).
        self.terminate_buffer_io(buf_id, true, 0, true, false)
    }

    /// `StartBufferIO` flavoured for a flush (write): begin I/O only if the
    /// buffer is still dirty. Returns false if it was already cleaned.
    ///
    /// This mirrors `StartBufferIO(buf, false, false)` (bufmgr.c:6038) for the
    /// write side: an in-flight flush already cleared `BM_DIRTY` once it wins the
    /// `BM_IO_IN_PROGRESS` race, so a concurrent flusher that finds the buffer
    /// no longer dirty must back out (returns false) rather than double-write.
    fn start_flush_io(&self, buf_id: usize) -> PgResult<bool> {
        // Make room to remember the buffer I/O before taking the header lock
        // (bufmgr.c:6042).
        sb::resowner_enlarge::call()?;
        loop {
            let buf_state = self.lock_buf_hdr(buf_id);
            if buf_state & BM_IO_IN_PROGRESS == 0 {
                if buf_state & BM_DIRTY == 0 {
                    self.unlock_buf_hdr(buf_id, buf_state);
                    return Ok(false);
                }
                self.unlock_buf_hdr(buf_id, buf_state | BM_IO_IN_PROGRESS);
                // ResourceOwnerRememberBufferIO(CurrentResourceOwner, ...) (bufmgr.c:6068).
                sb::remember_buffer_io::call(buf_id_to_buffer(buf_id as i32));
                return Ok(true);
            }
            self.unlock_buf_hdr(buf_id, buf_state);
            self.wait_io(buf_id)?;
        }
    }

    /// `FlushOneBuffer(buffer)` (bufmgr.c:5346) — flush a single pinned,
    /// exclusive-locked buffer.
    pub fn FlushOneBuffer(&self, buffer: Buffer) -> PgResult<()> {
        // Assert(BufferIsPinned(buffer)); Assert(LWLockHeldByMeInMode(
        // BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE)).
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        self.flush_buffer(buf_id)
    }

    // -----------------------------------------------------------------------
    // SyncOneBuffer (bufmgr.c:3918)
    // -----------------------------------------------------------------------

    /// `SyncOneBuffer(buf_id, skip_recently_used, wb_context)` (bufmgr.c:3918) —
    /// process a single buffer during syncing. Returns a bitmask of
    /// [`BUF_WRITTEN`] / [`BUF_REUSABLE`]. Locking/branch order is identical to C.
    fn sync_one_buffer(
        &self,
        buf_id: i32,
        skip_recently_used: bool,
        wb_context: &mut WritebackContext,
    ) -> PgResult<i32> {
        let buf_id = buf_id as usize;
        let mut result: i32 = 0;

        // Make sure we can handle the pin (bufmgr.c:3927).
        self.private_refcount().ReservePrivateRefCountEntry();
        sb::resowner_enlarge::call()?;

        // We can check this without taking the buffer content lock so long as we
        // mark pages dirty in access methods *before* logging changes with
        // XLogInsert() (bufmgr.c:3936).
        let buf_state = self.lock_buf_hdr(buf_id);

        if buf_state_get_refcount(buf_state) == 0 && buf_state_get_usagecount(buf_state) == 0 {
            result |= BUF_REUSABLE;
        } else if skip_recently_used {
            // Caller told us not to write recently-used buffers (bufmgr.c:3944).
            self.unlock_buf_hdr(buf_id, buf_state);
            return Ok(result);
        }

        if buf_state & BM_VALID == 0 || buf_state & BM_DIRTY == 0 {
            // It's clean, so nothing to do (bufmgr.c:3950).
            self.unlock_buf_hdr(buf_id, buf_state);
            return Ok(result);
        }

        // Pin it, share-lock it, write it. (FlushBuffer will do nothing if the
        // buffer is clean by the time we've locked it.) (bufmgr.c:3958.)
        self.pin_buffer_locked(buf_id, buf_state);

        let lock = self.content_lock(buf_id);
        lwlock::LWLockAcquire(
            lock,
            LWLockMode::LW_SHARED,
            backend_storage_lmgr_proc_seams::my_proc_number::call(),
        )?;

        let flush = self.flush_buffer(buf_id);

        lwlock::LWLockRelease(lock)?;
        flush?;

        let tag = self.desc_tag(buf_id);

        // UnpinBuffer(bufHdr) — with-owner (bufmgr.c:3972).
        self.unpin_buffer(buf_id);

        // SyncOneBuffer() is only called by checkpointer and bgwriter, so
        // IOContext will always be IOCONTEXT_NORMAL (bufmgr.c:3978).
        self.schedule_buffer_tag_for_writeback(wb_context, &tag)?;

        Ok(result | BUF_WRITTEN)
    }

    // -----------------------------------------------------------------------
    // BufferSync (bufmgr.c:3344)
    // -----------------------------------------------------------------------

    /// `BufferSync(flags)` (bufmgr.c:3344) — write out all dirty buffers in the
    /// pool at checkpoint time. The checkpoint request `flags` govern which
    /// buffers are written and the throttling.
    pub fn BufferSync(&self, flags: i32) -> PgResult<()> {
        let nbuffers = self.nbuffers() as usize;

        // Unless this is a shutdown checkpoint or we have been explicitly told,
        // we write only permanent, dirty buffers (bufmgr.c:3360).
        let mut mask = BM_DIRTY;
        if flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FLUSH_ALL) == 0
        {
            mask |= BM_PERMANENT;
        }

        // Loop over all buffers, marking the ones that need to be written with
        // BM_CHECKPOINT_NEEDED. Count them as we go (num_to_scan) (bufmgr.c:3375).
        let mut ckpt_buffer_ids: Vec<CkptSortItem> = Vec::new();
        ckpt_buffer_ids
            .try_reserve(nbuffers)
            .map_err(|_| PgError::error("BufferSync: out of memory for checkpoint sort array"))?;

        let mut num_to_scan: i32 = 0;
        for buf_id in 0..nbuffers {
            // Header spinlock is enough to examine BM_DIRTY, see SyncOneBuffer.
            let mut buf_state = self.lock_buf_hdr(buf_id);

            if buf_state & mask == mask {
                buf_state |= BM_CHECKPOINT_NEEDED;

                let tag = self.desc_tag(buf_id);
                ckpt_buffer_ids.push(CkptSortItem {
                    buf_id: buf_id as i32,
                    ts_id: tag.spcOid,
                    rel_number: tag.relNumber,
                    fork_num: tag.forkNum,
                    block_num: tag.blockNum,
                });
                num_to_scan += 1;
            }

            self.unlock_buf_hdr(buf_id, buf_state);

            // Check for barrier events in case NBuffers is large.
            backend_tcop_postgres_seams::check_for_interrupts::call()?;
        }

        if num_to_scan == 0 {
            return Ok(()); // nothing to do
        }

        let mut wb_context = WritebackContext::default();
        writeback_context_init(&mut wb_context, sb::checkpoint_flush_after::call());

        // Sort buffers that need to be written to reduce random IO + to balance
        // writes between tablespaces (bufmgr.c:3413).
        ckpt_buffer_ids.sort_by(ckpt_buforder_comparator);

        // Allocate progress status for each tablespace with buffers to flush
        // (bufmgr.c:3424). This requires the to-be-flushed array to be sorted.
        let mut per_ts_stat: Vec<CkptTsStatus> = Vec::new();
        let mut num_spaces: usize = 0;
        let mut last_tsid: Option<Oid> = None;

        for (i, item) in ckpt_buffer_ids.iter().enumerate().take(num_to_scan as usize) {
            let cur_tsid = item.ts_id;

            // Grow array of per-tablespace status structs, every time a new
            // tablespace is found (bufmgr.c:3437).
            if last_tsid.is_none() || last_tsid != Some(cur_tsid) {
                num_spaces += 1;
                per_ts_stat.push(CkptTsStatus {
                    ts_id: cur_tsid,
                    progress: 0.0,
                    progress_slice: 0.0,
                    num_to_scan: 0,
                    num_scanned: 0,
                    index: i,
                });
                last_tsid = Some(cur_tsid);
            }

            per_ts_stat[num_spaces - 1].num_to_scan += 1;

            // Check for barrier events.
            backend_tcop_postgres_seams::check_for_interrupts::call()?;
        }

        debug_assert!(num_spaces > 0);

        // Build a min-heap over the write-progress in the individual
        // tablespaces, and compute how large a portion of the total progress a
        // single processed buffer is (bufmgr.c:3461). We faithfully reproduce
        // the SAME balancing: at each step pick the tablespace with the least
        // `progress`, process its current buffer, advance its progress, and drop
        // it once all its buffers are scanned (the C `binaryheap_first`).
        for ts_stat in per_ts_stat.iter_mut() {
            ts_stat.progress_slice = num_to_scan as f64 / ts_stat.num_to_scan as f64;
        }

        // Iterate through to-be-checkpointed buffers and write the ones (still)
        // marked with BM_CHECKPOINT_NEEDED. Writes are balanced between
        // tablespaces (bufmgr.c:3484).
        let mut num_processed: i32 = 0;
        let mut num_written: i32 = 0;
        let mut remaining = num_spaces; // tablespaces with buffers still to scan

        while remaining > 0 {
            // binaryheap_first: the tablespace with the minimum `progress`. The
            // C comparator returns 0 on equal progress, so any of the ties is
            // admissible; pick the first by index for a deterministic stable min.
            let mut sel: Option<usize> = None;
            for (k, ts) in per_ts_stat.iter().enumerate() {
                if ts.num_scanned == ts.num_to_scan {
                    continue; // already fully processed (removed from heap)
                }
                match sel {
                    None => sel = Some(k),
                    Some(b) => {
                        if ts.progress < per_ts_stat[b].progress {
                            sel = Some(k);
                        }
                    }
                }
            }
            let k = match sel {
                Some(k) => k,
                None => break,
            };

            let buf_id = ckpt_buffer_ids[per_ts_stat[k].index].buf_id;
            debug_assert!(buf_id != -1);

            num_processed += 1;

            // We don't need the lock here, because we're only looking at a
            // single bit. SyncOneBuffer will do nothing if the flag is cleared
            // (bufmgr.c:3509).
            if self.read_state(buf_id as usize) & BM_CHECKPOINT_NEEDED != 0 {
                if self.sync_one_buffer(buf_id, false, &mut wb_context)? & BUF_WRITTEN != 0 {
                    sb::count_checkpoint_buffer_written::call();
                    num_written += 1;
                }
            }

            // Measure progress independent of actually having to flush the
            // buffer - otherwise writing become unbalanced (bufmgr.c:3525).
            {
                let s = &mut per_ts_stat[k];
                s.progress += s.progress_slice;
                s.num_scanned += 1;
                s.index += 1;

                // Have all the buffers from the tablespace been processed?
                if s.num_scanned == s.num_to_scan {
                    remaining -= 1;
                }
            }

            // Sleep to throttle our I/O rate (bufmgr.c:3536). (This will check
            // for barrier events even if it doesn't sleep.)
            sb::checkpoint_write_delay::call(flags, num_processed as f64 / num_to_scan as f64)?;
        }

        // Issue all pending flushes. Only checkpointer calls BufferSync(), so
        // IOContext will always be IOCONTEXT_NORMAL (bufmgr.c:3543).
        self.issue_pending_writebacks(&mut wb_context)?;

        // CheckpointStats.ckpt_bufs_written += num_written (accounted per written
        // buffer via the checkpoint-write tally seam above).
        let _ = num_written;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // BgBufferSync (bufmgr.c:3620)
    // -----------------------------------------------------------------------

    /// `BgBufferSync(wb_context)` (bufmgr.c:3620) — write out some dirty buffers
    /// in the pool. Called periodically by the background writer. Returns true if
    /// it's appropriate for the bgwriter process to hibernate.
    ///
    /// The cross-call state (the C function-static vars) is held in
    /// [`BgBufferSyncState`], owned + threaded back in by the bgwriter loop.
    pub fn BgBufferSync(
        &self,
        wb_context: &mut WritebackContext,
        st: &mut BgBufferSyncState,
    ) -> PgResult<bool> {
        let nbuffers = self.nbuffers() as i32;

        // Potentially these could be tunables, but for now, not (bufmgr.c:3650).
        let smoothing_samples: f32 = 16.0;
        let scan_whole_pool_milliseconds: f32 = 120000.0;

        // Find out where the freelist clock sweep currently is, and how many
        // buffer allocations have happened since our last call (bufmgr.c:3678).
        let (strategy_buf_id, strategy_passes, recent_alloc) =
            self.strategy_control().sync_start()?;
        let strategy_buf_id = strategy_buf_id;
        let recent_alloc = recent_alloc as i32;

        // Report buffer alloc counts to pgstat (bufmgr.c:3681).
        sb::report_bgwriter_buf_alloc::call(recent_alloc);

        // If we're not running the LRU scan, just stop after the stats stuff.
        let bgwriter_lru_maxpages = sb::bgwriter_lru_maxpages::call();
        if bgwriter_lru_maxpages <= 0 {
            st.saved_info_valid = false;
            return Ok(true);
        }

        // Compute strategy_delta = how many buffers have been scanned by the
        // clock sweep since last time (bufmgr.c:3697).
        let strategy_delta: i64;
        let bufs_to_lap: i32;

        if st.saved_info_valid {
            let passes_delta: i32 = strategy_passes.wrapping_sub(st.prev_strategy_passes) as i32;

            let mut sd: i64 = (strategy_buf_id - st.prev_strategy_buf_id) as i64;
            sd += passes_delta as i64 * nbuffers as i64;
            strategy_delta = sd;

            debug_assert!(strategy_delta >= 0);

            if (st.next_passes.wrapping_sub(strategy_passes)) as i32 > 0 {
                // we're one pass ahead of the strategy point
                bufs_to_lap = strategy_buf_id - st.next_to_clean;
            } else if st.next_passes == strategy_passes && st.next_to_clean >= strategy_buf_id {
                // on same pass, but ahead or at least not behind
                bufs_to_lap = nbuffers - (st.next_to_clean - strategy_buf_id);
            } else {
                // We're behind, so skip forward to the strategy point and start
                // cleaning from there.
                st.next_to_clean = strategy_buf_id;
                st.next_passes = strategy_passes;
                bufs_to_lap = nbuffers;
            }
        } else {
            // Initializing at startup or after LRU scanning had been off. Always
            // start at the strategy point (bufmgr.c:3733).
            strategy_delta = 0;
            st.next_to_clean = strategy_buf_id;
            st.next_passes = strategy_passes;
            bufs_to_lap = nbuffers;
        }

        // Update saved info for next time.
        st.prev_strategy_buf_id = strategy_buf_id;
        st.prev_strategy_passes = strategy_passes;
        st.saved_info_valid = true;

        // Compute how many buffers had to be scanned for each new allocation,
        // ie, 1/density of reusable buffers, and track a moving average of that.
        if strategy_delta > 0 && recent_alloc > 0 {
            let scans_per_alloc = strategy_delta as f32 / recent_alloc as f32;
            st.smoothed_density += (scans_per_alloc - st.smoothed_density) / smoothing_samples;
        }

        // Estimate how many reusable buffers there are between the current
        // strategy point and where we've scanned ahead to (bufmgr.c:3760).
        let bufs_ahead = nbuffers - bufs_to_lap;
        let reusable_buffers_est: i32 = (bufs_ahead as f32 / st.smoothed_density) as i32;

        // Track a moving average of recent buffer allocations. Fast-attack,
        // slow-decline.
        if st.smoothed_alloc <= recent_alloc as f32 {
            st.smoothed_alloc = recent_alloc as f32;
        } else {
            st.smoothed_alloc += (recent_alloc as f32 - st.smoothed_alloc) / smoothing_samples;
        }

        // Scale the estimate by a GUC to allow more aggressive tuning.
        let mut upcoming_alloc_est: i32 =
            (st.smoothed_alloc as f64 * sb::bgwriter_lru_multiplier::call()) as i32;

        // If recent_alloc remains at zero for many cycles, smoothed_alloc will
        // eventually underflow to zero; reset it once upcoming_alloc_est hits 0.
        if upcoming_alloc_est == 0 {
            st.smoothed_alloc = 0.0;
        }

        // Even with little or no buffer allocation activity, make a small amount
        // of progress through the buffer cache (bufmgr.c:3792).
        let bgwriter_delay = sb::bgwriter_delay::call();
        let min_scan_buffers: i32 =
            (nbuffers as f32 / (scan_whole_pool_milliseconds / bgwriter_delay as f32)) as i32;

        if upcoming_alloc_est < min_scan_buffers + reusable_buffers_est {
            upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
        }

        // Now write out dirty reusable buffers, working forward from the
        // next_to_clean point (bufmgr.c:3812).
        let mut num_to_scan = bufs_to_lap;
        let mut num_written: i32 = 0;
        let mut reusable_buffers = reusable_buffers_est;

        // Execute the LRU scan.
        while num_to_scan > 0 && reusable_buffers < upcoming_alloc_est {
            let sync_state = self.sync_one_buffer(st.next_to_clean, true, wb_context)?;

            st.next_to_clean += 1;
            if st.next_to_clean >= nbuffers {
                st.next_to_clean = 0;
                st.next_passes = st.next_passes.wrapping_add(1);
            }
            num_to_scan -= 1;

            if sync_state & BUF_WRITTEN != 0 {
                reusable_buffers += 1;
                num_written += 1;
                if num_written >= bgwriter_lru_maxpages {
                    sb::count_bgwriter_maxwritten_clean::call();
                    break;
                }
            } else if sync_state & BUF_REUSABLE != 0 {
                reusable_buffers += 1;
            }
        }

        for _ in 0..num_written {
            sb::count_bgwriter_buffer_written_clean::call();
        }

        // Consider the above scan as being like a new allocation scan. Update the
        // smoothed density based on it (bufmgr.c:3849).
        let new_strategy_delta: i64 = (bufs_to_lap - num_to_scan) as i64;
        let new_recent_alloc: i32 = reusable_buffers - reusable_buffers_est;
        if new_strategy_delta > 0 && new_recent_alloc > 0 {
            let scans_per_alloc = new_strategy_delta as f32 / new_recent_alloc as f32;
            st.smoothed_density += (scans_per_alloc - st.smoothed_density) / smoothing_samples;
        }

        // Return true if OK to hibernate.
        Ok(bufs_to_lap == 0 && recent_alloc == 0)
    }

    // -----------------------------------------------------------------------
    // CheckPointBuffers (bufmgr.c:4210)
    // -----------------------------------------------------------------------

    /// `CheckPointBuffers(flags)` (bufmgr.c:4210) — flush all dirty blocks in
    /// the buffer pool to disk at checkpoint time. Temporary relations do not
    /// participate in checkpoints, so they don't need to be flushed.
    pub fn CheckPointBuffers(&self, flags: i32) -> PgResult<()> {
        self.BufferSync(flags)
    }

    // -----------------------------------------------------------------------
    // FlushRelationBuffers (bufmgr.c:4936)
    // -----------------------------------------------------------------------

    /// `FlushRelationBuffers(rel)` (bufmgr.c:4936) — flush every dirty buffer of
    /// one relation. The shared-buffer path is ported here; the local-buffer arm
    /// (`RelationUsesLocalBuffers`) belongs to localbuf and is out of this shared
    /// core (callers route temp relations through the local-buffer flush).
    pub fn FlushRelationBuffers(&self, rd_locator: &RelFileLocator) -> PgResult<()> {
        let nbuffers = self.nbuffers() as usize;

        for i in 0..nbuffers {
            // As in DropRelationBuffers, an unlocked precheck should be safe and
            // saves some cycles (bufmgr.c:4977).
            let tag = self.desc_tag(i);
            if !tag_matches_rlocator(&tag, rd_locator) {
                continue;
            }

            // Make sure we can handle the pin (bufmgr.c:4985).
            self.private_refcount().ReservePrivateRefCountEntry();
            sb::resowner_enlarge::call()?;

            let buf_state = self.lock_buf_hdr(i);
            let tag = self.desc_tag(i);
            if tag_matches_rlocator(&tag, rd_locator)
                && buf_state & (BM_VALID | BM_DIRTY) == BM_VALID | BM_DIRTY
            {
                self.pin_buffer_locked(i, buf_state);
                let lock = self.content_lock(i);
                lwlock::LWLockAcquire(
                    lock,
                    LWLockMode::LW_SHARED,
                    backend_storage_lmgr_proc_seams::my_proc_number::call(),
                )?;
                let flush = self.flush_buffer(i);
                lwlock::LWLockRelease(lock)?;
                flush?;
                // UnpinBuffer(bufHdr) — with-owner (bufmgr.c:5009).
                self.unpin_buffer(i);
            } else {
                self.unlock_buf_hdr(i, buf_state);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // FlushRelationsAllBuffers (bufmgr.c:5026)
    // -----------------------------------------------------------------------

    /// `FlushRelationsAllBuffers(smgrs, nrels)` (bufmgr.c:5026) — flush out of
    /// the buffer pool all pages of all forks of the specified relations. The
    /// relations are assumed not to use local buffers.
    pub fn FlushRelationsAllBuffers(&self, rlocators: &[RelFileLocator]) -> PgResult<()> {
        let nrels = rlocators.len();
        if nrels == 0 {
            return Ok(());
        }

        // fill-in array for qsort (bufmgr.c:5044).
        let mut srels: Vec<RelFileLocator> = Vec::new();
        srels
            .try_reserve(nrels)
            .map_err(|_| PgError::error("FlushRelationsAllBuffers: out of memory"))?;
        srels.extend_from_slice(rlocators);

        // Save the bsearch overhead for low number of relations to sync.
        let use_bsearch = nrels > RELS_BSEARCH_THRESHOLD;

        // sort the list of relations if necessary.
        if use_bsearch {
            srels.sort_by(rlocator_comparator);
        }

        let nbuffers = self.nbuffers() as usize;
        for i in 0..nbuffers {
            let tag = self.desc_tag(i);

            // As in DropRelationBuffers, an unlocked precheck should be safe and
            // saves some cycles (bufmgr.c:5071).
            let srelent: Option<RelFileLocator> = if !use_bsearch {
                let mut found = None;
                for r in srels.iter() {
                    if tag_matches_rlocator(&tag, r) {
                        found = Some(*r);
                        break;
                    }
                }
                found
            } else {
                let rlocator = tag_to_rlocator(&tag);
                match srels.binary_search_by(|probe| rlocator_comparator(probe, &rlocator)) {
                    Ok(idx) => Some(srels[idx]),
                    Err(_) => None,
                }
            };

            // buffer doesn't belong to any of the given relfilelocators; skip it.
            let srelent = match srelent {
                Some(r) => r,
                None => continue,
            };

            // Make sure we can handle the pin (bufmgr.c:5093).
            self.private_refcount().ReservePrivateRefCountEntry();
            sb::resowner_enlarge::call()?;

            let buf_state = self.lock_buf_hdr(i);
            let tag = self.desc_tag(i);
            if tag_matches_rlocator(&tag, &srelent)
                && buf_state & (BM_VALID | BM_DIRTY) == BM_VALID | BM_DIRTY
            {
                self.pin_buffer_locked(i, buf_state);
                let lock = self.content_lock(i);
                lwlock::LWLockAcquire(
                    lock,
                    LWLockMode::LW_SHARED,
                    backend_storage_lmgr_proc_seams::my_proc_number::call(),
                )?;
                let flush = self.flush_buffer(i);
                lwlock::LWLockRelease(lock)?;
                flush?;
                // UnpinBuffer(bufHdr) — with-owner (bufmgr.c:5106).
                self.unpin_buffer(i);
            } else {
                self.unlock_buf_hdr(i, buf_state);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // FlushDatabaseBuffers (bufmgr.c:5304)
    // -----------------------------------------------------------------------

    /// `FlushDatabaseBuffers(dbid)` (bufmgr.c:5304) — flush every dirty buffer of
    /// one database.
    pub fn FlushDatabaseBuffers(&self, dbid: Oid) -> PgResult<()> {
        let nbuffers = self.nbuffers() as usize;

        for i in 0..nbuffers {
            let tag = self.desc_tag(i);

            // As in DropRelationBuffers, an unlocked precheck should be safe and
            // saves some cycles (bufmgr.c:5324).
            if tag.dbOid != dbid {
                continue;
            }

            // Make sure we can handle the pin (bufmgr.c:5331).
            self.private_refcount().ReservePrivateRefCountEntry();
            sb::resowner_enlarge::call()?;

            let buf_state = self.lock_buf_hdr(i);
            let tag = self.desc_tag(i);
            if tag.dbOid == dbid && buf_state & (BM_VALID | BM_DIRTY) == BM_VALID | BM_DIRTY {
                self.pin_buffer_locked(i, buf_state);
                let lock = self.content_lock(i);
                lwlock::LWLockAcquire(
                    lock,
                    LWLockMode::LW_SHARED,
                    backend_storage_lmgr_proc_seams::my_proc_number::call(),
                )?;
                let flush = self.flush_buffer(i);
                lwlock::LWLockRelease(lock)?;
                flush?;
                // UnpinBuffer(bufHdr) — with-owner (bufmgr.c:5334).
                self.unpin_buffer(i);
            } else {
                self.unlock_buf_hdr(i, buf_state);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Writeback accumulator (bufmgr.c:6420 / 6469)
    // -----------------------------------------------------------------------

    /// `ScheduleBufferTagForWriteback(wb_context, io_context, tag)`
    /// (bufmgr.c:6420) — add a buffer to the pending-writeback list, flushing the
    /// list to the kernel (`IssuePendingWritebacks`) once it reaches
    /// `max_pending`. A `max_pending` of 0 disables writeback control.
    fn schedule_buffer_tag_for_writeback(
        &self,
        wb_context: &mut WritebackContext,
        tag: &buftag,
    ) -> PgResult<()> {
        if wb_context.max_pending == 0 {
            return Ok(());
        }

        // Add buffer to the pending writeback array (bufmgr.c:6431).
        if (wb_context.nr_pending as usize) < wb_context.pending_writebacks.len() {
            wb_context.pending_writebacks[wb_context.nr_pending as usize] =
                PendingWriteback { tag: *tag };
        } else {
            wb_context.pending_writebacks.push(PendingWriteback { tag: *tag });
        }
        wb_context.nr_pending += 1;

        // If the pending writebacks exceeds threshold, issue them all
        // (bufmgr.c:6440).
        if wb_context.nr_pending >= wb_context.max_pending {
            self.issue_pending_writebacks(wb_context)?;
        }
        Ok(())
    }

    /// `IssuePendingWritebacks(wb_context, io_context)` (bufmgr.c:6469) — issue
    /// all pending kernel-writeback hints, sorting them into physical order and
    /// coalescing runs of consecutive blocks into one `smgrwriteback` call.
    fn issue_pending_writebacks(&self, wb_context: &mut WritebackContext) -> PgResult<()> {
        let n = wb_context.nr_pending as usize;
        if n == 0 {
            return Ok(());
        }

        // Sort pending writebacks by physical location (bufmgr.c:6492).
        wb_context.pending_writebacks[..n].sort_by(|a, b| buffertag_comparator(&a.tag, &b.tag));

        // Coalesce neighbouring writes, but nothing else. For that we iterate
        // through the, now sorted, array of pending flushes, and look forward to
        // find all neighbouring (or identical) writes (bufmgr.c:6498).
        let mut i = 0usize;
        while i < n {
            // `cur` is the last-merged tag (advances to `next` on each merge,
            // exactly like C's `cur = next`). `start` is the first block of the
            // run, which the final `smgrwriteback` is keyed on.
            let start = wb_context.pending_writebacks[i].tag;
            let currlocator = tag_to_rlocator(&start);
            let mut cur = start;
            let mut nblocks: u32 = 1;

            // Peek ahead into the following writeback requests to see if they
            // can be combined with the current one (bufmgr.c:6505).
            let mut ahead = 0usize;
            while i + ahead + 1 < n {
                let next = wb_context.pending_writebacks[i + ahead + 1].tag;

                // different file, stop (bufmgr.c:6512).
                if tag_to_rlocator(&next) != currlocator || cur.forkNum != next.forkNum {
                    break;
                }

                // ok, block queued twice, skip (bufmgr.c:6517).
                if cur.blockNum == next.blockNum {
                    ahead += 1;
                    continue;
                }

                // only merge consecutive writes (bufmgr.c:6521).
                if cur.blockNum + 1 != next.blockNum {
                    break;
                }

                nblocks += 1;
                cur = next;
                ahead += 1;
            }

            i += ahead;

            // and finally tell the kernel to write the data to storage
            // (bufmgr.c:6531).
            let backend = RelFileLocatorBackend {
                locator: currlocator,
                backend: types_core::primitive::INVALID_PROC_NUMBER,
            };
            // Same `smgropen` discipline as `flush_buffer`: the writeback may run
            // in a process (bgwriter/checkpointer) whose thread-local smgr cache
            // has no entry for this relation. C's `IssuePendingWritebacks` goes
            // through `smgropen`; mirror that (idempotent).
            smgr::smgropen(backend.locator, backend.backend)?;
            smgr::smgrwriteback(backend, start.forkNum, start.blockNum, nblocks)?;

            i += 1;
        }

        wb_context.nr_pending = 0;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WritebackContextInit (bufmgr.c:6394)
// ---------------------------------------------------------------------------

/// `WritebackContextInit(context, max_pending)` (bufmgr.c:6394) — initialise a
/// writeback context, discarding potential previous state. A value of 0 means no
/// writeback control will be performed.
pub fn writeback_context_init(context: &mut WritebackContext, max_pending: i32) {
    debug_assert!(max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

    context.max_pending = max_pending;
    context.nr_pending = 0;
    context.pending_writebacks.clear();
}

// ---------------------------------------------------------------------------
// Comparators (bufmgr.c:6306 / 6340)
// ---------------------------------------------------------------------------

/// `buffertag_comparator(ba, bb)` (bufmgr.c:6306) — order buffer tags by
/// relfilelocator (`rlocator_comparator`), then fork, then block number.
pub fn buffertag_comparator(ba: &buftag, bb: &buftag) -> CmpOrdering {
    let rlocatora = tag_to_rlocator(ba);
    let rlocatorb = tag_to_rlocator(bb);

    let ret = rlocator_comparator(&rlocatora, &rlocatorb);
    if ret != CmpOrdering::Equal {
        return ret;
    }

    if ba.forkNum < bb.forkNum {
        return CmpOrdering::Less;
    }
    if ba.forkNum > bb.forkNum {
        return CmpOrdering::Greater;
    }

    if ba.blockNum < bb.blockNum {
        return CmpOrdering::Less;
    }
    if ba.blockNum > bb.blockNum {
        return CmpOrdering::Greater;
    }

    CmpOrdering::Equal
}

/// `ckpt_buforder_comparator(a, b)` (bufmgr.c:6340) — the checkpoint writeout
/// order: tablespace, then relation, then fork, then block number. Tablespaces
/// MUST be compared first (the write-balancing logic relies on it).
pub fn ckpt_buforder_comparator(a: &CkptSortItem, b: &CkptSortItem) -> CmpOrdering {
    // compare tablespace
    if a.ts_id < b.ts_id {
        CmpOrdering::Less
    } else if a.ts_id > b.ts_id {
        CmpOrdering::Greater
    // compare relation
    } else if a.rel_number < b.rel_number {
        CmpOrdering::Less
    } else if a.rel_number > b.rel_number {
        CmpOrdering::Greater
    // compare fork
    } else if a.fork_num < b.fork_num {
        CmpOrdering::Less
    } else if a.fork_num > b.fork_num {
        CmpOrdering::Greater
    // compare block number
    } else if a.block_num < b.block_num {
        CmpOrdering::Less
    } else if a.block_num > b.block_num {
        CmpOrdering::Greater
    } else {
        // equal page IDs are unlikely, but not impossible
        CmpOrdering::Equal
    }
}

// ---------------------------------------------------------------------------
// Per-tablespace checkpoint write-balancing progress (CkptTsStatus, bufmgr.c)
// ---------------------------------------------------------------------------

/// One per-tablespace progress slot (`CkptTsStatus`, bufmgr.c) used by
/// [`BufferManager::BufferSync`] to balance checkpoint writes across
/// tablespaces.
struct CkptTsStatus {
    /// `tsId` — the tablespace OID this slot tracks. Stored for parity with C's
    /// `CkptTsStatus.tsId`; the write-balancing heap keys on `progress`.
    #[allow(dead_code)]
    ts_id: Oid,
    /// `progress` — accumulated progress (== num_scanned * progress_slice).
    progress: f64,
    /// `progress_slice` — how much one processed buffer advances `progress`.
    progress_slice: f64,
    /// `num_to_scan` — number of buffers of this tablespace to write.
    num_to_scan: i32,
    /// `num_scanned` — how many of this tablespace's buffers we've processed.
    num_scanned: i32,
    /// `index` — running index into the (sorted) `CkptBufferIds` array.
    index: usize,
}

/// The cross-call state of [`BufferManager::BgBufferSync`] — the C
/// function-static variables `saved_info_valid`, `prev_strategy_*`,
/// `next_to_clean`, `next_passes`, `smoothed_alloc`, `smoothed_density`. The
/// bgwriter main loop owns one of these and threads it back in on every call
/// (replacing C's process-local statics).
#[derive(Clone, Copy, Debug)]
pub struct BgBufferSyncState {
    /// `static bool saved_info_valid`.
    pub saved_info_valid: bool,
    /// `static int prev_strategy_buf_id`.
    pub prev_strategy_buf_id: i32,
    /// `static uint32 prev_strategy_passes`.
    pub prev_strategy_passes: u32,
    /// `static int next_to_clean`.
    pub next_to_clean: i32,
    /// `static uint32 next_passes`.
    pub next_passes: u32,
    /// `static float smoothed_alloc`.
    pub smoothed_alloc: f32,
    /// `static float smoothed_density`.
    pub smoothed_density: f32,
}

impl Default for BgBufferSyncState {
    /// The C static initialisers: `saved_info_valid = false`,
    /// `smoothed_alloc = 0`, `smoothed_density = 10.0`; the rest are 0.
    fn default() -> Self {
        Self {
            saved_info_valid: false,
            prev_strategy_buf_id: 0,
            prev_strategy_passes: 0,
            next_to_clean: 0,
            next_passes: 0,
            smoothed_alloc: 0.0,
            smoothed_density: 10.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::primitive::ForkNumber;

    fn tag(spc: u32, db: u32, rel: u32, fork: ForkNumber, blk: u32) -> buftag {
        buftag {
            spcOid: spc,
            dbOid: db,
            relNumber: rel,
            forkNum: fork,
            blockNum: blk,
        }
    }

    fn item(ts: u32, rel: u32, fork: ForkNumber, blk: u32, id: i32) -> CkptSortItem {
        CkptSortItem {
            ts_id: ts,
            rel_number: rel,
            fork_num: fork,
            block_num: blk,
            buf_id: id,
        }
    }

    #[test]
    fn ckpt_buforder_orders_by_ts_then_rel_then_fork_then_block() {
        let main = ForkNumber::MAIN_FORKNUM;
        let a = item(1, 1, main, 0, 0);
        let b = item(1, 1, main, 1, 1);
        let c = item(1, 2, main, 0, 2);
        let d = item(2, 1, main, 0, 3);

        assert_eq!(ckpt_buforder_comparator(&a, &b), CmpOrdering::Less);
        assert_eq!(ckpt_buforder_comparator(&b, &a), CmpOrdering::Greater);
        assert_eq!(ckpt_buforder_comparator(&a, &c), CmpOrdering::Less); // rel
        assert_eq!(ckpt_buforder_comparator(&c, &d), CmpOrdering::Less); // ts dominates rel
        assert_eq!(ckpt_buforder_comparator(&a, &a), CmpOrdering::Equal);
    }

    #[test]
    fn ckpt_sort_is_tablespace_major() {
        let main = ForkNumber::MAIN_FORKNUM;
        let mut v = vec![
            item(2, 1, main, 0, 0),
            item(1, 5, main, 9, 1),
            item(1, 1, main, 0, 2),
            item(2, 1, main, 0, 3),
        ];
        v.sort_by(ckpt_buforder_comparator);
        assert_eq!(v[0].ts_id, 1);
        assert_eq!(v[1].ts_id, 1);
        assert_eq!(v[2].ts_id, 2);
        assert_eq!(v[3].ts_id, 2);
        assert_eq!(v[0].rel_number, 1);
        assert_eq!(v[1].rel_number, 5);
    }

    #[test]
    fn buffertag_comparator_orders_by_rlocator_then_fork_then_block() {
        let main = ForkNumber::MAIN_FORKNUM;
        let fsm = ForkNumber::FSM_FORKNUM;
        let a = tag(1, 1, 100, main, 0);
        let b = tag(1, 1, 100, main, 1);
        let c = tag(1, 1, 100, fsm, 0);
        let d = tag(1, 1, 200, main, 0);

        assert_eq!(buffertag_comparator(&a, &b), CmpOrdering::Less); // block
        assert_eq!(buffertag_comparator(&a, &c), CmpOrdering::Less); // fork main<fsm
        assert_eq!(buffertag_comparator(&a, &d), CmpOrdering::Less); // rel 100<200
        assert_eq!(buffertag_comparator(&a, &a), CmpOrdering::Equal);
        assert_eq!(buffertag_comparator(&b, &a), CmpOrdering::Greater);
    }

    #[test]
    fn writeback_context_init_resets_state() {
        let mut ctx = WritebackContext::default();
        ctx.nr_pending = 7;
        ctx.max_pending = 3;
        ctx.pending_writebacks.push(PendingWriteback::default());
        writeback_context_init(&mut ctx, 128);
        assert_eq!(ctx.max_pending, 128);
        assert_eq!(ctx.nr_pending, 0);
        assert!(ctx.pending_writebacks.is_empty());
    }

    #[test]
    fn bg_buffer_sync_state_default_matches_c_static_init() {
        let st = BgBufferSyncState::default();
        assert!(!st.saved_info_valid);
        assert_eq!(st.smoothed_alloc, 0.0);
        assert_eq!(st.smoothed_density, 10.0);
        assert_eq!(st.next_to_clean, 0);
        assert_eq!(st.next_passes, 0);
    }

    #[test]
    fn rlocator_comparator_orders_by_rel_then_db_then_spc() {
        let a = RelFileLocator {
            spcOid: 1,
            dbOid: 1,
            relNumber: 100,
        };
        let b = RelFileLocator {
            spcOid: 1,
            dbOid: 1,
            relNumber: 200,
        };
        let c = RelFileLocator {
            spcOid: 1,
            dbOid: 2,
            relNumber: 100,
        };
        assert_eq!(rlocator_comparator(&a, &b), CmpOrdering::Less); // rel
        assert_eq!(rlocator_comparator(&a, &c), CmpOrdering::Less); // db
        assert_eq!(rlocator_comparator(&a, &a), CmpOrdering::Equal);
    }
}
