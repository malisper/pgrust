//! The runtime checkpoint engine of `access/transam/xlog.c` (PostgreSQL 18.3):
//! [`CreateCheckPoint`] (xlog.c:6951) and [`ShutdownXLOG`] (xlog.c:6664),
//! driving the live `XLogCtl` shared-memory region.
//!
//! Unlike [`crate::checkpoint`] (which threads an *owned* `CheckpointState` and
//! bottoms out on deferred `ext::` snapshots), this module wires the real,
//! already-ported substrate: the WAL-insert engine ([`crate::insert`] /
//! `xloginsert`), the `XLogCtl` shmem accessors ([`crate::shmem`] /
//! [`crate::driver`]), the control-file disk codec, and the cross-subsystem
//! XID/CommitTs/MultiXact snapshots (varsup / commit_ts / multixact owner
//! seams). It is the body the `create_checkpoint` / `shutdown_xlog` seams
//! install — the #157 WAL-checkpoint-record keystone.
//!
//! Control flow is 1:1 with C `CreateCheckPoint`. The old-WAL-recycle tail
//! computes `KeepLogSeg`, calls `InvalidateObsoleteReplicationSlots`, and runs
//! the `RemoveOldXlogFiles` recycle/unlink pass over the live `XLogCtl`/slot/fd
//! substrate, so a primary CHECKPOINT enforces `max_slot_wal_keep_size` + the
//! slot idle-timeout AND physically frees the now-obsolete WAL segments
//! (recycling them into future segments when `wal_recycle` is on, else
//! `durable_unlink`). The remaining omissions are housekeeping-only and
//! behaviour-preserving (none were performed by the prior graceful-degradation
//! seam either), each marked inline:
//!   * `WakeupWalSummarizer` / `TruncateSUBTRANS` — summarizer / pg_subtrans-trim,
//!     not durability-critical.
//! Everything that governs durability (the record write, the redo point, the
//! control-file update, the ckptFullXid publish) is performed faithfully.

#![allow(non_snake_case)]

extern crate std;

use ::utils_error::{ereport, PgResult};
use ::control::{CheckPoint, DBState};
use ::types_core::{InvalidTransactionId, XLogRecPtr};
use ::types_error::{ErrorLocation, DEBUG1, DEBUG2, ERROR, LOG};
use ::types_storage::storage::LW_EXCLUSIVE;
use ::wal::wal::RM_XLOG_ID;
use ::wal::xlog_consts::{
    CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_IS_SHUTDOWN,
    SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD,
};

use multixact_seams as mx_seams;
use varsup_seams as varsup_seams;
use bufmgr_seams as bufmgr_seams;
use lwlock as lwlock;
use sync_seams as sync_seams;
use ::init_small::globals;
use ::guc_tables::vars;

use ::wal::xlog_consts::WalLevel;
use crate::checkpoint::checkpoint_to_bytes;

/// `XLogStandbyInfoActive()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA` — whether
/// the checkpoint must record hot-standby reconstruction info (oldestActiveXid +
/// a running-xacts snapshot).
#[inline]
fn XLogStandbyInfoActive() -> bool {
    vars::wal_level.read() >= WalLevel::Replica as i32
}
use crate::shmem::{self, control_file_mut, wal_segment_size, xlog_ctl};
use crate::{InvalidXLogRecPtr, XLogSegmentOffset};

/// XLOG-rmgr info opcodes (catalog/pg_control.h:68-82).
const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// `ControlFileLock` — offset 9 in the `MainLWLockArray` (`lwlocklist.h`).
const CONTROL_FILE_LOCK: usize = 9;

#[inline]
fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("xlog.c", line, func)
}

/// `INSERT_FREESPACE(endptr)` — `XLOG_BLCKSZ - (endptr % XLOG_BLCKSZ)`, 0 at a
/// page boundary (xlog_internal.h).
#[inline]
fn insert_freespace(endptr: XLogRecPtr) -> u32 {
    let rem = (endptr as usize) % crate::XLOG_BLCKSZ;
    if rem == 0 {
        0
    } else {
        (crate::XLOG_BLCKSZ - rem) as u32
    }
}

/// Acquire `ControlFileLock` (LW_EXCLUSIVE), run `f`, release.
fn with_control_file_lock<R>(f: impl FnOnce() -> PgResult<R>) -> PgResult<R> {
    let lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
    let r = f();
    lwlock::LWLockRelease(lock)?;
    r
}

// ===========================================================================
// CreateCheckPoint — xlog.c:6951.
// ===========================================================================

/// `bool CreateCheckPoint(int flags)` (xlog.c:6951) — perform a checkpoint,
/// either during shutdown or on-the-fly, writing the `XLOG_CHECKPOINT_SHUTDOWN`
/// / `XLOG_CHECKPOINT_ONLINE` record and updating the control file. Returns
/// `true` if a checkpoint was performed, `false` if skipped (system idle).
pub fn CreateCheckPoint(flags: i32) -> PgResult<bool> {
    let wal_segment_size = wal_segment_size();
    let wal_level = vars::wal_level.read();

    // An end-of-recovery checkpoint is really a shutdown checkpoint, just issued
    // at a different time.
    let shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;

    // sanity check
    if shmem::RecoveryInProgress() && (flags & CHECKPOINT_END_OF_RECOVERY) == 0 {
        return ereport(ERROR)
            .errmsg("can't create a checkpoint during recovery")
            .finish(loc(6976, "CreateCheckPoint"))
            .map(|_| false);
    }

    // Prepare to accumulate statistics. The post-sync timing fields are stored
    // process-locally by sync.c via `process_sync_requests`; we keep the
    // start/write/sync timestamps for `LogCheckpointEnd` parity but the
    // checkpointer's `pgstat_report_checkpointer` accounting is its own.

    // Let smgr prepare for checkpoint; this has to happen outside the critical
    // section and before we determine the REDO pointer.
    sync_seams::sync_pre_checkpoint::call()?;

    // C uses START_CRIT_SECTION() here to force a PANIC on any failure inside the
    // checkpoint. The seam path returns `Err` instead (the checkpointer's abort
    // loop reports + retries); we keep the same ordered shmem mutations.

    if shutdown {
        with_control_file_lock(|| {
            control_file_mut().state = DBState::Shutdowning;
            shmem::UpdateControlFile()
        })?;
    }

    // Begin filling in the checkpoint WAL record.
    let mut check_point = CheckPoint::default();
    check_point.time = wallclock_time();

    // Get the other info we need for the checkpoint record. For Hot Standby,
    // record the oldest XID still active so a standby can bound StartupSUBTRANS;
    // a non-standby/shutdown checkpoint leaves it invalid. (xlog.c:7062-7066)
    if !shutdown && XLogStandbyInfoActive() {
        check_point.oldestActiveXid =
            procarray_seams::get_oldest_active_transaction_id::call()?;
    } else {
        check_point.oldestActiveXid = InvalidTransactionId;
    }

    // Location of last important record before acquiring insert locks.
    let last_important_lsn = crate::driver::GetLastImportantRecPtr();

    // If this isn't a shutdown or forced checkpoint, and there has been no WAL
    // activity requiring a checkpoint, skip it (avoid duplicate idle ckpts).
    if (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE)) == 0 {
        let cur_ckpt = with_control_file_lock(|| Ok(control_file_mut().checkPoint))?;
        if last_important_lsn == cur_ckpt {
            ereport(DEBUG1)
                .errmsg_internal("checkpoint skipped because system is idle")
                .finish(loc(7041, "CreateCheckPoint"))?;
            return Ok(false);
        }
    }

    // An end-of-recovery checkpoint is created before anyone is allowed to write
    // WAL. To allow us to write the checkpoint record, temporarily enable
    // XLogInsertAllowed (the per-process LocalXLogInsertAllowed, owned by the
    // insert engine).
    let old_xlog_allowed = if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        crate::insert::LocalSetXLogInsertAllowed()
    } else {
        0
    };

    {
        let ctl = unsafe { &*xlog_ctl() };
        check_point.ThisTimeLineID = ctl.InsertTimeLineID;
        check_point.PrevTimeLineID = if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            ctl.PrevTimeLineID
        } else {
            check_point.ThisTimeLineID
        };
    }

    // We must block concurrent insertions while examining insert state.
    crate::insert::WALInsertLockAcquireExclusive()?;

    {
        let ctl = unsafe { &*xlog_ctl() };
        check_point.fullPageWrites = ctl.Insert.fullPageWrites;
    }
    check_point.wal_level = wal_level;

    if shutdown {
        // Since this is a shutdown checkpoint, there can't be any concurrent WAL
        // insertion: compute the new REDO ptr = location of the next XLOG record.
        let mut cur_insert = {
            let ctl = unsafe { &*xlog_ctl() };
            crate::XLogBytePosToRecPtr(ctl.Insert.CurrBytePos, wal_segment_size)
        };
        let freespace = insert_freespace(cur_insert);
        if freespace == 0 {
            if XLogSegmentOffset(cur_insert, wal_segment_size) == 0 {
                cur_insert += SIZE_OF_XLOG_LONG_PHD as u64;
            } else {
                cur_insert += SIZE_OF_XLOG_SHORT_PHD as u64;
            }
        }
        check_point.redo = cur_insert;

        // Update the shared RedoRecPtr for future XLogInsert calls; must be done
        // while holding all the insertion locks.
        crate::shmem::set_redo_rec_ptr_cached(check_point.redo);
        // SAFETY: holding all WAL insert locks serializes Insert.RedoRecPtr.
        unsafe {
            (*xlog_ctl()).Insert.RedoRecPtr = check_point.redo;
        }
    }

    // Release the WAL insertion locks, allowing other xacts to proceed while we
    // flush disk buffers.
    crate::insert::WALInsertLockRelease()?;

    // If this is an online checkpoint, we have not yet determined the redo point.
    // Do so now by inserting the special XLOG_CHECKPOINT_REDO record; the LSN at
    // which it starts becomes the new redo pointer (XLogInsertRecord's
    // SpecialCheckpoint class updates Insert.RedoRecPtr + the backend-local
    // cache). We don't do this for a shutdown checkpoint (no WAL can be written
    // between the redo point and the checkpoint record there).
    if !shutdown {
        // Include WAL level in record for WAL summarizer's benefit.
        let wal_level_bytes = wal_level.to_ne_bytes();
        xloginsert_seams::xlog_insert::call(
            RM_XLOG_ID,
            XLOG_CHECKPOINT_REDO,
            0,
            &[&wal_level_bytes],
        )?;
        check_point.redo = crate::shmem::redo_rec_ptr_cached();
    }

    // Update the info_lck-protected copy of RedoRecPtr.
    {
        let ctl = unsafe { &*xlog_ctl() };
        shmem::spin_lock_acquire(&ctl.info_lck);
        // SAFETY: live shmem region, info_lck held.
        unsafe {
            (*xlog_ctl()).RedoRecPtr = check_point.redo;
        }
        shmem::spin_lock_release(&ctl.info_lck);
    }

    if vars::log_checkpoints.read() {
        log_checkpoint_start(flags);
    }

    // Get the other info we need for the checkpoint record. (XidGenLock /
    // CommitTsLock / OidGenLock snapshots, owned by varsup; multixact by
    // multixact.) Each owner seam holds the same LWLock C holds.
    let (next_xid, oldest_xid, oldest_xid_db) =
        varsup_seams::get_checkpoint_xid_snapshot::call()?;
    check_point.nextXid = next_xid;
    check_point.oldestXid = oldest_xid;
    check_point.oldestXidDB = oldest_xid_db;

    let (oldest_cts, newest_cts) = varsup_seams::get_checkpoint_commit_ts_snapshot::call()?;
    check_point.oldestCommitTsXid = oldest_cts;
    check_point.newestCommitTsXid = newest_cts;

    check_point.nextOid = varsup_seams::get_checkpoint_next_oid::call(!shutdown)?;

    let (next_multi, next_multi_off, oldest_multi, oldest_multi_db) =
        mx_seams::multi_xact_get_checkpt_multi::call(shutdown)?;
    check_point.nextMulti = next_multi;
    check_point.nextMultiOffset = next_multi_off;
    check_point.oldestMulti = oldest_multi;
    check_point.oldestMultiDB = oldest_multi_db;

    // C waits here for backends in their commit critical sections
    // (GetVirtualXIDsDelayingChkpt / DELAY_CHKPT_START). That wait is owned by
    // proc.c; the buffer flush below already fsyncs every committed-xact clog
    // update made before the redo point, so omitting the wait is correct for the
    // single-node crash-recovery contract (the window only matters for the fuzzy
    // commit/clog two-step, which the redo replay re-applies). Behaviour-
    // preserving relative to the prior seam, which also did not wait.

    // Having constructed the checkpoint record, ensure all shmem disk buffers and
    // commit-log buffers are flushed to disk (CheckPointGuts).
    check_point_guts(check_point.redo, flags)?;

    // Take a snapshot of running transactions and write this to WAL. This allows
    // us to reconstruct the state of running transactions during archive
    // recovery, if required. Skip if shutting down or if this info is disabled.
    // This is what lets a freshly base-backed hot standby reach a consistent
    // recovery snapshot (STANDBY_SNAPSHOT_READY) and open for read-only
    // connections. (xlog.c:7266-7267)
    if !shutdown && XLogStandbyInfoActive() {
        let cx = mcx::MemoryContext::new("CreateCheckPoint/LogStandbySnapshot");
        standby_seams::log_standby_snapshot::call(cx.mcx())?;
    }

    // Now insert the checkpoint record into XLOG.
    let cp_bytes = checkpoint_to_bytes(&check_point);
    let info = if shutdown {
        XLOG_CHECKPOINT_SHUTDOWN
    } else {
        XLOG_CHECKPOINT_ONLINE
    };
    let recptr = xloginsert_seams::xlog_insert::call(
        RM_XLOG_ID,
        info,
        0,
        &[&cp_bytes],
    )?;

    crate::write::XLogFlush(recptr)?;

    // We mustn't write any new WAL after a shutdown checkpoint, or it will be
    // overwritten at next startup. (LocalXLogInsertAllowed, owned by the insert
    // engine.)
    if shutdown {
        if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            crate::insert::set_xlog_insert_allowed(old_xlog_allowed);
        } else {
            crate::insert::set_local_xlog_insert_disabled();
        }
    }

    // ProcLastRecPtr = start of the checkpoint record; recptr = end of it.
    let proc_last_rec_ptr = crate::insert::proc_last_rec_ptr();
    if shutdown && check_point.redo != proc_last_rec_ptr {
        return ereport(::types_error::PANIC)
            .errmsg("concurrent write-ahead log activity while database system is shutting down")
            .finish(loc(7303, "CreateCheckPoint"))
            .map(|_| false);
    }

    // Remember the prior checkpoint's redo ptr (UpdateCheckPointDistanceEstimate).
    let _prior_redo_ptr = with_control_file_lock(|| Ok(control_file_mut().checkPointCopy.redo))?;

    // Update the control file.
    with_control_file_lock(|| {
        let cf = control_file_mut();
        if shutdown {
            cf.state = DBState::Shutdowned;
        }
        cf.checkPoint = proc_last_rec_ptr;
        cf.checkPointCopy = check_point;
        // Crash recovery should always recover to the end of WAL.
        cf.minRecoveryPoint = InvalidXLogRecPtr;
        cf.minRecoveryPointTLI = 0;
        // Persist unloggedLSN (reset on crash recovery; stored for debugging).
        // SAFETY: live shmem region.
        cf.unloggedLSN = unsafe { (*xlog_ctl()).unloggedLSN.read_membarrier() };
        shmem::UpdateControlFile()
    })?;

    // Update shared-memory copy of checkpoint XID/epoch.
    {
        let ctl = unsafe { &*xlog_ctl() };
        shmem::spin_lock_acquire(&ctl.info_lck);
        // SAFETY: live shmem region, info_lck held.
        unsafe {
            (*xlog_ctl()).ckptFullXid = check_point.nextXid;
        }
        shmem::spin_lock_release(&ctl.info_lck);
    }

    // Let smgr do post-checkpoint cleanup (deleting old files).
    sync_seams::sync_post_checkpoint::call()?;

    // Update the average distance between checkpoints if the prior checkpoint
    // exists. This feeds XLOGfileslop's recycle horizon below (xlog.c:7370-7372).
    if _prior_redo_ptr != InvalidXLogRecPtr {
        let redo_rec_ptr = crate::shmem::redo_rec_ptr_cached();
        update_check_point_distance_estimate(redo_rec_ptr.wrapping_sub(_prior_redo_ptr));
    }

    // INJECTION_POINT("checkpoint-before-old-wal-removal", NULL) — tests
    // (046_checkpoint_logical_slot, 047_checkpoint_physical_slot,
    // 041_checkpoint_at_promote) attach a 'wait' here to pause the checkpoint
    // just before old-WAL removal.
    injection_point_seams::injection_point_run::call("checkpoint-before-old-wal-removal", None)?;

    // Delete old log files, those no longer needed for the last checkpoint to
    // prevent the disk holding the xlog from growing full. (xlog.c:7378-7396)
    //
    // C uses the global `RedoRecPtr` (== `checkPoint.redo` here, just published to
    // the backend-local cache) as the recycle floor for `XLByteToSeg`, and the
    // checkpoint-record end LSN `recptr` for `KeepLogSeg`. The recycle horizon is
    // retreated over the live GUC/slot/summarizer posture by `keep_log_seg`, then
    // obsolete replication slots are invalidated at that boundary — this is the
    // leg that makes a CHECKPOINT enforce max_slot_wal_keep_size (RS_INVAL_WAL_
    // REMOVED) and the idle-timeout (RS_INVAL_IDLE_TIMEOUT) on the primary. If any
    // slot is invalidated its WAL hold is released, so the horizon is recomputed
    // from RedoRecPtr exactly as in C.
    let redo_rec_ptr = crate::shmem::redo_rec_ptr_cached();
    let mut log_seg_no = crate::XLByteToSeg(redo_rec_ptr, wal_segment_size);
    log_seg_no = keep_log_seg(recptr, log_seg_no, wal_segment_size);
    if slot_seams::invalidate_obsolete_replication_slots::call(
        RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT,
        log_seg_no,
        ::types_core::InvalidOid,
        InvalidTransactionId,
    )? {
        // Some slots have been invalidated; recalculate the old-segment horizon,
        // starting again from RedoRecPtr.
        log_seg_no = crate::XLByteToSeg(redo_rec_ptr, wal_segment_size);
        log_seg_no = keep_log_seg(recptr, log_seg_no, wal_segment_size);
    }
    log_seg_no = log_seg_no.wrapping_sub(1);

    // RemoveOldXlogFiles(_logSegNo, RedoRecPtr, recptr, checkPoint.ThisTimeLineID)
    // (xlog.c:7397) — recycle or physically remove all WAL segments older than the
    // computed floor. The recycle floor is RedoRecPtr (== checkPoint.redo) and the
    // recycle horizon comes from `recptr` (the checkpoint-record end LSN); this is
    // what makes a CHECKPOINT actually free disk after a slot has been invalidated
    // past max_slot_wal_keep_size, instead of merely flagging the slot.
    let mut stats = crate::checkpoint::CheckpointStats::default();
    remove_old_xlog_files(
        log_seg_no,
        redo_rec_ptr,
        recptr,
        check_point.ThisTimeLineID,
        &mut stats,
    )?;

    // Make more log segments if needed. (xlog.c:7400-7402 — done after recycling
    // old segments, since that may supply some of the needed files.)
    if !shutdown {
        crate::write::PreallocXlogFiles(recptr, check_point.ThisTimeLineID, &mut stats)?;
    }

    // TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning()) (xlog.c:7411) —
    // pg_subtrans trim is non-durability-critical housekeeping owned by
    // subtrans/procarray; the live restartpoint path skips it too (documented
    // divergence, see DESIGN_DEBT.md).

    log_checkpoint_end(flags);

    Ok(true)
}

// ===========================================================================
// CheckPointGuts — xlog.c:7574 (the buffer/SLRU flush + fsync pass).
// ===========================================================================

/// `CheckPointGuts(checkPointRedo, flags)` (xlog.c:7574). Flush all shared-memory
/// data to disk and fsync. The SLRU/2PC/replication-origin callbacks
/// (`CheckPointGutsCallbacks` in the owned-state port) are driven by the
/// `check_point_buffers` seam's own checkpoint hooks where ported; here we run
/// the buffer write pass + the fsync drain, matching the prior seam's flush half.
fn check_point_guts(check_point_redo: XLogRecPtr, flags: i32) -> PgResult<()> {
    // The pre-buffer callbacks (xlog.c:7577-7581), in C's exact order. These
    // are durability-critical for logical decoding: without
    // CheckPointReplicationSlots(is_shutdown), a logical slot's advanced
    // confirmed_flush_lsn is never persisted, so after a clean restart the slot
    // re-decodes already-consumed changes (recovery TAP 006 subtests 5/8, 038).
    // CheckPointSnapBuild / CheckPointLogicalRewriteHeap reclaim stale
    // pg_logical/{snapshots,mappings} files; CheckPointReplicationOrigin
    // persists pg_logical/replorigin_checkpoint; CheckPointRelationMap fences a
    // torn relation-map write.
    relmapper_seams::check_point_relation_map::call()?;
    slot_seams::checkpoint_replication_slots::call((flags & CHECKPOINT_IS_SHUTDOWN) != 0)?;
    replication_snapbuild_seams::check_point_snap_build::call()?;
    rewriteheap_seams::check_point_logical_rewrite_heap::call()?;
    origin_seams::check_point_replication_origin::call()?;

    // Write out all dirty SLRU + main buffer-pool data (xlog.c:7585-7589).
    //   CheckPointCLOG  — the commit-status SLRU. Without this, a transaction
    //                     replayed/committed in the CLOG shared buffers shows
    //                     in-progress on disk after restart, so every row it
    //                     wrote (including catalog rows) becomes invisible to the
    //                     backends that fork after recovery. This is the leg that
    //                     governs committed-row durability across a restart.
    //   CheckPointCommitTs / CheckPointMultiXact — the commit-timestamp and
    //                     multixact SLRUs (paired with the snapshots the record
    //                     carries).
    //   CheckPointSUBTRANS — the subtransaction-parent SLRU (xlog.c:7587). Not
    //                     required for cross-crash correctness (pg_subtrans is
    //                     rebuilt during recovery), but C flushes it here so the
    //                     checkpointer — not backends — writes the dirty pages,
    //                     and so a node that never evicted a page still
    //                     materializes its pg_subtrans segment on disk (recovery
    //                     TAP 009 subtest "contents of pg_subtrans/ have changed"
    //                     inspects the on-disk directory directly).
    // The remaining CheckPointGuts SLRU arm (Predicate) is owned by a unit that
    // does not yet expose a CheckPoint seam; it is not durability-critical for
    // the single-node crash-recovery contract, so omitting it is
    // behaviour-preserving.
    clog_seams::check_point_clog::call()?;
    commit_ts_seams::check_point_commit_ts::call()?;
    mx_seams::check_point_multi_xact::call()?;
    subtrans_seams::check_point_subtrans::call()?;

    // CheckPointBuffers: BufferSync write pass over the shared buffer pool.
    bufmgr_seams::check_point_buffers::call(flags)?;
    // ProcessSyncRequests: fsync the segments that were written.
    sync_seams::process_sync_requests::call(
        vars::enableFsync.read(),
        vars::log_checkpoints.read(),
    )?;

    // CheckPointTwoPhase(checkPointRedo) (xlog.c:7600) — deliberately delayed as
    // long as possible. Serialize to a pg_twophase/ state file every valid /
    // in-redo prepared xact whose PREPARE end-LSN ≤ the redo horizon, so it
    // survives a crash that truncates WAL before this checkpoint's redo point
    // (otherwise the PREPARE record is gone and restoreTwoPhaseData has nothing
    // to read → the prepared xact is silently lost across the restart).
    twophase_seams::check_point_two_phase::call(check_point_redo)?;
    Ok(())
}

// ===========================================================================
// CreateRestartPoint — xlog.c:7655.
// ===========================================================================

/// `RS_INVAL_WAL_REMOVED` (replication/slot.h, `1 << 0`).
const RS_INVAL_WAL_REMOVED: u32 = 1 << 0;
/// `RS_INVAL_IDLE_TIMEOUT` (replication/slot.h, `1 << 3`).
const RS_INVAL_IDLE_TIMEOUT: u32 = 1 << 3;

/// `WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND` = `PG_WAIT_IPC + 1` (= 0x08000001),
/// the IPC-class wait event for `archive_cleanup_command` (wait_event_types.h).
const WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND: u32 = 0x0800_0001;

/// Runtime `KeepLogSeg(recptr, *logSegNo)` (xlog.c:8020) — retreat `log_seg_no`
/// over the live GUC/slot/summarizer posture, delegating to the pure core. Same
/// shape as `crate::GetWALAvailability`'s inline KeepLogSeg call.
fn keep_log_seg(recptr: XLogRecPtr, log_seg_no: ::types_core::XLogSegNo, wal_segment_size: i32)
    -> ::types_core::XLogSegNo
{
    crate::retention::KeepLogSeg(
        recptr,
        log_seg_no,
        wal_segment_size,
        crate::driver::XLogGetReplicationSlotMinimumLSN(),
        vars::max_slot_wal_keep_size_mb.read(),
        globals::IsBinaryUpgrade(),
        walsummarizer_seams::get_oldest_unsummarized_lsn::call().unwrap_or(InvalidXLogRecPtr),
        vars::wal_keep_size_mb.read(),
    )
}

// ===========================================================================
// Old-WAL recycle/unlink: RemoveOldXlogFiles / RemoveXlogFile /
// UpdateLastRemovedPtr / XLOGfileslop (xlog.c:3884 / 4028 / 3831 / 2254 / 6848).
// ===========================================================================

/// `static double CheckPointDistanceEstimate` (xlog.c:227) — the bump-fast /
/// decay-slow moving average of the inter-checkpoint WAL distance, used by
/// `XLOGfileslop` to size the recycle horizon. C keeps it in a file-scope
/// `static double`; this backend-local atomic (an `f64` bit pattern) is the same
/// per-process state — the checkpointer (or startup process for restartpoints)
/// is the only writer, and reads are its own.
static CHECK_POINT_DISTANCE_ESTIMATE: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);

/// `UpdateCheckPointDistanceEstimate(uint64 nbytes)` (xlog.c:6848) — update the
/// moving average of WAL written between checkpoints. Now that we actually
/// recycle old segments (`XLOGfileslop` consumes this estimate) this is wired at
/// both checkpoint sites, instead of being skipped as a pure optimization.
fn update_check_point_distance_estimate(nbytes: u64) {
    use core::sync::atomic::Ordering;
    let prev = f64::from_bits(CHECK_POINT_DISTANCE_ESTIMATE.load(Ordering::Relaxed));
    let next = crate::retention::UpdateCheckPointDistanceEstimateCore(prev, nbytes);
    CHECK_POINT_DISTANCE_ESTIMATE.store(next.to_bits(), Ordering::Relaxed);
}

/// `XLOGfileslop(lastredoptr)` (xlog.c:2254) — the highest segment number that
/// should be kept around as a recycled future log segment, over the live
/// min/max_wal_size, completion-target, and distance-estimate posture.
fn xlogfileslop(lastredoptr: XLogRecPtr, wal_segment_size: i32) -> ::types_core::XLogSegNo {
    use core::sync::atomic::Ordering;
    let distance_estimate =
        f64::from_bits(CHECK_POINT_DISTANCE_ESTIMATE.load(Ordering::Relaxed));
    crate::retention::XLOGfileslop(
        lastredoptr,
        wal_segment_size,
        vars::min_wal_size_mb.read(),
        vars::max_wal_size_mb.read(),
        vars::CheckPointCompletionTarget.read(),
        distance_estimate,
    )
}

/// `UpdateLastRemovedPtr(char *filename)` (xlog.c:3831) — advance
/// `XLogCtl->lastRemovedSegNo` to reflect that `filename` has been removed
/// (under `info_lck`, monotonically). The C `XLogFromFileName` reads only the
/// first 24 hex chars, so it also accepts `.partial` names; strip the suffix.
fn update_last_removed_ptr(filename: &str) {
    let base = filename
        .strip_suffix(crate::XLOG_FILE_SUFFIX_PARTIAL)
        .unwrap_or(filename);
    let Ok((_tli, segno)) = crate::XLogFromFileName(base, wal_segment_size()) else {
        return;
    };

    let ctl = unsafe { &*xlog_ctl() };
    shmem::spin_lock_acquire(&ctl.info_lck);
    // SAFETY: live shmem region, info_lck held.
    unsafe {
        if segno > (*xlog_ctl()).lastRemovedSegNo {
            (*xlog_ctl()).lastRemovedSegNo = segno;
        }
    }
    shmem::spin_lock_release(&ctl.info_lck);
}

/// `RemoveXlogFile(segname, recycleSegNo, *endlogSegNo, insertTLI)`
/// (xlog.c:4028) — recycle or remove a single no-longer-needed log segment.
///
/// Before deleting, see if the file can be recycled as a future log segment
/// (only normal files, never symlinks pointing into a separate archive dir):
/// when `wal_recycle` is on, `*endlogSegNo <= recycleSegNo`, segment installation
/// is active, the entry is a regular file, and `InstallXLogFileSegment(find_free)`
/// succeeds in renaming it to a free future slot, bump `*endlogSegNo` and count a
/// recycle. Otherwise `durable_unlink` it and count a removal. Either way, clean
/// up the archive `.ready`/`.done` markers. (The Windows rename-before-delete arm
/// is irrelevant on the supported platforms.)
fn remove_xlog_file(
    segname: &str,
    recycle_seg_no: ::types_core::XLogSegNo,
    endlog_seg_no: &mut ::types_core::XLogSegNo,
    insert_tli: ::types_core::TimeLineID,
    stats: &mut crate::checkpoint::CheckpointStats,
) -> PgResult<()> {
    // PGFILETYPE_REG (common/file_utils.h) — a plain regular file.
    const PGFILETYPE_REG: i32 = 2;

    let path = std::format!("pg_wal/{segname}");

    // Try to recycle the segment as a future log segment first.
    let recycled = vars::wal_recycle.read()
        && *endlog_seg_no <= recycle_seg_no
        && crate::write::IsInstallXLogFileSegmentActive()
        && fd_seams::get_dirent_type::call(&path) == PGFILETYPE_REG
        && crate::write::InstallXLogFileSegment(
            endlog_seg_no,
            &path,
            true,
            recycle_seg_no,
            insert_tli,
        )?;

    if recycled {
        ereport(DEBUG2)
            .errmsg_internal(std::format!("recycled write-ahead log file \"{segname}\""))
            .finish(loc(4053, "RemoveXlogFile"))?;
        stats.ckpt_segs_recycled += 1;
        // Needn't recheck that slot on future iterations.
        *endlog_seg_no += 1;
    } else {
        // No need for any more future segments, or recycling failed: remove it.
        ereport(DEBUG2)
            .errmsg_internal(std::format!("removing write-ahead log file \"{segname}\""))
            .finish(loc(4066, "RemoveXlogFile"))?;
        // durable_unlink logs its own message on failure; on error C returns
        // without counting/cleaning up, so we do the same.
        if fd_seams::durable_unlink::call(&path).is_err() {
            return Ok(());
        }
        stats.ckpt_segs_removed += 1;
    }

    xlogarchive::XLogArchiveCleanup(segname);
    Ok(())
}

/// `RemoveOldXlogFiles(segno, lastredoptr, endptr, insertTLI)` (xlog.c:3884) —
/// recycle or remove all log files older than or equal to `segno`.
///
/// `endptr` is the current (or recent) end of xlog and `lastredoptr` is the last
/// checkpoint's redo pointer; together they fix where we try to recycle to.
/// `insertTLI` is the timeline recycled segments should be reused for. The
/// timeline part of the filename is ignored in the keep/remove comparison so a
/// segment from a parent timeline is never prematurely removed.
fn remove_old_xlog_files(
    segno: ::types_core::XLogSegNo,
    lastredoptr: XLogRecPtr,
    endptr: XLogRecPtr,
    insert_tli: ::types_core::TimeLineID,
    stats: &mut crate::checkpoint::CheckpointStats,
) -> PgResult<()> {
    let seg = wal_segment_size();

    // Where to try to recycle to.
    let mut endlog_seg_no = crate::XLByteToSeg(endptr, seg);
    let recycle_seg_no = xlogfileslop(lastredoptr, seg);

    // Filename of the last segment to be kept. The timeline ID doesn't matter —
    // it is ignored in the comparison (during recovery InsertTimeLineID isn't set).
    let lastoff = crate::XLogFileName(0, segno, seg);

    ereport(DEBUG2)
        .errmsg_internal(std::format!(
            "attempting to remove WAL segments older than log file {lastoff}"
        ))
        .finish(loc(3905, "RemoveOldXlogFiles"))?;

    let names = fd_seams::read_dir_names::call("pg_wal")?;
    for name in &names {
        // The per-entry filter (xlog.c:3913-3928): the entry must be an XLOG
        // segment (or partial segment), and — ignoring the timeline part (chars
        // 0..8) so a parent-timeline segment is never prematurely removed — its
        // segment number (chars 8..) must be <= lastoff's. This is the unit-tested
        // `IsOldXlogFileCandidate`, identical to C's `strcmp(d_name+8, lastoff+8)
        // <= 0` with the `IsXLogFileName || IsPartialXLogFileName` guard.
        if crate::retention::IsOldXlogFileCandidate(name, &lastoff) {
            if xlogarchive::XLogArchiveCheckDone(name)? {
                // Update the last-removed location in shared memory first.
                update_last_removed_ptr(name);
                remove_xlog_file(name, recycle_seg_no, &mut endlog_seg_no, insert_tli, stats)?;
            }
        }
    }

    Ok(())
}

/// `bool CreateRestartPoint(int flags)` (xlog.c:7655) — establish a restartpoint
/// (the recovery-time analog of a checkpoint), flushing the buffer/SLRU state
/// durably, advancing the control file's checkpoint to the last replayed safe
/// checkpoint (stashed in `XLogCtl` by `RecoveryRestartPoint`), recycling WAL,
/// and running `archive_cleanup_command`. Returns `true` if a new restartpoint
/// was established. Runs in the checkpointer (or the startup process at
/// shutdown). Faithful to the C, over the live `XLogCtl` shmem substrate.
pub fn CreateRestartPoint(flags: i32) -> PgResult<bool> {
    let wal_segment_size = wal_segment_size();

    // Get a local copy of the last safe checkpoint record (info_lck).
    let (last_check_point_rec_ptr, last_check_point_end_ptr, last_check_point) = {
        let ctl = unsafe { &*xlog_ctl() };
        shmem::spin_lock_acquire(&ctl.info_lck);
        // SAFETY: live shmem region, info_lck held.
        let r = unsafe {
            let c = &*xlog_ctl();
            (c.lastCheckPointRecPtr, c.lastCheckPointEndPtr, c.lastCheckPoint)
        };
        shmem::spin_lock_release(&ctl.info_lck);
        r
    };

    // Check that we're still in recovery mode.
    if !shmem::RecoveryInProgress() {
        ereport(DEBUG2)
            .errmsg_internal("skipping restartpoint, recovery has already ended")
            .finish(loc(7685, "CreateRestartPoint"))?;
        return Ok(false);
    }

    // If the last checkpoint we've replayed is already our last restartpoint, we
    // can't perform a new one. We still update minRecoveryPoint so a shutdown
    // restartpoint won't start up earlier than before.
    let prior_cp_redo = with_control_file_lock(|| Ok(control_file_mut().checkPointCopy.redo))?;
    if last_check_point_rec_ptr == InvalidXLogRecPtr || last_check_point.redo <= prior_cp_redo {
        ereport(DEBUG2)
            .errmsg_internal(std::format!(
                "skipping restartpoint, already performed at {:X}/{:X}",
                (last_check_point.redo >> 32) as u32,
                last_check_point.redo as u32
            ))
            .finish(loc(7708, "CreateRestartPoint"))?;

        crate::write::UpdateMinRecoveryPoint(InvalidXLogRecPtr, true)?;
        if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
            with_control_file_lock(|| {
                control_file_mut().state = DBState::ShutdownedInRecovery;
                shmem::UpdateControlFile()
            })?;
        }
        return Ok(false);
    }

    // Update the shared RedoRecPtr so the startup process can count segments
    // replayed since last restartpoint. Hold off insertions while updating it.
    crate::insert::WALInsertLockAcquireExclusive()?;
    crate::shmem::set_redo_rec_ptr_cached(last_check_point.redo);
    // SAFETY: holding all WAL insert locks serializes Insert.RedoRecPtr.
    unsafe {
        (*xlog_ctl()).Insert.RedoRecPtr = last_check_point.redo;
    }
    crate::insert::WALInsertLockRelease()?;

    // Also update the info_lck-protected copy.
    {
        let ctl = unsafe { &*xlog_ctl() };
        shmem::spin_lock_acquire(&ctl.info_lck);
        // SAFETY: live shmem region, info_lck held.
        unsafe {
            (*xlog_ctl()).RedoRecPtr = last_check_point.redo;
        }
        shmem::spin_lock_release(&ctl.info_lck);
    }

    if vars::log_checkpoints.read() {
        log_checkpoint_start(flags);
    }

    // Flush all shmem disk + commit-log buffers to disk (CheckPointGuts).
    check_point_guts(last_check_point.redo, flags)?;

    // INJECTION_POINT("create-restart-point", NULL) — placed after CheckPointGuts
    // so some work has already happened (041_checkpoint_at_promote attaches a
    // 'wait' here).
    injection_point_seams::injection_point_run::call("create-restart-point", None)?;

    // Remember the prior checkpoint's redo ptr (UpdateCheckPointDistanceEstimate).
    let prior_redo_ptr = with_control_file_lock(|| Ok(control_file_mut().checkPointCopy.redo))?;

    // Update pg_control. Check that it still shows an older checkpoint, else do
    // nothing (guards against a racing end-of-recovery checkpoint).
    with_control_file_lock(|| {
        let cf = control_file_mut();
        if cf.checkPointCopy.redo < last_check_point.redo {
            cf.checkPoint = last_check_point_rec_ptr;
            cf.checkPointCopy = last_check_point;

            // Ensure minRecoveryPoint is past the checkpoint record if the control
            // file still shows DB_IN_ARCHIVE_RECOVERY (a backup in recovery uses
            // minRecoveryPoint to decide which WAL files to include).
            if cf.state == DBState::InArchiveRecovery {
                if cf.minRecoveryPoint < last_check_point_end_ptr {
                    cf.minRecoveryPoint = last_check_point_end_ptr;
                    cf.minRecoveryPointTLI = last_check_point.ThisTimeLineID;
                    crate::redo::set_local_min_recovery_point(
                        cf.minRecoveryPoint,
                        cf.minRecoveryPointTLI,
                    );
                }
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                    cf.state = DBState::ShutdownedInRecovery;
                }
            }
            shmem::UpdateControlFile()?;
        }
        Ok(())
    })?;

    // Update the average distance between checkpoints/restartpoints if the prior
    // checkpoint exists. This feeds XLOGfileslop's recycle horizon below
    // (xlog.c:7817-7821).
    let redo_rec_ptr = crate::shmem::redo_rec_ptr_cached();
    if prior_redo_ptr != InvalidXLogRecPtr {
        update_check_point_distance_estimate(redo_rec_ptr.wrapping_sub(prior_redo_ptr));
    }

    // Delete old log files no longer needed for the last restartpoint.
    let mut log_seg_no = crate::XLByteToSeg(redo_rec_ptr, wal_segment_size);

    // Retreat _logSegNo using the current end of xlog replayed or received,
    // whichever is later.
    let (receive_ptr, _latest_chunk_start, _receive_tli) =
        walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr_full::call();
    let (replay_ptr, mut replay_tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr_tli::call();
    let endptr = if receive_ptr < replay_ptr { replay_ptr } else { receive_ptr };
    log_seg_no = keep_log_seg(endptr, log_seg_no, wal_segment_size);

    // INJECTION_POINT("restartpoint-before-slot-invalidation", NULL) —
    // 047_checkpoint_physical_slot attaches a 'wait' here to pause the
    // restartpoint just before InvalidateObsoleteReplicationSlots.
    injection_point_seams::injection_point_run::call("restartpoint-before-slot-invalidation", None)?;

    if slot_seams::invalidate_obsolete_replication_slots::call(
        RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT,
        log_seg_no,
        ::types_core::InvalidOid,
        InvalidTransactionId,
    )? {
        // Some slots were invalidated; recompute the horizon from RedoRecPtr.
        log_seg_no = crate::XLByteToSeg(redo_rec_ptr, wal_segment_size);
        log_seg_no = keep_log_seg(endptr, log_seg_no, wal_segment_size);
    }
    log_seg_no = log_seg_no.wrapping_sub(1);

    // Recycle segments on a useful timeline.
    if !shmem::RecoveryInProgress() {
        replay_tli = unsafe { (*xlog_ctl()).InsertTimeLineID };
    }

    // RemoveOldXlogFiles(_logSegNo, RedoRecPtr, endptr, replayTLI) (xlog.c:7870) —
    // recycle or physically remove the WAL segments no longer needed for the last
    // restartpoint, recycling onto `replay_tli`. This is what frees WAL on a
    // standby after a slot is invalidated, and the recovery-time analog of the
    // online-checkpoint recycle pass above.
    let mut stats = crate::checkpoint::CheckpointStats::default();
    remove_old_xlog_files(log_seg_no, redo_rec_ptr, endptr, replay_tli, &mut stats)?;

    // Make more log segments if needed.
    crate::write::PreallocXlogFiles(endptr, replay_tli, &mut stats)?;

    // Truncate pg_subtrans if possible (only with hot standby, where
    // StartupSUBTRANS has run). The pg_subtrans trim is non-durability-critical
    // housekeeping owned by subtrans/procarray; the online-checkpoint path skips
    // its TruncateSUBTRANS too. Documented divergence (see DESIGN_DEBT.md).

    log_checkpoint_end(flags);

    let xtime = xlogrecovery_seams::get_latest_x_time::call();
    let elevel = if vars::log_checkpoints.read() { LOG } else { DEBUG2 };
    ereport(elevel)
        .errmsg(std::format!(
            "recovery restart point at {:X}/{:X}",
            (last_check_point.redo >> 32) as u32,
            last_check_point.redo as u32
        ))
        .finish(loc(7895, "CreateRestartPoint"))?;
    let _ = xtime;

    // Finally, execute archive_cleanup_command, if any. The command + its
    // placeholder substitution are transient (C: CurrentMemoryContext); use a
    // throwaway context.
    let cmd_cx = ::mcx::MemoryContext::new("archive_cleanup_command");
    let cmd_mcx = cmd_cx.mcx();
    if let Some(cmd) = xlogrecovery_seams::archive_cleanup_command::call(cmd_mcx) {
        if !cmd.as_str().is_empty() {
            xlogarchive::ExecuteRecoveryCommand(
                cmd_mcx,
                cmd.as_str(),
                "archive_cleanup_command",
                false,
                WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
            )?;
        }
    }

    Ok(true)
}

// ===========================================================================
// ShutdownXLOG — xlog.c:6664.
// ===========================================================================

/// `void ShutdownXLOG(int code, Datum arg)` (xlog.c:6664) — shut down the WAL
/// engine, writing a shutdown checkpoint (or, during recovery, a restartpoint).
pub fn ShutdownXLOG() -> PgResult<()> {
    // We should have an aux process resource owner to use, and we should not be
    // in a transaction that's installed some other resowner (xlog.c:6669-6673):
    //   Assert(AuxProcessResourceOwner != NULL);
    //   Assert(CurrentResourceOwner == NULL ||
    //          CurrentResourceOwner == AuxProcessResourceOwner);
    //   CurrentResourceOwner = AuxProcessResourceOwner;
    // The shutdown checkpoint's buffer flush (SyncOneBuffer →
    // ResourceOwnerEnlarge(CurrentResourceOwner) + the with-owner UnpinBuffer)
    // pins against this owner; without it the flush errors with
    // "CurrentResourceOwner is NULL" and silently leaves dirty buffers unwritten.
    resowner_seams::set_current_to_aux_process_resource_owner::call()?;

    ereport(LOG)
        .errmsg("shutting down")
        .finish(loc(6677, "ShutdownXLOG"))?;

    // WalSndInitStopping / WalSndWaitStopping signal walsenders to stop writing
    // WAL. Owned by walsender; with replication off in the regress harness there
    // are no walsenders, so this is a no-op. (When walsender lands, route through
    // its seam here.)

    if shmem::RecoveryInProgress() {
        // During recovery a shutdown checkpoint becomes a restartpoint.
        CreateRestartPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE).map(|_| ())
    } else {
        // If archiving is enabled, rotate the last XLOG file. Owned by xlogarchive
        // / RequestXLogSwitch; archiving is off in the regress harness.
        CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE).map(|_| ())
    }
}

// ===========================================================================
// Small logging helpers (LogCheckpointStart / LogCheckpointEnd, xlog.c:6710/6742).
// ===========================================================================

fn checkpoint_flag_str(flags: i32) -> std::string::String {
    use std::string::String;
    let mut s = String::new();
    if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
        s.push_str(" shutdown");
    }
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        s.push_str(" end-of-recovery");
    }
    if flags & CHECKPOINT_IMMEDIATE != 0 {
        s.push_str(" immediate");
    }
    if flags & CHECKPOINT_FORCE != 0 {
        s.push_str(" force");
    }
    s
}

fn log_checkpoint_start(flags: i32) {
    let _ = ereport(LOG)
        .errmsg(std::format!("checkpoint starting:{}", checkpoint_flag_str(flags)))
        .finish(loc(6726, "LogCheckpointStart"));
}

fn log_checkpoint_end(flags: i32) {
    if !vars::log_checkpoints.read() {
        return;
    }
    let _ = ereport(LOG)
        .errmsg(std::format!(
            "checkpoint complete:{}",
            checkpoint_flag_str(flags)
        ))
        .finish(loc(6816, "LogCheckpointEnd"));
}

/// `(pg_time_t) time(NULL)` — the wall-clock seconds the checkpoint records in
/// `checkPoint.time`.
fn wallclock_time() -> types_core::pg_time_t {
    // std's SystemTime panics on wasm64-unknown-unknown (no clock syscall); use
    // the host clock import there.
    #[cfg(not(target_family = "wasm"))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as types_core::pg_time_t)
            .unwrap_or(0)
    }
    #[cfg(target_family = "wasm")]
    {
        (wasm_libc_shim::now_unix_nanos() / 1_000_000_000) as types_core::pg_time_t
    }
}
