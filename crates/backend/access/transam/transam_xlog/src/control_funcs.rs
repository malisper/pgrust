//! The small WAL-record-emitting + control-file housekeeping functions of
//! `access/transam/xlog.c` (PostgreSQL 18.3):
//!
//!   * [`XLogPutNextOid`] (xlog.c:8092) — log the next-OID checkpoint hint.
//!   * [`RequestXLogSwitch`] (xlog.c:8129) — force a WAL segment switch.
//!   * [`XLogRestorePoint`] (xlog.c:8147) — log a named restore point.
//!   * [`XLogReportParameters`] (xlog.c:8172) — propagate hot-standby-critical
//!     GUC changes into `pg_control`.
//!   * [`UpdateFullPageWrites`] (xlog.c:8235) — push a SIGHUP change of
//!     `full_page_writes` into shmem (+ `XLOG_FPW_CHANGE` WAL record).
//!   * [`ReachedEndOfBackup`] (xlog.c:6309) — mark the control file consistent
//!     at end of base backup.
//!   * [`AllowCascadeReplication`] (xlog.h, walreceiver.h macro) — whether
//!     cascading walsenders may run.
//!
//! These all build on the now-ported WAL-insertion path (reached via the
//! `xloginsert` seams, since `xloginsert` depends on this crate's seams) and on
//! the ported `XLogCtl`/`ControlFile` shmem region. The GUC globals they read
//! (`wal_level`, `full_page_writes`, `MaxConnections`, …) are modeled through
//! `::guc_tables::vars`, exactly as the rest of the crate
//! reads its GUCs.

#![allow(non_snake_case)]

extern crate std;

use alloc::vec;

use ::utils_error::PgResult;
use types_core::{Oid, TimeLineID, XLogRecPtr};
use ::types_storage::storage::LW_EXCLUSIVE;
use ::wal::wal::{RM_XLOG_ID, XLOG_MARK_UNIMPORTANT};
use ::wal::xlog_consts::WalLevel;

use xloginsert_seams as xloginsert;
use lwlock as lwlock;
use timestamp_seams as timestamp;
use ::init_small::globals;
use ::guc_tables::vars;

use crate::insert::{WALInsertLockAcquireExclusive, WALInsertLockRelease};
use crate::shmem::{self, RecoveryInProgress, UpdateControlFile};
use crate::InvalidXLogRecPtr;

// XLOG resource-manager info opcodes (catalog/pg_control.h).
const XLOG_NEXTOID: u8 = 0x30;
const XLOG_SWITCH: u8 = 0x40;
const XLOG_PARAMETER_CHANGE: u8 = 0x60;
const XLOG_RESTORE_POINT: u8 = 0x70;
const XLOG_FPW_CHANGE: u8 = 0x80;

/// `ControlFileLock` — offset 9 in the `MainLWLockArray` (`lwlocklist.h`).
const CONTROL_FILE_LOCK: usize = 9;

/// `MAXFNAMELEN` (xlog_internal.h).
const MAXFNAMELEN: usize = 64;

/// `XLogIsNeeded()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`.
#[inline]
fn XLogIsNeeded() -> bool {
    vars::wal_level.read() >= WalLevel::Replica as i32
}

/// `XLogStandbyInfoActive()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`.
#[inline]
fn XLogStandbyInfoActive() -> bool {
    vars::wal_level.read() >= WalLevel::Replica as i32
}

/// `XLogPutNextOid(Oid nextOid)` (xlog.c:8092) — emit an `XLOG_NEXTOID` record
/// logging the next-OID checkpoint hint. Need not be flushed immediately (the
/// buffer-LSN interlock guarantees ordering for any OID that reaches disk).
pub fn XLogPutNextOid(next_oid: Oid) -> PgResult<()> {
    // XLogBeginInsert(); XLogRegisterData(&nextOid, sizeof(Oid));
    // (void) XLogInsert(RM_XLOG_ID, XLOG_NEXTOID);
    let bytes = next_oid.to_ne_bytes();
    xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_NEXTOID, 0, &[&bytes])?;
    Ok(())
}

/// `RequestXLogSwitch(bool mark_unimportant)` (xlog.c:8129) — write an
/// `XLOG_SWITCH` record (no data). Returns the end+1 LSN of the switch record,
/// or the end+1 of the prior segment if already at segment start.
pub fn RequestXLogSwitch(mark_unimportant: bool) -> PgResult<XLogRecPtr> {
    // XLOG SWITCH has no data. XLogBeginInsert();
    // if (mark_unimportant) XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
    // RecPtr = XLogInsert(RM_XLOG_ID, XLOG_SWITCH);
    let flags = if mark_unimportant {
        XLOG_MARK_UNIMPORTANT
    } else {
        0
    };
    xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_SWITCH, flags, &[])
}

/// `XLogRestorePoint(const char *rpName)` (xlog.c:8147) — write an
/// `XLOG_RESTORE_POINT` record carrying the current timestamp and `rpName`,
/// then `ereport(LOG)`. Returns the record's LSN.
pub fn XLogRestorePoint(rp_name: &str) -> PgResult<XLogRecPtr> {
    // xl_restore_point xlrec;
    // xlrec.rp_time = GetCurrentTimestamp();
    // strlcpy(xlrec.rp_name, rpName, MAXFNAMELEN);
    let rp_time = timestamp::get_current_timestamp::call();

    // Serialize the C `xl_restore_point` struct: TimestampTz rp_time @0,
    // char rp_name[MAXFNAMELEN] @8 (NUL-terminated, the rest zero-filled).
    let mut xlrec = vec![0u8; 8 + MAXFNAMELEN];
    xlrec[0..8].copy_from_slice(&rp_time.to_ne_bytes());
    // strlcpy: copy up to MAXFNAMELEN-1 bytes, always NUL-terminated.
    let src = rp_name.as_bytes();
    let n = core::cmp::min(src.len(), MAXFNAMELEN - 1);
    xlrec[8..8 + n].copy_from_slice(&src[..n]);

    // XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xl_restore_point));
    // RecPtr = XLogInsert(RM_XLOG_ID, XLOG_RESTORE_POINT);
    let recptr = xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_RESTORE_POINT, 0, &[&xlrec])?;

    // ereport(LOG, errmsg("restore point \"%s\" created at %X/%X", ...))
    let _ = recptr;
    Ok(recptr)
}

/// `XLogReportParameters(void)` (xlog.c:8172) — check whether any of the GUC
/// parameters critical for hot standby have changed and, if so, update them in
/// the control file (WAL-logging the change via `XLOG_PARAMETER_CHANGE` when
/// `wal_level` changed or WAL is needed).
pub fn XLogReportParameters() -> PgResult<()> {
    let wal_level = vars::wal_level.read();
    let wal_log_hints = vars::wal_log_hints.read();
    let max_connections = vars::MaxConnections.read();
    let max_worker_processes = vars::max_worker_processes.read();
    let max_wal_senders = vars::max_wal_senders.read();
    let max_prepared_xacts = vars::max_prepared_xacts.read();
    let max_locks_per_xact = vars::max_locks_per_xact.read();
    let track_commit_timestamp = vars::track_commit_timestamp.read();

    let cf = shmem::control_file_mut();

    if wal_level != cf.wal_level
        || wal_log_hints != cf.wal_log_hints
        || max_connections != cf.MaxConnections
        || max_worker_processes != cf.max_worker_processes
        || max_wal_senders != cf.max_wal_senders
        || max_prepared_xacts != cf.max_prepared_xacts
        || max_locks_per_xact != cf.max_locks_per_xact
        || track_commit_timestamp != cf.track_commit_timestamp
    {
        // The change in number of backend slots doesn't need to be WAL-logged
        // if archiving is not enabled; but if wal_level changed or WAL is
        // needed, write an XLOG_PARAMETER_CHANGE record.
        if wal_level != cf.wal_level || XLogIsNeeded() {
            // xl_parameter_change xlrec; (xlog_internal.h, sizeof == 28)
            //   MaxConnections@0, max_worker_processes@4, max_wal_senders@8,
            //   max_prepared_xacts@12, max_locks_per_xact@16, wal_level@20,
            //   wal_log_hints@24, track_commit_timestamp@25.
            let mut xlrec = vec![0u8; 28];
            xlrec[0..4].copy_from_slice(&max_connections.to_ne_bytes());
            xlrec[4..8].copy_from_slice(&max_worker_processes.to_ne_bytes());
            xlrec[8..12].copy_from_slice(&max_wal_senders.to_ne_bytes());
            xlrec[12..16].copy_from_slice(&max_prepared_xacts.to_ne_bytes());
            xlrec[16..20].copy_from_slice(&max_locks_per_xact.to_ne_bytes());
            xlrec[20..24].copy_from_slice(&wal_level.to_ne_bytes());
            xlrec[24] = wal_log_hints as u8;
            xlrec[25] = track_commit_timestamp as u8;

            // XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xlrec));
            // recptr = XLogInsert(RM_XLOG_ID, XLOG_PARAMETER_CHANGE);
            // XLogFlush(recptr);
            let recptr =
                xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_PARAMETER_CHANGE, 0, &[&xlrec])?;
            crate::write::XLogFlush(recptr)?;
        }

        // LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
        lwlock::LWLockAcquire(control_file_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;

        // Re-acquire a fresh borrow after the WAL insert above; the shmem image
        // is stable but the borrow checker cannot prove it across the call.
        let cf = shmem::control_file_mut();
        cf.MaxConnections = max_connections;
        cf.max_worker_processes = max_worker_processes;
        cf.max_wal_senders = max_wal_senders;
        cf.max_prepared_xacts = max_prepared_xacts;
        cf.max_locks_per_xact = max_locks_per_xact;
        cf.wal_level = wal_level;
        cf.wal_log_hints = wal_log_hints;
        cf.track_commit_timestamp = track_commit_timestamp;
        UpdateControlFile()?;

        lwlock::LWLockRelease(control_file_lock)?;
    }

    Ok(())
}

/// `UpdateFullPageWrites(void)` (xlog.c:8235) — propagate a SIGHUP change of
/// `full_page_writes` into `XLogCtl->Insert.fullPageWrites`, writing an
/// `XLOG_FPW_CHANGE` record if required.
///
/// Assumes no other process concurrently updates `Insert->fullPageWrites`.
pub fn UpdateFullPageWrites() -> PgResult<()> {
    let full_page_writes = vars::fullPageWrites.read();

    // XLogCtlInsert *Insert = &XLogCtl->Insert;
    let ctl_ptr = shmem::xlog_ctl();
    assert!(!ctl_ptr.is_null(), "XLogCtl shmem not initialized");

    // Do nothing if full_page_writes has not been changed. Safe to read the
    // shared flag without the lock (no concurrent updater, by assumption).
    // SAFETY: live shmem region set by XLOGShmemInit.
    let insert_fpw = unsafe { (*ctl_ptr).Insert.fullPageWrites };
    if full_page_writes == insert_fpw {
        return Ok(());
    }

    // Perform RecoveryInProgress() outside the critical section.
    let recovery_in_progress = RecoveryInProgress();

    // START_CRIT_SECTION();

    // It's always safe to take full page images. So if setting to true, set the
    // flag first then write the WAL record; if setting to false, write the WAL
    // record first then clear the flag.
    if full_page_writes {
        WALInsertLockAcquireExclusive()?;
        // SAFETY: holding all insertion locks exclusively serializes the write.
        unsafe {
            (*ctl_ptr).Insert.fullPageWrites = true;
        }
        WALInsertLockRelease()?;
    }

    // Write an XLOG_FPW_CHANGE record so archive recovery can track
    // full_page_writes if required.
    if XLogStandbyInfoActive() && !recovery_in_progress {
        // XLogRegisterData(&fullPageWrites, sizeof(bool));
        let bytes = [full_page_writes as u8];
        xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_FPW_CHANGE, 0, &[&bytes])?;
    }

    if !full_page_writes {
        WALInsertLockAcquireExclusive()?;
        // SAFETY: holding all insertion locks exclusively serializes the write.
        unsafe {
            (*ctl_ptr).Insert.fullPageWrites = false;
        }
        WALInsertLockRelease()?;
    }
    // END_CRIT_SECTION();

    Ok(())
}

/// `ReachedEndOfBackup(XLogRecPtr EndRecPtr, TimeLineID tli)` (xlog.c:6309) —
/// at the end of a base backup, the on-disk data is now consistent. Reset
/// `backupStartPoint`/`backupEndPoint`/`backupEndRequired` and bump
/// `minRecoveryPoint` so we cannot start up at an earlier point.
pub fn ReachedEndOfBackup(end_rec_ptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    // LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(control_file_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;

    let cf = shmem::control_file_mut();
    if cf.minRecoveryPoint < end_rec_ptr {
        cf.minRecoveryPoint = end_rec_ptr;
        cf.minRecoveryPointTLI = tli;
    }
    cf.backupStartPoint = InvalidXLogRecPtr;
    cf.backupEndPoint = InvalidXLogRecPtr;
    cf.backupEndRequired = false;
    UpdateControlFile()?;

    // LWLockRelease(ControlFileLock);
    lwlock::LWLockRelease(control_file_lock)?;
    Ok(())
}

/// `AllowCascadeReplication()` (replication/walreceiver.h:40):
/// `EnableHotStandby && max_wal_senders > 0`. Whether cascading walsenders may
/// run, governing whether the redo loop wakes physical/logical walsenders.
#[inline]
pub fn AllowCascadeReplication() -> bool {
    vars::EnableHotStandby.read() && vars::max_wal_senders.read() > 0
}
