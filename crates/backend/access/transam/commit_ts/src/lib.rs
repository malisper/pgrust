//! `commit_ts.c` — PostgreSQL commit-timestamp (`pg_commit_ts`) manager
//! (`src/backend/access/transam/commit_ts.c`, PostgreSQL 18.3).
//!
//! A `pg_xact`-like system that stores, per transaction, the commit timestamp
//! and the replication-origin node id. Active only when the
//! `track_commit_timestamp` GUC is enabled.
//!
//! The SLRU buffer machinery (`SimpleLru*`) is consumed directly from the
//! ported sibling [`slru`]. The SLRU control struct
//! (C file-static `CommitTsCtlData`) and the cached last-committed value (C's
//! `commitTsShared` `ShmemInitStruct` block) are owned here by
//! [`CommitTsState`]; the genuinely-shared instance lives behind a
//! process-global `Mutex` materialized by [`CommitTsShmemInit`] and reached by
//! the inward seams via [`with_commit_ts_state`].
//!
//! `CommitTsLock` is one of lwlock.c's fixed individual locks (offset 39 in
//! `MainLWLockArray`, `lwlocklist.h`); it is acquired/released through the
//! ported `backend-storage-lmgr-lwlock` crate's main-array surface.
//! `TransamVariables->{oldest,newest}CommitTsXid` and `nextXid` are owned by
//! varsup.c, reached through `backend-access-transam-varsup-seams`. WAL
//! insertion goes through `backend-access-transam-xloginsert-seams`; the GUCs
//! are read directly from the `backend-utils-misc-guc-tables` slots.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use ::slru::{
    check_slru_buffers, SimpleLruAutotuneBuffers, SimpleLruDoesPhysicalPageExist,
    SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruShmemSize, SimpleLruTruncate, SimpleLruWriteAll, SimpleLruWritePage,
    SimpleLruZeroPage, SlruCtlData, SlruPagePrecedesUnitTests, SlruScanDirCbDeleteAll,
    SlruScanDirCbReportPresence, SlruScanDirectory, SlruSyncFileTag, SLRU_MAX_ALLOWED_BUFFERS,
};
use ::transam::{
    TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
};
use ::lwlock::{LWLockAcquire, LWLockRelease};
use ::types_storage::{LW_EXCLUSIVE, LW_SHARED};
use ::utils_error::errno::current_errno;
use ::utils_error::{ereport, PgError, PgResult};

use ::types_core::{
    FirstNormalTransactionId, InvalidRepOriginId, InvalidTransactionId, ProcNumber, RepOriginId,
    Size, TimestampTz, TransactionId, BLCKSZ,
};
use ::types_error::{ERROR, PANIC};
use ::types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_storage::sync::SyncRequestHandler;
use ::types_storage::{LWTRANCHE_COMMITTS_BUFFER, LWTRANCHE_COMMITTS_SLRU};

use varsup_seams as varsup;
use ::guc_tables::vars;
use ::types_guc::guc::{PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT, PGC_S_OVERRIDE};

#[cfg(test)]
mod tests;

pub mod fmgr_builtins;

// ===========================================================================
// commit_ts.h constants
// ===========================================================================

/// `COMMIT_TS_ZEROPAGE` (access/commit_ts.h).
pub const COMMIT_TS_ZEROPAGE: u8 = 0x00;
/// `COMMIT_TS_TRUNCATE` (access/commit_ts.h).
pub const COMMIT_TS_TRUNCATE: u8 = 0x10;

/// `RM_COMMIT_TS_ID` — the CommitTs resource manager (rmgrlist.h entry 18).
pub const RM_COMMIT_TS_ID: ::types_core::RmgrId = 18;

/// `CommitTsLock` — fixed individual LWLock at offset 39 in `MainLWLockArray`
/// (`PG_LWLOCK(39, CommitTs)`, lwlocklist.h).
const COMMIT_TS_LOCK_OFFSET: usize = 39;

/// `DT_NOBEGIN` — the "minus infinity" timestamp value
/// (`TIMESTAMP_NOBEGIN`/`PG_INT64_MIN`, datatype/timestamp.h).
const DT_NOBEGIN: TimestampTz = TimestampTz::MIN;

// ===========================================================================
// On-disk entry layout
// ===========================================================================

/// On-disk per-transaction commit-timestamp entry (`CommitTimestampEntry`):
/// `{ TimestampTz time; RepOriginId nodeid; }`, packed (no trailing padding).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitTimestampEntry {
    pub time: TimestampTz,
    pub nodeid: RepOriginId,
}

/// `SizeOfCommitTimestampEntry = offsetof(CommitTimestampEntry, nodeid) +
/// sizeof(RepOriginId)` — `TimestampTz`(8) + `RepOriginId`(2) = 10.
pub const SizeOfCommitTimestampEntry: usize =
    core::mem::size_of::<TimestampTz>() + core::mem::size_of::<RepOriginId>();

/// `COMMIT_TS_XACTS_PER_PAGE = BLCKSZ / SizeOfCommitTimestampEntry`.
pub const COMMIT_TS_XACTS_PER_PAGE: i64 = (BLCKSZ / SizeOfCommitTimestampEntry) as i64;

/// `TransactionIdToCTsPage(xid) = xid / COMMIT_TS_XACTS_PER_PAGE`.
#[inline]
fn TransactionIdToCTsPage(xid: TransactionId) -> i64 {
    xid as i64 / COMMIT_TS_XACTS_PER_PAGE
}

/// `TransactionIdToCTsEntry(xid) = xid % COMMIT_TS_XACTS_PER_PAGE`.
#[inline]
fn TransactionIdToCTsEntry(xid: TransactionId) -> u32 {
    xid % (COMMIT_TS_XACTS_PER_PAGE as TransactionId)
}

/// Serialize a [`CommitTimestampEntry`] into the packed 10-byte on-disk layout
/// (`memcpy(page + SizeOfCommitTimestampEntry * entryno, &entry, ...)`).
fn write_entry(page: &mut [u8], entryno: u32, entry: CommitTimestampEntry) {
    let base = SizeOfCommitTimestampEntry * entryno as usize;
    page[base..base + 8].copy_from_slice(&entry.time.to_ne_bytes());
    page[base + 8..base + 10].copy_from_slice(&entry.nodeid.to_ne_bytes());
}

/// Deserialize a [`CommitTimestampEntry`] from the packed on-disk layout.
fn read_entry(page: &[u8], entryno: u32) -> CommitTimestampEntry {
    let base = SizeOfCommitTimestampEntry * entryno as usize;
    let time = TimestampTz::from_ne_bytes(page[base..base + 8].try_into().unwrap());
    let nodeid = RepOriginId::from_ne_bytes(page[base + 8..base + 10].try_into().unwrap());
    CommitTimestampEntry { time, nodeid }
}

// ===========================================================================
// Module state
// ===========================================================================

/// `CommitTimestampShared` — the cached last-committed value plus the module's
/// activation status (protected in C by `CommitTsLock`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitTimestampShared {
    pub xidLastCommit: TransactionId,
    pub dataLastCommit: CommitTimestampEntry,
    pub commitTsActive: bool,
}

impl Default for CommitTimestampShared {
    fn default() -> Self {
        Self {
            xidLastCommit: InvalidTransactionId,
            dataLastCommit: CommitTimestampEntry {
                time: DT_NOBEGIN,
                nodeid: InvalidRepOriginId,
            },
            commitTsActive: false,
        }
    }
}

/// Module state for the commit-ts manager: the SLRU control object
/// (`CommitTsCtlData` / `CommitTsCtl`) and the cached shared state
/// (`commitTsShared`).
pub struct CommitTsState {
    /// `CommitTsCtl` — the SLRU control data for pg_commit_ts.
    pub CommitTsCtl: SlruCtlData,
    /// `commitTsShared` — the cached last-committed value and activation status.
    pub shared: CommitTimestampShared,
}

// ===========================================================================
// CommitTsLock helpers (the fixed individual LWLock)
// ===========================================================================

fn my_proc_number() -> ProcNumber {
    init_small_seams::my_proc_number::call()
}

/// `LWLockAcquire(CommitTsLock, LW_EXCLUSIVE | LW_SHARED)`. Mirrors C's bare
/// acquire/release of the fixed `CommitTsLock` (the SLRU bank-lock discipline;
/// an `ereport(ERROR)` between acquire and release unwinds with the lock held,
/// released by the transaction-abort `LWLockReleaseAll`).
fn commit_ts_lock_acquire(exclusive: bool) -> PgResult<()> {
    let mode = if exclusive { LW_EXCLUSIVE } else { LW_SHARED };
    LWLockAcquire(commit_ts_lock(), mode, my_proc_number())?;
    Ok(())
}

/// `LWLockRelease(CommitTsLock)`.
fn commit_ts_lock_release() -> PgResult<()> {
    LWLockRelease(commit_ts_lock())
}

fn commit_ts_lock() -> &'static ::types_storage::LWLock {
    ::lwlock::main_lock_ref(COMMIT_TS_LOCK_OFFSET)
}

// ===========================================================================
// Page-precedes comparator
// ===========================================================================

/// `CommitTsPagePrecedes(page1, page2)` — decide whether a commit-ts page
/// number is "older" for truncation purposes. Analogous to `CLOGPagePrecedes`.
fn CommitTsPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 =
        (page1 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE as TransactionId);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 =
        (page2 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE as TransactionId);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(
            xid1,
            xid2.wrapping_add(COMMIT_TS_XACTS_PER_PAGE as TransactionId - 1),
        )
}

// ===========================================================================
// Set commit-ts data
// ===========================================================================

/// `TransactionTreeSetCommitTsData` — record the commit timestamp for a
/// transaction tree (parent plus subxids), as efficiently as possible.
pub fn TransactionTreeSetCommitTsData(
    state: &mut CommitTsState,
    xid: TransactionId,
    subxids: &[TransactionId],
    timestamp: TimestampTz,
    nodeid: RepOriginId,
) -> PgResult<()> {
    let nsubxids = subxids.len();

    // No-op if the module is not active. An unlocked read here is fine, because
    // in a standby (the only place the flag can change in flight) this routine
    // is only called by the recovery process, which is also the only process
    // which can change the flag.
    if !state.shared.commitTsActive {
        return Ok(());
    }

    // Figure out the latest Xid in this batch: the last subxid if any,
    // otherwise the parent xid.
    let newestXact = if nsubxids > 0 {
        subxids[nsubxids - 1]
    } else {
        xid
    };

    // Split the xids to set the timestamp to in groups belonging to the same
    // SLRU page; the first element in each such set is its head. The first
    // group has the main XID as the head; subsequent sets use the first subxid
    // not on the previous page as head. This way, we only lock/modify each SLRU
    // page once.
    let mut headxid = xid;
    let mut i = 0usize;
    loop {
        let pageno = TransactionIdToCTsPage(headxid);

        let mut j = i;
        while j < nsubxids {
            if TransactionIdToCTsPage(subxids[j]) != pageno {
                break;
            }
            j += 1;
        }
        // subxids[i..j] are on the same page as the head

        SetXidCommitTsInPage(state, headxid, &subxids[i..j], timestamp, nodeid, pageno)?;

        // if we wrote out all subxids, we're done.
        if j >= nsubxids {
            break;
        }

        // Set the new head and skip over it, plus the subxids we just wrote.
        headxid = subxids[j];
        i = j + 1;
    }

    // Update the cached value in shared memory.
    commit_ts_lock_acquire(true)?;
    state.shared.xidLastCommit = xid;
    state.shared.dataLastCommit.time = timestamp;
    state.shared.dataLastCommit.nodeid = nodeid;

    // And move forwards our endpoint, if needed.
    if TransactionIdPrecedes(varsup::get_newest_commit_ts_xid::call(), newestXact) {
        varsup::set_newest_commit_ts_xid::call(newestXact);
    }
    commit_ts_lock_release()?;
    Ok(())
}

/// Record the commit timestamp of transaction entries for all entries on a
/// single page. Atomic only on this page.
fn SetXidCommitTsInPage(
    state: &mut CommitTsState,
    xid: TransactionId,
    subxids: &[TransactionId],
    ts: TimestampTz,
    nodeid: RepOriginId,
    pageno: i64,
) -> PgResult<()> {
    LWLockAcquire(
        SimpleLruGetBankLock(&state.CommitTsCtl, pageno),
        LW_EXCLUSIVE,
        my_proc_number(),
    )?;

    let slotno = SimpleLruReadPage(&mut state.CommitTsCtl, pageno, true, xid)?;

    TransactionIdSetCommitTs(state, xid, ts, nodeid, slotno);
    for &subxid in subxids {
        TransactionIdSetCommitTs(state, subxid, ts, nodeid, slotno);
    }

    state.CommitTsCtl.shared.page_dirty[slotno] = true;

    LWLockRelease(SimpleLruGetBankLock(&state.CommitTsCtl, pageno))?;
    Ok(())
}

/// Sets the commit timestamp of a single transaction. Caller must hold the
/// correct SLRU bank lock, which will be held at exit.
fn TransactionIdSetCommitTs(
    state: &mut CommitTsState,
    xid: TransactionId,
    ts: TimestampTz,
    nodeid: RepOriginId,
    slotno: usize,
) {
    let entryno = TransactionIdToCTsEntry(xid);
    debug_assert!(TransactionIdIsNormal(xid));

    let entry = CommitTimestampEntry { time: ts, nodeid };

    write_entry(
        state.CommitTsCtl.shared.page_buffer_mut(slotno),
        entryno,
        entry,
    );
}

// ===========================================================================
// Get commit-ts data
// ===========================================================================

/// `TransactionIdGetCommitTsData` — interrogate the commit timestamp of a
/// transaction. Returns `Ok(Some((ts, nodeid)))` when a non-zero commit
/// timestamp record was found (C's `*ts != 0`), `Ok(None)` otherwise.
pub fn TransactionIdGetCommitTsData(
    state: &mut CommitTsState,
    xid: TransactionId,
) -> PgResult<Option<(TimestampTz, RepOriginId)>> {
    let pageno = TransactionIdToCTsPage(xid);
    let entryno = TransactionIdToCTsEntry(xid);

    if !TransactionIdIsValid(xid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot retrieve commit timestamp for transaction {xid}"
            ))
            .into_error());
    } else if !TransactionIdIsNormal(xid) {
        // frozen and bootstrap xids are always committed far in the past
        return Ok(None);
    }

    commit_ts_lock_acquire(false)?;

    // Error if module not enabled.
    if !state.shared.commitTsActive {
        commit_ts_lock_release()?;
        return Err(error_commit_ts_disabled());
    }

    // If we're asked for the cached value, return that; otherwise fall through
    // to read from SLRU.
    if state.shared.xidLastCommit == xid {
        let ts = state.shared.dataLastCommit.time;
        let nodeid = state.shared.dataLastCommit.nodeid;
        commit_ts_lock_release()?;
        return Ok((ts != 0).then_some((ts, nodeid)));
    }

    let oldestCommitTsXid = varsup::get_oldest_commit_ts_xid::call();
    let newestCommitTsXid = varsup::get_newest_commit_ts_xid::call();
    // neither is invalid, or both are
    debug_assert_eq!(
        TransactionIdIsValid(oldestCommitTsXid),
        TransactionIdIsValid(newestCommitTsXid)
    );
    commit_ts_lock_release()?;

    // Return empty if the requested value is outside our valid range.
    if !TransactionIdIsValid(oldestCommitTsXid)
        || TransactionIdPrecedes(xid, oldestCommitTsXid)
        || TransactionIdPrecedes(newestCommitTsXid, xid)
    {
        return Ok(None);
    }

    // lock is acquired by SimpleLruReadPage_ReadOnly
    let slotno = SimpleLruReadPage_ReadOnly(&mut state.CommitTsCtl, pageno, xid)?;
    let entry = read_entry(state.CommitTsCtl.shared.page_buffer(slotno), entryno);

    let ts = entry.time;
    let nodeid = entry.nodeid;

    // SimpleLruReadPage_ReadOnly leaves the bank lock held; release it (the C
    // code releases SimpleLruGetBankLock(CommitTsCtl, pageno) here).
    LWLockRelease(SimpleLruGetBankLock(&state.CommitTsCtl, pageno))?;
    Ok((ts != 0).then_some((ts, nodeid)))
}

/// `GetLatestCommitTsData` — return the Xid of the latest committed transaction
/// (as far as this module is concerned), with its timestamp and origin.
pub fn GetLatestCommitTsData(
    state: &mut CommitTsState,
) -> PgResult<(TransactionId, TimestampTz, RepOriginId)> {
    commit_ts_lock_acquire(false)?;

    // Error if module not enabled.
    if !state.shared.commitTsActive {
        commit_ts_lock_release()?;
        return Err(error_commit_ts_disabled());
    }

    let xid = state.shared.xidLastCommit;
    let ts = state.shared.dataLastCommit.time;
    let nodeid = state.shared.dataLastCommit.nodeid;
    commit_ts_lock_release()?;

    Ok((xid, ts, nodeid))
}

/// `error_commit_ts_disabled` — raise the "could not get commit timestamp
/// data" error, with a recovery-aware hint.
fn error_commit_ts_disabled() -> PgError {
    let builder = ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg("could not get commit timestamp data");
    let builder = if transam_xlog_seams::recovery_in_progress::call() {
        builder.errhint(
            "Make sure the configuration parameter \"track_commit_timestamp\" is set on the primary server.",
        )
    } else {
        builder.errhint("Make sure the configuration parameter \"track_commit_timestamp\" is set.")
    };
    builder.into_error()
}

// ===========================================================================
// SQL-callable accessors
// ===========================================================================

/// `pg_xact_commit_timestamp` — SQL-callable wrapper to obtain commit time of a
/// transaction. `None` is SQL NULL (no commit timestamp found).
pub fn pg_xact_commit_timestamp(
    state: &mut CommitTsState,
    xid: TransactionId,
) -> PgResult<Option<TimestampTz>> {
    let found = TransactionIdGetCommitTsData(state, xid)?;
    Ok(found.map(|(ts, _nodeid)| ts))
}

/// `pg_last_committed_xact` — SQL-callable wrapper returning the latest
/// committed transaction's id, commit timestamp, and replication origin.
/// `None` is the all-NULL row (xid not normal).
pub fn pg_last_committed_xact(
    state: &mut CommitTsState,
) -> PgResult<Option<(TransactionId, TimestampTz, RepOriginId)>> {
    let (xid, ts, nodeid) = GetLatestCommitTsData(state)?;

    if !TransactionIdIsNormal(xid) {
        Ok(None)
    } else {
        Ok(Some((xid, ts, nodeid)))
    }
}

/// `pg_xact_commit_timestamp_origin` — SQL-callable wrapper returning the
/// commit timestamp and replication origin of a given transaction. `None` is
/// the all-NULL row (no data found).
pub fn pg_xact_commit_timestamp_origin(
    state: &mut CommitTsState,
    xid: TransactionId,
) -> PgResult<Option<(TimestampTz, RepOriginId)>> {
    TransactionIdGetCommitTsData(state, xid)
}

// ===========================================================================
// Shared-memory sizing and initialization
// ===========================================================================

/// Number of shared CommitTS buffers (`CommitTsShmemBuffers`).
fn CommitTsShmemBuffers() -> i32 {
    let buffers = vars::commit_timestamp_buffers.read();
    if buffers == 0 {
        // auto-tune based on shared buffers
        SimpleLruAutotuneBuffers(512, 1024)
    } else {
        buffers.max(16).min(SLRU_MAX_ALLOWED_BUFFERS)
    }
}

/// `CommitTsShmemSize` — shared-memory size for the commit-ts SLRU + control.
pub fn CommitTsShmemSize() -> Size {
    SimpleLruShmemSize(CommitTsShmemBuffers(), 0)
        + core::mem::size_of::<CommitTimestampShared>()
}

/// `CommitTsShmemInit` — initialize CommitTs at system startup (postmaster
/// start or standalone backend).
pub fn CommitTsShmemInit() -> PgResult<CommitTsState> {
    // If auto-tuning is requested, now is the time to do it.
    if vars::commit_timestamp_buffers.read() == 0 {
        let buf = CommitTsShmemBuffers().to_string();

        // SetConfigOption("commit_timestamp_buffers", buf, PGC_POSTMASTER,
        //                 PGC_S_DYNAMIC_DEFAULT)
        guc_seams::set_config_option::call(
            "commit_timestamp_buffers",
            &buf,
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        )?;

        // We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
        // However, if the DBA explicitly set commit_timestamp_buffers = 0 in
        // the config file, then PGC_S_DYNAMIC_DEFAULT fails to override that
        // and we must force the matter with PGC_S_OVERRIDE.
        if vars::commit_timestamp_buffers.read() == 0 {
            guc_seams::set_config_option::call(
                "commit_timestamp_buffers",
                &buf,
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(vars::commit_timestamp_buffers.read() != 0);

    let nslots = CommitTsShmemBuffers();
    let mut ctl = SimpleLruInit(
        "commit_timestamp",
        nslots,
        0,
        "pg_commit_ts",
        LWTRANCHE_COMMITTS_BUFFER,
        LWTRANCHE_COMMITTS_SLRU,
        SyncRequestHandler::SYNC_HANDLER_COMMIT_TS,
        false,
    )?;
    ctl.PagePrecedes = Some(CommitTsPagePrecedes);
    SlruPagePrecedesUnitTests(&ctl, COMMIT_TS_XACTS_PER_PAGE as i32);

    Ok(CommitTsState {
        CommitTsCtl: ctl,
        shared: CommitTimestampShared::default(),
    })
}

std::thread_local! {
    /// `static int commit_timestamp_buffers` (commit_ts.c) — the
    /// `commit_timestamp_buffers` GUC's backing storage (`conf->variable`).
    static COMMIT_TIMESTAMP_BUFFERS: core::cell::Cell<i32> = const { core::cell::Cell::new(0) };
    /// `bool track_commit_timestamp` (commit_ts.c:109) — the
    /// `track_commit_timestamp` GUC's backing storage.
    static TRACK_COMMIT_TIMESTAMP: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// `check_commit_ts_buffers` — GUC check_hook for `commit_timestamp_buffers`.
/// Returns `(ok, errdetail)`; the C function returns `bool` and sets
/// `GUC_check_errdetail` on failure.
pub fn check_commit_ts_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("commit_timestamp_buffers", newval)
}

/// `BootStrapCommitTs` — must be called ONCE on system install. Nothing to do
/// at present; segments are created when the server starts with this module
/// enabled (see [`ActivateCommitTs`]).
pub fn BootStrapCommitTs(_state: &mut CommitTsState) {}

/// `ZeroCommitTsPage` — initialize (or reinitialize) a page of CommitTs to
/// zeroes. If `writeXlog` is true, also emit an XLOG record. The (bank) lock
/// must be held at entry, and will be held at exit. Returns the slot number.
fn ZeroCommitTsPage(state: &mut CommitTsState, pageno: i64, writeXlog: bool) -> PgResult<usize> {
    let slotno = SimpleLruZeroPage(&mut state.CommitTsCtl, pageno)?;

    if writeXlog {
        WriteZeroPageXlogRec(pageno)?;
    }

    Ok(slotno)
}

/// `StartupCommitTs` — must be called ONCE during postmaster or
/// standalone-backend startup, after StartupXLOG has initialized
/// `TransamVariables->nextXid`.
pub fn StartupCommitTs(state: &mut CommitTsState) -> PgResult<()> {
    ActivateCommitTs(state)
}

/// `CompleteCommitTsInitialization` — must be called ONCE during postmaster or
/// standalone-backend startup, after recovery has finished.
pub fn CompleteCommitTsInitialization(state: &mut CommitTsState) -> PgResult<()> {
    // If the feature is not enabled, turn it off for good (also removes any
    // leftover data). Conversely, activate the module if enabled — necessary
    // for primary and standby as the activation depends on the control file
    // contents at the beginning of recovery or when XLOG_PARAMETER_CHANGE is
    // replayed.
    if !vars::track_commit_timestamp.read() {
        DeactivateCommitTs(state)
    } else {
        ActivateCommitTs(state)
    }
}

/// `CommitTsParameterChange` — activate or deactivate CommitTs upon reception
/// of a XLOG_PARAMETER_CHANGE XLog record during recovery.
pub fn CommitTsParameterChange(
    state: &mut CommitTsState,
    newvalue: bool,
    _oldvalue: bool,
) -> PgResult<()> {
    // Note this only runs in the recovery process, so an unlocked read is fine.
    if newvalue {
        if !state.shared.commitTsActive {
            ActivateCommitTs(state)?;
        }
    } else if state.shared.commitTsActive {
        DeactivateCommitTs(state)?;
    }
    Ok(())
}

/// `ActivateCommitTs` — activate this module whenever necessary. Creates the
/// currently active segment, if it's not already there.
fn ActivateCommitTs(state: &mut CommitTsState) -> PgResult<()> {
    // During bootstrap, we should not register commit timestamps, so skip the
    // activation in this case.
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    // If we've done this already, there's nothing to do.
    commit_ts_lock_acquire(true)?;
    if state.shared.commitTsActive {
        commit_ts_lock_release()?;
        return Ok(());
    }
    commit_ts_lock_release()?;

    let xid = varsup::read_next_full_transaction_id::call().xid();
    let pageno = TransactionIdToCTsPage(xid);

    // Re-Initialize our idea of the latest page number.
    state
        .CommitTsCtl
        .shared
        .latest_page_number
        .write(pageno as u64);

    // If CommitTs is enabled, but it wasn't in the previous server run, we need
    // to set the oldest and newest values to the next Xid; that way, we will
    // not try to read data that might not have been set.
    commit_ts_lock_acquire(true)?;
    if varsup::get_oldest_commit_ts_xid::call() == InvalidTransactionId {
        let next = varsup::read_next_transaction_id::call();
        varsup::set_oldest_commit_ts_xid::call(next);
        varsup::set_newest_commit_ts_xid::call(next);
    }
    commit_ts_lock_release()?;

    // Create the current segment file, if necessary.
    if !SimpleLruDoesPhysicalPageExist(&mut state.CommitTsCtl, pageno)? {
        LWLockAcquire(
            SimpleLruGetBankLock(&state.CommitTsCtl, pageno),
            LW_EXCLUSIVE,
            my_proc_number(),
        )?;
        let slotno = ZeroCommitTsPage(state, pageno, false)?;
        SimpleLruWritePage(&mut state.CommitTsCtl, slotno)?;
        debug_assert!(!state.CommitTsCtl.shared.page_dirty[slotno]);
        LWLockRelease(SimpleLruGetBankLock(&state.CommitTsCtl, pageno))?;
    }

    // Change the activation status in shared memory.
    commit_ts_lock_acquire(true)?;
    state.shared.commitTsActive = true;
    commit_ts_lock_release()?;
    Ok(())
}

/// `DeactivateCommitTs` — deactivate this module. Resets CommitTs into invalid
/// state; also removes segments of old data.
fn DeactivateCommitTs(state: &mut CommitTsState) -> PgResult<()> {
    // Cleanup the status in the shared memory. We reset everything in the
    // commitTsShared record to prevent the user from getting confusing data
    // about the last committed transaction on the standby when the module was
    // activated repeatedly on the primary.
    commit_ts_lock_acquire(true)?;

    state.shared.commitTsActive = false;
    state.shared.xidLastCommit = InvalidTransactionId;
    state.shared.dataLastCommit.time = DT_NOBEGIN;
    state.shared.dataLastCommit.nodeid = InvalidRepOriginId;

    varsup::set_oldest_commit_ts_xid::call(InvalidTransactionId);
    varsup::set_newest_commit_ts_xid::call(InvalidTransactionId);

    // Remove *all* files. This is necessary so that there are no leftover
    // files; in the case where this feature is later enabled after running with
    // it disabled for some time there may be a gap in the file sequence.
    SlruScanDirectory(&state.CommitTsCtl, |ctl, filename, segpage| {
        SlruScanDirCbDeleteAll(ctl, filename, segpage)
    })?;

    commit_ts_lock_release()?;
    Ok(())
}

/// `CheckPointCommitTs` — perform a checkpoint (during shutdown, or
/// on-the-fly). Writes dirty CommitTs pages to disk.
pub fn CheckPointCommitTs(state: &mut CommitTsState) -> PgResult<()> {
    SimpleLruWriteAll(&mut state.CommitTsCtl, true)
}

/// `ExtendCommitTs` — make sure that CommitTs has room for a newly-allocated
/// XID.
pub fn ExtendCommitTs(state: &mut CommitTsState, newestXact: TransactionId) -> PgResult<()> {
    // Nothing to do if module not enabled. Note we do an unlocked read of the
    // flag here, which is okay because this routine is only called from
    // GetNewTransactionId, which is never called in a standby.
    debug_assert!(!xlogrecovery_seams::in_recovery::call());
    if !state.shared.commitTsActive {
        return Ok(());
    }

    // No work except at first XID of a page. But beware: just after wraparound,
    // the first XID of page zero is FirstNormalTransactionId.
    if TransactionIdToCTsEntry(newestXact) != 0 && newestXact != FirstNormalTransactionId {
        return Ok(());
    }

    let pageno = TransactionIdToCTsPage(newestXact);

    LWLockAcquire(
        SimpleLruGetBankLock(&state.CommitTsCtl, pageno),
        LW_EXCLUSIVE,
        my_proc_number(),
    )?;

    // Zero the page and make an XLOG entry about it.
    let in_recovery = xlogrecovery_seams::in_recovery::call();
    ZeroCommitTsPage(state, pageno, !in_recovery)?;

    LWLockRelease(SimpleLruGetBankLock(&state.CommitTsCtl, pageno))?;
    Ok(())
}

/// `TruncateCommitTs` — remove all CommitTs segments before the one holding the
/// passed transaction ID. We don't need to flush XLOG here.
pub fn TruncateCommitTs(state: &mut CommitTsState, oldestXact: TransactionId) -> PgResult<()> {
    // The cutoff point is the start of the segment containing oldestXact. We
    // pass the *page* containing oldestXact to SimpleLruTruncate.
    let cutoffPage = TransactionIdToCTsPage(oldestXact);

    // Check to see if there are any files that could be removed.
    let found = SlruScanDirectory(&state.CommitTsCtl, |ctl, filename, segpage| {
        SlruScanDirCbReportPresence(ctl, filename, segpage, cutoffPage)
    })?;
    if !found {
        return Ok(()); // nothing to remove
    }

    // Write XLOG record.
    WriteTruncateXlogRec(cutoffPage, oldestXact)?;

    // Now we can remove the old CommitTs segment(s).
    SimpleLruTruncate(&mut state.CommitTsCtl, cutoffPage)
}

/// `SetCommitTsLimit` — set the limit values between which commit TS can be
/// consulted.
pub fn SetCommitTsLimit(oldestXact: TransactionId, newestXact: TransactionId) -> PgResult<()> {
    // Be careful not to overwrite values that are either further into the
    // "future" or signal a disabled committs.
    commit_ts_lock_acquire(true)?;
    if varsup::get_oldest_commit_ts_xid::call() != InvalidTransactionId {
        if TransactionIdPrecedes(varsup::get_oldest_commit_ts_xid::call(), oldestXact) {
            varsup::set_oldest_commit_ts_xid::call(oldestXact);
        }
        if TransactionIdPrecedes(newestXact, varsup::get_newest_commit_ts_xid::call()) {
            varsup::set_newest_commit_ts_xid::call(newestXact);
        }
    } else {
        debug_assert_eq!(
            varsup::get_newest_commit_ts_xid::call(),
            InvalidTransactionId
        );
        varsup::set_oldest_commit_ts_xid::call(oldestXact);
        varsup::set_newest_commit_ts_xid::call(newestXact);
    }
    commit_ts_lock_release()?;
    Ok(())
}

/// `AdvanceOldestCommitTsXid` — move forwards the oldest commitTS value that
/// can be consulted.
pub fn AdvanceOldestCommitTsXid(oldestXact: TransactionId) -> PgResult<()> {
    commit_ts_lock_acquire(true)?;
    if varsup::get_oldest_commit_ts_xid::call() != InvalidTransactionId
        && TransactionIdPrecedes(varsup::get_oldest_commit_ts_xid::call(), oldestXact)
    {
        varsup::set_oldest_commit_ts_xid::call(oldestXact);
    }
    commit_ts_lock_release()?;
    Ok(())
}

// ===========================================================================
// WAL record emission
// ===========================================================================

/// `WriteZeroPageXlogRec` — write a ZEROPAGE xlog record.
fn WriteZeroPageXlogRec(pageno: i64) -> PgResult<()> {
    // XLogBeginInsert(); XLogRegisterData(&pageno, sizeof(pageno));
    // XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE);
    let pageno_bytes = pageno.to_ne_bytes();
    xloginsert_seams::xlog_insert::call(
        RM_COMMIT_TS_ID,
        COMMIT_TS_ZEROPAGE,
        0,
        &[&pageno_bytes],
    )?;
    Ok(())
}

/// `WriteTruncateXlogRec` — write a TRUNCATE xlog record. The on-disk record is
/// `SizeOfCommitTsTruncate` (12) bytes: `pageno`(8) + `oldestXid`(4), no
/// trailing struct padding.
fn WriteTruncateXlogRec(pageno: i64, oldestXid: TransactionId) -> PgResult<()> {
    let mut xlrec = [0u8; 12];
    xlrec[0..8].copy_from_slice(&pageno.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&oldestXid.to_ne_bytes());
    xloginsert_seams::xlog_insert::call(
        RM_COMMIT_TS_ID,
        COMMIT_TS_TRUNCATE,
        0,
        &[&xlrec],
    )?;
    Ok(())
}

// ===========================================================================
// CommitTS resource manager's routines
// ===========================================================================

/// `commit_ts_redo` — CommitTS resource manager's WAL redo routine. `info` is
/// `XLogRecGetInfo(record) & ~XLR_INFO_MASK`; `data` is `XLogRecGetData`.
pub fn commit_ts_redo(state: &mut CommitTsState, info: u8, data: &[u8]) -> PgResult<()> {
    if info == COMMIT_TS_ZEROPAGE {
        let pageno = i64::from_ne_bytes(data[..8].try_into().unwrap());

        LWLockAcquire(
            SimpleLruGetBankLock(&state.CommitTsCtl, pageno),
            LW_EXCLUSIVE,
            my_proc_number(),
        )?;

        let slotno = ZeroCommitTsPage(state, pageno, false)?;
        SimpleLruWritePage(&mut state.CommitTsCtl, slotno)?;
        debug_assert!(!state.CommitTsCtl.shared.page_dirty[slotno]);

        LWLockRelease(SimpleLruGetBankLock(&state.CommitTsCtl, pageno))?;
    } else if info == COMMIT_TS_TRUNCATE {
        // xl_commit_ts_truncate { int64 pageno; TransactionId oldestXid; }
        let trunc_pageno = i64::from_ne_bytes(data[..8].try_into().unwrap());
        let trunc_oldest_xid = TransactionId::from_ne_bytes(data[8..12].try_into().unwrap());

        AdvanceOldestCommitTsXid(trunc_oldest_xid)?;

        // During XLOG replay, latest_page_number isn't set up yet; insert a
        // suitable value to bypass the sanity test in SimpleLruTruncate.
        state
            .CommitTsCtl
            .shared
            .latest_page_number
            .write(trunc_pageno as u64);

        SimpleLruTruncate(&mut state.CommitTsCtl, trunc_pageno)?;
    } else {
        return Err(PgError::new(
            PANIC,
            format!("commit_ts_redo: unknown op code {info}"),
        ));
    }
    Ok(())
}

/// `committssyncfiletag` — entrypoint for sync.c to sync commit_ts files.
/// Returns the fsync `(result, path, errno)` triple.
pub fn committssyncfiletag(
    state: &CommitTsState,
    ftag: &::types_storage::sync::FileTag,
) -> PgResult<::types_storage::sync::FileTagOpResult> {
    let (result, path) = SlruSyncFileTag(&state.CommitTsCtl, ftag)?;
    let errno = current_errno();
    Ok(::types_storage::sync::FileTagOpResult {
        result,
        path,
        errno,
    })
}

// ===========================================================================
// Seam install — process-global shared state + inward seam adapters
// ===========================================================================

/// The process-global commit-ts shared state, built by [`commit_ts_shmem_init`].
/// C keeps the SLRU control (`CommitTsCtlData`) file-static and the cached
/// `commitTsShared` in a `ShmemInitStruct` block; both are modeled here as a
/// single process-global [`CommitTsState`] (the `CommitTsLock` LWLock still
/// serializes the shared fields).
static COMMIT_TS_STATE: std::sync::OnceLock<std::sync::Mutex<CommitTsState>> =
    std::sync::OnceLock::new();

/// Run `f` with `&mut CommitTsState` over the process-global shared state.
/// Panics if [`commit_ts_shmem_init`] has not run (the C invariant: the shmem
/// struct exists before any commit-ts path touches it).
pub fn with_commit_ts_state<R>(f: impl FnOnce(&mut CommitTsState) -> R) -> R {
    let mtx = COMMIT_TS_STATE
        .get()
        .expect("CommitTsState accessed before CommitTsShmemInit");
    let mut guard = mtx.lock().expect("CommitTsLock poisoned");
    f(&mut guard)
}

/// `CommitTsShmemInit()` (inward seam) — build the process-global commit-ts
/// state. Idempotent per process (the `OnceLock` matches the C invariant that
/// `CommitTsShmemInit` runs once at shmem creation).
fn commit_ts_shmem_init_seam() -> PgResult<()> {
    let state = CommitTsShmemInit()?;
    let _ = COMMIT_TS_STATE.set(std::sync::Mutex::new(state));
    Ok(())
}

/// Install this crate's inward seams
/// (`backend-access-transam-commit-ts-seams`).
pub fn init_seams() {
    use commit_ts_seams as seams;

    // `BootStrapCommitTs()` (commit_ts.c) — called once by `BootStrapXLOG`
    // (xlog.c) at initdb. A no-op in current PG (segments created lazily on
    // ActivateCommitTs), but it must not panic on the bootstrap path.
    transam_xlog_seams::boot_strap_commit_ts::set(|| {
        with_commit_ts_state(BootStrapCommitTs);
        Ok(())
    });

    // Install the GUC check_hook for commit_timestamp_buffers (the table in
    // guc-tables references this slot by C symbol). The C
    // `check_commit_ts_buffers` records its detail via `GUC_check_errdetail`
    // and returns false on a bad value.
    ::guc_tables::hooks::check_commit_ts_buffers.install(
        |newval, _extra, _source| {
            let (ok, detail) = check_commit_ts_buffers(*newval);
            if let Some(detail) = detail {
                guc_seams::guc_check_errdetail::call(detail);
            }
            Ok(ok)
        },
    );

    // GUC variable accessors (commit_ts.c owns these `conf->variable`
    // integers/bool; there is no separate global beyond the GUC value).
    use ::guc_tables::{vars, GucVarAccessors};
    vars::commit_timestamp_buffers.install(GucVarAccessors {
        get: || COMMIT_TIMESTAMP_BUFFERS.with(core::cell::Cell::get),
        set: |v| COMMIT_TIMESTAMP_BUFFERS.with(|c| c.set(v)),
    });
    vars::track_commit_timestamp.install(GucVarAccessors {
        get: || TRACK_COMMIT_TIMESTAMP.with(core::cell::Cell::get),
        set: |v| TRACK_COMMIT_TIMESTAMP.with(|c| c.set(v)),
    });

    seams::commit_ts_shmem_size::set(|| Ok(CommitTsShmemSize()));
    seams::commit_ts_shmem_init::set(commit_ts_shmem_init_seam);

    seams::transaction_tree_set_commit_ts_data::set(|xid, subxids, timestamp, node_id| {
        with_commit_ts_state(|state| {
            TransactionTreeSetCommitTsData(state, xid, subxids, timestamp, node_id)
        })
    });

    seams::transaction_id_get_commit_ts_data::set(|xid| {
        with_commit_ts_state(|state| {
            let found = TransactionIdGetCommitTsData(state, xid)?;
            Ok(match found {
                Some((ts, nodeid)) => (true, ts, nodeid),
                None => (false, 0, InvalidRepOriginId),
            })
        })
    });

    seams::committssyncfiletag::set(|ftag| {
        with_commit_ts_state(|state| committssyncfiletag(state, &ftag))
    });

    seams::extend_commit_ts::set(|newest_xact| {
        with_commit_ts_state(|state| ExtendCommitTs(state, newest_xact))
    });

    seams::check_point_commit_ts::set(|| with_commit_ts_state(CheckPointCommitTs));

    seams::commit_ts_redo::set(|record| {
        let decoded = record
            .record
            .as_ref()
            .expect("commit_ts_redo called without a decoded record");
        // Backup blocks are not used in commit_ts records.
        debug_assert!(decoded.max_block_id() < 0);
        let info = decoded.info() & !wal::XLR_INFO_MASK;
        let data = decoded.main_data();
        // The decoded record's data slice is borrowed from `record`; copy the
        // bytes we need before reaching into the process-global state (which is
        // a distinct borrow).
        let owned: Vec<u8> = data.to_vec();
        with_commit_ts_state(|state| commit_ts_redo(state, info, &owned))
    });

    // WAL-startup entry points called once by `StartupXLOG` (xlog.c) on the
    // clean DB_SHUTDOWNED / end-of-recovery path. They wrap the owner-private
    // `CommitTsState` through the same ambient accessor the other seams use.
    seams::startup_commit_ts::set(|| with_commit_ts_state(StartupCommitTs));
    seams::complete_commit_ts_initialization::set(|| {
        with_commit_ts_state(CompleteCommitTsInitialization)
    });
    seams::set_commit_ts_limit::set(SetCommitTsLimit);

    // `CommitTsParameterChange` (commit_ts.c) — invoked by `xlog_redo`'s
    // `XLOG_PARAMETER_CHANGE` arm during recovery.
    seams::commit_ts_parameter_change::set(|newvalue, oldvalue| {
        with_commit_ts_state(|state| CommitTsParameterChange(state, newvalue, oldvalue))
    });

    // vacuum's `vac_truncate_clog` commitTS truncation entry points.
    seams::truncate_commit_ts::set(|oldest| {
        with_commit_ts_state(|state| TruncateCommitTs(state, oldest))
    });
    seams::advance_oldest_commit_ts_xid::set(AdvanceOldestCommitTsXid);

    // Register the SQL-callable commit-timestamp fmgr builtins.
    fmgr_builtins::register_backend_access_transam_commit_ts_builtins();
}
