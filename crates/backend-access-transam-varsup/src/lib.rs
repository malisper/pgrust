#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `src/backend/access/transam/varsup.c` — the transaction-id and OID
//! allocator.
//!
//! Owns the cluster-wide `TransamVariables` cache and the XID/OID assignment
//! algorithms: `GetNewTransactionId` / `GetNewObjectId`, the wraparound
//! vac/warn/stop-limit computation (`SetTransactionIdLimit`,
//! `ForceTransactionIdLimitUpdate`), `ReadNextFullTransactionId`,
//! `AdvanceNextFullTransactionIdPastXid`, `AdvanceOldestClogXid`, the
//! OID-prefetch accounting, and the `transam.h` XID/FullXID arithmetic the
//! limit math needs.
//!
//! `TransamVariables` is shared-memory state in C (one cluster-wide singleton,
//! carved by `VarsupShmemInit`). Here it is a process-shared, synchronized
//! singleton ([`TRANSAM_VARIABLES`]); the genuine cross-backend serialization
//! is the real LWLock (`XidGenLock` / `OidGenLock` / `XactTruncationLock`),
//! acquired through the ported lwlock crate as an RAII guard. The inner
//! `Mutex` only makes the shared struct safe to touch from Rust while the
//! LWLock is held.
//!
//! `ERROR`-level conditions (parallel mode, recovery, OID-counter overrun,
//! wraparound stop limit) are returned as `Err(PgError)`. WARNING/DEBUG1
//! wrap-limit reports go through `backend-utils-error` directly. Everything
//! varsup reaches outside itself — SLRU page extension (clog/commit_ts/
//! subtrans), the next-OID WAL record, proc-array publication, postmaster
//! signalling, catalog name/existence lookups, the process-mode predicates —
//! goes through the owners' `-seams` crates (panic until each owner lands).

use std::sync::Mutex;

use backend_access_transam_clog_seams as clog_seams;
use backend_access_transam_commit_ts_seams as commit_ts_seams;
use backend_access_transam_subtrans_seams as subtrans_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_access_transam_xlog_seams as xlog_seams;
use backend_commands_dbcommands_seams as dbcommands_seams;
use backend_storage_ipc_pmsignal_seams as pmsignal_seams;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as init_small_seams;

use backend_storage_lmgr_lwlock::{LWLockAcquireMain, MainLWLockGuard};
use backend_utils_error::{ereport, PgError, PgResult};
use types_error::ErrorLocation;

use mcx::{Mcx, MemoryContext};

use types_core::catalog::{FirstGenbkiObjectId, FirstNormalObjectId, FirstUnpinnedObjectId};
use types_core::init::BackendType;
use types_core::primitive::{Oid, Size, TransactionId};
use types_core::xact::{
    BootstrapTransactionId, FirstNormalFullTransactionId, FirstNormalTransactionId,
    FullTransactionId, InvalidTransactionId, MaxTransactionId, TransamVariablesData,
};
use types_error::{DEBUG1, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, WARNING};
use types_storage::{LWLockMode, OID_GEN_LOCK, XACT_TRUNCATION_LOCK, XID_GEN_LOCK};

/// Number of OIDs to prefetch (preallocate) per XLOG write (`VAR_OID_PREFETCH`).
const VAR_OID_PREFETCH: u32 = 8192;

/// `TransamVariables` (varsup.c:44) — the cluster-wide shared singleton, here
/// a process-shared `Mutex`. The LWLocks serialize cross-backend; the `Mutex`
/// makes the struct safe to touch from Rust while the LWLock is held.
static TRANSAM_VARIABLES: Mutex<TransamVariablesData> = Mutex::new(TransamVariablesData {
    nextOid: 0,
    oidCount: 0,
    nextXid: FullTransactionId { value: 0 },
    oldestXid: 0,
    xidVacLimit: 0,
    xidWarnLimit: 0,
    xidStopLimit: 0,
    xidWrapLimit: 0,
    oldestXidDB: 0,
    oldestCommitTsXid: 0,
    newestCommitTsXid: 0,
    latestCompletedXid: FullTransactionId { value: 0 },
    xactCompletionCount: 0,
    oldestClogXid: 0,
});

fn here(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("varsup.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// FullTransactionId / TransactionId helpers (access/transam.h inlines+macros).
// ---------------------------------------------------------------------------

/// `XidFromFullTransactionId(x)` — `((uint32) (x).value)`.
#[inline]
pub fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId {
    x.value as TransactionId
}

/// `EpochFromFullTransactionId(x)` — `((uint32) ((x).value >> 32))`.
#[inline]
pub fn EpochFromFullTransactionId(x: FullTransactionId) -> u32 {
    (x.value >> 32) as u32
}

/// `FullTransactionIdFromEpochAndXid(epoch, xid)`.
#[inline]
pub fn FullTransactionIdFromEpochAndXid(epoch: u32, xid: TransactionId) -> FullTransactionId {
    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

/// `FullTransactionIdPrecedes(a, b)` — `((a).value < (b).value)`.
#[inline]
fn FullTransactionIdPrecedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

/// `TransactionIdAdvance(dest)` (`access/transam.h`): advance a 32-bit
/// `TransactionId`, skipping the special low values on wraparound.
#[inline]
pub fn TransactionIdAdvance(dest: &mut TransactionId) {
    *dest = dest.wrapping_add(1);
    if *dest < FirstNormalTransactionId {
        *dest = FirstNormalTransactionId;
    }
}

/// `FullTransactionIdAdvance(dest)` (`access/transam.h` inline): advance a
/// `FullTransactionId`, stepping over XIDs that would appear special only when
/// viewed as 32-bit XIDs.
#[inline]
pub fn FullTransactionIdAdvance(dest: &mut FullTransactionId) {
    dest.value = dest.value.wrapping_add(1);

    if FullTransactionIdPrecedes(*dest, FirstNormalFullTransactionId) {
        return;
    }

    while XidFromFullTransactionId(*dest) < FirstNormalTransactionId {
        dest.value = dest.value.wrapping_add(1);
    }
}

// ---------------------------------------------------------------------------
// TransactionId comparison helpers (transam.h wrapping comparisons).
// ---------------------------------------------------------------------------

/// `TransactionIdIsValid(xid)` — `(xid) != InvalidTransactionId`.
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` — `(xid) >= FirstNormalTransactionId`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)`.
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `TransactionIdPrecedesOrEquals(id1, id2)`.
#[inline]
fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff <= 0
}

/// `TransactionIdFollowsOrEquals(id1, id2)`.
#[inline]
fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff >= 0
}

/// `AmStartupProcess()` (miscadmin.h) — `MyBackendType == B_STARTUP`.
#[inline]
fn am_startup_process() -> bool {
    init_small_seams::my_backend_type::call() == BackendType::Startup
}

/// `autovacuum_freeze_max_age` (autovacuum.c GUC) — read through the GUC-tables
/// accessor slot (`PGC_POSTMASTER`, so it is stable for the process lifetime).
#[inline]
fn autovacuum_freeze_max_age() -> i32 {
    backend_utils_misc_guc_tables::vars::autovacuum_freeze_max_age.read()
}

// ---------------------------------------------------------------------------
// LWLock acquisition helpers — the gen-locks / truncation lock as RAII guards.
// ---------------------------------------------------------------------------

#[inline]
fn acquire(offset: usize, exclusive: bool) -> PgResult<MainLWLockGuard> {
    let mode = if exclusive {
        LWLockMode::LW_EXCLUSIVE
    } else {
        LWLockMode::LW_SHARED
    };
    // `MyProcNumber` is the caller's backend identity (globals.c), read off the
    // init-small globals owner — the same explicit-parameter path lwlock's own
    // callers use.
    LWLockAcquireMain(offset, mode, init_small_seams::my_proc_number::call())
}

// ---------------------------------------------------------------------------
// Initialization of shared memory for TransamVariables. (varsup.c 47-72)
// ---------------------------------------------------------------------------

/// `VarsupShmemSize` (varsup.c 50-54) — the byte size C reserves for the
/// `TransamVariablesData` struct via `ShmemInitStruct`.
pub fn VarsupShmemSize() -> Size {
    core::mem::size_of::<TransamVariablesData>() as Size
}

/// `VarsupShmemInit` (varsup.c 56-72). C carves the cluster-wide
/// `TransamVariablesData` singleton from the shared segment and `memset`s it to
/// zero when freshly created (`!IsUnderPostmaster`); an attaching backend just
/// finds it. Here the singleton is [`TRANSAM_VARIABLES`] (a const-zeroed
/// `Mutex`); the postmaster zeroes it explicitly, an attaching backend leaves
/// it as found.
pub fn VarsupShmemInit() {
    if !init_small_seams::is_under_postmaster::call() {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        *tv = TransamVariablesData::default();
    }
}

// ---------------------------------------------------------------------------
// GetNewTransactionId (varsup.c 86-292)
// ---------------------------------------------------------------------------

/// `GetNewTransactionId(isSubXact)` (varsup.c 86-292). Allocate the next
/// `FullTransactionId` for a new (sub)transaction; the new XID is also stored
/// into `MyProc->xid` / `ProcGlobal->xids[]` (through the proc seam) before
/// returning.
///
/// `mcx` is the caller's current context, the home for the transient database
/// name fetched for a wraparound warning (C's `get_database_name` pstrdup).
pub fn GetNewTransactionId(mcx: Mcx<'_>, isSubXact: bool) -> PgResult<FullTransactionId> {
    // Workers synchronize transaction state at the beginning of each parallel
    // operation, so we can't account for new XIDs after that point.
    if xact_seams::is_in_parallel_mode::call() {
        return Err(PgError::new(
            ERROR,
            "cannot assign TransactionIds during a parallel operation",
        ));
    }

    // During bootstrap initialization, we return the special bootstrap
    // transaction id.
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        debug_assert!(!isSubXact);
        proc_seams::store_top_xid_in_proc::call(BootstrapTransactionId);
        return Ok(FullTransactionIdFromEpochAndXid(0, BootstrapTransactionId));
    }

    // safety check, we should never get this far in a HS standby
    if xlog_seams::recovery_in_progress::call() {
        return Err(PgError::new(
            ERROR,
            "cannot assign TransactionIds during recovery",
        ));
    }

    let mut guard = acquire(XID_GEN_LOCK, true)?;

    let mut full_xid = TRANSAM_VARIABLES.lock().unwrap().nextXid;
    let mut xid = XidFromFullTransactionId(full_xid);

    // Check to see if it's safe to assign another XID. This protects against
    // catastrophic data loss due to XID wraparound. Note that this coding also
    // appears in GetNewMultiXactId.
    let xid_vac_limit = TRANSAM_VARIABLES.lock().unwrap().xidVacLimit;
    if TransactionIdFollowsOrEquals(xid, xid_vac_limit) {
        // For safety's sake, we release XidGenLock while sending signals,
        // warnings, etc., to avoid any possibility of deadlock while doing
        // get_database_name(). First, copy all the shared values we'll need.
        let (xidStopLimit, xidWarnLimit, xidWrapLimit, oldest_datoid) = {
            let tv = TRANSAM_VARIABLES.lock().unwrap();
            (
                tv.xidStopLimit,
                tv.xidWarnLimit,
                tv.xidWrapLimit,
                tv.oldestXidDB,
            )
        };

        guard.release()?;

        // To avoid swamping the postmaster with signals, we issue the autovac
        // request only once per 64K transaction starts.
        if init_small_seams::is_under_postmaster::call() && (xid % 65536) == 0 {
            pmsignal_seams::send_postmaster_signal_start_autovac::call();
        }

        if init_small_seams::is_under_postmaster::call()
            && TransactionIdFollowsOrEquals(xid, xidStopLimit)
        {
            let oldest_datname = dbcommands_seams::get_database_name::call(mcx, oldest_datoid)?;

            // complain even if that DB has disappeared
            return match oldest_datname {
                Some(name) => Err(PgError::new(
                    ERROR,
                    format!("database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database \"{}\"", name.as_str()),
                )
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_hint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )),
                None => Err(PgError::new(
                    ERROR,
                    format!("database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database with OID {oldest_datoid}"),
                )
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_hint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )),
            };
        } else if TransactionIdFollowsOrEquals(xid, xidWarnLimit) {
            let oldest_datname = dbcommands_seams::get_database_name::call(mcx, oldest_datoid)?;
            let remaining = xidWrapLimit.wrapping_sub(xid);

            // complain even if that DB has disappeared
            match oldest_datname {
                Some(name) => ereport(WARNING)
                    .errmsg(format!(
                        "database \"{}\" must be vacuumed within {remaining} transactions",
                        name.as_str()
                    ))
                    .errhint("To avoid transaction ID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.")
                    .finish(here("GetNewTransactionId"))?,
                None => ereport(WARNING)
                    .errmsg(format!(
                        "database with OID {oldest_datoid} must be vacuumed within {remaining} transactions"
                    ))
                    .errhint("To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.")
                    .finish(here("GetNewTransactionId"))?,
            }
        }

        // Re-acquire lock and start over
        guard = acquire(XID_GEN_LOCK, true)?;
        full_xid = TRANSAM_VARIABLES.lock().unwrap().nextXid;
        xid = XidFromFullTransactionId(full_xid);
    }

    // If we are allocating the first XID of a new page of the commit log, zero
    // out that commit-log page before returning. We must do this while holding
    // XidGenLock. Extend pg_subtrans and pg_commit_ts too.
    clog_seams::extend_clog::call(xid)?;
    commit_ts_seams::extend_commit_ts::call(xid)?;
    subtrans_seams::extend_subtrans::call(xid)?;

    // Now advance the nextXid counter. This must not happen until after we have
    // successfully completed ExtendCLOG() --- if that routine fails, we want
    // the next incoming transaction to try it again.
    {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        FullTransactionIdAdvance(&mut tv.nextXid);
    }

    // We must store the new XID into the shared ProcArray before releasing
    // XidGenLock. If there's no room to fit a subtransaction XID into PGPROC,
    // the proc-publication seam sets the cache-overflowed flag instead.
    if !isSubXact {
        // LWLockRelease acts as barrier
        proc_seams::store_top_xid_in_proc::call(xid);
    } else {
        proc_seams::store_subxid_in_proc::call(xid);
    }

    guard.release()?;

    Ok(full_xid)
}

// ---------------------------------------------------------------------------
// ReadNextFullTransactionId (varsup.c 294-307)
// ---------------------------------------------------------------------------

/// `ReadNextFullTransactionId()` (varsup.c 297-307). Read nextXid but don't
/// allocate it.
pub fn ReadNextFullTransactionId() -> FullTransactionId {
    // Holds XidGenLock in SHARED mode for the read. The only failure surface is
    // the LWLock subsystem's "too many LWLocks taken" abort (never in normal
    // operation); the C signature is infallible, so we surface it as a panic
    // (matching that should-never-happen abort) rather than widen the inward
    // seam every consumer treats as infallible.
    let guard = acquire(XID_GEN_LOCK, false).expect("XidGenLock acquire");
    let fullXid = TRANSAM_VARIABLES.lock().unwrap().nextXid;
    guard.release().expect("XidGenLock release");
    fullXid
}

// ---------------------------------------------------------------------------
// AdvanceNextFullTransactionIdPastXid (varsup.c 309-353)
// ---------------------------------------------------------------------------

/// `AdvanceNextFullTransactionIdPastXid(xid)` (varsup.c 313-353). Advance
/// nextXid to the value after `xid`, inferring the epoch. Only called during
/// recovery or two-phase start-up.
pub fn AdvanceNextFullTransactionIdPastXid(xid: TransactionId) -> PgResult<()> {
    // It is safe to read nextXid without a lock, because this is only called
    // from the startup process or single-process mode.
    debug_assert!(am_startup_process() || !init_small_seams::is_under_postmaster::call());

    // Fast return if this isn't an xid high enough to move the needle.
    let cur_next = TRANSAM_VARIABLES.lock().unwrap().nextXid;
    let next_xid = XidFromFullTransactionId(cur_next);
    if !TransactionIdFollowsOrEquals(xid, next_xid) {
        return Ok(());
    }

    // Compute the FullTransactionId that comes after the given xid, preserving
    // the existing epoch but detecting when we've wrapped into a new epoch.
    let mut xid = xid;
    TransactionIdAdvance(&mut xid);
    let mut epoch = EpochFromFullTransactionId(cur_next);
    if xid < next_xid {
        epoch = epoch.wrapping_add(1);
    }
    let newNextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);

    // We still need to take a lock to modify the value when there are
    // concurrent readers.
    let guard = acquire(XID_GEN_LOCK, true)?;
    TRANSAM_VARIABLES.lock().unwrap().nextXid = newNextFullXid;
    guard.release()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// AdvanceOldestClogXid (varsup.c 355-374)
// ---------------------------------------------------------------------------

/// `AdvanceOldestClogXid(oldest_datfrozenxid)` (varsup.c 364-374). Advance the
/// cluster-wide value for the oldest valid clog entry.
pub fn AdvanceOldestClogXid(oldest_datfrozenxid: TransactionId) -> PgResult<()> {
    let guard = acquire(XACT_TRUNCATION_LOCK, true)?;
    {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        if TransactionIdPrecedes(tv.oldestClogXid, oldest_datfrozenxid) {
            tv.oldestClogXid = oldest_datfrozenxid;
        }
    }
    guard.release()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// SetTransactionIdLimit (varsup.c 376-513)
// ---------------------------------------------------------------------------

/// `SetTransactionIdLimit(oldest_datfrozenxid, oldest_datoid)` (varsup.c
/// 381-513). Determine the last safe XID to allocate from the oldest
/// datfrozenxid, and store the recomputed wraparound limits.
pub fn SetTransactionIdLimit(
    mcx: Mcx<'_>,
    oldest_datfrozenxid: TransactionId,
    oldest_datoid: Oid,
) -> PgResult<()> {
    debug_assert!(TransactionIdIsNormal(oldest_datfrozenxid));

    // The place where we actually get into deep trouble is halfway around from
    // the oldest potentially-existing XID.
    let mut xidWrapLimit = oldest_datfrozenxid.wrapping_add(MaxTransactionId >> 1);
    if xidWrapLimit < FirstNormalTransactionId {
        xidWrapLimit = xidWrapLimit.wrapping_add(FirstNormalTransactionId);
    }

    // We'll refuse to continue assigning XIDs in interactive mode once we get
    // within 3M transactions of data loss.
    let mut xidStopLimit = xidWrapLimit.wrapping_sub(3000000);
    if xidStopLimit < FirstNormalTransactionId {
        xidStopLimit = xidStopLimit.wrapping_sub(FirstNormalTransactionId);
    }

    // We'll start complaining loudly when we get within 40M transactions of
    // data loss.
    let mut xidWarnLimit = xidWrapLimit.wrapping_sub(40000000);
    if xidWarnLimit < FirstNormalTransactionId {
        xidWarnLimit = xidWarnLimit.wrapping_sub(FirstNormalTransactionId);
    }

    // We'll start trying to force autovacuums when oldest_datfrozenxid gets to
    // be more than autovacuum_freeze_max_age transactions old.
    let mut xidVacLimit = oldest_datfrozenxid.wrapping_add(autovacuum_freeze_max_age() as u32);
    if xidVacLimit < FirstNormalTransactionId {
        xidVacLimit = xidVacLimit.wrapping_add(FirstNormalTransactionId);
    }

    // Grab lock for just long enough to set the new limit values.
    let guard = acquire(XID_GEN_LOCK, true)?;
    let curXid = {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        tv.oldestXid = oldest_datfrozenxid;
        tv.xidVacLimit = xidVacLimit;
        tv.xidWarnLimit = xidWarnLimit;
        tv.xidStopLimit = xidStopLimit;
        tv.xidWrapLimit = xidWrapLimit;
        tv.oldestXidDB = oldest_datoid;
        XidFromFullTransactionId(tv.nextXid)
    };
    guard.release()?;

    // Log the info.
    ereport(DEBUG1)
        .errmsg_internal(format!(
            "transaction ID wrap limit is {xidWrapLimit}, limited by database with OID {oldest_datoid}"
        ))
        .finish(here("SetTransactionIdLimit"))?;

    // If past the autovacuum force point, immediately signal an autovac
    // request.
    if TransactionIdFollowsOrEquals(curXid, xidVacLimit)
        && init_small_seams::is_under_postmaster::call()
        && !xlog_seams::in_recovery::call()
    {
        pmsignal_seams::send_postmaster_signal_start_autovac::call();
    }

    // Give an immediate warning if past the wrap warn point.
    if TransactionIdFollowsOrEquals(curXid, xidWarnLimit) && !xlog_seams::in_recovery::call() {
        // We can be called when not inside a transaction (e.g. during
        // StartupXLOG()); then we cannot do database access, so we just report
        // the oldest DB's OID. get_database_name may also return NULL.
        let oldest_datname = if xact_seams::is_transaction_state::call() {
            dbcommands_seams::get_database_name::call(mcx, oldest_datoid)?
        } else {
            None
        };

        let remaining = xidWrapLimit.wrapping_sub(curXid);
        match oldest_datname {
            Some(name) => ereport(WARNING)
                .errmsg(format!(
                    "database \"{}\" must be vacuumed within {remaining} transactions",
                    name.as_str()
                ))
                .errhint("To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.")
                .finish(here("SetTransactionIdLimit"))?,
            None => ereport(WARNING)
                .errmsg(format!(
                    "database with OID {oldest_datoid} must be vacuumed within {remaining} transactions"
                ))
                .errhint("To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.")
                .finish(here("SetTransactionIdLimit"))?,
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ForceTransactionIdLimitUpdate (varsup.c 516-551)
// ---------------------------------------------------------------------------

/// `ForceTransactionIdLimitUpdate()` (varsup.c 526-551). Does the XID
/// wrap-limit data need updating?
pub fn ForceTransactionIdLimitUpdate() -> PgResult<bool> {
    // Locking is probably not really necessary, but let's be careful.
    let guard = acquire(XID_GEN_LOCK, false)?;
    let (nextXid, xidVacLimit, oldestXid, oldestXidDB) = {
        let tv = TRANSAM_VARIABLES.lock().unwrap();
        (
            XidFromFullTransactionId(tv.nextXid),
            tv.xidVacLimit,
            tv.oldestXid,
            tv.oldestXidDB,
        )
    };
    guard.release()?;

    if !TransactionIdIsNormal(oldestXid) {
        return Ok(true); // shouldn't happen, but just in case
    }
    if !TransactionIdIsValid(xidVacLimit) {
        return Ok(true); // this shouldn't happen anymore either
    }
    if TransactionIdFollowsOrEquals(nextXid, xidVacLimit) {
        return Ok(true); // past xidVacLimit, don't delay updating
    }
    // C: `!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(oldestXidDB))`.
    // The canonical syscache seam returns the row's datdba on a hit and `None`
    // on a miss, so existence is `is_some()`.
    if syscache_seams::database_datdba::call(oldestXidDB)?.is_none() {
        return Ok(true); // could happen, per comments above
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// GetNewObjectId (varsup.c 554-624)
// ---------------------------------------------------------------------------

/// `GetNewObjectId()` (varsup.c 564-624). Allocate a new OID from the
/// cluster-wide counter. Only `GetNewOidWithIndex()` / `GetNewRelFileNumber()`
/// should call this directly.
pub fn GetNewObjectId() -> PgResult<Oid> {
    // safety check, we should never get this far in a HS standby
    if xlog_seams::recovery_in_progress::call() {
        return Err(PgError::new(ERROR, "cannot assign OIDs during recovery"));
    }

    let guard = acquire(OID_GEN_LOCK, true)?;

    let result = {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();

        // Check for wraparound of the OID counter. We *must* not return 0
        // (InvalidOid), and in normal operation we mustn't return anything
        // below FirstNormalObjectId. During initdb we start the OID generator
        // at FirstGenbkiObjectId, so we only wrap if before that point when in
        // bootstrap or standalone mode.
        if tv.nextOid < FirstNormalObjectId {
            if init_small_seams::is_postmaster_environment::call() {
                // wraparound, or first post-initdb assignment, in normal mode
                tv.nextOid = FirstNormalObjectId;
                tv.oidCount = 0;
            } else {
                // we may be bootstrapping, so don't enforce the full range
                if tv.nextOid < FirstGenbkiObjectId {
                    // wraparound in standalone mode (unlikely but possible)
                    tv.nextOid = FirstNormalObjectId;
                    tv.oidCount = 0;
                }
            }
        }

        // If we run out of logged-for-use oids then we must log more.
        if tv.oidCount == 0 {
            let next_to_log = tv.nextOid.wrapping_add(VAR_OID_PREFETCH);
            // Release the data mutex across the WAL insert (the LWLock still
            // serializes); reacquire to commit the prefetch accounting.
            drop(tv);
            xlog_seams::xlog_put_next_oid::call(next_to_log)?;
            tv = TRANSAM_VARIABLES.lock().unwrap();
            tv.oidCount = VAR_OID_PREFETCH;
        }

        let result = tv.nextOid;
        tv.nextOid = tv.nextOid.wrapping_add(1);
        tv.oidCount = tv.oidCount.wrapping_sub(1);
        result
    };

    guard.release()?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// SetNextObjectId (varsup.c 626-650)
// ---------------------------------------------------------------------------

/// `SetNextObjectId(nextOid)` (varsup.c 632-650). May only be called during
/// initdb; advances the OID counter to the specified value.
fn SetNextObjectId(nextOid: Oid) -> PgResult<()> {
    // Safety check, this is only allowable during initdb.
    if init_small_seams::is_postmaster_environment::call() {
        return Err(PgError::new(ERROR, "cannot advance OID counter anymore"));
    }

    // Taking the lock is, therefore, just pro forma; but do it anyway.
    let guard = acquire(OID_GEN_LOCK, true)?;

    {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        if tv.nextOid > nextOid {
            let cur = tv.nextOid;
            drop(tv);
            guard.release()?;
            return Err(PgError::new(
                ERROR,
                format!("too late to advance OID counter to {nextOid}, it is now {cur}"),
            ));
        }

        tv.nextOid = nextOid;
        tv.oidCount = 0;
    }

    guard.release()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// XLOG_NEXTOID redo arm (xlog.c) — the live counter update during WAL replay.
// ---------------------------------------------------------------------------

/// The `XLOG_NEXTOID` redo of `xlog_redo` (xlog.c): install the logged
/// next-OID hint into the live counter during WAL replay.
///
/// ```c
/// LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
/// TransamVariables->nextOid = nextOid;
/// TransamVariables->oidCount = 0;
/// LWLockRelease(OidGenLock);
/// ```
pub fn XLogRedoNextOid(next_oid: Oid) -> PgResult<()> {
    let guard = acquire(OID_GEN_LOCK, true)?;
    {
        let mut tv = TRANSAM_VARIABLES.lock().unwrap();
        tv.nextOid = next_oid;
        tv.oidCount = 0;
    }
    guard.release()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// StopGeneratingPinnedObjectIds (varsup.c 652-665)
// ---------------------------------------------------------------------------

/// `StopGeneratingPinnedObjectIds()` (varsup.c 661-665). Called once during
/// initdb to force the OID counter up to `FirstUnpinnedObjectId`.
pub fn StopGeneratingPinnedObjectIds() -> PgResult<()> {
    SetNextObjectId(FirstUnpinnedObjectId)
}

// ---------------------------------------------------------------------------
// AssertTransactionIdInAllowableRange (varsup.c 668-715, USE_ASSERT_CHECKING)
// ---------------------------------------------------------------------------

/// `AssertTransactionIdInAllowableRange(xid)` (varsup.c 682-714). Assert that
/// `xid` is in `[oldestXid, nextXid]`. Effective only in debug builds, matching
/// the C macro which expands to `((void) true)` in non-assert builds.
pub fn AssertTransactionIdInAllowableRange(xid: TransactionId) {
    if cfg!(debug_assertions) {
        debug_assert!(TransactionIdIsValid(xid));

        // we may see bootstrap / frozen
        if !TransactionIdIsNormal(xid) {
            return;
        }

        // C cannot acquire XidGenLock here (it may already be held) and relies
        // on 32-bit atomic reads; the Mutex read of the owned struct is already
        // correctly ordered.
        let (oldest_xid, next_xid) = {
            let tv = TRANSAM_VARIABLES.lock().unwrap();
            (tv.oldestXid, XidFromFullTransactionId(tv.nextXid))
        };

        debug_assert!(
            TransactionIdFollowsOrEquals(xid, oldest_xid)
                || TransactionIdPrecedesOrEquals(xid, next_xid)
        );
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam this crate owns (`backend-access-transam-varsup-seams`).
pub fn init_seams() {
    use backend_access_transam_varsup_seams as seams;

    seams::read_next_full_transaction_id::set(ReadNextFullTransactionId);

    // `ReadNextTransactionId()` (access/transam.h): the xid part of nextXid.
    seams::read_next_transaction_id::set(|| XidFromFullTransactionId(ReadNextFullTransactionId()));

    // `GetNewTransactionId(isSubXact)`. The C callers are inside a live
    // transaction (XIDs are not allocated until the transaction does
    // something), so a wraparound warning's `get_database_name` may allocate.
    // The seam is declared without an `Mcx`, so the transient name lives in a
    // throwaway context created here for the duration of the call — it is only
    // read to format the warning, mirroring the transaction-lifetime pstrdup C
    // discards at xact end.
    seams::get_new_transaction_id::set(|is_subxact| {
        let cx = MemoryContext::new("GetNewTransactionId seam");
        GetNewTransactionId(cx.mcx(), is_subxact)
    });

    // `AdvanceNextFullTransactionIdPastXid(xid)` — the redo consumer treats it
    // as infallible (the only failure surface is the should-never-happen
    // LWLock-overflow abort), so surface that as a panic rather than widen the
    // seam.
    seams::advance_next_full_transaction_id_past_xid::set(|xid| {
        AdvanceNextFullTransactionIdPastXid(xid).expect("AdvanceNextFullTransactionIdPastXid")
    });

    // `AdvanceNextFullTransactionIdPastXid(xid)` — the two-phase startup
    // consumer carries the SLRU-extension failure surface on `Err`.
    seams::advance_next_full_xid_past_xid::set(AdvanceNextFullTransactionIdPastXid);

    // `AdvanceOldestClogXid(oldest_datfrozenxid)` (clog `TruncateCLOG` /
    // `clog_redo` caller) carries the shared-state-mutation channel on `Err`.
    seams::advance_oldest_clog_xid::set(AdvanceOldestClogXid);

    // `GetNewObjectId()` and `StopGeneratingPinnedObjectIds()` (catalog.c
    // callers) carry their `ereport(ERROR)` paths on `Err`.
    seams::get_new_object_id::set(GetNewObjectId);
    seams::stop_generating_pinned_object_ids::set(StopGeneratingPinnedObjectIds);

    // `TransamVariables->xactCompletionCount = 1;` — set once by procarray's
    // `ProcArrayShmemInit()` on first shared-memory init. `xactCompletionCount`
    // is a `ProcArrayLock`-protected field of the `TransamVariables` singleton
    // owned here; procarray reaches it through this owner seam.
    seams::init_xact_completion_count::set(|| {
        TRANSAM_VARIABLES.lock().unwrap().xactCompletionCount = 1;
    });

    // `VarsupShmemSize()` / `VarsupShmemInit()` (ipci.c shmem accumulator /
    // create-or-attach). Both are infallible in C/here; the seam contract
    // carries the generic shmem `add_size`/out-of-memory `ereport(ERROR)`
    // surface, so wrap in `Ok`.
    seams::varsup_shmem_size::set(|| Ok(VarsupShmemSize()));
    seams::varsup_shmem_init::set(|| {
        VarsupShmemInit();
        Ok(())
    });
}

#[cfg(test)]
mod tests;
