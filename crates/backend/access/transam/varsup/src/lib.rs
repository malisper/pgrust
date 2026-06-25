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
//! carved by `VarsupShmemInit` via `ShmemInitStruct("TransamVariables", ...)`).
//! Here it is likewise allocated in the main `MAP_SHARED` segment by
//! [`VarsupShmemInit`]; every backend's per-process pointer ([`TransamPtr`],
//! the realization of C's `TransamVariables *TransamVariables`) derefs the one
//! physical struct, so a mutation by one backend (e.g.
//! `MaintainLatestCompletedXid` advancing `latestCompletedXid` at commit) is
//! immediately visible to every other backend. The genuine cross-backend
//! serialization is the real LWLock (`XidGenLock` / `OidGenLock` /
//! `XactTruncationLock` / `ProcArrayLock` / `CommitTsLock`), acquired through
//! the ported lwlock crate as an RAII guard, exactly as in C.
//!
//! `ERROR`-level conditions (parallel mode, recovery, OID-counter overrun,
//! wraparound stop limit) are returned as `Err(PgError)`. WARNING/DEBUG1
//! wrap-limit reports go through `backend-utils-error` directly. Everything
//! varsup reaches outside itself — SLRU page extension (clog/commit_ts/
//! subtrans), the next-OID WAL record, proc-array publication, postmaster
//! signalling, catalog name/existence lookups, the process-mode predicates —
//! goes through the owners' `-seams` crates (panic until each owner lands).

use std::cell::Cell;

use clog_seams as clog_seams;
use commit_ts_seams as commit_ts_seams;
use subtrans_seams as subtrans_seams;
use transam_xact_seams as xact_seams;
use transam_xlog_seams as xlog_seams;
use dbcommands_seams as dbcommands_seams;
use pmsignal_seams as pmsignal_seams;
use lmgr_proc_seams as proc_seams;
use syscache_seams as syscache_seams;
use miscinit_seams as miscinit_seams;
use init_small_seams as init_small_seams;

use ::lwlock::{LWLockAcquireMain, MainLWLockGuard};
use ::utils_error::{ereport, PgError, PgResult};
use ::types_error::ErrorLocation;

use ::mcx::{Mcx, MemoryContext};

use ::types_core::catalog::{FirstGenbkiObjectId, FirstNormalObjectId, FirstUnpinnedObjectId};
use ::types_core::init::BackendType;
use ::types_core::primitive::{Oid, Size, TransactionId};
use ::types_core::xact::{
    BootstrapTransactionId, FirstNormalFullTransactionId, FirstNormalTransactionId,
    FullTransactionId, InvalidTransactionId, MaxTransactionId, TransamVariablesData,
};
use ::types_error::{DEBUG1, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, WARNING};
use ::types_storage::{COMMIT_TS_LOCK, LWLockMode, OID_GEN_LOCK, XACT_TRUNCATION_LOCK, XID_GEN_LOCK};

/// Number of OIDs to prefetch (preallocate) per XLOG write (`VAR_OID_PREFETCH`).
const VAR_OID_PREFETCH: u32 = 8192;

/// Per-process pointer into the shared [`TransamVariablesData`] singleton (the
/// realization of C's `TransamVariables *TransamVariables`, transam.c:32). The
/// struct it addresses lives in the main `MAP_SHARED` shmem segment (carved by
/// [`VarsupShmemInit`]), so the same physical bytes back every backend; the
/// pointer itself is per-process (correctly forked). `Deref`/`DerefMut` give
/// field access on the shared struct so the existing
/// `TransamVariables->latestCompletedXid` / `nextXid` call sites read and write
/// the one physical struct.
#[derive(Clone, Copy)]
struct TransamPtr(*mut TransamVariablesData);

// SAFETY: the pointer addresses the cluster-wide shmem `TransamVariablesData`,
// valid for the process lifetime; all field access is serialized by the LWLocks
// noted on each field group, exactly as in C. The `Cell` that holds it is
// per-backend (a thread-local).
unsafe impl Send for TransamPtr {}

impl core::ops::Deref for TransamPtr {
    type Target = TransamVariablesData;
    #[inline]
    fn deref(&self) -> &TransamVariablesData {
        // SAFETY: `self.0` points at the shared singleton in shmem.
        unsafe { &*self.0 }
    }
}

impl core::ops::DerefMut for TransamPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut TransamVariablesData {
        // SAFETY: as above; callers hold the appropriate LWLock for the write.
        unsafe { &mut *self.0 }
    }
}

thread_local! {
    /// `TransamVariables *TransamVariables;` (transam.c:32) — the per-process
    /// pointer at the shared singleton, set by [`VarsupShmemInit`].
    static TRANSAM_VARIABLES: Cell<Option<TransamPtr>> = const { Cell::new(None) };
}

/// Borrow the per-process pointer at the shared `TransamVariablesData`, panic
/// if accessed before `VarsupShmemInit`. Returns a copy of the `TransamPtr`
/// (the cheap `*mut`), which derefs the shared struct; the LWLock the caller
/// holds provides the cross-process serialization.
#[inline]
fn transam() -> TransamPtr {
    TRANSAM_VARIABLES.with(|p| {
        p.get()
            .expect("TransamVariables accessed before VarsupShmemInit")
    })
}

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
    guc_tables::vars::autovacuum_freeze_max_age.read()
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
/// `TransamVariablesData` singleton from the shared segment via
/// `ShmemInitStruct("TransamVariables", ...)` and `memset`s it to zero when
/// freshly created (`!found`); an attaching backend just finds it. Here the
/// singleton is allocated in the main `MAP_SHARED` segment the same way; the
/// first backend zeroes it, an attaching backend leaves it as found, and every
/// backend records its per-process [`TransamPtr`] at the one physical struct.
///
/// ```c
/// TransamVariables = (TransamVariablesData *)
///     ShmemInitStruct("TransamVariables", sizeof(TransamVariablesData), &found);
/// if (!found)
///     memset(TransamVariables, 0, sizeof(TransamVariablesData));
/// ```
pub fn VarsupShmemInit() -> PgResult<()> {
    use ipc_shmem_seams as shmem;

    let (addr, found) =
        shmem::shmem_init_struct::call("TransamVariables", VarsupShmemSize() as usize)?;
    let tv = addr as *mut TransamVariablesData;

    if !found {
        // We're the first — zero the freshly-carved shared struct in place.
        // SAFETY: `tv` addresses `sizeof(TransamVariablesData)` writable shmem
        // bytes just carved by `ShmemInitStruct`.
        unsafe {
            *tv = TransamVariablesData::default();
        }
    }

    // Record this backend's per-process pointer at the shared struct (C's
    // `TransamVariables = ...` assignment runs in every process, allocator or
    // attacher).
    TRANSAM_VARIABLES.with(|p| p.set(Some(TransamPtr(tv))));

    Ok(())
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

    let mut full_xid = transam().nextXid;
    let mut xid = XidFromFullTransactionId(full_xid);

    // Check to see if it's safe to assign another XID. This protects against
    // catastrophic data loss due to XID wraparound. Note that this coding also
    // appears in GetNewMultiXactId.
    let xid_vac_limit = transam().xidVacLimit;
    if TransactionIdFollowsOrEquals(xid, xid_vac_limit) {
        // For safety's sake, we release XidGenLock while sending signals,
        // warnings, etc., to avoid any possibility of deadlock while doing
        // get_database_name(). First, copy all the shared values we'll need.
        let (xidStopLimit, xidWarnLimit, xidWrapLimit, oldest_datoid) = {
            let tv = transam();
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
        full_xid = transam().nextXid;
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
        let mut tv = transam();
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
    let fullXid = transam().nextXid;
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
    let cur_next = transam().nextXid;
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
    transam().nextXid = newNextFullXid;
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
        let mut tv = transam();
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
        let mut tv = transam();
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
        let tv = transam();
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
        let mut tv = transam();

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
            // The WAL insert runs while we still hold OidGenLock (matching C);
            // the shared-struct pointer is re-fetched after to commit the
            // prefetch accounting.
            xlog_seams::xlog_put_next_oid::call(next_to_log)?;
            tv = transam();
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
        let mut tv = transam();
        if tv.nextOid > nextOid {
            let cur = tv.nextOid;
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
        let mut tv = transam();
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
        // on 32-bit atomic reads of the shared struct; the lockless read of the
        // two words mirrors that.
        let (oldest_xid, next_xid) = {
            let tv = transam();
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
    use varsup_seams as seams;

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

    // `SetTransactionIdLimit(oldest, oldest_db)` — the WAL-startup (xlog.c:5639)
    // and vacuum wraparound-limit setter. `varsup.c` OWNS the body but the seam
    // contract was scaffolded in the consumer crate
    // (`backend-commands-vacuum-seams`, `::call`ed by vacuum and the future
    // StartupXLOG driver); install it here so the single declared seam resolves.
    // The seam is `Mcx`-free; the body only consults its `mcx` to format a
    // database-name warning while inside a live transaction (never during
    // `StartupXLOG`), so a throwaway context suffices, mirroring the
    // `get_new_transaction_id` install above.
    vacuum_seams::set_transaction_id_limit::set(|oldest, oldest_db| {
        let cx = MemoryContext::new("SetTransactionIdLimit seam");
        SetTransactionIdLimit(cx.mcx(), oldest, oldest_db)
    });

    // `GetNewObjectId()` and `StopGeneratingPinnedObjectIds()` (catalog.c
    // callers) carry their `ereport(ERROR)` paths on `Err`.
    seams::get_new_object_id::set(GetNewObjectId);
    seams::stop_generating_pinned_object_ids::set(StopGeneratingPinnedObjectIds);

    // `TransamVariables->xactCompletionCount = 1;` — set once by procarray's
    // `ProcArrayShmemInit()` on first shared-memory init. `xactCompletionCount`
    // is a `ProcArrayLock`-protected field of the `TransamVariables` singleton
    // owned here; procarray reaches it through this owner seam.
    seams::init_xact_completion_count::set(|| {
        transam().xactCompletionCount = 1;
    });

    // `TransamVariables->latestCompletedXid` get/set + `xactCompletionCount++` —
    // the end-of-xact bookkeeping procarray.c's `MaintainLatestCompletedXid*` /
    // `ProcArray{EndTransactionInternal,Remove,ClearTransaction}` perform under
    // `ProcArrayLock`. The fields live in the shared `TransamVariables`
    // singleton owned here; procarray reaches them through these owner seams,
    // holding `ProcArrayLock` across the call exactly as in C — so the mutation
    // is visible to every backend reading the same shmem struct.
    // `TransamVariables->nextXid = checkPoint.nextXid; nextOid = checkPoint.nextOid;
    // oidCount = 0;` — the WAL-startup (xlog.c:5631-5634) seed of the XID/OID
    // counters from the starting checkpoint. No other process is up yet in C, so
    // there is no lock.
    seams::set_transam_variables_at_startup::set(|next_xid, next_oid| {
        let mut tv = transam();
        tv.nextXid = next_xid;
        tv.nextOid = next_oid;
        tv.oidCount = 0;
    });

    // Forward-only re-seed (COW-model `SeedTransamVariablesFromCheckpoint`): never
    // regress the cluster-wide counters below what redo already advanced them to.
    // `TransamVariables` is genuine shared memory, so the startup process' redo
    // advance is already visible here; this re-seed only lifts an unseeded child
    // up to the checkpoint. On the promotion path the durable checkpoint copy is
    // the pre-recovery one (promotion writes XLOG_END_OF_RECOVERY, not a
    // checkpoint), so an unconditional store would regress nextXid and make every
    // transaction committed during recovery invisible.
    seams::reseed_transam_variables_no_regress::set(|next_xid, next_oid| {
        let mut tv = transam();
        if FullTransactionIdPrecedes(tv.nextXid, next_xid) {
            tv.nextXid = next_xid;
        }
        // OIDs wrap, so "forward" is not a total order; only adopt the checkpoint
        // nextOid when the live value is still the unseeded zero (the COW child
        // case this re-seed exists for). A redo-advanced live value is left
        // untouched, exactly as for nextXid.
        if tv.nextOid == 0 {
            tv.nextOid = next_oid;
            tv.oidCount = 0;
        }
    });

    // `XLOG_NEXTOID` redo (xlog.c:8316-8331): believe the recorded nextOid
    // exactly and zero the prefetch count, under `OidGenLock`. varsup owns the
    // `TransamVariables` singleton + lock; the XLOG redo dispatcher reaches them
    // here.
    seams::redo_set_next_oid::set(|next_oid| {
        let guard = acquire(OID_GEN_LOCK, true)?;
        {
            let mut tv = transam();
            tv.nextOid = next_oid;
            tv.oidCount = 0;
        }
        guard.release()?;
        Ok(())
    });

    // The checkpoint XID/OID/CommitTs snapshots (`CreateCheckPoint`,
    // xlog.c:7159-7174). varsup owns the `TransamVariables` singleton + the
    // gen-locks; the checkpoint driver in xlog reaches the fields through these
    // owner seams, each holding the same LWLock C holds (XidGenLock / CommitTsLock
    // / OidGenLock in LW_SHARED).
    seams::get_checkpoint_xid_snapshot::set(|| {
        let guard = acquire(XID_GEN_LOCK, false)?;
        let snap = {
            let tv = transam();
            (tv.nextXid, tv.oldestXid, tv.oldestXidDB)
        };
        guard.release()?;
        Ok(snap)
    });
    seams::get_checkpoint_commit_ts_snapshot::set(|| {
        let guard = acquire(COMMIT_TS_LOCK, false)?;
        let snap = {
            let tv = transam();
            (tv.oldestCommitTsXid, tv.newestCommitTsXid)
        };
        guard.release()?;
        Ok(snap)
    });
    seams::get_checkpoint_next_oid::set(|include_oidcount| {
        let guard = acquire(OID_GEN_LOCK, false)?;
        let next_oid = {
            let tv = transam();
            if include_oidcount {
                tv.nextOid.wrapping_add(tv.oidCount)
            } else {
                tv.nextOid
            }
        };
        guard.release()?;
        Ok(next_oid)
    });

    // `XLOG_CHECKPOINT_ONLINE` redo (xlog.c:8446-8450): treat the recorded XID
    // counter as a minimum.
    seams::redo_advance_next_xid_min::set(|next_xid| {
        let guard = acquire(XID_GEN_LOCK, true)?;
        {
            let mut tv = transam();
            if FullTransactionIdPrecedes(tv.nextXid, next_xid) {
                tv.nextXid = next_xid;
            }
        }
        guard.release()?;
        Ok(())
    });
    // `XLOG_CHECKPOINT_SHUTDOWN` redo (xlog.c:8340-8346): believe the recorded
    // counters exactly.
    seams::redo_set_next_xid_oid_exact::set(|next_xid, next_oid| {
        let guard = acquire(XID_GEN_LOCK, true)?;
        transam().nextXid = next_xid;
        guard.release()?;
        let guard = acquire(OID_GEN_LOCK, true)?;
        {
            let mut tv = transam();
            tv.nextOid = next_oid;
            tv.oidCount = 0;
        }
        guard.release()?;
        Ok(())
    });

    seams::get_latest_completed_xid::set(|| transam().latestCompletedXid);
    seams::set_latest_completed_xid::set(|fxid| {
        transam().latestCompletedXid = fxid;
    });
    seams::increment_xact_completion_count::set(|| {
        transam().xactCompletionCount += 1;
    });
    seams::get_xact_completion_count::set(|| transam().xactCompletionCount);
    seams::get_oldest_xid::set(|| transam().oldestXid);
    seams::get_oldest_clog_xid::set(|| transam().oldestClogXid);

    // `TransamVariables->{oldest,newest}CommitTsXid` (access/transam.h field
    // accessors). commit_ts.c reads/writes these under `CommitTsLock`, which it
    // holds across the call; varsup owns the shared `TransamVariables`
    // singleton, so the seam is a plain field get/set on the shmem struct
    // (`CommitTsLock`, held by the caller, gives the cross-backend exclusion).
    seams::get_oldest_commit_ts_xid::set(|| transam().oldestCommitTsXid);
    seams::get_newest_commit_ts_xid::set(|| transam().newestCommitTsXid);
    seams::set_oldest_commit_ts_xid::set(|xid| {
        transam().oldestCommitTsXid = xid;
    });
    seams::set_newest_commit_ts_xid::set(|xid| {
        transam().newestCommitTsXid = xid;
    });

    // `VarsupShmemSize()` / `VarsupShmemInit()` (ipci.c shmem accumulator /
    // create-or-attach). `VarsupShmemInit` now carves the shared struct via
    // `ShmemInitStruct`, so it carries that out-of-memory `ereport(ERROR)`
    // surface on `Err`; `VarsupShmemSize` is the infallible struct size.
    seams::varsup_shmem_size::set(|| Ok(VarsupShmemSize()));
    seams::varsup_shmem_init::set(VarsupShmemInit);

    // vacuum's `vac_update_datfrozenxid` wraparound-limit refresh predicate.
    seams::force_transaction_id_limit_update::set(ForceTransactionIdLimitUpdate);

    // --- lazy-vacuum driver `ReadNextTransactionId()` (vacuumlazy.c index-scans
    //     diagnostic); home in vacuumlazy-seams, varsup.c/transam.h is its owner. ---
    vacuumlazy_seams::read_next_transaction_id::set(|| {
        Ok(XidFromFullTransactionId(ReadNextFullTransactionId()))
    });
}

#[cfg(test)]
mod tests;
