#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};
use std::sync::atomic::{fence, AtomicU32, AtomicU64};
use std::sync::OnceLock;

use elog::{elog, ereport};
use init_small::globals;
use lmgr_proc::{GetPGProcByNumber, MyProc, ProcGlobal};
use lwlock::{LWLockAcquire, LWLockRelease, LWLock, LW_EXCLUSIVE, LW_SHARED};
use pmsignal::{SendPostmasterSignal, PMSignalReason};
use types_core::{
    BootstrapTransactionId, FirstGenbkiObjectId, FirstNormalObjectId, FirstNormalTransactionId,
    FirstUnpinnedObjectId, FullTransactionId, MaxTransactionId, Oid, Size, TransactionId,
    TransactionIdFollowsOrEquals, TransactionIdIsNormal, TransactionIdIsValid,
    TransactionIdPrecedes, TransactionIdPrecedesOrEquals,
};
use types_error::{ErrorLocation, PgResult, DEBUG1, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, WARNING};
use types_storage::storage::PGPROC_MAX_CACHED_SUBXIDS;

const VAR_OID_PREFETCH: u32 = 8192;

// lwlocklist.h: OidGen=2, XidGen=3, XactTruncation=44.
pub const OID_GEN_LOCK: usize = 2;
pub const XID_GEN_LOCK: usize = 3;
pub const XACT_TRUNCATION_LOCK: usize = 44;

fn OidGenLock() -> &'static LWLock {
    lwlock::main_lock(OID_GEN_LOCK)
}

fn XidGenLock() -> &'static LWLock {
    lwlock::main_lock(XID_GEN_LOCK)
}

fn XactTruncationLock() -> &'static LWLock {
    lwlock::main_lock(XACT_TRUNCATION_LOCK)
}

// TransamVariablesData (transam.h); word atomics mirror C's aligned-word
// access, each field serialized by the lock named on it.
pub struct TransamVariablesShared {
    pub nextOid: AtomicU32,             // [OidGenLock]
    pub oidCount: AtomicU32,            // [OidGenLock]
    pub nextXid: AtomicU64,             // [XidGenLock]
    pub oldestXid: AtomicU32,           // [XidGenLock]
    pub xidVacLimit: AtomicU32,         // [XidGenLock]
    pub xidWarnLimit: AtomicU32,        // [XidGenLock]
    pub xidStopLimit: AtomicU32,        // [XidGenLock]
    pub xidWrapLimit: AtomicU32,        // [XidGenLock]
    pub oldestXidDB: AtomicU32,         // [XidGenLock]
    pub oldestCommitTsXid: AtomicU32,   // [CommitTsLock]
    pub newestCommitTsXid: AtomicU32,   // [CommitTsLock]
    pub latestCompletedXid: AtomicU64,  // [ProcArrayLock]
    pub xactCompletionCount: AtomicU64, // [ProcArrayLock]
    pub oldestClogXid: AtomicU32,       // [XactTruncationLock]
}

static TRANSAM_VARIABLES: OnceLock<TransamVariablesShared> = OnceLock::new();

pub fn TransamVariables() -> &'static TransamVariablesShared {
    TRANSAM_VARIABLES
        .get()
        .unwrap_or_else(|| panic!("VarsupShmemInit has not run"))
}

pub fn VarsupShmemSize() -> Size {
    core::mem::size_of::<TransamVariablesShared>()
}

fn boot_image() -> TransamVariablesShared {
    TransamVariablesShared {
        nextOid: AtomicU32::new(0),
        oidCount: AtomicU32::new(0),
        // DIVERGENCE: C memsets to zero and StartupXLOG seeds from the
        // checkpoint; FirstNormal seeds stand in until xlog owns startup.
        nextXid: AtomicU64::new(
            FullTransactionId::from_epoch_and_xid(0, FirstNormalTransactionId).value,
        ),
        oldestXid: AtomicU32::new(FirstNormalTransactionId),
        xidVacLimit: AtomicU32::new(0),
        xidWarnLimit: AtomicU32::new(0),
        xidStopLimit: AtomicU32::new(0),
        xidWrapLimit: AtomicU32::new(0),
        oldestXidDB: AtomicU32::new(0),
        oldestCommitTsXid: AtomicU32::new(0),
        newestCommitTsXid: AtomicU32::new(0),
        latestCompletedXid: AtomicU64::new(
            FullTransactionId::from_epoch_and_xid(0, FirstNormalTransactionId).value,
        ),
        xactCompletionCount: AtomicU64::new(1),
        oldestClogXid: AtomicU32::new(FirstNormalTransactionId),
    }
}

pub fn VarsupShmemInit() {
    TRANSAM_VARIABLES
        .set(boot_image())
        .unwrap_or_else(|_| panic!("VarsupShmemInit called twice"));
}

/// Crash-cycle reset in place (notes/crash-restart-design.md); startup re-seeds
/// from the checkpoint as after C's shmem re-create.
pub fn VarsupShmemReset() {
    use std::sync::atomic::Ordering::Relaxed;
    let live = TransamVariables();
    let boot = boot_image();
    live.nextOid.store(boot.nextOid.load(Relaxed), Relaxed);
    live.oidCount.store(boot.oidCount.load(Relaxed), Relaxed);
    live.nextXid.store(boot.nextXid.load(Relaxed), Relaxed);
    live.oldestXid.store(boot.oldestXid.load(Relaxed), Relaxed);
    live.xidVacLimit.store(boot.xidVacLimit.load(Relaxed), Relaxed);
    live.xidWarnLimit.store(boot.xidWarnLimit.load(Relaxed), Relaxed);
    live.xidStopLimit.store(boot.xidStopLimit.load(Relaxed), Relaxed);
    live.xidWrapLimit.store(boot.xidWrapLimit.load(Relaxed), Relaxed);
    live.oldestXidDB.store(boot.oldestXidDB.load(Relaxed), Relaxed);
    live.oldestCommitTsXid.store(boot.oldestCommitTsXid.load(Relaxed), Relaxed);
    live.newestCommitTsXid.store(boot.newestCommitTsXid.load(Relaxed), Relaxed);
    live.latestCompletedXid.store(boot.latestCompletedXid.load(Relaxed), Relaxed);
    live.xactCompletionCount.store(boot.xactCompletionCount.load(Relaxed), Relaxed);
    live.oldestClogXid.store(boot.oldestClogXid.load(Relaxed), Relaxed);
}

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn TransactionIdAdvance(xid: &mut TransactionId) {
    *xid = xid.wrapping_add(1);
    if *xid < FirstNormalTransactionId {
        *xid = FirstNormalTransactionId;
    }
}

fn FullTransactionIdAdvance(dest: &mut FullTransactionId) {
    dest.value += 1;
    while dest.xid() < FirstNormalTransactionId {
        dest.value += 1;
    }
}

fn my_proc() -> &'static types_storage::storage::PGPROC {
    GetPGProcByNumber(MyProc().expect("GetNewTransactionId without MyProc"))
}

pub fn GetNewTransactionId(isSubXact: bool) -> PgResult<FullTransactionId> {
    if xact_seams::is_in_parallel_mode::call() {
        elog(ERROR, "cannot assign TransactionIds during a parallel operation")?;
    }

    if miscinit_seams::is_bootstrap_processing_mode::call() {
        debug_assert!(!isSubXact);
        let proc = my_proc();
        proc.xid.value.store(BootstrapTransactionId, Relaxed);
        ProcGlobal().xids[proc.pgxactoff.load(Relaxed) as usize]
            .value
            .store(BootstrapTransactionId, Relaxed);
        return Ok(FullTransactionId::from_epoch_and_xid(0, BootstrapTransactionId));
    }

    if transam_xlog_seams::recovery_in_progress::call() {
        elog(ERROR, "cannot assign TransactionIds during recovery")?;
    }

    let tv = TransamVariables();
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;

    let mut full_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    let mut xid = full_xid.xid();

    // Wraparound defenses: autovac past xidVacLimit, warn past xidWarnLimit,
    // refuse past xidStopLimit (single-user mode is the DBA escape hatch).
    if TransactionIdFollowsOrEquals(xid, tv.xidVacLimit.load(Relaxed)) {
        // Copy shared values then drop XidGenLock: get_database_name must
        // not run under it (deadlock risk).
        let xidWarnLimit = tv.xidWarnLimit.load(Relaxed);
        let xidStopLimit = tv.xidStopLimit.load(Relaxed);
        let xidWrapLimit = tv.xidWrapLimit.load(Relaxed);
        let oldest_datoid = tv.oldestXidDB.load(Relaxed);

        LWLockRelease(XidGenLock())?;

        if globals::IsUnderPostmaster() && (xid % 65536) == 0 {
            SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }

        if globals::IsUnderPostmaster() && TransactionIdFollowsOrEquals(xid, xidStopLimit) {
            // complain even if that DB has disappeared
            let oldest_datname = dbcommands_seams::get_database_name::call(oldest_datoid)?;
            let msg = match oldest_datname {
                Some(name) => format!(
                    "database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database \"{name}\""
                ),
                None => format!(
                    "database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database with OID {oldest_datoid}"
                ),
            };
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(msg)
                .errhint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(loc("GetNewTransactionId"))?;
        } else if TransactionIdFollowsOrEquals(xid, xidWarnLimit) {
            let oldest_datname = dbcommands_seams::get_database_name::call(oldest_datoid)?;
            match oldest_datname {
                Some(name) => ereport(WARNING)
                    .errmsg(format!(
                        "database \"{name}\" must be vacuumed within {} transactions",
                        xidWrapLimit.wrapping_sub(xid)
                    ))
                    .errhint(
                        "To avoid transaction ID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                    )
                    .finish(loc("GetNewTransactionId"))?,
                None => ereport(WARNING)
                    .errmsg(format!(
                        "database with OID {oldest_datoid} must be vacuumed within {} transactions",
                        xidWrapLimit.wrapping_sub(xid)
                    ))
                    .errhint(
                        "To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                    )
                    .finish(loc("GetNewTransactionId"))?,
            }
        }

        LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
        full_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
        xid = full_xid.xid();
    }

    // Zero any fresh commit-log page under XidGenLock, else a later XID
    // could commit before the page exists.
    clog::ExtendCLOG(xid)?;
    // C keeps ExtendCommitTs's commitTsActive cheap-out inside the callee.
    if commit_ts_seams::extend_commit_ts::is_installed() {
        commit_ts_seams::extend_commit_ts::call(xid)?;
    }
    subtrans_seams::extend_subtrans::call(xid)?;

    let mut next = full_xid;
    FullTransactionIdAdvance(&mut next);
    tv.nextXid.store(next.value, Relaxed);

    // Publish the XID in the ProcArray before releasing XidGenLock: every
    // active XID older than latestCompletedXid must be visible there
    // (transam/README); a full PGPROC subxid cache overflows to pg_subtrans.
    let proc = my_proc();
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;
    if !isSubXact {
        debug_assert_eq!(ProcGlobal().subxidStates[pgxactoff].get().count, 0);
        debug_assert!(!ProcGlobal().subxidStates[pgxactoff].get().overflowed);
        debug_assert_eq!(proc.subxidStatus.get().count, 0);
        debug_assert!(!proc.subxidStatus.get().overflowed);

        // LWLockRelease acts as barrier
        proc.xid.value.store(xid, Relaxed);
        ProcGlobal().xids[pgxactoff].value.store(xid, Relaxed);
    } else {
        let substat = &ProcGlobal().subxidStates[pgxactoff];
        let mut my_status = proc.subxidStatus.get();
        let nxids = my_status.count as usize;

        debug_assert_eq!(substat.get().count, my_status.count);
        debug_assert_eq!(substat.get().overflowed, my_status.overflowed);

        if nxids < PGPROC_MAX_CACHED_SUBXIDS {
            // SAFETY: own PGPROC's subxid slot; writes serialized by
            // XidGenLock, readers pair with the fence below (pg_write_barrier).
            unsafe {
                (*proc.subxids.ptr()).xids[nxids] = xid;
            }
            fence(Release);
            my_status.count = nxids as u8 + 1;
            proc.subxidStatus.set(my_status);
            let mut shared_status = substat.get();
            shared_status.count = nxids as u8 + 1;
            substat.set(shared_status);
        } else {
            my_status.overflowed = true;
            proc.subxidStatus.set(my_status);
            let mut shared_status = substat.get();
            shared_status.overflowed = true;
            substat.set(shared_status);
        }
    }

    LWLockRelease(XidGenLock())?;

    Ok(full_xid)
}

pub fn ReadNextFullTransactionId() -> PgResult<FullTransactionId> {
    LWLockAcquire(XidGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let full_xid = FullTransactionId::from_u64(TransamVariables().nextXid.load(Relaxed));
    LWLockRelease(XidGenLock())?;
    Ok(full_xid)
}

pub fn ReadNextTransactionId() -> PgResult<TransactionId> {
    Ok(ReadNextFullTransactionId()?.xid())
}

pub fn AdvanceNextFullTransactionIdPastXid(xid: TransactionId) -> PgResult<()> {
    debug_assert!(
        miscinit::GetMyBackendType() == types_core::BackendType::Startup
            || !globals::IsUnderPostmaster()
    );

    let tv = TransamVariables();
    // Startup-process/single-process only, so the unlocked read is safe.
    let cur = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    let next_xid = cur.xid();
    if !TransactionIdFollowsOrEquals(xid, next_xid) {
        return Ok(());
    }

    // Safe: the active-xid span never exceeds one epoch in the WAL stream.
    let mut xid = xid;
    TransactionIdAdvance(&mut xid);
    let mut epoch = cur.epoch();
    if xid < next_xid {
        epoch += 1;
    }
    let new_next = FullTransactionId::from_epoch_and_xid(epoch, xid);

    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    tv.nextXid.store(new_next.value, Relaxed);
    LWLockRelease(XidGenLock())
}

pub fn AdvanceOldestClogXid(oldest_datfrozenxid: TransactionId) -> PgResult<()> {
    let tv = TransamVariables();
    LWLockAcquire(XactTruncationLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    if TransactionIdPrecedes(tv.oldestClogXid.load(Relaxed), oldest_datfrozenxid) {
        tv.oldestClogXid.store(oldest_datfrozenxid, Relaxed);
    }
    LWLockRelease(XactTruncationLock())
}

pub fn SetTransactionIdLimit(oldest_datfrozenxid: TransactionId, oldest_datoid: Oid) -> PgResult<()> {
    debug_assert!(TransactionIdIsNormal(oldest_datfrozenxid));

    let mut xidWrapLimit = oldest_datfrozenxid.wrapping_add(MaxTransactionId >> 1);
    if xidWrapLimit < FirstNormalTransactionId {
        xidWrapLimit = xidWrapLimit.wrapping_add(FirstNormalTransactionId);
    }

    // Refuse new XIDs within 3M of data loss.
    let mut xidStopLimit = xidWrapLimit.wrapping_sub(3_000_000);
    if xidStopLimit < FirstNormalTransactionId {
        xidStopLimit = xidStopLimit.wrapping_sub(FirstNormalTransactionId);
    }

    let mut xidWarnLimit = xidWrapLimit.wrapping_sub(40_000_000);
    if xidWarnLimit < FirstNormalTransactionId {
        xidWarnLimit = xidWarnLimit.wrapping_sub(FirstNormalTransactionId);
    }

    let mut xidVacLimit =
        oldest_datfrozenxid.wrapping_add(globals::autovacuum_freeze_max_age() as u32);
    if xidVacLimit < FirstNormalTransactionId {
        xidVacLimit = xidVacLimit.wrapping_add(FirstNormalTransactionId);
    }

    let tv = TransamVariables();
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    tv.oldestXid.store(oldest_datfrozenxid, Relaxed);
    tv.xidVacLimit.store(xidVacLimit, Relaxed);
    tv.xidWarnLimit.store(xidWarnLimit, Relaxed);
    tv.xidStopLimit.store(xidStopLimit, Relaxed);
    tv.xidWrapLimit.store(xidWrapLimit, Relaxed);
    tv.oldestXidDB.store(oldest_datoid, Relaxed);
    let curXid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();
    LWLockRelease(XidGenLock())?;

    ereport(DEBUG1)
        .errmsg_internal(format!(
            "transaction ID wrap limit is {xidWrapLimit}, limited by database with OID {oldest_datoid}"
        ))
        .finish(loc("SetTransactionIdLimit"))?;

    if TransactionIdFollowsOrEquals(curXid, xidVacLimit)
        && globals::IsUnderPostmaster()
        && !xlogutils_seams::in_recovery::call()
    {
        SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
    }

    if TransactionIdFollowsOrEquals(curXid, xidWarnLimit) && !xlogutils_seams::in_recovery::call()
    {
        // No database access outside a transaction (e.g. StartupXLOG).
        let oldest_datname = if xact_seams::is_transaction_state::call() {
            dbcommands_seams::get_database_name::call(oldest_datoid)?
        } else {
            None
        };

        match oldest_datname {
            Some(name) => ereport(WARNING)
                .errmsg(format!(
                    "database \"{name}\" must be vacuumed within {} transactions",
                    xidWrapLimit.wrapping_sub(curXid)
                ))
                .errhint(
                    "To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(loc("SetTransactionIdLimit"))?,
            None => ereport(WARNING)
                .errmsg(format!(
                    "database with OID {oldest_datoid} must be vacuumed within {} transactions",
                    xidWrapLimit.wrapping_sub(curXid)
                ))
                .errhint(
                    "To avoid XID assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(loc("SetTransactionIdLimit"))?,
        }
    }
    Ok(())
}

pub fn ForceTransactionIdLimitUpdate() -> PgResult<bool> {
    let tv = TransamVariables();
    LWLockAcquire(XidGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let nextXid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();
    let xidVacLimit = tv.xidVacLimit.load(Relaxed);
    let oldestXid = tv.oldestXid.load(Relaxed);
    let oldestXidDB = tv.oldestXidDB.load(Relaxed);
    LWLockRelease(XidGenLock())?;

    if !TransactionIdIsNormal(oldestXid) {
        return Ok(true);
    }
    if !TransactionIdIsValid(xidVacLimit) {
        return Ok(true);
    }
    if TransactionIdFollowsOrEquals(nextXid, xidVacLimit) {
        return Ok(true);
    }
    if !syscache_seams::search_syscache_exists_databaseoid::call(oldestXidDB)? {
        return Ok(true);
    }
    Ok(false)
}

pub fn GetNewObjectId() -> PgResult<Oid> {
    if transam_xlog_seams::recovery_in_progress::call() {
        elog(ERROR, "cannot assign OIDs during recovery")?;
    }

    let tv = TransamVariables();
    LWLockAcquire(OidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;

    // Never return InvalidOid; normal operation stays at or above
    // FirstNormalObjectId (10000..16384 is initdb's assignment range).
    if tv.nextOid.load(Relaxed) < FirstNormalObjectId {
        if globals::IsPostmasterEnvironment() {
            tv.nextOid.store(FirstNormalObjectId, Relaxed);
            tv.oidCount.store(0, Relaxed);
        } else if tv.nextOid.load(Relaxed) < FirstGenbkiObjectId {
            tv.nextOid.store(FirstNormalObjectId, Relaxed);
            tv.oidCount.store(0, Relaxed);
        }
    }

    if tv.oidCount.load(Relaxed) == 0 {
        transam_xlog_seams::xlog_put_next_oid::call(
            tv.nextOid.load(Relaxed).wrapping_add(VAR_OID_PREFETCH),
        )?;
        tv.oidCount.store(VAR_OID_PREFETCH, Relaxed);
    }

    let result = tv.nextOid.load(Relaxed);

    tv.nextOid.store(result.wrapping_add(1), Relaxed);
    tv.oidCount.store(tv.oidCount.load(Relaxed) - 1, Relaxed);

    LWLockRelease(OidGenLock())?;

    Ok(result)
}

fn SetNextObjectId(nextOid: Oid) -> PgResult<()> {
    if globals::IsPostmasterEnvironment() {
        elog(ERROR, "cannot advance OID counter anymore")?;
    }

    let tv = TransamVariables();
    LWLockAcquire(OidGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;

    if tv.nextOid.load(Relaxed) > nextOid {
        elog(
            ERROR,
            format!(
                "too late to advance OID counter to {nextOid}, it is now {}",
                tv.nextOid.load(Relaxed)
            ),
        )?;
    }

    tv.nextOid.store(nextOid, Relaxed);
    tv.oidCount.store(0, Relaxed);

    LWLockRelease(OidGenLock())
}

pub fn StopGeneratingPinnedObjectIds() -> PgResult<()> {
    SetNextObjectId(FirstUnpinnedObjectId)
}

#[cfg(debug_assertions)]
pub fn AssertTransactionIdInAllowableRange(xid: TransactionId) {
    debug_assert!(TransactionIdIsValid(xid));

    if !TransactionIdIsNormal(xid) {
        return;
    }

    // No XidGenLock (may already be held): 32-bit reads are atomic; the
    // fence pairs with GetNewTransactionId's lock release.
    fence(SeqCst);
    let tv = TransamVariables();
    let oldest_xid = tv.oldestXid.load(Relaxed);
    let next_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();

    debug_assert!(
        TransactionIdFollowsOrEquals(xid, oldest_xid)
            || TransactionIdPrecedesOrEquals(xid, next_xid)
    );
}

#[cfg(not(debug_assertions))]
pub fn AssertTransactionIdInAllowableRange(_xid: TransactionId) {}

pub fn init_seams() {
    varsup_seams::get_new_transaction_id::set(GetNewTransactionId);
    varsup_seams::read_next_transaction_id::set(ReadNextTransactionId);
    varsup_seams::advance_next_full_transaction_id_past_xid::set(
        AdvanceNextFullTransactionIdPastXid,
    );
    varsup_seams::advance_oldest_clog_xid::set(AdvanceOldestClogXid);
    varsup_seams::set_transaction_id_limit::set(SetTransactionIdLimit);
}

#[cfg(test)]
mod tests;
