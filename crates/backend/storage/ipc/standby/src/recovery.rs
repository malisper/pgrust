use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering::Relaxed};

use adt_timestamp::{GetCurrentTimestamp, TimestampDifference, TimestampDifferenceExceeds};
use elog::{elog, ereport};
use mcx::{Mcx, MemoryContext, PgFxHashMap, PgVec};
use timeout::{
    disable_all_timeouts, enable_timeouts, EnableTimeoutParams, STANDBY_DEADLOCK_TIMEOUT,
    STANDBY_LOCK_TIMEOUT, STANDBY_TIMEOUT,
};
use types_core::{
    FullTransactionId, InvalidOid, InvalidTransactionId, MaxTransactionId, Oid, TimestampTz,
    TransactionId, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
    VirtualTransactionId,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG2, DEBUG4, ERRCODE_T_R_DEADLOCK_DETECTED, ERROR, LOG,
};
use types_storage::lock::{AccessExclusiveLock, LOCKTAG};
use types_storage::storage::ProcSignalReason;
use types_storage::RelFileLocator;
use ProcSignalReason::*;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// wait_classes.h + wait_event_types.h (PG 18.3).
const PG_WAIT_BUFFERPIN: u32 = 0x0400_0000;
const PG_WAIT_LOCK: u32 = 0x0300_0000;
const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BUFFER_PIN: u32 = PG_WAIT_BUFFERPIN;
const WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT: u32 = PG_WAIT_IPC | 44;
const WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE: u32 = PG_WAIT_IPC | 45;

const STANDBY_INITIAL_WAIT_US: i64 = 1000;

// C volatile sig_atomic_t; timeout handlers may run off-thread (pgarch
// precedent), hence process atomics.
static GOT_STANDBY_DEADLOCK_TIMEOUT: AtomicBool = AtomicBool::new(false);
static GOT_STANDBY_DELAY_TIMEOUT: AtomicBool = AtomicBool::new(false);
static GOT_STANDBY_LOCK_TIMEOUT: AtomicBool = AtomicBool::new(false);

// GUC homes (standby.c globals).
static MAX_STANDBY_ARCHIVE_DELAY: AtomicI32 = AtomicI32::new(30 * 1000);
static MAX_STANDBY_STREAMING_DELAY: AtomicI32 = AtomicI32::new(30 * 1000);
static LOG_RECOVERY_CONFLICT_WAITS: AtomicBool = AtomicBool::new(false);

pub(crate) fn install_guc_backings() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::max_standby_archive_delay.install(GucVarAccessors {
        get: || MAX_STANDBY_ARCHIVE_DELAY.load(Relaxed),
        set: |v| MAX_STANDBY_ARCHIVE_DELAY.store(v, Relaxed),
    });
    guc_tables::vars::max_standby_streaming_delay.install(GucVarAccessors {
        get: || MAX_STANDBY_STREAMING_DELAY.load(Relaxed),
        set: |v| MAX_STANDBY_STREAMING_DELAY.store(v, Relaxed),
    });
    guc_tables::vars::log_recovery_conflict_waits.install(GucVarAccessors {
        get: || LOG_RECOVERY_CONFLICT_WAITS.load(Relaxed),
        set: |v| LOG_RECOVERY_CONFLICT_WAITS.store(v, Relaxed),
    });
}

struct RecoveryLockState {
    mcx: Mcx<'static>,
    // RecoveryLockHash: dedupe keyed (xid, dbOid, relOid);
    // RecoveryLockXidHash: per-xid chain of (dbOid, relOid).
    entries: PgFxHashMap<'static, (TransactionId, Oid, Oid), ()>,
    by_xid: PgFxHashMap<'static, TransactionId, PgVec<'static, (Oid, Oid)>>,
}

impl RecoveryLockState {
    fn insert_entry(&mut self, xid: TransactionId, db_oid: Oid, rel_oid: Oid) -> bool {
        if self.entries.insert((xid, db_oid, rel_oid), ()).is_some() {
            return false;
        }
        let mcx = self.mcx;
        self.by_xid.entry(xid).or_insert_with(|| PgVec::new_in(mcx)).push((db_oid, rel_oid));
        true
    }
}

thread_local! {
    // Startup-thread only (C statics in standby.c). None = tracking not
    // initialized or already shut down.
    static RECOVERY_LOCKS: RefCell<Option<RecoveryLockState>> = const { RefCell::new(None) };
    static STANDBY_WAIT_US: Cell<i64> = const { Cell::new(STANDBY_INITIAL_WAIT_US) };
}

fn new_lock_state() -> RecoveryLockState {
    let cx: &'static MemoryContext = ::mcx::session_root("RecoveryLockHash");
    // LIFO: empty the droppy TLS slot before its context is freed.
    ::mcx::register_session_cleanup(Box::new(|| {
        RECOVERY_LOCKS.with(|c| drop(c.borrow_mut().take()));
    }));
    let mcx = cx.mcx();
    RecoveryLockState {
        mcx,
        entries: PgFxHashMap::with_hasher_in(Default::default(), mcx),
        by_xid: PgFxHashMap::with_hasher_in(Default::default(), mcx),
    }
}

pub fn InitRecoveryTransactionEnvironment() -> PgResult<()> {
    RECOVERY_LOCKS.with(|s| {
        assert!(s.borrow().is_none(), "InitRecoveryTransactionEnvironment called twice");
        *s.borrow_mut() = Some(new_lock_state());
    });

    sinval::SharedInvalBackendInit(true)?;

    let my_procno = init_small::globals::MyProcNumber();
    let proc = lmgr_proc::GetPGProcByNumber(my_procno);
    proc.vxid.procNumber.store(my_procno, Relaxed);
    let vxid = VirtualTransactionId {
        procNumber: my_procno,
        localTransactionId: sinval::GetNextLocalTransactionId(),
    };
    lock::VirtualXactLockTableInsert(vxid)?;

    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);
    Ok(())
}

pub fn ShutdownRecoveryTransactionEnvironment() -> PgResult<()> {
    // None = a FATAL before initialization; there is nothing to do.
    if RECOVERY_LOCKS.with(|s| s.borrow().is_none()) {
        return Ok(());
    }

    procarray::ExpireAllKnownAssignedTransactionIds()?;
    StandbyReleaseAllLocks()?;
    RECOVERY_LOCKS.with(|s| *s.borrow_mut() = None);
    lock::VirtualXactLockTableCleanup()
}

// Returns 0 ("a time safely in the past") to mean wait forever, as C.
fn GetStandbyLimitTime() -> TimestampTz {
    let (rtime, from_stream) = xlogrecovery_seams::get_xlog_receipt_time::call();
    let delay = if from_stream {
        MAX_STANDBY_STREAMING_DELAY.load(Relaxed)
    } else {
        MAX_STANDBY_ARCHIVE_DELAY.load(Relaxed)
    };
    if delay < 0 {
        return 0;
    }
    rtime + (delay as i64) * 1000
}

fn WaitExceedsMaxStandbyDelay(wait_event_info: u32) -> PgResult<bool> {
    postgres_seams::check_for_interrupts::call()?;

    let ltime = GetStandbyLimitTime();
    if ltime != 0 && GetCurrentTimestamp() >= ltime {
        return Ok(true);
    }

    waitevent::pgstat_report_wait_start(wait_event_info);
    std::thread::sleep(std::time::Duration::from_micros(STANDBY_WAIT_US.get() as u64));
    waitevent::pgstat_report_wait_end();

    // Progressive backoff, capped at 1s as C.
    STANDBY_WAIT_US.set((STANDBY_WAIT_US.get() * 2).min(1_000_000));

    Ok(false)
}

pub fn LogRecoveryConflict(
    reason: ProcSignalReason,
    wait_start: TimestampTz,
    now: TimestampTz,
    wait_list: Option<&[VirtualTransactionId]>,
    still_waiting: bool,
) -> PgResult<()> {
    debug_assert!(still_waiting || wait_list.is_none());

    let (secs, usecs) = TimestampDifference(wait_start, now);
    let msecs = secs * 1000 + (usecs / 1000) as i64;
    let usecs = usecs % 1000;

    let mut buf = String::new();
    let mut nprocs: u64 = 0;
    if let Some(list) = wait_list {
        for vxid in list {
            // proc can be None if the target backend is no longer active.
            if let Some(proc) = lmgr_proc::ProcNumberGetProc(vxid.procNumber) {
                let pid = proc.pid.load(Relaxed);
                if nprocs > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&pid.to_string());
                nprocs += 1;
            }
        }
    }

    if still_waiting {
        let mut b = ereport(LOG).errmsg(format!(
            "recovery still waiting after {msecs}.{usecs:03} ms: {}",
            get_recovery_conflict_desc(reason)
        ));
        if nprocs > 0 {
            b = b.errdetail_log_plural(
                format!("Conflicting process: {buf}."),
                format!("Conflicting processes: {buf}."),
                nprocs,
            );
        }
        b.finish(loc("LogRecoveryConflict"))?;
    } else {
        ereport(LOG)
            .errmsg(format!(
                "recovery finished waiting after {msecs}.{usecs:03} ms: {}",
                get_recovery_conflict_desc(reason)
            ))
            .finish(loc("LogRecoveryConflict"))?;
    }
    Ok(())
}

fn ResolveRecoveryConflictWithVirtualXIDs(
    waitlist: &[VirtualTransactionId],
    reason: ProcSignalReason,
    wait_event_info: u32,
    report_waiting: bool,
) -> PgResult<()> {
    if waitlist.is_empty() {
        return Ok(());
    }

    let mut wait_start: TimestampTz = 0;
    let mut waiting = false;
    let mut logged_recovery_conflict = false;

    if report_waiting
        && (LOG_RECOVERY_CONFLICT_WAITS.load(Relaxed)
            || guc_tables::vars::update_process_title.read())
    {
        wait_start = GetCurrentTimestamp();
    }

    for (i, vxid) in waitlist.iter().enumerate() {
        STANDBY_WAIT_US.set(STANDBY_INITIAL_WAIT_US);

        while !lock::VirtualXactLock(*vxid, false)? {
            if WaitExceedsMaxStandbyDelay(wait_event_info)? {
                let pid = procarray::CancelVirtualTransaction(*vxid, reason)?;
                // Wait a little for it to die, to avoid flooding an
                // unresponsive backend under load.
                if pid != 0 {
                    std::thread::sleep(std::time::Duration::from_micros(5000));
                }
            }

            if wait_start != 0 && (!logged_recovery_conflict || !waiting) {
                let maybe_log_conflict =
                    LOG_RECOVERY_CONFLICT_WAITS.load(Relaxed) && !logged_recovery_conflict;
                let maybe_update_title =
                    guc_tables::vars::update_process_title.read() && !waiting;

                let mut now: TimestampTz = 0;
                if maybe_log_conflict || maybe_update_title {
                    now = GetCurrentTimestamp();
                }

                if maybe_update_title && TimestampDifferenceExceeds(wait_start, now, 500) {
                    ps_status::set_ps_display_suffix("waiting");
                    waiting = true;
                }

                if maybe_log_conflict
                    && TimestampDifferenceExceeds(
                        wait_start,
                        now,
                        guc_tables::vars::DeadlockTimeout.read(),
                    )
                {
                    LogRecoveryConflict(reason, wait_start, now, Some(&waitlist[i..]), true)?;
                    logged_recovery_conflict = true;
                }
            }
        }
    }

    if logged_recovery_conflict {
        LogRecoveryConflict(reason, wait_start, GetCurrentTimestamp(), None, false)?;
    }

    if waiting {
        ps_status::set_ps_display_remove_suffix();
    }
    Ok(())
}

pub fn ResolveRecoveryConflictWithSnapshot(
    snapshot_conflict_horizon: TransactionId,
    is_catalog_rel: bool,
    locator: RelFileLocator,
) -> PgResult<()> {
    // InvalidTransactionId means "definitely no conflicts" (WAL convention).
    if !TransactionIdIsValid(snapshot_conflict_horizon) {
        return Ok(());
    }

    debug_assert!(TransactionIdIsNormal(snapshot_conflict_horizon));
    let backends =
        procarray::GetConflictingVirtualXIDs(snapshot_conflict_horizon, locator.dbOid)?;
    ResolveRecoveryConflictWithVirtualXIDs(
        &backends,
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
        true,
    )?;

    if transam_xlog::wal_level() >= transam_xlog::WAL_LEVEL_LOGICAL && is_catalog_rel {
        slot::InvalidateObsoleteReplicationSlots(
            slot::RS_INVAL_HORIZON.0 as u32,
            0,
            locator.dbOid,
            snapshot_conflict_horizon,
        )?;
    }
    Ok(())
}

pub fn ResolveRecoveryConflictWithSnapshotFullXid(
    snapshot_conflict_horizon: FullTransactionId,
    is_catalog_rel: bool,
    locator: RelFileLocator,
) -> PgResult<()> {
    // If the logged value already wrapped around, no snapshot can see it.
    let next_xid = varsup::ReadNextFullTransactionId()?;
    let diff = next_xid.to_u64().wrapping_sub(snapshot_conflict_horizon.to_u64());
    if diff < (MaxTransactionId / 2) as u64 {
        ResolveRecoveryConflictWithSnapshot(
            snapshot_conflict_horizon.xid(),
            is_catalog_rel,
            locator,
        )?;
    }
    Ok(())
}

pub fn ResolveRecoveryConflictWithTablespace(_tsid: Oid) -> PgResult<()> {
    // Cancel everyone with a temp file, current users only, no commit wait.
    let temp_file_users = procarray::GetConflictingVirtualXIDs(InvalidTransactionId, InvalidOid)?;
    ResolveRecoveryConflictWithVirtualXIDs(
        &temp_file_users,
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
        true,
    )
}

pub fn ResolveRecoveryConflictWithDatabase(dbid: Oid) -> PgResult<()> {
    // No vxid wait (idle sessions would block us): force everyone off.
    while procarray::CountDBBackends(dbid)? > 0 {
        procarray::CancelDBBackends(dbid, PROCSIG_RECOVERY_CONFLICT_DATABASE, true)?;
        std::thread::sleep(std::time::Duration::from_micros(10000));
    }
    Ok(())
}

pub fn ResolveRecoveryConflictWithLock(locktag: LOCKTAG, logging_conflict: bool) -> PgResult<()> {
    debug_assert!(xlogutils::InHotStandby());

    let ltime = GetStandbyLimitTime();
    let now = GetCurrentTimestamp();

    // waitStart is written without the partition lock, as C (pg_locks may
    // read a transient 0).
    let my_proc = lmgr_proc::GetPGProcByNumber(init_small::globals::MyProcNumber());
    if my_proc.waitStart.read() == 0 {
        my_proc.waitStart.write(now as u64);
    }

    if now >= ltime && ltime != 0 {
        let cx = MemoryContext::new("ResolveRecoveryConflictWithLock");
        let backends = lock::GetLockConflicts(cx.mcx(), &locktag, AccessExclusiveLock)?;
        // report_waiting=false: WaitOnLock already reported it.
        ResolveRecoveryConflictWithVirtualXIDs(
            &backends,
            PROCSIG_RECOVERY_CONFLICT_LOCK,
            PG_WAIT_LOCK | locktag.locktag_type as u32,
            false,
        )?;
    } else {
        let mut timeouts =
            [EnableTimeoutParams::After { id: STANDBY_DEADLOCK_TIMEOUT, delay_ms: 0 }; 2];
        let mut cnt = 0;
        if ltime != 0 {
            GOT_STANDBY_LOCK_TIMEOUT.store(false, Relaxed);
            timeouts[cnt] = EnableTimeoutParams::At { id: STANDBY_LOCK_TIMEOUT, fin_time: ltime };
            cnt += 1;
        }
        GOT_STANDBY_DEADLOCK_TIMEOUT.store(false, Relaxed);
        timeouts[cnt] = EnableTimeoutParams::After {
            id: STANDBY_DEADLOCK_TIMEOUT,
            delay_ms: guc_tables::vars::DeadlockTimeout.read(),
        };
        cnt += 1;
        enable_timeouts(&timeouts[..cnt]);
    }

    // Wait to be signaled by the release of the relation lock.
    lmgr_proc::ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type as u32);

    // If ltime was reached, exit; the next call cancels the holders.
    if !GOT_STANDBY_LOCK_TIMEOUT.load(Relaxed) && GOT_STANDBY_DEADLOCK_TIMEOUT.load(Relaxed) {
        let cx = MemoryContext::new("ResolveRecoveryConflictWithLock");
        let backends = lock::GetLockConflicts(cx.mcx(), &locktag, AccessExclusiveLock)?;
        if !backends.is_empty() {
            for vxid in backends.iter() {
                procarray::SignalVirtualTransaction(
                    *vxid,
                    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
                    false,
                )?;
            }
            // If the conflict still needs logging, exit so the caller can log
            // it; it calls back with logging_conflict=false and we wait again.
            if !logging_conflict {
                GOT_STANDBY_DEADLOCK_TIMEOUT.store(false, Relaxed);
                lmgr_proc::ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type as u32);
            }
        }
    }

    disable_all_timeouts(false);
    GOT_STANDBY_LOCK_TIMEOUT.store(false, Relaxed);
    GOT_STANDBY_DEADLOCK_TIMEOUT.store(false, Relaxed);
    Ok(())
}

pub fn ResolveRecoveryConflictWithBufferPin() -> PgResult<()> {
    debug_assert!(xlogutils::InHotStandby());

    let ltime = GetStandbyLimitTime();

    if GetCurrentTimestamp() >= ltime && ltime != 0 {
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN)?;
    } else {
        let mut timeouts =
            [EnableTimeoutParams::After { id: STANDBY_DEADLOCK_TIMEOUT, delay_ms: 0 }; 2];
        let mut cnt = 0;
        if ltime != 0 {
            timeouts[cnt] = EnableTimeoutParams::At { id: STANDBY_TIMEOUT, fin_time: ltime };
            cnt += 1;
        }
        GOT_STANDBY_DEADLOCK_TIMEOUT.store(false, Relaxed);
        timeouts[cnt] = EnableTimeoutParams::After {
            id: STANDBY_DEADLOCK_TIMEOUT,
            delay_ms: guc_tables::vars::DeadlockTimeout.read(),
        };
        cnt += 1;
        enable_timeouts(&timeouts[..cnt]);
    }

    // Woken only by UnpinBuffer() or the timeouts above.
    lmgr_proc::ProcWaitForSignal(WAIT_EVENT_BUFFER_PIN);

    if GOT_STANDBY_DELAY_TIMEOUT.load(Relaxed) {
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN)?;
    } else if GOT_STANDBY_DEADLOCK_TIMEOUT.load(Relaxed) {
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK)?;
    }

    disable_all_timeouts(false);
    GOT_STANDBY_DELAY_TIMEOUT.store(false, Relaxed);
    GOT_STANDBY_DEADLOCK_TIMEOUT.store(false, Relaxed);
    Ok(())
}

fn SendRecoveryConflictWithBufferPin(reason: ProcSignalReason) -> PgResult<()> {
    debug_assert!(
        reason == PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
            || reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
    );
    // Most backends are innocent; the signal handler in each decides.
    procarray::CancelDBBackends(InvalidOid, reason, false)
}

pub fn CheckRecoveryConflictDeadlock() -> PgResult<()> {
    debug_assert!(!xlogutils::in_recovery());

    if !bufmgr::HoldingBufferPinThatDelaysRecovery() {
        return Ok(());
    }

    // Message matches ProcessInterrupts; only the current transaction is
    // canceled, so a pin held by a parent subtransaction keeps Startup waiting.
    ereport(ERROR)
        .errcode(ERRCODE_T_R_DEADLOCK_DETECTED)
        .errmsg("canceling statement due to conflict with recovery")
        .errdetail("User transaction caused buffer deadlock with recovery.")
        .finish(loc("CheckRecoveryConflictDeadlock"))
}

pub fn StandbyDeadLockHandler() {
    GOT_STANDBY_DEADLOCK_TIMEOUT.store(true, Relaxed);
}

pub fn StandbyTimeoutHandler() {
    GOT_STANDBY_DELAY_TIMEOUT.store(true, Relaxed);
}

pub fn StandbyLockTimeoutHandler() {
    GOT_STANDBY_LOCK_TIMEOUT.store(true, Relaxed);
}

pub fn StandbyAcquireAccessExclusiveLock(
    xid: TransactionId,
    db_oid: Oid,
    rel_oid: Oid,
) -> PgResult<()> {
    if !TransactionIdIsValid(xid)
        || transam::TransactionIdDidCommit(xid)?
        || transam::TransactionIdDidAbort(xid)?
    {
        return Ok(());
    }

    elog(DEBUG4, format!("adding recovery lock: db {db_oid} rel {rel_oid}"))?;

    // dbOid is InvalidOid when locking a shared relation.
    debug_assert!(rel_oid != InvalidOid);

    let inserted = RECOVERY_LOCKS.with(|s| {
        let mut st = s.borrow_mut();
        st.as_mut()
            .expect("StandbyAcquireAccessExclusiveLock before InitRecoveryTransactionEnvironment")
            .insert_entry(xid, db_oid, rel_oid)
    });

    if inserted {
        let locktag = LOCKTAG::relation(db_oid, rel_oid);
        lock::LockAcquire(&locktag, AccessExclusiveLock, true, false)?;
    }
    Ok(())
}

fn release_xid_entry_locks(
    st: &mut RecoveryLockState,
    xid: TransactionId,
    chain: &[(Oid, Oid)],
) -> PgResult<()> {
    for &(db_oid, rel_oid) in chain {
        elog(DEBUG4, format!("releasing recovery lock: xid {xid} db {db_oid} rel {rel_oid}"))?;
        let locktag = LOCKTAG::relation(db_oid, rel_oid);
        if !lock::LockRelease(&locktag, AccessExclusiveLock, true)? {
            elog(
                LOG,
                format!(
                    "RecoveryLockHash contains entry for lock no longer recorded by lock \
                     manager: xid {xid} database {db_oid} relation {rel_oid}"
                ),
            )?;
            debug_assert!(false);
        }
        st.entries.remove(&(xid, db_oid, rel_oid));
    }
    Ok(())
}

fn StandbyReleaseLocks(xid: TransactionId) -> PgResult<()> {
    if TransactionIdIsValid(xid) {
        RECOVERY_LOCKS.with(|s| {
            let mut st = s.borrow_mut();
            let st = st.as_mut().expect("StandbyReleaseLocks: recovery lock tables not active");
            if let Some(chain) = st.by_xid.remove(&xid) {
                release_xid_entry_locks(st, xid, &chain)?;
            }
            Ok(())
        })
    } else {
        StandbyReleaseAllLocks()
    }
}

pub fn StandbyReleaseLockTree(xid: TransactionId, subxids: &[TransactionId]) -> PgResult<()> {
    StandbyReleaseLocks(xid)?;
    for &subxid in subxids {
        StandbyReleaseLocks(subxid)?;
    }
    Ok(())
}

pub fn StandbyReleaseAllLocks() -> PgResult<()> {
    elog(DEBUG2, "release all standby locks")?;
    RECOVERY_LOCKS.with(|s| {
        let mut st = s.borrow_mut();
        let st = st.as_mut().expect("StandbyReleaseAllLocks: recovery lock tables not active");
        let xids: Vec<TransactionId> = st.by_xid.keys().copied().collect();
        for xid in xids {
            let chain = st.by_xid.remove(&xid).unwrap();
            release_xid_entry_locks(st, xid, &chain)?;
        }
        Ok(())
    })
}

pub fn StandbyReleaseOldLocks(oldxid: TransactionId) -> PgResult<()> {
    RECOVERY_LOCKS.with(|s| {
        let mut st = s.borrow_mut();
        let st = st.as_mut().expect("StandbyReleaseOldLocks: recovery lock tables not active");
        let xids: Vec<TransactionId> = st.by_xid.keys().copied().collect();
        for xid in xids {
            debug_assert!(TransactionIdIsValid(xid));
            if twophase_seams::standby_transaction_id_is_prepared::call(xid)? {
                continue;
            }
            if !TransactionIdPrecedes(xid, oldxid) {
                continue;
            }
            let chain = st.by_xid.remove(&xid).unwrap();
            release_xid_entry_locks(st, xid, &chain)?;
        }
        Ok(())
    })
}

fn get_recovery_conflict_desc(reason: ProcSignalReason) -> &'static str {
    match reason {
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => "recovery conflict on buffer pin",
        PROCSIG_RECOVERY_CONFLICT_LOCK => "recovery conflict on lock",
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE => "recovery conflict on tablespace",
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => "recovery conflict on snapshot",
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => "recovery conflict on replication slot",
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => "recovery conflict on buffer deadlock",
        PROCSIG_RECOVERY_CONFLICT_DATABASE => "recovery conflict on database",
        _ => "unknown reason",
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub fn recovery_lock_table_counts() -> (usize, usize) {
        RECOVERY_LOCKS.with(|s| {
            let st = s.borrow();
            let st = st.as_ref().expect("recovery lock tables not initialized");
            (st.entries.len(), st.by_xid.len())
        })
    }

    pub fn init_lock_tables_only() {
        RECOVERY_LOCKS.with(|s| {
            *s.borrow_mut() = Some(new_lock_state());
        });
    }

    pub fn insert_entry(xid: TransactionId, db_oid: Oid, rel_oid: Oid) -> bool {
        RECOVERY_LOCKS.with(|s| {
            s.borrow_mut().as_mut().unwrap().insert_entry(xid, db_oid, rel_oid)
        })
    }

    pub fn chain(xid: TransactionId) -> Vec<(Oid, Oid)> {
        RECOVERY_LOCKS.with(|s| {
            s.borrow()
                .as_ref()
                .unwrap()
                .by_xid
                .get(&xid)
                .map(|v| v.iter().copied().collect())
                .unwrap_or_default()
        })
    }

    pub fn standby_limit_time() -> TimestampTz {
        GetStandbyLimitTime()
    }

    pub fn set_delay_gucs(archive_ms: i32, streaming_ms: i32) {
        MAX_STANDBY_ARCHIVE_DELAY.store(archive_ms, Relaxed);
        MAX_STANDBY_STREAMING_DELAY.store(streaming_ms, Relaxed);
    }
}
