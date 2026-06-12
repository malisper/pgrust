//! Port of `src/backend/storage/ipc/standby.c` (PostgreSQL 18.3): misc
//! functions used in Hot Standby mode — handling of `RM_STANDBY_ID`
//! (AccessExclusiveLocks and starting snapshots for Hot Standby), plus
//! recovery-conflict processing.
//!
//! The two recovery-lock dynahash tables are process-local in C (the Startup
//! process; `hash_create` without `HASH_SHARED_MEM`), so they are owned
//! `HashMap`s in a thread-local here. The `sig_atomic_t` timeout flags and
//! the GUC globals are likewise per-backend state, kept in thread-locals.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use backend_utils_error::{elog, ereport};
use mcx::Mcx;
use types_core::xact::{
    FullTransactionId, InvalidTransactionId, MaxTransactionId, TransactionIdIsNormal,
    TransactionIdIsValid,
};
use types_core::{InvalidOid, Oid, OidIsValid, TimestampTz, TransactionId, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG2, DEBUG4, ERRCODE_T_R_DEADLOCK_DETECTED, LOG, PANIC,
};
use types_storage::{
    xl_standby_lock, AccessExclusiveLock, ProcSignalReason, RelFileLocator,
    RunningTransactionsData, SharedInvalidationMessage, VirtualTransactionId, DEFAULT_LOCKMETHOD,
    LOCKTAG, LOCKTAG_RELATION, LWLOCK_PROC_ARRAY, LWLOCK_XID_GEN,
    SHARED_INVALIDATION_MESSAGE_SIZE, SUBXIDS_IN_ARRAY, SUBXIDS_MISSING,
};
use types_timeout::{EnableTimeoutParams, TimeoutId, TimeoutType};
use types_wal::{
    RM_STANDBY_ID, RS_INVAL_HORIZON, STANDBY_DISABLED, STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING, WAL_LEVEL_LOGICAL, WAL_LEVEL_REPLICA, XLOG_MARK_UNIMPORTANT,
    XLR_INFO_MASK,
};

use backend_access_transam_transam_seams as transam;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_access_transam_xlogutils_seams as xlogutils;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_ipc_sinval_seams as sinval;
use backend_storage_lmgr_lock_seams as lock;
use backend_storage_lmgr_proc_seams as proc;
use backend_utils_init_small_seams as globals;
use backend_utils_misc_ps_status_seams as ps_status;

// ---------------------------------------------------------------------------
// standbydefs.h — XLOG message types and record shapes for RM_STANDBY_ID.
// ---------------------------------------------------------------------------

/// `XLOG_STANDBY_LOCK` (standbydefs.h).
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
/// `XLOG_RUNNING_XACTS` (standbydefs.h).
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
/// `XLOG_INVALIDATIONS` (standbydefs.h).
pub const XLOG_INVALIDATIONS: u8 = 0x20;

/// `xl_standby_locks` (standbydefs.h): `nlocks` plus the flexible lock array,
/// as an owned `Vec`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct XlStandbyLocks {
    pub locks: Vec<xl_standby_lock>,
}

/// `xl_running_xacts` (standbydefs.h): the running-xact snapshot in WAL.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct XlRunningXacts {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_overflow: bool,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    /// Length `xcnt + subxcnt`.
    pub xids: Vec<TransactionId>,
}

/// `xl_invalidations` (standbydefs.h).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct XlInvalidations {
    pub dbId: Oid,
    pub tsId: Oid,
    pub relcacheInitFileInval: bool,
    pub msgs: Vec<SharedInvalidationMessage>,
}

/// `MinSizeOfXactRunningXacts` — `offsetof(xl_running_xacts, xids)`: xcnt(4) +
/// subxcnt(4) + subxid_overflow(1)+pad(3) + nextXid(4) + oldestRunningXid(4) +
/// latestCompletedXid(4).
const MIN_SIZE_OF_XACT_RUNNING_XACTS: usize = 24;

/// `MinSizeOfInvalidations` — `offsetof(xl_invalidations, msgs)`: dbId(4) +
/// tsId(4) + relcacheInitFileInval(1)+pad(3) + nmsgs(4).
const MIN_SIZE_OF_INVALIDATIONS: usize = 16;

/// `offsetof(xl_standby_locks, locks)` — just the `int nlocks`.
const OFFSETOF_XL_STANDBY_LOCKS_LOCKS: usize = 4;

/// `sizeof(xl_standby_lock)` — three unpadded 4-byte fields.
const SIZEOF_XL_STANDBY_LOCK: usize = 12;

// ---------------------------------------------------------------------------
// User-settable GUC parameters (standby.c globals; per-backend state).
// ---------------------------------------------------------------------------

thread_local! {
    /// `int max_standby_archive_delay = 30 * 1000;` (ms; -1 = wait forever).
    static MAX_STANDBY_ARCHIVE_DELAY: Cell<i32> = const { Cell::new(30 * 1000) };
    /// `int max_standby_streaming_delay = 30 * 1000;` (ms; -1 = wait forever).
    static MAX_STANDBY_STREAMING_DELAY: Cell<i32> = const { Cell::new(30 * 1000) };
    /// `bool log_recovery_conflict_waits = false;`
    static LOG_RECOVERY_CONFLICT_WAITS: Cell<bool> = const { Cell::new(false) };
}

pub fn max_standby_archive_delay() -> i32 {
    MAX_STANDBY_ARCHIVE_DELAY.get()
}
pub fn set_max_standby_archive_delay(value: i32) {
    MAX_STANDBY_ARCHIVE_DELAY.set(value);
}
pub fn max_standby_streaming_delay() -> i32 {
    MAX_STANDBY_STREAMING_DELAY.get()
}
pub fn set_max_standby_streaming_delay(value: i32) {
    MAX_STANDBY_STREAMING_DELAY.set(value);
}
pub fn log_recovery_conflict_waits() -> bool {
    LOG_RECOVERY_CONFLICT_WAITS.get()
}
pub fn set_log_recovery_conflict_waits(value: bool) {
    LOG_RECOVERY_CONFLICT_WAITS.set(value);
}

// ---------------------------------------------------------------------------
// Recovery-lock tracking (process-local dynahash tables in the Startup
// process).
// ---------------------------------------------------------------------------

/// The `xl_standby_lock` blob key of `RecoveryLockHash`.
type RecoveryLockKey = (TransactionId, Oid, Oid);

/// The two C dynahash tables. `RecoveryLockHash` keys every known held lock
/// (the de-dup set); `RecoveryLockXidHash` maps an xid to the chain of its
/// locks. The C singly-linked chain (head-prepend in acquire) is a `Vec` with
/// inserts at the front, preserving release order.
struct RecoveryLockState {
    lock_hash: HashMap<RecoveryLockKey, ()>,
    xid_hash: HashMap<TransactionId, Vec<RecoveryLockKey>>,
}

thread_local! {
    /// `RecoveryLockHash`/`RecoveryLockXidHash` — `NULL` until
    /// `InitRecoveryTransactionEnvironment`, destroyed by
    /// `ShutdownRecoveryTransactionEnvironment`.
    static RECOVERY_LOCKS: RefCell<Option<RecoveryLockState>> = const { RefCell::new(None) };
}

// Flags set by timeout handlers (`volatile sig_atomic_t` in C; per-backend).
thread_local! {
    static GOT_STANDBY_DEADLOCK_TIMEOUT: Cell<bool> = const { Cell::new(false) };
    static GOT_STANDBY_DELAY_TIMEOUT: Cell<bool> = const { Cell::new(false) };
    static GOT_STANDBY_LOCK_TIMEOUT: Cell<bool> = const { Cell::new(false) };
}

// ---------------------------------------------------------------------------
// Helpers mirroring C macros used in this unit.
// ---------------------------------------------------------------------------

/// `TimestampTzPlusMilliseconds(tz, ms)` (utils/timestamp.h macro).
#[inline]
fn timestamp_tz_plus_milliseconds(tz: TimestampTz, ms: i32) -> TimestampTz {
    tz + (ms as i64) * 1000
}

/// `SET_LOCKTAG_RELATION(tag, dboid, reloid)` (storage/lock.h macro).
fn set_locktag_relation(dboid: Oid, reloid: Oid) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dboid,
        locktag_field2: reloid,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_RELATION,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD as u8,
    }
}

/// `InHotStandby` (access/xlogutils.h macro) —
/// `standbyState >= STANDBY_SNAPSHOT_PENDING`.
fn in_hot_standby() -> bool {
    xlogutils::standby_state::call() >= STANDBY_SNAPSHOT_PENDING
}

/// `XLogStandbyInfoActive()` (access/xlog.h macro) —
/// `wal_level >= WAL_LEVEL_REPLICA`.
fn xlog_standby_info_active() -> bool {
    xlog::wal_level::call() >= WAL_LEVEL_REPLICA
}

#[inline]
fn here(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("standby.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// InitRecoveryTransactionEnvironment / ShutdownRecoveryTransactionEnvironment
// ---------------------------------------------------------------------------

/// `InitRecoveryTransactionEnvironment` — initialize tracking of the
/// primary's in-progress transactions.
///
/// We need to issue shared invalidations and hold locks. Holding locks means
/// others may want to wait on us, so we need to make a lock table vxact entry
/// like a real transaction; it is simpler to create one permanent entry and
/// leave it there all the time.
pub fn InitRecoveryTransactionEnvironment() -> PgResult<()> {
    RECOVERY_LOCKS.with(|cell| {
        let mut state = cell.borrow_mut();
        assert!(state.is_none(), "don't run this twice");
        *state = Some(RecoveryLockState {
            lock_hash: HashMap::new(),
            xid_hash: HashMap::new(),
        });
    });

    // Initialize shared invalidation management for the Startup process,
    // registering ourselves as a sendOnly process so we don't need to read
    // messages, nor get signaled when the queue starts filling up.
    sinval::shared_inval_backend_init::call(true)?;

    // Lock a virtual transaction id for the Startup process. We need
    // GetNextLocalTransactionId() because SharedInvalBackendInit() leaves
    // localTransactionId invalid and the lock manager doesn't like that.
    // We don't need XactLockTableInsert() because nobody needs to wait on
    // xids: table locks are held by vxids and row-level locks by xids.
    proc::set_my_proc_vxid_proc_number::call(globals::my_proc_number::call());
    let vxid = VirtualTransactionId {
        procNumber: globals::my_proc_number::call(),
        localTransactionId: sinval::get_next_local_transaction_id::call(),
    };
    lock::virtual_xact_lock_table_insert::call(vxid)?;

    xlogutils::set_standby_state::call(STANDBY_INITIALIZED);
    Ok(())
}

/// `ShutdownRecoveryTransactionEnvironment` — shut down recovery-time
/// transaction tracking. Must be called even in shutdown of the startup
/// process if transaction tracking has been initialized; possibly-redundant
/// calls during process exit are safe.
pub fn ShutdownRecoveryTransactionEnvironment() -> PgResult<()> {
    // Do nothing if RecoveryLockHash is NULL: tracking was never initialized
    // or has already been shut down.
    if RECOVERY_LOCKS.with(|cell| cell.borrow().is_none()) {
        return Ok(());
    }

    // Mark all tracked in-progress transactions as finished.
    procarray::expire_all_known_assigned_transaction_ids::call()?;

    // Release all locks the tracked transactions were holding.
    StandbyReleaseAllLocks();

    // Destroy the lock hash tables.
    RECOVERY_LOCKS.with(|cell| {
        *cell.borrow_mut() = None;
    });

    // Cleanup our VirtualTransaction.
    lock::virtual_xact_lock_table_cleanup::call()
}

// ---------------------------------------------------------------------------
// Standby wait timers and backend cancel logic
// ---------------------------------------------------------------------------

/// `GetStandbyLimitTime` — the cutoff time at which we want to start
/// canceling conflicting transactions: last WAL receipt time plus the
/// appropriate delay GUC. Returns zero (a time safely in the past) if we are
/// willing to wait forever (delay of -1).
fn GetStandbyLimitTime() -> TimestampTz {
    let (rtime, from_stream) = backend_access_transam_xlogrecovery_seams::get_xlog_receipt_time::call();
    if from_stream {
        let delay = max_standby_streaming_delay();
        if delay < 0 {
            0
        } else {
            timestamp_tz_plus_milliseconds(rtime, delay)
        }
    } else {
        let delay = max_standby_archive_delay();
        if delay < 0 {
            0
        } else {
            timestamp_tz_plus_milliseconds(rtime, delay)
        }
    }
}

const STANDBY_INITIAL_WAIT_US: i32 = 1000;

thread_local! {
    /// `static int standbyWait_us = STANDBY_INITIAL_WAIT_US;`
    static STANDBY_WAIT_US: Cell<i32> = const { Cell::new(STANDBY_INITIAL_WAIT_US) };
}

/// `WaitExceedsMaxStandbyDelay` — wait here for a while then return. Returns
/// `true` if we can't wait any more, `false` to wait some more.
fn WaitExceedsMaxStandbyDelay(wait_event_info: u32) -> PgResult<bool> {
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    // Are we past the limit time?
    let ltime = GetStandbyLimitTime();
    if ltime != 0 && backend_utils_adt_timestamp_seams::get_current_timestamp::call() >= ltime {
        return Ok(true);
    }

    // Sleep a bit (this is essential to avoid busy-waiting).
    backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    port_pgsleep_seams::pg_usleep::call(STANDBY_WAIT_US.get() as i64);
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();

    // Progressively increase the sleep times, but not to more than 1s, since
    // pg_usleep isn't interruptible on some platforms.
    let mut wait_us = STANDBY_WAIT_US.get() * 2;
    if wait_us > 1_000_000 {
        wait_us = 1_000_000;
    }
    STANDBY_WAIT_US.set(wait_us);

    Ok(false)
}

/// `LogRecoveryConflict` — log the recovery conflict.
///
/// `wait_start` is when the caller started to wait; `now` is when this
/// function is called. `wait_list` is the list of conflicting processes'
/// vxids; `still_waiting` indicates whether the startup process is still
/// waiting for the conflict to be resolved.
pub fn LogRecoveryConflict(
    reason: ProcSignalReason,
    wait_start: TimestampTz,
    now: TimestampTz,
    wait_list: Option<&[VirtualTransactionId]>,
    still_waiting: bool,
) -> PgResult<()> {
    // There must be no conflicting processes when the recovery conflict has
    // already been resolved.
    assert!(still_waiting || wait_list.is_none());

    let (secs, mut usecs) =
        backend_utils_adt_timestamp_seams::timestamp_difference::call(wait_start, now);
    let msecs = secs * 1000 + (usecs as i64) / 1000;
    usecs %= 1000;

    let mut nprocs: u64 = 0;
    let mut buf = String::new();

    if let Some(list) = wait_list {
        // Construct a string of the list of the conflicting processes.
        for vxid in list {
            if !vxid.is_valid() {
                break;
            }
            let pid = procarray::proc_number_get_proc_pid::call(vxid.procNumber);
            // proc can be NULL if the target backend is not active.
            if pid != 0 {
                if nprocs == 0 {
                    buf = format!("{pid}");
                } else {
                    buf.push_str(&format!(", {pid}"));
                }
                nprocs += 1;
            }
        }
    }

    // If wait_list is specified, report the PIDs of active conflicting
    // backends in a detail message; if none are active, no detail message.
    if still_waiting {
        let mut builder = ereport(LOG).errmsg(format!(
            "recovery still waiting after {msecs}.{usecs:03} ms: {}",
            get_recovery_conflict_desc(reason)
        ));
        if nprocs > 0 {
            builder = builder.errdetail_log_plural(
                format!("Conflicting process: {buf}."),
                format!("Conflicting processes: {buf}."),
                nprocs,
            );
        }
        builder.finish(here("LogRecoveryConflict"))
    } else {
        ereport(LOG)
            .errmsg(format!(
                "recovery finished waiting after {msecs}.{usecs:03} ms: {}",
                get_recovery_conflict_desc(reason)
            ))
            .finish(here("LogRecoveryConflict"))
    }
}

/// `ResolveRecoveryConflictWithVirtualXIDs` — the main executioner for any
/// query backend that conflicts with recovery processing. Judgement has
/// already been passed within a specific rmgr; here we just issue the orders
/// to the procs, which throw the required error as instructed.
///
/// If `report_waiting` is true, "waiting" is reported in the PS display and
/// the wait is reported in the log if necessary; pass false when the caller
/// is responsible for that reporting.
fn ResolveRecoveryConflictWithVirtualXIDs(
    waitlist: &[VirtualTransactionId],
    reason: ProcSignalReason,
    wait_event_info: u32,
    report_waiting: bool,
) -> PgResult<()> {
    let mut wait_start: TimestampTz = 0;
    let mut waiting = false;
    let mut logged_recovery_conflict = false;

    // Fast exit, to avoid a kernel call if there's no work to be done.
    if waitlist.is_empty() || !waitlist[0].is_valid() {
        return Ok(());
    }

    // Set the wait start timestamp for reporting.
    if report_waiting
        && (log_recovery_conflict_waits() || ps_status::update_process_title::call())
    {
        wait_start = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    }

    let mut idx = 0;
    while idx < waitlist.len() && waitlist[idx].is_valid() {
        let vxid = waitlist[idx];

        // reset standbyWait_us for each xact we wait for
        STANDBY_WAIT_US.set(STANDBY_INITIAL_WAIT_US);

        // wait until the virtual xid is gone
        while !lock::virtual_xact_lock::call(vxid, false)? {
            // Is it time to kill it?
            if WaitExceedsMaxStandbyDelay(wait_event_info)? {
                // Now find out who to throw out of the balloon.
                assert!(vxid.is_valid());
                let pid = procarray::cancel_virtual_transaction::call(vxid, reason)?;

                // Wait a little bit for it to die so that we avoid flooding
                // an unresponsive backend when system is heavily loaded.
                if pid != 0 {
                    port_pgsleep_seams::pg_usleep::call(5000);
                }
            }

            if wait_start != 0 && (!logged_recovery_conflict || !waiting) {
                let mut now: TimestampTz = 0;
                let maybe_log_conflict =
                    log_recovery_conflict_waits() && !logged_recovery_conflict;
                let maybe_update_title = ps_status::update_process_title::call() && !waiting;

                // Get the current timestamp if not reported yet.
                if maybe_log_conflict || maybe_update_title {
                    now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
                }

                // Report via ps if we have been waiting for more than 500
                // msec (should that be configurable?)
                if maybe_update_title
                    && backend_utils_adt_timestamp_seams::timestamp_difference_exceeds::call(
                        wait_start, now, 500,
                    )
                {
                    ps_status::set_ps_display_suffix::call("waiting");
                    waiting = true;
                }

                // Emit the log message if the startup process is waiting
                // longer than deadlock_timeout for the recovery conflict.
                if maybe_log_conflict
                    && backend_utils_adt_timestamp_seams::timestamp_difference_exceeds::call(
                        wait_start,
                        now,
                        proc::deadlock_timeout::call(),
                    )
                {
                    LogRecoveryConflict(reason, wait_start, now, Some(&waitlist[idx..]), true)?;
                    logged_recovery_conflict = true;
                }
            }
        }

        // The virtual transaction is gone now, wait for the next one.
        idx += 1;
    }

    // Emit the log message if the recovery conflict was resolved but the
    // startup process waited longer than deadlock_timeout for it.
    if logged_recovery_conflict {
        LogRecoveryConflict(
            reason,
            wait_start,
            backend_utils_adt_timestamp_seams::get_current_timestamp::call(),
            None,
            false,
        )?;
    }

    // Reset ps display to remove the suffix if we added one.
    if waiting {
        ps_status::set_ps_display_remove_suffix::call();
    }

    Ok(())
}

/// `ResolveRecoveryConflictWithSnapshot` — generate whatever recovery
/// conflicts are needed to eliminate snapshots that might see XIDs <=
/// `snapshotConflictHorizon` as still running. `InvalidTransactionId` means
/// "definitely don't need any conflicts" (common when replaying
/// already-applied WAL after a standby crash, XLOG_HEAP2_VISIBLE on an
/// already-all-visible page, or index-deletion records).
pub fn ResolveRecoveryConflictWithSnapshot(
    snapshotConflictHorizon: TransactionId,
    isCatalogRel: bool,
    locator: RelFileLocator,
) -> PgResult<()> {
    if !TransactionIdIsValid(snapshotConflictHorizon) {
        return Ok(());
    }

    assert!(TransactionIdIsNormal(snapshotConflictHorizon));
    let backends =
        procarray::get_conflicting_virtual_xids::call(snapshotConflictHorizon, locator.dbOid)?;
    ResolveRecoveryConflictWithVirtualXIDs(
        &backends,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
        true,
    )?;

    // WaitExceedsMaxStandbyDelay() is not taken into account here; this kind
    // of conflict should not normally be reached, e.g. due to using a
    // physical replication slot.
    if xlog::wal_level::call() >= WAL_LEVEL_LOGICAL && isCatalogRel {
        backend_replication_slot_seams::invalidate_obsolete_replication_slots::call(
            RS_INVAL_HORIZON,
            0,
            locator.dbOid,
            snapshotConflictHorizon,
        )?;
    }
    Ok(())
}

/// `ResolveRecoveryConflictWithSnapshotFullXid` — variant working with
/// `FullTransactionId` values: truncate the logged FullTransactionId to a
/// 32-bit xid; if the value is so old that XID wrap-around already happened
/// on it, there can't be any snapshots that still see it.
pub fn ResolveRecoveryConflictWithSnapshotFullXid(
    snapshotConflictHorizon: FullTransactionId,
    isCatalogRel: bool,
    locator: RelFileLocator,
) -> PgResult<()> {
    let next_xid = backend_access_transam_varsup_seams::read_next_full_transaction_id::call();
    let diff = next_xid.value.wrapping_sub(snapshotConflictHorizon.value);
    if diff < (MaxTransactionId as u64) / 2 {
        let truncated = snapshotConflictHorizon.xid();
        ResolveRecoveryConflictWithSnapshot(truncated, isCatalogRel, locator)?;
    }
    Ok(())
}

/// `ResolveRecoveryConflictWithTablespace` — standby users may currently be
/// using this tablespace for their temporary files; ask everybody to cancel
/// their queries immediately so the tablespace can be removed.
pub fn ResolveRecoveryConflictWithTablespace(_tsid: Oid) -> PgResult<()> {
    let temp_file_users =
        procarray::get_conflicting_virtual_xids::call(InvalidTransactionId, InvalidOid)?;
    ResolveRecoveryConflictWithVirtualXIDs(
        &temp_file_users,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
        true,
    )
}

/// `ResolveRecoveryConflictWithDatabase` — no wait, just force conflicting
/// backends off immediately; completely idle sessions would otherwise block
/// us, and AccessExclusiveLock is already held.
pub fn ResolveRecoveryConflictWithDatabase(dbid: Oid) -> PgResult<()> {
    while procarray::count_db_backends::call(dbid)? > 0 {
        procarray::cancel_db_backends::call(
            dbid,
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE,
            true,
        )?;

        // Wait awhile for them to die so that we avoid flooding an
        // unresponsive backend when system is heavily loaded.
        port_pgsleep_seams::pg_usleep::call(10000);
    }
    Ok(())
}

/// `ResolveRecoveryConflictWithLock` — called from `ProcSleep()` to resolve
/// conflicts with other backends holding relation locks. Either resolves
/// conflicts immediately or sets a timeout to wake us at the limit of our
/// patience; also checks for deadlocks involving the Startup process once
/// deadlock_timeout is reached.
///
/// `logging_conflict` should be true if the recovery conflict has not been
/// logged yet even though logging is enabled; in that case, after the
/// deadlock-check request is sent we return without waiting again so the
/// caller can log, and are called again with `logging_conflict = false`.
pub fn ResolveRecoveryConflictWithLock(locktag: LOCKTAG, logging_conflict: bool) -> PgResult<()> {
    assert!(in_hot_standby());

    let ltime = GetStandbyLimitTime();
    let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();

    // Update waitStart if first time through after the startup process
    // started waiting for the lock (not on every call). waitStart is updated
    // without holding the lock table's partition lock, so "waitstart" in
    // pg_locks can be NULL for a very short period even though "granted" is
    // false; that is OK in practice.
    if proc::my_proc_wait_start::call() == 0 {
        proc::set_my_proc_wait_start::call(now);
    }

    if now >= ltime && ltime != 0 {
        // We're already behind, so clear a path as quickly as possible.
        let backends = lock::get_lock_conflicts::call(&locktag, AccessExclusiveLock)?;

        // Prevent ResolveRecoveryConflictWithVirtualXIDs() from reporting
        // "waiting" in PS display; the caller, WaitOnLock(), has already
        // reported that.
        ResolveRecoveryConflictWithVirtualXIDs(
            &backends,
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
            PG_WAIT_LOCK | locktag.locktag_type as u32,
            false,
        )?;
    } else {
        // Wait (or wait again) until ltime, and check for deadlocks as well
        // if we will be waiting longer than deadlock_timeout.
        let mut timeouts: Vec<EnableTimeoutParams> = Vec::with_capacity(2);

        if ltime != 0 {
            GOT_STANDBY_LOCK_TIMEOUT.set(false);
            timeouts.push(EnableTimeoutParams {
                id: TimeoutId::STANDBY_LOCK_TIMEOUT,
                r#type: TimeoutType::TMPARAM_AT,
                delay_ms: 0,
                fin_time: ltime,
            });
        }

        GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
        timeouts.push(EnableTimeoutParams {
            id: TimeoutId::STANDBY_DEADLOCK_TIMEOUT,
            r#type: TimeoutType::TMPARAM_AFTER,
            delay_ms: proc::deadlock_timeout::call(),
            fin_time: 0,
        });

        backend_utils_misc_timeout_seams::enable_timeouts::call(&timeouts)?;
    }

    // Wait to be signaled by the release of the Relation Lock.
    proc::proc_wait_for_signal::call(PG_WAIT_LOCK | locktag.locktag_type as u32)?;

    'cleanup: {
        // Exit if ltime is reached: all the backends holding conflicting
        // locks will be canceled in the next call.
        if GOT_STANDBY_LOCK_TIMEOUT.get() {
            break 'cleanup;
        }

        if GOT_STANDBY_DEADLOCK_TIMEOUT.get() {
            let backends = lock::get_lock_conflicts::call(&locktag, AccessExclusiveLock)?;

            // Quick exit if there's no work to be done.
            if backends.is_empty() || !backends[0].is_valid() {
                break 'cleanup;
            }

            // Send signals to all the backends holding the conflicting
            // locks, to ask them to check themselves for deadlocks.
            for vxid in &backends {
                if !vxid.is_valid() {
                    break;
                }
                procarray::signal_virtual_transaction::call(
                    *vxid,
                    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
                    false,
                )?;
            }

            // Exit if the recovery conflict has not been logged yet even
            // though logging is enabled, so that the caller can log it; we
            // will be called again and wait again for the lock release.
            if logging_conflict {
                break 'cleanup;
            }

            // Wait again to be signaled by the release of the Relation Lock,
            // to prevent the subsequent call from hitting deadlock_timeout
            // and re-sending a deadlock-check request immediately.
            GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
            proc::proc_wait_for_signal::call(PG_WAIT_LOCK | locktag.locktag_type as u32)?;
        }
    }

    // cleanup: clear any timeout requests established above. We assume the
    // Startup process has no other outstanding timeouts than those used here.
    backend_utils_misc_timeout_seams::disable_all_timeouts::call(false)?;
    GOT_STANDBY_LOCK_TIMEOUT.set(false);
    GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
    Ok(())
}

/// `ResolveRecoveryConflictWithBufferPin` — called from
/// `LockBufferForCleanup()` to resolve conflicts with other backends holding
/// buffer pins. Resolve conflicts by sending a PROCSIG signal to all backends
/// asking whether they hold the pin blocking the Startup process; the
/// innocent take no action, the guilty ERROR or FATAL themselves.
///
/// Deadlocks (query waits on a lock behind an AccessExclusiveLock while
/// Startup waits on the pin) are rare and relatively expensive to check for,
/// so the check only runs after deadlock_timeout. The reverse sequence is
/// checked before the query sleeps, in `CheckRecoveryConflictDeadlock()`.
pub fn ResolveRecoveryConflictWithBufferPin() -> PgResult<()> {
    assert!(in_hot_standby());

    let ltime = GetStandbyLimitTime();

    if backend_utils_adt_timestamp_seams::get_current_timestamp::call() >= ltime && ltime != 0 {
        // We're already behind, so clear a path as quickly as possible.
        SendRecoveryConflictWithBufferPin(
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
        )?;
    } else {
        // Wake up at ltime, and check for deadlocks as well if we will be
        // waiting longer than deadlock_timeout.
        let mut timeouts: Vec<EnableTimeoutParams> = Vec::with_capacity(2);

        if ltime != 0 {
            timeouts.push(EnableTimeoutParams {
                id: TimeoutId::STANDBY_TIMEOUT,
                r#type: TimeoutType::TMPARAM_AT,
                delay_ms: 0,
                fin_time: ltime,
            });
        }

        GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
        timeouts.push(EnableTimeoutParams {
            id: TimeoutId::STANDBY_DEADLOCK_TIMEOUT,
            r#type: TimeoutType::TMPARAM_AFTER,
            delay_ms: proc::deadlock_timeout::call(),
            fin_time: 0,
        });

        backend_utils_misc_timeout_seams::enable_timeouts::call(&timeouts)?;
    }

    // Wait to be signaled by UnpinBuffer() or for the wait to be interrupted
    // by one of the timeouts established above. Only UnpinBuffer() and those
    // timeouts can wake us here; WakeupRecovery() uses a different latch.
    proc::proc_wait_for_signal::call(WAIT_EVENT_BUFFER_PIN)?;

    if GOT_STANDBY_DELAY_TIMEOUT.get() {
        SendRecoveryConflictWithBufferPin(
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
        )?;
    } else if GOT_STANDBY_DEADLOCK_TIMEOUT.get() {
        // Send out a request for hot-standby backends to check themselves
        // for deadlocks. The subsequent call will wait again and re-send the
        // request every deadlock_timeout until the buffer is unpinned or
        // ltime is reached, which mirrors the C behavior (and its XXX note).
        SendRecoveryConflictWithBufferPin(
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
        )?;
    }

    // Clear any timeout requests established above. We assume here that the
    // Startup process doesn't have any other timeouts than what this function
    // uses.
    backend_utils_misc_timeout_seams::disable_all_timeouts::call(false)?;
    GOT_STANDBY_DELAY_TIMEOUT.set(false);
    GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
    Ok(())
}

/// `SendRecoveryConflictWithBufferPin` — signal all backends to ask whether
/// they hold the buffer pin delaying the Startup process. The conflict flag
/// must not be set yet, since most backends will be innocent; the SIGUSR1
/// handling in each backend decides its own fate.
fn SendRecoveryConflictWithBufferPin(reason: ProcSignalReason) -> PgResult<()> {
    assert!(
        reason == ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
            || reason == ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
    );
    procarray::cancel_db_backends::call(InvalidOid, reason, false)
}

/// `CheckRecoveryConflictDeadlock` — in Hot Standby perform early deadlock
/// detection: abort the lock wait (ERROR, as `Err`) if we are about to sleep
/// while holding the buffer pin the Startup process is waiting for. This is
/// pessimistic: the lock we wait for might be unrelated to any held by the
/// Startup process. Only the current transaction is canceled, so if we are
/// in a subtransaction and the pin is held by a parent the Startup process
/// will continue to wait even though we avoided deadlock.
pub fn CheckRecoveryConflictDeadlock() -> PgResult<()> {
    assert!(!xlog::in_recovery::call()); // do not call in Startup process

    if !backend_storage_buffer_bufmgr_seams::holding_buffer_pin_that_delays_recovery::call() {
        return Ok(());
    }

    // Error message should match ProcessInterrupts(), but we avoid calling
    // that because we aren't handling an interrupt at this point.
    Err(
        PgError::error("canceling statement due to conflict with recovery")
            .with_sqlstate(ERRCODE_T_R_DEADLOCK_DETECTED)
            .with_detail("User transaction caused buffer deadlock with recovery."),
    )
}

// ---------------------------------------------------------------------------
// timeout handler routines
// ---------------------------------------------------------------------------

/// `StandbyDeadLockHandler` — called if STANDBY_DEADLOCK_TIMEOUT is exceeded.
pub fn StandbyDeadLockHandler() {
    GOT_STANDBY_DEADLOCK_TIMEOUT.set(true);
}

/// `StandbyTimeoutHandler` — called if STANDBY_TIMEOUT is exceeded.
pub fn StandbyTimeoutHandler() {
    GOT_STANDBY_DELAY_TIMEOUT.set(true);
}

/// `StandbyLockTimeoutHandler` — called if STANDBY_LOCK_TIMEOUT is exceeded.
pub fn StandbyLockTimeoutHandler() {
    GOT_STANDBY_LOCK_TIMEOUT.set(true);
}

// ---------------------------------------------------------------------------
// Locking in Recovery Mode
//
// All locks are held by the Startup process using a single virtual
// transaction; the Startup process is the proxy by which the original locks
// are implemented. We only track AccessExclusiveLocks, which are only ever
// held by one transaction on one relation. Session locks are used rather
// than normal locks so we don't need ResourceOwners.
// ---------------------------------------------------------------------------

/// `StandbyAcquireAccessExclusiveLock(xid, dbOid, relOid)`.
pub fn StandbyAcquireAccessExclusiveLock(
    xid: TransactionId,
    dbOid: Oid,
    relOid: Oid,
) -> PgResult<()> {
    // Already processed?
    if !TransactionIdIsValid(xid)
        || transam::transaction_id_did_commit::call(xid)?
        || transam::transaction_id_did_abort::call(xid)?
    {
        return Ok(());
    }

    let _ = elog(DEBUG4, format!("adding recovery lock: db {dbOid} rel {relOid}"));

    // dbOid is InvalidOid when we are locking a shared relation.
    assert!(OidIsValid(relOid));

    let key: RecoveryLockKey = (xid, dbOid, relOid);
    let acquire = RECOVERY_LOCKS.with(|cell| {
        let mut state = cell.borrow_mut();
        let state = state.as_mut().expect("RecoveryLockHash not initialized");

        // Create a hash entry for this xid, if we don't have one already.
        state.xid_hash.entry(xid).or_default();

        // Create a hash entry for this lock, unless we have one already.
        if state.lock_hash.contains_key(&key) {
            false
        } else {
            state.lock_hash.insert(key, ());
            // It's new, so link it into the XID's list (head prepend) ...
            state.xid_hash.get_mut(&xid).unwrap().insert(0, key);
            true
        }
    });

    if acquire {
        // ... and acquire the lock locally.
        let locktag = set_locktag_relation(dbOid, relOid);
        lock::lock_acquire::call(&locktag, AccessExclusiveLock, true, false)?;
    }
    Ok(())
}

/// `StandbyReleaseXidEntryLocks` — release all the locks associated with one
/// `RecoveryLockXidEntry` chain.
fn StandbyReleaseXidEntryLocks(chain: &[RecoveryLockKey]) {
    for &(xid, db_oid, rel_oid) in chain {
        let _ = elog(
            DEBUG4,
            format!("releasing recovery lock: xid {xid} db {db_oid} rel {rel_oid}"),
        );
        // Release the lock ...
        let locktag = set_locktag_relation(db_oid, rel_oid);
        if !lock::lock_release::call(&locktag, AccessExclusiveLock, true) {
            let _ = elog(
                LOG,
                format!(
                    "RecoveryLockHash contains entry for lock no longer recorded by lock manager: xid {xid} database {db_oid} relation {rel_oid}"
                ),
            );
            debug_assert!(false);
        }
        // ... and remove the per-lock hash entry.
        RECOVERY_LOCKS.with(|cell| {
            if let Some(state) = cell.borrow_mut().as_mut() {
                state.lock_hash.remove(&(xid, db_oid, rel_oid));
            }
        });
    }
}

/// `StandbyReleaseLocks` — release locks for a specific XID, or all locks if
/// it's InvalidXid.
fn StandbyReleaseLocks(xid: TransactionId) {
    if TransactionIdIsValid(xid) {
        let chain = RECOVERY_LOCKS.with(|cell| {
            cell.borrow_mut()
                .as_mut()
                .and_then(|state| state.xid_hash.remove(&xid))
        });
        if let Some(chain) = chain {
            StandbyReleaseXidEntryLocks(&chain);
        }
    } else {
        StandbyReleaseAllLocks();
    }
}

/// `StandbyReleaseLockTree(xid, nsubxids, subxids)` — release locks for a
/// transaction tree, starting at xid down. Called during WAL replay of
/// COMMIT/ROLLBACK when in hot standby mode.
pub fn StandbyReleaseLockTree(xid: TransactionId, subxids: &[TransactionId]) {
    StandbyReleaseLocks(xid);
    for &subxid in subxids {
        StandbyReleaseLocks(subxid);
    }
}

/// `StandbyReleaseAllLocks` — called at end of recovery and when we see a
/// shutdown checkpoint.
pub fn StandbyReleaseAllLocks() {
    let _ = elog(DEBUG2, "release all standby locks");

    let entries: Vec<Vec<RecoveryLockKey>> = RECOVERY_LOCKS.with(|cell| {
        cell.borrow_mut()
            .as_mut()
            .map(|state| state.xid_hash.drain().map(|(_, chain)| chain).collect())
            .unwrap_or_default()
    });
    for chain in entries {
        StandbyReleaseXidEntryLocks(&chain);
    }
}

/// `StandbyReleaseOldLocks(oldxid)` — release standby locks held by top-level
/// XIDs that aren't running, as long as they're not prepared transactions.
/// Needed to prune the locks of crashed transactions, which didn't write an
/// ABORT/COMMIT record.
pub fn StandbyReleaseOldLocks(oldxid: TransactionId) -> PgResult<()> {
    // Snapshot the candidate xids; the C hash_seq scan tolerates deletion of
    // the current entry only, which removing per-xid below matches.
    let candidates: Vec<TransactionId> = RECOVERY_LOCKS.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|state| state.xid_hash.keys().copied().collect())
            .unwrap_or_default()
    });

    for xid in candidates {
        assert!(TransactionIdIsValid(xid));

        // Skip if prepared transaction.
        if backend_access_transam_twophase_seams::standby_transaction_id_is_prepared::call(xid)? {
            continue;
        }

        // Skip if >= oldxid.
        if !transam::transaction_id_precedes::call(xid, oldxid) {
            continue;
        }

        // Remove all locks and hash table entry.
        let chain = RECOVERY_LOCKS.with(|cell| {
            cell.borrow_mut()
                .as_mut()
                .and_then(|state| state.xid_hash.remove(&xid))
        });
        if let Some(chain) = chain {
            StandbyReleaseXidEntryLocks(&chain);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery handling for Rmgr RM_STANDBY_ID
//
// These record types will only be created if XLogStandbyInfoActive().
// ---------------------------------------------------------------------------

/// Parse an `xl_standby_locks` record body.
fn parse_xl_standby_locks(data: &[u8]) -> PgResult<XlStandbyLocks> {
    let nlocks = i32::from_ne_bytes(slice4(data, 0)?) as usize;
    let mut locks = Vec::new();
    locks
        .try_reserve_exact(nlocks)
        .map_err(|_| PgError::error("out of memory"))?;
    for i in 0..nlocks {
        let off = OFFSETOF_XL_STANDBY_LOCKS_LOCKS + i * SIZEOF_XL_STANDBY_LOCK;
        locks.push(xl_standby_lock {
            xid: u32::from_ne_bytes(slice4(data, off)?),
            dbOid: u32::from_ne_bytes(slice4(data, off + 4)?),
            relOid: u32::from_ne_bytes(slice4(data, off + 8)?),
        });
    }
    Ok(XlStandbyLocks { locks })
}

/// Parse an `xl_running_xacts` record body.
fn parse_xl_running_xacts(data: &[u8]) -> PgResult<XlRunningXacts> {
    let xcnt = i32::from_ne_bytes(slice4(data, 0)?);
    let subxcnt = i32::from_ne_bytes(slice4(data, 4)?);
    let subxid_overflow = *data
        .get(8)
        .ok_or_else(|| PgError::error("invalid xl_running_xacts record"))?
        != 0;
    let nextXid = u32::from_ne_bytes(slice4(data, 12)?);
    let oldestRunningXid = u32::from_ne_bytes(slice4(data, 16)?);
    let latestCompletedXid = u32::from_ne_bytes(slice4(data, 20)?);

    let nxids = (xcnt + subxcnt) as usize;
    let mut xids = Vec::new();
    xids.try_reserve_exact(nxids)
        .map_err(|_| PgError::error("out of memory"))?;
    for i in 0..nxids {
        xids.push(u32::from_ne_bytes(slice4(
            data,
            MIN_SIZE_OF_XACT_RUNNING_XACTS + i * 4,
        )?));
    }
    Ok(XlRunningXacts {
        xcnt,
        subxcnt,
        subxid_overflow,
        nextXid,
        oldestRunningXid,
        latestCompletedXid,
        xids,
    })
}

/// Parse an `xl_invalidations` record body.
fn parse_xl_invalidations(data: &[u8]) -> PgResult<XlInvalidations> {
    let dbId = u32::from_ne_bytes(slice4(data, 0)?);
    let tsId = u32::from_ne_bytes(slice4(data, 4)?);
    let relcacheInitFileInval = *data
        .get(8)
        .ok_or_else(|| PgError::error("invalid xl_invalidations record"))?
        != 0;
    let nmsgs = i32::from_ne_bytes(slice4(data, 12)?) as usize;

    let mut msgs = Vec::new();
    msgs.try_reserve_exact(nmsgs)
        .map_err(|_| PgError::error("out of memory"))?;
    for i in 0..nmsgs {
        let off = MIN_SIZE_OF_INVALIDATIONS + i * SHARED_INVALIDATION_MESSAGE_SIZE;
        let raw: [u8; SHARED_INVALIDATION_MESSAGE_SIZE] = data
            .get(off..off + SHARED_INVALIDATION_MESSAGE_SIZE)
            .ok_or_else(|| PgError::error("invalid xl_invalidations record"))?
            .try_into()
            .expect("slice length checked");
        msgs.push(SharedInvalidationMessage { raw });
    }
    Ok(XlInvalidations {
        dbId,
        tsId,
        relcacheInitFileInval,
        msgs,
    })
}

fn slice4(data: &[u8], off: usize) -> PgResult<[u8; 4]> {
    data.get(off..off + 4)
        .map(|s| s.try_into().expect("slice length checked"))
        .ok_or_else(|| PgError::error("invalid RM_STANDBY_ID record: too short"))
}

/// `standby_redo(record)` — replay of an `RM_STANDBY_ID` record. `info` is
/// the raw `XLogRecGetInfo` byte, `data` is `XLogRecGetData`, and
/// `has_any_block_refs` is `XLogRecHasAnyBlockRefs`.
pub fn standby_redo(info: u8, data: &[u8], has_any_block_refs: bool) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    // Backup blocks are not used in standby records.
    assert!(!has_any_block_refs);

    // Do nothing if we're not in hot standby mode.
    if xlogutils::standby_state::call() == STANDBY_DISABLED {
        return Ok(());
    }

    if info == XLOG_STANDBY_LOCK {
        let xlrec = parse_xl_standby_locks(data)?;
        for l in &xlrec.locks {
            StandbyAcquireAccessExclusiveLock(l.xid, l.dbOid, l.relOid)?;
        }
    } else if info == XLOG_RUNNING_XACTS {
        let xlrec = parse_xl_running_xacts(data)?;
        let running = RunningTransactionsData {
            xcnt: xlrec.xcnt,
            subxcnt: xlrec.subxcnt,
            subxid_status: if xlrec.subxid_overflow {
                SUBXIDS_MISSING
            } else {
                SUBXIDS_IN_ARRAY
            },
            nextXid: xlrec.nextXid,
            oldestRunningXid: xlrec.oldestRunningXid,
            // Not set by the C standby_redo (only used elsewhere).
            oldestDatabaseRunningXid: InvalidTransactionId,
            latestCompletedXid: xlrec.latestCompletedXid,
            xids: xlrec.xids,
        };

        procarray::proc_array_apply_recovery_info::call(&running)?;

        // The startup process currently has no convenient way to schedule
        // stats to be reported; XLOG_RUNNING_XACTS records issue at a regular
        // cadence, making this a convenient location to report stats.
        backend_utils_activity_pgstat_seams::pgstat_report_stat::call(true)?;
    } else if info == XLOG_INVALIDATIONS {
        let xlrec = parse_xl_invalidations(data)?;
        backend_utils_cache_inval_seams::process_committed_invalidation_messages::call(
            &xlrec.msgs,
            xlrec.relcacheInitFileInval,
            xlrec.dbId,
            xlrec.tsId,
        )?;
    } else {
        elog(PANIC, format!("standby_redo: unknown op code {info}"))?;
        unreachable!("elog(PANIC) returned");
    }
    Ok(())
}

/// `LogStandbySnapshot` — log details of the current snapshot to WAL,
/// allowing the snapshot state to be reconstructed on the standby and for
/// logical decoding. Returns the RecPtr of the last inserted record (the
/// running-xacts record; the standby opens up when it sees it).
///
/// (The `USE_INJECTION_POINTS` "skip-log-running-xacts" branch is a
/// test-only build option, absent from this build.)
pub fn LogStandbySnapshot(mcx: Mcx<'_>) -> PgResult<XLogRecPtr> {
    assert!(xlog_standby_info_active());

    // Get details of any AccessExclusiveLocks being held at the moment.
    let locks = lock::get_running_transaction_locks::call(mcx)?;
    if !locks.is_empty() {
        LogAccessExclusiveLocks(&locks)?;
    }
    drop(locks); // pfree(locks)

    // Log details of all in-progress transactions. This should be the last
    // record we write, because the standby will open up when it sees this.
    let running = procarray::get_running_transaction_data::call()?;

    // GetRunningTransactionData() acquired ProcArrayLock; release it. For
    // Hot Standby this can happen before inserting the WAL record because
    // ProcArrayApplyRecoveryInfo() rechecks the commit status using the
    // clog. For logical decoding the lock can't be released early because
    // the clog might be "in the future" from the POV of the historic
    // snapshot, which would allow waiting for the end of a transaction that
    // according to the WAL committed before the xl_running_xacts record.
    if xlog::wal_level::call() < WAL_LEVEL_LOGICAL {
        backend_storage_lmgr_lwlock_seams::lwlock_release_builtin::call(LWLOCK_PROC_ARRAY)?;
    }

    let recptr = LogCurrentRunningXacts(&running)?;

    // Release lock if we kept it longer ...
    if xlog::wal_level::call() >= WAL_LEVEL_LOGICAL {
        backend_storage_lmgr_lwlock_seams::lwlock_release_builtin::call(LWLOCK_PROC_ARRAY)?;
    }

    // GetRunningTransactionData() acquired XidGenLock, we must release it.
    backend_storage_lmgr_lwlock_seams::lwlock_release_builtin::call(LWLOCK_XID_GEN)?;

    Ok(recptr)
}

/// `LogCurrentRunningXacts` — record an enhanced snapshot of running
/// transactions into WAL. The definitions of `RunningTransactionsData` and
/// `xl_running_xacts` are similar, but the latter is the contiguous WAL
/// layout assembled here. The record is marked as not important for
/// durability, to avoid triggering superfluous checkpoint/archiving activity.
fn LogCurrentRunningXacts(CurrRunningXacts: &RunningTransactionsData) -> PgResult<XLogRecPtr> {
    let subxid_overflow = CurrRunningXacts.subxid_status != SUBXIDS_IN_ARRAY;

    // xl_running_xacts up to MinSizeOfXactRunningXacts (the flexible xids
    // array follows).
    let mut header = Vec::with_capacity(MIN_SIZE_OF_XACT_RUNNING_XACTS);
    header.extend_from_slice(&CurrRunningXacts.xcnt.to_ne_bytes());
    header.extend_from_slice(&CurrRunningXacts.subxcnt.to_ne_bytes());
    header.push(subxid_overflow as u8);
    header.extend_from_slice(&[0u8; 3]); // padding before nextXid
    header.extend_from_slice(&CurrRunningXacts.nextXid.to_ne_bytes());
    header.extend_from_slice(&CurrRunningXacts.oldestRunningXid.to_ne_bytes());
    header.extend_from_slice(&CurrRunningXacts.latestCompletedXid.to_ne_bytes());

    xloginsert::xlog_begin_insert::call();
    xloginsert::xlog_set_record_flags::call(XLOG_MARK_UNIMPORTANT);
    xloginsert::xlog_register_data::call(&header);

    // array of TransactionIds
    if CurrRunningXacts.xcnt > 0 {
        let nxids = (CurrRunningXacts.xcnt + CurrRunningXacts.subxcnt) as usize;
        let mut xids = Vec::with_capacity(nxids * 4);
        for &xid in CurrRunningXacts.xids.iter().take(nxids) {
            xids.extend_from_slice(&xid.to_ne_bytes());
        }
        xloginsert::xlog_register_data::call(&xids);
    }

    let recptr = xloginsert::xlog_insert::call(RM_STANDBY_ID, XLOG_RUNNING_XACTS)?;

    if subxid_overflow {
        let _ = elog(
            DEBUG2,
            format!(
                "snapshot of {} running transactions overflowed (lsn {:X}/{:X} oldest xid {} latest complete {} next xid {})",
                CurrRunningXacts.xcnt,
                (recptr >> 32) as u32,
                recptr as u32,
                CurrRunningXacts.oldestRunningXid,
                CurrRunningXacts.latestCompletedXid,
                CurrRunningXacts.nextXid
            ),
        );
    } else {
        let _ = elog(
            DEBUG2,
            format!(
                "snapshot of {}+{} running transaction ids (lsn {:X}/{:X} oldest xid {} latest complete {} next xid {})",
                CurrRunningXacts.xcnt,
                CurrRunningXacts.subxcnt,
                (recptr >> 32) as u32,
                recptr as u32,
                CurrRunningXacts.oldestRunningXid,
                CurrRunningXacts.latestCompletedXid,
                CurrRunningXacts.nextXid
            ),
        );
    }

    // Ensure running_xacts information is synced to disk not too far in the
    // future, without stalling anything (no XLogFlush()): let the WAL writer
    // do it during normal operation. XLogSetAsyncXactLSN() marks the LSN as
    // to-be-synced and nudges the WALWriter if sleeping.
    xlog::xlog_set_async_xact_lsn::call(recptr);

    Ok(recptr)
}

/// `LogAccessExclusiveLocks` — wholesale logging of AccessExclusiveLocks.
/// Other lock types need not be logged, as described in
/// backend/storage/lmgr/README.
fn LogAccessExclusiveLocks(locks: &[xl_standby_lock]) -> PgResult<()> {
    // xl_standby_locks up to offsetof(xl_standby_locks, locks): the nlocks.
    let header = (locks.len() as i32).to_ne_bytes();

    let mut body = Vec::with_capacity(locks.len() * SIZEOF_XL_STANDBY_LOCK);
    for l in locks {
        body.extend_from_slice(&l.xid.to_ne_bytes());
        body.extend_from_slice(&l.dbOid.to_ne_bytes());
        body.extend_from_slice(&l.relOid.to_ne_bytes());
    }

    xloginsert::xlog_begin_insert::call();
    xloginsert::xlog_register_data::call(&header);
    xloginsert::xlog_register_data::call(&body);
    xloginsert::xlog_set_record_flags::call(XLOG_MARK_UNIMPORTANT);

    xloginsert::xlog_insert::call(RM_STANDBY_ID, XLOG_STANDBY_LOCK)?;
    Ok(())
}

/// `LogAccessExclusiveLock(dbOid, relOid)` — individual logging of an
/// AccessExclusiveLock, for use during `LockAcquire()`.
pub fn LogAccessExclusiveLock(dbOid: Oid, relOid: Oid) -> PgResult<()> {
    let xlrec = xl_standby_lock {
        xid: xact::get_current_transaction_id::call()?,
        dbOid,
        relOid,
    };

    LogAccessExclusiveLocks(&[xlrec])?;
    xact::set_my_xact_flags_acquired_access_exclusive_lock::call();
    Ok(())
}

/// `LogAccessExclusiveLockPrepare` — prepare to log an AccessExclusiveLock,
/// for use during `LockAcquire()`. Ensures a TransactionId has been assigned
/// to this transaction: first so that RecordTransactionCommit/Abort do not
/// optimise away the completion record recovery relies on to release locks,
/// and second so a concurrent `GetRunningTransactionLocks()` can't see a
/// lock associated with an InvalidTransactionId.
pub fn LogAccessExclusiveLockPrepare() -> PgResult<()> {
    let _ = xact::get_current_transaction_id::call()?;
    Ok(())
}

/// `LogStandbyInvalidations(nmsgs, msgs, relcacheInitFileInval)` — emit WAL
/// for invalidations. Currently only used for commits without an xid that
/// contain invalidations.
pub fn LogStandbyInvalidations(
    msgs: &[SharedInvalidationMessage],
    relcacheInitFileInval: bool,
) -> PgResult<()> {
    // xl_invalidations up to MinSizeOfInvalidations: dbId, tsId,
    // relcacheInitFileInval (+3 bytes padding), nmsgs.
    let mut header = Vec::with_capacity(MIN_SIZE_OF_INVALIDATIONS);
    header.extend_from_slice(&globals::my_database_id::call().to_ne_bytes());
    header.extend_from_slice(&globals::my_database_table_space::call().to_ne_bytes());
    header.push(relcacheInitFileInval as u8);
    header.extend_from_slice(&[0u8; 3]); // padding before nmsgs
    header.extend_from_slice(&(msgs.len() as i32).to_ne_bytes());

    let mut body = Vec::with_capacity(msgs.len() * SHARED_INVALIDATION_MESSAGE_SIZE);
    for msg in msgs {
        body.extend_from_slice(&msg.raw);
    }

    xloginsert::xlog_begin_insert::call();
    xloginsert::xlog_register_data::call(&header);
    xloginsert::xlog_register_data::call(&body);
    xloginsert::xlog_insert::call(RM_STANDBY_ID, XLOG_INVALIDATIONS)?;
    Ok(())
}

/// `get_recovery_conflict_desc` — the description of a recovery conflict.
fn get_recovery_conflict_desc(reason: ProcSignalReason) -> &'static str {
    match reason {
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => "recovery conflict on buffer pin",
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK => "recovery conflict on lock",
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            "recovery conflict on tablespace"
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => "recovery conflict on snapshot",
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            "recovery conflict on replication slot"
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            "recovery conflict on buffer deadlock"
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE => "recovery conflict on database",
        _ => "unknown reason",
    }
}

// ---------------------------------------------------------------------------
// Wait-event identifiers this unit reports (generated wait_event_types.h /
// wait_event.h; values match the C build).
// ---------------------------------------------------------------------------

/// `WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT` (PG_WAIT_IPC class).
const WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT: u32 = 0x0800_002C;
/// `WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE` (PG_WAIT_IPC class).
const WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE: u32 = 0x0800_002D;
/// `WAIT_EVENT_BUFFER_PIN` (PG_WAIT_BUFFERPIN class).
const WAIT_EVENT_BUFFER_PIN: u32 = 0x0400_0000;
/// `PG_WAIT_LOCK` — class bits OR'd with `locktag_type`.
const PG_WAIT_LOCK: u32 = 0x0300_0000;

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's implementations into `backend-storage-ipc-standby-seams`.
pub fn init_seams() {
    use backend_storage_ipc_standby_seams as seams;

    seams::init_recovery_transaction_environment::set(InitRecoveryTransactionEnvironment);
    seams::shutdown_recovery_transaction_environment::set(ShutdownRecoveryTransactionEnvironment);
    seams::log_recovery_conflict::set(LogRecoveryConflict);
    seams::resolve_recovery_conflict_with_snapshot::set(ResolveRecoveryConflictWithSnapshot);
    seams::resolve_recovery_conflict_with_snapshot_full_xid::set(
        ResolveRecoveryConflictWithSnapshotFullXid,
    );
    seams::resolve_recovery_conflict_with_tablespace::set(ResolveRecoveryConflictWithTablespace);
    seams::resolve_recovery_conflict_with_database::set(ResolveRecoveryConflictWithDatabase);
    seams::resolve_recovery_conflict_with_lock::set(ResolveRecoveryConflictWithLock);
    seams::resolve_recovery_conflict_with_buffer_pin::set(ResolveRecoveryConflictWithBufferPin);
    seams::check_recovery_conflict_deadlock::set(CheckRecoveryConflictDeadlock);
    seams::standby_dead_lock_handler::set(StandbyDeadLockHandler);
    seams::standby_timeout_handler::set(StandbyTimeoutHandler);
    seams::standby_lock_timeout_handler::set(StandbyLockTimeoutHandler);
    seams::standby_acquire_access_exclusive_lock::set(StandbyAcquireAccessExclusiveLock);
    seams::standby_release_lock_tree::set(StandbyReleaseLockTree);
    seams::standby_release_all_locks::set(StandbyReleaseAllLocks);
    seams::standby_release_old_locks::set(StandbyReleaseOldLocks);
    seams::standby_redo::set(standby_redo);
    seams::log_standby_snapshot::set(LogStandbySnapshot);
    seams::log_access_exclusive_lock::set(LogAccessExclusiveLock);
    seams::log_access_exclusive_lock_prepare::set(LogAccessExclusiveLockPrepare);
    seams::log_standby_invalidations::set(LogStandbyInvalidations);
}

#[cfg(test)]
mod tests;
