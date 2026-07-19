//! Recovery targets, pause machinery, promotion triggers and the replay
//! monitoring state (xlogrecovery.c file statics + XLogRecoveryCtlData
//! fields; single address space, so atomics stand in for the spinlocked
//! shmem struct).

use std::cell::Cell;
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering::Relaxed,
};
use pgsync::Mutex;

use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableTimedSleep,
};
use elog::{elog, ereport};
use types_core::{TimeLineID, TimestampTz, TransactionId, XLogRecPtr};
use types_error::{PgResult, FATAL, LOG, WARNING};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::{data_path, loc, lsn_fmt, InvalidXLogRecPtr, PROMOTE_SIGNAL_FILE};

pub const MAXFNAMELEN: usize = 64;

const PG_WAIT_IPC: u32 = 0x0800_0000;
pub(crate) const WAIT_EVENT_RECOVERY_PAUSE: u32 = PG_WAIT_IPC + 47;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum RecoveryTargetType {
    Unset = 0,
    Xid = 1,
    Time = 2,
    Name = 3,
    Lsn = 4,
    Immediate = 5,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum RecoveryTargetTimeLineGoal {
    ControlFile = 0,
    Latest = 1,
    Numeric = 2,
}

// recovery_target_action_options[] values (xlogrecovery.h).
pub const RECOVERY_TARGET_ACTION_PAUSE: i32 = 0;
pub const RECOVERY_TARGET_ACTION_PROMOTE: i32 = 1;
pub const RECOVERY_TARGET_ACTION_SHUTDOWN: i32 = 2;

// RecoveryPauseState (xlogrecovery.h).
pub const RECOVERY_NOT_PAUSED: i32 = 0;
pub const RECOVERY_PAUSE_REQUESTED: i32 = 1;
pub const RECOVERY_PAUSED: i32 = 2;

// GUC assign hooks replay per child thread (C: per process). The parsed
// target state is therefore thread-local — each thread rebuilds its own
// consistent defaults-then-config sequence, exactly like a C backend; the
// startup thread's copy is the one recovery reads.
thread_local! {
    static RECOVERY_TARGET: Cell<i32> = const { Cell::new(RecoveryTargetType::Unset as i32) };
    static RECOVERY_TARGET_TIME: Cell<TimestampTz> = const { Cell::new(0) };
    static RECOVERY_TARGET_LSN: Cell<XLogRecPtr> = const { Cell::new(0) };
    static RECOVERY_TARGET_NAME: std::cell::RefCell<String> =
        const { std::cell::RefCell::new(String::new()) };
    static TIMELINE_GOAL: Cell<i32> =
        const { Cell::new(RecoveryTargetTimeLineGoal::Latest as i32) };
    static TLI_REQUESTED: Cell<TimeLineID> = const { Cell::new(0) };
    static RECOVERY_TARGET_XID_PARSED: Cell<u32> = const { Cell::new(0) };
}

static STOP_AFTER: AtomicBool = AtomicBool::new(false);
static STOP_XID: AtomicU32 = AtomicU32::new(0);
static STOP_TIME: AtomicI64 = AtomicI64::new(0);
static STOP_LSN: AtomicU64 = AtomicU64::new(0);
pgsync::process_global! {
    static STOP_NAME: Mutex<String> = Mutex::new(String::new());
}

static RECOVERY_PAUSE_STATE: AtomicI32 = AtomicI32::new(RECOVERY_NOT_PAUSED);
static RECOVERY_NOT_PAUSED_CV: ConditionVariable = ConditionVariable::new();

static SHARED_HOT_STANDBY_ACTIVE: AtomicBool = AtomicBool::new(false);
static RECOVERY_LAST_XTIME: AtomicI64 = AtomicI64::new(0);
static CURRENT_CHUNK_START_TIME: AtomicI64 = AtomicI64::new(0);

thread_local! {
    static LOCAL_HOT_STANDBY_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static LOCAL_PROMOTE_IS_TRIGGERED: Cell<bool> = const { Cell::new(false) };
}

pub fn recovery_target() -> RecoveryTargetType {
    match RECOVERY_TARGET.with(|c| c.get()) {
        1 => RecoveryTargetType::Xid,
        2 => RecoveryTargetType::Time,
        3 => RecoveryTargetType::Name,
        4 => RecoveryTargetType::Lsn,
        5 => RecoveryTargetType::Immediate,
        _ => RecoveryTargetType::Unset,
    }
}

fn set_recovery_target(t: RecoveryTargetType) {
    RECOVERY_TARGET.with(|c| c.set(t as i32));
}

pub fn recovery_target_xid() -> TransactionId {
    RECOVERY_TARGET_XID_PARSED.with(|c| c.get())
}

pub fn recovery_target_time() -> TimestampTz {
    RECOVERY_TARGET_TIME.with(|c| c.get())
}
pub(crate) fn set_recovery_target_time(t: TimestampTz) {
    RECOVERY_TARGET_TIME.with(|c| c.set(t));
}
pub fn recovery_target_lsn() -> XLogRecPtr {
    RECOVERY_TARGET_LSN.with(|c| c.get())
}
pub fn recovery_target_name() -> String {
    RECOVERY_TARGET_NAME.with(|c| c.borrow().clone())
}
pub fn recovery_target_time_string() -> String {
    guc_tables::vars::recovery_target_time_string
        .read()
        .unwrap_or_default()
}
pub fn timeline_goal() -> RecoveryTargetTimeLineGoal {
    match TIMELINE_GOAL.with(|c| c.get()) {
        0 => RecoveryTargetTimeLineGoal::ControlFile,
        2 => RecoveryTargetTimeLineGoal::Numeric,
        _ => RecoveryTargetTimeLineGoal::Latest,
    }
}
pub fn recovery_target_tli_requested() -> TimeLineID {
    TLI_REQUESTED.with(|c| c.get())
}
pub fn recovery_target_inclusive() -> bool {
    guc_tables::vars::recoveryTargetInclusive.read()
}
pub fn recovery_target_action() -> i32 {
    guc_tables::vars::recoveryTargetAction.read()
}

pub(crate) fn set_recovery_target_action_shutdown() {
    guc_tables::vars::recoveryTargetAction.write(RECOVERY_TARGET_ACTION_SHUTDOWN);
}

pub fn recovery_wakeup_latch() -> LatchHandle {
    static INITED: pgsync::OnceLock<()> = pgsync::OnceLock::new();
    let h = LatchHandle::recovery_wakeup();
    INITED.get_or_init(|| latch::InitSharedLatch(h));
    h
}

pub fn WakeupRecovery() {
    latch::SetLatch(recovery_wakeup_latch());
}

pub fn GetRecoveryPauseState() -> i32 {
    RECOVERY_PAUSE_STATE.load(Relaxed)
}

pub fn SetRecoveryPause(recovery_pause: bool) {
    if !recovery_pause {
        RECOVERY_PAUSE_STATE.store(RECOVERY_NOT_PAUSED, Relaxed);
    } else {
        let _ = RECOVERY_PAUSE_STATE.compare_exchange(
            RECOVERY_NOT_PAUSED,
            RECOVERY_PAUSE_REQUESTED,
            Relaxed,
            Relaxed,
        );
    }
    if !recovery_pause {
        ConditionVariableBroadcast(&RECOVERY_NOT_PAUSED_CV);
    }
}

pub(crate) fn ConfirmRecoveryPaused() {
    let _ = RECOVERY_PAUSE_STATE.compare_exchange(
        RECOVERY_PAUSE_REQUESTED,
        RECOVERY_PAUSED,
        Relaxed,
        Relaxed,
    );
}

pub fn PromoteIsTriggered() -> bool {
    if LOCAL_PROMOTE_IS_TRIGGERED.get() {
        return true;
    }
    let v = crate::PROMOTE_IS_TRIGGERED.load(Relaxed);
    LOCAL_PROMOTE_IS_TRIGGERED.set(v);
    v
}

fn SetPromoteIsTriggered() {
    crate::PROMOTE_IS_TRIGGERED.store(true, Relaxed);
    SetRecoveryPause(false);
    LOCAL_PROMOTE_IS_TRIGGERED.set(true);
}

pub(crate) fn CheckForStandbyTrigger() -> bool {
    if LOCAL_PROMOTE_IS_TRIGGERED.get() {
        return true;
    }
    let promote_signaled = startup_seams::is_promote_signaled::is_installed()
        && startup_seams::is_promote_signaled::call();
    if promote_signaled && CheckPromoteSignal() {
        let _ = elog(LOG, "received promote request".to_string());
        crate::RemovePromoteSignalFiles();
        startup_seams::reset_promote_signaled::call();
        SetPromoteIsTriggered();
        return true;
    }
    false
}

pub fn CheckPromoteSignal() -> bool {
    std::path::Path::new(&data_path(PROMOTE_SIGNAL_FILE)).exists()
}

pub fn HotStandbyActive() -> bool {
    if LOCAL_HOT_STANDBY_ACTIVE.get() {
        return true;
    }
    let v = SHARED_HOT_STANDBY_ACTIVE.load(Relaxed);
    LOCAL_HOT_STANDBY_ACTIVE.set(v);
    v
}

pub(crate) fn HotStandbyActiveInReplay() -> bool {
    LOCAL_HOT_STANDBY_ACTIVE.get()
}

pub(crate) fn set_shared_hot_standby_active() {
    SHARED_HOT_STANDBY_ACTIVE.store(true, Relaxed);
    LOCAL_HOT_STANDBY_ACTIVE.set(true);
}

pub(crate) fn SetLatestXTime(xtime: TimestampTz) {
    RECOVERY_LAST_XTIME.store(xtime, Relaxed);
}
pub fn GetLatestXTime() -> TimestampTz {
    RECOVERY_LAST_XTIME.load(Relaxed)
}
pub(crate) fn SetCurrentChunkStartTime(xtime: TimestampTz) {
    CURRENT_CHUNK_START_TIME.store(xtime, Relaxed);
}
pub fn GetCurrentChunkReplayStartTime() -> TimestampTz {
    CURRENT_CHUNK_START_TIME.load(Relaxed)
}

fn ts_str(t: TimestampTz) -> String {
    timestamp_seams::timestamptz_to_str::call(t)
}

// xl_restore_point: rp_time i64 at 0, rp_name[MAXFNAMELEN] at 8.
fn restore_point_name(data: &[u8]) -> String {
    let raw = &data[8..8 + MAXFNAMELEN.min(data.len().saturating_sub(8))];
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).into_owned()
}

pub(crate) fn getRecordTimestamp(reader: &xlogreader::XLogReaderState<'_>) -> Option<TimestampTz> {
    let info = reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK;
    let xact_info = info & xact::XLOG_XACT_OPMASK;
    let rmid = reader.XLogRecGetRmid();

    if rmid == transam_xlog::RM_XLOG_ID && info == transam_xlog::XLOG_RESTORE_POINT {
        let data = reader.XLogRecGetData();
        return Some(i64::from_ne_bytes(data[..8].try_into().unwrap()));
    }
    // xl_xact_commit / xl_xact_abort both put xact_time at offset 0.
    if rmid == xact::RM_XACT_ID
        && matches!(
            xact_info,
            xact::XLOG_XACT_COMMIT
                | xact::XLOG_XACT_COMMIT_PREPARED
                | xact::XLOG_XACT_ABORT
                | xact::XLOG_XACT_ABORT_PREPARED
        )
    {
        let data = reader.XLogRecGetData();
        return Some(i64::from_ne_bytes(data[..8].try_into().unwrap()));
    }
    None
}

fn record_end_xid(reader: &xlogreader::XLogReaderState<'_>, xact_info: u8) -> PgResult<TransactionId> {
    let info = reader.XLogRecGetInfo();
    let data = reader.XLogRecGetData();
    Ok(match xact_info {
        xact::XLOG_XACT_COMMIT_PREPARED => xact::parse_commit_record(info, data)?.twophase_xid,
        xact::XLOG_XACT_ABORT_PREPARED => xact::parse_abort_record(info, data)?.twophase_xid,
        _ => reader.XLogRecGetXid(),
    })
}

fn clear_stop(after: bool) {
    STOP_AFTER.store(after, Relaxed);
    STOP_XID.store(0, Relaxed);
    STOP_LSN.store(InvalidXLogRecPtr, Relaxed);
    STOP_TIME.store(0, Relaxed);
    STOP_NAME.lock().unwrap().clear();
}

pub(crate) fn recoveryStopsBefore(reader: &xlogreader::XLogReaderState<'_>) -> PgResult<bool> {
    if !crate::ArchiveRecoveryRequested() {
        return Ok(false);
    }
    let target = recovery_target();

    if target == RecoveryTargetType::Immediate && crate::reached_consistency() {
        let _ = elog(LOG, "recovery stopping after reaching consistency".to_string());
        clear_stop(false);
        return Ok(true);
    }

    if target == RecoveryTargetType::Lsn
        && !recovery_target_inclusive()
        && reader.v.ReadRecPtr >= recovery_target_lsn()
    {
        clear_stop(false);
        STOP_LSN.store(reader.v.ReadRecPtr, Relaxed);
        let _ = elog(
            LOG,
            format!(
                "recovery stopping before WAL location (LSN) \"{}\"",
                lsn_fmt(reader.v.ReadRecPtr)
            ),
        );
        return Ok(true);
    }

    if reader.XLogRecGetRmid() != xact::RM_XACT_ID {
        return Ok(false);
    }
    let xact_info = reader.XLogRecGetInfo() & xact::XLOG_XACT_OPMASK;
    let is_commit = match xact_info {
        xact::XLOG_XACT_COMMIT | xact::XLOG_XACT_COMMIT_PREPARED => true,
        xact::XLOG_XACT_ABORT | xact::XLOG_XACT_ABORT_PREPARED => false,
        _ => return Ok(false),
    };
    let record_xid = record_end_xid(reader, xact_info)?;

    let mut stops_here = false;
    if target == RecoveryTargetType::Xid && !recovery_target_inclusive() {
        stops_here = record_xid == recovery_target_xid();
    }

    let record_xtime = getRecordTimestamp(reader);
    if let Some(xtime) = record_xtime {
        if target == RecoveryTargetType::Time {
            stops_here = if recovery_target_inclusive() {
                xtime > recovery_target_time()
            } else {
                xtime >= recovery_target_time()
            };
        }
    }

    if stops_here {
        clear_stop(false);
        STOP_XID.store(record_xid.into(), Relaxed);
        STOP_TIME.store(record_xtime.unwrap_or(0), Relaxed);
        let _ = elog(
            LOG,
            format!(
                "recovery stopping before {} of transaction {}, time {}",
                if is_commit { "commit" } else { "abort" },
                u32::from(record_xid),
                ts_str(record_xtime.unwrap_or(0))
            ),
        );
    }
    Ok(stops_here)
}

pub(crate) fn recoveryStopsAfter(reader: &xlogreader::XLogReaderState<'_>) -> PgResult<bool> {
    if !crate::ArchiveRecoveryRequested() {
        return Ok(false);
    }
    let target = recovery_target();
    let info = reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK;
    let rmid = reader.XLogRecGetRmid();

    if target == RecoveryTargetType::Name
        && rmid == transam_xlog::RM_XLOG_ID
        && info == transam_xlog::XLOG_RESTORE_POINT
    {
        let rp_name = restore_point_name(reader.XLogRecGetData());
        if rp_name == recovery_target_name() {
            clear_stop(true);
            let xtime = getRecordTimestamp(reader).unwrap_or(0);
            STOP_TIME.store(xtime, Relaxed);
            *STOP_NAME.lock().unwrap() = rp_name.clone();
            let _ = elog(
                LOG,
                format!(
                    "recovery stopping at restore point \"{rp_name}\", time {}",
                    ts_str(xtime)
                ),
            );
            return Ok(true);
        }
    }

    if target == RecoveryTargetType::Lsn
        && recovery_target_inclusive()
        && reader.v.ReadRecPtr >= recovery_target_lsn()
    {
        clear_stop(true);
        STOP_LSN.store(reader.v.ReadRecPtr, Relaxed);
        let _ = elog(
            LOG,
            format!(
                "recovery stopping after WAL location (LSN) \"{}\"",
                lsn_fmt(reader.v.ReadRecPtr)
            ),
        );
        return Ok(true);
    }

    if rmid == xact::RM_XACT_ID {
        let xact_info = info & xact::XLOG_XACT_OPMASK;
        if matches!(
            xact_info,
            xact::XLOG_XACT_COMMIT
                | xact::XLOG_XACT_COMMIT_PREPARED
                | xact::XLOG_XACT_ABORT
                | xact::XLOG_XACT_ABORT_PREPARED
        ) {
            let record_xtime = getRecordTimestamp(reader);
            if let Some(xtime) = record_xtime {
                SetLatestXTime(xtime);
            }
            let record_xid = record_end_xid(reader, xact_info)?;

            if target == RecoveryTargetType::Xid
                && recovery_target_inclusive()
                && record_xid == recovery_target_xid()
            {
                clear_stop(true);
                STOP_XID.store(record_xid.into(), Relaxed);
                STOP_TIME.store(record_xtime.unwrap_or(0), Relaxed);
                let is_commit = matches!(
                    xact_info,
                    xact::XLOG_XACT_COMMIT | xact::XLOG_XACT_COMMIT_PREPARED
                );
                let _ = elog(
                    LOG,
                    format!(
                        "recovery stopping after {} of transaction {}, time {}",
                        if is_commit { "commit" } else { "abort" },
                        u32::from(record_xid),
                        ts_str(record_xtime.unwrap_or(0))
                    ),
                );
                return Ok(true);
            }
        }
    }

    if target == RecoveryTargetType::Immediate && crate::reached_consistency() {
        let _ = elog(LOG, "recovery stopping after reaching consistency".to_string());
        clear_stop(true);
        return Ok(true);
    }

    Ok(false)
}

pub(crate) fn getRecoveryStopReason() -> String {
    let after = STOP_AFTER.load(Relaxed);
    let pos = if after { "after" } else { "before" };
    match recovery_target() {
        RecoveryTargetType::Xid => format!("{pos} transaction {}", STOP_XID.load(Relaxed)),
        RecoveryTargetType::Time => format!("{pos} {}\n", ts_str(STOP_TIME.load(Relaxed))),
        RecoveryTargetType::Lsn => {
            format!("{pos} LSN {}\n", lsn_fmt(STOP_LSN.load(Relaxed)))
        }
        RecoveryTargetType::Name => {
            format!("at restore point \"{}\"", &*STOP_NAME.lock().unwrap())
        }
        RecoveryTargetType::Immediate => "reached consistency".to_string(),
        RecoveryTargetType::Unset => "no recovery target specified".to_string(),
    }
}

pub(crate) fn recoveryPausesHere(end_of_recovery: bool) -> PgResult<()> {
    if !HotStandbyActiveInReplay() {
        return Ok(());
    }
    if LOCAL_PROMOTE_IS_TRIGGERED.get() {
        return Ok(());
    }
    if end_of_recovery {
        let _ = ereport(LOG)
            .errmsg("pausing at the end of recovery")
            .errhint("Execute pg_wal_replay_resume() to promote.")
            .finish(loc("recoveryPausesHere"));
    } else {
        let _ = ereport(LOG)
            .errmsg("recovery has paused")
            .errhint("Execute pg_wal_replay_resume() to continue.")
            .finish(loc("recoveryPausesHere"));
    }
    while GetRecoveryPauseState() != RECOVERY_NOT_PAUSED {
        startup_seams::process_startup_proc_interrupts::call()?;
        if CheckForStandbyTrigger() {
            return Ok(());
        }
        ConfirmRecoveryPaused();
        let _ = ConditionVariableTimedSleep(
            &RECOVERY_NOT_PAUSED_CV,
            1000,
            WAIT_EVENT_RECOVERY_PAUSE,
        );
    }
    ConditionVariableCancelSleep();
    Ok(())
}

const PG_WAIT_TIMEOUT: u32 = 0x0900_0000;
const WAIT_EVENT_RECOVERY_APPLY_DELAY: u32 = PG_WAIT_TIMEOUT + 3;

pub(crate) fn recoveryApplyDelay(reader: &xlogreader::XLogReaderState<'_>) -> PgResult<bool> {
    let delay = guc_tables::vars::recovery_min_apply_delay.read();
    if delay <= 0 {
        return Ok(false);
    }
    if !crate::reached_consistency() {
        return Ok(false);
    }
    if !crate::ArchiveRecoveryRequested() {
        return Ok(false);
    }
    if reader.XLogRecGetRmid() != xact::RM_XACT_ID {
        return Ok(false);
    }
    let xact_info = reader.XLogRecGetInfo() & xact::XLOG_XACT_OPMASK;
    if xact_info != xact::XLOG_XACT_COMMIT && xact_info != xact::XLOG_XACT_COMMIT_PREPARED {
        return Ok(false);
    }
    let Some(xtime) = getRecordTimestamp(reader) else {
        return Ok(false);
    };

    let now = timestamp_seams::get_current_timestamp::call();
    let delay_until = xtime + (delay as i64) * 1000;
    if (delay_until - now) / 1000 <= 0 {
        return Ok(false);
    }

    loop {
        latch::ResetLatch(recovery_wakeup_latch());
        startup_seams::process_startup_proc_interrupts::call()?;
        if CheckForStandbyTrigger() {
            break;
        }
        let delay = guc_tables::vars::recovery_min_apply_delay.read() as i64;
        let delay_until = xtime + delay * 1000;
        let msecs = (delay_until - timestamp_seams::get_current_timestamp::call()) / 1000;
        if msecs <= 0 {
            break;
        }
        let _ = latch::WaitLatch(
            Some(recovery_wakeup_latch()),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            msecs,
            WAIT_EVENT_RECOVERY_APPLY_DELAY,
        )?;
    }
    Ok(true)
}

pub fn RecoveryRequiresIntParameter(param_name: &str, curr_value: i32, min_value: i32) -> PgResult<()> {
    if curr_value >= min_value {
        return Ok(());
    }
    if HotStandbyActiveInReplay() {
        let detail = format!(
            "{param_name} = {curr_value} is a lower setting than on the primary server, where its value was {min_value}."
        );
        let _ = ereport(WARNING)
            .errmsg("hot standby is not possible because of insufficient parameter settings")
            .errdetail(detail.clone())
            .finish(loc("RecoveryRequiresIntParameter"));
        SetRecoveryPause(true);
        let _ = ereport(LOG)
            .errmsg("recovery has paused")
            .errdetail("If recovery is unpaused, the server will shut down.")
            .errhint(
                "You can then restart the server after making the necessary configuration changes.",
            )
            .finish(loc("RecoveryRequiresIntParameter"));
        let mut warned_for_promote = false;
        while GetRecoveryPauseState() != RECOVERY_NOT_PAUSED {
            startup_seams::process_startup_proc_interrupts::call()?;
            if CheckForStandbyTrigger() && !warned_for_promote {
                let _ = ereport(WARNING)
                    .errmsg("promotion is not possible because of insufficient parameter settings")
                    .errdetail(detail.clone())
                    .errhint("Restart the server after making the necessary configuration changes.")
                    .finish(loc("RecoveryRequiresIntParameter"));
                warned_for_promote = true;
            }
            ConfirmRecoveryPaused();
            let _ = ConditionVariableTimedSleep(
                &RECOVERY_NOT_PAUSED_CV,
                1000,
                WAIT_EVENT_RECOVERY_PAUSE,
            );
        }
        ConditionVariableCancelSleep();
    }
    ereport(FATAL)
        .errmsg("recovery aborted because of insufficient parameter settings")
        .errdetail(format!(
            "{param_name} = {curr_value} is a lower setting than on the primary server, where its value was {min_value}."
        ))
        .errhint("You can restart the server after making the necessary configuration changes.")
        .finish(loc("RecoveryRequiresIntParameter"))
}


// C guards every assign, but its per-process init makes the empty-value
// guard unreachable; our per-thread snapshot replay is unordered across
// variables, so an empty assign skips the guard and unsets only its own kind.
fn guard_target(kind: RecoveryTargetType) {
    let cur = recovery_target();
    if cur != RecoveryTargetType::Unset && cur != kind {
        panic!("multiple recovery targets specified");
    }
}

fn unset_target(kind: RecoveryTargetType) {
    if recovery_target() == kind {
        set_recovery_target(RecoveryTargetType::Unset);
    }
}

pub(crate) fn install_guc_hooks() {
    use guc_tables::hooks;

    hooks::check_recovery_target.install(|newval, _extra, _source| {
        let v = newval.as_deref().unwrap_or("");
        if v != "immediate" && !v.is_empty() {
            return Err(Box::new(types_error::PgError::new(
                types_error::ERROR,
                "invalid value for parameter \"recovery_target\": The only allowed value is \"immediate\".".to_string(),
            )));
        }
        Ok(true)
    });
    hooks::assign_recovery_target.install(|newval, _extra| {
        if newval.map_or(false, |v| !v.is_empty()) {
            guard_target(RecoveryTargetType::Immediate);
            set_recovery_target(RecoveryTargetType::Immediate);
        } else {
            unset_target(RecoveryTargetType::Immediate);
        }
    });

    hooks::check_recovery_target_lsn.install(|newval, extra, _source| {
        if let Some(v) = newval.as_deref() {
            if !v.is_empty() {
                let Some(lsn) = adt_pg_lsn::pg_lsn_in_internal(v.as_bytes()) else {
                    return Ok(false);
                };
                *extra = Some(Box::new(lsn));
            }
        }
        Ok(true)
    });
    hooks::assign_recovery_target_lsn.install(|newval, extra| {
        if newval.map_or(false, |v| !v.is_empty()) {
            guard_target(RecoveryTargetType::Lsn);
            set_recovery_target(RecoveryTargetType::Lsn);
            let lsn = *extra
                .and_then(|e| e.downcast_ref::<XLogRecPtr>())
                .expect("check hook stored the parsed LSN");
            RECOVERY_TARGET_LSN.with(|c| c.set(lsn));
        } else {
            unset_target(RecoveryTargetType::Lsn);
        }
    });

    hooks::check_recovery_target_name.install(|newval, _extra, _source| {
        if newval.as_deref().map_or(0, |v| v.len()) >= MAXFNAMELEN {
            return Err(Box::new(types_error::PgError::new(
                types_error::ERROR,
                format!(
                    "invalid value for parameter \"recovery_target_name\": \"recovery_target_name\" is too long (maximum {} characters).",
                    MAXFNAMELEN - 1
                ),
            )));
        }
        Ok(true)
    });
    hooks::assign_recovery_target_name.install(|newval, _extra| {
        match newval {
            Some(v) if !v.is_empty() => {
                guard_target(RecoveryTargetType::Name);
                set_recovery_target(RecoveryTargetType::Name);
                RECOVERY_TARGET_NAME.with(|c| *c.borrow_mut() = v.to_string());
            }
            _ => unset_target(RecoveryTargetType::Name),
        }
    });

    hooks::check_recovery_target_time.install(|newval, _extra, _source| {
        if let Some(v) = newval.as_deref() {
            if !v.is_empty() {
                if matches!(v, "now" | "today" | "tomorrow" | "yesterday") {
                    return Ok(false);
                }
                if adt_timestamp::timestamptz_in(v, -1, None).is_err() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    });
    hooks::assign_recovery_target_time.install(|newval, _extra| {
        if newval.map_or(false, |v| !v.is_empty()) {
            guard_target(RecoveryTargetType::Time);
            set_recovery_target(RecoveryTargetType::Time);
        } else {
            unset_target(RecoveryTargetType::Time);
        }
    });

    hooks::check_recovery_target_timeline.install(|newval, extra, _source| {
        let v = newval.as_deref().unwrap_or("");
        let goal = match v {
            "current" => RecoveryTargetTimeLineGoal::ControlFile,
            "latest" => RecoveryTargetTimeLineGoal::Latest,
            _ => {
                if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) || v.parse::<u64>().is_err()
                {
                    return Err(Box::new(types_error::PgError::new(
                        types_error::ERROR,
                        "invalid value: \"recovery_target_timeline\" is not a valid number."
                            .to_string(),
                    )));
                }
                RecoveryTargetTimeLineGoal::Numeric
            }
        };
        *extra = Some(Box::new(goal));
        Ok(true)
    });
    hooks::assign_recovery_target_timeline.install(|newval, extra| {
        let goal = *extra
            .and_then(|e| e.downcast_ref::<RecoveryTargetTimeLineGoal>())
            .expect("check hook stored the goal");
        TIMELINE_GOAL.with(|c| c.set(goal as i32));
        if goal == RecoveryTargetTimeLineGoal::Numeric {
            TLI_REQUESTED
                .with(|c| c.set(newval.and_then(|v| v.parse::<u32>().ok()).unwrap_or(0)));
        } else {
            TLI_REQUESTED.with(|c| c.set(0));
        }
    });

    hooks::check_recovery_target_xid.install(|newval, extra, _source| {
        if let Some(v) = newval.as_deref() {
            if !v.is_empty() {
                let Ok(xid) = v.parse::<u64>() else {
                    return Ok(false);
                };
                *extra = Some(Box::new(xid as u32));
            }
        }
        Ok(true)
    });
    hooks::assign_recovery_target_xid.install(|newval, extra| {
        if newval.map_or(false, |v| !v.is_empty()) {
            guard_target(RecoveryTargetType::Xid);
            set_recovery_target(RecoveryTargetType::Xid);
            let xid = *extra
                .and_then(|e| e.downcast_ref::<u32>())
                .expect("check hook stored the parsed xid");
            RECOVERY_TARGET_XID_PARSED.with(|c| c.set(xid));
        } else {
            unset_target(RecoveryTargetType::Xid);
        }
    });

    hooks::check_primary_slot_name.install(|newval, _extra, _source| {
        if let Some(v) = newval.as_deref() {
            if !v.is_empty() {
                return slot::ReplicationSlotValidateName(v, types_error::ERROR);
            }
        }
        Ok(true)
    });
}
