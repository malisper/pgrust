#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::{
    pg_time_t, pid_t, uint32, uint8, InvalidOid, Oid, ProcNumber, ProtocolVersion, TimestampTz,
    DATEORDER_MDY, INTSTYLE_POSTGRES, INVALID_PROC_NUMBER, MAXPGPATH, MAX_CANCEL_KEY_LENGTH,
    PG_DIR_MODE_OWNER, USE_ISO_DATES,
};
use types_storage::latch::LatchHandle;

// One backend = one thread; every globals.c variable is a per-backend
// `thread_local!`, const-init, !needs_drop (AGENTS.md rule 10).
macro_rules! scalar_global {
    ($($cell:ident, $get:ident, $set:ident, $ty:ty, $init:expr;)+) => {
        $(
            thread_local! {
                static $cell: Cell<$ty> = const {
                    assert!(!core::mem::needs_drop::<$ty>());
                    Cell::new($init)
                };
            }

            #[inline]
            pub fn $get() -> $ty {
                $cell.get()
            }

            #[inline]
            pub fn $set(value: $ty) {
                $cell.set(value);
            }
        )+
    };
}

scalar_global! {
    FRONTEND_PROTOCOL, FrontendProtocol, SetFrontendProtocol, ProtocolVersion, 0;

    INTERRUPT_PENDING, InterruptPending, SetInterruptPending, bool, false;
    QUERY_CANCEL_PENDING, QueryCancelPending, SetQueryCancelPending, bool, false;
    PROC_DIE_PENDING, ProcDiePending, SetProcDiePending, bool, false;
    CHECK_CLIENT_CONNECTION_PENDING, CheckClientConnectionPending,
        SetCheckClientConnectionPending, bool, false;
    CLIENT_CONNECTION_LOST, ClientConnectionLost, SetClientConnectionLost, bool, false;
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT_PENDING, IdleInTransactionSessionTimeoutPending,
        SetIdleInTransactionSessionTimeoutPending, bool, false;
    TRANSACTION_TIMEOUT_PENDING, TransactionTimeoutPending, SetTransactionTimeoutPending,
        bool, false;
    IDLE_SESSION_TIMEOUT_PENDING, IdleSessionTimeoutPending, SetIdleSessionTimeoutPending,
        bool, false;
    PROC_SIGNAL_BARRIER_PENDING, ProcSignalBarrierPending, SetProcSignalBarrierPending,
        bool, false;
    LOG_MEMORY_CONTEXT_PENDING, LogMemoryContextPending, SetLogMemoryContextPending,
        bool, false;
    IDLE_STATS_UPDATE_TIMEOUT_PENDING, IdleStatsUpdateTimeoutPending,
        SetIdleStatsUpdateTimeoutPending, bool, false;

    INTERRUPT_HOLDOFF_COUNT, InterruptHoldoffCount, SetInterruptHoldoffCount, uint32, 0;
    QUERY_CANCEL_HOLDOFF_COUNT, QueryCancelHoldoffCount, SetQueryCancelHoldoffCount, uint32, 0;
    CRIT_SECTION_COUNT, CritSectionCount, SetCritSectionCount, uint32, 0;

    MY_PROC_PID, MyProcPid, SetMyProcPid, i32, 0;
    // `Latch *MyLatch`: None is C's NULL; miscinit points it at the process-
    // local latch or PGPROC's procLatch. Storage lives in the latch unit.
    MY_LATCH, MyLatch, SetMyLatch, Option<LatchHandle>, None;
    MY_START_TIME, MyStartTime, SetMyStartTime, pg_time_t, 0;
    MY_START_TIMESTAMP, MyStartTimestamp, SetMyStartTimestamp, TimestampTz, 0;
    MY_CANCEL_KEY, MyCancelKey, SetMyCancelKey, [uint8; MAX_CANCEL_KEY_LENGTH],
        [0; MAX_CANCEL_KEY_LENGTH];
    MY_CANCEL_KEY_LENGTH, MyCancelKeyLength, SetMyCancelKeyLength, i32, 0;
    MY_PM_CHILD_SLOT, MyPMChildSlot, SetMyPMChildSlot, i32, 0;

    DATA_DIRECTORY_MODE, data_directory_mode, set_data_directory_mode, i32, PG_DIR_MODE_OWNER;

    OUTPUT_FILE_NAME, OutputFileName, SetOutputFileName, [u8; MAXPGPATH], [0; MAXPGPATH];
    MY_EXEC_PATH, my_exec_path, set_my_exec_path, [u8; MAXPGPATH], [0; MAXPGPATH];
    PKGLIB_PATH, pkglib_path, set_pkglib_path, [u8; MAXPGPATH], [0; MAXPGPATH];

    MY_PROC_NUMBER, MyProcNumber, SetMyProcNumber, ProcNumber, INVALID_PROC_NUMBER;
    PARALLEL_LEADER_PROC_NUMBER, ParallelLeaderProcNumber, SetParallelLeaderProcNumber,
        ProcNumber, INVALID_PROC_NUMBER;

    MY_DATABASE_ID, MyDatabaseId, SetMyDatabaseId, Oid, InvalidOid;
    MY_DATABASE_TABLE_SPACE, MyDatabaseTableSpace, SetMyDatabaseTableSpace, Oid, InvalidOid;
    MY_DATABASE_HAS_LOGIN_EVENT_TRIGGERS, MyDatabaseHasLoginEventTriggers,
        SetMyDatabaseHasLoginEventTriggers, bool, false;

    POSTMASTER_PID, PostmasterPid, SetPostmasterPid, pid_t, 0;

    IS_POSTMASTER_ENVIRONMENT, IsPostmasterEnvironment, SetIsPostmasterEnvironment, bool, false;
    IS_UNDER_POSTMASTER, IsUnderPostmaster, SetIsUnderPostmaster, bool, false;
    IS_BINARY_UPGRADE, IsBinaryUpgrade, SetIsBinaryUpgrade, bool, false;

    EXIT_ON_ANY_ERROR, ExitOnAnyError, SetExitOnAnyError, bool, false;

    DATE_STYLE, DateStyle, SetDateStyle, i32, USE_ISO_DATES;
    DATE_ORDER, DateOrder, SetDateOrder, i32, DATEORDER_MDY;
    INTERVAL_STYLE, IntervalStyle, SetIntervalStyle, i32, INTSTYLE_POSTGRES;

    ENABLE_FSYNC, enableFsync, set_enableFsync, bool, true;
    ALLOW_SYSTEM_TABLE_MODS, allowSystemTableMods, set_allowSystemTableMods, bool, false;
    WORK_MEM, work_mem, set_work_mem, i32, 4096;
    HASH_MEM_MULTIPLIER, hash_mem_multiplier, set_hash_mem_multiplier, f64, 2.0;
    MAINTENANCE_WORK_MEM, maintenance_work_mem, set_maintenance_work_mem, i32, 65536;
    MAX_PARALLEL_MAINTENANCE_WORKERS, max_parallel_maintenance_workers,
        set_max_parallel_maintenance_workers, i32, 2;

    N_BUFFERS, NBuffers, SetNBuffers, i32, 16384;
    MAX_CONNECTIONS, MaxConnections, SetMaxConnections, i32, 100;
    MAX_WORKER_PROCESSES, max_worker_processes, set_max_worker_processes, i32, 8;
    MAX_PARALLEL_WORKERS, max_parallel_workers, set_max_parallel_workers, i32, 8;
    MAX_BACKENDS, MaxBackends, SetMaxBackends, i32, 0;

    VACUUM_BUFFER_USAGE_LIMIT, VacuumBufferUsageLimit, SetVacuumBufferUsageLimit, i32, 2048;
    VACUUM_COST_PAGE_HIT, VacuumCostPageHit, SetVacuumCostPageHit, i32, 1;
    VACUUM_COST_PAGE_MISS, VacuumCostPageMiss, SetVacuumCostPageMiss, i32, 2;
    VACUUM_COST_PAGE_DIRTY, VacuumCostPageDirty, SetVacuumCostPageDirty, i32, 20;
    VACUUM_COST_LIMIT, VacuumCostLimit, SetVacuumCostLimit, i32, 200;
    VACUUM_COST_DELAY, VacuumCostDelay, SetVacuumCostDelay, f64, 0.0;
    VACUUM_COST_BALANCE, VacuumCostBalance, SetVacuumCostBalance, i32, 0;
    VACUUM_COST_ACTIVE, VacuumCostActive, SetVacuumCostActive, bool, false;

    COMMIT_TIMESTAMP_BUFFERS, commit_timestamp_buffers, set_commit_timestamp_buffers, i32, 0;
    MULTIXACT_MEMBER_BUFFERS, multixact_member_buffers, set_multixact_member_buffers, i32, 32;
    MULTIXACT_OFFSET_BUFFERS, multixact_offset_buffers, set_multixact_offset_buffers, i32, 16;
    NOTIFY_BUFFERS, notify_buffers, set_notify_buffers, i32, 16;
    SERIALIZABLE_BUFFERS, serializable_buffers, set_serializable_buffers, i32, 32;
    SUBTRANSACTION_BUFFERS, subtransaction_buffers, set_subtransaction_buffers, i32, 0;
    TRANSACTION_BUFFERS, transaction_buffers, set_transaction_buffers, i32, 0;
}

// `char *DataDir` / `char *DatabasePath`: set once per backend, never freed in
// C; the leaked &'static str keeps reads a plain load with no per-read clone.
thread_local! {
    static DATA_DIR: Cell<Option<&'static str>> = const { Cell::new(None) };
    static DATABASE_PATH: Cell<Option<&'static str>> = const { Cell::new(None) };
}

pub fn DataDir() -> Option<&'static str> {
    DATA_DIR.get()
}

pub fn SetDataDir(value: &str) {
    DATA_DIR.set(Some(String::from(value).leak()));
}

pub fn DatabasePath() -> Option<&'static str> {
    DATABASE_PATH.get()
}

pub fn SetDatabasePath(value: &str) {
    DATABASE_PATH.set(Some(String::from(value).leak()));
}

// `DatabasePath = NULL` (inval.c's recovery-only poke via miscinit).
pub fn ClearDatabasePath() {
    DATABASE_PATH.set(None);
}

// miscadmin.h / c.h interrupt macros over the counters above. Per-query hot
// family (frontend reads, WAL critical sections): keep as inline Cell ops.

/// `HOLD_INTERRUPTS()`
#[inline]
pub fn HoldInterrupts() {
    SetInterruptHoldoffCount(InterruptHoldoffCount() + 1);
}

/// `RESUME_INTERRUPTS()`
#[inline]
pub fn ResumeInterrupts() {
    let count = InterruptHoldoffCount();
    assert!(count > 0, "InterruptHoldoffCount underflow");
    SetInterruptHoldoffCount(count - 1);
}

/// `HOLD_CANCEL_INTERRUPTS()`
#[inline]
pub fn HoldCancelInterrupts() {
    SetQueryCancelHoldoffCount(QueryCancelHoldoffCount() + 1);
}

/// `RESUME_CANCEL_INTERRUPTS()`
#[inline]
pub fn ResumeCancelInterrupts() {
    let count = QueryCancelHoldoffCount();
    assert!(count > 0, "QueryCancelHoldoffCount underflow");
    SetQueryCancelHoldoffCount(count - 1);
}

/// `START_CRIT_SECTION()`
#[inline]
pub fn StartCriticalSection() {
    SetCritSectionCount(CritSectionCount() + 1);
}

/// `END_CRIT_SECTION()`
#[inline]
pub fn EndCriticalSection() {
    let count = CritSectionCount();
    assert!(count > 0, "CritSectionCount underflow");
    SetCritSectionCount(count - 1);
}

/// `INTERRUPTS_CAN_BE_PROCESSED()`
#[inline]
pub fn InterruptsCanBeProcessed() -> bool {
    InterruptHoldoffCount() == 0 && CritSectionCount() == 0 && QueryCancelHoldoffCount() == 0
}
