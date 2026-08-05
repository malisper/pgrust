#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;

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

// C `int data_directory_mode` (globals.c): set once by checkDataDir() in the
// postmaster before any children exist, constant afterwards, fork-inherited.
// PROCESS-GLOBAL here — a thread-local (even one copied at child launch) can
// go stale for worker-pool threads spawned before checkDataDir ran, leaving
// `SHOW data_directory_mode` at 0700 on a group-access (0750) cluster
// (pg_basebackup/010 group-permission leg).
static DATA_DIRECTORY_MODE: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(PG_DIR_MODE_OWNER as i32);

#[inline]
pub fn data_directory_mode() -> i32 {
    DATA_DIRECTORY_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn set_data_directory_mode(mode: i32) {
    DATA_DIRECTORY_MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
}

scalar_global! {
    FRONTEND_PROTOCOL, FrontendProtocol, SetFrontendProtocol, ProtocolVersion, 0;

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
    PARALLEL_MESSAGE_PENDING, ParallelMessagePending, SetParallelMessagePending, bool, false;
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

    OUTPUT_FILE_NAME, OutputFileName, SetOutputFileName, [u8; MAXPGPATH], [0; MAXPGPATH];
    MY_EXEC_PATH, my_exec_path, set_my_exec_path, [u8; MAXPGPATH], [0; MAXPGPATH];
    PKGLIB_PATH, pkglib_path, set_pkglib_path, [u8; MAXPGPATH], [0; MAXPGPATH];

    MY_PROC_NUMBER, MyProcNumber, SetMyProcNumber, ProcNumber, INVALID_PROC_NUMBER;
    PARALLEL_LEADER_PROC_NUMBER, ParallelLeaderProcNumber, SetParallelLeaderProcNumber,
        ProcNumber, INVALID_PROC_NUMBER;

    MY_DATABASE_ID, MyDatabaseId, SetMyDatabaseId, Oid, InvalidOid;
    // pgstat.c's `bool pgstat_report_fixed`: written directly by the xlog
    // insert path, read/cleared by pgstat_report_stat.
    PGSTAT_REPORT_FIXED, PgStatReportFixed, SetPgStatReportFixed, bool, false;
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
    // 8->16, matching guc_tables::tables boot_vals (see jit-parallel-defaults).
    MAX_WORKER_PROCESSES, max_worker_processes, set_max_worker_processes, i32, 16;
    MAX_PARALLEL_WORKERS, max_parallel_workers, set_max_parallel_workers, i32, 16;
    MAX_BACKENDS, MaxBackends, SetMaxBackends, i32, 0;
    // M2 pool-binding: PGPROCs boot-reserved for the STANDING runtime
    // executor gang (parallel::standing) — an extra MaxBackends term and a
    // widened bgworker freelist segment (postinit::InitializeMaxBackends,
    // lmgr_proc::InitProcGlobal), so the gang never competes with the
    // bgworker/parallel-worker budgets. Set by the postmaster BEFORE
    // InitializeMaxBackends, only under PGRUST_RUNTIME=1; default 0 =
    // byte-identical sizing.
    RUNTIME_GANG_PROCS, RuntimeGangProcs, SetRuntimeGangProcs, i32, 0;

    VACUUM_BUFFER_USAGE_LIMIT, VacuumBufferUsageLimit, SetVacuumBufferUsageLimit, i32, 2048;
    VACUUM_COST_PAGE_HIT, VacuumCostPageHit, SetVacuumCostPageHit, i32, 1;
    VACUUM_COST_PAGE_MISS, VacuumCostPageMiss, SetVacuumCostPageMiss, i32, 2;
    VACUUM_COST_PAGE_DIRTY, VacuumCostPageDirty, SetVacuumCostPageDirty, i32, 20;
    VACUUM_COST_LIMIT, VacuumCostLimit, SetVacuumCostLimit, i32, 200;
    VACUUM_COST_DELAY, VacuumCostDelay, SetVacuumCostDelay, f64, 0.0;
    VACUUM_COST_BALANCE, VacuumCostBalance, SetVacuumCostBalance, i32, 0;
    VACUUM_COST_ACTIVE, VacuumCostActive, SetVacuumCostActive, bool, false;

    AUTOVACUUM_FREEZE_MAX_AGE, autovacuum_freeze_max_age, set_autovacuum_freeze_max_age,
        i32, 200000000;

    COMMIT_TIMESTAMP_BUFFERS, commit_timestamp_buffers, set_commit_timestamp_buffers, i32, 0;
    MULTIXACT_MEMBER_BUFFERS, multixact_member_buffers, set_multixact_member_buffers, i32, 32;
    MULTIXACT_OFFSET_BUFFERS, multixact_offset_buffers, set_multixact_offset_buffers, i32, 16;
    NOTIFY_BUFFERS, notify_buffers, set_notify_buffers, i32, 16;
    SERIALIZABLE_BUFFERS, serializable_buffers, set_serializable_buffers, i32, 32;
    SUBTRANSACTION_BUFFERS, subtransaction_buffers, set_subtransaction_buffers, i32, 0;
    TRANSACTION_BUFFERS, transaction_buffers, set_transaction_buffers, i32, 0;
}

// Cross-thread-writable, CFI-visible async wakes; leaked (notes/timeout-threads.md).
thread_local! {
    static INTERRUPT_PENDING: Cell<*const std::sync::atomic::AtomicBool> =
        const { Cell::new(std::ptr::null()) };
}

#[inline]
pub fn interrupt_pending_flag() -> &'static std::sync::atomic::AtomicBool {
    let p = INTERRUPT_PENDING.get();
    if p.is_null() {
        claim_interrupt_pending_flag()
    } else {
        // SAFETY: only ever null or Box::leak'd.
        unsafe { &*p }
    }
}

#[cold]
#[inline(never)]
fn claim_interrupt_pending_flag() -> &'static std::sync::atomic::AtomicBool {
    let flag: &'static _ = Box::leak(Box::new(std::sync::atomic::AtomicBool::new(false)));
    INTERRUPT_PENDING.set(flag);
    flag
}

// Per-tuple hot (C's `if (INTERRUPT_PENDING)` shape): leaf, no claim edge —
// unclaimed reads false; every setter/publisher claims via
// interrupt_pending_flag() first, so null never hides a raised interrupt.
#[inline(always)]
pub fn InterruptPending() -> bool {
    let p = INTERRUPT_PENDING.get();
    // SAFETY: only ever null or Box::leak'd.
    !p.is_null() && unsafe { &*p }.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn SetInterruptPending(value: bool) {
    interrupt_pending_flag().store(value, std::sync::atomic::Ordering::SeqCst);
}

// `ClientSocket *MyClientSocket` / `struct Port *MyProcPort` (globals.c).
// Port is droppy: ManuallyDrop keeps the TLS const-init with no dtor state
// machine; the Port leaks at thread exit as C's TopMemoryContext copy does at
// process exit (per-connection reclaim is thread-model debt, see M5 note in
// docs/strategy.md — session state must not assume thread identity).
thread_local! {
    static MY_CLIENT_SOCKET: Cell<Option<types_startup::ClientSocket>> = const { Cell::new(None) };
    static MY_PROC_PORT: RefCell<Option<ManuallyDrop<Box<types_startup::Port>>>> =
        const { RefCell::new(None) };
}

pub fn MyClientSocket() -> Option<types_startup::ClientSocket> {
    MY_CLIENT_SOCKET.get()
}

pub fn SetMyClientSocket(sock: types_startup::ClientSocket) {
    MY_CLIENT_SOCKET.set(Some(sock));
}

pub fn HaveMyProcPort() -> bool {
    MY_PROC_PORT.with(|p| p.borrow().is_some())
}

/// Session-memory teardown (FPBUDGET-1): free the Port copy at clean task
/// end (C's TopMemoryContext copy dies with the backend process). Nothing
/// reads MyProcPort after teardown; the thread is exiting.
pub fn SessionMemTeardownPort() {
    MY_PROC_PORT.with(|p| {
        if let Some(port) = p.borrow_mut().take() {
            drop(ManuallyDrop::into_inner(port));
        }
    });
}

pub fn SetMyProcPort(port: types_startup::Port) {
    MY_PROC_PORT.with(|p| *p.borrow_mut() = Some(ManuallyDrop::new(Box::new(port))));
}

pub fn WithMyProcPort<R>(f: impl FnOnce(&mut types_startup::Port) -> R) -> R {
    MY_PROC_PORT.with(|p| {
        let mut slot = p.borrow_mut();
        let port = slot.as_mut().expect("MyProcPort is not set");
        f(port)
    })
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
    // C's Assert: debug-only. This rides every LWLockRelease (per-snapshot
    // hot); a release-mode panic branch here is cost C does not pay.
    debug_assert!(count > 0, "InterruptHoldoffCount underflow");
    SetInterruptHoldoffCount(count.saturating_sub(1));
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

/// `getpid()` / `std::process::id()` with wasm and sim arms. WASI has no
/// process ids (std::process::id PANICS there); the instance is the only
/// process that can exist, so a fixed synthetic pid stands in. 1 is C's
/// classic "the only process" number; nonzero matters (0/negative are
/// sentinel words in the lockfile/procsignal vocabularies).
///
/// Under `--cfg pgrust_sim` the OS pid is ambient entropy: it reaches the
/// wire (BackendKeyData) and file names (xlogtemp), and the P4 sim-net
/// determinism gate byte-compares both across runs — the sim node is one
/// process, so the same fixed synthetic pid stands in (P2 seeded-RNG law
/// applied to the one non-RNG identity source).
#[inline]
pub fn process_id() -> u32 {
    #[cfg(not(any(target_family = "wasm", pgrust_sim)))]
    {
        std::process::id()
    }
    #[cfg(any(target_family = "wasm", pgrust_sim))]
    {
        1
    }
}

/// C's `ProcNumberForTempRelations()` macro (procnumber.h:52-54), verbatim:
///
/// ```c
/// (ParallelLeaderProcNumber == INVALID_PROC_NUMBER ? MyProcNumber : ParallelLeaderProcNumber)
/// ```
///
/// The proc number to use for OUR SESSION's temp relations is normally our own,
/// but a parallel worker must use its LEADER's — a temp relation's storage is
/// keyed by proc number (`t<procno>_<relnode>`) and lives in the leader's local
/// buffers, so a worker resolving against its own slot would look in the wrong
/// place entirely.
///
/// Today this returns `MyProcNumber()` on every call, because
/// `SetParallelLeaderProcNumber` has no callers: the global is never populated.
/// That is *why* the two relcache sites that open-coded `MyProcNumber()` were
/// not a live defect — but the equivalence rested on that accident plus four
/// independent guards keeping workers away from temp relations, not on the
/// expression being right. Spelled out here so the sites stop asserting a macro
/// they did not implement, and so wiring the setter is the only remaining step
/// rather than one of two. See notes/GL-VACGUARD-1-letter.md §10.
#[inline]
pub fn ProcNumberForTempRelations() -> ProcNumber {
    let leader = ParallelLeaderProcNumber();
    if leader == INVALID_PROC_NUMBER {
        MyProcNumber()
    } else {
        leader
    }
}
