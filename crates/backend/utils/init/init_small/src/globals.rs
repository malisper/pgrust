//! `src/backend/utils/init/globals.c` — global variable declarations.
//!
//! Globals used all over the place should be declared here and not in other
//! modules.
//!
//! Every variable in `globals.c` is backend-private state (each C backend is
//! a process and owns its own copy), so each is a `thread_local!` cell here,
//! never a shared static. Pointer-valued C globals (`MyClientSocket`,
//! `MyProcPort`, `DataDir`, `DatabasePath`) become owned optional values;
//! `MyLatch` — a pointer to a latch mutated cross-process — stays a handle
//! (`Arc<Latch>`) to the shared object; the fixed-size `char` path buffers
//! stay fixed-size byte arrays. Names, types-widths, and initial values
//! match the C declarations one-to-one. (`EXEC_BACKEND`'s
//! `postgres_exec_path` is compiled out, as in the unix build.)
//!
//! Five of these variables (`FrontendProtocol`, `CritSectionCount`,
//! `IsUnderPostmaster`, `ExitOnAnyError`, `OutputFileName`) are also read —
//! and for `CritSectionCount`, written — by the already-ported elog.c, whose
//! backend-local store for them lives in `utils_error::config`. To
//! keep each C variable a single variable, the accessors here delegate to
//! that store rather than keeping a second copy.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use types_core::{
    init::BackendType, pg_time_t, uint32, uint8, InvalidOid, Oid, ProcNumber, ProtocolVersion,
    TimestampTz, DATEORDER_MDY, INTSTYLE_POSTGRES, INVALID_PROC_NUMBER, MAXPGPATH,
    MAX_CANCEL_KEY_LENGTH, PG_DIR_MODE_OWNER, USE_ISO_DATES,
};
pub use types_core::pid_t;
use net::{ClientSocket, Port};
use types_storage::latch::Latch;

/// One backend-private scalar global: a `thread_local!` `Cell` plus a getter
/// named after the C variable and a setter.
macro_rules! scalar_global {
    ($(#[$attr:meta])* $cell:ident, $get:ident, $set:ident, $ty:ty, $init:expr) => {
        thread_local! {
            $(#[$attr])*
            static $cell: Cell<$ty> = const { Cell::new($init) };
        }

        $(#[$attr])*
        #[inline]
        pub fn $get() -> $ty {
            $cell.get()
        }

        #[inline]
        pub fn $set(value: $ty) {
            $cell.set(value);
        }
    };
}

// `FrontendProtocol`, `CritSectionCount`, `IsUnderPostmaster`,
// `ExitOnAnyError`, and `OutputFileName` are globals.c variables that elog.c
// also reads (and, for `CritSectionCount`, writes during ERROR recovery). The
// error crate already keeps the backend-local store for them in
// `utils_error::config`; a second cell here would split the single C
// variable into two diverging copies, so the C-named accessors delegate to
// that store instead.

/// `ProtocolVersion FrontendProtocol;`
#[inline]
pub fn FrontendProtocol() -> ProtocolVersion {
    utils_error::config::frontend_protocol()
}

#[inline]
pub fn SetFrontendProtocol(value: ProtocolVersion) {
    utils_error::config::set_frontend_protocol(value);
}

// `volatile sig_atomic_t` interrupt/signal flags. C stores them as 0/1;
// presented as `bool`.
scalar_global!(
    /// `volatile sig_atomic_t InterruptPending = false;`
    INTERRUPT_PENDING, InterruptPending, SetInterruptPending, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t QueryCancelPending = false;`
    QUERY_CANCEL_PENDING, QueryCancelPending, SetQueryCancelPending, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t ProcDiePending = false;`
    PROC_DIE_PENDING, ProcDiePending, SetProcDiePending, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t CheckClientConnectionPending = false;`
    CHECK_CLIENT_CONNECTION_PENDING, CheckClientConnectionPending,
    SetCheckClientConnectionPending, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t ClientConnectionLost = false;`
    CLIENT_CONNECTION_LOST, ClientConnectionLost, SetClientConnectionLost, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t IdleInTransactionSessionTimeoutPending = false;`
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT_PENDING, IdleInTransactionSessionTimeoutPending,
    SetIdleInTransactionSessionTimeoutPending, bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t TransactionTimeoutPending = false;`
    TRANSACTION_TIMEOUT_PENDING, TransactionTimeoutPending, SetTransactionTimeoutPending,
    bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t IdleSessionTimeoutPending = false;`
    IDLE_SESSION_TIMEOUT_PENDING, IdleSessionTimeoutPending, SetIdleSessionTimeoutPending,
    bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t ProcSignalBarrierPending = false;`
    PROC_SIGNAL_BARRIER_PENDING, ProcSignalBarrierPending, SetProcSignalBarrierPending,
    bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t LogMemoryContextPending = false;`
    LOG_MEMORY_CONTEXT_PENDING, LogMemoryContextPending, SetLogMemoryContextPending,
    bool, false
);
scalar_global!(
    /// `volatile sig_atomic_t IdleStatsUpdateTimeoutPending = false;`
    IDLE_STATS_UPDATE_TIMEOUT_PENDING, IdleStatsUpdateTimeoutPending,
    SetIdleStatsUpdateTimeoutPending, bool, false
);

scalar_global!(
    /// `volatile uint32 InterruptHoldoffCount = 0;`
    INTERRUPT_HOLDOFF_COUNT, InterruptHoldoffCount, SetInterruptHoldoffCount, uint32, 0
);
scalar_global!(
    /// `volatile uint32 QueryCancelHoldoffCount = 0;`
    QUERY_CANCEL_HOLDOFF_COUNT, QueryCancelHoldoffCount, SetQueryCancelHoldoffCount, uint32, 0
);
/// `volatile uint32 CritSectionCount = 0;` — single store in
/// `utils_error::config` (errfinish resets it to 0 during ERROR
/// recovery, exactly as C does).
#[inline]
pub fn CritSectionCount() -> uint32 {
    utils_error::config::crit_section_count()
}

#[inline]
pub fn SetCritSectionCount(value: uint32) {
    utils_error::config::set_crit_section_count(value);
}

scalar_global!(
    /// `int MyProcPid;`
    MY_PROC_PID, MyProcPid, SetMyProcPid, i32, 0
);
scalar_global!(
    /// `pg_time_t MyStartTime;`
    MY_START_TIME, MyStartTime, SetMyStartTime, pg_time_t, 0
);
scalar_global!(
    /// `TimestampTz MyStartTimestamp;`
    MY_START_TIMESTAMP, MyStartTimestamp, SetMyStartTimestamp, TimestampTz, 0
);
scalar_global!(
    /// `TimestampTz PgStartTime;` (globals.c) — the timestamp of the
    /// postmaster's (or, in single-user mode, the standalone backend's) start,
    /// reported by `pg_postmaster_start_time()`. The postmaster assigns it with
    /// `PgStartTime = GetCurrentTimestamp();` near startup; `SetPgStartTime` is
    /// the setter the boot path uses.
    PG_START_TIME, PgStartTime, SetPgStartTime, TimestampTz, 0
);
scalar_global!(
    /// `uint8 MyCancelKey[MAX_CANCEL_KEY_LENGTH];`
    MY_CANCEL_KEY, MyCancelKey, SetMyCancelKey, [uint8; MAX_CANCEL_KEY_LENGTH],
    [0; MAX_CANCEL_KEY_LENGTH]
);
scalar_global!(
    /// `int MyCancelKeyLength = 0;`
    MY_CANCEL_KEY_LENGTH, MyCancelKeyLength, SetMyCancelKeyLength, i32, 0
);
scalar_global!(
    /// `int MyPMChildSlot;`
    MY_PM_CHILD_SLOT, MyPMChildSlot, SetMyPMChildSlot, i32, 0
);

scalar_global!(
    /// Mode of the data directory (`int data_directory_mode`). The default is
    /// 0700 but it may be changed in `checkDataDir()` to 0750 if the data
    /// directory actually has that mode.
    DATA_DIRECTORY_MODE, data_directory_mode, set_data_directory_mode, i32, PG_DIR_MODE_OWNER
);

/// `char OutputFileName[MAXPGPATH];` — debugging output file. Single store
/// in `utils_error::config` (`DebugFileOpen` reads it there), which
/// keeps it as the C string contents (`None`/empty == `'\0'`-empty buffer);
/// the C-shaped fixed-size buffer is reconstructed on read.
#[inline]
pub fn OutputFileName() -> [u8; MAXPGPATH] {
    let mut buf = [0u8; MAXPGPATH];
    if let Some(name) = utils_error::config::output_file_name() {
        let bytes = name.as_bytes();
        // Like C's strlcpy into a MAXPGPATH buffer: keep at most
        // MAXPGPATH - 1 bytes and NUL-terminate.
        let len = bytes.len().min(MAXPGPATH - 1);
        buf[..len].copy_from_slice(&bytes[..len]);
    }
    buf
}

pub fn SetOutputFileName(value: [u8; MAXPGPATH]) {
    let len = value.iter().position(|&b| b == 0).unwrap_or(MAXPGPATH);
    let name = String::from_utf8_lossy(&value[..len]).into_owned();
    utils_error::config::set_output_file_name(if name.is_empty() {
        None
    } else {
        Some(name)
    });
}

/// String-typed setter for `OutputFileName` (the value is always written
/// from `argv` text in C).
pub fn SetOutputFileNameStr(value: &str) {
    utils_error::config::set_output_file_name(if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    });
}
scalar_global!(
    /// `char my_exec_path[MAXPGPATH];` — full path to my executable.
    MY_EXEC_PATH, my_exec_path, set_my_exec_path, [u8; MAXPGPATH], [0; MAXPGPATH]
);
scalar_global!(
    /// `char pkglib_path[MAXPGPATH];` — full path to lib directory.
    PKGLIB_PATH, pkglib_path, set_pkglib_path, [u8; MAXPGPATH], [0; MAXPGPATH]
);

scalar_global!(
    /// `ProcNumber MyProcNumber = INVALID_PROC_NUMBER;`
    MY_PROC_NUMBER, MyProcNumber, SetMyProcNumber, ProcNumber, INVALID_PROC_NUMBER
);
scalar_global!(
    /// `ProcNumber ParallelLeaderProcNumber = INVALID_PROC_NUMBER;`
    PARALLEL_LEADER_PROC_NUMBER, ParallelLeaderProcNumber, SetParallelLeaderProcNumber,
    ProcNumber, INVALID_PROC_NUMBER
);

scalar_global!(
    /// `Oid MyDatabaseId = InvalidOid;`
    MY_DATABASE_ID, MyDatabaseId, SetMyDatabaseId, Oid, InvalidOid
);
scalar_global!(
    /// `Oid MyDatabaseTableSpace = InvalidOid;`
    MY_DATABASE_TABLE_SPACE, MyDatabaseTableSpace, SetMyDatabaseTableSpace, Oid, InvalidOid
);
scalar_global!(
    /// `bool MyDatabaseHasLoginEventTriggers = false;`
    MY_DATABASE_HAS_LOGIN_EVENT_TRIGGERS, MyDatabaseHasLoginEventTriggers,
    SetMyDatabaseHasLoginEventTriggers, bool, false
);

scalar_global!(
    /// `pid_t PostmasterPid = 0;`
    POSTMASTER_PID, PostmasterPid, SetPostmasterPid, pid_t, 0
);

// IsPostmasterEnvironment is true in a postmaster process and any postmaster
// child process; it is false in a standalone process (bootstrap or
// standalone backend). IsUnderPostmaster is true in postmaster child
// processes. These should be set correctly as early as possible in the
// execution of a process, so that error handling will do the right things if
// an error should occur during process initialization. They are initialized
// for the bootstrap/standalone case.
scalar_global!(
    /// `bool IsPostmasterEnvironment = false;`
    IS_POSTMASTER_ENVIRONMENT, IsPostmasterEnvironment, SetIsPostmasterEnvironment, bool, false
);
/// `bool IsUnderPostmaster = false;` — single store in
/// `utils_error::config` (elog.c reads it).
#[inline]
pub fn IsUnderPostmaster() -> bool {
    utils_error::config::is_under_postmaster()
}

#[inline]
pub fn SetIsUnderPostmaster(value: bool) {
    utils_error::config::set_is_under_postmaster(value);
}
scalar_global!(
    /// `bool IsBinaryUpgrade = false;`
    IS_BINARY_UPGRADE, IsBinaryUpgrade, SetIsBinaryUpgrade, bool, false
);
scalar_global!(
    /// `Oid binary_upgrade_next_toast_pg_class_oid = InvalidOid;` (heap.c) —
    /// the pg_upgrade-supplied OID for the next TOAST table's pg_class row.
    /// Part of the pg_upgrade backend-global family (set by the
    /// `binary_upgrade_set_next_toast_pg_class_oid` support function, consumed
    /// by `heap_create_with_catalog` and `toasting.c`).
    BINARY_UPGRADE_NEXT_TOAST_PG_CLASS_OID,
    binary_upgrade_next_toast_pg_class_oid,
    set_binary_upgrade_next_toast_pg_class_oid,
    Oid,
    InvalidOid
);
/// `bool ExitOnAnyError = false;` — single store in
/// `utils_error::config` (errstart promotes ERROR to FATAL on it).
#[inline]
pub fn ExitOnAnyError() -> bool {
    utils_error::config::exit_on_any_error()
}

#[inline]
pub fn SetExitOnAnyError(value: bool) {
    utils_error::config::set_exit_on_any_error(value);
}

scalar_global!(
    /// `int DateStyle = USE_ISO_DATES;`
    DATE_STYLE, DateStyle, SetDateStyle, i32, USE_ISO_DATES
);
scalar_global!(
    /// `int DateOrder = DATEORDER_MDY;`
    DATE_ORDER, DateOrder, SetDateOrder, i32, DATEORDER_MDY
);
scalar_global!(
    /// `int IntervalStyle = INTSTYLE_POSTGRES;`
    INTERVAL_STYLE, IntervalStyle, SetIntervalStyle, i32, INTSTYLE_POSTGRES
);

scalar_global!(
    /// `bool enableFsync = true;`
    ENABLE_FSYNC, enableFsync, set_enableFsync, bool, true
);
scalar_global!(
    /// `bool allowSystemTableMods = false;`
    ALLOW_SYSTEM_TABLE_MODS, allowSystemTableMods, set_allowSystemTableMods, bool, false
);
scalar_global!(
    /// `int work_mem = 4096;`
    WORK_MEM, work_mem, set_work_mem, i32, 4096
);
scalar_global!(
    /// `double hash_mem_multiplier = 2.0;`
    HASH_MEM_MULTIPLIER, hash_mem_multiplier, set_hash_mem_multiplier, f64, 2.0
);
scalar_global!(
    /// `int maintenance_work_mem = 65536;`
    MAINTENANCE_WORK_MEM, maintenance_work_mem, set_maintenance_work_mem, i32, 65536
);
scalar_global!(
    /// `int max_parallel_maintenance_workers = 2;`
    MAX_PARALLEL_MAINTENANCE_WORKERS, max_parallel_maintenance_workers,
    set_max_parallel_maintenance_workers, i32, 2
);

// Primary determinants of sizes of shared-memory structures. MaxBackends is
// computed by PostmasterMain after modules have had a chance to register
// background workers.
scalar_global!(
    /// `int NBuffers = 16384;`
    N_BUFFERS, NBuffers, SetNBuffers, i32, 16384
);
scalar_global!(
    /// `int NLocBuffer = 0;` (globals.c) — number of local (temp-table) buffers;
    /// set during `InitLocalBuffers`.
    N_LOC_BUFFER, NLocBuffer, SetNLocBuffer, i32, 0
);
scalar_global!(
    /// `int MaxConnections = 100;`
    MAX_CONNECTIONS, MaxConnections, SetMaxConnections, i32, 100
);
scalar_global!(
    /// `int max_worker_processes = 8;`
    MAX_WORKER_PROCESSES, max_worker_processes, set_max_worker_processes, i32, 8
);
scalar_global!(
    /// `int max_parallel_workers = 8;`
    MAX_PARALLEL_WORKERS, max_parallel_workers, set_max_parallel_workers, i32, 8
);
scalar_global!(
    /// `int MaxBackends = 0;`
    MAX_BACKENDS, MaxBackends, SetMaxBackends, i32, 0
);
scalar_global!(
    /// `int FastPathLockGroupsPerBackend = 0;` — the number of fast-path lock
    /// groups per backend, computed at startup by `InitializeFastPathLocks`.
    FAST_PATH_LOCK_GROUPS_PER_BACKEND, fast_path_lock_groups_per_backend,
    set_fast_path_lock_groups_per_backend, i32, 0
);

scalar_global!(
    /// `int VacuumBufferUsageLimit = 2048;`
    VACUUM_BUFFER_USAGE_LIMIT, VacuumBufferUsageLimit, SetVacuumBufferUsageLimit, i32, 2048
);
scalar_global!(
    /// `int VacuumCostPageHit = 1;`
    VACUUM_COST_PAGE_HIT, VacuumCostPageHit, SetVacuumCostPageHit, i32, 1
);
scalar_global!(
    /// `int VacuumCostPageMiss = 2;`
    VACUUM_COST_PAGE_MISS, VacuumCostPageMiss, SetVacuumCostPageMiss, i32, 2
);
scalar_global!(
    /// `int VacuumCostPageDirty = 20;`
    VACUUM_COST_PAGE_DIRTY, VacuumCostPageDirty, SetVacuumCostPageDirty, i32, 20
);
scalar_global!(
    /// `int VacuumCostLimit = 200;`
    VACUUM_COST_LIMIT, VacuumCostLimit, SetVacuumCostLimit, i32, 200
);
scalar_global!(
    /// `double VacuumCostDelay = 0;`
    VACUUM_COST_DELAY, VacuumCostDelay, SetVacuumCostDelay, f64, 0.0
);
scalar_global!(
    /// `int VacuumCostBalance = 0;` — working state for vacuum.
    VACUUM_COST_BALANCE, VacuumCostBalance, SetVacuumCostBalance, i32, 0
);
scalar_global!(
    /// `bool VacuumCostActive = false;`
    VACUUM_COST_ACTIVE, VacuumCostActive, SetVacuumCostActive, bool, false
);

// Configurable SLRU buffer sizes.
scalar_global!(
    /// `int commit_timestamp_buffers = 0;`
    COMMIT_TIMESTAMP_BUFFERS, commit_timestamp_buffers, set_commit_timestamp_buffers, i32, 0
);
scalar_global!(
    /// `int multixact_member_buffers = 32;`
    MULTIXACT_MEMBER_BUFFERS, multixact_member_buffers, set_multixact_member_buffers, i32, 32
);
scalar_global!(
    /// `int multixact_offset_buffers = 16;`
    MULTIXACT_OFFSET_BUFFERS, multixact_offset_buffers, set_multixact_offset_buffers, i32, 16
);
scalar_global!(
    /// `int notify_buffers = 16;`
    NOTIFY_BUFFERS, notify_buffers, set_notify_buffers, i32, 16
);
scalar_global!(
    /// `int serializable_buffers = 32;`
    SERIALIZABLE_BUFFERS, serializable_buffers, set_serializable_buffers, i32, 32
);
scalar_global!(
    /// `int subtransaction_buffers = 0;`
    SUBTRANSACTION_BUFFERS, subtransaction_buffers, set_subtransaction_buffers, i32, 0
);
scalar_global!(
    /// `int transaction_buffers = 0;`
    TRANSACTION_BUFFERS, transaction_buffers, set_transaction_buffers, i32, 0
);

// ----- Pointer-valued globals -----
//
// In C these are raw pointers to memory owned elsewhere; here the backend
// owns the value. Each gets a getter (copy/clone), a setter that
// installs/replaces the value, an `IsSet` presence test (the C `!= NULL`
// check), and a `Take` that removes and returns the value.

thread_local! {
    /// `struct ClientSocket *MyClientSocket;`
    static MY_CLIENT_SOCKET: Cell<Option<ClientSocket>> = const { Cell::new(None) };

    /// `struct Port *MyProcPort;`
    static MY_PROC_PORT: RefCell<Option<Box<Port>>> = const { RefCell::new(None) };

    /// Raw pointer at the live `MyProcPort` while a [`WithMyProcPort`] closure is
    /// running. C's `MyProcPort` is a plain global pointer that the interrupt
    /// handlers (`ProcessNotifyInterrupt` → `pq_flush` → `socket_set_nonblocking`)
    /// dereference even while `secure_read`/`secure_write` are mid-flight on the
    /// same `Port` (the read is blocked in a WaitEventSet at that point, so it is
    /// not actively touching the port). This port lives behind a `Box`, so its
    /// heap address is stable; recording it here lets a re-entrant
    /// `WithMyProcPort` reach the same `Port` instead of seeing the cell emptied
    /// by the outer `take()`. Null when no closure is active.
    static MY_PROC_PORT_ACTIVE: Cell<*mut Port> = const { Cell::new(core::ptr::null_mut()) };

    /// `struct Latch *MyLatch;` — the latch the current process should use
    /// for signal handling: a process-local latch when the process has no
    /// PGPROC entry, else `PGPROC->procLatch`. Thus it can always be used in
    /// signal handlers, without checking for its existence. A latch is
    /// mutated cross-process (`SetLatch` from signal handlers and, for
    /// shared latches, other backends), so `MyLatch` is a shared handle to
    /// the synchronized `Latch` object, never an owned copy.
    static MY_LATCH: RefCell<Option<Arc<Latch>>> = const { RefCell::new(None) };

    /// `char *DataDir = NULL;` — the absolute path to the top level of the
    /// PGDATA directory tree. Except during early startup, this is also the
    /// server's working directory; most code therefore can simply use
    /// relative paths and not reference DataDir explicitly.
    static DATA_DIR: RefCell<Option<String>> = const { RefCell::new(None) };

    /// `char *DatabasePath = NULL;` — the path (relative to `DataDir`) of my
    /// database's primary directory, ie, its directory in the default
    /// tablespace.
    static DATABASE_PATH: RefCell<Option<String>> = const { RefCell::new(None) };

    /// `const char *progname;` (main.c global) — the program name, set once at
    /// startup by `get_progname(argv[0])`. Read for the bad-command-line FATAL
    /// hint. The repo's main.c port computes it as a local; this global stands
    /// in for the C process global so the `progname` seam can serve it.
    static PROGNAME: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub fn MyClientSocket() -> Option<ClientSocket> {
    MY_CLIENT_SOCKET.get()
}

pub fn SetMyClientSocket(value: Option<ClientSocket>) {
    MY_CLIENT_SOCKET.set(value);
}

pub fn MyClientSocketIsSet() -> bool {
    MY_CLIENT_SOCKET.get().is_some()
}

pub fn TakeMyClientSocket() -> Option<ClientSocket> {
    MY_CLIENT_SOCKET.take()
}

/// Snapshot accessor: returns a deep clone of `MyProcPort`, if set. C reads
/// a pointer here; a caller that mutates the returned clone does *not*
/// affect the live value — use [`WithMyProcPort`] (the primary accessor) to
/// read or mutate the live `Port` without cloning.
pub fn MyProcPort() -> Option<Port> {
    MY_PROC_PORT.with_borrow(|p| p.as_deref().cloned())
}

pub fn SetMyProcPort(value: Option<Box<Port>>) {
    MY_PROC_PORT.set(value);
}

pub fn MyProcPortIsSet() -> bool {
    MY_PROC_PORT.with_borrow(Option::is_some)
}

pub fn TakeMyProcPort() -> Option<Box<Port>> {
    MY_PROC_PORT.take()
}

/// Run `f` against the live `MyProcPort` value (the C idiom of mutating
/// through the pointer), if set. The boxed value is taken out of the cell while
/// `f` runs and put back afterwards, so no `RefCell` borrow is held across `f`.
///
/// While `f` runs, a raw pointer at the (heap-stable) boxed `Port` is recorded
/// in [`MY_PROC_PORT_ACTIVE`]. This makes the accessor **re-entrant**: a nested
/// `WithMyProcPort` — as happens when a notify/catchup interrupt fires inside
/// the `secure_read`/`secure_write` WaitEventSet block and runs
/// `pq_flush` → `socket_set_nonblocking` against the same `Port` — reaches the
/// live port rather than seeing the cell emptied by the outer `take()`. This
/// mirrors C, where `MyProcPort` is a plain global pointer dereferenced freely
/// by the interrupt path while a blocking read is parked in WaitEventSetWait.
///
/// `MyProcPort()`/`MyProcPortIsSet()` (the clone/presence accessors) still see
/// the cell as emptied during `f`, matching the prior behavior; only the
/// in-place `WithMyProcPort` mutation path is re-entrant.
pub fn WithMyProcPort<R>(f: impl FnOnce(&mut Port) -> R) -> Option<R> {
    // Re-entrant case: an outer WithMyProcPort already holds the box out of the
    // cell and published its address; reach the same live Port through it.
    let active = MY_PROC_PORT_ACTIVE.with(Cell::get);
    if !active.is_null() {
        // SAFETY: `active` points at the boxed `Port` owned by the outer
        // `WithMyProcPort` frame, which stays alive (and at this fixed heap
        // address) for the whole duration of this nested call.
        return Some(f(unsafe { &mut *active }));
    }

    let mut port = MY_PROC_PORT.take()?;
    let ptr: *mut Port = &mut *port;
    MY_PROC_PORT_ACTIVE.with(|c| c.set(ptr));
    // Clear the published pointer when this frame returns OR unwinds, so a later
    // access never dereferences a stale/dropped `Box`.
    let _guard = ActivePortGuard;
    let result = f(&mut port);
    MY_PROC_PORT.set(Some(port));
    Some(result)
}

/// Clears [`MY_PROC_PORT_ACTIVE`] when the outer `WithMyProcPort` frame exits
/// (normal return or unwind), so the published raw pointer never outlives the
/// boxed `Port` it addressed. (On unwind the boxed `Port` is lost exactly as the
/// C `MyProcPort` global would be left dangling-but-unused after a FATAL; the
/// process is tearing down.)
struct ActivePortGuard;
impl Drop for ActivePortGuard {
    fn drop(&mut self) {
        MY_PROC_PORT_ACTIVE.with(|c| c.set(core::ptr::null_mut()));
    }
}

/// Returns the `MyLatch` handle (the C pointer copy), if set.
pub fn MyLatch() -> Option<Arc<Latch>> {
    MY_LATCH.with_borrow(Clone::clone)
}

pub fn SetMyLatch(value: Option<Arc<Latch>>) {
    MY_LATCH.set(value);
}

pub fn MyLatchIsSet() -> bool {
    MY_LATCH.with_borrow(Option::is_some)
}

pub fn TakeMyLatch() -> Option<Arc<Latch>> {
    MY_LATCH.take()
}

pub fn DataDir() -> Option<String> {
    DATA_DIR.with_borrow(Clone::clone)
}

/// `progname` (main.c global): the program name. Returns an empty string
/// before `set_progname` runs (mirroring a NULL/empty global read).
pub fn progname() -> String {
    PROGNAME.with_borrow(|p| p.clone().unwrap_or_default())
}

/// Set `progname` from `get_progname(argv[0])` at startup.
pub fn set_progname(value: String) {
    PROGNAME.replace(Some(value));
}

pub fn SetDataDir(value: Option<String>) {
    DATA_DIR.set(value);
}

pub fn DatabasePath() -> Option<String> {
    DATABASE_PATH.with_borrow(Clone::clone)
}

pub fn SetDatabasePath(value: Option<String>) {
    DATABASE_PATH.set(value);
}

scalar_global!(
    /// `BackendType MyBackendType;` (globals.c, declared in miscadmin.h) —
    /// this process's identity, assigned once at startup.
    MY_BACKEND_TYPE, MyBackendType, SetMyBackendType, BackendType, BackendType::Invalid
);

/// `HOLD_INTERRUPTS()` (miscadmin.h) — `InterruptHoldoffCount++`.
#[inline]
pub fn HoldInterrupts() {
    SetInterruptHoldoffCount(InterruptHoldoffCount() + 1);
}

/// `INTERRUPTS_CAN_BE_PROCESSED()` (miscadmin.h) — whether `ProcessInterrupts()`
/// is guaranteed to clear `InterruptPending`:
///
/// ```c
/// #define INTERRUPTS_CAN_BE_PROCESSED() \
///     (InterruptHoldoffCount == 0 && CritSectionCount == 0 && \
///      QueryCancelHoldoffCount == 0)
/// ```
#[inline]
pub fn InterruptsCanBeProcessed() -> bool {
    InterruptHoldoffCount() == 0 && CritSectionCount() == 0 && QueryCancelHoldoffCount() == 0
}

/// `RESUME_INTERRUPTS()` (miscadmin.h) — `InterruptHoldoffCount--` with
/// underflow assertion.
#[inline]
pub fn ResumeInterrupts() {
    let count = InterruptHoldoffCount();
    assert!(count > 0, "InterruptHoldoffCount underflow");
    SetInterruptHoldoffCount(count - 1);
}

/// `START_CRIT_SECTION()` (c.h) — increment `CritSectionCount`; while
/// non-zero any ERROR escalates to PANIC.
#[inline]
pub fn StartCriticalSection() {
    SetCritSectionCount(CritSectionCount() + 1);
}

/// `END_CRIT_SECTION()` (c.h) — decrement `CritSectionCount`.
#[inline]
pub fn EndCriticalSection() {
    let count = CritSectionCount();
    assert!(count > 0, "CritSectionCount underflow");
    SetCritSectionCount(count - 1);
}

/// Wrapper for the `with_my_proc_port` seam: adapts the callback-style seam
/// signature `fn(&mut dyn FnMut(Option<&mut Port>))` to the internal
/// `WithMyProcPort` which provides `Option<R>` from `FnOnce`.
pub fn with_my_proc_port_seam(f: &mut dyn FnMut(Option<&mut net::Port>)) {
    // Drive through `WithMyProcPort`, which is re-entrant: it reaches the live
    // `Port` both when it sits in the cell (the common case) and when an outer
    // `WithMyProcPort` frame currently holds it out and published its address
    // (a notify/catchup interrupt firing inside `secure_read`'s WaitEventSet).
    // Gating on `MyProcPortIsSet()` would wrongly take the `f(None)` branch in
    // that nested case (the cell is emptied by the outer `take()`), which is the
    // bug behind a spurious "there is no client connection" while delivering a
    // cross-backend NOTIFY. `WithMyProcPort` returns `None` only when there is
    // genuinely no port (neither in the cell nor published), matching C's
    // `MyProcPort == NULL`.
    if WithMyProcPort(|port| f(Some(port))).is_none() {
        f(None);
    }
}

// --- GUC-backed integer globals (declared in globals.c, defined as GUCs in
// guc_tables.c). The single store lives in the GUC-table slot; the C-named
// accessor reads it there, mirroring the way the elog-shared globals above
// delegate to `utils_error::config`. ---

/// `int PostAuthDelay = 0;` (globals.c) — seconds to sleep after
/// authentication. Read of the `PostAuthDelay` GUC slot.
#[inline]
pub fn post_auth_delay() -> i32 {
    guc_tables::vars::PostAuthDelay.read()
}

/// `int ReservedConnections = 0;` (globals.c) — the `reserved_connections`
/// GUC: connection slots reserved for non-superuser roles with the
/// `pg_use_reserved_connections` privilege.
#[inline]
pub fn reserved_connections() -> i32 {
    guc_tables::vars::ReservedConnections.read()
}

/// `int SuperuserReservedConnections = 3;` (globals.c) — the
/// `superuser_reserved_connections` GUC: connection slots reserved for
/// superusers.
#[inline]
pub fn superuser_reserved_connections() -> i32 {
    guc_tables::vars::SuperuserReservedConnections.read()
}

/// `int max_prepared_xacts = 0;` — the `max_prepared_xacts` GUC bounding the
/// number of concurrently-prepared transactions (dummy `PGPROC` slots). The C
/// global lives in `twophase.c`; its store is the `max_prepared_xacts` GUC
/// slot, read here.
#[inline]
pub fn max_prepared_xacts() -> i32 {
    guc_tables::vars::max_prepared_xacts.read()
}

/// `int autovacuum_worker_slots;` — the `autovacuum_worker_slots` GUC bounding
/// the number of autovacuum-worker slots. The C global lives in
/// `autovacuum.c`; its store is the `autovacuum_worker_slots` GUC slot, read
/// here.
#[inline]
pub fn autovacuum_worker_slots() -> i32 {
    guc_tables::vars::autovacuum_worker_slots.read()
}

/// `char *DatabasePath` (globals.c): the path to the current database's data
/// directory. Returns an owned copy. A `NULL` `DatabasePath` would be a
/// programming error (the path is set at backend startup before any consumer
/// reads it, mirroring the way the C `Port`-field accessors dereference once a
/// `Port` exists); it is reported here as an internal ERROR rather than a
/// segfault. `Err` also carries the OOM surface of copying the global.
pub fn database_path_seam() -> types_error::PgResult<String> {
    match DatabasePath() {
        Some(p) => Ok(p),
        None => {
            utils_error::ereport(types_error::ERROR)
                .errmsg_internal("DatabasePath is NULL")
                .finish(types_error::ErrorLocation::new(
                    file!(),
                    line!() as i32,
                    "DatabasePath",
                ))?;
            unreachable!("ereport(ERROR) does not return Ok")
        }
    }
}

// --- `MyProcPort->...` field accessors copied into an `mcx` context (the C
// idiom of reading the per-connection `Port` string fields). Each returns the
// OOM surface of the copy; the C `MyProcPort == NULL` surface is reported as
// an error, since these are only reached once a client `Port` is established
// (`PerformAuthentication`/`process_startup_options` in postinit.c). ---

/// The `MyProcPort == NULL` surface of the `Port`-field accessors. In C these
/// fields are read by direct pointer dereference once a client `Port` exists
/// (`PerformAuthentication`/`process_startup_options` in postinit.c); a NULL
/// `MyProcPort` would be a programming error, reported here as an internal
/// ERROR rather than a segfault.
fn no_proc_port<T>() -> types_error::PgResult<T> {
    utils_error::ereport(types_error::ERROR)
        .errmsg_internal("MyProcPort is NULL")
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "MyProcPort"))?;
    unreachable!("ereport(ERROR) does not return Ok")
}

/// `MyProcPort->user_name` (globals.c `Port`), copied into `mcx`.
pub fn my_proc_port_user_name<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<mcx::PgString<'mcx>> {
    let name = match WithMyProcPort(|p| p.user_name.clone()) {
        Some(n) => n,
        None => return no_proc_port(),
    };
    mcx::PgString::from_str_in(name.as_deref().unwrap_or(""), mcx)
}

/// `MyProcPort->database_name` (globals.c `Port`), copied into `mcx`.
pub fn my_proc_port_database_name<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<mcx::PgString<'mcx>> {
    let name = match WithMyProcPort(|p| p.database_name.clone()) {
        Some(n) => n,
        None => return no_proc_port(),
    };
    mcx::PgString::from_str_in(name.as_deref().unwrap_or(""), mcx)
}

/// `MyProcPort->application_name` (globals.c `Port`), copied into `mcx`, or
/// `None` if not set.
pub fn my_proc_port_application_name<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<Option<mcx::PgString<'mcx>>> {
    let app = match WithMyProcPort(|p| p.application_name.clone()) {
        Some(a) => a,
        None => return no_proc_port(),
    };
    match app {
        Some(s) => Ok(Some(mcx::PgString::from_str_in(&s, mcx)?)),
        None => Ok(None),
    }
}

/// `MyProcPort->cmdline_options` (globals.c `Port`), copied into `mcx`, or
/// `None` if absent.
pub fn my_proc_port_cmdline_options<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<Option<mcx::PgString<'mcx>>> {
    let opts = match WithMyProcPort(|p| p.cmdline_options.clone()) {
        Some(o) => o,
        None => return no_proc_port(),
    };
    match opts {
        Some(s) => Ok(Some(mcx::PgString::from_str_in(&s, mcx)?)),
        None => Ok(None),
    }
}

/// `MyProcPort->guc_options` (globals.c `Port`): the alternating name/value
/// GUC settings, copied into `mcx`.
pub fn my_proc_port_guc_options<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<mcx::PgVec<'mcx, mcx::PgString<'mcx>>> {
    let opts = match WithMyProcPort(|p| p.guc_options.clone()) {
        Some(o) => o,
        None => return no_proc_port(),
    };
    let mut out = mcx::vec_with_capacity_in(mcx, opts.len())?;
    for s in &opts {
        out.push(mcx::PgString::from_str_in(s, mcx)?);
    }
    Ok(out)
}
