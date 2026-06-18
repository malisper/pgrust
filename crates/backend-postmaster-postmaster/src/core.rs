//! Shared core for the postmaster port (`postmaster/postmaster.c`).
//!
//! Holds everything the C file keeps as a file-static / exported global, plus
//! the inline helpers (`BackendTypeMask`, the `PMState`/`StartupStatusEnum`
//! enums, the shutdown-mode constants, the wait-status decode helpers).
//!
//! ## Single-process state, owned in Rust
//!
//! The postmaster is **strictly single-threaded** (it forks rather than
//! threads), so its globals are never concurrently accessed. We model that
//! exactly: all mutable postmaster state lives in one owned [`PostmasterState`]
//! reached through [`pm`] / [`pm_mut`]. The special children and the IO-worker
//! array are tracked as owned `Option<PMChild>` values (pmchild owns the
//! underlying slab; `PMChild` is `Copy`, so the postmaster's singleton globals
//! hold the value just as the C globals hold a `PMChild *`).

use backend_postmaster_pmchild::PMChild;
use backend_storage_ipc_waiteventset_seams::WaitEventSet;
pub use types_core::init::BackendType;
use types_core::pgsocket;
use types_core::primitive::TimestampTz;

// ---------------------------------------------------------------------------
// BackendType aliases — C's `B_*` enumerators map to our `BackendType::*`.
// ---------------------------------------------------------------------------

pub const B_INVALID: BackendType = BackendType::Invalid;
pub const B_BACKEND: BackendType = BackendType::Backend;
pub const B_DEAD_END_BACKEND: BackendType = BackendType::DeadEndBackend;
pub const B_AUTOVAC_LAUNCHER: BackendType = BackendType::AutovacLauncher;
pub const B_AUTOVAC_WORKER: BackendType = BackendType::AutovacWorker;
pub const B_BG_WORKER: BackendType = BackendType::BgWorker;
pub const B_WAL_SENDER: BackendType = BackendType::WalSender;
pub const B_SLOTSYNC_WORKER: BackendType = BackendType::SlotsyncWorker;
pub const B_STANDALONE_BACKEND: BackendType = BackendType::StandaloneBackend;
pub const B_ARCHIVER: BackendType = BackendType::Archiver;
pub const B_BG_WRITER: BackendType = BackendType::BgWriter;
pub const B_CHECKPOINTER: BackendType = BackendType::Checkpointer;
pub const B_IO_WORKER: BackendType = BackendType::IoWorker;
pub const B_STARTUP: BackendType = BackendType::Startup;
pub const B_WAL_RECEIVER: BackendType = BackendType::WalReceiver;
pub const B_WAL_SUMMARIZER: BackendType = BackendType::WalSummarizer;
pub const B_WAL_WRITER: BackendType = BackendType::WalWriter;
pub const B_LOGGER: BackendType = BackendType::Logger;

// ---------------------------------------------------------------------------
// BackendTypeMask
// ---------------------------------------------------------------------------

/// `BACKEND_NUM_TYPES` — number of distinct [`BackendType`] values.
pub const BACKEND_NUM_TYPES: u32 = types_core::init::BACKEND_NUM_TYPES as u32;

// C: StaticAssertDecl(BACKEND_NUM_TYPES < 32, "too many backend types for uint32");
const _: () = assert!(BACKEND_NUM_TYPES < 32, "too many backend types for uint32");

/// C: `typedef struct { uint32 mask; } BackendTypeMask;`
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BackendTypeMask {
    pub mask: u32,
}

/// C: `static const BackendTypeMask BTYPE_MASK_ALL = {(1 << BACKEND_NUM_TYPES) - 1};`
pub const BTYPE_MASK_ALL: BackendTypeMask = BackendTypeMask {
    mask: (1u32 << BACKEND_NUM_TYPES) - 1,
};

/// C: `static const BackendTypeMask BTYPE_MASK_NONE = {0};`
pub const BTYPE_MASK_NONE: BackendTypeMask = BackendTypeMask { mask: 0 };

/// C: `static inline BackendTypeMask btmask(BackendType t)`
#[inline]
pub fn btmask(t: BackendType) -> BackendTypeMask {
    BackendTypeMask {
        mask: 1u32 << (t as u32),
    }
}

/// C: `static inline BackendTypeMask btmask_add_n(BackendTypeMask mask, int
/// nargs, BackendType *t)`
#[inline]
pub fn btmask_add_n(mut mask: BackendTypeMask, t: &[BackendType]) -> BackendTypeMask {
    for &bt in t {
        mask.mask |= 1u32 << (bt as u32);
    }
    mask
}

/// C macro: `btmask_add(mask, ...)`.
#[macro_export]
macro_rules! btmask_add {
    ($mask:expr $(, $t:expr )* $(,)?) => {
        $crate::core::btmask_add_n($mask, &[ $( $t ),* ])
    };
}

/// C: `static inline BackendTypeMask btmask_del(BackendTypeMask mask, BackendType t)`
#[inline]
pub fn btmask_del(mut mask: BackendTypeMask, t: BackendType) -> BackendTypeMask {
    mask.mask &= !(1u32 << (t as u32));
    mask
}

/// C: `static inline BackendTypeMask btmask_all_except_n(int nargs, BackendType *t)`
#[inline]
pub fn btmask_all_except_n(t: &[BackendType]) -> BackendTypeMask {
    let mut mask = BTYPE_MASK_ALL;
    for &bt in t {
        mask = btmask_del(mask, bt);
    }
    mask
}

/// C macro: `btmask_all_except(...)`.
#[macro_export]
macro_rules! btmask_all_except {
    ($( $t:expr ),* $(,)?) => {
        $crate::core::btmask_all_except_n(&[ $( $t ),* ])
    };
}

/// C: `static inline bool btmask_contains(BackendTypeMask mask, BackendType t)`
#[inline]
pub fn btmask_contains(mask: BackendTypeMask, t: BackendType) -> bool {
    (mask.mask & (1u32 << (t as u32))) != 0
}

// ---------------------------------------------------------------------------
// Local enums
// ---------------------------------------------------------------------------

/// C: `typedef enum { STARTUP_NOT_RUNNING, STARTUP_RUNNING, STARTUP_SIGNALED,
/// STARTUP_CRASHED } StartupStatusEnum;`
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum StartupStatusEnum {
    StartupNotRunning,
    StartupRunning,
    /// we sent it a SIGQUIT or SIGKILL
    StartupSignaled,
    StartupCrashed,
}

/// C: `typedef enum { PM_INIT, ... } PMState;`
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(i32)]
pub enum PMState {
    PmInit,
    PmStartup,
    PmRecovery,
    PmHotStandby,
    PmRun,
    PmStopBackends,
    PmWaitBackends,
    PmWaitXlogShutdown,
    PmWaitXlogArchival,
    PmWaitIoWorkers,
    PmWaitCheckpointer,
    PmWaitDeadEnd,
    PmNoChildren,
}

// ---------------------------------------------------------------------------
// Startup/shutdown mode constants
// ---------------------------------------------------------------------------

pub const NO_SHUTDOWN: i32 = 0;
pub const SMART_SHUTDOWN: i32 = 1;
pub const FAST_SHUTDOWN: i32 = 2;
pub const IMMEDIATE_SHUTDOWN: i32 = 3;

/// C: `#define SIGKILL_CHILDREN_AFTER_SECS 5`
pub const SIGKILL_CHILDREN_AFTER_SECS: i64 = 5;

/// C: `#define MAXLISTEN 64`
pub const MAXLISTEN: usize = 64;

/// C: maximum configurable IO worker processes (`storage/io_worker.h`).
pub const MAX_IO_WORKERS: usize = 32;

// ---------------------------------------------------------------------------
// POSIX signal numbers used by the postmaster (libc ABI constants).
// ---------------------------------------------------------------------------

pub const SIGHUP: i32 = libc::SIGHUP;
pub const SIGINT: i32 = libc::SIGINT;
pub const SIGQUIT: i32 = libc::SIGQUIT;
pub const SIGABRT: i32 = libc::SIGABRT;
pub const SIGKILL: i32 = libc::SIGKILL;
pub const SIGUSR1: i32 = libc::SIGUSR1;
pub const SIGUSR2: i32 = libc::SIGUSR2;
pub const SIGTERM: i32 = libc::SIGTERM;
pub const SIGCHLD: i32 = libc::SIGCHLD;

// ---------------------------------------------------------------------------
// Exit-status checking helpers (POSIX wait-status layout).
// ---------------------------------------------------------------------------

/// C: `WIFEXITED(status)`.
#[inline]
pub fn wifexited(status: i32) -> bool {
    (status & 0x7f) == 0
}

/// C: `WIFSIGNALED(status)`.
#[inline]
pub fn wifsignaled(status: i32) -> bool {
    let low = (status & 0x7f) as i8;
    (((low as i32) + 1) >> 1) > 0
}

/// C: `WEXITSTATUS(status)`.
#[inline]
pub fn wexitstatus(status: i32) -> i32 {
    (status >> 8) & 0xff
}

/// C: `WTERMSIG(status)`.
#[inline]
pub fn wtermsig(status: i32) -> i32 {
    status & 0x7f
}

/// C: `EXIT_STATUS_0(st)`.
#[inline]
pub fn exit_status_0(st: i32) -> bool {
    st == 0
}

/// C: `EXIT_STATUS_1(st)`.
#[inline]
pub fn exit_status_1(st: i32) -> bool {
    wifexited(st) && wexitstatus(st) == 1
}

/// C: `EXIT_STATUS_3(st)`.
#[inline]
pub fn exit_status_3(st: i32) -> bool {
    wifexited(st) && wexitstatus(st) == 3
}

// ---------------------------------------------------------------------------
// The postmaster's single-process state.
// ---------------------------------------------------------------------------

/// Every mutable file-static / exported global the postmaster keeps, owned in
/// one struct. Reached through [`pm`] / [`pm_mut`].
pub struct PostmasterState {
    /// C: `bool ClientAuthInProgress`.
    pub client_auth_in_progress: bool,

    /// C: `static pgsocket *ListenSockets` + `static int NumListenSockets`.
    /// The postmaster owns the listen-socket array (palloc'd in C); here it is
    /// an owned `Vec` whose `len()` is `NumListenSockets`.
    pub listen_sockets: Vec<pgsocket>,

    /// C: `static WaitEventSet *pm_wait_set` — the main-loop poll set. `None`
    /// is the C `NULL`.
    pub pm_wait_set: Option<WaitEventSet>,

    /// C: `static StartupStatusEnum StartupStatus`.
    pub startup_status: StartupStatusEnum,
    /// C: `static int Shutdown`.
    pub shutdown: i32,
    /// C: `static bool FatalError`.
    pub fatal_error: bool,
    /// C: `static PMState pmState`.
    pub pm_state: PMState,
    /// C: `static bool connsAllowed`.
    pub conns_allowed: bool,
    /// C: `static time_t AbortStartTime` (0 == off).
    pub abort_start_time: i64,
    /// C: `static bool ReachedNormalRunning`.
    pub reached_normal_running: bool,
    /// C: `static bool reachedConsistency` (postmaster.c file-static) — set when
    /// recovery signals PMSIGNAL_RECOVERY_CONSISTENT, cleared on
    /// PMSIGNAL_RECOVERY_STARTED.
    pub reached_consistency: bool,
    /// C: `static bool start_autovac_launcher`.
    pub start_autovac_launcher: bool,
    /// C: `static bool avlauncher_needs_signal`.
    pub avlauncher_needs_signal: bool,
    /// C: `static bool WalReceiverRequested`.
    pub wal_receiver_requested: bool,
    /// C: `static bool StartWorkerNeeded`.
    pub start_worker_needed: bool,
    /// C: `static bool HaveCrashedWorker`.
    pub have_crashed_worker: bool,

    /// NO C COUNTERPART — process-local-statics divergence bookkeeping.
    ///
    /// `true` once `CreateSharedMemoryAndSemaphores` has run in this postmaster
    /// process (set at boot in `main_entry`, and would be set by the crash-
    /// recovery reinit path). In C, crash recovery destroys and re-creates the
    /// OS shared-memory segment every time; in this tree the "shared" structures
    /// are process-local statics published into write-once cells (and handed out
    /// as `&'static`), so they can be created exactly once per process. The
    /// crashed child only corrupts its own fork-COW copy, never the
    /// postmaster's, so the postmaster's segment is still valid after a crash
    /// and re-creation is both unnecessary and unsafe. The reinit path consults
    /// this flag to skip the (panic-inducing) second creation. See
    /// `PostmasterStateMachine`.
    pub shmem_created: bool,

    /// C: `static int io_worker_count` + `static PMChild
    /// *io_worker_children[MAX_IO_WORKERS]`.
    pub io_worker_count: i32,
    pub io_worker_children: [Option<PMChild>; MAX_IO_WORKERS],

    // --- special-child handles (None when not running) --------------------
    pub startup_pmchild: Option<PMChild>,
    pub bgwriter_pmchild: Option<PMChild>,
    pub checkpointer_pmchild: Option<PMChild>,
    pub walwriter_pmchild: Option<PMChild>,
    pub walreceiver_pmchild: Option<PMChild>,
    pub walsummarizer_pmchild: Option<PMChild>,
    pub autovac_launcher_pmchild: Option<PMChild>,
    pub pgarch_pmchild: Option<PMChild>,
    pub syslogger_pmchild: Option<PMChild>,
    pub slotsync_worker_pmchild: Option<PMChild>,

    // --- pending-signal flags (set in signal handlers) --------------------
    pub pending_pm_pmsignal: bool,
    pub pending_pm_child_exit: bool,
    pub pending_pm_reload_request: bool,
    pub pending_pm_shutdown_request: bool,
    pub pending_pm_fast_shutdown_request: bool,
    pub pending_pm_immediate_shutdown_request: bool,
}

impl PostmasterState {
    const fn new() -> Self {
        PostmasterState {
            client_auth_in_progress: false,
            listen_sockets: Vec::new(),
            pm_wait_set: None,
            startup_status: StartupStatusEnum::StartupNotRunning,
            shutdown: NO_SHUTDOWN,
            fatal_error: false,
            pm_state: PMState::PmInit,
            conns_allowed: true,
            abort_start_time: 0,
            reached_normal_running: false,
            reached_consistency: false,
            start_autovac_launcher: false,
            avlauncher_needs_signal: false,
            wal_receiver_requested: false,
            // C: `static bool StartWorkerNeeded = true;`
            start_worker_needed: true,
            have_crashed_worker: false,
            shmem_created: false,

            io_worker_count: 0,
            io_worker_children: [None; MAX_IO_WORKERS],

            startup_pmchild: None,
            bgwriter_pmchild: None,
            checkpointer_pmchild: None,
            walwriter_pmchild: None,
            walreceiver_pmchild: None,
            walsummarizer_pmchild: None,
            autovac_launcher_pmchild: None,
            pgarch_pmchild: None,
            syslogger_pmchild: None,
            slotsync_worker_pmchild: None,

            pending_pm_pmsignal: false,
            pending_pm_child_exit: false,
            pending_pm_reload_request: false,
            pending_pm_shutdown_request: false,
            pending_pm_fast_shutdown_request: false,
            pending_pm_immediate_shutdown_request: false,
        }
    }
}

// The single-process postmaster state. The postmaster forks rather than
// threads, so this is never accessed concurrently. `static mut` faithfully
// models the C file statics. `WaitEventSet`/`Vec` are not `const`-constructible
// inline, so this is initialized lazily through an `Option`.
static mut PM_STATE: Option<PostmasterState> = None;

/// Borrow the postmaster's single-process state immutably.
///
/// SAFETY: the postmaster is strictly single-threaded; no concurrent access
/// exists, so this shared borrow is sound (mirrors a C read of a file static).
#[allow(static_mut_refs)]
#[inline]
pub fn pm() -> &'static PostmasterState {
    unsafe {
        if PM_STATE.is_none() {
            PM_STATE = Some(PostmasterState::new());
        }
        PM_STATE.as_ref().unwrap()
    }
}

/// Borrow the postmaster's single-process state mutably.
///
/// SAFETY: the postmaster is strictly single-threaded; no concurrent access
/// exists, so this exclusive borrow is sound (mirrors a C write of a file
/// static). Callers must not hold two `pm_mut` borrows live simultaneously.
#[allow(static_mut_refs)]
#[inline]
pub fn pm_mut() -> &'static mut PostmasterState {
    unsafe {
        if PM_STATE.is_none() {
            PM_STATE = Some(PostmasterState::new());
        }
        PM_STATE.as_mut().unwrap()
    }
}

/// Reset the postmaster's single-process state to its boot defaults (tests).
#[cfg(test)]
pub fn reset_for_test() {
    unsafe {
        PM_STATE = Some(PostmasterState::new());
    }
}

/// Re-export for convenience inside the crate.
pub use types_core::primitive::TimestampTz as Timestamp;
#[allow(unused_imports)]
use TimestampTz as _Timestamp;
