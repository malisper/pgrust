//! Port of `src/backend/storage/ipc/pmsignal.c`: signaling between the
//! postmaster and its child processes.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::sync::atomic::Ordering::SeqCst;

use ::utils_error::elog;
use ::types_core::sig_atomic_t;
use ::types_error::{ErrorLocation, PgResult, FATAL};

use crate::{add_size, mul_size};

const FILE: &str = "pmsignal.c";

// ---------------------------------------------------------------------------
// PM_CHILD_* slot states (these values must fit in sig_atomic_t).
// ---------------------------------------------------------------------------

/// `#define PM_CHILD_UNUSED 0` — slot available for assignment.
pub const PM_CHILD_UNUSED: sig_atomic_t = 0;
/// `#define PM_CHILD_ASSIGNED 1` — slot bound to a child that has not (yet)
/// touched shmem, or that has cleaned up after itself.
pub const PM_CHILD_ASSIGNED: sig_atomic_t = 1;
/// `#define PM_CHILD_ACTIVE 2` — child is actively using shared memory.
pub const PM_CHILD_ACTIVE: sig_atomic_t = 2;
/// `#define PM_CHILD_WALSENDER 3` — like ACTIVE, but the child is a WAL sender.
pub const PM_CHILD_WALSENDER: sig_atomic_t = 3;

/// `PMSignalReason` (`storage/pmsignal.h`) — reasons a child signals the
/// postmaster. Indexes into `PMSignalState->PMSignalFlags[NUM_PMSIGNALS]`.
///
/// ```c
/// typedef enum
/// {
///     PMSIGNAL_RECOVERY_STARTED,
///     PMSIGNAL_RECOVERY_CONSISTENT,
///     PMSIGNAL_BEGIN_HOT_STANDBY,
///     PMSIGNAL_ROTATE_LOGFILE,
///     PMSIGNAL_START_AUTOVAC_LAUNCHER,
///     PMSIGNAL_START_AUTOVAC_WORKER,
///     PMSIGNAL_BACKGROUND_WORKER_CHANGE,
///     PMSIGNAL_START_WALRECEIVER,
///     PMSIGNAL_ADVANCE_STATE_MACHINE,
///     PMSIGNAL_XLOG_IS_SHUTDOWN,
/// } PMSignalReason;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PMSignalReason {
    /// recovery has started
    PMSIGNAL_RECOVERY_STARTED = 0,
    /// recovery has reached consistent state
    PMSIGNAL_RECOVERY_CONSISTENT = 1,
    /// begin Hot Standby
    PMSIGNAL_BEGIN_HOT_STANDBY = 2,
    /// send SIGUSR1 to syslogger to rotate logfile
    PMSIGNAL_ROTATE_LOGFILE = 3,
    /// start an autovacuum launcher
    PMSIGNAL_START_AUTOVAC_LAUNCHER = 4,
    /// start an autovacuum worker
    PMSIGNAL_START_AUTOVAC_WORKER = 5,
    /// background worker state change
    PMSIGNAL_BACKGROUND_WORKER_CHANGE = 6,
    /// start a walreceiver
    PMSIGNAL_START_WALRECEIVER = 7,
    /// advance postmaster's state machine
    PMSIGNAL_ADVANCE_STATE_MACHINE = 8,
    /// ShutdownXLOG() completed
    PMSIGNAL_XLOG_IS_SHUTDOWN = 9,
}

/// `NUM_PMSIGNALS` (`PMSIGNAL_XLOG_IS_SHUTDOWN + 1`) — the length of the
/// `PMSignalFlags[]` per-reason bitmap.
pub const NUM_PMSIGNALS: usize = PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN as usize + 1;

/// `QuitSignalReason` (`storage/pmsignal.h`) — why the postmaster broadcast
/// SIGQUIT, communicated postmaster→children via `PMSignalState->sigquit_reason`.
///
/// ```c
/// typedef enum
/// {
///     PMQUIT_NOT_SENT = 0,
///     PMQUIT_FOR_CRASH,
///     PMQUIT_FOR_STOP,
/// } QuitSignalReason;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum QuitSignalReason {
    /// postmaster hasn't sent SIGQUIT
    PMQUIT_NOT_SENT = 0,
    /// some other backend bought the farm
    PMQUIT_FOR_CRASH = 1,
    /// immediate stop was commanded
    PMQUIT_FOR_STOP = 2,
}

impl QuitSignalReason {
    /// Decode the `u32` discriminant stored in the shared `sigquit_reason`
    /// field. A freshly-zeroed shmem region (before any SIGQUIT) reads as `0`
    /// == `PMQUIT_NOT_SENT`; any unknown value is likewise the not-sent
    /// sentinel.
    fn from_u32(value: u32) -> Self {
        match value {
            1 => QuitSignalReason::PMQUIT_FOR_CRASH,
            2 => QuitSignalReason::PMQUIT_FOR_STOP,
            _ => QuitSignalReason::PMQUIT_NOT_SENT,
        }
    }
}

// ---------------------------------------------------------------------------
// PMSignalData — the shared control block.
//
// C `struct PMSignalData` lives in shared memory and is valid in both the
// postmaster and its children. Here it is a process-global synchronized
// struct: the `volatile sig_atomic_t` arrays/fields become atomics (lock-free
// loads/stores, exactly the C discipline), the flexible `PMChildFlags[]`
// becomes a boxed slice sized at creation.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32};

/// `struct PMSignalData` — the fixed header of pmsignal.c's shared region, laid
/// out `#[repr(C)]` and resident in the *real* process-shared memory segment
/// (`ShmemInitStruct("PMSignalState", …)`). The flexible `PMChildFlags[]` array
/// immediately follows this header in the same shared allocation.
///
/// This MUST be in genuine shared memory (not a process-local `OnceLock`): a
/// child backend calls `SendPostmasterSignal`, which sets `pm_signal_flags[r]`,
/// and the postmaster reads it via `CheckPostmasterSignal`. With process-local
/// storage every forked child gets a private COW copy, so the postmaster never
/// observes a child's writes — which silently breaks every postmaster signal
/// (dynamic bgworker registration, recovery handoff, …). Every process maps the
/// same `MAP_SHARED` bytes; `AtomicBool`/`AtomicI32`/`AtomicU32` provide the
/// async-signal-safe `sig_atomic_t` semantics the C uses.
#[repr(C)]
struct PMSignalState {
    /// `sig_atomic_t PMSignalFlags[NUM_PMSIGNALS]` — per-reason signal flags.
    pm_signal_flags: [AtomicBool; NUM_PMSIGNALS],
    /// `QuitSignalReason sigquit_reason` — why SIGQUIT was sent.
    sigquit_reason: AtomicU32,
    /// `int num_child_flags` — number of entries in `PMChildFlags[]`.
    num_child_flags: AtomicI32,
    // `sig_atomic_t PMChildFlags[FLEXIBLE_ARRAY_MEMBER]` follows immediately
    // after this header in the shared allocation; accessed via `child_flag()`.
}

impl PMSignalState {
    /// `&PMSignalState->PMChildFlags[idx]` — the flexible array element, resident
    /// just past the header in the same shared allocation.
    #[inline]
    fn child_flag(&self, idx: usize) -> &AtomicI32 {
        debug_assert!((idx as i32) < self.num_child_flags.load(SeqCst));
        // SAFETY: `self` is the start of the shared region; `PMChildFlags`
        // begins at `offsetof(PMSignalData, PMChildFlags)` and holds
        // `num_child_flags` elements (see `PMSignalShmemSize`/`PMSignalShmemInit`).
        unsafe {
            let base = (self as *const PMSignalState as *const u8).add(PM_CHILD_FLAGS_OFFSET)
                as *const AtomicI32;
            &*base.add(idx)
        }
    }
}

/// `offsetof(PMSignalData, PMChildFlags)`. The flexible array of `AtomicI32`
/// needs 4-byte alignment, and the header ends on a 4-byte boundary
/// (`AtomicBool[NUM_PMSIGNALS]` packed + `AtomicU32` + `AtomicI32`), so the
/// offset is the Rust `size_of` of the header. Statically tie the access offset
/// to the real layout so it cannot diverge from `PMSignalShmemSize`.
const PM_CHILD_FLAGS_OFFSET: usize = core::mem::size_of::<PMSignalState>();

/// `NON_EXEC_STATIC volatile PMSignalData *PMSignalState = NULL;` — the base of
/// the shared region, valid in both the postmaster and child processes once
/// `PMSignalShmemInit` ran. Stored as a raw `usize` (shared address); forked
/// children inherit the same pointer value (identical `MAP_SHARED` address).
static PM_SIGNAL_STATE: AtomicUsize = AtomicUsize::new(0);

use std::sync::atomic::AtomicUsize;

/// `volatile sig_atomic_t postmaster_possibly_dead = false;` — set by the
/// postmaster-death signal handler. `AtomicBool` models the async-signal-safe
/// `volatile sig_atomic_t`.
static POSTMASTER_POSSIBLY_DEAD: AtomicBool = AtomicBool::new(false);

/// The `PMSignalState != NULL` dereference. C would crash on use before
/// `PMSignalShmemInit`; here it is a loud panic. (Signal-handler-context
/// callers — `GetQuitSignalReason` — guard against this separately, matching
/// the C `PMSignalState == NULL` paranoia.)
fn state() -> &'static PMSignalState {
    let p = PM_SIGNAL_STATE.load(SeqCst);
    assert!(
        p != 0,
        "PMSignalState shared memory not initialized (PMSignalShmemInit not called)"
    );
    // SAFETY: `p` is the base of the live, process-shared PMSignalState region,
    // which outlives every process; the layout is `#[repr(C)]` and identical on
    // both sides of `fork()`.
    unsafe { &*(p as *const PMSignalState) }
}

/// `MaxLivePostmasterChildren()` (pmchild.c) — the length of `PMChildFlags[]`.
#[inline]
fn max_live_postmaster_children() -> i32 {
    pmchild_seams::max_live_postmaster_children::call()
}

// ---------------------------------------------------------------------------
// PMSignalShmemSize / PMSignalShmemInit
// ---------------------------------------------------------------------------

/// `PMSignalShmemSize` — compute space needed for pmsignal.c's shared memory.
///
/// ```c
/// size = offsetof(PMSignalData, PMChildFlags);
/// size = add_size(size, mul_size(MaxLivePostmasterChildren(),
///                                sizeof(sig_atomic_t)));
/// return size;
/// ```
///
/// `offsetof(PMSignalData, PMChildFlags)` is the C fixed-header size:
/// `PMSignalFlags[NUM_PMSIGNALS]` + `sigquit_reason` + `num_child_flags`, all
/// `sig_atomic_t`/`int`/`enum` (4 bytes each). The C body returns `Size`, but
/// its `add_size`/`mul_size` raise on overflow; we surface that as the
/// `PgResult` error the shmem allocator path expects.
pub fn PMSignalShmemSize() -> PgResult<usize> {
    // offsetof(PMSignalData, PMChildFlags). The region is accessed through real
    // Rust `#[repr(C)]` pointer arithmetic, so the header size must be the actual
    // Rust offset of the flexible array (`PM_CHILD_FLAGS_OFFSET`), not a
    // hand-summed C value, or the access could run past the allocation. (This is
    // >= the C `(NUM_PMSIGNALS + 2) * sizeof(sig_atomic_t)` since AtomicBool
    // flags pack tighter than the C `sig_atomic_t` flags; over-allocation is
    // harmless and the trailing `PMChildFlags[]` stride matches `AtomicI32`.)
    let header = PM_CHILD_FLAGS_OFFSET;
    let size = add_size(
        header,
        mul_size(
            max_live_postmaster_children() as usize,
            core::mem::size_of::<AtomicI32>(),
        )?,
    )?;
    Ok(size)
}

/// `PMSignalShmemInit` — initialize during shared-memory creation.
///
/// ```c
/// PMSignalState = ShmemInitStruct("PMSignalState", PMSignalShmemSize(), &found);
/// if (!found) {
///     MemSet(unvolatize(...), 0, PMSignalShmemSize());
///     num_child_flags = MaxLivePostmasterChildren();
///     PMSignalState->num_child_flags = num_child_flags;
/// }
/// ```
///
/// First caller constructs and zero-initializes the control block (the C
/// `!found` branch) and publishes `num_child_flags`; later callers just attach.
/// The C out-of-shared-memory `ereport(ERROR)` of `ShmemInitStruct` has no
/// analogue for the host allocation backing the `OnceLock`, so this returns
/// `Ok` today.
pub fn PMSignalShmemInit() -> PgResult<()> {
    // PMSignalState = ShmemInitStruct("PMSignalState", PMSignalShmemSize(), &found);
    let size = PMSignalShmemSize()?;
    debug_assert!(PM_CHILD_FLAGS_OFFSET >= core::mem::size_of::<PMSignalState>());
    debug_assert!(PM_CHILD_FLAGS_OFFSET % core::mem::align_of::<AtomicI32>() == 0);
    let (addr, found) =
        ipc_shmem_seams::shmem_init_struct::call("PMSignalState", size)?;

    // Publish the shared base so `state()` resolves it; every forked child
    // inherits the same pointer value (identical MAP_SHARED address).
    PM_SIGNAL_STATE.store(addr as usize, SeqCst);

    if !found {
        // C: MemSet(unvolatize(PMSignalData *, PMSignalState), 0, size);
        // SAFETY: `addr` addresses `size` writable bytes of the fresh region.
        unsafe {
            core::ptr::write_bytes(addr, 0, size);
        }
        let n = max_live_postmaster_children();
        // SAFETY: header is at the region base; `AtomicI32::new(0)` == zeroed.
        // num_child_flags = MaxLivePostmasterChildren().
        let s = state();
        s.num_child_flags.store(n, SeqCst);
        // PM_CHILD_UNUSED is the zero state (set by the MemSet above);
        // sigquit_reason PMQUIT_NOT_SENT and all flags false likewise zeroed.
        debug_assert_eq!(PM_CHILD_UNUSED, 0);
        debug_assert_eq!(QuitSignalReason::PMQUIT_NOT_SENT as u32, 0);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SendPostmasterSignal / CheckPostmasterSignal
// ---------------------------------------------------------------------------

/// `SendPostmasterSignal` — signal the postmaster from a child process.
///
/// ```c
/// if (!IsUnderPostmaster) return;
/// PMSignalState->PMSignalFlags[reason] = true;
/// kill(PostmasterPid, SIGUSR1);
/// ```
pub fn SendPostmasterSignal(reason: PMSignalReason) {
    // If called in a standalone backend, do nothing.
    if !init_small_seams::is_under_postmaster::call() {
        return;
    }
    // Atomically set the proper flag.
    state().pm_signal_flags[reason as usize].store(true, SeqCst);
    // Send signal to postmaster.
    let postmaster_pid = init_small_seams::postmaster_pid::call();
    unsafe {
        libc::kill(postmaster_pid, libc::SIGUSR1);
    }
}

/// `SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE)` — the single
/// reason `bgworker.c` signals, exposed as a narrow seam.
pub fn send_postmaster_signal_bgworker_change() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE);
}

/// `SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER)` — the single reason
/// `varsup.c` signals (XID-wraparound pressure), exposed as a narrow seam.
pub fn send_postmaster_signal_start_autovac() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
}

/// `SendPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN)` — the checkpointer's
/// signal that it finished writing the shutdown checkpoint.
pub fn send_postmaster_signal_xlog_is_shutdown() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN);
}

/// `SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED)` — the startup process's
/// signal that it has begun redo, so the postmaster can launch the
/// archiver/bgwriter/checkpointer for recovery.
pub fn send_postmaster_signal_recovery_started() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_STARTED);
}

/// `SendPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT)` — recovery has reached
/// a consistent state.
pub fn send_postmaster_signal_recovery_consistent() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_CONSISTENT);
}

/// `SendPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY)` — the postmaster may now
/// begin accepting read-only (hot-standby) connections.
pub fn send_postmaster_signal_begin_hot_standby() {
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_BEGIN_HOT_STANDBY);
}

/// `CheckPostmasterSignal` — check whether `reason` was signaled, clearing the
/// flag if so. Called by the postmaster after receiving `SIGUSR1`.
///
/// ```c
/// if (PMSignalState->PMSignalFlags[reason]) {
///     PMSignalState->PMSignalFlags[reason] = false;
///     return true;
/// }
/// return false;
/// ```
pub fn CheckPostmasterSignal(reason: PMSignalReason) -> bool {
    // Careful here --- don't clear flag if we haven't seen it set.
    let flag = &state().pm_signal_flags[reason as usize];
    if flag.load(SeqCst) {
        flag.store(false, SeqCst);
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// SetQuitSignalReason / GetQuitSignalReason
// ---------------------------------------------------------------------------

/// `SetQuitSignalReason` — broadcast the reason for a system shutdown. Called
/// by the postmaster before sending `SIGQUIT` to children.
///
/// ```c
/// PMSignalState->sigquit_reason = reason;
/// ```
pub fn SetQuitSignalReason(reason: QuitSignalReason) {
    state().sigquit_reason.store(reason as u32, SeqCst);
}

/// `GetQuitSignalReason` — obtain the reason for a system shutdown. Called by
/// children when they receive `SIGQUIT`. Returns `PMQUIT_NOT_SENT` if the
/// postmaster has not actually sent `SIGQUIT`.
///
/// ```c
/// if (!IsUnderPostmaster || PMSignalState == NULL)
///     return PMQUIT_NOT_SENT;
/// return PMSignalState->sigquit_reason;
/// ```
///
/// Called in signal handlers, so it is extra paranoid: a standalone backend, or
/// a not-yet-attached `PMSignalState`, reports `PMQUIT_NOT_SENT`.
pub fn GetQuitSignalReason() -> QuitSignalReason {
    if !init_small_seams::is_under_postmaster::call() {
        return QuitSignalReason::PMQUIT_NOT_SENT;
    }
    let p = PM_SIGNAL_STATE.load(SeqCst);
    if p == 0 {
        return QuitSignalReason::PMQUIT_NOT_SENT;
    }
    // SAFETY: `p` is the live shared PMSignalState base (see `state()`).
    let s = unsafe { &*(p as *const PMSignalState) };
    QuitSignalReason::from_u32(s.sigquit_reason.load(SeqCst))
}

// ---------------------------------------------------------------------------
// Per-child slot bookkeeping.
// ---------------------------------------------------------------------------

/// `MarkPostmasterChildSlotAssigned` — mark the given slot as `ASSIGNED` for a
/// new postmaster child. Only the postmaster runs this, so no locking.
///
/// ```c
/// Assert(slot > 0 && slot <= num_child_flags);
/// slot--;
/// if (PMSignalState->PMChildFlags[slot] != PM_CHILD_UNUSED)
///     elog(FATAL, "postmaster child slot is already in use");
/// PMSignalState->PMChildFlags[slot] = PM_CHILD_ASSIGNED;
/// ```
pub fn MarkPostmasterChildSlotAssigned(slot: i32) -> PgResult<()> {
    debug_assert!(slot > 0 && slot <= max_live_postmaster_children());
    let slot = (slot - 1) as usize;

    if state().child_flag(slot).load(SeqCst) != PM_CHILD_UNUSED {
        return elog_internal(FATAL, "postmaster child slot is already in use", 236);
    }

    state().child_flag(slot).store(PM_CHILD_ASSIGNED, SeqCst);
    Ok(())
}

/// `MarkPostmasterChildSlotUnassigned` — release a slot after death of a
/// postmaster child. Run in the postmaster.
///
/// ```c
/// Assert(slot > 0 && slot <= num_child_flags);
/// slot--;
/// result = (PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED);
/// PMSignalState->PMChildFlags[slot] = PM_CHILD_UNUSED;
/// return result;
/// ```
///
/// Returns `true` iff the slot had been `ASSIGNED` (the expected case); `false`
/// implies the child failed to clean itself up. No Assert on the state, because
/// this may be called twice when a child crashes.
pub fn MarkPostmasterChildSlotUnassigned(slot: i32) -> bool {
    debug_assert!(slot > 0 && slot <= max_live_postmaster_children());
    let slot = (slot - 1) as usize;

    let result = state().child_flag(slot).load(SeqCst) == PM_CHILD_ASSIGNED;
    state().child_flag(slot).store(PM_CHILD_UNUSED, SeqCst);
    result
}

/// `IsPostmasterChildWalSender` — is `slot` in use by a WAL sender? Called only
/// by the postmaster.
///
/// ```c
/// Assert(slot > 0 && slot <= num_child_flags);
/// slot--;
/// return PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER;
/// ```
pub fn IsPostmasterChildWalSender(slot: i32) -> bool {
    debug_assert!(slot > 0 && slot <= max_live_postmaster_children());
    let slot = (slot - 1) as usize;

    state().child_flag(slot).load(SeqCst) == PM_CHILD_WALSENDER
}

/// `RegisterPostmasterChildActive` — mark the current child about to begin
/// actively using shared memory. Run in the child; registers a shmem-exit hook
/// to mark itself inactive again on a clean exit.
///
/// ```c
/// int slot = MyPMChildSlot;
/// Assert(slot > 0 && slot <= PMSignalState->num_child_flags);
/// slot--;
/// Assert(PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED);
/// PMSignalState->PMChildFlags[slot] = PM_CHILD_ACTIVE;
/// on_shmem_exit(MarkPostmasterChildInactive, 0);
/// ```
pub fn RegisterPostmasterChildActive() -> PgResult<()> {
    let slot = init_small_seams::my_pm_child_slot::call();
    debug_assert!(slot > 0 && slot <= state().num_child_flags.load(SeqCst));
    let idx = (slot - 1) as usize;
    debug_assert_eq!(state().child_flag(idx).load(SeqCst), PM_CHILD_ASSIGNED);
    state().child_flag(idx).store(PM_CHILD_ACTIVE, SeqCst);

    // Arrange to clean up at exit.
    dsm_core_seams::on_shmem_exit::call(
        |_code, _arg| MarkPostmasterChildInactive(),
        // C: `on_shmem_exit(MarkPostmasterChildInactive, 0)` — the callback
        // takes an unused `Datum arg`. The `on_shmem_exit` seam contract is
        // owned by `backend-storage-ipc-dsm-core`, now on the canonical
        // unified `types_tuple::Datum<'static>` (Datum-unification); the null
        // arg crosses this seam edge as that type.
        types_tuple::Datum::null(),
    )
}

/// `MarkPostmasterChildWalSender` — mark the current child as a WAL sender. Run
/// in the child, after it has marked itself active.
///
/// ```c
/// int slot = MyPMChildSlot;
/// Assert(am_walsender);
/// Assert(slot > 0 && slot <= PMSignalState->num_child_flags);
/// slot--;
/// Assert(PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE);
/// PMSignalState->PMChildFlags[slot] = PM_CHILD_WALSENDER;
/// ```
///
/// The `Assert(am_walsender)` checks a walsender-process-local flag; the caller
/// is, by construction, a WAL sender, so that assert is retained at its callers.
pub fn MarkPostmasterChildWalSender() {
    let slot = init_small_seams::my_pm_child_slot::call();
    debug_assert!(slot > 0 && slot <= state().num_child_flags.load(SeqCst));
    let idx = (slot - 1) as usize;
    debug_assert_eq!(state().child_flag(idx).load(SeqCst), PM_CHILD_ACTIVE);
    state().child_flag(idx).store(PM_CHILD_WALSENDER, SeqCst);
}

/// `MarkPostmasterChildInactive` — mark the current child as done using shared
/// memory. Run in the child (as the `on_shmem_exit` callback).
///
/// ```c
/// static void
/// MarkPostmasterChildInactive(int code, Datum arg)
/// {
///     int slot = MyPMChildSlot;
///     Assert(slot > 0 && slot <= PMSignalState->num_child_flags);
///     slot--;
///     Assert(PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE ||
///            PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER);
///     PMSignalState->PMChildFlags[slot] = PM_CHILD_ASSIGNED;
/// }
/// ```
///
/// `pub` (rather than `static`) so the `on_shmem_exit` registration can
/// re-enter it at exit. The `code`/`arg` callback parameters are unused by the
/// C body and dropped by the adapter closure.
pub fn MarkPostmasterChildInactive() -> PgResult<()> {
    let slot = init_small_seams::my_pm_child_slot::call();
    debug_assert!(slot > 0 && slot <= state().num_child_flags.load(SeqCst));
    let idx = (slot - 1) as usize;
    debug_assert!(
        state().child_flag(idx).load(SeqCst) == PM_CHILD_ACTIVE
            || state().child_flag(idx).load(SeqCst) == PM_CHILD_WALSENDER
    );
    state().child_flag(idx).store(PM_CHILD_ASSIGNED, SeqCst);
    Ok(())
}

// ---------------------------------------------------------------------------
// Parent-death detection.
// ---------------------------------------------------------------------------

/// `POSTMASTER_DEATH_SIGNAL` — `SIGINFO` where available, else `SIGPWR`.
/// SIGUSR1/SIGUSR2 are taken, so a different signal carries parent death.
#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd"))]
const POSTMASTER_DEATH_SIGNAL: i32 = libc::SIGINFO;
#[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd")))]
const POSTMASTER_DEATH_SIGNAL: i32 = libc::SIGPWR;

/// `postmaster_death_handler(SIGNAL_ARGS)` — sets the
/// `postmaster_possibly_dead` flag. Installed for `POSTMASTER_DEATH_SIGNAL`.
fn postmaster_death_handler(_signo: i32) {
    POSTMASTER_POSSIBLY_DEAD.store(true, SeqCst);
}

/// `PostmasterIsAlive()` (`storage/pmsignal.h`) — the fast path. On a
/// parent-death-signal platform, short-circuit while `postmaster_possibly_dead`
/// is clear; otherwise fall through to the slow probe.
///
/// ```c
/// #ifdef USE_POSTMASTER_DEATH_SIGNAL
///     if (likely(!postmaster_possibly_dead)) return true;
/// #endif
///     return PostmasterIsAliveInternal();
/// ```
pub fn PostmasterIsAlive() -> bool {
    if !POSTMASTER_POSSIBLY_DEAD.load(SeqCst) {
        return true;
    }
    PostmasterIsAliveInternal()
}

/// `PostmasterIsAliveInternal` — the slow path of `PostmasterIsAlive()`, where
/// the caller has already checked `postmaster_possibly_dead`.
///
/// ```c
/// postmaster_possibly_dead = false;
/// rc = read(postmaster_alive_fds[POSTMASTER_FD_WATCH], &c, 1);
/// if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
/// else {
///     postmaster_possibly_dead = true;
///     if (rc < 0) elog(FATAL, "read on postmaster death monitoring pipe failed: %m");
///     else if (rc > 0) elog(FATAL, "unexpected data in postmaster death monitoring pipe");
///     return false;
/// }
/// ```
///
/// The death-watch pipe `read()` is the OS boundary (postmaster.c owns
/// `postmaster_alive_fds`); it rides the postmaster seam, returning
/// `(rc, errno)`. A FATAL read failure / unexpected data is a `PgResult` `Err`
/// path; but the C signature is `bool`, so the FATAL `elog`s here panic loudly
/// (a FATAL longjmp does not return to the caller in C either).
pub fn PostmasterIsAliveInternal() -> bool {
    // Reset the flag before checking, so that we don't miss a signal if the
    // postmaster dies right after the check. If it was indeed dead, we'll
    // re-arm it before returning.
    POSTMASTER_POSSIBLY_DEAD.store(false, SeqCst);

    let (rc, errno) = postmaster_seams::read_postmaster_death_watch::call();

    // In the usual case, the postmaster is still alive, and there is no data
    // in the pipe.
    if rc < 0 && (errno == libc::EAGAIN || errno == libc::EWOULDBLOCK) {
        return true;
    }

    // Postmaster is dead, or something went wrong with the read() call.
    POSTMASTER_POSSIBLY_DEAD.store(true, SeqCst);

    if rc < 0 {
        let _: PgResult<()> = elog_internal(
            FATAL,
            "read on postmaster death monitoring pipe failed",
            382,
        );
        // FATAL does not return; mirror the longjmp with a loud abort.
        panic!("read on postmaster death monitoring pipe failed");
    } else if rc > 0 {
        let _: PgResult<()> =
            elog_internal(FATAL, "unexpected data in postmaster death monitoring pipe", 384);
        panic!("unexpected data in postmaster death monitoring pipe");
    }

    false
}

/// `PostmasterDeathSignalInit` — request a signal on postmaster death if the
/// platform supports it.
///
/// ```c
/// int signum = POSTMASTER_DEATH_SIGNAL;
/// pqsignal(signum, postmaster_death_handler);
/// if (prctl(PR_SET_PDEATHSIG, signum) < 0)   // or procctl(...)
///     elog(ERROR, "could not request parent death signal: %m");
/// postmaster_possibly_dead = true;
/// ```
///
/// The handler install (`pqsignal`) and the parent-death request
/// (`prctl`/`procctl`) are the OS boundary, the latter behind the postmaster
/// seam. Seeds `postmaster_possibly_dead = true` so the first
/// `PostmasterIsAlive()` checks the slow way.
pub fn PostmasterDeathSignalInit() -> PgResult<()> {
    let signum = POSTMASTER_DEATH_SIGNAL;

    // Register our signal handler.
    port_pqsignal_seams::pqsignal::call(
        signum,
        signal::SigHandler::Handler(postmaster_death_handler),
    );

    // Request a signal on parent exit.
    postmaster_seams::request_parent_death_signal::call(signum)?;

    // Just in case the parent was gone already and we missed it, we'd better
    // check the slow way on the first call.
    POSTMASTER_POSSIBLY_DEAD.store(true, SeqCst);
    Ok(())
}

/// Seam adapter for `PostmasterDeathSignalInit` (the seam type is
/// `fn() -> PgResult<()>`, matching this directly).
pub fn PostmasterDeathSignalInit_seam() -> PgResult<()> {
    PostmasterDeathSignalInit()
}

// ---------------------------------------------------------------------------
// elog helper — pmsignal.c uses bare elog() with no errcode(), so these
// default to the level's SQLSTATE (ERRCODE_INTERNAL_ERROR) + errmsg_internal.
// `lineno` records the pmsignal.c source line for parity with C `__LINE__`.
// ---------------------------------------------------------------------------

fn elog_internal(level: ::types_error::ErrorLevel, msg: &str, lineno: i32) -> PgResult<()> {
    let _loc = ErrorLocation::new(FILE, lineno, "pmsignal");
    elog(level, msg)
}
