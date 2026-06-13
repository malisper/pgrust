//! Port of `src/backend/storage/ipc/latch.c` (PostgreSQL 18.3):
//! inter-process latches.
//!
//! The latch interface is a reliable replacement for the common pattern of
//! using `pg_usleep()` or `select()` to wait until a signal arrives, where
//! the signal handler sets a flag variable. See `storage/latch.h` for how to
//! use them.
//!
//! In PostgreSQL 18 `latch.c` is a thin layer over `waiteventset.c`: it keeps
//! one long-lived `WaitEventSet` (`LatchWaitSet`) for [`WaitLatch`], builds a
//! throwaway one for [`WaitLatchOrSocket`], and delegates every blocking wait
//! and cross-process wakeup to the wait-event-set unit, reached here through
//! `backend-storage-ipc-waiteventset-seams` (the owner is unported).
//!
//! # Model notes (audit against these)
//!
//! - C callers allocate `Latch` storage themselves (`LocalLatchData` in
//!   globals.c, `PGPROC.procLatch` in shared memory) and pass `Latch *`.
//!   Here a backend is a thread, and a shared latch is set across threads
//!   (and from signal handlers), so the [`Latch`] storage is owned by this
//!   crate in a process-global, append-only registry of synchronized values
//!   (atomic fields); consumers name a latch with
//!   [`types_storage::latch::LatchHandle`]. C never frees a latch (both
//!   backing allocations are process-lifetime), so the registry has no
//!   removal. [`allocate_latch`] is the analogue of the C caller's variable
//!   declaration — registry growth is std allocation, not a palloc path.
//! - `pg_memory_barrier()` is `fence(SeqCst)` (the c2rust object used the
//!   same mapping); the field accesses themselves are `SeqCst` atomics, at
//!   least as strong as the C's plain accesses bracketed by barriers.
//! - `Latch *MyLatch` is a globals.c variable, but until miscinit/globals
//!   land the per-backend slot lives here as a thread-local
//!   ([`set_my_latch`]/[`my_latch`]), the same convention procsignal used
//!   for `ProcSignalBarrierPending`. The `set_latch_my_latch` seam (the
//!   signal-handler shape, which cannot carry a parameter) resolves it at
//!   call time.
//! - `static WaitEventSet *LatchWaitSet` is backend-local: a thread-local
//!   holding the owning `WaitEventSet` guard. The `RefCell` borrow is held
//!   across the blocking wait; nothing re-enters `WaitLatch` from inside a
//!   wait (latch wakeups go through [`SetLatch`], which does not touch it).
//! - `MyProcPid` / `IsUnderPostmaster` are read through the existing
//!   `backend-utils-init-small-seams` getters, like the sibling ipc ports.
//! - WIN32-only code (`CreateEvent`/`SetEvent` and the `latch->event`
//!   handle) is not part of this build, matching the other ports.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{fence, AtomicBool, AtomicI32};
use std::sync::{Arc, RwLock};

use backend_storage_ipc_waiteventset_seams::{self as wes_seams, WaitEventSet};
use types_core::{pgsocket, PGINVALID_SOCKET};
use types_error::{PgError, PgResult, PANIC};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{
    WaitEvent, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_MASK, WL_TIMEOUT,
};

/// The positions of the latch and PM death events in `LatchWaitSet`.
const LatchWaitSetLatchPos: i32 = 0;
const LatchWaitSetPostmasterDeathPos: i32 = 1;

/// `struct Latch` (`storage/latch.h`), with the fields a concurrent setter
/// touches as atomics: `is_set` is `sig_atomic_t` written cross-process,
/// `maybe_sleeping` is written by the waiter and read by setters, and
/// `owner_pid` is fetched once in [`SetLatch`] exactly because it can change
/// concurrently (the C comments rely on `pid_t` being effectively atomic).
/// `is_shared` is only written at init, but `InitLatch` may re-initialize a
/// shared-registry latch (C's `SwitchToLocalLatch` path), so it is atomic
/// too. Deliberately not `Copy`/`Clone`: a latch is a synchronization object
/// with one identity, named by [`LatchHandle`].
#[derive(Debug)]
pub struct Latch {
    is_set: AtomicBool,
    maybe_sleeping: AtomicBool,
    is_shared: AtomicBool,
    owner_pid: AtomicI32,
}

impl Latch {
    /// Read `latch->is_set` (for the wait-event-set owner's poll loop).
    pub fn is_set(&self) -> bool {
        self.is_set.load(SeqCst)
    }

    /// Write `latch->maybe_sleeping` (waiteventset.c sets it around the
    /// blocking wait).
    pub fn set_maybe_sleeping(&self, value: bool) {
        self.maybe_sleeping.store(value, SeqCst);
    }

    /// Read `latch->owner_pid` (waiteventset.c asserts ownership when a
    /// latch is registered).
    pub fn owner_pid(&self) -> i32 {
        self.owner_pid.load(SeqCst)
    }
}

/// Process-global latch registry: handle id `n` names `LATCHES[n - 1]`
/// (`0` is never a valid handle). Latches are C shared-memory /
/// backend-private process-lifetime state — explicitly shared, synchronized,
/// append-only. The write lock is held only while [`allocate_latch`] grows
/// the vector; [`SetLatch`] (signal-handler-callable) takes only the read
/// lock.
static LATCHES: RwLock<Vec<Arc<Latch>>> = RwLock::new(Vec::new());

/// Mint a new, uninitialized latch and return its handle — the analogue of
/// the C caller declaring `Latch` storage (globals.c's `LocalLatchData`,
/// proc.c's `PGPROC.procLatch`) before calling [`InitLatch`] /
/// [`InitSharedLatch`].
pub fn allocate_latch() -> LatchHandle {
    let mut latches = LATCHES.write().unwrap();
    latches.push(Arc::new(Latch {
        is_set: AtomicBool::new(false),
        maybe_sleeping: AtomicBool::new(false),
        is_shared: AtomicBool::new(false),
        owner_pid: AtomicI32::new(0),
    }));
    LatchHandle::new(latches.len())
}

/// Resolve a handle to its latch. An invalid handle is the C wild-pointer
/// case: panic.
pub fn lookup_latch(latch: LatchHandle) -> Arc<Latch> {
    LATCHES
        .read()
        .unwrap()
        .get(latch.as_usize().wrapping_sub(1))
        .cloned()
        .expect("invalid LatchHandle")
}

thread_local! {
    /// `Latch *MyLatch` (globals.c). Owned here until miscinit/globals land;
    /// `None` is a NULL `MyLatch`.
    static MY_LATCH: Cell<Option<LatchHandle>> = const { Cell::new(None) };

    /// `static WaitEventSet *LatchWaitSet` — the common wait event set used
    /// to implement [`WaitLatch`]. `None` is a NULL pointer (not yet
    /// initialized).
    static LATCH_WAIT_SET: RefCell<Option<WaitEventSet>> = const { RefCell::new(None) };
}

/// Point this backend's `MyLatch` at `latch` (`MyLatch = ...` in
/// miscinit.c's `InitProcessLocalLatch` / `SwitchToSharedLatch` /
/// `SwitchToLocalLatch`).
pub fn set_my_latch(latch: Option<LatchHandle>) {
    MY_LATCH.with(|cell| cell.set(latch));
}

/// Read this backend's `MyLatch`.
pub fn my_latch() -> Option<LatchHandle> {
    MY_LATCH.with(Cell::get)
}

fn my_proc_pid() -> i32 {
    backend_utils_init_small_seams::my_proc_pid::call()
}

fn is_under_postmaster() -> bool {
    backend_utils_init_small_seams::is_under_postmaster::call()
}

/// `InitializeLatchWaitSet(void)` — set up the `WaitEventSet` used by
/// [`WaitLatch`]: `MyLatch` at position [`LatchWaitSetLatchPos`] and, under
/// the postmaster, a postmaster-death event at
/// [`LatchWaitSetPostmasterDeathPos`] that [`WaitLatch`] re-points to
/// `WL_EXIT_ON_PM_DEATH` / `WL_POSTMASTER_DEATH` on each call.
///
/// C passes a NULL resowner to `CreateWaitEventSet`; the guard lives in the
/// thread-local for the backend's lifetime.
pub fn InitializeLatchWaitSet() -> PgResult<()> {
    LATCH_WAIT_SET.with(|cell| {
        let mut slot = cell.borrow_mut();
        debug_assert!(slot.is_none());

        let set = WaitEventSet::create(2)?;
        let latch_pos = set.add_event(WL_LATCH_SET, PGINVALID_SOCKET, my_latch(), None)?;
        debug_assert_eq!(latch_pos, LatchWaitSetLatchPos);

        if is_under_postmaster() {
            let latch_pos = set.add_event(WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;
            debug_assert_eq!(latch_pos, LatchWaitSetPostmasterDeathPos);
        }

        *slot = Some(set);
        Ok(())
    })
}

/// `InitLatch(Latch *latch)` — initialize a process-local latch, owned by
/// the current process.
pub fn InitLatch(latch: LatchHandle) {
    let latch = lookup_latch(latch);
    latch.is_set.store(false, SeqCst);
    latch.maybe_sleeping.store(false, SeqCst);
    latch.owner_pid.store(my_proc_pid(), SeqCst);
    latch.is_shared.store(false, SeqCst);
}

/// `InitSharedLatch(Latch *latch)` — initialize a shared latch that can be
/// set from other processes. Initially owned by no-one; use [`OwnLatch`] to
/// associate it with the current process.
///
/// In C this must run in the postmaster before forking children (a Windows
/// handle-inheritance restriction), so there are no concurrency issues here.
pub fn InitSharedLatch(latch: LatchHandle) {
    let latch = lookup_latch(latch);
    latch.is_set.store(false, SeqCst);
    latch.maybe_sleeping.store(false, SeqCst);
    latch.owner_pid.store(0, SeqCst);
    latch.is_shared.store(true, SeqCst);
}

/// `OwnLatch(Latch *latch)` — associate a shared latch with the current
/// process, allowing it to wait on the latch.
///
/// There is no locking around the already-owned check; if two processes
/// could race to own the same latch, the caller must provide an interlock.
pub fn OwnLatch(latch: LatchHandle) -> PgResult<()> {
    let latch = lookup_latch(latch);

    debug_assert!(latch.is_shared.load(SeqCst));

    let owner_pid = latch.owner_pid.load(SeqCst);
    if owner_pid != 0 {
        // elog(PANIC, "latch already owned by PID %d", owner_pid)
        return Err(PgError::new(
            PANIC,
            format!("latch already owned by PID {owner_pid}"),
        ));
    }

    latch.owner_pid.store(my_proc_pid(), SeqCst);
    Ok(())
}

/// `DisownLatch(Latch *latch)` — disown a shared latch currently owned by
/// the current process.
pub fn DisownLatch(latch: LatchHandle) {
    let latch = lookup_latch(latch);
    debug_assert!(latch.is_shared.load(SeqCst));
    debug_assert_eq!(latch.owner_pid.load(SeqCst), my_proc_pid());

    latch.owner_pid.store(0, SeqCst);
}

/// `WaitLatch(Latch *latch, int wakeEvents, long timeout, uint32
/// wait_event_info)` — wait for the latch to be set, for postmaster death,
/// or until `timeout` (milliseconds; used only with `WL_TIMEOUT`) is
/// exceeded. Returns immediately if the latch is already set (and
/// `WL_LATCH_SET` is given).
///
/// The latch must be owned by the current process. Returns the bitmask of
/// the condition(s) that caused the wake-up; if several are true at least
/// one is reported.
pub fn WaitLatch(
    latch: Option<LatchHandle>,
    wakeEvents: u32,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    // Postmaster-managed callers must handle postmaster death somehow.
    debug_assert!(
        !is_under_postmaster() || wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH) != 0
    );

    // Some callers may have a latch other than MyLatch, or no latch at all,
    // or want to handle postmaster death differently. It's cheap to assign
    // those, so just do it every time.
    let latch = if wakeEvents & WL_LATCH_SET != 0 {
        latch
    } else {
        None
    };

    LATCH_WAIT_SET.with(|cell| {
        let slot = cell.borrow();
        // A NULL LatchWaitSet deref would crash the C; surface it loudly.
        let set = slot.as_ref().expect("LatchWaitSet is not initialized");

        set.modify_event(LatchWaitSetLatchPos, WL_LATCH_SET, latch)?;

        if is_under_postmaster() {
            set.modify_event(
                LatchWaitSetPostmasterDeathPos,
                wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH),
                None,
            )?;
        }

        let mut event = [WaitEvent::default()];
        let timeout = if wakeEvents & WL_TIMEOUT != 0 {
            timeout
        } else {
            -1
        };
        if set.wait(timeout, &mut event, wait_event_info)? == 0 {
            Ok(WL_TIMEOUT)
        } else {
            Ok(event[0].events)
        }
    })
}

/// `WaitLatchOrSocket(Latch *latch, int wakeEvents, pgsocket sock, long
/// timeout, uint32 wait_event_info)` — like [`WaitLatch`], but with an extra
/// socket argument for the `WL_SOCKET_*` conditions, using a throwaway
/// 3-event set.
///
/// When waiting on a socket, EOF and error conditions always cause it to be
/// reported as readable/writable/connected so the caller can deal with the
/// condition. `wakeEvents` must include `WL_EXIT_ON_PM_DEATH` or
/// `WL_POSTMASTER_DEATH`.
///
/// C creates the set under `CurrentResourceOwner` so an `ereport(ERROR)`
/// between create and the unconditional `FreeWaitEventSet` still releases
/// it; here the [`WaitEventSet`] guard's `Drop` is that release on every
/// path, including `?` propagation (docs/query-lifecycle-raii.md).
pub fn WaitLatchOrSocket(
    latch: Option<LatchHandle>,
    wakeEvents: u32,
    sock: pgsocket,
    mut timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    let mut ret: u32 = 0;
    let set = WaitEventSet::create(3)?;

    if wakeEvents & WL_TIMEOUT != 0 {
        debug_assert!(timeout >= 0);
    } else {
        timeout = -1;
    }

    if wakeEvents & WL_LATCH_SET != 0 {
        set.add_event(WL_LATCH_SET, PGINVALID_SOCKET, latch, None)?;
    }

    // Postmaster-managed callers must handle postmaster death somehow.
    debug_assert!(
        !is_under_postmaster() || wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH) != 0
    );

    if wakeEvents & WL_POSTMASTER_DEATH != 0 && is_under_postmaster() {
        set.add_event(WL_POSTMASTER_DEATH, PGINVALID_SOCKET, None, None)?;
    }

    if wakeEvents & WL_EXIT_ON_PM_DEATH != 0 && is_under_postmaster() {
        set.add_event(WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;
    }

    if wakeEvents & WL_SOCKET_MASK != 0 {
        let ev = wakeEvents & WL_SOCKET_MASK;
        set.add_event(ev, sock, None, None)?;
    }

    let mut event = [WaitEvent::default()];
    let rc = set.wait(timeout, &mut event, wait_event_info)?;

    if rc == 0 {
        ret |= WL_TIMEOUT;
    } else {
        ret |= event[0].events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    }

    Ok(ret) // drop(set) == FreeWaitEventSet(set)
}

/// `SetLatch(Latch *latch)` — set a latch and wake up anyone waiting on it.
///
/// Cheap if the latch is already set, otherwise not so much. Called from
/// critical sections and signal handlers, so it never errors.
pub fn SetLatch(latch: LatchHandle) {
    set_latch(&lookup_latch(latch));
}

fn set_latch(latch: &Latch) {
    // The memory barrier has to be placed here to ensure that any flag
    // variables possibly changed by this process have been flushed to main
    // memory, before we check/set is_set.
    fence(SeqCst);

    // Quick exit if already set
    if latch.is_set.load(SeqCst) {
        return;
    }

    latch.is_set.store(true, SeqCst);

    fence(SeqCst);
    if !latch.maybe_sleeping.load(SeqCst) {
        return;
    }

    // See if anyone's waiting for the latch. It can be the current process
    // if we're in a signal handler; waiteventset wakes it via the self-pipe
    // or SIGURG-to-self to avoid races. If it's another process, send a
    // signal.
    //
    // Fetch owner_pid only once, in case the latch is concurrently getting
    // owned or disowned: a new owner right after we look just doesn't get
    // signaled, which is fine as long as waiters check the latch at the
    // bottom of their loops, not the top.
    let owner_pid = latch.owner_pid.load(SeqCst);
    if owner_pid == 0 {
        // no-one to wake
    } else if owner_pid == my_proc_pid() {
        wes_seams::wakeup_my_proc::call();
    } else {
        wes_seams::wakeup_other_proc::call(owner_pid);
    }
}

/// `ResetLatch(Latch *latch)` — clear the latch. Calling [`WaitLatch`] after
/// this will sleep, unless the latch is set again before the [`WaitLatch`]
/// call.
pub fn ResetLatch(latch: LatchHandle) {
    let latch = lookup_latch(latch);

    // Only the owner should reset the latch.
    debug_assert_eq!(latch.owner_pid.load(SeqCst), my_proc_pid());
    debug_assert!(!latch.maybe_sleeping.load(SeqCst));

    latch.is_set.store(false, SeqCst);

    // Ensure that the write to is_set gets flushed to main memory before we
    // examine any flag variables. Otherwise a concurrent SetLatch might
    // falsely conclude that it needn't signal us, even though we have
    // missed seeing some flag updates that SetLatch was supposed to inform
    // us of.
    fence(SeqCst);
}

/// `SetLatch(MyLatch)` for the seam's parameterless signal-handler shape. A
/// NULL `MyLatch` deref would crash the C; surface it loudly.
fn set_latch_my_latch() {
    let latch = my_latch().expect("SetLatch(MyLatch): MyLatch is not set");
    SetLatch(latch);
}

/// `ResetLatch(MyLatch)` for the seam's parameterless shape. A NULL `MyLatch`
/// would crash the C; surface it loudly.
fn reset_latch_my_latch() {
    let latch = my_latch().expect("ResetLatch(MyLatch): MyLatch is not set");
    ResetLatch(latch);
}

/// `WaitLatch(MyLatch, ...)` for the seam's MyLatch shape.
fn wait_latch_my_latch(
    wake_events: u32,
    timeout: i64,
    wait_event_info: u32,
) -> types_error::PgResult<u32> {
    WaitLatch(my_latch(), wake_events, timeout, wait_event_info)
}

/// `WaitLatch(latch, ...)` for the seam's explicit-handle shape. The seam
/// takes a non-optional `LatchHandle` (C call sites that pass a concrete
/// latch pointer) and returns `PgResult<i32>` matching the C return type.
fn wait_latch_seam(
    latch: LatchHandle,
    wake_events: u32,
    timeout: i64,
    wait_event_info: u32,
) -> types_error::PgResult<i32> {
    WaitLatch(Some(latch), wake_events, timeout, wait_event_info).map(|v| v as i32)
}

/// Install this unit's seams (`backend-storage-ipc-latch-seams`).
pub fn init_seams() {
    backend_storage_ipc_latch_seams::set_latch_my_latch::set(set_latch_my_latch);
    backend_storage_ipc_latch_seams::reset_latch::set(ResetLatch);
    backend_storage_ipc_latch_seams::set_latch::set(SetLatch);
    backend_storage_ipc_latch_seams::reset_latch_my_latch::set(reset_latch_my_latch);
    backend_storage_ipc_latch_seams::wait_latch_my_latch::set(wait_latch_my_latch);
    backend_storage_ipc_latch_seams::wait_latch::set(wait_latch_seam);
}

#[cfg(test)]
mod tests;
