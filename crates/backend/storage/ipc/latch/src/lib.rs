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
//!   [`::types_storage::latch::LatchHandle`]. C never frees a latch (both
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
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock};

use waiteventset_seams::{self as wes_seams, WaitEventSet};
use types_core::{pgsocket, ProcNumber, PGINVALID_SOCKET};
use types_error::{PgError, PgResult, PANIC};
use ::types_storage::latch::{Latch, LatchHandle, LatchKind};
use ::types_storage::waiteventset::{
    WaitEvent, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_MASK, WL_TIMEOUT,
};

/// The positions of the latch and PM death events in `LatchWaitSet`.
const LatchWaitSetLatchPos: i32 = 0;
const LatchWaitSetPostmasterDeathPos: i32 = 1;

/// The single `Latch` representation lives in `::types_storage::latch::Latch`
/// (the C `struct Latch`, all fields atomic): both the latch unit's own
/// registry latches and the `Latch` embedded in each `PGPROC` (`procLatch`,
/// owned by the proc unit) use it, so a procno-derived handle resolves to the
/// real `&proc->procLatch` — see [`with_latch`].

/// Process-global registry of the latch unit's *own* `Latch` storage
/// (`LocalLatchData`-style backend-private / process-global latches): a
/// [`LatchKind::Local`] handle id `n` names `LATCHES[n - 1]` (`0` is never a
/// valid handle). Latches are process-lifetime — explicitly shared,
/// synchronized, append-only. The write lock is held only while
/// [`allocate_latch`] grows the vector; [`SetLatch`] (signal-handler-callable)
/// takes only the read lock. Per-PGPROC `procLatch` latches do *not* live
/// here; they are reached through the proc unit's `with_proc_latch` seam.
static LATCHES: RwLock<Vec<Arc<Latch>>> = RwLock::new(Vec::new());

/// Mint a new, uninitialized *local* latch and return its handle — the
/// analogue of the C caller declaring backend-private / process-global `Latch`
/// storage (globals.c's `LocalLatchData`) before calling [`InitLatch`] /
/// [`InitSharedLatch`]. (A `PGPROC`'s `procLatch` is *not* allocated here; it
/// is embedded in the proc array and named by [`LatchHandle::proc`].)
pub fn allocate_latch() -> LatchHandle {
    let mut latches = LATCHES.write().unwrap();
    latches.push(Arc::new(Latch::new(false, 0)));
    LatchHandle::new(latches.len())
}

/// Resolve a *local* handle to its registry latch. Only valid for
/// [`LatchKind::Local`] handles; a per-PGPROC `procLatch` is not an owned
/// `Arc` (it is embedded in the proc array), so use [`with_latch`] for the
/// general dispatch. An invalid local handle is the C wild-pointer case:
/// panic.
pub fn lookup_latch(latch: LatchHandle) -> Arc<Latch> {
    match latch.kind() {
        LatchKind::Local(id) => LATCHES
            .read()
            .unwrap()
            .get(id.wrapping_sub(1))
            .cloned()
            .expect("invalid LatchHandle"),
        LatchKind::Proc(_) => {
            panic!("lookup_latch: per-PGPROC procLatch is not an owned Arc; use with_latch")
        }
    }
}

/// Run `f` over the `Latch` a handle names, dispatching on the handle's tag:
/// a [`LatchKind::Local`] handle resolves in the latch unit's own registry; a
/// [`LatchKind::Proc`] handle resolves to `&ProcGlobal->allProcs[procno]
/// .procLatch` through the proc unit's `with_proc_latch` seam — the faithful
/// `&proc->procLatch`. This is how the latch unit applies `SetLatch` /
/// `OwnLatch` / `DisownLatch` / `ResetLatch` to either backing allocation
/// without a separate side-table for proc latches.
fn with_latch<R>(latch: LatchHandle, mut f: impl FnMut(&Latch) -> R) -> R {
    match latch.kind() {
        LatchKind::Local(id) => {
            let arc = LATCHES
                .read()
                .unwrap()
                .get(id.wrapping_sub(1))
                .cloned()
                .expect("invalid LatchHandle");
            f(&arc)
        }
        LatchKind::Proc(procno) => with_proc_latch(procno, f),
    }
}

/// Run `f` over `&ProcGlobal->allProcs[procno].procLatch` via the proc unit's
/// `with_proc_latch` seam, capturing `f`'s result out of the void-returning
/// callback shape.
fn with_proc_latch<R>(procno: ProcNumber, mut f: impl FnMut(&Latch) -> R) -> R {
    let mut result: Option<R> = None;
    lmgr_proc_seams::with_proc_latch::call(procno, &mut |latch: &Latch| {
        result = Some(f(latch));
    });
    result.expect("with_proc_latch: proc seam did not invoke the callback")
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
    init_small_seams::my_proc_pid::call()
}

fn is_under_postmaster() -> bool {
    init_small_seams::is_under_postmaster::call()
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
    with_latch(latch, |latch| {
        latch.is_set.store(0, SeqCst);
        latch.maybe_sleeping.store(0, SeqCst);
        latch.owner_pid.store(my_proc_pid(), SeqCst);
        latch.is_shared.store(false, SeqCst);
    });
}

/// `InitSharedLatch(Latch *latch)` — initialize a shared latch that can be
/// set from other processes. Initially owned by no-one; use [`OwnLatch`] to
/// associate it with the current process.
///
/// In C this must run in the postmaster before forking children (a Windows
/// handle-inheritance restriction), so there are no concurrency issues here.
pub fn InitSharedLatch(latch: LatchHandle) {
    with_latch(latch, |latch| {
        latch.is_set.store(0, SeqCst);
        latch.maybe_sleeping.store(0, SeqCst);
        latch.owner_pid.store(0, SeqCst);
        latch.is_shared.store(true, SeqCst);
    });
}

/// `OwnLatch(Latch *latch)` — associate a shared latch with the current
/// process, allowing it to wait on the latch.
///
/// There is no locking around the already-owned check; if two processes
/// could race to own the same latch, the caller must provide an interlock.
pub fn OwnLatch(latch: LatchHandle) -> PgResult<()> {
    with_latch(latch, |latch| {
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
    })
}

/// `DisownLatch(Latch *latch)` — disown a shared latch currently owned by
/// the current process.
pub fn DisownLatch(latch: LatchHandle) {
    with_latch(latch, |latch| {
        debug_assert!(latch.is_shared.load(SeqCst));
        debug_assert_eq!(latch.owner_pid.load(SeqCst), my_proc_pid());

        latch.owner_pid.store(0, SeqCst);
    });
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
    with_latch(latch, set_latch);
}

/// `SetLatch(Latch *latch)` for a latch reached by reference rather than by
/// [`LatchHandle`]. Some latches live embedded inside another subsystem's
/// shared-memory struct (e.g. `XLogRecoveryCtl->recoveryWakeupLatch`,
/// `PGPROC->procLatch`) instead of in this unit's handle registry; those
/// owners hold the `&Latch` directly and call this. Behaviour is identical to
/// [`SetLatch`].
pub fn SetLatchPtr(latch: &Latch) {
    set_latch(latch);
}

fn set_latch(latch: &Latch) {
    // The memory barrier has to be placed here to ensure that any flag
    // variables possibly changed by this process have been flushed to main
    // memory, before we check/set is_set.
    fence(SeqCst);

    // Quick exit if already set
    if latch.is_set.load(SeqCst) != 0 {
        return;
    }

    latch.is_set.store(1, SeqCst);

    fence(SeqCst);
    if latch.maybe_sleeping.load(SeqCst) == 0 {
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
    with_latch(latch, |latch| {
        // Only the owner should reset the latch.
        debug_assert_eq!(latch.owner_pid.load(SeqCst), my_proc_pid());
        debug_assert!(latch.maybe_sleeping.load(SeqCst) == 0);

        latch.is_set.store(0, SeqCst);

        // Ensure that the write to is_set gets flushed to main memory before
        // we examine any flag variables. Otherwise a concurrent SetLatch
        // might falsely conclude that it needn't signal us, even though we
        // have missed seeing some flag updates that SetLatch was supposed to
        // inform us of.
        fence(SeqCst);
    });
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
) -> ::types_error::PgResult<u32> {
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
) -> ::types_error::PgResult<i32> {
    WaitLatch(Some(latch), wake_events, timeout, wait_event_info).map(|v| v as i32)
}

/// `WaitLatch(NULL, WL_EXIT_ON_PM_DEATH | WL_TIMEOUT, 10,
/// WAIT_EVENT_REGISTER_SYNC_REQUEST)` (`storage/ipc/latch.c`, called from
/// `RegisterSyncRequest` in sync.c): the no-latch ~10 ms sleep performed
/// before retrying a full checkpointer request queue. C passes `NULL` for the
/// latch (so it waits only on the timeout / postmaster death); the flags,
/// timeout, and wait-event are fixed. Discards the returned event bitmask to
/// match the seam's `PgResult<()>` shape.
fn wait_latch_register_sync_request_seam() -> PgResult<()> {
    WaitLatch(
        None,
        WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
        10,
        types_pgstat::wait_event::WAIT_EVENT_REGISTER_SYNC_REQUEST,
    )
    .map(|_| ())
}

/// `WaitLatch(NULL, wake_events, timeout, wait_event_info)` for the seam's
/// no-latch shape (the summarizer's post-error back-off — it waits only on the
/// timeout and `WL_EXIT_ON_PM_DEATH`, never on a latch). C passes `NULL` for
/// the latch, so there is no `LatchHandle`; returns the bitmask of occurred
/// events.
fn wait_latch_no_latch(
    wake_events: u32,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    WaitLatch(None, wake_events, timeout, wait_event_info)
}

/// `WaitLatchOrSocket(MyLatch, wakeEvents, sock, timeout, wait_event_info)`
/// for the seam's MyLatch-implicit, bare-`i32` shape. C call sites (e.g.
/// walreceiver's main loop) pass `MyLatch`; the latch crate resolves it. The
/// seam's contract drops the `PgResult` channel that the owner's
/// [`WaitLatchOrSocket`] carries: the only error path is a kernel
/// event-queue failure inside `WaitEventSetWait`, which in C is a
/// `FATAL`/longjmp the caller cannot recover from, so we surface it as a panic
/// (the codebase's `ereport(ERROR)`-at-a-non-`PgResult`-boundary translation).
/// The seam's `wake_events: i32` widens to the owner's `u32` wake-event mask;
/// the returned `u32` bitmask narrows to the seam's `i32`.
fn wait_latch_or_socket_seam(
    wake_events: i32,
    sock: ::types_core::pgsocket,
    timeout: i64,
    wait_event_info: ::types_core::uint32,
) -> i32 {
    WaitLatchOrSocket(
        my_latch(),
        wake_events as u32,
        sock,
        timeout,
        wait_event_info,
    )
    .expect("WaitLatchOrSocket: WaitEventSetWait failed") as i32
}

/// `kill(pid, SIGUSR1)` (`ShutDownSlotSync`): signal the slot-sync worker so
/// it notices the stop request. The seam surfaces a failed `kill(2)` as `Err`
/// (the C ignores the return; the porter chose to make the failure visible).
fn kill_sigusr1(pid: i32) -> PgResult<()> {
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        return Err(PgError::error(format!(
            "could not send signal to process {pid}: {e}"
        )));
    }
    Ok(())
}

/// `SetLatch(&GetPGProcByNumber(procno)->procLatch)`
/// (walreceiver.c / walreceiverfuncs.c) — wake the backend owning slot
/// `procno` via its embedded process latch. The procno names the proc-tagged
/// handle; [`with_latch`] resolves it to the real `&proc->procLatch`.
/// Async-signal-safe and infallible in C.
fn set_latch_for_procno(procno: ProcNumber) {
    SetLatch(LatchHandle::proc(procno));
}

/// `SetLatch(&ProcGlobal->allProcs[pgprocno].procLatch)` (walsummarizer.c) —
/// the same wake, named by the proc number directly. Infallible in C.
fn set_latch_by_proc_number(pgprocno: ProcNumber) {
    SetLatch(LatchHandle::proc(pgprocno));
}

/// `SetLatch(&worker->proc->procLatch)` for the backend whose PID is `pid`
/// (launcher.c `logicalrep_worker_wakeup_ptr`). The launcher names the target
/// by PID; map PID -> `ProcNumber` via `BackendPidGetProc` (procarray.c),
/// then set that PGPROC's embedded latch. If the backend has already exited
/// (`BackendPidGetProc` returns NULL — `pid` no longer live), there is no
/// latch to set, exactly as the C dereference of a then-detached `worker->proc`
/// is guarded by the caller holding LogicalRepWorkerLock. Infallible in C.
fn set_latch_for_proc_pid(pid: i32) {
    if let Some((_role, procno)) =
        procarray_seams::backend_pid_get_proc_role::call(pid)
    {
        SetLatch(LatchHandle::proc(procno));
    }
}

/// Install this unit's seams (`backend-storage-ipc-latch-seams`).
pub fn init_seams() {
    latch_seams::set_latch_my_latch::set(set_latch_my_latch);
    latch_seams::reset_latch::set(ResetLatch);
    latch_seams::set_latch::set(SetLatch);
    latch_seams::reset_latch_my_latch::set(reset_latch_my_latch);
    latch_seams::wait_latch_my_latch::set(wait_latch_my_latch);
    latch_seams::wait_latch::set(wait_latch_seam);
    // `MyLatch` (globals.c): the seam returns a non-optional `LatchHandle`
    // (the few C callers that need the handle deref a non-NULL `MyLatch`); a
    // NULL `MyLatch` would crash the C, so surface it loudly, matching the
    // `set_latch_my_latch`/`reset_latch_my_latch` convention above.
    latch_seams::my_latch::set(|| my_latch().expect("MyLatch is not set"));
    latch_seams::wait_latch_register_sync_request::set(
        wait_latch_register_sync_request_seam,
    );
    latch_seams::wait_latch_no_latch::set(wait_latch_no_latch);
    latch_seams::wait_latch_or_socket::set(wait_latch_or_socket_seam);
    latch_seams::kill_sigusr1::set(kill_sigusr1);
    latch_seams::own_latch::set(OwnLatch);
    latch_seams::disown_latch::set(DisownLatch);
    // The SetLatch-by-proc seams: the proc-tagged handle space (unified with
    // the local registry through `with_latch`/`with_proc_latch`) lets these
    // resolve another backend's `&proc->procLatch` faithfully.
    latch_seams::set_latch_for_procno::set(set_latch_for_procno);
    latch_seams::set_latch_by_proc_number::set(set_latch_by_proc_number);
    latch_seams::set_latch_for_proc_pid::set(set_latch_for_proc_pid);
    // Latch field accessors consumed by waiteventset.c's wait loop.
    latch_seams::latch_is_set::set(latch_is_set);
    latch_seams::latch_maybe_sleeping::set(latch_maybe_sleeping);
    latch_seams::set_latch_maybe_sleeping::set(set_latch_maybe_sleeping);
    latch_seams::latch_owner_pid::set(latch_owner_pid);

    install_parallel_rt_latch_seams();

    // --- lazy-vacuum cost-delay sleep (vacuumlazy.c via vacuum_delay_point;
    //     WaitLatch/ResetLatch on MyLatch). Home in vacuumlazy-seams,
    //     latch.c is their owner. ---
    {
        use vacuumlazy_seams as vx;
        vx::wait_latch::set(|wake_events, timeout_ms, wait_event_info| {
            wait_latch_my_latch(wake_events as u32, timeout_ms, wait_event_info).map(|_| ())
        });
        vx::reset_latch::set(|| {
            reset_latch_my_latch();
            Ok(())
        });
    }
}

/// Install the `MyLatch` operations `access/transam/parallel.c` reaches outward
/// for, declared in `backend-access-transam-parallel-rt-seams` (latch.c owns the
/// bodies). These are thin shims over the same `MyLatch`-shaped helpers installed
/// into this crate's native seam slots above:
///
/// * `set_my_latch` → `SetLatch(MyLatch)`     (parallel.c `HandleParallelMessageInterrupt`)
/// * `reset_latch`  → `ResetLatch(MyLatch)`   (parallel.c worker-startup / finish loops)
/// * `wait_latch`   → `WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, wait_event)`
///   (parallel.c:773-775 / 893-894 — both call sites pass exactly these flags and
///   an infinite (`-1`) timeout).
fn install_parallel_rt_latch_seams() {
    use parallel_rt_seams as rt;

    rt::set_my_latch::set(|| {
        set_latch_my_latch();
        Ok(())
    });
    rt::reset_latch::set(|| {
        reset_latch_my_latch();
        Ok(())
    });
    rt::wait_latch::set(|wait_event| {
        wait_latch_my_latch(WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, wait_event).map(|v| v as i32)
    });
}

/// `latch->is_set` — read the latch's set flag.
fn latch_is_set(latch: LatchHandle) -> bool {
    with_latch(latch, |l| l.is_set.load(SeqCst) != 0)
}

/// `latch->maybe_sleeping` — read the maybe-sleeping hint.
fn latch_maybe_sleeping(latch: LatchHandle) -> bool {
    with_latch(latch, |l| l.maybe_sleeping.load(SeqCst) != 0)
}

/// `latch->maybe_sleeping = value` — write the maybe-sleeping hint.
fn set_latch_maybe_sleeping(latch: LatchHandle, value: bool) {
    with_latch(latch, |l| {
        l.maybe_sleeping.store(value as i32, SeqCst);
    });
}

/// `latch->owner_pid` — read the owning process PID.
fn latch_owner_pid(latch: LatchHandle) -> i32 {
    with_latch(latch, |l| l.owner_pid.load(SeqCst))
}

#[cfg(test)]
mod tests;
