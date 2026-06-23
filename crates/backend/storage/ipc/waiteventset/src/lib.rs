//! Port of `src/backend/storage/ipc/waiteventset.c` (PostgreSQL 18.3):
//! a `ppoll()`/`pselect()`-like abstraction for waiting on one or more events
//! (latch set, socket readiness, postmaster death, timeout) in a race-free way.
//!
//! # Implementation choice
//!
//! The C file selects a readiness primitive at compile time (epoll on Linux,
//! kqueue on the BSDs/macOS, poll elsewhere). We faithfully reproduce that:
//!
//! - **macOS / BSD (the build target here): `WAIT_USE_KQUEUE`.** Latch wakeups
//!   arrive via `EVFILT_SIGNAL` on `SIGURG`; postmaster death via `EVFILT_PROC`
//!   `NOTE_EXIT` on `PostmasterPid`; there is no self-pipe or signalfd, and
//!   `InitializeWaitEventSupport` just `pqsignal(SIGURG, SIG_IGN)`s.
//! - **Linux: `WAIT_USE_EPOLL` + `WAIT_USE_SIGNALFD`,** behind
//!   `cfg(target_os = "linux")`. Latch wakeups arrive via a `signalfd` reading
//!   `SIGURG`; postmaster death via the postmaster death-watch pipe fd.
//!
//! WIN32 and the `WAIT_USE_POLL`/`WAIT_USE_SELF_PIPE` fallbacks are not part of
//! any platform we build, so they are not reproduced (matching the other
//! ports' "skip WIN32 / non-target arms" convention).
//!
//! # Model notes (audit against these)
//!
//! - The C `WaitEventSet *` is header-opaque; consumers hold the owning
//!   [`waiteventset_seams::WaitEventSet`] guard naming a
//!   [`WaitEventSetHandle`]. The real `WaitEventSet` lives in this unit's
//!   backend-private (thread-local) registry, keyed by that handle. C allocates
//!   sets in `TopMemoryContext` (process lifetime, freed only via
//!   `FreeWaitEventSet`); here a backend is a thread, sets are thread-local,
//!   and `FreeWaitEventSet` (the guard `Drop`) removes the registry entry and
//!   closes the kernel object. The `resowner` argument is always NULL for the
//!   shapes consumers use, so the ResourceOwner remember/forget bookkeeping is
//!   not modeled (it only tracks the same lifetime the guard already enforces).
//! - `set->latch` is carried as an `Option<LatchHandle>`; the wait loop reads
//!   `latch->is_set`/`->maybe_sleeping`/`->owner_pid` and writes
//!   `->maybe_sleeping` through the latch unit's accessor seams (the `Latch`
//!   storage lives in that unit).
//! - `event->user_data` is the non-aliasing `Option<i32>` key the repo's
//!   `WaitEvent` carries (`None` = C's NULL `void *`), not a back-pointer.
//! - `waiting` (the signal-handler-visible flag) is a thread-local
//!   `AtomicBool`; on kqueue it is purely informational (no self-pipe), so it
//!   is maintained for fidelity but the wakeup path uses `kill(pid, SIGURG)`.
//! - `pg_memory_barrier()` is `fence(SeqCst)`; the latch field accesses behind
//!   the seams are `SeqCst` atomics.
//! - OS primitives (`kqueue`/`kevent`/`epoll_*`/`signalfd`/`close`/`kill`/
//!   `read`) are direct `libc` calls — the genuine OS boundary.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::SeqCst;

use ::types_core::{pgsocket, PGINVALID_SOCKET};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_storage::latch::LatchHandle;
use ::types_storage::waiteventset::{
    WaitEvent, WaitEventSetHandle, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH,
    WL_SOCKET_MASK,
};

mod backend;

/// `struct WaitEventSet` (`waiteventset.c`), modeled as an owned value held in
/// the per-thread registry. The platform-specific kernel handle / scratch
/// arrays live in [`backend::Backend`].
struct WaitEventSetData {
    /// `int nevents` — number of registered events.
    nevents: i32,
    /// `int nevents_space` — maximum number of events in this set.
    nevents_space: i32,
    /// `WaitEvent *events` — the registered events (owned, length grows up to
    /// `nevents_space`).
    events: Vec<WaitEvent>,
    /// `Latch *latch` (`set->latch`) — the single `WL_LATCH_SET` latch, if any.
    latch: Option<LatchHandle>,
    /// `int latch_pos` — the offset of the latch event in `events`.
    latch_pos: i32,
    /// `bool exit_on_postmaster_death`.
    exit_on_postmaster_death: bool,
    /// Platform-specific backend state (kqueue fd / epoll fd + scratch).
    backend: backend::Backend,
}

thread_local! {
    /// The backend-private registry of live `WaitEventSet`s, keyed by the
    /// handle minted in [`CreateWaitEventSet`].
    static SETS: RefCell<HashMap<usize, WaitEventSetData>> = RefCell::new(HashMap::new());
    /// `static int next_handle` — monotonic handle id (never 0).
    static NEXT_HANDLE: std::cell::Cell<usize> = const { std::cell::Cell::new(1) };
}

/// `MAXALIGN(len)` (kept for the size accounting C performs; allocation here is
/// `Vec` growth, but we reproduce the `nevents`-based sizing as capacity).
fn run_with_set<R>(handle: WaitEventSetHandle, f: impl FnOnce(&mut WaitEventSetData) -> R) -> R {
    SETS.with(|sets| {
        let mut map = sets.borrow_mut();
        let set = map
            .get_mut(&handle.as_usize())
            .expect("invalid WaitEventSetHandle");
        f(set)
    })
}

/// `InitializeWaitEventSupport()` — initialize the process-local wait event
/// infrastructure. On the kqueue build this just ignores `SIGURG` (it is
/// delivered through the kqueue `EVFILT_SIGNAL`). On Linux it sets up the
/// signalfd.
pub fn InitializeWaitEventSupport() -> PgResult<()> {
    backend::initialize_wait_event_support()
}

/// `CreateWaitEventSet(NULL, nevents)` — allocate a wait event set sized for
/// `nevents` events. Returns the handle naming the registry entry.
pub fn CreateWaitEventSet(nevents: i32) -> PgResult<WaitEventSetHandle> {
    let backend = backend::create(nevents)?;
    let handle_id = NEXT_HANDLE.with(|n| {
        let id = n.get();
        n.set(id + 1);
        id
    });
    let set = WaitEventSetData {
        nevents: 0,
        nevents_space: nevents,
        events: Vec::with_capacity(nevents.max(0) as usize),
        latch: None,
        latch_pos: 0,
        exit_on_postmaster_death: false,
        backend,
    };
    SETS.with(|sets| sets.borrow_mut().insert(handle_id, set));
    Ok(WaitEventSetHandle::new(handle_id))
}

/// `FreeWaitEventSet(set)` — release the set's kernel object and memory.
///
/// The owned [`WaitEventSet`](waiteventset_seams::WaitEventSet)
/// guard's `Drop` routes here, so this can run from a `thread_local!` destructor
/// at process exit. By then the per-thread [`SETS`] registry's own `thread_local!`
/// slot may ALREADY have been destroyed (TLS destruction order between distinct
/// `thread_local!`s is unspecified), and a plain `SETS.with(..)` would panic with
/// `AccessError` — and a panic out of a TLS destructor `abort()`s the process,
/// which the postmaster reaper would misread as a child crash. So we use
/// `try_with`: if the registry is gone we skip the bookkeeping/`close()`, which
/// is faithful to C, where `proc_exit`/`exit()` never calls `FreeWaitEventSet`
/// at all — the OS reclaims the kqueue/epoll fd (and the set's memory) on process
/// teardown.
pub fn FreeWaitEventSet(handle: WaitEventSetHandle) {
    let set = match SETS.try_with(|sets| sets.borrow_mut().remove(&handle.as_usize())) {
        Ok(set) => set,
        // Registry TLS already destroyed (process exit): the OS closes the fd.
        Err(_) => return,
    };
    if let Some(mut set) = set {
        backend::free(&mut set.backend);
    }
}

/// `AddWaitEventToSet(set, events, fd, latch, user_data)` — register an event;
/// returns its position.
pub fn AddWaitEventToSet(
    handle: WaitEventSetHandle,
    mut events: u32,
    fd: pgsocket,
    latch: Option<LatchHandle>,
    user_data: Option<i32>,
) -> PgResult<i32> {
    let my_proc_pid = init_small_seams::my_proc_pid::call();

    // Resolve the validations that need the set, then do the kernel work.
    let pos = run_with_set(handle, |set| -> PgResult<i32> {
        // not enough space
        debug_assert!(set.nevents < set.nevents_space);

        if events == WL_EXIT_ON_PM_DEATH {
            events = WL_POSTMASTER_DEATH;
            set.exit_on_postmaster_death = true;
        }

        if let Some(l) = latch {
            if latch_seams::latch_owner_pid::call(l) != my_proc_pid {
                return Err(PgError::new(
                    ERROR,
                    "cannot wait on a latch owned by another process".to_string(),
                ));
            }
            if set.latch.is_some() {
                return Err(PgError::new(
                    ERROR,
                    "cannot wait on more than one latch".to_string(),
                ));
            }
            if (events & WL_LATCH_SET) != WL_LATCH_SET {
                return Err(PgError::new(
                    ERROR,
                    "latch events only support being set".to_string(),
                ));
            }
        } else if events & WL_LATCH_SET != 0 {
            return Err(PgError::new(
                ERROR,
                "cannot wait on latch without a specified latch".to_string(),
            ));
        }

        // waiting for socket readiness without a socket indicates a bug
        if fd == PGINVALID_SOCKET && (events & WL_SOCKET_MASK) != 0 {
            return Err(PgError::new(
                ERROR,
                "cannot wait on socket event without a socket".to_string(),
            ));
        }

        let pos = set.nevents;
        set.nevents += 1;
        let mut event = WaitEvent {
            pos,
            fd,
            events,
            user_data,
        };

        if events == WL_LATCH_SET {
            set.latch = latch;
            set.latch_pos = pos;
            // kqueue/epoll latch wakeup needs no socket fd here.
            event.fd = backend::latch_set_fd();
        } else if events == WL_POSTMASTER_DEATH {
            event.fd = backend::postmaster_death_fd();
        }

        set.events.push(event);

        // Perform wait-primitive-specific initialization.
        backend::adjust_add(set, pos)?;

        Ok(pos)
    })?;

    Ok(pos)
}

/// `ModifyWaitEvent(set, pos, events, latch)` — change the event mask (and, for
/// a `WL_LATCH_SET` position, the latch) of position `pos`.
pub fn ModifyWaitEvent(
    handle: WaitEventSetHandle,
    pos: i32,
    events: u32,
    latch: Option<LatchHandle>,
) -> PgResult<()> {
    let my_proc_pid = init_small_seams::my_proc_pid::call();

    run_with_set(handle, |set| -> PgResult<()> {
        debug_assert!(pos < set.nevents);

        let old_events = set.events[pos as usize].events;

        // Allow switching between WL_POSTMASTER_DEATH and WL_EXIT_ON_PM_DEATH.
        if set.events[pos as usize].events == WL_POSTMASTER_DEATH {
            if events != WL_POSTMASTER_DEATH && events != WL_EXIT_ON_PM_DEATH {
                return Err(PgError::new(
                    ERROR,
                    "cannot remove postmaster death event".to_string(),
                ));
            }
            set.exit_on_postmaster_death = (events & WL_EXIT_ON_PM_DEATH) != 0;
            return Ok(());
        }

        // If neither the event mask nor the associated latch changes, return
        // early.
        if events == set.events[pos as usize].events
            && (set.events[pos as usize].events & WL_LATCH_SET == 0 || set.latch == latch)
        {
            return Ok(());
        }

        if set.events[pos as usize].events & WL_LATCH_SET != 0
            && events != set.events[pos as usize].events
        {
            return Err(PgError::new(ERROR, "cannot modify latch event".to_string()));
        }

        // FIXME (C): validate event mask
        set.events[pos as usize].events = events;

        if events == WL_LATCH_SET {
            if let Some(l) = latch {
                if latch_seams::latch_owner_pid::call(l) != my_proc_pid {
                    return Err(PgError::new(
                        ERROR,
                        "cannot wait on a latch owned by another process".to_string(),
                    ));
                }
            }
            set.latch = latch;
            // On Unix, the underlying notification object (kqueue signal /
            // signalfd) is the same for all latches, so we can return
            // immediately without touching the kernel object.
            return Ok(());
        }

        backend::adjust_modify(set, pos, old_events)
    })
}

/// `WaitEventSetWait(set, timeout, occurred_events, nevents, wait_event_info)`
/// — wait for events; fills `occurred_events` (up to its length) and returns
/// the count, `0` on timeout.
pub fn WaitEventSetWait(
    handle: WaitEventSetHandle,
    timeout: i64,
    occurred_events: &mut [WaitEvent],
    wait_event_info: u32,
) -> PgResult<i32> {
    let nevents = occurred_events.len() as i32;
    debug_assert!(nevents > 0);

    let mut returned_events = 0;
    let mut cur_timeout: i64 = -1;
    let mut timeout = timeout;

    // Record the current time so we can determine the remaining timeout if
    // interrupted. Monotonic milliseconds (INSTR_TIME_*).
    let start_time = if timeout >= 0 {
        debug_assert!(timeout <= i32::MAX as i64);
        cur_timeout = timeout;
        Some(now_millis())
    } else {
        None
    };

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);

    backend::set_waiting(true);

    let result: PgResult<i32> = (|| {
        while returned_events == 0 {
            // Check if the latch is set already first.
            let latch = run_with_set(handle, |set| set.latch);

            if let Some(l) = latch {
                if !latch_seams::latch_is_set::call(l) {
                    // about to sleep on a latch
                    latch_seams::set_latch_maybe_sleeping::call(l, true);
                    fence(SeqCst);
                    // and recheck
                }
            }

            if let Some(l) = latch {
                if latch_seams::latch_is_set::call(l) {
                    let (latch_pos, user_data) = run_with_set(handle, |set| {
                        (set.latch_pos, set.events[set.latch_pos as usize].user_data)
                    });
                    occurred_events[returned_events as usize] = WaitEvent {
                        fd: PGINVALID_SOCKET,
                        pos: latch_pos,
                        user_data,
                        events: WL_LATCH_SET,
                    };
                    returned_events += 1;

                    // could have been set above
                    latch_seams::set_latch_maybe_sleeping::call(l, false);

                    if returned_events == nevents {
                        break; // output buffer full already
                    }

                    // Poll just once with zero timeout to gather any non-latch
                    // events that fit alongside.
                    cur_timeout = 0;
                    timeout = 0;
                }
            }

            // Wait for events using the readiness primitive.
            let rc = backend::wait_block(
                handle,
                cur_timeout,
                &mut occurred_events[returned_events as usize..],
            )?;

            if let Some(l) = latch {
                if latch_seams::latch_maybe_sleeping::call(l) {
                    latch_seams::set_latch_maybe_sleeping::call(l, false);
                }
            }

            if rc == -1 {
                break; // timeout occurred
            } else {
                returned_events += rc;
            }

            // If we're not done, update cur_timeout for next iteration.
            if returned_events == 0 && timeout >= 0 {
                let elapsed = now_millis() - start_time.unwrap();
                cur_timeout = timeout - elapsed;
                if cur_timeout <= 0 {
                    break;
                }
            }
        }
        Ok(returned_events)
    })();

    backend::set_waiting(false);

    waitevent_seams::pgstat_report_wait_end::call();

    result
}

/// `GetNumRegisteredWaitEvents(set)` — `set->nevents`.
pub fn GetNumRegisteredWaitEvents(handle: WaitEventSetHandle) -> i32 {
    run_with_set(handle, |set| set.nevents)
}

/// `WaitEventSetCanReportClosed()` — true for epoll/kqueue (and poll with
/// `POLLRDHUP`).
pub fn WaitEventSetCanReportClosed() -> bool {
    true
}

/// `WakeupMyProc()` — wake this process's own blocked wait. On kqueue/signalfd
/// builds this is `kill(MyProcPid, SIGURG)` while `waiting`.
pub fn WakeupMyProc() {
    backend::wakeup_my_proc();
}

/// `WakeupOtherProc(pid)` — `kill(pid, SIGURG)` to wake another process.
pub fn WakeupOtherProc(pid: i32) {
    // SAFETY: kill with SIGURG; errors are ignored in C.
    unsafe {
        libc::kill(pid, libc::SIGURG);
    }
}

// ---- time helper (INSTR_TIME, monotonic milliseconds) ----

fn now_millis() -> i64 {
    // SAFETY: clock_gettime into a zeroed timespec.
    let mut ts: libc::timespec = unsafe { core::mem::zeroed() };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts as *mut libc::timespec);
    }
    (ts.tv_sec as i64) * 1000 + (ts.tv_nsec as i64) / 1_000_000
}

/// Install the inward `waiteventset` seams consumed by latch / pqcomm /
/// syslogger / nodeAppend / miscinit.
pub fn init_seams() {
    use waiteventset_seams as s;
    s::create_wait_event_set::set(CreateWaitEventSet);
    s::add_wait_event_to_set::set(AddWaitEventToSet);
    s::modify_wait_event::set(ModifyWaitEvent);
    s::wait_event_set_wait::set(WaitEventSetWait);
    s::get_num_registered_wait_events::set(GetNumRegisteredWaitEvents);
    s::free_wait_event_set::set(FreeWaitEventSet);
    s::wakeup_my_proc::set(WakeupMyProc);
    s::wakeup_other_proc::set(WakeupOtherProc);
    s::initialize_wait_event_support::set(InitializeWaitEventSupport);
}
