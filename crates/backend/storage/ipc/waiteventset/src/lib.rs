// Thread-model boundary (notes/waiteventset-threads.md): readiness keeps
// C's #ifdef split — epoll on Linux, kqueue elsewhere — for socket waits.
// The latch-wake half is the Waiter (storage/ipc/waiter): a WL_LATCH_SET
// event registers the thread's waiter wake-pipe read end, the wait blocks
// in fd-park mode, and SetLatch unparks the waker handle the owner
// published into Latch.waker. The old pid-keyed WAKEUP_REGISTRY and the
// per-backend self-pipe it routed are GONE — the pid-collision hijack and
// stale-rekey deafness wedge classes are structurally impossible (handles
// are token-validated, republished at every wait).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};

use init_small::globals::{IsUnderPostmaster, MyProcPid};
use types_core::{pgsocket, PGINVALID_SOCKET};
use types_error::{ErrorLevel, PgError, PgResult, ERROR, FATAL};
use types_storage::latch::{Latch, LatchHandle};
use types_storage::waiteventset::{
    WaitEvent, WaitEventSetHandle, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH,
    WL_SOCKET_MASK,
};

#[cfg(target_os = "linux")]
#[path = "epoll.rs"]
mod backend;
#[cfg(not(any(target_os = "linux", target_family = "wasm")))]
#[path = "kqueue.rs"]
mod backend;
// wasm32: compile-clean stub; the functional WASI poll_oneoff backend is the
// P5 boot increment (docs/design/dst-and-wasm.md §5).
#[cfg(target_family = "wasm")]
#[path = "wasm_stub.rs"]
mod backend;

#[cfg(test)]
mod tests;

pub(crate) struct WaitEventSetData {
    nevents: i32,
    nevents_space: i32,
    // Owner structure (kernel fd + registrations) per docs/no-drop.md.
    events: Vec<WaitEvent>,
    latch: Option<LatchHandle>,
    latch_pos: i32,
    backend: backend::BackendSet,
}

thread_local! {
    static SETS: RefCell<Vec<Option<WaitEventSetData>>> = const { RefCell::new(Vec::new()) };
    // Once-per-thread InitializeWaitEventSupport tripwire (C ran it once per
    // process; wretain warm claims deliberately skip it).
    static WES_INITIALIZED: Cell<bool> = const { Cell::new(false) };
}

// The postmaster thread's waiter handle (packed word, 0 = not yet
// published): the ONE well-known wake route that used to ride the pid
// registry (SendPostmasterSignal's kill(PostmasterPid, SIGUSR1) analog).
static POSTMASTER_WAKER: AtomicU64 = AtomicU64::new(0);

// Only the epoll/kqueue backends raise OS errors; the wasm stub never does.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
#[track_caller]
#[cold]
#[inline(never)]
fn os_error(level: ErrorLevel, msg: &str) -> Box<PgError> {
    Box::new(PgError::new(
        level,
        format!("{msg}: {}", std::io::Error::last_os_error()),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn wes_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg))
}

fn run_with_set<R>(handle: WaitEventSetHandle, f: impl FnOnce(&mut WaitEventSetData) -> R) -> R {
    SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        let set = sets
            .get_mut(handle.as_usize().wrapping_sub(1))
            .and_then(Option::as_mut)
            .expect("invalid WaitEventSetHandle");
        f(set)
    })
}

pub fn InitializeWaitEventSupport() -> PgResult<()> {
    assert!(
        !WES_INITIALIZED.get(),
        "InitializeWaitEventSupport called twice in this backend"
    );
    WES_INITIALIZED.set(true);

    // The waiter wake pipe replaces C's self-pipe (created eagerly here to
    // keep the per-backend fd accounting where it always was).
    // wasm32: no pipe(2) on WASI; the wasm backend blocks on time alone
    // (single thread — no cross-thread wakes exist to route).
    #[cfg(not(target_family = "wasm"))]
    {
        if let Err(errno) = waiter::ensure_wake_pipe() {
            return Err(Box::new(PgError::new(
                FATAL,
                format!("waiter wake pipe creation failed: errno {errno}"),
            )));
        }
        fd::ReserveExternalFD()?;
        fd::ReserveExternalFD()?;
    }

    // The postmaster publishes its well-known waker (children have
    // IsUnderPostmaster set before they reach this in InitPostmasterChild).
    if !IsUnderPostmaster() {
        POSTMASTER_WAKER.store(waiter::current_handle().as_u64(), Ordering::Release);
    }
    Ok(())
}

/// Pooled-thread reuse boundary (wretain warm claim): reissue this thread's
/// waiter token so handles published for the previous task go stale. The
/// old pid-keyed registry rekey this replaces was itself a wedge source (a
/// missed rekey silently dropped every wake); the waiter has no registry to
/// rekey — the live route is republished at every wait entry.
pub fn RekeyWakeupRegistry() {
    waiter::reissue_current_token();
}

/// C: kill(PostmasterPid, SIGUSR1) analog for SendPostmasterSignal's
/// fallback path — the single well-known cross-thread kick that used to be
/// pid-routed. Wakes the postmaster's waiter (fd-park aware: serverloop
/// blocks in epoll/kqueue on its accept sockets + latch).
pub fn WakeupPostmaster() {
    waiter::unpark_word(POSTMASTER_WAKER.load(Ordering::Acquire));
}

#[cfg(not(target_family = "wasm"))]
fn wakeup_read_fd() -> i32 {
    waiter::wake_read_fd()
}

// wasm32: no wake pipe exists; latch events carry no fd (the wasm backend
// blocks on time alone and the generic loop re-checks latch.is_set).
#[cfg(target_family = "wasm")]
fn wakeup_read_fd() -> i32 {
    PGINVALID_SOCKET
}

pub fn CreateWaitEventSet(nevents: i32) -> PgResult<WaitEventSetHandle> {
    let backend = backend::BackendSet::create(nevents)?;
    let data = WaitEventSetData {
        nevents: 0,
        nevents_space: nevents,
        events: Vec::with_capacity(nevents.max(0) as usize),
        latch: None,
        latch_pos: 0,
        backend,
    };
    let id = SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        match sets.iter().position(Option::is_none) {
            Some(i) => {
                sets[i] = Some(data);
                i + 1
            }
            None => {
                sets.push(Some(data));
                sets.len()
            }
        }
    });
    Ok(WaitEventSetHandle::new(id))
}

// C: CreateWaitEventSet(CurrentResourceOwner, nevents); owner tracking is
// deferred to the resowner port (the sole consumer frees on every path).
pub fn CreateWaitEventSetCurrentOwner(nevents: i32) -> PgResult<WaitEventSetHandle> {
    CreateWaitEventSet(nevents)
}

pub fn FreeWaitEventSet(handle: WaitEventSetHandle) {
    let set = match SETS.try_with(|sets| {
        sets.borrow_mut()
            .get_mut(handle.as_usize().wrapping_sub(1))
            .and_then(Option::take)
    }) {
        Ok(set) => set,
        // Registry TLS already destroyed (thread exit). Only PROCESS death
        // reclaims kernel fds, which is why WaitEventSetReleaseGuard frees
        // every still-registered set before TLS teardown; a call landing
        // here can only race that guard, whose sweep already freed the set.
        Err(_) => return,
    };
    if let Some(set) = set {
        set.backend.free();
    }
}

// Frees every set still registered on this thread. BackendSet deliberately
// has no Drop (docs/no-drop.md keeps drop authority in explicit owners), so
// a set that is still live when the thread's TLS unwinds would leak its
// epoll/kqueue fd: C's "static, never freed" sets (LatchWaitSet, FeBeWaitSet)
// are reclaimed by process death, and a session THREAD's death reclaims
// nothing (chaos F2: 2 leaked epoll fds per connection under churn).
fn release_thread_wait_event_sets() {
    // Defensive try_with: the guard drops before TLS destruction on the
    // launch paths, but an exotic exit must degrade to the old leak, not
    // panic inside unwinding.
    let Ok(sets) = SETS.try_with(|sets| std::mem::take(&mut *sets.borrow_mut())) else {
        return;
    };
    for set in sets.into_iter().flatten() {
        set.backend.free();
    }
}

/// Backend-thread teardown for this thread's WaitEventSets: Drop frees every
/// set still registered — the thread-model analog of C's reclaim-at-process-
/// death for the deliberately-never-freed sets (LatchWaitSet, FeBeWaitSet).
/// Held at the top of the child thread (LocalLatchReleaseGuard house style),
/// declared AFTER the latch guard so it drops FIRST: strictly after
/// run_child_task has drained the deferred proc_exit callback stacks (no
/// callback can wait on a freed set), and before the local latch slot the
/// sets reference is recycled. Thread-scoped, not per-task: a parked wretain
/// standby keeps its sets warm; only thread exit frees them.
#[must_use]
pub struct WaitEventSetReleaseGuard(());

impl WaitEventSetReleaseGuard {
    pub fn new() -> Self {
        Self(())
    }
}

impl Default for WaitEventSetReleaseGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WaitEventSetReleaseGuard {
    fn drop(&mut self) {
        release_thread_wait_event_sets();
    }
}

pub fn AddWaitEventToSet(
    handle: WaitEventSetHandle,
    events: u32,
    fd: pgsocket,
    latch: Option<LatchHandle>,
    user_data: Option<i32>,
) -> PgResult<i32> {
    // One address space: the postmaster cannot die while any backend thread
    // runs, so the death watch is an event that can never fire — registered
    // inert (position reserved, nothing handed to the kernel).
    if events == WL_EXIT_ON_PM_DEATH || events == WL_POSTMASTER_DEATH {
        return run_with_set(handle, |set| {
            assert!(set.nevents < set.nevents_space, "no space for wait event");
            let pos = set.nevents;
            set.nevents += 1;
            set.events.push(WaitEvent { pos, fd: PGINVALID_SOCKET, events, user_data });
            Ok(pos)
        });
    }
    let my_pid = MyProcPid();

    run_with_set(handle, |set| {
        assert!(set.nevents < set.nevents_space, "no space for wait event");

        if let Some(l) = latch {
            if latch::latch_ref(l).owner_pid() != my_pid {
                return Err(wes_error("cannot wait on a latch owned by another process"));
            }
            if set.latch.is_some() {
                return Err(wes_error("cannot wait on more than one latch"));
            }
            if events & WL_LATCH_SET != WL_LATCH_SET {
                return Err(wes_error("latch events only support being set"));
            }
        } else if events & WL_LATCH_SET != 0 {
            return Err(wes_error("cannot wait on latch without a specified latch"));
        }

        if fd == PGINVALID_SOCKET && events & WL_SOCKET_MASK != 0 {
            return Err(wes_error("cannot wait on socket event without a socket"));
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
            event.fd = wakeup_read_fd();
        }
        set.events.push(event);

        set.backend.register(&set.events[pos as usize], 0)?;
        Ok(pos)
    })
}

pub fn ModifyWaitEvent(
    handle: WaitEventSetHandle,
    pos: i32,
    events: u32,
    latch: Option<LatchHandle>,
) -> PgResult<()> {
    let my_pid = MyProcPid();

    run_with_set(handle, |set| {
        assert!(pos < set.nevents, "wait event position out of range");
        let old_events = set.events[pos as usize].events;

        if old_events & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH) != 0 {
            set.events[pos as usize].events = events;
            return Ok(());
        }

        // Fast path: mask and latch unchanged (read<->write socket switch).
        if events == old_events && (old_events & WL_LATCH_SET == 0 || set.latch == latch) {
            return Ok(());
        }

        if old_events & WL_LATCH_SET != 0 && events != old_events {
            return Err(wes_error("cannot modify latch event"));
        }

        set.events[pos as usize].events = events;

        if events == WL_LATCH_SET {
            if let Some(l) = latch {
                if latch::latch_ref(l).owner_pid() != my_pid {
                    return Err(wes_error("cannot wait on a latch owned by another process"));
                }
            }
            set.latch = latch;
            // The wakeup pipe is the same for every latch: no kernel change.
            return Ok(());
        }

        set.backend.register(&set.events[pos as usize], old_events)
    })
}

pub fn WaitEventSetWait(
    handle: WaitEventSetHandle,
    timeout: i64,
    occurred_events: &mut [WaitEvent],
    wait_event_info: u32,
) -> PgResult<i32> {
    debug_assert!(!occurred_events.is_empty());

    let mut cur_timeout: i64 = -1;
    let start_time = if timeout >= 0 {
        debug_assert!(timeout <= i32::MAX as i64);
        cur_timeout = timeout;
        Some(now_millis())
    } else {
        None
    };

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);

    let result = run_with_set(handle, |set| {
        wait_loop(set, timeout, cur_timeout, start_time, occurred_events)
    });

    waitevent_seams::pgstat_report_wait_end::call();
    result
}

fn wait_loop(
    set: &mut WaitEventSetData,
    mut timeout: i64,
    mut cur_timeout: i64,
    start_time: Option<i64>,
    occurred_events: &mut [WaitEvent],
) -> PgResult<i32> {
    let nevents = occurred_events.len() as i32;
    let latch: Option<&'static Latch> = set.latch.map(latch::latch_ref);
    let mut returned_events: i32 = 0;

    while returned_events == 0 {
        let mut fd_parked = false;
        if let Some(l) = latch {
            if !l.is_set() {
                // Dekker arm: publish the wake route first, then
                // maybe_sleeping (SeqCst store), then recheck — matching
                // WaitLatch's waiter path (notes/latch-atomics.md; the
                // Acquire pairing lives in set_latch).
                l.waker.store(
                    waiter::current_handle().as_u64(),
                    std::sync::atomic::Ordering::Release,
                );
                l.set_maybe_sleeping(true);
            }
            if l.is_set() {
                occurred_events[returned_events as usize] = WaitEvent {
                    fd: PGINVALID_SOCKET,
                    pos: set.latch_pos,
                    user_data: set.events[set.latch_pos as usize].user_data,
                    events: WL_LATCH_SET,
                };
                returned_events += 1;
                l.set_maybe_sleeping(false);

                if returned_events == nevents {
                    break;
                }
                // Poll once with zero timeout for non-latch events that fit.
                cur_timeout = 0;
                timeout = 0;
            } else {
                // fd-park: unparks aimed at this thread now write the wake
                // pipe registered in this set. A notification that already
                // landed (wake-before-park) degrades the block to a poll.
                fd_parked = waiter::begin_fd_park();
                if !fd_parked {
                    cur_timeout = 0;
                }
            }
        }

        // GL-RECWAKE-1: while fd-parked the ONLY route for a latch wake is a
        // waiter unpark writing the wake pipe — a lost one leaves the kernel
        // block deaf to an already-set latch for the caller's whole timeout
        // (forever at -1). Bound each kernel lap by the waiter recheck
        // cadence: the loop head re-tests latch.is_set every lap, so a lost
        // wake costs at most one cadence period — the untimed-park law,
        // extended to the fd-park mode. Latch-free waits are exempt (their
        // events are kernel-delivered; no wake can be lost).
        let mut block_timeout = cur_timeout;
        if fd_parked {
            let cadence = waiter::recheck_cadence_ms();
            if cadence > 0 && (block_timeout < 0 || block_timeout > cadence) {
                block_timeout = cadence;
            }
        }

        let rc = backend::wait_block(
            set,
            latch,
            block_timeout,
            &mut occurred_events[returned_events as usize..],
        )?;

        if let Some(l) = latch {
            if fd_parked {
                waiter::end_fd_park();
            }
            if l.maybe_sleeping.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                l.set_maybe_sleeping(false);
            }
        }

        if rc == -1 {
            // Events already gathered: the zero-timeout extra poll is done.
            if returned_events > 0 {
                break;
            }
            // The kernel block expired: only the CALLER's deadline ends the
            // wait; a shorter cadence lap re-arms and re-tests (lost-wake
            // backstop above).
            if timeout < 0 {
                continue;
            }
            cur_timeout = timeout - (now_millis() - start_time.expect("timeout without start"));
            if cur_timeout <= 0 {
                break;
            }
            continue;
        }
        returned_events += rc;

        if returned_events == 0 && timeout >= 0 {
            cur_timeout = timeout - (now_millis() - start_time.expect("timeout without start"));
            if cur_timeout <= 0 {
                break;
            }
        }
    }
    Ok(returned_events)
}

pub fn GetNumRegisteredWaitEvents(handle: WaitEventSetHandle) -> i32 {
    run_with_set(handle, |set| set.nevents)
}

pub fn WaitEventSetCanReportClosed() -> bool {
    true
}

// Read all pending data from the waiter wake pipe (C drain()). The
// lost-wake fault injection (PGRUST_TEST_DROP_WAKE_1IN) now lives at the
// Waiter layer (waiter::unpark drops every Nth cross-thread wake).
pub(crate) fn drain() -> PgResult<()> {
    match waiter::drain_wake_fd() {
        Ok(()) => Ok(()),
        Err(0) => Err(wes_error("unexpected EOF on waiter wake pipe")),
        Err(_errno) => Err(os_error(ERROR, "read() on waiter wake pipe failed")),
    }
}

fn now_millis() -> i64 {
    // DST P2 (contract §1.3): the named waiteventset gap — timeout math on
    // the one monotonic authority. The epoll block itself stays; scheduler
    // visibility of the fd-park is P3.
    pg_clock::mono_ms()
}

fn wait_event_set_wait_one(
    set: WaitEventSetHandle,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<Option<WaitEvent>> {
    let mut buf = [WaitEvent::default()];
    let n = WaitEventSetWait(set, timeout, &mut buf, wait_event_info)?;
    Ok((n > 0).then_some(buf[0]))
}

pub fn init_seams() {
    use waiteventset_seams as s;
    s::create_wait_event_set::set(CreateWaitEventSet);
    s::create_wait_event_set_current_owner::set(CreateWaitEventSetCurrentOwner);
    s::add_wait_event_to_set::set(AddWaitEventToSet);
    s::modify_wait_event::set(ModifyWaitEvent);
    s::wait_event_set_wait_one::set(wait_event_set_wait_one);
    s::free_wait_event_set::set(FreeWaitEventSet);
    s::wakeup_postmaster::set(WakeupPostmaster);
    s::rekey_wakeup_registry::set(RekeyWakeupRegistry);
}
