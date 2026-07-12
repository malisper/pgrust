// Thread-model boundary (notes/waiteventset-threads.md): C's SIGURG wakeup
// channels cannot route to one backend-thread, so latch wakeups use C's own
// self-pipe arm — one nonblocking pipe per backend, registered pid->write-end
// (SetLatch writes the target's pipe where C would kill(pid, SIGURG)).
// Readiness keeps C's #ifdef split: epoll on Linux, kqueue elsewhere.
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::sync::Mutex;

use init_small::globals::MyProcPid;
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
#[cfg(not(target_os = "linux"))]
#[path = "kqueue.rs"]
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
    // (read_fd, write_fd) of this backend's wakeup pipe (C selfpipe_*fd).
    static WAKEUP_PIPE: Cell<Option<(i32, i32)>> = const { Cell::new(None) };
    // C's `static volatile sig_atomic_t waiting`.
    static WAITING: Cell<bool> = const { Cell::new(false) };
}

// pid -> wakeup-pipe write fd; locked only at backend init and on
// cross-backend wakes (where C pays a kill(2) syscall).
static WAKEUP_REGISTRY: Mutex<Vec<(i32, i32)>> = Mutex::new(Vec::new());

#[cold]
#[inline(never)]
fn os_error(level: ErrorLevel, msg: &str) -> Box<PgError> {
    Box::new(PgError::new(
        level,
        format!("{msg}: {}", std::io::Error::last_os_error()),
    ))
}

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
        WAKEUP_PIPE.get().is_none(),
        "InitializeWaitEventSupport called twice in this backend"
    );

    let mut pipefd = [0i32; 2];
    // SAFETY: pipe(2) into a 2-slot array.
    if unsafe { libc::pipe(pipefd.as_mut_ptr()) } < 0 {
        return Err(os_error(FATAL, "pipe() failed"));
    }
    for (fd, end) in [(pipefd[0], "read-end"), (pipefd[1], "write-end")] {
        // SAFETY: fcntl on the fds just created.
        if unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) } == -1 {
            return Err(os_error(
                FATAL,
                &format!("fcntl(F_SETFL) failed on {end} of self-pipe"),
            ));
        }
        // SAFETY: as above.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(os_error(
                FATAL,
                &format!("fcntl(F_SETFD) failed on {end} of self-pipe"),
            ));
        }
    }

    fd::ReserveExternalFD()?;
    fd::ReserveExternalFD()?;

    WAKEUP_PIPE.set(Some((pipefd[0], pipefd[1])));

    let pid = MyProcPid();
    let mut registry = WAKEUP_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(entry) = registry.iter_mut().find(|(p, _)| *p == pid) {
        entry.1 = pipefd[1];
    } else {
        registry.push((pid, pipefd[1]));
    }
    Ok(())
}

/// Retained-thread pid refresh (wretain): a warm-claimed pool standby keeps
/// its wakeup pipe across tasks but runs each task under a fresh synthetic
/// MyProcPid, and WakeupOtherProc resolves targets by task pid — the entry
/// must follow the pid or every cross-thread SetLatch to this thread is lost
/// (shm_mq wakes, ConditionVariable broadcasts, SendThreadSignal).
pub fn RekeyWakeupRegistry() {
    let (_, write_fd) = WAKEUP_PIPE
        .get()
        .expect("RekeyWakeupRegistry before InitializeWaitEventSupport");
    let pid = MyProcPid();
    let mut registry = WAKEUP_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    match registry.iter_mut().find(|(_, w)| *w == write_fd) {
        Some(entry) => entry.0 = pid,
        None => registry.push((pid, write_fd)),
    }
}

/// Diagnostics for MQ stall self-reports: the registry write fd for `pid`
/// (None = no mapping, i.e. a SetLatch aimed at that pid wakes nobody) plus
/// the registry length. Cold path only — takes the registry lock.
pub fn WakeupRegistrySnapshot(pid: i32) -> (Option<i32>, usize) {
    let registry = WAKEUP_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    (
        registry.iter().find(|(p, _)| *p == pid).map(|(_, w)| *w),
        registry.len(),
    )
}

fn wakeup_read_fd() -> i32 {
    WAKEUP_PIPE
        .get()
        .expect("InitializeWaitEventSupport has not run in this backend")
        .0
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
        // Registry TLS already destroyed (thread exit): the OS reclaims fds.
        Err(_) => return,
    };
    if let Some(set) = set {
        set.backend.free();
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
    WAITING.set(true);

    let result = run_with_set(handle, |set| {
        wait_loop(set, timeout, cur_timeout, start_time, occurred_events)
    });

    WAITING.set(false);
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
        if let Some(l) = latch {
            if !l.is_set() {
                // C: store, pg_memory_barrier, recheck; the SeqCst store+load
                // pair carries that edge (notes/latch-atomics.md).
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
            }
        }

        let rc = backend::wait_block(
            set,
            latch,
            cur_timeout,
            &mut occurred_events[returned_events as usize..],
        )?;

        if let Some(l) = latch {
            if l.maybe_sleeping.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                l.set_maybe_sleeping(false);
            }
        }

        if rc == -1 {
            break; // timeout occurred
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

fn send_wakeup_byte(write_fd: i32) {
    let dummy = [0u8; 1];
    loop {
        // SAFETY: 1-byte write to a live nonblocking pipe fd.
        let rc = unsafe { libc::write(write_fd, dummy.as_ptr().cast(), 1) };
        if rc >= 0 {
            return;
        }
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::EINTR {
            continue;
        }
        // EAGAIN: queued bytes already wake the waiter; others: C's handler
        // cannot elog, silently ignore.
        return;
    }
}

// Signal-handler/critical-section reachable: no allocation, no errors.
pub fn WakeupMyProc() {
    if WAITING.get() {
        if let Some((_, write_fd)) = WAKEUP_PIPE.get() {
            send_wakeup_byte(write_fd);
        }
    }
}

// C: kill(pid, SIGURG); here: write the target backend's wakeup pipe.
pub fn WakeupOtherProc(pid: i32) {
    let registry = WAKEUP_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, write_fd)) = registry.iter().find(|(p, _)| *p == pid) {
        send_wakeup_byte(*write_fd);
    }
}

// Read all pending data from the wakeup pipe (C drain()).
pub(crate) fn drain() -> PgResult<()> {
    let fd = wakeup_read_fd();
    let mut buf = [0u8; 1024];
    loop {
        // SAFETY: read into a stack buffer of the stated length.
        let rc = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if rc < 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                return Ok(());
            }
            if errno == libc::EINTR {
                continue;
            }
            return Err(os_error(ERROR, "read() on self-pipe failed"));
        }
        if rc == 0 {
            return Err(wes_error("unexpected EOF on self-pipe"));
        }
        if (rc as usize) < buf.len() {
            return Ok(());
        }
    }
}

fn now_millis() -> i64 {
    // SAFETY: clock_gettime(CLOCK_MONOTONIC) into a zeroed timespec.
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to ts.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as i64 * 1000 + ts.tv_nsec as i64 / 1_000_000
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
    s::wakeup_my_proc::set(WakeupMyProc);
    s::wakeup_other_proc::set(WakeupOtherProc);
    s::rekey_wakeup_registry::set(RekeyWakeupRegistry);
    s::wakeup_registry_snapshot::set(WakeupRegistrySnapshot);
}
