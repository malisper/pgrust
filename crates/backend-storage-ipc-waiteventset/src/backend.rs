//! Platform-specific readiness-primitive backends for `waiteventset.c`.
//!
//! `cfg(target_os = "linux")` selects the `WAIT_USE_EPOLL` + `WAIT_USE_SIGNALFD`
//! path; everything else (macOS, the BSDs — the build target here) selects
//! `WAIT_USE_KQUEUE`. Each module exposes the same small surface used by the
//! generic code in `lib.rs`:
//!
//! - `Backend` — the per-set kernel handle + scratch arrays.
//! - `initialize_wait_event_support()` — process-local setup.
//! - `create(nevents)` / `free(&mut Backend)`.
//! - `adjust_add(set, pos)` / `adjust_modify(set, pos, old_events)`.
//! - `wait_block(handle, cur_timeout, occurred)` — the
//!   `WaitEventSetWaitBlock` body.
//! - `latch_set_fd()` / `postmaster_death_fd()` — the per-event fd assignment.
//! - `set_waiting(bool)` / `wakeup_my_proc()`.

use std::cell::Cell;

use types_core::PGINVALID_SOCKET;
#[cfg(not(target_family = "wasm"))]
use types_error::{PgError, ERROR};
use types_error::PgResult;
#[cfg(not(target_family = "wasm"))]
use types_storage::waiteventset::{
    WL_POSTMASTER_DEATH, WL_SOCKET_CLOSED, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};
use types_storage::waiteventset::{WaitEvent, WaitEventSetHandle, WL_LATCH_SET};

use crate::WaitEventSetData;

thread_local! {
    /// `static volatile sig_atomic_t waiting` — are we currently in
    /// `WaitEventSetWait`? Informational on the kqueue/signalfd builds.
    static WAITING: Cell<bool> = const { Cell::new(false) };
}

pub fn set_waiting(value: bool) {
    WAITING.with(|w| w.set(value));
}

fn is_waiting() -> bool {
    WAITING.with(|w| w.get())
}

/// `WakeupMyProc()` — wake our own blocked wait. On both supported builds this
/// is `kill(MyProcPid, SIGURG)` while `waiting` (kqueue/signalfd consume SIGURG
/// rather than the self-pipe).
pub fn wakeup_my_proc() {
    if is_waiting() {
        let _pid = backend_utils_init_small_seams::my_proc_pid::call();
        // SAFETY: kill with SIGURG; async-signal-safe and infallible in C.
        // wasm: no `kill`/`SIGURG`; single-user has no blocked wait to wake.
        #[cfg(not(target_family = "wasm"))]
        unsafe {
            libc::kill(_pid, libc::SIGURG);
        }
    }
}

#[cfg(not(target_family = "wasm"))]
fn errno() -> core::ffi::c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

#[cfg(not(target_family = "wasm"))]
fn os_error_string(e: core::ffi::c_int) -> String {
    std::io::Error::from_raw_os_error(e).to_string()
}

// ===========================================================================
// WAIT_USE_KQUEUE (macOS / BSD)
// ===========================================================================
#[cfg(all(not(target_os = "linux"), not(target_family = "wasm")))]
mod imp {
    use super::*;

    /// `#if defined(WAIT_USE_KQUEUE)` fields of `struct WaitEventSet`.
    pub struct Backend {
        /// `int kqueue_fd`.
        kqueue_fd: i32,
        /// `bool report_postmaster_not_running`.
        report_postmaster_not_running: bool,
    }

    /// `InitializeWaitEventSupport()` — ignore SIGURG (delivered via kqueue
    /// `EVFILT_SIGNAL`).
    pub fn initialize_wait_event_support() -> PgResult<()> {
        port_pqsignal_seams::pqsignal::call(libc::SIGURG, types_signal::SigHandler::Ignore);
        Ok(())
    }

    /// `CreateWaitEventSet` kqueue setup.
    pub fn create(_nevents: i32) -> PgResult<Backend> {
        if !backend_storage_file_seams::acquire_external_fd::call() {
            return Err(PgError::new(
                ERROR,
                format!("AcquireExternalFD, for kqueue, failed: {}", os_error_string(errno())),
            ));
        }
        // SAFETY: kqueue() syscall.
        let kqueue_fd = unsafe { libc::kqueue() };
        if kqueue_fd < 0 {
            backend_storage_file_seams::release_external_fd::call();
            return Err(PgError::new(
                ERROR,
                format!("kqueue failed: {}", os_error_string(errno())),
            ));
        }
        // SAFETY: fcntl FD_CLOEXEC on the kqueue descriptor.
        if unsafe { libc::fcntl(kqueue_fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            let save_errno = errno();
            unsafe {
                libc::close(kqueue_fd);
            }
            backend_storage_file_seams::release_external_fd::call();
            return Err(PgError::new(
                ERROR,
                format!(
                    "fcntl(F_SETFD) failed on kqueue descriptor: {}",
                    os_error_string(save_errno)
                ),
            ));
        }
        Ok(Backend {
            kqueue_fd,
            report_postmaster_not_running: false,
        })
    }

    /// `FreeWaitEventSet` kqueue teardown.
    pub fn free(backend: &mut Backend) {
        // SAFETY: close the kqueue fd.
        unsafe {
            libc::close(backend.kqueue_fd);
        }
        backend_storage_file_seams::release_external_fd::call();
    }

    /// In the kqueue case the latch event carries no socket fd.
    pub fn latch_set_fd() -> i32 {
        PGINVALID_SOCKET
    }

    /// In the kqueue case postmaster death is watched via EVFILT_PROC on
    /// `PostmasterPid`, not a pipe fd; the event fd stays invalid.
    pub fn postmaster_death_fd() -> i32 {
        PGINVALID_SOCKET
    }

    /// `WaitEventAdjustKqueue(set, event, old_events=0)` for an add.
    pub fn adjust_add(set: &mut WaitEventSetData, pos: i32) -> PgResult<()> {
        wait_event_adjust_kqueue(set, pos, 0)
    }

    /// `WaitEventAdjustKqueue(set, event, old_events)` for a modify.
    pub fn adjust_modify(set: &mut WaitEventSetData, pos: i32, old_events: u32) -> PgResult<()> {
        wait_event_adjust_kqueue(set, pos, old_events)
    }

    /// `udata` carries the event position (+1, so 0 is never a valid value we
    /// fail to recognise); the wait loop reads it back to find the WaitEvent.
    fn udata_for(pos: i32) -> *mut libc::c_void {
        (pos as usize + 1) as *mut libc::c_void
    }
    fn pos_from_udata(udata: *mut libc::c_void) -> i32 {
        (udata as usize - 1) as i32
    }

    fn kq_add(k_ev: &mut libc::kevent, ident: usize, filter: i16, action: u16, pos: i32) {
        k_ev.ident = ident;
        k_ev.filter = filter;
        k_ev.flags = action;
        k_ev.fflags = 0;
        k_ev.data = 0;
        k_ev.udata = udata_for(pos);
    }

    /// `WaitEventAdjustKqueue(set, event, old_events)`.
    fn wait_event_adjust_kqueue(
        set: &mut WaitEventSetData,
        pos: i32,
        old_events: u32,
    ) -> PgResult<()> {
        let event = set.events[pos as usize];
        if old_events == event.events {
            return Ok(());
        }

        let mut k_ev: [libc::kevent; 2] = unsafe { core::mem::zeroed() };
        let mut count = 0usize;

        if event.events == WL_POSTMASTER_DEATH {
            // Detect postmaster death via process notification on PostmasterPid.
            let pm_pid = backend_utils_init_small_seams::postmaster_pid::call();
            kq_add(
                &mut k_ev[count],
                pm_pid as usize,
                libc::EVFILT_PROC,
                libc::EV_ADD,
                pos,
            );
            k_ev[count].fflags = libc::NOTE_EXIT;
            count += 1;
        } else if event.events == WL_LATCH_SET {
            // Detect latch wakeup using a signal event.
            kq_add(
                &mut k_ev[count],
                libc::SIGURG as usize,
                libc::EVFILT_SIGNAL,
                libc::EV_ADD,
                pos,
            );
            count += 1;
        } else {
            // Compute adds/deletes between old and new socket masks.
            let old_filt_read = old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED) != 0;
            let new_filt_read = event.events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED) != 0;
            let old_filt_write = old_events & WL_SOCKET_WRITEABLE != 0;
            let new_filt_write = event.events & WL_SOCKET_WRITEABLE != 0;
            if old_filt_read && !new_filt_read {
                kq_add(&mut k_ev[count], event.fd as usize, libc::EVFILT_READ, libc::EV_DELETE, pos);
                count += 1;
            } else if !old_filt_read && new_filt_read {
                kq_add(&mut k_ev[count], event.fd as usize, libc::EVFILT_READ, libc::EV_ADD, pos);
                count += 1;
            }
            if old_filt_write && !new_filt_write {
                kq_add(&mut k_ev[count], event.fd as usize, libc::EVFILT_WRITE, libc::EV_DELETE, pos);
                count += 1;
            } else if !old_filt_write && new_filt_write {
                kq_add(&mut k_ev[count], event.fd as usize, libc::EVFILT_WRITE, libc::EV_ADD, pos);
                count += 1;
            }
        }

        // For WL_SOCKET_READ -> WL_SOCKET_CLOSED, no change needed.
        if count == 0 {
            return Ok(());
        }
        debug_assert!(count <= 2);

        // SAFETY: kevent registers count changes, expecting no events back.
        let rc = unsafe {
            libc::kevent(
                set.backend.kqueue_fd,
                k_ev.as_ptr(),
                count as libc::c_int,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };

        if rc < 0 {
            let e = errno();
            if event.events == WL_POSTMASTER_DEATH && (e == libc::ESRCH || e == libc::EACCES) {
                set.backend.report_postmaster_not_running = true;
            } else {
                return Err(PgError::new(ERROR, format!("kevent() failed: {}", os_error_string(e))));
            }
        } else if event.events == WL_POSTMASTER_DEATH {
            // The postmaster may already have exited (and the pid possibly
            // reused). Verify with PostmasterIsAlive; defer reporting if dead.
            let pm_pid = backend_utils_init_small_seams::postmaster_pid::call();
            // SAFETY: getppid is always safe.
            let ppid = unsafe { libc::getppid() };
            if pm_pid != ppid && !backend_storage_ipc_pmsignal_seams::postmaster_is_alive::call() {
                set.backend.report_postmaster_not_running = true;
            }
        }

        Ok(())
    }

    /// `WaitEventSetWaitBlock` (kqueue): sleep and decode events into
    /// `occurred` (length is the caller's remaining capacity). Returns the
    /// number decoded, `-1` on timeout, `0` to retry.
    pub fn wait_block(
        handle: WaitEventSetHandle,
        cur_timeout: i64,
        occurred: &mut [WaitEvent],
    ) -> PgResult<i32> {
        let nevents = occurred.len() as i32;

        let timeout_ts = if cur_timeout < 0 {
            None
        } else {
            Some(libc::timespec {
                tv_sec: (cur_timeout / 1000) as libc::time_t,
                tv_nsec: ((cur_timeout % 1000) * 1_000_000) as _,
            })
        };

        // Snapshot what we need from the set, and check the deferred
        // postmaster-death report.
        let (kqueue_fd, nevents_space, exit_on_pm_death, report_pm) = crate::run_with_set(handle, |set| {
            (
                set.backend.kqueue_fd,
                set.nevents_space,
                set.exit_on_postmaster_death,
                set.backend.report_postmaster_not_running,
            )
        });

        // Report postmaster events discovered earlier.
        if report_pm {
            if exit_on_pm_death {
                backend_storage_ipc_ipc_seams::proc_exit::call(1);
            }
            occurred[0] = WaitEvent {
                fd: PGINVALID_SOCKET,
                pos: 0,
                user_data: None,
                events: WL_POSTMASTER_DEATH,
            };
            return Ok(1);
        }

        let mut ret_events: Vec<libc::kevent> =
            (0..nevents_space.max(0)).map(|_| unsafe { core::mem::zeroed() }).collect();

        // SAFETY: kevent sleeps and fills ret_events.
        let rc = unsafe {
            libc::kevent(
                kqueue_fd,
                std::ptr::null(),
                0,
                ret_events.as_mut_ptr(),
                nevents.min(nevents_space),
                timeout_ts.as_ref().map_or(std::ptr::null(), |t| t as *const libc::timespec),
            )
        };

        if rc < 0 {
            let e = errno();
            if e != libc::EINTR {
                set_waiting(false);
                return Err(PgError::new(ERROR, format!("kevent() failed: {}", os_error_string(e))));
            }
            return Ok(0);
        } else if rc == 0 {
            return Ok(-1); // timeout exceeded
        }

        let mut returned_events = 0;
        for cur_kqueue_event in ret_events.iter().take(rc as usize) {
            if returned_events >= nevents {
                break;
            }
            let pos = pos_from_udata(cur_kqueue_event.udata);
            // Read the registered event fields.
            let cur_event = crate::run_with_set(handle, |set| set.events[pos as usize]);

            let mut out = WaitEvent {
                pos: cur_event.pos,
                user_data: cur_event.user_data,
                events: 0,
                fd: PGINVALID_SOCKET,
            };

            if cur_event.events == WL_LATCH_SET && cur_kqueue_event.filter == libc::EVFILT_SIGNAL {
                let latch = crate::run_with_set(handle, |set| set.latch);
                if let Some(l) = latch {
                    if backend_storage_ipc_latch_seams::latch_maybe_sleeping::call(l)
                        && backend_storage_ipc_latch_seams::latch_is_set::call(l)
                    {
                        out.fd = PGINVALID_SOCKET;
                        out.events = WL_LATCH_SET;
                        occurred[returned_events as usize] = out;
                        returned_events += 1;
                    }
                }
            } else if cur_event.events == WL_POSTMASTER_DEATH
                && cur_kqueue_event.filter == libc::EVFILT_PROC
                && (cur_kqueue_event.fflags & libc::NOTE_EXIT) != 0
            {
                // Remember for next time (level-triggered semantics).
                crate::run_with_set(handle, |set| {
                    set.backend.report_postmaster_not_running = true;
                });
                if exit_on_pm_death {
                    backend_storage_ipc_ipc_seams::proc_exit::call(1);
                }
                out.fd = PGINVALID_SOCKET;
                out.events = WL_POSTMASTER_DEATH;
                occurred[returned_events as usize] = out;
                returned_events += 1;
            } else if cur_event.events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED)
                != 0
            {
                debug_assert!(cur_event.fd >= 0);
                if (cur_event.events & WL_SOCKET_READABLE) != 0
                    && cur_kqueue_event.filter == libc::EVFILT_READ
                {
                    out.events |= WL_SOCKET_READABLE;
                }
                if (cur_event.events & WL_SOCKET_CLOSED) != 0
                    && cur_kqueue_event.filter == libc::EVFILT_READ
                    && (cur_kqueue_event.flags & libc::EV_EOF) != 0
                {
                    out.events |= WL_SOCKET_CLOSED;
                }
                if (cur_event.events & WL_SOCKET_WRITEABLE) != 0
                    && cur_kqueue_event.filter == libc::EVFILT_WRITE
                {
                    out.events |= WL_SOCKET_WRITEABLE;
                }
                if out.events != 0 {
                    out.fd = cur_event.fd;
                    occurred[returned_events as usize] = out;
                    returned_events += 1;
                }
            }
        }

        Ok(returned_events)
    }
}

// ===========================================================================
// WAIT_USE_EPOLL + WAIT_USE_SIGNALFD (Linux)
// ===========================================================================
#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        /// `static int signal_fd` — the signalfd reading SIGURG.
        static SIGNAL_FD: Cell<i32> = const { Cell::new(-1) };
    }

    /// `#if defined(WAIT_USE_EPOLL)` fields of `struct WaitEventSet`.
    pub struct Backend {
        /// `int epoll_fd`.
        epoll_fd: i32,
    }

    /// `InitializeWaitEventSupport()` — block SIGURG and set up a signalfd.
    pub fn initialize_wait_event_support() -> PgResult<()> {
        if backend_utils_init_small_seams::is_under_postmaster::call() {
            let old = SIGNAL_FD.with(|f| f.get());
            if old != -1 {
                // SAFETY: close the inherited signalfd; ignore errors.
                unsafe {
                    libc::close(old);
                }
                SIGNAL_FD.with(|f| f.set(-1));
                backend_storage_file_seams::release_external_fd::call();
            }
        }

        // Block SIGURG, because we'll receive it through a signalfd.
        backend_libpq_pqsignal_seams::add_unblock_sig::call(libc::SIGURG);

        // Set up the signalfd to receive SIGURG notifications.
        let mut mask: libc::sigset_t = unsafe { core::mem::zeroed() };
        // SAFETY: sigemptyset/sigaddset on a local mask.
        unsafe {
            libc::sigemptyset(&mut mask as *mut libc::sigset_t);
            libc::sigaddset(&mut mask as *mut libc::sigset_t, libc::SIGURG);
        }
        // SAFETY: signalfd creating a non-blocking, cloexec descriptor.
        let fd = unsafe {
            libc::signalfd(-1, &mask as *const libc::sigset_t, libc::SFD_NONBLOCK | libc::SFD_CLOEXEC)
        };
        if fd < 0 {
            return Err(PgError::new(ERROR, "signalfd() failed".to_string()));
        }
        SIGNAL_FD.with(|f| f.set(fd));
        backend_storage_file_seams::reserve_external_fd::call();
        Ok(())
    }

    pub fn create(_nevents: i32) -> PgResult<Backend> {
        if !backend_storage_file_seams::acquire_external_fd::call() {
            return Err(PgError::new(
                ERROR,
                format!("AcquireExternalFD, for epoll_create1, failed: {}", os_error_string(errno())),
            ));
        }
        // SAFETY: epoll_create1 with EPOLL_CLOEXEC.
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll_fd < 0 {
            backend_storage_file_seams::release_external_fd::call();
            return Err(PgError::new(
                ERROR,
                format!("epoll_create1 failed: {}", os_error_string(errno())),
            ));
        }
        Ok(Backend { epoll_fd })
    }

    pub fn free(backend: &mut Backend) {
        // SAFETY: close the epoll fd.
        unsafe {
            libc::close(backend.epoll_fd);
        }
        backend_storage_file_seams::release_external_fd::call();
    }

    /// The latch event reads from the signalfd.
    pub fn latch_set_fd() -> i32 {
        SIGNAL_FD.with(|f| f.get())
    }

    /// Postmaster death watches the death-watch pipe read-end fd.
    pub fn postmaster_death_fd() -> i32 {
        backend_postmaster_postmaster_seams::postmaster_death_watch_fd::call()
    }

    pub fn adjust_add(set: &mut WaitEventSetData, pos: i32) -> PgResult<()> {
        wait_event_adjust_epoll(set, pos, libc::EPOLL_CTL_ADD)
    }

    pub fn adjust_modify(set: &mut WaitEventSetData, pos: i32, _old_events: u32) -> PgResult<()> {
        wait_event_adjust_epoll(set, pos, libc::EPOLL_CTL_MOD)
    }

    /// `WaitEventAdjustEpoll(set, event, action)`.
    fn wait_event_adjust_epoll(set: &mut WaitEventSetData, pos: i32, action: i32) -> PgResult<()> {
        let event = set.events[pos as usize];
        let mut epoll_ev: libc::epoll_event = unsafe { core::mem::zeroed() };
        // epoll's data carries the event position (+1).
        epoll_ev.u64 = pos as u64 + 1;
        // always wait for errors
        let mut events = (libc::EPOLLERR | libc::EPOLLHUP) as u32;

        if event.events == WL_LATCH_SET {
            events |= libc::EPOLLIN as u32;
        } else if event.events == WL_POSTMASTER_DEATH {
            events |= libc::EPOLLIN as u32;
        } else {
            debug_assert!(event.fd != PGINVALID_SOCKET);
            if event.events & WL_SOCKET_READABLE != 0 {
                events |= libc::EPOLLIN as u32;
            }
            if event.events & WL_SOCKET_WRITEABLE != 0 {
                events |= libc::EPOLLOUT as u32;
            }
            if event.events & WL_SOCKET_CLOSED != 0 {
                events |= libc::EPOLLRDHUP as u32;
            }
        }
        epoll_ev.events = events;

        // SAFETY: epoll_ctl with the prepared event.
        let rc = unsafe {
            libc::epoll_ctl(set.backend.epoll_fd, action, event.fd, &mut epoll_ev as *mut libc::epoll_event)
        };
        if rc < 0 {
            return Err(PgError::new(ERROR, format!("epoll_ctl() failed: {}", os_error_string(errno()))));
        }
        Ok(())
    }

    /// Read all available data from the signalfd (`drain()`).
    fn drain() -> PgResult<()> {
        let fd = SIGNAL_FD.with(|f| f.get());
        let mut buf = [0u8; 1024];
        loop {
            // SAFETY: read into a stack buffer.
            let rc = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if rc < 0 {
                let e = errno();
                if e == libc::EAGAIN || e == libc::EWOULDBLOCK {
                    break; // empty
                } else if e == libc::EINTR {
                    continue;
                } else {
                    set_waiting(false);
                    return Err(PgError::new(ERROR, "read() on signalfd failed".to_string()));
                }
            } else if rc == 0 {
                set_waiting(false);
                return Err(PgError::new(ERROR, "unexpected EOF on signalfd".to_string()));
            } else if (rc as usize) < buf.len() {
                break;
            }
        }
        Ok(())
    }

    /// `WaitEventSetWaitBlock` (epoll).
    pub fn wait_block(
        handle: WaitEventSetHandle,
        cur_timeout: i64,
        occurred: &mut [WaitEvent],
    ) -> PgResult<i32> {
        let nevents = occurred.len() as i32;
        let (epoll_fd, nevents_space, exit_on_pm_death) = crate::run_with_set(handle, |set| {
            (set.backend.epoll_fd, set.nevents_space, set.exit_on_postmaster_death)
        });

        let mut ret_events: Vec<libc::epoll_event> =
            (0..nevents_space.max(0)).map(|_| unsafe { core::mem::zeroed() }).collect();

        // SAFETY: epoll_wait sleeps and fills ret_events.
        let rc = unsafe {
            libc::epoll_wait(
                epoll_fd,
                ret_events.as_mut_ptr(),
                nevents.min(nevents_space),
                cur_timeout as libc::c_int,
            )
        };

        if rc < 0 {
            let e = errno();
            if e != libc::EINTR {
                set_waiting(false);
                return Err(PgError::new(ERROR, format!("epoll_wait() failed: {}", os_error_string(e))));
            }
            return Ok(0);
        } else if rc == 0 {
            return Ok(-1); // timeout exceeded
        }

        let mut returned_events = 0;
        for cur_epoll_event in ret_events.iter().take(rc as usize) {
            if returned_events >= nevents {
                break;
            }
            let pos = (cur_epoll_event.u64 - 1) as i32;
            let cur_event = crate::run_with_set(handle, |set| set.events[pos as usize]);
            let ev_bits = cur_epoll_event.events;

            let mut out = WaitEvent {
                pos: cur_event.pos,
                user_data: cur_event.user_data,
                events: 0,
                fd: PGINVALID_SOCKET,
            };

            if cur_event.events == WL_LATCH_SET
                && ev_bits & (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0
            {
                // Drain the signalfd.
                drain()?;
                let latch = crate::run_with_set(handle, |set| set.latch);
                if let Some(l) = latch {
                    if backend_storage_ipc_latch_seams::latch_maybe_sleeping::call(l)
                        && backend_storage_ipc_latch_seams::latch_is_set::call(l)
                    {
                        out.fd = PGINVALID_SOCKET;
                        out.events = WL_LATCH_SET;
                        occurred[returned_events as usize] = out;
                        returned_events += 1;
                    }
                }
            } else if cur_event.events == WL_POSTMASTER_DEATH
                && ev_bits & (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0
            {
                if !backend_storage_ipc_pmsignal_seams::postmaster_is_alive::call() {
                    if exit_on_pm_death {
                        backend_storage_ipc_ipc_seams::proc_exit::call(1);
                    }
                    out.fd = PGINVALID_SOCKET;
                    out.events = WL_POSTMASTER_DEATH;
                    occurred[returned_events as usize] = out;
                    returned_events += 1;
                }
            } else if cur_event.events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED)
                != 0
            {
                debug_assert!(cur_event.fd != PGINVALID_SOCKET);
                if (cur_event.events & WL_SOCKET_READABLE) != 0
                    && ev_bits & (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0
                {
                    out.events |= WL_SOCKET_READABLE;
                }
                if (cur_event.events & WL_SOCKET_WRITEABLE) != 0
                    && ev_bits & (libc::EPOLLOUT | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0
                {
                    out.events |= WL_SOCKET_WRITEABLE;
                }
                if (cur_event.events & WL_SOCKET_CLOSED) != 0
                    && ev_bits & (libc::EPOLLRDHUP | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0
                {
                    out.events |= WL_SOCKET_CLOSED;
                }
                if out.events != 0 {
                    out.fd = cur_event.fd;
                    occurred[returned_events as usize] = out;
                    returned_events += 1;
                }
            }
        }

        Ok(returned_events)
    }
}

// ===========================================================================
// WASM (single-user, no kqueue/epoll/signalfd; no socket waits)
// ===========================================================================
//
// `wasm64-unknown-unknown` has no kqueue/epoll/signalfd and no `kill`/`SIGURG`.
// In single-user mode there is no postmaster, no listener socket, and no other
// process to wake us: the latch is set synchronously on the one backend thread.
// So this backend carries no kernel handle. The only wait that matters is
// `WL_LATCH_SET`, which we resolve by polling the latch directly (no kernel
// blocking primitive exists to register fds against). `WL_SOCKET_*` events are
// never registered single-user; `WL_POSTMASTER_DEATH` can never fire (there is
// no postmaster). A finite-timeout wait that finds nothing ready reports a
// timeout; an infinite-timeout wait reports the latch as ready rather than
// deadlocking (there is no external waker on this target).
#[cfg(target_family = "wasm")]
mod imp {
    use super::*;

    /// No kernel readiness handle on wasm.
    pub struct Backend;

    /// `InitializeWaitEventSupport()` — on wasm there is no kernel signal layer
    /// to register against (no `SIGURG`), so this is a no-op.
    pub fn initialize_wait_event_support() -> PgResult<()> {
        Ok(())
    }

    pub fn create(_nevents: i32) -> PgResult<Backend> {
        Ok(Backend)
    }

    pub fn free(_backend: &mut Backend) {}

    /// No fd carries the latch event (no signalfd on wasm).
    pub fn latch_set_fd() -> i32 {
        PGINVALID_SOCKET
    }

    /// No postmaster, hence no death-watch fd.
    pub fn postmaster_death_fd() -> i32 {
        PGINVALID_SOCKET
    }

    /// Nothing to register against a kernel object.
    pub fn adjust_add(_set: &mut WaitEventSetData, _pos: i32) -> PgResult<()> {
        Ok(())
    }

    pub fn adjust_modify(
        _set: &mut WaitEventSetData,
        _pos: i32,
        _old_events: u32,
    ) -> PgResult<()> {
        Ok(())
    }

    /// `WaitEventSetWaitBlock` (wasm single-user): scan the registered events
    /// for a `WL_LATCH_SET` whose latch is set, and report it. If none is
    /// ready, report a timeout for a finite wait; for an infinite wait, report
    /// the first latch event ready-anyway (no external waker exists, so
    /// blocking forever would deadlock the single thread).
    pub fn wait_block(
        handle: WaitEventSetHandle,
        cur_timeout: i64,
        occurred: &mut [WaitEvent],
    ) -> PgResult<i32> {
        let nevents = occurred.len() as i32;
        if nevents <= 0 {
            return Ok(-1);
        }

        let (events, latch) =
            crate::run_with_set(handle, |set| (set.events.clone(), set.latch));

        // First pass: a latch event whose latch is genuinely set.
        for cur_event in events.iter() {
            if cur_event.events == WL_LATCH_SET {
                if let Some(l) = latch {
                    if backend_storage_ipc_latch_seams::latch_maybe_sleeping::call(l)
                        && backend_storage_ipc_latch_seams::latch_is_set::call(l)
                    {
                        occurred[0] = WaitEvent {
                            fd: PGINVALID_SOCKET,
                            pos: cur_event.pos,
                            user_data: cur_event.user_data,
                            events: WL_LATCH_SET,
                        };
                        return Ok(1);
                    }
                }
            }
        }

        // Nothing ready. A finite wait times out.
        if cur_timeout >= 0 {
            return Ok(-1);
        }

        // Infinite wait with no external waker: report the first latch event so
        // the caller re-checks its condition rather than deadlocking.
        for cur_event in events.iter() {
            if cur_event.events == WL_LATCH_SET {
                occurred[0] = WaitEvent {
                    fd: PGINVALID_SOCKET,
                    pos: cur_event.pos,
                    user_data: cur_event.user_data,
                    events: WL_LATCH_SET,
                };
                return Ok(1);
            }
        }

        // No latch registered at all on an infinite wait: treat as a timeout.
        Ok(-1)
    }
}

pub use imp::{
    adjust_add, adjust_modify, create, free, initialize_wait_event_support, latch_set_fd,
    postmaster_death_fd, wait_block, Backend,
};
