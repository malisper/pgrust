use std::sync::atomic::Ordering::SeqCst;

use types_storage::waiteventset::{WL_SOCKET_CLOSED, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};

use crate::{drain, os_error, Latch, PgResult, WaitEvent, WaitEventSetData, ERROR};
use types_core::PGINVALID_SOCKET;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

pub(crate) struct BackendSet {
    kqueue_fd: i32,
    ret_events: Vec<libc::kevent>,
}

fn kev(fd: i32, filter: i16, flags: u16, pos: i32) -> libc::kevent {
    libc::kevent {
        ident: fd as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        // C's udata WaitEvent pointer -> stable position (Vec may realloc).
        udata: pos as libc::intptr_t as *mut libc::c_void,
    }
}

impl BackendSet {
    pub(crate) fn create(nevents: i32) -> PgResult<Self> {
        if !fd::AcquireExternalFD()? {
            return Err(os_error(ERROR, "AcquireExternalFD, for kqueue, failed"));
        }
        // SAFETY: kqueue(2).
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            fd::ReleaseExternalFD();
            return Err(os_error(ERROR, "kqueue failed"));
        }
        // SAFETY: fcntl on the fd just created.
        if unsafe { libc::fcntl(kq, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            let err = os_error(ERROR, "fcntl(F_SETFD) failed on kqueue descriptor");
            // SAFETY: closing the fd we just opened.
            unsafe { libc::close(kq) };
            fd::ReleaseExternalFD();
            return Err(err);
        }
        Ok(BackendSet {
            kqueue_fd: kq,
            ret_events: Vec::with_capacity(nevents.max(0) as usize),
        })
    }

    pub(crate) fn free(&self) {
        // SAFETY: closing our kqueue fd.
        unsafe { libc::close(self.kqueue_fd) };
        fd::ReleaseExternalFD();
    }

    // WaitEventAdjustKqueue; old_events is 0 on the add path.
    pub(crate) fn register(&self, event: &WaitEvent, old_events: u32) -> PgResult<()> {
        if old_events == event.events {
            return Ok(());
        }
        let mut k_ev = [kev(0, 0, 0, 0); 2];
        let mut count = 0;

        if event.events == WL_LATCH_SET {
            // DIVERGENCE from EVFILT_SIGNAL(SIGURG): the wakeup pipe
            // read end (crate-top note). Add-only, as in C.
            k_ev[count] = kev(event.fd, libc::EVFILT_READ, libc::EV_ADD, event.pos);
            count += 1;
        } else {
            let old_filt_read = old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED) != 0;
            let new_filt_read = event.events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED) != 0;
            let old_filt_write = old_events & WL_SOCKET_WRITEABLE != 0;
            let new_filt_write = event.events & WL_SOCKET_WRITEABLE != 0;

            if old_filt_read && !new_filt_read {
                k_ev[count] = kev(event.fd, libc::EVFILT_READ, libc::EV_DELETE, event.pos);
                count += 1;
            } else if !old_filt_read && new_filt_read {
                k_ev[count] = kev(event.fd, libc::EVFILT_READ, libc::EV_ADD, event.pos);
                count += 1;
            }
            if old_filt_write && !new_filt_write {
                k_ev[count] = kev(event.fd, libc::EVFILT_WRITE, libc::EV_DELETE, event.pos);
                count += 1;
            } else if !old_filt_write && new_filt_write {
                k_ev[count] = kev(event.fd, libc::EVFILT_WRITE, libc::EV_ADD, event.pos);
                count += 1;
            }
        }

        if count == 0 {
            return Ok(());
        }
        // SAFETY: `count` initialized changelist entries, no output buffer.
        let rc = unsafe {
            libc::kevent(
                self.kqueue_fd,
                k_ev.as_ptr(),
                count as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if rc < 0 {
            return Err(os_error(ERROR, "kevent() failed"));
        }
        Ok(())
    }
}

pub(crate) fn wait_block(
    set: &mut WaitEventSetData,
    latch: Option<&'static Latch>,
    cur_timeout: i64,
    occurred_events: &mut [WaitEvent],
) -> PgResult<i32> {
    let max_events = occurred_events.len().min(set.nevents_space.max(0) as usize);
    let events = &set.events;
    let backend = &mut set.backend;

    let mut timeout = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let timeout_p: *const libc::timespec = if cur_timeout < 0 {
        std::ptr::null()
    } else {
        timeout.tv_sec = (cur_timeout / 1000) as libc::time_t;
        timeout.tv_nsec = ((cur_timeout % 1000) * 1_000_000) as libc::c_long;
        &timeout
    };

    debug_assert!(backend.ret_events.capacity() >= max_events);
    // SAFETY: kevent writes <= max_events entries into reserved capacity; only the returned prefix is read.
    let rc = unsafe {
        libc::kevent(
            backend.kqueue_fd,
            std::ptr::null(),
            0,
            backend.ret_events.as_mut_ptr(),
            max_events as i32,
            timeout_p,
        )
    };

    if rc < 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::EINTR {
            return Ok(0);
        }
        return Err(os_error(ERROR, "kevent() failed"));
    }
    if rc == 0 {
        return Ok(-1);
    }

    // SAFETY: the kernel initialized the first `rc` entries.
    let returned = unsafe { std::slice::from_raw_parts(backend.ret_events.as_ptr(), rc as usize) };

    let mut returned_events = 0usize;
    for k_ev in returned {
        if returned_events == occurred_events.len() {
            break;
        }
        let cur_event = &events[k_ev.udata as usize];
        let mut out = WaitEvent {
            pos: cur_event.pos,
            user_data: cur_event.user_data,
            events: 0,
            fd: PGINVALID_SOCKET,
        };

        if cur_event.events == WL_LATCH_SET {
            drain()?;
            if let Some(l) = latch {
                if l.maybe_sleeping.load(SeqCst) != 0 && l.is_set() {
                    out.events = WL_LATCH_SET;
                }
            }
        } else if cur_event.events == WL_POSTMASTER_DEATH {
            unreachable!("postmaster death watch is not ported");
        } else if cur_event.events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED)
            != 0
        {
            debug_assert!(cur_event.fd >= 0);
            if cur_event.events & WL_SOCKET_READABLE != 0 && k_ev.filter == libc::EVFILT_READ {
                out.events |= WL_SOCKET_READABLE;
            }
            if cur_event.events & WL_SOCKET_CLOSED != 0
                && k_ev.filter == libc::EVFILT_READ
                && k_ev.flags & libc::EV_EOF != 0
            {
                out.events |= WL_SOCKET_CLOSED;
            }
            if cur_event.events & WL_SOCKET_WRITEABLE != 0 && k_ev.filter == libc::EVFILT_WRITE {
                out.events |= WL_SOCKET_WRITEABLE;
            }
            if out.events != 0 {
                out.fd = cur_event.fd;
            }
        }

        if out.events != 0 {
            occurred_events[returned_events] = out;
            returned_events += 1;
        }
    }

    Ok(returned_events as i32)
}
