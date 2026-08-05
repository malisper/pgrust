use std::sync::atomic::Ordering::SeqCst;

use types_storage::waiteventset::{WL_SOCKET_CLOSED, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};

use crate::{drain, os_error, Latch, PgResult, WaitEvent, WaitEventSetData, ERROR};
use types_core::PGINVALID_SOCKET;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

pub(crate) struct BackendSet {
    epoll_fd: i32,
    ret_events: Vec<libc::epoll_event>,
}

impl BackendSet {
    pub(crate) fn create(nevents: i32) -> PgResult<Self> {
        if !fd::AcquireExternalFD()? {
            return Err(os_error(ERROR, "AcquireExternalFD, for epoll_create1, failed"));
        }
        // SAFETY: epoll_create1(2).
        let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epfd < 0 {
            fd::ReleaseExternalFD();
            return Err(os_error(ERROR, "epoll_create1 failed"));
        }
        Ok(BackendSet {
            epoll_fd: epfd,
            ret_events: Vec::with_capacity(nevents.max(0) as usize),
        })
    }

    pub(crate) fn free(&self) {
        // SAFETY: closing our epoll fd.
        unsafe { libc::close(self.epoll_fd) };
        fd::ReleaseExternalFD();
    }

    // WaitEventAdjustEpoll; old_events 0 = CTL_ADD, else CTL_MOD.
    pub(crate) fn register(&self, event: &WaitEvent, old_events: u32) -> PgResult<()> {
        let mut mask = libc::EPOLLERR | libc::EPOLLHUP;

        if event.events == WL_LATCH_SET {
            // The wakeup pipe read end stands in for C's signalfd.
            mask |= libc::EPOLLIN;
        } else if event.events == WL_POSTMASTER_DEATH {
            unreachable!("postmaster death watch is not ported");
        } else {
            debug_assert!(event.fd != PGINVALID_SOCKET);
            if event.events & WL_SOCKET_READABLE != 0 {
                mask |= libc::EPOLLIN;
            }
            if event.events & WL_SOCKET_WRITEABLE != 0 {
                mask |= libc::EPOLLOUT;
            }
            if event.events & WL_SOCKET_CLOSED != 0 {
                mask |= libc::EPOLLRDHUP;
            }
        }

        let mut epoll_ev = libc::epoll_event {
            events: mask as u32,
            // C's data.ptr WaitEvent pointer -> stable position.
            u64: event.pos as u64,
        };
        let action = if old_events == 0 {
            libc::EPOLL_CTL_ADD
        } else {
            libc::EPOLL_CTL_MOD
        };
        // SAFETY: live epoll fd; epoll_ev outlives the call.
        let rc = unsafe { libc::epoll_ctl(self.epoll_fd, action, event.fd, &mut epoll_ev) };
        if rc < 0 {
            return Err(os_error(ERROR, "epoll_ctl() failed"));
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

    debug_assert!(backend.ret_events.capacity() >= max_events);
    // SAFETY: epoll_wait writes <= max_events entries into reserved capacity; only the returned prefix is read.
    let rc = unsafe {
        libc::epoll_wait(
            backend.epoll_fd,
            backend.ret_events.as_mut_ptr(),
            max_events as i32,
            cur_timeout as i32,
        )
    };

    if rc < 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::EINTR {
            return Ok(0);
        }
        return Err(os_error(ERROR, "epoll_wait() failed"));
    }
    if rc == 0 {
        return Ok(-1);
    }

    // SAFETY: the kernel initialized the first `rc` entries.
    let returned = unsafe { std::slice::from_raw_parts(backend.ret_events.as_ptr(), rc as usize) };

    let err_mask = (libc::EPOLLERR | libc::EPOLLHUP) as u32;
    let mut returned_events = 0usize;
    for epoll_ev in returned {
        if returned_events == occurred_events.len() {
            break;
        }
        let cur_event = &events[epoll_ev.u64 as usize];
        let mut out = WaitEvent {
            pos: cur_event.pos,
            user_data: cur_event.user_data,
            events: 0,
            fd: PGINVALID_SOCKET,
        };

        if cur_event.events == WL_LATCH_SET
            && epoll_ev.events & (libc::EPOLLIN as u32 | err_mask) != 0
        {
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
            debug_assert!(cur_event.fd != PGINVALID_SOCKET);
            if cur_event.events & WL_SOCKET_READABLE != 0
                && epoll_ev.events & (libc::EPOLLIN as u32 | err_mask) != 0
            {
                out.events |= WL_SOCKET_READABLE;
            }
            if cur_event.events & WL_SOCKET_WRITEABLE != 0
                && epoll_ev.events & (libc::EPOLLOUT as u32 | err_mask) != 0
            {
                out.events |= WL_SOCKET_WRITEABLE;
            }
            if cur_event.events & WL_SOCKET_CLOSED != 0
                && epoll_ev.events & (libc::EPOLLRDHUP as u32 | err_mask) != 0
            {
                out.events |= WL_SOCKET_CLOSED;
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
