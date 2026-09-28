// WASIX twin of the epoll slice of `libc` that `epoll.rs` names, imported by
// that file as `use crate::wasm_epoll as libc;`.
//
// The libc crate's `target_os = "wasi"` module is shared with wasip1 and
// declares no epoll surface (its socket/netdb/epoll bindings live only in
// `wasi/p2.rs`), so the handful of items the Linux backend uses is re-declared
// here against wasix-libc, which carries musl's `src/linux/epoll.c` over the
// `__wasi_epoll_*` syscalls. Values are from wasix-libc's
// `musl/include/sys/epoll.h` and `headers/public/wasi/api_wasix.h`.
//
// `close` and `EINTR` come from the libc crate unchanged: wasix-libc numbers
// errno with the WASI table (`__errno_values.h`: EINTR = __WASI_ERRNO_INTR =
// 27, EAGAIN = 6), which is what the libc crate's wasi module declares too.
#![allow(non_camel_case_types, dead_code)]

use core::ffi::c_int;

// musl's `struct epoll_event` is `__attribute__((__packed__))` only under
// `#ifdef __x86_64__`, so on wasm32 the union member is aligned to 8: size 16,
// the data view at offset 8 — exactly this layout.
//
// Only the 4-byte pointer view is written back by WASIX's `epoll_pwait` (see
// the tag bias in `epoll.rs`), so the high half of `u64` keeps whatever the
// caller's buffer held; `epoll.rs` truncates it with `as usize` on a 32-bit
// target, and the bias guarantees the low half is the tag.
#[repr(C)]
pub struct epoll_event {
    pub events: u32,
    pub u64: u64,
}

pub const EPOLLIN: c_int = 0x001;
pub const EPOLLOUT: c_int = 0x004;
pub const EPOLLERR: c_int = 0x008;
pub const EPOLLHUP: c_int = 0x010;
pub const EPOLLRDHUP: c_int = 0x2000;

pub const EPOLL_CLOEXEC: c_int = 0x8000; // == O_CLOEXEC; ignored by wasix-libc
pub const EPOLL_CTL_ADD: c_int = 1;
pub const EPOLL_CTL_MOD: c_int = 3;

extern "C" {
    pub fn epoll_create1(flags: c_int) -> c_int;
    pub fn epoll_wait(
        epfd: c_int,
        events: *mut epoll_event,
        maxevents: c_int,
        timeout: c_int,
    ) -> c_int;
    #[link_name = "epoll_ctl"]
    fn raw_epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut epoll_event) -> c_int;
    fn __errno_location() -> *mut c_int;
}

/// wasix-libc's `epoll_ctl` returns the raw `__wasi_errno_t` instead of
/// keeping C's `-1` + `errno` contract, so a failed registration would read
/// back as success (and a positive errno as an event count). Normalized here.
///
/// # Safety
/// Same contract as `libc::epoll_ctl`.
pub unsafe fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut epoll_event) -> c_int {
    // SAFETY: the caller upholds libc::epoll_ctl's fd/pointer contract.
    let rc = unsafe { raw_epoll_ctl(epfd, op, fd, event) };
    if rc == 0 {
        return 0;
    }
    // SAFETY: __errno_location(3) always returns a live slot.
    unsafe { *__errno_location() = rc };
    -1
}

pub use ::libc::{close, EINTR};
