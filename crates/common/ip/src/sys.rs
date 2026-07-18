//! libc socket/netdb surface indirection for the wasm32 arm.
//!
//! native: verbatim re-exports of the platform libc crate's items — callers
//! (`pqcomm`, `hba`) compile to exactly the code they compiled to before.
//!
//! wasm32: WASI p1 has no server-socket surface (no socket/bind/listen, no
//! resolver, no AF_UNIX) and the wasi libc crate exposes none of these
//! names. Constants take musl's values (matching the linked wasi-libc's
//! numbering convention), types are musl-layout twins (only zeroed, copied,
//! and size-of'd on this target), and every call stub fails with ENOSYS.
//! The doc §2.4 transport byte seam is the structural fix; W0 --single never
//! listens or accepts, so these stubs are boot-unreachable on wasm.

#[cfg(not(target_family = "wasm"))]
pub use libc::{
    accept, bind, chown, gai_strerror, getgrnam, getsockname, getsockopt, group, listen,
    sa_family_t, setsockopt, sockaddr, sockaddr_in, sockaddr_in6, sockaddr_un, socket, socklen_t,
    AF_INET, AF_INET6, AF_UNIX, AF_UNSPEC, AI_NUMERICHOST, AI_PASSIVE, EAI_NONAME, IPPROTO_IPV6,
    IPPROTO_TCP, IPV6_V6ONLY, NI_NAMEREQD, NI_NUMERICHOST, NI_NUMERICSERV, SOCK_STREAM, SOL_SOCKET, SO_KEEPALIVE,
    SO_REUSEADDR, TCP_KEEPCNT, TCP_KEEPINTVL, TCP_NODELAY,
};
// macOS spells the idle-probe option TCP_KEEPALIVE; consumers cfg that arm
// themselves (pqcomm PG_TCP_KEEPALIVE_IDLE), so only re-export where it exists.
#[cfg(not(any(target_family = "wasm", target_os = "macos", target_os = "ios")))]
pub use libc::TCP_KEEPIDLE;

#[cfg(target_family = "wasm")]
mod wasm_sys {
    #![allow(non_camel_case_types)]
    use core::ffi::{c_char, c_int, c_void};

    // musl socket.h / netinet / netdb values.
    pub const AF_UNSPEC: c_int = 0;
    pub const AF_UNIX: c_int = 1;
    pub const AF_INET: c_int = 2;
    pub const AF_INET6: c_int = 10;
    pub const SOCK_STREAM: c_int = 1;
    pub const SOL_SOCKET: c_int = 1;
    pub const SO_REUSEADDR: c_int = 2;
    pub const SO_KEEPALIVE: c_int = 9;
    pub const IPPROTO_TCP: c_int = 6;
    pub const IPPROTO_IPV6: c_int = 41;
    pub const IPV6_V6ONLY: c_int = 26;
    pub const TCP_NODELAY: c_int = 1;
    pub const TCP_KEEPIDLE: c_int = 4;
    pub const TCP_KEEPINTVL: c_int = 5;
    pub const TCP_KEEPCNT: c_int = 6;
    pub const AI_PASSIVE: c_int = 0x01;
    pub const AI_NUMERICHOST: c_int = 0x04;
    pub const NI_NUMERICHOST: c_int = 0x01;
    pub const NI_NUMERICSERV: c_int = 0x02;
    pub const NI_NAMEREQD: c_int = 0x08;
    pub const EAI_NONAME: c_int = -2;

    pub type sa_family_t = u16;
    pub type socklen_t = u32;

    #[repr(C)]
    pub struct sockaddr {
        pub sa_family: sa_family_t,
        pub sa_data: [c_char; 14],
    }
    #[repr(C)]
    pub struct sockaddr_un {
        pub sun_family: sa_family_t,
        pub sun_path: [c_char; 108],
    }
    #[repr(C)]
    pub struct in_addr {
        pub s_addr: u32,
    }
    #[repr(C)]
    pub struct sockaddr_in {
        pub sin_family: sa_family_t,
        pub sin_port: u16,
        pub sin_addr: in_addr,
        pub sin_zero: [u8; 8],
    }
    #[repr(C)]
    pub struct in6_addr {
        pub s6_addr: [u8; 16],
    }
    #[repr(C)]
    pub struct sockaddr_in6 {
        pub sin6_family: sa_family_t,
        pub sin6_port: u16,
        pub sin6_flowinfo: u32,
        pub sin6_addr: in6_addr,
        pub sin6_scope_id: u32,
    }
    #[repr(C)]
    pub struct group {
        pub gr_gid: u32,
    }

    // wasi-libc keeps musl's __errno_location; the libc crate binds ENOSYS
    // with WASI's own numbering, which is what wasi-libc reports too.
    extern "C" {
        fn __errno_location() -> *mut c_int;
    }
    unsafe fn enosys() -> c_int {
        unsafe { *__errno_location() = libc::ENOSYS };
        -1
    }

    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn socket(_domain: c_int, _ty: c_int, _protocol: c_int) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn bind(_fd: c_int, _addr: *const sockaddr, _len: socklen_t) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn listen(_fd: c_int, _backlog: c_int) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn accept(_fd: c_int, _addr: *mut sockaddr, _len: *mut socklen_t) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn setsockopt(
        _fd: c_int,
        _level: c_int,
        _name: c_int,
        _value: *const c_void,
        _len: socklen_t,
    ) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn getsockopt(
        _fd: c_int,
        _level: c_int,
        _name: c_int,
        _value: *mut c_void,
        _len: *mut socklen_t,
    ) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn getsockname(_fd: c_int, _addr: *mut sockaddr, _len: *mut socklen_t) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments.
    pub unsafe fn chown(_path: *const c_char, _uid: u32, _gid: u32) -> c_int {
        unsafe { enosys() }
    }
    /// # Safety
    /// No-op stub; never dereferences its arguments. NULL = "no such group".
    pub unsafe fn getgrnam(_name: *const c_char) -> *mut group {
        unsafe {
            *__errno_location() = libc::ENOSYS;
        }
        core::ptr::null_mut()
    }
    /// # Safety
    /// Returns a static NUL-terminated message; always valid.
    pub unsafe fn gai_strerror(_err: c_int) -> *const c_char {
        c"name resolution is not supported on this platform".as_ptr()
    }
}

#[cfg(target_family = "wasm")]
pub use wasm_sys::*;
