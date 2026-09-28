//! libc socket/netdb surface indirection for the wasm32 arms.
//!
//! native: verbatim re-exports of the platform libc crate's items — callers
//! (`pqcomm`, `hba`) compile to exactly the code they compiled to before.
//!
//! wasm32-wasip1: WASI p1 has no server-socket surface (no socket/bind/listen,
//! no resolver, no AF_UNIX) and the wasi libc crate exposes none of these
//! names. Constants take the linked libc's values (see wasm_net below), types
//! are its layout twins (only zeroed, copied, and size-of'd on this target),
//! and every call stub fails with ENOSYS.
//! The doc §2.4 transport byte seam is the structural fix; W0 --single never
//! listens or accepts, so these stubs are boot-unreachable on wasm.
//!
//! wasm32-wasmer-wasi (WASIX): the linked wasi-libc *does* provide the BSD
//! socket and netdb surface — `llvm-nm --defined-only` on the sysroot's
//! `lib/rustlib/wasm32-wasmer-wasi/lib/self-contained/libc.a` shows
//! `socket`, `bind`, `listen`, `accept`, `setsockopt`, `getsockopt`,
//! `getsockname`, `getaddrinfo` and `freeaddrinfo` all defined (they lower to
//! the `wasix_32v1` `sock_*` calls). What is missing is only the Rust-side
//! declaration: the `libc` crate's `target_os = "wasi"` module declares no
//! socket/netdb item at all, because that module is shared with wasip1. So
//! the externs are declared here, against the same ABI the constants and type
//! twins below mirror — wasix-libc's, which is musl-derived but NOT
//! musl-valued for the address families.
//!
//! AF_UNIX is the one surface left stubbed on WASIX: it has no WASIX call
//! (WASIX sockets are inet/inet6), so `chown`/`getgrnam` — reachable only
//! through `Setup_AF_UNIX` — keep the ENOSYS posture.

#[cfg(not(target_family = "wasm"))]
pub use libc::{
    accept, addrinfo, bind, chown, freeaddrinfo, gai_strerror, getaddrinfo, getgrnam, getsockname,
    getsockopt, group, listen, sa_family_t, setsockopt, sockaddr, sockaddr_in, sockaddr_in6,
    sockaddr_un, socket, socklen_t, AF_INET, AF_INET6, AF_UNIX, AF_UNSPEC, AI_NUMERICHOST,
    AI_PASSIVE, EAI_FAIL, EAI_NONAME, IPPROTO_IPV6, IPPROTO_TCP, IPV6_V6ONLY, NI_NAMEREQD,
    NI_NUMERICHOST, NI_NUMERICSERV, SOCK_STREAM, SOL_SOCKET, SO_KEEPALIVE, SO_REUSEADDR,
    TCP_KEEPCNT, TCP_KEEPINTVL, TCP_NODELAY,
};
// macOS spells the idle-probe option TCP_KEEPALIVE; consumers cfg that arm
// themselves (pqcomm PG_TCP_KEEPALIVE_IDLE), so only re-export where it exists.
#[cfg(not(any(target_family = "wasm", target_os = "macos", target_os = "ios")))]
pub use libc::TCP_KEEPIDLE;

// Shared by both wasm arms: constants and structs in the linked libc's own
// spelling. Values are checked against wasix-libc's wasm32 headers, NOT the
// generic musl ones (which are superseded for this target):
//   libc-bottom-half/headers/public/__header_sys_socket.h -> AF_*, SOCK_STREAM,
//     SOL_SOCKET, SO_* (each an alias for a __WASI_* enumerator in
//     libc-bottom-half/headers/public/wasi/api_wasix.h: INET4 1, INET6 2,
//     UNIX 3, SOCK_OPTION_REUSE_ADDR 2, _KEEP_ALIVE 12, SOL_SOCKET 0x7fffffff)
//   libc-bottom-half/headers/public/__header_netinet_in.h -> IPPROTO_TCP 6,
//     IPPROTO_IPV6 41, IPV6_V6ONLY 26, TCP_NODELAY 1, TCP_KEEPIDLE 4
//   libc-top-half/musl/include/netdb.h -> AI_*, NI_*, EAI_*
// Two of those families are load-bearing beyond the numbers: wasix numbers
// AF_INET as 1 (musl says 2), so a Linux table makes getaddrinfo's own IPv4
// result look like an AF_UNIX address, and setsockopt rewrites only
// (IPPROTO_IPV6,IPV6_V6ONLY) and (IPPROTO_TCP,TCP_NODELAY) into SOL_SOCKET
// (cloudlibc/src/libc/sys/socket/setsockopt.c) and answers ENOSYS for anything
// else, so TCP_KEEPIDLE/KEEPINTVL/KEEPCNT are declared-but-unsupported here.
// The wasip1 stubs never consult these values, so the sharing is safe; only
// the WASIX lane can observe a wrong one.
#[cfg(target_family = "wasm")]
mod wasm_net {
    #![allow(non_camel_case_types)]
    use core::ffi::{c_char, c_int};

    pub const AF_UNSPEC: c_int = 0;
    pub const AF_UNIX: c_int = 3;
    pub const AF_INET: c_int = 1;
    pub const AF_INET6: c_int = 2;
    pub const SOCK_STREAM: c_int = 1;
    pub const SOL_SOCKET: c_int = 0x7fff_ffff;
    pub const SO_REUSEADDR: c_int = 2;
    pub const SO_KEEPALIVE: c_int = 12;
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
    pub const EAI_FAIL: c_int = -4;

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
}

// wasip1 (and any other wasm32 without a socket ABI): every call stub fails.
#[cfg(all(target_family = "wasm", not(target_vendor = "wasmer")))]
mod wasm_sys {
    use super::wasm_net::*;
    use core::ffi::{c_char, c_int, c_void};

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

// WASIX (wasm32-wasmer-wasi): the sysroot's wasi-libc exports the real BSD
// socket + netdb surface, so these are declarations, not stubs. Signatures are
// musl's; the ABI twins above are the same ones wasip1 already carries.
#[cfg(all(target_family = "wasm", target_vendor = "wasmer"))]
mod wasix_sys {
    use super::wasm_net::*;
    use core::ffi::{c_char, c_int, c_void};

    // musl netdb.h, in field order.
    #[repr(C)]
    pub struct addrinfo {
        pub ai_flags: c_int,
        pub ai_family: c_int,
        pub ai_socktype: c_int,
        pub ai_protocol: c_int,
        pub ai_addrlen: socklen_t,
        pub ai_addr: *mut sockaddr,
        pub ai_canonname: *mut c_char,
        pub ai_next: *mut addrinfo,
    }

    extern "C" {
        pub fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
        pub fn bind(fd: c_int, addr: *const sockaddr, len: socklen_t) -> c_int;
        pub fn listen(fd: c_int, backlog: c_int) -> c_int;
        pub fn accept(fd: c_int, addr: *mut sockaddr, len: *mut socklen_t) -> c_int;
        pub fn setsockopt(
            fd: c_int,
            level: c_int,
            name: c_int,
            value: *const c_void,
            len: socklen_t,
        ) -> c_int;
        pub fn getsockopt(
            fd: c_int,
            level: c_int,
            name: c_int,
            value: *mut c_void,
            len: *mut socklen_t,
        ) -> c_int;
        pub fn getsockname(fd: c_int, addr: *mut sockaddr, len: *mut socklen_t) -> c_int;
        pub fn getaddrinfo(
            host: *const c_char,
            serv: *const c_char,
            hints: *const addrinfo,
            res: *mut *mut addrinfo,
        ) -> c_int;
        pub fn freeaddrinfo(res: *mut addrinfo);
        pub fn gai_strerror(err: c_int) -> *const c_char;
    }

    extern "C" {
        fn __errno_location() -> *mut c_int;
    }
    unsafe fn enosys() -> c_int {
        unsafe { *__errno_location() = libc::ENOSYS };
        -1
    }

    /// # Safety
    /// No-op stub; never dereferences its arguments. WASIX sockets are
    /// inet/inet6 only, so AF_UNIX's file-ownership path stays unreachable.
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
}

// The musl twins are part of the surface callers use (`sys::sockaddr`,
// `sys::socklen_t`, `sys::AF_INET`), so they are re-exported on every wasm arm.
#[cfg(all(target_family = "wasm", target_vendor = "wasmer"))]
pub use wasix_sys::*;
#[cfg(target_family = "wasm")]
pub use wasm_net::*;
#[cfg(all(target_family = "wasm", not(target_vendor = "wasmer")))]
pub use wasm_sys::*;
