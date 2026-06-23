use core::ffi::{c_int, c_void};

pub type pgsocket = c_int;

// On wasm (`target_family = "wasm"`) `libc` exports neither `sockaddr_storage`
// nor `socklen_t` (no BSD sockets on wasip1). Single-user wasm has no listener
// and never populates a real `SockAddr`, but the struct is part of the
// `ClientSocket`/`Port` ABI, so we provide layout-faithful local definitions
// matching the native glibc sizes pgrust's ABI assumes (128-byte storage,
// 4-byte socklen_t).
#[cfg(target_family = "wasm")]
pub type socklen_t = u32;
#[cfg(target_family = "wasm")]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct sockaddr_storage {
    pub ss_family: u16,
    pub __ss_padding: [u8; 126],
}

#[cfg(not(target_family = "wasm"))]
use libc::{sockaddr_storage, socklen_t};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SockAddr {
    pub addr: sockaddr_storage,
    pub salen: socklen_t,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ClientSocket {
    pub sock: pgsocket,
    pub raddr: SockAddr,
}

pub type Port = c_void;

pub type sig_atomic_t = c_int;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Latch {
    pub is_set: sig_atomic_t,
    pub maybe_sleeping: sig_atomic_t,
    pub is_shared: bool,
    pub owner_pid: c_int,
}
