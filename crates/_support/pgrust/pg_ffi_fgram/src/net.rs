use core::ffi::{c_int, c_void};

pub type pgsocket = c_int;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SockAddr {
    pub addr: libc::sockaddr_storage,
    pub salen: libc::socklen_t,
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
