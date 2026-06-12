//! `SockAddr` / `ClientSocket` / `Port` (libpq/pqcomm.h, libpq/libpq-be.h)
//! and the owned `addrinfo` shapes for the `common/ip.c` seam surface.
//!
//! `Port` is trimmed to the fields the ported units consume (pqcomm.c:
//! socket fd, blocking mode, addresses, TCP keepalive bookkeeping). Later
//! ports (be-secure, auth, hba) extend it with their fields as they land.

pub use types_core::{pgsocket, PGINVALID_SOCKET};

/// `SockAddr` (libpq/pqcomm.h): a platform `struct sockaddr_storage` byte
/// buffer (`_SS_MAXSIZE` == 128 bytes) plus its `socklen_t` length.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SockAddr {
    pub addr: [u8; 128],
    pub salen: u32,
}

impl SockAddr {
    pub const fn zeroed() -> Self {
        SockAddr { addr: [0; 128], salen: 0 }
    }
}

impl Default for SockAddr {
    fn default() -> Self {
        Self::zeroed()
    }
}

/// `ClientSocket` (libpq/libpq-be.h): an accepted connection's fd and remote
/// address, filled by `AcceptConnection` and handed to `pq_init`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClientSocket {
    pub sock: pgsocket,
    pub raddr: SockAddr,
}

impl Default for ClientSocket {
    fn default() -> Self {
        ClientSocket { sock: PGINVALID_SOCKET, raddr: SockAddr::zeroed() }
    }
}

/// `struct Port` (libpq/libpq-be.h), trimmed: per-connection state created by
/// `pq_init` and stored as the backend's `MyProcPort`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Port {
    /// File descriptor.
    pub sock: pgsocket,
    /// Is the socket in non-blocking mode?
    pub noblock: bool,
    /// Local addr (postmaster).
    pub laddr: SockAddr,
    /// Remote addr (client).
    pub raddr: SockAddr,

    /// TCP keepalive and user-timeout settings: the kernel defaults probed on
    /// first read (`0` = not probed yet, `-1` = unknown) and the values
    /// currently in effect (`0` = kernel default).
    pub default_keepalives_idle: i32,
    pub default_keepalives_interval: i32,
    pub default_keepalives_count: i32,
    pub default_tcp_user_timeout: i32,
    pub keepalives_idle: i32,
    pub keepalives_interval: i32,
    pub keepalives_count: i32,
    pub tcp_user_timeout: i32,
}

impl Port {
    /// The `palloc0(sizeof(Port))` template: everything zeroed.
    pub const fn zeroed() -> Self {
        Port {
            sock: 0,
            noblock: false,
            laddr: SockAddr::zeroed(),
            raddr: SockAddr::zeroed(),
            default_keepalives_idle: 0,
            default_keepalives_interval: 0,
            default_keepalives_count: 0,
            default_tcp_user_timeout: 0,
            keepalives_idle: 0,
            keepalives_interval: 0,
            keepalives_count: 0,
            tcp_user_timeout: 0,
        }
    }
}

impl Default for Port {
    fn default() -> Self {
        Self::zeroed()
    }
}

/// The lookup-relevant fields of the `struct addrinfo` hint passed to
/// `pg_getaddrinfo_all` (common/ip.h).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct AddrInfoHint {
    pub flags: i32,
    pub family: i32,
    pub socktype: i32,
}

/// One owned `struct addrinfo` result node from `pg_getaddrinfo_all`
/// (the list is a `Vec<PgAddrInfo>`; dropping it is `pg_freeaddrinfo_all`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PgAddrInfo {
    pub flags: i32,
    pub family: i32,
    pub socktype: i32,
    pub protocol: i32,
    pub addr: SockAddr,
}
