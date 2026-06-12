//! `libpq/pqcomm.h` socket types.

/// `pgsocket` — a socket descriptor (`int` on Unix).
pub type pgsocket = i32;

/// `SockAddr` (`libpq/pqcomm.h`).
///
/// `addr` mirrors the platform `struct sockaddr_storage`, a fixed-size
/// socket-address buffer (`_SS_MAXSIZE` == 128 bytes); `salen` mirrors the
/// platform `socklen_t`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SockAddr {
    pub addr: [u8; 128],
    pub salen: u32,
}

/// `ClientSocket` (`libpq/pqcomm.h`) — an accepted client connection: the
/// socket plus the client's remote address.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClientSocket {
    pub sock: pgsocket,
    pub raddr: SockAddr,
}
