//! `libpq/pqcomm.h` socket types.

/// `pgsocket` — a socket descriptor (`int` on Unix).
pub type pgsocket = i32;

/// `SockAddr` (`libpq/pqcomm.h`): `struct sockaddr_storage addr; socklen_t
/// salen;` — a socket address of any family, with its actual length.
#[derive(Copy, Clone)]
pub struct SockAddr {
    pub addr: libc::sockaddr_storage,
    pub salen: libc::socklen_t,
}

/// `ClientSocket` (`libpq/pqcomm.h`) — an accepted client connection: the
/// socket plus the client's remote address.
#[derive(Copy, Clone)]
pub struct ClientSocket {
    pub sock: pgsocket,
    pub raddr: SockAddr,
}
