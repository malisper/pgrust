//! Vocabulary types for `utils/adt/network.c` (the `inet`/`cidr` datatypes) and
//! the `macaddr`/`macaddr8` scalar-conversion helpers it shares.
//!
//! These mirror the on-disk / Datum C declarations in `src/include/utils/inet.h`.
//! The varlena envelope (`vl_len_`, `SET_INET_VARSIZE`, `PG_GETARG_INET_PP`) is
//! the project-wide fmgr/Datum deferral and is not modeled here; the in-memory
//! payload [`inet_struct`] is what the ported logic operates on.
//!
//! [`ResolvedName`]/[`SessionEndpoint`] are the seam-boundary value types used by
//! `inet_client_addr` / `inet_server_addr` to read the not-yet-ported
//! `MyProcPort` / `pg_getnameinfo_all` result without crossing an ambient-global
//! seam.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::vec::Vec;

// ---------------------------------------------------------------------------
// inet / cidr  (utils/inet.h)
// ---------------------------------------------------------------------------

/// Family field values for [`inet_struct`]. `PGSQL_AF_INET` is `AF_INET + 0`
/// and `PGSQL_AF_INET6` is `AF_INET + 1` (utils/inet.h); the on-disk values are
/// fixed at `2` / `3` for cross-platform stability.
pub const PGSQL_AF_INET: u8 = 2;
pub const PGSQL_AF_INET6: u8 = 2 + 1;

/// Internal storage format for IP addresses (both INET and CIDR). (utils/inet.h)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct inet_struct {
    /// `PGSQL_AF_INET` or `PGSQL_AF_INET6`.
    pub family: u8,
    /// Number of bits in netmask.
    pub bits: u8,
    /// Up to 128 bits of address.
    pub ipaddr: [u8; 16],
}

// ---------------------------------------------------------------------------
// macaddr / macaddr8  (utils/inet.h)
// ---------------------------------------------------------------------------

/// Internal storage format for MAC addresses (fixed-length pass-by-reference). (utils/inet.h)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct macaddr {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
}

/// Internal storage format for MAC8 addresses (fixed-length pass-by-reference). (utils/inet.h)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct macaddr8 {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
    pub g: u8,
    pub h: u8,
}

// ---------------------------------------------------------------------------
// inet_client_addr / inet_server_addr seam-boundary value types
// ---------------------------------------------------------------------------

/// The numeric host/port string resolved from a socket address, mirroring the
/// result of `pg_getnameinfo_all(... NI_NUMERICHOST | NI_NUMERICSERV)`
/// (libpq-be / `MyProcPort`).
///
/// `network.c`'s `inet_client_addr` / `inet_server_addr` call
/// `clean_ipv6_addr(addr.ss_family, host)` on the raw getnameinfo output before
/// feeding it to `network_in`; [`family`](ResolvedName::family) exposes the
/// socket's `ss_family` so the port can drive `clean_ipv6_addr` in-crate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResolvedName {
    /// Raw numeric host string from `getnameinfo` (still possibly carrying a
    /// `%zone` suffix for IPv6), a NUL-free byte string.
    pub host: Vec<u8>,
    /// Numeric port string, a NUL-free byte string (fed to `int4in`).
    pub port: Vec<u8>,
    /// Socket address family (`addr.ss_family`): the *system* `AF_INET` /
    /// `AF_INET6` value, used to drive `clean_ipv6_addr`.
    pub family: i32,
}

/// Which session endpoint to resolve for `inet_{client,server}_{addr,port}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEndpoint {
    /// C: `MyProcPort->raddr` — the remote (client) address.
    Client,
    /// C: `MyProcPort->laddr` — the local (server) address.
    Server,
}
