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

impl inet_struct {
    /// Encode the `inet` payload into by-reference `Datum` bytes: family, bits,
    /// then 16 address bytes (the `inet_struct` image; the varlena header is the
    /// fmgr boundary's concern).
    pub fn to_datum_bytes(&self) -> [u8; 18] {
        let mut out = [0u8; 18];
        out[0] = self.family;
        out[1] = self.bits;
        out[2..18].copy_from_slice(&self.ipaddr);
        out
    }

    /// Decode an `inet` payload from by-reference `Datum` bytes.
    pub fn from_datum_bytes(b: &[u8]) -> inet_struct {
        let mut ipaddr = [0u8; 16];
        ipaddr.copy_from_slice(&b[2..18]);
        inet_struct {
            family: b[0],
            bits: b[1],
            ipaddr,
        }
    }
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

// ---------------------------------------------------------------------------
// GiST inet_ops opclass key (network_gist.c)
// ---------------------------------------------------------------------------

use ::types_core::primitive::OffsetNumber;

/// `GistInetKey` (network_gist.c:79) — a GiST INET/CIDR index key.
///
/// Not identical to INET/CIDR because it tracks the length of the common
/// address prefix (`commonbits`) as well as the minimum netmask length
/// (`minbits`). In C this is a 1-byte-header varlena; the varlena envelope is
/// the fmgr/Datum boundary's concern, so this payload struct carries only the
/// fields. `family` of zero denotes a multiple-family union.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GistInetKey {
    /// `family` — `PGSQL_AF_INET`, `PGSQL_AF_INET6`, or zero.
    pub family: u8,
    /// `minbits` — minimum number of bits in netmask.
    pub minbits: u8,
    /// `commonbits` — number of common prefix bits in addresses.
    pub commonbits: u8,
    /// `ipaddr` — up to 128 bits of common address.
    pub ipaddr: [u8; 16],
}

impl GistInetKey {
    /// Encode the key into the bytes carried by a by-reference `Datum`
    /// (`InetKeyPGetDatum`).
    ///
    /// The C `GistInetKey` is a varlena whose struct layout is
    /// `{ char vl_len_[4]; uint8 family; uint8 minbits; uint8 commonbits;
    /// uint8 ipaddr[16]; }` — i.e. the payload fields begin at byte offset 4,
    /// *after* the 4-byte length word, and `gk_ip_family`/`gk_ip_addr` read the
    /// struct at those fixed offsets (they never use `VARDATA_ANY`). So this
    /// emits a full 4-byte-header varlena image (`SET_VARSIZE`) holding the whole
    /// 16-byte address; `index_form_tuple` copies the varlena verbatim onto the
    /// page, and `from_datum_bytes` reads the fields back at the same offsets.
    pub fn to_datum_bytes(&self) -> [u8; 23] {
        let mut out = [0u8; 23];
        // SET_VARSIZE(out, 23): the 4-byte length word (low two bits are the
        // uncompressed/long-header flags = 0). 23 << 2 = 92 fits in byte 0.
        let varsize: u32 = 23;
        out[..4].copy_from_slice(&(varsize << 2).to_le_bytes());
        out[4] = self.family;
        out[5] = self.minbits;
        out[6] = self.commonbits;
        out[7..23].copy_from_slice(&self.ipaddr);
        out
    }

    /// Decode a key from by-reference `Datum` bytes (`DatumGetInetKeyP`). The
    /// payload fields (`family`/`minbits`/`commonbits`/`ipaddr`) sit immediately
    /// after the varlena length word.
    ///
    /// C's `gk_ip_family`/`gk_ip_addr` read the `GistInetKey` struct at fixed
    /// offsets past `vl_len_[4]` (a 4-byte header), which is correct in C only
    /// because the GiST key the support proc receives is the un-packed
    /// (4-byte-header) form. This port stores varlenas header-ful while
    /// `SHORT_VARLENA_PACKING` is off, so a fixed 4-byte strip is faithful there;
    /// but `index_form_tuple` short-packs a small (23-byte) key once the flag is
    /// on, and `nocache_index_getattr`/`fetchatt` then hand the support procs the
    /// verbatim on-disk image with a 1-byte ("short") header. Read the payload at
    /// `VARDATA_ANY` — skip ONE byte for a short header, else `VARHDRSZ` — so the
    /// fields land correctly in both forms (a fixed 4-byte strip would drop three
    /// payload bytes off a short image). No-op while the flag is off (every stored
    /// key carries a 4-byte header).
    pub fn from_datum_bytes(b: &[u8]) -> GistInetKey {
        // VARDATA_ANY: a short header is a single byte with the low bit set (and
        // not the 0x01 external-pointer marker); else the 4-byte header.
        let data = match b.first() {
            Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &b[1..],
            _ => &b[4..],
        };
        let mut ipaddr = [0u8; 16];
        ipaddr.copy_from_slice(&data[3..19]);
        GistInetKey {
            family: data[0],
            minbits: data[1],
            commonbits: data[2],
            ipaddr,
        }
    }
}

/// Owned result of `inet_gist_picksplit`, mirroring the populated
/// `GIST_SPLITVEC` (`<access/gist.h>`). Offsets are 1-based `OffsetNumber`s,
/// exactly as written by the C `splitvec->spl_left[...] = i` assignments. The
/// fmgr boundary copies these into the real `GIST_SPLITVEC`.
#[derive(Clone, Debug, Default)]
pub struct GistInetSplitVec {
    /// `spl_left` — offsets of entries assigned to the left group.
    pub spl_left: Vec<OffsetNumber>,
    /// `spl_right` — offsets of entries assigned to the right group.
    pub spl_right: Vec<OffsetNumber>,
    /// `spl_ldatum` — union key of the left group.
    pub spl_ldatum: GistInetKey,
    /// `spl_rdatum` — union key of the right group.
    pub spl_rdatum: GistInetKey,
}

impl GistInetSplitVec {
    /// `v->spl_nleft`.
    #[inline]
    pub fn spl_nleft(&self) -> i32 {
        self.spl_left.len() as i32
    }

    /// `v->spl_nright`.
    #[inline]
    pub fn spl_nright(&self) -> i32 {
        self.spl_right.len() as i32
    }
}
