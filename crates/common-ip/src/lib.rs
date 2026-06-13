//! Port of PostgreSQL's `src/common/ip.c` — IPv6-aware network access
//! (AF_UNIX / IPv4 / IPv6 address-info and name-info helpers).
//!
//! Shared frontend/backend code wrapping the system resolver
//! (`getaddrinfo(3)` / `getnameinfo(3)`) with first-class support for AF_UNIX
//! socket addresses, which `getaddrinfo()` does not handle.
//!
//! The C API hands back a `struct addrinfo *` linked list freed with
//! `pg_freeaddrinfo_all`, and `getnameinfo` writes into caller `char` buffers.
//! Here the list is an owned `Vec<types_net::PgAddrInfo>` (dropping it is
//! `pg_freeaddrinfo_all`), and the name-info out-buffers are optional `String`s
//! filled only when requested. The socket address is carried in
//! `types_net::SockAddr` whose `addr` field is the platform
//! `struct sockaddr_storage` bytes and `salen` the valid length, exactly as C.
//!
//! These functions never ereport; they return the resolver's `EAI_*` code.

use std::mem::{size_of, MaybeUninit};
use std::ptr;

use types_net::{AddrInfoHint, PgAddrInfo, SockAddr};

/// `pg_getaddrinfo_all` — get address info for Unix, IPv4 and IPv6 sockets.
///
/// Resolved addresses are appended to `result` (cleared first, mirroring C's
/// `*result = NULL` since not all `getaddrinfo()` zero on failure). Returns `0`
/// on success or an `EAI_*` error code.
pub fn pg_getaddrinfo_all(
    hostname: Option<&str>,
    servname: Option<&str>,
    hint: &AddrInfoHint,
    result: &mut Vec<PgAddrInfo>,
) -> i32 {
    /* not all versions of getaddrinfo() zero *result on failure */
    result.clear();

    if hint.family == libc::AF_UNIX {
        return getaddrinfo_unix(servname, Some(hint), result);
    }

    /* NULL has special meaning to getaddrinfo(). */
    let host_c = match hostname {
        Some(h) if !h.is_empty() => match c_string(h) {
            Some(c) => Some(c),
            None => return libc::EAI_FAIL,
        },
        _ => None,
    };
    let serv_c = match servname {
        Some(s) => match c_string(s) {
            Some(c) => Some(c),
            None => return libc::EAI_FAIL,
        },
        None => None,
    };

    let hints = c_hint(hint);
    let mut res: *mut libc::addrinfo = ptr::null_mut();
    let rc = unsafe {
        libc::getaddrinfo(
            host_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
            serv_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
            &hints,
            &mut res,
        )
    };
    if rc != 0 {
        return rc;
    }

    let mut ai = res as *const libc::addrinfo;
    while !ai.is_null() {
        let info = unsafe { &*ai };
        result.push(copy_addrinfo(info));
        ai = info.ai_next;
    }
    unsafe { libc::freeaddrinfo(res) };

    0
}

/// `pg_freeaddrinfo_all` — free addrinfo structures.
///
/// In C this walks-and-`free()`s the AF_UNIX list or hands a system list to
/// `freeaddrinfo()`, keyed off the original hint family. Here the list is an
/// owned `Vec<PgAddrInfo>` already copied out of the OS structures, so this
/// just consumes (drops) it; the parameter is kept for API parity.
pub fn pg_freeaddrinfo_all(_hint_ai_family: i32, _ai: Vec<PgAddrInfo>) {}

/// `pg_getnameinfo_all` — get name info for Unix, IPv4 and IPv6 sockets.
///
/// `node` / `service` are filled (when `Some`) even on failure: a non-zero
/// resolver return fills them with `"???"`, mirroring C's guarantee. Returns
/// `0` on success or an `EAI_*` code.
pub fn pg_getnameinfo_all(
    addr: &SockAddr,
    node: Option<&mut String>,
    service: Option<&mut String>,
    flags: i32,
) -> i32 {
    let mut node = node;
    let mut service = service;

    let rc = if sockaddr_family(addr) == libc::AF_UNIX {
        getnameinfo_unix(addr, node.as_deref_mut(), service.as_deref_mut())
    } else {
        getnameinfo_system(addr, node.as_deref_mut(), service.as_deref_mut(), flags)
    };

    if rc != 0 {
        if let Some(n) = node.as_deref_mut() {
            *n = "???".to_string();
        }
        if let Some(s) = service.as_deref_mut() {
            *s = "???".to_string();
        }
    }

    rc
}

/* -------
 *	getaddrinfo_unix - get unix socket info using IPv6-compatible API
 *
 *	Bugs: only one addrinfo is set even though hintsp is NULL or
 *		  ai_socktype is 0
 *		  AI_CANONNAME is not supported.
 * -------
 */
fn getaddrinfo_unix(
    path: Option<&str>,
    hintsp: Option<&AddrInfoHint>,
    result: &mut Vec<PgAddrInfo>,
) -> i32 {
    let path = path.unwrap_or("");

    /* C uses strlen/strcpy on sun_path; an embedded NUL is not representable. */
    if path.as_bytes().contains(&0) || path.len() >= sun_path_len() {
        return libc::EAI_FAIL;
    }

    let (mut ai_family, mut ai_socktype, ai_protocol) = match hintsp {
        None => (libc::AF_UNIX, libc::SOCK_STREAM, 0),
        Some(h) => (h.family, h.socktype, 0),
    };
    if ai_socktype == 0 {
        ai_socktype = libc::SOCK_STREAM;
    }
    if ai_family != libc::AF_UNIX {
        /* shouldn't have been called */
        return libc::EAI_FAIL;
    }
    ai_family = libc::AF_UNIX;

    let mut unp: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    unp.sun_family = libc::AF_UNIX as libc::sa_family_t;

    /* strcpy(unp->sun_path, path); buffer is already zeroed (calloc). */
    for (dst, src) in unp.sun_path.iter_mut().zip(path.bytes()) {
        *dst = src as libc::c_char;
    }

    let mut addrlen = size_of::<libc::sockaddr_un>() as u32;

    /*
     * If the supplied path starts with @, replace that with a zero byte for
     * the internal representation, and set the address length to only include
     * the original string, so trailing zero bytes won't show up in OS socket
     * lists.
     */
    if path.as_bytes().first() == Some(&b'@') {
        unp.sun_path[0] = 0;
        addrlen = (sun_path_offset() + path.len()) as u32;
    }

    let mut sa = SockAddr::zeroed();
    let n = (addrlen as usize).min(sa.addr.len());
    unsafe {
        ptr::copy_nonoverlapping(
            (&unp as *const libc::sockaddr_un).cast::<u8>(),
            sa.addr.as_mut_ptr(),
            n,
        );
    }
    sa.salen = addrlen;

    result.push(PgAddrInfo {
        /* C calloc's the node; ai_flags is never set, stays 0. */
        flags: 0,
        family: ai_family,
        socktype: ai_socktype,
        protocol: ai_protocol,
        addr: sa,
    });

    0
}

/// Convert an AF_UNIX address to a hostname (`getnameinfo_unix`).
///
/// C writes into caller `char` buffers via `snprintf(buf, len, ...)` and
/// returns `EAI_MEMORY` when the formatted value would not fit
/// (`ret < 0 || ret >= len`, i.e. its length excluding the NUL is `>= len`).
/// The seam marshals the buffers as unbounded `String`s, dropping
/// `nodelen`/`servicelen`; the C API contract is that callers pass
/// `NI_MAXHOST`/`NI_MAXSERV`-sized buffers (verified across every caller:
/// backend_startup.c, elog.c, auth.c, hba.c, pgstatfuncs.c, network.c,
/// fe-connect.c). We re-impose those exact bounds so the
/// truncation -> `EAI_MEMORY` branch fires under the same predicate as C
/// (a `service` longer than `NI_MAXSERV`-1 is the live case for long Unix
/// socket paths).
fn getnameinfo_unix(
    addr: &SockAddr,
    node: Option<&mut String>,
    service: Option<&mut String>,
) -> i32 {
    /* Invalid arguments. */
    if sockaddr_family(addr) != libc::AF_UNIX || (node.is_none() && service.is_none()) {
        return libc::EAI_FAIL;
    }

    if let Some(n) = node {
        /* C: snprintf(node, nodelen, "%s", "[local]"); ret >= nodelen -> EAI_MEMORY */
        let formatted = "[local]".to_string();
        if formatted.len() >= libc::NI_MAXHOST as usize {
            return libc::EAI_MEMORY;
        }
        *n = formatted;
    }

    if let Some(s) = service {
        let sun = unsafe { &*(addr.addr.as_ptr().cast::<libc::sockaddr_un>()) };
        let path: Vec<u8> = sun.sun_path.iter().map(|c| *c as u8).collect();
        /*
         * Check whether it looks like an abstract socket, but it could also
         * just be an empty string.
         */
        let formatted = if path[0] == 0 && path.get(1).copied().unwrap_or(0) != 0 {
            format!("@{}", cstr_bytes_to_string(&path[1..]))
        } else {
            cstr_bytes_to_string(&path)
        };
        /* C: snprintf(service, servicelen, ...); ret >= servicelen -> EAI_MEMORY */
        if formatted.len() >= libc::NI_MAXSERV as usize {
            return libc::EAI_MEMORY;
        }
        *s = formatted;
    }

    0
}

/// Forward a system address to `getnameinfo()`.
fn getnameinfo_system(
    addr: &SockAddr,
    node: Option<&mut String>,
    service: Option<&mut String>,
    flags: i32,
) -> i32 {
    let mut node_buf = vec![0 as libc::c_char; libc::NI_MAXHOST as usize];
    let mut service_buf = vec![0 as libc::c_char; libc::NI_MAXSERV as usize];

    let node_ptr = if node.is_some() {
        node_buf.as_mut_ptr()
    } else {
        ptr::null_mut()
    };
    let service_ptr = if service.is_some() {
        service_buf.as_mut_ptr()
    } else {
        ptr::null_mut()
    };

    let rc = unsafe {
        libc::getnameinfo(
            addr.addr.as_ptr().cast::<libc::sockaddr>(),
            addr.salen,
            node_ptr,
            node_buf.len() as libc::socklen_t,
            service_ptr,
            service_buf.len() as libc::socklen_t,
            flags,
        )
    };
    if rc != 0 {
        return rc;
    }

    if let Some(n) = node {
        *n = c_char_buf_to_string(&node_buf);
    }
    if let Some(s) = service {
        *s = c_char_buf_to_string(&service_buf);
    }

    0
}

/// Read `ss_family` from a `SockAddr`'s `sockaddr_storage` bytes.
fn sockaddr_family(addr: &SockAddr) -> i32 {
    let ss = unsafe { &*(addr.addr.as_ptr().cast::<libc::sockaddr_storage>()) };
    ss.ss_family as i32
}

/// Build a `struct addrinfo` hint from the lookup-relevant fields C passes.
fn c_hint(hint: &AddrInfoHint) -> libc::addrinfo {
    let mut h: libc::addrinfo = unsafe { MaybeUninit::zeroed().assume_init() };
    h.ai_flags = hint.flags;
    h.ai_family = hint.family;
    h.ai_socktype = hint.socktype;
    h
}

/// Copy one `struct addrinfo` node into an owned `PgAddrInfo`.
fn copy_addrinfo(info: &libc::addrinfo) -> PgAddrInfo {
    let mut sa = SockAddr::zeroed();
    if !info.ai_addr.is_null() && (info.ai_addrlen as usize) <= sa.addr.len() {
        unsafe {
            ptr::copy_nonoverlapping(
                info.ai_addr.cast::<u8>(),
                sa.addr.as_mut_ptr(),
                info.ai_addrlen as usize,
            );
        }
        sa.salen = info.ai_addrlen as u32;
    }

    PgAddrInfo {
        flags: info.ai_flags,
        family: info.ai_family,
        socktype: info.ai_socktype,
        protocol: info.ai_protocol,
        addr: sa,
    }
}

fn c_string(s: &str) -> Option<std::ffi::CString> {
    std::ffi::CString::new(s).ok()
}

/// Decode a NUL-terminated C `char` buffer into an owned `String` (lossy; the
/// resolver may return locale-encoded names).
fn c_char_buf_to_string(buf: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = buf.iter().map(|c| *c as u8).collect();
    cstr_bytes_to_string(&bytes)
}

/// Decode bytes up to the first NUL into an owned `String` (lossy).
fn cstr_bytes_to_string(bytes: &[u8]) -> String {
    let nul = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..nul]).into_owned()
}

/// `sizeof(sun_path)` — the maximum AF_UNIX path length.
fn sun_path_len() -> usize {
    let su: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    su.sun_path.len()
}

/// `offsetof(struct sockaddr_un, sun_path)`.
fn sun_path_offset() -> usize {
    let su: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    let base = (&su as *const libc::sockaddr_un) as usize;
    let path = su.sun_path.as_ptr() as usize;
    path - base
}

/// Install this unit's seams.
pub fn init_seams() {
    common_ip_seams::pg_getaddrinfo_all::set(pg_getaddrinfo_all);
    common_ip_seams::pg_getnameinfo_all::set(pg_getnameinfo_all);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unix_hint(socktype: i32) -> AddrInfoHint {
        AddrInfoHint {
            flags: 0,
            family: libc::AF_UNIX,
            socktype,
        }
    }

    #[test]
    fn unix_getaddrinfo_defaults_socktype_to_stream() {
        let mut out = Vec::new();
        let rc = pg_getaddrinfo_all(None, Some("/tmp/.s.PGSQL.5432"), &unix_hint(0), &mut out);
        assert_eq!(rc, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].family, libc::AF_UNIX);
        assert_eq!(out[0].socktype, libc::SOCK_STREAM);
        assert_eq!(sockaddr_family(&out[0].addr), libc::AF_UNIX);
    }

    #[test]
    fn unix_getaddrinfo_flags_stay_zero() {
        let mut hint = unix_hint(libc::SOCK_STREAM);
        hint.flags = libc::AI_CANONNAME;
        let mut out = Vec::new();
        pg_getaddrinfo_all(None, Some("/tmp/x"), &hint, &mut out);
        assert_eq!(out[0].flags, 0);
    }

    #[test]
    fn unix_getnameinfo_local_node_and_path_service() {
        let mut out = Vec::new();
        pg_getaddrinfo_all(None, Some("/tmp/.s.PGSQL.5432"), &unix_hint(libc::SOCK_STREAM), &mut out);
        let mut node = String::new();
        let mut service = String::new();
        let rc = pg_getnameinfo_all(&out[0].addr, Some(&mut node), Some(&mut service), 0);
        assert_eq!(rc, 0);
        assert_eq!(node, "[local]");
        assert_eq!(service, "/tmp/.s.PGSQL.5432");
    }

    #[test]
    fn abstract_unix_path_round_trips_with_at_prefix() {
        let mut out = Vec::new();
        pg_getaddrinfo_all(None, Some("@postgres.sock"), &unix_hint(libc::SOCK_STREAM), &mut out);
        let mut service = String::new();
        let rc = pg_getnameinfo_all(&out[0].addr, None, Some(&mut service), 0);
        assert_eq!(rc, 0);
        assert_eq!(service, "@postgres.sock");
    }

    #[test]
    fn unix_path_too_long_fails() {
        let path = "x".repeat(sun_path_len());
        let mut out = Vec::new();
        let rc = pg_getaddrinfo_all(None, Some(&path), &unix_hint(libc::SOCK_STREAM), &mut out);
        assert_eq!(rc, libc::EAI_FAIL);
    }

    #[test]
    fn unix_path_with_embedded_nul_fails() {
        let mut out = Vec::new();
        let rc = pg_getaddrinfo_all(None, Some("/tmp/a\0b"), &unix_hint(libc::SOCK_STREAM), &mut out);
        assert_eq!(rc, libc::EAI_FAIL);
    }

    #[test]
    fn unix_nameinfo_requires_output_target() {
        let mut out = Vec::new();
        pg_getaddrinfo_all(None, Some("/tmp/socket"), &unix_hint(libc::SOCK_STREAM), &mut out);
        let rc = pg_getnameinfo_all(&out[0].addr, None, None, 0);
        assert_eq!(rc, libc::EAI_FAIL);
    }

    #[test]
    fn unix_nameinfo_long_path_overflows_service() {
        /*
         * C getnameinfo_unix snprintf()s the path into a NI_MAXSERV buffer and
         * returns EAI_MEMORY when it doesn't fit; pg_getnameinfo_all then fills
         * "???". Build a path longer than NI_MAXSERV-1 (but within sun_path).
         */
        let path = format!("/tmp/{}", "x".repeat(libc::NI_MAXSERV as usize));
        assert!(path.len() < sun_path_len());
        let mut out = Vec::new();
        pg_getaddrinfo_all(None, Some(&path), &unix_hint(libc::SOCK_STREAM), &mut out);
        let mut service = String::new();
        let rc = pg_getnameinfo_all(&out[0].addr, None, Some(&mut service), 0);
        assert_eq!(rc, libc::EAI_MEMORY);
        assert_eq!(service, "???");
    }

    #[test]
    fn system_getnameinfo_loopback_round_trips() {
        let hint = AddrInfoHint {
            flags: libc::AI_NUMERICHOST,
            family: libc::AF_INET,
            socktype: libc::SOCK_STREAM,
        };
        let mut out = Vec::new();
        let rc = pg_getaddrinfo_all(Some("127.0.0.1"), Some("80"), &hint, &mut out);
        assert_eq!(rc, 0);
        assert!(!out.is_empty());
        assert_eq!(sockaddr_family(&out[0].addr), libc::AF_INET);

        let mut node = String::new();
        let mut service = String::new();
        let rc = pg_getnameinfo_all(
            &out[0].addr,
            Some(&mut node),
            Some(&mut service),
            libc::NI_NUMERICHOST | libc::NI_NUMERICSERV,
        );
        assert_eq!(rc, 0);
        assert_eq!(node, "127.0.0.1");
        assert_eq!(service, "80");
    }
}
