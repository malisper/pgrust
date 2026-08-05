//! src/common/ip.c: AF_UNIX / IPv4 / IPv6 address-info and name-info wrappers
//! over the system resolver. C's malloc'd `struct addrinfo` list is an owned
//! `Vec<PgAddrInfo>` here (cold, connection/startup-frequency; dropping it is
//! pg_freeaddrinfo_all); name out-buffers are `String`s filled even on
//! failure. Never ereports — returns the resolver's `EAI_*` code.

pub mod sys;

#[cfg(not(target_family = "wasm"))]
use std::mem::{size_of, MaybeUninit};
#[cfg(not(target_family = "wasm"))]
use std::ptr;

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub const NI_MAXSERV: usize = libc::NI_MAXSERV as usize;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
pub const NI_MAXSERV: usize = 32;

#[cfg(not(target_family = "wasm"))]
pub const NI_MAXHOST: usize = libc::NI_MAXHOST as usize;
// wasm32: the wasi libc crate exposes no netdb surface; musl/wasi-libc value.
#[cfg(target_family = "wasm")]
pub const NI_MAXHOST: usize = 1025;

#[cfg(not(target_family = "wasm"))]
const SOCKADDR_STORAGE_SIZE: usize = size_of::<libc::sockaddr_storage>();
// wasm32: musl-compatible sockaddr_storage size; only zeroed/copied here.
#[cfg(target_family = "wasm")]
const SOCKADDR_STORAGE_SIZE: usize = 128;

// wasm32: WASI p1 has no resolver and no AF_UNIX sockets; the wasi libc
// crate exposes none of the netdb/sockaddr surface. The API keeps its shape
// and every resolution fails with EAI_FAIL (musl value) — pqcomm/backend
// callers treat that as "client address unknown", exactly the C failure arm.
#[cfg(target_family = "wasm")]
mod wasm_netdb {
    pub const EAI_FAIL: i32 = -4;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SockAddr {
    pub addr: [u8; SOCKADDR_STORAGE_SIZE],
    pub salen: u32,
}

impl SockAddr {
    pub const fn zeroed() -> Self {
        Self {
            addr: [0; SOCKADDR_STORAGE_SIZE],
            salen: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AddrInfoHint {
    pub flags: i32,
    pub family: i32,
    pub socktype: i32,
}

#[derive(Clone, Copy, Debug)]
pub struct PgAddrInfo {
    pub flags: i32,
    pub family: i32,
    pub socktype: i32,
    pub protocol: i32,
    pub addr: SockAddr,
}

// Resolved addresses replace `result`'s contents (C zeroes *result first).
#[cfg(target_family = "wasm")]
pub fn pg_getaddrinfo_all(
    _hostname: Option<&str>,
    _servname: Option<&str>,
    _hint: &AddrInfoHint,
    result: &mut Vec<PgAddrInfo>,
) -> i32 {
    result.clear();
    wasm_netdb::EAI_FAIL
}

#[cfg(not(target_family = "wasm"))]
pub fn pg_getaddrinfo_all(
    hostname: Option<&str>,
    servname: Option<&str>,
    hint: &AddrInfoHint,
    result: &mut Vec<PgAddrInfo>,
) -> i32 {
    result.clear();

    if hint.family == libc::AF_UNIX {
        return getaddrinfo_unix(servname.unwrap_or(""), Some(hint), result);
    }

    let host_c = match hostname {
        Some(h) if !h.is_empty() => match std::ffi::CString::new(h) {
            Ok(c) => Some(c),
            Err(_) => return libc::EAI_FAIL,
        },
        _ => None,
    };
    let serv_c = match servname {
        Some(s) => match std::ffi::CString::new(s) {
            Ok(c) => Some(c),
            Err(_) => return libc::EAI_FAIL,
        },
        None => None,
    };

    let mut hints: libc::addrinfo = unsafe { MaybeUninit::zeroed().assume_init() };
    hints.ai_flags = hint.flags;
    hints.ai_family = hint.family;
    hints.ai_socktype = hint.socktype;

    let mut res: *mut libc::addrinfo = ptr::null_mut();
    // SAFETY: host/serv are NUL-terminated CStrings or NULL; res freed iff rc==0.
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

    let mut ai = res.cast_const();
    while !ai.is_null() {
        // SAFETY: non-null node of the returned list.
        let info = unsafe { &*ai };
        result.push(copy_addrinfo(info));
        ai = info.ai_next;
    }
    // SAFETY: res came from a successful getaddrinfo and is freed exactly once.
    unsafe { libc::freeaddrinfo(res) };

    0
}

// The owned Vec already copied out of the OS structures: consuming it frees.
pub fn pg_freeaddrinfo_all(_hint_ai_family: i32, _ai: Vec<PgAddrInfo>) {}

// Unlike standard getnameinfo, node/service are filled even on failure.
#[cfg(target_family = "wasm")]
pub fn pg_getnameinfo_all(
    _addr: &SockAddr,
    node: Option<&mut String>,
    service: Option<&mut String>,
    _flags: i32,
) -> i32 {
    // C failure arm: out-buffers filled even on failure.
    if let Some(n) = node {
        *n = "???".to_string();
    }
    if let Some(s) = service {
        *s = "???".to_string();
    }
    wasm_netdb::EAI_FAIL
}

#[cfg(not(target_family = "wasm"))]
pub fn pg_getnameinfo_all(
    addr: &SockAddr,
    mut node: Option<&mut String>,
    mut service: Option<&mut String>,
    flags: i32,
) -> i32 {
    let rc = if sockaddr_family(addr) == libc::AF_UNIX {
        getnameinfo_unix(addr, node.as_deref_mut(), service.as_deref_mut())
    } else {
        getnameinfo_system(addr, node.as_deref_mut(), service.as_deref_mut(), flags)
    };

    if rc != 0 {
        if let Some(n) = node {
            *n = "???".to_string();
        }
        if let Some(s) = service {
            *s = "???".to_string();
        }
    }

    rc
}

// C bug parity: only one addrinfo is ever set; AI_CANONNAME unsupported.
#[cfg(not(target_family = "wasm"))]
fn getaddrinfo_unix(
    path: &str,
    hintsp: Option<&AddrInfoHint>,
    result: &mut Vec<PgAddrInfo>,
) -> i32 {
    // C strlen/strcpy: an embedded NUL is not representable.
    if path.as_bytes().contains(&0) || path.len() >= sun_path_len() {
        return libc::EAI_FAIL;
    }

    let (ai_family, mut ai_socktype, ai_protocol) = match hintsp {
        None => (libc::AF_UNIX, libc::SOCK_STREAM, 0),
        Some(h) => (h.family, h.socktype, 0),
    };
    if ai_socktype == 0 {
        ai_socktype = libc::SOCK_STREAM;
    }
    if ai_family != libc::AF_UNIX {
        return libc::EAI_FAIL; // shouldn't have been called
    }

    let mut unp: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    unp.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in unp.sun_path.iter_mut().zip(path.bytes()) {
        *dst = src as libc::c_char;
    }

    let mut addrlen = size_of::<libc::sockaddr_un>() as u32;

    // Abstract socket: zero first byte; addrlen covers only the original
    // string so trailing zeros don't show up in OS socket lists.
    if path.as_bytes().first() == Some(&b'@') {
        unp.sun_path[0] = 0;
        addrlen = (sun_path_offset() + path.len()) as u32;
    }

    let mut sa = SockAddr::zeroed();
    let n = (addrlen as usize).min(sa.addr.len());
    // SAFETY: n <= sizeof(sockaddr_un) and n <= sa.addr.len().
    unsafe {
        ptr::copy_nonoverlapping(ptr::from_ref(&unp).cast::<u8>(), sa.addr.as_mut_ptr(), n);
    }
    sa.salen = addrlen;

    result.push(PgAddrInfo {
        flags: 0, // C callocs the node; ai_flags stays 0
        family: ai_family,
        socktype: ai_socktype,
        protocol: ai_protocol,
        addr: sa,
    });

    0
}

// C snprintf()s into NI_MAXHOST/NI_MAXSERV caller buffers (every C caller
// passes those sizes); the same bounds keep the EAI_MEMORY truncation branch
// firing under C's predicate (long Unix socket paths overflow `service`).
#[cfg(not(target_family = "wasm"))]
fn getnameinfo_unix(addr: &SockAddr, node: Option<&mut String>, service: Option<&mut String>) -> i32 {
    if sockaddr_family(addr) != libc::AF_UNIX || (node.is_none() && service.is_none()) {
        return libc::EAI_FAIL;
    }

    if let Some(n) = node {
        let formatted = "[local]";
        if formatted.len() >= NI_MAXHOST {
            return libc::EAI_MEMORY;
        }
        n.clear();
        n.push_str(formatted);
    }

    if let Some(s) = service {
        // Copy the unaligned storage bytes into an aligned sockaddr_un.
        let mut sun: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
        let n = (addr.salen as usize).min(size_of::<libc::sockaddr_un>());
        // SAFETY: n is bounded by both buffers.
        unsafe {
            ptr::copy_nonoverlapping(addr.addr.as_ptr(), ptr::from_mut(&mut sun).cast::<u8>(), n);
        }
        let path: Vec<u8> = sun.sun_path.iter().map(|c| *c as u8).collect();
        // Abstract socket (but could just be an empty string).
        let formatted = if path[0] == 0 && path.get(1).copied().unwrap_or(0) != 0 {
            format!("@{}", cstr_bytes_to_string(&path[1..]))
        } else {
            cstr_bytes_to_string(&path)
        };
        if formatted.len() >= NI_MAXSERV {
            return libc::EAI_MEMORY;
        }
        *s = formatted;
    }

    0
}

#[cfg(not(target_family = "wasm"))]
fn getnameinfo_system(
    addr: &SockAddr,
    node: Option<&mut String>,
    service: Option<&mut String>,
    flags: i32,
) -> i32 {
    let mut node_buf = [0 as libc::c_char; NI_MAXHOST];
    let mut service_buf = [0 as libc::c_char; NI_MAXSERV];

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

    // SAFETY: addr holds salen valid sockaddr bytes; out pointers NULL or sized.
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

// addr->ss_family (a misaligned &sockaddr_storage reference would be UB).
#[cfg(not(target_family = "wasm"))]
pub fn sockaddr_family(addr: &SockAddr) -> i32 {
    let p = addr.addr.as_ptr().cast::<libc::sockaddr_storage>();
    // SAFETY: addr.addr is sockaddr_storage-sized; the read is unaligned-safe.
    let fam = unsafe { ptr::addr_of!((*p).ss_family).read_unaligned() };
    fam as i32
}

// wasm32: musl-layout ss_family — a native-endian u16 at offset 0. Only
// all-zero (unknown) addresses exist on wasm, so this reads 0/AF_UNSPEC.
#[cfg(target_family = "wasm")]
pub fn sockaddr_family(addr: &SockAddr) -> i32 {
    u16::from_ne_bytes([addr.addr[0], addr.addr[1]]) as i32
}

// pg_memory_is_all_zeros(&addr, sizeof(addr)): "client address unknown".
pub fn sockaddr_is_all_zeros(addr: &SockAddr) -> bool {
    addr.salen == 0 && addr.addr.iter().all(|&b| b == 0)
}

#[cfg(not(target_family = "wasm"))]
fn copy_addrinfo(info: &libc::addrinfo) -> PgAddrInfo {
    let mut sa = SockAddr::zeroed();
    if !info.ai_addr.is_null() && (info.ai_addrlen as usize) <= sa.addr.len() {
        // SAFETY: ai_addr holds ai_addrlen valid bytes, bounded by the dest.
        unsafe {
            ptr::copy_nonoverlapping(
                info.ai_addr.cast::<u8>(),
                sa.addr.as_mut_ptr(),
                info.ai_addrlen as usize,
            );
        }
        sa.salen = info.ai_addrlen;
    }

    PgAddrInfo {
        flags: info.ai_flags,
        family: info.ai_family,
        socktype: info.ai_socktype,
        protocol: info.ai_protocol,
        addr: sa,
    }
}

#[cfg(not(target_family = "wasm"))]
fn c_char_buf_to_string(buf: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = buf.iter().map(|c| *c as u8).collect();
    cstr_bytes_to_string(&bytes)
}

#[cfg(not(target_family = "wasm"))]
fn cstr_bytes_to_string(bytes: &[u8]) -> String {
    let nul = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..nul]).into_owned()
}

#[cfg(not(target_family = "wasm"))]
fn sun_path_len() -> usize {
    let su: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    su.sun_path.len()
}

#[cfg(not(target_family = "wasm"))]
fn sun_path_offset() -> usize {
    let su: libc::sockaddr_un = unsafe { MaybeUninit::zeroed().assume_init() };
    su.sun_path.as_ptr() as usize - ptr::from_ref(&su) as usize
}

#[cfg(test)]
mod tests;
