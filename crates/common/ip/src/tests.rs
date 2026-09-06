use std::mem::{size_of, MaybeUninit};
use std::ptr;

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
    assert_eq!(out[0].addr.salen as usize, size_of::<libc::sockaddr_un>());
    assert_eq!(sockaddr_family(&out[0].addr), libc::AF_UNIX);
}

#[test]
fn unix_getaddrinfo_flags_stay_zero() {
    let mut hint = unix_hint(libc::SOCK_STREAM);
    hint.flags = libc::AI_CANONNAME;
    let mut out = Vec::new();
    assert_eq!(pg_getaddrinfo_all(None, Some("/tmp/x"), &hint, &mut out), 0);
    assert_eq!(out[0].flags, 0);
}

#[test]
fn wrong_family_hint_fails() {
    let mut hint = unix_hint(libc::SOCK_STREAM);
    hint.family = libc::AF_INET;
    let mut out = Vec::new();
    assert_eq!(
        super::getaddrinfo_unix("/tmp/x", Some(&hint), &mut out),
        libc::EAI_FAIL
    );
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
    // Abstract sockets: addrlen excludes the trailing zero bytes.
    assert!((out[0].addr.salen as usize) < size_of::<libc::sockaddr_un>());
    let mut service = String::new();
    let rc = pg_getnameinfo_all(&out[0].addr, None, Some(&mut service), 0);
    assert_eq!(rc, 0);
    assert_eq!(service, "@postgres.sock");
}

#[test]
fn unix_path_too_long_fails() {
    let path = "x".repeat(super::sun_path_len());
    let mut out = Vec::new();
    let rc = pg_getaddrinfo_all(None, Some(&path), &unix_hint(libc::SOCK_STREAM), &mut out);
    assert_eq!(rc, libc::EAI_FAIL);
    assert!(out.is_empty());
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
    assert_eq!(pg_getnameinfo_all(&out[0].addr, None, None, 0), libc::EAI_FAIL);
}

#[test]
fn unix_nameinfo_long_path_overflows_service() {
    // Longer than NI_MAXSERV-1 but within sun_path: EAI_MEMORY, "???" fill.
    let path = format!("/tmp/{}", "x".repeat(NI_MAXSERV));
    assert!(path.len() < super::sun_path_len());
    let mut out = Vec::new();
    pg_getaddrinfo_all(None, Some(&path), &unix_hint(libc::SOCK_STREAM), &mut out);
    let mut service = String::new();
    let rc = pg_getnameinfo_all(&out[0].addr, None, Some(&mut service), 0);
    assert_eq!(rc, libc::EAI_MEMORY);
    assert_eq!(service, "???");
}

#[test]
fn sockaddr_family_reads_unaligned_storage_bytes() {
    let mut sin: libc::sockaddr_in = unsafe { MaybeUninit::zeroed().assume_init() };
    sin.sin_family = libc::AF_INET as libc::sa_family_t;
    let mut sa = SockAddr::zeroed();
    let n = size_of::<libc::sockaddr_in>();
    unsafe {
        ptr::copy_nonoverlapping(ptr::from_ref(&sin).cast::<u8>(), sa.addr.as_mut_ptr(), n);
    }
    sa.salen = n as u32;
    assert_eq!(sockaddr_family(&sa), libc::AF_INET);
    assert!(!sockaddr_is_all_zeros(&sa));
    assert!(sockaddr_is_all_zeros(&SockAddr::zeroed()));
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
    pg_freeaddrinfo_all(hint.family, out);
}

#[test]
fn system_getnameinfo_failure_fills_question_marks() {
    let mut sa = SockAddr::zeroed();
    sa.addr[0] = 0xff;
    sa.addr[1] = 0xff;
    let mut node = String::from("stale");
    let mut service = String::from("stale");
    let rc = pg_getnameinfo_all(&sa, Some(&mut node), Some(&mut service), 0);
    assert_ne!(rc, 0);
    assert_eq!(node, "???");
    assert_eq!(service, "???");
}

// hba.c:1118 check_hostname resolves with a NULL hints pointer.
#[test]
fn default_hints_resolve_numeric_host() {
    let mut out = Vec::new();
    assert_eq!(pg_getaddrinfo_default_hints("127.0.0.1", &mut out), 0);
    assert!(out.iter().any(|ai| sockaddr_family(&ai.addr) == libc::AF_INET));
    assert_ne!(pg_getaddrinfo_default_hints("no.such.host.invalid", &mut out), 0);
    assert!(out.is_empty());
}

/// `parse_numeric_host` must produce the EXACT bytes the system resolver
/// produces for the same numeric address under AI_NUMERICHOST. It is the
/// wasm arm's whole implementation (WASI p1 has no resolver), and the only
/// place its sockaddr layout can be checked against a real libc is here —
/// so check it here, byte for byte, for both families and a port.
#[test]
fn numeric_host_matches_getaddrinfo() {
    for (host, family, port) in [
        ("127.0.0.1", libc::AF_INET, 0u16),
        ("127.0.0.1", libc::AF_INET, 5432),
        ("10.0.0.0", libc::AF_INET, 0),
        ("255.255.255.0", libc::AF_INET, 0),
        ("::1", libc::AF_INET6, 0),
        ("::1", libc::AF_INET6, 5432),
        ("fe80::1", libc::AF_INET6, 0),
        ("ffff:ffff:ffff:ffff::", libc::AF_INET6, 0),
    ] {
        let hint = AddrInfoHint {
            flags: libc::AI_NUMERICHOST,
            family: libc::AF_UNSPEC,
            socktype: 0,
        };
        let mut out = Vec::new();
        let serv = port.to_string();
        let servname = if port == 0 { None } else { Some(serv.as_str()) };
        let rc = pg_getaddrinfo_all(Some(host), servname, &hint, &mut out);
        assert_eq!(rc, 0, "system getaddrinfo failed for {host}");
        let sys_addr = out[0].addr;

        let (fam, ours) = parse_numeric_host(host, port).expect("numeric parse");
        assert_eq!(fam, family, "{host}");
        assert_eq!(fam, out[0].family, "{host}");
        assert_eq!(ours.salen, sys_addr.salen, "{host} salen");
        let n = ours.salen as usize;
        assert_eq!(&ours.addr[..n], &sys_addr.addr[..n], "{host} sockaddr bytes");
        pg_freeaddrinfo_all(hint.family, out);
    }
}

/// A name is NOT a numeric address: the caller's EAI_NONAME arm (the hba
/// parser then records the token as a deferred hostname, as C does).
#[test]
fn numeric_host_declines_names_and_junk() {
    assert!(parse_numeric_host("localhost", 0).is_none());
    assert!(parse_numeric_host("example.com", 0).is_none());
    assert!(parse_numeric_host("", 0).is_none());
    assert!(parse_numeric_host("127.0.0.1/32", 0).is_none());
    assert!(parse_numeric_host("fe80::1%eth0", 0).is_none());
}
