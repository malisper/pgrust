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
