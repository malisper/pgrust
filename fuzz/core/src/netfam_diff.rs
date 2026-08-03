//! netfam_diff: differential fuzz driver for the two libpq "net family"
//! crates vs verbatim vendored PostgreSQL 18.3 C (csrc/pg_netfam_io.c,
//! upstream sha 62d6c7d3df; lane p1-mb-netfam). Selector = data[0] % 6:
//!
//!   0 range_sockaddr  — crates/backend/libpq/ifaddr::pg_range_sockaddr
//!                       over same-family v4/v6 (addr, netaddr, netmask)
//!                       triples; verdict compared. The Rust
//!                       mixed-family arm (`_ => false`) is exercised
//!                       in-driver with its postcondition asserted (C's
//!                       caller contract — ifaddr.c:45-47 "caller must
//!                       already have verified that all three addresses
//!                       are in the same address family" — makes mixed
//!                       triples a domain carve on the C side; C's own
//!                       non-IP else-arm is driven via pg_nf_range_other
//!                       and must agree with the Rust mismatch verdict:
//!                       both false).
//!   1 cidr_mask       — ifaddr::pg_sockaddr_cidr_mask over a fuzz
//!                       numbits string (truncated at the first NUL on
//!                       BOTH sides: the C parameter is a cstring) x
//!                       family {Inet, Inet6, Other} x the None arm;
//!                       error verdict + full mask bytes compared.
//!   2 foreach_ifaddr  — ifaddr::pg_foreach_ifaddr vs the vendored
//!                       getifaddrs pg_foreach_ifaddr run IN THE SAME
//!                       PROCESS: both walk the same live interface
//!                       list, the collected (family, addr, mask)
//!                       sequences are compared entry-for-entry
//!                       (mask-substitution logic in
//!                       run_ifaddr_callback rides the real data). One
//!                       retry absorbs an interface change between the
//!                       two walks; a second mismatch is a divergence.
//!   3 send stream     — crates/backend/libpq/pqformat send family over
//!                       an op stream: pq_beginmessage /
//!                       pq_beginmessage_reuse / pq_begintypsend init,
//!                       then sendbyte/int8/16/32/64/sendint(b)/
//!                       float4/8/bytes/text/countedtext/string/
//!                       ascii_string and the pq_writeintN/writestring
//!                       preallocated family (both sides pre-enlarged,
//!                       the C contract); after EVERY op: len, capacity
//!                       (== C maxlen), cursor and bytes[0..len]
//!                       compared. Stream ends in pq_endmessage /
//!                       pq_endmessage_reuse (payload compared at the
//!                       captured pq_putmessage seam) or pq_endtypsend
//!                       (full varlena image compared).
//!   4 getmsg stream   — pqformat getmsg family over a fuzz message:
//!                       getmsgbyte/int(b)/int64/float4/float8/bytes/
//!                       copymsgbytes/text/string/rawstring/end; values
//!                       (floats as raw bits), cursor after every op,
//!                       error verdict + exact SQLSTATE class compared;
//!                       the first error ends the exec (C longjmp
//!                       parity).
//!   5 put messages    — pq_puttextmessage / pq_putemptymessage via the
//!                       captured putmessage seam (msgtype + body).
//!
//! Comparison planes: value bytes/bits + error verdict + exact SQLSTATE
//! class (08P01 / XX000 / 22021 / 54000); message text out of scope.
//! No-panic everywhere.
//!
//! ENVIRONMENT MODEL (mocked environment, never computation):
//!   - encodings sit at the mbutils boot default (client == server ==
//!     SQL_ASCII), so pg_client_to_server verifies (NUL => 22021) and
//!     never converts; the C oracle shims the same environment.
//!   - pg_server_to_client is a SEAM (mbutils_seams); the driver installs
//!     a flag-driven identity conversion: when the exec arms CONVERT and
//!     the input is NUL-free, both sides take the `p != str` converted
//!     arms with a fresh identical copy. NUL-bearing inputs stay on the
//!     identity path (a real conversion result is a NUL-terminated C
//!     string, so a converted copy can never carry an interior NUL).
//!   - pq_putmessage is a SEAM (pqcomm_seams); both sides capture
//!     (msgtype, body) — the socket carve, compared at the seam.
//!   - receive-side conversion (pg_client_to_server returning Some) needs
//!     a live pg_conversion proc and stays excluded-state (charter carve;
//!     exception rows in proofs/coverage/phase1-exceptions.tsv).
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - arm 0: C compared only on same-family triples (see above).
//!   - arm 1: numbits truncated at the first NUL on both sides (C
//!     signature is char *).
//!   - arm 3: pq_writeintN/pq_writestring driven only after both sides
//!     enlarge (the "already has enough space preallocated" contract;
//!     C's Assert is compiled out and the memcpy would otherwise
//!     overrun).
//!   - arm 3/5: cursor compared as its low byte after
//!     pq_beginmessage(_reuse): C stashes a signed char msgtype into the
//!     int cursor (sign-extends >= 0x80), Rust stores u8-as-usize; the
//!     wire byte — which is what pq_putmessage receives back — is
//!     identical, so the representational difference is a C artifact,
//!     not a surface (documented here as the carve of record).
#![allow(dead_code)]

use std::ffi::CString;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::raw::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

use ifaddr::{AddressFamily, SockAddrError};
use types_error::PgError;

#[repr(C)]
#[derive(Clone, Copy)]
struct PgNfIfEntry {
    fam: u8,
    addr: [u8; 16],
    mask: [u8; 16],
}

extern "C" {
    fn pg_nf_set_convert(flag: i32);
    fn pg_nf_range(family: i32, addr: *const u8, netaddr: *const u8, netmask: *const u8) -> i32;
    fn pg_nf_range_other() -> i32;
    fn pg_nf_cidr_mask(numbits: *const c_char, family_sel: i32, out: *mut u8) -> i32;
    fn pg_nf_foreach(out: *mut PgNfIfEntry, cap: i32) -> i32;
    fn pg_nf_run_cb(
        addr_family: i32,
        addr: *const u8,
        mask_kind: i32,
        mask: *const u8,
        out_fam: *mut u8,
        out_addr: *mut u8,
        out_mask: *mut u8,
    ) -> i32;

    fn pg_nf_out_begin(kind: i32, msgtype: u8) -> i32;
    fn pg_nf_out_get(len: *mut i32, maxlen: *mut i32, cursor: *mut i32) -> *const c_char;
    fn pg_nf_out_enlarge(needed: i32) -> i32;
    fn pg_nf_sendbyte(b: u8) -> i32;
    fn pg_nf_sendint(i: u32, b: i32) -> i32;
    fn pg_nf_sendint8(i: u8) -> i32;
    fn pg_nf_sendint16(i: u16) -> i32;
    fn pg_nf_sendint32(i: u32) -> i32;
    fn pg_nf_sendint64(i: u64) -> i32;
    fn pg_nf_sendfloat4(bits: u32) -> i32;
    fn pg_nf_sendfloat8(bits: u64) -> i32;
    fn pg_nf_sendbytes(data: *const u8, datalen: i32) -> i32;
    fn pg_nf_sendtext(data: *const u8, datalen: i32) -> i32;
    fn pg_nf_sendcountedtext(data: *const u8, datalen: i32) -> i32;
    fn pg_nf_sendstring(s: *const c_char) -> i32;
    fn pg_nf_send_ascii_string(s: *const c_char) -> i32;
    fn pg_nf_writeint(width: i32, v: u64) -> i32;
    fn pg_nf_writestring(s: *const c_char) -> i32;
    fn pg_nf_endmessage(reuse: i32) -> i32;
    fn pg_nf_endtypsend(lenout: *mut i32) -> *const u8;
    fn pg_nf_puttextmessage(msgtype: u8, s: *const c_char) -> i32;
    fn pg_nf_putemptymessage(msgtype: u8) -> i32;
    fn pg_nf_put_get(msgtype: *mut i32, len: *mut usize) -> *const u8;
    fn pg_nf_put_reset();

    fn pg_nf_msg_set(bytes: *const u8, len: i32) -> i32;
    fn pg_nf_msg_cursor() -> i32;
    fn pg_nf_getmsgbyte(out: *mut i32) -> i32;
    fn pg_nf_getmsgint(b: i32, out: *mut u32) -> i32;
    fn pg_nf_getmsgint64(out: *mut i64) -> i32;
    fn pg_nf_getmsgfloat4(bits: *mut u32) -> i32;
    fn pg_nf_getmsgfloat8(bits: *mut u64) -> i32;
    fn pg_nf_getmsgbytes(datalen: i32, out: *mut *const c_char) -> i32;
    fn pg_nf_copymsgbytes(datalen: i32, out: *mut u8) -> i32;
    fn pg_nf_getmsgtext(rawbytes: i32, out: *mut *mut c_char, nbytes: *mut i32) -> i32;
    fn pg_nf_getmsgstring(out: *mut *const c_char) -> i32;
    fn pg_nf_getmsgrawstring(out: *mut *const c_char) -> i32;
    fn pg_nf_getmsgend() -> i32;
}

const C_OK: i32 = 0;
const C_ERR_PROTOCOL: i32 = 1; // 08P01
const C_ERR_INTERNAL: i32 = 2; // XX000
const C_ERR_CHAR_REPERTOIRE: i32 = 3; // 22021
const C_ERR_PROGRAM_LIMIT: i32 = 4; // 54000

fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == types_error::ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL
    } else if e.sqlstate == types_error::ERRCODE_INTERNAL_ERROR {
        C_ERR_INTERNAL
    } else if e.sqlstate == types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_CHAR_REPERTOIRE
    } else if e.sqlstate == types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        C_ERR_PROGRAM_LIMIT
    } else {
        99
    }
}

// ---------------- seams (environment mocks, set once per process) --------

thread_local! {
    static CONVERT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PUT_CAPTURE: std::cell::RefCell<Option<(u8, Vec<u8>)>> =
        const { std::cell::RefCell::new(None) };
}

static SEAMS_OWNED: AtomicBool = AtomicBool::new(false);

fn netfam_server_to_client<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    s: &[u8],
) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    if CONVERT.with(|c| c.get()) && !s.is_empty() && !s.contains(&0) {
        Ok(Some(mcx::slice_in(mcx, s)?))
    } else {
        Ok(None)
    }
}

fn netfam_putmessage(msgtype: u8, body: &[u8]) -> types_error::PgResult<i32> {
    PUT_CAPTURE.with(|c| *c.borrow_mut() = Some((msgtype, body.to_vec())));
    Ok(0)
}

/// Install the driver's seam impls. Tolerates another module in the same
/// (test) binary having installed first — SEAMS_OWNED then stays false and
/// the convert / put-capture sub-arms are skipped, keeping both sides on
/// the identity/no-capture paths. In the netfam_diff fuzz binary this
/// driver always owns both seams.
fn init_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        pqformat::init_seams(); // no-op body, executed for the record
        let s2c = std::panic::catch_unwind(|| {
            mbutils_seams::pg_server_to_client::set(netfam_server_to_client);
        });
        let put = std::panic::catch_unwind(|| {
            pqcomm_seams::pq_putmessage::set(netfam_putmessage);
        });
        if s2c.is_ok() && put.is_ok() {
            SEAMS_OWNED.store(true, Ordering::Relaxed);
        }
    });
}

fn seams_owned() -> bool {
    init_seams();
    SEAMS_OWNED.load(Ordering::Relaxed)
}

fn set_convert(flag: bool) {
    let flag = flag && seams_owned();
    CONVERT.with(|c| c.set(flag));
    unsafe { pg_nf_set_convert(flag as i32) };
}

// ---------------- byte cursor ----------------

struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        for s in &mut b {
            *s = self.u8();
        }
        u32::from_le_bytes(b)
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for s in &mut b {
            *s = self.u8();
        }
        u64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let start = self.pos.min(self.d.len());
        let end = (self.pos + n).min(self.d.len());
        self.pos += n;
        &self.d[start..end]
    }
    fn arr<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for s in &mut out {
            *s = self.u8();
        }
        out
    }
    fn done(&self) -> bool {
        self.pos >= self.d.len()
    }
}

// ---------------- arm 0: range_sockaddr ----------------

/// kind 4: the run_ifaddr_callback mask-substitution differential (the
/// Rust side goes through the crate's #[doc(hidden)] fuzz conduit).
fn check_run_cb(r: &mut Rdr) {
    let addr_v6 = r.u8() & 1 == 1;
    let mask_kind = r.u8() % 4; // 0 none, 1 v4, 2 v6, 3 other-family
    let ab: [u8; 16] = r.arr();
    let mb: [u8; 16] = r.arr();
    let addr = if addr_v6 {
        IpAddr::V6(Ipv6Addr::from(ab))
    } else {
        let a4: [u8; 4] = ab[..4].try_into().unwrap();
        IpAddr::V4(Ipv4Addr::from(a4))
    };
    let mask = match mask_kind {
        0 => None,
        1 => {
            let m4: [u8; 4] = mb[..4].try_into().unwrap();
            Some(IpAddr::V4(Ipv4Addr::from(m4)))
        }
        2 => Some(IpAddr::V6(Ipv6Addr::from(mb))),
        _ => {
            // C models this as an AF_UNSPEC mask; on the Rust side (IpAddr
            // carries only v4/v6) the behavioral counterpart is a mask of
            // the OTHER family — both are the family-mismatch -> fullmask
            // arm of run_ifaddr_callback.
            if addr_v6 {
                let m4: [u8; 4] = mb[..4].try_into().unwrap();
                Some(IpAddr::V4(Ipv4Addr::from(m4)))
            } else {
                Some(IpAddr::V6(Ipv6Addr::from(mb)))
            }
        }
    };
    let mut got: Option<(IpAddr, IpAddr)> = None;
    ifaddr::run_ifaddr_callback_for_fuzz(
        &mut |a, m| {
            assert!(got.is_none(), "callback fired twice");
            got = Some((a, m));
        },
        addr,
        mask,
    );
    let (ra, rm) = got.expect("callback fired");

    let (mut cf, mut ca, mut cm) = (0u8, [0u8; 16], [0u8; 16]);
    let crc = unsafe {
        pg_nf_run_cb(
            addr_v6 as i32,
            ab.as_ptr(),
            mask_kind as i32,
            mb.as_ptr(),
            &mut cf,
            ca.as_mut_ptr(),
            cm.as_mut_ptr(),
        )
    };
    assert_eq!(crc, 0, "C callback fired");
    let (c_addr, c_mask) = if cf == 4 {
        let a4: [u8; 4] = ca[..4].try_into().unwrap();
        let m4: [u8; 4] = cm[..4].try_into().unwrap();
        (
            IpAddr::V4(Ipv4Addr::from(a4)),
            IpAddr::V4(Ipv4Addr::from(m4)),
        )
    } else {
        (
            IpAddr::V6(Ipv6Addr::from(ca)),
            IpAddr::V6(Ipv6Addr::from(cm)),
        )
    };
    assert_eq!((ra, rm), (c_addr, c_mask), "run_ifaddr_callback (addr {addr:?} mask {mask:?})");
}

fn check_range(r: &mut Rdr) {
    let kind = r.u8() % 5;
    match kind {
        0 => {
            // v4 same-family
            let a: [u8; 4] = r.arr();
            let n: [u8; 4] = r.arr();
            let m: [u8; 4] = r.arr();
            let rr = ifaddr::pg_range_sockaddr(
                &IpAddr::V4(Ipv4Addr::from(a)),
                &IpAddr::V4(Ipv4Addr::from(n)),
                &IpAddr::V4(Ipv4Addr::from(m)),
            );
            let cr = unsafe { pg_nf_range(0, a.as_ptr(), n.as_ptr(), m.as_ptr()) };
            assert_eq!(rr, cr != 0, "range v4 {a:?} {n:?} {m:?}");
        }
        1 => {
            // v6 same-family
            let a: [u8; 16] = r.arr();
            let n: [u8; 16] = r.arr();
            let m: [u8; 16] = r.arr();
            let rr = ifaddr::pg_range_sockaddr(
                &IpAddr::V6(Ipv6Addr::from(a)),
                &IpAddr::V6(Ipv6Addr::from(n)),
                &IpAddr::V6(Ipv6Addr::from(m)),
            );
            let cr = unsafe { pg_nf_range(1, a.as_ptr(), n.as_ptr(), m.as_ptr()) };
            assert_eq!(rr, cr != 0, "range v6 {a:?} {n:?} {m:?}");
        }
        2 => {
            // Rust mixed-family arm; the C non-IP else-arm is the
            // behavioral counterpart (both: not in range).
            let a: [u8; 4] = r.arr();
            let n: [u8; 16] = r.arr();
            let m: [u8; 16] = r.arr();
            let rr = ifaddr::pg_range_sockaddr(
                &IpAddr::V4(Ipv4Addr::from(a)),
                &IpAddr::V6(Ipv6Addr::from(n)),
                &IpAddr::V6(Ipv6Addr::from(m)),
            );
            let cr = unsafe { pg_nf_range_other() };
            assert!(!rr, "mixed-family triple must not match");
            assert_eq!(cr, 0, "C non-IP family arm returns 0");
        }
        3 => {
            let a: [u8; 16] = r.arr();
            let n: [u8; 4] = r.arr();
            let m: [u8; 4] = r.arr();
            let rr = ifaddr::pg_range_sockaddr(
                &IpAddr::V6(Ipv6Addr::from(a)),
                &IpAddr::V4(Ipv4Addr::from(n)),
                &IpAddr::V4(Ipv4Addr::from(m)),
            );
            assert!(!rr, "mixed-family triple must not match");
        }
        _ => check_run_cb(r),
    }
}

// ---------------- arm 1: cidr_mask ----------------

fn check_cidr(r: &mut Rdr) {
    let family_sel = r.u8() % 3;
    let family = match family_sel {
        0 => AddressFamily::Inet,
        1 => AddressFamily::Inet6,
        _ => AddressFamily::Other,
    };
    let has_bits = r.u8() & 1 == 1;
    // numbits: a cstring in C — truncate at the first NUL on both sides;
    // lossy-decode invalid UTF-8 so both sides see identical bytes.
    let n = (r.u8() as usize) % 24;
    let raw = r.bytes(n);
    let s: String = String::from_utf8_lossy(raw).into_owned();
    let s = match s.find('\0') {
        Some(i) => s[..i].to_string(),
        None => s,
    };

    let (numbits, cstr);
    if has_bits {
        numbits = Some(s.as_str());
        cstr = Some(CString::new(s.clone()).expect("NUL-free by construction"));
    } else {
        numbits = None;
        cstr = None;
    }

    let rres = ifaddr::pg_sockaddr_cidr_mask(numbits, family);
    let mut out = [0u8; 16];
    let cres = unsafe {
        pg_nf_cidr_mask(
            cstr.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            family_sel as i32,
            out.as_mut_ptr(),
        )
    };
    match rres {
        Ok(IpAddr::V4(m)) => {
            assert_eq!(cres, 0, "cidr v4 verdict ({s:?})");
            assert_eq!(m.octets(), out[..4], "cidr v4 mask ({s:?})");
        }
        Ok(IpAddr::V6(m)) => {
            assert_eq!(cres, 0, "cidr v6 verdict ({s:?})");
            assert_eq!(m.octets(), out[..16], "cidr v6 mask ({s:?})");
        }
        Err(e) => {
            assert_eq!(cres, -1, "cidr error verdict ({s:?} family {family:?})");
            // error kinds partition as C's -1 sources, in C's order: the
            // numbits parse rejects first (InvalidBits), then the family
            // switch default (UnsupportedFamily).
            match family {
                AddressFamily::Other => {
                    if numbits.is_none() {
                        assert_eq!(e, SockAddrError::UnsupportedFamily);
                    }
                }
                _ => assert_eq!(e, SockAddrError::InvalidBits),
            }
        }
    }
}

// ---------------- arm 2: foreach_ifaddr ----------------

fn collect_rust() -> std::io::Result<Vec<(IpAddr, IpAddr)>> {
    let mut v = Vec::new();
    ifaddr::pg_foreach_ifaddr(|a, m| v.push((a, m)))?;
    Ok(v)
}

fn collect_c() -> Option<Vec<(IpAddr, IpAddr)>> {
    let mut buf = vec![
        PgNfIfEntry {
            fam: 0,
            addr: [0; 16],
            mask: [0; 16],
        };
        512
    ];
    let n = unsafe { pg_nf_foreach(buf.as_mut_ptr(), buf.len() as i32) };
    if n < 0 {
        return None;
    }
    Some(
        buf[..n as usize]
            .iter()
            .map(|e| {
                if e.fam == 4 {
                    let a: [u8; 4] = e.addr[..4].try_into().unwrap();
                    let m: [u8; 4] = e.mask[..4].try_into().unwrap();
                    (
                        IpAddr::V4(Ipv4Addr::from(a)),
                        IpAddr::V4(Ipv4Addr::from(m)),
                    )
                } else {
                    (
                        IpAddr::V6(Ipv6Addr::from(e.addr)),
                        IpAddr::V6(Ipv6Addr::from(e.mask)),
                    )
                }
            })
            .collect(),
    )
}

fn check_foreach() {
    // one retry absorbs interface churn between the two walks
    for attempt in 0..2 {
        let (Ok(rust), Some(c)) = (collect_rust(), collect_c()) else {
            // getifaddrs failure is environmental (ENOMEM); no-panic plane
            return;
        };
        if rust == c {
            // invariant plane: masks are family-matched and non-zero
            for (a, m) in &rust {
                assert_eq!(a.is_ipv4(), m.is_ipv4(), "mask family matches addr");
                match m {
                    IpAddr::V4(m) => assert_ne!(*m, Ipv4Addr::UNSPECIFIED),
                    IpAddr::V6(m) => assert_ne!(*m, Ipv6Addr::UNSPECIFIED),
                }
            }
            return;
        }
        assert!(
            attempt == 0,
            "foreach_ifaddr diverged twice: rust {rust:?} vs c {c:?}"
        );
    }
}

// ---------------- arms 3/5: send / put ----------------

const SEND_CAP: usize = 1 << 16;

fn take_put_captures(op: &str) {
    if !seams_owned() {
        return;
    }
    let rust = PUT_CAPTURE.with(|c| c.borrow_mut().take());
    let (mut cmt, mut clen) = (0i32, 0usize);
    let cptr = unsafe { pg_nf_put_get(&mut cmt, &mut clen) };
    let (rmt, rbody) = rust.unwrap_or_else(|| panic!("{op}: Rust putmessage not captured"));
    assert!(cmt >= 0, "{op}: C putmessage not captured");
    let cbody = if clen == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(cptr, clen) }
    };
    assert_eq!(rmt as i32, cmt, "{op}: putmessage msgtype");
    assert_eq!(&rbody[..], cbody, "{op}: putmessage body");
    unsafe { pg_nf_put_reset() };
}

/// NUL-free lossy string from fuzz bytes (identical bytes both sides).
fn cstring_of(raw: &[u8]) -> (String, CString) {
    let s: String = String::from_utf8_lossy(raw).replace('\0', "x");
    let c = CString::new(s.clone()).expect("NUL-free by construction");
    (s, c)
}

fn run_send(r: &mut Rdr) {
    let ctx = mcx::MemoryContext::new("netfam_send");
    let m = ctx.mcx();

    let init_kind = r.u8() % 2; // 0 message, 1 typsend
    let msgtype = r.u8();
    let mut buf = if init_kind == 0 {
        let cst = unsafe { pg_nf_out_begin(0, msgtype) };
        assert_eq!(cst, 0, "pq_beginmessage errored");
        pqformat::pq_beginmessage(m, msgtype).expect("pq_beginmessage")
    } else {
        let cst = unsafe { pg_nf_out_begin(1, 0) };
        assert_eq!(cst, 0, "pq_begintypsend errored");
        pqformat::pq_begintypsend(m).expect("pq_begintypsend")
    };
    let in_message = init_kind == 0;

    macro_rules! compare_state {
        ($op:expr) => {{
            let (mut clen, mut cmax, mut ccur) = (0i32, 0i32, 0i32);
            let cdata = unsafe { pg_nf_out_get(&mut clen, &mut cmax, &mut ccur) };
            assert_eq!(buf.len(), clen as usize, "len after {}", $op);
            assert_eq!(buf.capacity(), cmax as usize, "capacity after {}", $op);
            if in_message {
                // msgtype-in-cursor: compare the wire byte (see header)
                assert_eq!(buf.cursor as u8, ccur as u8, "cursor byte after {}", $op);
            } else {
                assert_eq!(buf.cursor, ccur as usize, "cursor after {}", $op);
            }
            let cbytes =
                unsafe { std::slice::from_raw_parts(cdata.cast::<u8>(), clen as usize) };
            assert_eq!(buf.as_bytes(), cbytes, "bytes after {}", $op);
        }};
    }

    compare_state!("init");

    // end-of-stream choice read UP FRONT (the tail of the payload is often
    // consumed by the op loop; a post-loop read would always see 0).
    let end_reuse = r.u8() & 1 == 1;

    let mut total: usize = buf.len();
    while !r.done() && total <= SEND_CAP {
        let op = r.u8() % 16;
        match op {
            0 => {
                let b = r.u8();
                let rres = pqformat::pq_sendbyte(&mut buf, b);
                let cst = unsafe { pg_nf_sendbyte(b) };
                assert!(rres.is_ok() && cst == 0, "sendbyte errored");
                total += 1;
                compare_state!("sendbyte");
            }
            1 => {
                let v = r.u8();
                let rres = pqformat::pq_sendint8(&mut buf, v);
                let cst = unsafe { pg_nf_sendint8(v) };
                assert!(rres.is_ok() && cst == 0, "sendint8 errored");
                total += 1;
                compare_state!("sendint8");
            }
            2 => {
                let v = r.u16();
                let rres = pqformat::pq_sendint16(&mut buf, v);
                let cst = unsafe { pg_nf_sendint16(v) };
                assert!(rres.is_ok() && cst == 0, "sendint16 errored");
                total += 2;
                compare_state!("sendint16");
            }
            3 => {
                let v = r.u32();
                let rres = pqformat::pq_sendint32(&mut buf, v);
                let cst = unsafe { pg_nf_sendint32(v) };
                assert!(rres.is_ok() && cst == 0, "sendint32 errored");
                total += 4;
                compare_state!("sendint32");
            }
            4 => {
                let v = r.u64();
                let rres = pqformat::pq_sendint64(&mut buf, v);
                let cst = unsafe { pg_nf_sendint64(v) };
                assert!(rres.is_ok() && cst == 0, "sendint64 errored");
                total += 8;
                compare_state!("sendint64");
            }
            5 => {
                // pq_sendint incl. the unsupported-width elog arm
                let v = r.u32();
                let b = match r.u8() % 5 {
                    0 => 1,
                    1 => 2,
                    2 => 4,
                    3 => 0,
                    _ => (r.u8() as i32) + 5,
                };
                let rres = pqformat::pq_sendint(&mut buf, v, b);
                let cst = unsafe { pg_nf_sendint(v, b) };
                match rres {
                    Ok(()) => assert_eq!(cst, 0, "sendint({b}) verdict"),
                    Err(e) => {
                        assert_eq!(err_class(&e), cst, "sendint({b}) error class");
                        assert_eq!(cst, C_ERR_INTERNAL, "sendint({b}) is XX000");
                    }
                }
                total += 8;
                compare_state!("sendint");
            }
            6 => {
                let bits = r.u32();
                let rres = pqformat::pq_sendfloat4(&mut buf, f32::from_bits(bits));
                let cst = unsafe { pg_nf_sendfloat4(bits) };
                assert!(rres.is_ok() && cst == 0, "sendfloat4 errored");
                total += 4;
                compare_state!("sendfloat4");
            }
            7 => {
                let bits = r.u64();
                let rres = pqformat::pq_sendfloat8(&mut buf, f64::from_bits(bits));
                let cst = unsafe { pg_nf_sendfloat8(bits) };
                assert!(rres.is_ok() && cst == 0, "sendfloat8 errored");
                total += 8;
                compare_state!("sendfloat8");
            }
            8 => {
                let n = r.u16() as usize % 1024;
                let chunk = r.bytes(n).to_vec();
                total += chunk.len();
                let rres = pqformat::pq_sendbytes(&mut buf, &chunk);
                let cst = unsafe { pg_nf_sendbytes(chunk.as_ptr(), chunk.len() as i32) };
                assert!(rres.is_ok() && cst == 0, "sendbytes errored");
                compare_state!("sendbytes");
            }
            9 | 10 => {
                // sendtext / sendcountedtext: raw chunks (interior NULs
                // legal), conversion flag per-op
                set_convert(r.u8() & 1 == 1);
                let n = r.u16() as usize % 1024;
                let chunk = r.bytes(n).to_vec();
                total += chunk.len() + 4;
                let (rres, cst, name) = if op == 9 {
                    (pqformat::pq_sendtext(&mut buf, &chunk), unsafe {
                        pg_nf_sendtext(chunk.as_ptr(), chunk.len() as i32)
                    }, "sendtext")
                } else {
                    (pqformat::pq_sendcountedtext(&mut buf, &chunk), unsafe {
                        pg_nf_sendcountedtext(chunk.as_ptr(), chunk.len() as i32)
                    }, "sendcountedtext")
                };
                assert!(rres.is_ok() && cst == 0, "{name} errored");
                set_convert(false);
                compare_state!(name);
            }
            11 => {
                set_convert(r.u8() & 1 == 1);
                let n = r.u16() as usize % 512;
                let (s, c) = cstring_of(r.bytes(n));
                total += s.len() + 1;
                let rres = pqformat::pq_sendstring(&mut buf, s.as_bytes());
                let cst = unsafe { pg_nf_sendstring(c.as_ptr()) };
                assert!(rres.is_ok() && cst == 0, "sendstring errored");
                set_convert(false);
                compare_state!("sendstring");
            }
            12 => {
                let n = r.u16() as usize % 512;
                let (s, c) = cstring_of(r.bytes(n));
                total += s.len() + 1;
                let rres = pqformat::pq_send_ascii_string(&mut buf, s.as_bytes());
                let cst = unsafe { pg_nf_send_ascii_string(c.as_ptr()) };
                assert!(rres.is_ok() && cst == 0, "send_ascii_string errored");
                compare_state!("send_ascii_string");
            }
            13 => {
                // preallocated write family: enlarge both sides first
                let width = [1usize, 2, 4, 8][(r.u8() % 4) as usize];
                let v = r.u64();
                let rres = buf.enlarge(width);
                let cst = unsafe { pg_nf_out_enlarge(width as i32) };
                assert!(rres.is_ok() && cst == 0, "enlarge errored");
                compare_state!("enlarge");
                let cst = unsafe { pg_nf_writeint(width as i32, v) };
                match width {
                    1 => pqformat::pq_writeint8(&mut buf, v as u8),
                    2 => pqformat::pq_writeint16(&mut buf, v as u16),
                    4 => pqformat::pq_writeint32(&mut buf, v as u32),
                    _ => pqformat::pq_writeint64(&mut buf, v),
                }
                assert_eq!(cst, 0, "writeint errored");
                total += width;
                compare_state!("writeint");
            }
            14 => {
                set_convert(r.u8() & 1 == 1);
                let n = r.u16() as usize % 512;
                let (s, c) = cstring_of(r.bytes(n));
                total += s.len() + 1;
                // preallocation contract: identity conversion keeps length
                let rres = buf.enlarge(s.len() + 1);
                let cst = unsafe { pg_nf_out_enlarge(s.len() as i32 + 1) };
                assert!(rres.is_ok() && cst == 0, "enlarge errored");
                let rres = pqformat::pq_writestring(&mut buf, s.as_bytes());
                let cst = unsafe { pg_nf_writestring(c.as_ptr()) };
                assert!(rres.is_ok() && cst == 0, "writestring errored");
                set_convert(false);
                compare_state!("writestring");
            }
            _ => {
                // beginmessage_reuse mid-stream (message mode only)
                if in_message {
                    let mt = r.u8();
                    pqformat::pq_beginmessage_reuse(&mut buf, mt);
                    let cst = unsafe { pg_nf_out_begin(2, mt) };
                    assert_eq!(cst, 0);
                    total = 0;
                    compare_state!("beginmessage_reuse");
                }
            }
        }
    }

    // stream end
    if in_message {
        if seams_owned() {
            let reuse = end_reuse;
            let cst = unsafe { pg_nf_endmessage(reuse as i32) };
            let rres = if reuse {
                pqformat::pq_endmessage_reuse(&buf)
            } else {
                pqformat::pq_endmessage(buf)
            };
            assert!(rres.is_ok() && cst == 0, "endmessage errored");
            take_put_captures("endmessage");
        }
    } else {
        let mut clen = 0i32;
        let cimg = unsafe { pg_nf_endtypsend(&mut clen) };
        let cbytes = unsafe { std::slice::from_raw_parts(cimg, clen as usize) };
        let bytea = pqformat::pq_endtypsend(buf);
        assert_eq!(bytea.as_bytes(), cbytes, "endtypsend varlena image");
        assert_eq!(bytea.varsize(), clen as usize, "endtypsend varsize");
    }
}

fn run_put(r: &mut Rdr) {
    if !seams_owned() {
        return;
    }
    let ctx = mcx::MemoryContext::new("netfam_put");
    let m = ctx.mcx();
    let msgtype = r.u8();
    if r.u8() & 1 == 0 {
        set_convert(r.u8() & 1 == 1);
        let n = r.u16() as usize % 1024;
        let (s, c) = cstring_of(r.bytes(n));
        let rres = pqformat::pq_puttextmessage(m, msgtype, s.as_bytes());
        let cst = unsafe { pg_nf_puttextmessage(msgtype, c.as_ptr()) };
        assert!(rres.is_ok() && cst == 0, "puttextmessage errored");
        set_convert(false);
        take_put_captures("puttextmessage");
    } else {
        let rres = pqformat::pq_putemptymessage(msgtype);
        let cst = unsafe { pg_nf_putemptymessage(msgtype) };
        assert!(rres.is_ok() && cst == 0, "putemptymessage errored");
        take_put_captures("putemptymessage");
    }
}

// ---------------- arm 4: getmsg ----------------

fn run_getmsg(r: &mut Rdr) {
    let ctx = mcx::MemoryContext::new("netfam_getmsg");
    let m = ctx.mcx();

    let n = r.u16() as usize % 2048;
    let body = r.bytes(n).to_vec();
    let mut v = mcx::PgVec::new_in(m);
    if v.try_reserve_exact(body.len().max(1)).is_err() {
        return;
    }
    v.extend_from_slice(&body);
    let mut msg = stringinfo::StringInfo::from_vec(v).expect("from_vec");
    let cst = unsafe { pg_nf_msg_set(body.as_ptr(), body.len() as i32) };
    assert_eq!(cst, 0, "msg_set errored");

    macro_rules! cursor_check {
        ($op:expr) => {{
            assert_eq!(
                msg.cursor,
                unsafe { pg_nf_msg_cursor() } as usize,
                "cursor after {}",
                $op
            );
        }};
    }
    // fail-fast plumbing: on any error compare the class and end the exec
    // (C's ereport longjmp ends the statement the same way).
    macro_rules! verdict {
        ($rres:expr, $cst:expr, $op:expr) => {{
            match $rres {
                Ok(val) => {
                    assert_eq!($cst, C_OK, "{}: C errored, Rust ok", $op);
                    Some(val)
                }
                Err(e) => {
                    assert_eq!(err_class(&e), $cst, "{}: error class", $op);
                    assert_ne!($cst, C_OK, "{}: Rust errored, C ok", $op);
                    None
                }
            }
        }};
    }

    while !r.done() {
        let op = r.u8() % 12;
        match op {
            0 => {
                let mut cout = 0i32;
                let cst = unsafe { pg_nf_getmsgbyte(&mut cout) };
                let rres = pqformat::pq_getmsgbyte(&mut msg);
                match verdict!(rres, cst, "getmsgbyte") {
                    Some(vr) => assert_eq!(vr, cout, "getmsgbyte value"),
                    None => return,
                }
                cursor_check!("getmsgbyte");
            }
            1 => {
                let b = match r.u8() % 5 {
                    0 => 1,
                    1 => 2,
                    2 => 4,
                    3 => 0,
                    _ => (r.u8() as i32) + 5,
                };
                let mut cout = 0u32;
                let cst = unsafe { pg_nf_getmsgint(b, &mut cout) };
                let rres = pqformat::pq_getmsgint(&mut msg, b);
                match verdict!(rres, cst, "getmsgint") {
                    Some(vr) => assert_eq!(vr, cout, "getmsgint({b}) value"),
                    None => return,
                }
                cursor_check!("getmsgint");
            }
            2 => {
                let mut cout = 0i64;
                let cst = unsafe { pg_nf_getmsgint64(&mut cout) };
                let rres = pqformat::pq_getmsgint64(&mut msg);
                match verdict!(rres, cst, "getmsgint64") {
                    Some(vr) => assert_eq!(vr, cout, "getmsgint64 value"),
                    None => return,
                }
                cursor_check!("getmsgint64");
            }
            3 => {
                let mut cbits = 0u32;
                let cst = unsafe { pg_nf_getmsgfloat4(&mut cbits) };
                let rres = pqformat::pq_getmsgfloat4(&mut msg);
                match verdict!(rres, cst, "getmsgfloat4") {
                    Some(vr) => assert_eq!(vr.to_bits(), cbits, "getmsgfloat4 bits"),
                    None => return,
                }
                cursor_check!("getmsgfloat4");
            }
            4 => {
                let mut cbits = 0u64;
                let cst = unsafe { pg_nf_getmsgfloat8(&mut cbits) };
                let rres = pqformat::pq_getmsgfloat8(&mut msg);
                match verdict!(rres, cst, "getmsgfloat8") {
                    Some(vr) => assert_eq!(vr.to_bits(), cbits, "getmsgfloat8 bits"),
                    None => return,
                }
                cursor_check!("getmsgfloat8");
            }
            5 => {
                // crosses the remaining-length boundary
                let datalen = (r.u16() as usize) % (body.len() + 8);
                let mut cout: *const c_char = std::ptr::null();
                let cst = unsafe { pg_nf_getmsgbytes(datalen as i32, &mut cout) };
                let rres = pqformat::pq_getmsgbytes(&mut msg, datalen);
                match verdict!(rres, cst, "getmsgbytes") {
                    Some(vr) => {
                        let cb =
                            unsafe { std::slice::from_raw_parts(cout.cast::<u8>(), datalen) };
                        assert_eq!(vr, cb, "getmsgbytes value");
                    }
                    None => return,
                }
                cursor_check!("getmsgbytes");
            }
            6 => {
                let datalen = (r.u16() as usize) % (body.len() + 8).min(4096);
                let mut rbuf = vec![0u8; datalen];
                let mut cbuf = vec![0u8; datalen.max(1)];
                let cst = unsafe { pg_nf_copymsgbytes(datalen as i32, cbuf.as_mut_ptr()) };
                let rres = pqformat::pq_copymsgbytes(&mut msg, &mut rbuf);
                match verdict!(rres, cst, "copymsgbytes") {
                    Some(()) => assert_eq!(rbuf, cbuf[..datalen], "copymsgbytes value"),
                    None => return,
                }
                cursor_check!("copymsgbytes");
            }
            7 => {
                let rawbytes = (r.u16() as usize) % (body.len() + 8);
                let mut cout: *mut c_char = std::ptr::null_mut();
                let mut cn = 0i32;
                let cst = unsafe { pg_nf_getmsgtext(rawbytes as i32, &mut cout, &mut cn) };
                let rres = pqformat::pq_getmsgtext(m, &mut msg, rawbytes);
                match verdict!(rres, cst, "getmsgtext") {
                    Some(vr) => {
                        assert_eq!(vr.len(), cn as usize, "getmsgtext nbytes");
                        let cb = unsafe {
                            std::slice::from_raw_parts(cout.cast::<u8>(), cn as usize)
                        };
                        assert_eq!(&vr[..], cb, "getmsgtext value");
                        unsafe { libc::free(cout.cast()) };
                    }
                    None => {
                        // C longjmp'd before returning ownership
                        return;
                    }
                }
                cursor_check!("getmsgtext");
            }
            8 | 9 => {
                let name = if op == 8 { "getmsgstring" } else { "getmsgrawstring" };
                let mut cout: *const c_char = std::ptr::null();
                let cst = if op == 8 {
                    unsafe { pg_nf_getmsgstring(&mut cout) }
                } else {
                    unsafe { pg_nf_getmsgrawstring(&mut cout) }
                };
                if op == 8 {
                    let rres = pqformat::pq_getmsgstring(m, &mut msg);
                    match verdict!(rres, cst, name) {
                        Some(vr) => {
                            let cb = unsafe { std::ffi::CStr::from_ptr(cout) }.to_bytes();
                            assert_eq!(vr.as_bytes(), cb, "{name} value");
                        }
                        None => return,
                    }
                } else {
                    let rres = pqformat::pq_getmsgrawstring(&mut msg);
                    match verdict!(rres, cst, name) {
                        Some(vr) => {
                            let cb = unsafe { std::ffi::CStr::from_ptr(cout) }.to_bytes();
                            assert_eq!(vr, cb, "{name} value");
                        }
                        None => return,
                    }
                }
                cursor_check!(name);
            }
            10 => {
                let cst = unsafe { pg_nf_getmsgend() };
                let rres = pqformat::pq_getmsgend(&msg);
                if verdict!(rres, cst, "getmsgend").is_none() {
                    return;
                }
            }
            _ => cursor_check!("noop"),
        }
    }
}

// ---------------- entry ----------------

pub fn netfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_seams();
    set_convert(false);
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let mut r = Rdr::new(payload);
    match sel % 6 {
        0 => check_range(&mut r),
        1 => check_cidr(&mut r),
        2 => check_foreach(),
        3 => run_send(&mut r),
        4 => run_getmsg(&mut r),
        _ => run_put(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EXHAUSTIVE-DIFF (a0): every numbits value 0..=200 plus malformed
    /// shapes, both families and the None arm, against the vendored C.
    #[test]
    fn cidr_exhaustive_bits() {
        let _serial = crate::c_oracle_serial();
        for family_sel in [0u8, 1, 2] {
            for bits in 0..=200u32 {
                let s = bits.to_string();
                let mut payload = vec![family_sel, 1, s.len() as u8];
                payload.extend_from_slice(s.as_bytes());
                let mut r = Rdr::new(&payload);
                check_cidr(&mut r);
            }
            for s in [
                "", " ", "+", "-", "12x", " 12", "+12", "-1", "0", "032",
                "99999999999999999999", "-99999999999999999999", "\t8", "8 ",
                "0x10", "1e2",
            ] {
                let mut payload = vec![family_sel, 1, s.len() as u8];
                payload.extend_from_slice(s.as_bytes());
                let mut r = Rdr::new(&payload);
                check_cidr(&mut r);
            }
            // the None (numbits == NULL) arm
            let payload = vec![family_sel, 0];
            let mut r = Rdr::new(&payload);
            check_cidr(&mut r);
        }
    }

    #[test]
    fn range_smoke() {
        let _serial = crate::c_oracle_serial();
        // v4: 10.1.2.3 in 10.0.0.0/8, not in 127.0.0.0/8
        let mut p = vec![0u8, 0];
        p.extend_from_slice(&[10, 1, 2, 3, 10, 0, 0, 0, 255, 0, 0, 0]);
        check_range(&mut Rdr::new(&p[1..]));
        let mut p = vec![0u8];
        p.extend_from_slice(&[10, 1, 2, 3, 127, 0, 0, 0, 255, 0, 0, 0]);
        check_range(&mut Rdr::new(&p));
        // v6 loopback /128
        let mut p = vec![1u8];
        let mut lo = [0u8; 16];
        lo[15] = 1;
        p.extend_from_slice(&lo);
        p.extend_from_slice(&lo);
        p.extend_from_slice(&[0xff; 16]);
        check_range(&mut Rdr::new(&p));
        // mixed-family arms
        check_range(&mut Rdr::new(&[2u8; 40]));
        check_range(&mut Rdr::new(&[3u8; 40]));
    }

    #[test]
    fn foreach_differential() {
        let _serial = crate::c_oracle_serial();
        check_foreach();
    }

    /// run_ifaddr_callback: every (addr family x mask kind) cell incl. the
    /// zero-mask (invalid -> fullmask) and family-mismatch arms.
    #[test]
    fn run_cb_cells() {
        let _serial = crate::c_oracle_serial();
        for addr_v6 in [0u8, 1] {
            for mask_kind in 0..4u8 {
                for mask_byte in [0u8, 0xff, 0xf0] {
                    let mut p = vec![addr_v6, mask_kind];
                    p.extend_from_slice(&[10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
                    p.extend_from_slice(&[mask_byte; 16]);
                    check_run_cb(&mut Rdr::new(&p));
                }
            }
        }
    }

    /// Deterministic seeds through every arm (smoke for the planes).
    #[test]
    fn arm_smoke() {
        // range v4
        netfam_diff(&[0, 0, 10, 1, 2, 3, 10, 0, 0, 0, 255, 0, 0, 0]);
        // cidr "24" v4
        netfam_diff(&[1, 0, 1, 2, b'2', b'4']);
        // foreach
        netfam_diff(&[2]);
        // send stream: beginmessage 'D', sendint32, sendstring converted
        let mut v = vec![3u8, 0, b'D'];
        v.push(3); // op sendint32
        v.extend_from_slice(&0xdeadbeefu32.to_le_bytes());
        v.push(11); // op sendstring
        v.push(1); // convert on
        v.extend_from_slice(&5u16.to_le_bytes());
        v.extend_from_slice(b"hello");
        v.push(0); // end: endmessage
        netfam_diff(&v);
        // typsend: begintypsend + sendfloat8 + endtypsend
        let mut v = vec![3u8, 1, 0];
        v.push(7);
        v.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        netfam_diff(&v);
        // getmsg: 6-byte message, getmsgint(4) + getmsgbyte + getmsgbyte(err)
        let mut v = vec![4u8];
        v.extend_from_slice(&6u16.to_le_bytes());
        v.extend_from_slice(&[1, 2, 3, 4, 0, 7]);
        v.push(1);
        v.push(2); // b = 4
        v.push(0);
        v.push(0);
        v.push(0); // one past end -> both error 08P01
        netfam_diff(&v);
        // getmsgstring + rawstring + end
        let mut v = vec![4u8];
        v.extend_from_slice(&8u16.to_le_bytes());
        v.extend_from_slice(b"ab\0cde\0x");
        v.push(8);
        v.push(9);
        v.push(10);
        netfam_diff(&v);
        // put messages
        let mut v = vec![5u8, b'N', 0, 1];
        v.extend_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(b"msg");
        netfam_diff(&v);
        netfam_diff(&[5, b'Z', 1]);
    }

    /// getmsgtext embedded-NUL: SQL_ASCII verify rejects on both sides
    /// with 22021 (the C shim mirrors pg_verify_mbstr; see TU header).
    #[test]
    fn getmsgtext_nul_verify() {
        let mut v = vec![4u8];
        v.extend_from_slice(&4u16.to_le_bytes());
        v.extend_from_slice(b"a\0bc");
        v.push(7); // getmsgtext
        v.extend_from_slice(&4u16.to_le_bytes());
        netfam_diff(&v);
    }
}
