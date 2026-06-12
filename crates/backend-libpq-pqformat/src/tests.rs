//! Tests drive the real routines with fixture implementations of the two
//! outward seams (transport capture; togglable encoding conversion). Seam
//! slots are process-global `OnceLock`s, so the fixtures are installed once
//! and dispatch through thread-locals to keep tests isolated across threads.

use super::*;
use std::cell::{Cell, RefCell};
use std::sync::Once;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    /// When true, the conversion fixtures "convert" by uppercasing ASCII and
    /// return `Some`; when false they return `None` (no conversion needed).
    static CONVERT: Cell<bool> = const { Cell::new(false) };
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        backend_libpq_pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            0
        });
        backend_utils_mb_mbutils_seams::pg_server_to_client::set(convert_fixture);
        backend_utils_mb_mbutils_seams::pg_client_to_server::set(convert_fixture);
    });
}

fn convert_fixture<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if CONVERT.with(|c| c.get()) {
        let upper: Vec<u8> = s.iter().map(|b| b.to_ascii_uppercase()).collect();
        Ok(Some(mcx::slice_in(mcx, &upper)?))
    } else {
        Ok(None)
    }
}

struct Fixture {
    ctx: mcx::MemoryContext,
}

fn setup() -> Fixture {
    install_fixtures();
    SENT.with(|s| s.borrow_mut().clear());
    CONVERT.with(|c| c.set(false));
    Fixture { ctx: mcx::MemoryContext::new("pqformat-test") }
}

fn sent() -> Vec<(u8, Vec<u8>)> {
    SENT.with(|s| s.borrow().clone())
}

fn recv<'mcx>(f: &'mcx Fixture, bytes: &[u8]) -> StringInfo<'mcx> {
    StringInfo::from_vec(mcx::slice_in(f.ctx.mcx(), bytes).unwrap())
}

#[test]
fn message_assembly_roundtrip() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'P').unwrap();
    pq_sendint8(&mut buf, 0xAB).unwrap();
    pq_sendint16(&mut buf, 0x1234).unwrap();
    pq_sendint32(&mut buf, 0xDEADBEEF).unwrap();
    pq_sendint64(&mut buf, 0x0102030405060708).unwrap();
    pq_sendbyte(&mut buf, 7).unwrap();
    pq_sendbytes(&mut buf, b"raw").unwrap();
    pq_endmessage(buf);

    let msgs = sent();
    assert_eq!(msgs.len(), 1);
    let (t, body) = &msgs[0];
    assert_eq!(*t, b'P');
    let mut expect = vec![0xAB, 0x12, 0x34, 0xDE, 0xAD, 0xBE, 0xEF];
    expect.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 7]);
    expect.extend_from_slice(b"raw");
    assert_eq!(body, &expect);
}

#[test]
fn beginmessage_reuse_and_endmessage_reuse() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'A').unwrap();
    pq_sendint32(&mut buf, 1).unwrap();
    pq_endmessage_reuse(&buf);
    pq_beginmessage_reuse(&mut buf, b'B');
    pq_sendint32(&mut buf, 2).unwrap();
    pq_endmessage_reuse(&buf);

    let msgs = sent();
    assert_eq!(msgs[0], (b'A', vec![0, 0, 0, 1]));
    assert_eq!(msgs[1], (b'B', vec![0, 0, 0, 2]));
}

#[test]
fn sendint_widths_and_unsupported_size() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'X').unwrap();
    pq_sendint(&mut buf, 0x01, 1).unwrap();
    pq_sendint(&mut buf, 0x0203, 2).unwrap();
    pq_sendint(&mut buf, 0x04050607, 4).unwrap();
    assert_eq!(buf.as_bytes(), &[1, 2, 3, 4, 5, 6, 7]);
    let err = pq_sendint(&mut buf, 0, 3).unwrap_err();
    assert_eq!(err.message(), "unsupported integer size 3");
}

#[test]
fn sendfloats_use_ieee_bits() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'X').unwrap();
    pq_sendfloat4(&mut buf, 1.5f32).unwrap();
    pq_sendfloat8(&mut buf, -2.25f64).unwrap();
    let mut expect = 1.5f32.to_bits().to_be_bytes().to_vec();
    expect.extend_from_slice(&(-2.25f64).to_bits().to_be_bytes());
    assert_eq!(buf.as_bytes(), &expect[..]);
}

#[test]
fn sendcountedtext_unconverted_and_converted() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'T').unwrap();
    pq_sendcountedtext(&mut buf, b"abc").unwrap();
    assert_eq!(buf.as_bytes(), &[0, 0, 0, 3, b'a', b'b', b'c']);

    CONVERT.with(|c| c.set(true));
    let mut buf2 = pq_beginmessage(f.ctx.mcx(), b'T').unwrap();
    pq_sendcountedtext(&mut buf2, b"abc").unwrap();
    assert_eq!(buf2.as_bytes(), &[0, 0, 0, 3, b'A', b'B', b'C']);
}

#[test]
fn sendstring_and_sendtext() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'S').unwrap();
    pq_sendstring(&mut buf, b"hi").unwrap();
    pq_sendtext(&mut buf, b"yo").unwrap();
    assert_eq!(buf.as_bytes(), &[b'h', b'i', 0, b'y', b'o']);

    CONVERT.with(|c| c.set(true));
    let mut buf2 = pq_beginmessage(f.ctx.mcx(), b'S').unwrap();
    pq_sendstring(&mut buf2, b"hi").unwrap();
    assert_eq!(buf2.as_bytes(), &[b'H', b'I', 0]);
}

#[test]
fn send_ascii_string_masks_high_bytes() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'E').unwrap();
    pq_send_ascii_string(&mut buf, &[b'o', b'k', 0xC3, 0xA9]).unwrap();
    assert_eq!(buf.as_bytes(), &[b'o', b'k', b'?', b'?', 0]);
}

#[test]
fn typsend_writes_varlena_header() {
    let f = setup();
    let mut buf = pq_begintypsend(f.ctx.mcx()).unwrap();
    pq_sendbytes(&mut buf, b"payload").unwrap();
    let bytea = pq_endtypsend(buf);
    let len = bytea.len();
    assert_eq!(len, 4 + 7);
    assert_eq!(&bytea[..4], ((len as u32) << 2).to_le_bytes());
    assert_eq!(&bytea[4..], b"payload");
}

#[test]
fn puttextmessage_and_putemptymessage() {
    let f = setup();
    pq_puttextmessage(f.ctx.mcx(), b'N', b"note").unwrap();
    pq_putemptymessage(b'Z');
    CONVERT.with(|c| c.set(true));
    pq_puttextmessage(f.ctx.mcx(), b'N', b"note").unwrap();

    let msgs = sent();
    assert_eq!(msgs[0], (b'N', b"note\0".to_vec()));
    assert_eq!(msgs[1], (b'Z', vec![]));
    assert_eq!(msgs[2], (b'N', b"NOTE\0".to_vec()));
}

#[test]
fn getmsgbyte_and_exhaustion() {
    let f = setup();
    let mut msg = recv(&f, &[0xFE]);
    assert_eq!(pq_getmsgbyte(&mut msg).unwrap(), 0xFE);
    let err = pq_getmsgbyte(&mut msg).unwrap_err();
    assert_eq!(err.message(), "no data left in message");
    assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
}

#[test]
fn getmsgint_widths() {
    let f = setup();
    let mut msg = recv(&f, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
    assert_eq!(pq_getmsgint(&mut msg, 1).unwrap(), 0x01);
    assert_eq!(pq_getmsgint(&mut msg, 2).unwrap(), 0x0203);
    assert_eq!(pq_getmsgint(&mut msg, 4).unwrap(), 0x04050607);
    pq_getmsgend(&msg).unwrap();

    let mut msg2 = recv(&f, &[1, 2, 3]);
    let err = pq_getmsgint(&mut msg2, 3).unwrap_err();
    assert_eq!(err.message(), "unsupported integer size 3");
    let err = pq_getmsgint(&mut msg2, 4).unwrap_err();
    assert_eq!(err.message(), "insufficient data left in message");
}

#[test]
fn getmsgint64_and_floats() {
    let f = setup();
    let mut bytes = (-5i64).to_be_bytes().to_vec();
    bytes.extend_from_slice(&1.5f32.to_bits().to_be_bytes());
    bytes.extend_from_slice(&(-2.25f64).to_bits().to_be_bytes());
    let mut msg = recv(&f, &bytes);
    assert_eq!(pq_getmsgint64(&mut msg).unwrap(), -5);
    assert_eq!(pq_getmsgfloat4(&mut msg).unwrap(), 1.5f32);
    assert_eq!(pq_getmsgfloat8(&mut msg).unwrap(), -2.25f64);
    pq_getmsgend(&msg).unwrap();
}

#[test]
fn getmsgbytes_and_copymsgbytes() {
    let f = setup();
    let mut msg = recv(&f, b"hello!");
    assert_eq!(pq_getmsgbytes(&mut msg, 3).unwrap(), b"hel");
    let mut out = [0u8; 2];
    pq_copymsgbytes(&mut msg, &mut out).unwrap();
    assert_eq!(&out, b"lo");
    let err = pq_getmsgbytes(&mut msg, 2).unwrap_err();
    assert_eq!(err.message(), "insufficient data left in message");
    assert_eq!(pq_getmsgbytes(&mut msg, 1).unwrap(), b"!");
    pq_getmsgend(&msg).unwrap();
}

#[test]
fn getmsgtext_both_branches() {
    let f = setup();
    let mut msg = recv(&f, b"abcdef");
    let t = pq_getmsgtext(f.ctx.mcx(), &mut msg, 3).unwrap();
    assert_eq!(&t[..], b"abc");

    CONVERT.with(|c| c.set(true));
    let t2 = pq_getmsgtext(f.ctx.mcx(), &mut msg, 3).unwrap();
    assert_eq!(&t2[..], b"DEF");

    let err = pq_getmsgtext(f.ctx.mcx(), &mut msg, 1).unwrap_err();
    assert_eq!(err.message(), "insufficient data left in message");
}

#[test]
fn getmsgstring_and_rawstring() {
    let f = setup();
    let mut msg = recv(&f, b"one\0two\0");
    let s = pq_getmsgstring(f.ctx.mcx(), &mut msg).unwrap();
    assert!(matches!(s, PqString::Borrowed(_)));
    assert_eq!(s.as_bytes(), b"one");
    let s2 = pq_getmsgrawstring(&mut msg).unwrap();
    assert_eq!(s2, b"two");
    pq_getmsgend(&msg).unwrap();

    CONVERT.with(|c| c.set(true));
    let mut msg2 = recv(&f, b"abc\0");
    let s3 = pq_getmsgstring(f.ctx.mcx(), &mut msg2).unwrap();
    assert!(matches!(s3, PqString::Converted(_)));
    assert_eq!(s3.as_bytes(), b"ABC");
}

#[test]
fn unterminated_string_is_invalid() {
    let f = setup();
    // No NUL inside the message: C's strlen would run to the trailing
    // sentinel, then fail cursor + slen >= len.
    let mut msg = recv(&f, b"abc");
    let err = pq_getmsgstring(f.ctx.mcx(), &mut msg).unwrap_err();
    assert_eq!(err.message(), "invalid string in message");
    let err = pq_getmsgrawstring(&mut msg).unwrap_err();
    assert_eq!(err.message(), "invalid string in message");
}

#[test]
fn getmsgend_rejects_leftover() {
    let f = setup();
    let mut msg = recv(&f, &[1, 2]);
    pq_getmsgbyte(&mut msg).unwrap();
    let err = pq_getmsgend(&msg).unwrap_err();
    assert_eq!(err.message(), "invalid message format");
    assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
}

#[test]
fn enlarge_cap_matches_stringinfo_error() {
    let f = setup();
    let mut buf = pq_beginmessage(f.ctx.mcx(), b'X').unwrap();
    // A request that crosses MaxAllocSize must fail with stringinfo.c's
    // PROGRAM_LIMIT_EXCEEDED "out of memory" before any allocation happens.
    let err = enlarge_string_info(&mut buf, 0x4000_0000).unwrap_err();
    assert_eq!(err.message(), "out of memory");
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        err.detail(),
        Some("Cannot enlarge string buffer containing 0 bytes by 1073741824 more bytes.")
    );
}
