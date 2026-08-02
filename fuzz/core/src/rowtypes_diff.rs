//! rowtypes_diff: differential fuzz driver — shipped Rust `adt_rowtypes` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_rowtypes_io.c). Crate under test: crates/backend/utils/adt/rowtypes.
//!
//! Comparison planes per arm: value (tuple image bytes / out cstring / send
//! bytes / cmp int / eq bool / hash u32/u64 / larger-smaller pick), error
//! verdict, and errcode class (sqlstate mapped to the oracle's small class
//! constants; message text out of scope).
//!
//! SEAM PINS (environment, not computation): the typcache record registry is
//! loaded with a 5-descriptor menu, and the io/cmp/hash dispatch seams are
//! pinned to the local codecs below. Every codec is transcribed IDENTICALLY
//! in csrc/pg_rowtypes_io.c SECTION D (the "codec contract" comment there is
//! the source of truth); asymmetry between the two transcriptions is a
//! harness bug, never a divergence. This realizes the crate's carve: typcache
//! per-column io/cmp dispatch stays out of scope, the record header logic +
//! literal parsing + framing + comparison loops (all shipped rowtypes code)
//! are what the differential exercises.
//!
//! Descriptor menu (typmod = registration index, mirrored in the C oracle):
//!   0: (text, text)             3: (int4, faketype)    5: (bool, int2, int8)
//!   1: (int4, text)             4: (text)              6: (fix8, bool)
//!   2: (text, [dropped], text)
//! `faketype` (oid 7777) has text io but no cmp/hash/eq support — it
//! witnesses the could-not-identify-function error arms; bool/int2/int8 and
//! the fixed-len-8 BY-REF `fix8` (oid 7778, hex io codec) likewise carry no
//! support fns and exist for the datum_image byval-width / fixed-byref arms.
//!
//! Input layout: [sel][flags][payload...]
//!   sel % 22 = arm (see dispatch); flags: bits 0-2 descriptor (%7),
//!   bit 3 = soft escontext mode (record_in only), bit 4 = details_wanted,
//!   bit 5 = anonymous-record typmod (-1) for record_in/record_recv (the
//!   not-implemented error arms).
//!
//! SKIPPED rows / carves (documented, executable where applicable):
//!   - TOASTed (external/compressed) record inputs: unreachable in-harness
//!     (both sides' detoast seams are identity; C oracle aborts if reached).
//!   - record_eq/ne/lt/gt/le/ge/btrecordcmp: ALSO proved in proofs/records
//!     (per-descriptor Kani theorems); fuzzed here as arms 10-16 for line
//!     coverage of the shipped wrappers + core loops over this menu.
//!   - embedded-NUL literals: a cstring input cannot carry an interior NUL
//!     on either side; the payload is truncated at the first NUL byte.

use alloc::vec;
use alloc::vec::Vec;
use core::ffi::c_char;
use core::ffi::c_int;
use core::ffi::c_uchar;

extern crate alloc;
extern crate std;

use datum::Datum;
use types_core::Oid;
use types_error::{PgResult, SqlState};
use types_fmgr::{ErrorSaveNode, FmgrInfo};

extern "C" {
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_record_in(desc: c_int, soft: c_int, literal: *const c_char,
                         out: *mut c_uchar, outlen: *mut c_int) -> c_int;
    fn pg_diff_record_out(img: *const c_uchar, imglen: c_int,
                          out: *mut c_uchar, outlen: *mut c_int) -> c_int;
    fn pg_diff_record_recv(desc: c_int, wire: *const c_uchar, wirelen: c_int,
                           out: *mut c_uchar, outlen: *mut c_int) -> c_int;
    fn pg_diff_record_send(img: *const c_uchar, imglen: c_int,
                           out: *mut c_uchar, outlen: *mut c_int) -> c_int;
    fn pg_diff_record_image_cmp(img1: *const c_uchar, len1: c_int,
                                img2: *const c_uchar, len2: c_int,
                                cmp_out: *mut c_int) -> c_int;
    fn pg_diff_record_image_eq(img1: *const c_uchar, len1: c_int,
                               img2: *const c_uchar, len2: c_int,
                               eq_out: *mut c_int) -> c_int;
    fn pg_diff_hash_record(img: *const c_uchar, imglen: c_int, h: *mut u32) -> c_int;
    fn pg_diff_hash_record_extended(img: *const c_uchar, imglen: c_int,
                                    seed: u64, h: *mut u64) -> c_int;
    fn pg_diff_record_larger(img1: *const c_uchar, len1: c_int,
                             img2: *const c_uchar, len2: c_int,
                             which: *mut c_int) -> c_int;
    fn pg_diff_record_smaller(img1: *const c_uchar, len1: c_int,
                              img2: *const c_uchar, len2: c_int,
                              which: *mut c_int) -> c_int;
    fn pg_diff_form_record(desc: c_int, fields: *const *const c_uchar,
                           fieldlens: *const c_int, isnull: *const c_int,
                           out: *mut c_uchar, outlen: *mut c_int) -> c_int;
    fn pg_diff_record_cmpfam(which: c_int, img1: *const c_uchar, len1: c_int,
                             img2: *const c_uchar, len2: c_int,
                             val_out: *mut c_int) -> c_int;
    fn pg_diff_record_imagefam(which: c_int, img1: *const c_uchar, len1: c_int,
                               img2: *const c_uchar, len2: c_int,
                               val_out: *mut c_int) -> c_int;
}

// ---------------------------------------------------------------------------
// Pinned environment: codec oids + type oids (the codec contract; see the
// C oracle SECTION D comment for the algorithm-of-record of each codec).
// ---------------------------------------------------------------------------

const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const FAKETYPE: Oid = 7777;
const BOOLOID: Oid = 16;
const INT2OID: Oid = 21;
const INT8OID: Oid = 20;
const FIX8TYPE: Oid = 7778;

const MYTEXTIN: Oid = 91001;
const MYTEXTOUT: Oid = 91002;
const MYTEXTRECV: Oid = 91003;
const MYTEXTSEND: Oid = 91004;
const MYINT4IN: Oid = 91011;
const MYINT4OUT: Oid = 91012;
const MYINT4RECV: Oid = 91013;
const MYINT4SEND: Oid = 91014;
const MYINT4CMP: Oid = 91021;
const MYTEXTCMP: Oid = 91022;
const MYINT4HASH: Oid = 91031;
const MYINT4HASHEXT: Oid = 91032;
const MYTEXTHASH: Oid = 91033;
const MYTEXTHASHEXT: Oid = 91034;
const MYINT4EQ: Oid = 91023;
const MYTEXTEQ: Oid = 91024;
const MYBOOLIN: Oid = 91041;
const MYBOOLOUT: Oid = 91042;
const MYBOOLRECV: Oid = 91043;
const MYBOOLSEND: Oid = 91044;
const MYINT2IN: Oid = 91051;
const MYINT2OUT: Oid = 91052;
const MYINT2RECV: Oid = 91053;
const MYINT2SEND: Oid = 91054;
const MYINT8IN: Oid = 91061;
const MYINT8OUT: Oid = 91062;
const MYINT8RECV: Oid = 91063;
const MYINT8SEND: Oid = 91064;
const MYFIX8IN: Oid = 91071;
const MYFIX8OUT: Oid = 91072;
const MYFIX8RECV: Oid = 91073;
const MYFIX8SEND: Oid = 91074;
// eq-operator oids (typcache: amop strategy-3 member -> operator -> oprcode)
const INT4EQ_OPR: Oid = 30001;
const TEXTEQ_OPR: Oid = 30003;

const BTREE_AM: Oid = 403;
const HASH_AM: Oid = 405;

const NDESC: usize = 7;

type Fcinfo = types_fmgr::FunctionCallInfoBaseData;

// varlena/text helpers ------------------------------------------------------

fn text_datum(mcx: mcx::Mcx<'_>, payload: &[u8]) -> PgResult<Datum> {
    let total = datum::VARHDRSZ + payload.len();
    let mut img = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut img, &datum::varlena::set_varsize_4b(total))?;
    mcx::vec_append_bytes(&mut img, payload)?;
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    Ok(d)
}

/// VARDATA_ANY/VARSIZE_ANY_EXHDR over a (possibly short-header) varlena.
fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena datum built by this harness or deformed out of
    // a tuple this harness built.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { datum::VARHDRSZ };
        core::slice::from_raw_parts(p.add(hdr), total - hdr)
    }
}

// Rust-side codecs (contract transcriptions) --------------------------------

fn fc_mytextin(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    text_datum(fcinfo.result_mcx(), s)
}

fn fc_mytextout(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let payload = varlena_payload(fcinfo.arg(0));
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, payload.len() + 1)?;
    mcx::vec_append_bytes(&mut out, payload)?;
    mcx::vec_append_bytes(&mut out, &[0u8])?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

fn fc_mytextrecv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    let n = buf.len() - buf.cursor;
    let bytes = pqformat::pq_getmsgbytes(buf, n)?.to_vec();
    text_datum(fcinfo.result_mcx(), &bytes)
}

fn fc_mytextsend(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // identical byte image to mytextin over the payload
    let payload = varlena_payload(fcinfo.arg(0)).to_vec();
    text_datum(fcinfo.result_mcx(), &payload)
}

#[cold]
fn int4in_invalid() -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error("myint4in: invalid input")
            .with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

fn fc_myint4in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let escontext = unsafe { fcinfo.error_save_node() };
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    let mut acc: i64 = 0;
    let mut ok = !digits.is_empty();
    for &b in digits {
        if !b.is_ascii_digit() || acc > (1i64 << 31) {
            ok = false;
            break;
        }
        acc = acc * 10 + i64::from(b - b'0');
    }
    if ok && ((!neg && acc > 2147483647) || (neg && acc > 2147483648)) {
        ok = false;
    }
    if !ok {
        return match escontext {
            Some(node) => {
                let err = *int4in_invalid();
                if node.ctx.details_wanted() {
                    node.ctx.save(err);
                } else {
                    node.ctx.mark_error_occurred();
                }
                Ok(Datum::null())
            }
            None => Err(int4in_invalid()),
        };
    }
    Ok(Datum::from_i32((if neg { -acc } else { acc }) as i32))
}

fn fc_myint4out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i32();
    let s = alloc::format!("{v}\0");
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

#[cold]
fn int4recv_short() -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error("myint4recv: insufficient data")
            .with_sqlstate(types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
    )
}

fn fc_myint4recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    if buf.len() - buf.cursor < 4 {
        return Err(int4recv_short());
    }
    let bytes = pqformat::pq_getmsgbytes(buf, 4)?;
    let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    Ok(Datum::from_i32(v))
}

fn fc_myint4send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i32();
    let bytes = v.to_be_bytes();
    text_datum(fcinfo.result_mcx(), &bytes)
}

fn fc_myint4cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = fcinfo.arg(0).as_i32();
    let b = fcinfo.arg(1).as_i32();
    Ok(Datum::from_i32(if a < b { -1 } else { i32::from(a > b) }))
}

fn fc_mytextcmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = varlena_payload(fcinfo.arg(0));
    let b = varlena_payload(fcinfo.arg(1));
    let n = a.len().min(b.len());
    let mut c = match a[..n].cmp(&b[..n]) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => 0,
    };
    if c == 0 && a.len() != b.len() {
        c = if a.len() < b.len() { -1 } else { 1 };
    }
    Ok(Datum::from_i32(c))
}

fn fc_myint4hash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(hashfn::hash_bytes_uint32(fcinfo.arg(0).as_i32() as u32)))
}

fn fc_myint4hashext(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u64(hashfn::hash_bytes_uint32_extended(
        fcinfo.arg(0).as_i32() as u32,
        fcinfo.arg(1).as_u64(),
    )))
}

fn fc_mytexthash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(hashfn::hash_bytes(varlena_payload(fcinfo.arg(0)))))
}

fn fc_mytexthashext(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u64(hashfn::hash_bytes_extended(
        varlena_payload(fcinfo.arg(0)),
        fcinfo.arg(1).as_u64(),
    )))
}

fn fc_myint4eq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(fcinfo.arg(0).as_i32() == fcinfo.arg(1).as_i32()))
}

fn fc_mytexteq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = varlena_payload(fcinfo.arg(0));
    let b = varlena_payload(fcinfo.arg(1));
    Ok(Datum::from_bool(a == b))
}

#[cold]
fn boolin_invalid() -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error("myboolin: invalid input")
            .with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

fn fc_myboolin(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let escontext = unsafe { fcinfo.error_save_node() };
    match s {
        b"t" => Ok(Datum::from_bool(true)),
        b"f" => Ok(Datum::from_bool(false)),
        _ => match escontext {
            Some(node) => {
                let err = *boolin_invalid();
                if node.ctx.details_wanted() {
                    node.ctx.save(err);
                } else {
                    node.ctx.mark_error_occurred();
                }
                Ok(Datum::null())
            }
            None => Err(boolin_invalid()),
        },
    }
}

fn fc_myboolout(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let c = if fcinfo.arg(0).as_bool() { b't' } else { b'f' };
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, 2)?;
    mcx::vec_append_bytes(&mut out, &[c, 0])?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

#[cold]
fn recv_short(what: &'static str) -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error(alloc::format!("{what}: insufficient data"))
            .with_sqlstate(types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
    )
}

fn fc_myboolrecv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    if buf.len() - buf.cursor < 1 {
        return Err(recv_short("myboolrecv"));
    }
    let b = pqformat::pq_getmsgbytes(buf, 1)?[0];
    Ok(Datum::from_bool(b != 0))
}

fn fc_myboolsend(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let b = [u8::from(fcinfo.arg(0).as_bool())];
    text_datum(fcinfo.result_mcx(), &b)
}

fn fc_myint2in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let escontext = unsafe { fcinfo.error_save_node() };
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    let mut acc: i64 = 0;
    let mut ok = !digits.is_empty();
    for &b in digits {
        if !b.is_ascii_digit() || acc > (1i64 << 31) {
            ok = false;
            break;
        }
        acc = acc * 10 + i64::from(b - b'0');
    }
    if ok && ((!neg && acc > 32767) || (neg && acc > 32768)) {
        ok = false;
    }
    if !ok {
        return match escontext {
            Some(node) => {
                let err = *int4in_invalid();
                if node.ctx.details_wanted() {
                    node.ctx.save(err);
                } else {
                    node.ctx.mark_error_occurred();
                }
                Ok(Datum::null())
            }
            None => Err(int4in_invalid()),
        };
    }
    Ok(Datum::from_i16((if neg { -acc } else { acc }) as i16))
}

fn fc_myint2out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i16();
    let s = alloc::format!("{v}\0");
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

fn fc_myint2recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    if buf.len() - buf.cursor < 2 {
        return Err(recv_short("myint2recv"));
    }
    let bytes = pqformat::pq_getmsgbytes(buf, 2)?;
    Ok(Datum::from_i16(i16::from_be_bytes([bytes[0], bytes[1]])))
}

fn fc_myint2send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i16();
    text_datum(fcinfo.result_mcx(), &v.to_be_bytes())
}

fn fc_myint8in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // Contract: optional '-', 1..18 digits (the 18-digit cap IS the codec
    // contract, not int8in semantics).
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let escontext = unsafe { fcinfo.error_save_node() };
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    let ok = !digits.is_empty()
        && digits.len() <= 18
        && digits.iter().all(u8::is_ascii_digit);
    if !ok {
        return match escontext {
            Some(node) => {
                let err = *int4in_invalid();
                if node.ctx.details_wanted() {
                    node.ctx.save(err);
                } else {
                    node.ctx.mark_error_occurred();
                }
                Ok(Datum::null())
            }
            None => Err(int4in_invalid()),
        };
    }
    let mut acc: i64 = 0;
    for &b in digits {
        acc = acc * 10 + i64::from(b - b'0');
    }
    Ok(Datum::from_i64(if neg { -acc } else { acc }))
}

fn fc_myint8out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i64();
    let s = alloc::format!("{v}\0");
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

fn fc_myint8recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    if buf.len() - buf.cursor < 8 {
        return Err(recv_short("myint8recv"));
    }
    let b = pqformat::pq_getmsgbytes(buf, 8)?;
    Ok(Datum::from_i64(i64::from_be_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ])))
}

fn fc_myint8send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = fcinfo.arg(0).as_i64();
    text_datum(fcinfo.result_mcx(), &v.to_be_bytes())
}

/// 8-byte fixed-length BY-REF buffer in mcx; datum = pointer.
fn fix8_datum(mcx: mcx::Mcx<'_>, bytes: &[u8]) -> PgResult<Datum> {
    let mut v = [0u8; 8];
    let n = bytes.len().min(8);
    v[..n].copy_from_slice(&bytes[..n]);
    let mut out = mcx::vec_with_capacity_in(mcx, 8)?;
    mcx::vec_append_bytes(&mut out, &v)?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

fn fc_myfix8in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of an input fn is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    fix8_datum(fcinfo.result_mcx(), s)
}

fn fc_myfix8out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let p = fcinfo.arg(0).as_usize() as *const u8;
    // SAFETY: a live fix8 datum is an 8-byte by-ref buffer.
    let v = unsafe { core::slice::from_raw_parts(p, 8) };
    const HX: &[u8; 16] = b"0123456789abcdef";
    let mut out16 = [0u8; 17];
    for (i, &b) in v.iter().enumerate() {
        out16[2 * i] = HX[(b >> 4) as usize];
        out16[2 * i + 1] = HX[(b & 0xf) as usize];
    }
    let mcx = fcinfo.result_mcx();
    let mut out = mcx::vec_with_capacity_in(mcx, 17)?;
    mcx::vec_append_bytes(&mut out, &out16)?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

fn fc_myfix8recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of a recv fn is a live &mut StringInfo.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut stringinfo::StringInfo<'_>) };
    if buf.len() - buf.cursor < 8 {
        return Err(recv_short("myfix8recv"));
    }
    let b = pqformat::pq_getmsgbytes(buf, 8)?.to_vec();
    fix8_datum(fcinfo.result_mcx(), &b)
}

fn fc_myfix8send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let p = fcinfo.arg(0).as_usize() as *const u8;
    // SAFETY: a live fix8 datum is an 8-byte by-ref buffer.
    let v = unsafe { core::slice::from_raw_parts(p, 8) };
    text_datum(fcinfo.result_mcx(), v)
}

// Seam installation + descriptor registration --------------------------------

fn io_shape(oid: Oid, input: Oid, output: Oid, recv: Oid, send: Oid,
            typlen: i16, byval: bool, align: u8) -> syscache_seams::PgTypeIoShape {
    syscache_seams::PgTypeIoShape {
        oid,
        typinput: input,
        typoutput: output,
        typreceive: recv,
        typsend: send,
        typmodin: types_core::primitive::InvalidOid,
        typmodout: types_core::primitive::InvalidOid,
        typelem: types_core::primitive::InvalidOid,
        typlen,
        typbyval: byval,
        typalign: align as i8,
        typdelim: b',' as i8,
        typisdefined: true,
    }
}

fn tc_shape(typlen: i16, byval: bool, storage: i8, align: u8) -> syscache_seams::PgTypeTypcacheShape {
    syscache_seams::PgTypeTypcacheShape {
        typname: Default::default(),
        typlen,
        typbyval: byval,
        typalign: align as i8,
        typstorage: storage,
        typtype: b'b' as i8,
        typisdefined: true,
        typrelid: types_core::primitive::InvalidOid,
        typsubscript: types_core::primitive::InvalidOid,
        typelem: types_core::primitive::InvalidOid,
        typarray: types_core::primitive::InvalidOid,
        typcollation: types_core::primitive::InvalidOid,
    }
}

fn att(name: &str, num: i16, typid: Oid, typlen: i16, byval: bool, storage: u8,
       dropped: bool) -> types_tuple::FormData_pg_attribute {
    att_a(name, num, typid, typlen, byval, storage, dropped, b'i')
}

#[allow(clippy::too_many_arguments)]
fn att_a(name: &str, num: i16, typid: Oid, typlen: i16, byval: bool, storage: u8,
         dropped: bool, align: u8) -> types_tuple::FormData_pg_attribute {
    let mut a = types_tuple::FormData_pg_attribute::default();
    a.attname.namestrcpy(name);
    a.attnum = num;
    a.atttypid = typid;
    a.attlen = typlen;
    a.attbyval = byval;
    a.attalign = align as i8;
    a.attstorage = storage as i8;
    a.atttypmod = -1;
    a.attisdropped = dropped;
    a
}

static INSTALL: std::sync::Once = std::sync::Once::new();
/// Seams are process-global set-once and array_userfuncs_diff pins the same
/// ones with ITS oid map: exactly one diff module can own the environment
/// per process. The fuzz binaries are one-target-per-process, so ownership
/// is always ours there; under `cargo test` whichever module installs first
/// owns it and the other's drivers become no-ops (run the rowtypes tests
/// with `cargo test rowtypes_diff` when the full suite raced the seams).
static OWNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn install() -> bool {
    INSTALL.call_once(|| {
        use types_core::primitive::InvalidOid;
        if syscache_seams::pg_type_io_shape::is_installed()
            || syscache_seams::lookup_pg_type_typcache_shape::is_installed()
            || fmgr_seams::fmgr_info::is_installed()
        {
            return; // another diff module owns the environment
        }
        OWNED.store(true, std::sync::atomic::Ordering::Relaxed);
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(match typid {
                TEXTOID => Some(io_shape(TEXTOID, MYTEXTIN, MYTEXTOUT, MYTEXTRECV, MYTEXTSEND, -1, false, b'i')),
                INT4OID => Some(io_shape(INT4OID, MYINT4IN, MYINT4OUT, MYINT4RECV, MYINT4SEND, 4, true, b'i')),
                FAKETYPE => Some(io_shape(FAKETYPE, MYTEXTIN, MYTEXTOUT, MYTEXTRECV, MYTEXTSEND, -1, false, b'i')),
                BOOLOID => Some(io_shape(BOOLOID, MYBOOLIN, MYBOOLOUT, MYBOOLRECV, MYBOOLSEND, 1, true, b'c')),
                INT2OID => Some(io_shape(INT2OID, MYINT2IN, MYINT2OUT, MYINT2RECV, MYINT2SEND, 2, true, b's')),
                INT8OID => Some(io_shape(INT8OID, MYINT8IN, MYINT8OUT, MYINT8RECV, MYINT8SEND, 8, true, b'd')),
                FIX8TYPE => Some(io_shape(FIX8TYPE, MYFIX8IN, MYFIX8OUT, MYFIX8RECV, MYFIX8SEND, 8, false, b'd')),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                TEXTOID => Some(tc_shape(-1, false, b'x' as i8, b'i')),
                INT4OID => Some(tc_shape(4, true, b'p' as i8, b'i')),
                FAKETYPE => Some(tc_shape(-1, false, b'x' as i8, b'i')),
                BOOLOID => Some(tc_shape(1, true, b'p' as i8, b'c')),
                INT2OID => Some(tc_shape(2, true, b'p' as i8, b's')),
                INT8OID => Some(tc_shape(8, true, b'p' as i8, b'd')),
                FIX8TYPE => Some(tc_shape(8, false, b'p' as i8, b'd')),
                _ => None,
            })
        });
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4OID, BTREE_AM) => 10001,
                (INT4OID, HASH_AM) => 10002,
                (TEXTOID, BTREE_AM) => 10003,
                (TEXTOID, HASH_AM) => 10004,
                _ => InvalidOid,
            })
        });
        syscache_seams::lookup_pg_opclass_shape::set(|opcoid| {
            Ok(match opcoid {
                10001 => Some((BTREE_AM, 20001, INT4OID)),
                10002 => Some((HASH_AM, 20002, INT4OID)),
                10003 => Some((BTREE_AM, 20003, TEXTOID)),
                10004 => Some((HASH_AM, 20004, TEXTOID)),
                _ => None,
            }
            .map(|(m, f, i)| syscache_seams::PgOpclassShape {
                opcmethod: m,
                opcfamily: f,
                opcintype: i,
                opckeytype: 0,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(match (opfamily, lefttype, righttype, procnum) {
                (20001, INT4OID, INT4OID, 1) => MYINT4CMP,
                (20002, INT4OID, INT4OID, 1) => MYINT4HASH,
                (20002, INT4OID, INT4OID, 2) => MYINT4HASHEXT,
                (20003, TEXTOID, TEXTOID, 1) => MYTEXTCMP,
                (20004, TEXTOID, TEXTOID, 1) => MYTEXTHASH,
                (20004, TEXTOID, TEXTOID, 2) => MYTEXTHASHEXT,
                _ => InvalidOid,
            })
        });
        // eq-operator resolution (typcache TYPECACHE_EQ_OPR_FINFO):
        // btree opfamily strategy-3 member -> operator -> oprcode codec.
        syscache_seams::lookup_pg_amop_by_strategy::set(
            |opfamily, lefttype, righttype, strategy| {
                Ok(match (opfamily, lefttype, righttype, strategy) {
                    // btree strategy-3 (=) members
                    (20001, INT4OID, INT4OID, 3) => INT4EQ_OPR,
                    (20003, TEXTOID, TEXTOID, 3) => TEXTEQ_OPR,
                    // hash strategy-1 (=) members: the SAME operators (real
                    // catalogs are self-consistent; resolve_hash_proc checks
                    // a determined eq_opr against the hash family's member)
                    (20002, INT4OID, INT4OID, 1) => INT4EQ_OPR,
                    (20004, TEXTOID, TEXTOID, 1) => TEXTEQ_OPR,
                    _ => InvalidOid,
                })
            },
        );
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            let code = match opno {
                INT4EQ_OPR => MYINT4EQ,
                TEXTEQ_OPR => MYTEXTEQ,
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgOperatorShape {
                oprnamespace: InvalidOid,
                oprleft: InvalidOid,
                oprright: InvalidOid,
                oprresult: InvalidOid,
                oprcom: InvalidOid,
                oprnegate: InvalidOid,
                oprcode: code,
                oprrest: InvalidOid,
                oprjoin: InvalidOid,
                oprcanmerge: false,
                oprcanhash: false,
            }))
        });
        fmgr_seams::fmgr_info::set(|oid| {
            let f: types_fmgr::PGFunction = match oid {
                MYTEXTIN => fc_mytextin,
                MYTEXTOUT => fc_mytextout,
                MYTEXTRECV => fc_mytextrecv,
                MYTEXTSEND => fc_mytextsend,
                MYINT4IN => fc_myint4in,
                MYINT4OUT => fc_myint4out,
                MYINT4RECV => fc_myint4recv,
                MYINT4SEND => fc_myint4send,
                MYINT4CMP => fc_myint4cmp,
                MYTEXTCMP => fc_mytextcmp,
                MYINT4HASH => fc_myint4hash,
                MYINT4HASHEXT => fc_myint4hashext,
                MYTEXTHASH => fc_mytexthash,
                MYTEXTHASHEXT => fc_mytexthashext,
                MYINT4EQ => fc_myint4eq,
                MYTEXTEQ => fc_mytexteq,
                MYBOOLIN => fc_myboolin,
                MYBOOLOUT => fc_myboolout,
                MYBOOLRECV => fc_myboolrecv,
                MYBOOLSEND => fc_myboolsend,
                MYINT2IN => fc_myint2in,
                MYINT2OUT => fc_myint2out,
                MYINT2RECV => fc_myint2recv,
                MYINT2SEND => fc_myint2send,
                MYINT8IN => fc_myint8in,
                MYINT8OUT => fc_myint8out,
                MYINT8RECV => fc_myint8recv,
                MYINT8SEND => fc_myint8send,
                MYFIX8IN => fc_myfix8in,
                MYFIX8OUT => fc_myfix8out,
                MYFIX8RECV => fc_myfix8recv,
                MYFIX8SEND => fc_myfix8send,
                _ => std::panic!("fmgr_info: unexpected oid {oid}"),
            };
            Ok(FmgrInfo::new(f, oid, 3, true, false))
        });
        // format_type_be (error-message construction only) probes type
        // visibility; message text is out of scope, visibility is inert.
        namespace_seams::type_is_visible::set(|_typid| Ok(true));
        // typcache cache-invalidation key (identity is fine: cache keying only)
        syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid));
        detoast_seams::detoast_attr::set(|mcx, raw| {
            let mut v = mcx::vec_with_capacity_in(mcx, raw.len())?;
            mcx::vec_append_bytes(&mut v, raw)?;
            Ok(v)
        });
    });
    if !OWNED.load(std::sync::atomic::Ordering::Relaxed) {
        return false;
    }
    // The typcache record registry is thread-native state: registration must
    // happen on EVERY thread that runs the driver (idempotent: equal rows
    // dedupe to the same typmod). libFuzzer is single-threaded; the stable
    // test suite is not.
    std::thread_local! {
        static DESCS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    DESCS.with(|d| {
        if !d.get() {
            register_descs();
            d.set(true);
        }
    });
    true
}

/// Register the 5-descriptor menu; registration order pins typmods 0..4
/// (mirrored by the C oracle's static menu).
fn register_descs() {
    use types_core::catalog::RECORDOID;
    let ctx = mcx::MemoryContext::new("rowtypes_diff_descs");
    let mcx = ctx.mcx();
    let d0 = [att("c1", 1, TEXTOID, -1, false, b'x', false),
              att("c2", 2, TEXTOID, -1, false, b'x', false)];
    let d1 = [att("c1", 1, INT4OID, 4, true, b'p', false),
              att("c2", 2, TEXTOID, -1, false, b'x', false)];
    let d2 = [att("c1", 1, TEXTOID, -1, false, b'x', false),
              att("c2", 2, types_core::primitive::InvalidOid, -1, false, b'x', true),
              att("c3", 3, TEXTOID, -1, false, b'x', false)];
    let d3 = [att("c1", 1, INT4OID, 4, true, b'p', false),
              att("c2", 2, FAKETYPE, -1, false, b'x', false)];
    let d4 = [att("c1", 1, TEXTOID, -1, false, b'x', false)];
    let d5 = [att_a("c1", 1, BOOLOID, 1, true, b'p', false, b'c'),
              att_a("c2", 2, INT2OID, 2, true, b'p', false, b's'),
              att_a("c3", 3, INT8OID, 8, true, b'p', false, b'd')];
    let d6 = [att_a("c1", 1, FIX8TYPE, 8, false, b'p', false, b'd'),
              att_a("c2", 2, BOOLOID, 1, true, b'p', false, b'c')];
    let menus: [&[types_tuple::FormData_pg_attribute]; NDESC] =
        [&d0, &d1, &d2, &d3, &d4, &d5, &d6];
    for (i, atts) in menus.iter().enumerate() {
        let mut td = tupdesc::CreateTupleDesc(mcx, atts).expect("CreateTupleDesc");
        td.tdtypeid = RECORDOID;
        td.tdtypmod = -1;
        typcache::assign_record_type_typmod(&mut td).expect("register record type");
        assert_eq!(td.tdtypmod, i as i32, "descriptor menu typmod drift");
    }
}

// Error-class mapping (mirror of the oracle's class constants) --------------

fn class_of(ss: SqlState) -> i32 {
    use types_error as te;
    if ss == te::ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if ss == te::ERRCODE_FEATURE_NOT_SUPPORTED {
        2
    } else if ss == te::ERRCODE_DATATYPE_MISMATCH {
        3
    } else if ss == te::ERRCODE_INVALID_BINARY_REPRESENTATION {
        4
    } else if ss == te::ERRCODE_UNDEFINED_FUNCTION {
        5
    } else if ss == te::ERRCODE_TOO_MANY_COLUMNS {
        6
    } else if ss == te::ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        8
    } else {
        7
    }
}

fn c_errcode() -> i32 {
    // SAFETY: plain TLS read.
    unsafe { pg_diff_errcode_get() }
}

// Record-image helpers -------------------------------------------------------

fn image_of<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live composite datum built by record_in/recv/heap_form_tuple.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, total)
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let start = self.i.min(self.b.len());
        let end = (self.i + n).min(self.b.len());
        self.i = self.i.saturating_add(n);
        &self.b[start..end]
    }
}

/// One decoded field: None = SQL NULL, Some(bytes) = payload. Fixed-width
/// columns consume exactly their width little-endian (bool 1 / int2 2 /
/// int4 4 / int8+fix8 8); text-io columns a 1-byte length + data.
type Fields = Vec<Option<Vec<u8>>>;

#[derive(Clone, Copy)]
enum ColKind {
    Text,
    Int4,
    Bool,
    Int2,
    Int8,
    Fix8,
    Dropped,
}

fn desc_shapes(desc: usize) -> &'static [ColKind] {
    use ColKind::*;
    match desc {
        0 => &[Text, Text],
        1 => &[Int4, Text],
        2 => &[Text, Dropped, Text],
        3 => &[Int4, Text],
        4 => &[Text],
        5 => &[Bool, Int2, Int8],
        _ => &[Fix8, Bool],
    }
}

fn decode_fields(cur: &mut Cursor<'_>, desc: usize) -> Fields {
    desc_shapes(desc)
        .iter()
        .map(|s| {
            let w = match s {
                ColKind::Dropped => return None, // dropped column: always null
                ColKind::Bool => 1,
                ColKind::Int2 => 2,
                ColKind::Int4 => 4,
                ColKind::Int8 | ColKind::Fix8 => 8,
                ColKind::Text => 0,
            };
            if cur.u8() & 1 == 0 {
                None
            } else if w > 0 {
                Some(cur.bytes(w).to_vec())
            } else {
                let n = cur.u8() as usize;
                Some(cur.bytes(n).to_vec())
            }
        })
        .collect()
}

/// Build a record image via the SHIPPED Rust heap_form_tuple, then cross-check
/// byte equality against the C oracle's heap_form_tuple (a compared plane in
/// its own right). Returns the image bytes.
fn build_record(mcx: mcx::Mcx<'_>, desc: usize, fields: &Fields) -> Option<Vec<u8>> {
    use types_core::catalog::RECORDOID;
    let tupdesc = typcache::lookup_rowtype_tupdesc_copy(mcx, RECORDOID, desc as i32).ok()?;
    let n = tupdesc.natts as usize;
    let mut values = mcx::vec_with_capacity_in(mcx, n).ok()?;
    let mut nulls = mcx::vec_with_capacity_in(mcx, n).ok()?;
    for i in 0..n {
        let attr = &tupdesc.attrs[i];
        match (&fields[i], attr.attisdropped) {
            (Some(bytes), false) => {
                if attr.attbyval {
                    // min(fieldlen, attlen) LE bytes into a zeroed word of
                    // the column width (contract: mirrored by the C
                    // oracle's pg_diff_form_record staging)
                    let mut v = [0u8; 8];
                    let w = attr.attlen as usize;
                    let m = bytes.len().min(w);
                    v[..m].copy_from_slice(&bytes[..m]);
                    values.push(match attr.attlen {
                        1 => Datum::from_bool(v[0] & 1 != 0),
                        2 => Datum::from_i16(i16::from_le_bytes([v[0], v[1]])),
                        8 => Datum::from_i64(i64::from_le_bytes(v)),
                        _ => Datum::from_i32(i32::from_le_bytes([v[0], v[1], v[2], v[3]])),
                    });
                } else if attr.attlen > 0 {
                    values.push(fix8_datum(mcx, bytes).ok()?);
                } else {
                    values.push(text_datum(mcx, bytes).ok()?);
                }
                nulls.push(false);
            }
            _ => {
                values.push(Datum::null());
                nulls.push(true);
            }
        }
    }
    let tuple = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls).ok()?;
    let img = tuple.image().to_vec();

    // Cross-check: C heap_form_tuple over the same fields must agree.
    let mut ptrs: Vec<*const c_uchar> = Vec::with_capacity(n);
    let mut lens: Vec<c_int> = Vec::with_capacity(n);
    let mut isnull: Vec<c_int> = Vec::with_capacity(n);
    let empty: [u8; 1] = [0];
    for f in fields {
        match f {
            Some(b) => {
                ptrs.push(if b.is_empty() { empty.as_ptr() } else { b.as_ptr() });
                lens.push(b.len() as c_int);
                isnull.push(0);
            }
            None => {
                ptrs.push(empty.as_ptr());
                lens.push(0);
                isnull.push(1);
            }
        }
    }
    let mut cbuf = vec![0u8; img.len() + 64];
    let mut clen: c_int = cbuf.len() as c_int;
    // SAFETY: pointers live for the call; C writes at most clen bytes.
    let st = unsafe {
        pg_diff_form_record(desc as c_int, ptrs.as_ptr(), lens.as_ptr(),
                            isnull.as_ptr(), cbuf.as_mut_ptr(), &mut clen)
    };
    assert_eq!(st, 0, "C heap_form_tuple failed where Rust succeeded");
    assert_eq!(&cbuf[..clen as usize], &img[..],
               "heap_form_tuple image divergence (desc {desc})");
    Some(img)
}

// fc-call plumbing ------------------------------------------------------------

/// Run an adt_rowtypes fc_* wrapper with its own FmgrInfo (fn_extra memo
/// alive across `calls` invocations); returns the last call's result.
fn run_fc<const N: usize>(
    f: types_fmgr::PGFunction,
    mcx: mcx::Mcx<'_>,
    args: &[Datum; N],
    mut esc: Option<&mut ErrorSaveNode>,
    calls: usize,
) -> PgResult<Datum> {
    let mut flinfo = FmgrInfo::new(f, 0, N as i16, true, false);
    let mut last: PgResult<Datum> = Ok(Datum::null());
    for _ in 0..calls {
        let mut fci = types_fmgr::LocalFcinfo::<N>::new(0);
        // SAFETY: the context owning `mcx` outlives this call.
        unsafe { fci.set_result_mcx(mcx) };
        for (i, a) in args.iter().enumerate() {
            fci.set_arg(i, *a);
        }
        if let Some(node) = esc.as_deref_mut() {
            fci.context = node.fm_node_ptr();
        }
        last = f(Some(&mut flinfo), &mut fci);
    }
    last
}

/// Map a Rust fc result (+ soft node) to (status, class): status 0 ok / 1 err.
fn verdict(r: &PgResult<Datum>, esc: Option<&ErrorSaveNode>) -> (i32, Option<i32>) {
    match r {
        Err(e) => (1, Some(class_of(e.sqlstate))),
        Ok(_) => {
            if let Some(node) = esc {
                if node.ctx.error_occurred() {
                    let class = node.ctx.error().map(|e| class_of(e.sqlstate));
                    return (1, class);
                }
            }
            (0, None)
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn rowtypes_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if !install() {
        return; // seams owned by a sibling diff module in this process
    }
    let [sel, flags, payload @ ..] = data else { return };
    let desc = (*flags & 0x07) as usize % NDESC;
    let soft = *flags & 0x08 != 0;
    let details = *flags & 0x10 != 0;
    let anon = *flags & 0x20 != 0;
    match sel % 22 {
        0 => record_in_diff(desc, anon, soft, details, payload),
        1 => record_out_diff(desc, payload),
        2 => record_recv_diff(desc, anon, payload),
        3 => record_send_diff(desc, payload),
        4 => two_record_diff(desc, payload, TwoRecArm::ImageCmp),
        5 => two_record_diff(desc, payload, TwoRecArm::ImageEq),
        6 => hash_diff(desc, payload, false),
        7 => hash_diff(desc, payload, true),
        8 => two_record_diff(desc, payload, TwoRecArm::Larger),
        9 => two_record_diff(desc, payload, TwoRecArm::Smaller),
        n @ 10..=16 => cmpfam_diff(desc, payload, (n - 10) as i32),
        n => imagefam_diff(desc, payload, (n - 17) as i32),
    }
}

/// record_eq/ne/lt/gt/le/ge/btrecordcmp (which = 0..=6), value plane =
/// bool (or the int32 cmp for btrecordcmp).
fn cmpfam_diff(desc1: usize, payload: &[u8], which: i32) {
    let mut cur = Cursor { b: payload, i: 0 };
    let desc2 = (cur.u8() & 0x07) as usize % NDESC;
    let f1 = decode_fields(&mut cur, desc1);
    let f2 = decode_fields(&mut cur, desc2);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let (Some(i1), Some(i2)) = (build_record(mcx, desc1, &f1), build_record(mcx, desc2, &f2))
    else {
        return;
    };
    let d1 = Datum::from_usize(i1.as_ptr() as usize);
    let d2 = Datum::from_usize(i2.as_ptr() as usize);
    let args = [d1, d2];
    let (rf, name): (types_fmgr::PGFunction, &str) = match which {
        0 => (adt_rowtypes::fc_record_eq, "record_eq"),
        1 => (adt_rowtypes::fc_record_ne, "record_ne"),
        2 => (adt_rowtypes::fc_record_lt, "record_lt"),
        3 => (adt_rowtypes::fc_record_gt, "record_gt"),
        4 => (adt_rowtypes::fc_record_le, "record_le"),
        5 => (adt_rowtypes::fc_record_ge, "record_ge"),
        _ => (adt_rowtypes::fc_btrecordcmp, "btrecordcmp"),
    };
    let r = run_fc::<2>(rf, mcx, &args, None, 2);
    let (rst, rclass) = verdict(&r, None);

    let mut cval: c_int = 0;
    // SAFETY: image buffers live for the call.
    let cst = unsafe {
        pg_diff_record_cmpfam(which, i1.as_ptr(), i1.len() as c_int,
                              i2.as_ptr(), i2.len() as c_int, &mut cval)
    };
    assert!(cst >= 0, "C harness internal failure {cst} ({name})");
    assert_eq!(rst, cst,
               "{name} verdict divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, c_errcode(), "{name} errcode divergence: desc1={desc1} desc2={desc2}");
        }
        return;
    }
    let rd = r.unwrap();
    let rval: c_int = if which == 6 {
        rd.as_i32()
    } else {
        c_int::from(rd.as_usize() != 0)
    };
    assert_eq!(rval, cval,
               "{name} value divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
}

/// record_image_ne/lt/gt/le/ge (which = 0..=4), value plane = bool.
fn imagefam_diff(desc1: usize, payload: &[u8], which: i32) {
    let mut cur = Cursor { b: payload, i: 0 };
    let desc2 = (cur.u8() & 0x07) as usize % NDESC;
    let f1 = decode_fields(&mut cur, desc1);
    let f2 = decode_fields(&mut cur, desc2);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let (Some(i1), Some(i2)) = (build_record(mcx, desc1, &f1), build_record(mcx, desc2, &f2))
    else {
        return;
    };
    let d1 = Datum::from_usize(i1.as_ptr() as usize);
    let d2 = Datum::from_usize(i2.as_ptr() as usize);
    let args = [d1, d2];
    let (rf, name): (types_fmgr::PGFunction, &str) = match which {
        0 => (adt_rowtypes::fc_record_image_ne, "record_image_ne"),
        1 => (adt_rowtypes::fc_record_image_lt, "record_image_lt"),
        2 => (adt_rowtypes::fc_record_image_gt, "record_image_gt"),
        3 => (adt_rowtypes::fc_record_image_le, "record_image_le"),
        _ => (adt_rowtypes::fc_record_image_ge, "record_image_ge"),
    };
    let r = run_fc::<2>(rf, mcx, &args, None, 2);
    let (rst, rclass) = verdict(&r, None);

    let mut cval: c_int = 0;
    // SAFETY: image buffers live for the call.
    let cst = unsafe {
        pg_diff_record_imagefam(which, i1.as_ptr(), i1.len() as c_int,
                                i2.as_ptr(), i2.len() as c_int, &mut cval)
    };
    assert!(cst >= 0, "C harness internal failure {cst} ({name})");
    assert_eq!(rst, cst,
               "{name} verdict divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, c_errcode(), "{name} errcode divergence: desc1={desc1} desc2={desc2}");
        }
        return;
    }
    assert_eq!(c_int::from(r.unwrap().as_usize() != 0), cval,
               "{name} value divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
}

fn record_in_diff(desc: usize, anon: bool, soft: bool, details: bool, payload: &[u8]) {
    // anonymous mode: typmod -1 witnesses the not-implemented arms
    let tm: i32 = if anon { -1 } else { desc as i32 };
    // cstring truncation at the first NUL — identical view on both sides
    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    let mut lit = payload[..end].to_vec();
    lit.push(0);
    let lit_c = core::ffi::CStr::from_bytes_with_nul(&lit).unwrap();

    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    use types_core::catalog::RECORDOID;
    let args = [
        Datum::from_usize(lit_c.as_ptr() as usize),
        Datum::from_oid(RECORDOID),
        Datum::from_i32(tm),
    ];
    let (rst, rclass, rimg) = if soft {
        let mut node = ErrorSaveNode::new(details);
        let r = run_fc::<3>(adt_rowtypes::fc_record_in, mcx, &args, Some(&mut node), 1);
        let (st, class) = verdict(&r, Some(&node));
        let img = (st == 0).then(|| r.as_ref().ok().map(|d| image_of(*d).to_vec())).flatten();
        (st, class, img)
    } else {
        let r = run_fc::<3>(adt_rowtypes::fc_record_in, mcx, &args, None, 2);
        let (st, class) = verdict(&r, None);
        let img = (st == 0).then(|| r.as_ref().ok().map(|d| image_of(*d).to_vec())).flatten();
        (st, class, img)
    };

    let mut out = vec![0u8; 1 << 16];
    let mut outlen: c_int = out.len() as c_int;
    // SAFETY: buffers live for the call.
    let cst = unsafe {
        pg_diff_record_in(tm, c_int::from(soft), lit_c.as_ptr(),
                          out.as_mut_ptr(), &mut outlen)
    };
    assert!(cst >= 0, "C harness internal failure {cst} (record_in)");
    let cclass = c_errcode();

    assert_eq!(rst, cst, "record_in verdict divergence: literal={lit:?} desc={desc} soft={soft}");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, cclass,
                       "record_in errcode divergence: literal={lit:?} desc={desc} soft={soft}");
        }
        return;
    }
    let rimg = rimg.unwrap();
    assert_eq!(rimg.as_slice(), &out[..outlen as usize],
               "record_in image divergence: literal={lit:?} desc={desc}");
    // in -> out roundtrip plane
    out_compare(mcx, &rimg);
}

/// record_out both sides over one image; compares cstrings.
fn out_compare(mcx: mcx::Mcx<'_>, img: &[u8]) {
    let d = Datum::from_usize(img.as_ptr() as usize);
    let r = run_fc::<1>(adt_rowtypes::fc_record_out, mcx, &[d], None, 2);
    let rout = r.expect("Rust record_out failed on a value record_in accepted");
    // SAFETY: record_out returns a NUL-terminated cstring datum.
    let rbytes =
        unsafe { core::ffi::CStr::from_ptr(rout.as_usize() as *const c_char) }.to_bytes();

    let mut out = vec![0u8; 1 << 16];
    let mut outlen: c_int = out.len() as c_int;
    // SAFETY: buffers live for the call.
    let cst = unsafe {
        pg_diff_record_out(img.as_ptr(), img.len() as c_int, out.as_mut_ptr(), &mut outlen)
    };
    assert_eq!(cst, 0, "C record_out failed on a value C record_in accepted");
    let cbytes = &out[..(outlen as usize).saturating_sub(1)]; // strip NUL
    assert_eq!(rbytes, cbytes, "record_out divergence");
}

fn record_out_diff(desc: usize, payload: &[u8]) {
    // build a record from decoded fields, then out-compare (hits record_out
    // with int4/dropped/single-column descriptors independently of arm 0)
    let mut cur = Cursor { b: payload, i: 0 };
    let fields = decode_fields(&mut cur, desc);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let Some(img) = build_record(mcx, desc, &fields) else { return };
    out_compare(mcx, &img);
}

fn record_recv_diff(desc: usize, anon: bool, payload: &[u8]) {
    let tm: i32 = if anon { -1 } else { desc as i32 };
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();

    // Rust side
    let Ok(mut si) = stringinfo::StringInfo::with_capacity_in(mcx, payload.len() + 1) else {
        return;
    };
    if si.append_bytes(payload).is_err() {
        return;
    }
    let args = [
        Datum::from_usize(core::ptr::addr_of_mut!(si) as usize),
        Datum::from_oid(types_core::catalog::RECORDOID),
        Datum::from_i32(tm),
    ];
    let r = run_fc::<3>(adt_rowtypes::fc_record_recv, mcx, &args, None, 1);
    let (rst, rclass) = verdict(&r, None);

    let mut out = vec![0u8; 1 << 16];
    let mut outlen: c_int = out.len() as c_int;
    // SAFETY: buffers live for the call.
    let cst = unsafe {
        pg_diff_record_recv(tm, payload.as_ptr(), payload.len() as c_int,
                            out.as_mut_ptr(), &mut outlen)
    };
    assert!(cst >= 0, "C harness internal failure {cst} (record_recv)");
    let cclass = c_errcode();

    assert_eq!(rst, cst, "record_recv verdict divergence: desc={desc} wire={payload:?}");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, cclass, "record_recv errcode divergence: desc={desc} wire={payload:?}");
        }
        return;
    }
    let rimg = image_of(r.unwrap()).to_vec();
    assert_eq!(rimg.as_slice(), &out[..outlen as usize],
               "record_recv image divergence: desc={desc} wire={payload:?}");
    send_compare(mcx, &rimg);
    // fn_extra memo-hit path: same flinfo, fresh buffer, image must repeat
    let Ok(mut si2) = stringinfo::StringInfo::with_capacity_in(mcx, payload.len() + 1) else {
        return;
    };
    if si2.append_bytes(payload).is_err() {
        return;
    }
    let mut flinfo = FmgrInfo::new(adt_rowtypes::fc_record_recv, 0, 3, true, false);
    let mut img2 = None;
    for _ in 0..2 {
        si2.cursor = 0;
        let mut fci = types_fmgr::LocalFcinfo::<3>::new(0);
        // SAFETY: the context owning `mcx` outlives this call.
        unsafe { fci.set_result_mcx(mcx) };
        fci.set_arg(0, Datum::from_usize(core::ptr::addr_of_mut!(si2) as usize));
        fci.set_arg(1, Datum::from_oid(types_core::catalog::RECORDOID));
        fci.set_arg(2, Datum::from_i32(tm));
        let Ok(rr) = adt_rowtypes::fc_record_recv(Some(&mut flinfo), &mut fci) else { return };
        let cur = image_of(rr).to_vec();
        if let Some(prev) = &img2 {
            assert_eq!(prev, &cur, "record_recv memo-hit image drift: desc={desc}");
        }
        img2 = Some(cur);
    }
}

/// record_send both sides over one image; compares wire bytes.
fn send_compare(mcx: mcx::Mcx<'_>, img: &[u8]) {
    let d = Datum::from_usize(img.as_ptr() as usize);
    let r = run_fc::<1>(adt_rowtypes::fc_record_send, mcx, &[d], None, 2);
    let (rst, rclass) = verdict(&r, None);

    let mut out = vec![0u8; 1 << 16];
    let mut outlen: c_int = out.len() as c_int;
    // SAFETY: buffers live for the call.
    let cst = unsafe {
        pg_diff_record_send(img.as_ptr(), img.len() as c_int, out.as_mut_ptr(), &mut outlen)
    };
    assert!(cst >= 0, "C harness internal failure {cst} (record_send)");
    assert_eq!(rst, cst, "record_send verdict divergence");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, c_errcode(), "record_send errcode divergence");
        }
        return;
    }
    let rd = r.unwrap();
    let rbytes = varlena_payload(rd);
    assert_eq!(rbytes, &out[..outlen as usize], "record_send wire divergence");
}

fn record_send_diff(desc: usize, payload: &[u8]) {
    let mut cur = Cursor { b: payload, i: 0 };
    let fields = decode_fields(&mut cur, desc);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let Some(img) = build_record(mcx, desc, &fields) else { return };
    send_compare(mcx, &img);
    // send -> recv roundtrip: recv(send(x)) must reproduce the image
    let d = Datum::from_usize(img.as_ptr() as usize);
    let Ok(wire_d) = run_fc::<1>(adt_rowtypes::fc_record_send, mcx, &[d], None, 1) else {
        return;
    };
    let wire = varlena_payload(wire_d).to_vec();
    let Ok(mut si) = stringinfo::StringInfo::with_capacity_in(mcx, wire.len() + 1) else {
        return;
    };
    if si.append_bytes(&wire).is_err() {
        return;
    }
    let args = [
        Datum::from_usize(core::ptr::addr_of_mut!(si) as usize),
        Datum::from_oid(types_core::catalog::RECORDOID),
        Datum::from_i32(desc as i32),
    ];
    let rt = run_fc::<3>(adt_rowtypes::fc_record_recv, mcx, &args, None, 1)
        .expect("recv(send(x)) failed");
    assert_eq!(image_of(rt), img.as_slice(), "recv(send(x)) roundtrip mismatch");
}

enum TwoRecArm {
    ImageCmp,
    ImageEq,
    Larger,
    Smaller,
}

fn two_record_diff(desc1: usize, payload: &[u8], arm: TwoRecArm) {
    let mut cur = Cursor { b: payload, i: 0 };
    let desc2 = (cur.u8() & 0x07) as usize % NDESC;
    let f1 = decode_fields(&mut cur, desc1);
    let f2 = decode_fields(&mut cur, desc2);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let (Some(i1), Some(i2)) = (build_record(mcx, desc1, &f1), build_record(mcx, desc2, &f2))
    else {
        return;
    };
    let d1 = Datum::from_usize(i1.as_ptr() as usize);
    let d2 = Datum::from_usize(i2.as_ptr() as usize);
    let args = [d1, d2];

    let (rf, name): (types_fmgr::PGFunction, &str) = match arm {
        TwoRecArm::ImageCmp => (adt_rowtypes::fc_btrecordimagecmp, "btrecordimagecmp"),
        TwoRecArm::ImageEq => (adt_rowtypes::fc_record_image_eq, "record_image_eq"),
        TwoRecArm::Larger => (adt_rowtypes::fc_record_larger, "record_larger"),
        TwoRecArm::Smaller => (adt_rowtypes::fc_record_smaller, "record_smaller"),
    };
    let r = run_fc::<2>(rf, mcx, &args, None, 2);
    let (rst, rclass) = verdict(&r, None);

    let mut cval: c_int = 0;
    // SAFETY: image buffers live for the call.
    let cst = unsafe {
        match arm {
            TwoRecArm::ImageCmp => pg_diff_record_image_cmp(
                i1.as_ptr(), i1.len() as c_int, i2.as_ptr(), i2.len() as c_int, &mut cval),
            TwoRecArm::ImageEq => pg_diff_record_image_eq(
                i1.as_ptr(), i1.len() as c_int, i2.as_ptr(), i2.len() as c_int, &mut cval),
            TwoRecArm::Larger => pg_diff_record_larger(
                i1.as_ptr(), i1.len() as c_int, i2.as_ptr(), i2.len() as c_int, &mut cval),
            TwoRecArm::Smaller => pg_diff_record_smaller(
                i1.as_ptr(), i1.len() as c_int, i2.as_ptr(), i2.len() as c_int, &mut cval),
        }
    };
    assert!(cst >= 0, "C harness internal failure {cst} ({name})");
    assert_eq!(rst, cst,
               "{name} verdict divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
    if rst == 1 {
        if let Some(rc) = rclass {
            assert_eq!(rc, c_errcode(), "{name} errcode divergence: desc1={desc1} desc2={desc2}");
        }
        return;
    }
    let rd = r.unwrap();
    let rval: c_int = match arm {
        TwoRecArm::ImageCmp => rd.as_i32(),
        TwoRecArm::ImageEq => c_int::from(rd.as_usize() != 0),
        TwoRecArm::Larger | TwoRecArm::Smaller => c_int::from(rd != d1),
    };
    assert_eq!(rval, cval,
               "{name} value divergence: desc1={desc1} desc2={desc2} f1={f1:?} f2={f2:?}");
}

fn hash_diff(desc: usize, payload: &[u8], extended: bool) {
    let mut cur = Cursor { b: payload, i: 0 };
    let seed = u64::from_le_bytes([
        cur.u8(), cur.u8(), cur.u8(), cur.u8(),
        cur.u8(), cur.u8(), cur.u8(), cur.u8(),
    ]);
    let fields = decode_fields(&mut cur, desc);
    let ctx = mcx::MemoryContext::new("rowtypes_diff");
    let mcx = ctx.mcx();
    let Some(img) = build_record(mcx, desc, &fields) else { return };
    let d = Datum::from_usize(img.as_ptr() as usize);

    if extended {
        let args = [d, Datum::from_u64(seed)];
        let r = run_fc::<2>(adt_rowtypes::fc_hash_record_extended, mcx, &args, None, 2);
        let (rst, rclass) = verdict(&r, None);
        let mut ch: u64 = 0;
        // SAFETY: image buffer lives for the call.
        let cst = unsafe {
            pg_diff_hash_record_extended(img.as_ptr(), img.len() as c_int, seed, &mut ch)
        };
        assert_eq!(rst, cst, "hash_record_extended verdict divergence: desc={desc}");
        if rst == 1 {
            if let Some(rc) = rclass {
                assert_eq!(rc, c_errcode(), "hash_record_extended errcode divergence");
            }
            return;
        }
        assert_eq!(r.unwrap().as_u64(), ch,
                   "hash_record_extended value divergence: desc={desc} fields={fields:?}");
    } else {
        let args = [d];
        let r = run_fc::<1>(adt_rowtypes::fc_hash_record, mcx, &args, None, 2);
        let (rst, rclass) = verdict(&r, None);
        let mut ch: u32 = 0;
        // SAFETY: image buffer lives for the call.
        let cst = unsafe { pg_diff_hash_record(img.as_ptr(), img.len() as c_int, &mut ch) };
        assert_eq!(rst, cst, "hash_record verdict divergence: desc={desc}");
        if rst == 1 {
            if let Some(rc) = rclass {
                assert_eq!(rc, c_errcode(), "hash_record errcode divergence");
            }
            return;
        }
        assert_eq!(r.unwrap().as_u32(), ch,
                   "hash_record value divergence: desc={desc} fields={fields:?}");
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/rowtypes_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/rowtypes_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                if std::env::var_os("ROWTYPES_SEED_TRACE").is_some() {
                    std::eprintln!("SEED {}", p.display());
                }
                rowtypes_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    fn run(bytes: &[u8]) {
        rowtypes_diff(bytes);
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // record_in, hard mode, desc 0: ok + each malformed class
        for lit in [
            &b"(a,b)"[..], b"(,)", b"(\"a\"\"b\",c)", b" \x0b\x0c(a,b) \x0b",
            b"(a,b,c)", b"(a)", b"(a,b))", b"junk", b"(unterminated", b"(a\\", b"",
        ] {
            let mut v = vec![0u8, 0u8];
            v.extend_from_slice(lit);
            run(&v);
        }
        // soft + details modes
        for flags in [0x08u8, 0x18] {
            let mut v = vec![0u8, flags];
            v.extend_from_slice(b"(a,b,c,d)");
            run(&v);
            let mut v = vec![0u8, flags];
            v.extend_from_slice(b"(a,b)");
            run(&v);
        }
        // int4 column parse (desc 1) incl. error
        for lit in [&b"(1,b)"[..], b"(-2147483648,x)", b"(2147483648,x)", b"(1x,y)"] {
            let mut v = vec![0u8, 1u8];
            v.extend_from_slice(lit);
            run(&v);
        }
        // dropped-column desc 2 + single-col desc 4
        run(&[0, 2, b'(', b'a', b',', b'b', b')']);
        run(&[0, 4, b'(', b'a', b')']);
        // record_out arm over built records, all descs
        for d in 0..5u8 {
            run(&[1, d, 1, 2, b'h', b'i', 1, 1, b'x', 1, 3, b'a', b'b', b'c']);
        }
        // record_recv: valid 2-col text wire for desc 0
        let mut wire = vec![2u8, 0];
        wire.extend_from_slice(&2u32.to_be_bytes()); // usercols
        for pay in [&b"aa"[..], b""] {
            wire.extend_from_slice(&TEXTOID.to_be_bytes());
            wire.extend_from_slice(&(pay.len() as u32).to_be_bytes());
            wire.extend_from_slice(pay);
        }
        run(&wire);
        // recv error shapes: wrong colcount / truncation / bad itemlen
        run(&[2, 0, 0, 0, 0, 9]);
        run(&[2, 0]);
        run(&[2, 1, 0, 0, 0, 2, 0, 0, 0, 23, 255, 255, 255, 200]);
        // record_send over built records
        for d in 0..5u8 {
            run(&[3, d, 1, 4, b't', b'e', b's', b't', 1, 1, b'q', 1, 2, b'z', b'w']);
        }
        // image cmp/eq: same-desc pairs, cross-desc (dissimilar, count mismatch)
        run(&[4, 0, 0, 1, 1, b'a', 1, 1, b'b', 1, 1, b'a', 1, 1, b'c']);
        run(&[4, 0, 1, 1, 1, b'a', 1, 1, b'b', 1, 1, 2, 2, 2, 2, 1, 1, b'b']);
        run(&[4, 0, 4, 1, 1, b'a', 1, 1, b'b', 1, 1, b'a']);
        run(&[5, 0, 0, 1, 1, b'a', 0, 1, 1, b'a', 0]);
        run(&[5, 1, 1, 1, 1, 2, 3, 4, 1, 1, b'x', 1, 4, 3, 2, 1, 1, 1, b'x']);
        // larger/smaller incl. faketype no-cmp error (desc 3)
        run(&[8, 0, 0, 1, 1, b'a', 1, 1, b'b', 1, 1, b'c', 1, 1, b'd']);
        run(&[8, 3, 3, 1, 1, 2, 3, 4, 1, 1, b'x', 1, 4, 3, 2, 1, 1, 1, b'y']);
        run(&[9, 0, 0, 1, 1, b'a', 0, 1, 1, b'b', 0]);
        // hash arms incl. faketype no-hash error
        run(&[6, 0, 1, 2, 3, 4, 5, 6, 7, 8, 1, 1, b'a', 1, 1, b'b']);
        run(&[6, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 1, 2, 3, 4, 0]);
        run(&[6, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 1, 2, 3, 4, 1, 1, b'z']);
        run(&[7, 0, 9, 9, 9, 9, 9, 9, 9, 9, 1, 1, b'a', 1, 1, b'b']);
        run(&[7, 2, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, b'a', 1, 1, b'b']);
    }

    /// New-arm smoke: record cmp family (10-16), image wrappers (17-21),
    /// byval-width descriptors 5/6, anonymous typmod, new-codec literals.
    #[test]
    fn arms_smoke_extended() {
        let _serial = crate::c_oracle_serial();
        // record_eq/ne/lt/gt/le/ge/btrecordcmp over (text,text) + (int4,text)
        for arm in 10u8..=16 {
            run(&[arm, 0, 0, 1, 1, b'a', 1, 1, b'b', 1, 1, b'a', 1, 1, b'c']);
            run(&[arm, 1, 1, 1, 1, 2, 3, 4, 1, 1, b'x', 1, 4, 3, 2, 1, 1, 1, b'x']);
            // dissimilar / count-mismatch / no-support (faketype) errors
            run(&[arm, 0, 1, 1, 1, b'a', 1, 1, b'b', 1, 1, 2, 2, 2, 2, 1, 1, b'b']);
            run(&[arm, 0, 4, 1, 1, b'a', 1, 1, b'b', 1, 1, b'a']);
            run(&[arm, 3, 3, 1, 1, 2, 3, 4, 1, 1, b'x', 1, 4, 3, 2, 1, 1, 1, b'y']);
        }
        // image ne/lt/gt/le/ge over text + byval descs
        for arm in 17u8..=21 {
            run(&[arm, 0, 0, 1, 1, b'a', 1, 1, b'b', 1, 1, b'a', 1, 1, b'c']);
            run(&[arm, 5, 5, 1, 1, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 9,
                  1, 1, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 8]);
        }
        // desc 5 (bool,int2,int8): in/out/recv/send + image cmp equal/less
        run(&[0, 5, b'(', b't', b',', b'7', b',', b'9', b')']);
        run(&[0, 5, b'(', b'f', b',', b'-', b'3', b'2', b'7', b'6', b'8', b',', b')']);
        run(&[0, 5, b'(', b'x', b',', b',', b')']); // boolin error
        run(&[1, 5, 1, 1, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 9]);
        run(&[3, 5, 1, 0, 1, 44, 1, 1, 7, 7, 7, 7, 7, 7, 7, 7]);
        // desc 5 wire: 3 cols (bool,int2,int8)
        let mut w = vec![2u8, 5];
        w.extend_from_slice(&3u32.to_be_bytes());
        w.extend_from_slice(&BOOLOID.to_be_bytes());
        w.extend_from_slice(&1u32.to_be_bytes());
        w.push(1);
        w.extend_from_slice(&INT2OID.to_be_bytes());
        w.extend_from_slice(&2u32.to_be_bytes());
        w.extend_from_slice(&[0, 7]);
        w.extend_from_slice(&INT8OID.to_be_bytes());
        w.extend_from_slice(&8u32.to_be_bytes());
        w.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 9]);
        run(&w);
        // desc 6 (fix8,bool): in/out + image pairs differing in one byte
        run(&[0, 6, b'(', b'a', b'b', b'c', b',', b't', b')']);
        run(&[1, 6, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 1]);
        run(&[4, 6, 6, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 1,
              1, 8, 1, 2, 3, 4, 5, 6, 7, 9, 1, 1]);
        run(&[5, 6, 6, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 0,
              1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 0]);
        // byval image cmp: equal / less / greater over desc 5 pairs
        run(&[4, 5, 5, 1, 1, 1, 2, 0, 1, 5, 5, 5, 5, 5, 5, 5, 5,
              1, 1, 1, 2, 0, 1, 5, 5, 5, 5, 5, 5, 5, 5]);
        run(&[4, 5, 5, 1, 0, 1, 2, 0, 1, 5, 5, 5, 5, 5, 5, 5, 5,
              1, 1, 1, 2, 0, 1, 5, 5, 5, 5, 5, 5, 5, 5]);
        // hash over desc 5/6: no-support error witnesses (bool has no hash)
        run(&[6, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 9]);
        run(&[7, 6, 1, 2, 3, 4, 5, 6, 7, 8, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 1]);
        // record_larger over desc 5: no cmp support error witness
        run(&[8, 5, 5, 1, 1, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 9,
              1, 0, 1, 2, 0, 1, 9, 9, 9, 9, 9, 9, 9, 9]);
        // anonymous typmod: hard + soft record_in, record_recv
        run(&[0, 0x20, b'(', b'a', b',', b'b', b')']);
        run(&[0, 0x28, b'(', b'a', b',', b'b', b')']);
        run(&[0, 0x38, b'(', b'a', b',', b'b', b')']);
        run(&[2, 0x20, 0, 0, 0, 2]);
    }

    /// Single-field-difference witness pairs (seeding obligation): records
    /// differing in exactly one column, each column, both orders — witnessed
    /// through cmp, eq, larger/smaller, and hash.
    #[test]
    fn single_field_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        let base: &[u8] = &[1, 1, b'a', 1, 1, b'b']; // desc 0: ("a","b")
        let variants: &[&[u8]] = &[
            &[1, 1, b'c', 1, 1, b'b'],       // col1 differs
            &[1, 1, b'a', 1, 1, b'c'],       // col2 differs
            &[1, 2, b'a', b'a', 1, 1, b'b'], // col1 longer (equal prefix)
            &[0, 1, 1, b'b'],                // col1 null
            &[1, 1, b'a', 0],                // col2 null
        ];
        for arm in [4u8, 5, 8, 9] {
            for v in variants {
                let mut fwd = vec![arm, 0, 0];
                fwd.extend_from_slice(base);
                fwd.extend_from_slice(v);
                run(&fwd);
                let mut rev = vec![arm, 0, 0];
                rev.extend_from_slice(v);
                rev.extend_from_slice(base);
                run(&rev);
            }
        }
        // hash: one-field deltas over int4 bytes (desc 1)
        for b in [&[1, 4, 0, 0, 0, 1][..], &[1, 4, 0, 0, 1, 0], &[1, 4, 1, 0, 0, 0]] {
            let mut v = vec![6, 1, 0, 0, 0, 0, 0, 0, 0, 0];
            v.extend_from_slice(b);
            v.extend_from_slice(&[1, 1, b'x']);
            run(&v);
        }
    }
}
