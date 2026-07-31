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
//!   0: (text, text)             2: (text, [dropped], text)   4: (text)
//!   1: (int4, text)             3: (int4, faketype)
//! `faketype` (oid 7777) has text io but no cmp/hash support — it witnesses
//! the could-not-identify-function error arms.
//!
//! Input layout: [sel][flags][payload...]
//!   sel % 10 = arm (see dispatch); flags: bits 0-2 descriptor (%5),
//!   bit 3 = soft escontext mode (record_in only), bit 4 = details_wanted.
//!
//! SKIPPED rows / carves (documented, executable where applicable):
//!   - TOASTed (external/compressed) record inputs: unreachable in-harness
//!     (both sides' detoast seams are identity; C oracle aborts if reached).
//!   - record_eq/ne/lt/gt/le/ge/btrecordcmp: proved in proofs/records
//!     (per-descriptor Kani theorems); not re-fuzzed here. record_cmp IS
//!     exercised through the record_larger/record_smaller arms.
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
}

// ---------------------------------------------------------------------------
// Pinned environment: codec oids + type oids (the codec contract; see the
// C oracle SECTION D comment for the algorithm-of-record of each codec).
// ---------------------------------------------------------------------------

const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const FAKETYPE: Oid = 7777;

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

const BTREE_AM: Oid = 403;
const HASH_AM: Oid = 405;

const NDESC: usize = 5;

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

// Seam installation + descriptor registration --------------------------------

fn io_shape(oid: Oid, input: Oid, output: Oid, recv: Oid, send: Oid,
            typlen: i16, byval: bool) -> syscache_seams::PgTypeIoShape {
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
        typalign: b'i' as i8,
        typdelim: b',' as i8,
        typisdefined: true,
    }
}

fn tc_shape(typlen: i16, byval: bool, storage: i8) -> syscache_seams::PgTypeTypcacheShape {
    syscache_seams::PgTypeTypcacheShape {
        typname: Default::default(),
        typlen,
        typbyval: byval,
        typalign: b'i' as i8,
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
    let mut a = types_tuple::FormData_pg_attribute::default();
    a.attname.namestrcpy(name);
    a.attnum = num;
    a.atttypid = typid;
    a.attlen = typlen;
    a.attbyval = byval;
    a.attalign = b'i' as i8;
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
                TEXTOID => Some(io_shape(TEXTOID, MYTEXTIN, MYTEXTOUT, MYTEXTRECV, MYTEXTSEND, -1, false)),
                INT4OID => Some(io_shape(INT4OID, MYINT4IN, MYINT4OUT, MYINT4RECV, MYINT4SEND, 4, true)),
                FAKETYPE => Some(io_shape(FAKETYPE, MYTEXTIN, MYTEXTOUT, MYTEXTRECV, MYTEXTSEND, -1, false)),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                TEXTOID => Some(tc_shape(-1, false, b'x' as i8)),
                INT4OID => Some(tc_shape(4, true, b'p' as i8)),
                FAKETYPE => Some(tc_shape(-1, false, b'x' as i8)),
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
    let menus: [&[types_tuple::FormData_pg_attribute]; NDESC] = [&d0, &d1, &d2, &d3, &d4];
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

/// One decoded field: None = SQL NULL, Some(bytes) = payload (int4 columns
/// consume exactly 4 bytes little-endian; text columns a 1-byte length + data).
type Fields = Vec<Option<Vec<u8>>>;

fn decode_fields(cur: &mut Cursor<'_>, desc: usize) -> Fields {
    let shapes: &[Option<bool>] = match desc {
        0 => &[Some(false), Some(false)],
        1 => &[Some(true), Some(false)],
        2 => &[Some(false), None, Some(false)],
        3 => &[Some(true), Some(false)],
        _ => &[Some(false)],
    };
    shapes
        .iter()
        .map(|s| match s {
            None => None, // dropped column: always null
            Some(byval) => {
                if cur.u8() & 1 == 0 {
                    None
                } else if *byval {
                    Some(cur.bytes(4).to_vec())
                } else {
                    let n = cur.u8() as usize;
                    Some(cur.bytes(n).to_vec())
                }
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
                    let mut v = [0u8; 4];
                    let m = bytes.len().min(4);
                    v[..m].copy_from_slice(&bytes[..m]);
                    values.push(Datum::from_i32(i32::from_le_bytes(v)));
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
    if !install() {
        return; // seams owned by a sibling diff module in this process
    }
    let [sel, flags, payload @ ..] = data else { return };
    let desc = (*flags & 0x07) as usize % NDESC;
    let soft = *flags & 0x08 != 0;
    let details = *flags & 0x10 != 0;
    match sel % 10 {
        0 => record_in_diff(desc, soft, details, payload),
        1 => record_out_diff(desc, payload),
        2 => record_recv_diff(desc, payload),
        3 => record_send_diff(desc, payload),
        4 => two_record_diff(desc, payload, TwoRecArm::ImageCmp),
        5 => two_record_diff(desc, payload, TwoRecArm::ImageEq),
        6 => hash_diff(desc, payload, false),
        7 => hash_diff(desc, payload, true),
        8 => two_record_diff(desc, payload, TwoRecArm::Larger),
        _ => two_record_diff(desc, payload, TwoRecArm::Smaller),
    }
}

fn record_in_diff(desc: usize, soft: bool, details: bool, payload: &[u8]) {
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
        Datum::from_i32(desc as i32),
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
        pg_diff_record_in(desc as c_int, c_int::from(soft), lit_c.as_ptr(),
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

fn record_recv_diff(desc: usize, payload: &[u8]) {
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
        Datum::from_i32(desc as i32),
    ];
    let r = run_fc::<3>(adt_rowtypes::fc_record_recv, mcx, &args, None, 1);
    let (rst, rclass) = verdict(&r, None);

    let mut out = vec![0u8; 1 << 16];
    let mut outlen: c_int = out.len() as c_int;
    // SAFETY: buffers live for the call.
    let cst = unsafe {
        pg_diff_record_recv(desc as c_int, payload.as_ptr(), payload.len() as c_int,
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
}

/// record_send both sides over one image; compares wire bytes.
fn send_compare(mcx: mcx::Mcx<'_>, img: &[u8]) {
    let d = Datum::from_usize(img.as_ptr() as usize);
    let r = run_fc::<1>(adt_rowtypes::fc_record_send, mcx, &[d], None, 1);
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
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/rowtypes_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/rowtypes_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
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

    /// Single-field-difference witness pairs (seeding obligation): records
    /// differing in exactly one column, each column, both orders — witnessed
    /// through cmp, eq, larger/smaller, and hash.
    #[test]
    fn single_field_witness_pairs() {
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
