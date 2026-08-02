//! Differential fuzz driver: adt_pg_lsn (shipped Rust) vs vendored
//! PostgreSQL 18.3 C (csrc/pg_lsn_oracle.c, Stamp 18.3 @ 62d6c7d).
//!
//! One composite target for the whole pg_lsn input-language family, sibling
//! ops behind a selector byte (the float_in_diff pattern). The arithmetic
//! ops (pg_lsn_mi / pg_lsn_pli / pg_lsn_mii / numeric_pg_lsn — the crate's
//! four DigitBuf-blocked proof-ledger rows) are the payload: they are
//! fuzz-only under the campaign cascade, and the C side runs the verbatim
//! numeric.c parse/add/sub/uint64 path the real functions take.
//!
//! Comparison planes (the harness contract): value bits / full result-image
//! bytes (varlena header included) + error-verdict + errcode class.
//! Message text is out of scope.
//!
//! Documented carves / non-compared planes:
//!  - NON-DECIMAL numeric literals (0x/0o/0b): oracle shim S5 routes them
//!    out of scope; the skip predicate below is C's own base-detection
//!    predicate. They cannot arise from pg_lsn's internal renderings and
//!    belong to the numeric family's own lanes.
//!  - numeric-text acceptance: if shipped Rust numeric_in REJECTS the
//!    nbytes text, the case is skipped (numeric_in parity is the numeric
//!    lane's claim, not this target's); when BOTH sides accept, the packed
//!    images are compared byte-exactly before use, so accepted-input
//!    numeric_in parity is still enforced in passing.
//!  - pg_lsn_hash / pg_lsn_hash_extended: EXECUTION-ONLY (line coverage);
//!    the hash kernels are proved C≡Rust in proofs/hash-rows, and the
//!    fold-to-lohalf is pinned by adt_pg_lsn's own tests. No C oracle here.
//!  - pg_lsn_recv / pg_lsn_send: compared against the pq_getmsgint64 /
//!    pq_sendint64 wire contract constructed inline (big-endian u64; send
//!    image = 4-byte LE varlena header (12<<2) + 8 BE payload bytes;
//!    insufficient data => ERRCODE_PROTOCOL_VIOLATION "insufficient data
//!    left in message"), per src/backend/libpq/pqformat.c @ Stamp 18.3.
//!    The C bodies are bare calls to exactly those primitives.

use std::ffi::CString;

use datum::Datum;
use mcx::MemoryContext;
use types_error::{
    PgError, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROTOCOL_VIOLATION,
};
use types_fmgr::LocalFcinfo;

extern "C" {
    fn pg_lsnfuzz_in(str_: *const core::ffi::c_char, out: *mut u64) -> i32;
    fn pg_lsnfuzz_out(lsn: u64, buf: *mut core::ffi::c_char) -> i32;
    fn pg_lsnfuzz_cmp(op: i32, lsn1: u64, lsn2: u64, out64: *mut u64) -> i64;
    fn pg_lsnfuzz_numeric_in(
        str_: *const core::ffi::c_char,
        img_out: *mut u8,
        img_cap: i32,
        img_len: *mut i32,
    ) -> i32;
    fn pg_lsnfuzz_mi(lsn1: u64, lsn2: u64, img_out: *mut u8, img_cap: i32, img_len: *mut i32)
        -> i32;
    fn pg_lsnfuzz_plimii(lsn: u64, nbytes_text: *const core::ffi::c_char, sub: i32, out: *mut u64)
        -> i32;
    fn pg_lsnfuzz_numeric_pg_lsn(num_text: *const core::ffi::c_char, out: *mut u64) -> i32;
}

/// Oracle error classes (csrc/pg_lsn_oracle.c S2).
const C_ERR_INVALID_TEXT: i64 = 1; /* 22P02 */
const C_ERR_OUT_OF_RANGE: i64 = 2; /* 22003 */
const C_ERR_NOT_SUPPORTED: i64 = 3; /* 0A000 */
const C_ERR_INVALID_PARAM: i64 = 4; /* 22023 */
const C_CARVE_NONDECIMAL: i64 = 98; /* S5 sentinel */

fn rust_err_class(e: &PgError) -> i64 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
        C_ERR_NOT_SUPPORTED
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        C_ERR_INVALID_PARAM
    } else {
        99
    }
}

const MAXPG_LSNLEN: usize = 17;

/// C numeric_in's base-detection predicate (numeric.c) applied to the text
/// after space/sign skip — the S5 carve's exact reach.
fn is_nondecimal_form(text: &[u8]) -> bool {
    let mut i = 0;
    while i < text.len() && (text[i] as char).is_whitespace() {
        i += 1;
    }
    if i < text.len() && (text[i] == b'+' || text[i] == b'-') {
        i += 1;
    }
    i + 1 < text.len()
        && text[i] == b'0'
        && matches!(text[i + 1], b'x' | b'X' | b'o' | b'O' | b'b' | b'B')
}

/// Both-sides numeric_in over the same text; Some((rust_img, ())) when both
/// accept (images asserted equal), None when the case is out of scope
/// (rejection, carve, or interior NUL).
fn numeric_both(text: &[u8]) -> Option<adt_numeric::NumericImage> {
    if text.len() > 256 || text.contains(&0) || is_nondecimal_form(text) {
        return None;
    }
    let s = std::str::from_utf8(text).ok()?;
    let rust = match adt_numeric::numeric_in(s, -1, None) {
        Ok(img) => img.expect("hard-error path returns Err"),
        Err(_) => return None, /* acceptance skip, see module doc */
    };
    let cs = CString::new(text).unwrap();
    let mut cimg = [0u8; 1024];
    let mut clen: i32 = 0;
    let crc = unsafe {
        pg_lsnfuzz_numeric_in(cs.as_ptr(), cimg.as_mut_ptr(), 1024, &mut clen)
    };
    assert!(
        crc == 0,
        "numeric_in DIVERGENCE input={s:?}: Rust accepted, C err {crc}"
    );
    assert!(
        rust.as_bytes() == &cimg[..clen as usize],
        "numeric_in IMAGE DIVERGENCE input={s:?}: Rust={:02x?} C={:02x?}",
        rust.as_bytes(),
        &cimg[..clen as usize]
    );
    Some(rust)
}

// --------------------------------------------------------------------------
// selector 0: pg_lsn_in (hard + soft error paths + fc wrapper)
// --------------------------------------------------------------------------

fn drive_in(text: &[u8]) {
    if text.len() > 64 || text.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(text) else {
        return;
    };
    let cs = CString::new(text).unwrap();
    let mut cval = 0u64;
    let crc = unsafe { pg_lsnfuzz_in(cs.as_ptr(), &mut cval) };

    // hard-error path
    match adt_pg_lsn::pg_lsn_in(s, None) {
        Ok(v) => assert!(
            crc == 0 && v == cval,
            "pg_lsn_in DIVERGENCE input={s:?}: C=(err {crc}, {cval:#x}) Rust=Ok({v:#x})"
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                i64::from(crc) == rerr,
                "pg_lsn_in DIVERGENCE input={s:?}: C err {crc} vs Rust err {rerr}"
            );
        }
    }

    // soft path: same verdict, saved error carries the same class
    let mut soft = SoftErrorContext::new(true);
    let softval = adt_pg_lsn::pg_lsn_in(s, Some(&mut soft))
        .expect("soft path never hard-errors for pg_lsn_in");
    if crc == 0 {
        assert!(!soft.error_occurred() && softval == cval, "soft/hard skew on {s:?}");
    } else {
        assert!(soft.error_occurred(), "soft path missed error on {s:?}");
        let saved = soft.error().expect("details_wanted");
        assert!(rust_err_class(saved) == i64::from(crc), "soft errcode skew on {s:?}");
    }

    // fc wrapper (hard shape), for builtins.rs coverage
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
    match adt_pg_lsn::builtins::fc_pg_lsn_in(None, &mut fcinfo) {
        Ok(d) => assert!(crc == 0 && d.as_i64() as u64 == cval, "fc_pg_lsn_in skew on {s:?}"),
        Err(e) => assert!(i64::from(crc) == rust_err_class(&e), "fc_pg_lsn_in err skew on {s:?}"),
    }
}

// --------------------------------------------------------------------------
// selector 1: pg_lsn_out image + in(out(x)) roundtrip + fc wrapper
// --------------------------------------------------------------------------

fn drive_out(lsn: u64) {
    let mut cbuf = [0u8; MAXPG_LSNLEN + 1];
    let clen = unsafe { pg_lsnfuzz_out(lsn, cbuf.as_mut_ptr().cast()) } as usize;

    let mut rbuf = [0u8; MAXPG_LSNLEN + 1];
    let rlen = adt_pg_lsn::pg_lsn_out_into(lsn, &mut rbuf);
    assert!(
        &cbuf[..clen] == &rbuf[..rlen],
        "pg_lsn_out DIVERGENCE lsn={lsn:#x}: C={:?} Rust={:?}",
        std::str::from_utf8(&cbuf[..clen]),
        std::str::from_utf8(&rbuf[..rlen])
    );

    // roundtrip (both sides accept their own image)
    let s = std::str::from_utf8(&rbuf[..rlen]).unwrap();
    assert!(
        adt_pg_lsn::pg_lsn_in(s, None).unwrap() == lsn,
        "pg_lsn roundtrip broke at {lsn:#x}"
    );

    // fc wrapper: cstring scratch datum
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_i64(lsn as i64));
    let d = adt_pg_lsn::builtins::fc_pg_lsn_out(None, &mut fcinfo).unwrap();
    // SAFETY: fc_pg_lsn_out returns a NUL-terminated cstring datum.
    let out = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert!(out.to_bytes() == &cbuf[..clen], "fc_pg_lsn_out image skew at {lsn:#x}");
}

// --------------------------------------------------------------------------
// selector 2: comparison / minmax / cmp family + hash execution + wrappers
// --------------------------------------------------------------------------

fn call2(f: types_fmgr::PGFunction, a: Datum, b: Datum) -> Datum {
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, a);
    fcinfo.set_arg(1, b);
    f(None, &mut fcinfo).unwrap()
}

fn drive_cmp(a: u64, b: u64) {
    use adt_pg_lsn::builtins::*;
    let (da, db) = (Datum::from_i64(a as i64), Datum::from_i64(b as i64));
    let mut sink = 0u64;
    let bools: [(types_fmgr::PGFunction, i32); 6] = [
        (fc_pg_lsn_eq, 0),
        (fc_pg_lsn_ne, 1),
        (fc_pg_lsn_lt, 2),
        (fc_pg_lsn_gt, 3),
        (fc_pg_lsn_le, 4),
        (fc_pg_lsn_ge, 5),
    ];
    for (f, op) in bools {
        let c = unsafe { pg_lsnfuzz_cmp(op, a, b, &mut sink) };
        let r = call2(f, da, db).as_bool();
        assert!(c == r as i64, "pg_lsn cmp op {op} DIVERGENCE a={a:#x} b={b:#x}");
    }
    let c = unsafe { pg_lsnfuzz_cmp(6, a, b, &mut sink) };
    assert!(
        c == i64::from(call2(fc_pg_lsn_cmp, da, db).as_i32()),
        "pg_lsn_cmp DIVERGENCE a={a:#x} b={b:#x}"
    );
    let mut clarger = 0u64;
    unsafe { pg_lsnfuzz_cmp(7, a, b, &mut clarger) };
    assert!(
        clarger == call2(fc_pg_lsn_larger, da, db).as_i64() as u64,
        "pg_lsn_larger DIVERGENCE a={a:#x} b={b:#x}"
    );
    let mut csmaller = 0u64;
    unsafe { pg_lsnfuzz_cmp(8, a, b, &mut csmaller) };
    assert!(
        csmaller == call2(fc_pg_lsn_smaller, da, db).as_i64() as u64,
        "pg_lsn_smaller DIVERGENCE a={a:#x} b={b:#x}"
    );

    // hash: wrapper-fold parity plane (mutants round-1 survivor fix,
    // p1-lane0a). The wrappers inline C hashint8's lo^hi fold
    // (src/backend/access/hash/hashfunc.c hashint8: lohalf ^= (val >= 0) ?
    // hihalf : ~hihalf) before the proved hash_bytes_uint32 kernel
    // (proofs/hash-rows). Recompute the fold independently here and pin the
    // wrapper output — kills wrapper-logic mutants the execution-only drive
    // let survive; the kernel differential stays owned by proofs/hash-rows.
    let val = a as i64;
    let expect_fold = (val as u32) ^ if val >= 0 { (val >> 32) as u32 } else { !((val >> 32) as u32) };
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, da);
    let h = fc_pg_lsn_hash(None, &mut fcinfo).unwrap();
    assert!(
        h.as_u32() == hashfn::hash_bytes_uint32(expect_fold),
        "fc_pg_lsn_hash fold skew a={a:#x}"
    );
    let hx = call2(fc_pg_lsn_hash_extended, da, db);
    assert!(
        hx.as_u64() == hashfn::hash_bytes_uint32_extended(expect_fold, b),
        "fc_pg_lsn_hash_extended fold skew a={a:#x} seed={b:#x}"
    );
}

// --------------------------------------------------------------------------
// selector 3: recv / send against the pqformat wire contract + wrappers
// --------------------------------------------------------------------------

fn drive_recv_send(payload: &[u8]) {
    if payload.len() > 64 {
        return;
    }
    let cx = MemoryContext::new_bump("lsn_fuzz");
    let mcx = cx.mcx();

    // recv: expectation per pq_getmsgint64 (BE u64; short => 08P01)
    let mut vec = mcx::vec_with_capacity_in::<u8>(mcx, payload.len()).unwrap();
    mcx::vec_append_bytes(&mut vec, payload).unwrap();
    let mut msg = stringinfo::StringInfo::from_vec(vec).unwrap();
    let expect: Result<u64, ()> = if payload.len() >= 8 {
        Ok(u64::from_be_bytes(payload[..8].try_into().unwrap()))
    } else {
        Err(())
    };
    match (adt_pg_lsn::pg_lsn_recv(&mut msg), expect) {
        (Ok(v), Ok(e)) => {
            assert!(v == e, "pg_lsn_recv value skew");
            assert!(msg.cursor == 8, "pg_lsn_recv cursor skew");
        }
        (Err(err), Err(())) => assert!(
            err.sqlstate == ERRCODE_PROTOCOL_VIOLATION,
            "pg_lsn_recv wrong errcode: {}",
            err.message
        ),
        (r, _) => panic!(
            "pg_lsn_recv CONTRACT DIVERGENCE len={}: Rust={:?}",
            payload.len(),
            r.map_err(|e| e.message)
        ),
    }

    // send: image = varlena hdr (12<<2 LE) + 8 BE bytes, per pq_sendint64
    if payload.len() >= 8 {
        let lsn = u64::from_be_bytes(payload[..8].try_into().unwrap());
        let bytea = adt_pg_lsn::pg_lsn_send(mcx, lsn).unwrap();
        let mut expect_img = Vec::with_capacity(12);
        expect_img.extend_from_slice(&((12u32) << 2).to_le_bytes());
        expect_img.extend_from_slice(&lsn.to_be_bytes());
        assert!(
            bytea.as_bytes() == expect_img.as_slice(),
            "pg_lsn_send image skew at {lsn:#x}: {:02x?}",
            bytea.as_bytes()
        );

        // fc wrappers
        let d = types_fmgr::direct_function_call1_coll_in(
            adt_pg_lsn::builtins::fc_pg_lsn_send,
            0,
            mcx,
            Datum::from_i64(lsn as i64),
        )
        .unwrap();
        // SAFETY: send result is a live 4B-header varlena in cx.
        let r = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
        assert!(r.as_bytes() == expect_img.as_slice(), "fc_pg_lsn_send image skew");
    }
    {
        let mut vec = mcx::vec_with_capacity_in::<u8>(mcx, payload.len()).unwrap();
        mcx::vec_append_bytes(&mut vec, payload).unwrap();
        let mut msg = stringinfo::StringInfo::from_vec(vec).unwrap();
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, Datum::from_usize(&mut msg as *mut _ as usize));
        let r = adt_pg_lsn::builtins::fc_pg_lsn_recv(None, &mut fcinfo);
        match expect {
            Ok(e) => assert!(r.unwrap().as_i64() as u64 == e, "fc_pg_lsn_recv skew"),
            Err(()) => assert!(r.is_err(), "fc_pg_lsn_recv missed error"),
        }
    }
}

// --------------------------------------------------------------------------
// selector 4: pg_lsn_mi — numeric result image, byte-exact + fc wrapper
// --------------------------------------------------------------------------

fn drive_mi(a: u64, b: u64) {
    let mut cimg = [0u8; 1024];
    let mut clen: i32 = 0;
    let crc = unsafe { pg_lsnfuzz_mi(a, b, cimg.as_mut_ptr(), 1024, &mut clen) };
    assert!(crc == 0, "oracle pg_lsn_mi errored: {crc}");

    let rimg = adt_pg_lsn::pg_lsn_mi(a, b).unwrap();
    assert!(
        rimg.as_bytes() == &cimg[..clen as usize],
        "pg_lsn_mi IMAGE DIVERGENCE a={a:#x} b={b:#x}: Rust={:02x?} C={:02x?}",
        rimg.as_bytes(),
        &cimg[..clen as usize]
    );

    // fc wrapper: byref numeric result copied into the bump context
    let cx = MemoryContext::new_bump("lsn_fuzz_mi");
    let d = types_fmgr::direct_function_call2_coll_in(
        adt_pg_lsn::builtins::fc_pg_lsn_mi,
        0,
        cx.mcx(),
        Datum::from_i64(a as i64),
        Datum::from_i64(b as i64),
    )
    .unwrap();
    // SAFETY: numeric result is a live 4B-header varlena in cx.
    let r = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert!(r.as_bytes() == &cimg[..clen as usize], "fc_pg_lsn_mi image skew");
}

// --------------------------------------------------------------------------
// selectors 5/6: pg_lsn_pli / pg_lsn_mii; selector 7: numeric_pg_lsn
// --------------------------------------------------------------------------

fn drive_plimii(lsn: u64, text: &[u8], sub: bool) {
    let Some(rust_num) = numeric_both(text) else {
        return;
    };
    let cs = CString::new(text).unwrap();
    let mut cval = 0u64;
    let crc = unsafe { pg_lsnfuzz_plimii(lsn, cs.as_ptr(), sub as i32, &mut cval) };
    assert!(i64::from(crc) != C_CARVE_NONDECIMAL, "carve leaked through skip predicate");

    let name = if sub { "pg_lsn_mii" } else { "pg_lsn_mi" };
    let rres = if sub {
        adt_pg_lsn::pg_lsn_mii(lsn, rust_num.num())
    } else {
        adt_pg_lsn::pg_lsn_pli(lsn, rust_num.num())
    };
    match rres {
        Ok(v) => assert!(
            crc == 0 && v == cval,
            "{name} DIVERGENCE lsn={lsn:#x} n={:?}: C=(err {crc}, {cval:#x}) Rust=Ok({v:#x})",
            String::from_utf8_lossy(text)
        ),
        Err(e) => assert!(
            i64::from(crc) == rust_err_class(&e),
            "{name} DIVERGENCE lsn={lsn:#x} n={:?}: C err {crc} vs Rust err {} ({})",
            String::from_utf8_lossy(text),
            rust_err_class(&e),
            e.message
        ),
    }

    // fc wrapper (same inputs; numeric arg = full varlena image datum)
    let f = if sub {
        adt_pg_lsn::builtins::fc_pg_lsn_mii
    } else {
        adt_pg_lsn::builtins::fc_pg_lsn_pli
    };
    let cx = MemoryContext::new_bump("lsn_fuzz_pli");
    let d = types_fmgr::direct_function_call2_coll_in(
        f,
        0,
        cx.mcx(),
        Datum::from_i64(lsn as i64),
        Datum::from_usize(rust_num.as_bytes().as_ptr() as usize),
    );
    match d {
        Ok(d) => assert!(crc == 0 && d.as_i64() as u64 == cval, "fc {name} value skew"),
        Err(e) => assert!(i64::from(crc) == rust_err_class(&e), "fc {name} err skew"),
    }
}

fn drive_numeric_pg_lsn(text: &[u8]) {
    let Some(rust_num) = numeric_both(text) else {
        return;
    };
    let cs = CString::new(text).unwrap();
    let mut cval = 0u64;
    let crc = unsafe { pg_lsnfuzz_numeric_pg_lsn(cs.as_ptr(), &mut cval) };

    match adt_pg_lsn::numeric_pg_lsn(rust_num.num()) {
        Ok(v) => assert!(
            crc == 0 && v == cval,
            "numeric_pg_lsn DIVERGENCE n={:?}: C=(err {crc}, {cval:#x}) Rust=Ok({v:#x})",
            String::from_utf8_lossy(text)
        ),
        Err(e) => assert!(
            i64::from(crc) == rust_err_class(&e),
            "numeric_pg_lsn DIVERGENCE n={:?}: C err {crc} vs Rust err {} ({})",
            String::from_utf8_lossy(text),
            rust_err_class(&e),
            e.message
        ),
    }

    // fc wrapper
    let cx = MemoryContext::new_bump("lsn_fuzz_npl");
    let d = types_fmgr::direct_function_call1_coll_in(
        adt_pg_lsn::builtins::fc_numeric_pg_lsn,
        0,
        cx.mcx(),
        Datum::from_usize(rust_num.as_bytes().as_ptr() as usize),
    );
    match d {
        Ok(d) => assert!(crc == 0 && d.as_i64() as u64 == cval, "fc_numeric_pg_lsn value skew"),
        Err(e) => assert!(i64::from(crc) == rust_err_class(&e), "fc_numeric_pg_lsn err skew"),
    }

    // Short-form (1-byte header) numeric arg: exercises num_arg's is_short
    // data_expanded arm (builtins.rs) — same value, same verdicts. Short
    // varlena header per varatt.h SET_VARSIZE_1B: (total_len << 1) | 0x01.
    let payload = &rust_num.as_bytes()[4..];
    if payload.len() <= 100 {
        let mut short = Vec::with_capacity(1 + payload.len());
        short.push((((1 + payload.len()) as u8) << 1) | 0x01);
        short.extend_from_slice(payload);
        let d = types_fmgr::direct_function_call1_coll_in(
            adt_pg_lsn::builtins::fc_numeric_pg_lsn,
            0,
            cx.mcx(),
            Datum::from_usize(short.as_ptr() as usize),
        );
        match d {
            Ok(d) => {
                assert!(crc == 0 && d.as_i64() as u64 == cval, "fc_numeric_pg_lsn short-form value skew")
            }
            Err(e) => assert!(
                i64::from(crc) == rust_err_class(&e),
                "fc_numeric_pg_lsn short-form err skew"
            ),
        }
    }
}

/// Input layout: [selector][payload]. selector % 8 picks the op family:
///   0 in (payload = text)         4 mi (16B le u64 pair)
///   1 out (8B le u64)             5 pli (8B le u64 + numeric text)
///   2 cmp family (16B le pair)    6 mii (8B le u64 + numeric text)
///   3 recv/send (raw wire bytes)  7 numeric_pg_lsn (numeric text)
pub fn pg_lsn_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 8 {
        0 => drive_in(rest),
        1 => {
            if rest.len() >= 8 {
                drive_out(u64::from_le_bytes(rest[..8].try_into().unwrap()));
            }
        }
        2 => {
            if rest.len() >= 16 {
                drive_cmp(
                    u64::from_le_bytes(rest[..8].try_into().unwrap()),
                    u64::from_le_bytes(rest[8..16].try_into().unwrap()),
                );
            }
        }
        3 => drive_recv_send(rest),
        4 => {
            if rest.len() >= 16 {
                drive_mi(
                    u64::from_le_bytes(rest[..8].try_into().unwrap()),
                    u64::from_le_bytes(rest[8..16].try_into().unwrap()),
                );
            }
        }
        5 | 6 => {
            if rest.len() >= 8 {
                drive_plimii(
                    u64::from_le_bytes(rest[..8].try_into().unwrap()),
                    &rest[8..],
                    sel % 8 == 6,
                );
            }
        }
        _ => drive_numeric_pg_lsn(rest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LSN_TEXT_CORPUS: &[&str] = &[
        "0/0", "0/12345678", "ABCD1234/beef0001", "FFFFFFFF/FFFFFFFF", "00000001/00000002",
        "1/2", "16/B374D848", "0/16B3748", "deadbeef/cafebabe", "", "/", "0", "0/", "/0",
        "123456789/0", "0/123456789", "0/0 ", " 0/0", "xyz/0", "0//0", "0x1/0", "g/0", "0/g",
        "-1/0", "0/-1", "1/2/3", "ffffffff/ffffffff", "00000000/00000000", "8/0",
    ];

    const NUM_TEXT_CORPUS: &[&str] = &[
        "0", "1", "-1", "10", "-10", "0.5", "-0.5", "1.5", "2.5", "-2.5", "0.49999", "1e10",
        "-1e10", "1e-10", "18446744073709551615", "18446744073709551616", "-18446744073709551615",
        "9223372036854775807", "4294967296", "1e38", "-1e38", "1e100", "NaN", "nan", "-NaN",
        "Infinity", "-Infinity", "inf", "-inf", "+inf", " 42 ", "1_000", "1.2.3", "abc", "",
        "1e1000000", "0.000000001", "123456.789", "-123456.789", "1e-1000",
    ];

    const LSN_VALS: &[u64] = &[
        0,
        1,
        0xFF,
        0x109,
        0xFFFF_FFFF,
        0x1_0000_0000,
        0x1_0000_0002,
        0xABCD_1234_BEEF_0001,
        u64::MAX,
        u64::MAX - 1,
        1 << 63,
        (1 << 63) - 1,
    ];

    #[test]
    fn lsn_in_corpus() {
        let _serial = crate::c_oracle_serial();
        for s in LSN_TEXT_CORPUS {
            let mut d = vec![0u8];
            d.extend_from_slice(s.as_bytes());
            pg_lsn_diff(&d);
        }
    }

    #[test]
    fn lsn_out_cmp_mi_corpus() {
        let _serial = crate::c_oracle_serial();
        for &a in LSN_VALS {
            let mut d = vec![1u8];
            d.extend_from_slice(&a.to_le_bytes());
            pg_lsn_diff(&d);
            for &b in LSN_VALS {
                for sel in [2u8, 4] {
                    let mut d = vec![sel];
                    d.extend_from_slice(&a.to_le_bytes());
                    d.extend_from_slice(&b.to_le_bytes());
                    pg_lsn_diff(&d);
                }
            }
        }
    }

    #[test]
    fn lsn_recv_send_corpus() {
        let _serial = crate::c_oracle_serial();
        for len in 0..=12 {
            let mut d = vec![3u8];
            d.extend_from_slice(&vec![0xA5u8; len]);
            pg_lsn_diff(&d);
        }
        for &v in LSN_VALS {
            let mut d = vec![3u8];
            d.extend_from_slice(&v.to_be_bytes());
            pg_lsn_diff(&d);
        }
    }

    #[test]
    fn lsn_arith_corpus() {
        let _serial = crate::c_oracle_serial();
        for &lsn in LSN_VALS {
            for n in NUM_TEXT_CORPUS {
                for sel in [5u8, 6] {
                    let mut d = vec![sel];
                    d.extend_from_slice(&lsn.to_le_bytes());
                    d.extend_from_slice(n.as_bytes());
                    pg_lsn_diff(&d);
                }
            }
        }
        for n in NUM_TEXT_CORPUS {
            let mut d = vec![7u8];
            d.extend_from_slice(n.as_bytes());
            pg_lsn_diff(&d);
        }
    }

    /// Must-disagree comparator controls: each plane's assert demonstrably
    /// fires on an injected skew (a corrupted expectation), proving the
    /// comparator is live — the fuzz-target analog of the proof suite's
    /// must-fail harnesses.
    #[test]
    fn comparator_fires_on_value_skew() {
        let _serial = crate::c_oracle_serial();
        // value plane: C oracle vs a WRONG Rust value
        let mut cval = 0u64;
        let cs = CString::new("1/2").unwrap();
        let crc = unsafe { pg_lsnfuzz_in(cs.as_ptr(), &mut cval) };
        assert_eq!((crc, cval), (0, 0x1_0000_0002));
        let r = std::panic::catch_unwind(|| {
            let v = adt_pg_lsn::pg_lsn_in("1/2", None).unwrap();
            assert!(v == cval + 1, "pg_lsn_in DIVERGENCE (injected)");
        });
        assert!(r.is_err(), "value-plane comparator must fire");
    }

    #[test]
    fn comparator_fires_on_error_plane_skew() {
        let _serial = crate::c_oracle_serial();
        // error plane: C rejects "junk"; asserting Rust ACCEPTS must fire
        let r = std::panic::catch_unwind(|| {
            let res = adt_pg_lsn::pg_lsn_in("junk", None);
            assert!(res.is_ok(), "pg_lsn_in DIVERGENCE (injected err-plane)");
        });
        assert!(r.is_err(), "error-plane comparator must fire");
    }

    #[test]
    fn comparator_fires_on_image_skew() {
        let _serial = crate::c_oracle_serial();
        // image plane: pg_lsn_mi image vs corrupted C image
        let mut cimg = [0u8; 1024];
        let mut clen: i32 = 0;
        let crc = unsafe { pg_lsnfuzz_mi(100, 1, cimg.as_mut_ptr(), 1024, &mut clen) };
        assert_eq!(crc, 0);
        cimg[clen as usize - 1] ^= 0xFF; /* inject skew */
        let r = std::panic::catch_unwind(|| {
            let rimg = adt_pg_lsn::pg_lsn_mi(100, 1).unwrap();
            assert!(rimg.as_bytes() == &cimg[..clen as usize], "mi IMAGE (injected)");
        });
        assert!(r.is_err(), "image-plane comparator must fire");
    }

    /// Live C 18.3 witnesses (values pinned from psql in adt_pg_lsn's own
    /// tests): the oracle agrees with the real server on the record rows.
    #[test]
    fn oracle_matches_live_18_3_records() {
        let _serial = crate::c_oracle_serial();
        let mut img = [0u8; 1024];
        let mut len = 0i32;
        // '1/2' - '0/FF' = 4294967043
        assert_eq!(
            unsafe { pg_lsnfuzz_mi(0x1_0000_0002, 0xFF, img.as_mut_ptr(), 1024, &mut len) },
            0
        );
        let rimg = adt_pg_lsn::pg_lsn_mi(0x1_0000_0002, 0xFF).unwrap();
        assert_eq!(rimg.as_bytes(), &img[..len as usize]);

        // '0/FF' + 10 = 0x109; NaN planes
        let mut v = 0u64;
        let ten = CString::new("10").unwrap();
        assert_eq!(unsafe { pg_lsnfuzz_plimii(0xFF, ten.as_ptr(), 0, &mut v) }, 0);
        assert_eq!(v, 0x109);
        let nan = CString::new("NaN").unwrap();
        assert_eq!(
            unsafe { pg_lsnfuzz_plimii(0xFF, nan.as_ptr(), 0, &mut v) },
            C_ERR_NOT_SUPPORTED as i32
        );
        // numeric_pg_lsn range planes
        let neg = CString::new("-1").unwrap();
        assert_eq!(
            unsafe { pg_lsnfuzz_numeric_pg_lsn(neg.as_ptr(), &mut v) },
            C_ERR_INVALID_PARAM as i32
        );
        let big = CString::new("18446744073709551616").unwrap();
        assert_eq!(
            unsafe { pg_lsnfuzz_numeric_pg_lsn(big.as_ptr(), &mut v) },
            C_ERR_INVALID_PARAM as i32
        );
        let max = CString::new("18446744073709551615").unwrap();
        assert_eq!(unsafe { pg_lsnfuzz_numeric_pg_lsn(max.as_ptr(), &mut v) }, 0);
        assert_eq!(v, u64::MAX);
        // rounding: numeric_pg_lsn('0.5') rounds to 1 (round_var half-up)
        let half = CString::new("0.5").unwrap();
        assert_eq!(unsafe { pg_lsnfuzz_numeric_pg_lsn(half.as_ptr(), &mut v) }, 0);
        assert_eq!(v, 1);
    }
}
