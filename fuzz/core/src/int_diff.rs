//! int_diff — shipped adt_int (int2/int4/int2vector) vs vendored
//! PostgreSQL 18.3 C.
//!
//! Oracle: csrc/pg_int_io.c — verbatim int.c/numutils.c/pqformat.c bodies @
//! 62d6c7d3df ("Stamp 18.3"), in-process. Comparison planes: value bits
//! (i16/i32/bool images), full cstring images (out-functions, int2vectorout),
//! full varlena images (int2vectorin result, send wire images),
//! error-vs-no-error, errcode/sqlstate class, and the soft-error
//! (ErrorSaveContext) plane for the three in-functions — at BOTH the core
//! entry point and the shipped fmgr wrapper (fc_*, plus th_* thin twins via
//! the wrap! consistency check). Any mismatch panics -> libFuzzer crash
//! artifact = divergence reproducer.
//!
//! Input layout: [family][payload]. family%12 selects:
//!    0 = int2in hard-error plane      (payload = text)
//!    1 = int2in soft-error plane      (payload = text)
//!    2 = int4in hard-error plane      (payload = text)
//!    3 = int4in soft-error plane      (payload = text)
//!    4 = int2out/int4out image        (payload = [which][value LE])
//!    5 = int2vectorin hard+soft image (payload = [soft][text])
//!    6 = int2vectorout image          (payload = [n][hdr_sel][hdr_val LE4][i16 LE x n])
//!    7 = recv/send wire image         (payload = [which][wire bytes])
//!    8..11 = whole-family fn dispatch [fn_sel][a LE8][b LE8][c LE8][flags]
//!            (flags bit0=sub bit1=less)
//!
//! Skipped rows (recorded per the fuzzuproof-crate skill):
//!   - generate_series_int4 / generate_series_step_int4 / _support
//!     (oids 1066/1067/3994): SRF + planner machinery, series.rs OUT via the
//!     claim's named carve.
//!   - int2vectorrecv/int2vectorsend (2410/2411): array_recv/array_send
//!     delegations owned by arrayfuncs, not this crate.
//!   - hashint2/hashint4/hashchar (+extended, oids 425/441/446/449/450/454):
//!     C home is hashfunc.c and the kernels live in the hashfn crate; the
//!     registry rows here are thin, proved wrapper-level in the ledger.
//!   - Non-UTF-8 inputs to the in-functions: the shipped core APIs take
//!     &str; the fc wrappers' from_utf8_lossy path is exercised by crate
//!     tests (the lossy replacement char is a parse error on both sides).

use std::ffi::{c_char, c_int, CString};

use types_error::{
    PgError, SoftErrorContext, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DIVISION_BY_ZERO,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};

use adt_int as ints;

extern "C" {
    fn pg_diff_int2in(num: *const c_char, soft: c_int, out: *mut i16) -> c_int;
    fn pg_diff_int4in(num: *const c_char, soft: c_int, out: *mut i32) -> c_int;
    fn pg_diff_int2out(val: i16, buf: *mut c_char) -> c_int;
    fn pg_diff_int4out(val: i32, buf: *mut c_char) -> c_int;
    fn pg_diff_int2vectorin(
        s: *const c_char,
        soft: c_int,
        out_img: *mut u8,
        out_cap: c_int,
        out_len: *mut c_int,
    ) -> c_int;
    fn pg_diff_int2vectorout(img: *const u8, buf: *mut c_char, buflen: c_int) -> c_int;
    fn pg_diff_int2recv(data: *const u8, len: c_int, out: *mut i16) -> c_int;
    fn pg_diff_int4recv(data: *const u8, len: c_int, out: *mut i32) -> c_int;
    fn pg_diff_int2send(val: i16, buf: *mut u8) -> c_int;
    fn pg_diff_int4send(val: i32, buf: *mut u8) -> c_int;
    fn pg_diff_int_fn(
        fn_id: c_int,
        a: i64,
        b: i64,
        c: i64,
        sub: c_int,
        less: c_int,
        out: *mut i64,
    ) -> c_int;
}

/* Same class ints as the csrc shim's PG_DIFF_ERR_* */
const C_ERR_INVALID_TEXT: i32 = 1;
const C_ERR_OUT_OF_RANGE: i32 = 2;
const C_ERR_DIVISION_BY_ZERO: i32 = 5;
const C_ERR_INVALID_PRECEDING_FOLLOWING: i32 = 6;
const C_ERR_PROTOCOL_VIOLATION: i32 = 7;
const C_ERR_DATATYPE_MISMATCH: i32 = 8;

fn rust_err_class(e: &PgError) -> i32 {
    let s = e.sqlstate();
    if s == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if s == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if s == ERRCODE_DIVISION_BY_ZERO {
        C_ERR_DIVISION_BY_ZERO
    } else if s == ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE {
        C_ERR_INVALID_PRECEDING_FOLLOWING
    } else if s == ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL_VIOLATION
    } else if s == ERRCODE_DATATYPE_MISMATCH {
        C_ERR_DATATYPE_MISMATCH
    } else {
        99
    }
}

fn le_i64(payload: &[u8], idx: usize) -> i64 {
    let mut b = [0u8; 8];
    let s = payload.get(idx * 8..).unwrap_or(&[]);
    let n = s.len().min(8);
    b[..n].copy_from_slice(&s[..n]);
    i64::from_le_bytes(b)
}

/// Guard shared by the text-input arms: cstring-representable UTF-8 only.
fn text_guard(text: &[u8], max: usize) -> Option<(&str, CString)> {
    if text.len() > max || text.contains(&0) {
        return None;
    }
    let s = core::str::from_utf8(text).ok()?;
    let cs = CString::new(text).unwrap();
    Some((s, cs))
}

fn int2in_diff(text: &[u8], soft: bool) {
    let Some((s, cs)) = text_guard(text, 64) else {
        return;
    };
    let mut c_out: i16 = 0;
    let c_rc = unsafe { pg_diff_int2in(cs.as_ptr(), soft as c_int, &mut c_out) };

    if soft {
        let mut sec = SoftErrorContext::new(true);
        match ints::int2in(s, Some(&mut sec)) {
            Ok(v) => {
                if sec.error_occurred() {
                    let e = sec.take_error().unwrap();
                    assert!(
                        c_rc < 0,
                        "int2in soft: rust soft-error {:?}, C rc={c_rc} input={s:?}",
                        e.message()
                    );
                    assert_eq!(rust_err_class(&e), -c_rc, "int2in soft errclass input={s:?}");
                    assert_eq!(v, c_out, "int2in soft dummy value input={s:?}");
                } else {
                    assert_eq!(c_rc, 0, "int2in soft: rust ok, C rc={c_rc} input={s:?}");
                    assert_eq!(v, c_out, "int2in soft value input={s:?}");
                }
            }
            Err(e) => panic!(
                "int2in soft: hard error {:?} escaped the soft context input={s:?}",
                e.message()
            ),
        }
        // fc-wrapper soft plane: armed ErrorSaveNode context.
        let mut esn = types_fmgr::ErrorSaveNode::new(true);
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        fci.context = esn.fm_node_ptr();
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        match ints::builtins::fc_int2in(None, &mut fci) {
            Ok(d) => {
                if esn.ctx.error_occurred() {
                    let e = esn.ctx.take_error().unwrap();
                    assert_eq!(rust_err_class(&e), -c_rc, "fc_int2in soft errclass {s:?}");
                } else {
                    assert_eq!(c_rc, 0, "fc_int2in soft verdict {s:?}");
                }
                assert_eq!(d.as_i16(), c_out, "fc_int2in soft value {s:?}");
            }
            Err(e) => panic!(
                "fc_int2in soft: hard error {:?} escaped input={s:?}",
                e.message()
            ),
        }
    } else {
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        let fcr = ints::builtins::fc_int2in(None, &mut fci);
        match ints::int2in(s, None) {
            Ok(v) => {
                assert_eq!(c_rc, 0, "int2in: rust ok, C rc={c_rc} input={s:?}");
                assert_eq!(v, c_out, "int2in value input={s:?}");
                assert_eq!(fcr.expect("fc_int2in verdict split").as_i16(), v, "fc_int2in {s:?}");
            }
            Err(e) => {
                assert!(c_rc > 0, "int2in: rust err {:?}, C ok input={s:?}", e.message());
                assert_eq!(rust_err_class(&e), c_rc, "int2in errclass input={s:?}");
                assert_eq!(
                    fcr.expect_err("fc_int2in verdict split").message(),
                    e.message(),
                    "fc_int2in error split {s:?}"
                );
            }
        }
    }
}

fn int4in_diff(text: &[u8], soft: bool) {
    let Some((s, cs)) = text_guard(text, 64) else {
        return;
    };
    let mut c_out: i32 = 0;
    let c_rc = unsafe { pg_diff_int4in(cs.as_ptr(), soft as c_int, &mut c_out) };

    if soft {
        let mut sec = SoftErrorContext::new(true);
        match ints::int4in(s, Some(&mut sec)) {
            Ok(v) => {
                if sec.error_occurred() {
                    let e = sec.take_error().unwrap();
                    assert!(
                        c_rc < 0,
                        "int4in soft: rust soft-error {:?}, C rc={c_rc} input={s:?}",
                        e.message()
                    );
                    assert_eq!(rust_err_class(&e), -c_rc, "int4in soft errclass input={s:?}");
                    assert_eq!(v, c_out, "int4in soft dummy value input={s:?}");
                } else {
                    assert_eq!(c_rc, 0, "int4in soft: rust ok, C rc={c_rc} input={s:?}");
                    assert_eq!(v, c_out, "int4in soft value input={s:?}");
                }
            }
            Err(e) => panic!(
                "int4in soft: hard error {:?} escaped the soft context input={s:?}",
                e.message()
            ),
        }
        let mut esn = types_fmgr::ErrorSaveNode::new(true);
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        fci.context = esn.fm_node_ptr();
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        match ints::builtins::fc_int4in(None, &mut fci) {
            Ok(d) => {
                if esn.ctx.error_occurred() {
                    let e = esn.ctx.take_error().unwrap();
                    assert_eq!(rust_err_class(&e), -c_rc, "fc_int4in soft errclass {s:?}");
                } else {
                    assert_eq!(c_rc, 0, "fc_int4in soft verdict {s:?}");
                }
                assert_eq!(d.as_i32(), c_out, "fc_int4in soft value {s:?}");
            }
            Err(e) => panic!(
                "fc_int4in soft: hard error {:?} escaped input={s:?}",
                e.message()
            ),
        }
    } else {
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        let fcr = ints::builtins::fc_int4in(None, &mut fci);
        match ints::int4in(s, None) {
            Ok(v) => {
                assert_eq!(c_rc, 0, "int4in: rust ok, C rc={c_rc} input={s:?}");
                assert_eq!(v, c_out, "int4in value input={s:?}");
                assert_eq!(fcr.expect("fc_int4in verdict split").as_i32(), v, "fc_int4in {s:?}");
            }
            Err(e) => {
                assert!(c_rc > 0, "int4in: rust err {:?}, C ok input={s:?}", e.message());
                assert_eq!(rust_err_class(&e), c_rc, "int4in errclass input={s:?}");
                assert_eq!(
                    fcr.expect_err("fc_int4in verdict split").message(),
                    e.message(),
                    "fc_int4in error split {s:?}"
                );
            }
        }
    }
}

fn int2out_diff(val: i16) {
    let mut rbuf = [0u8; ints::MAXINT2LEN];
    let rlen = ints::int2out(val, &mut rbuf);
    let mut cbuf = [0 as c_char; 16];
    let clen = unsafe { pg_diff_int2out(val, cbuf.as_mut_ptr()) };
    assert!(clen >= 0, "int2out C error rc={clen} val={val}");
    let cbytes: &[u8] =
        unsafe { core::slice::from_raw_parts(cbuf.as_ptr().cast(), clen as usize) };
    assert_eq!(&rbuf[..rlen], cbytes, "int2out image val={val}");
    let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
    fci.set_arg(0, datum::Datum::from_i16(val));
    let d = ints::builtins::fc_int2out(None, &mut fci).expect("fc_int2out infallible");
    // SAFETY: fc_int2out returns a NUL-terminated cstring in thread scratch.
    let fcs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(fcs.to_bytes(), &rbuf[..rlen], "fc_int2out image val={val}");
}

fn int4out_diff(val: i32) {
    let mut rbuf = [0u8; ints::MAXINT4LEN];
    let rlen = ints::int4out(val, &mut rbuf);
    let mut cbuf = [0 as c_char; 16];
    let clen = unsafe { pg_diff_int4out(val, cbuf.as_mut_ptr()) };
    assert!(clen >= 0, "int4out C error rc={clen} val={val}");
    let cbytes: &[u8] =
        unsafe { core::slice::from_raw_parts(cbuf.as_ptr().cast(), clen as usize) };
    assert_eq!(&rbuf[..rlen], cbytes, "int4out image val={val}");
    let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
    fci.set_arg(0, datum::Datum::from_i32(val));
    let d = ints::builtins::fc_int4out(None, &mut fci).expect("fc_int4out infallible");
    // SAFETY: fc_int4out returns a NUL-terminated cstring in thread scratch.
    let fcs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(fcs.to_bytes(), &rbuf[..rlen], "fc_int4out image val={val}");
}

fn int2vectorin_diff(text: &[u8], soft: bool) {
    let Some((s, cs)) = text_guard(text, 256) else {
        return;
    };
    let mut c_img = [0u8; 4096];
    let mut c_len: c_int = 0;
    let c_rc = unsafe {
        pg_diff_int2vectorin(cs.as_ptr(), soft as c_int, c_img.as_mut_ptr(), 4096, &mut c_len)
    };
    assert_ne!(c_rc, 99, "int2vectorin C image overflow (raise cap) input={s:?}");

    let cx = mcx::MemoryContext::new("int_diff");
    let mcx = cx.mcx();
    if soft {
        let mut sec = SoftErrorContext::new(true);
        match ints::int2vectorin(mcx, s, Some(&mut sec)) {
            Ok(img) => {
                if sec.error_occurred() {
                    let e = sec.take_error().unwrap();
                    assert!(
                        c_rc < 0,
                        "int2vectorin soft: rust soft-error {:?}, C rc={c_rc} input={s:?}",
                        e.message()
                    );
                    assert_eq!(
                        rust_err_class(&e),
                        -c_rc,
                        "int2vectorin soft errclass input={s:?}"
                    );
                } else {
                    assert_eq!(c_rc, 0, "int2vectorin soft: rust ok, C rc={c_rc} input={s:?}");
                    let img = img.expect("int2vectorin ok image");
                    assert_eq!(
                        img.as_slice(),
                        &c_img[..c_len as usize],
                        "int2vectorin soft image input={s:?}"
                    );
                }
            }
            Err(e) => panic!(
                "int2vectorin soft: hard error {:?} escaped the soft context input={s:?}",
                e.message()
            ),
        }
        // fc-wrapper soft plane: armed ErrorSaveNode + result mcx; on a soft
        // error the wrapper sets isnull and returns a NULL datum.
        let mut esn = types_fmgr::ErrorSaveNode::new(true);
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        fci.context = esn.fm_node_ptr();
        // SAFETY: cx outlives the call.
        unsafe { fci.set_result_mcx(mcx) };
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        match ints::builtins::fc_int2vectorin(None, &mut fci) {
            Ok(d) => {
                if esn.ctx.error_occurred() {
                    let e = esn.ctx.take_error().unwrap();
                    assert_eq!(rust_err_class(&e), -c_rc, "fc_int2vectorin soft errclass {s:?}");
                    assert!(fci.isnull, "fc_int2vectorin soft: isnull unset {s:?}");
                } else {
                    assert_eq!(c_rc, 0, "fc_int2vectorin soft verdict {s:?}");
                    let p = d.as_usize() as *const u8;
                    // SAFETY: leaked in-mcx varlena image, 4B LE varsize head.
                    let full = unsafe {
                        let sz = (core::ptr::read_unaligned(p.cast::<u32>()) >> 2) as usize;
                        core::slice::from_raw_parts(p, sz)
                    };
                    assert_eq!(full, &c_img[..c_len as usize], "fc_int2vectorin soft image {s:?}");
                }
            }
            Err(e) => panic!(
                "fc_int2vectorin soft: hard error {:?} escaped input={s:?}",
                e.message()
            ),
        }
    } else {
        match ints::int2vectorin(mcx, s, None) {
            Ok(img) => {
                assert_eq!(c_rc, 0, "int2vectorin: rust ok, C rc={c_rc} input={s:?}");
                let img = img.expect("int2vectorin ok image");
                assert_eq!(
                    img.as_slice(),
                    &c_img[..c_len as usize],
                    "int2vectorin image input={s:?}"
                );
            }
            Err(e) => {
                assert!(
                    c_rc > 0,
                    "int2vectorin: rust err {:?}, C ok input={s:?}",
                    e.message()
                );
                assert_eq!(rust_err_class(&e), c_rc, "int2vectorin errclass input={s:?}");
            }
        }
        // fc plane (hard path only; result context armed).
        let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
        // SAFETY: cx outlives the call.
        unsafe { fci.set_result_mcx(mcx) };
        fci.set_arg(0, datum::Datum::from_usize(cs.as_ptr() as usize));
        match ints::builtins::fc_int2vectorin(None, &mut fci) {
            Ok(d) => {
                assert_eq!(c_rc, 0, "fc_int2vectorin verdict {s:?}");
                let p = d.as_usize() as *const u8;
                // SAFETY: fc returns a leaked in-mcx varlena image; header is
                // the 4B little-endian varsize.
                let full = unsafe {
                    let sz = (core::ptr::read_unaligned(p.cast::<u32>()) >> 2) as usize;
                    core::slice::from_raw_parts(p, sz)
                };
                assert_eq!(full, &c_img[..c_len as usize], "fc_int2vectorin image {s:?}");
            }
            Err(e) => {
                assert!(c_rc > 0, "fc_int2vectorin: rust err, C ok {s:?}");
                assert_eq!(rust_err_class(&e), c_rc, "fc_int2vectorin errclass {s:?}");
            }
        }
    }
}

/// Payload: [n][hdr_sel][hdr_val LE4][i16 LE x n]; hdr_sel%4 != 0 corrupts
/// exactly one header field (1=ndim, 2=dataoffset, 3=elemtype) so the
/// check_valid_int2vector arm gets genuine single-field-difference coverage.
fn int2vectorout_diff(payload: &[u8]) {
    let Some((&n_raw, rest)) = payload.split_first() else {
        return;
    };
    let Some((&hdr_sel, rest)) = rest.split_first() else {
        return;
    };
    let mut hv = [0u8; 4];
    let Some(hvb) = rest.get(..4) else { return };
    hv.copy_from_slice(hvb);
    let hdr_val = i32::from_le_bytes(hv);
    let rest = &rest[4..];

    let n = (n_raw as usize) % 33;
    let mut elems = [0i16; 32];
    for (i, e) in elems.iter_mut().enumerate().take(n) {
        let mut b = [0u8; 2];
        let Some(sl) = rest.get(i * 2..i * 2 + 2) else {
            return;
        };
        b.copy_from_slice(sl);
        *e = i16::from_le_bytes(b);
    }
    let elems = &elems[..n];

    let cx = mcx::MemoryContext::new("int_diff");
    let mcx = cx.mcx();
    let mut img = ints::buildint2vector(mcx, elems).expect("buildint2vector");

    // Optionally corrupt exactly one header field (both sides see the same
    // fields, so verdicts must still agree).
    let (mut ndim, mut dataoffset, mut elemtype) = (1i32, 0i32, 21u32);
    match hdr_sel % 4 {
        1 => ndim = hdr_val,
        2 => dataoffset = hdr_val,
        3 => elemtype = hdr_val as u32,
        _ => {}
    }
    if hdr_sel % 4 != 0 {
        // int2vector header layout: vl_len_(4) ndim(4) dataoffset(4)
        // elemtype(4) dim1(4) lbound1(4).
        img[4..8].copy_from_slice(&ndim.to_ne_bytes());
        img[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
        img[12..16].copy_from_slice(&elemtype.to_ne_bytes());
    }

    let mut cbuf = [0 as c_char; 512];
    let c_rc = unsafe { pg_diff_int2vectorout(img.as_ptr(), cbuf.as_mut_ptr(), 512) };

    let r = ints::int2vectorout(mcx, ndim, dataoffset, elemtype as types_core::Oid, elems);
    match r {
        Ok(out) => {
            assert!(c_rc >= 0, "int2vectorout: rust ok, C rc={c_rc} n={n}");
            let cbytes: &[u8] =
                unsafe { core::slice::from_raw_parts(cbuf.as_ptr().cast(), c_rc as usize) };
            assert_eq!(out.as_slice(), cbytes, "int2vectorout image n={n}");
        }
        Err(e) => {
            assert!(c_rc < 0, "int2vectorout: rust err {:?}, C ok n={n}", e.message());
            assert_eq!(rust_err_class(&e), -c_rc, "int2vectorout errclass n={n}");
        }
    }

    // fc plane over the same (possibly corrupted) image.
    let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
    // SAFETY: cx outlives the call.
    unsafe { fci.set_result_mcx(mcx) };
    fci.set_arg(0, datum::Datum::from_usize(img.as_ptr() as usize));
    match ints::builtins::fc_int2vectorout(None, &mut fci) {
        Ok(d) => {
            assert!(c_rc >= 0, "fc_int2vectorout verdict n={n}");
            // SAFETY: fc returns a NUL-terminated in-mcx cstring datum.
            let fcs =
                unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
            let cbytes: &[u8] =
                unsafe { core::slice::from_raw_parts(cbuf.as_ptr().cast(), c_rc as usize) };
            assert_eq!(fcs.to_bytes(), cbytes, "fc_int2vectorout image n={n}");
        }
        Err(e) => {
            assert!(c_rc < 0, "fc_int2vectorout: rust err, C ok n={n}");
            assert_eq!(rust_err_class(&e), -c_rc, "fc_int2vectorout errclass n={n}");
        }
    }
}

fn recv_send_diff(payload: &[u8]) {
    let Some((&which, wire)) = payload.split_first() else {
        return;
    };
    if wire.len() > 32 {
        return;
    }
    let cx = mcx::MemoryContext::new("int_diff");
    let mcx = cx.mcx();
    let mk_msg = |bytes: &[u8]| -> Option<stringinfo::StringInfo<'_>> {
        let mut vec = mcx::vec_with_capacity_in::<u8>(mcx, bytes.len()).ok()?;
        mcx::vec_append_bytes(&mut vec, bytes).ok()?;
        stringinfo::StringInfo::from_vec(vec).ok()
    };

    if which & 1 == 0 {
        let mut c_out: i16 = 0;
        let c_rc = unsafe { pg_diff_int2recv(wire.as_ptr(), wire.len() as c_int, &mut c_out) };
        let Some(mut msg) = mk_msg(wire) else { return };
        match ints::int2recv(&mut msg) {
            Ok(v) => {
                assert_eq!(c_rc, 0, "int2recv: rust ok, C rc={c_rc} wire={wire:x?}");
                assert_eq!(v, c_out, "int2recv value wire={wire:x?}");
                // send image: full bytea image (varlena header + payload).
                let mut c_img = [0u8; 64];
                let c_ilen = unsafe { pg_diff_int2send(v, c_img.as_mut_ptr()) };
                assert_eq!(c_ilen, 6, "int2send image length v={v}");
                let sent = ints::int2send(mcx, v).expect("int2send");
                assert_eq!(
                    sent.data(),
                    &c_img[4..c_ilen as usize],
                    "int2send payload image v={v}"
                );
                // fc planes.
                let Some(mut msg2) = mk_msg(wire) else { return };
                let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
                fci.set_arg(0, datum::Datum::from_usize(&mut msg2 as *mut _ as usize));
                let d = ints::builtins::fc_int2recv(None, &mut fci).expect("fc_int2recv split");
                assert_eq!(d.as_i16(), v, "fc_int2recv value wire={wire:x?}");
                let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
                fci.set_arg(0, datum::Datum::from_i16(v));
                // SAFETY: cx outlives the call.
                unsafe { fci.set_result_mcx(mcx) };
                let _ = ints::builtins::fc_int2send(None, &mut fci).expect("fc_int2send");
            }
            Err(e) => {
                assert!(c_rc > 0, "int2recv: rust err {:?}, C ok wire={wire:x?}", e.message());
                assert_eq!(rust_err_class(&e), c_rc, "int2recv errclass wire={wire:x?}");
            }
        }
    } else {
        let mut c_out: i32 = 0;
        let c_rc = unsafe { pg_diff_int4recv(wire.as_ptr(), wire.len() as c_int, &mut c_out) };
        let Some(mut msg) = mk_msg(wire) else { return };
        match ints::int4recv(&mut msg) {
            Ok(v) => {
                assert_eq!(c_rc, 0, "int4recv: rust ok, C rc={c_rc} wire={wire:x?}");
                assert_eq!(v, c_out, "int4recv value wire={wire:x?}");
                let mut c_img = [0u8; 64];
                let c_ilen = unsafe { pg_diff_int4send(v, c_img.as_mut_ptr()) };
                assert_eq!(c_ilen, 8, "int4send image length v={v}");
                let sent = ints::int4send(mcx, v).expect("int4send");
                assert_eq!(
                    sent.data(),
                    &c_img[4..c_ilen as usize],
                    "int4send payload image v={v}"
                );
                let Some(mut msg2) = mk_msg(wire) else { return };
                let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
                fci.set_arg(0, datum::Datum::from_usize(&mut msg2 as *mut _ as usize));
                let d = ints::builtins::fc_int4recv(None, &mut fci).expect("fc_int4recv split");
                assert_eq!(d.as_i32(), v, "fc_int4recv value wire={wire:x?}");
                let mut fci = types_fmgr::LocalFcinfo::<1>::new(0);
                fci.set_arg(0, datum::Datum::from_i32(v));
                // SAFETY: cx outlives the call.
                unsafe { fci.set_result_mcx(mcx) };
                let _ = ints::builtins::fc_int4send(None, &mut fci).expect("fc_int4send");
            }
            Err(e) => {
                assert!(c_rc > 0, "int4recv: rust err {:?}, C ok wire={wire:x?}", e.message());
                assert_eq!(rust_err_class(&e), c_rc, "int4recv errclass wire={wire:x?}");
            }
        }
    }
}

/// One fn-family case against pg_diff_int_fn. fn ids match the C dispatcher.
#[allow(clippy::too_many_lines)]
fn fn_diff(fn_id: i32, a: i64, b: i64, c: i64, sub: bool, less: bool) {
    // bool_int4's argument is a bool Datum by fmgr contract (0/1); C's
    // (bool) cast truthifies any nonzero i64, which no real caller can
    // produce — normalize the plane to the contract domain.
    let a = if fn_id == 47 { a & 1 } else { a };
    let mut c_out: i64 = 0;
    let c_rc = unsafe {
        pg_diff_int_fn(fn_id as c_int, a, b, c, sub as c_int, less as c_int, &mut c_out)
    };

    let a32 = a as i32;
    let a16 = a as i16;
    let b32 = b as i32;
    let b16 = b as i16;

    use core::ptr::NonNull;
    use datum::Datum;
    use types_fmgr::LocalFcinfo;
    let mut fci = LocalFcinfo::<5>::new(0);

    macro_rules! wrap {
        ([$($arg:expr),*], $fc:path, $conv:expr) => {{
            let mut i = 0usize;
            $( fci.set_arg(i, $arg); i += 1; )*
            let _ = i;
            #[allow(clippy::redundant_closure_call)]
            $fc(None, &mut fci).map(|d| ($conv)(d))
        }};
        ([$($arg:expr),*], $fc:path, $th:path, $conv:expr) => {{
            let mut i = 0usize;
            $( fci.set_arg(i, $arg); i += 1; )*
            let _ = i;
            #[allow(clippy::redundant_closure_call)]
            let fcr = $fc(None, &mut fci).map(|d| ($conv)(d));
            // SAFETY: fci is a live 5-slot fcinfo image; registered arity of
            // every th_ twin here is <= the args just set.
            #[allow(clippy::redundant_closure_call)]
            let thr = unsafe { $th(NonNull::from(&mut fci).cast()) }.map(|d| ($conv)(d));
            match (&fcr, &thr) {
                (Ok(x), Ok(y)) => assert_eq!(x, y, "fc/th value split fn {fn_id}"),
                (Err(x), Err(y)) => {
                    assert_eq!(x.message(), y.message(), "fc/th error split fn {fn_id}")
                }
                _ => panic!("fc/th verdict split fn {fn_id}: {fcr:?} vs {thr:?}"),
            }
            fcr
        }};
    }
    let vbool = |d: Datum| d.as_bool() as i64;
    let vi32 = |d: Datum| d.as_i32() as u32 as i64; // C PG_RETURN_INT32 image
    let vi16 = |d: Datum| d.as_i16() as u16 as i64; // C PG_RETURN_INT16 image
    let d64 = Datum::from_i64;
    let d32 = Datum::from_i32;
    let d16 = Datum::from_i16;

    use ints::builtins as fcm;
    let r: Result<i64, Box<PgError>> = match fn_id {
        1 => wrap!([d32(a32)], fcm::fc_int4um, fcm::th_int4um, vi32),
        2 => wrap!([d32(a32)], fcm::fc_int4up, fcm::th_int4up, vi32),
        3 => wrap!([d32(a32), d32(b32)], fcm::fc_int4pl, fcm::th_int4pl, vi32),
        4 => wrap!([d32(a32), d32(b32)], fcm::fc_int4mi, fcm::th_int4mi, vi32),
        5 => wrap!([d32(a32), d32(b32)], fcm::fc_int4mul, fcm::th_int4mul, vi32),
        6 => wrap!([d32(a32), d32(b32)], fcm::fc_int4div, fcm::th_int4div, vi32),
        7 => wrap!([d32(a32)], fcm::fc_int4abs, fcm::th_int4abs, vi32),
        8 => wrap!([d32(a32), d32(b32)], fcm::fc_int4mod, fcm::th_int4mod, vi32),
        9 => wrap!([d32(a32), d32(b32)], fcm::fc_int4gcd, fcm::th_int4gcd, vi32),
        10 => wrap!([d32(a32), d32(b32)], fcm::fc_int4lcm, fcm::th_int4lcm, vi32),
        11 => wrap!([d32(a32)], fcm::fc_int4inc, fcm::th_int4inc, vi32),
        12 => wrap!([d16(a16)], fcm::fc_int2um, fcm::th_int2um, vi16),
        13 => wrap!([d16(a16)], fcm::fc_int2up, fcm::th_int2up, vi16),
        14 => wrap!([d16(a16), d16(b16)], fcm::fc_int2pl, fcm::th_int2pl, vi16),
        15 => wrap!([d16(a16), d16(b16)], fcm::fc_int2mi, fcm::th_int2mi, vi16),
        16 => wrap!([d16(a16), d16(b16)], fcm::fc_int2mul, fcm::th_int2mul, vi16),
        17 => wrap!([d16(a16), d16(b16)], fcm::fc_int2div, fcm::th_int2div, vi16),
        18 => wrap!([d16(a16)], fcm::fc_int2abs, fcm::th_int2abs, vi16),
        19 => wrap!([d16(a16), d16(b16)], fcm::fc_int2mod, fcm::th_int2mod, vi16),
        20 => wrap!([d16(a16), d16(b16)], fcm::fc_int2larger, fcm::th_int2larger, vi16),
        21 => wrap!([d16(a16), d16(b16)], fcm::fc_int2smaller, fcm::th_int2smaller, vi16),
        22 => wrap!([d32(a32), d32(b32)], fcm::fc_int4larger, fcm::th_int4larger, vi32),
        23 => wrap!([d32(a32), d32(b32)], fcm::fc_int4smaller, fcm::th_int4smaller, vi32),
        24 => wrap!([d16(a16), d32(b32)], fcm::fc_int24pl, fcm::th_int24pl, vi32),
        25 => wrap!([d16(a16), d32(b32)], fcm::fc_int24mi, fcm::th_int24mi, vi32),
        26 => wrap!([d16(a16), d32(b32)], fcm::fc_int24mul, fcm::th_int24mul, vi32),
        27 => wrap!([d16(a16), d32(b32)], fcm::fc_int24div, fcm::th_int24div, vi32),
        28 => wrap!([d32(a32), d16(b16)], fcm::fc_int42pl, fcm::th_int42pl, vi32),
        29 => wrap!([d32(a32), d16(b16)], fcm::fc_int42mi, fcm::th_int42mi, vi32),
        30 => wrap!([d32(a32), d16(b16)], fcm::fc_int42mul, fcm::th_int42mul, vi32),
        31 => wrap!([d32(a32), d16(b16)], fcm::fc_int42div, fcm::th_int42div, vi32),
        32 => wrap!([d32(a32), d32(b32)], fcm::fc_int4and, fcm::th_int4and, vi32),
        33 => wrap!([d32(a32), d32(b32)], fcm::fc_int4or, fcm::th_int4or, vi32),
        34 => wrap!([d32(a32), d32(b32)], fcm::fc_int4xor, fcm::th_int4xor, vi32),
        35 => wrap!([d32(a32)], fcm::fc_int4not, fcm::th_int4not, vi32),
        36 => wrap!([d32(a32), d32(b32)], fcm::fc_int4shl, fcm::th_int4shl, vi32),
        37 => wrap!([d32(a32), d32(b32)], fcm::fc_int4shr, fcm::th_int4shr, vi32),
        38 => wrap!([d16(a16), d16(b16)], fcm::fc_int2and, fcm::th_int2and, vi16),
        39 => wrap!([d16(a16), d16(b16)], fcm::fc_int2or, fcm::th_int2or, vi16),
        40 => wrap!([d16(a16), d16(b16)], fcm::fc_int2xor, fcm::th_int2xor, vi16),
        41 => wrap!([d16(a16)], fcm::fc_int2not, fcm::th_int2not, vi16),
        42 => wrap!([d16(a16), d32(b32)], fcm::fc_int2shl, fcm::th_int2shl, vi16),
        43 => wrap!([d16(a16), d32(b32)], fcm::fc_int2shr, fcm::th_int2shr, vi16),
        44 => wrap!([d16(a16)], fcm::fc_i2toi4, fcm::th_i2toi4, vi32),
        45 => wrap!([d32(a32)], fcm::fc_i4toi2, fcm::th_i4toi2, vi16),
        46 => wrap!([d32(a32)], fcm::fc_int4_bool, fcm::th_int4_bool, vbool),
        47 => wrap!(
            [Datum::from_bool(a & 1 != 0)],
            fcm::fc_bool_int4,
            fcm::th_bool_int4,
            vi32
        ),
        48 => wrap!([d32(a32), d32(b32)], fcm::fc_int4eq, fcm::th_int4eq, vbool),
        49 => wrap!([d32(a32), d32(b32)], fcm::fc_int4ne, fcm::th_int4ne, vbool),
        50 => wrap!([d32(a32), d32(b32)], fcm::fc_int4lt, fcm::th_int4lt, vbool),
        51 => wrap!([d32(a32), d32(b32)], fcm::fc_int4le, fcm::th_int4le, vbool),
        52 => wrap!([d32(a32), d32(b32)], fcm::fc_int4gt, fcm::th_int4gt, vbool),
        53 => wrap!([d32(a32), d32(b32)], fcm::fc_int4ge, fcm::th_int4ge, vbool),
        54 => wrap!([d16(a16), d16(b16)], fcm::fc_int2eq, fcm::th_int2eq, vbool),
        55 => wrap!([d16(a16), d16(b16)], fcm::fc_int2ne, fcm::th_int2ne, vbool),
        56 => wrap!([d16(a16), d16(b16)], fcm::fc_int2lt, fcm::th_int2lt, vbool),
        57 => wrap!([d16(a16), d16(b16)], fcm::fc_int2le, fcm::th_int2le, vbool),
        58 => wrap!([d16(a16), d16(b16)], fcm::fc_int2gt, fcm::th_int2gt, vbool),
        59 => wrap!([d16(a16), d16(b16)], fcm::fc_int2ge, fcm::th_int2ge, vbool),
        60 => wrap!([d16(a16), d32(b32)], fcm::fc_int24eq, fcm::th_int24eq, vbool),
        61 => wrap!([d16(a16), d32(b32)], fcm::fc_int24ne, fcm::th_int24ne, vbool),
        62 => wrap!([d16(a16), d32(b32)], fcm::fc_int24lt, fcm::th_int24lt, vbool),
        63 => wrap!([d16(a16), d32(b32)], fcm::fc_int24le, fcm::th_int24le, vbool),
        64 => wrap!([d16(a16), d32(b32)], fcm::fc_int24gt, fcm::th_int24gt, vbool),
        65 => wrap!([d16(a16), d32(b32)], fcm::fc_int24ge, fcm::th_int24ge, vbool),
        66 => wrap!([d32(a32), d16(b16)], fcm::fc_int42eq, fcm::th_int42eq, vbool),
        67 => wrap!([d32(a32), d16(b16)], fcm::fc_int42ne, fcm::th_int42ne, vbool),
        68 => wrap!([d32(a32), d16(b16)], fcm::fc_int42lt, fcm::th_int42lt, vbool),
        69 => wrap!([d32(a32), d16(b16)], fcm::fc_int42le, fcm::th_int42le, vbool),
        70 => wrap!([d32(a32), d16(b16)], fcm::fc_int42gt, fcm::th_int42gt, vbool),
        71 => wrap!([d32(a32), d16(b16)], fcm::fc_int42ge, fcm::th_int42ge, vbool),
        72 => wrap!(
            [d32(a32), d32(b32), d32(c as i32), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int4_int4,
            vbool
        ),
        73 => wrap!(
            [d32(a32), d32(b32), d16(c as i16), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int4_int2,
            vbool
        ),
        74 => wrap!(
            [d32(a32), d32(b32), d64(c), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int4_int8,
            vbool
        ),
        75 => wrap!(
            [d16(a16), d16(b16), d32(c as i32), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int2_int4,
            vbool
        ),
        76 => wrap!(
            [d16(a16), d16(b16), d16(c as i16), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int2_int2,
            vbool
        ),
        77 => wrap!(
            [d16(a16), d16(b16), d64(c), Datum::from_bool(sub), Datum::from_bool(less)],
            fcm::fc_in_range_int2_int8,
            vbool
        ),
        _ => return,
    };

    match r {
        Ok(v) => {
            assert_eq!(
                c_rc, 0,
                "fn {fn_id}: rust ok v={v}, C rc={c_rc} a={a} b={b} c={c} sub={sub} less={less}"
            );
            assert_eq!(v, c_out, "fn {fn_id} value a={a} b={b} c={c} sub={sub} less={less}");
        }
        Err(e) => {
            assert!(
                c_rc > 0,
                "fn {fn_id}: rust err {:?}, C ok a={a} b={b} c={c}",
                e.message()
            );
            assert_eq!(rust_err_class(&e), c_rc, "fn {fn_id} errclass a={a} b={b} c={c}");
        }
    }
}

pub fn int_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    // Exception-audit rail: every exec turns the crate's never_reached! arms
    // (OOM defensive arms, recorded exception rows) into panics if they fire.
    static ARM: std::sync::Once = std::sync::Once::new();
    ARM.call_once(types_error::exceptions::arm_exception_audit);
    let Some((&family, payload)) = data.split_first() else {
        return;
    };
    match family % 12 {
        0 => int2in_diff(payload, false),
        1 => int2in_diff(payload, true),
        2 => int4in_diff(payload, false),
        3 => int4in_diff(payload, true),
        4 => {
            let Some((&which, rest)) = payload.split_first() else {
                return;
            };
            if which & 1 == 0 {
                int2out_diff(le_i64(rest, 0) as i16);
            } else {
                int4out_diff(le_i64(rest, 0) as i32);
            }
        }
        5 => {
            let Some((&soft, rest)) = payload.split_first() else {
                return;
            };
            int2vectorin_diff(rest, soft & 1 != 0);
        }
        6 => int2vectorout_diff(payload),
        7 => recv_send_diff(payload),
        _ => {
            let Some((&fn_sel, rest)) = payload.split_first() else {
                return;
            };
            let fn_id = 1 + (fn_sel as i32) % 77;
            let a = le_i64(rest, 0);
            let b = le_i64(rest, 1);
            let c = le_i64(rest, 2);
            let flags = rest.get(24).copied().unwrap_or(0);
            fn_diff(fn_id, a, b, c, flags & 1 != 0, flags & 2 != 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_in_out() {
        let _serial = crate::c_oracle_serial();
        for soft in [false, true] {
            int2in_diff(b"12345", soft);
            int2in_diff(b"-32768", soft);
            int2in_diff(b"32768", soft);
            int2in_diff(b" +42 ", soft);
            int2in_diff(b"0x7fff", soft);
            int2in_diff(b"0b1_01", soft);
            int2in_diff(b"bogus", soft);
            int2in_diff(b"", soft);
            int4in_diff(b"2147483647", soft);
            int4in_diff(b"-2147483648", soft);
            int4in_diff(b"2147483648", soft);
            int4in_diff(b"9999999999", soft);
            int4in_diff(b"0o777", soft);
            int4in_diff(b"1_000_000", soft);
            int4in_diff(b"_1", soft);
            int4in_diff(b"1_", soft);
        }
        for v in [0i16, 1, -1, i16::MIN, i16::MAX, 100, -9999] {
            int2out_diff(v);
        }
        for v in [0i32, 1, -1, i32::MIN, i32::MAX, 1000000, -12345678] {
            int4out_diff(v);
        }
    }

    #[test]
    fn smoke_vector() {
        let _serial = crate::c_oracle_serial();
        for soft in [false, true] {
            int2vectorin_diff(b"", soft);
            int2vectorin_diff(b"1 2 3", soft);
            int2vectorin_diff(b"  -32768   32767 0 ", soft);
            int2vectorin_diff(b"32768", soft);
            int2vectorin_diff(b"1 bogus", soft);
            int2vectorin_diff(b"1,2", soft);
            int2vectorin_diff(b"999999999999999999999999", soft);
        }
        // valid image, then each header field corrupted individually
        let mut p = vec![3u8, 0, 0, 0, 0, 0];
        p.extend_from_slice(&1i16.to_le_bytes());
        p.extend_from_slice(&(-1i16).to_le_bytes());
        p.extend_from_slice(&i16::MIN.to_le_bytes());
        int2vectorout_diff(&p);
        for sel in 1u8..=3 {
            let mut q = p.clone();
            q[1] = sel;
            q[2] = 7; // corrupt value 7
            int2vectorout_diff(&q);
        }
    }

    #[test]
    fn smoke_recv_send() {
        let _serial = crate::c_oracle_serial();
        // exact, short, long wires; both widths
        recv_send_diff(&[0, 0x12, 0x34]);
        recv_send_diff(&[0, 0xff, 0xff]);
        recv_send_diff(&[0, 1]);
        recv_send_diff(&[0]);
        recv_send_diff(&[0, 1, 2, 3]);
        recv_send_diff(&[1, 0x12, 0x34, 0x56, 0x78]);
        recv_send_diff(&[1, 0xff, 0xff, 0xff, 0xff]);
        recv_send_diff(&[1, 1, 2]);
        recv_send_diff(&[1, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn smoke_fns() {
        let _serial = crate::c_oracle_serial();
        let spots = [
            0i64,
            1,
            -1,
            2,
            -2,
            i16::MIN as i64,
            i16::MAX as i64,
            i32::MIN as i64,
            i32::MAX as i64,
        ];
        for id in 1..=77 {
            for &a in &spots {
                for &b in &[0i64, 1, -1, i32::MIN as i64, i32::MAX as i64] {
                    fn_diff(id, a, b, 1, false, true);
                    fn_diff(id, a, b, -1, true, false);
                    fn_diff(id, a, b, i64::MIN, true, true);
                }
            }
        }
    }

    #[test]
    fn seed_corpus_replay() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/int_diff");
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        let mut n = 0;
        for ent in rd.flatten() {
            if ent.path().is_file() {
                if let Ok(bytes) = std::fs::read(ent.path()) {
                    int_diff(&bytes);
                    n += 1;
                }
            }
        }
        assert!(n >= 30, "committed int_diff corpus went missing (found {n})");
    }
}
