//! tsvector_core_diff: differential fuzz driver — shipped Rust
//! `adt_tsvector_core` vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C (csrc/pg_tsvector_core_io.c + verbatim files under
//! csrc/tsvec/). Crate under test: crates/backend/utils/adt/tsvector_core.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits (tsvector
//! IMAGE payload, out-text bytes, send wire bytes, cmp/bool/length scalars,
//! to_array element lists), error verdict, errcode/sqlstate CLASS, plus the
//! soft-error (escontext) verdict for tsvectorin. Message text out of scope.
//!
//! ENCODING PIN: database encoding = UTF-8 on both sides (C oracle hardwires
//! pg_utf_mblen; this driver calls SetDatabaseEncoding(PG_UTF8) per exec).
//! Text arms feed VALID UTF-8 without NUL only, mirroring the server's
//! pg_verifymbstr precondition on cstring input.
//!
//! Input layout: [sel][payload]; sel % 8 picks the arm:
//!   0 in/out/send   payload = tsvectorin text (hard + soft escontext modes;
//!                   on success: image plane, then tsvectorout + tsvectorsend
//!                   on the parsed image; fc plane for in/out/send)
//!   1 recv          payload = raw wire bytes into tsvectorrecv (image +
//!                   error planes; misordered/needs-sort paths; fc plane)
//!   2 cmp family    payload = [u16 len1][text1][text2]; all 7 wrappers
//!   3 unary ops     payload = [sub][params][text]; sub % 5: strip / length /
//!                   setweight(char incl invalid) / filter(weight chars incl
//!                   nulls + invalid) / to_array (element-list plane)
//!   4 concat        payload = [u16 len1][text1][text2] (maxpos offsetting,
//!                   add_pos MAXNUMPOS / position-ceiling clamps)
//!   5 lexeme ops    payload = [sub][lexeme list][text]; sub % 3:
//!                   delete_str / delete_arr (nulls skipped) /
//!                   setweight_by_filter
//!   6 array_to_tsvector  payload = lexeme list (null + empty-string error
//!                   arms; sort + dedup)
//!   7 match         payload = [u16 vlen][vtext][tsq_gen bytes];
//!                   ts_match_vq + ts_match_qv (TS_execute / phrase engine)
//!
//! Lexeme lists decode as [u8 n] then per element [u8 tag][tag bytes];
//! tag 0xFF = SQL NULL element (delete_arr skips, array_to_tsvector errors).
//!
//! SKIPPED rows (executable coverage lives in routes/exceptions, not here):
//!   - tsvector_unnest (oid 3322): SRF/funcapi + tupdesc/heaptuple/typcache
//!     machinery — phase-1 NAMED CARVE (same carve as the C side's
//!     `PG_DIFF CARVE` block for tsvector_unnest).
//!
//! valcrc on generated tsquery images is 0 on BOTH sides — TS_execute /
//! checkcondition_str never read it (tsq_gen.rs header).

use std::ffi::CString;

use datum::{Datum, NullableDatum};
use mcx::MemoryContext;
use types_error::{
    PgError, SoftErrorContext, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_ZERO_LENGTH_CHARACTER_STRING,
};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction, PackedVarlena};

use adt_tsvector_core::builtins as fcb;
use adt_tsvector_core::io::{
    tsvector_in_core, tsvector_out_core, tsvector_recv_core, tsvector_send_core,
};
use adt_tsvector_core::layout::TsVec;

use crate::tsq_gen::gen_tsquery_payload;

extern "C" {
    fn pg_diff_tsvec_in(
        input: *const std::ffi::c_char,
        soft: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_out(
        img: *const u8,
        imglen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_send(
        img: *const u8,
        imglen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_recv(
        wire: *const u8,
        wirelen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_cmp(
        a: *const u8,
        alen: i32,
        b: *const u8,
        blen: i32,
        cmp: *mut i32,
        boolbits: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_strip(
        img: *const u8,
        imglen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_length(img: *const u8, imglen: i32, res: *mut i32) -> i32;
    fn pg_diff_tsvec_setweight(
        img: *const u8,
        imglen: i32,
        w: std::ffi::c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_setweight_by_filter(
        img: *const u8,
        imglen: i32,
        w: std::ffi::c_char,
        lexbuf: *const u8,
        lexlens: *const i32,
        nlex: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_concat(
        a: *const u8,
        alen: i32,
        b: *const u8,
        blen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_filter(
        img: *const u8,
        imglen: i32,
        weights: *const std::ffi::c_char,
        wnulls: *const u8,
        nw: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_delete_str(
        img: *const u8,
        imglen: i32,
        lex: *const u8,
        lexlen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_delete_arr(
        img: *const u8,
        imglen: i32,
        lexbuf: *const u8,
        lexlens: *const i32,
        nlex: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_tsvec_to_array(
        img: *const u8,
        imglen: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_array_to_tsvector(
        lexbuf: *const u8,
        lexlens: *const i32,
        nlex: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_ts_match_vq(
        vimg: *const u8,
        vlen: i32,
        qimg: *const u8,
        qlen: i32,
        res: *mut i32,
    ) -> i32;
    fn pg_diff_ts_match_qv(
        qimg: *const u8,
        qlen: i32,
        vimg: *const u8,
        vlen: i32,
        res: *mut i32,
    ) -> i32;
    fn pg_diff_errcode_get() -> i32;
}

const MAX_TEXT: usize = 4096;
const CBUF: usize = 1 << 20;

/// C-side errcode class constants (csrc/tsvec/postgres.h).
fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_SYNTAX_ERROR {
        1
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        2
    } else if e.sqlstate == ERRCODE_NULL_VALUE_NOT_ALLOWED {
        3
    } else if e.sqlstate == ERRCODE_ZERO_LENGTH_CHARACTER_STRING {
        4
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        5
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        6
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        7
    } else {
        99 // elog / internal class
    }
}

fn pin_utf8() {
    // set-once seam install (panics on double-set; name_diff.rs pattern) —
    // tsvector_send_core goes through the pg_server_to_client seam.
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(mbutils::init_seams);
        // Arm 7 (match) reaches the TS_execute CHECK_FOR_INTERRUPTS calls
        // (tsvector_core::execute); shared no-op install, first-wins.
        crate::install_check_for_interrupts_seam_once();
    });
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 is a valid be-encoding");
}

/// fc-wrapper invocation (uuid_diff/quote_diff pattern).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    f(None, &mut fcinfo)
}

/// Inline varlena image (4B uncompressed header + payload) for fc args.
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 4) as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut img = Vec::with_capacity(payload.len() + 4);
    img.extend_from_slice(&word.to_ne_bytes());
    img.extend_from_slice(payload);
    img
}

fn varlena_datum(img: &[u8]) -> NullableDatum {
    NullableDatum::value(Datum::from_usize(img.as_ptr() as usize))
}

fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

// DIVERGENCE-2 carve RETIRED 2026-07-31: adjudicated pgrust-bug and FIXED in
// parser.rs (C atoi wrap semantics reproduced exactly; ground-truthed on
// postgres:18.3 — 'b:20069458489'::tsvector = 'b':8761). Position digit-runs
// of every magnitude are back on the strict image plane.

/// UTF-8 + NUL-free text gate (server cstring precondition; header comment).
fn take_text(payload: &[u8]) -> Option<(&[u8], CString)> {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return None;
    }
    std::str::from_utf8(payload).ok()?;
    let c = CString::new(payload).unwrap();
    Some((payload, c))
}

struct COut {
    buf: Vec<u8>,
    len: i32,
}

impl COut {
    fn new() -> Self {
        COut { buf: vec![0u8; CBUF], len: 0 }
    }
    fn bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

/// Run tsvectorin on BOTH sides. Returns Ok(rust_payload) when both parsed
/// (image plane already compared), Err(()) when both rejected (error planes
/// already compared).
#[allow(clippy::result_unit_err)]
fn parse_both(m: mcx::Mcx<'_>, text: &[u8], ctext: &CString) -> Result<Vec<u8>, ()> {
    let rres = tsvector_in_core(m, text, None);
    let mut cout = COut::new();
    let crc = unsafe {
        pg_diff_tsvec_in(ctext.as_ptr(), 0, cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
    };
    assert_ne!(crc, -2, "C output buffer overflow: harness bug");
    match (&rres, crc) {
        (Ok(Some(img)), 0) => {
            let rust_payload = img[4..].to_vec();
            assert_eq!(
                rust_payload,
                cout.bytes(),
                "tsvectorin IMAGE divergence on input {:?}",
                String::from_utf8_lossy(text)
            );

            // Soft mode: both sides must succeed identically (no error).
            let mut esc = SoftErrorContext::new(true);
            let rsoft = tsvector_in_core(m, text, Some(&mut esc));
            assert!(
                matches!(rsoft, Ok(Some(_))) && !esc.error_occurred(),
                "Rust soft-mode parse verdict != hard-mode on {:?}",
                String::from_utf8_lossy(text)
            );
            Ok(rust_payload)
        }
        (Err(e), 1) => {
            let cclass = unsafe { pg_diff_errcode_get() };
            assert_eq!(
                err_class(e),
                cclass,
                "tsvectorin errcode class divergence on {:?}: rust {:?} vs C {}",
                String::from_utf8_lossy(text),
                e.sqlstate,
                cclass
            );
            // Soft plane: C soft mode returns 2 (saved); Rust returns
            // Ok(None) with error_occurred.
            let mut esc = SoftErrorContext::new(true);
            let rsoft = tsvector_in_core(m, text, Some(&mut esc));
            let mut cout2 = COut::new();
            let crc2 = unsafe {
                pg_diff_tsvec_in(ctext.as_ptr(), 1, cout2.buf.as_mut_ptr(), CBUF as i32,
                                 &mut cout2.len)
            };
            assert!(
                matches!(rsoft, Ok(None)) && esc.error_occurred(),
                "Rust soft-mode did not capture the error hard mode threw: {:?}",
                String::from_utf8_lossy(text)
            );
            assert_eq!(
                crc2, 2,
                "C soft-mode verdict != hard-mode on {:?}",
                String::from_utf8_lossy(text)
            );
            // fc soft plane: escontext-armed wrapper must return SQL NULL
            // (fc_tsvectorin builtins.rs return_null arm) with the error saved.
            let mut node = ErrorSaveNode::new(true);
            let mut fcinfo = LocalFcinfo::<1>::new(0);
            // SAFETY: the context owning `m` outlives this single call.
            unsafe { fcinfo.set_result_mcx(m) };
            fcinfo.context = node.fm_node_ptr();
            fcinfo.args[0] =
                NullableDatum::value(Datum::from_usize(ctext.as_ptr() as usize));
            let _ = fcb::fc_tsvectorin(None, &mut fcinfo)
                .expect("fc_tsvectorin with escontext must not hard-error");
            assert!(fcinfo.isnull, "fc soft-mode tsvectorin must return NULL");
            assert!(node.ctx.error_occurred(), "fc soft context must save the error");
            Err(())
        }
        (Ok(_), 1) | (Err(_), 0) => {
            panic!(
                "tsvectorin VERDICT divergence on {:?}: rust {:?} vs C rc {}",
                String::from_utf8_lossy(text),
                rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate),
                crc
            );
        }
        (Ok(None), _) => unreachable!("hard-mode parse returned soft None"),
        (_, other) => panic!("unexpected C rc {other}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: tsvectorin / tsvectorout / tsvectorsend (+ fc plane)
// ---------------------------------------------------------------------------

fn arm_in_out_send(payload: &[u8]) {
    let Some((text, ctext)) = take_text(payload) else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    let Ok(payload_img) = parse_both(m, text, &ctext) else { return };

    let v = TsVec { payload: &payload_img };

    // tsvectorout plane.
    let rout = tsvector_out_core(m, v).expect("tsvectorout on valid image");
    let mut cout = COut::new();
    let rc = unsafe {
        pg_diff_tsvec_out(payload_img.as_ptr(), payload_img.len() as i32,
                          cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
    };
    assert_eq!(rc, 0, "C tsvectorout errored (class {}) on a valid image", unsafe {
        pg_diff_errcode_get()
    });
    // Rust out is NUL-terminated cstring bytes; compare without the NUL.
    assert_eq!(&rout[..rout.len() - 1], cout.bytes(), "tsvectorout divergence");

    // tsvectorsend plane.
    let rsend = tsvector_send_core(m, v).expect("tsvectorsend on valid image");
    let mut csend = COut::new();
    let rc = unsafe {
        pg_diff_tsvec_send(payload_img.as_ptr(), payload_img.len() as i32,
                           csend.buf.as_mut_ptr(), CBUF as i32, &mut csend.len)
    };
    assert_eq!(rc, 0, "C tsvectorsend errored on a valid image");
    assert_eq!(rsend.data(), csend.bytes(), "tsvectorsend divergence");

    // fc plane.
    let din = NullableDatum::value(Datum::from_usize(ctext.as_ptr() as usize));
    let d = fc_call::<1>(fcb::fc_tsvectorin, m, [din]).expect("fc_tsvectorin verdict");
    assert_eq!(read_varlena_data(d), &payload_img[..], "fc_tsvectorin != core");

    let img = varlena_image(&payload_img);
    let d = fc_call::<1>(fcb::fc_tsvectorout, m, [varlena_datum(&img)])
        .expect("fc_tsvectorout verdict");
    let cs = unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char) };
    assert_eq!(cs.to_bytes(), cout.bytes(), "fc_tsvectorout != core");

    let d = fc_call::<1>(fcb::fc_tsvectorsend, m, [varlena_datum(&img)])
        .expect("fc_tsvectorsend verdict");
    assert_eq!(read_varlena_data(d), csend.bytes(), "fc_tsvectorsend != core");

    // Short-varlena fc plane: a 1-byte-header stored form must expand to the
    // same output (arg_tsvector's pv.is_short() branch, builtins.rs:27).
    if let Some(simg) = short_varlena_image(&payload_img) {
        let d = fc_call::<1>(fcb::fc_tsvectorout, m, [varlena_datum(&simg)])
            .expect("fc_tsvectorout on short varlena");
        let cs = unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char) };
        assert_eq!(cs.to_bytes(), cout.bytes(), "short-varlena fc_tsvectorout != long");
    }
}

/// 1-byte-header (short) varlena image when the payload fits (total <= 126 B).
fn short_varlena_image(payload: &[u8]) -> Option<Vec<u8>> {
    let total = payload.len() + 1;
    if total > 126 {
        return None;
    }
    let mut img = Vec::with_capacity(total);
    #[cfg(target_endian = "little")]
    img.push(((total as u8) << 1) | 1);
    #[cfg(target_endian = "big")]
    img.push(0x80 | total as u8);
    img.extend_from_slice(payload);
    Some(img)
}

// ---------------------------------------------------------------------------
// Arm 1: tsvectorrecv (+ fc plane)
// ---------------------------------------------------------------------------

/// Decoded-content equality (sorted multiset of (lexeme, positions)) —
/// retained as a triage helper for divergence minimization; the recv plane
/// itself is fully strict since the pg_qsort tie-parity port.
#[allow(dead_code)]
fn tsvec_semantic_eq(a: &[u8], b: &[u8]) -> bool {
    let decode = |p: &[u8]| -> Vec<(Vec<u8>, Vec<u16>)> {
        let v = TsVec { payload: p };
        let mut out: Vec<(Vec<u8>, Vec<u16>)> = (0..v.size())
            .map(|i| {
                let e = v.entry(i);
                (v.lexeme(e).to_vec(), v.positions(e).to_vec())
            })
            .collect();
        out.sort();
        out
    };
    decode(a) == decode(b)
}

fn arm_recv(payload: &[u8]) {
    if payload.len() > MAX_TEXT * 4 {
        return;
    }
    // ALLOCATOR-MODEL CARVE (window): for declared entry counts in
    // (2^20, MaxAllocSize/4] the two sides differ only in WHERE the
    // allocation model errors (C preallocates hdrlen*2 up front and hits the
    // MaxAllocSize palloc guard; Rust sizes its builder differently and
    // fails later on data exhaustion / encoding). No wire message under the
    // harness size cap can make such a count valid, so no correctness
    // signal is lost. Counts <= 2^20, negative counts, and counts above
    // MaxAllocSize/4 (both sides: "invalid size of tsvector") stay in.
    if payload.len() >= 4 {
        let n = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        if n > (1 << 20) && n <= 0x3fff_ffff / 4 {
            return;
        }
    }
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();

    let rres = (|| -> types_error::PgResult<Vec<u8>> {
        let mut vec = mcx::vec_with_capacity_in::<u8>(m, payload.len())?;
        mcx::vec_append_bytes(&mut vec, payload)?;
        let mut si = stringinfo::StringInfo::from_vec(vec)?;
        Ok(tsvector_recv_core(m, &mut si)?[4..].to_vec())
    })();

    let mut cout = COut::new();
    let crc = unsafe {
        pg_diff_tsvec_recv(payload.as_ptr(), payload.len() as i32, cout.buf.as_mut_ptr(),
                           CBUF as i32, &mut cout.len)
    };
    assert_ne!(crc, -2, "C output buffer overflow: harness bug");
    match (&rres, crc) {
        (Ok(rimg), 0) => {
            // KNOWN-DIVERGENCE-1 FIXED (needSort entry-in-place sort) and the
            // duplicate-lexeme tie carve RETIRED 2026-07-31: io.rs now uses
            // the verbatim pg_qsort port, so tie order matches C exactly.
            // FULLY STRICT image plane.
            assert_eq!(
                &rimg[..],
                cout.bytes(),
                "tsvectorrecv IMAGE divergence (wire {:02x?})",
                payload
            );
            // Reconstruction plane (sorted wire only, where storage order ==
            // entry order): rebuild the image with TsVecBuilder::push_raw and
            // require byte identity — an independent check that the builder,
            // the entry accessors, and strdata() agree with the decode.
            if rimg[..] == *cout.bytes() {
                let v = TsVec { payload: rimg };
                let in_entry_order = (0..v.size()).all(|i| {
                    i == 0 || v.entry(i - 1).pos() <= v.entry(i).pos()
                });
                if in_entry_order {
                    let mut b = adt_tsvector_core::layout::TsVecBuilder::with_capacity(
                        m, v.size(), v.strdata().len(),
                    ).expect("builder cap");
                    for i in 0..v.size() {
                        let e = v.entry(i);
                        b.push_raw(v.lexeme(e), v.posblock(e)).expect("push_raw");
                    }
                    assert_eq!(b.nentries(), v.size(), "builder nentries != decoded size");
                    assert!(b.cur_off() <= v.strdata().len(), "builder cur_off past strdata");
                    let rebuilt = b.finish(m).expect("builder finish");
                    assert_eq!(&rebuilt[4..], rimg, "TsVecBuilder reconstruction != recv image");
                }
            }
            // fc plane.
            let mut vec = mcx::vec_with_capacity_in::<u8>(m, payload.len()).unwrap();
            mcx::vec_append_bytes(&mut vec, payload).unwrap();
            let mut si = stringinfo::StringInfo::from_vec(vec).unwrap();
            let dsi = NullableDatum::value(Datum::from_usize(
                &mut si as *mut stringinfo::StringInfo as usize,
            ));
            let d = fc_call::<1>(fcb::fc_tsvectorrecv, m, [dsi])
                .expect("fc_tsvectorrecv verdict");
            assert_eq!(read_varlena_data(d), &rimg[..], "fc_tsvectorrecv != core");
        }
        (Err(e), 1) => {
            let cclass = unsafe { pg_diff_errcode_get() };
            // ALLOCATOR-MODEL CARVE: huge declared entry counts hit the
            // allocation guard on both sides but surface differently —
            // C palloc's "invalid memory alloc request size" elog (class 99)
            // vs the Rust mcx OOM error (53200). Same guard, different
            // allocator plumbing; aliased for this arm only.
            let rclass = if e.sqlstate == types_error::ERRCODE_OUT_OF_MEMORY {
                99
            } else {
                err_class(e)
            };
            assert_eq!(
                rclass,
                cclass,
                "tsvectorrecv errcode class divergence: rust {:?} vs C {}",
                e.sqlstate,
                cclass
            );
        }
        (Ok(_), 1) | (Err(_), 0) => {
            panic!(
                "tsvectorrecv VERDICT divergence: rust {:?} vs C rc {} (wire {:02x?})",
                rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate),
                crc,
                payload
            );
        }
        (_, other) => panic!("unexpected C rc {other}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 2: comparison family (lt/le/eq/ne/ge/gt/cmp)
// ---------------------------------------------------------------------------

fn split_two_texts(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    let (hdr, rest) = payload.split_at_checked(2)?;
    let l1 = u16::from_le_bytes(hdr.try_into().unwrap()) as usize;
    if l1 > rest.len() {
        return None;
    }
    Some(rest.split_at(l1))
}

fn arm_cmp(payload: &[u8]) {
    let Some((t1, t2)) = split_two_texts(payload) else { return };
    let Some((t1, c1)) = take_text(t1) else { return };
    let Some((t2, c2)) = take_text(t2) else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    let Ok(p1) = parse_both(m, t1, &c1) else { return };
    let Ok(p2) = parse_both(m, t2, &c2) else { return };

    let a = TsVec { payload: &p1 };
    let b = TsVec { payload: &p2 };
    let rcmp = adt_tsvector_core::op::silly_cmp_tsvector(a, b);

    let (mut ccmp, mut cbits) = (0i32, 0i32);
    let rc = unsafe {
        pg_diff_tsvec_cmp(p1.as_ptr(), p1.len() as i32, p2.as_ptr(), p2.len() as i32,
                          &mut ccmp, &mut cbits)
    };
    assert_eq!(rc, 0, "C cmp family errored");
    assert_eq!(rcmp.signum(), ccmp.signum(), "tsvector_cmp divergence");
    let rbits = ((rcmp < 0) as i32)
        | (((rcmp <= 0) as i32) << 1)
        | (((rcmp == 0) as i32) << 2)
        | (((rcmp != 0) as i32) << 3)
        | (((rcmp >= 0) as i32) << 4)
        | (((rcmp > 0) as i32) << 5);
    assert_eq!(rbits, cbits, "tsvector bool comparison divergence");

    // fc plane: whole macro family.
    let i1 = varlena_image(&p1);
    let i2 = varlena_image(&p2);
    let d = fc_call::<2>(fcb::fc_tsvector_cmp, m, [varlena_datum(&i1), varlena_datum(&i2)])
        .expect("fc_tsvector_cmp verdict");
    assert_eq!((d.as_usize() as u32 as i32).signum(), ccmp.signum(), "fc_tsvector_cmp != core");
    for (f, bit) in [
        (fcb::fc_tsvector_lt as PGFunction, 0),
        (fcb::fc_tsvector_le, 1),
        (fcb::fc_tsvector_eq, 2),
        (fcb::fc_tsvector_ne, 3),
        (fcb::fc_tsvector_ge, 4),
        (fcb::fc_tsvector_gt, 5),
    ] {
        let d = fc_call::<2>(f, m, [varlena_datum(&i1), varlena_datum(&i2)])
            .expect("fc cmp wrapper verdict");
        assert_eq!(
            d.as_usize() & 1,
            ((cbits >> bit) & 1) as usize,
            "fc cmp wrapper bit {bit} != C"
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 3: unary ops (strip / length / setweight / filter / to_array)
// ---------------------------------------------------------------------------

fn arm_unary(payload: &[u8]) {
    let Some((&sub, rest)) = payload.split_first() else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    match sub % 5 {
        0 => {
            // strip
            let Some((t, c)) = take_text(rest) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let r = adt_tsvector_core::op::tsvector_strip_core(m, TsVec { payload: &p })
                .expect("strip on valid image");
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_strip(p.as_ptr(), p.len() as i32, cout.buf.as_mut_ptr(),
                                    CBUF as i32, &mut cout.len)
            };
            assert_eq!(rc, 0);
            assert_eq!(&r[4..], cout.bytes(), "tsvector_strip divergence");
            let img = varlena_image(&p);
            let d = fc_call::<1>(fcb::fc_tsvector_strip, m, [varlena_datum(&img)])
                .expect("fc_tsvector_strip verdict");
            assert_eq!(read_varlena_data(d), cout.bytes(), "fc strip != core");
        }
        1 => {
            // length
            let Some((t, c)) = take_text(rest) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let rlen = TsVec { payload: &p }.size() as i32;
            let mut clen = 0i32;
            let rc = unsafe { pg_diff_tsvec_length(p.as_ptr(), p.len() as i32, &mut clen) };
            assert_eq!(rc, 0);
            assert_eq!(rlen, clen, "tsvector_length divergence");
            let img = varlena_image(&p);
            let d = fc_call::<1>(fcb::fc_tsvector_length, m, [varlena_datum(&img)])
                .expect("fc_tsvector_length verdict");
            assert_eq!(d.as_usize() as u32 as i32, clen, "fc length != core");
        }
        2 => {
            // setweight: weight char from fuzz (incl invalid -> error arm)
            let Some((&w, rest2)) = rest.split_first() else { return };
            let Some((t, c)) = take_text(rest2) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let img = varlena_image(&p);
            let rres = fc_call::<2>(
                fcb::fc_tsvector_setweight,
                m,
                [varlena_datum(&img), NullableDatum::value(Datum::from_i32(w as i8 as i32))],
            );
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_setweight(p.as_ptr(), p.len() as i32, w as std::ffi::c_char,
                                        cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
            };
            match (&rres, rc) {
                (Ok(d), 0) => assert_eq!(
                    read_varlena_data(*d),
                    cout.bytes(),
                    "tsvector_setweight divergence"
                ),
                (Err(e), 1) => {
                    let cclass = unsafe { pg_diff_errcode_get() };
                    assert_eq!(err_class(e), cclass,
                               "setweight errcode class divergence (w={w:#x})");
                }
                _ => panic!(
                    "setweight VERDICT divergence (w={w:#x}): rust {:?} vs C rc {rc}",
                    rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate)
                ),
            }
        }
        3 => {
            // filter: weight chars (incl invalid + nulls)
            let Some((&nw_raw, rest2)) = rest.split_first() else { return };
            let nw = (nw_raw as usize % 5) + 1;
            if rest2.len() < nw {
                return;
            }
            let (wraw, t) = rest2.split_at(nw);
            let weights: Vec<std::ffi::c_char> = wraw.iter().map(|&b| b as std::ffi::c_char).collect();
            let wnulls: Vec<u8> = wraw.iter().map(|&b| (b == 0xEE) as u8).collect();
            let Some((t, c)) = take_text(t) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };

            let elems: Vec<Datum> =
                weights.iter().map(|&w| Datum::from_i32(w as i32)).collect();
            let nulls: Vec<bool> = wnulls.iter().map(|&n| n != 0).collect();
            let dims = [nw as i32];
            let lbs = [1i32];
            let arr = arrayfuncs::construct_md_array(
                m,
                &elems,
                Some(&nulls),
                1,
                &dims,
                &lbs,
                types_core::catalog::CHAROID,
                1,
                true,
                arrayfuncs::foundation::TYPALIGN_CHAR,
            )
            .expect("char[] image");
            let img = varlena_image(&p);
            let rres = fc_call::<2>(
                fcb::fc_tsvector_filter,
                m,
                [varlena_datum(&img),
                 NullableDatum::value(Datum::from_usize(arr.as_ptr() as usize))],
            );
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_filter(p.as_ptr(), p.len() as i32, weights.as_ptr(),
                                     wnulls.as_ptr(), nw as i32, cout.buf.as_mut_ptr(),
                                     CBUF as i32, &mut cout.len)
            };
            match (&rres, rc) {
                (Ok(d), 0) => {
                    assert_eq!(read_varlena_data(*d), cout.bytes(), "tsvector_filter divergence")
                }
                (Err(e), 1) => {
                    let cclass = unsafe { pg_diff_errcode_get() };
                    assert_eq!(err_class(e), cclass, "filter errcode class divergence");
                }
                _ => panic!(
                    "filter VERDICT divergence: rust {:?} vs C rc {rc}",
                    rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate)
                ),
            }
        }
        _ => {
            // to_array: element-list plane
            let Some((t, c)) = take_text(rest) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let img = varlena_image(&p);
            let d = fc_call::<1>(fcb::fc_tsvector_to_array, m, [varlena_datum(&img)])
                .expect("fc_tsvector_to_array verdict");
            // decode the Rust array image back to an element list
            let full = unsafe {
                let pv = PackedVarlena::from_ptr(d.as_usize() as *const u8);
                std::slice::from_raw_parts(d.as_usize() as *const u8, pv.data().len() + 4)
            };
            let (delems, dnulls) = arrayfuncs::deconstruct_array_builtin(
                m,
                full,
                types_core::catalog::TEXTOID,
                true,
            )
            .expect("deconstruct to_array output");
            assert!(dnulls.iter().all(|n| !n));
            let relems: Vec<&[u8]> = delems.iter().map(|&d| read_varlena_data(d)).collect();

            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_to_array(p.as_ptr(), p.len() as i32, cout.buf.as_mut_ptr(),
                                       CBUF as i32, &mut cout.len)
            };
            assert_eq!(rc, 0);
            let cb = cout.bytes();
            let n = i32::from_ne_bytes(cb[0..4].try_into().unwrap()) as usize;
            assert_eq!(relems.len(), n, "to_array element count divergence");
            let mut off = 4;
            for re in &relems {
                let el = i32::from_ne_bytes(cb[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                assert_eq!(*re, &cb[off..off + el], "to_array element divergence");
                off += el;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 4: concat
// ---------------------------------------------------------------------------

fn arm_concat(payload: &[u8]) {
    let Some((t1, t2)) = split_two_texts(payload) else { return };
    let Some((t1, c1)) = take_text(t1) else { return };
    let Some((t2, c2)) = take_text(t2) else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    let Ok(p1) = parse_both(m, t1, &c1) else { return };
    let Ok(p2) = parse_both(m, t2, &c2) else { return };

    let rres = adt_tsvector_core::op::tsvector_concat_core(
        m,
        TsVec { payload: &p1 },
        TsVec { payload: &p2 },
    );
    let mut cout = COut::new();
    let rc = unsafe {
        pg_diff_tsvec_concat(p1.as_ptr(), p1.len() as i32, p2.as_ptr(), p2.len() as i32,
                             cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
    };
    assert_ne!(rc, -2, "C output buffer overflow: harness bug");
    match (&rres, rc) {
        (Ok(r), 0) => {
            assert_eq!(&r[4..], cout.bytes(), "tsvector_concat divergence");
            let i1 = varlena_image(&p1);
            let i2 = varlena_image(&p2);
            let d = fc_call::<2>(fcb::fc_tsvector_concat, m,
                                 [varlena_datum(&i1), varlena_datum(&i2)])
                .expect("fc_tsvector_concat verdict");
            assert_eq!(read_varlena_data(d), cout.bytes(), "fc concat != core");
        }
        (Err(e), 1) => {
            let cclass = unsafe { pg_diff_errcode_get() };
            assert_eq!(err_class(e), cclass, "concat errcode class divergence");
        }
        _ => panic!(
            "concat VERDICT divergence: rust {:?} vs C rc {rc}",
            rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate)
        ),
    }
}

// ---------------------------------------------------------------------------
// Arms 5/6: lexeme-list ops
// ---------------------------------------------------------------------------

/// Decode a lexeme list: [u8 n], then per element [u8 tag][bytes]; tag 0xFF
/// = SQL NULL, else len = tag % 17. Lexemes must be valid UTF-8, NUL-free.
fn take_lexlist(payload: &[u8]) -> Option<(Vec<Option<&[u8]>>, &[u8])> {
    let (&n_raw, mut rest) = payload.split_first()?;
    let n = n_raw as usize % 9;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let (&tag, r) = rest.split_first()?;
        if tag == 0xFF {
            out.push(None);
            rest = r;
            continue;
        }
        let len = tag as usize % 17;
        let (lex, r) = r.split_at_checked(len)?;
        if lex.contains(&0) || std::str::from_utf8(lex).is_err() {
            return None;
        }
        out.push(Some(lex));
        rest = r;
    }
    Some((out, rest))
}

/// Marshal a lexeme list for the C side: (packed bytes, lens with -1=NULL).
fn c_lexlist(list: &[Option<&[u8]>]) -> (Vec<u8>, Vec<i32>) {
    let mut buf = Vec::new();
    let mut lens = Vec::with_capacity(list.len());
    for e in list {
        match e {
            None => lens.push(-1),
            Some(l) => {
                lens.push(l.len() as i32);
                buf.extend_from_slice(l);
            }
        }
    }
    (buf, lens)
}

/// Rust-side text[] image with nulls (real arrayfuncs on the fc plane).
fn rust_textarr<'m>(
    m: mcx::Mcx<'m>,
    list: &[Option<&[u8]>],
    keep: &mut Vec<Vec<u8>>,
) -> mcx::PgVec<'m, u8> {
    let mut elems = Vec::with_capacity(list.len());
    let mut nulls = Vec::with_capacity(list.len());
    for e in list {
        match e {
            None => {
                elems.push(Datum::null());
                nulls.push(true);
            }
            Some(l) => {
                let img = varlena_image(l);
                elems.push(Datum::from_usize(img.as_ptr() as usize));
                keep.push(img);
                nulls.push(false);
            }
        }
    }
    let dims = [list.len() as i32];
    let lbs = [1i32];
    arrayfuncs::construct_md_array(
        m,
        &elems,
        Some(&nulls),
        1,
        &dims,
        &lbs,
        types_core::catalog::TEXTOID,
        -1,
        false,
        arrayfuncs::foundation::TYPALIGN_INT,
    )
    .expect("text[] image")
}

fn arm_lexops(payload: &[u8]) {
    let Some((&sub, rest)) = payload.split_first() else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    match sub % 3 {
        0 => {
            // delete_str: [u8 lexlen][lex][text]
            let Some((&ll, rest2)) = rest.split_first() else { return };
            let ll = ll as usize % 17;
            let Some((lex, t)) = rest2.split_at_checked(ll) else { return };
            if lex.contains(&0) || std::str::from_utf8(lex).is_err() {
                return;
            }
            let Some((t, c)) = take_text(t) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let img = varlena_image(&p);
            let limg = varlena_image(lex);
            let d = fc_call::<2>(fcb::fc_tsvector_delete_str, m,
                                 [varlena_datum(&img), varlena_datum(&limg)])
                .expect("fc_tsvector_delete_str verdict");
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_delete_str(p.as_ptr(), p.len() as i32, lex.as_ptr(),
                                         lex.len() as i32, cout.buf.as_mut_ptr(),
                                         CBUF as i32, &mut cout.len)
            };
            assert_eq!(rc, 0);
            assert_eq!(read_varlena_data(d), cout.bytes(), "delete_str divergence");
        }
        1 => {
            // delete_arr: [lexlist][text]
            let Some((list, t)) = take_lexlist(rest) else { return };
            let Some((t, c)) = take_text(t) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let mut keep = Vec::new();
            let arr = rust_textarr(m, &list, &mut keep);
            let img = varlena_image(&p);
            let d = fc_call::<2>(
                fcb::fc_tsvector_delete_arr,
                m,
                [varlena_datum(&img),
                 NullableDatum::value(Datum::from_usize(arr.as_ptr() as usize))],
            )
            .expect("fc_tsvector_delete_arr verdict");
            let (buf, lens) = c_lexlist(&list);
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_delete_arr(p.as_ptr(), p.len() as i32, buf.as_ptr(),
                                         lens.as_ptr(), lens.len() as i32,
                                         cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
            };
            assert_eq!(rc, 0);
            assert_eq!(read_varlena_data(d), cout.bytes(), "delete_arr divergence");
        }
        _ => {
            // setweight_by_filter: [u8 w][lexlist][text]
            let Some((&w, rest2)) = rest.split_first() else { return };
            let Some((list, t)) = take_lexlist(rest2) else { return };
            let Some((t, c)) = take_text(t) else { return };
            let Ok(p) = parse_both(m, t, &c) else { return };
            let mut keep = Vec::new();
            let arr = rust_textarr(m, &list, &mut keep);
            let img = varlena_image(&p);
            let rres = fc_call::<3>(
                fcb::fc_tsvector_setweight_by_filter,
                m,
                [
                    varlena_datum(&img),
                    NullableDatum::value(Datum::from_i32(w as i8 as i32)),
                    NullableDatum::value(Datum::from_usize(arr.as_ptr() as usize)),
                ],
            );
            let (buf, lens) = c_lexlist(&list);
            let mut cout = COut::new();
            let rc = unsafe {
                pg_diff_tsvec_setweight_by_filter(p.as_ptr(), p.len() as i32, w as std::ffi::c_char,
                                                  buf.as_ptr(), lens.as_ptr(),
                                                  lens.len() as i32, cout.buf.as_mut_ptr(),
                                                  CBUF as i32, &mut cout.len)
            };
            match (&rres, rc) {
                (Ok(d), 0) => assert_eq!(
                    read_varlena_data(*d),
                    cout.bytes(),
                    "setweight_by_filter divergence"
                ),
                (Err(e), 1) => {
                    let cclass = unsafe { pg_diff_errcode_get() };
                    assert_eq!(err_class(e), cclass,
                               "setweight_by_filter errcode class divergence (w={w:#x})");
                }
                _ => panic!(
                    "setweight_by_filter VERDICT divergence (w={w:#x}): rust {:?} vs C rc {rc}",
                    rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate)
                ),
            }
        }
    }
}

fn arm_array_to_tsvector(payload: &[u8]) {
    let Some((list, _)) = take_lexlist(payload) else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    let mut keep = Vec::new();
    let arr = rust_textarr(m, &list, &mut keep);
    let rres = fc_call::<1>(
        fcb::fc_array_to_tsvector,
        m,
        [NullableDatum::value(Datum::from_usize(arr.as_ptr() as usize))],
    );
    let (buf, lens) = c_lexlist(&list);
    let mut cout = COut::new();
    let rc = unsafe {
        pg_diff_array_to_tsvector(buf.as_ptr(), lens.as_ptr(), lens.len() as i32,
                                  cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
    };
    match (&rres, rc) {
        (Ok(d), 0) => {
            assert_eq!(read_varlena_data(*d), cout.bytes(), "array_to_tsvector divergence")
        }
        (Err(e), 1) => {
            let cclass = unsafe { pg_diff_errcode_get() };
            assert_eq!(err_class(e), cclass, "array_to_tsvector errcode class divergence");
        }
        _ => panic!(
            "array_to_tsvector VERDICT divergence: rust {:?} vs C rc {rc}",
            rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate)
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 7: ts_match_vq / ts_match_qv over generated tsquery images
// ---------------------------------------------------------------------------

fn arm_match(payload: &[u8]) {
    let Some((t, qbytes)) = split_two_texts(payload) else { return };
    let Some((t, c)) = take_text(t) else { return };
    let cx = MemoryContext::new("tsvec_fuzz");
    let m = cx.mcx();
    let Ok(p) = parse_both(m, t, &c) else { return };
    let q = gen_tsquery_payload(qbytes);

    let rres = adt_tsvector_core::op::ts_match_vq_core(
        m,
        TsVec { payload: &p },
        adt_tsvector_core::query::TsQueryRef { payload: &q },
    );
    let mut cres = 0i32;
    let rc = unsafe {
        pg_diff_ts_match_vq(p.as_ptr(), p.len() as i32, q.as_ptr(), q.len() as i32, &mut cres)
    };
    assert_eq!(rc, 0, "C ts_match_vq errored (class {})", unsafe { pg_diff_errcode_get() });
    let r = rres.expect("Rust ts_match_vq errored where C succeeded");
    assert_eq!(r as i32, cres, "ts_match_vq divergence (query {q:02x?})");

    // qv swap plus fc plane in both directions.
    let mut cres_qv = 0i32;
    let rc = unsafe {
        pg_diff_ts_match_qv(q.as_ptr(), q.len() as i32, p.as_ptr(), p.len() as i32, &mut cres_qv)
    };
    assert_eq!(rc, 0);
    assert_eq!(cres, cres_qv, "C ts_match_qv != ts_match_vq");

    let vimg = varlena_image(&p);
    let qimg = varlena_image(&q);
    let d = fc_call::<2>(fcb::fc_ts_match_vq, m, [varlena_datum(&vimg), varlena_datum(&qimg)])
        .expect("fc_ts_match_vq verdict");
    assert_eq!(d.as_usize() & 1, cres as usize, "fc_ts_match_vq != C");
    let d = fc_call::<2>(fcb::fc_ts_match_qv, m, [varlena_datum(&qimg), varlena_datum(&vimg)])
        .expect("fc_ts_match_qv verdict");
    assert_eq!(d.as_usize() & 1, cres as usize, "fc_ts_match_qv != C");

    // Short-varlena fc plane for the tsquery arg (arg_tsquery's is_short()
    // expansion branch, builtins.rs:38).
    if let Some(sq) = short_varlena_image(&q) {
        let d = fc_call::<2>(fcb::fc_ts_match_vq, m, [varlena_datum(&vimg), varlena_datum(&sq)])
            .expect("fc_ts_match_vq short-q verdict");
        assert_eq!(d.as_usize() & 1, cres as usize, "fc_ts_match_vq short-q != long");
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Arm 8 (sel 0xff): oversize inputs — the >MAXSTRPOS limit arms that no
// input under MAX_TEXT can reach. All texts/wires are built DETERMINISTICALLY
// on this side and fed identically to both engines; every mode must ERROR
// with ERRCODE_PROGRAM_LIMIT_EXCEEDED on both sides.
// ---------------------------------------------------------------------------

fn arm_oversize(payload: &[u8]) {
    const MAXSTRPOS: usize = (1 << 20) - 1;
    // COST GATE (re-floor lesson 2026-07-31): an oversize exec parses ~1.2 MiB
    // on four engine sides (~20+ ms); ungated, mutants collapsed the fleet
    // campaign to ~190 exec/s and blew the job deadline. The arm is a
    // deterministic boundary WITNESS, not a search surface — require the
    // 4-byte magic so only the committed seeds (and their prefix-preserving
    // descendants) pay; random selector hits return in nanoseconds.
    if payload.len() < 5 || &payload[0..4] != b"OVSZ" {
        return;
    }
    let mode = payload[4] % 4;
    let jitter = 0usize;
    let cx = MemoryContext::new("tsvec_fuzz_big");
    let m = cx.mcx();

    let expect_limit_err = |rres: &Result<(), (u32, [u8; 5])>, crc: i32, what: &str| {
        // encoded as Err((class_marker, sqlstate)) below; here we only assert shape
        let _ = rres;
        assert_eq!(crc, 1, "{what}: C did not error on an over-limit input");
        let cclass = unsafe { pg_diff_errcode_get() };
        assert_eq!(cclass, 2, "{what}: C errcode class != program-limit");
    };

    match mode {
        0 => {
            // In-parse total-length guard (io.rs strlen_total > MAXSTRPOS).
            // Distinct 8-byte words, total word bytes ~1.15 MiB.
            let n = 150_000 + jitter;
            let mut text = Vec::with_capacity(n * 9);
            for i in 0..n {
                text.extend_from_slice(format!("w{:07}", i).as_bytes());
                text.push(b' ');
            }
            let ctext = CString::new(text.clone()).unwrap();
            let rres = tsvector_in_core(m, &text, None);
            let e = match rres {
                Err(e) => e,
                Ok(_) => panic!("oversize mode 0: Rust accepted a >MAXSTRPOS input"),
            };
            assert_eq!(err_class(&e), 2, "oversize mode 0: Rust class != program-limit");
            let mut cout = COut::new();
            let crc = unsafe {
                pg_diff_tsvec_in(ctext.as_ptr(), 0, cout.buf.as_mut_ptr(), CBUF as i32,
                                 &mut cout.len)
            };
            expect_limit_err(&Ok(()), crc, "oversize mode 0");
            // Soft mode: the ereturn path records and returns None (io.rs:85).
            let mut esc = SoftErrorContext::new(true);
            let rsoft = tsvector_in_core(m, &text, Some(&mut esc));
            assert!(matches!(rsoft, Ok(None)) && esc.error_occurred(),
                    "oversize mode 0: Rust soft verdict != hard");
            let mut cout2 = COut::new();
            let crc2 = unsafe {
                pg_diff_tsvec_in(ctext.as_ptr(), 1, cout2.buf.as_mut_ptr(), CBUF as i32,
                                 &mut cout2.len)
            };
            assert_eq!(crc2, 2, "oversize mode 0: C soft verdict != hard");
        }
        1 => {
            // Post-merge buflen guard (io.rs buflen > MAXSTRPOS): distinct
            // 4-byte words, each with one position, so buflen (word + align +
            // npos + pos = 8 B/word) crosses 1 MiB while strlen_total stays
            // ~528 KiB under the in-parse guard.
            let n = 132_000 + jitter;
            let mut text = Vec::with_capacity(n * 7);
            for i in 0..n {
                text.extend_from_slice(format!("{:04}", i % 10_000).as_bytes());
                text.extend_from_slice(format!("{:03}", i / 10_000).as_bytes());
                text.extend_from_slice(b":1 ");
            }
            let ctext = CString::new(text.clone()).unwrap();
            let rres = tsvector_in_core(m, &text, None);
            let e = match rres {
                Err(e) => e,
                Ok(_) => panic!("oversize mode 1: Rust accepted a >MAXSTRPOS buflen"),
            };
            assert_eq!(err_class(&e), 2, "oversize mode 1: Rust class != program-limit");
            let mut cout = COut::new();
            let crc = unsafe {
                pg_diff_tsvec_in(ctext.as_ptr(), 0, cout.buf.as_mut_ptr(), CBUF as i32,
                                 &mut cout.len)
            };
            expect_limit_err(&Ok(()), crc, "oversize mode 1");
            // Soft mode: the ereturn path records and returns None (io.rs:139).
            let mut esc = SoftErrorContext::new(true);
            let rsoft = tsvector_in_core(m, &text, Some(&mut esc));
            assert!(matches!(rsoft, Ok(None)) && esc.error_occurred(),
                    "oversize mode 1: Rust soft verdict != hard");
            let mut cout2 = COut::new();
            let crc2 = unsafe {
                pg_diff_tsvec_in(ctext.as_ptr(), 1, cout2.buf.as_mut_ptr(), CBUF as i32,
                                 &mut cout2.len)
            };
            assert_eq!(crc2, 2, "oversize mode 1: C soft verdict != hard");
        }
        2 => {
            // Concat over-limit (op.rs b.strlen() > MAXSTRPOS): two halves
            // that each parse fine but exceed the limit joined.
            let mut halves: Vec<Vec<u8>> = Vec::with_capacity(2);
            for half in 0..2u8 {
                let n = 70_000 + jitter;
                let mut text = Vec::with_capacity(n * 8);
                for i in 0..n {
                    text.push(b'a' + half);
                    text.extend_from_slice(format!("{:06}:1 ", i).as_bytes());
                }
                let img = tsvector_in_core(m, &text, None)
                    .expect("oversize half parse")
                    .expect("no soft ctx");
                halves.push(img[4..].to_vec());
            }
            let a = TsVec { payload: &halves[0] };
            let b = TsVec { payload: &halves[1] };
            let rres = adt_tsvector_core::op::tsvector_concat_core(m, a, b);
            let e = match rres {
                Err(e) => e,
                Ok(_) => panic!("oversize mode 2: Rust concat accepted >MAXSTRPOS"),
            };
            assert_eq!(err_class(&e), 2, "oversize mode 2: Rust class != program-limit");
            let mut cout = COut::new();
            let crc = unsafe {
                pg_diff_tsvec_concat(halves[0].as_ptr(), halves[0].len() as i32,
                                     halves[1].as_ptr(), halves[1].len() as i32,
                                     cout.buf.as_mut_ptr(), CBUF as i32, &mut cout.len)
            };
            expect_limit_err(&Ok(()), crc, "oversize mode 2");
        }
        _ => {
            // recv total-lexeme-length guard (io.rs b.strlen() > MAXSTRPOS):
            // ascending 207-byte lexemes; the guard fires only BEFORE an
            // entry push, so the limit must be crossed before the last one:
            // (n-1)*207 > MAXSTRPOS needs n >= 5069.
            let n = 5_200 + jitter;
            let mut wire = Vec::with_capacity(n * 220 + 4);
            wire.extend_from_slice(&(n as i32).to_be_bytes());
            for i in 0..n {
                wire.extend_from_slice(format!("{:07}", i).as_bytes());
                wire.extend_from_slice(&[b'a'; 200]);
                wire.push(0);
                wire.extend_from_slice(&0i16.to_be_bytes());
            }
            let rres = (|| -> types_error::PgResult<Vec<u8>> {
                let mut vec = mcx::vec_with_capacity_in::<u8>(m, wire.len())?;
                mcx::vec_append_bytes(&mut vec, &wire)?;
                let mut si = stringinfo::StringInfo::from_vec(vec)?;
                Ok(tsvector_recv_core(m, &mut si)?[4..].to_vec())
            })();
            let e = match rres {
                Err(e) => e,
                Ok(_) => panic!("oversize mode 3: Rust recv accepted >MAXSTRPOS"),
            };
            // recv reports the limit as a generic protocol error in both
            // engines ("invalid tsvector: maximum total lexeme length
            // exceeded" — errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
            // on the C side); require the SAME class both sides.
            let rclass = err_class(&e);
            let mut cout = COut::new();
            let crc = unsafe {
                pg_diff_tsvec_recv(wire.as_ptr(), wire.len() as i32, cout.buf.as_mut_ptr(),
                                   CBUF as i32, &mut cout.len)
            };
            assert_eq!(crc, 1, "oversize mode 3: C recv did not error");
            let cclass = unsafe { pg_diff_errcode_get() };
            assert_eq!(rclass, cclass, "oversize mode 3: errcode class divergence");
        }
    }
}

pub fn tsvector_core_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    pin_utf8();
    if sel == 0xff {
        // Arm 8 (oversize): sel byte 0xff reserved AFTER the fleet floor run
        // to witness the >MAXSTRPOS limit arms unreachable under MAX_TEXT.
        arm_oversize(payload);
        return;
    }
    match sel % 8 {
        0 => arm_in_out_send(payload),
        1 => arm_recv(payload),
        2 => arm_cmp(payload),
        3 => arm_unary(payload),
        4 => arm_concat(payload),
        5 => arm_lexops(payload),
        6 => arm_array_to_tsvector(payload),
        _ => arm_match(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sel: u8, payload: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        tsvector_core_diff(&v);
    }

    #[test]
    fn smoke_in_out_send() {
        run(0, b"'cat':3A 'dog':1,2B fish");
        run(0, b"a:1 b:2 a:3");
        run(0, b"'quoted lexeme' plain\\ escaped");
        run(0, b"w:16383 w:16384 w:99999"); // position clamp
        run(0, b"bad:"); // syntax error, both planes
        run(0, b"'unterminated");
        run(0, "\u{e9}:1 \u{65e5}\u{672c}:2".as_bytes());
        run(0, b"x:0"); // wrong position info
        run(0, b"a:1a1"); // weight then digit -> syntax error
    }

    #[test]
    fn smoke_recv() {
        // 1 entry, "ab", 1 position
        let mut wire = Vec::new();
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(b"ab\0");
        wire.extend_from_slice(&1u16.to_be_bytes());
        wire.extend_from_slice(&5u16.to_be_bytes());
        run(1, &wire);
        // misordered positions
        let mut wire = Vec::new();
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(b"ab\0");
        wire.extend_from_slice(&2u16.to_be_bytes());
        wire.extend_from_slice(&5u16.to_be_bytes());
        wire.extend_from_slice(&5u16.to_be_bytes());
        run(1, &wire);
        // unsorted lexemes (needSort path)
        let mut wire = Vec::new();
        wire.extend_from_slice(&2u32.to_be_bytes());
        wire.extend_from_slice(b"bb\0");
        wire.extend_from_slice(&0u16.to_be_bytes());
        wire.extend_from_slice(b"aa\0");
        wire.extend_from_slice(&0u16.to_be_bytes());
        run(1, &wire);
        // truncated
        run(1, &[0, 0, 0, 5, b'a']);
    }

    #[test]
    fn smoke_ops() {
        let two = |a: &[u8], b: &[u8]| {
            let mut v = (a.len() as u16).to_le_bytes().to_vec();
            v.extend_from_slice(a);
            v.extend_from_slice(b);
            v
        };
        run(2, &two(b"a b c", b"a b c"));
        run(2, &two(b"a:1", b"a:2"));
        run(4, &two(b"a:1 b:2", b"b:3 c:1"));
        run(4, &two(b"a:16383", b"b:1"));
        run(3, &[0, b'a', b':', b'1']); // strip
        run(3, &[1, b'a']); // length
        run(3, &[2, b'A', b'a', b':', b'1']); // setweight A
        run(3, &[2, b'!', b'a', b':', b'1']); // setweight invalid
        run(3, &[3, 2, b'A', b'b', b'a', b':', b'1']); // filter
        run(3, &[4, b'a', b' ', b'b']); // to_array
        run(6, &[2, 1, b'a', 2, b'b', b'b']); // array_to_tsvector
        run(6, &[1, 0xFF]); // null element -> error
        run(6, &[1, 0]); // empty string -> error
        run(5, &[0, 1, b'a', b'a', b' ', b'b']); // delete_str
        run(5, &[1, 1, 1, b'a', b'a', b':', b'1']); // delete_arr
        run(5, &[2, b'B', 1, 1, b'a', b'a', b':', b'1']); // setweight_by_filter
    }

    #[test]
    fn smoke_match() {
        let mk = |t: &[u8], q: &[u8]| {
            let mut v = (t.len() as u16).to_le_bytes().to_vec();
            v.extend_from_slice(t);
            v.extend_from_slice(q);
            v
        };
        for qseed in 0..48u8 {
            run(7, &mk(b"cat:1A dog:2 abc:3,4B", &[qseed, qseed ^ 0x5a, 3, 7, qseed]));
        }
        run(7, &mk(b"a:1 b:2 c:3", &[31])); // empty tsquery -> false
    }

    /// Filtered-run regression (2026-08-01): 229915b8d7 restored the
    /// TS_execute CHECK_FOR_INTERRUPTS calls and this target relied on
    /// OTHER modules in the shared test binary installing the seam first —
    /// a filtered run (`--exact smoke_match`, the fleet fuzz-binary
    /// posture) panicked "seam not installed" at exec of arm 7. Re-exec
    /// the test binary with ONLY smoke_match selected so no benefactor
    /// module can mask a dropped install (stack_depth.rs precedent).
    #[test]
    fn smoke_match_survives_filtered_run() {
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(&exe)
            .args(["--exact", "tsvector_core_diff::tests::smoke_match"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "filtered smoke_match failed (check_for_interrupts seam install dropped?):\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
