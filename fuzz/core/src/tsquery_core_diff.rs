//! tsquery_core_diff: differential fuzz driver — shipped Rust `adt_tsquery_core` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tsquery_core_io.c + csrc/tsq/*). Crate under test:
//! crates/backend/utils/adt/tsquery_core.
//!
//! Comparison planes: value bytes (tsquery IMAGES in the zero-header
//! convention, infix TEXT, wire BYTES, cmp ints, bools), error-verdict,
//! errcode/sqlstate class (map below), soft-error verdict (real
//! ErrorSaveContext on both sides), and the NOTICE plane as a boolean
//! (C notice count > 0 <=> the parse produced the empty tsquery — the only
//! two upstream notice sites are the empty-query and all-stopwords arms,
//! both of which return the empty image; the Rust side has no notice
//! counter, so the empty image IS the observation). Message text out of
//! scope.
//!
//! Input layout: [selector][payload]; selector % 8 picks the arm family
//! (each family drives every function whose lines it owns, so all 20
//! catalog rows are on a plane):
//!   0 tsqueryin standard (oid 3612) noisy + soft, then tsqueryout
//!     (oid 3613) on the shared parse result. fc plane: fc_tsqueryin
//!     (noisy + ErrorSaveNode soft), fc_tsqueryout.
//!   1 parse_tsquery P_TSQ_WEB (websearch tokenizer + push_stop +
//!     stopword-cleanup machinery; upstream reaches it via
//!     websearch_to_tsquery, whose pushval_morph/config plumbing is the
//!     to_tsany crate's — HERE the tokenizer runs with pushval_asis on
//!     both sides). payload byte 0 bit 0 additionally drives the
//!     database_ctype_is_c knob on BOTH sides (environment pinned per
//!     exec, an input plane not a carve).
//!   2 parse_tsquery P_TSQ_PLAIN (plainto_tsquery tokenizer), same
//!     pushval_asis pairing.
//!   3 tsqueryrecv (oid 3641) over raw wire bytes, + pq_getmsgend
//!     consumed-cursor plane, then tsquerysend (oid 3640) on the shared
//!     recv result. fc plane: fc_tsqueryrecv, fc_tsquerysend. (recv(send(
//!     parse(x))) is NOT parse(x) — upstream rebuilds the operand pool in
//!     item order — so send/recv are only ever compared same-side.)
//!   4 image operators over two standard-parsed sub-inputs:
//!     tsquery_and/or/phrase/phrase_distance/not (oids 3669/3670/5003/
//!     5004/3671; distance = i16 field, in-domain outside 0..=MAXENTRYPOS
//!     to hit the 22023 arm), tsquery_cmp + the six sign tests
//!     (3662..3668), tsq_mcontains/tsq_mcontained (3691/3692),
//!     tsquery_numnode (3672). fc plane for every one of them.
//!   5 tsquerytree (oid 3673; clean_NOT + infix). fc plane:
//!     fc_tsquerytree.
//!   6 cleanup_tsquery_stopwords over GENERATED parse-internal images
//!     (QI_VALSTOP under every operator incl. OP_PHRASE — the band the
//!     in-scope parsers cannot mint; see the arm-6 section comment).
//!   7 BULK (input cap ~1.1MiB, recursion bounded by pre-scans, not
//!     bytes): standard parse + recv, reaching the MAXSTRLEN/MAXSTRPOS
//!     program-limit guards the 2KiB cap fences (arm-7 section comment).
//!
//! Errcode classes (must mirror csrc/tsq/shim/postgres.h): 1=42601
//! syntax_error, 2=22023 invalid_parameter_value, 3=54000
//! program_limit_exceeded, 4=22021 character_not_in_repertoire, 5=08P01
//! protocol_violation, 100=XX000 (elog/internal — upstream tsqueryrecv
//! validation is elog, and the Rust port's un-sqlstated PgError::error
//! defaults to XX000 the same way).
//!
//! Domain caps (documented seams, not carves of Rust lines):
//!   - total fuzz input <= 2048 bytes (arms 0-6; arm 7 <= BULK_CAP with
//!     its own recursion pre-scans). This bounds parser/QTN recursion on
//!     both sides: the C oracle's check_stack_depth is a no-op shim
//!     (upstream call sites: tsquery.c makepol/findoprnd, tsquery_util.c
//!     QT2QTN — the stack-depth ereport is session-configured state,
//!     54001 never enters the shared domain), and the Rust side's
//!     guards (proofs/boundary-audit merge; release-effective PgResult
//!     returns, armed by set_stack_base at backend launch) stay INERT
//!     here because this process never calls set_stack_base
//!     (STACK_BASE_PTR=0 short-circuits stack_is_too_deep) — so the
//!     no-op is symmetric and ASan fake-stack addresses cannot
//!     false-fire it. Platform/stack-depth seam, same class as prior
//!     recursive-parser lanes; the Rust guard error paths carry
//!     excluded-state exception rows.
//!   - text-arm inputs are NUL-free (the C oracle boundary is a cstring;
//!     PG text can't carry NUL either, so NUL-bearing inputs are outside
//!     the SQL-reachable domain, not a lost plane).
//!   - text-arm inputs are VALID UTF-8: upstream, every text reaching
//!     tsqueryin has passed pg_verify_mbstr at the protocol/input layer
//!     (invalid sequences die with 22021 before the parser runs), and the
//!     vendored parser's pg_mblen walk over-reads on invalid lead bytes
//!     exactly as upstream would if handed such bytes (found by this
//!     target: infix over a 0xF0-lead operand). The recv arm (3) keeps
//!     RAW bytes — its wire path validates in-function via
//!     pq_getmsgstring on both sides.
//!
//! SKIPPED rows: none — all 20 catalog functions of the crate are driven.

use core::ffi::c_char;
use std::ffi::CString;

use adt_tsquery_core::io::{
    compare_tsq, tsq_mcontains_core, tsquery_in_core, tsquery_out_core, tsquery_recv_core,
    tsquery_send_core, tsquerytree_core,
};
use adt_tsquery_core::parse::{parse_tsquery, pushval_asis, P_TSQ_PLAIN, P_TSQ_WEB};
use adt_tsvector_core::query::TsQueryRef;
use datum::{Datum, NullableDatum};
use stringinfo::StringInfo;
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_SYNTAX_ERROR,
};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_tsquery_in(
        input: *const c_char,
        flags: i32,
        soft_mode: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
        out_notices: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_out(
        img: *const u8,
        len: i32,
        out: *mut c_char,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_send(
        img: *const u8,
        len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_recv(
        wire: *const u8,
        wire_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
        out_consumed: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_binop(
        op: i32,
        img_a: *const u8,
        len_a: i32,
        img_b: *const u8,
        len_b: i32,
        distance: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_cmp(
        img_a: *const u8,
        len_a: i32,
        img_b: *const u8,
        len_b: i32,
        out_cmp: *mut i32,
    ) -> i32;
    fn pg_diff_tsq_mcontains(
        img_q: *const u8,
        len_q: i32,
        img_ex: *const u8,
        len_ex: i32,
        out_bool: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_numnode(img: *const u8, len: i32, out_n: *mut i32) -> i32;
    fn pg_diff_tsquerytree(
        img: *const u8,
        len: i32,
        out: *mut c_char,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_tsquery_cleanup(
        img: *const u8,
        len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_tsq_set_ctype_is_c(v: i32) -> i32;
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
}

/// Total-input cap; see the module header (stack-depth seam bound).
const MAX_INPUT: usize = 2048;
/// Bulk-arm input cap: large enough that the op-pool program-limit guards
/// (MAXSTRPOS = 2^20-1 total operand bytes) are REACHABLE — ~525 operands of
/// ~2000 bytes overflow the pool — while the arm's own recursion guards
/// (paren-count / item-count pre-scans) keep the stack-depth seam sound.
const BULK_CAP: usize = 1_150_000;
/// Out-buffer size: parse of <=2KiB text yields <=~16KiB images; infix
/// output multiplies by <~8 (quotes + weight suffixes + ` <16384> `). A
/// 1MiB buffer keeps the C-side abort-on-overflow guard unreachable.
const OUT_CAP: usize = 1 << 20;
/// Bulk-arm out cap: a just-under-MAXSTRPOS parse yields a ~1.07MiB image
/// and larger infix text; 8MiB keeps the abort guard unreachable there too.
const BULK_OUT_CAP: usize = 8 << 20;

fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_SYNTAX_ERROR {
        1
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        2
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        3
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        4
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        5
    } else if e.sqlstate == ERRCODE_INTERNAL_ERROR {
        100
    } else {
        -1
    }
}

/// Copy a zero-header image and stamp the 4B uncompressed varlena header
/// (set_varsize_4b encoding) so the fc arg layer can detoast it.
fn stamp_header(img: &[u8]) -> Vec<u8> {
    let len = img.len() as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut v = img.to_vec();
    v[..4].copy_from_slice(&word.to_ne_bytes());
    v
}

fn tsq_ref(img: &[u8]) -> TsQueryRef<'_> {
    TsQueryRef { payload: &img[4..] }
}

/// The empty tsquery image is exactly HDRSIZETQ = 8 bytes (zeroed vl_len_ +
/// size 0): the NOTICE observation (module header).
fn is_empty_img(img: &[u8]) -> bool {
    img.len() == 8
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; verbatim from uuid_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// fc_call with an armed ErrorSaveNode (the input_function_call_safe shape).
fn fc_call_soft<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
    esc: &mut ErrorSaveNode,
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.context = esc.fm_node_ptr();
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Payload bytes of a varlena result Datum (image bytes AFTER the header).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// A cstring result Datum's bytes (fc_tsqueryout).
fn read_cstring_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: cstring_result datums are live NUL-terminated allocations in
    // the armed arena.
    unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const c_char) }.to_bytes()
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}

// ---------------------------------------------------------------------------
// Shared parse comparator (arms 0/1/2)
// ---------------------------------------------------------------------------

/// C oracle status for one (flags, soft) mode.
struct CParse {
    st: i32,
    err: i32,
    notices: i32,
    img: Vec<u8>,
}

fn c_parse(text: &CString, flags: i32, soft: bool, cap: usize) -> CParse {
    let mut out = vec![0u8; cap];
    let mut out_len = 0i32;
    let mut notices = 0i32;
    // SAFETY: NUL-terminated input; out is OUT_CAP bytes.
    let st = unsafe {
        pg_diff_tsquery_in(
            text.as_ptr(),
            flags,
            soft as i32,
            out.as_mut_ptr(),
            cap as i32,
            &mut out_len,
            &mut notices,
        )
    };
    let err = unsafe { pg_diff_errcode_get() };
    out.truncate(out_len.max(0) as usize);
    CParse { st, err, notices, img: out }
}

/// Both-mode parse differential for one tokenizer; returns the shared
/// success image (zero-header convention) when both sides parsed clean.
fn parse_diff(m: mcx::Mcx<'_>, text: &[u8], flags: i32) -> Option<Vec<u8>> {
    parse_diff_cap(m, text, flags, OUT_CAP)
}

fn parse_diff_cap(m: mcx::Mcx<'_>, text: &[u8], flags: i32, cap: usize) -> Option<Vec<u8>> {
    let cs = CString::new(text).expect("caller rejects NUL");

    // --- noisy mode ---
    let c = c_parse(&cs, flags, false, cap);
    let r = parse_tsquery(m, text, flags, None, &mut pushval_asis);
    let img = match r {
        Ok(Some(p)) => {
            assert!(
                c.st == 0 && c.img == p.img.as_slice(),
                "parse({flags}) noisy DIVERGENCE input={:?}: C=(st {} err {} img {:?}) Rust=Ok({:?})",
                String::from_utf8_lossy(text),
                c.st,
                c.err,
                c.img,
                p.img.as_slice(),
            );
            // NOTICE plane (boolean; module header).
            assert!(
                (c.notices > 0) == is_empty_img(&c.img),
                "parse({flags}) NOTICE-plane DIVERGENCE input={:?}: notices={} img_len={}",
                String::from_utf8_lossy(text),
                c.notices,
                c.img.len(),
            );
            Some(p.img)
        }
        Ok(None) => unreachable!("esc=None cannot yield a soft error"),
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                c.st == 1 && c.err == rc,
                "parse({flags}) noisy DIVERGENCE input={:?}: C=(st {} err {}) Rust=Err(class {rc} sqlstate {:?} {})",
                String::from_utf8_lossy(text),
                c.st,
                c.err,
                e.sqlstate,
                e.message,
            );
            None
        }
    };

    // --- soft mode ---
    let c2 = c_parse(&cs, flags, true, cap);
    let mut esc = SoftErrorContext::new(true);
    let r2 = parse_tsquery(m, text, flags, Some(&mut esc), &mut pushval_asis);
    match r2 {
        Ok(Some(p2)) => {
            assert!(
                c2.st == 0 && c2.img == p2.img.as_slice(),
                "parse({flags}) soft DIVERGENCE input={:?}: C=(st {} err {}) Rust=Ok",
                String::from_utf8_lossy(text),
                c2.st,
                c2.err,
            );
        }
        Ok(None) => {
            let rc = esc.error().map(err_class).unwrap_or(-1);
            assert!(
                c2.st == 2 && c2.err == rc,
                "parse({flags}) soft DIVERGENCE input={:?}: C=(st {} err {}) Rust=soft(class {rc})",
                String::from_utf8_lossy(text),
                c2.st,
                c2.err,
            );
        }
        Err(e) => {
            // Errors that refuse soft-capture (hard even with escontext).
            let rc = err_class(&e);
            assert!(
                c2.st == 1 && c2.err == rc,
                "parse({flags}) soft-hard DIVERGENCE input={:?}: C=(st {} err {}) Rust=Err(class {rc} {})",
                String::from_utf8_lossy(text),
                c2.st,
                c2.err,
                e.message,
            );
        }
    }

    img.map(|v| v.as_slice().to_vec())
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// The noisy parse arm's empty-query NOTICE walks the elog emit path, which
/// crosses the mbutils pg_server_to_client seam: install the REAL mbutils
/// seams once per process and pin the thread's database encoding to UTF8
/// (identical to name_diff.rs's setup; catch_unwind because init_seams is
/// set-once and another module may have installed first).
fn setup() {
    use std::sync::Once;
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(mbutils::init_seams);
    });
    std::thread_local! {
        static ENC_PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ENC_PINNED.with(|c| {
        if !c.get() {
            mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 is a valid be-encoding");
            c.set(true);
        }
    });
}

pub fn tsquery_core_diff(data: &[u8]) {
    setup();
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let arm = sel % 8;
    // Per-arm input caps: the BULK arm exists precisely to reach the
    // program-limit guards the 2 KiB cap fences; its own recursion guards
    // live inside bulk_arm.
    if payload.len() > if arm == 7 { BULK_CAP } else { MAX_INPUT - 1 } {
        return;
    }
    match arm {
        0 => in_out_arm(payload),
        1 => web_arm(payload),
        2 => plain_arm(payload),
        3 => recv_send_arm(payload),
        4 => ops_arm(payload),
        5 => tree_arm(payload),
        6 => cleanup_arm(payload),
        _ => bulk_arm(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: tsqueryin (3612) noisy+soft, tsqueryout (3613); fc planes.
// ---------------------------------------------------------------------------

fn in_out_arm(payload: &[u8]) {
    if payload.contains(&0) || core::str::from_utf8(payload).is_err() {
        return; // cstring + pg_verify_mbstr boundary (module header)
    }
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let m = cx.mcx();
    let Some(img) = parse_diff(m, payload, 0) else {
        // fc plane still owes the error shape: noisy fc must Err with the
        // same sqlstate class, soft fc must return SQL NULL.
        fc_parse_error_plane(m, payload);
        return;
    };

    // tsqueryout over the shared parse result.
    let mut cbuf = vec![0u8; OUT_CAP];
    let mut clen = 0i32;
    // SAFETY: img/cbuf live; caps passed.
    let cst = unsafe {
        pg_diff_tsquery_out(
            img.as_ptr(),
            img.len() as i32,
            cbuf.as_mut_ptr() as *mut c_char,
            OUT_CAP as i32,
            &mut clen,
        )
    };
    let rt = tsquery_out_core(m, tsq_ref(&img)).expect("tsquery_out alloc at fuzz sizes");
    // Rust out is NUL-terminated; C reports strlen.
    let rt_text = &rt.as_slice()[..rt.len() - 1];
    assert!(
        cst == 0 && &cbuf[..clen as usize] == rt_text,
        "tsqueryout DIVERGENCE input={:?}: C={:?} Rust={:?}",
        String::from_utf8_lossy(payload),
        String::from_utf8_lossy(&cbuf[..clen.max(0) as usize]),
        String::from_utf8_lossy(rt_text),
    );

    // fc planes: fc_tsqueryin (noisy) == core image; fc_tsqueryout == core.
    let cs = CString::new(payload).expect("checked");
    let (r, isnull) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsqueryin,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
    );
    let d = r.expect("fc_tsqueryin: core parsed clean");
    assert!(!isnull, "fc_tsqueryin returned NULL where core parsed clean");
    assert!(
        read_varlena_data(d) == &img[4..],
        "fc_tsqueryin vs core DIVERGENCE input={:?}",
        String::from_utf8_lossy(payload),
    );

    let arg = stamp_header(&img);
    let (r, _) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsqueryout,
        m,
        [Datum::from_usize(arg.as_ptr() as usize)],
    );
    let d = r.expect("fc_tsqueryout infallible on valid images at fuzz sizes");
    assert!(
        read_cstring_data(d) == rt_text,
        "fc_tsqueryout vs core DIVERGENCE input={:?}",
        String::from_utf8_lossy(payload),
    );
}

/// fc error-shape plane for arm 0 inputs whose core parse failed.
fn fc_parse_error_plane(m: mcx::Mcx<'_>, payload: &[u8]) {
    let cs = CString::new(payload).expect("checked");
    let (r, _) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsqueryin,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
    );
    let e = r.expect_err("fc_tsqueryin: core errored, wrapper must too");
    let noisy = tsquery_in_core(m, payload, None).expect_err("core reproduces the error");
    assert!(
        e.sqlstate == noisy.sqlstate,
        "fc_tsqueryin error-shape DIVERGENCE input={:?}: fc {:?} core {:?}",
        String::from_utf8_lossy(payload),
        e.sqlstate,
        noisy.sqlstate,
    );

    let mut node = ErrorSaveNode::new(true);
    let (r, isnull) = fc_call_soft::<1>(
        adt_tsquery_core::builtins::fc_tsqueryin,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
        &mut node,
    );
    match r {
        Ok(_) => assert!(
            isnull && node.ctx.error_occurred(),
            "fc_tsqueryin soft plane DIVERGENCE input={:?}: expected NULL+soft, got isnull={isnull}",
            String::from_utf8_lossy(payload),
        ),
        Err(e) => {
            // Hard-even-when-soft errors (e.g. stack/alloc) must match core.
            assert!(
                err_class(&e) == err_class(&noisy),
                "fc_tsqueryin soft-hard DIVERGENCE input={:?}",
                String::from_utf8_lossy(payload),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Arms 1/2: websearch / plain tokenizers (image + verdict planes; the
// SQL-callable wrappers over these flags live in the to_tsany crate).
// ---------------------------------------------------------------------------

fn web_arm(payload: &[u8]) {
    let Some((&knobs, text)) = payload.split_first() else {
        return;
    };
    if text.contains(&0) || core::str::from_utf8(text).is_err() {
        return; // cstring + pg_verify_mbstr boundary (module header)
    }
    let ctype_is_c = knobs & 1 != 0;
    // Environment knob pinned identically on both sides, per exec.
    pg_locale::set_database_ctype_is_c(ctype_is_c);
    // SAFETY: plain int setter.
    unsafe { pg_diff_tsq_set_ctype_is_c(ctype_is_c as i32) };
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let _ = parse_diff(cx.mcx(), text, P_TSQ_WEB);
    pg_locale::set_database_ctype_is_c(false);
    // SAFETY: plain int setter.
    unsafe { pg_diff_tsq_set_ctype_is_c(0) };
}

fn plain_arm(payload: &[u8]) {
    if payload.contains(&0) || core::str::from_utf8(payload).is_err() {
        return; // cstring + pg_verify_mbstr boundary (module header)
    }
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let _ = parse_diff(cx.mcx(), payload, P_TSQ_PLAIN);
}

// ---------------------------------------------------------------------------
// Arm 3: tsqueryrecv (3641) + tsquerysend (3640); fc planes.
// ---------------------------------------------------------------------------

fn recv_send_arm(payload: &[u8]) {
    recv_send_core_arm(payload, OUT_CAP)
}

fn recv_send_core_arm(payload: &[u8], cap: usize) {
    // Oracle-plumbing bound (NOT a domain carve): upstream tsqueryrecv
    // pallocs COMPUTESIZE(claimed_size) BEFORE reading items, so a frame
    // claiming up to MaxAllocSize/16 items demands up to ~1GiB — real PG
    // serves it and then fails on "insufficient data" (the wire is <=2KiB),
    // the Rust side identically; only the shim's 8MiB arena cannot. Skip
    // exactly the allocate-then-starve band. Claims ABOVE MaxAllocSize/16
    // stay in-domain (the "invalid size" check fires before the palloc),
    // as do all frames small enough to allocate (<= 2^16 items = 1MiB).
    {
        use adt_tsvector_core::query::{MAX_ALLOC_SIZE, QUERYITEM_SIZE};
        let claimed = payload
            .first_chunk::<4>()
            .map(|b| u32::from_be_bytes(*b) as usize)
            .unwrap_or(0);
        if claimed > (1 << 16) && claimed <= MAX_ALLOC_SIZE / QUERYITEM_SIZE {
            return;
        }
    }
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let m = cx.mcx();

    let mut cbuf = vec![0u8; cap];
    let (mut clen, mut consumed) = (0i32, 0i32);
    // SAFETY: payload/cbuf live; caps passed.
    let cst = unsafe {
        pg_diff_tsquery_recv(
            payload.as_ptr(),
            payload.len() as i32,
            cbuf.as_mut_ptr(),
            cap as i32,
            &mut clen,
            &mut consumed,
        )
    };
    let cerr = unsafe { pg_diff_errcode_get() };

    let Some(mut si) = make_si(m, payload) else {
        return;
    };
    let img = match tsquery_recv_core(m, &mut si) {
        Ok(img) => {
            assert!(
                cst == 0 && &cbuf[..clen as usize] == img.as_slice(),
                "tsqueryrecv DIVERGENCE wire={payload:?}: C=(st {cst} err {cerr} len {clen}) Rust=Ok(len {})",
                img.len(),
            );
            // Consumed-cursor plane (the caller-side pq_getmsgend check).
            assert!(
                consumed as usize == si.cursor,
                "tsqueryrecv consumed DIVERGENCE wire={payload:?}: C={consumed} Rust={}",
                si.cursor,
            );
            img
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                cst == 1 && cerr == rc,
                "tsqueryrecv DIVERGENCE wire={payload:?}: C=(st {cst} err {cerr}) Rust=Err(class {rc} sqlstate {:?} {})",
                e.sqlstate,
                e.message,
            );
            return;
        }
    };

    // tsquerysend over the shared recv result.
    let mut wbuf = vec![0u8; cap];
    let mut wlen = 0i32;
    // SAFETY: img/wbuf live; caps passed.
    let wst = unsafe {
        pg_diff_tsquery_send(
            img.as_ptr(),
            img.len() as i32,
            wbuf.as_mut_ptr(),
            cap as i32,
            &mut wlen,
        )
    };
    let rs = tsquery_send_core(m, tsq_ref(&img)).expect("send alloc at fuzz sizes");
    assert!(
        wst == 0 && &wbuf[..wlen as usize] == rs.data(),
        "tsquerysend DIVERGENCE wire={payload:?}: C len {wlen} Rust len {}",
        rs.data().len(),
    );

    // tsqueryout over the shared recv result: recv admits images the text
    // parser can never mint (weight bitmaps on any operand, phrase
    // distances read as raw u16 -> negative i16), so this is the only arm
    // that exercises infix over that band (push_i32_dec's negative arm).
    {
        let mut obuf = vec![0u8; cap];
        let mut olen = 0i32;
        // SAFETY: img/obuf live; caps passed.
        let ost = unsafe {
            pg_diff_tsquery_out(
                img.as_ptr(),
                img.len() as i32,
                obuf.as_mut_ptr() as *mut c_char,
                cap as i32,
                &mut olen,
            )
        };
        let rt = tsquery_out_core(m, tsq_ref(&img)).expect("tsquery_out alloc at fuzz sizes");
        let rt_text = &rt.as_slice()[..rt.len() - 1];
        assert!(
            ost == 0 && &obuf[..olen as usize] == rt_text,
            "tsqueryout(recv) DIVERGENCE wire={payload:?}: C={:?} Rust={:?}",
            String::from_utf8_lossy(&obuf[..olen.max(0) as usize]),
            String::from_utf8_lossy(rt_text),
        );
    }

    // fc planes.
    let Some(mut si2) = make_si(m, payload) else {
        return;
    };
    let (r, _) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsqueryrecv,
        m,
        [Datum::from_usize(core::ptr::from_mut(&mut si2) as usize)],
    );
    let d = r.expect("fc_tsqueryrecv: core recv succeeded");
    assert!(
        read_varlena_data(d) == &img[4..],
        "fc_tsqueryrecv vs core DIVERGENCE wire={payload:?}",
    );

    let arg = stamp_header(&img);
    let (r, _) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsquerysend,
        m,
        [Datum::from_usize(arg.as_ptr() as usize)],
    );
    let d = r.expect("fc_tsquerysend infallible on valid images at fuzz sizes");
    assert!(
        read_varlena_data(d) == rs.data(),
        "fc_tsquerysend vs core DIVERGENCE wire={payload:?}",
    );
}

// ---------------------------------------------------------------------------
// Arm 4: image operators (3662..3672, 3691/3692, 5003/5004); fc planes.
// ---------------------------------------------------------------------------

fn ops_arm(payload: &[u8]) {
    let Some((head, rest)) = payload.split_first_chunk::<3>() else {
        return;
    };
    if rest.contains(&0) || core::str::from_utf8(rest).is_err() {
        return; // cstring + pg_verify_mbstr boundary (module header)
    }
    let distance = i16::from_le_bytes([head[0], head[1]]) as i32;
    let split = head[2] as usize % (rest.len() + 1);
    let (ta, tb) = rest.split_at(split);

    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let m = cx.mcx();

    // Build both images Rust-side in soft mode (parser parity is arms 0-2's
    // plane; failures here just leave the domain).
    let mut esc = SoftErrorContext::new(false);
    let Ok(Some(pa)) = parse_tsquery(m, ta, 0, Some(&mut esc), &mut pushval_asis) else {
        return;
    };
    let mut esc = SoftErrorContext::new(false);
    let Ok(Some(pb)) = parse_tsquery(m, tb, 0, Some(&mut esc), &mut pushval_asis) else {
        return;
    };
    let (ia, ib) = (pa.img.as_slice(), pb.img.as_slice());
    let (ra, rb) = (tsq_ref(ia), tsq_ref(ib));
    let (aa, ab) = (stamp_header(ia), stamp_header(ib));
    let (da, db) = (
        Datum::from_usize(aa.as_ptr() as usize),
        Datum::from_usize(ab.as_ptr() as usize),
    );

    // --- binops: (name, fc, C op selector, distance) ---
    use adt_tsquery_core::builtins as b;
    let binops: [(&str, PGFunction, i32, i32); 4] = [
        ("tsquery_and", b::fc_tsquery_and as PGFunction, 0, 0),
        ("tsquery_or", b::fc_tsquery_or, 1, 0),
        ("tsquery_phrase", b::fc_tsquery_phrase, 2, 1),
        ("tsquery_not", b::fc_tsquery_not, 3, 0),
    ];
    for (name, fc, cop, dist) in binops {
        let mut obuf = vec![0u8; OUT_CAP];
        let mut olen = 0i32;
        // SAFETY: images/obuf live; caps passed.
        let cst = unsafe {
            pg_diff_tsquery_binop(
                cop,
                ia.as_ptr(),
                ia.len() as i32,
                ib.as_ptr(),
                ib.len() as i32,
                dist,
                obuf.as_mut_ptr(),
                OUT_CAP as i32,
                &mut olen,
            )
        };
        let (r, _) = if cop == 3 {
            fc_call::<1>(fc, m, [da])
        } else {
            fc_call::<2>(fc, m, [da, db])
        };
        let d = r.unwrap_or_else(|e| panic!("{name} unexpectedly errored: {}", e.message));
        assert!(
            cst == 0 && &obuf[4..olen as usize] == read_varlena_data(d),
            "{name} DIVERGENCE a={:?} b={:?}: C len {olen}",
            String::from_utf8_lossy(ta),
            String::from_utf8_lossy(tb),
        );
    }

    // --- tsquery_phrase_distance (in-domain distance incl. out-of-range) ---
    {
        let mut obuf = vec![0u8; OUT_CAP];
        let mut olen = 0i32;
        // SAFETY: images/obuf live; caps passed.
        let cst = unsafe {
            pg_diff_tsquery_binop(
                2,
                ia.as_ptr(),
                ia.len() as i32,
                ib.as_ptr(),
                ib.len() as i32,
                distance,
                obuf.as_mut_ptr(),
                OUT_CAP as i32,
                &mut olen,
            )
        };
        let cerr = unsafe { pg_diff_errcode_get() };
        let mut fcinfo = LocalFcinfo::<3>::new(0);
        // SAFETY: cx outlives the call.
        unsafe { fcinfo.set_result_mcx(m) };
        fcinfo.args[0] = NullableDatum::value(da);
        fcinfo.args[1] = NullableDatum::value(db);
        fcinfo.args[2] = NullableDatum::value(Datum::from_i32(distance));
        match b::fc_tsquery_phrase_distance(None, &mut fcinfo) {
            Ok(d) => assert!(
                cst == 0 && &obuf[4..olen as usize] == read_varlena_data(d),
                "tsquery_phrase_distance DIVERGENCE dist={distance} a={:?} b={:?}",
                String::from_utf8_lossy(ta),
                String::from_utf8_lossy(tb),
            ),
            Err(e) => {
                let rc = err_class(&e);
                assert!(
                    cst == 1 && cerr == rc,
                    "tsquery_phrase_distance DIVERGENCE dist={distance}: C=(st {cst} err {cerr}) Rust=Err(class {rc})",
                );
            }
        }
    }

    // --- cmp + the six sign tests ---
    {
        let mut ccmp = 0i32;
        // SAFETY: images live.
        let cst = unsafe {
            pg_diff_tsquery_cmp(
                ia.as_ptr(),
                ia.len() as i32,
                ib.as_ptr(),
                ib.len() as i32,
                &mut ccmp,
            )
        };
        let rcmp = compare_tsq(ra, rb, m).expect("compare alloc at fuzz sizes");
        assert!(
            cst == 0 && ccmp == rcmp,
            "tsquery_cmp DIVERGENCE a={:?} b={:?}: C={ccmp} Rust={rcmp}",
            String::from_utf8_lossy(ta),
            String::from_utf8_lossy(tb),
        );
        let signs: [(&str, PGFunction, bool); 7] = [
            ("lt", b::fc_tsquery_lt as PGFunction, ccmp < 0),
            ("le", b::fc_tsquery_le, ccmp <= 0),
            ("eq", b::fc_tsquery_eq, ccmp == 0),
            ("ne", b::fc_tsquery_ne, ccmp != 0),
            ("ge", b::fc_tsquery_ge, ccmp >= 0),
            ("gt", b::fc_tsquery_gt, ccmp > 0),
            ("cmp", b::fc_tsquery_cmp, true /* value checked below */),
        ];
        for (name, fc, want) in signs {
            let (r, _) = fc_call::<2>(fc, m, [da, db]);
            let d = r.expect("cmp family infallible at fuzz sizes");
            if name == "cmp" {
                assert!(
                    d.as_i32() == ccmp,
                    "fc_tsquery_cmp DIVERGENCE: C={ccmp} fc={}",
                    d.as_i32(),
                );
            } else {
                assert!(
                    d.as_bool() == want,
                    "fc_tsquery_{name} DIVERGENCE: ccmp={ccmp} fc={}",
                    d.as_bool(),
                );
            }
        }
    }

    // --- tsq_mcontains / tsq_mcontained ---
    {
        let mut cb = 0i32;
        // SAFETY: images live.
        let cst = unsafe {
            pg_diff_tsq_mcontains(
                ia.as_ptr(),
                ia.len() as i32,
                ib.as_ptr(),
                ib.len() as i32,
                &mut cb,
            )
        };
        let rres = tsq_mcontains_core(m, ra, rb).expect("mcontains alloc at fuzz sizes");
        assert!(
            cst == 0 && (cb != 0) == rres,
            "tsq_mcontains DIVERGENCE a={:?} b={:?}: C={cb} Rust={rres}",
            String::from_utf8_lossy(ta),
            String::from_utf8_lossy(tb),
        );
        let (r, _) = fc_call::<2>(b::fc_tsq_mcontains, m, [da, db]);
        assert!(r.expect("infallible").as_bool() == rres, "fc_tsq_mcontains vs core");
        // mcontained(a, b) == mcontains(b, a): C driven swapped.
        let mut cb2 = 0i32;
        // SAFETY: images live.
        let cst2 = unsafe {
            pg_diff_tsq_mcontains(
                ib.as_ptr(),
                ib.len() as i32,
                ia.as_ptr(),
                ia.len() as i32,
                &mut cb2,
            )
        };
        let (r, _) = fc_call::<2>(b::fc_tsq_mcontained, m, [da, db]);
        assert!(
            cst2 == 0 && r.expect("infallible").as_bool() == (cb2 != 0),
            "tsq_mcontained DIVERGENCE a={:?} b={:?}",
            String::from_utf8_lossy(ta),
            String::from_utf8_lossy(tb),
        );
    }

    // --- tsquery_numnode ---
    {
        let mut cn = 0i32;
        // SAFETY: image live.
        let cst = unsafe { pg_diff_tsquery_numnode(ia.as_ptr(), ia.len() as i32, &mut cn) };
        let (r, _) = fc_call::<1>(b::fc_tsquery_numnode, m, [da]);
        let rn = r.expect("numnode infallible").as_i32();
        assert!(
            cst == 0 && cn == rn,
            "tsquery_numnode DIVERGENCE a={:?}: C={cn} Rust={rn}",
            String::from_utf8_lossy(ta),
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 5: tsquerytree (3673); fc plane.
// ---------------------------------------------------------------------------

fn tree_arm(payload: &[u8]) {
    if payload.contains(&0) || core::str::from_utf8(payload).is_err() {
        return; // cstring + pg_verify_mbstr boundary (module header)
    }
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let m = cx.mcx();
    let mut esc = SoftErrorContext::new(false);
    let Ok(Some(p)) = parse_tsquery(m, payload, 0, Some(&mut esc), &mut pushval_asis) else {
        return;
    };
    let img = p.img.as_slice();

    let mut cbuf = vec![0u8; OUT_CAP];
    let mut clen = 0i32;
    // SAFETY: img/cbuf live; caps passed.
    let cst = unsafe {
        pg_diff_tsquerytree(
            img.as_ptr(),
            img.len() as i32,
            cbuf.as_mut_ptr() as *mut c_char,
            OUT_CAP as i32,
            &mut clen,
        )
    };
    let rt = tsquerytree_core(m, tsq_ref(img)).expect("tsquerytree alloc at fuzz sizes");
    assert!(
        cst == 0 && &cbuf[..clen as usize] == rt.as_slice(),
        "tsquerytree DIVERGENCE input={:?}: C={:?} Rust={:?}",
        String::from_utf8_lossy(payload),
        String::from_utf8_lossy(&cbuf[..clen.max(0) as usize]),
        String::from_utf8_lossy(rt.as_slice()),
    );

    let arg = stamp_header(img);
    let (r, _) = fc_call::<1>(
        adt_tsquery_core::builtins::fc_tsquerytree,
        m,
        [Datum::from_usize(arg.as_ptr() as usize)],
    );
    let d = r.expect("fc_tsquerytree infallible on valid images at fuzz sizes");
    assert!(
        read_varlena_data(d) == rt.as_slice(),
        "fc_tsquerytree vs core DIVERGENCE input={:?}",
        String::from_utf8_lossy(payload),
    );
}

// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Arm 6: cleanup_tsquery_stopwords over generated parse-internal images.
//
// Upstream reaches the QI_VALSTOP folding arms (incl. the OP_PHRASE
// distance folding) only through the dictionary-morph pushval of the
// excluded(engine) to_tsany crate — pushval_asis can mint at most one
// trailing QI_VALSTOP (websearch push_stop at end-of-input) and never under
// OP_PHRASE. The FUNCTION is pure over the parse-internal image, so this
// arm generates well-formed polish trees directly (valid opers, real
// valcrc, bounded depth/size — the same trust contract parse_tsquery's own
// polstr gets on both sides) and compares C cleanup_tsquery_stopwords
// (tsquery_cleanup.c:387, noisy=false) against the shipped Rust
// cleanup_tsquery_stopwords. Payload byte 0 bit 7 selects the empty image
// (the size==0 early-copy arm on both sides).
// ---------------------------------------------------------------------------

struct TreeGen<'a> {
    b: &'a [u8],
    i: usize,
    items_left: usize,
}

impl TreeGen<'_> {
    fn byte(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }

    fn gen(
        &mut self,
        depth: usize,
        items: &mut Vec<adt_tsvector_core::query::Item>,
        pool: &mut Vec<u8>,
    ) {
        use adt_tsvector_core::query::{Item, Operand, Operator, OP_AND, OP_NOT, OP_OR, OP_PHRASE};
        self.items_left = self.items_left.saturating_sub(1);
        let sel = self.byte();
        // Leaf when out of budget or 1-in-4 by the selector.
        if depth >= 16 || self.items_left < 2 || sel & 0x03 == 0 {
            if sel & 0x04 != 0 {
                items.push(Item::ValStop);
                return;
            }
            let w = self.byte();
            let len = 1 + (w as usize & 0x03);
            const ALPHA: [u8; 4] = [b'a', b'b', b'c', b'x'];
            let mut word = [0u8; 4];
            for (k, ch) in word.iter_mut().take(len).enumerate() {
                *ch = ALPHA[((w >> (2 * k)) & 3) as usize];
            }
            let word = &word[..len];
            items.push(Item::Val(Operand {
                weight: (w >> 4) & 0x0F,
                prefix: sel & 0x08 != 0,
                valcrc: crc32c::legacy_crc32_lexeme(word) as i32,
                length: len,
                distance: pool.len(),
            }));
            pool.extend_from_slice(word);
            pool.push(0);
            return;
        }
        let oper = match sel & 0x03 {
            1 => OP_NOT,
            2 => {
                if sel & 0x04 != 0 {
                    OP_AND
                } else {
                    OP_OR
                }
            }
            _ => OP_PHRASE,
        };
        let distance = if oper == OP_PHRASE {
            (u16::from_le_bytes([self.byte(), self.byte()]) % 16385) as i16
        } else {
            0
        };
        let idx = items.len();
        items.push(Item::Opr(Operator { oper, distance, left: 1 }));
        self.gen(depth + 1, items, pool); // right child (polish +1)
        if oper != OP_NOT {
            let left = (items.len() - idx) as u32;
            if let Item::Opr(ref mut o) = items[idx] {
                o.left = left;
            }
            self.gen(depth + 1, items, pool); // left child (polish +left)
        }
    }
}

fn cleanup_arm(payload: &[u8]) {
    let cx = mcx::MemoryContext::new("tsq_fuzz");
    let m = cx.mcx();

    let mut items = Vec::new();
    let mut pool = Vec::new();
    if !payload.first().is_none_or(|&b| b & 0x80 != 0) {
        let mut g = TreeGen { b: payload, i: 1, items_left: 96 };
        g.gen(0, &mut items, &mut pool);
    }
    let img = adt_tsquery_core::parse::build_query_image(m, &items, &pool)
        .expect("image alloc at fuzz sizes");

    let mut cbuf = vec![0u8; OUT_CAP];
    let mut clen = 0i32;
    // SAFETY: img/cbuf live; caps passed.
    let cst = unsafe {
        pg_diff_tsquery_cleanup(
            img.as_ptr(),
            img.len() as i32,
            cbuf.as_mut_ptr(),
            OUT_CAP as i32,
            &mut clen,
        )
    };
    let r = adt_tsquery_core::cleanup::cleanup_tsquery_stopwords(m, &img, false)
        .expect("cleanup alloc at fuzz sizes");
    assert!(
        cst == 0 && &cbuf[..clen as usize] == r.as_slice(),
        "cleanup_tsquery_stopwords DIVERGENCE items={items:?}: C=(st {cst} len {clen}) Rust len {}",
        r.len(),
    );
}

// ---------------------------------------------------------------------------
// Arm 7: BULK — the program-limit guards the 2KiB cap fences.
//
// Sub-mode by payload byte 0 bit 0: 0 = standard-mode parse (push_value
// word-too-long 2047 / op-pool MAXSTRPOS 54000 arms), 1 = tsqueryrecv
// (operand-too-long / total-operand-length guards). The stack-depth seam
// stays sound under the bigger cap because recursion is bounded by
// PRE-SCANS, not the byte count: parse recursion (makepol, both sides) is
// nesting depth <= '(' count, capped at 64; recv recursion (findoprnd,
// both sides) is <= the claimed item count, capped at 1024.
// ---------------------------------------------------------------------------

/// Magic gate on the megabyte arm: random mutation lands on arm 7 for 1/8
/// of all inputs, and an ungated oversize path collapses throughput
/// (p1-laneae measured ~190 exec/s and a blown job deadline on the same
/// shape). Requiring this prefix makes entry deliberate — seeds and the
/// dictionary carry it; mutation almost never mints it.
const BULK_MAGIC: &[u8; 4] = b"TQB7";

fn bulk_arm(payload: &[u8]) {
    let Some((magic, payload)) = payload.split_first_chunk::<4>() else {
        return;
    };
    if magic != BULK_MAGIC {
        return;
    }
    let Some((&mode, rest)) = payload.split_first() else {
        return;
    };
    if mode & 1 == 0 {
        if rest.contains(&0) || core::str::from_utf8(rest).is_err() {
            return; // same cstring + pg_verify_mbstr boundary as arm 0
        }
        if rest.iter().filter(|&&b| b == b'(').count() > 64 {
            return; // recursion pre-scan (module header)
        }
        let cx = mcx::MemoryContext::new("tsq_fuzz");
        let _ = parse_diff_cap(cx.mcx(), rest, 0, BULK_OUT_CAP);
    } else {
        let claimed = rest
            .first_chunk::<4>()
            .map(|b| u32::from_be_bytes(*b) as usize)
            .unwrap_or(0);
        if claimed > 1024 {
            // Recursion + arena pre-scan: findoprnd depth <= item count on
            // both sides, and ASAN-inflated release frames make 4096 chained
            // NOTs marginal against the 8MiB main stack; 1024 keeps >5x
            // margin while the pool-overflow band (~600 items) stays in.
            return;
        }
        recv_send_core_arm(rest, BULK_OUT_CAP);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        // 32MiB stack: the BULK seeds recurse ~530 findoprnd frames, which
        // fits the fuzz binary's 8MiB main stack but not the 2MiB default
        // test-thread stack under debug frame sizes (16MiB sufficed on the
        // lane; post boundary-audit the PgResult-threaded frames are deeper
        // still and 16MiB SIGSEGVed on main — same bump as the tsqrw_diff
        // sibling).
        std::thread::Builder::new()
            .stack_size(32 << 20)
            .spawn(|| {
                let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsquery_core_diff");
                let mut n = 0;
                for e in std::fs::read_dir(dir).expect("corpus/tsquery_core_diff missing") {
                    let p = e.unwrap().path();
                    if p.is_file() {
                        tsquery_core_diff(&std::fs::read(&p).unwrap());
                        n += 1;
                    }
                }
                assert!(n >= 30, "expected >=30 seeds, found {n}");
            })
            .expect("spawn")
            .join()
            .expect("replay thread panicked");
    }

    /// Per-arm smoke: known-answer shapes through the full diff drivers
    /// (any C-vs-Rust disagreement asserts inside).
    #[test]
    fn arms_smoke() {
        // arm 0: value, ops, weights/prefix, parens, NOT, phrase, errors,
        // empty (NOTICE plane), distance overflow (22023 in-parser).
        tsquery_core_diff(b"\x00cat");
        tsquery_core_diff(b"\x00cat & dog");
        tsquery_core_diff(b"\x00cat:A | dog:*");
        tsquery_core_diff(b"\x00!( cat <-> dog ) & mouse:BC");
        tsquery_core_diff(b"\x00cat <5> dog");
        tsquery_core_diff(b"\x00cat &"); // syntax error
        tsquery_core_diff(b"\x00"); // empty -> NOTICE + empty image
        tsquery_core_diff(b"\x00 ( "); // syntax error
        tsquery_core_diff(b"\x00cat <99999> dog"); // distance too big
        // arm 1: websearch (implicit AND, OR keyword, -NOT, quoted phrase),
        // both ctype knob values.
        tsquery_core_diff(b"\x01\x00cat dog");
        tsquery_core_diff(b"\x01\x00cat or dog");
        tsquery_core_diff(b"\x01\x00-cat \"fat rat\"");
        tsquery_core_diff(b"\x01\x01cat or dog");
        tsquery_core_diff(b"\x01\x00\"\"");
        // arm 2: plain.
        tsquery_core_diff(b"\x02fat rats eat");
        tsquery_core_diff(b"\x02");
        // arm 3: recv — valid frames (built by hand) + junk.
        // size=1, one QI_VAL(weight 0, prefix 0, "a").
        tsquery_core_diff(b"\x03\x00\x00\x00\x01\x01\x00\x00a\x00");
        // size=3: OPR(AND) VAL(b) VAL(a) polish order.
        tsquery_core_diff(b"\x03\x00\x00\x00\x03\x02\x02\x01\x00\x00b\x00\x01\x00\x00a\x00");
        tsquery_core_diff(b"\x03\xff\xff\xff\xff"); // absurd size
        tsquery_core_diff(b"\x03\x00\x00\x00\x01\x07"); // bad item type
        tsquery_core_diff(b"\x03\x00\x00\x00\x01\x02\x09"); // bad oper
        tsquery_core_diff(b"\x03"); // truncated
        // trailing junk after a valid frame (consumed-cursor plane).
        tsquery_core_diff(b"\x03\x00\x00\x00\x01\x01\x00\x00a\x00JUNK");
        // arm 4: ops over two parses (split byte 3, distance 2).
        tsquery_core_diff(b"\x04\x02\x00\x03cata & dog");
        tsquery_core_diff(b"\x04\xff\xff\x03cata & dog"); // negative distance
        tsquery_core_diff(b"\x04\xff\x7f\x03cata & dog"); // distance 32767 > MAXENTRYPOS
        tsquery_core_diff(b"\x04\x01\x00\x00cat"); // empty a-side
        tsquery_core_diff(b"\x04\x01\x00\x08cat & ratcat & rat"); // eq shapes
        // arm 5: tree (NOT stripping, degenerate T, empty).
        tsquery_core_diff(b"\x05cat & !dog");
        tsquery_core_diff(b"\x05!cat");
        tsquery_core_diff(b"\x05(cat | !dog) & mouse");
        tsquery_core_diff(b"\x05");
    }
}

#[cfg(test)]
mod int_wrap_parity {
    use super::*;

    /// Step-D pin (p1-laneae handoff): C parses phrase distance with strtol
    /// into `long` and range-checks BEFORE the (int16) narrowing
    /// (tsquery.c:203-231), so — unlike the tsvector position wrap laneae
    /// found — there is no wrap window: every distance > MAXENTRYPOS is
    /// 22023 on both sides, every in-range distance is value-identical, and
    /// the recv path wraps identically via (int16) casts on both sides.
    /// The whole-driver call asserts all planes (value/image/error/sqlstate).
    #[test]
    fn phrase_distance_parity_pins() {
        for d in [
            "20069458489",          // laneae's tsvector wrap witness
            "2147483648",           // 2^31
            "2147483656761",        // would wrap int32 to 8761
            "65541",                // 2^16 + 5 (int16-wrap band)
            "16384",                // MAXENTRYPOS
            "16385",                // MAXENTRYPOS + 1
            "10000000000000000000", // 10^19 (> i64/LONG_MAX: ERANGE vs saturate)
            "16383",                // MAXENTRYPOS - 1 (in-range value plane)
            "0",
        ] {
            let mut input = vec![0u8]; // arm 0
            input.extend_from_slice(format!("a <{d}> b").as_bytes());
            tsquery_core_diff(&input);
        }
        // recv-plane narrowing: distance half-words 0x8000 and 0xFFFF wrap
        // to negative i16 identically (seeded in the corpus; replayed by
        // seed_corpus_replays_clean).
    }
}
