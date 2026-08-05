//! jsonpath_diff: differential fuzz driver — shipped Rust `adt_jsonpath` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/jsonpath/*: verbatim jsonpath.c + the bison/flex-generated
//! jsonpath_gram.c / jsonpath_scan.c + the full 18.3 regex engine + verbatim
//! numeric/formatting/stringinfo/pqformat/json/mbutils extracts).
//! Crate under test: crates/backend/utils/adt/jsonpath.
//!
//! Comparison planes (the harness contract): value bytes (the full on-disk
//! jsonpath varlena image, the canonical output text, the wire payload, the
//! mutability bool), error verdict (ok / hard error / soft error), and
//! errcode/sqlstate. Message text is out of scope (the C side captures it for
//! the panic report only).
//!
//! Input layout: [selector][payload]; selector % 3 picks the arm:
//!   0 IN + OUT (oids 4001/4003) — payload = [mode][jsonpath source text].
//!     mode&1 = soft-error mode: BOTH sides get a live escontext
//!     (ErrorSaveNode / ErrorSaveContext), so soft errors must return
//!     "no value + recorded errcode" on both sides instead of raising.
//!     Text must be NUL-free (the C entry is a cstring); len capped.
//!     On success the two images are compared byte-for-byte, then both sides'
//!     jsonpath_out over the image is compared as exact text.
//!   1 RECV + SEND (oids 4002/4004) — payload = raw wire bytes exactly as a
//!     client would send them (version byte + text). On success the images are
//!     compared, then both sides' jsonpath_send over the image.
//!   2 MUTABILITY (jspIsMutable, mutability.rs) — payload =
//!     [varsel][jsonpath source text]: both sides parse (verdict+sqlstate
//!     compared as in arm 0), and on success run jsp_is_mutable/jspIsMutable
//!     over a selector-driven PASSING-variables model (0..3 vars drawn from a
//!     fixed name × type table including the datetime-typed oids that drive
//!     the interesting DtStatus transitions) and compare the bool.
//!
//! FC-WRAPPER PLANE: arms 0 and 1 additionally route the (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrappers over a native
//! types_fmgr::LocalFcinfo frame — fc_jsonpath_in (hard and escontext-armed
//! shapes), fc_jsonpath_out, fc_jsonpath_recv, fc_jsonpath_send — and asserts
//! wrapper == core. C-parity keeps being carried by the core comparison; the
//! plane makes the wrapper lines execute every iteration with an in-harness
//! oracle.
//!
//! PINNED ENVIRONMENT (identical on both sides, documented models, not
//! carves of behavior):
//!   - server encoding UTF-8: Rust `mbutils::SetDatabaseEncoding(PG_UTF8)`;
//!     C: shim mb/pg_wchar.h + pg_support_min.c pin GetDatabaseEncoding to
//!     PG_UTF8 with the verbatim UTF-8 wchar functions. This is the pin the
//!     crate itself makes.
//!   - default collation = C ctype: Rust
//!     `pg_locale::set_default_locale_c_for_tests()` (C_LOCALE,
//!     ctype_is_c = true) so regex_core's pg_set_regex_collation lands on its
//!     C strategy; C: pg_jsonpath_env.c's pg_newlocale_from_collation returns
//!     a ctype_is_c entry so regc_pg_locale.c lands on
//!     PG_REGEX_STRATEGY_C. like_regex validity therefore compares under one
//!     locale model on both sides.
//!
//! CARVE-OUTS (ratified non-surfaces, documented per the skill's rules):
//!   - stack-depth exhaustion (54001): pgrust's recursive-descent parser
//!     guards NATIVE-stack recursion with check_stack_depth (54001); C's
//!     bison parser keeps its stacks on the heap (YYMAXDEPTH -> 42601) and
//!     the in-harness C shim's check_stack_depth is a no-op, so a Rust-side
//!     54001 has no C counterpart by construction. setup() arms the Rust
//!     guard per-thread at 1536kB (= the 2 MiB libtest thread minus C's
//!     STACK_DEPTH_SLOP admission rule; this also keeps deep corpus seeds
//!     from aborting a debug test thread — the pre-guard bug this lane
//!     fixed, see README divergence 2), and any Rust 54001 verdict is carved
//!     from comparison (`depth_carved`). In release the 1536kB budget is far
//!     above what any MAX_TEXT-bounded input can consume (~1.2kB/level,
//!     <=511 levels), so deep-nesting inputs stay in-domain up to the cap
//!     and ARE compared.
//!   - message/detail text: out of scope by the standing harness contract
//!     (sqlstate is the error-identity plane).
//!   - INVALID-UTF-8 SOURCE TEXT in arms 0 and 2 (added 2026-07-31 after the
//!     first local smoke, see fuzz/README-TODO-jsonpath_diff.md "divergence
//!     1"): a real server never calls jsonpath_in on unvalidated bytes —
//!     pg_any_to_server (mbutils.c) validates with pg_verify_mbstr at the
//!     client/server boundary before any input function runs, so an invalid
//!     sequence cannot reach jsonpath_in, nor be stored in a jsonpath value,
//!     nor reach jspIsMutable's datetime-template inspection. The encoding
//!     plane is therefore tested exactly where PostgreSQL itself tests it:
//!     arm 1 (jsonpath_recv), which runs that validation in-band and IS
//!     compared on all planes (that comparison already caught one oracle
//!     shim bug). Arms 0/2 require `core::str::from_utf8(text).is_ok()`,
//!     matching the invariant the pipeline guarantees; all multibyte-valid
//!     text stays in domain.
//!
//! SKIPPED rows: none — the crate's four catalog functions (4001-4004) are all
//! driven through both their cores and their fc wrappers, plus the
//! non-catalog planner entry jsp_is_mutable (arm 2).

use core::ffi::{c_char, c_int, c_uint};
use std::ffi::CString;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_diff_jsonpath_in(
        s: *const u8,
        len: usize,
        soft: c_int,
        image_out: *mut *const u8,
        image_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonpath_out(
        image: *const u8,
        image_len: usize,
        text_out: *mut *const u8,
        text_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonpath_recv(
        wire: *const u8,
        wire_len: usize,
        image_out: *mut *const u8,
        image_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonpath_send(
        image: *const u8,
        image_len: usize,
        wire_out: *mut *const u8,
        wire_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsp_is_mutable(
        image: *const u8,
        image_len: usize,
        nvars: c_int,
        varnames: *const *const c_char,
        vartypes: *const c_uint,
        mutable_out: *mut c_int,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonpath_last_msg() -> *const c_char;
}

/// Source-text cap (see the stack-depth carve in the module header). Also
/// bounds the oracle's per-iteration arena.
const MAX_TEXT: usize = 512;
/// Wire-buffer cap for arm 1.
const MAX_WIRE: usize = 512;

// ---------------------------------------------------------------------------
// Pinned environment (both sides; see the module header)
// ---------------------------------------------------------------------------

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Tolerate the sibling jsonpathexec_diff target installing the
        // IDENTICAL seam implementations first (one test binary; seam set
        // panics on double install after swapping in the same impl).
        let _ = std::panic::catch_unwind(mbutils::init_seams);
        let _ = std::panic::catch_unwind(pg_locale::init_seams);
        pg_locale::set_default_locale_c_for_tests();
    });
    // Per-thread (cargo test runs arms on parallel threads).
    let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    if !pg_locale::default_locale_installed() {
        pg_locale::set_default_locale_c_for_tests();
    }
    // Arm the Rust-side recursion guard exactly as a backend thread does
    // (base at the dispatch frame — every parser/flatten/print recursion is
    // deeper). Threshold: the smallest thread this harness runs on is a
    // 2 MiB libtest thread, and C's own admission rule for max_stack_depth
    // is stack minus STACK_DEPTH_SLOP (512kB), hence 1536kB. That keeps deep
    // corpus inputs from ABORTING a debug test thread (the pre-guard bug
    // this lane fixed), while in release 1536kB is far above what any
    // MAX_TEXT-bounded input can use (~1.2kB/nesting level measured, <=511
    // levels), so the carve below never engages there. See "divergence 2"
    // in fuzz/README-TODO-jsonpath_diff.md.
    const HARNESS_MAX_STACK_DEPTH_KB: i32 =
        (2048 - stack_depth::STACK_DEPTH_SLOP as i32 / 1024);
    if stack_depth::max_stack_depth() != HARNESS_MAX_STACK_DEPTH_KB {
        stack_depth::set_max_stack_depth(HARNESS_MAX_STACK_DEPTH_KB);
        stack_depth::assign_max_stack_depth(HARNESS_MAX_STACK_DEPTH_KB);
    }
    let _ = stack_depth::set_stack_base();
}

/// STACK-DEPTH CARVE (divergence 2 in the README): pgrust's parser guards
/// native-stack recursion with check_stack_depth (54001) because it is a
/// recursive-descent port; C's bison parser keeps its stacks on the HEAP and
/// the in-harness C shim's check_stack_depth is a no-op, so a Rust-side
/// 54001 has no C counterpart by construction. Whenever the Rust side (core
/// or fc wrapper) reports 54001, the row is out of the comparison domain —
/// this is a ratified non-surface, exactly bounded by that one sqlstate.
fn depth_carved(v: Verdict) -> bool {
    matches!(
        v,
        Verdict::Hard(st) | Verdict::Soft(st)
            if st == types_error::ERRCODE_STATEMENT_TOO_COMPLEX.0
    )
}

/// Shared domain for a jsonpath SOURCE TEXT (arms 0 and 2). See the
/// module-header carve-outs:
///   - length cap (recursion bound, so the 54001 plane stays out of domain);
///   - NUL-free: the C entry point is a cstring and PG text never carries an
///     interior NUL;
///   - valid UTF-8: pg_any_to_server validates at the client/server boundary
///     before any input function runs, so unvalidated bytes cannot reach
///     jsonpath_in in a real server. Arm 1 (recv) runs that validation
///     in-band and keeps the encoding plane under comparison.
fn in_domain(text: &[u8]) -> bool {
    text.len() <= MAX_TEXT && !text.contains(&0) && core::str::from_utf8(text).is_ok()
}

// ---------------------------------------------------------------------------
// Verdicts + oracle-side wrappers
// ---------------------------------------------------------------------------

/// The error-verdict plane: what a call did, independent of any value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ok,
    /// Hard error (raised): carries the sqlstate.
    Hard(i32),
    /// Soft error (recorded in the escontext): carries the sqlstate.
    Soft(i32),
}

fn c_last_msg() -> String {
    // SAFETY: the oracle's message buffer is a live TLS NUL-terminated array.
    unsafe { std::ffi::CStr::from_ptr(pg_diff_jsonpath_last_msg()) }
        .to_string_lossy()
        .into_owned()
}

/// C jsonpath_in: (verdict, image bytes on success).
fn c_in(text: &[u8], soft: bool) -> (Verdict, Vec<u8>) {
    let mut img: *const u8 = core::ptr::null();
    let mut ilen: usize = 0;
    let mut st: c_int = 0;
    // SAFETY: text is a live slice; out params are live locals.
    let rc = unsafe {
        pg_diff_jsonpath_in(
            text.as_ptr(),
            text.len(),
            soft as c_int,
            &mut img,
            &mut ilen,
            &mut st,
        )
    };
    match rc {
        0 => {
            // SAFETY: on rc==0 the oracle returned a live arena image of ilen
            // bytes, valid until the next pg_diff_* call on this thread.
            let bytes = unsafe { core::slice::from_raw_parts(img, ilen) }.to_vec();
            (Verdict::Ok, bytes)
        }
        2 => (Verdict::Soft(st), Vec::new()),
        _ => (Verdict::Hard(st), Vec::new()),
    }
}

/// C jsonpath_out over a full varlena image: (verdict, text bytes).
fn c_out(image: &[u8]) -> (Verdict, Vec<u8>) {
    let mut txt: *const u8 = core::ptr::null();
    let mut tlen: usize = 0;
    let mut st: c_int = 0;
    // SAFETY: image is a live slice; out params are live locals.
    let rc = unsafe {
        pg_diff_jsonpath_out(image.as_ptr(), image.len(), &mut txt, &mut tlen, &mut st)
    };
    if rc == 0 {
        // SAFETY: live arena cstring of tlen bytes (see c_in).
        (Verdict::Ok, unsafe { core::slice::from_raw_parts(txt, tlen) }.to_vec())
    } else {
        (Verdict::Hard(st), Vec::new())
    }
}

/// C jsonpath_recv over a wire buffer: (verdict, image bytes).
fn c_recv(wire: &[u8]) -> (Verdict, Vec<u8>) {
    let mut img: *const u8 = core::ptr::null();
    let mut ilen: usize = 0;
    let mut st: c_int = 0;
    // SAFETY: wire is a live slice; out params are live locals.
    let rc =
        unsafe { pg_diff_jsonpath_recv(wire.as_ptr(), wire.len(), &mut img, &mut ilen, &mut st) };
    if rc == 0 {
        // SAFETY: live arena image (see c_in).
        (Verdict::Ok, unsafe { core::slice::from_raw_parts(img, ilen) }.to_vec())
    } else {
        (Verdict::Hard(st), Vec::new())
    }
}

/// C jsonpath_send over a full varlena image: (verdict, wire payload bytes).
fn c_send(image: &[u8]) -> (Verdict, Vec<u8>) {
    let mut wire: *const u8 = core::ptr::null();
    let mut wlen: usize = 0;
    let mut st: c_int = 0;
    // SAFETY: image is a live slice; out params are live locals.
    let rc = unsafe {
        pg_diff_jsonpath_send(image.as_ptr(), image.len(), &mut wire, &mut wlen, &mut st)
    };
    if rc == 0 {
        // SAFETY: live arena bytea payload (see c_in).
        (Verdict::Ok, unsafe { core::slice::from_raw_parts(wire, wlen) }.to_vec())
    } else {
        (Verdict::Hard(st), Vec::new())
    }
}

/// C jspIsMutable over a full varlena image + the PASSING-variables model.
fn c_is_mutable(image: &[u8], vars: &[(&[u8], u32)]) -> (Verdict, bool) {
    let names: Vec<CString> = vars
        .iter()
        .map(|(n, _)| CString::new(*n).expect("var names are NUL-free literals"))
        .collect();
    let ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
    let types: Vec<c_uint> = vars.iter().map(|(_, t)| *t as c_uint).collect();
    let mut mutable: c_int = -1;
    let mut st: c_int = 0;
    // SAFETY: image/ptrs/types are live slices; out params are live locals.
    let rc = unsafe {
        pg_diff_jsp_is_mutable(
            image.as_ptr(),
            image.len(),
            vars.len() as c_int,
            ptrs.as_ptr(),
            types.as_ptr(),
            &mut mutable,
            &mut st,
        )
    };
    if rc == 0 {
        (Verdict::Ok, mutable != 0)
    } else {
        (Verdict::Hard(st), false)
    }
}

// ---------------------------------------------------------------------------
// Rust-side helpers
// ---------------------------------------------------------------------------

/// Shipped-Rust jsonpath_in with the same soft/hard mode: (verdict, image).
fn rust_in(mcx: mcx::Mcx<'_>, text: &[u8], soft: bool) -> (Verdict, Vec<u8>) {
    if soft {
        let mut esc = types_error::SoftErrorContext::new(true);
        match adt_jsonpath::path::jsonpath_in(mcx, text, Some(&mut esc)) {
            Ok(Some(v)) => (Verdict::Ok, v.to_vec()),
            Ok(None) => {
                assert!(
                    esc.error_occurred(),
                    "rust jsonpath_in returned None without recording a soft error"
                );
                (
                    Verdict::Soft(esc.error().expect("recorded soft error").sqlstate().0),
                    Vec::new(),
                )
            }
            Err(e) => (Verdict::Hard(e.sqlstate().0), Vec::new()),
        }
    } else {
        match adt_jsonpath::path::jsonpath_in(mcx, text, None) {
            Ok(Some(v)) => (Verdict::Ok, v.to_vec()),
            Ok(None) => unreachable!("hard-mode jsonpath_in cannot report a soft error"),
            Err(e) => (Verdict::Hard(e.sqlstate().0), Vec::new()),
        }
    }
}

/// Shipped-Rust jsonpath_out: (verdict, text bytes without the trailing NUL).
fn rust_out(mcx: mcx::Mcx<'_>, image: &[u8]) -> (Verdict, Vec<u8>) {
    match adt_jsonpath::path::jsonpath_out(mcx, image) {
        Ok(v) => {
            assert_eq!(v.last(), Some(&0), "jsonpath_out result is NUL-terminated");
            (Verdict::Ok, v[..v.len() - 1].to_vec())
        }
        Err(e) => (Verdict::Hard(e.sqlstate().0), Vec::new()),
    }
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx)
// ---------------------------------------------------------------------------

fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Same, with an armed ErrorSaveNode in fcinfo->context (soft-error shape).
fn fc_call_soft<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    esc: &mut ErrorSaveNode,
    args: [NullableDatum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    fcinfo.context = esc.fm_node_ptr();
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Read back a full jsonpath varlena image behind a by-ref result Datum.
fn datum_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: fc jsonpath results are live 4B-header varlena images in the
    // armed arena, read before the arena drops.
    let hdr = unsafe { core::slice::from_raw_parts(p, 4) };
    let len = (u32::from_ne_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) >> 2) as usize;
    // SAFETY: the image is readable through its full VARSIZE.
    unsafe { core::slice::from_raw_parts(p, len) }
}

/// Read back a NUL-terminated cstring result Datum (jsonpath_out).
fn datum_cstring<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc_jsonpath_out returns a live NUL-terminated cstring.
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char) }.to_bytes()
}

/// Read back a bytea result payload (jsonpath_send).
fn datum_bytea_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: fc_jsonpath_send returns a live 4B-header bytea image.
    let hdr = unsafe { core::slice::from_raw_parts(p, 4) };
    let len = (u32::from_ne_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) >> 2) as usize;
    // SAFETY: readable through its full VARSIZE; payload starts at +4.
    unsafe { core::slice::from_raw_parts(p.add(4), len - 4) }
}

/// The verdict of an fc wrapper call, in the same three-way shape.
fn fc_verdict(r: &PgResult<Datum>, isnull: bool, esc: Option<&ErrorSaveNode>) -> Verdict {
    match r {
        Err(e) => Verdict::Hard(e.sqlstate().0),
        Ok(_) => match esc {
            Some(n) if n.ctx.error_occurred() => {
                assert!(isnull, "soft-error wrapper result must be SQL NULL");
                Verdict::Soft(n.ctx.error().expect("recorded soft error").sqlstate().0)
            }
            _ => Verdict::Ok,
        },
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn jsonpath_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    setup();
    match sel % 3 {
        0 => in_out_diff(payload),
        1 => recv_send_diff(payload),
        _ => mutability_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: jsonpath_in (4001) + jsonpath_out (4003)
// ---------------------------------------------------------------------------

fn in_out_diff(payload: &[u8]) {
    let Some((&mode, text)) = payload.split_first() else {
        return;
    };
    if !in_domain(text) {
        return;
    }
    let soft = (mode & 1) != 0;

    let (cv, cimg) = c_in(text, soft);
    let cmsg = c_last_msg();

    let cx = mcx::MemoryContext::new("jsonpath_fuzz_in");
    let m = cx.mcx();
    let (rv, rimg) = rust_in(m, text, soft);
    if depth_carved(rv) {
        return; // stack-depth carve, see depth_carved
    }

    assert!(
        cv == rv,
        "jsonpath_in VERDICT/SQLSTATE DIVERGENCE soft={soft} input={:?}: C={cv:?} ({cmsg:?}) Rust={rv:?}",
        String::from_utf8_lossy(text)
    );
    assert!(
        cimg == rimg,
        "jsonpath_in IMAGE DIVERGENCE soft={soft} input={:?}: C={cimg:02x?} Rust={rimg:02x?}",
        String::from_utf8_lossy(text)
    );

    // fc-wrapper plane for jsonpath_in.
    let cs = CString::new(text).expect("NUL-free above");
    let din = NullableDatum::value(Datum::from_usize(cs.as_ptr() as usize));
    if soft {
        let mut esc = ErrorSaveNode::new(true);
        let (r, isnull) =
            fc_call_soft::<1>(adt_jsonpath::builtins::fc_jsonpath_in, m, &mut esc, [din]);
        let wv = fc_verdict(&r, isnull, Some(&esc));
        if depth_carved(wv) {
            return; // stack-depth carve (boundary frames differ), see depth_carved
        }
        assert!(
            wv == rv,
            "fc_jsonpath_in (soft) vs core VERDICT DIVERGENCE input={:?}: wrapper={wv:?} core={rv:?}",
            String::from_utf8_lossy(text)
        );
        if let (Verdict::Ok, Ok(d)) = (wv, &r) {
            assert!(
                datum_image(*d) == rimg.as_slice(),
                "fc_jsonpath_in (soft) vs core IMAGE DIVERGENCE input={:?}",
                String::from_utf8_lossy(text)
            );
        }
    } else {
        let (r, isnull) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_in, m, [din]);
        let wv = fc_verdict(&r, isnull, None);
        if depth_carved(wv) {
            return; // stack-depth carve (boundary frames differ), see depth_carved
        }
        assert!(
            wv == rv,
            "fc_jsonpath_in vs core VERDICT DIVERGENCE input={:?}: wrapper={wv:?} core={rv:?}",
            String::from_utf8_lossy(text)
        );
        if let (Verdict::Ok, Ok(d)) = (wv, &r) {
            assert!(
                datum_image(*d) == rimg.as_slice(),
                "fc_jsonpath_in vs core IMAGE DIVERGENCE input={:?}",
                String::from_utf8_lossy(text)
            );
        }
    }

    if cv != Verdict::Ok {
        return;
    }

    // ---- jsonpath_out over the (agreed) image, both sides ----
    out_planes(m, &rimg, text);
}

/// jsonpath_out core + wrapper + C oracle over one agreed image.
fn out_planes(m: mcx::Mcx<'_>, image: &[u8], provenance: &[u8]) {
    let (cov, ctext) = c_out(image);
    let cmsg = c_last_msg();
    let (rov, rtext) = rust_out(m, image);
    if depth_carved(rov) {
        return; // stack-depth carve, see depth_carved
    }
    assert!(
        cov == rov,
        "jsonpath_out VERDICT DIVERGENCE from={:?}: C={cov:?} ({cmsg:?}) Rust={rov:?}",
        String::from_utf8_lossy(provenance)
    );
    assert!(
        ctext == rtext,
        "jsonpath_out TEXT DIVERGENCE from={:?}: C={:?} Rust={:?}",
        String::from_utf8_lossy(provenance),
        String::from_utf8_lossy(&ctext),
        String::from_utf8_lossy(&rtext)
    );
    if cov != Verdict::Ok {
        return;
    }

    // fc-wrapper plane (the wrapper detoasts + frames the cstring result).
    let img = mcx::slice_in(m, image).expect("image copy at fuzz sizes");
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let (r, isnull) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_out, m, [din]);
    let wv = fc_verdict(&r, isnull, None);
    if depth_carved(wv) {
        return; // stack-depth carve (boundary frames differ), see depth_carved
    }
    assert!(
        wv == rov,
        "fc_jsonpath_out vs core VERDICT DIVERGENCE from={:?}: wrapper={wv:?} core={rov:?}",
        String::from_utf8_lossy(provenance)
    );
    if let Ok(d) = &r {
        assert!(
            datum_cstring(*d) == rtext.as_slice(),
            "fc_jsonpath_out vs core TEXT DIVERGENCE from={:?}",
            String::from_utf8_lossy(provenance)
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 1: jsonpath_recv (4002) + jsonpath_send (4004)
// ---------------------------------------------------------------------------

fn recv_send_diff(wire: &[u8]) {
    if wire.len() > MAX_WIRE {
        return;
    }

    let (cv, cimg) = c_recv(wire);
    let cmsg = c_last_msg();

    let cx = mcx::MemoryContext::new("jsonpath_fuzz_recv");
    let m = cx.mcx();
    let (rv, rimg) = {
        let mut buf = StringInfo::from_vec(mcx::slice_in(m, wire).expect("wire copy"))
            .expect("StringInfo from wire");
        buf.cursor = 0;
        match adt_jsonpath::path::jsonpath_recv(m, &mut buf) {
            Ok(v) => (Verdict::Ok, v.to_vec()),
            Err(e) => (Verdict::Hard(e.sqlstate().0), Vec::new()),
        }
    };

    if depth_carved(rv) {
        return; // stack-depth carve, see depth_carved
    }
    assert!(
        cv == rv,
        "jsonpath_recv VERDICT/SQLSTATE DIVERGENCE wire={wire:02x?}: C={cv:?} ({cmsg:?}) Rust={rv:?}"
    );
    assert!(
        cimg == rimg,
        "jsonpath_recv IMAGE DIVERGENCE wire={wire:02x?}: C={cimg:02x?} Rust={rimg:02x?}"
    );

    // fc-wrapper plane for jsonpath_recv (StringInfo arg).
    {
        let mut buf = StringInfo::from_vec(mcx::slice_in(m, wire).expect("wire copy"))
            .expect("StringInfo from wire");
        buf.cursor = 0;
        let din = NullableDatum::value(Datum::from_usize(&mut buf as *mut _ as usize));
        let (r, isnull) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_recv, m, [din]);
        let wv = fc_verdict(&r, isnull, None);
        if depth_carved(wv) {
            return; // stack-depth carve (boundary frames differ), see depth_carved
        }
        assert!(
            wv == rv,
            "fc_jsonpath_recv vs core VERDICT DIVERGENCE wire={wire:02x?}: wrapper={wv:?} core={rv:?}"
        );
        if let (Verdict::Ok, Ok(d)) = (wv, &r) {
            assert!(
                datum_image(*d) == rimg.as_slice(),
                "fc_jsonpath_recv vs core IMAGE DIVERGENCE wire={wire:02x?}"
            );
        }
    }

    if cv != Verdict::Ok {
        return;
    }

    // ---- jsonpath_send over the (agreed) image, both sides ----
    let (csv, cpayload) = c_send(&rimg);
    let cmsg = c_last_msg();
    let (rsv, rpayload) = match adt_jsonpath::path::jsonpath_send(m, &rimg) {
        Ok(b) => (Verdict::Ok, b.data().to_vec()),
        Err(e) => (Verdict::Hard(e.sqlstate().0), Vec::new()),
    };
    if depth_carved(rsv) {
        return; // stack-depth carve, see depth_carved
    }
    assert!(
        csv == rsv,
        "jsonpath_send VERDICT DIVERGENCE wire={wire:02x?}: C={csv:?} ({cmsg:?}) Rust={rsv:?}"
    );
    assert!(
        cpayload == rpayload,
        "jsonpath_send PAYLOAD DIVERGENCE wire={wire:02x?}: C={cpayload:02x?} Rust={rpayload:02x?}"
    );

    // fc-wrapper plane for jsonpath_send.
    let img = mcx::slice_in(m, &rimg).expect("image copy at fuzz sizes");
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let (r, isnull) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_send, m, [din]);
    let wv = fc_verdict(&r, isnull, None);
    if depth_carved(wv) {
        return; // stack-depth carve (boundary frames differ), see depth_carved
    }
    assert!(
        wv == rsv,
        "fc_jsonpath_send vs core VERDICT DIVERGENCE wire={wire:02x?}: wrapper={wv:?} core={rsv:?}"
    );
    if let Ok(d) = &r {
        assert!(
            datum_bytea_payload(*d) == rpayload.as_slice(),
            "fc_jsonpath_send vs core PAYLOAD DIVERGENCE wire={wire:02x?}"
        );
    }

    // The canonical text is also reachable from a recv'd image: exercise the
    // out plane on it too (same image, second consumer).
    out_planes(m, &rimg, wire);

    // SHORT-VARLENA PLANE: arg_jsonpath (builtins.rs) mirrors
    // PG_GETARG_JSONPATH_P's expansion of a 1-byte-header short varlena into
    // a 4-aligned 4B-header image. Inside the harness every image we build is
    // 4B-headed, so exercise the expansion deliberately: re-frame the agreed
    // image's payload as a short varlena (when it fits) and require the fc
    // wrappers to produce identical out/send results through both framings.
    let payload = &rimg[4..];
    if payload.len() + 1 <= 0x7F {
        let mut short = Vec::with_capacity(payload.len() + 1);
        short.push((((payload.len() + 1) as u8) << 1) | 1);
        short.extend_from_slice(payload);
        let simg = mcx::slice_in(m, &short).expect("short image copy");
        let din_s = NullableDatum::value(Datum::from_usize(simg.as_ptr() as usize));
        let din_l = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
        let (rs, _) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_out, m, [din_s]);
        let (rl, _) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_out, m, [din_l]);
        match (&rs, &rl) {
            (Ok(ds), Ok(dl)) => assert!(
                datum_cstring(*ds) == datum_cstring(*dl),
                "fc_jsonpath_out SHORT-VS-LONG VARLENA DIVERGENCE wire={wire:02x?}"
            ),
            (a, b) => assert!(
                a.is_err() == b.is_err(),
                "fc_jsonpath_out SHORT-VS-LONG VERDICT DIVERGENCE wire={wire:02x?}"
            ),
        }
        let (ss, _) = fc_call::<1>(adt_jsonpath::builtins::fc_jsonpath_send, m, [din_s]);
        if let (Ok(ds), Verdict::Ok) = (&ss, rsv) {
            assert!(
                datum_bytea_payload(*ds) == rpayload.as_slice(),
                "fc_jsonpath_send SHORT-VARLENA PAYLOAD DIVERGENCE wire={wire:02x?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 2: jspIsMutable (mutability.rs)
// ---------------------------------------------------------------------------

/// PASSING-variables model: a fixed name × type table. The type oids are the
/// ones jspIsMutableWalker actually branches on (DATE/TIME/TIMESTAMP ->
/// NonZoned, TIMETZ/TIMESTAMPTZ -> Zoned, anything else -> NonDateTime), so
/// the selector reaches every DtStatus transition. Names are short and share
/// prefixes on purpose: the C matcher is `strncmp(varname, name, len)`, a
/// prefix match, and the Rust port mirrors it — prefix-colliding names are
/// exactly the witness pairs that plane needs.
const VAR_TABLE: &[(&[u8], u32)] = &[
    (b"a", 25),         // TEXTOID -> NonDateTime
    (b"ab", 1082),      // DATEOID -> NonZoned
    (b"d", 1083),       // TIMEOID -> NonZoned
    (b"dz", 1266),      // TIMETZOID -> Zoned
    (b"ts", 1114),      // TIMESTAMPOID -> NonZoned
    (b"tsz", 1184),     // TIMESTAMPTZOID -> Zoned
    (b"zip", 3802),     // JSONBOID -> NonDateTime
    (b"x", 23),         // INT4OID -> NonDateTime
];

fn mutability_diff(payload: &[u8]) {
    let Some((&varsel, text)) = payload.split_first() else {
        return;
    };
    if !in_domain(text) {
        return;
    }

    // Both sides parse in hard mode; the verdict plane is arm-0 logic reused.
    let (cv, cimg) = c_in(text, false);
    let cmsg = c_last_msg();
    let cx = mcx::MemoryContext::new("jsonpath_fuzz_mut");
    let m = cx.mcx();
    let (rv, rimg) = rust_in(m, text, false);
    if depth_carved(rv) {
        return; // stack-depth carve, see depth_carved
    }
    assert!(
        cv == rv,
        "jsonpath_in (mut arm) VERDICT DIVERGENCE input={:?}: C={cv:?} ({cmsg:?}) Rust={rv:?}",
        String::from_utf8_lossy(text)
    );
    assert!(
        cimg == rimg,
        "jsonpath_in (mut arm) IMAGE DIVERGENCE input={:?}",
        String::from_utf8_lossy(text)
    );
    if cv != Verdict::Ok {
        return;
    }

    // Selector -> 0..=3 vars, drawn from VAR_TABLE by the selector's bits
    // (low 3 bits = count-ish + start offset, so both empty and multi-var
    // shapes are reachable).
    let nvars = (varsel & 0x03) as usize;
    let start = ((varsel >> 2) as usize) % VAR_TABLE.len();
    let vars: Vec<(&[u8], u32)> = (0..nvars)
        .map(|i| VAR_TABLE[(start + i) % VAR_TABLE.len()])
        .collect();

    let (cmv, cmut) = c_is_mutable(&rimg, &vars);
    let cmsg = c_last_msg();
    let (rmv, rmut) = match adt_jsonpath::mutability::jsp_is_mutable(&rimg, &vars) {
        Ok(b) => (Verdict::Ok, b),
        Err(e) => (Verdict::Hard(e.sqlstate().0), false),
    };
    if depth_carved(rmv) {
        return; // stack-depth carve, see depth_carved
    }
    assert!(
        cmv == rmv,
        "jspIsMutable VERDICT DIVERGENCE input={:?} vars={vars:?}: C={cmv:?} ({cmsg:?}) Rust={rmv:?}",
        String::from_utf8_lossy(text)
    );
    assert!(
        cmut == rmut,
        "jspIsMutable VALUE DIVERGENCE input={:?} vars={vars:?}: C={cmut} Rust={rmut}",
        String::from_utf8_lossy(text)
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn arm0(text: &str) {
        jsonpath_diff(&[b'\x00', 0x00].iter().copied().chain(text.bytes()).collect::<Vec<_>>());
    }

    fn arm0_soft(text: &str) {
        jsonpath_diff(&[b'\x00', 0x01].iter().copied().chain(text.bytes()).collect::<Vec<_>>());
    }

    fn arm2(varsel: u8, text: &str) {
        jsonpath_diff(&[2u8, varsel].iter().copied().chain(text.bytes()).collect::<Vec<_>>());
    }

    /// TIMING ATTRIBUTION for the fleet slow-units (run manually):
    ///   cargo test --release --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    ///     timing_slow_unit -- --ignored --nocapture
    /// Times C-oracle jsonpath_in and shipped-Rust jsonpath_in SEPARATELY on
    /// the exact source text of fleet slow-unit
    /// 899856ad3a5f72f09a52598b9bc434076004cd93 (campaign
    /// pgrust-fuzz-campaign-1785518461-61c1-18958, 52.5 s/exec instrumented).
    #[test]
    #[ignore]
    fn timing_slow_unit_attribution() {
        let _serial = crate::c_oracle_serial();
        let text: &[u8] =
            include_bytes!("../../testdata/jsonpath-slow/slow-unit-899856ad-text.bin");
        setup();

        let t0 = std::time::Instant::now();
        let (cv, _) = c_in(text, false);
        let c_dur = t0.elapsed();

        let cx = mcx::MemoryContext::new("jsonpath_timing");
        let m = cx.mcx();
        let t1 = std::time::Instant::now();
        let (rv, _) = rust_in(m, text, false);
        let r_dur = t1.elapsed();

        eprintln!("C    jsonpath_in: {c_dur:?} verdict={cv:?}");
        eprintln!("Rust jsonpath_in: {r_dur:?} verdict={rv:?}");
    }

    /// SCALING LAW for like_regex compile cost (run manually):
    ///   cargo test --release --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    ///     timing_scaling_family -- --ignored --nocapture
    /// For each synthetic pattern family, prints a (N, C seconds, Rust
    /// seconds) table for jsonpath_in over
    ///   $ ? (@ like_regex "(<unit repeated N times>)+")
    /// `artifact-unit` is the repeated unit minimized out of fleet slow-unit
    /// 899856ad3a5f72f09a52598b9bc434076004cd93; the other families are the
    /// same tokens with pieces removed (they do NOT blow up — the blowup needs
    /// the whole unit). The N list can be overridden with $JP_NS. The exact
    /// same family is run against real PostgreSQL 18.3 by
    /// fuzz/jsonpath_parse_scaling.sh, so the three engines are comparable.
    #[test]
    #[ignore]
    fn timing_scaling_family() {
        let _serial = crate::c_oracle_serial();
        setup();
        let ns: Vec<usize> = match std::env::var("JP_NS") {
            Ok(s) => s.split_whitespace().map(|t| t.parse().unwrap()).collect(),
            Err(_) => vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 1024],
        };
        // (name, unit) — unit is SOURCE text (jsonpath string escapes apply).
        let families: &[(&str, &str)] = &[
            ("artifact-unit", r"^^^^|\\\\\?\^^^\\Y||pawt@r"),
            ("unit-no-backslash", "^^^^|Y||pawt@r"),
            ("bars", "a|b|ab"),
            ("carets", "^"),
        ];
        for (name, unit) in families {
            eprintln!("family {name}: unit={unit:?}");
            for &n in &ns {
                let pat = unit.repeat(n);
                let text = format!("$ ? (@ like_regex \"({pat})+\")");
                if text.len() > 65536 {
                    break;
                }
                let t0 = std::time::Instant::now();
                let (cv, _) = c_in(text.as_bytes(), false);
                let c_dur = t0.elapsed();
                let cx = mcx::MemoryContext::new("jsonpath_scaling");
                let m = cx.mcx();
                let t1 = std::time::Instant::now();
                let (rv, _) = rust_in(m, text.as_bytes(), false);
                let r_dur = t1.elapsed();
                eprintln!(
                    "  N={n:4} len={:5}  C={:>12.6}s ({cv:?})  Rust={:>12.6}s ({rv:?})  ratio={:.2}",
                    text.len(),
                    c_dur.as_secs_f64(),
                    r_dur.as_secs_f64(),
                    r_dur.as_secs_f64() / c_dur.as_secs_f64().max(1e-9),
                );
            }
        }
    }

    /// GRAMMAR-ONLY scaling (no like_regex, so zero regex compilation): the
    /// hypothesis that gram.rs's `Ok(None)` returns constitute BACKTRACKING
    /// with re-parse (and therefore exponential blowup on ambiguous shapes)
    /// predicts super-linear growth here. Run manually:
    ///   cargo test --release --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    ///     timing_grammar_only -- --ignored --nocapture
    /// (Measured: linear on every shape, C-ratio ~1-2x. `Ok(None)` in gram.rs
    /// is a TERMINAL failure that propagates to parsejsonpath — no alternative
    /// is ever retried at the same input position, so the parser is strict LL(1)
    /// and linear. See fuzz/FINDING-jsonpath-parse-complexity.md.)
    #[test]
    #[ignore]
    fn timing_grammar_only() {
        let _serial = crate::c_oracle_serial();
        setup();
        let ns: Vec<usize> = match std::env::var("JP_NS") {
            Ok(s) => s.split_whitespace().map(|t| t.parse().unwrap()).collect(),
            Err(_) => vec![32, 64, 128, 256, 512, 1024, 2048],
        };
        // Shapes chosen for prefix ambiguity / alternation pressure in the
        // grammar itself: nested parens (accessor-vs-predicate ambiguity),
        // repeated filters, unary chains, ambiguous accessor/method keywords,
        // and a prefix that only fails at the very last token.
        let shapes: &[(&str, fn(usize) -> String)] = &[
            ("nested-paren", |n| format!("{}$.a{}", "(".repeat(n), ")".repeat(n))),
            ("filter-chain", |n| format!("$ {}", "? (@.a > 1) ".repeat(n))),
            ("unary-chain", |n| format!("${}1", "-+".repeat(n))),
            ("method-keywords", |n| format!("${}", ".time".repeat(n))),
            ("index-list", |n| format!("$[{}0]", "0,".repeat(n))),
            ("or-chain", |n| format!("$ ? ({})", vec!["@.a == 1"; n].join(" || "))),
            // fails on the final token: worst case for any retry-on-failure
            ("late-failure", |n| format!("{}$.a{}", "(".repeat(n), ")".repeat(n - 1))),
        ];
        for (name, gen) in shapes {
            eprintln!("shape {name}");
            for &n in &ns {
                let text = gen(n);
                if text.len() > 200_000 {
                    break;
                }
                let t0 = std::time::Instant::now();
                let (cv, _) = c_in(text.as_bytes(), false);
                let c_dur = t0.elapsed();
                let cx = mcx::MemoryContext::new("jsonpath_grammar_scaling");
                let m = cx.mcx();
                let t1 = std::time::Instant::now();
                let (rv, _) = rust_in(m, text.as_bytes(), false);
                let r_dur = t1.elapsed();
                eprintln!(
                    "  N={n:5} len={:7}  C={:>12.6}s ({cv:?})  Rust={:>12.6}s ({rv:?})  ratio={:.2}",
                    text.len(),
                    c_dur.as_secs_f64(),
                    r_dur.as_secs_f64(),
                    r_dur.as_secs_f64() / c_dur.as_secs_f64().max(1e-9),
                );
            }
        }
    }

    /// Ad-hoc timing driver (run manually): times C and Rust jsonpath_in on
    /// each FILE (raw source text) listed in $JP_TIME_FILES (colon-separated).
    ///   JP_TIME_FILES=/path/a:/path/b cargo test --release \
    ///     --manifest-path fuzz/Cargo.toml -p decoder_fuzz timing_files -- \
    ///     --ignored --nocapture
    #[test]
    #[ignore]
    fn timing_files() {
        let _serial = crate::c_oracle_serial();
        let Ok(files) = std::env::var("JP_TIME_FILES") else {
            eprintln!("JP_TIME_FILES not set; skipping");
            return;
        };
        setup();
        for f in files.split(':') {
            let text = std::fs::read(f).expect("readable input file");
            let t0 = std::time::Instant::now();
            let (cv, _) = c_in(&text, false);
            let c_dur = t0.elapsed();
            let cx = mcx::MemoryContext::new("jsonpath_timing_files");
            let m = cx.mcx();
            let t1 = std::time::Instant::now();
            let (rv, _) = rust_in(m, &text, false);
            let r_dur = t1.elapsed();
            eprintln!(
                "{f}: len={:5}  C={:>12.6}s ({cv:?})  Rust={:>12.6}s ({rv:?})  ratio={:.2}",
                text.len(),
                c_dur.as_secs_f64(),
                r_dur.as_secs_f64(),
                r_dur.as_secs_f64() / c_dur.as_secs_f64().max(1e-9),
            );
        }
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/jsonpath_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/jsonpath_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                jsonpath_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Every regress round-trip vector through the full differential arm 0
    /// (in + out, core + wrapper + C oracle), hard and soft modes.
    #[test]
    fn regress_ok_vectors_both_sides() {
        let _serial = crate::c_oracle_serial();
        for (input, _) in JSONPATH_OK_VECTORS {
            arm0(input);
            arm0_soft(input);
        }
    }

    /// Error vectors: verdict + sqlstate parity in both modes.
    #[test]
    fn regress_err_vectors_both_sides() {
        let _serial = crate::c_oracle_serial();
        for input in JSONPATH_ERR_INPUTS {
            arm0(input);
            arm0_soft(input);
        }
    }

    /// Scanner/unicode/numeric edges called out in the lane charter.
    #[test]
    fn edge_shapes() {
        let _serial = crate::c_oracle_serial();
        for s in [
            "$.a[",
            "\"\\u00e9\"",
            "\"é\"",
            "\"\\u{1F600}\"",
            "\"\\ud83d\\ude00\"",   // surrogate pair
            "\"\\ud83d\"",          // lone high surrogate
            "\"\\uZZZZ\"",
            "1e1000000",
            "1e-1000000",
            "0x7FFFFFFFFFFFFFFF",
            "0b1111111111111111111111111111111111111111111111111111111111111111",
            "$ ? (@ like_regex \"^a(b|c)+d$\" flag \"ix\")",
            "$ ? (@ like_regex \"[[:alpha:]]+\" flag \"i\")",
            "$ ? (@ like_regex \"(bad\")",
            "$ ? (@ like_regex \"a\" flag \"z\")",
            "$.datetime(\"HH24:MI TZH\")",
            "$.datetime(\"YYYY-MM-DD\")",
            "$.timestamp_tz(6)",
            "$.decimal(1000,-1000)",
            "$.decimal(100000,0)",
            "$.a.**{4294967295}.b",
            "$[last to last]",
        ] {
            arm0(s);
            arm0_soft(s);
        }
        // Nesting up to the documented text cap (recursion depth plane).
        for depth in [1usize, 8, 32, 64] {
            let s = format!("{}$.a{}", "(".repeat(depth), ")".repeat(depth));
            if s.len() <= MAX_TEXT {
                arm0(&s);
            }
            let filt = format!("$ {}", "? (@.a > 1) ".repeat(depth));
            if filt.len() <= MAX_TEXT {
                arm0(&filt);
            }
        }
    }

    /// Arm 1: wire framing — good version byte, bad version, truncated,
    /// empty, and a full send/recv round trip for every ok vector.
    #[test]
    fn recv_send_shapes() {
        let _serial = crate::c_oracle_serial();
        for (input, _) in JSONPATH_OK_VECTORS.iter().take(60) {
            let mut wire = vec![1u8; 1];
            wire.extend_from_slice(input.as_bytes());
            let mut data = vec![1u8];
            data.extend_from_slice(&wire);
            jsonpath_diff(&data);
        }
        // framing edges
        for wire in [
            vec![],
            vec![1u8],
            vec![0u8, b'$'],
            vec![2u8, b'$'],
            vec![255u8, b'$'],
            vec![1u8, b'$', b'.', b'a'],
            vec![1u8, 0x80, 0x80], // invalid UTF-8 body
        ] {
            let mut data = vec![1u8];
            data.extend_from_slice(&wire);
            jsonpath_diff(&data);
        }
    }

    /// Arm 2: mutability over every var-model selector, including the
    /// datetime shapes and the prefix-colliding variable names.
    #[test]
    fn mutability_shapes() {
        let _serial = crate::c_oracle_serial();
        let paths = [
            "$",
            "$.a",
            "$.datetime()",
            "$.datetime(\"HH24 TZH\")",
            "$.datetime(\"YYYY-MM-DD\")",
            "$ ? (@.a == $d)",
            "$ ? ($d == $dz)",
            "$ ? ($ts == $tsz)",
            "$ ? ($d == $ts)",
            "$ ? ($a == $d)",
            "$ ? (@.datetime() == $d)",
            "$.time()",
            "$.timestamp_tz()",
            "strict $[*] ? ($d < $dz)",
            "$.a[$d ? (@ > 0)]",
            "$ ? (exists (@.x ? (@ == $dz)))",
            "$ ? (@ like_regex \"a\")",
            "$.**{2}.a",
            "$.**.a",
        ];
        for p in paths {
            for varsel in 0u8..32 {
                arm2(varsel, p);
            }
        }
    }

    // Vector tables mirrored from the crate's own regress-derived vectors
    // (crates/backend/utils/adt/jsonpath/src/vectors.rs is #[cfg(test)]-only,
    // so the differential harness carries its own copy of the inputs; the
    // expected forms live on the C side here).
    static JSONPATH_OK_VECTORS: &[(&str, &str)] = &[
        ("$", ""), ("strict $", ""), ("lax $", ""), ("$.a", ""), ("$.a.v", ""),
        ("$.a.*", ""), ("$.*[*]", ""), ("$.a[*]", ""), ("$.a[*][*]", ""),
        ("$[*]", ""), ("$[0]", ""), ("$[*][0]", ""), ("$[*].a", ""),
        ("$[*][0].a.b", ""), ("$.a.**.b", ""), ("$.a.**{2}.b", ""),
        ("$.a.**{2 to 2}.b", ""), ("$.a.**{2 to 5}.b", ""), ("$.a.**{0 to 5}.b", ""),
        ("$.a.**{5 to last}.b", ""), ("$.a.**{last}.b", ""), ("$.a.**{last to 5}.b", ""),
        ("$+1", ""), ("$-1", ""), ("$--+1", ""), ("$.a/+-1", ""),
        ("1 * 2 + 4 % -3 != false", ""),
        ("\"\\b\\f\\r\\n\\t\\v\\\"\\'\\\\\"", ""),
        ("\"\\x50\\u0067\\u{53}\\u{051}\\u{00004C}\"", ""),
        ("$.foo\\x50\\u0067\\u{53}\\u{051}\\u{00004C}\\t\\\"bar", ""),
        ("$.g ? ($.a == 1)", ""), ("$.g ? (@ == 1)", ""), ("$.g ? (@.a == 1)", ""),
        ("$.g ? (@.a == 1 || @.a == 4)", ""), ("$.g ? (@.a == 1 && @.a == 4)", ""),
        ("$.g ? (@.a == 1 || @.a == 4 && @.b == 7)", ""),
        ("$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)", ""),
        ("$.g ? (@.a == 1 || !(@.x >= 123 || @.a == 4) && @.b == 7)", ""),
        ("$.g ? (@.x >= @[*]?(@.a > \"abc\"))", ""),
        ("$.g ? ((@.x >= 123 || @.a == 4) is unknown)", ""),
        ("$.g ? (exists (@.x))", ""), ("$.g ? (exists (@.x ? (@ == 14)))", ""),
        ("$.g ? ((@.x >= 123 || @.a == 4) && exists (@.x ? (@ == 14)))", ""),
        ("$.g ? (+@.x >= +-(+@.a + 2))", ""),
        ("$a", ""), ("$a.b", ""), ("$a[*]", ""), ("$.g ? (@.zip == $zip)", ""),
        ("$.a[1,2, 3 to 16]", ""), ("$.a[$a + 1, ($b[*]) to -($[0] * 2)]", ""),
        ("$.a[$.a.size() - 3]", ""), ("\"last\"", ""), ("$.last", ""), ("$[last]", ""),
        ("$[$[0] ? (last > 0)]", ""), ("null.type()", ""), ("(1).type()", ""),
        ("1.2.type()", ""), ("\"aaa\".type()", ""), ("true.type()", ""),
        ("$.double().floor().ceiling().abs()", ""), ("$.keyvalue().key", ""),
        ("$.datetime()", ""), ("$.datetime(\"datetime template\")", ""),
        ("$.bigint().integer().number().decimal()", ""), ("$.boolean()", ""),
        ("$.date()", ""), ("$.decimal(4,2)", ""), ("$.string()", ""), ("$.time()", ""),
        ("$.time(6)", ""), ("$.time_tz()", ""), ("$.time_tz(4)", ""),
        ("$.timestamp()", ""), ("$.timestamp(2)", ""), ("$.timestamp_tz()", ""),
        ("$.timestamp_tz(0)", ""),
        ("$ ? (@ starts with \"abc\")", ""), ("$ ? (@ starts with $var)", ""),
        ("$ ? (@ like_regex \"pattern\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"i\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"is\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"isim\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"q\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"iq\")", ""),
        ("$ ? (@ like_regex \"pattern\" flag \"smixq\")", ""),
        ("$ < 1", ""), ("($ < 1) || $.a.b <= $x", ""), ("($).a.b", ""),
        ("($.a.b).c.d", ""), ("($.a.b + -$.x.y).c.d", ""), ("(-+$.a.b).c.d", ""),
        ("1 + ($.a.b + 2).c.d", ""), ("1 + ($.a.b > 2).c.d", ""), ("($)", ""),
        ("(($))", ""),
        ("((($ + 1)).a + ((2)).b ? ((((@ > 1)) || (exists(@.c)))))", ""),
        ("$ ? (@.a < 1)", ""), ("$ ? (@.a < -1)", ""), ("$ ? (@.a < +1)", ""),
        ("$ ? (@.a < .1)", ""), ("$ ? (@.a < -.1)", ""), ("$ ? (@.a < +.1)", ""),
        ("$ ? (@.a < 0.1)", ""), ("$ ? (@.a < 10.1)", ""), ("$ ? (@.a < 1e1)", ""),
        ("$ ? (@.a < -1e1)", ""), ("$ ? (@.a < .1e1)", ""), ("$ ? (@.a < 0.1e1)", ""),
        ("$ ? (@.a < 10.1e1)", ""), ("$ ? (@.a < 1e-1)", ""), ("$ ? (@.a < .1e-1)", ""),
        ("$ ? (@.a < 0.1e-1)", ""), ("$ ? (@.a < 10.1e-1)", ""),
        ("$ ? (@.a < 1e+1)", ""), ("$ ? (@.a < .1e+1)", ""), ("$ ? (@.a < 0.1e+1)", ""),
        ("$ ? (@.a < 10.1e+1)", ""),
        ("0", ""), ("0.0", ""), ("0.000", ""), ("0.000e1", ""), ("0.000e2", ""),
        ("0.000e3", ""), ("0.0010", ""), ("0.0010e-1", ""), ("0.0010e+1", ""),
        ("0.0010e+2", ""), (".001", ""), (".001e1", ""), ("1.", ""), ("1.e1", ""),
        ("1.2.e", ""), ("(1.2).e", ""), ("1e3", ""), ("1.e3", ""), ("1.e3.e", ""),
        ("1.e3.e4", ""), ("1.2e3", ""), ("1.2.e3", ""), ("(1.2).e3", ""), ("1..e", ""),
        ("1..e3", ""), ("(1.).e", ""), ("(1.).e3", ""), ("1?(2>3)", ""),
        ("0b100101", ""), ("0o273", ""), ("0x42F", ""), ("1_000_000", ""),
        ("1_2_3", ""), ("0x1EEE_FFFF", ""), ("0o2_73", ""), ("0b10_0101", ""),
        ("1_000.000_005", ""), ("1_000.", ""), (".000_005", ""), ("1_000.5e0_1", ""),
    ];

    static JSONPATH_ERR_INPUTS: &[&str] = &[
        "", "last", "$ ? (last > 0)", "1.type()",
        "$ ? (@ like_regex \"(invalid pattern\")",
        "$ ? (@ like_regex \"pattern\" flag \"xsms\")",
        "$ ? (@ like_regex \"pattern\" flag \"a\")",
        "@ + 1", "00", "0755", "1a", "1e", "1.e", "1.2a", "1.2e", "1.2e3a",
        "0b", "1b", "0b0x", "0o", "1o", "0o0x", "0x", "1x", "0x0y",
        "_100", "100_", "100__000", "_1_000.5", "1_000_.5", "1_000._5",
        "1_000.5_", "1_000.5e_1", "0b_10_0101", "0o_273", "0x_42F",
        "1 && 2", "$ ? (@)", "$?(1)", "exists(1 == 2)", "!(1)",
        "(1) is unknown", "$[1 == 1]", "(1 == 1) + 2", "exists($.a) + 1",
        "-(1 == 1)", "1 == !(true)", "\"abc\\", "\"a\\\nb\"", "1 2 \"x",
    ];
}
