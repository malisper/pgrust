//! vlmisc_diff: differential fuzz driver — shipped Rust `varlena` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_vlmisc_io.c). Crate under test: crates/backend/utils/adt/varlena.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! ENCODING FENCE (decision of record, mirrored in the C oracle header):
//! the database encoding is pinned to UTF8 on both sides — setup() installs
//! the real mbutils seams and SetDatabaseEncoding(PG_UTF8) per thread; the C
//! oracle resolves the same fence at compile time. unistr, unicode_assigned
//! and the normalization family are UTF8-dependent in real PG. The C locale
//! is never changed (isxdigit/isupper see LC_CTYPE=C on both sides).
//!
//! Input layout: [selector][payload]; selector % 15 picks the arm:
//!   0 to_hex32  (oid 2089) — payload: ≤4 LE bytes zero-padded to an i32.
//!   1 to_hex64  (oid 2090) — payload: ≤8 LE bytes zero-padded to an i64.
//!   2 to_bin32  (oid 6330) — as arm 0.
//!   3 to_bin64  (oid 6331) — as arm 1.
//!   4 to_oct32  (oid 6332) — as arm 0.
//!   5 to_oct64  (oid 6333) — as arm 1.
//!   (to_hex32/64 and to_bin/oct32/64 are already Kani-proved full-domain;
//!   these arms cheaply add the line-coverage union + the fc plane.)
//!   6 unistr  (oid 6198) — payload: raw text bytes (embedded NULs are data
//!     on both sides; length-bounded scans).
//!   7 unicode_version  (oid 4549) — no payload; constant "16.0" compared.
//!   8 unicode_assigned  (oid 6105) — payload: text bytes (gates below).
//!   9 unicode_normalize_func  (oid 4350) — payload: [form selector][text].
//!   10 unicode_is_normalized  (oid 4351) — payload: [form selector][text].
//!   11 varstr_levenshtein  (non-SQL helper, levenshtein.c plain
//!      instantiation) — payload: [c0][c1][split u16 LE][string bytes];
//!      costs decoded from a band table (small values + extremes).
//!   12 varstr_levenshtein_less_equal  (LEVENSHTEIN_LESS_EQUAL
//!      instantiation) — payload: [c0][c1][c2 max_d band][split][bytes].
//!   13 SplitIdentifierString  (non-SQL helper) — payload: [sep][string].
//!   14 SplitGUCList  (non-SQL helper) — payload: [sep][string].
//!
//! FC-WRAPPER PLANE: arms 0-10 additionally route the (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and assert wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). Arms 11-14 have NO fc plane:
//! varstr_levenshtein* live in contrib/fuzzystrmatch at the SQL layer and
//! SplitIdentifierString/SplitGUCList are backend-internal helpers — none
//! has an fc_* wrapper in this crate's builtins.rs.
//!
//! Domain fences (executable gates below, not comment-only carves):
//!  - NUL fence (arms 8/9/10/13/14): PostgreSQL text/cstring values can
//!    never contain NUL bytes (textin forbids them), and the C machinery is
//!    NUL-stopped at the pg_mbstrlen_with_len / cstring layer while the
//!    shipped Rust is slice-based; embedded NULs are out of both contracts.
//!    unistr (arm 6) and levenshtein (11/12) take NULs — both sides are
//!    length-bounded there in the same way.
//!  - utf8-walk fence (arm 8 only): unicode_assigned inputs whose trailing
//!    multibyte character overruns the slice are skipped. C raises 22021
//!    via its pg_mbstrlen_with_len pre-scan; the shipped Rust
//!    unicode::unicode_assigned has NO pre-scan and would slice-index-panic.
//!    Out of contract for a validated-UTF8 database (stored text can't hold
//!    truncated sequences) — recorded here and in the lane report as a
//!    robustness FINDING (panic vs 22003-class error on hostile input).
//!  - (FIXED 2026-07-31, same lane) fuzz-found pgrust-bug: the shipped
//!    split_identifier_string / split_guc_list panicked on a trailing
//!    separator ("a,") where C returns false — NUL-sentinel state dropped
//!    in the port; fixed at the loop top in varlena/src/lib.rs with crate
//!    regression tests. The dangling-separator shape is fuzzer-owned again
//!    (the former ends_with_dangling_sep fence is deleted).
//!  - separator fence (arms 13/14): the separator must be ASCII non-NUL.
//!    A NUL separator makes the C code walk past the terminator (`*nextp
//!    == separator` is tested before `*nextp == '\0'` — UB); a high-bit
//!    separator can split a multibyte character, where C returns the raw
//!    byte pieces while the shipped Rust's String conversion
//!    (from_utf8_lossy) replaces them with U+FFFD. Neither is a reachable
//!    configuration: every C caller passes ASCII punctuation (',' '.').
//!  - split inputs (13/14) must be valid UTF-8: the shipped Rust API takes
//!    &str (C takes bytes; the UTF8 database fence makes them equal).
//!  - text payloads capped at 2 KiB (MAX_TEXT) so libFuzzer's budget goes
//!    to shapes, not megabyte memcpys; levenshtein strings capped at 700
//!    bytes — comfortably both sides of MAX_LEVENSHTEIN_STRLEN = 255.
//!
//!  - U+11A7 carve (arms 9/10) — ORACLE-VERSION divergence of record: the
//!    pinned 18.3 C recompose_code treats TBASE (U+11A7, the T-filler) as a
//!    composable T (tindex 0), silently ABSORBING it into a preceding LV
//!    syllable (NFC(<1100,1161,11A7>) = <AC00>); the shipped unicode_norm
//!    crate deliberately implements the later upstream fix (its test cites
//!    C 273fe94: "U+11A7 must survive NFC unrecomposed", giving
//!    <AC00,11A7>). Under the campaign's 18.3 oracle pin the shapes cannot
//!    be compared; texts containing U+11A7 (UTF-8 E1 86 A7) are gated out
//!    of the normalization arms and the deviation is recorded in the lane
//!    report for a conformance ruling.
//! SKIPPED rows, with reasons:
//!  - icu_unicode_version (oid 9147) / fc_icu_unicode_version:
//!    excluded(state) — the result depends on whether libicu is loadable in
//!    the process (pg_locale seam / USE_ICU build state), not on the input.
//!  - bttextsortsupport & friends: SortSupport plumbing, no pure entry.

use std::ffi::CString;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR,
};
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};
use varlena::builtins as vb;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // Oracle entries (csrc/pg_vlmisc_io.c section 10). All return 0 on ok /
    // the errcode class on error (1=22023, 2=42601, 3=22021, 9=internal).
    fn pg_diff_to_hex32(arg: i32, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_to_hex64(arg: i64, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_to_bin32(arg: i32, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_to_bin64(arg: i64, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_to_oct32(arg: i32, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_to_oct64(arg: i64, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_unistr(inp: *const u8, len: i32, out: *mut u8, outcap: i32, outlen: *mut i32)
        -> i32;
    fn pg_diff_unicode_version(out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_unicode_assigned(inp: *const u8, len: i32, result: *mut i32) -> i32;
    fn pg_diff_unicode_normalize(
        inp: *const u8,
        len: i32,
        formstr: *const core::ffi::c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_unicode_is_normalized(
        inp: *const u8,
        len: i32,
        formstr: *const core::ffi::c_char,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_varstr_levenshtein(
        s: *const u8,
        slen: i32,
        t: *const u8,
        tlen: i32,
        ins_c: i32,
        del_c: i32,
        sub_c: i32,
        trusted: i32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_varstr_levenshtein_less_equal(
        s: *const u8,
        slen: i32,
        t: *const u8,
        tlen: i32,
        ins_c: i32,
        del_c: i32,
        sub_c: i32,
        max_d: i32,
        trusted: i32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_split_identifier_string(
        raw: *const u8,
        rawlen: i32,
        separator: core::ffi::c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_split_guc_list(
        raw: *const u8,
        rawlen: i32,
        separator: core::ffi::c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
}

/// Oracle error classes (csrc/pg_vlmisc_io.c header).
const C_ERR_INVALID_PARAMETER_VALUE: i32 = 1; // 22023
const C_ERR_SYNTAX_ERROR: i32 = 2; // 42601
const C_ERR_NOT_IN_REPERTOIRE: i32 = 3; // 22021

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        C_ERR_INVALID_PARAMETER_VALUE
    } else if e.sqlstate == ERRCODE_SYNTAX_ERROR {
        C_ERR_SYNTAX_ERROR
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_NOT_IN_REPERTOIRE
    } else {
        99
    }
}

/// Cap text payloads (module doc: budget goes to shapes).
const MAX_TEXT: usize = 2048;
/// Levenshtein string cap — both sides of MAX_LEVENSHTEIN_STRLEN = 255.
const MAX_LEV: usize = 700;

/// Pin the thread's database encoding to UTF8 and install the production
/// mbutils seams (the levenshtein core reaches pg_mbstrlen_with_len /
/// pg_mblen_range through mbutils_seams). Same posture as name_diff::setup.
fn setup() {
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

/// text/varlena arg construction: inline 4B-uncompressed header + body
/// (the shipped set_varsize_4b_word encoding; body is capped by MAX_TEXT so
/// the length always fits).
fn text_image(body: &[u8]) -> Vec<u8> {
    let len = (body.len() + 4) as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut img = Vec::with_capacity(body.len() + 4);
    img.extend_from_slice(&word.to_ne_bytes());
    img.extend_from_slice(body);
    img
}

/// Varlena result readback (text payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// fc plane for a text-returning wrapper whose C-checked core outcome is
/// `core` (Ok payload bytes or Err class). Wrapper must agree exactly.
fn fc_expect_text(fname: &str, f: PGFunction, m: mcx::Mcx<'_>, args_img: &[&[u8]], core: &Result<&[u8], i32>) {
    let imgs: Vec<Vec<u8>> = args_img.iter().map(|b| text_image(b)).collect();
    let r = match imgs.len() {
        1 => fc_call(f, m, [Datum::from_usize(imgs[0].as_ptr() as usize)]).0,
        2 => {
            fc_call(
                f,
                m,
                [
                    Datum::from_usize(imgs[0].as_ptr() as usize),
                    Datum::from_usize(imgs[1].as_ptr() as usize),
                ],
            )
            .0
        }
        _ => unreachable!("fc_expect_text supports 1-2 text args"),
    };
    match (r, core) {
        (Ok(d), Ok(want)) => assert!(
            read_varlena_data(d) == *want,
            "{fname} fc-wrapper DIVERGENCE: wrapper bytes != core bytes"
        ),
        (Err(e), Err(class)) => assert!(
            rust_err_class(&e) == *class,
            "{fname} fc-wrapper DIVERGENCE: wrapper class {} core class {class}",
            rust_err_class(&e)
        ),
        (Ok(_), Err(class)) => panic!("{fname} fc-wrapper DIVERGENCE: wrapper Ok, core Err({class})"),
        (Err(e), Ok(_)) => panic!(
            "{fname} fc-wrapper DIVERGENCE: wrapper Err({} {}), core Ok",
            rust_err_class(&e),
            e.message
        ),
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn vlmisc_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics) —
    // the fuzz TARGET's own frame stack needs the lock, same driver-entry
    // idiom as every other pub *_diff (task #144 addendum, trgm precedent).
    let _oracle = crate::oracle_serial();

    setup();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 16 {
        15 => qualified_name_list_diff(payload),
        0 => to_hex32_diff(payload),
        1 => to_hex64_diff(payload),
        2 => to_bin32_diff(payload),
        3 => to_bin64_diff(payload),
        4 => to_oct32_diff(payload),
        5 => to_oct64_diff(payload),
        6 => unistr_diff(payload),
        7 => unicode_version_diff(payload),
        8 => unicode_assigned_diff(payload),
        9 => unicode_normalize_func_diff(payload),
        10 => unicode_is_normalized_diff(payload),
        11 => levenshtein_diff(payload),
        12 => levenshtein_less_equal_diff(payload),
        13 => split_identifier_string_diff(payload),
        _ => split_guc_list_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arms 0-5: to_hex/to_bin/to_oct over full-width i32/i64 (total functions).
// ---------------------------------------------------------------------------

fn decode_i32(payload: &[u8]) -> i32 {
    let mut b = [0u8; 4];
    for (i, &x) in payload.iter().take(4).enumerate() {
        b[i] = x;
    }
    i32::from_le_bytes(b)
}

fn decode_i64(payload: &[u8]) -> i64 {
    let mut b = [0u8; 8];
    for (i, &x) in payload.iter().take(8).enumerate() {
        b[i] = x;
    }
    i64::from_le_bytes(b)
}

/// Shared core for the six conversion arms: run the C driver entry, the
/// shipped convert_to_base core, and the fc wrapper; all three must agree.
fn to_base_diff(
    fname: &str,
    value_core: u64,
    base: u64,
    cres: (i32, [u8; 64], i32),
    fc: PGFunction,
    fc_arg: Datum,
) {
    let (cst, cout, clen) = cres;
    let cerr = unsafe { pg_diff_errcode_get() };
    assert!(cst == 0 && cerr == 0, "{fname}: C oracle errored unexpectedly ({cst}/{cerr})");
    let cbytes = &cout[..clen as usize];

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let r = varlena::convert_to_base(cx.mcx(), value_core, base)
        .expect("convert_to_base is infallible");
    assert!(
        r.data() == cbytes,
        "{fname} DIVERGENCE value={value_core:#x}: C={:?} Rust={:?}",
        cbytes,
        r.data()
    );

    // ---- fc-wrapper plane (wrapper == core) ----
    let (d, isnull) = fc_call(fc, cx.mcx(), [fc_arg]);
    let d = d.unwrap_or_else(|e| panic!("{fname} fc wrapper errored: {}", e.message));
    assert!(!isnull, "{fname} fc wrapper returned NULL");
    assert!(
        read_varlena_data(d) == cbytes,
        "{fname} fc-wrapper DIVERGENCE value={value_core:#x}"
    );
}

macro_rules! to_base_arm32 {
    ($fn_name:ident, $c_entry:ident, $fc:path, $base:literal, $label:literal) => {
        fn $fn_name(payload: &[u8]) {
            let v = decode_i32(payload);
            let mut out = [0u8; 64];
            let mut len = 0i32;
            let cst = unsafe { $c_entry(v, out.as_mut_ptr(), 64, &mut len) };
            to_base_diff($label, v as u32 as u64, $base, (cst, out, len), $fc, Datum::from_i32(v));
        }
    };
}
macro_rules! to_base_arm64 {
    ($fn_name:ident, $c_entry:ident, $fc:path, $base:literal, $label:literal) => {
        fn $fn_name(payload: &[u8]) {
            let v = decode_i64(payload);
            let mut out = [0u8; 64];
            let mut len = 0i32;
            let cst = unsafe { $c_entry(v, out.as_mut_ptr(), 64, &mut len) };
            to_base_diff($label, v as u64, $base, (cst, out, len), $fc, Datum::from_i64(v));
        }
    };
}

to_base_arm32!(to_hex32_diff, pg_diff_to_hex32, vb::fc_to_hex32, 16, "to_hex32");
to_base_arm64!(to_hex64_diff, pg_diff_to_hex64, vb::fc_to_hex64, 16, "to_hex64");
to_base_arm32!(to_bin32_diff, pg_diff_to_bin32, vb::fc_to_bin32, 2, "to_bin32");
to_base_arm64!(to_bin64_diff, pg_diff_to_bin64, vb::fc_to_bin64, 2, "to_bin64");
to_base_arm32!(to_oct32_diff, pg_diff_to_oct32, vb::fc_to_oct32, 8, "to_oct32");
to_base_arm64!(to_oct64_diff, pg_diff_to_oct64, vb::fc_to_oct64, 8, "to_oct64");

// ---------------------------------------------------------------------------
// Arm 6: unistr (oid 6198). Embedded NULs are data on both sides.
// ---------------------------------------------------------------------------

fn unistr_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    // Output never exceeds the input length (every escape shrinks).
    let cap = payload.len().max(1);
    let mut cout = vec![0u8; cap];
    let mut clen = 0i32;
    let cst = unsafe {
        pg_diff_unistr(payload.as_ptr(), payload.len() as i32, cout.as_mut_ptr(), cap as i32, &mut clen)
    };
    let cerr = unsafe { pg_diff_errcode_get() };

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let core = match varlena::unistr(cx.mcx(), payload) {
        Ok(v) => {
            assert!(
                cst == 0 && v.data() == &cout[..clen as usize],
                "unistr DIVERGENCE input={payload:?}: C=(st {cst}, err {cerr}, {:?}) Rust=Ok({:?})",
                &cout[..clen.max(0) as usize],
                v.data()
            );
            Ok(v)
        }
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "unistr DIVERGENCE input={payload:?}: C=(st {cst}, err {cerr}) Rust=Err({rerr} {})",
                e.message
            );
            Err(e)
        }
    };

    // ---- fc-wrapper plane (wrapper == core) ----
    let core_ref = match &core {
        Ok(v) => Ok(v.data()),
        Err(e) => Err(rust_err_class(e)),
    };
    fc_expect_text("fc_unistr", vb::fc_unistr, cx.mcx(), &[payload], &core_ref);
}

// ---------------------------------------------------------------------------
// Arm 7: unicode_version (oid 4549). Constant, still compared.
// ---------------------------------------------------------------------------

fn unicode_version_diff(_payload: &[u8]) {
    let mut cout = [0u8; 16];
    let mut clen = 0i32;
    let cst = unsafe { pg_diff_unicode_version(cout.as_mut_ptr(), 16, &mut clen) };
    assert!(cst == 0, "unicode_version C oracle errored");
    let cbytes = &cout[..clen as usize];

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let r = varlena::unicode::unicode_version(cx.mcx()).expect("unicode_version is infallible");
    assert!(
        r.data() == cbytes,
        "unicode_version DIVERGENCE: C={cbytes:?} Rust={:?}",
        r.data()
    );

    let (d, _) = fc_call(vb::fc_unicode_version, cx.mcx(), []);
    let d = d.expect("fc_unicode_version is infallible");
    assert!(
        read_varlena_data(d) == cbytes,
        "fc_unicode_version fc-wrapper DIVERGENCE"
    );
}

// ---------------------------------------------------------------------------
// Arm 8: unicode_assigned (oid 6105). NUL fence + utf8-walk fence (module
// doc: the C pre-scan raises 22021 where the shipped Rust would panic —
// out of contract for validated-UTF8 stored text; recorded as a FINDING).
// ---------------------------------------------------------------------------

/// True iff a pg_utf_mblen walk over `s` lands exactly on the end.
fn utf8_walk_in_bounds(s: &[u8]) -> bool {
    let mut off = 0usize;
    while off < s.len() {
        let l = wchar::pg_utf_mblen(&s[off..]) as usize;
        if off + l > s.len() {
            return false;
        }
        off += l;
    }
    true
}

fn unicode_assigned_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) || !utf8_walk_in_bounds(payload) {
        return;
    }
    let mut cres = 0i32;
    let cst = unsafe { pg_diff_unicode_assigned(payload.as_ptr(), payload.len() as i32, &mut cres) };
    assert!(cst == 0, "unicode_assigned C oracle errored on gated input ({cst})");

    let r = varlena::unicode::unicode_assigned(payload)
        .expect("unicode_assigned is infallible under the UTF8 pin");
    assert!(
        r == (cres != 0),
        "unicode_assigned DIVERGENCE input={payload:?}: C={cres} Rust={r}"
    );

    // ---- fc-wrapper plane (wrapper == core) ----
    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let img = text_image(payload);
    let (d, _) = fc_call(
        vb::fc_unicode_assigned,
        cx.mcx(),
        [Datum::from_usize(img.as_ptr() as usize)],
    );
    let d = d.expect("fc_unicode_assigned infallible on gated input");
    assert!(
        d.as_bool() == r,
        "fc_unicode_assigned fc-wrapper DIVERGENCE input={payload:?}"
    );
}

// ---------------------------------------------------------------------------
// Arms 9/10: unicode_normalize_func / unicode_is_normalized. Payload:
// [form selector][text]; NUL fence on the text (module doc).
// ---------------------------------------------------------------------------

/// U+11A7 carve walk (module doc): true iff the pg_utf_mblen walk decodes
/// any character to TBASE. An overrun tail is NOT gated — both sides error
/// (22021) before normalizing.
fn decodes_tbase(text: &[u8]) -> bool {
    let mut off = 0usize;
    while off < text.len() {
        let l = wchar::pg_utf_mblen(&text[off..]) as usize;
        if off + l > text.len() {
            return false;
        }
        if wchar::utf8_to_unicode(&text[off..]) == 0x11a7 {
            return true;
        }
        off += l;
    }
    false
}

/// Form-name table: the four valid forms in several casings (pg_strcasecmp /
/// eq_ignore_ascii_case are both ASCII-case-blind) plus invalid strings for
/// the 22023 error plane.
const FORMS: [&str; 12] = [
    "NFC", "nfc", "NfC", "NFD", "nfd", "NFKC", "nfkc", "NFKD", "nfkd", "NFKX", "", "nf c",
];

fn norm_payload(payload: &[u8]) -> Option<(&'static str, &[u8])> {
    let (&fsel, text) = payload.split_first()?;
    if text.len() > MAX_TEXT || text.contains(&0) {
        return None;
    }
    // VALID-UTF8 INPUT FENCE (lane p1-lanes, smoke 2026-07-31): normalize
    // compares OUTPUT IMAGES, and on invalid UTF-8 both sides "succeed" over
    // garbage with different decode conventions (C pg_utf_mblen walk vs the
    // shipped decode; witnessed on 0xC0 overlongs — fuzz/artifacts kept as
    // regression seeds). PG normalizes stored text only, which the server
    // validated at ingest; invalid encodings are un-ingestible and not a
    // conformance surface (same fence class as vltext text_starts_with).
    if std::str::from_utf8(text).is_err() {
        return None;
    }
    // U+11A7 carve (module doc): 18.3 C absorbs TBASE into LV syllables;
    // the shipped unicode_norm implements the later upstream fix (273fe94).
    // Decoded-codepoint grain: invalid sequences (e.g. E1 86 E7, or 4-byte
    // overlongs) also decode to 0x11A7 through the shared utf8_to_unicode.
    if decodes_tbase(text) {
        return None;
    }
    Some((FORMS[fsel as usize % FORMS.len()], text))
}

fn unicode_normalize_func_diff(payload: &[u8]) {
    let Some((form, text)) = norm_payload(payload) else {
        return;
    };
    let formc = CString::new(form).unwrap();
    // NFKD expansion bound: <= 18 codepoints per char, <= 4 bytes each.
    let cap = text.len() * 18 * 4 + 16;
    let mut cout = vec![0u8; cap];
    let mut clen = 0i32;
    let cst = unsafe {
        pg_diff_unicode_normalize(
            text.as_ptr(),
            text.len() as i32,
            formc.as_ptr(),
            cout.as_mut_ptr(),
            cap as i32,
            &mut clen,
        )
    };
    let cerr = unsafe { pg_diff_errcode_get() };

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let core = match varlena::unicode::unicode_normalize_func(cx.mcx(), text, form.as_bytes()) {
        Ok(v) => {
            assert!(
                cst == 0 && v.data() == &cout[..clen as usize],
                "unicode_normalize DIVERGENCE form={form} input={text:?}: C=(st {cst}, err {cerr}) Rust=Ok({:?})",
                v.data()
            );
            Ok(v)
        }
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "unicode_normalize DIVERGENCE form={form} input={text:?}: C=(st {cst}, err {cerr}) Rust=Err({rerr} {})",
                e.message
            );
            Err(e)
        }
    };

    // ---- fc-wrapper plane (wrapper == core) ----
    let core_ref = match &core {
        Ok(v) => Ok(v.data()),
        Err(e) => Err(rust_err_class(e)),
    };
    fc_expect_text(
        "fc_unicode_normalize_func",
        vb::fc_unicode_normalize_func,
        cx.mcx(),
        &[text, form.as_bytes()],
        &core_ref,
    );
}

fn unicode_is_normalized_diff(payload: &[u8]) {
    let Some((form, text)) = norm_payload(payload) else {
        return;
    };
    let formc = CString::new(form).unwrap();
    let mut cres = 0i32;
    let cst = unsafe {
        pg_diff_unicode_is_normalized(text.as_ptr(), text.len() as i32, formc.as_ptr(), &mut cres)
    };
    let cerr = unsafe { pg_diff_errcode_get() };

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let core = match varlena::unicode::unicode_is_normalized(cx.mcx(), text, form.as_bytes()) {
        Ok(b) => {
            assert!(
                cst == 0 && b == (cres != 0),
                "unicode_is_normalized DIVERGENCE form={form} input={text:?}: C=(st {cst}, res {cres}) Rust=Ok({b})"
            );
            Ok(b)
        }
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "unicode_is_normalized DIVERGENCE form={form} input={text:?}: C=(st {cst}, err {cerr}) Rust=Err({rerr} {})",
                e.message
            );
            Err(e)
        }
    };

    // ---- fc-wrapper plane (wrapper == core) ----
    let timg = text_image(text);
    let fimg = text_image(form.as_bytes());
    let (d, _) = fc_call(
        vb::fc_unicode_is_normalized,
        cx.mcx(),
        [
            Datum::from_usize(timg.as_ptr() as usize),
            Datum::from_usize(fimg.as_ptr() as usize),
        ],
    );
    match (d, &core) {
        (Ok(d), Ok(b)) => assert!(
            d.as_bool() == *b,
            "fc_unicode_is_normalized fc-wrapper DIVERGENCE form={form} input={text:?}"
        ),
        (Err(e), Err(ce)) => assert!(
            rust_err_class(&e) == rust_err_class(ce),
            "fc_unicode_is_normalized fc-wrapper class DIVERGENCE"
        ),
        (r, _) => panic!(
            "fc_unicode_is_normalized fc-wrapper verdict DIVERGENCE form={form}: wrapper Ok={}",
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// Arms 11/12: varstr_levenshtein / varstr_levenshtein_less_equal (non-SQL
// helpers, core plane only — module doc). Cost bands mix small values and
// extremes; string cap straddles MAX_LEVENSHTEIN_STRLEN.
// ---------------------------------------------------------------------------

/// Cost band: small values + extremes. Bounded so DP arithmetic stays well
/// inside i32 on both sides (m,n <= 700; 700 * 20000 * 2 < i32::MAX).
const COSTS: [i32; 8] = [0, 1, 2, 3, 4, 10, 255, 20000];
/// max_d band for the less_equal instantiation (-1 = unbounded).
const MAXDS: [i32; 8] = [-1, 0, 1, 2, 5, 100, 255, 20000];

struct LevInput<'a> {
    source: &'a [u8],
    target: &'a [u8],
    ins_c: i32,
    del_c: i32,
    sub_c: i32,
    trusted: bool,
}

fn lev_decode(payload: &[u8], extra: usize) -> Option<(LevInput<'_>, &[u8])> {
    if payload.len() < 4 + extra {
        return None;
    }
    let (hdr, rest) = payload.split_at(4 + extra);
    if rest.len() > 2 * MAX_LEV {
        return None;
    }
    let split = u16::from_le_bytes([hdr[2], hdr[3]]) as usize % (rest.len() + 1);
    let (source, target) = rest.split_at(split);
    if source.len() > MAX_LEV || target.len() > MAX_LEV {
        return None;
    }
    Some((
        LevInput {
            source,
            target,
            ins_c: COSTS[(hdr[0] & 0x0f) as usize % COSTS.len()],
            del_c: COSTS[(hdr[0] >> 4) as usize % COSTS.len()],
            sub_c: COSTS[(hdr[1] & 0x0f) as usize % COSTS.len()],
            trusted: (hdr[1] >> 4) & 1 == 1,
        },
        &hdr[4..],
    ))
}

fn lev_check(fname: &str, li: &LevInput<'_>, cst: i32, cres: i32, r: PgResult<i32>) {
    let cerr = unsafe { pg_diff_errcode_get() };
    match r {
        Ok(d) => assert!(
            cst == 0 && d == cres,
            "{fname} DIVERGENCE s={:?} t={:?} costs=({},{},{}) trusted={}: C=(st {cst}, {cres}) Rust=Ok({d})",
            li.source, li.target, li.ins_c, li.del_c, li.sub_c, li.trusted
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "{fname} DIVERGENCE s={:?} t={:?}: C=(st {cst}, err {cerr}) Rust=Err({rerr} {})",
                li.source, li.target, e.message
            );
        }
    }
}

fn levenshtein_diff(payload: &[u8]) {
    let Some((li, _)) = lev_decode(payload, 0) else {
        return;
    };
    let mut cres = 0i32;
    let cst = unsafe {
        pg_diff_varstr_levenshtein(
            li.source.as_ptr(),
            li.source.len() as i32,
            li.target.as_ptr(),
            li.target.len() as i32,
            li.ins_c,
            li.del_c,
            li.sub_c,
            li.trusted as i32,
            &mut cres,
        )
    };
    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let r = varlena::levenshtein::varstr_levenshtein(
        cx.mcx(),
        li.source,
        li.target,
        li.ins_c,
        li.del_c,
        li.sub_c,
        li.trusted,
    );
    lev_check("varstr_levenshtein", &li, cst, cres, r);
}

fn levenshtein_less_equal_diff(payload: &[u8]) {
    let Some((li, extra)) = lev_decode(payload, 1) else {
        return;
    };
    let max_d = MAXDS[extra[0] as usize % MAXDS.len()];
    let mut cres = 0i32;
    let cst = unsafe {
        pg_diff_varstr_levenshtein_less_equal(
            li.source.as_ptr(),
            li.source.len() as i32,
            li.target.as_ptr(),
            li.target.len() as i32,
            li.ins_c,
            li.del_c,
            li.sub_c,
            max_d,
            li.trusted as i32,
            &mut cres,
        )
    };
    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    let r = varlena::levenshtein::varstr_levenshtein_less_equal(
        cx.mcx(),
        li.source,
        li.target,
        li.ins_c,
        li.del_c,
        li.sub_c,
        max_d,
        li.trusted,
    );
    lev_check("varstr_levenshtein_less_equal", &li, cst, cres, r);
}

// ---------------------------------------------------------------------------
// Arms 13/14: SplitIdentifierString / SplitGUCList (non-SQL helpers, core
// plane only). Comparison image: split verdict + items joined with 0x1F.
// Fences: separator != 0, string NUL-free valid UTF-8 (module doc).
// ---------------------------------------------------------------------------

/// scanner_isspace's whitespace set (scan.l {space}).
fn is_scanner_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn split_payload(payload: &[u8]) -> Option<(u8, &str)> {
    let (&sep, rest) = payload.split_first()?;
    if sep == 0 || sep >= 0x80 || rest.len() > MAX_TEXT || rest.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(rest).ok()?;
    Some((sep, s))
}

/// Run one C split entry and return its (ok, joined-bytes) image.
fn c_split(
    entry: unsafe extern "C" fn(*const u8, i32, core::ffi::c_char, *mut u8, i32, *mut i32) -> i32,
    s: &str,
    sep: u8,
) -> (bool, Vec<u8>) {
    let raw = s.as_bytes();
    let cap = 2 * raw.len() + 16;
    let mut out = vec![0u8; cap];
    let mut outlen = 0i32;
    let cst = unsafe {
        entry(
            raw.as_ptr(),
            raw.len() as i32,
            sep as core::ffi::c_char,
            out.as_mut_ptr(),
            cap as i32,
            &mut outlen,
        )
    };
    out.truncate(if cst == 0 { outlen as usize } else { 0 });
    (cst == 0, out)
}

fn split_identifier_string_diff(payload: &[u8]) {
    let Some((sep, s)) = split_payload(payload) else {
        return;
    };
    let (cok, cjoined) = c_split(pg_diff_split_identifier_string, s, sep);

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    // Encoding threaded explicitly, pinned UTF8 (the fence of record; C's
    // downcase_identifier sees pg_database_encoding_max_length()==4 too).
    let r = varlena::split_identifier_string(cx.mcx(), s, sep, wchar::PG_UTF8)
        .expect("split_identifier_string only errors on alloc failure");
    match r {
        Some(items) => {
            let joined = items.join("\u{1f}");
            assert!(
                cok && joined.as_bytes() == &cjoined[..],
                "SplitIdentifierString DIVERGENCE sep={sep} input={s:?}: C=(ok {cok}, {:?}) Rust=Some({items:?})",
                String::from_utf8_lossy(&cjoined)
            );
        }
        None => assert!(
            !cok,
            "SplitIdentifierString DIVERGENCE sep={sep} input={s:?}: C ok, Rust None"
        ),
    }
}

/// Arm 15: textToQualifiedNameList (varlena.c) — the '.'-separated wrapper
/// over SplitIdentifierString. The wrapper's own rule is what this arm pins:
/// C raises 42602 "invalid name syntax" iff the split fails OR yields NIL;
/// otherwise it returns the split's names. The list itself is cross-checked
/// against the C split's joined image (arm 13 pins split ≡ C independently).
/// This is the SQL-visible face of the trailing-separator reject fixed in
/// this lane (`textToQualifiedNameList(mcx, "a.")` must ERROR, not panic).
fn qualified_name_list_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(payload) else {
        return;
    };
    let (cok, cjoined) = c_split(pg_diff_split_identifier_string, s, b'.');

    let cx = mcx::MemoryContext::new("vlmisc_fuzz");
    // The split the wrapper is defined over. An empty joined image is
    // ambiguous on the C side (NIL vs one empty quoted name), so the
    // NIL-ness that drives the wrapper's second ereport is read here.
    let split = varlena::split_identifier_string(cx.mcx(), s, b'.', wchar::PG_UTF8)
        .expect("split_identifier_string only errors on alloc failure");
    let want_err = !matches!(&split, Some(items) if !items.is_empty());

    match varlena::textToQualifiedNameList(cx.mcx(), s) {
        Ok(items) => {
            let joined = items.join("\u{1f}");
            assert!(
                !want_err && cok && joined.as_bytes() == &cjoined[..],
                "textToQualifiedNameList DIVERGENCE input={s:?}: C=(ok {cok}, {:?}) Rust=Ok({items:?})",
                String::from_utf8_lossy(&cjoined)
            );
        }
        Err(e) => {
            assert!(
                want_err,
                "textToQualifiedNameList DIVERGENCE input={s:?}: C=(ok {cok}, {:?}) Rust=Err({})",
                String::from_utf8_lossy(&cjoined),
                e.message
            );
            assert!(
                e.sqlstate == types_error::ERRCODE_INVALID_NAME,
                "textToQualifiedNameList sqlstate DIVERGENCE input={s:?}: want 42602, got {}",
                e.message
            );
        }
    }
}

fn split_guc_list_diff(payload: &[u8]) {
    let Some((sep, s)) = split_payload(payload) else {
        return;
    };
    let (cok, cjoined) = c_split(pg_diff_split_guc_list, s, sep);

    match varlena::split_guc_list(s, sep) {
        Some(items) => {
            let joined = items.join("\u{1f}");
            assert!(
                cok && joined.as_bytes() == &cjoined[..],
                "SplitGUCList DIVERGENCE sep={sep} input={s:?}: C=(ok {cok}, {:?}) Rust=Some({items:?})",
                String::from_utf8_lossy(&cjoined)
            );
        }
        None => assert!(
            !cok,
            "SplitGUCList DIVERGENCE sep={sep} input={s:?}: C ok, Rust None"
        ),
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke: per-arm ok+error shapes + fc plane + seed replay.
// GATING CONVENTION: these link against the vendored C oracle, whose
// .file("csrc/pg_vlmisc_io.c") line in core/build.rs stays commented until
// the parent uncomments it (sibling vltext/vlbytea oracles are mid-fill in
// this worktree; a full-workspace link would break on their #error gates).
// Un-ignore = delete the #[ignore] lines when the build gate opens; the
// bodies are complete.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        vlmisc_diff(&d);
    }

    #[test]
    fn to_base_arms_smoke() {
        let _g = crate::c_oracle_serial();
        for sel in 0u8..6 {
            drive(sel, &[]); // zero
            drive(sel, &[0xff; 8]); // -1 / all-ones
            drive(sel, &[0x01]); // 1
            drive(sel, &[0x00, 0x00, 0x00, 0x80]); // i32::MIN image
            drive(sel, &[0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0x7f]);
            drive(sel, &[0x39, 0x30, 0x00, 0x00]); // 12345
        }
    }

    #[test]
    fn unistr_arm_smoke() {
        let _g = crate::c_oracle_serial();
        // ok shapes
        drive(6, b"");
        drive(6, b"plain text, no escapes");
        drive(6, br"\0041 B \+000043 \U00000044");
        drive(6, br"backslash \\ pair");
        drive(6, br"\d83d\de04"); // valid surrogate pair -> U+1F604
        drive(6, "caf\u{e9} déjà".as_bytes()); // multibyte passthrough
        drive(6, b"NUL \x00 is data");
        // error shapes
        drive(6, br"\d83d"); // dangling first surrogate -> 42601
        drive(6, br"\de04"); // bare second surrogate -> 42601
        drive(6, br"\d83dX"); // pair broken by literal -> 42601
        drive(6, br"\d83d\d83d"); // first+first -> 42601
        drive(6, br"\xyz"); // invalid escape -> 42601
        drive(6, br"\"); // dangling backslash -> 42601
        drive(6, br"\+110000"); // > U+10FFFF -> 22023
        drive(6, br"\0000"); // U+0000 invalid -> 22023
        drive(6, br"\U00110000"); // > U+10FFFF -> 22023
    }

    #[test]
    fn unicode_arms_smoke() {
        let _g = crate::c_oracle_serial();
        drive(7, b""); // unicode_version constant
        // unicode_assigned: assigned / unassigned / gated shapes
        drive(8, b"hello");
        drive(8, "é🐉".as_bytes());
        drive(8, &[0xcd, 0xb8]); // U+0378 unassigned -> false
        drive(8, &[0xf4, 0x8f, 0xbf, 0xbf]); // U+10FFFF unassigned -> false
        drive(8, &[0xc3]); // truncated tail: gated out (walk fence)
        // normalize / is_normalized over every form x interesting strings
        let strings: [&[u8]; 6] = [
            b"",
            b"abc",
            "e\u{301}".as_bytes(),           // e + combining acute (NFC -> é)
            "\u{e9}".as_bytes(),             // precomposed é (NFD -> e + mark)
            "\u{1100}\u{1161}\u{11a8}".as_bytes(), // Hangul jamo (NFC -> 각)
            "\u{fb01}nal".as_bytes(),        // ﬁ ligature (NFKC/NFKD fold)
        ];
        for fsel in 0u8..12 {
            for s in strings {
                let mut p = vec![fsel];
                p.extend_from_slice(s);
                drive(9, &p);
                drive(10, &p);
            }
        }
    }

    #[test]
    fn levenshtein_arms_smoke() {
        let _g = crate::c_oracle_serial();
        let mk = |c0: u8, c1: u8, extra: &[u8], src: &[u8], tgt: &[u8]| {
            let all = [src, tgt].concat();
            let mut p = vec![c0, c1];
            p.extend_from_slice(&(src.len() as u16).to_le_bytes());
            p.extend_from_slice(extra);
            p.extend_from_slice(&all);
            p
        };
        // classic pair, unit costs (c0=0x11 -> ins=del=1, c1=0x01 -> sub=1)
        drive(11, &mk(0x11, 0x01, &[], b"kitten", b"sitting"));
        drive(11, &mk(0x11, 0x01, &[], b"", b"abc"));
        drive(11, &mk(0x11, 0x01, &[], b"abc", b""));
        // multibyte path
        drive(11, &mk(0x11, 0x01, &[], "héllo".as_bytes(), "hello".as_bytes()));
        // extreme costs band
        drive(11, &mk(0x77, 0x07, &[], b"aaab", b"abab"));
        // MAX_LEVENSHTEIN_STRLEN boundary: 255 ok, 256 errors (untrusted)
        drive(11, &mk(0x11, 0x01, &[], &[b'a'; 255], &[b'b'; 3]));
        drive(11, &mk(0x11, 0x01, &[], &[b'a'; 256], &[b'b'; 3]));
        // trusted bypasses the length error (c1 bit 4)
        drive(11, &mk(0x11, 0x11, &[], &[b'a'; 256], &[b'b'; 3]));
        // less_equal: max_d bands incl. pruning fast-exit
        for md in 0u8..8 {
            drive(12, &mk(0x11, 0x01, &[md], b"kitten", b"sitting"));
        }
        drive(12, &mk(0x11, 0x01, &[2], &[b'q'; 200], &[b'z'; 100]));
    }

    #[test]
    fn split_arms_smoke() {
        let _g = crate::c_oracle_serial();
        let mk = |sep: u8, s: &str| {
            let mut p = vec![sep];
            p.extend_from_slice(s.as_bytes());
            p
        };
        for arm in [13u8, 14] {
            drive(arm, &mk(b',', ""));
            drive(arm, &mk(b',', "   "));
            drive(arm, &mk(b',', "a,b , c"));
            drive(arm, &mk(b',', "Foo,BAR")); // downcased by 13, kept by 14
            drive(arm, &mk(b',', "\"Quoted Name\",plain"));
            drive(arm, &mk(b',', "\"a\"\"b\"")); // quote-quote collapse
            drive(arm, &mk(b',', "\"unterminated")); // false / None
            drive(arm, &mk(b',', "a,,b")); // empty name -> false
            drive(arm, &mk(b',', "a b")); // junk after name -> false
            drive(arm, &mk(b'.', "pg_catalog.pg_class"));
            // 70-char identifier: NAMEDATALEN-1 truncation (arm 13 only
            // truncates; arm 14 keeps it whole — both vs their own C)
            drive(arm, &mk(b',', &"x".repeat(70)));
            // multibyte identifier straddling the 63-byte clip
            drive(arm, &mk(b',', &"é".repeat(40)));
        }
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). Corpus committed under ../corpus/vlmisc_diff.
    #[test]
    fn seed_corpus_replays_clean() {
        let _g = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/vlmisc_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/vlmisc_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                vlmisc_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Fuzz-shaped byte soup through every selector: the whole driver must
    /// be panic-free on arbitrary input (asserts fire only on divergence).
    #[test]
    fn selector_soup() {
        let _g = crate::c_oracle_serial();
        for sel in 0u8..16 {
            for len in [0usize, 1, 2, 5, 16, 65, 256, 700] {
                let payload: Vec<u8> = (0..len)
                    .map(|i| (i as u8).wrapping_mul(41).wrapping_add(sel))
                    .collect();
                drive(sel, &payload);
            }
        }
        vlmisc_diff(&[]);
    }
}
