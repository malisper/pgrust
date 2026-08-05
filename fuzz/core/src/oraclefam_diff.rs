//! oraclefam_diff: differential fuzz driver — shipped Rust `adt_oracle_compat`
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_oraclefam_io.c). Crate under test:
//! crates/backend/utils/adt/oracle_compat.
//!
//! Comparison planes (float_in_diff conventions): full output payload bytes,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//! Any mismatch panics, so a libFuzzer crash artifact is a divergence
//! reproducer.
//!
//! ENCODING ENVIRONMENT (the key seam): every exec pins ONE database
//! encoding drawn from {SQL_ASCII, UTF8, LATIN1} out of the selector byte —
//! on the Rust side via the shipped mbutils::SetDatabaseEncoding (TLS cell,
//! the same hook enc_tables_diff already uses), on the C side via the enc
//! argument every pg_diff_oc_* entry pins into its TLS cell before the
//! vendored body runs (the C GetDatabaseEncoding / pg_mblen family read it,
//! backed by the verbatim pg_utf_mblen + wchar.c maxmblen rows). The
//! ENVIRONMENT is mocked identically on both sides; the COMPUTATION (mb
//! walkers, kernels) is verbatim on the C side and shipped code on the Rust
//! side.
//!
//! Input layout: [selector][enc_sel][payload]; selector % 12 picks the arm:
//!    0 case      [which][text]         asc_tolower/toupper/initcap kernels +
//!                                      crate lower/upper/initcap/casefold at
//!                                      C_COLLATION_OID + fc wrappers;
//!                                      which & 0x80 => the INVALID-collid
//!                                      arm (collid = InvalidOid, 42P22
//!                                      verdict both sides — rule-pinned,
//!                                      see carves)
//!    1 lpad      [len4][mode][l2][s2][s1]
//!    2 rpad      [len4][mode][l2][s2][s1]
//!    3 trim      [flags][setlen][set][string]  btrim/ltrim/rtrim + 1-arg forms
//!    4 byteatrim [flags][setlen][set][string]  byteatrim/bytealtrim/byteartrim
//!    5 translate [fl][from][tl][to][string]
//!    6 ascii     [text]
//!    7 chr       [mode][arg4]
//!    8 repeat    [mode][count4][text]
//!    9 text_left [n4][text]
//!   10 text_right[n4][text]
//!   11 text_reverse [text]
//!
//! Domain carves (documented, ratified non-surfaces):
//!   - LOCALE PROVIDERS ARE OUT OF SCOPE (lane carve of record): only the
//!     C-collation arm of the case functions is diffed (C_COLLATION_OID =>
//!     ctype_is_c => the asc_* kernels, byte-for-byte what C's str_tolower/
//!     str_toupper/str_initcap take on that arm in formatting.c). The
//!     pg_locale pg_strlower/strupper/strtitle/strfold dispatch and non-C
//!     locale_for arms are NOT wired.
//!   - casefold verdicts are RULE-PINNED to formatting.c str_casefold @
//!     62d6c7d3df: server encoding != UTF8 => 42601 (its encoding gate);
//!     UTF8 + C collation => asc_tolower — the value plane there is still
//!     the C asc_tolower oracle. (Vendoring str_casefold itself would drag
//!     in the carved-out locale dispatch.)
//!   - INVALID-collid verdicts are likewise RULE-PINNED to formatting.c @
//!     62d6c7d3df: str_tolower/str_toupper/str_initcap/str_casefold all
//!     gate `!OidIsValid(collid)` FIRST (before any locale lookup, and in
//!     str_casefold before its encoding gate — formatting.c:1645-1656,
//!     1709-1720, 1773-1784, 1837-1848) and raise 42P22
//!     ERRCODE_INDETERMINATE_COLLATION. The C oracle entries don't model
//!     the collid check (it lives in the carved-out str_* dispatch);
//!     value plane n/a on the error verdict, message text out of scope
//!     (str_casefold spells its message with "lower()" — errcode plane
//!     only).
//!   - ENCODING GRID: arms 6 (ascii) and 7 (chr) draw from a FOUR-encoding
//!     grid {SQL_ASCII, UTF8, LATIN1, EUC_JP} — EUC_JP (max_length 3)
//!     reaches their multibyte-non-UTF8 reject arms (ascii 54000
//!     "character too large"; chr's is_mb && cvalue > 127 54000), which
//!     are unreachable under the single-byte/UTF8 trio. These two
//!     functions only consult pg_encoding_max_length and the first byte —
//!     no mblen walking — so the C oracle needs only the pinned
//!     max_length row (wchar.c EUC_JP maxmblen = 3). All other arms stay
//!     on the three-encoding grid; EUC_JP is never routed through the
//!     mblen-walking family.
//!   - ascii() under UTF8 assumes server-verified text (the C body indexes
//!     continuation bytes unchecked — invalid UTF8 is C out-of-bounds, not
//!     a comparable behavior): inputs failing pg_verify_mbstr(UTF8) are
//!     skipped for arm 6 only. Every other arm except one (next carve)
//!     walks with the bounded pg_mblen_range/pg_mblen_with_len family on
//!     BOTH sides, so invalid multibyte inputs flow through and the 22021
//!     invalid-byte-sequence error plane is compared, not carved.
//!   - text_left(n >= 0) under UTF8 also assumes server-verified text
//!     (found by this target's first smoke, artifact
//!     crash-e204081f8188..., banked as seed oc-tleft-carve-regression):
//!     C's text_substring counts chars via pg_mbcharcliplen_chars, which
//!     stops at EXACTLY n chars and never validates the (n+1)-th, while
//!     the shipped Rust inlining uses pg_mbcharcliplen, whose lookahead
//!     validates one char further (mbutils.c pg_mbcharcliplen computes
//!     pg_mblen_with_len BEFORE its nch > limit break). On verified text
//!     the two are byte-identical; on text with an invalid sequence right
//!     after char n, C returns the prefix and Rust raises 22021 — a
//!     verified-text-domain difference, not a bug (server text is always
//!     verified at ingestion). Inputs failing pg_verify_mbstr(UTF8) are
//!     skipped for the arm-9 n >= 0 path only; the n < 0 path and
//!     text_right keep the shared walker shape and stay ungated.
//!   - Interior NUL is IN DOMAIN everywhere (a compared case, not a skip):
//!     the case kernels' pnstrdup/first-NUL truncation and the mb walkers'
//!     NUL stops are part of the compared contract on both sides.
//!   - Budget clamps (input shaping, identical on both sides, not carves):
//!     text fields are capped (<= ~400B), pad len / repeat count are drawn
//!     from {small, negative, > MaxAllocSize} bands so the 54000 planes are
//!     hit without gigabyte pallocs, and repeat count stays small when the
//!     string is empty (C would spin count times producing "").
//!
//! FC-WRAPPER PLANE: each arm routes the same input through the crate's
//! builtins.rs fc_* wrapper via a native types_fmgr::LocalFcinfo frame and
//! asserts wrapper == core (payload bytes / error verdict + sqlstate).
//! Executed: fc_lower, fc_upper, fc_initcap, fc_casefold, fc_lpad, fc_rpad,
//! fc_btrim, fc_ltrim, fc_rtrim, fc_btrim1, fc_ltrim1, fc_rtrim1,
//! fc_byteatrim, fc_bytealtrim, fc_byteartrim, fc_translate, fc_ascii,
//! fc_chr, fc_repeat, fc_text_left, fc_text_right, fc_text_reverse.
//!
//! C errcode classes (csrc/pg_oraclefam_io.c): 1 = 54000, 2 = 22023,
//! 3 = 22021, 4 = 22011 (defined, unreachable via these arms).

use adt_oracle_compat::{builtins, casemap};
use datum::{Datum, NullableDatum, Varlena};
use mcx::{Mcx, MemoryContext};
use types_error::{
    PgResult, SqlState, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SUBSTRING_ERROR,
    ERRCODE_SYNTAX_ERROR,
};
use types_fmgr::{LocalFcinfo, PGFunction};
use wchar::{pg_enc, PG_EUC_JP, PG_LATIN1, PG_SQL_ASCII, PG_UTF8};

extern "C" {
    fn pg_diff_oc_case(
        which: i32,
        enc: i32,
        buf: *const u8,
        nbytes: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_pad(
        left: i32,
        enc: i32,
        s1: *const u8,
        l1: i32,
        len: i32,
        s2: *const u8,
        l2: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_trim(
        enc: i32,
        s: *const u8,
        slen: i32,
        set: *const u8,
        setlen: i32,
        doltrim: i32,
        dortrim: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_byteatrim(
        s: *const u8,
        slen: i32,
        set: *const u8,
        setlen: i32,
        doltrim: i32,
        dortrim: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_translate(
        enc: i32,
        s: *const u8,
        slen: i32,
        from: *const u8,
        fromlen: i32,
        to: *const u8,
        tolen: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_ascii(enc: i32, s: *const u8, slen: i32, result: *mut i32) -> i32;
    fn pg_diff_oc_chr(enc: i32, arg: i32, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_oc_repeat(
        enc: i32,
        s: *const u8,
        slen: i32,
        count: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_text_leftright(
        which: i32,
        enc: i32,
        t: *const u8,
        tlen: i32,
        n: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_oc_text_reverse(
        enc: i32,
        t: *const u8,
        tlen: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
}

/// C errcode class -> the sqlstate the shipped Rust must have raised.
fn c_class_state(rc: i32) -> SqlState {
    match rc {
        1 => ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        2 => ERRCODE_INVALID_PARAMETER_VALUE,
        3 => ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
        4 => ERRCODE_SUBSTRING_ERROR,
        other => panic!("C oracle returned unknown errcode class {other}"),
    }
}

const ENCS: [pg_enc; 3] = [PG_SQL_ASCII, PG_UTF8, PG_LATIN1];
/// Arms 6/7 (ascii/chr) only — see the EUC_JP encoding-grid note in the
/// header. Never routed through the mblen-walking family.
const ENCS4: [pg_enc; 4] = [PG_SQL_ASCII, PG_UTF8, PG_LATIN1, PG_EUC_JP];
const TEXT_CAP: usize = 400;

// ---------------------------------------------------------------------------
// payload decoding helpers
// ---------------------------------------------------------------------------

fn cap(s: &[u8], n: usize) -> &[u8] {
    &s[..s.len().min(n)]
}

fn take_i32(p: &[u8]) -> (i32, &[u8]) {
    let mut b = [0u8; 4];
    for (i, &x) in p.iter().take(4).enumerate() {
        b[i] = x;
    }
    (i32::from_le_bytes(b), p.get(4..).unwrap_or(&[]))
}

/// One-byte-length-prefixed field (keeps the full byte alphabet available —
/// no separator byte is excluded from the field domain).
fn lp_field(p: &[u8]) -> (&[u8], &[u8]) {
    let Some((&l, rest)) = p.split_first() else {
        return (&[], &[]);
    };
    let l = (l as usize).min(rest.len());
    (&rest[..l], &rest[l..])
}

// ---------------------------------------------------------------------------
// C-call + comparison plumbing
// ---------------------------------------------------------------------------

/// Run a C entry writing into an `out_cap`-byte buffer; return (rc, bytes).
fn c_text_call(
    out_cap: usize,
    f: impl FnOnce(*mut u8, *mut i32) -> i32,
) -> (i32, Vec<u8>) {
    let mut out = vec![0u8; out_cap];
    let mut outlen: i32 = 0;
    let rc = f(out.as_mut_ptr(), &mut outlen);
    if rc == 0 {
        assert!(
            (outlen as usize) <= out.len(),
            "C oracle overran its out buffer: {outlen} > {}",
            out.len()
        );
        out.truncate(outlen as usize);
    } else {
        out.clear();
    }
    (rc, out)
}

/// Value + verdict + sqlstate planes for a text-returning core function.
fn check_text(who: &str, r: PgResult<Varlena<'_>>, rc: i32, c_bytes: &[u8]) {
    match r {
        Ok(v) => assert!(
            rc == 0 && v.data() == c_bytes,
            "{who} DIVERGENCE: C=(rc {rc}, {c_bytes:02x?}) Rust=Ok({:02x?})",
            v.data()
        ),
        Err(e) => assert!(
            rc != 0 && e.sqlstate() == c_class_state(rc),
            "{who} DIVERGENCE: C rc={rc} vs Rust Err({:?} {})",
            e.sqlstate(),
            e.message
        ),
    }
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — verbatim shape
// from uuid_diff.rs / enc_tables.rs).
// ---------------------------------------------------------------------------

/// Build a 4B-uncompressed text varlena image: [4-byte LE header][payload].
fn text_image(bytes: &[u8]) -> Vec<u8> {
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    img.extend_from_slice(bytes);
    img
}

/// Read back a 4B-uncompressed varlena result datum's payload.
unsafe fn result_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let word = u32::from_le_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
    let total = (word >> 2) as usize;
    std::slice::from_raw_parts(p.add(4), total - 4)
}

/// Invoke an fc_* wrapper over non-null args under a collation.
fn fc_call<const N: usize>(
    f: PGFunction,
    m: Mcx<'_>,
    collation: types_core::Oid,
    args: [Datum; N],
) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(collation);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    f(None, &mut fcinfo)
}

/// fc wrapper (text-returning) must agree with the already-C-checked core
/// result on all planes.
fn check_fc_text(who: &str, fc: PgResult<Datum>, core: &PgResult<Varlena<'_>>) {
    match (fc, core) {
        (Ok(d), Ok(v)) => {
            // SAFETY: wrapper result is a live 4B varlena in the arming mcx.
            let payload = unsafe { result_payload(d) };
            assert!(
                payload == v.data(),
                "{who} fc-plane DIVERGENCE: fc={payload:02x?} core={:02x?}",
                v.data()
            );
        }
        (Err(fe), Err(ce)) => assert!(
            fe.sqlstate() == ce.sqlstate(),
            "{who} fc-plane errcode DIVERGENCE: fc={:?} core={:?}",
            fe.sqlstate(),
            ce.sqlstate()
        ),
        (f, c) => panic!("{who} fc-plane verdict DIVERGENCE: fc={f:?} core={c:?}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: case family (kernels + crate entries at C collation + fc plane).
// ---------------------------------------------------------------------------

fn case_diff(enc: pg_enc, payload: &[u8]) {
    let Some((&wb, s)) = payload.split_first() else {
        return;
    };
    let which = wb % 4;
    let s = cap(s, TEXT_CAP);
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    // INVALID-collid arm (wb & 0x80): both the crate entry and the fc
    // wrapper must raise 42P22 — rule-pinned to formatting.c's leading
    // !OidIsValid(collid) gates (see the header carve; the C side of this
    // verdict is the carved-out str_* dispatch, so no oracle call here).
    if wb & 0x80 != 0 {
        let invalid = types_core::INVALID_OID;
        let core = match which {
            0 => adt_oracle_compat::lower(mcx, s, invalid),
            1 => adt_oracle_compat::upper(mcx, s, invalid),
            2 => adt_oracle_compat::initcap(mcx, s, invalid),
            _ => adt_oracle_compat::casefold(mcx, s, invalid),
        };
        match &core {
            Ok(_) => panic!("case entry which={which}: invalid collid must error (42P22)"),
            Err(e) => assert!(
                e.sqlstate() == ERRCODE_INDETERMINATE_COLLATION,
                "case entry which={which}: invalid collid raised ({:?} {}) not 42P22",
                e.sqlstate(),
                e.message
            ),
        }
        let img = text_image(s);
        let td = Datum::from_usize(img.as_ptr() as usize);
        let fc: PGFunction = match which {
            0 => builtins::fc_lower,
            1 => builtins::fc_upper,
            2 => builtins::fc_initcap,
            _ => builtins::fc_casefold,
        };
        check_fc_text("fc_case_invalid_collid", fc_call(fc, mcx, invalid, [td]), &core);
        return;
    }

    // C kernel oracle (casefold's UTF8+C-collation value oracle is
    // asc_tolower — see the rule-pin carve in the header).
    let kernel_which = if which == 3 { 0 } else { which as i32 };
    let mut out = vec![0u8; s.len() + 1];
    let mut outlen: i32 = 0;
    // SAFETY: out holds nbytes + 1 (NUL-terminated result contract).
    let rc = unsafe {
        pg_diff_oc_case(kernel_which, enc, s.as_ptr(), s.len() as i32, out.as_mut_ptr(), &mut outlen)
    };
    assert_eq!(rc, 0, "asc_* kernels cannot error");
    let c_out = &out[..outlen as usize];

    // Shipped kernel plane.
    let r_kernel = match which {
        0 | 3 => casemap::asc_tolower(mcx, s),
        1 => casemap::asc_toupper(mcx, s),
        _ => casemap::asc_initcap(mcx, s),
    }
    .expect("asc_* kernels cannot error");
    assert!(
        &r_kernel[..] == c_out,
        "asc_* kernel DIVERGENCE which={which}: C={c_out:02x?} Rust={:02x?}",
        &r_kernel[..]
    );

    // Crate entry plane at C_COLLATION_OID (ctype_is_c => asc_* arm).
    let collid = types_core::C_COLLATION_OID;
    let core = match which {
        0 => adt_oracle_compat::lower(mcx, s, collid),
        1 => adt_oracle_compat::upper(mcx, s, collid),
        2 => adt_oracle_compat::initcap(mcx, s, collid),
        _ => adt_oracle_compat::casefold(mcx, s, collid),
    };
    match &core {
        Ok(v) => {
            let ok_expected = which != 3 || enc == PG_UTF8;
            assert!(
                ok_expected && v.data() == c_out,
                "case entry DIVERGENCE which={which} enc={enc}: C={c_out:02x?} Rust=Ok({:02x?})",
                v.data()
            );
        }
        Err(e) => {
            assert!(
                which == 3 && enc != PG_UTF8 && e.sqlstate() == ERRCODE_SYNTAX_ERROR,
                "case entry DIVERGENCE which={which} enc={enc}: Rust Err({:?} {})",
                e.sqlstate(),
                e.message
            );
        }
    }

    // fc plane.
    let img = text_image(s);
    let td = Datum::from_usize(img.as_ptr() as usize);
    let fc: PGFunction = match which {
        0 => builtins::fc_lower,
        1 => builtins::fc_upper,
        2 => builtins::fc_initcap,
        _ => builtins::fc_casefold,
    };
    check_fc_text("fc_case", fc_call(fc, mcx, collid, [td]), &core);
}

// ---------------------------------------------------------------------------
// Arm 1/2: lpad / rpad.
// ---------------------------------------------------------------------------

fn pad_diff(enc: pg_enc, payload: &[u8], left: bool) {
    let (raw, rest) = take_i32(payload);
    let Some((&mode, rest)) = rest.split_first() else {
        return;
    };
    let (s2, s1) = lp_field(rest);
    let (s2, s1) = (cap(s2, 300), cap(s1, 300));
    // len bands: small / negative / >MaxAllocSize (54000 plane) / tiny.
    let len = match mode % 4 {
        0 => raw & 0x7FF,
        1 => -(raw & 0x7FF),
        2 => (raw & 0x3FFF_FFFF) | 0x4000_0000,
        _ => raw & 0xF,
    };

    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();
    // Worst-case success: effective len <= 2047 (bands) or s1 char count.
    let out_cap = 4 * 2048 + 4 * 300 + 16;
    let (rc, c_out) = c_text_call(out_cap, |o, ol| {
        // SAFETY: buffers sized per the entry contracts (see csrc).
        unsafe {
            pg_diff_oc_pad(
                left as i32,
                enc,
                s1.as_ptr(),
                s1.len() as i32,
                len,
                s2.as_ptr(),
                s2.len() as i32,
                o,
                ol,
            )
        }
    });
    let core = if left {
        adt_oracle_compat::lpad(mcx, s1, len, s2)
    } else {
        adt_oracle_compat::rpad(mcx, s1, len, s2)
    };
    check_text(if left { "lpad" } else { "rpad" }, core, rc, &c_out);

    // fc plane (re-run the core through the wrapper; compare to a fresh core
    // result — Varlena results are moved by check_text's Ok arm read).
    let core2 = if left {
        adt_oracle_compat::lpad(mcx, s1, len, s2)
    } else {
        adt_oracle_compat::rpad(mcx, s1, len, s2)
    };
    let i1 = text_image(s1);
    let i2 = text_image(s2);
    let fc = fc_call(
        if left { builtins::fc_lpad } else { builtins::fc_rpad },
        mcx,
        0,
        [
            Datum::from_usize(i1.as_ptr() as usize),
            Datum::from_i32(len),
            Datum::from_usize(i2.as_ptr() as usize),
        ],
    );
    check_fc_text(if left { "fc_lpad" } else { "fc_rpad" }, fc, &core2);
}

// ---------------------------------------------------------------------------
// Arm 3: text trim family (2-arg + 1-arg forms).
// ---------------------------------------------------------------------------

fn trim_diff(enc: pg_enc, payload: &[u8]) {
    let Some((&flags, rest)) = payload.split_first() else {
        return;
    };
    let (set, string) = lp_field(rest);
    let (set, string) = (cap(set, 200), cap(string, TEXT_CAP));
    let (dol, dor) = match flags % 3 {
        0 => (true, true),
        1 => (true, false),
        _ => (false, true),
    };
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    // 2-arg form.
    let (rc, c_out) = c_text_call(string.len() + 4, |o, ol| {
        // SAFETY: out >= slen per the entry contract.
        unsafe {
            pg_diff_oc_trim(
                enc,
                string.as_ptr(),
                string.len() as i32,
                set.as_ptr(),
                set.len() as i32,
                dol as i32,
                dor as i32,
                o,
                ol,
            )
        }
    });
    let core = match flags % 3 {
        0 => adt_oracle_compat::btrim(mcx, string, set),
        1 => adt_oracle_compat::ltrim(mcx, string, set),
        _ => adt_oracle_compat::rtrim(mcx, string, set),
    };
    check_text("trim2", core, rc, &c_out);

    // 1-arg form (set fixed as " " — the btrim1/ltrim1/rtrim1 SQL forms).
    let (rc1, c_out1) = c_text_call(string.len() + 4, |o, ol| {
        // SAFETY: out >= slen per the entry contract.
        unsafe {
            pg_diff_oc_trim(
                enc,
                string.as_ptr(),
                string.len() as i32,
                b" ".as_ptr(),
                1,
                dol as i32,
                dor as i32,
                o,
                ol,
            )
        }
    });
    let core1 = match flags % 3 {
        0 => adt_oracle_compat::btrim1(mcx, string),
        1 => adt_oracle_compat::ltrim1(mcx, string),
        _ => adt_oracle_compat::rtrim1(mcx, string),
    };
    check_text("trim1", core1, rc1, &c_out1);

    // fc plane, both forms.
    let core2 = match flags % 3 {
        0 => adt_oracle_compat::btrim(mcx, string, set),
        1 => adt_oracle_compat::ltrim(mcx, string, set),
        _ => adt_oracle_compat::rtrim(mcx, string, set),
    };
    let is_ = text_image(string);
    let iset = text_image(set);
    let fc2: PGFunction = match flags % 3 {
        0 => builtins::fc_btrim,
        1 => builtins::fc_ltrim,
        _ => builtins::fc_rtrim,
    };
    check_fc_text(
        "fc_trim2",
        fc_call(
            fc2,
            mcx,
            0,
            [
                Datum::from_usize(is_.as_ptr() as usize),
                Datum::from_usize(iset.as_ptr() as usize),
            ],
        ),
        &core2,
    );
    let core3 = match flags % 3 {
        0 => adt_oracle_compat::btrim1(mcx, string),
        1 => adt_oracle_compat::ltrim1(mcx, string),
        _ => adt_oracle_compat::rtrim1(mcx, string),
    };
    let fc1: PGFunction = match flags % 3 {
        0 => builtins::fc_btrim1,
        1 => builtins::fc_ltrim1,
        _ => builtins::fc_rtrim1,
    };
    check_fc_text(
        "fc_trim1",
        fc_call(fc1, mcx, 0, [Datum::from_usize(is_.as_ptr() as usize)]),
        &core3,
    );
}

// ---------------------------------------------------------------------------
// Arm 4: bytea trim family (encoding-independent, full byte alphabet).
// ---------------------------------------------------------------------------

fn byteatrim_diff(payload: &[u8]) {
    let Some((&flags, rest)) = payload.split_first() else {
        return;
    };
    let (set, string) = lp_field(rest);
    let (set, string) = (cap(set, 200), cap(string, TEXT_CAP));
    let (dol, dor) = match flags % 3 {
        0 => (true, true),
        1 => (true, false),
        _ => (false, true),
    };
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    let (rc, c_out) = c_text_call(string.len() + 4, |o, ol| {
        // SAFETY: out >= slen per the entry contract.
        unsafe {
            pg_diff_oc_byteatrim(
                string.as_ptr(),
                string.len() as i32,
                set.as_ptr(),
                set.len() as i32,
                dol as i32,
                dor as i32,
                o,
                ol,
            )
        }
    });
    assert_eq!(rc, 0, "dobyteatrim cannot error");
    // The pure window core, then the varlena-building entry points.
    let window = adt_oracle_compat::dobyteatrim(string, set, dol, dor);
    assert!(
        window == &c_out[..],
        "dobyteatrim window DIVERGENCE: C={c_out:02x?} Rust={window:02x?}"
    );
    let core = match flags % 3 {
        0 => adt_oracle_compat::byteatrim(mcx, string, set),
        1 => adt_oracle_compat::bytealtrim(mcx, string, set),
        _ => adt_oracle_compat::byteartrim(mcx, string, set),
    };
    check_text("byteatrim", core, rc, &c_out);

    let core2 = match flags % 3 {
        0 => adt_oracle_compat::byteatrim(mcx, string, set),
        1 => adt_oracle_compat::bytealtrim(mcx, string, set),
        _ => adt_oracle_compat::byteartrim(mcx, string, set),
    };
    let is_ = text_image(string);
    let iset = text_image(set);
    let fc: PGFunction = match flags % 3 {
        0 => builtins::fc_byteatrim,
        1 => builtins::fc_bytealtrim,
        _ => builtins::fc_byteartrim,
    };
    check_fc_text(
        "fc_byteatrim",
        fc_call(
            fc,
            mcx,
            0,
            [
                Datum::from_usize(is_.as_ptr() as usize),
                Datum::from_usize(iset.as_ptr() as usize),
            ],
        ),
        &core2,
    );
}

// ---------------------------------------------------------------------------
// Arm 5: translate.
// ---------------------------------------------------------------------------

fn translate_diff(enc: pg_enc, payload: &[u8]) {
    let (from, rest) = lp_field(payload);
    let (to, string) = lp_field(rest);
    let (from, to, string) = (cap(from, 64), cap(to, 64), cap(string, 300));
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    let (rc, c_out) = c_text_call(4 * string.len() + 8, |o, ol| {
        // SAFETY: out >= 4*slen + 4 per the entry contract.
        unsafe {
            pg_diff_oc_translate(
                enc,
                string.as_ptr(),
                string.len() as i32,
                from.as_ptr(),
                from.len() as i32,
                to.as_ptr(),
                to.len() as i32,
                o,
                ol,
            )
        }
    });
    let core = adt_oracle_compat::translate(mcx, string, from, to);
    check_text("translate", core, rc, &c_out);

    let core2 = adt_oracle_compat::translate(mcx, string, from, to);
    let (is_, ifrom, ito) = (text_image(string), text_image(from), text_image(to));
    check_fc_text(
        "fc_translate",
        fc_call(
            builtins::fc_translate,
            mcx,
            0,
            [
                Datum::from_usize(is_.as_ptr() as usize),
                Datum::from_usize(ifrom.as_ptr() as usize),
                Datum::from_usize(ito.as_ptr() as usize),
            ],
        ),
        &core2,
    );
}

// ---------------------------------------------------------------------------
// Arm 6: ascii (UTF8 inputs pre-verified — see the header carve).
// ---------------------------------------------------------------------------

fn ascii_diff(enc: pg_enc, payload: &[u8]) {
    let s = cap(payload, TEXT_CAP);
    if enc == PG_UTF8 && !matches!(mbutils::pg_verify_mbstr(PG_UTF8, s, true), Ok(true)) {
        return; // domain carve: server text is always verified (header)
    }
    let mut c_val: i32 = 0;
    // SAFETY: scalar out param.
    let rc = unsafe { pg_diff_oc_ascii(enc, s.as_ptr(), s.len() as i32, &mut c_val) };
    let core = adt_oracle_compat::ascii(s);
    match &core {
        Ok(v) => assert!(
            rc == 0 && *v == c_val,
            "ascii DIVERGENCE: C=(rc {rc}, {c_val}) Rust=Ok({v})"
        ),
        Err(e) => assert!(
            rc != 0 && e.sqlstate() == c_class_state(rc),
            "ascii DIVERGENCE: C rc={rc} vs Rust Err({:?} {})",
            e.sqlstate(),
            e.message
        ),
    }

    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();
    let img = text_image(s);
    let fc = fc_call(
        builtins::fc_ascii,
        mcx,
        0,
        [Datum::from_usize(img.as_ptr() as usize)],
    );
    match (fc, &core) {
        (Ok(d), Ok(v)) => assert!(
            d.as_i32() == *v,
            "fc_ascii fc-plane DIVERGENCE: fc={} core={v}",
            d.as_i32()
        ),
        (Err(fe), Err(ce)) => assert_eq!(fe.sqlstate(), ce.sqlstate(), "fc_ascii errcode"),
        (f, c) => panic!("fc_ascii verdict DIVERGENCE: fc={f:?} core={c:?}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 7: chr.
// ---------------------------------------------------------------------------

/// Core + C planes for one (enc, arg) cell (shared with the sweep tests).
fn chr_case_core(enc: pg_enc, arg: i32, mcx: Mcx<'_>) {
    mbutils::SetDatabaseEncoding(enc).expect("grid encoding valid");
    let (rc, c_out) = c_text_call(8, |o, ol| {
        // SAFETY: out >= 4 per the entry contract.
        unsafe { pg_diff_oc_chr(enc, arg, o, ol) }
    });
    let core = adt_oracle_compat::chr(mcx, arg);
    check_text("chr", core, rc, &c_out);
}

/// All planes (adds the fc wrapper) for one (enc, arg) cell.
fn chr_case_full(enc: pg_enc, arg: i32, mcx: Mcx<'_>) {
    chr_case_core(enc, arg, mcx);
    let core = adt_oracle_compat::chr(mcx, arg);
    check_fc_text(
        "fc_chr",
        fc_call(builtins::fc_chr, mcx, 0, [Datum::from_i32(arg)]),
        &core,
    );
}

fn chr_diff(enc: pg_enc, payload: &[u8]) {
    let Some((&mode, rest)) = payload.split_first() else {
        return;
    };
    let (raw, _) = take_i32(rest);
    let arg = match mode % 4 {
        0 => raw,
        1 => raw & 0x1F_FFFF,                  // UTF8 4-byte band incl > U+10FFFF
        2 => 0xD800 + (raw & 0x7FF),           // surrogate band
        _ => raw & 0x1FF,                      // single-byte boundary band
    };
    let ctx = MemoryContext::new("oraclefam_diff");
    chr_case_full(enc, arg, ctx.mcx());
}

// ---------------------------------------------------------------------------
// Arm 8: repeat.
// ---------------------------------------------------------------------------

fn repeat_diff(enc: pg_enc, payload: &[u8]) {
    let Some((&mode, rest)) = payload.split_first() else {
        return;
    };
    let (raw, s) = take_i32(rest);
    let s = cap(s, 128);
    // count bands: small / >MaxAllocSize (54000 plane; needs slen > 0 to
    // error rather than spin) / negative.
    let count = match mode % 3 {
        1 if !s.is_empty() => (raw & 0x3FFF_FFFF) | 0x4000_0000,
        2 => -(raw & 0x7FFF),
        _ => raw & 0x3FF,
    };
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    let out_cap = (count.max(0).min(0x400) as usize) * s.len() + 8;
    let (rc, c_out) = c_text_call(out_cap, |o, ol| {
        // SAFETY: out sized from the same clamped count both sides receive.
        unsafe { pg_diff_oc_repeat(enc, s.as_ptr(), s.len() as i32, count, o, ol) }
    });
    let core = adt_oracle_compat::repeat(mcx, s, count);
    check_text("repeat", core, rc, &c_out);

    let core2 = adt_oracle_compat::repeat(mcx, s, count);
    let img = text_image(s);
    check_fc_text(
        "fc_repeat",
        fc_call(
            builtins::fc_repeat,
            mcx,
            0,
            [Datum::from_usize(img.as_ptr() as usize), Datum::from_i32(count)],
        ),
        &core2,
    );
}

// ---------------------------------------------------------------------------
// Arms 9/10/11: text_left / text_right / text_reverse.
// ---------------------------------------------------------------------------

fn text_leftright_diff(enc: pg_enc, payload: &[u8], leftarm: bool) {
    let (n, t) = take_i32(payload);
    let t = cap(t, TEXT_CAP);
    if leftarm
        && n >= 0
        && enc == PG_UTF8
        && !matches!(mbutils::pg_verify_mbstr(PG_UTF8, t, true), Ok(true))
    {
        return; // verified-text domain carve for text_left(n >= 0) — header
    }
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    let (rc, c_out) = c_text_call(t.len() + 8, |o, ol| {
        // SAFETY: out >= tlen per the entry contract.
        unsafe {
            pg_diff_oc_text_leftright(
                if leftarm { 0 } else { 1 },
                enc,
                t.as_ptr(),
                t.len() as i32,
                n,
                o,
                ol,
            )
        }
    });
    let core = if leftarm {
        adt_oracle_compat::text_left(mcx, t, n)
    } else {
        adt_oracle_compat::text_right(mcx, t, n)
    };
    check_text(if leftarm { "text_left" } else { "text_right" }, core, rc, &c_out);

    let core2 = if leftarm {
        adt_oracle_compat::text_left(mcx, t, n)
    } else {
        adt_oracle_compat::text_right(mcx, t, n)
    };
    let img = text_image(t);
    let fc = fc_call(
        if leftarm {
            builtins::fc_text_left
        } else {
            builtins::fc_text_right
        },
        mcx,
        0,
        [Datum::from_usize(img.as_ptr() as usize), Datum::from_i32(n)],
    );
    check_fc_text("fc_text_leftright", fc, &core2);
}

fn text_reverse_diff(enc: pg_enc, payload: &[u8]) {
    let t = cap(payload, TEXT_CAP);
    let ctx = MemoryContext::new("oraclefam_diff");
    let mcx = ctx.mcx();

    let (rc, c_out) = c_text_call(t.len() + 8, |o, ol| {
        // SAFETY: out >= tlen per the entry contract.
        unsafe { pg_diff_oc_text_reverse(enc, t.as_ptr(), t.len() as i32, o, ol) }
    });
    let core = adt_oracle_compat::text_reverse(mcx, t);
    check_text("text_reverse", core, rc, &c_out);

    let core2 = adt_oracle_compat::text_reverse(mcx, t);
    let img = text_image(t);
    check_fc_text(
        "fc_text_reverse",
        fc_call(
            builtins::fc_text_reverse,
            mcx,
            0,
            [Datum::from_usize(img.as_ptr() as usize)],
        ),
        &core2,
    );
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn oraclefam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&enc_sel, payload)) = rest.split_first() else {
        return;
    };
    let arm = sel % 12;
    // ascii/chr draw from the four-encoding grid (EUC_JP reject arms —
    // header note); every other arm stays on the three-encoding grid.
    let enc = if arm == 6 || arm == 7 {
        ENCS4[(enc_sel % 4) as usize]
    } else {
        ENCS[(enc_sel % 3) as usize]
    };
    // Rust-side encoding environment pin (the C side pins per entry).
    mbutils::SetDatabaseEncoding(enc).expect("grid encoding valid");
    match arm {
        0 => case_diff(enc, payload),
        1 => pad_diff(enc, payload, true),
        2 => pad_diff(enc, payload, false),
        3 => trim_diff(enc, payload),
        4 => byteatrim_diff(payload),
        5 => translate_diff(enc, payload),
        6 => ascii_diff(enc, payload),
        7 => chr_diff(enc, payload),
        8 => repeat_diff(enc, payload),
        9 => text_leftright_diff(enc, payload, true),
        10 => text_leftright_diff(enc, payload, false),
        _ => text_reverse_diff(enc, payload),
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (also the CI regression rail).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/oraclefam_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/oraclefam_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                oraclefam_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Deterministic smoke: every arm x every encoding over edge shapes
    /// (ok + error planes both driven).
    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        for enc_sel in 0u8..3 {
            for sel in 0u8..12 {
                oraclefam_diff(&[sel, enc_sel]);
                // generic text payloads through every arm
                for t in [
                    &b""[..],
                    b" ",
                    b"Hello, World 42!",
                    b"  padded  ",
                    b"a\x00b\x00",
                    "héllo wörld".as_bytes(),
                    "\u{10348}x\u{7ff}\u{800}".as_bytes(),
                    b"\xff\xfe\x80garbage",
                    b"\xc3", // truncated UTF8 tail
                ] {
                    let mut d = vec![sel, enc_sel];
                    // arm-appropriate scalar prefixes so text lands in the
                    // text slot: i32 + mode/flag bytes are consumed first.
                    d.extend_from_slice(&[7, 0, 0, 0, 0, 3]);
                    d.extend_from_slice(t);
                    oraclefam_diff(&d);
                }
            }
        }
        // case: interior NUL is a COMPARED case (truncation plane).
        for which in 0u8..4 {
            for enc_sel in 0u8..3 {
                let mut d = vec![0, enc_sel, which];
                d.extend_from_slice(b"miXed CASE\x00dropped tail");
                oraclefam_diff(&d);
                // invalid-collid arm: 42P22 verdict, entry + fc wrapper.
                let mut d = vec![0, enc_sel, which | 0x80];
                d.extend_from_slice(b"any text");
                oraclefam_diff(&d);
            }
        }
        // ascii/chr under EUC_JP (enc_sel % 4 == 3): the multibyte-non-UTF8
        // 54000 reject arms (character/value too large) plus the accept arm.
        for t in [&b"A"[..], b"\x80rest", b"\xff", b""] {
            let mut d = vec![6, 3];
            d.extend_from_slice(t);
            oraclefam_diff(&d);
        }
        for arg in [1i32, 127, 128, 255, 256, 0x7FFF_FFFF] {
            let mut d = vec![7, 3, 0];
            d.extend_from_slice(&arg.to_le_bytes());
            oraclefam_diff(&d);
        }
        // lpad/rpad: multi-char pad into width 7, len bands, empty pad.
        for sel in [1u8, 2] {
            for enc_sel in 0u8..3 {
                for (len, mode) in [(7i32, 0u8), (2, 0), (0, 0), (5, 1), (1, 2), (3, 3)] {
                    let mut d = vec![sel, enc_sel];
                    d.extend_from_slice(&len.to_le_bytes());
                    d.push(mode);
                    d.push(3); // l2
                    d.extend_from_slice(b"xyz");
                    d.extend_from_slice(b"hi");
                    oraclefam_diff(&d);
                    // empty pad string (len collapses to s1len)
                    let mut d = vec![sel, enc_sel];
                    d.extend_from_slice(&len.to_le_bytes());
                    d.push(mode);
                    d.push(0);
                    d.extend_from_slice(b"hi");
                    oraclefam_diff(&d);
                }
            }
        }
        // trims: repeated/multibyte set chars.
        for sel in [3u8, 4] {
            for enc_sel in 0u8..3 {
                for flags in 0u8..3 {
                    let mut d = vec![sel, enc_sel, flags, 4];
                    d.extend_from_slice(b"xy x");
                    d.extend_from_slice(b"xxhello worldyy  ");
                    oraclefam_diff(&d);
                    let mut d = vec![sel, enc_sel, flags, 4];
                    d.extend_from_slice("éa".as_bytes());
                    d.extend_from_slice("ééabcéé".as_bytes());
                    oraclefam_diff(&d);
                }
            }
        }
        // translate: delete arm (from longer than to), multibyte.
        for enc_sel in 0u8..3 {
            let mut d = vec![5, enc_sel, 3];
            d.extend_from_slice(b"abc");
            d.push(1);
            d.extend_from_slice(b"X");
            d.extend_from_slice(b"cabbage");
            oraclefam_diff(&d);
            let mut d = vec![5, enc_sel, 2];
            d.extend_from_slice("é".as_bytes());
            d.push(1);
            d.extend_from_slice(b"e");
            d.extend_from_slice("crémé".as_bytes());
            oraclefam_diff(&d);
        }
        // ascii: 0x80+ first bytes under all encodings.
        for enc_sel in 0u8..3 {
            for t in [&b"A"[..], b"", "é".as_bytes(), "\u{10348}".as_bytes(), b"\x80"] {
                let mut d = vec![6, enc_sel];
                d.extend_from_slice(t);
                oraclefam_diff(&d);
            }
        }
        // repeat: overflow band (54000), zero, negative.
        for enc_sel in 0u8..3 {
            for (mode, raw, s) in [
                (0u8, 5i32, &b"ab"[..]),
                (0, 0, b"ab"),
                (1, 0x7FFF_FFFF, b"ab"),
                (2, 7, b"ab"),
                (0, 5, b""),
            ] {
                let mut d = vec![8, enc_sel, mode];
                d.extend_from_slice(&raw.to_le_bytes());
                d.extend_from_slice(s);
                oraclefam_diff(&d);
            }
        }
        // text_left / text_right: n = INT32_MIN wrap arm, negatives, clips.
        for sel in [9u8, 10] {
            for enc_sel in 0u8..3 {
                for n in [i32::MIN, i32::MAX, -1, 0, 1, 3, 100, -100] {
                    let mut d = vec![sel, enc_sel];
                    d.extend_from_slice(&n.to_le_bytes());
                    d.extend_from_slice("abédef".as_bytes());
                    oraclefam_diff(&d);
                }
            }
        }
    }

    /// chr deterministic sweep: the full boundary band -2..=0x120000 (all
    /// UTF8 length steps, the surrogate band, U+10FFFF/0x110000, the
    /// single-byte 127/128/255/256 edges) x 4 encodings (incl. the EUC_JP
    /// multibyte-non-UTF8 reject arm), all planes, every cargo test. The
    /// FULL i32 domain runs under ORACLE_EXHAUSTIVE=1 (fleet; core+C
    /// planes — the fc wrapper adds no chr logic).
    #[test]
    fn chr_boundary_sweep() {
        let _serial = crate::c_oracle_serial();
        for &enc in &ENCS4 {
            let mut arg: i64 = -2;
            while arg <= 0x12_0000 {
                let ctx = MemoryContext::new("oraclefam_chr_sweep");
                let mcx = ctx.mcx();
                for _ in 0..4096 {
                    if arg > 0x12_0000 {
                        break;
                    }
                    chr_case_full(enc, arg as i32, mcx);
                    arg += 1;
                }
            }
            // high stragglers
            for a in [0x0020_0000, 0x7FFF_FFFF, i32::MIN as i64, -1] {
                let ctx = MemoryContext::new("oraclefam_chr_sweep");
                chr_case_full(enc, a as i32, ctx.mcx());
            }
        }
    }

    /// Full-i32 chr sweep (fleet-scale; ~2^32 x 3 cells): ORACLE_EXHAUSTIVE=1.
    #[test]
    fn chr_exhaustive_full_i32() {
        let _serial = crate::c_oracle_serial();
        if std::env::var_os("ORACLE_EXHAUSTIVE").map_or(true, |v| v != "1") {
            eprintln!("chr_exhaustive_full_i32: skipped (set ORACLE_EXHAUSTIVE=1)");
            return;
        }
        for &enc in &ENCS4 {
            let mut arg: i64 = i32::MIN as i64;
            while arg <= i32::MAX as i64 {
                let ctx = MemoryContext::new("oraclefam_chr_exhaustive");
                let mcx = ctx.mcx();
                for _ in 0..65536 {
                    if arg > i32::MAX as i64 {
                        break;
                    }
                    chr_case_core(enc, arg as i32, mcx);
                    arg += 1;
                }
            }
        }
    }
}
