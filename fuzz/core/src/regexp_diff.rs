//! regexp_diff: differential fuzz driver — shipped Rust `adt_regexp` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_regexp_io.c + csrc/regexfam/ verbatim Spencer engine).
//! Crate under test: crates/backend/utils/adt/regexp.
//!
//! Comparison planes (float_in_diff conventions): value bytes / bool / i32 /
//! per-row string lists incl. NULL slots, error-verdict, and errcode/sqlstate
//! class (map in `err_class`; message text out of scope).  Scalar arms add
//! the fc-wrapper plane (wrapper == core over a native LocalFcinfo frame).
//!
//! PINS (both sides):
//!   - encoding: UTF-8 (`mbutils::SetDatabaseEncoding(PG_UTF8)` here; the C
//!     oracle resolves the encoding table to the UTF-8 row at pin time —
//!     csrc/regexfam/pg_regexfam_glue.c).  All text inputs are enforced
//!     valid UTF-8 and NUL-free (server text invariant), non-conforming
//!     payloads are out of domain and skipped.
//!   - collation: C_COLLATION_OID (950) — Rust cores take it as an argument,
//!     the C oracle pins PG_GET_COLLATION() to it, and the vendored
//!     regc_pg_locale.c therefore always selects PG_REGEX_STRATEGY_C.
//!   - engine: regex_engine GUC pinned to SPENCER every exec
//!     (`regexp_alt::set_regex_engine(REGEX_ENGINE_SPENCER)`), so
//!     `regexp_alt::dispatch` returns None and the shipped Spencer path runs
//!     on the Rust side.  VERIFIED at runtime, not assumed: the
//!     `spencer_path_pinned` test calls dispatch() directly after the pin
//!     and asserts None on an RE2-compatible pattern (this build DOES link
//!     libre2 — homebrew re2 is present — so without the pin auto-dispatch
//!     would take RE2; the shipped build.rs drops RE2 silently only when
//!     libre2 is absent, which is NOT the case here).  The RE2 tier and the
//!     engine-GUC dispatch are the lane's carve.
//!
//! CARVES (out of this lane's assertion scope):
//!   - compiled-pattern cache: the Rust side runs its real cache; the C
//!     oracle compiles per call.  No assertions on cache behavior.
//!   - regexp_alt / RE2 dispatch (pinned OFF, see above).
//!   - Spencer eval core internals (regex/regex_core): another lane owns its
//!     coverage; it still links/runs here and any wrapper-visible divergence
//!     (match spans, error codes) is reported, not suppressed.
//!   - fmgr/array/SRF result plumbing: C construct_md_array/accumArrayResult
//!     /SRF machinery are carved to flat (ptr,len,isnull) lists (the per-row
//!     SRF loop IS the per-match core loop, driven directly); Rust SRF fc
//!     wrappers (regexp_matches / regexp_split_to_table, fn_extra state
//!     machine) are exercised by the crate's own tests, not this target.
//!
//! Input layout: [selector][payload]; selector % 22 picks the arm.  Fields
//! are drawn by a cursor: `text(cap)` = 1..2 length bytes (mod cap+1), then
//! that many bytes (short reads allowed at end-of-payload); ints are 1 byte
//! small-biased (-3..=247 plus MIN/MAX/65536/1e6/256 sentinels); optional
//! trailing SQL args use prefix-presence (a count byte, matching SQL arity).
//! Caps: pattern <= 128 bytes (regex compile cost), subject <= 512,
//! replacement <= 256, flags <= 8.
//!   0 textregexeq    [oid 1254]  pat, s
//!   1 textregexne    [oid 1256]  pat, s
//!   2 texticregexeq  [oid 1238]  pat, s
//!   3 texticregexne  [oid 1239]  pat, s
//!   4 nameregexeq    [oid 79]    pat, s (name payload; fc plane when <64B)
//!   5 nameregexne    [oid 1252]  pat, s
//!   6 similar_escape [oid 1623]  bits(pat_null, esc_null), pat, esc
//!   7 similar_to_escape_1 [1987] pat
//!   8 similar_to_escape_2 [1986] pat, esc
//!   9 textregexsubstr [2073]     pat, s
//!  10 textregexreplace_noopt [2284] pat, s, r
//!  11 textregexreplace [2285]    pat, s, r, flags
//!  12 regexp_count [6254/55/56]  nopt(0..=2), pat, s, [start], [flags]
//!  13 regexp_instr [6257..6262]  nopt(0..=5), pat, s, [start],[n],[endopt],[flags],[subexpr]
//!  14 regexp_like  [6263/6264]   nopt(0..=1), pat, s, [flags]
//!  15 regexp_substr [6265..69]   nopt(0..=4), pat, s, [start],[n],[flags],[subexpr]
//!  16 regexp_match [3396/3397] (+regexp_matches core loop) bits(as_matches,
//!     has_flags), pat, s, [flags]
//!  17 regexp_split_to_array [2767/2768] nopt(0..=1), pat, s, [flags]
//!  18 textregexreplace_extended [6251/52/53] nopt(0..=2), pat, s, r,
//!     start, [n], [flags]
//!  19 nameicregexeq  [oid 1240]  pat, s (name payload; fc plane when <64B)
//!  20 nameicregexne  [oid 1241]  pat, s
//!  21 regexp_fixed_prefix [planner support, no SQL face] bits(bit0 =
//!     case_insensitive), pat — diffed against the verbatim C
//!     regexp_fixed_prefix + pg_regprefix (csrc/regexfam/regprefix.c,
//!     vendored verbatim); planes: (prefix bytes, exact flag) / NULL +
//!     verdict + errcode.
//!
//! SKIPPED rows (executable exceptions live in the arms/tests, not comments
//! only — the SRF carve above is enforced by the arm set itself):
//!   - 2763/2764 regexp_matches, 2765/2766 regexp_split_to_table: SRF fmgr
//!     machinery (fn_extra/multi-call state) — their per-row CORES are arm
//!     16/17's loops; the SRF shells are crate-test-covered.
//!   - 1656..1659 bpchar*: same cores as arms 2..5 (identical prosrc
//!     dispatch), covered through them.

use datum::{Datum, NullableDatum};
use types_core::C_COLLATION_OID;
use types_error::{
    PgError, ERRCODE_INVALID_ESCAPE_SEQUENCE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    // csrc/pg_regexp_io.c driver entries.  Nonzero return = errcode class
    // (see that file's header).  Out pointers/byte buffers live in the C TLS
    // arena and are valid until the next pg_diff_* call on this thread.
    fn pg_diff_parse_re_flags(
        opts: *const u8, optlen: i32, has_opts: i32,
        out_cflags: *mut i32, out_glob: *mut i32,
    ) -> i32;
    fn pg_diff_textregexeq(s: *const u8, slen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_textregexne(s: *const u8, slen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_texticregexeq(s: *const u8, slen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_texticregexne(s: *const u8, slen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_nameregexeq(n: *const u8, nlen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_nameregexne(n: *const u8, nlen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_nameicregexeq(n: *const u8, nlen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_nameicregexne(n: *const u8, nlen: i32, p: *const u8, plen: i32, out: *mut i32) -> i32;
    fn pg_diff_regexp_fixed_prefix(
        p: *const u8, plen: i32, case_insensitive: i32,
        out: *mut *const u8, outlen: *mut i32, out_exact: *mut i32, out_isnull: *mut i32,
    ) -> i32;
    fn pg_diff_similar_escape(
        pat: *const u8, patlen: i32, pat_isnull: i32,
        esc: *const u8, esclen: i32, has_esc: i32,
        out: *mut *const u8, outlen: *mut i32, out_isnull: *mut i32,
    ) -> i32;
    fn pg_diff_textregexsubstr(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        out: *mut *const u8, outlen: *mut i32, out_isnull: *mut i32,
    ) -> i32;
    fn pg_diff_textregexreplace_noopt(
        s: *const u8, slen: i32, p: *const u8, plen: i32, r: *const u8, rlen: i32,
        out: *mut *const u8, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_textregexreplace(
        s: *const u8, slen: i32, p: *const u8, plen: i32, r: *const u8, rlen: i32,
        o: *const u8, olen: i32,
        out: *mut *const u8, outlen: *mut i32,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_textregexreplace_extended(
        s: *const u8, slen: i32, p: *const u8, plen: i32, r: *const u8, rlen: i32,
        start: i32, has_start: i32, n: i32, has_n: i32,
        f: *const u8, flen: i32, has_flags: i32,
        out: *mut *const u8, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_regexp_count(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        start: i32, has_start: i32,
        f: *const u8, flen: i32, has_flags: i32,
        out: *mut i32,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_regexp_instr(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        start: i32, has_start: i32, n: i32, has_n: i32,
        endoption: i32, has_endoption: i32,
        f: *const u8, flen: i32, has_flags: i32,
        subexpr: i32, has_subexpr: i32,
        out: *mut i32,
    ) -> i32;
    fn pg_diff_regexp_like(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        f: *const u8, flen: i32, has_flags: i32,
        out: *mut i32,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_regexp_substr(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        start: i32, has_start: i32, n: i32, has_n: i32,
        f: *const u8, flen: i32, has_flags: i32,
        subexpr: i32, has_subexpr: i32,
        out: *mut *const u8, outlen: *mut i32, out_isnull: *mut i32,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_regexp_match(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        f: *const u8, flen: i32, has_flags: i32,
        as_matches: i32,
        out_nrows: *mut i32, out_ncols: *mut i32,
        out_ptrs: *mut *const *const u8, out_lens: *mut *const i32,
    ) -> i32;
    fn pg_diff_regexp_split(
        s: *const u8, slen: i32, p: *const u8, plen: i32,
        f: *const u8, flen: i32, has_flags: i32,
        out_n: *mut i32,
        out_ptrs: *mut *const *const u8, out_lens: *mut *const i32,
    ) -> i32;
}

const C: types_core::Oid = C_COLLATION_OID;
const PAT_CAP: usize = 128; // regex compile cost cap (module header)
const TXT_CAP: usize = 512;
const REPL_CAP: usize = 256;
const FLAGS_CAP: usize = 8;

/// Environment pin: UTF-8 encoding + Spencer engine, every exec (both are
/// cheap TLS writes).  See the module header for the runtime verification.
fn pin() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("PG_UTF8 pin");
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_SPENCER);
    // varlena::replace_regexp reaches the regexp engine through the
    // circular-dep-breaking seams; install the shipped implementations once
    // per process (exactly what server startup does; seam slots are global
    // and set() panics on a second install).  CHECK_FOR_INTERRUPTS is the
    // sanctioned no-op seam (no signal sources in-harness — the same
    // CANCEL_REQUESTED=false pin as the C oracle); is_installed() tolerates
    // the other p1 lanes' oracles installing the identical no-op first.
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        // CROSS-FAMILY DOUBLE INSTALL (the datetime_io_diff 2026-08-03
        // class): regex_diff's init installs the identical shipped
        // regex_core impls; first-wins via catch_unwind (the
        // name_diff/arrayfuncs convention) so the loser's Once is never
        // poisoned.
        let _ = std::panic::catch_unwind(adt_regexp::init_seams);
        let _ = std::panic::catch_unwind(regex_core::init_seams);
        if !postgres_seams::check_for_interrupts::is_installed() {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        }
    });
}

/// Map a Rust-side PgError sqlstate to the C oracle's errcode class
/// (csrc/pg_regexp_io.c header).  0 = a sqlstate this family never raises
/// (always a divergence).
fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_REGULAR_EXPRESSION {
        1
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        2
    } else if e.sqlstate == ERRCODE_INVALID_ESCAPE_SEQUENCE {
        3
    } else if e.sqlstate == ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER {
        4
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        5
    } else if e.sqlstate == types_error::ERRCODE_INTERNAL_ERROR {
        6
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Payload cursor (module-header grammar)
// ---------------------------------------------------------------------------

struct Cur<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn new(d: &'a [u8]) -> Self {
        Cur { d, i: 0 }
    }

    fn byte(&mut self) -> u8 {
        let b = self.d.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        b
    }

    /// Length-prefixed field: 1 length byte for caps < 256, else 2 (LE),
    /// reduced mod cap+1; short reads at end-of-payload allowed.
    fn field(&mut self, cap: usize) -> &'a [u8] {
        let want = if cap < 256 {
            (self.byte() as usize) % (cap + 1)
        } else {
            let lo = self.byte() as usize;
            let hi = self.byte() as usize;
            (lo | (hi << 8)) % (cap + 1)
        };
        // byte() advances past end-of-payload (returning 0), so clamp the
        // field start to the buffer.
        let start = self.i.min(self.d.len());
        let take = want.min(self.d.len() - start);
        let f = &self.d[start..start + take];
        self.i = start + take;
        f
    }

    /// Small-biased i32 (module-header table).
    fn int(&mut self) -> i32 {
        match self.byte() {
            255 => i32::MAX,
            254 => i32::MIN,
            253 => 65536,
            252 => 1_000_000,
            251 => 256,
            b => b as i32 - 3,
        }
    }
}

/// Server text invariant: valid UTF-8 and NUL-free, or out of domain.
fn text_ok(b: &[u8]) -> bool {
    !b.contains(&0) && core::str::from_utf8(b).is_ok()
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; verbatim from uuid_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over the given args at the pinned C collation;
/// returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> (types_error::PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(C);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    let r = f(None, &mut fcinfo);
    let isnull = fcinfo.isnull;
    (r, isnull)
}

/// text/varlena arg construction: inline 4B-uncompressed header + body
/// (the shipped set_varsize_4b_word encoding; bodies are capped well below
/// the length limit).  Verbatim pattern from name_diff.rs / quote_diff.rs.
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

fn text_arg(img: &[u8]) -> NullableDatum {
    NullableDatum::value(Datum::from_usize(img.as_ptr() as usize))
}

/// Varlena result readback (text payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// Assert the fc-wrapper plane agrees with an already-C-checked core result.
/// `core`: Ok(Some(bytes)) value / Ok(None) SQL NULL / Err(class).
fn fc_expect_text(
    arm: &str,
    got: (types_error::PgResult<Datum>, bool),
    core: &Result<Option<Vec<u8>>, i32>,
) {
    match (got, core) {
        ((Ok(d), false), Ok(Some(bytes))) => {
            assert!(
                read_varlena_data(d) == &bytes[..],
                "{arm}: fc wrapper text differs from core"
            );
        }
        ((Ok(_), true), Ok(None)) => {}
        ((Err(e), _), Err(class)) => {
            assert!(
                err_class(&e) == *class,
                "{arm}: fc wrapper sqlstate class {} != core {class}",
                err_class(&e)
            );
        }
        ((r, isnull), _) => panic!(
            "{arm}: fc wrapper verdict (ok={}, isnull={isnull}) differs from core {:?}",
            r.is_ok(),
            core.as_ref().map(|o| o.is_some())
        ),
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn regexp_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    pin();
    match sel % 22 {
        0 => textre_bool_diff(payload, BoolArm::TextEq),
        1 => textre_bool_diff(payload, BoolArm::TextNe),
        2 => textre_bool_diff(payload, BoolArm::TextIcEq),
        3 => textre_bool_diff(payload, BoolArm::TextIcNe),
        4 => textre_bool_diff(payload, BoolArm::NameEq),
        5 => textre_bool_diff(payload, BoolArm::NameNe),
        6 => similar_escape_diff(payload),
        7 => similar_to_escape_1_diff(payload),
        8 => similar_to_escape_2_diff(payload),
        9 => textregexsubstr_diff(payload),
        10 => textregexreplace_noopt_diff(payload),
        11 => textregexreplace_diff(payload),
        12 => regexp_count_diff(payload),
        13 => regexp_instr_diff(payload),
        14 => regexp_like_diff(payload),
        15 => regexp_substr_diff(payload),
        16 => regexp_match_diff(payload),
        17 => regexp_split_diff(payload),
        18 => textregexreplace_extended_diff(payload),
        19 => textre_bool_diff(payload, BoolArm::NameIcEq),
        20 => textre_bool_diff(payload, BoolArm::NameIcNe),
        _ => regexp_fixed_prefix_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arms 0..5: boolean match operators (oids 1254/1256/1238/1239/79/1252).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum BoolArm {
    TextEq,
    TextNe,
    TextIcEq,
    TextIcNe,
    NameEq,
    NameNe,
    NameIcEq,
    NameIcNe,
}

fn textre_bool_diff(payload: &[u8], arm: BoolArm) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    if !text_ok(pat) || !text_ok(s) {
        return;
    }

    // C oracle.
    let mut cb: i32 = -1;
    let cst = unsafe {
        match arm {
            BoolArm::TextEq => pg_diff_textregexeq(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::TextNe => pg_diff_textregexne(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::TextIcEq => pg_diff_texticregexeq(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::TextIcNe => pg_diff_texticregexne(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::NameEq => pg_diff_nameregexeq(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::NameNe => pg_diff_nameregexne(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::NameIcEq => pg_diff_nameicregexeq(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
            BoolArm::NameIcNe => pg_diff_nameicregexne(s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32, &mut cb),
        }
    };

    // Shipped Rust core.
    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let name = |a: BoolArm| match a {
        BoolArm::TextEq => "textregexeq",
        BoolArm::TextNe => "textregexne",
        BoolArm::TextIcEq => "texticregexeq",
        BoolArm::TextIcNe => "texticregexne",
        BoolArm::NameEq => "nameregexeq",
        BoolArm::NameNe => "nameregexne",
        BoolArm::NameIcEq => "nameicregexeq",
        BoolArm::NameIcNe => "nameicregexne",
    };
    let r = match arm {
        BoolArm::TextEq => adt_regexp::textregexeq(m, s, pat, C),
        BoolArm::TextNe => adt_regexp::textregexne(m, s, pat, C),
        BoolArm::TextIcEq => adt_regexp::texticregexeq(m, s, pat, C),
        BoolArm::TextIcNe => adt_regexp::texticregexne(m, s, pat, C),
        BoolArm::NameEq => adt_regexp::nameregexeq(m, s, pat, C),
        BoolArm::NameNe => adt_regexp::nameregexne(m, s, pat, C),
        BoolArm::NameIcEq => adt_regexp::nameicregexeq(m, s, pat, C),
        BoolArm::NameIcNe => adt_regexp::nameicregexne(m, s, pat, C),
    };

    let core: Result<bool, i32> = match (cst, &r) {
        (0, Ok(b)) => {
            assert!(
                *b == (cb != 0),
                "{}: value DIVERGENCE pat={pat:?} s={s:?}: C={cb} Rust={b}",
                name(arm)
            );
            Ok(*b)
        }
        (code, Err(e)) if code != 0 => {
            assert!(
                err_class(e) == code,
                "{}: sqlstate DIVERGENCE pat={pat:?} s={s:?}: C class {code} Rust {} ({:?})",
                name(arm),
                err_class(e),
                e.sqlstate
            );
            Err(code)
        }
        _ => panic!(
            "{}: verdict DIVERGENCE pat={pat:?} s={s:?}: C status {cst} vs Rust ok={}",
            name(arm),
            r.is_ok()
        ),
    };

    // fc-wrapper plane.  Name arms need the payload to fit a NameData block.
    let is_name = matches!(
        arm,
        BoolArm::NameEq | BoolArm::NameNe | BoolArm::NameIcEq | BoolArm::NameIcNe
    );
    if is_name && s.len() >= 64 {
        return;
    }
    let simg = text_image(s);
    let pimg = text_image(pat);
    let mut nbuf = [0u8; 64];
    let sarg = if is_name {
        nbuf[..s.len()].copy_from_slice(s);
        NullableDatum::value(Datum::from_usize(nbuf.as_ptr() as usize))
    } else {
        text_arg(&simg)
    };
    let f: PGFunction = match arm {
        BoolArm::TextEq => adt_regexp::builtins::fc_textregexeq,
        BoolArm::TextNe => adt_regexp::builtins::fc_textregexne,
        BoolArm::TextIcEq => adt_regexp::builtins::fc_texticregexeq,
        BoolArm::TextIcNe => adt_regexp::builtins::fc_texticregexne,
        BoolArm::NameEq => adt_regexp::builtins::fc_nameregexeq,
        BoolArm::NameNe => adt_regexp::builtins::fc_nameregexne,
        BoolArm::NameIcEq => adt_regexp::builtins::fc_nameicregexeq,
        BoolArm::NameIcNe => adt_regexp::builtins::fc_nameicregexne,
    };
    match (fc_call::<2>(f, m, [sarg, text_arg(&pimg)]).0, &core) {
        (Ok(d), Ok(b)) => assert!(
            d.as_bool() == *b,
            "{}: fc wrapper bool differs from core (pat={pat:?} s={s:?})",
            name(arm)
        ),
        (Err(e), Err(class)) => assert!(
            err_class(&e) == *class,
            "{}: fc wrapper sqlstate class differs from core",
            name(arm)
        ),
        (r, _) => panic!("{}: fc wrapper verdict ok={} differs from core", name(arm), r.is_ok()),
    }
}

// ---------------------------------------------------------------------------
// Arms 6..8: similar_escape family (oids 1623/1987/1986).
// ---------------------------------------------------------------------------

fn similar_escape_c(
    pat: Option<&[u8]>,
    esc: Option<&[u8]>,
) -> Result<Option<Vec<u8>>, i32> {
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let mut isnull: i32 = 0;
    let (pp, pl, pn) = match pat {
        Some(p) => (p.as_ptr(), p.len() as i32, 0),
        None => (core::ptr::null(), 0, 1),
    };
    let (ep, el, eh) = match esc {
        Some(e) => (e.as_ptr(), e.len() as i32, 1),
        None => (core::ptr::null(), 0, 0),
    };
    let cst = unsafe {
        pg_diff_similar_escape(pp, pl, pn, ep, el, eh, &mut out, &mut outlen, &mut isnull)
    };
    if cst != 0 {
        return Err(cst);
    }
    if isnull != 0 {
        return Ok(None);
    }
    // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
    Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
}

/// None = KNOWN-DIVERGENCE executable exception fired (skip further planes).
fn compare_text_result(
    arm: &str,
    pat: &[u8],
    cres: Result<Option<Vec<u8>>, i32>,
    rres: types_error::PgResult<Option<Vec<u8>>>,
) -> Option<Result<Option<Vec<u8>>, i32>> {
    match (cres, rres) {
        (Ok(c), Ok(r)) => {
            assert!(
                c == r,
                "{arm}: value DIVERGENCE pat={pat:?}: C={c:?} Rust={r:?}"
            );
            Some(Ok(c))
        }
        (Err(code), Err(e)) => {
            assert!(
                err_class(&e) == code,
                "{arm}: sqlstate DIVERGENCE pat={pat:?}: C class {code} Rust {} ({:?})",
                err_class(&e),
                e.sqlstate
            );
            Some(Err(code))
        }
        (c, r) => panic!(
            "{arm}: verdict DIVERGENCE pat={pat:?}: C ok={} Rust ok={}",
            c.is_ok(),
            r.is_ok()
        ),
    }
}

fn similar_escape_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let bits = cur.byte();
    let pat_null = bits & 1 != 0;
    let esc_null = bits & 2 != 0;
    let pat = cur.field(PAT_CAP);
    let esc = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(esc) {
        return;
    }
    let patq = if pat_null { None } else { Some(pat) };
    let escq = if esc_null { None } else { Some(esc) };

    let cres = similar_escape_c(patq, escq);

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::similar_escape(m, patq, escq)
        .map(|o| o.map(|v| v.as_slice().to_vec()));
    let Some(core) = compare_text_result("similar_escape", pat, cres, rres) else { return; };

    // fc plane (non-strict wrapper: NULL args in-domain).
    let pimg = text_image(pat);
    let eimg = text_image(esc);
    let parg = if pat_null { NullableDatum::null() } else { text_arg(&pimg) };
    let earg = if esc_null { NullableDatum::null() } else { text_arg(&eimg) };
    let got = fc_call::<2>(adt_regexp::builtins::fc_similar_escape, m, [parg, earg]);
    fc_expect_text("similar_escape", got, &core);
}

fn similar_to_escape_1_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    if !text_ok(pat) {
        return;
    }
    let cres = similar_escape_c(Some(pat), None);
    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::similar_to_escape_1(m, pat).map(|v| Some(v.as_slice().to_vec()));
    let Some(core) = compare_text_result("similar_to_escape_1", pat, cres, rres) else { return; };

    let pimg = text_image(pat);
    let got = fc_call::<1>(adt_regexp::builtins::fc_similar_to_escape_1, m, [text_arg(&pimg)]);
    fc_expect_text("similar_to_escape_1", got, &core);
}

fn similar_to_escape_2_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    let esc = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(esc) {
        return;
    }
    let cres = similar_escape_c(Some(pat), Some(esc));
    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::similar_to_escape_2(m, pat, esc).map(|v| Some(v.as_slice().to_vec()));
    let Some(core) = compare_text_result("similar_to_escape_2", pat, cres, rres) else { return; };

    let pimg = text_image(pat);
    let eimg = text_image(esc);
    let got = fc_call::<2>(
        adt_regexp::builtins::fc_similar_to_escape_2,
        m,
        [text_arg(&pimg), text_arg(&eimg)],
    );
    fc_expect_text("similar_to_escape_2", got, &core);
}

// ---------------------------------------------------------------------------
// Arm 9: textregexsubstr (oid 2073).
// ---------------------------------------------------------------------------

fn textregexsubstr_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    if !text_ok(pat) || !text_ok(s) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let mut isnull: i32 = 0;
    let cst = unsafe {
        pg_diff_textregexsubstr(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            &mut out, &mut outlen, &mut isnull,
        )
    };
    let cres: Result<Option<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else if isnull != 0 {
        Ok(None)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::textregexsubstr(m, s, pat, C).map(|o| o.map(|v| v.as_slice().to_vec()));
    let Some(core) = compare_text_result("textregexsubstr", pat, cres, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let got = fc_call::<2>(
        adt_regexp::builtins::fc_textregexsubstr,
        m,
        [text_arg(&simg), text_arg(&pimg)],
    );
    fc_expect_text("textregexsubstr", got, &core);
}

// ---------------------------------------------------------------------------
// Arms 10/11/18: regexp_replace family (oids 2284/2285/6251..6253).
// ---------------------------------------------------------------------------

fn textregexreplace_noopt_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let r = cur.field(REPL_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(r) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let cst = unsafe {
        pg_diff_textregexreplace_noopt(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            r.as_ptr(), r.len() as i32, &mut out, &mut outlen,
        )
    };
    let cres: Result<Option<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres =
        adt_regexp::textregexreplace_noopt(m, s, pat, r, C).map(|v| Some(v.as_slice().to_vec()));
    let Some(core) = compare_text_result("textregexreplace_noopt", pat, cres, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let rimg = text_image(r);
    let got = fc_call::<3>(
        adt_regexp::builtins::fc_textregexreplace_noopt,
        m,
        [text_arg(&simg), text_arg(&pimg), text_arg(&rimg)],
    );
    fc_expect_text("textregexreplace_noopt", got, &core);
}

fn textregexreplace_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let r = cur.field(REPL_CAP);
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(r) || !text_ok(flags) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let cst = unsafe {
        pg_diff_textregexreplace(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            r.as_ptr(), r.len() as i32, flags.as_ptr(), flags.len() as i32,
            &mut out, &mut outlen,
        )
    };
    let cres: Result<Option<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres =
        adt_regexp::textregexreplace(m, s, pat, r, flags, C).map(|v| Some(v.as_slice().to_vec()));
    let Some(core) = compare_text_result("textregexreplace", pat, cres, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let rimg = text_image(r);
    let fimg = text_image(flags);
    let got = fc_call::<4>(
        adt_regexp::builtins::fc_textregexreplace,
        m,
        [text_arg(&simg), text_arg(&pimg), text_arg(&rimg), text_arg(&fimg)],
    );
    fc_expect_text("textregexreplace", got, &core);
}

fn textregexreplace_extended_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    // SQL faces: (s,p,r,start) / (s,p,r,start,n) / (s,p,r,start,n,flags)
    let nopt = (cur.byte() % 3) as usize;
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let r = cur.field(REPL_CAP);
    let start = cur.int();
    let n = cur.int();
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(r) || !text_ok(flags) {
        return;
    }
    let has_n = nopt >= 1;
    let has_flags = nopt >= 2;

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let cst = unsafe {
        pg_diff_textregexreplace_extended(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            r.as_ptr(), r.len() as i32,
            start, 1, n, has_n as i32,
            flags.as_ptr(), flags.len() as i32, has_flags as i32,
            &mut out, &mut outlen,
        )
    };
    let cres: Result<Option<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::textregexreplace_extended(
        m,
        s,
        pat,
        r,
        Some(start),
        has_n.then_some(n),
        has_flags.then_some(flags),
        C,
    )
    .map(|v| Some(v.as_slice().to_vec()));
    let Some(core) = compare_text_result("textregexreplace_extended", pat, cres, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let rimg = text_image(r);
    let fimg = text_image(flags);
    let got = match nopt {
        0 => fc_call::<4>(
            adt_regexp::builtins::fc_textregexreplace_extended_no_n,
            m,
            [text_arg(&simg), text_arg(&pimg), text_arg(&rimg),
             NullableDatum::value(Datum::from_i32(start))],
        ),
        1 => fc_call::<5>(
            adt_regexp::builtins::fc_textregexreplace_extended_no_flags,
            m,
            [text_arg(&simg), text_arg(&pimg), text_arg(&rimg),
             NullableDatum::value(Datum::from_i32(start)),
             NullableDatum::value(Datum::from_i32(n))],
        ),
        _ => fc_call::<6>(
            adt_regexp::builtins::fc_textregexreplace_extended,
            m,
            [text_arg(&simg), text_arg(&pimg), text_arg(&rimg),
             NullableDatum::value(Datum::from_i32(start)),
             NullableDatum::value(Datum::from_i32(n)), text_arg(&fimg)],
        ),
    };
    fc_expect_text("textregexreplace_extended", got, &core);
}

// ---------------------------------------------------------------------------
// Arms 12..15: regexp_count / regexp_instr / regexp_like / regexp_substr.
// ---------------------------------------------------------------------------

/// None = KNOWN-DIVERGENCE executable exception fired (skip further planes).
fn compare_i32_result(
    arm: &str,
    pat: &[u8],
    s: &[u8],
    cst: i32,
    cval: i32,
    rres: types_error::PgResult<i32>,
) -> Option<Result<i32, i32>> {
    match (cst, rres) {
        (0, Ok(v)) => {
            assert!(
                v == cval,
                "{arm}: value DIVERGENCE pat={pat:?} s={s:?}: C={cval} Rust={v}"
            );
            Some(Ok(v))
        }
        (code, Err(e)) if code != 0 => {
            assert!(
                err_class(&e) == code,
                "{arm}: sqlstate DIVERGENCE pat={pat:?} s={s:?}: C class {code} Rust {} ({:?})",
                err_class(&e),
                e.sqlstate
            );
            Some(Err(code))
        }
        (cst, r) => panic!(
            "{arm}: verdict DIVERGENCE pat={pat:?} s={s:?}: C status {cst} vs Rust ok={}",
            r.is_ok()
        ),
    }
}

fn fc_expect_i32(arm: &str, got: (types_error::PgResult<Datum>, bool), core: &Result<i32, i32>) {
    match (got.0, core) {
        (Ok(d), Ok(v)) => assert!(
            d.as_i32() == *v,
            "{arm}: fc wrapper i32 {} differs from core {v}",
            d.as_i32()
        ),
        (Err(e), Err(class)) => assert!(
            err_class(&e) == *class,
            "{arm}: fc wrapper sqlstate class differs from core"
        ),
        (r, _) => panic!("{arm}: fc wrapper verdict ok={} differs from core", r.is_ok()),
    }
}

fn regexp_count_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let nopt = (cur.byte() % 3) as usize; // 0: none, 1: start, 2: start+flags
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let start = cur.int();
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }
    let has_start = nopt >= 1;
    let has_flags = nopt >= 2;

    let mut cval: i32 = -1;
    let cst = unsafe {
        pg_diff_regexp_count(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            start, has_start as i32,
            flags.as_ptr(), flags.len() as i32, has_flags as i32,
            &mut cval,
        )
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::matches::regexp_count(
        m, s, pat, has_start.then_some(start), has_flags.then_some(flags), C,
    );
    let Some(core) = compare_i32_result("regexp_count", pat, s, cst, cval, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let fimg = text_image(flags);
    let got = match nopt {
        0 => fc_call::<2>(
            adt_regexp::builtins::fc_regexp_count_no_start,
            m,
            [text_arg(&simg), text_arg(&pimg)],
        ),
        1 => fc_call::<3>(
            adt_regexp::builtins::fc_regexp_count_no_flags,
            m,
            [text_arg(&simg), text_arg(&pimg), NullableDatum::value(Datum::from_i32(start))],
        ),
        _ => fc_call::<4>(
            adt_regexp::builtins::fc_regexp_count,
            m,
            [text_arg(&simg), text_arg(&pimg),
             NullableDatum::value(Datum::from_i32(start)), text_arg(&fimg)],
        ),
    };
    fc_expect_i32("regexp_count", got, &core);
}

fn regexp_instr_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let nopt = (cur.byte() % 6) as usize; // prefix-presence over 5 optionals
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let start = cur.int();
    let n = cur.int();
    let endoption = cur.int();
    let flags = cur.field(FLAGS_CAP);
    let subexpr = cur.int();
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }

    let mut cval: i32 = -1;
    let cst = unsafe {
        pg_diff_regexp_instr(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            start, (nopt >= 1) as i32, n, (nopt >= 2) as i32,
            endoption, (nopt >= 3) as i32,
            flags.as_ptr(), flags.len() as i32, (nopt >= 4) as i32,
            subexpr, (nopt >= 5) as i32,
            &mut cval,
        )
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::matches::regexp_instr(
        m,
        s,
        pat,
        (nopt >= 1).then_some(start),
        (nopt >= 2).then_some(n),
        (nopt >= 3).then_some(endoption),
        (nopt >= 4).then_some(flags),
        (nopt >= 5).then_some(subexpr),
        C,
    );
    let Some(core) = compare_i32_result("regexp_instr", pat, s, cst, cval, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let fimg = text_image(flags);
    let iv = |x: i32| NullableDatum::value(Datum::from_i32(x));
    let got = match nopt {
        0 => fc_call::<2>(
            adt_regexp::builtins::fc_regexp_instr_no_start,
            m,
            [text_arg(&simg), text_arg(&pimg)],
        ),
        1 => fc_call::<3>(
            adt_regexp::builtins::fc_regexp_instr_no_n,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start)],
        ),
        2 => fc_call::<4>(
            adt_regexp::builtins::fc_regexp_instr_no_endoption,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n)],
        ),
        3 => fc_call::<5>(
            adt_regexp::builtins::fc_regexp_instr_no_flags,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n), iv(endoption)],
        ),
        4 => fc_call::<6>(
            adt_regexp::builtins::fc_regexp_instr_no_subexpr,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n), iv(endoption), text_arg(&fimg)],
        ),
        _ => fc_call::<7>(
            adt_regexp::builtins::fc_regexp_instr,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n), iv(endoption),
             text_arg(&fimg), iv(subexpr)],
        ),
    };
    fc_expect_i32("regexp_instr", got, &core);
}

fn regexp_like_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let nopt = (cur.byte() % 2) as usize;
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }
    let has_flags = nopt >= 1;

    let mut cval: i32 = -1;
    let cst = unsafe {
        pg_diff_regexp_like(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            flags.as_ptr(), flags.len() as i32, has_flags as i32,
            &mut cval,
        )
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres =
        adt_regexp::matches::regexp_like(m, s, pat, has_flags.then_some(flags), C);
    let Some(core): Option<Result<i32, i32>> = compare_i32_result(
        "regexp_like",
        pat,
        s,
        cst,
        cval,
        rres.map(|b| b as i32),
    ) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let fimg = text_image(flags);
    let got = if has_flags {
        fc_call::<3>(
            adt_regexp::builtins::fc_regexp_like,
            m,
            [text_arg(&simg), text_arg(&pimg), text_arg(&fimg)],
        )
    } else {
        fc_call::<2>(
            adt_regexp::builtins::fc_regexp_like_no_flags,
            m,
            [text_arg(&simg), text_arg(&pimg)],
        )
    };
    match (got.0, &core) {
        (Ok(d), Ok(v)) => assert!(
            d.as_bool() as i32 == *v,
            "regexp_like: fc wrapper bool differs from core"
        ),
        (Err(e), Err(class)) => assert!(
            err_class(&e) == *class,
            "regexp_like: fc wrapper sqlstate class differs from core"
        ),
        (r, _) => panic!("regexp_like: fc wrapper verdict ok={} differs from core", r.is_ok()),
    }
}

fn regexp_substr_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let nopt = (cur.byte() % 5) as usize; // start, n, flags, subexpr
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let start = cur.int();
    let n = cur.int();
    let flags = cur.field(FLAGS_CAP);
    let subexpr = cur.int();
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let mut isnull: i32 = 0;
    let cst = unsafe {
        pg_diff_regexp_substr(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            start, (nopt >= 1) as i32, n, (nopt >= 2) as i32,
            flags.as_ptr(), flags.len() as i32, (nopt >= 3) as i32,
            subexpr, (nopt >= 4) as i32,
            &mut out, &mut outlen, &mut isnull,
        )
    };
    let cres: Result<Option<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else if isnull != 0 {
        Ok(None)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some(unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec()))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::matches::regexp_substr(
        m,
        s,
        pat,
        (nopt >= 1).then_some(start),
        (nopt >= 2).then_some(n),
        (nopt >= 3).then_some(flags),
        (nopt >= 4).then_some(subexpr),
        C,
    )
    .map(|o| o.map(|v| v.as_slice().to_vec()));
    let Some(core) = compare_text_result("regexp_substr", pat, cres, rres) else { return; };

    let simg = text_image(s);
    let pimg = text_image(pat);
    let fimg = text_image(flags);
    let iv = |x: i32| NullableDatum::value(Datum::from_i32(x));
    let got = match nopt {
        0 => fc_call::<2>(
            adt_regexp::builtins::fc_regexp_substr_no_start,
            m,
            [text_arg(&simg), text_arg(&pimg)],
        ),
        1 => fc_call::<3>(
            adt_regexp::builtins::fc_regexp_substr_no_n,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start)],
        ),
        2 => fc_call::<4>(
            adt_regexp::builtins::fc_regexp_substr_no_flags,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n)],
        ),
        3 => fc_call::<5>(
            adt_regexp::builtins::fc_regexp_substr_no_subexpr,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n), text_arg(&fimg)],
        ),
        _ => fc_call::<6>(
            adt_regexp::builtins::fc_regexp_substr,
            m,
            [text_arg(&simg), text_arg(&pimg), iv(start), iv(n), text_arg(&fimg), iv(subexpr)],
        ),
    };
    fc_expect_text("regexp_substr", got, &core);
}

// ---------------------------------------------------------------------------
// Arm 16: regexp_match core (oid 3397) + regexp_matches per-row core loop.
// SRF fc wrappers are carved (fmgr machinery; module header).
// ---------------------------------------------------------------------------

fn regexp_match_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let bits = cur.byte();
    let as_matches = bits & 1 != 0;
    let has_flags = bits & 2 != 0;
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }

    let mut nrows: i32 = 0;
    let mut ncols: i32 = 0;
    let mut ptrs: *const *const u8 = core::ptr::null();
    let mut lens: *const i32 = core::ptr::null();
    let cst = unsafe {
        pg_diff_regexp_match(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            flags.as_ptr(), flags.len() as i32, has_flags as i32,
            as_matches as i32,
            &mut nrows, &mut ncols, &mut ptrs, &mut lens,
        )
    };
    let cres: Result<Vec<Vec<Option<Vec<u8>>>>, i32> = if cst != 0 {
        Err(cst)
    } else {
        let mut rows = Vec::with_capacity(nrows as usize);
        for row in 0..nrows as usize {
            let mut cols = Vec::with_capacity(ncols as usize);
            for i in 0..ncols as usize {
                let idx = row * ncols as usize + i;
                // SAFETY: idx < nrows*ncols arena arrays; copied before the
                // next pg_diff_* call.
                let (p, l) = unsafe { (*ptrs.add(idx), *lens.add(idx)) };
                if l < 0 {
                    cols.push(None);
                } else {
                    // SAFETY: as above.
                    cols.push(Some(
                        unsafe { core::slice::from_raw_parts(p, l as usize) }.to_vec(),
                    ));
                }
            }
            rows.push(cols);
        }
        Ok(rows)
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres: types_error::PgResult<Vec<Vec<Option<Vec<u8>>>>> = (|| {
        let fl = has_flags.then_some(flags);
        let mut rows = Vec::new();
        if as_matches {
            let mut ctx = adt_regexp::matches::regexp_matches_setup(m, s, pat, fl, C)?;
            while ctx.next_match < ctx.nmatches {
                let mut row = Vec::with_capacity(ctx.npatterns as usize);
                adt_regexp::matches::build_regexp_match_result(&ctx, |e| {
                    row.push(e.map(|v| v.as_slice().to_vec()));
                    Ok(())
                })?;
                rows.push(row);
                ctx.next_match += 1;
            }
        } else if let Some(ctx) = adt_regexp::matches::regexp_match(m, s, pat, fl, C)? {
            let mut row = Vec::with_capacity(ctx.npatterns as usize);
            adt_regexp::matches::build_regexp_match_result(&ctx, |e| {
                row.push(e.map(|v| v.as_slice().to_vec()));
                Ok(())
            })?;
            rows.push(row);
        }
        Ok(rows)
    })();

    match (cres, rres) {
        (Ok(c), Ok(r)) => assert!(
            c == r,
            "regexp_match(as_matches={as_matches}): value DIVERGENCE pat={pat:?} s={s:?}: C={c:?} Rust={r:?}"
        ),
        (Err(code), Err(e)) => assert!(
            err_class(&e) == code,
            "regexp_match: sqlstate DIVERGENCE pat={pat:?}: C class {code} Rust {} ({:?})",
            err_class(&e),
            e.sqlstate
        ),
        (c, r) => panic!(
            "regexp_match: verdict DIVERGENCE pat={pat:?} s={s:?}: C ok={} Rust ok={}",
            c.is_ok(),
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 17: regexp_split_to_array core (oid 2768) — the split result-glue loop.
// ---------------------------------------------------------------------------

fn regexp_split_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let nopt = (cur.byte() % 2) as usize;
    let pat = cur.field(PAT_CAP);
    let s = cur.field(TXT_CAP);
    let flags = cur.field(FLAGS_CAP);
    if !text_ok(pat) || !text_ok(s) || !text_ok(flags) {
        return;
    }
    let has_flags = nopt >= 1;

    let mut n: i32 = 0;
    let mut ptrs: *const *const u8 = core::ptr::null();
    let mut lens: *const i32 = core::ptr::null();
    let cst = unsafe {
        pg_diff_regexp_split(
            s.as_ptr(), s.len() as i32, pat.as_ptr(), pat.len() as i32,
            flags.as_ptr(), flags.len() as i32, has_flags as i32,
            &mut n, &mut ptrs, &mut lens,
        )
    };
    let cres: Result<Vec<Vec<u8>>, i32> = if cst != 0 {
        Err(cst)
    } else {
        let mut items = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            // SAFETY: i < n arena arrays; copied before the next pg_diff_* call.
            let (p, l) = unsafe { (*ptrs.add(i), *lens.add(i)) };
            items.push(unsafe { core::slice::from_raw_parts(p, l as usize) }.to_vec());
        }
        Ok(items)
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres: types_error::PgResult<Vec<Vec<u8>>> = (|| {
        let mut ctx = adt_regexp::matches::regexp_split_setup(
            m,
            s,
            pat,
            has_flags.then_some(flags),
            C,
            "regexp_split_to_array()",
        )?;
        let mut items = Vec::with_capacity((ctx.nmatches + 1).max(1) as usize);
        while ctx.next_match <= ctx.nmatches {
            items.push(adt_regexp::matches::build_regexp_split_result(&ctx)?.as_slice().to_vec());
            ctx.next_match += 1;
        }
        Ok(items)
    })();

    match (cres, rres) {
        (Ok(c), Ok(r)) => assert!(
            c == r,
            "regexp_split_to_array: value DIVERGENCE pat={pat:?} s={s:?}: C={c:?} Rust={r:?}"
        ),
        (Err(code), Err(e)) => assert!(
            err_class(&e) == code,
            "regexp_split_to_array: sqlstate DIVERGENCE pat={pat:?}: C class {code} Rust {} ({:?})",
            err_class(&e),
            e.sqlstate
        ),
        (c, r) => panic!(
            "regexp_split_to_array: verdict DIVERGENCE pat={pat:?} s={s:?}: C ok={} Rust ok={}",
            c.is_ok(),
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 21: regexp_fixed_prefix (planner support; no SQL face, no fc plane).
// Planes: Some((prefix bytes, exact)) / None + verdict + errcode class.
// ---------------------------------------------------------------------------

fn regexp_fixed_prefix_diff(payload: &[u8]) {
    let mut cur = Cur::new(payload);
    let case_insensitive = cur.byte() & 1 != 0;
    let pat = cur.field(PAT_CAP);
    if !text_ok(pat) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: i32 = 0;
    let mut exact: i32 = 0;
    let mut isnull: i32 = 0;
    let cst = unsafe {
        pg_diff_regexp_fixed_prefix(
            pat.as_ptr(), pat.len() as i32, case_insensitive as i32,
            &mut out, &mut outlen, &mut exact, &mut isnull,
        )
    };
    let cres: Result<Option<(Vec<u8>, bool)>, i32> = if cst != 0 {
        Err(cst)
    } else if isnull != 0 {
        Ok(None)
    } else {
        // SAFETY: arena bytes valid until the next pg_diff_* call; copied now.
        Ok(Some((
            unsafe { core::slice::from_raw_parts(out, outlen as usize) }.to_vec(),
            exact != 0,
        )))
    };

    let cx = mcx::MemoryContext::new("regexp_fuzz");
    let m = cx.mcx();
    let rres = adt_regexp::regexp_fixed_prefix(m, pat, case_insensitive, C)
        .map(|o| o.map(|(v, e)| (v.as_slice().to_vec(), e)));

    match (cres, rres) {
        (Ok(c), Ok(r)) => assert!(
            c == r,
            "regexp_fixed_prefix(ci={case_insensitive}): value DIVERGENCE pat={pat:?}: C={c:?} Rust={r:?}"
        ),
        (Err(code), Err(e)) => assert!(
            err_class(&e) == code,
            "regexp_fixed_prefix: sqlstate DIVERGENCE pat={pat:?}: C class {code} Rust {} ({:?})",
            err_class(&e),
            e.sqlstate
        ),
        (c, r) => panic!(
            "regexp_fixed_prefix: verdict DIVERGENCE pat={pat:?} ci={case_insensitive}: C ok={} Rust ok={}",
            c.is_ok(),
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// parse_re_flags plane (in-test exhaustive sweep; also exercised through the
// flags field of every arm above).
// ---------------------------------------------------------------------------

/// Diff parse_re_flags over one option string (None = absent argument).
/// Panics on any plane divergence.  Used by the tests below.
pub fn parse_re_flags_diff(opts: Option<&[u8]>) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    pin();
    if let Some(o) = opts {
        if !text_ok(o) {
            return;
        }
    }
    let mut cflags: i32 = 0;
    let mut glob: i32 = 0;
    let (p, l, h) = match opts {
        Some(o) => (o.as_ptr(), o.len() as i32, 1),
        None => (core::ptr::null(), 0, 0),
    };
    let cst = unsafe { pg_diff_parse_re_flags(p, l, h, &mut cflags, &mut glob) };
    let rres = adt_regexp::parse_re_flags(opts);
    match (cst, rres) {
        (0, Ok(f)) => {
            assert!(
                f.cflags == cflags && (f.glob as i32) == glob,
                "parse_re_flags: value DIVERGENCE opts={opts:?}: C=({cflags},{glob}) Rust=({},{})",
                f.cflags,
                f.glob
            );
        }
        (code, Err(e)) if code != 0 => {
            assert!(
                err_class(&e) == code,
                "parse_re_flags: sqlstate DIVERGENCE opts={opts:?}: C class {code} Rust {}",
                err_class(&e)
            );
        }
        (cst, r) => panic!(
            "parse_re_flags: verdict DIVERGENCE opts={opts:?}: C status {cst} vs Rust ok={}",
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine pin is REAL, not assumed: after pin(), regexp_alt::dispatch
    /// must return None even for a trivially RE2-compatible pattern+subject
    /// (this dev build links libre2, so auto mode WOULD dispatch without the
    /// pin).  See the module header.
    #[test]
    fn spencer_path_pinned() {
        pin();
        let d = regexp_alt::dispatch(b"abc", 0, b"xabcx").expect("dispatch");
        assert!(d.is_none(), "regex_engine=spencer pin failed: RE2 dispatch engaged");
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    /// Big-stack thread (64MB, like libFuzzer's main thread): the corpus now
    /// contains fuzz-found nested-quantifier patterns on which the Rust
    /// engine recurses past the 2MB default test stack before reporting
    /// REG_ETOOBIG — the shipped rstacktoodeep() is pinned to 0 (never too
    /// deep), so unlike C there is no stack guard.  Engine-lane finding,
    /// reported with the ETOOBIG divergence (module header).
    #[test]
    fn seed_corpus_replays_clean() {
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(|| {
                let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/regexp_diff");
                let mut n = 0;
                for e in std::fs::read_dir(dir).expect("corpus/regexp_diff missing") {
                    let p = e.unwrap().path();
                    if p.is_file() {
                        regexp_diff(&std::fs::read(&p).unwrap());
                        n += 1;
                    }
                }
                assert!(n >= 30, "expected >=30 seeds, found {n}");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Build one driver input: selector + length-prefixed fields per the
    /// module-header grammar.
    fn arm(sel: u8, parts: &[&[u8]]) -> Vec<u8> {
        let mut v = vec![sel];
        for p in parts {
            v.extend_from_slice(p);
        }
        v
    }
    /// One length-prefixed field (cap < 256 arm of the cursor).
    fn f1(body: &[u8]) -> Vec<u8> {
        let mut v = vec![body.len() as u8];
        v.extend_from_slice(body);
        v
    }
    /// One length-prefixed field for the 512-cap (2 length bytes).
    fn f2(body: &[u8]) -> Vec<u8> {
        let mut v = vec![body.len() as u8, 0];
        v.extend_from_slice(body);
        v
    }
    /// Small-biased int byte encoding x (must be in -3..=247).
    fn ib(x: i32) -> Vec<u8> {
        vec![(x + 3) as u8]
    }

    /// Per-arm ok + error smokes: a known match WITH captures per arm, plus
    /// an invalid-pattern error shape per arm (both sides must agree; any
    /// divergence asserts inside the driver).
    #[test]
    fn arms_smoke() {
        let cap_pat: &[u8] = b"^(a+)(b+)$"; // captures
        let bad: &[u8] = b"(unbalanced";
        for sel in [0u8, 1, 2, 3, 4, 5, 19, 20] {
            regexp_diff(&arm(sel, &[&f1(cap_pat), &f2(b"aabbb")]));
            regexp_diff(&arm(sel, &[&f1(cap_pat), &f2(b"xyz")]));
            regexp_diff(&arm(sel, &[&f1(bad), &f2(b"abc")]));
        }
        // nameic arms: the fold is the point.
        for sel in [19u8, 20] {
            regexp_diff(&arm(sel, &[&f1(b"^abc$"), &f2(b"AbC")]));
            regexp_diff(&arm(sel, &[&f1(b"^ABC$"), &f2(b"xyz")]));
        }
        // similar_escape family: value + null shapes (arm 6 bits byte).
        regexp_diff(&arm(6, &[&[0], &f1(b"ab%_"), &f1(b"")]));
        regexp_diff(&arm(6, &[&[1], &f1(b""), &f1(b"")])); // NULL pattern
        regexp_diff(&arm(6, &[&[2], &f1(b"a#%b"), &f1(b"#")])); // NULL escape -> default
        regexp_diff(&arm(7, &[&f1(b"ab(cd)%")]));
        regexp_diff(&arm(8, &[&f1(b"a#%b"), &f1(b"#")]));
        regexp_diff(&arm(8, &[&f1(b"a%b"), &f1(b"xy")])); // multi-char escape error
        // textregexsubstr: capture + no-capture + no-match + 'foo(bar)?'.
        regexp_diff(&arm(9, &[&f1(cap_pat), &f2(b"aabbb")]));
        regexp_diff(&arm(9, &[&f1(b"b+"), &f2(b"aabbb")]));
        regexp_diff(&arm(9, &[&f1(b"foo(bar)?"), &f2(b"foo")])); // matched, no submatch
        regexp_diff(&arm(9, &[&f1(bad), &f2(b"abc")]));
        // replace family: backrefs, \&, global, bad pattern, digit-flag HINT.
        regexp_diff(&arm(10, &[&f1(b"(b+)"), &f2(b"aabbb"), &f1(b"[\\1]")]));
        regexp_diff(&arm(11, &[&f1(b"o"), &f2(b"hello world"), &f1(b"0"), &f1(b"g")]));
        regexp_diff(&arm(11, &[&f1(b"o"), &f2(b"hello"), &f1(b"0"), &f1(b"1")])); // digit flag
        regexp_diff(&arm(11, &[&f1(bad), &f2(b"abc"), &f1(b"x"), &f1(b"")]));
        regexp_diff(&arm(18, &[&[2], &f1(b"a"), &f2(b"banana"), &f1(b"X"), &ib(2), &ib(2), &f1(b"")]));
        regexp_diff(&arm(18, &[&[0], &f1(b"a"), &f2(b"banana"), &f1(b"X"), &ib(-1), &ib(0), &f1(b"")])); // bad start
        // count/instr/like/substr: ok + param-error + flags-error shapes.
        regexp_diff(&arm(12, &[&[2], &f1(b"a"), &f2(b"banana"), &ib(1), &f1(b"i")]));
        regexp_diff(&arm(12, &[&[1], &f1(b"a"), &f2(b"banana"), &ib(0), &f1(b"")])); // start=0 error
        regexp_diff(&arm(13, &[&[5], &f1(b"(n)(a)"), &f2(b"banana"), &ib(1), &ib(2), &ib(1), &f1(b""), &ib(2)]));
        regexp_diff(&arm(13, &[&[3], &f1(b"a"), &f2(b"banana"), &ib(1), &ib(1), &ib(7), &f1(b""), &ib(0)])); // endoption=7
        regexp_diff(&arm(14, &[&[1], &f1(b"^b"), &f2(b"banana"), &f1(b"i")]));
        regexp_diff(&arm(14, &[&[1], &f1(b"a"), &f2(b"banana"), &f1(b"g")])); // g rejected
        regexp_diff(&arm(15, &[&[4], &f1(b"(an)"), &f2(b"banana"), &ib(1), &ib(2), &f1(b""), &ib(1)]));
        regexp_diff(&arm(15, &[&[2], &f1(b"x"), &f2(b"banana"), &ib(1), &ib(1), &f1(b""), &ib(0)])); // NULL result
        // match/matches: captures, no-match NULL, glob loop, g-rejected.
        regexp_diff(&arm(16, &[&[0], &f1(b"(bar)(beque)"), &f2(b"foobarbequebaz"), &f1(b"")]));
        regexp_diff(&arm(16, &[&[0], &f1(b"x"), &f2(b"abc"), &f1(b"")]));
        regexp_diff(&arm(16, &[&[2], &f1(b"a"), &f2(b"banana"), &f1(b"g")])); // regexp_match + g -> error
        regexp_diff(&arm(16, &[&[3], &f1(b"(a)(n)?"), &f2(b"banana"), &f1(b"g")])); // matches loop, NULL slots
        // split: basic, degenerate empty-match pattern, error.
        regexp_diff(&arm(17, &[&[0], &f1(b"\\s+"), &f2(b"the quick brown fox"), &f1(b"")]));
        regexp_diff(&arm(17, &[&[0], &f1(b""), &f2(b"abc"), &f1(b"")])); // empty pattern
        regexp_diff(&arm(17, &[&[1], &f1(b"a*"), &f2(b"baaab"), &f1(b"i")]));
        regexp_diff(&arm(17, &[&[0], &f1(bad), &f2(b"abc"), &f1(b"")]));
        // fixed prefix: none / prefix / exact / icase-degraded / error.
        regexp_diff(&arm(21, &[&[0], &f1(b"abc")])); // unanchored: no prefix
        regexp_diff(&arm(21, &[&[0], &f1(b"^abc")])); // prefix
        regexp_diff(&arm(21, &[&[0], &f1(b"^abc$")])); // exact
        regexp_diff(&arm(21, &[&[0], &f1(b"^abc[xy]z*")]));
        regexp_diff(&arm(21, &[&[1], &f1(b"^abc")])); // icase: two colors/char
        regexp_diff(&arm(21, &[&[1], &f1(b"^123$")])); // icase, caseless chars
        regexp_diff(&arm(21, &[&[0], &f1(bad)])); // 2201B both sides
        regexp_diff(&arm(21, &[&[0], &f1("^\u{e9}t\u{e9}".as_bytes())])); // mb prefix
    }

    /// Every documented argument-validation / early-return branch of the
    /// regexp_count/instr/like/substr/split faces, one witness each (the
    /// same shapes are seeded into corpus/regexp_diff for coverage replay).
    #[test]
    fn matches_branch_witnesses() {
        // 'g' flag rejected by the non-SRF faces.
        regexp_diff(&arm(12, &[&[2], &f1(b"a"), &f2(b"banana"), &ib(1), &f1(b"g")]));
        regexp_diff(&arm(13, &[&[4], &f1(b"a"), &f2(b"banana"), &ib(1), &ib(1), &ib(0), &f1(b"g"), &ib(0)]));
        regexp_diff(&arm(14, &[&[1], &f1(b"a"), &f2(b"banana"), &f1(b"g")]));
        regexp_diff(&arm(15, &[&[3], &f1(b"a"), &f2(b"banana"), &ib(1), &ib(1), &f1(b"g"), &ib(0)]));
        regexp_diff(&arm(17, &[&[1], &f1(b"a"), &f2(b"banana"), &f1(b"g")]));
        // start <= 0 (count/instr/substr + replace_extended).
        regexp_diff(&arm(12, &[&[1], &f1(b"a"), &f2(b"banana"), &ib(0), &f1(b"")]));
        regexp_diff(&arm(13, &[&[1], &f1(b"a"), &f2(b"banana"), &ib(-2), &f1(b"")]));
        regexp_diff(&arm(18, &[&[0], &f1(b"a"), &f2(b"banana"), &f1(b"X"), &ib(0), &ib(1), &f1(b"")]));
        regexp_diff(&arm(18, &[&[1], &f1(b"a"), &f2(b"banana"), &f1(b"X"), &ib(-3), &ib(1), &f1(b"")]));
        // n <= 0 / n > nmatches.
        regexp_diff(&arm(13, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(0), &ib(0), &f1(b""), &ib(0)]));
        regexp_diff(&arm(15, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(-1), &f1(b""), &ib(0)]));
        regexp_diff(&arm(13, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(5), &ib(0), &f1(b""), &ib(0)]));
        regexp_diff(&arm(15, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(5), &f1(b""), &ib(0)]));
        // endoption outside {0,1}.
        regexp_diff(&arm(13, &[&[3], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(1), &ib(2), &f1(b""), &ib(0)]));
        regexp_diff(&arm(13, &[&[3], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(1), &ib(-1), &f1(b""), &ib(0)]));
        // subexpr < 0 / > npatterns / unmatched optional group (so < 0).
        regexp_diff(&arm(13, &[&[5], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &ib(0), &f1(b""), &ib(-1)]));
        regexp_diff(&arm(15, &[&[4], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &f1(b""), &ib(-2)]));
        regexp_diff(&arm(13, &[&[5], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &ib(0), &f1(b""), &ib(3)]));
        regexp_diff(&arm(15, &[&[4], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &f1(b""), &ib(3)]));
        regexp_diff(&arm(13, &[&[5], &f1(b"foo(bar)?"), &f2(b"foo"), &ib(1), &ib(1), &ib(0), &f1(b""), &ib(1)]));
        regexp_diff(&arm(15, &[&[4], &f1(b"foo(bar)?"), &f2(b"foo"), &ib(1), &ib(1), &f1(b""), &ib(1)]));
        // digit-first 4th arg on textregexreplace: 22023 + HINT path.
        regexp_diff(&arm(11, &[&f1(b"o"), &f2(b"hello"), &f1(b"0"), &f1(b"1")]));
        regexp_diff(&arm(11, &[&f1(b"o"), &f2(b"hello"), &f1(b"0"), &f1(b"9g")]));
        // invalid multibyte / junk option letters (invalid_re_option mblen).
        regexp_diff(&arm(14, &[&[1], &f1(b"a.c"), &f2(b"abc"), &f1("\u{e9}".as_bytes())]));
        regexp_diff(&arm(12, &[&[2], &f1(b"a"), &f2(b"banana"), &ib(1), &f1("z\u{20ac}".as_bytes())]));
        // similar_escape empty / 2-char / multibyte escapes, both faces.
        regexp_diff(&arm(8, &[&f1(b"a%b_c\\d"), &f1(b"")])); // Some(empty): elen==0
        regexp_diff(&arm(6, &[&[0], &f1(b"a%b"), &f1(b"")])); // legacy face, empty esc
        regexp_diff(&arm(8, &[&f1(b"a%b"), &f1(b"xy")])); // 2 chars: 22025
        regexp_diff(&arm(8, &[&f1("a\u{e9}%b".as_bytes()), &f1("\u{e9}".as_bytes())])); // 1 mb char: legal
        regexp_diff(&arm(6, &[&[0], &f1(b"a#%"), &f1("\u{e9}x".as_bytes())])); // mb+1: 22025
        // multibyte pattern char immediately AFTER a multibyte escape (the
        // afterescape mblen>1 arm) + escape-char-is-prefix-mismatch shapes.
        regexp_diff(&arm(8, &[&f1("\u{e9}\u{e9}%x".as_bytes()), &f1("\u{e9}".as_bytes())]));
        regexp_diff(&arm(6, &[&[0], &f1("\u{e9}\u{20ac}".as_bytes()), &f1("\u{e9}".as_bytes())]));
    }

    /// RE cache eviction (adt_regexp::MAX_CACHED_RES = 32, thread-local):
    /// compile more than 32 distinct patterns on one thread, then re-use the
    /// first (evicted -> recompiled) and a recent one (hit + move-to-front).
    /// Deterministic, no oracle side — the cache is this lane's carve; only
    /// the Rust results are asserted.
    #[test]
    fn re_cache_eviction_sweep() {
        pin();
        let cx = mcx::MemoryContext::new("t");
        let m = cx.mcx();
        for i in 0..(adt_regexp::MAX_CACHED_RES + 8) {
            let pat = format!("^evict{i}Z$");
            let subj = format!("evict{i}Z");
            assert!(adt_regexp::matches::regexp_like(m, subj.as_bytes(), pat.as_bytes(), None, C)
                .expect("compile"));
        }
        // hit + move-to-front (i > 0 branch), then the evicted first pattern.
        assert!(adt_regexp::matches::regexp_like(m, b"evict38Z", b"^evict38Z$", None, C).unwrap());
        assert!(adt_regexp::matches::regexp_like(m, b"evict0Z", b"^evict0Z$", None, C).unwrap());
        assert!(!adt_regexp::matches::regexp_like(m, b"nope", b"^evict0Z$", None, C).unwrap());
    }

    /// Degenerate/empty-match loops (prev_match_end / start_search+1) and
    /// multibyte position arithmetic — exactly the logic under test.
    #[test]
    fn degenerate_and_multibyte() {
        for pat in [&b""[..], b"()", b"a*", b"(?:)", b"x?"] {
            regexp_diff(&arm(12, &[&[2], &f1(pat), &f2("caf\u{e9}s".as_bytes()), &ib(1), &f1(b"")]));
            regexp_diff(&arm(16, &[&[3], &f1(pat), &f2("\u{3b1}\u{3b2}\u{3b3}".as_bytes()), &f1(b"g")]));
            regexp_diff(&arm(17, &[&[0], &f1(pat), &f2("na\u{ef}ve".as_bytes()), &f1(b"")]));
            regexp_diff(&arm(11, &[&f1(pat), &f2("\u{e9}l\u{e8}ve".as_bytes()), &f1(b"-"), &f1(b"g")]));
        }
        // multibyte SIMILAR TO escape character (2-arg face, mblen>1 path)
        regexp_diff(&arm(8, &[&f1("a\u{e9}%b".as_bytes()), &f1("\u{e9}".as_bytes())]));
    }

    /// Single-field witness pairs: same inputs, one int param +/-1 around
    /// each boundary (start/n/endoption/subexpr off-by-one behavior).
    #[test]
    fn param_boundary_witnesses() {
        for start in [0, 1, 2, 6, 7] {
            if !(-3..=247).contains(&start) {
                continue;
            }
            regexp_diff(&arm(12, &[&[1], &f1(b"a"), &f2(b"banana"), &ib(start), &f1(b"")]));
        }
        for n in [0, 1, 2, 3, 4] {
            regexp_diff(&arm(13, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(n), &ib(0), &f1(b""), &ib(0)]));
            regexp_diff(&arm(15, &[&[2], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(n), &f1(b""), &ib(0)]));
        }
        for endoption in [-1, 0, 1, 2] {
            regexp_diff(&arm(13, &[&[3], &f1(b"an"), &f2(b"banana"), &ib(1), &ib(1), &ib(endoption), &f1(b""), &ib(0)]));
        }
        for subexpr in [-1, 0, 1, 2, 3] {
            regexp_diff(&arm(13, &[&[5], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &ib(0), &f1(b""), &ib(subexpr)]));
            regexp_diff(&arm(15, &[&[4], &f1(b"(a)(n)"), &f2(b"banana"), &ib(1), &ib(1), &f1(b""), &ib(subexpr)]));
        }
    }

    /// similar_escape quote-separator cases: 0, 1, 2, and 3 (error)
    /// escape-double-quotes, plus bracket-depth interaction.
    #[test]
    fn similar_escape_quote_separators() {
        regexp_diff(&arm(7, &[&f1(br#"abc"#)]));
        regexp_diff(&arm(7, &[&f1(br#"a\"b\"c"#)]));
        regexp_diff(&arm(7, &[&f1(br#"a\"b\"c\"d"#)])); // 3 separators -> error
        regexp_diff(&arm(8, &[&f1(br##"a#"b#"c"##), &f1(b"#")]));
        regexp_diff(&arm(8, &[&f1(br##"a#"b#"c#"d"##), &f1(b"#")])); // error
        regexp_diff(&arm(8, &[&f1(br##"[#"]a"##), &f1(b"#")])); // inside brackets: literal
        regexp_diff(&arm(7, &[&f1(br#"[^]]a\"b"#)]));
    }

    /// Exhaustive parse_re_flags sweep vs C: ALL option strings of len<=2
    /// over the full single-byte alphabet (NUL excluded, non-UTF8 skipped by
    /// the domain check inside), plus 1e5 random len<=4 strings.  Cheap.
    #[test]
    fn parse_re_flags_sweep() {
        parse_re_flags_diff(None);
        parse_re_flags_diff(Some(b""));
        for a in 1..=255u8 {
            parse_re_flags_diff(Some(&[a]));
            for b in 1..=255u8 {
                parse_re_flags_diff(Some(&[a, b]));
            }
        }
        // 1e5 random len<=4 (xorshift; deterministic).
        let mut x: u64 = 0x9e3779b97f4a7c15;
        let mut buf = [0u8; 4];
        for _ in 0..100_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let len = (x % 5) as usize;
            for (i, b) in buf.iter_mut().enumerate().take(len) {
                *b = (x >> (8 * i)) as u8;
            }
            parse_re_flags_diff(Some(&buf[..len]));
        }
    }

    /// Flags-error smoke: every documented flag letter accepted, junk
    /// rejected identically (through a full arm, not just parse_re_flags).
    #[test]
    fn flags_smoke() {
        for fl in [
            &b"g"[..], b"b", b"c", b"e", b"i", b"m", b"n", b"p", b"q", b"s", b"t", b"w", b"x",
            b"gi", b"biq", b"z", b"1", b"\xc3\xa9",
        ] {
            regexp_diff(&arm(14, &[&[1], &f1(b"a.c"), &f2(b"abc"), &f1(fl)]));
            regexp_diff(&arm(16, &[&[2], &f1(b"a(b)c"), &f2(b"abc"), &f1(fl)]));
        }
    }
    /// Regression witness for the FIXED REG_ETOOBIG divergence (found
    /// 2026-07-31 by this target's first 200k-exec smoke: the Rust Spencer
    /// port reported "regular expression is too complex" on a
    /// nested-bounded-quantifier pattern that both the vendored 18.3 C
    /// engine and real postgres:18.3 accept; the engine lane fixed it and
    /// the known_etoobig_divergence carve was removed 2026-08-01).  The
    /// minimized 32-byte fuzz unit must replay through the full driver with
    /// no divergence, and the direct split call must succeed.  Runs on a
    /// big stack: the engine recurses deeply on this pattern.
    #[test]
    fn known_etoobig_divergence_repro() {
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(|| {
                let unit: &[u8] = &[
                    0x11, 0x6e, 0x43, 0x28, 0x5c, 0x5c, 0x77, 0x28, 0x5c, 0x79, 0x43, 0x28,
                    0x7c, 0x29, 0x7b, 0x31, 0x36, 0x7d, 0x29, 0x7b, 0x31, 0x36, 0x7d, 0x03,
                    0x2b, 0x29, 0x7b, 0x31, 0x36, 0x7d, 0x03, 0x2b,
                ];
                // full comparator plane: any C-vs-Rust divergence panics
                regexp_diff(unit);
                // and the once-failing direct call now succeeds
                pin();
                let exact: Vec<u8> = vec![
                    40, 92, 92, 119, 40, 92, 121, 67, 40, 124, 41, 123, 49, 54, 125, 41,
                    123, 49, 54, 125, 3, 43, 41, 123, 49, 54, 125, 3, 43,
                ];
                let cx = mcx::MemoryContext::new("t");
                adt_regexp::matches::regexp_split_setup(
                    cx.mcx(), b"", &exact, None, C, "regexp_split_to_array()",
                )
                .expect("REG_ETOOBIG regression: engine rejects the fixed pattern again");
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
