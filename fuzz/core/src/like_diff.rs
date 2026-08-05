//! like_diff: differential fuzz driver — shipped Rust `adt_like`
//! (crates/backend/utils/adt/like) vs vendored PostgreSQL 18.3 C
//! (Stamp-18.3, upstream sha 62d6c7d3df; csrc/pg_like_io.c: like.c with
//! like_match.c pasted verbatim once per stamping).
//!
//! Comparison planes (float_in_diff conventions): result value (bool, or
//! output bytes for the like_escape arms) + Ok/Err verdict + errcode/
//! sqlstate class. Message text is out of scope. Any mismatch panics —
//! libFuzzer minimizes that into the divergence reproducer.
//!
//! Input layout: [selector][mode][l0][l1][piece A][piece B]
//!   selector % 15 picks the arm:
//!     0 textlike    (oid  850)  A = text,   B = pattern
//!     1 textnlike   (oid  851)  A = text,   B = pattern
//!     2 namelike    (oid  858)  A = name,   B = pattern
//!     3 namenlike   (oid  859)  A = name,   B = pattern
//!     4 texticlike  (oid 1633)  A = text,   B = pattern
//!     5 texticnlike (oid 1634)  A = text,   B = pattern
//!     6 nameiclike  (oid 1635)  A = name,   B = pattern
//!     7 nameicnlike (oid 1636)  A = name,   B = pattern
//!     8 like_escape (oid 1637)  A = pattern, B = escape string
//!     9 bytealike   (oid 2005)  A = bytes,  B = pattern (raw bytes)
//!    10 byteanlike  (oid 2006)  A = bytes,  B = pattern (raw bytes)
//!    11 like_escape_bytea (oid 2009) A = pattern, B = escape (raw bytes)
//!    12 sb_match_text   [kernel] A = text, B = pattern (raw bytes) —
//!       direct SB_MatchText stamping diff, tristate TRUE/FALSE/ABORT
//!    13 utf8_match_text [kernel] A = text, B = pattern (raw bytes) —
//!       direct UTF8_MatchText stamping diff
//!    14 sb_imatch_text  [kernel] A = text, B = pattern (raw bytes) —
//!       direct SB_IMatchText stamping diff (C locale, ASCII fold)
//!   mode bit0 = encoding plane: 0 -> UTF8 (max_length 4), 1 -> LATIN1
//!     (max_length 1) — set per exec on BOTH sides
//!     (mbutils::SetDatabaseEncoding / pg_diff_like_set_encoding).
//!   mode bit1 = collation: 0 -> C_COLLATION_OID (950), 1 -> InvalidOid
//!     (exercises the 42P22 indeterminate-collation arm); ignored by the
//!     escape and bytea arms whose C bodies never read collation.  The
//!     kernel arms 12/13 reuse it as the locale selector: 0 -> Some(C
//!     locale) on both sides, 1 -> None/NULL (C's bytealike / lowered-ILIKE
//!     `MatchText(..., 0)` call shape); arm 14's SB_IMatchText always folds
//!     through the C locale.
//!   [l0][l1] little-endian u16; len(A) = u16 % (rest.len() + 1); B is the
//!   remainder.  Kernel-arm pieces are RAW BYTES: none of the three
//!   stampings consults pg_mblen (the UTF8 stamping's NextChar is the pure
//!   continuation-byte skip), so no text/encoding gate applies there; the
//!   only reachable error is the trailing-escape 22025.
//!
//! INPUT INVARIANTS (enforced here, mirroring the server's datum contracts):
//!   - Each piece is capped at 512 bytes: MatchText recursion depth is
//!     bounded by the pattern length, and the C oracle shims
//!     check_stack_depth() to a no-op — the bound stands in for the guard.
//!   - Text arms (0-8): both pieces must be NUL-free AND valid in the
//!     selected encoding (the server's text-datum invariant — datatype
//!     input verified them; every byte is valid LATIN1, so that plane only
//!     needs the NUL check).
//!   - Name arms (2/3/6/7): piece A additionally capped at 63 bytes and
//!     built into a NUL-padded 64-byte NameData block (NameStr semantics by
//!     construction; namein's mbcliplen truncation is the name crate's
//!     lane). The C side strlen-stops exactly as NameStr does.
//!   - Bytea arms (9/10/11): raw bytes, embedded NUL legal — C bytealike
//!     works from VARDATA/VARSIZE, never strlen (like_escape_bytea always
//!     takes the SB stamping, so no mblen either).
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). The ic and escape arms call
//! twice through one resolved FmgrInfo to hit the fn_extra scratch-reuse
//! branch. None of these wrappers takes an escontext (LIKE ops are not
//! soft-error input functions), so there is no ErrorSaveNode shape here.
//!
//! SKIPPED rows / scope carves (executable where possible):
//!   - like/notlike alias oids (1569-1572, 2007/2008) and the bpchar rows
//!     (1631/1632/1660/1661): the catalog binds them to the SAME PGFunction
//!     values fuzzed here (fc_textlike etc.) — covered by identity.
//!   - textlike_support/texticlike_support/textregexeq_support/
//!     texticregexeq_support/text_starts_with_support (oids 1023-1025,
//!     1364, 6242): planner prosupport rows.  Their UNHANDLED-TAG leg (C
//!     like_regex_support returns NULL for any request other than
//!     Selectivity/IndexCondition) IS exercised — every exec runs the five
//!     wrappers over non-planner NodeTags and asserts Ok(Datum 0)
//!     (fc_support_unhandled_tag_plane).  Only the Selectivity/
//!     IndexCondition panic legs stay untested: they are the defensive
//!     closed-set planner assertion, deliberately never triggered.
//!   - Non-UTF8 MULTIBYTE encodings: out of scope on both sides of this
//!     target — shipped Rust raises 0A000 (mb_matchtext_unported) while C
//!     runs MB_MatchText; the two fuzzed planes (UTF8, LATIN1) never route
//!     there. The MB stampings still execute here through MB_do_like_escape
//!     (like_escape under UTF8).
//!   - Nondeterministic collations: C-collation pin on both sides
//!     (deterministic=true), like_match.c's pg_strncoll arm is dead and the
//!     oracle shims it ABORT-LOUD, never silently.
//!
//! EXHAUSTIVE KERNEL SWEEP (tests::exhaustive_kernel_sweep, #[ignore]d):
//! all (text, pattern) pairs over the alphabet {a, b, %, _, \} with
//! len(text) <= 4 and len(pattern) <= 4 — 781 x 781 = 609,961 pairs —
//! through the textlike arm (UTF8 plane, C collation), full three-plane
//! comparison per pair. Run once at build time on this host (M-series
//! laptop, unoptimized test profile): 609,961 pairs in 0.65 s wall
//! (`cargo test -p decoder_fuzz like_diff::tests::exhaustive_kernel_sweep
//! -- --ignored` leg, 2026-07-31), zero divergences.

use std::ffi::c_char;

use adt_like::builtins as lb;
use adt_like::IcScratch;
use datum::{Datum, NullableDatum};
use types_core::{C_COLLATION_OID, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDETERMINATE_COLLATION, ERRCODE_INVALID_ESCAPE_SEQUENCE,
};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // Oracle entries (csrc/pg_like_io.c section 2).
    fn pg_diff_like_set_encoding(utf8: i32);
    fn pg_diff_like_textlike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_textnlike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_namelike(
        name64: *const c_char, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_namenlike(
        name64: *const c_char, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_texticlike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_texticnlike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_nameiclike(
        name64: *const c_char, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_nameicnlike(
        name64: *const c_char, p: *const c_char, plen: i32,
        collation: u32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_bytealike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        out: *mut i32,
    ) -> i32;
    fn pg_diff_like_byteanlike(
        s: *const c_char, slen: i32, p: *const c_char, plen: i32,
        out: *mut i32,
    ) -> i32;
    fn pg_diff_like_escape(
        p: *const c_char, plen: i32, e: *const c_char, elen: i32,
        out: *mut c_char, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_like_escape_bytea(
        p: *const c_char, plen: i32, e: *const c_char, elen: i32,
        out: *mut c_char, outlen: *mut i32,
    ) -> i32;
    // Direct kernel entries (arms 12..14): raw LIKE_TRUE/FALSE/ABORT.
    fn pg_diff_like_sb_match(
        t: *const c_char, tlen: i32, p: *const c_char, plen: i32,
        use_locale: i32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_utf8_match(
        t: *const c_char, tlen: i32, p: *const c_char, plen: i32,
        use_locale: i32, out: *mut i32,
    ) -> i32;
    fn pg_diff_like_sb_imatch(
        t: *const c_char, tlen: i32, p: *const c_char, plen: i32,
        out: *mut i32,
    ) -> i32;
}

/// Oracle error classes (csrc/pg_like_io.c shim 2).
const C_ERR_INVALID_ESCAPE: i32 = 1; /* 22025 */
const C_ERR_INDETERMINATE_COLLATION: i32 = 2; /* 42P22 */
const C_ERR_FEATURE_NOT_SUPPORTED: i32 = 3; /* 0A000 */
const C_ERR_INVALID_BYTE_SEQ: i32 = 4; /* 22021 */

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_ESCAPE_SEQUENCE {
        C_ERR_INVALID_ESCAPE
    } else if e.sqlstate == ERRCODE_INDETERMINATE_COLLATION {
        C_ERR_INDETERMINATE_COLLATION
    } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
        C_ERR_FEATURE_NOT_SUPPORTED
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_INVALID_BYTE_SEQ
    } else {
        99
    }
}

/// Recursion/latency bound (module doc): MatchText recursion depth is
/// bounded by the pattern length; the C oracle no-ops check_stack_depth.
const MAX_PIECE: usize = 512;

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the name_diff
// pattern; collation rides fncollation exactly as the executor sets it).
// ---------------------------------------------------------------------------

/// One native fmgr call: N non-null arg Datums, optional resolved FmgrInfo,
/// optional armed result mcx, explicit collation. LIKE wrappers never
/// return SQL NULL.
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    coll: u32,
    mcx: Option<mcx::Mcx<'_>>,
    args: [Datum; N],
) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(coll);
    if let Some(m) = mcx {
        // SAFETY: the arming context outlives this single call.
        unsafe { fcinfo.set_result_mcx(m) };
    }
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(flinfo, &mut fcinfo);
    if r.is_ok() {
        assert!(!fcinfo.isnull, "fc wrapper returned SQL NULL unexpectedly");
    }
    r
}

/// text/bytea arg construction: inline 4B-uncompressed header + body (the
/// shipped set_varsize_4b encoding; body <= MAX_PIECE so the length fits).
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

/// NAME arg convention: fixed 64-byte by-ref pointer.
fn name_datum(block: &[u8; 64]) -> Datum {
    Datum::from_usize(block.as_ptr() as usize)
}

/// Varlena result readback (like_escape wrapper results).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images (flinfo OutBuf
    // scratch), read before the next call through the same flinfo.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// fc wrapper vs the already-C-checked core outcome, bool plane.
fn fc_expect(
    fname: &str,
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    coll: u32,
    mcx: Option<mcx::Mcx<'_>>,
    args: [Datum; 2],
    core: &PgResult<bool>,
) {
    match (fc_call(f, flinfo, coll, mcx, args), core) {
        (Ok(d), Ok(want)) => assert!(
            d.as_bool() == *want,
            "{fname} fc-wrapper DIVERGENCE: wrapper={} core={want}",
            d.as_bool()
        ),
        (Err(we), Err(ce)) => assert!(
            we.sqlstate == ce.sqlstate,
            "{fname} fc-wrapper sqlstate DIVERGENCE: wrapper={:?} core={:?}",
            we.sqlstate,
            ce.sqlstate
        ),
        (Ok(_), Err(ce)) => panic!("{fname} fc-wrapper Ok but core Err({:?})", ce.sqlstate),
        (Err(we), Ok(v)) => panic!("{fname} fc-wrapper Err({:?}) but core Ok({v})", we.sqlstate),
    }
}

// ---------------------------------------------------------------------------
// Input decode + per-exec environment
// ---------------------------------------------------------------------------

struct Frame<'a> {
    a: &'a [u8],
    b: &'a [u8],
    utf8: bool,
    coll: Oid,
}

fn decode(payload: &[u8]) -> Option<Frame<'_>> {
    let (&mode, rest) = payload.split_first()?;
    if rest.len() < 2 {
        return None;
    }
    let l = u16::from_le_bytes([rest[0], rest[1]]) as usize;
    let rest = &rest[2..];
    let alen = l % (rest.len() + 1);
    let (a, b) = rest.split_at(alen);
    if a.len() > MAX_PIECE || b.len() > MAX_PIECE {
        return None;
    }
    let utf8 = mode & 1 == 0;
    let coll: Oid = if mode & 2 == 0 { C_COLLATION_OID } else { 0 };
    // Pin the encoding plane on BOTH sides for this exec.
    mbutils::SetDatabaseEncoding(if utf8 { wchar::PG_UTF8 } else { wchar::PG_LATIN1 })
        .expect("UTF8/LATIN1 are valid be-encodings");
    unsafe { pg_diff_like_set_encoding(if utf8 { 1 } else { 0 }) };
    Some(Frame { a, b, utf8, coll })
}

/// The server's text-datum invariant: NUL-free and valid in the database
/// encoding (every byte is valid LATIN1, so that plane is NUL-only).
fn text_ok(bytes: &[u8], utf8: bool) -> bool {
    !bytes.contains(&0) && (!utf8 || std::str::from_utf8(bytes).is_ok())
}

/// NameData block by construction (NUL-padded; input capped at 63).
fn name_block(src: &[u8]) -> [u8; 64] {
    let mut block = [0u8; 64];
    block[..src.len()].copy_from_slice(src);
    block
}

fn cptr(b: &[u8]) -> *const c_char {
    b.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn like_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let Some(f) = decode(payload) else { return };
    // Deterministic prosupport plane (module header): the unhandled-tag NULL
    // leg of the five *_support wrappers, every exec (cheap: a tag read).
    fc_support_unhandled_tag_plane();
    match sel % 15 {
        0 => textlike_diff(&f),
        1 => textnlike_diff(&f),
        2 => namelike_diff(&f),
        3 => namenlike_diff(&f),
        4 => texticlike_diff(&f),
        5 => texticnlike_diff(&f),
        6 => nameiclike_diff(&f),
        7 => nameicnlike_diff(&f),
        8 => like_escape_diff(&f),
        9 => bytealike_diff(&f),
        10 => byteanlike_diff(&f),
        11 => like_escape_bytea_diff(&f),
        12 => kernel_match_diff(&f, KernelArm::Sb),
        13 => kernel_match_diff(&f, KernelArm::Utf8),
        _ => kernel_match_diff(&f, KernelArm::SbI),
    }
}

// ---------------------------------------------------------------------------
// Prosupport unhandled-tag plane (module header): C like_regex_support /
// text_starts_with_support return NULL for any request that is not
// Selectivity/IndexCondition; the shipped wrappers mirror that with
// Ok(Datum 0).  The panic legs are the planner closed-set defensive arm and
// must never be triggered here.
// ---------------------------------------------------------------------------

fn fc_support_unhandled_tag_plane() {
    use types_nodes::NodeTag;
    for tag in [
        NodeTag::T_Invalid,
        NodeTag::T_SupportRequestSimplify,
        NodeTag::T_SupportRequestCost,
    ] {
        for (fname, func) in [
            ("fc_textlike_support", lb::fc_textlike_support as PGFunction),
            ("fc_texticlike_support", lb::fc_texticlike_support),
            ("fc_textregexeq_support", lb::fc_textregexeq_support),
            ("fc_texticregexeq_support", lb::fc_texticregexeq_support),
            ("fc_text_starts_with_support", lb::fc_text_starts_with_support),
        ] {
            let d = fc_call(
                func,
                None,
                0,
                None,
                [Datum::from_usize(&tag as *const NodeTag as usize)],
            )
            .unwrap_or_else(|e| {
                panic!("{fname} DIVERGENCE: Err({:?}) on unhandled tag {tag:?} (C returns NULL)",
                       e.sqlstate)
            });
            assert!(
                d.as_usize() == 0,
                "{fname} DIVERGENCE: non-NULL Datum on unhandled tag {tag:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Arms 12..14: direct like_match.c kernel stampings (module header) — the
// shipped pub wrappers sb_match_text / utf8_match_text / sb_imatch_text vs
// the C SB_MatchText / UTF8_MatchText / SB_IMatchText stampings, on the raw
// LIKE_TRUE/FALSE/ABORT tristate.  Raw-byte domain; mode bit1 selects
// None/NULL vs Some(C) locale for the CS arms.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum KernelArm {
    Sb,
    Utf8,
    SbI,
}

fn kernel_match_diff(f: &Frame<'_>, arm: KernelArm) {
    let use_locale = f.coll != 0; // mode bit1 clear -> C locale on both sides
    let locale = use_locale.then_some(&pg_locale::C_LOCALE);
    let mut cval = 0i32;
    let (fname, cst, core) = match arm {
        KernelArm::Sb => (
            "sb_match_text",
            unsafe {
                pg_diff_like_sb_match(
                    cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32,
                    use_locale as i32, &mut cval,
                )
            },
            adt_like::sb_match_text(f.a, f.b, locale),
        ),
        KernelArm::Utf8 => (
            "utf8_match_text",
            unsafe {
                pg_diff_like_utf8_match(
                    cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32,
                    use_locale as i32, &mut cval,
                )
            },
            adt_like::utf8_match_text(f.a, f.b, locale),
        ),
        KernelArm::SbI => (
            "sb_imatch_text",
            unsafe {
                pg_diff_like_sb_imatch(
                    cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32, &mut cval,
                )
            },
            adt_like::sb_imatch_text(f.a, f.b, &pg_locale::C_LOCALE),
        ),
    };
    let ctx = format!("t={:?} p={:?} use_locale={use_locale}", f.a, f.b);
    match &core {
        // Exact tristate: LIKE_ABORT must match too, not just truthiness.
        Ok(v) => assert!(
            cst == 0 && cval == *v,
            "{fname} DIVERGENCE {ctx}: C=(st {cst}, val {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cst == rc && cst != 0,
                "{fname} DIVERGENCE {ctx}: C=(st {cst}) Rust=Err(class {rc}, {})",
                e.message
            );
        }
    }
    let cerr = unsafe { pg_diff_errcode_get() };
    assert!(
        cst == cerr || cst == 0,
        "{fname} oracle errcode plane inconsistent {ctx}: st={cst} errcode={cerr}"
    );
}

/// Shared comparator: C entry outcome vs shipped-core outcome vs fc wrapper.
/// `cres` = (status, value) from the oracle; `core` from the shipped crate.
fn compare_bool_planes(fname: &str, ctx: &str, cst: i32, cval: i32, core: &PgResult<bool>) {
    match core {
        Ok(v) => assert!(
            cst == 0 && (cval != 0) == *v,
            "{fname} DIVERGENCE {ctx}: C=(st {cst}, val {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cst == rc && cst != 0,
                "{fname} DIVERGENCE {ctx}: C=(st {cst}) Rust=Err(class {rc}, {})",
                e.message
            );
        }
    }
    let cerr = unsafe { pg_diff_errcode_get() };
    assert!(
        cst == cerr || cst == 0,
        "{fname} oracle errcode plane inconsistent {ctx}: st={cst} errcode={cerr}"
    );
}

// ---------------------------------------------------------------------------
// Arms 0/1: textlike / textnlike (oids 850/851)
// ---------------------------------------------------------------------------

macro_rules! text_match_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident) => {
        fn $fn_name(f: &Frame<'_>) {
            if !text_ok(f.a, f.utf8) || !text_ok(f.b, f.utf8) {
                return;
            }
            let mut cval = 0i32;
            let cst = unsafe {
                $cfn(cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32, f.coll, &mut cval)
            };
            let core = adt_like::$core(f.a, f.b, f.coll);
            compare_bool_planes(
                stringify!($core),
                &format!("s={:?} p={:?} utf8={} coll={}", f.a, f.b, f.utf8, f.coll),
                cst,
                cval,
                &core,
            );
            // fc plane
            let (si, pi) = (text_image(f.a), text_image(f.b));
            fc_expect(
                stringify!($fc),
                lb::$fc,
                None,
                f.coll,
                None,
                [
                    Datum::from_usize(si.as_ptr() as usize),
                    Datum::from_usize(pi.as_ptr() as usize),
                ],
                &core,
            );
        }
    };
}

text_match_arm!(textlike_diff, pg_diff_like_textlike, textlike, fc_textlike);
text_match_arm!(textnlike_diff, pg_diff_like_textnlike, textnlike, fc_textnlike);

// ---------------------------------------------------------------------------
// Arms 2/3: namelike / namenlike (oids 858/859)
// ---------------------------------------------------------------------------

macro_rules! name_match_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident) => {
        fn $fn_name(f: &Frame<'_>) {
            if f.a.len() > 63 || !text_ok(f.a, f.utf8) || !text_ok(f.b, f.utf8) {
                return;
            }
            let block = name_block(f.a);
            let mut cval = 0i32;
            let cst = unsafe {
                $cfn(cptr(&block), cptr(f.b), f.b.len() as i32, f.coll, &mut cval)
            };
            let core = adt_like::$core(&block, f.b, f.coll);
            compare_bool_planes(
                stringify!($core),
                &format!("name={:?} p={:?} utf8={} coll={}", f.a, f.b, f.utf8, f.coll),
                cst,
                cval,
                &core,
            );
            // fc plane
            let pi = text_image(f.b);
            fc_expect(
                stringify!($fc),
                lb::$fc,
                None,
                f.coll,
                None,
                [name_datum(&block), Datum::from_usize(pi.as_ptr() as usize)],
                &core,
            );
        }
    };
}

name_match_arm!(namelike_diff, pg_diff_like_namelike, namelike, fc_namelike);
name_match_arm!(namenlike_diff, pg_diff_like_namenlike, namenlike, fc_namenlike);

// ---------------------------------------------------------------------------
// Arms 4/5: texticlike / texticnlike (oids 1633/1634)
// ---------------------------------------------------------------------------

macro_rules! text_ic_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident) => {
        fn $fn_name(f: &Frame<'_>) {
            if !text_ok(f.a, f.utf8) || !text_ok(f.b, f.utf8) {
                return;
            }
            let mut cval = 0i32;
            let cst = unsafe {
                $cfn(cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32, f.coll, &mut cval)
            };
            let cx = mcx::MemoryContext::new("like_fuzz");
            let mut scratch = IcScratch::default();
            let core = adt_like::$core(cx.mcx(), f.a, f.b, f.coll, &mut scratch);
            compare_bool_planes(
                stringify!($core),
                &format!("s={:?} p={:?} utf8={} coll={}", f.a, f.b, f.utf8, f.coll),
                cst,
                cval,
                &core,
            );
            // fc plane: twice through one resolved FmgrInfo (fn_extra
            // IcScratch install + reuse branches).
            let (si, pi) = (text_image(f.a), text_image(f.b));
            let args = [
                Datum::from_usize(si.as_ptr() as usize),
                Datum::from_usize(pi.as_ptr() as usize),
            ];
            let mut fl = FmgrInfo::unresolved();
            fc_expect(stringify!($fc), lb::$fc, Some(&mut fl), f.coll, Some(cx.mcx()), args, &core);
            fc_expect(stringify!($fc), lb::$fc, Some(&mut fl), f.coll, Some(cx.mcx()), args, &core);
        }
    };
}

text_ic_arm!(texticlike_diff, pg_diff_like_texticlike, texticlike, fc_texticlike);
text_ic_arm!(texticnlike_diff, pg_diff_like_texticnlike, texticnlike, fc_texticnlike);

// ---------------------------------------------------------------------------
// Arms 6/7: nameiclike / nameicnlike (oids 1635/1636)
// ---------------------------------------------------------------------------

macro_rules! name_ic_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident) => {
        fn $fn_name(f: &Frame<'_>) {
            if f.a.len() > 63 || !text_ok(f.a, f.utf8) || !text_ok(f.b, f.utf8) {
                return;
            }
            let block = name_block(f.a);
            let mut cval = 0i32;
            let cst = unsafe {
                $cfn(cptr(&block), cptr(f.b), f.b.len() as i32, f.coll, &mut cval)
            };
            let cx = mcx::MemoryContext::new("like_fuzz");
            let mut scratch = IcScratch::default();
            let core = adt_like::$core(cx.mcx(), &block, f.b, f.coll, &mut scratch);
            compare_bool_planes(
                stringify!($core),
                &format!("name={:?} p={:?} utf8={} coll={}", f.a, f.b, f.utf8, f.coll),
                cst,
                cval,
                &core,
            );
            // fc plane (scratch install + reuse).
            let pi = text_image(f.b);
            let args = [name_datum(&block), Datum::from_usize(pi.as_ptr() as usize)];
            let mut fl = FmgrInfo::unresolved();
            fc_expect(stringify!($fc), lb::$fc, Some(&mut fl), f.coll, Some(cx.mcx()), args, &core);
            fc_expect(stringify!($fc), lb::$fc, Some(&mut fl), f.coll, Some(cx.mcx()), args, &core);
        }
    };
}

name_ic_arm!(nameiclike_diff, pg_diff_like_nameiclike, nameiclike, fc_nameiclike);
name_ic_arm!(nameicnlike_diff, pg_diff_like_nameicnlike, nameicnlike, fc_nameicnlike);

// ---------------------------------------------------------------------------
// Arms 9/10: bytealike / byteanlike (oids 2005/2006) — raw bytes
// ---------------------------------------------------------------------------

macro_rules! bytea_match_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident) => {
        fn $fn_name(f: &Frame<'_>) {
            let mut cval = 0i32;
            let cst = unsafe {
                $cfn(cptr(f.a), f.a.len() as i32, cptr(f.b), f.b.len() as i32, &mut cval)
            };
            let core = adt_like::$core(f.a, f.b);
            compare_bool_planes(
                stringify!($core),
                &format!("s={:?} p={:?}", f.a, f.b),
                cst,
                cval,
                &core,
            );
            // fc plane (collation is ignored by the core; pass it anyway —
            // the executor would).
            let (si, pi) = (text_image(f.a), text_image(f.b));
            fc_expect(
                stringify!($fc),
                lb::$fc,
                None,
                f.coll,
                None,
                [
                    Datum::from_usize(si.as_ptr() as usize),
                    Datum::from_usize(pi.as_ptr() as usize),
                ],
                &core,
            );
        }
    };
}

bytea_match_arm!(bytealike_diff, pg_diff_like_bytealike, bytealike, fc_bytealike);
bytea_match_arm!(byteanlike_diff, pg_diff_like_byteanlike, byteanlike, fc_byteanlike);

// ---------------------------------------------------------------------------
// Arms 8/11: like_escape / like_escape_bytea (oids 1637/2009)
// ---------------------------------------------------------------------------

macro_rules! escape_arm {
    ($fn_name:ident, $cfn:ident, $core:ident, $fc:ident, $check_text:expr) => {
        fn $fn_name(f: &Frame<'_>) {
            if $check_text && (!text_ok(f.a, f.utf8) || !text_ok(f.b, f.utf8)) {
                return;
            }
            let mut cbuf = vec![0u8; f.a.len() * 2 + 4];
            let mut coutlen = 0i32;
            let cst = unsafe {
                $cfn(
                    cptr(f.a),
                    f.a.len() as i32,
                    cptr(f.b),
                    f.b.len() as i32,
                    cbuf.as_mut_ptr() as *mut c_char,
                    &mut coutlen,
                )
            };
            let mut rbuf: Vec<u8> = Vec::new();
            let core = adt_like::$core(f.a, f.b, &mut rbuf);
            let ctx = format!("pat={:?} esc={:?} utf8={}", f.a, f.b, f.utf8);
            match &core {
                Ok(()) => assert!(
                    cst == 0 && &cbuf[..coutlen as usize] == &rbuf[..],
                    "{} DIVERGENCE {ctx}: C=(st {cst}, {:?}) Rust=Ok({:?})",
                    stringify!($core),
                    &cbuf[..coutlen.max(0) as usize],
                    &rbuf[..]
                ),
                Err(e) => {
                    let rc = rust_err_class(e);
                    assert!(
                        cst == rc && cst != 0,
                        "{} DIVERGENCE {ctx}: C=(st {cst}) Rust=Err(class {rc}, {})",
                        stringify!($core),
                        e.message
                    );
                }
            }
            // fc plane: result varlena rides the resolved FmgrInfo's OutBuf
            // scratch; compare BEFORE the reuse call rewrites the buffer.
            let (pi, ei) = (text_image(f.a), text_image(f.b));
            let args = [
                Datum::from_usize(pi.as_ptr() as usize),
                Datum::from_usize(ei.as_ptr() as usize),
            ];
            let mut fl = FmgrInfo::unresolved();
            for pass in 0..2 {
                match (fc_call(lb::$fc, Some(&mut fl), f.coll, None, args), &core) {
                    (Ok(d), Ok(())) => assert!(
                        read_varlena_data(d) == &rbuf[..],
                        "{} fc-wrapper DIVERGENCE (pass {pass}) {ctx}",
                        stringify!($fc)
                    ),
                    (Err(we), Err(ce)) => assert!(
                        we.sqlstate == ce.sqlstate,
                        "{} fc-wrapper sqlstate DIVERGENCE (pass {pass}) {ctx}: {:?} vs {:?}",
                        stringify!($fc),
                        we.sqlstate,
                        ce.sqlstate
                    ),
                    (Ok(_), Err(_)) | (Err(_), Ok(())) => panic!(
                        "{} fc-wrapper verdict DIVERGENCE (pass {pass}) {ctx}",
                        stringify!($fc)
                    ),
                }
            }
        }
    };
}

escape_arm!(like_escape_diff, pg_diff_like_escape, like_escape_into, fc_like_escape, true);
escape_arm!(
    like_escape_bytea_diff,
    pg_diff_like_escape_bytea,
    like_escape_bytea_into,
    fc_like_escape_bytea,
    false
);

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a driver input: [sel][mode][l0][l1][a][b].
    fn frame(sel: u8, mode: u8, a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut d = vec![sel, mode];
        d.extend_from_slice(&(a.len() as u16).to_le_bytes());
        d.extend_from_slice(a);
        d.extend_from_slice(b);
        d
    }

    fn drive(sel: u8, mode: u8, a: &[u8], b: &[u8]) {
        like_diff(&frame(sel, mode, a, b));
    }

    /// (text, pattern) pairs exercising escape/wildcard interactions —
    /// mirrored into the seed corpus by tests::write_seed_corpus_shapes.
    fn witness_pairs() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            (b"abc", b"abc"),
            (b"abc", b"a%"),
            (b"abc", b"%"),
            (b"abc", b"%%"),
            (b"abc", b"%b%"),
            (b"abc", b"_bc"),
            (b"abc", b"a_c"),
            (b"abc", b"%_"),
            (b"abc", b"_%"),
            (b"abc", b"%_%_%"),
            (b"abc", b"\\a%"),
            (b"a%c", b"a\\%c"),
            (b"a_c", b"a\\_c"),
            (b"a\\c", b"a\\\\c"),
            (b"abc", b"abc\\"),   // trailing escape -> 22025 both sides
            (b"abc", b"%\\"),     // trailing escape after % -> 22025
            (b"abc", b"%\\c"),    // escape-before-wildcard-scan literal
            (b"abc", b"\\%"),     // escaped % must match literal %
            (b"%", b"\\%"),
            (b"_", b"\\_"),
            (b"aaab", b"%ab"),
            (b"aaaa", b"a%a%a"),
            (b"", b""),
            (b"", b"%"),
            (b"", b"%%%"),
            (b"", b"_"),
            (b"x", b""),
            ("héllo".as_bytes(), "h_llo".as_bytes()), // 2-byte char under _
            ("héllo".as_bytes(), "h%o".as_bytes()),
            ("éé".as_bytes(), "_%".as_bytes()),
            ("é".as_bytes(), "_".as_bytes()),
            ("\u{20ac}x".as_bytes(), "_x".as_bytes()), // 3-byte char
            ("\u{1f409}".as_bytes(), "_".as_bytes()),  // 4-byte char
            ("aé".as_bytes(), "a\u{0301}%".as_bytes()), // mb after wildcard
        ]
    }

    /// Every arm, ok + error shapes, both encoding planes, both collations
    /// (fc plane rides inside every drive).
    #[test]
    fn arms_smoke() {
        for sel in 0u8..15 {
            for mode in 0u8..4 {
                for (t, p) in witness_pairs() {
                    drive(sel, mode, t, p);
                }
            }
        }
    }

    /// Direct kernel arms (12..14): raw-byte domain (invalid UTF-8 legal),
    /// the ABORT tristate, both locale selections, trailing-escape error,
    /// and ASCII case folding through SB_IMatchText.
    #[test]
    fn kernel_arms_smoke() {
        for sel in [12u8, 13, 14] {
            for mode in 0u8..4 {
                drive(sel, mode, b"abc", b"a%c");
                drive(sel, mode, b"abc", b"%zz"); // %-scan exhausts text: ABORT
                drive(sel, mode, b"abc", b"ab"); // text longer than pattern
                drive(sel, mode, b"ab", b"ab_"); // pattern longer: ABORT tail
                drive(sel, mode, b"abc", b"abc\\"); // trailing escape: 22025
                drive(sel, mode, b"AbC", b"a_c"); // folds only under arm 14
                drive(sel, mode, b"\xff\xfe\x80", b"_%"); // raw non-UTF8 bytes
                drive(sel, mode, "é".as_bytes(), b"_"); // _ char-width differs by arm
                drive(sel, mode, b"", b"%%");
            }
        }
    }

    /// like_escape-specific shapes: empty / 1-char / multibyte / 2-char
    /// (error) escapes, doubled escapes, bytea with embedded NUL.
    #[test]
    fn escape_arm_smoke() {
        for sel in [8u8, 11] {
            for mode in 0u8..2 {
                drive(sel, mode, b"a%b_c", b"");       // empty esc: double backslashes
                drive(sel, mode, b"a\\b", b"");
                drive(sel, mode, b"a#%b#_c", b"#");    // custom escape
                drive(sel, mode, b"a\\%b", b"#");      // backslash doubling
                drive(sel, mode, b"##", b"#");         // doubled escape chars
                drive(sel, mode, b"a%b", b"\\");       // '\' escape: copy as-is
                drive(sel, mode, b"a%b", b"xy");       // 2-char escape -> 22025
                drive(sel, mode, b"", b"x");
                drive(sel, mode, b"", b"");
            }
        }
        // multibyte escape char (UTF8 plane): 1 char / 2 bytes is legal for
        // text like_escape, and its bytes are 2 chars (error) for bytea.
        drive(8, 0, "a%b".as_bytes(), "é".as_bytes());
        drive(11, 0, "a%b".as_bytes(), "é".as_bytes());
        // multibyte pattern chars through CopyAdvChar
        drive(8, 0, "é%é_é".as_bytes(), "#".as_bytes());
        drive(8, 0, "éé".as_bytes(), "é".as_bytes()); // esc char == pattern char
        // bytea: embedded NUL is data
        drive(11, 0, b"a\0%b", b"");
        drive(11, 0, b"a\0b", b"\0");
        drive(9, 0, b"a\0b", b"a\0%");
        drive(10, 0, b"a\0b", b"a_b");
    }

    /// Collation plane: InvalidOid -> 42P22 on both sides for every arm that
    /// consults collation; bytea/escape arms ignore it.
    #[test]
    fn indeterminate_collation_smoke() {
        for sel in [0u8, 1, 2, 3, 4, 5, 6, 7] {
            drive(sel, 2, b"abc", b"a%");
        }
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). Corpora are COMMITTED (plain `git add`).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/like_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/like_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                like_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Fuzz-shaped byte soup through every selector: the whole driver must
    /// be panic-free on arbitrary input (asserts fire only on divergence).
    #[test]
    fn selector_soup() {
        for sel in 0u8..=24 {
            for len in [0usize, 1, 2, 3, 4, 7, 16, 65, 130, 300] {
                let payload: Vec<u8> = (0..len)
                    .map(|i| (i as u8).wrapping_mul(31).wrapping_add(sel))
                    .collect();
                let mut d = vec![sel];
                d.extend_from_slice(&payload);
                like_diff(&d);
            }
        }
        like_diff(&[]);
    }

    /// EXHAUSTIVE KERNEL SWEEP (module doc): all (text, pattern) pairs over
    /// {a, b, %, _, \} with len <= 4, UTF8 plane, C collation, through the
    /// textlike arm — full enumeration vs the C oracle, three planes.
    /// 609,961 pairs; wall time recorded in the module header.
    #[test]
    #[ignore = "exhaustive sweep: run explicitly via -- --ignored (~1-2 s)"]
    fn exhaustive_kernel_sweep() {
        let _serial = crate::c_oracle_serial();
        const ALPHABET: [u8; 5] = *b"ab%_\\";
        let mut words: Vec<Vec<u8>> = vec![Vec::new()];
        let mut layer: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..4 {
            let mut next = Vec::new();
            for w in &layer {
                for &c in &ALPHABET {
                    let mut w2 = w.clone();
                    w2.push(c);
                    next.push(w2);
                }
            }
            words.extend(next.iter().cloned());
            layer = next;
        }
        assert_eq!(words.len(), 781);
        // Pin the environment once (textlike_diff assumes decode() ran).
        mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
        unsafe { pg_diff_like_set_encoding(1) };
        let start = std::time::Instant::now();
        let mut n = 0u64;
        for t in &words {
            for p in &words {
                let f = Frame { a: t, b: p, utf8: true, coll: C_COLLATION_OID };
                textlike_diff(&f);
                n += 1;
            }
        }
        eprintln!("exhaustive_kernel_sweep: {n} pairs in {:?}", start.elapsed());
        assert_eq!(n, 609_961);
    }
}
