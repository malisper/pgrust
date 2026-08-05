//! vltext_diff: differential fuzz driver — shipped Rust `varlena` (text
//! family) vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df)
//! C (csrc/pg_vltext_io.c). Crate under test:
//! crates/backend/utils/adt/varlena.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//! Any mismatch panics — libFuzzer minimizes that into the divergence
//! reproducer.
//!
//! ENVIRONMENT FENCE (pins the environment, never the computation; mirrored
//! in the csrc/pg_vltext_io.c header):
//!   - Database encoding = UTF8 on BOTH sides (PostgreSQL's default): the
//!     driver installs the real mbutils seams and pins
//!     `SetDatabaseEncoding(PG_UTF8)` per thread, so textlen / text_substr /
//!     textpos / overlay exercise the REAL multibyte walk (pg_utf_mblen +
//!     bounded pg_mblen_range/pg_mblen_with_len, 22021 on truncated
//!     sequences) against the verbatim vendored equivalents.
//!   - Collation = C collation (C_COLLATION_OID 950) for all
//!     comparison/hash functions; locale/ICU arms are OUT of scope per the
//!     campaign carve. InvalidOid (0) IS driven (flag nibble 0xF) to pin the
//!     42P22 check_collation_set arm on both sides.
//!   - Client encoding stays SQL_ASCII (pgrust default): recv validates
//!     wire bytes against UTF8 (22021), send is the identity.
//!
//! Input layout: [selector][payload]; selector % 36 picks the arm. Common
//! decoders: `[flag]` = 1 byte, collation flag (low nibble 0xF ⇒ InvalidOid,
//! else C collation); `[split]` = 2 bytes u16 LE, first-text length modulo
//! the remaining payload; `[i32]` = 3 bytes band decode (tag byte + u16:
//! tag%8==7 selects a raw-extreme table incl. i32::MIN/i32::MAX, else a
//! small ±2048 band) so substr/overlay integer params hit INT_MIN /
//! negative / overflow edges without multi-MB allocations. Text payloads are
//! capped at 2 KiB (replace_text: src/from 1 KiB, to 8 B) so per-exec
//! allocations stay small.
//!   0 textin             [cstring bytes] (NUL-free; skipped otherwise)
//!   1 textout            [text bytes] (embedded NULs are data)
//!   2 textlen            [text bytes] (22021 on truncated multibyte)
//!   3 textoctetlen       [text bytes]
//!   4 textcat            [split][t1][t2] (1 KiB caps)
//!   5 text_substr        [i32 start][i32 length][text]
//!   6 text_substr_no_len [i32 start][text]
//!   7 textpos            [flag][split][t1][t2]
//!   8..13 texteq/ne/lt/le/gt/ge  [flag][split][t1][t2]
//!   14 bttextcmp         [flag][split][t1][t2] (raw memcmp magnitude)
//!   15 text_larger       [flag][split][t1][t2] (which argument returned)
//!   16 text_smaller      [flag][split][t1][t2]
//!   17..20 text_pattern_lt/le/ge/gt  [split][t1][t2]
//!   21 bttext_pattern_cmp [split][t1][t2] (raw memcmp magnitude)
//!   22 btvarstrequalimage [flag]
//!   23 text_starts_with  [flag][split][t1][t2] — INPUT FENCE: t1/t2 must be
//!      valid UTF-8 and NUL-free (executable gate below). PG's invariant is
//!      that server-encoded text is validated at ingest; on that domain the
//!      shipped byte-prefix fast path and C's text_substring walk agree
//!      byte-for-byte. On invalid encodings C's walk can raise 22021 where
//!      the byte compare cannot — an un-ingestible input, not a conformance
//!      surface. Every other multibyte-walking arm feeds ARBITRARY bytes
//!      (both sides implement the identical walk).
//!   24 replace_text      [flag][2B l1][2B l2][src][from][to]
//!   25 split_part        [flag][i32 fldnum][split][str][sep]
//!   26 textoverlay       [i32 sp][i32 sl][split][t1][t2]
//!   27 textoverlay_no_len [i32 sp][split][t1][t2]
//!   28 textsend          [text bytes] (full bytea wire image compared)
//!   29 textrecv          [wire bytes] (22021 validation on both sides)
//!   30 unknownin         [cstring bytes] (NUL-free)
//!   31 unknownout        [cstring bytes] (NUL-free)
//!   32 unknownrecv       [wire bytes]
//!   33 unknownsend       [cstring bytes] (NUL-free)
//!   34 hashtext          [flag][text] (vendored hash_bytes C parity)
//!   35 hashtextextended  [flag][8B seed][text]
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame (collation set on the frame) and asserts
//! wrapper == core (Datum value / returned bytes / error verdict +
//! sqlstate). C-parity keeps being carried by the core comparison; the
//! plane makes the wrapper lines (including the error wrappers, via the
//! InvalidOid flag and the substr/overlay/split error decodes) execute
//! every iteration with an in-harness oracle. Executed: fc_textin (incl.
//! the retained-scratch reuse arm), fc_textout, fc_textlen,
//! fc_textoctetlen, fc_textcat, fc_text_substr, fc_text_substr_no_len,
//! fc_textpos, fc_texteq/textne/text_lt/text_le/text_gt/text_ge,
//! fc_bttextcmp, fc_text_larger, fc_text_smaller, fc_text_pattern_lt/le/
//! ge/gt, fc_bttext_pattern_cmp, fc_btvarstrequalimage,
//! fc_text_starts_with, fc_replace_text, fc_split_part, fc_textoverlay,
//! fc_textoverlay_no_len, fc_textsend, fc_textrecv, fc_unknownin,
//! fc_unknownout, fc_unknownrecv, fc_unknownsend, fc_hashtext,
//! fc_hashtextextended.
//!
//! SKIPPED rows: none of the 36 chartered functions is skipped. The only
//! input-domain carve is arm 23's valid-UTF-8/NUL-free fence documented
//! above (executable gate, not a comment-only carve). Locale/ICU collation
//! arms are outside this target's charter (campaign carve of record; the
//! C-collation fence above).

use std::ffi::{c_char, CString};
use std::sync::Once;

use datum::{Datum, NullableDatum};
use stringinfo::StringInfo;
use types_core::{Oid, C_COLLATION_OID};
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_SUBSTRING_ERROR,
};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction, PackedVarlena};
use varlena::builtins as vb;

extern "C" {
    fn pg_diff_vltext_textin(s: *const c_char, out: *mut u8, outcap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_vltext_textout(
        t: *const u8,
        len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_textlen(t: *const u8, len: i32, result: *mut i32) -> i32;
    fn pg_diff_vltext_textoctetlen(t: *const u8, len: i32, result: *mut i32) -> i32;
    fn pg_diff_vltext_textcat(
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_substr(
        t: *const u8,
        len: i32,
        start: i32,
        length: i32,
        no_len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_textpos(
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        collid: u32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_cmpop(
        op: i32,
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        collid: u32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_minmax(
        larger: i32,
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        collid: u32,
        which: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_patcmp(
        op: i32,
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_btvarstrequalimage(collid: u32, result: *mut i32) -> i32;
    fn pg_diff_vltext_starts_with(
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        collid: u32,
        result: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_replace_text(
        src: *const u8,
        lsrc: i32,
        from: *const u8,
        lfrom: i32,
        to: *const u8,
        lto: i32,
        collid: u32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_split_part(
        s: *const u8,
        lstr: i32,
        sep: *const u8,
        lsep: i32,
        fldnum: i32,
        collid: u32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_overlay(
        t1: *const u8,
        l1: i32,
        t2: *const u8,
        l2: i32,
        sp: i32,
        sl: i32,
        no_len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_textsend(
        t: *const u8,
        len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_textrecv(
        data: *const u8,
        len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_unknowninout(
        s: *const c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_unknownrecv(
        data: *const u8,
        len: i32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_unknownsend(
        s: *const c_char,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_vltext_hashtext(t: *const u8, len: i32, collid: u32, result: *mut u32) -> i32;
    fn pg_diff_vltext_hashtextextended(
        t: *const u8,
        len: i32,
        collid: u32,
        seed: u64,
        result: *mut u64,
    ) -> i32;
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
}

/// Oracle error classes (csrc/pg_vltext_io.c header).
const C_ERR_SUBSTRING: i32 = 1; /* 22011 */
const C_ERR_OUT_OF_RANGE: i32 = 2; /* 22003 */
const C_ERR_INVALID_PARAM: i32 = 3; /* 22023 */
const C_ERR_NOT_IN_REPERTOIRE: i32 = 4; /* 22021 */
const C_ERR_PROTOCOL: i32 = 5; /* 08P01 */
const C_ERR_INDET_COLLATION: i32 = 6; /* 42P22 */

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_SUBSTRING_ERROR {
        C_ERR_SUBSTRING
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        C_ERR_INVALID_PARAM
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_NOT_IN_REPERTOIRE
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL
    } else if e.sqlstate == ERRCODE_INDETERMINATE_COLLATION {
        C_ERR_INDET_COLLATION
    } else {
        99
    }
}

/// Payload caps: keep per-exec allocations small (fleet throughput).
const MAX_TEXT: usize = 2048;
/// textpos-only haystack cap: reaches text_position_setup's >=4096
/// skip-table stride arm (varlena.c), unreachable under MAX_TEXT.
const MAX_TEXT_POS_HAYSTACK: usize = 6144;
const MAX_REPLACE_SRC: usize = 1024;
const MAX_REPLACE_TO: usize = 8;
/// C out-buffer size covering every capped arm's worst case (replace_text:
/// 1024 matches x 8 bytes + residual; textcat: 2 x 1 KiB; others <= 2 KiB+4).
const C_OUT_CAP: usize = 16384;

// The shipped text_length/text_substring/textpos go through the mbutils
// seams; install the REAL mbutils implementations (what production boot
// installs) once per process, and pin the thread's database encoding to UTF8
// (client encoding stays at its SQL_ASCII default => identity conversion +
// mandatory validation on recv). Same posture as name_diff.rs.
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
// wrapper-level pattern run without kani; name_diff.rs conventions).
// ---------------------------------------------------------------------------

/// One native fmgr call: N non-null arg Datums, optional resolved FmgrInfo
/// (for the retained-scratch cstring/text results), explicit collation,
/// optional armed result mcx.
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    coll: Oid,
    m: Option<mcx::Mcx<'_>>,
    args: [Datum; N],
) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(coll);
    if let Some(m) = m {
        // SAFETY: the context owning `m` outlives this single call.
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

/// text/varlena arg construction: inline 4B-uncompressed header + body (the
/// shipped set_varsize_4b encoding; bodies are capped, the length always
/// fits).
fn text_image(body: &[u8]) -> Vec<u8> {
    let mut img = Vec::with_capacity(body.len() + 4);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(body.len() + 4));
    img.extend_from_slice(body);
    img
}

fn img_datum(img: &[u8]) -> Datum {
    Datum::from_usize(img.as_ptr() as usize)
}

/// Varlena result readback (text/bytea payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena /
    // retained scratch, read before their owner drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// First `n` bytes behind a by-ref result Datum (cstring results of known
/// length). Caller contract: the allocation is still live.
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: caller contract above.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}

/// Wrapper outcome == core outcome, both PgResult, value plane via `eq`.
fn assert_fc_matches<T, F>(fname: &str, wrapper: PgResult<Datum>, core: &PgResult<T>, eq: F)
where
    F: FnOnce(Datum, &T) -> bool,
{
    match (wrapper, core) {
        (Ok(d), Ok(v)) => assert!(eq(d, v), "{fname} fc-wrapper value DIVERGENCE vs core"),
        (Err(we), Err(ce)) => assert!(
            rust_err_class(&we) == rust_err_class(ce),
            "{fname} fc-wrapper sqlstate DIVERGENCE: wrapper={:?} core={:?}",
            we.sqlstate,
            ce.sqlstate
        ),
        (Ok(_), Err(ce)) => panic!("{fname} fc-wrapper Ok but core Err({:?})", ce.sqlstate),
        (Err(we), Ok(_)) => panic!("{fname} fc-wrapper Err({:?}) but core Ok", we.sqlstate),
    }
}

/// C verdict (cst, cerr) == Rust core verdict, value plane via `eq`.
fn assert_c_matches<T, F>(fname: &str, cst: i32, cerr: i32, core: &PgResult<T>, eq: F)
where
    F: FnOnce(&T) -> bool,
{
    match core {
        Ok(v) => assert!(
            cst == 0 && eq(v),
            "{fname} DIVERGENCE: C=(st {cst}, err {cerr}) but Rust Ok with value mismatch or C errored"
        ),
        Err(e) => {
            let rerr = rust_err_class(e);
            assert!(
                cst == 1 && cerr == rerr,
                "{fname} DIVERGENCE: C=(st {cst}, err {cerr}) Rust=Err(class {rerr}, {:?})",
                e.sqlstate
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Payload decoders (documented in the module header).
// ---------------------------------------------------------------------------

/// Collation flag: low nibble 0xF => InvalidOid (42P22 arm), else C collation.
fn coll_of(flag: u8) -> Oid {
    if flag & 0x0F == 0x0F {
        0
    } else {
        C_COLLATION_OID
    }
}

/// Band decode for i32 params: 3 bytes = [tag][u16 LE]. tag%8==7 injects raw
/// extremes (INT_MIN/INT_MAX and friends); otherwise a small +/-2048 band.
fn decode_i32(b: &[u8]) -> Option<(i32, &[u8])> {
    let (head, rest) = (b.get(..3)?, &b[3..]);
    let tag = head[0];
    let v = u16::from_le_bytes([head[1], head[2]]);
    let val = if tag % 8 == 7 {
        const EXTREMES: [i32; 10] = [
            i32::MIN,
            i32::MIN + 1,
            i32::MIN / 2,
            -65536,
            -1,
            0,
            1,
            i32::MAX / 2,
            i32::MAX - 1,
            i32::MAX,
        ];
        EXTREMES[usize::from(v) % EXTREMES.len()]
    } else {
        i32::from(v % 4096) - 2048
    };
    Some((val, rest))
}

/// Two texts: [u16 LE first-length % (rest+1)][t1][t2], both capped.
fn split_two(b: &[u8], cap: usize) -> Option<(&[u8], &[u8])> {
    let head = b.get(..2)?;
    let rest = &b[2..];
    let l = usize::from(u16::from_le_bytes([head[0], head[1]])) % (rest.len() + 1);
    let (a, t) = rest.split_at(l);
    (a.len() <= cap && t.len() <= cap).then_some((a, t))
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn vltext_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics) —
    // the fuzz TARGET's own frame stack needs the lock, same driver-entry
    // idiom as every other pub *_diff (task #144 addendum, trgm precedent).
    let _oracle = crate::oracle_serial();

    setup();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 36 {
        0 => textin_diff(payload),
        1 => textout_diff(payload),
        2 => textlen_diff(payload),
        3 => textoctetlen_diff(payload),
        4 => textcat_diff(payload),
        5 => substr_diff(payload, false),
        6 => substr_diff(payload, true),
        7 => textpos_diff(payload),
        8 => cmp_diff(0, payload),
        9 => cmp_diff(1, payload),
        10 => cmp_diff(2, payload),
        11 => cmp_diff(3, payload),
        12 => cmp_diff(4, payload),
        13 => cmp_diff(5, payload),
        14 => cmp_diff(6, payload),
        15 => minmax_diff(true, payload),
        16 => minmax_diff(false, payload),
        17 => pattern_diff(0, payload),
        18 => pattern_diff(1, payload),
        19 => pattern_diff(2, payload),
        20 => pattern_diff(3, payload),
        21 => pattern_diff(4, payload),
        22 => btvarstrequalimage_diff(payload),
        23 => text_starts_with_diff(payload),
        24 => replace_text_diff(payload),
        25 => split_part_diff(payload),
        26 => textoverlay_diff(payload, false),
        27 => textoverlay_diff(payload, true),
        28 => textsend_diff(payload),
        29 => textrecv_diff(payload),
        30 => unknownin_diff(payload),
        31 => unknownout_diff(payload),
        32 => unknownrecv_diff(payload),
        33 => unknownsend_diff(payload),
        34 => hashtext_diff(payload),
        _ => hashtextextended_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: textin (oid 46) — cstring in, text image out. Infallible.
// ---------------------------------------------------------------------------

fn textin_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return; /* cstring contract */
    }
    let cs = CString::new(payload).unwrap();
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textin(cs.as_ptr(), out.as_mut_ptr(), C_OUT_CAP as i32, &mut outlen)
    };
    let cbytes = &out[..outlen as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let r = varlena::textin(m, payload).expect("textin is infallible at capped sizes");
    assert!(
        cst == 0 && r.data() == cbytes,
        "textin DIVERGENCE input={payload:?}: C={cbytes:?} Rust={:?}",
        r.data()
    );

    // fc plane: fresh + retained-scratch reuse call through one FmgrInfo.
    let mut fl = FmgrInfo::unresolved();
    for pass in 0..2 {
        let d = fc_call(
            vb::fc_textin,
            Some(&mut fl),
            0,
            None,
            [Datum::from_usize(cs.as_ptr() as usize)],
        )
        .expect("fc_textin is infallible");
        assert!(
            read_varlena_data(d) == r.data(),
            "fc_textin vs core DIVERGENCE (pass {pass}) input={payload:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 1: textout (oid 47) — text in, cstring out (embedded NULs are data).
// ---------------------------------------------------------------------------

fn textout_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textout(
            payload.as_ptr(),
            payload.len() as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    // C wrote outlen payload bytes + a trailing NUL.
    let cbytes = &out[..outlen as usize + 1];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let r = varlena::textout(m, payload).expect("textout is infallible at capped sizes");
    assert!(
        cst == 0 && &r[..] == cbytes,
        "textout DIVERGENCE input={payload:?}: C={cbytes:?} Rust={:?}",
        &r[..]
    );

    // fc plane (retained scratch; result = payload + NUL).
    let img = text_image(payload);
    let mut fl = FmgrInfo::unresolved();
    let d = fc_call(vb::fc_textout, Some(&mut fl), 0, None, [img_datum(&img)])
        .expect("fc_textout is infallible");
    assert!(
        datum_bytes(d, payload.len() + 1) == &r[..],
        "fc_textout vs core DIVERGENCE input={payload:?}"
    );
}

// ---------------------------------------------------------------------------
// Arm 2: textlen (oid 1257) — UTF8 char count; 22021 on truncated multibyte.
// ---------------------------------------------------------------------------

fn textlen_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut cval = 0i32;
    let cst =
        unsafe { pg_diff_vltext_textlen(payload.as_ptr(), payload.len() as i32, &mut cval) };
    let cerr = c_errcode();

    let core = varlena::text_length(payload);
    assert_c_matches("textlen", cst, cerr, &core, |v| *v == cval);

    let img = text_image(payload);
    let w = fc_call(vb::fc_textlen, None, 0, None, [img_datum(&img)]);
    assert_fc_matches("fc_textlen", w, &core, |d, v| d.as_i32() == *v);
}

// ---------------------------------------------------------------------------
// Arm 3: textoctetlen (oid 1374).
// ---------------------------------------------------------------------------

fn textoctetlen_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut cval = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textoctetlen(payload.as_ptr(), payload.len() as i32, &mut cval)
    };
    let rv = varlena::textoctetlen(payload);
    assert!(
        cst == 0 && cval == rv,
        "textoctetlen DIVERGENCE len={}: C={cval} Rust={rv}",
        payload.len()
    );

    let img = text_image(payload);
    let d = fc_call(vb::fc_textoctetlen, None, 0, None, [img_datum(&img)])
        .expect("fc_textoctetlen is infallible");
    assert!(
        d.as_i32() == rv,
        "fc_textoctetlen fc-wrapper DIVERGENCE: wrapper={} core={rv}",
        d.as_i32()
    );
}

// ---------------------------------------------------------------------------
// Arm 4: textcat (oid 1258).
// ---------------------------------------------------------------------------

fn textcat_diff(payload: &[u8]) {
    let Some((t1, t2)) = split_two(payload, MAX_TEXT / 2) else {
        return;
    };
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textcat(
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cbytes = &out[..outlen as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let r = varlena::text_catenate(m, t1, t2).expect("textcat is infallible at capped sizes");
    assert!(
        cst == 0 && r.data() == cbytes,
        "textcat DIVERGENCE l1={} l2={}: C={cbytes:?} Rust={:?}",
        t1.len(),
        t2.len(),
        r.data()
    );

    let (i1, i2) = (text_image(t1), text_image(t2));
    let d = fc_call(
        vb::fc_textcat,
        None,
        0,
        Some(m),
        [img_datum(&i1), img_datum(&i2)],
    )
    .expect("fc_textcat allocation");
    assert!(
        read_varlena_data(d) == r.data(),
        "fc_textcat vs core DIVERGENCE"
    );
}

// ---------------------------------------------------------------------------
// Arms 5/6: text_substr (oid 877) / text_substr_no_len (oid 883).
// Errors: 22011 negative length, 22021 truncated multibyte.
// ---------------------------------------------------------------------------

fn substr_diff(payload: &[u8], no_len: bool) {
    let Some((start, rest)) = decode_i32(payload) else {
        return;
    };
    let (length, text) = if no_len {
        (0, rest)
    } else {
        let Some((l, rest2)) = decode_i32(rest) else {
            return;
        };
        (l, rest2)
    };
    if text.len() > MAX_TEXT {
        return;
    }

    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_substr(
            text.as_ptr(),
            text.len() as i32,
            start,
            length,
            no_len as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..outlen.max(0) as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let img = text_image(text);
    let core = varlena::text_substring(m, &img, start, if no_len { -1 } else { length }, no_len);
    assert_c_matches(
        if no_len { "text_substr_no_len" } else { "text_substr" },
        cst,
        cerr,
        &core,
        |v| v.data() == cbytes,
    );

    let w = if no_len {
        fc_call(
            vb::fc_text_substr_no_len,
            None,
            0,
            Some(m),
            [img_datum(&img), Datum::from_i32(start)],
        )
    } else {
        fc_call(
            vb::fc_text_substr,
            None,
            0,
            Some(m),
            [img_datum(&img), Datum::from_i32(start), Datum::from_i32(length)],
        )
    };
    assert_fc_matches("fc_text_substr", w, &core, |d, v| {
        read_varlena_data(d) == v.data()
    });
}

// ---------------------------------------------------------------------------
// Arm 7: textpos (oid 849). Errors: 42P22 (flag), 22021 (match-pos walk).
// ---------------------------------------------------------------------------

fn textpos_diff(payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    // BIG-HAYSTACK MODE (lane p1-lanes): text_position_setup picks its B-M-H
    // skip-table stride from the search length (127 below 4096, 255 at or
    // above it — varlena.c). Under the shared 2 KiB cap that >=4096 arm is
    // unreachable, so this arm alone accepts a 6 KiB haystack, with the
    // needle kept short (the stride choice depends on haystack length only).
    let Some((t1, t2)) = split_two(rest, MAX_TEXT_POS_HAYSTACK) else {
        return;
    };
    // Above the shared cap the needle stays small: the stride choice depends
    // on the haystack length alone, and a big*big pair only costs execs.
    if t2.len() > MAX_TEXT || (t1.len() > MAX_TEXT && t2.len() > 64) {
        return;
    }
    let mut cval = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textpos(
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            coll,
            &mut cval,
        )
    };
    let cerr = c_errcode();

    let core = varlena::textpos(t1, t2, coll);
    assert_c_matches("textpos", cst, cerr, &core, |v| *v == cval);

    let (i1, i2) = (text_image(t1), text_image(t2));
    let w = fc_call(
        vb::fc_textpos,
        None,
        coll,
        None,
        [img_datum(&i1), img_datum(&i2)],
    );
    assert_fc_matches("fc_textpos", w, &core, |d, v| d.as_i32() == *v);
}

// ---------------------------------------------------------------------------
// Arms 8..14: texteq/textne/text_lt/le/gt/ge (bool) + bttextcmp (raw i32
// magnitude), C collation or InvalidOid via the flag byte.
// ---------------------------------------------------------------------------

fn cmp_diff(op: i32, payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let Some((t1, t2)) = split_two(rest, MAX_TEXT) else {
        return;
    };
    let mut cval = 0i32;
    let cst = unsafe {
        pg_diff_vltext_cmpop(
            op,
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            coll,
            &mut cval,
        )
    };
    let cerr = c_errcode();

    let (fname, wrapper, core_i32): (&str, PGFunction, PgResult<i32>) = match op {
        0 => ("texteq", vb::fc_texteq, varlena::texteq(t1, t2, coll).map(i32::from)),
        1 => ("textne", vb::fc_textne, varlena::textne(t1, t2, coll).map(i32::from)),
        2 => ("text_lt", vb::fc_text_lt, varlena::text_lt(t1, t2, coll).map(i32::from)),
        3 => ("text_le", vb::fc_text_le, varlena::text_le(t1, t2, coll).map(i32::from)),
        4 => ("text_gt", vb::fc_text_gt, varlena::text_gt(t1, t2, coll).map(i32::from)),
        5 => ("text_ge", vb::fc_text_ge, varlena::text_ge(t1, t2, coll).map(i32::from)),
        _ => ("bttextcmp", vb::fc_bttextcmp, varlena::bttextcmp(t1, t2, coll)),
    };
    assert_c_matches(fname, cst, cerr, &core_i32, |v| *v == cval);

    let (i1, i2) = (text_image(t1), text_image(t2));
    let w = fc_call(wrapper, None, coll, None, [img_datum(&i1), img_datum(&i2)]);
    assert_fc_matches(fname, w, &core_i32, |d, v| {
        if op == 6 {
            d.as_i32() == *v
        } else {
            i32::from(d.as_bool()) == *v
        }
    });
}

// ---------------------------------------------------------------------------
// Arms 15/16: text_larger (oid 458) / text_smaller (oid 459) — which
// argument is returned (C returns one of the argument pointers).
// ---------------------------------------------------------------------------

fn minmax_diff(larger: bool, payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let Some((t1, t2)) = split_two(rest, MAX_TEXT) else {
        return;
    };
    let mut cwhich = 0i32;
    let cst = unsafe {
        pg_diff_vltext_minmax(
            larger as i32,
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            coll,
            &mut cwhich,
        )
    };
    let cerr = c_errcode();
    let fname = if larger { "text_larger" } else { "text_smaller" };

    let core = if larger {
        varlena::text_larger(t1, t2, coll)
    } else {
        varlena::text_smaller(t1, t2, coll)
    };
    // Which-argument plane: C returns arg1 or arg2 by pointer; the shipped
    // core returns one of the borrowed input slices. When the two slices are
    // indistinguishable (split at 0 with both empty: same ptr AND same len)
    // the tie answer is arg2 in both C and the shipped core (cmp is 0, the
    // strict < / > test fails).
    let core_which: PgResult<i32> = match core {
        Ok(s) => Ok(
            if t1.as_ptr() == t2.as_ptr() && t1.len() == t2.len() {
                2
            } else if s.as_ptr() == t1.as_ptr() && s.len() == t1.len() {
                1
            } else {
                2
            },
        ),
        Err(e) => Err(e),
    };
    assert_c_matches(fname, cst, cerr, &core_which, |v| *v == cwhich);

    let (i1, i2) = (text_image(t1), text_image(t2));
    let w = fc_call(
        if larger { vb::fc_text_larger } else { vb::fc_text_smaller },
        None,
        coll,
        None,
        [img_datum(&i1), img_datum(&i2)],
    );
    assert_fc_matches(fname, w, &core_which, |d, v| {
        let p = d.as_usize();
        (p == i1.as_ptr() as usize && *v == 1) || (p == i2.as_ptr() as usize && *v == 2)
    });
}

// ---------------------------------------------------------------------------
// Arms 17..21: text_pattern_lt/le/ge/gt (oids 2160/2161/2163/2164) +
// bttext_pattern_cmp (oid 2166) — raw memcmp magnitude, no collation leg.
// ---------------------------------------------------------------------------

fn pattern_diff(op: i32, payload: &[u8]) {
    let Some((t1, t2)) = split_two(payload, MAX_TEXT) else {
        return;
    };
    let mut cval = 0i32;
    let cst = unsafe {
        pg_diff_vltext_patcmp(
            op,
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            &mut cval,
        )
    };
    let (fname, wrapper, rv): (&str, PGFunction, i32) = match op {
        0 => ("text_pattern_lt", vb::fc_text_pattern_lt, varlena::text_pattern_lt(t1, t2).into()),
        1 => ("text_pattern_le", vb::fc_text_pattern_le, varlena::text_pattern_le(t1, t2).into()),
        2 => ("text_pattern_ge", vb::fc_text_pattern_ge, varlena::text_pattern_ge(t1, t2).into()),
        3 => ("text_pattern_gt", vb::fc_text_pattern_gt, varlena::text_pattern_gt(t1, t2).into()),
        _ => ("bttext_pattern_cmp", vb::fc_bttext_pattern_cmp, varlena::bttext_pattern_cmp(t1, t2)),
    };
    assert!(
        cst == 0 && cval == rv,
        "{fname} DIVERGENCE t1={t1:?} t2={t2:?}: C={cval} Rust={rv}"
    );

    let (i1, i2) = (text_image(t1), text_image(t2));
    let d = fc_call(wrapper, None, C_COLLATION_OID, None, [img_datum(&i1), img_datum(&i2)])
        .expect("pattern ops are infallible");
    let wv = if op == 4 { d.as_i32() } else { i32::from(d.as_bool()) };
    assert!(wv == rv, "{fname} fc-wrapper DIVERGENCE: wrapper={wv} core={rv}");
}

// ---------------------------------------------------------------------------
// Arm 22: btvarstrequalimage (oid 5050) — deterministic under C collation;
// 42P22 for InvalidOid.
// ---------------------------------------------------------------------------

fn btvarstrequalimage_diff(payload: &[u8]) {
    let Some((&flag, _)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let mut cval = 0i32;
    let cst = unsafe { pg_diff_vltext_btvarstrequalimage(coll, &mut cval) };
    let cerr = c_errcode();

    let core = varlena::btvarstrequalimage(coll);
    assert_c_matches("btvarstrequalimage", cst, cerr, &core, |v| {
        i32::from(*v) == cval
    });

    // fc plane: arg 0 is the opcintype Oid (TEXTOID; unused by the body).
    let w = fc_call(vb::fc_btvarstrequalimage, None, coll, None, [Datum::from_u32(25)]);
    assert_fc_matches("fc_btvarstrequalimage", w, &core, |d, v| d.as_bool() == *v);
}

// ---------------------------------------------------------------------------
// Arm 23: text_starts_with (oid 3696). INPUT FENCE (module header): both
// texts valid UTF-8 and NUL-free — PG's server-encoding invariant; on that
// domain the shipped byte-prefix compare and C's substring walk agree.
// ---------------------------------------------------------------------------

fn text_starts_with_diff(payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let Some((t1, t2)) = split_two(rest, MAX_TEXT) else {
        return;
    };
    // Executable input fence (see module header).
    if std::str::from_utf8(t1).is_err() || std::str::from_utf8(t2).is_err() {
        return;
    }
    if t1.contains(&0) || t2.contains(&0) {
        return;
    }
    let mut cval = 0i32;
    let cst = unsafe {
        pg_diff_vltext_starts_with(
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            coll,
            &mut cval,
        )
    };
    let cerr = c_errcode();

    let core = varlena::text_starts_with(t1, t2, coll);
    assert_c_matches("text_starts_with", cst, cerr, &core, |v| {
        i32::from(*v) == cval
    });

    let (i1, i2) = (text_image(t1), text_image(t2));
    let w = fc_call(
        vb::fc_text_starts_with,
        None,
        coll,
        None,
        [img_datum(&i1), img_datum(&i2)],
    );
    assert_fc_matches("fc_text_starts_with", w, &core, |d, v| d.as_bool() == *v);
}

// ---------------------------------------------------------------------------
// Arm 24: replace_text (oid 2087). Caps: src/from 1 KiB, to 8 B (worst-case
// output ~8 KiB; keeps fleet throughput).
// ---------------------------------------------------------------------------

fn replace_text_diff(payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let head = match rest.get(..4) {
        Some(h) => h,
        None => return,
    };
    let body = &rest[4..];
    let l1 = usize::from(u16::from_le_bytes([head[0], head[1]])) % (body.len() + 1);
    let (src, tail) = body.split_at(l1);
    let l2 = usize::from(u16::from_le_bytes([head[2], head[3]])) % (tail.len() + 1);
    let (from, to) = tail.split_at(l2);
    if src.len() > MAX_REPLACE_SRC || from.len() > MAX_REPLACE_SRC || to.len() > MAX_REPLACE_TO {
        return;
    }

    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_replace_text(
            src.as_ptr(),
            src.len() as i32,
            from.as_ptr(),
            from.len() as i32,
            to.as_ptr(),
            to.len() as i32,
            coll,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..outlen.max(0) as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let core = varlena::replace_text(m, src, from, to, coll);
    assert_c_matches("replace_text", cst, cerr, &core, |v| v.data() == cbytes);

    let (i1, i2, i3) = (text_image(src), text_image(from), text_image(to));
    let w = fc_call(
        vb::fc_replace_text,
        None,
        coll,
        Some(m),
        [img_datum(&i1), img_datum(&i2), img_datum(&i3)],
    );
    assert_fc_matches("fc_replace_text", w, &core, |d, v| {
        read_varlena_data(d) == v.data()
    });
}

// ---------------------------------------------------------------------------
// Arm 25: split_part (oid 2088). Errors: 22023 fldnum 0, 42P22 flag.
// ---------------------------------------------------------------------------

fn split_part_diff(payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let coll = coll_of(flag);
    let Some((fldnum, rest)) = decode_i32(rest) else {
        return;
    };
    let Some((s, sep)) = split_two(rest, MAX_TEXT) else {
        return;
    };
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_split_part(
            s.as_ptr(),
            s.len() as i32,
            sep.as_ptr(),
            sep.len() as i32,
            fldnum,
            coll,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..outlen.max(0) as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let core = varlena::split_part(m, s, sep, fldnum, coll);
    assert_c_matches("split_part", cst, cerr, &core, |v| v.data() == cbytes);

    let (i1, i2) = (text_image(s), text_image(sep));
    let w = fc_call(
        vb::fc_split_part,
        None,
        coll,
        Some(m),
        [img_datum(&i1), img_datum(&i2), Datum::from_i32(fldnum)],
    );
    assert_fc_matches("fc_split_part", w, &core, |d, v| {
        read_varlena_data(d) == v.data()
    });
}

// ---------------------------------------------------------------------------
// Arms 26/27: textoverlay (oid 1404) / textoverlay_no_len (oid 1405).
// Errors: 22011 sp <= 0, 22003 sp+sl overflow, 22021 multibyte.
// ---------------------------------------------------------------------------

fn textoverlay_diff(payload: &[u8], no_len: bool) {
    let Some((sp, rest)) = decode_i32(payload) else {
        return;
    };
    let (sl_raw, rest) = if no_len {
        (0, rest)
    } else {
        let Some((sl, rest2)) = decode_i32(rest) else {
            return;
        };
        (sl, rest2)
    };
    let Some((t1, t2)) = split_two(rest, MAX_TEXT / 2) else {
        return;
    };
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_overlay(
            t1.as_ptr(),
            t1.len() as i32,
            t2.as_ptr(),
            t2.len() as i32,
            sp,
            sl_raw,
            no_len as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..outlen.max(0) as usize];
    let fname = if no_len { "textoverlay_no_len" } else { "textoverlay" };

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let i1 = text_image(t1);
    // no_len: sl = textlen(t2), which itself can raise 22021 (fc wrapper
    // order of operations) — compose exactly like fc_textoverlay_no_len.
    let core = if no_len {
        varlena::text_length(t2).and_then(|sl| varlena::text_overlay(m, &i1, t2, sp, sl))
    } else {
        varlena::text_overlay(m, &i1, t2, sp, sl_raw)
    };
    assert_c_matches(fname, cst, cerr, &core, |v| v.data() == cbytes);

    let i2 = text_image(t2);
    let w = if no_len {
        fc_call(
            vb::fc_textoverlay_no_len,
            None,
            0,
            Some(m),
            [img_datum(&i1), img_datum(&i2), Datum::from_i32(sp)],
        )
    } else {
        fc_call(
            vb::fc_textoverlay,
            None,
            0,
            Some(m),
            [
                img_datum(&i1),
                img_datum(&i2),
                Datum::from_i32(sp),
                Datum::from_i32(sl_raw),
            ],
        )
    };
    assert_fc_matches(fname, w, &core, |d, v| read_varlena_data(d) == v.data());
}

// ---------------------------------------------------------------------------
// Arm 28: textsend (oid 2415) — full bytea wire image (identity conversion).
// ---------------------------------------------------------------------------

fn textsend_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textsend(
            payload.as_ptr(),
            payload.len() as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cimg = &out[..outlen as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let r = varlena::textsend(m, payload).expect("textsend is infallible on identity encoding");
    assert!(
        cst == 0 && r.as_bytes() == cimg,
        "textsend DIVERGENCE len={}: C={cimg:?} Rust={:?}",
        payload.len(),
        r.as_bytes()
    );

    let img = text_image(payload);
    let d = fc_call(vb::fc_textsend, None, 0, Some(m), [img_datum(&img)])
        .expect("fc_textsend is infallible on identity encoding");
    assert!(
        read_varlena_data(d) == r.data(),
        "fc_textsend vs core DIVERGENCE"
    );
}

// ---------------------------------------------------------------------------
// Arm 29: textrecv (oid 2414) — wire bytes; both sides validate against
// UTF8 (22021 class on invalid sequences).
// ---------------------------------------------------------------------------

fn textrecv_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_textrecv(
            payload.as_ptr(),
            payload.len() as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..outlen.max(0) as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let Some(mut si) = make_si(m, payload) else {
        return;
    };
    let core = varlena::textrecv(m, &mut si);
    assert_c_matches("textrecv", cst, cerr, &core, |v| v.data() == cbytes);

    // fc plane over a fresh StringInfo image of the same wire payload.
    let Some(mut si2) = make_si(m, payload) else {
        return;
    };
    let w = fc_call(
        vb::fc_textrecv,
        None,
        0,
        Some(m),
        [Datum::from_usize(core::ptr::from_mut(&mut si2) as usize)],
    );
    assert_fc_matches("fc_textrecv", w, &core, |d, v| {
        read_varlena_data(d) == v.data()
    });
}

// ---------------------------------------------------------------------------
// Arms 30/31: unknownin (oid 109) / unknownout (oid 110) — both are
// pstrdup of a cstring in C (one oracle entry serves both; the Rust cores
// and fc wrappers are exercised separately).
// ---------------------------------------------------------------------------

fn unknown_inout_diff(payload: &[u8], is_in: bool) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return; /* cstring contract */
    }
    let cs = CString::new(payload).unwrap();
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_unknowninout(cs.as_ptr(), out.as_mut_ptr(), C_OUT_CAP as i32, &mut outlen)
    };
    let cbytes = &out[..outlen as usize + 1]; /* payload + NUL */

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let fname = if is_in { "unknownin" } else { "unknownout" };
    let r = if is_in {
        varlena::unknownin(m, payload)
    } else {
        varlena::unknownout(m, payload)
    }
    .expect("unknown in/out are infallible at capped sizes");
    assert!(
        cst == 0 && &r[..] == cbytes,
        "{fname} DIVERGENCE input={payload:?}: C={cbytes:?} Rust={:?}",
        &r[..]
    );

    // fl must outlive the datum read below: the fc_unknownout result aliases
    // the resolved FmgrInfo's retained scratch (OutBuf in fn_extra).
    let mut fl = FmgrInfo::unresolved();
    let d = if is_in {
        fc_call(
            vb::fc_unknownin,
            None,
            0,
            Some(m),
            [Datum::from_usize(cs.as_ptr() as usize)],
        )
        .expect("fc_unknownin is infallible")
    } else {
        fc_call(
            vb::fc_unknownout,
            Some(&mut fl),
            0,
            None,
            [Datum::from_usize(cs.as_ptr() as usize)],
        )
        .expect("fc_unknownout is infallible")
    };
    assert!(
        datum_bytes(d, payload.len() + 1) == &r[..],
        "fc_{fname} vs core DIVERGENCE input={payload:?}"
    );
}

fn unknownin_diff(payload: &[u8]) {
    unknown_inout_diff(payload, true);
}

fn unknownout_diff(payload: &[u8]) {
    unknown_inout_diff(payload, false);
}

// ---------------------------------------------------------------------------
// Arm 32: unknownrecv (oid 2416) — like textrecv but the result is the
// cstring itself (validated wire bytes + NUL).
// ---------------------------------------------------------------------------

fn unknownrecv_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_unknownrecv(
            payload.as_ptr(),
            payload.len() as i32,
            out.as_mut_ptr(),
            C_OUT_CAP as i32,
            &mut outlen,
        )
    };
    let cerr = c_errcode();
    let cbytes = &out[..(outlen.max(0) as usize + if cst == 0 { 1 } else { 0 })];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let Some(mut si) = make_si(m, payload) else {
        return;
    };
    let core = varlena::unknownrecv(m, &mut si);
    assert_c_matches("unknownrecv", cst, cerr, &core, |v| &v[..] == cbytes);

    let Some(mut si2) = make_si(m, payload) else {
        return;
    };
    let w = fc_call(
        vb::fc_unknownrecv,
        None,
        0,
        Some(m),
        [Datum::from_usize(core::ptr::from_mut(&mut si2) as usize)],
    );
    assert_fc_matches("fc_unknownrecv", w, &core, |d, v| {
        datum_bytes(d, v.len()) == &v[..]
    });
}

// ---------------------------------------------------------------------------
// Arm 33: unknownsend (oid 2417) — cstring -> bytea wire image.
// ---------------------------------------------------------------------------

fn unknownsend_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return; /* cstring contract */
    }
    let cs = CString::new(payload).unwrap();
    let mut out = vec![0u8; C_OUT_CAP];
    let mut outlen = 0i32;
    let cst = unsafe {
        pg_diff_vltext_unknownsend(cs.as_ptr(), out.as_mut_ptr(), C_OUT_CAP as i32, &mut outlen)
    };
    let cimg = &out[..outlen as usize];

    let cx = mcx::MemoryContext::new("vltext_fuzz");
    let m = cx.mcx();
    let r = varlena::unknownsend(m, payload)
        .expect("unknownsend is infallible on identity encoding");
    assert!(
        cst == 0 && r.as_bytes() == cimg,
        "unknownsend DIVERGENCE input={payload:?}: C={cimg:?} Rust={:?}",
        r.as_bytes()
    );

    let d = fc_call(
        vb::fc_unknownsend,
        None,
        0,
        Some(m),
        [Datum::from_usize(cs.as_ptr() as usize)],
    )
    .expect("fc_unknownsend is infallible on identity encoding");
    assert!(
        read_varlena_data(d) == r.data(),
        "fc_unknownsend vs core DIVERGENCE"
    );
}

// ---------------------------------------------------------------------------
// Arm 34: hashtext (oid 400) — vendored hash_bytes C parity + 42P22 arm.
// ---------------------------------------------------------------------------

fn hashtext_diff(payload: &[u8]) {
    let Some((&flag, text)) = payload.split_first() else {
        return;
    };
    if text.len() > MAX_TEXT {
        return;
    }
    let coll = coll_of(flag);
    let mut cval = 0u32;
    let cst =
        unsafe { pg_diff_vltext_hashtext(text.as_ptr(), text.len() as i32, coll, &mut cval) };
    let cerr = c_errcode();

    let core = varlena::hashtext_bytes(coll, text);
    assert_c_matches("hashtext", cst, cerr, &core, |v| *v == cval);

    let img = text_image(text);
    let w = fc_call(vb::fc_hashtext, None, coll, None, [img_datum(&img)]);
    assert_fc_matches("fc_hashtext", w, &core, |d, v| d.as_u32() == *v);
}

// ---------------------------------------------------------------------------
// Arm 35: hashtextextended (oid 448) — 64-bit seeded variant.
// ---------------------------------------------------------------------------

fn hashtextextended_diff(payload: &[u8]) {
    let Some((&flag, rest)) = payload.split_first() else {
        return;
    };
    let Some(seed_b) = rest.get(..8) else {
        return;
    };
    let seed = u64::from_le_bytes(seed_b.try_into().unwrap());
    let text = &rest[8..];
    if text.len() > MAX_TEXT {
        return;
    }
    let coll = coll_of(flag);
    let mut cval = 0u64;
    let cst = unsafe {
        pg_diff_vltext_hashtextextended(text.as_ptr(), text.len() as i32, coll, seed, &mut cval)
    };
    let cerr = c_errcode();

    // Core: raw-byte extended hash under a valid deterministic collation;
    // the collation gate itself is the wrapper's (hashtext_nondeterministic).
    let img = text_image(text);
    let w = fc_call(
        vb::fc_hashtextextended,
        None,
        coll,
        None,
        [img_datum(&img), Datum::from_u64(seed)],
    );
    let core: PgResult<u64> = w.map(|d| d.as_u64());
    assert_c_matches("hashtextextended", cst, cerr, &core, |v| *v == cval);
    if let Ok(v) = &core {
        // Value plane vs the shipped hashfn core directly (wrapper == core).
        assert!(
            *v == ::hashfn::hash_bytes_extended(text, seed),
            "fc_hashtextextended vs hashfn core DIVERGENCE"
        );
        // Seed-0 low-word identity with hashtext (C 18.3 ground truth).
        if seed == 0 {
            let h32 = varlena::hashtext_bytes(coll, text).expect("valid collation");
            assert!(*v as u32 == h32, "hashtextextended(0) low-word identity broke");
        }
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke: replay seeds through every arm so `cargo test`
// exercises the C link + comparators without cargo-fuzz. NOTE: these link
// against the C oracle, so they run only once the parent uncomments the
// .file("csrc/pg_vltext_io.c") gate in core/build.rs (the #[ignore] texts
// say so; `cargo check` stays green either way).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        vltext_diff(&d);
    }

    /// Text corpus with ASCII, 2/3/4-byte UTF-8, invalid/truncated
    /// sequences, embedded NULs, '%' and backslashes.
    fn text_corpus() -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"hello world".to_vec(),
            b"100%_escape\\d".to_vec(),
            b"a,b,,c,".to_vec(),
            vec![b'x'; 300],
            vec![0xff; 10],       /* invalid UTF-8 */
            vec![0xc3],           /* truncated 2-byte */
            vec![0xe2, 0x82],     /* truncated 3-byte */
            vec![0xf0, 0x9f, 0x90], /* truncated 4-byte */
            b"ab\0cd".to_vec(),   /* embedded NUL is data for text ops */
        ];
        v.push("éé".repeat(10).into_bytes()); /* 2-byte chars */
        v.push("\u{20ac}abc\u{20ac}".repeat(5).into_bytes()); /* 3-byte */
        v.push("\u{1f409}x".repeat(6).into_bytes()); /* 4-byte */
        v
    }

    fn two(flag: Option<u8>, t1: &[u8], t2: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        if let Some(f) = flag {
            p.push(f);
        }
        p.extend_from_slice(&(t1.len() as u16).to_le_bytes());
        p.extend_from_slice(t1);
        p.extend_from_slice(t2);
        p
    }

    fn i32b(tag: u8, v: u16) -> [u8; 3] {
        let vb = v.to_le_bytes();
        [tag, vb[0], vb[1]]
    }

    #[test]
    fn seed_corpus_replays_clean() {
        let _g = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/vltext_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/vltext_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                vltext_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm ok-shape smoke: every selector executes its core + fc plane.
    #[test]
    fn arms_smoke_ok_shapes() {
        let _g = crate::c_oracle_serial();
        for t in text_corpus() {
            for sel in [1u8, 2, 3, 28, 29, 32] {
                drive(sel, &t); /* single-text arms incl. invalid UTF-8 */
            }
            if !t.contains(&0) {
                for sel in [0u8, 30, 31, 33] {
                    drive(sel, &t); /* cstring arms */
                }
            }
            drive(34, &[0x00].iter().chain(t.iter()).copied().collect::<Vec<u8>>());
            let mut ext = vec![0x00, 1, 2, 3, 4, 5, 6, 7, 8];
            ext.extend_from_slice(&t);
            drive(35, &ext);
        }
        // two-text arms: equal / single-byte diff at first, middle, last
        // position, both orders; length ties.
        let pairs: [(&[u8], &[u8]); 8] = [
            (b"same", b"same"),
            (b"abc", b"bbc"),
            (b"abc", b"aBc"),
            (b"abc", b"abd"),
            (b"abd", b"abc"),
            (b"abc", b"abcd"),
            (b"abcd", b"abc"),
            (b"", b"x"),
        ];
        for (a, b) in pairs {
            drive(4, &two(None, a, b));
            for sel in 7..=16u8 {
                drive(sel, &two(Some(0), a, b));
            }
            for sel in 17..=21u8 {
                drive(sel, &two(None, a, b));
            }
            drive(23, &two(Some(0), a, b));
        }
        drive(22, &[0x00]);
        // substr / overlay / split with in-band and extreme params
        for (tag, v) in [(0u8, 2050u16), (0, 0), (0, 2049), (7, 0), (7, 9), (7, 4)] {
            let mut p = i32b(tag, v).to_vec();
            p.extend_from_slice(&i32b(0, 2051));
            p.extend_from_slice("héllo\u{20ac}world".as_bytes());
            drive(5, &p);
            let mut q = i32b(tag, v).to_vec();
            q.extend_from_slice(b"abcdef");
            drive(6, &q);
        }
        let mut ov = i32b(0, 2050).to_vec(); /* sp = 2 */
        ov.extend_from_slice(&i32b(0, 2051)); /* sl = 3 */
        ov.extend_from_slice(&two(None, b"abcdefgh", b"XY"));
        drive(26, &ov);
        let mut ov2 = i32b(0, 2050).to_vec();
        ov2.extend_from_slice(&two(None, "aé\u{20ac}b\u{1f409}c".as_bytes(), b"ZZ"));
        drive(27, &ov2);
        // replace / split_part
        let mut rp = vec![0x00];
        rp.extend_from_slice(&11u16.to_le_bytes());
        rp.extend_from_slice(&2u16.to_le_bytes());
        rp.extend_from_slice(b"abcabcabcab");
        rp.extend_from_slice(b"ab");
        rp.extend_from_slice(b"Z");
        drive(24, &rp);
        let mut sp = vec![0x00];
        sp.extend_from_slice(&i32b(0, 2050)); /* fldnum = 2 */
        sp.extend_from_slice(&two(None, b"a,b,,c,", b","));
        drive(25, &sp);
        let mut spn = vec![0x00];
        spn.extend_from_slice(&i32b(0, 2046)); /* fldnum = -2 */
        spn.extend_from_slice(&two(None, b"a,b,,c,", b","));
        drive(25, &spn);
    }

    /// Per-arm error-shape smoke: every errcode class fires on both sides.
    #[test]
    fn arms_smoke_error_shapes() {
        let _g = crate::c_oracle_serial();
        // 22011: negative substring length
        let mut p = i32b(0, 2049).to_vec(); /* start = 1 */
        p.extend_from_slice(&i32b(0, 2047)); /* length = -1 */
        p.extend_from_slice(b"abc");
        drive(5, &p);
        // 22011 via overlay sp <= 0; 22003 via sp+sl overflow
        let mut ov = i32b(0, 2048).to_vec(); /* sp = 0 */
        ov.extend_from_slice(&i32b(0, 2051));
        ov.extend_from_slice(&two(None, b"abc", b"Z"));
        drive(26, &ov);
        let mut ovf = i32b(7, 9).to_vec(); /* sp = i32::MAX */
        ovf.extend_from_slice(&i32b(7, 9)); /* sl = i32::MAX */
        ovf.extend_from_slice(&two(None, b"abc", b"Z"));
        drive(26, &ovf);
        // 22023: split_part fldnum == 0
        let mut sp = vec![0x00];
        sp.extend_from_slice(&i32b(0, 2048)); /* fldnum = 0 */
        sp.extend_from_slice(&two(None, b"a,b", b","));
        drive(25, &sp);
        // 22021: truncated multibyte through textlen / recv validation
        drive(2, &[0xe2, 0x82]);
        drive(29, &[0xff, 0x41]);
        drive(32, &[0xf0, 0x9f]);
        // 42P22: InvalidOid collation through every flagged arm
        for sel in 7..=16u8 {
            drive(sel, &two(Some(0x0F), b"a", b"b"));
        }
        drive(22, &[0x0F]);
        drive(23, &two(Some(0x0F), b"a", b"b"));
        let mut rp = vec![0x0F];
        rp.extend_from_slice(&3u16.to_le_bytes());
        rp.extend_from_slice(&1u16.to_le_bytes());
        rp.extend_from_slice(b"abcb");
        rp.push(b'Z');
        drive(24, &rp);
        let mut spc = vec![0x0F];
        spc.extend_from_slice(&i32b(0, 2049));
        spc.extend_from_slice(&two(None, b"a,b", b","));
        drive(25, &spc);
        drive(34, &[0x0F, b'x']);
        let mut ext = vec![0x0F, 0, 0, 0, 0, 0, 0, 0, 0];
        ext.push(b'x');
        drive(35, &ext);
    }

    /// Ground-truth magnitude pins (C 18.3): the raw memcmp difference is
    /// SQL-visible through bttextcmp / bttext_pattern_cmp.
    #[test]
    fn cmp_magnitude_pins() {
        let _g = crate::c_oracle_serial();
        setup();
        assert_eq!(varlena::bttextcmp(b"a", b"c", C_COLLATION_OID).unwrap(), -2);
        assert_eq!(varlena::bttext_pattern_cmp(b"a", b"c"), -2);
        assert_eq!(varlena::bttextcmp(b"ab", b"abc", C_COLLATION_OID).unwrap(), -1);
        let mut cval = 0i32;
        let cst = unsafe {
            pg_diff_vltext_cmpop(6, b"a".as_ptr(), 1, b"c".as_ptr(), 1, C_COLLATION_OID, &mut cval)
        };
        assert!(cst == 0 && cval == -2);
        let cst = unsafe {
            pg_diff_vltext_patcmp(4, b"a".as_ptr(), 1, b"c".as_ptr(), 1, &mut cval)
        };
        assert!(cst == 0 && cval == -2);
    }

    /// Fuzz-shaped byte soup through every selector: the whole driver must
    /// be panic-free on arbitrary input (asserts fire only on divergence).
    #[test]
    fn selector_soup() {
        let _g = crate::c_oracle_serial();
        for sel in 0u8..72 {
            for len in [0usize, 1, 2, 3, 7, 9, 16, 65, 130, 300] {
                let payload: Vec<u8> = (0..len)
                    .map(|i| (i as u8).wrapping_mul(41).wrapping_add(sel))
                    .collect();
                drive(sel, &payload);
            }
        }
        vltext_diff(&[]);
    }
}
