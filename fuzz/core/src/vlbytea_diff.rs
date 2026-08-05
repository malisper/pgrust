//! vlbytea_diff: differential fuzz driver — shipped Rust `varlena` bytea
//! family (crates/backend/utils/adt/varlena src/bytea.rs + builtins.rs) vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_vlbytea_io.c).
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//! Any mismatch panics — libFuzzer minimizes that into the divergence
//! reproducer.
//!
//! Environment pinning (documented decisions, not computation mocking):
//!  - bytea_output GUC: both arms run under ONE pinned enum value per exec —
//!    a selector bit picks hex/escape, the C oracle takes it as an argument
//!    into its settable static, the Rust side pins it via
//!    varlena::set_bytea_output (crate-root thread-local the fc_byteaout
//!    wrapper reads through get_bytea_output).
//!  - database encoding pinned UTF8 on both sides (PostgreSQL's default; the
//!    name_diff posture). Reachable in this family only through
//!    hex_decode's invalid-digit errmsg, whose pg_mblen_range call raises
//!    22021 when the offending character's mblen overruns the input end.
//!  - detoast seam: shipped bytea_substring/bytea_overlay slice through the
//!    detoast_attr_slice seam (production boot installs detoast::init_seams;
//!    inputs here are plain inline 4B images, so both sides take the
//!    plain-value slicing arm, vendored verbatim in the oracle). The
//!    `detoast` crate is NOT currently a decoder_fuzz dependency and the
//!    fuzz manifests are parent-owned, so this driver PROBES seam
//!    availability once (seam_core uninstalled call = loud panic) and, when
//!    uninstalled, carves exactly the slice-reaching execs of arms 19..22
//!    (the pre-seam error/empty planes still run). CAMPAIGN REQUIREMENT
//!    (flagged in the lane report): add `detoast = { path = ... }` to
//!    fuzz/core/Cargo.toml and call detoast::init_seams in setup() so the
//!    substr/overlay value planes are live for the coverage run.
//!
//! Input layout: [selector][payload]; selector % 36 picks the arm:
//!    0 byteain            (oid 1244) payload = input text (NUL-free);
//!      hard + soft (SoftErrorContext) + fc hard + fc soft (ErrorSaveNode).
//!    1 byteaout           (oid 31)   [mode][data]: mode&1 hex/escape.
//!    2 bytearecv          (oid 2412) payload = wire bytes (via StringInfo).
//!    3 byteasend          (oid 2413) payload = bytea payload.
//!    4 byteaoctetlen      (oid 720)  payload = bytea payload.
//!    5 byteacat           (oid 2011) [l1:2le][d1][d2].
//!    6..11 byteaeq/ne/lt/le/gt/ge (oids 1948/1953/1949/1950/1951/1952)
//!      [l1:2le][d1][d2].
//!   12 byteacmp           (oid 1954) [l1:2le][d1][d2]; exact raw magnitude.
//!   13 bytea_larger       (oid 6393) [l1:2le][d1][d2]; which-arg identity.
//!   14 bytea_smaller      (oid 6394) same.
//!   15 byteaGetByte       (oid 721)  [m][n:4le][data]; banded n.
//!   16 byteaSetByte       (oid 722)  [m][n:4le][mb][nb:4le][data].
//!   17 byteaGetBit        (oid 723)  [m][n:8le][data]; banded i64 n.
//!   18 byteaSetBit        (oid 724)  [m][n:8le][mb][data]; banded newBit.
//!   19 bytea_substr       (oid 2012) [m1][s:4le][m2][l:4le][data].
//!   20 bytea_substr_no_len(oid 2013) [m1][s:4le][data].
//!   21 byteaoverlay       (oid 749)  [m1][sp:4le][m2][sl:4le][l1:2le][d1][d2].
//!   22 byteaoverlay_no_len(oid 752)  [m1][sp:4le][l1:2le][d1][d2].
//!   23 byteapos           (oid 2014) [l1:2le][d1][d2].
//!   24 bytea_bit_count    (oid 6163) payload = bytea payload.
//!   25 bytea_int2         (oid 6370) payload = bytea payload (len 0..9 in
//!      seeds; C raises 22003 above the width).
//!   26 bytea_int4         (oid 6371) same.
//!   27 bytea_int8         (oid 6372) same.
//!   28 int2_bytea         (oid 6367) [v:2le]; total.
//!   29 int4_bytea         (oid 6368) [v:4le]; total.
//!   30 int8_bytea         (oid 6369) [v:8le]; total.
//!   31 bytea_reverse      (oid 6382) payload = bytea payload.
//!   32 hashvarlena        (oid 456)  payload = key bytes; exact u32.
//!   33 hashvarlenaextended(oid 772)  [seed:8le][key]; exact u64 + the
//!      seed-0 low-word identity vs hashvarlena.
//!   34 hashbytea          (oid 6413) alias arm (C aliases hashvarlena;
//!      the catalog row maps to fc_hashvarlena).
//!   35 hashbyteaextended  (oid 6414) alias arm.
//!
//! Banded integer decode (Get/Set/substr/overlay scalar params): a mode
//! byte mixes raw extreme injections (INT_MIN/-1/0/INT_MAX and the i64
//! equivalents), raw 4/8-byte values, and in/near-range folds so both the
//! error ereports and the value paths stay hot.
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). C-parity keeps being carried
//! by the core comparison; the plane makes the wrapper lines execute every
//! iteration with an in-harness oracle.
//!
//! SKIPPED (excluded rows, with reasons):
//!  - bytea_sortsupport (oid 3331 family): SortSupport plumbing
//!    (varstr_sortsupport), no pure entry point in the shipped crate's bytea
//!    surface and no bytea fc_* wrapper row; the comparator it installs is
//!    byteacmp's, pinned by arm 12.
//!  - bytea_string_agg_transfn/finalfn (oids 3543/3544): excluded(state) —
//!    aggregate transition state over agg_context, not reachable purely;
//!    they belong to the string_agg lane.
//!  - Toasted/compressed/external argument forms of byteaeq/ne/octetlen/
//!    substr (toast_raw_datum_size fast paths): the fuzz frame constructs
//!    plain inline images only; toast form dispatch is owned by the detoast
//!    lane. The value semantics compared here are the post-detoast ones.

use std::ffi::{c_char, CStr, CString};
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, SoftErrorContext, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SUBSTRING_ERROR,
};
use types_fmgr::{ErrorSaveNode, FmgrInfo, LocalFcinfo, PGFunction, PackedVarlena};
use varlena::builtins as vb;
use varlena::bytea as rb;

extern "C" {
    fn pg_diff_byteain(input: *const c_char, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_byteaout(
        data: *const u8,
        len: i32,
        mode: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_bytearecv(payload: *const u8, nbytes: i32, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_byteasend(data: *const u8, len: i32, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_byteaoctetlen(data: *const u8, len: i32, out: *mut i32) -> i32;
    fn pg_diff_byteacat(
        d1: *const u8,
        l1: i32,
        d2: *const u8,
        l2: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_byteaeq(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_byteane(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_bytealt(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_byteale(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_byteagt(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_byteage(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_byteacmp(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_bytea_larger(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_bytea_smaller(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32)
        -> i32;
    fn pg_diff_byteaGetByte(data: *const u8, len: i32, n: i32, out: *mut i32) -> i32;
    fn pg_diff_byteaSetByte(
        data: *const u8,
        len: i32,
        n: i32,
        new_byte: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_byteaGetBit(data: *const u8, len: i32, n: i64, out: *mut i32) -> i32;
    fn pg_diff_byteaSetBit(
        data: *const u8,
        len: i32,
        n: i64,
        new_bit: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_bytea_substr(
        data: *const u8,
        len: i32,
        s: i32,
        l: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_bytea_substr_no_len(
        data: *const u8,
        len: i32,
        s: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_byteaoverlay(
        d1: *const u8,
        l1: i32,
        d2: *const u8,
        l2: i32,
        sp: i32,
        sl: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_byteaoverlay_no_len(
        d1: *const u8,
        l1: i32,
        d2: *const u8,
        l2: i32,
        sp: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_byteapos(d1: *const u8, l1: i32, d2: *const u8, l2: i32, out: *mut i32) -> i32;
    fn pg_diff_bytea_bit_count(data: *const u8, len: i32, out: *mut i64) -> i32;
    fn pg_diff_bytea_int2(data: *const u8, len: i32, out: *mut i16) -> i32;
    fn pg_diff_bytea_int4(data: *const u8, len: i32, out: *mut i32) -> i32;
    fn pg_diff_bytea_int8(data: *const u8, len: i32, out: *mut i64) -> i32;
    fn pg_diff_int2_bytea(v: i16, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_int4_bytea(v: i32, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_int8_bytea(v: i64, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_bytea_reverse(data: *const u8, len: i32, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_hashvarlena(data: *const u8, len: i32) -> u32;
    fn pg_diff_hashvarlenaextended(data: *const u8, len: i32, seed: u64) -> u64;
    fn pg_diff_hashbytea(data: *const u8, len: i32) -> u32;
    fn pg_diff_hashbyteaextended(data: *const u8, len: i32, seed: u64) -> u64;
    fn pg_diff_vlbytea_hex_encode(
        data: *const u8,
        len: i32,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
}

/// Oracle errcode classes (csrc/pg_vlbytea_io.c shim 4).
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_INVALID_PARAM: i32 = 2; /* 22023 */
const C_ERR_SUBSTRING: i32 = 3; /* 22011 */
const C_ERR_NUM_OOR: i32 = 4; /* 22003 */
const C_ERR_ARRAY_SUBSCRIPT: i32 = 5; /* 2202E */
const C_ERR_PROGRAM_LIMIT: i32 = 6; /* 54000 */
const C_ERR_NOT_IN_REPERTOIRE: i32 = 7; /* 22021 */

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        C_ERR_INVALID_PARAM
    } else if e.sqlstate == ERRCODE_SUBSTRING_ERROR {
        C_ERR_SUBSTRING
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_NUM_OOR
    } else if e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR {
        C_ERR_ARRAY_SUBSCRIPT
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        C_ERR_PROGRAM_LIMIT
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_NOT_IN_REPERTOIRE
    } else {
        98
    }
}

/// Cap bytea payloads (fleet throughput; size-driving args fold so results
/// stay <= ~4 KiB).
const MAX_BYTEA: usize = 2048;
/// C out-buffer capacity: escape byteaout worst case 4*len + "\x"/NUL slack,
/// and every other result is <= l1 + l2 <= 2*MAX_BYTEA.
const C_OUT: usize = 4 * MAX_BYTEA + 16;

/// C bytea_output enum values (vartypes.h / guc_tables::consts — the crate
/// is not a decoder_fuzz dependency, values pinned by guc_tables tests).
const BYTEA_OUTPUT_ESCAPE: i32 = 0;
const BYTEA_OUTPUT_HEX: i32 = 1;

/// Production-boot seams both sides depend on: real mbutils (pg_mblen_range
/// behind the invalid-hex-digit errmsg) and the UTF8 database-encoding pin
/// (see module doc). The detoast seam is probed, not installed (module doc).
fn setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(mbutils::init_seams);
        // Campaign wiring (lane p1-lanes): install the production detoast
        // seams so the substr/overlay slice value planes are live (the
        // driver's detoast_slice_installed probe sees them). Idempotence
        // guard mirrors the mbutils call above.
        let _ = std::panic::catch_unwind(detoast::init_seams);
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
// wrapper-level pattern run without kani; name_diff conventions).
// ---------------------------------------------------------------------------

/// One native fmgr call: N arg Datums, optional resolved FmgrInfo (cstring
/// scratch), optional armed result mcx. Wrappers under test never return SQL
/// NULL on the Ok path (fc_byteain's soft-NULL is driven separately).
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    mcx: Option<mcx::Mcx<'_>>,
    args: [Datum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    if let Some(m) = mcx {
        // SAFETY: the arming context outlives this single call (caller scope).
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

/// bytea/varlena arg construction: inline 4B-uncompressed header + body
/// (the shipped set_varsize_4b_word encoding; bodies are capped by MAX_BYTEA
/// so the length always fits).
fn bytea_image(body: &[u8]) -> Vec<u8> {
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

fn image_datum(img: &[u8]) -> Datum {
    Datum::from_usize(img.as_ptr() as usize)
}

/// Varlena result readback (bytea payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// fc plane for an infallible-on-this-input varlena-result wrapper.
fn fc_expect_bytes<const N: usize>(fname: &str, f: PGFunction, args: [Datum; N], want: &[u8]) {
    let cx = mcx::MemoryContext::new("vlbytea_fc");
    let d = fc_call(f, None, Some(cx.mcx()), args)
        .unwrap_or_else(|e| panic!("{fname} wrapper errored: {}", e.message));
    assert!(
        read_varlena_data(d) == want,
        "{fname} fc-wrapper DIVERGENCE: wrapper={:?} core={want:?}",
        read_varlena_data(d)
    );
}

/// fc plane for a fallible varlena-result wrapper vs the C-checked outcome.
fn fc_expect_bytes_or_err<const N: usize>(
    fname: &str,
    f: PGFunction,
    args: [Datum; N],
    cst: i32,
    cerr: i32,
    want: &[u8],
) {
    let cx = mcx::MemoryContext::new("vlbytea_fc");
    match fc_call(f, None, Some(cx.mcx()), args) {
        Ok(d) => assert!(
            cst == 0 && read_varlena_data(d) == want,
            "{fname} fc-wrapper DIVERGENCE: wrapper=Ok({:?}) C=(st {cst})",
            read_varlena_data(d)
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "{fname} fc-wrapper error DIVERGENCE: wrapper=Err({:?}) C=(st {cst}, err {cerr})",
            e.sqlstate
        ),
    }
}

/// Banded i32 decode: extremes / raw / length-relative folds (module doc).
fn band_i32(m: u8, raw: i32, len: i32) -> i32 {
    match m % 8 {
        0 => i32::MIN,
        1 => -1,
        2 => 0,
        3 => i32::MAX,
        4 => raw,
        5 => len,
        6 => {
            if len > 0 {
                raw.rem_euclid(len)
            } else {
                0
            }
        }
        _ => raw.rem_euclid(2 * len.max(1) + 3) - 1,
    }
}

/// Banded i64 decode (bit indexes range over len*8).
fn band_i64(m: u8, raw: i64, len: i64) -> i64 {
    match m % 8 {
        0 => i64::MIN,
        1 => -1,
        2 => 0,
        3 => i64::MAX,
        4 => raw,
        5 => len * 8,
        6 => {
            if len > 0 {
                raw.rem_euclid(len * 8)
            } else {
                0
            }
        }
        _ => raw.rem_euclid(2 * (len * 8).max(1) + 3) - 1,
    }
}

/// Once-per-process probe: is the detoast_attr_slice seam installed?
/// (Uninstalled seam call = loud panic by seam_core contract.)
fn detoast_slice_installed() -> bool {
    use std::sync::OnceLock;
    static PROBE: OnceLock<bool> = OnceLock::new();
    *PROBE.get_or_init(|| {
        let r = std::panic::catch_unwind(|| {
            let cx = mcx::MemoryContext::new("vlbytea_probe");
            let img = bytea_image(b"x");
            let ok = rb::bytea_substring(cx.mcx(), &img, 1, 1, false).is_ok();
            ok
        });
        matches!(r, Ok(true))
    })
}

/// Would bytea_substring(s, l) reach the DatumGetByteaPSlice fetch (vs the
/// pre-seam 22011 error / E<1 empty paths)?
fn substr_uses_slice(s: i32, l: Option<i32>) -> bool {
    match l {
        None => true,
        Some(l) if l < 0 => false,
        Some(l) => match s.checked_add(l) {
            None => true,
            Some(e) => e >= 1,
        },
    }
}

fn take_i32(p: &[u8]) -> Option<(i32, &[u8])> {
    let (h, rest) = (p.get(..4)?, &p[4..]);
    Some((i32::from_le_bytes(h.try_into().unwrap()), rest))
}

fn take_i64(p: &[u8]) -> Option<(i64, &[u8])> {
    let (h, rest) = (p.get(..8)?, &p[8..]);
    Some((i64::from_le_bytes(h.try_into().unwrap()), rest))
}

/// Two payloads out of one: [l1:2le][d1][d2] with l1 folded into range.
fn split2(p: &[u8]) -> Option<(&[u8], &[u8])> {
    let (h, rest) = (p.get(..2)?, &p[2..]);
    if rest.len() > 2 * MAX_BYTEA {
        return None;
    }
    let l1 = (u16::from_le_bytes(h.try_into().unwrap()) as usize) % (rest.len() + 1);
    Some((&rest[..l1], &rest[l1..]))
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn vlbytea_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics) —
    // the fuzz TARGET's own frame stack needs the lock, same driver-entry
    // idiom as every other pub *_diff (task #144 addendum, trgm precedent).
    let _oracle = crate::oracle_serial();

    setup();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 37 {
        36 => hex_encode_diff(payload),
        0 => byteain_diff(payload),
        1 => byteaout_diff(payload),
        2 => bytearecv_diff(payload),
        3 => byteasend_diff(payload),
        4 => byteaoctetlen_diff(payload),
        5 => byteacat_diff(payload),
        6 => cmp_diff(payload, CmpOp::Eq),
        7 => cmp_diff(payload, CmpOp::Ne),
        8 => cmp_diff(payload, CmpOp::Lt),
        9 => cmp_diff(payload, CmpOp::Le),
        10 => cmp_diff(payload, CmpOp::Gt),
        11 => cmp_diff(payload, CmpOp::Ge),
        12 => byteacmp_diff(payload),
        13 => minmax_diff(payload, true),
        14 => minmax_diff(payload, false),
        15 => get_byte_diff(payload),
        16 => set_byte_diff(payload),
        17 => get_bit_diff(payload),
        18 => set_bit_diff(payload),
        19 => substr_diff(payload),
        20 => substr_no_len_diff(payload),
        21 => overlay_diff(payload),
        22 => overlay_no_len_diff(payload),
        23 => byteapos_diff(payload),
        24 => bit_count_diff(payload),
        25 => bytea_int_diff(payload, 2),
        26 => bytea_int_diff(payload, 4),
        27 => bytea_int_diff(payload, 8),
        28 => int_bytea_diff(payload, 2),
        29 => int_bytea_diff(payload, 4),
        30 => int_bytea_diff(payload, 8),
        31 => bytea_reverse_diff(payload),
        32 => hash_diff(payload, false),
        33 => hash_ext_diff(payload, false),
        34 => hash_diff(payload, true),
        _ => hash_ext_diff(payload, true),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: byteain (oid 1244) — hex + escaped forms, hard/soft/fc/fc-soft.
// ---------------------------------------------------------------------------

fn byteain_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA || payload.contains(&0) {
        return;
    }
    let cs = CString::new(payload).unwrap();
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe { pg_diff_byteain(cs.as_ptr(), cout.as_mut_ptr(), &mut cn) };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let m = cx.mcx();

    // Hard-error shape (escontext = None).
    match rb::byteain(m, payload, None) {
        Ok(Some(v)) => assert!(
            cst == 0 && v.data() == cval,
            "byteain DIVERGENCE input={payload:?}: C=(st {cst}, {cval:?}) Rust=Ok({:?})",
            v.data()
        ),
        Ok(None) => panic!("byteain returned soft-None without an escontext, input={payload:?}"),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "byteain error DIVERGENCE input={payload:?}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            e.sqlstate
        ),
    }

    // Soft-error shape. The invalid-hex-digit errmsg's pg_mblen_range call
    // can itself raise 22021 as a HARD error on both sides (C evaluates the
    // errmsg when details are wanted; Rust's `invalid_hex_digit(..)?`).
    let mut sc = SoftErrorContext::new(true);
    match rb::byteain(m, payload, Some(&mut sc)) {
        Ok(Some(v)) => assert!(
            cst == 0 && v.data() == cval,
            "byteain soft-path value DIVERGENCE input={payload:?}"
        ),
        Ok(None) => {
            let saved = sc.error().expect("details_wanted context must save the error");
            assert!(
                cst != 0 && rust_err_class(saved) == cerr,
                "byteain soft verdict DIVERGENCE input={payload:?}: C=(st {cst}, err {cerr}) saved={:?}",
                saved.sqlstate
            );
        }
        Err(e) => assert!(
            cst != 0 && cerr == C_ERR_NOT_IN_REPERTOIRE && rust_err_class(&e) == cerr,
            "byteain soft-path hard error DIVERGENCE input={payload:?}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            e.sqlstate
        ),
    }

    // fc plane, hard shape.
    let din = Datum::from_usize(cs.as_ptr() as usize);
    fc_expect_bytes_or_err("fc_byteain", vb::fc_byteain, [din], cst, cerr, cval);

    // fc plane, soft shape (real ErrorSaveNode on the frame; the wrapper
    // returns Datum::null() after the context saved the error).
    let cx2 = mcx::MemoryContext::new("vlbytea_fc");
    let mut node = ErrorSaveNode::new(true);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: `cx2` outlives this single call.
    unsafe { fcinfo.set_result_mcx(cx2.mcx()) };
    fcinfo.context = node.fm_node_ptr();
    fcinfo.args[0] = NullableDatum::value(din);
    match vb::fc_byteain(None, &mut fcinfo) {
        Ok(d) => {
            assert_eq!(
                node.ctx.error_occurred(),
                cst != 0,
                "fc_byteain soft verdict DIVERGENCE input={payload:?}"
            );
            if cst == 0 {
                assert!(
                    read_varlena_data(d) == cval,
                    "fc_byteain soft value DIVERGENCE input={payload:?}"
                );
            } else {
                let saved = node.ctx.error().expect("fc soft context must save the error");
                assert_eq!(rust_err_class(saved), cerr, "fc_byteain soft class DIVERGENCE");
            }
        }
        Err(e) => assert!(
            cst != 0 && cerr == C_ERR_NOT_IN_REPERTOIRE && rust_err_class(&e) == cerr,
            "fc_byteain soft-path hard error DIVERGENCE input={payload:?}: Err({:?})",
            e.sqlstate
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 1: byteaout (oid 31) — hex and escape modes under one pinned GUC.
// ---------------------------------------------------------------------------

fn byteaout_diff(payload: &[u8]) {
    let Some((&mb, data)) = payload.split_first() else {
        return;
    };
    if data.len() > MAX_BYTEA {
        return;
    }
    let mode = if mb & 1 != 0 {
        BYTEA_OUTPUT_HEX
    } else {
        BYTEA_OUTPUT_ESCAPE
    };

    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst =
        unsafe { pg_diff_byteaout(data.as_ptr(), data.len() as i32, mode, cout.as_mut_ptr(), &mut cn) };
    assert!(cst == 0, "byteaout C oracle errored (st {cst}) at len {}", data.len());
    let cval = &cout[..cn as usize + 1]; // include the NUL

    let mut rbuf = Vec::new();
    rb::byteaout_into(data, mode, &mut rbuf).expect("byteaout is infallible at this size");
    assert!(
        rbuf == cval,
        "byteaout DIVERGENCE mode={mode} input={data:?}: C={cval:?} Rust={rbuf:?}"
    );

    // fc plane: pin the crate GUC (environment pinning, module doc) and read
    // the wrapper's retained-scratch cstring back.
    varlena::set_bytea_output(mode);
    let img = bytea_image(data);
    let mut fl = FmgrInfo::unresolved();
    let d = fc_call(vb::fc_byteaout, Some(&mut fl), None, [image_datum(&img)])
        .expect("fc_byteaout is infallible at this size");
    // SAFETY: fc_byteaout returns the live NUL-terminated fn_extra scratch.
    let out = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
    assert!(
        out.to_bytes_with_nul() == &rbuf[..],
        "fc_byteaout vs core DIVERGENCE mode={mode} input={data:?}"
    );
}

// ---------------------------------------------------------------------------
// Arm 2: bytearecv (oid 2412) — wire payload through a real StringInfo.
// ---------------------------------------------------------------------------

fn bytearecv_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_bytearecv(payload.as_ptr(), payload.len() as i32, cout.as_mut_ptr(), &mut cn)
    };
    assert!(cst == 0, "bytearecv C oracle errored (st {cst})");
    let cval = &cout[..cn as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let m = cx.mcx();
    let Ok(mut si) = stringinfo::StringInfo::new_in(m) else { return };
    if si.append_bytes(payload).is_err() {
        return;
    }
    let v = rb::bytearecv(m, &mut si).expect("bytearecv is infallible on a full buffer");
    assert!(
        v.data() == cval,
        "bytearecv DIVERGENCE payload={payload:?}: C={cval:?} Rust={:?}",
        v.data()
    );

    // fc plane over its own StringInfo image of the same wire payload.
    let Ok(mut si2) = stringinfo::StringInfo::new_in(m) else { return };
    if si2.append_bytes(payload).is_err() {
        return;
    }
    let dsi = Datum::from_usize(core::ptr::from_mut(&mut si2) as usize);
    fc_expect_bytes("fc_bytearecv", vb::fc_bytearecv, [dsi], cval);
}

// ---------------------------------------------------------------------------
// Arm 3: byteasend (oid 2413) — identity copy.
// ---------------------------------------------------------------------------

fn byteasend_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_byteasend(payload.as_ptr(), payload.len() as i32, cout.as_mut_ptr(), &mut cn)
    };
    assert!(cst == 0, "byteasend C oracle errored (st {cst})");
    let cval = &cout[..cn as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let v = rb::byteasend(cx.mcx(), payload).expect("byteasend is infallible at this size");
    assert!(
        v.data() == cval,
        "byteasend DIVERGENCE payload={payload:?}: C={cval:?} Rust={:?}",
        v.data()
    );

    let img = bytea_image(payload);
    fc_expect_bytes("fc_byteasend", vb::fc_byteasend, [image_datum(&img)], cval);
}

// ---------------------------------------------------------------------------
// Arm 4: byteaoctetlen (oid 720).
// ---------------------------------------------------------------------------

fn byteaoctetlen_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cv: i32 = 0;
    let cst = unsafe { pg_diff_byteaoctetlen(payload.as_ptr(), payload.len() as i32, &mut cv) };
    assert!(cst == 0);
    let rv = rb::byteaoctetlen(payload);
    assert!(
        rv == cv,
        "byteaoctetlen DIVERGENCE len={}: C={cv} Rust={rv}",
        payload.len()
    );

    let img = bytea_image(payload);
    let d = fc_call(vb::fc_byteaoctetlen, None, None, [image_datum(&img)])
        .expect("fc_byteaoctetlen is infallible");
    assert!(
        d.as_i32() == rv,
        "fc_byteaoctetlen fc-wrapper DIVERGENCE: wrapper={} core={rv}",
        d.as_i32()
    );
}

// ---------------------------------------------------------------------------
// Arm 5: byteacat (oid 2011).
// ---------------------------------------------------------------------------

fn byteacat_diff(payload: &[u8]) {
    let Some((d1, d2)) = split2(payload) else { return };
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_byteacat(
            d1.as_ptr(),
            d1.len() as i32,
            d2.as_ptr(),
            d2.len() as i32,
            cout.as_mut_ptr(),
            &mut cn,
        )
    };
    assert!(cst == 0, "byteacat C oracle errored (st {cst})");
    let cval = &cout[..cn as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let v = rb::bytea_catenate(cx.mcx(), d1, d2).expect("byteacat is infallible at this size");
    assert!(
        v.data() == cval,
        "byteacat DIVERGENCE d1={d1:?} d2={d2:?}: C={cval:?} Rust={:?}",
        v.data()
    );

    let (i1, i2) = (bytea_image(d1), bytea_image(d2));
    fc_expect_bytes(
        "fc_byteacat",
        vb::fc_byteacat,
        [image_datum(&i1), image_datum(&i2)],
        cval,
    );
}

// ---------------------------------------------------------------------------
// Arms 6..11: byteaeq/ne/lt/le/gt/ge (oids 1948/1953/1949/1950/1951/1952).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn cmp_diff(payload: &[u8], op: CmpOp) {
    let Some((d1, d2)) = split2(payload) else { return };
    let (p1, l1, p2, l2) = (d1.as_ptr(), d1.len() as i32, d2.as_ptr(), d2.len() as i32);
    let mut cv: i32 = 0;
    let (name, fc, rres, cst) = match op {
        CmpOp::Eq => ("byteaeq", vb::fc_byteaeq as PGFunction, rb::byteaeq(d1, d2), unsafe {
            pg_diff_byteaeq(p1, l1, p2, l2, &mut cv)
        }),
        CmpOp::Ne => ("byteane", vb::fc_byteane as PGFunction, rb::byteane(d1, d2), unsafe {
            pg_diff_byteane(p1, l1, p2, l2, &mut cv)
        }),
        CmpOp::Lt => ("bytealt", vb::fc_bytealt as PGFunction, rb::bytealt(d1, d2), unsafe {
            pg_diff_bytealt(p1, l1, p2, l2, &mut cv)
        }),
        CmpOp::Le => ("byteale", vb::fc_byteale as PGFunction, rb::byteale(d1, d2), unsafe {
            pg_diff_byteale(p1, l1, p2, l2, &mut cv)
        }),
        CmpOp::Gt => ("byteagt", vb::fc_byteagt as PGFunction, rb::byteagt(d1, d2), unsafe {
            pg_diff_byteagt(p1, l1, p2, l2, &mut cv)
        }),
        CmpOp::Ge => ("byteage", vb::fc_byteage as PGFunction, rb::byteage(d1, d2), unsafe {
            pg_diff_byteage(p1, l1, p2, l2, &mut cv)
        }),
    };
    assert!(cst == 0);
    assert!(
        rres == (cv != 0),
        "{name} DIVERGENCE d1={d1:?} d2={d2:?}: C={cv} Rust={rres}"
    );

    let (i1, i2) = (bytea_image(d1), bytea_image(d2));
    let d = fc_call(fc, None, None, [image_datum(&i1), image_datum(&i2)])
        .unwrap_or_else(|e| panic!("fc_{name} wrapper errored: {}", e.message));
    assert!(
        d.as_bool() == rres,
        "fc_{name} fc-wrapper DIVERGENCE: wrapper={} core={rres}",
        d.as_bool()
    );
}

// ---------------------------------------------------------------------------
// Arm 12: byteacmp (oid 1954) — exact raw memcmp magnitude (SQL-visible).
// ---------------------------------------------------------------------------

fn byteacmp_diff(payload: &[u8]) {
    let Some((d1, d2)) = split2(payload) else { return };
    let mut cv: i32 = 0;
    let cst = unsafe {
        pg_diff_byteacmp(d1.as_ptr(), d1.len() as i32, d2.as_ptr(), d2.len() as i32, &mut cv)
    };
    assert!(cst == 0);
    let rv = rb::byteacmp(d1, d2);
    assert!(
        rv == cv,
        "byteacmp DIVERGENCE d1={d1:?} d2={d2:?}: C={cv} Rust={rv}"
    );

    let (i1, i2) = (bytea_image(d1), bytea_image(d2));
    let d = fc_call(vb::fc_byteacmp, None, None, [image_datum(&i1), image_datum(&i2)])
        .expect("fc_byteacmp is infallible");
    assert!(
        d.as_i32() == rv,
        "fc_byteacmp fc-wrapper DIVERGENCE: wrapper={} core={rv}",
        d.as_i32()
    );
}

// ---------------------------------------------------------------------------
// Arms 13/14: bytea_larger / bytea_smaller (oids 6393/6394) — C returns one
// of its ARGUMENTS; the differential pins which one (0/1), and the fc plane
// pins the wrapper's pointer identity to the same arg image.
// ---------------------------------------------------------------------------

fn minmax_diff(payload: &[u8], larger: bool) {
    let Some((d1, d2)) = split2(payload) else { return };
    let (p1, l1, p2, l2) = (d1.as_ptr(), d1.len() as i32, d2.as_ptr(), d2.len() as i32);
    let mut cwhich: i32 = 0;
    let (name, cst) = if larger {
        ("bytea_larger", unsafe { pg_diff_bytea_larger(p1, l1, p2, l2, &mut cwhich) })
    } else {
        ("bytea_smaller", unsafe { pg_diff_bytea_smaller(p1, l1, p2, l2, &mut cwhich) })
    };
    assert!(cst == 0);
    let rv = if larger {
        rb::bytea_larger(d1, d2)
    } else {
        rb::bytea_smaller(d1, d2)
    };
    // Tie-biased which-detection: C returns arg2 on every tie and so does the
    // shipped core, so test identity against d2 first (when l1 == 0 the two
    // slices can alias byte-for-byte and a d1-first test would misreport).
    let rwhich = if std::ptr::eq(rv, d2) { 1 } else { 0 };
    assert!(
        rwhich == cwhich,
        "{name} DIVERGENCE d1={d1:?} d2={d2:?}: C picked arg{cwhich} Rust picked arg{rwhich}"
    );

    let (i1, i2) = (bytea_image(d1), bytea_image(d2));
    let f: PGFunction = if larger { vb::fc_bytea_larger } else { vb::fc_bytea_smaller };
    let d = fc_call(f, None, None, [image_datum(&i1), image_datum(&i2)])
        .unwrap_or_else(|e| panic!("fc_{name} wrapper errored: {}", e.message));
    let want = if cwhich == 0 { i1.as_ptr() } else { i2.as_ptr() };
    assert!(
        d.as_usize() == want as usize,
        "fc_{name} fc-wrapper DIVERGENCE: wrapper returned the other arg"
    );
}

// ---------------------------------------------------------------------------
// Arm 15: byteaGetByte (oid 721) — banded index incl. raw extremes.
// ---------------------------------------------------------------------------

fn get_byte_diff(payload: &[u8]) {
    let Some((&mb, rest)) = payload.split_first() else { return };
    let Some((raw, data)) = take_i32(rest) else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let n = band_i32(mb, raw, data.len() as i32);

    let mut cv: i32 = 0;
    let cst = unsafe { pg_diff_byteaGetByte(data.as_ptr(), data.len() as i32, n, &mut cv) };
    let cerr = c_errcode();
    match rb::bytea_get_byte(data, n) {
        Ok(rv) => assert!(
            cst == 0 && rv == cv,
            "byteaGetByte DIVERGENCE n={n} len={}: C=(st {cst}, {cv}) Rust={rv}",
            data.len()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "byteaGetByte error DIVERGENCE n={n} len={}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            data.len(),
            e.sqlstate
        ),
    }

    let img = bytea_image(data);
    match fc_call(vb::fc_bytea_get_byte, None, None, [image_datum(&img), Datum::from_i32(n)]) {
        Ok(d) => assert!(cst == 0 && d.as_i32() == cv, "fc_bytea_get_byte DIVERGENCE n={n}"),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "fc_bytea_get_byte error DIVERGENCE n={n}: Err({:?})",
            e.sqlstate
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 16: byteaSetByte (oid 722).
// ---------------------------------------------------------------------------

fn set_byte_diff(payload: &[u8]) {
    let Some((&mb, rest)) = payload.split_first() else { return };
    let Some((nraw, rest)) = take_i32(rest) else { return };
    let Some((&nbm, rest)) = rest.split_first() else { return };
    let Some((nbraw, data)) = take_i32(rest) else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let n = band_i32(mb, nraw, data.len() as i32);
    let new_byte = band_i32(nbm, nbraw, 256);

    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_byteaSetByte(data.as_ptr(), data.len() as i32, n, new_byte, cout.as_mut_ptr(), &mut cn)
    };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    match rb::bytea_set_byte(cx.mcx(), data, n, new_byte) {
        Ok(v) => assert!(
            cst == 0 && v.data() == cval,
            "byteaSetByte DIVERGENCE n={n} nb={new_byte} len={}: C=(st {cst}) Rust=Ok",
            data.len()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "byteaSetByte error DIVERGENCE n={n} len={}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            data.len(),
            e.sqlstate
        ),
    }

    let img = bytea_image(data);
    fc_expect_bytes_or_err(
        "fc_bytea_set_byte",
        vb::fc_bytea_set_byte,
        [image_datum(&img), Datum::from_i32(n), Datum::from_i32(new_byte)],
        cst,
        cerr,
        cval,
    );
}

// ---------------------------------------------------------------------------
// Arm 17: byteaGetBit (oid 723) — int64 bit index.
// ---------------------------------------------------------------------------

fn get_bit_diff(payload: &[u8]) {
    let Some((&mb, rest)) = payload.split_first() else { return };
    let Some((raw, data)) = take_i64(rest) else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let n = band_i64(mb, raw, data.len() as i64);

    let mut cv: i32 = 0;
    let cst = unsafe { pg_diff_byteaGetBit(data.as_ptr(), data.len() as i32, n, &mut cv) };
    let cerr = c_errcode();
    match rb::bytea_get_bit(data, n) {
        Ok(rv) => assert!(
            cst == 0 && rv == cv,
            "byteaGetBit DIVERGENCE n={n} len={}: C=(st {cst}, {cv}) Rust={rv}",
            data.len()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "byteaGetBit error DIVERGENCE n={n} len={}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            data.len(),
            e.sqlstate
        ),
    }

    let img = bytea_image(data);
    match fc_call(vb::fc_bytea_get_bit, None, None, [image_datum(&img), Datum::from_i64(n)]) {
        Ok(d) => assert!(cst == 0 && d.as_i32() == cv, "fc_bytea_get_bit DIVERGENCE n={n}"),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "fc_bytea_get_bit error DIVERGENCE n={n}: Err({:?})",
            e.sqlstate
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 18: byteaSetBit (oid 724) — banded newBit hits the 22023 arm too.
// ---------------------------------------------------------------------------

fn set_bit_diff(payload: &[u8]) {
    let Some((&mb, rest)) = payload.split_first() else { return };
    let Some((nraw, rest)) = take_i64(rest) else { return };
    let Some((&bm, data)) = rest.split_first() else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let n = band_i64(mb, nraw, data.len() as i64);
    let new_bit: i32 = match bm % 6 {
        0 | 1 => (bm % 2) as i32,
        2 => -1,
        3 => 2,
        4 => i32::MAX,
        _ => (bm as i32) - 128,
    };

    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_byteaSetBit(data.as_ptr(), data.len() as i32, n, new_bit, cout.as_mut_ptr(), &mut cn)
    };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    match rb::bytea_set_bit(cx.mcx(), data, n, new_bit) {
        Ok(v) => assert!(
            cst == 0 && v.data() == cval,
            "byteaSetBit DIVERGENCE n={n} bit={new_bit} len={}",
            data.len()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "byteaSetBit error DIVERGENCE n={n} bit={new_bit} len={}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            data.len(),
            e.sqlstate
        ),
    }

    let img = bytea_image(data);
    fc_expect_bytes_or_err(
        "fc_bytea_set_bit",
        vb::fc_bytea_set_bit,
        [image_datum(&img), Datum::from_i64(n), Datum::from_i32(new_bit)],
        cst,
        cerr,
        cval,
    );
}

// ---------------------------------------------------------------------------
// Arms 19/20: bytea_substr / bytea_substr_no_len (oids 2012/2013).
// ---------------------------------------------------------------------------

fn substr_diff(payload: &[u8]) {
    let Some((&m1, rest)) = payload.split_first() else { return };
    let Some((sraw, rest)) = take_i32(rest) else { return };
    let Some((&m2, rest)) = rest.split_first() else { return };
    let Some((lraw, data)) = take_i32(rest) else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let s = band_i32(m1, sraw, data.len() as i32);
    let l = band_i32(m2, lraw, data.len() as i32);
    if substr_uses_slice(s, Some(l)) && !detoast_slice_installed() {
        return; // carved: detoast seam uninstalled (module doc)
    }

    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_bytea_substr(data.as_ptr(), data.len() as i32, s, l, cout.as_mut_ptr(), &mut cn)
    };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    let img = bytea_image(data);
    let cx = mcx::MemoryContext::new("vlbytea");
    match rb::bytea_substring(cx.mcx(), &img, s, l, false) {
        Ok(v) => assert!(
            cst == 0 && v.data() == cval,
            "bytea_substr DIVERGENCE s={s} l={l} len={}: C=(st {cst}, {cval:?}) Rust={:?}",
            data.len(),
            v.data()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "bytea_substr error DIVERGENCE s={s} l={l}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            e.sqlstate
        ),
    }

    fc_expect_bytes_or_err(
        "fc_bytea_substr",
        vb::fc_bytea_substr,
        [image_datum(&img), Datum::from_i32(s), Datum::from_i32(l)],
        cst,
        cerr,
        cval,
    );
}

fn substr_no_len_diff(payload: &[u8]) {
    let Some((&m1, rest)) = payload.split_first() else { return };
    let Some((sraw, data)) = take_i32(rest) else { return };
    if data.len() > MAX_BYTEA {
        return;
    }
    let s = band_i32(m1, sraw, data.len() as i32);
    if !detoast_slice_installed() {
        return; // carved: no_len always reaches the slice (module doc)
    }

    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_bytea_substr_no_len(data.as_ptr(), data.len() as i32, s, cout.as_mut_ptr(), &mut cn)
    };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    let img = bytea_image(data);
    let cx = mcx::MemoryContext::new("vlbytea");
    match rb::bytea_substring(cx.mcx(), &img, s, -1, true) {
        Ok(v) => assert!(
            cst == 0 && v.data() == cval,
            "bytea_substr_no_len DIVERGENCE s={s} len={}: C={cval:?} Rust={:?}",
            data.len(),
            v.data()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "bytea_substr_no_len error DIVERGENCE s={s}: Err({:?})",
            e.sqlstate
        ),
    }

    fc_expect_bytes_or_err(
        "fc_bytea_substr_no_len",
        vb::fc_bytea_substr_no_len,
        [image_datum(&img), Datum::from_i32(s)],
        cst,
        cerr,
        cval,
    );
}

// ---------------------------------------------------------------------------
// Arms 21/22: byteaoverlay / byteaoverlay_no_len (oids 749/752).
// ---------------------------------------------------------------------------

fn overlay_diff(payload: &[u8]) {
    let Some((&m1, rest)) = payload.split_first() else { return };
    let Some((spraw, rest)) = take_i32(rest) else { return };
    let Some((&m2, rest)) = rest.split_first() else { return };
    let Some((slraw, rest)) = take_i32(rest) else { return };
    let Some((d1, d2)) = split2(rest) else { return };
    let sp = band_i32(m1, spraw, d1.len() as i32);
    let sl = band_i32(m2, slraw, d2.len() as i32);
    overlay_common(d1, d2, sp, Some(sl));
}

fn overlay_no_len_diff(payload: &[u8]) {
    let Some((&m1, rest)) = payload.split_first() else { return };
    let Some((spraw, rest)) = take_i32(rest) else { return };
    let Some((d1, d2)) = split2(rest) else { return };
    let sp = band_i32(m1, spraw, d1.len() as i32);
    overlay_common(d1, d2, sp, None);
}

fn overlay_common(d1: &[u8], d2: &[u8], sp: i32, sl: Option<i32>) {
    // Every Ok overlay path slices t1; only the sp<=0 / overflow error arms
    // are pre-seam.
    let reaches_slice = sp > 0 && sp.checked_add(sl.unwrap_or(d2.len() as i32)).is_some();
    if reaches_slice && !detoast_slice_installed() {
        return; // carved: detoast seam uninstalled (module doc)
    }
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let (name, cst) = match sl {
        Some(sl) => ("byteaoverlay", unsafe {
            pg_diff_byteaoverlay(
                d1.as_ptr(),
                d1.len() as i32,
                d2.as_ptr(),
                d2.len() as i32,
                sp,
                sl,
                cout.as_mut_ptr(),
                &mut cn,
            )
        }),
        None => ("byteaoverlay_no_len", unsafe {
            pg_diff_byteaoverlay_no_len(
                d1.as_ptr(),
                d1.len() as i32,
                d2.as_ptr(),
                d2.len() as i32,
                sp,
                cout.as_mut_ptr(),
                &mut cn,
            )
        }),
    };
    let cerr = c_errcode();
    let cval = &cout[..cn.max(0) as usize];

    // Shipped fc_byteaoverlay_no_len computes sl = byteaoctetlen(t2).
    let rsl = sl.unwrap_or(d2.len() as i32);
    let i1 = bytea_image(d1);
    let cx = mcx::MemoryContext::new("vlbytea");
    match rb::bytea_overlay(cx.mcx(), &i1, d2, sp, rsl) {
        Ok(v) => assert!(
            cst == 0 && v.data() == cval,
            "{name} DIVERGENCE sp={sp} sl={rsl} d1={d1:?} d2={d2:?}: C={cval:?} Rust={:?}",
            v.data()
        ),
        Err(e) => assert!(
            cst != 0 && rust_err_class(&e) == cerr,
            "{name} error DIVERGENCE sp={sp} sl={rsl}: C=(st {cst}, err {cerr}) Rust=Err({:?})",
            e.sqlstate
        ),
    }

    let i2 = bytea_image(d2);
    match sl {
        Some(sl) => fc_expect_bytes_or_err(
            "fc_byteaoverlay",
            vb::fc_byteaoverlay,
            [image_datum(&i1), image_datum(&i2), Datum::from_i32(sp), Datum::from_i32(sl)],
            cst,
            cerr,
            cval,
        ),
        None => fc_expect_bytes_or_err(
            "fc_byteaoverlay_no_len",
            vb::fc_byteaoverlay_no_len,
            [image_datum(&i1), image_datum(&i2), Datum::from_i32(sp)],
            cst,
            cerr,
            cval,
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 23: byteapos (oid 2014).
// ---------------------------------------------------------------------------

fn byteapos_diff(payload: &[u8]) {
    let Some((d1, d2)) = split2(payload) else { return };
    let mut cv: i32 = 0;
    let cst = unsafe {
        pg_diff_byteapos(d1.as_ptr(), d1.len() as i32, d2.as_ptr(), d2.len() as i32, &mut cv)
    };
    assert!(cst == 0);
    let rv = rb::byteapos(d1, d2);
    assert!(
        rv == cv,
        "byteapos DIVERGENCE d1={d1:?} d2={d2:?}: C={cv} Rust={rv}"
    );

    let (i1, i2) = (bytea_image(d1), bytea_image(d2));
    let d = fc_call(vb::fc_byteapos, None, None, [image_datum(&i1), image_datum(&i2)])
        .expect("fc_byteapos is infallible");
    assert!(
        d.as_i32() == rv,
        "fc_byteapos fc-wrapper DIVERGENCE: wrapper={} core={rv}",
        d.as_i32()
    );
}

// ---------------------------------------------------------------------------
// Arm 24: bytea_bit_count (oid 6163).
// ---------------------------------------------------------------------------

fn bit_count_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cv: i64 = 0;
    let cst = unsafe { pg_diff_bytea_bit_count(payload.as_ptr(), payload.len() as i32, &mut cv) };
    assert!(cst == 0);
    let rv = rb::bytea_bit_count(payload);
    assert!(
        rv == cv,
        "bytea_bit_count DIVERGENCE len={}: C={cv} Rust={rv}",
        payload.len()
    );

    let img = bytea_image(payload);
    let d = fc_call(vb::fc_bytea_bit_count, None, None, [image_datum(&img)])
        .expect("fc_bytea_bit_count is infallible");
    assert!(
        d.as_i64() == rv,
        "fc_bytea_bit_count fc-wrapper DIVERGENCE: wrapper={} core={rv}",
        d.as_i64()
    );
}

// ---------------------------------------------------------------------------
// Arm 36: hex_encode_into (no oid) — the public hex codec kernel other
// crates call directly (encode::Codec::Hex, backup/manifest), so it gets a
// dedicated arm instead of being reachable only through byteaout's "\x"
// path. C counterpart: encode.c hex_encode, vendored verbatim.
// ---------------------------------------------------------------------------

fn hex_encode_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cout = vec![0u8; 2 * payload.len() + 16];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_vlbytea_hex_encode(
            payload.as_ptr(),
            payload.len() as i32,
            cout.as_mut_ptr(),
            &mut cn,
        )
    };
    assert!(cst == 0, "hex_encode C oracle errored ({cst})");
    let cval = &cout[..cn.max(0) as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let mut out = mcx::vec_with_capacity_in(cx.mcx(), 2 * payload.len())
        .expect("proof-heap alloc for the hex output");
    rb::hex_encode_into(payload, &mut out);
    assert!(
        &out[..] == cval,
        "hex_encode_into DIVERGENCE len={}: C={cval:?} Rust={:?}",
        payload.len(),
        &out[..]
    );
}

// ---------------------------------------------------------------------------
// Arms 25..27: bytea_int2/int4/int8 (oids 6370/6371/6372) — 22003 above the
// width; seeds cover lengths 0..9 on both sides of every boundary.
// ---------------------------------------------------------------------------

fn bytea_int_diff(payload: &[u8], width: usize) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let data = payload;
    let (p, l) = (data.as_ptr(), data.len() as i32);
    match width {
        2 => {
            let mut cv: i16 = 0;
            let cst = unsafe { pg_diff_bytea_int2(p, l, &mut cv) };
            let cerr = c_errcode();
            match rb::bytea_int2(data) {
                Ok(rv) => assert!(
                    cst == 0 && rv == cv,
                    "bytea_int2 DIVERGENCE data={data:?}: C=(st {cst}, {cv}) Rust={rv}"
                ),
                Err(e) => assert!(
                    cst != 0 && rust_err_class(&e) == cerr,
                    "bytea_int2 error DIVERGENCE len={}: Err({:?})",
                    data.len(),
                    e.sqlstate
                ),
            }
            let img = bytea_image(data);
            match fc_call(vb::fc_bytea_int2, None, None, [image_datum(&img)]) {
                Ok(d) => assert!(cst == 0 && d.as_i16() == cv, "fc_bytea_int2 DIVERGENCE"),
                Err(e) => assert!(cst != 0 && rust_err_class(&e) == cerr, "fc_bytea_int2 error DIVERGENCE"),
            }
        }
        4 => {
            let mut cv: i32 = 0;
            let cst = unsafe { pg_diff_bytea_int4(p, l, &mut cv) };
            let cerr = c_errcode();
            match rb::bytea_int4(data) {
                Ok(rv) => assert!(
                    cst == 0 && rv == cv,
                    "bytea_int4 DIVERGENCE data={data:?}: C=(st {cst}, {cv}) Rust={rv}"
                ),
                Err(e) => assert!(
                    cst != 0 && rust_err_class(&e) == cerr,
                    "bytea_int4 error DIVERGENCE len={}: Err({:?})",
                    data.len(),
                    e.sqlstate
                ),
            }
            let img = bytea_image(data);
            match fc_call(vb::fc_bytea_int4, None, None, [image_datum(&img)]) {
                Ok(d) => assert!(cst == 0 && d.as_i32() == cv, "fc_bytea_int4 DIVERGENCE"),
                Err(e) => assert!(cst != 0 && rust_err_class(&e) == cerr, "fc_bytea_int4 error DIVERGENCE"),
            }
        }
        _ => {
            let mut cv: i64 = 0;
            let cst = unsafe { pg_diff_bytea_int8(p, l, &mut cv) };
            let cerr = c_errcode();
            match rb::bytea_int8(data) {
                Ok(rv) => assert!(
                    cst == 0 && rv == cv,
                    "bytea_int8 DIVERGENCE data={data:?}: C=(st {cst}, {cv}) Rust={rv}"
                ),
                Err(e) => assert!(
                    cst != 0 && rust_err_class(&e) == cerr,
                    "bytea_int8 error DIVERGENCE len={}: Err({:?})",
                    data.len(),
                    e.sqlstate
                ),
            }
            let img = bytea_image(data);
            match fc_call(vb::fc_bytea_int8, None, None, [image_datum(&img)]) {
                Ok(d) => assert!(cst == 0 && d.as_i64() == cv, "fc_bytea_int8 DIVERGENCE"),
                Err(e) => assert!(cst != 0 && rust_err_class(&e) == cerr, "fc_bytea_int8 error DIVERGENCE"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arms 28..30: int2/int4/int8_bytea (oids 6367/6368/6369) — total; the
// big-endian intNsend image.
// ---------------------------------------------------------------------------

fn int_bytea_diff(payload: &[u8], width: usize) {
    let mut cout = [0u8; 8];
    let mut cn: i32 = 0;
    let cx = mcx::MemoryContext::new("vlbytea");
    let m = cx.mcx();
    let (name, cst, rres, fcres) = match width {
        2 => {
            let Some(b) = payload.get(..2) else { return };
            let v = i16::from_le_bytes(b.try_into().unwrap());
            let cst = unsafe { pg_diff_int2_bytea(v, cout.as_mut_ptr(), &mut cn) };
            let r = rb::int_bytea(m, &v.to_be_bytes()).expect("int_bytea is infallible");
            let d = fc_call(vb::fc_int2_bytea, None, Some(m), [Datum::from_i16(v)])
                .expect("fc_int2_bytea is infallible");
            ("int2_bytea", cst, r, d)
        }
        4 => {
            let Some(b) = payload.get(..4) else { return };
            let v = i32::from_le_bytes(b.try_into().unwrap());
            let cst = unsafe { pg_diff_int4_bytea(v, cout.as_mut_ptr(), &mut cn) };
            let r = rb::int_bytea(m, &v.to_be_bytes()).expect("int_bytea is infallible");
            let d = fc_call(vb::fc_int4_bytea, None, Some(m), [Datum::from_i32(v)])
                .expect("fc_int4_bytea is infallible");
            ("int4_bytea", cst, r, d)
        }
        _ => {
            let Some(b) = payload.get(..8) else { return };
            let v = i64::from_le_bytes(b.try_into().unwrap());
            let cst = unsafe { pg_diff_int8_bytea(v, cout.as_mut_ptr(), &mut cn) };
            let r = rb::int_bytea(m, &v.to_be_bytes()).expect("int_bytea is infallible");
            let d = fc_call(vb::fc_int8_bytea, None, Some(m), [Datum::from_i64(v)])
                .expect("fc_int8_bytea is infallible");
            ("int8_bytea", cst, r, d)
        }
    };
    assert!(cst == 0);
    let cval = &cout[..cn as usize];
    assert!(
        rres.data() == cval,
        "{name} DIVERGENCE: C={cval:?} Rust={:?}",
        rres.data()
    );
    assert!(
        read_varlena_data(fcres) == cval,
        "fc_{name} fc-wrapper DIVERGENCE: wrapper={:?} C={cval:?}",
        read_varlena_data(fcres)
    );
}

// ---------------------------------------------------------------------------
// Arm 31: bytea_reverse (oid 6382).
// ---------------------------------------------------------------------------

fn bytea_reverse_diff(payload: &[u8]) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let mut cout = vec![0u8; C_OUT];
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_bytea_reverse(payload.as_ptr(), payload.len() as i32, cout.as_mut_ptr(), &mut cn)
    };
    assert!(cst == 0);
    let cval = &cout[..cn as usize];

    let cx = mcx::MemoryContext::new("vlbytea");
    let v = rb::bytea_reverse(cx.mcx(), payload).expect("bytea_reverse is infallible at this size");
    assert!(
        v.data() == cval,
        "bytea_reverse DIVERGENCE payload={payload:?}: C={cval:?} Rust={:?}",
        v.data()
    );

    let img = bytea_image(payload);
    fc_expect_bytes("fc_bytea_reverse", vb::fc_bytea_reverse, [image_datum(&img)], cval);
}

// ---------------------------------------------------------------------------
// Arms 32/34: hashvarlena / hashbytea (oids 456/6413) — exact u32 vs the
// vendored hashfn.c kernel; hashbytea is C's verbatim alias.
// ---------------------------------------------------------------------------

fn hash_diff(payload: &[u8], alias: bool) {
    if payload.len() > MAX_BYTEA {
        return;
    }
    let (name, ch) = if alias {
        ("hashbytea", unsafe { pg_diff_hashbytea(payload.as_ptr(), payload.len() as i32) })
    } else {
        ("hashvarlena", unsafe { pg_diff_hashvarlena(payload.as_ptr(), payload.len() as i32) })
    };
    let rh = hashfn::hash_bytes(payload);
    assert!(
        rh == ch,
        "{name} DIVERGENCE len={}: C={ch:#x} Rust={rh:#x}",
        payload.len()
    );

    // Catalog maps both oids 456 and 6413 to fc_hashvarlena.
    let img = bytea_image(payload);
    let d = fc_call(vb::fc_hashvarlena, None, None, [image_datum(&img)])
        .expect("fc_hashvarlena is infallible");
    assert!(
        d.as_u32() == rh,
        "fc_hashvarlena fc-wrapper DIVERGENCE ({name} arm): wrapper={:#x} core={rh:#x}",
        d.as_u32()
    );
}

// ---------------------------------------------------------------------------
// Arms 33/35: hashvarlenaextended / hashbyteaextended (oids 772/6414).
// ---------------------------------------------------------------------------

fn hash_ext_diff(payload: &[u8], alias: bool) {
    let Some((seed, key)) = take_i64(payload) else { return };
    if key.len() > MAX_BYTEA {
        return;
    }
    let seed = seed as u64;
    let (name, ch) = if alias {
        ("hashbyteaextended", unsafe {
            pg_diff_hashbyteaextended(key.as_ptr(), key.len() as i32, seed)
        })
    } else {
        ("hashvarlenaextended", unsafe {
            pg_diff_hashvarlenaextended(key.as_ptr(), key.len() as i32, seed)
        })
    };
    let rh = hashfn::hash_bytes_extended(key, seed);
    assert!(
        rh == ch,
        "{name} DIVERGENCE len={} seed={seed:#x}: C={ch:#x} Rust={rh:#x}",
        key.len()
    );
    // Seed-0 low-word identity (hashfn.c contract) rides the same exec.
    if seed == 0 {
        assert!(
            rh as u32 == hashfn::hash_bytes(key),
            "{name} seed-0 identity broke len={}",
            key.len()
        );
    }

    // Catalog maps both oids 772 and 6414 to fc_hashvarlenaextended.
    let img = bytea_image(key);
    let d = fc_call(
        vb::fc_hashvarlenaextended,
        None,
        None,
        [image_datum(&img), Datum::from_u64(seed)],
    )
    .expect("fc_hashvarlenaextended is infallible");
    assert!(
        d.as_u64() == rh,
        "fc_hashvarlenaextended fc-wrapper DIVERGENCE ({name} arm): wrapper={:#x} core={rh:#x}",
        d.as_u64()
    );
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke: drive every arm over regress-shaped seeds so
// `cargo test` exercises the C link + all comparators without cargo-fuzz.
// (These link once the parent uncomments the csrc gate in core/build.rs;
// `cargo check` stays green either way.)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        vlbytea_diff(&d);
    }

    /// [l1:2le][d1][d2] pair payload.
    fn pair(d1: &[u8], d2: &[u8]) -> Vec<u8> {
        let mut p = (d1.len() as u16).to_le_bytes().to_vec();
        p.extend_from_slice(d1);
        p.extend_from_slice(d2);
        p
    }

    /// byteain corpus: hex form, escape form, invalid forms, boundary shapes.
    fn in_corpus() -> Vec<Vec<u8>> {
        vec![
            b"".to_vec(),
            b"\\x".to_vec(),
            b"\\x0aFF".to_vec(),
            b"\\x 0a\t0B\n".to_vec(),
            b"\\x0".to_vec(),          // odd digits
            b"\\xzz".to_vec(),         // bad digit
            b"\\x0a ".to_vec(),        // trailing whitespace
            b"\\x0a\xc3".to_vec(),     // truncated UTF8 lead -> 22021 both sides
            b"\\x0a\x80".to_vec(),     // bad continuation byte -> 22023
            b"abc".to_vec(),
            b"\\000".to_vec(),
            b"\\377".to_vec(),
            b"\\\\".to_vec(),
            b"a\\134b".to_vec(),
            b"\\400".to_vec(),         // first octal digit out of 0..3
            b"\\07".to_vec(),          // too-short octal
            b"\\q".to_vec(),           // bad escape
            b"\\".to_vec(),            // lone backslash
            b"mixed\\001\\\\end".to_vec(),
            vec![b'A'; 300],
        ]
    }

    /// Witness pairs for the comparison family: single-byte differences at
    /// first/mid/last positions, both orders, prefix ties, unsigned-compare
    /// bytes (the packing/merge witness-pair obligation).
    fn cmp_pairs() -> Vec<Vec<u8>> {
        let mut v = vec![
            pair(b"", b""),
            pair(b"", b"a"),
            pair(b"a", b""),
            pair(b"abc", b"abc"),
            pair(b"abc", b"abd"),
            pair(b"abd", b"abc"),
            pair(b"xbc", b"ybc"),
            pair(b"ybc", b"xbc"),
            pair(b"aXc", b"aYc"),
            pair(b"aYc", b"aXc"),
            pair(b"ab", b"abc"),
            pair(b"abc", b"ab"),
            pair(b"a\xff", b"a\x00"),
            pair(b"a\x00", b"a\xff"),
            pair(b"\x80", b"\x7f"),
            pair(b"\x7f", b"\x80"),
        ];
        // one-byte-difference pairs at EVERY position of an 8-byte block,
        // both orders.
        for i in 0..8usize {
            let a = [0x55u8; 8];
            let mut b = a;
            b[i] ^= 0x01;
            v.push(pair(&a, &b));
            v.push(pair(&b, &a));
        }
        v
    }

    #[test]
    fn byteain_arm_corpus() {
        let _g = crate::c_oracle_serial();
        for s in in_corpus() {
            drive(0, &s);
        }
    }

    #[test]
    fn byteaout_both_modes() {
        let _g = crate::c_oracle_serial();
        for data in [&b""[..], b"\x01\\ z", b"\x00\x1f\x20\x7e\x7f\xff", b"plain"] {
            for mode in [0u8, 1u8] {
                let mut p = vec![mode];
                p.extend_from_slice(data);
                drive(1, &p);
            }
        }
    }

    #[test]
    fn recv_send_octetlen_cat_reverse_bitcount() {
        let _g = crate::c_oracle_serial();
        for data in [&b""[..], b"a", b"\x00\xff\x80", b"wire bytes \\x00"] {
            drive(2, data);
            drive(3, data);
            drive(4, data);
            drive(24, data);
            drive(31, data);
            drive(32, data);
            drive(34, data);
        }
        drive(5, &pair(b"abc", b"def"));
        drive(5, &pair(b"", b"xy"));
        drive(5, &pair(b"xy", b""));
    }

    #[test]
    fn cmp_family_witness_pairs() {
        let _g = crate::c_oracle_serial();
        for p in cmp_pairs() {
            for sel in 6u8..=14 {
                drive(sel, &p);
            }
            drive(23, &p); // byteapos over the same shapes
        }
    }

    fn scalar_payload_i32(m: u8, n: i32, data: &[u8]) -> Vec<u8> {
        let mut p = vec![m];
        p.extend_from_slice(&n.to_le_bytes());
        p.extend_from_slice(data);
        p
    }

    fn scalar_payload_i64(m: u8, n: i64, data: &[u8]) -> Vec<u8> {
        let mut p = vec![m];
        p.extend_from_slice(&n.to_le_bytes());
        p.extend_from_slice(data);
        p
    }

    #[test]
    fn get_set_byte_bit_ok_and_error_planes() {
        let _g = crate::c_oracle_serial();
        // in-range (mode 6 folds), exact raw (mode 4), extremes (modes 0..3).
        for m in [0u8, 1, 2, 3, 4, 5, 6, 7] {
            drive(15, &scalar_payload_i32(m, 1, b"abcd"));
            drive(17, &scalar_payload_i64(m, 9, b"abcd"));
        }
        drive(15, &scalar_payload_i32(4, 3, b"abcd")); // last byte
        drive(15, &scalar_payload_i32(4, 4, b"abcd")); // one past -> 2202E
        drive(17, &scalar_payload_i64(4, 31, b"abcd"));
        drive(17, &scalar_payload_i64(4, 32, b"abcd")); // one past -> 2202E
        drive(15, &scalar_payload_i32(4, 0, b"")); // empty -> 2202E

        // SetByte: [m][n:4][mb][nb:4][data]
        for (m, n, nbm, nb) in [
            (4u8, 0i32, 4u8, 0x41i32),
            (4, 3, 4, -1),
            (4, 4, 4, 7),      // index error
            (0, 0, 4, 7),      // INT_MIN index
            (4, 1, 3, 0),      // INT_MAX newByte (truncates in C and Rust)
        ] {
            let mut p = vec![m];
            p.extend_from_slice(&n.to_le_bytes());
            p.push(nbm);
            p.extend_from_slice(&nb.to_le_bytes());
            p.extend_from_slice(b"abcd");
            drive(16, &p);
        }

        // SetBit: [m][n:8][bm][data] — bm%6: 0/1 valid, 2..5 invalid bits.
        for (m, n, bm) in [
            (4u8, 0i64, 0u8),
            (4, 31, 1),
            (4, 5, 2),   // newBit -1 -> 22023
            (4, 5, 3),   // newBit 2 -> 22023
            (4, 32, 0),  // index error fires FIRST (C order pinned)
            (3, 0, 0),   // INT64_MAX index
            (0, 0, 1),   // INT64_MIN index
        ] {
            let mut p = vec![m];
            p.extend_from_slice(&n.to_le_bytes());
            p.push(bm);
            p.extend_from_slice(b"abcd");
            drive(18, &p);
        }
    }

    #[test]
    fn substr_overlay_planes() {
        let _g = crate::c_oracle_serial();
        // substr: [m1][s:4][m2][l:4][data]
        let sub = |m1: u8, s: i32, m2: u8, l: i32, data: &[u8]| {
            let mut p = vec![m1];
            p.extend_from_slice(&s.to_le_bytes());
            p.push(m2);
            p.extend_from_slice(&l.to_le_bytes());
            p.extend_from_slice(data);
            p
        };
        drive(19, &sub(4, 2, 4, 3, b"hello"));
        drive(19, &sub(4, -2, 4, 4, b"hello")); // negative start clamps
        drive(19, &sub(4, 2, 4, -1, b"hello")); // 22011
        drive(19, &sub(4, 0, 4, 0, b"hello")); // E < 1 -> empty
        drive(19, &sub(3, 0, 3, 0, b"hello")); // INT_MAX + INT_MAX overflow arm
        drive(19, &sub(0, 0, 3, 0, b"hello")); // INT_MIN start
        drive(19, &sub(4, 99, 4, 2, b"hello")); // past end -> empty
        drive(20, &scalar_payload_i32(4, 3, b"hello"));
        drive(20, &scalar_payload_i32(4, -5, b"hello"));
        drive(20, &scalar_payload_i32(0, 0, b"hello"));

        // overlay: [m1][sp:4][m2][sl:4][l1:2][d1d2]
        let ovl = |m1: u8, sp: i32, m2: u8, sl: i32, d1: &[u8], d2: &[u8]| {
            let mut p = vec![m1];
            p.extend_from_slice(&sp.to_le_bytes());
            p.push(m2);
            p.extend_from_slice(&sl.to_le_bytes());
            p.extend_from_slice(&pair(d1, d2));
            p
        };
        drive(21, &ovl(4, 2, 4, 3, b"hello", b"XY"));
        drive(21, &ovl(4, 0, 4, 3, b"hello", b"XY")); // sp<=0 -> 22011
        drive(21, &ovl(0, 0, 4, 3, b"hello", b"XY")); // INT_MIN sp -> 22011
        drive(21, &ovl(3, 0, 3, 0, b"hello", b"XY")); // sp+sl overflow -> 22003
        drive(21, &ovl(4, 6, 4, 0, b"hello", b"XY")); // append at end
        // overlay_no_len: [m1][sp:4][l1:2][d1d2]
        let ovn = |m1: u8, sp: i32, d1: &[u8], d2: &[u8]| {
            let mut p = vec![m1];
            p.extend_from_slice(&sp.to_le_bytes());
            p.extend_from_slice(&pair(d1, d2));
            p
        };
        drive(22, &ovn(4, 2, b"hello", b"XY"));
        drive(22, &ovn(4, -1, b"hello", b"XY"));
        drive(22, &ovn(3, 0, b"hello", b"XY")); // INT_MAX sp + sl=2 overflow
    }

    /// bytea_int2/4/8 length boundary sweep 0..9 (packing witness pairs:
    /// each length on both sides of every width boundary).
    #[test]
    fn bytea_int_boundary_lengths() {
        let _g = crate::c_oracle_serial();
        let data = [0x80u8, 0x01, 0xff, 0x00, 0x7f, 0xaa, 0x55, 0x10, 0x42];
        for len in 0..=9usize {
            for sel in [25u8, 26, 27] {
                drive(sel, &data[..len]);
            }
        }
    }

    #[test]
    fn int_to_bytea_total() {
        let _g = crate::c_oracle_serial();
        for sel_w in [(28u8, 2usize), (29, 4), (30, 8)] {
            for fill in [0x00u8, 0xff, 0x80, 0x01] {
                drive(sel_w.0, &vec![fill; sel_w.1]);
            }
        }
    }

    #[test]
    fn hash_arms() {
        let _g = crate::c_oracle_serial();
        for data in [&b""[..], b"a", b"abc", b"0123456789ab", &[0xffu8; 64][..]] {
            drive(32, data);
            drive(34, data);
            let mut p = 0u64.to_le_bytes().to_vec();
            p.extend_from_slice(data);
            drive(33, &p); // seed 0: low-word identity asserted in-arm
            drive(35, &p);
            let mut p = 0xdead_beef_cafe_f00du64.to_le_bytes().to_vec();
            p.extend_from_slice(data);
            drive(33, &p);
            drive(35, &p);
        }
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). Corpora are COMMITTED (plain `git add`).
    #[test]
    fn seed_corpus_replays_clean() {
        let _g = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/vlbytea_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/vlbytea_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                vlbytea_diff(&std::fs::read(&p).unwrap());
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
        for sel in 0u8..40 {
            for len in [0usize, 1, 2, 5, 9, 17, 33, 130, 300] {
                let payload: Vec<u8> = (0..len)
                    .map(|i| (i as u8).wrapping_mul(41).wrapping_add(sel))
                    .collect();
                drive(sel, &payload);
            }
        }
        vlbytea_diff(&[]);
    }
}
