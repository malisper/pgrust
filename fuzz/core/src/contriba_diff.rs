//! contriba_diff: differential fuzz driver for the two contrib "A" crates
//! vs verbatim vendored PostgreSQL 18.3 C (csrc/pg_contribafam_io.c,
//! upstream sha 62d6c7d3df; lane p1-mb-contriba). Selector = data[0] % 7:
//!
//!   0 soundex     — crates/contrib/fuzzystrmatch: SQL soundex + difference
//!                   via the dfmgr fc wrappers (contrib fns — no fixed
//!                   pg_proc oids, routes oid `-`) vs the verbatim C wrapper
//!                   bodies (_soundex + the difference loop).
//!   1 metaphone   — fc_metaphone (limits: >255-byte arg 22023, >255 reqlen
//!                   22023, <=0 reqlen 2200F, empty-word early return) vs
//!                   the verbatim metaphone wrapper body + _metaphone.
//!   2 levenshtein — all four fc wrappers (plain/with_costs/less_equal/
//!                   less_equal_with_costs) vs the two verbatim
//!                   levenshtein.c expansions. Encoding pinned UTF8 on both
//!                   sides每 exec (mbutils::SetDatabaseEncoding vs the
//!                   C TU's one-row pg_wchar_table); errors compared by
//!                   sqlstate (22023 length cap, 22021 invalid byte
//!                   sequence from the pg_mblen_range/with_len walkers).
//!   3 dmetaphone  — fc_dmetaphone + fc_dmetaphone_alt vs the verbatim
//!                   dmetaphone.c wrapper bodies + DoubleMetaphone.
//!   4 daitch      — fc_daitch_mokotoff (text[] result decoded element by
//!                   element, incl. the no-encodable-chars NULL arm) vs
//!                   verbatim daitch_mokotoff_coding (the C oracle captures
//!                   codes through the accumArrayResult seam).
//!   5 isn input   — crates/contrib/isn: string2ean core over accept in
//!                   Any..Upc x weak x {hard, soft escontext} (value +
//!                   verdict + sqlstate + the C ereturn soft-branch witness
//!                   counter) + the five fc *_in wrappers (hard AND armed
//!                   ErrorSaveNode shapes) + the isn.weak GUC fc pair
//!                   (fc_accept_weak_input / fc_weak_input_status).
//!   6 isn output  — ean2string / ean2isn cores (value bytes as the
//!                   PG-visible cstring, 22003 out-of-range, 22P02 wrong
//!                   type) + fc ean13_out / isn_out / the four
//!                   *_cast_from_ean13 / is_valid / make_valid wrappers.
//!
//! Comparison planes: value bytes + error verdict + exact sqlstate class;
//! message text out of scope. Soft-error plane (arm 5) ships the C-side
//! branch witness (pg_ca_soft_fired) per the one-sided-plane gate rule.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - arms 0-3 + 5: input bytes are NUL-sanitized (0x00 -> 0x01): PG text
//!     and cstrings never contain NUL (text_to_cstring output feeds every C
//!     path here). Arm 4 keeps raw bytes — both sides stop at the first
//!     NUL/decode-0 identically (read_char / the C cstring).
//!   - arm 2 costs and max_d folded to i16 range: PG accepts any int32, but
//!     the cost arithmetic then overflows int on both sides (C -fwrapv
//!     wrap vs Rust release wrap is identical, yet the fuzz build's
//!     overflow checks would panic Rust-side first); |cost| <= 32767 with
//!     len <= 384 keeps every intermediate < 2^26. Same-analysis precedent:
//!     miscfam arm-5 clock-domain fold.
//!   - arm 5 fc-input plane runs only when the isn.weak GUC round-trip
//!     (fc_accept_weak_input) succeeded, so the wrapper's g_weak() read is
//!     pinned to the same fuzz-selected weak value the C oracle got via
//!     pg_ca_set_weak. Core string2ean plane always runs (weak is a
//!     parameter there).
//!   - daitch fc wrapper's pg_server_to_any leg is identity under the UTF8
//!     pin (same carve as the C wrapper body not being vendored — the
//!     conversion is encoding machinery owned by the mbconv/wcharfam
//!     lanes).

#![allow(dead_code)]

use std::sync::Once;

use datum::{Datum, NullableDatum};
use isn::{ean2isn, ean2string, string2ean, IsnType, MAXEAN13LEN};
use types_error::{PgError, SoftErrorContext, SqlState};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_ca_soundex(arg0: *const u8) -> i32;
    fn pg_ca_difference(arg0: *const u8, arg1: *const u8) -> i32;
    fn pg_ca_metaphone(arg0: *const u8, reqlen: i32) -> i32;
    fn pg_ca_dmetaphone(arg0: *const u8) -> i32;
    fn pg_ca_dmetaphone_alt(arg0: *const u8) -> i32;
    fn pg_ca_levenshtein(
        s: *const u8,
        slen: i32,
        t: *const u8,
        tlen: i32,
        ins_c: i32,
        del_c: i32,
        sub_c: i32,
        out: *mut i32,
    ) -> i32;
    fn pg_ca_levenshtein_less_equal(
        s: *const u8,
        slen: i32,
        t: *const u8,
        tlen: i32,
        ins_c: i32,
        del_c: i32,
        sub_c: i32,
        max_d: i32,
        out: *mut i32,
    ) -> i32;
    fn pg_ca_daitch(word: *const u8, out: *mut u8, cap: i32, count: *mut i32) -> i32;
    fn pg_ca_set_weak(w: i32);
    fn pg_ca_string2ean(
        str_: *const u8,
        accept: i32,
        soft: i32,
        result: *mut u64,
        soft_fired: *mut i32,
    ) -> i32;
    fn pg_ca_ean2string(ean: u64, short_type: i32, out: *mut u8) -> i32;
    fn pg_ca_ean2isn(ean: u64, accept: i32, result: *mut u64) -> i32;
    fn pg_ca_out_get() -> *const u8;
    fn pg_ca_int_out_get() -> i32;
}

// C oracle errcode classes (pg_contribafam_io.c shim header).
const C_OK: i32 = 0;
const C_ERR_INVALID_TEXT: i32 = 1; // 22P02
const C_ERR_OUT_OF_RANGE: i32 = 2; // 22003
const C_ERR_INVALID_PARAM: i32 = 3; // 22023
const C_ERR_ZERO_LENGTH: i32 = 4; // 2200F
const C_ERR_INVALID_BYTE_SEQ: i32 = 5; // 22021
const C_ERR_INTERNAL: i32 = 6; // XX000

fn c_class(sqlstate: SqlState) -> i32 {
    if sqlstate == types_error::ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if sqlstate == types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if sqlstate == types_error::ERRCODE_INVALID_PARAMETER_VALUE {
        C_ERR_INVALID_PARAM
    } else if sqlstate == types_error::ERRCODE_ZERO_LENGTH_CHARACTER_STRING {
        C_ERR_ZERO_LENGTH
    } else if sqlstate == types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_INVALID_BYTE_SEQ
    } else if sqlstate == types_error::ERRCODE_INTERNAL_ERROR {
        C_ERR_INTERNAL
    } else {
        99
    }
}

/// Byte-cursor over the fuzz payload; exhausted reads return zeros.
struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn i16(&mut self) -> i16 {
        self.u16() as i16
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for s in &mut b {
            *s = self.u8();
        }
        u64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let start = self.pos.min(self.d.len());
        let end = (self.pos + n).min(self.d.len());
        self.pos += n;
        &self.d[start..end]
    }
}

/// NUL-sanitized copy with a trailing NUL (the PG text / cstring domain —
/// see DOMAIN CARVES) suitable to hand to both sides.
fn cstring_of(bytes: &[u8]) -> Vec<u8> {
    let mut v: Vec<u8> = bytes.iter().map(|&b| if b == 0 { 1 } else { b }).collect();
    v.push(0);
    v
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (mac_diff pattern: native LocalFcinfo, real mcx).
// ---------------------------------------------------------------------------

/// Process-global GUC environment for the isn.weak fc plane. See the
/// FUZZ-BINARY ONLY note in seams_setup for why this is gated.
fn guc_env_bootstrap() {
    use std::panic::catch_unwind;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = catch_unwind(guc_tables::init_seams);
        let _ = catch_unwind(elog::init_seams);
        let _ = catch_unwind(guc::init_seams);
        let _ = catch_unwind(|| xact_seams::is_in_parallel_mode::set(|| false));
        // the SHIPPED bool parser (computation stays real; seam = wiring)
        let _ = catch_unwind(|| scalar_seams::parse_bool::set(adt_bool::parse_bool));
        let _ = catch_unwind(|| aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true)));
        let _ = catch_unwind(|| superuser_seams::superuser::set(|| Ok(true)));
        if !guc::store::is_initialized() {
            let _ = catch_unwind(guc::store::initialize_guc_options);
        }
    });
    // Session identity is THREAD-LOCAL, so it must be (re)set per thread,
    // never once per process — libtest runs each test on its own thread.
    miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);
}

fn seams_setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use std::panic::catch_unwind;
        // First-wins across lanes sharing one test binary (seam set()
        // panics on a second install; every impl below is the standard
        // environment the other lanes install too).
        let _ = catch_unwind(fuzzystrmatch::init_seams);
        let _ = catch_unwind(isn::init_seams);
        let _ = catch_unwind(mbutils::init_seams);
    });
    // GUC ENVIRONMENT (isn.weak fc plane) — FUZZ-BINARY ONLY.
    //
    // The bootstrap below installs PROCESS-GLOBAL seams (guc_tables, elog,
    // guc) that sibling lanes install UNGUARDED inside their own `Once`
    // (e.g. datetime_io_diff::init_env). seam_core's set() panics on a
    // second install, so whoever runs first poisons the other's Once — and
    // in the shared `cargo test` binary every lane's driver lives in one
    // process. Installing here therefore breaks eleven datetime tests.
    // The dedicated `contriba_diff` fuzz binary has no such neighbours, and
    // it is the binary the coverage capture replays, so the plane is fully
    // exercised exactly where it is measured. Under `cargo test` the fc
    // isn.weak leg degrades to the already-handled "GUC store unavailable"
    // path.
    if cfg!(fuzzing) {
        guc_env_bootstrap();
    }
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 pin");
}

/// Residual-line legs that need no fuzz input: the two dfmgr lookup tables'
/// miss arms, and g_weak()'s "isn.weak not in the store" read (the crate's
/// documented placeholder-GUC divergence — reachable only before the first
/// SET, so it runs once, first).
fn lookup_miss_legs() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // g_weak() before any SET: GetConfigOption -> Ok(None) -> false.
        let ctx = mcx::MemoryContext::new("contriba_gweak_probe");
        if let Ok(Ok(d)) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fc_call(lookup("isn", "weak_input_status"), ctx.mcx(), [])
        })) {
            assert!(!d.as_bool(), "unset isn.weak reads false");
        }
    });
    assert!(matches!(
        dfmgr::load_external_function("fuzzystrmatch", "no_such_function", false),
        Ok(None)
    ));
    assert!(matches!(
        dfmgr::load_external_function("isn", "no_such_function", false),
        Ok(None)
    ));
}

fn lookup(lib: &str, name: &str) -> PGFunction {
    dfmgr::load_external_function(lib, name, true)
        .expect("library registered")
        .expect("function resolves")
}

fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    f(None, &mut fcinfo)
}

/// 4B-U text varlena image: [4-byte LE header][payload].
fn text_image(bytes: &[u8]) -> Vec<u8> {
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    img.extend_from_slice(bytes);
    img
}

/// Read back a 4B-U varlena result datum's payload.
///
/// SAFETY: `d` came from a wrapper returning a live 4B-header varlena in the
/// arming context.
unsafe fn result_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let word = u32::from_le_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
    let total = (word >> 2) as usize;
    std::slice::from_raw_parts(p.add(4), total - 4)
}

/// Read back a cstring result datum's bytes (isn *_out wrappers).
///
/// SAFETY: `d` points at a live NUL-terminated allocation.
unsafe fn result_cstring<'a>(d: Datum) -> &'a [u8] {
    std::ffi::CStr::from_ptr(d.as_usize() as *const std::os::raw::c_char).to_bytes()
}

/// The C TLS out buffer as a NUL-terminated byte view.
fn c_out() -> &'static [u8] {
    // SAFETY: pg_ca_out is a live NUL-terminated TLS buffer.
    unsafe { std::ffi::CStr::from_ptr(pg_ca_out_get().cast()).to_bytes() }
}

// ---------------- arm 0: soundex + difference ----------------

fn run_soundex(r: &mut Rdr) {
    let n1 = r.u8() as usize;
    let n2 = r.u8() as usize;
    let s1 = cstring_of(r.bytes(n1));
    let s2 = cstring_of(r.bytes(n2));

    let crc = unsafe { pg_ca_soundex(s1.as_ptr()) };
    assert_eq!(crc, 0, "C soundex errored");
    let cval = c_out().to_vec();

    let ctx = mcx::MemoryContext::new("contriba_fc");
    let img = text_image(&s1[..s1.len() - 1]);
    let d = fc_call(
        lookup("fuzzystrmatch", "soundex"),
        ctx.mcx(),
        [Datum::from_usize(img.as_ptr() as usize)],
    )
    .expect("fc_soundex never errors");
    // SAFETY: fc_soundex returns a live text varlena in ctx.
    let rval = unsafe { result_payload(d) };
    assert_eq!(rval, &cval[..], "soundex({s1:?})");

    // difference
    let crc = unsafe { pg_ca_difference(s1.as_ptr(), s2.as_ptr()) };
    assert_eq!(crc, 0, "C difference errored");
    let cdiff = unsafe { pg_ca_int_out_get() };
    let img1 = text_image(&s1[..s1.len() - 1]);
    let img2 = text_image(&s2[..s2.len() - 1]);
    let d = fc_call(
        lookup("fuzzystrmatch", "difference"),
        ctx.mcx(),
        [
            Datum::from_usize(img1.as_ptr() as usize),
            Datum::from_usize(img2.as_ptr() as usize),
        ],
    )
    .expect("fc_difference never errors");
    assert_eq!(d.as_i32(), cdiff, "difference({s1:?},{s2:?})");
}

// ---------------- arm 1: metaphone ----------------

fn run_metaphone(r: &mut Rdr) {
    // reqlen spans <=0 / 1..=255 / >255 (see wrapper limits).
    let reqlen = (r.u16() as i32 % 300) - 20;
    let n = r.u16() as usize % 300;
    let word = cstring_of(r.bytes(n));

    let crc = unsafe { pg_ca_metaphone(word.as_ptr(), reqlen) };
    let cval = if crc == 0 { Some(c_out().to_vec()) } else { None };

    let ctx = mcx::MemoryContext::new("contriba_fc");
    let img = text_image(&word[..word.len() - 1]);
    let res = fc_call(
        lookup("fuzzystrmatch", "metaphone"),
        ctx.mcx(),
        [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(reqlen),
        ],
    );
    match res {
        Ok(d) => {
            // SAFETY: live text varlena in ctx.
            let rval = unsafe { result_payload(d) };
            assert_eq!(crc, 0, "metaphone({word:?},{reqlen}): C errored {crc}, Rust ok");
            assert_eq!(rval, &cval.unwrap()[..], "metaphone({word:?},{reqlen})");
        }
        Err(e) => {
            assert_eq!(
                c_class(e.sqlstate),
                crc,
                "metaphone({word:?},{reqlen}) error class (rust {:?})",
                e.sqlstate
            );
        }
    }
}

// ---------------- arm 2: levenshtein ----------------

fn run_levenshtein(r: &mut Rdr) {
    let variant = r.u8();
    let ins_c = r.i16() as i32;
    let del_c = r.i16() as i32;
    let sub_c = r.i16() as i32;
    let max_d = r.i16() as i32;
    let n1 = r.u16() as usize % 384;
    let n2 = r.u16() as usize % 384;
    let s = cstring_of(r.bytes(n1));
    let t = cstring_of(r.bytes(n2));
    let (sb, tb) = (&s[..s.len() - 1], &t[..t.len() - 1]);

    let with_costs = variant & 1 != 0;
    let less_equal = variant & 2 != 0;
    let (ic, dc, sc) = if with_costs { (ins_c, del_c, sub_c) } else { (1, 1, 1) };

    let mut cd: i32 = 0;
    let crc = if less_equal {
        unsafe {
            pg_ca_levenshtein_less_equal(
                sb.as_ptr(),
                sb.len() as i32,
                tb.as_ptr(),
                tb.len() as i32,
                ic,
                dc,
                sc,
                max_d,
                &mut cd,
            )
        }
    } else {
        unsafe {
            pg_ca_levenshtein(
                sb.as_ptr(),
                sb.len() as i32,
                tb.as_ptr(),
                tb.len() as i32,
                ic,
                dc,
                sc,
                &mut cd,
            )
        }
    };

    let ctx = mcx::MemoryContext::new("contriba_fc");
    let img1 = text_image(sb);
    let img2 = text_image(tb);
    let d1 = Datum::from_usize(img1.as_ptr() as usize);
    let d2 = Datum::from_usize(img2.as_ptr() as usize);

    let res = match (less_equal, with_costs) {
        (false, false) => fc_call(lookup("fuzzystrmatch", "levenshtein"), ctx.mcx(), [d1, d2]),
        (false, true) => fc_call(
            lookup("fuzzystrmatch", "levenshtein_with_costs"),
            ctx.mcx(),
            [d1, d2, Datum::from_i32(ic), Datum::from_i32(dc), Datum::from_i32(sc)],
        ),
        (true, false) => fc_call(
            lookup("fuzzystrmatch", "levenshtein_less_equal"),
            ctx.mcx(),
            [d1, d2, Datum::from_i32(max_d)],
        ),
        (true, true) => fc_call(
            lookup("fuzzystrmatch", "levenshtein_less_equal_with_costs"),
            ctx.mcx(),
            [
                d1,
                d2,
                Datum::from_i32(ic),
                Datum::from_i32(dc),
                Datum::from_i32(sc),
                Datum::from_i32(max_d),
            ],
        ),
    };
    match res {
        Ok(d) => {
            assert_eq!(crc, 0, "levenshtein(le={less_equal},wc={with_costs}): C errored {crc}, Rust ok ({s:?},{t:?})");
            assert_eq!(d.as_i32(), cd, "levenshtein value ({s:?},{t:?},{ic},{dc},{sc},{max_d})");
        }
        Err(e) => {
            assert_ne!(crc, 0, "levenshtein: Rust errored {:?}, C ok ({s:?},{t:?})", e.sqlstate);
            assert_eq!(c_class(e.sqlstate), crc, "levenshtein error class ({s:?},{t:?})");
        }
    }
}

// ---------------- arm 3: dmetaphone ----------------

fn run_dmetaphone(r: &mut Rdr) {
    let n = r.u16() as usize % 300;
    let word = cstring_of(r.bytes(n));
    let ctx = mcx::MemoryContext::new("contriba_fc");
    let img = text_image(&word[..word.len() - 1]);

    for (alt, name) in [(false, "dmetaphone"), (true, "dmetaphone_alt")] {
        let crc = unsafe {
            if alt {
                pg_ca_dmetaphone_alt(word.as_ptr())
            } else {
                pg_ca_dmetaphone(word.as_ptr())
            }
        };
        assert_eq!(crc, 0, "C {name} errored");
        let cval = c_out().to_vec();
        let d = fc_call(
            lookup("fuzzystrmatch", name),
            ctx.mcx(),
            [Datum::from_usize(img.as_ptr() as usize)],
        )
        .unwrap_or_else(|e| panic!("{name} unexpectedly errored: {:?}", e.sqlstate));
        // SAFETY: live text varlena in ctx.
        let rval = unsafe { result_payload(d) };
        assert_eq!(rval, &cval[..], "{name}({word:?})");
    }
}

// ---------------- arm 4: daitch_mokotoff ----------------

/// Decode a 1-D text[] array image (typalign 'i') into its element payloads.
///
/// SAFETY: `d` points at a live construct_array image in the arming context.
unsafe fn decode_text_array<'a>(d: Datum) -> Vec<&'a [u8]> {
    let base = d.as_usize() as *const u8;
    let rd_u32 = |off: usize| -> u32 {
        u32::from_le_bytes([
            *base.add(off),
            *base.add(off + 1),
            *base.add(off + 2),
            *base.add(off + 3),
        ])
    };
    let ndim = rd_u32(4) as usize;
    assert_eq!(ndim, 1, "daitch result array ndim");
    let dataoffset = rd_u32(8) as usize;
    assert_eq!(dataoffset, 0, "daitch result array has no nulls");
    let nelems = rd_u32(16) as usize;
    let mut off = 24usize; // vl_len + ndim + dataoffset + elemtype + dim + lbound
    let mut out = Vec::with_capacity(nelems);
    for _ in 0..nelems {
        let word = rd_u32(off);
        let total = (word >> 2) as usize;
        out.push(std::slice::from_raw_parts(base.add(off + 4), total - 4));
        off += total;
        off = (off + 3) & !3; // typalign 'i'
    }
    out
}

fn run_daitch(r: &mut Rdr) {
    let n = r.u16() as usize % 200;
    // Raw bytes: both sides stop at the first NUL/undecodable-0 identically.
    //
    // ORACLE OVER-READ SEAM (found by this target, 2026-08-01): C's
    // read_char calls utf8_to_unicode, documented "no error checks here, c
    // must point to a long-enough string" (pg_wchar.h:563). A TRUNCATED
    // multibyte lead at end-of-cstring makes it read 1-3 bytes PAST the
    // NUL — past the caller's buffer. The shipped Rust models exactly that
    // read as a zero-pad (dmetaphone.rs read_char's "C decodes into its
    // cstring NUL terminator" comment), so the two agree iff those
    // past-the-NUL bytes are zero. We therefore hand the C side FOUR
    // trailing NULs: the read becomes defined and equals the Rust model,
    // keeping every byte pattern in the domain instead of carving invalid
    // UTF-8 tails out. Unreachable in a server (text values are
    // encoding-validated before they reach a contrib function), so this is
    // a driver buffer contract, not an upstream defect.
    let mut word = r.bytes(n).to_vec();
    word.extend_from_slice(&[0u8; 4]);

    let mut codes = vec![0u8; 6 * 4096];
    let mut count: i32 = 0;
    let crc = unsafe { pg_ca_daitch(word.as_ptr(), codes.as_mut_ptr(), 4096, &mut count) };
    assert!(crc == 0 || crc == -1, "C daitch errclass {crc}");

    let ctx = mcx::MemoryContext::new("contriba_fc");
    let img = text_image(&word[..word.len() - 4]);
    let f = lookup("fuzzystrmatch", "daitch_mokotoff");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.args[0] = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let d = f(None, &mut fcinfo).expect("fc_daitch_mokotoff never errors");

    if crc == -1 {
        assert!(fcinfo.isnull, "daitch({word:?}): C no-encodable, Rust non-null");
        return;
    }
    assert!(!fcinfo.isnull, "daitch({word:?}): Rust NULL, C coded");
    // SAFETY: live array image in ctx.
    let relems = unsafe { decode_text_array(d) };
    assert_eq!(relems.len(), count as usize, "daitch({word:?}) code count");
    for (i, e) in relems.iter().enumerate() {
        assert_eq!(*e, &codes[6 * i..6 * i + 6], "daitch({word:?}) code {i}");
    }
}

// ---------------- arms 5/6: isn ----------------

fn isn_type_of(v: u8) -> (IsnType, i32) {
    match v % 6 {
        0 => (IsnType::Any, 1),
        1 => (IsnType::Ean13, 2),
        2 => (IsnType::Isbn, 3),
        3 => (IsnType::Ismn, 4),
        4 => (IsnType::Issn, 5),
        _ => (IsnType::Upc, 6),
    }
}

/// The five *_in wrapper names by C accept value (2..=6).
fn in_fn_of(caccept: i32) -> Option<&'static str> {
    Some(match caccept {
        2 => "ean13_in",
        3 => "isbn_in",
        4 => "ismn_in",
        5 => "issn_in",
        6 => "upc_in",
        _ => return None,
    })
}

fn run_isn_input(r: &mut Rdr) {
    let (accept, caccept) = isn_type_of(r.u8());
    let flags = r.u8();
    let weak = flags & 1 != 0;
    let soft = flags & 2 != 0;
    let n = r.u8() as usize % 40;
    let input = cstring_of(r.bytes(n));
    let ib = &input[..input.len() - 1];

    unsafe { pg_ca_set_weak(weak as i32) };
    let mut cres: u64 = 0;
    let mut csf: i32 = 0;
    let crc = unsafe {
        pg_ca_string2ean(input.as_ptr(), caccept, soft as i32, &mut cres, &mut csf)
    };

    // Core plane (weak is a parameter on the Rust side).
    let mut esc = SoftErrorContext::new(true);
    let rres = string2ean(ib, soft.then_some(&mut esc), accept, weak);
    match rres {
        Ok(Some(v)) => {
            assert_eq!(crc, 0, "string2ean({input:?},{caccept},w{weak}): C errored {crc}, Rust ok");
            assert_eq!(v, cres, "string2ean({input:?},{caccept},w{weak}) value");
            assert_eq!(csf, 0, "string2ean soft witness fired on C success");
        }
        Ok(None) => {
            // Soft error saved.
            assert!(soft, "Ok(None) without escontext");
            assert!(esc.error_occurred(), "Ok(None) but no error_occurred");
            assert_ne!(crc, 0, "string2ean({input:?},{caccept}): Rust soft-failed, C ok");
            assert_eq!(csf, 1, "C ereturn soft branch did not fire (witness)");
            let e = esc.error().expect("details_wanted saves the error");
            assert_eq!(c_class(e.sqlstate), crc, "string2ean({input:?},{caccept}) soft class");
        }
        Err(e) => {
            assert!(!soft || is_hard_even_soft(&e), "hard error under escontext: {:?}", e.sqlstate);
            assert_ne!(crc, 0, "string2ean({input:?},{caccept},w{weak}): Rust errored, C ok");
            assert_eq!(csf, 0, "C took the soft branch, Rust the hard one");
            assert_eq!(c_class(e.sqlstate), crc, "string2ean({input:?},{caccept}) hard class");
        }
    }

    // fc plane: only for the five typed input functions, and only with the
    // GUC pinned to the same weak value (see DOMAIN CARVES).
    let Some(fname) = in_fn_of(caccept) else { return };
    let ctx = mcx::MemoryContext::new("contriba_fc");
    // The GUC set path needs session identity (GetUserId) this bare fuzz
    // process does not have; a panic there = "GUC store unavailable", the
    // same degradation as an Err (see DOMAIN CARVES).
    let guc_ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fc_call(
            lookup("isn", "accept_weak_input"),
            ctx.mcx(),
            [Datum::from_bool(weak)],
        )
    }));
    let guc_ok = match guc_ok {
        Ok(Ok(d)) => {
            assert_eq!(d.as_bool(), weak, "fc_accept_weak_input readback");
            let st = fc_call(lookup("isn", "weak_input_status"), ctx.mcx(), [])
                .expect("fc_weak_input_status never errors");
            assert_eq!(st.as_bool(), weak, "fc_weak_input_status");
            true
        }
        _ => false,
    };
    if !guc_ok && weak {
        return; // g_weak() would read a value we cannot pin
    }
    if !guc_ok {
        // g_weak() must still read false through the placeholder store for
        // the fc plane below to be pinned; probe it panics-safely first.
        let readable = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fc_call(lookup("isn", "weak_input_status"), ctx.mcx(), [])
        }));
        match readable {
            Ok(Ok(d)) => assert!(!d.as_bool(), "unset isn.weak must read false"),
            _ => return,
        }
    }

    let f = lookup("isn", fname);
    // hard shape + (sometimes) armed ErrorSaveNode shape
    let arm_soft = soft;
    let mut node = ErrorSaveNode::new(true);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    if arm_soft {
        fcinfo.context = node.fm_node_ptr();
    }
    fcinfo.args[0] = NullableDatum::value(Datum::from_usize(input.as_ptr() as usize));
    match f(None, &mut fcinfo) {
        Ok(d) => {
            if arm_soft && node.ctx.error_occurred() {
                assert_ne!(crc, 0, "{fname}({input:?}): fc soft-failed, C ok");
                let e = node.ctx.error().expect("details wanted");
                assert_eq!(c_class(e.sqlstate), crc, "{fname}({input:?}) fc soft class");
            } else {
                assert_eq!(crc, 0, "{fname}({input:?}): C errored {crc}, fc ok");
                assert_eq!(d.as_i64() as u64, cres, "{fname}({input:?}) fc value");
            }
        }
        Err(e) => {
            assert_ne!(crc, 0, "{fname}({input:?}): fc errored, C ok");
            assert_eq!(c_class(e.sqlstate), crc, "{fname}({input:?}) fc class");
        }
    }
}

/// string2ean escapes to a hard error under an armed escontext only for
/// classes it never softens; with the full ereturn inventory soft, any hard
/// Err under soft shape is a divergence candidate — keep the predicate
/// explicit so the panic message names it.
fn is_hard_even_soft(_e: &PgError) -> bool {
    false
}

fn run_isn_output(r: &mut Rdr) {
    let flags = r.u8();
    let short_type = flags & 1 != 0;
    // Half the domain folded near the EAN13 range so hyphenation runs; raw
    // u64 half covers the 22003 out-of-range arm (ean>>1 > 9999999999999).
    let raw = r.u64();
    let val = if flags & 2 != 0 {
        raw
    } else {
        raw % ((9_999_999_999_999u64 << 1) | 2)
    };

    // ean2string core
    let mut cbuf = [0u8; MAXEAN13LEN + 1];
    let crc = unsafe { pg_ca_ean2string(val, short_type as i32, cbuf.as_mut_ptr()) };
    let mut rbuf = [0u8; MAXEAN13LEN + 1];
    let rres = ean2string(val, &mut rbuf, short_type);
    let c_str = |b: &[u8]| b.iter().position(|&c| c == 0).map(|i| b[..i].to_vec()).unwrap();
    match rres {
        Ok(()) => {
            assert_eq!(crc, 0, "ean2string({val},{short_type}): C errored {crc}, Rust ok");
            assert_eq!(c_str(&rbuf), c_str(&cbuf), "ean2string({val},{short_type})");
        }
        Err(e) => {
            assert_ne!(crc, 0, "ean2string({val}): Rust errored, C ok");
            assert_eq!(c_class(e.sqlstate), crc, "ean2string({val}) class");
        }
    }

    // fc out wrappers
    let ctx = mcx::MemoryContext::new("contriba_fc");
    let fname = if short_type { "isn_out" } else { "ean13_out" };
    match fc_call(lookup("isn", fname), ctx.mcx(), [Datum::from_i64(val as i64)]) {
        Ok(d) => {
            assert_eq!(crc, 0, "{fname}({val}): C errored {crc}, fc ok");
            // SAFETY: live cstring in ctx.
            let rv = unsafe { result_cstring(d) };
            assert_eq!(rv, &c_str(&cbuf)[..], "{fname}({val})");
        }
        Err(e) => {
            assert_ne!(crc, 0, "{fname}({val}): fc errored, C ok");
            assert_eq!(c_class(e.sqlstate), crc, "{fname}({val}) fc class");
        }
    }

    // ean2isn core + the four cast wrappers
    let (accept, caccept) = isn_type_of(r.u8());
    let mut cres: u64 = 0;
    let crc = unsafe { pg_ca_ean2isn(val, caccept, &mut cres) };
    match ean2isn(val, accept) {
        Ok(v) => {
            assert_eq!(crc, 0, "ean2isn({val},{caccept}): C errored {crc}, Rust ok");
            assert_eq!(v, cres, "ean2isn({val},{caccept}) value");
        }
        Err(e) => {
            assert_ne!(crc, 0, "ean2isn({val},{caccept}): Rust errored, C ok");
            assert_eq!(c_class(e.sqlstate), crc, "ean2isn({val},{caccept}) class");
        }
    }
    let cast_fn = match caccept {
        3 => Some("isbn_cast_from_ean13"),
        4 => Some("ismn_cast_from_ean13"),
        5 => Some("issn_cast_from_ean13"),
        6 => Some("upc_cast_from_ean13"),
        _ => None,
    };
    if let Some(fname) = cast_fn {
        match fc_call(lookup("isn", fname), ctx.mcx(), [Datum::from_i64(val as i64)]) {
            Ok(d) => {
                assert_eq!(crc, 0, "{fname}({val}): C errored {crc}, fc ok");
                assert_eq!(d.as_i64() as u64, cres, "{fname}({val}) value");
            }
            Err(e) => {
                assert_ne!(crc, 0, "{fname}({val}): fc errored, C ok");
                assert_eq!(c_class(e.sqlstate), crc, "{fname}({val}) fc class");
            }
        }
    }

    // is_valid / make_valid (pure bit ops; C truth is the ean13 contract)
    let d = fc_call(lookup("isn", "is_valid"), ctx.mcx(), [Datum::from_i64(val as i64)])
        .expect("is_valid never errors");
    assert_eq!(d.as_bool(), (val & 1) == 0, "is_valid({val})");
    let d = fc_call(lookup("isn", "make_valid"), ctx.mcx(), [Datum::from_i64(val as i64)])
        .expect("make_valid never errors");
    assert_eq!(d.as_i64() as u64, val & !1u64, "make_valid({val})");
}

// ---------------- entry ----------------

pub fn contriba_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    seams_setup();
    lookup_miss_legs();
    let mut r = Rdr::new(payload);
    match sel % 7 {
        0 => run_soundex(&mut r),
        1 => run_metaphone(&mut r),
        2 => run_levenshtein(&mut r),
        3 => run_dmetaphone(&mut r),
        4 => run_daitch(&mut r),
        5 => run_isn_input(&mut r),
        _ => run_isn_output(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The isn.weak GUC environment must actually work in this process —
    /// otherwise the fc-input plane silently degrades to weak=false only.
    /// IGNORED BY DEFAULT — running it installs the process-global GUC
    /// seams that sibling lanes install unguarded, poisoning their `Once`
    /// in the shared `cargo test` binary (see seams_setup). Run standalone:
    ///   cargo test -p decoder_fuzz contriba_diff::tests::guc_env_works -- --ignored
    /// The standing witness that this plane is live is the coverage bank:
    /// crates/contrib/isn/src/builtins.rs has exactly ONE residual line
    /// (111, the SetConfigOption `?` closer) — every other line of
    /// fc_accept_weak_input / fc_weak_input_status / g_weak is measured.
    #[test]
    #[ignore = "installs process-global GUC seams; poisons sibling lanes in the shared test binary"]
    fn guc_env_works() {
        seams_setup();
        guc_env_bootstrap();
        let ctx = mcx::MemoryContext::new("contriba_guc_probe");
        for weak in [true, false] {
            let d = fc_call(
                lookup("isn", "accept_weak_input"),
                ctx.mcx(),
                [Datum::from_bool(weak)],
            )
            .expect("accept_weak_input works under the bootstrap env");
            assert_eq!(d.as_bool(), weak);
            let st = fc_call(lookup("isn", "weak_input_status"), ctx.mcx(), [])
                .expect("weak_input_status works");
            assert_eq!(st.as_bool(), weak);
        }
    }

    /// Deterministic seeds through every arm (smoke for all planes).
    #[test]
    fn arm_smoke() {
        // soundex/difference
        let mut v = vec![0u8, 8, 8];
        v.extend_from_slice(b"Anderson");
        v.extend_from_slice(b"Andersen");
        contriba_diff(&v);
        // interior-vowel + every-code witness (injection-sweep plant G)
        let mut v = vec![0u8, 10, 4];
        v.extend_from_slice(b"Tatawwrrcc");
        v.extend_from_slice(b"Bobs");
        contriba_diff(&v);
        // metaphone: valid reqlen + word
        let mut v = vec![1u8];
        v.extend_from_slice(&24u16.to_le_bytes()); // reqlen fold -> 4
        v.extend_from_slice(&5u16.to_le_bytes());
        v.extend_from_slice(b"GUMBO");
        contriba_diff(&v);
        // metaphone error arms: reqlen<=0 and long word
        let mut v = vec![1u8];
        v.extend_from_slice(&20u16.to_le_bytes()); // -> 0 => 2200F
        v.extend_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(b"abc");
        contriba_diff(&v);
        // levenshtein all four variants
        for variant in 0u8..4 {
            let mut v = vec![2u8, variant];
            v.extend_from_slice(&1i16.to_le_bytes());
            v.extend_from_slice(&1i16.to_le_bytes());
            v.extend_from_slice(&1i16.to_le_bytes());
            v.extend_from_slice(&2i16.to_le_bytes()); // max_d
            v.extend_from_slice(&9u16.to_le_bytes());
            v.extend_from_slice(&10u16.to_le_bytes());
            v.extend_from_slice(b"extensive");
            v.extend_from_slice(b"exhaustive");
            contriba_diff(&v);
        }
        // less_equal with a max_d loose enough to clamp stop_column to m0+1
        let mut v = vec![2u8, 2];
        v.extend_from_slice(&1i16.to_le_bytes());
        v.extend_from_slice(&1i16.to_le_bytes());
        v.extend_from_slice(&30000i16.to_le_bytes()); // sub_c > ins+del
        v.extend_from_slice(&40i16.to_le_bytes()); // max_d: tight-but-loose band
        v.extend_from_slice(&6u16.to_le_bytes());
        v.extend_from_slice(&6u16.to_le_bytes());
        v.extend_from_slice(b"abcdef");
        v.extend_from_slice(b"ghijkl");
        contriba_diff(&v);
        // levenshtein multibyte + invalid tail
        let mut v = vec![2u8, 0];
        v.extend_from_slice(&[0; 8]);
        v.extend_from_slice(&4u16.to_le_bytes());
        v.extend_from_slice(&2u16.to_le_bytes());
        v.extend_from_slice("é!".as_bytes());
        v.extend_from_slice(&[0xC3, 0x28]); // invalid UTF-8 pair
        contriba_diff(&v);
        // dmetaphone
        let mut v = vec![3u8];
        v.extend_from_slice(&8u16.to_le_bytes());
        v.extend_from_slice(b"Schmidt!");
        contriba_diff(&v);
        // 4-phoneme code witness (injection-sweep plant D)
        let mut v = vec![3u8];
        v.extend_from_slice(&9u16.to_le_bytes());
        v.extend_from_slice(b"Christmas");
        contriba_diff(&v);
        // SUGAR arm + short words that drive string_at past the padded tail
        for w in [&b"SUGAR"[..], b"sugarcane", b"C", b"S", b"X", b"Z", b"W"] {
            let mut v = vec![3u8];
            v.extend_from_slice(&(w.len() as u16).to_le_bytes());
            v.extend_from_slice(w);
            contriba_diff(&v);
        }
        // daitch
        let mut v = vec![4u8];
        v.extend_from_slice(&8u16.to_le_bytes());
        v.extend_from_slice(b"Holubica");
        contriba_diff(&v);
        // daitch: short word -> template-padded code (injection plant E)
        let mut v = vec![4u8];
        v.extend_from_slice(&1u16.to_le_bytes());
        v.extend_from_slice(b"B");
        contriba_diff(&v);
        // daitch: no encodable characters (NULL arm)
        let mut v = vec![4u8];
        v.extend_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(b"!!!");
        contriba_diff(&v);
        // isn input: good ISBN hard, bad check soft, weak
        for flags in [0u8, 1, 2, 3] {
            let mut v = vec![5u8, 2 /* Isbn */, flags, 17];
            v.extend_from_slice(b"978-0-393-04002-9");
            contriba_diff(&v);
            let mut v = vec![5u8, 2, flags, 17];
            v.extend_from_slice(b"978-0-393-04002-8"); // bad check digit
            contriba_diff(&v);
        }
        // isn input: magic '?' check digit + '!' suffix + garbage
        let mut v = vec![5u8, 2, 0, 17];
        v.extend_from_slice(b"978-0-393-04002-?");
        contriba_diff(&v);
        let mut v = vec![5u8, 1, 0, 14];
        v.extend_from_slice(b"9780393040029!");
        contriba_diff(&v);
        let mut v = vec![5u8, 0, 2, 5];
        v.extend_from_slice(b"hello");
        contriba_diff(&v);
        // isn output: in-range value both shorts, cast, out-of-range
        let good: u64 = 19_560_786_080_058; // 9780393040029 << 1
        for flags in [0u8, 1] {
            let mut v = vec![6u8, flags];
            v.extend_from_slice(&good.to_le_bytes());
            v.push(2); // Isbn cast
            contriba_diff(&v);
        }
        let mut v = vec![6u8, 3];
        v.extend_from_slice(&u64::MAX.to_le_bytes());
        v.push(1);
        contriba_diff(&v);
    }
}
