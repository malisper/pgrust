//! trgm_diff: dual-exec differential driver for crates/contrib/pg_trgm
//! (trgm_op.c half) against the vendored 18.3 C oracle in
//! `csrc/pg_trgm_io.c`. 100%-coverage campaign, lane p1-trgm.
//!
//! INPUT SHAPE: byte0 = selector — low nibble = arm (0..=9; 10..=15
//! reserved; 9 = the trgm_regexp arm), bits 4-5 = locale_arm (0/1/2; 3
//! reserved). The rest is payload; two-string arms split on the FIRST
//! 0xFF byte (0xFF is never valid UTF-8, so it cannot collide with the
//! domain gate below; under arm 2's raw-byte domain the split is simply
//! defined at the first 0xFF).
//!
//! DOMAIN GATE: every string payload must be valid UTF-8 — the server
//! invariant for `text` under a UTF-8 database (the C oracle's
//! pg_mblen_unbounded may over-read otherwise). Arms 7/8 (compact_trigram /
//! trgm2int / cmp_trgm) are exempt: those kernels are pure over raw bytes
//! on both sides. Strings are capped at 2048 bytes (oracle out-buffer
//! sizing; the MaxAllocSize "out of memory" guards in init/
//! enlarge_trgm_array need ~357 MB inputs — recorded as an executable
//! exception, not fuzzed).
//!
//! LOCALE ARMS (both sides pinned identically per exec):
//!   arm 0 = database ctype "C":   Rust set_database_ctype_is_c(true) +
//!           DEFAULT_LOCALE = C_LOCALE; C database_ctype_is_c=1, locale
//!           model ctype_is_c. t_isalnum = byte isalnum; tolower =
//!           asc_tolower. Multibyte chars are never word chars here.
//!   arm 1 = builtin "C.UTF-8":    Rust set_database_ctype_is_c(false) +
//!           DEFAULT_LOCALE = BUILTIN_C_UTF8_LOCALE; C model provider
//!           BUILTIN, casemap_full=false. t_isalnum multibyte path goes
//!           char2wchar/mbstowcs + iswalnum under the PROCESS LC_CTYPE
//!           (pinned to a UTF-8 locale in init_env — both sides call the
//!           same libc, so the classification is shared; the diff
//!           validates the dispatch + trigram machinery around it).
//!           tolower = unicode_strlower (pgrust unicode_case crate vs
//!           vendored src/common/unicode_case.c — a REAL differential).
//!           PLATFORM CARVE: macOS libc wctype tables differ from glibc in
//!           spots; the Linux fleet run is the record for arm 1.
//!   arm 2 = SQL_ASCII single-byte database (locale "C"): encoding pinned
//!           pg_enc 0 on both sides — pg_database_encoding_max_length()==1
//!           reaches make_trigrams' single-byte fast path, unreachable
//!           under UTF-8. ctype model = arm 0's byte model. DOMAIN = raw
//!           bytes (every byte string is valid SQL_ASCII; no mb walker is
//!           reachable — verified: find_word/make_trigrams take the
//!           1-byte-per-char rows). Interior NUL truncation parity
//!           (C strlen(lowered) vs Rust nul_pos) is IN and compared.
//!           Ground truth: live 18.3 SQL_ASCII/C database (arm2 pins).
//!
//! COMPARISON PLANES (per arm): value bytes — trigram arrays compared
//! byte-for-byte INCLUDING order (comparator-order surface); floats by
//! to_bits(); element lists (show_trgm) byte-for-byte in order — plus
//! error verdict + errcode class (no in-domain input may error on either
//! side; a C nonzero class or a Rust panic is a finding). fc-wrapper
//! plane: similarity + word_similarity/strict_word_similarity + show_trgm
//! through the registered fc_* wrappers with real LocalFcinfo (float
//! datums bit-compared against the already-C-compared core float; show fc
//! leg asserts success + element count against the factored core).
//! Thresholds are NOT read from GUC in the harness: boolean ops on both
//! sides are >= against the C initializer constants 0.3/0.6/0.5 applied to
//! the already-bit-compared float, so no separate boolean plane exists to
//! go dead.
//!
//! SKIPPED (reasons; the crate claim carves these):
//!   - set_limit / show_limit / index_strategy_get_limit: GUC plumbing
//!     carve (claim row); set_limit additionally drags float4out+GUC store.
//!   - gist/gin support (gist.rs + gin cores): index-machinery carve per
//!     the phase1-ranking.tsv cell.
//!   - trgm_regexp (regexp.rs): reserved arm 9, second half of the lane.
//!
//! INJECTION SWEEP (2026-08-01, run at driver completion; each defect
//! planted alone, `cargo test trgm_diff` seed replay observed FAILING on
//! the expected plane, then reverted — seeds-only kills, no fuzzing):
//!   i1 cmp_trgm signed -> unsigned byte cmp (product trgm.rs)
//!      KILLED: show_trgm rendered-element order + seed replay (arm 1
//!      CRC/high-bit ordering seeds).
//!   i2 make_trigrams `bytelen < 3` gate -> `< 4` (product trgm.rs)
//!      KILLED: generate_wildcard_trgm + generate arms (short-word seeds).
//!   i3 iterate_word_similarity drop `count += 1` (product trgm.rs)
//!      KILLED: word_similarity(flags=2) value plane (rust 0 vs c
//!      0.33333334, hedgehog seed).
//!   i4 wrong CRC variant in harness env wiring (bit-flipped legacy crc)
//!      KILLED: compact_trigram raw-byte arm + arm-1 CRC trigram seeds.
//!   i5 get_wildcard_part drop trailing-pad push (product trgm.rs)
//!      KILLED: generate_wildcard_trgm value plane (pct20/esc seeds).
//! ARM-9 (regexp) SWEEP (2026-08-01, same protocol, seeds-only kills):
//!   r1 PENALTIES[6] 25.0 -> 2.0 (product regexp.rs)
//!      KILLED: trigram-multiset plane (^abc$ seed: rust 3 vs C 2).
//!   r2 valid_arc_label nonblank-blank rule dropped (product regexp.rs)
//!      KILLED: trigram-multiset plane (bigclass seed: rust 5 vs C 4).
//!   r3 enter_keys prefix_contains dedup skipped (product regexp.rs)
//!      KILLED-BY-HANG: key-queue explosion — seed replay wedges >150s
//!      where the C side terminates (bounded-replay divergence).
//!   r4 expand_color_trigrams emits one extra trigram (product regexp.rs)
//!      KILLED: trigram-multiset plane ('abc': rust 2 vs C 1).
//!   r5 TrgmPackedGraph::matches treats unset check bits as set
//!      (product gin_vocab)
//!      KILLED: graph-semantics plane (round 0 all-false subset).
//!
//! Ground-truth pins (live postgres:18.3 docker, aarch64 Debian,
//! 2026-08-01): the arm-tagged unit tests at the bottom.

use std::ffi::c_int;
use std::sync::Once;

use datum::Datum;
use datum::NullableDatum;
use types_fmgr::{LocalFcinfo, PGFunction};

use pg_trgm::trgm::{
    calc_word_similarity, cnt_sml, compact_trigram, generate_trgm, generate_wildcard_trgm,
    trgm2int, trgm_contained_by, trgm_presence_map, Trgm,
};

extern "C" {
    fn pg_diff_trgm_generate(
        locale_arm: c_int,
        s: *const u8,
        len: c_int,
        out: *mut u8,
        cap: c_int,
        n: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_wildcard(
        locale_arm: c_int,
        s: *const u8,
        len: c_int,
        out: *mut u8,
        cap: c_int,
        n: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_show(
        locale_arm: c_int,
        s: *const u8,
        len: c_int,
        out: *mut u8,
        cap: c_int,
        outlen: *mut i32,
        nelems: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_similarity(
        locale_arm: c_int,
        a: *const u8,
        alen: c_int,
        b: *const u8,
        blen: c_int,
        res: *mut f32,
    ) -> c_int;
    fn pg_diff_trgm_cnt_sml_inexact(
        locale_arm: c_int,
        a: *const u8,
        alen: c_int,
        b: *const u8,
        blen: c_int,
        res: *mut f32,
    ) -> c_int;
    fn pg_diff_trgm_word_similarity(
        locale_arm: c_int,
        a: *const u8,
        alen: c_int,
        b: *const u8,
        blen: c_int,
        flags: u8,
        res: *mut f32,
    ) -> c_int;
    fn pg_diff_trgm_contained_by(
        locale_arm: c_int,
        a: *const u8,
        alen: c_int,
        b: *const u8,
        blen: c_int,
        res: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_presence_map(
        locale_arm: c_int,
        q: *const u8,
        qlen: c_int,
        k: *const u8,
        klen: c_int,
        out: *mut u8,
        cap: c_int,
        n: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_trgm2int(t: *const u8) -> u32;
    fn pg_diff_trgm_compact(s: *const u8, len: c_int, out: *mut u8);
    fn pg_diff_trgm_cmp(a: *const u8, b: *const u8, is_signed: c_int) -> c_int;
    // arm 9 (pg_trgm_regexp_io.c)
    fn pg_diff_trgm_regexp(
        pat: *const u8,
        len: c_int,
        trg_out: *mut u8,
        trg_cap: c_int,
        ntrgms: *mut i32,
        groups_out: *mut i32,
        groups_cap: c_int,
        ngroups: *mut i32,
        states_out: *mut i32,
        states_cap: c_int,
        nstates: *mut i32,
        arcs_out: *mut i32,
        arcs_cap: c_int,
        narcs: *mut i32,
    ) -> c_int;
    fn pg_diff_trgm_regexp_matches(
        pat: *const u8,
        len: c_int,
        check: *const u8,
        ncheck: c_int,
        out_match: *mut i32,
    ) -> c_int;
}

const MAX_STR: usize = 2048;

/// C GUC initializer constants (trgm_op.c lines 27-29); the boolean-op
/// planes apply them to the bit-compared float on both sides.
const WORD_SIMILARITY_THRESHOLD: f64 = 0.6f32 as f64;
const STRICT_WORD_SIMILARITY_THRESHOLD: f64 = 0.5f32 as f64;

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _seams = std::panic::catch_unwind(mbutils::init_seams);
    });
    // Thread-locals: per-exec/per-thread, never once per process.
    // (The encoding pin itself is per locale-arm: see pin_locale_arm.)
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 pin");
}

/// Thread LC_CTYPE for the arm, via uselocale(3) — the honest model of
/// PG's backend-start `setlocale(LC_CTYPE, <database ctype>)`: a C-ctype
/// database runs its backends with LC_CTYPE=C (byte isalnum is pure
/// ASCII), a C.UTF-8 database with a UTF-8 LC_CTYPE (mbstowcs/iswalnum
/// decode UTF-8). uselocale is per-thread and read by BOTH sides' libc
/// ctype calls, so the two sides can never disagree about the ctype
/// environment. C.UTF-8 exists on glibc (fleet = platform of record);
/// macOS falls back to en_US.UTF-8 (documented platform carve, header).
fn thread_ctype(locale_arm: i32) {
    use std::cell::Cell;
    thread_local! {
        static LOCS: Cell<Option<(libc::locale_t, libc::locale_t)>> = const { Cell::new(None) };
    }
    LOCS.with(|cell| {
        let (c_loc, u_loc) = cell.get().unwrap_or_else(|| {
            // SAFETY: newlocale with a base of 0 builds a fresh locale.
            let c_loc = unsafe { libc::newlocale(libc::LC_CTYPE_MASK, c"C".as_ptr(), std::ptr::null_mut()) };
            assert!(!c_loc.is_null(), "newlocale(C) failed");
            let mut u_loc = unsafe { libc::newlocale(libc::LC_CTYPE_MASK, c"C.UTF-8".as_ptr(), std::ptr::null_mut()) };
            if u_loc.is_null() {
                u_loc = unsafe { libc::newlocale(libc::LC_CTYPE_MASK, c"en_US.UTF-8".as_ptr(), std::ptr::null_mut()) };
            }
            assert!(!u_loc.is_null(), "no UTF-8 LC_CTYPE available");
            cell.set(Some((c_loc, u_loc)));
            (c_loc, u_loc)
        });
        // SAFETY: both locale_t values live for the thread's lifetime.
        let rc = unsafe { libc::uselocale(if locale_arm == 0 { c_loc } else { u_loc }) };
        assert!(!rc.is_null(), "uselocale failed");
    });
}

fn pin_locale_arm(locale_arm: i32) {
    // arm 2 keeps the C-ctype thread locale (byte isalnum); arms 0/2 differ
    // only in database encoding (UTF-8 vs SQL_ASCII single-byte).
    thread_ctype(if locale_arm == 2 { 0 } else { locale_arm });
    let enc = if locale_arm == 2 { wchar::PG_SQL_ASCII } else { wchar::PG_UTF8 };
    mbutils::SetDatabaseEncoding(enc).expect("encoding pin");
    // ENCODING PIN ASSERT (harness law): both sides must agree what a byte
    // sequence means. C side is pinned inside every pg_diff_trgm_* entry.
    assert_eq!(
        mbutils::pg_database_encoding_max_length(),
        if locale_arm == 2 { 1 } else { 4 },
        "encoding pin"
    );
    if locale_arm == 1 {
        pg_locale::set_database_ctype_is_c(false);
        pg_locale::set_default_locale_builtin_utf8_for_tests();
    } else {
        pg_locale::set_database_ctype_is_c(true);
        pg_locale::set_default_locale_c_for_tests();
    }
}

fn flat(v: &[Trgm]) -> Vec<u8> {
    v.iter().flat_map(|t| t.iter().copied()).collect()
}

fn crc(b: &[u8]) -> u32 {
    crc32c::legacy_crc32_lexeme(b)
}

/// C-side generate_trgm (locale-armed); error-verdict plane: no in-domain
/// input may error on either side.
fn c_generate(locale_arm: i32, s: &[u8]) -> Vec<u8> {
    let cap = 3 * (s.len() + 8);
    let mut out = vec![0u8; cap];
    let mut n: i32 = 0;
    let rc = unsafe {
        pg_diff_trgm_generate(
            locale_arm,
            s.as_ptr(),
            s.len() as c_int,
            out.as_mut_ptr(),
            cap as c_int,
            &mut n,
        )
    };
    assert_eq!(rc, 0, "C generate_trgm errored (class {rc}) in-domain");
    out.truncate(3 * n as usize);
    out
}

fn c_wildcard(locale_arm: i32, s: &[u8]) -> Vec<u8> {
    let cap = 3 * (s.len() + 8);
    let mut out = vec![0u8; cap];
    let mut n: i32 = 0;
    let rc = unsafe {
        pg_diff_trgm_wildcard(
            locale_arm,
            s.as_ptr(),
            s.len() as c_int,
            out.as_mut_ptr(),
            cap as c_int,
            &mut n,
        )
    };
    assert_eq!(
        rc, 0,
        "C generate_wildcard_trgm errored (class {rc}) in-domain"
    );
    out.truncate(3 * n as usize);
    out
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (contriba_diff pattern).
// ---------------------------------------------------------------------------

/// fc-plane gate (contriba_diff precedent, FUZZ-BINARY ONLY): the wrapper
/// path reads pg_trgm.* thresholds through the GUC store, whose bootstrap
/// installs process-global seams that sibling lanes' tests install
/// unguarded — so in the shared `cargo test` binary the fc plane degrades
/// to skipped, and the dedicated trgm_diff fuzz binary (the binary the
/// coverage capture replays) runs it fully.
fn fc_ready() -> bool {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use std::panic::catch_unwind;
        // First-wins across lanes sharing one test binary.
        let _trgm = catch_unwind(pg_trgm::init_seams);
        if cfg!(fuzzing) {
            // contriba_diff guc_env_bootstrap, verbatim environment set:
            // fc wrappers reach GUC reads + array construction, which walk
            // these process-global seams.
            let _g1 = catch_unwind(guc_tables::init_seams);
            let _g2 = catch_unwind(elog::init_seams);
            let _g3 = catch_unwind(guc::init_seams);
            let _g4 = catch_unwind(|| xact_seams::is_in_parallel_mode::set(|| false));
            // the SHIPPED bool parser (computation stays real; seam = wiring)
            let _g4b = catch_unwind(|| scalar_seams::parse_bool::set(adt_bool::parse_bool));
            let _g5 = catch_unwind(|| aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true)));
            let _g6 = catch_unwind(|| superuser_seams::superuser::set(|| Ok(true)));
            if !guc::store::is_initialized() {
                let _g7 = catch_unwind(guc::store::initialize_guc_options);
            }
        }
    });
    guc::store::is_initialized()
}

fn lookup(name: &str) -> PGFunction {
    dfmgr::load_external_function("pg_trgm", name, true)
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

/// fc plane for the float-returning wrappers: wrapper result must
/// bit-equal the already-C-compared core float.
fn fc_float_plane(name: &str, a: &[u8], b: &[u8], expect: f32) {
    if !fc_ready() {
        return;
    }
    let ctx = mcx::MemoryContext::new("trgm_diff fc");
    let (ia, ib) = (text_image(a), text_image(b));
    let d = fc_call(
        lookup(name),
        ctx.mcx(),
        [
            Datum::from_usize(ia.as_ptr() as usize),
            Datum::from_usize(ib.as_ptr() as usize),
        ],
    )
    .unwrap_or_else(|e| panic!("fc {name} errored where core succeeded: {e:?}"));
    let got = d.as_f32();
    assert_eq!(
        got.to_bits(),
        expect.to_bits(),
        "fc {name} != core: fc {got} core {expect}"
    );
}

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------

fn arm_generate(locale_arm: i32, s: &[u8]) {
    let env = pg_trgm::harness_env();
    let r = flat(&generate_trgm(s, &env, &crc));
    let c = c_generate(locale_arm, s);
    assert_eq!(r, c, "generate_trgm trigram array (order included) diverged");
}

fn arm_show(locale_arm: i32, s: &[u8]) {
    let r: Vec<u8> = pg_trgm::show_trgm_elements(s)
        .into_iter()
        .flat_map(|mut e| {
            e.push(b'\n');
            e
        })
        .collect();
    let cap = 16 * (s.len() + 8);
    let mut out = vec![0u8; cap];
    let (mut outlen, mut nelems) = (0i32, 0i32);
    let rc = unsafe {
        pg_diff_trgm_show(
            locale_arm,
            s.as_ptr(),
            s.len() as c_int,
            out.as_mut_ptr(),
            cap as c_int,
            &mut outlen,
            &mut nelems,
        )
    };
    assert_eq!(rc, 0, "C show_trgm errored (class {rc}) in-domain");
    out.truncate(outlen as usize);
    assert_eq!(r, out, "show_trgm rendered elements diverged");

    // fc plane: the wrapper succeeds and agrees on the element count
    // (ArrayType image: ndim at offset 4; dims[0] at offset 16 for 1-D).
    if !fc_ready() {
        return;
    }
    let ctx = mcx::MemoryContext::new("trgm_diff fc show");
    let img = text_image(s);
    let d = fc_call(
        lookup("show_trgm"),
        ctx.mcx(),
        [Datum::from_usize(img.as_ptr() as usize)],
    )
    .expect("fc show_trgm errored where core succeeded");
    let p = d.as_usize() as *const u8;
    // SAFETY: wrapper returns a live 4B-header ArrayType in the arming mcx.
    let word = |off: usize| unsafe {
        i32::from_le_bytes(std::slice::from_raw_parts(p.add(off), 4).try_into().unwrap())
    };
    let ndim = word(4);
    if nelems == 0 {
        assert_eq!(ndim, 0, "fc show_trgm: empty result must be 0-D");
    } else {
        assert_eq!(ndim, 1, "fc show_trgm: 1-D array expected");
        assert_eq!(word(16), nelems, "fc show_trgm element count diverged");
    }
}

fn arm_similarity(locale_arm: i32, a: &[u8], b: &[u8]) {
    let env = pg_trgm::harness_env();
    let t1 = generate_trgm(a, &env, &crc);
    let t2 = generate_trgm(b, &env, &crc);
    let r = cnt_sml(&t1, &t2, false);
    let mut c: f32 = 0.0;
    let rc = unsafe {
        pg_diff_trgm_similarity(
            locale_arm,
            a.as_ptr(),
            a.len() as c_int,
            b.as_ptr(),
            b.len() as c_int,
            &mut c,
        )
    };
    assert_eq!(rc, 0, "C similarity errored (class {rc}) in-domain");
    assert_eq!(r.to_bits(), c.to_bits(), "similarity diverged: rust {r} c {c}");
    fc_float_plane("similarity", a, b, r);
}

fn arm_cnt_sml_inexact(locale_arm: i32, a: &[u8], b: &[u8]) {
    let env = pg_trgm::harness_env();
    let t1 = generate_trgm(a, &env, &crc);
    let t2 = generate_trgm(b, &env, &crc);
    let r = cnt_sml(&t1, &t2, true);
    let mut c: f32 = 0.0;
    let rc = unsafe {
        pg_diff_trgm_cnt_sml_inexact(
            locale_arm,
            a.as_ptr(),
            a.len() as c_int,
            b.as_ptr(),
            b.len() as c_int,
            &mut c,
        )
    };
    assert_eq!(rc, 0, "C cnt_sml(inexact) errored (class {rc}) in-domain");
    assert_eq!(
        r.to_bits(),
        c.to_bits(),
        "cnt_sml inexact diverged: rust {r} c {c}"
    );
}

fn arm_word_similarity(locale_arm: i32, flags: u8, a: &[u8], b: &[u8]) {
    let flags = flags & 0x03;
    let env = pg_trgm::harness_env();
    let r = calc_word_similarity(
        a,
        b,
        flags,
        &env,
        &crc,
        WORD_SIMILARITY_THRESHOLD,
        STRICT_WORD_SIMILARITY_THRESHOLD,
    );
    let mut c: f32 = 0.0;
    let rc = unsafe {
        pg_diff_trgm_word_similarity(
            locale_arm,
            a.as_ptr(),
            a.len() as c_int,
            b.as_ptr(),
            b.len() as c_int,
            flags,
            &mut c,
        )
    };
    assert_eq!(rc, 0, "C calc_word_similarity errored (class {rc}) in-domain");
    assert_eq!(
        r.to_bits(),
        c.to_bits(),
        "word_similarity(flags={flags}) diverged: rust {r} c {c}"
    );
    // fc plane exercises the wrapper spellings of the same flag combos
    // (CHECK_ONLY spellings return booleans derived from the float already
    // bit-compared above).
    match flags {
        0 => fc_float_plane("word_similarity", a, b, r),
        f if f == pg_trgm::trgm::WORD_SIMILARITY_STRICT => {
            fc_float_plane("strict_word_similarity", a, b, r)
        }
        _ => {}
    }
}

fn arm_wildcard(locale_arm: i32, s: &[u8]) {
    let env = pg_trgm::harness_env();
    let r = flat(&generate_wildcard_trgm(s, &env, &crc));
    let c = c_wildcard(locale_arm, s);
    assert_eq!(r, c, "generate_wildcard_trgm diverged");
}

fn arm_contain_presence(locale_arm: i32, a: &[u8], b: &[u8]) {
    let env = pg_trgm::harness_env();
    let t1 = generate_trgm(a, &env, &crc);
    let t2 = generate_trgm(b, &env, &crc);

    let r = trgm_contained_by(&t1, &t2);
    let mut c: i32 = -1;
    let rc = unsafe {
        pg_diff_trgm_contained_by(
            locale_arm,
            a.as_ptr(),
            a.len() as c_int,
            b.as_ptr(),
            b.len() as c_int,
            &mut c,
        )
    };
    assert_eq!(rc, 0);
    assert_eq!(r, c == 1, "trgm_contained_by diverged");

    let rmap = trgm_presence_map(&t1, &t2);
    let cap = t1.len().max(1);
    let mut out = vec![0u8; cap];
    let mut n: i32 = 0;
    let rc = unsafe {
        pg_diff_trgm_presence_map(
            locale_arm,
            a.as_ptr(),
            a.len() as c_int,
            b.as_ptr(),
            b.len() as c_int,
            out.as_mut_ptr(),
            cap as c_int,
            &mut n,
        )
    };
    assert_eq!(rc, 0);
    out.truncate(n as usize);
    let rbytes: Vec<u8> = rmap.iter().map(|&x| x as u8).collect();
    assert_eq!(rbytes, out, "trgm_presence_map diverged");
}

fn arm_compact(payload: &[u8]) {
    // Raw-byte kernels: no UTF-8 gate (see header). len 1..=12.
    if payload.is_empty() || payload.len() > 12 {
        return;
    }
    let r = compact_trigram(payload, &crc);
    let mut c = [0u8; 3];
    unsafe { pg_diff_trgm_compact(payload.as_ptr(), payload.len() as c_int, c.as_mut_ptr()) };
    assert_eq!(r, c, "compact_trigram diverged");
    if payload.len() >= 3 {
        let t: Trgm = [payload[0], payload[1], payload[2]];
        let ri = trgm2int(&t);
        let ci = unsafe { pg_diff_trgm_trgm2int(t.as_ptr()) };
        assert_eq!(ri, ci, "trgm2int diverged");
    }
}

fn arm_cmp(payload: &[u8]) {
    if payload.len() < 6 {
        return;
    }
    let a: Trgm = [payload[0], payload[1], payload[2]];
    let b: Trgm = [payload[3], payload[4], payload[5]];
    let r = match pg_trgm::trgm::cmp_trgm(&a, &b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    let c = unsafe { pg_diff_trgm_cmp(a.as_ptr(), b.as_ptr(), 1) };
    assert_eq!(r, c.signum(), "cmp_trgm vs CMPTRGM_SIGNED diverged");
}

// ---------------------------------------------------------------------------
// Arm 9: trgm_regexp.c (createTrgmNFA / trigramsMatchGraph), phase B.
//
// LOCALE: arm 9 always pins locale arm 0 (database ctype "C") and collation
// C (950) on BOTH sides — the C oracle's regex engine is the regexfam build
// with the C-collation strategy pin; the builtin-C.UTF-8 colormap strategy
// is a NAMED RESIDUAL of this increment (oracle TU header).
//
// PLANES (tiered; the C-vs-Rust ORDER couplings are pre-audited, see the
// lane notes: dynahash hash_seq iteration order + PG-qsort penalty ties +
// memcmp-vs-numeric ColorTrgm order make raw ARRAY ORDER differ even for
// semantically identical extractions):
//   a. verdict (HARD): error(class) / NULL-fallback / success must match.
//      Rust maps: Err => error; Ok(None) => fallback; Ok(Some) => success.
//      C maps: rc>0 error class; rc -1 fallback; rc 0 success.
//      EXCEPTION (ratified, regex_diff precedent): one-sided "regular
//      expression is too complex" — the stack-band carve; both sides run
//      the byte-based stack guard at the real-server 2048kB budget (see
//      arm_stack_guard), but trip points are per-frame-size functions and
//      cannot coincide across compilers.
//   b. trigram MULTISET (HARD): both sides' trigram arrays sorted under one
//      harness comparator, compared byte-for-byte.
//   c. graph semantics (HARD): 32 deterministic pseudo-random subsets of
//      the common trigram VALUE set (FNV-1a(pattern) seeded xorshift64,
//      plus all-true / all-false); each subset is translated to each
//      side's own check-vector layout by value membership and evaluated
//      through the REAL evaluators (C trigramsMatchGraph vs Rust
//      TrgmPackedGraph::matches). Duplicated trigram values mark all their
//      positions on both sides, so membership semantics are well-defined.
//   d. order witness (SOFT): counters, printed by the unit tests — the
//      evidence base for the pending order ruling; never fails the target.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering as AtOrd};

pub static REGEXP_ORDER_EQ: AtomicU64 = AtomicU64::new(0);
pub static REGEXP_ORDER_DIFF: AtomicU64 = AtomicU64::new(0);
pub static REGEXP_SUCCESS: AtomicU64 = AtomicU64::new(0);
pub static REGEXP_FALLBACK: AtomicU64 = AtomicU64::new(0);
pub static REGEXP_ERR: AtomicU64 = AtomicU64::new(0);
/// One-sided "regular expression is too complex" (the ratified stack-band
/// carve — see arm_regexp): telemetry, never a failure.
pub static REGEXP_ETOOBIG_CARVE: AtomicU64 = AtomicU64::new(0);

// C error classes on the arm-9 channel (pg_trgm_regexp_io.c header).
const C_RX_ERR_INVALID_RE: i32 = 4;
const C_RX_ERR_RE_TOO_COMPLEX: i32 = 5;
/// pg_regerror's REG_ETOOBIG text; the Rust side surfaces it inside
/// "invalid regular expression: {}" (create_trgm_nfa), so match by suffix.
const RE_TOO_COMPLEX_MSG: &str = "regular expression is too complex";

const REGEXP_MAX_PAT: usize = 128; // regexfam recursion-soundness cap
const RX_TRG_CAP: usize = 3 * 1024;
const RX_GROUPS_CAP: usize = 1024;
const RX_STATES_CAP: usize = 1024; // (off,len) pairs for <=130 states
const RX_ARCS_CAP: usize = 8192; // (target,ctrgm) pairs for <=1024 arcs

struct CRegexpOut {
    trg: Vec<u8>,
    groups: Vec<i32>,
    states: Vec<(i32, i32)>,
    arcs: Vec<(i32, i32)>,
}

/// rc semantics: Ok(Some) success, Ok(None) fallback, Err(class) error.
fn c_regexp(pat: &[u8]) -> Result<Option<CRegexpOut>, i32> {
    let mut trg = vec![0u8; RX_TRG_CAP];
    let mut groups = vec![0i32; RX_GROUPS_CAP];
    let mut states = vec![0i32; 2 * RX_STATES_CAP];
    let mut arcs = vec![0i32; 2 * RX_ARCS_CAP];
    let (mut ntrgms, mut ngroups, mut nstates, mut narcs) = (0i32, 0i32, 0i32, 0i32);
    let rc = unsafe {
        pg_diff_trgm_regexp(
            pat.as_ptr(),
            pat.len() as c_int,
            trg.as_mut_ptr(),
            RX_TRG_CAP as c_int,
            &mut ntrgms,
            groups.as_mut_ptr(),
            RX_GROUPS_CAP as c_int,
            &mut ngroups,
            states.as_mut_ptr(),
            (2 * RX_STATES_CAP) as c_int,
            &mut nstates,
            arcs.as_mut_ptr(),
            (2 * RX_ARCS_CAP) as c_int,
            &mut narcs,
        )
    };
    match rc {
        0 => {
            trg.truncate(3 * ntrgms as usize);
            groups.truncate(ngroups as usize);
            let states = (0..nstates as usize)
                .map(|i| (states[2 * i], states[2 * i + 1]))
                .collect();
            let arcs = (0..narcs as usize).map(|i| (arcs[2 * i], arcs[2 * i + 1])).collect();
            Ok(Some(CRegexpOut { trg, groups, states, arcs }))
        }
        -1 => Ok(None),
        cls => Err(cls),
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// arm 9 serialization: the vendored dynahash keeps its per-BACKEND
/// hash_seq_search scan registry in process-global statics (verbatim;
/// real PG is one backend per process). The fuzz runtime is
/// single-threaded, but `cargo test` runs tests in parallel threads —
/// concurrent arm-9 execs would race the registry and fabricate
/// "too many active hash_seq_search scans" oracle errors.
static REGEXP_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Arm this thread's Rust-side stack guard at the C oracle's pinned budget
/// (pg_regexfam.c: 2048kB, the real-server default; regex_diff::init
/// precedent). Unarmed (base==0), stack_is_too_deep() short-circuits false
/// and the C-parity guards in regex_core's recursive walks (regc_nfa.c's
/// duptraverse et al.) are INERT — the 2026-08-03 fleet CONFIRM
/// stack-overflow class (156 ASan crash artifacts, unbounded duptraverse
/// self-recursion on quantified-alternation patterns; job
/// pgrust-fuzz-campaign-1785794601-6bab-5889 @ main 5b707bdb597). The real
/// backend arms every statement-executing thread at spawn
/// (launch_backend's set_stack_base); this reproduces that contract here.
/// The C oracle side is anchored symmetrically inside every
/// pg_diff_trgm_regexp* entry (trgmrx_enter -> pg_diff_regex_stack_arm).
fn arm_stack_guard() {
    std::thread_local! {
        static ARMED: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    }
    ARMED.with(|armed| {
        if !armed.get() {
            stack_depth::set_stack_base();
            stack_depth::set_max_stack_depth(2048);
            stack_depth::assign_max_stack_depth(2048);
            armed.set(true);
        }
    });
}

fn arm_regexp(payload: &[u8]) {
    if payload.len() > REGEXP_MAX_PAT || !utf8_ok(payload) {
        return;
    }
    let _serial = REGEXP_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    arm_stack_guard();
    pin_locale_arm(0); // arm 9 is locale-arm-0 only (header)
    let env = pg_trgm::harness_env();

    let c = c_regexp(payload);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pg_trgm::regexp::create_trgm_nfa(
            payload,
            types_core::C_COLLATION_OID,
            &env,
            &crc,
        )
    }));

    // Plane a: verdict. A Rust panic that is not one of OUR assert panics
    // is a finding — surface it (re-panic) rather than classifying it.
    let r = match r {
        Ok(v) => v,
        Err(e) => std::panic::resume_unwind(e),
    };

    // RATIFIED stack-band carve (regex_diff is_etoobig precedent): "regular
    // expression is too complex" is the byte-based stack guard's verdict;
    // its trip point is a function of per-frame sizes, so the rustc and
    // clang builds of the same recursive walk cannot trip at identical
    // inputs even at the identical 2048kB budget. Tolerate ONLY the
    // asymmetric too-complex band; any other one-sided verdict still
    // panics below, and a matching (both-sided) too-complex flows through
    // the ordinary both-error arm.
    let r_etoobig = matches!(&r, Err(re) if re.message().ends_with(RE_TOO_COMPLEX_MSG));
    let c_etoobig = matches!(&c, Err(cls) if *cls == C_RX_ERR_RE_TOO_COMPLEX);
    if r_etoobig != c_etoobig {
        REGEXP_ETOOBIG_CARVE.fetch_add(1, AtOrd::Relaxed);
        return;
    }

    match (&r, &c) {
        (Err(re), Err(ccls)) => {
            // Both errored. C class 4 = ERRCODE_INVALID_REGULAR_EXPRESSION
            // (the only ereport in the vendored file); class 5 = its
            // REG_ETOOBIG subset (both sides too-complex — the carve above
            // guarantees agreement here). The Rust side's create_trgm_nfa
            // maps every compile failure to the same sqlstate.
            assert!(
                *ccls == C_RX_ERR_INVALID_RE || *ccls == C_RX_ERR_RE_TOO_COMPLEX,
                "unexpected C error class {ccls} pat={payload:02x?}"
            );
            assert_eq!(
                re.sqlstate(),
                types_error::ERRCODE_INVALID_REGULAR_EXPRESSION,
                "rust regexp error is not invalid-regular-expression: {re:?} pat={payload:02x?}"
            );
            REGEXP_ERR.fetch_add(1, AtOrd::Relaxed);
            return;
        }
        (Err(re), _) => panic!(
            "verdict diverged: rust Err({re:?}) vs C {:?} pat={payload:02x?}",
            c.as_ref().map(|o| o.is_some())
        ),
        (_, Err(ccls)) => panic!(
            "verdict diverged: rust {:?} vs C error class {ccls} pat={payload:02x?}",
            r.as_ref().map(|o| o.is_some())
        ),
        _ => {}
    }
    let r = r.unwrap();
    let c = c.unwrap();

    // NULL-fallback plane. Rust Ok(Some((trg, _))) with EMPTY trg is the
    // lib.rs fallback condition; C returns NULL. Fold both to "fallback".
    let r_success = r.as_ref().is_some_and(|(trg, _)| !trg.is_empty());
    let c_success = c.as_ref().is_some_and(|o| !o.trg.is_empty());
    assert_eq!(
        r_success, c_success,
        "fallback verdict diverged (rust {:?} vs C {:?}) pat={payload:02x?}",
        r.as_ref().map(|(t, _)| t.len()),
        c.as_ref().map(|o| o.trg.len() / 3)
    );
    if !r_success {
        REGEXP_FALLBACK.fetch_add(1, AtOrd::Relaxed);
        return;
    }
    REGEXP_SUCCESS.fetch_add(1, AtOrd::Relaxed);
    let (rtrg, mut rgraph) = r.unwrap();
    let cout = c.unwrap();

    // Plane b: trigram array, ORDER INCLUDED — HARD since the order-exact
    // ruling (Michael 2026-08-01): regexp.rs now reproduces C's memcmp ctrgm
    // ordering and pg_qsort penalty-tie eviction order, so the returned
    // array must be byte-identical, not merely multiset-equal. The counters
    // stay as cheap telemetry (diff>0 now means a REAL divergence was about
    // to be reported by the assert below).
    let rflat = flat(&rtrg);
    if rflat == cout.trg {
        REGEXP_ORDER_EQ.fetch_add(1, AtOrd::Relaxed);
    } else {
        REGEXP_ORDER_DIFF.fetch_add(1, AtOrd::Relaxed);
    }
    assert_eq!(
        rflat, cout.trg,
        "trigram ARRAY (order included) diverged pat={payload:02x?}"
    );
    let mut rsorted: Vec<Trgm> = rtrg.clone();
    rsorted.sort_unstable();
    let mut csorted: Vec<Trgm> = cout
        .trg
        .chunks_exact(3)
        .map(|ch| [ch[0], ch[1], ch[2]])
        .collect();
    csorted.sort_unstable();
    assert_eq!(
        rsorted, csorted,
        "trigram MULTISET diverged (rust {} vs C {} trigrams) pat={payload:02x?}",
        rtrg.len(),
        cout.trg.len() / 3
    );

    // Plane c: graph semantics over shared value subsets.
    let ctrg: Vec<Trgm> = cout.trg.chunks_exact(3).map(|ch| [ch[0], ch[1], ch[2]]).collect();
    let mut values: Vec<Trgm> = rsorted.clone();
    values.dedup();
    let mut seed = fnv1a64(payload) | 1;
    for round in 0..32 {
        let member = |t: &Trgm, sel: &dyn Fn(usize) -> bool| -> bool {
            match values.binary_search(t) {
                Ok(i) => sel(i),
                Err(_) => unreachable!("trigram not in value set"),
            }
        };
        // rounds 0/1 = all-false / all-true; the rest pseudo-random
        let bits: Vec<bool> = match round {
            0 => vec![false; values.len()],
            1 => vec![true; values.len()],
            _ => values
                .iter()
                .map(|_| xorshift64(&mut seed) & 1 == 1)
                .collect(),
        };
        let sel = |i: usize| bits[i];
        let rcheck: Vec<bool> = rtrg.iter().map(|t| member(t, &sel)).collect();
        let ccheck: Vec<u8> = ctrg.iter().map(|t| member(t, &sel) as u8).collect();
        let rmatch = rgraph.matches(&rcheck);
        let mut cmatch: i32 = -1;
        let rc = unsafe {
            pg_diff_trgm_regexp_matches(
                payload.as_ptr(),
                payload.len() as c_int,
                ccheck.as_ptr(),
                ccheck.len() as c_int,
                &mut cmatch,
            )
        };
        assert_eq!(rc, 0, "C re-extraction changed verdict (rc {rc}) pat={payload:02x?}");
        assert_eq!(
            rmatch,
            cmatch == 1,
            "graph semantics diverged (round {round}, subset {bits:?}) pat={payload:02x?}"
        );
    }
}

fn split_two(payload: &[u8]) -> (&[u8], &[u8]) {
    match payload.iter().position(|&b| b == 0xFF) {
        Some(i) => (&payload[..i], &payload[i + 1..]),
        None => (payload, &[][..]),
    }
}

fn utf8_ok(s: &[u8]) -> bool {
    s.len() <= MAX_STR && std::str::from_utf8(s).is_ok()
}

pub fn trgm_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics);
    // EVERY pg_diff_trgm_* entry is holder-checked via its per-exec
    // wfam_x_set_db_encoding pin, so the fuzz TARGET dies vacuous without
    // this frame (trgmrx arm-9 CONFIRM, task #144 addendum).
    let _oracle = crate::oracle_serial();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let arm = sel & 0x0F;
    let locale_arm = i32::from(sel >> 4 & 3);
    if locale_arm == 3 {
        return; // reserved
    }
    init_env();
    pin_locale_arm(locale_arm);

    match arm {
        0 | 1 | 4 => {
            if locale_arm != 2 && !utf8_ok(payload) {
                return;
            }
            if payload.len() > MAX_STR {
                return;
            }
            match arm {
                0 => arm_generate(locale_arm, payload),
                1 => arm_show(locale_arm, payload),
                _ => arm_wildcard(locale_arm, payload),
            }
        }
        2 | 5 | 6 => {
            let (a, b) = split_two(payload);
            if locale_arm != 2 && (!utf8_ok(a) || !utf8_ok(b)) {
                return;
            }
            if a.len() > MAX_STR || b.len() > MAX_STR {
                return;
            }
            match arm {
                2 => arm_similarity(locale_arm, a, b),
                5 => arm_contain_presence(locale_arm, a, b),
                _ => arm_cnt_sml_inexact(locale_arm, a, b),
            }
        }
        3 => {
            let Some((&flags, rest)) = payload.split_first() else {
                return;
            };
            let (a, b) = split_two(rest);
            if locale_arm != 2 && (!utf8_ok(a) || !utf8_ok(b)) {
                return;
            }
            if a.len() > MAX_STR || b.len() > MAX_STR {
                return;
            }
            arm_word_similarity(locale_arm, flags, a, b);
        }
        7 => arm_compact(payload),
        8 => arm_cmp(payload),
        9 => arm_regexp(payload),
        _ => {} // 10..=15 reserved
    }
}

// ---------------------------------------------------------------------------
// Exhaustive rails (fleet-run; #[ignore] locally except a truncated smoke).
// ---------------------------------------------------------------------------

/// All valid-UTF-8 byte strings of length 0..=maxlen (maxlen <= 3), both
/// locale arms, through the generate_trgm plane. Returns the number of
/// in-domain strings visited; callers MUST assert it equals
/// `utf8_count_upto(maxlen)` (a silently-short loop fails loudly).
pub fn exhaustive_short_generate_impl(maxlen: usize) -> u64 {
    init_env();
    let mut visited: u64 = 0;
    // length 0
    for arm in 0..=1 {
        pin_locale_arm(arm);
        arm_generate(arm, b"");
    }
    visited += 1;
    for len in 1..=maxlen {
        let mut idx = vec![0u32; len];
        'odo: loop {
            let buf: Vec<u8> = idx.iter().map(|&v| v as u8).collect();
            if std::str::from_utf8(&buf).is_ok() {
                visited += 1;
                for arm in 0..=1 {
                    pin_locale_arm(arm);
                    arm_generate(arm, &buf);
                }
            }
            let mut i = len;
            while i > 0 {
                idx[i - 1] += 1;
                if idx[i - 1] < 256 {
                    continue 'odo;
                }
                idx[i - 1] = 0;
                i -= 1;
            }
            break;
        }
    }
    visited
}

/// Closed-form count of valid-UTF-8 strings of length <= n (n <= 3):
/// codepoint encodings per length: 1-byte 128, 2-byte 30*64 = 1920,
/// 3-byte E0(32*64) + E1-EC(12*64*64) + ED(32*64) + EE-EF(2*64*64) = 61440.
pub fn utf8_count_upto(n: usize) -> u64 {
    let (c1, c2, c3) = (128u64, 1920u64, 61440u64);
    let per_len = |len: usize| -> u64 {
        match len {
            0 => 1,
            1 => c1,
            2 => c1 * c1 + c2,
            3 => c1 * c1 * c1 + 2 * c1 * c2 + c3,
            _ => unreachable!("rail is defined for n <= 3"),
        }
    };
    (0..=n).map(per_len).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wsim(arm: i32, a: &[u8], b: &[u8], flags: u8) -> f32 {
        init_env();
        pin_locale_arm(arm);
        let env = pg_trgm::harness_env();
        let r = calc_word_similarity(
            a,
            b,
            flags,
            &env,
            &crc,
            WORD_SIMILARITY_THRESHOLD,
            STRICT_WORD_SIMILARITY_THRESHOLD,
        );
        // the differential plane must agree while we're here
        arm_word_similarity(arm, flags, a, b);
        r
    }

    fn sim(arm: i32, a: &[u8], b: &[u8]) -> f32 {
        init_env();
        pin_locale_arm(arm);
        let env = pg_trgm::harness_env();
        let (t1, t2) = (generate_trgm(a, &env, &crc), generate_trgm(b, &env, &crc));
        arm_similarity(arm, a, b);
        cnt_sml(&t1, &t2, false)
    }

    fn show(arm: i32, s: &[u8]) -> Vec<String> {
        init_env();
        pin_locale_arm(arm);
        arm_show(arm, s);
        pg_trgm::show_trgm_elements(s)
            .into_iter()
            .map(|e| String::from_utf8_lossy(&e).into_owned())
            .collect()
    }

    /// Bit pattern of a PG float4-out literal (float4 shortest-repr
    /// round-trips exactly through Rust's f32 parser).
    fn f4(lit: &str) -> u32 {
        lit.parse::<f32>().unwrap().to_bits()
    }

    // ---- live postgres:18.3 pins, pure-ASCII: both DBs agree ----
    #[test]
    fn pins_ascii_both_arms() {
        let _serial = crate::c_oracle_serial();
        for arm in 0..=1 {
            assert_eq!(show(arm, b"a b c"), ["  a", "  b", "  c", " a ", " b ", " c "]);
            assert_eq!(
                show(arm, br"a\b%c_d"),
                ["  a", "  b", "  c", "  d", " a ", " b ", " c ", " d "]
            );
            assert_eq!(show(arm, &b"ab".repeat(5)), ["  a", " ab", "ab ", "aba", "bab"]);
            assert_eq!(sim(arm, b"", b"").to_bits(), 0.0f32.to_bits());
            assert_eq!(sim(arm, b"a", b"a").to_bits(), f4("1"));
            assert_eq!(sim(arm, b"abc", b"abc").to_bits(), f4("1"));
            assert_eq!(
                sim(arm, b"qwertyu0988", b"qwertyu0987").to_bits(),
                f4("0.71428573")
            );
            assert_eq!(sim(arm, &b"xyz".repeat(30), b"xyz").to_bits(), f4("0.6666667"));
            assert_eq!(wsim(arm, b"Sunday", b"Saturday", 0).to_bits(), f4("0.2857143"));
            assert_eq!(wsim(arm, b"word", b"two words", 2).to_bits(), f4("0.5714286"));
            assert_eq!(wsim(arm, b"eq", b"postgres_fdw", 0).to_bits(), f4("0"));
            assert_eq!(wsim(arm, b"", b"abc", 0).to_bits(), f4("0"));
            assert_eq!(wsim(arm, b"ab", b"ab cd", 2).to_bits(), f4("1"));
            assert_eq!(wsim(arm, b"Kabankala", b"Waikala", 2).to_bits(), f4("0.2"));
            assert_eq!(
                wsim(arm, b"Kabankala", b"Kabankala, Niger", 0).to_bits(),
                f4("1")
            );
        }
    }

    // ---- C-locale-database pins (arm 0) ----
    #[test]
    fn pins_c_locale_arm0() {
        let _serial = crate::c_oracle_serial();
        assert_eq!(show(0, "café".as_bytes()), ["  c", " ca", "af ", "caf"]);
        assert_eq!(sim(0, "café".as_bytes(), b"cafe").to_bits(), f4("0.5"));
        // Cyrillic: no word chars at all under ctype C
        assert_eq!(show(0, "Ация тест".as_bytes()), Vec::<String>::new());
    }

    // ---- builtin C.UTF-8 pins (arm 1) ----
    #[test]
    fn pins_builtin_utf8_arm1() {
        let _serial = crate::c_oracle_serial();
        assert_eq!(
            show(1, "café".as_bytes()),
            ["0xef5960", "  c", " ca", "0x544980", "caf"]
        );
        assert_eq!(
            show(1, "Ация".as_bytes()),
            ["0xaeccca", "0xd2f34a", "0x1c5129", "0x1faaab", "0x27efbc"]
        );
        assert_eq!(
            show(1, "мир123".as_bytes()),
            ["0x99a3b6", "0xc7e732", "0xd4fb88", "0x18ee68", "123", "23 ", "0x34e61d"]
        );
        assert_eq!(
            show(1, "aЯb".as_bytes()),
            ["0xe017dd", "0x19d3e4", "  a", "0x7e8cca"]
        );
        assert_eq!(show(1, "€a".as_bytes()), ["  a", " a "]);
        assert_eq!(sim(1, "Ация".as_bytes(), "ация".as_bytes()).to_bits(), f4("1"));
        assert_eq!(
            sim(1, "привет".as_bytes(), "превед".as_bytes()).to_bits(),
            f4("0.16666667")
        );
        assert_eq!(
            wsim(1, "привет".as_bytes(), "привет мир".as_bytes(), 0).to_bits(),
            f4("1")
        );
        assert_eq!(
            wsim(1, "слон".as_bytes(), "сложность".as_bytes(), 0).to_bits(),
            f4("0.6")
        );
        assert_eq!(
            wsim(1, "ёж".as_bytes(), "ёжик в тумане".as_bytes(), 2).to_bits(),
            f4("0.33333334")
        );
    }

    #[test]
    fn wildcard_pins() {
        let _serial = crate::c_oracle_serial();
        init_env();
        for arm in 0..=1 {
            pin_locale_arm(arm);
            for p in [
                &b"20%"[..],
                br"a%bcd%",
                br"\%a",
                br"abc\",
                b"%",
                b"_",
                br"%abc_def%",
                br"\a\b",
            ] {
                arm_wildcard(arm, p);
            }
            let env = pg_trgm::harness_env();
            let t = generate_wildcard_trgm(b"20%", &env, &crc);
            let shown: Vec<String> =
                t.iter().map(|x| String::from_utf8_lossy(x).into_owned()).collect();
            assert_eq!(shown, ["  2", " 20"]);
        }
    }

    #[test]
    fn seed_replay_all_arms() {
        // BIG-STACK thread: the corpus includes the arm-9 stackband seeds,
        // whose replay legitimately recurses up to the 2048kB guard budget
        // (see regexp_tests::run_on_big_stack).
        regexp_tests::run_on_big_stack("trgm-seed-replay-all-arms", || {
            let _serial = crate::c_oracle_serial();
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/trgm_diff");
            let mut n = 0;
            for entry in std::fs::read_dir(dir).expect("corpus dir") {
                let path = entry.unwrap().path();
                if path.is_file() && path.file_name().is_some_and(|f| f != ".gitkeep") {
                    trgm_diff(&std::fs::read(&path).unwrap());
                    n += 1;
                }
            }
            assert!(n >= 40, "committed seed corpus shrank: {n} < 40");
        });
    }

    /// Truncated exhaustive rail (lengths 0..=2) as a local smoke; the
    /// full 0..=3 sweep is the fleet job (#[ignore] below).
    #[test]
    fn exhaustive_short_generate_len2() {
        let _serial = crate::c_oracle_serial();
        let visited = exhaustive_short_generate_impl(2);
        assert_eq!(visited, utf8_count_upto(2), "domain not fully enumerated");
    }

    #[test]
    #[ignore = "fleet-scale: full 0..=3 valid-UTF-8 sweep (~2.4M strings x 2 arms)"]
    fn exhaustive_short_generate_len3() {
        let _serial = crate::c_oracle_serial();
        let visited = exhaustive_short_generate_impl(3);
        assert_eq!(visited, utf8_count_upto(3), "domain not fully enumerated");
    }

    #[test]
    #[ignore = "fleet-scale: full 2^24 trigram sweep"]
    fn exhaustive_trgm2int() {
        let _serial = crate::c_oracle_serial();
        init_env();
        let mut visited: u64 = 0;
        let mut prev: Option<Trgm> = None;
        for v in 0..(1u32 << 24) {
            let t: Trgm = [(v >> 16) as u8, (v >> 8) as u8, v as u8];
            let ri = trgm2int(&t);
            let ci = unsafe { pg_diff_trgm_trgm2int(t.as_ptr()) };
            assert_eq!(ri, ci);
            assert_eq!(ri, v, "trgm2int must be the big-endian pack");
            let mut c3 = [0u8; 3];
            unsafe { pg_diff_trgm_compact(t.as_ptr(), 3, c3.as_mut_ptr()) };
            assert_eq!(compact_trigram(&t, &crc), c3);
            if let Some(p) = prev {
                let r = match pg_trgm::trgm::cmp_trgm(&p, &t) {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                };
                let c = unsafe { pg_diff_trgm_cmp(p.as_ptr(), t.as_ptr(), 1) }.signum();
                assert_eq!(r, c, "cmp_trgm vs CMPTRGM_SIGNED at {v:#08x}");
            }
            prev = Some(t);
            visited += 1;
        }
        assert_eq!(visited, 1 << 24, "domain not fully enumerated");
    }
}

#[cfg(test)]
mod regexp_tests {
    use super::*;

    /// Deep-replay tests run on an explicit 32MB thread: the 2048kB guard
    /// budget (server parity, pinned on both sides) needs real stack
    /// under it regardless of the libtest default thread size — the
    /// byte-based guard bounds DEPTH FROM BASE, not remaining stack.
    pub(super) fn run_on_big_stack(name: &str, f: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .name(name.into())
            .stack_size(32 * 1024 * 1024)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("big-stack test thread panicked");
    }

    fn rx(pat: &[u8]) {
        let mut input = vec![9u8];
        input.extend_from_slice(pat);
        trgm_diff(&input);
    }

    /// exploratory dump helper (not a plane): C-side extraction summary
    fn c_dump(pat: &[u8]) -> String {
        init_env();
        pin_locale_arm(0);
        match c_regexp(pat) {
            Err(cls) => format!("ERR({cls})"),
            Ok(None) => "FALLBACK".into(),
            Ok(Some(o)) => {
                let trgs: Vec<String> = o
                    .trg
                    .chunks_exact(3)
                    .map(|t| {
                        if t.iter().all(|&b| (0x20..0x7f).contains(&b)) {
                            format!("{:?}", String::from_utf8_lossy(t))
                        } else {
                            format!("0x{:02x}{:02x}{:02x}", t[0], t[1], t[2])
                        }
                    })
                    .collect();
                format!(
                    "n={} groups={:?} states={} arcs={} trgs={}",
                    o.trg.len() / 3,
                    o.groups,
                    o.states.len(),
                    o.arcs.len(),
                    trgs.join(",")
                )
            }
        }
    }

    /// STANDING RAIL (task #150): arm-9 execs must not accumulate regex
    /// ENGINE memory. The vendored 18.3 createTrgmNFA frees engine memory
    /// via MemoryContextDelete of its tmpcontext (regcustom.h MALLOC =
    /// palloc_extended into the current context); the oracle shim maps
    /// palloc_extended to raw malloc and context deletion to a no-op, so
    /// the compiled regex guts MUST be pg_regfree'd through the TLS
    /// live-regex slot (trgmrx_pg_regcomp_capture registers, trgmrx_enter
    /// frees). When that registration was missing, every arm-9 compile
    /// leaked its whole regex (~25-30KB/exec RSS climb, sustained runs
    /// died at ~125k execs, fleet CONFIRM flooded with leak artifacts).
    ///
    /// Witness = the per-thread malloc-level balance over the engine's
    /// MALLOC/FREE/REALLOC contract (vendor/postgres.h counting shim) —
    /// it counts REAL engine allocations, so removing the registration
    /// again turns this red; it cannot go vacuous on bookkeeping.
    /// Steady-state contract: after any completed exec exactly one
    /// deferred-free compiled regex is live, so the balance after N execs
    /// of the same pattern must equal the balance after the first.
    /// (Detection power witnessed 2026-08-03: registration disabled ->
    /// 363 -> 3267 live allocs over 8 execs, red on the first pattern.)
    #[test]
    fn regexp_engine_memory_does_not_accumulate() {
        extern "C" {
            fn pg_diff_regexfam_live_allocs() -> std::os::raw::c_long;
        }
        let _serial = crate::c_oracle_serial();
        // Success-path pattern (extraction + all 32 graph rounds => 33
        // compiles per exec); plus a fallback and an error pattern so all
        // three verdict paths are held to the same balance.
        let pats: [&[u8]; 3] = [b"abc(def|ghi)jkl", b"a", b"(abc"];
        for pat in pats {
            rx(pat); // warm ctype cache + leave the deferred regex live
            let base = unsafe { pg_diff_regexfam_live_allocs() };
            for _ in 0..8 {
                rx(pat);
            }
            let after = unsafe { pg_diff_regexfam_live_allocs() };
            assert_eq!(
                after, base,
                "regex engine allocations accumulated across arm-9 execs \
                 (pat={:?}: {} -> {}): per-entry pg_regfree cleanup regressed \
                 — see trgmrx_pg_regcomp_capture / trgmrx_enter in \
                 csrc/pg_trgm_regexp_io.c",
                String::from_utf8_lossy(pat),
                base,
                after
            );
        }
    }

    #[test]
    fn regexp_explore_dump() {
        let _serial = crate::c_oracle_serial();
        for pat in [
            &b"abc"[..], b"a", b"", b"a|b", b"(a|b)cd", b".*", b"^abc$",
            b"[a-z]foo", b"ab{2,4}c", b"(abc)+", b"(a|b)(c|d)(e|f)(g|h)(i|j)",
            b"abc|def|ghi|jkl|mno|pqr", b"(", b"a{2,1}", b"[z-a]",
        ] {
            eprintln!("{:24} -> {}", String::from_utf8_lossy(pat), c_dump(pat));
        }
    }

    /// Limit-path WITNESSES (not exec-count hopes): sweep a family that
    /// crosses the expansion caps and require BOTH a success and a
    /// fallback inside the bracket — every point runs the full
    /// differential, so the limit paths are COMPARED, not just reached.
    /// (MAX_EXPANDED_STATES=128 / MAX_EXPANDED_ARCS=1024 / MAX_TRGM_COUNT
    /// =256 / WISH_TRGM_PENALTY=16 all live inside this bracket family.)
    #[test]
    fn regexp_limit_boundary_witness() {
        let _serial = crate::c_oracle_serial();
        let mut verdicts = Vec::new();
        for n in 1..=10 {
            let pat: Vec<u8> = (0..n)
                .flat_map(|i| {
                    let a = b'a' + (2 * i) as u8 % 26;
                    let b = b'a' + (2 * i + 1) as u8 % 26;
                    vec![b'(', a, b'|', b, b')']
                })
                .collect();
            let before = REGEXP_SUCCESS.load(AtOrd::Relaxed);
            rx(&pat);
            let after = REGEXP_SUCCESS.load(AtOrd::Relaxed);
            verdicts.push(after > before);
        }
        assert!(
            verdicts.iter().any(|&v| v) && verdicts.iter().any(|&v| !v),
            "alternation-product sweep never crossed a limit: {verdicts:?}"
        );

        // Big-class sweep: the >COLOR_COUNT_LIMIT (256-char) class goes
        // unexpandable on both sides; the bracket witnesses the transition.
        let mut class_verdicts = Vec::new();
        for &n in &[8usize, 64, 255, 257, 300] {
            let mut pat = b"foo[".to_vec();
            for c in 0..n {
                let ch = char::from_u32(0x100 + c as u32).unwrap();
                let mut buf = [0u8; 4];
                pat.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            pat.extend_from_slice(b"]bar");
            let before = REGEXP_SUCCESS.load(AtOrd::Relaxed);
            rx(&pat);
            class_verdicts.push(REGEXP_SUCCESS.load(AtOrd::Relaxed) > before);
        }
        // every point compared; record the shape for the report
        eprintln!("class-size sweep verdicts: {class_verdicts:?}");
    }

    /// Oracle-derived pins (eyeballed against the trigram-extraction
    /// contract — the required trigrams are ones every matching string
    /// must contain): regenerate with regexp_explore_dump.
    #[test]
    fn regexp_oracle_pins() {
        let _serial = crate::c_oracle_serial();
        init_env();
        pin_locale_arm(0);
        let pins: &[(&[u8], &str)] = &[
            (b"(a|b)cd", r#"n=2 trgs="acd","bcd""#),
            (b"^abc$", r#"n=2 trgs="abc"," ab""#),
            (b"ab{2,4}c", r#"n=3 trgs="abb","bbb","bbc""#),
            (b"(abc)+", r#"n=3 trgs="abc","bca","cab""#),
            (b"abc|def|ghi|jkl|mno|pqr", r#"n=6 trgs="abc","def","ghi","jkl","mno","pqr""#),
        ];
        for (pat, expect) in pins {
            let got = c_dump(pat);
            for frag in expect.split(" trgs=") {
                assert!(
                    got.contains(frag),
                    "pin drift for {:?}: expected fragment {frag:?} in {got}",
                    String::from_utf8_lossy(pat)
                );
            }
            rx(pat); // and the full differential agrees
        }
    }

    #[test]
    fn regexp_seed_corpus_replay() {
        // BIG-STACK thread (not the libtest default): the stack guards on
        // both sides run at the real-server 2048kB budget (arm_stack_guard
        // header), measured from a base anchored at replay entry — the
        // stackband seeds legitimately recurse right up to that budget
        // before the guard trips, which needs budget + entry depth +
        // >=2048kB headroom of REAL stack (debug frames; stack-guard law).
        // The fuzz target itself runs on libFuzzer's 8MB main thread.
        run_on_big_stack("trgm-regexp-corpus-replay", || {
            let _serial = crate::c_oracle_serial();
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/trgm_diff");
            let mut n = 0;
            for e in std::fs::read_dir(dir).expect("corpus dir") {
                let p = e.unwrap().path();
                if p.file_name().unwrap().to_string_lossy().starts_with("regexp-") {
                    trgm_diff(&std::fs::read(&p).unwrap());
                    n += 1;
                }
            }
            assert!(n >= 25, "regexp seed corpus went missing (found {n})");
            eprintln!(
                "regexp corpus replay: {n} seeds; order witness eq={} diff={} succ={} fb={} err={} carve={}",
                REGEXP_ORDER_EQ.load(AtOrd::Relaxed),
                REGEXP_ORDER_DIFF.load(AtOrd::Relaxed),
                REGEXP_SUCCESS.load(AtOrd::Relaxed),
                REGEXP_FALLBACK.load(AtOrd::Relaxed),
                REGEXP_ERR.load(AtOrd::Relaxed),
                REGEXP_ETOOBIG_CARVE.load(AtOrd::Relaxed),
            );
        });
    }

    /// 2026-08-03 fleet CONFIRM stack-overflow class (job
    /// pgrust-fuzz-campaign-1785794601-6bab-5889 @ main 5b707bdb597, 156
    /// ASan crash artifacts): quantified-alternation patterns drove
    /// regex_core::regex_nfa::duptraverse into unbounded self-recursion
    /// because neither side's stack guard base was armed in this harness
    /// (product + oracle guards both exist and both short-circuit on
    /// base==0). Replays the banked artifacts (committed as the
    /// regexp-stackband-confirm-* corpus seeds) through the FULL driver:
    /// graceful verdict planes, never a crash. This is also the
    /// guard-DISABLED must-fail control's counterpart: the pre-fix build
    /// (base unarmed = guard disabled) demonstrably crashed on exactly
    /// these bytes — fleet artifacts + local macOS replay, banked in the
    /// lane evidence.
    #[test]
    fn regexp_stackband_banked_artifact_replay() {
        run_on_big_stack("trgm-stackband-banked-replay", || {
            let _serial = crate::c_oracle_serial();
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/trgm_diff");
            let mut n = 0;
            for name in [
                "regexp-stackband-confirm-a",
                "regexp-stackband-confirm-b",
                "regexp-stackband-confirm-c",
            ] {
                let data = std::fs::read(format!("{dir}/{name}")).expect("banked seed");
                assert_eq!(data[0] & 0x0F, 9, "banked seed is not arm 9: {name}");
                trgm_diff(&data);
                n += 1;
            }
            assert_eq!(n, 3);
            eprintln!(
                "stackband replay: succ={} fb={} err={} carve={}",
                REGEXP_SUCCESS.load(AtOrd::Relaxed),
                REGEXP_FALLBACK.load(AtOrd::Relaxed),
                REGEXP_ERR.load(AtOrd::Relaxed),
                REGEXP_ETOOBIG_CARVE.load(AtOrd::Relaxed),
            );
        });
    }

    /// Stack-guard law must-fail pair (RELEASE-effective: the guard is a
    /// runtime branch, not a debug_assert). The SAME deep-duplication
    /// pattern must (a) fail with the guard's "too complex" verdict at a
    /// 64kB budget and (b) compile cleanly at a 16384kB budget on a 32MB
    /// thread (>= 2048kB headroom rule) — proving the guard is LIVE and
    /// BYTE-bounded, and that (b)'s success is the guard abstaining, not
    /// dead code. Pattern provenance: the pg_regexfam.c header's
    /// calibration pattern family ((a|bb){N} duplication — the 100kB boot
    /// default fake-fired on it, real PG at 2048kB compiles it).
    /// The guard-disabled crash arm cannot run in-process (a blown stack
    /// aborts, uncatchably); its witness is the pre-fix fleet run + local
    /// pre-fix replay recorded in regexp_stackband_banked_artifact_replay.
    #[test]
    fn regexp_stack_guard_must_fail_pair() {
        run_on_big_stack("trgm-stackguard-mustfail", || {
                let _serial = crate::c_oracle_serial();
                init_env();
                pin_locale_arm(0);
                stack_depth::set_stack_base();
                let env = pg_trgm::harness_env();
                let pat: &[u8] = b"(a|bb){96}";

                // (a) tight budget: the guard MUST fire, as the graceful
                // too-complex statement error (PG contract: statement
                // failure, never process death).
                stack_depth::assign_max_stack_depth(64);
                let r = pg_trgm::regexp::create_trgm_nfa(
                    pat,
                    types_core::C_COLLATION_OID,
                    &env,
                    &crc,
                );
                match &r {
                    Err(re) => assert!(
                        re.message().ends_with(RE_TOO_COMPLEX_MSG),
                        "64kB-budget failure is not the stack guard's verdict: {re:?}"
                    ),
                    Ok(o) => panic!(
                        "64kB budget did not trip the stack guard: Ok(is_some={})",
                        o.is_some()
                    ),
                }

                // (b) roomy budget, same input, same thread: the guard must
                // abstain and the compile succeed — the error in (a) came
                // from the BYTE bound, not from the pattern. (Ok(None) =
                // the legitimate index-fallback verdict is fine; only an
                // Err would mean the guard is not byte-bounded.)
                stack_depth::assign_max_stack_depth(16 * 1024);
                let r = pg_trgm::regexp::create_trgm_nfa(
                    pat,
                    types_core::C_COLLATION_OID,
                    &env,
                    &crc,
                );
                assert!(
                    r.is_ok(),
                    "16384kB budget still failed (guard not byte-bounded?): {:?}",
                    r.as_ref().err()
                );

                // restore the harness budget for any later test on this thread
                stack_depth::assign_max_stack_depth(2048);
        });
    }

    #[test]
    fn regexp_smoke_seeds() {
        let _serial = crate::c_oracle_serial();
        for pat in [
            &b"abc"[..], b"a", b"", b"a|b", b"(a|b)cd", b".*", b"^abc$",
            b"[a-z]foo", b"ab{2,4}c", b"(abc)+", b"(a|b)(c|d)(e|f)(g|h)(i|j)",
            b"abc|def|ghi|jkl|mno|pqr", b"(", b"a{2,1}", b"[z-a]",
        ] {
            rx(pat);
        }
        eprintln!(
            "order witness: eq={} diff={} success={} fallback={} err={}",
            REGEXP_ORDER_EQ.load(AtOrd::Relaxed),
            REGEXP_ORDER_DIFF.load(AtOrd::Relaxed),
            REGEXP_SUCCESS.load(AtOrd::Relaxed),
            REGEXP_FALLBACK.load(AtOrd::Relaxed),
            REGEXP_ERR.load(AtOrd::Relaxed),
        );
    }
}

#[cfg(test)]
mod arm2_tests {
    use super::*;

    fn run(sel: u8, pay: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(pay);
        trgm_diff(&v);
    }

    /// Ground-truth pins from a live postgres:18.3 SQL_ASCII/C database
    /// (docker aarch64, 2026-08-01): show_trgm('a b c') =
    /// {"  a","  b","  c"," a "," b "," c "}; show_trgm('café') =
    /// {"  c"," ca","af ",caf}; similarity('café','cafe') = 0.5;
    /// word_similarity('Sunday','Saturday') = 0.2857143;
    /// show_trgm('abc\xc3\x9f') = {"  a"," ab",abc,"bc "}.
    #[test]
    fn pins_sqlascii_arm2() {
        let _serial = crate::c_oracle_serial();
        init_env();
        pin_locale_arm(2);
        let show = |s: &[u8]| -> Vec<Vec<u8>> { pg_trgm::show_trgm_elements(s) };
        // Rust side pins (the differential below then proves C agrees):
        let e: Vec<Vec<u8>> =
            [b"  a", b"  b", b"  c", b" a ", b" b ", b" c "].iter().map(|s| s.to_vec()).collect();
        assert_eq!(show(b"a b c"), e);
        let e: Vec<Vec<u8>> =
            [&b"  c"[..], b" ca", b"af ", b"caf"].iter().map(|s| s.to_vec()).collect();
        assert_eq!(show("café".as_bytes()), e);
        let e: Vec<Vec<u8>> =
            [&b"  a"[..], b" ab", b"abc", b"bc "].iter().map(|s| s.to_vec()).collect();
        assert_eq!(show(b"abc\xc3\x9f"), e);
        let env = pg_trgm::harness_env();
        let t1 = generate_trgm("café".as_bytes(), &env, &crc);
        let t2 = generate_trgm(b"cafe", &env, &crc);
        assert_eq!(cnt_sml(&t1, &t2, false).to_bits(), 0.5f32.to_bits());
        let ws = calc_word_similarity(b"Sunday", b"Saturday", 0, &env, &crc, 0.6, 0.5);
        assert_eq!(ws.to_bits(), 0.2857143f32.to_bits());
        // full differential on the same inputs (raw-byte domain incl. the
        // 0xfe high-bit and interior-NUL truncation parity seeds)
        for (arm, pay) in [
            (1u8, &b"a b c"[..]),
            (0, "café".as_bytes()),
            (0, b"abc\xc3\x9f"),
            (0, b"ab\x00cd"),
            (0, b"ab\xc3\x9fcd\xfe"),
        ] {
            run(0x20 | arm, pay);
        }
        run(0x22, &[b'c', b'a', b'f', 0xc3, 0xa9, 0xFF, b'c', b'a', b'f', b'e']);
        run(0x23, b"\x00Sunday\xffSaturday");
    }

    /// Line-101 witness (make_trigrams multibyte-branch early return,
    /// trgm.rs:100-102): a wildcard part that is a SINGLE unpadded
    /// multibyte word char has bytelen == lenfirst, so the branch returns
    /// with zero trigrams — the pattern extracts an EMPTY required set.
    /// The padded variants ('%一a%') extract non-empty. Both sides must
    /// agree on all of it (differential), and the empty/non-empty split
    /// witnesses the branch fired.
    #[test]
    fn wildcard_mb_line101_witness() {
        let _serial = crate::c_oracle_serial();
        init_env();
        pin_locale_arm(1);
        let env = pg_trgm::harness_env();
        let lone = generate_wildcard_trgm("%一%".as_bytes(), &env, &crc);
        assert!(lone.is_empty(), "single-mb-char wildcard part must yield no trigrams");
        // "%一a%" = 2-char unpadded word: the SECOND early return
        // (ptr+lenfirst+lenmiddle >= bytelen) — also zero trigrams.
        let two = generate_wildcard_trgm("%一a%".as_bytes(), &env, &crc);
        assert!(two.is_empty(), "2-char mb wildcard part must also yield no trigrams");
        let three = generate_wildcard_trgm("%一ab%".as_bytes(), &env, &crc);
        assert!(!three.is_empty(), "3-char variant must yield a trigram");
        for pat in ["%一%", "%一a%", "%一ab%", "%ж%", "%жa%", "%жab%", "%𠀀%"] {
            run(0x14, pat.as_bytes());
        }
    }
}

#[cfg(test)]
mod repro_tests {
    use super::*;

    fn dump_both(pat: &[u8]) -> (String, String) {
        init_env();
        pin_locale_arm(0);
        let env = pg_trgm::harness_env();
        let r = pg_trgm::regexp::create_trgm_nfa(pat, types_core::C_COLLATION_OID, &env, &crc);
        let rs = match &r {
            Err(e) => format!("ERR({e:?})"),
            Ok(None) => "FALLBACK".into(),
            Ok(Some((t, _))) => {
                let mut v: Vec<String> = t.iter().map(|t| format!("{:02x}{:02x}{:02x}", t[0], t[1], t[2])).collect();
                v.sort();
                format!("n={} {}", t.len(), v.join(","))
            }
        };
        let cs = match c_regexp(pat) {
            Err(c) => format!("ERR({c})"),
            Ok(None) => "FALLBACK".into(),
            Ok(Some(o)) => {
                let mut v: Vec<String> = o.trg.chunks_exact(3).map(|t| format!("{:02x}{:02x}{:02x}", t[0], t[1], t[2])).collect();
                v.sort();
                format!("n={} {}", o.trg.len() / 3, v.join(","))
            }
        };
        (rs, cs)
    }

    /// DIVERGENCE-OF-RECORD (p1-trgm phase B, 2026-08-01, minimized from
    /// fuzz crash-70ab9f26be5e154f15be814574ae45553b68b4d9): pattern
    /// `(\x16{31}a|b)(c|d)(e|f)(g|h)(i|j)` — both sides extract 16
    /// trigrams but DIFFERENT multisets (C keeps the ace/acf/ade family,
    /// Rust keeps ceg/ceh/cfg). Mechanism: selectColorTrigrams' over-budget
    /// eviction removes equal-penalty color trigrams in SORT ORDER; C's
    /// order = PG qsort (unstable) over the dynahash hash_seq collection
    /// order (both vendored order-exact in this oracle), Rust's = stable
    /// sort over state-insertion order. Both extractions are sound
    /// (recheck-protected; NOT SQL-result-visible) — pending ruling:
    /// order-exact port vs certified value-equal relaxation (multirange
    /// precedent). RESOLVED by the order-exact-port ruling (Michael,
    /// 2026-08-01): regexp.rs now uses colorTrgmInfoCmp's memcmp byte order
    /// for every ctrgm sort/bsearch and evicts equal-penalty color trigrams
    /// in the exact pg_qsort permutation C produces (canonical
    /// crates/_support/pg_qsort port over (penalty, index) proxies; swapped
    /// from the retired gistproc copy at GL-PGQSORT-PERF consolidation).
    /// The historical repro is
    /// kept as a REGRESSION equality assertion — if either side's tie
    /// behavior drifts, this fires first.
    #[test]
    fn resolved_penalty_tie_order_regression() {
        let _serial = crate::c_oracle_serial();
        let pat = format!("({}a|b)(c|d)(e|f)(g|h)(i|j)", "\x16".repeat(31));
        let pat = pat.replace("\\x16", "\x16");
        let (r, c) = dump_both(pat.as_bytes());
        assert!(r.starts_with("n=16") && c.starts_with("n=16"), "shape drift: {r} vs {c}");
        assert_eq!(r, c, "penalty-tie eviction order regressed vs C (order-exact ruling 2026-08-01)");
    }

    #[test]
    fn minimize_crash_70ab9f26() {
        let _serial = crate::c_oracle_serial();
        // original: (\x16{31}a|b)(c|d)(e|f)(g|h)(i|j)
        for (name, pat) in [
            ("orig-shape", format!("({}a|b)(c|d)(e|f)(g|h)(i|j)", "\x16".repeat(31))),
            ("short1", format!("({}a|b)(c|d)", "\x16".repeat(31))),
            ("short2", format!("({}a|b)(c|d)", "\x16".repeat(3))),
            ("short3", "(\x16a|b)(c|d)".to_string()),
            ("short4", "(\x16a|b)c".to_string()),
            ("short5", "\x16a".to_string()),
            ("short6", "(!a|b)c".to_string()),
        ] {
            let (r, c) = dump_both(pat.as_bytes());
            eprintln!("{name}: rust={r}");
            eprintln!("{name}:    c={c}");
        }
    }
}
