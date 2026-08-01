//! trgm_diff: dual-exec differential driver for crates/contrib/pg_trgm
//! (trgm_op.c half) against the vendored 18.3 C oracle in
//! `csrc/pg_trgm_io.c`. 100%-coverage campaign, lane p1-trgm.
//!
//! INPUT SHAPE: byte0 = selector — low nibble = arm (0..=8; 9..=15
//! reserved, 9 will be the trgm_regexp arm), bit 4 = locale_arm. The rest
//! is payload; two-string arms split on the FIRST 0xFF byte (0xFF is never
//! valid UTF-8, so it cannot collide with the domain gate below).
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
//!           spots; the Linux CI cluster run is the record for arm 1.
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
//! INJECTION SWEEP: kill table maintained at the END of this header —
//! filled in by the sweep run; a plane with an unfilled table has never
//! been shown to compare anything.
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
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 pin");
    // ENCODING PIN ASSERT (harness law): both sides must agree what a byte
    // sequence means. C side is pinned inside every pg_diff_trgm_* entry.
    assert_eq!(mbutils::pg_database_encoding_max_length(), 4, "UTF8 pin");
}

/// Thread LC_CTYPE for the arm, via uselocale(3) — the honest model of
/// PG's backend-start `setlocale(LC_CTYPE, <database ctype>)`: a C-ctype
/// database runs its backends with LC_CTYPE=C (byte isalnum is pure
/// ASCII), a C.UTF-8 database with a UTF-8 LC_CTYPE (mbstowcs/iswalnum
/// decode UTF-8). uselocale is per-thread and read by BOTH sides' libc
/// ctype calls, so the two sides can never disagree about the ctype
/// environment. C.UTF-8 exists on glibc (CI cluster = platform of record);
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
    thread_ctype(locale_arm);
    if locale_arm == 0 {
        pg_locale::set_database_ctype_is_c(true);
        pg_locale::set_default_locale_c_for_tests();
    } else {
        pg_locale::set_database_ctype_is_c(false);
        pg_locale::set_default_locale_builtin_utf8_for_tests();
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
            let _g1 = catch_unwind(guc_tables::init_seams);
            let _g2 = catch_unwind(elog::init_seams);
            let _g3 = catch_unwind(guc::init_seams);
            if !guc::store::is_initialized() {
                let _g4 = catch_unwind(guc::store::initialize_guc_options);
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
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let arm = sel & 0x0F;
    let locale_arm = i32::from(sel >> 4 & 1);
    init_env();
    pin_locale_arm(locale_arm);

    match arm {
        0 | 1 | 4 => {
            if !utf8_ok(payload) {
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
            if !utf8_ok(a) || !utf8_ok(b) {
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
            if !utf8_ok(a) || !utf8_ok(b) {
                return;
            }
            arm_word_similarity(locale_arm, flags, a, b);
        }
        7 => arm_compact(payload),
        8 => arm_cmp(payload),
        _ => {} // 9..=15 reserved (9 = regexp arm, second lane half)
    }
}

// ---------------------------------------------------------------------------
// Exhaustive rails (CI-run; #[ignore] locally except a truncated smoke).
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
        assert_eq!(show(0, "café".as_bytes()), ["  c", " ca", "af ", "caf"]);
        assert_eq!(sim(0, "café".as_bytes(), b"cafe").to_bits(), f4("0.5"));
        // Cyrillic: no word chars at all under ctype C
        assert_eq!(show(0, "Ация тест".as_bytes()), Vec::<String>::new());
    }

    // ---- builtin C.UTF-8 pins (arm 1) ----
    #[test]
    fn pins_builtin_utf8_arm1() {
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
    }

    /// Truncated exhaustive rail (lengths 0..=2) as a local smoke; the
    /// full 0..=3 sweep is the CI cluster job (#[ignore] below).
    #[test]
    fn exhaustive_short_generate_len2() {
        let visited = exhaustive_short_generate_impl(2);
        assert_eq!(visited, utf8_count_upto(2), "domain not fully enumerated");
    }

    #[test]
    #[ignore = "CI-scale: full 0..=3 valid-UTF-8 sweep (~2.4M strings x 2 arms)"]
    fn exhaustive_short_generate_len3() {
        let visited = exhaustive_short_generate_impl(3);
        assert_eq!(visited, utf8_count_upto(3), "domain not fully enumerated");
    }

    #[test]
    #[ignore = "CI-scale: full 2^24 trigram sweep"]
    fn exhaustive_trgm2int() {
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
