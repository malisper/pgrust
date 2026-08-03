//! regex_diff: differential fuzz driver — shipped Rust `regex_core` (the
//! Spencer engine port, crates/backend/regex/regex_core) vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/regexfam/, byte-for-byte copy of bench/cref/regex_vendor plus
//! regprefix.c/regexport.c fetched verbatim at the same sha).
//!
//! Pattern and subject cross both engine boundaries as pg_wchar code
//! points — no mb conversion on either side, so both engines see the
//! identical chr sequence (mb parity is owned by the mbutils lanes).
//! Locale surface: C collation (950) only; the BUILTIN/LIBC/ICU strategy
//! arms are covered by the exhaustive pg_wc_* class sweeps (routes rows)
//! and are aborting stubs in the vendored oracle.
//!
//! Comparison planes, all iterations:
//!   compile — REG_* verdict; on failure the pg_regerror-formatted message
//!             (regerror.c table parity IS the error plane); on success
//!             re_nsub.
//!   exec    — verdict (match / no-match / error message) + every requested
//!             pmatch slot's (rm_so, rm_eo), for a fuzz-selected nmatch in
//!             {0, 1, nsub+1, nsub+2} and fuzz-selected search_start.
//!             eflags fixed 0 (the pg_regexec seam contract).
//!   prefix  — pg_regprefix variant (NoMatch/Prefix/Exact/Failed) + the
//!             extracted prefix code points (regprefix.c).
//!   export  — the regexport.c NFA view (states/colors/arcs/color chars)
//!             serialized identically on both sides (pg_trgm's consumer
//!             surface), deterministically truncated at the same cap.
//!
//! Input layout: [cflags][start_sel][nmatch_sel][plen][pattern..][subject..]
//!   cflags    = low 8 bits of the compile flags, giving EXTENDED/ADVF/
//!               QUOTE/ICASE/NOSUB/EXPANDED/NLSTOP/NLANCH (REG_PEND etc.
//!               are caller-API bits regexp.c never sets; out of scope).
//!   plen      = pattern length in input BYTES, capped at 96; subject is
//!               the remainder, capped at 160.
//!   byte->chr = bytes < 0x80 are that ASCII code point; bytes >= 0x80 map
//!               through a 128-entry table of boundary code points
//!               (MAX_SIMPLE_CHR 0x7FF +/-1, surrogate-adjacent, plane
//!               boundaries, 0x10FFFF) and Latin/Greek letters, so class
//!               and colormap high-arc paths are reachable. Both sides see
//!               the mapped values; the raw-byte grammar stays ASCII so
//!               the corpus reads as patterns.
//!
//! Stack note: both sides guard recursion (C: stack_is_too_deep over a
//! 100kB budget; Rust: stack_depth crate). At the 96-chr pattern cap parse
//! depth stays far below either budget, so the guards are dead on both
//! sides rather than a compared surface.

use regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexecResult, RegprefixResult,
};
use regex_core::regex_export_free_error::{
    pg_reg_colorisbegin, pg_reg_colorisend, pg_reg_getcharacters,
    pg_reg_getfinalstate, pg_reg_getinitialstate, pg_reg_getnumcharacters,
    pg_reg_getnumcolors, pg_reg_getnumoutarcs, pg_reg_getnumstates,
    pg_reg_getoutarcs, seam_pg_regcomp, seam_pg_regexec,
    seam_pg_regprefix, RegexArc,
};
use regex_core::regguts::RegexT;
use types_core::{PgWChar, C_COLLATION_OID};

extern "C" {
    fn pg_diff_regex_init();
    fn pg_diff_regcomp(pat: *const u32, plen: i32, cflags: i32, nsub_out: *mut i32) -> i32;
    fn pg_diff_regexec(
        data: *const u32,
        dlen: i32,
        search_start: i32,
        nmatch: i32,
        so_eo: *mut i64,
    ) -> i32;
    fn pg_diff_regprefix(prefix_out: *mut u32, cap: i32, plen_out: *mut i32) -> i32;
    fn pg_diff_regfree();
    fn pg_diff_regerror(errcode: i32, errbuf: *mut u8, errbuf_size: usize) -> usize;
    fn pg_diff_reg_export(out: *mut i32, cap: i32) -> i32;
}

const MAX_PATTERN: usize = 96;
const MAX_SUBJECT: usize = 160;
const MAX_NMATCH: usize = 63; // C shim slots at 64
const PREFIX_CAP: usize = 256;
const EXPORT_CAP: usize = 4096;

/// C regerror(code, re, buf, n) message for a code, as the oracle formats it.
fn c_regerror(code: i32) -> String {
    let mut buf = [0u8; 256];
    let n = unsafe { pg_diff_regerror(code, buf.as_mut_ptr(), buf.len()) };
    // pg_regerror returns strlen+1 (space needed incl. NUL); the written
    // string may be truncated to the buffer, which 256 never triggers for
    // the fixed message table.
    let len = (n - 1).min(buf.len() - 1);
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

/// byte -> pg_wchar mapping (documented in the module header).
fn cp(b: u8) -> u32 {
    if b < 0x80 {
        return b as u32;
    }
    const SPECIALS: [u32; 32] = [
        0x80, 0xFF, 0x100, 0x101, 0x130, 0x131, 0x17F, 0x1C4, // Latin ext / case oddities
        0x3B1, 0x391, 0x430, 0x410, // greek/cyrillic case pairs
        0x7FE, 0x7FF, 0x800, 0x801, // MAX_SIMPLE_CHR boundary
        0x9, 0xA, 0xD, 0x0, // control/NL (chr level; SQL never sends NUL but the engine domain does)
        0xD7FF, 0xE000, 0xFFFD, 0xFFFF, // surrogate-adjacent / BMP edge
        0x10000, 0x1D7CE, 0x10FFFE, 0x10FFFF, // astral / CHR domain top (SQL-reachable)
        0x2028, 0x2029, 0x200B, 0x2060, // separators / zero-width
    ];
    let k = b & 0x7F;
    if (k as usize) < SPECIALS.len() {
        SPECIALS[k as usize]
    } else {
        // Latin-1 supplement letters onward: dense case-pair region.
        0xC0 + (k as u32 - SPECIALS.len() as u32)
    }
}

fn decode_wchars(bytes: &[u8]) -> Vec<u32> {
    bytes.iter().map(|&b| cp(b)).collect()
}

fn init() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        unsafe { pg_diff_regex_init() };
        // the crate's real server-startup entry (installs the engine seams);
        // the driver calls the seam impls directly, but the installer IS
        // shipped-crate surface.
        regex_core::init_seams();
    });
    // Per-thread (thread_local state in stack_depth): arm the Rust stack
    // guard exactly as a real backend does, at the same 2048kB budget the C
    // oracle shim pins (real-server default; see pg_regexfam.c header).
    std::thread_local! {
        static THREAD_ARMED: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    }
    THREAD_ARMED.with(|armed| {
        if !armed.get() {
            stack_depth::set_stack_base();
            stack_depth::set_max_stack_depth(2048);
            stack_depth::assign_max_stack_depth(2048);
            armed.set(true);
        }
    });
}

/// RATIFIED PLATFORM NON-SURFACE: "regular expression is too complex"
/// (REG_ETOOBIG) on exactly ONE side. The guard that raises it is
/// PostgreSQL's stack-depth check inside the engine's recursive walks —
/// its trip point is a function of machine frame sizes, so rustc and
/// clang builds of the same algorithm cannot trip at the same input even
/// at identical byte budgets (real PG's own trip point varies by compiler
/// and platform; the GUC exists because the number is environmental).
/// Both sides run the guard at the real-server default (2048kB). The
/// DETERMINISTIC too-complex mechanism — the C-exact v->spaceused batch
/// accounting, REG_MAX_COMPILE_SPACE — is C-parity in regex_core (fixed
/// by this lane, fuzz-found) and covered by tests::etoobig_spaceused_parity
/// below; this carve tolerates only the stack-band asymmetry.
fn is_etoobig(msg: &str) -> bool {
    msg == "regular expression is too complex"
}

/// Rust-side export serialization — the exact layout pg_diff_reg_export
/// writes (see the C header comment). Truncation cap identical.
fn rust_export(re: &RegexT) -> Vec<i32> {
    const EXPORT_MAXCHARS: usize = 32;
    let mut out: Vec<i32> = Vec::with_capacity(EXPORT_CAP);
    macro_rules! put {
        ($v:expr) => {
            if out.len() >= EXPORT_CAP {
                return out;
            } else {
                out.push($v as i32);
            }
        };
    }
    let numstates = pg_reg_getnumstates(re);
    let numcolors = pg_reg_getnumcolors(re);
    put!(numstates);
    put!(pg_reg_getinitialstate(re));
    put!(pg_reg_getfinalstate(re));
    put!(numcolors);
    for co in 1..numcolors {
        let nchars = pg_reg_getnumcharacters(re, co);
        put!(pg_reg_colorisbegin(re, co) as i32);
        put!(pg_reg_colorisend(re, co) as i32);
        put!(nchars);
        if nchars > 0 {
            let take = (nchars as usize).min(EXPORT_MAXCHARS);
            let mut chars = vec![0 as PgWChar; take];
            pg_reg_getcharacters(re, co, &mut chars);
            for c in chars {
                put!(c);
            }
        }
    }
    for st in 0..numstates {
        let narcs = pg_reg_getnumoutarcs(re, st);
        let take = (narcs as usize).min(64);
        let mut arcs = vec![RegexArc { co: 0, to: 0 }; take];
        pg_reg_getoutarcs(re, st, &mut arcs);
        put!(narcs);
        for a in arcs {
            put!(a.co);
            put!(a.to);
        }
    }
    out
}

pub fn regex_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init();
    if data.len() < 4 {
        return;
    }
    let cflags = data[0] as i32; // low 8 REG_* compile bits, see header
    let start_sel = data[1] as usize;
    let nmatch_sel = data[2] as usize;
    let plen = (data[3] as usize).min(MAX_PATTERN);
    let rest = &data[4..];
    if rest.len() < plen {
        return;
    }
    let (pat_bytes, subj_bytes) = rest.split_at(plen);
    let subj_bytes = &subj_bytes[..subj_bytes.len().min(MAX_SUBJECT)];

    let pat = decode_wchars(pat_bytes);
    let subj = decode_wchars(subj_bytes);

    // ---- compile plane ----
    let mut c_nsub: i32 = -1;
    let c_code =
        unsafe { pg_diff_regcomp(pat.as_ptr(), pat.len() as i32, cflags, &mut c_nsub) };
    let r_res = seam_pg_regcomp(&pat, cflags, C_COLLATION_OID)
        .expect("rust pg_regcomp ereport'd (alloc/interrupt only) at fuzz sizes");

    let compiled: RegexCompiled = match r_res {
        RegcompResult::Failed(f) => {
            if c_code == 0 && is_etoobig(&f.message) {
                // one-sided too-complex: stack-band non-surface (see is_etoobig)
                unsafe { pg_diff_regfree() };
                return;
            }
            assert_ne!(
                c_code, 0,
                "COMPILE DIVERGENCE: C compiled, Rust failed with {:?} (pattern {:x?}, cflags {:o})",
                f.message, pat, cflags
            );
            let c_msg = c_regerror(c_code);
            assert_eq!(
                f.message, c_msg,
                "COMPILE ERROR-PLANE DIVERGENCE (pattern {:x?}, cflags {:o})",
                pat, cflags
            );
            unsafe { pg_diff_regfree() };
            return;
        }
        RegcompResult::Compiled(re) => {
            if c_code == 19 {
                // one-sided too-complex: stack-band non-surface (see is_etoobig)
                unsafe { pg_diff_regfree() };
                return;
            }
            assert_eq!(
                c_code,
                0,
                "COMPILE DIVERGENCE: Rust compiled, C failed with code {} ({}) (pattern {:x?}, cflags {:o})",
                c_code,
                c_regerror(c_code),
                pat,
                cflags
            );
            assert_eq!(
                re.re_nsub, c_nsub as usize,
                "re_nsub DIVERGENCE (pattern {:x?}, cflags {:o})",
                pat, cflags
            );
            re
        }
    };

    // ---- exec plane ----
    let nsub = compiled.re_nsub;
    let search_start = if subj.is_empty() {
        0
    } else {
        start_sel % (subj.len() + 1)
    };
    let nmatch = match nmatch_sel % 4 {
        0 => 0,
        1 => 1,
        2 => nsub + 1,
        _ => nsub + 2,
    }
    .min(MAX_NMATCH);

    let mut c_soeo = vec![0i64; 2 * nmatch.max(1)];
    let c_exec = unsafe {
        pg_diff_regexec(
            subj.as_ptr(),
            subj.len() as i32,
            search_start as i32,
            nmatch as i32,
            c_soeo.as_mut_ptr(),
        )
    };
    assert_ne!(c_exec, -100, "driver bug: C shim rejected the call");

    let mut pmatch = vec![RegMatch::UNSET; nmatch];
    let r_exec = seam_pg_regexec(&compiled, &subj, search_start as i32, &mut pmatch)
        .expect("rust pg_regexec ereport'd (alloc/interrupt only) at fuzz sizes");

    if (c_exec == 19) != matches!(&r_exec, RegexecResult::Failed(f) if is_etoobig(&f.message)) {
        if c_exec == 19 || matches!(&r_exec, RegexecResult::Failed(f) if is_etoobig(&f.message)) {
            // one-sided too-complex during exec (cdissect/DFA recursion):
            // stack-band non-surface (see is_etoobig)
            unsafe { pg_diff_regfree() };
            return;
        }
    }
    match r_exec {
        RegexecResult::Matched => {
            assert_eq!(
                c_exec, 0,
                "EXEC DIVERGENCE: Rust matched, C returned {} ({}) (pattern {:x?}, cflags {:o}, subj {:x?}, start {})",
                c_exec, c_regerror(c_exec), pat, cflags, subj, search_start
            );
            for i in 0..nmatch {
                assert_eq!(
                    (pmatch[i].rm_so, pmatch[i].rm_eo),
                    (c_soeo[2 * i], c_soeo[2 * i + 1]),
                    "PMATCH DIVERGENCE slot {} (pattern {:x?}, cflags {:o}, subj {:x?}, start {})",
                    i, pat, cflags, subj, search_start
                );
            }
        }
        RegexecResult::NoMatch => {
            assert_eq!(
                c_exec, 1,
                "EXEC DIVERGENCE: Rust no-match, C returned {} (pattern {:x?}, cflags {:o}, subj {:x?}, start {})",
                c_exec, pat, cflags, subj, search_start
            );
        }
        RegexecResult::Failed(f) => {
            assert!(
                c_exec != 0 && c_exec != 1,
                "EXEC DIVERGENCE: Rust failed ({:?}), C returned {} (pattern {:x?}, cflags {:o}, subj {:x?}, start {})",
                f.message, c_exec, pat, cflags, subj, search_start
            );
            assert_eq!(
                f.message,
                c_regerror(c_exec),
                "EXEC ERROR-PLANE DIVERGENCE (pattern {:x?}, cflags {:o}, subj {:x?}, start {})",
                pat, cflags, subj, search_start
            );
        }
    }

    // ---- prefix plane ----
    let mut c_prefix = vec![0u32; PREFIX_CAP];
    let mut c_plen: i32 = 0;
    let c_pref =
        unsafe { pg_diff_regprefix(c_prefix.as_mut_ptr(), PREFIX_CAP as i32, &mut c_plen) };
    let cx = mcx::MemoryContext::new("regex_diff_prefix");
    let r_pref = seam_pg_regprefix(cx.mcx(), &compiled)
        .expect("rust pg_regprefix ereport'd (alloc/interrupt only) at fuzz sizes");
    if (c_pref == 19) != matches!(&r_pref, RegprefixResult::Failed(f) if is_etoobig(&f.message)) {
        if c_pref == 19 || matches!(&r_pref, RegprefixResult::Failed(f) if is_etoobig(&f.message)) {
            unsafe { pg_diff_regfree() };
            return;
        }
    }
    match r_pref {
        RegprefixResult::NoMatch => {
            assert_eq!(c_pref, 1, "PREFIX DIVERGENCE: Rust NoMatch, C {} (pattern {:x?}, cflags {:o})", c_pref, pat, cflags);
        }
        RegprefixResult::Prefix(v) => {
            assert_eq!(c_pref, -1, "PREFIX DIVERGENCE: Rust Prefix, C {} (pattern {:x?}, cflags {:o})", c_pref, pat, cflags);
            cmp_prefix(&v, &c_prefix, c_plen, &pat, cflags);
        }
        RegprefixResult::Exact(v) => {
            assert_eq!(c_pref, -2, "PREFIX DIVERGENCE: Rust Exact, C {} (pattern {:x?}, cflags {:o})", c_pref, pat, cflags);
            cmp_prefix(&v, &c_prefix, c_plen, &pat, cflags);
        }
        RegprefixResult::Failed(f) => {
            assert!(
                c_pref != 1 && c_pref != -1 && c_pref != -2,
                "PREFIX DIVERGENCE: Rust failed ({:?}), C {} (pattern {:x?}, cflags {:o})",
                f.message, c_pref, pat, cflags
            );
            assert_eq!(f.message, c_regerror(c_pref), "PREFIX ERROR-PLANE DIVERGENCE (pattern {:x?}, cflags {:o})", pat, cflags);
        }
    }

    // ---- export plane (regexport.c / pg_trgm consumer surface) ----
    let re_t = compiled
        .engine
        .downcast_ref::<RegexT>()
        .expect("engine carrier is regex_core::RegexT");
    let r_exp = rust_export(re_t);
    let mut c_exp = vec![0i32; EXPORT_CAP];
    let c_n = unsafe { pg_diff_reg_export(c_exp.as_mut_ptr(), EXPORT_CAP as i32) };
    assert!(c_n >= 0, "C export with no compiled RE");
    assert_eq!(
        &r_exp[..],
        &c_exp[..c_n as usize],
        "EXPORT-PLANE DIVERGENCE (pattern {:x?}, cflags {:o})",
        pat, cflags
    );

    unsafe { pg_diff_regfree() };
    // Rust free path through the shipped seam entry (pg_regfree = Rc drop).
    regex_core::regex_export_free_error::seam_pg_regfree(compiled);
}

fn cmp_prefix(v: &mcx::PgVec<'_, PgWChar>, c_prefix: &[u32], c_plen: i32, pat: &[u32], cflags: i32) {
    assert_eq!(
        v.len(),
        c_plen as usize,
        "PREFIX LENGTH DIVERGENCE (pattern {:x?}, cflags {:o})",
        pat, cflags
    );
    let take = v.len().min(c_prefix.len());
    assert_eq!(
        &v[..take],
        &c_prefix[..take],
        "PREFIX CONTENT DIVERGENCE (pattern {:x?}, cflags {:o})",
        pat, cflags
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a driver input: cflags byte, start_sel, nmatch_sel, pattern, subject.
    fn input(cflags: u8, start: u8, nm: u8, pat: &str, subj: &str) -> Vec<u8> {
        let mut v = vec![cflags, start, nm, pat.len() as u8];
        v.extend_from_slice(pat.as_bytes());
        v.extend_from_slice(subj.as_bytes());
        v
    }

    const ADV: u8 = 0o3; // REG_ADVANCED
    const ADV_ICASE: u8 = 0o13;

    #[test]
    fn smoke_literal_and_classes() {
        for (p, s) in [
            ("abc", "xabcy"),
            ("a.c", "abc"),
            ("^a+b*$", "aaab"),
            ("[[:alpha:]]+", "abc123"),
            ("[a-m]+", "xyzmma"),
            ("(a)(b)?(c)", "ac"),
            ("a{2,4}", "aaaaa"),
            ("(?:ab|cd)+", "abcdab"),
            ("\\d+\\s\\w+", "42 abc"),
            ("a(?=b)", "ab"),
            ("(a|b)\\1", "aa"),
            ("x", ""),
        ] {
            for nm in 0..4u8 {
                for st in 0..3u8 {
                    regex_diff(&input(ADV, st, nm, p, s));
                    regex_diff(&input(ADV_ICASE, st, nm, p, s));
                }
            }
        }
    }

    #[test]
    fn smoke_error_patterns() {
        for p in [
            "(", ")", "[", "a{", "a{2,1}", "*a", "a\\", "(?P<x>a)", "a**",
            "[[:bogus:]]", "\\m\\M\\y\\Y", "x{100000}", "((((((((((a))))))))))",
        ] {
            regex_diff(&input(ADV, 0, 1, p, "test"));
            regex_diff(&input(0o0, 0, 1, p, "test")); // BASIC
            regex_diff(&input(0o1, 0, 1, p, "test")); // EXTENDED
            regex_diff(&input(0o4, 0, 1, p, "test")); // QUOTE
        }
    }

    #[test]
    fn smoke_flag_matrix() {
        // every low cflags bit combination over a flag-sensitive pattern
        for cf in 0..=255u8 {
            regex_diff(&input(cf, 0, 2, "^a.(b|c)$", "a\nxb"));
        }
    }

    #[test]
    fn smoke_high_codepoints() {
        // bytes >= 0x80 map through the cp table (MAX_SIMPLE_CHR straddles)
        let mut v = vec![ADV, 0, 2, 4, b'[', 0x8C, b'-', 0x8F];
        v.push(b']');
        v.extend_from_slice(&[0x8C, 0x8D, 0x8E, 0x8F, b'a']);
        // note: plen=4 so pattern is "[" 0x8C "-" 0x8F and "]" leads the subject;
        // intentionally also fuzzes the unbalanced-bracket error path.
        regex_diff(&v);
        let mut w = vec![ADV, 0, 2, 5, b'[', 0x8C, b'-', 0x90, b']'];
        w.extend_from_slice(&[0x8C, 0x8D, 0x8E, 0x8F, b'a']);
        regex_diff(&w);
        // dot over astral subjects
        let mut x = vec![ADV_ICASE, 0, 2, 2, b'.', b'+'];
        x.extend_from_slice(&[0x98, 0x99, 0x9A, 0x9B, 0x80, 0x81]);
        regex_diff(&x);
    }

    #[test]
    fn regression_etoobig_accounting() {
        // fuzz-found 2026-07-31 (first artifact of the target): C oracle at
        // the bench rig's 100kB stack budget fake-fired REG_ETOOBIG where
        // real postgres:18.3 compiles fine; fix = 2048kB server-default
        // budgets BOTH sides + the is_etoobig stack-band carve. The same
        // investigation found and fixed regex_core's compile-space
        // accounting (per-NFA/per-element/Rust-sizeof -> C-exact per-vars
        // batch charging). pattern ^((f*)\0{7}4(￿*).|){96}fo, ADVANCED|NOSUB
        let mut v = vec![0o23u8, 0, 2, 27];
        v.extend_from_slice(&[0x5e, 0x28, 0x28, 0x66, 0x2a, 0x29]);
        v.extend_from_slice(&[0x93, 0x93, 0x93, 0x93, 0x93, 0x93, 0x93]); // cp(0x93)=0 (NUL)
        v.extend_from_slice(&[0x34, 0x28, 0x97, 0x2a, 0x29, 0x2e, 0x7c, 0x29]);
        v.extend_from_slice(&[0x7b, 0x39, 0x36, 0x7d, 0x66, 0x6f]);
        v.extend_from_slice(b"fo");
        regex_diff(&v);
    }

    /// Deterministic guard for the spaceused mechanism the fuzz carve
    /// (is_etoobig) would otherwise mask: both engines must agree verdict-
    /// for-verdict on a ladder crossing REG_MAX_COMPILE_SPACE. The ladder
    /// shape is breadth-heavy (quantified alternation nests), the same
    /// family ground-truthed on postgres:18.3 ("regular expression is too
    /// complex" at the triple-nest rung).
    #[test]
    fn etoobig_spaceused_parity() {
        // 64MB thread: cargo-test threads get 2MB — exactly the guard
        // budget — so the guarded recursion needs real headroom beneath it
        // (libFuzzer's main thread has 8MB; here the ladder is deeper).
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(etoobig_spaceused_parity_body)
            .unwrap()
            .join()
            .unwrap();
    }

    fn etoobig_spaceused_parity_body() {
        use regex_core::regex_export_free_error::seam_pg_regcomp;
        let _oracle = crate::oracle_serial(); // guard held on the EXECUTING (spawned) thread
        init(); // arm BOTH stack guards on this thread (2048kB budgets)
        for p in [
            "((w*)x*y*z*.|){30}a".to_string(),
            "(((w*)x*y*z*.|){30}a|){30}b".to_string(),
            "((((w*)x*y*z*.|){30}a|){30}b|){30}c".to_string(),
        ] {
            let pw: Vec<u32> = p.chars().map(|c| c as u32).collect();
            let mut c_nsub = 0i32;
            let c = unsafe { pg_diff_regcomp(pw.as_ptr(), pw.len() as i32, 0o3, &mut c_nsub) };
            let r = seam_pg_regcomp(&pw, 0o3, types_core::C_COLLATION_OID).unwrap();
            let (r_ok, r_msg) = match &r {
                regex::RegcompResult::Compiled(_) => (true, String::new()),
                regex::RegcompResult::Failed(f) => (false, f.message.clone()),
            };
            eprintln!("rung len={}: C={} Rust={}", p.len(), c, if r_ok { "OK" } else { "ERR" });
            assert_eq!(
                c == 0,
                r_ok,
                "spaceused-parity DIVERGENCE at {p:?}: C={} ({}) Rust={}",
                c,
                c_regerror(c),
                if r_ok { "OK".into() } else { r_msg.clone() }
            );
            if !r_ok {
                assert_eq!(r_msg, c_regerror(c), "error-plane at {p:?}");
            }
            unsafe { pg_diff_regfree() };
        }
    }


    // -----------------------------------------------------------------
    // EXHAUSTIVE-DIFF (a0 route, phase1-routes.tsv regex/regex_core):
    // pg_wc_isclass family + pg_wc_toupper/tolower, FULL CODESPACE
    // (0..=0x10FFFF) x {C, builtin-posix, builtin-full} strategies,
    // shipped Rust regex_locale.rs vs the standalone verbatim
    // regc_pg_locale.c probe (real unicode tables — see
    // pg_regexfam_locale.c). LIBC/ICU strategies carved (locale FFI).
    // Total over the domain: 3 x 0x110000 codepoints x 12 planes.
    // Heavy under `cargo test` debug; run release:
    //   cargo test -p decoder_fuzz --release exhaustive_wc -- --ignored --nocapture
    // -----------------------------------------------------------------
    extern "C" {
        fn pg_diff_locale_set(collid: u32);
        fn pg_diff_wc_class_mask(c: u32) -> u32;
        fn pg_diff_wc_toupper(c: u32) -> u32;
        fn pg_diff_wc_tolower(c: u32) -> u32;
    }

    fn rust_wc_class_mask(c: u32) -> u32 {
        use regex_core::regex_locale as rl;
        let mut m = 0u32;
        m |= (rl::pg_wc_isdigit(c) as u32) << 0;
        m |= (rl::pg_wc_isalpha(c) as u32) << 1;
        m |= (rl::pg_wc_isalnum(c) as u32) << 2;
        m |= (rl::pg_wc_isword(c) as u32) << 3;
        m |= (rl::pg_wc_isupper(c) as u32) << 4;
        m |= (rl::pg_wc_islower(c) as u32) << 5;
        m |= (rl::pg_wc_isgraph(c) as u32) << 6;
        m |= (rl::pg_wc_isprint(c) as u32) << 7;
        m |= (rl::pg_wc_ispunct(c) as u32) << 8;
        m |= (rl::pg_wc_isspace(c) as u32) << 9;
        m
    }

    fn install_builtin_collation_rows() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
            mbutils_seams::get_database_encoding::set(|| 6 /* PG_UTF8 */);
            syscache_seams::lookup_pg_collation_locale_row::set(|mcx, collid| {
                // the probe TU's two synthetic builtin rows (see
                // pg_regexfam_locale.c): 61001 C.UTF-8, 61002 PG_UNICODE_FAST
                let locstr = match collid {
                    61001 => "C.UTF-8",
                    61002 => "PG_UNICODE_FAST",
                    _ => return Ok(None),
                };
                let mut collname = types_tuple::NameData::default();
                collname.namestrcpy("lanew_sweep");
                Ok(Some(syscache_seams::PgCollationLocaleRow {
                    collname,
                    collnamespace: 11,
                    collprovider: pg_locale::COLLPROVIDER_BUILTIN,
                    collisdeterministic: true,
                    collencoding: -1,
                    collcollate: None,
                    collctype: None,
                    colllocale: Some(mcx::PgString::from_str_in(locstr, mcx)?),
                    collicurules: None,
                    collversion: None,
                }))
            });
        });
    }

    #[test]
    #[ignore = "full-codespace sweep; run release with --ignored (route a0 evidence)"]
    fn exhaustive_wc_class_case_sweep() {
        let _oracle = crate::oracle_serial();
        install_builtin_collation_rows();
        let t0 = std::time::Instant::now();
        let mut compares: u64 = 0;
        for (oid, label) in [
            (types_core::C_COLLATION_OID, "C"),
            (61001u32, "builtin-posix"),
            (61002u32, "builtin-full"),
        ] {
            unsafe { pg_diff_locale_set(oid) };
            let cx = mcx::MemoryContext::new("wc_sweep");
            regex_core::regex_locale::pg_set_regex_collation(cx.mcx(), oid)
                .unwrap_or_else(|e| panic!("rust pg_set_regex_collation({label}): {e:?}"));
            for c in 0u32..=0x10FFFF {
                let cm = unsafe { pg_diff_wc_class_mask(c) };
                let rm = rust_wc_class_mask(c);
                assert_eq!(rm, cm, "CLASS-MASK DIVERGENCE strategy={label} c=U+{c:04X}");
                let cu = unsafe { pg_diff_wc_toupper(c) };
                let ru = regex_core::regex_locale::pg_wc_toupper(c);
                assert_eq!(ru, cu, "TOUPPER DIVERGENCE strategy={label} c=U+{c:04X}");
                let cl = unsafe { pg_diff_wc_tolower(c) };
                let rl_ = regex_core::regex_locale::pg_wc_tolower(c);
                assert_eq!(rl_, cl, "TOLOWER DIVERGENCE strategy={label} c=U+{c:04X}");
                compares += 12;
            }
            eprintln!("strategy {label}: full codespace green");
        }
        eprintln!(
            "exhaustive_wc_class_case_sweep: {} compares in {:?} (host: laptop)",
            compares,
            t0.elapsed()
        );
    }

    /// Fast companion to exhaustive_regerror_full_i32 (which is the banked
    /// full-domain evidence): every table code, the BSD-vestige ATOI/ITOA
    /// arms, and unknown codes both signs — runs in CI and carries the
    /// line-coverage credit for the same paths.
    #[test]
    fn regerror_representative() {
        let _oracle = crate::oracle_serial();
        init();
        for code in (-40..=40).chain([100, 101, 102, 103, i32::MIN, i32::MAX]) {
            assert_eq!(
                regex_core::regex_export_free_error::pg_regerror(code),
                c_regerror(code),
                "REGERROR DIVERGENCE code={code}"
            );
        }
    }

    // EXHAUSTIVE-DIFF (a0): pg_regerror over the FULL i32 code domain, with
    // the empty-buffer convention for the BSD-vestige REG_ATOI/REG_ITOA
    // input-reading arms (PostgreSQL never calls them; the Rust port
    // hard-codes exactly the empty-input rendering, which this sweep pins).
    // Run release with --ignored; ~4.3G formatted compares.
    #[test]
    #[ignore = "full-i32 sweep; run release with --ignored (route a0 evidence)"]
    fn exhaustive_regerror_full_i32() {
        let _oracle = crate::oracle_serial();
        init();
        let t0 = std::time::Instant::now();
        let mut n: u64 = 0;
        let mut code = i32::MIN;
        loop {
            let c_msg = c_regerror(code);
            let r_msg = regex_core::regex_export_free_error::pg_regerror(code);
            assert_eq!(r_msg, c_msg, "REGERROR DIVERGENCE code={code}");
            n += 1;
            if code == i32::MAX {
                break;
            }
            code += 1;
        }
        eprintln!("exhaustive_regerror_full_i32: {n} compares in {:?}", t0.elapsed());
    }

    /// p1-laneag banked engine finding: the Rust engine reported
    /// REG_ETOOBIG on this nested-bounded-quantifier pattern where both
    /// the vendored 18.3 C engine and real postgres:18.3 (Docker replay,
    /// 2026-07-31) compile it fine. Pattern chrs (ASCII bytes incl. 0x03):
    /// `(\\w(\yC(|){16}){16}+){16}+`-shaped, 29 chrs. Runs on a 64MB
    /// thread: the unfixed engine also recursed deep enough to overflow
    /// the 2MB test stack before reporting the error.
    #[test]
    fn etoobig_nested_bounded_quantifier_witness() {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let _oracle = crate::oracle_serial(); // guard on the EXECUTING (spawned) thread
                init();
                // Widen the Rust budget past the debug-build frame inflation:
                // this witness pins compile-space parity, not the ratified
                // stack-band asymmetry (debug frames are ~10x; at 2048kB the
                // guard fires on the Rust side only under cargo test's debug
                // profile, which is the is_etoobig carve's class, not ours).
                stack_depth::set_max_stack_depth(30720);
                stack_depth::assign_max_stack_depth(30720);
                let pat: Vec<u32> = vec![
                    40, 92, 92, 119, 40, 92, 121, 67, 40, 124, 41, 123, 49, 54, 125, 41,
                    123, 49, 54, 125, 3, 43, 41, 123, 49, 54, 125, 3, 43,
                ];
                let mut c_nsub = 0i32;
                let c =
                    unsafe { pg_diff_regcomp(pat.as_ptr(), pat.len() as i32, 0o3, &mut c_nsub) };
                let r = seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap();
                let (r_ok, r_msg) = match &r {
                    RegcompResult::Compiled(_) => (true, String::new()),
                    RegcompResult::Failed(f) => (false, f.message.clone()),
                };
                assert_eq!(
                    c == 0,
                    r_ok,
                    "ETOOBIG witness DIVERGENCE: C={} ({}) Rust={}",
                    c,
                    c_regerror(c),
                    if r_ok { "OK".into() } else { r_msg.clone() }
                );
                if !r_ok {
                    assert_eq!(r_msg, c_regerror(c), "error-plane");
                }
                unsafe { pg_diff_regfree() };
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Guard-fires direction of the STACK_TOO_DEEP parity fix: a
    /// parse-recursion bomb must come back as a clean REG_ETOOBIG from the
    /// armed stack guard (C regcomp.c subre()), not a process-killing
    /// stack overflow (the p1-laneag ASan artifact: rstacktoodeep()==0 and
    /// the NFA walks unguarded). Threshold parity with C is a ratified
    /// non-surface (frame sizes differ; the fuzz driver's is_etoobig
    /// carve) so only the Rust side's verdict is pinned here. 64MB thread
    /// with the 2048kB budget: the guard must fire long before the real
    /// stack ends.
    #[test]
    fn stack_guard_fires_cleanly_on_paren_bomb() {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                // init() touches pg_diff_regex_init even though the pinned
                // verdict here is Rust-side only; guard on the EXECUTING
                // (spawned) thread.
                let _oracle = crate::oracle_serial();
                init();
                let n = 200_000;
                let mut pat: Vec<u32> = Vec::with_capacity(2 * n + 1);
                pat.extend(std::iter::repeat(b'(' as u32).take(n));
                pat.push(b'a' as u32);
                pat.extend(std::iter::repeat(b')' as u32).take(n));
                match seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap() {
                    RegcompResult::Compiled(_) => {
                        panic!("paren bomb compiled: guard did not fire")
                    }
                    RegcompResult::Failed(f) => assert_eq!(
                        f.message, "regular expression is too complex",
                        "expected REG_ETOOBIG from the stack guard"
                    ),
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// p1-laneag CI cluster crasher class (7 debug_assert artifacts): internal
    /// RegResult failures (spaceused REG_ETOOBIG from the NFA layer, etc.)
    /// were converted to `return None` in the parse layer WITHOUT C's VERR
    /// side effects (record first error + force nexttype=EOS), so
    /// pg_regcomp's C-invariant asserts fired in debug and v.tree.unwrap()
    /// panicked in release (process abort, SQL-reachable). Pattern below is
    /// the DUMP_PAT extraction from banked unit crash-3e775e18 (nested
    /// {62}{62}{16} quantifiers driving compile-space exhaustion mid-parse).
    /// Both engines must now agree it fails cleanly with the same message.
    #[test]
    fn error_funnel_records_nfa_layer_failures() {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let _oracle = crate::oracle_serial(); // guard on the EXECUTING (spawned) thread
                init();
                let pat: Vec<u32> = vec![
                    40, 78, 92, 40, 46, 2, 3, 84, 105, 78, 78, 110, 37, 67, 67, 122, 67,
                    67, 67, 65, 67, 40, 92, 121, 46, 67, 67, 67, 67, 40, 39, 92, 121, 46,
                    124, 41, 123, 54, 50, 125, 124, 46, 124, 41, 123, 54, 50, 125, 124,
                    124, 58, 92, 119, 43, 118, 80, 41, 123, 49, 54, 125, 105, 92,
                ];
                let mut c_nsub = 0i32;
                let c =
                    unsafe { pg_diff_regcomp(pat.as_ptr(), pat.len() as i32, 0o3, &mut c_nsub) };
                let r = seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap();
                match &r {
                    RegcompResult::Compiled(_) => {
                        assert_eq!(c, 0, "C failed ({}) where Rust compiled", c_regerror(c))
                    }
                    RegcompResult::Failed(f) => {
                        assert_ne!(c, 0, "Rust failed ({}) where C compiled", f.message);
                        assert_eq!(f.message, c_regerror(c), "error-plane");
                    }
                }
                unsafe { pg_diff_regfree() };
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// CI replay rail (done-gate item 4): replay the deterministic hand
    /// seeds (seed-*) committed in corpus/regex_diff. The full corpus is
    /// replayed by the CI cluster campaign and cov-export; CI replays only the
    /// named seeds because the corpus contains compile-bomb-family units
    /// (inherited-upstream pathological compile cost, minutes each
    /// instrumented).
    #[test]
    fn seed_corpus_replay() {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/regex_diff");
                let mut n = 0;
                for e in std::fs::read_dir(dir).unwrap() {
                    let p = e.unwrap().path();
                    let name = p.file_name().unwrap().to_str().unwrap().to_string();
                    if !name.starts_with("seed-") {
                        continue;
                    }
                    regex_diff(&std::fs::read(&p).unwrap());
                    n += 1;
                }
                assert!(n >= 15, "expected the committed seed-* units, saw {n}");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Timeout-artifact triage probe (timeout-as-a-plane discipline): time
    /// each engine SEPARATELY, uninstrumented, on the banked timeout unit's
    /// pattern (fuzz artifacts are instrumented builds, ~20x+ slower — the
    /// jsonpath lane's lesson). Run release with --ignored --nocapture.
    #[test]
    #[ignore = "timing probe; run release with --ignored --nocapture"]
    fn timeout_unit_attribution() {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let _oracle = crate::oracle_serial(); // guard on the EXECUTING (spawned) thread
                init();
                // pattern chrs from artifacts/regex_diff/timeout-deb6fe...
                // (layout [cflags=3][0][1][plen=63][pattern][subject])
                let unit: &[u8] = &[
                    0x03, 0x00, 0x01, 0x3f, 0x28, 0x4e, 0x5c, 0x28, 0x2e, 0x02, 0x03, 0x54,
                    0x69, 0x4e, 0x4e, 0x6e, 0x25, 0x43, 0x43, 0x7a, 0x43, 0x43, 0x43, 0x41,
                    0x43, 0x28, 0x5c, 0x79, 0x2e, 0x43, 0x43, 0x43, 0x43, 0x28, 0x27, 0x5c,
                    0x79, 0x2e, 0x7c, 0x29, 0x7b, 0x36, 0x32, 0x7d, 0x7c, 0x2e, 0x7c, 0x29,
                    0x7b, 0x36, 0x32, 0x7d, 0x7c, 0x7c, 0x3a, 0x5c, 0x77, 0x2b, 0x76, 0x50,
                    0x29, 0x61, 0x36, 0x31, 0x62, 0x69, 0x7b, 0x5c, 0x7d, 0x63,
                ];
                let plen = unit[3] as usize;
                let pat = decode_wchars(&unit[4..4 + plen]);
                let t0 = std::time::Instant::now();
                let mut c_nsub = 0i32;
                let c =
                    unsafe { pg_diff_regcomp(pat.as_ptr(), pat.len() as i32, 0o3, &mut c_nsub) };
                let t_c = t0.elapsed();
                unsafe { pg_diff_regfree() };
                let t1 = std::time::Instant::now();
                let r = seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap();
                let t_r = t1.elapsed();
                let r_ok = matches!(r, RegcompResult::Compiled(_));
                eprintln!(
                    "timeout-unit attribution: C compile {:?} (rc={c}) / Rust compile {:?} (ok={r_ok}) / ratio {:.2}x",
                    t_c, t_r,
                    t_r.as_secs_f64() / t_c.as_secs_f64().max(1e-9)
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn smoke_prefix_shapes() {
        // exact / prefix / no-prefix shapes for the regprefix plane
        for p in ["abc", "abc.*", "abc|abd", "a|b", ".*", "^fixed$", "(ab)+c"] {
            regex_diff(&input(ADV, 0, 1, p, "abcd"));
        }
    }
}
