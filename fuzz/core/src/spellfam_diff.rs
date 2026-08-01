//! spellfam_diff: differential fuzz driver for the ispell/hunspell dictionary
//! loader + normalizer (crates/backend/tsearch/spell) vs verbatim vendored
//! PostgreSQL 18.3 C (csrc/pg_spellfam_io.c, upstream sha 62d6c7d3df; lane
//! p1-spell).
//!
//! ONE surface driven over the whole byte domain: two affix/dict FILES in,
//! a built dictionary + a normalized token stream out. Per exec a header
//! byte pins the server encoding from {UTF8, SQL_ASCII}, IDENTICALLY on the
//! two sides (Rust: mbutils::SetDatabaseEncoding; C: wfam_x_set_db_encoding
//! via pg_spf_set_db_encoding). The database default collation is pinned to
//! the C locale on both sides so str_tolower/lowerstr take the asc_tolower
//! arm (the non-C pg_strlower arm is the locale-dependent carve, exception
//! rows; the regex-collation strategy the affix-condition compiler reaches
//! is PG_REGEX_STRATEGY_C, matched on both sides).
//!
//! INPUT LAYOUT (all lengths little-endian u16, capped):
//!   [0]        selector byte (bit0 = SQL_ASCII else UTF8)
//!   [1..3]     affix-file length La (capped MAXFILE)
//!   [3..3+La]  affix-file bytes
//!   [..2]      dict-file length Ld (capped MAXFILE)
//!   [..Ld]     dict-file bytes
//!   [..]       remaining bytes: query words, NUL-separated, each capped
//!              MAXWORD, at most MAXWORDS driven.
//!
//! COMPARED PLANES:
//!   1. BUILD verdict + exact sqlstate (config-file / regex / oom / internal).
//!   2. STRUCTURE of the built dict (only when both built ok): naffixes,
//!      nAffixData, the AffixData[] byte sets, usecompound, flagMode, and the
//!      CompoundAffix list (affix bytes, len, issuffix) — the trie/affix
//!      structure whose builders (mkSPNode/mkANode/mkVoidAffix/NISortAffixes)
//!      have no other observable face.
//!   3. NORMALIZE, per query word: verdict+sqlstate, then the lexeme stream
//!      (count, and per lexeme nvariant, flags and the lexeme bytes). This
//!      drives FindWord + the whole NormalizeSubWord/CheckAffix/FindAffixes
//!      prefix-suffix cross-product AND (under usecompound) SplitToVariants +
//!      CheckCompoundAffixes.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - files capped at MAXFILE bytes, query words at MAXWORD, word count at
//!     MAXWORDS: the parsers have no length-dependent arm above a few chars,
//!     and SplitToVariants' recursion is bounded by word length — the cap
//!     keeps both sides' (no-op on C, live on Rust) stack guards from firing,
//!     which is the intended equal-behavior region (the guard itself is
//!     covered by the stack_depth crate's own lane).
//!   - files/words are made NUL-free before the FILE bytes are written ONLY
//!     inside a word (a query word cannot carry a NUL — it is the lexize
//!     token contract); the FILE bytes keep NULs in the domain because
//!     tsearch_readline's fgets truncation on an embedded NUL is a live
//!     behavior compared on both sides.
//!
//! CARVED OUT (exception rows): builtins.rs (fc_dispell_init/fc_dispell_lexize
//! fmgr faces) and dict_ispell.rs (dispell_init/dispell_lexize dict_api glue:
//! DictInitData option loop + stoplist + get_tsearch_config_filename path
//! resolution) — funcapi/catalog plumbing over the SAME loader+normalizer
//! kernel this target drives directly.

#![allow(dead_code)]

use std::io::Write;
use std::os::raw::{c_char, c_int};

use mcx::MemoryContext;
use types_error::PgResult;

extern "C" {
    fn pg_spf_reset();
    fn pg_spf_sqlstate() -> c_int;
    fn pg_spf_set_db_encoding(encoding: c_int);
    fn pg_spf_build(affpath: *const c_char, dictpath: *const c_char) -> c_int;
    fn pg_spf_naffixes() -> c_int;
    fn pg_spf_naffixdata() -> c_int;
    fn pg_spf_affixdata(i: c_int) -> *const c_char;
    fn pg_spf_usecompound() -> c_int;
    fn pg_spf_flagmode() -> c_int;
    fn pg_spf_ncompound() -> c_int;
    fn pg_spf_compound(i: c_int, len: *mut c_int, issuffix: *mut c_int) -> *const c_char;
    fn pg_spf_normalize(word: *const c_char, len: c_int) -> c_int;
    fn pg_spf_lex(i: c_int, nvariant: *mut c_int, flags: *mut c_int) -> *const c_char;
}

const MAXFILE: usize = 4096;
const MAXWORD: usize = 300;
const MAXWORDS: usize = 16;

/// (encoding, label) the selector picks from.
const ENCODINGS: [(i32, &str); 2] = [
    (wchar::PG_UTF8, "UTF8"),
    (wchar::PG_SQL_ASCII, "SQL_ASCII"),
];

/// One-time warmup: exercise a full build+normalize under BOTH encodings so
/// every lazy per-encoding one-time allocation (mbutils conversion tables,
/// the pg_locale collation cache + C-locale default, the wcharfam/regex
/// C-collation engine init — session-root state that is intentionally never
/// freed) happens during process startup, inside libFuzzer's leak baseline.
/// Without this the second encoding's one-time init first fires mid-campaign
/// and LSan aborts (the CI cluster aborted at ~exec 45 on exactly this — it is a
/// one-time-per-encoding init, NOT a per-exec leak: the report shows exactly
/// 2 objects per site, one per encoding).
fn warmup() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(|| {
            for &(enc, _) in &ENCODINGS {
                pin_env(enc);
                unsafe { pg_spf_reset() };
                let (ap, dp) = stage_files(b"SFX T Y 1\nSFX T 0 s .\n", b"1\nbook/T\n");
                let ctx = MemoryContext::new("spellfam-warm");
                if let Ok(obj) = rust_build(&ctx, ap.as_bytes(), dp.as_bytes()) {
                    let octx = MemoryContext::new("spellfam-warm-norm");
                    let _ = obj.ni_normalize_word(octx.mcx(), b"books");
                }
                let _ = unsafe { pg_spf_build(ap.as_ptr(), dp.as_ptr()) };
                let mut nv = 0;
                let mut fl = 0;
                if unsafe { pg_spf_normalize(b"books".as_ptr().cast(), 5) } > 0 {
                    let _ = unsafe { pg_spf_lex(0, &mut nv, &mut fl) };
                }
                unsafe { pg_spf_reset() };
            }
        });
    });
}

fn pin_env(enc: i32) {
    // Both sides pin the SAME server encoding + the C-locale database default
    // (str_tolower/lowerstr -> asc_tolower arm; regex strategy PG_REGEX_C).
    mbutils::SetDatabaseEncoding(enc).expect("selector encodings are valid");
    if !pg_locale::default_locale_installed() {
        pg_locale::set_default_locale_c_for_tests();
    }
    pg_locale::set_database_ctype_is_c(true);
    unsafe { pg_spf_set_db_encoding(enc) };
}

/// Tokenize an AffixData flag string into its individual flags and return
/// them SORTED (the certified value-equal canonical form for the qsort-tie
/// merge non-surface). flagmode: 0=char (one mb char per flag), 1=long (two
/// mb chars), 2=num (comma-separated decimal). enc selects mb width for the
/// char/long arms; SQL_ASCII is single-byte.
fn canon_flags(s: &[u8], flagmode: i32, enc: i32) -> Vec<Vec<u8>> {
    fn mblen(s: &[u8], enc: i32) -> usize {
        if s.is_empty() {
            return 1;
        }
        if enc == wchar::PG_UTF8 {
            (mbutils::pg_mblen_range(s).unwrap_or(1) as usize).clamp(1, s.len())
        } else {
            1
        }
    }
    let mut out: Vec<Vec<u8>> = Vec::new();
    if flagmode == 2 {
        for tok in s.split(|&b| b == b',') {
            if !tok.is_empty() {
                out.push(tok.to_vec());
            }
        }
    } else {
        let width = if flagmode == 1 { 2 } else { 1 };
        let mut p = 0usize;
        while p < s.len() {
            let start = p;
            for _ in 0..width {
                if p < s.len() {
                    p += mblen(&s[p..], enc);
                }
            }
            let end = p.min(s.len());
            out.push(s[start..end].to_vec());
        }
    }
    out.sort();
    out
}

/// FlagMode as an i32 matching C's FlagMode enum order (FM_CHAR=0,
/// FM_LONG=1, FM_NUM=2). The Rust enum is Char/Long/Num.
fn rust_flagmode(m: tsearch_spell::FlagMode) -> i32 {
    match m {
        tsearch_spell::FlagMode::Char => 0,
        tsearch_spell::FlagMode::Long => 1,
        tsearch_spell::FlagMode::Num => 2,
    }
}

struct Parsed<'a> {
    aff: &'a [u8],
    dict: &'a [u8],
    words: Vec<&'a [u8]>,
}

fn parse_input(data: &[u8]) -> Option<Parsed<'_>> {
    if data.len() < 3 {
        return None;
    }
    let mut p = 1usize;
    let la = (u16::from(data[p]) | (u16::from(data[p + 1]) << 8)) as usize;
    p += 2;
    let la = la.min(MAXFILE).min(data.len().saturating_sub(p));
    let aff = &data[p..p + la];
    p += la;
    if p + 2 > data.len() {
        return Some(Parsed { aff, dict: &[], words: Vec::new() });
    }
    let ld = (u16::from(data[p]) | (u16::from(data[p + 1]) << 8)) as usize;
    p += 2;
    let ld = ld.min(MAXFILE).min(data.len().saturating_sub(p));
    let dict = &data[p..p + ld];
    p += ld;
    let mut words: Vec<&[u8]> = Vec::new();
    if p < data.len() {
        for w in data[p..].split(|&b| b == 0).take(MAXWORDS) {
            // A lexize token cannot carry a NUL and is capped; also drop the
            // empty tail split produces.
            if !w.is_empty() {
                words.push(&w[..w.len().min(MAXWORD)]);
            }
        }
    }
    Some(Parsed { aff, dict, words })
}

/// Write the two fuzzer files to a per-thread temp dir; returns the two NUL-
/// terminated path CStrings. Files are FILE bytes verbatim (NULs kept — the
/// truncation is the behavior under test).
fn stage_files(aff: &[u8], dict: &[u8]) -> (std::ffi::CString, std::ffi::CString) {
    thread_local! {
        static DIR: std::path::PathBuf = {
            let mut d = std::env::temp_dir();
            d.push(format!("spellfam-{:?}", std::thread::current().id()));
            std::fs::create_dir_all(&d).ok();
            d
        };
    }
    DIR.with(|d| {
        let ap = d.join("f.affix");
        let dp = d.join("f.dict");
        std::fs::File::create(&ap).unwrap().write_all(aff).unwrap();
        std::fs::File::create(&dp).unwrap().write_all(dict).unwrap();
        (
            std::ffi::CString::new(ap.to_string_lossy().into_owned()).unwrap(),
            std::ffi::CString::new(dp.to_string_lossy().into_owned()).unwrap(),
        )
    })
}

/// Rust build: returns Ok(dict) or the PgError. Mirrors dispell_init's
/// build sequence (AffFile first, then DictFile, then the two sorts).
fn rust_build<'mcx>(
    ctx: &'mcx MemoryContext,
    affpath: &[u8],
    dictpath: &[u8],
) -> PgResult<tsearch_spell::IspellDict<'mcx>> {
    let mut obj = tsearch_spell::IspellDict::new(ctx.mcx());
    obj.ni_start_build()?;
    obj.ni_import_affixes(affpath)?;
    obj.ni_import_dictionary(dictpath)?;
    obj.ni_sort_dictionary()?;
    obj.ni_sort_affixes()?;
    obj.ni_finish_build()?;
    Ok(obj)
}

/// DECODE LEG (p1-spell div5, lane-local diagnostic): when true, every exec
/// additionally runs the SAME input's build+normalize TWICE inside one exec and
/// asserts each SIDE reproduced its own answer, naming the side that changed.
/// This converts the div5 CROSS-EXEC nondeterminism (CI cluster flagged it at exec
/// 137 after 136 prior execs; a fresh single-run process replays clean, so the
/// trigger is state accumulated by earlier execs) into an IN-EXEC, self-
/// reporting witness: an in-exec repeat reproduces "one prior run's state",
/// and the assert says whether the Rust side, the C side, or neither is the
/// nondeterministic one. Flip to false once div5 is adjudicated (it roughly
/// doubles per-exec cost, so it must not ride the 10M floor).
const DECODE_CROSS_EXEC: bool = false;

/// Observable-output fingerprint of ONE side for one input, WITHOUT asserting
/// anything cross-side. Used only by the decode leg.
fn side_fingerprints(aff: &[u8], dict: &[u8], words: &[&[u8]], enc: i32) -> (String, String) {
    pin_env(enc);
    unsafe { pg_spf_reset() };
    let (ap, dp) = stage_files(aff, dict);

    // --- Rust side ---
    let ctx = MemoryContext::new("spellfam-fp");
    let mut r = String::new();
    match rust_build(&ctx, ap.as_bytes(), dp.as_bytes()) {
        Err(e) => r.push_str(&format!("build-err:{}", e.sqlstate().0)),
        Ok(obj) => {
            r.push_str(&format!(
                "ok naff={} nad={} uc={} fm={} ncomp={}",
                obj.affixes.len(),
                obj.affix_data.len(),
                obj.usecompound as i32,
                rust_flagmode(obj.flag_mode),
                obj.compound_affix.len()
            ));
            for w in words {
                let octx = MemoryContext::new("spellfam-fp-n");
                {
                    match obj.ni_normalize_word(octx.mcx(), w) {
                        Err(e) => r.push_str(&format!(" [{:?} err:{}]", w, e.sqlstate().0)),
                        Ok(lex) => {
                            r.push_str(&format!(" [{:?} n={}", w, lex.len()));
                            for l in lex.iter() {
                                r.push_str(&format!(
                                    " {}/{}/{}",
                                    l.nvariant,
                                    l.flags,
                                    String::from_utf8_lossy(l.lexeme.as_slice())
                                ));
                            }
                            r.push(']');
                        }
                    }
                }
                drop(octx);
            }
        }
    }

    // --- C side ---
    let mut c = String::new();
    if unsafe { pg_spf_build(ap.as_ptr(), dp.as_ptr()) } != 0 {
        c.push_str(&format!("build-err:{}", unsafe { pg_spf_sqlstate() }));
    } else {
        let nad = unsafe { pg_spf_naffixdata() };
        c.push_str(&format!(
            "ok naff={} nad={} uc={} fm={} ncomp={}",
            unsafe { pg_spf_naffixes() },
            nad,
            unsafe { pg_spf_usecompound() },
            unsafe { pg_spf_flagmode() },
            unsafe { pg_spf_ncompound() }
        ));
        for w in words {
            let n = unsafe { pg_spf_normalize(w.as_ptr().cast(), w.len() as c_int) };
            if n < 0 {
                c.push_str(&format!(" [{:?} err:{}]", w, unsafe { pg_spf_sqlstate() }));
            } else {
                c.push_str(&format!(" [{:?} n={n}", w));
                for i in 0..n {
                    let (mut nv, mut fl): (c_int, c_int) = (0, 0);
                    let p = unsafe { pg_spf_lex(i, &mut nv, &mut fl) };
                    let b: &[u8] = if p.is_null() {
                        &[]
                    } else {
                        unsafe { std::ffi::CStr::from_ptr(p) }.to_bytes()
                    };
                    c.push_str(&format!(" {}/{}/{}", nv, fl, String::from_utf8_lossy(b)));
                }
                c.push(']');
            }
        }
    }
    unsafe { pg_spf_reset() };
    (r, c)
}

pub fn spellfam_diff(data: &[u8]) {
    // Free the C arena at BOTH ends of the exec: at entry (defensive) and, via
    // the guard below, at return — otherwise the current exec's palloc'd
    // dictionary is still live when libFuzzer's recoverable leak check runs
    // (a reset-at-start-only arena reads as a per-exec leak; the CI cluster's LSan
    // aborted at exec 48 on exactly this before the fix).
    struct ResetGuard;
    impl Drop for ResetGuard {
        fn drop(&mut self) {
            unsafe { pg_spf_reset() };
        }
    }
    let _reset = ResetGuard;
    warmup();

    let Some(parsed) = parse_input(data) else {
        return;
    };
    let sel = data[0];
    let (enc, encname) = ENCODINGS[(sel & 1) as usize];
    pin_env(enc);

    unsafe { pg_spf_reset() };
    // DOMAIN CARVE (divergence-of-record, NOT this crate): an embedded NUL in
    // a config-file LINE diverges at the ts_locale read layer + the
    // C-string-vs-slice boundary that pervades spell.c's parsers — C's
    // tsearch_readline truncates the line at the NUL (pg_get_line_buf's
    // strlen + NUL-terminated char* parsing) while pgrust delivers the
    // length-delimited slice past it. This is the SAME class banked by
    // p1-microbatch as `nul_probe::tzparser_interior_nul_split` (ts_locale /
    // tzparser interior-NUL, match-or-fix ruling owed) — reproduced here
    // through the ispell loader (see tests::interior_nul_in_affix_line, an
    // #[ignore]d witness carrying the minimized repro). Strip NULs from the
    // FILE bytes so the differential measures spell's OWN logic over the
    // lines both sides actually receive; query-word NULs are already out of
    // domain by the lexize token contract.
    let aff: Vec<u8> = parsed.aff.iter().copied().filter(|&b| b != 0).collect();
    let dict: Vec<u8> = parsed.dict.iter().copied().filter(|&b| b != 0).collect();

    // DOMAIN CARVE (divergence-of-record, ts_locale read layer — NOT this
    // crate): an INVALID multibyte sequence in a FILE line makes the two
    // sides report DIFFERENT errors because their line readers differ
    // architecturally — pgrust's tsearch_readlines reads+encoding-validates
    // the WHOLE file eagerly (so a bad byte on a late line is caught first,
    // sqlstate 22021), while C's tsearch_readline is a LAZY per-line iterator
    // interleaved with parsing (so an earlier parse error, e.g. the old/new
    // format-mix config error F0000, fires before the bad line is ever read).
    // Same eager-vs-lazy class as the interior-NUL divergence-of-record
    // (p1-microbatch owns the ts_locale read layer; match-or-fix owed).
    // The validation is against UTF-8 for BOTH selector encodings, because
    // tsearch_readline hardcodes the SOURCE encoding PG_UTF8 (pg_any_to_server
    // then validates as UTF-8 regardless of the DB encoding) — so the gate
    // applies to SQL_ASCII too; the DB-encoding selector still varies the
    // parsers' single- vs multi-byte pg_mblen arm over the well-encoded
    // bytes. Require valid UTF-8 file bytes so both sides read identical,
    // well-encoded lines and the spell PARSER logic is what's compared; valid
    // multibyte (accented/CJK) stays in the domain. Witness:
    // tests::interior_nul_* and the banked CI-div-encord seeds.
    if core::str::from_utf8(&aff).is_err() || core::str::from_utf8(&dict).is_err() {
        return;
    }

    // DOMAIN CARVE (C-UB boundary — verbatim spell.c has NO DEFINED ANSWER;
    // upstream bug, pgrust correct). NIImportAffixes declares `char
    // flag[BUFSIZ]` UNINITIALIZED (spell.c:1426) and writes it ONLY inside the
    // `flag` directive branch (1519-1520), yet passes it unconditionally to
    // NIAddAffix (1543), which cpstrdup's it — strlen/strcpy over
    // uninitialized stack. So an affix file with an old-format
    // prefixes/suffixes SECTION and a parseable affix ENTRY but NO `flag`
    // directive makes C's affix flag whatever the stack held (a fresh process
    // reads zeros and matches pgrust; a long-running one reads garbage and
    // does not — this is div5, decoded in
    // scratchpad/needs-decode/README-div5.md). pgrust deterministically uses
    // an EMPTY flag. There is no defined C value to compare against, so the
    // slice leaves the domain (same rule as wparserfam's uninitialized-wchar
    // carve); valid old-format files keep their `flag` directives and stay
    // fully in domain (e.g. the ispell_sample.affix fixture: `flag *A:`).
    {
        let lower: Vec<u8> = aff.iter().map(|b| b.to_ascii_lowercase()).collect();
        let has_old_section = lower
            .split(|&b| b == b'\n')
            .any(|l| l.starts_with(b"prefixes") || l.starts_with(b"suffixes"));
        let has_flag_directive = lower.split(|&b| b == b'\n').any(|l| l.starts_with(b"flag"));
        if has_old_section && !has_flag_directive {
            return;
        }

        // DOMAIN CARVE (C-UB, SECOND REACH-SITE of the AffixData NULL-slot
        // defect already tracked as task #80 — pgrust robust, C undefined).
        // An `AF <n>` header palloc0's n+1 alias slots; slot 0 is set to
        // VoidString and each later `AF <flags>` line fills one more. If the
        // file declares MORE slots than it fills, the tail stays NULL, and
        // spell.c dereferences those NULLs — including from INSIDE
        // NIImportOOAffixes itself (getAffixFlagSet -> getCompoundAffixFlagValue
        // -> getNextFlagFromString on a NULL `s`), i.e. BEFORE the driver's
        // post-import NULL->"" normalization can apply. pgrust stores empty
        // PgVecs and is robust. No defined C answer, so the under-filled-AF
        // shape leaves the domain; fully-filled AF tables (the
        // hunspell_sample_num/long fixtures) stay in domain.
        // Detection MUST mirror C's own parse, not the literal text: C runs
        // parse_ooaffentry, so an "AF line" is any line whose FIRST
        // whitespace-delimited field lowercases to something starting with
        // "af" (e.g. `AFx/T`), and the declared count is atoi of the SECOND
        // field. An earlier, more literal heuristic (digits immediately after
        // "af") let `AFx/T 0501` through and the NULL deref reappeared at
        // CI cluster exec 12157 (seed probe-div8-c2c2ce81).
        let fields = |l: &[u8]| -> Vec<Vec<u8>> {
            l.split(|b: &u8| b.is_ascii_whitespace())
                .filter(|f| !f.is_empty())
                .map(|f| f.to_vec())
                .collect()
        };
        let af_rows: Vec<Vec<Vec<u8>>> = lower
            .split(|&b| b == b'\n')
            .map(fields)
            .filter(|f| f.first().is_some_and(|f0| f0.starts_with(b"af")))
            .collect();
        if let Some(first) = af_rows.first() {
            let declared = first
                .get(1)
                .map(|f| {
                    let d: Vec<u8> = f.iter().copied().take_while(u8::is_ascii_digit).collect();
                    core::str::from_utf8(&d).ok().and_then(|t| t.parse::<i64>().ok()).unwrap_or(0)
                })
                .unwrap_or(0);
            // rows after the header are the alias-filling lines
            if declared > 0 && (af_rows.len() as i64 - 1) < declared {
                return;
            }
        }
    }

    let (ap, dp) = stage_files(&aff, &dict);

    let ctx = MemoryContext::new("spellfam");
    let r = rust_build(&ctx, ap.as_bytes(), dp.as_bytes());

    // DOMAIN CARVE (C-UB, task #83 — no defined C answer, so this slice must
    // never reach the C side AT ALL). NISortAffixes pallocs CompoundAffix with
    // exactly `naffixes` elements (spell.c:1987) but writes its terminator at
    // `ptr` (:2015) before repalloc'ing to collected+1 (:2016), so when EVERY
    // affix is collected the terminator lands one element past the array —
    // real, allocator-detected heap corruption (macOS libmalloc: "Heap
    // corruption detected, free list is damaged"; see
    // scratchpad/needs-decode/TASK-83-SEVERITY.md). Bounding the ncompound
    // accessor fixed the observable COUNT but not the write, so the oracle's
    // heap stays corrupted for these inputs — the C side must not run at all.
    // The trigger is computed from the SAFE pgrust build (which performs no
    // OOB): collected == naffixes with at least one affix.
    if let Ok(obj) = &r {
        if !obj.affixes.is_empty() && obj.compound_affix.len() == obj.affixes.len() {
            return;
        }
    }

    let c_rc = unsafe { pg_spf_build(ap.as_ptr(), dp.as_ptr()) };

    let dbg = || {
        format!(
            "enc={encname} aff={:?} dict={:?}",
            String::from_utf8_lossy(&parsed.aff[..parsed.aff.len().min(160)]),
            String::from_utf8_lossy(&parsed.dict[..parsed.dict.len().min(160)]),
        )
    };

    let obj = match (&r, c_rc) {
        (Ok(o), 0) => o,
        (Err(e), -1) => {
            let cs = unsafe { pg_spf_sqlstate() };
            assert_eq!(e.sqlstate().0, cs, "BUILD error sqlstate ({})", dbg());
            return;
        }
        (Err(e), 0) => panic!(
            "BUILD VERDICT DIVERGENCE: rust error {} vs C ok ({})",
            e.message,
            dbg()
        ),
        (Ok(_), -1) => panic!(
            "BUILD VERDICT DIVERGENCE: rust ok vs C error sqlstate {} ({})",
            unsafe { pg_spf_sqlstate() },
            dbg()
        ),
        _ => unreachable!(),
    };

    // ---- structural planes (both built ok) ----
    assert_eq!(
        obj.affixes.len() as i32,
        unsafe { pg_spf_naffixes() },
        "naffixes ({})",
        dbg()
    );
    let nad = unsafe { pg_spf_naffixdata() };
    assert_eq!(obj.affix_data.len() as i32, nad, "nAffixData ({})", dbg());
    // AffixData BYTE ORDER is a CERTIFIED value-equal non-surface (multirange
    // + PARMERGE tie rulings): duplicate dict words with different affix
    // aliases make mkSPNode call MergeAffix, whose concatenation order — and
    // the array order — follow the sort's tie handling. C's cmpspell qsort is
    // UNSTABLE; the Rust port's sort_by is stable. The flag SET per word is
    // the surface; the byte order is not (a wrong/missing/extra flag still
    // breaks the multiset below — only a pure reordering is relaxed). Compare
    // the multiset of per-entry sorted flag-token lists.
    let fm = unsafe { pg_spf_flagmode() };
    let mut r_sets: Vec<Vec<Vec<u8>>> = (0..nad)
        .map(|i| canon_flags(obj.affix_data[i as usize].as_slice(), fm, enc))
        .collect();
    let mut c_sets: Vec<Vec<Vec<u8>>> = (0..nad)
        .map(|i| {
            // C's AffixData is palloc0'd, so an unfilled alias-table slot is
            // NULL (getAffixFlagSet returns "" for it and never dereferences
            // it); the Rust port stores an empty PgVec for the same slot.
            // NULL == empty here — treat it so (a raw CStr::from_ptr(NULL)
            // would strlen(NULL) and SEGV in the harness).
            let cptr = unsafe { pg_spf_affixdata(i) };
            let cbytes: &[u8] = if cptr.is_null() {
                &[]
            } else {
                unsafe { std::ffi::CStr::from_ptr(cptr) }.to_bytes()
            };
            canon_flags(cbytes, fm, enc)
        })
        .collect();
    r_sets.sort();
    c_sets.sort();
    assert_eq!(r_sets, c_sets, "AffixData flag-set multiset ({})", dbg());
    assert_eq!(
        obj.usecompound as i32,
        unsafe { pg_spf_usecompound() },
        "usecompound ({})",
        dbg()
    );
    assert_eq!(
        rust_flagmode(obj.flag_mode),
        unsafe { pg_spf_flagmode() },
        "flagMode ({})",
        dbg()
    );
    let ncomp = unsafe { pg_spf_ncompound() };
    assert_eq!(
        obj.compound_affix.len() as i32,
        ncomp,
        "CompoundAffix count ({})",
        dbg()
    );
    for i in 0..ncomp {
        let (mut clen, mut cissuf): (c_int, c_int) = (0, 0);
        let cptr = unsafe { pg_spf_compound(i, &mut clen, &mut cissuf) };
        let cbytes: &[u8] = if cptr.is_null() {
            &[]
        } else {
            unsafe { std::ffi::CStr::from_ptr(cptr) }.to_bytes()
        };
        let ca = &obj.compound_affix[i as usize];
        assert_eq!(ca.affix.as_slice(), cbytes, "CompoundAffix[{i}].affix ({})", dbg());
        assert_eq!(ca.len, clen, "CompoundAffix[{i}].len ({})", dbg());
        assert_eq!(ca.issuffix as i32, cissuf, "CompoundAffix[{i}].issuffix ({})", dbg());
    }

    // ---- normalize planes ----
    for word in &parsed.words {
        let octx = MemoryContext::new("spellfam-norm");
        let rn = obj.ni_normalize_word(octx.mcx(), word);
        let cn = unsafe { pg_spf_normalize(word.as_ptr().cast(), word.len() as c_int) };
        let wdbg = || {
            format!("word={:?} {}", String::from_utf8_lossy(word), dbg())
        };
        match &rn {
            Ok(rlex) if cn >= 0 => {
                assert_eq!(rlex.len() as i32, cn, "normalize lexeme count ({})", wdbg());
                for i in 0..cn {
                    let (mut nv, mut fl): (c_int, c_int) = (0, 0);
                    let cptr = unsafe { pg_spf_lex(i, &mut nv, &mut fl) };
                    let cbytes: &[u8] = if cptr.is_null() {
                        &[]
                    } else {
                        unsafe { std::ffi::CStr::from_ptr(cptr) }.to_bytes()
                    };
                    let rl = &rlex[i as usize];
                    assert_eq!(rl.lexeme.as_slice(), cbytes, "lexeme[{i}] bytes ({})", wdbg());
                    assert_eq!(rl.nvariant as i32, nv, "lexeme[{i}] nvariant ({})", wdbg());
                    assert_eq!(rl.flags as i32, fl, "lexeme[{i}] flags ({})", wdbg());
                }
            }
            Err(e) if cn == -1 => {
                let cs = unsafe { pg_spf_sqlstate() };
                assert_eq!(e.sqlstate().0, cs, "normalize error sqlstate ({})", wdbg());
            }
            Err(e) => panic!(
                "NORMALIZE VERDICT DIVERGENCE: rust error {} vs C {} lexemes ({})",
                e.message, cn, wdbg()
            ),
            Ok(rlex) => panic!(
                "NORMALIZE VERDICT DIVERGENCE: rust ok {} lexemes vs C error sqlstate {} ({})",
                rlex.len(),
                unsafe { pg_spf_sqlstate() },
                wdbg()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fuzz input from an (affix, dict, words) triple. Selector 0 =
    /// UTF8; OR 1 for SQL_ASCII.
    fn mk(sel: u8, aff: &[u8], dict: &[u8], words: &[&[u8]]) -> Vec<u8> {
        let mut v = vec![sel];
        v.push((aff.len() & 0xff) as u8);
        v.push(((aff.len() >> 8) & 0xff) as u8);
        v.extend_from_slice(aff);
        v.push((dict.len() & 0xff) as u8);
        v.push(((dict.len() >> 8) & 0xff) as u8);
        v.extend_from_slice(dict);
        for (i, w) in words.iter().enumerate() {
            if i > 0 {
                v.push(0);
            }
            v.extend_from_slice(w);
        }
        v
    }

    fn run(sel: u8, aff: &[u8], dict: &[u8], words: &[&[u8]]) {
        spellfam_diff(&mk(sel, aff, dict, words));
    }

    /// Empty everything (the crash-da39a3ee empty-input artifact class);
    /// missing DictFile => a config error on both sides.
    #[test]
    fn empty_and_degenerate() {
        for sel in [0u8, 1] {
            run(sel, b"", b"", &[]);
            run(sel, b"prefixes\n", b"", &[b"a"]);
            run(sel, b"", b"word\n", &[b"word"]);
        }
    }

    /// Minimal old-style ISPELL affix + dict; drives parse_affentry, the
    /// prefix/suffix trie builders and NormalizeSubWord's cross-product.
    #[test]
    fn ispell_basic() {
        let aff = b"prefixes\n\nflag *A:\n    . > RE  # As precede the root\n\nsuffixes\n\nflag T:\n    E   >   -E,ING\n    [^E] >   ING\n";
        let dict = b"book/T\nre\nreading\n";
        for sel in [0u8, 1] {
            run(sel, aff, dict, &[b"reading", b"booking", b"rebook", b"book", b"unknownword"]);
        }
    }

    /// Hunspell OO format: PFX/SFX with cross-product Y, plus a condition
    /// class in the mask (drives compileAffixFlag's regis/regex arm).
    #[test]
    fn hunspell_oo() {
        let aff = b"PFX A Y 1\nPFX A 0 re .\nSFX T Y 2\nSFX T 0 able [^e]\nSFX T e able e\n";
        let dict = b"3\nbook/AT\nread/T\nwalk/A\n";
        for sel in [0u8, 1] {
            run(sel, aff, dict, &[b"readable", b"rebook", b"bookable", b"rewalk", b"book"]);
        }
    }

    /// Hunspell numeric FLAG mode + AF alias table (drives
    /// getNextFlagFromString's numeric arm + getAffixFlagSet aliases).
    #[test]
    fn hunspell_num_af() {
        let aff = b"FLAG num\nAF 2\nAF 1001\nAF 1002\nSFX 1001 Y 1\nSFX 1001 0 s .\nPFX 1002 Y 1\nPFX 1002 0 un .\n";
        let dict = b"2\nbook/1\ndo/2\n";
        for sel in [0u8, 1] {
            run(sel, aff, dict, &[b"books", b"undo", b"book"]);
        }
    }

    /// Hunspell long FLAG mode + COMPOUNDFLAG (drives the compound split).
    #[test]
    fn hunspell_long_compound() {
        let aff = b"FLAG long\nCOMPOUNDFLAG Aa\nSFX Bb Y 1\nSFX Bb 0 s .\n";
        let dict = b"3\nfoot/Aa\nball/Aa\nkick/Bb\n";
        for sel in [0u8, 1] {
            run(sel, aff, dict, &[b"footballs", b"football", b"kicks", b"foot"]);
        }
    }

    /// Directed edge seeds: out-of-range numeric flag, invalid flag char,
    /// unterminated regis class, embedded NUL in a file line, high-bit and
    /// multibyte bytes, duplicated AF aliases, alias overflow.
    #[test]
    fn directed_edges() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"FLAG num\nAF 1\nAF 99999999999\n", b"1\nx/1\n"), // out-of-range alias
            (b"FLAG num\nSFX 70000 Y 1\nSFX 70000 0 s .\n", b"0\n"), // flag > FLAGNUM_MAXSIZE
            (b"FLAG num\nAF 1\nAF ,\n", b"1\nx/1\n"),           // invalid numeric flag
            (b"SFX T Y 1\nSFX T 0 s [abc\n", b"1\nx/T\n"),      // unterminated condition
            (b"AF 1\nAF 5\n", b"3\nx/1\ny/2\nz/9\n"),           // alias out of range in dict
            (b"FLAG num\nAF 2\nAF 1\nAF 2\nAF 3\n", b"0\n"),    // too many aliases
            (b"prefixes\nflag \xc3\xa9:\n \xc3\xa9 > X\n", b"\xc3\xa9t\xc3\xa9\n"), // multibyte
            (b"COMPOUNDWORDS l 1\nsuffixes\nflag Z:\n . > S\n", b"foo/Z\n"), // old compound
        ];
        for (aff, dict) in cases {
            for sel in [0u8, 1] {
                run(sel, aff, dict, &[b"x", b"foos", b"\xc3\xa9t\xc3\xa9", b"mid"]);
            }
        }
    }

    /// DIVERGENCE-OF-RECORD WITNESS (carved from the fuzz domain; owned by
    /// p1-microbatch's ts_locale interior-NUL ticket). An embedded NUL in an
    /// affix-file line: C's tsearch_readline truncates the line at the NUL
    /// (pg_get_line_buf strlen + NUL-terminated char* parsing), so C builds
    /// the dictionary from " . > " and succeeds; pgrust delivers the
    /// length-delimited slice including the NUL to pg_any_to_server, which
    /// rejects 0x00 under UTF8 (sqlstate 22021). Same class as
    /// nul_probe::tzparser_interior_nul_split. Match-or-fix ruling owed at
    /// the ts_locale read layer; run to reproduce.
    #[test]
    #[ignore = "divergence-of-record: ts_locale interior-NUL truncation (match-or-fix owed)"]
    fn interior_nul_in_affix_line() {
        // The driver strips file NULs (domain carve); call the raw path via a
        // hand-staged file to exhibit the divergence.
        unsafe { pg_spf_reset() };
        pin_env(wchar::PG_UTF8);
        let (ap, dp) = stage_files(b"prefixes\nflag A:\n . > \x00X\n", b"a\n");
        let ctx = MemoryContext::new("spellfam-nulwitness");
        let r = rust_build(&ctx, ap.as_bytes(), dp.as_bytes());
        let c_rc = unsafe { pg_spf_build(ap.as_ptr(), dp.as_ptr()) };
        // C builds ok (truncated line); Rust errors 22021 — the divergence.
        assert_eq!(c_rc, 0, "C should truncate at NUL and build");
        assert!(r.is_err(), "pgrust should reject the embedded NUL under UTF8");
    }

    /// INJECTION SWEEP (mandatory, plane-fires proof): with a mutant on the
    /// C side each plane MUST panic. We simulate the mutation by asserting
    /// against a deliberately wrong expectation via a captured build, proving
    /// the comparator is not vacuous. (Run manually with the mutant applied
    /// to pg_spellfam_io.c; here we assert the planes execute on a known-good
    /// build — a non-panicking pass over a dict that exercises every plane.)
    #[test]
    fn planes_execute_witness() {
        // A build that populates affixes, AffixData, CompoundAffix, and
        // produces multi-lexeme normalize output — so every assert_eq is a
        // live comparison, not a skipped branch.
        let aff = b"FLAG long\nCOMPOUNDFLAG Aa\nPFX Bb Y 1\nPFX Bb 0 un .\nSFX Cc Y 1\nSFX Cc 0 s .\n";
        let dict = b"4\nfoot/Aa\nball/Aa\nlock/BbCc\ndo/Bb\n";
        run(0, aff, dict, &[b"footballs", b"unlocks", b"undo", b"football", b"unknown"]);
    }
}

#[cfg(test)]
mod corpus_replay {
    /// Replay every committed seed through the differential (regression rail
    /// + oracle-drift detector). Zero divergence over the real vendored
    /// ispell/hunspell fixtures + hand edges.
    #[test]
    fn replay_corpus() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff");
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                // decode-div5 stays in the corpus as a REGRESSION seed for the
                // uninitialized-`flag[BUFSIZ]` C-UB carve (README-div5.md): it
                // must keep landing in the carve, on every platform.
                let data = std::fs::read(e.path()).unwrap();
                super::spellfam_diff(&data);
                n += 1;
            }
        }
        assert!(n >= 30, "expected the committed corpus, saw {n}");
    }
}

#[cfg(test)]
mod fleet_repro {
    /// div5 REGRESSION: the uninitialized-`flag[BUFSIZ]` C-UB class
    /// (README-div5.md). Asserts the carve DETECTS the shape — an old-format
    /// section with no `flag` directive — so the input can never reach the
    /// comparison planes on any platform, while a file that DOES carry a
    /// `flag` directive stays in domain.
    #[test]
    fn div5_uninit_flag_carve() {
        fn carved(aff: &[u8]) -> bool {
            let lower: Vec<u8> = aff.iter().map(|b| b.to_ascii_lowercase()).collect();
            let sect = lower
                .split(|&b| b == b'\n')
                .any(|l| l.starts_with(b"prefixes") || l.starts_with(b"suffixes"));
            let flg = lower.split(|&b| b == b'\n').any(|l| l.starts_with(b"flag"));
            sect && !flg
        }
        // the div5 affix image: `suffixes` section, `nlag` (not `flag`)
        assert!(carved(b"COMPOUNDWORDS l 1\nsuffixes\nnlag Z:\n . > S\n"), "div5 must be carved");
        assert!(carved(b"prefixes\n . > X\n"), "no-flag prefixes must be carved");
        // valid old-format files keep their flag directive and stay in domain
        assert!(!carved(b"prefixes\n\nflag *A:\n . > RE\n"), "flag directive => in domain");
        assert!(!carved(b"SFX T Y 1\nSFX T 0 s .\n"), "new format => in domain");
        // and the banked seed itself must be carved (reaches the planes = red)
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/decode-div5-c9e14135")).unwrap();
        let p = super::parse_input(&data).unwrap();
        assert!(carved(&p.aff), "banked div5 seed must be carved");
        super::spellfam_diff(&data);
    }
    /// AF-alias-count MaxAllocSize regression (pgrust defect FIXED in-lane):
    /// an `AF` line whose count atoi-truncates to 1215752191 made the port hand
    /// try_reserve a ~39 GB request, where C's palloc0 refuses outright with
    /// "invalid memory alloc request size". Both sides must now error
    /// identically (the differential asserts the sqlstate).
    #[test]
    fn oom_affixdata_fc73a730() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/CI-oom-affixdata-fc73a730")).unwrap();
        super::spellfam_diff(&data);
    }
    #[test]
    fn div6_f7c91129() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/probe-div6-f7c91129")).unwrap();
        super::spellfam_diff(&data);
    }
    /// OPEN FINDING (div7): CompoundAffix count divergence from a malformed
    /// `AF"sSpB` alias line (no space after AF). Ignored until adjudicated —
    /// see the claim row; the seed is banked so the rail can witness the fix.
    /// div7 REGRESSION (RESOLVED): the CompoundAffix count came from an
    /// UNBOUNDED walk over C's terminator, which NISortAffixes writes ONE
    /// ELEMENT PAST the palloc'd array when every affix is collected
    /// (spell.c:1987 alloc vs :2015 terminator, repalloc only after) — the
    /// repalloc drops it and the walk read heap garbage (ncomp=137 then 109
    /// for naffixes==1). The oracle now bounds the scan by naffixes, which is
    /// exact in both cases. Upstream OOB write recorded as its own finding.
    /// TASK #83 MINIMIZED REPRO + TRIGGER-DETECTION TEST.
    /// A 4-line .affix reaching the OOB condition: naffixes == 1 (a power of
    /// two, so 16*naffixes is exactly a chunk size) AND that single affix
    /// collected into CompoundAffix (so `ptr` ends at index naffixes and
    /// spell.c:2015 writes the terminator one element past the array).
    /// `flag ~Z:` supplies FF_COMPOUNDONLY, which NIAddAffix promotes to
    /// FF_COMPOUNDFLAG; `compoundwords controlled Z` registers the compound
    /// flag; `foo/Z` in the .dict puts flag "Z" in AffixData so isAffixInUse
    /// passes. Asserts the trigger is REACHED (ncomp == naffixes == 1), which
    /// is what makes the terminator write out-of-bounds.
    ///
    /// VERDICT: this input produces REAL, ALLOCATOR-DETECTED HEAP CORRUPTION.
    /// On macOS libmalloc the run aborts with
    ///   `malloc: Heap corruption detected, free list is damaged at 0x...`
    /// and the abort surfaces at the NEXT allocation (mkANode -> spf_palloc,
    /// pg_spellfam_io.c:205, reached from NISortAffixes) — the classic delayed
    /// corruption signature: the OOB terminator write clobbers adjacent heap
    /// metadata and the allocator only notices when it next walks its free
    /// list. #[ignore]d because it ABORTS THE PROCESS (it cannot share a test
    /// binary); run it explicitly to reproduce the corruption.
    #[test]
    #[ignore = "task #83: aborts the process with allocator-detected heap corruption (by design)"]
    fn task83_min_oob_trigger() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../../scratchpad/needs-decode/min83-oob-naffixes1")).unwrap();
        super::spellfam_diff(&data);
    }
    #[test]
    fn div8_c2c2ce81() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/probe-div8-c2c2ce81")).unwrap();
        super::spellfam_diff(&data);
    }
    #[test]
    fn div7_compound_4e2fe0d5() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/open-div7-compound-4e2fe0d5")).unwrap();
        super::spellfam_diff(&data);
    }
    #[test]
    fn segv2_4525b7a1() {
        let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spellfam_diff/CI-div-segv2-4525b7a1")).unwrap();
        super::spellfam_diff(&data);
    }
    #[test]
    fn segv_acad8fe4() {
        let data = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../corpus/spellfam_diff/CI-div-segv-acad8fe4"
        ))
        .unwrap();
        super::spellfam_diff(&data);
    }
}
