//! Tests for the in-crate `regexp.c` logic, with deterministic test
//! providers behind the genuine cross-subsystem seams:
//!
//! * Encoding: SQL_ASCII semantics — 1 byte per character, identity
//!   byte<->`pg_wchar` mapping (character offsets == byte offsets, exactly
//!   as PostgreSQL behaves in a single-byte database).
//! * Engine: a literal-substring matcher with optional `REG_ICASE` support
//!   and a single `(...)` capture group, sufficient to exercise the
//!   compiled-RE cache, the match-location collection, and the
//!   count/instr/substr/match/split position math without the real
//!   `backend-regex-core`.

use super::*;
use std::any::Any;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

const C_COLL: Oid = 950;

// ---- test regex engine ----------------------------------------------------

#[derive(Clone)]
struct TestRe {
    needle: Vec<u8>,
    icase: bool,
    /// byte offset within `needle` where the single capture group begins,
    /// and its length, if any
    capture: Option<(usize, usize)>,
}

/// The owned engine value carried (type-erased) inside `RegexCompiled.engine`,
/// mirroring how `backend-regex-core` carries its real `RegexT`. Its `Drop`
/// stands in for `pg_regfree` freeing the engine state — exactly what the real
/// engine does when the last `Rc` reference goes away.
impl Drop for TestRe {
    fn drop(&mut self) {
        FREES.with(|c| c.set(c.get() + 1));
    }
}

thread_local! {
    static COMPILES: Cell<usize> = const { Cell::new(0) };
    static FREES: Cell<usize> = const { Cell::new(0) };
}

/// Recover the carried `TestRe` from the public carrier (the test mirror of
/// `backend-regex-core`'s `regex_of` downcast).
fn regex_of(re: &RegexCompiled) -> Rc<TestRe> {
    re.engine
        .clone()
        .downcast::<TestRe>()
        .expect("RegexCompiled.engine is not a TestRe")
}

fn test_regcomp(pattern: &[PgWChar], cflags: i32, _collation: Oid) -> PgResult<RegcompResult> {
    let mut bytes: Vec<u8> = pattern.iter().map(|&c| c as u8).collect();
    let mut capture = None;
    if let (Some(i), Some(j)) = (
        bytes.iter().position(|&b| b == b'('),
        bytes.iter().position(|&b| b == b')'),
    ) {
        assert!(i < j);
        bytes.remove(j);
        bytes.remove(i);
        capture = Some((i, j - i - 1));
    }
    let nsub = usize::from(capture.is_some());
    let re = TestRe { needle: bytes, icase: cflags & REG_ICASE != 0, capture };
    let engine: Rc<dyn Any> = Rc::new(re);
    COMPILES.with(|c| c.set(c.get() + 1));
    Ok(RegcompResult::Compiled(RegexCompiled { engine, re_nsub: nsub }))
}

fn fold(b: u8, icase: bool) -> u8 {
    if icase {
        b.to_ascii_lowercase()
    } else {
        b
    }
}

fn test_regexec(
    re: &RegexCompiled,
    data: &[PgWChar],
    search_start: i32,
    pmatch: &mut [RegMatch],
) -> PgResult<RegexecResult> {
    let re = regex_of(re);
    let hay: Vec<u8> = data.iter().map(|&c| c as u8).collect();
    let start = search_start as usize;
    if start > hay.len() {
        return Ok(RegexecResult::NoMatch);
    }
    let nlen = re.needle.len();
    let mut found = None;
    for so in start..=hay.len().saturating_sub(nlen).max(start) {
        if so + nlen > hay.len() {
            break;
        }
        if hay[so..so + nlen]
            .iter()
            .zip(re.needle.iter())
            .all(|(&h, &n)| fold(h, re.icase) == fold(n, re.icase))
        {
            found = Some(so);
            break;
        }
    }
    let Some(so) = found else { return Ok(RegexecResult::NoMatch) };
    let eo = so + nlen;
    for slot in pmatch.iter_mut() {
        *slot = RegMatch::UNSET;
    }
    if let Some(m) = pmatch.get_mut(0) {
        *m = RegMatch { rm_so: so as i64, rm_eo: eo as i64 };
    }
    if let (Some((cs, cl)), Some(m)) = (re.capture, pmatch.get_mut(1)) {
        *m = RegMatch { rm_so: (so + cs) as i64, rm_eo: (so + cs + cl) as i64 };
    }
    Ok(RegexecResult::Matched)
}

fn test_regprefix<'mcx>(mcx: Mcx<'mcx>, re: &RegexCompiled) -> PgResult<RegprefixResult<'mcx>> {
    let re = regex_of(re);
    if re.needle.is_empty() {
        return Ok(RegprefixResult::NoMatch);
    }
    let mut v = vec_with_capacity_in(mcx, re.needle.len())?;
    v.extend(re.needle.iter().map(|&b| b as PgWChar));
    Ok(RegprefixResult::Exact(v))
}

fn test_regfree(re: RegexCompiled) {
    // Take the carrier by value and drop it; the last `Rc` drop runs
    // `TestRe::drop`, which bumps `FREES` (the test mirror of the engine
    // releasing its state).
    drop(re);
}

// ---- test encoding providers (SQL_ASCII) ----------------------------------

fn test_mb2wchar<'mcx>(mcx: Mcx<'mcx>, from: &[u8]) -> PgResult<PgVec<'mcx, PgWChar>> {
    let mut v = vec_with_capacity_in(mcx, from.len())?;
    v.extend(from.iter().map(|&b| b as PgWChar));
    Ok(v)
}

fn test_wchar2mb<'mcx>(mcx: Mcx<'mcx>, from: &[PgWChar]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = vec_with_capacity_in(mcx, from.len())?;
    v.extend(from.iter().map(|&c| c as u8));
    Ok(v)
}

fn test_text_substr<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    start: i32,
    length: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // single-byte text_substring semantics, enough for in-range requests
    let s1 = start.max(1) as usize - 1;
    let e = (start as i64 + length as i64 - 1).max(s1 as i64) as usize;
    let begin = s1.min(s.len());
    let end = e.min(s.len()).max(begin);
    slice_in(mcx, &s[begin..end])
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        engine::pg_regcomp::set(test_regcomp);
        engine::pg_regexec::set(test_regexec);
        engine::pg_regprefix::set(test_regprefix);
        engine::pg_regfree::set(test_regfree);
        mb::pg_database_encoding_max_length::set(|| 1);
        mb::pg_mbstrlen_with_len::set(|s, limit| (limit.max(0) as usize).min(s.len()) as i32);
        mb::pg_mblen_range::set(|_| 1);
        mb::pg_mb2wchar_with_len::set(test_mb2wchar);
        mb::pg_wchar2mb_with_len::set(test_wchar2mb);
        varlena_seams::text_substr::set(test_text_substr);
    });
}

// ---- tests -----------------------------------------------------------------

#[test]
fn parse_re_flags_options() {
    let f = parse_re_flags(None).unwrap();
    assert_eq!(f.cflags, REG_ADVANCED);
    assert!(!f.glob);

    let f = parse_re_flags(Some(b"gi")).unwrap();
    assert!(f.glob);
    assert_ne!(f.cflags & REG_ICASE, 0);

    let f = parse_re_flags(Some(b"n")).unwrap();
    assert_eq!(f.cflags & REG_NEWLINE, REG_NEWLINE);
    let f = parse_re_flags(Some(b"p")).unwrap();
    assert_ne!(f.cflags & REG_NLSTOP, 0);
    assert_eq!(f.cflags & REG_NLANCH, 0);

    let f = parse_re_flags(Some(b"q")).unwrap();
    assert_ne!(f.cflags & REG_QUOTE, 0);
    assert_eq!(f.cflags & (REG_ADVANCED | REG_EXTENDED) & !REG_QUOTE, 0);
}

#[test]
fn parse_re_flags_invalid_option() {
    install();
    let err = parse_re_flags(Some(b"z")).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message(), "invalid regular expression option: \"z\"");
}

#[test]
fn operators_and_cache_hits() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let before = COMPILES.with(|c| c.get());
    assert!(textregexeq(mcx, b"hello world", b"world", C_COLL).unwrap());
    assert!(!textregexne(mcx, b"hello world", b"world", C_COLL).unwrap());
    assert!(texticregexeq(mcx, b"hello WORLD", b"world", C_COLL).unwrap());
    assert!(!textregexeq(mcx, b"hello world", b"absent", C_COLL).unwrap());
    // the second textregex* call reuses the cached compile of "world" with
    // identical (cflags, collation)
    assert!(textregexeq(mcx, b"another world", b"world", C_COLL).unwrap());
    let compiles = COMPILES.with(|c| c.get()) - before;
    assert_eq!(compiles, 3); // world, world+icase, absent
}

#[test]
fn cache_eviction_frees_engine_state() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let before = FREES.with(|c| c.get());
    for i in 0..(MAX_CACHED_RES + 5) {
        let pat = format!("uniquepat{i}");
        assert!(!textregexeq(mcx, b"zzz", pat.as_bytes(), C_COLL).unwrap());
    }
    let frees = FREES.with(|c| c.get()) - before;
    assert!(frees >= 5, "expected evictions to free engine handles, got {frees}");
}

#[test]
fn textregexsubstr_whole_and_capture() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let r = textregexsubstr(mcx, b"xxabcyy", b"abc", C_COLL).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"abc");

    let r = textregexsubstr(mcx, b"xxabcyy", b"a(b)c", C_COLL).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"b");

    assert!(textregexsubstr(mcx, b"xxabcyy", b"nope", C_COLL).unwrap().is_none());
}

#[test]
fn regexp_count_and_instr() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    assert_eq!(regexp_count(mcx, b"abcabcab", b"ab", None, None, C_COLL).unwrap(), 3);
    assert_eq!(regexp_count(mcx, b"abcabcab", b"ab", Some(2), None, C_COLL).unwrap(), 2);
    let err = regexp_count(mcx, b"x", b"x", Some(0), None, C_COLL).unwrap_err();
    assert_eq!(err.message(), "invalid value for parameter \"start\": 0");
    let err = regexp_count(mcx, b"x", b"x", None, Some(b"g"), C_COLL).unwrap_err();
    assert_eq!(err.message(), "regexp_count() does not support the \"global\" option");

    // 1-based position of the second occurrence
    assert_eq!(
        regexp_instr(mcx, b"abcabcab", b"ab", None, Some(2), None, None, None, C_COLL).unwrap(),
        4
    );
    // endoption = 1: position after the end of the match
    assert_eq!(
        regexp_instr(mcx, b"abcabcab", b"ab", None, Some(2), Some(1), None, None, C_COLL).unwrap(),
        6
    );
    // n beyond match count
    assert_eq!(
        regexp_instr(mcx, b"abcabcab", b"ab", None, Some(9), None, None, None, C_COLL).unwrap(),
        0
    );
}

#[test]
fn regexp_like_and_match() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    assert!(regexp_like(mcx, b"hello", b"ell", None, C_COLL).unwrap());
    assert!(!regexp_like(mcx, b"hello", b"zzz", None, C_COLL).unwrap());

    let rows = regexp_match(mcx, b"xabcx", b"a(b)c", None, C_COLL).unwrap().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().as_slice(), b"b");

    assert!(regexp_match(mcx, b"xabcx", b"zzz", None, C_COLL).unwrap().is_none());

    let err = regexp_match(mcx, b"x", b"x", Some(b"g"), C_COLL).unwrap_err();
    assert_eq!(err.hint(), Some("Use the regexp_matches function instead."));
}

#[test]
fn regexp_matches_materialized() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let rows = regexp_matches(mcx, b"abcabc", b"a(b)c", Some(b"g"), C_COLL).unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows.iter() {
        assert_eq!(row[0].as_ref().unwrap().as_slice(), b"b");
    }
}

#[test]
fn regexp_split() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let parts = regexp_split_to_array(mcx, b"a,b,c", b",", None, C_COLL).unwrap();
    let parts: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    assert_eq!(parts, vec![&b"a"[..], b"b", b"c"]);

    let parts = regexp_split_to_table(mcx, b"a,,c", b",", None, C_COLL).unwrap();
    let parts: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    assert_eq!(parts, vec![&b"a"[..], b"", b"c"]);

    // no match: the whole string comes back
    let parts = regexp_split_to_array(mcx, b"abc", b",", None, C_COLL).unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].as_slice(), b"abc");
}

#[test]
fn regexp_substr_selection() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let r = regexp_substr(mcx, b"abcabc", b"abc", None, Some(2), None, None, C_COLL)
        .unwrap()
        .unwrap();
    assert_eq!(r.as_slice(), b"abc");
    assert!(regexp_substr(mcx, b"abcabc", b"abc", None, Some(3), None, None, C_COLL)
        .unwrap()
        .is_none());

    // subexpr selection
    let r = regexp_substr(mcx, b"xabcx", b"a(b)c", None, None, None, Some(1), C_COLL)
        .unwrap()
        .unwrap();
    assert_eq!(r.as_slice(), b"b");
}

#[test]
fn regexp_fixed_prefix_exact() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let (prefix, exact) = regexp_fixed_prefix(mcx, b"abc", false, C_COLL).unwrap().unwrap();
    assert_eq!(prefix.as_slice(), b"abc");
    assert!(exact);
}

#[test]
fn similar_escape_translation() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let r = similar_to_escape_1(mcx, b"ab%c_").unwrap();
    assert_eq!(r.as_slice(), b"^(?:ab.*c.)$");

    // escaped % is a literal; (..) becomes non-capturing; specials escaped
    let r = similar_to_escape_1(mcx, b"a\\%b(c).").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a\\%b(?:c)\\.)$");

    // explicit escape character
    let r = similar_to_escape_2(mcx, b"a#%b", b"#").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a\\%b)$");

    // empty escape string: no escape character at all
    let r = similar_to_escape_2(mcx, b"a\\b", b"").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a\\\\b)$");

    // escape-double-quote separators
    let r = similar_to_escape_1(mcx, b"a\\\"b\\\"c").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a){1,1}?(b){1,1}(?:c)$");
    let err = similar_to_escape_1(mcx, b"a\\\"b\\\"c\\\"d").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER);

    // character class: ']' right after '[' is literal; '_' inside stays
    let r = similar_to_escape_1(mcx, b"[]a_]%").unwrap();
    assert_eq!(r.as_slice(), b"^(?:[]a_].*)$");

    // multi-character escape string is rejected
    let err = similar_to_escape_2(mcx, b"x", b"ab").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE);

    // non-strict legacy wrapper
    assert!(similar_escape(mcx, None, None).unwrap().is_none());
    let r = similar_escape(mcx, Some(b"a%"), None).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"^(?:a.*)$");
}

#[test]
fn textregexreplace_flag_hint() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let err = textregexreplace(mcx, b"s", b"p", b"r", b"1", C_COLL).unwrap_err();
    assert_eq!(err.message(), "invalid regular expression option: \"1\"");
    assert!(err.hint().unwrap().contains("cast the fourth argument to integer"));

    let err =
        textregexreplace_extended(mcx, b"s", b"p", b"r", Some(0), None, None, C_COLL).unwrap_err();
    assert_eq!(err.message(), "invalid value for parameter \"start\": 0");
    let err =
        textregexreplace_extended(mcx, b"s", b"p", b"r", None, Some(-1), None, C_COLL).unwrap_err();
    assert_eq!(err.message(), "invalid value for parameter \"n\": -1");
}

#[test]
fn inward_seams_installed() {
    init_seams();
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut pmatch = [RegMatch::UNSET; 1];
    assert!(regexp_seams::RE_compile_and_execute::call(
        mcx,
        b"needle",
        b"hay needle hay",
        REG_ADVANCED,
        C_COLL,
        &mut pmatch,
    )
    .unwrap());
    assert_eq!(pmatch[0].rm_so, 4);
}
