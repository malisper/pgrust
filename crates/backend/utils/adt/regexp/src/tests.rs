use super::*;
use ::mcx::MemoryContext;
use types_core::C_COLLATION_OID;

const C: Oid = C_COLLATION_OID;

fn utf8() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
}

#[test]
fn match_operators() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    for (s, p, want) in [
        ("thomas", ".*thomas.*", true),
        ("thomas", ".*Thomas.*", false),
        ("thomas", "^tho", true),
        ("thomas", "mas$", true),
        ("thomas", "^mas", false),
        ("foo", "^(b|f)o+$", true),
        ("foobar", "^(b|f)o+$", false),
        ("", "^$", true),
        ("abc", "a.c", true),
    ] {
        assert_eq!(textregexeq(m, s.as_bytes(), p.as_bytes(), C).unwrap(), want, "{s:?} ~ {p:?}");
        assert_eq!(textregexne(m, s.as_bytes(), p.as_bytes(), C).unwrap(), !want, "{s:?} !~ {p:?}");
    }
    for (s, p, want) in [
        ("thomas", ".*Thomas.*", true),
        ("THOMAS", "^tho", true),
        ("thomas", "^MAS", false),
    ] {
        assert_eq!(texticregexeq(m, s.as_bytes(), p.as_bytes(), C).unwrap(), want, "{s:?} ~* {p:?}");
        assert_eq!(texticregexne(m, s.as_bytes(), p.as_bytes(), C).unwrap(), !want, "{s:?} !~* {p:?}");
    }
    assert!(nameregexeq(m, b"pg_class", b"^pg_", C).unwrap());
    assert!(nameregexne(m, b"pg_class", b"^xx", C).unwrap());
    assert!(nameicregexeq(m, b"PG_CLASS", b"^pg_", C).unwrap());
    assert!(nameicregexne(m, b"PG_CLASS", b"^xx", C).unwrap());
}

#[test]
fn invalid_pattern_errors() {
    utf8();
    let cx = MemoryContext::new("test");
    let err = textregexeq(cx.mcx(), b"abc", b"(unbalanced", C).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("invalid regular expression"), "{msg}");
}

#[test]
fn submatches_filled() {
    utf8();
    let cx = MemoryContext::new("test");
    let mut pmatch = [RegMatch::UNSET; 3];
    let matched = RE_compile_and_execute(
        cx.mcx(), b"^(a+)(b+)$", b"aabbb", REG_ADVANCED, C, &mut pmatch,
    )
    .unwrap();
    assert!(matched);
    assert_eq!(pmatch[0], RegMatch { rm_so: 0, rm_eo: 5 });
    assert_eq!(pmatch[1], RegMatch { rm_so: 0, rm_eo: 2 });
    assert_eq!(pmatch[2], RegMatch { rm_so: 2, rm_eo: 5 });
}

#[test]
fn cache_hit_and_move_to_front() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    let f = REG_ADVANCED | REG_NOSUB;

    RE_compile_and_cache(m, b"aaa", f, C).unwrap();
    RE_compile_and_cache(m, b"bbb", f, C).unwrap();
    assert_eq!(
        cache_keys(),
        vec![(b"bbb".to_vec(), f, C), (b"aaa".to_vec(), f, C)]
    );

    RE_compile_and_cache(m, b"aaa", f, C).unwrap();
    assert_eq!(
        cache_keys(),
        vec![(b"aaa".to_vec(), f, C), (b"bbb".to_vec(), f, C)]
    );

    RE_compile_and_cache(m, b"aaa", f, C).unwrap();
    assert_eq!(cache_keys().len(), 2);
}

#[test]
fn cache_key_includes_flags_and_collation() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    RE_compile_and_cache(m, b"xyz", REG_ADVANCED, C).unwrap();
    RE_compile_and_cache(m, b"xyz", REG_ADVANCED | REG_ICASE, C).unwrap();
    RE_compile_and_cache(m, b"xyz", REG_ADVANCED, C).unwrap();
    assert_eq!(
        cache_keys(),
        vec![
            (b"xyz".to_vec(), REG_ADVANCED, C),
            (b"xyz".to_vec(), REG_ADVANCED | REG_ICASE, C),
        ]
    );
}

#[test]
fn cache_evicts_lru_at_capacity() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    let f = REG_ADVANCED | REG_NOSUB;

    for i in 0..MAX_CACHED_RES + 1 {
        RE_compile_and_cache(m, format!("p{i}").as_bytes(), f, C).unwrap();
    }
    let keys = cache_keys();
    assert_eq!(keys.len(), MAX_CACHED_RES);
    assert_eq!(keys[0].0, format!("p{MAX_CACHED_RES}").into_bytes());
    assert!(!keys.iter().any(|k| k.0 == b"p0"), "oldest entry evicted");

    RE_compile_and_cache(m, b"p1", f, C).unwrap();
    let keys = cache_keys();
    assert_eq!(keys.len(), MAX_CACHED_RES);
    assert_eq!(keys[0].0, b"p1".to_vec());
}

#[test]
fn fixed_prefix() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let (pre, exact) = regexp_fixed_prefix(m, b"^test", false, C).unwrap().unwrap();
    assert_eq!(pre.as_slice(), b"test");
    assert!(!exact);

    let (pre, exact) = regexp_fixed_prefix(m, b"^foo$", false, C).unwrap().unwrap();
    assert_eq!(pre.as_slice(), b"foo");
    assert!(exact);

    assert!(regexp_fixed_prefix(m, b"test", false, C).unwrap().is_none());
    assert!(regexp_fixed_prefix(m, b"^foo", true, C).unwrap().is_none());

    let (pre, exact) = regexp_fixed_prefix(m, b"^abc(def|dex)", false, C).unwrap().unwrap();
    assert_eq!(pre.as_slice(), b"abcd");
    assert!(!exact);
}

#[test]
fn builtins_table() {
    // (oid, name, nargs, strict, retset) vs pg_proc.dat.
    let expected: &[(Oid, &str, i16, bool, bool)] = &[
        (79, "nameregexeq", 2, true, false),
        (1238, "texticregexeq", 2, true, false),
        (1239, "texticregexne", 2, true, false),
        (1240, "nameicregexeq", 2, true, false),
        (1241, "nameicregexne", 2, true, false),
        (1252, "nameregexne", 2, true, false),
        (1254, "textregexeq", 2, true, false),
        (1256, "textregexne", 2, true, false),
        (1623, "similar_escape", 2, false, false),
        (1656, "bpcharicregexeq", 2, true, false),
        (1657, "bpcharicregexne", 2, true, false),
        (1658, "bpcharregexeq", 2, true, false),
        (1659, "bpcharregexne", 2, true, false),
        (1986, "similar_to_escape_2", 2, true, false),
        (1987, "similar_to_escape_1", 1, true, false),
        (2073, "textregexsubstr", 2, true, false),
        (2284, "textregexreplace_noopt", 3, true, false),
        (2285, "textregexreplace", 4, true, false),
        (2763, "regexp_matches_no_flags", 2, true, true),
        (2764, "regexp_matches", 3, true, true),
        (2765, "regexp_split_to_table_no_flags", 2, true, true),
        (2766, "regexp_split_to_table", 3, true, true),
        (2767, "regexp_split_to_array_no_flags", 2, true, false),
        (2768, "regexp_split_to_array", 3, true, false),
        (3396, "regexp_match_no_flags", 2, true, false),
        (3397, "regexp_match", 3, true, false),
        (6251, "textregexreplace_extended", 6, true, false),
        (6252, "textregexreplace_extended_no_flags", 5, true, false),
        (6253, "textregexreplace_extended_no_n", 4, true, false),
        (6254, "regexp_count_no_start", 2, true, false),
        (6255, "regexp_count_no_flags", 3, true, false),
        (6256, "regexp_count", 4, true, false),
        (6257, "regexp_instr_no_start", 2, true, false),
        (6258, "regexp_instr_no_n", 3, true, false),
        (6259, "regexp_instr_no_endoption", 4, true, false),
        (6260, "regexp_instr_no_flags", 5, true, false),
        (6261, "regexp_instr_no_subexpr", 6, true, false),
        (6262, "regexp_instr", 7, true, false),
        (6263, "regexp_like_no_flags", 2, true, false),
        (6264, "regexp_like", 3, true, false),
        (6265, "regexp_substr_no_start", 2, true, false),
        (6266, "regexp_substr_no_n", 3, true, false),
        (6267, "regexp_substr_no_flags", 4, true, false),
        (6268, "regexp_substr_no_subexpr", 5, true, false),
        (6269, "regexp_substr", 6, true, false),
    ];
    assert_eq!(builtins::REGEXP_BUILTINS.len(), expected.len());
    for (b, (oid, name, nargs, strict, retset)) in builtins::REGEXP_BUILTINS.iter().zip(expected) {
        assert_eq!((b.foid, b.name), (*oid, *name));
        assert_eq!(b.nargs, *nargs, "{name}");
        assert_eq!(b.strict, *strict, "{name}");
        assert_eq!(b.retset, *retset, "{name}");
    }
}

fn full_setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        regex_core::init_seams();
        init_seams();
        postgres_seams::check_for_interrupts::set(cfi_ok);
    });
    utf8();
}

fn cfi_ok() -> PgResult<()> {
    Ok(())
}

fn sqlstate(err: &types_error::PgError) -> String {
    let mut s = err.message.clone();
    if let Some(h) = &err.hint {
        s.push(' ');
        s.push_str(h);
    }
    s
}

fn code(err: &types_error::PgError) -> [u8; 5] {
    types_error::unpack_sqlstate(err.sqlstate())
}

#[test]
fn parse_flags() {
    let f = parse_re_flags(None).unwrap();
    assert_eq!(f.cflags, REG_ADVANCED);
    assert!(!f.glob);

    let f = parse_re_flags(Some(b"gi")).unwrap();
    assert!(f.glob);
    assert_eq!(f.cflags, REG_ADVANCED | REG_ICASE);

    let f = parse_re_flags(Some(b"n")).unwrap();
    assert_eq!(f.cflags, REG_ADVANCED | ::regex::REG_NEWLINE);

    let err = parse_re_flags(Some(b"z")).unwrap_err();
    let msg = sqlstate(&err);
    assert!(msg.contains("invalid regular expression option: \"z\""), "{msg}");
    assert_eq!(&code(&err), b"22023");
}

#[test]
fn regex_substr() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let r = textregexsubstr(m, b"foobar", b"o.b", C).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"oob");
    let r = textregexsubstr(m, b"foobar", b"o(.)b", C).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"o");
    assert!(textregexsubstr(m, b"foobar", b"xyz", C).unwrap().is_none());
    assert!(textregexsubstr(m, b"foo", b"foo(bar)?", C).unwrap().is_none());
}

#[test]
fn regex_replace() {
    full_setup();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let r = textregexreplace_noopt(m, b"aaa bbb aaa", b"a+", b"X", C).unwrap();
    assert_eq!(r.as_slice(), b"X bbb aaa");
    let r = textregexreplace(m, b"aaa bbb aaa", b"a+", b"X", b"g", C).unwrap();
    assert_eq!(r.as_slice(), b"X bbb X");
    let r = textregexreplace(m, b"foobar", b"o(.)b", b"[\\1]", b"", C).unwrap();
    assert_eq!(r.as_slice(), b"f[o]ar");
    let r = textregexreplace(m, b"foobar", b"oob", b"<\\&>", b"", C).unwrap();
    assert_eq!(r.as_slice(), b"f<oob>ar");
    let r = textregexreplace(m, b"abc", b"", b"X", b"g", C).unwrap();
    assert_eq!(r.as_slice(), b"XaXbXcX");

    let r = textregexreplace_extended(m, b"A PostgreSQL function", b"a|e|i|o|u", b"X",
        Some(1), Some(3), Some(b"i"), C).unwrap();
    assert_eq!(r.as_slice(), b"A PostgrXSQL function");
    let r = textregexreplace_extended(m, b"A PostgreSQL function", b"a|e|i|o|u", b"X",
        Some(1), Some(0), Some(b"i"), C).unwrap();
    assert_eq!(r.as_slice(), b"X PXstgrXSQL fXnctXXn");

    let err = textregexreplace_extended(m, b"x", b"x", b"y", Some(0), None, None, C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"start\": 0"));
    let err = textregexreplace_extended(m, b"x", b"x", b"y", Some(1), Some(-1), None, C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"n\": -1"));
    let err = textregexreplace(m, b"x", b"x", b"y", b"1", C).unwrap_err();
    let msg = sqlstate(&err);
    assert!(msg.contains("invalid regular expression option: \"1\""), "{msg}");
    assert!(msg.contains("cast the fourth argument to integer explicitly"), "{msg}");
}

#[test]
fn similar_escape_family() {
    utf8();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let r = similar_to_escape_1(m, b"_bcd%").unwrap();
    assert_eq!(r.as_slice(), b"^(?:.bcd.*)$");
    let r = similar_to_escape_2(m, b"_bcd%", b"$").unwrap();
    assert_eq!(r.as_slice(), b"^(?:.bcd.*)$");
    let r = similar_to_escape_2(m, b"a$_b", b"$").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a\\_b)$");
    let r = similar_to_escape_2(m, b"a_b", b"").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a.b)$");
    let r = similar_to_escape_1(m, b"x\\\"y\\\"z").unwrap();
    assert_eq!(r.as_slice(), b"^(?:x){1,1}?(y){1,1}(?:z)$");
    let r = similar_to_escape_1(m, b"a(b)c").unwrap();
    assert_eq!(r.as_slice(), b"^(?:a(?:b)c)$");
    let r = similar_to_escape_1(m, b"[a^b]c").unwrap();
    assert_eq!(r.as_slice(), b"^(?:[a^b]c)$");

    let err = similar_to_escape_2(m, b"x", b"ab").unwrap_err();
    let msg = sqlstate(&err);
    assert!(msg.contains("invalid escape string"), "{msg}");
    assert_eq!(&code(&err), b"22025");
    let err = similar_to_escape_1(m, b"a\\\"b\\\"c\\\"d").unwrap_err();
    assert!(sqlstate(&err).contains("more than two escape-double-quote separators"));

    assert!(similar_escape(m, None, Some(b"\\")).unwrap().is_none());
    let r = similar_escape(m, Some(b"a_b"), None).unwrap().unwrap();
    assert_eq!(r.as_slice(), b"^(?:a.b)$");
}

#[test]
fn count_instr_like() {
    full_setup();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    use crate::matches::{regexp_count, regexp_instr, regexp_like};

    assert_eq!(regexp_count(m, b"ABCABCAXYaxy", b"A.", None, None, C).unwrap(), 3);
    assert_eq!(regexp_count(m, b"ABCABCAXYaxy", b"A.", Some(5), None, C).unwrap(), 1);
    assert_eq!(regexp_count(m, b"ABCABCAXYaxy", b"A.", Some(1), Some(b"i"), C).unwrap(), 4);
    assert_eq!(regexp_count(m, b"abc", b"", None, None, C).unwrap(), 4);
    let err = regexp_count(m, b"x", b"x", Some(0), None, C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"start\": 0"));
    let err = regexp_count(m, b"x", b"x", None, Some(b"g"), C).unwrap_err();
    assert!(sqlstate(&err).contains("regexp_count() does not support the \"global\" option"));

    let i = regexp_instr(m, b"number of your street, town zip, FR", b"[^,]+", None, Some(2), None, None, None, C).unwrap();
    assert_eq!(i, 23);
    assert_eq!(regexp_instr(m, b"ABCDEF", b"c(.)(..)", None, None, None, Some(b"i"), Some(2), C).unwrap(), 5);
    assert_eq!(regexp_instr(m, b"ABCDEF", b"c(.)(..)", None, None, Some(1), Some(b"i"), Some(2), C).unwrap(), 7);
    assert_eq!(regexp_instr(m, b"abc", b"x", None, None, None, None, None, C).unwrap(), 0);
    assert_eq!(regexp_instr(m, b"abc", b"a(x)?b", None, None, None, None, Some(1), C).unwrap(), 0);
    let err = regexp_instr(m, b"x", b"x", None, Some(0), None, None, None, C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"n\": 0"));
    let err = regexp_instr(m, b"x", b"x", None, None, Some(2), None, None, C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"endoption\": 2"));
    let err = regexp_instr(m, b"x", b"x", None, None, None, None, Some(-1), C).unwrap_err();
    assert!(sqlstate(&err).contains("invalid value for parameter \"subexpr\": -1"));

    assert!(regexp_like(m, b"abc", b"a.c", None, C).unwrap());
    assert!(!regexp_like(m, b"abc", b"A.C", None, C).unwrap());
    assert!(regexp_like(m, b"abc", b"A.C", Some(b"i"), C).unwrap());
    let err = regexp_like(m, b"x", b"x", Some(b"g"), C).unwrap_err();
    assert!(sqlstate(&err).contains("regexp_like() does not support the \"global\" option"));
}

#[test]
fn match_and_matches() {
    full_setup();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    use crate::matches::{
        build_regexp_match_result, regexp_match, regexp_matches_setup,
    };

    let row = |ctx: &crate::matches::RegexpMatchesCtx<'_, '_>| {
        let mut out: Vec<Option<Vec<u8>>> = Vec::new();
        build_regexp_match_result(ctx, |e| {
            out.push(e.map(|v| v.as_slice().to_vec()));
            Ok(())
        })
        .unwrap();
        out
    };

    let ctx = regexp_match(m, b"foobarbequebaz", b"(bar)(beque)", None, C).unwrap().unwrap();
    assert_eq!(row(&ctx), vec![Some(b"bar".to_vec()), Some(b"beque".to_vec())]);

    let ctx = regexp_match(m, b"foo", b"foo(bar)?", None, C).unwrap().unwrap();
    assert_eq!(row(&ctx), vec![None]);

    assert!(regexp_match(m, b"abc", b"xyz", None, C).unwrap().is_none());
    let err = regexp_match(m, b"x", b"x", Some(b"g"), C).map(|_| ()).unwrap_err();
    let msg = sqlstate(&err);
    assert!(msg.contains("regexp_match() does not support the \"global\" option"), "{msg}");
    assert!(msg.contains("Use the regexp_matches function instead."), "{msg}");

    let mut ctx = regexp_matches_setup(m, b"foobarbequebazilbarfbonk", b"b[^b]+", Some(b"g"), C).unwrap();
    let mut rows = Vec::new();
    while ctx.next_match < ctx.nmatches {
        rows.push(row(&ctx)[0].clone().unwrap());
        ctx.next_match += 1;
    }
    assert_eq!(rows, vec![b"bar".to_vec(), b"beque".to_vec(), b"bazil".to_vec(), b"barf".to_vec(), b"bonk".to_vec()]);
}

#[test]
fn substr_and_split() {
    full_setup();
    let cx = MemoryContext::new("test");
    let m = cx.mcx();
    use crate::matches::{build_regexp_split_result, regexp_split_setup, regexp_substr};

    let r = regexp_substr(m, b"number of your street, town zip, FR", b"[^,]+", None, Some(2), None, None, C)
        .unwrap()
        .unwrap();
    assert_eq!(r.as_slice(), b" town zip");
    assert!(regexp_substr(m, b"abc", b"x", None, None, None, None, C).unwrap().is_none());
    assert!(regexp_substr(m, b"abc", b"a(x)?c", None, None, None, Some(1), C).unwrap().is_none());
    assert!(regexp_substr(m, b"abc", b"a(b)c", None, None, None, Some(2), C).unwrap().is_none());

    let split = |s: &[u8], p: &[u8], f: Option<&[u8]>| -> Vec<Vec<u8>> {
        let mut ctx = regexp_split_setup(m, s, p, f, C, "regexp_split_to_array()").unwrap();
        let mut out = Vec::new();
        while ctx.next_match <= ctx.nmatches {
            out.push(build_regexp_split_result(&ctx).unwrap().as_slice().to_vec());
            ctx.next_match += 1;
        }
        out
    };
    assert_eq!(
        split(b"the quick brown fox", b"\\s+", None),
        vec![b"the".to_vec(), b"quick".to_vec(), b"brown".to_vec(), b"fox".to_vec()]
    );
    assert_eq!(
        split(b"abc", b"", None),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(split(b"", b",", None), vec![b"".to_vec()]);
    let err = regexp_split_setup(m, b"x", b"x", Some(b"g"), C, "regexp_split_to_array()").map(|_| ()).unwrap_err();
    assert!(sqlstate(&err).contains("regexp_split_to_array() does not support the \"global\" option"));
}


// regex_engine differential corpus: at engine=auto, classifier-admitted
// patterns run RE2 in POSIX longest-match mode; every regexp entry point
// must agree byte-for-byte (results AND errors) with a forced-spencer run.
// Classifier-rejected patterns dispatch to Spencer and agree trivially, so
// the same assertions ride for both kinds.

fn engine_snapshot(m: Mcx<'_>, s: &[u8], p: &[u8]) -> String {
    use crate::matches::{
        build_regexp_match_result, build_regexp_split_result, regexp_count, regexp_instr,
        regexp_like, regexp_match, regexp_matches_setup, regexp_split_setup, regexp_substr,
    };
    use core::fmt::Write;

    let mut out = String::new();
    macro_rules! snap {
        ($tag:expr, $e:expr) => {
            match $e {
                Ok(v) => writeln!(out, "{}: {:?}", $tag, v).unwrap(),
                Err(e) => writeln!(out, "{}: ERR {}", $tag, e.message).unwrap(),
            }
        };
    }

    snap!("eq", textregexeq(m, s, p, C));
    snap!(
        "substr_op",
        textregexsubstr(m, s, p, C).map(|o| o.map(|v| v.as_slice().to_vec()))
    );
    snap!(
        "replace_g",
        textregexreplace(m, s, p, b"<\\1>", b"g", C).map(|v| v.as_slice().to_vec())
    );
    snap!(
        "replace_1",
        textregexreplace_noopt(m, s, p, b"X", C).map(|v| v.as_slice().to_vec())
    );
    snap!(
        "replace_s2n2",
        textregexreplace_extended(m, s, p, b"[\\&]", Some(2), Some(2), None, C)
            .map(|v| v.as_slice().to_vec())
    );
    snap!("like", regexp_like(m, s, p, None, C));
    snap!("count", regexp_count(m, s, p, None, None, C));
    snap!("count_s3", regexp_count(m, s, p, Some(3), None, C));
    for n in 1..=3 {
        for endopt in [0, 1] {
            for sub in [None, Some(1)] {
                snap!(
                    format!("instr n{n} e{endopt} sub{sub:?}"),
                    regexp_instr(m, s, p, None, Some(n), Some(endopt), None, sub, C)
                );
            }
        }
    }
    for n in 1..=2 {
        for sub in [None, Some(1)] {
            snap!(
                format!("substr n{n} sub{sub:?}"),
                regexp_substr(m, s, p, None, Some(n), None, sub, C)
                    .map(|o| o.map(|v| v.as_slice().to_vec()))
            );
        }
    }

    let rows = |ctx: &crate::matches::RegexpMatchesCtx<'_, '_>| {
        let mut row: Vec<Option<Vec<u8>>> = Vec::new();
        build_regexp_match_result(ctx, |e| {
            row.push(e.map(|v| v.as_slice().to_vec()));
            Ok(())
        })
        .unwrap();
        row
    };
    match regexp_match(m, s, p, None, C) {
        Ok(None) => writeln!(out, "match: NULL").unwrap(),
        Ok(Some(ctx)) => writeln!(out, "match: {:?}", rows(&ctx)).unwrap(),
        Err(e) => writeln!(out, "match: ERR {}", e.message).unwrap(),
    }
    match regexp_matches_setup(m, s, p, Some(b"g"), C) {
        Ok(mut ctx) => {
            let mut all = Vec::new();
            while ctx.next_match < ctx.nmatches {
                all.push(rows(&ctx));
                ctx.next_match += 1;
            }
            writeln!(out, "matches_g: {all:?}").unwrap();
        }
        Err(e) => writeln!(out, "matches_g: ERR {}", e.message).unwrap(),
    }
    match regexp_split_setup(m, s, p, None, C, "regexp_split_to_array()") {
        Ok(mut ctx) => {
            let mut parts = Vec::new();
            while ctx.next_match <= ctx.nmatches {
                match build_regexp_split_result(&ctx) {
                    Ok(v) => parts.push(v.as_slice().to_vec()),
                    Err(e) => {
                        writeln!(out, "split_part: ERR {}", e.message).unwrap();
                        break;
                    }
                }
                ctx.next_match += 1;
            }
            writeln!(out, "split: {parts:?}").unwrap();
        }
        Err(e) => writeln!(out, "split: ERR {}", e.message).unwrap(),
    }
    out
}

fn assert_engine_parity(m: Mcx<'_>, s: &[u8], p: &[u8]) {
    // Three arms through every entry point: forced Spencer, auto with the
    // pattern-program tier disabled (pure RE2 dispatch), and auto with the
    // tier on (the default). Together with the forced-re2 crate tests this
    // is the four-engine differential — spencer / re2 / auto-RE2 /
    // pattern-program — over results AND errors.
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_SPENCER);
    let spencer = engine_snapshot(m, s, p);
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
    regexp_alt::set_regex_pattern_program(false);
    let auto_re2 = engine_snapshot(m, s, p);
    regexp_alt::set_regex_pattern_program(true);
    let auto_prog = engine_snapshot(m, s, p);
    assert_eq!(
        spencer,
        auto_re2,
        "auto (program off) diverges from spencer: pattern {:?} input {:?}",
        String::from_utf8_lossy(p),
        String::from_utf8_lossy(s)
    );
    assert_eq!(
        spencer,
        auto_prog,
        "auto (pattern program) diverges from spencer: pattern {:?} input {:?}",
        String::from_utf8_lossy(p),
        String::from_utf8_lossy(s)
    );
}

#[test]
fn auto_vs_spencer_differential() {
    full_setup();
    if !regexp_alt::re2_available() {
        return;
    }
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let corpus: &[&str] = &[
        "http://www.example.com/path/x?y=1",
        "https://sub.host.ru/",
        "http://hostonly.com",
        "not-a-url",
        "",
        "https://www.xn--80ak6aa92e.com/страница/1",
        "a1b2c3 déf gh",
        "aaa bbb aaa",
        "line one\nline two\n",
        "ab",
        "aab aab",
        ",,a,,b,,",
    ];
    // Compatible class (must actually dispatch) plus known-incompatible
    // patterns (must fail closed and agree trivially).
    let compatible: &[&str] = &[
        r"^https?://(?:www\.)?([^/]+)/.*$",
        "",
        "a",
        "a+",
        "a|ab",
        "(a+|a)(b?)",
        "([a-z0-9]+) ([a-z0-9]+)",
        "x*",
        "[0-9]",
        "((a)(b))",
        "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)",
        "^",
        "$",
        "^$",
        ".",
        "a{2}",
        "a{2,}",
        "a{0,3}b",
        "[^,]+",
        "[é]",
        "é+",
        "(?:aa|bb)+",
        "b[^b]+",
        r"\.",
        "a\nb",
        // Pattern-program subset shapes (the anchored fast tier): the
        // three-arm parity harness runs these program-on AND program-off.
        "^https?://",
        r"^(?:www\.)?([^/]+)$",
        "^([a-z0-9]+) ",
        "^[^,]*,",
        "^a.+$",
        "^é?a",
        "^([0-9]{1,3})b",
    ];
    let incompatible: &[&str] =
        &[r"(a)\1", r"\w+", r"\bword\b", "a*?", "[[:alpha:]]", r"\d", r"[\d]", "(?i)a"];
    // Whole-match tier: quantified capture subtrees dispatch for
    // whole-match-only probes but must fall to Spencer for every
    // capture-consuming probe (adversarial find, pinned: RE2-longest and
    // Spencer disagree on the last-iteration capture).
    let whole_match_only: &[&str] = &[
        "(a)+",
        "(x*)+y",
        "^(foo|bar)$",
        "(é|.[^a])",
        r"(.?|é|(?:0{2,}|é[^a][^/,]+ ?|\*)+é(?:c?0|\n{2})+)+|[a-c0-9]?|[ab]0+",
        r"\*{0,2}(.|\n*,*)|(é|[ab]?/+(?:0x,\.?|é|é,? *\n{1,3}){2}|.(?:0éé*|é)*[^a]).*",
    ];

    for p in compatible {
        regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
        assert!(
            regexp_alt::dispatch(p.as_bytes(), REG_ADVANCED, b"x").unwrap().is_some(),
            "expected {p:?} to dispatch to re2"
        );
        for s in corpus {
            assert_engine_parity(m, s.as_bytes(), p.as_bytes());
        }
    }
    for p in incompatible {
        regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
        assert!(
            regexp_alt::dispatch(p.as_bytes(), REG_ADVANCED, b"x").unwrap().is_none(),
            "expected {p:?} to fail closed to spencer"
        );
        for s in corpus {
            assert_engine_parity(m, s.as_bytes(), p.as_bytes());
        }
    }

    for p in whole_match_only {
        regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
        let re = regexp_alt::dispatch(p.as_bytes(), REG_ADVANCED, b"x").unwrap().unwrap();
        assert!(!re.capture_safe(), "expected {p:?} to be whole-match tier");
        for s in corpus {
            assert_engine_parity(m, s.as_bytes(), p.as_bytes());
        }
        assert_engine_parity(m, "/100éxb\nx".as_bytes(), p.as_bytes());
    }

    // Spencer-ETOOBIG class (regex suite): RE2 would compile this, Spencer
    // errors "regular expression is too complex" — the complexity budget
    // must fail it closed so the error surface stays Spencer's.
    let toobig = "x*y*z*".repeat(1000);
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
    assert!(regexp_alt::dispatch(toobig.as_bytes(), REG_ADVANCED, b"x").unwrap().is_none());
    assert_engine_parity(m, b"x", toobig.as_bytes());
}

// DATA-adversarial differential corpus: subjects on which the Spencer view
// (pg_mb2wchar: NUL-terminated, bytewise-decoded invalid UTF-8) diverges
// from RE2's raw-byte view. The per-evaluation data guard must route every
// one of these to Spencer at engine=auto; parity failure here means the
// guard has a hole. Patterns still dispatch to RE2 for clean subjects.
#[test]
fn data_adversarial_differential() {
    full_setup();
    if !regexp_alt::re2_available() {
        return;
    }
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let subjects: &[&[u8]] = &[
        // NUL-only, and NUL at start / middle / end / adjacent to matches.
        b"\x00",
        b"\x00\x00\x00",
        b"\x00abc",
        b"abc\x00",
        b"ab\x00cd",
        b"a1\x00b2c3",
        b"aaa\x00aaa",
        b"caf\x00",
        b"http://www.ex\x00ample.com/path/x?y=1",
        b"http://www.example.com/path\x00",
        // The two encoding-suite shapes that routed the lift back.
        b"caf\xc3\xa9\x00dcba",
        b"caf\xc3\x00dcba",
        // Invalid UTF-8 the C engine tolerates (decoded bytewise or dropped
        // at a truncated tail) but RE2 can never match.
        b"caf\xc3",
        b"a\xffb",
        b"\xc3\x28",
        b"\xed\xa0\x80xyz",
        b"\xc0\xafabc",
        b"caf\xc3\xa9\xf0\x9f",
    ];
    let patterns: &[&[u8]] = &[
        // The two encoding-suite patterns, verbatim.
        b"^caf(.)$",
        b"^caf(.)dcba$",
        b"a",
        b"a+",
        b"a|ab",
        b".",
        b"x*",
        b"f$",
        b"^caf",
        b"b[^b]+",
        b"(a)(b)?",
        br"^https?://(?:www\.)?([^/]+)/.*$",
    ];
    for p in patterns {
        regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
        assert!(
            regexp_alt::dispatch(p, REG_ADVANCED, b"clean").unwrap().is_some(),
            "expected {:?} to dispatch to re2 on clean subjects",
            String::from_utf8_lossy(p)
        );
        for s in subjects {
            assert!(
                regexp_alt::dispatch(p, REG_ADVANCED, s).unwrap().is_none(),
                "expected subject {s:?} to fall back to spencer"
            );
            assert_engine_parity(m, s, p);
        }
    }

    // NUL-bearing patterns classify to Spencer (Spencer's pattern view stops
    // at the NUL; RE2 would compile the full byte string).
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
    assert!(regexp_alt::dispatch(b"a\x00b", REG_ADVANCED, b"clean").unwrap().is_none());
    for s in [&b"ab"[..], b"a\x00b", b"a"] {
        assert_engine_parity(m, s, b"a\x00b");
    }
}

// Long-haystack boundaries: the RE2 shim carries i32 lengths/offsets and the
// data guard scans the full subject — matches at the far end, NUL/invalid
// bytes deep in the subject, and empty-match walks must all hold parity.
#[test]
fn long_haystack_differential() {
    full_setup();
    if !regexp_alt::re2_available() {
        return;
    }
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let mut clean = "ab".repeat(32 * 1024);
    clean.push_str("needle");
    let mut nul_deep = clean.clone().into_bytes();
    nul_deep[48 * 1024] = 0;
    let mut invalid_deep = clean.clone().into_bytes();
    invalid_deep[48 * 1024] = 0xff;
    let mut long_url = String::from("http://www.example.com/");
    long_url.push_str(&"p/".repeat(16 * 1024));
    long_url.push_str("end");

    for p in [
        &b"needle$"[..],
        b"(needle)$",
        b"^ab|needle$",
        br"^https?://(?:www\.)?([^/]+)/.*$",
        b"e.d",
    ] {
        for s in [clean.as_bytes(), &nul_deep, &invalid_deep, long_url.as_bytes()] {
            assert_engine_parity(m, s, p);
        }
    }
    // Empty-match walk (per-character advance) at a boundary-crossing size.
    assert_engine_parity(m, &clean.as_bytes()[..2048], b"x*");
    assert_engine_parity(m, &nul_deep[47 * 1024..49 * 1024], b"x*");
}

// Generated adversarial patterns within the compatible class: a seeded
// grammar over greedy quantifiers, classes, groups, alternation and anchors;
// every generated pattern must both classify compatible and agree with
// Spencer across generated haystacks.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound
    }
}

fn gen_atom(rng: &mut Lcg, depth: u32, out: &mut String) {
    match rng.next(if depth == 0 { 6 } else { 8 }) {
        0 => out.push(['a', 'b', 'c', '0', '/'][rng.next(5)]),
        1 => out.push('.'),
        2 => out.push_str(["[ab]", "[^a]", "[a-c0-9]", "[^/,]", "[é]"][rng.next(5)]),
        3 => out.push('é'),
        4 => out.push_str(["\\.", "\\*", "\\n"][rng.next(3)]),
        5 => out.push(['x', ' ', ','][rng.next(3)]),
        6 => {
            out.push('(');
            gen_alt(rng, depth - 1, out);
            out.push(')');
        }
        _ => {
            out.push_str("(?:");
            gen_alt(rng, depth - 1, out);
            out.push(')');
        }
    }
}

fn gen_concat(rng: &mut Lcg, depth: u32, out: &mut String) {
    for _ in 0..rng.next(4) + 1 {
        gen_atom(rng, depth, out);
        match rng.next(8) {
            0 => out.push('*'),
            1 => out.push('+'),
            2 => out.push('?'),
            3 => out.push_str(["{2}", "{1,3}", "{0,2}", "{2,}"][rng.next(4)]),
            _ => {}
        }
    }
}

fn gen_alt(rng: &mut Lcg, depth: u32, out: &mut String) {
    gen_concat(rng, depth, out);
    for _ in 0..rng.next(3) {
        out.push('|');
        gen_concat(rng, depth, out);
    }
}

fn gen_pattern(rng: &mut Lcg) -> String {
    let mut p = String::new();
    if rng.next(4) == 0 {
        p.push('^');
    }
    gen_alt(rng, 2, &mut p);
    if rng.next(4) == 0 {
        p.push('$');
    }
    p
}

fn gen_haystack(rng: &mut Lcg) -> String {
    const CHARS: &[char] = &['a', 'b', 'c', '0', '1', '/', ',', ' ', '\n', 'é', 'x'];
    (0..rng.next(13)).map(|_| CHARS[rng.next(CHARS.len())]).collect()
}

#[test]
fn generated_adversarial_parity() {
    full_setup();
    if !regexp_alt::re2_available() {
        return;
    }
    let cx = MemoryContext::new("test");
    let m = cx.mcx();

    let mut rng = Lcg(0x5eed_2026_0707);
    let haystacks: Vec<String> = (0..8).map(|_| gen_haystack(&mut rng)).collect();
    // Large generated patterns overflow the classifier's complexity budget
    // and fail closed — parity must hold either way, but the run is only
    // meaningful if most of the corpus actually dispatches to RE2.
    let mut admitted = 0u32;
    for _ in 0..300 {
        let p = gen_pattern(&mut rng);
        if regexp_alt::re2_compatible(p.as_bytes(), REG_ADVANCED) {
            admitted += 1;
        }
        for s in &haystacks {
            assert_engine_parity(m, s.as_bytes(), p.as_bytes());
        }
        assert_engine_parity(m, b"", p.as_bytes());
    }
    assert!(admitted >= 200, "only {admitted}/300 generated patterns dispatched to re2");
}

// Dispatch-overhead microbench (run with --ignored --nocapture): the auto
// probe on a Spencer-class pattern must be ~zero next to the Spencer match
// itself — one TLS read plus one cached-verdict probe.
#[test]
#[ignore]
fn dispatch_overhead_microbench() {
    full_setup();
    let cx = MemoryContext::new("bench");
    let m = cx.mcx();
    // \w+ classifies incompatible: auto pays the dispatch probe and then
    // runs the identical Spencer path.
    let pat = br"\w+ \w+";
    let hay = b"lorem ipsum dolor sit amet consectetur adipiscing elit";

    let time = |engine: i32| -> f64 {
        regexp_alt::set_regex_engine(engine);
        let iters = 200_000u32;
        // Warm both caches.
        for _ in 0..1000 {
            assert!(RE_compile_and_execute(m, pat, hay, REG_ADVANCED, C, &mut []).unwrap());
        }
        let t0 = std::time::Instant::now();
        for _ in 0..iters {
            assert!(RE_compile_and_execute(m, pat, hay, REG_ADVANCED, C, &mut []).unwrap());
        }
        t0.elapsed().as_nanos() as f64 / iters as f64
    };

    let spencer = time(regexp_alt::REGEX_ENGINE_SPENCER);
    let auto = time(regexp_alt::REGEX_ENGINE_AUTO);
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
    println!(
        "spencer-class bool match: spencer={spencer:.1}ns auto={auto:.1}ns overhead={:.1}ns ({:+.2}%)",
        auto - spencer,
        (auto / spencer - 1.0) * 100.0
    );

    let time_replace = |engine: i32| -> f64 {
        regexp_alt::set_regex_engine(engine);
        let iters = 50_000u32;
        for _ in 0..1000 {
            textregexreplace_noopt(m, hay, pat, b"x", C).unwrap();
        }
        let t0 = std::time::Instant::now();
        for _ in 0..iters {
            textregexreplace_noopt(m, hay, pat, b"x", C).unwrap();
        }
        t0.elapsed().as_nanos() as f64 / iters as f64
    };
    let spencer = time_replace(regexp_alt::REGEX_ENGINE_SPENCER);
    let auto = time_replace(regexp_alt::REGEX_ENGINE_AUTO);
    regexp_alt::set_regex_engine(regexp_alt::REGEX_ENGINE_AUTO);
    println!(
        "spencer-class replace: spencer={spencer:.1}ns auto={auto:.1}ns overhead={:.1}ns ({:+.2}%)",
        auto - spencer,
        (auto / spencer - 1.0) * 100.0
    );
}
