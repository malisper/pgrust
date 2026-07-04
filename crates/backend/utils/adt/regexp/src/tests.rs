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
    utf8();
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
    utf8();
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
    utf8();
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
