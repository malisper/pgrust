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
    let expected: &[(Oid, &str)] = &[
        (79, "nameregexeq"),
        (1238, "texticregexeq"),
        (1239, "texticregexne"),
        (1240, "nameicregexeq"),
        (1241, "nameicregexne"),
        (1252, "nameregexne"),
        (1254, "textregexeq"),
        (1256, "textregexne"),
        (1656, "bpcharicregexeq"),
        (1657, "bpcharicregexne"),
        (1658, "bpcharregexeq"),
        (1659, "bpcharregexne"),
    ];
    assert_eq!(builtins::REGEXP_BUILTINS.len(), expected.len());
    for (b, (oid, name)) in builtins::REGEXP_BUILTINS.iter().zip(expected) {
        assert_eq!((b.foid, b.name), (*oid, *name));
        assert_eq!(b.nargs, 2);
        assert!(b.strict);
        assert!(!b.retset);
    }
}
