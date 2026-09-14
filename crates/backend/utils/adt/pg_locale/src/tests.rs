use super::*;
use std::sync::Once;

use mcx::PgString as PgStr;

// Set-once process-global seams dispatch through thread-local fixtures so
// each test thread configures its own catalog rows.
thread_local! {
    static TEST_DB_ROW: Cell<Option<TestDbRow>> = const { Cell::new(None) };
    static TEST_COLL_ROW: Cell<Option<TestCollRow>> = const { Cell::new(None) };
}

#[derive(Clone, Copy)]
struct TestDbRow {
    provider: u8,
    collate: &'static str,
    ctype: &'static str,
    locale: Option<&'static str>,
}

#[derive(Clone, Copy)]
struct TestCollRow {
    provider: u8,
    collate: Option<&'static str>,
    ctype: Option<&'static str>,
    locale: Option<&'static str>,
    version: Option<&'static str>,
}

fn install_db_stub() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        pg_database_seams::search_database_syscache::set(|mcx, dboid| {
            let Some(row) = TEST_DB_ROW.with(Cell::get) else {
                return Ok(None);
            };
            let s = |v: &str| PgStr::from_str_in(v, mcx);
            Ok(Some(pg_database_seams::PgDatabaseForm {
                oid: dboid,
                datname: s("testdb")?,
                datdba: 10,
                datistemplate: false,
                dattablespace: 1663,
                datallowconn: true,
                dathasloginevt: false,
                datconnlimit: -1,
                datfrozenxid: 0,
                datminmxid: 0,
                encoding: 6,
                datlocprovider: row.provider,
                datcollate: s(row.collate)?,
                datctype: s(row.ctype)?,
                datlocale: row.locale.map(s).transpose()?,
                daticurules: None,
                datcollversion: None,
            }))
        });
    });
}

fn install_coll_stub() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_collation_locale_row::set(|mcx, _collid| {
            let Some(row) = TEST_COLL_ROW.with(Cell::get) else {
                return Ok(None);
            };
            let s = |v: &'static str| PgStr::from_str_in(v, mcx);
            let mut collname = types_tuple::NameData::default();
            collname.namestrcpy("testcoll");
            Ok(Some(syscache_seams::PgCollationLocaleRow {
                collname,
                collnamespace: 11,
                collprovider: row.provider,
                collisdeterministic: true,
                collencoding: -1,
                collcollate: row.collate.map(s).transpose()?,
                collctype: row.ctype.map(s).transpose()?,
                colllocale: row.locale.map(s).transpose()?,
                collicurules: None,
                collversion: row.version.map(s).transpose()?,
            }))
        });
    });
}

#[test]
fn c_collation_resolves_without_catalog() {
    let l = pg_newlocale_from_collation(C_COLLATION_OID).unwrap();
    assert_eq!(l.provider, COLLPROVIDER_LIBC);
    assert!(l.deterministic && l.collate_is_c && l.ctype_is_c && !l.is_default);
    assert!(collation_is_deterministic(C_COLLATION_OID).unwrap());
}

#[test]
fn varstr_cmp_locale_c_is_memcmp_with_length_tiebreak() {
    let c = |a: &[u8], b: &[u8]| varstr_cmp_locale(C_COLLATION_OID, a, b).unwrap();
    assert_eq!(c(b"abc", b"abc"), 0);
    assert!(c(b"abc", b"abd") < 0);
    assert!(c(b"abd", b"abc") > 0);
    assert!(c(b"ab", b"abc") < 0);
    assert!(c(b"abc", b"ab") > 0);
}

#[test]
fn invalid_oid_is_cache_lookup_failed() {
    let err = pg_newlocale_from_collation(0).unwrap_err();
    assert_eq!(err.message(), "cache lookup failed for collation 0");
}

#[test]
#[should_panic(expected = "default_locale read before init_database_collation")]
fn default_locale_before_init_panics() {
    let _ = pg_newlocale_from_collation(DEFAULT_COLLATION_OID);
}

#[test]
fn init_database_collation_libc_c() {
    install_db_stub();
    TEST_DB_ROW.with(|r| {
        r.set(Some(TestDbRow {
            provider: COLLPROVIDER_LIBC,
            collate: "C",
            ctype: "POSIX",
            locale: None,
        }))
    });
    init_database_collation().unwrap();
    let l = pg_newlocale_from_collation(DEFAULT_COLLATION_OID).unwrap();
    assert_eq!(l.provider, COLLPROVIDER_LIBC);
    assert!(l.is_default && l.collate_is_c && l.ctype_is_c && l.deterministic);
    assert_eq!(varstr_cmp_locale(DEFAULT_COLLATION_OID, b"a", b"ab").unwrap(), -1);
    assert!(collation_is_deterministic(DEFAULT_COLLATION_OID).unwrap());
}

#[test]
fn init_database_collation_builtin() {
    install_db_stub();
    TEST_DB_ROW.with(|r| {
        r.set(Some(TestDbRow {
            provider: COLLPROVIDER_BUILTIN,
            collate: "en_US.UTF-8",
            ctype: "en_US.UTF-8",
            locale: Some("C"),
        }))
    });
    init_database_collation().unwrap();
    let l = pg_newlocale_from_collation(DEFAULT_COLLATION_OID).unwrap();
    assert_eq!(l.provider, COLLPROVIDER_BUILTIN);
    assert!(l.is_default && l.collate_is_c && l.ctype_is_c);
    assert_eq!(l.builtin_locale, Some("C"));
    assert!(!l.builtin_casemap_full);
}

#[test]
fn init_database_collation_libc_noncc_builds_collator() {
    install_db_stub();
    TEST_DB_ROW.with(|r| {
        r.set(Some(TestDbRow {
            provider: COLLPROVIDER_LIBC,
            collate: "en_US.UTF-8",
            ctype: "en_US.UTF-8",
            locale: None,
        }))
    });
    init_database_collation().unwrap();
    let l = pg_newlocale_from_collation(DEFAULT_COLLATION_OID).unwrap();
    assert!(l.is_default && l.deterministic);
    assert!(!l.collate_is_c && !l.ctype_is_c);
    // en_US.UTF-8 strcoll: case-insensitive-ish primary weights, unlike memcmp.
    assert!(l.pg_strncoll(b"apple", b"Banana").unwrap() < 0);
    assert!(l.pg_strncoll(b"a", b"a").unwrap() == 0);
}

#[test]
fn collation_cache_interns_and_reuses() {
    install_coll_stub();
    TEST_COLL_ROW.with(|r| {
        r.set(Some(TestCollRow {
            provider: COLLPROVIDER_BUILTIN,
            collate: None,
            ctype: None,
            locale: Some("C"),
            version: Some("1"),
        }))
    });
    let a = pg_newlocale_from_collation(12345).unwrap();
    let b = pg_newlocale_from_collation(12345).unwrap();
    assert!(core::ptr::eq(a, b));
    assert!(a.collate_is_c && !a.is_default);
    assert_eq!(a.builtin_locale, Some("C"));
}

#[test]
fn missing_collation_row_is_cache_lookup_failed() {
    install_coll_stub();
    TEST_COLL_ROW.with(|r| r.set(None));
    let err = pg_newlocale_from_collation(54321).unwrap_err();
    assert_eq!(err.message(), "cache lookup failed for collation 54321");
}

#[test]
fn collation_actual_versions_match_c() {
    assert_eq!(
        get_collation_actual_version(COLLPROVIDER_BUILTIN, "C").unwrap(),
        Some("1".to_owned())
    );
    assert_eq!(
        get_collation_actual_version(COLLPROVIDER_BUILTIN, "PG_UNICODE_FAST").unwrap(),
        Some("1".to_owned())
    );
    let err = get_collation_actual_version(COLLPROVIDER_BUILTIN, "en_US").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);

    assert_eq!(get_collation_actual_version(COLLPROVIDER_LIBC, "C").unwrap(), None);
    assert_eq!(get_collation_actual_version(COLLPROVIDER_LIBC, "posix").unwrap(), None);
    assert_eq!(get_collation_actual_version(COLLPROVIDER_LIBC, "C.UTF-8").unwrap(), None);
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    assert!(get_collation_actual_version(COLLPROVIDER_LIBC, "en_US.utf8")
        .unwrap()
        .is_some());
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    assert_eq!(
        get_collation_actual_version(COLLPROVIDER_LIBC, "en_US.UTF-8").unwrap(),
        None
    );

    // ICU: version comes from the system libicu (ucol_getVersion); pinned
    // catalog-exactness is gated by collate-e2e against the in-pod C twin.
    let icu_ver = get_collation_actual_version(COLLPROVIDER_ICU, "en").unwrap();
    assert!(icu_ver.is_some_and(|v| !v.is_empty() && v.contains('.')));
}

// icu_unicode_version (varlena.c): C returns the header constant
// U_UNICODE_VERSION ("16.0", "15.1", ... — always dotted-numeric) under
// USE_ICU, else NULL. Ours reports the loaded library's u_getUnicodeVersion
// rendered by ICU's own u_versionToString; assert the C constant's shape.
// None = no loadable libicu, the C-without---with-icu arm.
#[test]
fn icu_unicode_version_str_has_c_constant_shape() {
    if let Some(v) = crate::icu_unicode_version_str() {
        let parts: Vec<&str> = v.split('.').collect();
        assert!(parts.len() >= 2, "U_UNICODE_VERSION is dotted: {v:?}");
        assert!(
            parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())),
            "numeric fields: {v:?}"
        );
        // Stable across calls (OnceLock) — the fmgr result must not move.
        assert_eq!(crate::icu_unicode_version_str(), Some(v));
    }
}

#[test]
fn builtin_validators_match_c() {
    assert_eq!(builtin_locale_encoding("C").unwrap(), -1);
    assert_eq!(builtin_locale_encoding("C.UTF-8").unwrap(), PG_UTF8);
    assert_eq!(builtin_locale_encoding("PG_UNICODE_FAST").unwrap(), PG_UTF8);
    assert_eq!(
        builtin_locale_encoding("en_US").unwrap_err().sqlstate(),
        ERRCODE_WRONG_OBJECT_TYPE
    );

    assert_eq!(builtin_validate_locale(PG_UTF8, "C.UTF8").unwrap(), "C.UTF-8");
    assert_eq!(builtin_validate_locale(-1, "C").unwrap(), "C");
    assert_eq!(builtin_validate_locale(0, "C").unwrap(), "C");
    assert_eq!(
        builtin_validate_locale(0, "C.UTF-8").unwrap_err().sqlstate(),
        ERRCODE_WRONG_OBJECT_TYPE
    );
    assert_eq!(
        builtin_validate_locale(PG_UTF8, "bogus").unwrap_err().sqlstate(),
        ERRCODE_WRONG_OBJECT_TYPE
    );
}

#[test]
fn pg_perm_setlocale_c_and_bogus() {
    let ctx = MemoryContext::new("t");
    let got = pg_perm_setlocale(ctx.mcx(), libc::LC_MONETARY, "C").unwrap();
    assert_eq!(got.unwrap().as_str(), "C");
    assert_eq!(std::env::var("LC_MONETARY").unwrap(), "C");

    let got = pg_perm_setlocale(ctx.mcx(), libc::LC_MONETARY, "bogus_locale.nope").unwrap();
    assert!(got.is_none());
}

#[test]
fn check_locale_hooks() {
    assert!(check_locale_monetary("C").unwrap());
    assert!(check_locale_numeric("POSIX").unwrap());
    assert!(!check_locale_time("bogus_locale.nope").unwrap());
    assert!(check_locale_messages("", true).unwrap());
    assert!(!check_locale_messages("", false).unwrap());
    let (ok, canon) = check_locale(libc::LC_MONETARY, "C").unwrap();
    assert!(ok);
    assert_eq!(canon.as_deref(), Some("C"));
}

#[test]
fn database_ctype_flag_round_trips() {
    assert!(!database_ctype_is_c());
    set_database_ctype_is_c(true);
    assert!(database_ctype_is_c());
}

// Regression pin for the diesel-parallel-suite SIGABRT: concurrent GUC
// locale checks (SET lc_messages on every diesel connection) ran C's
// setlocale save/set/restore dance on many backend threads at once;
// setlocale mutates process-global storage, and the race corrupted libc's
// locale heap (malloc abort, whole-server SIGABRT — no Rust panic). The
// dance is now a private newlocale probe, so this hammering must be safe.
// At the pre-fix code this test aborts the test process on macOS.
#[test]
fn concurrent_locale_checks_are_threadsafe() {
    let threads: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                for i in 0..2000 {
                    // Alternate names so every check is a genuine
                    // transition in the pre-fix save/set/restore dance.
                    let name = if (t + i) % 2 == 0 { "en_US.UTF-8" } else { "C" };
                    let _ = check_locale_messages(name, false).unwrap();
                    assert!(check_locale_monetary("C").unwrap());
                    let _ = check_locale_time(name).unwrap();
                    assert!(!check_locale_numeric("bogus_locale.nope").unwrap());
                    let (ok, canon) = check_locale(libc::LC_MONETARY, "POSIX").unwrap();
                    assert!(ok);
                    assert_eq!(canon.as_deref(), Some(setup::canonical_locale_name("POSIX")));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("locale-check thread panicked");
    }
}

// CREATE DATABASE ... LC_COLLATE 'POSIX' on a C-locale cluster: C's
// check_locale hands createdb setlocale's canonical name, "C" on glibc and
// musl, so the template comparison passes; macOS setlocale keeps "POSIX".
#[test]
fn posix_canonicalises_like_setlocale() {
    let expect = if cfg!(target_os = "linux") { "C" } else { "POSIX" };
    assert_eq!(setup::canonical_locale_name("POSIX"), expect);
    assert_eq!(setup::canonical_locale_name("C"), "C");
    assert_eq!(setup::canonical_locale_name("en_US.UTF-8"), "en_US.UTF-8");
    let (ok, canon) = check_locale(libc::LC_COLLATE, "POSIX").unwrap();
    assert!(ok);
    assert_eq!(canon.as_deref(), Some(expect));
}

#[test]
fn seams_install_and_dispatch() {
    static ONCE: Once = Once::new();
    ONCE.call_once(init_seams);
    assert_eq!(
        pg_locale_seams::varstr_cmp_locale::call(C_COLLATION_OID, b"x", b"xy").unwrap(),
        -1
    );
    assert!(pg_locale_seams::collation_is_deterministic::call(C_COLLATION_OID).unwrap());
    pg_locale_seams::set_database_ctype_is_c::call(true);
    assert!(database_ctype_is_c());
    let ctx = MemoryContext::new("t");
    assert_eq!(
        pg_locale_seams::get_collation_actual_version::call(ctx.mcx(), COLLPROVIDER_BUILTIN, "C")
            .unwrap()
            .unwrap()
            .as_str(),
        "1"
    );
    assert_eq!(
        guc_tables::vars::locale_monetary.read().as_deref(),
        Some("C")
    );
    guc_tables::vars::locale_time.write(Some("C".to_owned()));
    assert!((guc_tables::hooks::check_locale_time.get())(
        &mut Some("C".to_owned()),
        &mut None,
        types_guc::GucSource::PGC_S_DEFAULT
    )
    .unwrap());
    assert_eq!(guc_tables::vars::icu_validation_level.read(), WARNING.0);
}

// Differential vs live PostgreSQL 18.3 (same-machine libc). Gated on
// PG_LOCALE_DIFF_DIR = dir holding scratchpad/locale-diff outputs.
#[test]
fn live_pg_differential() {
    let Ok(dir) = std::env::var("PG_LOCALE_DIFF_DIR") else {
        return;
    };
    mbutils::SetDatabaseEncoding(6).unwrap();
    let ctx = MemoryContext::new("diff");
    let mcx = ctx.mcx();

    let builtin = |full: bool| PgLocale {
        provider: COLLPROVIDER_BUILTIN,
        deterministic: true,
        collate_is_c: true,
        ctype_is_c: false,
        is_default: false,
        builtin_locale: None,
        builtin_casemap_full: full,
        lt: libc_locale::LibcLocale::NONE,
        icu: crate::icu::IcuLocale::NONE,
    };

    let run = |f: fn(Mcx<'_>, &mut [u8], &[u8], &PgLocale) -> PgResult<usize>,
               src: &[u8],
               loc: &PgLocale|
     -> String {
        let mut dst = vec![0u8; src.len() + 1];
        let mut n = f(mcx, &mut dst, src, loc).unwrap();
        if n + 1 > dst.len() {
            dst.resize(n + 1, 0);
            n = f(mcx, &mut dst, src, loc).unwrap();
        }
        let end = dst[..n].iter().position(|&b| b == 0).unwrap_or(n);
        String::from_utf8(dst[..end].to_vec()).unwrap()
    };

    let mut checked = 0usize;
    for (file, full) in [
        ("expected_case_cutf8.tsv", false),
        ("expected_case_pgufast.tsv", true),
    ] {
        let loc = builtin(full);
        let data = std::fs::read_to_string(format!("{dir}/{file}")).unwrap();
        for line in data.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(f.len(), 5, "{file}: {line}");
            let s = f[0].as_bytes();
            assert_eq!(run(pg_strlower, s, &loc), f[1], "lower({}) full={full}", f[0]);
            assert_eq!(run(pg_strupper, s, &loc), f[2], "upper({}) full={full}", f[0]);
            assert_eq!(run(pg_strtitle, s, &loc), f[3], "initcap({}) full={full}", f[0]);
            assert_eq!(run(pg_strfold, s, &loc), f[4], "casefold({}) full={full}", f[0]);
            checked += 4;
        }
    }

    for (locname, file) in [
        ("en_US.UTF-8", "expected_order_enus.tsv"),
        ("de_DE.UTF-8", "expected_order_de_DE.tsv"),
        ("fr_FR.UTF-8", "expected_order_fr_FR.tsv"),
        ("sv_SE.UTF-8", "expected_order_sv_SE.tsv"),
        ("tr_TR.UTF-8", "expected_order_tr_TR.tsv"),
    ] {
        let loc = create_pg_locale_libc(locname, locname).unwrap();
        let data = std::fs::read_to_string(format!("{dir}/{file}")).unwrap();
        let expected: Vec<&str> = data.lines().collect();
        let mut ours: Vec<&str> = expected.clone();
        ours.sort_by(|a, b| {
            varstr_cmp_locale_with(&loc, a.as_bytes(), b.as_bytes()).cmp(&0)
        });
        assert_eq!(ours, expected, "{locname} strcoll order");
        checked += expected.len();
    }
    eprintln!("live_pg_differential: {checked} comparisons OK");
}

#[cfg(test)]
fn varstr_cmp_locale_with(locale: &PgLocale, arg1: &[u8], arg2: &[u8]) -> i32 {
    if locale.collate_is_c {
        return varlena::varstrfastcmp_c(arg1, arg2);
    }
    if arg1 == arg2 {
        return 0;
    }
    let result = locale.pg_strncoll(arg1, arg2).unwrap();
    if result == 0 && locale.deterministic {
        return varlena::varstrfastcmp_c(arg1, arg2);
    }
    result
}

// Message/SQLSTATE verified verbatim against live PG 18.3 on this platform
// (ERROR 22023 + report_newlocale_failure detail).
#[test]
fn newlocale_failure_matches_c() {
    let err = create_pg_locale_libc("xx_XX.UTF-8", "xx_XX.UTF-8").unwrap_err();
    assert_eq!(
        err.message(),
        "could not create locale \"xx_XX.UTF-8\": No such file or directory"
    );
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn chklocale_c_posix_and_bogus() {
    assert_eq!(
        crate::pg_get_encoding_from_locale(Some("C"), false).unwrap(),
        0 // PG_SQL_ASCII
    );
    assert_eq!(
        crate::pg_get_encoding_from_locale(Some("posix"), false).unwrap(),
        0
    );
    assert_eq!(
        crate::pg_get_encoding_from_locale(Some("nonsense"), false).unwrap(),
        -1
    );
}

#[test]
fn cache_locale_time_c_locale_names() {
    let ctx = mcx::MemoryContext::new("lc_time_test");
    let names = crate::cache_locale_time(ctx.mcx()).unwrap();
    assert_eq!(names.abbrev_months[0], b"Jan");
    assert_eq!(names.full_months[1].to_ascii_lowercase(), b"february");
    assert_eq!(names.abbrev_days[0], b"Sun");
    assert_eq!(names.full_days[6].to_ascii_lowercase(), b"saturday");
}

// upstream 9021c8f3cabc (18.6): unicode_case.c: defend against truncated UTF8.
#[test]
fn strtitle_builtin_stops_at_invalid_utf8() {
    for full in [false, true] {
        let loc = PgLocale {
            provider: COLLPROVIDER_BUILTIN,
            deterministic: true,
            collate_is_c: true,
            ctype_is_c: false,
            is_default: false,
            builtin_locale: None,
            builtin_casemap_full: full,
            lt: libc_locale::LibcLocale::NONE,
            icu: crate::icu::IcuLocale::NONE,
        };
        let mut dst = [0u8; 16];
        let n = builtin_case::strtitle_builtin(&mut dst, b"abc\xCE", &loc);
        assert_eq!((n, &dst[..n]), (3, &b"Abc"[..]), "full={full}");
        let n = builtin_case::strtitle_builtin(&mut dst, b"abc\xF8xyz", &loc);
        assert_eq!((n, &dst[..n]), (3, &b"Abc"[..]), "full={full}");
    }
}

// upstream 5f003855e7f0 (18.6): the C ctype never reaches libc with a NULL locale_t
// (pre-fix: the LibcLocale::get debug_assert on the UTF-8 wide-char arm).
#[test]
fn c_locale_case_mapping_is_ascii_without_libc() {
    mbutils::SetDatabaseEncoding(6).unwrap();
    let ctx = MemoryContext::new("c-case");
    let mcx = ctx.mcx();
    let run = |f: fn(Mcx<'_>, &mut [u8], &[u8], &PgLocale) -> PgResult<usize>,
               src: &[u8]|
     -> Vec<u8> {
        let mut dst = vec![0xAAu8; src.len() + 2];
        let n = f(mcx, &mut dst, src, &C_LOCALE).unwrap();
        assert_eq!(n, src.len());
        assert_eq!(dst[n], 0, "NUL-terminated when it fits");
        assert_eq!(dst[n + 1], 0xAA, "nothing written past the terminator");
        dst.truncate(n);
        dst
    };
    assert_eq!(
        run(pg_strlower, "FeBrUaRy \u{c9}T\u{c9} 1\0X".as_bytes()),
        "february \u{c9}t\u{c9} 1\0x".as_bytes()
    );
    assert_eq!(
        run(pg_strupper, "hello, w\u{f6}rld 42".as_bytes()),
        "HELLO, W\u{f6}RLD 42".as_bytes()
    );
    assert_eq!(run(pg_strtitle, b"hello wORLD 4x b-c \xc3\xa9a"), b"Hello World 4x B-C \xc3\xa9A");
    assert_eq!(run(pg_strfold, b"MiXeD"), b"mixed");

    // a short destination gets the prefix, no terminator, and the full length
    let mut short = [0xAAu8; 3];
    assert_eq!(pg_strupper(mcx, &mut short, b"abcde", &C_LOCALE).unwrap(), 5);
    assert_eq!(short, *b"ABC");
    // exactly srclen bytes of room: no terminator either
    let mut exact = [0xAAu8; 5];
    assert_eq!(pg_strlower(mcx, &mut exact, b"ABCDE", &C_LOCALE).unwrap(), 5);
    assert_eq!(exact, *b"abcde");
}

// pg_locale_icu.c:838-848 (init_icu_converter): a database encoding without
// an ICU converter name is ereport(ERROR) 0A000 "encoding \"%s\" not
// supported by ICU", and a failing ucnv_open is XX000 "could not open ICU
// converter for encoding \"%s\": %s"; the non-UTF-8 strncoll / strnxfrm /
// strnxfrm_prefix arms propagate them (the port unwrapped them with panic!).
// SQL_ASCII has no ICU converter name (pg_enc2icu_tbl), so it reaches the
// first arm without touching the collator.
#[test]
fn icu_converter_failure_is_a_pg_error() {
    mbutils::SetDatabaseEncoding(0).unwrap(); // PG_SQL_ASCII
    let locale = PgLocale {
        provider: COLLPROVIDER_ICU,
        deterministic: true,
        collate_is_c: false,
        ctype_is_c: false,
        is_default: false,
        builtin_locale: None,
        builtin_casemap_full: false,
        lt: libc_locale::LibcLocale::NONE,
        icu: crate::icu::IcuLocale::null_collator(false),
    };
    let expect = |err: Box<PgError>| {
        assert_eq!(err.message(), "encoding \"SQL_ASCII\" not supported by ICU");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    };
    expect(locale.pg_strncoll(b"a", b"b").unwrap_err());
    let mut dest = [0u8; 32];
    expect(locale.pg_strnxfrm(&mut dest, b"a").unwrap_err());
    expect(locale.pg_strnxfrm_prefix(&mut dest, b"a").unwrap_err());
    mbutils::SetDatabaseEncoding(6).unwrap();
}

// pg_locale_icu.c:838-848 (init_icu_converter): a database encoding without
// an ICU converter name is ereport(ERROR) 0A000 "encoding \"%s\" not
// supported by ICU" (and a failing ucnv_open XX000 "could not open ICU
// converter for encoding \"%s\": %s"); the non-UTF-8 strncoll / strnxfrm /
// strnxfrm_prefix arms must report it, never panic. SQL_ASCII has no ICU
// converter name (pg_enc2icu_tbl), so it reaches the arm without a collator.
#[test]
fn icu_converter_failure_does_not_panic() {
    mbutils::SetDatabaseEncoding(0).unwrap(); // PG_SQL_ASCII
    let locale = PgLocale {
        provider: COLLPROVIDER_ICU,
        deterministic: true,
        collate_is_c: false,
        ctype_is_c: false,
        is_default: false,
        builtin_locale: None,
        builtin_casemap_full: false,
        lt: libc_locale::LibcLocale::NONE,
        icu: crate::icu::IcuLocale::null_collator(false),
    };
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut dest = [0u8; 32];
        let _ = locale.pg_strncoll(b"a", b"b");
        let _ = locale.pg_strnxfrm(&mut dest, b"a");
        let _ = locale.pg_strnxfrm_prefix(&mut dest, b"a");
    }));
    mbutils::SetDatabaseEncoding(6).unwrap();
    assert!(r.is_ok(), "an ICU converter failure panicked instead of being reported");
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn lconv_strings_keep_non_utf8_bytes_under_sql_ascii() {
    mbutils::SetDatabaseEncoding(0).unwrap(); // PG_SQL_ASCII
    let ctx = mcx::MemoryContext::new_bump("t");
    const PG_LATIN1: i32 = 8;
    let got = crate::lconv::db_encoding_convert(ctx.mcx(), PG_LATIN1, b"\xa3").unwrap();
    assert_eq!(got, b"\xa3");
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn default_collation_sb_case_uses_database_ctype() {
    let lt = ["en_US.ISO8859-1", "en_US.ISO-8859-1", "en_US.iso88591"]
        .iter()
        .find_map(|n| libc_locale::make_libc_collator(n, n).ok());
    let Some(lt) = lt else {
        eprintln!("no en_US ISO8859-1 locale installed; skipping");
        return;
    };
    let loc = |is_default: bool, lt: libc_locale::LibcLocale| PgLocale {
        provider: COLLPROVIDER_LIBC,
        deterministic: true,
        collate_is_c: false,
        ctype_is_c: false,
        is_default,
        builtin_locale: None,
        builtin_casemap_full: false,
        lt,
        icu: crate::icu::IcuLocale::NONE,
    };
    let lower = |l: &PgLocale| {
        let mut dst = [0u8; 4];
        libc_locale::strlower_libc_sb(&mut dst, b"\xC4Ab", l);
        dst[..3].to_vec()
    };
    let upper = |l: &PgLocale| {
        let mut dst = [0u8; 4];
        libc_locale::strupper_libc_sb(&mut dst, b"\xE4Ab", l);
        dst[..3].to_vec()
    };
    assert_eq!(lower(&loc(true, lt)), b"\xE4ab");
    assert_eq!(lower(&loc(false, lt)), b"\xE4ab");
    assert_eq!(upper(&loc(true, lt)), b"\xC4AB");
    assert_eq!(lower(&loc(true, libc_locale::LibcLocale::NONE)), b"\xC4ab");
}

// pg_locale_libc.c:581 / pg_locale_icu.c:606: the scratch buffers go through
// palloc, so inputs whose combined buffer exceeds MaxAllocSize are the
// "invalid memory alloc request size" error, not a comparison or conversion.
#[test]
fn collation_scratch_buffers_have_palloc_admission() {
    assert!(libc_locale::strncoll_buf_admission(600_000_000, 473_741_821).is_ok());
    let e = libc_locale::strncoll_buf_admission(600_000_000, 600_000_000).unwrap_err();
    assert_eq!(e.message(), "invalid memory alloc request size 1200000002");
    assert!(icu::uchar_buf_admission(536_870_910).is_ok());
    let e = icu::uchar_buf_admission(536_870_911).unwrap_err();
    assert_eq!(e.message(), "invalid memory alloc request size 1073741824");
}
