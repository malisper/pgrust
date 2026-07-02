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
                dattablespace: 1663,
                datallowconn: true,
                dathasloginevt: false,
                datconnlimit: -1,
                encoding: 6,
                datlocprovider: row.provider,
                datcollate: s(row.collate)?,
                datctype: s(row.ctype)?,
                datlocale: row.locale.map(s).transpose()?,
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
                collcollate: row.collate.map(s).transpose()?,
                collctype: row.ctype.map(s).transpose()?,
                colllocale: row.locale.map(s).transpose()?,
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
#[should_panic(expected = "libc locale_t arm not ported")]
fn init_database_collation_libc_noncc_defers_loud() {
    install_db_stub();
    TEST_DB_ROW.with(|r| {
        r.set(Some(TestDbRow {
            provider: COLLPROVIDER_LIBC,
            collate: "en_US.UTF-8",
            ctype: "en_US.UTF-8",
            locale: None,
        }))
    });
    let _ = init_database_collation();
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

    assert_eq!(get_collation_actual_version(COLLPROVIDER_ICU, "en").unwrap(), None);
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
