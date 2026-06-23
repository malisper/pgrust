//! Unit tests for the `dbsize` port.
//!
//! The outbound filesystem/catalog seams are process-wide `OnceLock`
//! function-pointer slots, so a [`SEAMS_ONCE`] installs a single shared mock set
//! exactly once for the whole test binary; test-specific scenarios live in the
//! [`STATE`] mutex the mocks read.  The numeric arithmetic is the REAL ported
//! `backend-utils-adt-numeric` driven through an owned [`::mcx::MemoryContext`].
//!
//! Golden vectors are from `postgres-18.3/src/test/regress/expected/dbsize.out`.

use super::*;
use ::mcx::MemoryContext;
use std::sync::{Mutex, Once};

static TEST_LOCK: Mutex<()> = Mutex::new(());
static SEAMS_ONCE: Once = Once::new();

const ENOENT: i32 = 2;

/// A fake directory tree + catalog the filesystem/catalog seams consult.
#[derive(Default)]
struct State {
    dirs: std::collections::BTreeMap<String, Vec<String>>,
    dir_failures: std::collections::BTreeMap<String, i32>,
    files: std::collections::BTreeMap<String, (i64, bool)>,
    stat_errors: std::collections::BTreeMap<String, i32>,
    acl_ok: bool,
    has_stats_priv: bool,
}

static STATE: Mutex<State> = Mutex::new(State {
    dirs: std::collections::BTreeMap::new(),
    dir_failures: std::collections::BTreeMap::new(),
    files: std::collections::BTreeMap::new(),
    stat_errors: std::collections::BTreeMap::new(),
    acl_ok: true,
    has_stats_priv: true,
});

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    let mut g = STATE.lock().unwrap();
    f(&mut g)
}

fn reset_state() {
    with_state(|s| {
        *s = State {
            acl_ok: true,
            has_stats_priv: true,
            ..Default::default()
        };
    });
}

fn install_seams() {
    SEAMS_ONCE.call_once(|| {
        read_dir::set(|path| {
            with_state(|s| {
                if let Some(&errno) = s.dir_failures.get(path) {
                    return OpenDir::Failed { errno };
                }
                match s.dirs.get(path) {
                    Some(names) => OpenDir::Opened(
                        names.iter().map(|n| DirEntry { name: n.clone() }).collect(),
                    ),
                    None => OpenDir::Failed { errno: ENOENT },
                }
            })
        });
        stat::set(|path| {
            with_state(|s| {
                if let Some(&errno) = s.stat_errors.get(path) {
                    return StatResult::Error { errno };
                }
                match s.files.get(path) {
                    Some(&(size, is_dir)) => StatResult::Ok(FileStat { size, is_dir }),
                    None => StatResult::NotFound,
                }
            })
        });
        check_for_interrupts::set(|| Ok(()));
        get_user_id::set(|| Ok(42));
        has_privs_of_role::set(|_, _| Ok(with_state(|s| s.has_stats_priv)));
        object_aclcheck::set(|_, _, _, _| Ok(with_state(|s| s.acl_ok)));
        aclcheck_error::set(|objtype, obj_id| {
            let what = match objtype {
                AclObjectType::Database => "database",
                AclObjectType::Tablespace => "tablespace",
            };
            ereport(ERROR)
                .errcode(types_error::error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied for {what} {obj_id}"))
                .into_error()
        });
        my_database_tablespace::set(|| 1663);
        database_exists::set(|_| Ok(true));
        tablespace_exists::set(|_| Ok(true));
    });
}

// ---------------------------------------------------------------------------
// pg_size_pretty (int64) — pure, no seams/numeric. dbsize.out golden vectors.
// ---------------------------------------------------------------------------

#[test]
fn size_pretty_int64_golden() {
    let cases: &[(i64, &str)] = &[
        (1000, "1000 bytes"),
        (-1000, "-1000 bytes"),
        (10 * 1024 - 1, "10239 bytes"),
        (10 * 1024, "10 kB"),
        (1000000, "977 kB"),
        (-1000000, "-977 kB"),
        (1000000000, "954 MB"),
        (1000000000000, "931 GB"),
        (1000000000000000, "909 TB"),
        (1000000000000000000, "888 PB"),
    ];
    for &(input, expected) in cases {
        assert_eq!(pg_size_pretty(input), expected, "pg_size_pretty({input})");
    }
}

// ---------------------------------------------------------------------------
// pg_size_bytes / pg_size_pretty_numeric — real numeric crate.
// ---------------------------------------------------------------------------

#[test]
fn size_bytes_basic_units() {
    let ctx = MemoryContext::new("dbsize-test");
    let mcx = ctx.mcx();

    let cases: &[(&str, i64)] = &[
        ("1", 1),
        ("123bytes", 123),
        ("1kB", 1024),
        ("1MB", 1048576),
        (" 1 GB ", 1073741824),
        ("1.5 GB", 1610612736),
        ("1TB", 1099511627776),
        ("3000 B", 3000),
        ("-1kB", -1024),
        ("1e3 kB", 1024000),
    ];
    for &(input, expected) in cases {
        let got = pg_size_bytes(mcx, input.as_bytes())
            .unwrap_or_else(|e| panic!("pg_size_bytes({input:?}) errored: {e:?}"));
        assert_eq!(got, expected, "pg_size_bytes({input:?})");
    }
}

#[test]
fn size_bytes_invalid() {
    let ctx = MemoryContext::new("dbsize-test");
    let mcx = ctx.mcx();
    // No digits.
    assert!(pg_size_bytes(mcx, b"foo").is_err());
    // Unknown unit.
    assert!(pg_size_bytes(mcx, b"1 AB").is_err());
}

#[test]
fn size_pretty_numeric_golden() {
    let ctx = MemoryContext::new("dbsize-test");
    let mcx = ctx.mcx();

    let mk = |s: &str| adt_numeric::io::numeric_in(mcx, s, -1).unwrap();

    let cases: &[(&str, &str)] = &[
        ("1000", "1000 bytes"),
        ("10240", "10 kB"),
        ("1000000", "977 kB"),
        ("1000000000", "954 MB"),
        ("-1000000", "-977 kB"),
    ];
    for &(input, expected) in cases {
        let num = mk(input);
        let got = pg_size_pretty_numeric(mcx, &num).unwrap();
        assert_eq!(got, expected, "pg_size_pretty_numeric({input})");
    }
}

// ---------------------------------------------------------------------------
// db_dir_size + calculate_*_size via the mocked filesystem.
// ---------------------------------------------------------------------------

#[test]
fn db_dir_size_walk() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams();
    reset_state();
    with_state(|s| {
        s.dirs
            .insert("base/16384".into(), vec![".".into(), "..".into(), "1259".into(), "1259.1".into()]);
        s.files.insert("base/16384/1259".into(), (8192, false));
        s.files.insert("base/16384/1259.1".into(), (4096, false));
    });
    assert_eq!(db_dir_size("base/16384").unwrap(), 12288);
    // Missing dir -> 0.
    assert_eq!(db_dir_size("base/does-not-exist").unwrap(), 0);
}

#[test]
fn db_dir_size_stat_error_propagates() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams();
    reset_state();
    with_state(|s| {
        s.dirs.insert("base/9".into(), vec!["bad".into()]);
        s.stat_errors.insert("base/9/bad".into(), 13); // EACCES
    });
    assert!(db_dir_size("base/9").is_err());
}

#[test]
fn database_size_acl_denied() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams();
    reset_state();
    with_state(|s| {
        s.acl_ok = false;
        s.has_stats_priv = false;
    });
    // ACL check fails -> aclcheck_error.
    assert!(pg_database_size_oid(16384).is_err());
}

#[test]
fn database_size_sums_base_and_tablespaces() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams();
    reset_state();
    with_state(|s| {
        s.dirs.insert("base/16384".into(), vec!["a".into()]);
        s.files.insert("base/16384/a".into(), (100, false));
        // pg_tblspc scan.
        s.dirs.insert(PG_TBLSPC_DIR.into(), vec![".".into(), "..".into(), "16500".into()]);
        let ts = format!("{PG_TBLSPC_DIR}/16500/{TABLESPACE_VERSION_DIRECTORY}/16384");
        s.dirs.insert(ts.clone(), vec!["b".into()]);
        s.files.insert(format!("{ts}/b"), (50, false));
    });
    assert_eq!(pg_database_size_oid(16384).unwrap(), Some(150));
}
