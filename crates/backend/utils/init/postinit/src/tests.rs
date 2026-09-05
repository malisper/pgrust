use super::*;

// Test-local accessors for GUC slots whose owner units are unported
// (autovacuum, walsender, lock); values are the C boot_vals.
fn install_test_gucs() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Once;
    static ONCE: Once = Once::new();
    static AV_SLOTS: AtomicI32 = AtomicI32::new(16);
    static WAL_SENDERS: AtomicI32 = AtomicI32::new(10);
    static MAX_LOCKS: AtomicI32 = AtomicI32::new(64);
    ONCE.call_once(|| {
        guc_tables::vars::autovacuum_worker_slots.install(guc_tables::GucVarAccessors {
            get: || AV_SLOTS.load(Ordering::Relaxed),
            set: |v| AV_SLOTS.store(v, Ordering::Relaxed),
        });
        guc_tables::vars::max_wal_senders.install(guc_tables::GucVarAccessors {
            get: || WAL_SENDERS.load(Ordering::Relaxed),
            set: |v| WAL_SENDERS.store(v, Ordering::Relaxed),
        });
        guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
            get: || MAX_LOCKS.load(Ordering::Relaxed),
            set: |v| MAX_LOCKS.store(v, Ordering::Relaxed),
        });
    });
}

#[test]
fn constants_match_headers() {
    assert_eq!(INIT_PG_LOAD_SESSION_LIBS, 0x0001);
    assert_eq!(INIT_PG_OVERRIDE_ALLOW_CONNS, 0x0002);
    assert_eq!(INIT_PG_OVERRIDE_ROLE_LOGIN, 0x0004);
    assert_eq!(MAX_BACKENDS, (1 << 18) - 1);
    assert_eq!(FP_LOCK_GROUPS_PER_BACKEND_MAX, 1024);
    assert_eq!(FP_LOCK_SLOTS_PER_GROUP, 16);
    assert_eq!(TEMPLATE1_DB_OID, 1);
    assert_eq!(DEFAULTTABLESPACE_OID, 1663);
    assert_eq!(DB_ROLE_SETTING_RELATION_ID, 2964);
    assert_eq!(ROLE_PG_USE_RESERVED_CONNECTIONS, 4550);
    assert_eq!(ACL_CONNECT, 1 << 11);
    assert_eq!(types_storage::storage::NUM_SPECIAL_WORKER_PROCS, 2);
}

#[test]
fn split_opts_matches_c() {
    let mut av = Vec::new();
    pg_split_opts(&mut av, "  -c work_mem=64MB   -c search_path=a\\ b ");
    assert_eq!(av, vec!["-c", "work_mem=64MB", "-c", "search_path=a b"]);

    let mut av = Vec::new();
    pg_split_opts(&mut av, r"a\\b \\ x");
    assert_eq!(av, vec![r"a\b", r"\", "x"]);

    let mut av = Vec::new();
    pg_split_opts(&mut av, "   ");
    assert!(av.is_empty());

    // Trailing single escape swallows nothing and terminates the option.
    let mut av = Vec::new();
    pg_split_opts(&mut av, r"end\");
    assert_eq!(av, vec!["end"]);
}

#[test]
fn fastpath_groups_default_is_four() {
    install_test_gucs();
    // max_locks_per_transaction boot value 64 => nextpower2(64)/16 = 4.
    assert_eq!(InitializeFastPathLocks(), 4);
}

#[test]
fn initialize_max_backends_sums_and_gates() {
    install_test_gucs();
    // Boot values: 100 + 16 + 16 + 10 + 2 (max_connections + autovacuum_worker_slots
    // + max_worker_processes + max_wal_senders + NUM_SPECIAL_WORKER_PROCS).
    // max_worker_processes moved 8 -> 16 with the t34 jit/parallel shipped defaults.
    init_small::globals::SetMaxBackends(0);
    InitializeMaxBackends().unwrap();
    assert_eq!(init_small::globals::MaxBackends(), 144);
}

#[test]
fn quote_identifier_shapes() {
    assert_eq!(quote_identifier("plain_db1"), "plain_db1");
    assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
    assert_eq!(quote_identifier("has space"), "\"has space\"");
    assert_eq!(quote_identifier("qu\"ote"), "\"qu\"\"ote\"");
}

#[test]
fn strlcpy_truncates_to_namedatalen() {
    assert_eq!(strlcpy_name("short"), "short");
    let long = "x".repeat(100);
    assert_eq!(strlcpy_name(&long).len(), 63);
}

#[test]
fn timeout_flag_handlers_set_pending_flags() {
    init_small::globals::SetInterruptPending(false);
    init_small::globals::SetTransactionTimeoutPending(false);
    TransactionTimeoutHandler();
    assert!(init_small::globals::TransactionTimeoutPending());
    assert!(init_small::globals::InterruptPending());

    init_small::globals::SetInterruptPending(false);
    IdleInTransactionSessionTimeoutHandler();
    assert!(init_small::globals::IdleInTransactionSessionTimeoutPending());
    assert!(init_small::globals::InterruptPending());

    init_small::globals::SetInterruptPending(false);
    IdleSessionTimeoutHandler();
    assert!(init_small::globals::IdleSessionTimeoutPending());

    init_small::globals::SetInterruptPending(false);
    IdleStatsUpdateTimeoutHandler();
    assert!(init_small::globals::IdleStatsUpdateTimeoutPending());

    init_small::globals::SetInterruptPending(false);
    ClientCheckTimeoutHandler();
    assert!(init_small::globals::CheckClientConnectionPending());
}

#[test]
fn init_postgres_fails_loud_at_first_unported_seam() {
    // Boot-readiness probe: bootstrap-mode InitPostgres must reach the C
    // sequence and stop at a NAMED uninstalled seam, not a mystery. With
    // landed units wired, the first loud stop for a bootstrap backend is
    // pgstat_beinit (backend_status.c unported); InitProcessPhase2 requires
    // shmem-armed PGPROC, so with no proc it panics with its own name first.
    let result = std::panic::catch_unwind(|| {
        let top = mcx::MemoryContext::new("postinit test");
        miscinit::SetProcessingMode(types_core::ProcessingMode::BootstrapProcessing);
        let _ = InitPostgres(top.mcx(), None, InvalidOid, None, InvalidOid, 0, None);
    });
    let err = result.expect_err("must panic before completing");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("MyProc") || msg.contains("seam not installed"),
        "panic must name the missing unit, got: {msg}"
    );
}

// UTF-8-only server-encoding carve (docs/design/carve-ratifications.md §11,
// ratified 2026-08-18): CheckMyDatabase refuses non-UTF8/non-SQL_ASCII
// databases with a clean per-database FATAL naming the database, its
// encoding, and the carve.
#[test]
fn utf8_only_connection_gate() {
    assert!(unsupported_database_encoding_error("db", wchar::PG_UTF8).is_none());
    assert!(unsupported_database_encoding_error("db", wchar::PG_SQL_ASCII).is_none());

    for (enc, ename) in [(wchar::PG_LATIN1, "LATIN1"), (wchar::PG_EUC_JP, "EUC_JP")] {
        let e = unsupported_database_encoding_error("legacy", enc).expect("must be refused");
        assert_eq!(e.level(), types_error::FATAL);
        assert_eq!(e.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            e.message(),
            format!(
                "database \"legacy\" has server encoding \"{ename}\", which is not supported \
                 by pgrust; only \"UTF8\" and \"SQL_ASCII\" server encodings are accepted \
                 (UTF-8-only carve, docs/design/carve-ratifications.md)"
            )
        );
        assert_eq!(
            e.hint(),
            Some(
                "Restore non-UTF8 dumps into a UTF8 database; client_encoding conversion is \
                 supported."
            )
        );
    }
}

// audit-18.6 b051 (postinit.c:530): pg_split_opts appends raw bytes, so a
// multi-byte UTF-8 option value survives the split unchanged.
#[test]
fn split_opts_preserves_utf8_bytes() {
    let mut av = Vec::new();
    pg_split_opts(&mut av, "-c b051.name=café -c search_path=naïve\\ ünïcode");
    assert_eq!(av, ["-c", "b051.name=café", "-c", "search_path=naïve ünïcode"]);
    assert_eq!(av[1].as_bytes(), b"b051.name=caf\xc3\xa9");
}

// audit-18.6 b051 (postinit.c:479 via ruleutils.c:13112-13115): the
// collation-mismatch HINT quotes the database name like C's
// quote_identifier — reserved keywords are quoted even when all-lowercase.
#[test]
fn quote_identifier_quotes_keywords() {
    assert_eq!(quote_identifier("user"), "\"user\"");
    assert_eq!(quote_identifier("order"), "\"order\"");
    assert_eq!(quote_identifier("table"), "\"table\"");
    // Unreserved keywords stay bare, exactly like C.
    assert_eq!(quote_identifier("data"), "data");
    assert_eq!(quote_identifier("plain_db1"), "plain_db1");
}
