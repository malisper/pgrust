//! wal_syscall_check — workload binary for syscall verification.
//!
//! Runs a fixed set of SQL statements against a fresh database, prints
//! labelled markers before and after each statement, then exits.
//!
//! Designed to be run under a syscall tracer (strace on Linux, dtruss on
//! macOS) so the surrounding shell script can verify:
//!
//!   1. `fdatasync` is called exactly once per committed DML statement
//!      (the WAL flush that makes each commit durable).
//!   2. `fdatasync` is NOT called on heap data files — WAL has taken over
//!      the durability responsibility for those writes.
//!
//! The binary prints the database path to stderr so the tracer output can
//! be cross-referenced with file paths.
//!
//! Run with:  cargo run --bin wal_syscall_check

use pgrust::database::Database;
use pgrust::ClientId;

const CLIENT: ClientId = 1;

fn exec(db: &Database, sql: &str) {
    let label = if sql.len() > 60 { &sql[..60] } else { sql };
    eprintln!("STMT_BEGIN: {label}");
    db.execute(CLIENT, sql).expect("query failed");
    eprintln!("STMT_END: {label}");
}

fn main() {
    let base = std::env::temp_dir().join(format!(
        "pgrust_syscall_check_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);

    eprintln!("DB_PATH: {}", base.display());
    eprintln!("WAL_PATH: {}/pg_wal/wal.log", base.display());

    let db = Database::open(&base, 64).expect("open failed");

    // DDL — no WAL flush expected (catalog only, no buffer pool DML).
    eprintln!("--- DDL (0 fdatasync expected) ---");
    exec(&db, "CREATE TABLE t (id INT, name TEXT)");

    // 5 auto-commit DML statements — each calls finish_txn → flush_wal
    // → fdatasync on pg_wal/wal.log.  Exactly 5 fdatasync calls expected.
    eprintln!("--- DML: 5 statements, 5 fdatasync expected ---");
    exec(&db, "INSERT INTO t VALUES (1, 'alpha')");
    exec(&db, "INSERT INTO t VALUES (2, 'beta')");
    exec(&db, "INSERT INTO t VALUES (3, 'gamma')");
    exec(&db, "UPDATE t SET name = 'ALPHA' WHERE id = 1");
    exec(&db, "DELETE FROM t WHERE id = 2");

    // SELECT — read-only, no WAL flush, no fsync.
    eprintln!("--- SELECT (0 additional fdatasync expected) ---");
    let result = db.execute(CLIENT, "SELECT id, name FROM t ORDER BY id").unwrap();
    match result {
        pgrust::executor::StatementResult::Query(qr) => {
            for row in qr.rows() {
                let cols: Vec<String> = row.iter().map(|v| format!("{v:?}")).collect();
                eprintln!("  row: {}", cols.join(", "));
            }
            eprintln!("  ({} rows)", qr.row_count());
        }
        other => eprintln!("  {other:?}"),
    }

    eprintln!("DONE");
}
