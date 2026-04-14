use super::*;
use crate::backend::access::heap::heapam::{heap_scan_begin, heap_scan_next};
use crate::backend::executor::{StatementResult, Value};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pgrust_toast_{}_{}_{}",
        label,
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

fn query_rows(db: &Database, client_id: u32, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(client_id, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {:?}", other),
    }
}

fn session_query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(db, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {:?}", other),
    }
}

fn single_oid(rows: &[Vec<Value>]) -> u32 {
    match rows {
        [row] => match row.first() {
            Some(Value::Int64(oid)) => *oid as u32,
            other => panic!("expected oid row, got {:?}", other),
        },
        other => panic!("expected single row, got {:?}", other),
    }
}

fn count_toast_chunks(
    db: &Database,
    client_id: u32,
    txn_ctx: Option<(TransactionId, CommandId)>,
    toast_oid: u32,
) -> usize {
    let toast_entry = db
        .describe_relation_by_oid(client_id, txn_ctx, toast_oid)
        .unwrap();
    let mut scan = heap_scan_begin(&db.pool, toast_entry.rel).unwrap();
    let mut count = 0;
    while heap_scan_next(&db.pool, client_id, &mut scan)
        .unwrap()
        .is_some()
    {
        count += 1;
    }
    count
}

#[test]
fn toast_externalizes_large_text_values() {
    let base = temp_dir("insert_externalizes");
    let db = Database::open(&base, 64).unwrap();
    let payload = "x".repeat(10_000);

    db.execute(1, "create table docs (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        &format!("insert into docs (id, payload) values (1, '{payload}')"),
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, payload from docs"),
        vec![vec![Value::Int32(1), Value::Text(payload.clone().into())]]
    );

    let toast_oid = single_oid(&query_rows(
        &db,
        1,
        "select reltoastrelid from pg_class where relname = 'docs'",
    ));
    assert_ne!(toast_oid, 0);
    assert!(count_toast_chunks(&db, 1, None, toast_oid) > 0);
}

#[test]
fn temp_tables_create_and_use_temp_toast_namespace() {
    let base = temp_dir("temp_namespace");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);
    let payload = "y".repeat(10_000);

    session
        .execute(&db, "create temp table docs (id int4, payload text)")
        .unwrap();
    session
        .execute(
            &db,
            &format!("insert into docs (id, payload) values (1, '{payload}')"),
        )
        .unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select id, payload from docs"),
        vec![vec![Value::Int32(1), Value::Text(payload.clone().into())]]
    );

    let toast_oid = single_oid(&session_query_rows(
        &mut session,
        &db,
        "select reltoastrelid from pg_class where relname = 'docs'",
    ));
    assert_ne!(toast_oid, 0);
    assert_eq!(
        db.relation_namespace_name(1, None, toast_oid).as_deref(),
        Some("pg_toast_temp_1")
    );
    assert!(count_toast_chunks(&db, 1, None, toast_oid) > 0);
}

#[test]
fn create_table_as_externalizes_large_rows() {
    let base = temp_dir("ctas_externalizes");
    let db = Database::open(&base, 64).unwrap();
    let payload = "z".repeat(10_000);

    db.execute(1, "create table source_docs (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        &format!("insert into source_docs (id, payload) values (1, '{payload}')"),
    )
    .unwrap();
    db.execute(
        1,
        "create table copied_docs as select id, payload from source_docs",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, payload from copied_docs"),
        vec![vec![Value::Int32(1), Value::Text(payload.clone().into())]]
    );

    let toast_oid = single_oid(&query_rows(
        &db,
        1,
        "select reltoastrelid from pg_class where relname = 'copied_docs'",
    ));
    assert_ne!(toast_oid, 0);
    assert!(count_toast_chunks(&db, 1, None, toast_oid) > 0);
}

#[test]
fn create_table_as_toasted_relation_is_visible_before_commit() {
    let base = temp_dir("ctas_visibility");
    let db = Database::open(&base, 64).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);
    let payload = "q".repeat(10_000);

    writer
        .execute(&db, "create table source_docs (id int4, payload text)")
        .unwrap();
    writer
        .execute(
            &db,
            &format!("insert into source_docs (id, payload) values (1, '{payload}')"),
        )
        .unwrap();

    writer.execute(&db, "begin").unwrap();
    writer
        .execute(
            &db,
            "create table copied_docs as select id, payload from source_docs",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(&mut writer, &db, "select id, payload from copied_docs"),
        vec![vec![Value::Int32(1), Value::Text(payload.clone().into())]]
    );

    let toast_oid = single_oid(&session_query_rows(
        &mut writer,
        &db,
        "select reltoastrelid from pg_class where relname = 'copied_docs'",
    ));
    assert_ne!(toast_oid, 0);
    assert!(count_toast_chunks(&db, 1, writer.catalog_txn_ctx(), toast_oid) > 0);

    assert!(
        reader
            .execute(&db, "select count(*) from copied_docs")
            .is_err(),
        "other sessions must not see uncommitted CTAS catalog rows"
    );

    writer.execute(&db, "commit").unwrap();

    assert_eq!(
        session_query_rows(&mut reader, &db, "select count(*) from copied_docs"),
        vec![vec![Value::Int64(1)]]
    );
}
