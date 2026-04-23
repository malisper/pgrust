use super::*;
use crate::backend::access::common::toast_compression::compress_inline_datum;
use crate::backend::access::heap::heapam::{
    heap_insert_mvcc_with_cid, heap_scan_begin, heap_scan_begin_visible, heap_scan_next,
    heap_scan_next_visible,
};
use crate::backend::access::heap::heaptoast::{
    ExternalToastValueInput, encoded_pointer_bytes, store_external_value,
};
use crate::backend::access::transam::xact::{CommandId, INVALID_TRANSACTION_ID, TransactionId};
use crate::backend::executor::{ExecutorContext, StatementResult, Value};
use crate::include::access::detoast::{
    decode_ondisk_toast_pointer, is_compressed_inline_datum,
    varatt_external_get_compression_method, varatt_external_is_compressed,
};
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::access::toast_compression::ToastCompressionId;
use crate::include::nodes::primnodes::ToastRelationRef;
use crate::include::varatt::VARHDRSZ;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

fn temp_dir(label: &str) -> PathBuf {
    let _ = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    crate::pgrust::test_support::seeded_temp_dir("toast", label)
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

fn relation_payload_bytes(db: &Database, client_id: u32, table_name: &str, id: i32) -> Vec<u8> {
    let catalog = db.lazy_catalog_lookup(client_id, None, None);
    let relation = catalog
        .lookup_relation(table_name)
        .expect("relation exists");
    let attr_descs = relation.desc.attribute_descs();
    let snapshot = db.txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap();
    let mut scan = heap_scan_begin_visible(&db.pool, client_id, relation.rel, snapshot).unwrap();
    let txns = db.txns.read();
    while let Some((_tid, tuple)) =
        heap_scan_next_visible(&db.pool, client_id, &txns, &mut scan).unwrap()
    {
        let values = tuple.deform(&attr_descs).unwrap();
        let row_id = i32::from_le_bytes(values[0].unwrap().try_into().unwrap());
        if row_id == id {
            return crate::include::access::htup::deform_raw(&tuple.serialize(), &attr_descs)
                .unwrap()[1]
                .unwrap()
                .to_vec();
        }
    }
    panic!("row {id} not found in {table_name}");
}

fn toast_relation_ref(db: &Database, client_id: u32, table_name: &str) -> ToastRelationRef {
    let toast_oid = single_oid(&query_rows(
        db,
        client_id,
        &format!("select reltoastrelid from pg_class where relname = '{table_name}'"),
    ));
    let entry = db
        .describe_relation_by_oid(client_id, None, toast_oid)
        .expect("toast relation exists");
    ToastRelationRef {
        rel: entry.rel,
        relation_oid: toast_oid,
    }
}

fn toast_executor_context(
    db: &Database,
    client_id: u32,
    xid: TransactionId,
    cid: CommandId,
) -> ExecutorContext {
    ExecutorContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: std::sync::Arc::clone(&db.advisory_locks),
        row_locks: std::sync::Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts: db.interrupt_state(client_id),
        stats: db.stats.clone(),
        session_stats: db.session_stats_state(client_id),
        snapshot: db.txns.read().snapshot_for_command(xid, cid).unwrap(),
        transaction_state: None,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: db.auth_state(client_id).session_user_oid(),
        current_user_oid: db.auth_state(client_id).current_user_oid(),
        active_role_oid: db.auth_state(client_id).active_role_oid(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: cid,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog: None,
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
    }
}

fn compressible_payload(len: usize) -> String {
    "toast-compressible-"
        .repeat((len + 18) / 19)
        .chars()
        .take(len)
        .collect()
}

fn externalizable_compressible_payload() -> String {
    for repeats in (2_000..20_000).step_by(500) {
        let mut payload = String::new();
        for i in 0..repeats {
            use std::fmt::Write;
            write!(
                &mut payload,
                "{:04x}:abcdefghijabcdefghijabcdefghij|",
                i % 1024
            )
            .unwrap();
        }
        if let Some(compressed) = compress_inline_datum(
            payload.as_bytes(),
            crate::include::access::htup::AttributeCompression::Pglz,
            crate::include::access::htup::AttributeCompression::Pglz,
        )
        .unwrap()
            && compressed.encoded.len()
                > crate::backend::storage::page::bufpage::MAX_HEAP_TUPLE_SIZE
        {
            return payload;
        }
    }
    panic!("failed to find externally toasted compressible payload");
}

#[test]
fn toast_externalizes_large_text_values() {
    let base = temp_dir("insert_externalizes");
    let db = Database::open(&base, 64).unwrap();
    let payload = externalizable_compressible_payload();

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
    let payload = externalizable_compressible_payload();

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
    let payload = externalizable_compressible_payload();

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
    let payload = externalizable_compressible_payload();

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

#[test]
fn alter_column_compression_can_keep_large_values_inline() {
    let base = temp_dir("inline_compressed");
    let db = Database::open(&base, 64).unwrap();
    let payload = compressible_payload(10_000);

    db.execute(1, "create table docs (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        "alter table docs alter column payload set compression pglz",
    )
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

    let raw = relation_payload_bytes(&db, 1, "docs", 1);
    assert!(is_compressed_inline_datum(&raw));

    let toast_oid = single_oid(&query_rows(
        &db,
        1,
        "select reltoastrelid from pg_class where relname = 'docs'",
    ));
    assert_eq!(count_toast_chunks(&db, 1, None, toast_oid), 0);
}

#[test]
fn storage_external_disables_compression_even_when_requested() {
    let base = temp_dir("external_without_compression");
    let db = Database::open(&base, 64).unwrap();
    let payload = compressible_payload(10_000);

    db.execute(1, "create table docs (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        "alter table docs alter column payload set storage external",
    )
    .unwrap();
    db.execute(
        1,
        "alter table docs alter column payload set compression pglz",
    )
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

    let raw = relation_payload_bytes(&db, 1, "docs", 1);
    let pointer = decode_ondisk_toast_pointer(&raw).expect("external toast pointer");
    assert!(!varatt_external_is_compressed(pointer));

    let toast_oid = single_oid(&query_rows(
        &db,
        1,
        "select reltoastrelid from pg_class where relname = 'docs'",
    ));
    assert!(count_toast_chunks(&db, 1, None, toast_oid) > 0);
}

#[test]
fn compressed_external_values_round_trip() {
    let base = temp_dir("compressed_external_roundtrip");
    let db = Database::open(&base, 64).unwrap();
    let payload = externalizable_compressible_payload();

    db.execute(1, "create table docs (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        "alter table docs alter column payload set compression pglz",
    )
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

    let raw = relation_payload_bytes(&db, 1, "docs", 1);
    let pointer = decode_ondisk_toast_pointer(&raw).expect("external toast pointer");
    assert_eq!(
        varatt_external_get_compression_method(pointer),
        ToastCompressionId::Pglz as u32
    );

    let toast_oid = single_oid(&query_rows(
        &db,
        1,
        "select reltoastrelid from pg_class where relname = 'docs'",
    ));
    assert!(count_toast_chunks(&db, 1, None, toast_oid) > 0);
}

#[test]
fn legacy_and_exact_external_pointer_encodings_both_decode() {
    let base = temp_dir("legacy_external_compat");
    let db = Database::open(&base, 64).unwrap();
    let payload = compressible_payload(8_000);

    db.execute(1, "create table docs (id int4, payload text)")
        .unwrap();

    let catalog = db.lazy_catalog_lookup(1, None, None);
    let relation = catalog
        .lookup_relation("docs")
        .expect("heap relation exists");
    let attr_descs = relation.desc.attribute_descs();
    let toast = toast_relation_ref(&db, 1, "docs");

    let xid = db.txns.write().begin();
    let cid = 0;
    let mut ctx = toast_executor_context(&db, 1, xid, cid);
    let stored = store_external_value(
        &mut ctx,
        toast,
        None,
        &ExternalToastValueInput {
            data: payload.as_bytes().to_vec(),
            rawsize: i32::try_from(payload.len() + VARHDRSZ).unwrap(),
            compression_id: ToastCompressionId::Invalid,
        },
        xid,
        cid,
    )
    .unwrap();

    let legacy_tuple = HeapTuple::from_values(
        &attr_descs,
        &[
            TupleValue::Bytes(1i32.to_le_bytes().to_vec()),
            TupleValue::Bytes(encoded_pointer_bytes(stored.pointer)),
        ],
    )
    .unwrap();
    let exact_tuple = HeapTuple::from_values(
        &attr_descs,
        &[
            TupleValue::Bytes(2i32.to_le_bytes().to_vec()),
            TupleValue::EncodedVarlena(encoded_pointer_bytes(stored.pointer)),
        ],
    )
    .unwrap();

    heap_insert_mvcc_with_cid(&db.pool, 1, relation.rel, xid, cid, &legacy_tuple).unwrap();
    heap_insert_mvcc_with_cid(&db.pool, 1, relation.rel, xid, cid, &exact_tuple).unwrap();
    db.txns.write().commit(xid).unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, payload from docs order by id"),
        vec![
            vec![Value::Int32(1), Value::Text(payload.clone().into())],
            vec![Value::Int32(2), Value::Text(payload.into())],
        ]
    );
}
