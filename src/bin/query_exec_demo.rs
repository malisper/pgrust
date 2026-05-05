//! query_exec_demo — hand-built executor demo for SeqScan + Filter + Projection.
//!
//! Run with: cargo run --bin query_exec_demo

use parking_lot::RwLock;
use pgrust::catalog::column_desc;
use pgrust::sql::{SqlType, SqlTypeKind};
use pgrust::{BufferPool, RelFileLocator, SmgrStorageBackend};
use pgrust::{
    ExecError, ExecutorContext, Expr, Plan, RelationDesc, TargetEntry, Value, exec_next,
    executor_start,
};
use pgrust_access::access::htup::{HeapTuple, TupleValue};
use pgrust_access::heap::heapam::{heap_flush, heap_insert_mvcc};
use pgrust_access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust_core::InterruptState;
use pgrust_nodes::primnodes::{Var, user_attrno};
use pgrust_storage::smgr::MdStorageManager;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 15000,
    }
}

fn desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("name", SqlType::new(SqlTypeKind::Text), false),
            column_desc("note", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
}

fn tuple(id: i32, name: &str, note: Option<&str>) -> HeapTuple {
    let attrs = desc()
        .columns
        .iter()
        .map(|c| c.storage.clone())
        .collect::<Vec<_>>();
    HeapTuple::from_values(
        &attrs,
        &[
            TupleValue::Bytes(id.to_le_bytes().to_vec()),
            TupleValue::Bytes(name.as_bytes().to_vec()),
            match note {
                Some(note) => TupleValue::Bytes(note.as_bytes().to_vec()),
                None => TupleValue::Null,
            },
        ],
    )
    .unwrap()
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF),
        Value::Money(v) => pgrust_expr::money_format_text(*v),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => pgrust_expr::render_datetime_value_text(value).unwrap(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => format!("{v:?}"),
        Value::Uuid(v) => pgrust_expr::render_uuid_text(v),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => format!("{:?}", v),
        Value::JsonPath(v) => v.to_string(),
        Value::Multirange(_) => pgrust_expr::render_multirange_text(value).unwrap_or_default(),
        Value::Xml(v) => v.to_string(),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => format!("{value:?}"),
        Value::TsVector(v) => v.render(),
        Value::TsQuery(v) => v.render(),
        Value::Bit(v) => v.render(),
        Value::Bytea(v) => {
            pgrust_expr::libpq::pqformat::format_bytea_text(v, pgrust::ByteaOutputFormat::Hex)
        }
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => pgrust_expr::render_macaddr_text(v),
        Value::MacAddr8(v) => pgrust_expr::render_macaddr8_text(v),
        Value::Tid(v) => format!("({},{})", v.block_number, v.offset_number),
        Value::Text(v) => format!("{:?}", v),
        Value::TextRef(_, _) => format!("{:?}", value.as_text().unwrap()),
        Value::InternalChar(v) => pgrust_expr::render_internal_char_text(*v),
        Value::EnumOid(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Range(_) => pgrust_expr::render_range_text(value).unwrap_or_default(),
        Value::Array(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::PgArray(array) => pgrust_expr::format_array_value_text(array),
        Value::Record(record) => format!("{:?}", record.fields),
        Value::Null => "NULL".into(),
        _ => format!("{value:?}"),
    }
}

fn local_var(index: usize, ty: SqlType) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: ty,
        collation_oid: None,
    })
}

fn main() -> Result<(), ExecError> {
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_query_exec_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    println!("=== Setup ===");
    println!("  base directory: {:?}", base_dir);

    let txns = Arc::new(RwLock::new(
        TransactionManager::new_durable(&base_dir).unwrap(),
    ));
    let smgr = MdStorageManager::new(&base_dir);
    let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));

    let xid = txns.write().begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    txns.write().commit(xid).unwrap();

    println!("=== Plan ===");
    println!("  PROJECT name, note");
    println!("  FILTER id > 1");
    println!("  SEQSCAN rel {}", rel().rel_number);

    let plan = Plan::Projection {
        plan_info: pgrust_nodes::plannodes::PlanEstimate::default(),
        input: Box::new(Plan::Filter {
            plan_info: pgrust_nodes::plannodes::PlanEstimate::default(),
            input: Box::new(Plan::SeqScan {
                plan_info: pgrust_nodes::plannodes::PlanEstimate::default(),
                source_id: 1,
                parallel_scan_id: None,
                rel: rel(),
                relation_name: "items".into(),
                relation_oid: 0,
                relkind: 'r',
                relispopulated: true,
                disabled: false,
                toast: None,
                tablesample: None,
                desc: desc(),
                parallel_aware: false,
            }),
            predicate: Expr::op_auto(
                pgrust_nodes::primnodes::OpExprKind::Gt,
                vec![
                    local_var(0, SqlType::new(SqlTypeKind::Int4)),
                    Expr::Const(Value::Int32(1)),
                ],
            ),
        }),
        targets: vec![
            TargetEntry::new(
                "name",
                local_var(1, SqlType::new(SqlTypeKind::Text)),
                SqlType::new(SqlTypeKind::Text),
                1,
            ),
            TargetEntry::new(
                "note",
                local_var(2, SqlType::new(SqlTypeKind::Text)),
                SqlType::new(SqlTypeKind::Text),
                2,
            ),
        ],
    };

    let mut state = executor_start(plan);
    let interrupts = Arc::new(InterruptState::new());
    let stats = std::sync::Arc::new(parking_lot::RwLock::new(
        pgrust::DatabaseStatsStore::with_default_io_rows(),
    ));
    let session_stats = std::sync::Arc::new(parking_lot::RwLock::new(
        pgrust::SessionStatsState::default(),
    ));
    let mut ctx = ExecutorContext {
        pool: std::sync::Arc::clone(&pool),
        data_dir: None,
        txns: txns.clone(),
        txn_waiter: None,
        lock_status_provider: None,
        sequences: None,
        large_objects: None,
        stats_import_runtime: None,
        async_notify_runtime: None,
        checkpoint_stats: pgrust_access::transam::CheckpointStatsSnapshot::default(),
        datetime_config: pgrust_expr::DateTimeConfig::default(),
        statement_timestamp_usecs:
            pgrust_expr::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts,
        stats,
        session_stats,
        snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap(),
        write_xid_override: None,
        transaction_state: None,
        client_id: 7,
        session_user_oid: pgrust_catalog_data::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: pgrust_catalog_data::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        next_command_id: 0,
        default_toast_compression: pgrust::AttributeCompression::Pglz,
        random_state: pgrust_expr::PgPrngState::shared(),
        expr_bindings: pgrust_executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        active_grouping_refs: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        security_restricted: false,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: None,
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        pending_portals: Vec::new(),
        catalog: None,
        scalar_function_cache: std::collections::HashMap::new(),
        proc_execute_acl_cache: std::collections::HashSet::new(),
        srf_rows_cache: std::collections::HashMap::new(),
        plpgsql_function_cache: std::sync::Arc::new(parking_lot::RwLock::new(
            pgrust::plpgsql::PlpgsqlFunctionCache::default(),
        )),
        pinned_cte_tables: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
        advisory_locks: Arc::new(pgrust_storage::lmgr::AdvisoryLockManager::new()),
        row_locks: Arc::new(pgrust_storage::lmgr::RowLockManager::new()),
        current_database_name: String::new(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
    };

    let names = state.column_names().to_vec();
    println!("=== Output Rows ===");
    while let Some(slot) = exec_next(&mut state, &mut ctx)? {
        let values: Vec<_> = slot.values()?.iter().map(|v| v.to_owned_value()).collect();
        let rendered = names
            .iter()
            .zip(values.iter())
            .map(|(name, value)| format!("{}={}", name, render_value(value)))
            .collect::<Vec<_>>()
            .join(" ");
        println!("  {}", rendered);
    }

    Ok(())
}
