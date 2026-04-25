//! query_exec_demo — hand-built executor demo for SeqScan + Filter + Projection.
//!
//! Run with: cargo run --bin query_exec_demo

use parking_lot::RwLock;
use pgrust::backend::access::heap::heapam::{heap_flush, heap_insert_mvcc};
use pgrust::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::backend::catalog::catalog::column_desc;
use pgrust::backend::storage::smgr::MdStorageManager;
use pgrust::backend::utils::misc::interrupts::InterruptState;
use pgrust::executor::{
    ExecError, ExecutorContext, Expr, Plan, RelationDesc, TargetEntry, Value, exec_next,
    executor_start,
};
use pgrust::include::access::htup::{HeapTuple, TupleValue};
use pgrust::include::nodes::primnodes::{Var, user_attrno};
use pgrust::parser::{SqlType, SqlTypeKind};
use pgrust::{BufferPool, RelFileLocator, SmgrStorageBackend};
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
        Value::PgLsn(v) => format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF),
        Value::Money(v) => pgrust::backend::executor::money_format_text(*v),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            pgrust::backend::executor::render_datetime_value_text(value).unwrap()
        }
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => format!("{v:?}"),
        Value::Uuid(v) => pgrust::backend::executor::render_uuid_text(v),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => format!("{:?}", v),
        Value::JsonPath(v) => v.to_string(),
        Value::Multirange(_) => {
            pgrust::backend::executor::render_multirange_text(value).unwrap_or_default()
        }
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
        Value::Bytea(v) => pgrust::backend::libpq::pqformat::format_bytea_text(
            v,
            pgrust::pgrust::session::ByteaOutputFormat::Hex,
        ),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => pgrust::backend::executor::render_macaddr_text(v),
        Value::MacAddr8(v) => pgrust::backend::executor::render_macaddr8_text(v),
        Value::Text(v) => format!("{:?}", v),
        Value::TextRef(_, _) => format!("{:?}", value.as_text().unwrap()),
        Value::InternalChar(v) => pgrust::backend::executor::render_internal_char_text(*v),
        Value::Bool(v) => v.to_string(),
        Value::Range(_) => pgrust::backend::executor::render_range_text(value).unwrap_or_default(),
        Value::Array(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::PgArray(array) => pgrust::backend::executor::format_array_value_text(array),
        Value::Record(record) => format!("{:?}", record.fields),
        Value::Null => "NULL".into(),
    }
}

fn local_var(index: usize, ty: SqlType) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: ty,
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
        plan_info: pgrust::backend::executor::PlanEstimate::default(),
        input: Box::new(Plan::Filter {
            plan_info: pgrust::backend::executor::PlanEstimate::default(),
            input: Box::new(Plan::SeqScan {
                plan_info: pgrust::backend::executor::PlanEstimate::default(),
                source_id: 1,
                rel: rel(),
                relation_name: "items".into(),
                relation_oid: 0,
                relkind: 'r',
                relispopulated: true,
                toast: None,
                desc: desc(),
            }),
            predicate: Expr::op_auto(
                pgrust::include::nodes::primnodes::OpExprKind::Gt,
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
        pgrust::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
    ));
    let session_stats = std::sync::Arc::new(parking_lot::RwLock::new(
        pgrust::pgrust::database::SessionStatsState::default(),
    ));
    let mut ctx = ExecutorContext {
        pool: std::sync::Arc::clone(&pool),
        txns: txns.clone(),
        txn_waiter: None,
        lock_status_provider: None,
        sequences: None,
        large_objects: None,
        async_notify_runtime: None,
        checkpoint_stats:
            pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
        datetime_config: pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        statement_timestamp_usecs:
            pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts,
        stats,
        session_stats,
        snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap(),
        transaction_state: None,
        client_id: 7,
        session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        next_command_id: 0,
        default_toast_compression: pgrust::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog: None,
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
        advisory_locks: Arc::new(pgrust::backend::storage::lmgr::AdvisoryLockManager::new()),
        row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
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
