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
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            pgrust::backend::executor::render_datetime_value_text(value).unwrap()
        }
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => format!("{:?}", v),
        Value::JsonPath(v) => v.to_string(),
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
        Value::Text(v) => format!("{:?}", v),
        Value::TextRef(_, _) => format!("{:?}", value.as_text().unwrap()),
        Value::InternalChar(v) => pgrust::backend::executor::render_internal_char_text(*v),
        Value::Bool(v) => v.to_string(),
        Value::Array(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::PgArray(array) => pgrust::backend::executor::format_array_value_text(array),
        Value::Null => "NULL".into(),
    }
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
                rel: rel(),
                relation_oid: 0,
                toast: None,
                desc: desc(),
            }),
            predicate: Expr::op_auto(
                pgrust::include::nodes::primnodes::OpExprKind::Gt,
                vec![Expr::Column(0), Expr::Const(Value::Int32(1))],
            ),
        }),
        targets: vec![
            TargetEntry::new("name", Expr::Column(1), SqlType::new(SqlTypeKind::Text), 1),
            TargetEntry::new("note", Expr::Column(2), SqlType::new(SqlTypeKind::Text), 2),
        ],
    };

    let mut state = executor_start(plan);
    let interrupts = Arc::new(InterruptState::new());
    let mut ctx = ExecutorContext {
        pool: std::sync::Arc::clone(&pool),
        txns: txns.clone(),
        txn_waiter: None,
        interrupts,
        snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap(),
        client_id: 7,
        next_command_id: 0,
        outer_rows: Vec::new(),
        subplans: Vec::new(),
        timed: false,
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
