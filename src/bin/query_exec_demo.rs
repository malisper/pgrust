//! query_exec_demo — hand-built executor demo for SeqScan + Filter + Projection.
//!
//! Run with: cargo run --bin query_exec_demo

use pgrust::access::heap::am::{heap_flush, heap_insert_mvcc};
use pgrust::access::heap::mvcc::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::access::heap::tuple::{HeapTuple, TupleValue};
use pgrust::catalog::column_desc;
use pgrust::executor::{
    ExecError, ExecutorContext, Expr, Plan, RelationDesc, TargetEntry,
    Value, exec_next, executor_start,
};
use pgrust::parser::{SqlType, SqlTypeKind};
use pgrust::storage::smgr::MdStorageManager;
use pgrust::{BufferPool, RelFileLocator, SmgrStorageBackend};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;

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
        Value::Int32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Text(v) => format!("{:?}", v),
        Value::TextRef(_, _) => format!("{:?}", value.as_text().unwrap()),
        Value::Bool(v) => v.to_string(),
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
        input: Box::new(Plan::Filter {
            input: Box::new(Plan::SeqScan {
                rel: rel(),
                desc: desc(),
            }),
            predicate: Expr::Gt(
                Box::new(Expr::Column(0)),
                Box::new(Expr::Const(Value::Int32(1))),
            ),
        }),
        targets: vec![
            TargetEntry {
                name: "name".into(),
                expr: Expr::Column(1),
                sql_type: SqlType::new(SqlTypeKind::Text),
            },
            TargetEntry {
                name: "note".into(),
                expr: Expr::Column(2),
                sql_type: SqlType::new(SqlTypeKind::Text),
            },
        ],
    };

    let mut state = executor_start(plan);
    let mut ctx = ExecutorContext {
        pool: std::sync::Arc::clone(&pool),
        txns: txns.clone(),
        snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap(),
        client_id: 7,
        next_command_id: 0,
        outer_rows: Vec::new(),
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
