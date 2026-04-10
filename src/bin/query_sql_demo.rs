//! query_sql_demo — parse a tiny SQL SELECT and run it through the executor.
//!
//! Run with:
//!   cargo run --bin query_sql_demo
//!   cargo run --bin query_sql_demo -- "select name, note from people where id > 1"

use parking_lot::RwLock;
use pgrust::backend::access::heap::heapam::{heap_flush, heap_insert_mvcc};
use pgrust::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::backend::catalog::catalog::column_desc;
use pgrust::backend::storage::smgr::MdStorageManager;
use pgrust::executor::{
    ExecError, ExecutorContext, RelationDesc, StatementResult, Value, execute_sql,
};
use pgrust::include::access::htup::{HeapTuple, TupleValue};
use pgrust::parser::{Catalog, CatalogEntry, SqlType, SqlTypeKind};
use pgrust::{BufferPool, RelFileLocator, SmgrStorageBackend};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 16000,
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
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => format!("{:?}", v),
        Value::Text(v) => format!("{:?}", v),
        Value::TextRef(_, _) => format!("{:?}", value.as_text().unwrap()),
        Value::Bool(v) => v.to_string(),
        Value::Array(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Null => "NULL".into(),
    }
}

fn main() -> Result<(), ExecError> {
    let sql = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "select name, note from people where id > 1".into());

    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_query_sql_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    println!("=== Setup ===");
    println!("  base directory: {:?}", base_dir);
    println!("  sql: {}", sql);

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

    let mut catalog = Catalog::default();
    catalog.insert(
        "people",
        CatalogEntry {
            rel: rel(),
            desc: desc(),
        },
    );

    let mut ctx = ExecutorContext {
        pool: std::sync::Arc::clone(&pool),
        txns: txns.clone(),
        snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap(),
        client_id: 11,
        next_command_id: 0,
        outer_rows: Vec::new(),
        timed: false,
    };

    match execute_sql(&sql, &mut catalog, &mut ctx, INVALID_TRANSACTION_ID)? {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            println!("=== Output Rows ===");
            for row in rows {
                println!(
                    "  {}",
                    column_names
                        .iter()
                        .zip(row.iter())
                        .map(|(name, value)| format!("{}={}", name, render_value(value)))
                        .collect::<Vec<_>>()
                        .join(" ")
                );
            }
        }
        StatementResult::AffectedRows(count) => {
            println!("=== Statement Result ===");
            println!("  affected_rows={}", count);
        }
    }

    Ok(())
}
