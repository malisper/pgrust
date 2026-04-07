//! query_sql_demo — parse a tiny SQL SELECT and run it through the executor.
//!
//! Run with:
//!   cargo run --bin query_sql_demo
//!   cargo run --bin query_sql_demo -- "select name, note from people where id > 1"

use pgrust::access::heap::am::{heap_flush, heap_insert_mvcc};
use pgrust::access::heap::mvcc::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::access::heap::tuple::{AttributeAlign, AttributeDesc, HeapTuple, TupleValue};
use pgrust::executor::{
    ColumnDesc, ExecError, ExecutorContext, RelationDesc, ScalarType, StatementResult, Value,
    execute_sql,
};
use pgrust::parser::{Catalog, CatalogEntry};
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
        rel_number: 16000,
    }
}

fn desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            ColumnDesc {
                name: "id".into(),
                storage: AttributeDesc {
                    name: "id".into(),
                    attlen: 4,
                    attalign: AttributeAlign::Int,
                    nullable: false,
                },
                ty: ScalarType::Int32,
            },
            ColumnDesc {
                name: "name".into(),
                storage: AttributeDesc {
                    name: "name".into(),
                    attlen: -1,
                    attalign: AttributeAlign::Int,
                    nullable: false,
                },
                ty: ScalarType::Text,
            },
            ColumnDesc {
                name: "note".into(),
                storage: AttributeDesc {
                    name: "note".into(),
                    attlen: -1,
                    attalign: AttributeAlign::Int,
                    nullable: true,
                },
                ty: ScalarType::Text,
            },
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
        timed: false,
    };

    match execute_sql(&sql, &mut catalog, &mut ctx, INVALID_TRANSACTION_ID)? {
        StatementResult::Query { column_names, rows } => {
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
