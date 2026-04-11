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
use pgrust::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use pgrust::executor::{
    ExecError, ExecutorContext, RelationDesc, StatementResult, Value, execute_readonly_statement,
};
use pgrust::include::access::htup::{HeapTuple, TupleValue};
use pgrust::parser::{SqlType, SqlTypeKind, Statement, parse_statement};
use pgrust::pl::plpgsql::{RaiseLevel, clear_notices, take_notices};
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
        Value::JsonPath(v) => v.to_string(),
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

    let mut relcache = RelCache::default();
    relcache.insert(
        "people",
        RelCacheEntry {
            rel: rel(),
            relation_oid: 16_384,
            namespace_oid: 11,
            row_type_oid: 16_385,
            relkind: 'r',
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

    let stmt = parse_statement(&sql)?;
    clear_notices();
    let result = match stmt {
        Statement::Do(stmt) => pgrust::pl::plpgsql::execute_do(&stmt)?,
        other => execute_readonly_statement(other, &relcache, &mut ctx)?,
    };
    for notice in take_notices() {
        let level = match notice.level {
            RaiseLevel::Notice => "NOTICE",
            RaiseLevel::Warning => "WARNING",
            RaiseLevel::Exception => "EXCEPTION",
        };
        println!("{level}: {}", notice.message);
    }
    match result {
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
