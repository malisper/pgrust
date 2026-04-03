use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use pgrust::access::heap::am::{heap_flush, heap_insert_mvcc};
use pgrust::access::heap::mvcc::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::access::heap::tuple::{AttributeAlign, AttributeDesc, HeapTuple, TupleValue};
use pgrust::executor::{
    ColumnDesc, ExecError, ExecutorContext, RelationDesc, ScalarType, StatementResult, Value,
    execute_statement,
};
use pgrust::parser::{Catalog, CatalogEntry, Statement, parse_statement};
use pgrust::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use pgrust::{BufferPool, RelFileLocator, SmgrStorageBackend};

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

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "people",
        CatalogEntry {
            rel: rel(),
            desc: desc(),
        },
    );
    catalog
}

fn default_base_dir() -> PathBuf {
    std::env::temp_dir().join("pgrust_query_repl")
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Int32(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Bool(v) => v.to_string(),
        Value::Null => "NULL".into(),
    }
}

fn print_result(result: StatementResult) {
    match result {
        StatementResult::AffectedRows(count) => {
            println!("AFFECTED ROWS: {}", count);
        }
        StatementResult::Query { column_names, rows } => {
            if column_names.is_empty() {
                println!("EMPTY RESULT");
                return;
            }

            let mut widths = column_names.iter().map(|name| name.len()).collect::<Vec<_>>();
            let rendered_rows = rows
                .into_iter()
                .map(|row| row.into_iter().map(|value| render_value(&value)).collect::<Vec<_>>())
                .collect::<Vec<_>>();

            for row in &rendered_rows {
                for (idx, value) in row.iter().enumerate() {
                    widths[idx] = widths[idx].max(value.len());
                }
            }

            let header = column_names
                .iter()
                .enumerate()
                .map(|(idx, name)| format!("{name:<width$}", width = widths[idx]))
                .collect::<Vec<_>>()
                .join(" | ");
            let separator = widths
                .iter()
                .map(|width| "-".repeat(*width))
                .collect::<Vec<_>>()
                .join("-+-");

            println!("{}", header);
            println!("{}", separator);
            for row in rendered_rows {
                println!(
                    "{}",
                    row.iter()
                        .enumerate()
                        .map(|(idx, value)| format!("{value:<width$}", width = widths[idx]))
                        .collect::<Vec<_>>()
                        .join(" | ")
                );
            }
        }
    }
}

fn seed_if_empty(
    pool: &mut BufferPool<SmgrStorageBackend>,
    txns: &mut TransactionManager,
) -> Result<(), ExecError> {
    if pool.storage_mut().smgr.exists(rel(), ForkNumber::Main) {
        return Ok(());
    }

    let xid = txns.begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(pool, 1, rel(), xid, &row)?;
        heap_flush(pool, 1, rel(), tid.block_number)?;
    }
    txns.commit(xid)?;
    Ok(())
}

fn run_statement(
    sql: &str,
    pool: &mut BufferPool<SmgrStorageBackend>,
    txns: &mut TransactionManager,
    catalog: &Catalog,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    match stmt {
        Statement::Select(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns,
                snapshot: txns.snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(Statement::Select(stmt), catalog, &mut ctx, INVALID_TRANSACTION_ID)
        }
        Statement::Insert(stmt) => {
            let xid = txns.begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns,
                    snapshot: txns.snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(Statement::Insert(stmt), catalog, &mut ctx, xid)
            };
            match result {
                Ok(result) => {
                    txns.commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.abort(xid)?;
                    Err(err)
                }
            }
        }
        Statement::Update(stmt) => {
            let xid = txns.begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns,
                    snapshot: txns.snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(Statement::Update(stmt), catalog, &mut ctx, xid)
            };
            match result {
                Ok(result) => {
                    txns.commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.abort(xid)?;
                    Err(err)
                }
            }
        }
        Statement::Delete(stmt) => {
            let xid = txns.begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns,
                    snapshot: txns.snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(Statement::Delete(stmt), catalog, &mut ctx, xid)
            };
            match result {
                Ok(result) => {
                    txns.commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.abort(xid)?;
                    Err(err)
                }
            }
        }
    }
}

fn main() -> Result<(), String> {
    let base_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(default_base_dir);
    fs::create_dir_all(&base_dir).map_err(|e| e.to_string())?;

    let mut txns = TransactionManager::new_durable(&base_dir).map_err(|e| format!("{e:?}"))?;
    let smgr = MdStorageManager::new(&base_dir);
    let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
    seed_if_empty(&mut pool, &mut txns).map_err(|e| format!("{e:?}"))?;
    let catalog = catalog();

    println!("PGRUST SQL REPL");
    println!("BASE DIRECTORY: {}", base_dir.display());
    println!("TABLE: people(id int4, name text, note text null)");
    println!("COMMANDS: .help, .exit");

    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("pgrust> ");
        io::stdout().flush().map_err(|e| e.to_string())?;

        line.clear();
        if stdin.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            println!();
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case(".exit") || input.eq_ignore_ascii_case(".quit") {
            break;
        }
        if input.eq_ignore_ascii_case(".help") {
            println!("ENTER A SINGLE SQL STATEMENT PER LINE.");
            println!("SUPPORTED: SELECT, INSERT, UPDATE, DELETE");
            println!("EXAMPLE: select id, name from people where id > 1;");
            continue;
        }

        let sql = input.trim_end_matches(';').trim();
        match run_statement(sql, &mut pool, &mut txns, &catalog) {
            Ok(result) => print_result(result),
            Err(err) => eprintln!("ERROR: {:?}", err),
        }
    }

    Ok(())
}
