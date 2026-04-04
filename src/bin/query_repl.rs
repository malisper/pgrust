use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;

use pgrust::access::heap::am::{heap_flush, heap_insert_mvcc};
use pgrust::access::heap::mvcc::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::access::heap::tuple::{AttributeAlign, AttributeDesc, HeapTuple, TupleValue};
use pgrust::catalog::{Catalog, DurableCatalog};
use pgrust::executor::{
    ColumnDesc, ExecError, ExecutorContext, RelationDesc, ScalarType, StatementResult, Value,
    execute_statement,
};
use pgrust::parser::{ParseError, Statement, parse_statement};
use pgrust::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use pgrust::{BufferPool, SmgrStorageBackend};

struct RawModeGuard {
    fd: i32,
    original: libc::termios,
}

impl RawModeGuard {
    fn new(fd: i32) -> Result<Self, String> {
        let mut original = MaybeUninit::<libc::termios>::uninit();
        let rc = unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error().to_string());
        }

        let original = unsafe { original.assume_init() };
        let mut raw = original;
        raw.c_iflag &= !(libc::BRKINT | libc::ICRNL | libc::INPCK | libc::ISTRIP | libc::IXON);
        raw.c_oflag &= !(libc::OPOST);
        raw.c_cflag |= libc::CS8;
        raw.c_lflag &= !(libc::ECHO | libc::ICANON | libc::IEXTEN);
        raw.c_cc[libc::VMIN] = 1;
        raw.c_cc[libc::VTIME] = 0;

        let rc = unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, &raw) };
        if rc != 0 {
            return Err(io::Error::last_os_error().to_string());
        }

        Ok(Self { fd, original })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::tcsetattr(self.fd, libc::TCSAFLUSH, &self.original) };
    }
}

#[derive(Default)]
struct ReplHistory {
    entries: Vec<String>,
    cursor: Option<usize>,
}

fn history_path(base_dir: &std::path::Path) -> PathBuf {
    base_dir.join("repl_history")
}

fn load_history(path: &std::path::Path) -> Result<ReplHistory, String> {
    if !path.exists() {
        return Ok(ReplHistory::default());
    }

    let contents = fs::read_to_string(path).map_err(|e| e.to_string())?;
    Ok(ReplHistory {
        entries: contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        cursor: None,
    })
}

fn append_history(path: &std::path::Path, line: &str) -> Result<(), String> {
    if line.trim().is_empty() {
        return Ok(());
    }

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "{line}").map_err(|e| e.to_string())
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

fn default_base_dir() -> PathBuf {
    std::env::temp_dir().join("pgrust_query_repl")
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Int32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Text(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Null => "NULL".into(),
    }
}

fn print_result(result: StatementResult) {
    match result {
        StatementResult::AffectedRows(count) => {
            println!("AFFECTED ROWS: {}", count);
        }
        StatementResult::Query(qr) => {
            if qr.column_names().is_empty() {
                println!("({} rows)", qr.row_count());
                return;
            }

            let mut widths = qr.column_names().iter().map(|name| name.len()).collect::<Vec<_>>();
            let rendered_rows = qr.rows()
                .map(|row| row.iter().map(|value| render_value(value)).collect::<Vec<_>>())
                .collect::<Vec<_>>();

            for row in &rendered_rows {
                for (idx, value) in row.iter().enumerate() {
                    widths[idx] = widths[idx].max(value.len());
                }
            }

            let header = qr.column_names()
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

fn render_exec_error(err: &ExecError) -> String {
    match err {
        ExecError::Parse(parse) => parse.to_string(),
        other => format!("{other:?}"),
    }
}

fn redraw_line(prompt: &str, buffer: &str, cursor: usize) -> Result<(), String> {
    let mut stdout = io::stdout().lock();
    write!(stdout, "\r{prompt}{buffer}\x1b[K").map_err(|e| e.to_string())?;
    let tail_len = buffer.len().saturating_sub(cursor);
    if tail_len > 0 {
        write!(stdout, "\x1b[{}D", tail_len).map_err(|e| e.to_string())?;
    }
    stdout.flush().map_err(|e| e.to_string())
}

fn read_repl_line(prompt: &str, history: &mut ReplHistory) -> Result<Option<String>, String> {
    let stdin = io::stdin();
    if !stdin.is_terminal() {
        print!("{prompt}");
        io::stdout().flush().map_err(|e| e.to_string())?;
        let mut line = String::new();
        if stdin.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            return Ok(None);
        }
        return Ok(Some(line));
    }

    let _raw_mode = RawModeGuard::new(stdin.as_raw_fd())?;
    let mut input = stdin.lock();
    let mut buffer = String::new();
    let mut cursor = 0usize;
    history.cursor = None;

    print!("{prompt}");
    io::stdout().flush().map_err(|e| e.to_string())?;

    loop {
        let mut byte = [0u8; 1];
        input.read_exact(&mut byte).map_err(|e| e.to_string())?;
        match byte[0] {
            b'\n' | b'\r' => {
                {
                    let mut stdout = io::stdout().lock();
                    write!(stdout, "\r\n").map_err(|e| e.to_string())?;
                    stdout.flush().map_err(|e| e.to_string())?;
                }
                if !buffer.is_empty() {
                    history.entries.push(buffer.clone());
                }
                history.cursor = None;
                return Ok(Some(buffer));
            }
            4 => {
                if buffer.is_empty() {
                    let mut stdout = io::stdout().lock();
                    write!(stdout, "\r\n").map_err(|e| e.to_string())?;
                    stdout.flush().map_err(|e| e.to_string())?;
                    return Ok(None);
                }
            }
            1 => {
                cursor = 0;
                redraw_line(prompt, &buffer, cursor)?;
            }
            5 => {
                cursor = buffer.len();
                redraw_line(prompt, &buffer, cursor)?;
            }
            8 | 127 => {
                if cursor > 0 {
                    cursor -= 1;
                    buffer.remove(cursor);
                    redraw_line(prompt, &buffer, cursor)?;
                }
            }
            27 => {
                let mut seq = [0u8; 2];
                if input.read_exact(&mut seq).is_err() {
                    continue;
                }
                if seq[0] != b'[' {
                    continue;
                }
                match seq[1] {
                    b'A' => {
                        if history.entries.is_empty() {
                            continue;
                        }
                        history.cursor = Some(match history.cursor {
                            Some(0) => 0,
                            Some(idx) => idx - 1,
                            None => history.entries.len() - 1,
                        });
                        buffer = history.entries[history.cursor.unwrap()].clone();
                        cursor = buffer.len();
                        redraw_line(prompt, &buffer, cursor)?;
                    }
                    b'B' => {
                        if history.entries.is_empty() {
                            continue;
                        }
                        match history.cursor {
                            Some(idx) if idx + 1 < history.entries.len() => {
                                history.cursor = Some(idx + 1);
                                buffer = history.entries[idx + 1].clone();
                            }
                            Some(_) => {
                                history.cursor = None;
                                buffer.clear();
                            }
                            None => continue,
                        }
                        cursor = buffer.len();
                        redraw_line(prompt, &buffer, cursor)?;
                    }
                    b'C' => {
                        if cursor < buffer.len() {
                            cursor += 1;
                            redraw_line(prompt, &buffer, cursor)?;
                        }
                    }
                    b'D' => {
                        if cursor > 0 {
                            cursor -= 1;
                            redraw_line(prompt, &buffer, cursor)?;
                        }
                    }
                    _ => {}
                }
            }
            byte if byte.is_ascii_graphic() || byte == b' ' => {
                buffer.insert(cursor, byte as char);
                cursor += 1;
                redraw_line(prompt, &buffer, cursor)?;
            }
            _ => {}
        }
    }
}

fn ensure_default_people_table(catalog_store: &mut DurableCatalog) -> Result<(), String> {
    if catalog_store.catalog().get("people").is_some() {
        return Ok(());
    }
    catalog_store
        .catalog_mut()
        .create_table("people", desc())
        .map_err(|e| format!("{e:?}"))?;
    catalog_store.persist().map_err(|e| format!("{e:?}"))
}

fn seed_if_empty(
    pool: &BufferPool<SmgrStorageBackend>,
    catalog: &Catalog,
    txns: &Arc<RwLock<TransactionManager>>,
) -> Result<(), ExecError> {
    let rel = catalog
        .get("people")
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable("people".into())))?
        .rel;

    if pool.with_storage_mut(|s| s.smgr.exists(rel, ForkNumber::Main)) {
        return Ok(());
    }

    let xid = txns.write().begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(pool, 1, rel, xid, &row)?;
        heap_flush(pool, 1, rel, tid.block_number)?;
    }
    txns.write().commit(xid)?;
    Ok(())
}

fn run_statement(
    sql: &str,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &Arc<RwLock<TransactionManager>>,
    catalog_store: &mut DurableCatalog,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    let needs_catalog_persist =
        matches!(stmt, Statement::CreateTable(_) | Statement::DropTable(_));

    let result = match stmt {
        Statement::Explain(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::Explain(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::Select(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::Select(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::ShowTables => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::ShowTables,
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::CreateTable(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::CreateTable(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::DropTable(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::DropTable(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::TruncateTable(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::TruncateTable(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::Vacuum(stmt) => {
            let mut ctx = ExecutorContext {
                pool,
                txns: txns.clone(),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                client_id: 21,
                next_command_id: 0,
            };
            execute_statement(
                Statement::Vacuum(stmt),
                catalog_store.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )
        }
        Statement::Insert(stmt) => {
            let xid = txns.write().begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns: txns.clone(),
                    snapshot: txns.read().snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(
                    Statement::Insert(stmt),
                    catalog_store.catalog_mut(),
                    &mut ctx,
                    xid,
                )
            };
            match result {
                Ok(result) => {
                    txns.write().commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.write().abort(xid)?;
                    Err(err)
                }
            }
        }
        Statement::Update(stmt) => {
            let xid = txns.write().begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns: txns.clone(),
                    snapshot: txns.read().snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(
                    Statement::Update(stmt),
                    catalog_store.catalog_mut(),
                    &mut ctx,
                    xid,
                )
            };
            match result {
                Ok(result) => {
                    txns.write().commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.write().abort(xid)?;
                    Err(err)
                }
            }
        }
        Statement::Delete(stmt) => {
            let xid = txns.write().begin();
            let result = {
                let mut ctx = ExecutorContext {
                    pool,
                    txns: txns.clone(),
                    snapshot: txns.read().snapshot(xid)?,
                    client_id: 21,
                    next_command_id: 0,
                };
                execute_statement(
                    Statement::Delete(stmt),
                    catalog_store.catalog_mut(),
                    &mut ctx,
                    xid,
                )
            };
            match result {
                Ok(result) => {
                    txns.write().commit(xid)?;
                    Ok(result)
                }
                Err(err) => {
                    txns.write().abort(xid)?;
                    Err(err)
                }
            }
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Ok(StatementResult::AffectedRows(0))
        }
    }?;

    if needs_catalog_persist {
        catalog_store.persist().map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "catalog persistence",
                actual: format!("{err:?}"),
            })
        })?;
    }

    Ok(result)
}

fn main() -> Result<(), String> {
    let base_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(default_base_dir);
    fs::create_dir_all(&base_dir).map_err(|e| e.to_string())?;

    let txns = Arc::new(RwLock::new(
        TransactionManager::new_durable(&base_dir).map_err(|e| format!("{e:?}"))?,
    ));
    let mut catalog_store = DurableCatalog::load(&base_dir).map_err(|e| format!("{e:?}"))?;
    ensure_default_people_table(&mut catalog_store)?;

    let smgr = MdStorageManager::new(&base_dir);
    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
    seed_if_empty(&pool, catalog_store.catalog(), &txns).map_err(|e| format!("{e:?}"))?;

    println!("PGRUST SQL REPL");
    println!("BASE DIRECTORY: {}", base_dir.display());
    println!(
        "TABLES: {}",
        catalog_store
            .catalog()
            .table_names()
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("COMMANDS: .help, .exit");

    let history_path = history_path(&base_dir);
    let mut history = load_history(&history_path)?;
    loop {
        let Some(line) = read_repl_line("pgrust> ", &mut history)? else {
            break;
        };

        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        append_history(&history_path, input)?;
        if input.eq_ignore_ascii_case(".exit") || input.eq_ignore_ascii_case(".quit") {
            break;
        }
        if input.eq_ignore_ascii_case(".help") {
            println!("ENTER A SINGLE SQL STATEMENT PER LINE.");
            println!("SUPPORTED: CREATE TABLE, DROP TABLE, SELECT, INSERT, UPDATE, DELETE");
            println!("EXAMPLE: create table widgets (id int4 not null, name text);");
            continue;
        }

        let sql = input.trim_end_matches(';').trim();
        match run_statement(sql, &pool, &txns, &mut catalog_store) {
            Ok(result) => print_result(result),
            Err(err) => eprintln!("ERROR: {}", render_exec_error(&err)),
        }
    }

    Ok(())
}
