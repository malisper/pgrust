use parking_lot::RwLock;
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use pgrust::backend::access::heap::heapam::{heap_flush, heap_insert_mvcc};
use pgrust::backend::access::transam::xact::{INVALID_TRANSACTION_ID, TransactionManager};
use pgrust::backend::catalog::{CatalogStore, column_desc};
use pgrust::backend::commands::tablecmds::{
    execute_delete_with_waiter, execute_insert, execute_truncate_table, execute_update_with_waiter,
};
use pgrust::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use pgrust::backend::utils::cache::relcache::RelCache;
use pgrust::backend::utils::misc::interrupts::InterruptState;
use pgrust::executor::{
    ExecError, ExecutorContext, RelationDesc, StatementResult, Value, execute_readonly_statement,
};
use pgrust::include::access::htup::{HeapTuple, TupleValue};
use pgrust::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, Statement, bind_delete, bind_insert,
    bind_update, create_relation_desc, normalize_create_table_name, parse_statement,
};
use pgrust::pl::plpgsql::{RaiseLevel, clear_notices, execute_do, take_notices};
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

fn default_base_dir() -> PathBuf {
    std::env::temp_dir().join("pgrust_query_repl")
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
        Value::Xml(v) => v.to_string(),
        Value::Range(_) => pgrust::backend::executor::render_range_text(value).unwrap_or_default(),
        Value::Multirange(_) => {
            pgrust::backend::executor::render_multirange_text(value).unwrap_or_default()
        }
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
        Value::Text(v) => v.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap().to_string(),
        Value::InternalChar(v) => pgrust::backend::executor::render_internal_char_text(*v),
        Value::EnumOid(v) => v.to_string(),
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
        Value::Record(record) => format!("{:?}", record.fields),
        Value::Null => "NULL".into(),
    }
}

fn print_result(result: StatementResult) {
    match result {
        StatementResult::AffectedRows(count) => {
            println!("AFFECTED ROWS: {}", count);
        }
        StatementResult::Query {
            column_names, rows, ..
        } => {
            if column_names.is_empty() {
                println!("({} rows)", rows.len());
                return;
            }

            let mut widths = column_names
                .iter()
                .map(|name| name.len())
                .collect::<Vec<_>>();
            let rendered_rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| render_value(&value))
                        .collect::<Vec<_>>()
                })
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

fn print_plpgsql_notices() {
    for notice in take_notices() {
        let level = match notice.level {
            RaiseLevel::Info => "INFO",
            RaiseLevel::Notice => "NOTICE",
            RaiseLevel::Warning => "WARNING",
            RaiseLevel::Exception => "EXCEPTION",
        };
        println!("{level}: {}", notice.message);
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

fn ensure_default_people_table(catalog_store: &mut CatalogStore) -> Result<(), String> {
    if catalog_store
        .relation("people")
        .map_err(|e| format!("{e:?}"))?
        .is_some()
    {
        return Ok(());
    }
    catalog_store
        .create_table("people", desc())
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn seed_if_empty(
    pool: &std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    relcache: &RelCache,
    txns: &Arc<RwLock<TransactionManager>>,
) -> Result<(), ExecError> {
    let rel = relcache
        .get_by_name("people")
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
        let tid = heap_insert_mvcc(&**pool, 1, rel, xid, &row)?;
        heap_flush(&**pool, 1, rel, tid.block_number)?;
    }
    txns.write().commit(xid)?;
    Ok(())
}

fn run_statement(
    sql: &str,
    pool: &std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
    catalog_store: &mut CatalogStore,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    let interrupts = Arc::new(InterruptState::new());
    let stats = Arc::new(parking_lot::RwLock::new(
        pgrust::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
    ));
    let session_stats = Arc::new(parking_lot::RwLock::new(
        pgrust::pgrust::database::SessionStatsState::default(),
    ));
    let relcache = catalog_store.relcache().map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "physical relcache",
            actual: format!("{err:?}"),
        })
    })?;

    clear_notices();
    let result = match stmt {
        Statement::Do(stmt) => execute_do(&stmt),
        Statement::SetConstraints(_) => {
            pgrust::backend::utils::misc::notices::push_warning(
                "SET CONSTRAINTS can only be used in transaction blocks",
            );
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Show(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::Set(_)
        | Statement::Reset(_)
        | Statement::Checkpoint(_)
        | Statement::Notify(_)
        | Statement::Listen(_)
        | Statement::Unlisten(_)
        | Statement::CopyFrom(_)
        | Statement::CopyTo(_)
        | Statement::CreatePublication(_)
        | Statement::AlterPublication(_)
        | Statement::DropPublication(_)
        | Statement::CommentOnPublication(_)
        | Statement::CommentOnAggregate(_)
        | Statement::CommentOnFunction(_)
        | Statement::CommentOnOperator(_)
        | Statement::CreateTrigger(_)
        | Statement::DropTrigger(_)
        | Statement::AlterTableTriggerState(_)
        | Statement::AlterTriggerRename(_)
        | Statement::CreateAggregate(_)
        | Statement::DropAggregate(_)
        | Statement::AlterTableSet(_)
        | Statement::CreateStatistics(_)
        | Statement::AlterStatistics(_)
        | Statement::DropStatistics(_)
        | Statement::CommentOnStatistics(_)
        | Statement::AlterTableAddColumn(_)
        | Statement::AlterTableAddConstraint(_)
        | Statement::AlterTableDropConstraint(_)
        | Statement::AlterTableAlterConstraint(_)
        | Statement::AlterTableRenameConstraint(_)
        | Statement::AlterTableAlterColumnCompression(_)
        | Statement::AlterTableAlterColumnOptions(_)
        | Statement::AlterTableAlterColumnStatistics(_)
        | Statement::AlterIndexAlterColumnStatistics(_)
        | Statement::AlterIndexAttachPartition(_)
        | Statement::AlterTableAlterColumnStorage(_)
        | Statement::AlterTableAlterColumnDefault(_)
        | Statement::AlterTableAlterColumnExpression(_)
        | Statement::AlterTableSetNotNull(_)
        | Statement::AlterTableDropNotNull(_)
        | Statement::AlterTableValidateConstraint(_)
        | Statement::AlterTableInherit(_)
        | Statement::AlterTableNoInherit(_)
        | Statement::AlterTableAttachPartition(_)
        | Statement::AlterTableDetachPartition(_)
        | Statement::AlterIndexRename(_)
        | Statement::AlterTableSetRowSecurity(_)
        | Statement::CreatePolicy(_)
        | Statement::AlterPolicy(_)
        | Statement::AlterOperator(_)
        | Statement::RefreshMaterializedView(_)
        | Statement::DropMaterializedView(_)
        | Statement::DropPolicy(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::AlterTableOwner(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER TABLE OWNER in query_repl: {} -> {}",
                stmt.relation_name, stmt.new_owner
            ))))
        }
        Statement::AlterSchemaOwner(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER SCHEMA OWNER in query_repl: {} -> {}",
                stmt.schema_name, stmt.new_owner
            ))))
        }
        Statement::CommentOnRole(_)
        | Statement::CommentOnConversion(_)
        | Statement::CommentOnForeignDataWrapper(_)
        | Statement::CreateForeignDataWrapper(_)
        | Statement::CreateForeignServer(_)
        | Statement::CreateForeignTable(_)
        | Statement::AlterForeignDataWrapper(_)
        | Statement::AlterForeignDataWrapperOwner(_)
        | Statement::AlterForeignDataWrapperRename(_)
        | Statement::DropForeignDataWrapper(_)
        | Statement::CreateRole(_)
        | Statement::CreateDatabase(_)
        | Statement::AlterRole(_)
        | Statement::DropRole(_)
        | Statement::DropDatabase(_)
        | Statement::GrantObject(_)
        | Statement::RevokeObject(_)
        | Statement::GrantRoleMembership(_)
        | Statement::RevokeRoleMembership(_)
        | Statement::SetRole(_)
        | Statement::ResetRole(_)
        | Statement::SetSessionAuthorization(_)
        | Statement::ResetSessionAuthorization(_)
        | Statement::DropOwned(_)
        | Statement::ReassignOwned(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "role management".into(),
        ))),
        Statement::AlterViewOwner(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER VIEW OWNER in query_repl: {} -> {}",
                stmt.relation_name, stmt.new_owner
            ))))
        }
        Statement::AlterViewRename(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER VIEW RENAME in query_repl: {} -> {}",
                stmt.table_name, stmt.new_table_name
            ))))
        }
        Statement::AlterSequence(stmt) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            format!("ALTER SEQUENCE in query_repl: {}", stmt.sequence_name),
        ))),
        Statement::AlterSequenceOwner(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER SEQUENCE OWNER in query_repl: {} -> {}",
                stmt.relation_name, stmt.new_owner
            ))))
        }
        Statement::AlterSequenceRename(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER SEQUENCE RENAME in query_repl: {} -> {}",
                stmt.table_name, stmt.new_table_name
            ))))
        }
        Statement::AlterTableRenameColumn(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER TABLE RENAME COLUMN in query_repl: {}.{} -> {}",
                stmt.table_name, stmt.column_name, stmt.new_column_name
            ))))
        }
        Statement::AlterTableRename(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER TABLE RENAME in query_repl: {} -> {}",
                stmt.table_name, stmt.new_table_name
            ))))
        }
        Statement::AlterTableDropColumn(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER TABLE DROP COLUMN in query_repl: {}.{}",
                stmt.table_name, stmt.column_name
            ))))
        }
        Statement::AlterTableAlterColumnType(stmt) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER TABLE ALTER COLUMN TYPE in query_repl: {}.{} -> {:?}",
                stmt.table_name, stmt.column_name, stmt.ty
            ))))
        }
        Statement::Unsupported(stmt) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            format!("{}: {}", stmt.feature, stmt.sql),
        ))),
        Statement::CommentOnTable(stmt) => {
            let xid = txns.write().begin();
            let result = {
                let ctx = pgrust::backend::catalog::store::CatalogWriteContext {
                    pool: std::sync::Arc::clone(pool),
                    txns: txns.clone(),
                    xid,
                    cid: 0,
                    client_id: 21,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let relcache = catalog_store.relcache().map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "physical relcache",
                        actual: format!("{err:?}"),
                    })
                })?;
                let relation =
                    relcache
                        .get_by_name(&stmt.table_name)
                        .cloned()
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(stmt.table_name.clone()))
                        })?;
                if relation.relpersistence == 't' {
                    Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "permanent table for COMMENT ON TABLE",
                        actual: "temporary table".into(),
                    }))
                } else {
                    catalog_store
                        .comment_relation_mvcc(relation.relation_oid, stmt.comment.as_deref(), &ctx)
                        .map_err(|other| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "table comment update",
                                actual: format!("{other:?}"),
                            })
                        })?;
                    Ok(StatementResult::AffectedRows(0))
                }
            };
            match result {
                Ok(ok) => {
                    txns.write().commit(xid)?;
                    Ok(ok)
                }
                Err(err) => {
                    let _ = txns.write().abort(xid);
                    Err(err)
                }
            }
        }
        Statement::CreateIndex(stmt) => Ok(catalog_store
            .create_index(
                stmt.index_name,
                &stmt.table_name,
                stmt.unique,
                &stmt.columns,
            )
            .map(|entry| {
                let _ = pool.with_storage_mut(|s| {
                    let _ = s.smgr.open(entry.rel);
                    let _ = s.smgr.create(entry.rel, ForkNumber::Main, false);
                });
                StatementResult::AffectedRows(0)
            })
            .map_err(|other| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "catalog index creation",
                    actual: format!("{other:?}"),
                })
            })?),
        Statement::Merge(stmt) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            format!("MERGE in query_repl: {}", stmt.target_table),
        ))),
        Statement::Explain(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_readonly_statement(Statement::Explain(stmt), &relcache, &mut ctx)
        }
        Statement::Select(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_readonly_statement(Statement::Select(stmt), &relcache, &mut ctx)
        }
        Statement::Values(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_readonly_statement(Statement::Values(stmt), &relcache, &mut ctx)
        }
        Statement::Analyze(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_readonly_statement(Statement::Analyze(stmt), &relcache, &mut ctx)
        }
        Statement::CommentOnConstraint(_)
        | Statement::CommentOnDomain(_)
        | Statement::CommentOnTrigger(_)
        | Statement::CreateConversion(_)
        | Statement::CommentOnIndex(_)
        | Statement::CommentOnRule(_)
        | Statement::CreateFunction(_)
        | Statement::CreateOperator(_)
        | Statement::CreateOperatorClass(_)
        | Statement::CreateRule(_)
        | Statement::CreateSchema(_)
        | Statement::CreateTablespace(_)
        | Statement::CreateDomain(_)
        | Statement::CreateType(_)
        | Statement::AlterType(_)
        | Statement::AlterTypeOwner(_)
        | Statement::CreateSequence(_)
        | Statement::DropFunction(_)
        | Statement::DropOperator(_)
        | Statement::DropDomain(_)
        | Statement::DropConversion(_)
        | Statement::DropRule(_)
        | Statement::DropType(_)
        | Statement::DropSequence(_)
        => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "conversion/domain/function/type/sequence/rule statements are not supported in query_repl"
                .into(),
        ))),
        Statement::CreateTable(stmt) => {
            let (table_name, _) = normalize_create_table_name(&stmt)?;
            let entry = catalog_store
                .create_table(table_name, create_relation_desc(&stmt, &relcache)?)
                .map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "catalog table creation",
                        actual: format!("{err:?}"),
                    })
                })?;
            let _ = pool.with_storage_mut(|s| {
                let _ = s.smgr.open(entry.rel);
                let _ = s.smgr.create(entry.rel, ForkNumber::Main, false);
            });
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::CreateTableAs(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TABLE AS through Database/session path",
            actual: "CREATE TABLE AS".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE VIEW through Database/session path",
            actual: "CREATE VIEW".into(),
        })),
        Statement::DropTable(stmt) => {
            let mut dropped = 0;
            for table_name in stmt.table_names {
                match catalog_store.drop_table(&table_name) {
                    Ok(entries) => {
                        for entry in entries {
                            let _ = pool.invalidate_relation(entry.rel);
                            pool.with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                        }
                        dropped += 1;
                    }
                    Err(pgrust::backend::catalog::CatalogError::UnknownTable(_))
                        if stmt.if_exists => {}
                    Err(pgrust::backend::catalog::CatalogError::UnknownTable(name)) => {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
                    }
                    Err(other) => {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "droppable table",
                            actual: format!("{other:?}"),
                        }));
                    }
                }
            }
            Ok(StatementResult::AffectedRows(dropped))
        }
        Statement::DropIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP INDEX through Database/session path",
            actual: "DROP INDEX".into(),
        })),
        Statement::DropView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP VIEW through Database/session path",
            actual: "DROP VIEW".into(),
        })),
        Statement::DropSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP SCHEMA through Database/session path",
            actual: "DROP SCHEMA".into(),
        })),
        Statement::TruncateTable(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: true,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_truncate_table(stmt, &relcache, &mut ctx, INVALID_TRANSACTION_ID)
        }
        Statement::Vacuum(stmt) => {
            let mut ctx = ExecutorContext {
                pool: std::sync::Arc::clone(pool),
                txns: txns.clone(),
                txn_waiter: None,
                lock_status_provider: None,
                sequences: None,
                large_objects: None,
                async_notify_runtime: None,
                checkpoint_stats:
                    pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config:
                    pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&stats),
                session_stats: Arc::clone(&session_stats),
                snapshot: txns.read().snapshot(INVALID_TRANSACTION_ID)?,
                transaction_state: None,
                client_id: 21,
                session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                next_command_id: 0,
                default_toast_compression:
                    pgrust::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog: relcache.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                advisory_locks: Arc::new(
                    pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                ),
                row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                current_database_name: String::new(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
            };
            execute_readonly_statement(Statement::Vacuum(stmt), &relcache, &mut ctx)
        }
        Statement::Insert(stmt) => {
            let xid = txns.write().begin();
            let result = {
                let bound = bind_insert(&stmt, &relcache)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(pool),
                    txns: txns.clone(),
                    txn_waiter: None,
                    lock_status_provider: None,
                    sequences: None,
                    large_objects: None,
                    async_notify_runtime: None,
                    checkpoint_stats:
                        pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                    datetime_config:
                        pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                    gucs: std::collections::HashMap::new(),
                    interrupts: Arc::clone(&interrupts),
                    stats: Arc::clone(&stats),
                    session_stats: Arc::clone(&session_stats),
                    snapshot: txns.read().snapshot(xid)?,
                    transaction_state: None,
                    client_id: 21,
                    session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    active_role_oid: None,
                    session_replication_role: Default::default(),
                    next_command_id: 0,
                    default_toast_compression:
                        pgrust::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: relcache.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                    advisory_locks: Arc::new(
                        pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                    ),
                    row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                    current_database_name: String::new(),
                    statement_lock_scope_id: None,
                    transaction_lock_scope_id: None,
                };
                execute_insert(bound, &relcache, &mut ctx, xid, 0)
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
                let bound = bind_update(&stmt, &relcache)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(pool),
                    txns: txns.clone(),
                    txn_waiter: None,
                    lock_status_provider: None,
                    sequences: None,
                    large_objects: None,
                    async_notify_runtime: None,
                    checkpoint_stats:
                        pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                    datetime_config:
                        pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                    gucs: std::collections::HashMap::new(),
                    interrupts: Arc::clone(&interrupts),
                    stats: Arc::clone(&stats),
                    session_stats: Arc::clone(&session_stats),
                    snapshot: txns.read().snapshot(xid)?,
                    transaction_state: None,
                    client_id: 21,
                    session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    active_role_oid: None,
                    session_replication_role: Default::default(),
                    next_command_id: 0,
                    default_toast_compression:
                        pgrust::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: relcache.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                    advisory_locks: Arc::new(
                        pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                    ),
                    row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                    current_database_name: String::new(),
                    statement_lock_scope_id: None,
                    transaction_lock_scope_id: None,
                };
                execute_update_with_waiter(bound, &relcache, &mut ctx, xid, 0, None)
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
                let bound = bind_delete(&stmt, &relcache)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(pool),
                    txns: txns.clone(),
                    txn_waiter: None,
                    lock_status_provider: None,
                    sequences: None,
                    large_objects: None,
                    async_notify_runtime: None,
                    checkpoint_stats:
                        pgrust::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                    datetime_config:
                        pgrust::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    statement_timestamp_usecs: pgrust::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                    gucs: std::collections::HashMap::new(),
                    interrupts: Arc::clone(&interrupts),
                    stats: Arc::clone(&stats),
                    session_stats: Arc::clone(&session_stats),
                    snapshot: txns.read().snapshot(xid)?,
                    transaction_state: None,
                    client_id: 21,
                    session_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    current_user_oid: pgrust::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    active_role_oid: None,
                    session_replication_role: Default::default(),
                    next_command_id: 0,
                    default_toast_compression:
                        pgrust::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: pgrust::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: relcache.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                    advisory_locks: Arc::new(
                        pgrust::backend::storage::lmgr::AdvisoryLockManager::new(),
                    ),
                    row_locks: Arc::new(pgrust::backend::storage::lmgr::RowLockManager::new()),
                    current_database_name: String::new(),
                    statement_lock_scope_id: None,
                    transaction_lock_scope_id: None,
                };
                execute_delete_with_waiter(bound, &relcache, &mut ctx, xid, None)
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
        Statement::DeclareCursor(_)
        | Statement::Fetch(_)
        | Statement::Move(_)
        | Statement::ClosePortal(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::Savepoint(_)
        | Statement::RollbackTo(_) => {
            Ok(StatementResult::AffectedRows(0))
        }
    };

    print_plpgsql_notices();
    result
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
    let mut catalog_store = CatalogStore::load(&base_dir).map_err(|e| format!("{e:?}"))?;
    ensure_default_people_table(&mut catalog_store)?;

    let smgr = MdStorageManager::new(&base_dir);
    let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
    let relcache = catalog_store.relcache().map_err(|e| format!("{e:?}"))?;
    seed_if_empty(&pool, &relcache, &txns).map_err(|e| format!("{e:?}"))?;

    println!("PGRUST SQL REPL");
    println!("BASE DIRECTORY: {}", base_dir.display());
    println!(
        "TABLES: {}",
        catalog_store
            .visible_table_names()
            .map_err(|e| format!("{e:?}"))?
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
