use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::access::transam::xact::TransactionId;
use crate::backend::commands::copyfrom::parse_text_array_literal;
use crate::backend::commands::tablecmds::{
    execute_analyze, execute_delete_with_waiter, execute_insert,
    execute_prepared_insert_row, execute_truncate_table, execute_update_with_waiter,
};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, Value, execute_readonly_statement,
    parse_bytea_text,
};
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::parser::{
    ParseError, PreparedInsert, SelectStatement, Statement, bind_delete, bind_insert,
    bind_insert_prepared, bind_update,
};
use crate::backend::storage::lmgr::{TableLockManager, TableLockMode, unlock_relations};
use crate::backend::storage::smgr::StorageManager;
use crate::backend::utils::misc::guc::{is_postgres_guc, normalize_guc_name};
use crate::include::nodes::execnodes::ScalarType;
use crate::pgrust::database::Database;
use crate::pl::plpgsql::execute_do;
use crate::{ClientId, RelFileLocator};

pub struct SelectGuard<'a> {
    pub state: crate::include::nodes::execnodes::PlanState,
    pub ctx: ExecutorContext,
    pub columns: Vec<crate::backend::executor::QueryColumn>,
    pub column_names: Vec<String>,
    pub(crate) rels: Vec<RelFileLocator>,
    pub(crate) table_locks: &'a TableLockManager,
    pub(crate) client_id: ClientId,
}

impl Drop for SelectGuard<'_> {
    fn drop(&mut self) {
        unlock_relations(self.table_locks, self.client_id, &self.rels);
    }
}

struct ActiveTransaction {
    xid: TransactionId,
    failed: bool,
    held_table_locks: Vec<RelFileLocator>,
    next_command_id: u32,
}

pub struct Session {
    pub client_id: ClientId,
    active_txn: Option<ActiveTransaction>,
    gucs: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}

impl Session {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            active_txn: None,
            gucs: HashMap::new(),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn transaction_failed(&self) -> bool {
        self.active_txn.as_ref().is_some_and(|t| t.failed)
    }

    pub fn ready_status(&self) -> u8 {
        match &self.active_txn {
            None => b'I',
            Some(t) if t.failed => b'E',
            Some(_) => b'T',
        }
    }

    pub fn extra_float_digits(&self) -> i32 {
        self.gucs
            .get("extra_float_digits")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(1)
    }

    pub fn bytea_output(&self) -> ByteaOutputFormat {
        match self
            .gucs
            .get("bytea_output")
            .map(|value| value.trim().to_ascii_lowercase())
        {
            Some(value) if value == "escape" => ByteaOutputFormat::Escape,
            _ => ByteaOutputFormat::Hex,
        }
    }

    pub fn execute(&mut self, db: &Database, sql: &str) -> Result<StatementResult, ExecError> {
        let stmt = db.plan_cache.get_statement(sql)?;

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(reset_stmt),
            Statement::Begin => {
                if self.active_txn.is_some() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "no active transaction",
                        actual: "already in a transaction block".into(),
                    }));
                }
                let xid = db.txns.write().begin();
                self.active_txn = Some(ActiveTransaction {
                    xid,
                    failed: false,
                    held_table_locks: Vec::new(),
                    next_command_id: 0,
                });
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Commit => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                for rel in &txn.held_table_locks {
                    db.table_locks.unlock_table(*rel, self.client_id);
                }
                db.pool.write_wal_commit(txn.xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                db.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                db.txns.write().commit(txn.xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
                })?;
                db.apply_temp_on_commit(self.client_id)?;
                db.txn_waiter.notify();
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Rollback => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                for rel in &txn.held_table_locks {
                    db.table_locks.unlock_table(*rel, self.client_id);
                }
                let _ = db.txns.write().abort(txn.xid);
                db.txn_waiter.notify();
                Ok(StatementResult::AffectedRows(0))
            }
            _ => {
                if let Some(ref txn) = self.active_txn {
                    if txn.failed {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "ROLLBACK",
                            actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                        }));
                    }
                }

                if matches!(stmt, Statement::Vacuum(_)) && self.active_txn.is_some() {
                    return Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")));
                }

                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute(self.client_id, sql)
                }
            }
        }
    }

    pub fn execute_streaming<'a>(
        &mut self,
        db: &'a Database,
        select_stmt: &SelectStatement,
    ) -> Result<SelectGuard<'a>, ExecError> {
        let txn_ctx = if let Some(ref mut txn) = self.active_txn {
            let xid = txn.xid;
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            Some((xid, cid))
        } else {
            None
        };
        db.execute_streaming(self.client_id, select_stmt, txn_ctx)
    }

    fn execute_in_transaction(
        &mut self,
        db: &Database,
        stmt: Statement,
    ) -> Result<StatementResult, ExecError> {
        let txn = self.active_txn.as_mut().unwrap();
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);
        let client_id = self.client_id;

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(reset_stmt),
            Statement::Analyze(ref analyze_stmt) => {
                let visible_relcache = db.visible_relcache(client_id);
                execute_analyze(analyze_stmt.clone(), &visible_relcache)
            }
            Statement::Vacuum(_) => {
                Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")))
            }
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) | Statement::ShowTables => {
                db.sync_visible_catalog_heaps(client_id);
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let visible_relcache = db.visible_relcache(client_id);
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                execute_readonly_statement(stmt, &visible_relcache, &mut ctx)
            }
            Statement::Insert(ref insert_stmt) => {
                let visible_relcache = db.visible_relcache(client_id);
                let bound = bind_insert(insert_stmt, &visible_relcache)?;
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks
                        .lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                execute_insert(bound, &mut ctx, xid, cid)
            }
            Statement::Update(ref update_stmt) => {
                let visible_relcache = db.visible_relcache(client_id);
                let bound = bind_update(update_stmt, &visible_relcache)?;
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks
                        .lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                execute_update_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    cid,
                    Some((&db.txns, &db.txn_waiter)),
                )
            }
            Statement::Delete(ref delete_stmt) => {
                let visible_relcache = db.visible_relcache(client_id);
                let bound = bind_delete(delete_stmt, &visible_relcache)?;
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks
                        .lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                execute_delete_with_waiter(bound, &mut ctx, xid, Some((&db.txns, &db.txn_waiter)))
            }
            Statement::CreateTable(ref create_stmt) => {
                db.execute_create_table_stmt(client_id, create_stmt)
            }
            Statement::CreateTableAs(ref create_stmt) => {
                db.execute_create_table_as_stmt(client_id, create_stmt, Some(xid), cid)
            }
            Statement::DropTable(ref drop_stmt) => {
                let relcache = db.visible_relcache(client_id);
                let rels = {
                    drop_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| relcache.get_by_name(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    let txn = self.active_txn.as_mut().unwrap();
                    if !txn.held_table_locks.contains(&rel) {
                        db.table_locks
                            .lock_table(rel, TableLockMode::AccessExclusive, client_id);
                        txn.held_table_locks.push(rel);
                    }
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                let mut dropped = 0usize;
                for table_name in &drop_stmt.table_names {
                    if db.temp_entry(client_id, table_name).is_some() {
                        db.drop_temp_relation(client_id, table_name)?;
                        dropped += 1;
                    } else {
                        let mut catalog_guard = db.catalog.write();
                        match catalog_guard.drop_table(table_name) {
                            Ok(entry) => {
                                let _ = ctx.pool.invalidate_relation(entry.rel);
                                ctx.pool.with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                                dropped += 1;
                                db.plan_cache.invalidate_all();
                            }
                            Err(crate::backend::catalog::CatalogError::UnknownTable(_))
                                if drop_stmt.if_exists => {}
                            Err(crate::backend::catalog::CatalogError::UnknownTable(name)) => {
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
                }
                Ok(StatementResult::AffectedRows(dropped))
            }
            Statement::TruncateTable(ref truncate_stmt) => {
                let visible_relcache = db.visible_relcache(client_id);
                let rels = {
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| visible_relcache.get_by_name(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    let txn = self.active_txn.as_mut().unwrap();
                    if !txn.held_table_locks.contains(&rel) {
                        db.table_locks
                            .lock_table(rel, TableLockMode::AccessExclusive, client_id);
                        txn.held_table_locks.push(rel);
                    }
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                };
                execute_truncate_table(truncate_stmt.clone(), &visible_relcache, &mut ctx)
            }
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                unreachable!("handled in Session::execute")
            }
        }
    }

    fn apply_set(
        &mut self,
        stmt: &crate::backend::parser::SetStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = normalize_guc_name(&stmt.name);
        if !is_postgres_guc(&name) {
            return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                name,
            )));
        }
        self.gucs.insert(name, stmt.value.clone());
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_reset(
        &mut self,
        stmt: &crate::backend::parser::ResetStatement,
    ) -> Result<StatementResult, ExecError> {
        if let Some(name) = &stmt.name {
            let normalized = normalize_guc_name(name);
            if !is_postgres_guc(&normalized) {
                return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                    normalized,
                )));
            }
            self.gucs.remove(&normalized);
        } else {
            self.gucs.clear();
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub fn prepare_insert(
        &self,
        db: &Database,
        table_name: &str,
        columns: Option<&[String]>,
        num_params: usize,
    ) -> Result<PreparedInsert, ExecError> {
        let visible_relcache = db.visible_relcache(self.client_id);
        Ok(bind_insert_prepared(
            table_name,
            columns,
            num_params,
            &visible_relcache,
        )?)
    }

    pub fn execute_prepared_insert(
        &mut self,
        db: &Database,
        prepared: &PreparedInsert,
        params: &[Value],
    ) -> Result<(), ExecError> {
        let txn = self.active_txn.as_mut().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "active transaction",
                actual: "no active transaction for prepared insert".into(),
            })
        })?;
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);
        let client_id = self.client_id;

        let rel = prepared.rel;
        let txn = self.active_txn.as_mut().unwrap();
        if !txn.held_table_locks.contains(&rel) {
            db.table_locks
                .lock_table(rel, TableLockMode::RowExclusive, client_id);
            txn.held_table_locks.push(rel);
        }

        let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&db.pool),
            txns: db.txns.clone(),
            snapshot,
            client_id,
            next_command_id: cid,
            timed: false,
            outer_rows: Vec::new(),
        };
        execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid)
    }

    pub fn copy_from_rows(
        &mut self,
        db: &Database,
        table_name: &str,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let started_txn = if self.active_txn.is_none() {
            let xid = db.txns.write().begin();
            self.active_txn = Some(ActiveTransaction {
                xid,
                failed: false,
                held_table_locks: Vec::new(),
                next_command_id: 0,
            });
            true
        } else {
            false
        };

        let txn = self.active_txn.as_mut().unwrap();
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);

        let visible_relcache = db.visible_relcache(self.client_id);
        let (rel, desc) = {
            let entry = visible_relcache.get_by_name(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(table_name.to_string()))
            })?;
            (entry.rel, entry.desc.clone())
        };

        if !txn.held_table_locks.contains(&rel) {
            db.table_locks
                .lock_table(rel, TableLockMode::RowExclusive, self.client_id);
            txn.held_table_locks.push(rel);
        }

        let parsed_rows = rows
            .iter()
            .map(|row| {
                if row.len() != desc.columns.len() {
                    return Err(ExecError::Parse(ParseError::InvalidInsertTargetCount {
                        expected: desc.columns.len(),
                        actual: row.len(),
                    }));
                }

                row.iter()
                    .zip(desc.columns.iter())
                    .map(|(raw, column)| {
                        if raw == "\\N" {
                            return Ok(Value::Null);
                        }
                        match column.ty {
                            ScalarType::Int16 => {
                                raw.parse::<i16>().map(Value::Int16).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })
                            }
                            ScalarType::Int32 => {
                                raw.parse::<i32>().map(Value::Int32).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })
                            }
                            ScalarType::Int64 => {
                                raw.parse::<i64>().map(Value::Int64).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })
                            }
                            ScalarType::Float32 | ScalarType::Float64 => raw
                                .parse::<f64>()
                                .map(Value::Float64)
                                .map_err(|_| ExecError::TypeMismatch {
                                    op: "copy assignment",
                                    left: Value::Null,
                                    right: Value::Text(raw.clone().into()),
                                }),
                            ScalarType::Numeric => Ok(Value::Numeric(raw.as_str().into())),
                            ScalarType::Json => Ok(Value::Json(raw.clone().into())),
                            ScalarType::Jsonb => Ok(Value::Jsonb(
                                crate::backend::executor::jsonb::parse_jsonb_text(raw)?,
                            )),
                            ScalarType::JsonPath => Ok(Value::JsonPath(
                                canonicalize_jsonpath(raw)
                                    .map_err(|_| ExecError::InvalidStorageValue {
                                        column: "<copy>".into(),
                                        details: format!(
                                            "invalid input syntax for type jsonpath: \"{raw}\""
                                        ),
                                    })?
                                    .into(),
                            )),
                            ScalarType::Bytea => Ok(Value::Bytea(parse_bytea_text(raw)?)),
                            ScalarType::Text => Ok(Value::Text(raw.clone().into())),
                            ScalarType::Bool => match raw.as_str() {
                                "t" | "true" | "1" => Ok(Value::Bool(true)),
                                "f" | "false" | "0" => Ok(Value::Bool(false)),
                                _ => Err(ExecError::TypeMismatch {
                                    op: "copy assignment",
                                    left: Value::Null,
                                    right: Value::Text(raw.clone().into()),
                                }),
                            },
                            ScalarType::Array(_) => {
                                parse_text_array_literal(raw, column.sql_type.element_type())
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, ExecError>>()
            })
            .collect::<Result<Vec<_>, ExecError>>();

        let result = parsed_rows.and_then(|parsed_rows| {
            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&db.pool),
                txns: db.txns.clone(),
                snapshot,
                client_id: self.client_id,
                next_command_id: cid,
                timed: false,
                outer_rows: Vec::new(),
            };
            crate::backend::commands::tablecmds::execute_insert_values(
                rel,
                &desc,
                &parsed_rows,
                &mut ctx,
                xid,
                cid,
            )
        });

        if started_txn {
            let txn = self.active_txn.take().unwrap();
            for rel in &txn.held_table_locks {
                db.table_locks.unlock_table(*rel, self.client_id);
            }
            match result {
                Ok(n) => {
                    db.pool.write_wal_commit(txn.xid).map_err(|e| {
                        ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                            crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            )),
                        ))
                    })?;
                    db.pool.flush_wal().map_err(|e| {
                        ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                            crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            )),
                        ))
                    })?;
                    db.txns.write().commit(txn.xid).map_err(|e| {
                        ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
                    })?;
                    db.apply_temp_on_commit(self.client_id)?;
                    db.txn_waiter.notify();
                    Ok(n)
                }
                Err(e) => {
                    let _ = db.txns.write().abort(txn.xid);
                    db.txn_waiter.notify();
                    Err(e)
                }
            }
        } else {
            result
        }
    }
}
