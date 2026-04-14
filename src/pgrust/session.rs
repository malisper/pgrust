use std::collections::HashMap;
use std::fs;
use std::mem;
use std::sync::Arc;

use crate::backend::access::transam::xact::TransactionId;
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::commands::copyfrom::parse_text_array_literal;
use crate::backend::commands::tablecmds::{
    execute_delete_with_waiter, execute_insert, execute_prepared_insert_row,
    execute_truncate_table, execute_update_with_waiter,
};
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, Value, cast_value, execute_readonly_statement,
    parse_bytea_text,
};
use crate::backend::parser::{
    CatalogLookup, CopyFromStatement, CopySource, ParseError, ParseOptions, PreparedInsert,
    SelectStatement, Statement, bind_delete, bind_insert, bind_insert_prepared, bind_update,
};
use crate::backend::storage::lmgr::{TableLockManager, TableLockMode, unlock_relations};
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::guc::{is_postgres_guc, normalize_guc_name};
use crate::backend::utils::misc::guc_datetime::{
    DateTimeConfig, default_datestyle, default_timezone, format_datestyle, parse_datestyle,
    parse_timezone,
};
use crate::include::nodes::execnodes::ScalarType;
use crate::pgrust::database::{Database, TempMutationEffect};
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
    catalog_effects: Vec<CatalogMutationEffect>,
    current_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    prior_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    temp_effects: Vec<TempMutationEffect>,
}

pub struct Session {
    pub client_id: ClientId,
    active_txn: Option<ActiveTransaction>,
    gucs: HashMap<String, String>,
    datetime_config: DateTimeConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}

impl Session {
    const DEFAULT_MAINTENANCE_WORK_MEM_KB: usize = 65_536;

    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            active_txn: None,
            gucs: HashMap::new(),
            datetime_config: DateTimeConfig::default(),
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

    pub fn standard_conforming_strings(&self) -> bool {
        !matches!(
            self.gucs
                .get("standard_conforming_strings")
                .map(|value| value.trim().to_ascii_lowercase())
                .as_deref(),
            Some("off" | "false")
        )
    }

    pub fn maintenance_work_mem_kb(&self) -> Result<usize, ExecError> {
        let Some(raw) = self.gucs.get("maintenance_work_mem") else {
            return Ok(Self::DEFAULT_MAINTENANCE_WORK_MEM_KB);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::DEFAULT_MAINTENANCE_WORK_MEM_KB);
        }
        let split_at = trimmed
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(trimmed.len());
        let (digits, suffix) = trimmed.split_at(split_at);
        let value = digits.parse::<usize>().map_err(|_| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid maintenance_work_mem value",
                actual: trimmed.to_string(),
            })
        })?;
        let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
            "" | "kb" => 1usize,
            "mb" => 1024usize,
            "gb" => 1024usize * 1024usize,
            _ => {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "maintenance_work_mem with optional kB, MB, or GB suffix",
                    actual: trimmed.to_string(),
                }));
            }
        };
        value.checked_mul(multiplier).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "maintenance_work_mem within usize range",
                actual: trimmed.to_string(),
            })
        })
    }

    pub(crate) fn catalog_txn_ctx(&self) -> Option<(TransactionId, u32)> {
        self.active_txn
            .as_ref()
            .map(|txn| (txn.xid, txn.next_command_id))
    }

    pub(crate) fn configured_search_path(&self) -> Option<Vec<String>> {
        let value = self.gucs.get("search_path")?;
        if value.trim().eq_ignore_ascii_case("default") {
            return None;
        }
        Some(
            value
                .split(',')
                .map(|schema| {
                    schema
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_ascii_lowercase()
                })
                .filter(|schema| !schema.is_empty())
                .collect(),
        )
    }

    pub(crate) fn catalog_lookup<'a>(&self, db: &'a Database) -> LazyCatalogLookup<'a> {
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(
            self.client_id,
            self.active_txn
                .as_ref()
                .map(|txn| (txn.xid, txn.next_command_id)),
            search_path.as_deref(),
        )
    }

    fn catalog_lookup_for_command<'a>(
        &self,
        db: &'a Database,
        xid: TransactionId,
        cid: u32,
    ) -> LazyCatalogLookup<'a> {
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(self.client_id, Some((xid, cid)), search_path.as_deref())
    }

    fn process_catalog_command_end(&mut self, db: &Database, effect_start: usize) {
        let client_id = self.client_id;
        let Some(txn) = self.active_txn.as_mut() else {
            return;
        };
        txn.current_cmd_catalog_invalidations = txn.catalog_effects[effect_start..]
            .iter()
            .map(Database::catalog_invalidation_from_effect)
            .filter(|invalidation| !invalidation.is_empty())
            .collect();
        if txn.current_cmd_catalog_invalidations.is_empty() {
            return;
        }
        db.finalize_command_end_local_catalog_invalidations(
            client_id,
            &txn.current_cmd_catalog_invalidations,
        );
        txn.prior_cmd_catalog_invalidations
            .extend(mem::take(&mut txn.current_cmd_catalog_invalidations));
    }

    pub fn execute(&mut self, db: &Database, sql: &str) -> Result<StatementResult, ExecError> {
        // :HACK: Support simple file-backed COPY FROM on the normal SQL path
        // until COPY is modeled as a real parsed/bound statement.
        if let Some((table_name, columns, file_path)) = parse_copy_from_file(sql) {
            let rows = read_copy_from_file(&file_path)?;
            let inserted = self.copy_from_rows_into(db, &table_name, columns.as_deref(), &rows)?;
            return Ok(StatementResult::AffectedRows(inserted));
        }
        let stmt = if self.standard_conforming_strings() {
            db.plan_cache.get_statement(sql)?
        } else {
            crate::backend::parser::parse_statement_with_options(
                sql,
                ParseOptions {
                    standard_conforming_strings: false,
                },
            )?
        };

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Show(ref show_stmt) => self.apply_show(show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(reset_stmt),
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CreateIndex(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_index_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                    self.maintenance_work_mem_kb()?,
                )
            }
            Statement::AlterTableAddColumn(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_add_column_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CommentOnTable(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_table_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
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
                    catalog_effects: Vec::new(),
                    current_cmd_catalog_invalidations: Vec::new(),
                    prior_cmd_catalog_invalidations: Vec::new(),
                    temp_effects: Vec::new(),
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
                db.finalize_committed_catalog_effects(
                    self.client_id,
                    &txn.catalog_effects,
                    &txn.prior_cmd_catalog_invalidations,
                );
                db.finalize_committed_temp_effects(self.client_id, &txn.temp_effects);
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
                db.finalize_aborted_local_catalog_invalidations(
                    self.client_id,
                    &txn.prior_cmd_catalog_invalidations,
                    &txn.current_cmd_catalog_invalidations,
                );
                db.finalize_aborted_catalog_effects(&txn.catalog_effects);
                db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
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
                    let search_path = self.configured_search_path();
                    db.execute_statement_with_search_path(
                        self.client_id,
                        stmt,
                        search_path.as_deref(),
                    )
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
        let search_path = self.configured_search_path();
        db.execute_streaming_with_search_path(
            self.client_id,
            select_stmt,
            txn_ctx,
            search_path.as_deref(),
        )
    }

    fn execute_in_transaction(
        &mut self,
        db: &Database,
        stmt: Statement,
    ) -> Result<StatementResult, ExecError> {
        let effect_start = self
            .active_txn
            .as_ref()
            .map(|txn| txn.catalog_effects.len())
            .unwrap_or(0);
        let (xid, cid) = {
            let txn = self.active_txn.as_mut().unwrap();
            let xid = txn.xid;
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            (xid, cid)
        };
        let client_id = self.client_id;

        let result = match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Show(ref show_stmt) => self.apply_show(show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(reset_stmt),
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CreateIndex(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let maintenance_work_mem_kb = self.maintenance_work_mem_kb()?;
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_index_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    maintenance_work_mem_kb,
                    catalog_effects,
                )
            }
            Statement::AlterTableAddColumn(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation =
                    catalog
                        .lookup_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&relation.rel) {
                    db.table_locks.lock_table(
                        relation.rel,
                        TableLockMode::AccessExclusive,
                        client_id,
                    );
                    txn.held_table_locks.push(relation.rel);
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Unsupported(ref unsupported_stmt) => {
                Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                    "{}: {}",
                    unsupported_stmt.feature, unsupported_stmt.sql
                ))))
            }
            Statement::CommentOnTable(ref comment_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_relation(&comment_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            comment_stmt.table_name.clone(),
                        ))
                    })?;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&relation.rel) {
                    db.table_locks.lock_table(
                        relation.rel,
                        TableLockMode::AccessExclusive,
                        client_id,
                    );
                    txn.held_table_locks.push(relation.rel);
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_table_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::Analyze(ref analyze_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_analyze_stmt_in_transaction_with_search_path(
                    client_id,
                    analyze_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::Vacuum(_) => {
                Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")))
            }
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let search_path = self.configured_search_path();
                let catalog =
                    db.lazy_catalog_lookup(client_id, Some((xid, cid)), search_path.as_deref());
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    txn_waiter: Some(db.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                };
                execute_readonly_statement(stmt, &catalog, &mut ctx)
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_insert(insert_stmt, &catalog)?;
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
                    txn_waiter: Some(db.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                };
                execute_insert(bound, &catalog, &mut ctx, xid, cid)
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_update(update_stmt, &catalog)?;
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
                    txn_waiter: Some(db.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                };
                execute_update_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    cid,
                    Some((&db.txns, &db.txn_waiter)),
                )
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_delete(delete_stmt, &catalog)?;
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
                    txn_waiter: Some(db.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                };
                execute_delete_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&db.txns, &db.txn_waiter)),
                )
            }
            Statement::CreateTable(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_table_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
            }
            Statement::CreateView(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_view_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CreateTableAs(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_table_as_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
            }
            Statement::DropView(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let rels = {
                    drop_stmt
                        .view_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
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
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_view_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropTable(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let rels = {
                    drop_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
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
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
            }
            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let rels = {
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
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
                    txn_waiter: Some(db.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                };
                execute_truncate_table(truncate_stmt.clone(), &catalog, &mut ctx, xid)
            }
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                unreachable!("handled in Session::execute")
            }
        };

        if result.is_ok() {
            self.process_catalog_command_end(db, effect_start);
        }

        result
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
        match name.as_str() {
            "datestyle" => {
                let Some((date_style_format, date_order)) = parse_datestyle(&stmt.value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        stmt.value.clone(),
                    )));
                };
                self.datetime_config.date_style_format = date_style_format;
                self.datetime_config.date_order = date_order;
            }
            "timezone" => {
                let Some(time_zone) = parse_timezone(&stmt.value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        stmt.value.clone(),
                    )));
                };
                self.datetime_config.time_zone = time_zone;
            }
            _ => {}
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
            match normalized.as_str() {
                "datestyle" => self.guc_reset_datestyle(),
                "timezone" => self.guc_reset_timezone(),
                _ => {}
            }
            self.gucs.remove(&normalized);
        } else {
            self.gucs.clear();
            self.guc_reset_datestyle();
            self.guc_reset_timezone();
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_show(
        &mut self,
        stmt: &crate::backend::parser::ShowStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = normalize_guc_name(&stmt.name);
        if name == "tables" {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "configuration parameter",
                actual: stmt.name.clone(),
            }));
        }
        if !is_postgres_guc(&name) {
            return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                name,
            )));
        }

        let (column_name, value) = match name.as_str() {
            "datestyle" => (
                "DateStyle".to_string(),
                format_datestyle(&self.datetime_config),
            ),
            "timezone" => (
                "TimeZone".to_string(),
                self.datetime_config.time_zone.clone(),
            ),
            _ => (
                stmt.name.clone(),
                self.gucs
                    .get(&name)
                    .cloned()
                    .unwrap_or_else(|| match name.as_str() {
                        "datestyle" => default_datestyle().to_string(),
                        "timezone" => default_timezone().to_string(),
                        _ => "default".to_string(),
                    }),
            ),
        };

        Ok(StatementResult::Query {
            columns: vec![crate::backend::executor::QueryColumn::text(
                column_name.clone(),
            )],
            column_names: vec![column_name],
            rows: vec![vec![Value::Text(value.into())]],
        })
    }

    fn guc_reset_datestyle(&mut self) {
        let (date_style_format, date_order) =
            parse_datestyle(default_datestyle()).expect("default DateStyle must parse");
        self.datetime_config.date_style_format = date_style_format;
        self.datetime_config.date_order = date_order;
    }

    fn guc_reset_timezone(&mut self) {
        self.datetime_config.time_zone = default_timezone().to_string();
    }

    pub fn prepare_insert(
        &self,
        db: &Database,
        table_name: &str,
        columns: Option<&[String]>,
        num_params: usize,
    ) -> Result<PreparedInsert, ExecError> {
        let catalog = self.catalog_lookup(db);
        Ok(bind_insert_prepared(
            table_name, columns, num_params, &catalog,
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
            txn_waiter: Some(db.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: cid,
            timed: false,
            outer_rows: Vec::new(),
            subplans: Vec::new(),
        };
        execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid)
    }

    pub fn copy_from_rows(
        &mut self,
        db: &Database,
        table_name: &str,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        self.copy_from_rows_into(db, table_name, None, rows)
    }

    pub fn copy_from_rows_into(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let started_txn = if self.active_txn.is_none() {
            let xid = db.txns.write().begin();
            self.active_txn = Some(ActiveTransaction {
                xid,
                failed: false,
                held_table_locks: Vec::new(),
                next_command_id: 0,
                catalog_effects: Vec::new(),
                current_cmd_catalog_invalidations: Vec::new(),
                prior_cmd_catalog_invalidations: Vec::new(),
                temp_effects: Vec::new(),
            });
            true
        } else {
            false
        };

        let (xid, cid) = {
            let txn = self.active_txn.as_mut().unwrap();
            let xid = txn.xid;
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            (xid, cid)
        };

        let catalog = self.catalog_lookup_for_command(db, xid, cid);
        let (rel, toast, toast_index, desc, indexes) = {
            let entry = catalog.lookup_any_relation(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(table_name.to_string()))
            })?;
            let toast_index = entry.toast.and_then(|toast| {
                catalog
                    .index_relations_for_heap(toast.relation_oid)
                    .into_iter()
                    .next()
            });
            (
                entry.rel,
                entry.toast,
                toast_index,
                entry.desc.clone(),
                catalog.index_relations_for_heap(entry.relation_oid),
            )
        };
        let target_indexes = if let Some(columns) = target_columns {
            let mut indexes = Vec::with_capacity(columns.len());
            for name in columns {
                let Some(index) = desc.columns.iter().position(|column| column.name == *name)
                else {
                    return Err(ExecError::Parse(ParseError::UnknownColumn(name.clone())));
                };
                indexes.push(index);
            }
            indexes
        } else {
            (0..desc.columns.len()).collect()
        };

        let txn = self.active_txn.as_mut().unwrap();
        if !txn.held_table_locks.contains(&rel) {
            db.table_locks
                .lock_table(rel, TableLockMode::RowExclusive, self.client_id);
            txn.held_table_locks.push(rel);
        }

        let parsed_rows = rows
            .iter()
            .map(|row| {
                if row.len() != target_indexes.len() {
                    return Err(ExecError::Parse(ParseError::InvalidInsertTargetCount {
                        expected: target_indexes.len(),
                        actual: row.len(),
                    }));
                }

                let mut values = vec![Value::Null; desc.columns.len()];
                for (raw, target_index) in row.iter().zip(target_indexes.iter().copied()) {
                    let column = &desc.columns[target_index];
                    let value = if raw == "\\N" {
                        Value::Null
                    } else {
                        match column.ty {
                            ScalarType::Int16 => {
                                raw.parse::<i16>().map(Value::Int16).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })?
                            }
                            ScalarType::Int32 => {
                                raw.parse::<i32>().map(Value::Int32).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })?
                            }
                            ScalarType::Int64 => {
                                raw.parse::<i64>().map(Value::Int64).map_err(|_| {
                                    ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                })?
                            }
                            ScalarType::Date
                            | ScalarType::Time
                            | ScalarType::TimeTz
                            | ScalarType::Timestamp
                            | ScalarType::TimestampTz
                            | ScalarType::Point
                            | ScalarType::Lseg
                            | ScalarType::Path
                            | ScalarType::Line
                            | ScalarType::Box
                            | ScalarType::Polygon
                            | ScalarType::Circle
                            | ScalarType::TsVector
                            | ScalarType::TsQuery => {
                                cast_value(Value::Text(raw.clone().into()), column.sql_type)?
                            }
                            ScalarType::BitString => {
                                cast_value(Value::Text(raw.clone().into()), column.sql_type)?
                            }
                            ScalarType::Float32 | ScalarType::Float64 => raw
                                .parse::<f64>()
                                .map(Value::Float64)
                                .map_err(|_| ExecError::TypeMismatch {
                                    op: "copy assignment",
                                    left: Value::Null,
                                    right: Value::Text(raw.clone().into()),
                                })?,
                            ScalarType::Numeric => Value::Numeric(raw.as_str().into()),
                            ScalarType::Json => Value::Json(raw.clone().into()),
                            ScalarType::Jsonb => Value::Jsonb(
                                crate::backend::executor::jsonb::parse_jsonb_text(raw)?,
                            ),
                            ScalarType::JsonPath => Value::JsonPath(
                                canonicalize_jsonpath(raw)
                                    .map_err(|_| ExecError::InvalidStorageValue {
                                        column: "<copy>".into(),
                                        details: format!(
                                            "invalid input syntax for type jsonpath: \"{raw}\""
                                        ),
                                    })?
                                    .into(),
                            ),
                            ScalarType::Bytea => Value::Bytea(parse_bytea_text(raw)?),
                            ScalarType::Text => Value::Text(raw.clone().into()),
                            ScalarType::Record => {
                                return Err(ExecError::UnsupportedStorageType {
                                    column: column.name.clone(),
                                    ty: column.ty.clone(),
                                    attlen: column.storage.attlen,
                                });
                            }
                            ScalarType::Bool => match raw.as_str() {
                                "t" | "true" | "1" => Value::Bool(true),
                                "f" | "false" | "0" => Value::Bool(false),
                                _ => {
                                    return Err(ExecError::TypeMismatch {
                                        op: "copy assignment",
                                        left: Value::Null,
                                        right: Value::Text(raw.clone().into()),
                                    });
                                }
                            },
                            ScalarType::Array(_) => {
                                parse_text_array_literal(raw, column.sql_type.element_type())?
                            }
                        }
                    };
                    values[target_index] = value;
                }

                Ok(values)
            })
            .collect::<Result<Vec<_>, ExecError>>();

        let result = parsed_rows.and_then(|parsed_rows| {
            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&db.pool),
                txns: db.txns.clone(),
                txn_waiter: Some(db.txn_waiter.clone()),
                snapshot,
                client_id: self.client_id,
                next_command_id: cid,
                timed: false,
                outer_rows: Vec::new(),
                subplans: Vec::new(),
            };
            crate::backend::commands::tablecmds::execute_insert_values(
                rel,
                toast,
                toast_index.as_ref(),
                &desc,
                &indexes,
                &parsed_rows,
                &mut ctx,
                xid,
                cid,
            )
        });

        let final_result = if started_txn {
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
        };
        final_result
    }

    fn execute_copy_from_file(
        &mut self,
        db: &Database,
        stmt: &CopyFromStatement,
    ) -> Result<StatementResult, ExecError> {
        let CopySource::File(path) = &stmt.source;
        let text = std::fs::read_to_string(path).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "readable COPY source file",
                actual: format!("{path}: {err}"),
            })
        })?;
        let rows = text
            .lines()
            .map(|line| line.trim_end_matches('\r'))
            .filter(|line| !line.is_empty() && *line != "\\.")
            .map(|line| {
                line.split('\t')
                    .map(|part| part.to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let count =
            self.copy_from_rows_into(db, &stmt.table_name, stmt.columns.as_deref(), &rows)?;
        Ok(StatementResult::AffectedRows(count))
    }
}

fn parse_copy_from_file(sql: &str) -> Option<(String, Option<Vec<String>>, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = "copy ";
    let from_kw = " from ";
    if !lower.starts_with(prefix) {
        return None;
    }
    let from_idx = lower.find(from_kw)?;
    let target = trimmed[prefix.len()..from_idx].trim();
    let source = trimmed[from_idx + from_kw.len()..].trim();
    if !(source.starts_with('\'') && source.ends_with('\'')) {
        return None;
    }
    let file_path = source[1..source.len() - 1].to_string();
    if let Some(open_paren) = target.find('(') {
        let close_paren = target.rfind(')')?;
        if close_paren < open_paren {
            return None;
        }
        let table = target[..open_paren].trim();
        let columns = target[open_paren + 1..close_paren]
            .split(',')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .map(|part| part.to_string())
            .collect::<Vec<_>>();
        if table.is_empty() || columns.is_empty() {
            return None;
        }
        Some((table.to_string(), Some(columns), file_path))
    } else if target.is_empty() {
        None
    } else {
        Some((target.to_string(), None, file_path))
    }
}

fn read_copy_from_file(file_path: &str) -> Result<Vec<Vec<String>>, ExecError> {
    let resolved = resolve_copy_file_path(file_path);
    let text = fs::read_to_string(&resolved).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "readable COPY source file",
            actual: format!("{file_path}: {err}"),
        })
    })?;
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split('\t').map(|field| field.to_string()).collect())
        .collect())
}

fn resolve_copy_file_path(file_path: &str) -> String {
    if std::path::Path::new(file_path).exists() {
        return file_path.to_string();
    }
    if let Some(stripped) = file_path.strip_prefix(':')
        && let Some((_, remainder)) = stripped.split_once('/')
        && let Some(root) = postgres_regress_root()
    {
        let candidate = root.join(remainder);
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    file_path.to_string()
}

fn postgres_regress_root() -> Option<std::path::PathBuf> {
    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        here.parent()?.join("postgres/src/test/regress"),
        here.join("../../postgres/src/test/regress"),
    ];
    candidates.into_iter().find(|path| path.exists())
}
