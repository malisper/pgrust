use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::TransactionId;
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::commands::copyfrom::parse_text_array_literal;
use crate::backend::commands::tablecmds::{
    execute_delete_with_waiter, execute_insert, execute_prepared_insert_row,
    execute_update_with_waiter,
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
use crate::backend::utils::misc::checkpoint::is_checkpoint_guc;
use crate::backend::utils::misc::guc::{is_postgres_guc, normalize_guc_name};
use crate::backend::utils::misc::guc_datetime::{
    DateTimeConfig, default_datestyle, default_timezone, format_datestyle, parse_datestyle,
    parse_timezone,
};
use crate::backend::utils::misc::interrupts::{InterruptState, StatementInterruptGuard};
use crate::include::catalog::PG_CHECKPOINT_OID;
use crate::include::nodes::execnodes::ScalarType;
use crate::pgrust::auth::AuthState;
use crate::pgrust::database::{
    Database, SequenceMutationEffect, TempMutationEffect, alter_table_add_constraint_lock_requests,
    alter_table_validate_constraint_lock_requests, delete_foreign_key_lock_requests,
    insert_foreign_key_lock_requests, prepared_insert_foreign_key_lock_requests,
    reject_relation_with_referencing_foreign_keys, relation_foreign_key_lock_requests,
    update_foreign_key_lock_requests,
};
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
    pub(crate) interrupt_guard: Option<StatementInterruptGuard>,
}

impl Drop for SelectGuard<'_> {
    fn drop(&mut self) {
        unlock_relations(self.table_locks, self.client_id, &self.rels);
    }
}

struct ActiveTransaction {
    xid: TransactionId,
    failed: bool,
    held_table_locks: BTreeMap<RelFileLocator, TableLockMode>,
    next_command_id: u32,
    catalog_effects: Vec<CatalogMutationEffect>,
    current_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    prior_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    temp_effects: Vec<TempMutationEffect>,
    sequence_effects: Vec<SequenceMutationEffect>,
}

pub struct Session {
    pub client_id: ClientId,
    active_txn: Option<ActiveTransaction>,
    gucs: HashMap<String, String>,
    datetime_config: DateTimeConfig,
    interrupts: Arc<InterruptState>,
    auth: AuthState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}

fn default_stats_guc_value(name: &str) -> Option<&'static str> {
    match name {
        "track_counts" => Some("on"),
        "track_functions" => Some("none"),
        "stats_fetch_consistency" => Some("cache"),
        _ => None,
    }
}

impl Session {
    const DEFAULT_MAINTENANCE_WORK_MEM_KB: usize = 65_536;

    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            active_txn: None,
            gucs: HashMap::new(),
            datetime_config: DateTimeConfig::default(),
            interrupts: Arc::new(InterruptState::new()),
            auth: AuthState::default(),
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

    pub fn datetime_config(&self) -> &DateTimeConfig {
        &self.datetime_config
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

    pub fn session_user_oid(&self) -> u32 {
        self.auth.session_user_oid()
    }

    pub fn current_user_oid(&self) -> u32 {
        self.auth.current_user_oid()
    }

    pub(crate) fn auth_state(&self) -> &AuthState {
        &self.auth
    }

    pub(crate) fn set_session_authorization_oid(&mut self, role_oid: u32) {
        self.auth.assume_authenticated_user(role_oid);
    }

    pub(crate) fn reset_session_authorization(&mut self) {
        self.auth.reset_session_authorization();
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
        let _interrupt_guard = self.statement_interrupt_guard()?;
        db.install_auth_state(self.client_id, self.auth.clone());
        self.execute_internal(db, sql)
    }

    fn execute_internal(&mut self, db: &Database, sql: &str) -> Result<StatementResult, ExecError> {
        db.install_interrupt_state(self.client_id, self.interrupts());
        if self.active_txn.is_none() {
            db.accept_invalidation_messages(self.client_id);
        }
        // :HACK: Support simple file-backed COPY FROM on the normal SQL path
        // until COPY is modeled as a real parsed/bound statement.
        if let Some((table_name, columns, file_path)) = parse_copy_from_file(sql) {
            let rows = read_copy_from_file(&file_path)?;
            let inserted =
                self.copy_from_rows_into_internal(db, &table_name, columns.as_deref(), &rows)?;
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
            Statement::Show(ref show_stmt) => self.apply_show(db, show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(db, set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(db, reset_stmt),
            Statement::Checkpoint(_) => self.apply_checkpoint(db),
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CreateFunction(ref create_stmt) => {
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
                    db.execute_create_function_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateDatabase(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_database_stmt(self.client_id, create_stmt)
                }
            }
            Statement::CreateSchema(ref create_stmt) => {
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
                    db.execute_create_schema_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateTablespace(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_tablespace_stmt(self.client_id, create_stmt)
                }
            }
            Statement::CreateDomain(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_domain_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateConversion(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_conversion_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateIndex(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_index_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                    self.maintenance_work_mem_kb()?,
                )
            }
            Statement::AlterTableOwner(ref alter_stmt) => {
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
                    db.execute_alter_table_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRename(ref rename_stmt) => {
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
                    db.execute_alter_table_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewOwner(ref alter_stmt) => {
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
                    db.execute_alter_view_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => {
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
                    db.execute_alter_schema_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameColumn(ref rename_stmt) => {
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
                    db.execute_alter_table_rename_column_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
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
            Statement::AlterTableDropColumn(ref drop_stmt) => {
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
                    db.execute_alter_table_drop_column_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnType(ref alter_stmt) => {
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
                    db.execute_alter_table_alter_column_type_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAddConstraint(ref alter_stmt) => {
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
                    db.execute_alter_table_add_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableDropConstraint(ref alter_stmt) => {
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
                    db.execute_alter_table_drop_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameConstraint(ref alter_stmt) => {
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
                    db.execute_alter_table_rename_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetNotNull(ref alter_stmt) => {
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
                    db.execute_alter_table_set_not_null_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableDropNotNull(ref alter_stmt) => {
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
                    db.execute_alter_table_drop_not_null_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableValidateConstraint(ref alter_stmt) => {
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
                    db.execute_alter_table_validate_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateRole(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_role_stmt(
                        self.client_id,
                        create_stmt,
                        self.gucs.get("createrole_self_grant").map(String::as_str),
                    )
                }
            }
            Statement::AlterRole(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_alter_role_stmt(self.client_id, alter_stmt)
                }
            }
            Statement::DropRole(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_drop_role_stmt(self.client_id, drop_stmt)
                }
            }
            Statement::DropDatabase(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_drop_database_stmt(self.client_id, drop_stmt)
                }
            }
            Statement::GrantObject(ref grant_stmt) => {
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
                    db.execute_grant_object_stmt_with_search_path(
                        self.client_id,
                        grant_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::RevokeObject(ref revoke_stmt) => {
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
                    db.execute_revoke_object_stmt_with_search_path(
                        self.client_id,
                        revoke_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::GrantRoleMembership(ref grant_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_grant_role_membership_stmt(self.client_id, grant_stmt)
                }
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_revoke_role_membership_stmt(self.client_id, revoke_stmt)
                }
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth =
                        db.execute_set_session_authorization_stmt(self.client_id, set_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth =
                        db.execute_reset_session_authorization_stmt(self.client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
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
            Statement::CommentOnRole(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_comment_on_role_stmt(self.client_id, comment_stmt)
                }
            }
            Statement::CommentOnConversion(ref comment_stmt) => {
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
                    db.execute_comment_on_conversion_stmt_with_search_path(
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
                    held_table_locks: BTreeMap::new(),
                    next_command_id: 0,
                    catalog_effects: Vec::new(),
                    current_cmd_catalog_invalidations: Vec::new(),
                    prior_cmd_catalog_invalidations: Vec::new(),
                    temp_effects: Vec::new(),
                    sequence_effects: Vec::new(),
                });
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Commit => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
                let result = (|| {
                    let _checkpoint_guard = db.checkpoint_commit_guard();
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
                })();
                for rel in held_locks {
                    db.table_locks.unlock_table(rel, self.client_id);
                }
                db.finalize_committed_sequence_effects(&txn.sequence_effects)?;
                result
            }
            Statement::Rollback => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
                let result = {
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
                };
                for rel in held_locks {
                    db.table_locks.unlock_table(rel, self.client_id);
                }
                db.finalize_aborted_sequence_effects(&txn.sequence_effects);
                result
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
                    db.execute_statement_with_search_path_and_datetime_config(
                        self.client_id,
                        stmt,
                        search_path.as_deref(),
                        &self.datetime_config,
                    )
                }
            }
        }
    }

    fn statement_timeout_duration(&self) -> Result<Option<Duration>, ExecError> {
        let Some(value) = self.gucs.get("statement_timeout") else {
            return Ok(None);
        };
        parse_statement_timeout(value)
    }

    fn statement_interrupt_guard(&self) -> Result<StatementInterruptGuard, ExecError> {
        Ok(self
            .interrupts
            .statement_interrupt_guard(self.statement_timeout_duration()?))
    }

    pub(crate) fn apply_startup_parameters(
        &mut self,
        params: &HashMap<String, String>,
    ) -> Result<(), ExecError> {
        if let Some(options) = params.get("options") {
            for (name, value) in parse_startup_options(options)? {
                self.apply_guc_value(&name, &value)?;
            }
        }
        for (name, value) in params {
            if name.eq_ignore_ascii_case("options") {
                continue;
            }
            let normalized = normalize_guc_name(name);
            if is_postgres_guc(&normalized) {
                self.apply_guc_value(name, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn interrupts(&self) -> Arc<InterruptState> {
        Arc::clone(&self.interrupts)
    }

    pub(crate) fn cleanup_on_disconnect(&mut self, db: &Database) {
        if let Some(txn) = self.active_txn.take() {
            let _ = db.txns.write().abort(txn.xid);
            db.finalize_aborted_local_catalog_invalidations(
                self.client_id,
                &txn.prior_cmd_catalog_invalidations,
                &txn.current_cmd_catalog_invalidations,
            );
            db.finalize_aborted_catalog_effects(&txn.catalog_effects);
            db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
            db.finalize_aborted_sequence_effects(&txn.sequence_effects);
            db.txn_waiter.notify();
            for rel in txn.held_table_locks.keys().copied() {
                db.table_locks.unlock_table(rel, self.client_id);
            }
        }

        // :HACK: Session-scoped table locks are currently tracked partly on the
        // session and partly in the global table lock manager. Release anything
        // still associated with this backend on disconnect, mirroring PostgreSQL
        // backend-exit lock cleanup even if the session missed normal unwind.
        db.table_locks.unlock_all_for_client(self.client_id);
    }

    fn lock_table_if_needed(
        &mut self,
        db: &Database,
        rel: RelFileLocator,
        mode: TableLockMode,
    ) -> Result<(), ExecError> {
        let Some(txn) = self.active_txn.as_mut() else {
            db.table_locks.lock_table_interruptible(
                rel,
                mode,
                self.client_id,
                self.interrupts.as_ref(),
            )?;
            return Ok(());
        };
        if txn
            .held_table_locks
            .get(&rel)
            .is_some_and(|existing| existing.strongest(mode) == *existing)
        {
            return Ok(());
        }
        db.table_locks.lock_table_interruptible(
            rel,
            mode,
            self.client_id,
            self.interrupts.as_ref(),
        )?;
        txn.held_table_locks
            .entry(rel)
            .and_modify(|existing| *existing = existing.strongest(mode))
            .or_insert(mode);
        Ok(())
    }

    fn lock_table_requests_if_needed(
        &mut self,
        db: &Database,
        requests: &[(RelFileLocator, TableLockMode)],
    ) -> Result<(), ExecError> {
        for (rel, mode) in requests {
            self.lock_table_if_needed(db, *rel, *mode)?;
        }
        Ok(())
    }

    pub fn execute_streaming<'a>(
        &mut self,
        db: &'a Database,
        select_stmt: &SelectStatement,
    ) -> Result<SelectGuard<'a>, ExecError> {
        let interrupt_guard = self.statement_interrupt_guard()?;
        db.install_auth_state(self.client_id, self.auth.clone());
        db.install_interrupt_state(self.client_id, self.interrupts());
        let txn_ctx = if let Some(ref mut txn) = self.active_txn {
            let xid = txn.xid;
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            Some((xid, cid))
        } else {
            None
        };
        let search_path = self.configured_search_path();
        let mut guard = db.execute_streaming_with_search_path_and_datetime_config(
            self.client_id,
            select_stmt,
            txn_ctx,
            search_path.as_deref(),
            &self.datetime_config,
        )?;
        guard.interrupt_guard = Some(interrupt_guard);
        Ok(guard)
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
            Statement::Show(ref show_stmt) => self.apply_show(db, show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(db, set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(db, reset_stmt),
            Statement::Checkpoint(_) => self.apply_checkpoint(db),
            Statement::CommentOnDomain(ref comment_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_comment_on_domain_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CommentOnConversion(ref comment_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_comment_on_conversion_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CreateDomain(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_domain_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateConversion(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_conversion_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
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
            Statement::AlterTableOwner(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.relation_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.relation_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_owner_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableRename(ref rename_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_relation(&rename_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            rename_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_rename_stmt_in_transaction_with_search_path(
                    client_id,
                    rename_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
            }
            Statement::AlterViewOwner(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.relation_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.relation_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_view_owner_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterSequence(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.sequence_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.sequence_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.sequence_effects,
                )
            }
            Statement::AlterSequenceOwner(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.relation_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.relation_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_sequence_owner_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterSequenceRename(ref rename_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&rename_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            rename_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_sequence_rename_stmt_in_transaction_with_search_path(
                    client_id,
                    rename_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
            }
            Statement::AlterTableRenameColumn(ref rename_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&rename_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            rename_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_rename_column_stmt_in_transaction_with_search_path(
                    client_id,
                    rename_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
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
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                    &mut txn.sequence_effects,
                )
            }
            Statement::AlterTableDropColumn(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&drop_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            drop_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAlterColumnType(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAddConstraint(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                let lock_requests =
                    alter_table_add_constraint_lock_requests(&relation, alter_stmt, &catalog)?;
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableDropConstraint(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableRenameConstraint(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_rename_constraint_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableSetNotNull(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableDropNotNull(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableValidateConstraint(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                let lock_requests =
                    alter_table_validate_constraint_lock_requests(&relation, alter_stmt, &catalog)?;
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CreateRole(ref create_stmt) => db.execute_create_role_stmt(
                client_id,
                create_stmt,
                self.gucs.get("createrole_self_grant").map(String::as_str),
            ),
            Statement::CreateDatabase(_) => Err(ExecError::Parse(
                ParseError::ActiveSqlTransaction("CREATE DATABASE"),
            )),
            Statement::AlterRole(ref alter_stmt) => {
                db.execute_alter_role_stmt(client_id, alter_stmt)
            }
            Statement::DropRole(ref drop_stmt) => db.execute_drop_role_stmt(client_id, drop_stmt),
            Statement::DropDatabase(_) => Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                "DROP DATABASE",
            ))),
            Statement::GrantObject(ref grant_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_grant_object_stmt_with_search_path(
                    client_id,
                    grant_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::RevokeObject(ref revoke_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_revoke_object_stmt_with_search_path(
                    client_id,
                    revoke_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::GrantRoleMembership(ref grant_stmt) => {
                db.execute_grant_role_membership_stmt(client_id, grant_stmt)
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                db.execute_revoke_role_membership_stmt(client_id, revoke_stmt)
            }
            Statement::ReassignOwned(ref reassign_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_reassign_owned_stmt_in_transaction(
                    client_id,
                    reassign_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_role_stmt_in_transaction(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropConversion(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_conversion_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                self.auth = db.execute_set_session_authorization_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                self.auth = db.execute_reset_session_authorization_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Unsupported(ref unsupported_stmt) => {
                Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                    "{}: {}",
                    unsupported_stmt.feature, unsupported_stmt.sql
                ))))
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_schema_owner_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
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
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
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
                    sequences: Some(db.sequences.clone()),
                    checkpoint_stats: db.checkpoint_stats_snapshot(),
                    datetime_config: self.datetime_config.clone(),
                    interrupts: self.interrupts(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    allow_side_effects: true,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                execute_readonly_statement(stmt, &catalog, &mut ctx)
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let lock_requests = insert_foreign_key_lock_requests(&bound);
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let interrupts = self.interrupts();
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    txn_waiter: Some(db.txn_waiter.clone()),
                    sequences: Some(db.sequences.clone()),
                    checkpoint_stats: db.checkpoint_stats_snapshot(),
                    datetime_config: self.datetime_config.clone(),
                    interrupts,
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    allow_side_effects: true,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                execute_insert(bound, &catalog, &mut ctx, xid, cid)
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_update(update_stmt, &catalog)?;
                let lock_requests = update_foreign_key_lock_requests(&bound);
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let interrupts = self.interrupts();
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    txn_waiter: Some(db.txn_waiter.clone()),
                    sequences: Some(db.sequences.clone()),
                    checkpoint_stats: db.checkpoint_stats_snapshot(),
                    datetime_config: self.datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    allow_side_effects: true,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                execute_update_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    cid,
                    Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                )
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let lock_requests = delete_foreign_key_lock_requests(&bound);
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let interrupts = self.interrupts();
                let mut ctx = ExecutorContext {
                    pool: Arc::clone(&db.pool),
                    txns: db.txns.clone(),
                    txn_waiter: Some(db.txn_waiter.clone()),
                    sequences: Some(db.sequences.clone()),
                    checkpoint_stats: db.checkpoint_stats_snapshot(),
                    datetime_config: self.datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                    timed: false,
                    allow_side_effects: true,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                execute_delete_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                )
            }
            Statement::CreateFunction(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_function_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CreateSchema(ref create_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::CreateSequence(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                    &mut txn.sequence_effects,
                )
            }
            Statement::CreateTablespace(ref create_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_tablespace_stmt_in_transaction(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
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
                    &mut txn.sequence_effects,
                )
            }
            Statement::CreateType(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_type_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropDomain(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_domain_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    search_path.as_deref(),
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
            Statement::DropType(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_type_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
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
                    self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
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
            Statement::DropIndex(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let rels = {
                    drop_stmt
                        .index_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_index_stmt_in_transaction_with_search_path(
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
                    self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
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
            Statement::DropSchema(ref drop_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropSequence(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let rels = {
                    drop_stmt
                        .sequence_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|entry| entry.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                    &mut txn.sequence_effects,
                )
            }
            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relations = {
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name))
                        .collect::<Vec<_>>()
                };
                for relation in &relations {
                    reject_relation_with_referencing_foreign_keys(
                        &catalog,
                        relation.relation_oid,
                        "TRUNCATE on table without referencing foreign keys",
                    )?;
                }
                for relation in relations {
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_truncate_table_in_transaction_with_search_path(
                    client_id,
                    truncate_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
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
        _db: &Database,
        stmt: &crate::backend::parser::SetStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = normalize_guc_name(&stmt.name);
        if !is_postgres_guc(&name) {
            return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                name,
            )));
        }
        if is_checkpoint_guc(&name) {
            return Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name)));
        }
        self.apply_guc_value(&stmt.name, &stmt.value)?;
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_reset(
        &mut self,
        _db: &Database,
        stmt: &crate::backend::parser::ResetStatement,
    ) -> Result<StatementResult, ExecError> {
        if let Some(name) = &stmt.name {
            let normalized = normalize_guc_name(name);
            if !is_postgres_guc(&normalized) {
                return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                    normalized,
                )));
            }
            if is_checkpoint_guc(&normalized) {
                return Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(
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
        db: &Database,
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

        let fallback_value = || -> String {
            match name.as_str() {
                "datestyle" => default_datestyle().to_string(),
                "timezone" => default_timezone().to_string(),
                _ => default_stats_guc_value(&name)
                    .map(str::to_string)
                    .unwrap_or_else(|| "default".to_string()),
            }
        };

        let (column_name, value) = match name.as_str() {
            "datestyle" => (
                "DateStyle".to_string(),
                format_datestyle(&self.datetime_config),
            ),
            "timezone" => (
                "TimeZone".to_string(),
                self.datetime_config.time_zone.clone(),
            ),
            _ if is_checkpoint_guc(&name) => (
                stmt.name.clone(),
                db.checkpoint_config_value(&name)
                    .unwrap_or_else(|| "default".to_string()),
            ),
            _ => (
                stmt.name.clone(),
                self.gucs
                    .get(&name)
                    .cloned()
                    .unwrap_or_else(fallback_value),
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

    fn apply_checkpoint(&mut self, db: &Database) -> Result<StatementResult, ExecError> {
        if self.active_txn.as_ref().is_some_and(|txn| txn.failed) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ROLLBACK",
                actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
            }));
        }
        let auth_catalog = db.auth_catalog(self.client_id, None).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "authorization catalog",
                actual: format!("{err:?}"),
            })
        })?;
        if !self
            .auth
            .has_effective_membership(PG_CHECKPOINT_OID, &auth_catalog)
        {
            return Err(ExecError::DetailedError {
                message: "permission denied to execute CHECKPOINT command".into(),
                detail: Some(
                    "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
                        .into(),
                ),
                hint: None,
                sqlstate: "42501",
            });
        }
        db.request_checkpoint(crate::backend::access::transam::CheckpointRequestFlags::sql())?;
        Ok(StatementResult::AffectedRows(0))
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

    fn apply_guc_value(&mut self, name: &str, value: &str) -> Result<(), ExecError> {
        let normalized = normalize_guc_name(name);
        if !is_postgres_guc(&normalized) {
            return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                normalized,
            )));
        }
        match normalized.as_str() {
            "datestyle" => {
                let Some((date_style_format, date_order)) = parse_datestyle(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.datetime_config.date_style_format = date_style_format;
                self.datetime_config.date_order = date_order;
            }
            "timezone" => {
                let Some(time_zone) = parse_timezone(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.datetime_config.time_zone = time_zone;
            }
            "statement_timeout" => {
                parse_statement_timeout(value)?;
            }
            "max_stack_depth" => {
                self.datetime_config.max_stack_depth_kb = parse_max_stack_depth(value)?;
            }
            _ => {}
        }
        self.gucs.insert(normalized, value.to_string());
        Ok(())
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

        let lock_requests = prepared_insert_foreign_key_lock_requests(prepared);
        self.lock_table_requests_if_needed(db, &lock_requests)?;

        let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
        let interrupts = self.interrupts();
        let catalog = self.catalog_lookup_for_command(db, xid, cid);
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&db.pool),
            txns: db.txns.clone(),
            txn_waiter: Some(db.txn_waiter.clone()),
            sequences: Some(db.sequences.clone()),
            checkpoint_stats: db.checkpoint_stats_snapshot(),
            datetime_config: self.datetime_config.clone(),
            interrupts,
            snapshot,
            client_id,
            next_command_id: cid,
            timed: false,
            allow_side_effects: true,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
        };
        execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid)
    }

    pub fn copy_from_rows(
        &mut self,
        db: &Database,
        table_name: &str,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        self.copy_from_rows_into_internal(db, table_name, None, rows)
    }

    pub fn copy_from_rows_into(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        self.copy_from_rows_into_internal(db, table_name, target_columns, rows)
    }

    fn copy_from_rows_into_internal(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        db.install_interrupt_state(self.client_id, self.interrupts());
        let started_txn = if self.active_txn.is_none() {
            let xid = db.txns.write().begin();
            self.active_txn = Some(ActiveTransaction {
                xid,
                failed: false,
                held_table_locks: BTreeMap::new(),
                next_command_id: 0,
                catalog_effects: Vec::new(),
                current_cmd_catalog_invalidations: Vec::new(),
                prior_cmd_catalog_invalidations: Vec::new(),
                temp_effects: Vec::new(),
                sequence_effects: Vec::new(),
            });
            true
        } else {
            false
        };

        let result = (|| -> Result<usize, ExecError> {
            let (xid, cid) = {
                let txn = self.active_txn.as_mut().unwrap();
                let xid = txn.xid;
                let cid = txn.next_command_id;
                txn.next_command_id = txn.next_command_id.saturating_add(1);
                (xid, cid)
            };

            let catalog = self.catalog_lookup_for_command(db, xid, cid);
            let (relation_oid, rel, toast, toast_index, desc, indexes) = {
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
                    entry.relation_oid,
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

            let relation_constraints = crate::backend::parser::bind_relation_constraints(
                None,
                relation_oid,
                &desc,
                &catalog,
            )?;
            let lock_requests = relation_foreign_key_lock_requests(rel, &relation_constraints);
            self.lock_table_requests_if_needed(db, &lock_requests)?;

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
                                ScalarType::Money => {
                                    crate::backend::executor::money_parse_text(raw)
                                        .map(Value::Money)?
                                }
                                ScalarType::Date
                                | ScalarType::Time
                                | ScalarType::TimeTz
                                | ScalarType::Timestamp
                                | ScalarType::TimestampTz
                                | ScalarType::Range(_)
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
                .collect::<Result<Vec<_>, ExecError>>()?;

            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            let interrupts = self.interrupts();
            let catalog = self.catalog_lookup_for_command(db, xid, cid);
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&db.pool),
                txns: db.txns.clone(),
                txn_waiter: Some(db.txn_waiter.clone()),
                sequences: Some(db.sequences.clone()),
                checkpoint_stats: db.checkpoint_stats_snapshot(),
                datetime_config: self.datetime_config.clone(),
                interrupts,
                snapshot,
                client_id: self.client_id,
                next_command_id: cid,
                timed: false,
                allow_side_effects: true,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                catalog: catalog.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
            };
            crate::backend::commands::tablecmds::execute_insert_values(
                table_name,
                rel,
                toast,
                toast_index.as_ref(),
                &desc,
                &relation_constraints,
                &indexes,
                &parsed_rows,
                &mut ctx,
                xid,
                cid,
            )
        })();

        let final_result = if started_txn {
            let txn = self.active_txn.take().unwrap();
            let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
            match result {
                Ok(n) => {
                    let _checkpoint_guard = db.checkpoint_commit_guard();
                    let commit_result = db.pool.write_wal_commit(txn.xid).map_err(|e| {
                        ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                            crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            )),
                        ))
                    });
                    let commit_result = commit_result.and_then(|_| {
                        db.pool.flush_wal().map_err(|e| {
                            ExecError::Heap(
                                crate::backend::access::heap::heapam::HeapError::Storage(
                                    crate::backend::storage::smgr::SmgrError::Io(
                                        std::io::Error::new(std::io::ErrorKind::Other, e),
                                    ),
                                ),
                            )
                        })
                    });
                    let commit_result = commit_result.and_then(|_| {
                        db.txns.write().commit(txn.xid).map_err(|e| {
                            ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(
                                e,
                            ))
                        })
                    });
                    let commit_result =
                        commit_result.and_then(|_| db.apply_temp_on_commit(self.client_id));
                    db.txn_waiter.notify();
                    for rel in held_locks {
                        db.table_locks.unlock_table(rel, self.client_id);
                    }
                    commit_result?;
                    Ok(n)
                }
                Err(e) => {
                    let _ = db.txns.write().abort(txn.xid);
                    db.txn_waiter.notify();
                    for rel in held_locks {
                        db.table_locks.unlock_table(rel, self.client_id);
                    }
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
        let count = self.copy_from_rows_into_internal(
            db,
            &stmt.table_name,
            stmt.columns.as_deref(),
            &rows,
        )?;
        Ok(StatementResult::AffectedRows(count))
    }
}

fn parse_statement_timeout(value: &str) -> Result<Option<Duration>, ExecError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    let amount = number
        .parse::<f64>()
        .map_err(|_| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    if !amount.is_finite() || amount < 0.0 {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    if amount == 0.0 {
        return Ok(None);
    }

    let multiplier_ms = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
        "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
        "d" | "day" | "days" => 86_400_000.0,
        _ => {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
    };
    let millis = amount * multiplier_ms;
    if !millis.is_finite() || millis > u64::MAX as f64 {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    Ok(Some(Duration::from_millis(millis.ceil() as u64)))
}

fn parse_max_stack_depth(value: &str) -> Result<u32, ExecError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let amount = number
        .parse::<u32>()
        .map_err(|_| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    let multiplier_kb = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "kb" => 1_u32,
        "mb" => 1024_u32,
        _ => {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
    };
    amount
        .checked_mul(multiplier_kb)
        .ok_or_else(|| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))
}

fn parse_startup_options(options: &str) -> Result<Vec<(String, String)>, ExecError> {
    let tokens = split_startup_option_words(options)?;
    let mut gucs = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        let token = &tokens[index];
        let assignment = if token == "-c" {
            index += 1;
            tokens.get(index).ok_or_else(|| {
                ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
            })?
        } else if let Some(assignment) = token.strip_prefix("-c") {
            assignment
        } else if let Some(assignment) = token.strip_prefix("--") {
            assignment
        } else {
            index += 1;
            continue;
        };
        let (name, value) = assignment.split_once('=').ok_or_else(|| {
            ExecError::Parse(ParseError::UnrecognizedParameter(assignment.to_string()))
        })?;
        gucs.push((name.to_string(), value.to_string()));
        index += 1;
    }
    Ok(gucs)
}

fn split_startup_option_words(options: &str) -> Result<Vec<String>, ExecError> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut chars = options.chars().peekable();
    let mut quote = None::<char>;

    while let Some(ch) = chars.next() {
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) if ch == '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
                })?;
                current.push(escaped);
            }
            Some(_) => current.push(ch),
            None if ch.is_ascii_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
                })?;
                current.push(escaped);
            }
            None => current.push(ch),
        }
    }

    if quote.is_some() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            options.to_string(),
        )));
    }
    if !current.is_empty() {
        words.push(current);
    }
    Ok(words)
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

#[cfg(test)]
mod tests {
    use super::{parse_max_stack_depth, parse_startup_options, parse_statement_timeout};
    use crate::backend::executor::ExecError;
    use crate::backend::parser::ParseError;
    use std::time::Duration;

    #[test]
    fn parse_statement_timeout_accepts_postgres_units() {
        assert_eq!(parse_statement_timeout("0").unwrap(), None);
        assert_eq!(
            parse_statement_timeout("15").unwrap(),
            Some(Duration::from_millis(15))
        );
        assert_eq!(
            parse_statement_timeout("1.5s").unwrap(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_statement_timeout("2 min").unwrap(),
            Some(Duration::from_millis(120_000))
        );
        assert_eq!(
            parse_statement_timeout("1h").unwrap(),
            Some(Duration::from_millis(3_600_000))
        );
        assert_eq!(
            parse_statement_timeout("1d").unwrap(),
            Some(Duration::from_millis(86_400_000))
        );
    }

    #[test]
    fn parse_statement_timeout_rejects_invalid_values() {
        for value in ["", "-1", "abc", "10fortnights"] {
            assert!(matches!(
                parse_statement_timeout(value),
                Err(ExecError::Parse(ParseError::UnrecognizedParameter(_)))
            ));
        }
    }

    #[test]
    fn parse_max_stack_depth_accepts_postgres_units() {
        assert_eq!(parse_max_stack_depth("100").unwrap(), 100);
        assert_eq!(parse_max_stack_depth("100kB").unwrap(), 100);
        assert_eq!(parse_max_stack_depth("2MB").unwrap(), 2048);
    }

    #[test]
    fn parse_max_stack_depth_rejects_invalid_values() {
        for value in ["", "-1", "abc", "1GB"] {
            assert!(matches!(
                parse_max_stack_depth(value),
                Err(ExecError::Parse(ParseError::UnrecognizedParameter(_)))
            ));
        }
    }

    #[test]
    fn parse_startup_options_extracts_gucs() {
        assert_eq!(
            parse_startup_options("-c statement_timeout=5s --DateStyle='SQL, DMY'").unwrap(),
            vec![
                ("statement_timeout".to_string(), "5s".to_string()),
                ("DateStyle".to_string(), "SQL, DMY".to_string()),
            ]
        );
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
