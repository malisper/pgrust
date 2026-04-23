use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{CommandId, INVALID_TRANSACTION_ID, TransactionId};
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::commands::copyfrom::parse_text_array_literal;
use crate::backend::commands::tablecmds::{execute_merge, execute_prepared_insert_row};
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::executor::{
    DeferredForeignKeyTracker, ExecError, ExecutorContext, ExecutorTransactionState,
    StatementResult, Value, cast_value, execute_readonly_statement, parse_bytea_text,
};
use crate::backend::parser::{
    CatalogLookup, CopyFromStatement, CopySource, ParseError, ParseOptions, PreparedInsert,
    SelectStatement, Statement, bind_delete, bind_insert, bind_insert_prepared, bind_update,
    plan_merge,
};
use crate::backend::rewrite::relation_has_row_security;
use crate::backend::storage::lmgr::{TableLockManager, TableLockMode, unlock_relations};
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::checkpoint::is_checkpoint_guc;
use crate::backend::utils::misc::guc::{is_postgres_guc, normalize_guc_name};
use crate::backend::utils::misc::guc_datetime::{
    DateTimeConfig, default_datestyle, default_timezone, format_datestyle, parse_datestyle,
    parse_timezone,
};
use crate::backend::utils::misc::guc_xml::{
    format_xmlbinary, format_xmloption, parse_xmlbinary, parse_xmloption,
};
use crate::backend::utils::misc::interrupts::{InterruptState, StatementInterruptGuard};
use crate::include::catalog::PG_CHECKPOINT_OID;
use crate::include::nodes::execnodes::ScalarType;
use crate::pgrust::auth::AuthState;
use crate::pgrust::database::{
    AsyncListenAction, AsyncListenOp, Database, PendingNotification, SequenceMutationEffect,
    SessionStatsState, StatsFetchConsistency, TempMutationEffect, TrackFunctionsSetting,
    alter_table_add_constraint_lock_requests, alter_table_validate_constraint_lock_requests,
    delete_foreign_key_lock_requests, insert_foreign_key_lock_requests,
    merge_pending_notifications, merge_table_lock_requests,
    prepared_insert_foreign_key_lock_requests, queue_pending_notification,
    reject_relation_with_referencing_foreign_keys, relation_foreign_key_lock_requests,
    update_foreign_key_lock_requests, validate_deferred_foreign_key_constraints,
};
use crate::pl::plpgsql::execute_do;
use crate::{ClientId, RelFileLocator};
use parking_lot::RwLock;

pub struct SelectGuard<'a> {
    pub state: crate::include::nodes::execnodes::PlanState,
    pub ctx: ExecutorContext,
    pub columns: Vec<crate::backend::executor::QueryColumn>,
    pub column_names: Vec<String>,
    pub(crate) rels: Vec<RelFileLocator>,
    pub(crate) table_locks: &'a TableLockManager,
    pub(crate) client_id: ClientId,
    pub(crate) advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
    pub(crate) row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
    pub(crate) statement_lock_scope_id: Option<u64>,
    pub(crate) interrupt_guard: Option<StatementInterruptGuard>,
}

impl Drop for SelectGuard<'_> {
    fn drop(&mut self) {
        unlock_relations(self.table_locks, self.client_id, &self.rels);
        if let Some(scope_id) = self.statement_lock_scope_id {
            self.advisory_locks
                .unlock_all_statement(self.client_id, scope_id);
            self.row_locks
                .unlock_all_statement(self.client_id, scope_id);
        }
    }
}

struct StatementLockScopeGuard {
    advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
    row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
    client_id: ClientId,
    scope_id: Option<u64>,
}

impl StatementLockScopeGuard {
    fn new(
        advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
        row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
        client_id: ClientId,
        scope_id: Option<u64>,
    ) -> Self {
        Self {
            advisory_locks,
            row_locks,
            client_id,
            scope_id,
        }
    }

    fn scope_id(&self) -> Option<u64> {
        self.scope_id
    }
}

impl Drop for StatementLockScopeGuard {
    fn drop(&mut self) {
        if let Some(scope_id) = self.scope_id {
            self.advisory_locks
                .unlock_all_statement(self.client_id, scope_id);
            self.row_locks
                .unlock_all_statement(self.client_id, scope_id);
        }
    }
}

struct ActiveTransaction {
    xid: Option<TransactionId>,
    advisory_scope_id: u64,
    failed: bool,
    auth_at_start: AuthState,
    held_table_locks: BTreeMap<RelFileLocator, TableLockMode>,
    next_command_id: u32,
    catalog_effects: Vec<CatalogMutationEffect>,
    current_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    prior_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    temp_effects: Vec<TempMutationEffect>,
    sequence_effects: Vec<SequenceMutationEffect>,
    deferred_foreign_keys: DeferredForeignKeyTracker,
    async_listen_ops: Vec<AsyncListenOp>,
    pending_async_notifications: Vec<PendingNotification>,
}

pub struct Session {
    pub client_id: ClientId,
    pub(crate) temp_backend_id: crate::pgrust::database::TempBackendId,
    active_txn: Option<ActiveTransaction>,
    gucs: HashMap<String, String>,
    datetime_config: DateTimeConfig,
    interrupts: Arc<InterruptState>,
    auth: AuthState,
    stats_state: Arc<RwLock<SessionStatsState>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}

fn default_stats_guc_value(name: &str) -> Option<&'static str> {
    match name {
        "default_toast_compression" => Some("pglz"),
        "track_counts" => Some("on"),
        "track_functions" => Some("none"),
        "stats_fetch_consistency" => Some("cache"),
        _ => None,
    }
}

fn available_default_toast_compression_values() -> &'static str {
    #[cfg(feature = "lz4")]
    {
        "pglz, lz4"
    }
    #[cfg(not(feature = "lz4"))]
    {
        "pglz"
    }
}

fn invalid_default_toast_compression_value(value: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid value for parameter \"default_toast_compression\": \"{value}\""),
        detail: None,
        hint: Some(format!(
            "Available values: {}.",
            available_default_toast_compression_values()
        )),
        sqlstate: "22023",
    }
}

fn parse_default_toast_compression_guc_value(value: &str) -> Result<&'static str, ExecError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pglz" => Ok("pglz"),
        #[cfg(feature = "lz4")]
        "lz4" => Ok("lz4"),
        _ => Err(invalid_default_toast_compression_value(value)),
    }
}

impl Session {
    const DEFAULT_MAINTENANCE_WORK_MEM_KB: usize = 65_536;

    pub fn new(client_id: ClientId) -> Self {
        Self::with_temp_backend_id(client_id, client_id)
    }

    pub fn with_temp_backend_id(
        client_id: ClientId,
        temp_backend_id: crate::pgrust::database::TempBackendId,
    ) -> Self {
        Self {
            client_id,
            temp_backend_id,
            active_txn: None,
            gucs: HashMap::new(),
            datetime_config: DateTimeConfig::default(),
            interrupts: Arc::new(InterruptState::new()),
            auth: AuthState::default(),
            stats_state: Arc::new(RwLock::new(SessionStatsState::default())),
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

    pub fn allow_in_place_tablespaces(&self) -> bool {
        matches!(
            self.gucs
                .get("allow_in_place_tablespaces")
                .map(|value| value.trim().to_ascii_lowercase())
                .as_deref(),
            Some("on" | "true")
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
            .and_then(|txn| txn.xid.map(|xid| (xid, txn.next_command_id)))
    }

    pub fn session_user_oid(&self) -> u32 {
        self.auth.session_user_oid()
    }

    pub fn current_user_oid(&self) -> u32 {
        self.auth.current_user_oid()
    }

    pub fn active_role_oid(&self) -> Option<u32> {
        self.auth.active_role_oid()
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

    pub(crate) fn row_security_enabled(&self) -> bool {
        self.gucs
            .get("row_security")
            .map(|value| parse_bool_guc(value).unwrap_or(true))
            .unwrap_or(true)
    }

    pub(crate) fn catalog_lookup<'a>(&self, db: &'a Database) -> LazyCatalogLookup<'a> {
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(
            self.client_id,
            self.active_txn
                .as_ref()
                .and_then(|txn| txn.xid.map(|xid| (xid, txn.next_command_id))),
            search_path.as_deref(),
        )
    }

    fn catalog_lookup_for_command<'a>(
        &self,
        db: &'a Database,
        xid: TransactionId,
        cid: u32,
    ) -> LazyCatalogLookup<'a> {
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(self.client_id, Some((xid, cid)), search_path.as_deref())
    }

    fn executor_context_for_catalog(
        &self,
        db: &Database,
        snapshot: crate::backend::access::transam::xact::Snapshot,
        cid: u32,
        catalog: &crate::backend::utils::cache::lsyscache::LazyCatalogLookup<'_>,
        deferred_foreign_keys: Option<DeferredForeignKeyTracker>,
        statement_lock_scope_id: Option<u64>,
    ) -> ExecutorContext {
        let transaction_state = Some(Arc::new(parking_lot::Mutex::new(
            ExecutorTransactionState {
                xid: (snapshot.current_xid != INVALID_TRANSACTION_ID)
                    .then_some(snapshot.current_xid),
                cid,
            },
        )));
        ExecutorContext {
            pool: Arc::clone(&db.pool),
            txns: db.txns.clone(),
            txn_waiter: Some(db.txn_waiter.clone()),
            sequences: Some(db.sequences.clone()),
            large_objects: Some(db.large_objects.clone()),
            async_notify_runtime: Some(db.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&db.advisory_locks),
            row_locks: Arc::clone(&db.row_locks),
            checkpoint_stats: db.checkpoint_stats_snapshot(),
            datetime_config: self.datetime_config.clone(),
            interrupts: self.interrupts(),
            stats: Arc::clone(&db.stats),
            session_stats: Arc::clone(&self.stats_state),
            snapshot,
            transaction_state,
            client_id: self.client_id,
            current_database_name: db.current_database_name(),
            session_user_oid: self.session_user_oid(),
            current_user_oid: self.current_user_oid(),
            active_role_oid: self.active_role_oid(),
            statement_lock_scope_id,
            transaction_lock_scope_id: self.active_advisory_scope_id(),
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys,
        }
    }

    fn active_transaction_without_xid(&self, db: &Database) -> ActiveTransaction {
        ActiveTransaction {
            xid: None,
            advisory_scope_id: db.allocate_statement_lock_scope_id(),
            failed: false,
            auth_at_start: self.auth.clone(),
            held_table_locks: BTreeMap::new(),
            next_command_id: 0,
            catalog_effects: Vec::new(),
            current_cmd_catalog_invalidations: Vec::new(),
            prior_cmd_catalog_invalidations: Vec::new(),
            temp_effects: Vec::new(),
            sequence_effects: Vec::new(),
            deferred_foreign_keys: DeferredForeignKeyTracker::default(),
            async_listen_ops: Vec::new(),
            pending_async_notifications: Vec::new(),
        }
    }

    fn ensure_active_xid(&mut self, db: &Database) -> TransactionId {
        let txn = self
            .active_txn
            .as_mut()
            .expect("ensure_active_xid requires an active transaction");
        if let Some(xid) = txn.xid {
            return xid;
        }
        let xid = db.txns.write().begin();
        txn.xid = Some(xid);
        xid
    }

    fn active_txn_ctx_for_command(&self, cid: CommandId) -> Option<(TransactionId, CommandId)> {
        self.active_txn
            .as_ref()
            .and_then(|txn| txn.xid.map(|xid| (xid, cid)))
    }

    fn active_advisory_scope_id(&self) -> Option<u64> {
        self.active_txn.as_ref().map(|txn| txn.advisory_scope_id)
    }

    fn statement_requires_xid_in_transaction(stmt: &Statement) -> bool {
        !matches!(
            stmt,
            Statement::Do(_)
                | Statement::Show(_)
                | Statement::Set(_)
                | Statement::Reset(_)
                | Statement::Checkpoint(_)
                | Statement::Select(_)
                | Statement::Values(_)
                | Statement::Explain(_)
                | Statement::Notify(_)
                | Statement::Listen(_)
                | Statement::Unlisten(_)
                | Statement::SetSessionAuthorization(_)
                | Statement::ResetSessionAuthorization(_)
                | Statement::SetRole(_)
                | Statement::ResetRole(_)
                | Statement::Begin
                | Statement::Commit
                | Statement::Rollback
        )
    }

    fn queue_txn_listener_op(&mut self, action: AsyncListenAction, channel: Option<String>) {
        if let Some(txn) = self.active_txn.as_mut() {
            txn.async_listen_ops.push(AsyncListenOp { action, channel });
        }
    }

    fn queue_txn_notification(&mut self, channel: &str, payload: &str) -> Result<(), ExecError> {
        let txn = self.active_txn.as_mut().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "active transaction",
                actual: "no active transaction for NOTIFY".into(),
            })
        })?;
        queue_pending_notification(&mut txn.pending_async_notifications, channel, payload)
    }

    fn merge_ctx_pending_async_notifications(
        &mut self,
        ctx: &mut ExecutorContext,
        succeeded: bool,
    ) {
        if !succeeded {
            ctx.pending_async_notifications.clear();
            return;
        }
        let Some(txn) = self.active_txn.as_mut() else {
            ctx.pending_async_notifications.clear();
            return;
        };
        let pending = mem::take(&mut ctx.pending_async_notifications);
        merge_pending_notifications(&mut txn.pending_async_notifications, pending);
    }

    fn validate_deferred_foreign_keys_for_active_txn(
        &self,
        db: &Database,
    ) -> Result<(), ExecError> {
        let Some(txn) = self.active_txn.as_ref() else {
            return Ok(());
        };
        if txn.deferred_foreign_keys.is_empty() {
            return Ok(());
        }
        let Some(xid) = txn.xid else {
            debug_assert!(
                false,
                "deferred foreign keys require a transaction id before commit"
            );
            return Ok(());
        };
        let catalog = self.catalog_lookup_for_command(db, xid, txn.next_command_id);
        validate_deferred_foreign_key_constraints(
            db,
            self.client_id,
            &catalog,
            xid,
            txn.next_command_id,
            self.interrupts(),
            &self.datetime_config,
            &txn.deferred_foreign_keys,
        )
    }

    fn finalize_taken_transaction(
        &mut self,
        db: &Database,
        txn: ActiveTransaction,
        result: Result<StatementResult, ExecError>,
    ) -> Result<StatementResult, ExecError> {
        let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
        let result = match result {
            Ok(r) => {
                (|| {
                    if let Some(xid) = txn.xid {
                        let _checkpoint_guard = db.checkpoint_commit_guard();
                        db.pool.write_wal_commit(xid).map_err(|e| {
                            ExecError::Heap(
                                crate::backend::access::heap::heapam::HeapError::Storage(
                                    crate::backend::storage::smgr::SmgrError::Io(
                                        std::io::Error::new(std::io::ErrorKind::Other, e),
                                    ),
                                ),
                            )
                        })?;
                        db.pool.flush_wal().map_err(|e| {
                            ExecError::Heap(
                                crate::backend::access::heap::heapam::HeapError::Storage(
                                    crate::backend::storage::smgr::SmgrError::Io(
                                        std::io::Error::new(std::io::ErrorKind::Other, e),
                                    ),
                                ),
                            )
                        })?;
                        db.txns.write().commit(xid).map_err(|e| {
                            ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(
                                e,
                            ))
                        })?;
                        // :HACK: See `Database::finish_txn()`: session commit also needs the
                        // transaction status flushed so fresh durable snapshot readers observe
                        // catalog changes immediately.
                        db.txns.write().flush_clog().map_err(|e| {
                            ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(
                                e,
                            ))
                        })?;
                        db.txn_waiter.notify();
                    } else {
                        debug_assert!(txn.catalog_effects.is_empty());
                        debug_assert!(txn.temp_effects.is_empty());
                        debug_assert!(txn.sequence_effects.is_empty());
                    }
                    db.finalize_committed_catalog_effects(
                        self.client_id,
                        &txn.catalog_effects,
                        &txn.prior_cmd_catalog_invalidations,
                    );
                    db.finalize_committed_temp_effects(self.client_id, &txn.temp_effects);
                    db.finalize_committed_sequence_effects(&txn.sequence_effects)?;
                    db.apply_temp_on_commit(self.client_id)?;
                    db.async_notify_runtime
                        .apply_listener_ops(self.client_id, &txn.async_listen_ops);
                    db.async_notify_runtime
                        .publish(self.client_id, &txn.pending_async_notifications);
                    db.advisory_locks
                        .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
                    db.row_locks
                        .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
                    self.stats_state.write().commit_top_level_xact(&db.stats);
                    Ok(r)
                })()
            }
            Err(e) => {
                self.abort_taken_transaction(db, &txn);
                Err(e)
            }
        };
        for rel in held_locks {
            db.table_locks.unlock_table(rel, self.client_id);
        }
        result
    }

    fn abort_taken_transaction(&mut self, db: &Database, txn: &ActiveTransaction) {
        if let Some(xid) = txn.xid {
            let _ = db.txns.write().abort(xid);
            db.txn_waiter.notify();
        } else {
            debug_assert!(txn.catalog_effects.is_empty());
            debug_assert!(txn.temp_effects.is_empty());
            debug_assert!(txn.sequence_effects.is_empty());
        }
        db.finalize_aborted_local_catalog_invalidations(
            self.client_id,
            &txn.prior_cmd_catalog_invalidations,
            &txn.current_cmd_catalog_invalidations,
        );
        db.finalize_aborted_catalog_effects(&txn.catalog_effects);
        db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
        db.finalize_aborted_sequence_effects(&txn.sequence_effects);
        db.advisory_locks
            .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
        db.row_locks
            .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
        if self.auth != txn.auth_at_start {
            self.auth = txn.auth_at_start.clone();
            db.install_auth_state(self.client_id, self.auth.clone());
            db.plan_cache.invalidate_all();
        }
        self.stats_state.write().rollback_top_level_xact();
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

    fn advance_catalog_command_id_after_statement(&mut self, base_cid: u32, effect_start: usize) {
        let Some(txn) = self.active_txn.as_mut() else {
            return;
        };
        let consumed_catalog_cids = txn
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        let next_cid = base_cid.saturating_add(consumed_catalog_cids as u32);
        txn.next_command_id = txn.next_command_id.max(next_cid);
    }

    pub fn execute(&mut self, db: &Database, sql: &str) -> Result<StatementResult, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        let statement_lock_scope = StatementLockScopeGuard::new(
            Arc::clone(&db.advisory_locks),
            Arc::clone(&db.row_locks),
            self.client_id,
            self.active_txn
                .is_none()
                .then(|| db.allocate_statement_lock_scope_id()),
        );
        db.install_auth_state(self.client_id, self.auth.clone());
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        db.install_temp_backend_id(self.client_id, self.temp_backend_id);
        db.install_stats_state(self.client_id, Arc::clone(&self.stats_state));
        let result = stacker::grow(32 * 1024 * 1024, || {
            self.execute_internal(db, sql, statement_lock_scope.scope_id())
        });
        if matches!(result, Err(ExecError::Interrupted(_))) {
            self.interrupts.reset_statement_state();
        }
        result
    }

    fn execute_internal(
        &mut self,
        db: &Database,
        sql: &str,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
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

        if self.active_txn.is_some()
            && !matches!(
                stmt,
                Statement::Begin | Statement::Commit | Statement::Rollback
            )
        {
            if self.transaction_failed() {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "ROLLBACK",
                    actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                }));
            }
            if matches!(stmt, Statement::Vacuum(_)) {
                return Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")));
            }
            let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
            if result.is_err() {
                if let Some(ref mut txn) = self.active_txn {
                    txn.failed = true;
                }
            }
            return result;
        }

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Show(ref show_stmt) => self.apply_show(db, show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(db, set_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(db, reset_stmt),
            Statement::Checkpoint(_) => self.apply_checkpoint(db),
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CreateFunction(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::CreateAggregate(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_aggregate_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateOperator(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_operator_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropFunction(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_function_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropAggregate(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_aggregate_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropOperator(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_operator_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateDatabase(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_tablespace_stmt(
                        self.client_id,
                        create_stmt,
                        self.allow_in_place_tablespaces(),
                    )
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
            Statement::CreatePublication(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_publication_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateTrigger(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_trigger_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreatePolicy(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_policy_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateStatistics(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_statistics_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterStatistics(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_statistics_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::DropTrigger(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_trigger_stmt_with_search_path(
                    self.client_id,
                    drop_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::DropPublication(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_publication_stmt_with_search_path(
                    self.client_id,
                    drop_stmt,
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::AlterIndexRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_alter_column_statistics_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_view_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewOwner(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::AlterPublication(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_publication_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterOperator(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_operator_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameColumn(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_default_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_compression_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_storage_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_options_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_statistics_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAddConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::AlterTableAlterConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::AlterTableInherit(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_inherit_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableNoInherit(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_no_inherit_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAttachPartition(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_attach_partition_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_row_security_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterPolicy(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_policy_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateRole(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::SetRole(ref set_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth = db.execute_set_role_stmt(self.client_id, set_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::ResetRole(ref reset_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth = db.execute_reset_role_stmt(self.client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CommentOnTable(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::CommentOnIndex(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_index_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnConstraint(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_constraint_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnRule(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_rule_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnTrigger(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_trigger_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnAggregate(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_aggregate_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnFunction(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_function_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            Statement::CommentOnPublication(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_publication_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::Merge(ref merge_stmt) => {
                let _ = merge_stmt;
                let search_path = self.configured_search_path();
                db.execute_statement_with_search_path(self.client_id, stmt, search_path.as_deref())
            }
            Statement::Begin => {
                if self.active_txn.is_some() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "no active transaction",
                        actual: "already in a transaction block".into(),
                    }));
                }
                self.active_txn = Some(self.active_transaction_without_xid(db));
                self.stats_state.write().begin_top_level_xact();
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Commit => {
                if self.active_txn.is_none() {
                    return Ok(StatementResult::AffectedRows(0));
                }
                let result = self
                    .validate_deferred_foreign_keys_for_active_txn(db)
                    .map(|_| StatementResult::AffectedRows(0));
                let txn = self.active_txn.take().unwrap();
                self.finalize_taken_transaction(db, txn, result)
            }
            Statement::Rollback => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
                self.abort_taken_transaction(db, &txn);
                for rel in held_locks {
                    db.table_locks.unlock_table(rel, self.client_id);
                }
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
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
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
            if let Some(xid) = txn.xid {
                let _ = db.txns.write().abort(xid);
                db.txn_waiter.notify();
            } else {
                debug_assert!(txn.catalog_effects.is_empty());
                debug_assert!(txn.temp_effects.is_empty());
                debug_assert!(txn.sequence_effects.is_empty());
            }
            db.finalize_aborted_local_catalog_invalidations(
                self.client_id,
                &txn.prior_cmd_catalog_invalidations,
                &txn.current_cmd_catalog_invalidations,
            );
            db.finalize_aborted_catalog_effects(&txn.catalog_effects);
            db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
            db.finalize_aborted_sequence_effects(&txn.sequence_effects);
            db.advisory_locks
                .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
            db.row_locks
                .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
            for rel in txn.held_table_locks.keys().copied() {
                db.table_locks.unlock_table(rel, self.client_id);
            }
        }
        db.async_notify_runtime.disconnect(self.client_id);

        // :HACK: Session-scoped table locks are currently tracked partly on the
        // session and partly in the global table lock manager. Release anything
        // still associated with this backend on disconnect, mirroring PostgreSQL
        // backend-exit lock cleanup even if the session missed normal unwind.
        db.table_locks.unlock_all_for_client(self.client_id);
        db.advisory_locks.unlock_all_session(self.client_id);
        db.row_locks.unlock_all_session(self.client_id);
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
        db.install_auth_state(self.client_id, self.auth.clone());
        db.install_temp_backend_id(self.client_id, self.temp_backend_id);
        db.install_interrupt_state(self.client_id, self.interrupts());
        let (txn_ctx, transaction_lock_scope_id) = if let Some(ref mut txn) = self.active_txn {
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            (txn.xid.map(|xid| (xid, cid)), Some(txn.advisory_scope_id))
        } else {
            (None, None)
        };
        let statement_lock_scope_id = txn_ctx
            .is_none()
            .then(|| db.allocate_statement_lock_scope_id());
        let search_path = self.configured_search_path();
        let mut guard = db.execute_streaming_with_search_path_and_datetime_config(
            self.client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            search_path.as_deref(),
            &self.datetime_config,
        )?;
        guard.interrupt_guard = Some(self.statement_interrupt_guard()?);
        Ok(guard)
    }

    fn execute_in_transaction(
        &mut self,
        db: &Database,
        stmt: Statement,
        _statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        let effect_start = self
            .active_txn
            .as_ref()
            .map(|txn| txn.catalog_effects.len())
            .unwrap_or(0);
        let cid = {
            let txn = self.active_txn.as_mut().unwrap();
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            cid
        };
        let xid = if Self::statement_requires_xid_in_transaction(&stmt) {
            self.ensure_active_xid(db)
        } else {
            self.active_txn
                .as_ref()
                .and_then(|txn| txn.xid)
                .unwrap_or(INVALID_TRANSACTION_ID)
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
            Statement::CommentOnForeignDataWrapper(ref comment_stmt) => {
                db.execute_comment_on_foreign_data_wrapper_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnPublication(ref comment_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_publication_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
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
            Statement::CreateForeignDataWrapper(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterForeignDataWrapper(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterForeignDataWrapperOwner(ref alter_stmt) => {
                db.execute_alter_foreign_data_wrapper_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignDataWrapperRename(ref alter_stmt) => {
                db.execute_alter_foreign_data_wrapper_rename_stmt(client_id, alter_stmt)
            }
            Statement::DropForeignDataWrapper(ref drop_stmt) => {
                db.execute_drop_foreign_data_wrapper_stmt(client_id, drop_stmt)
            }
            Statement::CreatePublication(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_publication_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CreateTrigger(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_trigger_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::CreatePolicy(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_policy_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
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
            Statement::CreateStatistics(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_statistics_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::AlterStatistics(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_alter_statistics_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::CreateOperatorClass(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_operator_class_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::CreateOperator(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_create_operator_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
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
            Statement::AlterIndexRename(ref rename_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_index_rename_stmt_in_transaction_with_search_path(
                    client_id,
                    rename_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                if let Some(relation) = catalog.lookup_any_relation(&alter_stmt.index_name) {
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_index_alter_column_statistics_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterViewRename(ref rename_stmt) => {
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
                db.execute_alter_view_rename_stmt_in_transaction_with_search_path(
                    client_id,
                    rename_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
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
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => {
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
                db.execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => {
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
                db.execute_alter_table_alter_column_compression_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => {
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
                db.execute_alter_table_alter_column_storage_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => {
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
                db.execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => {
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
                db.execute_alter_table_alter_column_statistics_stmt_in_transaction_with_search_path(
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
            Statement::AlterTableAlterConstraint(ref alter_stmt) => {
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
                db.execute_alter_table_alter_constraint_stmt_in_transaction_with_search_path(
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
            Statement::AlterTableInherit(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                let parent = catalog
                    .lookup_any_relation(&alter_stmt.parent_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.parent_name.clone(),
                        ))
                    })?;
                let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                requests
                    .entry(relation.rel)
                    .and_modify(|existing| {
                        *existing = existing.strongest(TableLockMode::AccessExclusive)
                    })
                    .or_insert(TableLockMode::AccessExclusive);
                requests
                    .entry(parent.rel)
                    .and_modify(|existing| {
                        *existing = existing.strongest(TableLockMode::AccessShare)
                    })
                    .or_insert(TableLockMode::AccessShare);
                let requests = requests.into_iter().collect::<Vec<_>>();
                self.lock_table_requests_if_needed(db, &requests)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_inherit_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableNoInherit(ref alter_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                let parent = catalog
                    .lookup_any_relation(&alter_stmt.parent_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.parent_name.clone(),
                        ))
                    })?;
                let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                requests
                    .entry(relation.rel)
                    .and_modify(|existing| {
                        *existing = existing.strongest(TableLockMode::AccessExclusive)
                    })
                    .or_insert(TableLockMode::AccessExclusive);
                requests
                    .entry(parent.rel)
                    .and_modify(|existing| {
                        *existing = existing.strongest(TableLockMode::AccessShare)
                    })
                    .or_insert(TableLockMode::AccessShare);
                let requests = requests.into_iter().collect::<Vec<_>>();
                self.lock_table_requests_if_needed(db, &requests)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_no_inherit_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableAttachPartition(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => {
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
                db.execute_alter_table_set_row_security_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterPolicy(ref alter_stmt) => {
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
                db.execute_alter_policy_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CreateRole(ref create_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_role_stmt_in_transaction(
                    client_id,
                    create_stmt,
                    self.gucs.get("createrole_self_grant").map(String::as_str),
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::CreateDatabase(_) => Err(ExecError::Parse(
                ParseError::ActiveSqlTransaction("CREATE DATABASE"),
            )),
            Statement::AlterRole(ref alter_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_role_stmt_in_transaction(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropRole(ref drop_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_role_stmt_in_transaction(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
            }
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
            Statement::DropOwned(ref drop_stmt) => {
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_owned_stmt_in_transaction(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    &mut txn.catalog_effects,
                )
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
            Statement::DropPublication(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_drop_publication_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::DropTrigger(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_drop_trigger_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
            }
            Statement::DropPolicy(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                db.execute_drop_policy_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
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
            Statement::SetRole(ref set_stmt) => {
                self.auth = db.execute_set_role_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetRole(ref reset_stmt) => {
                self.auth = db.execute_reset_role_stmt(client_id, reset_stmt)?;
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
            Statement::AlterPublication(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_publication_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnTable(ref comment_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = catalog
                    .lookup_relation(&comment_stmt.table_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("relation \"{}\" does not exist", comment_stmt.table_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P01",
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
            Statement::CommentOnIndex(ref comment_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = match catalog.lookup_any_relation(&comment_stmt.index_name) {
                    Some(relation) if relation.relkind == 'i' => relation,
                    Some(_) => {
                        return Err(ExecError::Parse(ParseError::WrongObjectType {
                            name: comment_stmt.index_name.clone(),
                            expected: "index",
                        }));
                    }
                    None => {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                            comment_stmt.index_name.clone(),
                        )));
                    }
                };
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_index_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnAggregate(ref comment_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnFunction(ref comment_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_function_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnConstraint(ref comment_stmt) => {
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
                db.execute_comment_on_constraint_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnRule(ref comment_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = match catalog.lookup_any_relation(&comment_stmt.relation_name) {
                    Some(relation) if matches!(relation.relkind, 'r' | 'v') => relation,
                    Some(_) => {
                        return Err(ExecError::Parse(ParseError::WrongObjectType {
                            name: comment_stmt.relation_name.clone(),
                            expected: "table or view",
                        }));
                    }
                    None => {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                            comment_stmt.relation_name.clone(),
                        )));
                    }
                };
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_comment_on_rule_stmt_in_transaction_with_search_path(
                    client_id,
                    comment_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::CommentOnTrigger(ref comment_stmt) => {
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
                db.execute_comment_on_trigger_stmt_in_transaction_with_search_path(
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
                let targets = db.effective_analyze_targets_with_search_path(
                    client_id,
                    Some((xid, cid)),
                    search_path.as_deref(),
                    analyze_stmt,
                )?;
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_analyze_stmt_in_transaction_with_search_path(
                    client_id,
                    &targets,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::Vacuum(_) => {
                Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")))
            }
            Statement::Notify(ref notify_stmt) => self
                .queue_txn_notification(
                    &notify_stmt.channel,
                    notify_stmt.payload.as_deref().unwrap_or(""),
                )
                .map(|_| StatementResult::AffectedRows(0)),
            Statement::Listen(ref listen_stmt) => {
                self.queue_txn_listener_op(
                    AsyncListenAction::Listen,
                    Some(listen_stmt.channel.clone()),
                );
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Unlisten(ref unlisten_stmt) => {
                self.queue_txn_listener_op(
                    AsyncListenAction::Unlisten,
                    unlisten_stmt.channel.clone(),
                );
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Merge(ref merge_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = plan_merge(merge_stmt, &catalog)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx =
                    self.executor_context_for_catalog(db, snapshot, cid, &catalog, None, None);
                let result = execute_merge(bound, &catalog, &mut ctx, xid, cid);
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
            }
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let search_path = self.configured_search_path();
                let txn_ctx = self.active_txn_ctx_for_command(cid);
                let snapshot = match txn_ctx {
                    Some((xid, cid)) => db.txns.read().snapshot_for_command(xid, cid)?,
                    None => db
                        .txns
                        .read()
                        .snapshot_for_command(INVALID_TRANSACTION_ID, cid)?,
                };
                let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, search_path.as_deref());
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                let result = execute_readonly_statement(stmt, &catalog, &mut ctx);
                if let Some(xid) = ctx.transaction_xid()
                    && let Some(txn) = self.active_txn.as_mut()
                {
                    txn.xid = Some(xid);
                }
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let prepared =
                    crate::pgrust::database::commands::rules::prepare_bound_insert_for_execution(
                        bound, &catalog,
                    )?;
                let lock_requests = merge_table_lock_requests(
                    &insert_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                let result =
                    crate::pgrust::database::commands::rules::execute_bound_insert_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        cid,
                    );
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_update(update_stmt, &catalog)?;
                let prepared =
                    crate::pgrust::database::commands::rules::prepare_bound_update_for_execution(
                        bound, &catalog,
                    )?;
                let lock_requests = merge_table_lock_requests(
                    &update_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let interrupts = self.interrupts();
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                ctx.interrupts = Arc::clone(&interrupts);
                let result =
                    crate::pgrust::database::commands::rules::execute_bound_update_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        cid,
                        Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                    );
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let prepared =
                    crate::pgrust::database::commands::rules::prepare_bound_delete_for_execution(
                        bound, &catalog,
                    )?;
                let lock_requests = merge_table_lock_requests(
                    &delete_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                self.lock_table_requests_if_needed(db, &lock_requests)?;
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let interrupts = self.interrupts();
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                ctx.interrupts = Arc::clone(&interrupts);
                let result =
                    crate::pgrust::database::commands::rules::execute_bound_delete_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                    );
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
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
            Statement::CreateAggregate(ref create_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_aggregate_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::AlterOperator(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_alter_operator_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
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
                let allow_in_place_tablespaces = self.allow_in_place_tablespaces();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_tablespace_stmt_in_transaction(
                    client_id,
                    create_stmt,
                    allow_in_place_tablespaces,
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
            Statement::DropFunction(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_function_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropAggregate(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_aggregate_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
            }
            Statement::DropOperator(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_operator_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
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
            Statement::CreateRule(ref create_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let relation = match catalog.lookup_any_relation(&create_stmt.relation_name) {
                    Some(relation) if matches!(relation.relkind, 'r' | 'v') => relation,
                    Some(_) => {
                        return Err(ExecError::Parse(ParseError::WrongObjectType {
                            name: create_stmt.relation_name.clone(),
                            expected: "table or view",
                        }));
                    }
                    None => {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                            create_stmt.relation_name.clone(),
                        )));
                    }
                };
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_create_rule_stmt_in_transaction_with_search_path(
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
            Statement::DropRule(ref drop_stmt) => {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                if let Some(relation) = catalog.lookup_any_relation(&drop_stmt.relation_name) {
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                }
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                db.execute_drop_rule_stmt_in_transaction_with_search_path(
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
            self.advance_catalog_command_id_after_statement(cid, effect_start);
            self.process_catalog_command_end(db, effect_start);
        }

        result
    }

    fn apply_set(
        &mut self,
        db: &Database,
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
        if name == "row_security" {
            db.install_row_security_enabled(self.client_id, self.row_security_enabled());
            db.plan_cache.invalidate_all();
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_reset(
        &mut self,
        db: &Database,
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
                "xmlbinary" => self.datetime_config.xml.binary = Default::default(),
                "xmloption" => self.datetime_config.xml.option = Default::default(),
                "stats_fetch_consistency" => self
                    .stats_state
                    .write()
                    .set_fetch_consistency(StatsFetchConsistency::Cache),
                "track_functions" => self
                    .stats_state
                    .write()
                    .set_track_functions(TrackFunctionsSetting::None),
                _ => {}
            }
            self.gucs.remove(&normalized);
            if normalized == "row_security" {
                db.install_row_security_enabled(self.client_id, self.row_security_enabled());
                db.plan_cache.invalidate_all();
            }
        } else {
            self.gucs.clear();
            self.guc_reset_datestyle();
            self.guc_reset_timezone();
            self.datetime_config.xml = Default::default();
            self.stats_state
                .write()
                .set_fetch_consistency(StatsFetchConsistency::Cache);
            db.install_row_security_enabled(self.client_id, true);
            db.plan_cache.invalidate_all();
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
                "xmlbinary" => format_xmlbinary(self.datetime_config.xml.binary).to_string(),
                "xmloption" => format_xmloption(self.datetime_config.xml.option).to_string(),
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
            "xmlbinary" => (
                "xmlbinary".to_string(),
                format_xmlbinary(self.datetime_config.xml.binary).to_string(),
            ),
            "xmloption" => (
                "xmloption".to_string(),
                format_xmloption(self.datetime_config.xml.option).to_string(),
            ),
            _ if is_checkpoint_guc(&name) => (
                stmt.name.clone(),
                db.checkpoint_config_value(&name)
                    .unwrap_or_else(|| "default".to_string()),
            ),
            _ => (
                stmt.name.clone(),
                self.gucs.get(&name).cloned().unwrap_or_else(fallback_value),
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
        let mut stored_value = value.to_string();
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
            "xmlbinary" => {
                let Some(binary) = parse_xmlbinary(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.datetime_config.xml.binary = binary;
            }
            "xmloption" => {
                let Some(option) = parse_xmloption(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.datetime_config.xml.option = option;
            }
            "max_stack_depth" => {
                self.datetime_config.max_stack_depth_kb = parse_max_stack_depth(value)?;
            }
            "stats_fetch_consistency" => {
                let Some(fetch_consistency) = StatsFetchConsistency::parse(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.stats_state
                    .write()
                    .set_fetch_consistency(fetch_consistency);
            }
            "track_functions" => {
                let Some(track_functions) = TrackFunctionsSetting::parse(value) else {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                };
                self.stats_state
                    .write()
                    .set_track_functions(track_functions);
            }
            "row_security" => {
                parse_bool_guc(value).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string()))
                })?;
            }
            "default_toast_compression" => {
                stored_value = parse_default_toast_compression_guc_value(value)?.to_string();
            }
            _ => {}
        }
        self.gucs.insert(normalized, stored_value);
        Ok(())
    }

    pub fn prepare_insert(
        &self,
        db: &Database,
        table_name: &str,
        columns: Option<&[String]>,
        num_params: usize,
    ) -> Result<PreparedInsert, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            let catalog = self.catalog_lookup(db);
            Ok(bind_insert_prepared(
                table_name, columns, num_params, &catalog,
            )?)
        })
    }

    pub fn execute_prepared_insert(
        &mut self,
        db: &Database,
        prepared: &PreparedInsert,
        params: &[Value],
    ) -> Result<(), ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            if self.active_txn.is_none() {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "active transaction",
                    actual: "no active transaction for prepared insert".into(),
                }));
            }
            let xid = self.ensure_active_xid(db);
            let txn = self.active_txn.as_mut().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "active transaction",
                    actual: "no active transaction for prepared insert".into(),
                })
            })?;
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            let _client_id = self.client_id;

            let lock_requests = prepared_insert_foreign_key_lock_requests(prepared);
            self.lock_table_requests_if_needed(db, &lock_requests)?;

            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            let catalog = self.catalog_lookup_for_command(db, xid, cid);
            let interrupts = self.interrupts();
            let deferred_foreign_keys = self
                .active_txn
                .as_ref()
                .unwrap()
                .deferred_foreign_keys
                .clone();
            let mut ctx = self.executor_context_for_catalog(
                db,
                snapshot,
                cid,
                &catalog,
                Some(deferred_foreign_keys),
                None,
            );
            ctx.interrupts = interrupts;
            let result = execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid);
            self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
            result
        })
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
        stacker::grow(32 * 1024 * 1024, || {
            db.install_interrupt_state(self.client_id, self.interrupts());
            let started_txn = if self.active_txn.is_none() {
                self.active_txn = Some(self.active_transaction_without_xid(db));
                self.stats_state.write().begin_top_level_xact();
                true
            } else {
                false
            };

            let result = (|| -> Result<usize, ExecError> {
                let xid = self.ensure_active_xid(db);
                let cid = {
                    let txn = self.active_txn.as_mut().unwrap();
                    let cid = txn.next_command_id;
                    txn.next_command_id = txn.next_command_id.saturating_add(1);
                    cid
                };

                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let (relation_oid, rel, toast, toast_index, desc, indexes) = {
                    let entry = catalog.lookup_any_relation(table_name).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(table_name.to_string()))
                    })?;
                    if relation_has_row_security(entry.relation_oid, &catalog) {
                        return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                            "COPY FROM is not yet supported on tables with row-level security"
                                .into(),
                        )));
                    }
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
                        let Some(index) =
                            desc.columns.iter().position(|column| column.name == *name)
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
                                | ScalarType::Multirange(_)
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
                                ScalarType::Xml => {
                                    cast_value(Value::Text(raw.clone().into()), column.sql_type)?
                                }
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
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let interrupts = self.interrupts();
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                ctx.interrupts = interrupts;
                let result = crate::backend::commands::tablecmds::execute_insert_values(
                    table_name,
                    relation_oid,
                    rel,
                    toast,
                    toast_index.as_ref(),
                    &desc,
                    &relation_constraints,
                    &[],
                    &indexes,
                    &parsed_rows,
                    &mut ctx,
                    xid,
                    cid,
                );
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                result
            })();

            if started_txn {
                let result = result.and_then(|n| {
                    self.validate_deferred_foreign_keys_for_active_txn(db)?;
                    Ok(StatementResult::AffectedRows(n))
                });
                let txn = self.active_txn.take().unwrap();
                self.finalize_taken_transaction(db, txn, result)
                    .map(|result| match result {
                        StatementResult::AffectedRows(rows) => rows as usize,
                        other => {
                            panic!(
                                "expected COPY finalization to return affected rows, got {other:?}"
                            )
                        }
                    })
            } else {
                result
            }
        })
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

fn parse_bool_guc(value: &str) -> Option<bool> {
    match normalize_guc_name(value).as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
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
    use super::{
        parse_default_toast_compression_guc_value, parse_max_stack_depth, parse_startup_options,
        parse_statement_timeout,
    };
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

    #[test]
    fn default_toast_compression_guc_accepts_pglz() {
        assert_eq!(
            parse_default_toast_compression_guc_value("pglz").unwrap(),
            "pglz"
        );
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn default_toast_compression_guc_rejects_invalid_values() {
        for value in ["", "I do not exist compression", "lz4"] {
            let err = parse_default_toast_compression_guc_value(value).unwrap_err();
            match err {
                ExecError::DetailedError {
                    message,
                    hint,
                    sqlstate,
                    ..
                } => {
                    assert_eq!(
                        message,
                        format!(
                            "invalid value for parameter \"default_toast_compression\": \"{value}\""
                        )
                    );
                    assert_eq!(hint.as_deref(), Some("Available values: pglz."));
                    assert_eq!(sqlstate, "22023");
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn default_toast_compression_guc_accepts_lz4() {
        assert_eq!(
            parse_default_toast_compression_guc_value("lz4").unwrap(),
            "lz4"
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
