#![allow(unused_imports)]

mod agg;
mod constraints;
mod domain;
mod driver;
pub mod exec_expr;
pub(crate) mod exec_tuples;
mod expr_agg_support;
mod expr_async;
mod expr_casts;
mod expr_compile;
mod expr_json;
mod expr_locks;
pub(crate) mod expr_ops;
mod expr_partition;
pub(crate) mod expr_reg;
mod expr_txid;
mod expr_xml;
mod fmgr;
mod foreign_keys;
pub(crate) mod function_guc;
pub(crate) mod hashjoin;
pub(crate) mod jsonb;
pub(crate) mod jsonpath;
pub(crate) mod mergejoin;
mod node_hash;
mod node_hashjoin;
mod node_mergejoin;
mod nodes;
pub(crate) mod parallel;
mod permissions;
mod pg_regex;
mod random;
mod sqlfunc;
mod srf;
mod startup;
mod tsearch;
pub(crate) mod value_io;
mod window;
pub(crate) mod expr {
    pub(crate) use super::exec_expr::*;
}
pub(crate) mod node_types {
    pub(crate) use crate::include::nodes::datum::*;
    pub(crate) use crate::include::nodes::execnodes::*;
    pub(crate) use crate::include::nodes::plannodes::*;
}
pub(crate) mod tuple_decoder {
    pub(crate) use super::exec_tuples::*;
}

pub use crate::include::executor::execdesc::*;
pub use crate::include::nodes::datum::*;
pub use crate::include::nodes::execnodes::*;
pub use crate::include::nodes::plannodes::*;
pub(crate) use agg::{AccumState, AggGroup, AggTransitionFn, AggregateRuntime, OrderedAggInput};
pub(crate) use domain::{
    cast_domain_text_input, enforce_domain_constraints_for_value,
    enforce_domain_constraints_for_value_ref,
};
pub use driver::{
    exec_next, execute_plan, execute_planned_stmt, execute_readonly_statement,
    execute_readonly_statement_with_config, execute_sql, execute_statement,
};
pub(crate) use exec_expr::clear_subquery_eval_cache;
pub use exec_expr::{eval_expr, eval_plpgsql_expr};
pub(crate) use expr_agg_support::build_aggregate_runtime;
pub(crate) use expr_agg_support::execute_scalar_function_value_call;
pub(crate) use expr_casts::cast_value_with_source_type_catalog_and_config;
pub(crate) use expr_casts::parse_bytea_text;
pub(crate) use expr_casts::parse_interval_text_value;
pub(crate) use expr_casts::parse_text_array_literal_with_op;
pub use expr_casts::render_internal_char_text;
pub(crate) use expr_casts::render_interval_text;
pub(crate) use expr_casts::render_interval_text_with_config;
pub(crate) use expr_casts::render_pg_lsn_text;
pub(crate) use expr_casts::{cast_value, cast_value_with_config};
pub(crate) use expr_casts::{
    parse_text_array_literal_with_catalog_and_op,
    parse_text_array_literal_with_catalog_op_and_explicit,
};
pub(crate) use expr_json::apply_jsonb_subscript_assignment;
pub(crate) fn eval_to_char_function(
    values: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_string::eval_to_char_function(values, datetime_config).map_err(Into::into)
}
pub(crate) use expr_txid::{
    cast_text_to_txid_snapshot, eval_txid_builtin_function, is_txid_snapshot_type_oid,
};
pub(crate) use expr_xml::{render_xml_output_text, strip_xml_declaration, validate_xml_input};
pub(crate) use nodes::{
    ensure_no_deferred_column_errors, pg_sql_sort_by, push_explain_datetime_config,
    render_explain_expr, render_explain_join_expr, render_explain_join_expr_inner,
    render_explain_literal, render_explain_projection_expr_with_qualifier, render_index_order_by,
    render_index_scan_condition_with_key_names,
    render_index_scan_condition_with_key_names_and_runtime_renderer,
    render_verbose_range_support_expr, runtime_pruned_startup_child_indexes,
};
pub use pgrust_executor::ScalarFunctionCallInfo;
pub(crate) use pgrust_expr::current_timestamp_value;
pub use pgrust_expr::money_format_text;
pub(crate) use pgrust_expr::money_parse_text;
pub(crate) use pgrust_expr::render_bit_text;
pub(crate) use pgrust_expr::{
    compare_multirange_values, decode_multirange_bytes, encode_multirange_bytes,
    multirange_intersection_agg_transition, parse_multirange_text, range_agg_transition,
};
pub(crate) use pgrust_expr::{
    compare_network_values, encode_network_bytes, eval_network_function, network_btree_upper_bound,
    network_contains, network_merge, network_prefix, parse_cidr_bytes, parse_cidr_text,
    parse_inet_bytes, parse_inet_text, render_network_text,
};
pub(crate) use pgrust_expr::{
    compare_range_values, decode_range_bytes, encode_range_bytes, parse_range_text,
};
pub use pgrust_expr::{render_datetime_value_text, render_datetime_value_text_with_config};
pub use pgrust_expr::{render_macaddr_text, render_macaddr8_text};
pub use pgrust_expr::{render_multirange_text, render_multirange_text_with_config};
pub use pgrust_expr::{render_range_text, render_range_text_with_config};
pub use random::PgPrngState;
pub(crate) use sqlfunc::{
    execute_user_defined_sql_scalar_function_values, render_sql_literal, substitute_named_arg,
    substitute_positional_args,
};
pub(crate) use srf::set_returning_call_label;
pub use startup::executor_start;
pub(crate) use tsearch::{
    array_to_tsvector, canonicalize_tsquery_rewrite_result, compare_tsquery, compare_tsvector,
    concat_tsvector, decode_tsquery_bytes, decode_tsvector_bytes, delete_tsvector_lexemes,
    encode_tsquery_bytes, encode_tsvector_bytes, eval_tsquery_matches_tsvector,
    eval_tsvector_matches_tsquery, filter_tsvector, numnode, parse_ts_weight, parse_tsquery_text,
    parse_tsvector_text, render_tsquery_text, render_tsvector_text, setweight_tsvector,
    strip_tsvector, text_array_items, ts_headline, ts_rank, ts_rank_cd, tsquery_and,
    tsquery_contained_by, tsquery_contains, tsquery_input_error, tsquery_not, tsquery_operands,
    tsquery_or, tsquery_phrase, tsquery_rewrite, tsvector_input_error, tsvector_to_array,
    unnest_tsvector,
};
pub use value_io::{format_array_value_text, render_uuid_text};

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::access::transam::xact::{
    CommandId, INVALID_TRANSACTION_ID, MvccError, Snapshot, TransactionId, TransactionManager,
};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::Catalog;
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::commands::tablecmds::*;
use crate::backend::parser::{
    CatalogLookup, ParseError, Statement, bind_delete, bind_insert, bind_update, parse_statement,
    pg_plan_query, pg_plan_values_query,
};
use crate::backend::storage::lmgr::{
    AdvisoryLockManager, RowLockError, RowLockManager, RowLockMode, RowLockOwner, RowLockTag,
};
use crate::backend::storage::lmgr::{
    PredicateFailureReason, PredicateLockError, PredicateLockTarget, SerializableXactId,
    TableLockError,
};
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc::{TEMP_BUFFERS_DEFAULT_PAGES, TEMP_BUFFERS_MIN_PAGES};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::backend::utils::misc::stack_depth;
use crate::include::access::htup::TupleError;
use crate::include::access::itemptr::ItemPointerData;
use crate::pgrust::database::{
    AsyncNotifyRuntime, Database, DatabaseStatsStore, LargeObjectRuntime, PendingNotification,
    SequenceRuntime, SessionStatsState, TempMutationEffect, TransactionWaiter,
};
use crate::pgrust::portal::Portal;
use crate::pl::plpgsql::PlpgsqlFunctionCache;
use crate::{BufferPool, ClientId, LocalBufferManager, SmgrStorageBackend};

pub type ExecutorCatalog = std::sync::Arc<dyn CatalogLookup>;

pub fn executor_catalog<C>(catalog: C) -> ExecutorCatalog
where
    C: CatalogLookup + 'static,
{
    std::sync::Arc::new(catalog)
}
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

pub(crate) use constraints::{
    enforce_relation_constraints, enforce_row_security_write_checks,
    enforce_row_security_write_checks_with_tid,
};
pub(crate) use expr_ops::compare_order_values;
use expr_ops::parse_numeric_text;
pub(crate) use foreign_keys::{
    enforce_deferred_inbound_foreign_key_check, enforce_inbound_foreign_key_reference,
    enforce_inbound_foreign_keys_on_delete, enforce_inbound_foreign_keys_on_update,
    enforce_outbound_foreign_keys, enforce_outbound_foreign_keys_for_insert,
    foreign_key_action_trigger_enabled_on_delete, foreign_key_action_trigger_enabled_on_update,
    validate_outbound_foreign_key_for_ddl,
};
pub(crate) use permissions::relation_values_visible_for_error_detail;
pub(crate) use pgrust_executor::InsertForeignKeyCheckPhase;
pub use pgrust_executor::{
    DeferredConstraintSnapshot, DeferredConstraintTracker, DeferredForeignKeyTracker,
    ExecutorMutationSink, ExecutorPredicateLockServices, ExecutorRowLockServices,
    ExecutorTransactionServices, ExecutorTransactionState, ExprEvalBindings, LockStatusProvider,
    PendingForeignKeyCheck, PendingParentForeignKeyCheck, PendingUniqueCheck,
    PendingUserConstraintTrigger, SharedExecutorTransactionState,
};
pub use pgrust_nodes::{
    ConstraintTiming, SessionReplicationRole, StatementResult, TypedFunctionArg,
};

pub trait StatsImportRuntime: Send + Sync {
    fn pg_restore_relation_stats(
        &self,
        ctx: &mut ExecutorContext,
        args: Vec<TypedFunctionArg>,
    ) -> Result<Value, ExecError>;

    fn pg_clear_relation_stats(
        &self,
        ctx: &mut ExecutorContext,
        schemaname: Value,
        relname: Value,
    ) -> Result<Value, ExecError>;

    fn pg_restore_attribute_stats(
        &self,
        ctx: &mut ExecutorContext,
        args: Vec<TypedFunctionArg>,
    ) -> Result<Value, ExecError>;

    fn pg_clear_attribute_stats(
        &self,
        ctx: &mut ExecutorContext,
        schemaname: Value,
        relname: Value,
        attname: Value,
        inherited: Value,
    ) -> Result<Value, ExecError>;
}

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub data_dir: Option<std::path::PathBuf>,
    pub txns: std::sync::Arc<parking_lot::RwLock<TransactionManager>>,
    pub txn_waiter: Option<std::sync::Arc<TransactionWaiter>>,
    pub lock_status_provider: Option<std::sync::Arc<dyn LockStatusProvider>>,
    pub sequences: Option<std::sync::Arc<SequenceRuntime>>,
    pub large_objects: Option<std::sync::Arc<LargeObjectRuntime>>,
    pub stats_import_runtime: Option<std::sync::Arc<dyn StatsImportRuntime>>,
    pub async_notify_runtime: Option<std::sync::Arc<AsyncNotifyRuntime>>,
    pub advisory_locks: std::sync::Arc<AdvisoryLockManager>,
    pub row_locks: std::sync::Arc<RowLockManager>,
    pub checkpoint_stats: CheckpointStatsSnapshot,
    pub datetime_config: DateTimeConfig,
    pub statement_timestamp_usecs: i64,
    pub gucs: HashMap<String, String>,
    pub interrupts: std::sync::Arc<InterruptState>,
    pub stats: std::sync::Arc<parking_lot::RwLock<DatabaseStatsStore>>,
    pub session_stats: std::sync::Arc<parking_lot::RwLock<SessionStatsState>>,
    pub snapshot: Snapshot,
    pub write_xid_override: Option<TransactionId>,
    pub transaction_state: Option<SharedExecutorTransactionState>,
    pub client_id: ClientId,
    pub current_database_name: String,
    pub session_user_oid: u32,
    pub current_user_oid: u32,
    pub active_role_oid: Option<u32>,
    pub session_replication_role: SessionReplicationRole,
    pub statement_lock_scope_id: Option<u64>,
    pub transaction_lock_scope_id: Option<u64>,
    pub next_command_id: CommandId,
    pub default_toast_compression: crate::include::access::htup::AttributeCompression,
    pub random_state: std::sync::Arc<parking_lot::Mutex<PgPrngState>>,
    pub expr_bindings: ExprEvalBindings,
    pub case_test_values: Vec<Value>,
    pub system_bindings: Vec<SystemVarBinding>,
    pub active_grouping_refs: Vec<usize>,
    pub subplans: Vec<Plan>,
    /// When true, each node records per-node timing stats (for EXPLAIN ANALYZE).
    pub timed: bool,
    pub allow_side_effects: bool,
    pub security_restricted: bool,
    pub pending_async_notifications: Vec<PendingNotification>,
    pub catalog_effects: Vec<CatalogMutationEffect>,
    pub temp_effects: Vec<TempMutationEffect>,
    pub database: Option<Database>,
    pub pending_catalog_effects: Vec<CatalogMutationEffect>,
    pub pending_table_locks: Vec<RelFileLocator>,
    pub pending_portals: Vec<Portal>,
    pub copy_freeze_relation_oids: Vec<u32>,
    pub catalog: Option<ExecutorCatalog>,
    pub scalar_function_cache: HashMap<u32, ScalarFunctionCallInfo>,
    pub proc_execute_acl_cache: HashSet<(u32, u32)>,
    pub srf_rows_cache: HashMap<String, Vec<TupleSlot>>,
    pub plpgsql_function_cache: Arc<parking_lot::RwLock<PlpgsqlFunctionCache>>,
    pub pinned_cte_tables: HashMap<usize, Rc<RefCell<MaterializedCteTable>>>,
    pub cte_tables: HashMap<usize, Rc<RefCell<MaterializedCteTable>>>,
    pub cte_producers: HashMap<usize, Rc<RefCell<PlanState>>>,
    pub recursive_worktables: HashMap<usize, Rc<RefCell<RecursiveWorkTable>>>,
    pub deferred_foreign_keys: Option<DeferredForeignKeyTracker>,
    pub trigger_depth: usize,
}

impl ExecutorContext {
    pub fn check_for_interrupts(&self) -> Result<(), ExecError> {
        check_for_interrupts(&self.interrupts).map_err(ExecError::Interrupted)
    }

    pub fn check_stack_depth(&self) -> Result<(), ExecError> {
        stack_depth::check_stack_depth(self.datetime_config.max_stack_depth_kb)
    }

    pub fn bytea_output(&self) -> crate::pgrust::session::ByteaOutputFormat {
        match self
            .gucs
            .get("bytea_output")
            .map(|value| value.trim().to_ascii_lowercase())
        {
            Some(value) if value == "escape" => crate::pgrust::session::ByteaOutputFormat::Escape,
            _ => crate::pgrust::session::ByteaOutputFormat::Hex,
        }
    }

    pub fn temp_buffers_pages(&self) -> Result<usize, ExecError> {
        let default_pages = TEMP_BUFFERS_DEFAULT_PAGES.to_string();
        let value = self
            .gucs
            .get("temp_buffers")
            .map(String::as_str)
            .unwrap_or(default_pages.as_str())
            .trim();
        let pages = value
            .parse::<usize>()
            .map_err(|_| ExecError::DetailedError {
                message: format!("invalid value for parameter \"temp_buffers\": \"{value}\""),
                detail: None,
                hint: None,
                sqlstate: "22023",
            })?;
        if pages < TEMP_BUFFERS_MIN_PAGES {
            return Err(ExecError::DetailedError {
                message: format!("invalid value for parameter \"temp_buffers\": \"{value}\""),
                detail: Some(format!(
                    "\"temp_buffers\" must be at least {TEMP_BUFFERS_MIN_PAGES}."
                )),
                hint: None,
                sqlstate: "22023",
            });
        }
        Ok(pages)
    }

    pub fn local_buffer_manager(
        &self,
    ) -> Result<Arc<LocalBufferManager<SmgrStorageBackend>>, ExecError> {
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| ExecError::DetailedError {
                message: "temporary buffers require a database session".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        Ok(database.local_buffer_manager(self.client_id, self.temp_buffers_pages()?))
    }

    pub fn relation_uses_local_buffers(&self, relation_oid: u32) -> bool {
        self.catalog
            .as_deref()
            .and_then(|catalog| catalog.relation_by_oid(relation_oid))
            .is_some_and(|relation| relation.relpersistence == 't')
    }

    pub fn transaction_xid(&self) -> Option<TransactionId> {
        if self.snapshot.current_xid != INVALID_TRANSACTION_ID {
            return Some(self.snapshot.current_xid);
        }
        self.transaction_state
            .as_ref()
            .and_then(|state| state.lock().xid)
    }

    pub fn write_snapshot(&self) -> Snapshot {
        let mut snapshot = self.snapshot.clone();
        if let Some(xid) = self.write_xid_override {
            snapshot.current_xid = xid;
            snapshot.own_xids.insert(xid);
        }
        snapshot
    }

    pub fn uses_transaction_snapshot(&self) -> bool {
        self.transaction_state
            .as_ref()
            .is_some_and(|state| state.lock().transaction_snapshot.is_some())
    }

    pub fn serializable_xact_id(&self) -> Option<SerializableXactId> {
        if !self
            .gucs
            .get("transaction_isolation")
            .is_some_and(|value| value.eq_ignore_ascii_case("serializable"))
        {
            return None;
        }
        self.transaction_state
            .as_ref()
            .and_then(|state| state.lock().serializable_xact)
    }

    pub fn constraint_timing(
        &self,
        constraint_oid: u32,
        deferrable: bool,
        initially_deferred: bool,
    ) -> ConstraintTiming {
        self.deferred_foreign_keys
            .as_ref()
            .map(|tracker| tracker.effective_timing(constraint_oid, deferrable, initially_deferred))
            .unwrap_or(if deferrable && initially_deferred {
                ConstraintTiming::Deferred
            } else {
                ConstraintTiming::Immediate
            })
    }

    pub fn ensure_write_xid(&mut self) -> Result<TransactionId, ExecError> {
        if let Some(xid) = self.write_xid_override {
            return Ok(xid);
        }
        if self.snapshot.current_xid != INVALID_TRANSACTION_ID {
            return Ok(self.snapshot.current_xid);
        }

        let Some(transaction_state) = &self.transaction_state else {
            return Err(ExecError::DetailedError {
                message: "cannot execute heap write without a transaction id".into(),
                detail: Some("executor context did not provide lazy transaction-id state".into()),
                hint: None,
                sqlstate: "XX000",
            });
        };

        let mut state = transaction_state.lock();
        let xid = match state.xid {
            Some(xid) => xid,
            None => {
                let xid = self.txns.write().begin();
                state.xid = Some(xid);
                if let Some(waiter) = &self.txn_waiter {
                    waiter.register_holder(xid, self.client_id);
                }
                if let (Some(db), Some(serializable_xact)) =
                    (self.database.as_ref(), state.serializable_xact)
                {
                    db.predicate_locks
                        .register_xid(serializable_xact, xid)
                        .map_err(ExecError::from)?;
                }
                xid
            }
        };

        if let Some(base_snapshot) = &state.transaction_snapshot {
            self.snapshot = base_snapshot.clone();
            self.snapshot.current_xid = xid;
            // :HACK: pgrust does not yet model PostgreSQL SPI command counters
            // inside PL/pgSQL, so keep existing same-statement read-your-writes
            // behavior after a lazy XID is assigned.
            self.snapshot.current_cid = CommandId::MAX;
        } else {
            self.snapshot = self
                .txns
                .read()
                // :HACK: pgrust does not yet model PostgreSQL SPI command counters
                // inside PL/pgSQL, so keep existing same-statement read-your-writes
                // behavior after a lazy XID is assigned.
                .snapshot_for_command(xid, CommandId::MAX)
                .map_err(|e| ExecError::Heap(HeapError::Mvcc(e)))?;
        }
        Ok(xid)
    }

    pub fn row_lock_owner(&self) -> RowLockOwner {
        if let Some(scope_id) = self.transaction_lock_scope_id {
            RowLockOwner::transaction(self.client_id, scope_id)
        } else if let Some(scope_id) = self.statement_lock_scope_id {
            RowLockOwner::statement(self.client_id, scope_id)
        } else {
            RowLockOwner::session(self.client_id)
        }
    }

    pub fn acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: crate::include::access::itemptr::ItemPointerData,
        mode: RowLockMode,
    ) -> Result<(), ExecError> {
        match self.row_locks.lock_interruptible(
            RowLockTag { relation_oid, tid },
            mode,
            self.row_lock_owner(),
            self.interrupts.as_ref(),
        ) {
            Ok(()) => Ok(()),
            Err(RowLockError::Interrupted(reason)) => Err(ExecError::Interrupted(reason)),
            Err(RowLockError::DeadlockTimeout) => Err(ExecError::DetailedError {
                message: "deadlock detected".into(),
                detail: None,
                hint: None,
                sqlstate: "40P01",
            }),
        }
    }

    pub fn try_acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: crate::include::access::itemptr::ItemPointerData,
        mode: RowLockMode,
    ) -> bool {
        self.row_locks.try_lock(
            RowLockTag { relation_oid, tid },
            mode,
            self.row_lock_owner(),
        )
    }

    pub fn predicate_lock_relation(&self, relation_oid: u32) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        db.predicate_locks
            .predicate_lock(
                serializable_xact,
                PredicateLockTarget::relation(db.database_oid, relation_oid),
            )
            .map_err(ExecError::from)
    }

    pub fn predicate_lock_page(
        &self,
        relation_oid: u32,
        block_number: u32,
    ) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        db.predicate_locks
            .predicate_lock(
                serializable_xact,
                PredicateLockTarget::page(db.database_oid, relation_oid, block_number),
            )
            .map_err(ExecError::from)
    }

    pub fn predicate_lock_tuple(
        &self,
        relation_oid: u32,
        tid: crate::include::access::itemptr::ItemPointerData,
    ) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        db.predicate_locks
            .predicate_lock(
                serializable_xact,
                PredicateLockTarget::tuple(
                    db.database_oid,
                    relation_oid,
                    tid.block_number,
                    tid.offset_number,
                ),
            )
            .map_err(ExecError::from)
    }

    pub fn check_serializable_visible_tuple_xmax(
        &self,
        xmax: Option<TransactionId>,
    ) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        let Some(xmax) = xmax else {
            return Ok(());
        };
        if xmax == INVALID_TRANSACTION_ID || self.snapshot.transaction_is_own(xmax) {
            return Ok(());
        }
        db.predicate_locks
            .check_conflict_out(serializable_xact, xmax)
            .map_err(ExecError::from)
    }

    pub fn check_serializable_write_relation(&self, relation_oid: u32) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        db.predicate_locks
            .check_conflict_in(
                serializable_xact,
                PredicateLockTarget::relation(db.database_oid, relation_oid),
            )
            .map_err(ExecError::from)
    }

    pub fn check_serializable_write_tuple(
        &self,
        relation_oid: u32,
        tid: crate::include::access::itemptr::ItemPointerData,
    ) -> Result<(), ExecError> {
        let Some(serializable_xact) = self.serializable_xact_id() else {
            return Ok(());
        };
        let Some(db) = self.database.as_ref() else {
            return Ok(());
        };
        db.predicate_locks
            .check_conflict_in(
                serializable_xact,
                PredicateLockTarget::tuple(
                    db.database_oid,
                    relation_oid,
                    tid.block_number,
                    tid.offset_number,
                ),
            )
            .map_err(ExecError::from)
    }

    pub fn record_catalog_effect(&mut self, effect: CatalogMutationEffect) {
        self.pending_catalog_effects.push(effect);
    }

    pub fn record_table_lock(&mut self, rel: RelFileLocator) {
        if !self.pending_table_locks.contains(&rel) {
            self.pending_table_locks.push(rel);
        }
    }
}

impl ExecutorTransactionServices for ExecutorContext {
    type Error = ExecError;

    fn transaction_xid(&self) -> Option<TransactionId> {
        ExecutorContext::transaction_xid(self)
    }

    fn write_snapshot(&self) -> Snapshot {
        ExecutorContext::write_snapshot(self)
    }

    fn uses_transaction_snapshot(&self) -> bool {
        ExecutorContext::uses_transaction_snapshot(self)
    }

    fn ensure_write_xid(&mut self) -> Result<TransactionId, Self::Error> {
        ExecutorContext::ensure_write_xid(self)
    }
}

impl ExecutorRowLockServices for ExecutorContext {
    type Error = ExecError;

    fn row_lock_owner(&self) -> RowLockOwner {
        ExecutorContext::row_lock_owner(self)
    }

    fn acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
        mode: RowLockMode,
    ) -> Result<(), Self::Error> {
        ExecutorContext::acquire_row_lock(self, relation_oid, tid, mode)
    }

    fn try_acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
        mode: RowLockMode,
    ) -> bool {
        ExecutorContext::try_acquire_row_lock(self, relation_oid, tid, mode)
    }
}

impl ExecutorPredicateLockServices for ExecutorContext {
    type Error = ExecError;

    fn serializable_xact_id(&self) -> Option<SerializableXactId> {
        ExecutorContext::serializable_xact_id(self)
    }

    fn predicate_lock_relation(&self, relation_oid: u32) -> Result<(), Self::Error> {
        ExecutorContext::predicate_lock_relation(self, relation_oid)
    }

    fn predicate_lock_page(&self, relation_oid: u32, block_number: u32) -> Result<(), Self::Error> {
        ExecutorContext::predicate_lock_page(self, relation_oid, block_number)
    }

    fn predicate_lock_tuple(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
    ) -> Result<(), Self::Error> {
        ExecutorContext::predicate_lock_tuple(self, relation_oid, tid)
    }

    fn check_serializable_visible_tuple_xmax(
        &self,
        xmax: Option<TransactionId>,
    ) -> Result<(), Self::Error> {
        ExecutorContext::check_serializable_visible_tuple_xmax(self, xmax)
    }

    fn check_serializable_write_relation(&self, relation_oid: u32) -> Result<(), Self::Error> {
        ExecutorContext::check_serializable_write_relation(self, relation_oid)
    }

    fn check_serializable_write_tuple(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
    ) -> Result<(), Self::Error> {
        ExecutorContext::check_serializable_write_tuple(self, relation_oid, tid)
    }
}

impl ExecutorMutationSink for ExecutorContext {
    fn record_catalog_effect(&mut self, effect: CatalogMutationEffect) {
        ExecutorContext::record_catalog_effect(self, effect);
    }

    fn record_table_lock(&mut self, rel: RelFileLocator) {
        ExecutorContext::record_table_lock(self, rel);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug)]
pub enum ExecError {
    WithContext {
        source: Box<ExecError>,
        context: String,
    },
    WithInternalQueryContext {
        source: Box<ExecError>,
        context: String,
        query: String,
        position: Option<usize>,
    },
    Heap(HeapError),
    Tuple(TupleError),
    Parse(ParseError),
    UniqueViolation {
        constraint: String,
        detail: Option<String>,
    },
    NotNullViolation {
        relation: String,
        column: String,
        constraint: String,
        detail: Option<String>,
    },
    CheckViolation {
        relation: String,
        constraint: String,
        detail: Option<String>,
    },
    ForeignKeyViolation {
        constraint: String,
        message: String,
        detail: Option<String>,
    },
    InvalidColumn(usize),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    NonBoolQual(Value),
    UnsupportedStorageType {
        column: String,
        ty: ScalarType,
        attlen: i16,
        actual_len: Option<usize>,
    },
    InvalidStorageValue {
        column: String,
        details: String,
    },
    JsonInput {
        raw_input: String,
        message: String,
        detail: Option<String>,
        context: Option<String>,
        sqlstate: &'static str,
    },
    XmlInput {
        raw_input: String,
        message: String,
        detail: Option<String>,
        context: Option<String>,
        sqlstate: &'static str,
    },
    ArrayInput {
        message: String,
        value: String,
        detail: Option<String>,
        sqlstate: &'static str,
    },
    DetailedError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    DiagnosticError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
        column_name: Option<String>,
        constraint_name: Option<String>,
        datatype_name: Option<String>,
        table_name: Option<String>,
        schema_name: Option<String>,
    },
    StringDataRightTruncation {
        ty: String,
    },
    CardinalityViolation {
        message: String,
        hint: Option<String>,
    },
    UnboundOuterColumn {
        depth: usize,
        index: usize,
    },
    MissingRequiredColumn(String),
    Regex(RegexError),
    InvalidRegex(String),
    RaiseException(String),
    DivisionByZero(&'static str),
    GenerateSeriesZeroStep,
    GenerateSeriesInvalidArg(&'static str, &'static str),
    InvalidIntegerInput {
        ty: &'static str,
        value: String,
    },
    IntegerOutOfRange {
        ty: &'static str,
        value: String,
    },
    InvalidNumericInput(String),
    InvalidByteaInput {
        value: String,
    },
    InvalidUuidInput {
        value: String,
    },
    InvalidByteaHexDigit {
        value: String,
        digit: String,
    },
    InvalidByteaHexOddDigits {
        value: String,
    },
    InvalidGeometryInput {
        ty: &'static str,
        value: String,
    },
    InvalidRangeInput {
        ty: &'static str,
        value: String,
    },
    InvalidBitInput {
        digit: char,
        is_hex: bool,
    },
    BitStringLengthMismatch {
        actual: i32,
        expected: i32,
    },
    BitStringTooLong {
        actual: i32,
        limit: i32,
    },
    BitStringSizeMismatch {
        op: &'static str,
    },
    BitIndexOutOfRange {
        index: i32,
        max_index: i32,
    },
    NegativeSubstringLength,
    InvalidBooleanInput {
        value: String,
    },
    InvalidFloatInput {
        ty: &'static str,
        value: String,
    },
    FloatOutOfRange {
        ty: &'static str,
        value: String,
    },
    FloatOverflow,
    FloatUnderflow,
    NumericNaNToInt {
        ty: &'static str,
    },
    NumericInfinityToInt {
        ty: &'static str,
    },
    Int2OutOfRange,
    Int4OutOfRange,
    Int8OutOfRange,
    OidOutOfRange,
    NumericFieldOverflow,
    RequestedLengthTooLarge,
    Interrupted(InterruptReason),
}

impl From<HeapError> for ExecError {
    fn from(value: HeapError) -> Self {
        match value {
            HeapError::Interrupted(reason) => Self::Interrupted(reason),
            other => Self::Heap(other),
        }
    }
}

impl From<TupleError> for ExecError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

impl From<pgrust_executor::TupleDecodeError> for ExecError {
    fn from(value: pgrust_executor::TupleDecodeError) -> Self {
        match value {
            pgrust_executor::TupleDecodeError::Tuple(error) => Self::Tuple(error),
            pgrust_executor::TupleDecodeError::Expr(error) => error.into(),
            pgrust_executor::TupleDecodeError::ToastCompression(error) => error.into(),
            pgrust_executor::TupleDecodeError::InvalidStorageValue { column, details } => {
                Self::InvalidStorageValue { column, details }
            }
        }
    }
}

impl From<pgrust_executor::GenerateSeriesError> for ExecError {
    fn from(value: pgrust_executor::GenerateSeriesError) -> Self {
        match value {
            pgrust_executor::GenerateSeriesError::TypeMismatch { op, left, right } => {
                Self::TypeMismatch { op, left, right }
            }
            pgrust_executor::GenerateSeriesError::ZeroStep => Self::GenerateSeriesZeroStep,
            pgrust_executor::GenerateSeriesError::InfiniteStep => Self::DetailedError {
                message: "step size cannot be infinite".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            },
            pgrust_executor::GenerateSeriesError::InvalidArg(arg, detail) => {
                Self::GenerateSeriesInvalidArg(arg, detail)
            }
        }
    }
}

impl From<pgrust_executor::UnnestError> for ExecError {
    fn from(value: pgrust_executor::UnnestError) -> Self {
        match value {
            pgrust_executor::UnnestError::TypeMismatch { op, left, right } => {
                Self::TypeMismatch { op, left, right }
            }
        }
    }
}

impl From<pgrust_executor::PgOptionsToTableError> for ExecError {
    fn from(value: pgrust_executor::PgOptionsToTableError) -> Self {
        match value {
            pgrust_executor::PgOptionsToTableError::TypeMismatch { op, left, right } => {
                Self::TypeMismatch { op, left, right }
            }
        }
    }
}

impl From<pgrust_executor::GenerateSubscriptsError> for ExecError {
    fn from(value: pgrust_executor::GenerateSubscriptsError) -> Self {
        match value {
            pgrust_executor::GenerateSubscriptsError::Int4OutOfRange => Self::Int4OutOfRange,
        }
    }
}

impl From<pgrust_executor::SqlFunctionBodyError> for ExecError {
    fn from(value: pgrust_executor::SqlFunctionBodyError) -> Self {
        match value {
            pgrust_executor::SqlFunctionBodyError::UnexpectedEof => {
                Self::Parse(ParseError::UnexpectedEof)
            }
        }
    }
}

impl From<ParseError> for ExecError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<pgrust_expr::ExprError> for ExecError {
    fn from(value: pgrust_expr::ExprError) -> Self {
        match value {
            pgrust_expr::ExprError::WithContext { source, context } => Self::WithContext {
                source: Box::new((*source).into()),
                context,
            },
            pgrust_expr::ExprError::Parse(error) => Self::Parse(error),
            pgrust_expr::ExprError::TypeMismatch { op, left, right } => {
                Self::TypeMismatch { op, left, right }
            }
            pgrust_expr::ExprError::NonBoolQual(value) => Self::NonBoolQual(value),
            pgrust_expr::ExprError::UnsupportedStorageType {
                column,
                ty,
                attlen,
                actual_len,
            } => Self::UnsupportedStorageType {
                column,
                ty,
                attlen,
                actual_len,
            },
            pgrust_expr::ExprError::InvalidStorageValue { column, details } => {
                Self::InvalidStorageValue { column, details }
            }
            pgrust_expr::ExprError::JsonInput {
                raw_input,
                message,
                detail,
                context,
                sqlstate,
            } => Self::JsonInput {
                raw_input,
                message,
                detail,
                context,
                sqlstate,
            },
            pgrust_expr::ExprError::XmlInput {
                raw_input,
                message,
                detail,
                context,
                sqlstate,
            } => Self::XmlInput {
                raw_input,
                message,
                detail,
                context,
                sqlstate,
            },
            pgrust_expr::ExprError::ArrayInput {
                message,
                value,
                detail,
                sqlstate,
            } => Self::ArrayInput {
                message,
                value,
                detail,
                sqlstate,
            },
            pgrust_expr::ExprError::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            } => Self::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            },
            pgrust_expr::ExprError::DiagnosticError {
                message,
                detail,
                hint,
                sqlstate,
                column_name,
                constraint_name,
                datatype_name,
                table_name,
                schema_name,
            } => Self::DiagnosticError {
                message,
                detail,
                hint,
                sqlstate,
                column_name,
                constraint_name,
                datatype_name,
                table_name,
                schema_name,
            },
            pgrust_expr::ExprError::StringDataRightTruncation { ty } => {
                Self::StringDataRightTruncation { ty }
            }
            pgrust_expr::ExprError::MissingRequiredColumn(column) => {
                Self::MissingRequiredColumn(column)
            }
            pgrust_expr::ExprError::Regex(error) => Self::Regex(RegexError {
                sqlstate: error.sqlstate,
                message: error.message,
                detail: error.detail,
                hint: error.hint,
                context: error.context,
            }),
            pgrust_expr::ExprError::InvalidRegex(message) => Self::InvalidRegex(message),
            pgrust_expr::ExprError::RaiseException(message) => Self::RaiseException(message),
            pgrust_expr::ExprError::DivisionByZero(op) => Self::DivisionByZero(op),
            pgrust_expr::ExprError::InvalidIntegerInput { ty, value } => {
                Self::InvalidIntegerInput { ty, value }
            }
            pgrust_expr::ExprError::IntegerOutOfRange { ty, value } => {
                Self::IntegerOutOfRange { ty, value }
            }
            pgrust_expr::ExprError::InvalidNumericInput(value) => Self::InvalidNumericInput(value),
            pgrust_expr::ExprError::InvalidByteaInput { value } => {
                Self::InvalidByteaInput { value }
            }
            pgrust_expr::ExprError::InvalidUuidInput { value } => Self::InvalidUuidInput { value },
            pgrust_expr::ExprError::InvalidByteaHexDigit { value, digit } => {
                Self::InvalidByteaHexDigit { value, digit }
            }
            pgrust_expr::ExprError::InvalidByteaHexOddDigits { value } => {
                Self::InvalidByteaHexOddDigits { value }
            }
            pgrust_expr::ExprError::InvalidGeometryInput { ty, value } => {
                Self::InvalidGeometryInput { ty, value }
            }
            pgrust_expr::ExprError::InvalidRangeInput { ty, value } => {
                Self::InvalidRangeInput { ty, value }
            }
            pgrust_expr::ExprError::InvalidBitInput { digit, is_hex } => {
                Self::InvalidBitInput { digit, is_hex }
            }
            pgrust_expr::ExprError::BitStringLengthMismatch { actual, expected } => {
                Self::BitStringLengthMismatch { actual, expected }
            }
            pgrust_expr::ExprError::BitStringTooLong { actual, limit } => {
                Self::BitStringTooLong { actual, limit }
            }
            pgrust_expr::ExprError::BitStringSizeMismatch { op } => {
                Self::BitStringSizeMismatch { op }
            }
            pgrust_expr::ExprError::BitIndexOutOfRange { index, max_index } => {
                Self::BitIndexOutOfRange { index, max_index }
            }
            pgrust_expr::ExprError::NegativeSubstringLength => Self::NegativeSubstringLength,
            pgrust_expr::ExprError::InvalidBooleanInput { value } => {
                Self::InvalidBooleanInput { value }
            }
            pgrust_expr::ExprError::InvalidFloatInput { ty, value } => {
                Self::InvalidFloatInput { ty, value }
            }
            pgrust_expr::ExprError::FloatOutOfRange { ty, value } => {
                Self::FloatOutOfRange { ty, value }
            }
            pgrust_expr::ExprError::FloatOverflow => Self::FloatOverflow,
            pgrust_expr::ExprError::FloatUnderflow => Self::FloatUnderflow,
            pgrust_expr::ExprError::NumericNaNToInt { ty } => Self::NumericNaNToInt { ty },
            pgrust_expr::ExprError::NumericInfinityToInt { ty } => {
                Self::NumericInfinityToInt { ty }
            }
            pgrust_expr::ExprError::Int2OutOfRange => Self::Int2OutOfRange,
            pgrust_expr::ExprError::Int4OutOfRange => Self::Int4OutOfRange,
            pgrust_expr::ExprError::Int8OutOfRange => Self::Int8OutOfRange,
            pgrust_expr::ExprError::OidOutOfRange => Self::OidOutOfRange,
            pgrust_expr::ExprError::NumericFieldOverflow => Self::NumericFieldOverflow,
            pgrust_expr::ExprError::RequestedLengthTooLarge => Self::RequestedLengthTooLarge,
        }
    }
}

impl From<CatalogError> for ExecError {
    fn from(value: CatalogError) -> Self {
        match value {
            CatalogError::UniqueViolation(constraint) => Self::UniqueViolation {
                constraint,
                detail: None,
            },
            CatalogError::Interrupted(reason) => Self::Interrupted(reason),
            other => Self::Parse(ParseError::UnexpectedToken {
                expected: "catalog operation",
                actual: format!("{other:?}"),
            }),
        }
    }
}

impl From<TableLockError> for ExecError {
    fn from(value: TableLockError) -> Self {
        match value {
            TableLockError::Interrupted(reason) => Self::Interrupted(reason),
        }
    }
}

impl From<PredicateLockError> for ExecError {
    fn from(value: PredicateLockError) -> Self {
        match value {
            PredicateLockError::SerializationFailure(reason) => {
                serialization_failure_for_ssi(reason)
            }
            PredicateLockError::UnknownSerializableTransaction(id) => Self::DetailedError {
                message: format!("unknown serializable transaction {:?}", id),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            },
            PredicateLockError::Interrupted(reason) => Self::Interrupted(reason),
        }
    }
}

fn serialization_failure_for_ssi(reason: PredicateFailureReason) -> ExecError {
    ExecError::DetailedError {
        message: "could not serialize access due to read/write dependencies among transactions"
            .into(),
        detail: Some(reason.detail().into()),
        hint: Some("The transaction might succeed if retried.".into()),
        sqlstate: "40001",
    }
}

impl From<MvccError> for ExecError {
    fn from(value: MvccError) -> Self {
        Self::Heap(HeapError::Mvcc(value))
    }
}

#[cfg(test)]
mod tests_support;

#[cfg(test)]
mod tests;
