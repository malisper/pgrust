#![allow(unused_imports)]

mod agg;
mod constraints;
mod driver;
pub mod exec_expr;
pub(crate) mod exec_tuples;
mod expr_agg_support;
mod expr_async;
mod expr_bit;
pub(crate) mod expr_bool;
mod expr_casts;
mod expr_compile;
mod expr_date;
mod expr_datetime;
mod expr_format;
pub(crate) mod expr_geometry;
mod expr_json;
mod expr_locks;
mod expr_mac;
mod expr_math;
mod expr_money;
pub(crate) mod expr_multirange;
mod expr_network;
pub(crate) mod expr_numeric;
pub(crate) mod expr_ops;
pub(crate) mod expr_range;
pub(crate) mod expr_reg;
mod expr_string;
mod expr_txid;
mod expr_xml;
mod foreign_keys;
pub(crate) mod hashjoin;
pub(crate) mod jsonb;
pub(crate) mod jsonpath;
pub(crate) mod mergejoin;
mod node_hash;
mod node_hashjoin;
mod node_mergejoin;
mod nodes;
mod permissions;
mod pg_regex;
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
pub use driver::{
    exec_next, execute_plan, execute_planned_stmt, execute_readonly_statement,
    execute_readonly_statement_with_config, execute_sql, execute_statement,
};
pub use exec_expr::{eval_expr, eval_plpgsql_expr};
pub(crate) use expr_agg_support::build_aggregate_runtime;
pub(crate) use expr_agg_support::execute_scalar_function_value_call;
pub(crate) use expr_bit::render_bit_text;
pub(crate) use expr_casts::cast_value_with_source_type_catalog_and_config;
pub(crate) use expr_casts::parse_bytea_text;
pub(crate) use expr_casts::parse_text_array_literal_with_catalog_and_op;
pub(crate) use expr_casts::parse_text_array_literal_with_op;
pub use expr_casts::render_internal_char_text;
pub(crate) use expr_casts::render_interval_text;
pub(crate) use expr_casts::render_interval_text_with_config;
pub(crate) use expr_casts::render_pg_lsn_text;
pub(crate) use expr_casts::{cast_value, cast_value_with_config};
pub use expr_datetime::{render_datetime_value_text, render_datetime_value_text_with_config};
pub(crate) use expr_geometry::eval_geometry_function;
pub(crate) use expr_geometry::geometry_input_error_message;
pub(crate) use expr_geometry::render_geometry_text;
pub(crate) use expr_json::apply_jsonb_subscript_assignment;
pub(crate) use expr_mac::{
    eval_macaddr_function, macaddr_to_macaddr8, macaddr8_to_macaddr, parse_macaddr_bytes,
    parse_macaddr_text, parse_macaddr8_bytes, parse_macaddr8_text,
};
pub use expr_mac::{render_macaddr_text, render_macaddr8_text};
pub use expr_money::money_format_text;
pub(crate) use expr_money::money_parse_text;
pub(crate) use expr_multirange::{
    compare_multirange_values, decode_multirange_bytes, encode_multirange_bytes,
    eval_multirange_function, multirange_intersection_agg_transition, parse_multirange_text,
    range_agg_transition,
};
pub use expr_multirange::{render_multirange_text, render_multirange_text_with_config};
pub(crate) use expr_network::{
    compare_network_values, encode_network_bytes, eval_network_function, network_btree_upper_bound,
    network_contains, network_merge, network_prefix, parse_cidr_bytes, parse_cidr_text,
    parse_inet_bytes, parse_inet_text, render_network_text,
};
pub(crate) use expr_range::{
    compare_range_values, decode_range_bytes, encode_range_bytes, eval_range_function,
    parse_range_text,
};
pub use expr_range::{render_range_text, render_range_text_with_config};
pub(crate) use expr_txid::{
    cast_text_to_txid_snapshot, eval_txid_builtin_function, is_txid_snapshot_type_oid,
};
pub(crate) use expr_xml::validate_xml_input;
pub(crate) use nodes::{
    render_explain_expr, render_explain_join_expr, render_explain_projection_expr_with_qualifier,
    render_index_order_by, render_index_scan_condition_with_key_names,
    render_verbose_range_support_expr,
};
pub(crate) use sqlfunc::{render_sql_literal, substitute_named_arg, substitute_positional_args};
pub(crate) use srf::set_returning_call_label;
pub use startup::executor_start;
pub(crate) use tsearch::{
    array_to_tsvector, compare_tsquery, compare_tsvector, concat_tsvector, decode_tsquery_bytes,
    decode_tsvector_bytes, delete_tsvector_lexemes, encode_tsquery_bytes, encode_tsvector_bytes,
    eval_tsquery_matches_tsvector, eval_tsvector_matches_tsquery, filter_tsvector, numnode,
    parse_ts_weight, parse_tsquery_text, parse_tsvector_text, render_tsquery_text,
    render_tsvector_text, setweight_tsvector, strip_tsvector, text_array_items, ts_rank,
    ts_rank_cd, tsquery_and, tsquery_input_error, tsquery_not, tsquery_or, tsquery_phrase,
    tsvector_input_error, tsvector_to_array, unnest_tsvector,
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
    ParseError, Statement, bind_delete, bind_insert, bind_update, parse_statement, pg_plan_query,
    pg_plan_values_query,
};
use crate::backend::storage::lmgr::TableLockError;
use crate::backend::storage::lmgr::{
    AdvisoryLockManager, RowLockError, RowLockManager, RowLockMode, RowLockOwner, RowLockTag,
};
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::backend::utils::misc::stack_depth;
use crate::include::access::htup::TupleError;
use crate::include::access::itemptr::ItemPointerData;
use crate::pgrust::database::{
    AsyncNotifyRuntime, DatabaseStatsStore, LargeObjectRuntime, PendingNotification,
    SequenceRuntime, SessionStatsState, TransactionWaiter,
};
use crate::pl::plpgsql::CompiledFunction;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

pub(crate) use constraints::{enforce_relation_constraints, enforce_row_security_write_checks};
pub(crate) use expr_ops::compare_order_values;
use expr_ops::parse_numeric_text;
pub(crate) use foreign_keys::{
    enforce_inbound_foreign_key_reference, enforce_inbound_foreign_keys_on_delete,
    enforce_inbound_foreign_keys_on_update, enforce_outbound_foreign_keys,
};
pub(crate) use permissions::relation_values_visible_for_error_detail;

#[derive(Debug, Clone, Default)]
pub struct ExprEvalBindings {
    pub exec_params: HashMap<usize, Value>,
    pub outer_tuple: Option<Vec<Value>>,
    pub outer_system_bindings: Vec<SystemVarBinding>,
    pub inner_tuple: Option<Vec<Value>>,
    pub inner_system_bindings: Vec<SystemVarBinding>,
    pub index_tuple: Option<Vec<Value>>,
    pub index_system_bindings: Vec<SystemVarBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintTiming {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingUniqueCheck {
    pub heap_tid: ItemPointerData,
    pub key_values: Vec<Value>,
}

#[derive(Debug, Default)]
struct DeferredConstraintState {
    all_override: Option<ConstraintTiming>,
    named_overrides: BTreeMap<u32, ConstraintTiming>,
    affected_constraint_oids: BTreeSet<u32>,
    pending_unique_checks: HashMap<u32, HashSet<PendingUniqueCheck>>,
}

#[derive(Debug, Clone, Default)]
pub struct DeferredConstraintTracker {
    state: Arc<parking_lot::Mutex<DeferredConstraintState>>,
}

pub type DeferredForeignKeyTracker = DeferredConstraintTracker;

impl DeferredConstraintTracker {
    pub fn record(&self, constraint_oid: u32) {
        if constraint_oid == 0 {
            return;
        }
        self.state
            .lock()
            .affected_constraint_oids
            .insert(constraint_oid);
    }

    pub fn record_unique(
        &self,
        constraint_oid: u32,
        heap_tid: ItemPointerData,
        mut key_values: Vec<Value>,
    ) {
        if constraint_oid == 0 {
            return;
        }
        Value::materialize_all(&mut key_values);
        self.state
            .lock()
            .pending_unique_checks
            .entry(constraint_oid)
            .or_default()
            .insert(PendingUniqueCheck {
                heap_tid,
                key_values,
            });
    }

    pub fn affected_constraint_oids(&self) -> Vec<u32> {
        self.state
            .lock()
            .affected_constraint_oids
            .iter()
            .copied()
            .collect()
    }

    pub fn pending_unique_constraint_oids(&self) -> Vec<u32> {
        self.state
            .lock()
            .pending_unique_checks
            .keys()
            .copied()
            .collect()
    }

    pub fn pending_unique_checks(&self, constraint_oid: u32) -> Vec<PendingUniqueCheck> {
        self.state
            .lock()
            .pending_unique_checks
            .get(&constraint_oid)
            .map(|checks| checks.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn clear_foreign_key_constraints(&self, constraint_oids: &BTreeSet<u32>) {
        let mut state = self.state.lock();
        for constraint_oid in constraint_oids {
            state.affected_constraint_oids.remove(constraint_oid);
        }
    }

    pub fn clear_unique_constraints(&self, constraint_oids: &BTreeSet<u32>) {
        let mut state = self.state.lock();
        for constraint_oid in constraint_oids {
            state.pending_unique_checks.remove(constraint_oid);
        }
    }

    pub fn set_all_timing(&self, timing: ConstraintTiming) {
        let mut state = self.state.lock();
        state.named_overrides.clear();
        state.all_override = Some(timing);
    }

    pub fn set_constraint_timing(&self, constraint_oid: u32, timing: ConstraintTiming) {
        if constraint_oid == 0 {
            return;
        }
        self.state
            .lock()
            .named_overrides
            .insert(constraint_oid, timing);
    }

    pub fn effective_timing(
        &self,
        constraint_oid: u32,
        deferrable: bool,
        initially_deferred: bool,
    ) -> ConstraintTiming {
        if !deferrable {
            return ConstraintTiming::Immediate;
        }
        let state = self.state.lock();
        state
            .named_overrides
            .get(&constraint_oid)
            .copied()
            .or(state.all_override)
            .unwrap_or(if initially_deferred {
                ConstraintTiming::Deferred
            } else {
                ConstraintTiming::Immediate
            })
    }

    pub fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.affected_constraint_oids.is_empty() && state.pending_unique_checks.is_empty()
    }
}

pub trait LockStatusProvider: Send + Sync {
    fn pg_lock_status_rows(&self, current_client_id: ClientId) -> Vec<Vec<Value>>;
}

#[derive(Debug, Clone)]
pub struct TypedFunctionArg {
    pub value: Value,
    pub sql_type: Option<SqlType>,
}

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionReplicationRole {
    #[default]
    Origin,
    Replica,
    Local,
}

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
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
    pub expr_bindings: ExprEvalBindings,
    pub case_test_values: Vec<Value>,
    pub system_bindings: Vec<SystemVarBinding>,
    pub subplans: Vec<Plan>,
    /// When true, each node records per-node timing stats (for EXPLAIN ANALYZE).
    pub timed: bool,
    pub allow_side_effects: bool,
    pub pending_async_notifications: Vec<PendingNotification>,
    pub pending_catalog_effects: Vec<CatalogMutationEffect>,
    pub pending_table_locks: Vec<RelFileLocator>,
    pub catalog: Option<VisibleCatalog>,
    pub compiled_functions: HashMap<u32, Arc<CompiledFunction>>,
    pub cte_tables: HashMap<usize, Rc<RefCell<MaterializedCteTable>>>,
    pub cte_producers: HashMap<usize, Rc<RefCell<PlanState>>>,
    pub recursive_worktables: HashMap<usize, Rc<RefCell<RecursiveWorkTable>>>,
    pub deferred_foreign_keys: Option<DeferredForeignKeyTracker>,
    pub trigger_depth: usize,
}

#[derive(Debug)]
pub struct ExecutorTransactionState {
    pub xid: Option<TransactionId>,
    pub cid: CommandId,
}

pub type SharedExecutorTransactionState = Arc<parking_lot::Mutex<ExecutorTransactionState>>;

impl ExecutorContext {
    pub fn check_for_interrupts(&self) -> Result<(), ExecError> {
        check_for_interrupts(&self.interrupts).map_err(ExecError::Interrupted)
    }

    pub fn check_stack_depth(&self) -> Result<(), ExecError> {
        stack_depth::check_stack_depth(self.datetime_config.max_stack_depth_kb)
    }

    pub fn transaction_xid(&self) -> Option<TransactionId> {
        if self.snapshot.current_xid != INVALID_TRANSACTION_ID {
            return Some(self.snapshot.current_xid);
        }
        self.transaction_state
            .as_ref()
            .and_then(|state| state.lock().xid)
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
                xid
            }
        };

        self.snapshot = self
            .txns
            .read()
            // :HACK: pgrust does not yet model PostgreSQL SPI command counters
            // inside PL/pgSQL, so keep existing same-statement read-your-writes
            // behavior after a lazy XID is assigned.
            .snapshot_for_command(xid, CommandId::MAX)
            .map_err(|e| ExecError::Heap(HeapError::Mvcc(e)))?;
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

    pub fn record_catalog_effect(&mut self, effect: CatalogMutationEffect) {
        self.pending_catalog_effects.push(effect);
    }

    pub fn record_table_lock(&mut self, rel: RelFileLocator) {
        if !self.pending_table_locks.contains(&rel) {
            self.pending_table_locks.push(rel);
        }
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

impl From<ParseError> for ExecError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
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

impl From<MvccError> for ExecError {
    fn from(value: MvccError) -> Self {
        Self::Heap(HeapError::Mvcc(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResult {
    Query {
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(usize),
}

#[cfg(test)]
mod tests_support;

#[cfg(test)]
mod tests;
