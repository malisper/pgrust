#![allow(unused_imports)]

mod agg;
mod constraints;
mod driver;
pub mod exec_expr;
pub(crate) mod exec_tuples;
mod expr_bit;
mod expr_bool;
mod expr_casts;
mod expr_compile;
mod expr_date;
mod expr_datetime;
mod expr_format;
pub(crate) mod expr_geometry;
mod expr_json;
mod expr_locks;
mod expr_math;
mod expr_money;
mod expr_multirange;
mod expr_numeric;
pub(crate) mod expr_ops;
pub(crate) mod expr_range;
mod expr_string;
mod expr_xml;
mod foreign_keys;
pub(crate) mod hashjoin;
pub(crate) mod jsonb;
pub(crate) mod jsonpath;
mod node_hash;
mod node_hashjoin;
mod nodes;
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
pub(crate) use agg::{AccumState, AggGroup, AggTransitionFn, OrderedAggInput};
pub use driver::{
    exec_next, execute_plan, execute_planned_stmt, execute_readonly_statement, execute_sql,
    execute_statement,
};
pub use exec_expr::{eval_expr, eval_plpgsql_expr};
pub(crate) use expr_bit::render_bit_text;
pub(crate) use expr_casts::cast_value;
pub(crate) use expr_casts::parse_bytea_text;
pub(crate) use expr_casts::parse_text_array_literal_with_op;
pub use expr_casts::render_internal_char_text;
pub use expr_datetime::{render_datetime_value_text, render_datetime_value_text_with_config};
pub(crate) use expr_geometry::eval_geometry_function;
pub(crate) use expr_geometry::geometry_input_error_message;
pub(crate) use expr_geometry::render_geometry_text;
pub use expr_money::money_format_text;
pub(crate) use expr_money::money_parse_text;
pub use expr_multirange::render_multirange_text;
pub(crate) use expr_multirange::{
    compare_multirange_values, decode_multirange_bytes, encode_multirange_bytes,
    eval_multirange_function, multirange_intersection_agg_transition, parse_multirange_text,
    range_agg_transition,
};
pub use expr_range::render_range_text;
pub(crate) use expr_range::{
    compare_range_values, decode_range_bytes, encode_range_bytes, eval_range_function,
    parse_range_text,
};
pub(crate) use expr_xml::validate_xml_input;
pub(crate) use nodes::{
    render_explain_expr, render_explain_projection_expr_with_qualifier,
};
pub use startup::executor_start;
pub(crate) use tsearch::{
    compare_tsquery, compare_tsvector, concat_tsvector, decode_tsquery_bytes,
    decode_tsvector_bytes, encode_tsquery_bytes, encode_tsvector_bytes,
    eval_tsquery_matches_tsvector, eval_tsvector_matches_tsquery, parse_tsquery_text,
    parse_tsvector_text, render_tsquery_text, render_tsvector_text, tsquery_and,
    tsquery_input_error, tsquery_not, tsquery_or, tsvector_input_error,
};
pub use value_io::format_array_value_text;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::access::transam::xact::{
    CommandId, MvccError, Snapshot, TransactionId, TransactionManager,
};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::Catalog;
use crate::backend::commands::tablecmds::*;
use crate::backend::parser::{
    ParseError, Statement, bind_delete, bind_insert, bind_update, parse_statement, pg_plan_query,
    pg_plan_values_query,
};
use crate::backend::storage::lmgr::AdvisoryLockManager;
use crate::backend::storage::lmgr::TableLockError;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::include::access::htup::TupleError;
use crate::pgrust::database::{
    DatabaseStatsStore, LargeObjectRuntime, SequenceRuntime, SessionStatsState, TransactionWaiter,
};
use crate::pl::plpgsql::CompiledFunction;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;

pub(crate) use constraints::{enforce_relation_constraints, enforce_row_security_write_checks};
pub(crate) use expr_ops::compare_order_values;
use expr_ops::parse_numeric_text;
pub(crate) use foreign_keys::{
    enforce_inbound_foreign_key_reference, enforce_inbound_foreign_keys_on_delete,
    enforce_inbound_foreign_keys_on_update, enforce_outbound_foreign_keys,
};

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

#[derive(Debug, Clone, Default)]
pub struct DeferredForeignKeyTracker {
    affected_constraint_oids: Arc<parking_lot::Mutex<BTreeSet<u32>>>,
}

impl DeferredForeignKeyTracker {
    pub fn record(&self, constraint_oid: u32) {
        if constraint_oid == 0 {
            return;
        }
        self.affected_constraint_oids.lock().insert(constraint_oid);
    }

    pub fn affected_constraint_oids(&self) -> Vec<u32> {
        self.affected_constraint_oids
            .lock()
            .iter()
            .copied()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.affected_constraint_oids.lock().is_empty()
    }
}

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<parking_lot::RwLock<TransactionManager>>,
    pub txn_waiter: Option<std::sync::Arc<TransactionWaiter>>,
    pub sequences: Option<std::sync::Arc<SequenceRuntime>>,
    pub large_objects: Option<std::sync::Arc<LargeObjectRuntime>>,
    pub advisory_locks: std::sync::Arc<AdvisoryLockManager>,
    pub checkpoint_stats: CheckpointStatsSnapshot,
    pub datetime_config: DateTimeConfig,
    pub interrupts: std::sync::Arc<InterruptState>,
    pub stats: std::sync::Arc<parking_lot::RwLock<DatabaseStatsStore>>,
    pub session_stats: std::sync::Arc<parking_lot::RwLock<SessionStatsState>>,
    pub snapshot: Snapshot,
    pub client_id: ClientId,
    pub current_database_name: String,
    pub session_user_oid: u32,
    pub current_user_oid: u32,
    pub active_role_oid: Option<u32>,
    pub statement_lock_scope_id: Option<u64>,
    pub next_command_id: CommandId,
    pub default_toast_compression: crate::include::access::htup::AttributeCompression,
    pub expr_bindings: ExprEvalBindings,
    pub case_test_values: Vec<Value>,
    pub system_bindings: Vec<SystemVarBinding>,
    pub subplans: Vec<Plan>,
    /// When true, each node records per-node timing stats (for EXPLAIN ANALYZE).
    pub timed: bool,
    pub allow_side_effects: bool,
    pub catalog: Option<VisibleCatalog>,
    pub compiled_functions: HashMap<u32, Arc<CompiledFunction>>,
    pub cte_tables: HashMap<usize, Rc<RefCell<MaterializedCteTable>>>,
    pub cte_producers: HashMap<usize, Rc<RefCell<PlanState>>>,
    pub recursive_worktables: HashMap<usize, Rc<RefCell<RecursiveWorkTable>>>,
    pub deferred_foreign_keys: Option<DeferredForeignKeyTracker>,
}

impl ExecutorContext {
    pub fn check_for_interrupts(&self) -> Result<(), ExecError> {
        check_for_interrupts(&self.interrupts).map_err(ExecError::Interrupted)
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
    Heap(HeapError),
    Tuple(TupleError),
    Parse(ParseError),
    UniqueViolation {
        constraint: String,
    },
    NotNullViolation {
        relation: String,
        column: String,
        constraint: String,
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
            CatalogError::UniqueViolation(constraint) => Self::UniqueViolation { constraint },
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
mod tests;
