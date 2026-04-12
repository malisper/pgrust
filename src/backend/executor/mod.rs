mod agg;
mod expr_bit;
mod expr_casts;
mod expr_bool;
mod expr_compile;
mod expr_format;
mod expr_math;
mod expr_numeric;
mod expr_ops;
mod expr_string;
mod driver;
mod expr_json;
pub mod exec_expr;
pub(crate) mod exec_tuples;
pub(crate) mod jsonb;
pub(crate) mod jsonpath;
mod nodes;
mod startup;
pub(crate) mod value_io;
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

pub(crate) use agg::{AccumState, AggGroup};
pub use crate::include::nodes::datum::*;
pub use crate::include::nodes::execnodes::*;
pub use crate::include::nodes::plannodes::*;
pub use driver::{
    exec_next, execute_plan, execute_readonly_statement, execute_sql, execute_statement,
};
pub use exec_expr::{eval_expr, eval_plpgsql_expr};
pub(crate) use expr_casts::parse_bytea_text;
pub(crate) use expr_casts::cast_value;
pub(crate) use expr_bit::render_bit_text;
pub use expr_casts::render_internal_char_text;
pub use startup::executor_start;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::access::transam::xact::{
    CommandId, MvccError, Snapshot, TransactionId, TransactionManager,
};
use crate::backend::catalog::catalog::Catalog;
use crate::backend::commands::tablecmds::*;
use crate::backend::parser::{
    ParseError, Statement, bind_delete, bind_insert, bind_update, build_plan, build_values_plan,
    parse_statement,
};
use crate::include::access::htup::TupleError;
use crate::{BufferPool, ClientId, SmgrStorageBackend};

use expr_ops::{compare_order_values, parse_numeric_text};

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<parking_lot::RwLock<TransactionManager>>,
    pub snapshot: Snapshot,
    pub client_id: ClientId,
    pub next_command_id: CommandId,
    pub outer_rows: Vec<Vec<Value>>,
    /// When true, each node records per-node timing stats (for EXPLAIN ANALYZE).
    pub timed: bool,
}

#[derive(Debug)]
pub enum ExecError {
    Heap(HeapError),
    Tuple(TupleError),
    Parse(ParseError),
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
    StringDataRightTruncation {
        ty: String,
    },
    CardinalityViolation(String),
    UnboundOuterColumn {
        depth: usize,
        index: usize,
    },
    MissingRequiredColumn(String),
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
    Int2OutOfRange,
    Int4OutOfRange,
    Int8OutOfRange,
    OidOutOfRange,
    NumericFieldOverflow,
    RequestedLengthTooLarge,
}

impl From<HeapError> for ExecError {
    fn from(value: HeapError) -> Self {
        Self::Heap(value)
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
